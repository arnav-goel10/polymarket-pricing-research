//! Leader lease with a fencing epoch.
//!
//! One node at a time archives recordings. It holds a lease: a grant with a
//! holder, an expiry and an epoch. Every new tenure gets a higher epoch than
//! any before it. Renewing keeps the epoch; losing the lease (expiry,
//! release, takeover) means the next holder gets a new one.
//!
//! A lease alone is not enough. A leader can stall (GC pause, a frozen VM,
//! a lost network) past its expiry, wake up and keep writing while a new
//! leader also writes. The fix is fencing: every write carries the epoch of
//! the grant it was made under, and the sink checks it at the moment of the
//! write.
//!
//! The sink asks the lease, not its own memory. [`Fenced::write`] goes
//! through [`Lease::fence`], which runs the write only if the epoch is the
//! live grant's epoch, as one atomic step with that check. So a write is
//! refused as soon as its grant stops being the live one, whatever the
//! reason, and nothing depends on the new leader doing something first:
//!
//! - after takeover, before the new leader has written anything;
//! - after expiry, even if nobody has taken over yet;
//! - after the writer's own release.
//!
//! A sink that only remembered the highest epoch it had accepted would let
//! all three through until the next leader's first write. The tests keep
//! that design as a mutant and show it fails.
//!
//! [`Fenced`] also keeps the highest epoch it has accepted and refuses
//! anything lower, which only matters if the lease itself goes wrong, for
//! example a lease service restarted without its epoch (see [`Lease`]).
//!
//! In production the lease and the check live in one store, as a
//! conditional write: "commit this only if the lease item's epoch is E and
//! its expiry is in the future, by the store's clock".
//!
//! Clocks. The lease decides expiry by its own clock, the store's. A holder
//! reads its own clock, which may differ from the store's by up to some
//! bound S. [`Grant::is_held_at`] takes that bound: a holder must treat its
//! grant as lost S milliseconds before the expiry it was given. With that
//! margin no two nodes ever both believe they lead. The fence does not
//! depend on any node's clock, so a node whose clock is wrong by more than S
//! may believe it leads, but its writes are still refused.
//!
//! [`MemoryLease`] keeps the state in a mutex and reads time from an
//! injected [`Clock`], so tests can stop and move time.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

pub type NodeId = u32;

/// Fencing token. Higher is newer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Epoch(pub u64);

/// A lease held by one node until `expires_at_millis`, by the store's clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Grant {
    pub holder: NodeId,
    pub epoch: Epoch,
    pub expires_at_millis: u64,
}

impl Grant {
    /// Whether the grant is live at `now_millis` on the store's clock.
    pub fn is_live_at(&self, now_millis: u64) -> bool {
        now_millis < self.expires_at_millis
    }

    /// Whether the holder may still act as leader at `local_now_millis` on
    /// its own clock, when that clock is within `max_skew_millis` of the
    /// store's. True only while the grant is live on the store's clock too.
    pub fn is_held_at(&self, local_now_millis: u64, max_skew_millis: u64) -> bool {
        local_now_millis.saturating_add(max_skew_millis) < self.expires_at_millis
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseError {
    /// Another node holds a live lease.
    Held(Grant),
    /// The grant is no longer the live one (expired, released or taken).
    Lost { current: Option<Grant> },
}

impl std::fmt::Display for LeaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LeaseError::Held(g) => {
                write!(f, "lease held by node {} at epoch {}", g.holder, g.epoch.0)
            }
            LeaseError::Lost { current } => match current {
                Some(g) => write!(f, "lease lost; node {} holds epoch {}", g.holder, g.epoch.0),
                None => write!(f, "lease lost; nobody holds it"),
            },
        }
    }
}

impl std::error::Error for LeaseError {}

/// Why a fenced write was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FenceError {
    /// The epoch is not the live grant's: it expired, was released or was
    /// taken over. `live` is the live grant's epoch, if any.
    NotLive { offered: Epoch, live: Option<Epoch> },
    /// The epoch is below one this sink already accepted. With a correct
    /// lease that cannot happen; it means the lease lost its epoch.
    BelowHighest { offered: Epoch, highest: Epoch },
}

impl std::fmt::Display for FenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FenceError::NotLive { offered, live } => match live {
                Some(l) => write!(f, "epoch {} is not live (epoch {} is)", offered.0, l.0),
                None => write!(
                    f,
                    "epoch {} is not live (nobody holds the lease)",
                    offered.0
                ),
            },
            FenceError::BelowHighest { offered, highest } => write!(
                f,
                "epoch {} is below {}, which this sink already accepted",
                offered.0, highest.0
            ),
        }
    }
}

impl std::error::Error for FenceError {}

/// A leader lease.
///
/// Contract:
///
/// - Epochs strictly increase across tenures, for as long as any sink they
///   fence exists. That includes restarts of the lease service: a real
///   implementation must persist the last epoch it handed out, in the same
///   store and the same atomic write as the grant, before returning it.
///   [`MemoryLease`] does not, which is its main limit (see there).
/// - Expiry is decided by one clock, the store's.
/// - [`Lease::fence`] runs the write only while the epoch is the live
///   grant's, atomically with that check.
pub trait Lease {
    /// Take the lease if nobody holds a live one. Asking again while
    /// holding it returns the current grant unchanged.
    fn acquire(&self, node: NodeId) -> Result<Grant, LeaseError>;
    /// Extend a live grant. Fails once it has expired, even if nobody took
    /// over: the holder must acquire again and gets a new epoch.
    fn renew(&self, grant: &Grant) -> Result<Grant, LeaseError>;
    /// Give the lease up early. Writes under the released grant are refused
    /// from this moment.
    fn release(&self, grant: &Grant) -> Result<(), LeaseError>;
    /// The live grant, if any.
    fn current(&self) -> Option<Grant>;
    /// Call `write` exactly once if `epoch` is the live grant's epoch, as one
    /// atomic step with that check, and return `Ok`. Otherwise do not call it
    /// and return why.
    fn fence(&self, epoch: Epoch, write: &mut dyn FnMut()) -> Result<(), FenceError>;
}

impl<L: Lease + ?Sized> Lease for &L {
    fn acquire(&self, node: NodeId) -> Result<Grant, LeaseError> {
        (**self).acquire(node)
    }
    fn renew(&self, grant: &Grant) -> Result<Grant, LeaseError> {
        (**self).renew(grant)
    }
    fn release(&self, grant: &Grant) -> Result<(), LeaseError> {
        (**self).release(grant)
    }
    fn current(&self) -> Option<Grant> {
        (**self).current()
    }
    fn fence(&self, epoch: Epoch, write: &mut dyn FnMut()) -> Result<(), FenceError> {
        (**self).fence(epoch, write)
    }
}

impl<L: Lease + ?Sized> Lease for Arc<L> {
    fn acquire(&self, node: NodeId) -> Result<Grant, LeaseError> {
        (**self).acquire(node)
    }
    fn renew(&self, grant: &Grant) -> Result<Grant, LeaseError> {
        (**self).renew(grant)
    }
    fn release(&self, grant: &Grant) -> Result<(), LeaseError> {
        (**self).release(grant)
    }
    fn current(&self) -> Option<Grant> {
        (**self).current()
    }
    fn fence(&self, epoch: Epoch, write: &mut dyn FnMut()) -> Result<(), FenceError> {
        (**self).fence(epoch, write)
    }
}

/// Milliseconds on some monotonic-enough timeline.
pub trait Clock {
    fn now_millis(&self) -> u64;
}

impl<C: Clock + ?Sized> Clock for Arc<C> {
    fn now_millis(&self) -> u64 {
        (**self).now_millis()
    }
}

impl<C: Clock + ?Sized> Clock for &C {
    fn now_millis(&self) -> u64 {
        (**self).now_millis()
    }
}

/// A clock that moves only when told to.
#[derive(Debug, Default)]
pub struct ManualClock(AtomicU64);

impl ManualClock {
    pub fn new(start_millis: u64) -> Self {
        ManualClock(AtomicU64::new(start_millis))
    }

    pub fn advance(&self, millis: u64) {
        self.0.fetch_add(millis, Ordering::SeqCst);
    }

    pub fn set(&self, millis: u64) {
        self.0.store(millis, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_millis(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// Wall-clock time.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_millis(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

/// The fencing rule on its own: accept `offered` only if it is the live
/// grant's epoch.
pub fn check_epoch(live: Option<Epoch>, offered: Epoch) -> Result<(), FenceError> {
    if live == Some(offered) {
        Ok(())
    } else {
        Err(FenceError::NotLive { offered, live })
    }
}

#[derive(Debug, Default)]
struct State {
    epoch: u64,
    holder: Option<(NodeId, u64)>,
}

impl State {
    fn live(&self, now: u64) -> Option<Grant> {
        let (holder, exp) = self.holder?;
        (now < exp).then_some(self.grant(holder, exp))
    }

    fn grant(&self, holder: NodeId, exp: u64) -> Grant {
        Grant {
            holder,
            epoch: Epoch(self.epoch),
            expires_at_millis: exp,
        }
    }
}

/// Deliberately broken variants, used only by the tests to show the
/// properties can fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) enum LeaseMutant {
    None,
    /// A takeover keeps the previous tenure's epoch.
    ReuseEpoch,
    /// Renewing an expired grant works if nobody took over in between.
    RenewAfterExpiry,
}

/// An in-process lease, for tests and single-host runs.
///
/// Limit: the epoch lives only in memory. A fresh `MemoryLease` starts again
/// at epoch 1, so after a restart a sink that remembers a higher epoch
/// refuses every new leader ([`FenceError::BelowHighest`]), which is the
/// safe failure. To carry on, save [`MemoryLease::last_epoch`] somewhere
/// durable and start the next one with [`MemoryLease::resume`]. A real
/// lease does that itself, as the [`Lease`] contract requires.
#[derive(Debug)]
pub struct MemoryLease<C: Clock> {
    clock: C,
    ttl_millis: u64,
    state: Mutex<State>,
    mutant: LeaseMutant,
}

impl<C: Clock> MemoryLease<C> {
    pub fn new(clock: C, ttl_millis: u64) -> Self {
        Self::resume(clock, ttl_millis, Epoch(0))
    }

    /// Start after `last`, the highest epoch an earlier lease handed out.
    /// The next tenure gets `last + 1`. Nobody holds the lease at start.
    pub fn resume(clock: C, ttl_millis: u64, last: Epoch) -> Self {
        MemoryLease {
            clock,
            ttl_millis,
            state: Mutex::new(State {
                epoch: last.0,
                holder: None,
            }),
            mutant: LeaseMutant::None,
        }
    }

    pub fn ttl_millis(&self) -> u64 {
        self.ttl_millis
    }

    /// The highest epoch handed out so far. This is what must survive a
    /// restart.
    pub fn last_epoch(&self) -> Epoch {
        Epoch(self.lock().epoch)
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // The state is two plain fields; a panic elsewhere cannot leave it
        // half written.
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn next_epoch(&self, current: u64) -> u64 {
        match self.mutant {
            LeaseMutant::ReuseEpoch if current > 0 => current,
            _ => current + 1,
        }
    }
}

impl<C: Clock> Lease for MemoryLease<C> {
    fn acquire(&self, node: NodeId) -> Result<Grant, LeaseError> {
        let now = self.clock.now_millis();
        let mut s = self.lock();
        if let Some(g) = s.live(now) {
            return if g.holder == node {
                Ok(g)
            } else {
                Err(LeaseError::Held(g))
            };
        }
        s.epoch = self.next_epoch(s.epoch);
        let exp = now.saturating_add(self.ttl_millis);
        s.holder = Some((node, exp));
        Ok(s.grant(node, exp))
    }

    fn renew(&self, grant: &Grant) -> Result<Grant, LeaseError> {
        let now = self.clock.now_millis();
        let mut s = self.lock();
        let cur = match self.mutant {
            LeaseMutant::RenewAfterExpiry => s.holder.map(|(h, exp)| s.grant(h, exp)),
            _ => s.live(now),
        };
        match cur {
            Some(cur) if cur.holder == grant.holder && cur.epoch == grant.epoch => {
                let exp = now.saturating_add(self.ttl_millis);
                s.holder = Some((cur.holder, exp));
                Ok(Grant {
                    expires_at_millis: exp,
                    ..cur
                })
            }
            _ => Err(LeaseError::Lost {
                current: s.live(now),
            }),
        }
    }

    fn release(&self, grant: &Grant) -> Result<(), LeaseError> {
        let now = self.clock.now_millis();
        let mut s = self.lock();
        match s.live(now) {
            Some(cur) if cur.holder == grant.holder && cur.epoch == grant.epoch => {
                s.holder = None;
                Ok(())
            }
            current => Err(LeaseError::Lost { current }),
        }
    }

    fn current(&self) -> Option<Grant> {
        let now = self.clock.now_millis();
        self.lock().live(now)
    }

    fn fence(&self, epoch: Epoch, write: &mut dyn FnMut()) -> Result<(), FenceError> {
        // Held across the write, so the lease cannot change in between.
        let s = self.lock();
        // Read the clock after locking, so a write that waited for the lock
        // is checked against the time it actually runs at.
        let now = self.clock.now_millis();
        check_epoch(s.live(now).map(|g| g.epoch), epoch)?;
        write();
        Ok(())
    }
}

/// A sink guarded by a lease. A write is applied only if its epoch is the
/// live grant's epoch at that moment, and never if it is below one this
/// sink already accepted. Refused writes are counted.
#[derive(Debug, Clone)]
pub struct Fenced<T, L> {
    inner: T,
    lease: L,
    highest: Epoch,
    rejected: u64,
    /// Test-only broken variant: trust any epoch not below the highest
    /// seen, without asking the lease (the design this replaced).
    #[cfg(test)]
    relaxed: bool,
}

impl<T, L: Lease> Fenced<T, L> {
    pub fn new(inner: T, lease: L) -> Self {
        Self::with_highest(inner, lease, Epoch(0))
    }

    /// A sink that already accepted writes up to `highest`, for example
    /// one reopened from storage.
    pub fn with_highest(inner: T, lease: L, highest: Epoch) -> Self {
        Fenced {
            inner,
            lease,
            highest,
            rejected: 0,
            #[cfg(test)]
            relaxed: false,
        }
    }

    /// Apply `f` if `epoch` is the live grant's epoch, atomically with the
    /// check.
    pub fn write<R>(&mut self, epoch: Epoch, f: impl FnOnce(&mut T) -> R) -> Result<R, FenceError> {
        if epoch < self.highest {
            self.rejected += 1;
            return Err(FenceError::BelowHighest {
                offered: epoch,
                highest: self.highest,
            });
        }
        #[cfg(test)]
        if self.relaxed {
            self.highest = epoch;
            return Ok(f(&mut self.inner));
        }
        let mut f = Some(f);
        let mut out = None;
        let inner = &mut self.inner;
        let r = self.lease.fence(epoch, &mut || {
            if let Some(f) = f.take() {
                out = Some(f(inner));
            }
        });
        match (r, out) {
            (Ok(()), Some(v)) => {
                self.highest = epoch;
                Ok(v)
            }
            (Err(e), _) => {
                self.rejected += 1;
                Err(e)
            }
            // A lease that said yes without running the write broke its
            // contract. Nothing was written, so report a refusal.
            (Ok(()), None) => {
                self.rejected += 1;
                Err(FenceError::NotLive {
                    offered: epoch,
                    live: None,
                })
            }
        }
    }

    /// Point the sink at another lease, keeping the highest epoch it has
    /// accepted. Models a lease service restart under a sink that stays.
    pub fn rebind<L2: Lease>(self, lease: L2) -> Fenced<T, L2> {
        Fenced {
            inner: self.inner,
            lease,
            highest: self.highest,
            rejected: self.rejected,
            #[cfg(test)]
            relaxed: self.relaxed,
        }
    }

    pub fn highest(&self) -> Epoch {
        self.highest
    }

    /// Writes refused so far.
    pub fn rejected(&self) -> u64 {
        self.rejected
    }

    pub fn get(&self) -> &T {
        &self.inner
    }

    pub fn into_inner(self) -> T {
        self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use proptest::test_runner::{Config, RngAlgorithm, TestCaseError, TestRng, TestRunner};

    const TTL: u64 = 100;
    /// The most any node's clock may differ from the store's in the tests.
    const MAX_SKEW: u64 = 20;

    fn setup() -> (Arc<ManualClock>, MemoryLease<Arc<ManualClock>>) {
        let clock = Arc::new(ManualClock::new(1_000));
        let lease = MemoryLease::new(clock.clone(), TTL);
        (clock, lease)
    }

    #[test]
    fn one_holder_at_a_time() {
        let (_, lease) = setup();
        let a = lease.acquire(1).unwrap();
        assert_eq!(a.epoch, Epoch(1));
        assert!(matches!(lease.acquire(2), Err(LeaseError::Held(g)) if g.holder == 1));
        // Asking again is idempotent.
        assert_eq!(lease.acquire(1).unwrap(), a);
        assert_eq!(lease.current(), Some(a));
    }

    #[test]
    fn renewal_extends_and_keeps_the_epoch() {
        let (clock, lease) = setup();
        let a = lease.acquire(1).unwrap();
        clock.advance(TTL - 1);
        let a2 = lease.renew(&a).unwrap();
        assert_eq!(a2.epoch, a.epoch);
        assert_eq!(a2.expires_at_millis, 1_000 + TTL - 1 + TTL);
        clock.advance(TTL - 1);
        assert!(
            lease.current().is_some(),
            "renewed lease must still be live"
        );
    }

    #[test]
    fn expiry_lets_another_node_take_over_with_a_higher_epoch() {
        let (clock, lease) = setup();
        let a = lease.acquire(1).unwrap();
        clock.advance(TTL);
        assert_eq!(lease.current(), None, "expires exactly at the deadline");
        let b = lease.acquire(2).unwrap();
        assert!(b.epoch > a.epoch);
        assert!(matches!(lease.renew(&a), Err(LeaseError::Lost { current: Some(g) }) if g == b));
    }

    #[test]
    fn an_expired_grant_cannot_be_renewed_even_if_nobody_took_over() {
        let (clock, lease) = setup();
        let a = lease.acquire(1).unwrap();
        clock.advance(TTL + 5);
        assert_eq!(lease.renew(&a), Err(LeaseError::Lost { current: None }));
        let again = lease.acquire(1).unwrap();
        assert!(again.epoch > a.epoch, "a new tenure gets a new epoch");
    }

    #[test]
    fn release_hands_over_at_once() {
        let (_, lease) = setup();
        let a = lease.acquire(1).unwrap();
        lease.release(&a).unwrap();
        assert_eq!(lease.current(), None);
        let b = lease.acquire(2).unwrap();
        assert_eq!(b.epoch, Epoch(a.epoch.0 + 1));
        assert!(lease.release(&a).is_err(), "an old grant cannot release");
    }

    /// Node 1 leads and writes, then stalls past its expiry. Node 2 takes
    /// over but has not written yet. Node 1 wakes and writes. Returns true
    /// if that zombie write was refused and the history is clean.
    fn zombie_in_the_gap_is_refused(relaxed: bool) -> bool {
        let (clock, lease) = setup();
        let mut sink = Fenced::new(Vec::new(), &lease);
        sink.relaxed = relaxed;
        let a = lease.acquire(1).unwrap();
        sink.write(a.epoch, |v| v.push("a1")).unwrap();
        clock.advance(TTL + 1);
        let b = lease.acquire(2).unwrap();
        // Node 1 still believes it leads. Node 2 has done nothing yet.
        let zombie = sink.write(a.epoch, |v| v.push("zombie"));
        let b1 = sink.write(b.epoch, |v| v.push("b1"));
        b1.is_ok()
            && zombie
                == Err(FenceError::NotLive {
                    offered: a.epoch,
                    live: Some(b.epoch),
                })
            && sink.get() == &["a1", "b1"]
            && sink.rejected() == 1
    }

    #[test]
    fn a_zombie_write_between_takeover_and_the_new_leaders_first_write_is_refused() {
        assert!(zombie_in_the_gap_is_refused(false));
    }

    #[test]
    fn the_gap_test_catches_a_sink_that_only_remembers_the_highest_epoch() {
        assert!(!zombie_in_the_gap_is_refused(true));
    }

    #[test]
    fn a_zombie_write_after_expiry_is_refused_before_anyone_takes_over() {
        let (clock, lease) = setup();
        let mut sink = Fenced::new(Vec::new(), &lease);
        let a = lease.acquire(1).unwrap();
        sink.write(a.epoch, |v| v.push(1)).unwrap();
        clock.advance(TTL);
        assert_eq!(
            sink.write(a.epoch, |v| v.push(2)),
            Err(FenceError::NotLive {
                offered: a.epoch,
                live: None
            })
        );
        assert_eq!(sink.get(), &[1]);
    }

    #[test]
    fn a_released_grant_cannot_write() {
        let (_, lease) = setup();
        let mut sink = Fenced::new(Vec::new(), &lease);
        let a = lease.acquire(1).unwrap();
        sink.write(a.epoch, |v| v.push(1)).unwrap();
        lease.release(&a).unwrap();
        // Nobody else has acquired. The release alone stops the writes.
        assert!(sink.write(a.epoch, |v| v.push(2)).is_err());
        let b = lease.acquire(2).unwrap();
        assert!(sink.write(a.epoch, |v| v.push(3)).is_err());
        sink.write(b.epoch, |v| v.push(4)).unwrap();
        assert_eq!(sink.get(), &[1, 4]);
        assert_eq!(sink.rejected(), 2);
    }

    #[test]
    fn a_lease_restarted_without_its_epoch_is_refused_and_a_resumed_one_works() {
        let (clock, lease) = setup();
        let mut sink = Fenced::new(Vec::new(), &lease);
        for n in 1..=3 {
            let g = lease.acquire(n).unwrap();
            sink.write(g.epoch, |v| v.push(n)).unwrap();
            lease.release(&g).unwrap();
        }
        assert_eq!(sink.highest(), Epoch(3));
        let saved = lease.last_epoch();
        assert_eq!(saved, Epoch(3));

        // Restart from nothing: epochs begin at 1 again, and the sink that
        // remembers 3 refuses the new leader. Safe, but stuck.
        let fresh = MemoryLease::new(clock.clone(), TTL);
        let mut sink = sink.rebind(&fresh);
        let g = fresh.acquire(4).unwrap();
        assert_eq!(g.epoch, Epoch(1));
        assert_eq!(
            sink.write(g.epoch, |v| v.push(4)),
            Err(FenceError::BelowHighest {
                offered: Epoch(1),
                highest: Epoch(3)
            })
        );

        // Restart from the saved epoch: the next leader is above the sink.
        let resumed = MemoryLease::resume(clock.clone(), TTL, saved);
        let mut sink = sink.rebind(&resumed);
        let g = resumed.acquire(4).unwrap();
        assert_eq!(g.epoch, Epoch(4));
        sink.write(g.epoch, |v| v.push(4)).unwrap();
        assert_eq!(sink.get(), &[1, 2, 3, 4]);
    }

    #[test]
    fn check_epoch_accepts_only_the_live_epoch() {
        assert!(check_epoch(Some(Epoch(3)), Epoch(3)).is_ok());
        assert!(check_epoch(Some(Epoch(3)), Epoch(4)).is_err());
        assert_eq!(
            check_epoch(Some(Epoch(3)), Epoch(2)),
            Err(FenceError::NotLive {
                offered: Epoch(2),
                live: Some(Epoch(3))
            })
        );
        assert!(check_epoch(None, Epoch(3)).is_err());
    }

    #[test]
    fn a_holder_gives_up_its_grant_one_skew_bound_early() {
        let g = Grant {
            holder: 1,
            epoch: Epoch(1),
            expires_at_millis: 1_000,
        };
        assert!(g.is_held_at(979, 20));
        assert!(!g.is_held_at(980, 20));
        assert!(g.is_held_at(999, 0));
        assert!(!g.is_held_at(u64::MAX, 20));
    }

    #[derive(Debug, Clone, Copy)]
    enum Op {
        Acquire(NodeId),
        Renew(NodeId),
        Release(NodeId),
        Write(NodeId),
        Wait(u64),
    }

    /// Three nodes, each with its own clock offset from the store's within
    /// `MAX_SKEW`, and a random list of operations.
    #[derive(Debug, Clone)]
    struct Schedule {
        skew: [i64; 3],
        ops: Vec<Op>,
    }

    fn schedules() -> impl Strategy<Value = Schedule> {
        let s = MAX_SKEW as i64;
        let op = prop_oneof![
            (0u32..3).prop_map(Op::Acquire),
            (0u32..3).prop_map(Op::Renew),
            (0u32..3).prop_map(Op::Release),
            (0u32..3).prop_map(Op::Write),
            (0u64..150).prop_map(Op::Wait),
        ];
        (
            prop::array::uniform3(-s..=s),
            prop::collection::vec(op, 1..80),
        )
            .prop_map(|(skew, ops)| Schedule { skew, ops })
    }

    /// What to break.
    #[derive(Debug, Clone, Copy)]
    struct Variant {
        lease: LeaseMutant,
        relaxed_sink: bool,
        /// The skew margin nodes use when deciding whether they lead.
        margin: u64,
    }

    const REAL: Variant = Variant {
        lease: LeaseMutant::None,
        relaxed_sink: false,
        margin: MAX_SKEW,
    };

    /// The lease contract, written out separately from `MemoryLease`.
    #[derive(Debug, Default)]
    struct Model {
        epoch: u64,
        holder: Option<(NodeId, u64)>,
    }

    impl Model {
        fn live(&self, now: u64) -> Option<Grant> {
            let (holder, exp) = self.holder?;
            (now < exp).then_some(Grant {
                holder,
                epoch: Epoch(self.epoch),
                expires_at_millis: exp,
            })
        }
    }

    /// Run a schedule against the lease, a fenced sink and the model. Each
    /// node remembers the newest grant it ever got and keeps writing with
    /// it after expiry, after takeover and after its own release, as a
    /// zombie would. Checks, after every step:
    ///
    /// - every acquire, renew and release returns what the contract says;
    /// - a new tenure's epoch is above every earlier one;
    /// - a write is accepted exactly when its epoch is the live one;
    /// - a node that believes it leads (by its own skewed clock, with the
    ///   margin) is never refused;
    /// - at most one node believes it leads.
    fn check_schedule(s: &Schedule, v: Variant) -> Result<(), TestCaseError> {
        let clock = Arc::new(ManualClock::new(1_000));
        let mut lease = MemoryLease::new(clock.clone(), TTL);
        lease.mutant = v.lease;
        let mut sink = Fenced::new(Vec::new(), &lease);
        sink.relaxed = v.relaxed_sink;
        let mut model = Model::default();
        let mut last: [Option<Grant>; 3] = [None; 3];
        let mut believes = [false; 3];
        let mut top_epoch = 0;
        let local = |n: usize| (clock.now_millis() as i64 + s.skew[n]) as u64;
        for &op in &s.ops {
            let now = clock.now_millis();
            match op {
                Op::Acquire(n) => {
                    let want = match model.live(now) {
                        Some(g) if g.holder == n => Ok(g),
                        Some(g) => Err(LeaseError::Held(g)),
                        None => {
                            model.epoch += 1;
                            model.holder = Some((n, now + TTL));
                            Ok(Grant {
                                holder: n,
                                epoch: Epoch(model.epoch),
                                expires_at_millis: now + TTL,
                            })
                        }
                    };
                    let got = lease.acquire(n);
                    prop_assert_eq!(got, want);
                    if let Ok(g) = got {
                        if last[n as usize] != Some(g) {
                            prop_assert!(g.epoch.0 > top_epoch, "epoch did not increase");
                            top_epoch = g.epoch.0;
                        }
                        last[n as usize] = Some(g);
                        believes[n as usize] = true;
                    }
                }
                Op::Renew(n) => {
                    if let Some(g) = last[n as usize] {
                        let want = match model.live(now) {
                            Some(cur) if cur.holder == g.holder && cur.epoch == g.epoch => {
                                model.holder = Some((n, now + TTL));
                                Ok(Grant {
                                    expires_at_millis: now + TTL,
                                    ..cur
                                })
                            }
                            current => Err(LeaseError::Lost { current }),
                        };
                        let got = lease.renew(&g);
                        prop_assert_eq!(got, want);
                        if let Ok(g2) = got {
                            last[n as usize] = Some(g2);
                        }
                    }
                }
                Op::Release(n) => {
                    if let Some(g) = last[n as usize] {
                        let want = match model.live(now) {
                            Some(cur) if cur.holder == g.holder && cur.epoch == g.epoch => {
                                model.holder = None;
                                Ok(())
                            }
                            current => Err(LeaseError::Lost { current }),
                        };
                        let got = lease.release(&g);
                        prop_assert_eq!(got, want);
                        if got.is_ok() {
                            // It stops believing, but keeps the grant: a
                            // buggy node may still write with it.
                            believes[n as usize] = false;
                        }
                    }
                }
                Op::Write(n) => {
                    if let Some(g) = last[n as usize] {
                        let i = n as usize;
                        let leads = believes[i] && g.is_held_at(local(i), v.margin);
                        let live = model.live(now).is_some_and(|cur| cur.epoch == g.epoch);
                        let ok = sink.write(g.epoch, |w| w.push(n)).is_ok();
                        prop_assert_eq!(
                            ok,
                            live,
                            "node {} epoch {}: accepted {}",
                            n,
                            g.epoch.0,
                            ok
                        );
                        if leads {
                            prop_assert!(ok, "node {} believes it leads and was refused", n);
                        }
                    }
                }
                Op::Wait(ms) => clock.advance(ms),
            }
            let leaders = (0..3)
                .filter(|&i| {
                    believes[i] && last[i].is_some_and(|g| g.is_held_at(local(i), v.margin))
                })
                .count();
            prop_assert!(leaders <= 1, "{} nodes believe they lead", leaders);
        }
        Ok(())
    }

    proptest! {
        #![proptest_config(Config { cases: 512, ..Config::default() })]

        #[test]
        fn lease_schedules_keep_the_contract_and_fence_every_zombie(s in schedules()) {
            check_schedule(&s, REAL)?;
        }
    }

    fn caught(v: Variant) -> bool {
        let cfg = Config {
            cases: 512,
            failure_persistence: None,
            ..Config::default()
        };
        let rng = TestRng::deterministic_rng(RngAlgorithm::ChaCha);
        TestRunner::new_with_rng(cfg, rng)
            .run(&schedules(), |s| check_schedule(&s, v))
            .is_err()
    }

    #[test]
    fn the_schedule_property_catches_broken_leases_and_sinks() {
        // Control: the real lease and sink pass on the same seed.
        assert!(!caught(REAL));
        assert!(caught(Variant {
            lease: LeaseMutant::ReuseEpoch,
            ..REAL
        }));
        assert!(caught(Variant {
            lease: LeaseMutant::RenewAfterExpiry,
            ..REAL
        }));
        // A sink that only remembers the highest epoch accepts zombies.
        assert!(caught(Variant {
            relaxed_sink: true,
            ..REAL
        }));
        // Nodes that ignore clock skew can both believe they lead.
        assert!(caught(Variant { margin: 0, ..REAL }));
    }
}
