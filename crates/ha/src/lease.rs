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
//! the grant it was made under, and the sink refuses any epoch lower than
//! the highest it has accepted. [`Fenced`] is that check. A new leader calls
//! [`Fenced::advance`] with its epoch before its first real write, so a
//! zombie is refused even if the new leader has not written yet.
//!
//! The check has to live in the sink (in production a conditional write on
//! the store), not in the writer, because the writer is the party that may
//! be wrong about its own status.
//!
//! [`MemoryLease`] keeps the state in a mutex and reads time from an
//! injected [`Clock`], so tests can stop and move time. All nodes share one
//! clock here. A real deployment has per-host clocks and must leave a safety
//! margin between a holder's own view of its expiry and the store's.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

pub type NodeId = u32;

/// Fencing token. Higher is newer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Epoch(pub u64);

/// A lease held by one node until `expires_at_millis`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Grant {
    pub holder: NodeId,
    pub epoch: Epoch,
    pub expires_at_millis: u64,
}

impl Grant {
    pub fn is_live_at(&self, now_millis: u64) -> bool {
        now_millis < self.expires_at_millis
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

/// A leader lease. Implementations must hand out strictly increasing epochs
/// across tenures.
pub trait Lease {
    /// Take the lease if nobody holds a live one. Asking again while
    /// holding it returns the current grant unchanged.
    fn acquire(&self, node: NodeId) -> Result<Grant, LeaseError>;
    /// Extend a live grant. Fails once it has expired, even if nobody took
    /// over: the holder must acquire again and gets a new epoch.
    fn renew(&self, grant: &Grant) -> Result<Grant, LeaseError>;
    /// Give the lease up early.
    fn release(&self, grant: &Grant) -> Result<(), LeaseError>;
    /// The live grant, if any.
    fn current(&self) -> Option<Grant>;
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

#[derive(Debug, Default)]
struct State {
    epoch: u64,
    holder: Option<(NodeId, u64)>,
}

impl State {
    fn live(&self, now: u64) -> Option<Grant> {
        let (holder, exp) = self.holder?;
        (now < exp).then_some(Grant {
            holder,
            epoch: Epoch(self.epoch),
            expires_at_millis: exp,
        })
    }
}

/// An in-process lease, for tests and single-host runs.
#[derive(Debug)]
pub struct MemoryLease<C: Clock> {
    clock: C,
    ttl_millis: u64,
    state: Mutex<State>,
    /// Test-only broken variant: keep the old epoch on takeover.
    #[cfg(test)]
    reuse_epoch: bool,
}

impl<C: Clock> MemoryLease<C> {
    pub fn new(clock: C, ttl_millis: u64) -> Self {
        MemoryLease {
            clock,
            ttl_millis,
            state: Mutex::new(State::default()),
            #[cfg(test)]
            reuse_epoch: false,
        }
    }

    pub fn ttl_millis(&self) -> u64 {
        self.ttl_millis
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // The state is two plain fields; a panic elsewhere cannot leave it
        // half written.
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[cfg(not(test))]
    fn next_epoch(&self, current: u64) -> u64 {
        current + 1
    }

    #[cfg(test)]
    fn next_epoch(&self, current: u64) -> u64 {
        if self.reuse_epoch {
            current
        } else {
            current + 1
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
        Ok(Grant {
            holder: node,
            epoch: Epoch(s.epoch),
            expires_at_millis: exp,
        })
    }

    fn renew(&self, grant: &Grant) -> Result<Grant, LeaseError> {
        let now = self.clock.now_millis();
        let mut s = self.lock();
        match s.live(now) {
            Some(cur) if cur.holder == grant.holder && cur.epoch == grant.epoch => {
                let exp = now.saturating_add(self.ttl_millis);
                s.holder = Some((cur.holder, exp));
                Ok(Grant {
                    expires_at_millis: exp,
                    ..cur
                })
            }
            current => Err(LeaseError::Lost { current }),
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
}

/// A write carried an epoch older than one the sink already accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StaleEpoch {
    pub offered: Epoch,
    pub highest: Epoch,
}

impl std::fmt::Display for StaleEpoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "stale epoch {} (sink has accepted {})",
            self.offered.0, self.highest.0
        )
    }
}

impl std::error::Error for StaleEpoch {}

/// The fencing rule on its own: accept `offered` only if it is not older
/// than `highest`.
pub fn check_epoch(highest: Epoch, offered: Epoch) -> Result<(), StaleEpoch> {
    if offered < highest {
        Err(StaleEpoch { offered, highest })
    } else {
        Ok(())
    }
}

/// A sink guarded by a fencing epoch. Writes under an older epoch than the
/// highest seen are refused and counted.
#[derive(Debug, Clone, Default)]
pub struct Fenced<T> {
    inner: T,
    highest: Epoch,
    rejected: u64,
}

impl<T> Fenced<T> {
    pub fn new(inner: T) -> Self {
        Fenced {
            inner,
            highest: Epoch(0),
            rejected: 0,
        }
    }

    /// Record a new leader's epoch without writing, so older epochs are
    /// refused from now on.
    pub fn advance(&mut self, epoch: Epoch) -> Result<(), StaleEpoch> {
        self.admit(epoch)
    }

    /// Apply `f` if `epoch` is current.
    pub fn write<R>(&mut self, epoch: Epoch, f: impl FnOnce(&mut T) -> R) -> Result<R, StaleEpoch> {
        self.admit(epoch)?;
        Ok(f(&mut self.inner))
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

    fn admit(&mut self, epoch: Epoch) -> Result<(), StaleEpoch> {
        match check_epoch(self.highest, epoch) {
            Ok(()) => {
                self.highest = epoch;
                Ok(())
            }
            Err(e) => {
                self.rejected += 1;
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use proptest::test_runner::{Config, RngAlgorithm, TestCaseError, TestRng, TestRunner};

    const TTL: u64 = 100;

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
    /// over and fences the sink. Node 1 wakes and tries to write. Returns
    /// true if that zombie write was refused.
    fn zombie_is_refused(lease: &MemoryLease<Arc<ManualClock>>, clock: &ManualClock) -> bool {
        let mut sink: Fenced<Vec<&str>> = Fenced::new(Vec::new());
        let a = lease.acquire(1).unwrap();
        sink.write(a.epoch, |v| v.push("a1")).unwrap();
        clock.advance(TTL + 1);
        let b = lease.acquire(2).unwrap();
        sink.advance(b.epoch).unwrap();
        // Node 1 still believes it leads.
        let zombie = sink.write(a.epoch, |v| v.push("zombie"));
        sink.write(b.epoch, |v| v.push("b1")).unwrap();
        zombie.is_err() && sink.get() == &["a1", "b1"] && sink.rejected() == 1
    }

    #[test]
    fn a_zombie_leader_is_fenced_off() {
        let (clock, lease) = setup();
        assert!(zombie_is_refused(&lease, &clock));
    }

    #[test]
    fn the_zombie_test_catches_a_lease_that_reuses_epochs() {
        let (clock, mut lease) = setup();
        lease.reuse_epoch = true;
        assert!(!zombie_is_refused(&lease, &clock));
    }

    #[test]
    fn check_epoch_accepts_equal_and_newer() {
        assert!(check_epoch(Epoch(3), Epoch(3)).is_ok());
        assert!(check_epoch(Epoch(3), Epoch(4)).is_ok());
        assert_eq!(
            check_epoch(Epoch(3), Epoch(2)),
            Err(StaleEpoch {
                offered: Epoch(2),
                highest: Epoch(3)
            })
        );
    }

    #[derive(Debug, Clone, Copy)]
    enum Op {
        Acquire(NodeId),
        Renew(NodeId),
        Release(NodeId),
        Write(NodeId),
        Wait(u64),
    }

    fn ops() -> impl Strategy<Value = Vec<Op>> {
        let op = prop_oneof![
            (0u32..3).prop_map(Op::Acquire),
            (0u32..3).prop_map(Op::Renew),
            (0u32..3).prop_map(Op::Release),
            (0u32..3).prop_map(Op::Write),
            (0u64..150).prop_map(Op::Wait),
        ];
        prop::collection::vec(op, 1..80)
    }

    /// Random schedules of three nodes. Each node keeps the last grant it
    /// got and writes with it whether or not it is still valid, as a zombie
    /// would. Checks: at most one node believes it holds a live grant,
    /// tenures get strictly higher epochs, the sink only accepts writes from
    /// the latest tenure's holder, and that holder is never refused while
    /// its grant is live.
    fn check_schedule(ops: &[Op], reuse_epoch: bool) -> Result<(), TestCaseError> {
        let clock = Arc::new(ManualClock::new(0));
        let mut lease = MemoryLease::new(clock.clone(), TTL);
        lease.reuse_epoch = reuse_epoch;
        let mut held: [Option<Grant>; 3] = [None; 3];
        let mut sink: Fenced<Vec<NodeId>> = Fenced::new(Vec::new());
        let mut latest: Option<Grant> = None;
        for &op in ops {
            match op {
                Op::Acquire(n) => {
                    let new_tenure = lease.current().is_none_or(|c| c.holder != n);
                    if let Ok(g) = lease.acquire(n) {
                        if new_tenure {
                            if let Some(prev) = latest {
                                prop_assert!(g.epoch > prev.epoch, "epoch did not increase");
                            }
                            prop_assert!(sink.advance(g.epoch).is_ok());
                            latest = Some(g);
                        }
                        held[n as usize] = Some(g);
                    }
                }
                Op::Renew(n) => {
                    if let Some(g) = held[n as usize] {
                        if let Ok(g2) = lease.renew(&g) {
                            held[n as usize] = Some(g2);
                            latest = Some(g2);
                        }
                    }
                }
                Op::Release(n) => {
                    if let Some(g) = held[n as usize] {
                        if lease.release(&g).is_ok() {
                            held[n as usize] = None;
                        }
                    }
                }
                Op::Write(n) => {
                    if let Some(g) = held[n as usize] {
                        let ok = sink.write(g.epoch, |v| v.push(n)).is_ok();
                        let is_latest = latest.is_some_and(|l| l.holder == n && l.epoch == g.epoch);
                        if ok {
                            prop_assert!(is_latest, "node {} wrote outside its tenure", n);
                        }
                        if is_latest {
                            prop_assert!(ok, "the current leader was refused");
                        }
                    }
                }
                Op::Wait(ms) => clock.advance(ms),
            }
            let now = clock.now_millis();
            // Each node's own view: does it think it leads right now?
            let believers = held.iter().flatten().filter(|g| g.is_live_at(now)).count();
            prop_assert!(believers <= 1, "two nodes believe they lead");
        }
        Ok(())
    }

    proptest! {
        #![proptest_config(Config { cases: 512, ..Config::default() })]

        #[test]
        fn lease_schedules_keep_one_leader_and_fence_the_rest(ops in ops()) {
            check_schedule(&ops, false)?;
        }
    }

    #[test]
    fn the_schedule_property_catches_reused_epochs() {
        let run = |reuse: bool| {
            let cfg = Config {
                cases: 512,
                failure_persistence: None,
                ..Config::default()
            };
            let rng = TestRng::deterministic_rng(RngAlgorithm::ChaCha);
            TestRunner::new_with_rng(cfg, rng)
                .run(&ops(), |o| check_schedule(&o, reuse))
                .is_err()
        };
        assert!(!run(false));
        assert!(run(true));
    }
}
