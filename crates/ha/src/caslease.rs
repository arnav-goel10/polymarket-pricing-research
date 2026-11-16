//! The archive lease: one HEAD document whose every grant, renew and
//! commit is one conditional write.
//!
//! HEAD holds the epoch, the holder and the list of tenures, the slot
//! ranges each epoch committed to the archive. Because the grant and the
//! epoch are one write, the persisted epoch can never fall behind a grant,
//! and because every commit carries `If-Match` on the ETag the holder last
//! wrote, a commit from an old epoch fails once any later write has landed.
//! Readers trust only what HEAD references.
//!
//! [`CasLease`] is a sans-IO state machine. The caller asks [`CasLease::poll`]
//! for the next request, runs it against a [`HeadStore`] and hands the
//! result back. It never reads a clock: every call takes `now`, the node's
//! own monotonic milliseconds. No date from the store is parsed anywhere,
//! so the wall-clock offset between hosts has no effect.
//!
//! - Acquire: read HEAD. Take over only after the ETag stayed unchanged for
//!   `ttl + takeover margin` on my own clock, or at once if `holder` is
//!   null. The takeover writes `epoch + 1`, holder me, with `If-Match`. A
//!   missing HEAD is created at epoch 1 with `If-None-Match: *`, but only by
//!   a node that has not seen one: after that a missing HEAD is a fault and
//!   is never recreated. A restarted node is a plain contender.
//! - Renew every `renew_ms`: same epoch, new nonce, `If-Match`. The holder
//!   believes it leads until the time its last successful write was **sent**
//!   plus `ttl - holder margin`.
//! - Commit slot n: set the current tenure's `to = n`, or append a tenure
//!   from `previous to + 1` on the epoch's first commit. A commit is a
//!   renew.
//! - Deadline passed: no new commits; one renew. A 412 means fenced
//!   (counted), success means nobody took over. Expiry is advisory, the
//!   conditional write is the truth.
//! - Unknown outcome: read HEAD. My document (nonce, epoch, holder and
//!   tenures) means it landed, an ETag still equal to the one I matched on
//!   means retry the same write, anything else means fenced. Such a write
//!   stays unsettled until a later write of mine lands: while any is
//!   unsettled, a 412 reads HEAD first, and a HEAD equal to one of them is
//!   taken as that write landing late (also when a poll finds it), since a
//!   resend may have been refused at once and a fresh write sent since.
//! - Release: holder null, epoch kept.
//!
//! [`audit`] is the second, weaker model. It knows nothing about clocks or
//! the state machine and checks only the history of applied HEAD writes:
//! epochs never go back, so once a takeover to E' has landed no write from
//! an epoch below E' is ever accepted again; each epoch has one holder; and
//! the tenure list only grows at its end, apart from trimming its oldest
//! entries. [`read_archive`] is the reader rule for archive objects.
//!
//! This is not an implementation of [`crate::lease::Lease`]. No store can
//! evaluate "now", so a renew can succeed after expiry when nobody took
//! over, and a closure-style fence cannot be atomic with a remote write.

use crate::store::{Cond, Etag, HeadRead, HeadStore, WriteOutcome};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

pub const HEAD_VERSION: u32 = 1;

/// How many unsettled writes a node remembers. Each needs its own unknown
/// outcome and none of mine may have landed since, so a few is plenty.
const MAX_UNSETTLED: usize = 8;

/// The slot range one epoch committed. `n` is the holder node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tenure {
    pub e: u64,
    pub n: u32,
    pub from: i64,
    pub to: i64,
}

/// The HEAD document, `lease/HEAD.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Head {
    pub v: u32,
    pub epoch: u64,
    pub holder: Option<u32>,
    pub ttl_ms: u64,
    pub nonce: String,
    pub tenures: Vec<Tenure>,
}

impl Head {
    /// Compact JSON with no newline.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("head serializes")
    }

    pub fn decode(b: &[u8]) -> Option<Head> {
        serde_json::from_slice(b).ok()
    }

    /// The highest committed slot.
    pub fn committed_to(&self) -> Option<i64> {
        self.tenures.last().map(|t| t.to)
    }
}

/// Lease times, all in milliseconds on each node's own clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timing {
    pub ttl_ms: u64,
    pub renew_ms: u64,
    pub poll_ms: u64,
    /// The holder stops believing this long before `ttl` runs out.
    pub holder_margin_ms: u64,
    /// A contender waits this long past `ttl` before taking over.
    pub takeover_margin_ms: u64,
}

impl Default for Timing {
    /// ttl 30 s, renew 10 s, poll 5 s, holder margin 2 s, takeover margin
    /// 1 s. Expected takeover 31 to 41 s after the holder's last write.
    fn default() -> Self {
        Timing {
            ttl_ms: 30_000,
            renew_ms: 10_000,
            poll_ms: 5_000,
            holder_margin_ms: 2_000,
            takeover_margin_ms: 1_000,
        }
    }
}

/// Seeded lease faults. The property tests show each one is caught; a
/// build with the `mutants` feature can select one for `chaos local`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(not(any(test, feature = "mutants")), allow(dead_code))]
pub enum LeaseMutant {
    None,
    /// A takeover keeps the old epoch.
    ReuseEpoch,
    /// Renews and commits are sent with no condition.
    Unconditional,
    /// The holder's deadline counts from when the reply arrived, not from
    /// when the write was sent.
    DeadlineFromReceipt,
    /// A contender takes over as soon as it sees the same ETag twice.
    ShortWatch,
    /// An unknown outcome is treated as a failure and the write is sent
    /// again, matched on whatever ETag HEAD has now.
    ResendOnUnknown,
    /// A contender waits three ttl before taking over (a liveness fault).
    SlowTakeover,
    /// A contender waits two ttl before taking over (a liveness fault
    /// that the old 30 s settle hid).
    TwoTtlTakeover,
    /// A 412 is taken as a fence without reading HEAD, even on a resent
    /// write or while an earlier write is unsettled, so a late first
    /// attempt fences a healthy holder.
    TrustRetry412,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub node: u32,
    pub timing: Timing,
    /// Tenures whose `to` is more than this many slots below the slot
    /// being committed are dropped, so HEAD stays bounded.
    pub keep_slots: i64,
    /// Seeds the per-write nonces.
    pub nonce_seed: u64,
    mutant: LeaseMutant,
}

impl Config {
    /// Three days of one-minute slots are kept.
    pub fn new(node: u32, nonce_seed: u64) -> Self {
        Config {
            node,
            timing: Timing::default(),
            keep_slots: 3 * 24 * 60,
            nonce_seed,
            mutant: LeaseMutant::None,
        }
    }

    pub fn with_timing(mut self, t: Timing) -> Self {
        self.timing = t;
        self
    }

    #[cfg(any(test, feature = "mutants"))]
    pub fn with_mutant(mut self, m: LeaseMutant) -> Self {
        self.mutant = m;
        self
    }

    pub fn mutant(&self) -> LeaseMutant {
        self.mutant
    }
}

/// What the caller must do next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    Read,
    Write { body: Vec<u8>, cond: Cond },
}

/// Something worth logging.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseEvent {
    Granted { epoch: u64 },
    Committed { epoch: u64, slot: i64 },
    Fenced { epoch: u64 },
    Released { epoch: u64 },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseStats {
    pub grants: u64,
    pub renews: u64,
    pub commits: u64,
    /// Holder writes refused because a later write had landed:
    /// `poly_fence_rejected_total`.
    pub fence_rejected: u64,
    /// Takeovers or creates that lost to another contender.
    pub lost_races: u64,
    pub unknown_outcomes: u64,
    pub retries: u64,
    pub unreadable_heads: u64,
    /// Polls that found no HEAD after this node had seen one.
    pub missing_heads: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Poll,
    Create,
    Takeover,
    Renew,
    Commit(i64),
    Release,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Waiting for the reply.
    Sent,
    /// The outcome is unknown; read HEAD at this time.
    ResolveAt(u64),
    /// The resolving read is out.
    Resolving,
}

#[derive(Debug, Clone)]
struct Inflight {
    op: Op,
    doc: Option<Head>,
    cond: Cond,
    /// When this write (same nonce) was first sent. A landed write landed
    /// after this, whichever attempt carried it.
    first_sent: u64,
    /// An earlier attempt of this write had an unknown outcome, so it may
    /// still land, and a 412 may be that attempt's own ETag.
    retried: bool,
    phase: Phase,
}

#[derive(Debug, Clone)]
struct Queued {
    op: Op,
    doc: Head,
    cond: Cond,
    first_sent: Option<u64>,
    retried: bool,
    not_before: u64,
}

#[derive(Debug, Clone)]
struct Held {
    epoch: u64,
    etag: Etag,
    doc: Head,
    deadline: u64,
    next_renew: u64,
    retry_at: u64,
}

/// The lease state machine. One request is out at a time.
#[derive(Debug)]
pub struct CasLease {
    cfg: Config,
    held: Option<Held>,
    seen: Option<(Etag, u64)>,
    next_poll: u64,
    last_doc: Option<Head>,
    inflight: Option<Inflight>,
    queued: Option<Queued>,
    /// Writes of mine whose outcome was unknown and that no later landed
    /// write has ruled out. Any of them may still land, even after a
    /// resend of it was refused at once and a fresh write went out.
    unsettled: Vec<Inflight>,
    want_commit: Option<i64>,
    want_release: bool,
    stopped: bool,
    nonces: u64,
    stats: LeaseStats,
    events: Vec<LeaseEvent>,
}

fn splitmix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

impl CasLease {
    pub fn new(cfg: Config) -> Self {
        CasLease {
            cfg,
            held: None,
            seen: None,
            next_poll: 0,
            last_doc: None,
            inflight: None,
            queued: None,
            unsettled: Vec::new(),
            want_commit: None,
            want_release: false,
            stopped: false,
            nonces: 0,
            stats: LeaseStats::default(),
            events: Vec::new(),
        }
    }

    pub fn config(&self) -> &Config {
        &self.cfg
    }

    pub fn stats(&self) -> LeaseStats {
        self.stats
    }

    pub fn take_events(&mut self) -> Vec<LeaseEvent> {
        std::mem::take(&mut self.events)
    }

    /// Whether this node believes it leads at `now`.
    pub fn leads(&self, now: u64) -> bool {
        let releasing =
            self.want_release || self.inflight.as_ref().is_some_and(|f| f.op == Op::Release);
        !releasing && self.held.as_ref().is_some_and(|h| now < h.deadline)
    }

    /// The time on this node's clock until which it believes it leads, if
    /// it believes it leads at all. Lets a reader judge belief at the
    /// moment it asks, even if the caller has not run since.
    pub fn lead_until(&self, now: u64) -> Option<u64> {
        if self.leads(now) {
            self.held.as_ref().map(|h| h.deadline)
        } else {
            None
        }
    }

    /// The epoch this node holds, if it holds one (believed or not).
    pub fn held_epoch(&self) -> Option<u64> {
        self.held.as_ref().map(|h| h.epoch)
    }

    /// The newest HEAD this node has read or written.
    pub fn last_head(&self) -> Option<&Head> {
        match &self.held {
            Some(h) => Some(&h.doc),
            None => self.last_doc.as_ref(),
        }
    }

    /// Whether the archiver may start an upload now: this node leads,
    /// nothing is out or waiting, and no commit is pending.
    pub fn can_commit(&self, now: u64) -> bool {
        self.leads(now)
            && self.inflight.is_none()
            && self.queued.is_none()
            && self.want_commit.is_none()
    }

    /// The highest slot committed in the HEAD this node holds.
    pub fn committed_to(&self) -> Option<i64> {
        self.held.as_ref().and_then(|h| h.doc.committed_to())
    }

    /// Ask for slot `slot` to be committed. Ignored unless this node holds
    /// the lease and `slot` is above the committed head.
    pub fn commit(&mut self, slot: i64) {
        if self.held.is_some() && self.committed_to().is_none_or(|c| slot > c) {
            self.want_commit = Some(slot);
        }
    }

    pub fn commit_pending(&self) -> bool {
        self.want_commit.is_some()
    }

    /// Give the lease up and stop contending.
    pub fn release(&mut self) {
        if self.held.is_some() {
            self.want_release = true;
        } else {
            self.stopped = true;
        }
    }

    /// Whether a release has finished, or there was nothing to release.
    pub fn is_stopped(&self) -> bool {
        self.stopped && self.inflight.is_none() && self.queued.is_none()
    }

    fn nonce(&mut self) -> String {
        self.nonces += 1;
        let a = splitmix(self.cfg.nonce_seed ^ self.nonces.wrapping_mul(0xa076_1d64_78bd_642f));
        let b = splitmix(a ^ self.cfg.node as u64);
        format!("{a:016x}{b:016x}")
    }

    fn holder_cond(&self, h: &Held) -> Cond {
        if self.cfg.mutant == LeaseMutant::Unconditional {
            Cond::Unconditional
        } else {
            Cond::IfMatch(h.etag.clone())
        }
    }

    fn holder_doc(&mut self, h: &Held, op: Op) -> Head {
        let mut d = h.doc.clone();
        d.nonce = self.nonce();
        d.holder = Some(self.cfg.node);
        d.ttl_ms = self.cfg.timing.ttl_ms;
        match op {
            Op::Release => d.holder = None,
            Op::Commit(n) => {
                let prev_to = d.committed_to();
                let (epoch, keep) = (d.epoch, self.cfg.keep_slots);
                // The current epoch's own tenure is never dropped.
                d.tenures.retain(|t| t.to >= n - keep || t.e == epoch);
                if d.tenures.last().is_some_and(|t| t.e == epoch) {
                    if let Some(t) = d.tenures.last_mut() {
                        t.to = n;
                    }
                } else {
                    d.tenures.push(Tenure {
                        e: epoch,
                        n: self.cfg.node,
                        from: prev_to.map_or(n, |p| p + 1),
                        to: n,
                    });
                }
            }
            _ => {}
        }
        d
    }

    fn send(
        &mut self,
        now: u64,
        op: Op,
        doc: Head,
        cond: Cond,
        first_sent: Option<u64>,
        retried: bool,
    ) -> Request {
        let body = doc.encode();
        self.inflight = Some(Inflight {
            op,
            doc: Some(doc),
            cond: cond.clone(),
            first_sent: first_sent.unwrap_or(now),
            retried,
            phase: Phase::Sent,
        });
        Request::Write { body, cond }
    }

    /// The next request to run, if one is due at `now`.
    pub fn poll(&mut self, now: u64) -> Option<Request> {
        if let Some(f) = &mut self.inflight {
            return match f.phase {
                Phase::ResolveAt(t) if now >= t => {
                    f.phase = Phase::Resolving;
                    Some(Request::Read)
                }
                _ => None,
            };
        }
        if let Some(q) = self.queued.take() {
            if now < q.not_before {
                self.queued = Some(q);
                return None;
            }
            return Some(self.send(now, q.op, q.doc, q.cond, q.first_sent, q.retried));
        }
        if let Some(h) = self.held.clone() {
            if now < h.retry_at {
                return None;
            }
            let op = if self.want_release {
                Op::Release
            } else if now >= h.deadline || now >= h.next_renew {
                // Past the deadline nothing new is committed: one renew
                // first, and a 412 means fenced.
                match self.want_commit {
                    Some(n) if now < h.deadline => Op::Commit(n),
                    _ => Op::Renew,
                }
            } else {
                Op::Commit(self.want_commit?)
            };
            let cond = if op == Op::Release {
                Cond::IfMatch(h.etag.clone())
            } else {
                self.holder_cond(&h)
            };
            let doc = self.holder_doc(&h, op);
            return Some(self.send(now, op, doc, cond, None, false));
        }
        if self.stopped || now < self.next_poll {
            return None;
        }
        self.inflight = Some(Inflight {
            op: Op::Poll,
            doc: None,
            cond: Cond::Unconditional,
            first_sent: now,
            retried: false,
            phase: Phase::Sent,
        });
        Some(Request::Read)
    }

    fn takeover(&mut self, doc: &Head, etag: Etag) {
        let epoch = if self.cfg.mutant == LeaseMutant::ReuseEpoch && doc.epoch > 0 {
            doc.epoch
        } else {
            doc.epoch + 1
        };
        let new = Head {
            v: HEAD_VERSION,
            epoch,
            holder: Some(self.cfg.node),
            ttl_ms: self.cfg.timing.ttl_ms,
            nonce: self.nonce(),
            tenures: doc.tenures.clone(),
        };
        self.queued = Some(Queued {
            op: Op::Takeover,
            doc: new,
            cond: Cond::IfMatch(etag),
            first_sent: None,
            retried: false,
            not_before: 0,
        });
    }

    fn on_poll(&mut self, now: u64, r: HeadRead) {
        self.next_poll = now + self.cfg.timing.poll_ms;
        if self.stopped {
            return;
        }
        let (body, etag) = match r {
            Err(_) => return,
            Ok(None) if self.last_doc.is_some() => {
                // HEAD was there before. A missing one now is a store fault
                // or a misread path, and creating it again would send the
                // epoch back to 1.
                self.stats.missing_heads += 1;
                return;
            }
            Ok(None) => {
                let doc = Head {
                    v: HEAD_VERSION,
                    epoch: 1,
                    holder: Some(self.cfg.node),
                    ttl_ms: self.cfg.timing.ttl_ms,
                    nonce: self.nonce(),
                    tenures: Vec::new(),
                };
                self.queued = Some(Queued {
                    op: Op::Create,
                    doc,
                    cond: Cond::IfNoneMatch,
                    first_sent: None,
                    retried: false,
                    not_before: 0,
                });
                return;
            }
            Ok(Some(x)) => x,
        };
        let Some(doc) = Head::decode(&body) else {
            // Never overwrite a HEAD this node cannot read.
            self.stats.unreadable_heads += 1;
            return;
        };
        // My own grant that landed after I had given up on it.
        if let Some(u) = self.take_unsettled(&doc) {
            self.landed(now, u, etag);
            return;
        }
        if doc.holder.is_none() {
            self.takeover(&doc, etag);
        } else {
            let since = self
                .seen
                .as_ref()
                .filter(|(e, _)| *e == etag)
                .map(|(_, s)| *s);
            match since {
                Some(since) => {
                    let wait = match self.cfg.mutant {
                        LeaseMutant::ShortWatch => 0,
                        LeaseMutant::SlowTakeover => 3 * doc.ttl_ms,
                        LeaseMutant::TwoTtlTakeover => 2 * doc.ttl_ms,
                        _ => doc.ttl_ms + self.cfg.timing.takeover_margin_ms,
                    };
                    if now.saturating_sub(since) >= wait {
                        self.takeover(&doc, etag);
                    }
                }
                None => self.seen = Some((etag, now)),
            }
        }
        self.last_doc = Some(doc);
    }

    /// Hand back the result of a [`Request::Read`].
    pub fn on_read(&mut self, now: u64, r: HeadRead) {
        let Some(f) = self.inflight.take() else {
            return;
        };
        match (f.op, f.phase) {
            (Op::Poll, _) => self.on_poll(now, r),
            (_, Phase::Resolving) => self.resolve(now, f, r),
            _ => self.inflight = Some(f),
        }
    }

    /// Hand back the result of a [`Request::Write`].
    pub fn on_write(&mut self, now: u64, outcome: WriteOutcome) {
        let Some(mut f) = self.inflight.take() else {
            return;
        };
        if f.phase != Phase::Sent || f.op == Op::Poll {
            self.inflight = Some(f);
            return;
        }
        match outcome {
            WriteOutcome::Done(etag) => self.landed(now, f, etag),
            // An earlier attempt of this write, or an earlier write of mine
            // with an unknown outcome, may have landed late and moved the
            // ETag: read HEAD before calling it a fence.
            WriteOutcome::PreconditionFailed
                if (f.retried || !self.unsettled.is_empty())
                    && self.cfg.mutant != LeaseMutant::TrustRetry412 =>
            {
                f.phase = Phase::ResolveAt(now);
                self.inflight = Some(f);
            }
            WriteOutcome::PreconditionFailed => self.refused(now, f),
            WriteOutcome::Unreachable => self.not_applied(now, f),
            WriteOutcome::Unknown => {
                self.stats.unknown_outcomes += 1;
                if !self.unsettled.iter().any(|u| u.doc == f.doc) {
                    if self.unsettled.len() >= MAX_UNSETTLED {
                        self.unsettled.remove(0);
                    }
                    self.unsettled.push(f.clone());
                }
                f.phase = Phase::ResolveAt(now);
                self.inflight = Some(f);
            }
        }
    }

    fn landed(&mut self, now: u64, f: Inflight, etag: Etag) {
        let Some(doc) = f.doc else {
            return;
        };
        // Every unsettled write was conditioned on an older ETag (or on no
        // HEAD), so none of them can land after this one.
        self.unsettled.clear();
        let t = self.cfg.timing;
        let base = if self.cfg.mutant == LeaseMutant::DeadlineFromReceipt {
            now
        } else {
            f.first_sent
        };
        let deadline = (base + t.ttl_ms).saturating_sub(t.holder_margin_ms);
        match f.op {
            Op::Create | Op::Takeover => {
                // Asked to stop while the grant was out: give it back.
                if self.stopped {
                    self.stopped = false;
                    self.want_release = true;
                }
                self.stats.grants += 1;
                self.events.push(LeaseEvent::Granted { epoch: doc.epoch });
                self.seen = None;
                self.held = Some(Held {
                    epoch: doc.epoch,
                    etag,
                    doc: doc.clone(),
                    deadline,
                    next_renew: base + t.renew_ms,
                    retry_at: 0,
                });
            }
            Op::Renew | Op::Commit(_) => {
                if let Op::Commit(n) = f.op {
                    self.stats.commits += 1;
                    self.events.push(LeaseEvent::Committed {
                        epoch: doc.epoch,
                        slot: n,
                    });
                    if self.want_commit.is_some_and(|w| w <= n) {
                        self.want_commit = None;
                    }
                } else {
                    self.stats.renews += 1;
                }
                if let Some(h) = &mut self.held {
                    h.etag = etag;
                    h.doc = doc.clone();
                    h.deadline = deadline;
                    h.next_renew = base + t.renew_ms;
                    h.retry_at = 0;
                }
            }
            Op::Release => {
                self.events.push(LeaseEvent::Released { epoch: doc.epoch });
                self.held = None;
                self.want_release = false;
                self.stopped = true;
            }
            Op::Poll => {}
        }
        self.last_doc = Some(doc);
    }

    /// Remove and return the unsettled write whose document is `doc`.
    fn take_unsettled(&mut self, doc: &Head) -> Option<Inflight> {
        let i = self
            .unsettled
            .iter()
            .position(|u| u.doc.as_ref() == Some(doc))?;
        Some(self.unsettled.remove(i))
    }

    fn lose_hold(&mut self, now: u64) {
        self.unsettled.clear();
        self.held = None;
        self.want_commit = None;
        self.seen = None;
        self.next_poll = now;
    }

    fn refused(&mut self, now: u64, f: Inflight) {
        match f.op {
            Op::Create | Op::Takeover => {
                self.stats.lost_races += 1;
                self.seen = None;
                self.next_poll = now;
            }
            Op::Renew | Op::Commit(_) => {
                self.stats.fence_rejected += 1;
                let epoch = self.held.as_ref().map_or(0, |h| h.epoch);
                self.events.push(LeaseEvent::Fenced { epoch });
                self.lose_hold(now);
            }
            Op::Release => {
                self.lose_hold(now);
                self.want_release = false;
                self.stopped = true;
            }
            Op::Poll => {}
        }
    }

    fn not_applied(&mut self, now: u64, f: Inflight) {
        let t = self.cfg.timing;
        match f.op {
            Op::Create | Op::Takeover => self.next_poll = now + t.poll_ms,
            Op::Renew | Op::Commit(_) => {
                if let Some(h) = &mut self.held {
                    h.retry_at = now + t.poll_ms.min(t.renew_ms);
                }
            }
            // Best effort: the lease runs out on its own.
            Op::Release => {
                self.lose_hold(now);
                self.want_release = false;
                self.stopped = true;
            }
            Op::Poll => {}
        }
    }

    fn resolve(&mut self, now: u64, mut f: Inflight, r: HeadRead) {
        // A retry waits a fifth of a poll, so a store that keeps timing out
        // cannot spin the caller.
        let not_before = now + self.cfg.timing.poll_ms / 5;
        let retry = |f: &Inflight| Queued {
            op: f.op,
            doc: f.doc.clone().expect("a write has a doc"),
            cond: f.cond.clone(),
            first_sent: Some(f.first_sent),
            retried: true,
            not_before,
        };
        match r {
            Err(_) => {
                f.phase = Phase::ResolveAt(now + self.cfg.timing.poll_ms);
                self.inflight = Some(f);
            }
            Ok(None) => {
                if f.cond == Cond::IfNoneMatch {
                    self.stats.retries += 1;
                    self.queued = Some(retry(&f));
                } else {
                    self.refused(now, f);
                }
            }
            Ok(Some((body, etag))) => {
                if self.cfg.mutant == LeaseMutant::ResendOnUnknown {
                    self.stats.retries += 1;
                    let mut q = retry(&f);
                    q.cond = Cond::IfMatch(etag);
                    self.queued = Some(q);
                    return;
                }
                // Mine only when the whole document is the one I sent, so a
                // nonce that collides with another node's write (another
                // holder) is never read as my landed write.
                let cur = Head::decode(&body);
                let mine = matches!((&cur, &f.doc), (Some(c), Some(d)) if c == d);
                let earlier = match (&cur, self.cfg.mutant) {
                    (Some(c), m) if m != LeaseMutant::TrustRetry412 => self.take_unsettled(c),
                    _ => None,
                };
                if mine {
                    self.landed(now, f, etag);
                } else if let Some(u) = earlier {
                    // An earlier write of mine landed late. It is the
                    // newest state; this write was refused because of it
                    // and is built again from it by the next poll.
                    self.landed(now, u, etag);
                } else if f.cond == Cond::IfMatch(etag) {
                    self.stats.retries += 1;
                    self.queued = Some(retry(&f));
                } else {
                    if let Some(c) = cur {
                        self.last_doc = Some(c);
                    }
                    self.refused(now, f);
                }
            }
        }
    }
}

/// Run requests against `store` until nothing more is due. `now` reads the
/// node's monotonic clock. Returns how many requests were made.
pub fn drive<S: HeadStore + ?Sized>(
    m: &mut CasLease,
    store: &S,
    now: &mut dyn FnMut() -> u64,
) -> usize {
    let mut n = 0;
    while let Some(req) = m.poll(now()) {
        n += 1;
        match req {
            Request::Read => {
                let r = store.read_head();
                m.on_read(now(), r);
            }
            Request::Write { body, cond } => {
                let r = store.write_head(&body, &cond);
                m.on_write(now(), r);
            }
        }
    }
    n
}

/// What the history of applied HEAD writes shows.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Audit {
    pub writes: u64,
    /// Distinct (epoch, holder) grants.
    pub grants: u64,
    pub top_epoch: u64,
    /// Writes accepted under an epoch below one already granted.
    pub stale_epoch_commits: u64,
    /// Epochs written with more than one holder, or granted again after
    /// a release.
    pub epoch_two_holders: u64,
    /// Writes whose tenure list is malformed or rewrites an earlier one.
    pub tenure_violations: u64,
    pub unreadable: u64,
}

impl Audit {
    /// The first broken safety property, by name.
    pub fn reason(&self) -> Option<&'static str> {
        if self.stale_epoch_commits > 0 {
            Some("stale_epoch_commit")
        } else if self.epoch_two_holders > 0 {
            Some("epoch_two_holders")
        } else if self.tenure_violations > 0 {
            Some("tenure")
        } else if self.unreadable > 0 {
            Some("unreadable_head")
        } else {
            None
        }
    }
}

fn tenures_well_formed(d: &Head) -> bool {
    d.tenures.iter().all(|t| t.from <= t.to && t.e <= d.epoch)
        && d.tenures
            .windows(2)
            .all(|w| w[1].e > w[0].e && w[1].from == w[0].to + 1)
}

/// Whether `q` keeps `p` apart from dropping its oldest entries: the
/// shared run is equal except that the last shared tenure may have grown,
/// and anything after it is new.
fn tenures_extend(p: &[Tenure], q: &[Tenure]) -> bool {
    let Some(first) = q.first() else {
        return true;
    };
    let Some(k) = p.iter().position(|t| t.e == first.e) else {
        // Every old tenure was trimmed, or `q` starts after all of them.
        return p.last().is_none_or(|l| first.e > l.e && first.from > l.to);
    };
    let shared = &p[k..];
    if q.len() < shared.len() {
        return false;
    }
    shared.iter().zip(q).enumerate().all(|(i, (a, b))| {
        if i + 1 == shared.len() {
            a.e == b.e && a.n == b.n && a.from == b.from && b.to >= a.to
        } else {
            a == b
        }
    })
}

/// The second model: check a history of applied HEAD writes, in order.
pub fn audit<'a>(docs: impl IntoIterator<Item = &'a Head>) -> Audit {
    let mut a = Audit::default();
    let mut holders: HashMap<u64, u32> = HashMap::new();
    let mut two: HashSet<u64> = HashSet::new();
    let mut prev: Option<&Head> = None;
    for d in docs {
        a.writes += 1;
        if d.epoch < a.top_epoch {
            a.stale_epoch_commits += 1;
        }
        a.top_epoch = a.top_epoch.max(d.epoch);
        if let Some(h) = d.holder {
            match holders.get(&d.epoch) {
                None => {
                    holders.insert(d.epoch, h);
                    a.grants += 1;
                }
                // Counted once per epoch.
                Some(&x) if x != h && two.insert(d.epoch) => a.epoch_two_holders += 1,
                _ => {}
            }
            // A grant after a release must raise the epoch, even when the
            // same node takes it again.
            let regrant = prev.is_some_and(|p| p.holder.is_none() && p.epoch == d.epoch);
            if regrant && two.insert(d.epoch) {
                a.epoch_two_holders += 1;
            }
        }
        let owners_ok = d
            .tenures
            .iter()
            .all(|t| holders.get(&t.e).is_none_or(|&h| h == t.n));
        let extends = prev.is_none_or(|p| tenures_extend(&p.tenures, &d.tenures));
        if !(tenures_well_formed(d) && owners_ok && extends) {
            a.tenure_violations += 1;
        }
        prev = Some(d);
    }
    a
}

/// Audit raw journal lines; a line that is not a HEAD counts as unreadable.
pub fn audit_bodies(lines: &[Vec<u8>]) -> Audit {
    let docs: Vec<Option<Head>> = lines.iter().map(|l| Head::decode(l)).collect();
    let mut a = audit(docs.iter().flatten());
    a.unreadable = docs.iter().filter(|d| d.is_none()).count() as u64;
    a
}

/// The archive object for one slot under one epoch.
pub fn object_name(epoch: u64, slot: i64) -> String {
    format!("seg/e{epoch:010}/s{slot:010}.prec.gz")
}

pub fn parse_object_name(name: &str) -> Option<(u64, i64)> {
    let rest = name.strip_prefix("seg/e")?;
    let (e, rest) = rest.split_once("/s")?;
    let s = rest.strip_suffix(".prec.gz")?;
    if e.len() != 10 || !e.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if s.len() != 10 || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some((e.parse().ok()?, s.parse().ok()?))
}

/// What a reader can trust from HEAD and the list of objects.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveView {
    /// Slots covered by exactly one tenure.
    pub covered: u64,
    /// Covered slots whose object under the covering epoch exists.
    pub archived: u64,
    /// Covered slots with no object.
    pub holes: u64,
    /// Overlap objects: an epoch's copy of the slot just before its tenure,
    /// shipped from the new leader's own files when it took over. A reader
    /// merges one with that slot's archived object by key.
    pub overlaps: u64,
    /// Objects outside their epoch's tenure range: writes from a fenced
    /// epoch that HEAD never referenced.
    pub orphans: u64,
    /// Slots covered by more than one tenure. Never trusted.
    pub ambiguous: u64,
    /// Names under `seg/` that are not archive objects.
    pub foreign: u64,
}

/// What the object for `(epoch, slot)` is to a reader of `head`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectRole {
    /// The slot's archived object: exactly one tenure covers the slot and
    /// it is this epoch's.
    Archived,
    /// This epoch's overlap copy of the slot before its tenure.
    Overlap,
    /// Neither: never trusted.
    Orphan,
}

/// The tenures covering `slot`.
fn covering(head: &Head, slot: i64) -> impl Iterator<Item = &Tenure> {
    head.tenures
        .iter()
        .filter(move |t| t.from <= slot && slot <= t.to)
}

pub fn object_role(head: &Head, epoch: u64, slot: i64) -> ObjectRole {
    let mut c = covering(head, slot);
    if let (Some(t), None) = (c.next(), c.next()) {
        if t.e == epoch {
            return ObjectRole::Archived;
        }
    }
    if head
        .tenures
        .iter()
        .any(|t| t.e == epoch && t.from - 1 == slot)
    {
        ObjectRole::Overlap
    } else {
        ObjectRole::Orphan
    }
}

/// The reader rule: slot n is archived iff exactly one tenure covers n and
/// `seg/e<that epoch>/s<n>` exists. An epoch's object for the slot just
/// before its tenure is an overlap, merged with that slot by key; any other
/// object outside its epoch's tenure is an orphan.
pub fn read_archive(head: &Head, names: &[String]) -> ArchiveView {
    let mut v = ArchiveView::default();
    let mut objects: HashSet<(u64, i64)> = HashSet::new();
    for n in names {
        match parse_object_name(n) {
            Some(x) => {
                objects.insert(x);
            }
            None => v.foreign += 1,
        }
    }
    let mut cover: HashMap<i64, Vec<u64>> = HashMap::new();
    for t in &head.tenures {
        for s in t.from..=t.to {
            cover.entry(s).or_default().push(t.e);
        }
    }
    for (slot, es) in &cover {
        if es.len() > 1 {
            v.ambiguous += 1;
            continue;
        }
        v.covered += 1;
        if objects.contains(&(es[0], *slot)) {
            v.archived += 1;
        } else {
            v.holes += 1;
        }
    }
    for &(e, s) in &objects {
        match object_role(head, e, s) {
            ObjectRole::Archived => {}
            ObjectRole::Overlap => v.overlaps += 1,
            ObjectRole::Orphan => v.orphans += 1,
        }
    }
    v
}

#[cfg(test)]
#[path = "caslease_tests.rs"]
mod tests;
