use super::*;
use crate::store::{Fault, FsStore, MemStore, StoreError};
use proptest::prelude::*;
use proptest::test_runner::{RngAlgorithm, TestCaseError, TestRng, TestRunner};

fn cfg(node: u32) -> Config {
    Config::new(node, 7 + node as u64)
}

fn step<S: HeadStore>(m: &mut CasLease, s: &S, now: u64) {
    drive(m, s, &mut || now);
}

fn head(s: &MemStore) -> Head {
    Head::decode(&s.peek_head().expect("a head")).expect("a readable head")
}

fn journal(s: &MemStore) -> Vec<Head> {
    s.journal()
        .iter()
        .map(|b| Head::decode(b).expect("head"))
        .collect()
}

#[test]
fn the_spec_example_parses_and_heads_round_trip() {
    let ex = br#"{"v":1,"epoch":7,"holder":2,"ttl_ms":30000,"nonce":"ab",
 "tenures":[{"e":6,"n":1,"from":28862001,"to":28862040},{"e":7,"n":2,"from":28862041,"to":28862055}]}"#;
    let h = Head::decode(ex).expect("the spec example parses");
    assert_eq!(h.epoch, 7);
    assert_eq!(h.holder, Some(2));
    assert_eq!(h.committed_to(), Some(28_862_055));
    let b = h.encode();
    assert!(!b.contains(&b'\n'));
    assert_eq!(Head::decode(&b), Some(h));
    let released = br#"{"v":1,"epoch":7,"holder":null,"ttl_ms":30000,"nonce":"x","tenures":[]}"#;
    assert_eq!(Head::decode(released).unwrap().holder, None);
}

#[test]
fn the_first_node_creates_epoch_one_and_a_silent_holder_is_replaced_after_ttl_and_a_second() {
    let s = MemStore::new();
    let mut a = CasLease::new(cfg(1));
    let mut b = CasLease::new(cfg(2));
    step(&mut a, &s, 0);
    assert_eq!(head(&s).epoch, 1);
    assert_eq!(head(&s).holder, Some(1));
    assert!(a.leads(0) && a.leads(27_999) && !a.leads(28_000));
    // b first sees the ETag at 0 and polls every 5 s. a goes silent.
    for t in (0..=30_000).step_by(5_000) {
        step(&mut b, &s, t);
        assert_eq!(head(&s).holder, Some(1), "taken over early at {t}");
        assert!(!b.leads(t));
    }
    // 31 s have passed by the poll at 35 s.
    step(&mut b, &s, 35_000);
    assert_eq!(head(&s).epoch, 2);
    assert_eq!(head(&s).holder, Some(2));
    assert!(b.leads(35_000));
    assert_eq!(b.stats().grants, 1);
}

#[test]
fn a_renewing_holder_is_never_replaced() {
    let s = MemStore::new();
    let mut a = CasLease::new(cfg(1));
    let mut b = CasLease::new(cfg(2));
    for t in (0..300_000).step_by(1_000) {
        step(&mut a, &s, t);
        step(&mut b, &s, t);
        assert!(a.leads(t));
        assert!(!b.leads(t));
    }
    assert!(journal(&s)
        .iter()
        .all(|h| h.epoch == 1 && h.holder == Some(1)));
    assert!(a.stats().renews >= 29);
}

/// a leads, freezes past its deadline, b takes over, a wakes.
fn zombie(m: LeaseMutant) -> (MemStore, CasLease) {
    let s = MemStore::new();
    let mut a = CasLease::new(cfg(1).with_mutant(m));
    let mut b = CasLease::new(cfg(2));
    step(&mut a, &s, 0);
    for t in (0..=35_000).step_by(5_000) {
        step(&mut b, &s, t);
    }
    assert_eq!(head(&s).holder, Some(2));
    step(&mut a, &s, 40_000);
    (s, a)
}

#[test]
fn a_zombie_past_its_deadline_is_fenced_on_its_one_renew() {
    let (s, a) = zombie(LeaseMutant::None);
    assert_eq!(a.stats().fence_rejected, 1);
    assert!(!a.leads(40_000));
    assert_eq!(a.held_epoch(), None);
    assert_eq!(head(&s).holder, Some(2));
    assert_eq!(audit(&journal(&s)).reason(), None);
}

#[test]
fn without_if_match_the_zombie_overwrites_the_new_leader_and_the_audit_sees_it() {
    let (s, _) = zombie(LeaseMutant::Unconditional);
    assert_eq!(head(&s).holder, Some(1), "the zombie's renew landed");
    let a = audit(&journal(&s));
    assert_eq!(a.stale_epoch_commits, 1);
    assert_eq!(a.reason(), Some("stale_epoch_commit"));
}

#[test]
fn an_unknown_outcome_that_landed_is_recognised_by_its_nonce() {
    let s = MemStore::new();
    let mut a = CasLease::new(cfg(1));
    step(&mut a, &s, 0);
    s.set_fault(Fault::AppliedReplyLost);
    step(&mut a, &s, 10_000);
    assert_eq!(a.stats().unknown_outcomes, 1);
    assert_eq!(s.journal().len(), 2, "the renew landed");
    s.set_fault(Fault::None);
    step(&mut a, &s, 15_000);
    assert_eq!(s.journal().len(), 2, "not written again");
    assert_eq!(a.stats().renews, 1);
    // The deadline counts from when the renew was first sent.
    assert!(a.leads(37_999) && !a.leads(38_000));
}

#[test]
fn an_unknown_outcome_that_was_lost_is_retried_on_the_same_etag() {
    let s = MemStore::new();
    let mut a = CasLease::new(cfg(1));
    step(&mut a, &s, 0);
    s.set_fault(Fault::RequestLost);
    step(&mut a, &s, 10_000);
    assert_eq!(s.journal().len(), 1);
    s.set_fault(Fault::None);
    step(&mut a, &s, 15_000);
    assert_eq!(a.stats().retries, 1);
    step(&mut a, &s, 16_000);
    assert_eq!(s.journal().len(), 2);
    assert_eq!(a.stats().renews, 1);
    assert!(a.leads(16_000));
}

/// a's renew times out before reaching the store, HEAD still shows the old
/// ETag, so a resends it; the first attempt then lands before the resend.
fn late_first_attempt(m: LeaseMutant) -> (MemStore, CasLease) {
    let s = MemStore::new();
    let mut a = CasLease::new(cfg(1).with_mutant(m));
    step(&mut a, &s, 0);
    let Some(Request::Write { body, cond }) = a.poll(10_000) else {
        panic!("a renew is due at 10 s");
    };
    a.on_write(15_000, WriteOutcome::Unknown);
    assert_eq!(a.poll(15_000), Some(Request::Read));
    a.on_read(15_000, s.read_head());
    assert_eq!(a.stats().retries, 1);
    // The resend waits a fifth of a poll; the first attempt lands now.
    assert_eq!(a.poll(15_000), None);
    assert!(matches!(s.write_head(&body, &cond), WriteOutcome::Done(_)));
    step(&mut a, &s, 16_000);
    (s, a)
}

#[test]
fn a_late_first_attempt_is_not_read_as_a_fence() {
    let (s, a) = late_first_attempt(LeaseMutant::None);
    assert_eq!(a.stats().fence_rejected, 0);
    assert_eq!(a.stats().renews, 1);
    assert!(a.leads(16_000));
    // The deadline counts from the first send of the write that landed.
    assert!(a.leads(37_999) && !a.leads(38_000));
    assert_eq!(s.journal().len(), 2);
    assert_eq!(head(&s).holder, Some(1));
}

#[test]
fn trusting_the_412_on_a_resend_fences_a_healthy_holder() {
    let (s, a) = late_first_attempt(LeaseMutant::TrustRetry412);
    assert_eq!(a.stats().fence_rejected, 1);
    assert!(!a.leads(16_000));
    assert_eq!(head(&s).holder, Some(1), "HEAD still names a");
}

#[test]
fn a_landed_write_is_matched_on_the_whole_document_not_the_nonce() {
    // b's takeover happens to carry the nonce of a's lost renew.
    let s = MemStore::new();
    let mut a = CasLease::new(cfg(1));
    step(&mut a, &s, 0);
    let Some(Request::Write { body, .. }) = a.poll(10_000) else {
        panic!("a renew is due at 10 s");
    };
    a.on_write(15_000, WriteOutcome::Unknown);
    let mut other = Head::decode(&body).unwrap();
    other.epoch += 1;
    other.holder = Some(2);
    let (_, etag) = s.read_head().unwrap().unwrap();
    assert!(matches!(
        s.write_head(&other.encode(), &Cond::IfMatch(etag)),
        WriteOutcome::Done(_)
    ));
    step(&mut a, &s, 15_000);
    assert_eq!(a.stats().fence_rejected, 1);
    assert_eq!(a.stats().renews, 0);
    assert!(!a.leads(15_000));
}

/// a's renew is lost and its outcome unknown; b takes over before a can
/// read HEAD again.
fn unknown_then_takeover(m: LeaseMutant) -> (MemStore, CasLease) {
    let s = MemStore::new();
    let mut a = CasLease::new(cfg(1).with_mutant(m));
    let mut b = CasLease::new(cfg(2));
    step(&mut a, &s, 0);
    step(&mut b, &s, 0);
    s.set_fault(Fault::RequestLost);
    step(&mut a, &s, 10_000);
    s.set_fault(Fault::None);
    for t in (5_000..=35_000).step_by(5_000) {
        step(&mut b, &s, t);
    }
    assert_eq!(head(&s).holder, Some(2));
    step(&mut a, &s, 36_000);
    step(&mut a, &s, 37_000);
    (s, a)
}

#[test]
fn an_unknown_outcome_after_a_takeover_means_fenced() {
    let (s, a) = unknown_then_takeover(LeaseMutant::None);
    assert_eq!(a.stats().fence_rejected, 1);
    assert_eq!(head(&s).holder, Some(2));
    assert_eq!(audit(&journal(&s)).reason(), None);
}

#[test]
fn resending_on_an_unknown_outcome_overwrites_the_new_leader() {
    let (s, _) = unknown_then_takeover(LeaseMutant::ResendOnUnknown);
    assert_eq!(head(&s).holder, Some(1));
    assert_eq!(audit(&journal(&s)).reason(), Some("stale_epoch_commit"));
}

#[test]
fn a_head_that_disappears_after_it_was_seen_is_not_created_again() {
    let s = MemStore::new();
    let mut a = CasLease::new(cfg(1));
    let mut b = CasLease::new(cfg(2));
    step(&mut a, &s, 0);
    step(&mut b, &s, 0);
    assert_eq!(b.poll(5_000), Some(Request::Read));
    b.on_read(5_000, Ok(None));
    assert_eq!(b.poll(5_000), None, "no create at epoch 1");
    assert_eq!(b.stats().missing_heads, 1);
    // A node that never saw a HEAD still creates one.
    let mut c = CasLease::new(cfg(3));
    assert_eq!(c.poll(0), Some(Request::Read));
    c.on_read(0, Ok(None));
    assert!(matches!(
        c.poll(0),
        Some(Request::Write {
            cond: Cond::IfNoneMatch,
            ..
        })
    ));
}

#[test]
fn a_release_hands_over_at_once_and_keeps_the_epoch() {
    let s = MemStore::new();
    let mut a = CasLease::new(cfg(1));
    let mut b = CasLease::new(cfg(2));
    step(&mut a, &s, 0);
    step(&mut b, &s, 0);
    a.release();
    assert!(!a.leads(1_000));
    step(&mut a, &s, 1_000);
    assert!(a.is_stopped());
    assert_eq!(head(&s).holder, None);
    assert_eq!(head(&s).epoch, 1);
    step(&mut b, &s, 5_000);
    assert_eq!(head(&s).holder, Some(2));
    assert_eq!(head(&s).epoch, 2);
    // A stopped node does not contend again.
    step(&mut a, &s, 100_000);
    assert_eq!(head(&s).holder, Some(2));
}

#[test]
fn commits_keep_one_tenure_per_epoch_and_drop_old_ones() {
    let s = MemStore::new();
    let mut a = CasLease::new(cfg(1));
    let mut c = cfg(2);
    c.keep_slots = 5;
    let mut b = CasLease::new(c);
    step(&mut a, &s, 0);
    step(&mut b, &s, 0);
    assert!(a.can_commit(1_000));
    a.commit(100);
    assert!(!a.can_commit(1_000));
    step(&mut a, &s, 1_000);
    a.commit(101);
    step(&mut a, &s, 2_000);
    assert_eq!(a.committed_to(), Some(101));
    // A slot at or below the head is not committed again.
    a.commit(101);
    assert!(!a.commit_pending());
    // The commits at 1 s and 2 s reset b's watch at its 5 s poll.
    for t in (5_000..=35_000).step_by(5_000) {
        step(&mut b, &s, t);
        assert!(!b.leads(t));
    }
    step(&mut b, &s, 40_000);
    assert_eq!(b.committed_to(), Some(101));
    b.commit(102);
    step(&mut b, &s, 41_000);
    assert_eq!(
        head(&s).tenures,
        vec![
            Tenure {
                e: 1,
                n: 1,
                from: 100,
                to: 101
            },
            Tenure {
                e: 2,
                n: 2,
                from: 102,
                to: 102
            },
        ]
    );
    b.commit(200);
    step(&mut b, &s, 42_000);
    assert_eq!(
        head(&s).tenures,
        vec![Tenure {
            e: 2,
            n: 2,
            from: 102,
            to: 200
        }]
    );
    assert_eq!(audit(&journal(&s)).reason(), None);
    assert_eq!(b.take_events().len(), 3);
}

#[test]
fn the_lease_runs_on_the_directory_store_with_two_handles() {
    let n = std::process::id();
    let dir = std::env::temp_dir().join(format!("ha-caslease-fs-{n}"));
    let _ = std::fs::remove_dir_all(&dir);
    let sa = FsStore::open(&dir).unwrap();
    let sb = FsStore::open(&dir).unwrap();
    let mut a = CasLease::new(cfg(1));
    let mut b = CasLease::new(cfg(2));
    step(&mut a, &sa, 0);
    a.commit(5);
    step(&mut a, &sa, 1);
    for t in (0..=35_000).step_by(5_000) {
        step(&mut b, &sb, t);
    }
    assert!(b.leads(35_000));
    step(&mut a, &sa, 40_000);
    assert_eq!(a.stats().fence_rejected, 1);
    let lines = FsStore::read_journal(&dir).unwrap();
    assert_eq!(lines.len(), 3);
    let au = audit_bodies(&lines);
    assert_eq!(au.reason(), None);
    assert_eq!(au.grants, 2);
    let _ = std::fs::remove_dir_all(&dir);
}

fn doc(epoch: u64, holder: Option<u32>, tenures: &[(u64, u32, i64, i64)]) -> Head {
    Head {
        v: 1,
        epoch,
        holder,
        ttl_ms: 30_000,
        nonce: String::new(),
        tenures: tenures
            .iter()
            .map(|&(e, n, from, to)| Tenure { e, n, from, to })
            .collect(),
    }
}

#[test]
fn the_audit_passes_a_clean_history_and_names_each_broken_one() {
    let clean = [
        doc(1, Some(1), &[]),
        doc(1, Some(1), &[(1, 1, 10, 10)]),
        doc(1, Some(1), &[(1, 1, 10, 12)]),
        doc(1, None, &[(1, 1, 10, 12)]),
        doc(2, Some(2), &[(1, 1, 10, 12)]),
        doc(2, Some(2), &[(1, 1, 10, 12), (2, 2, 13, 13)]),
        // The oldest tenure is trimmed.
        doc(2, Some(2), &[(2, 2, 13, 14)]),
    ];
    let a = audit(&clean);
    assert_eq!(a.reason(), None, "{a:?}");
    assert_eq!(a.grants, 2);
    assert_eq!(a.top_epoch, 2);

    let stale = [
        doc(1, Some(1), &[]),
        doc(2, Some(2), &[]),
        doc(1, Some(1), &[(1, 1, 5, 5)]),
    ];
    assert_eq!(audit(&stale).reason(), Some("stale_epoch_commit"));

    let reused = [doc(1, Some(1), &[]), doc(1, Some(2), &[])];
    assert_eq!(audit(&reused).reason(), Some("epoch_two_holders"));
    // The same node granted the same epoch again after releasing it.
    let regranted = [
        doc(1, Some(1), &[]),
        doc(1, None, &[]),
        doc(1, Some(1), &[]),
    ];
    assert_eq!(audit(&regranted).reason(), Some("epoch_two_holders"));
    let renewed = [
        doc(1, Some(1), &[]),
        doc(1, Some(1), &[]),
        doc(1, None, &[]),
    ];
    assert_eq!(audit(&renewed).reason(), None);

    let rewritten = [
        doc(1, Some(1), &[(1, 1, 10, 12)]),
        doc(1, Some(1), &[(1, 1, 10, 11)]),
    ];
    assert_eq!(audit(&rewritten).reason(), Some("tenure"));
    let gap = [doc(2, Some(2), &[(1, 1, 10, 12), (2, 2, 14, 14)])];
    assert_eq!(audit(&gap).reason(), Some("tenure"));
    let wrong_owner = [doc(1, Some(1), &[]), doc(1, Some(1), &[(1, 3, 1, 1)])];
    assert_eq!(audit(&wrong_owner).reason(), Some("tenure"));
    let twice = [doc(1, Some(1), &[(1, 1, 1, 1), (1, 1, 2, 2)])];
    assert_eq!(audit(&twice).reason(), Some("tenure"));

    let a = audit_bodies(&[b"not json".to_vec()]);
    assert_eq!(a.reason(), Some("unreadable_head"));
}

#[test]
fn object_names_round_trip() {
    let n = object_name(7, 28_862_041);
    assert_eq!(n, "seg/e0000000007/s0028862041.prec.gz");
    assert_eq!(parse_object_name(&n), Some((7, 28_862_041)));
    assert_eq!(parse_object_name("seg/e7/s1.prec.gz"), None);
    assert_eq!(parse_object_name("drill/x"), None);
}

#[test]
fn the_reader_rule_counts_holes_and_orphans() {
    let h = doc(2, Some(2), &[(1, 1, 10, 12), (2, 2, 13, 14)]);
    let names: Vec<String> = [(1, 10), (1, 11), (1, 13), (2, 13), (2, 14), (1, 15)]
        .iter()
        .map(|&(e, s)| object_name(e, s))
        .chain(["seg/readme".to_string()])
        .collect();
    let v = read_archive(&h, &names);
    assert_eq!(v.covered, 5);
    assert_eq!(v.archived, 4);
    // Slot 12 was committed under epoch 1 with no object.
    assert_eq!(v.holes, 1);
    // A fenced epoch-1 writer kept uploading 13 and 15: never a winner.
    assert_eq!(v.orphans, 2);
    assert_eq!(v.foreign, 1);
}

// ---------------------------------------------------------------------
// The schedule property.
//
// Three nodes run the real state machine against a simulated store in
// 50 ms steps of real time. Each node's clock runs at its own rate (up to
// 1% off) from its own offset. Requests land after a latency and replies
// come back after another; a reply can be slow (up to 3 s past the
// timeout), lost (the write may still apply), refused at once, cut by a
// partition, or land late: after its sender timed out, with no reply. Nodes
// pause (frozen VMs), crash and restart, and sometimes release first. The
// leader commits a slot every 2 s, uploading its object first.
//
// Checked:
// - safety, always: the audit of every applied write is clean (epochs
//   never go back, one holder per epoch, tenures only grow) and no
//   committed slot lacks its object;
// - no false fence, always: a node never counts a fence while HEAD is its
//   own write under the epoch it held;
// - belief, when no node pauses: at most one node believes it leads;
// - liveness, in a calm tail: after `silent_at` faults stop and the holder
//   freezes for good. The contenders are ready once each is up, has no
//   request out from before the calm and holds nothing. From the later of
//   that and the last applied write, a different node holds HEAD within
//   ttl + 1 s + 2 polls (plus 3 s for round trips and clock rate).
// ---------------------------------------------------------------------

const STEP: u64 = 50;
const OFFSET: [i64; 3] = [1_000_000, 50_000_000, 7_000];

#[derive(Debug, Clone)]
struct Sched {
    rate_ppm: [i64; 3],
    /// (node, start, length), real ms.
    pauses: Vec<(usize, u64, u64)>,
    parts: Vec<(usize, u64, u64)>,
    /// (node, at, down for, release first)
    restarts: Vec<(usize, u64, u64, bool)>,
    slow_pct: u64,
    drop_pct: u64,
    fail_pct: u64,
    /// Requests that land after their sender timed out, with no reply.
    late_pct: u64,
    timeout_ms: u64,
    seed: u64,
    silent_at: Option<u64>,
}

fn in_iv(v: &[(usize, u64, u64)], i: usize, t: u64, cut: Option<u64>) -> bool {
    v.iter()
        .any(|&(n, a, len)| n == i && t >= a && t < (a + len).min(cut.unwrap_or(u64::MAX)))
}

impl Sched {
    fn belief(&self) -> bool {
        self.pauses.is_empty() && self.silent_at.is_none()
    }
    fn paused(&self, i: usize, t: u64) -> bool {
        in_iv(&self.pauses, i, t, self.silent_at)
    }
    fn parted(&self, i: usize, t: u64) -> bool {
        in_iv(&self.parts, i, t, self.silent_at)
    }
    fn duration(&self) -> u64 {
        self.silent_at.map_or(300_000, |a| a + 80_000)
    }
}

fn scheds() -> impl Strategy<Value = Sched> {
    let iv = || (0usize..3, 0u64..240_000, 1_000u64..80_000);
    let faults = (
        prop::array::uniform3(-10_000i64..=10_000),
        any::<bool>(),
        prop::collection::vec(iv(), 0..4),
        prop::collection::vec(iv(), 0..6),
        prop::collection::vec(
            (0usize..3, 0u64..240_000, 0u64..20_000, any::<bool>()),
            0..3,
        ),
    );
    let rates = (
        0u64..60,
        0u64..20,
        0u64..10,
        0u64..10,
        any::<bool>(),
        any::<u64>(),
        prop::option::weighted(0.3, 60_000u64..200_000),
    );
    (faults, rates).prop_map(
        |(
            (rate_ppm, pause, pauses, parts, restarts),
            (slow, lost, fail, late, long, seed, silent),
        )| {
            Sched {
                rate_ppm,
                pauses: if pause { pauses } else { Vec::new() },
                parts,
                restarts,
                slow_pct: slow,
                drop_pct: lost,
                fail_pct: fail,
                late_pct: late,
                timeout_ms: if long { 10_000 } else { 5_000 },
                seed,
                silent_at: silent,
            }
        },
    )
}

struct Rng(u64);

impl Rng {
    fn draw(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(1);
        splitmix(self.0)
    }
    fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            0
        } else {
            self.draw() % n
        }
    }
    fn pct(&mut self, p: u64) -> bool {
        self.below(100) < p
    }
}

#[derive(Debug)]
struct Flight {
    write: Option<(Vec<u8>, Cond)>,
    sent: u64,
    land_at: u64,
    applies: bool,
    landed: bool,
    reply_at: Option<u64>,
    timeout_at: u64,
    read: Option<HeadRead>,
    wrote: Option<WriteOutcome>,
}

struct SimNode {
    m: CasLease,
    flight: Option<Flight>,
    last_commit: u64,
    down_until: Option<u64>,
    life: u64,
}

struct World {
    head: Option<(Vec<u8>, Etag)>,
    version: u64,
    /// When HEAD was last written.
    last_write: u64,
    journal: Vec<Head>,
    objects: HashSet<(u64, i64)>,
}

impl World {
    fn land(&mut self, f: &mut Flight) {
        f.landed = true;
        if !f.applies {
            return;
        }
        let Some((body, cond)) = &f.write else {
            f.read = Some(Ok(self.head.clone()));
            return;
        };
        let holds = match cond {
            Cond::IfMatch(e) => self.head.as_ref().is_some_and(|(_, c)| c == e),
            Cond::IfNoneMatch => self.head.is_none(),
            Cond::Unconditional => true,
        };
        if !holds {
            f.wrote = Some(WriteOutcome::PreconditionFailed);
            return;
        }
        self.version += 1;
        self.last_write = f.land_at;
        let etag = Etag(format!("\"{}\"", self.version));
        self.head = Some((body.clone(), etag.clone()));
        self.journal
            .push(Head::decode(body).expect("the sim writes heads"));
        f.wrote = Some(WriteOutcome::Done(etag));
    }

    fn holder(&self) -> Option<u32> {
        self.doc().and_then(|h| h.holder)
    }

    fn doc(&self) -> Option<Head> {
        self.head.as_ref().and_then(|(b, _)| Head::decode(b))
    }
}

fn make_flight(node: usize, req: Request, t: u64, s: &Sched, rng: &mut Rng, calm: bool) -> Flight {
    let write = match req {
        Request::Read => None,
        Request::Write { body, cond } => Some((body, cond)),
    };
    let mut f = Flight {
        write,
        sent: t,
        land_at: t,
        applies: true,
        landed: false,
        reply_at: None,
        timeout_at: t + s.timeout_ms,
        read: None,
        wrote: None,
    };
    let quick = |rng: &mut Rng| 5 + rng.below(200);
    if calm {
        let d1 = quick(rng);
        f.land_at = t + d1;
        f.reply_at = Some(t + d1 + quick(rng));
        return f;
    }
    if s.parted(node, t) {
        // Never leaves: times out.
        f.applies = false;
        f.landed = true;
        return f;
    }
    if rng.pct(s.fail_pct) {
        // Refused at once, nothing applied.
        f.applies = false;
        f.landed = true;
        f.reply_at = Some(t);
        if f.write.is_some() {
            f.wrote = Some(WriteOutcome::Unreachable);
        } else {
            f.read = Some(Err(StoreError("refused".into())));
        }
        return f;
    }
    let d1 = quick(rng);
    if rng.pct(s.late_pct) {
        // Lands after the sender timed out; no reply comes back.
        f.land_at = f.timeout_at + rng.below(3_000);
        f.applies = !s.parted(node, f.land_at);
        return f;
    }
    let d2 = if rng.pct(s.slow_pct) {
        3_000 + rng.below(s.timeout_ms)
    } else {
        quick(rng)
    };
    let lost_request = rng.pct(s.drop_pct);
    let lost_reply = rng.pct(s.drop_pct);
    f.land_at = t + d1;
    f.applies = !lost_request && !s.parted(node, f.land_at);
    let r = t + d1 + d2;
    if f.applies && !lost_reply && !s.parted(node, r) && r < f.timeout_at {
        f.reply_at = Some(r);
    }
    f
}

fn sim(s: &Sched, mutant: LeaseMutant) -> Result<(), String> {
    let timing = Timing::default();
    let mk = |i: usize, life: u64| {
        CasLease::new(
            Config::new(i as u32 + 1, s.seed ^ (life << 8) ^ i as u64).with_mutant(mutant),
        )
    };
    let local =
        |i: usize, t: u64| (OFFSET[i] + t as i64 + t as i64 * s.rate_ppm[i] / 1_000_000) as u64;
    let mut nodes: Vec<SimNode> = (0..3)
        .map(|i| SimNode {
            m: mk(i, 0),
            flight: None,
            last_commit: 0,
            down_until: None,
            life: 0,
        })
        .collect();
    let mut orphans: Vec<Flight> = Vec::new();
    let mut w = World {
        head: None,
        version: 0,
        last_write: 0,
        journal: Vec::new(),
        objects: HashSet::new(),
    };
    let mut rng = Rng(s.seed);
    let mut victim: Option<usize> = None;
    let mut ready_at: Option<u64> = None;
    let bound = timing.ttl_ms + timing.takeover_margin_ms + 2 * timing.poll_ms + 3_000;
    let mut t = 0;
    while t <= s.duration() {
        let calm = s.silent_at.is_some_and(|a| t >= a);
        // Crashes and restarts.
        for &(n, at, down, release) in &s.restarts {
            if s.silent_at.is_some_and(|a| at >= a) {
                continue;
            }
            if release && at - at % STEP == t {
                nodes[n].m.release();
            }
            let crash = if release { at + 2_000 } else { at };
            if crash - crash % STEP == t && nodes[n].down_until.is_none() {
                if let Some(f) = nodes[n].flight.take() {
                    orphans.push(f);
                }
                let until = s.silent_at.map_or(crash + down, |a| (crash + down).min(a));
                nodes[n].down_until = Some(until.max(t + STEP));
            }
        }
        for (i, nd) in nodes.iter_mut().enumerate() {
            if nd.down_until.is_some_and(|u| t >= u) {
                nd.life += 1;
                nd.m = mk(i, nd.life);
                nd.down_until = None;
                nd.last_commit = 0;
            }
        }
        if calm && victim.is_none() {
            victim = Some(w.holder().map_or(0, |h| h as usize - 1));
        }

        // Requests reach the store in the order they land.
        let mut due: Vec<(u64, bool, usize)> = Vec::new();
        for (i, nd) in nodes.iter().enumerate() {
            if let Some(f) = &nd.flight {
                if !f.landed && f.land_at <= t {
                    due.push((f.land_at, false, i));
                }
            }
        }
        for (j, f) in orphans.iter().enumerate() {
            if !f.landed && f.land_at <= t {
                due.push((f.land_at, true, j));
            }
        }
        due.sort();
        for (_, orphan, k) in due {
            let f = if orphan {
                &mut orphans[k]
            } else {
                nodes[k].flight.as_mut().expect("due flight")
            };
            w.land(f);
        }
        orphans.retain(|f| !f.landed);

        for (i, nd) in nodes.iter_mut().enumerate() {
            if nd.down_until.is_some() || s.paused(i, t) || (calm && victim == Some(i)) {
                continue;
            }
            let now = local(i, t);
            let done = nd.flight.as_ref().map(|f| {
                let replied = f.landed && f.reply_at.is_some_and(|r| r <= t);
                (replied, replied || t >= f.timeout_at)
            });
            if let Some((replied, true)) = done {
                let mut f = nd.flight.take().expect("flight");
                let is_read = f.write.is_none();
                let fences = nd.m.stats().fence_rejected;
                let held = nd.m.held_epoch();
                match (is_read, replied) {
                    (true, true) => nd.m.on_read(
                        now,
                        f.read
                            .take()
                            .unwrap_or_else(|| Err(StoreError("lost".into()))),
                    ),
                    (true, false) => nd.m.on_read(now, Err(StoreError("timed out".into()))),
                    (false, true) => {
                        nd.m.on_write(now, f.wrote.take().unwrap_or(WriteOutcome::Unknown))
                    }
                    (false, false) => nd.m.on_write(now, WriteOutcome::Unknown),
                }
                if nd.m.stats().fence_rejected > fences {
                    let me = Some(i as u32 + 1);
                    if w.doc()
                        .is_some_and(|h| h.holder == me && Some(h.epoch) == held)
                    {
                        return Err(format!(
                            "node {} counted a fence at {t} ms while HEAD is its own epoch {held:?}",
                            i + 1
                        ));
                    }
                }
                // A write that timed out before landing may still land.
                if !is_read && !f.landed {
                    orphans.push(f);
                }
            }
            if nd.flight.is_none() && nd.m.can_commit(now) && now >= nd.last_commit + 2_000 {
                let n = nd.m.committed_to().map_or(0, |c| c + 1);
                if let Some(e) = nd.m.held_epoch() {
                    w.objects.insert((e, n));
                }
                nd.m.commit(n);
                nd.last_commit = now;
            }
            if nd.flight.is_none() {
                if let Some(req) = nd.m.poll(now) {
                    nd.flight = Some(make_flight(i, req, t, s, &mut rng, calm));
                }
            }
        }

        if s.belief() {
            let leaders = nodes
                .iter()
                .enumerate()
                .filter(|(i, nd)| nd.down_until.is_none() && nd.m.leads(local(*i, t)))
                .count();
            if leaders > 1 {
                return Err(format!("{leaders} nodes believe they lead at {t} ms"));
            }
        }
        if let (Some(a), Some(v)) = (s.silent_at, victim) {
            let ready = nodes.iter().enumerate().all(|(i, nd)| {
                i == v
                    || (nd.down_until.is_none()
                        && !nd.m.is_stopped()
                        && nd.m.held_epoch().is_none()
                        && nd.flight.as_ref().is_none_or(|f| f.sent >= a))
            });
            if ready_at.is_none() && ready {
                ready_at = Some(t);
            }
            if let Some(r) = ready_at {
                let h = w.holder();
                let from = r.max(w.last_write);
                if (h.is_none() || h == Some(v as u32 + 1)) && t >= from + bound {
                    return Err(format!(
                        "silent holder {} not replaced by {t} ms: contenders ready at {r} ms, \
                         last write at {} ms (HEAD holder {h:?})",
                        v + 1,
                        w.last_write
                    ));
                }
            }
        }
        t += STEP;
    }
    if let (Some(v), None) = (victim, ready_at) {
        let h = w.holder();
        if h.is_none() || h == Some(v as u32 + 1) {
            return Err("the contenders never became ready in the calm tail".into());
        }
    }

    let a = audit(&w.journal);
    if let Some(r) = a.reason() {
        return Err(format!("{r}: {a:?}"));
    }
    if let Some((b, _)) = &w.head {
        let head = Head::decode(b).expect("head");
        let names: Vec<String> = w.objects.iter().map(|&(e, n)| object_name(e, n)).collect();
        let v = read_archive(&head, &names);
        if v.holes > 0 || v.ambiguous > 0 {
            return Err(format!("committed slots without objects: {v:?}"));
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 128, ..ProptestConfig::default() })]

    #[test]
    fn cas_lease_schedules_keep_safety_belief_and_liveness(s in scheds()) {
        sim(&s, LeaseMutant::None).map_err(TestCaseError::fail)?;
    }
}

/// Schedules aimed at one hard case: a holder whose last successful renew
/// had a slow reply, then is cut off. With 5 s polls the earliest takeover
/// is 35 s after the holder's last write and its deadline is 28 s after it
/// sent that write, so the reply must come back more than 7 s late for a
/// deadline counted from the reply to overlap a takeover. These use a 30 s
/// request timeout, half the replies slow, no pauses, and four windows in
/// which two of the three nodes are cut off for 45 to 50 s. Forty seeds.
fn pinned() -> Vec<Sched> {
    let pair = |a: usize, b: usize, at: u64, len: u64| [(a, at, len), (b, at, len)];
    let parts: Vec<(usize, u64, u64)> = [
        pair(0, 1, 40_000, 50_000),
        pair(1, 2, 110_000, 50_000),
        pair(0, 2, 180_000, 50_000),
        pair(0, 1, 250_000, 45_000),
    ]
    .concat();
    (0..40)
        .map(|seed| Sched {
            rate_ppm: [0, 5_000, -5_000],
            pauses: Vec::new(),
            parts: parts.clone(),
            restarts: Vec::new(),
            slow_pct: 50,
            drop_pct: 0,
            fail_pct: 0,
            late_pct: 0,
            timeout_ms: 30_000,
            seed,
            silent_at: None,
        })
        .collect()
}

/// Run the property over the same 256 seeded random schedules and the
/// pinned ones. True if any schedule fails.
fn caught(m: LeaseMutant) -> (bool, bool) {
    let cfg = ProptestConfig {
        cases: 256,
        failure_persistence: None,
        ..ProptestConfig::default()
    };
    let rng = TestRng::deterministic_rng(RngAlgorithm::ChaCha);
    let random = TestRunner::new_with_rng(cfg, rng)
        .run(&scheds(), |s| sim(&s, m).map_err(TestCaseError::fail))
        .is_err();
    let pinned = pinned().iter().any(|s| sim(s, m).is_err());
    (random, pinned)
}

#[test]
fn the_schedule_property_passes_the_real_lease() {
    assert_eq!(caught(LeaseMutant::None), (false, false));
}

#[test]
fn the_schedule_property_catches_epoch_reuse() {
    assert!(caught(LeaseMutant::ReuseEpoch).0);
}

#[test]
fn the_schedule_property_catches_a_commit_without_if_match() {
    assert!(caught(LeaseMutant::Unconditional).0);
}

#[test]
fn the_schedule_property_catches_a_deadline_timed_from_the_reply() {
    let (random, pinned) = caught(LeaseMutant::DeadlineFromReceipt);
    eprintln!("deadline from reply: random search {random}, pinned {pinned}");
    assert!(random || pinned);
}

#[test]
fn the_schedule_property_catches_a_takeover_without_a_full_ttl_watch() {
    assert!(caught(LeaseMutant::ShortWatch).0);
}

#[test]
fn the_schedule_property_catches_a_resend_after_an_unknown_outcome() {
    assert!(caught(LeaseMutant::ResendOnUnknown).0);
}

#[test]
fn the_schedule_property_catches_a_slow_takeover() {
    assert!(caught(LeaseMutant::SlowTakeover).0);
}

#[test]
fn the_schedule_property_catches_a_takeover_after_two_ttl() {
    assert!(caught(LeaseMutant::TwoTtlTakeover).0);
}

#[test]
fn the_schedule_property_catches_a_fence_read_from_a_late_first_attempt() {
    assert!(caught(LeaseMutant::TrustRetry412).0);
}
