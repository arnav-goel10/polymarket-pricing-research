//! Property-based tests for order-book reconstruction.
//!
//! A plain `BTreeMap` replay of the same messages is the reference model. The
//! properties feed `LocalBook` duplicated, dropped, reordered, malformed and
//! crossing messages and check that it either matches the reference exactly
//! or reports itself out of sync. It must never hold a wrong book while
//! claiming to be in sync.

use market::{ApplyOutcome, BookSide, Level, LevelDelta, LocalBook, OrderBook, SeqPolicy};
use proptest::prelude::*;
use std::collections::BTreeMap;

const TICK: f64 = 1000.0;
const SNAP_SEQ: u64 = 100;

#[derive(Debug, Clone, Default, PartialEq)]
struct RefBook {
    bids: BTreeMap<u32, u32>,
    asks: BTreeMap<u32, u32>,
}

impl RefBook {
    fn apply(&mut self, changes: &[(BookSide, u32, u32)]) {
        for &(side, tick, size) in changes {
            let m = match side {
                BookSide::Bid => &mut self.bids,
                BookSide::Ask => &mut self.asks,
            };
            if size == 0 {
                m.remove(&tick);
            } else {
                m.insert(tick, size);
            }
        }
    }
}

fn to_levels(m: &BTreeMap<u32, u32>) -> Vec<Level> {
    m.iter()
        .map(|(&t, &s)| Level {
            price: t as f64 / TICK,
            size: s as f64,
        })
        .collect()
}

fn to_deltas(changes: &[(BookSide, u32, u32)]) -> Vec<LevelDelta> {
    changes
        .iter()
        .map(|&(side, tick, size)| LevelDelta {
            side,
            price: tick as f64 / TICK,
            size: size as f64,
        })
        .collect()
}

/// Bids and asks as integer (tick, size) pairs, best first.
type Sides = (Vec<(u32, u32)>, Vec<(u32, u32)>);

/// The book as integer (tick, size) pairs in best-first order.
fn as_ticks(b: &OrderBook) -> Sides {
    let f = |ls: &[Level]| {
        ls.iter()
            .map(|l| ((l.price * TICK).round() as u32, l.size as u32))
            .collect::<Vec<_>>()
    };
    (f(&b.bids), f(&b.asks))
}

fn ref_ticks(r: &RefBook) -> Sides {
    (
        r.bids.iter().rev().map(|(&t, &s)| (t, s)).collect(),
        r.asks.iter().map(|(&t, &s)| (t, s)).collect(),
    )
}

/// Structural invariants that must hold after every single message.
fn assert_well_formed(lb: &LocalBook) {
    let b = lb.book();
    for w in b.bids.windows(2) {
        assert!(
            w[0].price > w[1].price,
            "bids not strictly descending: {b:?}"
        );
    }
    for w in b.asks.windows(2) {
        assert!(
            w[0].price < w[1].price,
            "asks not strictly ascending: {b:?}"
        );
    }
    for l in b.bids.iter().chain(&b.asks) {
        assert!(l.size > 0.0 && l.size.is_finite(), "bad size {l:?}");
        assert!(l.price > 0.0 && l.price < 1.0, "bad price {l:?}");
    }
    if lb.is_synced() {
        if let (Some(bid), Some(ask)) = (b.best_bid(), b.best_ask()) {
            assert!(bid < ask, "synced book is crossed: {b:?}");
        }
    }
}

type Change = (BookSide, u32, u32);

/// One change on the honest (never-crossing) stream: bids 1..=499, asks 501..=999.
fn honest_change() -> impl Strategy<Value = Change> {
    prop_oneof![
        (1u32..=499, 0u32..=5).prop_map(|(t, s)| (BookSide::Bid, t, s)),
        (501u32..=999, 0u32..=5).prop_map(|(t, s)| (BookSide::Ask, t, s)),
    ]
}

/// A change anywhere in the price range, so it may cross the book.
fn any_change() -> impl Strategy<Value = Change> {
    prop_oneof![
        (1u32..=999, 0u32..=5).prop_map(|(t, s)| (BookSide::Bid, t, s)),
        (1u32..=999, 0u32..=5).prop_map(|(t, s)| (BookSide::Ask, t, s)),
    ]
}

fn snapshot() -> impl Strategy<Value = RefBook> {
    (
        prop::collection::btree_map(1u32..=499, 1u32..=5, 0..20),
        prop::collection::btree_map(501u32..=999, 1u32..=5, 0..20),
    )
        .prop_map(|(bids, asks)| RefBook { bids, asks })
}

fn stream(change: BoxedStrategy<Change>) -> impl Strategy<Value = (RefBook, Vec<Vec<Change>>)> {
    (
        snapshot(),
        prop::collection::vec(prop::collection::vec(change, 1..4), 1..60),
    )
}

/// Start a book from `snap` at `SNAP_SEQ`.
fn start(snap: &RefBook) -> LocalBook {
    let mut lb = LocalBook::probability(SeqPolicy::Contiguous);
    assert_eq!(
        lb.apply_snapshot(SNAP_SEQ, &to_levels(&snap.bids), &to_levels(&snap.asks)),
        ApplyOutcome::Applied
    );
    lb
}

/// Reference state after the snapshot plus the first `n` deltas.
fn reference_prefix(snap: &RefBook, deltas: &[Vec<Change>], n: usize) -> RefBook {
    let mut r = snap.clone();
    for d in &deltas[..n] {
        r.apply(d);
    }
    r
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// In-order delivery with arbitrary re-sent (duplicate) messages ends in
    /// exactly the reference book, and duplicates never change anything.
    #[test]
    fn duplicates_are_idempotent(
        (snap, deltas) in stream(honest_change().boxed()),
        dup_picks in prop::collection::vec((any::<prop::sample::Index>(), any::<prop::sample::Index>()), 0..40),
    ) {
        // Build the delivery order: each delta once in order, plus duplicates of
        // already-delivered deltas inserted at random later points.
        let mut delivery: Vec<usize> = (0..deltas.len()).collect();
        for (at, which) in dup_picks {
            let pos = at.index(delivery.len() + 1);
            let delivered_max = delivery[..pos].iter().copied().max();
            if let Some(max) = delivered_max {
                delivery.insert(pos, which.index(max + 1));
            }
        }
        let mut lb = start(&snap);
        let mut highest = None::<usize>;
        for i in delivery {
            let before = lb.book().clone();
            let out = lb.apply_delta(SNAP_SEQ + 1 + i as u64, &to_deltas(&deltas[i]));
            if highest.is_some_and(|h| i <= h) {
                prop_assert_eq!(out, ApplyOutcome::Duplicate);
                prop_assert_eq!(lb.book(), &before);
            } else {
                prop_assert_eq!(out, ApplyOutcome::Applied);
                highest = Some(i);
            }
            assert_well_formed(&lb);
        }
        prop_assert!(lb.is_synced());
        let want = reference_prefix(&snap, &deltas, deltas.len());
        prop_assert_eq!(as_ticks(lb.book()), ref_ticks(&want));
    }

    /// Dropping any delta is detected at the first missing sequence number;
    /// the book stops applying deltas and a fresh snapshot restores it exactly.
    #[test]
    fn gaps_are_detected_and_snapshot_resyncs(
        (snap, deltas) in stream(honest_change().boxed()),
        drop_mask in prop::collection::vec(any::<bool>(), 60),
    ) {
        let kept: Vec<usize> = (0..deltas.len()).filter(|&i| !drop_mask[i]).collect();
        let first_drop = (0..deltas.len()).find(|&i| drop_mask[i]);
        let mut lb = start(&snap);
        let mut saw_gap = false;
        for &i in &kept {
            let out = lb.apply_delta(SNAP_SEQ + 1 + i as u64, &to_deltas(&deltas[i]));
            match first_drop {
                Some(gap) if i > gap => {
                    if saw_gap {
                        prop_assert_eq!(out, ApplyOutcome::AwaitingSnapshot);
                    } else {
                        prop_assert_eq!(out, ApplyOutcome::Gap {
                            expected: SNAP_SEQ + 1 + gap as u64,
                            got: SNAP_SEQ + 1 + i as u64,
                        });
                        saw_gap = true;
                    }
                }
                _ => prop_assert_eq!(out, ApplyOutcome::Applied),
            }
            assert_well_formed(&lb);
        }
        match first_drop {
            Some(gap) => {
                // The book holds the last consistent state, flagged stale (or,
                // if no later delta arrived, it has not noticed yet: still exact).
                let want = reference_prefix(&snap, &deltas, gap);
                prop_assert_eq!(as_ticks(lb.book()), ref_ticks(&want));
                prop_assert_eq!(lb.is_synced(), !saw_gap);
            }
            None => {
                prop_assert!(lb.is_synced());
                let want = reference_prefix(&snap, &deltas, deltas.len());
                prop_assert_eq!(as_ticks(lb.book()), ref_ticks(&want));
            }
        }
        // Resync with the true current state.
        let truth = reference_prefix(&snap, &deltas, deltas.len());
        let resync_seq = SNAP_SEQ + 1 + deltas.len() as u64;
        prop_assert_eq!(
            lb.apply_snapshot(resync_seq, &to_levels(&truth.bids), &to_levels(&truth.asks)),
            ApplyOutcome::Applied
        );
        prop_assert!(lb.is_synced());
        prop_assert_eq!(as_ticks(lb.book()), ref_ticks(&truth));
    }

    /// Under any delivery order, a book that still claims to be synced holds
    /// exactly the reference state for the sequence number it reports.
    #[test]
    fn reordering_never_yields_a_wrong_synced_book(
        (snap, deltas) in stream(honest_change().boxed()),
        order in any::<u64>(),
    ) {
        let mut idx: Vec<usize> = (0..deltas.len()).collect();
        // Deterministic Fisher-Yates from the drawn seed.
        let mut s = order | 1;
        for i in (1..idx.len()).rev() {
            s ^= s << 13; s ^= s >> 7; s ^= s << 17;
            idx.swap(i, (s % (i as u64 + 1)) as usize);
        }
        let mut lb = start(&snap);
        for &i in &idx {
            lb.apply_delta(SNAP_SEQ + 1 + i as u64, &to_deltas(&deltas[i]));
            assert_well_formed(&lb);
            if lb.is_synced() {
                let applied = (lb.last_seq().unwrap() - SNAP_SEQ) as usize;
                let want = reference_prefix(&snap, &deltas, applied);
                prop_assert_eq!(as_ticks(lb.book()), ref_ticks(&want));
            }
        }
    }

    /// With deltas allowed anywhere in the price range, the book is never
    /// both synced and crossed, and every crossing is reported.
    #[test]
    fn crossed_books_are_always_flagged(
        (snap, deltas) in stream(any_change().boxed()),
    ) {
        let mut lb = start(&snap);
        let mut reference = snap.clone();
        for (i, d) in deltas.iter().enumerate() {
            let out = lb.apply_delta(SNAP_SEQ + 1 + i as u64, &to_deltas(d));
            if out == ApplyOutcome::AwaitingSnapshot {
                break;
            }
            reference.apply(d);
            let crossed = matches!(
                (reference.bids.keys().next_back(), reference.asks.keys().next()),
                (Some(b), Some(a)) if b >= a
            );
            prop_assert_eq!(out == ApplyOutcome::Crossed, crossed);
            prop_assert_eq!(lb.is_synced(), !crossed);
            assert_well_formed(&lb);
        }
    }

    /// A malformed level anywhere in a message leaves the book untouched and
    /// out of sync: no partial application.
    #[test]
    fn malformed_messages_are_atomic(
        (snap, deltas) in stream(honest_change().boxed()),
        at in any::<prop::sample::Index>(),
        bad in prop_oneof![
            Just(f64::NAN), Just(f64::INFINITY), Just(-0.25), Just(0.0), Just(1.0), Just(7.5)
        ],
        bad_size in any::<bool>(),
    ) {
        let at = at.index(deltas.len());
        let mut lb = start(&snap);
        for (i, d) in deltas[..at].iter().enumerate() {
            prop_assert_eq!(lb.apply_delta(SNAP_SEQ + 1 + i as u64, &to_deltas(d)), ApplyOutcome::Applied);
        }
        let before = lb.book().clone();
        let mut msg = to_deltas(&deltas[at]);
        let last = msg.last_mut().unwrap();
        if bad_size {
            last.size = if bad.is_finite() { -1.0 } else { bad };
        } else {
            last.price = bad;
        }
        let out = lb.apply_delta(SNAP_SEQ + 1 + at as u64, &msg);
        prop_assert_eq!(out, ApplyOutcome::Malformed);
        prop_assert!(!lb.is_synced());
        prop_assert_eq!(lb.book(), &before);
        assert_well_formed(&lb);
    }
}
