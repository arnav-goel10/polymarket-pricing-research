//! Local order-book reconstruction from a snapshot followed by deltas.
//!
//! [`LocalBook`] owns one side-sorted book and decides, for every incoming
//! message, whether it can be applied safely. Anything that would leave the
//! book in an unknown state (a gap in the sequence, a malformed level, a book
//! that ends up crossed) flips the book to *desynced*. When the venue reports
//! its own best bid and ask alongside a delta, levels the venue no longer
//! shows at the top (consumed by trades) are pruned before the crossed check;
//! see [`LocalBook::apply_delta_with_top`]. A desynced book keeps
//! rejecting deltas until a fresh snapshot arrives, and callers must treat it
//! as stale data. This is the resync contract the live feed and the property
//! tests rely on.

use crate::book::{Level, OrderBook};
use std::cmp::Ordering;

/// Which side of the book a delta touches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookSide {
    Bid,
    Ask,
}

/// A single price-level change. `size == 0` removes the level.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LevelDelta {
    pub side: BookSide,
    pub price: f64,
    pub size: f64,
}

/// How the sequence key on each message is interpreted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeqPolicy {
    /// Every delta must carry exactly `last + 1`. Anything larger is a gap.
    /// Use for venues that publish per-message sequence numbers.
    Contiguous,
    /// The key only has to be non-decreasing (for example an exchange
    /// timestamp). Older keys are dropped; gaps cannot be detected.
    Monotonic,
}

/// What happened to one message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// The message changed (or confirmed) the book.
    Applied,
    /// Already seen under [`SeqPolicy::Contiguous`]; ignored, book unchanged.
    Duplicate,
    /// Older than the current book; ignored, book unchanged.
    Stale,
    /// A sequence gap: at least one delta was lost. Book is now desynced.
    Gap { expected: u64, got: u64 },
    /// A level was non-finite, negative or outside the price range. Book is
    /// now desynced because the venue's true state is unknown.
    Malformed,
    /// Best bid >= best ask after applying. Book is now desynced.
    Crossed,
    /// No usable snapshot yet (or desynced); delta dropped.
    AwaitingSnapshot,
}

impl ApplyOutcome {
    /// True when this outcome means the caller must fetch a new snapshot.
    pub fn needs_resync(self) -> bool {
        matches!(
            self,
            ApplyOutcome::Gap { .. } | ApplyOutcome::Malformed | ApplyOutcome::Crossed
        )
    }
}

/// Price equality tolerance (prices are decimal ticks parsed from strings).
const PRICE_EPS: f64 = 1e-12;

/// A snapshot-plus-deltas order book with gap, duplicate and crossed-book
/// detection.
#[derive(Debug, Clone)]
pub struct LocalBook {
    policy: SeqPolicy,
    /// Inclusive bounds a valid price must sit in (Polymarket: (0, 1)).
    price_bounds: (f64, f64),
    book: OrderBook,
    last_seq: Option<u64>,
    synced: bool,
    resyncs_needed: u64,
    pruned: u64,
}

impl LocalBook {
    pub fn new(policy: SeqPolicy) -> Self {
        Self::with_price_bounds(policy, (0.0, f64::INFINITY))
    }

    /// A book for probability-priced contracts: every price must be in (0, 1).
    pub fn probability(policy: SeqPolicy) -> Self {
        Self::with_price_bounds(policy, (0.0, 1.0))
    }

    pub fn with_price_bounds(policy: SeqPolicy, price_bounds: (f64, f64)) -> Self {
        Self {
            policy,
            price_bounds,
            book: OrderBook {
                bids: Vec::new(),
                asks: Vec::new(),
            },
            last_seq: None,
            synced: false,
            resyncs_needed: 0,
            pruned: 0,
        }
    }

    /// The current book. Only trustworthy while [`is_synced`](Self::is_synced).
    pub fn book(&self) -> &OrderBook {
        &self.book
    }

    pub fn is_synced(&self) -> bool {
        self.synced
    }

    pub fn last_seq(&self) -> Option<u64> {
        self.last_seq
    }

    /// How many times this book has fallen out of sync since creation.
    pub fn resyncs_needed(&self) -> u64 {
        self.resyncs_needed
    }

    fn valid_level(&self, price: f64, size: f64) -> bool {
        price.is_finite()
            && size.is_finite()
            && size >= 0.0
            && price > self.price_bounds.0
            && price < self.price_bounds.1
    }

    fn desync(&mut self, outcome: ApplyOutcome) -> ApplyOutcome {
        if self.synced {
            self.resyncs_needed += 1;
        }
        self.synced = false;
        outcome
    }

    fn crossed(&self) -> bool {
        matches!(
            (self.book.best_bid(), self.book.best_ask()),
            (Some(bid), Some(ask)) if bid >= ask - PRICE_EPS
        )
    }

    /// Force the book out of sync (for example the caller saw a frame it
    /// could not decode) and return [`ApplyOutcome::Malformed`].
    pub fn mark_desynced(&mut self) -> ApplyOutcome {
        self.desync(ApplyOutcome::Malformed)
    }

    /// Replace the whole book. A snapshot older than the current state is
    /// ignored; a malformed or crossed snapshot leaves the book desynced.
    pub fn apply_snapshot(&mut self, seq: u64, bids: &[Level], asks: &[Level]) -> ApplyOutcome {
        if self.synced && self.last_seq.is_some_and(|last| seq < last) {
            return ApplyOutcome::Stale;
        }
        if bids
            .iter()
            .chain(asks)
            .any(|l| !self.valid_level(l.price, l.size))
        {
            return self.desync(ApplyOutcome::Malformed);
        }
        let mut new_bids: Vec<Level> = Vec::with_capacity(bids.len());
        let mut new_asks: Vec<Level> = Vec::with_capacity(asks.len());
        for l in bids {
            upsert(&mut new_bids, l.price, l.size, BookSide::Bid);
        }
        for l in asks {
            upsert(&mut new_asks, l.price, l.size, BookSide::Ask);
        }
        self.book = OrderBook {
            bids: new_bids,
            asks: new_asks,
        };
        self.last_seq = Some(seq);
        if self.crossed() {
            return self.desync(ApplyOutcome::Crossed);
        }
        self.synced = true;
        ApplyOutcome::Applied
    }

    /// Apply one message worth of level changes carrying sequence key `seq`.
    /// The message is applied atomically: if any level is malformed, none are.
    pub fn apply_delta(&mut self, seq: u64, changes: &[LevelDelta]) -> ApplyOutcome {
        self.apply_delta_with_top(seq, changes, VenueTop::default())
    }

    /// [`apply_delta`](Self::apply_delta), then reconcile with the venue's
    /// reported top of book before the crossed check: bids priced above the
    /// venue's best bid and asks below its best ask are removed. Some venues
    /// (Polymarket among them) report fills as trades rather than as level
    /// removals, so without this the local book keeps consumed levels and
    /// ends up crossed. Pruned levels are counted in [`pruned_levels`](Self::pruned_levels).
    pub fn apply_delta_with_top(
        &mut self,
        seq: u64,
        changes: &[LevelDelta],
        top: VenueTop,
    ) -> ApplyOutcome {
        if !self.synced {
            return ApplyOutcome::AwaitingSnapshot;
        }
        let last = self.last_seq.unwrap_or(0);
        match self.policy {
            SeqPolicy::Contiguous => {
                if seq <= last {
                    return ApplyOutcome::Duplicate;
                }
                if seq != last + 1 {
                    return self.desync(ApplyOutcome::Gap {
                        expected: last + 1,
                        got: seq,
                    });
                }
            }
            SeqPolicy::Monotonic => {
                if seq < last {
                    return ApplyOutcome::Stale;
                }
            }
        }
        if changes.iter().any(|c| !self.valid_level(c.price, c.size)) {
            return self.desync(ApplyOutcome::Malformed);
        }
        for c in changes {
            let levels = match c.side {
                BookSide::Bid => &mut self.book.bids,
                BookSide::Ask => &mut self.book.asks,
            };
            upsert(levels, c.price, c.size, c.side);
        }
        self.last_seq = Some(seq);
        self.reconcile(top);
        if self.crossed() {
            return self.desync(ApplyOutcome::Crossed);
        }
        ApplyOutcome::Applied
    }

    fn reconcile(&mut self, top: VenueTop) {
        let before = self.book.bids.len() + self.book.asks.len();
        if let Some(bb) = top.best_bid.filter(|p| p.is_finite()) {
            self.book.bids.retain(|l| l.price <= bb + PRICE_EPS);
        }
        if let Some(ba) = top.best_ask.filter(|p| p.is_finite()) {
            self.book.asks.retain(|l| l.price >= ba - PRICE_EPS);
        }
        self.pruned += (before - self.book.bids.len() - self.book.asks.len()) as u64;
    }

    /// Levels removed so far because the venue's top of book no longer
    /// showed them.
    pub fn pruned_levels(&self) -> u64 {
        self.pruned
    }
}

/// Best bid and ask as reported by the venue with a message, when it reports
/// them. `None` means "not reported", not "empty side".
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct VenueTop {
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
}

/// Ordering used to keep each side best-first: bids descending, asks ascending.
fn side_cmp(side: BookSide, level_price: f64, target: f64) -> Ordering {
    if (level_price - target).abs() <= PRICE_EPS {
        return Ordering::Equal;
    }
    match side {
        BookSide::Bid => target.total_cmp(&level_price),
        BookSide::Ask => level_price.total_cmp(&target),
    }
}

/// Insert, replace or remove one level with a binary search, keeping the side
/// sorted. O(log n) search plus one shift, versus a full re-sort per update.
fn upsert(levels: &mut Vec<Level>, price: f64, size: f64, side: BookSide) {
    match levels.binary_search_by(|l| side_cmp(side, l.price, price)) {
        Ok(i) => {
            if size > 0.0 {
                levels[i].size = size;
            } else {
                levels.remove(i);
            }
        }
        Err(i) => {
            if size > 0.0 {
                levels.insert(i, Level { price, size });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lv(price: f64, size: f64) -> Level {
        Level { price, size }
    }

    fn d(side: BookSide, price: f64, size: f64) -> LevelDelta {
        LevelDelta { side, price, size }
    }

    fn seeded() -> LocalBook {
        let mut b = LocalBook::probability(SeqPolicy::Contiguous);
        let out = b.apply_snapshot(
            10,
            &[lv(0.48, 10.0), lv(0.49, 5.0)],
            &[lv(0.53, 10.0), lv(0.52, 5.0)],
        );
        assert_eq!(out, ApplyOutcome::Applied);
        b
    }

    #[test]
    fn snapshot_is_sorted_best_first() {
        let b = seeded();
        assert_eq!(b.book().best_bid(), Some(0.49));
        assert_eq!(b.book().best_ask(), Some(0.52));
        assert!(b.is_synced());
    }

    #[test]
    fn contiguous_delta_applies_and_zero_size_removes() {
        let mut b = seeded();
        let out = b.apply_delta(
            11,
            &[d(BookSide::Bid, 0.50, 3.0), d(BookSide::Ask, 0.52, 0.0)],
        );
        assert_eq!(out, ApplyOutcome::Applied);
        assert_eq!(b.book().best_bid(), Some(0.50));
        assert_eq!(b.book().best_ask(), Some(0.53));
    }

    #[test]
    fn duplicate_is_ignored() {
        let mut b = seeded();
        b.apply_delta(11, &[d(BookSide::Bid, 0.50, 3.0)]);
        let before = b.book().clone();
        assert_eq!(
            b.apply_delta(11, &[d(BookSide::Bid, 0.50, 99.0)]),
            ApplyOutcome::Duplicate
        );
        assert_eq!(b.book(), &before);
    }

    #[test]
    fn gap_desyncs_until_next_snapshot() {
        let mut b = seeded();
        assert_eq!(
            b.apply_delta(13, &[d(BookSide::Bid, 0.50, 3.0)]),
            ApplyOutcome::Gap {
                expected: 11,
                got: 13
            }
        );
        assert!(!b.is_synced());
        assert_eq!(b.resyncs_needed(), 1);
        assert_eq!(
            b.apply_delta(14, &[d(BookSide::Bid, 0.50, 3.0)]),
            ApplyOutcome::AwaitingSnapshot
        );
        assert_eq!(
            b.apply_snapshot(20, &[lv(0.40, 1.0)], &[lv(0.60, 1.0)]),
            ApplyOutcome::Applied
        );
        assert!(b.is_synced());
        assert_eq!(
            b.apply_delta(21, &[d(BookSide::Ask, 0.59, 1.0)]),
            ApplyOutcome::Applied
        );
    }

    #[test]
    fn crossing_delta_desyncs() {
        let mut b = seeded();
        assert_eq!(
            b.apply_delta(11, &[d(BookSide::Bid, 0.55, 1.0)]),
            ApplyOutcome::Crossed
        );
        assert!(!b.is_synced());
    }

    #[test]
    fn crossed_snapshot_is_never_synced() {
        let mut b = LocalBook::probability(SeqPolicy::Contiguous);
        assert_eq!(
            b.apply_snapshot(1, &[lv(0.60, 1.0)], &[lv(0.55, 1.0)]),
            ApplyOutcome::Crossed
        );
        assert!(!b.is_synced());
    }

    #[test]
    fn malformed_delta_is_atomic_and_desyncs() {
        let mut b = seeded();
        let before = b.book().clone();
        let out = b.apply_delta(
            11,
            &[d(BookSide::Bid, 0.50, 3.0), d(BookSide::Ask, f64::NAN, 1.0)],
        );
        assert_eq!(out, ApplyOutcome::Malformed);
        assert_eq!(b.book(), &before, "no partial application");
        assert!(!b.is_synced());
    }

    #[test]
    fn out_of_range_probability_is_malformed() {
        let mut b = seeded();
        assert_eq!(
            b.apply_delta(11, &[d(BookSide::Ask, 1.5, 1.0)]),
            ApplyOutcome::Malformed
        );
    }

    #[test]
    fn monotonic_policy_drops_older_and_accepts_equal_keys() {
        let mut b = LocalBook::probability(SeqPolicy::Monotonic);
        b.apply_snapshot(1_000, &[lv(0.40, 1.0)], &[lv(0.60, 1.0)]);
        assert_eq!(
            b.apply_delta(999, &[d(BookSide::Bid, 0.45, 1.0)]),
            ApplyOutcome::Stale
        );
        assert_eq!(
            b.apply_delta(1_000, &[d(BookSide::Bid, 0.45, 1.0)]),
            ApplyOutcome::Applied
        );
        assert_eq!(
            b.apply_delta(5_000, &[d(BookSide::Bid, 0.46, 1.0)]),
            ApplyOutcome::Applied
        );
        assert_eq!(b.book().best_bid(), Some(0.46));
    }

    #[test]
    fn venue_top_prunes_levels_consumed_by_trades() {
        let mut b = seeded();
        // A buy at 0.52 filled against the 0.52 ask; the venue reports the
        // resting remainder as a new 0.52 bid and its new top as 0.52 / 0.53,
        // but sends no removal for the consumed ask.
        let out = b.apply_delta_with_top(
            11,
            &[d(BookSide::Bid, 0.52, 3.0)],
            VenueTop {
                best_bid: Some(0.52),
                best_ask: Some(0.53),
            },
        );
        assert_eq!(out, ApplyOutcome::Applied);
        assert_eq!(b.book().best_bid(), Some(0.52));
        assert_eq!(b.book().best_ask(), Some(0.53));
        assert_eq!(b.pruned_levels(), 1);
        // Without the venue top the same delta is a crossed book.
        let mut b = seeded();
        assert_eq!(
            b.apply_delta(11, &[d(BookSide::Bid, 0.52, 3.0)]),
            ApplyOutcome::Crossed
        );
    }

    #[test]
    fn older_snapshot_than_current_state_is_stale() {
        let mut b = seeded();
        assert_eq!(
            b.apply_snapshot(5, &[lv(0.10, 1.0)], &[lv(0.90, 1.0)]),
            ApplyOutcome::Stale
        );
        assert_eq!(b.book().best_bid(), Some(0.49));
    }
}
