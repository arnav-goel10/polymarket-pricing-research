//! Merge the event streams of N nodes into one, delivering each key once.
//!
//! Each feed has its own window of recently delivered keys. A key already in
//! the window is a duplicate. A key not in the window is new, unless the
//! window can no longer tell whether it was delivered before, in which case
//! it is late and dropped.
//!
//! Where a key sits in the window comes from the key alone. The venue
//! timestamp is part of [`EventKey`], so every copy of a key has the same
//! timestamp and no copy can carry the key past the window under a
//! different time. (If a venue ever sent one sequence number with two
//! different timestamps, those are two keys and both are delivered; see
//! [`crate::key`] for why each rule's timestamp is the same in every copy.)
//!
//! Timed keys. The window holds them ordered by (timestamp, key) and drops
//! them in two ways:
//!
//! - Expiry: a key whose timestamp is below the horizon, `max venue
//!   timestamp seen - span`, leaves. The horizon never moves back.
//! - The capacity cap: when the window is full, the smallest (timestamp,
//!   key) leaves, and the floor is raised to it. The floor never moves back.
//!
//! A timed key not in the window is late if its timestamp is below the
//! horizon or if (timestamp, key) is at or below the floor. Every key that
//! ever left satisfies one of these from then on, so:
//!
//! - Never twice. A timed key is delivered at most once, however late its
//!   copies arrive and however small the capacity.
//! - Nothing lost while lateness fits. If every node delivers each event
//!   within `span` of the newest venue timestamp the merger has seen for
//!   that feed, and the capacity holds every key inside one span, then every
//!   timed key any node delivered comes out exactly once and `late` stays
//!   zero.
//! - When the cap does bite, the loss is as small as it can be. The floor is
//!   the exact (timestamp, key) that was forced out, not its millisecond, so
//!   other keys at the same millisecond are still accepted. A key is late
//!   because of the cap only if at least `capacity` keys ordered after it
//!   were already delivered.
//!
//! Late events are dropped, not delivered, because a late copy cannot be
//! told apart from a duplicate of a key that has left the window. The
//! counter makes the loss visible; size `span` above the worst node lag.
//!
//! Untimed keys (no venue timestamp: unkeyed raw frames, a Binance message
//! without `T`) have no place on the venue timeline. They are kept apart, in
//! arrival order, each stamped with the newest venue time seen when it
//! arrived, and leave once that falls below the horizon or when their own
//! cap is full. They are never late, are deduplicated while in the window,
//! and can come out twice after that. They never move the floor, so they
//! cannot make a timed event late. They are counted as `untimed`.
//!
//! Memory is fixed at construction: each window preallocates room for
//! `capacity` timed and `capacity` untimed keys and never holds more. The
//! merger is deterministic: the same arrivals in the same order give the
//! same verdicts.

use crate::key::{EventKey, Feed, KeyBuildHasher};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet, VecDeque};

/// One feed's dedup window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WindowConfig {
    /// How far behind the newest venue timestamp an event may arrive and
    /// still be accepted.
    pub span_millis: i64,
    /// The most timed keys held at once (untimed keys get the same amount
    /// again). Keep it above the event rate times the span, or the cap
    /// shrinks the effective span (counted as `forced_evictions`).
    pub capacity: usize,
}

impl Default for WindowConfig {
    fn default() -> Self {
        // The 10-minute live recording peaked near 1,600 messages a second
        // across all feeds. 10 s at that rate is 16,000 keys.
        WindowConfig {
            span_millis: 10_000,
            capacity: 32_768,
        }
    }
}

/// Windows for the three feeds, indexed by [`Feed::index`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MergeConfig {
    pub windows: [WindowConfig; 3],
}

impl MergeConfig {
    pub fn uniform(w: WindowConfig) -> Self {
        MergeConfig { windows: [w; 3] }
    }

    pub fn window(&self, feed: Feed) -> WindowConfig {
        self.windows[feed.index()]
    }
}

/// What the merger did with one offered event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// First copy: pass it on.
    Deliver,
    /// Already delivered: drop it.
    Duplicate,
    /// Older than the window: dropped and counted.
    Late,
}

/// Counters for one feed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FeedCounters {
    pub delivered: u64,
    pub duplicates: u64,
    pub late: u64,
    /// Keys that left the window by falling below the horizon.
    pub expired: u64,
    /// Keys pushed out by a capacity cap (timed or untimed).
    pub forced_evictions: u64,
    /// Offered events that had no venue timestamp.
    pub untimed: u64,
}

impl FeedCounters {
    fn add(&mut self, o: &FeedCounters) {
        self.delivered += o.delivered;
        self.duplicates += o.duplicates;
        self.late += o.late;
        self.expired += o.expired;
        self.forced_evictions += o.forced_evictions;
        self.untimed += o.untimed;
    }
}

#[derive(Debug)]
struct Window {
    cfg: WindowConfig,
    seen: HashSet<EventKey, KeyBuildHasher>,
    /// Timed keys in the window, smallest (timestamp, key) on top.
    timed: BinaryHeap<Reverse<(i64, EventKey)>>,
    /// Untimed keys in arrival order, with the newest venue time at arrival.
    untimed: VecDeque<(i64, EventKey)>,
    max_seen: Option<i64>,
    /// The largest (timestamp, key) the cap forced out.
    floor: Option<(i64, EventKey)>,
}

impl Window {
    fn new(cfg: WindowConfig) -> Self {
        let cfg = WindowConfig {
            capacity: cfg.capacity.max(1),
            span_millis: cfg.span_millis.max(0),
        };
        Window {
            cfg,
            seen: HashSet::with_capacity_and_hasher(
                2 * cfg.capacity + 1,
                KeyBuildHasher::default(),
            ),
            timed: BinaryHeap::with_capacity(cfg.capacity + 1),
            untimed: VecDeque::with_capacity(cfg.capacity),
            max_seen: None,
            floor: None,
        }
    }

    fn horizon(&self, span: i64) -> Option<i64> {
        self.max_seen.map(|m| m.saturating_sub(span))
    }

    fn is_late(&self, t: i64, key: &EventKey, span: i64, coarse_floor: bool) -> bool {
        if self.horizon(span).is_some_and(|h| t < h) {
            return true;
        }
        match self.floor {
            Some((ft, _)) if coarse_floor => t <= ft,
            Some(f) => (t, *key) <= f,
            None => false,
        }
    }
}

/// Deliberately broken variants, used only by the tests to show the
/// property tests can fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) enum Mutant {
    None,
    /// Treat every key missing from the set as new, ignoring horizon and
    /// floor.
    ForgetLate,
    /// Use half the configured span.
    HalfSpan,
    /// Never raise the floor when the cap forces a key out.
    NoFloor,
    /// The old floor: block the forced-out key's whole millisecond.
    CoarseFloor,
}

/// Merges keyed events from several nodes.
#[derive(Debug)]
pub struct Merger {
    windows: [Window; 3],
    counters: [FeedCounters; 3],
    first_from: Vec<u64>,
    mutant: Mutant,
}

impl Merger {
    pub fn new(nodes: usize, cfg: MergeConfig) -> Self {
        Merger {
            windows: Feed::ALL.map(|f| Window::new(cfg.window(f))),
            counters: [FeedCounters::default(); 3],
            first_from: vec![0; nodes],
            mutant: Mutant::None,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_mutant(mut self, m: Mutant) -> Self {
        self.mutant = m;
        self
    }

    /// Offer one event that `node` received. Returns what happened to it.
    pub fn offer(&mut self, node: usize, key: EventKey) -> Verdict {
        let i = key.kind.feed().index();
        let mutant = self.mutant;
        let w = &mut self.windows[i];
        let c = &mut self.counters[i];
        let span = match mutant {
            Mutant::HalfSpan => w.cfg.span_millis / 2,
            _ => w.cfg.span_millis,
        };

        if key.ts_millis.is_none() {
            c.untimed += 1;
        }
        if w.seen.contains(&key) {
            c.duplicates += 1;
            return Verdict::Duplicate;
        }
        match key.ts_millis {
            Some(t) => {
                let check = mutant != Mutant::ForgetLate;
                if check && w.is_late(t, &key, span, mutant == Mutant::CoarseFloor) {
                    c.late += 1;
                    return Verdict::Late;
                }
                w.timed.push(Reverse((t, key)));
                w.seen.insert(key);
                if w.timed.len() > w.cfg.capacity {
                    // The smallest goes, which may be the key just added.
                    if let Some(Reverse(out)) = w.timed.pop() {
                        w.seen.remove(&out.1);
                        if mutant != Mutant::NoFloor {
                            w.floor = w.floor.max(Some(out));
                        }
                        c.forced_evictions += 1;
                    }
                }
                w.max_seen = Some(w.max_seen.map_or(t, |m| m.max(t)));
            }
            None => {
                if w.untimed.len() >= w.cfg.capacity {
                    if let Some((_, out)) = w.untimed.pop_front() {
                        w.seen.remove(&out);
                        c.forced_evictions += 1;
                    }
                }
                let at = w.max_seen.unwrap_or(i64::MIN);
                w.untimed.push_back((at, key));
                w.seen.insert(key);
            }
        }

        if let Some(h) = w.horizon(span) {
            while let Some(&Reverse((t, k))) = w.timed.peek() {
                if t >= h {
                    break;
                }
                w.timed.pop();
                w.seen.remove(&k);
                c.expired += 1;
            }
            while let Some(&(t, k)) = w.untimed.front() {
                if t >= h {
                    break;
                }
                w.untimed.pop_front();
                w.seen.remove(&k);
                c.expired += 1;
            }
        }

        c.delivered += 1;
        if let Some(n) = self.first_from.get_mut(node) {
            *n += 1;
        }
        Verdict::Deliver
    }

    pub fn counters(&self, feed: Feed) -> FeedCounters {
        self.counters[feed.index()]
    }

    /// Counters summed over all feeds.
    pub fn totals(&self) -> FeedCounters {
        let mut t = FeedCounters::default();
        for c in &self.counters {
            t.add(c);
        }
        t
    }

    /// How many delivered events each node supplied first.
    pub fn first_from(&self) -> &[u64] {
        &self.first_from
    }

    /// Keys currently held for a feed, timed and untimed.
    pub fn window_len(&self, feed: Feed) -> usize {
        self.windows[feed.index()].seen.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::{for_each_event, KeyKind};
    use feeds::recording::Source;
    use proptest::prelude::*;
    use proptest::test_runner::{Config, RngAlgorithm, TestCaseError, TestRng, TestRunner};
    use std::collections::HashSet as Set;

    const FEED_KINDS: [KeyKind; 3] = [
        KeyKind::RtdsPrice,
        KeyKind::BinanceBook,
        KeyKind::ClobPriceChange,
    ];

    fn ev(feed: usize, id: u128, ts: i64) -> EventKey {
        EventKey {
            kind: FEED_KINDS[feed],
            id,
            ts_millis: Some(ts),
        }
    }

    fn untimed(feed: Feed, id: u128) -> EventKey {
        EventKey {
            kind: KeyKind::Unkeyed(feed),
            id,
            ts_millis: None,
        }
    }

    fn window(span_millis: i64, capacity: usize) -> MergeConfig {
        MergeConfig::uniform(WindowConfig {
            span_millis,
            capacity,
        })
    }

    #[test]
    fn duplicates_inside_the_window_are_dropped() {
        let mut m = Merger::new(2, window(1_000, 64));
        assert_eq!(m.offer(0, ev(1, 7, 100)), Verdict::Deliver);
        assert_eq!(m.offer(1, ev(1, 7, 100)), Verdict::Duplicate);
        assert_eq!(m.offer(1, ev(1, 8, 101)), Verdict::Deliver);
        assert_eq!(m.offer(0, ev(1, 8, 101)), Verdict::Duplicate);
        let c = m.counters(Feed::Binance);
        assert_eq!((c.delivered, c.duplicates, c.late), (2, 2, 0));
        assert_eq!(m.first_from(), &[1, 1]);
    }

    #[test]
    fn a_duplicate_outside_the_window_is_counted_late_not_delivered() {
        let mut m = Merger::new(2, window(100, 64));
        assert_eq!(m.offer(0, ev(2, 1, 0)), Verdict::Deliver);
        // Moves the horizon to 900 and expires key 1.
        assert_eq!(m.offer(0, ev(2, 2, 1_000)), Verdict::Deliver);
        assert_eq!(m.window_len(Feed::Clob), 1);
        assert_eq!(m.counters(Feed::Clob).expired, 1);
        // Node 1 is far behind and sends key 1 again.
        assert_eq!(m.offer(1, ev(2, 1, 0)), Verdict::Late);
        let c = m.counters(Feed::Clob);
        assert_eq!((c.delivered, c.duplicates, c.late), (2, 0, 1));
        // Other feeds are untouched.
        assert_eq!(m.counters(Feed::Rtds), FeedCounters::default());
    }

    #[test]
    fn the_capacity_cap_raises_the_floor_so_forced_out_keys_stay_caught() {
        let mut m = Merger::new(2, window(1_000_000, 2));
        for id in 0..3 {
            assert_eq!(m.offer(0, ev(0, id, 10 + id as i64)), Verdict::Deliver);
        }
        assert_eq!(m.window_len(Feed::Rtds), 2);
        assert_eq!(m.counters(Feed::Rtds).forced_evictions, 1);
        // Key 0 was pushed out well inside the span. Its copy is still caught.
        assert_eq!(m.offer(1, ev(0, 0, 10)), Verdict::Late);
        assert_eq!(m.offer(1, ev(0, 2, 12)), Verdict::Duplicate);
    }

    #[test]
    fn a_forced_eviction_does_not_block_its_whole_millisecond() {
        // The old floor was the evicted timestamp plus one, which made every
        // other key at that millisecond late.
        let mut m = Merger::new(2, window(1_000_000, 2));
        assert_eq!(m.offer(0, ev(0, 1, 10)), Verdict::Deliver);
        assert_eq!(m.offer(0, ev(0, 2, 10)), Verdict::Deliver);
        assert_eq!(m.offer(0, ev(0, 3, 10)), Verdict::Deliver);
        assert_eq!(m.counters(Feed::Rtds).forced_evictions, 1);
        // Key 1 left; its copy is caught.
        assert_eq!(m.offer(1, ev(0, 1, 10)), Verdict::Late);
        // Key 4 at the same millisecond was never delivered and must be.
        assert_eq!(m.offer(1, ev(0, 4, 10)), Verdict::Deliver);
        assert_eq!(m.offer(1, ev(0, 5, 11)), Verdict::Deliver);
        // A key ordered below what the cap already pushed out is late: two
        // keys after it were delivered and the window cannot tell.
        assert_eq!(m.offer(1, ev(0, 0, 10)), Verdict::Late);
    }

    #[test]
    fn untimed_evictions_never_make_timed_events_late() {
        // The old floor took an untimed key's position (the newest venue
        // time) and jumped above every current timestamp.
        let mut m = Merger::new(1, window(1_000, 2));
        assert_eq!(m.offer(0, ev(0, 1, 5_000)), Verdict::Deliver);
        for id in 1..=3 {
            assert_eq!(m.offer(0, untimed(Feed::Rtds, id)), Verdict::Deliver);
        }
        assert_eq!(m.counters(Feed::Rtds).forced_evictions, 1);
        assert_eq!(m.offer(0, ev(0, 2, 5_000)), Verdict::Deliver);
        assert_eq!(m.offer(0, ev(0, 3, 4_999)), Verdict::Deliver);
        assert_eq!(m.counters(Feed::Rtds).late, 0);
    }

    #[test]
    fn untimed_events_are_counted_and_never_late() {
        let mut m = Merger::new(1, window(10, 8));
        let k = untimed(Feed::Rtds, 42);
        assert_eq!(m.offer(0, ev(0, 1, 5_000)), Verdict::Deliver);
        assert_eq!(m.offer(0, k), Verdict::Deliver);
        assert_eq!(m.offer(0, k), Verdict::Duplicate);
        assert_eq!(m.counters(Feed::Rtds).untimed, 2);
        // Once newer venue time passes, the key has left and comes out again.
        assert_eq!(m.offer(0, ev(0, 2, 5_011)), Verdict::Deliver);
        assert_eq!(m.offer(0, k), Verdict::Deliver);
    }

    #[test]
    fn binance_copies_with_different_push_times_merge_once() {
        // Same book update seen by two nodes, `E` differs. Node 1's copy
        // arrives after the window has moved well past the push times but
        // inside the span of `T`.
        let a =
            r#"{"stream":"btcusdt@bookTicker","data":{"u":5,"b":"1","a":"2","T":10000,"E":10001}}"#;
        let b =
            r#"{"stream":"btcusdt@bookTicker","data":{"u":5,"b":"1","a":"2","T":10000,"E":19000}}"#;
        let c =
            r#"{"stream":"btcusdt@bookTicker","data":{"u":6,"b":"1","a":"2","T":10900,"E":10901}}"#;
        let key = |f: &str| {
            let mut out = Vec::new();
            for_each_event(Source::Binance, f, |k| out.push(k));
            out[0]
        };
        let mut m = Merger::new(2, window(1_000, 64));
        assert_eq!(m.offer(0, key(a)), Verdict::Deliver);
        assert_eq!(m.offer(0, key(c)), Verdict::Deliver);
        assert_eq!(m.offer(1, key(b)), Verdict::Duplicate);
    }

    #[test]
    fn memory_stays_within_capacity_under_any_rate() {
        let mut m = Merger::new(1, window(i64::MAX, 64));
        for id in 0..10_000u128 {
            m.offer(0, ev(1, id, 0));
            m.offer(0, untimed(Feed::Binance, id));
            assert!(m.window_len(Feed::Binance) <= 128);
        }
    }

    /// Every kind the generator may use. Two per feed, keyed and unkeyed,
    /// so one id shows up under several kinds and several feeds.
    const KINDS: [KeyKind; 6] = [
        KeyKind::RtdsPrice,
        KeyKind::BinanceBook,
        KeyKind::ClobPriceChange,
        KeyKind::Unkeyed(Feed::Rtds),
        KeyKind::Unkeyed(Feed::Binance),
        KeyKind::Unkeyed(Feed::Clob),
    ];

    /// k nodes, each holding an arbitrary subset of a base stream, each
    /// event delayed by its own amount. Arrival order is base time plus
    /// delay, which gives an arbitrary interleaving across nodes.
    #[derive(Debug, Clone)]
    struct Case {
        /// (key, base time), base times non-decreasing. A timed key's base
        /// time is its venue timestamp; an untimed key has only the base
        /// time. Ids come from a small range, so the same id appears in
        /// several feeds and kinds, and the same kind and id at several
        /// timestamps (copies of one sequence number stamped differently,
        /// which are different keys).
        base: Vec<(EventKey, i64)>,
        /// nodes[n][i] = delay of base event i at node n, or None if node n
        /// never got it.
        nodes: Vec<Vec<Option<i64>>>,
        span: i64,
        capacity: usize,
    }

    fn case(bounded: bool) -> impl Strategy<Value = Case> {
        (1usize..=4, 1usize..120, 0i64..=400, 1usize..24)
            .prop_flat_map(|(k, n, span, cap)| {
                // Many zero steps, so several keys share a millisecond.
                let step = prop_oneof![2 => Just(0i64), 3 => 1i64..30];
                let one = (
                    0usize..KINDS.len(),
                    0u128..24,
                    prop::bool::weighted(0.8),
                    step,
                );
                let base = prop::collection::vec(one, n);
                let nodes = prop::collection::vec(
                    prop::collection::vec(prop::option::weighted(0.7, 0i64..=2_000), n),
                    k,
                );
                (base, nodes, Just(span), Just(cap))
            })
            .prop_map(move |(steps, mut nodes, span, cap)| {
                let mut at = 0;
                let base: Vec<(EventKey, i64)> = steps
                    .into_iter()
                    .map(|(kind, id, timed, step)| {
                        at += step;
                        let key = EventKey {
                            kind: KINDS[kind],
                            id,
                            ts_millis: timed.then_some(at),
                        };
                        (key, at)
                    })
                    .collect();
                let capacity = if bounded {
                    // Lateness within the span, and room for every key.
                    for d in nodes.iter_mut().flatten().flatten() {
                        *d %= span + 1;
                    }
                    base.len() + 1
                } else {
                    cap
                };
                Case {
                    base,
                    nodes,
                    span,
                    capacity,
                }
            })
    }

    /// Feed every arrival through a merger. Returns (key, verdict) per
    /// offer, and the merger.
    fn run(c: &Case, mutant: Mutant) -> (Vec<(EventKey, Verdict)>, Merger) {
        let mut arrivals: Vec<(i64, usize, usize)> = Vec::new();
        for (n, delays) in c.nodes.iter().enumerate() {
            for (i, d) in delays.iter().enumerate() {
                if let Some(d) = d {
                    arrivals.push((c.base[i].1 + d, n, i));
                }
            }
        }
        arrivals.sort_unstable();
        let mut m = Merger::new(c.nodes.len(), window(c.span, c.capacity)).with_mutant(mutant);
        let log = arrivals
            .iter()
            .map(|&(_, n, i)| {
                let key = c.base[i].0;
                (key, m.offer(n, key))
            })
            .collect();
        (log, m)
    }

    /// Holds at any lateness and any capacity: no timed key is delivered
    /// twice, a duplicate is only ever a copy of something delivered, no
    /// untimed key is late, every late timed key had a cause (it was a copy,
    /// it was behind the horizon, or `capacity` keys ordered after it were
    /// already delivered), and every counter adds up.
    fn check_safety(c: &Case, mutant: Mutant) -> Result<(), TestCaseError> {
        let (log, m) = run(c, mutant);
        let mut delivered: Set<EventKey> = Set::new();
        let mut timed_out: [Vec<(i64, EventKey)>; 3] = Default::default();
        let mut newest: [Option<i64>; 3] = [None; 3];
        let mut late = [0u64; 3];
        let mut untimed = [0u64; 3];
        let mut delivers = [0u64; 3];
        for &(key, v) in &log {
            let f = key.kind.feed().index();
            match (v, key.ts_millis) {
                (Verdict::Deliver, Some(t)) => {
                    prop_assert!(delivered.insert(key), "{:?} delivered twice", key);
                    timed_out[f].push((t, key));
                }
                (Verdict::Deliver, None) => {
                    delivered.insert(key);
                }
                (Verdict::Duplicate, _) => {
                    prop_assert!(
                        delivered.contains(&key),
                        "duplicate of undelivered {:?}",
                        key
                    );
                }
                (Verdict::Late, None) => {
                    return Err(TestCaseError::fail(format!("untimed {key:?} marked late")));
                }
                (Verdict::Late, Some(t)) => {
                    let copy = delivered.contains(&key);
                    let behind = newest[f].is_some_and(|n| t < n.saturating_sub(c.span));
                    let after = timed_out[f].iter().filter(|&&p| p > (t, key)).count();
                    prop_assert!(
                        copy || behind || after >= c.capacity,
                        "{:?} dropped as late with only {} keys after it",
                        key,
                        after
                    );
                    late[f] += 1;
                }
            }
            if v == Verdict::Deliver {
                delivers[f] += 1;
            }
            match key.ts_millis {
                Some(t) => newest[f] = Some(newest[f].map_or(t, |n| n.max(t))),
                None => untimed[f] += 1,
            }
        }
        for feed in Feed::ALL {
            let f = feed.index();
            let k = m.counters(feed);
            prop_assert_eq!(k.late, late[f]);
            prop_assert_eq!(k.untimed, untimed[f]);
            prop_assert_eq!(k.delivered, delivers[f]);
            // Every delivered key is in the window or left it exactly once.
            prop_assert_eq!(
                m.window_len(feed) as u64,
                k.delivered - k.expired - k.forced_evictions
            );
            prop_assert!(m.window_len(feed) <= 2 * c.capacity);
        }
        let t = m.totals();
        prop_assert_eq!(t.delivered + t.duplicates + t.late, log.len() as u64);
        Ok(())
    }

    /// Holds when lateness fits the window: every key any node received
    /// comes out, each timed key exactly once, nothing is late and the cap
    /// never bites.
    fn check_liveness(c: &Case, mutant: Mutant) -> Result<(), TestCaseError> {
        check_safety(c, mutant)?;
        let (log, m) = run(c, mutant);
        let union: Set<EventKey> = log.iter().map(|&(k, _)| k).collect();
        let delivered: Set<EventKey> = log
            .iter()
            .filter(|(_, v)| *v == Verdict::Deliver)
            .map(|&(k, _)| k)
            .collect();
        prop_assert_eq!(&delivered, &union);
        let t = m.totals();
        prop_assert_eq!(t.late, 0);
        prop_assert_eq!(t.forced_evictions, 0);
        Ok(())
    }

    proptest! {
        #![proptest_config(Config { cases: 512, ..Config::default() })]

        #[test]
        fn merge_delivers_the_union_once_when_lateness_fits(c in case(true)) {
            check_liveness(&c, Mutant::None)?;
        }

        #[test]
        fn merge_never_delivers_twice_at_any_lateness(c in case(false)) {
            check_safety(&c, Mutant::None)?;
        }
    }

    type Check = fn(&Case, Mutant) -> Result<(), TestCaseError>;

    fn caught(bounded: bool, check: Check, mutant: Mutant) -> bool {
        let cfg = Config {
            cases: 512,
            failure_persistence: None,
            ..Config::default()
        };
        let rng = TestRng::deterministic_rng(RngAlgorithm::ChaCha);
        let mut runner = TestRunner::new_with_rng(cfg, rng);
        runner.run(&case(bounded), |c| check(&c, mutant)).is_err()
    }

    #[test]
    fn the_properties_catch_broken_mergers() {
        // Control: the real merger passes on the same seed.
        assert!(!caught(true, check_liveness, Mutant::None));
        assert!(!caught(false, check_safety, Mutant::None));
        // Forgetting the horizon lets an expired key through a second time.
        assert!(caught(false, check_safety, Mutant::ForgetLate));
        // Half the span drops events that arrived within the real span.
        assert!(caught(true, check_liveness, Mutant::HalfSpan));
        // Without the floor, a key the cap pushed out comes through again.
        assert!(caught(false, check_safety, Mutant::NoFloor));
        // Blocking the whole millisecond drops keys that had no cause.
        assert!(caught(false, check_safety, Mutant::CoarseFloor));
    }
}
