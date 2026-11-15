//! Merge the event streams of N nodes into one, delivering each key once.
//!
//! Each feed has its own window: a set of recently delivered keys plus their
//! venue timestamps in arrival order. A key already in the set is a
//! duplicate. A key not in the set is new, unless its venue timestamp is
//! older than the window's horizon, in which case it is late.
//!
//! The horizon is `max venue timestamp seen - span`, raised further whenever
//! the capacity cap forces out a key that was still inside the span. Every
//! key that leaves the window has a timestamp below the horizon at that
//! moment, and the horizon never moves back, so a key at or above the
//! horizon that is not in the set has never been delivered. That gives the
//! guarantee:
//!
//! - Never twice. A timed key is delivered at most once, however late its
//!   copies arrive. A copy that arrives after its key left the window is
//!   dropped and counted as late, never passed on as new.
//! - Nothing lost while lateness fits. If every node delivers each event
//!   within `span` of the newest venue timestamp the merger has seen for
//!   that feed, and the capacity holds every key inside one span, then every
//!   event any node delivered comes out exactly once and `late` stays zero.
//!
//! Late events are dropped, not delivered, because a late copy cannot be
//! told apart from a duplicate of a key that has left the window. The
//! counter makes the loss visible; size `span` above the worst node lag.
//!
//! Events without a venue timestamp (unkeyed raw frames) are placed at the
//! current newest timestamp and are never late. They are deduplicated while
//! their key is in the window and can come out twice after that. They are
//! counted as `untimed`.
//!
//! Memory is fixed at construction: each window preallocates its capacity
//! and never grows. The merger is deterministic: the same arrivals in the
//! same order give the same verdicts, since the hash set is only ever asked
//! about membership.

use crate::key::{EventKey, Feed, KeyBuildHasher, Stamped};
use std::collections::{HashSet, VecDeque};

/// One feed's dedup window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WindowConfig {
    /// How far behind the newest venue timestamp an event may arrive and
    /// still be accepted.
    pub span_millis: i64,
    /// The most keys held at once. Keep it above the event rate times the
    /// span, or the cap shrinks the effective span (counted as
    /// `forced_evictions`).
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
    /// Keys pushed out by the capacity cap while still inside the span.
    pub forced_evictions: u64,
    /// Offered events that had no venue timestamp.
    pub untimed: u64,
}

impl FeedCounters {
    fn add(&mut self, o: &FeedCounters) {
        self.delivered += o.delivered;
        self.duplicates += o.duplicates;
        self.late += o.late;
        self.forced_evictions += o.forced_evictions;
        self.untimed += o.untimed;
    }
}

#[derive(Debug)]
struct Window {
    cfg: WindowConfig,
    seen: HashSet<EventKey, KeyBuildHasher>,
    order: VecDeque<(i64, EventKey)>,
    max_seen: Option<i64>,
    /// Raised past every key the cap forced out.
    floor: i64,
}

impl Window {
    fn new(cfg: WindowConfig) -> Self {
        let cfg = WindowConfig {
            capacity: cfg.capacity.max(1),
            span_millis: cfg.span_millis.max(0),
        };
        Window {
            cfg,
            seen: HashSet::with_capacity_and_hasher(cfg.capacity, KeyBuildHasher::default()),
            order: VecDeque::with_capacity(cfg.capacity),
            max_seen: None,
            floor: i64::MIN,
        }
    }

    fn horizon(&self, span: i64) -> i64 {
        match self.max_seen {
            Some(m) => m.saturating_sub(span).max(self.floor),
            None => self.floor,
        }
    }
}

/// Test-only broken variants, used to show the property tests can fail.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mutant {
    None,
    /// Treat every key missing from the set as new, ignoring the horizon.
    ForgetLate,
    /// Use half the configured span.
    HalfSpan,
}

/// Merges keyed events from several nodes.
#[derive(Debug)]
pub struct Merger {
    windows: [Window; 3],
    counters: [FeedCounters; 3],
    first_from: Vec<u64>,
    #[cfg(test)]
    mutant: Mutant,
}

impl Merger {
    pub fn new(nodes: usize, cfg: MergeConfig) -> Self {
        Merger {
            windows: Feed::ALL.map(|f| Window::new(cfg.window(f))),
            counters: [FeedCounters::default(); 3],
            first_from: vec![0; nodes],
            #[cfg(test)]
            mutant: Mutant::None,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_mutant(mut self, m: Mutant) -> Self {
        self.mutant = m;
        self
    }

    /// The span to use and whether to apply the late check.
    #[cfg(not(test))]
    fn policy(&self, i: usize) -> (i64, bool) {
        (self.windows[i].cfg.span_millis, true)
    }

    #[cfg(test)]
    fn policy(&self, i: usize) -> (i64, bool) {
        let span = self.windows[i].cfg.span_millis;
        match self.mutant {
            Mutant::None => (span, true),
            Mutant::ForgetLate => (span, false),
            Mutant::HalfSpan => (span / 2, true),
        }
    }

    /// Offer one event that `node` received. Returns what happened to it.
    pub fn offer(&mut self, node: usize, ev: Stamped) -> Verdict {
        let i = ev.key.kind.feed().index();
        let (span, check_late) = self.policy(i);
        let w = &mut self.windows[i];
        let c = &mut self.counters[i];

        if w.seen.contains(&ev.key) {
            c.duplicates += 1;
            return Verdict::Duplicate;
        }
        let t = match ev.venue_ts_millis {
            Some(t) => {
                if check_late && t < w.horizon(span) {
                    c.late += 1;
                    return Verdict::Late;
                }
                t
            }
            None => {
                c.untimed += 1;
                w.max_seen.unwrap_or(w.floor)
            }
        };

        if w.seen.len() >= w.cfg.capacity {
            if let Some((ft, fk)) = w.order.pop_front() {
                w.seen.remove(&fk);
                w.floor = w.floor.max(ft.saturating_add(1));
                c.forced_evictions += 1;
            }
        }
        w.seen.insert(ev.key);
        w.order.push_back((t, ev.key));
        if ev.venue_ts_millis.is_some() {
            w.max_seen = Some(w.max_seen.map_or(t, |m| m.max(t)));
        }

        // Expire from the front. Out-of-order arrivals behind a newer front
        // stay a little longer, which is safe and still capped.
        let h = w.horizon(span);
        while let Some(&(ft, fk)) = w.order.front() {
            if ft >= h {
                break;
            }
            w.order.pop_front();
            w.seen.remove(&fk);
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

    /// Keys currently held for a feed.
    pub fn window_len(&self, feed: Feed) -> usize {
        self.windows[feed.index()].seen.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::KeyKind;
    use proptest::prelude::*;
    use proptest::test_runner::{Config, RngAlgorithm, TestCaseError, TestRng, TestRunner};
    use std::collections::HashSet as Set;

    const KINDS: [KeyKind; 3] = [
        KeyKind::RtdsPrice,
        KeyKind::BinanceBook,
        KeyKind::ClobPriceChange,
    ];

    fn ev(feed: usize, id: u128, ts: i64) -> Stamped {
        Stamped {
            key: EventKey {
                kind: KINDS[feed],
                id,
            },
            venue_ts_millis: Some(ts),
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
        // Node 1 is far behind and sends key 1 again.
        assert_eq!(m.offer(1, ev(2, 1, 0)), Verdict::Late);
        let c = m.counters(Feed::Clob);
        assert_eq!((c.delivered, c.duplicates, c.late), (2, 0, 1));
        // Other feeds are untouched.
        assert_eq!(m.counters(Feed::Rtds), FeedCounters::default());
    }

    #[test]
    fn the_capacity_cap_raises_the_horizon_so_forced_out_keys_stay_caught() {
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
    fn untimed_events_are_counted_and_never_late() {
        let mut m = Merger::new(1, window(10, 8));
        let k = Stamped {
            key: EventKey {
                kind: KeyKind::Unkeyed(Feed::Rtds),
                id: 42,
            },
            venue_ts_millis: None,
        };
        assert_eq!(m.offer(0, ev(0, 1, 5_000)), Verdict::Deliver);
        assert_eq!(m.offer(0, k), Verdict::Deliver);
        assert_eq!(m.offer(0, k), Verdict::Duplicate);
        assert_eq!(m.counters(Feed::Rtds).untimed, 2);
    }

    #[test]
    fn memory_stays_within_capacity_under_any_rate() {
        let mut m = Merger::new(1, window(i64::MAX, 64));
        for id in 0..10_000u128 {
            m.offer(0, ev(1, id, 0));
            assert!(m.window_len(Feed::Binance) <= 64);
        }
    }

    /// k nodes, each holding an arbitrary subset of a base stream, each
    /// event delayed by its own amount. Arrival order is venue time plus
    /// delay, which gives an arbitrary interleaving across nodes.
    #[derive(Debug, Clone)]
    struct Case {
        /// (feed, venue timestamp), timestamps non-decreasing.
        base: Vec<(usize, i64)>,
        /// nodes[n][i] = delay of base event i at node n, or None if node n
        /// never got it.
        nodes: Vec<Vec<Option<i64>>>,
        span: i64,
        capacity: usize,
    }

    fn case(bounded: bool) -> impl Strategy<Value = Case> {
        (1usize..=4, 1usize..120, 0i64..=400, 1usize..48)
            .prop_flat_map(|(k, n, span, cap)| {
                let base = prop::collection::vec((0usize..3, 0i64..30), n);
                let nodes = prop::collection::vec(
                    prop::collection::vec(prop::option::weighted(0.7, 0i64..=2_000), n),
                    k,
                );
                (base, nodes, Just(span), Just(cap))
            })
            .prop_map(move |(steps, mut nodes, span, cap)| {
                let mut ts = 0;
                let base: Vec<(usize, i64)> = steps
                    .into_iter()
                    .map(|(f, step)| {
                        ts += step;
                        (f, ts)
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

    /// Feed every arrival through a merger. Returns (base index, verdict)
    /// per offer, and the merger.
    fn run(c: &Case, mutant: Mutant) -> (Vec<(usize, Verdict)>, Merger) {
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
                let (feed, ts) = c.base[i];
                (i, m.offer(n, ev(feed, i as u128, ts)))
            })
            .collect();
        (log, m)
    }

    /// Holds for any lateness: nothing is delivered twice, a duplicate is
    /// only ever a copy of something delivered, and every offer is counted.
    fn check_safety(c: &Case, mutant: Mutant) -> Result<(), TestCaseError> {
        let (log, m) = run(c, mutant);
        let mut delivered = Set::new();
        let mut late = [0u64; 3];
        for &(i, v) in &log {
            match v {
                Verdict::Deliver => {
                    prop_assert!(delivered.insert(i), "event {} delivered twice", i);
                }
                Verdict::Duplicate => {
                    prop_assert!(delivered.contains(&i), "duplicate of undelivered {}", i);
                }
                Verdict::Late => late[c.base[i].0] += 1,
            }
        }
        for (f, feed) in Feed::ALL.iter().enumerate() {
            prop_assert_eq!(m.counters(*feed).late, late[f]);
        }
        let t = m.totals();
        prop_assert_eq!(t.delivered, delivered.len() as u64);
        prop_assert_eq!(t.delivered + t.duplicates + t.late, log.len() as u64);
        Ok(())
    }

    /// Holds when lateness fits the window: the output is exactly the union
    /// of what the nodes received, each once.
    fn check_liveness(c: &Case, mutant: Mutant) -> Result<(), TestCaseError> {
        check_safety(c, mutant)?;
        let (log, m) = run(c, mutant);
        let union: Set<usize> = c
            .nodes
            .iter()
            .flat_map(|d| d.iter().enumerate().filter(|(_, x)| x.is_some()))
            .map(|(i, _)| i)
            .collect();
        let delivered: Set<usize> = log
            .iter()
            .filter(|(_, v)| *v == Verdict::Deliver)
            .map(|&(i, _)| i)
            .collect();
        prop_assert_eq!(&delivered, &union);
        prop_assert_eq!(m.totals().late, 0);
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
    }
}
