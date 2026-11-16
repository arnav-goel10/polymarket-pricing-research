//! The node's bounded in-memory stream: a sequence number for every venue
//! frame, a ring of the most recent frames for resume, and the few frames a
//! client needs to resync when it cannot resume.
//!
//! Pure: no clock, no I/O. The node pushes every frame it receives, in
//! order, and asks for a [`Plan`] when a client subscribes.
//!
//! - `seq` starts at 1 per boot and is given only to frames that
//!   [`frame_keys`](crate::key::frame_keys) calls [`FrameKind::Venue`]. Noise
//!   and control records get no number and are not forwarded.
//! - The ring keeps the newest frames up to a frame cap and a byte cap,
//!   whichever binds first (64 MiB or 200,000 frames by default, about 80 s
//!   of the measured live rate). It always keeps the newest frame.
//! - Resync slots live outside the ring: the latest RTDS frame, the latest
//!   Binance frame of each kind (trade, book ticker), and the latest CLOB
//!   frame holding a `book` for each asset id. A control `subscribe` record
//!   clears the CLOB slots, because books for the old token no longer apply.
//! - [`Ring::plan`] resumes a client whose last sequence number is from this
//!   boot and still followed by retained frames. Anyone else gets a
//!   snapshot: the slot frames plus every retained CLOB frame after the
//!   oldest current book, in sequence order. The snapshot is `complete`
//!   only when no CLOB frame after that book has left the ring.

use crate::key::{EventKey, FrameKind, KeyKind};
use crate::wire::{Boot, ResumeFrom};
use feeds::recording::Source;
use serde_json::Value;
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RingConfig {
    pub max_bytes: usize,
    pub max_frames: usize,
}

impl Default for RingConfig {
    fn default() -> Self {
        Self {
            max_bytes: 64 * 1024 * 1024,
            max_frames: 200_000,
        }
    }
}

/// One forwarded frame. Cloning is cheap: the text is shared.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub seq: u64,
    pub src: Source,
    pub rx_micros: i64,
    pub frame: Arc<str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Plan {
    /// Continue at `next`; every frame from there on is still held.
    Resume { next: u64 },
    /// Send `frames` (in sequence order) as snapshot frames, then continue
    /// at `next`.
    Snapshot {
        frames: Vec<Entry>,
        next: u64,
        complete: bool,
    },
}

/// The connection's next sequence number is no longer held.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lagged {
    pub oldest: u64,
}

/// Deliberately broken variants, used only by the tests to show the
/// property test can fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) enum RingMutant {
    None,
    /// Skip a sequence number every 7th venue frame.
    SkipSeq,
    /// Resume any client of this boot, even when its frames are gone.
    StaleResume,
    /// Keep the CLOB slots across a subscribe record.
    KeepBooks,
    /// Snapshot the slots only, without the CLOB changes after the book.
    NoClobTail,
    /// Count Noise frames as venue frames.
    NumberNoise,
}

#[derive(Debug)]
pub struct Ring {
    cfg: RingConfig,
    boot: Boot,
    entries: VecDeque<Entry>,
    bytes: usize,
    head: u64,
    rtds: Option<Entry>,
    binance: [Option<Entry>; 2],
    books: BTreeMap<String, Entry>,
    mutant: RingMutant,
}

/// Asset ids of the `book` events in a CLOB frame (an object or an array).
fn book_assets(frame: &str) -> Vec<String> {
    let Ok(v) = serde_json::from_str::<Value>(frame) else {
        return Vec::new();
    };
    let one = |e: &Value| -> Option<String> {
        if e.get("event_type")?.as_str()? == "book" {
            Some(e.get("asset_id")?.as_str()?.to_string())
        } else {
            None
        }
    };
    match v.as_array() {
        Some(items) => items.iter().filter_map(one).collect(),
        None => one(&v).into_iter().collect(),
    }
}

fn is_subscribe(frame: &str) -> bool {
    serde_json::from_str::<Value>(frame).is_ok_and(|v| v.get("subscribe").is_some())
}

impl Ring {
    pub fn new(boot: Boot, cfg: RingConfig) -> Self {
        Ring {
            cfg,
            boot,
            // Preallocate the frame slots; frame text is allocated per frame.
            entries: VecDeque::with_capacity(cfg.max_frames.clamp(1, 1 << 20)),
            bytes: 0,
            head: 0,
            rtds: None,
            binance: [None, None],
            books: BTreeMap::new(),
            mutant: RingMutant::None,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_mutant(mut self, m: RingMutant) -> Self {
        self.mutant = m;
        self
    }

    pub fn boot(&self) -> Boot {
        self.boot
    }

    /// The newest sequence number given out, 0 before the first.
    pub fn head(&self) -> u64 {
        self.head
    }

    /// The oldest sequence number still held; `head + 1` when empty.
    pub fn oldest(&self) -> u64 {
        self.entries.front().map_or(self.head + 1, |e| e.seq)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Bytes of frame text held in the ring (slots not counted).
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Take one received frame. `kind` and `keys` are what
    /// [`frame_keys`](crate::key::frame_keys) returned for it. Returns the
    /// sequence number when the frame is forwarded.
    pub fn push(
        &mut self,
        src: Source,
        rx_micros: i64,
        frame: impl Into<Arc<str>>,
        kind: FrameKind,
        keys: &[EventKey],
    ) -> Option<u64> {
        let frame: Arc<str> = frame.into();
        let venue = match kind {
            FrameKind::Venue => true,
            FrameKind::Noise => self.mutant == RingMutant::NumberNoise,
            FrameKind::Control => false,
        };
        if !venue {
            if kind == FrameKind::Control
                && self.mutant != RingMutant::KeepBooks
                && is_subscribe(&frame)
            {
                self.books.clear();
            }
            return None;
        }
        self.head += 1;
        if self.mutant == RingMutant::SkipSeq && self.head.is_multiple_of(7) {
            self.head += 1;
        }
        let e = Entry {
            seq: self.head,
            src,
            rx_micros,
            frame,
        };
        match src {
            Source::Rtds => self.rtds = Some(e.clone()),
            Source::Binance => {
                for k in keys {
                    match k.kind {
                        KeyKind::BinanceTrade => self.binance[0] = Some(e.clone()),
                        KeyKind::BinanceBook => self.binance[1] = Some(e.clone()),
                        _ => {}
                    }
                }
            }
            Source::ClobBook => {
                for a in book_assets(&e.frame) {
                    self.books.insert(a, e.clone());
                }
            }
            Source::Control => {}
        }
        self.bytes += e.frame.len();
        self.entries.push_back(e);
        while self.entries.len() > 1
            && (self.entries.len() > self.cfg.max_frames || self.bytes > self.cfg.max_bytes)
        {
            let old = self.entries.pop_front().expect("non-empty");
            self.bytes -= old.frame.len();
        }
        Some(self.head)
    }

    /// Up to `max` held entries starting at `from`. Empty when `from` is
    /// past the head. `Err` when `from` is older than anything held.
    pub fn read_from(&self, from: u64, max: usize) -> Result<Vec<Entry>, Lagged> {
        let oldest = self.oldest();
        if from < oldest {
            return Err(Lagged { oldest });
        }
        let start = ((from - oldest) as usize).min(self.entries.len());
        Ok(self.entries.range(start..).take(max).cloned().collect())
    }

    /// How to serve a client that asks to resume from `resume`.
    pub fn plan(&self, resume: Option<ResumeFrom>) -> Plan {
        let oldest = self.oldest();
        if let Some(r) = resume {
            let held = if self.mutant == RingMutant::StaleResume {
                r.seq <= self.head
            } else {
                r.seq.checked_add(1).is_some_and(|n| n >= oldest) && r.seq <= self.head
            };
            if r.boot == self.boot && held {
                return Plan::Resume { next: r.seq + 1 };
            }
        }
        let mut frames: BTreeMap<u64, Entry> = BTreeMap::new();
        let slots = self
            .rtds
            .iter()
            .chain(self.binance.iter().flatten())
            .chain(self.books.values());
        for e in slots {
            frames.insert(e.seq, e.clone());
        }
        let first_book = self.books.values().map(|e| e.seq).min();
        let complete = first_book.is_none_or(|b| b + 1 >= oldest);
        if let (Some(b), false) = (first_book, self.mutant == RingMutant::NoClobTail) {
            let from = (b + 1).max(oldest);
            let tail = self.read_from(from, usize::MAX).unwrap_or_default();
            for e in tail.into_iter().filter(|e| e.src == Source::ClobBook) {
                frames.insert(e.seq, e);
            }
        }
        Plan::Snapshot {
            frames: frames.into_values().collect(),
            next: self.head + 1,
            complete,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::frame_keys;
    use proptest::prelude::*;
    use proptest::test_runner::{Config, RngAlgorithm, TestCaseError, TestRng, TestRunner};

    /// One step of a generated schedule.
    #[derive(Debug, Clone)]
    enum Op {
        Rtds,
        BinanceTrade,
        BinanceBook,
        /// A `book` for asset 0..3.
        Book(u8),
        /// Books for two assets in one array frame.
        TwoBooks(u8, u8),
        /// A price change for asset 0..3.
        Delta(u8),
        Subscribe,
        Noise,
        /// Ask for a plan. `back` frames behind the head, from this boot or
        /// another.
        Plan {
            back: u16,
            same_boot: bool,
        },
        /// Ask for a plan with no resume point.
        Fresh,
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            3 => Just(Op::Rtds),
            2 => Just(Op::BinanceTrade),
            2 => Just(Op::BinanceBook),
            2 => (0u8..3).prop_map(Op::Book),
            1 => (0u8..3, 0u8..3).prop_map(|(a, b)| Op::TwoBooks(a, b)),
            5 => (0u8..3).prop_map(Op::Delta),
            1 => Just(Op::Subscribe),
            2 => Just(Op::Noise),
            3 => (0u16..40, any::<bool>()).prop_map(|(back, same_boot)| Op::Plan { back, same_boot }),
            1 => Just(Op::Fresh),
        ]
    }

    #[derive(Debug, Clone)]
    struct Case {
        cfg: RingConfig,
        ops: Vec<Op>,
    }

    fn case() -> impl Strategy<Value = Case> {
        (
            1usize..24,
            60usize..2_000,
            prop::collection::vec(op(), 1..160),
        )
            .prop_map(|(max_frames, max_bytes, ops)| Case {
                cfg: RingConfig {
                    max_frames,
                    max_bytes,
                },
                ops,
            })
    }

    fn frame(op: &Op, i: usize) -> Option<(Source, String)> {
        Some(match op {
            Op::Rtds => (
                Source::Rtds,
                format!(
                    r#"{{"topic":"crypto_prices","type":"update","payload":{{"symbol":"btcusdt","value":{i}.5,"timestamp":{i}}}}}"#
                ),
            ),
            Op::BinanceTrade => (
                Source::Binance,
                format!(
                    r#"{{"stream":"btcusdt@aggTrade","data":{{"a":{i},"T":{i},"p":"1","q":"1","m":false}}}}"#
                ),
            ),
            Op::BinanceBook => (
                Source::Binance,
                format!(
                    r#"{{"stream":"btcusdt@bookTicker","data":{{"u":{i},"T":{i},"b":"1","B":"1","a":"2","A":"1"}}}}"#
                ),
            ),
            Op::Book(a) => (Source::ClobBook, book(*a, i)),
            Op::TwoBooks(a, b) => (
                Source::ClobBook,
                format!("[{},{}]", book(*a, i), book(*b, i)),
            ),
            Op::Delta(a) => (
                Source::ClobBook,
                format!(
                    r#"{{"event_type":"price_change","timestamp":"{i}","price_changes":[{{"asset_id":"a{a}","hash":"h{i}","side":"BUY","price":"0.5","size":"{i}"}}]}}"#
                ),
            ),
            Op::Subscribe => (Source::Control, r#"{"subscribe":"tok"}"#.to_string()),
            Op::Noise => (Source::Rtds, "PONG".to_string()),
            Op::Plan { .. } | Op::Fresh => return None,
        })
    }

    fn book(a: u8, i: usize) -> String {
        format!(
            r#"{{"event_type":"book","asset_id":"a{a}","hash":"b{i}","timestamp":"{i}","bids":[],"asks":[]}}"#
        )
    }

    /// The reference: every forwarded frame ever, with what a snapshot must
    /// hold, computed from the whole history rather than from the ring.
    #[derive(Default)]
    struct Model {
        all: Vec<Entry>,
        rtds: Option<u64>,
        binance: [Option<u64>; 2],
        books: BTreeMap<u8, u64>,
    }

    const BOOT: Boot = Boot(7);

    fn check(c: &Case, mutant: RingMutant) -> Result<(), TestCaseError> {
        let mut ring = Ring::new(BOOT, c.cfg).with_mutant(mutant);
        let mut m = Model::default();
        let mut keys = Vec::new();
        for (i, op) in c.ops.iter().enumerate() {
            if let Some((src, text)) = frame(op, i) {
                keys.clear();
                let kind = frame_keys(src, &text, &mut keys);
                let got = ring.push(src, i as i64, text.as_str(), kind, &keys);
                if kind != FrameKind::Venue {
                    prop_assert_eq!(got, None, "non-venue frame numbered");
                    if matches!(op, Op::Subscribe) {
                        m.books.clear();
                    }
                    continue;
                }
                let seq = m.all.len() as u64 + 1;
                prop_assert_eq!(got, Some(seq), "sequence must be 1, 2, 3, ...");
                m.all.push(Entry {
                    seq,
                    src,
                    rx_micros: i as i64,
                    frame: text.as_str().into(),
                });
                match op {
                    Op::Rtds => m.rtds = Some(seq),
                    Op::BinanceTrade => m.binance[0] = Some(seq),
                    Op::BinanceBook => m.binance[1] = Some(seq),
                    Op::Book(a) => {
                        m.books.insert(*a, seq);
                    }
                    Op::TwoBooks(a, b) => {
                        m.books.insert(*a, seq);
                        m.books.insert(*b, seq);
                    }
                    _ => {}
                }
            }
            check_retained(&ring, &m, c.cfg)?;
            let resume = match op {
                Op::Plan { back, same_boot } => Some(ResumeFrom {
                    boot: if *same_boot { BOOT } else { Boot(8) },
                    seq: ring.head().saturating_sub(*back as u64),
                }),
                Op::Fresh => None,
                _ => continue,
            };
            check_plan(&ring, &m, resume)?;
        }
        Ok(())
    }

    fn check_retained(ring: &Ring, m: &Model, cfg: RingConfig) -> Result<(), TestCaseError> {
        let head = m.all.len() as u64;
        prop_assert_eq!(ring.head(), head);
        let held = ring.read_from(ring.oldest(), usize::MAX).unwrap();
        // The ring is exactly a suffix of the history, newest included.
        let suffix = &m.all[m.all.len() - held.len()..];
        prop_assert_eq!(&held[..], suffix);
        if head > 0 {
            prop_assert!(!held.is_empty(), "the newest frame must be kept");
        }
        prop_assert!(held.len() <= cfg.max_frames.max(1));
        let bytes: usize = held.iter().map(|e| e.frame.len()).sum();
        prop_assert_eq!(ring.bytes(), bytes);
        prop_assert!(held.len() <= 1 || bytes <= cfg.max_bytes);
        // Nothing was dropped that both caps had room for.
        if let Some(prev) = (held.len() < m.all.len()).then(|| &m.all[m.all.len() - held.len() - 1])
        {
            prop_assert!(
                held.len() + 1 > cfg.max_frames || bytes + prev.frame.len() > cfg.max_bytes,
                "frame {} evicted with room to spare",
                prev.seq
            );
        }
        prop_assert!(
            ring.read_from(ring.oldest().saturating_sub(1), 1).is_err() || ring.oldest() == 1
        );
        Ok(())
    }

    fn check_plan(ring: &Ring, m: &Model, resume: Option<ResumeFrom>) -> Result<(), TestCaseError> {
        let head = m.all.len() as u64;
        let oldest = ring.oldest();
        let plan = ring.plan(resume);
        let resumable =
            resume.is_some_and(|r| r.boot == BOOT && r.seq + 1 >= oldest && r.seq <= head);
        match (&plan, resumable) {
            (Plan::Resume { next }, true) => {
                let r = resume.unwrap();
                prop_assert_eq!(*next, r.seq + 1);
                // Everything from `next` on is still there to send.
                let rest = ring.read_from(*next, usize::MAX);
                prop_assert!(rest.is_ok(), "resumed at {} but oldest is {}", next, oldest);
                prop_assert_eq!(rest.unwrap().len() as u64, head - r.seq);
            }
            (
                Plan::Snapshot {
                    frames,
                    next,
                    complete,
                },
                false,
            ) => {
                prop_assert_eq!(*next, head + 1);
                let seqs: Vec<u64> = frames.iter().map(|e| e.seq).collect();
                prop_assert!(
                    seqs.windows(2).all(|w| w[0] < w[1]),
                    "snapshot out of order"
                );
                for e in frames {
                    prop_assert_eq!(e, &m.all[e.seq as usize - 1], "snapshot frame differs");
                }
                let mut want: Vec<u64> = m
                    .rtds
                    .iter()
                    .chain(m.binance.iter().flatten())
                    .chain(m.books.values())
                    .copied()
                    .collect();
                let first_book = m.books.values().min().copied();
                if let Some(b) = first_book {
                    for e in &m.all[b as usize..] {
                        if e.src == Source::ClobBook && e.seq >= oldest {
                            want.push(e.seq);
                        }
                    }
                }
                want.sort_unstable();
                want.dedup();
                prop_assert_eq!(&seqs, &want, "snapshot holds the wrong frames");
                let lost = first_book.is_some_and(|b| {
                    m.all[b as usize..]
                        .iter()
                        .any(|e| e.src == Source::ClobBook && e.seq < oldest)
                });
                let gap = first_book.is_some_and(|b| b + 1 < oldest);
                prop_assert_eq!(*complete, !gap, "complete flag");
                prop_assert!(!lost || !*complete, "CLOB frames lost but complete");
            }
            (p, r) => return Err(TestCaseError::fail(format!(
                "resume {resume:?} with oldest {oldest} head {head}: resumable={r} but plan {p:?}"
            ))),
        }
        Ok(())
    }

    proptest! {
        #![proptest_config(Config { cases: 512, ..Config::default() })]

        #[test]
        fn ring_numbers_retains_and_plans_correctly(c in case()) {
            check(&c, RingMutant::None)?;
        }
    }

    fn caught(mutant: RingMutant) -> bool {
        let cfg = Config {
            cases: 512,
            failure_persistence: None,
            ..Config::default()
        };
        let rng = TestRng::deterministic_rng(RngAlgorithm::ChaCha);
        let mut runner = TestRunner::new_with_rng(cfg, rng);
        runner.run(&case(), |c| check(&c, mutant)).is_err()
    }

    #[test]
    fn the_property_catches_broken_rings() {
        // Control: the real ring passes on the same seed.
        assert!(!caught(RingMutant::None));
        // A skipped number is a gap the client would count.
        assert!(caught(RingMutant::SkipSeq));
        // Resuming past the ring's tail silently loses frames.
        assert!(caught(RingMutant::StaleResume));
        // Books from before a resubscribe resync the client to a dead token.
        assert!(caught(RingMutant::KeepBooks));
        // Books without the changes since leave the client's book stale.
        assert!(caught(RingMutant::NoClobTail));
        // A numbered keepalive is a frame the client cannot key.
        assert!(caught(RingMutant::NumberNoise));
    }

    #[test]
    fn resume_and_snapshot_by_hand() {
        let cfg = RingConfig {
            max_frames: 3,
            max_bytes: 1 << 20,
        };
        let mut r = Ring::new(BOOT, cfg);
        assert_eq!(
            r.plan(Some(ResumeFrom { boot: BOOT, seq: 0 })),
            Plan::Resume { next: 1 }
        );
        let mut keys = Vec::new();
        let mut push = |r: &mut Ring, src, text: &str| {
            keys.clear();
            let k = frame_keys(src, text, &mut keys);
            r.push(src, 0, text, k, &keys)
        };
        assert_eq!(push(&mut r, Source::ClobBook, &book(1, 1)), Some(1));
        let delta = r#"{"event_type":"price_change","timestamp":"2","price_changes":[{"asset_id":"a1","hash":"x","side":"BUY","price":"0.5","size":"1"}]}"#;
        assert_eq!(push(&mut r, Source::ClobBook, delta), Some(2));
        assert_eq!(push(&mut r, Source::Rtds, "PING"), None);
        assert_eq!(push(&mut r, Source::Control, r#"{"subscribe":"t2"}"#), None);
        assert_eq!(push(&mut r, Source::ClobBook, &book(2, 3)), Some(3));
        assert_eq!(push(&mut r, Source::ClobBook, delta), Some(4));
        // Frame 1 left the ring; resuming after it still works, before not.
        assert_eq!(r.oldest(), 2);
        assert_eq!(
            r.plan(Some(ResumeFrom { boot: BOOT, seq: 1 })),
            Plan::Resume { next: 2 }
        );
        assert!(matches!(
            r.plan(Some(ResumeFrom { boot: BOOT, seq: 0 })),
            Plan::Snapshot { .. }
        ));
        assert!(matches!(
            r.plan(Some(ResumeFrom { boot: BOOT, seq: 5 })),
            Plan::Snapshot { .. }
        ));
        assert!(matches!(
            r.plan(Some(ResumeFrom {
                boot: Boot(1),
                seq: 3
            })),
            Plan::Snapshot { .. }
        ));
        // The subscribe dropped the book for a1; the snapshot is a2's book
        // and the change after it.
        match r.plan(None) {
            Plan::Snapshot {
                frames,
                next,
                complete,
            } => {
                assert_eq!(frames.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![3, 4]);
                assert_eq!(next, 5);
                assert!(complete);
            }
            p => panic!("{p:?}"),
        }
        assert_eq!(r.read_from(1, 10), Err(Lagged { oldest: 2 }));
        assert_eq!(r.read_from(5, 10), Ok(vec![]));
    }
}
