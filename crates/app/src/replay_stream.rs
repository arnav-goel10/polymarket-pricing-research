//! Event-level replay: merge per-feed event streams into one time-ordered
//! stream and drive the hot path (book reconstruction, volatility update,
//! four-model re-price) on every event.
//!
//! Used by the throughput benchmark and the latency report. The generator
//! below produces a deterministic synthetic session (no network, no
//! credentials) with realistic message ratios: per simulated second about
//! one Chainlink tick, ten Binance ticks and twenty order-book deltas.

use feeds::binance::BinanceEvent;
use feeds::recording::{Record, Source};
use feeds::FeedSource;
use market::clob_book::ClobBookStream;
use market::SECONDS_PER_YEAR;
use poly_core::PricingContext;
use pricing::{
    BlackScholesDigital, CosVgDigital, DigitalModel, Ensemble, SharpDigital, StudentTDigital,
};
use stats::ewma::EwmaVar;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

/// One market-data event as it arrived from a feed.
#[derive(Debug, Clone, PartialEq)]
pub enum EventKind {
    /// Settlement reference price.
    Chainlink(f64),
    /// Fast-venue price used for volatility.
    Binance(f64),
    /// A raw order-book frame (JSON text), parsed during replay.
    BookFrame(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub ts_millis: i64,
    pub kind: EventKind,
}

/// K-way merge of per-feed streams (each already sorted by time) into one
/// stream ordered by timestamp. Ties keep feed order then arrival order, so
/// the output is deterministic. O(n log k).
pub fn merge_streams(streams: Vec<Vec<Event>>) -> Vec<Event> {
    let total = streams.iter().map(Vec::len).sum();
    let mut out = Vec::with_capacity(total);
    let mut iters: Vec<std::vec::IntoIter<Event>> =
        streams.into_iter().map(Vec::into_iter).collect();
    let mut heads: Vec<Option<Event>> = iters.iter_mut().map(Iterator::next).collect();
    let mut heap: BinaryHeap<Reverse<(i64, usize)>> = heads
        .iter()
        .enumerate()
        .filter_map(|(i, h)| h.as_ref().map(|e| Reverse((e.ts_millis, i))))
        .collect();
    while let Some(Reverse((_, i))) = heap.pop() {
        if let Some(ev) = heads[i].take() {
            out.push(ev);
        }
        heads[i] = iters[i].next();
        if let Some(e) = &heads[i] {
            heap.push(Reverse((e.ts_millis, i)));
        }
    }
    out
}

/// Window length of the replayed contract.
pub const WINDOW_MILLIS: i64 = 300_000;

/// State carried across events: the reconstructed book, the volatility
/// estimate, the window reference price and the model ensemble.
pub struct Pipeline {
    book: ClobBookStream,
    ewma: EwmaVar,
    last_binance: Option<f64>,
    last_chainlink: Option<f64>,
    window_start: Option<i64>,
    s0: f64,
    ensemble: Ensemble,
    pub repriced: u64,
    pub resyncs: u64,
    /// Frames from a recording that no parser accepted.
    pub ignored: u64,
    last_vol_ts: i64,
    time_scaled_vol: bool,
    /// Latest model probability minus the book mid.
    pub last_edge: f64,
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::new()
    }
}

impl Pipeline {
    pub fn new() -> Self {
        let models: Vec<Box<dyn DigitalModel>> = vec![
            Box::new(BlackScholesDigital),
            Box::new(StudentTDigital { nu: 4.0 }),
            Box::new(SharpDigital::new(2.5)),
            Box::new(CosVgDigital::new(64)),
        ];
        Self {
            book: ClobBookStream::new("tok"),
            ewma: EwmaVar::new(0.97),
            last_binance: None,
            last_chainlink: None,
            window_start: None,
            s0: 0.0,
            ensemble: Ensemble::new(models),
            repriced: 0,
            resyncs: 0,
            ignored: 0,
            last_vol_ts: 0,
            time_scaled_vol: false,
            last_edge: 0.0,
        }
    }

    /// Apply one event and, when inputs are ready, re-price the contract.
    /// Returns the ensemble probability if a re-price happened.
    pub fn on_event(&mut self, ev: &Event) -> Option<f64> {
        match &ev.kind {
            EventKind::Chainlink(p) => {
                let start = ev.ts_millis - ev.ts_millis.rem_euclid(WINDOW_MILLIS);
                if self.window_start != Some(start) {
                    self.window_start = Some(start);
                    self.s0 = *p;
                }
                self.last_chainlink = Some(*p);
            }
            EventKind::Binance(p) => {
                if let Some(prev) = self.last_binance {
                    self.ewma.update((p / prev).ln());
                }
                self.last_binance = Some(*p);
            }
            EventKind::BookFrame(txt) => {
                if self.book.on_text(txt, |_| {}).is_err() {
                    self.resyncs += 1;
                }
            }
        }
        self.reprice(ev.ts_millis)
    }

    /// Apply one raw recorded frame through the real feed parsers and
    /// re-price. Volatility uses time-scaled returns, since real frames
    /// arrive irregularly.
    pub fn on_record(&mut self, rec: &Record) -> Option<f64> {
        let now = rec.recv_micros / 1000;
        match rec.source {
            Source::Rtds => {
                let ticks = feeds::rtds::parse_frame(&rec.frame);
                if ticks.is_empty() {
                    self.ignored += 1;
                }
                for t in ticks {
                    if t.source == FeedSource::Chainlink {
                        self.on_event(&Event {
                            ts_millis: now,
                            kind: EventKind::Chainlink(t.price),
                        });
                    }
                }
            }
            Source::Binance => match feeds::binance::parse_message(&rec.frame) {
                Some(ev) => {
                    let px = match ev {
                        BinanceEvent::Trade(t) => t.price,
                        BinanceEvent::Book(b) => 0.5 * (b.bid + b.ask),
                    };
                    if let Some(prev) = self.last_binance {
                        let dt = (now - self.last_vol_ts) as f64 / 1000.0;
                        if dt > 0.0 && px > 0.0 {
                            self.ewma.update((px / prev).ln() / dt.sqrt());
                            self.last_binance = Some(px);
                            self.last_vol_ts = now;
                        }
                    } else {
                        self.last_binance = Some(px);
                        self.last_vol_ts = now;
                    }
                }
                None => self.ignored += 1,
            },
            Source::ClobBook => {
                if self.book.on_text(&rec.frame, |_| {}).is_err() {
                    self.resyncs += 1;
                }
            }
            Source::Control => {
                if let Some(token) = serde_json::from_str::<serde_json::Value>(&rec.frame)
                    .ok()
                    .and_then(|v| v.get("subscribe")?.as_str().map(str::to_owned))
                {
                    self.book = ClobBookStream::new(token);
                }
                return None;
            }
        }
        self.time_scaled_vol = true;
        self.reprice(now)
    }

    pub fn book_mid(&self) -> Option<f64> {
        self.book.local_book().book().mid()
    }

    fn reprice(&mut self, now: i64) -> Option<f64> {
        let s_t = self.last_chainlink?;
        let start = self.window_start?;
        let remaining = (start + WINDOW_MILLIS - now).max(1) as f64 / 1000.0;
        // Synthetic Binance ticks arrive every 100 ms; recorded returns are
        // already scaled to one second.
        let per_year = if self.time_scaled_vol {
            SECONDS_PER_YEAR
        } else {
            SECONDS_PER_YEAR / 0.1
        };
        let sigma = (self.ewma.vol() * per_year.sqrt()).clamp(0.05, 5.0);
        let ctx = PricingContext {
            s_t,
            s0: self.s0,
            tau: remaining / SECONDS_PER_YEAR,
            mu: 0.0,
            sigma,
            skew: 0.0,
            kurt: 3.0,
        };
        let p = self.ensemble.price(&ctx);
        if let Some(mid) = self.book.local_book().book().mid() {
            self.last_edge = p - mid;
        }
        self.repriced += 1;
        Some(p)
    }
}

/// Deterministic xorshift64* generator (keeps the fixture reproducible
/// without a dependency).
struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// Generate a synthetic session of roughly `n_events` events as three
/// per-feed streams (Chainlink, Binance, book), each sorted by time.
pub fn generate_session(seed: u64, n_events: usize) -> Vec<Vec<Event>> {
    let mut rng = Rng(seed | 1);
    // 31 events per simulated second: 1 Chainlink, 10 Binance, 20 book.
    let seconds = (n_events / 31).max(1) as i64;
    let t0: i64 = 1_767_225_600_000; // aligned to a window boundary
    let mut chainlink = Vec::with_capacity(seconds as usize);
    let mut binance = Vec::with_capacity(seconds as usize * 10);
    let mut book = Vec::with_capacity(seconds as usize * 20 + 1);

    let mut px = 100_000.0f64;
    for i in 0..seconds * 10 {
        px *= 1.0 + (rng.unit() - 0.5) * 4e-4;
        let ts = t0 + i * 100 + (rng.next_u64() % 7) as i64;
        binance.push(Event {
            ts_millis: ts,
            kind: EventKind::Binance(px),
        });
        if i % 10 == 0 {
            chainlink.push(Event {
                ts_millis: ts + 40,
                kind: EventKind::Chainlink(px),
            });
        }
    }

    let levels = |lo: u32, hi: u32| {
        (lo..=hi)
            .map(|t| format!(r#"{{"price":"0.{t:02}","size":"{}"}}"#, 100 + t))
            .collect::<Vec<_>>()
            .join(",")
    };
    book.push(Event {
        ts_millis: t0,
        kind: EventKind::BookFrame(format!(
            r#"{{"event_type":"book","asset_id":"tok","timestamp":"{t0}","bids":[{}],"asks":[{}]}}"#,
            levels(30, 49),
            levels(51, 70)
        )),
    });
    for i in 0..seconds * 20 {
        let ts = t0 + 1 + i * 50;
        let bid = rng.unit() < 0.5;
        let tick = if bid {
            30 + rng.next_u64() % 20
        } else {
            51 + rng.next_u64() % 20
        };
        // One in eight updates removes the level; the rest resize it.
        let size = if rng.next_u64().is_multiple_of(8) {
            0
        } else {
            1 + rng.next_u64() % 500
        };
        let side = if bid { "BUY" } else { "SELL" };
        book.push(Event {
            ts_millis: ts,
            kind: EventKind::BookFrame(format!(
                r#"{{"event_type":"price_change","timestamp":"{ts}","price_changes":[{{"asset_id":"tok","price":"0.{tick:02}","size":"{size}","side":"{side}"}}]}}"#
            )),
        });
    }
    vec![chainlink, binance, book]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_is_time_ordered_complete_and_stable() {
        let a = vec![
            Event {
                ts_millis: 1,
                kind: EventKind::Chainlink(1.0),
            },
            Event {
                ts_millis: 5,
                kind: EventKind::Chainlink(2.0),
            },
        ];
        let b = vec![
            Event {
                ts_millis: 1,
                kind: EventKind::Binance(9.0),
            },
            Event {
                ts_millis: 3,
                kind: EventKind::Binance(8.0),
            },
        ];
        let out = merge_streams(vec![a, b, Vec::new()]);
        let ts: Vec<i64> = out.iter().map(|e| e.ts_millis).collect();
        assert_eq!(ts, vec![1, 1, 3, 5]);
        assert_eq!(
            out[0].kind,
            EventKind::Chainlink(1.0),
            "ties keep feed order"
        );
    }

    #[test]
    fn replays_raw_recorded_frames_through_the_real_parsers() {
        let rec = |ms: i64, source, frame: &str| Record {
            recv_micros: ms * 1000,
            source,
            frame: frame.to_string(),
        };
        let t0 = 1_767_225_600_000;
        let frames = [
            rec(t0, Source::Control, r#"{"subscribe":"tok"}"#),
            rec(
                t0 + 1,
                Source::ClobBook,
                r#"{"event_type":"book","asset_id":"tok","timestamp":"1","bids":[{"price":"0.48","size":"10"}],"asks":[{"price":"0.52","size":"10"}]}"#,
            ),
            rec(
                t0 + 2,
                Source::Binance,
                r#"{"stream":"btcusdt@aggTrade","data":{"p":"100000","q":"1","m":false,"T":1}}"#,
            ),
            rec(
                t0 + 900,
                Source::Binance,
                r#"{"stream":"btcusdt@aggTrade","data":{"p":"100010","q":"1","m":false,"T":2}}"#,
            ),
            rec(
                t0 + 1000,
                Source::Rtds,
                r#"{"topic":"crypto_prices_chainlink","type":"update","payload":{"value":100005.0,"timestamp":1}}"#,
            ),
            rec(
                t0 + 1001,
                Source::ClobBook,
                r#"{"event_type":"price_change","timestamp":"2","price_changes":[{"asset_id":"tok","price":"0.49","size":"5","side":"BUY","best_bid":"0.49","best_ask":"0.52"}]}"#,
            ),
            rec(t0 + 1002, Source::Rtds, "{garbage"),
        ];
        let mut p = Pipeline::new();
        let probs: Vec<f64> = frames.iter().filter_map(|r| p.on_record(r)).collect();
        assert_eq!(p.resyncs, 0);
        assert_eq!(p.ignored, 1);
        assert_eq!(probs.len(), 3, "re-priced once the reference price exists");
        assert!(probs.iter().all(|x| (0.0..=1.0).contains(x)));
        assert_eq!(p.book_mid(), Some(0.505));
    }

    #[test]
    fn generated_session_replays_without_resyncs() {
        let streams = generate_session(7, 31_000);
        let n: usize = streams.iter().map(Vec::len).sum();
        let merged = merge_streams(streams);
        assert_eq!(merged.len(), n);
        assert!(merged.windows(2).all(|w| w[0].ts_millis <= w[1].ts_millis));
        let mut p = Pipeline::new();
        let mut last = None;
        for ev in &merged {
            if let Some(prob) = p.on_event(ev) {
                assert!((0.0..=1.0).contains(&prob));
                last = Some(prob);
            }
        }
        assert!(last.is_some());
        assert_eq!(p.resyncs, 0);
        assert!(p.repriced as usize > n * 9 / 10);
    }
}
