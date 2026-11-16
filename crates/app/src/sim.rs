//! Frames for the venue simulator (`bin/venue_sim`): a synthetic generator
//! deterministic from a seed, and the greeting a real venue sends to a new
//! subscriber.
//!
//! The generator's content (trade and book ids, prices, sizes, hashes, which
//! asset a change is for) is a pure function of the seed and the frame
//! count. Venue timestamps are the wall clock in milliseconds when a frame
//! is made, so the HA harness can line them up with its own clock.
//!
//! Greetings. On every new RTDS connection the venue sends, per price
//! series, one `payload.data[]` snapshot of the last [`RTDS_HISTORY`]
//! points; on every new CLOB connection, the latest `book` frame of each
//! asset. Both are copies of frames already broadcast, so their keys are
//! already in the truth. A node that reconnects therefore gets old events
//! again, which the client's merger has to drop as duplicates or as late.

use feeds::recording::Source;
use serde_json::Value;
use std::collections::{BTreeMap, VecDeque};

/// Points per RTDS series in a greeting.
pub const RTDS_HISTORY: usize = 60;
/// The fast RTDS series ticks this often, the slow one four times slower,
/// so a greeting covers 15 s of the fast series, more than the merger's
/// 10 s span.
pub const RTDS_TICK_MS: i64 = 250;
/// The two CLOB assets. Nodes follow the first.
pub const ASSETS: [&str; 2] = ["tokA", "tokB"];
/// A fresh `book` for an asset every this many CLOB frames for it.
const BOOK_EVERY: u64 = 25;

/// splitmix64.
#[derive(Debug, Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Uniform in `0..n`.
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n.max(1)
    }
}

/// The synthetic venues.
#[derive(Debug, Clone)]
pub struct Synth {
    rng: Rng,
    agg_id: u64,
    book_u: u64,
    /// Integer price in cents.
    btc_cents: i64,
    clob_frames: [u64; 2],
    rtds_points: u64,
}

impl Synth {
    pub fn new(seed: u64) -> Self {
        Synth {
            rng: Rng::new(seed),
            agg_id: 1_000_000,
            book_u: 5_000_000,
            btc_cents: 10_000_000,
            clob_frames: [0; 2],
            rtds_points: 0,
        }
    }

    fn walk(&mut self) -> String {
        self.btc_cents += self.rng.below(201) as i64 - 100;
        format!("{}.{:02}", self.btc_cents / 100, self.btc_cents % 100)
    }

    /// One Binance frame: an aggregate trade or a book ticker, half each.
    pub fn binance(&mut self, now_ms: i64) -> String {
        let p = self.walk();
        let q = 1 + self.rng.below(5_000);
        if self.rng.below(2) == 0 {
            self.agg_id += 1;
            let a = self.agg_id;
            let m = self.rng.below(2) == 0;
            format!(
                r#"{{"stream":"btcusdt@aggTrade","data":{{"e":"aggTrade","E":{now_ms},"s":"BTCUSDT","a":{a},"p":"{p}","q":"0.{q:04}","f":{a},"l":{a},"T":{now_ms},"m":{m}}}}}"#
            )
        } else {
            self.book_u += 1 + self.rng.below(3);
            let u = self.book_u;
            let bq = 1 + self.rng.below(9_000);
            format!(
                r#"{{"stream":"btcusdt@bookTicker","data":{{"e":"bookTicker","u":{u},"s":"BTCUSDT","b":"{p}","B":"{q}.000","a":"{p}","A":"{bq}.000","T":{now_ms},"E":{now_ms}}}}}"#
            )
        }
    }

    /// One CLOB frame: the first frame for each asset and every 25th after
    /// it is a `book`, the rest a one or two entry `price_change`. Bids stay
    /// at 0.40 to 0.45 and asks at 0.55 to 0.60, so the book never crosses.
    pub fn clob(&mut self, now_ms: i64) -> String {
        let i = (self.rng.below(10) >= 7) as usize;
        let asset = ASSETS[i];
        let n = self.clob_frames[i];
        self.clob_frames[i] += 1;
        let hash = self.rng.next_u64();
        if n.is_multiple_of(BOOK_EVERY) {
            let level = |r: &mut Rng, p: u64| {
                format!(
                    r#"{{"price":"0.{p:02}","size":"{}.{:02}"}}"#,
                    1 + r.below(500),
                    r.below(100)
                )
            };
            let bids: Vec<String> = (40..=45).map(|p| level(&mut self.rng, p)).collect();
            let asks: Vec<String> = (55..=60).map(|p| level(&mut self.rng, p)).collect();
            return format!(
                r#"{{"event_type":"book","asset_id":"{asset}","market":"0xsim","hash":"0x{hash:016x}","timestamp":"{now_ms}","bids":[{}],"asks":[{}]}}"#,
                bids.join(","),
                asks.join(",")
            );
        }
        let entries = 1 + self.rng.below(2);
        let mut changes = Vec::new();
        for e in 0..entries {
            let buy = self.rng.below(2) == 0;
            let (side, p) = if buy {
                ("BUY", 40 + self.rng.below(6))
            } else {
                ("SELL", 55 + self.rng.below(6))
            };
            let size = format!("{}.{:02}", 1 + self.rng.below(500), self.rng.below(100));
            changes.push(format!(
                r#"{{"asset_id":"{asset}","price":"0.{p:02}","size":"{size}","side":"{side}","hash":"0x{h:016x}"}}"#,
                h = hash ^ e
            ));
        }
        format!(
            r#"{{"event_type":"price_change","market":"0xsim","timestamp":"{now_ms}","price_changes":[{}]}}"#,
            changes.join(",")
        )
    }

    /// One RTDS update. Every fourth point is on the Chainlink series.
    pub fn rtds(&mut self, now_ms: i64) -> String {
        let chainlink = self.rtds_points % 4 == 3;
        self.rtds_points += 1;
        let v = self.walk();
        let (topic, symbol) = if chainlink {
            ("crypto_prices_chainlink", "btc/usd")
        } else {
            ("crypto_prices", "btcusdt")
        };
        format!(
            r#"{{"topic":"{topic}","type":"update","timestamp":{now_ms},"payload":{{"symbol":"{symbol}","timestamp":{now_ms},"value":{v}}}}}"#
        )
    }
}

/// What a venue sends a new subscriber, kept up to date from every
/// broadcast frame.
#[derive(Debug, Default)]
pub struct Greeter {
    rtds: BTreeMap<(String, String), VecDeque<(i64, Value)>>,
    books: BTreeMap<String, std::sync::Arc<str>>,
}

const PRICE_TOPICS: [&str; 2] = ["crypto_prices", "crypto_prices_chainlink"];

impl Greeter {
    pub fn observe(&mut self, src: Source, frame: &str) {
        match src {
            Source::Rtds => self.observe_rtds(frame),
            Source::ClobBook => self.observe_clob(frame),
            _ => {}
        }
    }

    fn observe_rtds(&mut self, frame: &str) {
        let Ok(v) = serde_json::from_str::<Value>(frame) else {
            return;
        };
        let (Some(topic), Some(payload)) =
            (v.get("topic").and_then(Value::as_str), v.get("payload"))
        else {
            return;
        };
        if !PRICE_TOPICS.contains(&topic) {
            return;
        }
        let Some(symbol) = payload.get("symbol").and_then(Value::as_str) else {
            return;
        };
        let series = self
            .rtds
            .entry((topic.to_string(), symbol.to_string()))
            .or_default();
        let mut add = |p: &Value| {
            if let (Some(t), Some(val)) =
                (p.get("timestamp").and_then(Value::as_i64), p.get("value"))
            {
                if val.is_number() {
                    if series.len() >= RTDS_HISTORY {
                        series.pop_front();
                    }
                    series.push_back((t, val.clone()));
                }
            }
        };
        match payload.get("data").and_then(Value::as_array) {
            Some(points) => points.iter().for_each(&mut add),
            None => add(payload),
        }
    }

    fn observe_clob(&mut self, frame: &str) {
        let Ok(v) = serde_json::from_str::<Value>(frame) else {
            return;
        };
        let items = match &v {
            Value::Array(a) => a.iter().collect::<Vec<_>>(),
            other => vec![other],
        };
        let shared: std::sync::Arc<str> = frame.into();
        for e in items {
            if e.get("event_type").and_then(Value::as_str) == Some("book") {
                if let Some(a) = e.get("asset_id").and_then(Value::as_str) {
                    self.books.insert(a.to_string(), shared.clone());
                }
            }
        }
    }

    /// One snapshot frame per RTDS price series, oldest point first.
    pub fn rtds_greeting(&self) -> Vec<String> {
        self.rtds
            .iter()
            .filter(|(_, s)| !s.is_empty())
            .map(|((topic, symbol), points)| {
                let data: Vec<Value> = points
                    .iter()
                    .map(|(t, v)| serde_json::json!({ "timestamp": t, "value": v }))
                    .collect();
                serde_json::json!({
                    "topic": topic,
                    "type": "subscribe",
                    "payload": { "symbol": symbol, "data": data },
                })
                .to_string()
            })
            .collect()
    }

    /// The latest `book` frame of each asset, each frame once.
    pub fn clob_greeting(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for f in self.books.values() {
            if !out.iter().any(|o| o.as_str() == &**f) {
                out.push(f.to_string());
            }
        }
        out
    }

    pub fn greeting(&self, src: Source) -> Vec<String> {
        match src {
            Source::Rtds => self.rtds_greeting(),
            Source::ClobBook => self.clob_greeting(),
            _ => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ha::key::{frame_keys, FrameKind, KeyKind};
    use std::collections::HashSet;

    fn keys(src: Source, f: &str) -> Vec<ha::EventKey> {
        let mut out = Vec::new();
        assert_eq!(frame_keys(src, f, &mut out), FrameKind::Venue, "{f}");
        out
    }

    #[test]
    fn the_generator_is_deterministic_and_every_key_is_unique_and_timed() {
        let run = |seed| {
            let mut s = Synth::new(seed);
            let mut frames = Vec::new();
            for i in 0..3_000i64 {
                frames.push((Source::Binance, s.binance(1_000 + i)));
                frames.push((Source::ClobBook, s.clob(1_000 + i)));
                if i % 10 == 0 {
                    frames.push((Source::Rtds, s.rtds(1_000 + i)));
                }
            }
            frames
        };
        let a = run(7);
        assert_eq!(a, run(7));
        assert_ne!(a, run(8));
        let mut seen = HashSet::new();
        let mut kinds = HashSet::new();
        for (src, f) in &a {
            for k in keys(*src, f) {
                assert!(k.kind.is_keyed(), "{f}");
                assert!(k.ts_millis.is_some(), "{f}");
                assert!(seen.insert(k), "repeated key in {f}");
                kinds.insert(k.kind);
            }
        }
        for k in [
            KeyKind::BinanceTrade,
            KeyKind::BinanceBook,
            KeyKind::ClobBook,
            KeyKind::ClobPriceChange,
            KeyKind::RtdsPrice,
        ] {
            assert!(kinds.contains(&k), "{k:?} never generated");
        }
    }

    #[test]
    fn synthetic_books_parse_and_never_cross() {
        let mut s = Synth::new(1);
        let mut book = market::clob_book::ClobBookStream::new(ASSETS[0]);
        let mut updates = 0;
        for i in 0..5_000 {
            let f = s.clob(i);
            book.on_text(&f, |_| updates += 1)
                .unwrap_or_else(|e| panic!("desync on {f}: {e}"));
        }
        assert!(updates > 1_000);
    }

    #[test]
    fn greetings_repeat_broadcast_keys_only() {
        let mut s = Synth::new(3);
        let mut g = Greeter::default();
        let mut truth = HashSet::new();
        for i in 0..400i64 {
            let r = s.rtds(i * RTDS_TICK_MS);
            let c = s.clob(i);
            for (src, f) in [(Source::Rtds, &r), (Source::ClobBook, &c)] {
                truth.extend(keys(src, f));
                g.observe(src, f);
            }
        }
        let rtds = g.rtds_greeting();
        assert_eq!(rtds.len(), 2, "one frame per series");
        let mut greeted = 0;
        for f in &rtds {
            for k in keys(Source::Rtds, f) {
                assert!(truth.contains(&k), "greeting invented {k:?}");
                greeted += 1;
            }
        }
        assert_eq!(greeted, 2 * RTDS_HISTORY);
        let clob = g.clob_greeting();
        assert_eq!(clob.len(), 2, "latest book per asset");
        for f in &clob {
            let k = keys(Source::ClobBook, f);
            assert_eq!(k[0].kind, KeyKind::ClobBook);
            assert!(truth.contains(&k[0]));
        }
        // A fresh greeter has nothing to send.
        assert!(Greeter::default().greeting(Source::Rtds).is_empty());
    }
}
