//! Venue event identity.
//!
//! Two nodes reading the same venue must give the same event the same key,
//! so a key is built only from fields the venue sent. Receive time, node id,
//! connection id and anything else local to one socket never go in.
//!
//! A key is a kind plus a 128-bit id. For Binance the id holds the venue's
//! own sequence number exactly. For the other feeds it is a 128-bit FNV-1a
//! fingerprint of the identity fields, each length prefixed so that field
//! boundaries cannot shift. Fingerprints keep the key a fixed 32 bytes with
//! no heap allocation, which matters because the merger holds tens of
//! thousands of them. The chance of two different identities sharing a
//! fingerprint is about n^2 / 2^129, negligible next to the real risk, which
//! is that the identity fields themselves are not unique. `keycheck`
//! measures that on a recording.
//!
//! One raw frame can carry several events, so keys are per event, not per
//! frame. The rules, per feed:
//!
//! | Feed and event | Identity fields | Why |
//! | --- | --- | --- |
//! | Binance `aggTrade` | symbol, aggregate trade id `a` | `a` is the venue's trade sequence |
//! | Binance `bookTicker` | symbol, book update id `u` | `u` is the venue's book sequence |
//! | RTDS price (update or snapshot point) | topic, `payload.symbol`, point `timestamp` | one price per symbol per venue timestamp; a snapshot point and the live update for the same instant share a key |
//! | CLOB `book` | asset id, `hash`, `timestamp` | the venue's book hash at that time |
//! | CLOB `price_change`, one key per entry | asset id, `hash`, frame `timestamp`, `side`, `price` | one frame holds changes for both outcome tokens, and the live recording showed different frames at the same millisecond sharing a hash, so asset, hash and time alone collided 298 times; adding side and price removed every collision |
//! | CLOB `best_bid_ask` | asset id, `timestamp`, `best_bid`, `best_ask` | there is no hash; the quoted top is the event |
//! | CLOB `last_trade_price` | asset id, `transaction_hash`, `timestamp`, `side`, `price`, `size` | one transaction can fill at several prices |
//!
//! Anything else (another topic or event type, a frame missing an identity
//! field, a frame that is not JSON) becomes [`KeyKind::Unkeyed`] with a
//! fingerprint of its content as the id. RTDS envelopes carry a per
//! connection id, so for RTDS the content is the `payload`, not the whole
//! frame. Unkeyed events merge by content, which is only best effort: two
//! nodes that see the same unkeyed frame with different bytes deliver it
//! twice. Keepalive frames (`PING`, `PONG`, empty acks) are not events at all
//! and are reported as [`FrameKind::Noise`]. Recorder control records are
//! [`FrameKind::Control`].
//!
//! Each event also carries the venue timestamp when it has one. The merger
//! uses it to bound its window. It is part of the identity only where the
//! table says so.

use feeds::recording::Source;
use serde_json::Value;
use std::fmt::{self, Write as _};
use std::hash::{BuildHasherDefault, Hash, Hasher};

/// The three venue feeds. Control records belong to none of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Feed {
    Rtds,
    Binance,
    Clob,
}

impl Feed {
    pub const ALL: [Feed; 3] = [Feed::Rtds, Feed::Binance, Feed::Clob];

    pub fn index(self) -> usize {
        match self {
            Feed::Rtds => 0,
            Feed::Binance => 1,
            Feed::Clob => 2,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Feed::Rtds => "rtds",
            Feed::Binance => "binance",
            Feed::Clob => "clob",
        }
    }

    pub fn from_source(source: Source) -> Option<Feed> {
        match source {
            Source::Rtds => Some(Feed::Rtds),
            Source::Binance => Some(Feed::Binance),
            Source::ClobBook => Some(Feed::Clob),
            Source::Control => None,
        }
    }
}

/// What kind of venue event a key names, and so which fields built it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum KeyKind {
    BinanceTrade,
    BinanceBook,
    RtdsPrice,
    ClobBook,
    ClobPriceChange,
    ClobBestBidAsk,
    ClobLastTrade,
    /// No venue identity. The id is a fingerprint of the event's content.
    Unkeyed(Feed),
}

impl KeyKind {
    pub fn feed(self) -> Feed {
        match self {
            KeyKind::BinanceTrade | KeyKind::BinanceBook => Feed::Binance,
            KeyKind::RtdsPrice => Feed::Rtds,
            KeyKind::ClobBook
            | KeyKind::ClobPriceChange
            | KeyKind::ClobBestBidAsk
            | KeyKind::ClobLastTrade => Feed::Clob,
            KeyKind::Unkeyed(f) => f,
        }
    }

    /// True when the key comes from venue identity fields.
    pub fn is_keyed(self) -> bool {
        !matches!(self, KeyKind::Unkeyed(_))
    }

    fn code(self) -> u64 {
        match self {
            KeyKind::BinanceTrade => 1,
            KeyKind::BinanceBook => 2,
            KeyKind::RtdsPrice => 3,
            KeyKind::ClobBook => 4,
            KeyKind::ClobPriceChange => 5,
            KeyKind::ClobBestBidAsk => 6,
            KeyKind::ClobLastTrade => 7,
            KeyKind::Unkeyed(f) => 8 + f.index() as u64,
        }
    }
}

/// The identity of one venue event. Fixed size, `Copy`, no heap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct EventKey {
    pub kind: KeyKind,
    pub id: u128,
}

impl Hash for EventKey {
    // One write per key. The id is already well mixed (or a sequence number,
    // which `KeyHasher::finish` mixes), so this stays cheap.
    fn hash<H: Hasher>(&self, state: &mut H) {
        let lo = self.id as u64;
        let hi = (self.id >> 64) as u64;
        state.write_u64(lo ^ hi.rotate_left(29) ^ self.kind.code().wrapping_mul(GOLDEN));
    }
}

const GOLDEN: u64 = 0x9E37_79B9_7F4A_7C15;

/// A fast, deterministic hasher for [`EventKey`] sets. Not meant to resist
/// chosen inputs: keys come from venues, not from users.
#[derive(Debug, Clone, Copy, Default)]
pub struct KeyHasher(u64);

impl Hasher for KeyHasher {
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0 ^ b as u64).wrapping_mul(0x0000_0100_0000_01b3);
        }
    }

    fn write_u64(&mut self, v: u64) {
        self.0 = self.0.rotate_left(5) ^ v;
    }

    fn finish(&self) -> u64 {
        mix64(self.0)
    }
}

pub type KeyBuildHasher = BuildHasherDefault<KeyHasher>;

fn mix64(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// A key plus the venue timestamp of the event, when the venue sent one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stamped {
    pub key: EventKey,
    pub venue_ts_millis: Option<i64>,
}

const FNV128_OFFSET: u128 = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d;
const FNV128_PRIME: u128 = 0x0000_0000_0100_0000_0000_0000_0000_013b;

/// 128-bit FNV-1a. Stable across runs, platforms and processes, which a
/// cross-node identity needs and `std`'s hashers do not promise.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fingerprint(u128);

impl Default for Fingerprint {
    fn default() -> Self {
        Fingerprint(FNV128_OFFSET)
    }
}

impl Fingerprint {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn bytes(&mut self, b: &[u8]) -> &mut Self {
        for &x in b {
            self.0 ^= x as u128;
            self.0 = self.0.wrapping_mul(FNV128_PRIME);
        }
        self
    }

    /// A length-prefixed field, so ("ab", "c") and ("a", "bc") differ.
    pub fn field(&mut self, b: &[u8]) -> &mut Self {
        self.bytes(&(b.len() as u32).to_le_bytes()).bytes(b)
    }

    pub fn int(&mut self, v: i64) -> &mut Self {
        self.bytes(&v.to_le_bytes())
    }

    /// A canonical walk of a JSON value. Object keys come out sorted (the
    /// workspace builds `serde_json` without `preserve_order`), so spacing
    /// and key order in the raw text do not matter.
    pub fn value(&mut self, v: &Value) -> &mut Self {
        match v {
            Value::Null => {
                self.bytes(b"n");
            }
            Value::Bool(b) => {
                self.bytes(if *b { b"t" } else { b"f" });
            }
            Value::Number(n) => {
                self.bytes(b"#");
                // Writing into the hasher cannot fail.
                let _ = write!(self, "{n}");
                self.bytes(b";");
            }
            Value::String(s) => {
                self.bytes(b"s").field(s.as_bytes());
            }
            Value::Array(a) => {
                self.bytes(b"[").int(a.len() as i64);
                for x in a {
                    self.value(x);
                }
            }
            Value::Object(m) => {
                self.bytes(b"{").int(m.len() as i64);
                for (k, x) in m {
                    self.field(k.as_bytes()).value(x);
                }
            }
        }
        self
    }

    pub fn finish(&self) -> u128 {
        self.0
    }
}

impl fmt::Write for Fingerprint {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.bytes(s.as_bytes());
        Ok(())
    }
}

/// Fingerprint of a sequence of JSON values.
pub fn canonical(values: &[&Value]) -> u128 {
    let mut fp = Fingerprint::new();
    for v in values {
        fp.value(v);
    }
    fp.finish()
}

/// What a raw frame turned out to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameKind {
    /// A recorder control record, not from a venue.
    Control,
    /// A venue frame with no events: keepalive, empty ack, empty array.
    Noise,
    /// A venue frame that produced at least one event.
    Venue,
}

static NULL: Value = Value::Null;

fn get<'v>(v: &'v Value, k: &str) -> &'v Value {
    v.get(k).unwrap_or(&NULL)
}

fn str_field<'v>(v: &'v Value, k: &str) -> Option<&'v str> {
    v.get(k)?.as_str()
}

/// The CLOB sends timestamps as strings of milliseconds, sometimes as
/// numbers.
pub(crate) fn clob_ts(v: &Value) -> Option<i64> {
    let x = v.get("timestamp")?;
    x.as_str()
        .and_then(|s| s.parse().ok())
        .or_else(|| x.as_i64())
}

struct Sink<'a, F, const C: bool> {
    f: &'a mut F,
    events: u32,
}

impl<F: FnMut(Stamped, u128), const C: bool> Sink<'_, F, C> {
    fn keyed(&mut self, kind: KeyKind, id: u128, ts: Option<i64>, content: &[&Value]) {
        let c = if C { canonical(content) } else { 0 };
        let key = EventKey { kind, id };
        (self.f)(
            Stamped {
                key,
                venue_ts_millis: ts,
            },
            c,
        );
        self.events += 1;
    }

    fn unkeyed(&mut self, feed: Feed, v: &Value, ts: Option<i64>) {
        let id = canonical(&[v]);
        self.emit_unkeyed(feed, id, ts);
    }

    fn unkeyed_raw(&mut self, feed: Feed, raw: &str) {
        let mut fp = Fingerprint::new();
        fp.field(raw.as_bytes());
        self.emit_unkeyed(feed, fp.finish(), None);
    }

    fn emit_unkeyed(&mut self, feed: Feed, id: u128, ts: Option<i64>) {
        let key = EventKey {
            kind: KeyKind::Unkeyed(feed),
            id,
        };
        (self.f)(
            Stamped {
                key,
                venue_ts_millis: ts,
            },
            id,
        );
        self.events += 1;
    }
}

fn walk<F: FnMut(Stamped, u128), const C: bool>(
    source: Source,
    frame: &str,
    f: &mut F,
) -> FrameKind {
    let Some(feed) = Feed::from_source(source) else {
        return FrameKind::Control;
    };
    let trimmed = frame.trim();
    if trimmed.is_empty() || trimmed == "PONG" || trimmed == "PING" {
        return FrameKind::Noise;
    }
    let mut sink: Sink<'_, F, C> = Sink { f, events: 0 };
    match serde_json::from_str::<Value>(frame) {
        Err(_) => sink.unkeyed_raw(feed, frame),
        Ok(root) => match feed {
            Feed::Binance => binance(&root, &mut sink),
            Feed::Rtds => rtds(&root, &mut sink),
            Feed::Clob => clob(&root, &mut sink),
        },
    }
    if sink.events == 0 {
        FrameKind::Noise
    } else {
        FrameKind::Venue
    }
}

/// Call `f` with the key of every event in a raw frame.
pub fn for_each_event<F: FnMut(Stamped)>(source: Source, frame: &str, mut f: F) -> FrameKind {
    walk::<_, false>(source, frame, &mut |s: Stamped, _: u128| f(s))
}

/// Like [`for_each_event`], and also pass a fingerprint of the event's
/// venue content (prices, sizes, levels), for measuring key collisions.
pub fn for_each_event_with_content<F: FnMut(Stamped, u128)>(
    source: Source,
    frame: &str,
    mut f: F,
) -> FrameKind {
    walk::<_, true>(source, frame, &mut f)
}

/// Append the keys of every event in a raw frame to `out`.
pub fn frame_keys(source: Source, frame: &str, out: &mut Vec<Stamped>) -> FrameKind {
    for_each_event(source, frame, |s| out.push(s))
}

fn binance_id(symbol: &str, seq: u64) -> u128 {
    let mut fp = Fingerprint::new();
    fp.field(symbol.as_bytes());
    ((fp.finish() as u64 as u128) << 64) | seq as u128
}

fn binance<F: FnMut(Stamped, u128), const C: bool>(root: &Value, s: &mut Sink<'_, F, C>) {
    let (Some(stream), Some(data)) = (str_field(root, "stream"), root.get("data")) else {
        s.unkeyed(Feed::Binance, root, None);
        return;
    };
    // The stream name is always present and names the symbol in lower case.
    let symbol = stream.split('@').next().unwrap_or("");
    let ts = data
        .get("T")
        .and_then(Value::as_i64)
        .or_else(|| data.get("E").and_then(Value::as_i64));
    let (kind, id_field) = if stream.ends_with("@aggTrade") {
        (KeyKind::BinanceTrade, "a")
    } else if stream.ends_with("@bookTicker") {
        (KeyKind::BinanceBook, "u")
    } else {
        s.unkeyed(Feed::Binance, data, ts);
        return;
    };
    match data.get(id_field).and_then(Value::as_u64) {
        Some(seq) => s.keyed(kind, binance_id(symbol, seq), ts, &[data]),
        None => s.unkeyed(Feed::Binance, data, ts),
    }
}

fn rtds<F: FnMut(Stamped, u128), const C: bool>(root: &Value, s: &mut Sink<'_, F, C>) {
    let topic = str_field(root, "topic");
    let payload = root.get("payload");
    let price_topic = matches!(topic, Some("crypto_prices" | "crypto_prices_chainlink"));
    let (Some(topic), Some(payload), true) = (topic, payload, price_topic) else {
        // Not a price event. Hash the payload when there is one: the
        // envelope carries a per-connection id that differs between nodes.
        let body = payload.unwrap_or(root);
        let ts = body.get("timestamp").and_then(Value::as_i64);
        s.unkeyed(Feed::Rtds, body, ts);
        return;
    };
    let Some(symbol) = str_field(payload, "symbol") else {
        let ts = payload.get("timestamp").and_then(Value::as_i64);
        s.unkeyed(Feed::Rtds, payload, ts);
        return;
    };
    match payload.get("data").and_then(Value::as_array) {
        Some(points) => {
            for p in points {
                rtds_point(topic, symbol, p, s);
            }
        }
        None => rtds_point(topic, symbol, payload, s),
    }
}

fn rtds_point<F: FnMut(Stamped, u128), const C: bool>(
    topic: &str,
    symbol: &str,
    p: &Value,
    s: &mut Sink<'_, F, C>,
) {
    // Only the point's own timestamp counts. The envelope timestamp is the
    // send time on that connection.
    let ts = p.get("timestamp").and_then(Value::as_i64);
    match (ts, p.get("value")) {
        (Some(t), Some(v)) if v.is_number() => {
            let mut fp = Fingerprint::new();
            fp.field(topic.as_bytes()).field(symbol.as_bytes()).int(t);
            s.keyed(KeyKind::RtdsPrice, fp.finish(), Some(t), &[v]);
        }
        _ => s.unkeyed(Feed::Rtds, p, ts),
    }
}

fn clob<F: FnMut(Stamped, u128), const C: bool>(root: &Value, s: &mut Sink<'_, F, C>) {
    match root.as_array() {
        Some(items) => {
            for item in items {
                clob_event(item, s);
            }
        }
        None => clob_event(root, s),
    }
}

fn clob_event<F: FnMut(Stamped, u128), const C: bool>(e: &Value, s: &mut Sink<'_, F, C>) {
    let ts = clob_ts(e);
    let asset = str_field(e, "asset_id");
    match str_field(e, "event_type") {
        Some("book") => match (asset, str_field(e, "hash"), ts) {
            (Some(a), Some(h), Some(t)) => {
                let mut fp = Fingerprint::new();
                fp.field(a.as_bytes()).field(h.as_bytes()).int(t);
                let content = [get(e, "bids"), get(e, "asks")];
                s.keyed(KeyKind::ClobBook, fp.finish(), Some(t), &content);
            }
            _ => s.unkeyed(Feed::Clob, e, ts),
        },
        Some("price_change") => {
            let Some(changes) = e.get("price_changes").and_then(Value::as_array) else {
                s.unkeyed(Feed::Clob, e, ts);
                return;
            };
            for c in changes {
                let fields = (
                    str_field(c, "asset_id"),
                    str_field(c, "hash"),
                    ts,
                    str_field(c, "side"),
                    str_field(c, "price"),
                );
                match fields {
                    (Some(a), Some(h), Some(t), Some(side), Some(price)) => {
                        let mut fp = Fingerprint::new();
                        fp.field(a.as_bytes())
                            .field(h.as_bytes())
                            .int(t)
                            .field(side.as_bytes())
                            .field(price.as_bytes());
                        s.keyed(KeyKind::ClobPriceChange, fp.finish(), Some(t), &[c]);
                    }
                    _ => s.unkeyed(Feed::Clob, c, ts),
                }
            }
        }
        Some("best_bid_ask") => {
            match (
                asset,
                ts,
                str_field(e, "best_bid"),
                str_field(e, "best_ask"),
            ) {
                (Some(a), Some(t), Some(bid), Some(ask)) => {
                    let mut fp = Fingerprint::new();
                    fp.field(a.as_bytes())
                        .int(t)
                        .field(bid.as_bytes())
                        .field(ask.as_bytes());
                    let content = [get(e, "best_bid"), get(e, "best_ask"), get(e, "spread")];
                    s.keyed(KeyKind::ClobBestBidAsk, fp.finish(), Some(t), &content);
                }
                _ => s.unkeyed(Feed::Clob, e, ts),
            }
        }
        Some("last_trade_price") => {
            let fields = (
                asset,
                str_field(e, "transaction_hash"),
                ts,
                str_field(e, "side"),
                str_field(e, "price"),
                str_field(e, "size"),
            );
            match fields {
                (Some(a), Some(tx), Some(t), Some(side), Some(price), Some(size)) => {
                    let mut fp = Fingerprint::new();
                    fp.field(a.as_bytes())
                        .field(tx.as_bytes())
                        .int(t)
                        .field(side.as_bytes())
                        .field(price.as_bytes())
                        .field(size.as_bytes());
                    let content = [
                        get(e, "price"),
                        get(e, "size"),
                        get(e, "side"),
                        get(e, "fee_rate_bps"),
                    ];
                    s.keyed(KeyKind::ClobLastTrade, fp.finish(), Some(t), &content);
                }
                _ => s.unkeyed(Feed::Clob, e, ts),
            }
        }
        _ => s.unkeyed(Feed::Clob, e, ts),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(source: Source, frame: &str) -> (FrameKind, Vec<Stamped>) {
        let mut out = Vec::new();
        let kind = frame_keys(source, frame, &mut out);
        (kind, out)
    }

    fn one(source: Source, frame: &str) -> Stamped {
        let (kind, v) = keys(source, frame);
        assert_eq!(kind, FrameKind::Venue, "{frame}");
        assert_eq!(v.len(), 1, "{frame}");
        v[0]
    }

    #[test]
    fn binance_keys_hold_the_venue_sequence_exactly() {
        let book = r#"{"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","u":12055123599894,"s":"BTCUSDT","b":"74271.30","B":"4.702","a":"74271.40","A":"10.044","T":1794206104504,"E":1794206104505}}"#;
        let k = one(Source::Binance, book);
        assert_eq!(k.key.kind, KeyKind::BinanceBook);
        assert_eq!(k.key.id as u64, 12055123599894);
        assert_eq!(k.venue_ts_millis, Some(1794206104504));

        let trade = r#"{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","a":987654321,"p":"60050.10","q":"0.5","m":false,"T":1700000000000}}"#;
        let t = one(Source::Binance, trade);
        assert_eq!(t.key.kind, KeyKind::BinanceTrade);
        assert_eq!(t.key.id as u64, 987654321);
        // Same number, other stream: different keys.
        assert_ne!(t.key, k.key);
    }

    #[test]
    fn rtds_key_ignores_connection_id_and_envelope_time() {
        let a = r#"{"connection_id":"AAA=","payload":{"symbol":"btc/usd","timestamp":1794206104000,"value":74213.13},"timestamp":1794206104917,"topic":"crypto_prices_chainlink","type":"update"}"#;
        let b = r#"{"connection_id":"BBB=","payload":{"symbol":"btc/usd","timestamp":1794206104000,"value":74213.13},"timestamp":1794206104999,"topic":"crypto_prices_chainlink","type":"update"}"#;
        assert_eq!(one(Source::Rtds, a), one(Source::Rtds, b));
        // Another topic with the same symbol and time is another event.
        let c = a.replace("crypto_prices_chainlink", "crypto_prices");
        assert_ne!(one(Source::Rtds, a).key, one(Source::Rtds, &c).key);
    }

    #[test]
    fn rtds_snapshot_points_share_keys_with_live_updates() {
        let snap = r#"{"topic":"crypto_prices","type":"subscribe","timestamp":1,"payload":{"symbol":"btcusdt","data":[{"timestamp":1000,"value":1.5},{"timestamp":2000,"value":1.6}]}}"#;
        let upd = r#"{"topic":"crypto_prices","type":"update","timestamp":9,"payload":{"symbol":"btcusdt","timestamp":2000,"value":1.6}}"#;
        let (_, s) = keys(Source::Rtds, snap);
        assert_eq!(s.len(), 2);
        assert_eq!(s[1], one(Source::Rtds, upd));
    }

    const PC: &str = r#"{"market":"0x66", "price_changes":[{"asset_id":"812", "price":"0.44", "size":"309.4", "side":"BUY", "hash":"b620", "best_bid":"0.49", "best_ask":"0.5"}, {"asset_id":"559", "price":"0.56", "size":"309.4", "side":"SELL", "hash":"66c3", "best_bid":"0.5", "best_ask":"0.51"}], "timestamp":"1794206105917", "event_type":"price_change"}"#;

    #[test]
    fn a_price_change_frame_yields_one_key_per_entry() {
        let (kind, k) = keys(Source::ClobBook, PC);
        assert_eq!(kind, FrameKind::Venue);
        assert_eq!(k.len(), 2);
        assert!(k.iter().all(|s| s.key.kind == KeyKind::ClobPriceChange));
        assert_ne!(k[0].key, k[1].key);
        assert_eq!(k[0].venue_ts_millis, Some(1794206105917));
    }

    #[test]
    fn price_changes_sharing_a_hash_and_time_get_different_keys() {
        // Seen live: two frames, same asset, hash and millisecond, different
        // price. Without side and price in the key these collided.
        let other_price = PC.replacen(r#""price":"0.44""#, r#""price":"0.45""#, 1);
        let (_, a) = keys(Source::ClobBook, PC);
        let (_, b) = keys(Source::ClobBook, &other_price);
        assert_ne!(a[0].key, b[0].key);
        assert_eq!(a[1].key, b[1].key);
    }

    #[test]
    fn clob_keys_do_not_depend_on_spacing_or_key_order() {
        let compact: String = PC.split(", ").collect::<Vec<_>>().join(",");
        let (_, a) = keys(Source::ClobBook, PC);
        let (_, b) = keys(Source::ClobBook, &compact);
        assert_eq!(a, b);
    }

    #[test]
    fn clob_book_best_bid_ask_and_trades_are_keyed() {
        let book = r#"[{"market":"0x66","asset_id":"559","timestamp":"1794206105925","hash":"0055","bids":[{"price":"0.01","size":"9"}],"asks":[],"event_type":"book"}]"#;
        assert_eq!(one(Source::ClobBook, book).key.kind, KeyKind::ClobBook);
        let bba = r#"{"market":"0x66","asset_id":"559","best_bid":"0.5","best_ask":"0.51","spread":"0.01","timestamp":"1794206105921","event_type":"best_bid_ask"}"#;
        assert_eq!(one(Source::ClobBook, bba).key.kind, KeyKind::ClobBestBidAsk);
        let ltp = r#"{"market":"0x66","asset_id":"559","price":"0.5","size":"0.93","fee_rate_bps":"0","side":"SELL","timestamp":"1794206105939","event_type":"last_trade_price","transaction_hash":"0x1203"}"#;
        assert_eq!(one(Source::ClobBook, ltp).key.kind, KeyKind::ClobLastTrade);
    }

    #[test]
    fn frames_without_identity_are_unkeyed_not_dropped() {
        let no_hash =
            r#"{"asset_id":"559","timestamp":"1","event_type":"book","bids":[],"asks":[]}"#;
        let k = one(Source::ClobBook, no_hash);
        assert_eq!(k.key.kind, KeyKind::Unkeyed(Feed::Clob));
        let other = r#"{"asset_id":"559","timestamp":"1","event_type":"tick_size_change"}"#;
        assert_eq!(
            one(Source::ClobBook, other).key.kind,
            KeyKind::Unkeyed(Feed::Clob)
        );
        let garbage = one(Source::Binance, "not json");
        assert_eq!(garbage.key.kind, KeyKind::Unkeyed(Feed::Binance));
        assert_eq!(garbage.venue_ts_millis, None);
        // Same content, same unkeyed id.
        assert_eq!(garbage, one(Source::Binance, "not json"));
    }

    #[test]
    fn keepalives_are_noise_and_control_records_are_control() {
        assert_eq!(keys(Source::Rtds, "PONG").0, FrameKind::Noise);
        assert_eq!(keys(Source::Rtds, "").0, FrameKind::Noise);
        assert_eq!(keys(Source::ClobBook, "[]").0, FrameKind::Noise);
        assert_eq!(
            keys(Source::Control, r#"{"subscribe":"559"}"#).0,
            FrameKind::Control
        );
    }

    #[test]
    fn fingerprint_fields_are_length_prefixed() {
        let mut a = Fingerprint::new();
        a.field(b"ab").field(b"c");
        let mut b = Fingerprint::new();
        b.field(b"a").field(b"bc");
        assert_ne!(a.finish(), b.finish());
    }

    #[test]
    fn content_fingerprint_changes_with_the_payload() {
        let mut seen = Vec::new();
        for_each_event_with_content(Source::ClobBook, PC, |s, c| seen.push((s.key, c)));
        let bigger = PC.replacen(r#""size":"309.4""#, r#""size":"310""#, 1);
        let mut again = Vec::new();
        for_each_event_with_content(Source::ClobBook, &bigger, |s, c| again.push((s.key, c)));
        assert_eq!(seen[0].0, again[0].0, "size is not part of the key");
        assert_ne!(seen[0].1, again[0].1, "but it is part of the content");
    }
}
