//! Venue event identity.
//!
//! Two nodes reading the same venue must give the same event the same key,
//! so a key is built only from fields the venue sent. Receive time, node id,
//! connection id and anything else local to one socket never go in.
//!
//! A key is a kind, a 128-bit id and the event's venue timestamp. The
//! timestamp is part of the key, not a side value that travels with it: two
//! copies with the same key always carry the same timestamp, so the merger
//! can place a key in its window by the key alone. A rule may only use a
//! timestamp that is the same in every copy of the event, which is why
//! Binance uses the trade time `T` and never the event time `E` (see below).
//!
//! For Binance the id holds the venue's own sequence number exactly. For the
//! other feeds it is a 128-bit FNV-1a fingerprint of the identity fields,
//! each length prefixed so that field boundaries cannot shift. Fingerprints
//! keep the key a fixed size with no heap allocation, which matters because
//! the merger holds tens of thousands of them. The chance of two different
//! identities sharing a fingerprint is about n^2 / 2^129, negligible next to
//! the real risk, which is that the identity fields themselves are not
//! unique. `keycheck` measures that on a recording.
//!
//! One raw frame can carry several events, so keys are per event, not per
//! frame. The rules, per feed:
//!
//! | Feed and event | Identity fields | Why |
//! | --- | --- | --- |
//! | Binance `aggTrade` | symbol, aggregate trade id `a`, trade time `T` | `a` is the venue's trade sequence |
//! | Binance `bookTicker` | symbol, book update id `u`, transaction time `T` | `u` is the venue's book sequence |
//! | RTDS price (update or snapshot point) | topic, `payload.symbol`, point `timestamp` | one price per symbol per venue timestamp; a snapshot point and the live update for the same instant share a key |
//! | CLOB `book` | asset id, `hash`, `timestamp` | the venue's book hash at that time |
//! | CLOB `price_change`, one key per entry | asset id, `hash`, frame `timestamp`, `side`, `price`, `size` | see below |
//! | CLOB `best_bid_ask` | asset id, `timestamp`, `best_bid`, `best_ask` | there is no hash; the quoted top is the event |
//! | CLOB `last_trade_price` | asset id, `transaction_hash`, `timestamp`, `side`, `price`, `size` | one transaction can fill at several prices |
//! | CLOB `new_market` | `market` (condition id), market `id`, `timestamp` | the venue's own ids for the market it announces |
//! | CLOB `tick_size_change` | asset id, `timestamp`, `old_tick_size`, `new_tick_size` | there is no hash; the change is the event |
//!
//! Binance time. `T` is set by the matching engine and is the same in every
//! copy of a message. `E` is the time the message was pushed, and on the
//! committed recording it differs from `T` in 1,435 of 5,697 frames, so a
//! node that fell back to `E` could stamp one event differently from a node
//! that did not. A Binance event without `T` is still keyed by its sequence
//! number but has no timestamp, and the merger treats it as untimed.
//!
//! Price changes. One frame holds changes for both outcome tokens, and the
//! live recording showed different frames at the same millisecond sharing a
//! hash for the same asset at a different price, so asset, hash and time
//! alone collided 298 times. Side and price removed those collisions. Size
//! is in the key as well, by decision rather than from data: an entry gives
//! the new total size at one price level, so two entries equal in asset,
//! hash, millisecond, side and price but different in size would be two
//! states of that level, and dropping either would leave the book wrong.
//! The committed recording has no such pair (the key without size also
//! shows 0 collisions there, and `keycheck` prints that number), so nothing
//! measured forces this; it costs nothing, because every copy of one entry
//! carries the same size.
//!
//! Market announcements. With `custom_feature_enabled` the market channel
//! also sends `new_market` for markets the venue opens, whatever assets the
//! connection asked for, and `tick_size_change` when a book's tick changes. Both are
//! venue events, not keepalives, so a subscriber must get each once. The
//! first live drill had them unkeyed: the 10-minute live recording holds
//! 611 `new_market` (in bursts of up to 289 a minute) and 2
//! `tick_size_change`, and they were the only events of any feed without a
//! key there.
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
    ClobNewMarket,
    ClobTickSize,
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
            | KeyKind::ClobLastTrade
            | KeyKind::ClobNewMarket
            | KeyKind::ClobTickSize => Feed::Clob,
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
            KeyKind::ClobNewMarket => 11,
            KeyKind::ClobTickSize => 12,
        }
    }
}

/// The identity of one venue event. Fixed size, `Copy`, no heap.
///
/// `ts_millis` is the venue timestamp named by the key's rule, or `None`
/// when the event has none. It is part of the identity: equality, ordering
/// and hashing all include it, so one key cannot carry two timestamps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct EventKey {
    pub kind: KeyKind,
    pub id: u128,
    pub ts_millis: Option<i64>,
}

impl Hash for EventKey {
    // One write per key. The id is already well mixed (or a sequence number,
    // which `KeyHasher::finish` mixes), so this stays cheap.
    fn hash<H: Hasher>(&self, state: &mut H) {
        let lo = self.id as u64;
        let hi = (self.id >> 64) as u64;
        let ts = self.ts_millis.map_or(UNTIMED, |t| t as u64);
        state.write_u64(
            lo ^ hi.rotate_left(29)
                ^ self.kind.code().wrapping_mul(GOLDEN)
                ^ ts.rotate_left(43).wrapping_mul(SALT),
        );
    }
}

const GOLDEN: u64 = 0x9E37_79B9_7F4A_7C15;
const SALT: u64 = 0xD6E8_FEB8_6659_FD93;
const UNTIMED: u64 = 0x5BD1_E995_5BD1_E995;

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

    /// Like [`Fingerprint::value`], leaving out one member of an object.
    pub fn value_without(&mut self, v: &Value, skip: &str) -> &mut Self {
        let Some(m) = v.as_object() else {
            return self.value(v);
        };
        let n = m.keys().filter(|k| k.as_str() != skip).count();
        self.bytes(b"{").int(n as i64);
        for (k, x) in m {
            if k != skip {
                self.field(k.as_bytes()).value(x);
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
    /// The frame as received, used as the content of unkeyed events.
    raw: &'a str,
    events: u32,
}

impl<F: FnMut(EventKey, u128), const C: bool> Sink<'_, F, C> {
    /// Emit a keyed event. `content` writes the whole venue event into a
    /// fingerprint; it only runs when content is being measured.
    fn keyed(
        &mut self,
        kind: KeyKind,
        id: u128,
        ts: Option<i64>,
        content: impl FnOnce(&mut Fingerprint),
    ) {
        let c = if C {
            let mut fp = Fingerprint::new();
            content(&mut fp);
            fp.finish()
        } else {
            0
        };
        (self.f)(
            EventKey {
                kind,
                id,
                ts_millis: ts,
            },
            c,
        );
        self.events += 1;
    }

    fn unkeyed(&mut self, feed: Feed, v: &Value, ts: Option<i64>) {
        let id = canonical(&[v]);
        self.emit_unkeyed(feed, id, ts);
    }

    fn unkeyed_raw(&mut self, feed: Feed) {
        let mut fp = Fingerprint::new();
        fp.field(self.raw.as_bytes());
        self.emit_unkeyed(feed, fp.finish(), None);
    }

    fn emit_unkeyed(&mut self, feed: Feed, id: u128, ts: Option<i64>) {
        // The id already is the event's content, so content compared under
        // one id could never differ. Use the raw frame instead: the check
        // then sees the same unkeyed content arriving in frames that differ
        // elsewhere (an RTDS envelope, the rest of a CLOB array).
        let c = if C {
            let mut fp = Fingerprint::new();
            fp.field(self.raw.as_bytes());
            fp.finish()
        } else {
            0
        };
        let key = EventKey {
            kind: KeyKind::Unkeyed(feed),
            id,
            ts_millis: ts,
        };
        (self.f)(key, c);
        self.events += 1;
    }
}

fn walk<F: FnMut(EventKey, u128), const C: bool>(
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
    let mut sink: Sink<'_, F, C> = Sink {
        f,
        raw: frame,
        events: 0,
    };
    match serde_json::from_str::<Value>(frame) {
        Err(_) => sink.unkeyed_raw(feed),
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
pub fn for_each_event<F: FnMut(EventKey)>(source: Source, frame: &str, mut f: F) -> FrameKind {
    walk::<_, false>(source, frame, &mut |k: EventKey, _: u128| f(k))
}

/// Like [`for_each_event`], and also pass a fingerprint of the event's full
/// venue content, for measuring key collisions. For a keyed event that is
/// the whole event object (for a price change entry, the entry plus the
/// rest of its frame; for an RTDS point, the point without the symbol,
/// which is identity). For an unkeyed event it is the raw frame.
pub fn for_each_event_with_content<F: FnMut(EventKey, u128)>(
    source: Source,
    frame: &str,
    mut f: F,
) -> FrameKind {
    walk::<_, true>(source, frame, &mut f)
}

/// Append the keys of every event in a raw frame to `out`.
pub fn frame_keys(source: Source, frame: &str, out: &mut Vec<EventKey>) -> FrameKind {
    for_each_event(source, frame, |k| out.push(k))
}

fn binance_id(symbol: &str, seq: u64) -> u128 {
    let mut fp = Fingerprint::new();
    fp.field(symbol.as_bytes());
    ((fp.finish() as u64 as u128) << 64) | seq as u128
}

fn binance<F: FnMut(EventKey, u128), const C: bool>(root: &Value, s: &mut Sink<'_, F, C>) {
    let (Some(stream), Some(data)) = (str_field(root, "stream"), root.get("data")) else {
        s.unkeyed(Feed::Binance, root, None);
        return;
    };
    // The stream name is always present and names the symbol in lower case.
    let symbol = stream.split('@').next().unwrap_or("");
    // Only the engine's time `T`. `E` is the push time and is not promised
    // to match across connections, so it never becomes part of a key.
    let ts = data.get("T").and_then(Value::as_i64);
    let (kind, id_field) = if stream.ends_with("@aggTrade") {
        (KeyKind::BinanceTrade, "a")
    } else if stream.ends_with("@bookTicker") {
        (KeyKind::BinanceBook, "u")
    } else {
        s.unkeyed(Feed::Binance, data, ts);
        return;
    };
    match data.get(id_field).and_then(Value::as_u64) {
        Some(seq) => s.keyed(kind, binance_id(symbol, seq), ts, |fp| {
            fp.value(data);
        }),
        None => s.unkeyed(Feed::Binance, data, ts),
    }
}

fn rtds<F: FnMut(EventKey, u128), const C: bool>(root: &Value, s: &mut Sink<'_, F, C>) {
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

fn rtds_point<F: FnMut(EventKey, u128), const C: bool>(
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
            // A live update repeats the symbol inside the point and a
            // snapshot point does not; the symbol is identity either way.
            s.keyed(KeyKind::RtdsPrice, fp.finish(), Some(t), |c| {
                c.value_without(p, "symbol");
            });
        }
        _ => s.unkeyed(Feed::Rtds, p, ts),
    }
}

fn clob<F: FnMut(EventKey, u128), const C: bool>(root: &Value, s: &mut Sink<'_, F, C>) {
    match root.as_array() {
        Some(items) => {
            for item in items {
                clob_event(item, s);
            }
        }
        None => clob_event(root, s),
    }
}

fn clob_event<F: FnMut(EventKey, u128), const C: bool>(e: &Value, s: &mut Sink<'_, F, C>) {
    let ts = clob_ts(e);
    let asset = str_field(e, "asset_id");
    let whole = |fp: &mut Fingerprint| {
        fp.value(e);
    };
    match str_field(e, "event_type") {
        Some("book") => match (asset, str_field(e, "hash"), ts) {
            (Some(a), Some(h), Some(t)) => {
                let mut fp = Fingerprint::new();
                fp.field(a.as_bytes()).field(h.as_bytes()).int(t);
                s.keyed(KeyKind::ClobBook, fp.finish(), Some(t), whole);
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
                    str_field(c, "size"),
                );
                match fields {
                    (Some(a), Some(h), Some(t), Some(side), Some(price), Some(size)) => {
                        let mut fp = Fingerprint::new();
                        fp.field(a.as_bytes())
                            .field(h.as_bytes())
                            .int(t)
                            .field(side.as_bytes())
                            .field(price.as_bytes())
                            .field(size.as_bytes());
                        s.keyed(KeyKind::ClobPriceChange, fp.finish(), Some(t), |fp| {
                            fp.value_without(e, "price_changes").value(c);
                        });
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
                    s.keyed(KeyKind::ClobBestBidAsk, fp.finish(), Some(t), whole);
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
                    s.keyed(KeyKind::ClobLastTrade, fp.finish(), Some(t), whole);
                }
                _ => s.unkeyed(Feed::Clob, e, ts),
            }
        }
        Some("new_market") => match (str_field(e, "market"), str_field(e, "id"), ts) {
            (Some(m), Some(id), Some(t)) => {
                let mut fp = Fingerprint::new();
                fp.field(m.as_bytes()).field(id.as_bytes()).int(t);
                s.keyed(KeyKind::ClobNewMarket, fp.finish(), Some(t), whole);
            }
            _ => s.unkeyed(Feed::Clob, e, ts),
        },
        Some("tick_size_change") => match (
            asset,
            ts,
            str_field(e, "old_tick_size"),
            str_field(e, "new_tick_size"),
        ) {
            (Some(a), Some(t), Some(old), Some(new)) => {
                let mut fp = Fingerprint::new();
                fp.field(a.as_bytes())
                    .int(t)
                    .field(old.as_bytes())
                    .field(new.as_bytes());
                s.keyed(KeyKind::ClobTickSize, fp.finish(), Some(t), whole);
            }
            _ => s.unkeyed(Feed::Clob, e, ts),
        },
        _ => s.unkeyed(Feed::Clob, e, ts),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn keys(source: Source, frame: &str) -> (FrameKind, Vec<EventKey>) {
        let mut out = Vec::new();
        let kind = frame_keys(source, frame, &mut out);
        (kind, out)
    }

    fn one(source: Source, frame: &str) -> EventKey {
        let (kind, v) = keys(source, frame);
        assert_eq!(kind, FrameKind::Venue, "{frame}");
        assert_eq!(v.len(), 1, "{frame}");
        v[0]
    }

    fn with_content(source: Source, frame: &str) -> Vec<(EventKey, u128)> {
        let mut out = Vec::new();
        for_each_event_with_content(source, frame, |k, c| out.push((k, c)));
        out
    }

    #[test]
    fn binance_keys_hold_the_venue_sequence_exactly() {
        let book = r#"{"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","u":12055123599894,"s":"BTCUSDT","b":"74271.30","B":"4.702","a":"74271.40","A":"10.044","T":1794206104504,"E":1794206104505}}"#;
        let k = one(Source::Binance, book);
        assert_eq!(k.kind, KeyKind::BinanceBook);
        assert_eq!(k.id as u64, 12055123599894);
        assert_eq!(k.ts_millis, Some(1794206104504));

        let trade = r#"{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","a":987654321,"p":"60050.10","q":"0.5","m":false,"T":1700000000000}}"#;
        let t = one(Source::Binance, trade);
        assert_eq!(t.kind, KeyKind::BinanceTrade);
        assert_eq!(t.id as u64, 987654321);
        // Same number, other stream: different keys.
        assert_ne!(t, k);
    }

    fn binance_book(u: u64, t: Option<i64>, e: Option<i64>) -> String {
        let mut data = format!(r#""e":"bookTicker","u":{u},"s":"BTCUSDT","b":"1.0","a":"1.1""#);
        if let Some(t) = t {
            data.push_str(&format!(r#","T":{t}"#));
        }
        if let Some(e) = e {
            data.push_str(&format!(r#","E":{e}"#));
        }
        format!(r#"{{"stream":"btcusdt@bookTicker","data":{{{data}}}}}"#)
    }

    #[test]
    fn binance_time_is_the_engine_time_and_never_the_push_time() {
        // Without `T` the event keeps its sequence key but has no time,
        // even when `E` is there: `E` is never used as a fallback.
        let k = one(Source::Binance, &binance_book(7, None, Some(500)));
        assert_eq!(k.kind, KeyKind::BinanceBook);
        assert_eq!(k.ts_millis, None);
        assert_eq!(k, one(Source::Binance, &binance_book(7, None, Some(900))));
    }

    proptest! {
        /// Copies of one Binance event that differ only in push time get one
        /// key with one timestamp. Different engine times are different keys.
        #[test]
        fn binance_copies_share_a_key_and_a_timestamp(
            u in any::<u64>(),
            t in 0i64..4_000_000_000_000,
            e1 in proptest::option::of(0i64..4_000_000_000_000),
            e2 in proptest::option::of(0i64..4_000_000_000_000),
            dt in 1i64..1_000,
        ) {
            let a = one(Source::Binance, &binance_book(u, Some(t), e1));
            let b = one(Source::Binance, &binance_book(u, Some(t), e2));
            prop_assert_eq!(a, b);
            prop_assert_eq!(a.ts_millis, Some(t));
            let c = one(Source::Binance, &binance_book(u, Some(t + dt), e1));
            prop_assert_ne!(a, c);
            prop_assert_eq!(a.id, c.id);
        }
    }

    #[test]
    fn rtds_key_ignores_connection_id_and_envelope_time() {
        let a = r#"{"connection_id":"AAA=","payload":{"symbol":"btc/usd","timestamp":1794206104000,"value":74213.13},"timestamp":1794206104917,"topic":"crypto_prices_chainlink","type":"update"}"#;
        let b = r#"{"connection_id":"BBB=","payload":{"symbol":"btc/usd","timestamp":1794206104000,"value":74213.13},"timestamp":1794206104999,"topic":"crypto_prices_chainlink","type":"update"}"#;
        assert_eq!(one(Source::Rtds, a), one(Source::Rtds, b));
        assert_eq!(with_content(Source::Rtds, a), with_content(Source::Rtds, b));
        // Another topic with the same symbol and time is another event.
        let c = a.replace("crypto_prices_chainlink", "crypto_prices");
        assert_ne!(one(Source::Rtds, a), one(Source::Rtds, &c));
    }

    #[test]
    fn rtds_snapshot_points_share_keys_and_content_with_live_updates() {
        let snap = r#"{"topic":"crypto_prices","type":"subscribe","timestamp":1,"payload":{"symbol":"btcusdt","data":[{"timestamp":1000,"value":1.5},{"timestamp":2000,"value":1.6}]}}"#;
        let upd = r#"{"topic":"crypto_prices","type":"update","timestamp":9,"payload":{"symbol":"btcusdt","timestamp":2000,"value":1.6}}"#;
        let (_, s) = keys(Source::Rtds, snap);
        assert_eq!(s.len(), 2);
        assert_eq!(s[1], one(Source::Rtds, upd));
        // Same point, so a recording holding both counts a repeat, not a
        // collision.
        assert_eq!(
            with_content(Source::Rtds, snap)[1],
            with_content(Source::Rtds, upd)[0]
        );
    }

    const PC: &str = r#"{"market":"0x66", "price_changes":[{"asset_id":"812", "price":"0.44", "size":"309.4", "side":"BUY", "hash":"b620", "best_bid":"0.49", "best_ask":"0.5"}, {"asset_id":"559", "price":"0.56", "size":"309.4", "side":"SELL", "hash":"66c3", "best_bid":"0.5", "best_ask":"0.51"}], "timestamp":"1794206105917", "event_type":"price_change"}"#;

    #[test]
    fn a_price_change_frame_yields_one_key_per_entry() {
        let (kind, k) = keys(Source::ClobBook, PC);
        assert_eq!(kind, FrameKind::Venue);
        assert_eq!(k.len(), 2);
        assert!(k.iter().all(|s| s.kind == KeyKind::ClobPriceChange));
        assert_ne!(k[0], k[1]);
        assert_eq!(k[0].ts_millis, Some(1794206105917));
    }

    #[test]
    fn price_changes_sharing_a_hash_and_time_get_different_keys() {
        // Seen live: two frames, same asset, hash and millisecond, different
        // price. Without side and price in the key these collided.
        let other_price = PC.replacen(r#""price":"0.44""#, r#""price":"0.45""#, 1);
        let (_, a) = keys(Source::ClobBook, PC);
        let (_, b) = keys(Source::ClobBook, &other_price);
        assert_ne!(a[0], b[0]);
        assert_eq!(a[1], b[1]);
    }

    #[test]
    fn price_changes_differing_only_in_size_get_different_keys() {
        // Two states of one price level at one millisecond. Neither may be
        // dropped as a copy of the other.
        let bigger = PC.replacen(r#""size":"309.4""#, r#""size":"310""#, 1);
        let (_, a) = keys(Source::ClobBook, PC);
        let (_, b) = keys(Source::ClobBook, &bigger);
        assert_ne!(a[0], b[0]);
        assert_eq!(a[1], b[1]);
    }

    #[test]
    fn clob_keys_do_not_depend_on_spacing_or_key_order() {
        let compact: String = PC.split(", ").collect::<Vec<_>>().join(",");
        let (_, a) = keys(Source::ClobBook, PC);
        let (_, b) = keys(Source::ClobBook, &compact);
        assert_eq!(a, b);
        assert_eq!(
            with_content(Source::ClobBook, PC),
            with_content(Source::ClobBook, &compact)
        );
    }

    #[test]
    fn clob_book_best_bid_ask_and_trades_are_keyed() {
        let book = r#"[{"market":"0x66","asset_id":"559","timestamp":"1794206105925","hash":"0055","bids":[{"price":"0.01","size":"9"}],"asks":[],"event_type":"book"}]"#;
        assert_eq!(one(Source::ClobBook, book).kind, KeyKind::ClobBook);
        let bba = r#"{"market":"0x66","asset_id":"559","best_bid":"0.5","best_ask":"0.51","spread":"0.01","timestamp":"1794206105921","event_type":"best_bid_ask"}"#;
        assert_eq!(one(Source::ClobBook, bba).kind, KeyKind::ClobBestBidAsk);
        let ltp = r#"{"market":"0x66","asset_id":"559","price":"0.5","size":"0.93","fee_rate_bps":"0","side":"SELL","timestamp":"1794206105939","event_type":"last_trade_price","transaction_hash":"0x1203"}"#;
        assert_eq!(one(Source::ClobBook, ltp).kind, KeyKind::ClobLastTrade);
    }

    // Shapes from the 10-minute live recording, description shortened.
    const NEW_MARKET: &str = r#"{"id":"6894295", "question":"Dogecoin Up or Down - November 10, 1:20AM-1:25AM ET", "market":"0xe709", "slug":"doge-updown-5m-1794291600", "description":"This market will resolve to Up.", "assets_ids":["101","102"], "outcomes":["Up","Down"], "active":true, "timestamp":"1794205786533", "event_type":"new_market"}"#;
    const TICK: &str = r#"{"market":"0x0629", "asset_id":"5609", "old_tick_size":"0.01", "new_tick_size":"0.001", "timestamp":"1794205786533", "event_type":"tick_size_change"}"#;

    #[test]
    fn market_announcements_and_tick_changes_are_keyed() {
        let n = one(Source::ClobBook, NEW_MARKET);
        assert_eq!(n.kind, KeyKind::ClobNewMarket);
        assert_eq!(n.ts_millis, Some(1794205786533));
        // Another market announced at the same millisecond is another event.
        let other = NEW_MARKET.replacen(r#""id":"6894295""#, r#""id":"6894296""#, 1);
        assert_ne!(n, one(Source::ClobBook, &other));
        // Text outside the identity does not move the key.
        let reworded = NEW_MARKET.replacen("resolve to Up", "resolve Up", 1);
        assert_eq!(n, one(Source::ClobBook, &reworded));

        let t = one(Source::ClobBook, TICK);
        assert_eq!(t.kind, KeyKind::ClobTickSize);
        assert_eq!(t.ts_millis, Some(1794205786533));
        let back = TICK
            .replacen(r#""old_tick_size":"0.01""#, r#""old_tick_size":"0.001""#, 1)
            .replacen(r#""new_tick_size":"0.001""#, r#""new_tick_size":"0.01""#, 1);
        assert_ne!(t, one(Source::ClobBook, &back));
        // Same ids and time, other kind: different keys.
        assert_ne!(t.kind.code(), n.kind.code());
    }

    #[test]
    fn frames_without_identity_are_unkeyed_not_dropped() {
        let no_hash =
            r#"{"asset_id":"559","timestamp":"1","event_type":"book","bids":[],"asks":[]}"#;
        let k = one(Source::ClobBook, no_hash);
        assert_eq!(k.kind, KeyKind::Unkeyed(Feed::Clob));
        assert_eq!(k.ts_millis, Some(1));
        // A tick size change without its sizes has no identity.
        let other = r#"{"asset_id":"559","timestamp":"1","event_type":"tick_size_change"}"#;
        assert_eq!(
            one(Source::ClobBook, other).kind,
            KeyKind::Unkeyed(Feed::Clob)
        );
        let garbage = one(Source::Binance, "not json");
        assert_eq!(garbage.kind, KeyKind::Unkeyed(Feed::Binance));
        assert_eq!(garbage.ts_millis, None);
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
    fn value_without_skips_only_the_named_member() {
        let v: Value = serde_json::from_str(r#"{"a":1,"b":2}"#).unwrap();
        let w: Value = serde_json::from_str(r#"{"a":1}"#).unwrap();
        let mut x = Fingerprint::new();
        x.value_without(&v, "b");
        let mut y = Fingerprint::new();
        y.value(&w);
        assert_eq!(x.finish(), y.finish());
        let mut z = Fingerprint::new();
        z.value_without(&v, "c");
        assert_eq!(z.finish(), canonical(&[&v]));
    }

    #[test]
    fn content_covers_every_field_not_only_the_key_fields() {
        // best_bid sits in the entry but not in the key. Content must see it,
        // or a collision on it could never show up.
        let seen = with_content(Source::ClobBook, PC);
        let moved = PC.replacen(r#""best_bid":"0.49""#, r#""best_bid":"0.48""#, 1);
        let again = with_content(Source::ClobBook, &moved);
        assert_eq!(seen[0].0, again[0].0, "best_bid is not part of the key");
        assert_ne!(seen[0].1, again[0].1, "but it is part of the content");
        assert_eq!(seen[1], again[1]);
        // The rest of the frame counts too.
        let other_market = PC.replacen(r#""market":"0x66""#, r#""market":"0x67""#, 1);
        let m = with_content(Source::ClobBook, &other_market);
        assert_eq!(seen[1].0, m[1].0);
        assert_ne!(seen[1].1, m[1].1);
    }

    #[test]
    fn unkeyed_content_is_the_raw_frame() {
        // Same payload, different connection: one unkeyed key, two contents.
        let a = r#"{"connection_id":"AAA=","topic":"other","payload":{"x":1}}"#;
        let b = r#"{"connection_id":"BBB=","topic":"other","payload":{"x":1}}"#;
        let ca = with_content(Source::Rtds, a);
        let cb = with_content(Source::Rtds, b);
        assert_eq!(ca[0].0, cb[0].0);
        assert_ne!(ca[0].1, cb[0].1);
        assert_ne!(ca[0].1, ca[0].0.id, "content is not the key id");
    }
}
