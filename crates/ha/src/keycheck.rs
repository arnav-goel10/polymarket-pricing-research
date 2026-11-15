//! Measure the event key assumption on a recording.
//!
//! The whole merge design rests on one claim: two events with the same key
//! are the same event. This walks a recording and counts, per feed, how
//! many events got a venue key, how many did not, and how many keys were
//! shared by events with different content (a collision, which would make
//! the merger drop a real event). Same key with the same content is counted
//! as a repeat: the venue sent the same thing twice, and dropping the
//! second copy loses nothing.
//!
//! Content means the whole venue event, not only the fields that are not in
//! the key, so a collision on any field shows up. For unkeyed events the key
//! id is already a fingerprint of the event, so content is the raw frame:
//! the same unkeyed event inside frames that differ elsewhere is counted as
//! a collision.

use crate::key::{clob_ts, for_each_event_with_content, EventKey, Feed, Fingerprint, FrameKind};
use feeds::recording::{ReadError, Record, RecordReader};
use serde_json::Value;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;
use std::path::Path;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FeedCounts {
    pub frames: u64,
    /// Frames with no events (keepalives, empty acks).
    pub noise_frames: u64,
    pub events: u64,
    pub keyed: u64,
    pub unkeyed: u64,
    /// Same key, different content.
    pub collisions: u64,
    /// Same key, same content.
    pub repeats: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KeyReport {
    /// Indexed by [`Feed::index`].
    pub feeds: [FeedCounts; 3],
    pub control_frames: u64,
    /// Byte offset of a partial final record, if the file had one.
    pub truncated_tail: Option<u64>,
    /// Collisions the first design's price change key (asset id, hash and
    /// timestamp only) would have had. Kept to show why side and price are
    /// in the key.
    pub plan_key_collisions: u64,
    /// Collisions the price change key would have without size. Kept to
    /// show that size is in the key by decision, not because this recording
    /// needed it.
    pub no_size_key_collisions: u64,
}

impl KeyReport {
    pub fn feed(&self, f: Feed) -> FeedCounts {
        self.feeds[f.index()]
    }

    pub fn collisions(&self) -> u64 {
        self.feeds.iter().map(|f| f.collisions).sum()
    }
}

impl fmt::Display for KeyReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "{:<8} {:>8} {:>6} {:>8} {:>8} {:>8} {:>10} {:>8}",
            "source", "frames", "noise", "events", "keyed", "unkeyed", "collisions", "repeats"
        )?;
        for feed in Feed::ALL {
            let c = self.feed(feed);
            writeln!(
                f,
                "{:<8} {:>8} {:>6} {:>8} {:>8} {:>8} {:>10} {:>8}",
                feed.name(),
                c.frames,
                c.noise_frames,
                c.events,
                c.keyed,
                c.unkeyed,
                c.collisions,
                c.repeats
            )?;
        }
        writeln!(f, "control frames: {}", self.control_frames)?;
        writeln!(
            f,
            "price_change collisions with the plan's first key (asset, hash, timestamp): {}",
            self.plan_key_collisions
        )?;
        writeln!(
            f,
            "price_change collisions without size in the key: {}",
            self.no_size_key_collisions
        )?;
        match self.truncated_tail {
            Some(off) => writeln!(f, "truncated tail at byte {off}"),
            None => writeln!(f, "truncated tail: none"),
        }
    }
}

/// Accumulates a [`KeyReport`] one record at a time.
#[derive(Debug, Default)]
pub struct KeyCheck {
    report: KeyReport,
    first: HashMap<EventKey, u128>,
    plan: HashMap<u128, u128>,
    no_size: HashMap<u128, u128>,
}

impl KeyCheck {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, rec: &Record) {
        let Some(feed) = Feed::from_source(rec.source) else {
            self.report.control_frames += 1;
            return;
        };
        let first = &mut self.first;
        let c = &mut self.report.feeds[feed.index()];
        c.frames += 1;
        let kind = for_each_event_with_content(rec.source, &rec.frame, |k, content| {
            c.events += 1;
            if k.kind.is_keyed() {
                c.keyed += 1;
            } else {
                c.unkeyed += 1;
            }
            match first.entry(k) {
                Entry::Vacant(v) => {
                    v.insert(content);
                }
                Entry::Occupied(o) => {
                    if *o.get() == content {
                        c.repeats += 1;
                    } else {
                        c.collisions += 1;
                    }
                }
            }
        });
        if kind == FrameKind::Noise {
            c.noise_frames += 1;
        }
        if feed == Feed::Clob {
            older_keys(
                &rec.frame,
                [&mut self.plan, &mut self.no_size],
                [
                    &mut self.report.plan_key_collisions,
                    &mut self.report.no_size_key_collisions,
                ],
            );
        }
    }

    pub fn finish(self) -> KeyReport {
        self.report
    }
}

/// Two older price change keys, for comparison only: the plan's first key
/// (asset id, hash, timestamp) and the current key without size. Content is
/// the same as for the real key.
fn older_keys(frame: &str, seen: [&mut HashMap<u128, u128>; 2], collisions: [&mut u64; 2]) {
    let Ok(root) = serde_json::from_str::<Value>(frame) else {
        return;
    };
    let items = match root.as_array() {
        Some(a) => a.as_slice(),
        None => std::slice::from_ref(&root),
    };
    let [plan, no_size] = seen;
    let [plan_n, no_size_n] = collisions;
    for e in items {
        if e.get("event_type").and_then(Value::as_str) != Some("price_change") {
            continue;
        }
        let Some(ts) = clob_ts(e) else { continue };
        let changes = e.get("price_changes").and_then(Value::as_array);
        for c in changes.into_iter().flatten() {
            let field = move |k: &str| c.get(k).and_then(Value::as_str);
            let (Some(asset), Some(hash)) = (field("asset_id"), field("hash")) else {
                continue;
            };
            let mut v = Fingerprint::new();
            v.value_without(e, "price_changes").value(c);
            let content = v.finish();
            let mut k = Fingerprint::new();
            k.field(asset.as_bytes()).field(hash.as_bytes()).int(ts);
            count(plan, k.finish(), content, plan_n);
            if let (Some(side), Some(price)) = (field("side"), field("price")) {
                k.field(side.as_bytes()).field(price.as_bytes());
                count(no_size, k.finish(), content, no_size_n);
            }
        }
    }
}

fn count(seen: &mut HashMap<u128, u128>, key: u128, content: u128, collisions: &mut u64) {
    match seen.entry(key) {
        Entry::Vacant(x) => {
            x.insert(content);
        }
        Entry::Occupied(o) => {
            if *o.get() != content {
                *collisions += 1;
            }
        }
    }
}

/// Check every record of a recording file. A truncated final record is
/// noted in the report; any other read error fails.
pub fn check_file(path: impl AsRef<Path>) -> Result<KeyReport, ReadError> {
    let mut kc = KeyCheck::new();
    let mut tail = None;
    for r in RecordReader::open(path)? {
        match r {
            Ok(rec) => kc.add(&rec),
            Err(ReadError::TruncatedTail { offset }) => {
                tail = Some(offset);
                break;
            }
            Err(e) => return Err(e),
        }
    }
    let mut report = kc.finish();
    report.truncated_tail = tail;
    Ok(report)
}

/// Check records already in memory.
pub fn check_records<'a>(records: impl IntoIterator<Item = &'a Record>) -> KeyReport {
    let mut kc = KeyCheck::new();
    for r in records {
        kc.add(r);
    }
    kc.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use feeds::recording::Source;

    fn rec(source: Source, frame: &str) -> Record {
        Record {
            recv_micros: 0,
            source,
            frame: frame.to_string(),
        }
    }

    const PC: &str = r#"{"market":"0x66","price_changes":[{"asset_id":"559","price":"0.56","size":"309.4","side":"SELL","hash":"66c3","best_bid":"0.5","best_ask":"0.51"}],"timestamp":"1794206105917","event_type":"price_change"}"#;

    #[test]
    fn same_key_different_content_is_a_collision() {
        let other_bid = PC.replace(r#""best_bid":"0.5""#, r#""best_bid":"0.4""#);
        let r = check_records(&[rec(Source::ClobBook, PC), rec(Source::ClobBook, &other_bid)]);
        let c = r.feed(Feed::Clob);
        assert_eq!((c.events, c.keyed, c.collisions, c.repeats), (2, 2, 1, 0));
        assert_eq!(r.collisions(), 1);
    }

    #[test]
    fn a_different_size_is_a_different_event_not_a_collision() {
        let other_size = PC.replace(r#""size":"309.4""#, r#""size":"1""#);
        let r = check_records(&[
            rec(Source::ClobBook, PC),
            rec(Source::ClobBook, &other_size),
        ]);
        assert_eq!(r.collisions(), 0);
        // The key without size would have dropped one of them.
        assert_eq!(r.no_size_key_collisions, 1);
        assert_eq!(r.plan_key_collisions, 1);
    }

    #[test]
    fn collisions_show_on_fields_outside_the_key() {
        // best_bid_ask: the key holds bid and ask, so the old content check
        // (bid, ask, spread) could only ever see the spread. Any other field
        // now counts.
        let bba = r#"{"market":"0x66","asset_id":"559","best_bid":"0.5","best_ask":"0.51","spread":"0.01","timestamp":"1794206105921","event_type":"best_bid_ask"}"#;
        let bba2 = bba.replace(r#""market":"0x66""#, r#""market":"0x67""#);
        let ltp = r#"{"market":"0x66","asset_id":"559","price":"0.5","size":"0.93","fee_rate_bps":"0","side":"SELL","timestamp":"1794206105939","event_type":"last_trade_price","transaction_hash":"0x1203"}"#;
        let ltp2 = ltp.replace(r#""market":"0x66""#, r#""market":"0x67""#);
        let r = check_records(&[
            rec(Source::ClobBook, bba),
            rec(Source::ClobBook, &bba2),
            rec(Source::ClobBook, ltp),
            rec(Source::ClobBook, &ltp2),
        ]);
        assert_eq!(r.feed(Feed::Clob).collisions, 2);
    }

    #[test]
    fn unkeyed_events_can_collide() {
        // The same unkeyed payload on two connections. Before, content was
        // the key id itself, so this was always 0.
        let a = r#"{"connection_id":"AAA=","topic":"other","payload":{"x":1}}"#;
        let b = r#"{"connection_id":"BBB=","topic":"other","payload":{"x":1}}"#;
        let r = check_records(&[
            rec(Source::Rtds, a),
            rec(Source::Rtds, b),
            rec(Source::Rtds, a),
        ]);
        let c = r.feed(Feed::Rtds);
        assert_eq!((c.unkeyed, c.collisions, c.repeats), (3, 1, 1));
    }

    #[test]
    fn same_key_same_content_is_a_repeat() {
        let r = check_records(&[rec(Source::ClobBook, PC), rec(Source::ClobBook, PC)]);
        let c = r.feed(Feed::Clob);
        assert_eq!((c.collisions, c.repeats), (0, 1));
    }

    #[test]
    fn the_plan_key_collides_where_the_real_key_does_not() {
        let other_price = PC.replace(r#""price":"0.56""#, r#""price":"0.57""#);
        let r = check_records(&[
            rec(Source::ClobBook, PC),
            rec(Source::ClobBook, &other_price),
        ]);
        assert_eq!(r.collisions(), 0);
        assert_eq!(r.plan_key_collisions, 1);
        assert_eq!(r.no_size_key_collisions, 0);
    }

    #[test]
    fn noise_unkeyed_and_control_are_counted() {
        let r = check_records(&[
            rec(Source::Rtds, "PONG"),
            rec(Source::Rtds, "garbage"),
            rec(Source::Control, r#"{"subscribe":"559"}"#),
        ]);
        let c = r.feed(Feed::Rtds);
        assert_eq!(
            (c.frames, c.noise_frames, c.events, c.unkeyed),
            (2, 1, 1, 1)
        );
        assert_eq!(r.control_frames, 1);
    }
}
