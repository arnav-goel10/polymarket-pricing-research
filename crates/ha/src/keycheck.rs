//! Measure the event key assumption on a recording.
//!
//! The whole merge design rests on one claim: two events with the same key
//! are the same event. This walks a recording and counts, per feed, how
//! many events got a venue key, how many did not, and how many keys were
//! shared by events with different content (a collision, which would make
//! the merger drop a real event). Same key with the same content is counted
//! as a repeat: the venue sent the same thing twice, and dropping the
//! second copy loses nothing.

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
        let kind = for_each_event_with_content(rec.source, &rec.frame, |s, content| {
            c.events += 1;
            if s.key.kind.is_keyed() {
                c.keyed += 1;
            } else {
                c.unkeyed += 1;
            }
            match first.entry(s.key) {
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
            plan_key(
                &rec.frame,
                &mut self.plan,
                &mut self.report.plan_key_collisions,
            );
        }
    }

    pub fn finish(self) -> KeyReport {
        self.report
    }
}

/// The price change key as first planned, for comparison only.
fn plan_key(frame: &str, seen: &mut HashMap<u128, u128>, collisions: &mut u64) {
    let Ok(root) = serde_json::from_str::<Value>(frame) else {
        return;
    };
    let items = match root.as_array() {
        Some(a) => a.as_slice(),
        None => std::slice::from_ref(&root),
    };
    for e in items {
        if e.get("event_type").and_then(Value::as_str) != Some("price_change") {
            continue;
        }
        let Some(ts) = clob_ts(e) else { continue };
        let changes = e.get("price_changes").and_then(Value::as_array);
        for c in changes.into_iter().flatten() {
            let asset = c.get("asset_id").and_then(Value::as_str);
            let hash = c.get("hash").and_then(Value::as_str);
            let (Some(asset), Some(hash)) = (asset, hash) else {
                continue;
            };
            let mut k = Fingerprint::new();
            k.field(asset.as_bytes()).field(hash.as_bytes()).int(ts);
            let mut v = Fingerprint::new();
            v.value(c);
            match seen.entry(k.finish()) {
                Entry::Vacant(x) => {
                    x.insert(v.finish());
                }
                Entry::Occupied(o) => {
                    if *o.get() != v.finish() {
                        *collisions += 1;
                    }
                }
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
        let other_size = PC.replace(r#""size":"309.4""#, r#""size":"1""#);
        let r = check_records(&[
            rec(Source::ClobBook, PC),
            rec(Source::ClobBook, &other_size),
        ]);
        let c = r.feed(Feed::Clob);
        assert_eq!((c.events, c.keyed, c.collisions, c.repeats), (2, 2, 1, 0));
        assert_eq!(r.collisions(), 1);
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
