//! Lock-free per-feed health counters shared by the feed tasks and the
//! `/metrics` endpoint.
//!
//! Every method takes `&self` and uses relaxed atomics, so a feed callback can
//! record a message without taking a lock on the hot path.

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering::Relaxed};
use std::time::{SystemTime, UNIX_EPOCH};

/// Width of the sliding window used for the messages-per-second gauge.
pub const RATE_WINDOW_SECS: usize = 10;

pub fn wall_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Health counters for one upstream feed.
#[derive(Debug)]
pub struct FeedMetrics {
    pub name: &'static str,
    /// Age after which the feed is reported stale.
    pub stale_after_millis: i64,
    messages: AtomicU64,
    malformed: AtomicU64,
    reconnects: AtomicU64,
    resyncs: AtomicU64,
    connected: AtomicBool,
    last_message_millis: AtomicI64,
    last_exchange_ts_millis: AtomicI64,
    bucket_counts: [AtomicU64; RATE_WINDOW_SECS],
    bucket_secs: [AtomicI64; RATE_WINDOW_SECS],
}

/// A consistent-enough copy of one feed's counters at `now_millis`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FeedSnapshot {
    pub name: &'static str,
    pub messages: u64,
    pub malformed: u64,
    pub reconnects: u64,
    pub resyncs: u64,
    pub connected: bool,
    /// Milliseconds since the last message, `None` before the first one.
    pub age_millis: Option<i64>,
    /// Receive time minus exchange timestamp of the latest message.
    pub lag_millis: Option<i64>,
    pub messages_per_sec: f64,
    pub stale: bool,
}

impl FeedMetrics {
    pub fn new(name: &'static str, stale_after_millis: i64) -> Self {
        Self {
            name,
            stale_after_millis,
            messages: AtomicU64::new(0),
            malformed: AtomicU64::new(0),
            reconnects: AtomicU64::new(0),
            resyncs: AtomicU64::new(0),
            connected: AtomicBool::new(false),
            last_message_millis: AtomicI64::new(0),
            last_exchange_ts_millis: AtomicI64::new(0),
            bucket_counts: Default::default(),
            bucket_secs: Default::default(),
        }
    }

    /// Record one well-formed message received at `now_millis`. Pass the
    /// exchange timestamp when the venue provides one (0 or negative = none).
    pub fn record_message_at(&self, now_millis: i64, exchange_ts_millis: i64) {
        self.messages.fetch_add(1, Relaxed);
        self.last_message_millis.store(now_millis, Relaxed);
        if exchange_ts_millis > 0 {
            self.last_exchange_ts_millis
                .store(exchange_ts_millis, Relaxed);
        }
        let sec = now_millis.div_euclid(1000);
        let i = sec.rem_euclid(RATE_WINDOW_SECS as i64) as usize;
        if self.bucket_secs[i].swap(sec, Relaxed) != sec {
            self.bucket_counts[i].store(0, Relaxed);
        }
        self.bucket_counts[i].fetch_add(1, Relaxed);
    }

    pub fn record_message(&self, exchange_ts_millis: i64) {
        self.record_message_at(wall_millis(), exchange_ts_millis);
    }

    /// A frame arrived but could not be parsed into a usable event.
    pub fn record_malformed(&self) {
        self.malformed.fetch_add(1, Relaxed);
    }

    pub fn record_reconnect(&self) {
        self.reconnects.fetch_add(1, Relaxed);
    }

    /// The local book fell out of sync and a fresh snapshot was requested.
    pub fn record_resync(&self) {
        self.resyncs.fetch_add(1, Relaxed);
    }

    pub fn set_connected(&self, connected: bool) {
        self.connected.store(connected, Relaxed);
    }

    /// Messages per second averaged over the last fully elapsed
    /// [`RATE_WINDOW_SECS`] seconds.
    pub fn messages_per_sec_at(&self, now_millis: i64) -> f64 {
        let now_sec = now_millis.div_euclid(1000);
        let total: u64 = (0..RATE_WINDOW_SECS)
            .filter(|&i| {
                let s = self.bucket_secs[i].load(Relaxed);
                s < now_sec && s >= now_sec - RATE_WINDOW_SECS as i64
            })
            .map(|i| self.bucket_counts[i].load(Relaxed))
            .sum();
        total as f64 / RATE_WINDOW_SECS as f64
    }

    pub fn snapshot_at(&self, now_millis: i64) -> FeedSnapshot {
        let last = self.last_message_millis.load(Relaxed);
        let age_millis = (last > 0).then(|| (now_millis - last).max(0));
        let exch = self.last_exchange_ts_millis.load(Relaxed);
        let lag_millis = (last > 0 && exch > 0).then(|| last - exch);
        FeedSnapshot {
            name: self.name,
            messages: self.messages.load(Relaxed),
            malformed: self.malformed.load(Relaxed),
            reconnects: self.reconnects.load(Relaxed),
            resyncs: self.resyncs.load(Relaxed),
            connected: self.connected.load(Relaxed),
            age_millis,
            lag_millis,
            messages_per_sec: self.messages_per_sec_at(now_millis),
            stale: age_millis.is_none_or(|a| a > self.stale_after_millis),
        }
    }

    pub fn snapshot(&self) -> FeedSnapshot {
        self.snapshot_at(wall_millis())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_feed_is_stale_until_first_message() {
        let m = FeedMetrics::new("t", 5_000);
        let s = m.snapshot_at(1_000_000);
        assert!(s.stale);
        assert_eq!(s.age_millis, None);
        m.record_message_at(1_000_000, 999_900);
        let s = m.snapshot_at(1_000_100);
        assert!(!s.stale);
        assert_eq!(s.age_millis, Some(100));
        assert_eq!(s.lag_millis, Some(100));
        assert!(m.snapshot_at(1_006_000).stale);
    }

    #[test]
    fn rate_counts_only_completed_seconds_in_window() {
        let m = FeedMetrics::new("t", 5_000);
        for sec in 100..110 {
            for k in 0..20 {
                m.record_message_at(sec * 1000 + k, 0);
            }
        }
        // At t=110.5s the window is seconds 100..=109: 200 messages / 10s.
        assert!((m.messages_per_sec_at(110_500) - 20.0).abs() < 1e-9);
        // Much later, nothing recent.
        assert_eq!(m.messages_per_sec_at(500_000), 0.0);
    }

    #[test]
    fn counters_accumulate() {
        let m = FeedMetrics::new("t", 5_000);
        m.record_malformed();
        m.record_reconnect();
        m.record_reconnect();
        m.record_resync();
        m.set_connected(true);
        let s = m.snapshot_at(1);
        assert_eq!((s.malformed, s.reconnects, s.resyncs), (1, 2, 1));
        assert!(s.connected);
    }
}
