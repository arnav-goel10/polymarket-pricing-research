//! Prometheus text exposition for feed health.

use poly_core::metrics::wall_millis;
use poly_core::FeedMetrics;
use std::fmt::Write;
use std::sync::Arc;
use std::time::Instant;

/// Every feed's counters, rendered by `GET /metrics`.
#[derive(Debug)]
pub struct Metrics {
    pub feeds: Vec<Arc<FeedMetrics>>,
    /// Raw-frame recorder counters, when recording is enabled.
    pub recorder: Option<Arc<feeds::recorder::RecorderStats>>,
    started: Instant,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

/// Stale thresholds match the engine's own freshness gates: the risk check
/// blocks on a reference price older than 5 s and a book older than 3 s.
pub const RTDS_STALE_MILLIS: i64 = 5_000;
pub const BINANCE_STALE_MILLIS: i64 = 5_000;
pub const BOOK_STALE_MILLIS: i64 = 3_000;

impl Metrics {
    pub fn new(feeds: Vec<Arc<FeedMetrics>>) -> Self {
        Self {
            feeds,
            recorder: None,
            started: Instant::now(),
        }
    }

    pub fn feed(&self, name: &str) -> Option<&Arc<FeedMetrics>> {
        self.feeds.iter().find(|f| f.name == name)
    }

    /// Feeds that are past their freshness threshold. Empty means ready.
    pub fn stale_feeds_at(&self, now_millis: i64) -> Vec<&'static str> {
        self.feeds
            .iter()
            .map(|f| f.snapshot_at(now_millis))
            .filter(|s| s.stale)
            .map(|s| s.name)
            .collect()
    }

    pub fn render(&self) -> String {
        self.render_at(wall_millis())
    }

    /// Render in Prometheus text format 0.0.4.
    pub fn render_at(&self, now_millis: i64) -> String {
        let snaps: Vec<_> = self
            .feeds
            .iter()
            .map(|f| f.snapshot_at(now_millis))
            .collect();
        let mut out = String::with_capacity(2048);
        let mut family = |name: &str, kind: &str, help: &str, values: Vec<(&str, Option<f64>)>| {
            let _ = writeln!(out, "# HELP {name} {help}");
            let _ = writeln!(out, "# TYPE {name} {kind}");
            for (feed, v) in values {
                if let Some(v) = v {
                    let _ = writeln!(out, "{name}{{feed=\"{feed}\"}} {v}");
                }
            }
        };
        let per = |f: &dyn Fn(&poly_core::FeedSnapshot) -> Option<f64>| {
            snaps.iter().map(|s| (s.name, f(s))).collect::<Vec<_>>()
        };
        family(
            "poly_feed_messages_total",
            "counter",
            "Well-formed messages received.",
            per(&|s| Some(s.messages as f64)),
        );
        family(
            "poly_feed_malformed_total",
            "counter",
            "Frames that could not be decoded.",
            per(&|s| Some(s.malformed as f64)),
        );
        family(
            "poly_feed_reconnects_total",
            "counter",
            "Reconnects after a drop, timeout or resync.",
            per(&|s| Some(s.reconnects as f64)),
        );
        family(
            "poly_feed_resyncs_total",
            "counter",
            "Order-book resyncs after a gap, malformed level or crossed book.",
            per(&|s| Some(s.resyncs as f64)),
        );
        family(
            "poly_feed_connected",
            "gauge",
            "1 while the feed socket is open.",
            per(&|s| Some(f64::from(u8::from(s.connected)))),
        );
        family(
            "poly_feed_messages_per_second",
            "gauge",
            "Messages per second over the last 10 complete seconds.",
            per(&|s| Some(s.messages_per_sec)),
        );
        family(
            "poly_feed_last_message_age_seconds",
            "gauge",
            "Seconds since the last message.",
            per(&|s| s.age_millis.map(|a| a as f64 / 1000.0)),
        );
        family(
            "poly_feed_lag_seconds",
            "gauge",
            "Receive time minus exchange timestamp of the latest message.",
            per(&|s| s.lag_millis.map(|a| a as f64 / 1000.0)),
        );
        family(
            "poly_feed_stale",
            "gauge",
            "1 when the feed is older than its freshness threshold; decisions are blocked.",
            per(&|s| Some(f64::from(u8::from(s.stale)))),
        );
        if let Some(r) = &self.recorder {
            use std::sync::atomic::Ordering::Relaxed;
            for (name, kind, help, v) in [
                (
                    "poly_recorder_written_total",
                    "counter",
                    "Frames written to the recording.",
                    r.written.load(Relaxed),
                ),
                (
                    "poly_recorder_dropped_total",
                    "counter",
                    "Frames dropped because the recorder queue was full.",
                    r.dropped.load(Relaxed),
                ),
                (
                    "poly_recorder_queue_depth",
                    "gauge",
                    "Frames queued and not yet written.",
                    r.queue_depth(),
                ),
                (
                    "poly_recorder_queue_capacity",
                    "gauge",
                    "Recorder queue capacity in frames.",
                    r.capacity.load(Relaxed),
                ),
            ] {
                let _ = writeln!(out, "# HELP {name} {help}");
                let _ = writeln!(out, "# TYPE {name} {kind}");
                let _ = writeln!(out, "{name} {v}");
            }
        }
        let _ = writeln!(out, "# HELP poly_uptime_seconds Process uptime.");
        let _ = writeln!(out, "# TYPE poly_uptime_seconds gauge");
        let _ = writeln!(
            out,
            "poly_uptime_seconds {}",
            self.started.elapsed().as_secs_f64()
        );
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_valid_exposition_for_each_feed() {
        let a = Arc::new(FeedMetrics::new("rtds", 5_000));
        let b = Arc::new(FeedMetrics::new("clob_book", 3_000));
        a.record_message_at(1_000_000, 999_750);
        a.record_reconnect();
        b.record_resync();
        let m = Metrics::new(vec![a, b]);
        let text = m.render_at(1_001_000);
        assert!(text.contains("poly_feed_messages_total{feed=\"rtds\"} 1\n"));
        assert!(text.contains("poly_feed_reconnects_total{feed=\"rtds\"} 1\n"));
        assert!(text.contains("poly_feed_resyncs_total{feed=\"clob_book\"} 1\n"));
        assert!(text.contains("poly_feed_lag_seconds{feed=\"rtds\"} 0.25\n"));
        assert!(text.contains("poly_feed_stale{feed=\"rtds\"} 0\n"));
        // Never-seen feed: stale, and no age sample is emitted.
        assert!(text.contains("poly_feed_stale{feed=\"clob_book\"} 1\n"));
        assert!(!text.contains("poly_feed_last_message_age_seconds{feed=\"clob_book\"}"));
        // Every non-comment line is `name{labels} value` or `name value`.
        for line in text.lines().filter(|l| !l.starts_with('#')) {
            let (name, value) = line.rsplit_once(' ').unwrap();
            assert!(name.starts_with("poly_"), "{line}");
            value.parse::<f64>().unwrap();
        }
    }
}
