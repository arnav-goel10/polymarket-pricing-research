//! The three venue feeds wired to a recorder queue, shared by the `record`
//! and `node` binaries.
//!
//! Every raw text frame from RTDS, Binance and the CLOB order book goes to
//! the recorder handle untouched, and the order-book feed writes a control
//! record each time it (re)subscribes so a reader knows which token the
//! following frames belong to.

use feeds::conn::{supervise, ConnOpts};
use feeds::recorder::RecorderHandle;
use feeds::recording::Source;
use market::clob_book;
use poly_core::{Backoff, FeedMetrics};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{info, warn};

/// Where each feed connects. `token` pins the order-book subscription;
/// without it the current BTC five-minute market is discovered and followed.
#[derive(Debug, Clone)]
pub struct FeedUrls {
    pub rtds: String,
    pub binance: String,
    pub clob: String,
    pub token: Option<String>,
}

impl Default for FeedUrls {
    fn default() -> Self {
        Self {
            rtds: feeds::rtds::RTDS_URL.to_string(),
            binance: feeds::binance::default_url().to_string(),
            clob: clob_book::CLOB_MARKET_WS.to_string(),
            token: None,
        }
    }
}

/// Optional health counters, one per feed, for readiness and `/metrics`.
#[derive(Debug, Clone)]
pub struct FeedHealth {
    pub rtds: Arc<FeedMetrics>,
    pub binance: Arc<FeedMetrics>,
    pub clob: Arc<FeedMetrics>,
}

impl Default for FeedHealth {
    fn default() -> Self {
        Self {
            rtds: Arc::new(FeedMetrics::new("rtds", 30_000)),
            binance: Arc::new(FeedMetrics::new("binance", 30_000)),
            clob: Arc::new(FeedMetrics::new("clob_book", 30_000)),
        }
    }
}

impl FeedHealth {
    pub fn all(&self) -> [&Arc<FeedMetrics>; 3] {
        [&self.rtds, &self.binance, &self.clob]
    }

    /// True when every feed has a live connection.
    pub fn all_connected(&self) -> bool {
        self.all().iter().all(|m| m.snapshot().connected)
    }
}

/// Whether the recorder queue is keeping up, for `/readyz`.
///
/// A frame the queue drops never reaches the ring, so it gets no `seq` and
/// a client cannot see the loss on the wire. A writer stalled on the disk
/// stops the stream while `hb` keeps coming. Neither shows in the stream,
/// so the node reports itself not ready instead: for [`DROP_HOLD`] after
/// any rise in the drop count, and while the queue holds more than a
/// sixteenth of its capacity (4,096 of 65,536 frames, about 2 s at the
/// measured rate; it is near zero when the writer keeps up).
#[derive(Debug, Default)]
pub struct Backlog {
    dropped: u64,
    rose_at: Option<std::time::Instant>,
}

pub const DROP_HOLD: Duration = Duration::from_secs(5);

impl Backlog {
    pub fn healthy(
        &mut self,
        now: std::time::Instant,
        dropped: u64,
        depth: u64,
        capacity: u64,
    ) -> bool {
        if dropped > self.dropped {
            self.dropped = dropped;
            self.rose_at = Some(now);
        }
        let recent = self
            .rose_at
            .is_some_and(|t| now.saturating_duration_since(t) < DROP_HOLD);
        !recent && depth.saturating_mul(16) <= capacity
    }
}

#[cfg(test)]
mod backlog_tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn a_drop_or_a_deep_queue_makes_the_node_not_ready() {
        let mut b = Backlog::default();
        let t = Instant::now();
        let s = |ms| t + Duration::from_millis(ms);
        assert!(b.healthy(t, 0, 0, 65_536));
        assert!(b.healthy(s(10), 0, 4_096, 65_536));
        assert!(!b.healthy(s(20), 0, 4_097, 65_536), "a stalled writer");
        assert!(!b.healthy(s(30), 3, 0, 65_536), "a drop");
        assert!(!b.healthy(s(5_029), 3, 0, 65_536));
        assert!(b.healthy(s(5_030), 3, 0, 65_536), "held 5 s, then clear");
        assert!(!b.healthy(s(6_000), 4, 0, 65_536), "a later drop again");
    }
}

fn opts(metrics: Option<&Arc<FeedMetrics>>) -> ConnOpts {
    match metrics {
        Some(m) => ConnOpts::with_metrics(m.clone()),
        None => ConnOpts::default(),
    }
}

/// Spawn the three feed tasks. Abort and await the handles to stop them;
/// each task owns a clone of `rec`, so the recorder drains only after that.
pub fn spawn_feeds(
    rec: &RecorderHandle,
    urls: &FeedUrls,
    health: Option<&FeedHealth>,
) -> Vec<JoinHandle<()>> {
    let mut tasks = Vec::new();
    {
        let metrics = health.map(|h| h.rtds.clone());
        let opts = opts(metrics.as_ref()).recording(rec.clone(), Source::Rtds);
        let url = urls.rtds.clone();
        tasks.push(tokio::spawn(async move {
            supervise("rtds", metrics, Backoff::default(), None, || {
                feeds::rtds::run_with(&url, &opts, |_| {})
            })
            .await
        }));
    }
    {
        let metrics = health.map(|h| h.binance.clone());
        let opts = opts(metrics.as_ref()).recording(rec.clone(), Source::Binance);
        let url = urls.binance.clone();
        tasks.push(tokio::spawn(async move {
            supervise("binance", metrics, Backoff::default(), None, || {
                feeds::binance::run_with(&url, &opts, |_| {})
            })
            .await
        }));
    }
    tasks.push(tokio::spawn(book_loop(
        rec.clone(),
        urls.clob.clone(),
        urls.token.clone(),
        health.map(|h| h.clob.clone()),
    )));
    tasks
}

/// The token of the current BTC five-minute market, if discovery works.
pub async fn current_token() -> Option<String> {
    let now = poly_core::metrics::wall_millis();
    match market::gamma::fetch_current_btc_updown(now, 300).await {
        Ok(Some(m)) => Some(m.yes_token_id),
        Ok(None) => None,
        Err(e) => {
            warn!(error = %e, "market discovery failed");
            None
        }
    }
}

/// Order-book feed: subscribe to the current token, follow window changes,
/// and write a control record at every (re)subscription.
pub async fn book_loop(
    rec: RecorderHandle,
    url: String,
    fixed: Option<String>,
    metrics: Option<Arc<FeedMetrics>>,
) {
    let mut backoff = Backoff::default();
    loop {
        let token = match &fixed {
            Some(t) => Some(t.clone()),
            None => current_token().await,
        };
        let Some(token) = token else {
            tokio::time::sleep(Duration::from_secs(2)).await;
            continue;
        };
        rec.record(
            Source::Control,
            &serde_json::json!({ "subscribe": token }).to_string(),
        );
        let opts = opts(metrics.as_ref()).recording(rec.clone(), Source::ClobBook);
        let started = std::time::Instant::now();
        let stream = clob_book::stream_book_ws_with(&url, &token, &opts, |_| {});
        tokio::pin!(stream);
        loop {
            tokio::select! {
                res = &mut stream => {
                    if let Some(m) = &metrics {
                        m.set_connected(false);
                        m.record_reconnect();
                    }
                    let delay = backoff.on_session_end(started.elapsed());
                    warn!(feed = "clob_book", error = ?res.err(), retry_in_ms = delay.as_millis() as u64, "reconnecting");
                    tokio::time::sleep(delay).await;
                    break;
                }
                _ = tokio::time::sleep(Duration::from_secs(5)), if fixed.is_none() => {
                    if current_token().await.is_some_and(|t| t != token) {
                        info!("market window rolled; resubscribing");
                        break;
                    }
                }
            }
        }
        if let Some(m) = &metrics {
            m.set_connected(false);
        }
    }
}

/// Resolves on SIGINT or SIGTERM with the signal's name.
pub async fn shutdown_signal() -> &'static str {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => "SIGINT",
            _ = term.recv() => "SIGTERM",
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        "SIGINT"
    }
}
