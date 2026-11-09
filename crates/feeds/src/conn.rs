//! Connection options shared by every streaming feed, plus the reconnect
//! supervisor that keeps a feed alive across drops.

use crate::recorder::RecorderHandle;
use crate::recording::Source;
use poly_core::{Backoff, FeedMetrics};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Per-connection settings.
#[derive(Debug, Clone)]
pub struct ConnOpts {
    /// Return an error if no frame arrives for this long. A half-open TCP
    /// socket never errors on its own, so this is what turns a silent feed
    /// into a reconnect.
    pub idle_timeout: Duration,
    /// Optional health counters updated on every frame.
    pub metrics: Option<Arc<FeedMetrics>>,
    /// Optional recorder: every raw text frame is queued, unparsed, before
    /// the feed handles it. Never blocks (see `recorder` for the policy).
    pub tap: Option<Tap>,
}

/// A recorder handle bound to one feed's source tag.
#[derive(Clone)]
pub struct Tap {
    pub handle: RecorderHandle,
    pub source: Source,
}

impl std::fmt::Debug for Tap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tap").field("source", &self.source).finish()
    }
}

impl Default for ConnOpts {
    fn default() -> Self {
        Self {
            idle_timeout: Duration::from_secs(30),
            metrics: None,
            tap: None,
        }
    }
}

impl ConnOpts {
    pub fn with_metrics(metrics: Arc<FeedMetrics>) -> Self {
        Self {
            metrics: Some(metrics),
            ..Self::default()
        }
    }

    /// Attach a recorder for this feed.
    pub fn recording(mut self, handle: RecorderHandle, source: Source) -> Self {
        self.tap = Some(Tap { handle, source });
        self
    }

    /// Hand a raw frame to the recorder, if one is attached.
    pub fn tap_frame(&self, frame: &str) {
        if let Some(t) = &self.tap {
            t.handle.record(t.source, frame);
        }
    }

    pub(crate) fn connected(&self, up: bool) {
        if let Some(m) = &self.metrics {
            m.set_connected(up);
        }
    }

    pub(crate) fn message(&self, exchange_ts_millis: i64) {
        if let Some(m) = &self.metrics {
            m.record_message(exchange_ts_millis);
        }
    }

    pub(crate) fn malformed(&self) {
        if let Some(m) = &self.metrics {
            m.record_malformed();
        }
    }
}

/// Run `session` again and again, sleeping with `backoff` between attempts.
///
/// Each call to `session` should connect, stream until the connection fails,
/// and return the error. The supervisor marks the feed disconnected, counts a
/// reconnect, logs the cause with structured fields and waits. A session that
/// stayed up for `backoff.healthy_after` resets the delay, so one blip after
/// hours of uptime reconnects in 250 ms, while a flapping endpoint backs off to
/// the cap. `max_sessions` bounds the loop (tests); `None` runs forever.
pub async fn supervise<F, Fut>(
    feed: &str,
    metrics: Option<Arc<FeedMetrics>>,
    mut backoff: Backoff,
    max_sessions: Option<usize>,
    mut session: F,
) where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(), BoxError>>,
{
    let mut sessions = 0usize;
    loop {
        let started = Instant::now();
        let res = session().await;
        sessions += 1;
        let lasted = started.elapsed();
        if let Some(m) = &metrics {
            m.set_connected(false);
        }
        if max_sessions.is_some_and(|max| sessions >= max) {
            return;
        }
        let delay = backoff.on_session_end(lasted);
        if let Some(m) = &metrics {
            m.record_reconnect();
        }
        let error = match &res {
            Ok(()) => "closed".to_string(),
            Err(e) => e.to_string(),
        };
        tracing::warn!(
            feed,
            error = %error,
            session_ms = lasted.as_millis() as u64,
            retry_in_ms = delay.as_millis() as u64,
            attempt = sessions,
            "feed disconnected; reconnecting"
        );
        tokio::time::sleep(delay).await;
    }
}
