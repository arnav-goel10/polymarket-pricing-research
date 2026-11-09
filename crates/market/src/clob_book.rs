use crate::book::{Level, OrderBook};
use crate::sync::{ApplyOutcome, BookSide, LevelDelta, LocalBook, SeqPolicy};
use feeds::conn::{BoxError, ConnOpts};
use serde_json::Value;
use std::sync::OnceLock;

const CLOB_BASE: &str = "https://clob.polymarket.com";
pub const CLOB_MARKET_WS: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

static CLOB_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

fn clob_client() -> &'static reqwest::Client {
    CLOB_CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("clob reqwest client")
    })
}

fn levels(v: &Value, key: &str) -> Vec<Level> {
    v.get(key)
        .and_then(|a| a.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|l| {
                    let price: f64 = l.get("price")?.as_str()?.parse().ok()?;
                    let size: f64 = l.get("size")?.as_str()?.parse().ok()?;
                    // Reject non-finite/garbage levels (e.g. a stray "NaN") so they
                    // can't poison sorting or fill math.
                    (price.is_finite() && size.is_finite() && size > 0.0)
                        .then_some(Level { price, size })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Parse a CLOB `/book` response into an OrderBook.
pub fn parse_book(body: &str) -> Option<OrderBook> {
    let v: Value = serde_json::from_str(body).ok()?;
    // Polymarket's /book is NOT returned best-first. Enforce our invariant:
    // bids sorted by price descending, asks ascending, so `first()` is the top.
    let mut bids = levels(&v, "bids");
    let mut asks = levels(&v, "asks");
    bids.sort_by(|a, b| {
        b.price
            .partial_cmp(&a.price)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    asks.sort_by(|a, b| {
        a.price
            .partial_cmp(&b.price)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    Some(OrderBook { bids, asks })
}

/// Fetch the current order book for a token id (real-time client).
pub async fn fetch_book(
    token_id: &str,
) -> Result<OrderBook, Box<dyn std::error::Error + Send + Sync>> {
    let url = format!("{CLOB_BASE}/book?token_id={token_id}");
    let body = clob_client().get(&url).send().await?.text().await?;
    parse_book(&body).ok_or_else(|| "bad book json".into())
}

#[derive(Debug, Clone)]
pub struct StreamBook {
    pub book: OrderBook,
    pub exchange_ts_millis: Option<i64>,
}

fn parse_ts_millis(v: &Value) -> Option<i64> {
    v.get("timestamp").and_then(|x| {
        x.as_str()
            .and_then(|s| s.parse().ok())
            .or_else(|| x.as_i64())
    })
}

fn num(v: &Value, key: &str) -> Option<f64> {
    let x = v.get(key)?;
    x.as_str()
        .and_then(|s| s.parse().ok())
        .or_else(|| x.as_f64())
}

/// Strict level parse for the stream path: one bad level makes the whole
/// snapshot unusable (`None`) instead of silently producing a partial book.
fn strict_levels(v: &Value, key: &str) -> Option<Vec<Level>> {
    let Some(arr) = v.get(key) else {
        return Some(Vec::new());
    };
    arr.as_array()?
        .iter()
        .map(|l| {
            Some(Level {
                price: num(l, "price")?,
                size: num(l, "size")?,
            })
        })
        .collect()
}

/// Why the stream had to stop and resync.
#[derive(Debug, Clone, PartialEq)]
pub enum StreamError {
    /// The local book can no longer be trusted (gap, bad level, crossed).
    Desync(ApplyOutcome),
    /// A frame was not valid JSON; a lost delta cannot be ruled out.
    BadFrame,
}

impl std::fmt::Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamError::Desync(o) => write!(f, "clob book desynced ({o:?}); resync required"),
            StreamError::BadFrame => {
                write!(f, "clob book frame was not valid JSON; resync required")
            }
        }
    }
}

impl std::error::Error for StreamError {}

/// The Polymarket market channel as a state machine: feed it raw frames, get
/// back consistent books or a resync request.
///
/// Polymarket publishes no per-message sequence number, so ordering uses the
/// exchange timestamp under [`SeqPolicy::Monotonic`]: snapshots and deltas
/// older than the current book are dropped, and every applied message is
/// checked for malformed levels and a crossed book.
#[derive(Debug, Clone)]
pub struct ClobBookStream {
    token_id: String,
    book: LocalBook,
    scratch: Vec<LevelDelta>,
}

impl ClobBookStream {
    pub fn new(token_id: impl Into<String>) -> Self {
        Self {
            token_id: token_id.into(),
            book: LocalBook::probability(SeqPolicy::Monotonic),
            scratch: Vec::with_capacity(8),
        }
    }

    pub fn local_book(&self) -> &LocalBook {
        &self.book
    }

    fn seq_for(&self, v: &Value) -> u64 {
        parse_ts_millis(v)
            .map(|t| t.max(0) as u64)
            .or(self.book.last_seq())
            .unwrap_or(0)
    }

    fn emit(&self, v: &Value) -> StreamBook {
        StreamBook {
            book: self.book.book().clone(),
            exchange_ts_millis: parse_ts_millis(v),
        }
    }

    /// Apply one decoded event. `Ok(None)` means nothing to publish (another
    /// token, an ignored event type, or a stale message).
    pub fn on_event(&mut self, v: &Value) -> Result<Option<StreamBook>, StreamError> {
        match v.get("event_type").and_then(|x| x.as_str()) {
            Some("book") => {
                if v.get("asset_id").and_then(|x| x.as_str()) != Some(self.token_id.as_str()) {
                    return Ok(None);
                }
                let seq = self.seq_for(v);
                let (Some(bids), Some(asks)) = (strict_levels(v, "bids"), strict_levels(v, "asks"))
                else {
                    return Err(StreamError::Desync(ApplyOutcome::Malformed));
                };
                let outcome = self.book.apply_snapshot(seq, &bids, &asks);
                self.publish(outcome, v)
            }
            Some("price_change") => {
                let seq = self.seq_for(v);
                self.scratch.clear();
                let mut malformed = false;
                if let Some(changes) = v.get("price_changes").and_then(|x| x.as_array()) {
                    for c in changes {
                        if c.get("asset_id").and_then(|x| x.as_str())
                            != Some(self.token_id.as_str())
                        {
                            continue;
                        }
                        let side = match c.get("side").and_then(|x| x.as_str()) {
                            Some("BUY") => BookSide::Bid,
                            Some("SELL") => BookSide::Ask,
                            _ => {
                                malformed = true;
                                continue;
                            }
                        };
                        match (num(c, "price"), num(c, "size")) {
                            (Some(price), Some(size)) => {
                                self.scratch.push(LevelDelta { side, price, size })
                            }
                            _ => malformed = true,
                        }
                    }
                }
                if malformed {
                    if self.book.is_synced() {
                        return Err(StreamError::Desync(self.book.mark_desynced()));
                    }
                    return Ok(None);
                }
                if self.scratch.is_empty() {
                    return Ok(None);
                }
                let changes = std::mem::take(&mut self.scratch);
                let outcome = self.book.apply_delta(seq, &changes);
                self.scratch = changes;
                self.publish(outcome, v)
            }
            _ => Ok(None),
        }
    }

    fn publish(
        &mut self,
        outcome: ApplyOutcome,
        v: &Value,
    ) -> Result<Option<StreamBook>, StreamError> {
        match outcome {
            ApplyOutcome::Applied => Ok(Some(self.emit(v))),
            o if o.needs_resync() => Err(StreamError::Desync(o)),
            _ => Ok(None),
        }
    }

    /// Feed one raw text frame (a single event or an array of events).
    pub fn on_text<F: FnMut(StreamBook)>(
        &mut self,
        txt: &str,
        mut on_book: F,
    ) -> Result<usize, StreamError> {
        let root: Value = serde_json::from_str(txt).map_err(|_| StreamError::BadFrame)?;
        let mut emitted = 0;
        if let Some(arr) = root.as_array() {
            for item in arr {
                if let Some(update) = self.on_event(item)? {
                    on_book(update);
                    emitted += 1;
                }
            }
        } else if let Some(update) = self.on_event(&root)? {
            on_book(update);
            emitted += 1;
        }
        Ok(emitted)
    }
}

/// Stream books for `token_id` from the public market channel until the
/// connection fails or the book needs a resync.
pub async fn stream_book_ws<F>(token_id: &str, on_book: F) -> Result<(), BoxError>
where
    F: FnMut(StreamBook),
{
    stream_book_ws_with(CLOB_MARKET_WS, token_id, &ConnOpts::default(), on_book).await
}

/// [`stream_book_ws`] against an explicit URL with an idle timeout and
/// health counters. Returns `Err` on drop, silence, or desync; the caller
/// refetches a snapshot and reconnects.
pub async fn stream_book_ws_with<F>(
    url: &str,
    token_id: &str,
    opts: &ConnOpts,
    mut on_book: F,
) -> Result<(), BoxError>
where
    F: FnMut(StreamBook),
{
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let (ws, _) = tokio_tungstenite::connect_async(url).await?;
    if let Some(m) = &opts.metrics {
        m.set_connected(true);
    }
    let (mut write, mut read) = ws.split();
    let subscribe = serde_json::json!({
        "assets_ids": [token_id],
        "type": "market",
        "custom_feature_enabled": true,
    });
    write.send(Message::Text(subscribe.to_string())).await?;

    let mut state = ClobBookStream::new(token_id);
    loop {
        match tokio::time::timeout(opts.idle_timeout, read.next()).await {
            Err(_) => return Err("clob market ws timeout (no data)".into()),
            Ok(None) => return Err("clob market ws closed".into()),
            Ok(Some(Err(e))) => return Err(e.into()),
            Ok(Some(Ok(Message::Text(txt)))) => {
                let metrics = opts.metrics.as_deref();
                let res = state.on_text(&txt, |update| {
                    if let Some(m) = metrics {
                        m.record_message(update.exchange_ts_millis.unwrap_or(0));
                    }
                    on_book(update);
                });
                if let Err(e) = res {
                    if let Some(m) = metrics {
                        if matches!(
                            e,
                            StreamError::BadFrame | StreamError::Desync(ApplyOutcome::Malformed)
                        ) {
                            m.record_malformed();
                        }
                        m.record_resync();
                    }
                    tracing::warn!(feed = "clob_book", token_id, error = %e, "book resync");
                    return Err(Box::new(e));
                }
            }
            Ok(Some(Ok(Message::Ping(data)))) => {
                write.send(Message::Pong(data)).await?;
            }
            Ok(Some(Ok(Message::Close(_)))) => return Err("clob market ws closed by server".into()),
            Ok(Some(Ok(_))) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_abs_diff_eq;

    #[test]
    fn parses_clob_book_levels() {
        let body = r#"{"bids":[{"price":"0.49","size":"100"},{"price":"0.48","size":"200"}],
                       "asks":[{"price":"0.51","size":"100"}]}"#;
        let b = parse_book(body).unwrap();
        assert_abs_diff_eq!(b.best_bid().unwrap(), 0.49, epsilon = 1e-12);
        assert_abs_diff_eq!(b.best_ask().unwrap(), 0.51, epsilon = 1e-12);
    }

    #[test]
    fn sorts_unordered_levels_so_first_is_best() {
        // Polymarket returns bids ascending / asks descending (NOT best-first).
        let body = r#"{"bids":[{"price":"0.01","size":"5"},{"price":"0.70","size":"53"}],
                       "asks":[{"price":"0.99","size":"5"},{"price":"0.71","size":"261"}]}"#;
        let b = parse_book(body).unwrap();
        assert_abs_diff_eq!(b.best_bid().unwrap(), 0.70, epsilon = 1e-12);
        assert_abs_diff_eq!(b.best_ask().unwrap(), 0.71, epsilon = 1e-12);
    }

    const SNAP: &str = r#"{"event_type":"book","asset_id":"tok","timestamp":"123",
        "bids":[{"price":"0.48","size":"10"},{"price":"0.49","size":"5"}],
        "asks":[{"price":"0.53","size":"10"},{"price":"0.52","size":"5"}]}"#;

    fn collect(s: &mut ClobBookStream, txt: &str) -> Result<Vec<StreamBook>, StreamError> {
        let mut out = Vec::new();
        s.on_text(txt, |b| out.push(b))?;
        Ok(out)
    }

    #[test]
    fn parses_ws_book_snapshot() {
        let mut s = ClobBookStream::new("tok");
        let out = collect(&mut s, SNAP).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].exchange_ts_millis, Some(123));
        assert_abs_diff_eq!(out[0].book.best_bid().unwrap(), 0.49, epsilon = 1e-12);
        assert_abs_diff_eq!(out[0].book.best_ask().unwrap(), 0.52, epsilon = 1e-12);
    }

    #[test]
    fn applies_ws_price_change_delta() {
        let mut s = ClobBookStream::new("tok");
        collect(&mut s, SNAP).unwrap();
        let out = collect(
            &mut s,
            r#"{"event_type":"price_change","timestamp":"456","price_changes":[
                {"asset_id":"tok","price":"0.50","size":"8","side":"BUY"},
                {"asset_id":"tok","price":"0.52","size":"0","side":"SELL"},
                {"asset_id":"other","price":"0.99","size":"100","side":"BUY"}
            ]}"#,
        )
        .unwrap();
        assert_eq!(out[0].exchange_ts_millis, Some(456));
        assert_abs_diff_eq!(out[0].book.best_bid().unwrap(), 0.50, epsilon = 1e-12);
        assert_abs_diff_eq!(out[0].book.best_ask().unwrap(), 0.53, epsilon = 1e-12);
    }

    #[test]
    fn deltas_before_a_snapshot_are_not_published() {
        let mut s = ClobBookStream::new("tok");
        let out = collect(
            &mut s,
            r#"{"event_type":"price_change","timestamp":"1","price_changes":[
                {"asset_id":"tok","price":"0.50","size":"8","side":"BUY"}]}"#,
        )
        .unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn out_of_order_delta_is_dropped() {
        let mut s = ClobBookStream::new("tok");
        collect(&mut s, SNAP).unwrap();
        let out = collect(
            &mut s,
            r#"{"event_type":"price_change","timestamp":"100","price_changes":[
                {"asset_id":"tok","price":"0.50","size":"8","side":"BUY"}]}"#,
        )
        .unwrap();
        assert!(out.is_empty());
        assert_eq!(s.local_book().book().best_bid(), Some(0.49));
    }

    #[test]
    fn crossing_delta_requests_resync() {
        let mut s = ClobBookStream::new("tok");
        collect(&mut s, SNAP).unwrap();
        let err = collect(
            &mut s,
            r#"{"event_type":"price_change","timestamp":"200","price_changes":[
                {"asset_id":"tok","price":"0.60","size":"1","side":"BUY"}]}"#,
        )
        .unwrap_err();
        assert_eq!(err, StreamError::Desync(ApplyOutcome::Crossed));
        assert!(!s.local_book().is_synced());
    }

    #[test]
    fn garbage_level_and_bad_json_request_resync() {
        let mut s = ClobBookStream::new("tok");
        collect(&mut s, SNAP).unwrap();
        let err = collect(
            &mut s,
            r#"{"event_type":"price_change","timestamp":"200","price_changes":[
                {"asset_id":"tok","price":"abc","size":"1","side":"BUY"}]}"#,
        )
        .unwrap_err();
        assert_eq!(err, StreamError::Desync(ApplyOutcome::Malformed));
        let mut s = ClobBookStream::new("tok");
        assert_eq!(
            collect(&mut s, "{not json").unwrap_err(),
            StreamError::BadFrame
        );
    }
}
