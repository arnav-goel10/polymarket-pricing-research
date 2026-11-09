//! Polymarket Real-Time Data Socket (RTDS) adapter.
//!
//! `wss://ws-live-data.polymarket.com` — no auth. Streams crypto prices from
//! two sources, both with payload `{symbol, timestamp, value}`:
//!   - `crypto_prices`            (Binance source)
//!   - `crypto_prices_chainlink`  (Chainlink source, symbol `btc/usd`) — the
//!     settlement-relevant price, usable for testing without a paid key.
//!
//! On subscribe, a topic first sends a snapshot (`payload.data[]`, a run of
//! recent `{timestamp,value}` points — a free volatility warm-up history); then real-time
//! `update` frames carry a single `payload.value`. Keep-alive: `PING` ~5s.

use crate::conn::{supervise, BoxError, ConnOpts};
use crate::tick::{FeedSource, Tick};
use poly_core::Backoff;
use serde_json::Value;
use std::time::Duration;

pub const RTDS_URL: &str = "wss://ws-live-data.polymarket.com";

/// Subscribe envelope for BTC on both the Binance and Chainlink price topics.
pub fn subscribe_message() -> String {
    r#"{"action":"subscribe","subscriptions":[{"topic":"crypto_prices","type":"update","filters":"btcusdt"},{"topic":"crypto_prices_chainlink","type":"*","filters":"{\"symbol\":\"btc/usd\"}"}]}"#
        .to_string()
}

fn source_for_topic(topic: &str) -> Option<FeedSource> {
    match topic {
        "crypto_prices" => Some(FeedSource::Binance),
        "crypto_prices_chainlink" => Some(FeedSource::Chainlink),
        _ => None,
    }
}

/// Parse a single real-time `update` message into a normalized tick.
/// Returns `None` for non-price/snapshot/malformed frames (use [`parse_frame`]
/// to also capture snapshots).
pub fn parse_tick(raw: &str) -> Option<Tick> {
    let root: Value = serde_json::from_str(raw).ok()?;
    let source = source_for_topic(root.get("topic")?.as_str()?)?;
    let payload = root.get("payload")?;
    let price = payload.get("value")?.as_f64()?;
    let ts_millis = payload
        .get("timestamp")
        .and_then(|v| v.as_i64())
        .or_else(|| root.get("timestamp").and_then(|v| v.as_i64()))
        .unwrap_or(0);
    Some(Tick {
        source,
        price,
        ts_millis,
    })
}

/// Parse any RTDS price frame into 0+ ticks. Handles both single real-time updates
/// (`payload.value`) and the snapshot/backfill (`payload.data[]`).
pub fn parse_frame(raw: &str) -> Vec<Tick> {
    let Ok(root) = serde_json::from_str::<Value>(raw) else {
        return Vec::new();
    };
    let Some(source) = root
        .get("topic")
        .and_then(|t| t.as_str())
        .and_then(source_for_topic)
    else {
        return Vec::new();
    };
    let Some(payload) = root.get("payload") else {
        return Vec::new();
    };
    if let Some(arr) = payload.get("data").and_then(|d| d.as_array()) {
        return arr
            .iter()
            .filter_map(|p| {
                Some(Tick {
                    source,
                    price: p.get("value")?.as_f64()?,
                    ts_millis: p.get("timestamp").and_then(|v| v.as_i64()).unwrap_or(0),
                })
            })
            .collect();
    }
    parse_tick(raw).into_iter().collect()
}

/// A frame that yielded no ticks is malformed if it is not JSON (keep-alive
/// `PONG` and the empty subscription ack aside) or if it is on a price topic but carries no usable price.
/// Frames on other topics are expected noise.
fn is_malformed(raw: &str) -> bool {
    // RTDS acknowledges a subscription with an empty text frame.
    if raw == "PONG" || raw.trim().is_empty() {
        return false;
    }
    match serde_json::from_str::<Value>(raw) {
        Err(_) => true,
        Ok(root) => root
            .get("topic")
            .and_then(|t| t.as_str())
            .and_then(source_for_topic)
            .is_some(),
    }
}

/// Real-time connection shell: connect, subscribe to BTC on both topics, keep alive
/// with periodic PINGs, and emit normalized ticks (snapshots + real-time updates)
/// until the socket errors.
pub async fn run<F>(url: &str, on_tick: F) -> Result<(), BoxError>
where
    F: FnMut(Tick),
{
    run_with(url, &ConnOpts::default(), on_tick).await
}

/// [`run`] with an explicit idle timeout and optional health counters.
pub async fn run_with<F>(url: &str, opts: &ConnOpts, mut on_tick: F) -> Result<(), BoxError>
where
    F: FnMut(Tick),
{
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let (ws, _) = tokio_tungstenite::connect_async(url).await?;
    opts.connected(true);
    let (mut write, mut read) = ws.split();
    write.send(Message::Text(subscribe_message())).await?;

    let ping_every = opts.idle_timeout.min(Duration::from_secs(5)) / 2;
    let mut ping = tokio::time::interval(ping_every.max(Duration::from_millis(10)));
    // Data-inactivity guard: a silently half-open TCP socket leaves `read.next()`
    // pending forever with no error, so the loop would spin sending PINGs into the
    // void and never reconnect, leaving the settlement-critical Chainlink price
    // frozen. If no frame arrives within the idle timeout, return an error so the
    // supervisor reconnects.
    let mut last_data = tokio::time::Instant::now();
    loop {
        tokio::select! {
            _ = ping.tick() => {
                if last_data.elapsed() > opts.idle_timeout {
                    return Err("rtds inactivity timeout (no data)".into());
                }
                write.send(Message::Text("PING".into())).await?;
            }
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(txt))) => {
                        last_data = tokio::time::Instant::now();
                        opts.tap_frame(&txt);
                        let ticks = parse_frame(&txt);
                        if ticks.is_empty() && is_malformed(&txt) {
                            opts.malformed();
                        }
                        for t in ticks {
                            opts.message(t.ts_millis);
                            on_tick(t);
                        }
                    }
                    Some(Ok(Message::Close(_))) => return Err("rtds closed by server".into()),
                    Some(Ok(_)) => {
                        last_data = tokio::time::Instant::now(); // any frame = alive
                    }
                    Some(Err(e)) => return Err(e.into()),
                    None => return Err("rtds stream ended".into()),
                }
            }
        }
    }
}

/// Reconnect forever with capped exponential backoff.
pub async fn run_reconnecting<F>(url: &str, opts: ConnOpts, on_tick: F)
where
    F: FnMut(Tick) + Clone,
{
    let metrics = opts.metrics.clone();
    supervise("rtds", metrics, Backoff::default(), None, || {
        run_with(url, &opts, on_tick.clone())
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_abs_diff_eq;

    #[test]
    fn parses_binance_source_as_binance_tick() {
        let msg = r#"{"topic":"crypto_prices","type":"update","timestamp":1753314088421,
            "payload":{"symbol":"btcusdt","timestamp":1753314088395,"value":67234.50}}"#;
        let t = parse_tick(msg).unwrap();
        assert_eq!(t.source, FeedSource::Binance);
        assert_abs_diff_eq!(t.price, 67234.50, epsilon = 1e-9);
        assert_eq!(t.ts_millis, 1753314088395);
    }

    #[test]
    fn parses_chainlink_source_as_chainlink_tick() {
        let msg = r#"{"topic":"crypto_prices_chainlink","type":"update","timestamp":1753314088421,
            "payload":{"symbol":"btc/usd","timestamp":1753314088395,"value":67250.0}}"#;
        let t = parse_tick(msg).unwrap();
        assert_eq!(t.source, FeedSource::Chainlink);
        assert_abs_diff_eq!(t.price, 67250.0, epsilon = 1e-9);
    }

    #[test]
    fn parses_snapshot_backfill_into_many_ticks() {
        let msg = r#"{"topic":"crypto_prices","type":"subscribe","timestamp":1782020993474,
            "payload":{"symbol":"btc/usd","data":[
                {"timestamp":1782020934000,"value":64175.9},
                {"timestamp":1782020935000,"value":64177.1},
                {"timestamp":1782020936000,"value":64178.5}]}}"#;
        let ticks = parse_frame(msg);
        assert_eq!(ticks.len(), 3);
        assert_eq!(ticks[0].source, FeedSource::Binance);
        assert_abs_diff_eq!(ticks[2].price, 64178.5, epsilon = 1e-9);
    }

    #[test]
    fn parse_frame_handles_single_update_and_ignores_noise() {
        let upd = r#"{"topic":"crypto_prices_chainlink","type":"update","payload":{"value":64180.0,"timestamp":1}}"#;
        assert_eq!(parse_frame(upd).len(), 1);
        assert!(parse_frame(r#"{"topic":"comments","payload":{}}"#).is_empty());
        assert!(parse_frame("not json").is_empty());
    }

    #[test]
    fn ignores_non_price_topics() {
        assert!(
            parse_tick(r#"{"topic":"comments","type":"reaction_created","payload":{}}"#).is_none()
        );
        assert!(parse_tick(
            r#"{"topic":"crypto_prices","type":"update","payload":{"symbol":"btcusdt"}}"#
        )
        .is_none());
    }

    #[test]
    fn subscribe_message_targets_both_btc_topics() {
        let m = subscribe_message();
        assert!(m.contains("crypto_prices"));
        assert!(m.contains("crypto_prices_chainlink"));
        assert!(m.contains("btcusdt"));
        assert!(m.contains("btc/usd"));
        let _: serde_json::Value = serde_json::from_str(&m).unwrap();
    }
}
