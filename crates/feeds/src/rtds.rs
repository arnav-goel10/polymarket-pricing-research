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

use crate::tick::{FeedSource, Tick};
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

/// Real-time connection shell: connect, subscribe to BTC on both topics, keep alive
/// with periodic PINGs, and emit normalized ticks (snapshots + real-time updates)
/// until the socket errors.
pub async fn run<F>(
    url: &str,
    mut on_tick: F,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    F: FnMut(Tick),
{
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let (ws, _) = tokio_tungstenite::connect_async(url).await?;
    let (mut write, mut read) = ws.split();
    write.send(Message::Text(subscribe_message())).await?;

    let mut ping = tokio::time::interval(std::time::Duration::from_secs(5));
    // Data-inactivity guard: a silently half-open TCP socket leaves `read.next()`
    // pending forever with no error, so the loop would spin sending PINGs into the
    // void and never reconnect — leaving the settlement-critical Chainlink price
    // frozen. If no frame arrives for 30s, return an error so the caller reconnects.
    let mut last_data = std::time::Instant::now();
    loop {
        tokio::select! {
            _ = ping.tick() => {
                if last_data.elapsed() > std::time::Duration::from_secs(30) {
                    return Err("rtds inactivity timeout (30s no data)".into());
                }
                write.send(Message::Text("PING".to_string())).await?;
            }
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(txt))) => {
                        last_data = std::time::Instant::now();
                        for t in parse_frame(&txt) {
                            on_tick(t);
                        }
                    }
                    Some(Ok(_)) => {
                        last_data = std::time::Instant::now(); // any frame = alive
                    }
                    Some(Err(e)) => return Err(e.into()),
                    None => return Ok(()),
                }
            }
        }
    }
}

/// Reconnect forever with capped exponential backoff.
pub async fn run_reconnecting<F>(url: &str, on_tick: F) -> !
where
    F: FnMut(Tick) + Clone,
{
    let mut delay = Duration::from_millis(250);
    loop {
        let res = run(url, on_tick.clone()).await;
        tracing::warn!(?res, "rtds disconnected");
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(10));
    }
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
