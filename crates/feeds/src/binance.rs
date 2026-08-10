use crate::tick::{BookUpdate, Trade};
use serde_json::Value;
use std::time::Duration;

#[derive(Debug, Clone, Copy)]
pub enum BinanceEvent {
    Trade(Trade),
    Book(BookUpdate),
}

fn f(v: &Value, k: &str) -> Option<f64> {
    v.get(k)?.as_str()?.parse::<f64>().ok()
}

/// Parse a Binance combined-stream message into a normalized event.
pub fn parse_message(raw: &str) -> Option<BinanceEvent> {
    let root: Value = serde_json::from_str(raw).ok()?;
    let stream = root.get("stream")?.as_str()?;
    let data = root.get("data")?;
    if stream.ends_with("@aggTrade") {
        Some(BinanceEvent::Trade(Trade {
            price: f(data, "p")?,
            qty: f(data, "q")?,
            buyer_is_maker: data.get("m")?.as_bool()?,
            ts_millis: data.get("T")?.as_i64().unwrap_or(0),
        }))
    } else if stream.ends_with("@bookTicker") {
        Some(BinanceEvent::Book(BookUpdate {
            bid: f(data, "b")?,
            bid_qty: f(data, "B")?,
            ask: f(data, "a")?,
            ask_qty: f(data, "A")?,
            ts_millis: data.get("T").and_then(|x| x.as_i64()).unwrap_or(0),
        }))
    } else {
        None
    }
}

/// Real-time connection shell. Streams Binance perp events to a callback until error.
///
/// Exits on any of: read error, stream closed, or 30s of silence (silent TCP
/// freeze). The caller is responsible for reconnecting.
pub async fn run<F>(
    url: &str,
    mut on_event: F,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    F: FnMut(BinanceEvent),
{
    use futures_util::{SinkExt, StreamExt};
    use tokio::time::{timeout, Duration};
    use tokio_tungstenite::tungstenite::Message;

    let (ws, _) = tokio_tungstenite::connect_async(url).await?;
    let (mut writer, mut reader) = ws.split();

    loop {
        match timeout(Duration::from_secs(30), reader.next()).await {
            Err(_) => return Err("binance read timeout (30s of silence)".into()),
            Ok(None) => return Err("binance stream closed".into()),
            Ok(Some(Err(e))) => return Err(e.into()),
            Ok(Some(Ok(msg))) => match msg {
                Message::Text(txt) => {
                    if let Some(ev) = parse_message(&txt) {
                        on_event(ev);
                    }
                }
                Message::Ping(data) => {
                    writer.send(Message::Pong(data)).await?;
                }
                Message::Close(_) => return Err("binance stream closed by server".into()),
                _ => {}
            },
        }
    }
}

pub async fn run_reconnecting<F>(url: &str, on_event: F) -> !
where
    F: FnMut(BinanceEvent) + Clone,
{
    let mut delay = Duration::from_millis(250);
    loop {
        let res = run(url, on_event.clone()).await;
        tracing::warn!(?res, "binance disconnected");
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(10));
    }
}

/// Default combined-stream URL for BTCUSDT perp aggTrade + bookTicker.
pub fn default_url() -> &'static str {
    "wss://fstream.binance.com/stream?streams=btcusdt@aggTrade/btcusdt@bookTicker"
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_abs_diff_eq;

    #[test]
    fn parses_agg_trade() {
        let msg = r#"{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","p":"60050.10","q":"0.5","m":false,"T":1700000000000}}"#;
        match parse_message(msg).unwrap() {
            BinanceEvent::Trade(t) => {
                assert_abs_diff_eq!(t.price, 60050.10, epsilon = 1e-9);
                assert_abs_diff_eq!(t.qty, 0.5, epsilon = 1e-9);
                assert!(!t.buyer_is_maker);
            }
            _ => panic!("expected trade"),
        }
    }

    #[test]
    fn parses_book_ticker() {
        let msg = r#"{"stream":"btcusdt@bookTicker","data":{"b":"60049.0","B":"1.2","a":"60051.0","A":"0.8"}}"#;
        match parse_message(msg).unwrap() {
            BinanceEvent::Book(b) => {
                assert_abs_diff_eq!(b.bid, 60049.0, epsilon = 1e-9);
                assert_abs_diff_eq!(b.ask, 60051.0, epsilon = 1e-9);
            }
            _ => panic!("expected book"),
        }
    }

    #[test]
    fn ignores_unknown() {
        assert!(parse_message(r#"{"stream":"x","data":{}}"#).is_none());
    }
}
