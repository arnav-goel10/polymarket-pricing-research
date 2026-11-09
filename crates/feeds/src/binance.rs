use crate::conn::{supervise, BoxError, ConnOpts};
use crate::tick::{BookUpdate, Trade};
use poly_core::Backoff;
use serde_json::Value;

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
/// freeze). Use [`run_reconnecting`] or [`crate::conn::supervise`] to reconnect.
pub async fn run<F>(url: &str, on_event: F) -> Result<(), BoxError>
where
    F: FnMut(BinanceEvent),
{
    run_with(url, &ConnOpts::default(), on_event).await
}

/// [`run`] with an explicit idle timeout and optional health counters.
pub async fn run_with<F>(url: &str, opts: &ConnOpts, mut on_event: F) -> Result<(), BoxError>
where
    F: FnMut(BinanceEvent),
{
    use futures_util::{SinkExt, StreamExt};
    use tokio::time::timeout;
    use tokio_tungstenite::tungstenite::Message;

    let (ws, _) = tokio_tungstenite::connect_async(url).await?;
    opts.connected(true);
    let (mut writer, mut reader) = ws.split();

    loop {
        match timeout(opts.idle_timeout, reader.next()).await {
            Err(_) => return Err("binance read timeout (silence)".into()),
            Ok(None) => return Err("binance stream closed".into()),
            Ok(Some(Err(e))) => return Err(e.into()),
            Ok(Some(Ok(msg))) => match msg {
                Message::Text(txt) => {
                    opts.tap_frame(&txt);
                    match parse_message(&txt) {
                        Some(ev) => {
                            let ts = match ev {
                                BinanceEvent::Trade(t) => t.ts_millis,
                                BinanceEvent::Book(b) => b.ts_millis,
                            };
                            opts.message(ts);
                            on_event(ev);
                        }
                        None => opts.malformed(),
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

/// Reconnect forever with capped exponential backoff.
pub async fn run_reconnecting<F>(url: &str, opts: ConnOpts, on_event: F)
where
    F: FnMut(BinanceEvent) + Clone,
{
    let metrics = opts.metrics.clone();
    supervise("binance", metrics, Backoff::default(), None, || {
        run_with(url, &opts, on_event.clone())
    })
    .await
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
