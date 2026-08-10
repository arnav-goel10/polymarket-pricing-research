use crate::book::{Level, OrderBook};
use serde_json::Value;
use std::sync::OnceLock;

const CLOB_BASE: &str = "https://clob.polymarket.com";
const CLOB_MARKET_WS: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

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

fn sort_book(book: &mut OrderBook) {
    book.bids.sort_by(|a, b| {
        b.price
            .partial_cmp(&a.price)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    book.asks.sort_by(|a, b| {
        a.price
            .partial_cmp(&b.price)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
}

fn parse_ts_millis(v: &Value) -> Option<i64> {
    v.get("timestamp").and_then(|x| {
        x.as_str()
            .and_then(|s| s.parse().ok())
            .or_else(|| x.as_i64())
    })
}

fn parse_book_value(v: &Value) -> Option<StreamBook> {
    if v.get("event_type")?.as_str()? != "book" {
        return None;
    }
    let bids = levels(v, "bids");
    let asks = levels(v, "asks");
    let mut book = OrderBook { bids, asks };
    sort_book(&mut book);
    Some(StreamBook {
        book,
        exchange_ts_millis: parse_ts_millis(v),
    })
}

fn upsert_level(levels: &mut Vec<Level>, price: f64, size: f64) {
    if !price.is_finite() || !size.is_finite() {
        return;
    }
    if size <= 0.0 {
        levels.retain(|l| (l.price - price).abs() > 1e-12);
        return;
    }
    if let Some(level) = levels.iter_mut().find(|l| (l.price - price).abs() <= 1e-12) {
        level.size = size;
    } else {
        levels.push(Level { price, size });
    }
}

fn apply_price_change(book: &mut OrderBook, change: &Value, token_id: &str) {
    if change.get("asset_id").and_then(|x| x.as_str()) != Some(token_id) {
        return;
    }
    let Some(price) = change
        .get("price")
        .and_then(|x| x.as_str())
        .and_then(|s| s.parse::<f64>().ok())
    else {
        return;
    };
    let Some(size) = change
        .get("size")
        .and_then(|x| x.as_str())
        .and_then(|s| s.parse::<f64>().ok())
    else {
        return;
    };
    match change.get("side").and_then(|x| x.as_str()) {
        Some("BUY") => upsert_level(&mut book.bids, price, size),
        Some("SELL") => upsert_level(&mut book.asks, price, size),
        _ => {}
    }
    sort_book(book);
}

fn apply_market_event(
    current: &mut Option<OrderBook>,
    v: &Value,
    token_id: &str,
) -> Option<StreamBook> {
    match v.get("event_type").and_then(|x| x.as_str()) {
        Some("book") if v.get("asset_id").and_then(|x| x.as_str()) == Some(token_id) => {
            let update = parse_book_value(v)?;
            *current = Some(update.book.clone());
            Some(update)
        }
        Some("price_change") => {
            let book = current.as_mut()?;
            if let Some(changes) = v.get("price_changes").and_then(|x| x.as_array()) {
                for change in changes {
                    apply_price_change(book, change, token_id);
                }
            }
            Some(StreamBook {
                book: book.clone(),
                exchange_ts_millis: parse_ts_millis(v),
            })
        }
        _ => None,
    }
}

pub async fn stream_book_ws<F>(
    token_id: &str,
    mut on_book: F,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    F: FnMut(StreamBook),
{
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let (ws, _) = tokio_tungstenite::connect_async(CLOB_MARKET_WS).await?;
    let (mut write, mut read) = ws.split();
    let subscribe = serde_json::json!({
        "assets_ids": [token_id],
        "type": "market",
        "custom_feature_enabled": true,
    });
    write.send(Message::Text(subscribe.to_string())).await?;

    let mut current: Option<OrderBook> = None;
    let mut last_data = std::time::Instant::now();
    loop {
        match tokio::time::timeout(std::time::Duration::from_secs(30), read.next()).await {
            Err(_) => return Err("clob market ws timeout (30s no data)".into()),
            Ok(None) => return Err("clob market ws closed".into()),
            Ok(Some(Err(e))) => return Err(e.into()),
            Ok(Some(Ok(Message::Text(txt)))) => {
                last_data = std::time::Instant::now();
                let root: Value = serde_json::from_str(&txt)?;
                if let Some(arr) = root.as_array() {
                    for item in arr {
                        if let Some(update) = apply_market_event(&mut current, item, token_id) {
                            on_book(update);
                        }
                    }
                } else if let Some(update) = apply_market_event(&mut current, &root, token_id) {
                    on_book(update);
                }
            }
            Ok(Some(Ok(Message::Ping(data)))) => {
                write.send(Message::Pong(data)).await?;
            }
            Ok(Some(Ok(Message::Close(_)))) => return Err("clob market ws closed by server".into()),
            Ok(Some(Ok(_))) => {
                if last_data.elapsed() > std::time::Duration::from_secs(30) {
                    return Err("clob market ws inactive".into());
                }
            }
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

    #[test]
    fn parses_ws_book_snapshot() {
        let v: Value = serde_json::from_str(
            r#"{"event_type":"book","asset_id":"tok","timestamp":"123",
                "bids":[{"price":"0.48","size":"10"},{"price":"0.49","size":"5"}],
                "asks":[{"price":"0.53","size":"10"},{"price":"0.52","size":"5"}]}"#,
        )
        .unwrap();
        let update = parse_book_value(&v).unwrap();
        assert_eq!(update.exchange_ts_millis, Some(123));
        assert_abs_diff_eq!(update.book.best_bid().unwrap(), 0.49, epsilon = 1e-12);
        assert_abs_diff_eq!(update.book.best_ask().unwrap(), 0.52, epsilon = 1e-12);
    }

    #[test]
    fn applies_ws_price_change_delta() {
        let mut current = Some(OrderBook {
            bids: vec![Level {
                price: 0.49,
                size: 5.0,
            }],
            asks: vec![Level {
                price: 0.52,
                size: 5.0,
            }],
        });
        let v: Value = serde_json::from_str(
            r#"{"event_type":"price_change","timestamp":"456","price_changes":[
                {"asset_id":"tok","price":"0.50","size":"8","side":"BUY"},
                {"asset_id":"tok","price":"0.52","size":"0","side":"SELL"},
                {"asset_id":"other","price":"0.99","size":"100","side":"BUY"}
            ]}"#,
        )
        .unwrap();
        let update = apply_market_event(&mut current, &v, "tok").unwrap();
        assert_eq!(update.exchange_ts_millis, Some(456));
        assert_abs_diff_eq!(update.book.best_bid().unwrap(), 0.50, epsilon = 1e-12);
        assert!(update.book.best_ask().is_none());
    }
}
