//! Fault injection for the order-book stream: a local scripted server feeds
//! the real `stream_book_ws_with` loop crossed books, malformed levels, bad
//! frames, drops and silence. Every book handed to the caller must be
//! internally consistent, and every fault must end the session so the caller
//! refetches a snapshot (resync).

use feeds::conn::{supervise, ConnOpts};
use feeds::testkit::{serve, text, Step};
use market::clob_book::{stream_book_ws_with, StreamBook};
use poly_core::{Backoff, FeedMetrics};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn snap(ts: i64, bid: f64, ask: f64) -> Step {
    text(format!(
        r#"{{"event_type":"book","asset_id":"tok","timestamp":"{ts}","bids":[{{"price":"{bid}","size":"10"}}],"asks":[{{"price":"{ask}","size":"10"}}]}}"#
    ))
}

fn change(ts: i64, side: &str, price: &str, size: &str) -> Step {
    text(format!(
        r#"{{"event_type":"price_change","timestamp":"{ts}","price_changes":[{{"asset_id":"tok","price":"{price}","size":"{size}","side":"{side}"}}]}}"#
    ))
}

fn opts(m: &Arc<FeedMetrics>) -> ConnOpts {
    ConnOpts {
        idle_timeout: Duration::from_millis(400),
        metrics: Some(m.clone()),
    }
}

fn assert_consistent(books: &[StreamBook]) {
    for b in books {
        if let (Some(bid), Some(ask)) = (b.book.best_bid(), b.book.best_ask()) {
            assert!(bid < ask, "published a crossed book: {:?}", b.book);
        }
        for w in b.book.bids.windows(2) {
            assert!(w[0].price > w[1].price);
        }
        for w in b.book.asks.windows(2) {
            assert!(w[0].price < w[1].price);
        }
    }
}

async fn run_one(script: Vec<Step>) -> (Result<(), String>, Vec<StreamBook>, Arc<FeedMetrics>) {
    let url = serve(vec![script]).await;
    let m = Arc::new(FeedMetrics::new("clob_book", 3_000));
    let mut books = Vec::new();
    let res = stream_book_ws_with(&url, "tok", &opts(&m), |b| books.push(b))
        .await
        .map_err(|e| e.to_string());
    (res, books, m)
}

#[tokio::test]
async fn crossing_delta_mid_stream_stops_publishing_and_requests_resync() {
    let (res, books, m) = run_one(vec![
        snap(100, 0.49, 0.52),
        change(110, "BUY", "0.50", "5"),
        change(120, "BUY", "0.60", "1"),  // crosses the 0.52 ask
        change(130, "SELL", "0.55", "1"), // must never be applied
        Step::Silent(Duration::from_secs(5)),
    ])
    .await;
    let err = res.unwrap_err();
    assert!(err.contains("Crossed"), "{err}");
    assert_eq!(books.len(), 2, "snapshot and the good delta only");
    assert_eq!(books[1].book.best_bid(), Some(0.50));
    assert_consistent(&books);
    let s = m.snapshot();
    assert_eq!(
        (s.resyncs, s.malformed),
        (1, 0),
        "crossed is a resync, not malformed"
    );
}

#[tokio::test]
async fn malformed_level_requests_resync() {
    let (res, books, m) = run_one(vec![
        snap(100, 0.49, 0.52),
        change(110, "SELL", "NaN", "5"),
        change(120, "BUY", "0.50", "5"),
    ])
    .await;
    assert!(res.unwrap_err().contains("Malformed"));
    assert_eq!(books.len(), 1);
    assert_consistent(&books);
    let s = m.snapshot();
    assert_eq!((s.resyncs, s.malformed), (1, 1));
}

#[tokio::test]
async fn bad_json_frame_requests_resync() {
    let (res, books, _m) = run_one(vec![snap(100, 0.49, 0.52), text("{\"event_type\":")]).await;
    assert!(res.unwrap_err().contains("not valid JSON"));
    assert_eq!(books.len(), 1);
}

#[tokio::test]
async fn fills_reported_only_as_trades_are_reconciled() {
    let (res, books, _m) = run_one(vec![
        snap(100, 0.49, 0.52),
        text(r#"{"event_type":"last_trade_price","asset_id":"tok","price":"0.52","size":"10","side":"BUY","timestamp":"110"}"#),
        text(r#"{"event_type":"price_change","timestamp":"110","price_changes":[{"asset_id":"tok","price":"0.52","size":"4","side":"BUY","best_bid":"0.52","best_ask":"0.6"}]}"#),
        Step::Close,
    ])
    .await;
    assert!(res.is_err());
    assert_eq!(books.len(), 2);
    assert_consistent(&books);
    assert_eq!(books[1].book.best_bid(), Some(0.52));
    assert_eq!(books[1].book.best_ask(), None, "the only ask was consumed");
}

#[tokio::test]
async fn silent_book_feed_times_out_and_goes_stale() {
    let url = serve(vec![vec![
        snap(100, 0.49, 0.52),
        Step::Silent(Duration::from_secs(30)),
    ]])
    .await;
    let m = Arc::new(FeedMetrics::new("clob_book", 200));
    let res = stream_book_ws_with(&url, "tok", &opts(&m), |_| {}).await;
    assert!(res.unwrap_err().to_string().contains("timeout"));
    assert!(m.snapshot().stale);
}

#[tokio::test]
async fn supervisor_resyncs_from_a_fresh_snapshot_after_desync_and_drop() {
    // Session 1 desyncs on a crossed book, session 2 drops mid-stream, session 3
    // starts from a fresh snapshot. Every published book stays consistent and
    // the final book is exactly the last snapshot plus its delta.
    let url = serve(vec![
        vec![snap(100, 0.49, 0.52), change(110, "BUY", "0.70", "1")],
        vec![
            snap(200, 0.45, 0.55),
            change(210, "SELL", "0.54", "3"),
            Step::Drop,
        ],
        vec![
            snap(300, 0.40, 0.60),
            change(310, "BUY", "0.41", "2"),
            Step::Close,
        ],
    ])
    .await;
    let m = Arc::new(FeedMetrics::new("clob_book", 3_000));
    let books = Arc::new(Mutex::new(Vec::new()));
    let o = opts(&m);
    let backoff = Backoff::new(
        Duration::from_millis(10),
        Duration::from_millis(40),
        Duration::from_secs(30),
    );
    supervise("clob_book", Some(m.clone()), backoff, Some(3), || {
        let books = books.clone();
        let o = o.clone();
        let url = url.clone();
        async move {
            stream_book_ws_with(&url, "tok", &o, move |b| books.lock().unwrap().push(b)).await
        }
    })
    .await;
    let books = books.lock().unwrap();
    assert_consistent(&books);
    assert_eq!(books.len(), 5, "1 + 2 + 2 consistent books");
    let last = &books.last().unwrap().book;
    assert_eq!(last.best_bid(), Some(0.41));
    assert_eq!(last.best_ask(), Some(0.60));
    let s = m.snapshot();
    assert_eq!((s.resyncs, s.reconnects), (1, 2));
}
