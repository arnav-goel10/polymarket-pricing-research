//! Fault injection against a local scripted WebSocket server: connections
//! dropped mid-stream, malformed frames, stale (silent) feeds, and the
//! reconnect supervisor recovering across drops.

use feeds::conn::{supervise, ConnOpts};
use feeds::testkit::{serve, text, Step};
use feeds::{binance, rtds, FeedSource, Tick};
use poly_core::{Backoff, FeedMetrics};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn rtds_tick(price: f64, ts: i64) -> Step {
    text(format!(
        r#"{{"topic":"crypto_prices_chainlink","type":"update","payload":{{"symbol":"btc/usd","timestamp":{ts},"value":{price}}}}}"#
    ))
}

fn opts(metrics: &Arc<FeedMetrics>, idle_ms: u64) -> ConnOpts {
    ConnOpts {
        idle_timeout: Duration::from_millis(idle_ms),
        metrics: Some(metrics.clone()),
    }
}

#[tokio::test]
async fn rtds_connection_dropped_mid_stream_returns_error_after_delivering_ticks() {
    let url = serve(vec![vec![
        rtds_tick(60_000.0, 1),
        rtds_tick(60_001.0, 2),
        Step::Drop,
    ]])
    .await;
    let m = Arc::new(FeedMetrics::new("rtds", 5_000));
    let mut got = Vec::new();
    let res = rtds::run_with(&url, &opts(&m, 2_000), |t| got.push(t.price)).await;
    assert!(res.is_err(), "a dropped socket must surface as an error");
    assert_eq!(got, vec![60_000.0, 60_001.0]);
    assert_eq!(m.snapshot().messages, 2);
}

#[tokio::test]
async fn rtds_malformed_frames_are_counted_and_skipped() {
    let url = serve(vec![vec![
        text("{not json"),
        text(r#"{"topic":"crypto_prices","payload":{"value":"NaN-ish"}}"#),
        text(r#"{"topic":"comments","payload":{}}"#),
        rtds_tick(61_000.0, 5),
        Step::Close,
    ]])
    .await;
    let m = Arc::new(FeedMetrics::new("rtds", 5_000));
    let mut got: Vec<Tick> = Vec::new();
    let _ = rtds::run_with(&url, &opts(&m, 2_000), |t| got.push(t)).await;
    assert_eq!(got.len(), 1, "only the valid tick is delivered");
    assert_eq!(got[0].source, FeedSource::Chainlink);
    assert_eq!(got[0].price, 61_000.0);
    let s = m.snapshot();
    assert_eq!(
        s.malformed, 2,
        "bad JSON and a price frame without a numeric price are malformed; other topics are not"
    );
    assert_eq!(s.messages, 1);
}

#[tokio::test]
async fn rtds_silent_feed_times_out_and_is_flagged_stale() {
    let url = serve(vec![vec![
        rtds_tick(60_000.0, 1),
        Step::Silent(Duration::from_secs(30)),
    ]])
    .await;
    let m = Arc::new(FeedMetrics::new("rtds", 150));
    let started = Instant::now();
    let res = rtds::run_with(&url, &opts(&m, 300), |_| {}).await;
    let took = started.elapsed();
    let err = res.expect_err("silence must end the session").to_string();
    assert!(err.contains("inactivity"), "{err}");
    assert!(took < Duration::from_secs(2), "timed out in {took:?}");
    assert!(m.snapshot().stale, "no data for > threshold => stale");
}

#[tokio::test]
async fn binance_malformed_then_drop() {
    let url = serve(vec![vec![
        text("garbage"),
        text(r#"{"stream":"btcusdt@bookTicker","data":{"b":"60049.0","B":"1.2","a":"oops","A":"0.8"}}"#),
        text(r#"{"stream":"btcusdt@aggTrade","data":{"p":"60050.1","q":"0.5","m":false,"T":1700000000000}}"#),
        Step::Drop,
    ]])
    .await;
    let m = Arc::new(FeedMetrics::new("binance", 5_000));
    let mut n = 0;
    let res = binance::run_with(&url, &opts(&m, 2_000), |_| n += 1).await;
    assert!(res.is_err());
    assert_eq!(n, 1);
    let s = m.snapshot();
    assert_eq!((s.messages, s.malformed), (1, 2));
}

#[tokio::test]
async fn supervisor_reconnects_after_drop_and_after_silence() {
    // Connection 1 drops abruptly, connection 2 goes silent, connection 3 is
    // healthy. The supervisor must ride through both faults.
    let url = serve(vec![
        vec![rtds_tick(1.0, 1), Step::Drop],
        vec![rtds_tick(2.0, 2), Step::Silent(Duration::from_secs(30))],
        vec![rtds_tick(3.0, 3), Step::Close],
    ])
    .await;
    let m = Arc::new(FeedMetrics::new("rtds", 5_000));
    let got = Arc::new(Mutex::new(Vec::new()));
    let o = opts(&m, 250);
    let backoff = Backoff::new(
        Duration::from_millis(10),
        Duration::from_millis(40),
        Duration::from_secs(30),
    );
    let started = Instant::now();
    supervise("rtds", Some(m.clone()), backoff, Some(3), || {
        let got = got.clone();
        let o = o.clone();
        let url = url.clone();
        async move { rtds::run_with(&url, &o, move |t| got.lock().unwrap().push(t.price)).await }
    })
    .await;
    assert_eq!(*got.lock().unwrap(), vec![1.0, 2.0, 3.0]);
    let s = m.snapshot();
    assert_eq!(s.reconnects, 2);
    assert!(!s.connected);
    assert!(started.elapsed() < Duration::from_secs(3));
}

#[tokio::test]
async fn supervisor_survives_refused_connections() {
    // Nothing listens on this port: every attempt fails at connect time and
    // the supervisor keeps backing off instead of exiting or spinning.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    drop(listener);
    let m = Arc::new(FeedMetrics::new("rtds", 5_000));
    let backoff = Backoff::new(
        Duration::from_millis(5),
        Duration::from_millis(20),
        Duration::from_secs(30),
    );
    let o = opts(&m, 250);
    supervise("rtds", Some(m.clone()), backoff, Some(4), || {
        let o = o.clone();
        let url = url.clone();
        async move { rtds::run_with(&url, &o, |_| {}).await }
    })
    .await;
    assert_eq!(m.snapshot().reconnects, 3);
    assert_eq!(m.snapshot().messages, 0);
}
