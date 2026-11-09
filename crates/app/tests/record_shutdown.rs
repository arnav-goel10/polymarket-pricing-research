//! Graceful shutdown: the `record` binary, streaming from scripted local
//! feeds, receives SIGTERM, drains its queue, fsyncs and exits 0 with a
//! recording that reads back cleanly (no truncated tail).

#![cfg(unix)]

use feeds::recording::{read_all, Source};
use feeds::testkit::{serve, text, Step};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn burst(n: usize, f: impl Fn(usize) -> String) -> Vec<Step> {
    let mut v: Vec<Step> = (0..n).map(|i| text(f(i))).collect();
    v.push(Step::Silent(Duration::from_secs(120)));
    v
}

#[tokio::test(flavor = "multi_thread")]
async fn sigterm_drains_flushes_and_exits_cleanly() {
    let rtds = serve(vec![burst(200, |i| {
        format!(r#"{{"topic":"crypto_prices_chainlink","type":"update","payload":{{"value":{}.0,"timestamp":{i}}}}}"#, 100_000 + i)
    })])
    .await;
    let binance = serve(vec![burst(300, |i| {
        format!(
            r#"{{"stream":"btcusdt@aggTrade","data":{{"p":"{}","q":"1","m":false,"T":{i}}}}}"#,
            100_000 + i
        )
    })])
    .await;
    let clob = serve(vec![burst(100, |i| {
        if i == 0 {
            r#"{"event_type":"book","asset_id":"tok","timestamp":"1","bids":[{"price":"0.4","size":"1"}],"asks":[{"price":"0.6","size":"1"}]}"#.to_string()
        } else {
            format!(r#"{{"event_type":"price_change","timestamp":"{i}","price_changes":[{{"asset_id":"tok","price":"0.41","size":"{i}","side":"BUY","best_bid":"0.41","best_ask":"0.6"}}]}}"#)
        }
    })])
    .await;

    let dir = std::env::temp_dir().join(format!("rec-shutdown-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let out = dir.join("t.prec");
    let mut child = Command::new(env!("CARGO_BIN_EXE_record"))
        .arg(&out)
        .args(["--rtds-url", &rtds, "--binance-url", &binance])
        .args(["--clob-url", &clob, "--token", "tok"])
        .env("RUST_LOG", "warn")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    // All 601 scripted frames are sent within milliseconds; give the
    // recorder time to receive them, then ask it to stop.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let killed = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(killed.success());

    let started = Instant::now();
    let status = loop {
        if let Some(s) = child.try_wait().unwrap() {
            break s;
        }
        assert!(started.elapsed() < Duration::from_secs(10), "did not exit");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let mut stdout = String::new();
    std::io::Read::read_to_string(child.stdout.as_mut().unwrap(), &mut stdout).unwrap();
    assert!(status.success(), "exit status {status:?}");
    assert!(stdout.contains("(SIGTERM)"), "{stdout}");
    assert!(stdout.contains("dropped 0"), "{stdout}");

    let (records, tail) = read_all(&out).unwrap();
    assert!(tail.is_none(), "truncated tail after graceful shutdown");
    let count = |s| records.iter().filter(|r| r.source == s).count();
    assert_eq!(count(Source::Rtds), 200);
    assert_eq!(count(Source::Binance), 300);
    assert_eq!(count(Source::ClobBook), 100);
    assert_eq!(count(Source::Control), 1);
    assert!(stdout.contains(&format!("recorded {} frames", records.len())));
    std::fs::remove_dir_all(dir).unwrap();
}
