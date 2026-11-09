//! Record raw frames from the public feeds into an append-only recording.
//!
//! cargo run --release -p app --bin record -- <out.prec> [--secs N]
//!     [--rtds-url URL] [--binance-url URL] [--clob-url URL] [--token ID]
//!
//! Without `--token` the current BTC five-minute market is discovered and the
//! order-book subscription follows each new window. Stops after `--secs`, or
//! on SIGINT / SIGTERM: feed tasks are cancelled, the queue is drained, the
//! file is flushed and fsynced, and a summary is printed.

use feeds::conn::{supervise, ConnOpts};
use feeds::recorder::{start_file, RecorderHandle};
use feeds::recording::Source;
use market::clob_book;
use poly_core::Backoff;
use std::time::Duration;
use tracing::{info, warn};

struct Args {
    out: String,
    secs: Option<u64>,
    rtds_url: String,
    binance_url: String,
    clob_url: String,
    token: Option<String>,
}

fn parse_args() -> Result<Args, String> {
    let mut it = std::env::args().skip(1);
    let mut a = Args {
        out: String::new(),
        secs: None,
        rtds_url: feeds::rtds::RTDS_URL.to_string(),
        binance_url: feeds::binance::default_url().to_string(),
        clob_url: clob_book::CLOB_MARKET_WS.to_string(),
        token: None,
    };
    while let Some(arg) = it.next() {
        let mut val = || it.next().ok_or(format!("{arg} needs a value"));
        match arg.as_str() {
            "--secs" => a.secs = Some(val()?.parse().map_err(|_| "bad --secs")?),
            "--rtds-url" => a.rtds_url = val()?,
            "--binance-url" => a.binance_url = val()?,
            "--clob-url" => a.clob_url = val()?,
            "--token" => a.token = Some(val()?),
            s if !s.starts_with("--") && a.out.is_empty() => a.out = s.to_string(),
            s => return Err(format!("unknown argument {s}")),
        }
    }
    if a.out.is_empty() {
        return Err("usage: record <out.prec> [--secs N] [--token ID] [--*-url URL]".into());
    }
    Ok(a)
}

async fn current_token() -> Option<String> {
    let now = poly_core::metrics::wall_millis();
    match market::gamma::fetch_current_btc_updown(now, 300).await {
        Ok(Some(m)) => Some(m.yes_token_id),
        Ok(None) => None,
        Err(e) => {
            warn!(error = %e, "market discovery failed");
            None
        }
    }
}

/// Order-book feed: subscribe to the current token, follow window changes,
/// and write a control record at every (re)subscription.
async fn book_loop(rec: RecorderHandle, url: String, fixed: Option<String>) {
    let mut backoff = Backoff::default();
    loop {
        let token = match &fixed {
            Some(t) => Some(t.clone()),
            None => current_token().await,
        };
        let Some(token) = token else {
            tokio::time::sleep(Duration::from_secs(2)).await;
            continue;
        };
        rec.record(
            Source::Control,
            &serde_json::json!({ "subscribe": token }).to_string(),
        );
        let opts = ConnOpts::default().recording(rec.clone(), Source::ClobBook);
        let started = std::time::Instant::now();
        let stream = clob_book::stream_book_ws_with(&url, &token, &opts, |_| {});
        tokio::pin!(stream);
        loop {
            tokio::select! {
                res = &mut stream => {
                    let delay = backoff.on_session_end(started.elapsed());
                    warn!(feed = "clob_book", error = ?res.err(), retry_in_ms = delay.as_millis() as u64, "reconnecting");
                    tokio::time::sleep(delay).await;
                    break;
                }
                _ = tokio::time::sleep(Duration::from_secs(5)), if fixed.is_none() => {
                    if current_token().await.is_some_and(|t| t != token) {
                        info!("market window rolled; resubscribing");
                        break;
                    }
                }
            }
        }
    }
}

async fn shutdown_signal() -> &'static str {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => "SIGINT",
            _ = term.recv() => "SIGTERM",
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        "SIGINT"
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,hyper=warn,reqwest=warn".into()),
        )
        .init();
    let args = parse_args()?;
    let (rec, task) = start_file(&args.out, 65_536)?;
    let stats = rec.stats.clone();
    info!(path = %args.out, "recording");

    let mut tasks = Vec::new();
    {
        let opts = ConnOpts::default().recording(rec.clone(), Source::Rtds);
        let url = args.rtds_url.clone();
        tasks.push(tokio::spawn(async move {
            supervise("rtds", None, Backoff::default(), None, || {
                feeds::rtds::run_with(&url, &opts, |_| {})
            })
            .await
        }));
    }
    {
        let opts = ConnOpts::default().recording(rec.clone(), Source::Binance);
        let url = args.binance_url.clone();
        tasks.push(tokio::spawn(async move {
            supervise("binance", None, Backoff::default(), None, || {
                feeds::binance::run_with(&url, &opts, |_| {})
            })
            .await
        }));
    }
    tasks.push(tokio::spawn(book_loop(
        rec.clone(),
        args.clob_url.clone(),
        args.token.clone(),
    )));

    let reason = match args.secs {
        Some(s) => tokio::select! {
            r = shutdown_signal() => r,
            _ = tokio::time::sleep(Duration::from_secs(s)) => "duration reached",
        },
        None => shutdown_signal().await,
    };
    info!(reason, "stopping: cancelling feeds, draining recorder");
    for t in &tasks {
        t.abort();
    }
    for t in tasks {
        let _ = t.await; // wait so every handle clone inside is dropped
    }
    drop(rec);
    let (records, bytes) = tokio::task::spawn_blocking(move || task.finish()).await??;
    println!(
        "recorded {records} frames, {bytes} bytes, dropped {} ({reason})",
        stats.dropped.load(std::sync::atomic::Ordering::Relaxed)
    );
    Ok(())
}
