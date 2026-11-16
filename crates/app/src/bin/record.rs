//! Record raw frames from the public feeds into an append-only recording.
//!
//! cargo run --release -p app --bin record -- <out.prec> [--secs N]
//!     [--rtds-url URL] [--binance-url URL] [--clob-url URL] [--token ID]
//!
//! Without `--token` the current BTC five-minute market is discovered and the
//! order-book subscription follows each new window. Stops after `--secs`, or
//! on SIGINT / SIGTERM: feed tasks are cancelled, the queue is drained, the
//! file is flushed and fsynced, and a summary is printed.

use app::ingest::{shutdown_signal, spawn_feeds, FeedUrls};
use feeds::recorder::start_file;
use std::time::Duration;
use tracing::info;

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
        rtds_url: FeedUrls::default().rtds,
        binance_url: FeedUrls::default().binance,
        clob_url: FeedUrls::default().clob,
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

    let urls = FeedUrls {
        rtds: args.rtds_url.clone(),
        binance: args.binance_url.clone(),
        clob: args.clob_url.clone(),
        token: args.token.clone(),
    };
    let tasks = spawn_feeds(&rec, &urls, None);

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
