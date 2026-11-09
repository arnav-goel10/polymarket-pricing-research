// Observe the public Polymarket market channel for one token and report how
// the local book behaved: books published, levels pruned by venue-top
// reconciliation, and resyncs (each one reconnects with a fresh snapshot).
//
// cargo run --release -p market --example book_health -- <token_id> [seconds]
use feeds::conn::ConnOpts;
use market::clob_book::{stream_book_ws_with, CLOB_MARKET_WS};
use poly_core::FeedMetrics;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let token = args
        .next()
        .expect("usage: book_health <token_id> [seconds]");
    let secs: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(120);
    let m = Arc::new(FeedMetrics::new("clob_book", 3_000));
    let opts = ConnOpts::with_metrics(m.clone());
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut crossed_published = 0u64;
    let mut sessions = 0u64;
    while Instant::now() < deadline {
        sessions += 1;
        let left = deadline.saturating_duration_since(Instant::now());
        let run = stream_book_ws_with(CLOB_MARKET_WS, &token, &opts, |b| {
            if let (Some(bid), Some(ask)) = (b.book.best_bid(), b.book.best_ask()) {
                if bid >= ask {
                    crossed_published += 1;
                }
            }
        });
        match tokio::time::timeout(left, run).await {
            Err(_) => break,
            Ok(res) => eprintln!("session ended: {res:?}"),
        }
    }
    let s = m.snapshot();
    println!(
        "observed {secs}s: books_published={} resyncs={} malformed={} sessions={} crossed_books_published={}",
        s.messages, s.resyncs, s.malformed, sessions, crossed_published
    );
}
