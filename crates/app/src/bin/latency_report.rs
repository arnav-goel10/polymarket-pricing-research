//! Per-event latency and end-to-end throughput of the replay pipeline.
//!
//! cargo run --release -p app --bin latency_report -- [events]
//!
//! Generates a deterministic synthetic session, merges the three feeds into
//! one ordered stream, then times every event through the pipeline (raw JSON
//! book frame parse + apply, volatility update, four-model re-price). Each
//! sample includes one `Instant::now()` pair, so sub-100 ns figures carry
//! timer overhead.

use app::replay_stream::{generate_session, merge_streams, EventKind, Pipeline};
use std::time::Instant;

fn pct(sorted: &[u64], p: f64) -> u64 {
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

fn report(name: &str, mut v: Vec<u64>) {
    if v.is_empty() {
        return;
    }
    v.sort_unstable();
    println!(
        "{name:<12} n={:<8} p50={:>6} ns  p99={:>6} ns  p99.9={:>6} ns  max={:>7} ns",
        v.len(),
        pct(&v, 0.50),
        pct(&v, 0.99),
        pct(&v, 0.999),
        v[v.len() - 1]
    );
}

fn main() {
    let n: usize = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(1_000_000);
    let streams = generate_session(42, n);
    let total: usize = streams.iter().map(Vec::len).sum();

    let t = Instant::now();
    let merged = merge_streams(streams);
    let merge_s = t.elapsed().as_secs_f64();

    // Warm-up pass, then the measured pass.
    let mut warm = Pipeline::new();
    for ev in merged.iter().take(50_000) {
        warm.on_event(ev);
    }

    let mut p = Pipeline::new();
    let (mut all, mut book, mut tick) = (
        Vec::with_capacity(total),
        Vec::with_capacity(total),
        Vec::with_capacity(total),
    );
    let wall = Instant::now();
    for ev in &merged {
        let s = Instant::now();
        std::hint::black_box(p.on_event(ev));
        let ns = s.elapsed().as_nanos() as u64;
        all.push(ns);
        match ev.kind {
            EventKind::BookFrame(_) => book.push(ns),
            _ => tick.push(ns),
        }
    }
    let wall_s = wall.elapsed().as_secs_f64();

    println!(
        "events={total} repriced={} resyncs={}",
        p.repriced, p.resyncs
    );
    println!(
        "merge_3_feeds: {:.1} ms ({:.1} M events/s)",
        merge_s * 1e3,
        total as f64 / merge_s / 1e6
    );
    println!(
        "pipeline: {:.3} s wall incl. timing overhead, {:.0} events/s",
        wall_s,
        total as f64 / wall_s
    );
    report("all_events", all);
    report("book_frames", book);
    report("price_ticks", tick);
}
