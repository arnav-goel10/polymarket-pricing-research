//! Per-event latency and end-to-end throughput of the replay pipeline.
//!
//! cargo run --release -p app --bin latency_report -- [events]
//! cargo run --release -p app --bin latency_report -- --recording <file.prec>
//!
//! With `--recording`, every recorded raw frame goes through the real feed
//! parsers (RTDS, Binance, CLOB book) and the pipeline instead.
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

fn recording_mode(path: &str) {
    let t = Instant::now();
    let (records, tail) = feeds::recording::read_all(path).expect("read recording");
    let read_s = t.elapsed().as_secs_f64();
    let bytes = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    let span_s = match (records.first(), records.last()) {
        (Some(a), Some(b)) => (b.recv_micros - a.recv_micros) as f64 / 1e6,
        _ => 0.0,
    };
    println!(
        "recording={path} frames={} bytes={bytes} span={span_s:.1}s truncated_tail={tail:?}",
        records.len()
    );
    println!(
        "read+verify: {:.1} ms ({:.0} MB/s, {:.2} M frames/s)",
        read_s * 1e3,
        bytes as f64 / read_s / 1e6,
        records.len() as f64 / read_s / 1e6
    );
    let mut warm = Pipeline::new();
    for r in records.iter().take(50_000) {
        warm.on_record(r);
    }
    let mut p = Pipeline::new();
    let mut all = Vec::with_capacity(records.len());
    let mut by_source: [Vec<u64>; 3] = Default::default();
    let wall = Instant::now();
    for r in &records {
        let s = Instant::now();
        std::hint::black_box(p.on_record(r));
        let ns = s.elapsed().as_nanos() as u64;
        all.push(ns);
        match r.source {
            feeds::recording::Source::Rtds => by_source[0].push(ns),
            feeds::recording::Source::Binance => by_source[1].push(ns),
            feeds::recording::Source::ClobBook => by_source[2].push(ns),
            feeds::recording::Source::Control => {}
        }
    }
    let wall_s = wall.elapsed().as_secs_f64();
    println!(
        "repriced={} resyncs={} ignored={}",
        p.repriced, p.resyncs, p.ignored
    );
    println!(
        "pipeline: {:.3} s wall incl. timing overhead, {:.0} frames/s, {:.0}x real time",
        wall_s,
        records.len() as f64 / wall_s,
        span_s / wall_s
    );
    report("all_frames", all);
    report("rtds", std::mem::take(&mut by_source[0]));
    report("binance", std::mem::take(&mut by_source[1]));
    report("clob_book", std::mem::take(&mut by_source[2]));
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if let Some(i) = args.iter().position(|a| a == "--recording") {
        recording_mode(args.get(i + 1).expect("--recording needs a path"));
        return;
    }
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
