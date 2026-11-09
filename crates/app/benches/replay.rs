//! Replay throughput: merging feeds into one ordered stream, and the full
//! per-event pipeline (book reconstruction from raw JSON, volatility update,
//! four-model re-price on every event) over a 100k-event synthetic session.

use app::replay_stream::{generate_session, merge_streams, Pipeline};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use std::hint::black_box;

const EVENTS: usize = 100_000;

fn bench(c: &mut Criterion) {
    let streams = generate_session(42, EVENTS);
    let n: usize = streams.iter().map(Vec::len).sum();
    let merged = merge_streams(streams.clone());

    let mut g = c.benchmark_group("replay");
    g.throughput(Throughput::Elements(n as u64));
    g.sample_size(20);
    g.bench_function("merge_3_feeds", |b| {
        b.iter_batched(
            || streams.clone(),
            |s| black_box(merge_streams(s)),
            BatchSize::LargeInput,
        )
    });
    g.bench_function("pipeline_full_reprice", |b| {
        b.iter(|| {
            let mut p = Pipeline::new();
            for ev in &merged {
                black_box(p.on_event(ev));
            }
            black_box(p.repriced)
        })
    });
    g.finish();

    // Real recorded session, if present (POLY_RECORDING overrides the path).
    let path = std::env::var("POLY_RECORDING").unwrap_or_else(|_| {
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/live-sample.prec"
        )
        .into()
    });
    if let Ok((records, _)) = feeds::recording::read_all(&path) {
        let bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        let mut g = c.benchmark_group("recording");
        g.sample_size(10);
        g.throughput(Throughput::Elements(records.len() as u64));
        g.bench_function("pipeline_real_frames", |b| {
            b.iter(|| {
                let mut p = Pipeline::new();
                for r in &records {
                    black_box(p.on_record(r));
                }
                black_box(p.repriced)
            })
        });
        g.throughput(Throughput::Bytes(bytes));
        g.bench_function("read_and_verify", |b| {
            b.iter(|| black_box(feeds::recording::read_all(&path).unwrap().0.len()))
        });
        g.finish();
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);
