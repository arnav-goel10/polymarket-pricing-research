//! Order-book hot path: one level update on a 20-levels-per-side book.
//!
//! `local_book_apply_delta` is the current path (binary-search upsert).
//! `legacy_sort_per_update` reproduces the previous path from this repo
//! (linear find, push, then re-sort both sides on every change) as a baseline.
//! `stream_price_change_frame` is the full per-message cost on the live path:
//! JSON parse, validation, apply, crossed check, and cloning the book for
//! publication.

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};

use market::clob_book::ClobBookStream;
use market::{BookSide, Level, LevelDelta, LocalBook, OrderBook, SeqPolicy};
use std::hint::black_box;

fn levels(lo: u32, hi: u32) -> Vec<Level> {
    (lo..=hi)
        .map(|t| Level {
            price: t as f64 / 100.0,
            size: 100.0 + t as f64,
        })
        .collect()
}

fn legacy_upsert(levels: &mut Vec<Level>, price: f64, size: f64) {
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

fn legacy_sort(book: &mut OrderBook) {
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

fn bench(c: &mut Criterion) {
    let bids = levels(30, 49);
    let asks = levels(51, 70);
    let mut g = c.benchmark_group("book_update");

    let mut lb = LocalBook::probability(SeqPolicy::Monotonic);
    lb.apply_snapshot(1, &bids, &asks);
    let mut i = 0u64;
    g.bench_function("local_book_apply_delta", |b| {
        b.iter(|| {
            i += 1;
            let tick = (30 + i % 20) as f64;
            let d = [LevelDelta {
                side: BookSide::Bid,
                price: tick / 100.0,
                size: (i % 7) as f64, // 0 removes, otherwise insert/resize
            }];
            black_box(lb.apply_delta(2, black_box(&d)))
        })
    });

    let mut legacy = OrderBook {
        bids: bids.iter().rev().copied().collect(),
        asks: asks.clone(),
    };
    let mut j = 0u64;
    g.bench_function("legacy_sort_per_update", |b| {
        b.iter(|| {
            j += 1;
            let tick = (30 + j % 20) as f64;
            legacy_upsert(&mut legacy.bids, black_box(tick / 100.0), (j % 7) as f64);
            legacy_sort(&mut legacy);
            black_box(legacy.bids.len())
        })
    });

    let snapshot = format!(
        r#"{{"event_type":"book","asset_id":"tok","timestamp":"1","bids":[{}],"asks":[{}]}}"#,
        bids.iter()
            .map(|l| format!(r#"{{"price":"{}","size":"{}"}}"#, l.price, l.size))
            .collect::<Vec<_>>()
            .join(","),
        asks.iter()
            .map(|l| format!(r#"{{"price":"{}","size":"{}"}}"#, l.price, l.size))
            .collect::<Vec<_>>()
            .join(",")
    );
    let frames: Vec<String> = (0..64)
        .map(|k| {
            format!(
                r#"{{"event_type":"price_change","timestamp":"{}","price_changes":[{{"asset_id":"tok","price":"0.{}","size":"{}","side":"BUY"}}]}}"#,
                10 + k,
                30 + k % 20,
                k % 7
            )
        })
        .collect();
    g.finish();

    // 64 frames per iteration; Criterion reports per-frame throughput.
    let mut g = c.benchmark_group("book_stream");
    g.throughput(criterion::Throughput::Elements(frames.len() as u64));
    g.bench_function("price_change_frames_x64", |b| {
        b.iter_batched_ref(
            || {
                let mut s = ClobBookStream::new("tok");
                s.on_text(&snapshot, |_| {}).unwrap();
                (s, 0usize)
            },
            |(s, k)| {
                for f in &frames {
                    *k += s
                        .on_text(black_box(f), |b| {
                            black_box(b);
                        })
                        .unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    g.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
