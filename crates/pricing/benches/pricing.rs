//! Price one five-minute digital contract with each model and the ensemble.

use criterion::{criterion_group, criterion_main, Criterion};
use poly_core::PricingContext;
use pricing::{
    BlackScholesDigital, CornishFisherDigital, CosVgDigital, DigitalModel, Ensemble, SharpDigital,
    StudentTDigital,
};
use std::hint::black_box;

fn bench(c: &mut Criterion) {
    let ctx = PricingContext {
        s_t: 100_050.0,
        s0: 100_000.0,
        tau: 150.0 / 31_557_600.0,
        mu: 0.0,
        sigma: 0.6,
        skew: -0.1,
        kurt: 4.0,
    };
    let models: Vec<(&str, Box<dyn DigitalModel>)> = vec![
        ("black_scholes", Box::new(BlackScholesDigital)),
        ("student_t", Box::new(StudentTDigital { nu: 4.0 })),
        ("sharp", Box::new(SharpDigital::new(2.5))),
        ("cornish_fisher", Box::new(CornishFisherDigital)),
        ("cos_vg_64", Box::new(CosVgDigital::new(64))),
    ];
    let mut g = c.benchmark_group("price_contract");
    for (name, m) in &models {
        g.bench_function(*name, |b| b.iter(|| black_box(m.price(black_box(&ctx)))));
    }
    let ensemble = Ensemble::new(vec![
        Box::new(BlackScholesDigital),
        Box::new(StudentTDigital { nu: 4.0 }),
        Box::new(SharpDigital::new(2.5)),
        Box::new(CosVgDigital::new(64)),
    ]);
    g.bench_function("ensemble_4_models", |b| {
        b.iter(|| black_box(ensemble.price(black_box(&ctx))))
    });
    g.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
