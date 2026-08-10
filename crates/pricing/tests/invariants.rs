use poly_core::PricingContext;
use pricing::{BlackScholesDigital, CornishFisherDigital, DigitalModel, StudentTDigital};
use proptest::prelude::*;

fn models() -> Vec<Box<dyn DigitalModel>> {
    vec![
        Box::new(BlackScholesDigital),
        Box::new(StudentTDigital { nu: 4.0 }),
        Box::new(CornishFisherDigital),
    ]
}

proptest! {
    #[test]
    fn price_is_a_probability(
        s_t in 30_000.0f64..120_000.0,
        delta in -500.0f64..500.0,
        tau in 1e-6f64..0.001,
        sigma in 0.2f64..2.5,
    ) {
        let ctx = PricingContext { s_t, s0: s_t + delta, tau, mu: 0.0, sigma, skew: 0.0, kurt: 3.0 };
        for m in models() {
            let p = m.price(&ctx);
            prop_assert!((0.0..=1.0).contains(&p), "{} -> {p}", m.name());
        }
    }

    #[test]
    fn monotonic_in_current_price(
        s0 in 30_000.0f64..120_000.0,
        tau in 1e-6f64..0.001,
        sigma in 0.2f64..2.5,
    ) {
        let lo = PricingContext { s_t: s0 - 50.0, s0, tau, mu: 0.0, sigma, skew: 0.0, kurt: 3.0 };
        let hi = PricingContext { s_t: s0 + 50.0, s0, tau, mu: 0.0, sigma, skew: 0.0, kurt: 3.0 };
        for m in models() {
            prop_assert!(m.price(&hi) >= m.price(&lo), "{} not monotonic", m.name());
        }
    }
}
