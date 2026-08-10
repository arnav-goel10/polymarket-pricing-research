use poly_core::PricingContext;
use statrs::distribution::{ContinuousCDF, Normal};
use std::sync::OnceLock;

static STANDARD_NORMAL: OnceLock<Normal> = OnceLock::new();

/// Standard normal CDF.
#[inline]
pub fn normal_cdf(x: f64) -> f64 {
    STANDARD_NORMAL
        .get_or_init(|| Normal::new(0.0, 1.0).expect("standard normal"))
        .cdf(x)
}

/// A model that prices the YES (Up) digital: P(S_T > S0) in [0, 1].
pub trait DigitalModel: Send + Sync {
    fn price(&self, ctx: &PricingContext) -> f64;
    fn name(&self) -> &str;
}
