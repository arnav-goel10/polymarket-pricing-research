use crate::model::{normal_cdf, DigitalModel};
use poly_core::PricingContext;

/// Tier 1 baseline: lognormal (Gaussian log-return) digital.
/// P(R_tau > k) = Phi((mu*tau - k) / (sigma*sqrt(tau))).
pub struct BlackScholesDigital;

impl DigitalModel for BlackScholesDigital {
    fn price(&self, ctx: &PricingContext) -> f64 {
        if ctx.tau <= 0.0 {
            return if ctx.s_t > ctx.s0 { 1.0 } else { 0.0 };
        }
        let denom = ctx.sigma * ctx.tau.sqrt();
        let drift_minus_barrier = ctx.mu * ctx.tau - ctx.k();
        if denom <= 0.0 {
            return if drift_minus_barrier > 0.0 { 1.0 } else { 0.0 };
        }
        normal_cdf(drift_minus_barrier / denom)
    }
    fn name(&self) -> &str {
        "black_scholes"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::DigitalModel;
    use approx::assert_abs_diff_eq;
    use poly_core::PricingContext;

    fn ctx(s_t: f64, s0: f64) -> PricingContext {
        PricingContext {
            s_t,
            s0,
            tau: 5.0 / (365.0 * 24.0 * 60.0),
            mu: 0.0,
            sigma: 0.8,
            skew: 0.0,
            kurt: 3.0,
        }
    }

    #[test]
    fn at_the_money_is_one_half() {
        assert_abs_diff_eq!(
            BlackScholesDigital.price(&ctx(60_000.0, 60_000.0)),
            0.5,
            epsilon = 1e-9
        );
    }
    #[test]
    fn far_above_reference_is_near_one() {
        assert!(BlackScholesDigital.price(&ctx(61_000.0, 60_000.0)) > 0.99);
    }
    #[test]
    fn far_below_reference_is_near_zero() {
        assert!(BlackScholesDigital.price(&ctx(59_000.0, 60_000.0)) < 0.01);
    }
    #[test]
    fn expired_above_resolves_one() {
        let mut c = ctx(60_010.0, 60_000.0);
        c.tau = 0.0;
        assert_eq!(BlackScholesDigital.price(&c), 1.0);
    }
}
