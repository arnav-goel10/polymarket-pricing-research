use crate::model::{normal_cdf, DigitalModel};
use poly_core::PricingContext;

/// Tier 2b: Cornish-Fisher / Edgeworth correction to the Gaussian digital
/// using forecast skewness and excess kurtosis.
pub struct CornishFisherDigital;

impl DigitalModel for CornishFisherDigital {
    fn price(&self, ctx: &PricingContext) -> f64 {
        if ctx.tau <= 0.0 {
            return if ctx.s_t > ctx.s0 { 1.0 } else { 0.0 };
        }
        let denom = ctx.sigma * ctx.tau.sqrt();
        let z0 = (ctx.k() - ctx.mu * ctx.tau) / denom;
        if !z0.is_finite() {
            return if ctx.s_t > ctx.s0 { 1.0 } else { 0.0 };
        }
        let s = ctx.skew;
        let ex_k = ctx.kurt - 3.0;
        let w = z0 - (s / 6.0) * (z0 * z0 - 1.0) - (ex_k / 24.0) * (z0 * z0 * z0 - 3.0 * z0)
            + (s * s / 36.0) * (2.0 * z0 * z0 * z0 - 5.0 * z0);
        (1.0 - normal_cdf(w)).clamp(0.0, 1.0)
    }
    fn name(&self) -> &str {
        "cornish_fisher"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::black_scholes::BlackScholesDigital;
    use crate::model::DigitalModel;
    use approx::assert_abs_diff_eq;
    use poly_core::PricingContext;

    fn ctx(s_t: f64, s0: f64, skew: f64, kurt: f64) -> PricingContext {
        PricingContext {
            s_t,
            s0,
            tau: 5.0 / (365.0 * 24.0 * 60.0),
            mu: 0.0,
            sigma: 0.8,
            skew,
            kurt,
        }
    }

    #[test]
    fn zero_skew_zero_excess_kurt_matches_gaussian() {
        let c = ctx(60_050.0, 60_000.0, 0.0, 3.0);
        assert_abs_diff_eq!(
            CornishFisherDigital.price(&c),
            BlackScholesDigital.price(&c),
            epsilon = 1e-9
        );
    }
    #[test]
    fn negative_skew_raises_up_probability_near_the_money() {
        // With ~zero mean, negative return-skew (rare large crashes, frequent
        // small ups) shifts the MEDIAN return positive, so a near-ATM up-digital
        // is worth MORE than the symmetric case. This is a real, desirable
        // property for BTC's negatively-skewed 5-min returns.
        let skewed = CornishFisherDigital.price(&ctx(60_030.0, 60_000.0, -1.0, 6.0));
        let sym = CornishFisherDigital.price(&ctx(60_030.0, 60_000.0, 0.0, 3.0));
        assert!(skewed > sym);
    }
}
