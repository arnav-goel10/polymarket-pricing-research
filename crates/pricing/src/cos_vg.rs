use crate::charfn::VarianceGammaCF;
use crate::cos::CosDigital;
use crate::model::DigitalModel;
use poly_core::PricingContext;

/// Context-driven COS Variance-Gamma digital. Unlike a fixed-parameter
/// `CosDigital`, this moment-matches the VG to the current `PricingContext`
/// (sigma, skew, kurtosis) on every call, so it tracks the market like the
/// other tiers.
pub struct CosVgDigital {
    n_terms: usize,
}

impl CosVgDigital {
    pub fn new(n_terms: usize) -> Self {
        CosVgDigital { n_terms }
    }
}

impl DigitalModel for CosVgDigital {
    fn price(&self, ctx: &PricingContext) -> f64 {
        if ctx.tau <= 0.0 {
            return if ctx.s_t > ctx.s0 { 1.0 } else { 0.0 };
        }
        let m = ctx.mu * ctx.tau;
        let v = (ctx.sigma * ctx.sigma * ctx.tau).max(1e-18);
        let nu = ((ctx.kurt - 3.0) / 3.0).clamp(1e-6, 2.0);
        let theta = ctx.skew.clamp(-3.0, 3.0) * v.sqrt() / 3.0;
        let sigma_vg = (v - theta * theta * nu).max(1e-12).sqrt();
        let cf = VarianceGammaCF::new(m, sigma_vg, theta, nu);
        CosDigital::new(cf, self.n_terms).price(ctx)
    }
    fn name(&self) -> &str {
        "cos_vg"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::black_scholes::BlackScholesDigital;
    use crate::model::DigitalModel;
    use approx::assert_abs_diff_eq;
    use poly_core::PricingContext;

    fn ctx(s_t: f64, skew: f64, kurt: f64) -> PricingContext {
        PricingContext {
            s_t,
            s0: 60_000.0,
            tau: 5.0 / (365.0 * 24.0 * 60.0),
            mu: 0.0,
            sigma: 0.8,
            skew,
            kurt,
        }
    }

    #[test]
    fn gaussian_moments_match_black_scholes() {
        let m = CosVgDigital::new(256);
        let c = ctx(60_050.0, 0.0, 3.0);
        assert_abs_diff_eq!(m.price(&c), BlackScholesDigital.price(&c), epsilon = 5e-3);
    }

    #[test]
    fn responds_to_context_unlike_fixed_params() {
        let m = CosVgDigital::new(256);
        let below = m.price(&ctx(59_950.0, 0.0, 3.0));
        let above = m.price(&ctx(60_050.0, 0.0, 3.0));
        assert!(
            above > below + 0.2,
            "cos-vg must track spot: {below} -> {above}"
        );
    }

    #[test]
    fn fat_kurtosis_less_extreme_deep_itm() {
        let m = CosVgDigital::new(256);
        let normalish = m.price(&ctx(60_400.0, 0.0, 3.0));
        let fat = m.price(&ctx(60_400.0, 0.0, 9.0));
        assert!(
            fat < normalish,
            "fat tails pull deep-ITM digital toward 0.5"
        );
    }

    #[test]
    fn price_is_a_probability() {
        let m = CosVgDigital::new(128);
        for s in [59_500.0, 60_000.0, 60_500.0] {
            let p = m.price(&ctx(s, -0.5, 6.0));
            assert!((0.0..=1.0).contains(&p));
        }
    }
}
