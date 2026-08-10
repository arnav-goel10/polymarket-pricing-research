use crate::model::{normal_cdf, DigitalModel};
use poly_core::PricingContext;

/// Sharpness/continuation digital — grounded in the empirical finding that the
/// 5-minute Polymarket crypto market is *calibrated but not sharp*: it hugs
/// fifty-fifty even when the near-term outcome is largely decided, so a rule
/// that simply backs the side that is already ahead predicts direction far more
/// decisively than the market quote.
///
/// This is the Gaussian digital with the standardized move scaled by a
/// `sharpness` factor (>1), i.e. `P = Phi(sharpness * (mu*tau - k)/(sigma*sqrt(tau)))`.
/// At sharpness 1 it is exactly the Black-Scholes digital; above 1 it is more
/// confident (sharper) than the (timid) market — the source of the edge. As
/// tau -> 0 it converges to `1{S_t > S0}` (back the leader), like the 81%-rule.
pub struct SharpDigital {
    pub sharpness: f64,
}

impl SharpDigital {
    pub fn new(sharpness: f64) -> Self {
        SharpDigital {
            sharpness: sharpness.max(0.0),
        }
    }
}

impl DigitalModel for SharpDigital {
    fn price(&self, ctx: &PricingContext) -> f64 {
        if ctx.tau <= 0.0 {
            return if ctx.s_t > ctx.s0 { 1.0 } else { 0.0 };
        }
        let denom = ctx.sigma * ctx.tau.sqrt();
        let z = (ctx.mu * ctx.tau - ctx.k()) / denom;
        if !z.is_finite() {
            return if ctx.s_t > ctx.s0 { 1.0 } else { 0.0 };
        }
        normal_cdf(self.sharpness * z)
    }
    fn name(&self) -> &str {
        "sharp"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::black_scholes::BlackScholesDigital;
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
            SharpDigital::new(2.5).price(&ctx(60_000.0, 60_000.0)),
            0.5,
            epsilon = 1e-9
        );
    }

    #[test]
    fn sharpness_one_equals_black_scholes() {
        let c = ctx(60_050.0, 60_000.0);
        assert_abs_diff_eq!(
            SharpDigital::new(1.0).price(&c),
            BlackScholesDigital.price(&c),
            epsilon = 1e-12
        );
    }

    #[test]
    fn sharper_is_more_decisive_when_ahead() {
        // When Up is ahead (spot > S0), a sharper model is MORE confident (further
        // from 0.5) than Black-Scholes — that's the under-reaction edge.
        let c = ctx(60_050.0, 60_000.0);
        let bs = BlackScholesDigital.price(&c);
        let sharp = SharpDigital::new(2.5).price(&c);
        assert!(
            sharp > bs,
            "sharp {sharp} should exceed BS {bs} when Up is ahead"
        );
    }

    #[test]
    fn converges_to_leader_at_expiry() {
        let mut c = ctx(60_010.0, 60_000.0);
        c.tau = 0.0;
        assert_eq!(SharpDigital::new(3.0).price(&c), 1.0); // Up ahead -> 1
        c.s_t = 59_990.0;
        assert_eq!(SharpDigital::new(3.0).price(&c), 0.0);
    }

    #[test]
    fn price_is_a_probability() {
        let m = SharpDigital::new(3.0);
        for s in [59_500.0, 60_000.0, 60_500.0] {
            let p = m.price(&ctx(s, 60_000.0));
            assert!((0.0..=1.0).contains(&p));
        }
    }
}
