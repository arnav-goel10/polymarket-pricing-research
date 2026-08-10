use crate::model::DigitalModel;
use poly_core::PricingContext;
use statrs::distribution::{ContinuousCDF, StudentsT};
use std::sync::OnceLock;

static STUDENT_T_NU4: OnceLock<StudentsT> = OnceLock::new();

/// Tier 2a: Student-t terminal return with `nu` degrees of freedom,
/// scaled to UNIT variance so `sigma` keeps its meaning.
pub struct StudentTDigital {
    pub nu: f64,
}

impl DigitalModel for StudentTDigital {
    fn price(&self, ctx: &PricingContext) -> f64 {
        if ctx.tau <= 0.0 {
            return if ctx.s_t > ctx.s0 { 1.0 } else { 0.0 };
        }
        let denom = ctx.sigma * ctx.tau.sqrt();
        let z = (ctx.k() - ctx.mu * ctx.tau) / denom;
        if !z.is_finite() {
            return if ctx.s_t > ctx.s0 { 1.0 } else { 0.0 };
        }
        let nu = self.nu.max(2.0001);
        let scale = (nu / (nu - 2.0)).sqrt();
        if (nu - 4.0).abs() < 1e-12 {
            return 1.0
                - STUDENT_T_NU4
                    .get_or_init(|| StudentsT::new(0.0, 1.0, 4.0).expect("student-t nu=4"))
                    .cdf(z * scale);
        }
        let t = StudentsT::new(0.0, 1.0, nu).unwrap();
        1.0 - t.cdf(z * scale)
    }
    fn name(&self) -> &str {
        "student_t"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::black_scholes::BlackScholesDigital;
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
            kurt: 6.0,
        }
    }

    #[test]
    fn at_the_money_is_one_half() {
        assert_abs_diff_eq!(
            StudentTDigital { nu: 4.0 }.price(&ctx(60_000.0, 60_000.0)),
            0.5,
            epsilon = 1e-9
        );
    }
    #[test]
    fn high_nu_approaches_gaussian() {
        let c = ctx(60_050.0, 60_000.0);
        assert_abs_diff_eq!(
            StudentTDigital { nu: 200.0 }.price(&c),
            BlackScholesDigital.price(&c),
            epsilon = 5e-3
        );
    }
    #[test]
    fn fat_tails_pull_toward_one_half_when_deep_in_the_money() {
        // Deep ITM (~3+ std), where the unit-variance t tail exceeds the Gaussian
        // tail: heavier tails assign more probability to crossing back, so the
        // in-the-money digital is LESS extreme (closer to 0.5) than Gaussian.
        let c = ctx(60_600.0, 60_000.0);
        assert!(StudentTDigital { nu: 3.0 }.price(&c) < BlackScholesDigital.price(&c));
    }
}
