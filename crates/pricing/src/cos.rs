use crate::charfn::CharFn;
use crate::model::DigitalModel;
use poly_core::PricingContext;
use std::f64::consts::PI;

/// Tier 3 kernel: COS-method digital pricer parametrized by a characteristic
/// function of the terminal log-return.
pub struct CosDigital<F: CharFn> {
    cf: F,
    n_terms: usize,
}

impl<F: CharFn> CosDigital<F> {
    pub fn new(cf: F, n_terms: usize) -> Self {
        Self { cf, n_terms }
    }

    /// Truncation range [a, b] sized from cumulants (Fang-Oosterlee rule, L=10).
    fn range(&self) -> (f64, f64) {
        let cu = self.cf.cumulants();
        let l = 10.0;
        let width = l * (cu.c2.abs() + cu.c4.abs().sqrt()).sqrt();
        (cu.c1 - width, cu.c1 + width)
    }

    /// Cosine coefficient of the Heaviside payoff 1{x > kb} on [a,b].
    fn payoff_coeff(n: usize, a: f64, b: f64, kb: f64) -> f64 {
        let span = b - a;
        if n == 0 {
            return (2.0 / span) * (b - kb);
        }
        let w = n as f64 * PI / span;
        (2.0 / span) * ((w * (b - a)).sin() - (w * (kb - a)).sin()) / w
    }
}

impl<F: CharFn> DigitalModel for CosDigital<F> {
    fn price(&self, ctx: &PricingContext) -> f64 {
        if ctx.tau <= 0.0 {
            return if ctx.s_t > ctx.s0 { 1.0 } else { 0.0 };
        }
        let kb = ctx.k();
        let (a, b) = self.range();
        if kb <= a {
            return 1.0;
        }
        if kb >= b {
            return 0.0;
        }
        let span = b - a;
        let mut sum = 0.0;
        for n in 0..self.n_terms {
            let u = n as f64 * PI / span;
            let phi = self.cf.phi(u);
            let (cos_ua, sin_ua) = ((-u * a).cos(), (-u * a).sin());
            let re = phi.re * cos_ua - phi.im * sin_ua;
            let v = Self::payoff_coeff(n, a, b, kb);
            let term = re * v;
            sum += if n == 0 { 0.5 * term } else { term };
        }
        sum.clamp(0.0, 1.0)
    }
    fn name(&self) -> &str {
        "cos"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::black_scholes::BlackScholesDigital;
    use crate::charfn::GaussianCF;
    use crate::model::DigitalModel;
    use approx::assert_abs_diff_eq;
    use poly_core::PricingContext;

    fn ctx(s_t: f64, s0: f64) -> PricingContext {
        PricingContext {
            s_t,
            s0,
            tau: 5.0 / (365.0 * 24.0 * 60.0),
            mu: 0.05,
            sigma: 0.8,
            skew: 0.0,
            kurt: 3.0,
        }
    }

    #[test]
    fn cos_gaussian_matches_black_scholes_digital() {
        for (s_t, s0) in [
            (60_000.0, 60_000.0),
            (60_050.0, 60_000.0),
            (59_970.0, 60_000.0),
        ] {
            let c = ctx(s_t, s0);
            let mean = c.mu * c.tau;
            let var = c.sigma * c.sigma * c.tau;
            let cos = CosDigital::new(GaussianCF { mean, var }, 256);
            assert_abs_diff_eq!(cos.price(&c), BlackScholesDigital.price(&c), epsilon = 1e-4);
        }
    }

    #[test]
    fn cos_price_is_a_probability() {
        let var = 0.64 * (5.0 / (365.0 * 24.0 * 60.0));
        let cos = CosDigital::new(GaussianCF { mean: 0.0, var }, 256);
        let p = cos.price(&ctx(60_010.0, 60_000.0));
        assert!((0.0..=1.0).contains(&p), "p={p}");
    }
}
