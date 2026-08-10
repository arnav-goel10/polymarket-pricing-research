/// Minimal complex number (avoids pulling in num-complex for one use).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct C {
    pub re: f64,
    pub im: f64,
}

impl C {
    #[inline]
    pub fn new(re: f64, im: f64) -> Self {
        C { re, im }
    }
    /// exp(z) for complex z.
    #[inline]
    pub fn exp(self) -> C {
        let m = self.re.exp();
        C::new(m * self.im.cos(), m * self.im.sin())
    }
}

/// Characteristic function of the terminal log-return over the window:
/// phi(u) = E[exp(i u R_tau)]. The COS method integrates this.
pub trait CharFn: Send + Sync {
    fn phi(&self, u: f64) -> C;
    /// Cumulants used to size the COS truncation [a, b].
    fn cumulants(&self) -> Cumulants;
}

/// First/second/fourth cumulants used to size the COS truncation [a, b].
#[derive(Debug, Clone, Copy)]
pub struct Cumulants {
    pub c1: f64,
    pub c2: f64,
    pub c4: f64,
}

/// Gaussian terminal return: R ~ N(mean, var).
pub struct GaussianCF {
    pub mean: f64,
    pub var: f64,
}

impl CharFn for GaussianCF {
    fn phi(&self, u: f64) -> C {
        let z = C::new(-0.5 * self.var * u * u, self.mean * u);
        z.exp()
    }
    fn cumulants(&self) -> Cumulants {
        Cumulants {
            c1: self.mean,
            c2: self.var,
            c4: 0.0,
        }
    }
}

/// Variance-Gamma terminal return over the window. `nu -> 0` recovers Gaussian.
pub struct VarianceGammaCF {
    mu0: f64,
    sigma: f64,
    theta: f64,
    nu: f64,
}

impl VarianceGammaCF {
    /// `mean` = target mean, `sigma` = std-dev scale, `theta` = skew
    /// (negative => left skew), `nu` = variance rate.
    pub fn new(mean: f64, sigma: f64, theta: f64, nu: f64) -> Self {
        VarianceGammaCF {
            mu0: mean - theta,
            sigma,
            theta,
            nu: nu.max(1e-9),
        }
    }

    fn cpow(base: C, p: f64) -> C {
        let r = (base.re * base.re + base.im * base.im).sqrt();
        let arg = base.im.atan2(base.re);
        let log = C::new(r.ln(), arg);
        C::new(p * log.re, p * log.im).exp()
    }
}

impl CharFn for VarianceGammaCF {
    fn phi(&self, u: f64) -> C {
        let inner = C::new(
            1.0 + 0.5 * self.sigma * self.sigma * self.nu * u * u,
            -self.theta * self.nu * u,
        );
        let powed = Self::cpow(inner, -1.0 / self.nu);
        let drift = C::new(0.0, self.mu0 * u).exp();
        C::new(
            drift.re * powed.re - drift.im * powed.im,
            drift.re * powed.im + drift.im * powed.re,
        )
    }
    fn cumulants(&self) -> Cumulants {
        let c1 = self.mu0 + self.theta;
        let c2 = self.sigma * self.sigma + self.theta * self.theta * self.nu;
        let c4 = 3.0 * self.sigma.powi(4) * self.nu
            + 12.0 * self.sigma * self.sigma * self.theta * self.theta * self.nu * self.nu
            + 6.0 * self.theta.powi(4) * self.nu.powi(3);
        Cumulants { c1, c2, c4 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_abs_diff_eq;

    #[test]
    fn gaussian_cf_at_zero_is_one() {
        let phi = GaussianCF {
            mean: 0.1,
            var: 0.04,
        }
        .phi(0.0);
        assert_abs_diff_eq!(phi.re, 1.0, epsilon = 1e-12);
        assert_abs_diff_eq!(phi.im, 0.0, epsilon = 1e-12);
    }

    #[test]
    fn gaussian_cf_matches_closed_form() {
        let cf = GaussianCF {
            mean: -0.02,
            var: 0.09,
        };
        let u = 1.7;
        let phi = cf.phi(u);
        let env = (-0.5 * 0.09 * u * u).exp();
        assert_abs_diff_eq!(phi.re, env * (-0.02 * u).cos(), epsilon = 1e-12);
        assert_abs_diff_eq!(phi.im, env * (-0.02 * u).sin(), epsilon = 1e-12);
    }

    #[test]
    fn vg_cf_at_zero_is_one() {
        let phi = VarianceGammaCF::new(0.0, 0.3, -0.1, 0.2).phi(0.0);
        assert_abs_diff_eq!(phi.re, 1.0, epsilon = 1e-12);
        assert_abs_diff_eq!(phi.im, 0.0, epsilon = 1e-12);
    }

    #[test]
    fn vg_approaches_gaussian_as_nu_to_zero() {
        use crate::black_scholes::BlackScholesDigital;
        use crate::cos::CosDigital;
        use crate::model::DigitalModel;
        use poly_core::PricingContext;
        let c = PricingContext {
            s_t: 60_050.0,
            s0: 60_000.0,
            tau: 5.0 / (365.0 * 24.0 * 60.0),
            mu: 0.0,
            sigma: 0.8,
            skew: 0.0,
            kurt: 3.0,
        };
        let mean = c.mu * c.tau;
        let var = c.sigma * c.sigma * c.tau;
        let vg = CosDigital::new(VarianceGammaCF::new(mean, var.sqrt(), 0.0, 1e-6), 256);
        assert_abs_diff_eq!(vg.price(&c), BlackScholesDigital.price(&c), epsilon = 5e-3);
    }
}
