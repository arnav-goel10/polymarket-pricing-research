/// Heterogeneous AutoRegressive realized-variance forecaster (Corsi 2009).
pub struct HarRv {
    pub c: f64,
    pub beta_d: f64,
    pub beta_w: f64,
    pub beta_m: f64,
}

impl HarRv {
    /// `rv_d`, `rv_w`, `rv_m` are realized variances over short/medium/long horizons.
    pub fn forecast(&self, rv_d: f64, rv_w: f64, rv_m: f64) -> f64 {
        (self.c + self.beta_d * rv_d + self.beta_w * rv_w + self.beta_m * rv_m).max(0.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_abs_diff_eq;

    #[test]
    fn constant_rv_forecasts_itself() {
        let har = HarRv {
            c: 0.0,
            beta_d: 0.3,
            beta_w: 0.4,
            beta_m: 0.3,
        };
        assert_abs_diff_eq!(har.forecast(0.04, 0.04, 0.04), 0.04, epsilon = 1e-12);
    }

    #[test]
    fn forecast_is_linear_combination() {
        let har = HarRv {
            c: 0.001,
            beta_d: 0.5,
            beta_w: 0.3,
            beta_m: 0.2,
        };
        let expected = 0.001 + 0.5 * 0.05 + 0.3 * 0.04 + 0.2 * 0.03;
        assert_abs_diff_eq!(har.forecast(0.05, 0.04, 0.03), expected, epsilon = 1e-15);
    }

    #[test]
    fn forecast_is_non_negative() {
        let har = HarRv {
            c: 0.0,
            beta_d: -2.0,
            beta_w: 0.0,
            beta_m: 0.0,
        };
        assert!(har.forecast(0.04, 0.04, 0.04) >= 0.0);
    }
}
