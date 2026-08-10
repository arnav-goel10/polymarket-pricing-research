/// Exponentially-weighted moving-average variance (RiskMetrics style).
/// var_t = lambda * var_{t-1} + (1 - lambda) * r_t^2.
pub struct EwmaVar {
    lambda: f64,
    var: Option<f64>,
}

impl EwmaVar {
    pub fn new(lambda: f64) -> Self {
        EwmaVar { lambda, var: None }
    }

    pub fn update(&mut self, ret: f64) {
        let sq = ret * ret;
        self.var = Some(match self.var {
            None => sq,
            Some(v) => self.lambda * v + (1.0 - self.lambda) * sq,
        });
    }

    pub fn variance(&self) -> f64 {
        self.var.unwrap_or(0.0)
    }

    pub fn vol(&self) -> f64 {
        self.variance().sqrt()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_abs_diff_eq;

    #[test]
    fn first_update_seeds_with_squared_return() {
        let mut e = EwmaVar::new(0.94);
        e.update(0.01);
        assert_abs_diff_eq!(e.variance(), 0.0001, epsilon = 1e-15);
    }

    #[test]
    fn recursion_blends_old_and_new() {
        let mut e = EwmaVar::new(0.9);
        e.update(0.01);
        e.update(0.02);
        assert_abs_diff_eq!(e.variance(), 0.9 * 0.0001 + 0.1 * 0.0004, epsilon = 1e-15);
    }
}
