use crate::model::DigitalModel;
use poly_core::{ModelPrice, PricingContext};

/// Weighted blend of digital models. Weights default to uniform and are
/// updated externally from the calibration scoreboard.
pub struct Ensemble {
    models: Vec<Box<dyn DigitalModel>>,
    weights: Vec<f64>,
}

impl Ensemble {
    pub fn new(models: Vec<Box<dyn DigitalModel>>) -> Self {
        let n = models.len();
        let weights = if n == 0 {
            vec![]
        } else {
            vec![1.0 / n as f64; n]
        };
        Ensemble { models, weights }
    }

    pub fn set_weights(&mut self, weights: Vec<f64>) {
        assert_eq!(
            weights.len(),
            self.models.len(),
            "weight/model length mismatch"
        );
        let sum: f64 = weights.iter().sum();
        self.weights = if sum > 0.0 {
            weights.iter().map(|w| w / sum).collect()
        } else {
            vec![1.0 / self.models.len() as f64; self.models.len()]
        };
    }

    pub fn model_prices(&self, ctx: &PricingContext) -> Vec<ModelPrice> {
        self.models
            .iter()
            .map(|m| ModelPrice {
                p_yes: m.price(ctx),
            })
            .collect()
    }

    pub fn names(&self) -> Vec<&str> {
        self.models.iter().map(|m| m.name()).collect()
    }

    /// The ensemble fair value: weighted average of model prices.
    pub fn price(&self, ctx: &PricingContext) -> f64 {
        self.models
            .iter()
            .zip(&self.weights)
            .map(|(m, w)| w * m.price(ctx))
            .sum::<f64>()
            .clamp(0.0, 1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BlackScholesDigital, StudentTDigital};
    use approx::assert_abs_diff_eq;
    use poly_core::PricingContext;

    fn ctx() -> PricingContext {
        PricingContext {
            s_t: 60_040.0,
            s0: 60_000.0,
            tau: 5.0 / (365.0 * 24.0 * 60.0),
            mu: 0.0,
            sigma: 0.8,
            skew: 0.0,
            kurt: 3.0,
        }
    }

    #[test]
    fn equal_weights_average_the_models() {
        let ens = Ensemble::new(vec![
            Box::new(BlackScholesDigital),
            Box::new(StudentTDigital { nu: 4.0 }),
        ]);
        let prices = ens.model_prices(&ctx());
        let mean = (prices[0].p_yes + prices[1].p_yes) / 2.0;
        assert_abs_diff_eq!(ens.price(&ctx()), mean, epsilon = 1e-12);
    }

    #[test]
    fn weights_shift_the_blend() {
        let mut ens = Ensemble::new(vec![
            Box::new(BlackScholesDigital),
            Box::new(StudentTDigital { nu: 4.0 }),
        ]);
        ens.set_weights(vec![1.0, 0.0]);
        assert_abs_diff_eq!(
            ens.price(&ctx()),
            BlackScholesDigital.price(&ctx()),
            epsilon = 1e-12
        );
    }
}
