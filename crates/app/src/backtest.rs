//! Offline scoring of pricing models against resolved windows.

use poly_core::{PricingContext, ResolvedWindow};
use pricing::{softmax_weights, DigitalModel, Scoreboard};

/// One resolved observation: the context at decision time and the outcome.
#[derive(Debug, Clone, Copy)]
pub struct Sample {
    pub ctx: PricingContext,
    pub outcome_up: bool,
}

/// Per-model calibration result.
#[derive(Debug, Clone, PartialEq)]
pub struct ModelScore {
    pub name: String,
    pub brier: f64,
    pub log_loss: f64,
}

/// Score every model over the samples (Brier + log-loss, lower is better).
pub fn score_models(models: &[Box<dyn DigitalModel>], samples: &[Sample]) -> Vec<ModelScore> {
    models
        .iter()
        .map(|m| {
            let mut sb = Scoreboard::new(0.0);
            for s in samples {
                sb.record(ResolvedWindow {
                    p_yes_predicted: m.price(&s.ctx),
                    outcome_up: s.outcome_up,
                });
            }
            ModelScore {
                name: m.name().to_string(),
                brier: sb.brier(),
                log_loss: sb.log_loss(),
            }
        })
        .collect()
}

/// Ensemble weights from Brier scores (lower Brier => more weight).
pub fn suggest_weights(scores: &[ModelScore], beta: f64) -> Vec<f64> {
    let briers: Vec<f64> = scores.iter().map(|s| s.brier).collect();
    softmax_weights(&briers, beta)
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_abs_diff_eq;
    use pricing::{BlackScholesDigital, StudentTDigital};

    fn ctx(s_t: f64) -> PricingContext {
        PricingContext {
            s_t,
            s0: 60_000.0,
            tau: 5.0 / (365.0 * 24.0 * 60.0),
            mu: 0.0,
            sigma: 0.8,
            skew: 0.0,
            kurt: 3.0,
        }
    }

    /// A degenerate model that always returns a fixed probability (for scoring tests).
    struct Const(f64);
    impl DigitalModel for Const {
        fn price(&self, _c: &PricingContext) -> f64 {
            self.0
        }
        fn name(&self) -> &str {
            "const"
        }
    }

    #[test]
    fn perfect_model_scores_zero_brier() {
        let samples = vec![
            Sample {
                ctx: ctx(60_500.0),
                outcome_up: true,
            },
            Sample {
                ctx: ctx(59_500.0),
                outcome_up: false,
            },
        ];
        // a model that predicts 1.0 then... use two const models is awkward; instead
        // verify a near-perfect predictor (BS deep ITM/OTM) has very low Brier.
        let models: Vec<Box<dyn DigitalModel>> = vec![Box::new(BlackScholesDigital)];
        let s = score_models(&models, &samples);
        assert!(
            s[0].brier < 0.01,
            "deep in/out the money BS should be ~perfect: {}",
            s[0].brier
        );
    }

    #[test]
    fn lower_brier_model_gets_more_weight() {
        // outcome always Up; a const-0.9 model beats a const-0.1 model.
        let samples = vec![Sample {
            ctx: ctx(60_000.0),
            outcome_up: true,
        }];
        let models: Vec<Box<dyn DigitalModel>> = vec![Box::new(Const(0.9)), Box::new(Const(0.1))];
        let scores = score_models(&models, &samples);
        assert!(scores[0].brier < scores[1].brier);
        let w = suggest_weights(&scores, 5.0);
        assert!(w[0] > w[1]);
        assert_abs_diff_eq!(w[0] + w[1], 1.0, epsilon = 1e-12);
    }

    #[test]
    fn scores_multiple_models() {
        let samples = vec![Sample {
            ctx: ctx(60_200.0),
            outcome_up: true,
        }];
        let models: Vec<Box<dyn DigitalModel>> = vec![
            Box::new(BlackScholesDigital),
            Box::new(StudentTDigital { nu: 4.0 }),
        ];
        assert_eq!(score_models(&models, &samples).len(), 2);
    }
}
