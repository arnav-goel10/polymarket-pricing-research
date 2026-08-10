use poly_core::ResolvedWindow;
use std::collections::VecDeque;

/// Rolling Brier/log-loss scoreboard for one model.
/// `decay` in [0,1): 0 = simple mean over the window; >0 = exponential recency.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Scoreboard {
    window: VecDeque<ResolvedWindow>,
    capacity: usize,
    decay: f64,
}

impl Scoreboard {
    pub fn new(decay: f64) -> Self {
        Scoreboard {
            window: VecDeque::new(),
            capacity: 500,
            decay,
        }
    }

    pub fn record(&mut self, r: ResolvedWindow) {
        if self.window.len() == self.capacity {
            self.window.pop_front();
        }
        self.window.push_back(r);
    }

    fn weighted(&self, f: impl Fn(&ResolvedWindow) -> f64) -> f64 {
        if self.window.is_empty() {
            return 0.0;
        }
        if self.decay <= 0.0 {
            let n = self.window.len() as f64;
            return self.window.iter().map(&f).sum::<f64>() / n;
        }
        let mut num = 0.0;
        let mut den = 0.0;
        let n = self.window.len();
        for (i, r) in self.window.iter().enumerate() {
            let age = (n - 1 - i) as f64;
            let w = (1.0 - self.decay).powf(age);
            num += w * f(r);
            den += w;
        }
        num / den
    }

    /// Mean squared error of probability vs. {0,1} outcome.
    pub fn brier(&self) -> f64 {
        self.weighted(|r| {
            let y = if r.outcome_up { 1.0 } else { 0.0 };
            (r.p_yes_predicted - y).powi(2)
        })
    }

    /// Negative log-likelihood (lower is better).
    pub fn log_loss(&self) -> f64 {
        self.weighted(|r| {
            let p = r.p_yes_predicted.clamp(1e-9, 1.0 - 1e-9);
            if r.outcome_up {
                -p.ln()
            } else {
                -(1.0 - p).ln()
            }
        })
    }
}

/// Turn per-model Brier scores into ensemble weights (lower score => higher
/// weight) via a softmax over `-beta * score`.
pub fn softmax_weights(scores: &[f64], beta: f64) -> Vec<f64> {
    if scores.is_empty() {
        return vec![];
    }
    let neg: Vec<f64> = scores.iter().map(|s| -beta * s).collect();
    let max = neg.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let exps: Vec<f64> = neg.iter().map(|x| (x - max).exp()).collect();
    let sum: f64 = exps.iter().sum();
    exps.iter().map(|e| e / sum).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_abs_diff_eq;
    use poly_core::ResolvedWindow;

    #[test]
    fn perfect_predictions_score_zero_brier() {
        let mut s = Scoreboard::new(0.0);
        s.record(ResolvedWindow {
            p_yes_predicted: 1.0,
            outcome_up: true,
        });
        s.record(ResolvedWindow {
            p_yes_predicted: 0.0,
            outcome_up: false,
        });
        assert_abs_diff_eq!(s.brier(), 0.0, epsilon = 1e-12);
    }

    #[test]
    fn always_half_scores_quarter_brier() {
        let mut s = Scoreboard::new(0.0);
        for up in [true, false, true, false] {
            s.record(ResolvedWindow {
                p_yes_predicted: 0.5,
                outcome_up: up,
            });
        }
        assert_abs_diff_eq!(s.brier(), 0.25, epsilon = 1e-12);
    }

    #[test]
    fn lower_brier_gets_more_weight() {
        let good = {
            let mut s = Scoreboard::new(0.0);
            s.record(ResolvedWindow {
                p_yes_predicted: 0.9,
                outcome_up: true,
            });
            s
        };
        let bad = {
            let mut s = Scoreboard::new(0.0);
            s.record(ResolvedWindow {
                p_yes_predicted: 0.1,
                outcome_up: true,
            });
            s
        };
        let w = softmax_weights(&[good.brier(), bad.brier()], 5.0);
        assert!(w[0] > w[1]);
        assert_abs_diff_eq!(w[0] + w[1], 1.0, epsilon = 1e-12);
    }
}
