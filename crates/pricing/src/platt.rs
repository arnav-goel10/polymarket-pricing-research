//! Online Platt scaling — learns the "sharpening" recalibration map from
//! realized outcomes instead of guessing a fixed sharpness constant.
//!
//! The 5-minute crypto market is *calibrated but not sharp*: posted probabilities
//! hug 0.5 even when the outcome is largely decided (SSRN 6863546). The principled
//! fix is post-hoc probability calibration. We fit a logistic map on the model's
//! probability in logit space:
//!
//! ```text
//! p' = sigmoid(a * logit(p) + b)
//! ```
//!
//! `a > 1` sharpens (steepens) an underconfident forecast — the learned analogue
//! of the fixed `sharpness` factor; `b` corrects a directional (YES/NO) bias.
//! Parameters are fit online by SGD on log-loss as each window resolves
//! (Online Platt Scaling, Gupta & Ramdas 2023), so the transform adapts as the
//! market's regime drifts.

const EPS: f64 = 1e-6;

fn sigmoid(z: f64) -> f64 {
    1.0 / (1.0 + (-z).exp())
}

fn logit(p: f64) -> f64 {
    let p = p.clamp(EPS, 1.0 - EPS);
    (p / (1.0 - p)).ln()
}

/// Online Platt calibrator. Starts at the identity map (`a=1, b=0`) so before any
/// training it passes the raw probability through unchanged.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OnlinePlatt {
    a: f64,
    b: f64,
    lr: f64,
    samples: usize,
}

impl Default for OnlinePlatt {
    fn default() -> Self {
        Self::new(0.05)
    }
}

impl OnlinePlatt {
    pub fn new(lr: f64) -> Self {
        OnlinePlatt {
            a: 1.0,
            b: 0.0,
            lr: lr.max(0.0),
            samples: 0,
        }
    }

    /// Recalibrate a raw model probability through the learned map.
    pub fn apply(&self, p: f64) -> f64 {
        let out = sigmoid(self.a * logit(p) + self.b);
        if out.is_finite() {
            out.clamp(0.0, 1.0)
        } else {
            p.clamp(0.0, 1.0)
        }
    }

    /// One SGD step on log-loss for a resolved (raw prediction, outcome) pair.
    /// `outcome_up` is the realized label. Keeps `a` non-negative (a flip would be
    /// pathological) and lightly bounds it so a noisy early sample can't explode it.
    pub fn update(&mut self, raw_p: f64, outcome_up: bool) {
        let x = logit(raw_p);
        let y = if outcome_up { 1.0 } else { 0.0 };
        let pred = sigmoid(self.a * x + self.b);
        // d(logloss)/d(z) = (pred - y); chain through z = a*x + b.
        let grad = pred - y;
        // Decay the step as we gather samples for stability (Robbins-Monro-ish).
        let step = self.lr / (1.0 + self.samples as f64 * 0.01);
        self.a -= step * grad * x;
        self.b -= step * grad;
        self.a = self.a.clamp(0.25, 6.0);
        self.b = self.b.clamp(-3.0, 3.0);
        self.samples += 1;
    }

    pub fn slope(&self) -> f64 {
        self.a
    }
    pub fn bias(&self) -> f64 {
        self.b
    }
    pub fn samples(&self) -> usize {
        self.samples
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_abs_diff_eq;

    #[test]
    fn identity_before_training() {
        let p = OnlinePlatt::default();
        assert_abs_diff_eq!(p.apply(0.7), 0.7, epsilon = 1e-9);
        assert_abs_diff_eq!(p.apply(0.3), 0.3, epsilon = 1e-9);
    }

    #[test]
    fn learns_to_sharpen_underconfident_forecasts() {
        // Market says 0.55 when Up, 0.45 when Down — directionally right but timid.
        // After training, applying the map to 0.55 should push it ABOVE 0.55.
        let mut p = OnlinePlatt::new(0.2);
        for _ in 0..400 {
            p.update(0.55, true);
            p.update(0.45, false);
        }
        assert!(p.slope() > 1.0, "should steepen, slope={}", p.slope());
        assert!(
            p.apply(0.55) > 0.55,
            "0.55 -> {} should be sharper",
            p.apply(0.55)
        );
        assert!(p.apply(0.45) < 0.45);
    }

    #[test]
    fn learns_directional_debias() {
        // Outcomes resolve Up only 30% of the time at p=0.5 -> bias should go negative.
        let mut p = OnlinePlatt::new(0.2);
        for i in 0..500 {
            p.update(0.5, i % 10 < 3); // 30% Up
        }
        assert!(p.bias() < 0.0, "bias={}", p.bias());
        assert!(p.apply(0.5) < 0.5);
    }

    #[test]
    fn output_is_always_a_probability() {
        let mut p = OnlinePlatt::new(0.5);
        for _ in 0..1000 {
            p.update(0.99, true);
            p.update(0.01, false);
        }
        for q in [0.0, 0.01, 0.5, 0.99, 1.0] {
            let r = p.apply(q);
            assert!((0.0..=1.0).contains(&r), "apply({q})={r}");
        }
    }
}
