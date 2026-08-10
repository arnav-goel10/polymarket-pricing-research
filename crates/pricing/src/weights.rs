//! Online ensemble weights via **Dynamic Model Averaging** (Raftery, Kárný &
//! Ettler 2010) — a forgetting-factor generalisation of Bayesian model averaging /
//! multiplicative-weights, purpose-built for settings where the *best model
//! changes over time*. That is exactly this market: which pricing model is sharpest
//! drifts with the volatility regime, so a fixed or slowly-cumulative weighting
//! lags. DMA is the standard in finance/macro for time-varying combination
//! (Raftery 2010; Koop & Korobilis 2012, "Forecasting inflation using DMA").
//!
//! Each resolved window:
//!   1. **Forget:**  `w_i ← w_i^α`  (α∈(0,1)) — discounts stale performance and
//!      pulls extreme weights back toward uniform, so a model that recovers can
//!      regain weight. α≈0.97 ⇒ effective memory ~1/(1-α) ≈ 33 windows (~2.7h),
//!      matched to how fast 5-minute vol regimes turn over.
//!   2. **Update:** `w_i ← w_i · e^(-η·Brier_i)` — the (tempered) predictive
//!      likelihood; Brier (square loss) is *mixable*, so this multiplicative form
//!      is the right update and is robust (bounded loss, no blow-ups from a single
//!      confident miss the way raw log-likelihood would).
//!   3. **Renormalise.**
//!
//! This reduces to plain Hedge/BMA at α=1, and to recency-tracking (Herbster-
//! Warmuth "tracking the best expert") for α<1. The fixed equal-weight `blend`
//! is kept alongside as the robust "simple-average-is-hard-to-beat" baseline
//! (Smith & Wallis 2009, the forecast-combination puzzle) — DMA only earns its
//! keep if it actually beats that.

use std::collections::HashMap;

fn default_alpha() -> f64 {
    0.97
}
fn default_eta() -> f64 {
    2.0
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OnlineWeights {
    #[serde(default)]
    w: HashMap<String, f64>,
    // Field-level defaults so persisted state survives parameter/shape changes
    // (a missing field resumes at the default instead of nuking the whole resume).
    #[serde(default = "default_alpha")]
    alpha: f64, // forgetting factor in (0,1]; <1 = discount stale performance
    #[serde(default = "default_eta")]
    eta: f64, // likelihood temperature (penalty strength per unit Brier loss)
}

impl Default for OnlineWeights {
    fn default() -> Self {
        Self::new(0.97, 2.0)
    }
}

impl OnlineWeights {
    pub fn new(alpha: f64, eta: f64) -> Self {
        OnlineWeights {
            w: HashMap::new(),
            alpha: alpha.clamp(0.5, 1.0),
            eta: eta.max(0.0),
        }
    }

    fn ensure(&mut self, names: &[String]) {
        for n in names {
            self.w.entry(n.clone()).or_insert(1.0);
        }
    }

    /// Pre-register components at uniform weight so they're visible before any
    /// window has resolved (no behavioural effect — learning still starts uniform).
    pub fn seed(&mut self, names: &[&str]) {
        for n in names {
            self.w.entry((*n).to_string()).or_insert(1.0);
        }
    }

    /// Weighted combination of `(name, prob)` forecasts using the current weights
    /// (normalised over the present names). Simple average until weights are learned.
    pub fn combine(&self, preds: &[(String, f64)]) -> f64 {
        if preds.is_empty() {
            return 0.5;
        }
        let total: f64 = preds
            .iter()
            .map(|(n, _)| self.w.get(n).copied().unwrap_or(1.0))
            .sum();
        if total <= 0.0 || !total.is_finite() {
            return preds.iter().map(|(_, p)| p).sum::<f64>() / preds.len() as f64;
        }
        preds
            .iter()
            .map(|(n, p)| self.w.get(n).copied().unwrap_or(1.0) / total * p)
            .sum()
    }

    /// DMA update from a resolved window: forget (`w^α`), then multiply by the
    /// tempered Brier likelihood, then renormalise.
    pub fn update(&mut self, preds: &[(String, f64)], outcome_up: bool) {
        let names: Vec<String> = preds.iter().map(|(n, _)| n.clone()).collect();
        if names.is_empty() {
            return;
        }
        self.ensure(&names);
        let y = if outcome_up { 1.0 } else { 0.0 };
        // 1. forget + normalise the forgotten weights (keeps the power step stable)
        let mut forgotten: HashMap<String, f64> = HashMap::new();
        let mut fsum = 0.0;
        for n in &names {
            let f = self.w[n].max(1e-12).powf(self.alpha);
            forgotten.insert(n.clone(), f);
            fsum += f;
        }
        for n in &names {
            forgotten.insert(n.clone(), forgotten[n] / fsum.max(1e-300));
        }
        // 2. multiply by the tempered Brier likelihood
        for (n, p) in preds {
            let loss = (p - y).powi(2); // Brier in [0,1]
            let w = forgotten[n] * (-self.eta * loss).exp();
            forgotten.insert(n.clone(), if w.is_finite() { w.max(1e-12) } else { 1e-12 });
        }
        // 3. renormalise
        let sum: f64 = names.iter().map(|n| forgotten[n]).sum();
        if sum > 0.0 {
            for n in &names {
                self.w.insert(n.clone(), forgotten[n] / sum);
            }
        }
    }

    /// Current normalised weights, sorted desc (for display / hover).
    pub fn snapshot(&self) -> Vec<(String, f64)> {
        let sum: f64 = self.w.values().sum();
        if sum <= 0.0 {
            return self.w.keys().map(|n| (n.clone(), 0.0)).collect();
        }
        let mut out: Vec<_> = self.w.iter().map(|(n, w)| (n.clone(), w / sum)).collect();
        out.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_abs_diff_eq;

    #[test]
    fn starts_as_simple_average() {
        let w = OnlineWeights::default();
        let preds = vec![("a".into(), 0.8), ("b".into(), 0.4)];
        assert_abs_diff_eq!(w.combine(&preds), 0.6, epsilon = 1e-9);
    }

    #[test]
    fn upweights_the_better_forecaster() {
        // `a` predicts 0.9 on Up (right), `b` predicts 0.1 on Up (wrong).
        let mut w = OnlineWeights::new(0.97, 2.0);
        for _ in 0..60 {
            w.update(&[("a".into(), 0.9), ("b".into(), 0.1)], true);
        }
        let snap: std::collections::HashMap<_, _> = w.snapshot().into_iter().collect();
        assert!(
            snap["a"] > 0.8,
            "good model should dominate, got {}",
            snap["a"]
        );
        assert!(w.combine(&[("a".into(), 0.9), ("b".into(), 0.1)]) > 0.75);
    }

    #[test]
    fn forgetting_lets_a_recovered_model_regain_weight() {
        // `a` good then bad, `b` bad then good — forgetting should hand weight to b.
        let mut w = OnlineWeights::new(0.9, 3.0);
        for _ in 0..40 {
            w.update(&[("a".into(), 0.9), ("b".into(), 0.1)], true); // a right
        }
        let a_before = w
            .snapshot()
            .into_iter()
            .collect::<std::collections::HashMap<_, _>>()["a"];
        for _ in 0..40 {
            w.update(&[("a".into(), 0.9), ("b".into(), 0.1)], false); // now b right
        }
        let snap: std::collections::HashMap<_, _> = w.snapshot().into_iter().collect();
        assert!(
            snap["b"] > snap["a"],
            "b should overtake after regime flip: a={} b={}",
            snap["a"],
            snap["b"]
        );
        assert!(a_before > 0.5); // a really did dominate first
    }

    #[test]
    fn output_is_a_probability() {
        let mut w = OnlineWeights::default();
        for i in 0..100 {
            w.update(&[("a".into(), 0.7), ("b".into(), 0.3)], i % 2 == 0);
        }
        let r = w.combine(&[("a".into(), 0.7), ("b".into(), 0.3)]);
        assert!((0.0..=1.0).contains(&r));
    }
}
