//! Adaptive edge threshold. Instead of a fixed `min_edge`, this learns each
//! market's edge regime over time: it tracks the *maximum* edge that appears in
//! each window, keeps a rolling upper quantile of those maxima, and sets the
//! acting threshold to a fraction of that robust reference. This behaves like a
//! no-trade band: ignore ordinary/noisy edge, act only on unusually good
//! opportunities, and avoid letting one huge dislocation dominate the threshold.

use poly_core::config::AdaptiveEdgeMode;

#[derive(Debug, Clone, Copy)]
pub struct AdaptiveEdgeParams {
    pub fraction: f64,
    pub floor: f64,
    pub ceil: f64,
    pub lambda: f64,
    pub quantile: f64,
    pub max_samples: usize,
    pub mode: AdaptiveEdgeMode,
    pub instability_buffer: f64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AdaptiveEdge {
    ewma_max: f64, // smoothed per-window maximum edge (fallback/display)
    window_max: f64,
    fraction: f64, // threshold = fraction * robust max-edge reference
    floor: f64,
    ceil: f64,
    lambda: f64,
    samples: usize,
    #[serde(default)]
    recent_maxima: Vec<f64>,
    #[serde(default = "default_quantile")]
    quantile: f64,
    #[serde(default = "default_max_samples")]
    max_samples: usize,
    #[serde(default)]
    mode: AdaptiveEdgeMode,
    #[serde(default = "default_instability_buffer")]
    instability_buffer: f64,
}

impl AdaptiveEdge {
    pub fn new(
        fraction: f64,
        floor: f64,
        ceil: f64,
        lambda: f64,
        quantile: f64,
        max_samples: usize,
    ) -> Self {
        Self::from_params(AdaptiveEdgeParams {
            fraction,
            floor,
            ceil,
            lambda,
            quantile,
            max_samples,
            mode: AdaptiveEdgeMode::RobustQuantile,
            instability_buffer: default_instability_buffer(),
        })
    }

    pub fn from_params(params: AdaptiveEdgeParams) -> Self {
        AdaptiveEdge {
            ewma_max: 0.0,
            window_max: 0.0,
            fraction: params.fraction.max(0.0),
            floor: params.floor.max(0.0),
            ceil: params.ceil.max(params.floor),
            lambda: params.lambda.clamp(0.0, 0.999),
            samples: 0,
            recent_maxima: Vec::new(),
            quantile: params.quantile.clamp(0.50, 0.99),
            max_samples: params.max_samples.max(8),
            mode: params.mode,
            instability_buffer: params.instability_buffer.clamp(0.0, 4.0),
        }
    }

    /// Record the best edge available right now (>=0). Keeps the window's max.
    pub fn observe(&mut self, edge: f64) {
        if edge > self.window_max {
            self.window_max = edge;
        }
    }

    /// Close the current window: fold its max edge into the EWMA, reset.
    pub fn roll_window(&mut self) {
        self.remember_window_max();
        if self.samples == 0 {
            // First window: only initialize from a non-zero observation.
            if self.window_max > 0.0 {
                self.ewma_max = self.window_max;
                self.samples += 1;
            }
        } else {
            // Subsequent windows: always update even on zero-max (book outage / no edge).
            // Without this, a prolonged outage freezes the threshold at its last value
            // instead of decaying toward 0, causing the system to over-filter entries
            // when the book returns.
            self.ewma_max = self.lambda * self.ewma_max + (1.0 - self.lambda) * self.window_max;
            self.samples += 1;
        }
        self.window_max = 0.0;
    }

    /// The current acting threshold. Until we've seen any window, use the floor.
    pub fn threshold(&self) -> f64 {
        if self.samples == 0 {
            return self.floor;
        }
        self.raw_threshold().clamp(self.floor, self.ceil)
    }

    /// The learned robust max-edge reference for display.
    pub fn typical_max(&self) -> f64 {
        if self.samples == 0 {
            0.0
        } else {
            self.robust_reference()
        }
    }

    pub fn samples(&self) -> usize {
        self.samples
    }

    fn remember_window_max(&mut self) {
        if !self.window_max.is_finite() {
            return;
        }
        self.recent_maxima.push(self.window_max.max(0.0));
        if self.recent_maxima.len() > self.max_samples {
            let excess = self.recent_maxima.len() - self.max_samples;
            self.recent_maxima.drain(0..excess);
        }
    }

    fn robust_reference(&self) -> f64 {
        if self.recent_maxima.len() >= 8 {
            unweighted_quantile(&self.recent_maxima, self.quantile)
        } else {
            self.ewma_max
        }
    }

    fn raw_threshold(&self) -> f64 {
        match self.mode {
            AdaptiveEdgeMode::RobustQuantile => self.fraction * self.robust_reference(),
            AdaptiveEdgeMode::Conformal => self.conformal_threshold(),
        }
    }

    fn conformal_threshold(&self) -> f64 {
        if self.recent_maxima.len() < 8 {
            return self.fraction * self.ewma_max;
        }
        let location = weighted_quantile_recent(&self.recent_maxima, self.quantile, self.lambda);
        if self.instability_buffer <= 0.0 {
            return location;
        }
        let median = weighted_quantile_recent(&self.recent_maxima, 0.5, self.lambda);
        let residuals: Vec<f64> = self
            .recent_maxima
            .iter()
            .map(|x| (x - median).abs())
            .collect();
        let mad = weighted_quantile_recent(&residuals, 0.5, self.lambda);
        location + self.instability_buffer * mad
    }
}

impl Default for AdaptiveEdge {
    fn default() -> Self {
        Self::new(
            0.5,
            0.05,
            0.20,
            0.9,
            default_quantile(),
            default_max_samples(),
        )
    }
}

fn default_quantile() -> f64 {
    0.75
}

fn default_max_samples() -> usize {
    96
}

fn default_instability_buffer() -> f64 {
    0.25
}

fn unweighted_quantile(values: &[f64], q: f64) -> f64 {
    let mut xs: Vec<f64> = values.iter().copied().filter(|v| v.is_finite()).collect();
    if xs.is_empty() {
        return 0.0;
    }
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    if xs.len() == 1 {
        return xs[0];
    }
    let pos = (xs.len() - 1) as f64 * q.clamp(0.0, 1.0);
    let lo = pos.floor() as usize;
    let hi = pos.ceil() as usize;
    if lo == hi {
        xs[lo]
    } else {
        let w = pos - lo as f64;
        xs[lo] * (1.0 - w) + xs[hi] * w
    }
}

fn weighted_quantile_recent(values: &[f64], q: f64, lambda: f64) -> f64 {
    let decay = lambda.clamp(0.0, 0.999);
    let newest = values.len().saturating_sub(1);
    let mut xs: Vec<(f64, f64)> = values
        .iter()
        .enumerate()
        .filter(|(_, v)| v.is_finite())
        .map(|(i, v)| {
            let age = newest.saturating_sub(i) as i32;
            (*v, decay.powi(age))
        })
        .collect();
    if xs.is_empty() {
        return 0.0;
    }
    xs.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    let total_weight: f64 = xs.iter().map(|(_, w)| *w).sum();
    if total_weight <= 0.0 {
        return xs.last().map(|(v, _)| *v).unwrap_or(0.0);
    }
    let target = total_weight * q.clamp(0.0, 1.0);
    let mut seen = 0.0;
    for (value, weight) in xs {
        seen += weight;
        if seen >= target {
            return value;
        }
    }
    0.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_abs_diff_eq;

    #[test]
    fn before_any_window_uses_floor() {
        let a = AdaptiveEdge::new(0.5, 0.01, 0.2, 0.8, 0.75, 96);
        assert_abs_diff_eq!(a.threshold(), 0.01, epsilon = 1e-12);
    }

    #[test]
    fn threshold_is_fraction_of_typical_max() {
        let mut a = AdaptiveEdge::new(0.5, 0.01, 0.5, 0.8, 0.75, 96);
        a.observe(0.02);
        a.observe(0.10); // window max = 0.10
        a.observe(0.04);
        a.roll_window();
        // first window: ewma_max = 0.10; threshold = 0.5 * 0.10 = 0.05
        assert_abs_diff_eq!(a.typical_max(), 0.10, epsilon = 1e-12);
        assert_abs_diff_eq!(a.threshold(), 0.05, epsilon = 1e-12);
    }

    #[test]
    fn ewma_blends_across_windows() {
        let mut a = AdaptiveEdge::new(0.5, 0.0, 1.0, 0.5, 0.75, 96);
        a.observe(0.10);
        a.roll_window(); // ewma = 0.10
        a.observe(0.20);
        a.roll_window(); // ewma = 0.5*0.10 + 0.5*0.20 = 0.15
        assert_abs_diff_eq!(a.typical_max(), 0.15, epsilon = 1e-12);
    }

    #[test]
    fn threshold_is_clamped_to_floor_and_ceil() {
        let mut a = AdaptiveEdge::new(0.5, 0.03, 0.06, 0.0, 0.75, 96);
        a.observe(0.01); // tiny edge regime
        a.roll_window(); // ewma 0.01 -> 0.5*0.01 = 0.005 -> clamped up to floor 0.03
        assert_abs_diff_eq!(a.threshold(), 0.03, epsilon = 1e-12);

        let mut b = AdaptiveEdge::new(0.5, 0.03, 0.06, 0.0, 0.75, 96);
        b.observe(0.30); // huge edge regime
        b.roll_window(); // 0.5*0.30 = 0.15 -> clamped down to ceil 0.06
        assert_abs_diff_eq!(b.threshold(), 0.06, epsilon = 1e-12);
    }

    #[test]
    fn rolling_quantile_reduces_single_outlier_influence() {
        let mut a = AdaptiveEdge::new(0.5, 0.0, 1.0, 0.9, 0.75, 16);
        for _ in 0..12 {
            a.observe(0.10);
            a.roll_window();
        }
        a.observe(1.00);
        a.roll_window();

        // A max-EWMA threshold would jump materially after the outlier. The rolling
        // upper quantile still treats the ordinary edge regime as the reference.
        assert_abs_diff_eq!(a.typical_max(), 0.10, epsilon = 1e-12);
        assert_abs_diff_eq!(a.threshold(), 0.05, epsilon = 1e-12);
    }

    #[test]
    fn rolling_quantile_decays_after_quiet_windows() {
        let mut a = AdaptiveEdge::new(0.5, 0.0, 1.0, 0.9, 0.75, 16);
        for _ in 0..12 {
            a.observe(0.20);
            a.roll_window();
        }
        assert_abs_diff_eq!(a.typical_max(), 0.20, epsilon = 1e-12);

        for _ in 0..13 {
            a.roll_window();
        }

        assert_abs_diff_eq!(a.typical_max(), 0.0, epsilon = 1e-12);
        assert_abs_diff_eq!(a.threshold(), 0.0, epsilon = 1e-12);
    }

    #[test]
    fn conformal_mode_targets_weighted_upper_tail_without_fraction_discount() {
        let mut a = AdaptiveEdge::from_params(AdaptiveEdgeParams {
            fraction: 0.5,
            floor: 0.0,
            ceil: 1.0,
            lambda: 0.9,
            quantile: 0.75,
            max_samples: 16,
            mode: AdaptiveEdgeMode::Conformal,
            instability_buffer: 0.0,
        });
        for edge in [0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10, 0.20] {
            a.observe(edge);
            a.roll_window();
        }

        assert!(a.threshold() >= 0.10);
        assert!(a.threshold() <= 0.20);
    }

    #[test]
    fn conformal_mode_weights_recent_regime_more_than_old_regime() {
        let mut a = AdaptiveEdge::from_params(AdaptiveEdgeParams {
            fraction: 0.5,
            floor: 0.0,
            ceil: 1.0,
            lambda: 0.5,
            quantile: 0.75,
            max_samples: 32,
            mode: AdaptiveEdgeMode::Conformal,
            instability_buffer: 0.0,
        });
        for _ in 0..8 {
            a.observe(0.20);
            a.roll_window();
        }
        for _ in 0..8 {
            a.observe(0.04);
            a.roll_window();
        }

        assert!(a.threshold() < 0.20);
    }

    #[test]
    fn conformal_instability_buffer_raises_threshold_in_choppy_regime() {
        let mut plain = AdaptiveEdge::from_params(AdaptiveEdgeParams {
            fraction: 0.5,
            floor: 0.0,
            ceil: 1.0,
            lambda: 0.9,
            quantile: 0.75,
            max_samples: 16,
            mode: AdaptiveEdgeMode::Conformal,
            instability_buffer: 0.0,
        });
        let mut buffered = AdaptiveEdge::from_params(AdaptiveEdgeParams {
            fraction: 0.5,
            floor: 0.0,
            ceil: 1.0,
            lambda: 0.9,
            quantile: 0.75,
            max_samples: 16,
            mode: AdaptiveEdgeMode::Conformal,
            instability_buffer: 1.0,
        });
        for edge in [0.04, 0.12, 0.05, 0.13, 0.06, 0.14, 0.07, 0.15] {
            plain.observe(edge);
            plain.roll_window();
            buffered.observe(edge);
            buffered.roll_window();
        }

        assert!(buffered.threshold() > plain.threshold());
    }
}
