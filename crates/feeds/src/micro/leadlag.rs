/// Normalized (Pearson) cross-correlation of `a` and `b` at integer `lag`
/// (positive => a leads b). Means are subtracted and the product is divided by
/// the per-lag standard deviations, so the result is a true correlation in
/// [-1, 1] and lag detection isn't biased toward whichever lag happens to align
/// the highest-variance ticks (the old raw-product version's failure mode).
fn xcorr(a: &[f64], b: &[f64], lag: i64) -> f64 {
    let n = a.len().min(b.len()) as i64;
    // Collect the overlapping (a[i-lag], b[i]) pairs for this lag.
    let mut xs = Vec::new();
    let mut ys = Vec::new();
    for i in 0..n {
        let j = i - lag;
        if j >= 0 && j < n {
            xs.push(a[j as usize]);
            ys.push(b[i as usize]);
        }
    }
    let m = xs.len();
    if m < 2 {
        return 0.0;
    }
    let mean = |v: &[f64]| v.iter().sum::<f64>() / v.len() as f64;
    let (mx, my) = (mean(&xs), mean(&ys));
    let mut cov = 0.0;
    let mut vx = 0.0;
    let mut vy = 0.0;
    for k in 0..m {
        let dx = xs[k] - mx;
        let dy = ys[k] - my;
        cov += dx * dy;
        vx += dx * dx;
        vy += dy * dy;
    }
    let denom = (vx * vy).sqrt();
    if denom > 0.0 {
        cov / denom
    } else {
        0.0
    }
}

/// Lag in [-max_lag, max_lag] maximizing cross-correlation. Positive => `a` leads `b`.
pub fn best_lag(a: &[f64], b: &[f64], max_lag: i64) -> i64 {
    let mut best = 0i64;
    let mut best_val = f64::NEG_INFINITY;
    for lag in -max_lag..=max_lag {
        let v = xcorr(a, b, lag);
        if v > best_val {
            best_val = v;
            best = lag;
        }
    }
    best
}

/// Rolling lead-lag tracker over recent return series from two venues.
#[derive(Debug, Default, Clone)]
pub struct LeadLag {
    pub lead_steps: i64,
}

impl LeadLag {
    pub fn update(&mut self, a_returns: &[f64], b_returns: &[f64], max_lag: i64) {
        self.lead_steps = best_lag(a_returns, b_returns, max_lag);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_a_leading_b_by_one_step() {
        let a = vec![0.0, 1.0, -1.0, 2.0, -2.0, 0.5, 0.3, -0.4];
        let mut b = vec![0.0];
        b.extend_from_slice(&a[..a.len() - 1]);
        assert_eq!(best_lag(&a, &b, 3), 1);
    }

    #[test]
    fn zero_lag_for_identical_series() {
        let a = vec![0.1, -0.2, 0.3, -0.1, 0.2, -0.3];
        assert_eq!(best_lag(&a, &a, 3), 0);
    }
}
