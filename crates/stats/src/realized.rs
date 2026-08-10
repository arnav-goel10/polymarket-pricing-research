/// Log returns of a price series.
pub fn log_returns(prices: &[f64]) -> Vec<f64> {
    prices.windows(2).map(|w| (w[1] / w[0]).ln()).collect()
}

/// Naive realized variance: sum of squared log returns.
pub fn realized_variance(prices: &[f64]) -> f64 {
    log_returns(prices).iter().map(|r| r * r).sum()
}

/// Parzen kernel weight on [0,1].
fn parzen(x: f64) -> f64 {
    let x = x.abs();
    if x <= 0.5 {
        1.0 - 6.0 * x * x + 6.0 * x * x * x
    } else if x <= 1.0 {
        2.0 * (1.0 - x).powi(3)
    } else {
        0.0
    }
}

/// Realized-kernel estimator (Barndorff-Nielsen et al.): noise-robust RV using
/// Parzen-weighted autocovariances of returns up to lag `h`.
pub fn realized_kernel(prices: &[f64], h: usize) -> f64 {
    let r = log_returns(prices);
    let n = r.len();
    if n == 0 {
        return 0.0;
    }
    let gamma0: f64 = r.iter().map(|x| x * x).sum();
    let mut total = gamma0;
    for lag in 1..=h {
        if lag >= n {
            break;
        }
        let mut g = 0.0;
        for i in lag..n {
            g += r[i] * r[i - lag];
        }
        let w = parzen(lag as f64 / (h as f64 + 1.0));
        total += 2.0 * w * g;
    }
    total.max(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_abs_diff_eq;

    #[test]
    fn realized_variance_sums_squared_log_returns() {
        let prices: [f64; 4] = [100.0, 101.0, 100.0, 102.0];
        let expected: f64 = prices.windows(2).map(|w| (w[1] / w[0]).ln().powi(2)).sum();
        assert_abs_diff_eq!(realized_variance(&prices), expected, epsilon = 1e-15);
    }

    #[test]
    fn realized_kernel_reduces_noise_bias() {
        let noisy: Vec<f64> = (0..200)
            .map(|i| 100.0 + if i % 2 == 0 { 0.05 } else { -0.05 })
            .collect();
        assert!(realized_kernel(&noisy, 10) < realized_variance(&noisy));
    }

    #[test]
    fn empty_or_single_price_is_zero() {
        assert_eq!(realized_variance(&[]), 0.0);
        assert_eq!(realized_variance(&[100.0]), 0.0);
    }
}
