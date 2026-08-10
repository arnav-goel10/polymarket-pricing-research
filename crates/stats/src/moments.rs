fn mean(xs: &[f64]) -> f64 {
    xs.iter().sum::<f64>() / xs.len() as f64
}

/// Sample skewness of log returns. Returns 0 for < 3 points.
pub fn realized_skewness(r: &[f64]) -> f64 {
    let n = r.len();
    if n < 3 {
        return 0.0;
    }
    let m = mean(r);
    let var = r.iter().map(|x| (x - m).powi(2)).sum::<f64>() / n as f64;
    if var <= 0.0 {
        return 0.0;
    }
    let m3 = r.iter().map(|x| (x - m).powi(3)).sum::<f64>() / n as f64;
    m3 / var.powf(1.5)
}

/// Sample kurtosis (not excess) of log returns. Returns 3.0 for < 4 points.
pub fn realized_kurtosis(r: &[f64]) -> f64 {
    let n = r.len();
    if n < 4 {
        return 3.0;
    }
    let m = mean(r);
    let var = r.iter().map(|x| (x - m).powi(2)).sum::<f64>() / n as f64;
    if var <= 0.0 {
        return 3.0;
    }
    let m4 = r.iter().map(|x| (x - m).powi(4)).sum::<f64>() / n as f64;
    m4 / (var * var)
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_abs_diff_eq;

    #[test]
    fn symmetric_returns_have_zero_skew() {
        assert_abs_diff_eq!(
            realized_skewness(&[-0.02, -0.01, 0.0, 0.01, 0.02]),
            0.0,
            epsilon = 1e-12
        );
    }

    #[test]
    fn light_tailed_sample_kurtosis_below_three() {
        let k = realized_kurtosis(&[-0.02, -0.01, 0.0, 0.01, 0.02]);
        assert!(k > 1.0 && k < 3.0, "k={k}");
    }

    #[test]
    fn left_skew_is_negative() {
        assert!(realized_skewness(&[-0.10, 0.005, 0.006, 0.004, 0.005]) < 0.0);
    }

    #[test]
    fn too_few_points_are_safe_defaults() {
        assert_eq!(realized_skewness(&[0.01]), 0.0);
        assert_eq!(realized_kurtosis(&[0.01]), 3.0);
    }
}
