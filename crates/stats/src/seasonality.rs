/// Intraday volatility seasonality: a multiplier on forecast vol by minute of
/// the UTC day (0..1440), normalized so the daily mean multiplier is 1.0.
pub struct Seasonality {
    multipliers: Vec<f64>,
}

impl Seasonality {
    pub fn flat() -> Self {
        Seasonality {
            multipliers: vec![1.0; 1440],
        }
    }

    /// Build from per-minute average variance buckets (length 1440).
    pub fn from_minute_buckets(buckets: Vec<f64>) -> Self {
        assert_eq!(buckets.len(), 1440, "need 1440 minute buckets");
        let mean: f64 = buckets.iter().sum::<f64>() / 1440.0;
        let multipliers = if mean > 0.0 {
            buckets.iter().map(|b| b / mean).collect()
        } else {
            vec![1.0; 1440]
        };
        Seasonality { multipliers }
    }

    /// Volatility multiplier for the given UTC minute-of-day (wraps mod 1440).
    pub fn multiplier(&self, minute_of_day: usize) -> f64 {
        self.multipliers[minute_of_day % 1440]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_abs_diff_eq;

    #[test]
    fn flat_table_returns_unit_multiplier() {
        let s = Seasonality::flat();
        assert_abs_diff_eq!(s.multiplier(0), 1.0, epsilon = 1e-12);
        assert_abs_diff_eq!(s.multiplier(13 * 60 + 30), 1.0, epsilon = 1e-12);
    }

    #[test]
    fn multiplier_reflects_relative_bucket_activity() {
        let mut buckets = vec![1.0; 1440];
        buckets[810] = 2.0;
        let s = Seasonality::from_minute_buckets(buckets);
        assert!(s.multiplier(810) > 1.9);
        assert!(s.multiplier(0) < 1.01);
    }

    #[test]
    fn out_of_range_minute_wraps() {
        let s = Seasonality::flat();
        assert_abs_diff_eq!(s.multiplier(1440), s.multiplier(0), epsilon = 1e-12);
    }
}
