pub const SECONDS_PER_YEAR: f64 = 365.0 * 24.0 * 60.0 * 60.0;

/// One 5-minute up/down window: reference price S0 frozen at open, fixed length.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Window {
    pub s0: f64,
    pub t0_millis: i64,
    pub length_secs: i64,
}

impl Window {
    pub fn open(s0: f64, t0_millis: i64, length_secs: i64) -> Self {
        Window {
            s0,
            t0_millis,
            length_secs,
        }
    }

    pub fn close_millis(&self) -> i64 {
        self.t0_millis + self.length_secs * 1000
    }

    /// Whole seconds remaining until close, clamped at 0.
    pub fn seconds_remaining(&self, now_millis: i64) -> i64 {
        ((self.close_millis() - now_millis) / 1000).max(0)
    }

    /// Remaining time to expiry in years, floored at 0.
    pub fn tau_years(&self, now_millis: i64) -> f64 {
        let secs = (self.close_millis() - now_millis) as f64 / 1000.0;
        secs.max(0.0) / SECONDS_PER_YEAR
    }

    pub fn is_expired(&self, now_millis: i64) -> bool {
        now_millis >= self.close_millis()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_abs_diff_eq;

    #[test]
    fn open_fixes_reference_and_full_tau() {
        let w = Window::open(60_000.0, 1_700_000_000_000, 300);
        assert_eq!(w.s0, 60_000.0);
        assert_abs_diff_eq!(
            w.tau_years(1_700_000_000_000),
            300.0 / SECONDS_PER_YEAR,
            epsilon = 1e-15
        );
    }

    #[test]
    fn tau_shrinks_and_floors_at_zero() {
        let w = Window::open(60_000.0, 1_700_000_000_000, 300);
        assert_abs_diff_eq!(
            w.tau_years(1_700_000_150_000),
            150.0 / SECONDS_PER_YEAR,
            epsilon = 1e-12
        );
        assert_eq!(w.tau_years(1_700_000_999_000), 0.0);
    }

    #[test]
    fn seconds_remaining_is_clamped() {
        let w = Window::open(60_000.0, 1_700_000_000_000, 300);
        assert_eq!(w.seconds_remaining(1_700_000_290_000), 10);
        assert_eq!(w.seconds_remaining(1_700_001_000_000), 0);
    }

    #[test]
    fn is_expired_after_close() {
        let w = Window::open(60_000.0, 1_700_000_000_000, 300);
        assert!(!w.is_expired(1_700_000_299_000));
        assert!(w.is_expired(1_700_000_300_000));
    }
}
