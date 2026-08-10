use stats::{realized_kurtosis, realized_skewness};

pub const SECONDS_PER_YEAR: f64 = 365.0 * 24.0 * 60.0 * 60.0;

/// Rolling buffer of (timestamp_millis, price) for one source.
#[derive(Debug, Default, Clone)]
pub struct PriceBuffer {
    ts: std::collections::VecDeque<i64>,
    px: std::collections::VecDeque<f64>,
    cap: usize,
    /// Cached log-return series — invalidated on every `push()` so callers
    /// never compute it more than once per push regardless of how many methods
    /// (ewma_vol, skew, kurt, lead-lag) consume it in the same tick.
    /// `RefCell` gives interior mutability so the cache can be filled through
    /// a shared `&self` reference without changing the public API.
    returns_cache: std::cell::RefCell<Option<Vec<f64>>>,
}

impl PriceBuffer {
    pub fn new(cap: usize) -> Self {
        PriceBuffer {
            ts: Default::default(),
            px: Default::default(),
            cap: cap.max(2),
            returns_cache: std::cell::RefCell::new(None),
        }
    }

    pub fn push(&mut self, ts_millis: i64, price: f64) {
        if self.px.len() == self.cap {
            self.ts.pop_front();
            self.px.pop_front();
        }
        self.ts.push_back(ts_millis);
        self.px.push_back(price);
        *self.returns_cache.borrow_mut() = None; // invalidate on every new sample
    }

    pub fn last(&self) -> Option<f64> {
        self.px.back().copied()
    }

    pub fn last_ts_millis(&self) -> Option<i64> {
        self.ts.back().copied()
    }

    /// Price of the buffered tick whose timestamp is closest to `target_millis`.
    /// Used to anchor S0 to the true window open instead of "whatever price the
    /// loop happened to read when it noticed the boundary".
    pub fn price_at(&self, target_millis: i64) -> Option<f64> {
        let mut best: Option<(i64, f64)> = None;
        for (ts, px) in self.ts.iter().zip(self.px.iter()) {
            let d = (ts - target_millis).abs();
            if best.is_none_or(|(bd, _)| d < bd) {
                best = Some((d, *px));
            }
        }
        best.map(|(_, px)| px)
    }

    /// Price of the latest buffered tick at or before `target_millis` — the
    /// "last known value as of that instant". This is what an oracle settlement
    /// reads (it cannot use a report from after the timestamp), so it is more
    /// faithful for anchoring S0 / the close than `price_at` (nearest by distance,
    /// which can pick a tick just AFTER the boundary and fold in a post-open move).
    /// Falls back to the earliest tick if every tick is after the target.
    pub fn price_at_or_before(&self, target_millis: i64) -> Option<f64> {
        let mut best: Option<(i64, f64)> = None;
        for (ts, px) in self.ts.iter().zip(self.px.iter()) {
            if *ts <= target_millis && best.is_none_or(|(bt, _)| *ts > bt) {
                best = Some((*ts, *px));
            }
        }
        best.map(|(_, px)| px).or_else(|| self.px.front().copied())
    }

    pub fn len(&self) -> usize {
        self.px.len()
    }

    pub fn is_empty(&self) -> bool {
        self.px.is_empty()
    }

    /// Log returns between consecutive samples. Result is cached until the next
    /// `push()` so all callers in a single tick share a single computation.
    /// Uses `RefCell` interior mutability so `&self` callers get the cached slice.
    pub fn returns(&self) -> std::cell::Ref<'_, [f64]> {
        {
            let mut cache = self.returns_cache.borrow_mut();
            if cache.is_none() {
                *cache = Some(
                    self.px
                        .iter()
                        .zip(self.px.iter().skip(1))
                        .map(|(a, b)| (b / a).ln())
                        .collect(),
                );
            }
        }
        std::cell::Ref::map(self.returns_cache.borrow(), |opt| {
            opt.as_deref().unwrap_or(&[])
        })
    }

    /// Median inter-sample spacing in seconds (defaults to 1.0 if undeterminable).
    fn dt_secs(&self) -> f64 {
        if self.ts.len() < 2 {
            return 1.0;
        }
        let mut diffs: Vec<f64> = self
            .ts
            .iter()
            .zip(self.ts.iter().skip(1))
            .map(|(a, b)| (b - a) as f64 / 1000.0)
            .filter(|d| *d > 0.0)
            .collect();
        if diffs.is_empty() {
            return 1.0;
        }
        diffs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        diffs[diffs.len() / 2]
    }

    /// Annualized volatility from the buffered returns.
    pub fn annualized_vol(&self) -> f64 {
        let r = self.returns();
        let r: &[f64] = &r;
        if r.is_empty() {
            return 0.0;
        }
        let mean_r2 = r.iter().map(|x| x * x).sum::<f64>() / r.len() as f64;
        let per_sec_var = mean_r2 / self.dt_secs();
        (per_sec_var * SECONDS_PER_YEAR).sqrt()
    }

    /// Annualized EWMA volatility (RiskMetrics): an exponentially-weighted estimate
    /// of return variance, `σ²_t = λ·σ²_{t-1} + (1-λ)·r²_t`. Unlike the equal-weight
    /// estimators it reacts to the CURRENT vol regime (recent moves weighted more) —
    /// which is what matters for pricing the next few minutes. `lambda` in [0,1):
    /// higher = longer memory.
    pub fn annualized_ewma_vol(&self, lambda: f64) -> f64 {
        let r = self.returns();
        let r: &[f64] = &r;
        if r.is_empty() {
            return 0.0;
        }
        let lambda = lambda.clamp(0.0, 0.999);
        let mut var = r[0] * r[0];
        for x in &r[1..] {
            var = lambda * var + (1.0 - lambda) * (x * x);
        }
        let per_sec_var = var / self.dt_secs();
        (per_sec_var * SECONDS_PER_YEAR).sqrt()
    }

    pub fn annualized_kernel_vol(&self, bandwidth: usize) -> f64 {
        if self.px.len() < 2 {
            return 0.0;
        }
        let prices: Vec<f64> = self.px.iter().copied().collect();
        let rv = stats::realized_kernel(&prices, bandwidth);
        let elapsed_secs = match (self.ts.front(), self.ts.back()) {
            (Some(a), Some(b)) if b > a => (*b - *a) as f64 / 1000.0,
            _ => self.dt_secs() * (self.px.len().saturating_sub(1) as f64),
        }
        .max(1.0);
        (rv / elapsed_secs * SECONDS_PER_YEAR).sqrt()
    }

    pub fn skew(&self) -> f64 {
        realized_skewness(&self.returns())
    }

    pub fn kurt(&self) -> f64 {
        realized_kurtosis(&self.returns())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_abs_diff_eq;

    #[test]
    fn price_at_finds_closest_tick_to_target() {
        let mut b = PriceBuffer::new(16);
        b.push(1_000, 100.0);
        b.push(2_000, 101.0);
        b.push(3_000, 102.0);
        // closest to 2_100ms is the 2_000ms tick (101.0)
        assert_eq!(b.price_at(2_100), Some(101.0));
        // closest to 2_900ms is the 3_000ms tick (102.0)
        assert_eq!(b.price_at(2_900), Some(102.0));
        assert_eq!(PriceBuffer::new(4).price_at(0), None);
    }

    #[test]
    fn price_at_or_before_uses_last_known_value() {
        let mut b = PriceBuffer::new(16);
        b.push(1_000, 100.0);
        b.push(2_000, 101.0);
        b.push(3_000, 102.0);
        // as of 2_500ms the last known is the 2_000ms tick (101.0), NOT the nearer
        // 3_000ms tick (which would fold in a future move).
        assert_eq!(b.price_at_or_before(2_500), Some(101.0));
        assert_eq!(b.price_at_or_before(3_000), Some(102.0)); // exact ts included
        assert_eq!(b.price_at_or_before(9_999), Some(102.0)); // latest
                                                              // before the first tick -> fall back to the earliest known
        assert_eq!(b.price_at_or_before(500), Some(100.0));
        assert_eq!(PriceBuffer::new(4).price_at_or_before(0), None);
    }

    #[test]
    fn constant_price_has_zero_vol() {
        let mut b = PriceBuffer::new(100);
        for i in 0..10 {
            b.push(1_000 * i, 60_000.0);
        }
        assert_abs_diff_eq!(b.annualized_vol(), 0.0, epsilon = 1e-12);
    }

    #[test]
    fn ewma_vol_reacts_to_recent_regime() {
        // Calm early, volatile late: EWMA (recency-weighted) should read HIGHER vol
        // than the equal-weight estimate, which dilutes the recent burst.
        let mut b = PriceBuffer::new(200);
        let mut p = 60_000.0;
        for i in 0..80 {
            let r: f64 = if i < 60 { 0.00002 } else { 0.0004 }; // quiet then loud
            p *= (if i % 2 == 0 { r } else { -r }).exp();
            b.push(1_000 * i, p);
        }
        let ewma = b.annualized_ewma_vol(0.97);
        let equal = b.annualized_vol();
        assert!(ewma.is_finite() && ewma > 0.0);
        assert!(
            ewma > equal,
            "EWMA {ewma} should exceed equal-weight {equal} when vol just rose"
        );
    }

    #[test]
    fn ewma_vol_zero_for_constant_price() {
        let mut b = PriceBuffer::new(50);
        for i in 0..20 {
            b.push(1_000 * i, 60_000.0);
        }
        assert_abs_diff_eq!(b.annualized_ewma_vol(0.97), 0.0, epsilon = 1e-12);
    }

    #[test]
    fn capacity_is_enforced() {
        let mut b = PriceBuffer::new(3);
        for i in 0..10 {
            b.push(i, 100.0 + i as f64);
        }
        assert_eq!(b.len(), 3);
        assert_eq!(b.last(), Some(109.0));
    }

    #[test]
    fn annualized_vol_matches_analytic_for_known_step() {
        // 1s samples, each return ~ +/- r0; mean_r2 = r0^2; sigma = r0*sqrt(SPY).
        let mut b = PriceBuffer::new(100);
        let r0 = 0.0001_f64;
        let mut p = 60_000.0;
        for i in 0..50 {
            b.push(1_000 * i, p);
            p *= (if i % 2 == 0 { r0 } else { -r0 }).exp();
        }
        let expected = r0 * SECONDS_PER_YEAR.sqrt();
        assert_abs_diff_eq!(b.annualized_vol(), expected, epsilon = expected * 0.05);
    }

    #[test]
    fn kernel_vol_is_finite() {
        let mut b = PriceBuffer::new(16);
        for i in 0..16 {
            b.push(1_000 * i, 100.0 + i as f64);
        }
        assert!(b.annualized_kernel_vol(3).is_finite());
    }

    #[test]
    fn skew_kurt_default_safely_when_short() {
        let b = PriceBuffer::new(10);
        assert_eq!(b.skew(), 0.0);
        assert_eq!(b.kurt(), 3.0);
    }
}
