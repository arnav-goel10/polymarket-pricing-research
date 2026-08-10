use crate::report::V3Report;
use crate::tick::{FeedSource, Tick};

/// Convert a decoded Chainlink v3 report into a normalized tick (uses the
/// consensus median price and the observations timestamp).
pub fn tick_from_report(r: &V3Report) -> Tick {
    Tick {
        source: FeedSource::Chainlink,
        price: r.price,
        ts_millis: r.observations_timestamp as i64 * 1000,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report::V3Report;
    use crate::FeedSource;
    use approx::assert_abs_diff_eq;

    #[test]
    fn v3_report_becomes_chainlink_tick() {
        let r = V3Report {
            feed_id: [0u8; 32],
            valid_from_timestamp: 1,
            observations_timestamp: 1_700_000_300,
            expires_at: 2,
            price: 60_000.0,
            bid: 59_999.0,
            ask: 60_001.0,
        };
        let t = tick_from_report(&r);
        assert_eq!(t.source, FeedSource::Chainlink);
        assert_abs_diff_eq!(t.price, 60_000.0, epsilon = 1e-9);
        assert_eq!(t.ts_millis, 1_700_000_300_000);
    }
}
