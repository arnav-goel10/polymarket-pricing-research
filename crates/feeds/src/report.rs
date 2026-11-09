use alloy_primitives::{Bytes, FixedBytes, I256, U256};
use alloy_sol_types::SolValue;

/// Decoded Data Streams v3 report (prices already scaled out of 1e18).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct V3Report {
    pub feed_id: [u8; 32],
    pub valid_from_timestamp: u32,
    pub observations_timestamp: u32,
    pub expires_at: u32,
    pub price: f64,
    pub bid: f64,
    pub ask: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ReportFreshness {
    pub observation_age_millis: i64,
    pub expires_in_millis: i64,
    pub fresh: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("abi decode failed: {0}")]
    Abi(String),
}

const WAD: f64 = 1e18;

fn i256_to_f64_wad(x: I256) -> f64 {
    x.to_string().parse::<f64>().unwrap_or(0.0) / WAD
}

/// Decode a Data Streams `fullReport` blob into a v3 report.
///
/// `fullReport = abi.encode(bytes32[3] reportContext, bytes reportData,
///   bytes32[] rs, bytes32[] ss, bytes32 rawVs)`; `reportData` is the v3 schema.
pub fn decode_full_report(raw: &[u8]) -> Result<V3Report, DecodeError> {
    type Outer = (
        [FixedBytes<32>; 3],
        Bytes,
        Vec<FixedBytes<32>>,
        Vec<FixedBytes<32>>,
        FixedBytes<32>,
    );
    let (_ctx, report_data, _rs, _ss, _vs) = <Outer as SolValue>::abi_decode_params_validate(raw)
        .map_err(|e| DecodeError::Abi(e.to_string()))?;

    type Inner = (FixedBytes<32>, u32, u32, U256, U256, u32, I256, I256, I256);
    let (feed_id, valid_from, observations, _native, _link, expires, price, bid, ask) =
        <Inner as SolValue>::abi_decode_params_validate(report_data.as_ref())
            .map_err(|e| DecodeError::Abi(e.to_string()))?;

    Ok(V3Report {
        feed_id: feed_id.into(),
        valid_from_timestamp: valid_from,
        observations_timestamp: observations,
        expires_at: expires,
        price: i256_to_f64_wad(price),
        bid: i256_to_f64_wad(bid),
        ask: i256_to_f64_wad(ask),
    })
}

impl V3Report {
    pub fn freshness(&self, now_millis: i64, max_age_millis: i64) -> ReportFreshness {
        let observed_millis = self.observations_timestamp as i64 * 1000;
        let expires_millis = self.expires_at as i64 * 1000;
        let observation_age_millis = now_millis.saturating_sub(observed_millis);
        let expires_in_millis = expires_millis.saturating_sub(now_millis);
        ReportFreshness {
            observation_age_millis,
            expires_in_millis,
            fresh: observation_age_millis <= max_age_millis && expires_in_millis > 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Bytes, FixedBytes, I256, U256};
    use alloy_sol_types::SolValue;
    use approx::assert_abs_diff_eq;

    fn synthetic_full_report(price_1e18: i128) -> Vec<u8> {
        let report_data = (
            FixedBytes::<32>::from([0u8; 32]),
            1_700_000_000u32,
            1_700_000_300u32,
            U256::from(0u64),
            U256::from(0u64),
            1_700_000_600u32,
            I256::try_from(price_1e18).unwrap(),
            I256::try_from(price_1e18 - 1_000_000_000_000_000_000i128).unwrap(),
            I256::try_from(price_1e18 + 1_000_000_000_000_000_000i128).unwrap(),
        )
            .abi_encode_params();
        let ctx = [FixedBytes::<32>::from([0u8; 32]); 3];
        let rs: Vec<FixedBytes<32>> = vec![];
        let ss: Vec<FixedBytes<32>> = vec![];
        let raw_vs = FixedBytes::<32>::from([0u8; 32]);
        (ctx, Bytes::from(report_data), rs, ss, raw_vs).abi_encode_params()
    }

    #[test]
    fn decodes_price_from_full_report() {
        let raw = synthetic_full_report(60_123_500_000_000_000_000_000i128);
        let r = decode_full_report(&raw).unwrap();
        assert_abs_diff_eq!(r.price, 60_123.5, epsilon = 1e-6);
        assert_eq!(r.observations_timestamp, 1_700_000_300);
        assert_abs_diff_eq!(r.ask - r.bid, 2.0, epsilon = 1e-6);
    }

    #[test]
    fn rejects_garbage() {
        assert!(decode_full_report(&[0u8; 7]).is_err());
    }

    #[test]
    fn freshness_rejects_expired_report() {
        let r = V3Report {
            feed_id: [0; 32],
            valid_from_timestamp: 1_700_000_000,
            observations_timestamp: 1_700_000_001,
            expires_at: 1_700_000_002,
            price: 1.0,
            bid: 0.9,
            ask: 1.1,
        };
        let f = r.freshness(1_700_000_003_000, 5_000);
        assert!(!f.fresh);
    }
}
