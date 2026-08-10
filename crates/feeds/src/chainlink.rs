use crate::auth::{sign, Creds};
use crate::report::{decode_full_report, V3Report};
use serde_json::Value;

/// Pull the `report.fullReport` hex (0x-stripped) out of a Data Streams JSON body.
pub fn full_report_hex_from_json(body: &str) -> Option<String> {
    let v: Value = serde_json::from_str(body).ok()?;
    let raw = v.get("report")?.get("fullReport")?.as_str()?;
    Some(raw.trim_start_matches("0x").to_string())
}

fn now_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Fetch the latest decoded report for a feed over REST.
pub async fn fetch_latest(
    rest_base: &str,
    creds: &Creds,
    feed_id: &str,
) -> Result<V3Report, Box<dyn std::error::Error + Send + Sync>> {
    let path = format!("/api/v1/reports/latest?feedID={feed_id}");
    let ts = now_millis();
    let h = sign(creds, "GET", &path, b"", ts);
    let url = format!("{rest_base}{path}");
    let body = reqwest::Client::new()
        .get(&url)
        .header("Authorization", h.api_key)
        .header("X-Authorization-Timestamp", h.timestamp_millis.to_string())
        .header("X-Authorization-Signature-SHA256", h.signature)
        .send()
        .await?
        .text()
        .await?;
    let hex = full_report_hex_from_json(&body).ok_or("no report in response")?;
    let raw = hex::decode(hex)?;
    Ok(decode_full_report(&raw)?)
}

/// Fetch the decoded report for a feed AT a specific Unix timestamp (seconds) —
/// the exact report the Polymarket settlement reads at a window's open/close.
/// Endpoint: `/api/v1/reports?feedID=<id>&timestamp=<unix_seconds>`. This is what
/// makes our S0 / settlement byte-identical to Polymarket's (requires Data Streams
/// credentials; the RTDS relay is only an approximation).
pub async fn fetch_report_at(
    rest_base: &str,
    creds: &Creds,
    feed_id: &str,
    timestamp_secs: i64,
) -> Result<V3Report, Box<dyn std::error::Error + Send + Sync>> {
    let path = format!("/api/v1/reports?feedID={feed_id}&timestamp={timestamp_secs}");
    let ts = now_millis();
    let h = sign(creds, "GET", &path, b"", ts);
    let url = format!("{rest_base}{path}");
    let body = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(8))
        .build()?
        .get(&url)
        .header("Authorization", h.api_key)
        .header("X-Authorization-Timestamp", h.timestamp_millis.to_string())
        .header("X-Authorization-Signature-SHA256", h.signature)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    let hex = full_report_hex_from_json(&body).ok_or("no report in response")?;
    let raw = hex::decode(hex)?;
    Ok(decode_full_report(&raw)?)
}

pub async fn fetch_latest_fresh(
    rest_base: &str,
    creds: &Creds,
    feed_id: &str,
    now_millis: i64,
    max_age_millis: i64,
) -> Result<V3Report, Box<dyn std::error::Error + Send + Sync>> {
    let report = fetch_latest(rest_base, creds, feed_id).await?;
    let freshness = report.freshness(now_millis, max_age_millis);
    if freshness.fresh {
        Ok(report)
    } else {
        Err(format!(
            "stale Chainlink report: age={}ms expires_in={}ms",
            freshness.observation_age_millis, freshness.expires_in_millis
        )
        .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_full_report_hex_from_rest_json() {
        let body = r#"{"report":{"feedID":"0xabc","fullReport":"0x1234","validFromTimestamp":1,"observationsTimestamp":2}}"#;
        assert_eq!(full_report_hex_from_json(body).unwrap(), "1234");
    }

    #[test]
    fn missing_report_is_none() {
        assert!(full_report_hex_from_json("{}").is_none());
    }
}
