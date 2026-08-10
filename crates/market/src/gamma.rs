use serde_json::Value;
use std::sync::OnceLock;

/// A discovered Polymarket binary market (Up/Down).
#[derive(Debug, Clone, PartialEq)]
pub struct DiscoveredMarket {
    pub slug: String,
    pub condition_id: String,
    pub yes_token_id: String,
    pub no_token_id: String,
    pub active: bool,
    pub closed: bool,
    pub start_date: String,
    pub end_date: String,
}

const GAMMA_BASE: &str = "https://gamma-api.polymarket.com";
const USER_AGENT: &str = "poly-shadow-engine/0.1";

/// Shared HTTP client — built once, reused across all Gamma polls so connections
/// are kept alive and TLS is not re-negotiated on every 750ms book poll.
static GAMMA_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

fn gamma_client() -> &'static reqwest::Client {
    GAMMA_CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("gamma reqwest client")
    })
}

fn string_array(value: Option<&Value>) -> Vec<String> {
    match value {
        Some(Value::String(s)) => serde_json::from_str(s).unwrap_or_default(),
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|v| v.as_str().map(ToString::to_string))
            .collect(),
        _ => Vec::new(),
    }
}

fn up_down_token_ids(token_ids: &[String], outcomes: &[String]) -> Option<(String, String)> {
    if token_ids.len() < 2 {
        return None;
    }
    let mut up = None;
    let mut down = None;
    for (idx, outcome) in outcomes.iter().enumerate() {
        let normalized = outcome.to_lowercase();
        if normalized == "up" || normalized == "yes" {
            up = token_ids.get(idx).cloned();
        } else if normalized == "down" || normalized == "no" {
            down = token_ids.get(idx).cloned();
        }
    }
    Some((
        up.unwrap_or_else(|| token_ids[0].clone()),
        down.unwrap_or_else(|| token_ids[1].clone()),
    ))
}

/// Parse a Gamma `/markets` JSON array. `clobTokenIds`/`outcomes` are
/// JSON-encoded strings inside the JSON (Gamma quirk), decoded here.
pub fn parse_markets(body: &str) -> Result<Vec<DiscoveredMarket>, serde_json::Error> {
    let arr: Vec<Value> = serde_json::from_str(body)?;
    let mut out = Vec::new();
    for m in arr {
        let token_ids = string_array(m.get("clobTokenIds"));
        let outcomes = string_array(m.get("outcomes"));
        let Some((yes_token_id, no_token_id)) = up_down_token_ids(&token_ids, &outcomes) else {
            continue;
        };
        out.push(DiscoveredMarket {
            slug: m
                .get("slug")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            condition_id: m
                .get("conditionId")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            yes_token_id,
            no_token_id,
            active: m.get("active").and_then(|v| v.as_bool()).unwrap_or(false),
            closed: m.get("closed").and_then(|v| v.as_bool()).unwrap_or(true),
            start_date: m
                .get("startDate")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            end_date: m
                .get("endDate")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
        });
    }
    Ok(out)
}

async fn fetch_markets_url(
    url: &str,
) -> Result<Vec<DiscoveredMarket>, Box<dyn std::error::Error + Send + Sync>> {
    let body = gamma_client()
        .get(url)
        .header(reqwest::header::USER_AGENT, USER_AGENT)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    Ok(parse_markets(&body)?)
}

/// Fetch markets by slug (real-time client).
pub async fn fetch_market_by_slug(
    slug: &str,
) -> Result<Vec<DiscoveredMarket>, Box<dyn std::error::Error + Send + Sync>> {
    let url = format!("{GAMMA_BASE}/markets?slug={slug}");
    fetch_markets_url(&url).await
}

/// Fetch the deterministic BTC up/down market for a window start timestamp.
pub async fn fetch_btc_updown_by_start(
    t0_seconds: i64,
) -> Result<Option<DiscoveredMarket>, Box<dyn std::error::Error + Send + Sync>> {
    let slug = format!("btc-updown-5m-{t0_seconds}");
    Ok(fetch_market_by_slug(&slug).await?.into_iter().next())
}

/// Polymarket's duration label for a window length, e.g. 300s -> "5m".
pub fn window_label(window_secs: i64) -> String {
    let mins = (window_secs / 60).max(1);
    format!("{mins}m")
}

/// Fetch the current BTC up/down market by its deterministic slug, with the
/// duration label derived from `window_secs` (300s -> "5m", 900s -> "15m").
pub async fn fetch_current_btc_updown(
    now_millis: i64,
    window_secs: i64,
) -> Result<Option<DiscoveredMarket>, Box<dyn std::error::Error + Send + Sync>> {
    let window_secs = window_secs.max(1);
    let now_seconds = now_millis / 1000;
    let t0_seconds = now_seconds - now_seconds.rem_euclid(window_secs);
    let slug = format!("btc-updown-{}-{}", window_label(window_secs), t0_seconds);
    Ok(fetch_market_by_slug(&slug).await?.into_iter().next())
}

/// Keep only active, open BTC up/down markets (slug contains "btc" and
/// "up"/"down"). Pure so it is unit-testable.
pub fn filter_btc_updown(markets: Vec<DiscoveredMarket>) -> Vec<DiscoveredMarket> {
    markets
        .into_iter()
        .filter(|m| {
            let slug = m.slug.to_lowercase();
            m.active
                && !m.closed
                && slug.contains("btc")
                && (slug.contains("up") || slug.contains("down"))
        })
        .collect()
}

/// Fetch active, open BTC up/down markets (real-time client).
pub async fn fetch_active_btc_updown(
) -> Result<Vec<DiscoveredMarket>, Box<dyn std::error::Error + Send + Sync>> {
    let url = format!("{GAMMA_BASE}/markets?active=true&closed=false&limit=500");
    Ok(filter_btc_updown(fetch_markets_url(&url).await?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_market_token_ids_and_times() {
        let body = r#"[{"slug":"btc-updown-5m-x","conditionId":"0xcond","active":true,"closed":false,
            "clobTokenIds":"[\"111\",\"222\"]","outcomes":"[\"Up\",\"Down\"]",
            "startDate":"2026-06-21T00:00:00Z","endDate":"2026-06-21T00:05:00Z"}]"#;
        let m = parse_markets(body).unwrap();
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].yes_token_id, "111");
        assert_eq!(m[0].no_token_id, "222");
        assert!(m[0].active && !m[0].closed);
    }

    #[test]
    fn maps_up_down_tokens_from_outcomes() {
        let body = r#"[{"slug":"btc-updown-5m-x","conditionId":"0xcond","active":true,"closed":false,
            "clobTokenIds":"[\"down-token\",\"up-token\"]","outcomes":"[\"Down\",\"Up\"]",
            "startDate":"2026-06-21T00:00:00Z","endDate":"2026-06-21T00:05:00Z"}]"#;
        let m = parse_markets(body).unwrap();
        assert_eq!(m[0].yes_token_id, "up-token");
        assert_eq!(m[0].no_token_id, "down-token");
    }

    #[test]
    fn empty_array_is_empty() {
        assert!(parse_markets("[]").unwrap().is_empty());
    }

    #[test]
    fn filter_keeps_active_btc_updown_only() {
        let mk = |slug: &str, active: bool, closed: bool| DiscoveredMarket {
            slug: slug.into(),
            condition_id: "0x".into(),
            yes_token_id: "1".into(),
            no_token_id: "2".into(),
            active,
            closed,
            start_date: String::new(),
            end_date: String::new(),
        };
        let out = filter_btc_updown(vec![
            mk("btc-updown-5m-abc", true, false),
            mk("eth-updown-5m", true, false),    // not btc
            mk("btc-updown-5m-old", true, true), // closed
            mk("btc-hourly-vol", true, false),   // no up/down
        ]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].slug, "btc-updown-5m-abc");
    }
}
