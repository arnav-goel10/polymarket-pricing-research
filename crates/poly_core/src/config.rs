use serde::{Deserialize, Serialize};
use std::env;
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdaptiveEdgeMode {
    #[default]
    RobustQuantile,
    Conformal,
}

impl FromStr for AdaptiveEdgeMode {
    type Err = ConfigError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "robust" | "robust_quantile" | "robust-quantile" | "legacy" => {
                Ok(AdaptiveEdgeMode::RobustQuantile)
            }
            "conformal" | "weighted_conformal" | "weighted-conformal" | "sota" => {
                Ok(AdaptiveEdgeMode::Conformal)
            }
            other => Err(ConfigError::InvalidValue {
                key: "ADAPTIVE_EDGE_MODE".into(),
                value: other.into(),
                reason: "expected robust_quantile or conformal".into(),
            }),
        }
    }
}

impl fmt::Display for AdaptiveEdgeMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AdaptiveEdgeMode::RobustQuantile => f.write_str("robust_quantile"),
            AdaptiveEdgeMode::Conformal => f.write_str("conformal"),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunMode {
    #[default]
    Shadow,
    DryRun,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum DryRunStrategy {
    #[default]
    HoldSharp,
    HoldCalibrated,
    Blend,
}

impl DryRunStrategy {
    pub fn as_str(self) -> &'static str {
        match self {
            DryRunStrategy::HoldSharp => "hold_sharp",
            DryRunStrategy::HoldCalibrated => "hold_calibrated",
            DryRunStrategy::Blend => "blend",
        }
    }
}

impl FromStr for DryRunStrategy {
    type Err = ConfigError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "hold_sharp" | "hold-sharp" | "sharp" => Ok(DryRunStrategy::HoldSharp),
            "hold_calibrated" | "hold-calibrated" | "calibrated" => {
                Ok(DryRunStrategy::HoldCalibrated)
            }
            "blend" => Ok(DryRunStrategy::Blend),
            other => Err(ConfigError::InvalidValue {
                key: "DRY_RUN_STRATEGY".into(),
                value: other.into(),
                reason: "expected hold_sharp, hold_calibrated, or blend".into(),
            }),
        }
    }
}

impl fmt::Display for DryRunStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl RunMode {
    pub fn as_str(self) -> &'static str {
        match self {
            RunMode::Shadow => "Shadow",
            RunMode::DryRun => "DryRun",
        }
    }
}

impl FromStr for RunMode {
    type Err = ConfigError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "shadow" => Ok(RunMode::Shadow),
            "dry-run" | "dry_run" | "dryrun" => Ok(RunMode::DryRun),
            other => Err(ConfigError::InvalidValue {
                key: "mode".into(),
                value: other.into(),
                reason: "expected shadow or dry-run".into(),
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub mode: RunMode,
    pub bind_addr: String,
    #[serde(skip_serializing, skip_deserializing)]
    pub control_token: Option<String>,
    pub window_secs: i64,
    pub decision_tick_millis: u64,
    pub execution: ExecutionConfig,
    pub feeds: FeedConfig,
    pub risk: RiskConfig,
    pub models: ModelConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionConfig {
    pub token_id_override: Option<String>,
    pub persist_fills_path: String,
    pub dry_run_strategy: DryRunStrategy,
    pub latency_edge_buffer: f64,
    pub min_profit_buffer: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeedConfig {
    pub use_chainlink_streams: bool,
    pub allow_rtds_shadow_fallback: bool,
    pub chainlink_feed_id: Option<String>,
    pub max_feed_age_millis: i64,
    pub max_book_age_millis: i64,
    pub rtds_url: String,
    pub binance_url: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct RiskConfig {
    pub min_edge: f64,
    pub max_stake_per_window: f64,
    pub max_open_exposure: f64,
    pub max_daily_loss: f64,
    pub no_trade_last_secs: i64,
    pub max_model_disagreement: f64,
    pub max_decision_rate_per_minute: u32,
    pub max_market_loss: f64,
    pub max_daily_notional: f64,
    pub min_depth: f64,
    pub max_spread: f64,
    /// Notional per trade in USD (shares = usd / price), instead of fixed shares.
    pub trade_size_usd: f64,
    /// Adaptive edge: act when edge >= fraction * (learned typical max edge),
    /// clamped to [min_edge, adaptive_edge_ceil]. `min_edge` is the floor.
    pub adaptive_edge: bool,
    pub adaptive_edge_fraction: f64,
    pub adaptive_edge_ceil: f64,
    pub adaptive_edge_lambda: f64,
    pub adaptive_edge_quantile: f64,
    pub adaptive_edge_window: usize,
    pub adaptive_edge_mode: AdaptiveEdgeMode,
    pub adaptive_edge_instability_buffer: f64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ModelConfig {
    pub enable_cos_vg: bool,
    pub min_calibration_samples: usize,
    pub calibration_beta: f64,
    pub max_alpha_tilt_abs: f64,
    pub enable_alpha_tilt_dry_run: bool,
    pub sharpness: f64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    InvalidValue {
        key: String,
        value: String,
        reason: String,
    },
    UnsafeConfig(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::InvalidValue { key, value, reason } => {
                write!(f, "invalid {key}={value:?}: {reason}")
            }
            ConfigError::UnsafeConfig(msg) => write!(f, "unsafe configuration: {msg}"),
        }
    }
}

impl std::error::Error for ConfigError {}

impl AppConfig {
    pub fn from_env_args<I, S>(args: I) -> Result<Self, ConfigError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let args: Vec<String> = args.into_iter().map(Into::into).collect();
        let mode = mode_from_args_env(&args)?;
        let cfg = AppConfig {
            mode,
            bind_addr: env_string("BIND_ADDR", "127.0.0.1:8080"),
            control_token: env::var("CONTROL_TOKEN")
                .ok()
                .filter(|token| !token.trim().is_empty()),
            window_secs: env_parse("WINDOW_SECS", 300)?,
            decision_tick_millis: env_parse("DECISION_TICK_MILLIS", default_tick_millis(mode))?,
            execution: ExecutionConfig {
                token_id_override: env::var("POLY_TOKEN_ID").ok(),
                persist_fills_path: env_string("PERSIST_FILLS_PATH", "fills.jsonl"),
                dry_run_strategy: env_parse("DRY_RUN_STRATEGY", DryRunStrategy::HoldSharp)?,
                latency_edge_buffer: env_parse("LATENCY_EDGE_BUFFER", 0.02)?,
                min_profit_buffer: env_parse("MIN_PROFIT_BUFFER", 0.10)?,
            },
            feeds: FeedConfig {
                use_chainlink_streams: env_bool("USE_CHAINLINK_STREAMS", false)?,
                allow_rtds_shadow_fallback: env_bool("ALLOW_RTDS_SHADOW_FALLBACK", true)?,
                chainlink_feed_id: env::var("BTC_FEED_ID").ok(),
                max_feed_age_millis: env_parse("MAX_FEED_AGE_MILLIS", 5_000)?,
                max_book_age_millis: env_parse("MAX_BOOK_AGE_MILLIS", 3_000)?,
                rtds_url: env_string("RTDS_URL", "wss://ws-live-data.polymarket.com"),
                binance_url: env_string(
                    "BINANCE_URL",
                    "wss://fstream.binance.com/stream?streams=btcusdt@aggTrade/btcusdt@bookTicker",
                ),
            },
            risk: RiskConfig {
                min_edge: env_parse("MIN_EDGE", 0.05)?,
                max_stake_per_window: env_parse("MAX_STAKE_PER_WINDOW", 100.0)?,
                max_open_exposure: env_parse("MAX_OPEN_EXPOSURE", 1_000.0)?,
                max_daily_loss: env_parse("MAX_DAILY_LOSS", 200.0)?,
                no_trade_last_secs: env_parse("NO_TRADE_LAST_SECS", 20)?,
                max_model_disagreement: env_parse("MAX_MODEL_DISAGREEMENT", 0.20)?,
                max_decision_rate_per_minute: env_parse("MAX_DECISION_RATE_PER_MINUTE", 6)?,
                max_market_loss: env_parse("MAX_MARKET_LOSS", 200.0)?,
                max_daily_notional: env_parse("MAX_DAILY_NOTIONAL", 1_000.0)?,
                min_depth: env_parse("MIN_DEPTH", 10.0)?,
                max_spread: env_parse("MAX_SPREAD", 0.08)?,
                trade_size_usd: env_parse("TRADE_SIZE_USD", 2.0)?,
                adaptive_edge: env_bool("ADAPTIVE_EDGE", true)?,
                adaptive_edge_fraction: env_parse("ADAPTIVE_EDGE_FRACTION", 0.5)?,
                adaptive_edge_ceil: env_parse("ADAPTIVE_EDGE_CEIL", 0.20)?,
                adaptive_edge_lambda: env_parse("ADAPTIVE_EDGE_LAMBDA", 0.9)?,
                adaptive_edge_quantile: env_parse("ADAPTIVE_EDGE_QUANTILE", 0.75)?,
                adaptive_edge_window: env_parse("ADAPTIVE_EDGE_WINDOW", 96)?,
                adaptive_edge_mode: env_parse(
                    "ADAPTIVE_EDGE_MODE",
                    AdaptiveEdgeMode::RobustQuantile,
                )?,
                adaptive_edge_instability_buffer: env_parse(
                    "ADAPTIVE_EDGE_INSTABILITY_BUFFER",
                    0.25,
                )?,
            },
            models: ModelConfig {
                enable_cos_vg: env_bool("ENABLE_COS_VG", true)?,
                min_calibration_samples: env_parse("MIN_CALIBRATION_SAMPLES", 20)?,
                calibration_beta: env_parse("CALIBRATION_BETA", 5.0)?,
                max_alpha_tilt_abs: env_parse("MAX_ALPHA_TILT_ABS", 0.25)?,
                enable_alpha_tilt_dry_run: env_bool("ENABLE_ALPHA_TILT_DRY_RUN", false)?,
                sharpness: env_parse("SHARPNESS", 2.5)?,
            },
        };
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if let Some(token) = &self.control_token {
            if token.len() < 32 {
                return invalid(
                    "CONTROL_TOKEN",
                    "<redacted>",
                    "must contain at least 32 characters",
                );
            }
        }
        if self.mode != RunMode::Shadow
            && !bind_is_loopback(&self.bind_addr)
            && self.control_token.is_none()
        {
            return Err(ConfigError::UnsafeConfig(
                "non-loopback dry-run dashboard requires CONTROL_TOKEN (32+ characters)".into(),
            ));
        }
        if self.window_secs <= 0 {
            return invalid("WINDOW_SECS", self.window_secs, "must be positive");
        }
        if self.decision_tick_millis < 25 || self.decision_tick_millis > 1_000 {
            return invalid(
                "DECISION_TICK_MILLIS",
                self.decision_tick_millis,
                "must be in 25..=1000 milliseconds",
            );
        }
        if !(0.0..=1.0).contains(&self.risk.min_edge) {
            return invalid("MIN_EDGE", self.risk.min_edge, "must be in [0,1]");
        }
        if !(0.0..=1.0).contains(&self.risk.max_model_disagreement) {
            return invalid(
                "MAX_MODEL_DISAGREEMENT",
                self.risk.max_model_disagreement,
                "must be in [0,1]",
            );
        }
        if self.risk.no_trade_last_secs < 0 || self.risk.no_trade_last_secs >= self.window_secs {
            return invalid(
                "NO_TRADE_LAST_SECS",
                self.risk.no_trade_last_secs,
                "must be non-negative and below WINDOW_SECS",
            );
        }
        if self.risk.max_decision_rate_per_minute == 0 {
            return invalid(
                "MAX_DECISION_RATE_PER_MINUTE",
                self.risk.max_decision_rate_per_minute,
                "must be positive",
            );
        }
        if !(0.0..=1.0).contains(&self.risk.max_spread) {
            return invalid("MAX_SPREAD", self.risk.max_spread, "must be in [0,1]");
        }
        if !(0.50..=0.99).contains(&self.risk.adaptive_edge_quantile) {
            return invalid(
                "ADAPTIVE_EDGE_QUANTILE",
                self.risk.adaptive_edge_quantile,
                "must be in [0.50,0.99]",
            );
        }
        if self.risk.adaptive_edge_window < 8 {
            return invalid(
                "ADAPTIVE_EDGE_WINDOW",
                self.risk.adaptive_edge_window,
                "must be >= 8",
            );
        }
        if !(0.0..=4.0).contains(&self.risk.adaptive_edge_instability_buffer) {
            return invalid(
                "ADAPTIVE_EDGE_INSTABILITY_BUFFER",
                self.risk.adaptive_edge_instability_buffer,
                "must be in [0,4]",
            );
        }
        if !(0.0..=1.0).contains(&self.execution.latency_edge_buffer) {
            return invalid(
                "LATENCY_EDGE_BUFFER",
                self.execution.latency_edge_buffer,
                "must be in [0,1]",
            );
        }
        if !(0.0..=1.0).contains(&self.execution.min_profit_buffer) {
            return invalid(
                "MIN_PROFIT_BUFFER",
                self.execution.min_profit_buffer,
                "must be in [0,1]",
            );
        }
        Ok(())
    }
}

fn bind_is_loopback(bind_addr: &str) -> bool {
    bind_addr
        .parse::<std::net::SocketAddr>()
        .map(|address| address.ip().is_loopback())
        .unwrap_or(false)
}

fn default_tick_millis(mode: RunMode) -> u64 {
    match mode {
        RunMode::Shadow | RunMode::DryRun => 250,
    }
}

fn mode_from_args_env(args: &[String]) -> Result<RunMode, ConfigError> {
    let mut mode = env::var("RUN_MODE")
        .ok()
        .map(|m| RunMode::from_str(&m))
        .transpose()?
        .unwrap_or_default();
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        if arg.starts_with("--") && arg != "--mode" && !arg.starts_with("--mode=") {
            return Err(ConfigError::InvalidValue {
                key: "argument".into(),
                value: "unsupported".into(),
                reason: format!("unknown option {arg}"),
            });
        } else if arg == "--mode" {
            let value = it.next().ok_or_else(|| ConfigError::InvalidValue {
                key: "--mode".into(),
                value: "".into(),
                reason: "missing value".into(),
            })?;
            mode = RunMode::from_str(value)?;
        } else if let Some(value) = arg.strip_prefix("--mode=") {
            mode = RunMode::from_str(value)?;
        }
    }
    Ok(mode)
}

fn env_string(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_bool(key: &str, default: bool) -> Result<bool, ConfigError> {
    match env::var(key) {
        Ok(v) => match v.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => Err(ConfigError::InvalidValue {
                key: key.into(),
                value: v,
                reason: "expected boolean".into(),
            }),
        },
        Err(_) => Ok(default),
    }
}

fn env_parse<T>(key: &str, default: T) -> Result<T, ConfigError>
where
    T: FromStr + ToString,
{
    match env::var(key) {
        Ok(v) => v.parse().map_err(|_| ConfigError::InvalidValue {
            key: key.into(),
            value: v,
            reason: "parse failed".into(),
        }),
        Err(_) => Ok(default),
    }
}

fn invalid<T: ToString>(key: &str, value: T, reason: &str) -> Result<(), ConfigError> {
    Err(ConfigError::InvalidValue {
        key: key.into(),
        value: value.to_string(),
        reason: reason.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_mode_arg() {
        let mode = mode_from_args_env(&["app".into(), "--mode=dry-run".into()]).unwrap();
        assert_eq!(mode, RunMode::DryRun);
    }

    #[test]
    fn parses_dry_run_strategy() {
        assert_eq!(
            DryRunStrategy::from_str("hold_sharp").unwrap(),
            DryRunStrategy::HoldSharp
        );
    }

    #[test]
    fn parses_adaptive_edge_mode_aliases() {
        assert_eq!(
            AdaptiveEdgeMode::from_str("robust_quantile").unwrap(),
            AdaptiveEdgeMode::RobustQuantile
        );
        assert_eq!(
            AdaptiveEdgeMode::from_str("sota").unwrap(),
            AdaptiveEdgeMode::Conformal
        );
    }

    #[test]
    fn dry_run_public_bind_requires_control_token() {
        let mut cfg = AppConfig::from_env_args(["app"]).unwrap();
        cfg.mode = RunMode::DryRun;
        cfg.bind_addr = "0.0.0.0:8080".into();
        cfg.control_token = None;
        assert!(matches!(cfg.validate(), Err(ConfigError::UnsafeConfig(_))));
        cfg.control_token = Some("a".repeat(32));
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn short_control_token_is_rejected_without_echoing_secret() {
        let mut cfg = AppConfig::from_env_args(["app"]).unwrap();
        cfg.control_token = Some("too-short".into());
        let error = cfg.validate().unwrap_err().to_string();
        assert!(error.contains("<redacted>"));
        assert!(!error.contains("too-short"));
    }

    #[test]
    fn unsupported_modes_and_flags_are_rejected() {
        assert!(RunMode::from_str("production").is_err());
        assert!(mode_from_args_env(&["app".into(), "--unknown-mode".into()]).is_err());
    }
}
