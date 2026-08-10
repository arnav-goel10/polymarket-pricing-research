use serde::Serialize;

/// One model's quote for the dashboard.
#[derive(Debug, Clone, Serialize, Default, PartialEq)]
pub struct ModelQuote {
    pub name: String,
    pub p_yes: f64,
    pub weight: f64,
    pub brier: Option<f64>,
    pub log_loss: Option<f64>,
}

#[derive(Debug, Clone, serde::Serialize, Default, PartialEq)]
pub struct StrategyPnl {
    pub name: String,
    pub realized_pnl: f64,
    pub unrealized: f64,
    pub max_drawdown: f64,
    pub trades: usize,
    pub wins: usize,
    pub notional: f64,
    pub open_side: Option<String>,
    pub open_entry: Option<f64>,
}

/// Active intraday (buy-cheap/sell-rich/inverse-short) paper P&L per strategy.
#[derive(Debug, Clone, serde::Serialize, Default, PartialEq)]
pub struct ActivePnl {
    pub name: String,
    pub realized_pnl: f64,
    pub unrealized: f64,
    pub max_drawdown: f64,
    pub trades: usize,
    pub position: f64,
}

/// Convergence (fair-value reversion) intraday paper P&L per strategy.
#[derive(Debug, Clone, serde::Serialize, Default, PartialEq)]
pub struct ConvergencePnl {
    pub name: String,
    pub realized_pnl: f64,
    pub unrealized: f64,
    pub max_drawdown: f64,
    pub trades: usize,
    pub wins: usize,
    pub position: f64,
}

/// One closed trade (hold or active) for one strategy/window — the History tab.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct TradeRecord {
    pub t0_millis: i64,
    pub strategy: String,
    pub mode: String,   // "hold" or "active"
    pub detail: String, // human-readable position summary
    pub outcome_up: bool,
    pub won: bool,
    pub pnl: f64,
    #[serde(default)]
    pub strategy_version: String,
    #[serde(default)]
    pub entry_edge: Option<f64>,
    #[serde(default)]
    pub spread: Option<f64>,
    #[serde(default)]
    pub book_depth: Option<f64>,
    #[serde(default)]
    pub entry_timestamp: Option<i64>,
    #[serde(default)]
    pub entry_price: Option<f64>,
    #[serde(default)]
    pub notional: Option<f64>,
    #[serde(default)]
    pub model_p_yes: Option<f64>,
}

/// One sampled point of the realized-P&L time series, for the dashboard graphs.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct PnlPoint {
    pub t_millis: i64,
    pub hold: Vec<(String, f64)>, // (strategy, realized_pnl) hold-to-resolution
    pub active: Vec<(String, f64)>, // (strategy, realized_pnl) active intraday
    #[serde(default)]
    pub convergence: Vec<(String, f64)>, // (strategy, realized_pnl) convergence
    pub mean_brier: f64,
}

#[derive(Debug, Clone, serde::Serialize, Default, PartialEq)]
pub struct LatencyStats {
    pub samples: usize,
    pub p50: u128,
    pub p90: u128,
    pub p99: u128,
    pub max: u128,
}

#[derive(Debug, Clone, serde::Serialize, Default, PartialEq)]
pub struct LatencySnapshot {
    pub tick_to_decision_micros: Option<LatencyStats>,
    pub book_age_millis: Option<LatencyStats>,
    pub chainlink_age_millis: Option<LatencyStats>,
    pub binance_age_millis: Option<LatencyStats>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default, PartialEq)]
pub struct DryRunPnlPoint {
    pub t_millis: i64,
    pub equity: f64,
}

/// Full snapshot pushed to the browser each tick.
#[derive(Debug, Clone, Serialize, Default)]
pub struct DashboardState {
    pub now_millis: i64,
    pub spot: f64,
    pub s0: f64,
    pub seconds_remaining: i64,
    pub sigma_annual: f64,
    pub skew: f64,
    pub kurt: f64,
    pub lead_lag_steps: i64,
    pub models: Vec<ModelQuote>,
    pub ensemble_p_yes: f64,
    pub model_disagreement: f64,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub edge: Option<f64>,
    pub edge_side: Option<String>,
    pub risk: String,
    pub mode: String,
    pub dry_run_enabled: bool,
    pub shadow_fills: usize,
    pub shadow_notional: f64,
    pub resolved_windows: usize,
    pub mean_brier: f64,
    pub market_slug: Option<String>,
    pub yes_token_id: Option<String>,
    pub no_token_id: Option<String>,
    pub chainlink_age_millis: Option<i64>,
    pub binance_age_millis: Option<i64>,
    pub book_age_millis: Option<i64>,
    pub book_depth: Option<f64>,
    pub spread: Option<f64>,
    /// Binance top-of-book queue imbalance (bid_qty-ask_qty)/(bid_qty+ask_qty),
    /// in [-1,1]. Research: top-of-book imbalance is the #1 short-horizon
    /// directional predictor for crypto mid-price (arXiv 2602.00776).
    pub book_imbalance: f64,
    pub position: f64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub last_simulation_error: Option<String>,
    pub killed: bool,
    pub paused: bool,
    /// True only when S0 is anchored to the real window open (clean window). On a
    /// window inherited mid-run at startup it's false and S0 is approximate.
    pub s0_anchored: bool,
    /// Adaptive edge threshold currently in force, and the learned typical-max edge.
    pub edge_threshold: f64,
    pub typical_max_edge: f64,
    #[serde(default)]
    pub hold_edge_threshold: f64,
    #[serde(default)]
    pub active_edge_threshold: f64,
    #[serde(default)]
    pub convergence_edge_threshold: f64,
    #[serde(default)]
    pub hold_typical_max_edge: f64,
    #[serde(default)]
    pub active_typical_max_edge: f64,
    #[serde(default)]
    pub convergence_typical_max_edge: f64,
    pub tick_to_decision_micros: Option<u128>,
    #[serde(default)]
    pub latency: LatencySnapshot,
    #[serde(default)]
    pub dry_run_strategy: String,
    #[serde(default)]
    pub dry_run_kind: String,
    #[serde(default)]
    pub dry_run_size_usd: f64,
    #[serde(default)]
    pub dry_run_min_profit_buffer: f64,
    #[serde(default)]
    pub dry_run_latency_edge_buffer: f64,
    #[serde(default)]
    pub dry_run_edge_floor: f64,
    #[serde(default)]
    pub dry_run_clean_window: bool,
    #[serde(default)]
    pub dry_run_market_in_sync: bool,
    #[serde(default)]
    pub dry_run_attempted_this_window: bool,
    #[serde(default)]
    pub dry_run_position_side: Option<String>,
    #[serde(default)]
    pub dry_run_position_shares: f64,
    #[serde(default)]
    pub dry_run_avg_entry: Option<f64>,
    #[serde(default)]
    pub dry_run_mark_price: Option<f64>,
    #[serde(default)]
    pub dry_run_realized_pnl: f64,
    #[serde(default)]
    pub dry_run_unrealized_pnl: f64,
    #[serde(default)]
    pub dry_run_equity: f64,
    #[serde(default)]
    pub dry_run_max_drawdown: f64,
    #[serde(default)]
    pub dry_run_trade_count: usize,
    #[serde(default)]
    pub dry_run_win_count: usize,
    #[serde(default)]
    pub dry_run_notional: f64,
    #[serde(default)]
    pub dry_run_last_fill_status: Option<String>,
    #[serde(default)]
    pub dry_run_last_decision_id: Option<String>,
    #[serde(default)]
    pub dry_run_last_fill_price: Option<f64>,
    #[serde(default)]
    pub dry_run_last_fill_ts_millis: Option<i64>,
    #[serde(default)]
    pub dry_run_pnl_curve: Vec<DryRunPnlPoint>,
    pub strategy_pnls: Vec<StrategyPnl>,
    pub active_pnls: Vec<ActivePnl>,
    pub convergence_pnls: Vec<ConvergencePnl>,
    /// Learned recalibration parameters for the `calibrated` strategy.
    pub platt_slope: f64,
    pub platt_bias: f64,
    pub platt_samples: usize,
    /// Current learned weights of the `blend_dyn` ensemble (component, weight),
    /// normalised, sorted desc — surfaced for the dashboard hover tooltip.
    #[serde(default)]
    pub blend_weights: Vec<(String, f64)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_with_expected_fields() {
        let s = DashboardState::default();
        let j = serde_json::to_string(&s).unwrap();
        assert!(j.contains("ensemble_p_yes"));
        assert!(j.contains("seconds_remaining"));
    }
}
