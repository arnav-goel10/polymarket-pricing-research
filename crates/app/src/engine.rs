use crate::signals::PriceBuffer;
use crate::state::{DashboardState, ModelQuote};
use execution::{best_trade, RiskInputs, RiskManager, RiskVerdict, Side};
use market::{OrderBook, Window};
use poly_core::PricingContext;
use pricing::DigitalModel;
use std::collections::HashMap;

/// Inputs the engine needs each step that aren't the rolling buffers.
pub struct StepCtx<'a> {
    pub now_millis: i64,
    pub window: Window,
    /// Settlement-price buffer (Chainlink): drives spot and the barrier.
    pub chainlink: &'a PriceBuffer,
    /// Volatility buffer (fast venue, e.g. Binance): drives sigma/skew/kurt.
    /// Falls back to `chainlink` when it is too sparse to estimate.
    pub vol_buffer: &'a PriceBuffer,
    pub lead_lag_steps: i64,
    pub book: Option<&'a OrderBook>,
    pub book_age_millis: Option<i64>,
    pub models: &'a [Box<dyn DigitalModel>],
    pub model_scores: &'a HashMap<String, (f64, f64)>,
    pub fee: f64,
    pub trade_size: f64,
    pub mode: &'a str,
    pub dry_run_enabled: bool,
    pub killed: bool,
    pub shadow_fills: usize,
    pub shadow_notional: f64,
    /// Current open exposure used for the risk check (per-window, not lifetime).
    pub open_exposure: f64,
    pub feed_fresh: bool,
    pub chainlink_age_millis: Option<i64>,
    pub binance_age_millis: Option<i64>,
    pub decision_count_last_minute: u32,
    pub market_pnl: f64,
    pub daily_notional: f64,
    pub market_slug: Option<&'a str>,
    pub yes_token_id: Option<&'a str>,
    pub no_token_id: Option<&'a str>,
    pub last_simulation_error: Option<&'a str>,
    pub tick_to_decision_micros: Option<u128>,
    pub alpha_tilt: f64,
}

fn verdict_str(v: RiskVerdict) -> String {
    match v {
        RiskVerdict::Allow => "Allow".to_string(),
        RiskVerdict::Block(r) => format!("Block({r:?})"),
    }
}

/// Assemble a full DashboardState from real-time inputs (pure).
pub fn step(rm: &RiskManager, c: &StepCtx) -> DashboardState {
    let spot = c.chainlink.last().unwrap_or(c.window.s0);
    let tau = c.window.tau_years(c.now_millis);
    let secs = c.window.seconds_remaining(c.now_millis);
    // Volatility/higher moments come from the fast venue (Binance); the smoothed
    // Chainlink settlement feed under-estimates realized vol. Fall back to the
    // Chainlink buffer only while the fast venue is still warming up.
    let vol_n = c.vol_buffer.len();
    let (sigma_raw, skew, kurt) = if vol_n >= 12 {
        let bandwidth = (vol_n / 8).clamp(2, 20);
        // Blend a noise-robust LEVEL (realized kernel, debiases microstructure) with
        // a regime-RESPONSIVE estimate (EWMA, weights recent moves) — the average is
        // more accurate than either alone for pricing the next few minutes. EWMA
        // λ=0.97 over ~1s bars ≈ a ~30s half-life, reactive without being jumpy.
        let kernel = c.vol_buffer.annualized_kernel_vol(bandwidth);
        let ewma = c.vol_buffer.annualized_ewma_vol(0.97);
        (
            0.5 * kernel + 0.5 * ewma,
            c.vol_buffer.skew(),
            c.vol_buffer.kurt(),
        )
    } else {
        (
            c.chainlink.annualized_vol(),
            c.chainlink.skew(),
            c.chainlink.kurt(),
        )
    };
    // Sanity bound: short-horizon realized-vol estimates are noisy and can be
    // inflated by residual microstructure; clamp to a defensible BTC band so a
    // bad estimate can't corrupt every model price. Non-finite -> safe default
    // (guards against any NaN propagating into prices/edges -> sort panics).
    // Widened band: the old 0.08 floor was constantly binding (σ pinned at the
    // floor), which meant the digitals ran on a near-constant vol rather than a
    // real estimate. With higher-frequency Binance sampling the realized-vol
    // estimate is meaningful; only guard against degenerate/explosive values.
    let sigma = if sigma_raw.is_finite() {
        sigma_raw.clamp(0.05, 2.5)
    } else {
        0.5
    };
    let skew = if skew.is_finite() {
        skew.clamp(-5.0, 5.0)
    } else {
        0.0
    };
    let kurt = if kurt.is_finite() {
        kurt.clamp(1.0, 50.0)
    } else {
        3.0
    };

    let pctx = PricingContext {
        s_t: spot,
        s0: c.window.s0,
        tau,
        mu: c.alpha_tilt,
        sigma,
        skew,
        kurt,
    };

    let n_models = c.models.len().max(1) as f64;
    let quotes: Vec<ModelQuote> = c
        .models
        .iter()
        .map(|m| {
            let name = m.name().to_string();
            let (brier, log_loss) = c
                .model_scores
                .get(&name)
                .copied()
                .map(|(b, l)| (Some(b), Some(l)))
                .unwrap_or((None, None));
            let p = m.price(&pctx);
            ModelQuote {
                name,
                p_yes: if p.is_finite() {
                    p.clamp(0.0, 1.0)
                } else {
                    0.5
                },
                weight: 1.0 / n_models, // equal — no calibration weighting
                brier,
                log_loss,
            }
        })
        .collect();
    // Reference average only (each model is evaluated as its own strategy; we do
    // NOT trade a weighted blend). Kept for the headline + a single edge readout.
    let ensemble = quotes.iter().map(|q| q.p_yes).sum::<f64>() / n_models;
    let disagreement = match (
        quotes
            .iter()
            .map(|q| q.p_yes)
            .fold(f64::NEG_INFINITY, f64::max),
        quotes.iter().map(|q| q.p_yes).fold(f64::INFINITY, f64::min),
    ) {
        (hi, lo) if hi.is_finite() && lo.is_finite() => hi - lo,
        _ => 0.0,
    };

    let (best_bid, best_ask) = match c.book {
        Some(b) => (b.best_bid(), b.best_ask()),
        None => (None, None),
    };
    let book_depth = c.book.map(|b| b.top_depth());
    let spread = c.book.and_then(|b| b.spread());
    let trade = c
        .book
        .and_then(|b| best_trade(ensemble, b, c.trade_size, c.fee));
    let (edge, edge_side) = match trade {
        Some(t) => (Some(t.edge), Some(format!("{:?}", t.side))),
        None => (None, None),
    };

    let risk = rm.check(&RiskInputs {
        edge: edge.unwrap_or(0.0),
        stake: c.trade_size * best_ask.unwrap_or(0.5),
        open_exposure: c.open_exposure,
        daily_pnl: 0.0,
        seconds_remaining: secs,
        model_disagreement: disagreement,
        feed_fresh: c.feed_fresh,
        book_fresh: c.book.is_some() && c.book_age_millis.is_some_and(|age| age <= 3_000),
        decision_count_last_minute: c.decision_count_last_minute,
        market_pnl: c.market_pnl,
        daily_notional: c.daily_notional,
        available_depth: book_depth.unwrap_or(0.0),
        spread: spread.unwrap_or(1.0),
    });

    let _ = Side::Buy; // keep Side in scope for downstream callers

    DashboardState {
        now_millis: c.now_millis,
        spot,
        s0: c.window.s0,
        seconds_remaining: secs,
        sigma_annual: sigma,
        skew,
        kurt,
        lead_lag_steps: c.lead_lag_steps,
        models: quotes,
        ensemble_p_yes: ensemble,
        model_disagreement: disagreement,
        best_bid,
        best_ask,
        edge,
        edge_side,
        risk: verdict_str(risk),
        mode: c.mode.to_string(),
        dry_run_enabled: c.dry_run_enabled,
        shadow_fills: c.shadow_fills,
        shadow_notional: c.shadow_notional,
        resolved_windows: 0,
        mean_brier: 0.0,
        market_slug: c.market_slug.map(str::to_string),
        yes_token_id: c.yes_token_id.map(str::to_string),
        no_token_id: c.no_token_id.map(str::to_string),
        chainlink_age_millis: c.chainlink_age_millis,
        binance_age_millis: c.binance_age_millis,
        book_age_millis: c.book_age_millis,
        book_depth,
        spread,
        book_imbalance: 0.0,
        position: 0.0,
        realized_pnl: 0.0,
        unrealized_pnl: 0.0,
        last_simulation_error: c.last_simulation_error.map(str::to_string),
        killed: c.killed,
        paused: false,
        s0_anchored: false,
        edge_threshold: 0.0,
        typical_max_edge: 0.0,
        hold_edge_threshold: 0.0,
        active_edge_threshold: 0.0,
        convergence_edge_threshold: 0.0,
        hold_typical_max_edge: 0.0,
        active_typical_max_edge: 0.0,
        convergence_typical_max_edge: 0.0,
        tick_to_decision_micros: c.tick_to_decision_micros,
        latency: Default::default(),
        dry_run_strategy: String::new(),
        dry_run_kind: String::new(),
        dry_run_size_usd: 0.0,
        dry_run_min_profit_buffer: 0.0,
        dry_run_latency_edge_buffer: 0.0,
        dry_run_edge_floor: 0.0,
        dry_run_clean_window: false,
        dry_run_market_in_sync: false,
        dry_run_attempted_this_window: false,
        dry_run_position_side: None,
        dry_run_position_shares: 0.0,
        dry_run_avg_entry: None,
        dry_run_mark_price: None,
        dry_run_realized_pnl: 0.0,
        dry_run_unrealized_pnl: 0.0,
        dry_run_equity: 0.0,
        dry_run_max_drawdown: 0.0,
        dry_run_trade_count: 0,
        dry_run_win_count: 0,
        dry_run_notional: 0.0,
        dry_run_last_fill_status: None,
        dry_run_last_decision_id: None,
        dry_run_last_fill_price: None,
        dry_run_last_fill_ts_millis: None,
        dry_run_pnl_curve: Vec::new(),
        strategy_pnls: Vec::new(),
        active_pnls: Vec::new(),
        convergence_pnls: Vec::new(),
        platt_slope: 1.0,
        platt_bias: 0.0,
        platt_samples: 0,
        blend_weights: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use execution::RiskLimits;
    use market::book::Level;
    use pricing::{BlackScholesDigital, CornishFisherDigital, StudentTDigital};

    fn models() -> Vec<Box<dyn DigitalModel>> {
        vec![
            Box::new(BlackScholesDigital),
            Box::new(StudentTDigital { nu: 4.0 }),
            Box::new(CornishFisherDigital),
        ]
    }

    fn limits() -> RiskLimits {
        RiskLimits {
            min_edge: 0.03,
            max_stake_per_window: 1_000.0,
            max_open_exposure: 100_000.0,
            max_daily_loss: 10_000.0,
            no_trade_last_secs: 20,
            max_model_disagreement: 0.5,
            max_decision_rate_per_minute: 10,
            max_market_loss: 10_000.0,
            max_daily_notional: 100_000.0,
            min_depth: 1.0,
            max_spread: 0.5,
        }
    }

    fn buffer_at(price: f64) -> PriceBuffer {
        let mut b = PriceBuffer::new(64);
        // small wiggle so sigma > 0
        for i in 0..40 {
            b.push(1_000 * i, price * (1.0 + 0.00002 * ((i % 2) as f64 - 0.5)));
        }
        b
    }

    fn ctx<'a>(
        now_millis: i64,
        window: Window,
        chainlink: &'a PriceBuffer,
        book: Option<&'a OrderBook>,
        models: &'a [Box<dyn DigitalModel>],
    ) -> StepCtx<'a> {
        StepCtx {
            now_millis,
            window,
            chainlink,
            vol_buffer: chainlink,
            lead_lag_steps: 0,
            book,
            book_age_millis: book.map(|_| 0),
            models,
            model_scores: Box::leak(Box::new(HashMap::new())),
            fee: 0.0,
            trade_size: 100.0,
            mode: "Shadow",
            dry_run_enabled: false,
            killed: false,
            shadow_fills: 0,
            shadow_notional: 0.0,
            open_exposure: 0.0,
            feed_fresh: true,
            chainlink_age_millis: Some(0),
            binance_age_millis: Some(0),
            decision_count_last_minute: 0,
            market_pnl: 0.0,
            daily_notional: 0.0,
            market_slug: None,
            yes_token_id: None,
            no_token_id: None,
            last_simulation_error: None,
            tick_to_decision_micros: None,
            alpha_tilt: 0.0,
        }
    }

    #[test]
    fn deep_itm_buy_edge_against_cheap_asks() {
        let rm = RiskManager::new(limits());
        // window opened at 60_000; spot now well above => fair near 1
        let w = Window::open(60_000.0, 1_700_000_000_000, 300);
        let book = OrderBook {
            bids: vec![Level {
                price: 0.80,
                size: 500.0,
            }],
            asks: vec![Level {
                price: 0.85,
                size: 500.0,
            }],
        };
        let cl = buffer_at(60_400.0);
        let ms = models();
        let st = step(
            &rm,
            &StepCtx {
                lead_lag_steps: 1,
                ..ctx(1_700_000_060_000, w, &cl, Some(&book), &ms)
            },
        );
        assert!(
            st.ensemble_p_yes > 0.9,
            "fair {} should be high ITM",
            st.ensemble_p_yes
        );
        assert_eq!(st.edge_side.as_deref(), Some("Buy"));
        assert!(st.edge.unwrap() > 0.0);
    }

    #[test]
    fn sigma_comes_from_the_vol_buffer_not_chainlink() {
        let rm = RiskManager::new(limits());
        let w = Window::open(60_000.0, 1_700_000_000_000, 300);
        let ms = models();
        // Chainlink buffer: near-flat (smoothed) => tiny vol.
        let mut cl = PriceBuffer::new(128);
        for i in 0..60 {
            cl.push(
                1_000 * i,
                60_000.0 * (1.0 + 0.000001 * ((i % 2) as f64 - 0.5)),
            );
        }
        // Vol buffer: genuinely volatile fast venue => much higher vol.
        let mut vb = PriceBuffer::new(128);
        for i in 0..60 {
            vb.push(1_000 * i, 60_000.0 * (1.0 + 0.002 * ((i % 2) as f64 - 0.5)));
        }
        let st = step(
            &rm,
            &StepCtx {
                vol_buffer: &vb,
                ..ctx(1_700_000_060_000, w, &cl, None, &ms)
            },
        );
        assert!(
            st.sigma_annual > 0.1,
            "sigma should reflect the volatile vol_buffer, got {}",
            st.sigma_annual
        );
    }

    #[test]
    fn pin_window_blocks() {
        let rm = RiskManager::new(limits());
        let w = Window::open(60_000.0, 1_700_000_000_000, 300);
        let cl = buffer_at(60_010.0);
        let ms = models();
        let book = OrderBook {
            bids: vec![Level {
                price: 0.49,
                size: 500.0,
            }],
            asks: vec![Level {
                price: 0.51,
                size: 500.0,
            }],
        };
        let st = step(&rm, &ctx(1_700_000_295_000, w, &cl, Some(&book), &ms));
        assert_eq!(st.seconds_remaining, 5);
        assert_eq!(st.risk, "Block(PinWindow)");
    }
}
