//! BTC five-minute prediction-market research orchestrator.
//!
//! Shadow mode evaluates every strategy. `--mode dry-run` exercises the selected
//! paper strategy against observed books. Neither mode can submit an order.

use app::adaptive::{AdaptiveEdge, AdaptiveEdgeParams};
use app::engine::{step, StepCtx};
use app::latency::RollingLatency;
use app::metrics::{Metrics, BINANCE_STALE_MILLIS, BOOK_STALE_MILLIS, RTDS_STALE_MILLIS};
use app::resolution::{
    ActiveObs, ActivePaperTracker, PaperObservation, PendingWindow, ResolutionTracker,
    StrategyPaperTracker,
};
use app::server::{router, ControlFlags, Shared};
use app::signals::PriceBuffer;
use app::state::DashboardState;
use app::state::StrategyPnl;
use app::window_clock::WindowClock;
use execution::{ExecMode, ExecutionService, RiskLimits, RiskManager, Side, TradeDecision};
use feeds::conn::ConnOpts;
use feeds::micro::best_lag;
use feeds::recording::Source;
use feeds::FeedSource;
use market::{clob_book, DiscoveredMarket};
use poly_core::{AppConfig, Backoff, DryRunStrategy, FeedMetrics, RunMode};
use pricing::{
    BlackScholesDigital, CosVgDigital, DigitalModel, OnlinePlatt, OnlineWeights, SharpDigital,
    StudentTDigital,
};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
struct Buffers {
    chainlink: PriceBuffer,
    binance: PriceBuffer,
    last_chainlink_millis: i64,
    last_binance_millis: i64,
    /// Binance bookTicker queue imbalance (bid_qty-ask_qty)/(bid_qty+ask_qty) in
    /// [-1,1]; a documented short-horizon directional (order-flow) signal.
    last_imbalance: f64,
}

#[derive(Clone, Default)]
struct BookSlot {
    token_id: Option<String>,
    book: Option<market::OrderBook>,
    updated_millis: i64,
    exchange_ts_millis: Option<i64>,
}

fn now_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Full accumulated run state, persisted to `shadow-data/run_state.json` so a
/// restart RESUMES (Brier boards, per-strategy realized P&L, learned recalibration
/// and adaptive-edge state) instead of resetting. Open positions are dropped on
/// load (their windows are gone), keeping only the accumulated history.
#[derive(serde::Serialize, serde::Deserialize)]
struct RunState {
    tracker: app::resolution::ResolutionTracker,
    paper: StrategyPaperTracker,
    active: ActivePaperTracker,
    convergence: app::convergence::ConvergencePaperTracker,
    platt: OnlinePlatt,
    #[serde(default)]
    adaptive: app::adaptive::AdaptiveEdge,
    #[serde(default)]
    adaptive_hold: app::adaptive::AdaptiveEdge,
    #[serde(default)]
    adaptive_active: app::adaptive::AdaptiveEdge,
    #[serde(default)]
    adaptive_convergence: app::adaptive::AdaptiveEdge,
    #[serde(default)]
    online_w: OnlineWeights,
    #[serde(default)]
    dry_run_pnl: DryRunPnlTracker,
}

/// Component models combined by the `blend` (fixed equal-weight) and `blend_dyn`
/// (online-learned weight) ensembles. Includes `calibrated` (the Platt-recalibrated
/// sharp) as a distinct forecast — diversity helps combination (Bates-Granger).
const BLEND_COMPONENTS: [&str; 5] = [
    "black_scholes",
    "student_t",
    "sharp",
    "cos_vg",
    "calibrated",
];
const STRATEGY_VERSION: &str = "paper-evidence-v1";
const OPEN_REPRICE_STRATEGY: &str = "hold_open_reprice_sharp";
const OPEN_REPRICE_MIN_SECONDS_REMAINING: i64 = 240;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct DryRunPosition {
    t0_millis: i64,
    s0: f64,
    side: String,
    shares: f64,
    cost: f64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct DryRunPnlTracker {
    position: Option<DryRunPosition>,
    realized_pnl: f64,
    peak_equity: f64,
    max_drawdown: f64,
    trade_count: usize,
    win_count: usize,
    notional: f64,
    last_fill_status: Option<String>,
    last_decision_id: Option<String>,
    last_fill_price: Option<f64>,
    last_fill_ts_millis: Option<i64>,
    curve: VecDeque<app::state::DryRunPnlPoint>,
}

struct DryRunFillInput<'a> {
    t0_millis: i64,
    s0: f64,
    is_yes: bool,
    matched_size: f64,
    matched_notional: f64,
    fill_status: &'a str,
    decision_id: Option<&'a str>,
    fill_price: f64,
    ts_millis: i64,
}

struct DryRunPnlView {
    side: Option<String>,
    shares: f64,
    avg_entry: Option<f64>,
    mark: Option<f64>,
    realized: f64,
    unrealized: f64,
    equity: f64,
    max_drawdown: f64,
    curve: Vec<app::state::DryRunPnlPoint>,
}

impl Default for DryRunPnlTracker {
    fn default() -> Self {
        Self {
            position: None,
            realized_pnl: 0.0,
            peak_equity: 0.0,
            max_drawdown: 0.0,
            trade_count: 0,
            win_count: 0,
            notional: 0.0,
            last_fill_status: None,
            last_decision_id: None,
            last_fill_price: None,
            last_fill_ts_millis: None,
            curve: VecDeque::with_capacity(720),
        }
    }
}

impl DryRunPnlTracker {
    fn settle_expired(&mut self, current_t0_millis: i64, spot: f64) {
        let Some(pos) = self.position.take() else {
            return;
        };
        if pos.t0_millis == current_t0_millis {
            self.position = Some(pos);
            return;
        }
        let won = if pos.side == "YES" {
            spot > pos.s0
        } else {
            spot < pos.s0
        };
        let payout = if won { pos.shares } else { 0.0 };
        let pnl = payout - pos.cost;
        self.realized_pnl += pnl;
        if pnl > 0.0 {
            self.win_count += 1;
        }
    }

    fn record_fill(&mut self, fill: DryRunFillInput<'_>) {
        if fill.matched_size <= 0.0 || fill.matched_notional <= 0.0 {
            self.last_fill_status = Some(fill.fill_status.to_string());
            self.last_decision_id = fill.decision_id.map(str::to_string);
            self.last_fill_ts_millis = Some(fill.ts_millis);
            return;
        }
        let side = if fill.is_yes { "YES" } else { "NO" }.to_string();
        self.trade_count += 1;
        self.notional += fill.matched_notional;
        self.last_fill_status = Some(fill.fill_status.to_string());
        self.last_decision_id = fill.decision_id.map(str::to_string);
        self.last_fill_price = Some(fill.fill_price);
        self.last_fill_ts_millis = Some(fill.ts_millis);

        match self.position.as_mut() {
            Some(pos) if pos.t0_millis == fill.t0_millis && pos.side == side => {
                pos.shares += fill.matched_size;
                pos.cost += fill.matched_notional;
            }
            _ => {
                self.position = Some(DryRunPosition {
                    t0_millis: fill.t0_millis,
                    s0: fill.s0,
                    side,
                    shares: fill.matched_size,
                    cost: fill.matched_notional,
                });
            }
        }
    }

    fn mark_price(&self, book: Option<&market::OrderBook>) -> Option<f64> {
        let side = self.position.as_ref()?.side.as_str();
        let book = book?;
        match side {
            "YES" => book.best_bid(),
            "NO" => book.best_ask().map(|ask| 1.0 - ask),
            _ => None,
        }
    }

    fn snapshot(&mut self, now_millis: i64, book: Option<&market::OrderBook>) -> DryRunPnlView {
        let mark = self.mark_price(book);
        let (side, shares, avg_entry, unrealized) = match (&self.position, mark) {
            (Some(pos), Some(mark)) => (
                Some(pos.side.clone()),
                pos.shares,
                Some(pos.cost / pos.shares.max(1e-9)),
                pos.shares * mark - pos.cost,
            ),
            (Some(pos), None) => (
                Some(pos.side.clone()),
                pos.shares,
                Some(pos.cost / pos.shares.max(1e-9)),
                0.0,
            ),
            (None, _) => (None, 0.0, None, 0.0),
        };
        let equity = self.realized_pnl + unrealized;
        self.peak_equity = self.peak_equity.max(equity);
        self.max_drawdown = self.max_drawdown.max(self.peak_equity - equity);
        self.curve.push_back(app::state::DryRunPnlPoint {
            t_millis: now_millis,
            equity,
        });
        while self.curve.len() > 720 {
            self.curve.pop_front();
        }
        DryRunPnlView {
            side,
            shares,
            avg_entry,
            mark,
            realized: self.realized_pnl,
            unrealized,
            equity,
            max_drawdown: self.max_drawdown,
            curve: self.curve.iter().cloned().collect(),
        }
    }
}

/// Rebuild the dashboard P&L-curve series from the closed-trade ledger: one point
/// per resolved window, cumulative realized P&L per strategy for each track. This
/// makes the graphs a faithful function of the ledger (no reset artifacts).
fn rebuild_history_from_ledger(
    trades: &[app::state::TradeRecord],
    window_secs: i64,
) -> Vec<app::state::PnlPoint> {
    use std::collections::HashMap;
    let mut sorted: Vec<&app::state::TradeRecord> = trades.iter().collect();
    sorted.sort_by_key(|t| t.t0_millis);
    // (mode, strategy) -> running cumulative realized.
    let mut cum: HashMap<(String, String), f64> = HashMap::new();
    let mut points = Vec::new();
    let mut i = 0;
    while i < sorted.len() {
        let t0 = sorted[i].t0_millis;
        while i < sorted.len() && sorted[i].t0_millis == t0 {
            let t = sorted[i];
            *cum.entry((t.mode.clone(), t.strategy.clone())).or_default() += t.pnl;
            i += 1;
        }
        let series = |mode: &str| -> Vec<(String, f64)> {
            cum.iter()
                .filter(|((m, _), _)| m == mode)
                .map(|((_, s), v)| (s.clone(), *v))
                .collect()
        };
        points.push(app::state::PnlPoint {
            t_millis: t0 + window_secs * 1000, // window close
            hold: series("hold"),
            active: series("active"),
            convergence: series("conv"),
            mean_brier: 0.0, // not reconstructable from the ledger
        });
    }
    points
}

fn build_models(enable_cos_vg: bool, sharpness: f64) -> Vec<Box<dyn DigitalModel>> {
    // Base set after the 59-window run. Dropped: cornish_fisher (Brier ~0.21) and the
    // lead-lag standalones (real-time Data Stream → no oracle lead). cos_vg is KEPT:
    // its Brier was nominally last but the gap (~0.006) is well inside one standard
    // error at n=59, AND it is the only fat-tail/jump engine (COS on a Variance-Gamma
    // characteristic function) — that diversity is exactly what helps the `blend`
    // (Bates-Granger combination gains come from models that err differently).
    // Two derived strategies are appended at runtime (need running state): `calibrated` =
    // online-Platt recalibration of `sharp`, and `blend` = Brier-weighted combination.
    let mut models: Vec<Box<dyn DigitalModel>> = vec![
        Box::new(BlackScholesDigital),
        Box::new(StudentTDigital { nu: 4.0 }),
        // Sharpness/continuation model — exploits the market's under-reaction
        // (calibrated but not sharp); the best individual base in the run.
        Box::new(SharpDigital::new(sharpness)),
    ];
    if enable_cos_vg {
        models.push(Box::new(CosVgDigital::new(64))); // 64 terms: <0.001 error at 5m horizon, 2x faster
    }
    models
}

fn build_models_for_run(cfg: &AppConfig) -> Vec<Box<dyn DigitalModel>> {
    if cfg.mode == RunMode::Shadow {
        return build_models(cfg.models.enable_cos_vg, cfg.models.sharpness);
    }
    match cfg.execution.dry_run_strategy {
        DryRunStrategy::HoldSharp | DryRunStrategy::HoldCalibrated => {
            vec![Box::new(SharpDigital::new(cfg.models.sharpness))]
        }
        DryRunStrategy::Blend => build_models(cfg.models.enable_cos_vg, cfg.models.sharpness),
    }
}

fn risk_limits(cfg: &AppConfig) -> RiskLimits {
    RiskLimits {
        min_edge: cfg.risk.min_edge,
        max_stake_per_window: cfg.risk.max_stake_per_window,
        max_open_exposure: cfg.risk.max_open_exposure,
        max_daily_loss: cfg.risk.max_daily_loss,
        no_trade_last_secs: cfg.risk.no_trade_last_secs,
        max_model_disagreement: cfg.risk.max_model_disagreement,
        max_decision_rate_per_minute: cfg.risk.max_decision_rate_per_minute,
        max_market_loss: cfg.risk.max_market_loss,
        max_daily_notional: cfg.risk.max_daily_notional,
        min_depth: cfg.risk.min_depth,
        max_spread: cfg.risk.max_spread,
    }
}

fn new_adaptive_edge(cfg: &AppConfig) -> AdaptiveEdge {
    AdaptiveEdge::from_params(AdaptiveEdgeParams {
        fraction: cfg.risk.adaptive_edge_fraction,
        floor: cfg.risk.min_edge,
        ceil: cfg.risk.adaptive_edge_ceil,
        lambda: cfg.risk.adaptive_edge_lambda,
        quantile: cfg.risk.adaptive_edge_quantile,
        max_samples: cfg.risk.adaptive_edge_window,
        mode: cfg.risk.adaptive_edge_mode,
        instability_buffer: cfg.risk.adaptive_edge_instability_buffer,
    })
}

fn trade_record_base(
    t0_millis: i64,
    strategy: String,
    mode: &str,
    detail: String,
    outcome_up: bool,
    won: bool,
    pnl: f64,
) -> app::state::TradeRecord {
    app::state::TradeRecord {
        t0_millis,
        strategy,
        mode: mode.to_string(),
        detail,
        outcome_up,
        won,
        pnl,
        strategy_version: STRATEGY_VERSION.to_string(),
        entry_edge: None,
        spread: None,
        book_depth: None,
        entry_timestamp: None,
        entry_price: None,
        notional: None,
        model_p_yes: None,
    }
}

fn dry_run_strategy_fair(
    strategy: DryRunStrategy,
    st: &DashboardState,
    min_cal_samples: usize,
) -> Option<f64> {
    let pick = |name: &str| st.models.iter().find(|m| m.name == name).map(|m| m.p_yes);
    match strategy {
        DryRunStrategy::HoldSharp => pick("sharp"),
        DryRunStrategy::HoldCalibrated if st.platt_samples >= min_cal_samples => pick("calibrated"),
        DryRunStrategy::HoldCalibrated => None,
        DryRunStrategy::Blend => pick("blend"),
    }
}

fn best_buy_token_trade(
    fair_yes: f64,
    book: &market::OrderBook,
    budget_usd: f64,
    min_edge: f64,
) -> Option<(bool, TradeDecision)> {
    let fills = book.simulate_dollar_sides(budget_usd);
    let yes = fills.buy_yes.and_then(|f| {
        let edge = fair_yes - f.avg_price;
        (edge >= min_edge && f.avg_price > 0.0).then(|| {
            (
                true,
                TradeDecision {
                    side: Side::Buy,
                    size: (budget_usd / f.avg_price).min(f.filled),
                    vwap: f.avg_price,
                    edge,
                },
            )
        })
    });
    let no = fills.sell_yes.and_then(|f| {
        let no_price = 1.0 - f.avg_price;
        let edge = (1.0 - fair_yes) - no_price;
        (edge >= min_edge && no_price > 0.0).then(|| {
            (
                false,
                TradeDecision {
                    side: Side::Buy,
                    size: (budget_usd / no_price).min(f.filled),
                    vwap: no_price,
                    edge,
                },
            )
        })
    });
    [yes, no].into_iter().flatten().max_by(|(_, a), (_, b)| {
        a.edge
            .partial_cmp(&b.edge)
            .unwrap_or(std::cmp::Ordering::Equal)
    })
}

async fn discover_active_market(window_secs: i64) -> Option<DiscoveredMarket> {
    match market::gamma::fetch_current_btc_updown(now_millis(), window_secs).await {
        Ok(Some(market)) => return Some(market),
        Ok(None) => {}
        Err(e) => warn!(error = %e, "current market discovery failed"),
    }
    match market::gamma::fetch_active_btc_updown().await {
        Ok(mut markets) => {
            markets.sort_by(|a, b| a.end_date.cmp(&b.end_date));
            markets.into_iter().next()
        }
        Err(e) => {
            warn!(error = %e, "market discovery failed");
            None
        }
    }
}

/// `RUST_LOG` sets the filter; `LOG_FORMAT=json` switches to one JSON object
/// per line (with the structured fields, e.g. `feed`, `retry_in_ms`) for log
/// shippers.
fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,hyper=warn,reqwest=warn"));
    if std::env::var("LOG_FORMAT").is_ok_and(|v| v.eq_ignore_ascii_case("json")) {
        tracing_subscriber::fmt()
            .json()
            .with_current_span(false)
            .with_env_filter(filter)
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();
    let cfg = AppConfig::from_env_args(std::env::args())?;
    info!(mode = cfg.mode.as_str(), "starting app");

    let buffers = Arc::new(Mutex::new(Buffers {
        chainlink: PriceBuffer::new(1_200),
        // ~1s sampling (see ingest below) over ~10 min of history → real intra-
        // window resolution for vol estimation and lead-lag (was 5s bars / 180 cap,
        // far too coarse to see a sub-second lead).
        binance: PriceBuffer::new(640),
        last_chainlink_millis: 0,
        last_binance_millis: 0,
        last_imbalance: 0.0,
    }));
    let flags = Arc::new(Mutex::new(ControlFlags {
        dry_run_enabled: cfg.mode != RunMode::Shadow,
        killed: false,
        paused: false,
    }));
    let (tx, rx) = watch::channel(DashboardState::default());

    let rtds_metrics = Arc::new(FeedMetrics::new("rtds", RTDS_STALE_MILLIS));
    let binance_metrics = Arc::new(FeedMetrics::new("binance", BINANCE_STALE_MILLIS));
    let book_metrics = Arc::new(FeedMetrics::new("clob_book", BOOK_STALE_MILLIS));
    // Optional raw-frame recorder (RECORD_PATH). Feed tasks never block on it:
    // a full queue drops the frame and counts it (see feeds::recorder).
    let recorder = match std::env::var("RECORD_PATH") {
        Ok(path) if !path.is_empty() => {
            let (handle, task) = feeds::recorder::start_file(&path, 65_536)?;
            info!(path = %path, "recording raw frames");
            Some((handle, task))
        }
        _ => None,
    };
    let rec_handle = recorder.as_ref().map(|(h, _)| h.clone());
    let with_tap = |opts: ConnOpts, source: Source| match &rec_handle {
        Some(h) => opts.recording(h.clone(), source),
        None => opts,
    };
    let mut metrics = Metrics::new(vec![
        rtds_metrics.clone(),
        binance_metrics.clone(),
        book_metrics.clone(),
    ]);
    metrics.recorder = recorder.as_ref().map(|(h, _)| h.stats.clone());
    let metrics = Arc::new(metrics);
    let mut feed_tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    {
        let buffers = buffers.clone();
        let url = cfg.feeds.rtds_url.clone();
        let opts = with_tap(ConnOpts::with_metrics(rtds_metrics.clone()), Source::Rtds);
        feed_tasks.push(tokio::spawn(async move {
            let on_tick = move |t: feeds::Tick| {
                if let Ok(mut b) = buffers.lock() {
                    match t.source {
                        FeedSource::Chainlink => {
                            b.last_chainlink_millis = t.ts_millis.max(now_millis());
                            b.chainlink.push(t.ts_millis, t.price);
                        }
                        FeedSource::Binance => {
                            b.last_binance_millis = t.ts_millis.max(now_millis());
                            b.binance.push(t.ts_millis, t.price);
                        }
                        _ => {}
                    }
                }
            };
            feeds::rtds::run_reconnecting(&url, opts, on_tick).await;
        }));
    }

    // Direct Binance perp feed drives the volatility estimate (the RTDS Chainlink
    // feed is smoothed and under-states vol). We sample the constantly-updating
    // bookTicker mid (and any trades), throttled to ~5/s so the buffer holds
    // minutes of history rather than seconds.
    {
        let buffers = buffers.clone();
        let url = cfg.feeds.binance_url.clone();
        let opts = with_tap(
            ConnOpts::with_metrics(binance_metrics.clone()),
            Source::Binance,
        );
        feed_tasks.push(tokio::spawn(async move {
            let mut last_push = 0i64;
            let on_event = move |ev: feeds::binance::BinanceEvent| {
                // Queue imbalance from every bookTicker (order-flow signal).
                let imbalance = match &ev {
                    feeds::binance::BinanceEvent::Book(b) => {
                        let tot = b.bid_qty + b.ask_qty;
                        (tot > 0.0).then(|| (b.bid_qty - b.ask_qty) / tot)
                    }
                    _ => None,
                };
                let price = match ev {
                    feeds::binance::BinanceEvent::Trade(t) => t.price,
                    feeds::binance::BinanceEvent::Book(b) => 0.5 * (b.bid + b.ask),
                };
                // Use wall-clock time for both cadence and storage: bookTicker
                // frames have no `T` field (exchange ts = 0), which would break a
                // cadence based on the event timestamp. Sample ~1s bars so vol
                // and lead-lag have intra-window resolution; imbalance every tick.
                let ts = now_millis();
                let push = price > 0.0 && ts - last_push >= 1_000;
                if push {
                    last_push = ts;
                }
                if imbalance.is_some() || push {
                    if let Ok(mut b) = buffers.lock() {
                        if let Some(im) = imbalance {
                            b.last_imbalance = im;
                        }
                        if push {
                            b.last_binance_millis = ts;
                            b.binance.push(ts, price);
                        }
                    }
                }
            };
            feeds::binance::run_reconnecting(&url, opts, on_event).await;
        }));
    }

    let active_market = Arc::new(Mutex::new(None::<DiscoveredMarket>));
    {
        let active_market = active_market.clone();
        let window_secs = cfg.window_secs;
        tokio::spawn(async move {
            loop {
                if let Some(market) = discover_active_market(window_secs).await {
                    if let Ok(mut slot) = active_market.lock() {
                        *slot = Some(market);
                    }
                }
                // Fast cadence so the discovered market (and its book/tokens)
                // tracks the current 5-min window soon after each boundary.
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            }
        });
    }

    let book = Arc::new(Mutex::new(BookSlot::default()));
    {
        let book = book.clone();
        let active_market = active_market.clone();
        let token_override = cfg.execution.token_id_override.clone();
        let book_metrics = book_metrics.clone();
        let book_rec = rec_handle.clone();
        feed_tasks.push(tokio::spawn(async move {
            let mut backoff = Backoff::default();
            loop {
                let token_id = token_override.clone().or_else(|| {
                    active_market
                        .lock()
                        .ok()
                        .and_then(|m| m.as_ref().map(|m| m.yes_token_id.clone()))
                });
                if let Some(token_id) = token_id {
                    match clob_book::fetch_book(&token_id).await {
                        Ok(b) => {
                            if let Ok(mut slot) = book.lock() {
                                slot.token_id = Some(token_id.clone());
                                slot.book = Some(b);
                                slot.updated_millis = now_millis();
                                slot.exchange_ts_millis = None;
                            }
                        }
                        Err(e) => warn!(error = %e, "book fetch failed"),
                    }
                    let token_for_stream = token_id.clone();
                    let token_for_callback = token_for_stream.clone();
                    let book_for_stream = book.clone();
                    let active_market_for_check = active_market.clone();
                    let token_override_for_check = token_override.clone();
                    let mut opts = ConnOpts::with_metrics(book_metrics.clone());
                    if let Some(h) = &book_rec {
                        h.record(
                            Source::Control,
                            &serde_json::json!({ "subscribe": token_id }).to_string(),
                        );
                        opts = opts.recording(h.clone(), Source::ClobBook);
                    }
                    let started = std::time::Instant::now();
                    let mut stream = Box::pin(clob_book::stream_book_ws_with(
                        clob_book::CLOB_MARKET_WS,
                        &token_for_stream,
                        &opts,
                        move |update| {
                            if let Ok(mut slot) = book_for_stream.lock() {
                                slot.token_id = Some(token_for_callback.clone());
                                slot.book = Some(update.book);
                                slot.updated_millis = now_millis();
                                slot.exchange_ts_millis = update.exchange_ts_millis;
                            }
                        },
                    ));
                    loop {
                        tokio::select! {
                            res = &mut stream => {
                                // Drop, silence or book desync: back off, then the
                                // outer loop refetches a REST snapshot and resubscribes.
                                book_metrics.set_connected(false);
                                book_metrics.record_reconnect();
                                let delay = backoff.on_session_end(started.elapsed());
                                warn!(
                                    feed = "clob_book",
                                    error = ?res.err(),
                                    retry_in_ms = delay.as_millis() as u64,
                                    "feed disconnected; reconnecting"
                                );
                                tokio::time::sleep(delay).await;
                                break;
                            }
                            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                                let latest_token = token_override_for_check.clone().or_else(|| {
                                    active_market_for_check
                                        .lock()
                                        .ok()
                                        .and_then(|m| m.as_ref().map(|m| m.yes_token_id.clone()))
                                });
                                if latest_token.as_deref() != Some(token_id.as_str()) {
                                    break;
                                }
                            }
                        }
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
        }));
    }

    // Resume: load the persisted ledger; REBUILD the P&L-curve series from it (the
    // source of truth) rather than loading the stored history, so the graphs never
    // carry artifacts (e.g. the spurious drop-to-zero left by a reset). One point
    // per resolved window, cumulative realized per strategy per track.
    fn load_json_vec<T: serde::de::DeserializeOwned>(path: &str) -> Vec<T> {
        std::fs::read(path)
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default()
    }
    let trades_vec = load_json_vec::<app::state::TradeRecord>("shadow-data/trades.json");
    let history_vec = rebuild_history_from_ledger(&trades_vec, cfg.window_secs);
    let history = Arc::new(Mutex::new(history_vec));
    let trades = Arc::new(Mutex::new(trades_vec));
    {
        let n_t = trades.lock().map(|t| t.len()).unwrap_or(0);
        let n_h = history.lock().map(|h| h.len()).unwrap_or(0);
        if n_t > 0 {
            info!(
                trades = n_t,
                history_points = n_h,
                "resumed ledger; rebuilt P&L curves"
            );
        }
    }

    {
        let buffers = buffers.clone();
        // P0 fix: offload run_state.json writes off the engine task so blocking
        // fs::write doesn't stall the 250ms tick loop. The engine sends pre-serialized
        // bytes; this task does the actual write in spawn_blocking.
        let (persist_tx, mut persist_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(4); // bounded: drop if engine is ahead
        tokio::spawn(async move {
            while let Some(bytes) = persist_rx.recv().await {
                tokio::task::spawn_blocking(move || {
                    let dir = std::path::Path::new("shadow-data");
                    let _ = std::fs::create_dir_all(dir);
                    let tmp = dir.join("run_state.json.tmp");
                    if std::fs::write(&tmp, &bytes).is_ok() {
                        let _ = std::fs::rename(&tmp, dir.join("run_state.json"));
                    }
                })
                .await
                .ok();
            }
        });

        let flags = flags.clone();
        let book = book.clone();
        let active_market = active_market.clone();
        let history = history.clone();
        let trades = trades.clone();
        let cfg = cfg.clone();
        tokio::spawn(async move {
            let models = build_models_for_run(&cfg);
            let mut clock = WindowClock::new(cfg.window_secs);
            let mut rm = RiskManager::new(risk_limits(&cfg));
            // Resume accumulated tracker state from disk if present.
            let resumed: Option<RunState> = std::fs::read("shadow-data/run_state.json")
                .ok()
                .and_then(|b| serde_json::from_slice(&b).ok());
            let mut tracker = ResolutionTracker::default();
            let mut paper = StrategyPaperTracker::default();
            let mut active = ActivePaperTracker::default();
            let mut convergence = app::convergence::ConvergencePaperTracker::default();
            // Online recalibration for the learned-sharpness `calibrated` strategy:
            // fits a logistic map on the BlackScholes digital's (prediction, outcome)
            // pairs so the sharpening is learned from data, not a fixed constant.
            let mut platt = OnlinePlatt::default();
            // Dynamic-blend weights, learned online (DMA) from per-window Brier.
            let mut online_w = OnlineWeights::default();
            let mut dry_run_pnl = DryRunPnlTracker::default();
            let mut pending: Option<PendingWindow> = None;
            let mut last_t0 = i64::MIN;
            let mut clean_window = false; // S0 anchored at the true window open?
                                          // Exact-reference-price path: when Chainlink Data Streams credentials are
                                          // configured, fetch the precise settlement report at each window's open
                                          // and close (byte-identical to Polymarket). Without creds this is None and
                                          // we fall back to the RTDS relay. Credentials come from Chainlink (Data
                                          // Streams signup), NOT Polymarket: CHAINLINK_STREAMS_API_KEY/SECRET.
            let cl_exact: Option<(feeds::auth::Creds, String)> = if cfg.feeds.use_chainlink_streams
            {
                match (feeds::load_creds(), cfg.feeds.chainlink_feed_id.clone()) {
                    (Ok(c), Some(fid)) => {
                        info!("exact Chainlink Data Streams reference price ENABLED");
                        Some((c, fid))
                    }
                    _ => {
                        warn!("USE_CHAINLINK_STREAMS set but creds/feed_id missing; using RTDS");
                        None
                    }
                }
            } else {
                None
            };
            let cl_rest_base = feeds::Endpoints::mainnet().rest_base;
            let mut anchored_t0 = i64::MIN;
            let mut anchored_s0 = 0.0f64;
            let mut adaptive_hold = new_adaptive_edge(&cfg);
            let mut adaptive_active = new_adaptive_edge(&cfg);
            let mut adaptive_convergence = new_adaptive_edge(&cfg);
            // Resume Brier boards + learned calibration + adaptive edge from the
            // run-state snapshot (these can't be reconstructed from the ledger).
            if let Some(r) = resumed {
                tracker = r.tracker;
                platt = r.platt;
                adaptive_hold = if r.adaptive_hold.samples() > 0 {
                    r.adaptive_hold
                } else {
                    r.adaptive
                };
                adaptive_active = if r.adaptive_active.samples() > 0 {
                    r.adaptive_active
                } else {
                    adaptive_hold.clone()
                };
                adaptive_convergence = if r.adaptive_convergence.samples() > 0 {
                    r.adaptive_convergence
                } else {
                    adaptive_hold.clone()
                };
                online_w = r.online_w;
                dry_run_pnl = r.dry_run_pnl;
                info!(
                    resolved = tracker.resolved_count(),
                    platt_samples = platt.samples(),
                    "RESUMED Brier/calibration/adaptive state from disk"
                );
            }
            // Show component weights immediately (uniform until learning starts),
            // for both fresh and resumed runs.
            online_w.seed(&BLEND_COMPONENTS);
            // Realized P&L is rebuilt from the closed-trade LEDGER (the source of
            // truth) — so it survives even when in-memory tracker state was lost
            // (e.g. a crash, or a build without run_state.json). Grouped by mode.
            {
                let mut ledger = trades.lock().map(|t| t.clone()).unwrap_or_default();
                // Chronological order so the reconstructed equity path / drawdown is
                // correct (stable sort preserves intra-window push order).
                ledger.sort_by_key(|t| t.t0_millis);
                let by = |mode: &str| -> Vec<(String, f64, bool)> {
                    ledger
                        .iter()
                        .filter(|t| t.mode == mode)
                        .map(|t| (t.strategy.clone(), t.pnl, t.won))
                        .collect()
                };
                paper.restore_pnl(&by("hold"));
                active.restore_pnl(&by("active"));
                convergence.restore_pnl(&by("conv"));
                if !ledger.is_empty() {
                    info!(
                        ledger = ledger.len(),
                        "rebuilt realized P&L from trade ledger"
                    );
                }
            }
            let mut execution =
                ExecutionService::new(ExecMode::from(cfg.mode), cfg.execution.clone());
            // Per-window / per-day accounting (not lifetime).
            let mut last_dry_run_t0 = i64::MIN; // one dry-run decision per window guard
            let mut day_key = i64::MIN; // UTC-day bucket for daily_notional reset
            let mut daily_notional = 0.0f64;
            let mut window_exposure = 0.0f64;
            // Capture the scored prediction this many seconds before close (avoids
            // optimistic Brier bias from the near-deterministic tau->0 prediction).
            let score_lead_secs = (cfg.window_secs / 5).clamp(10, 90);
            let mut last_hist_millis = 0i64; // P&L time-series sampling clock
            let mut latency = RollingLatency::new(1_200);
            let mut interval =
                tokio::time::interval(std::time::Duration::from_millis(cfg.decision_tick_millis));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;
                let decision_start = std::time::Instant::now();
                let now = now_millis();
                let (chainlink, binance, lead_lag, feed_fresh, spot, cl_age, bn_age, imbalance) = {
                    let b = buffers.lock().unwrap_or_else(|e| e.into_inner());
                    // Copy returns out of the lock — the cache makes this O(1) for
                    // all subsequent callers in the same tick (skew, kurt, ewma_vol).
                    let cl_ret: Vec<f64> = b.chainlink.returns().to_vec();
                    let bn_ret: Vec<f64> = b.binance.returns().to_vec();
                    let ll = if cl_ret.len() >= 5 && bn_ret.len() >= 5 {
                        let n = cl_ret.len().min(bn_ret.len());
                        best_lag(&bn_ret[bn_ret.len() - n..], &cl_ret[cl_ret.len() - n..], 5)
                    } else {
                        0
                    };
                    let cl_age =
                        (b.last_chainlink_millis > 0).then_some(now - b.last_chainlink_millis);
                    let bn_age = (b.last_binance_millis > 0).then_some(now - b.last_binance_millis);
                    let fresh = cl_age.is_some_and(|age| age <= cfg.feeds.max_feed_age_millis);
                    let spot = b
                        .chainlink
                        .last()
                        .or_else(|| b.binance.last())
                        .unwrap_or(0.0);
                    (
                        b.chainlink.clone(),
                        b.binance.clone(),
                        ll,
                        fresh,
                        spot,
                        cl_age,
                        bn_age,
                        b.last_imbalance,
                    )
                };
                if spot <= 0.0 {
                    continue;
                }
                // Anchor S0 to the Chainlink price at the TRUE window open (the
                // buffered tick nearest t0), not whatever price we read when the
                // loop noticed the boundary — this is the market's "Price to Beat".
                let t0_boundary = clock.window_start(now);
                // S0 = the EXACT Chainlink Data Stream report at the window open when
                // credentials are configured (byte-identical to Polymarket's
                // settlement); otherwise the last RTDS report at/before the open (a
                // close approximation, limited by the relay's update granularity).
                // Fetched once per window boundary (not per tick), with safe fallback.
                if t0_boundary != anchored_t0 {
                    let rtds_s0 = chainlink.price_at_or_before(t0_boundary).unwrap_or(spot);
                    anchored_s0 = match &cl_exact {
                        Some((creds, feed_id)) => {
                            match feeds::chainlink::fetch_report_at(
                                &cl_rest_base,
                                creds,
                                feed_id,
                                t0_boundary / 1000,
                            )
                            .await
                            {
                                Ok(r) => r.price,
                                Err(e) => {
                                    warn!(?e, "exact S0 fetch failed; using RTDS");
                                    rtds_s0
                                }
                            }
                        }
                        None => rtds_s0,
                    };
                    anchored_t0 = t0_boundary;
                }
                let s0 = anchored_s0;
                let window = clock.tick(now, s0);
                let today = now / 86_400_000;
                if today != day_key {
                    day_key = today;
                    daily_notional = 0.0;
                }
                if window.t0_millis != last_t0 {
                    if let Some(mut p) = pending.take() {
                        // Only resolve windows we anchored at their true open
                        // (predictions are only captured when `clean`).
                        if !p.predictions.is_empty() {
                            if let Some(scored) = p.scored.take() {
                                p.predictions = scored;
                            }
                            // Settle on the Chainlink price at the TRUE window close
                            // (symmetric with S0 open-anchoring), not the current spot we
                            // happened to read when we noticed the boundary. The bet is
                            // sign(close - open); a sloppy close read flips marginal
                            // windows and contaminates every Brier score + paper P&L.
                            let close_millis = p.t0_millis + cfg.window_secs * 1000;
                            let rtds_close =
                                chainlink.price_at_or_before(close_millis).unwrap_or(spot);
                            // Exact settlement report at close when creds present.
                            let close_spot = match &cl_exact {
                                Some((creds, feed_id)) => feeds::chainlink::fetch_report_at(
                                    &cl_rest_base,
                                    creds,
                                    feed_id,
                                    close_millis / 1000,
                                )
                                .await
                                .map(|r| r.price)
                                .unwrap_or(rtds_close),
                                None => rtds_close,
                            };
                            // Polymarket resolves "Up" when close >= open (ties -> Up),
                            // per the market description; match that inclusive bound.
                            let outcome_up = close_spot >= p.s0;
                            tracker.resolve(&p, close_spot);
                            let holds = paper.resolve(p.t0_millis, outcome_up);
                            let actives = active.resolve(p.t0_millis, outcome_up);
                            let convs = convergence.resolve(p.t0_millis, outcome_up);
                            if let Ok(mut tr) = trades.lock() {
                                for h in holds {
                                    let mut row = trade_record_base(
                                        p.t0_millis,
                                        h.name,
                                        "hold",
                                        format!(
                                            "{} @{:.3} x{:.0} @t-{}s",
                                            h.side, h.entry, h.size, h.entry_secs_remaining
                                        ),
                                        outcome_up,
                                        h.won,
                                        h.pnl,
                                    );
                                    row.entry_edge = Some(h.entry_edge);
                                    row.spread = h.spread;
                                    row.book_depth = h.book_depth;
                                    row.entry_timestamp = Some(h.entry_millis);
                                    row.entry_price = Some(h.entry);
                                    row.notional = Some(h.entry * h.size);
                                    row.model_p_yes = Some(h.model_p_yes);
                                    tr.push(row);
                                }
                                for a in actives {
                                    tr.push(trade_record_base(
                                        p.t0_millis,
                                        a.name,
                                        "active",
                                        format!("{} trades, end pos {:.0}", a.trades, a.final_pos),
                                        outcome_up,
                                        a.won,
                                        a.pnl,
                                    ));
                                }
                                for c in convs {
                                    tr.push(trade_record_base(
                                        p.t0_millis,
                                        c.name,
                                        "conv",
                                        format!("{} end pos {:.0}", c.reason, c.final_pos),
                                        outcome_up,
                                        c.won,
                                        c.pnl,
                                    ));
                                }
                                let len = tr.len();
                                if len > 5_000 {
                                    tr.drain(0..len - 5_000);
                                }
                            }
                            // Train the online recalibrator on the `sharp` base's
                            // scored prediction (calibrated = Platt∘sharp).
                            if let Some((_, sharp_pred)) =
                                p.predictions.iter().find(|(n, _)| n == "sharp")
                            {
                                platt.update(*sharp_pred, outcome_up);
                            }
                            // Update the dynamic-blend weights (Hedge) from each
                            // component's scored prediction this window.
                            let comp_preds: Vec<(String, f64)> = p
                                .predictions
                                .iter()
                                .filter(|(n, _)| BLEND_COMPONENTS.contains(&n.as_str()))
                                .cloned()
                                .collect();
                            online_w.update(&comp_preds, outcome_up);
                        }
                    }
                    pending = Some(PendingWindow {
                        t0_millis: window.t0_millis,
                        s0: window.s0,
                        predictions: Vec::new(),
                        scored: None,
                    });
                    last_t0 = window.t0_millis;
                    window_exposure = 0.0;
                    adaptive_hold.roll_window(); // fold the just-closed window's max edge
                    adaptive_active.roll_window();
                    adaptive_convergence.roll_window();
                    // A window is "clean" (S0 == true open) only if we caught its
                    // boundary in real time. A window inherited mid-run at startup has a
                    // stale S0 and is skipped from trading/scoring.
                    clean_window = (now - window.t0_millis) < 2_000;
                }

                let (dry_run_enabled, killed, paused) = {
                    let f = flags.lock().unwrap_or_else(|e| e.into_inner());
                    (f.dry_run_enabled, f.killed, f.paused)
                };
                if killed {
                    rm.kill();
                } else {
                    rm.reset_kill();
                }

                let market_snapshot = active_market.lock().ok().and_then(|m| m.clone());
                // Deterministic slug for THIS window — display this (always in sync
                // with S0), not the possibly-laggy discovered slug.
                let expected_slug = format!(
                    "btc-updown-{}m-{}",
                    cfg.window_secs / 60,
                    window.t0_millis / 1000
                );
                // Only trade when the discovered market (whose book we use) is the
                // SAME window as our S0; otherwise the book is for a different
                // window and edge would be meaningless.
                let market_in_sync = cfg.execution.token_id_override.is_some()
                    || market_snapshot
                        .as_ref()
                        .is_some_and(|m| m.slug == expected_slug);
                let token_id = cfg.execution.token_id_override.clone().or_else(|| {
                    market_snapshot
                        .as_ref()
                        .filter(|m| m.slug == expected_slug)
                        .map(|m| m.yes_token_id.clone())
                });
                let book_snapshot = book.lock().unwrap_or_else(|e| e.into_inner()).clone();
                let book_matches_token = token_id
                    .as_deref()
                    .is_some_and(|tid| book_snapshot.token_id.as_deref() == Some(tid));
                let book_view = book_matches_token
                    .then_some(())
                    .and(book_snapshot.book.as_ref());
                let book_age = (book_matches_token && book_snapshot.updated_millis > 0)
                    .then_some(now - book_snapshot.updated_millis);
                let scores = tracker.scores();
                let alpha_tilt =
                    if cfg.mode == RunMode::DryRun && !cfg.models.enable_alpha_tilt_dry_run {
                        0.0
                    } else {
                        (lead_lag as f64 * 0.01).clamp(
                            -cfg.models.max_alpha_tilt_abs,
                            cfg.models.max_alpha_tilt_abs,
                        )
                    };
                let decision_count = execution.decision_count_since(now, 60_000);
                let mode_label = if dry_run_enabled {
                    cfg.mode.as_str()
                } else {
                    "Shadow"
                };

                let mut st = step(
                    &rm,
                    &StepCtx {
                        now_millis: now,
                        window,
                        chainlink: &chainlink,
                        vol_buffer: &binance,
                        lead_lag_steps: lead_lag,
                        book: book_view,
                        book_age_millis: book_age,
                        models: &models,
                        model_scores: &scores,
                        fee: 0.0,
                        trade_size: 50.0,
                        mode: mode_label,
                        dry_run_enabled,
                        killed,
                        shadow_fills: execution.shadow_fills(),
                        shadow_notional: execution.notional(),
                        open_exposure: window_exposure,
                        feed_fresh,
                        chainlink_age_millis: cl_age,
                        binance_age_millis: bn_age,
                        decision_count_last_minute: decision_count,
                        market_pnl: 0.0,
                        daily_notional,
                        market_slug: Some(expected_slug.as_str()),
                        yes_token_id: token_id.as_deref(),
                        no_token_id: market_snapshot.as_ref().map(|m| m.no_token_id.as_str()),
                        last_simulation_error: execution.last_error(),
                        tick_to_decision_micros: Some(decision_start.elapsed().as_micros()),
                        alpha_tilt,
                    },
                );

                let dry_run_mode = cfg.mode == RunMode::DryRun;
                let needs_calibrated = !dry_run_mode
                    || matches!(
                        cfg.execution.dry_run_strategy,
                        DryRunStrategy::HoldCalibrated | DryRunStrategy::Blend
                    );
                let needs_blend = !dry_run_mode
                    || matches!(cfg.execution.dry_run_strategy, DryRunStrategy::Blend);

                // Two derived strategies, appended BEFORE prediction-capture so they
                // are traded AND Brier-scored like the bases.
                let score_of = |name: &str| {
                    scores
                        .get(name)
                        .copied()
                        .map(|(b, l)| (Some(b), Some(l)))
                        .unwrap_or((None, None))
                };
                // `calibrated` = online-Platt recalibration of the BEST base (`sharp`).
                // Run finding: Platt on BlackScholes under-sharpened vs the fixed
                // sharp 2.5; recalibrating the strongest base is the right target.
                if needs_calibrated {
                    let sharp_p = st
                        .models
                        .iter()
                        .find(|m| m.name == "sharp")
                        .map(|m| m.p_yes)
                        .unwrap_or(0.5);
                    let p_cal = platt.apply(sharp_p);
                    let (brier, log_loss) = score_of("calibrated");
                    st.models.push(app::state::ModelQuote {
                        name: "calibrated".to_string(),
                        p_yes: if p_cal.is_finite() {
                            p_cal.clamp(0.0, 1.0)
                        } else {
                            0.5
                        },
                        weight: 0.0,
                        brier,
                        log_loss,
                    });
                }
                // Two forecast-combination strategies over the SAME components
                // (the bases + the recalibrated `calibrated`):
                //   `blend`     — FIXED equal weights (simple average: the robust
                //                 "hard to beat" baseline, Makridakis/Clemen).
                //   `blend_dyn` — DYNAMIC online weights (Hedge): up-weights whoever
                //                 forecasts best, keeps improving over time.
                if needs_blend {
                    let comps: Vec<(String, f64)> = BLEND_COMPONENTS
                        .iter()
                        .filter_map(|name| {
                            st.models
                                .iter()
                                .find(|m| m.name == *name)
                                .map(|m| (m.name.clone(), m.p_yes))
                        })
                        .collect();
                    let n = comps.len().max(1) as f64;
                    let p_fixed = comps.iter().map(|(_, p)| p).sum::<f64>() / n;
                    let blend_quotes: Vec<(&str, f64)> = if dry_run_mode {
                        vec![("blend", p_fixed)]
                    } else {
                        vec![("blend", p_fixed), ("blend_dyn", online_w.combine(&comps))]
                    };
                    for (name, p) in blend_quotes {
                        let (brier, log_loss) = score_of(name);
                        st.models.push(app::state::ModelQuote {
                            name: name.to_string(),
                            p_yes: if p.is_finite() {
                                p.clamp(0.0, 1.0)
                            } else {
                                0.5
                            },
                            weight: 0.0,
                            brier,
                            log_loss,
                        });
                    }
                }
                // Headline edge = the Brier-WEIGHTED likelihood (`blend`) vs the
                // market, not the naive equal-weight ensemble the engine used. The
                // blend is our best single aggregate estimate, so the edge we display,
                // adapt the threshold on, and paper-execute against, is the gap between
                // it and the book. Depth-aware via best_trade.
                if let Some(book_ref) = book_view {
                    if let Some(edge_p) = dry_run_strategy_fair(
                        cfg.execution.dry_run_strategy,
                        &st,
                        cfg.models.min_calibration_samples,
                    )
                    .or_else(|| {
                        st.models
                            .iter()
                            .find(|m| m.name == "blend")
                            .map(|m| m.p_yes)
                    }) {
                        match execution::best_trade(edge_p, book_ref, cfg.risk.trade_size_usd, 0.0)
                        {
                            Some(t) => {
                                st.edge = Some(t.edge);
                                st.edge_side = Some(format!("{:?}", t.side));
                            }
                            None => {
                                st.edge = None;
                                st.edge_side = None;
                            }
                        }
                    } else {
                        st.edge = None;
                        st.edge_side = None;
                    }
                }
                // Surface the raw book imbalance for the dashboard OFI gauge and the
                // learned recalibration parameters.
                st.book_imbalance = imbalance;
                st.platt_slope = platt.slope();
                st.platt_bias = platt.bias();
                st.platt_samples = platt.samples();
                st.blend_weights = online_w.snapshot();
                st.paused = paused;
                st.dry_run_strategy = cfg.execution.dry_run_strategy.as_str().to_string();
                st.dry_run_kind = "SIMULATED".into();
                st.dry_run_size_usd = cfg.risk.trade_size_usd;
                st.dry_run_min_profit_buffer = cfg.execution.min_profit_buffer;
                st.dry_run_latency_edge_buffer = cfg.execution.latency_edge_buffer;
                st.dry_run_clean_window = clean_window;
                st.dry_run_market_in_sync = market_in_sync;
                st.dry_run_attempted_this_window = last_dry_run_t0 == window.t0_millis;
                dry_run_pnl.settle_expired(window.t0_millis, spot);
                // Recompute the reference mean + disagreement over ALL strategies
                // (the engine computed them over the base models only).
                {
                    let ps: Vec<f64> = st.models.iter().map(|m| m.p_yes).collect();
                    let n = ps.len().max(1) as f64;
                    st.ensemble_p_yes = ps.iter().sum::<f64>() / n;
                    let hi = ps.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
                    let lo = ps.iter().cloned().fold(f64::INFINITY, f64::min);
                    st.model_disagreement = if hi.is_finite() && lo.is_finite() {
                        hi - lo
                    } else {
                        0.0
                    };
                }

                if !dry_run_mode && clean_window {
                    if let Some(p) = pending.as_mut() {
                        let preds: Vec<(String, f64)> = st
                            .models
                            .iter()
                            .map(|m| (m.name.clone(), m.p_yes))
                            .collect();
                        if p.scored.is_none() && st.seconds_remaining <= score_lead_secs {
                            p.scored = Some(preds.clone());
                        }
                        p.predictions = preds;
                    }
                }
                st.resolved_windows = tracker.resolved_count();
                st.mean_brier = tracker.mean_brier();
                st.s0_anchored = clean_window;
                // Dollar-denominated sizing: the trackers convert these budgets to
                // shares at the ACTUAL per-side entry price (so a NO bet costs the
                // same as a YES bet). Hold spends the full budget; Active steps in
                // 1/5 chunks up to the full budget of position value.
                let mid = book_view.and_then(|b| b.mid());
                let hold_budget = cfg.risk.trade_size_usd;

                // Adaptive edge is strategy-family specific. Hold/dry-run cares about
                // one-way executable edge, active taker must overcome one spread, and
                // convergence trades fair-vs-mid gaps rather than bid/ask edge.
                if let (Some(bid), Some(ask)) = (st.best_bid, st.best_ask) {
                    let book_mid = 0.5 * (bid + ask);
                    let spread = (ask - bid).max(0.0);
                    let mut best_hold_edge = 0.0_f64;
                    let mut best_active_edge = 0.0_f64;
                    let mut best_convergence_gap = 0.0_f64;
                    for m in &st.models {
                        let one_way_edge = (m.p_yes - ask).max(bid - m.p_yes).max(0.0);
                        best_hold_edge = best_hold_edge.max(one_way_edge);
                        best_active_edge = best_active_edge.max((one_way_edge - spread).max(0.0));
                        best_convergence_gap = best_convergence_gap.max((m.p_yes - book_mid).abs());
                    }
                    adaptive_hold.observe(best_hold_edge);
                    adaptive_active.observe(best_active_edge);
                    adaptive_convergence.observe(best_convergence_gap);
                }
                let hold_edge_threshold = if cfg.risk.adaptive_edge {
                    adaptive_hold.threshold()
                } else {
                    cfg.risk.min_edge
                };
                let active_edge_threshold = if cfg.risk.adaptive_edge {
                    adaptive_active.threshold()
                } else {
                    cfg.risk.min_edge
                };
                let convergence_edge_threshold = if cfg.risk.adaptive_edge {
                    adaptive_convergence.threshold()
                } else {
                    cfg.risk.min_edge
                };
                rm.set_min_edge(hold_edge_threshold);
                st.edge_threshold = hold_edge_threshold;
                st.typical_max_edge = adaptive_hold.typical_max();
                st.hold_edge_threshold = hold_edge_threshold;
                st.active_edge_threshold = active_edge_threshold;
                st.convergence_edge_threshold = convergence_edge_threshold;
                st.hold_typical_max_edge = adaptive_hold.typical_max();
                st.active_typical_max_edge = adaptive_active.typical_max();
                st.convergence_typical_max_edge = adaptive_convergence.typical_max();
                st.dry_run_edge_floor = hold_edge_threshold.max(
                    st.spread.unwrap_or(0.0).max(0.0)
                        + cfg.execution.latency_edge_buffer
                        + cfg.execution.min_profit_buffer,
                );

                // HOLD entries are gated by the adaptive EDGE threshold, not a crude
                // time window. Early in the window the move is small so fair sits near
                // 0.5 and edge is naturally tiny → no entry; the under-reaction edge
                // (fair vs the timid market) is captured WHENEVER it appears, which is
                // often mid-window, not only at the close. We record each entry's
                // time-to-close so the timing effect can be measured, not assumed.
                let mut conv_closes: Vec<app::convergence::ConvergenceClose> = Vec::new();
                if !dry_run_mode {
                    let active_unit_usd = cfg.risk.trade_size_usd / 5.0;
                    let active_max_usd = cfg.risk.trade_size_usd;
                    let secs_remaining = window.seconds_remaining(now);
                    let observe = |paper: &mut StrategyPaperTracker,
                                   active: &mut ActivePaperTracker,
                                   name: &str,
                                   fair: f64| {
                        paper.observe(
                            name,
                            PaperObservation {
                                now_millis: now,
                                t0_millis: window.t0_millis,
                                fair_yes: fair,
                                book: book_view,
                                min_edge: hold_edge_threshold,
                                max_spread: cfg.risk.max_spread,
                                budget_usd: hold_budget,
                                entry_secs_remaining: secs_remaining,
                            },
                        );
                        active.observe(
                            name,
                            ActiveObs {
                                t0_millis: window.t0_millis,
                                now_millis: now,
                                fair_yes: fair,
                                book: book_view,
                                edge: active_edge_threshold,
                                max_spread: cfg.risk.max_spread,
                                min_trade_interval_millis: 2_000,
                                unit_usd: active_unit_usd,
                                max_usd: active_max_usd,
                            },
                        );
                    };
                    // Each model is its own independent strategy (no weighted ensemble)
                    // — Hold, Active and Convergence are tracked per model. (Market-making
                    // was removed: non-viable at retail latency vs a public fast-moving
                    // fundamental — it was ~100% adverse selection.)
                    let fairs: Vec<(String, f64)> = st
                        .models
                        .iter()
                        .map(|m| (m.name.clone(), m.p_yes))
                        .collect();
                    // Convergence intraday: enter on a wide blend-vs-market gap, take
                    // profit when the price converges to our estimate. Entry must clear
                    // the round trip; exit fires once converged. Records both the
                    // convergence take-profits and the resolution-backstop settles.
                    let conv_entry_edge = (convergence_edge_threshold + 0.02).max(0.05);
                    let conv_exit_edge = 0.02;
                    if clean_window && market_in_sync && !paused {
                        for (name, fair) in &fairs {
                            observe(&mut paper, &mut active, name, *fair);
                            if name == "sharp"
                                && secs_remaining >= OPEN_REPRICE_MIN_SECONDS_REMAINING
                            {
                                paper.observe(
                                    OPEN_REPRICE_STRATEGY,
                                    PaperObservation {
                                        now_millis: now,
                                        t0_millis: window.t0_millis,
                                        fair_yes: *fair,
                                        book: book_view,
                                        min_edge: hold_edge_threshold,
                                        max_spread: cfg.risk.max_spread,
                                        budget_usd: hold_budget,
                                        entry_secs_remaining: secs_remaining,
                                    },
                                );
                            }
                            if let Some(c) = convergence.observe(
                                name,
                                app::convergence::ConvergenceObs {
                                    t0_millis: window.t0_millis,
                                    fair_yes: *fair,
                                    book: book_view,
                                    entry_edge: conv_entry_edge,
                                    exit_edge: conv_exit_edge,
                                    max_spread: cfg.risk.max_spread,
                                    budget_usd: hold_budget,
                                },
                            ) {
                                conv_closes.push(c);
                            }
                        }
                    }
                }
                // Record convergence take-profits (intra-window closes) to the ledger.
                if !conv_closes.is_empty() {
                    if let Ok(mut tr) = trades.lock() {
                        for c in &conv_closes {
                            tr.push(trade_record_base(
                                window.t0_millis,
                                c.name.clone(),
                                "conv",
                                format!("{} end pos {:.0}", c.reason, c.final_pos),
                                false, // intraday close, outcome not yet known
                                c.won,
                                c.pnl,
                            ));
                        }
                    }
                }
                if !dry_run_mode {
                    paper.mark_equity(mid);
                    active.mark_equity(mid);
                    convergence.mark_equity(mid);
                    st.strategy_pnls = paper
                        .snapshots(mid)
                        .into_iter()
                        .map(|s| StrategyPnl {
                            name: s.name,
                            realized_pnl: s.realized_pnl,
                            unrealized: s.unrealized,
                            max_drawdown: s.max_drawdown,
                            trades: s.trades,
                            wins: s.wins,
                            notional: s.notional,
                            open_side: s.open_side,
                            open_entry: s.open_entry,
                        })
                        .collect();
                    st.active_pnls = active
                        .snapshots(mid)
                        .into_iter()
                        .map(|s| app::state::ActivePnl {
                            name: s.name,
                            realized_pnl: s.realized_pnl,
                            unrealized: s.unrealized,
                            max_drawdown: s.max_drawdown,
                            trades: s.trades,
                            position: s.position,
                        })
                        .collect();
                    st.convergence_pnls = convergence
                        .snapshots(mid)
                        .into_iter()
                        .map(|s| app::state::ConvergencePnl {
                            name: s.name,
                            realized_pnl: s.realized_pnl,
                            unrealized: s.unrealized,
                            max_drawdown: s.max_drawdown,
                            trades: s.trades,
                            wins: s.wins,
                            position: s.position,
                        })
                        .collect();

                    // Dashboard shadow numbers aggregate the per-model paper trades
                    // (realistic one-per-window), not lifetime fills.
                    st.shadow_fills = st.strategy_pnls.iter().map(|s| s.trades).sum();
                    st.shadow_notional = st.strategy_pnls.iter().map(|s| s.notional).sum();
                }

                // Dry-run execution: record at most one simulated strategy decision
                // per window. The execution service has no signing or posting path.
                if dry_run_enabled
                    && cfg.mode == RunMode::DryRun
                    && clean_window
                    && market_in_sync
                    && st.risk == "Allow"
                    && last_dry_run_t0 != window.t0_millis
                {
                    if let (Some(book_ref), Some(yes_id), Some(no_id)) = (
                        book_view,
                        token_id.as_deref(),
                        market_snapshot.as_ref().map(|m| m.no_token_id.as_str()),
                    ) {
                        if let Some(exec_fair) = dry_run_strategy_fair(
                            cfg.execution.dry_run_strategy,
                            &st,
                            cfg.models.min_calibration_samples,
                        ) {
                            let spread_buffer = st.spread.unwrap_or(0.0).max(0.0);
                            // Require either the learned adaptive edge OR enough raw
                            // executable edge to still leave `min_profit_buffer`
                            // after spread and latency/slippage reserve.
                            let dry_run_edge_floor = st.dry_run_edge_floor.max(spread_buffer);
                            if let Some((is_yes, decision)) = best_buy_token_trade(
                                exec_fair,
                                book_ref,
                                cfg.risk.trade_size_usd,
                                dry_run_edge_floor,
                            ) {
                                let token = if is_yes { yes_id } else { no_id };
                                last_dry_run_t0 = window.t0_millis;
                                match execution.execute(token, &decision, now).await {
                                    Ok(report) => {
                                        st.last_simulation_error = report.error.clone();
                                        dry_run_pnl.record_fill(DryRunFillInput {
                                            t0_millis: window.t0_millis,
                                            s0: window.s0,
                                            is_yes,
                                            matched_size: report.matched_size,
                                            matched_notional: report.matched_notional,
                                            fill_status: "simulated",
                                            decision_id: Some(&report.idempotency_key),
                                            fill_price: decision.vwap,
                                            ts_millis: now,
                                        });
                                        window_exposure += report.matched_notional;
                                        daily_notional += report.matched_notional;
                                    }
                                    Err(e) => {
                                        error!(error = %e, "execution failed");
                                        st.last_simulation_error = Some(e.to_string());
                                    }
                                }
                            }
                        }
                    }
                }

                let dry_run_view = dry_run_pnl.snapshot(now, book_view);
                st.dry_run_position_side = dry_run_view.side;
                st.dry_run_position_shares = dry_run_view.shares;
                st.dry_run_avg_entry = dry_run_view.avg_entry;
                st.dry_run_mark_price = dry_run_view.mark;
                st.dry_run_realized_pnl = dry_run_view.realized;
                st.dry_run_unrealized_pnl = dry_run_view.unrealized;
                st.dry_run_equity = dry_run_view.equity;
                st.dry_run_max_drawdown = dry_run_view.max_drawdown;
                st.dry_run_trade_count = dry_run_pnl.trade_count;
                st.dry_run_win_count = dry_run_pnl.win_count;
                st.dry_run_notional = dry_run_pnl.notional;
                st.dry_run_last_fill_status = dry_run_pnl.last_fill_status.clone();
                st.dry_run_last_decision_id = dry_run_pnl.last_decision_id.clone();
                st.dry_run_last_fill_price = dry_run_pnl.last_fill_price;
                st.dry_run_last_fill_ts_millis = dry_run_pnl.last_fill_ts_millis;
                st.dry_run_pnl_curve = dry_run_view.curve;

                // Sample the realized-P&L time series every 10s for the graphs.
                if now - last_hist_millis >= 10_000 {
                    last_hist_millis = now;
                    let point = app::state::PnlPoint {
                        t_millis: now,
                        // Paper equity = realized + current open mark-to-market, so
                        // the curve moves intra-window (not just at resolution).
                        hold: st
                            .strategy_pnls
                            .iter()
                            .map(|s| (s.name.clone(), s.realized_pnl + s.unrealized))
                            .collect(),
                        active: st
                            .active_pnls
                            .iter()
                            .map(|s| (s.name.clone(), s.realized_pnl + s.unrealized))
                            .collect(),
                        convergence: st
                            .convergence_pnls
                            .iter()
                            .map(|s| (s.name.clone(), s.realized_pnl + s.unrealized))
                            .collect(),
                        mean_brier: st.mean_brier,
                    };
                    if let Ok(mut h) = history.lock() {
                        h.push(point);
                        let len = h.len();
                        if len > 3_000 {
                            h.drain(0..len - 3_000); // ~8h at 10s cadence
                        }
                    }
                    // Persist accumulated tracker state for resume-on-restart. Cloned
                    // (all trackers are Clone); written atomically via a temp file.
                    let rs = RunState {
                        tracker: tracker.clone(),
                        paper: paper.clone(),
                        active: active.clone(),
                        convergence: convergence.clone(),
                        platt: platt.clone(),
                        adaptive: adaptive_hold.clone(),
                        adaptive_hold: adaptive_hold.clone(),
                        adaptive_active: adaptive_active.clone(),
                        adaptive_convergence: adaptive_convergence.clone(),
                        online_w: online_w.clone(),
                        dry_run_pnl: dry_run_pnl.clone(),
                    };
                    if let Ok(j) = serde_json::to_vec(&rs) {
                        // Non-blocking send: if the persist task is busy, drop this
                        // snapshot (the next resolution will produce a fresh one).
                        let _ = persist_tx.try_send(j);
                    }
                }

                let decision_micros = decision_start.elapsed().as_micros();
                latency.observe_tick_to_decision(decision_micros);
                latency.observe_book_age(book_age);
                latency.observe_chainlink_age(cl_age);
                latency.observe_binance_age(bn_age);
                st.tick_to_decision_micros = Some(decision_micros);
                st.latency = latency.snapshot();
                let _ = tx.send(st);
            }
        });
    }

    // Persistence: dump history + trades + a summary to disk every 30s so a 6-hour
    // unattended run survives a crash and the data is easy to hand off.
    {
        let history = history.clone();
        let trades = trades.clone();
        let rx_p = rx.clone();
        tokio::spawn(async move {
            let dir = std::path::Path::new("shadow-data");
            let _ = std::fs::create_dir_all(dir);
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                interval.tick().await;
                // Serialise on the async task (cheap CPU), then hand off disk I/O to a
                // blocking thread so we never stall the tokio worker on a slow write.
                let hbytes = history
                    .lock()
                    .ok()
                    .and_then(|h| serde_json::to_vec(&*h).ok());
                let tbytes = trades
                    .lock()
                    .ok()
                    .and_then(|t| serde_json::to_vec(&*t).ok());
                let sbytes = serde_json::to_vec(&rx_p.borrow().clone()).ok();
                let dir2 = dir;
                tokio::task::spawn_blocking(move || {
                    if let Some(b) = hbytes {
                        let _ = std::fs::write(dir2.join("history.json"), b);
                    }
                    if let Some(b) = tbytes {
                        let _ = std::fs::write(dir2.join("trades.json"), b);
                    }
                    if let Some(b) = sbytes {
                        let _ = std::fs::write(dir2.join("latest_state.json"), b);
                    }
                })
                .await
                .ok();
            }
        });
    }

    let shared = Shared {
        rx,
        flags,
        history,
        trades,
        fills_path: cfg.execution.persist_fills_path.clone(),
        control_token: cfg.control_token.clone().map(Into::into),
        metrics,
    };
    let app = router(shared);
    println!(
        "dashboard: http://{}  (mode: {})",
        cfg.bind_addr,
        cfg.mode.as_str()
    );
    let listener = tokio::net::TcpListener::bind(&cfg.bind_addr).await?;
    // Graceful shutdown on SIGINT/SIGTERM: stop accepting requests and let
    // in-flight ones finish, cancel feed tasks (closing their sockets), then
    // drain and fsync the recorder before exiting.
    // Open dashboard WebSockets never finish on their own, so in-flight
    // connections get a 5 s grace period before the server is dropped.
    let (stop_tx, mut stop_rx) = watch::channel(false);
    let server = axum::serve(listener, app).with_graceful_shutdown(async move {
        let signal = app::lifecycle::shutdown_signal().await;
        info!(signal, "shutdown requested");
        let _ = stop_tx.send(true);
    });
    tokio::select! {
        res = server => res?,
        _ = async {
            let _ = stop_rx.wait_for(|stop| *stop).await;
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        } => warn!("grace period over; closing remaining connections"),
    }
    for t in &feed_tasks {
        t.abort();
    }
    for t in feed_tasks {
        let _ = t.await;
    }
    info!("feed tasks stopped");
    drop(rec_handle);
    if let Some((handle, task)) = recorder {
        let dropped = handle
            .stats
            .dropped
            .load(std::sync::atomic::Ordering::Relaxed);
        drop(handle);
        match tokio::task::spawn_blocking(move || task.finish()).await {
            Ok(Ok((records, bytes))) => info!(records, bytes, dropped, "recorder flushed"),
            Ok(Err(e)) => error!(error = %e, "recorder flush failed"),
            Err(e) => error!(error = %e, "recorder task failed"),
        }
    }
    info!("shutdown complete");
    Ok(())
}
