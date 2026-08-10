//! Tracks 5-minute window resolutions and scores each model continuously.

use market::OrderBook;
use poly_core::ResolvedWindow;
use pricing::Scoreboard;
use std::collections::HashMap;

/// Is the book healthy enough to trade against? Rejects empty, crossed, or
/// wide-spread books (transient at window open) so shadow fills match what would
/// actually be achievable in a paper simulation. `max_spread` is in probability units.
fn book_tradeable(book: &OrderBook, max_spread: f64) -> bool {
    match (book.best_bid(), book.best_ask()) {
        (Some(bid), Some(ask)) => ask > bid && (ask - bid) <= max_spread,
        _ => false,
    }
}

/// One window's per-model predictions, captured before it resolves.
#[derive(Debug, Clone)]
pub struct PendingWindow {
    pub t0_millis: i64,
    pub s0: f64,
    pub predictions: Vec<(String, f64)>, // (model name, p_yes) — latest, for display
    /// Predictions captured once at a fixed lead time before close. Scoring uses
    /// these (not the near-deterministic tau->0 prediction) to avoid optimistic
    /// Brier bias. `None` until the lead time is reached.
    pub scored: Option<Vec<(String, f64)>>,
}

/// Accumulates resolved windows and per-model Brier scores.
#[derive(Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct ResolutionTracker {
    boards: HashMap<String, Scoreboard>,
    resolved: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
enum PaperSide {
    Up,
    Down,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PaperPosition {
    t0_millis: i64,
    side: PaperSide,
    entry: f64,
    size: f64,
    #[serde(default)]
    entry_millis: i64,
    #[serde(default)]
    entry_edge: f64,
    #[serde(default)]
    spread: Option<f64>,
    #[serde(default)]
    book_depth: Option<f64>,
    #[serde(default)]
    model_p_yes: f64,
    /// Seconds-to-close when this position was opened — lets us measure whether the
    /// under-reaction edge actually lives late or mid-window (instead of assuming).
    entry_secs_remaining: i64,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct PaperStats {
    realized_pnl: f64,
    trades: usize,
    wins: usize,
    notional: f64,
    peak: f64, // highest realized equity seen
    mdd: f64,  // realized-equity max drawdown = worst peak-to-trough drop
    open: Option<PaperPosition>,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct StrategyPaperTracker {
    stats: HashMap<String, PaperStats>,
}

#[derive(Debug, Clone, Copy)]
pub struct PaperObservation<'a> {
    pub now_millis: i64,
    pub t0_millis: i64,
    pub fair_yes: f64,
    pub book: Option<&'a OrderBook>,
    pub min_edge: f64,
    pub max_spread: f64,
    /// Dollar budget for the position. Shares are sized so that
    /// `entry_price * shares == budget_usd` for EITHER side, so a NO bet costs the
    /// same as a YES bet (previously NO bets used YES-mid shares -> wrong notional).
    pub budget_usd: f64,
    /// Seconds-to-close at this observation (recorded on the opened position).
    pub entry_secs_remaining: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StrategyPnlSnapshot {
    pub name: String,
    pub realized_pnl: f64,
    /// Mark-to-market of the currently-open position at the current mid (so the
    /// curve moves intra-window, not only at resolution). equity = realized + this.
    pub unrealized: f64,
    pub max_drawdown: f64,
    pub trades: usize,
    pub wins: usize,
    pub notional: f64,
    pub open_side: Option<String>,
    pub open_entry: Option<f64>,
}

/// Mark-to-market P&L of an open hold position at the current `mid`.
fn hold_unrealized(pos: &PaperPosition, mid: f64) -> f64 {
    match pos.side {
        PaperSide::Up => pos.size * (mid - pos.entry), // long YES, worth ~mid
        PaperSide::Down => pos.size * ((1.0 - mid) - pos.entry), // long NO, worth ~1-mid
    }
}

/// A closed hold-to-resolution trade for one strategy/window (for the ledger).
#[derive(Debug, Clone, PartialEq)]
pub struct HoldClose {
    pub name: String,
    pub side: &'static str, // "YES" or "NO"
    pub entry: f64,
    pub size: f64,
    pub entry_millis: i64,
    pub entry_edge: f64,
    pub spread: Option<f64>,
    pub book_depth: Option<f64>,
    pub model_p_yes: f64,
    pub won: bool,
    pub pnl: f64,
    pub entry_secs_remaining: i64,
}

impl StrategyPaperTracker {
    /// Drop any open positions (used on resume — stale windows never re-resolve).
    pub fn clear_open(&mut self) {
        for s in self.stats.values_mut() {
            s.open = None;
        }
    }

    /// Rebuild realized P&L / trades / wins / max-drawdown from the closed-trade
    /// ledger (the source of truth), so these survive even when in-memory tracker
    /// state was lost. `entries` MUST be in chronological order so the equity path
    /// (and thus peak/drawdown) is reconstructed correctly. MDD here is the
    /// realized-equity drawdown (coarser than the real-time tick-level mark, but faithful).
    pub fn restore_pnl(&mut self, entries: &[(String, f64, bool)]) {
        for (name, pnl, won) in entries {
            let s = self.stats.entry(name.clone()).or_default();
            s.realized_pnl += *pnl;
            s.trades += 1;
            if *won {
                s.wins += 1;
            }
            s.peak = s.peak.max(s.realized_pnl);
            s.mdd = s.mdd.max(s.peak - s.realized_pnl);
        }
    }

    pub fn observe(&mut self, name: &str, obs: PaperObservation) {
        let stats = self.stats.entry(name.to_string()).or_default();
        if stats
            .open
            .as_ref()
            .is_some_and(|p| p.t0_millis == obs.t0_millis)
        {
            return; // one position per window
        }
        let Some(book) = obs.book else {
            return;
        };
        if !book_tradeable(book, obs.max_spread) {
            return; // skip transient crossed/wide books at window open
        }
        let summary = book.summary();
        let (Some(_bid), Some(_ask)) = (book.best_bid(), book.best_ask()) else {
            return;
        };
        // Probe ~budget worth on each side for the slippage-adjusted VWAP (shares =
        // budget / touch price). Decide on the depth-walked VWAP, then size the
        // position to the dollar budget at the achieved entry (so notional == budget
        // for YES and NO alike).
        let fills = book.simulate_dollar_sides(obs.budget_usd);
        let buy_yes = fills.buy_yes;
        let sell_yes = fills.sell_yes;
        let candidate = buy_yes
            .filter(|f| obs.fair_yes - f.avg_price >= obs.min_edge)
            .map(|f| {
                (
                    PaperSide::Up,
                    f.avg_price,
                    f.filled,
                    obs.fair_yes - f.avg_price,
                )
            })
            .or_else(|| {
                sell_yes
                    .filter(|f| f.avg_price - obs.fair_yes >= obs.min_edge)
                    .map(|f| {
                        (
                            PaperSide::Down,
                            1.0 - f.avg_price,
                            f.filled,
                            f.avg_price - obs.fair_yes,
                        )
                    })
            });
        if let Some((side, entry, filled, entry_edge)) = candidate {
            if entry <= 0.0 {
                return;
            }
            // Exact dollar notional, never exceeding the fillable depth.
            let size = (obs.budget_usd / entry).min(filled);
            stats.open = Some(PaperPosition {
                t0_millis: obs.t0_millis,
                side,
                entry,
                size,
                entry_millis: obs.now_millis,
                entry_edge,
                spread: summary.map(|s| s.spread),
                book_depth: summary.map(|s| s.top_depth),
                model_p_yes: obs.fair_yes,
                entry_secs_remaining: obs.entry_secs_remaining,
            });
            stats.trades += 1;
            stats.notional += entry * size;
        }
    }

    pub fn resolve(&mut self, t0_millis: i64, outcome_up: bool) -> Vec<HoldClose> {
        let mut closed = Vec::new();
        for (name, stats) in self.stats.iter_mut() {
            if stats.open.as_ref().is_none_or(|p| p.t0_millis != t0_millis) {
                continue;
            }
            let Some(pos) = stats.open.take() else {
                continue;
            };
            let won = matches!(
                (pos.side, outcome_up),
                (PaperSide::Up, true) | (PaperSide::Down, false)
            );
            if won {
                stats.wins += 1;
            }
            let payoff = if won { 1.0 } else { 0.0 };
            let pnl = (payoff - pos.entry) * pos.size;
            stats.realized_pnl += pnl;
            stats.peak = stats.peak.max(stats.realized_pnl);
            stats.mdd = stats.mdd.max(stats.peak - stats.realized_pnl);
            closed.push(HoldClose {
                name: name.clone(),
                side: match pos.side {
                    PaperSide::Up => "YES",
                    PaperSide::Down => "NO",
                },
                entry: pos.entry,
                size: pos.size,
                entry_millis: pos.entry_millis,
                entry_edge: pos.entry_edge,
                spread: pos.spread,
                book_depth: pos.book_depth,
                model_p_yes: pos.model_p_yes,
                won,
                pnl,
                entry_secs_remaining: pos.entry_secs_remaining,
            });
        }
        closed
    }

    /// Hold-to-resolution reports realized-equity MDD only. Open mark-to-market P&L
    /// is still surfaced as `unrealized`, but it must not update MDD; otherwise a
    /// cheap option-like position can show a large common drawdown after giving
    /// back an intrawindow mark, even though the closed-trade equity curve did not.
    pub fn mark_equity(&mut self, _mid: Option<f64>) {}

    pub fn snapshots(&self, mid: Option<f64>) -> Vec<StrategyPnlSnapshot> {
        let mut out: Vec<_> = self
            .stats
            .iter()
            .map(|(name, stats)| StrategyPnlSnapshot {
                name: name.clone(),
                realized_pnl: stats.realized_pnl,
                unrealized: match (&stats.open, mid) {
                    (Some(p), Some(m)) => hold_unrealized(p, m),
                    _ => 0.0,
                },
                max_drawdown: stats.mdd,
                trades: stats.trades,
                wins: stats.wins,
                notional: stats.notional,
                open_side: stats.open.as_ref().map(|p| match p.side {
                    PaperSide::Up => "YES".to_string(),
                    PaperSide::Down => "NO".to_string(),
                }),
                open_entry: stats.open.as_ref().map(|p| p.entry),
            })
            .collect();
        out.sort_by(|a, b| {
            b.realized_pnl
                .partial_cmp(&a.realized_pnl)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        out
    }
}

/// Active intraday market-making paper strategy. Unlike the hold-to-resolution
/// tracker, this continuously moves a (signed, YES-equivalent) position toward
/// the model's edge: buy YES when it's cheap, sell held YES when it's rich, and
/// — since Polymarket has no short-selling — take short exposure by buying the
/// inverse NO token (and cover by selling NO). Profits only if fair value moves
/// more than the round-trip spread, which the paper comparison will reveal.
#[derive(Debug, Clone, Copy)]
pub struct ActiveObs<'a> {
    pub t0_millis: i64,
    pub now_millis: i64,
    pub fair_yes: f64,
    pub book: Option<&'a OrderBook>,
    pub edge: f64,                      // min edge vs the depth-aware fill to act
    pub max_spread: f64,                // skip crossed/wide books
    pub min_trade_interval_millis: i64, // cadence throttle (avoid 250ms churn)
    pub unit_usd: f64,                  // dollar notional traded per step
    pub max_usd: f64,                   // |position| value cap in dollars
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct ActivePos {
    t0_millis: i64,
    pos: f64,      // signed YES-equivalent units (>0 long YES, <0 long NO)
    cash: f64,     // net cash flow so far (buys negative, sells positive)
    trades: usize, // trades taken this window
}

/// Mark-to-market value of an open active position at the current `mid`. Equals the
/// running window P&L (cash + liquidation value), so the curve updates continuously.
fn active_unrealized(p: &ActivePos, mid: f64) -> f64 {
    let value = if p.pos >= 0.0 {
        p.pos * mid // long YES worth ~mid each
    } else {
        -p.pos * (1.0 - mid) // long NO worth ~(1-mid) each
    };
    p.cash + value
}

/// A closed active-intraday trade summary for one strategy/window (ledger).
#[derive(Debug, Clone, PartialEq)]
pub struct ActiveClose {
    pub name: String,
    pub trades: usize,
    pub final_pos: f64,
    pub won: bool,
    pub pnl: f64,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct ActiveStats {
    realized_pnl: f64,
    trades: usize,
    last_trade_millis: i64,
    peak: f64,
    mdd: f64,
    open: Option<ActivePos>,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ActivePaperTracker {
    stats: HashMap<String, ActiveStats>,
}

/// Per-strategy active-trading snapshot.
#[derive(Debug, Clone, PartialEq)]
pub struct ActivePnlSnapshot {
    pub name: String,
    pub realized_pnl: f64,
    /// Current mark-to-market of the open position. Equity = realized + this.
    pub unrealized: f64,
    pub max_drawdown: f64,
    pub trades: usize,
    pub position: f64,
}

impl ActivePaperTracker {
    /// Drop any open positions (used on resume).
    pub fn clear_open(&mut self) {
        for s in self.stats.values_mut() {
            s.open = None;
        }
    }

    /// Rebuild realized P&L / trades / max-drawdown from the closed-trade ledger.
    /// `entries` MUST be chronological so the drawdown path is correct.
    pub fn restore_pnl(&mut self, entries: &[(String, f64, bool)]) {
        for (name, pnl, _won) in entries {
            let s = self.stats.entry(name.clone()).or_default();
            s.realized_pnl += *pnl;
            s.trades += 1;
            s.peak = s.peak.max(s.realized_pnl);
            s.mdd = s.mdd.max(s.peak - s.realized_pnl);
        }
    }

    pub fn observe(&mut self, name: &str, o: ActiveObs) {
        let s = self.stats.entry(name.to_string()).or_default();
        let Some(book) = o.book else {
            return;
        };
        if !book_tradeable(book, o.max_spread) {
            return; // skip transient crossed/wide books
        }
        // Cadence throttle: don't churn every 250ms loop tick.
        if o.now_millis - s.last_trade_millis < o.min_trade_interval_millis {
            return;
        }
        if s.open.as_ref().is_none_or(|p| p.t0_millis != o.t0_millis) {
            s.open = Some(ActivePos {
                t0_millis: o.t0_millis,
                pos: 0.0,
                cash: 0.0,
                trades: 0,
            });
        }
        let p = s.open.as_mut().unwrap();
        let trades_before = p.trades;

        let (Some(bid), Some(ask)) = (book.best_bid(), book.best_ask()) else {
            return;
        };
        // Depth-aware marginal fill for ~one dollar unit per side (shares = $unit /
        // touch price), so each step deploys the same notional regardless of price.
        let fills = book.simulate_dollar_sides(o.unit_usd);
        let Some(buy) = fills.buy_yes else {
            return;
        };
        let Some(sell) = fills.sell_yes else {
            return;
        };
        let (buy_vwap, buy_fill) = (buy.avg_price, buy.filled);
        let (sell_vwap, sell_fill) = (sell.avg_price, sell.filled);
        // A taker must cover the ROUND TRIP: it crosses the spread on entry and will
        // cross it again on exit. Requiring only the one-way `edge` (as the hold
        // tracker does) means it enters on a thin edge then bleeds the spread back
        // out — the documented cause of the active strategy's losses. Require the
        // entry edge to clear the spread it will pay to unwind.
        let round_trip = o.edge + (ask - bid).max(0.0);
        let want_long = o.fair_yes - buy_vwap >= round_trip; // YES cheap past round-trip
        let want_short = sell_vwap - o.fair_yes >= round_trip; // YES rich past round-trip
                                                               // |position| value caps in shares at the current touch.
        let max_long = if ask > 0.0 { o.max_usd / ask } else { 0.0 };
        let max_short = if (1.0 - bid) > 0.0 {
            o.max_usd / (1.0 - bid)
        } else {
            0.0
        };

        if want_long {
            if p.pos >= 0.0 {
                // add to long, but never beyond the position-value cap
                let qty = buy_fill.min((max_long - p.pos).max(0.0));
                if qty > 0.0 {
                    p.cash -= buy_vwap * qty; // buy YES
                    p.pos += qty;
                    p.trades += 1;
                    s.trades += 1;
                }
            } else {
                // cover short: selling NO = hitting YES bid depth (sell_fill), not ask (buy_fill)
                let qty = sell_fill.min(-p.pos);
                if qty > 0.0 {
                    p.cash += (1.0 - sell_vwap) * qty; // sell NO at 1 - YES-bid VWAP
                    p.pos += qty;
                    p.trades += 1;
                    s.trades += 1;
                }
            }
        } else if want_short {
            if p.pos <= 0.0 {
                // add to short (buy NO), capped at the position-value cap
                let qty = sell_fill.min((max_short + p.pos).max(0.0));
                if qty > 0.0 {
                    p.cash -= (1.0 - sell_vwap) * qty; // buy NO at 1 - YES-bid VWAP
                    p.pos -= qty;
                    p.trades += 1;
                    s.trades += 1;
                }
            } else {
                // reduce long — only up to the amount actually held
                let qty = sell_fill.min(p.pos);
                if qty > 0.0 {
                    p.cash += sell_vwap * qty; // sell held YES
                    p.pos -= qty;
                    p.trades += 1;
                    s.trades += 1;
                }
            }
        }
        if p.trades > trades_before {
            s.last_trade_millis = o.now_millis;
        }
    }

    pub fn resolve(&mut self, t0_millis: i64, outcome_up: bool) -> Vec<ActiveClose> {
        let mut closed = Vec::new();
        for (name, s) in self.stats.iter_mut() {
            if s.open.as_ref().is_none_or(|p| p.t0_millis != t0_millis) {
                continue;
            }
            let p = s.open.take().unwrap();
            // Each remaining YES-equiv unit pays 1 on Up; each NO unit pays 1 on Down.
            let payoff = if outcome_up {
                p.pos.max(0.0)
            } else {
                (-p.pos).max(0.0)
            };
            let pnl = p.cash + payoff;
            s.realized_pnl += pnl;
            if p.trades > 0 {
                closed.push(ActiveClose {
                    name: name.clone(),
                    trades: p.trades,
                    final_pos: p.pos,
                    won: pnl > 0.0,
                    pnl,
                });
            }
        }
        closed
    }

    /// Update each strategy's running peak/drawdown from current equity.
    pub fn mark_equity(&mut self, mid: Option<f64>) {
        for s in self.stats.values_mut() {
            let unreal = match (&s.open, mid) {
                (Some(p), Some(m)) => active_unrealized(p, m),
                _ => 0.0,
            };
            let equity = s.realized_pnl + unreal;
            s.peak = s.peak.max(equity);
            s.mdd = s.mdd.max(s.peak - equity);
        }
    }

    pub fn snapshots(&self, mid: Option<f64>) -> Vec<ActivePnlSnapshot> {
        let mut out: Vec<_> = self
            .stats
            .iter()
            .map(|(name, s)| ActivePnlSnapshot {
                name: name.clone(),
                realized_pnl: s.realized_pnl,
                unrealized: match (&s.open, mid) {
                    (Some(p), Some(m)) => active_unrealized(p, m),
                    _ => 0.0,
                },
                max_drawdown: s.mdd,
                trades: s.trades,
                position: s.open.as_ref().map(|p| p.pos).unwrap_or(0.0),
            })
            .collect();
        // Sort by current P&L (realized + unrealized) so the leader is the current leader.
        out.sort_by(|a, b| {
            (b.realized_pnl + b.unrealized)
                .partial_cmp(&(a.realized_pnl + a.unrealized))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        out
    }
}

impl ResolutionTracker {
    /// Resolve a window with the realized close price; updates each model's board.
    /// Polymarket resolves "Up" when close >= open (ties -> Up), so use the same
    /// inclusive bound as the paper trackers for a consistent outcome.
    pub fn resolve(&mut self, pending: &PendingWindow, final_spot: f64) {
        let outcome_up = final_spot >= pending.s0;
        for (name, p) in &pending.predictions {
            self.boards
                .entry(name.clone())
                .or_insert_with(|| Scoreboard::new(0.0))
                .record(ResolvedWindow {
                    p_yes_predicted: *p,
                    outcome_up,
                });
        }
        self.resolved += 1;
    }

    pub fn resolved_count(&self) -> usize {
        self.resolved
    }

    pub fn brier(&self, name: &str) -> Option<f64> {
        self.boards.get(name).map(|b| b.brier())
    }

    pub fn log_loss(&self, name: &str) -> Option<f64> {
        self.boards.get(name).map(|b| b.log_loss())
    }

    pub fn scores(&self) -> HashMap<String, (f64, f64)> {
        self.boards
            .iter()
            .map(|(name, board)| (name.clone(), (board.brier(), board.log_loss())))
            .collect()
    }

    pub fn weights_for(&self, names: &[&str], min_samples: usize, beta: f64) -> Vec<f64> {
        if self.resolved < min_samples || names.is_empty() {
            return uniform(names.len());
        }
        let mut scores = Vec::with_capacity(names.len());
        for name in names {
            let Some(board) = self.boards.get(*name) else {
                return uniform(names.len());
            };
            scores.push(board.brier());
        }
        pricing::softmax_weights(&scores, beta)
    }

    /// Mean Brier across all tracked models (overall calibration).
    pub fn mean_brier(&self) -> f64 {
        if self.boards.is_empty() {
            return 0.0;
        }
        self.boards.values().map(|b| b.brier()).sum::<f64>() / self.boards.len() as f64
    }
}

fn uniform(n: usize) -> Vec<f64> {
    if n == 0 {
        Vec::new()
    } else {
        vec![1.0 / n as f64; n]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_abs_diff_eq;

    fn pending(s0: f64, p: f64) -> PendingWindow {
        PendingWindow {
            t0_millis: 0,
            s0,
            predictions: vec![("m".into(), p)],
            scored: None,
        }
    }

    // Deep book at the given touch so unit/size fills fully at the touch price.
    fn book(bid: f64, ask: f64) -> OrderBook {
        OrderBook {
            bids: vec![market::book::Level {
                price: bid,
                size: 1_000.0,
            }],
            asks: vec![market::book::Level {
                price: ask,
                size: 1_000.0,
            }],
        }
    }

    fn aobs<'a>(t0: i64, fair: f64, b: &'a OrderBook) -> ActiveObs<'a> {
        ActiveObs {
            t0_millis: t0,
            now_millis: t0,
            fair_yes: fair,
            book: Some(b),
            edge: 0.03,
            max_spread: 0.5,
            min_trade_interval_millis: 0,
            unit_usd: 5.5, // ~10 shares at the test's 0.55 touch
            max_usd: 50.0,
        }
    }

    fn paper_obs<'a>(t0: i64, fair: f64, b: &'a OrderBook) -> PaperObservation<'a> {
        PaperObservation {
            now_millis: t0,
            t0_millis: t0,
            fair_yes: fair,
            book: Some(b),
            min_edge: 0.01,
            max_spread: 0.5,
            budget_usd: 10.0,
            entry_secs_remaining: 60,
        }
    }

    #[test]
    fn active_buys_cheap_yes_and_profits_on_up() {
        let mut a = ActivePaperTracker::default();
        let b = book(0.50, 0.55);
        a.observe("m", aobs(1, 0.90, &b)); // YES cheap -> buy 10 @ .55
        a.resolve(1, true); // Up -> 10 payoff
        let s = &a.snapshots(None)[0];
        assert_eq!(s.trades, 1);
        assert_abs_diff_eq!(s.realized_pnl, 10.0 - 5.5, epsilon = 1e-9);
    }

    #[test]
    fn active_takes_profit_within_window_regardless_of_outcome() {
        let mut a = ActivePaperTracker::default();
        let b1 = book(0.50, 0.55);
        let b2 = book(0.65, 0.70);
        a.observe("m", aobs(1, 0.90, &b1)); // buy YES @ .55
        a.observe("m", aobs(1, 0.50, &b2)); // YES rich -> sell @ .65, flat
        a.resolve(1, false); // outcome irrelevant; profit already locked
        let s = &a.snapshots(None)[0];
        assert_eq!(s.trades, 2);
        assert_abs_diff_eq!(s.realized_pnl, 6.5 - 5.5, epsilon = 1e-9); // +1.0
        assert_abs_diff_eq!(s.position, 0.0, epsilon = 1e-12);
    }

    #[test]
    fn active_takes_short_via_inverse_no_token() {
        let mut a = ActivePaperTracker::default();
        let b = book(0.50, 0.55);
        // YES very rich vs fair -> short by buying NO at 1 - bid = 0.50
        a.observe("m", aobs(1, 0.10, &b));
        a.resolve(1, false); // Down -> NO pays 1 each
        let s = &a.snapshots(None)[0];
        assert_eq!(s.trades, 1);
        // $5.5 of NO at 0.50 = 11 shares; Down pays 11, cost 5.5 -> +5.5.
        assert_abs_diff_eq!(s.realized_pnl, 11.0 - 5.5, epsilon = 1e-9);
        assert_abs_diff_eq!(s.position, 0.0, epsilon = 1e-12);
    }

    #[test]
    fn hold_mdd_uses_realized_equity_not_intrawindow_marks() {
        let mut p = StrategyPaperTracker::default();
        let cheap_yes = book(0.01, 0.02);
        p.observe("m", paper_obs(1, 0.90, &cheap_yes));

        // A temporary mark near 1.0 creates a huge unrealized gain for a cheap YES,
        // then the mark falls back. Hold-to-resolution MDD should ignore that
        // intrawindow giveback and only update when closed P&L is realized.
        p.mark_equity(Some(0.99));
        p.mark_equity(Some(0.02));
        assert_abs_diff_eq!(p.snapshots(Some(0.02))[0].max_drawdown, 0.0, epsilon = 1e-9);

        p.resolve(1, false);
        let s = &p.snapshots(None)[0];
        assert_abs_diff_eq!(s.realized_pnl, -10.0, epsilon = 1e-9);
        assert_abs_diff_eq!(s.max_drawdown, 10.0, epsilon = 1e-9);
    }

    #[test]
    fn resolves_up_when_close_above_reference() {
        let mut t = ResolutionTracker::default();
        t.resolve(&pending(60_000.0, 1.0), 60_100.0); // predicted Up=1, outcome Up
        assert_eq!(t.resolved_count(), 1);
        assert_abs_diff_eq!(t.brier("m").unwrap(), 0.0, epsilon = 1e-12);
    }

    #[test]
    fn penalizes_wrong_confident_call() {
        let mut t = ResolutionTracker::default();
        t.resolve(&pending(60_000.0, 1.0), 59_900.0); // predicted Up=1, outcome Down
        assert_abs_diff_eq!(t.brier("m").unwrap(), 1.0, epsilon = 1e-12);
    }

    #[test]
    fn mean_brier_averages_models() {
        let mut t = ResolutionTracker::default();
        let w = PendingWindow {
            t0_millis: 0,
            s0: 60_000.0,
            predictions: vec![("a".into(), 1.0), ("b".into(), 0.0)],
            scored: None,
        };
        t.resolve(&w, 60_100.0); // outcome Up: a perfect (0), b wrong (1)
        assert_abs_diff_eq!(t.mean_brier(), 0.5, epsilon = 1e-12);
    }

    #[test]
    fn weights_fallback_until_min_samples() {
        let mut t = ResolutionTracker::default();
        t.resolve(&pending(60_000.0, 1.0), 60_100.0);
        let w = t.weights_for(&["m"], 20, 5.0);
        assert_abs_diff_eq!(w[0], 1.0, epsilon = 1e-12);
    }

    #[test]
    fn paper_tracker_records_and_settles_model_trades() {
        let mut t = StrategyPaperTracker::default();
        let b = book(0.50, 0.55); // YES cheap vs fair 0.70 -> buy YES @ .55
        t.observe(
            "m",
            PaperObservation {
                now_millis: 10_000,
                t0_millis: 10,
                fair_yes: 0.70,
                book: Some(&b),
                min_edge: 0.03,
                max_spread: 0.10,
                budget_usd: 55.0, // $55 / 0.55 = 100 shares
                entry_secs_remaining: 120,
            },
        );
        assert_eq!(t.snapshots(None)[0].open_side.as_deref(), Some("YES"));
        t.resolve(10, true);
        let s = t.snapshots(None);
        assert_eq!(s[0].trades, 1);
        assert_eq!(s[0].wins, 1);
        assert_abs_diff_eq!(s[0].realized_pnl, 45.0, epsilon = 1e-12); // (1-.55)*100
    }

    #[test]
    fn paper_fill_walks_depth_for_worse_vwap() {
        // Thin top (10 @ .55) then deeper (90 @ .60): buying 100 must average up.
        let mut t = StrategyPaperTracker::default();
        let thin = OrderBook {
            bids: vec![market::book::Level {
                price: 0.20,
                size: 100.0,
            }],
            asks: vec![
                market::book::Level {
                    price: 0.55,
                    size: 10.0,
                },
                market::book::Level {
                    price: 0.60,
                    size: 90.0,
                },
            ],
        };
        t.observe(
            "m",
            PaperObservation {
                now_millis: 1_000,
                t0_millis: 1,
                fair_yes: 0.90,
                book: Some(&thin),
                min_edge: 0.03,
                max_spread: 1.0,  // testing depth, not the spread guard
                budget_usd: 60.0, // probe 60/0.55=109 sh, book caps at 100 -> walks both levels
                entry_secs_remaining: 120,
            },
        );
        // entry VWAP = (10*.55 + 90*.60)/100 = .595, not the .55 top of book
        let entry = t.snapshots(None)[0].open_entry.unwrap();
        assert_abs_diff_eq!(entry, 0.595, epsilon = 1e-9);
    }
}
