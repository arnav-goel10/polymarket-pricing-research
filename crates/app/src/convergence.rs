//! Convergence (fair-value reversion) intraday strategy.
//!
//! Thesis: our weighted estimate (`blend`) is a better fair value than the market
//! quote, and — because the 5-minute market is *calibrated but not sharp* and
//! under-reacts (confirmed: Platt slope > 1) — the market price tends to drift
//! TOWARD our estimate over the window. So when the gap `fair - mid` is large we
//! take the cheap side, and we TAKE PROFIT once the price has converged to our
//! estimate (gap ≈ 0), recycling capital instead of waiting for resolution.
//!
//! This differs from the `active` taker (which only unwinds when the gap *flips*,
//! i.e. the price overshoots fair) and from `hold` (which waits for settlement).
//! It is a classic statistical-arbitrage / mean-reversion-to-fundamental trade
//! (cf. "Statistical Arbitrage in Binary Prediction Markets", SSRN 6446502).
//!
//! Risk: if our estimate is wrong the price never converges; a resolution backstop
//! settles any position still open at the close, so a wrong view costs at most the
//! binary outcome (like `hold`), while correct views are recycled faster.

use market::OrderBook;

#[derive(Debug, Clone, Copy)]
pub struct ConvergenceObs<'a> {
    pub t0_millis: i64,
    pub fair_yes: f64,
    pub book: Option<&'a OrderBook>,
    /// Enter when |fair - mid| >= this (must clear the round-trip spread to profit).
    pub entry_edge: f64,
    /// Take profit / flatten when |fair - mid| <= this (the price reached us).
    pub exit_edge: f64,
    pub max_spread: f64,
    pub budget_usd: f64,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct ConvPos {
    t0_millis: i64,
    pos: f64,  // signed YES-equivalent (>0 long YES, <0 long NO)
    cash: f64, // net cash flow
}

fn unrealized(p: &ConvPos, mid: f64) -> f64 {
    let value = if p.pos >= 0.0 {
        p.pos * mid
    } else {
        -p.pos * (1.0 - mid)
    };
    p.cash + value
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct ConvStats {
    realized_pnl: f64,
    trades: usize,
    wins: usize,
    peak: f64,
    mdd: f64,
    open: Option<ConvPos>,
    traded_t0: i64, // one round-trip per window
}

/// A closed convergence trade (ledger).
#[derive(Debug, Clone, PartialEq)]
pub struct ConvergenceClose {
    pub name: String,
    pub reason: &'static str, // "converged" or "resolved"
    pub final_pos: f64,
    pub won: bool,
    pub pnl: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConvergenceSnapshot {
    pub name: String,
    pub realized_pnl: f64,
    pub unrealized: f64,
    pub max_drawdown: f64,
    pub trades: usize,
    pub wins: usize,
    pub position: f64,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ConvergencePaperTracker {
    stats: std::collections::HashMap<String, ConvStats>,
}

fn book_tradeable(book: &OrderBook, max_spread: f64) -> bool {
    match (book.best_bid(), book.best_ask()) {
        (Some(bid), Some(ask)) => ask > bid && (ask - bid) <= max_spread,
        _ => false,
    }
}

impl ConvergencePaperTracker {
    /// Drop any open positions (used on resume).
    pub fn clear_open(&mut self) {
        for s in self.stats.values_mut() {
            s.open = None;
        }
    }

    /// Rebuild realized P&L / trades / wins / max-drawdown from the closed-trade
    /// ledger. `entries` MUST be chronological so the drawdown path is correct.
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

    pub fn observe(&mut self, name: &str, o: ConvergenceObs) -> Option<ConvergenceClose> {
        let s = self.stats.entry(name.to_string()).or_default();
        let book = o.book?;
        if !book_tradeable(book, o.max_spread) {
            return None;
        }
        let (Some(bid), Some(ask)) = (book.best_bid(), book.best_ask()) else {
            return None;
        };
        let mid = 0.5 * (bid + ask);
        let gap = o.fair_yes - mid;

        // Manage an open position: take profit once converged.
        if let Some(p) = s.open.as_mut().filter(|p| p.t0_millis == o.t0_millis) {
            if gap.abs() <= o.exit_edge {
                // Converged — flatten at the touch (taker exit).
                if p.pos > 0.0 {
                    p.cash += bid * p.pos;
                } else {
                    p.cash += (1.0 - ask) * (-p.pos);
                }
                let pnl = p.cash;
                let final_pos = p.pos;
                s.realized_pnl += pnl;
                if pnl > 0.0 {
                    s.wins += 1;
                }
                s.open = None;
                return Some(ConvergenceClose {
                    name: name.to_string(),
                    reason: "converged",
                    final_pos,
                    won: pnl > 0.0,
                    pnl,
                });
            }
            return None; // still waiting to converge
        }

        // No open position: enter once per window when the gap is wide enough.
        if s.traded_t0 == o.t0_millis {
            return None;
        }
        let fills = book.simulate_dollar_sides(o.budget_usd);
        if gap >= o.entry_edge {
            // YES cheap vs our fair -> buy YES, expecting the market to rise to fair.
            if let Some(f) = fills.buy_yes {
                let size = (o.budget_usd / f.avg_price).min(f.filled);
                if size > 0.0 {
                    s.open = Some(ConvPos {
                        t0_millis: o.t0_millis,
                        pos: size,
                        cash: -f.avg_price * size,
                    });
                    s.trades += 1;
                    s.traded_t0 = o.t0_millis;
                }
            }
        } else if -gap >= o.entry_edge {
            // YES rich vs our fair -> short via NO, expecting the price to fall to fair.
            if let Some(f) = fills.sell_yes {
                let no_price = 1.0 - f.avg_price;
                if no_price > 0.0 {
                    let size = (o.budget_usd / no_price).min(f.filled);
                    if size > 0.0 {
                        s.open = Some(ConvPos {
                            t0_millis: o.t0_millis,
                            pos: -size,
                            cash: -no_price * size,
                        });
                        s.trades += 1;
                        s.traded_t0 = o.t0_millis;
                    }
                }
            }
        }
        None
    }

    /// Settle any position still open at window close (resolution backstop).
    pub fn resolve(&mut self, t0_millis: i64, outcome_up: bool) -> Vec<ConvergenceClose> {
        let mut closed = Vec::new();
        for (name, s) in self.stats.iter_mut() {
            let Some(p) = s.open.take().filter(|p| p.t0_millis == t0_millis) else {
                continue;
            };
            let payoff = if outcome_up {
                p.pos.max(0.0)
            } else {
                (-p.pos).max(0.0)
            };
            let pnl = p.cash + payoff;
            s.realized_pnl += pnl;
            if pnl > 0.0 {
                s.wins += 1;
            }
            closed.push(ConvergenceClose {
                name: name.clone(),
                reason: "resolved",
                final_pos: p.pos,
                won: pnl > 0.0,
                pnl,
            });
        }
        closed
    }

    pub fn mark_equity(&mut self, mid: Option<f64>) {
        for s in self.stats.values_mut() {
            let unreal = match (&s.open, mid) {
                (Some(p), Some(m)) => unrealized(p, m),
                _ => 0.0,
            };
            let equity = s.realized_pnl + unreal;
            s.peak = s.peak.max(equity);
            s.mdd = s.mdd.max(s.peak - equity);
        }
    }

    pub fn snapshots(&self, mid: Option<f64>) -> Vec<ConvergenceSnapshot> {
        let mut out: Vec<_> = self
            .stats
            .iter()
            .map(|(name, s)| ConvergenceSnapshot {
                name: name.clone(),
                realized_pnl: s.realized_pnl,
                unrealized: match (&s.open, mid) {
                    (Some(p), Some(m)) => unrealized(p, m),
                    _ => 0.0,
                },
                max_drawdown: s.mdd,
                trades: s.trades,
                wins: s.wins,
                position: s.open.as_ref().map(|p| p.pos).unwrap_or(0.0),
            })
            .collect();
        out.sort_by(|a, b| {
            (b.realized_pnl + b.unrealized)
                .partial_cmp(&(a.realized_pnl + a.unrealized))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_abs_diff_eq;

    fn book(bid: f64, ask: f64) -> OrderBook {
        OrderBook {
            bids: vec![market::book::Level {
                price: bid,
                size: 10_000.0,
            }],
            asks: vec![market::book::Level {
                price: ask,
                size: 10_000.0,
            }],
        }
    }

    fn obs<'a>(t0: i64, fair: f64, b: &'a OrderBook) -> ConvergenceObs<'a> {
        ConvergenceObs {
            t0_millis: t0,
            fair_yes: fair,
            book: Some(b),
            entry_edge: 0.10,
            exit_edge: 0.02,
            max_spread: 0.5,
            budget_usd: 10.0,
        }
    }

    #[test]
    fn takes_profit_when_price_converges_up_to_fair() {
        let mut c = ConvergencePaperTracker::default();
        // fair 0.70, mid 0.50 -> gap 0.20 >= entry -> buy YES at ask 0.51.
        let close = c.observe("m", obs(1, 0.70, &book(0.49, 0.51)));
        assert!(close.is_none());
        assert!(c.snapshots(Some(0.5))[0].position > 0.0, "should be long");
        // price rises to mid 0.69 -> gap 0.01 <= exit -> flatten at bid 0.68.
        let close = c.observe("m", obs(1, 0.70, &book(0.68, 0.70)));
        let close = close.expect("should take profit on convergence");
        assert_eq!(close.reason, "converged");
        assert!(close.won && close.pnl > 0.0, "bought ~.51 sold ~.68");
        assert_abs_diff_eq!(c.snapshots(None)[0].position, 0.0, epsilon = 1e-9);
    }

    #[test]
    fn shorts_and_converges_down() {
        let mut c = ConvergencePaperTracker::default();
        // fair 0.30, mid 0.50 -> gap -0.20 -> short via NO (buy NO at 1-bid=0.51).
        c.observe("m", obs(1, 0.30, &book(0.49, 0.51)));
        assert!(c.snapshots(Some(0.5))[0].position < 0.0, "should be short");
        // price falls to mid 0.31 -> converged -> flatten (sell NO at 1-ask=0.68).
        let close = c
            .observe("m", obs(1, 0.30, &book(0.30, 0.32)))
            .expect("converged");
        assert!(close.won && close.pnl > 0.0);
    }

    #[test]
    fn no_entry_when_gap_small() {
        let mut c = ConvergencePaperTracker::default();
        c.observe("m", obs(1, 0.52, &book(0.49, 0.51))); // gap 0.02 < entry 0.10
        assert!(c.snapshots(None).is_empty() || c.snapshots(None)[0].position == 0.0);
    }

    #[test]
    fn one_round_trip_per_window() {
        let mut c = ConvergencePaperTracker::default();
        c.observe("m", obs(1, 0.70, &book(0.49, 0.51))); // enter
        c.observe("m", obs(1, 0.70, &book(0.68, 0.70))); // converged -> close
        let trades_after = c.snapshots(None)[0].trades;
        // a fresh wide gap in the SAME window must not re-enter
        c.observe("m", obs(1, 0.90, &book(0.49, 0.51)));
        assert_eq!(c.snapshots(None)[0].trades, trades_after);
    }

    #[test]
    fn resolution_backstop_settles_open_position() {
        let mut c = ConvergencePaperTracker::default();
        c.observe("m", obs(1, 0.70, &book(0.49, 0.51))); // long YES, never converges
        assert!(c.snapshots(Some(0.5))[0].position > 0.0);
        let closed = c.resolve(1, true); // Up -> YES pays
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].reason, "resolved");
        assert!(closed[0].won);
    }
}
