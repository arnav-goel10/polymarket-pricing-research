/// One price level (price in probability units [0,1], size in shares).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Level {
    pub price: f64,
    pub size: f64,
}

/// A snapshot order book. `bids` sorted desc by price, `asks` sorted asc.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderBook {
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

/// Result of walking the book for a marketable order.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FillSim {
    pub filled: f64,
    pub avg_price: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BookSummary {
    pub best_bid: f64,
    pub best_ask: f64,
    pub spread: f64,
    pub top_depth: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DollarFillSims {
    pub buy_yes: Option<FillSim>,
    pub sell_yes: Option<FillSim>,
}

impl OrderBook {
    pub fn best_bid(&self) -> Option<f64> {
        self.bids.first().map(|l| l.price)
    }
    pub fn best_ask(&self) -> Option<f64> {
        self.asks.first().map(|l| l.price)
    }
    pub fn mid(&self) -> Option<f64> {
        Some((self.best_bid()? + self.best_ask()?) / 2.0)
    }
    pub fn spread(&self) -> Option<f64> {
        Some(self.best_ask()? - self.best_bid()?)
    }
    pub fn ask_depth(&self) -> f64 {
        self.asks.iter().map(|l| l.size).sum()
    }
    pub fn bid_depth(&self) -> f64 {
        self.bids.iter().map(|l| l.size).sum()
    }
    pub fn top_depth(&self) -> f64 {
        self.ask_depth().min(self.bid_depth())
    }

    pub fn summary(&self) -> Option<BookSummary> {
        let best_bid = self.best_bid()?;
        let best_ask = self.best_ask()?;
        Some(BookSummary {
            best_bid,
            best_ask,
            spread: best_ask - best_bid,
            top_depth: self.top_depth(),
        })
    }

    fn walk(levels: &[Level], size: f64) -> Option<FillSim> {
        if levels.is_empty() || size <= 0.0 {
            return None;
        }
        let mut remaining = size;
        let mut cost = 0.0;
        let mut filled = 0.0;
        for l in levels {
            let take = remaining.min(l.size);
            cost += take * l.price;
            filled += take;
            remaining -= take;
            if remaining <= 0.0 {
                break;
            }
        }
        if filled <= 0.0 {
            None
        } else {
            Some(FillSim {
                filled,
                avg_price: cost / filled,
            })
        }
    }

    /// Simulate a marketable BUY walking the asks; returns VWAP + filled size.
    pub fn simulate_buy(&self, size: f64) -> Option<FillSim> {
        Self::walk(&self.asks, size)
    }
    /// Simulate a marketable SELL walking the bids.
    pub fn simulate_sell(&self, size: f64) -> Option<FillSim> {
        Self::walk(&self.bids, size)
    }

    /// Simulate equal-dollar YES buy and NO buy (via selling YES) probes.
    pub fn simulate_dollar_sides(&self, budget_usd: f64) -> DollarFillSims {
        let buy_yes = self
            .best_ask()
            .filter(|ask| *ask > 0.0)
            .and_then(|ask| self.simulate_buy(budget_usd / ask))
            .filter(|f| f.filled > 0.0);
        let sell_yes = self
            .best_bid()
            .filter(|bid| 1.0 - *bid > 0.0)
            .and_then(|bid| self.simulate_sell(budget_usd / (1.0 - bid)))
            .filter(|f| f.filled > 0.0);
        DollarFillSims { buy_yes, sell_yes }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_abs_diff_eq;

    fn book() -> OrderBook {
        OrderBook {
            bids: vec![
                Level {
                    price: 0.49,
                    size: 100.0,
                },
                Level {
                    price: 0.48,
                    size: 200.0,
                },
            ],
            asks: vec![
                Level {
                    price: 0.51,
                    size: 100.0,
                },
                Level {
                    price: 0.52,
                    size: 200.0,
                },
            ],
        }
    }

    #[test]
    fn best_bid_ask() {
        let b = book();
        assert_abs_diff_eq!(b.best_bid().unwrap(), 0.49, epsilon = 1e-12);
        assert_abs_diff_eq!(b.best_ask().unwrap(), 0.51, epsilon = 1e-12);
        assert_abs_diff_eq!(b.mid().unwrap(), 0.50, epsilon = 1e-12);
        assert_abs_diff_eq!(b.spread().unwrap(), 0.02, epsilon = 1e-12);
        assert_abs_diff_eq!(b.top_depth(), 300.0, epsilon = 1e-12);
        assert_abs_diff_eq!(b.summary().unwrap().spread, 0.02, epsilon = 1e-12);
    }

    #[test]
    fn buy_fill_walks_the_asks_and_reports_vwap() {
        let f = book().simulate_buy(150.0).unwrap();
        assert_abs_diff_eq!(f.filled, 150.0, epsilon = 1e-9);
        assert_abs_diff_eq!(
            f.avg_price,
            (100.0 * 0.51 + 50.0 * 0.52) / 150.0,
            epsilon = 1e-9
        );
    }

    #[test]
    fn buy_beyond_depth_is_partial() {
        let f = book().simulate_buy(10_000.0).unwrap();
        assert_abs_diff_eq!(f.filled, 300.0, epsilon = 1e-9);
    }

    #[test]
    fn empty_side_is_none() {
        let b = OrderBook {
            bids: vec![],
            asks: vec![],
        };
        assert!(b.best_bid().is_none());
        assert!(b.simulate_buy(1.0).is_none());
    }

    #[test]
    fn simulates_equal_dollar_sides() {
        let fills = book().simulate_dollar_sides(51.0);
        assert_abs_diff_eq!(fills.buy_yes.unwrap().filled, 100.0, epsilon = 1e-9);
        assert!(fills.sell_yes.unwrap().filled > 0.0);
    }
}
