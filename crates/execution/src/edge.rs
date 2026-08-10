use market::book::OrderBook;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

/// A candidate trade decision: side, size, achievable VWAP, and net edge.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TradeDecision {
    pub side: Side,
    pub size: f64,
    pub vwap: f64,
    pub edge: f64,
}

/// Best trade vs the book for the YES token, given model fair value `fair_yes`.
/// `fee` is a per-unit cost in probability units. Returns the more profitable of
/// buy/sell if its net edge is positive.
pub fn best_trade(fair_yes: f64, book: &OrderBook, size: f64, fee: f64) -> Option<TradeDecision> {
    let buy = book.simulate_buy(size).map(|f| TradeDecision {
        side: Side::Buy,
        size: f.filled,
        vwap: f.avg_price,
        edge: fair_yes - f.avg_price - fee,
    });
    let sell = book.simulate_sell(size).map(|f| TradeDecision {
        side: Side::Sell,
        size: f.filled,
        vwap: f.avg_price,
        edge: f.avg_price - fair_yes - fee,
    });
    [buy, sell]
        .into_iter()
        .flatten()
        .filter(|d| d.edge > 0.0 && d.edge.is_finite())
        .max_by(|a, b| {
            a.edge
                .partial_cmp(&b.edge)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_abs_diff_eq;
    use market::book::{Level, OrderBook};

    fn book() -> OrderBook {
        OrderBook {
            bids: vec![Level {
                price: 0.48,
                size: 500.0,
            }],
            asks: vec![Level {
                price: 0.52,
                size: 500.0,
            }],
        }
    }

    #[test]
    fn buy_edge_is_fair_minus_vwap_minus_fee() {
        let d = best_trade(0.60, &book(), 100.0, 0.0).unwrap();
        assert_eq!(d.side, Side::Buy);
        assert_abs_diff_eq!(d.edge, 0.08, epsilon = 1e-9);
    }

    #[test]
    fn sell_edge_when_fair_below_bid() {
        let d = best_trade(0.40, &book(), 100.0, 0.0).unwrap();
        assert_eq!(d.side, Side::Sell);
        assert_abs_diff_eq!(d.edge, 0.08, epsilon = 1e-9);
    }

    #[test]
    fn no_edge_inside_the_spread_returns_none() {
        assert!(best_trade(0.50, &book(), 100.0, 0.0).is_none());
    }

    #[test]
    fn fee_erodes_edge() {
        let d = best_trade(0.60, &book(), 100.0, 0.02);
        assert_abs_diff_eq!(d.unwrap().edge, 0.06, epsilon = 1e-9);
    }
}
