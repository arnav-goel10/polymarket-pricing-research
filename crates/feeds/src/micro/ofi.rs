use crate::Trade;

/// Aggressor side: a trade where `buyer_is_maker == false` is buyer-initiated.
fn signed_qty(t: &Trade) -> f64 {
    if t.buyer_is_maker {
        -t.qty
    } else {
        t.qty
    }
}

/// Cumulative Volume Delta: running sum of signed trade volume.
#[derive(Debug, Default, Clone, Copy)]
pub struct Cvd {
    value: f64,
}

impl Cvd {
    pub fn update(&mut self, t: &Trade) {
        self.value += signed_qty(t);
    }
    pub fn value(&self) -> f64 {
        self.value
    }
}

/// Order-flow imbalance over a batch of trades, normalized to [-1, 1]:
/// (buy_vol - sell_vol) / (buy_vol + sell_vol).
pub fn order_flow_imbalance(trades: &[Trade]) -> f64 {
    let mut buy = 0.0;
    let mut sell = 0.0;
    for t in trades {
        if t.buyer_is_maker {
            sell += t.qty;
        } else {
            buy += t.qty;
        }
    }
    let total = buy + sell;
    if total <= 0.0 {
        0.0
    } else {
        (buy - sell) / total
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Trade;
    use approx::assert_abs_diff_eq;

    fn buy(qty: f64) -> Trade {
        Trade {
            price: 1.0,
            qty,
            buyer_is_maker: false,
            ts_millis: 0,
        }
    }
    fn sell(qty: f64) -> Trade {
        Trade {
            price: 1.0,
            qty,
            buyer_is_maker: true,
            ts_millis: 0,
        }
    }

    #[test]
    fn cvd_accumulates_signed_volume() {
        let mut c = Cvd::default();
        c.update(&buy(3.0));
        c.update(&sell(1.0));
        assert_abs_diff_eq!(c.value(), 2.0, epsilon = 1e-12);
    }

    #[test]
    fn ofi_is_normalized_in_minus_one_to_one() {
        let v = order_flow_imbalance(&[buy(4.0), sell(1.0)]);
        assert_abs_diff_eq!(v, 0.6, epsilon = 1e-12);
        assert!((-1.0..=1.0).contains(&v));
    }

    #[test]
    fn ofi_zero_when_no_volume() {
        assert_eq!(order_flow_imbalance(&[]), 0.0);
    }
}
