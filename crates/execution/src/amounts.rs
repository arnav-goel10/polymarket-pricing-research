use crate::edge::Side;

const BASE: f64 = 1_000_000.0; // 6 decimals

/// Integer base-unit amounts for a CLOB order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderAmounts {
    pub maker_amount: u128,
    pub taker_amount: u128,
}

/// Convert (side, price, size) into maker/taker base-unit amounts.
pub fn order_amounts(side: Side, price: f64, size: f64) -> OrderAmounts {
    let usdc = (price * size * BASE).round() as u128;
    let shares = (size * BASE).round() as u128;
    match side {
        Side::Buy => OrderAmounts {
            maker_amount: usdc,
            taker_amount: shares,
        },
        Side::Sell => OrderAmounts {
            maker_amount: shares,
            taker_amount: usdc,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buy_amounts_usdc_out_shares_in() {
        let a = order_amounts(Side::Buy, 0.52, 100.0);
        assert_eq!(a.maker_amount, 52_000_000);
        assert_eq!(a.taker_amount, 100_000_000);
    }

    #[test]
    fn sell_amounts_shares_out_usdc_in() {
        let a = order_amounts(Side::Sell, 0.52, 100.0);
        assert_eq!(a.maker_amount, 100_000_000);
        assert_eq!(a.taker_amount, 52_000_000);
    }

    #[test]
    fn rounds_to_nearest_base_unit() {
        let a = order_amounts(Side::Buy, 0.333, 33.0);
        assert_eq!(a.maker_amount, 10_989_000);
    }
}
