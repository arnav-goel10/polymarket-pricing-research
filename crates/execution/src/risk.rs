/// Configurable hard limits (all from config; conservative defaults at call site).
#[derive(Debug, Clone, Copy)]
pub struct RiskLimits {
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
}

/// Real-time inputs evaluated against the limits for a single proposed order.
#[derive(Debug, Clone, Copy)]
pub struct RiskInputs {
    pub edge: f64,
    pub stake: f64,
    pub open_exposure: f64,
    pub daily_pnl: f64,
    pub seconds_remaining: i64,
    pub model_disagreement: f64,
    pub feed_fresh: bool,
    pub book_fresh: bool,
    pub decision_count_last_minute: u32,
    pub market_pnl: f64,
    pub daily_notional: f64,
    pub available_depth: f64,
    pub spread: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockReason {
    KillSwitch,
    StaleFeed,
    DailyLossBreached,
    PinWindow,
    ModelDisagreement,
    EdgeTooSmall,
    StakeTooLarge,
    ExposureExceeded,
    StaleBook,
    OrderRateExceeded,
    MarketLossBreached,
    DailyNotionalExceeded,
    DepthTooThin,
    SpreadTooWide,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RiskVerdict {
    Allow,
    Block(BlockReason),
}

/// Stateful gatekeeper. Evaluates gates in priority order; first breach wins.
pub struct RiskManager {
    limits: RiskLimits,
    killed: bool,
}

impl RiskManager {
    pub fn new(limits: RiskLimits) -> Self {
        RiskManager {
            limits,
            killed: false,
        }
    }
    pub fn kill(&mut self) {
        self.killed = true;
    }
    pub fn reset_kill(&mut self) {
        self.killed = false;
    }
    /// Update the minimum-edge gate (used for the adaptive edge threshold).
    pub fn set_min_edge(&mut self, min_edge: f64) {
        self.limits.min_edge = min_edge;
    }

    pub fn check(&self, i: &RiskInputs) -> RiskVerdict {
        use BlockReason::*;
        let l = &self.limits;
        let breach = if self.killed {
            Some(KillSwitch)
        } else if !i.feed_fresh {
            Some(StaleFeed)
        } else if !i.book_fresh {
            Some(StaleBook)
        } else if i.daily_pnl <= -l.max_daily_loss {
            Some(DailyLossBreached)
        } else if i.market_pnl <= -l.max_market_loss {
            Some(MarketLossBreached)
        } else if i.seconds_remaining <= l.no_trade_last_secs {
            Some(PinWindow)
        } else if i.model_disagreement > l.max_model_disagreement {
            Some(ModelDisagreement)
        } else if i.decision_count_last_minute >= l.max_decision_rate_per_minute {
            Some(OrderRateExceeded)
        } else if i.edge < l.min_edge {
            Some(EdgeTooSmall)
        } else if i.stake > l.max_stake_per_window {
            Some(StakeTooLarge)
        } else if i.open_exposure + i.stake > l.max_open_exposure {
            Some(ExposureExceeded)
        } else if i.daily_notional + i.stake > l.max_daily_notional {
            Some(DailyNotionalExceeded)
        } else if i.available_depth < l.min_depth {
            Some(DepthTooThin)
        } else if i.spread > l.max_spread {
            Some(SpreadTooWide)
        } else {
            None
        };
        match breach {
            Some(r) => RiskVerdict::Block(r),
            None => RiskVerdict::Allow,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> RiskLimits {
        RiskLimits {
            min_edge: 0.03,
            max_stake_per_window: 100.0,
            max_open_exposure: 250.0,
            max_daily_loss: 200.0,
            no_trade_last_secs: 20,
            max_model_disagreement: 0.15,
            max_decision_rate_per_minute: 3,
            max_market_loss: 100.0,
            max_daily_notional: 500.0,
            min_depth: 10.0,
            max_spread: 0.10,
        }
    }

    fn ok_inputs() -> RiskInputs {
        RiskInputs {
            edge: 0.05,
            stake: 50.0,
            open_exposure: 100.0,
            daily_pnl: -50.0,
            seconds_remaining: 120,
            model_disagreement: 0.05,
            feed_fresh: true,
            book_fresh: true,
            decision_count_last_minute: 0,
            market_pnl: -10.0,
            daily_notional: 100.0,
            available_depth: 100.0,
            spread: 0.02,
        }
    }

    #[test]
    fn passes_when_all_gates_satisfied() {
        assert_eq!(
            RiskManager::new(limits()).check(&ok_inputs()),
            RiskVerdict::Allow
        );
    }

    #[test]
    fn blocks_below_min_edge() {
        let mut i = ok_inputs();
        i.edge = 0.01;
        assert_eq!(
            RiskManager::new(limits()).check(&i),
            RiskVerdict::Block(BlockReason::EdgeTooSmall)
        );
    }

    #[test]
    fn blocks_in_pin_window() {
        let mut i = ok_inputs();
        i.seconds_remaining = 10;
        assert_eq!(
            RiskManager::new(limits()).check(&i),
            RiskVerdict::Block(BlockReason::PinWindow)
        );
    }

    #[test]
    fn blocks_on_model_disagreement() {
        let mut i = ok_inputs();
        i.model_disagreement = 0.3;
        assert_eq!(
            RiskManager::new(limits()).check(&i),
            RiskVerdict::Block(BlockReason::ModelDisagreement)
        );
    }

    #[test]
    fn blocks_on_daily_loss_breach() {
        let mut i = ok_inputs();
        i.daily_pnl = -250.0;
        assert_eq!(
            RiskManager::new(limits()).check(&i),
            RiskVerdict::Block(BlockReason::DailyLossBreached)
        );
    }

    #[test]
    fn blocks_on_stale_feed_and_oversize_and_exposure() {
        let mut stale = ok_inputs();
        stale.feed_fresh = false;
        assert_eq!(
            RiskManager::new(limits()).check(&stale),
            RiskVerdict::Block(BlockReason::StaleFeed)
        );

        let mut big = ok_inputs();
        big.stake = 150.0;
        assert_eq!(
            RiskManager::new(limits()).check(&big),
            RiskVerdict::Block(BlockReason::StakeTooLarge)
        );

        let mut exp = ok_inputs();
        exp.open_exposure = 240.0;
        exp.stake = 50.0;
        assert_eq!(
            RiskManager::new(limits()).check(&exp),
            RiskVerdict::Block(BlockReason::ExposureExceeded)
        );
    }

    #[test]
    fn blocks_on_paper_safety_gates() {
        let rm = RiskManager::new(limits());

        let mut stale_book = ok_inputs();
        stale_book.book_fresh = false;
        assert_eq!(
            rm.check(&stale_book),
            RiskVerdict::Block(BlockReason::StaleBook)
        );

        let mut rate = ok_inputs();
        rate.decision_count_last_minute = 3;
        assert_eq!(
            rm.check(&rate),
            RiskVerdict::Block(BlockReason::OrderRateExceeded)
        );

        let mut depth = ok_inputs();
        depth.available_depth = 2.0;
        assert_eq!(
            rm.check(&depth),
            RiskVerdict::Block(BlockReason::DepthTooThin)
        );

        let mut spread = ok_inputs();
        spread.spread = 0.20;
        assert_eq!(
            rm.check(&spread),
            RiskVerdict::Block(BlockReason::SpreadTooWide)
        );
    }

    #[test]
    fn kill_switch_blocks_everything() {
        let mut rm = RiskManager::new(limits());
        rm.kill();
        assert_eq!(
            rm.check(&ok_inputs()),
            RiskVerdict::Block(BlockReason::KillSwitch)
        );
    }
}
