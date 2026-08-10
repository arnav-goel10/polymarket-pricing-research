use serde::{Deserialize, Serialize};

/// Real-time state + moment forecasts needed to price the YES (Up) digital.
/// `sigma` and `mu` are annualized; `tau` is in years. Variance over the
/// remaining window is `sigma^2 * tau`.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct PricingContext {
    pub s_t: f64,   // current price
    pub s0: f64,    // window reference price (frozen at t0)
    pub tau: f64,   // remaining time to expiry, in years
    pub mu: f64,    // annualized drift of log-price (incl. alpha tilt)
    pub sigma: f64, // annualized volatility forecast over remaining tau
    pub skew: f64,  // forecast skewness of terminal return
    pub kurt: f64,  // forecast kurtosis of terminal return (Gaussian = 3.0)
}

impl PricingContext {
    /// Log-distance from current price to the barrier: k = ln(s0 / s_t).
    /// YES pays if the remaining log-return R_tau > k.
    #[inline]
    pub fn k(&self) -> f64 {
        (self.s0 / self.s_t).ln()
    }
}

/// A single model's price for a window, used by the calibration scoreboard.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ModelPrice {
    pub p_yes: f64,
}

/// A resolved window: what a model predicted vs. the realized outcome.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ResolvedWindow {
    pub p_yes_predicted: f64,
    pub outcome_up: bool, // true if S_T > S0
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ReplaySample {
    pub ctx: PricingContext,
    pub outcome_up: bool,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub fill_price: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pricing_context_log_barrier_is_zero_at_the_money() {
        let ctx = PricingContext {
            s_t: 60_000.0,
            s0: 60_000.0,
            tau: 0.001,
            mu: 0.0,
            sigma: 0.8,
            skew: 0.0,
            kurt: 3.0,
        };
        assert!(ctx.k().abs() < 1e-12);
    }

    #[test]
    fn pricing_context_barrier_sign_tracks_price_vs_reference() {
        let up = PricingContext {
            s_t: 60_100.0,
            s0: 60_000.0,
            tau: 0.001,
            mu: 0.0,
            sigma: 0.8,
            skew: 0.0,
            kurt: 3.0,
        };
        assert!(up.k() < 0.0);
    }
}
