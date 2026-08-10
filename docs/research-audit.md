# BTC 5m Polymarket Strategy Audit - 2026-06-23

## Executive Conclusion

The current strategy is profitable in this sample, but not proven as a statistically reliable edge after conservative window-level uncertainty treatment. Treat it as promising forward-test evidence, not a deployment claim.

The main positive signal is not the dashboard headline alone; it is the repeated positive window-level PnL across several current modes and models. The main weakness is evidentiary: only 525 scored forecast windows are available in `run_state.json`, while the trade ledger spans 334 windows and includes 0 legacy `maker` rows from a removed strategy. The right next decision is to freeze one candidate strategy definition and run a forward-only holdout before drawing a production conclusion.

## Data Snapshot

- Source directory: `fixtures/shadow-ledger-v1`
- Trade rows: 5000
- Distinct trade windows: 334 total, 334 current-mode windows
- Window range: `1782367500000` to `1782469200000`
- Mode rows: active=1560, conv=1535, hold=1905
- Strategy rows: sharp=979, cos_vg=860, blend=634, blend_dyn=625, student_t=582, calibrated=570, black_scholes=497, hold_open_reprice_sharp=253
- Forecast scoreboard windows: 525; latest dashboard resolved windows: 525
- History points loaded: 3000

## Strategy Mechanics Audited

- `crates/app/src/main.rs` builds base models (`black_scholes`, `student_t`, `sharp`, optional `cos_vg`) and appends derived `calibrated`, `blend`, and `blend_dyn` forecasts. Current paper trading tracks each model as its own strategy rather than one monolithic portfolio.
- `crates/app/src/resolution.rs` implements hold-to-resolution and active intraday paper strategies. Hold opens at most one position per model/window when edge clears the adaptive threshold. Active moves a signed YES-equivalent position and requires entry edge to cover the spread round trip.
- `hold_open_reprice_sharp` is a shadow-only derived hold candidate: it reuses the `sharp` forecast but can only enter in the first minute after market open (`seconds_remaining >= 240`). It is designed to test whether the observed PnL comes from early-window under-reaction rather than generic hold behavior.
- `crates/app/src/convergence.rs` implements fair-value convergence: enter when model fair differs from market mid by enough, exit when the market converges, otherwise settle at resolution.
- `crates/app/src/adaptive.rs` learns a per-window typical maximum edge and sets the acting threshold as a clamped fraction of that value.
- `crates/pricing/src/platt.rs` performs online Platt scaling of `sharp`; `crates/pricing/src/weights.rs` performs online dynamic model averaging for `blend_dyn`.
- `crates/execution/src/risk.rs` blocks dry-run decisions on stale feeds/books, pin-window timing, model disagreement, spread, depth, exposure, notional, order-rate, and loss limits.
- The dashboard `shadow_fills`/PnL should not be read as one capital-allocated portfolio. It aggregates multiple model variants that often trade the same window, so statistical tests must use window-level blocks.

Hold entries span t-2s to t-299s, median t-233s, p25 t-135s, p75 t-274s.

## Data Quality Findings

- convergence intraday close rows with placeholder outcome_up=false: 1496; use pnl/won, not outcome_up, for these rows
- history point count (3000) differs materially from trade windows (334); ledger is the source of truth
- forecast scoreboard resolved windows (525) differs from current-mode trade windows (334)
- active rows per window are uneven: min=1, max=7; strategy set changed or some windows were partial
- conv rows per window are uneven: min=1, max=7; strategy set changed or some windows were partial
- hold rows per window are uneven: min=1, max=8; strategy set changed or some windows were partial
- latest_state shadow_fills is a runtime dashboard counter, not a closed-ledger count

These findings do not make the ledger useless, but they do mean the report separates current modes (`hold`, `active`, `conv`) from legacy `maker` and treats model variants as correlated observations.

## Current Strategy Statistics

Window-level PnL is the independent unit. `Raw trades` is shown for traceability, but p-values and confidence intervals use one PnL observation per `(mode, strategy, window)`. Approx ROI assumes the configured `$10` paper budget per window-equivalent exposure.

| Mode | Strategy | Windows | Raw trades | Total PnL | Mean/window | Boot 95% CI | p mean | Holm p | Max DD | Approx ROI |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| active | sharp | 325 | 325 | 170.88 | 0.53 | [0.17, 1.02] | 0.017 | 0.374 | 17.79 | 5.3% |
| active | blend_dyn | 203 | 203 | 142.84 | 0.70 | [0.18, 1.40] | 0.027 | 0.545 | 23.73 | 7.0% |
| active | blend | 208 | 208 | 140.80 | 0.68 | [0.16, 1.37] | 0.030 | 0.575 | 29.36 | 6.8% |
| active | student_t | 193 | 193 | 134.06 | 0.69 | [0.16, 1.44] | 0.036 | 0.655 | 20.25 | 6.9% |
| active | calibrated | 184 | 184 | 129.24 | 0.70 | [0.08, 1.52] | 0.061 | 1.000 | 31.32 | 7.0% |
| active | black_scholes | 161 | 161 | 119.28 | 0.74 | [0.04, 1.68] | 0.078 | 1.000 | 27.02 | 7.4% |
| active | cos_vg | 286 | 286 | 96.86 | 0.34 | [-0.11, 0.91] | 0.191 | 1.000 | 56.94 | 3.4% |
| conv | black_scholes | 158 | 158 | 17.30 | 0.11 | [-0.12, 0.39] | 0.412 | 1.000 | 12.30 | 1.1% |
| conv | student_t | 187 | 187 | 14.58 | 0.08 | [-0.10, 0.31] | 0.458 | 1.000 | 10.44 | 0.8% |
| conv | calibrated | 184 | 184 | -4.38 | -0.02 | [-0.16, 0.11] | 0.734 | 1.000 | 24.08 | -0.2% |
| conv | sharp | 323 | 323 | -6.39 | -0.02 | [-0.11, 0.07] | 0.648 | 1.000 | 14.82 | -0.2% |
| conv | blend_dyn | 201 | 201 | -9.40 | -0.05 | [-0.17, 0.07] | 0.447 | 1.000 | 23.84 | -0.5% |
| conv | blend | 204 | 204 | -15.21 | -0.07 | [-0.19, 0.04] | 0.207 | 1.000 | 24.01 | -0.7% |
| conv | cos_vg | 278 | 278 | -32.64 | -0.12 | [-0.22, -0.01] | 0.023 | 0.491 | 42.46 | -1.2% |
| hold | blend_dyn | 221 | 221 | 50.91 | 0.23 | [-0.06, 0.55] | 0.145 | 1.000 | 15.94 | 2.3% |
| hold | black_scholes | 178 | 178 | 45.26 | 0.25 | [-0.25, 0.86] | 0.364 | 1.000 | 29.59 | 2.5% |
| hold | blend | 222 | 222 | 44.50 | 0.20 | [-0.10, 0.52] | 0.212 | 1.000 | 18.20 | 2.0% |
| hold | student_t | 202 | 202 | 39.37 | 0.19 | [-0.14, 0.54] | 0.257 | 1.000 | 20.71 | 1.9% |
| hold | calibrated | 202 | 202 | 31.34 | 0.16 | [-0.27, 0.65] | 0.510 | 1.000 | 42.50 | 1.6% |
| hold | sharp | 331 | 331 | 19.76 | 0.06 | [-0.12, 0.24] | 0.509 | 1.000 | 20.52 | 0.6% |
| hold | hold_open_reprice_sharp | 253 | 253 | 18.02 | 0.07 | [-0.13, 0.28] | 0.497 | 1.000 | 17.75 | 0.7% |
| hold | cos_vg | 296 | 296 | -21.81 | -0.07 | [-0.30, 0.15] | 0.525 | 1.000 | 60.41 | -0.7% |

Interpretation:

- Positive totals are broad, especially for `hold` and `active`, but the table is not a portfolio result because strategies are highly correlated and were run simultaneously on the same markets.
- A strategy cell is stronger evidence when its bootstrap CI is entirely above zero and its Holm-adjusted p-value remains below 0.05.
- Large positive max trades and positive skew mean a few big resolution wins contribute meaningfully; trimmed means in the script output should be checked before increasing size.

## Forecast Quality

Forecast scores come from `shadow-data/run_state.json`, not the trade ledger. Lower Brier/log-loss is better. `vs climatology` is positive when the model beats a constant forecast equal to the observed up-rate.

| Model | N | Brier | Log loss | Direction acc | Mean p | Up rate | Brier vs clim | LL vs clim |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| student_t | 500 | 0.09580 | 0.31119 | 87.6% | 0.468 | 0.462 | +0.15275 | +0.37907 |
| blend | 500 | 0.09602 | 0.31276 | 87.0% | 0.467 | 0.462 | +0.15253 | +0.37750 |
| blend_dyn | 500 | 0.09651 | 0.31471 | 87.0% | 0.466 | 0.462 | +0.15205 | +0.37555 |
| black_scholes | 500 | 0.09721 | 0.31618 | 87.6% | 0.468 | 0.462 | +0.15135 | +0.37407 |
| calibrated | 500 | 0.09847 | 0.32724 | 86.8% | 0.461 | 0.462 | +0.15009 | +0.36302 |
| cos_vg | 500 | 0.09863 | 0.37819 | 86.4% | 0.471 | 0.462 | +0.14993 | +0.31207 |
| sharp | 500 | 0.10138 | 0.48909 | 87.6% | 0.466 | 0.462 | +0.14718 | +0.20117 |

Best Brier model calibration bins: `student_t`.

| Pred bin | N | Avg pred | Observed up |
| --- | --- | --- | --- |
| 0.0-0.2 | 193 | 0.051 | 0.057 |
| 0.2-0.4 | 55 | 0.291 | 0.182 |
| 0.4-0.6 | 37 | 0.498 | 0.568 |
| 0.6-0.8 | 53 | 0.699 | 0.698 |
| 0.8-1.0 | 162 | 0.942 | 0.938 |

The forecast layer looks directionally strong in-sample, but 525 windows is a small sample for seven closely related models. The ranking currently favors the sharpened/calibrated family, which matches the code thesis that this market under-reacts, but it still needs forward-only confirmation.

## Legacy Strategy Evidence

The `maker` rows are historical and should not be included in current strategy claims.

No legacy rows found.

## Issues And Caveats

- Sample size is still thin for statistical proof, especially for forecast calibration: 525 scored windows is not enough to trust subtle model ranking.
- Model variants are correlated because they trade the same 5-minute windows; naive per-trade significance overstates confidence.
- The ledger mixes code generations. `maker` is present historically but current code says market-making was removed as non-viable.
- The script assumes the ledger PnL already reflects the current paper fill model. It does not reconstruct order book depth, queue priority, latency, or fees from raw market data.
- Convergence intraday close rows use `outcome_up=false` as a placeholder before final settlement; PnL/won are usable, but outcome labels are not.
- Shadow execution is taker-style and optimistic relative to actual execution: no failed posts, queue movement between decision and order, API latency, or external cancellation/reconciliation errors are represented.
- Adaptive threshold and online Platt/DMA learning can data-snoop if tuned on the same run used to judge significance.
- Dashboard totals can be misleading if read as deployable portfolio PnL because multiple strategies are simultaneous alternatives, not capital-allocated decisions.

## Recommended Improvements

1. Freeze one candidate for the next run, preferably the best current-mode/model pair after excluding legacy rows and checking trimmed mean, then evaluate only that candidate forward.
2. Split ledgers by strategy version or add a `strategy_version` field so old `maker` and parameter revisions cannot contaminate future audits.
3. Add a market-implied baseline: compare each model forecast and trade against best bid/ask, mid, and a no-trade baseline at the same decision timestamp.
4. Add cost stress tests to the analysis: subtract fee/slippage buffers of 1c, 2c, and 5c per contract-equivalent and re-run significance.
5. Report portfolio-level results separately from model-cell results. A deployed system can only allocate finite capital across variants.
6. Promote window-level confidence metrics to the dashboard: scored windows, best candidate CI, current drawdown, and whether the latest run is out-of-sample.
7. Add a replay/backtest fixture with raw observations so the paper trackers can be rerun from source events rather than only summarized ledgers.
8. Keep the project paper-only and require a forward holdout before making stronger performance claims.

## Reproducibility

Commands:

```bash
python3 -m py_compile scripts/analyze_shadow_strategy.py
python3 scripts/analyze_shadow_strategy.py --check --data shadow-data
python3 scripts/analyze_shadow_strategy.py --data shadow-data --report docs/strategy-analysis-2026-06-23.md
```

Assumptions:

- `shadow-data/trades.json` is the source of truth for closed trade PnL.
- `shadow-data/run_state.json` is the source of truth for forecast Brier/log-loss boards.
- One `(mode, strategy, t0_millis)` PnL block is the statistical unit.
- Legacy `maker` rows are excluded from current-strategy conclusions.
- Approx ROI uses the default configured paper budget of `$10` because the ledger does not store per-row notional.
