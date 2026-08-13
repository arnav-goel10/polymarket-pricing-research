# Research methodology

## Questions

1. **Forecasting:** do model probabilities improve Brier score and log loss against 0.5, climatology, and market-implied probability?
2. **Decision quality:** does an apparent edge remain positive after executable spread and explicit cost stress?
3. **Robustness:** does a frozen strategy retain expectancy, drawdown, and calibration quality on a forward-only holdout?

## Unit of analysis

Model variants in the same five-minute market are dependent. Analysis therefore aggregates P&L at `(mode, strategy, t0_millis)` and treats the window—not each fill—as the observation. Bootstrap confidence intervals resample windows, while Holm correction controls family-wise error across strategy cells.

## Metrics

- Brier score and log loss for probability quality.
- Directional accuracy and reliability bins for calibration context.
- Mean, median, trimmed mean, standard deviation, skew, and expectancy per window.
- Bootstrap 95% confidence interval, approximate mean test, sign test, and Holm-adjusted p-value.
- Profit factor, maximum drawdown, and approximate ROI.

No single metric is sufficient. Positive simulated P&L with weak calibration may be unstable; good probability scores without executable edge may not produce useful decisions.

## Evidence labels

- `fixtures/replay-sample.json` is synthetic test data and supports pipeline-correctness claims only.
- `fixtures/shadow-ledger-v1/` is versioned historical paper-run evidence and remains subject to simulator limitations.
- Ignored `shadow-data/` is mutable runtime state and is not a published evidence source.

## Acceptance criteria

A candidate strategy must have a frozen definition, a forward-only sample, stressed costs, stable calibration, and portfolio-level reporting before it can be treated as research evidence. Poly intentionally stops at paper evaluation.
