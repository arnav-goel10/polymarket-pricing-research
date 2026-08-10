# Paper-Only Research Architecture

Polymarket Pricing Research is a paper-only evaluation pipeline. It observes public market data, produces calibrated probability estimates, applies risk gates, simulates hypothetical fills, and generates auditable replay evidence. Market-data ingestion, forecasting, decision logic, paper-fill simulation, and evidence generation are separated so each layer can be tested independently.

The executable boundary is documented in [Publication boundary](publication-boundary.md): the repository has no wallet, signer, credentialed exchange client, or external order-submission path.

## Paper-only research flow

1. Feed adapters normalize reference prices, exchange prices, and order-book updates.
2. Rolling buffers estimate volatility, skew, kurtosis, order-book imbalance, and lead/lag features.
3. Independent pricing models produce `P(YES)` for the current five-minute window.
4. Online Platt scaling and dynamic model averaging create calibrated forecasts.
5. Adaptive edge logic compares forecasts with observed market prices.
6. The risk manager evaluates staleness, spread, depth, timing, disagreement, exposure, notional, decision rate, and simulated losses.
7. The paper-fill simulator records shadow or dry-run decisions, hypothetical fills, and P&amp;L. It has no credential, signer, or order-posting interface.

## Control plane

The Axum dashboard exposes state, history, fills, trades, health, and same-origin WebSocket endpoints. State-changing dry-run controls are registered only when `CONTROL_TOKEN` is configured and require an exact Bearer token.

Non-loopback dry-run operation requires a control token with at least 32 characters. Public shadow demos remain read-only.

## Persistence and evidence

- `fixtures/shadow-ledger-v1/` is the versioned, non-secret evidence source.
- Ignored `shadow-data/` contains mutable local state.
- `fills.jsonl` is the append-only paper-fill audit trail.
- `scripts/analyze_shadow_strategy.py` validates evidence and generates the published summary.
- `fixtures/replay-sample.json` powers deterministic offline model replay.

The static demo consumes generated evidence and never connects to the runtime process.

## Failure posture

- Shadow mode is the default.
- No application mode can sign or submit an external order.
- Stale data, excessive risk, or invalid configuration blocks simulated decisions.
- A stop action disables dry-run evaluation.
- Docker starts in shadow mode.
