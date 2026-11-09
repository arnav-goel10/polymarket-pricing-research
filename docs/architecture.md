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

## Feed ingestion and the local order book

Every WebSocket feed (`feeds::rtds`, `feeds::binance`, `market::clob_book`) is a `run_with(url, &ConnOpts, callback)` function that streams until the connection fails and then returns the error. `feeds::conn::supervise` owns the retry loop: it marks the feed disconnected, counts the reconnect, logs the cause with structured fields and sleeps with `poly_core::Backoff` (250 ms doubling to 10 s, reset after a 30 s healthy session). `ConnOpts::idle_timeout` (30 s by default) turns a silent half-open socket into an error.

`market::sync::LocalBook` rebuilds one book from a snapshot plus deltas. Under `SeqPolicy::Contiguous` a duplicate is ignored and a gap stops the book until the next snapshot. A malformed level rejects the whole message and desyncs the book. After each delta the book is reconciled with the venue-reported best bid and ask (`VenueTop`), then checked for a crossed state, which also desyncs it. A desynced book publishes nothing.

`market::clob_book::ClobBookStream` adapts the Polymarket market channel to `LocalBook`. Polymarket sends no sequence numbers and its timestamps are not ordered across event types, so messages are sequenced by arrival on the connection. Fills arrive as `last_trade_price` events without a level removal, which is why each `price_change` entry's `best_bid` / `best_ask` is used for reconciliation. Any desync ends the session, and the app refetches a REST snapshot before resubscribing.

## Recording, lifecycle and operations

`feeds::recording` defines the append-only recording format and `feeds::recorder` the non-blocking writer (bounded queue, drop newest and count). `RECORD_PATH` turns recording on in the app; `record` and `prec` are the standalone tools. SIGTERM or SIGINT stops HTTP, cancels feeds, and drains and fsyncs the recorder. The full design, trade-offs and failure modes are in [DESIGN.md](DESIGN.md), and the operator guide for each metric is in [RUNBOOK.md](RUNBOOK.md).

## Observability

`poly_core::FeedMetrics` holds relaxed atomic counters per feed (messages, malformed frames, reconnects, resyncs, connection state, last receive time, last exchange timestamp and a 10-second message-rate ring). `app::metrics::Metrics` renders them at `GET /metrics` in Prometheus text format, including a stale flag per feed and the recorder queue counters. `/healthz` is liveness only; `/readyz` returns 503 until every feed is fresh. `LOG_FORMAT=json` switches `tracing` output to JSON lines.

## Control plane

The Axum dashboard exposes state, history, fills, trades, health, Prometheus metrics, and same-origin WebSocket endpoints. State-changing dry-run controls are registered only when `CONTROL_TOKEN` is configured and require an exact Bearer token.

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
