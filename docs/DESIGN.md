# Design

This document covers the market-data side of the system: how feeds are ingested, how the order book is kept correct, how data is recorded and replayed, and how the process behaves when things fail. The pricing models and the research method are in [research-methodology.md](research-methodology.md). Everything here runs as a paper-only simulation (see [publication-boundary.md](publication-boundary.md)).

## Requirements

Functional

- Ingest three public WebSocket feeds: the Polymarket RTDS reference price (Chainlink and Binance relays), the Binance BTCUSDT perpetual trades and top of book, and the Polymarket CLOB market channel for the current five-minute contract.
- Keep a local order book for the contract that matches the venue, and price the contract on every update.
- Record raw frames to disk and replay them bit-for-bit through the same parsers.
- Expose health, readiness and metrics over HTTP.

Non-functional

| Requirement | Target | Measured |
| --- | --- | --- |
| Per-message cost on the hot path | well under the ~1 ms between book messages at peak | p99 3.7 µs per frame replaying a 10-minute live recording |
| Never price from a book the venue would not recognise | zero crossed or partial books published | property tests, fuzzing, 150 s live check with 0 crossed books |
| A feed outage must not stop the process | reconnect automatically, mark data stale | fault-injection tests |
| Bounded memory under any input rate | no unbounded queues | backpressure test, soak test |
| Clean shutdown | no truncated recording, sockets closed | SIGTERM test |

Non-goals: horizontal scaling (one process handles one contract, and a 10-minute live recording averaged about 1,600 messages per second across the three feeds), exactly-once delivery across restarts, order submission of any kind.

## Architecture

```text
               +-----------------------------  one tokio runtime  -----------------------------+
 RTDS ws  ---> | feed task (supervisor) --+                                                     |
 Binance ws -> | feed task (supervisor) --+--> shared buffers (Mutex, short critical sections)    |
 CLOB ws  ---> | feed task (supervisor) --+--> LocalBook  -->  engine tick (250 ms) --> watch --> | --> /ws dashboard
               |        |                                                                       |
               |        +-- raw frame tap --> bounded queue (65,536) --> recorder thread --> .prec file
               |                                                                                 |
               |  FeedMetrics (atomics) <------------------------------ /metrics /readyz /healthz |
               +---------------------------------------------------------------------------------+
```

Each feed is a function `run_with(url, &ConnOpts, callback)` that connects, streams, and returns an error when the session ends. A shared `supervise` loop owns retries, so reconnect policy lives in one place and every feed gets it. The callback runs on the socket-reading task, so it does only cheap work: parse, update a buffer or the book, bump atomics, enqueue the raw frame.

## Data flow for one order-book message

1. The CLOB task reads a text frame. The idle timer resets.
2. The raw text is offered to the recorder queue with `try_send`. If the queue is full the frame is dropped from the recording and counted, and the feed carries on.
3. `ClobBookStream::on_text` parses the JSON and validates every level (finite, size ≥ 0, price in (0, 1)). One bad level rejects the whole message.
4. `LocalBook` applies the changes with a binary-search upsert per level, then trims levels that sit beyond the venue-reported best bid and ask, then checks for a crossed book.
5. If the book is consistent it is published to the shared slot with its receive time. If not, the session ends with a resync error, the supervisor backs off, the app refetches a REST snapshot and resubscribes.
6. The engine reads the slot every 250 ms. A book older than 3 s, or a reference price older than 5 s, blocks every simulated decision.

## Recording format

Length-prefixed, checksummed, append-only (`crates/feeds/src/recording.rs`):

```text
header  "POLYREC\0" | u16 version | u16 reserved
record  u32 len | u32 crc32(payload) | payload = i64 recv_micros | u8 source | frame bytes
```

The reader yields every complete record and reports a partial final record as `TruncatedTail`, so a file cut short by a crash is still usable up to the last full record. A checksum mismatch, an impossible length (over 4 MiB) or an unknown source byte stops reading with an error. It never returns a corrupt record. Control records (`{"subscribe": token}`) mark each order-book resubscription, so replay knows which contract the following deltas belong to.

## Trade-offs and alternatives considered

| Decision | Chosen | Alternatives | Why |
| --- | --- | --- | --- |
| Order-book storage | Sorted `Vec<Level>` per side, binary-search upsert | `BTreeMap<price, size>`; fixed tick-indexed array | Books here hold 20 to 100 levels. A contiguous Vec is cache-friendly and reads best-first with `first()`. It benches the same as the old sort-per-update path at 20 levels (19.1 ns each), so I kept it for simpler code that never needs a full re-sort. A tick array would be fastest but couples the book to one venue's tick size. |
| Message ordering | Arrival order within a connection | Exchange timestamp ordering | Polymarket timestamps are not ordered across event types on the wire. A `best_bid_ask` stamped later can arrive before a `price_change` stamped earlier. TCP already guarantees order and no duplicates within a connection. Timestamp ordering would have dropped valid deltas. `SeqPolicy::Contiguous` remains for venues with real sequence numbers. |
| Fill handling | Trim the book to the venue-reported best bid and ask on each delta | Apply `last_trade_price` events to the book; resync on every cross | Trades do not say which resting levels they consumed, so applying them means guessing. Resync-on-cross caused 6 resyncs in the first 90 s live. Per-change best bid and ask are authoritative and come with the same message. |
| Recorder backpressure | Bounded queue, drop newest and count | Block the producer; unbounded queue; drop oldest | Blocking stalls the socket reader, and a stalled reader is what makes the exchange connection go stale. Unbounded grows memory without limit when the disk is slow. Drop-oldest and drop-newest have the same memory bound. Drop-newest is what `try_send` does without extra locking, and the drop counter makes the gap visible either way. |
| Recorder writer | Dedicated OS thread, `std::sync::mpsc::sync_channel` | tokio task with async file I/O | File writes are blocking syscalls. A thread keeps them off the runtime's worker threads and needs no async file layer. |
| Dashboard fan-out | `tokio::sync::watch` (latest value only) | broadcast channel per client | A slow browser can only ever be one snapshot behind. No per-client queue can grow. |
| Metrics | Hand-rolled Prometheus text over atomics | `prometheus` or `metrics` crates | Fourteen metric families. Relaxed atomics on the hot path, no registry lock, no extra dependency. Would switch to a crate once histograms are needed. |
| Liveness vs readiness | `/healthz` ignores feeds, `/readyz` requires all feeds fresh | One health endpoint | An exchange outage should take the instance out of rotation, not get it restarted in a loop. |
| Reconnect backoff | Exponential 250 ms to 10 s, reset after 30 s healthy, no jitter | Fixed delay (the old 2 s); jittered backoff | One client per feed, so no thundering herd to spread out. Deterministic delays keep the tests exact. |

## Failure modes

| Failure | Detection | Handling | Tested by |
| --- | --- | --- | --- |
| Socket dropped mid-stream | read returns error or end | supervisor reconnects with backoff, counts `reconnects_total` | `fault_injection.rs`, `stream_faults.rs` |
| Half-open socket (no data, no error) | idle timeout (30 s) | session ends, reconnect | `rtds_silent_feed_times_out_and_is_flagged_stale` |
| Upstream refuses connections | connect error | backoff up to 10 s, retries forever | `supervisor_survives_refused_connections` |
| Malformed frame (bad JSON, NaN, wrong types) | parser rejects | price feeds skip and count it. The book feed resyncs, since a lost delta cannot be ruled out. | fault tests, fuzzing |
| Crossed book after reconciliation | check after every apply | not published, resync | `crossed_books_are_always_flagged`, fuzz target `clob_book_stream` |
| Sequence gap or duplicate (sequenced venues) | `SeqPolicy::Contiguous` | duplicate ignored. On a gap the book stops publishing until the next snapshot. | `book_props.rs` |
| REST snapshot fails or is slow during resync | request error or 10 s client timeout | logged, the stream subscribes anyway and the venue's own `book` event on subscribe seeds the book. The book publishes nothing until a snapshot arrives. | `deltas_before_a_snapshot_are_not_published` |
| Market rolls to the next five-minute contract | token from discovery changes (checked every second) | the old stream is dropped, a fresh `ClobBookStream` starts on the new token, and the recorder writes a control record. If discovery fails the old token stays until it succeeds. | `record` rollover in the live recording (2 rolls in 10 min) |
| Host clock skew | `poly_feed_lag_seconds` drifts or goes negative | staleness uses receive time on the host, so skew distorts lag only. RUNBOOK covers the NTP check. | none (operational) |
| Data goes stale | per-feed age vs threshold | `poly_feed_stale = 1`, `/readyz` 503, risk gate blocks decisions | `readyz_tracks_feed_freshness_and_kill_switch` |
| Disk slower than feeds | recorder queue full | frames dropped from recording, `poly_recorder_dropped_total` | `backpressure.rs` |
| Crash while recording | partial final record | reader returns all complete records and reports the tail | `every_truncation_point_yields_a_clean_prefix` |
| Bit rot in a recording | CRC-32 mismatch | read stops with an error at that record | `any_single_bit_flip_in_a_payload_is_caught`, fuzz target `recording_reader` |
| SIGTERM / SIGINT | signal handler | stop accepting HTTP, 5 s grace for open WebSockets, cancel feeds, drain and fsync recorder, exit 0 | `record_shutdown.rs` |

## Known gaps

- The Prometheus rules and Grafana dashboard have not been validated with promtool or loaded into Grafana.
- The CI container build and its smoke test have not been run yet.
- All measurements come from one Apple M5 laptop. Linux x86 numbers are still to be taken.
- `app/src/main.rs` (process wiring) has no unit coverage. It is exercised by the live runs and the shutdown check only.

## What would change at larger scale

- **Many contracts.** One `LocalBook` per token in a map, and a single CLOB connection subscribing to all of them (the market channel accepts a list of asset ids).
- **Many consumers.** Replace the Mutex slot with a shared-memory ring or a broker.
- **Long recordings.** Rotate files hourly and compress closed segments (the JSON compresses well). Index control records for seeking.
- **Histograms.** Per-stage latency histograms in `/metrics` (parse, apply, publish), which is the point to adopt a metrics crate.
