# Runbook

How to tell whether the app is healthy and what to do when it is not. Alert rules live in [`ops/prometheus/alerts.yml`](../ops/prometheus/alerts.yml) and a dashboard in [`ops/grafana/poly-feeds.json`](../ops/grafana/poly-feeds.json). Neither file has been validated with promtool or loaded into Grafana yet.

Feeds are labelled `feed="rtds"` (reference price), `feed="binance"` (volatility input) and `feed="clob_book"` (Polymarket order book).

## Endpoints

| Path | Meaning | Use for |
| --- | --- | --- |
| `/healthz` | 200 while the process serves HTTP. Ignores feeds. | Liveness probe. A failure means restart. |
| `/readyz` | 200 only when every feed is fresh and the run is not stopped. 503 with `stale_feeds` otherwise. | Readiness probe and load-balancer membership. Never restart on it. |
| `/metrics` | Prometheus text format. | Scraping. |
| `/health` | Older JSON summary for the dashboard. | Humans. |

## Signals

### poly_feed_stale

1 when a feed's last message is older than its threshold (5 s for `rtds` and `binance`, 3 s for `clob_book`).

- **Alert:** `PolyFeedStale`, page after 30 s.
- **Effect:** a stale `rtds` or `clob_book` blocks every simulated decision and `/readyz` returns 503.
- **Check:** `poly_feed_connected` for the same feed. If connected but stale, the upstream is quiet or half-open, and the idle timeout will cycle the connection within 30 s. If disconnected, see the next section.
- **A quiet market is normal near window boundaries.** `clob_book` can go stale for a few seconds while the next contract's market is discovered.

### poly_feed_connected

1 while the socket is open.

- **Alert:** `PolyFeedDisconnected`, page after 2 min.
- **Check:** logs, filtered to the feed: `LOG_FORMAT=json` then `jq 'select(.fields.feed=="binance")'`. Each retry logs `error`, `session_ms` and `retry_in_ms`.
- **Common causes:** DNS or egress failure (try `curl -I https://clob.polymarket.com`), upstream maintenance, TLS interception on the host.
- **Action:** nothing to restart. The supervisor retries forever at up to 10 s intervals. Fix the network path.

### poly_feed_reconnects_total

Counts sessions that ended and were retried.

- **Alert:** `PolyFeedReconnectStorm`, ticket when more than 10 in 10 min.
- **Meaning:** short sessions back to back. The backoff only resets after a session lasts 30 s, so flapping already slows itself down.
- **Check:** `retry_in_ms` in the logs climbing towards 10,000 confirms flapping. Look at the `error` field: `timeout` means a silent upstream, `closed by server` usually means the venue is rate-limiting or rotating connections.

### poly_feed_resyncs_total

Order-book resyncs: a gap, a malformed level, or a book still crossed after trimming to the venue's best bid and ask.

- **Alert:** `PolyBookResyncs`, ticket when more than 3 in 15 min. The 150 s live check had 0.
- **Check:** the `book resync` log line names the cause: `Crossed`, `Malformed`, or `not valid JSON`.
- **`Crossed` repeating** suggests the venue changed how it reports fills or top of book. Capture frames with `cargo run --release -p app --bin record -- /tmp/x.prec --secs 60` and replay them with `latency_report --recording /tmp/x.prec` to reproduce offline.
- **`Malformed`** points to an upstream schema change. The same capture works.

### poly_feed_malformed_total

Frames that could not be decoded. The RTDS empty subscription ack and `PONG` are not counted.

- **Alert:** `PolyMalformedFrames`, ticket above 0.1 per second for 5 min.
- **Action:** record a sample as above and add the frame to the parser tests and the fuzz corpus (`fuzz/corpus/<target>/`).

### poly_feed_messages_per_second

Average over the last 10 complete seconds. In a live run, `clob_book` was about 1,100 per second and `binance` about 100 to 200.

- **Alert:** `PolyFeedSilentButConnected` when 0 while connected for 1 min. The idle timeout should already have fired, so this alert means that guard failed.
- **A sudden doubling** is usually a volatile market and is fine. Check `poly_recorder_queue_depth` if recording.

### poly_feed_lag_seconds

Receive time minus the exchange timestamp of the latest message.

- **Alert:** `PolyFeedLagHigh` when above 2 s for 2 min, on `binance` and `clob_book` only.
- **Expected values:** live, `binance` was about 0.04 s and `clob_book` about 0.12 s. `rtds` sits around 1.4 s, because the Chainlink relay itself lags, so it is excluded from the alert.
- **Check:** host clock first (`sntp -t 1 time.apple.com`, or `chronyc tracking` on Linux). Negative lag means the host clock is behind. Then network RTT to the venue.

### poly_recorder_dropped_total, poly_recorder_queue_depth

Present only when `RECORD_PATH` is set.

- **Alerts:** `PolyRecorderDropping` on any drop in 5 min. `PolyRecorderQueueFilling` when depth stays above half of `poly_recorder_queue_capacity` for 1 min.
- **Meaning:** the disk cannot keep up. Drops never slow the feeds (by design), but the recording now has gaps and is not suitable for exact replay.
- **Action:** move `RECORD_PATH` to a faster local disk. Live sessions write roughly 1 MB per second.

### poly_uptime_seconds

- **Alert:** `PolyRestarted` (info) below 300.
- **Action:** a clean stop logs `shutdown requested` with the signal, then `feed tasks stopped`, then `shutdown complete`. A restart without those lines was a crash, so collect the logs.

## Process down

`PolyDown` fires when Prometheus cannot scrape. Check the process, then `/healthz`. On restart, the app reloads `shadow-data/trades.json` and `run_state.json` and rebuilds the P&L curves from the ledger.

## Planned stop

Send SIGTERM (or Ctrl-C). The app stops accepting HTTP, gives open dashboard WebSockets 5 s, cancels feed tasks, then drains and fsyncs the recorder before exiting 0. A recording interrupted by a hard kill is still readable up to its last complete record.
