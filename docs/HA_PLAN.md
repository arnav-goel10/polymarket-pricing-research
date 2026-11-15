# High availability plan

Design note for running the ingest side on more than one node. Milestone A is built in `crates/ha`; the rest is planned.

Goal: a distributed setup with a measured failure guarantee, run live with alerts and incident write-ups, built so other people can use the pieces.

## Decision

Hot-hot ingest with client-side merge, not consensus. Three designs were drawn and attacked: a Raft log, NATS JetStream, and active-standby. All three were over-scoped, and the strongest objection was the same each time. Every node reads the same venues, so the venue is the source of truth, and a replicated log solves a problem this system does not have.

## Shape

1. Two or three identical nodes, each ingesting the feeds on its own.
2. Every event gets an identity built only from venue fields: the Binance trade id and book update id, RTDS topic, symbol and timestamp, CLOB asset id, hash and timestamp (plus side and price for price changes, see below).
3. A client library subscribes to two nodes at once and merges by event key, so one node dying loses nothing and delivers nothing twice.
4. A lease with a fencing epoch (a DynamoDB conditional write in production) picks the one node that ships recordings to S3. An old leader that wakes up is refused because its epoch is stale.
5. A chaos check in CI on recorded data, and a weekly live kill on AWS. The numbers to report: client-observed gap, duplicates delivered, and events missing against the union of the node recordings.

Out of scope: Raft, a broker, and a public raw-data stream. Re-serving exchange data publicly risks the venues' terms, so the public surface is the status page, the metrics and the code.

## Milestones

- **A. Event identity, merge and lease core.** Includes a tool that measures key uniqueness on the real recording. No cloud needed; builds in GitHub Actions. Done, see the High availability section of [DESIGN.md](DESIGN.md).
- **B.** Stream API with snapshot, sequence and resume. Client library with hedged subscribe. Chaos harness in CI with mutants that must fail.
- **C.** AWS: Terraform, two or three t4g.nano or micro nodes, DynamoDB lease, S3 archive, a Budgets alarm. Needs the AWS account.
- **D.** Alerts to a phone, a public status page, runbook updates, the first live kill drill.
- **E.** Operate for weeks, a postmortem per incident, Linux numbers, a README rewrite, and decisions on the repo name and on publishing crates.

## What milestone A changed in the plan

The plan's first CLOB key for a price change was asset id, hash and timestamp. Measured on the committed live recording it collided 298 times: different frames at the same millisecond carry the same hash for the same asset with a different price. Adding side and price to the key removed every collision on that file. `keycheck` prints both numbers so the reason stays visible.
