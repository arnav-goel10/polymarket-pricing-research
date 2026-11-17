# High availability plan

Design note for running the ingest side on more than one node. Milestones A, B and C are built: the event keys, merge and lease in `crates/ha`, the stream, client and chaos harness, and two nodes on Azure with a Blob lease and archive. The build record is [HA_BUILD_SPEC_B_C_AZURE.md](HA_BUILD_SPEC_B_C_AZURE.md), the live drill's method [HA_DRILL.md](HA_DRILL.md), and operation [AZURE_RUNBOOK.md](AZURE_RUNBOOK.md).

Note: CI also runs on pushes to `ha-failover` (the `push` branches in `.github/workflows/ci.yml`). Remove that entry before merging to main.

Goal: a distributed setup with a measured failure guarantee, run live with alerts and incident write-ups, built so other people can use the pieces.

## Decision

Hot-hot ingest with client-side merge, not consensus. Three designs were drawn and attacked: a Raft log, NATS JetStream, and active-standby. All three were over-scoped, and the strongest objection was the same each time. Every node reads the same venues, so the venue is the source of truth, and a replicated log solves a problem this system does not have.

## Shape

1. Two or three identical nodes, each ingesting the feeds on its own.
2. Every event gets an identity built only from venue fields: the Binance trade id and book update id with the engine time, RTDS topic, symbol and timestamp, CLOB asset id, hash and timestamp (plus side, price and size for price changes, see below).
3. A client library subscribes to two nodes at once and merges by event key, so one node dying loses nothing and delivers nothing twice.
4. A lease with a fencing epoch picks the one node that ships recordings to the archive. As built it is one Blob, `lease/HEAD.json`, written only with an If-Match conditional put, so the grant, the epoch and each archive commit are one atomic write. An old leader that wakes up is refused because the blob has moved on.
5. A chaos check in CI on simulated and recorded data, and a live drill of node kills and leader freezes on the two Azure VMs. The numbers to report: events missing and delivered twice against the union of the node recordings, added delay, time to two live subscriptions again, and archive takeover time.

Out of scope: Raft, a broker, and a public raw-data stream. Re-serving exchange data publicly risks the venues' terms, so the public surface is the status page, the metrics and the code.

## Milestones

- **A. Event identity, merge and lease core.** Includes a tool that measures key uniqueness on the real recording. No cloud needed; builds in GitHub Actions. Done, see the High availability section of [DESIGN.md](DESIGN.md).
- **B.** Stream API with snapshot, sequence and resume. Client library with hedged subscribe. Chaos harness in CI with mutants that must fail. Done.
- **C.** Cloud: two nodes, a lease, an archive, a budget alarm. Built on Azure, not AWS, because Azure for Students credit was there: two B-series VMs in different zones and CPU architectures, Bicep instead of Terraform, the Blob lease above instead of DynamoDB, and deploys from GitHub Actions with no stored secret. The epoch in an object's name is not the fence (a zombie can always create a new name); the conditional write on HEAD is, and readers trust only what HEAD references. Deployed on 17 November 2026. The live drill is built and is the next step.
- **D.** Alerts to a phone, a public status page, runbook updates.
- **E.** Operate for weeks, a postmortem per incident, Linux numbers, a README rewrite, and decisions on the repo name and on publishing crates.

## What milestone A changed in the plan

The plan's first CLOB key for a price change was asset id, hash and timestamp. Measured on the committed live recording it collided 298 times: different frames at the same millisecond carry the same hash for the same asset with a different price. Adding side and price to the key removed every collision on that file. Size was added later by decision, not from data: the key without size also has 0 collisions there. `keycheck` prints all three numbers so the reason stays visible.

The hardening pass after milestone A also put the venue timestamp into the key (so the merger's window cannot be fooled by copies stamped differently), replaced the merge floor with the exact key the cap pushed out, and made the sink check each write against the live grant instead of waiting for the new leader to advance it.
