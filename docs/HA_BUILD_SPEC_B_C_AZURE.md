# Poly HA build spec: milestones B and C on Azure

Read-only design. Nothing was edited, committed, pushed or created. Checked this session: branch `ha-failover` at e813cf6, `crates/ha` API (`frame_keys`, `Merger::offer`, sync `Lease` trait, `FrameKind` = Control, Noise, Venue), `Source` numbers 1,2,3,4, default Binance URL already subscribes `aggTrade` and `bookTicker`, workspace TLS is native-tls, fixture is 3,999,803 bytes, `az account show` = Azure for Students, Enabled. Azure SKU, policy and price facts are taken from the attack's verified list, not re-run. The third design arrived truncated, so this merges the two complete ones.

## 1. Corrections to the brief and the old plan

| Item | Brief or old doc | This spec |
|---|---|---|
| Region | southeastasia | **eastasia**. Subscription policy allows only centralindia, japaneast, indiasouthcentral, eastasia, koreacentral |
| VM sizes | "B-series" | node-a `Standard_B2ats_v2` (x64, zone 2), node-b `Standard_B2pts_v2` (Arm64, zone 1). B1s is blocked in every allowed region |
| Fencing | HA_PLAN: epoch in object name plus conditional put | Not a fence. A zombie can always create a new name. The fence is the If-Match commit on one HEAD blob, and readers trust only what HEAD references |
| Data rate | DESIGN.md: fixture is "about 10 minutes" | Fixture is 9,941 records in 4.99 s, about 2,000 frames/s and 0.8 MB/s (RUNBOOK agrees, 425 MB per 10 min). All sizing below uses this |
| Triggers | schedule or dispatch | Tag pushes only. `schedule` and `workflow_dispatch` need the file on main |

## 2. Shape

- No new crates. New modules in `crates/ha`: `wire`, `ring`, `client`, `store`, `caslease`, `azure`. New in `crates/app`: `ingest`, `archiver`, bins `node`, `venue_sim`, `chaos`.
- Two VMs run the bare `node` binary under systemd. No Docker, no Caddy, no TLS, no tokens, no status page (milestone D).
- Stream binds `127.0.0.1:9100` only. The drill client on the GitHub runner reaches it through a per-run SSH port-forward. HTTP (`/healthz /readyz /metrics /ha`, no raw data) binds the private IP, reachable from the VNet only.
- One blob, `lease/HEAD.json`, is lease store and archive manifest. Every grant, renew and commit is one If-Match Put Blob.
- IaC is **Bicep**: no state file to host or protect, already inside az on the Mac and runners, `what-if` and `az bicep build` give a cloud-free lint, and Terraform's azuread provider would want directory rights the university tenant will not give.

## 3. Milestone B: stream, client, chaos in CI

### 3.1 Tap and ingest
- `feeds::recorder::QueueOnly::start_with(on_frame, on_done)`. `start` delegates to it. Drop policy unchanged.
- `app::ingest::spawn_feeds(...)` holds `book_loop`, `current_token`, `shutdown_signal` moved from `bin/record.rs`. `record_shutdown.rs` passes unchanged.

### 3.2 Ring (`ha::ring`, pure)
- `seq` starts at 1 per boot, assigned only to frames where `frame_keys` returns `FrameKind::Venue`. Noise and Control are not forwarded. `boot` is a random u64.
- Caps: 64 MiB or 200,000 frames, whichever first (about 80 s at the measured rate). Preallocated.
- Resync slots kept outside the ring: latest RTDS frame, latest Binance frame per kind, latest CLOB `book` per asset id. A Control subscribe record clears the CLOB slots.
- `plan(resume)`: same boot and seq inside the ring gives `Resume{next}`. Otherwise `Snapshot{frames, next, complete}` = slot frames plus every retained CLOB frame since the oldest current book. `complete=false` when that tail has left the ring.

### 3.3 Wire v1 (`ha::wire`, JSON text over WebSocket)
- Client: `{"t":"sub","v":1,"feeds":[..],"resume":{"boot":"..","seq":N}|null}`
- Node: `hello{v,node,boot,mode,next,oldest,head,complete}`, `ev{seq,src,rx,snap,frame}`, `snap_end{next}`, `hb{head,epoch,leader}` every 250 ms, `bye{reason: shutdown|lagged|bad_request}`.
- Keys are not on the wire. The client derives them with `ha::key::frame_keys`, the same code as the node.
- No per-connection queue. A connection whose next seq falls below `oldest` gets `bye lagged`, then reconnects into a snapshot.

### 3.4 Node (`bin/node`)
- Per frame, in order: append to the open segment, compute keys, push to the ring.
- Local segments `<data>/seg/<slot>.<boot>.prec.open`, slot = minute. Flushed every 1 s, renamed `.prec` at slot end, raw. Keep 60 slots (about 3 GB of the 30 GB disk).
- Refuses to start if `POLY_MUTANT` is set in a build without the `mutants` feature.

### 3.5 Client (`ha::client`)
- Sans-IO `Core` plus a thin tokio-tungstenite driver. Subscribes to both nodes at once. Every frame from every node goes through one shared `Merger`.
- A live `seq != expect` counts `seq_gaps` and reconnects with resume at the last contiguous seq. `mode=resume` with `next != seq+1` counts `resume_violations`.
- No `hb` or `ev` for 1 s means the connection is dead. Redial with 100 ms to 2 s jittered backoff.
- The delivered log records Deliver and Late verdicts (private file, never an artifact).
- `BookView` helper: drop a CLOB delta whose venue timestamp is below the latest delivered `book` for that asset.
- Not guaranteed and reported: cross-node order (`order_inversions`).

### 3.6 Simulator (`bin/venue_sim`)
- Three WS listeners, one generator, deterministic from `--seed`, `--rate` frames/s.
- **On every new RTDS connection it sends a history snapshot (`payload.data[]`) of the last 60 points, and on every new CLOB connection the latest `book` frames.** This is how real venues behave and is what makes Late verdicts appear in CI.
- `--lag-ms K` delays every second connection. `--manifest out.prec` writes every broadcast frame, so truth is a `.prec` file and CI and live share one verify path.
- `--recording fixtures/live-sample.prec --rate N` plays real frames once, paced by rate.

### 3.7 Chaos (`bin/chaos`: `local`, `plan`, `client`, `verify`, `verify-archive`)
Faults: `kill9` (SIGKILL, respawn after 1 s) and `freeze(ms)` (SIGSTOP then SIGCONT). Before each fault the other node's `/readyz` must have been 200 for 500 ms.

Window W is cut by **venue timestamp of the key**, not receive time: `T0 <= ts < T1`, T0 = 15 s after both subscriptions are Live, T1 = 15 s after the last recovery, client runs 15 s past T1.

| Check | Rule | Fail reason |
|---|---|---|
| A1 | timed truth keys in W never delivered = 0 | `missing` |
| A2 | no timed key delivered twice, whole run | `duplicate` |
| A3 | delivered keys absent from truth = 0 (CI only) | `extra` |
| A4 | executed == planned, client saw a disconnect per fault | `faults_not_observed` |
| A5 | each node delivered first copies, truth events exist inside every fault window | `vacuous` |
| A6 | `seq_gaps`, `resume_violations`, `unkeyed` = 0 | `seq_gap`, `resume`, `unkeyed` |
| A7 | two nodes down at once | exit 2, invalid |

Late is **not** gated at zero. After the run each Late key is classed `late_already_delivered`, `late_pre_window` (ts < T0) or `late_lost`. `late_lost` keys are a subset of `missing`.

Mutants, behind `--features mutants`, each must exit 1 with exactly the named reason:

| Mutant | Expect |
|---|---|
| client no-dedup | `duplicate` |
| client single-node | `missing` |
| client no-reconnect (kill A then B) | `missing` |
| node seq-skip | `seq_gap` |
| node stale-resume | `resume` |
| harness overlap-kill | `missing` (proves the detector sees a gap) |
| node dup-frame: every 500th frame sent again under a **fresh contiguous seq** | must PASS with 0 duplicates and merger duplicate counter > 0 (proves dedup is by key) |

CI matrix: synthetic 90 s, 12 faults, seed 1 at 200 frames/s. Seed 2 at **2,000 frames/s**. Fixture playback at rate 300 with 3 faults. Then each mutant at 45 s.

## 4. Milestone C: lease, archive, Azure

### 4.1 Lease store choice
| Option | Verdict |
|---|---|
| Native Blob lease | Rejected. Gives a lease id, not an epoch. The epoch would be a second write, so grant and epoch are not atomic |
| Table Storage entity with ETag | Same CAS, cheaper per write, but a second API and role when Blob is already needed |
| **Blob ETag conditional Put Blob on one JSON blob** | Chosen. One request stores `epoch+1` and the holder, so the grant and the persisted epoch are the same atomic write, which is the contract in `lease.rs` |

### 4.2 HEAD document `lease/HEAD.json`
```
{"v":1,"epoch":7,"holder":2,"ttl_ms":30000,"nonce":"<128-bit per write>",
 "tenures":[{"e":6,"n":1,"from":28862001,"to":28862040},{"e":7,"n":2,"from":28862041,"to":28862055}]}
```
- **Acquire**: read HEAD and ETag. Take over only after watching the ETag stay unchanged for `ttl + 1 s` on my own monotonic clock, or at once if `holder` is null. Write `epoch+1`, holder=me, with If-Match. 404 means create at epoch 1 with If-None-Match `*`. A restarted node is a plain contender, no special case.
- **Renew** every 10 s: same epoch, new nonce, If-Match. Holder deadline = monotonic time the last successful renew was **sent** + ttl - 2 s.
- **Commit slot n**: upload the object, then write HEAD with the current tenure's `to = n` (first commit of an epoch appends a tenure with `from = previous to + 1`), If-Match. A commit is a renew.
- **Deadline passed**: the node stops starting uploads and makes one renew CAS. 412 counts `poly_fence_rejected_total` and it becomes a follower. Success means nobody took over and it carries on. Expiry is advisory, the CAS is the truth.
- **Unknown outcome** (timeout after send): read HEAD. Nonce mine = landed. Higher epoch = fenced. ETag unchanged = retry.
- **Release** on SIGTERM: holder=null, epoch kept.
- Tenures older than 3 days are dropped, so the document stays bounded.
- No Date or Last-Modified parsing anywhere. Wall-clock offset between hosts has no effect.

### 4.3 Honest contract
- The sync `Lease` trait is **not** implemented for Blob. No store can evaluate "now", so renew can succeed after expiry when nobody took over, and a closure-style `fence` cannot be atomic. `MemoryLease`, `Fenced` and the existing property stay as they are.
- `ha::caslease::CasLease` is a sans-IO state machine over `HeadStore { read, cas }`, checked by a second, weaker model. Safety: epochs strictly rise, one holder per epoch, no commit is accepted under epoch E once a grant of E' > E exists, tenures are append-only. Belief: with bounded drift and no pauses at most one node believes it leads. Liveness: a silent holder is replaced within ttl + 1 s + 2 polls.
- Property schedule: 3 nodes, per-node clock rate error, pauses, store latency, failures, applied-but-timed-out writes.
- Mutants that must be caught: epoch reuse, commit without If-Match, deadline timed from response receipt, takeover without a full-TTL unchanged observation, unknown outcome treated as failure and committed again.
- Timing: ttl 30 s, renew 10 s, poll 5 s. Expected takeover 31 to 41 s after the fault.

### 4.4 Archive
- Object `seg/e<epoch:010>/s<slot:010>.prec.gz`, created with If-None-Match `*`. 409 and 412 both map to PreconditionFailed, which on a retry means my earlier attempt.
- Archiver thread, every 5 s: for each closed local slot above the committed head (look back 60 slots), stream the slot's local files through `RecordWriter` into a gzip temp file (constant memory, not 100 MB on a 1 GiB VM), upload, commit.
- **Reader rule**: slot n is archived iff exactly one tenure covers n and `seg/e<that epoch>/s<n>` exists. A covered slot with no object is a hole (counted). Any object outside its epoch's tenure range is an orphan from a fenced writer. The skipped-slot zombie case from the attack lands as hole plus orphan, never as a winner.
- A new leader starts at `previous to + 1` from its own local segments, so a failover leaves no hole while the outage is under 60 minutes.
- Lifecycle: delete `seg/` and `drill/` after 2 days, `releases/` after 14 days. No rule touches `lease/`.
- Stores: `MemStore` (fault injection), `FsStore` (directory, `File::lock`, append-only journal of every HEAD write, used by multi-process `chaos local`), `AzureStore` (reqwest blocking, IMDS token, `x-ms-version 2023-11-03`, single-shot Put Blob, 5 s timeout). One contract suite runs against all three, against Azure as a deploy step on prefix `selftest/`.
- Archive mutants in `chaos local` with `FsStore`: no-fence (commit with no condition) must fail `stale_epoch_commit`, epoch-reuse must fail `epoch_two_holders`. `verify-archive` reads the journal in CI and the final HEAD plus both nodes' `lease.jsonl` on Azure.

### 4.5 Infrastructure
`infra/bootstrap.bicep` (subscription scope, run once from the Mac, no VM, returns in seconds with `--no-wait`): resource group, two user-assigned identities, federated credential, storage account and container, role assignments, budget (behind a `deployBudget` flag, start date as a parameter, so a redeploy cannot break it).

`infra/main.bicep` (resource group scope, from Actions): VNet, NSG, two public IPs, two NICs, two VMs with cloud-init. The admin SSH public key and client, tenant and subscription ids come from GitHub environment variables, never the repo.

cloud-init: users `poly` and `drill` (nologin), `/opt/poly`, `/var/lib/poly`, `poly-node.service` (`Restart=always`, `RestartSec=1`, `MemoryMax=600M`).

### 4.6 Deploy with no stored secret (`.github/workflows/azure.yml`)
- Triggers: tag push only, `ha-infra-*`, `ha-deploy-*`, `ha-drill-*`, `ha-stop-*`, `ha-destroy-*`. An ordinary branch push never touches Azure.
- Every job uses environment `azure`, `permissions: id-token: write, contents: read`, `azure/login@v2` with OIDC.
- `ha-infra`: `what-if`, then create.
- `ha-deploy`: build `node` on `ubuntu-24.04` and `ubuntu-24.04-arm` without the mutants feature, upload to `releases/<sha>/node-<arch>` with `--auth-mode login`, start the VMs if deallocated, then per node `az vm run-command invoke` runs `install.sh <sha> <sha256>` (IMDS token, digest check, symlink flip, restart). Gate before the second node: `/readyz` 200 and all three feeds connected. Then the live contract suite.
- `ha-stop`: deallocate both. `ha-destroy`: delete the group.
- No step prints frames. The repo is public, so logs and artifacts are public.

### 4.7 Live drill (`ha-drill-*`, about 40 minutes of runner time)
1. OIDC login, build `chaos`.
2. Per-run ed25519 key. `drill-open.sh` adds it for user `drill` as `restrict,port-forwarding,permitopen="127.0.0.1:9100"` and returns the host key.
3. NSG rule `drill-<run_id>`: tcp/22 from the runner IP only.
4. Two SSH forwards. Main client on both nodes, plus a **control client on node-a only**.
5. Plan from `--seed <run_id>`: 20 faults, 90 s apart, 14 `kill9` alternating nodes, 6 `freeze_leader` of 60 s (longer than the worst takeover, so the frozen leader is a real zombie).
6. `chaos-agent.sh` runs detached under `systemd-run`, acts at absolute UTC times, freezes only if local `/ha` says leader, skips a kill unless the peer's `/readyz` is 200, logs to `faults.jsonl`.
7. `drill-collect.sh` uploads `faults.jsonl`, `lease.jsonl`, a metrics snapshot and gzipped drill-window segments to `drill/<run>/n<id>/`.
8. Runner downloads them, runs `chaos verify` (truth = union of both nodes' recordings in W) and `chaos verify-archive`.
9. `always()`: delete the NSG rule, remove the key, kill ssh, delete downloaded data.
10. Only `report.json` (counts and timings) becomes an artifact and job summary. The owner commits it under `bench-results/ha-drills/` himself.

Run is **invalid** if the control client shows 0 missing after node-a kills, or if executed faults differ from planned.

## 5. Numbers the system reports

All from `chaos verify` into `report.json`. Every time is on **the runner's monotonic clock**. Fault instant = the last `ev` or `hb` received from the victim.

| Number | How it is measured | Gate |
|---|---|---|
| `faults.executed`, by type | plan vs client-observed disconnects and `faults.jsonl` count | == planned |
| `missing` | timed truth keys in W never delivered. Truth = sim manifest in CI, union of both node recordings on Azure | 0 |
| `duplicates` | timed keys delivered more than once, whole run | 0 |
| `extra` | delivered keys in no truth source | 0 in CI, reported on Azure (a killed node can forward a frame and lose its last 1 s of file) |
| `late_already_delivered`, `late_pre_window`, `late_lost` | Late verdicts resolved against the delivered log after the run | `late_lost` = 0 |
| `content_mismatch` | same key, different content across the two recordings | 0 (Azure) |
| `added_delay_ms` p50, p99, max | lag = deliver time minus the delivering node's rx stamp, minus **that node's own** steady-state median, over the 5 s after each fault | reported |
| `rehedge_ms` p50, max | fault instant to the client again holding two Live subscriptions | reported |
| `archive.takeover_ms` p50, max | leader fault instant to the first `hb` with a higher epoch and `leader:true` | <= 45,000 |
| `archive.stale_epoch_commits`, `epoch_two_holders` | tenure list and `lease.jsonl` | 0 |
| `archive.fence_rejected` | sum of `poly_fence_rejected_total` | >= executed leader freezes |
| `archive.holes`, `orphans` | reader rule | reported |
| `control_missing` | the single-node client | > 0 or run invalid |
| `binance_agg_id_gaps` | breaks in aggregate trade id `a` in the delivered stream inside W, the only truth source outside the system | reported |
| `seq_gaps`, `resume_violations`, `unkeyed`, `resets`, `reconnects`, `incomplete_snapshots`, `order_inversions` | client counters | first three 0, rest reported |

Headline this supports once drills have run: "Across N node kills and M leader freezes on two Azure VMs in different zones and CPU architectures, a client subscribed to both saw 0 missing and 0 duplicate events against the union of both nodes' recordings, worst added delivery delay X ms, redundancy restored within Y ms, archive leadership moved within Z s with 0 commits under a stale epoch. The same harness fails on 6 seeded stream faults and 7 lease and archive faults in CI."

Limits the method page must state: union truth cannot see an event both nodes missed, there is no takeover on the data path so X is added delay not a failover time, and cross-node order is not guaranteed.

## 6. Ordered steps (each ends in a green GitHub Actions run on `ha-failover`)

| # | Step | Package | Green run |
|---|---|---|---|
| 1 | Tap, `serve_broadcast`, ingest extraction | P1 | `rust` job, `record_shutdown` unchanged |
| 2 | `ha::wire`, `ha::ring` with property test | P1 | `rust` job |
| 3 | `bin/node`, `stream_roundtrip` test | P1 | `rust`, `deny` |
| 4 | `ha::client` Core, driver, `BookView` | P2 | `rust` job |
| 5 | `venue_sim`, `chaos local`, mutants, new `chaos` job | P2 | `chaos` job: 3 baselines pass, 6 mutants fail by name, control passes. **Milestone B done** |
| 6 | `ha::store`, `ha::caslease`, Mem and Fs stores, property and 5 mutants | P3 | `rust` job |
| 7 | Archiver, `verify-archive`, freeze faults, 2 archive mutants | P3 | `chaos` job with archive lines |
| 8 | `AzureStore`, Bicep, ops scripts, `azure.yml` | P4 | `evidence` job: `az bicep build`, `shellcheck`, header and status-mapping unit tests |
| G1 | Owner gate: approve the Azure list, run the Mac commands | | |
| 9 | Tag `ha-infra-1` | P4 | `azure.yml` infra job |
| 10 | Tag `ha-deploy-1` | P4 | deploy job: feeds gate on both nodes, live contract suite, one leader, slots committing |
| G2 | Owner gate at 72 h: read Cost analysis, pick always-on or drill-only | | |
| 11 | Tag `ha-drill-rehearsal` (4 faults), read the public log for leaks, then `ha-drill-1` (20 faults) | P5 | drill job verdict pass, `report.json` artifact |
| 12 | Docs: HA_PLAN, DESIGN, new HA_DRILL method page, fix the fixture-length line | P5 | `rust` and `evidence` jobs |

No work lands on main, no PR is opened, no workflow commits or pushes, commits carry no AI attribution. The HA_PLAN note stays: remove `ha-failover` from the CI push branches before any merge.

## 7. Cost and how to stop

Region eastasia, Azure Retail Prices API, 730 h month.

| Resource | Unit price | Monthly |
|---|---|---|
| node-a B2ats_v2 | $0.0131/h | $0 if free hours apply, else $9.56 |
| node-b B2pts_v2 | $0.0116/h | $0 if free hours apply, else $8.47 |
| 2 OS disks, 30 GB Standard HDD S4 | $1.536 each | $3.07, plus about $0.1 to $0.5 disk transactions (estimate) |
| 2 Standard static public IPv4 | $0.005/h each | $7.30 |
| Blob writes, about 302k (renew or commit every 10 s, one upload a minute) | $0.05 per 10k | $1.51 |
| Blob reads, about 518k follower polls | $0.004 per 10k | $0.21 |
| Blob data, Hot LRS, gzip, 2-day lifecycle | $0.024/GB | $0.35 to $1.10 (estimate 5 to 15 GB/day compressed from 69 GB/day raw, measure on day one) |
| Egress, VNet, NSG, identities, run-command, budget | free tier | $0 |

- Free VM hours apply: about **$13 a month** (range $12 to $15), about 7 months of credit.
- Free hours do not apply, always on: about **$31 a month**, about 3 months. Then switch to drill-only.
- Drill-only (deallocated between drills): about **$11 a month**, about 9 months.
- The free-hours claim is from the Students page text. Nobody has seen a zero meter on this subscription. Gate G2 settles it.

Stop: tag `ha-stop-<date>` or `az vm deallocate -g poly-ha -n node-a --no-wait` and node-b (disks, IPs, storage keep billing). Stop everything: tag `ha-destroy-<date>` or `az group delete -n poly-ha --yes --no-wait`, cost goes to $0. Hard ceiling: no card, so Azure disables the subscription when the credit ends.

## 8. Risks

- Binance or Polymarket may refuse Hong Kong cloud addresses. The deploy feeds gate catches it on day one. Fallback is japaneast (B2ts_v2 plus B2pts_v2, only one free size).
- Burstable CPU at 2,000 frames/s with TLS, key parsing and gzip may drain credits. Read "CPU Credits Remaining" at G2.
- Quotas unread: need 4 vCPUs and 2 public IPs. Read after provider registration.
- Budgets may be unsupported on the Students offer or need 48 h. The flag lets bootstrap proceed without it.
- B2pts_v2 exists only in zone 1 of eastasia.
- The gzip ratio is an estimate. If day-one data is above 15 GB/day, cut the lifecycle to 1 day.
- A real drill can go red if both nodes' RTDS feeds drop together for longer than the 10 s merge span. That is reported as `late_lost`, not hidden.
## 9. Build notes: choices where this spec was open

Recorded while building, each the smaller option.

### P1 (steps 1 to 3)

- `QueueOnly::start_with(state, on_frame, on_done)` takes a state value that `on_frame` gets by `&mut` and `on_done` gets back, so `start` delegates with the `RecordWriter` as the state and no shared ownership. `on_frame` owns the frame text, so the node moves it into the ring without a copy.
- `serve_broadcast` is a `feeds::testkit` helper: one tokio broadcast channel fanned out to every open WebSocket connection, with a per-connection greeting (a venue's snapshot on subscribe). The node test drives the three feeds with it and `venue_sim` can reuse it.
- `sub.feeds` must be empty (all) or name all three feeds. Anything else gets `bye bad_request`, because a filtered stream has holes in `seq` that a client cannot tell from loss.
- `boot` is sent as 16 hex digits, `src` as the recording source number, `rx` in microseconds.
- A resume is served when the boot matches and `oldest <= seq + 1 <= head + 1`. The ring always keeps its newest frame, even when that one frame is over the byte cap.
- The latest RTDS slot is the latest RTDS venue frame. A Binance slot is set only by a keyed `aggTrade` or `bookTicker` frame. A CLOB slot is set by any CLOB frame holding a `book` with an `asset_id`, one slot per asset, and a frame with books for two assets fills both.
- `complete` is false only when a CLOB frame after the oldest current book has left the ring, that is when that book's seq + 1 < `oldest`. With no current book it is true.
- `hb` sends `epoch: 0, leader: false` until the lease lands in P3.
- A slow client is never queued for. A send or flush that takes over 5 s drops the connection, and a client whose next seq has left the ring gets `bye lagged`.
- Segments flush when a frame arrives at least 1 s after the last flush, and roll when the first frame of a later minute arrives, so a silent feed can leave up to a second unflushed until the next frame or shutdown. A clock that steps back keeps writing into the open slot. Pruning deletes any boot's segment whose slot is 60 or more behind the newest.
- A failed segment write is counted in `poly_node_segment_errors_total` and the frame still goes to the ring, so a full disk does not stop the stream.
- `--listen` (default `127.0.0.1:9100`) must be loopback or the node exits 2. `--http` defaults to `127.0.0.1:9101`; the deploy passes the private IP. `/readyz` is 200 when all three feeds are connected. `--ring-frames` and `--ring-bytes` exist for tests.
- The node mutants are chosen at run time by `POLY_MUTANT` (`seq-skip`, `stale-resume`, `dup-frame`) and honoured only with `--features mutants`. Without the feature the node exits 2 and says it refuses to start. `seq-skip` leaves out each ev whose seq is a multiple of 1,000. `stale-resume` answers every same-boot resume at `head + 1`. `dup-frame` pushes every 500th venue frame again under the next seq.
- Until the P2 client exists, `stream_roundtrip` holds a minimal checker (seq contiguity, resume next, repeated keys). The P2 chaos job takes over the named-reason gating from section 3.7.

### P2 (steps 4 and 5)

- `ha::client` adds tokio, tokio-tungstenite and futures-util to `ha`. All three were already in the workspace, so no new crate enters the lock file. The client faults sit behind a `mutants` feature on `ha`, which the `app` feature of the same name turns on.
- A live `ev` with an unexpected `seq` is counted and not offered, the connection ends, and the redial resumes at the last contiguous `seq`. A resume `hello` when the client sent no resume, or sent one for another boot, also counts as a `resume_violation`. After a violation the client follows the node's `next`.
- The 1 s dead clock starts at the dial, so a connect into a frozen node is dead after 1 s too and there is no separate connect timeout. Backoff is 100 ms doubling per failed try, capped at 2 s, scaled by a jitter in [0.5, 1] and never under 100 ms. It starts again at 100 ms when a connection goes live.
- `order_inversions` counts a delivered timed key older than the newest delivered key of its feed when that one came from another node. `self_repeats` counts a key a node sends again on one connection, over its last 8,192 keys.
- `BookView` treats only `price_change` entries as deltas. An entry is dropped when its frame timestamp is below the latest delivered `book` of its asset; an older book never moves that back.
- `venue_sim --rate` is Binance and CLOB frames a second, half each. RTDS is fixed at 4 updates a second, three on `crypto_prices` and one on `crypto_prices_chainlink`, so the 60-point greeting covers 15 s of the fast series. That is more than the merger's 10 s span, which is what makes Late verdicts appear when a node reconnects. Content is deterministic from the seed and venue timestamps are the wall clock in milliseconds. There are two CLOB assets and nodes follow the first; each gets a `book` every 25th frame, and price changes carry no `best_bid` or `best_ask`, so the node's book never crosses or prunes. RTDS answers `PING` with `PONG`, so the RTDS idle guard does not fire on a sparse recording. `--start-after N` holds the generator until every venue has had N connections. Greetings are not written to the manifest, since they repeat broadcast frames. In recording mode control records are skipped and `chaos local` gives the nodes the recording's first subscribe token.
- The window maps wall time to venue time per feed: the newest timestamp among that feed's truth keys broadcast by then. A key is in W when `map(T0) < ts <= map(T1)`. For synthetic frames that is `T0 <= ts < T1` to the millisecond; for a played recording, whose timestamps are from the day it was recorded, it keeps every key broadcast before T0 out of W.
- A Late key that is not delivered and whose timestamp is above `map(T1)` is counted as `late_post_window`, so `late_lost` stays a subset of `missing`.
- The first failing check names the reason, in the order `seq_gap`, `resume`, `unkeyed`, `missing`, `duplicate`, `extra`, `faults_not_observed`, `vacuous`. Protocol counters come first because a broken stream also loses events, and `vacuous` last because it only matters for a run that would otherwise pass. `report.json` lists every failure.
- `freeze` is built here, not in P3, as 3.7 lists it. A freeze lasts 2 s.
- Deviation: a third fault, `cut`, is added. `chaos local` puts a loopback TCP relay between the client and each node, and `cut` drops the client's connection there while the node keeps running; its recovery is the client live on that node again, or 10 s. Reason: `stale-resume` answers a same-boot resume at the head, which is only wrong when the node is ahead of the client. A kill always brings a new boot, and the first CI run showed a freeze is no better: the client's pending connect is served the moment the node wakes, before the frames that queued during the freeze reach the ring, so the head still equals the client's last seq and the mutant passed. A cut leaves the node ingesting through the client's backoff, so the resume point is behind the head. It also stands for the drill's SSH forward breaking.
- Faults alternate nodes and go kill, freeze, cut, so over six faults each node gets every kind. Fault i is due 1 s + i x gap after T0, plus a seeded jitter under a quarter gap. The mutant runs use 3 faults (kill a, freeze b, cut a), except `overlap-kill`, which uses 2.
- The 15 s margins are kept, so run length follows from them: about 95 s for the two synthetic baselines (gap 4 s), 60 s for the fixture and 50 to 65 s for each mutant (gap 3 s, seed 3), not 90 s and 45 s.
- Before each fault, besides the peer's `/readyz` holding 200 for 500 ms, the harness waits up to 10 s for the client to be live on the peer. A client that never rehedges still meets the fault after the wait. The run is invalid (A7, exit 2) when the subscriptions are not all live within 30 s, the peer is not ready within 20 s, a node exits without the harness stopping it, or a victim is not ready 20 s after its fault. `overlap-kill` skips the gate, so its double outage is planned and judged by `missing`.
- The `dup-frame` control cannot use the merger's duplicate counter: with two nodes every event is offered twice, so that counter is always above zero. It must pass with `self_repeats > 0` instead, which only a node resending its own frames produces.
- Step 5's "control passes" is read as that `dup-frame` control.
- A5 "each node delivered first copies" covers the nodes the client dials; the single-node client dials one, so that mutant would also fail `vacuous`, after `missing`.
- The `chaos` job builds once with the `mutants` feature; baseline runs set no mutant. The refusal without the feature is tested in the `rust` job for both `node` and `chaos`.
- The seed 1 baseline runs with `--lag-ms 20`, so one node is always behind and the other supplies most first copies.
- `verify-archive` is left to P3 with the archive it checks.
