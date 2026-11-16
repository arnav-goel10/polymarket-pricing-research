# Poly HA build spec: milestones B and C on Azure

Written as a read-only design before the build started; section 9 records what the build chose and changed. Checked when it was written: branch `ha-failover` at e813cf6, `crates/ha` API (`frame_keys`, `Merger::offer`, sync `Lease` trait, `FrameKind` = Control, Noise, Venue), `Source` numbers 1,2,3,4, default Binance URL already subscribes `aggTrade` and `bookTicker`, workspace TLS is native-tls, fixture is 3,999,803 bytes, `az account show` = Azure for Students, Enabled. Azure SKU, policy and price facts are taken from the attack's verified list, not re-run. The third design arrived truncated, so this merges the two complete ones.

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
- `plan(resume)`: same boot and seq inside the ring gives `Resume{next}`. Otherwise `Snapshot{frames, next, complete}` = slot frames plus the retained CLOB frames since the oldest current book, that tail capped at a quarter of the ring. `complete=false` when part of that tail has left the ring or was cut by the cap. `complete` covers the CLOB book only: a snapshot never carries the RTDS and Binance frames between a client's last seq and the head, so a complete snapshot after a stale or foreign-boot resume still skips those frames by design.

### 3.3 Wire v1 (`ha::wire`, JSON text over WebSocket)
- Client: `{"t":"sub","v":1,"feeds":[..],"resume":{"boot":"..","seq":N}|null}`
- Node: `hello{v,node,boot,mode,next,oldest,head,complete}`, `ev{seq,src,rx,snap,frame}`, `snap_end{next}`, `hb{head,epoch,leader}` every 250 ms, `bye{reason: shutdown|lagged|bad_request}`.
- Keys are not on the wire. The client derives them with `ha::key::frame_keys`, the same code as the node.
- No per-connection queue. A connection whose next seq falls below `oldest` gets `bye lagged`, then reconnects into a snapshot.

### 3.4 Node (`bin/node`)
- Per frame, in order: append to the open segment, compute keys, push to the ring.
- Local segments `<data>/seg/<slot>.<boot>.prec.open`, slot = minute. Flushed when a frame arrives 1 s or more after the last flush, renamed `.prec` at slot end, raw. Keep 60 slots (about 3 GB of the 30 GB disk).
- Refuses to start if `POLY_MUTANT` is set in a build without the `mutants` feature.

### 3.5 Client (`ha::client`)
- Sans-IO `Core` plus a thin tokio-tungstenite driver. Subscribes to both nodes at once. Every frame from every node goes through one shared `Merger`.
- A live `seq != expect` counts `seq_gaps` and reconnects with resume at the last contiguous seq. `mode=resume` with `next != seq+1` counts `resume_violations`.
- No `hb` or `ev` for 1 s means the connection is dead. Redial with 100 ms to 2 s jittered backoff.
- The delivered log records Deliver and Late verdicts (private file, never an artifact).
- `BookView` helper: drop a CLOB delta whose venue timestamp is below the latest delivered `book` for that asset.
- Verdicts are per key and a frame can carry several keys, so a frame consumer takes `Event::delivered_frame`, the frame cut to its delivered events. `BookView::admit_event` does that cut first.
- Not guaranteed and reported: cross-node order (`order_inversions`).

### 3.6 Simulator (`bin/venue_sim`)
- Three WS listeners, one generator, deterministic from `--seed`, `--rate` frames/s.
- **On every new RTDS connection it sends a history snapshot (`payload.data[]`) of the last 60 points, and on every new CLOB connection the latest `book` frames.** This is how real venues behave. It reaches the client live only when a node reconnects to a venue while the client is connected to that node, which the `venue_cut` fault (section 9, P2 review) produces; the two synthetic baselines run two venue cuts each and must show Late verdicts.
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
| A7 | two nodes down at once | exit 2, invalid (as built, judged from the harness's own conditions; see section 9) |

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
- A slow client is never queued for. The WebSocket handshake, the hello, each chunk of 256 snapshot frames and each live batch must be sent and flushed within 5 s or the connection is dropped, so a client that stops reading mid-snapshot frees its snapshot at once. A client whose next seq has left the ring gets `bye lagged`. `stream_roundtrip` holds a client in the handshake and one in a 20 MB snapshot and requires both dropped; the `unbounded-snapshot` mutant (the old unbounded sends) must keep both.
- The snapshot's CLOB tail is capped at a quarter of the ring's frame cap and byte cap, newest frames kept, and a cut tail makes the snapshot incomplete. Reason: a quiet asset's book can be most of the ring old, and a snapshot that takes longer to send than the ring lasts ends in `bye lagged` and another full snapshot, forever. With the cap, a client that can carry the live rate finishes the snapshot and its backlog inside the ring. The ring property models the cap and the `UncappedTail` mutant must fail it. `fixture_snapshot` prints the snapshot size on the live sample.
- Segments live in `app::segments`, unit tested with the time passed in. They flush when a frame arrives at least 1 s after the last flush, and roll when the first frame of a later minute arrives, so a silent feed can leave up to a second unflushed until the next frame or shutdown. A frame up to one slot behind the open slot (a small clock step, or a frame stamped before the roll) goes to the open file. A frame further behind closes it and opens its own slot, so a clock that jumped ahead and came back does not pin the writer in the future. Pruning deletes any boot's segment whose slot is 60 or more behind the slot being opened, but that slot is capped at the first slot this boot opened plus the monotonic time since, so a forward jump of the wall clock cannot delete the retained files. A slot's second file from one boot is `<slot>.<boot>.r<part>.prec`: no file is ever reopened or truncated, and name order is write order within a boot.
- A failed segment append or flush abandons the open file as it is (closed to `.prec`, tail possibly torn) and nothing is written for 1 s; the next frame opens a new file. One failure costs the failing frame, what was still buffered (at most 64 KiB) and the frames of that second, never the rest of the slot. Failures count in `poly_node_segment_errors_total`, frames not written in `poly_node_segment_lost_total`. The frame still goes to the ring, so a disk error does not stop the stream. A unit test fails one write mid-record and requires every frame reported written to be readable; the `KeepWriter` mutant (the old behaviour) must lose the rest of the slot.
- `/readyz` is 503 for 5 s after any rise in `poly_recorder_dropped_total` and while the recorder queue holds more than 4,096 frames (about 2 s at the measured rate). A dropped frame never reaches the ring, so it gets no seq and a client cannot see it on the wire, and the union of two nodes hides it while the other node is healthy. Readiness is where it shows.

Known limits of P1, from its review:

- A disk that stalls (no error) stops the stream: the segment write runs on the thread that feeds the ring. `hb` keeps coming from each connection with the head frozen, so the client's 1 s dead rule does not fire. `/readyz` goes 503 after about 2 s of backlog and the queue drops newest after about 30 s.
- A drop can swallow the Control subscribe record, which leaves the old token's books in the resync slots until the next subscribe. Readiness reports the drop; the slots are not repaired.
- Files are cut by arrival order, not by `rx`. `rx` is stamped in the feed task before the frame is queued, so at a boundary a frame stamped in minute N can sit in file N+1. The archiver groups a slot by file name and `verify-archive` checks HEAD and the objects, so neither assumes a file holds exactly its minute. A reader that wants one minute's records must filter by `rx`, and anything that compares content compares by key.
- After `kill -9` the old boot's `.prec.open` is never renamed and may end in a torn record. Readers treat any `.open` file of a dead boot as closed, with a possibly torn tail.
- A wall clock that jumps forward still moves the archiver's notion of closed slots (P3): it can commit the skipped slots as holes, and the slots after the clock returns sit below the committed head until real time passes it.
- `--listen` (default `127.0.0.1:9100`) must be loopback or the node exits 2. `--http` defaults to `127.0.0.1:9101`; the deploy passes the private IP. `/readyz` is 200 when all three feeds are connected. `--ring-frames` and `--ring-bytes` exist for tests.
- The node mutants are chosen at run time by `POLY_MUTANT` (`seq-skip`, `stale-resume`, `dup-frame`) and honoured only with `--features mutants`. Without the feature the node exits 2 and says it refuses to start. `seq-skip` leaves out each ev whose seq is a multiple of 1,000. `stale-resume` answers every same-boot resume at `head + 1`. `dup-frame` pushes every 500th venue frame again under the next seq.
- Until the P2 client exists, `stream_roundtrip` holds a minimal checker (seq contiguity, resume next, repeated keys). The P2 chaos job takes over the named-reason gating from section 3.7.

### P2 (steps 4 and 5)

- `ha::client` adds tokio, tokio-tungstenite and futures-util to `ha`. All three were already in the workspace, so no new crate enters the lock file. The client faults sit behind a `mutants` feature on `ha`, which the `app` feature of the same name turns on.
- A live `ev` with an unexpected `seq` is counted and not offered, the connection ends, and the redial resumes at the last contiguous `seq`. A resume `hello` when the client sent no resume, or sent one for another boot, also counts as a `resume_violation`. After a violation the client follows the node's `next`.
- The 1 s dead clock starts at the dial, so a connect into a frozen node is dead after 1 s too and there is no separate connect timeout. Backoff is 100 ms doubling per failed try, capped at 2 s, scaled by a jitter in [0.5, 1] and never under 100 ms. It starts again at 100 ms when a connection goes live.
- `order_inversions` counts a delivered timed key older than the newest delivered key of its feed when that one came from another node. `self_repeats` counts a key a node sends again on one connection, over its last 8,192 keys.
- `BookView` treats only `price_change` entries as deltas. An entry is dropped when its frame timestamp is below the latest delivered `book` of its asset; an older book never moves that back.
- `venue_sim --rate` is Binance and CLOB frames a second, half each. RTDS is fixed at 4 updates a second, three on `crypto_prices` and one on `crypto_prices_chainlink`, so the 60-point greeting covers 20 s of the fast series and 60 s of the slow one, which is three times slower. Both are more than the merger's 10 s span, so a greeting that reaches the client live carries Late copies. Content is deterministic from the seed and venue timestamps are the wall clock in milliseconds. There are two CLOB assets and nodes follow the first; each gets a `book` every 25th frame, and price changes carry no `best_bid` or `best_ask`, so the node's book never crosses or prunes. RTDS answers `PING` with `PONG`, so the RTDS idle guard does not fire on a sparse recording. `--start-after N` holds the generator until every venue has had N connections. Greetings are not written to the manifest, since they repeat broadcast frames. In recording mode control records are skipped and `chaos local` gives the nodes the recording's first subscribe token.
- The window maps wall time to venue time per feed: the newest timestamp among that feed's truth keys broadcast by then. A key is in W when `map(T0) < ts <= map(T1)`. For synthetic frames that runs from just after the last millisecond broadcast by T0 to the last millisecond broadcast by T1, inclusive, so keys sharing T0's last millisecond are outside W (the spec's `T0 <= ts < T1` is off by that one millisecond at each end); for a played recording, whose timestamps are from the day it was recorded, it keeps every key broadcast before T0 out of W.
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

P2 review fixes:

- Venue cut. `chaos local` puts a loopback relay between each node and the RTDS and CLOB venues, and `--venue-cuts N` adds N `venue_cut` faults after the others, a gap apart, alternating nodes. A venue cut drops both of the victim's venue relays while the node and the client's connection to it stay up. The harness first waits up to 10 s for the client to be live on the victim. The node redials and is greeted with 60 points per RTDS series and the latest books, and those flow live to the client. Recovery is both redials seen at the relays and the node's `/readyz` at 200 again. A4 counts a venue cut as observed when the client logged a Late copy from the victim between the cut and 5 s after recovery. The seed 1 and seed 2 baselines add two venue cuts each and expect `pass+late`: pass, merger `late > 0` and `late_lost = 0`. A new client mutant, `late-deliver` (a late verdict passed on as a delivery), must fail `duplicate` with one venue cut, since the greeting reaching the client live is certain to carry late copies of keys it already delivered.
- Frame unit. `Event::delivered_frame` returns the frame cut to its delivered events in the venue's own shape (RTDS history points, CLOB array items, price change entries), `None` when nothing was delivered. Each kept piece is keyed again alone and must match the key at its position, so a cut never passes on a duplicate or late piece; a frame whose pieces do not line up gives `None`. `BookView::admit_event` takes the client's event and applies only that cut. The chaos log writes the delivered lines of a mixed-verdict frame from the keys of its cut, so A1 and A2 judge the cut on real frames, and `mixed_frames` counts such frames. A unit test feeds a history frame with Late, Deliver and Duplicate points and a CLOB array with a duplicate entry, and shows that whole-frame application repeats events while the cut gives each once.
- A second seq gap at the same `(boot, seq)` makes the next dial ask for a snapshot instead of a resume, so a node that always skips one frame no longer holds the connection in a resume loop. `seq_gaps` still counts every gap.
- `resyncs` counts snapshots from a boot the client already followed (after `bye lagged`, or the snapshot above). That connection skips the node's frames in between; the other node covers them. It is reported in `report.json`, not gated.
- `untimed_duplicates` reports untimed keys delivered more than once. A1 and A2 still cover timed keys only, as the merger allows an untimed key twice once it leaves the window. It is not gated.
- RTDS point timestamps in the generator are strictly increasing per series, so two ticks in one wall millisecond after a stall no longer share a key.

Known limits of P2, from its review:

- A7 is judged from the harness's conditions (a node exited on its own, a peer not ready before a fault, a victim not ready after one, subscriptions never all live), not from overlapping down intervals in the client's own log. A run where both subscriptions were down together fails `missing` rather than exiting 2. That never turns a bad run into a pass, and it keeps `no-reconnect` and `overlap-kill` judged by `missing` as the mutant table says.
- Truth is a set of key texts made by the same `frame_keys` the client uses. Two distinct venue events that collapse to one key are one truth entry, and the merger dropping the second is invisible to A1. `keycheck` on the committed recording is the only guard, and only for that recording.
- The synthetic generator emits no untimed or unkeyed frames and no CLOB arrays. Only the fixture case carries those shapes, and the cut path for CLOB arrays is covered by unit tests.
- Fixture playback at 300 frames/s runs venue time about 6.6 times slower than wall time, so the merger's 10 s span is about 66 s of wall time and a Late verdict is close to impossible there. The fixture baseline says nothing about the Late boundary on real frames; the venue cut baselines carry that check on synthetic frames.

### P3 (steps 6 and 7)

- `ha::store` speaks bytes: `HeadStore { read_head, write_head(body, cond) }` and `ObjectStore { put_new(name, file), list(prefix) }`. A HEAD write ends `Done(etag)`, `PreconditionFailed`, `Unknown` (timed out after send, may have landed) or `Unreachable` (refused before the store, nothing written). Bodies may not hold a newline, so the journal stays one write per line.
- `MemStore` faults: `Slow(d)`, `Unreachable`, `AppliedReplyLost`, `RequestLost`, and `IgnoreConditions`, a broken store that exists only so the contract suite is shown to fail. `FsStore` keeps HEAD in `lease/HEAD` (the ETag on the first line, the body on the second), takes `lease/.lock` with `File::lock` around each write, appends each applied body to `lease/journal.jsonl`, and creates objects by hard link so one appears whole and only once. `contract_suite(store, scratch, prefix)` runs against both here; the prefix is for the Azure `selftest/` run in P4.
- `caslease::Timing` carries ttl, renew, poll, the holder margin (2 s) and the takeover margin (1 s); the defaults are the spec's. A contender starts its watch when the reply that first showed an ETag arrives, which is never before that ETag was written. The holder's deadline counts from the first send of the write that landed, so a retry after an unknown outcome never moves it later. A retry waits a fifth of a poll, so a store that keeps timing out cannot spin the driver. An unreadable HEAD is never overwritten. A node asked to stop whose takeover lands afterwards releases it.
- A commit sets the current tenure's `to = n`, or appends `{e, n: me, from: previous to + 1}`; with no earlier tenure `from = n`. Trimming drops tenures more than `keep_slots` below the slot being committed (three days of slots) but never the current epoch's own tenure, so the list only loses entries at its front.
- The second, weaker model is `caslease::audit`, which reads only the history of applied HEAD writes: epochs never go back (so once a takeover to E' has landed, no write under an epoch below E' is accepted again, which is "at most one epoch commits past a takeover"), each epoch has one holder, and the tenure list only grows at its end apart from trimming. `chaos verify-archive` runs the same function on the `FsStore` journal.
- Mutant readings. "Commit without If-Match" sends every renew and commit unconditionally, since a commit is a renew; the archive mutant `no-fence` is the same fault. "Unknown outcome treated as failure and committed again" resends the write matched on whatever ETag HEAD holds when it is read again, which is the lost update a fresh read invites. A sixth lease mutant, `SlowTakeover` (wait three ttl), is added so the liveness check is shown to fail too.
- The property runs the real state machine for three nodes against a simulated store in 50 ms steps: clock rates up to 1% off, latency, replies up to 3 s past a 5 s or 10 s timeout, lost requests and replies, refusals, partitions (requests time out), pauses, crashes with and without a release, and a leader committing a slot every 2 s. Safety (the audit, and no committed slot without its object) is checked on every schedule. Belief (at most one node believes it leads) is checked only on schedules with no pause, as the spec states it. Liveness is checked on a calm tail: faults stop and the holder freezes for good, then a different node must hold HEAD within ttl + 1 s + 2 polls + 3 s of the later of the last HEAD write and the contenders being ready (the first build used a 30 s settle; see the P3 review fixes).
- Each mutant must fail the property on the same 256 seeded random schedules or on 40 pinned ones. The pinned family exists for the deadline-from-receipt mutant. With 5 s polls the earliest takeover is 35 s after the holder's last write and the holder stops at 28 s after sending it, so that mutant shows only when a successful reply comes back more than 7 s late. The pinned schedules use a 30 s request timeout, half the replies slow, no pauses, and four windows that cut two of the three nodes off for 45 to 50 s. The CI log prints which set caught it.
- Archiver: a slot is closed once it is two slots old and below the slot the segment writer has open (the first build only excluded the open slot; see the P3 review fixes). The first pass runs at once, then every 5 s. A slot's files from every boot are concatenated in name order; a torn tail ends that file. A slot with no local file is committed without an object and counted as a hole, except that with no committed head the archiver waits for local data. A head more than 60 slots behind jumps to the oldest kept slot and the skipped slots are holes. Every lease event goes to `<data>/lease.jsonl`. On SIGTERM the node releases within 2 s.
- The node publishes the time until which it believes it leads, not a flag, and `hb`, `/ha` and `/metrics` judge it when they are read. The first CI run showed why: a leader woken from a freeze still said `leader: true` for the few milliseconds before its archiver thread ran, and the harness froze the same zombie twice.
- Node flags: `--store DIR` turns the lease on (without it `hb` stays at epoch 0, not leader), `--slot-ms` shortens slots, `--lease-timing ttl,renew,poll,hold,take`. Metrics add `poly_lease_epoch`, `poly_lease_leader`, `poly_fence_rejected_total`, `poly_lease_unknown_outcomes_total` and `poly_archive_{commits,uploads,upload_errors,holes}_total`. `POLY_MUTANT` adds `no-fence` and `epoch-reuse`.
- `chaos local --archive 1` gives both nodes one `FsStore` in the run directory, 2 s slots and a scaled lease (ttl 4 s, renew 1 s, poll 0.5 s, margins 1 s and 0.5 s), so takeovers fit a short CI run. `--leader-freezes N` turns the last N faults into `freeze_leader`: the victim is whichever node `/ha` says leads, it is frozen for 9 s, and the harness polls the peer's `/ha` every 100 ms for a higher epoch with `leader: true` to time the takeover.
- Archive verdict, first failure named, in order: `stale_epoch_commit`, `epoch_two_holders`, `tenure`, `no_takeover` (a leader freeze with no takeover seen), `fence_not_observed` (fenced lines in the nodes' logs below the number of leader freezes), `vacuous_archive` (nothing archived, or a freeze run with under two grants). Holes and orphans are reported. The 45 s takeover gate belongs to the real timing on Azure; CI only requires the takeover to happen and reports its time.
- Deviation: gzip comes from `flate2`, which adds five crates to the lock file (`flate2`, `miniz_oxide`, `adler2`, `simd-adler32`, `zlib-rs`), all under licences `deny.toml` already allows. Re-resolving also moved two crates' Windows-only `windows-sys` to a version already in the lock.

P3 review fixes:

- Archive readiness. A slot is closed once it is two slots old and below the slot the segment writer has open; with no slot open (after boot, or the pause after a disk error) nothing is closed. The old rule (two slots old and not the open slot) committed a slot as a hole while its frames were still queued behind a slow writer, or right after boot, and that data could never be added. A unit test runs a pass with no slot open and with the writer three slots behind and requires no commit and no hole.
- Partial slots. A shipped slot whose local files may miss frames is counted in `poly_archive_partial_total` and logged as `partial` in `lease.jsonl`: a dead boot's `.open` file in it (writer killed), a part file (writer abandoned a file), or no boot with a file in it that also has a file in an earlier slot (no writer was running when it began). Each hole is logged as `hole`. `verify-archive` reports `partial_slots`. Neither is gated.
- Overlap object. Before a new epoch's first commit the leader uploads its own files of the previous tenure's last slot `c` as `seg/e<its epoch>/s<c>`. Each node files a frame by its own receive time, so near the `c`/`c+1` boundary the old leader can file an event in `c+1` (never committed) while the new leader filed it in `c` (committed under the old epoch from the old leader's copy). The overlap holds the new leader's copy. Reader rule addition: an object of epoch E at the slot just before E's tenure is an overlap, merged with that slot by key, and not an orphan. `ha::caslease::object_role` gives each object's role. The reverse case (an event in both committed objects) is a repeat that readers drop by key. Metric `poly_archive_overlaps_total`.
- Content check. `verify-archive` reads every archived and overlap object and both nodes' local segment files, and fails `archive_content` when a key that every node filed in a slot strictly inside the committed range is in no archived or overlap object. The two end slots are left out, as are keys only one node holds. A unit test builds the boundary case and fails without the overlap object and with the key only in an orphan.
- Journal. The directory store mends its journal under the lock before each HEAD write: a torn last line is cut and, if the last line is not the current HEAD (a writer killed between its rename and its append), the current HEAD is appended first. `verify-archive` fails `journal` when the last journal line is not the final HEAD. Order of archive reasons: `stale_epoch_commit`, `epoch_two_holders`, `tenure`, `journal`, `archive_content`, `no_takeover`, `fence_not_observed`, `vacuous_archive`.
- False fence. A 412 on a resent write (one whose earlier attempt had an unknown outcome) reads HEAD before it counts as a fence: if HEAD is the write's own document, the first attempt landed after the resend went out and the write is treated as landed. More generally a write with an unknown outcome stays unsettled until a later write of the node lands, and while any is unsettled every 412 reads HEAD first: a HEAD equal to an unsettled write is that write landing late, and a poll that finds one adopts it too. The first CI run of this fix found the case it misses otherwise: the resend is refused at once with nothing applied, a fresh renew goes out on the old ETag, the first attempt lands, and the fresh renew's 412 was counted as a fence. Two unit tests replay that case for a renew and for a takeover. The schedule generator now lands some requests after their sender timed out (up to 3 s, no reply), a timed-out write keeps flying, and the property fails any fence counted while HEAD is the counting node's own write under the epoch it held. The `TrustRetry412` mutant (the old behaviour) must fail it, and a unit test replays the case by hand.
- Landed write. An unknown outcome is resolved as landed only when HEAD equals the whole document sent (nonce, epoch, holder, tenures), not the nonce alone, so a nonce collision with another node's write cannot be read as mine. A unit test writes another node's takeover carrying the same nonce.
- Audit. A grant after a release must raise the epoch even when the same node takes it again; epoch 1 holder 1, then holder null, then holder 1 again under epoch 1 now fails `epoch_two_holders`.
- Liveness bound. The 30 s settle is gone. Contenders count as ready once each is up, holds nothing, is not stopped and has no request out from before the calm. From the later of that and the last applied HEAD write, a different node must hold HEAD within ttl + 1 s + 2 polls + 3 s (the 3 s covers round trips and clock rate). A `TwoTtlTakeover` mutant joins `SlowTakeover` and must fail it.
- Missing HEAD. A node that has seen a HEAD never creates one again on a 404: it counts `missing_heads` and keeps polling, so a deleted or misread HEAD cannot send the epoch back to 1. A unit test covers both a node that saw HEAD and one that never did.

Known limits of P3, from its review:

- A new leader ships only its own files. When it was down or behind for a slot that the fenced leader holds in full, the slot is committed as a hole or a partial slot; the archive does not fetch from the peer. Holes and partial slots are counted, not gated.
- The overlap covers a boundary shift of one slot. A node that files an event two or more slots later than the other (a freeze or a stall longer than a slot just before a takeover) can still leave it out of the archive; the content check reports it.
- Nonces carry 64 bits of entropy, not 128: both halves come from one 64-bit seed per process. The whole-document match makes a collision harmless to resolution, since another node's write has another holder.
- A SIGSTOP that lands while the leader holds `lease/.lock` blocks the peer's HEAD write until the leader wakes, which would show as `no_takeover` in CI. The window is a few milliseconds per write; it is accepted as a rare flake, not a safety fault, and does not exist on Blob.
- Leadership belief runs on `Instant`, which advances through a SIGSTOP on Linux. A suspended VM may not advance `CLOCK_MONOTONIC`, so a woken Azure zombie can report `leader: true` until its first write is refused; the conditional write still fences it. The P5 method page must say belief is advisory and time each takeover from the peer only.
- A node that has never seen a HEAD still creates one at epoch 1 on a 404. P4's `AzureStore` must not turn any error into a 404.
- Overlap, partial and hole counters can count a slot again when its commit has an unknown outcome and the next pass ships it again.
