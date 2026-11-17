# The live drill: method

How the two-node setup on Azure is tested under real faults, what each reported number means, and what a pass does and does not show. The design is section 4.7 and section 5 of [HA_BUILD_SPEC_B_C_AZURE.md](HA_BUILD_SPEC_B_C_AZURE.md); the choices made while building it are in that file's section 9, under P5. How to deploy and operate the VMs is in [AZURE_RUNBOOK.md](AZURE_RUNBOOK.md).

**State on 18 November 2026: one rehearsal run, verdict fail.** `ha-drill-rehearsal` (Actions run 44135979040) opened, ran all four faults, collected, closed and restored cleanly, and the archive passed with a 27 s takeover. The stream failed on `unkeyed` (396) and `missing` (11,037, all `late.lost`). Section 9 says what was found and what changed.

## 1. What runs

Two VMs in different zones and CPU architectures each run the node: node-a (x64, zone 2) and node-b (Arm64, zone 1). Each reads the three venues on its own, keeps a ring of recent frames, serves them on a loopback WebSocket, records every frame to local minute files, and competes for the archive lease.

A drill is a GitHub Actions job started by a tag. On the runner it:

1. Records each VM's power state, starts any that is not running, and waits until both nodes run and are ready with exactly one leader.
2. Makes an ed25519 key for this run only. `drill-open.sh` first installs a `Match User drill` block for sshd (`deploy/azure/drill/sshd-drill.conf`) that allows that user local TCP forwards to the three loopback ports and nothing else: no Unix socket, agent or X11 forward, no listener, no terminal. It checks the config with `sshd -t` before any reload, takes the file out again if the check fails, and reads back what sshd would apply to the user. Then it puts the key on both VMs for the user `drill`, limited to forwarding to the node's stream (9100), health endpoints (9101) and the drill agent (9102) on loopback, with no shell, no remote forward and an expiry four hours out. It starts the drill agent and returns the VM's host key, which the runner pins.
3. Adds a security group rule `drill-<run>` that lets TCP 22 in from the runner's address only.
4. Opens SSH forwards to both nodes. A **main client** subscribes to both nodes at once through them, and a **control client** subscribes to node-a only.
5. Runs the fault plan. Before each fault the other node's `/readyz` must have answered 200 for 500 ms and the main client must be live on it. Faults go to the agent on the victim's VM: `kill9` is SIGKILL on the node's main process, and systemd restarts it a second later; `freeze_leader` is SIGSTOP on whichever node says it leads, for 60 s, longer than the worst takeover, so the frozen leader wakes as a zombie. The agent sends SIGCONT itself, so a lost runner never leaves a node stopped.
6. `drill-collect.sh` uploads, from each VM with its own identity, the agent's fault log, the node's `lease.jsonl`, its counters and its local minute files for the drill window, gzipped, to `drill/<run>/n<id>/` in the archive container. The runner downloads them, HEAD, and the list of archive objects.
7. `chaos verify-drill` judges the run and writes `report.json`.
8. Whatever happened before, also on a failure or a cancel: the job signs in to Azure again, the forwards are closed, every `drill-*` rule is deleted (the delete and the check are tried up to five times, five seconds apart), `drill-close.sh` empties the drill user's keys, stops the agent, wakes the node and ends every drill session, the runner's copy of the data is deleted, and the job fails unless no `drill-*` rule is left and both nodes run and are ready with exactly one leader.
9. Last, also on a failure or a cancel, each VM goes back to the state step 1 found it in. A VM that was deallocated is deallocated again. A VM that was running, or stopped but still allocated, or whose state could not be read, is left running, the last with a warning. A drill on deallocated VMs therefore costs only the hours it runs.

The job signs in once more before collecting, as well as before the cleanup. The GitHub token behind a sign-in lasts minutes, and Azure's CLI asks for a token per service when it first needs one, so a storage token first wanted an hour after the sign-in would be refused. The fault step has a time limit of 15 minutes for a rehearsal and 45 for a full drill, and collect one of 20, so the cleanup always starts as an ordinary step with time left in the job.

The agent on each VM stops promptly on SIGTERM. The signal only sets a flag; the agent finishes the command in hand, leaves its loop and sends SIGCONT to a node it froze on the way out, so stopping it can never wait on its own lock and never leaves a node stopped.

| Tag | Faults | Of which | Runner time |
|---|---|---|---|
| `ha-drill-rehearsal`, `ha-drill-rehearsal-<N>` | 4 | 3 `kill9` (node-a, node-b, node-a), 1 `freeze_leader` | about 20 minutes |
| any other `ha-drill-*`, for example `ha-drill-1` | 20 | 14 `kill9` alternating from node-a, 6 `freeze_leader` spread through the run | about 50 minutes |

Faults are 90 s apart plus a seeded jitter under a quarter gap; the seed is the workflow's run id. The window starts 15 s after both main subscriptions are live and ends 15 s after the last recovery, and the client runs 15 s past that.

## 2. Truth

Truth is **the union of both nodes' own recordings** over the window. There is no simulator on Azure, so nothing outside the system knows every event the venues sent. An event is "in truth" if either node wrote it to disk. Keys are derived with the same `frame_keys` the client uses.

The window is cut by each key's venue timestamp, mapped from the runner's wall time per feed: a key is in it when its timestamp is above the newest one recorded by T0 and at or below the newest recorded by T1.

## 3. The numbers

Every time is on the runner's monotonic clock unless said otherwise. A fault's instant, for the client, is the last `ev` or `hb` it received from the victim.

| Number | Definition | Gate |
|---|---|---|
| `faults.executed`, `kill9`, `freeze_leader` | faults run, by kind | executed equals planned, or the run is invalid |
| `faults.observed` | faults where the main client saw the victim go down between the fault and 5 s after its recovery | equals executed (`faults_not_observed`) |
| `truth_keys_in_window` | distinct timed keys in truth inside the window | above 0 (`vacuous`) |
| `missing` | truth keys in the window the main client never delivered | 0 |
| `duplicates` | timed keys the main client delivered more than once, whole run | 0 |
| `extra` | delivered keys in neither recording | reported only |
| `late.lost` | keys the merger judged late, never delivered, inside the window (a subset of `missing`) | 0, through `missing` |
| `control_missing` | truth keys in the same window the node-a-only client never delivered | above 0, or the run is invalid |
| `content.mismatch` per feed | keys that each node recorded alone in a frame, where the two copies' venue content differs: the content fingerprint `keycheck` uses, which for an RTDS price leaves out the envelope's connection id and send time | Binance 0; RTDS and CLOB reported |
| `added_delay_ms` p50, p99, max | for deliveries within 5 s after a fault: delivery time minus the delivering node's receive stamp, minus that node's own median of the same lag outside those 5 s windows | reported |
| `rehedge_ms` p50, max | from the victim's last message to the main client live on both nodes again | reported |
| `archive.takeover_ms` p50, max | from the agent's answer to the SIGSTOP to the first `/ha` answer from the other node with a higher epoch and `leader: true`, polled every 100 ms through the forward | each leader freeze has one, max at most 45,000 ms |
| `archive.fence_rejected` | `fenced` lines in both nodes' `lease.jsonl` during the drill | at least the number of leader freezes |
| `archive.stale_epoch_commits` | `commit` lines during the drill whose slot the final HEAD does not hold under that epoch, or written more than 2 s after a higher epoch was granted | 0 |
| `archive.epoch_two_holders` | epochs granted to two nodes, in either log or in HEAD's tenures | 0 |
| `archive.tenure_violations` | HEAD tenures out of epoch order or not contiguous | 0 |
| `archive.view` | the reader rule over the drill's slots: covered, archived, holes, overlaps, orphans | archived above 0; holes and orphans reported |
| `binance_agg_id_gaps` | breaks in the Binance aggregate trade id among delivered trades in the window, the one truth source outside the system | reported |
| `seq_gaps`, `resume_violations`, `unkeyed` | client protocol counters | 0 |
| `truth.unkeyed` | events without a key in each node's recording, by feed and venue event type, as [node-a, node-b] | reported; says what `unkeyed` counted |
| `missing_detail` | the missing keys by feed and by which node recorded them (node-a only, node-b only, both); by the node whose late copy the merger dropped, or never offered; their receive lag (first receive on any node minus venue time) and how many were received more than the 10 s merge span late; and per fault, how many were first recorded during it or within 30 s after its recovery | reported; the delivered log never leaves the runner, so this is how a failed `missing` is read |
| `resets`, `reconnects`, `incomplete_snapshots`, `order_inversions`, `resyncs` | client counters | reported |

The archive's slots are those between the drill's start and two slots before its end (a slot is committed once it is two slots old), cut at the last committed slot. Slots outside that range are not judged.

## 4. What the result means

- **pass**: every gate above held. Across the run's kills and leader freezes, a client subscribed to both nodes delivered every event either node recorded in the window exactly once, the control client shows the faults would have cost a single-node client events, and the archive lease moved within 45 s of each leader freeze with no commit under a stale epoch.
- **fail**: the run counts and a gate failed. `report.json` names the first failure and lists all of them, in the order `seq_gap`, `resume`, `unkeyed`, `missing`, `duplicate`, `faults_not_observed`, `vacuous`, `content_mismatch`, then the archive's `head_unreadable`, `stale_epoch_commit`, `epoch_two_holders`, `tenure`, `no_takeover`, `takeover_slow`, `fence_not_observed`, `vacuous_archive`.
- **invalid**: the run cannot count. A node was not ready before or within 120 s after a fault, the subscriptions never all went live, a fault did not run (an agent declined a leader freeze three times, or did not answer), or the control client missed nothing although node-a was killed.

The job's exit follows the verdict, and the cleanup step's own check is separate: a passing drill that leaves a rule, a key or an unhealthy pair behind still fails the job.

## 5. Known limits

- **Union truth cannot see an event both nodes missed.** If both nodes' venue connections drop at once, the events in between are in neither recording, and `missing` is 0 for them. `binance_agg_id_gaps` is the only check against the venue itself, and only for Binance trades.
- **There is no takeover on the data path.** Both nodes serve all the time and the client merges them, so `added_delay_ms` is extra delay seen around faults, not a failover time. The only takeover is the archive lease's.
- **Cross-node order is not guaranteed.** `order_inversions` counts it and is not gated.
- **Leadership belief is advisory.** A node believes it leads until a deadline on its own monotonic clock. A VM that is suspended may not advance that clock, so a woken zombie can say `leader: true` until its first write is refused; the conditional write on HEAD still fences it. So the drill never times a takeover from the frozen node: it reads only the other node's `/ha`, and when it picks a leader to freeze it takes the node with the newest epoch. A freeze is SIGSTOP on the process, not a suspended VM, so on these runs `Instant` keeps advancing through it.
- **`extra` is not gated.** A killed node can forward a frame to the client and lose its last second of buffered file, so a delivered key in neither recording does not prove a fault.
- **Content is gated on Binance only.** Binance sends each event as its own message, so two connections should get the same text, and the rehearsal found 0 differences in 430,092. RTDS frames carry a per-connection id and send time in the envelope, so the rehearsal's raw comparison called all 431 compared RTDS keys different; the comparison now uses the venue content, and the next run shows whether RTDS can be gated. CLOB had 0 differences in 5,127.
- **The takeover time includes the forward.** `/ha` is polled every 100 ms through an SSH forward to a runner in another region, so each reading lags by up to one round trip plus 100 ms. Against a 45 s gate this does not matter; the number is not a precise measurement below a second.
- **Both subscriptions share the runner's network.** A stall on the runner's path to Azure hits both at once. Each node keeps about 80 s of frames and the client resumes from its last sequence, so a short stall costs nothing, but one longer than the merger's 10 s span can show as `late_lost` and fail the run. That would be a property of this test setup, and the log of the run says which faults it fell on.
- **The archive content check of CI does not run on Azure.** It needs every archived object downloaded. On Azure the archive verdict reads HEAD, the object names and the lease logs.
- **The runner's address comes from a public echo service** (`api.ipify.org`, then `checkip.amazonaws.com`). If both fail, or the runner's outbound address changes during the run, SSH cannot reach the VMs and the run is invalid; nothing is left open, because the rule names only the address that was seen.
- **Data older than two days is gone.** The lifecycle rule deletes `seg/` and `drill/` after two days, so a drill can only be judged again from its collected files within that time.

## 6. What leaves the runner

`missing_detail` and `truth.unkeyed` are counts and venue event type names, and their line in the log carries nothing else.


Only `report.json` (counts, timings and verdicts) becomes an artifact, and the same numbers go to the job summary. The delivered logs, the recordings and the lease logs stay on the runner until the cleanup deletes them, and in the private archive container under `drill/<run>/` for two days. The owner commits a report under `bench-results/ha-drills/` himself.

The workflow log is public. Every address (the runner's and both VMs'), the run's key, the VMs' host keys and the storage account name are masked before anything could print them, no step traces its commands, and output from ssh, the VM agent and az goes to private files on the runner, printed only as counts or with anything shaped like an address replaced. `ops/azure/test_dry_run.sh` runs all the runner's steps against fakes on every push and fails if any of those values reaches the output or the job summary.

## 7. How it was checked before any run

| Check | Where | What it shows |
|---|---|---|
| `drill` module unit tests | `rust` job | the plan's shape for 4 and 20 faults, union truth from plain and gzipped files with a torn tail, the content comparison by venue content with RTDS envelopes that differ, the missing breakdown by feed, recorder, late offer and fault, unkeyed events named by type, every archive check failing by its own name, holes and orphans counted inside the window only |
| `ops/azure/test_drill_local.sh` | `drill-local` job | the real agent, `chaos drill`, collection and `verify-drill` on loopback with two nodes under restart loops and a shared directory store: three kills and a leader freeze, a pass against the union of the recordings and against the simulator's own manifest, `control_missing` above 0 |
| `ops/azure/test_dry_run.sh` | `azure-deploy-code` job | the workflow's steps against fakes of az, ssh and curl: the rule names only the runner's address and port 22, both VMs get the key and are closed, a cleanup after a failed open still removes everything, a rule delete or list that fails for a while is retried, a stuck rule, an unlistable group, an unclosed VM or two leaders fail the job loudly, VMs found deallocated are deallocated again and no others, another subscription is refused, the tools bundle digest is checked, and no address, key or account name reaches the log. Against the runner's real sshd: the drill user's Match block passes `sshd -t` beside the base config, gives exactly the TCP forwards to the three ports, and does not leak to other users |
| `ops/azure/test_drill_agent.py` | `azure-deploy-code` job, from `test_dry_run.sh` | the real agent exits within seconds of SIGTERM when idle, mid-freeze, and while it holds its lock answering a leader freeze, and its stand-in node is never left stopped |
| `shellcheck`, `actionlint` | `azure-deploy-code` job | the scripts and the workflow lint |

What none of this shows: that SSH to the drill user works with these options on the VMs' sshd, that `systemctl kill` and `systemd-run` behave on the VMs as on a runner, that the venues send the same message text to two connections, and how long a node takes to be ready again after a kill against the real venues. The rehearsal is for those.

## 8. Running it

The VMs must carry the drill tools of the commit the drill tag points at; the drill checks their digest and stops before opening anything if they differ. They are installed by the deploy job, so a drill tag needs a deploy tag on the same commit first, and any change under `deploy/azure/drill/` needs a new deploy tag before the next drill tag. From the repository root on `ha-failover`:

```bash
git tag ha-deploy-2 && git push origin ha-deploy-2           # about 30 minutes; wait for green
git tag ha-drill-rehearsal && git push origin ha-drill-rehearsal
```

Then read the rehearsal's public log for anything that should not be there, and its `report.json`. If both are clean, `ha-drill-1` runs the 20-fault drill. Each tag is used once. A second rehearsal is `ha-drill-rehearsal-2`, and the next full drill `ha-drill-2`.

## 9. The first rehearsal

Run 44135979040 on 17 November 2026, 4 faults (kill node-a, kill node-b, kill node-a, freeze the leader, which by the grants was node-b), window 364.5 s.

**`unkeyed` 396.** The client counts each offered event with no key. The CI simulator made only keyed kinds (Binance trades and book tickers, CLOB `book` and `price_change`, RTDS prices), so the gate had never seen a real one. In the 10-minute live recording the only events of any feed without a key are CLOB `new_market` (611, in bursts of up to 289 a minute) and `tick_size_change` (2). The market channel sends them because the subscription sets `custom_feature_enabled`, and `new_market` comes for markets the connection never asked for. They are venue events with venue ids, not keepalives, so both now have keys (see `crates/ha/src/key.rs`), and the simulator now sends one `new_market` in 400 CLOB frames and one `tick_size_change` in 4,000, so CI would catch an unkeyed kind on that path again. The gate is unchanged. The report did not name the kind; `truth.unkeyed` now does, so the next run confirms it.

**`missing` 11,037, all `late.lost`.** Every missing key reached the client and was dropped as late: offered after the merger's horizon (10 s behind the newest venue time of its feed) had passed it, and no node's copy came in time. 31,899 further late keys had been delivered by the other node and cost nothing. Steady state does not explain it: on the live recording no event of any feed arrives more than 10 s behind its feed's newest time, and the merger's 717,161 duplicates out of 915,141 deliveries fit both nodes sending nearly every key outside the fault gaps. So the lost keys reached the client late from a node during or after a fault, with no on-time copy from the other. The rehearsal's report could not say which feed, which node or which fault, and its delivered log was deleted with the runner. `missing_detail` now says. The gate is unchanged: a key either node recorded and the client never delivered is a loss to a subscriber, whatever the cause.

**RTDS content 431 of 431.** A checker artefact, above: the keys matched, the envelopes did not.

