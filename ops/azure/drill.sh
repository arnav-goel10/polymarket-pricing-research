#!/usr/bin/env bash
# The live drill, from the runner's side (spec 4.7, docs/HA_DRILL.md). The
# `drill` job of .github/workflows/azure.yml runs these steps in order,
# `close` and `restore` always, also after a failure or a cancel:
#
#   ops/azure/drill.sh start     record each VM's power state, start the
#                                ones that are not running
#   ops/azure/drill.sh ready     both nodes running and ready, one leader
#   ops/azure/drill.sh open      per-run key on both VMs, an SSH rule for
#                                this runner's address, SSH forwards up
#   ops/azure/drill.sh run       chaos drill through the forwards
#   ops/azure/drill.sh collect   recordings and lease logs to the archive,
#                                down to the runner, chaos verify-drill
#   ops/azure/drill.sh close     forwards down, rule removed, key removed,
#                                both nodes running with one leader
#   ops/azure/drill.sh restore   deallocate again each VM `start` found
#                                deallocated, and no other
#
# Settings, from the environment:
#   DRILL_RUN       run id, plain characters (the workflow's run id and attempt)
#   DRILL_DIR       private working directory (default $RUNNER_TEMP/drill)
#   DRILL_OUT       where report.json is copied for the artifact (default
#                   $RUNNER_TEMP/drill-report, outside DRILL_DIR, which
#                   `close` deletes)
#   CHAOS           the chaos binary (default target/release/chaos)
#   DRILL_FAULTS, DRILL_LEADER_FREEZES, DRILL_SEED, DRILL_GAP_MS,
#   DRILL_FREEZE_MS chaos drill's plan
#   DRILL_POWER     where `start` records the VMs' power states for
#                   `restore` (default $RUNNER_TEMP/drill-power)
#   DRILL_HEALTH_TRIES, DRILL_SLEEP   how long `close` waits for health
#   DRILL_RULE_TRIES                  how often `close` tries the rule delete
#
# The workflow's log is public. Every address (the runner's, the VMs'),
# the run's key, the VMs' host keys and the storage account name are
# masked with ::add-mask:: before anything could print them, nothing is
# run with tracing, and the output of commands that could carry them (ssh,
# the VM agent, az) goes to files in DRILL_DIR, which are printed only as
# counts or through `redact`. ops/azure/test_dry_run.sh runs all four
# steps against fakes and fails if any of those values reaches the output.
#
# Like the other scripts it refuses any subscription but the project's.
# There is no --yes: it is the workflow's, and the tag that starts it is
# the owner's decision.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=ops/azure/lib.sh
. "$here/lib.sh"

step=${1:-}
run=${DRILL_RUN:?DRILL_RUN is required}
[[ $run =~ ^[A-Za-z0-9_-]{1,64}$ ]] || die "DRILL_RUN must be plain characters"
dir=${DRILL_DIR:-${RUNNER_TEMP:-/tmp}/drill}
out=${DRILL_OUT:-${RUNNER_TEMP:-/tmp}/drill-report}
power=${DRILL_POWER:-${RUNNER_TEMP:-/tmp}/drill-power}
chaos=${CHAOS:-target/release/chaos}
nsg=poly-ha-nsg
rule="drill-$run"
# Local ends of the forwards: stream, health, agent, per node.
ports_a=(19100 19101 19102)
ports_b=(29100 29101 29102)
sleep_s=${DRILL_SLEEP:-5}

mask() {
  if [ -n "${1:-}" ] && [ "${GITHUB_ACTIONS:-}" = true ]; then
    printf '::add-mask::%s\n' "$1"
  fi
}

# Anything shaped like an IPv4 address becomes <addr>.
redact() {
  sed -E 's/[0-9]{1,3}(\.[0-9]{1,3}){3}/<addr>/g'
}

is_ipv4() {
  [[ $1 =~ ^([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})$ ]]
}

# Masks for values an earlier step found, so a later step's own log is
# covered even if the runner restarted the shell.
remask() {
  local f
  for f in "$dir"/mask.*; do
    [ -f "$f" ] && mask "$(cat "$f")"
  done
  return 0
}

keep_mask() {
  mask "$2"
  printf '%s' "$2" >"$dir/mask.$1"
}

node_addr() {
  az network public-ip show -g "$RG" -n "$1-ip" --query ipAddress -o tsv 2>>"$dir/az.err"
}

forward() {
  local n=$1 addr=$2
  shift 2
  local p=("$@")
  (
    while [ ! -e "$dir/stop" ]; do
      ssh -N -o BatchMode=yes -o IdentitiesOnly=yes -o IdentityFile="$dir/key" \
        -o UserKnownHostsFile="$dir/known_hosts" -o GlobalKnownHostsFile=/dev/null \
        -o StrictHostKeyChecking=yes -o HostKeyAlgorithms=ssh-ed25519 \
        -o ExitOnForwardFailure=yes -o ServerAliveInterval=5 -o ServerAliveCountMax=3 \
        -o ConnectTimeout=10 -o LogLevel=ERROR \
        -L "127.0.0.1:${p[0]}:127.0.0.1:9100" \
        -L "127.0.0.1:${p[1]}:127.0.0.1:9101" \
        -L "127.0.0.1:${p[2]}:127.0.0.1:9102" \
        "drill@$addr" </dev/null >>"$dir/ssh-$n.log" 2>&1 || true
      [ -e "$dir/stop" ] || sleep 1
    done
  ) </dev/null >/dev/null 2>&1 &
  # It outlives this step: the forwards serve `run` and `collect`, and
  # `close` ends them.
  echo $! >"$dir/forward-$n.pid"
}

http_code() {
  curl -s -m 3 -o /dev/null -w '%{http_code}' "http://127.0.0.1:$1$2" 2>/dev/null || true
}

step_open() {
  require_subscription
  rm -rf "$dir"
  mkdir -p "$dir" "$out"
  chmod 0700 "$dir"

  ssh-keygen -q -t ed25519 -N '' -C "drill-$run" -f "$dir/key" >/dev/null
  local pub keyarg
  pub=$(cut -d' ' -f2 "$dir/key.pub")
  keyarg=$(printf '%s' "$pub" | tr '+/' '-_' | tr -d '=')
  keep_mask key "$pub"
  keep_mask keyarg "$keyarg"

  local ip
  ip=$(curl -fsS -m 10 https://api.ipify.org 2>/dev/null || curl -fsS -m 10 https://checkip.amazonaws.com 2>/dev/null || true)
  ip=$(printf '%s' "$ip" | tr -d '[:space:]')
  keep_mask runner "$ip"
  is_ipv4 "$ip" || die "could not learn this runner's address"

  # A rule left by a run that never reached its cleanup goes first.
  local stale
  stale=$(az network nsg rule list -g "$RG" --nsg-name "$nsg" \
    --query "[?starts_with(name, 'drill-')].name" -o tsv 2>>"$dir/az.err" || true)
  for r in $stale; do
    say "removing a rule left by an earlier drill"
    az network nsg rule delete -g "$RG" --nsg-name "$nsg" -n "$r" --output none 2>>"$dir/az.err"
  done
  printf '%s\n' "$rule" >"$dir/rule"
  az network nsg rule create -g "$RG" --nsg-name "$nsg" -n "$rule" --priority 110 \
    --direction Inbound --access Allow --protocol Tcp \
    --source-address-prefixes "$ip/32" --source-port-ranges '*' \
    --destination-address-prefixes '*' --destination-port-ranges 22 \
    --output none 2>>"$dir/az.err" || die "the SSH rule for this runner was not created"
  say "SSH rule $rule added for this runner's address"

  local digest
  digest=$("$here/vm_tools.sh" digest)
  : >"$dir/known_hosts"
  local n addr hostkey
  for n in a b; do
    addr=$(node_addr "node-$n" || true)
    keep_mask "addr-$n" "$addr"
    is_ipv4 "$addr" || die "no address for node-$n"
    printf '%s' "$addr" >"$dir/addr-$n"
    if ! "$here/vm_run.sh" "node-$n" drill-open "$run" "$keyarg" "$digest" >"$dir/open-$n.out" 2>&1; then
      grep -E '^(drill|vm_run|error)' "$dir/open-$n.out" | grep -v '^hostkey' | redact >&2 || true
      die "drill-open failed on node-$n"
    fi
    hostkey=$(awk '$1 == "hostkey" && $2 == "ssh-ed25519" {print $3}' "$dir/open-$n.out")
    keep_mask "hostkey-$n" "$hostkey"
    [ -n "$hostkey" ] || die "node-$n gave no host key"
    printf '%s ssh-ed25519 %s\n' "$addr" "$hostkey" >>"$dir/known_hosts"
    grep '^drill-open:' "$dir/open-$n.out" | redact | sed "s/^/node-$n: /"
  done

  forward a "$(cat "$dir/addr-a")" "${ports_a[@]}"
  forward b "$(cat "$dir/addr-b")" "${ports_b[@]}"
  local p ok
  for p in "${ports_a[1]}" "${ports_b[1]}"; do
    ok=0
    for _ in $(seq 1 60); do
      if [ "$(http_code "$p" /healthz)" = 200 ]; then
        ok=1
        break
      fi
      sleep 1
    done
    [ "$ok" = 1 ] || die "the forward to a node's health port did not come up"
  done
  say "forwards up to both nodes"
}

step_run() {
  remask
  [ -f "$dir/known_hosts" ] || die "the drill was not opened"
  local code=0
  "$chaos" drill --run "$dir/run" \
    --nodes "ws://127.0.0.1:${ports_a[0]},ws://127.0.0.1:${ports_b[0]}" \
    --http "127.0.0.1:${ports_a[1]},127.0.0.1:${ports_b[1]}" \
    --agents "127.0.0.1:${ports_a[2]},127.0.0.1:${ports_b[2]}" \
    --seed "${DRILL_SEED:-1}" --faults "${DRILL_FAULTS:-20}" \
    --leader-freezes "${DRILL_LEADER_FREEZES:-6}" \
    --gap-ms "${DRILL_GAP_MS:-90000}" --leader-freeze-ms "${DRILL_FREEZE_MS:-60000}" \
    >"$dir/drill.out" 2>&1 || code=$?
  redact <"$dir/drill.out" | tail -n 40
  return "$code"
}

step_collect() {
  remask
  require_subscription
  local times="$dir/run/times.json"
  [ -f "$times" ] || die "the drill left no times.json"
  local from to n
  from=$(jq -r .start_ms "$times")
  to=$(jq -r .end_ms "$times")
  for n in a b; do
    if ! "$here/vm_run.sh" "node-$n" drill-collect "$run" "$from" "$to" >"$dir/collect-$n.out" 2>&1; then
      grep -E '^(drill|vm_run|error)' "$dir/collect-$n.out" | redact >&2 || true
      die "drill-collect failed on node-$n"
    fi
    grep '^drill-collect:' "$dir/collect-$n.out" | redact
  done

  local account
  account=$(az storage account list -g "$RG" --query "[0].name" -o tsv 2>>"$dir/az.err")
  keep_mask account "$account"
  [ -n "$account" ] || die "no storage account"
  local dl="$dir/dl" col="$dir/dl/drill/$run"
  mkdir -p "$dl"
  az storage blob download-batch --auth-mode login --account-name "$account" \
    --source archive --destination "$dl" --pattern "drill/$run/*" \
    --output none >>"$dir/az.out" 2>>"$dir/az.err" || die "download of the run's files failed"
  [ -d "$col/n1" ] && [ -d "$col/n2" ] || die "the run's files are not both there"
  az storage blob download --auth-mode login --account-name "$account" \
    --container-name lease --name HEAD.json --file "$col/head.json" \
    --output none >>"$dir/az.out" 2>>"$dir/az.err" || say "HEAD could not be read; the archive verdict will say so"
  az storage blob list --auth-mode login --account-name "$account" \
    --container-name archive --prefix seg/ --num-results '*' \
    --query "[].name" -o tsv >"$col/objects.txt" 2>>"$dir/az.err" || die "listing the archive failed"

  local code=0
  "$chaos" verify-drill --run "$dir/run" --collected "$col" >"$dir/verify.out" 2>&1 || code=$?
  grep -E '^chaos' "$dir/verify.out" | redact || true
  if [ -f "$dir/run/report.json" ]; then
    mkdir -p "$out"
    cp "$dir/run/report.json" "$out/report.json"
    summary "$out/report.json"
  else
    redact <"$dir/verify.out" | tail -n 20 >&2
  fi
  return "$code"
}

# The report as a table in the job summary. Counts and timings only.
summary() {
  [ -n "${GITHUB_STEP_SUMMARY:-}" ] || return 0
  jq -r '
    def v(x): if x == null then "-" else (x | tostring) end;
    "### Drill \(.verdict.result)" + (if .verdict.reason then " (\(.verdict.reason))" else "" end),
    "",
    "| Number | Value |", "|---|---|",
    "| faults executed / planned | \(.stream.faults.executed) / \(.stream.faults.planned) |",
    "| kill9, freeze_leader | \(.stream.faults.kill9), \(.stream.faults.freeze_leader) |",
    "| truth keys in window | \(.stream.truth_keys_in_window) |",
    "| missing | \(.stream.missing) |",
    "| duplicates | \(.stream.duplicates) |",
    "| extra (reported) | \(.stream.extra) |",
    "| late_lost | \(.stream.late.lost) |",
    "| control_missing | \(.control_missing) |",
    "| content mismatch rtds, binance, clob | \(.content.mismatch | map(tostring) | join(", ")) |",
    "| added delay p50, p99, max ms | \(v(.stream.added_delay_ms.p50)), \(v(.stream.added_delay_ms.p99)), \(v(.stream.added_delay_ms.max)) |",
    "| rehedge p50, max ms | \(v(.stream.rehedge_ms.p50)), \(v(.stream.rehedge_ms.max)) |",
    "| takeovers / leader freezes | \(.archive.takeovers_seen) / \(.archive.leader_freezes) |",
    "| takeover p50, max ms | \(v(.archive.takeover_ms.p50)), \(v(.archive.takeover_ms.max)) |",
    "| fence rejected | \(.archive.fence_rejected) |",
    "| stale epoch commits, epoch two holders | \(.archive.stale_epoch_commits), \(.archive.epoch_two_holders) |",
    "| archive holes, orphans in window | \(.archive.view.holes), \(.archive.view.orphans) |",
    "| seq gaps, resume violations, unkeyed | \(.stream.counters.seq_gaps), \(.stream.counters.resume_violations), \(.stream.counters.unkeyed) |",
    "| binance agg id gaps | \(.stream.binance_agg_id_gaps) |"
  ' "$1" >>"$GITHUB_STEP_SUMMARY" || true
}

# Exactly one of the two status outputs says it leads, and both nodes run
# and are ready.
healthy() {
  local a=$1 b=$2 leaders
  leaders=$(printf '%s\n%s\n' "$a" "$b" | grep -c '^poly_lease_leader 1$' || true)
  grep -qx 'service=active' <<<"$a" && grep -qx 'service=active' <<<"$b" &&
    grep -qx 'readyz=200' <<<"$a" && grep -qx 'readyz=200' <<<"$b" &&
    [ "$leaders" = 1 ]
}

step_close() {
  remask
  require_subscription
  mkdir -p "$dir"
  local fail=0 n f
  touch "$dir/stop"
  for f in "$dir"/forward-*.pid; do
    [ -f "$f" ] || continue
    pkill -P "$(cat "$f")" >/dev/null 2>&1 || true
    kill "$(cat "$f")" >/dev/null 2>&1 || true
  done
  pkill -f "UserKnownHostsFile=$dir/known_hosts" >/dev/null 2>&1 || true

  remove_drill_rules || fail=1

  for n in a b; do
    if "$here/vm_run.sh" "node-$n" drill-close "$run" >"$dir/close-$n.out" 2>&1; then
      grep '^drill-close:' "$dir/close-$n.out" | redact | sed "s/^/node-$n: /"
    else
      grep -E '^(drill|vm_run|error)' "$dir/close-$n.out" | redact >&2 || true
      echo "::error::drill-close did not finish on node-$n: its drill key may still be there"
      fail=1
    fi
  done

  wait_healthy "after the drill" || fail=1

  rm -rf "$dir"
  return "$fail"
}

drill_rule_names() {
  az network nsg rule list -g "$RG" --nsg-name "$nsg" \
    --query "[?starts_with(name, 'drill-')].name" -o tsv 2>>"$dir/az.err"
}

# Delete every drill-* rule, this run's and any an earlier run left, and
# list again, a few times with a pause, since a delete or a list can fail
# for a moment while the security group is being updated. Fails unless a
# last list answers and names none.
remove_drill_rules() {
  local tries=${DRILL_RULE_TRIES:-5} i names r
  for i in $(seq 1 "$tries"); do
    if names=$(drill_rule_names); then
      if [ -z "$names" ]; then
        say "no drill rule left in the security group"
        return 0
      fi
      for r in $names; do
        az network nsg rule delete -g "$RG" --nsg-name "$nsg" -n "$r" --output none 2>>"$dir/az.err" || true
      done
    fi
    [ "$i" = "$tries" ] || sleep "$sleep_s"
  done
  if ! names=$(drill_rule_names); then
    echo "::error::the security group's rules could not be listed after $tries tries: a drill SSH rule may still be there"
    return 1
  fi
  if [ -z "$names" ]; then
    say "no drill rule left in the security group"
    return 0
  fi
  echo "::error::a drill SSH rule is still in the security group after $tries tries ($(wc -w <<<"$names" | tr -d ' ') left)"
  return 1
}

# Wait until both nodes run and are ready with exactly one leader, through
# the VM agent. Prints the state of each, without addresses.
wait_healthy() {
  local when=$1 tries=${DRILL_HEALTH_TRIES:-20} a='' b='' ok=0 n f
  for _ in $(seq 1 "$tries"); do
    a=$("$here/vm_run.sh" node-a status 2>/dev/null || true)
    b=$("$here/vm_run.sh" node-b status 2>/dev/null || true)
    if healthy "$a" "$b"; then
      ok=1
      break
    fi
    sleep "$sleep_s"
  done
  for n in a b; do
    [ "$n" = a ] && f=$a || f=$b
    printf 'node-%s %s: %s %s %s\n' "$n" "$when" \
      "$(grep '^service=' <<<"$f" || echo service=?)" \
      "$(grep '^readyz=' <<<"$f" || echo readyz=?)" \
      "$(grep '^poly_lease_leader ' <<<"$f" | tr ' ' '=')"
  done
  if [ "$ok" = 1 ]; then
    say "both nodes running and ready, one leader"
    return 0
  fi
  echo "::error::$when the nodes are not both running and ready with exactly one leader"
  return 1
}

step_ready() {
  require_subscription
  mkdir -p "$dir"
  wait_healthy "before the drill"
}

power_state() {
  az vm show -d -g "$RG" -n "$1" --query powerState -o tsv 2>/dev/null | tr -d '\r' || true
}

# Record each VM's power state before anything else, then start the ones
# that are not running. The record lives outside DRILL_DIR, which `close`
# deletes, so `restore` reads it after `close`.
step_start() {
  require_subscription
  mkdir -p "$(dirname "$power")"
  local n state tmp="$power.new"
  : >"$tmp"
  for n in node-a node-b; do
    state=$(power_state "$n")
    printf '%s %s\n' "$n" "${state:-unknown}" >>"$tmp"
    say "$n was ${state:-in an unknown state} before the drill"
  done
  mv -f "$tmp" "$power"
  for n in node-a node-b; do
    az vm start -g "$RG" -n "$n" --output none
  done
}

# Put the VMs back as `start` found them: deallocate a VM that was
# deallocated, leave every other one running. A VM whose state was not
# read is left running, with a warning, since stopping a VM that was
# serving would be worse than paying for one more hour.
step_restore() {
  require_subscription
  if [ ! -f "$power" ]; then
    say "no power states were recorded, so no VM was started by this drill"
    return 0
  fi
  local n state fail=0
  while read -r n state; do
    case "$n" in node-a | node-b) ;; *) continue ;; esac
    case "$state" in
      "VM deallocated")
        if az vm deallocate -g "$RG" -n "$n" --output none; then
          say "$n deallocated again, as it was before the drill"
        else
          echo "::error::$n was deallocated before the drill and could not be deallocated again: it is still running and costs money"
          fail=1
        fi
        ;;
      unknown)
        echo "::warning::$n's state before the drill is not known, so it was left running"
        ;;
      *)
        say "$n left running, as it was before the drill ($state)"
        ;;
    esac
  done <"$power"
  [ "$fail" = 1 ] || rm -f "$power"
  return "$fail"
}

case "$step" in
  start) step_start ;;
  ready) step_ready ;;
  open) step_open ;;
  run) step_run ;;
  collect) step_collect ;;
  close) step_close ;;
  restore) step_restore ;;
  *) die "usage: drill.sh start|ready|open|run|collect|close|restore" ;;
esac
