#!/usr/bin/env bash
# The drill path end to end on loopback, with no Azure and no SSH: the
# venue simulator, two nodes on one directory store, each under a restart
# loop that stands in for systemd, the real drill agent beside each
# (deploy/azure/drill/drill-agent.py, given the node's pid file instead of
# systemd), `chaos drill` with a rehearsal's faults (three kills and one
# leader freeze), the collection drill-collect.sh does (each node's segment
# files gzipped, its lease log, the agent's fault log), `chaos store-dump`
# for HEAD and the object list, and `chaos verify-drill` with the union of
# the two nodes' recordings as truth. The same run is also judged against
# the simulator's own manifest, which sees every event, as a check on the
# union.
#
#   ops/azure/test_drill_local.sh        (binaries from CHAOS_BIN, default target/release)
#
# Run by the drill-local job of CI on every push. Prints counts only.
set -euo pipefail
bin=${CHAOS_BIN:-target/release}
root=$(cd "$(dirname "$0")/../.." && pwd)
run=${RUNNER_TEMP:-/tmp}/drill-local
rm -rf "$run"
mkdir -p "$run"
pids=()

cleanup() {
  touch "$run/stop"
  for p in "${pids[@]}"; do
    pkill -TERM -P "$p" >/dev/null 2>&1 || true
    kill -TERM "$p" >/dev/null 2>&1 || true
  done
  for i in 1 2; do
    [ -f "$run/n$i.pid" ] && kill -CONT "$(cat "$run/n$i.pid")" >/dev/null 2>&1 || true
  done
  sleep 1
  for p in "${pids[@]}"; do
    pkill -KILL -P "$p" >/dev/null 2>&1 || true
    kill -KILL "$p" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

dump() {
  for f in "$run"/sim.log "$run"/n?.log "$run"/n?-faults.jsonl "$run"/agent-n?.log "$run"/n?-data/lease.jsonl; do
    [ -f "$f" ] && { echo "== $f"; tail -n 30 "$f"; }
  done
  return 0
}

"$bin/venue_sim" --seed 7 --rate 200 --manifest "$run/truth.prec" --start-after 2 \
  >"$run/sim.out" 2>"$run/sim.log" &
pids+=($!)
for _ in $(seq 1 100); do
  [ -s "$run/sim.out" ] && break
  sleep 0.1
done
read -r w0 _ rtds _ binance _ clob <"$run/sim.out"
[ "$w0" = venue_sim ] || { echo "venue_sim did not start"; dump; exit 1; }

# A node restarted a second after it exits, as poly-node.service does.
node_loop() {
  local i=$1 port=$2 http=$3
  while [ ! -e "$run/stop" ]; do
    # tokA is the simulator's first asset (app::sim::ASSETS), the one
    # chaos local gives its nodes.
    "$bin/node" --data "$run/n$i-data" --node "$i" \
      --listen "127.0.0.1:$port" --http "127.0.0.1:$http" \
      --rtds-url "$rtds" --binance-url "$binance" --clob-url "$clob" --token tokA \
      --store "$run/store" --slot-ms 6000 --lease-timing 4000,1000,500,1000,500 \
      >>"$run/n$i.out" 2>>"$run/n$i.log" &
    echo $! >"$run/n$i.pid"
    wait $! || true
    [ -e "$run/stop" ] || sleep 1
  done
}
node_loop 1 21100 21101 &
pids+=($!)
node_loop 2 22100 22101 &
pids+=($!)
for i in 1 2; do
  for _ in $(seq 1 100); do
    [ -s "$run/n$i.pid" ] && break
    sleep 0.1
  done
  python3 "$root/deploy/azure/drill/drill-agent.py" --listen "127.0.0.1:2${i}102" \
    --http "127.0.0.1:2${i}101" --pidfile "$run/n$i.pid" --log "$run/n$i-faults.jsonl" \
    --max-seconds 900 >"$run/agent-n$i.log" 2>&1 &
  pids+=($!)
done
sleep 2

code=0
"$bin/chaos" drill --run "$run/run" \
  --nodes ws://127.0.0.1:21100,ws://127.0.0.1:22100 \
  --http 127.0.0.1:21101,127.0.0.1:22101 \
  --agents 127.0.0.1:21102,127.0.0.1:22102 \
  --seed 7 --faults 4 --leader-freezes 1 --gap-ms 8000 --leader-freeze-ms 9000 \
  --margin-s 10 --ready-limit-s 30 || code=$?
[ "$code" = 0 ] || { echo "chaos drill exited $code"; dump; exit 1; }

# Stop the nodes (they release the lease) so their files are final.
touch "$run/stop"
for i in 1 2; do
  kill -TERM "$(cat "$run/n$i.pid")" >/dev/null 2>&1 || true
done
sleep 3

col="$run/collected"
for i in 1 2; do
  mkdir -p "$col/n$i/seg"
  for f in "$run/n$i-data"/seg/*.prec "$run/n$i-data"/seg/*.prec.open; do
    [ -e "$f" ] || continue
    gzip -c "$f" >"$col/n$i/seg/${f##*/}.gz"
  done
  cp "$run/n$i-data/lease.jsonl" "$col/n$i/lease.jsonl"
  cp "$run/n$i-faults.jsonl" "$col/n$i/faults.jsonl"
done
"$bin/chaos" store-dump --store "$run/store" --out "$col"

fail=0
# Against the simulator's total truth first: verify writes report.json,
# which verify-drill then replaces.
"$bin/chaos" verify --run "$run/run" --truth "$run/truth.prec" >"$run/verify-sim.out" 2>&1 || {
  echo "the drill's client fails against the simulator's own truth"
  fail=1
}
grep '^chaos: ' "$run/verify-sim.out" | sed 's/^/against the simulator: /' || true
"$bin/chaos" verify-drill --run "$run/run" --collected "$col" --slot-ms 6000 >"$run/verify.out" 2>&1 || {
  echo "verify-drill did not pass"
  fail=1
}
grep '^chaos drill: ' "$run/verify.out" || cat "$run/verify.out"

report="$run/run/report.json"
control=$(jq -r '.control_missing' "$report")
compared=$(jq -r '.content.compared[1]' "$report")
agent_kills=$(cat "$run"/n?-faults.jsonl | grep -c '"ev": "kill9"' || true)
agent_freezes=$(cat "$run"/n?-faults.jsonl | grep -c '"ev": "freeze_leader"' || true)
echo "control_missing=$control binance_keys_compared=$compared agent_kills=$agent_kills agent_leader_freezes=$agent_freezes"
[ "$control" -gt 0 ] || { echo "the control client missed nothing"; fail=1; }
[ "$compared" -gt 0 ] || { echo "no content was compared"; fail=1; }
[ "$agent_kills" = 3 ] && [ "$agent_freezes" = 1 ] || { echo "the agents did not log three kills and one leader freeze"; fail=1; }
if [ "$fail" != 0 ]; then
  cat "$run/verify.out"
  dump
  exit 1
fi
echo "drill path on loopback: pass"
