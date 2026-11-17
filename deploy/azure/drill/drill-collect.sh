#!/usr/bin/env bash
# Upload what this node recorded during one drill run. Runs as root through
# the VM agent.
#
#   drill-collect.sh <run> <start ms> <end ms>
#
# Into the archive container, under drill/<run>/n<node id>/, with this VM's
# own identity: the agent's fault log (faults.jsonl), the node's lease log
# (lease.jsonl), its counters at the start and the end (metrics-*.txt, the
# poly_ lines only), and every local segment file whose minute slot lies
# between one slot before the start and one after the end, each gzipped
# (seg/<file>.gz). The lifecycle rule deletes drill/ after two days.
#
# Prints counts only. The segments hold venue frames, so they go to the
# private container and never to the workflow log or an artifact.
# shellcheck source=deploy/azure/drill/drill-lib.sh
. "$(dirname "$0")/drill-lib.sh"

[ $# -eq 3 ] || fail "usage: drill-collect.sh <run> <start ms> <end ms>"
run=$1
from=$2
to=$3
check_run "$run"
[[ $from =~ ^[0-9]{13}$ && $to =~ ^[0-9]{13}$ ]] || fail "bad times"
[ "$from" -le "$to" ] || fail "start after end"

state="$state_root/$run"
install -d -o root -g root -m 0700 "$state_root" "$state"
prefix="drill/$run/n$POLY_NODE_ID"
lo=$((from / 60000 - 1))
hi=$((to / 60000 + 1))

stage=token
token_header
tmpd=$(mktemp -d)

stage=logs

[ -f "$state/faults.jsonl" ] || : >"$state/faults.jsonl"
put_blob "$state/faults.jsonl" "$prefix/faults.jsonl"
if [ -f "$data/lease.jsonl" ]; then
  cp "$data/lease.jsonl" "$tmpd/lease.jsonl"
else
  : >"$tmpd/lease.jsonl"
fi
put_blob "$tmpd/lease.jsonl" "$prefix/lease.jsonl"
[ -f "$state/metrics-start.txt" ] || : >"$state/metrics-start.txt"
put_blob "$state/metrics-start.txt" "$prefix/metrics-start.txt"
curl -s -m 3 "$http/metrics" 2>/dev/null | grep '^poly_' >"$tmpd/metrics-end.txt" || true
put_blob "$tmpd/metrics-end.txt" "$prefix/metrics-end.txt"

files=0
bytes=0
declare -A sent=()
shopt -s nullglob
for f in "$data"/seg/*.prec "$data"/seg/*.prec.open; do
  name=${f##*/}
  slot=${name%%.*}
  [[ $slot =~ ^[0-9]+$ ]] || continue
  [ "$slot" -ge "$lo" ] && [ "$slot" -le "$hi" ] || continue
  stage="segment $slot"
  # The node closes a minute's file by renaming it from .prec.open to
  # .prec, and that can happen between the listing above and here: the
  # second rehearsal lost its collect to it on node-a.
  if [ ! -e "$f" ] && [ -e "${f%.open}" ]; then
    f=${f%.open}
    name=${f##*/}
  fi
  [ -z "${sent[$name]:-}" ] || continue
  # From standard input, so a file still being written is read to where it
  # is and gzip does not stop at its size changing; the reader takes a
  # torn last record as the end.
  gzip -c <"$f" >"$tmpd/seg.gz" 2>/dev/null || fail "segment $slot could not be read"
  put_blob "$tmpd/seg.gz" "$prefix/seg/$name.gz"
  sent[$name]=1
  files=$((files + 1))
  bytes=$((bytes + $(stat -c %s "$tmpd/seg.gz")))
done
stage=finished

echo "drill-collect: node $POLY_NODE_ID segments=$files gz_bytes=$bytes slots=$lo..$hi"
echo "POLY_OK drill-collect"
