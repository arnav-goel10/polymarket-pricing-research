#!/usr/bin/env bash
# Run one `chaos local` case and check its outcome.
#
#   scripts/chaos_case.sh NAME EXPECT [chaos local options...]
#
# EXPECT is `pass`, `pass+self_repeats` (pass, and a node repeated keys on
# its own stream, which proves dedup is by key), `pass+late` (pass, the
# merger dropped late copies, and none of them was lost: with venue cuts this
# proves the venue's greeting reached the client live and was dropped, not
# delivered again), or the reason a seeded
# fault must fail with (exit 1 and exactly that reason first). With
# `--archive 1` a stream pass hands the reason to the archive verdict.
#
# The run directory, with the private delivered log, stays in the runner's
# temp directory and is never uploaded.
set -uo pipefail

name=$1
expect=$2
shift 2
bin=${CHAOS_BIN:-target/release}
run="${RUNNER_TEMP:-/tmp}/chaos-$name"
rm -rf "$run" "$run.out"

"$bin/chaos" local --run "$run" "$@" >"$run.out" 2>&1
code=$?

report="$run/report.json"
reason=$(jq -r '.verdict.reason // "none"' "$report" 2>/dev/null || echo none)
if [ "$reason" = none ] && [ -f "$run/archive.json" ]; then
  reason=$(jq -r '.verdict.reason // "none"' "$run/archive.json" 2>/dev/null || echo none)
fi
repeats=$(jq -r '.counters.self_repeats // 0' "$report" 2>/dev/null || echo 0)
late=$(jq -r '.merger.late // 0' "$report" 2>/dev/null || echo 0)
late_lost=$(jq -r '.late.lost // 0' "$report" 2>/dev/null || echo 0)
summary=$(grep '^chaos: ' "$run.out" | tail -n 1)
archive=$(grep '^chaos archive: ' "$run.out" | tail -n 1)
[ -n "$archive" ] && summary="$summary $archive"

ok=0
case "$expect" in
pass) [ "$code" = 0 ] && ok=1 ;;
pass+self_repeats) [ "$code" = 0 ] && [ "$repeats" -gt 0 ] && ok=1 ;;
pass+late) [ "$code" = 0 ] && [ "$late" -gt 0 ] && [ "$late_lost" = 0 ] && ok=1 ;;
*) [ "$code" = 1 ] && [ "$reason" = "$expect" ] && ok=1 ;;
esac

line="$name: expect $expect, exit $code, reason $reason. $summary"
echo "$line"
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  echo "- $line" >>"$GITHUB_STEP_SUMMARY"
fi
if [ "$ok" != 1 ]; then
  echo "::error::$name expected $expect but got exit $code, reason $reason"
  cat "$run.out"
  for f in "$run"/sim.log "$run"/node-*.log "$run"/node-*-data/lease.jsonl; do
    [ -f "$f" ] && { echo "== $f"; tail -n 40 "$f"; }
  done
  exit 1
fi
