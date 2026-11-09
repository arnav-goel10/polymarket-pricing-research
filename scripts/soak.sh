#!/usr/bin/env bash
# Soak: replay a recording at full speed for SECS seconds (default 600) and
# sample resident memory every 10 s. Writes bench-results/soak-<date>.csv and
# the run log next to it.
set -euo pipefail
cd "$(dirname "$0")/.."
rec="${1:?usage: scripts/soak.sh <file.prec> [secs]}"
secs="${2:-600}"
cargo build --release -q -p app --bin soak
mkdir -p bench-results
csv="bench-results/soak-$(date +%Y-%m-%d).csv"
log="${csv%.csv}.log"
./target/release/soak "$rec" --secs "$secs" > "$log" 2>&1 &
pid=$!
echo "elapsed_s,rss_kb" > "$csv"
t=0
while kill -0 "$pid" 2>/dev/null; do
  rss=$(ps -o rss= -p "$pid" 2>/dev/null | tr -d ' ' || true)
  [[ -n "$rss" ]] && echo "$t,$rss" >> "$csv"
  sleep 10
  t=$((t + 10))
done
wait "$pid"
echo "saved $csv and $log"
tail -1 "$log"
