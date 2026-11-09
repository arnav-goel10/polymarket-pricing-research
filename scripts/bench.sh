#!/usr/bin/env bash
# Run every benchmark and the latency report, and save raw output with the
# machine spec to bench-results/<date>.txt. Takes about three minutes.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p bench-results
out="bench-results/$(date +%Y-%m-%d).txt"
{
  echo "# Poly benchmark run $(date '+%Y-%m-%d %H:%M %Z'), commit $(git rev-parse --short HEAD)"
  echo "# rustc: $(rustc --version)"
  if [[ "$(uname)" == "Darwin" ]]; then
    echo "# cpu: $(sysctl -n machdep.cpu.brand_string), $(sysctl -n hw.ncpu) cores, $(( $(sysctl -n hw.memsize) / 1073741824 )) GB RAM, macOS $(sw_vers -productVersion)"
  else
    echo "# cpu: $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2), $(nproc) cores"
  fi
  echo "# command: scripts/bench.sh (cargo bench -p market -p pricing -p app; cargo run --release -p app --bin latency_report -- 1000000)"
  echo
  cargo bench -p market -p pricing -p app --bench book --bench pricing --bench replay 2>/dev/null
  echo
  echo "## latency_report (1,000,000 events)"
  cargo run --release -q -p app --bin latency_report -- 1000000
} | tee "$out"
echo "saved $out"
