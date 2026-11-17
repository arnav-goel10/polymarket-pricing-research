#!/usr/bin/env bash
# Liveness check for the node, run by poly-health.timer every 30 s.
#
# systemd already restarts a node that exits. This catches one that is
# running but no longer answers: after five checks in a row without an
# answer from /healthz (about 150 s) it restarts the service. Readiness
# (/readyz, the venue feeds) never triggers a restart, since a venue being
# down is not something a restart fixes.
#
# 150 s is longer than the 60 s leader freeze of a drill, so the check does
# not cut a drill fault short.
set -u

state=/run/poly-health.fails
limit=5

if ! systemctl is-active --quiet poly-node.service; then
  echo 0 >"$state"
  exit 0
fi
if curl -fsS -m 3 -o /dev/null http://127.0.0.1:9101/healthz; then
  echo 0 >"$state"
  exit 0
fi
fails=$(($(cat "$state" 2>/dev/null || echo 0) + 1))
echo "$fails" >"$state"
logger -t poly-health "healthz did not answer, $fails of $limit"
if [ "$fails" -ge "$limit" ]; then
  echo 0 >"$state"
  logger -t poly-health "restarting poly-node.service"
  systemctl restart poly-node.service
fi
