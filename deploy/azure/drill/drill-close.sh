#!/usr/bin/env bash
# Close this node after a drill run, whatever state the run left. Runs as
# root through the VM agent, from the drill job's always-run cleanup.
#
#   drill-close.sh <run>
#
# Stops the drill agent (which wakes the node if it froze it), sends
# SIGCONT to the node anyway, empties the drill user's authorized_keys,
# ends any session of that user (and so every SSH forward), and removes
# the run's state. It fails unless the drill user has no key left and the
# agent is gone. Safe to run twice, and on a VM where the drill never
# opened.
# shellcheck source=deploy/azure/drill/drill-lib.sh
. "$(dirname "$0")/drill-lib.sh"

[ $# -eq 1 ] || fail "usage: drill-close.sh <run>"
run=$1
check_run "$run"

systemctl stop "$agent_unit.service" >/dev/null 2>&1 || true
systemctl reset-failed "$agent_unit.service" >/dev/null 2>&1 || true
pid=$(node_pid)
if [ "${pid:-0}" -gt 0 ]; then
  kill -CONT "$pid" 2>/dev/null || true
fi

if [ -d "$drill_home/.ssh" ]; then
  : >"$drill_home/.ssh/authorized_keys"
fi
pkill -KILL -u drill >/dev/null 2>&1 || true
rm -rf "${state_root:?}/$run"

keys=0
if [ -s "$drill_home/.ssh/authorized_keys" ]; then
  keys=$(grep -c . "$drill_home/.ssh/authorized_keys" || true)
fi
agent=$(systemctl is-active "$agent_unit.service" 2>/dev/null || true)
sessions=$(pgrep -c -u drill 2>/dev/null || true)
echo "drill-close: keys=$keys agent=${agent:-inactive} sessions=${sessions:-0} node=$(systemctl is-active poly-node.service || true)"
[ "$keys" = 0 ] || fail "the drill user still has a key"
case "$agent" in
  active | activating | reloading) fail "the drill agent is still running" ;;
esac
echo "POLY_OK drill-close"
