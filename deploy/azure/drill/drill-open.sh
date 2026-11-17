#!/usr/bin/env bash
# Open this node for one drill run. Runs as root through the VM agent.
#
#   drill-open.sh <run> <key> <tools digest>
#
# <key> is the run's ed25519 public key body in base64url (the runner turns
# + and / into - and _ and drops the padding, so it passes vm_run.sh's
# plain-word rule). It replaces the drill user's authorized_keys with that
# one key, limited to forwarding to the node's stream (9100), its health
# endpoints (9101) and the drill agent (9102) on loopback, no shell, no
# remote forward, and an expiry four hours out in case the close never
# runs. Then it starts the drill agent and prints this VM's ed25519 host
# key, which the runner pins. The runner keeps that output out of the log.
#
# It refuses to run unless the installed drill tools have the digest the
# runner expects, so a drill never runs against scripts of another commit.
# shellcheck source=deploy/azure/drill/drill-lib.sh
. "$(dirname "$0")/drill-lib.sh"

[ $# -eq 3 ] || fail "usage: drill-open.sh <run> <key> <tools digest>"
run=$1
key=$2
want=$3
check_run "$run"
[[ $key =~ ^[A-Za-z0-9_-]{60,100}$ ]] || fail "bad key"
[[ $want =~ ^[0-9a-f]{64}$ ]] || fail "bad digest"
have=$(tools_digest)
[ "$have" = "$want" ] || fail "the drill tools here are not this commit's; push an ha-deploy tag first"
id -u drill >/dev/null 2>&1 || fail "no drill user"

# base64url back to base64, padded to a multiple of four.
b64=$(printf '%s' "$key" | tr '_-' '/+')
while [ $((${#b64} % 4)) -ne 0 ]; do b64="$b64="; done
# An ed25519 public key blob is 51 bytes and names its type.
blob_len=$(printf '%s' "$b64" | base64 -d 2>/dev/null | wc -c)
[ "$blob_len" -eq 51 ] || fail "the key is not an ed25519 public key"
printf '%s' "$b64" | base64 -d | head -c 15 | grep -qa 'ssh-ed25519' || fail "the key is not an ed25519 public key"

expiry=$(date -u -d '+4 hours' +%Y%m%d%H%MZ)
opts="restrict,port-forwarding,permitopen=\"127.0.0.1:9100\",permitopen=\"127.0.0.1:9101\",permitopen=\"127.0.0.1:9102\""
# -R would need a listener; port 1 is privileged, so the drill user can
# open none.
opts="$opts,permitlisten=\"127.0.0.1:1\",command=\"/usr/sbin/nologin\",expiry-time=\"$expiry\""

install -d -o drill -g drill -m 0700 "$drill_home/.ssh"
tmpd=$(mktemp -d)
printf '%s ssh-ed25519 %s drill-%s\n' "$opts" "$b64" "$run" >"$tmpd/keys"
install -o drill -g drill -m 0600 "$tmpd/keys" "$drill_home/.ssh/authorized_keys"

state="$state_root/$run"
install -d -o root -g root -m 0700 "$state_root" "$state"
curl -s -m 3 "$http/metrics" 2>/dev/null | grep '^poly_' >"$state/metrics-start.txt" || true

# One agent at a time. An agent left from an earlier run wakes its node on
# the way out.
systemctl stop "$agent_unit.service" >/dev/null 2>&1 || true
systemctl reset-failed "$agent_unit.service" >/dev/null 2>&1 || true
systemd-run --quiet --unit="$agent_unit" --collect \
  --property=RuntimeMaxSec=14400 \
  /usr/bin/python3 "$drill_dir/drill-agent.py" \
  --listen 127.0.0.1:9102 --http 127.0.0.1:9101 --log "$state/faults.jsonl"

ok=0
for _ in $(seq 1 20); do
  if [ "$(python3 -c '
import socket
s = socket.create_connection(("127.0.0.1", 9102), timeout=2)
s.sendall(b"ping\n")
print(s.recv(64).decode().strip())
' 2>/dev/null || true)" = ok ]; then
    ok=1
    break
  fi
  sleep 0.5
done
[ "$ok" = 1 ] || fail "the drill agent did not start"

echo "drill-open: key installed until $expiry, agent answering"
echo "hostkey $(cut -d' ' -f1,2 /etc/ssh/ssh_host_ed25519_key.pub)"
echo "POLY_OK drill-open"
