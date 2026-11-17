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
# runs. Before the key goes in, it installs sshd-drill.conf, a Match block
# that holds the drill user to those TCP forwards whatever a key says,
# checked with `sshd -t` before sshd reloads. Then it starts the drill agent and prints this VM's ed25519 host
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

# What sshd lets the drill user do, before any key of this run is there.
# The key's options cover TCP forwards only; the Match block also shuts
# Unix socket, agent and X11 forwards and listeners (sshd-drill.conf).
# `sshd -t` checks the whole config with the new file in it, and a file
# that fails is taken out again before anything reloads, so a bad file can
# never lock SSH out. The setting sshd would apply to the drill user is
# then read back and must be exactly this.
sshd_conf=/etc/ssh/sshd_config.d/20-poly-drill.conf
old=
if ! cmp -s "$drill_dir/sshd-drill.conf" "$sshd_conf"; then
  if [ -e "$sshd_conf" ]; then
    old=$(mktemp)
    cp -p "$sshd_conf" "$old"
  fi
  install -o root -g root -m 0644 "$drill_dir/sshd-drill.conf" "$sshd_conf.new"
  mv -f "$sshd_conf.new" "$sshd_conf"
fi
# sshd -t wants its privilege separation directory, which a socket
# activated ssh.service has not made yet if no one has logged in.
install -d -m 0755 /run/sshd
if ! /usr/sbin/sshd -t >/dev/null 2>&1; then
  if [ -n "$old" ]; then mv -f "$old" "$sshd_conf"; else rm -f "$sshd_conf"; fi
  fail "sshd refused its config with the drill user's settings; sshd was not reloaded"
fi
[ -z "$old" ] || rm -f "$old"
systemctl try-reload-or-restart ssh >/dev/null 2>&1 || fail "sshd did not reload"
effective=$(/usr/sbin/sshd -T -C user=drill,host=drill,addr=127.0.0.1 2>/dev/null) ||
  fail "sshd could not report the drill user's settings"
for want_line in "allowtcpforwarding local" \
  "permitopen 127.0.0.1:9100 127.0.0.1:9101 127.0.0.1:9102" \
  "permitlisten none" "allowstreamlocalforwarding no" \
  "allowagentforwarding no" "x11forwarding no"; do
  grep -qixF -- "$want_line" <<<"$effective" || fail "sshd does not apply '$want_line' to the drill user"
done

# Local time with no zone suffix: sshd reads it in the system's own zone,
# and the Z suffix is not understood by every OpenSSH release.
expiry=$(date -d '+4 hours' +%Y%m%d%H%M)
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
