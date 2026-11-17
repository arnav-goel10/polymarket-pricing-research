#!/usr/bin/env bash
# Install, check and inspect the node on this VM. Runs as root, from the
# deploy workflow through `az vm run-command` or by hand over SSH.
#
#   polyctl.sh install <git sha> <sha256 of the binary>
#   polyctl.sh selftest <id>
#   polyctl.sh status
#
# `install` fetches releases/<sha>/node-<arch> from the archive container
# with this VM's own identity, checks the digest, switches the `current`
# link, restarts the service and waits for /healthz and then /readyz (all
# three venue feeds connected). `selftest` runs the store contract suite
# against the real account under selftest/<id>/. `status` prints the
# service state, the health answers and the lease counters.
#
# Each command ends with one line `POLY_OK <command>` on success. The
# caller looks for that line, because run-command does not pass the exit
# code on. Nothing here prints a token or any venue data.
set -euo pipefail

env_file=/etc/poly/node.env
root=/opt/poly
http=http://127.0.0.1:9101

# shellcheck source=/dev/null
. "$env_file"

hdr=
trap '[ -z "$hdr" ] || rm -f "$hdr"' EXIT

fail() {
  echo "polyctl: $*" >&2
  exit 1
}

http_code() {
  curl -s -m 3 -o /dev/null -w '%{http_code}' "$http$1" 2>/dev/null || true
}

wait_for() {
  local path=$1 limit=$2 i
  for ((i = 0; i < limit; i++)); do
    [ "$(http_code "$path")" = 200 ] && return 0
    sleep 1
  done
  return 1
}

install_build() {
  local sha=$1 want=$2 arch dir got
  [[ $sha =~ ^[0-9a-f]{40}$ ]] || fail "bad git sha"
  [[ $want =~ ^[0-9a-f]{64}$ ]] || fail "bad sha256"
  case "$(uname -m)" in
    x86_64) arch=x64 ;;
    aarch64) arch=arm64 ;;
    *) fail "unknown architecture $(uname -m)" ;;
  esac
  dir=$root/releases/$sha
  mkdir -p "$dir"
  # The token goes from the metadata service into a root-only header file
  # and from there to curl. It is never on a command line or in the log.
  hdr=$(mktemp)
  curl -fsS -m 5 -H Metadata:true \
    "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https%3A%2F%2Fstorage.azure.com%2F&client_id=${POLY_AZURE_CLIENT_ID}" |
    jq -r '"Authorization: Bearer " + .access_token' >"$hdr" ||
    fail "no identity token"
  curl -fsS -m 120 -H @"$hdr" -H "x-ms-version: 2023-11-03" \
    -o "$dir/node.part" \
    "${POLY_AZURE_STORE%/}/archive/releases/${sha}/node-${arch}" ||
    fail "download of releases/${sha}/node-${arch} failed"
  rm -f "$hdr"
  got=$(sha256sum "$dir/node.part" | cut -d' ' -f1)
  if [ "$got" != "$want" ]; then
    rm -f "$dir/node.part"
    fail "digest mismatch: got $got"
  fi
  chmod 0755 "$dir/node.part"
  mv "$dir/node.part" "$dir/node"
  ln -sfn "$dir" "$root/current.new"
  mv -T "$root/current.new" "$root/current"
  systemctl restart poly-node.service
  wait_for /healthz 30 || fail "/healthz not 200 within 30 s"
  wait_for /readyz 120 || fail "/readyz not 200 within 120 s: a venue feed is not connected"
  # Keep this build and the two before it.
  find "$root/releases" -mindepth 1 -maxdepth 1 -type d -printf '%T@ %p\n' |
    sort -rn | tail -n +4 | cut -d' ' -f2- | while read -r old; do
    [ "$old" = "$dir" ] || rm -rf "$old"
  done
  echo "installed $sha ($arch), healthz 200, readyz 200"
}

selftest() {
  local id=$1
  [ -x "$root/current/node" ] || fail "no build installed"
  runuser -u poly -- env POLY_AZURE_CLIENT_ID="$POLY_AZURE_CLIENT_ID" \
    "$root/current/node" --data /var/lib/poly \
    --azure-store "$POLY_AZURE_STORE" --azure-selftest "$id" ||
    fail "store self test failed"
}

status() {
  echo "node_id=$POLY_NODE_ID"
  echo "build=$(basename "$(readlink -f "$root/current" 2>/dev/null || echo none)")"
  echo "service=$(systemctl is-active poly-node.service || true)"
  echo "watchdog=$(systemctl is-active poly-health.timer || true)"
  echo "healthz=$(http_code /healthz)"
  echo "readyz=$(http_code /readyz)"
  # Counters only: epoch, leadership, commits, fences. No frame content.
  echo "ha=$(curl -s -m 3 "$http/ha" 2>/dev/null || true)"
  curl -s -m 3 "$http/metrics" 2>/dev/null |
    grep -E '^poly_(lease_epoch|lease_leader|fence_rejected_total|archive_(commits|uploads|upload_errors|holes)_total) ' || true
}

cmd=${1:-}
case "$cmd" in
  install)
    [ $# -eq 3 ] || fail "usage: polyctl.sh install <git sha> <sha256>"
    install_build "$2" "$3"
    ;;
  selftest)
    [ $# -eq 2 ] || fail "usage: polyctl.sh selftest <id>"
    selftest "$2"
    ;;
  status)
    status
    ;;
  *)
    fail "usage: polyctl.sh install <git sha> <sha256> | selftest <id> | status"
    ;;
esac
echo "POLY_OK $cmd"
