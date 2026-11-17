# Shared by the drill scripts on a node VM. Sourced, not run.
#
# The drill scripts are installed in /opt/poly/drill by the deploy job
# (ops/azure/vm_tools.sh), not by cloud-init, and run as root through the
# VM agent (ops/azure/vm_run.sh drill-open|drill-collect|drill-close).
# Each ends with one line `POLY_OK <command>` on success, as polyctl.sh
# does, because run-command does not pass the exit code on.
#
# Nothing here prints an address, a key, a token or venue data. The output
# of these scripts reaches the public workflow log only through the drill
# runner (ops/azure/drill.sh), which keeps it in a private file and prints
# counts.
# shellcheck shell=bash disable=SC2034

set -Eeuo pipefail

# The paths can be moved for ops/azure/test_drill_collect.sh. The VM agent
# starts the scripts with none of these set.
env_file=${DRILL_ENV_FILE:-/etc/poly/node.env}
drill_dir=/opt/poly/drill
state_root=${DRILL_STATE_ROOT:-/var/lib/poly-drill}
data=${DRILL_DATA:-/var/lib/poly}
http=http://127.0.0.1:9101
agent_unit=poly-drill-agent
drill_home=/home/drill

# shellcheck source=/dev/null
. "$env_file"

hdr=
tmpd=
trap '[ -z "$hdr" ] || rm -f "$hdr"; [ -z "$tmpd" ] || rm -rf "$tmpd"' EXIT

# A command that fails under `set -e` says where, so the runner's log has a
# reason even when the command's own message is not one of ours. `stage`
# names plain steps, never a path or a value.
stage=start
trap 'echo "drill: ${0##*/} stopped at ${stage} (exit $?)" >&2' ERR

fail() {
  echo "drill: $*" >&2
  exit 1
}

# A run id: the workflow's run id and attempt, plain characters only.
check_run() {
  [[ $1 =~ ^[A-Za-z0-9_-]{1,64}$ ]] || fail "bad run id"
}

# The digest of the installed drill tools, the same expression as
# `ops/azure/vm_tools.sh digest` on the runner.
tools_digest() {
  (cd "$drill_dir" && export LC_ALL=C && sha256sum -- * | sha256sum | cut -c1-64)
}

# The storage token goes from the metadata service into a root-only header
# file and from there to curl. It is never on a command line or printed.
token_header() {
  hdr=$(mktemp)
  curl -fsS -m 5 -H Metadata:true \
    "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https%3A%2F%2Fstorage.azure.com%2F&client_id=${POLY_AZURE_CLIENT_ID}" |
    jq -r '"Authorization: Bearer " + .access_token' >"$hdr" ||
    fail "no identity token"
}

# Upload one file as a new blob under the archive container. Fails if the
# blob exists: a run's files are written once.
put_blob() {
  local src=$1 name=$2
  curl -fsS -m 600 -o /dev/null -X PUT -T "$src" \
    -H @"$hdr" -H "x-ms-version: 2023-11-03" -H "x-ms-blob-type: BlockBlob" \
    -H "If-None-Match: *" \
    "${POLY_AZURE_STORE%/}/archive/${name}" ||
    fail "upload of ${name} failed"
}

node_pid() {
  systemctl show -P MainPID poly-node.service 2>/dev/null || echo 0
}
