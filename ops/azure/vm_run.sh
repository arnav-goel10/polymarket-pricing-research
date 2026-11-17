#!/usr/bin/env bash
# Run one polyctl.sh command on a VM through the VM agent and fail unless
# it succeeded. Used by the deploy workflow; needs no SSH and no open port.
#
#   ops/azure/vm_run.sh VM COMMAND [ARG...]
#   ops/azure/vm_run.sh node-a install <git sha> <sha256>
#   ops/azure/vm_run.sh node-a status
#   ops/azure/vm_run.sh node-a drill-close <run>
#
# run-command reports success even when the script exits non-zero, so the
# check is the line `POLY_OK <command>` that polyctl.sh prints last.
# This changes the VM (install restarts the node) and has no dry run; the
# workflow is its caller. Like the other scripts it refuses any
# subscription but the project's.
set -euo pipefail
# shellcheck source=ops/azure/lib.sh
. "$(dirname "$0")/lib.sh"
vm=${1:?usage: vm_run.sh VM COMMAND [ARG...]}
cmd=${2:?usage: vm_run.sh VM COMMAND [ARG...]}
shift 2

case "$vm" in
  node-a | node-b) ;;
  *) echo "vm_run: unknown VM $vm" >&2; exit 2 ;;
esac
# The arguments end up in a shell command on the VM, so only plain words
# pass.
for word in "$cmd" "$@"; do
  [[ $word =~ ^[A-Za-z0-9_-]+$ ]] || {
    echo "vm_run: refusing argument with other characters than letters, digits, - and _" >&2
    exit 2
  }
done

# On a failure the runner's log gets one short reason: an error class and
# the stage, never the VM's own text, which can carry addresses or the
# storage account. vm_reason reads the VM's output on stdin.
vm_reason() {
  local text line
  text=$(cat)
  line=$(grep -E '^drill: ' <<<"$text" | tail -n 1 || true)
  if [ -n "$line" ]; then
    # Our own messages, cut to plain characters, with anything shaped like
    # an address or a URL taken out.
    sed -E 's#[a-z]+://[^ ]*#<url>#g; s/[0-9]{1,3}(\.[0-9]{1,3}){3}/<addr>/g' <<<"$line" |
      tr -cd 'A-Za-z0-9 _:./()=<>-' | cut -c1-160
    echo
  elif grep -q '^gzip: .*No such file' <<<"$text"; then
    echo "gzip error (file missing)"
  elif grep -q '^gzip: .*changed while' <<<"$text"; then
    echo "gzip error (file changed)"
  elif grep -q '^gzip: ' <<<"$text"; then
    echo "gzip error"
  elif line=$(grep -oE '^curl: \([0-9]+\)' <<<"$text" | tail -n 1) && [ -n "$line" ]; then
    echo "upload error ($line)"
  elif grep -q 'Killed' <<<"$text"; then
    echo "killed (out of memory?)"
  elif ! grep -vqE '^(Enable succeeded:.*|\[stdout\]|\[stderr\]|[[:space:]]*)$' <<<"$text"; then
    echo "no output (run-command limit or timeout)"
  else
    echo "no finish marker and no error line"
  fi
}

# The class of an az failure, from its error text on stdin.
az_reason() {
  local text
  text=$(cat)
  case "$text" in
    *Conflict* | *"in progress"* | *"another operation"* | *OperationNotAllowed*) echo "busy: another run-command or operation on the VM" ;;
    *imed\ out* | *imeout*) echo "timeout" ;;
    *AuthorizationFailed* | *AADSTS* | *expired* | *"az login"*) echo "not signed in or not allowed" ;;
    *NotFound*) echo "VM not found" ;;
    *) echo "other az error" ;;
  esac
}

if [ "$cmd" = reason ]; then
  # For ops/azure/test_dry_run.sh: classify text on stdin.
  case "${1:-vm}" in az) az_reason ;; *) vm_reason ;; esac
  exit 0
fi

# Its output is the command's output, so the subscription line goes to stderr.
require_subscription >&2

# The drill commands are scripts of their own, installed by vm_tools.sh;
# everything else is a polyctl.sh command.
case "$cmd" in
  drill-open | drill-collect | drill-close) script="/opt/poly/drill/$cmd.sh" ;;
  *) script="/opt/poly/polyctl.sh $cmd" ;;
esac

err=$(mktemp)
trap 'rm -f "$err"' EXIT
rc=0
out=$(az vm run-command invoke -g "$RG" -n "$vm" --command-id RunShellScript \
  --scripts "$script $*" --query "value[0].message" -o tsv 2>"$err") || rc=$?
if [ "$rc" -ne 0 ]; then
  echo "vm_run: $cmd on $vm failed: $(az_reason <"$err")" >&2
  exit 1
fi
printf '%s\n' "$out"
grep -qx "POLY_OK $cmd" <<<"$out" || {
  echo "vm_run: $cmd on $vm did not finish: $(vm_reason <<<"$out")" >&2
  exit 1
}
