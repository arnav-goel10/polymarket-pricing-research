#!/usr/bin/env bash
# Run one polyctl.sh command on a VM through the VM agent and fail unless
# it succeeded. Used by the deploy workflow; needs no SSH and no open port.
#
#   ops/azure/vm_run.sh VM COMMAND [ARG...]
#   ops/azure/vm_run.sh node-a install <git sha> <sha256>
#   ops/azure/vm_run.sh node-a status
#
# run-command reports success even when the script exits non-zero, so the
# check is the line `POLY_OK <command>` that polyctl.sh prints last.
# This changes the VM (install restarts the node), so it is not for a
# dry run; the workflow is its caller.
set -euo pipefail
rg=${RG:-poly-ha}
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

out=$(az vm run-command invoke -g "$rg" -n "$vm" --command-id RunShellScript \
  --scripts "/opt/poly/polyctl.sh $cmd $*" --query "value[0].message" -o tsv)
printf '%s\n' "$out"
grep -qx "POLY_OK $cmd" <<<"$out" || {
  echo "vm_run: $cmd on $vm did not finish" >&2
  exit 1
}
