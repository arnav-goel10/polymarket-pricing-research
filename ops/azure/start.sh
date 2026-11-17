#!/usr/bin/env bash
# Start both VMs. The node service starts on boot if a build is installed.
#
#   ops/azure/start.sh [--yes]
set -euo pipefail
# shellcheck source=ops/azure/lib.sh
. "$(dirname "$0")/lib.sh"
for a in "$@"; do
  case "$a" in
    --yes) YES=1 ;;
    *) die "unknown argument $a" ;;
  esac
done

require_subscription
group_exists || die "resource group $RG does not exist"

say ""
say "Start ${NODES[*]} in $RG. Compute bills again from now."
for n in "${NODES[@]}"; do
  run az vm start -g "$RG" -n "$n" --output none
done
need_yes
az vm list -g "$RG" -d --query "[].{name:name, power:powerState, publicIp:publicIps}" -o table
