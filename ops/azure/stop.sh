#!/usr/bin/env bash
# Deallocate both VMs to stop paying for compute between drills. Disks,
# public IPs and storage keep billing. start.sh brings them back with the
# same addresses and data.
#
#   ops/azure/stop.sh [--yes]
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
say "Deallocate ${NODES[*]} in $RG. The node on each stops, so the stream and"
say "the archive stop. Disks, IPs and storage stay and keep billing."
for n in "${NODES[@]}"; do
  run az vm deallocate -g "$RG" -n "$n" --output none
done
need_yes
az vm list -g "$RG" -d --query "[].{name:name, power:powerState}" -o table
