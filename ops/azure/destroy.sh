#!/usr/bin/env bash
# Delete everything: the resource group with the VMs, disks, IPs, network,
# storage account (lease, archive and every archived slot), identities,
# role assignments and budget. After this the project costs nothing.
# It cannot be undone.
#
#   ops/azure/destroy.sh [--yes]
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
if ! group_exists; then
  say "Resource group $RG does not exist. Nothing to delete."
  exit 0
fi

# Only a group this project made. POLY_RG can name any group, and the
# bootstrap template tags the one it creates.
project=$(az group show -n "$RG" --query tags.project -o tsv 2>/dev/null || true)
if [ "$project" != poly-ha ]; then
  die "resource group $RG is not tagged project=poly-ha. Not deleting it."
fi

say ""
say "This deletes resource group $RG and everything in it, for good:"
az resource list -g "$RG" --query "[].{name:name, type:type}" -o table
say ""
run az group delete -n "$RG" --yes
# The record of the subscription-scope deployment is free and holds no
# resource, but it names the group, so it goes too.
run az deployment sub delete --name poly-ha-bootstrap
need_yes

say ""
if group_exists; then
  die "resource group $RG still exists"
fi
left=$(az resource list --tag project=poly-ha --query "length(@)" -o tsv)
[ "$left" = 0 ] || die "$left resources tagged project=poly-ha are left; see ops/azure/status.sh"
say "Deleted. No resource group $RG and no resource tagged project=poly-ha."
