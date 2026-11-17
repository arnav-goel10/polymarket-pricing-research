#!/usr/bin/env bash
# Show what is deployed and whether the VMs are running. Read-only.
#
#   ops/azure/status.sh
set -euo pipefail
# shellcheck source=ops/azure/lib.sh
. "$(dirname "$0")/lib.sh"
[ $# -eq 0 ] || die "status takes no arguments"

require_subscription
if ! group_exists; then
  say "Resource group $RG does not exist. Nothing is deployed and nothing is billed."
  exit 0
fi

say ""
say "VMs (a deallocated VM costs no compute; its disk and IP still bill):"
az vm list -g "$RG" -d \
  --query "[].{name:name, power:powerState, size:hardwareProfile.vmSize, zone:zones[0], publicIp:publicIps}" \
  -o table

say ""
say "Every resource in $RG:"
az resource list -g "$RG" --query "[].{name:name, type:type, project:tags.project}" -o table

say ""
say "Resources tagged project=poly-ha outside $RG (expected: none):"
az resource list --tag project=poly-ha \
  --query "[?resourceGroup!='$RG'].{name:name, type:type, group:resourceGroup}" -o table

say ""
say "Budget:"
az consumption budget show -g "$RG" --budget-name poly-ha-monthly \
  --query "{name:name, amount:amount, spent:currentSpend.amount, unit:currentSpend.unit}" \
  -o table 2>/dev/null || say "  no budget named poly-ha-monthly"
