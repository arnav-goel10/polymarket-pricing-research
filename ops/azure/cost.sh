#!/usr/bin/env bash
# What the project has cost. Read-only: it asks Cost Management for the
# actual cost of the resource group, by meter, and shows the budget.
#
#   ops/azure/cost.sh [--days N]      default: month to date
#
# Cost data lags real use by 8 to 24 hours. A free-hours VM shows a meter
# with a cost of 0; a meter with a cost means that resource is being paid
# for out of the credit.
set -euo pipefail
# shellcheck source=ops/azure/lib.sh
. "$(dirname "$0")/lib.sh"

days=
while [ $# -gt 0 ]; do
  case "$1" in
    --days) days=${2:?}; shift 2 ;;
    *) die "unknown argument $1" ;;
  esac
done
[[ -z $days || $days =~ ^[0-9]+$ ]] || die "--days takes a number"

require_subscription
group_exists || {
  say "Resource group $RG does not exist. Nothing is billed for this project."
  exit 0
}
sub=$(az account show --query id -o tsv)

if [ -n "$days" ]; then
  from=$(date -u -v-"$days"d +%Y-%m-%dT00:00:00Z 2>/dev/null || date -u -d "$days days ago" +%Y-%m-%dT00:00:00Z)
  to=$(date -u +%Y-%m-%dT23:59:59Z)
  period="\"timeframe\":\"Custom\",\"timePeriod\":{\"from\":\"$from\",\"to\":\"$to\"}"
  label="last $days days"
else
  period='"timeframe":"MonthToDate"'
  label="month to date"
fi
agg='"aggregation":{"totalCost":{"name":"Cost","function":"Sum"},"quantity":{"name":"UsageQuantity","function":"Sum"}}'
group='"grouping":[{"type":"Dimension","name":"MeterCategory"},{"type":"Dimension","name":"Meter"}]'
by_meter="{\"type\":\"ActualCost\",$period,\"dataset\":{\"granularity\":\"None\",$agg,$group}}"
total="{\"type\":\"ActualCost\",$period,\"dataset\":{\"granularity\":\"None\",$agg}}"
url="https://management.azure.com/subscriptions/$sub/resourceGroups/$RG/providers/Microsoft.CostManagement/query?api-version=2023-11-01"

say ""
say "Actual cost of $RG, $label, by meter (cost, quantity, category, meter, currency):"
# A query, sent as POST because the API takes its filter in a body.
az rest --method post --url "$url" --body "$by_meter" --query "properties.rows" -o tsv

say ""
say "Total (cost, quantity, currency):"
az rest --method post --url "$url" --body "$total" --query "properties.rows" -o tsv

say ""
say "Budget:"
az consumption budget show -g "$RG" --budget-name poly-ha-monthly \
  --query "{amount:amount, spent:currentSpend.amount, unit:currentSpend.unit}" -o table 2>/dev/null ||
  say "  no budget named poly-ha-monthly"
say ""
say "Credit left: https://www.microsoftazuresponsorships.com/Balance"
