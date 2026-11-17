#!/usr/bin/env bash
# Deploy the two-node setup: first the group, identities, storage, roles
# and budget (bootstrap.bicep, subscription scope), then the network and
# the two VMs (nodes.bicep).
#
#   ops/azure/deploy.sh --github-repo OWNER/REPO --budget-email ADDRESS \
#       --ssh-cidr A.B.C.D/32 --ssh-key-file ~/.ssh/id_ed25519.pub \
#       [--bootstrap-only] [--no-budget] [--what-if] [--yes]
#
# Without --yes it prints the plan and changes nothing. With --what-if it
# also asks Azure what the bootstrap template would create, which creates
# nothing either. --no-budget leaves the budget out, for a subscription
# that refuses one. The node binary is not installed here; the ha-deploy tag
# does that (see docs/AZURE_RUNBOOK.md).
set -euo pipefail
# shellcheck source=ops/azure/lib.sh
. "$(dirname "$0")/lib.sh"

repo="" subject_repo="" email="" cidr="" key_file="" bootstrap_only=0 what_if=0 no_budget=0
while [ $# -gt 0 ]; do
  case "$1" in
    --github-repo) repo=${2:?}; shift 2 ;;
    --github-subject-repo) subject_repo=${2:?}; shift 2 ;;
    --budget-email) email=${2:?}; shift 2 ;;
    --ssh-cidr) cidr=${2:?}; shift 2 ;;
    --ssh-key-file) key_file=${2:?}; shift 2 ;;
    --bootstrap-only) bootstrap_only=1; shift ;;
    --no-budget) no_budget=1; shift ;;
    --what-if) what_if=1; shift ;;
    --yes) YES=1; shift ;;
    *) die "unknown argument $1" ;;
  esac
done

[[ $repo =~ ^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$ ]] || die "--github-repo OWNER/REPO is required"
# GitHub can present the subject with immutable ids: OWNER@ID/REPO@ID.
[ -z "$subject_repo" ] || [[ $subject_repo =~ ^[A-Za-z0-9._-]+@[0-9]+/[A-Za-z0-9._-]+@[0-9]+$ ]] || die "--github-subject-repo must look like OWNER@ID/REPO@ID"
[[ $email =~ ^[^@[:space:]]+@[^@[:space:]]+\.[^@[:space:]]+$ ]] || die "--budget-email is required"
if [ "$bootstrap_only" -eq 0 ]; then
  # The same check the ha-infra job of the workflow runs.
  "$(dirname "$0")/check_cidr.sh" "$cidr" || die "--ssh-cidr A.B.C.D/N is required, /24 or narrower"
  [ -r "$key_file" ] || die "--ssh-key-file must name a readable public key"
  case "$(cut -d' ' -f1 "$key_file")" in
    ssh-ed25519 | ssh-rsa) ;;
    *) die "$key_file is not an ssh-ed25519 or ssh-rsa public key" ;;
  esac
fi

require_subscription

missing=()
for p in "${PROVIDERS[@]}"; do
  state=$(az provider show -n "$p" --query registrationState -o tsv 2>/dev/null || echo Unknown)
  [ "$state" = Registered ] || missing+=("$p")
done
if [ "${#missing[@]}" -gt 0 ]; then
  say "These resource providers are not registered. Register them once, then run this again:"
  for p in "${missing[@]}"; do
    say "  az provider register --namespace $p --wait"
  done
  if [ "$YES" -eq 1 ]; then
    exit 1
  fi
fi

# A budget's start date cannot change, so it is created once and left
# alone on every later deploy.
deploy_budget=true
if [ "$no_budget" -eq 1 ]; then
  deploy_budget=false
elif group_exists && az consumption budget show -g "$RG" --budget-name poly-ha-monthly >/dev/null 2>&1; then
  deploy_budget=false
fi

bootstrap=(az deployment sub create --name poly-ha-bootstrap --location "$LOCATION"
  --template-file "$REPO_ROOT/deploy/azure/bootstrap.bicep"
  --parameters "location=$LOCATION" "resourceGroupName=$RG" "githubRepo=$repo" "githubSubjectRepo=$subject_repo"
  "budgetEmail=$email" "deployBudget=$deploy_budget" --output none)

say ""
say "Plan, in region $LOCATION, resource group $RG:"
say "1. Create or update the group, three identities (two for the VMs, one for"
say "   GitHub Actions, trusted only for repo:$repo:environment:azure), one"
say "   storage account with containers lease and archive, a 2-day lifecycle rule,"
say "   blob access for the identities on that account, and a monthly budget of 20"
say "   with alerts to $email (budget created this run: $deploy_budget)."
if [ "$what_if" -eq 1 ]; then
  say ""
  say "What Azure says the bootstrap template would change (nothing is created):"
  az deployment sub what-if --name poly-ha-bootstrap --location "$LOCATION" \
    --template-file "$REPO_ROOT/deploy/azure/bootstrap.bicep" \
    --parameters "location=$LOCATION" "resourceGroupName=$RG" "githubRepo=$repo" "githubSubjectRepo=$subject_repo" \
    "budgetEmail=$email" "deployBudget=$deploy_budget" --result-format ResourceIdOnly
fi
run "${bootstrap[@]}"

if [ "$bootstrap_only" -eq 0 ]; then
  nodes=(az deployment group create --name poly-ha-nodes --resource-group "$RG"
    --template-file "$REPO_ROOT/deploy/azure/nodes.bicep"
    --parameters "sshSourceCidr=$cidr" "sshPublicKey=$(cat "$key_file")" --output none)
  say "2. Create or update the network (SSH from $cidr only, all other inbound"
  say "   denied), two static public IPs and two VMs: node-a Standard_B2ats_v2 in"
  say "   zone 2 and node-b Standard_B2pts_v2 (Arm64) in zone 1, each with a 30 GB"
  say "   standard disk, key-only SSH and its own identity."
  run "${nodes[@]}"
fi

need_yes

say ""
say "Deployed. Values for the GitHub environment 'azure' (variables, not secrets):"
az deployment sub show --name poly-ha-bootstrap \
  --query "properties.outputs.{AZURE_CLIENT_ID:deployerClientId.value, AZURE_TENANT_ID:tenantId.value, AZURE_SUBSCRIPTION_ID:subscriptionId.value, storageAccount:storageAccount.value}" \
  -o yaml
if [ "$bootstrap_only" -eq 0 ]; then
  az deployment group show --name poly-ha-nodes --resource-group "$RG" \
    --query "properties.outputs.nodes.value" -o table
fi
