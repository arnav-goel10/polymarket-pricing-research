# Shared by the scripts in ops/azure. Sourced, not run.
#
# Every script prints what it is about to do. A script that changes
# anything in Azure does so only when it is given --yes; without it the
# script stops after the plan, having made read-only calls at most.
# Some of the names below are used only by the scripts that source this.
# shellcheck shell=bash disable=SC2034

RG=${POLY_RG:-poly-ha}
LOCATION=${POLY_LOCATION:-eastasia}
# The scripts refuse to act on any other subscription. They never switch
# subscription or tenant themselves.
SUBSCRIPTION_NAME=${POLY_SUBSCRIPTION_NAME:-Azure for Students}
NODES=(node-a node-b)
PROVIDERS=(Microsoft.Compute Microsoft.Network Microsoft.Storage Microsoft.ManagedIdentity)
YES=0

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
export REPO_ROOT

say() {
  printf '%s\n' "$*"
}

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

# Print a command, and run it only with --yes.
run() {
  printf '  $'
  printf ' %q' "$@"
  printf '\n'
  if [ "$YES" -eq 1 ]; then
    "$@"
  fi
}

# Stop here unless --yes was given.
need_yes() {
  if [ "$YES" -ne 1 ]; then
    say ""
    say "Nothing was changed. Run again with --yes to do the above."
    exit 0
  fi
}

require_subscription() {
  command -v az >/dev/null || die "the az CLI is not installed"
  local name
  name=$(az account show --query name -o tsv 2>/dev/null) || die "az is not signed in"
  if [ "$name" != "$SUBSCRIPTION_NAME" ]; then
    die "the active subscription is '$name', not '$SUBSCRIPTION_NAME'. Not touching it."
  fi
  say "Subscription: $name"
}

group_exists() {
  [ "$(az group exists -n "$RG" -o tsv)" = true ]
}
