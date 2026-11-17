#!/usr/bin/env bash
# Decide whether the Azure workflow may act. Read-only.
#
#   ops/azure/gate.sh ENVIRONMENT
#
# Prints `ready=true` only when the GitHub environment exists, limits which
# branches and tags may deploy to it, and every entry of that list is a tag
# pattern starting with `ha-`. Otherwise it prints `ready=false` and why:
# `missing` (no such environment), `open` (it exists, but any branch or tag
# may use it), `policy` (the list is empty or holds something other than
# ha-* tags) or `unreadable` (the question could not be answered). Needs
# GH_TOKEN and GITHUB_REPOSITORY, as a workflow has them.
set -uo pipefail
env_name=${1:?usage: gate.sh ENVIRONMENT}

verdict() {
  if [ "$1" = ok ]; then
    echo "ready=true"
  else
    echo "ready=false"
  fi
  echo "reason=$1"
  exit 0
}

err=$(mktemp)
trap 'rm -f "$err"' EXIT
api="repos/$GITHUB_REPOSITORY/environments/$env_name"
if ! json=$(gh api "$api" 2>"$err"); then
  if grep -q "HTTP 404" "$err"; then
    verdict missing
  fi
  verdict unreadable
fi
custom=$(jq -r '.deployment_branch_policy.custom_branch_policies // false' <<<"$json") || verdict unreadable
[ "$custom" = true ] || verdict open

# The list itself: one page holds every entry, or the answer is not trusted.
policies=$(gh api "$api/deployment-branch-policies?per_page=100" 2>"$err") || verdict unreadable
counts=$(jq -r '
  (.branch_policies // []) as $p
  | [.total_count, ($p | length),
     ($p | map(select(.type == "tag" and (.name | startswith("ha-")))) | length)]
  | @tsv' <<<"$policies") || verdict unreadable
read -r total listed good <<<"$counts"
[ "$total" = "$listed" ] || verdict unreadable
if [ "$listed" -ge 1 ] && [ "$good" = "$listed" ]; then
  verdict ok
fi
verdict policy
