#!/usr/bin/env bash
# Decide whether the Azure workflow may act. Read-only.
#
#   ops/azure/gate.sh ENVIRONMENT
#
# Prints `ready=true` only when the GitHub environment exists and limits
# which branches and tags may deploy to it. Otherwise it prints
# `ready=false` and why: `missing` (no such environment), `open` (it
# exists, but any branch or tag may use it) or `unreadable` (the question
# could not be answered). Needs GH_TOKEN and GITHUB_REPOSITORY, as a
# workflow has them.
set -uo pipefail
env_name=${1:?usage: gate.sh ENVIRONMENT}

err=$(mktemp)
trap 'rm -f "$err"' EXIT
if json=$(gh api "repos/$GITHUB_REPOSITORY/environments/$env_name" 2>"$err"); then
  custom=$(jq -r '.deployment_branch_policy.custom_branch_policies // false' <<<"$json")
  if [ "$custom" = true ]; then
    echo "ready=true"
    echo "reason=ok"
  else
    echo "ready=false"
    echo "reason=open"
  fi
elif grep -q "HTTP 404" "$err"; then
  echo "ready=false"
  echo "reason=missing"
else
  echo "ready=false"
  echo "reason=unreadable"
fi
