#!/usr/bin/env bash
# Check the promise the scripts make: without --yes nothing is changed, on
# the wrong subscription nothing is changed, and with --yes exactly the
# expected commands run. A fake `az` records every call; no real one is
# made, so this runs in CI with no Azure account.
#
#   ops/azure/test_dry_run.sh
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir "$tmp/bin"
export AZ_LOG="$tmp/az.log" AZ_STATE="$tmp/state"

cat >"$tmp/bin/az" <<'STUB'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$AZ_LOG"
case "$*" in
  "account show --query name -o tsv") echo "${FAKE_SUBSCRIPTION:-Azure for Students}" ;;
  "account show --query id -o tsv") echo 00000000-0000-0000-0000-000000000000 ;;
  "group exists"*)
    if [ -e "$AZ_STATE/group-deleted" ]; then echo false; else echo "${FAKE_GROUP_EXISTS:-true}"; fi
    ;;
  "group delete"*) touch "$AZ_STATE/group-deleted" ;;
  "provider show"*) echo "${FAKE_PROVIDER_STATE:-Registered}" ;;
  "consumption budget show"*) [ "${FAKE_BUDGET:-0}" = 1 ] || exit 3 ;;
  "resource list --tag"*"length(@)"*) echo 0 ;;
esac
STUB
chmod +x "$tmp/bin/az"
PATH="$tmp/bin:$PATH"

key="$tmp/id.pub"
echo "ssh-ed25519 AAAAnotARealKeyOnlyTheShapeOfOne dry-run" >"$key"
deploy_args=(--github-repo owner/repo --budget-email owner@example.com
  --ssh-cidr 203.0.113.7/32 --ssh-key-file "$key")

# Calls that change something in Azure.
mutating='^(deployment (sub|group|mg|tenant) (create|delete)|group (create|delete)|vm (deallocate|start|stop|delete|create|restart)|provider register|role assignment|storage|identity|ad |account set|login)'

fails=0
check() {
  if "$@"; then
    printf 'ok    %s\n' "$name"
  else
    printf 'FAIL  %s\n' "$name"
    fails=$((fails + 1))
  fi
}

# Run a script with a clean log. Sets $code and $out.
start() {
  name=$1
  shift
  rm -rf "$AZ_STATE"
  mkdir "$AZ_STATE"
  : >"$AZ_LOG"
  set +e
  out=$("$@" 2>&1)
  code=$?
  set -e
}

changed() {
  grep -Ec "$mutating" "$AZ_LOG" || true
}

no_change() {
  [ "$(changed)" = 0 ]
}

for s in stop start destroy; do
  start "$s without --yes changes nothing" "$here/$s.sh"
  check test "$code" = 0
  check no_change
  check grep -q "Nothing was changed" <<<"$out"
done

start "deploy without --yes changes nothing" "$here/deploy.sh" "${deploy_args[@]}"
check test "$code" = 0
check no_change
check grep -q "Nothing was changed" <<<"$out"
check grep -q "az deployment sub create" <<<"$out"

start "deploy --what-if still changes nothing" "$here/deploy.sh" "${deploy_args[@]}" --what-if
check test "$code" = 0
check no_change
check grep -q "^deployment sub what-if" "$AZ_LOG"

start "status changes nothing" "$here/status.sh"
check test "$code" = 0
check no_change

start "cost changes nothing" "$here/cost.sh"
check test "$code" = 0
check no_change

for s in stop start destroy; do
  start "$s refuses another subscription" env FAKE_SUBSCRIPTION=Klimacap "$here/$s.sh" --yes
  check test "$code" != 0
  check no_change
  check grep -q "Not touching it" <<<"$out"
done
for s in status cost; do
  start "$s refuses another subscription" env FAKE_SUBSCRIPTION=Klimacap "$here/$s.sh"
  check test "$code" != 0
  check grep -q "Not touching it" <<<"$out"
done
start "deploy refuses another subscription" env FAKE_SUBSCRIPTION=Klimacap "$here/deploy.sh" "${deploy_args[@]}" --yes
check test "$code" != 0
check no_change

start "deploy --yes stops at an unregistered provider" env FAKE_PROVIDER_STATE=NotRegistered "$here/deploy.sh" "${deploy_args[@]}" --yes
check test "$code" = 1
check no_change
check grep -q "az provider register --namespace Microsoft.Compute --wait" <<<"$out"

start "deploy refuses SSH open to everyone" "$here/deploy.sh" --github-repo owner/repo \
  --budget-email owner@example.com --ssh-cidr 0.0.0.0/0 --ssh-key-file "$key" --yes
check test "$code" != 0
check no_change

start "deploy --yes runs the two deployments and nothing else" "$here/deploy.sh" "${deploy_args[@]}" --yes
check test "$code" = 0
check test "$(changed)" = 2
check grep -q "^deployment sub create --name poly-ha-bootstrap --location eastasia .*deployBudget=true" "$AZ_LOG"
check grep -q "^deployment group create --name poly-ha-nodes --resource-group poly-ha .*sshSourceCidr=203.0.113.7/32" "$AZ_LOG"

start "a second deploy leaves the budget alone" env FAKE_BUDGET=1 "$here/deploy.sh" "${deploy_args[@]}" --yes
check test "$code" = 0
check grep -q "^deployment sub create .*deployBudget=false" "$AZ_LOG"

start "deploy --no-budget leaves the budget out" "$here/deploy.sh" "${deploy_args[@]}" --no-budget --yes
check test "$code" = 0
check grep -q "^deployment sub create .*deployBudget=false" "$AZ_LOG"

start "stop --yes deallocates both VMs and nothing else" "$here/stop.sh" --yes
check test "$code" = 0
check test "$(changed)" = 2
check grep -q "^vm deallocate -g poly-ha -n node-a" "$AZ_LOG"
check grep -q "^vm deallocate -g poly-ha -n node-b" "$AZ_LOG"

start "start --yes starts both VMs and nothing else" "$here/start.sh" --yes
check test "$code" = 0
check test "$(changed)" = 2
check grep -q "^vm start -g poly-ha -n node-a" "$AZ_LOG"
check grep -q "^vm start -g poly-ha -n node-b" "$AZ_LOG"

start "destroy --yes deletes the group and the deployment record" "$here/destroy.sh" --yes
check test "$code" = 0
check test "$(changed)" = 2
check grep -q "^group delete -n poly-ha --yes" "$AZ_LOG"
check grep -q "^deployment sub delete --name poly-ha-bootstrap" "$AZ_LOG"

start "destroy with no group deletes nothing" env FAKE_GROUP_EXISTS=false "$here/destroy.sh" --yes
check test "$code" = 0
check no_change

if [ "$fails" -gt 0 ]; then
  echo "$fails checks failed"
  exit 1
fi
echo "all checks passed"
