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
a="$*"
case "$*" in
  "account show --query name -o tsv") echo "${FAKE_SUBSCRIPTION:-Azure for Students}" ;;
  "account show --query id -o tsv") echo 00000000-0000-0000-0000-000000000000 ;;
  "group exists"*)
    if [ -e "$AZ_STATE/group-deleted" ]; then echo false; else echo "${FAKE_GROUP_EXISTS:-true}"; fi
    ;;
  "group delete"*) touch "$AZ_STATE/group-deleted" ;;
  "group show"*"tags.project"*) echo "${FAKE_GROUP_TAG-poly-ha}" ;;
  "vm run-command invoke"*)
    if [ -n "${FAKE_RUN_OUTPUT:-}" ]; then
      echo "$FAKE_RUN_OUTPUT"
      exit 0
    fi
    case "$*" in *"-n node-a "*) vm=node-a id=1 ;; *) vm=node-b id=2 ;; esac
    case "$*" in
      *"--scripts @"*)
        file=${a##*--scripts @}
        file=${file%% *}
        want=$(sed -n 's/.*!= "\([0-9a-f]\{64\}\)".*/\1/p' "$file")
        echo "tools ${FAKE_TOOLS_DIGEST:-$want}"
        echo "POLY_OK tools"
        ;;
      *drill-open.sh*)
        if [ "${FAKE_OPEN_FAIL:-}" = "$vm" ]; then
          echo "drill: the drill tools here are not this commit's"
          exit 0
        fi
        echo "drill-open: key installed, agent answering"
        echo "hostkey ssh-ed25519 AAAAC3NzaC1lZDI1NTE5HostKeyOf$vm"
        echo "POLY_OK drill-open"
        ;;
      *drill-collect.sh*)
        echo "drill-collect: node $id segments=3 gz_bytes=100 slots=1..2"
        echo "POLY_OK drill-collect"
        ;;
      *drill-close.sh*)
        touch "$AZ_STATE/closed-$vm"
        if [ "${FAKE_CLOSE_FAIL:-}" = "$vm" ]; then
          echo "drill: the drill user still has a key"
          exit 0
        fi
        # An address in the output must come out redacted.
        echo "drill-close: keys=0 agent=inactive sessions=0 node=active peer 10.42.0.5"
        echo "POLY_OK drill-close"
        ;;
      *)
        leader=0
        if [ "$vm" = node-a ] || [ "${FAKE_TWO_LEADERS:-0}" = 1 ]; then leader=1; fi
        printf 'node_id=%s\nservice=active\nreadyz=200\npoly_lease_leader %s\nPOLY_OK status\n' "$id" "$leader"
        ;;
    esac
    ;;
  "network public-ip show"*node-a-ip*) echo 203.0.113.10 ;;
  "network public-ip show"*node-b-ip*) echo 203.0.113.11 ;;
  "network nsg rule create"*)
    name=${a##*-n }
    name=${name%% *}
    touch "$AZ_STATE/rule.$name"
    ;;
  "network nsg rule delete"*)
    name=${a##*-n }
    name=${name%% *}
    [ "${FAKE_RULE_STUCK:-0}" = 1 ] || rm -f "$AZ_STATE/rule.$name"
    ;;
  "network nsg rule list"*"length("*)
    find "$AZ_STATE" -maxdepth 1 -name 'rule.drill-*' | wc -l | tr -d ' '
    ;;
  "network nsg rule list"*)
    for f in "$AZ_STATE"/rule.*; do [ -e "$f" ] && echo "${f##*/rule.}"; done
    ;;
  "storage account list"*) echo polyhafakeaccount7 ;;
  "storage blob download-batch"*)
    dest=${a##*--destination }
    dest=${dest%% *}
    pat=${a##*--pattern }
    pat=${pat%% *}
    mkdir -p "$dest/${pat%/\*}/n1" "$dest/${pat%/\*}/n2"
    ;;
  "storage blob download "*)
    f=${a##*--file }
    echo '{}' >"${f%% *}"
    ;;
  "storage blob list"*) echo "seg/e0000000002/s0000000100.prec.gz" ;;
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
mutating='^(deployment (sub|group|mg|tenant) (create|delete)|group (create|delete)|vm (deallocate|start|stop|delete|create|restart|run-command)|provider register|role assignment|storage|identity|ad |account set|login)'

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

start "destroy refuses a group this project did not tag" env FAKE_GROUP_TAG=other "$here/destroy.sh" --yes
check test "$code" != 0
check no_change
check grep -q "not tagged project=poly-ha" <<<"$out"

start "destroy refuses a group with no tag" env FAKE_GROUP_TAG= "$here/destroy.sh" --yes
check test "$code" != 0
check no_change

# The SSH range: one check, shared by deploy.sh and the workflow.
not_printed() {
  [ -z "$range" ] || ! grep -qF -- "$range" <<<"$out"
}
for range in 203.0.113.7/32 198.51.100.0/24; do
  start "an SSH range of $range is accepted" "$here/check_cidr.sh" "$range"
  check test "$code" = 0
done
for range in 0.0.0.0/0 10.0.0.0/8 203.0.113.0/23 "*" Internet 203.0.113.7 300.0.113.7/32 203.0.113.7/33 "::/0" ""; do
  start "an SSH range of '$range' is refused" "$here/check_cidr.sh" "$range"
  check test "$code" = 1
  # The log of the workflow is public: the range is never printed back.
  check not_printed
done

# vm_run.sh changes a VM, so it holds to the subscription rule too.
start "vm_run refuses another subscription" env FAKE_SUBSCRIPTION=Klimacap "$here/vm_run.sh" node-a status
check test "$code" != 0
check no_change
check grep -q "Not touching it" <<<"$out"

start "vm_run runs one command on the right subscription" "$here/vm_run.sh" node-a status
check test "$code" = 0
check test "$(changed)" = 1
check grep -q "^vm run-command invoke -g poly-ha -n node-a .*polyctl.sh status" "$AZ_LOG"

start "vm_run fails when the command did not finish" env FAKE_RUN_OUTPUT=nothing "$here/vm_run.sh" node-a status
check test "$code" = 1

start "vm_run refuses an argument that is not a plain word" "$here/vm_run.sh" node-a status 'x;reboot'
check test "$code" = 2
check no_change

# The gate of the workflow, against a fake `gh`. It must say ready only for
# an environment whose every deployment rule is an ha-* tag.
cat >"$tmp/bin/gh" <<'STUB'
#!/usr/bin/env bash
case "$2" in
  */deployment-branch-policies*)
    [ -n "${FAKE_POLICIES:-}" ] || exit 1
    printf '%s\n' "$FAKE_POLICIES"
    ;;
  */environments/*)
    case "${FAKE_ENV:-missing}" in
      missing) echo "gh: Not Found (HTTP 404)" >&2; exit 1 ;;
      broken) echo "gh: Server Error (HTTP 502)" >&2; exit 1 ;;
      *) printf '%s\n' "$FAKE_ENV" ;;
    esac
    ;;
esac
STUB
chmod +x "$tmp/bin/gh"
export GITHUB_REPOSITORY=owner/repo GH_TOKEN=none
limited='{"deployment_branch_policy":{"protected_branches":false,"custom_branch_policies":true}}'
policies() {
  local n=$# list
  list=$(printf '%s,' "$@")
  printf '{"total_count":%s,"branch_policies":[%s]}' "${TOTAL:-$n}" "${list%,}"
}
tag='{"name":"ha-*","type":"tag"}'
gate() {
  local want=$1
  shift
  start "gate: $name_for_gate" env "$@" "$here/gate.sh" azure
  check test "$code" = 0
  check grep -qx "reason=$want" <<<"$out"
  if [ "$want" = ok ]; then
    check grep -qx "ready=true" <<<"$out"
  else
    check grep -qx "ready=false" <<<"$out"
  fi
}
name_for_gate="no environment is not ready"
gate missing FAKE_ENV=missing
name_for_gate="an unreadable environment is not ready"
gate unreadable FAKE_ENV=broken
name_for_gate="an environment open to every ref is not ready"
gate open FAKE_ENV='{"deployment_branch_policy":null}'
name_for_gate="protected branches only is not ready"
gate open FAKE_ENV='{"deployment_branch_policy":{"protected_branches":true,"custom_branch_policies":false}}'
name_for_gate="an unreadable rule list is not ready"
gate unreadable FAKE_ENV="$limited"
name_for_gate="an empty rule list is not ready"
gate policy FAKE_ENV="$limited" FAKE_POLICIES="$(policies)"
name_for_gate="a branch rule is not ready"
gate policy FAKE_ENV="$limited" FAKE_POLICIES="$(policies "$tag" '{"name":"main","type":"branch"}')"
name_for_gate="a branch named like the tags is not ready"
gate policy FAKE_ENV="$limited" FAKE_POLICIES="$(policies '{"name":"ha-*","type":"branch"}')"
name_for_gate="a tag rule for every tag is not ready"
gate policy FAKE_ENV="$limited" FAKE_POLICIES="$(policies "$tag" '{"name":"*","type":"tag"}')"
name_for_gate="a rule list cut short is not ready"
gate unreadable FAKE_ENV="$limited" FAKE_POLICIES="$(TOTAL=140 policies "$tag")"
name_for_gate="only ha-* tag rules is ready"
gate ok FAKE_ENV="$limited" FAKE_POLICIES="$(policies "$tag" '{"name":"ha-drill-*","type":"tag"}')"

# The live drill, runner side (ops/azure/drill.sh), against fakes of az,
# ssh, ssh-keygen, curl and chaos. The workflow log is public, so the
# runner's address, the VMs' addresses, the run's key, the VMs' host keys
# and the storage account name must each be masked and never printed.
cat >"$tmp/bin/ssh-keygen" <<'STUB'
#!/usr/bin/env bash
while [ $# -gt 0 ]; do
  [ "$1" = -f ] && f=$2
  shift
done
echo "FAKE PRIVATE KEY" >"$f"
echo "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFakeRunKey+With/Slashes0123456789abcdefgh drill" >"$f.pub"
STUB
cat >"$tmp/bin/ssh" <<'STUB'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$AZ_STATE/ssh.log"
# What a real ssh might say on stderr. It must stay in a private file.
echo "ssh: connect to host 203.0.113.10 port 22: Connection timed out" >&2
# Keep the arguments in the process title, as a real ssh has them.
exec -a "ssh $*" sleep 300
STUB
cat >"$tmp/bin/curl" <<'STUB'
#!/usr/bin/env bash
case "$*" in
  *ipify* | *checkip*) echo "${FAKE_RUNNER_IP-198.51.100.77}" ;;
  *127.0.0.1:*/healthz*) printf '%s' "${FAKE_HEALTH:-200}" ;;
  *) exit 7 ;;
esac
STUB
cat >"$tmp/bin/chaos" <<'STUB'
#!/usr/bin/env bash
cmd=$1
shift
while [ $# -gt 0 ]; do
  [ "$1" = --run ] && run=$2
  shift
done
case "$cmd" in
  drill)
    mkdir -p "$run"
    echo '{"start_ms":1763000000000,"end_ms":1763000600000}' >"$run/times.json"
    echo "chaos drill: faults 4/4 invalid=false via ws://127.0.0.1:19100"
    ;;
  verify-drill)
    cat >"$run/report.json" <<'JSON'
{"verdict":{"result":"pass","reason":null,"failures":[]},"control_missing":12,
 "content":{"compared":[1,2,3],"mismatch":[0,0,0]},
 "stream":{"faults":{"executed":4,"planned":4,"kill9":3,"freeze_leader":1},"truth_keys_in_window":100,
  "missing":0,"duplicates":0,"extra":0,"late":{"lost":0},"added_delay_ms":{"p50":1,"p99":2,"max":3},
  "rehedge_ms":{"p50":10,"max":20},"counters":{"seq_gaps":0,"resume_violations":0,"unkeyed":0},
  "binance_agg_id_gaps":0},
 "archive":{"takeovers_seen":1,"leader_freezes":1,"takeover_ms":{"p50":36000,"max":36000},"fence_rejected":1,
  "stale_epoch_commits":0,"epoch_two_holders":0,"view":{"holes":0,"orphans":0}}}
JSON
    echo "chaos drill: ${FAKE_VERDICT:-pass} reason=-"
    exit "${FAKE_VERIFY_CODE:-0}"
    ;;
esac
STUB
chmod +x "$tmp/bin/ssh-keygen" "$tmp/bin/ssh" "$tmp/bin/curl" "$tmp/bin/chaos"
export CHAOS="$tmp/bin/chaos" DRILL_SLEEP=0 GITHUB_ACTIONS=true
secrets=(198.51.100.77 203.0.113.10 203.0.113.11 10.42.0.5
  "AAAAC3NzaC1lZDI1NTE5AAAAIFakeRunKey+With/Slashes0123456789abcdefgh"
  "AAAAC3NzaC1lZDI1NTE5AAAAIFakeRunKey-With_Slashes0123456789abcdefgh"
  AAAAC3NzaC1lZDI1NTE5HostKeyOfnode-a AAAAC3NzaC1lZDI1NTE5HostKeyOfnode-b
  polyhafakeaccount7)

# Run drill steps in sequence on one state. Sets $code to the last step's
# exit code and appends every line any step printed to $all.
drill_case() {
  name=$1
  shift
  rm -rf "$AZ_STATE" "$tmp/drill" "$tmp/report"
  mkdir "$AZ_STATE"
  : >"$AZ_LOG"
  all="$tmp/all.out"
  : >"$all"
  export DRILL_DIR="$tmp/drill" DRILL_OUT="$tmp/report" DRILL_RUN=12345-1
  export GITHUB_STEP_SUMMARY="$tmp/summary.md"
  : >"$GITHUB_STEP_SUMMARY"
}
drill_step() {
  set +e
  "$@" >>"$all" 2>&1
  code=$?
  set -e
}
# No secret on any printed line but the mask commands, which the runner
# hides, and none in the job summary.
no_leak() {
  local v
  for v in "${secrets[@]}"; do
    if grep -v '^::add-mask::' "$all" | grep -qF -- "$v"; then
      echo "      leaked: $v"
      return 1
    fi
    if grep -qF -- "$v" "$GITHUB_STEP_SUMMARY"; then
      echo "      leaked into the summary: $v"
      return 1
    fi
  done
}
masked() {
  grep -qxF -- "::add-mask::$1" "$all"
}
rules_left() {
  find "$AZ_STATE" -maxdepth 1 -name 'rule.*' | wc -l | tr -d ' '
}

drill_case "a whole drill"
touch "$AZ_STATE/rule.drill-999-1"
drill_step env DRILL_RUN=12345-1 "$here/drill.sh" ready
check test "$code" = 0
drill_step "$here/drill.sh" open
name="drill open succeeds"
check test "$code" = 0
name="drill open removes a rule an earlier run left"
check test ! -e "$AZ_STATE/rule.drill-999-1"
name="drill open adds a rule for the runner's address, SSH only"
check grep -q "^network nsg rule create -g poly-ha --nsg-name poly-ha-nsg -n drill-12345-1 --priority 110 .*--source-address-prefixes 198.51.100.77/32 .*--destination-port-ranges 22 " "$AZ_LOG"
name="drill open puts the key on both VMs through the agent, after their tools digest"
check grep -q "drill-open.sh 12345-1 AAAAC3NzaC1lZDI1NTE5AAAAIFakeRunKey-With_Slashes0123456789abcdefgh [0-9a-f]\{64\}" "$AZ_LOG"
check test "$(grep -c 'drill-open.sh' "$AZ_LOG")" = 2
for _ in $(seq 1 50); do
  [ -s "$AZ_STATE/ssh.log" ] && [ "$(wc -l <"$AZ_STATE/ssh.log")" -ge 2 ] && break
  sleep 0.1
done
name="drill open pins each host key and forwards only to loopback"
check grep -q "^203.0.113.10 ssh-ed25519 AAAAC3NzaC1lZDI1NTE5HostKeyOfnode-a$" "$tmp/drill/known_hosts"
check grep -q "StrictHostKeyChecking=yes .*-L 127.0.0.1:19100:127.0.0.1:9100 -L 127.0.0.1:19101:127.0.0.1:9101 -L 127.0.0.1:19102:127.0.0.1:9102 drill@203.0.113.10" "$AZ_STATE/ssh.log"
drill_step "$here/drill.sh" run
name="drill run succeeds"
check test "$code" = 0
drill_step "$here/drill.sh" collect
name="drill collect judges and keeps report.json for the artifact"
check test "$code" = 0
check test -s "$tmp/report/report.json"
check grep -q "^### Drill pass" "$GITHUB_STEP_SUMMARY"
check test "$(grep -c 'drill-collect.sh 12345-1 1763000000000 1763000600000' "$AZ_LOG")" = 2
drill_step "$here/drill.sh" close
name="drill close removes the rule, closes both VMs and finds them healthy"
check test "$code" = 0
check test "$(rules_left)" = 0
check test -e "$AZ_STATE/closed-node-a"
check test -e "$AZ_STATE/closed-node-b"
check test ! -e "$tmp/drill"
check test -s "$tmp/report/report.json"
name="drill close ends the forwards"
check test -z "$(pgrep -f "UserKnownHostsFile=$tmp/drill/known_hosts" || true)"
name="no address, key or account name reaches the log"
check no_leak
for v in "${secrets[@]}"; do
  [ "$v" = 10.42.0.5 ] && continue
  name="masked: $v"
  check masked "$v"
done

drill_case "an open that fails on node-b"
drill_step env FAKE_OPEN_FAIL=node-b "$here/drill.sh" open
name="drill open fails when one VM refuses"
check test "$code" != 0
drill_step "$here/drill.sh" close
name="close after a failed open still removes the rule and closes both VMs"
check test "$code" = 0
check test "$(rules_left)" = 0
check test -e "$AZ_STATE/closed-node-a"
check test -e "$AZ_STATE/closed-node-b"
name="no secret leaks from a failed open"
check no_leak

drill_case "a rule that will not go"
drill_step "$here/drill.sh" open
drill_step env FAKE_RULE_STUCK=1 "$here/drill.sh" close
name="close fails loudly when the rule is still there"
check test "$code" != 0
check grep -q "::error::a drill SSH rule is still in the security group" "$all"
check test -e "$AZ_STATE/closed-node-b"

drill_case "a VM whose close does not finish"
drill_step "$here/drill.sh" open
drill_step env FAKE_CLOSE_FAIL=node-a "$here/drill.sh" close
name="close fails loudly when a VM's close does not finish, and still closes the other"
check test "$code" != 0
check grep -q "::error::drill-close did not finish on node-a" "$all"
check test -e "$AZ_STATE/closed-node-b"
check test "$(rules_left)" = 0

drill_case "two leaders after the drill"
drill_step env FAKE_TWO_LEADERS=1 DRILL_HEALTH_TRIES=2 "$here/drill.sh" close
name="close fails loudly unless exactly one node leads"
check test "$code" != 0
check grep -q "::error::after the drill the nodes are not both running and ready with exactly one leader" "$all"

drill_case "a failing verdict"
drill_step "$here/drill.sh" open
drill_step "$here/drill.sh" run
drill_step env FAKE_VERIFY_CODE=1 FAKE_VERDICT=fail "$here/drill.sh" collect
name="collect passes the verdict's exit code on, and still keeps the report"
check test "$code" = 1
check test -s "$tmp/report/report.json"
drill_step "$here/drill.sh" close
check test "$code" = 0

drill_case "another subscription"
drill_step env FAKE_SUBSCRIPTION=Klimacap "$here/drill.sh" open
name="drill open refuses another subscription and changes nothing"
check test "$code" != 0
check no_change
check test "$(rules_left)" = 0

drill_case "no runner address"
drill_step env FAKE_RUNNER_IP= "$here/drill.sh" open
name="drill open stops before any rule when the runner's address is unknown"
check test "$code" != 0
check test "$(rules_left)" = 0

# The drill tools bundle.
start "vm_tools digest is stable" "$here/vm_tools.sh" digest
check test "$code" = 0
check grep -qx '[0-9a-f]\{64\}' <<<"$out"
d1=$out
start "vm_tools digest again" "$here/vm_tools.sh" digest
check test "$out" = "$d1"
start "vm_tools install ships the bundle and checks the digest the VM reports" "$here/vm_tools.sh" install node-a
check test "$code" = 0
check test "$(changed)" = 1
check grep -q "^vm run-command invoke -g poly-ha -n node-a .*--scripts @" "$AZ_LOG"
start "vm_tools install fails when the VM reports another digest" env FAKE_TOOLS_DIGEST=0000 "$here/vm_tools.sh" install node-b
check test "$code" != 0
start "vm_tools install refuses another subscription" env FAKE_SUBSCRIPTION=Klimacap "$here/vm_tools.sh" install node-a
check test "$code" != 0
check no_change
start "vm_run runs the drill script for a drill command" env FAKE_RUN_OUTPUT="POLY_OK drill-close" "$here/vm_run.sh" node-a drill-close 12345-1
check test "$code" = 0
check grep -q "^vm run-command invoke -g poly-ha -n node-a .*--scripts /opt/poly/drill/drill-close.sh 12345-1 " "$AZ_LOG"

# Lint: no drill script traces its commands or looks up an address.
name="the drill scripts never trace or look up an address"
check test -z "$(grep -nE 'set -x|set -o xtrace|hostname -I|ifconfig|ip (-[46] )?a(ddr)?( |$)|metadata/instance' \
  "$here/drill.sh" "$here/vm_tools.sh" "$here/../../deploy/azure/drill/"* || true)"
name="the drill agent compiles"
check python3 -c 'import ast, sys; ast.parse(open(sys.argv[1]).read())' "$here/../../deploy/azure/drill/drill-agent.py"

if [ "$fails" -gt 0 ]; then
  echo "$fails checks failed"
  exit 1
fi
echo "all checks passed"
