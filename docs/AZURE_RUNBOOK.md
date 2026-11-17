# Azure runbook: the two-node setup

How to deploy, check, pause and delete the two Poly HA nodes on Azure. The design is in [HA_BUILD_SPEC_B_C_AZURE.md](HA_BUILD_SPEC_B_C_AZURE.md), sections 4 and 7.

**State on 18 November 2026: deployed.** The bootstrap was run from the owner's machine on 17 November, and the tags `ha-infra-3` and `ha-deploy-1` created the network and the two VMs and installed the node. No drill has run yet (section 7a). Every step below that creates something is the owner's to run.

## 1. What gets created

One resource group, `poly-ha`, in `eastasia`. Everything is inside it and every resource that can carry a tag has `project=poly-ha`.

| # | Resource | Name | From |
|---|---|---|---|
| 1 | Resource group | `poly-ha` | bootstrap |
| 2 | User-assigned identity, node-a | `poly-ha-node-a` | bootstrap |
| 3 | User-assigned identity, node-b | `poly-ha-node-b` | bootstrap |
| 4 | User-assigned identity, GitHub Actions | `poly-ha-deployer` | bootstrap |
| 5 | Federated credential on 4 | `github-azure`, subject `repo:<owner>/<repo>:environment:azure` | bootstrap |
| 6 | Storage account, StorageV2, Standard LRS, Hot | `polyha<13 characters>` | bootstrap |
| 7 | Blob container | `lease` (holds `HEAD.json`) | bootstrap |
| 8 | Blob container | `archive` (`seg/`, `drill/`, `selftest/`, `releases/`) | bootstrap |
| 9 | Lifecycle policy on 6 | `seg/`, `drill/`, `selftest/` deleted after 2 days, `releases/` after 14, `lease` untouched | bootstrap |
| 10 | Role assignment, Storage Blob Data Contributor on 6 | to identity 2 | bootstrap |
| 11 | Role assignment, Storage Blob Data Contributor on 6 | to identity 3 | bootstrap |
| 12 | Role assignment, Storage Blob Data Contributor on 6 | to identity 4 (uploads node builds) | bootstrap |
| 13 | Role assignment, Contributor on the resource group | to identity 4 | bootstrap |
| 14 | Budget, 20 a month, resource group scope | `poly-ha-monthly` | bootstrap |
| 15 | Network security group | `poly-ha-nsg` | nodes |
| 16 | Virtual network, one subnet | `poly-ha-vnet`, `nodes` 10.42.0.0/27 | nodes |
| 17, 18 | Public IPv4, Standard, static, zonal | `node-a-ip`, `node-b-ip` | nodes |
| 19, 20 | Network interface | `node-a-nic`, `node-b-nic` | nodes |
| 21 | VM, Standard_B2ats_v2, x64, zone 2, Ubuntu 24.04 | `node-a` | nodes |
| 22 | VM, Standard_B2pts_v2, Arm64, zone 1, Ubuntu 24.04 | `node-b` | nodes |
| 23, 24 | OS disk, 30 GB Standard HDD | `node-a-os`, `node-b-os` | nodes |

"bootstrap" is `deploy/azure/bootstrap.bicep` (subscription scope, run from the owner's machine). "nodes" is `deploy/azure/nodes.bicep` (resource group scope, run by the workflow or from the owner's machine).

Azure also keeps two deployment records, `poly-ha-bootstrap` at the subscription and `poly-ha-nodes` in the group. They hold no resource and cost nothing.

What the network lets in: TCP 22 from the one address range you give, and nothing else (a drill adds a rule for its runner's address while it runs, section 7a). A deny rule at priority 4096 overrides Azure's default rules that would let the virtual network and the load balancer in. The stream (`127.0.0.1:9100`) and the health endpoints (`127.0.0.1:9101`) bind loopback on each VM and are reached through an SSH forward. Password login is off, at Azure and again in `sshd`. The storage account refuses account keys, so there is no connection string for it anywhere.

## 2. Monthly cost at eastasia prices

Prices are from the Azure Retail Prices API, `https://prices.azure.com/api/retail/prices`, queried on 17 November 2026 with `armRegionName eq 'eastasia'`, pay-as-you-go, US dollars. The API is described at `https://learn.microsoft.com/rest/api/cost-management/retail-prices/azure-retail-prices`. The same list prices are published for people on `azure.microsoft.com/pricing/details/` under `virtual-machines/linux`, `managed-disks`, `ip-addresses` and `storage/blobs`. Those four pages were not opened for this check; the API was.

A month is 730 hours.

| Resource | Meter and unit price | Per month |
|---|---|---|
| VM node-a | B2ats v2, $0.0131 an hour | $9.56, or $0 if the free hours apply |
| VM node-b | B2pts v2, $0.0116 an hour | $8.47, or $0 if the free hours apply |
| 2 OS disks | S4 LRS, $1.536 a month each | $3.07 |
| Disk operations | S4 LRS, $0.0005 per 10,000 | $0.10 to $0.50 (estimate) |
| 2 public IPs | Standard IPv4 static, $0.005 an hour each | $7.30 |
| Blob writes | Hot LRS, $0.05 per 10,000. About 302,000: a renew or commit every 10 s and one upload a minute | $1.51 |
| Blob reads | Hot, $0.004 per 10,000. About 518,000 follower polls | $0.21 |
| Blob data | Hot LRS, $0.024 per GB a month, 2 days kept | $0.35 to $1.10 (estimate, from 5 to 15 GB a day compressed) |
| Resource group, identities, federated credential, role assignments, virtual network, security group, network interfaces, lifecycle policy, budget | no meter | $0 |
| Egress | first 100 GB a month free | $0 |

| Case | Per month |
|---|---|
| Always on, free VM hours apply | about $13 (range $12 to $15) |
| Always on, free VM hours do not apply | about $31 |
| Both VMs deallocated between drills | about $11 |

Azure for Students lists 750 free hours a month each of B2ats v2 and B2pts v2. Nobody has yet seen those meters bill zero on this subscription. Section 7 is how to find out.

A deallocated VM stops its compute meter only. Disks, public IPs and storage keep billing until the group is deleted.

## 3. What the owner does himself

Nothing here was done for you, because each step creates something or needs a choice.

1. **Register four resource providers.** On 17 November `Microsoft.Compute`, `Microsoft.Network`, `Microsoft.Storage` and `Microsoft.ManagedIdentity` were `NotRegistered` on the subscription. `Microsoft.Consumption`, `Microsoft.CostManagement`, `Microsoft.Authorization` and `Microsoft.Resources` were `Registered`.
2. **Choose the budget email.** The address that gets a mail at 50, 80 and 100 percent of 20 a month, and when the forecast passes 100 percent. The budget only sends mail. It stops nothing.
3. **Choose the SSH source range.** Your own public address as `/32`. Both ways of deploying refuse anything wider than `/24`: `deploy.sh` and the `ha-infra` job run the same check, `ops/azure/check_cidr.sh`.
4. **Choose the SSH public key.** An `ssh-ed25519` or `ssh-rsa` public key file. It becomes the only login on both VMs.
5. **Create the GitHub environment `azure`** and limit it to `ha-*` tags. Until it exists, and while its rules allow anything but `ha-*` tags, the workflow does nothing.
6. **Run the bootstrap.** It creates the group, the identities, the federated credential, the storage account, the roles and the budget. It needs Owner rights on the subscription, which the workflow's identity does not have.
7. **Set five variables** in the environment, three of them printed by the bootstrap.
8. **Push the tags** that create the VMs and install the node.
9. **Read the bill after 72 hours** and decide always-on or drill-only.

The order of steps 5 and 6 matters. The federated credential made in step 6 is the only link between GitHub and Azure. It names this repository and the environment `azure`, so the tag limit of step 5 is what decides who can deploy. A workflow that names an environment which does not exist creates it with no limit, so the environment and its limit must be there before the credential is.

## 4. Deploy, in order

Run from the repository root on branch `ha-failover`. Replace the four values in capitals.

```bash
# 0. The right subscription. Must print: Azure for Students
az account show --query name -o tsv

# 1. Register the providers (once, a few minutes each)
az provider register --namespace Microsoft.Compute --wait
az provider register --namespace Microsoft.Network --wait
az provider register --namespace Microsoft.Storage --wait
az provider register --namespace Microsoft.ManagedIdentity --wait

# 2. Quotas, readable only after step 1. Needs 4 vCPUs (2 Basv2, 2 Bpsv2) and 2 public IPs.
az vm list-usage -l eastasia -o table
az network list-usages -l eastasia -o table

# 3. See the plan. Changes nothing. --what-if also asks Azure what the bootstrap would create.
ops/azure/deploy.sh --github-repo arnav-goel10/polymarket-pricing-research \
  --budget-email YOU@EXAMPLE.COM --bootstrap-only --what-if

# 4. The GitHub environment, limited to ha-* tags. Before the bootstrap, so the
#    credential it creates never points at an environment anyone can use.
gh api -X PUT repos/arnav-goel10/polymarket-pricing-research/environments/azure \
  -F "deployment_branch_policy[protected_branches]=false" \
  -F "deployment_branch_policy[custom_branch_policies]=true"
gh api -X POST repos/arnav-goel10/polymarket-pricing-research/environments/azure/deployment-branch-policies \
  -f name='ha-*' -f type=tag
# Must print exactly one line: ha-* tag
gh api repos/arnav-goel10/polymarket-pricing-research/environments/azure/deployment-branch-policies \
  --jq '.branch_policies[] | "\(.name) \(.type)"'

# 5. Bootstrap: group, identities, storage, roles, budget. No VM yet.
ops/azure/deploy.sh --github-repo arnav-goel10/polymarket-pricing-research \
  --budget-email YOU@EXAMPLE.COM --bootstrap-only --yes
```

If the workflow's Azure login fails with `AADSTS700213` and the log shows a subject like `repo:OWNER@123/REPO@456:environment:azure`, GitHub is sending the subject with immutable ids. Run step 5 again with `--github-subject-repo 'OWNER@123/REPO@456'`, copied from that log line. This happened on the first deploy, 17 November 2026.

Step 5 ends by printing `AZURE_CLIENT_ID`, `AZURE_TENANT_ID` and `AZURE_SUBSCRIPTION_ID`. If Azure refuses the budget (a new subscription can wait up to 48 hours, and the Students offer may not allow one), run step 5 again with `--no-budget` and add the budget later by running it once more without that flag.

```bash
# 6. The environment's variables. None is a secret: the ids open nothing without GitHub's own token.
gh variable set AZURE_CLIENT_ID --env azure --body "VALUE_FROM_STEP_5"
gh variable set AZURE_TENANT_ID --env azure --body "VALUE_FROM_STEP_5"
gh variable set AZURE_SUBSCRIPTION_ID --env azure --body "VALUE_FROM_STEP_5"
gh variable set SSH_SOURCE_CIDR --env azure --body "A.B.C.D/32"
gh variable set SSH_PUBLIC_KEY --env azure < ~/.ssh/id_ed25519.pub

# 7. Create the network and the two VMs (about 5 minutes)
git tag ha-infra-1 && git push origin ha-infra-1

# 8. Build the node for both architectures, install it, check it (about 30 minutes)
git tag ha-deploy-1 && git push origin ha-deploy-1

# 9. Look
gh run list --event push --limit 5      # the runs named Azure
ops/azure/status.sh
```

Each tag is used once. The next deploy is `ha-deploy-2`, and so on.

Every run starts with a `gate` job that reads the environment and its rules. If the environment is missing, lets any ref deploy, or has a rule that is not an `ha-*` tag, the run says so in a notice and does nothing else. The `ha-infra` job then checks `SSH_SOURCE_CIDR` before it asks Azure anything, and fails on a range wider than `/24`.

The `ha-deploy` run passes only when both nodes answer `/readyz` with all three venue feeds connected, the store contract suite passes against the real account from each VM, exactly one node leads, and at least one slot has been committed.

Step 7 can also be done from your machine with no tag, if the workflow is not wanted for it:

```bash
ops/azure/deploy.sh --github-repo arnav-goel10/polymarket-pricing-research \
  --budget-email YOU@EXAMPLE.COM --ssh-cidr A.B.C.D/32 \
  --ssh-key-file ~/.ssh/id_ed25519.pub --yes
```

## 5. Reach a node

```bash
# Addresses
az vm list -g poly-ha -d --query "[].{name:name, ip:publicIps}" -o table

# State of the service, health answers and lease counters
ssh azureuser@NODE_IP sudo /opt/poly/polyctl.sh status

# The stream, on your own loopback while this runs
ssh -N -L 9100:127.0.0.1:9100 azureuser@NODE_IP
```

If your address changed, SSH times out. Set the new range in the `SSH_SOURCE_CIDR` variable and push the next `ha-infra-N` tag, or run the `deploy.sh` line at the end of section 4 with the new `--ssh-cidr`.

On each VM the node runs as `poly-node.service` under user `poly`: `Restart=always`, `RestartSec=1`, `MemoryMax=600M`. It stays inert until a build is installed. `poly-health.timer` asks `/healthz` every 30 s and restarts the service after five checks in a row with no answer. A venue being down (`/readyz` 503) never restarts it.

## 6. Pause and resume

Every script prints what it will do and changes nothing without `--yes`. Each refuses to run on any subscription other than Azure for Students. `ops/azure/vm_run.sh`, which the deploy job uses to run a command on a VM, has no dry run but holds to the same subscription rule.

```bash
ops/azure/stop.sh            # the plan
ops/azure/stop.sh --yes      # deallocate both VMs. Compute stops billing
ops/azure/start.sh --yes     # start both. Same addresses, same disks
ops/azure/status.sh          # read-only
```

Or by tag: `ha-stop-N` and `ha-start-N`.

## 7a. Run a drill

The live drill is described in [HA_DRILL.md](HA_DRILL.md): what it does, what it measures and what a pass means. It runs from a tag, like everything else here.

**Any change under `deploy/azure/drill/` needs a new `ha-deploy` tag on the same commit before the next drill tag.** The deploy job is what installs those files on the VMs, and the drill checks their digest against its own commit and stops before it opens anything if they differ. The same holds for a drill tag on a commit that has never had a deploy tag.

```bash
# The VMs must carry this commit's drill tools, which the deploy job installs.
git tag ha-deploy-2 && git push origin ha-deploy-2              # wait for it to pass
git tag ha-drill-rehearsal && git push origin ha-drill-rehearsal  # 4 faults, about 20 minutes
# Read the run's public log and its report.json artifact. Then:
git tag ha-drill-1 && git push origin ha-drill-1                # 20 faults, about 50 minutes
```

The drill job records each VM's power state, then starts the ones that are deallocated, so it also works in drill-only mode. Its very last step puts them back: a VM that was deallocated is deallocated again, and a VM that was running (or stopped without being deallocated, or whose state could not be read, with a warning) is left running. So a drill in drill-only mode costs only its own hours.

While it runs it adds one security group rule, `drill-<run id>`, that lets SSH in from the runner's address only, and a key for the user `drill` on both VMs that can only forward to the node's loopback ports. `drill-open.sh` also installs `/etc/ssh/sshd_config.d/20-poly-drill.conf`, a `Match User drill` block that holds that user to local TCP forwards to the three loopback ports, with no Unix socket, agent or X11 forward and no listener, whatever a key says. It checks the whole sshd config with `sshd -t` before it reloads and takes the file out again if the check fails, so a bad file cannot lock SSH out. The file stays between drills. It is not in the base `sshd-poly.conf`, because that is cloud-init custom data, which Azure will not change on a running VM.

The cleanup removes every `drill-*` rule and the key, also after a failure or a cancel, trying the rule delete and the check up to five times, and fails the run unless both are gone and both nodes run with exactly one leader. The job signs in to Azure again before it collects and before it cleans up, because a storage token first asked for long after the sign-in would be refused. The fault and collect steps have their own time limits (15 minutes for a rehearsal's faults, 45 for a full drill's, 20 for collect), so a hung step cannot eat the cleanup's time. To check by hand afterwards:

```bash
az network nsg rule list -g poly-ha --nsg-name poly-ha-nsg --query "[].name" -o tsv   # no drill- name
ops/azure/status.sh
```

If a runner was lost before its cleanup, the next drill removes any `drill-*` rule before it adds its own, the key expires four hours after it was set, and `az network nsg rule delete -g poly-ha --nsg-name poly-ha-nsg -n drill-<run id>` removes the rule at once.

The recordings a drill collects stay in the archive container under `drill/<run id>/` for two days. Only `report.json` leaves the runner.

## 7. Check the bill after 72 hours

Cost data is 8 to 24 hours behind use, so the first full picture is on day 3.

```bash
ops/azure/cost.sh --days 3
```

It prints the actual cost of the group by meter, the total and the budget. Read four things.

1. The two VM meters, `B2ats v2` and `B2pts v2`. A quantity near 72 hours each with a cost of 0 means the free hours apply. A cost near $0.94 and $0.84 means they do not.
2. The total against section 2. Three days always-on should be about $1.30 with free hours and about $3.10 without.
3. The `Hot LRS Data Stored` quantity. If the archive grows by more than 15 GB a day, lower `archiveRetentionDays` to 1.
4. The `S4 LRS Disk Operations` meter. The estimate in section 2 is $0.10 to $0.50 a month, so three days should be a cent or two. This is the one number in the table with a high ceiling: a disk written at its 500 operations a second limit all month would cost about $66. More than $0.10 in three days means the node writes to disk far more often than once a second, and that needs a look before leaving it on.

The same numbers are in the portal under Cost Management, Cost analysis, scope `poly-ha`, grouped by Meter. The credit that is left is at `https://www.microsoftazuresponsorships.com/Balance`.

Then choose. Free hours apply: leave it on. They do not: run `ops/azure/stop.sh --yes` and start the VMs only for drills.

## 8. Tear down

```bash
ops/azure/destroy.sh         # lists what will go
ops/azure/destroy.sh --yes   # deletes the group, waits, then checks nothing tagged project=poly-ha is left
```

The script deletes only a group tagged `project=poly-ha`, which the bootstrap sets. It refuses any other group, whatever `POLY_RG` names.

The one command underneath is `az group delete -n poly-ha --yes`. It removes every row of the table in section 1, the archive included, and cannot be undone. The cost goes to $0. The tag `ha-destroy-N` does the same from the workflow without waiting.

To check by hand that nothing is left:

```bash
az group exists -n poly-ha                          # false
az resource list --tag project=poly-ha -o table     # empty
```

What stays, and costs nothing: the registered providers, and the GitHub environment. Remove the environment with

```bash
gh api -X DELETE repos/arnav-goel10/polymarket-pricing-research/environments/azure
```

After a teardown the storage account name is the same on the next deploy, because it is derived from the subscription and the group. The identities get new ids, so step 6 of section 4 must be run again with the new `AZURE_CLIENT_ID`.

## 9. How this was checked without deploying

All read-only, on 17 November 2026.

| Check | Result |
|---|---|
| `az bicep build` on both templates | compiled, no warning |
| `az deployment sub what-if` on the bootstrap template with placeholder parameters | 15 resources to create, nothing created; the group still did not exist afterwards and the providers were still unregistered |
| `az vm list-skus -l eastasia` | `Standard_B2ats_v2` x64, zones 1, 2, 3, no restriction. `Standard_B2pts_v2` Arm64, zone 1 only, no restriction |
| `az vm image list-skus -p Canonical -f ubuntu-24_04-lts` | `server` and `server-arm64` exist in eastasia |
| `az provider show` | four providers unregistered, listed in section 3 |
| `az policy assignment list` | one assignment, the allowed regions |
| Quotas | not readable before the providers are registered (step 2 of section 4) |

Not checked, because it needs the group to exist: a what-if of `nodes.bicep`. The `ha-infra` job runs one before it creates anything. Not proven until the first deploy: that the budget is accepted on this offer, that the free VM hours bill zero, and that Binance and Polymarket accept connections from Hong Kong cloud addresses.

In CI, the `azure-deploy-code` job compiles both templates, checks the compiled JSON against the promises in sections 1 and 5 (`deploy/azure/check_templates.py`), lints the scripts and the workflow, and runs every ops script against a fake `az` to show that none changes anything without `--yes`. The same test runs the SSH range check on good and bad ranges, and the gate against a fake `gh` for a missing, open, wrongly limited and correctly limited environment. The `azurite` job runs the Blob store against the Azurite emulator.
