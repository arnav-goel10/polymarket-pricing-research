#!/usr/bin/env python3
"""Check the compiled templates against what docs/AZURE_RUNBOOK.md promises.

    az bicep build --file deploy/azure/bootstrap.bicep --outdir OUT
    az bicep build --file deploy/azure/nodes.bicep --outdir OUT
    python3 deploy/azure/check_templates.py OUT/bootstrap.json OUT/nodes.json

It reads the two JSON files and nothing else. Each check names the promise
it stands for, and the script exits 1 if any fails.
"""

import json
import re
import sys

# Resource types that cannot carry a tag. They live inside the tagged group
# or under a tagged parent and go when it goes.
UNTAGGED = {
    "Microsoft.Resources/deployments",
    "Microsoft.ManagedIdentity/userAssignedIdentities/federatedIdentityCredentials",
    "Microsoft.Storage/storageAccounts/blobServices",
    "Microsoft.Storage/storageAccounts/blobServices/containers",
    "Microsoft.Storage/storageAccounts/managementPolicies",
    "Microsoft.Authorization/roleAssignments",
    "Microsoft.Consumption/budgets",
}

# Every resource type the two templates may create, with how many
# declarations of it there are (a loop over the two nodes counts once).
EXPECTED = {
    "Microsoft.Resources/resourceGroups": 1,
    "Microsoft.Resources/deployments": 1,
    "Microsoft.ManagedIdentity/userAssignedIdentities": 2,
    "Microsoft.ManagedIdentity/userAssignedIdentities/federatedIdentityCredentials": 1,
    "Microsoft.Storage/storageAccounts": 1,
    "Microsoft.Storage/storageAccounts/blobServices": 1,
    "Microsoft.Storage/storageAccounts/blobServices/containers": 2,
    "Microsoft.Storage/storageAccounts/managementPolicies": 1,
    "Microsoft.Authorization/roleAssignments": 3,
    "Microsoft.Consumption/budgets": 1,
    "Microsoft.Network/networkSecurityGroups": 1,
    "Microsoft.Network/virtualNetworks": 1,
    "Microsoft.Network/publicIPAddresses": 1,
    "Microsoft.Network/networkInterfaces": 1,
    "Microsoft.Compute/virtualMachines": 1,
}

failures = []


def check(promise, ok):
    print(("ok    " if ok else "FAIL  ") + promise)
    if not ok:
        failures.append(promise)


def flatten(template):
    out = []
    for r in template["resources"]:
        out.append(r)
        if r["type"] == "Microsoft.Resources/deployments":
            out.extend(flatten(r["properties"]["template"]))
    return out


def of(resources, kind):
    return [r for r in resources if r["type"] == kind]


def default(template, name):
    return template["parameters"][name].get("defaultValue")


def main():
    bootstrap = json.load(open(sys.argv[1]))
    nodes = json.load(open(sys.argv[2]))
    base = of(bootstrap["resources"], "Microsoft.Resources/deployments")[0]["properties"]["template"]
    every = flatten(bootstrap) + flatten(nodes)

    counts = {}
    for r in every:
        counts[r["type"]] = counts.get(r["type"], 0) + 1
    check("the templates declare exactly the listed resource types", counts == EXPECTED)

    check(
        "every resource that can carry a tag carries the project tag parameter",
        all("tags" in r for r in every if r["type"] not in UNTAGGED)
        and default(bootstrap, "tags") == {"project": "poly-ha"}
        and default(nodes, "tags") == {"project": "poly-ha"},
    )
    check("the region defaults to eastasia", default(bootstrap, "location") == "eastasia")

    # Network.
    nsg = of(every, "Microsoft.Network/networkSecurityGroups")[0]
    rules = [r["properties"] for r in nsg["properties"]["securityRules"]]
    allow = [r for r in rules if r["direction"] == "Inbound" and r["access"] == "Allow"]
    deny = [r for r in rules if r["direction"] == "Inbound" and r["access"] == "Deny"]
    check(
        "the only inbound allow is tcp/22 from the sshSourceCidr parameter",
        len(allow) == 1
        and allow[0]["protocol"] == "Tcp"
        and allow[0]["destinationPortRange"] == "22"
        and allow[0]["sourceAddressPrefix"] == "[parameters('sshSourceCidr')]",
    )
    check(
        "all other inbound is denied, after the allow and before the default rules",
        len(deny) == 1
        and deny[0]["protocol"] == "*"
        and deny[0]["sourceAddressPrefix"] == "*"
        and deny[0]["destinationPortRange"] == "*"
        and allow[0]["priority"] < deny[0]["priority"] <= 4096,
    )
    check("sshSourceCidr has no default", "defaultValue" not in nodes["parameters"]["sshSourceCidr"])
    # 0.0.0.0/0 is 9 characters, * is 1 and Internet is 8.
    check(
        "sshSourceCidr cannot be as short as 0.0.0.0/0, * or Internet",
        nodes["parameters"]["sshSourceCidr"].get("minLength", 0) >= 10,
    )
    vnet = of(every, "Microsoft.Network/virtualNetworks")[0]
    check(
        "the one subnet is behind that security group",
        len(vnet["properties"]["subnets"]) == 1
        and "networkSecurityGroup" in vnet["properties"]["subnets"][0]["properties"],
    )
    ip = of(every, "Microsoft.Network/publicIPAddresses")[0]
    check(
        "two static Standard public IPv4 addresses",
        ip["copy"]["count"] == "[length(variables('nodes'))]"
        and ip["sku"]["name"] == "Standard"
        and ip["properties"]["publicIPAllocationMethod"] == "Static",
    )

    # VMs.
    sizes = [(n["name"], n["size"], n["zone"], n["imageSku"]) for n in nodes["variables"]["nodes"]]
    check(
        "node-a is B2ats_v2 x64 in zone 2 and node-b is B2pts_v2 Arm64 in zone 1",
        sizes
        == [
            ("node-a", "Standard_B2ats_v2", "2", "server"),
            ("node-b", "Standard_B2pts_v2", "1", "server-arm64"),
        ],
    )
    vm = of(every, "Microsoft.Compute/virtualMachines")[0]
    os_profile = vm["properties"]["osProfile"]
    linux = os_profile["linuxConfiguration"]
    check(
        "password login is off and the only credential is the SSH key parameter",
        linux["disablePasswordAuthentication"] is True
        and "adminPassword" not in os_profile
        and [k["keyData"] for k in linux["ssh"]["publicKeys"]] == ["[parameters('sshPublicKey')]"],
    )
    check("each VM has a user-assigned identity only", vm["identity"]["type"] == "UserAssigned")
    disk = vm["properties"]["storageProfile"]["osDisk"]
    check(
        "each VM has one 30 GB standard disk that is deleted with it",
        disk["diskSizeGB"] == 30
        and disk["managedDisk"]["storageAccountType"] == "Standard_LRS"
        and disk["deleteOption"] == "Delete"
        and "dataDisks" not in vm["properties"]["storageProfile"],
    )

    # What cloud-init writes.
    unit = open("deploy/azure/vm/poly-node.service").read()
    check(
        "the unit restarts always after 1 s under a 600M memory limit",
        "Restart=always\n" in unit and "RestartSec=1\n" in unit and "MemoryMax=600M\n" in unit,
    )
    check(
        "the stream and the health endpoints bind loopback",
        "--listen 127.0.0.1:9100" in unit and "--http 127.0.0.1:9101" in unit,
    )
    placeholders = ["__NODE_ID__", "__STORE_URL__", "__CLIENT_ID__"]
    files = ["__POLYCTL_B64__", "__HEALTHCHECK_B64__", "__NODE_SERVICE_B64__",
             "__HEALTH_SERVICE_B64__", "__HEALTH_TIMER_B64__", "__SSHD_B64__"]
    cloud_init = open("deploy/azure/cloud-init.yaml").read()
    found = sorted(re.findall(r"__[A-Z0-9_]+__", cloud_init))
    check(
        "every placeholder in cloud-init is filled in by nodes.bicep",
        found == sorted(placeholders + files)
        and sorted(nodes["variables"]["files"]) == sorted(files)
        and all(p in json.dumps(vm) for p in placeholders),
    )

    # Storage.
    account = of(every, "Microsoft.Storage/storageAccounts")[0]
    props = account["properties"]
    check(
        "the storage account takes no account key, no public blob and only TLS 1.2 over https",
        props["allowSharedKeyAccess"] is False
        and props["allowBlobPublicAccess"] is False
        and props["supportsHttpsTrafficOnly"] is True
        and props["minimumTlsVersion"] == "TLS1_2",
    )
    containers = of(every, "Microsoft.Storage/storageAccounts/blobServices/containers")
    check(
        "the containers are lease and archive, both private",
        sorted(c["name"].split("'")[-2] for c in containers) == ["archive", "lease"]
        and all(c["properties"]["publicAccess"] == "None" for c in containers),
    )
    policy = of(every, "Microsoft.Storage/storageAccounts/managementPolicies")[0]
    rules = {r["name"]: r["definition"] for r in policy["properties"]["policy"]["rules"]}
    prefixes = [p for d in rules.values() for p in d["filters"]["prefixMatch"]]
    check(
        "archived slots are deleted after 2 days and no rule touches the lease container",
        rules["archive-retention"]["actions"]["baseBlob"]["delete"]["daysAfterModificationGreaterThan"]
        == "[parameters('archiveRetentionDays')]"
        and default(base, "archiveRetentionDays") == 2
        and "archive/seg/" in rules["archive-retention"]["filters"]["prefixMatch"]
        and all(p.startswith("archive/") for p in prefixes),
    )

    # Access.
    roles = of(every, "Microsoft.Authorization/roleAssignments")
    node_role = [r for r in roles if "copy" in r]
    check(
        "each VM identity gets Storage Blob Data Contributor on the storage account and nothing else",
        len(node_role) == 1
        and node_role[0]["scope"]
        == "[resourceId('Microsoft.Storage/storageAccounts', variables('storageAccountName'))]"
        and node_role[0]["properties"]["roleDefinitionId"] == "[variables('blobDataContributor')]"
        and "ba92f5b4-2d11-453d-a403-e96b0029c9fe" in base["variables"]["blobDataContributor"],
    )
    check(
        "no role is assigned above the resource group",
        bootstrap["resources"][0]["type"] == "Microsoft.Resources/resourceGroups"
        and all(r in flatten(base) for r in roles),
    )
    cred = of(every, "Microsoft.ManagedIdentity/userAssignedIdentities/federatedIdentityCredentials")[0]
    check(
        "the deploy identity trusts only the named environment of the named repository",
        cred["properties"]["issuer"] == "https://token.actions.githubusercontent.com"
        and cred["properties"]["subject"]
        == "[format('repo:{0}:environment:{1}', parameters('githubRepo'), parameters('githubEnvironment'))]"
        and default(bootstrap, "githubEnvironment") == "azure",
    )

    # Budget.
    budget = of(every, "Microsoft.Consumption/budgets")[0]
    check(
        "a monthly budget of 20 mails the budgetEmail parameter, which has no default",
        default(bootstrap, "budgetAmount") == 20
        and budget["properties"]["timeGrain"] == "Monthly"
        and base["variables"]["alert"]["contactEmails"] == ["[parameters('budgetEmail')]"]
        and "defaultValue" not in bootstrap["parameters"]["budgetEmail"],
    )

    if failures:
        print(f"{len(failures)} checks failed")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
