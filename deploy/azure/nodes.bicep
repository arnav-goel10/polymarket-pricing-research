// Poly HA on Azure, part 2 of 2: the network and the two VMs.
//
// Resource group scope. bootstrap.bicep must have run first: this template
// attaches the identities it created and points the nodes at its storage
// account. The deploy workflow or ops/azure/deploy.sh runs it.
//
// Inbound, the network security group allows SSH from one address range
// and denies everything else, traffic from inside the virtual network
// included. The node's stream and its health endpoints bind loopback and
// are reached through an SSH forward.

param location string = resourceGroup().location

// The length bounds turn away 0.0.0.0/0, * and a service tag here too. The
// full rule, one IPv4 range of /24 or narrower, is ops/azure/check_cidr.sh,
// which both ways of deploying run first.
@description('The only address range that may open SSH, for example 203.0.113.7/32.')
@minLength(10)
@maxLength(18)
param sshSourceCidr string

@description('The admin SSH public key. Password login is off.')
param sshPublicKey string

param adminUsername string = 'azureuser'

@description('Tags on every resource.')
param tags object = {
  project: 'poly-ha'
}

// The two free-hour sizes of Azure for Students that deploy in eastasia.
// B2pts_v2 exists only in zone 1 there, so node-b takes zone 1 and node-a
// another zone.
var nodes = [
  {
    name: 'node-a'
    id: 1
    size: 'Standard_B2ats_v2'
    zone: '2'
    imageSku: 'server'
  }
  {
    name: 'node-b'
    id: 2
    size: 'Standard_B2pts_v2'
    zone: '1'
    imageSku: 'server-arm64'
  }
]

// Same expression as modules/base.bicep.
var storageAccountName = 'polyha${uniqueString(subscription().subscriptionId, resourceGroup().id)}'

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' existing = {
  name: storageAccountName
}

resource nodeIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' existing = [
  for n in nodes: {
    name: 'poly-ha-${n.name}'
  }
]

resource nsg 'Microsoft.Network/networkSecurityGroups@2024-05-01' = {
  name: 'poly-ha-nsg'
  location: location
  tags: tags
  properties: {
    securityRules: [
      {
        name: 'allow-ssh-from-owner'
        properties: {
          priority: 100
          direction: 'Inbound'
          access: 'Allow'
          protocol: 'Tcp'
          sourceAddressPrefix: sshSourceCidr
          sourcePortRange: '*'
          destinationAddressPrefix: '*'
          destinationPortRange: '22'
        }
      }
      {
        // Overrides the default rules that let the virtual network and
        // the load balancer in.
        name: 'deny-all-other-inbound'
        properties: {
          priority: 4096
          direction: 'Inbound'
          access: 'Deny'
          protocol: '*'
          sourceAddressPrefix: '*'
          sourcePortRange: '*'
          destinationAddressPrefix: '*'
          destinationPortRange: '*'
        }
      }
    ]
  }
}

resource vnet 'Microsoft.Network/virtualNetworks@2024-05-01' = {
  name: 'poly-ha-vnet'
  location: location
  tags: tags
  properties: {
    addressSpace: {
      addressPrefixes: [
        '10.42.0.0/24'
      ]
    }
    subnets: [
      {
        name: 'nodes'
        properties: {
          addressPrefix: '10.42.0.0/27'
          networkSecurityGroup: {
            id: nsg.id
          }
        }
      }
    ]
  }
}

resource publicIp 'Microsoft.Network/publicIPAddresses@2024-05-01' = [
  for n in nodes: {
    name: '${n.name}-ip'
    location: location
    tags: tags
    sku: {
      name: 'Standard'
      tier: 'Regional'
    }
    zones: [
      n.zone
    ]
    properties: {
      publicIPAllocationMethod: 'Static'
      publicIPAddressVersion: 'IPv4'
    }
  }
]

resource nic 'Microsoft.Network/networkInterfaces@2024-05-01' = [
  for (n, i) in nodes: {
    name: '${n.name}-nic'
    location: location
    tags: tags
    properties: {
      ipConfigurations: [
        {
          name: 'primary'
          properties: {
            privateIPAllocationMethod: 'Dynamic'
            subnet: {
              id: vnet.properties.subnets[0].id
            }
            publicIPAddress: {
              id: publicIp[i].id
              properties: {
                deleteOption: 'Delete'
              }
            }
          }
        }
      ]
    }
  }
]

// The files cloud-init writes, carried base64 so their content needs no
// escaping.
var cloudInitTemplate = loadTextContent('cloud-init.yaml')
var files = {
  __POLYCTL_B64__: loadFileAsBase64('vm/polyctl.sh')
  __HEALTHCHECK_B64__: loadFileAsBase64('vm/healthcheck.sh')
  __NODE_SERVICE_B64__: loadFileAsBase64('vm/poly-node.service')
  __HEALTH_SERVICE_B64__: loadFileAsBase64('vm/poly-health.service')
  __HEALTH_TIMER_B64__: loadFileAsBase64('vm/poly-health.timer')
  __SSHD_B64__: loadFileAsBase64('vm/sshd-poly.conf')
}
var cloudInitFiles = reduce(
  items(files),
  cloudInitTemplate,
  (text, file) => replace(string(text), file.key, file.value)
)
var blobEndpoint = 'https://${storage.name}.blob.${environment().suffixes.storage}'

resource vm 'Microsoft.Compute/virtualMachines@2024-07-01' = [
  for (n, i) in nodes: {
    name: n.name
    location: location
    tags: tags
    zones: [
      n.zone
    ]
    identity: {
      type: 'UserAssigned'
      userAssignedIdentities: {
        '${nodeIdentity[i].id}': {}
      }
    }
    properties: {
      hardwareProfile: {
        vmSize: n.size
      }
      storageProfile: {
        imageReference: {
          publisher: 'Canonical'
          offer: 'ubuntu-24_04-lts'
          sku: n.imageSku
          version: 'latest'
        }
        osDisk: {
          name: '${n.name}-os'
          createOption: 'FromImage'
          diskSizeGB: 30
          caching: 'ReadWrite'
          deleteOption: 'Delete'
          managedDisk: {
            // Standard HDD. 30 GB bills as the 32 GB S4 tier.
            storageAccountType: 'Standard_LRS'
          }
        }
      }
      osProfile: {
        computerName: n.name
        adminUsername: adminUsername
        linuxConfiguration: {
          disablePasswordAuthentication: true
          ssh: {
            publicKeys: [
              {
                path: '/home/${adminUsername}/.ssh/authorized_keys'
                keyData: sshPublicKey
              }
            ]
          }
        }
        customData: base64(replace(
          replace(
            replace(cloudInitFiles, '__NODE_ID__', string(n.id)),
            '__STORE_URL__',
            blobEndpoint
          ),
          '__CLIENT_ID__',
          nodeIdentity[i].properties.clientId
        ))
      }
      networkProfile: {
        networkInterfaces: [
          {
            id: nic[i].id
            properties: {
              deleteOption: 'Delete'
            }
          }
        ]
      }
      diagnosticsProfile: {
        bootDiagnostics: {
          enabled: false
        }
      }
    }
  }
]

output nodes array = [
  for (n, i) in nodes: {
    name: n.name
    size: n.size
    zone: n.zone
    publicIp: publicIp[i].properties.ipAddress
  }
]
output blobEndpoint string = blobEndpoint
