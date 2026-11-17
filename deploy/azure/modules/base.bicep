// Identities, storage, role assignments and the budget, in one resource
// group. Called by bootstrap.bicep.

param location string
param tags object
param githubRepo string
param githubEnvironment string
param budgetEmail string
param deployBudget bool
param budgetAmount int
param budgetStartDate string

@description('Days an archived slot, a drill upload or a self test object is kept.')
param archiveRetentionDays int = 2

@description('Days an uploaded node build is kept.')
param releaseRetentionDays int = 14

// 3 to 24 lower-case letters and digits, unique across Azure.
var storageAccountName = 'polyha${uniqueString(subscription().subscriptionId, resourceGroup().id)}'

var nodeNames = [
  'node-a'
  'node-b'
]

// Built-in role ids.
var blobDataContributor = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  'ba92f5b4-2d11-453d-a403-e96b0029c9fe'
)
var contributor = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  'b24988ac-6180-42a0-ab88-20f7382dd24c'
)

// One identity per VM. A VM asks the metadata service for a token as this
// identity; no key or password exists for it.
resource nodeIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = [
  for name in nodeNames: {
    name: 'poly-ha-${name}'
    location: location
    tags: tags
  }
]

// The identity GitHub Actions signs in as. It has no secret: GitHub's OIDC
// token for the named environment of the named repository is exchanged for
// an Azure token, and nothing else can use it.
resource deployer 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: 'poly-ha-deployer'
  location: location
  tags: tags
}

resource federation 'Microsoft.ManagedIdentity/userAssignedIdentities/federatedIdentityCredentials@2023-01-31' = {
  parent: deployer
  name: 'github-${githubEnvironment}'
  properties: {
    issuer: 'https://token.actions.githubusercontent.com'
    subject: 'repo:${githubRepo}:environment:${githubEnvironment}'
    audiences: [
      'api://AzureADTokenExchange'
    ]
  }
}

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageAccountName
  location: location
  tags: tags
  kind: 'StorageV2'
  sku: {
    name: 'Standard_LRS'
  }
  properties: {
    accessTier: 'Hot'
    supportsHttpsTrafficOnly: true
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    // No account key works against this account. Every request carries an
    // identity token.
    allowSharedKeyAccess: false
    defaultToOAuthAuthentication: true
    allowCrossTenantReplication: false
  }
}

resource blobs 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: storage
  name: 'default'
  properties: {
    // Deleted data is gone, so the lifecycle rule really bounds the bill.
    deleteRetentionPolicy: {
      enabled: false
    }
    containerDeleteRetentionPolicy: {
      enabled: false
    }
    isVersioningEnabled: false
  }
}

// HEAD.json, the lease and the archive manifest. No lifecycle rule names
// this container.
resource leaseContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobs
  name: 'lease'
  properties: {
    publicAccess: 'None'
  }
}

// seg/ (archived slots), drill/, selftest/ and releases/.
resource archiveContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobs
  name: 'archive'
  properties: {
    publicAccess: 'None'
  }
}

resource lifecycle 'Microsoft.Storage/storageAccounts/managementPolicies@2023-05-01' = {
  parent: storage
  name: 'default'
  dependsOn: [
    archiveContainer
  ]
  properties: {
    policy: {
      rules: [
        {
          name: 'archive-retention'
          enabled: true
          type: 'Lifecycle'
          definition: {
            filters: {
              blobTypes: [
                'blockBlob'
              ]
              prefixMatch: [
                'archive/seg/'
                'archive/drill/'
                'archive/selftest/'
              ]
            }
            actions: {
              baseBlob: {
                delete: {
                  daysAfterModificationGreaterThan: archiveRetentionDays
                }
              }
            }
          }
        }
        {
          name: 'release-retention'
          enabled: true
          type: 'Lifecycle'
          definition: {
            filters: {
              blobTypes: [
                'blockBlob'
              ]
              prefixMatch: [
                'archive/releases/'
              ]
            }
            actions: {
              baseBlob: {
                delete: {
                  daysAfterModificationGreaterThan: releaseRetentionDays
                }
              }
            }
          }
        }
      ]
    }
  }
}

// Each VM identity may read and write blobs in this account and nowhere
// else: the assignment's scope is the account.
resource nodeBlobAccess 'Microsoft.Authorization/roleAssignments@2022-04-01' = [
  for (name, i) in nodeNames: {
    scope: storage
    name: guid(storage.id, nodeIdentity[i].id, blobDataContributor)
    properties: {
      roleDefinitionId: blobDataContributor
      principalId: nodeIdentity[i].properties.principalId
      principalType: 'ServicePrincipal'
    }
  }
]

// The deployer uploads node builds to releases/.
resource deployerBlobAccess 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  scope: storage
  name: guid(storage.id, deployer.id, blobDataContributor)
  properties: {
    roleDefinitionId: blobDataContributor
    principalId: deployer.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

// The deployer creates, starts, stops and deletes what is in this group. It
// has no right outside it and cannot grant roles.
resource deployerGroupAccess 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(resourceGroup().id, deployer.id, contributor)
  properties: {
    roleDefinitionId: contributor
    principalId: deployer.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

var alert = {
  enabled: true
  operator: 'GreaterThanOrEqualTo'
  contactEmails: [
    budgetEmail
  ]
}

resource budget 'Microsoft.Consumption/budgets@2023-11-01' = if (deployBudget) {
  name: 'poly-ha-monthly'
  properties: {
    category: 'Cost'
    amount: budgetAmount
    timeGrain: 'Monthly'
    timePeriod: {
      startDate: budgetStartDate
    }
    notifications: {
      actual50: union(alert, {
        threshold: 50
        thresholdType: 'Actual'
      })
      actual80: union(alert, {
        threshold: 80
        thresholdType: 'Actual'
      })
      actual100: union(alert, {
        threshold: 100
        thresholdType: 'Actual'
      })
      forecast100: union(alert, {
        threshold: 100
        thresholdType: 'Forecasted'
      })
    }
  }
}

output storageAccount string = storage.name
output blobEndpoint string = storage.properties.primaryEndpoints.blob
output deployerClientId string = deployer.properties.clientId
