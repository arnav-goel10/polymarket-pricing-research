// Poly HA on Azure, part 1 of 2: everything that needs the owner's rights.
//
// Run once from the owner's machine at subscription scope. It creates one
// resource group and, inside it, the identities, the storage account with
// the lease and archive containers, the role assignments and the budget.
// No VM is created here; nodes.bicep adds those.
//
// Every resource lives in the one group and carries the project tag, so
// `az group delete -n <group>` removes all of it.
targetScope = 'subscription'

@description('Region. The subscription policy allows centralindia, japaneast, indiasouthcentral, eastasia and koreacentral.')
@allowed([
  'eastasia'
  'japaneast'
  'centralindia'
  'koreacentral'
  'indiasouthcentral'
])
param location string = 'eastasia'

@description('The one resource group everything goes into.')
param resourceGroupName string = 'poly-ha'

@description('owner/repo of the GitHub repository whose environment may deploy.')
param githubRepo string

@description('The GitHub environment the deploy workflow runs in.')
param githubEnvironment string = 'azure'

@description('Address that receives the budget alerts.')
param budgetEmail string

@description('Create the budget. Set to false on a redeploy: a budget start date cannot be changed once set.')
param deployBudget bool = true

@description('Monthly budget in the billing currency of the subscription.')
param budgetAmount int = 20

@description('First day of the month the budget starts in, as yyyy-MM-01.')
param budgetStartDate string = utcNow('yyyy-MM-01')

@description('Tags on every resource.')
param tags object = {
  project: 'poly-ha'
}

resource group 'Microsoft.Resources/resourceGroups@2024-03-01' = {
  name: resourceGroupName
  location: location
  tags: tags
}

module base 'modules/base.bicep' = {
  name: 'poly-ha-base'
  scope: group
  params: {
    location: location
    tags: tags
    githubRepo: githubRepo
    githubEnvironment: githubEnvironment
    budgetEmail: budgetEmail
    deployBudget: deployBudget
    budgetAmount: budgetAmount
    budgetStartDate: budgetStartDate
  }
}

output groupName string = group.name
output storageAccount string = base.outputs.storageAccount
output blobEndpoint string = base.outputs.blobEndpoint
@description('Set as AZURE_CLIENT_ID in the GitHub environment.')
output deployerClientId string = base.outputs.deployerClientId
@description('Set as AZURE_TENANT_ID in the GitHub environment.')
output tenantId string = subscription().tenantId
@description('Set as AZURE_SUBSCRIPTION_ID in the GitHub environment.')
output subscriptionId string = subscription().subscriptionId
