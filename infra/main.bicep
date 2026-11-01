targetScope = 'resourceGroup'
@allowed(['dev', 'prod'])
param environment string = 'dev'
param location string = resourceGroup().location
param storageAccountName string = 'dltmprojects'
@description('Reuse an existing HNS-enabled account in this resource group without changing its settings. Upgrade existing non-HNS accounts separately with explicit approval.')
param reuseExistingStorage bool = false
param dataFactoryName string = 'dftmprojects'
param namePrefix string = 'mobility-${environment}'
@description('Entra group object ID for Azure SQL administrators; supplied by deployment, never committed.')
param sqlAdminObjectId string
param sqlAdminLogin string
param alertEmail string
@description('Public egress IPs allowed to administer Azure SQL. ADF trusted Azure access is separately enabled.')
param sqlAdminIps array = []
param tags object = { project: 'urban-mobility', environment: environment }

module platform './modules/platform.bicep' = {
  name: 'platform'
  params: {
    location: location
    environment: environment
    storageAccountName: storageAccountName
    reuseExistingStorage: reuseExistingStorage
    dataFactoryName: dataFactoryName
    namePrefix: namePrefix
    sqlAdminObjectId: sqlAdminObjectId
    sqlAdminLogin: sqlAdminLogin
    alertEmail: alertEmail
    sqlAdminIps: sqlAdminIps
    tags: tags
  }
}
output storageAccountName string = platform.outputs.storageAccountName
output factoryName string = platform.outputs.factoryName
output synapseWorkspaceName string = platform.outputs.synapseWorkspaceName
output synapseEndpoint string = platform.outputs.synapseEndpoint
output serverlessSqlEndpoint string = platform.outputs.serverlessSqlEndpoint
output synapseWorkspaceId string = platform.outputs.synapseWorkspaceId
output sparkPoolName string = platform.outputs.sparkPoolName
output keyVaultUri string = platform.outputs.keyVaultUri
output sqlServer string = platform.outputs.sqlServer
output synapsePrincipalId string = platform.outputs.synapsePrincipalId
output factoryPrincipalId string = platform.outputs.factoryPrincipalId
