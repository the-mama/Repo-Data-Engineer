param location string
param environment string
param storageAccountName string
param reuseExistingStorage bool
param dataFactoryName string
param namePrefix string
param sqlAdminObjectId string
param sqlAdminLogin string
param alertEmail string
param sqlAdminIps array
param tags object
var suffix = uniqueString(resourceGroup().id)
resource logs 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: '${namePrefix}-logs'
  location: location
  tags: tags
  properties: { sku: { name: 'PerGB2018' }, retentionInDays: 30 }
}
// Existing accounts must be upgraded to HNS separately after explicit operator approval.
// Reuse never changes redundancy, account security/network settings or retention policies.
module storageProvision './storage.bicep' = if (!reuseExistingStorage) {
  name: 'storage-provision'
  params: {
    storageAccountName: storageAccountName
    location: location
    tags: tags
  }
}
resource lake 'Microsoft.Storage/storageAccounts@2023-05-01' existing = {
  name: storageAccountName
}
resource blobs 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' existing = {
  parent: lake
  name: 'default'
}
resource containers 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = [for layer in ['landing','bronze','silver','gold','quarantine','synapse']: {
  dependsOn: [storageProvision]
  parent: blobs
  name: layer
  properties: { publicAccess: 'None' }
}]
resource factory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: dataFactoryName
  location: location
  tags: tags
  identity: { type: 'SystemAssigned' }
  properties: { publicNetworkAccess: 'Enabled' }
}
resource workspace 'Microsoft.Synapse/workspaces@2021-06-01' = {
  name: '${namePrefix}-syn-${suffix}'
  location: location
  tags: tags
  identity: { type: 'SystemAssigned' }
  properties: {
    defaultDataLakeStorage: {
      accountUrl: lake.properties.primaryEndpoints.dfs
      filesystem: containers[5].name
      resourceId: lake.id
      createManagedPrivateEndpoint: false
    }
    managedResourceGroupName: '${namePrefix}-syn-managed-${suffix}'
    azureADOnlyAuthentication: true
    publicNetworkAccess: 'Enabled'
  }
}
resource synapseAdmin 'Microsoft.Synapse/workspaces/administrators@2021-06-01' = {
  parent: workspace
  name: 'activeDirectory'
  properties: {
    administratorType: 'ActiveDirectory'
    login: sqlAdminLogin
    sid: sqlAdminObjectId
    tenantId: tenant().tenantId
  }
}
resource sparkPool 'Microsoft.Synapse/workspaces/bigDataPools@2021-06-01' = {
  parent: workspace
  name: 'mobilityspark'
  location: location
  tags: tags
  properties: {
    sparkVersion: '3.5'
    nodeSize: 'Small'
    nodeSizeFamily: 'MemoryOptimized'
    nodeCount: 3
    autoScale: { enabled: false }
    autoPause: { enabled: true, delayInMinutes: 15 }
    dynamicExecutorAllocation: { enabled: false }
    sessionLevelPackagesEnabled: true
    libraryRequirements: {
      filename: 'requirements.txt'
      content: loadTextContent('../../synapse/requirements.txt')
    }
  }
}
resource synapseTrusted 'Microsoft.Synapse/workspaces/firewallRules@2021-06-01' = {
  parent: workspace
  name: 'AllowAzureServices'
  properties: { startIpAddress: '0.0.0.0', endIpAddress: '0.0.0.0' }
}
resource synapseAdminIps 'Microsoft.Synapse/workspaces/firewallRules@2021-06-01' = [for (ip,i) in sqlAdminIps: {
  parent: workspace
  name: 'admin-${i}'
  properties: { startIpAddress: ip, endIpAddress: ip }
}]
resource vault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: 'um-${environment}-${suffix}'
  location: location
  tags: tags
  properties: {
    tenantId: tenant().tenantId
    sku: { family: 'A', name: 'standard' }
    enableRbacAuthorization: true
    enableSoftDelete: true
    enablePurgeProtection: true
    softDeleteRetentionInDays: 90
    publicNetworkAccess: 'Enabled'
    networkAcls: { defaultAction: 'Allow', bypass: 'AzureServices' }
    accessPolicies: []
  }
}
// Azure SQL is Entra-only; there is no database password or SQL administrator secret.
resource sql 'Microsoft.Sql/servers@2023-08-01' = {
  name: '${namePrefix}-sql-${suffix}'
  location: location
  tags: tags
  identity: { type: 'SystemAssigned' }
  properties: {
    version: '12.0'
    minimalTlsVersion: '1.2'
    publicNetworkAccess: 'Enabled'
    administrators: {
      administratorType: 'ActiveDirectory'
      principalType: 'Group'
      login: sqlAdminLogin
      sid: sqlAdminObjectId
      tenantId: tenant().tenantId
      azureADOnlyAuthentication: true
    }
  }
}
resource database 'Microsoft.Sql/servers/databases@2023-08-01' = {
  parent: sql
  name: 'mobility_control'
  location: location
  sku: { name: 'Basic', tier: 'Basic', capacity: 5 }
  properties: { maxSizeBytes: 2147483648, zoneRedundant: false }
}
resource trusted 'Microsoft.Sql/servers/firewallRules@2023-08-01' = {
  parent: sql
  name: 'AllowAzureServices'
  properties: { startIpAddress: '0.0.0.0', endIpAddress: '0.0.0.0' }
}
resource adminIps 'Microsoft.Sql/servers/firewallRules@2023-08-01' = [for (ip,i) in sqlAdminIps: {
  parent: sql
  name: 'admin-${i}'
  properties: { startIpAddress: ip, endIpAddress: ip }
}]
var blobContributor = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
resource adfLanding 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(containers[0].id, factory.id, blobContributor)
  scope: containers[0]
  properties: { principalId: factory.identity.principalId, principalType: 'ServicePrincipal', roleDefinitionId: blobContributor }
}
resource synapseLakeRoles 'Microsoft.Authorization/roleAssignments@2022-04-01' = [for (layer,i) in ['landing','bronze','silver','gold','quarantine','synapse']: {
  name: guid(containers[i].id, workspace.id, blobContributor)
  scope: containers[i]
  properties: {
    principalId: workspace.identity.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: blobContributor
  }
}]
resource adfSecrets 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(vault.id, factory.id, 'secret-reader')
  scope: vault
  properties: {
    principalId: factory.identity.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions','4633458b-17de-408a-b874-0445c86b69e6')
  }
}
// Synapse artifact/compute/credential execution rights are data-plane RBAC, configured by bootstrap_synapse.sh.
resource diagnostics 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name: 'adf-logs'
  scope: factory
  properties: {
    workspaceId: logs.id
    logAnalyticsDestinationType: 'Dedicated'
    logs: [for category in ['PipelineRuns','ActivityRuns','TriggerRuns']: { category: category, enabled: true }]
    metrics: [{ category: 'AllMetrics', enabled: true }]
  }
}
resource action 'Microsoft.Insights/actionGroups@2023-01-01' = {
  name: '${namePrefix}-operations'
  location: 'global'
  properties: {
    groupShortName: 'mobility'
    enabled: true
    emailReceivers: [{ name: 'operator', emailAddress: alertEmail, useCommonAlertSchema: true }]
  }
}
var alerts = [
  { name: 'scheduler-heartbeat', query: 'ADFPipelineRun | where PipelineName == "MobilityMaster" | summarize failures=count() | project failures=iff(failures == 0, 1, 0)', threshold: 0 }
  { name: 'pipeline-failure', query: 'ADFPipelineRun | where Status == "Failed" | summarize failures=count()', threshold: 0 }
  { name: 'api-ingestion-failure', query: 'ADFActivityRun | where ActivityType == "Copy" and Status == "Failed" | summarize failures=count()', threshold: 0 }
  { name: 'data-quality-failure', query: 'ADFActivityRun | where ActivityName == "CheckQuality" and Status == "Failed" | summarize failures=count()', threshold: 0 }
  { name: 'freshness-breach', query: 'ADFActivityRun | where ActivityName == "CheckFreshness" and Status == "Failed" | summarize failures=count()', threshold: 0 }
]
resource rules 'Microsoft.Insights/scheduledQueryRules@2023-12-01' = [for alert in alerts: {
  name: '${namePrefix}-${alert.name}'
  location: location
  tags: tags
  properties: {
    displayName: alert.name
    enabled: true
    severity: 2
    evaluationFrequency: 'PT5M'
    windowSize: 'PT15M'
    scopes: [logs.id]
    criteria: { allOf: [{
      query: alert.query
      timeAggregation: 'Maximum'
      metricMeasureColumn: 'failures'
      operator: 'GreaterThan'
      threshold: alert.threshold
      failingPeriods: { numberOfEvaluationPeriods: 1, minFailingPeriodsToAlert: 1 }
    }] }
    actions: { actionGroups: [action.id] }
    autoMitigate: true
    skipQueryValidation: true
  }
}]
output storageAccountName string = lake.name
output factoryName string = factory.name
output synapseWorkspaceName string = workspace.name
output synapseEndpoint string = 'https://${workspace.name}.dev.azuresynapse.net'
output serverlessSqlEndpoint string = '${workspace.name}-ondemand.sql.azuresynapse.net'
output synapseWorkspaceId string = workspace.id
output sparkPoolName string = sparkPool.name
output keyVaultUri string = vault.properties.vaultUri
output sqlServer string = sql.properties.fullyQualifiedDomainName
output synapsePrincipalId string = workspace.identity.principalId
output factoryPrincipalId string = factory.identity.principalId
