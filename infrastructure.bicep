param location string = resourceGroup().location
param projectName string = 'athena'
param environment string = 'dev'
param keyVaultName string = '${projectName}-kv-${environment}'
param appServicePlanName string = '${projectName}-asp-${environment}'
param backendAppName string = '${projectName}-backend-${environment}'
param frontendAppName string = '${projectName}-frontend-${environment}'
param appInsightsName string = '${projectName}-ai-${environment}'

@secure()
param sqlConnectionString string
@secure()
param apiKey string

// Application Insights
resource appInsights 'Microsoft.Insights/components@2020-02-02' = {
  name: appInsightsName
  location: location
  kind: 'web'
  properties: {
    Application_Type: 'web'
    RetentionInDays: 30
  }
}

// App Service Plan (shared for both frontend and backend)
resource appServicePlan 'Microsoft.Web/serverfarms@2021-02-01' = {
  name: appServicePlanName
  location: location
  sku: {
    name: 'B2'
    tier: 'Basic'
  }
  kind: 'linux'
  properties: {
    reserved: true
  }
}

// Key Vault
resource keyVault 'Microsoft.KeyVault/vaults@2021-06-01-preview' = {
  name: keyVaultName
  location: location
  properties: {
    enabledForDeployment: true
    enabledForTemplateDeployment: true
    enableSoftDelete: true
    softDeleteRetentionInDays: 90
    tenantId: subscription().tenantId
    sku: {
      family: 'A'
      name: 'standard'
    }
    accessPolicies: []
  }
}

// Key Vault Secrets
resource kvSecretSqlConn 'Microsoft.KeyVault/vaults/secrets@2021-06-01-preview' = {
  parent: keyVault
  name: 'sql-connection-string'
  properties: {
    value: sqlConnectionString
  }
}

resource kvSecretApiKey 'Microsoft.KeyVault/vaults/secrets@2021-06-01-preview' = {
  parent: keyVault
  name: 'api-key'
  properties: {
    value: apiKey
  }
}

// Backend App Service (Python)
resource backendApp 'Microsoft.Web/sites@2021-02-01' = {
  name: backendAppName
  location: location
  kind: 'app,linux'
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    serverFarmId: appServicePlan.id
    siteConfig: {
      linuxFxVersion: 'PYTHON|3.9'
      appSettings: [
        {
          name: 'WEBSITES_ENABLE_APP_SERVICE_STORAGE'
          value: 'false'
        }
        {
          name: 'APPINSIGHTS_INSTRUMENTATIONKEY'
          value: appInsights.properties.InstrumentationKey
        }
        {
          name: 'APPLICATIONINSIGHTS_CONNECTION_STRING'
          value: 'InstrumentationKey=${appInsights.properties.InstrumentationKey}'
        }
      ]
      connectionStrings: [
        {
          name: 'SqlConnection'
          connectionString: '@Microsoft.KeyVault(VaultName=${keyVaultName};SecretName=sql-connection-string)'
          type: 'SQLServer'
        }
      ]
    }
    httpsOnly: true
  }
}

// Backend App Service - enable Key Vault access
resource backendAppKeyVaultAccess 'Microsoft.KeyVault/vaults/accessPolicies@2021-06-01-preview' = {
  parent: keyVault
  name: 'add'
  properties: {
    accessPolicies: [
      {
        tenantId: subscription().tenantId
        objectId: backendApp.identity.principalId
        permissions: {
          secrets: [
            'get'
            'list'
          ]
        }
      }
    ]
  }
}

// Frontend App Service (Node.js)
resource frontendApp 'Microsoft.Web/sites@2021-02-01' = {
  name: frontendAppName
  location: location
  kind: 'app,linux'
  properties: {
    serverFarmId: appServicePlan.id
    siteConfig: {
      linuxFxVersion: 'NODE|16-lts'
      appSettings: [
        {
          name: 'WEBSITES_ENABLE_APP_SERVICE_STORAGE'
          value: 'false'
        }
        {
          name: 'APPINSIGHTS_INSTRUMENTATIONKEY'
          value: appInsights.properties.InstrumentationKey
        }
        {
          name: 'REACT_APP_API_ENDPOINT'
          value: 'https://${backendApp.properties.defaultHostName}'
        }
      ]
    }
    httpsOnly: true
  }
}

// Backend Deployment Slot (staging)
resource backendStagingSlot 'Microsoft.Web/sites/slots@2021-02-01' = {
  parent: backendApp
  name: 'staging'
  location: location
  kind: 'app,linux'
  properties: {
    serverFarmId: appServicePlan.id
    siteConfig: {
      linuxFxVersion: 'PYTHON|3.9'
      appSettings: [
        {
          name: 'WEBSITES_ENABLE_APP_SERVICE_STORAGE'
          value: 'false'
        }
        {
          name: 'APPINSIGHTS_INSTRUMENTATIONKEY'
          value: appInsights.properties.InstrumentationKey
        }
      ]
    }
  }
}

output backendAppName string = backendApp.name
output frontendAppName string = frontendApp.name
output keyVaultName string = keyVault.name
output appInsightsInstrumentationKey string = appInsights.properties.InstrumentationKey
output backendDefaultHostname string = backendApp.properties.defaultHostName
output frontendDefaultHostname string = frontendApp.properties.defaultHostName
