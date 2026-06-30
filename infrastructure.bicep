param location string = resourceGroup().location
param projectName string = 'athena'
param environment string = 'dev'
param appServicePlanName string = '${projectName}-asp-${environment}'
param combinedAppName string = '${projectName}-combined-${environment}'
param appInsightsName string = '${projectName}-ai-${environment}'

// SQL Configuration
@secure()
param sqlHost string
@secure()
param sqlUsername string
@secure()
param sqlPassword string
param sqlDatabase string = 'metadata'
param sqlSchema string = 'metadata'

// Azure Services Configuration
param corsSites string = 'http://localhost:3000'
@secure()
param pineconeApiKey string = ''
param adlsAccountUrl string = ''
param adlsFileSystem string = ''
param athenaLlmProvider string = 'azure_openai'
param azureOpenaiDeployment string = ''
@secure()
param azureOpenaiApiKey string = ''
param azureOpenaiEndpoint string = ''

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



// Combined App Service (Python FastAPI + React Static Files)
resource combinedApp 'Microsoft.Web/sites@2021-02-01' = {
  name: combinedAppName
  location: location
  kind: 'app,linux'
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    serverFarmId: appServicePlan.id
    siteConfig: {
      linuxFxVersion: 'PYTHON|3.10'
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
        {
          name: 'PYTHONUNBUFFERED'
          value: '1'
        }
        {
          name: 'ENVIRONMENT'
          value: environment
        }
        {
          name: 'LOG_LEVEL'
          value: 'INFO'
        }
        {
          name: 'AZURE_SQL_HOST'
          value: sqlHost
        }
        {
          name: 'AZURE_SQL_USERNAME'
          value: sqlUsername
        }
        {
          name: 'AZURE_SQL_PASSWORD'
          value: sqlPassword
        }
        {
          name: 'AZURE_SQL_PIPELINE_DATABASE'
          value: sqlDatabase
        }
        {
          name: 'AZURE_SQL_PIPELINE_SCHEMA'
          value: sqlSchema
        }
        {
          name: 'ATHENA_CORS_ORIGINS'
          value: corsSites
        }
        {
          name: 'PINECONE_API_KEY'
          value: pineconeApiKey
        }
        {
          name: 'ADLS_ACCOUNT_URL'
          value: adlsAccountUrl
        }
        {
          name: 'ADLS_FILE_SYSTEM'
          value: adlsFileSystem
        }
        {
          name: 'ATHENA_LLM_PROVIDER'
          value: athenaLlmProvider
        }
        {
          name: 'AZURE_OPENAI_DEPLOYMENT'
          value: azureOpenaiDeployment
        }
        {
          name: 'AZURE_OPENAI_API_KEY'
          value: azureOpenaiApiKey
        }
        {
          name: 'AZURE_OPENAI_ENDPOINT'
          value: azureOpenaiEndpoint
        }
      ]
    }
    httpsOnly: true
  }
}



// Deployment Slot (staging) - for zero-downtime deployments
resource combinedStagingSlot 'Microsoft.Web/sites/slots@2021-02-01' = {
  parent: combinedApp
  name: 'staging'
  location: location
  kind: 'app,linux'
  properties: {
    serverFarmId: appServicePlan.id
    siteConfig: {
      linuxFxVersion: 'PYTHON|3.10'
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
          name: 'PYTHONUNBUFFERED'
          value: '1'
        }
        {
          name: 'ENVIRONMENT'
          value: environment
        }
        {
          name: 'LOG_LEVEL'
          value: 'INFO'
        }
        {
          name: 'AZURE_SQL_HOST'
          value: sqlHost
        }
        {
          name: 'AZURE_SQL_USERNAME'
          value: sqlUsername
        }
        {
          name: 'AZURE_SQL_PASSWORD'
          value: sqlPassword
        }
        {
          name: 'AZURE_SQL_PIPELINE_DATABASE'
          value: sqlDatabase
        }
        {
          name: 'AZURE_SQL_PIPELINE_SCHEMA'
          value: sqlSchema
        }
        {
          name: 'ATHENA_CORS_ORIGINS'
          value: corsSites
        }
        {
          name: 'PINECONE_API_KEY'
          value: pineconeApiKey
        }
        {
          name: 'ADLS_ACCOUNT_URL'
          value: adlsAccountUrl
        }
        {
          name: 'ADLS_FILE_SYSTEM'
          value: adlsFileSystem
        }
        {
          name: 'ATHENA_LLM_PROVIDER'
          value: athenaLlmProvider
        }
        {
          name: 'AZURE_OPENAI_DEPLOYMENT'
          value: azureOpenaiDeployment
        }
        {
          name: 'AZURE_OPENAI_API_KEY'
          value: azureOpenaiApiKey
        }
        {
          name: 'AZURE_OPENAI_ENDPOINT'
          value: azureOpenaiEndpoint
        }
      ]
    }
  }
}

output combinedAppName string = combinedApp.name
output appInsightsInstrumentationKey string = appInsights.properties.InstrumentationKey
output combinedDefaultHostname string = combinedApp.properties.defaultHostName
