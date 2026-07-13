# Azure DevOps Deployment Guide

This guide explains how to deploy the Athena application to Azure as a single combined App Service running both backend and frontend.

## Architecture

- **Combined App Service**: Single Linux App Service running Python 3.9 (FastAPI) + React frontend as static files
- **Key Vault**: Azure Key Vault for secure secret management
- **App Service Plan**: B2 tier Linux plan
- **Application Insights**: Monitoring and telemetry

## Prerequisites

1. **Azure Subscription**
2. **Azure DevOps Organization** (free tier acceptable)
3. **Azure CLI** installed locally
4. Service Principal for Azure DevOps

## Step 1: Create Azure AD Service Principal

```bash
# Create service principal
az ad sp create-for-rbac \
  --name "athena-devops-sp" \
  --role "Contributor" \
  --scopes /subscriptions/{subscription-id}
```

Save the output:
- `appId` → Client ID
- `password` → Client Secret
- `tenant` → Tenant ID

## Step 2: Create Resource Group

```bash
az group create \
  --name athena-rg-dev \
  --location eastus
```

## Step 3: Set Up Azure DevOps Service Connection

1. Go to **Project Settings** → **Service Connections** → **Create service connection**
2. Choose **Azure Resource Manager**
3. Select **Service Principal (manual)**
4. Enter details from Step 1:
   - Subscription ID
   - Subscription Name
   - Service Principal ID (appId)
   - Service Principal Key (password)
   - Tenant ID
5. Name it: `azure-devops-service-connection`

## Step 4: Create Variable Group for Secrets

1. Go to **Pipelines** → **Library** → **+ Variable group**
2. Create group `athena-secrets`:
   - `AZURE_SUBSCRIPTION_ID`: Your subscription ID
   - `AZURE_DEVOPS_SERVICE_CONNECTION`: `azure-devops-service-connection`
   - `SQL_CONNECTION_STRING`: Your SQL connection string
   - `API_KEY`: Your API key

## Step 5: Update Infrastructure Parameters

Edit `infrastructure.parameters.json`:

```json
{
  "parameters": {
    "sqlConnectionString": {
      "value": "Your actual SQL connection string"
    },
    "apiKey": {
      "value": "Your actual API key"
    }
  }
}
```

## Step 6: Create Pipelines in Azure DevOps

### Pipeline 1: Backend deployment (azure-pipelines-backend.yml)
*Builds and deploys the backend to the configured Azure App Service*

1. **Pipelines** → **New pipeline** → **GitHub** (or your repo)
2. Select **Existing Azure Pipelines YAML file**
3. Path: `azure-pipelines-backend.yml`
4. Name: `Athena-Backend-CI`
5. Save & queue

### Legacy CD pipeline (removed)
*Deploys combined artifact to single App Service*

1. **Pipelines** → **New pipeline**
2. This legacy pipeline has been removed; use `azure-pipelines-backend.yml`.
3. Name: `Athena-CD`
4. Configure trigger: **Pipeline completion** for Athena-Backend-CI
5. Save & queue

## Step 7: Deploy Infrastructure

```bash
# Deploy Bicep template
az deployment group create \
  --resource-group athena-rg-dev \
  --template-file infrastructure.bicep \
  --parameters infrastructure.parameters.json \
  --parameters projectName=athena environment=dev
```

## Step 8: Grant Key Vault Access to App Service

After infrastructure deployment:

```bash
# Get app service identity
APP_PRINCIPAL=$(az webapp identity show \
  --resource-group athena-rg-dev \
  --name athena-combined-dev \
  --query principalId -o tsv)

# Grant Key Vault access
az keyvault set-policy \
  --name athena-kv-dev \
  --object-id $APP_PRINCIPAL \
  --secret-permissions get list
```

## Step 9: Configure App Service Environment Variables

### Combined App Service (Python + React)

```bash
az webapp config appsettings set \
  --resource-group athena-rg-dev \
  --name athena-combined-dev \
  --settings \
    SQL_CONNECTION_STRING="@Microsoft.KeyVault(VaultName=athena-kv-dev;SecretName=sql-connection-string)" \
    API_KEY="@Microsoft.KeyVault(VaultName=athena-kv-dev;SecretName=api-key)" \
    ENVIRONMENT="dev" \
    PYTHONUNBUFFERED="1" \
    LOG_LEVEL="INFO"
```

## Step 10: Enable Deployment Slot Swap

```bash
# Create staging slot if not already created
az webapp deployment slot create \
  --resource-group athena-rg-dev \
  --name athena-combined-dev \
  --slot staging
```

## Step 11: Verify Deployment

Check app service:

```bash
# Health check (backend)
curl https://athena-combined-dev.azurewebsites.net/health

# Frontend (React app loads at root)
curl https://athena-combined-dev.azurewebsites.net
```

View logs:

```bash
# Stream logs from combined app service
az webapp log tail \
  --resource-group athena-rg-dev \
  --name athena-combined-dev
```

## Environment Variables Reference

### Combined App Service (Athena_backend + React Frontend)

- `SQL_CONNECTION_STRING`: Database connection (from Key Vault)
- `API_KEY`: API authentication key (from Key Vault)
- `ENVIRONMENT`: dev/prod
- `LOG_LEVEL`: DEBUG/INFO/WARNING/ERROR
- `PYTHONUNBUFFERED`: 1 (for unbuffered Python output)
- `APPINSIGHTS_INSTRUMENTATIONKEY`: Application Insights key

### Frontend (frontend)

- `REACT_APP_API_ENDPOINT`: Backend API base URL
- `REACT_APP_ENVIRONMENT`: dev/prod
- `PUBLIC_URL`: CDN URL (if applicable)

## Pipeline Flow

```
Commit to main
    ↓
[Backend-CI] Install deps → Run tests → Build artifact
[Frontend-CI] npm install → npm build → Build artifact
    ↓
[CD Pipeline] 
  ├→ Deploy Infrastructure (Bicep)
  ├→ Deploy Backend (to staging slot)
  │   ├→ Swap staging → production
  │   └→ Configure env vars from Key Vault
  └→ Deploy Frontend
      └→ Configure env vars
```

## Rollback Strategy

### Backend (Blue-Green Swap)

```bash
# Swap back to previous version (if staging has previous code)
az webapp deployment slot swap \
  --resource-group athena-rg-dev \
  --name athena-backend-dev \
  --slot staging
```

### Frontend (Manual Redeploy)

Redeploy previous build artifact or manually update the app.

## Monitoring

1. **Application Insights**: https://portal.azure.com → Resource group → App Insights resource
2. **Log Analytics**: View live logs in portal
3. **Alerts**: Configure alert rules for failures, high latency, exceptions

## Troubleshooting

### App Service logs not showing

```bash
# Enable application logging
az webapp log config \
  --resource-group athena-rg-dev \
  --name athena-backend-dev \
  --application-logging filesystem \
  --level information
```

### Key Vault secret not accessible

```bash
# Verify access policy
az keyvault show \
  --name athena-kv-dev \
  --query properties.accessPolicies
```

### Deployment slot swap failed

```bash
# Check slot status
az webapp deployment slot list \
  --resource-group athena-rg-dev \
  --name athena-backend-dev
```

## Next Steps

1. Configure custom domain (DNS)
2. Set up SSL/TLS certificates
3. Enable App Service authentication (Azure AD)
4. Configure auto-scaling based on CPU/memory
5. Set up backup and disaster recovery
6. Implement deployment approvals in pipeline

## Cost Optimization

- **Dev**: B2 (Basic) tier ~$40/month per app
- Consider reducing to B1 if load is minimal
- Combine multiple services on one App Service Plan to reduce costs
- Use Azure DevOps Free tier (up to 1 job concurrency)
