#!/bin/bash

# Athena Azure Deployment Setup Script
# This script automates the setup of Azure resources and DevOps pipeline

set -e

# Configuration
PROJECT_NAME="athena"
ENVIRONMENT="dev"
LOCATION="eastus"
RG_NAME="$PROJECT_NAME-rg-$ENVIRONMENT"
KV_NAME="$PROJECT_NAME-kv-$ENVIRONMENT"
BACKEND_APP_NAME="$PROJECT_NAME-backend-$ENVIRONMENT"
FRONTEND_APP_NAME="$PROJECT_NAME-frontend-$ENVIRONMENT"

echo "=========================================="
echo "Athena Azure Deployment Setup"
echo "=========================================="
echo "Project: $PROJECT_NAME"
echo "Environment: $ENVIRONMENT"
echo "Location: $LOCATION"
echo ""

# Step 1: Create Resource Group
echo "[1/6] Creating Resource Group..."
if az group show --name "$RG_NAME" &>/dev/null; then
    echo "✓ Resource group $RG_NAME already exists"
else
    az group create --name "$RG_NAME" --location "$LOCATION"
    echo "✓ Resource group created"
fi

# Step 2: Deploy Bicep Template
echo "[2/6] Deploying infrastructure (Bicep)..."
read -p "Enter SQL Connection String: " SQL_CONN_STR
read -p "Enter API Key: " API_KEY

az deployment group create \
    --resource-group "$RG_NAME" \
    --template-file infrastructure.bicep \
    --parameters \
        location="$LOCATION" \
        projectName="$PROJECT_NAME" \
        environment="$ENVIRONMENT" \
        sqlConnectionString="$SQL_CONN_STR" \
        apiKey="$API_KEY"

echo "✓ Infrastructure deployed"

# Step 3: Grant Backend App Service access to Key Vault
echo "[3/6] Configuring Key Vault access for App Services..."
BACKEND_PRINCIPAL=$(az webapp identity show \
    --resource-group "$RG_NAME" \
    --name "$BACKEND_APP_NAME" \
    --query principalId -o tsv)

az keyvault set-policy \
    --name "$KV_NAME" \
    --object-id "$BACKEND_PRINCIPAL" \
    --secret-permissions get list

echo "✓ Key Vault access configured"

# Step 4: Configure Backend App Settings
echo "[4/6] Configuring backend app settings..."
az webapp config appsettings set \
    --resource-group "$RG_NAME" \
    --name "$BACKEND_APP_NAME" \
    --settings \
        SQL_CONNECTION_STRING="@Microsoft.KeyVault(VaultName=$KV_NAME;SecretName=sql-connection-string)" \
        API_KEY="@Microsoft.KeyVault(VaultName=$KV_NAME;SecretName=api-key)" \
        ENVIRONMENT="$ENVIRONMENT" \
        PYTHONUNBUFFERED="1" \
        LOG_LEVEL="INFO"

echo "✓ Backend app settings configured"

# Step 5: Configure Frontend App Settings
echo "[5/6] Configuring frontend app settings..."
BACKEND_HOSTNAME=$(az webapp show \
    --resource-group "$RG_NAME" \
    --name "$BACKEND_APP_NAME" \
    --query defaultHostName -o tsv)

az webapp config appsettings set \
    --resource-group "$RG_NAME" \
    --name "$FRONTEND_APP_NAME" \
    --settings \
        REACT_APP_API_ENDPOINT="https://$BACKEND_HOSTNAME" \
        REACT_APP_ENVIRONMENT="$ENVIRONMENT"

echo "✓ Frontend app settings configured"

# Step 6: Create deployment slots
echo "[6/6] Creating deployment slots..."
az webapp deployment slot create \
    --resource-group "$RG_NAME" \
    --name "$BACKEND_APP_NAME" \
    --slot staging || echo "⚠ Staging slot may already exist"

echo ""
echo "=========================================="
echo "✓ Setup completed successfully!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Create Azure DevOps Service Connection (see DEPLOYMENT_GUIDE.md)"
echo "2. Create pipelines in Azure DevOps:"
echo "   - azure-pipelines-backend.yml → Name: 'Athena-Backend-CI'"
echo "   - azure-pipelines-frontend.yml → Name: 'Athena-Frontend-CI'"
echo "   - azure-pipelines-cd.yml → Name: 'Athena-CD'"
echo ""
echo "Resources created:"
echo "  Resource Group: $RG_NAME"
echo "  Backend App: $BACKEND_APP_NAME"
echo "  Frontend App: $FRONTEND_APP_NAME"
echo "  Key Vault: $KV_NAME"
echo ""
echo "Backend URL: https://$BACKEND_APP_NAME.azurewebsites.net"
echo "Frontend URL: https://$FRONTEND_APP_NAME.azurewebsites.net"
