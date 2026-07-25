#!/bin/bash

# Athena Azure dev/bootstrap setup.
# Production environments should use reviewed IaC parameters, secret-store
# references, approvals, and environment-specific pipeline variables.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

PROJECT_NAME="${PROJECT_NAME:-athena}"
ENVIRONMENT="${ENVIRONMENT:-dev}"
LOCATION="${LOCATION:-eastus}"
RG_NAME="${RG_NAME:-$PROJECT_NAME-rg-$ENVIRONMENT}"
COMBINED_APP_NAME="${COMBINED_APP_NAME:-$PROJECT_NAME-combined-$ENVIRONMENT}"

echo "Athena Azure dev/bootstrap setup"
echo "Project:     $PROJECT_NAME"
echo "Environment: $ENVIRONMENT"
echo "Location:    $LOCATION"
echo "Resource group: $RG_NAME"
echo ""

echo "[1/2] Ensuring resource group exists"
if az group show --name "$RG_NAME" >/dev/null 2>&1; then
    echo "Resource group already exists: $RG_NAME"
else
    az group create --name "$RG_NAME" --location "$LOCATION"
fi

echo "[2/2] Deploying infra/azure/main.bicep"
read -r -p "Azure SQL host: " SQL_HOST
read -r -p "Azure SQL username: " SQL_USERNAME
read -r -s -p "Azure SQL password: " SQL_PASSWORD
echo ""
read -r -p "Azure SQL database [metadata]: " SQL_DATABASE
read -r -p "Azure SQL schema [metadata]: " SQL_SCHEMA
read -r -p "CORS origins [http://localhost:3000,http://127.0.0.1:3000]: " CORS_SITES

SQL_DATABASE="${SQL_DATABASE:-metadata}"
SQL_SCHEMA="${SQL_SCHEMA:-metadata}"
CORS_SITES="${CORS_SITES:-http://localhost:3000,http://127.0.0.1:3000}"

az deployment group create \
    --resource-group "$RG_NAME" \
    --template-file "$REPO_ROOT/infra/azure/main.bicep" \
    --parameters \
        location="$LOCATION" \
        projectName="$PROJECT_NAME" \
        environment="$ENVIRONMENT" \
        combinedAppName="$COMBINED_APP_NAME" \
        sqlHost="$SQL_HOST" \
        sqlUsername="$SQL_USERNAME" \
        sqlPassword="$SQL_PASSWORD" \
        sqlDatabase="$SQL_DATABASE" \
        sqlSchema="$SQL_SCHEMA" \
        corsSites="$CORS_SITES"

BACKEND_HOSTNAME="$(az webapp show \
    --resource-group "$RG_NAME" \
    --name "$COMBINED_APP_NAME" \
    --query defaultHostName -o tsv)"

echo ""
echo "Setup complete"
echo "Resource group: $RG_NAME"
echo "App Service:    $COMBINED_APP_NAME"
echo "Backend URL:    https://$BACKEND_HOSTNAME"
echo ""
echo "Next steps:"
echo "1. Configure remaining App Service settings from ENV_VARIABLES.md."
echo "2. Create or update the Azure DevOps backend pipeline at deploy/azure-pipelines/backend.yml."
echo "3. Configure the frontend Static Web App workflow secret in GitHub Actions."
echo "4. Validate the backend with: curl https://$BACKEND_HOSTNAME/health"
