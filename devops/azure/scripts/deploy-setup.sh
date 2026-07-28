#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AZURE_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TEMPLATE_FILE="${AZURE_ROOT}/infrastructure/main.bicep"
PARAMETERS_FILE="${AZURE_ROOT}/infrastructure/main.parameters.json"

PROJECT_NAME="${PROJECT_NAME:-athena}"
ENVIRONMENT="${ENVIRONMENT:-dev}"
LOCATION="${LOCATION:-eastus}"
RESOURCE_GROUP="${RESOURCE_GROUP:-${PROJECT_NAME}-rg-${ENVIRONMENT}}"

SQL_HOST="${ATHENA_SQL_HOST:-}"
SQL_USERNAME="${ATHENA_SQL_USERNAME:-}"
SQL_PASSWORD="${ATHENA_SQL_PASSWORD:-}"

if [[ -z "${SQL_HOST}" ]]; then
  read -r -p "Azure SQL host: " SQL_HOST
fi
if [[ -z "${SQL_USERNAME}" ]]; then
  read -r -p "Azure SQL username: " SQL_USERNAME
fi
if [[ -z "${SQL_PASSWORD}" ]]; then
  read -r -s -p "Azure SQL password: " SQL_PASSWORD
  echo
fi

if [[ -z "${SQL_HOST}" || -z "${SQL_USERNAME}" || -z "${SQL_PASSWORD}" ]]; then
  echo "Azure SQL host, username, and password are required." >&2
  exit 2
fi

az group create \
  --name "${RESOURCE_GROUP}" \
  --location "${LOCATION}" \
  --output none

hostname="$(
  az deployment group create \
    --resource-group "${RESOURCE_GROUP}" \
    --template-file "${TEMPLATE_FILE}" \
    --parameters "@${PARAMETERS_FILE}" \
    --parameters \
      location="${LOCATION}" \
      projectName="${PROJECT_NAME}" \
      environment="${ENVIRONMENT}" \
      sqlHost="${SQL_HOST}" \
      sqlUsername="${SQL_USERNAME}" \
      sqlPassword="${SQL_PASSWORD}" \
    --query properties.outputs.combinedDefaultHostname.value \
    --output tsv
)"

echo "Infrastructure deployment completed: https://${hostname}"
