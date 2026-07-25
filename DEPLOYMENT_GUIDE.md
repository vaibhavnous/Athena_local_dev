# Athena Deployment Guide

This guide documents the deployment files that currently exist in this repository after the `apps/`, `deploy/`, and `infra/` restructure.

## Current Repository Layout

```text
apps/backend/                      FastAPI backend
apps/frontend/                     React frontend
deploy/azure-pipelines/backend.yml Azure DevOps backend CI/CD
deploy/azure-pipelines/frontend.yml Azure DevOps frontend build artifact pipeline
deploy/github/frontend-build.yml   GitHub Actions frontend build workflow
.github/workflows/azure-static-web-apps-black-sand-013f9d900.yml
infra/azure/main.bicep             Azure dev/bootstrap infrastructure
infra/azure/parameters/dev.json    Dev/bootstrap parameter file
```

## Active Deployment Paths

### Backend App Service

The backend production pipeline is:

```text
deploy/azure-pipelines/backend.yml
```

It:

- uses Python `3.10`
- installs dependencies from `apps/backend/requirements.txt`
- runs backend tests from `apps/backend/tests`
- packages `apps/backend`
- deploys to the configured Linux App Service
- verifies `/health`

Required Azure DevOps variables:

```text
azureServiceConnection
webAppName
backendHealthUrl
```

The checked-in pipeline currently sets those values directly in `deploy/azure-pipelines/backend.yml`. For enterprise environments, move environment-specific values into Azure DevOps variable groups or pipeline library variables.

### Frontend Static Web App

The active frontend deployment workflow is:

```text
.github/workflows/azure-static-web-apps-black-sand-013f9d900.yml
```

It:

- uses Node `20`
- installs dependencies in `apps/frontend`
- runs frontend tests
- builds the React app
- deploys `apps/frontend/build` to Azure Static Web Apps

Required GitHub secret:

```text
AZURE_STATIC_WEB_APPS_API_TOKEN_ASTRADATA
```

The workflow also supports the legacy secret names already present in the YAML.

### Azure DevOps Frontend Build

```text
deploy/azure-pipelines/frontend.yml
```

This pipeline builds and publishes a frontend artifact. It does not currently perform a production deployment by itself.

## Dev Infrastructure Bootstrap

The Bicep template is:

```text
infra/azure/main.bicep
```

The default parameter file is:

```text
infra/azure/parameters/dev.json
```

Current template defaults:

- Linux App Service
- Python `3.10`
- Basic `B2` App Service Plan
- Application Insights
- staging slot
- app settings for Azure SQL, Pinecone, ADLS, and Azure OpenAI

The current template is suitable as a dev/bootstrap baseline. For enterprise production, validate and harden it before rollout:

- use Key Vault references for secrets instead of plain App Settings values
- use Premium v3 or isolated tiers if SLA, scale-out, VNet integration, or deployment-slot limits require it
- define per-environment parameter files under `infra/azure/parameters/`
- enforce private networking where required
- add backup, disaster recovery, alerting, and retention policies
- move environment-specific app names out of checked-in pipeline YAML

Deploy the dev/bootstrap template:

```bash
az deployment group create \
  --resource-group athena-rg-dev \
  --template-file infra/azure/main.bicep \
  --parameters infra/azure/parameters/dev.json
```

The setup helper:

```text
deploy/azure/setup.sh
```

is a convenience wrapper around the same Bicep deployment. It is not a replacement for reviewed production infrastructure automation.

## Required Runtime Configuration

Use `ENV_VARIABLES.md` as the environment-variable source of truth.

Minimum backend categories:

- Azure SQL pipeline database
- Azure OpenAI
- authentication secrets
- CORS origins
- optional Pinecone settings
- optional Snowflake and dbt settings
- optional ADLS/SFTP/Databricks settings

Never commit real `.env` files, private keys, tokens, passwords, or publish profiles.

## Pre-Deployment Validation

Run from the repository root.

Backend:

```powershell
cd apps/backend
$env:PYTHONPATH='.'
pytest -p no:cacheprovider tests/test_projects.py tests/test_snowflake_dbt_runtime.py
```

Frontend:

```powershell
cd apps/frontend
npm ci
npm run build
```

For full release readiness, run the complete backend test suite and the frontend test suite in CI.

## Deployment Checks

Before deployment:

1. Confirm no active pipeline execution will be interrupted.
2. Confirm App Service settings match `ENV_VARIABLES.md`.
3. Confirm CORS allows only approved frontend origins.
4. Confirm Snowflake, ADLS, Azure SQL, and Databricks network allowlists include the deployed app where applicable.
5. Confirm rollback artifact or previous release version is available.

After backend deployment:

```bash
curl https://<backend-app-host>/health
```

After frontend deployment:

```bash
curl https://<frontend-host>
```

Then run one small end-to-end project flow through project creation and the selected target execution path.

## Rollback

Backend rollback depends on the configured App Service deployment method:

- redeploy the previous backend artifact, or
- swap back from a staging slot if the environment uses slot-based release.

Frontend rollback:

- redeploy the previous Static Web App artifact or rerun the previous successful workflow.

## Production Readiness Gaps

The repository is deployable, but the checked-in infrastructure is not yet a complete enterprise production baseline. Before treating it as production-standard, close these gaps:

- secret references through Key Vault or equivalent secret store
- environment-specific IaC parameter files
- deployment approvals
- least-privilege service connections
- managed identity access model
- private networking and firewall policy
- centralized monitoring dashboards and alerts
- backup and disaster-recovery runbooks
- load, resilience, and concurrency validation
