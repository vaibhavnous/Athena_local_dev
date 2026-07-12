# Azure App Service B3 deployment

This backend can run on Linux App Service B3 as a **single-instance, low-concurrency production pilot**.

## Non-negotiable limits

- Keep the App Service at exactly **one instance**. Pipeline ownership and duplicate-job protection are process-local.
- Deploy only when `GET /health` reports `background_capacity.active` as `0`. A deployment restart interrupts active pipelines.
- B3 has no deployment slots. Keep the previous deployment artifact so a failed release can be redeployed immediately.
- Do not expose the API publicly without Microsoft Entra authentication, API Management, or restrictive App Service access rules.

## Create or configure the app

Use Python 3.11 on Linux. Replace the placeholders before running these commands.

```powershell
az appservice plan create --name <plan> --resource-group <resource-group> --location <region> --is-linux --sku B3 --number-of-workers 1
az webapp create --name <backend-app> --resource-group <resource-group> --plan <plan> --runtime "PYTHON:3.11"
az webapp config set --name <backend-app> --resource-group <resource-group> --startup-file "bash startup.sh" --always-on true --ftps-state Disabled --http20-enabled true
```

In **App Service > Settings > Environment variables**, set these non-secret values:

```text
SCM_DO_BUILD_DURING_DEPLOYMENT=true
ATHENA_DEMO_MODE=false
ATHENA_STARTUP_IMPORT_SMOKE=true
ATHENA_BACKGROUND_WORKERS=1
BRONZE_MAX_WORKERS=3
SILVER_MAX_WORKERS=3
COLUMN_PROFILING_MAX_WORKERS=3
GUNICORN_TIMEOUT_SECONDS=600
ATHENA_BLOCK_EMBEDDINGS=true
ATHENA_ENABLE_EMBEDDINGS=false
ATHENA_PRELOAD_EMBEDDINGS=false
ATHENA_ALLOW_LOCAL_EMBEDDING_FALLBACK=false
ATHENA_MARK_INTERRUPTED_RUNS_ON_STARTUP=true
ATHENA_APP_DATA_DIR=/home/site/data
ATHENA_GENERATED_CODE_DIR=/home/site/data/generated_code
ATHENA_UPLOAD_DIR=/home/site/data/uploads
ATHENA_PIPELINE_LOG_FILE=/home/LogFiles/athena/pipeline_logs.json
WEBSITE_HEALTHCHECK_MAXPINGFAILURES=3
WEBSITES_CONTAINER_START_TIME_LIMIT=600
ATHENA_CORS_ORIGINS=https://<frontend-host>
```

Store Azure SQL, Azure OpenAI, Snowflake, Pinecone, ADLS, and SFTP credentials as secret App Settings backed by Key Vault references. Never put secret values in deployment commands, source control, or deployment logs.

In **Monitoring > Health check**, use `/health`. Enable Application Insights and alerts for:

- HTTP 5xx responses
- response time
- CPU above 80%
- memory above 80%
- application restarts

## Pre-deployment checks

From the repository root:

```powershell
cd Athena_backend
python -m pytest -q
cd ..\frontend
npm ci
npm test -- --watchAll=false --runInBand
npm run build
```

Before each deployment:

1. Confirm no pipeline is running: `/health` must show `active: 0`.
2. Record the currently deployed commit and retain its artifact for rollback.
3. Confirm the Azure SQL firewall/private networking and Snowflake allow the App Service outbound addresses.
4. Confirm the frontend origin exactly matches `ATHENA_CORS_ORIGINS`.

## Post-deployment smoke test

1. `GET /health` returns HTTP 200 and one available background worker.
2. `GET /runs` returns without a 5xx response.
3. Run one small Snowflake pipeline through Bronze, merge-key review, Silver, and Gold.
4. Verify phases 3-5 show only one active frontier and no stale downstream stage.
5. Check Application Insights and Log Stream for startup, SQL, authentication, or Snowflake errors.

If startup or smoke testing fails, stop new submissions and redeploy the previous artifact. Do not retry deployment while a pipeline is active.
