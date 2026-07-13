# Environment Variables

This project currently deploys as one Azure App Service:

- FastAPI backend serves the API
- React frontend is built in GitHub Actions
- React build output is copied into `Athena_backend/static`
- The same App Service serves both UI and API

Because of that, production configuration is mostly backend App Service settings plus one GitHub Actions secret for deployment.

## 1. Required For GitHub Actions Deployment

Repository secret:

```text
AZURE_WEBAPP_PUBLISH_PROFILE
```

Value:

- Paste the full contents of the downloaded Azure publish profile XML

Used by:

- [main_Athenademo1.yml](C:/Users/vaibhavmalik/Athena%20agentic/.github/workflows/main_Athenademo1.yml)

Notes:

- Do not split the XML into multiple secrets
- Do not add `azure/login` when you are using publish profile deployment

## 2. Required In Azure App Service Configuration

Set these under:

- Azure Portal
- App Service
- `Configuration`
- `Application settings`

### 2.1 Core Azure OpenAI

```text
AZURE_OPENAI_API_KEY=<secret>
AZURE_OPENAI_ENDPOINT=https://<your-openai-resource>.openai.azure.com/
AZURE_OPENAI_API_VERSION=2024-12-01-preview
AZURE_OPENAI_DEPLOYMENT=gpt-4.1
AZURE_OPENAI_MODEL=gpt-4.1
```

### 2.2 Pipeline Database

This is the Athena system database that stores:

- run registry
- checkpoints
- review queues
- persistent AI artifacts

```text
AZURE_SQL_HOST=<server>.database.windows.net
AZURE_SQL_PORT=1433
AZURE_SQL_DRIVER=ODBC Driver 18 for SQL Server
AZURE_SQL_ENCRYPT=yes
AZURE_SQL_TRUST_SERVER_CERTIFICATE=no
AZURE_SQL_CONNECTION_TIMEOUT=30
AZURE_SQL_PIPELINE_DATABASE=<pipeline-db-name>
AZURE_SQL_PIPELINE_SCHEMA=metadata
AZURE_SQL_USERNAME=<sql-username>
AZURE_SQL_PASSWORD=<sql-password>
```

### 2.3 Source Database

This is the client or business data database Athena reads for discovery, schema, profiling, and SQL generation.

```text
AZURE_SQL_SOURCE_HOST=<server>.database.windows.net
AZURE_SQL_SOURCE_DATABASE=<source-db-name>
AZURE_SQL_SOURCE_SCHEMA=dbo
AZURE_SQL_SOURCE_USERNAME=<sql-username>
AZURE_SQL_SOURCE_PASSWORD=<sql-password>
```

### 2.4 CORS

For production on one App Service, set your production URL here.

```text
ATHENA_CORS_ORIGINS=https://astra-data-eecthacqb5eherhk.southindia-01.azurewebsites.net
```

For local + production together:

```text
ATHENA_CORS_ORIGINS=http://localhost:3000,http://127.0.0.1:3000,https://astra-data-eecthacqb5eherhk.southindia-01.azurewebsites.net
```

### 2.5 Pipeline Runtime Controls

```text
DEV_MODE=false
ATHENA_MIN_STAGE_RUNTIME_SECONDS=4
ATHENA_PIPELINE_JOB_TIMEOUT_SECONDS=3600
ATHENA_RUNS_ENDPOINT_TIMEOUT_SECONDS=5
ATHENA_RUNS_LIST_LIMIT=25
ATHENA_SQL_QUERY_TIMEOUT_SECONDS=5
ATHENA_BACKGROUND_WORKERS=2
ATHENA_SQL_CONNECT_RETRIES=3
ATHENA_SQL_CONNECT_RETRY_DELAY_SECONDS=1
ATHENA_SQL_TCP_PROBE_TIMEOUT_SECONDS=5
```

### 2.6 Logging

```text
ATHENA_SUPPRESS_CONSOLE=false
LOGTAIL_TOKEN=<optional-secret>
```

## 3. Optional Pinecone Settings

Required only if you want semantic memory, schema vector search, or domain knowledge base features.

```text
PINECONE_API_KEY=<secret>
PINECONE_INDEX_NAME=<main-index>
PINECONE_ENVIRONMENT=<pinecone-env>
PINECONE_SCHEMA_INDEX_NAME=<schema-index>
PINECONE_KNOWLEDGE_BASE_INDEX_NAME=knowledgebase
PINECONE_KB_INDEX_NAME=<optional-alias>
PINECONE_KNOWLEDGE_BASE_NAMESPACE=<optional-namespace>
```

Optional domain KB flags:

```text
ATHENA_USE_DOMAIN_KB=false
ATHENA_KB_ID=PC_Insurance_V1
ATHENA_DOMAIN_PROFILE=Insurance
ATHENA_KB_TOP_K_ENRICHMENT=8
ATHENA_KB_TOP_K_GOLD=10
ATHENA_KB_MAX_CHARS_ENRICHMENT=4000
ATHENA_KB_MAX_CHARS_GOLD=5000
```

Optional embedding gate:

```text
ATHENA_ENABLE_EMBEDDINGS=true
```

Optional embedding cache settings for deployed App Service:

```text
HF_HOME=/home/site/huggingface
SENTENCE_TRANSFORMERS_HOME=/home/site/huggingface
ATHENA_EMBEDDING_PRELOAD_REQUIRED=false
```

Recommended production behavior:

- Set `ATHENA_ENABLE_EMBEDDINGS=true`
- Set `HF_HOME` and `SENTENCE_TRANSFORMERS_HOME` to a persistent path
- Keep `ATHENA_EMBEDDING_PRELOAD_REQUIRED=false` for first rollout so the app stays up if Hugging Face download fails
- After the cache is warmed successfully, switch `ATHENA_EMBEDDING_PRELOAD_REQUIRED=true` if you want startup to fail hard when embeddings are unavailable

Verification after deployment:

- `GET /health`
- Confirm `embeddings.ready=true`
- Confirm `embeddings.env_enabled=true`
- If `ready=false`, inspect `embeddings.reason`, `sentence_transformer_error`, and `langchain_embedding_error`

## 4. Optional ADLS Settings

Required for the ADLS / data lake path.

```text
ADLS_ACCOUNT_URL=https://<storage-account>.dfs.core.windows.net
ADLS_FILE_SYSTEM=<container-or-filesystem>
ADLS_SOURCE_ROOT=<path-inside-filesystem>
ADLS_VENDOR_ROOT=<path-inside-filesystem>
ADLS_VENDOR_NAME=Vendor1
ADLS_ALLOWED_EXTENSIONS=csv,json,xml
```

If using service principal auth:

```text
AZURE_TENANT_ID=<tenant-id>
AZURE_CLIENT_ID=<client-id>
AZURE_CLIENT_SECRET=<client-secret>
```

## 5. Optional SFTP Settings

Required only for the SFTP ingestion path.

```text
ATHENA_SFTP_HOST=<host>
ATHENA_SFTP_USERNAME=<username>
ATHENA_SFTP_PASSWORD=<password>
ATHENA_SFTP_PORT=22
ATHENA_SFTP_PRIVATE_KEY_PATH=<optional-path>
ATHENA_SFTP_PRIVATE_KEY_PASSPHRASE=<optional-passphrase>
ATHENA_SFTP_LANDING_ROOT=/Volumes/sftp_landing
ATHENA_SFTP_HITL_AUTO=false
```

## 6. Optional Databricks / Medallion Settings

These affect bronze, silver, gold, and ADLS-generated script paths.

```text
BRONZE_CATALOG=main
BRONZE_SCHEMA=bronze
SILVER_CATALOG=main
SILVER_SCHEMA=silver
GOLD_SCHEMA=gold
DATABRICKS_VOLUME_CATALOG=main
DATABRICKS_VOLUME_SCHEMA=bronze
DATABRICKS_VOLUME_NAME=pipeline_artifacts
```

## 7. Optional LLM Feature Toggles

```text
ATHENA_LLM_PROVIDER=azure_openai
ATHENA_BRONZE_LLM_MODEL=<optional-model-name>
ATHENA_GOLD_LLM_PROVIDER=azure_openai
ATHENA_GOLD_LLM_MODEL=<optional-model-name>
ATHENA_KEYWORD_EXPANSION_MODEL=<optional-model-name>
ATHENA_ENABLE_LLM_KEYWORD_EXPANSION=true
ATHENA_ENABLE_LLM_BRONZE_ENHANCEMENT=false
ATHENA_GOLD_USE_LLM=false
USE_LLM=false
ATHENA_ENABLE_LLM_SFTP_SILVER=false
ATHENA_ENABLE_LLM_SFTP_GOLD=false
ATHENA_SFTP_SILVER_LLM_MODEL=<optional-model-name>
ATHENA_SFTP_GOLD_LLM_MODEL=<optional-model-name>
ATHENA_SFTP_SILVER_LLM_TIMEOUT_SECONDS=60
ATHENA_SFTP_GOLD_LLM_TIMEOUT_SECONDS=60
```

## 8. Local Backend `.env`

Use:

- [Athena_backend/.env.example](C:/Users/vaibhavmalik/Athena%20agentic/Athena_backend/.env.example)

Recommended local minimum:

```text
AZURE_OPENAI_API_KEY=<secret>
AZURE_OPENAI_ENDPOINT=https://<your-openai-resource>.openai.azure.com/
AZURE_OPENAI_API_VERSION=2024-12-01-preview
AZURE_OPENAI_DEPLOYMENT=gpt-4.1
AZURE_OPENAI_MODEL=gpt-4.1

AZURE_SQL_HOST=<server>.database.windows.net
AZURE_SQL_PORT=1433
AZURE_SQL_DRIVER=ODBC Driver 18 for SQL Server
AZURE_SQL_ENCRYPT=yes
AZURE_SQL_TRUST_SERVER_CERTIFICATE=no
AZURE_SQL_CONNECTION_TIMEOUT=30
AZURE_SQL_PIPELINE_DATABASE=<pipeline-db-name>
AZURE_SQL_PIPELINE_SCHEMA=metadata
AZURE_SQL_USERNAME=<sql-username>
AZURE_SQL_PASSWORD=<sql-password>

AZURE_SQL_SOURCE_HOST=<server>.database.windows.net
AZURE_SQL_SOURCE_DATABASE=<source-db-name>
AZURE_SQL_SOURCE_SCHEMA=dbo
AZURE_SQL_SOURCE_USERNAME=<sql-username>
AZURE_SQL_SOURCE_PASSWORD=<sql-password>

ATHENA_CORS_ORIGINS=http://localhost:3000,http://127.0.0.1:3000
DEV_MODE=false
ATHENA_MIN_STAGE_RUNTIME_SECONDS=4
```

## 9. Local Frontend `.env`

Create `frontend/.env.local` if needed:

```text
REACT_APP_API_BASE_URL=http://127.0.0.1:8000
```

Notes:

- Do not use `REACT_APP_API_ENDPOINT`
- Current frontend code uses `REACT_APP_API_BASE_URL`
- In Azure production build, the workflow sets `REACT_APP_API_BASE_URL=''` so the UI uses same-origin API calls

## 10. Current GitHub Actions Production Behavior

Workflow:

- [main_Athenademo1.yml](C:/Users/vaibhavmalik/Athena%20agentic/.github/workflows/main_Athenademo1.yml)

Current behavior:

- builds React with `REACT_APP_API_BASE_URL=''`
- copies frontend build into backend static folder
- deploys backend + static frontend together to one App Service
- health check uses:

```text
https://astra-data-eecthacqb5eherhk.southindia-01.azurewebsites.net/health
```

## 11. What To Configure Where

### GitHub Repository Secrets

```text
AZURE_WEBAPP_PUBLISH_PROFILE
```

### Azure App Service Application Settings

Set these at minimum:

```text
AZURE_OPENAI_API_KEY
AZURE_OPENAI_ENDPOINT
AZURE_OPENAI_API_VERSION
AZURE_OPENAI_DEPLOYMENT
AZURE_OPENAI_MODEL
AZURE_SQL_HOST
AZURE_SQL_PORT
AZURE_SQL_DRIVER
AZURE_SQL_ENCRYPT
AZURE_SQL_TRUST_SERVER_CERTIFICATE
AZURE_SQL_CONNECTION_TIMEOUT
AZURE_SQL_PIPELINE_DATABASE
AZURE_SQL_PIPELINE_SCHEMA
AZURE_SQL_USERNAME
AZURE_SQL_PASSWORD
AZURE_SQL_SOURCE_HOST
AZURE_SQL_SOURCE_DATABASE
AZURE_SQL_SOURCE_SCHEMA
AZURE_SQL_SOURCE_USERNAME
AZURE_SQL_SOURCE_PASSWORD
ATHENA_CORS_ORIGINS
DEV_MODE
ATHENA_MIN_STAGE_RUNTIME_SECONDS
```

Add these too if you use those features:

```text
PINECONE_API_KEY
PINECONE_INDEX_NAME
PINECONE_ENVIRONMENT
PINECONE_SCHEMA_INDEX_NAME
ADLS_ACCOUNT_URL
ADLS_FILE_SYSTEM
ADLS_SOURCE_ROOT
AZURE_TENANT_ID
AZURE_CLIENT_ID
AZURE_CLIENT_SECRET
```

## 12. Security

Your local backend `.env` currently contains real secrets.

You should rotate these immediately:

- Azure OpenAI key
- Azure SQL password
- Azure client secret
- Pinecone API key

Reason:

- those credentials were exposed in plaintext locally and should be treated as compromised

## 13. Quick Setup Checklist

1. Add `AZURE_WEBAPP_PUBLISH_PROFILE` in GitHub repository secrets.
2. Open Azure App Service `Configuration`.
3. Add all required `AZURE_OPENAI_*` settings.
4. Add all required `AZURE_SQL_*` settings.
5. Add `ATHENA_CORS_ORIGINS` with your Azure site URL.
6. Add Pinecone settings if semantic memory is required.
7. Add ADLS settings if data lake ingestion is required.
8. Save and restart the App Service.
9. Push to `main`.
10. Verify:
```text
https://astra-data-eecthacqb5eherhk.southindia-01.azurewebsites.net/health
```
