# Shravan SFTP/ADLS Promotion Report

Date: 2026-08-12

## Scope

- Source branch: `origin/feature/sftp`
- Source commit: `f205d06` (`feat: add metadata-driven ADLS source flow`)
- QA merge: `b4d1bce`
- Main merge: `a328245`
- Post-merge frontend CI fix: `7f56069`
- Final intent: retain the existing main project-initiation UI while using Shravan's ADLS Silver merge-key workflow.

## Environment Audit

The final code adds a metadata-driven ADLS source flow. QA and main need the following deployment configuration for that flow.

### Operational values

| Variable | Purpose | Requirement |
| --- | --- | --- |
| `ADLS_ACCOUNT_URL` | ADLS Gen2 DFS endpoint | Set to the deployed storage account. Code has a project-specific default, which should not be relied on outside that account. |
| `ADLS_FILE_SYSTEM` | ADLS container | Set to the deployed container. Code defaults to `athena`. |
| `ADLS_SOURCE_ROOT` | Canonical folder scanned recursively | Set to the deployed source root. Code defaults to `INSURANCE_SFTP/insurance`. |
| `AZURE_TENANT_ID` | Service-principal tenant | Required with the other two service-principal values, unless the deployment uses managed identity/default Azure credentials. |
| `AZURE_CLIENT_ID` | Service-principal client | Same authentication rule as above. |
| `AZURE_CLIENT_SECRET` | Service-principal secret | Same authentication rule as above; store only in the deployment secret store. |
| `AZURE_OPENAI_API_KEY` | Merge-key LLM authentication | Required when using the default `azure_openai` provider. |
| `AZURE_OPENAI_ENDPOINT` | Azure OpenAI endpoint | Required when using the default provider. |
| `AZURE_OPENAI_DEPLOYMENT` | Default merge-key model/deployment | Required unless `ATHENA_ADLS_MERGE_KEY_LLM_MODEL` supplies a model. |
| `AZURE_OPENAI_API_VERSION` | Azure OpenAI API version | Optional; existing code supplies a default. |

Azure authentication is all-or-nothing for the three service-principal variables. Supplying only one or two raises a runtime error. Supplying none invokes `DefaultAzureCredential`, suitable for managed identity or another configured Azure credential source.

The ADLS merge-key resolver calls the LLM only when deterministic database-style resolution leaves a feed unresolved. Failure after all configured attempts leaves that feed pending with an error for review rather than silently approving an unvalidated key.

### Optional tuning

| Variable | Default |
| --- | ---: |
| `ADLS_MAX_DISCOVERED_FILES` | `10000` |
| `ADLS_SCHEMA_SAMPLE_BYTES` | `4194304` |
| `ADLS_SCHEMA_SAMPLE_ROWS` | `1000` |
| `ADLS_MERGE_KEY_MAX_CANDIDATE_COLUMNS` | `14` |
| `ADLS_MERGE_KEY_MAX_WIDTH` | `4` |
| `ADLS_MERGE_KEY_MAX_VALIDATED_CANDIDATES` | `32` |
| `ADLS_MERGE_KEY_MIN_SAMPLE_UNIQUENESS` | `0.98` |
| `ADLS_MERGE_KEY_MIN_SAMPLE_COMPLETENESS` | `1.0` |
| `ATHENA_ADLS_MERGE_KEY_LLM_PROVIDER` | `ATHENA_LLM_PROVIDER`, then `azure_openai` |
| `ATHENA_ADLS_MERGE_KEY_LLM_MODEL` | Existing Azure OpenAI deployment/model |
| `ATHENA_ADLS_MERGE_KEY_LLM_TIMEOUT_SECONDS` | `45` |
| `ADLS_MERGE_KEY_LLM_ATTEMPTS` | `3` |

No new frontend environment variables are required. Target catalog/schema variables continue to use their existing defaults and environment-specific values.

### Metadata control plane introduced with the promoted QA baseline

The final main delta also includes the metadata framework that was already present on QA and became part of the same promotion. These are not unique to Shravan's final commit, but current main code expects them:

| Variable | Current behavior |
| --- | --- |
| `ATHENA_TARGET_ENVIRONMENT` | Required for target metadata operations and must match each run's selected environment. Configure `qa` in QA and the intended production/main value in main. |
| `ATHENA_DATABRICKS_METADATA_CATALOG` | Required when generating or operating Databricks target metadata. |
| `ATHENA_SNOWFLAKE_METADATA_DATABASE` | Required when generating or operating Snowflake target metadata. |
| `DATABRICKS_SQL_WAREHOUSE_ID` | Required when Databricks metadata operations open the SQL repository. |
| `ATHENA_GENERATED_CODE_DIR` | Must identify durable shared storage before generated Bronze/Silver/Gold metadata can be activated. A local default is sufficient only before activation/local development. |
| `ATHENA_METADATA_ALLOW_ENV_SOURCE_FALLBACK` | Optional compatibility switch; defaults to disabled. |
| `DATABRICKS_SOURCE_SECRET_SCOPE` | Optional; defaults to `dataedge-secrets`. |
| `DATABRICKS_SOURCE_USERNAME_SECRET_KEY` | Optional; defaults to `azure-sql-username`. |
| `DATABRICKS_SOURCE_PASSWORD_SECRET_KEY` | Optional; defaults to `azure-sql-password`. |

`ATHENA_METADATA_SCHEMA` is present in `.env.example`, but the current repository factory selects `metadata_schema` for Databricks and `metadata` for Snowflake directly. Treat it as reserved/documentary unless another runtime path consumes it later.

Generated Databricks database-source scripts can also read `ATHENA_SOURCE_JDBC_URL` or legacy `SOURCE_JDBC_URL` when an approved connection does not provide a JDBC URL. This is a conditional execution fallback, not an ADLS requirement.

Other newly read variables are optional controls with code defaults: `ATHENA_GOLD_KPI_PARALLELISM=2`, `ATHENA_GOLD_MIN_MEASURE_MATCH_SCORE=10`, `ATHENA_GOLD_MIN_SUCCESS_RATIO=0.9`, `ATHENA_GOLD_MAX_JOIN_MULTIPLIER=1.05`, `ATHENA_SNOWFLAKE_NATIVE_WORKERS=4`, `ATHENA_SNOWFLAKE_RESUME_WAIT_SECONDS=300`, and `ATHENA_SNOWFLAKE_BRONZE_BATCH_SIZE=10000`. Catalog and schema variables retain their existing Databricks (`main`/`bronze`/`silver`/`gold`) and Snowflake (`ATHENA_DB`/`BRONZE`/`SILVER`/`GOLD`) defaults, but deployments should set them explicitly when those defaults are not the real target.

## QA Merge Conflicts

Merging `f205d06` into QA produced five conflicts.

| File | Conflict | Resolution |
| --- | --- | --- |
| `apps/backend/nodes/gold_gen.py` | Content | Integrated Shravan's metadata-driven path with QA safeguards: source/dimension limits, ordered parallel KPI generation, canonical column corrections, enriched dimensions, and compatible candidate validation. |
| `apps/backend/nodes/silver_gen.py` | Content | Integrated Shravan's Silver generation with QA protections: system-table filtering, rendered Databricks contract checks, reviewed source-key preservation, and effective type inference for common identifier/insurance fields. |
| `apps/backend/sftp_nodes/gold_code_generation.py` | Modify/delete | Accepted Shravan's deletion because the replacement flow uses `sftp_nodes/gold_generation.py` and shared generators. |
| `apps/backend/tests/test_file_source_execution_first.py` | Modify/delete | Accepted Shravan's deletion because the retired execution-first SFTP flow was replaced by metadata-driven ADLS tests. |
| `apps/backend/tests/test_gold_generation.py` | Content | Kept tests aligned with the integrated behavior, including ordered parallel generation and current source-table limits. |

Supporting conflict-resolution changes were made in `services/metadata_contracts.py`, `tests/test_silver_generation.py`, and `tests/test_snowflake_gold_runtime.py` to keep shared canonical-name and generated-contract behavior coherent.

## Main Merge Conflict

Merging QA into main produced one conflict in `apps/frontend/src/pages/ProjectInitiation.tsx`.

Resolution: kept the pre-existing main UI (`ours`) as requested. This preserved the existing layout, target selection, database controls, and SFTP/API presentation instead of taking Shravan's alternate ADLS project-form layout.

The incoming ADLS test expected the retained validator to require a selected ADLS connection. Commit `7f56069` updated only the shared validation condition so both SFTP and ADLS require `connectionName`; it did not replace the retained UI.

## Silver Merge-Key Decision

An intermediate compatibility commit (`94522ad`) temporarily restored the legacy Silver merge-key routing. The requirement was then clarified to allow Shravan's merge-key behavior while retaining our UI. Commit `8f927d2` reverted that compatibility commit before promotion to main.

Final behavior: Shravan's profile-validated ADLS merge-key resolver and review flow are active. The existing main project-initiation UI remains in place.

## Verification

- Backend focused generator suite: `79 passed` (`test_gold_generation.py` and `test_silver_generation.py`).
- Shared Silver merge-key checkpoint/API tests: `3 passed` during the intermediate compatibility check.
- Frontend regression suite for the CI failure: `NewRunModal.test.ts`, `3 passed` after `7f56069`.
- A broader local frontend run showed eight passing suites before the local 120-second command limit; CI remains the authoritative full frontend run.

## Deployment Checklist

1. Configure the operational ADLS, Azure identity, and Azure OpenAI values independently in QA and main secret/config stores.
2. Use either all three service-principal variables or managed identity; do not partially configure the service principal.
3. Confirm the identity has read/list access to the configured ADLS container and source root.
4. Confirm the Azure OpenAI deployment is reachable for unresolved Silver merge-key proposals.
5. Keep optional thresholds at defaults initially, then tune only from observed file size, schema, and key-candidate behavior.
6. Do not copy local `.env` secret values into source control or this report.
