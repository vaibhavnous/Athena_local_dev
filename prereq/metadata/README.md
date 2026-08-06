# Target metadata prerequisite

Run this setup once per target environment before onboarding a source. The
scripts create only the eight tables defined by `metadata-README.md`.

## Databricks

Replace `__TARGET_CATALOG__` in `databricks.sql` with an existing Unity Catalog
catalog. Execute the rendered script through a SQL warehouse with permission to
create the `metadata` schema and tables.

Required bootstrap settings:

- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`
- `DATABRICKS_SQL_WAREHOUSE_ID`
- `ATHENA_DATABRICKS_METADATA_CATALOG`

## Snowflake

Replace `__TARGET_DATABASE__` in `snowflake.sql` with an existing database. Run
the rendered script using a role that may create the `METADATA` schema and its
tables.

Required bootstrap settings:

- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_WAREHOUSE`
- `ATHENA_SNOWFLAKE_METADATA_DATABASE`

## Fabric

Fabric is intentionally not represented by a pretend SQL script. This codebase
does not yet define whether the metadata target is a Fabric Warehouse or a
Lakehouse/Spark catalog, and it has no Fabric client or runtime. See
`fabric/README.md` for the decision required before implementing that adapter.

## Rules

- Do not put credentials in these files.
- Bootstrap is idempotent (`CREATE ... IF NOT EXISTS`).
- Application preflight must validate every required table and column before
  onboarding or runtime work.
- IDs are supplied by the repository's deterministic, collision-checked
  allocator; `MAX(id) + 1` is prohibited.
- Configuration rows remain inactive until their JSON contracts and
  relationships validate.
- Set `ATHENA_TARGET_ENVIRONMENT` to the one environment served by the backend
  deployment. Requests for another environment fail closed.
- Run onboarding as a single-writer administrative operation until live
  platform concurrency tests are deployment-validated.
- A connection remains an inactive draft until real secret resolution and a
  source connectivity test succeed; syntactically valid JSON does not activate it.

For a Snowflake target, the Azure SQL connector runs from the backend and reuses
the deployment's `AZURE_SQL_SOURCE_*` settings. Its `secrets_json` must contain
references—not values—in this form:

```json
{
  "username": {"scope": "DEPLOYMENT_ENV", "key": "AZURE_SQL_SOURCE_USERNAME"},
  "password": {"scope": "DEPLOYMENT_ENV", "key": "AZURE_SQL_SOURCE_PASSWORD"}
}
```

For a Databricks target, create one secret scope per environment, for example
`astra-qa-source-secrets`, and add exactly these BASIC-auth source secrets:

```text
claims-db-username = <database login username>
claims-db-password = <database login password>
```

Reference only their names in `cfg_connection.secrets_json`:

```json
{
  "username": {"scope": "astra-qa-source-secrets", "key": "claims-db-username"},
  "password": {"scope": "astra-qa-source-secrets", "key": "claims-db-password"}
}
```

Grant the Databricks job's run-as service principal `READ` access to the scope.
Host, port, database, driver and credential-free JDBC URL template remain in
`cfg_connection`; Databricks and source credentials must not be stored there.
Onboarding validates that both secret keys exist and runs a separate `SELECT 1`
through the backend source client. Generated jobs resolve values only inside
Databricks through `dbutils.secrets.get`.

The endpoint in the connection draft must match the configured source host,
port, and database. The administrator command activates the draft only after a
successful `SELECT 1`; failures leave it inactive.

For access control, `config_json.allowed_project_ids` is required. List the
application project IDs permitted to use the source, or use `["*"]` only when
the connection is intentionally shared across every authenticated project.

## Runtime readiness boundary

The database-source design flow, exact mapping-version selection, artifact
registration, and target-resident queue/control lifecycle are implemented.
FULL/stateless Source-to-Bronze execution consumes the immutable queued runtime
context and uses `logical_work_id` as its replay identity. Databricks resolves
JDBC credentials inside the job and replaces only that logical work. Snowflake
loads through the validated deployment connector into a session-local temporary
landing table, then transactionally replaces only that logical work in Bronze.
These paths are implementation-complete locally, but are not deployment-validated
until their live target checks pass.

Silver and Gold consume only the same `logical_work_id` released by their exact
upstream dependencies. Databricks success requires a matching Delta commit
metadata/version receipt. Snowflake success requires the target query receipt;
Bronze additionally reconciles loaded and committed row counts. A reclaimed
RUNNING queue item resumes the same attempt and target submission identity.
If Databricks accepts a job but its submission receipt cannot be persisted, the
lease is released without creating a new attempt; reclaim reuses the same
Databricks idempotency token and run identity.
Every control-table mutation is fenced by the current worker lease. Snowflake
enqueue uses the active ingestion-object row to serialize duplicate logical-work
requests in one transaction. Reclaimed Snowflake attempts use a queue/attempt/run
query tag and wait for a prior tagged query before replay; the runtime role
therefore requires access to `INFORMATION_SCHEMA.QUERY_HISTORY`.

Blocking validation is rule-level rather than a generic success flag. Silver
reports mapped-column, target-schema, and null-key observations. Gold reports
input/schema observations and the actual join multiplier configured by
`ATHENA_GOLD_MAX_JOIN_MULTIPLIER`; returned rules must exactly match the active
`validation_policy_json` before finalization.
Gold key policies are explicit `KEYS_NOT_NULL` and `KEYS_UNIQUE` rules with
observed target counts. Multi-input Snowflake Gold requires a logical-work
predicate on every approved input alias. Metadata Gold executes the registered,
hash-verified SQL bytes without runtime normalization or schema-evolution
injection.

Checkpoint state, runtime error details, structured log fields, log messages,
and tracebacks use the same recursive credential redactor before persistence or
emission. Approved `{scope, key}` secret references remain usable; secret values
are never retained.

The current Snowflake database connector is intentionally deployment-bound: one
backend deployment serves the one `AZURE_SQL_SOURCE_*` endpoint whose fingerprint
must match the active `cfg_connection`. Supporting multiple dynamic Snowflake-side
JDBC sources requires an approved target-aware secret resolver; the runtime does
not silently fall back to another connection.

Production execution deliberately fails closed for these unfinished contracts:

- Stateful/watermarked execution until artifacts return a typed candidate
  checkpoint and watermark recovery/non-regression is deployment-validated.
- Snowflake metadata-driven dbt Gold until its exact mapping/write contract has
  a native runtime and live target validation. Native SQL multi-input Gold is
  supported through exact dependency/join pins and rule-level join checks.
- Fabric until Warehouse versus Lakehouse and a supported client/runtime are
  selected.

Keep `ATHENA_EXECUTE_SNOWFLAKE_BRONZE` disabled until live source/target
connectivity is ready. Deployment validation must cover target transactions,
concurrent queue claims, duplicate submission, retry after target commit,
temporary-landing isolation, artifact hash rejection, and secret redaction.
Snowflake deployment validation must also include two-session enqueue/claim
testing, query-tag recovery, and serialization on the ingestion-object row;
local unit tests cannot prove Snowflake transaction and lock behavior.
