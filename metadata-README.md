# Generic Ingestion Metadata Framework

This README proposes an updated metadata contract for the existing generic
ingestion model. It keeps the current design constraint: no additional metadata
tables are introduced. Production controls are added as columns on the existing
tables and as documented runtime behavior.

The target runtime must support:

- Database ingestion through JDBC-style connections.
- File ingestion from SFTP, ADLS, and S3.
- API ingestion from REST-style endpoints.
- Source-to-Bronze, Bronze-to-Silver, and Silver-to-Gold processing.
- Deterministic retries, backfills, validation gates, idempotent writes, and
  safe watermark commits.

## Design Position

The current DDL is a good starter catalog, but a production metadata-driven
framework needs the metadata to answer these questions before execution starts:

- What exact source, payload, and parser contract should be used?
- Which stage is being executed?
- Which source layer and target layer are involved?
- Which mappings apply to this stage and target?
- Which write strategy should be used?
- Which data-quality rules are warnings, row rejects, quarantines, or run
  failures?
- Which bounded data scope is being requested?
- Which metadata version was used by the run?
- Which queue request produced each execution attempt?
- Can a retry safely detect that target data was already written?
- Can the watermark be committed without racing another run?

Because no new tables are being added, several complex contracts are expressed
as governed JSON columns. These JSON columns must be treated as strict schemas,
not free-form notes.

## Updated DDLs

The DDL below is written for Databricks SQL / Delta tables. JSON payloads are
stored as `STRING` columns so the contract remains compatible with standard
Delta tables. The runtime should validate each JSON column against a documented
schema before activating metadata.

```sql
CREATE SCHEMA IF NOT EXISTS metadata;

CREATE TABLE IF NOT EXISTS metadata.cfg_source_system (
    source_system_id BIGINT,
    source_system_name STRING,
    business_domain STRING,
    owner_name STRING,
    owner_email STRING,
    description STRING,
    active_flag BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS metadata.cfg_connection (
    connection_id BIGINT,
    source_system_id BIGINT,
    connection_name STRING,
    connection_type STRING,          -- SFTP, ADLS, S3, JDBC, REST_API
    connection_contract_name STRING, -- Contract name understood by runtime
    connection_schema_version STRING,
    host_name STRING,
    port INT,
    base_path STRING,                -- Folder, bucket/prefix, or landing path
    base_url STRING,                 -- API base URL
    database_name STRING,
    auth_type STRING,                -- BASIC, TOKEN, OAUTH, KEY, MANAGED_IDENTITY, SERVICE_PRINCIPAL
    secret_scope STRING,
    secret_key STRING,               -- Legacy/single-secret reference
    secrets_json STRING,             -- Multi-secret references by logical name
    config_json STRING,              -- Source-specific options, schema validated
    config_hash STRING,              -- Hash of executable connection contract
    config_version INT,
    effective_from TIMESTAMP,
    effective_to TIMESTAMP,
    is_current BOOLEAN,
    active_flag BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS metadata.cfg_ingestion_object (
    ingestion_object_id BIGINT,
    source_system_id BIGINT,
    connection_id BIGINT,
    object_kind STRING,              -- INGESTION, TRANSFORMATION
    ingestion_type STRING,           -- FILE, DATABASE, API
    processing_stage STRING,         -- SOURCE_TO_BRONZE, BRONZE_TO_SILVER, SILVER_TO_GOLD
    source_layer STRING,             -- SOURCE, BRONZE, SILVER, GOLD
    target_layer STRING,             -- BRONZE, SILVER, GOLD
    object_name STRING,
    object_type STRING,              -- Legacy field: CSV, JSON, XML, PARQUET, TABLE, QUERY, ENDPOINT
    source_resource_type STRING,     -- FILE, TABLE, QUERY, ENDPOINT
    payload_format STRING,           -- CSV, JSON, XML, PARQUET, AVRO, DELTA, TEXT
    container_format STRING,         -- NONE, GZIP, ZIP, TAR, TAR_GZIP, BZIP2
    source_path STRING,
    file_pattern STRING,
    database_schema STRING,
    table_name STRING,
    query_text STRING,
    endpoint_path STRING,
    http_method STRING,
    request_headers_json STRING,
    request_params_json STRING,
    request_body_template STRING,
    response_root_path STRING,
    pagination_type STRING,          -- NONE, OFFSET, PAGE_NUMBER, NEXT_LINK, CURSOR
    pagination_config_json STRING,
    parser_options_json STRING,
    normalization_options_json STRING,
    schema_inference_policy STRING,  -- DISABLED, SAMPLE, FULL_SCAN
    schema_evolution_policy STRING,  -- FAIL, ADD_COLUMNS, RESCUE, CAST_COMPATIBLE
    load_type STRING,                -- FULL, INCREMENTAL, CDC
    watermark_column STRING,
    boundary_operator STRING,        -- >, >=, =, CURSOR_AFTER
    tie_breaker_columns_json STRING,
    sort_columns_json STRING,
    lookback_interval STRING,        -- Example: 2 HOURS, 7 DAYS
    checkpoint_type STRING,          -- SINGLE_COLUMN, COMPOSITE, CDC_VERSION, API_CURSOR, FILE_MODIFIED_TIME
    target_bronze_table STRING,
    target_silver_table STRING,
    target_gold_table STRING,
    target_table STRING,             -- Resolved target for the current processing_stage
    write_mode STRING,               -- APPEND, OVERWRITE, MERGE, SCD1, SCD2, SNAPSHOT_REPLACE
    merge_keys_json STRING,
    dedupe_keys_json STRING,
    partition_columns_json STRING,
    delete_strategy STRING,          -- IGNORE, HARD_DELETE, SOFT_DELETE, CDC_DELETE
    scd_config_json STRING,
    dependency_objects_json STRING,  -- Upstream object/stage requirements
    validation_policy_json STRING,   -- Object-level validation policy
    execution_spec_json STRING,      -- Runtime engine, artifact/entry point, hash, and generator contract
    config_hash STRING,
    config_version INT,
    effective_from TIMESTAMP,
    effective_to TIMESTAMP,
    is_current BOOLEAN,
    active_flag BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS metadata.cfg_mapping (
    mapping_id BIGINT,
    ingestion_object_id BIGINT,
    processing_stage STRING,          -- SOURCE_TO_BRONZE, BRONZE_TO_SILVER, SILVER_TO_GOLD
    source_layer STRING,
    target_layer STRING,
    source_object_name STRING,
    target_object_name STRING,
    target_table STRING,
    mapping_group STRING,
    transformation_group STRING,
    input_objects_json STRING,        -- Multi-input sources for joins/aggregations
    join_rules_json STRING,
    aggregation_rules_json STRING,
    build_order INT,
    source_field_path STRING,         -- DB column, CSV column, XML path, or JSON path
    source_data_type STRING,
    target_column_name STRING,
    target_data_type STRING,
    is_nullable BOOLEAN,
    is_array BOOLEAN,
    is_primary_key BOOLEAN,
    transformation_rule STRING,
    transformation_language STRING,   -- SQL, PYSPARK_EXPR, JSONPATH, NONE
    default_value STRING,
    ordinal_position INT,
    validation_rule_json STRING,
    severity STRING,                  -- INFO, WARNING, ERROR, CRITICAL
    failure_action STRING,            -- WARN, REJECT_RECORD, QUARANTINE_BATCH, FAIL_RUN
    threshold_value DOUBLE,
    threshold_unit STRING,            -- COUNT, PERCENT
    stop_watermark_on_failure BOOLEAN,
    mapping_hash STRING,
    mapping_version INT,
    effective_from TIMESTAMP,
    effective_to TIMESTAMP,
    is_current BOOLEAN,
    active_flag BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS metadata.ctl_ingestion_queue (
    queue_id BIGINT,
    ingestion_object_id BIGINT,
    trigger_type STRING,             -- MANUAL, SCHEDULED, FILE_ARRIVAL, API_POLL, CDC
    queue_status STRING,             -- PENDING, RUNNING, SUCCESS, FAILED, SKIPPED, RETRY_WAIT
    priority INT,
    logical_work_id STRING,          -- Stable identity for the logical work request
    idempotency_key STRING,          -- Deterministic key for duplicate-trigger protection
    work_scope_json STRING,          -- Date range, partition, batch, files, cursor, replay scope
    requested_start_boundary STRING,
    requested_end_boundary STRING,
    partition_spec_json STRING,
    batch_id STRING,
    requested_by STRING,
    manual_override_json STRING,
    requested_at TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    claimed_by_worker_id STRING,
    lease_acquired_at TIMESTAMP,
    lease_expires_at TIMESTAMP,
    last_heartbeat_at TIMESTAMP,
    attempt_count INT,
    max_attempts INT,
    next_retry_at TIMESTAMP,
    retry_policy_json STRING,
    metadata_snapshot_id STRING,
    run_id STRING,                   -- Latest/final run id for convenience
    message STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS metadata.ctl_run (
    run_id STRING,
    queue_id BIGINT,
    attempt_number INT,
    logical_work_id STRING,
    idempotency_key STRING,
    ingestion_object_id BIGINT,
    source_system_id BIGINT,
    connection_id BIGINT,
    ingestion_type STRING,
    processing_stage STRING,
    source_layer STRING,
    target_layer STRING,
    target_table STRING,
    write_mode STRING,
    notebook_name STRING,
    pipeline_name STRING,
    metadata_snapshot_id STRING,
    connection_config_version INT,
    ingestion_object_config_version INT,
    mapping_version INT,
    metadata_hash STRING,
    source_boundary_hash STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status STRING,                   -- SUCCESS, FAILED, RUNNING, SKIPPED
    current_phase STRING,            -- CONFIG_RESOLVED, EXTRACTED, VALIDATED, TARGET_WRITTEN, WATERMARK_COMMITTED, FINALIZED
    phase_status_json STRING,
    rows_read BIGINT,
    rows_written BIGINT,
    files_processed BIGINT,
    bytes_processed BIGINT,
    execution_time_seconds DOUBLE,
    target_write_id STRING,
    target_commit_status STRING,     -- NOT_STARTED, STARTED, COMMITTED, FAILED, SKIPPED
    validation_status STRING,        -- NOT_STARTED, PASSED, WARNING, FAILED
    validation_summary_json STRING,
    watermark_before STRING,
    watermark_after STRING,
    watermark_commit_status STRING,  -- NOT_STARTED, CANDIDATE_STAGED, COMMITTED, FAILED, SKIPPED
    recovery_action STRING,
    created_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS metadata.ctl_error_log (
    error_id STRING,
    run_id STRING,
    queue_id BIGINT,
    attempt_number INT,
    ingestion_object_id BIGINT,
    source_system_id BIGINT,
    connection_id BIGINT,
    error_stage STRING,              -- CONNECT, READ, PARSE, VALIDATE, TRANSFORM, WRITE, WATERMARK, FINALIZE
    error_phase STRING,
    error_code STRING,
    error_message STRING,
    error_detail STRING,
    severity STRING,                 -- INFO, WARNING, ERROR, CRITICAL
    retryable_flag BOOLEAN,
    retry_action STRING,
    error_time TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS metadata.ctl_watermark (
    watermark_id BIGINT,
    ingestion_object_id BIGINT,
    watermark_type STRING,           -- TIMESTAMP, ID, CDC_VERSION, API_CURSOR, FILE_MODIFIED_TIME
    watermark_column STRING,
    last_watermark_value STRING,     -- Legacy alias for committed_watermark_value
    committed_watermark_value STRING,
    candidate_watermark_value STRING,
    candidate_run_id STRING,
    candidate_created_at TIMESTAMP,
    last_successful_run_id STRING,
    watermark_version BIGINT,
    commit_status STRING,            -- COMMITTED, CANDIDATE_STAGED, FAILED
    boundary_operator STRING,
    tie_breaker_value_json STRING,
    checkpoint_state_json STRING,
    committed_at TIMESTAMP,
    updated_at TIMESTAMP
) USING DELTA;
```

## JSON Contract Guidance

The framework should publish JSON schemas for each governed JSON column. A row
should not be activated unless its JSON contracts validate.

Important JSON columns:

- `cfg_connection.secrets_json`
- `cfg_connection.config_json`
- `cfg_ingestion_object.parser_options_json`
- `cfg_ingestion_object.normalization_options_json`
- `cfg_ingestion_object.pagination_config_json`
- `cfg_ingestion_object.validation_policy_json`
- `cfg_ingestion_object.dependency_objects_json`
- `cfg_ingestion_object.execution_spec_json`
- `cfg_mapping.input_objects_json`
- `cfg_mapping.join_rules_json`
- `cfg_mapping.aggregation_rules_json`
- `cfg_mapping.validation_rule_json`
- `ctl_ingestion_queue.work_scope_json`
- `ctl_ingestion_queue.retry_policy_json`
- `ctl_run.phase_status_json`
- `ctl_run.validation_summary_json`
- `ctl_watermark.checkpoint_state_json`

## Connection Contracts

### JDBC / Database

`cfg_connection.connection_type = 'JDBC'`

Required metadata:

- `host_name`
- `port`
- `database_name`
- `auth_type`
- `secrets_json`
- `config_json`

Example `secrets_json`:

```json
{
  "username": {"scope": "source-secrets", "key": "crm-db-user"},
  "password": {"scope": "source-secrets", "key": "crm-db-password"}
}
```

Example `config_json`:

```json
{
  "jdbc_driver": "org.postgresql.Driver",
  "jdbc_url_template": "jdbc:postgresql://{host_name}:{port}/{database_name}",
  "fetch_size": 10000,
  "query_timeout_seconds": 900,
  "partition_column": "customer_id",
  "lower_bound": 1,
  "upper_bound": 10000000,
  "num_partitions": 16
}
```

### SFTP

`cfg_connection.connection_type = 'SFTP'`

Required metadata:

- `host_name`
- `port`
- `base_path`
- `auth_type`
- `secrets_json`
- `config_json`

Example `secrets_json`:

```json
{
  "username": {"scope": "source-secrets", "key": "sftp-user"},
  "private_key": {"scope": "source-secrets", "key": "sftp-private-key"},
  "passphrase": {"scope": "source-secrets", "key": "sftp-key-passphrase"}
}
```

Example `config_json`:

```json
{
  "known_hosts_secret": {"scope": "source-secrets", "key": "sftp-known-hosts"},
  "recursive_listing": true,
  "archive_after_success": true,
  "archive_path": "/archive",
  "delete_after_success": false,
  "max_files_per_run": 500
}
```

### ADLS

`cfg_connection.connection_type = 'ADLS'`

Required metadata:

- `base_path`
- `auth_type`
- `secrets_json` when not using managed identity
- `config_json`

Example `config_json`:

```json
{
  "account_name": "rawstorageacct",
  "container": "landing",
  "path_style": "abfss",
  "recursive_listing": true,
  "max_files_per_run": 1000
}
```

### S3

`cfg_connection.connection_type = 'S3'`

Required metadata:

- `base_path`
- `auth_type`
- `secrets_json` or instance/profile configuration
- `config_json`

Example `config_json`:

```json
{
  "bucket": "raw-landing",
  "prefix": "erp/orders/",
  "region": "us-east-1",
  "recursive_listing": true,
  "max_files_per_run": 1000
}
```

### REST API

`cfg_connection.connection_type = 'REST_API'`

Required metadata:

- `base_url`
- `auth_type`
- `secrets_json`
- `config_json`

Example `secrets_json`:

```json
{
  "token": {"scope": "source-secrets", "key": "orders-api-token"}
}
```

Example `config_json`:

```json
{
  "rate_limit_per_minute": 300,
  "timeout_seconds": 120,
  "retry_status_codes": [429, 500, 502, 503, 504],
  "max_retries": 5,
  "backoff_strategy": "EXPONENTIAL"
}
```

## Ingestion Object Contracts

### Object Kind

`cfg_ingestion_object.object_kind` separates external source ingestion from
internal target-platform transformations:

- `INGESTION`: reads from an external source connection. `ingestion_type` and
  `connection_id` are required, and `processing_stage` is normally
  `SOURCE_TO_BRONZE`.
- `TRANSFORMATION`: reads target-platform Bronze or Silver inputs. The runtime
  must not dispatch an external connector. `ingestion_type` and `connection_id`
  may be null because inputs are resolved through `source_layer`,
  `cfg_mapping.input_objects_json`, and
  `cfg_ingestion_object.dependency_objects_json`.

This allows each independently executable Bronze, Silver, fact, or dimension
target to have its own ingestion object, write strategy, dependencies, retry
boundary, and execution artifact without pretending that internal processing
uses the original external connection.

### Database Table Or Query

Database ingestion uses:

- `object_kind = 'INGESTION'`
- `ingestion_type = 'DATABASE'`
- `source_resource_type = 'TABLE'` or `QUERY`
- `database_schema`, `table_name`, or `query_text`
- `load_type`
- `watermark_column`
- `boundary_operator`
- `checkpoint_type`
- target table and `write_mode`

For table-based ingestion, the runtime builds a source query from
`database_schema`, `table_name`, and the incremental boundary metadata.

For query-based ingestion, `query_text` must be written so the runtime can
inject bounded parameters safely. The implementation should use parameterized
queries or a controlled placeholder format, not string concatenation.

Example incremental boundary:

```json
{
  "work_scope_json": {
    "mode": "INCREMENTAL",
    "start_watermark": "2026-08-01T00:00:00Z",
    "end_watermark": "2026-08-02T00:00:00Z"
  }
}
```

### SFTP / ADLS / S3 Files

File ingestion uses:

- `object_kind = 'INGESTION'`
- `ingestion_type = 'FILE'`
- `source_resource_type = 'FILE'`
- `source_path`
- `file_pattern`
- `payload_format`
- `container_format`
- `parser_options_json`
- `normalization_options_json`
- target table and `write_mode`

The runtime should first resolve the bounded file list for the queue request.
The resolved list should be stored in `ctl_ingestion_queue.work_scope_json` or
derived from it in a deterministic way.

Example `parser_options_json` for CSV:

```json
{
  "header": true,
  "delimiter": ",",
  "quote": "\"",
  "escape": "\"",
  "multiLine": false,
  "mode": "PERMISSIVE",
  "encoding": "UTF-8"
}
```

Example `normalization_options_json`:

```json
{
  "trim_strings": true,
  "empty_string_as_null": true,
  "add_metadata_columns": true,
  "metadata_columns": [
    "_source_file",
    "_source_file_modified_at",
    "_ingestion_run_id",
    "_ingested_at"
  ]
}
```

### REST API Endpoint

API ingestion uses:

- `object_kind = 'INGESTION'`
- `ingestion_type = 'API'`
- `source_resource_type = 'ENDPOINT'`
- `endpoint_path`
- `http_method`
- `request_headers_json`
- `request_params_json`
- `request_body_template`
- `pagination_type`
- `pagination_config_json`
- `response_root_path`
- `payload_format`
- target table and `write_mode`

Example `pagination_config_json` for page-number pagination:

```json
{
  "page_param": "page",
  "page_size_param": "pageSize",
  "page_size": 500,
  "start_page": 1,
  "stop_condition": "EMPTY_PAGE",
  "max_pages": 10000
}
```

Example `pagination_config_json` for cursor pagination:

```json
{
  "cursor_param": "cursor",
  "next_cursor_json_path": "$.nextCursor",
  "records_json_path": "$.items",
  "stop_condition": "NO_NEXT_CURSOR"
}
```

### Execution Specification

`cfg_ingestion_object.execution_spec_json` is the source-neutral contract that
connects an approved ingestion object and metadata version to the executable
runtime artifact. It prevents a worker from reading an unversioned "latest"
artifact or depending on `ai_store` during data execution.

Example for generated target-platform code:

```json
{
  "contract_version": "1.0",
  "execution_mode": "GENERATED_ARTIFACT",
  "target_platform": "DATABRICKS",
  "engine": "DATABRICKS_JOB",
  "artifact_uri": "/Workspace/Shared/astra/generated/claims_source_to_bronze",
  "entry_point": "main",
  "artifact_hash": "sha256:8be42c...",
  "generator_version": "astra-codegen-1.0.0"
}
```

The same contract can represent a generic metadata interpreter:

```json
{
  "contract_version": "1.0",
  "execution_mode": "METADATA_INTERPRETER",
  "target_platform": "SNOWFLAKE",
  "engine": "SNOWPARK_WORKER",
  "entry_point": "generic_ingestion_worker",
  "artifact_hash": "sha256:31fd90...",
  "generator_version": "astra-runtime-1.0.0"
}
```

Rules:

- Draft objects may omit `execution_spec_json` while mappings and code are
  still being designed.
- An active executable object must have a valid execution specification.
- `artifact_hash` must be verified before execution and included in the
  object's `config_hash` and the run's combined `metadata_hash`.
- `artifact_uri` and `entry_point` must be selected through an allow-listed
  target-platform adapter. They must not contain credentials.
- `object_kind = 'TRANSFORMATION'` uses the same contract, but its inputs come
  from target-platform tables rather than an external connector.

## Stage And Mapping Selection

The runtime must never load mappings only by `ingestion_object_id`. It must
also filter by:

- `processing_stage`
- `source_layer`
- `target_layer`
- `target_table`
- active/effective version columns
- `metadata_snapshot_id` or metadata hash resolved for the run

This prevents a Bronze-to-Silver mapping from being used during
Silver-to-Gold processing.

Example stage values:

```text
SOURCE_TO_BRONZE
BRONZE_TO_SILVER
SILVER_TO_GOLD
```

For complex Gold builds, use these `cfg_mapping` columns:

- `transformation_group`
- `input_objects_json`
- `join_rules_json`
- `aggregation_rules_json`
- `build_order`

Example `input_objects_json`:

```json
[
  {"alias": "o", "object_name": "silver.orders", "required": true},
  {"alias": "c", "object_name": "silver.customer", "required": true},
  {"alias": "p", "object_name": "silver.product", "required": true},
  {"alias": "pay", "object_name": "silver.payment", "required": true}
]
```

Example `join_rules_json`:

```json
[
  {
    "left_alias": "o",
    "right_alias": "c",
    "join_type": "left",
    "condition": "o.customer_id = c.customer_id"
  },
  {
    "left_alias": "o",
    "right_alias": "p",
    "join_type": "left",
    "condition": "o.product_id = p.product_id"
  }
]
```

## Target Write Strategy

`cfg_ingestion_object.write_mode` determines the writer implementation.

Supported modes:

- `APPEND`: insert all output rows.
- `OVERWRITE`: replace the full target table or scoped partition.
- `MERGE`: upsert by `merge_keys_json`.
- `SCD1`: type 1 dimension update by business key.
- `SCD2`: type 2 dimension history using `scd_config_json`.
- `SNAPSHOT_REPLACE`: replace the current snapshot for a bounded scope.

Example `merge_keys_json`:

```json
["customer_id"]
```

Example `scd_config_json`:

```json
{
  "business_keys": ["customer_id"],
  "effective_from_column": "effective_from",
  "effective_to_column": "effective_to",
  "current_flag_column": "is_current",
  "change_hash_column": "record_hash",
  "delete_handling": "EXPIRE"
}
```

Target writes must set `ctl_run.target_write_id` and update
`ctl_run.target_commit_status`. Append-style writers must use
`logical_work_id`, `idempotency_key`, or `target_write_id` to avoid duplicate
inserts on retry.

## Validation And Failure Gates

Validation can be defined at two levels:

- Object-level policy in `cfg_ingestion_object.validation_policy_json`.
- Column or mapping-level rule in `cfg_mapping.validation_rule_json`.

Example object validation policy:

```json
{
  "rules": [
    {
      "name": "max_type_cast_failures",
      "rule_type": "TYPE_CAST_FAILURE_RATE",
      "threshold_value": 1.0,
      "threshold_unit": "PERCENT",
      "failure_action": "FAIL_RUN",
      "stop_watermark_on_failure": true
    },
    {
      "name": "reject_null_customer_id",
      "rule_type": "NOT_NULL",
      "columns": ["customer_id"],
      "failure_action": "REJECT_RECORD",
      "threshold_value": 0,
      "threshold_unit": "COUNT"
    }
  ]
}
```

Runtime behavior:

1. Execute validation before target commit unless policy explicitly says
   otherwise.
2. Record results in `ctl_run.validation_status` and
   `ctl_run.validation_summary_json`.
3. If any rule has `stop_watermark_on_failure = true`, do not advance the
   watermark.
4. If records are rejected or quarantined, include counts and locations in the
   validation summary.

## Queue And Retry Semantics

Queue rows must identify exact work scope. A manual backfill, replay, or retry
must not rely on the latest object defaults.

Example `work_scope_json` for a date-range backfill:

```json
{
  "scope_type": "DATE_RANGE",
  "start": "2026-01-01",
  "end": "2026-03-31",
  "reason": "manual_backfill",
  "requested_by": "data_ops"
}
```

Example `work_scope_json` for file ingestion:

```json
{
  "scope_type": "FILE_LIST",
  "files": [
    "landing/orders/orders_20260801.csv",
    "landing/orders/orders_20260802.csv"
  ]
}
```

Example `retry_policy_json`:

```json
{
  "max_attempts": 3,
  "retry_delay_seconds": 300,
  "backoff_strategy": "EXPONENTIAL",
  "retryable_stages": ["CONNECT", "READ", "WRITE"]
}
```

Worker claim behavior:

1. Worker atomically claims a `PENDING` queue row or an expired `RUNNING` row.
2. Claim sets `claimed_by_worker_id`, `lease_acquired_at`,
   `lease_expires_at`, `last_heartbeat_at`, and increments `attempt_count`.
3. Worker creates a new `ctl_run` row with the same `queue_id`,
   `logical_work_id`, and `idempotency_key`.
4. Heartbeat updates `last_heartbeat_at` and extends `lease_expires_at`.
5. If the worker crashes, another worker can reclaim after lease expiry.

The queue may store the latest `run_id` for convenience, but full attempt
lineage must come from `ctl_run.queue_id` and `ctl_run.attempt_number`.

## Metadata Snapshot And Reproducibility

At queue creation or run start, the runtime must resolve a frozen metadata
snapshot:

- connection config version/hash
- ingestion object config version/hash
- mapping version/hash
- source boundary/work scope

The runtime writes this into:

- `ctl_ingestion_queue.metadata_snapshot_id`
- `ctl_run.metadata_snapshot_id`
- `ctl_run.connection_config_version`
- `ctl_run.ingestion_object_config_version`
- `ctl_run.mapping_version`
- `ctl_run.metadata_hash`

Once a run starts, it must not switch to newer active metadata. This prevents a
run from combining old object metadata with newly activated mappings.

## Watermark Safety

The watermark update flow should be two-phase:

1. Stage the candidate watermark after extraction/validation/write succeeds.
2. Commit the candidate only if the stored `watermark_version` still matches
   the version read by the run.

Runtime flow:

1. Read `committed_watermark_value` and `watermark_version`.
2. Extract data using the configured boundary semantics.
3. Write target data successfully.
4. Set `candidate_watermark_value`, `candidate_run_id`,
   `candidate_created_at`, and `commit_status = 'CANDIDATE_STAGED'`.
5. Commit by updating `committed_watermark_value`, `last_watermark_value`,
   `last_successful_run_id`, `watermark_version`, and `committed_at` only when
   the previous `watermark_version` still matches.

If the compare-and-swap update fails, another run moved the watermark. The
runtime should fail or re-evaluate the run instead of overwriting the newer
watermark.

## Incremental Boundary Rules

`watermark_column` alone is not enough. The runtime must also use:

- `boundary_operator`
- `tie_breaker_columns_json`
- `sort_columns_json`
- `lookback_interval`
- `checkpoint_type`
- `ctl_watermark.checkpoint_state_json`

For timestamp watermarks, a safe pattern is:

- Read from `committed_watermark_value - lookback_interval`.
- Sort by watermark column and tie-breakers.
- Dedupe by target keys or source event keys.
- Commit both the final watermark and tie-breaker state.

This prevents skipped records when multiple rows share the same final timestamp.

## Dependency Handling

Because no dependency table is being added, dependencies are stored in
`cfg_ingestion_object.dependency_objects_json`.

Example:

```json
{
  "dependencies": [
    {
      "object_name": "silver.orders",
      "required_stage": "BRONZE_TO_SILVER",
      "wait_for_successful_commit": true
    },
    {
      "object_name": "silver.customer",
      "required_stage": "BRONZE_TO_SILVER",
      "wait_for_successful_commit": true
    }
  ],
  "condition": "ALL_SUCCESS"
}
```

Before running a downstream object, the scheduler must check that all required
upstream objects have successful runs with committed target and watermark state
for the required scope.

## Runtime Execution Flow

The same high-level flow applies to Database, SFTP/ADLS/S3, and API sources.

1. Submit work.
   - Resolve `logical_work_id`.
   - Compute `idempotency_key`.
   - Store bounded request in `work_scope_json`.
   - Resolve or create `metadata_snapshot_id`.
   - Insert or merge a `ctl_ingestion_queue` row.

2. Claim work.
   - Worker claims queue row by status, priority, retry time, and lease state.
   - Worker creates `ctl_run` with `queue_id` and `attempt_number`.

3. Resolve config.
   - Load `cfg_connection`, `cfg_ingestion_object`, and `cfg_mapping` using the
     frozen metadata version/hash.
   - Validate all JSON contracts.
   - Validate `object_kind` and dispatch an external connector only for
     `object_kind = 'INGESTION'`.
   - Resolve `execution_spec_json`, verify `artifact_hash`, and record the
     selected engine/entry point in `ctl_run.pipeline_name` or the appropriate
     execution field.
   - Set `ctl_run.current_phase = 'CONFIG_RESOLVED'`.

4. Read source.
   - JDBC: execute table/query extraction with boundary pushdown.
   - SFTP: list and fetch scoped files.
   - ADLS/S3: list/read scoped paths or files.
   - API: call endpoint with pagination and cursor/boundary handling.
   - Set `ctl_run.current_phase = 'EXTRACTED'`.

5. Parse and normalize.
   - Apply `container_format`.
   - Parse using `payload_format` and `parser_options_json`.
   - Normalize using `normalization_options_json`.

6. Transform.
   - Load mappings for the exact stage/layer/target.
   - Apply `transformation_rule`.
   - For Gold, resolve `input_objects_json`, joins, aggregations, and
     `build_order`.

7. Validate.
   - Apply object and mapping validation policies.
   - Write validation summary.
   - Stop target commit or watermark commit if policy requires it.

8. Write target.
   - Use `write_mode` and write strategy columns.
   - Record `target_write_id` and `target_commit_status`.

9. Commit watermark.
   - Stage candidate watermark.
   - Commit with version check.
   - Record `watermark_commit_status`.

10. Finalize.
    - Mark `ctl_run.status`.
    - Mark queue status.
    - Release lease.

## Source-Specific Implementation Detail

### Database Runtime

The database connector should:

- Build a JDBC URL from `cfg_connection` and `config_json`.
- Resolve credentials from `secrets_json`.
- Use `database_schema` plus `table_name`, or `query_text`.
- Apply `load_type`:
  - `FULL`: read the full table/query.
  - `INCREMENTAL`: apply configured watermark boundary.
  - `CDC`: use CDC version, operation column, or source-specific CDC contract
    from `config_json`.
- Push down predicates where possible.
- Use partitioning options from `config_json`.
- Record `rows_read`, `bytes_processed` if available, and watermark candidate.

For `query_text`, the runtime should support controlled placeholders such as:

```text
${START_WATERMARK}
${END_WATERMARK}
${BATCH_ID}
```

Only approved placeholders should be substituted.

### SFTP Runtime

The SFTP connector should:

- Connect using host, port, auth settings, and `secrets_json`.
- Validate known hosts when configured.
- List files under `base_path` plus `source_path`.
- Filter by `file_pattern` and `work_scope_json`.
- Download to a controlled landing/staging path.
- Decompress using `container_format`.
- Parse using `payload_format`.
- Optionally archive or delete source files only after target and watermark
  commit succeed.

The file list used by a run must be deterministic. If the queue was created
from a file-arrival trigger, the specific file paths should be stored in
`work_scope_json`.

### ADLS Runtime

The ADLS connector should:

- Resolve storage account, container, and path from `base_path` and
  `config_json`.
- Authenticate using managed identity, service principal, or configured
  secrets.
- Read files directly with Spark when possible.
- Support recursive listing and file pattern filters.
- Use file modification time as a checkpoint when
  `checkpoint_type = 'FILE_MODIFIED_TIME'`.
- Add source metadata columns during normalization.

For high-volume incremental file ingestion, the implementation can use
Databricks Auto Loader internally, but queue scope, idempotency, target write,
and watermark behavior must still be governed by this metadata contract.

### S3 Runtime

The S3 connector should:

- Resolve bucket, prefix, and region from `base_path` and `config_json`.
- Authenticate using instance profile, access keys, or external credential
  configuration.
- List objects deterministically for the queue scope.
- Apply file pattern and partition filters.
- Read with Spark-compatible S3 paths.
- Track source object key, ETag, version ID if available, and modified time in
  normalized metadata columns.

Retries must avoid re-appending the same objects by using `idempotency_key`,
`logical_work_id`, and target-level dedupe/write tracking.

### API Runtime

The API connector should:

- Build request URL from `base_url` plus `endpoint_path`.
- Resolve headers, params, and body templates from metadata.
- Resolve tokens/secrets from `secrets_json`.
- Apply retry and rate-limit policy from `config_json`.
- Execute pagination according to `pagination_type` and
  `pagination_config_json`.
- Extract records from `response_root_path`.
- Persist cursor or high-water mark in `ctl_watermark.checkpoint_state_json`.

For cursor-based APIs, the committed cursor is the source of truth. A retry
must reuse the queue work scope or staged checkpoint rather than asking the API
for "latest" again.

## Recovery Rules

Recovery must be phase-aware.

Examples:

- If extraction failed before target write, retry can re-read the same
  `work_scope_json`.
- If target write committed but watermark commit failed, retry should not append
  again. It should verify `target_write_id` and complete watermark commit if
  safe.
- If watermark commit succeeded but final queue update failed, retry should mark
  the queue successful after verifying `ctl_run.watermark_commit_status`.
- If queue lease expired while the run is still heartbeating, do not reclaim it.
- If queue lease expired and no heartbeat is current, reclaim according to
  `retry_policy_json`.

## Recommended Activation Checks

Before setting `active_flag = true` and `is_current = true`, validate:

- Exactly one current row exists per logical connection/object/mapping version.
- `object_kind` is `INGESTION` or `TRANSFORMATION`, and its required/forbidden
  connection behavior is valid for the processing stage.
- `connection_type` and `ingestion_type` are compatible.
- Required connection fields exist for the selected connector.
- JSON columns validate against the expected schema version.
- `processing_stage`, `source_layer`, and `target_layer` are valid.
- Target table exists or the runtime is allowed to create it.
- `write_mode` has required keys/config.
- Incremental objects define complete boundary semantics.
- Validation policies have explicit failure actions.
- Dependencies refer to known object names.
- Active executable objects have a schema-valid `execution_spec_json`, and the
  referenced artifact/entry point is allow-listed and hash-verifiable.
- Config and mapping hashes are recomputed and stored.

## Expected Maturity

With the current lightweight DDL, the framework is roughly `35-40 / 100` for
production metadata-driven orchestration.

With the column additions above and strict runtime enforcement, the design can
reach roughly `80-85 / 100` while staying within the no-new-table constraint.
The remaining gap versus a `85-90 / 100` normalized design is that complex
relationships, dependencies, validations, and multi-input transformations live
inside governed JSON columns instead of dedicated child tables.
