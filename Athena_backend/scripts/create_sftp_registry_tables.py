from __future__ import annotations

"""
Create SFTP/file-pipeline registry tables in the pipeline DB.

Tables:
- file_feed_registry
- file_feed_schema_registry
- file_feed_manifest
- file_feed_sync_log
- bronze_execution_plan
- bronze_file_ingestion_log
- pipeline_run_log
- hitl_review_queue
- column_profiles
- enriched_metadata
- file_feed_dq_results
- file_feed_quarantine_audit
- gold_publish_log

Idempotent: safe to run multiple times.
"""

from datetime import datetime, timezone
import sys
from pathlib import Path

# Ensure `Athena_backend/` is on sys.path so `import utilis.*` works even when
# executing from `Athena_backend/scripts/`.
BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from utilis.db import config, get_pipeline_connection


def _pipeline_schema() -> str:
    return (
        config["azure_sql"].get("pipeline_schema")
        or config["azure_sql"].get("schema_name")
        or "dbo"
    )


def _exec(cursor, sql: str) -> None:
    cursor.execute(sql)


def main() -> None:
    schema = _pipeline_schema()
    conn = get_pipeline_connection()
    try:
        cur = conn.cursor()

        # Schema
        _exec(
            cur,
            f"""
            IF SCHEMA_ID(N'{schema}') IS NULL
            BEGIN
                EXEC(N'CREATE SCHEMA [{schema}]');
            END
            """,
        )

        # file_feed_schema_registry
        _exec(
            cur,
            f"""
            IF OBJECT_ID(N'[{schema}].[file_feed_schema_registry]', N'U') IS NULL
            BEGIN
                CREATE TABLE [{schema}].[file_feed_schema_registry] (
                    [id] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    [feed_id] NVARCHAR(255) NOT NULL,
                    [vendor] NVARCHAR(255) NOT NULL,
                    [entity] NVARCHAR(255) NOT NULL,
                    [format] NVARCHAR(50) NOT NULL,
                    [schema_json] NVARCHAR(MAX) NOT NULL,
                    [schema_fingerprint] NVARCHAR(64) NOT NULL,
                    [version] INT NOT NULL,
                    [discovered_at] DATETIME2(7) NOT NULL,
                    [source_type] NVARCHAR(50) NOT NULL,
                    [created_at] DATETIME2(7) NOT NULL CONSTRAINT [DF_file_feed_schema_registry_created_at] DEFAULT SYSUTCDATETIME()
                );

                CREATE INDEX [IX_file_feed_schema_registry_feed_id]
                    ON [{schema}].[file_feed_schema_registry] ([feed_id], [version] DESC);

                CREATE INDEX [IX_file_feed_schema_registry_vendor_entity]
                    ON [{schema}].[file_feed_schema_registry] ([vendor], [entity], [version] DESC);
            END
            """,
        )
        _exec(cur, f"IF COL_LENGTH(N'[{schema}].[file_feed_schema_registry]', 'row_tag') IS NULL ALTER TABLE [{schema}].[file_feed_schema_registry] ADD [row_tag] NVARCHAR(255) NULL;")
        _exec(cur, f"IF COL_LENGTH(N'[{schema}].[file_feed_schema_registry]', 'sample_file_path') IS NULL ALTER TABLE [{schema}].[file_feed_schema_registry] ADD [sample_file_path] NVARCHAR(2048) NULL;")
        _exec(cur, f"IF COL_LENGTH(N'[{schema}].[file_feed_schema_registry]', 'source_path') IS NULL ALTER TABLE [{schema}].[file_feed_schema_registry] ADD [source_path] NVARCHAR(2048) NULL;")
        _exec(cur, f"IF COL_LENGTH(N'[{schema}].[file_feed_schema_registry]', 'schema_status') IS NULL ALTER TABLE [{schema}].[file_feed_schema_registry] ADD [schema_status] NVARCHAR(50) NOT NULL CONSTRAINT [DF_file_feed_schema_registry_schema_status] DEFAULT 'PENDING_REVIEW';")
        _exec(cur, f"IF COL_LENGTH(N'[{schema}].[file_feed_schema_registry]', 'approved_by') IS NULL ALTER TABLE [{schema}].[file_feed_schema_registry] ADD [approved_by] NVARCHAR(255) NULL;")
        _exec(cur, f"IF COL_LENGTH(N'[{schema}].[file_feed_schema_registry]', 'approved_at') IS NULL ALTER TABLE [{schema}].[file_feed_schema_registry] ADD [approved_at] DATETIME2(7) NULL;")
        _exec(cur, f"IF COL_LENGTH(N'[{schema}].[file_feed_schema_registry]', 'rejection_reason') IS NULL ALTER TABLE [{schema}].[file_feed_schema_registry] ADD [rejection_reason] NVARCHAR(MAX) NULL;")

        # column_profiles
        _exec(
            cur,
            f"""
            IF OBJECT_ID(N'[{schema}].[column_profiles]', N'U') IS NULL
            BEGIN
                CREATE TABLE [{schema}].[column_profiles] (
                    [id] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    [feed_id] NVARCHAR(255) NOT NULL,
                    [vendor] NVARCHAR(255) NOT NULL,
                    [entity] NVARCHAR(255) NOT NULL,
                    [column_name] NVARCHAR(255) NOT NULL,
                    [metrics_json] NVARCHAR(MAX) NOT NULL,
                    [profiled_at] DATETIME2(7) NOT NULL,
                    [created_at] DATETIME2(7) NOT NULL CONSTRAINT [DF_column_profiles_created_at] DEFAULT SYSUTCDATETIME()
                );

                CREATE INDEX [IX_column_profiles_feed_column]
                    ON [{schema}].[column_profiles] ([feed_id], [column_name], [profiled_at] DESC);

                CREATE INDEX [IX_column_profiles_vendor_entity]
                    ON [{schema}].[column_profiles] ([vendor], [entity], [profiled_at] DESC);
            END
            """,
        )

        # enriched_metadata (used by SFTP semantic enrichment + Gate 3)
        _exec(
            cur,
            f"""
            IF OBJECT_ID(N'[{schema}].[enriched_metadata]', N'U') IS NULL
            BEGIN
                CREATE TABLE [{schema}].[enriched_metadata] (
                    [id] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    [feed_id] NVARCHAR(255) NOT NULL,
                    [vendor] NVARCHAR(255) NOT NULL,
                    [entity] NVARCHAR(255) NOT NULL,
                    [column_name] NVARCHAR(255) NOT NULL,
                    [semantic_type] NVARCHAR(50) NOT NULL,
                    [approved] BIT NOT NULL CONSTRAINT [DF_enriched_metadata_approved] DEFAULT 0,
                    [approved_by] NVARCHAR(255) NULL,
                    [approved_at] DATETIME2(7) NULL,
                    [created_at] DATETIME2(7) NOT NULL,
                    [payload_json] NVARCHAR(MAX) NOT NULL
                );

                CREATE INDEX [IX_enriched_metadata_feed_column]
                    ON [{schema}].[enriched_metadata] ([feed_id], [column_name], [created_at] DESC);

                CREATE INDEX [IX_enriched_metadata_pending]
                    ON [{schema}].[enriched_metadata] ([approved], [vendor], [entity]);
            END
            """,
        )

        # file_feed_registry
        _exec(
            cur,
            f"""
            IF OBJECT_ID(N'[{schema}].[file_feed_registry]', N'U') IS NULL
            BEGIN
                CREATE TABLE [{schema}].[file_feed_registry] (
                    [id] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    [feed_id] NVARCHAR(255) NOT NULL,
                    [vendor] NVARCHAR(255) NOT NULL,
                    [entity] NVARCHAR(255) NOT NULL,
                    [format] NVARCHAR(50) NOT NULL,
                    [file_name] NVARCHAR(1024) NULL,
                    [file_path] NVARCHAR(2048) NULL,
                    [remote_path] NVARCHAR(2048) NULL,
                    [status] NVARCHAR(50) NOT NULL CONSTRAINT [DF_file_feed_registry_status] DEFAULT 'NOMINATED',
                    [source] NVARCHAR(50) NOT NULL,
                    [last_modified_at] DATETIME2(7) NULL,
                    [approved_at] DATETIME2(7) NULL,
                    [created_at] DATETIME2(7) NOT NULL CONSTRAINT [DF_file_feed_registry_created_at] DEFAULT SYSUTCDATETIME(),
                    [updated_at] DATETIME2(7) NOT NULL CONSTRAINT [DF_file_feed_registry_updated_at] DEFAULT SYSUTCDATETIME()
                );

                CREATE INDEX [IX_file_feed_registry_feed_id]
                    ON [{schema}].[file_feed_registry] ([feed_id]);

                CREATE INDEX [IX_file_feed_registry_vendor_entity]
                    ON [{schema}].[file_feed_registry] ([vendor], [entity], [status]);
            END
            """,
        )

        # file_feed_manifest
        _exec(
            cur,
            f"""
            IF OBJECT_ID(N'[{schema}].[file_feed_manifest]', N'U') IS NULL
            BEGIN
                CREATE TABLE [{schema}].[file_feed_manifest] (
                    [id] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    [feed_id] NVARCHAR(255) NOT NULL,
                    [file_name] NVARCHAR(1024) NULL,
                    [file_path] NVARCHAR(2048) NOT NULL,
                    [remote_path] NVARCHAR(2048) NULL,
                    [file_size] BIGINT NULL,
                    [modified_time] DATETIME2(7) NULL,
                    [checksum] NVARCHAR(128) NULL,
                    [digest_algorithm] NVARCHAR(50) NOT NULL CONSTRAINT [DF_file_feed_manifest_digest_algorithm] DEFAULT 'SHA256',
                    [state] NVARCHAR(50) NOT NULL CONSTRAINT [DF_file_feed_manifest_state] DEFAULT 'PENDING',
                    [found_at] DATETIME2(7) NOT NULL,
                    [downloaded_at] DATETIME2(7) NULL,
                    [created_at] DATETIME2(7) NOT NULL CONSTRAINT [DF_file_feed_manifest_created_at] DEFAULT SYSUTCDATETIME()
                );

                CREATE INDEX [IX_file_feed_manifest_feed_id]
                    ON [{schema}].[file_feed_manifest] ([feed_id]);

                CREATE INDEX [IX_file_feed_manifest_state]
                    ON [{schema}].[file_feed_manifest] ([state]);
            END
            """,
        )

        # bronze_execution_plan
        _exec(
            cur,
            f"""
            IF OBJECT_ID(N'[{schema}].[bronze_execution_plan]', N'U') IS NULL
            BEGIN
                CREATE TABLE [{schema}].[bronze_execution_plan] (
                    [id] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    [run_id] NVARCHAR(255) NOT NULL,
                    [feed_id] NVARCHAR(255) NOT NULL,
                    [vendor] NVARCHAR(255) NOT NULL,
                    [entity] NVARCHAR(255) NOT NULL,
                    [plan_json] NVARCHAR(MAX) NOT NULL,
                    [script_text] NVARCHAR(MAX) NOT NULL,
                    [config_json] NVARCHAR(MAX) NOT NULL,
                    [review_status] NVARCHAR(50) NOT NULL,
                    [created_at] DATETIME2(7) NOT NULL CONSTRAINT [DF_bronze_execution_plan_created_at] DEFAULT SYSUTCDATETIME(),
                    [updated_at] DATETIME2(7) NOT NULL CONSTRAINT [DF_bronze_execution_plan_updated_at] DEFAULT SYSUTCDATETIME()
                );

                CREATE INDEX [IX_bronze_execution_plan_run_id]
                    ON [{schema}].[bronze_execution_plan] ([run_id], [feed_id], [review_status]);
            END
            """,
        )

        # file_feed_sync_log
        _exec(
            cur,
            f"""
            IF OBJECT_ID(N'[{schema}].[file_feed_sync_log]', N'U') IS NULL
            BEGIN
                CREATE TABLE [{schema}].[file_feed_sync_log] (
                    [id] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    [feed_id] NVARCHAR(255) NOT NULL,
                    [manifest_id] BIGINT NULL,
                    [sync_status] NVARCHAR(50) NOT NULL,
                    [attempt] INT NOT NULL DEFAULT 1,
                    [started_at] DATETIME2(7) NOT NULL,
                    [completed_at] DATETIME2(7) NULL,
                    [processed_rows] BIGINT NULL,
                    [error_message] NVARCHAR(MAX) NULL,
                    [created_at] DATETIME2(7) NOT NULL CONSTRAINT [DF_file_feed_sync_log_created_at] DEFAULT SYSUTCDATETIME()
                );

                CREATE INDEX [IX_file_feed_sync_log_feed_id]
                    ON [{schema}].[file_feed_sync_log] ([feed_id], [sync_status]);
            END
            """,
        )

        # bronze_file_ingestion_log
        _exec(
            cur,
            f"""
            IF OBJECT_ID(N'[{schema}].[bronze_file_ingestion_log]', N'U') IS NULL
            BEGIN
                CREATE TABLE [{schema}].[bronze_file_ingestion_log] (
                    [id] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    [feed_id] NVARCHAR(255) NOT NULL,
                    [target_table] NVARCHAR(1024) NOT NULL,
                    [source_file_path] NVARCHAR(2048) NULL,
                    [record_count] BIGINT NULL,
                    [status] NVARCHAR(50) NOT NULL,
                    [started_at] DATETIME2(7) NOT NULL,
                    [completed_at] DATETIME2(7) NULL,
                    [error_message] NVARCHAR(MAX) NULL,
                    [created_at] DATETIME2(7) NOT NULL CONSTRAINT [DF_bronze_file_ingestion_log_created_at] DEFAULT SYSUTCDATETIME()
                );

                CREATE INDEX [IX_bronze_file_ingestion_log_feed_id]
                    ON [{schema}].[bronze_file_ingestion_log] ([feed_id], [status]);
            END
            """,
        )

        # pipeline_run_log
        _exec(
            cur,
            f"""
            IF OBJECT_ID(N'[{schema}].[pipeline_run_log]', N'U') IS NULL
            BEGIN
                CREATE TABLE [{schema}].[pipeline_run_log] (
                    [id] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    [run_id] NVARCHAR(255) NOT NULL,
                    [source] NVARCHAR(100) NULL,
                    [status] NVARCHAR(50) NOT NULL,
                    [started_at] DATETIME2(7) NOT NULL,
                    [completed_at] DATETIME2(7) NULL,
                    [error_message] NVARCHAR(MAX) NULL,
                    [created_at] DATETIME2(7) NOT NULL CONSTRAINT [DF_pipeline_run_log_created_at] DEFAULT SYSUTCDATETIME()
                );

                CREATE INDEX [IX_pipeline_run_log_run_id]
                    ON [{schema}].[pipeline_run_log] ([run_id]);

                CREATE INDEX [IX_pipeline_run_log_status]
                    ON [{schema}].[pipeline_run_log] ([status]);
            END
            """,
        )

        # hitl_review_queue
        _exec(
            cur,
            f"""
            IF OBJECT_ID(N'[{schema}].[hitl_review_queue]', N'U') IS NULL
            BEGIN
                CREATE TABLE [{schema}].[hitl_review_queue] (
                    [id] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    [run_id] NVARCHAR(255) NOT NULL,
                    [gate_name] NVARCHAR(100) NOT NULL,
                    [payload_json] NVARCHAR(MAX) NOT NULL,
                    [decision] NVARCHAR(50) NULL,
                    [reviewed_by] NVARCHAR(255) NULL,
                    [reviewed_at] DATETIME2(7) NULL,
                    [status] NVARCHAR(50) NOT NULL CONSTRAINT [DF_hitl_review_queue_status] DEFAULT 'PENDING',
                    [created_at] DATETIME2(7) NOT NULL CONSTRAINT [DF_hitl_review_queue_created_at] DEFAULT SYSUTCDATETIME()
                );

                CREATE INDEX [IX_hitl_review_queue_run_id]
                    ON [{schema}].[hitl_review_queue] ([run_id], [gate_name], [status]);
            END
            """,
        )

        # file_feed_dq_results
        _exec(
            cur,
            f"""
            IF OBJECT_ID(N'[{schema}].[file_feed_dq_results]', N'U') IS NULL
            BEGIN
                CREATE TABLE [{schema}].[file_feed_dq_results] (
                    [id] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    [feed_id] NVARCHAR(255) NOT NULL,
                    [dq_rule] NVARCHAR(255) NULL,
                    [result_json] NVARCHAR(MAX) NOT NULL,
                    [status] NVARCHAR(50) NOT NULL,
                    [checked_at] DATETIME2(7) NOT NULL,
                    [created_at] DATETIME2(7) NOT NULL CONSTRAINT [DF_file_feed_dq_results_created_at] DEFAULT SYSUTCDATETIME()
                );

                CREATE INDEX [IX_file_feed_dq_results_feed_id]
                    ON [{schema}].[file_feed_dq_results] ([feed_id], [status]);
            END
            """,
        )

        # file_feed_quarantine_audit
        _exec(
            cur,
            f"""
            IF OBJECT_ID(N'[{schema}].[file_feed_quarantine_audit]', N'U') IS NULL
            BEGIN
                CREATE TABLE [{schema}].[file_feed_quarantine_audit] (
                    [id] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    [feed_id] NVARCHAR(255) NOT NULL,
                    [issue_type] NVARCHAR(255) NOT NULL,
                    [details_json] NVARCHAR(MAX) NULL,
                    [quarantined_rows] BIGINT NULL,
                    [audit_at] DATETIME2(7) NOT NULL,
                    [created_at] DATETIME2(7) NOT NULL CONSTRAINT [DF_file_feed_quarantine_audit_created_at] DEFAULT SYSUTCDATETIME()
                );

                CREATE INDEX [IX_file_feed_quarantine_audit_feed_id]
                    ON [{schema}].[file_feed_quarantine_audit] ([feed_id], [issue_type]);
            END
            """,
        )

        # gold_publish_log
        _exec(
            cur,
            f"""
            IF OBJECT_ID(N'[{schema}].[gold_publish_log]', N'U') IS NULL
            BEGIN
                CREATE TABLE [{schema}].[gold_publish_log] (
                    [id] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    [feed_id] NVARCHAR(255) NOT NULL,
                    [target_table] NVARCHAR(1024) NOT NULL,
                    [publish_status] NVARCHAR(50) NOT NULL,
                    [published_at] DATETIME2(7) NULL,
                    [error_message] NVARCHAR(MAX) NULL,
                    [created_at] DATETIME2(7) NOT NULL CONSTRAINT [DF_gold_publish_log_created_at] DEFAULT SYSUTCDATETIME()
                );

                CREATE INDEX [IX_gold_publish_log_feed_id]
                    ON [{schema}].[gold_publish_log] ([feed_id], [publish_status]);
            END
            """,
        )

        conn.commit()

        now = datetime.now(timezone.utc).isoformat()
        print(f"[ok] ensured tables in schema='{schema}' at {now}")
        print(f"[ok] {schema}.file_feed_registry")
        print(f"[ok] {schema}.file_feed_schema_registry")
        print(f"[ok] {schema}.file_feed_manifest")
        print(f"[ok] {schema}.file_feed_sync_log")
        print(f"[ok] {schema}.bronze_execution_plan")
        print(f"[ok] {schema}.bronze_file_ingestion_log")
        print(f"[ok] {schema}.pipeline_run_log")
        print(f"[ok] {schema}.hitl_review_queue")
        print(f"[ok] {schema}.column_profiles")
        print(f"[ok] {schema}.enriched_metadata")
        print(f"[ok] {schema}.file_feed_dq_results")
        print(f"[ok] {schema}.file_feed_quarantine_audit")
        print(f"[ok] {schema}.gold_publish_log")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
