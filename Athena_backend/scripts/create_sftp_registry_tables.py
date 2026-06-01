from __future__ import annotations

"""
Create SFTP/file-pipeline registry tables in the pipeline DB.

Tables:
- file_feed_schema_registry
- column_profiles
- enriched_metadata

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

        conn.commit()

        now = datetime.now(timezone.utc).isoformat()
        print(f"[ok] ensured tables in schema='{schema}' at {now}")
        print(f"[ok] {schema}.file_feed_schema_registry")
        print(f"[ok] {schema}.column_profiles")
        print(f"[ok] {schema}.enriched_metadata")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
