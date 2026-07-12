from __future__ import annotations

import json
import uuid
from pathlib import Path

from services import pipeline_runtime
from services import snowflake_bronze_runtime


def test_snowflake_bronze_runtime_is_disabled_by_default(monkeypatch):
    monkeypatch.delenv("ATHENA_EXECUTE_SNOWFLAKE_BRONZE", raising=False)

    result = snowflake_bronze_runtime.run_snowflake_bronze_scripts(
        {"target_warehouse": "snowflake", "bronze_generation_results": [{"table": "claims"}]}
    )

    assert result["snowflake_bronze_execution_status"] == "DISABLED"


def test_snowflake_account_url_is_normalized():
    assert (
        snowflake_bronze_runtime._normalize_account("https://app.snowflake.com/xbuxnho/pr61204/#/workspaces/ws")
        == "xbuxnho-pr61204"
    )


def test_snowflake_bronze_runtime_executes_generated_sql(monkeypatch):
    workdir = Path.cwd() / ".tmp-tests" / f"snowflake_runtime_{uuid.uuid4().hex}"
    workdir.mkdir(parents=True, exist_ok=True)
    sql_path = workdir / "bronze_claims.sql"
    sql_path.write_text(
        'CREATE SCHEMA IF NOT EXISTS "main"."bronze";\n'
        'CREATE TABLE IF NOT EXISTS "main"."bronze"."bronze_claims" (\n'
        '    "claim_id" NUMBER(38,0),\n'
        '    "run_id" VARCHAR,\n'
        '    "ingestion_timestamp" TIMESTAMP_NTZ,\n'
        '    "source_system" VARCHAR,\n'
        '    "source_table" VARCHAR\n'
        ');\n'
        'INSERT INTO "main"."bronze"."bronze_claims" (\n'
        '    "claim_id", "run_id", "ingestion_timestamp", "source_system", "source_table"\n'
        ')\n'
        'SELECT TRY_CAST(src."claim_id" AS NUMBER(38,0)), \'run-1\', CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, \'insurance\', \'claims\'\n'
        'FROM "insurance"."dbo"."claims" AS src;',
        encoding="utf-8",
    )

    class FakeSnowflakeConnection:
        def __init__(self):
            self.closed = False
            self.sql = []

        def execute_string(self, sql, return_cursors=True):
            self.sql.append((sql, return_cursors))
            return [object(), object()]

        def close(self):
            self.closed = True

    fake_conn = FakeSnowflakeConnection()
    monkeypatch.setenv("ATHENA_EXECUTE_SNOWFLAKE_BRONZE", "true")
    monkeypatch.setenv("ATHENA_SNOWFLAKE_BRONZE_LOAD_SOURCE", "false")
    monkeypatch.setattr(snowflake_bronze_runtime, "_snowflake_connect", lambda: fake_conn)

    result = snowflake_bronze_runtime.run_snowflake_bronze_scripts(
        {
            "target_warehouse": "snowflake",
            "bronze_generation_results": [
                {
                    "table": "claims",
                    "database_name": "insurance",
                    "schema_name": "dbo",
                    "script_path": str(sql_path),
                }
            ],
        }
    )

    assert result["snowflake_bronze_execution_status"] == "COMPLETED"
    assert result["snowflake_bronze_execution_results"][0]["statement_count"] == 2
    assert fake_conn.sql[0][0].startswith("CREATE SCHEMA")
    assert fake_conn.closed is True


def test_snowflake_bronze_runtime_adls_executes_only_approved_scripts(monkeypatch):
    workdir = Path.cwd() / ".tmp-tests" / f"snowflake_adls_{uuid.uuid4().hex}"
    workdir.mkdir(parents=True, exist_ok=True)

    def write_script(table):
        path = workdir / f"bronze_{table}.sql"
        path.write_text(
            f'CREATE SCHEMA IF NOT EXISTS "main"."bronze";\n'
            f'CREATE TABLE IF NOT EXISTS "main"."bronze"."bronze_{table}" (\n'
            f'    "claim_id" NUMBER(38,0),\n'
            f'    "run_id" VARCHAR,\n'
            f'    "ingestion_timestamp" TIMESTAMP_NTZ,\n'
            f'    "source_system" VARCHAR,\n'
            f'    "source_table" VARCHAR\n'
            f');\n'
            f'INSERT INTO "main"."bronze"."bronze_{table}" (\n'
            f'    "claim_id", "run_id", "ingestion_timestamp", "source_system", "source_table"\n'
            f')\n'
            f'SELECT TRY_CAST(src."claim_id" AS NUMBER(38,0)), \'run-1\', CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, \'insurance\', \'{table}\'\n'
            f'FROM "insurance"."dbo"."{table}" AS src;',
            encoding="utf-8",
        )
        return str(path)

    class FakeCursor:
        description = [("status",)]

        def __init__(self, conn):
            self.conn = conn

        def execute(self, sql):
            self.conn.sql.append(sql)

        def fetchall(self):
            return [("loaded",)]

        def close(self):
            pass

    class FakeSnowflakeConnection:
        def __init__(self):
            self.sql = []
            self.closed = False

        def cursor(self):
            return FakeCursor(self)

        def execute_string(self, sql, return_cursors=True):
            self.sql.append(sql)
            return [object(), object()]

        def close(self):
            self.closed = True

    fake_conn = FakeSnowflakeConnection()
    monkeypatch.setenv("ATHENA_EXECUTE_SNOWFLAKE_BRONZE", "true")
    monkeypatch.setenv("ATHENA_SNOWFLAKE_BRONZE_LOAD_SOURCE", "true")
    monkeypatch.setenv("ATHENA_SNOWFLAKE_BRONZE_SOURCE_MODE", "adls")
    monkeypatch.setenv("SNOWFLAKE_ADLS_STAGE_URL", "azure://atheastorage.blob.core.windows.net/athena/Insurance/")
    monkeypatch.setattr(snowflake_bronze_runtime, "_snowflake_connect", lambda: fake_conn)

    result = snowflake_bronze_runtime.run_snowflake_bronze_scripts(
        {
            "target_warehouse": "snowflake",
            "bronze_generation_results": [
                {
                    "table": "claim_information",
                    "database_name": "insurance",
                    "schema_name": "dbo",
                    "script_path": write_script("claim_information"),
                    "source_columns": [{"source": "claim_id"}],
                },
                {
                    "table": "policy_transactions",
                    "database_name": "insurance",
                    "schema_name": "dbo",
                    "script_path": write_script("policy_transactions"),
                    "source_columns": [{"source": "claim_id"}],
                },
            ],
        },
        review_artifact={
            "feeds": [
                {
                    "table": "claim_information",
                    "database_name": "insurance",
                    "schema_name": "dbo",
                    "review_status": "APPROVED",
                },
                {
                    "table": "policy_transactions",
                    "database_name": "insurance",
                    "schema_name": "dbo",
                    "review_status": "REJECTED",
                },
            ]
        },
        approved_only=True,
    )

    assert result["snowflake_bronze_execution_status"] == "COMPLETED"
    assert result["snowflake_bronze_source_mode"] == "adls"
    assert [item["table"] for item in result["snowflake_bronze_execution_results"]] == ["claim_information"]
    assert any("CREATE STAGE IF NOT EXISTS" in sql for sql in fake_conn.sql)
    assert any("COPY INTO \"insurance\".\"dbo\".\"claim_information\"" in sql for sql in fake_conn.sql)
    assert not any("COPY INTO \"insurance\".\"dbo\".\"policy_transactions\"" in sql for sql in fake_conn.sql)
    assert fake_conn.closed is True


def test_approved_review_scripts_match_case_sensitive_variants():
    lower_script = {
        "table": "policy_cover_level_transactions_dup_del",
        "database_name": "insurance",
        "schema_name": "dbo",
        "script_path": "lower.sql",
    }
    mixed_script = {
        "table": "policy_cover_level_transactions_Dup_Del",
        "database_name": "insurance",
        "schema_name": "dbo",
        "script_path": "mixed.sql",
    }

    approved = snowflake_bronze_runtime._approved_review_scripts(
        {
            "bronze_generation_results": [
                lower_script,
                mixed_script,
            ]
        },
        {
            "feeds": [
                {
                    "table": "policy_cover_level_transactions_dup_del",
                    "database_name": "insurance",
                    "schema_name": "dbo",
                    "review_status": "APPROVED",
                }
            ]
        },
    )

    assert len(approved) == 1
    assert approved[0]["script_path"] == "lower.sql"


def test_approved_review_scripts_use_selected_subset_and_keep_all_pending_legacy():
    scripts = [
        {
            "table": "claim_information",
            "database_name": "insurance",
            "schema_name": "dbo",
            "script_path": "claims.sql",
        },
        {
            "table": "policy_transactions",
            "database_name": "insurance",
            "schema_name": "dbo",
            "script_path": "policy.sql",
        },
    ]

    selected = snowflake_bronze_runtime._approved_review_scripts(
        {"bronze_generation_results": scripts},
        {
            "feeds": [
                {
                    "table": "claim_information",
                    "database_name": "insurance",
                    "schema_name": "dbo",
                    "review_status": "APPROVED",
                },
                {
                    "table": "policy_transactions",
                    "database_name": "insurance",
                    "schema_name": "dbo",
                    "review_status": "PENDING",
                },
            ]
        },
    )
    legacy_all = snowflake_bronze_runtime._approved_review_scripts(
        {"bronze_generation_results": scripts},
        {
            "feeds": [
                {
                    "table": "claim_information",
                    "database_name": "insurance",
                    "schema_name": "dbo",
                    "review_status": "PENDING",
                },
                {
                    "table": "policy_transactions",
                    "database_name": "insurance",
                    "schema_name": "dbo",
                    "review_status": "PENDING",
                },
            ]
        },
    )

    assert [item["script_path"] for item in selected] == ["claims.sql"]
    assert [item["script_path"] for item in legacy_all] == ["claims.sql", "policy.sql"]


def test_snowflake_bronze_runtime_rejects_wrong_script_format(monkeypatch):
    workdir = Path.cwd() / ".tmp-tests" / f"snowflake_runtime_bad_{uuid.uuid4().hex}"
    workdir.mkdir(parents=True, exist_ok=True)
    sql_path = workdir / "bronze_claims.sql"
    sql_path.write_text("CREATE SCHEMA IF NOT EXISTS \"main\".\"bronze\";\nSELECT 1;", encoding="utf-8")

    class FakeSnowflakeConnection:
        def execute_string(self, sql, return_cursors=True):
            raise AssertionError("Bad Snowflake bronze SQL should not execute")

        def close(self):
            pass

    monkeypatch.setenv("ATHENA_EXECUTE_SNOWFLAKE_BRONZE", "true")
    monkeypatch.setattr(snowflake_bronze_runtime, "_snowflake_connect", lambda: FakeSnowflakeConnection())

    try:
        snowflake_bronze_runtime.run_snowflake_bronze_scripts(
            {
                "target_warehouse": "snowflake",
                "bronze_generation_results": [
                    {
                        "table": "claims",
                        "database_name": "insurance",
                        "schema_name": "dbo",
                        "script_path": str(sql_path),
                    }
                ],
            }
        )
    except ValueError as exc:
        assert "missing required statements" in str(exc).lower()
    else:
        raise AssertionError("Wrong Snowflake bronze script format should be rejected")


def test_load_azure_sql_table_to_snowflake_replaces_landing_table_and_logs_progress(monkeypatch):
    progress_messages = []

    class FakeSourceCursor:
        description = [("claim_id",), ("status",)]

        def __init__(self):
            self._batches = [
                [(1, "open"), (2, "closed")],
                [(3, "open"), (4, "closed")],
                [],
            ]

        def execute(self, sql):
            self.sql = sql

        def fetchmany(self, size):
            return self._batches.pop(0)

    class FakeSourceConnection:
        def __init__(self):
            self.closed = False
            self.cursor_instance = FakeSourceCursor()

        def cursor(self):
            return self.cursor_instance

        def close(self):
            self.closed = True

    class FakeSnowflakeCursor:
        def __init__(self):
            self.sql = []
            self.executemany_calls = []
            self.closed = False

        def execute(self, sql):
            self.sql.append(sql)

        def executemany(self, sql, values):
            self.executemany_calls.append((sql, values))

        def close(self):
            self.closed = True

    class FakeSnowflakeConnection:
        def __init__(self):
            self.cursor_instance = FakeSnowflakeCursor()

        def cursor(self):
            return self.cursor_instance

    fake_source_conn = FakeSourceConnection()
    fake_snowflake_conn = FakeSnowflakeConnection()

    monkeypatch.setattr(snowflake_bronze_runtime, "get_client_connection", lambda database_name: fake_source_conn)
    monkeypatch.setattr(snowflake_bronze_runtime, "_batch_size", lambda: 2)
    monkeypatch.setattr(snowflake_bronze_runtime, "_progress_log_interval", lambda: 3)

    def capture_info(message, *args, **kwargs):
        progress_messages.append(message % args if args else message)

    monkeypatch.setattr(snowflake_bronze_runtime.logger, "info", capture_info)

    result = snowflake_bronze_runtime.load_azure_sql_table_to_snowflake(
        {
            "table": "claim_payment_indemnity",
            "database_name": "insurance",
            "schema_name": "dbo",
        },
        fake_snowflake_conn,
        run_id="run-123",
    )

    assert result["rows_loaded"] == 4
    assert any(sql.startswith('CREATE OR REPLACE TABLE "insurance"."dbo"."claim_payment_indemnity"') for sql in fake_snowflake_conn.cursor_instance.sql)
    assert len(fake_snowflake_conn.cursor_instance.executemany_calls) == 2
    assert any("rows_loaded=4" in message for message in progress_messages)
    assert fake_source_conn.closed is True
    assert fake_snowflake_conn.cursor_instance.closed is True


def test_load_bronze_scripts_reads_snowflake_bundle(monkeypatch):
    workdir = Path.cwd() / ".tmp-tests" / f"snowflake_bundle_{uuid.uuid4().hex}"
    workdir.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(workdir)
    output_dir = workdir / "generated_code" / "snowflake" / "bronze"
    output_dir.mkdir(parents=True)
    sql_path = output_dir / "bronze_ingest_run_snow_claims.sql"
    sql_path.write_text("SELECT 1;", encoding="utf-8")
    bundle = {
        "run_id": "run-snow",
        "generated_at": "2026-07-07T00:00:00",
        "scripts": [
            {
                "run_id": "run-snow",
                "table": "claims",
                "script_path": str(sql_path),
                "target_warehouse": "snowflake",
            }
        ],
    }
    (output_dir / "run_snow_bronze_scripts.json").write_text(json.dumps(bundle), encoding="utf-8")

    loaded = pipeline_runtime.load_bronze_scripts(
        "run-snow",
        {"run_id": "run-snow", "target_warehouse": "snowflake"},
    )

    assert loaded["scripts"][0]["script_body"] == "SELECT 1;"
