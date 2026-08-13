from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest

from services import pipeline_runtime
from services import snowflake_bronze_runtime


def test_snowflake_bronze_runtime_is_disabled_by_default(monkeypatch):
    monkeypatch.delenv("ATHENA_EXECUTE_SNOWFLAKE_BRONZE", raising=False)

    result = snowflake_bronze_runtime.run_snowflake_bronze_scripts(
        {"target_warehouse": "snowflake", "bronze_generation_results": [{"table": "claims"}]}
    )

    assert result["snowflake_bronze_execution_status"] == "DISABLED"


def test_gate4_refuses_silver_when_snowflake_bronze_execution_does_not_complete(monkeypatch):
    saved = []

    monkeypatch.setattr(
        pipeline_runtime,
        "load_checkpoint_state",
        lambda run_id: {
            "run_id": run_id,
            "target_warehouse": "snowflake",
            "bronze_generation_results": [{"table": "claims"}],
        },
    )
    monkeypatch.setattr(pipeline_runtime, "save_checkpoint_state", lambda run_id, state: saved.append(state))
    monkeypatch.setattr(pipeline_runtime, "continue_database_pipeline", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("silver should not start")))
    monkeypatch.setattr(
        snowflake_bronze_runtime,
        "run_snowflake_bronze_scripts",
        lambda state, **kwargs: {**state, "snowflake_bronze_execution_status": "DISABLED"},
    )

    with pytest.raises(RuntimeError, match="Snowflake Bronze execution did not complete"):
        pipeline_runtime.submit_gate4_review("run-1", action="APPROVED", review_artifact={"feeds": [{"table": "claims"}]})

    assert saved[-1]["status"] == "FAILED"
    assert saved[-1]["failed_background_stage"] == "bronze_code_execution"


def test_snowflake_account_url_is_normalized():
    assert (
        snowflake_bronze_runtime._normalize_account("https://app.snowflake.com/xbuxnho/pr61204/#/workspaces/ws")
        == "xbuxnho-pr61204"
    )


def test_snowflake_identifiers_drop_configuration_quotes():
    assert snowflake_bronze_runtime._normalize_identifier('"insurance"') == "insurance"
    assert snowflake_bronze_runtime._normalize_identifier("'dbo'") == "dbo"
    assert snowflake_bronze_runtime._snowflake_quote_identifier('"insurance"') == '"insurance"'


def test_bronze_landing_reuses_existing_database():
    statements = []
    cursor = type("Cursor", (), {"execute": lambda self, sql: statements.append(sql)})()

    snowflake_bronze_runtime._use_existing_database(cursor, "insurance")

    assert statements == ['USE DATABASE "insurance"']


def test_metadata_runtime_configures_safe_snowflake_session_identity():
    calls = []

    class Cursor:
        def execute(self, sql, parameters=None):
            calls.append((sql, parameters))

        def close(self):
            calls.append(("CLOSE", None))

    connection = type("Connection", (), {"cursor": lambda self: Cursor()})()
    context = {
        "contract_version": "1.0",
        "logical_work_id": "logical-1",
        "queue_id": 91,
        "ingestion_object_id": 101,
        "processing_stage": "SOURCE_TO_BRONZE",
        "load_type": "FULL",
        "target_table": "ATHENA.BRONZE.CLAIMS",
        "config_version": 2,
        "mapping_version": 3,
        "attempt_number": 0,
        "runtime_run_id": "runtime-1",
    }

    configured = snowflake_bronze_runtime.configure_snowflake_runtime_session(
        connection, {"metadata_runtime_context": context}
    )

    assert configured == context
    assert calls[0] == (
        "ALTER SESSION SET QUERY_TAG = %s",
        ('{"attempt_number":0,"logical_work_id":"logical-1","processing_stage":"SOURCE_TO_BRONZE",'
         '"queue_id":91,"runtime_run_id":"runtime-1"}',),
    )
    assert calls[1] == ("SET ATHENA_LOGICAL_WORK_ID = %s", ("logical-1",))
    assert calls[2] == ("SET ATHENA_RUNTIME_RUN_ID = %s", ("runtime-1",))


def test_resumed_snowflake_attempt_waits_for_prior_tagged_query(monkeypatch):
    active_counts = iter([1, 0])
    calls = []

    class Cursor:
        def execute(self, sql, parameters=None):
            calls.append((sql, parameters))

        def fetchone(self):
            return (next(active_counts),)

        def close(self):
            pass

    connection = type("Connection", (), {"cursor": lambda self: Cursor()})()
    monkeypatch.setattr(snowflake_bronze_runtime.time, "sleep", lambda _seconds: None)
    context = {
        "contract_version": "1.0",
        "logical_work_id": "logical-1",
        "queue_id": 91,
        "ingestion_object_id": 101,
        "processing_stage": "SOURCE_TO_BRONZE",
        "load_type": "FULL",
        "target_table": "ATHENA.BRONZE.CLAIMS",
        "config_version": 2,
        "mapping_version": 3,
        "attempt_number": 1,
        "runtime_run_id": "runtime-1",
        "resumed_attempt": True,
    }

    snowflake_bronze_runtime.reconcile_snowflake_resumed_attempt(
        connection, {"metadata_runtime_context": context}
    )

    assert len(calls) == 2
    assert all("QUERY_HISTORY" in sql for sql, _ in calls)
    assert calls[0][1] == calls[1][1]


def test_snowflake_execution_result_returns_last_statement_query_id(monkeypatch):
    monkeypatch.setattr(
        snowflake_bronze_runtime,
        "validate_snowflake_bronze_script",
        lambda _script: "SELECT 1; SELECT 2;",
    )
    cursors = [type("Cursor", (), {"sfqid": "query-1"})(), type("Cursor", (), {"sfqid": "query-2"})()]
    connection = type(
        "Connection",
        (),
        {"execute_string": lambda self, sql, return_cursors=True: cursors},
    )()

    result = snowflake_bronze_runtime.execute_snowflake_sql_file(
        {"table": "claims", "script_path": "claims.sql"}, connection
    )

    assert result["statement_count"] == 2
    assert result["snowflake_query_id"] == "query-2"


def test_metadata_snowflake_bronze_always_loads_database_source(monkeypatch):
    loaded = []
    connection = type("Connection", (), {"close": lambda self: None})()
    monkeypatch.setenv("ATHENA_EXECUTE_SNOWFLAKE_BRONZE", "true")
    monkeypatch.setenv("ATHENA_SNOWFLAKE_BRONZE_LOAD_SOURCE", "false")
    monkeypatch.setattr(snowflake_bronze_runtime, "_snowflake_connect", lambda: connection)
    monkeypatch.setattr(snowflake_bronze_runtime, "configure_snowflake_runtime_session", lambda *_: None)
    monkeypatch.setattr(snowflake_bronze_runtime, "validate_snowflake_bronze_script", lambda _script: "SQL")
    monkeypatch.setattr(
        snowflake_bronze_runtime,
        "load_azure_sql_table_to_snowflake",
        lambda script, _connection, **_kwargs: loaded.append(script["table"]) or {"rows_loaded": 2},
    )
    monkeypatch.setattr(
        snowflake_bronze_runtime,
        "execute_snowflake_sql_file",
        lambda script, _connection: {
            "table": script["table"],
            "status": "COMPLETED",
            "statement_count": 1,
            "snowflake_query_id": "query-1",
        },
    )
    monkeypatch.setattr(
        snowflake_bronze_runtime,
        "save_external_execution_progress",
        lambda state, **_kwargs: state,
    )

    result = snowflake_bronze_runtime.run_snowflake_bronze_scripts({
        "run_id": "runtime-1",
        "target_warehouse": "snowflake",
        "metadata_runtime_context": {"logical_work_id": "logical-1"},
        "bronze_generation_results": [{
            "table": "claims",
            "database_name": "ClaimsDB",
            "schema_name": "dbo",
            "script_path": "claims.sql",
        }],
    })

    assert loaded == ["claims"]
    assert result["snowflake_bronze_execution_status"] == "COMPLETED"


def test_adls_stage_uses_sas_without_logging_token(monkeypatch):
    token = "sv=test&sig=secret"
    monkeypatch.setenv("SNOWFLAKE_ADLS_SAS_TOKEN", "?" + token)

    class FakeCursor:
        def __init__(self):
            self.sql = []

        def execute(self, sql):
            self.sql.append(sql)

        def close(self):
            pass

    cursor = FakeCursor()
    result = snowflake_bronze_runtime.ensure_adls_stage(type("Connection", (), {"cursor": lambda self: cursor})())

    stage_sql = cursor.sql[-1]
    assert "CREATE OR REPLACE STAGE" in stage_sql
    assert f"AZURE_SAS_TOKEN = '{token}'" in stage_sql
    assert "STORAGE_INTEGRATION" not in stage_sql
    assert token not in str(result)
    assert result["credential_type"] == "sas"


def test_snowflake_bronze_runtime_executes_generated_sql(monkeypatch):
    monkeypatch.setenv("BRONZE_CATALOG", "main")
    monkeypatch.setenv("BRONZE_SCHEMA", "bronze")
    workdir = Path.cwd() / ".tmp-tests" / f"snowflake_runtime_{uuid.uuid4().hex}"
    workdir.mkdir(parents=True, exist_ok=True)
    sql_path = workdir / "bronze_claims.sql"
    sql_path.write_text(
        'CREATE SCHEMA IF NOT EXISTS "main"."bronze";\n'
        'CREATE OR REPLACE TABLE "main"."bronze"."bronze_claims" AS\n'
        'SELECT TRY_CAST(src."claim_id" AS NUMBER(38,0)) AS "claim_id", '
        '\'run-1\' AS "run_id", CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS "ingestion_timestamp", '
        '\'insurance\' AS "source_system", \'claims\' AS "source_table"\n'
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
    monkeypatch.setenv("BRONZE_CATALOG", "main")
    monkeypatch.setenv("BRONZE_SCHEMA", "bronze")
    workdir = Path.cwd() / ".tmp-tests" / f"snowflake_adls_{uuid.uuid4().hex}"
    workdir.mkdir(parents=True, exist_ok=True)

    def write_script(table):
        path = workdir / f"bronze_{table}.sql"
        path.write_text(
            f'CREATE SCHEMA IF NOT EXISTS "main"."bronze";\n'
            f'CREATE OR REPLACE TABLE "main"."bronze"."bronze_{table}" AS\n'
            f'SELECT TRY_CAST(src."claim_id" AS NUMBER(38,0)) AS "claim_id", '
            f'\'run-1\' AS "run_id", CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS "ingestion_timestamp", '
            f'\'insurance\' AS "source_system", \'{table}\' AS "source_table"\n'
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
    monkeypatch.delenv("SNOWFLAKE_ADLS_SAS_TOKEN", raising=False)
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
    assert any("TRUNCATE TABLE \"insurance\".\"dbo\".\"claim_information\"" in sql for sql in fake_conn.sql)
    assert any("COPY INTO \"insurance\".\"dbo\".\"claim_information\"" in sql for sql in fake_conn.sql)
    assert any("FILES = ('claim_information.csv')" in sql for sql in fake_conn.sql)
    assert not any("COPY INTO \"insurance\".\"dbo\".\"policy_transactions\"" in sql for sql in fake_conn.sql)
    assert fake_conn.closed is True


def test_snowflake_dbt_load_only_lands_sources_without_executing_native_sql(monkeypatch):
    loaded = []

    class FakeSnowflakeConnection:
        def close(self):
            pass

    monkeypatch.setenv("ATHENA_EXECUTE_SNOWFLAKE_BRONZE", "false")
    monkeypatch.setenv("ATHENA_SNOWFLAKE_BRONZE_LOAD_SOURCE", "true")
    monkeypatch.setenv("ATHENA_SNOWFLAKE_BRONZE_SOURCE_MODE", "adls")
    monkeypatch.setattr(snowflake_bronze_runtime, "_snowflake_connect", lambda: FakeSnowflakeConnection())
    monkeypatch.setattr(snowflake_bronze_runtime, "ensure_adls_stage", lambda _conn: None)
    monkeypatch.setattr(
        snowflake_bronze_runtime,
        "load_adls_table_to_snowflake",
        lambda script, _conn: loaded.append(script["table"]) or {
            "snowflake_landing_table": "insurance.dbo.claims",
            "copy_result_count": 1,
        },
    )
    monkeypatch.setattr(
        snowflake_bronze_runtime,
        "execute_snowflake_sql_file",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("native SQL must not execute")),
    )
    monkeypatch.setattr(
        snowflake_bronze_runtime,
        "save_external_execution_progress",
        lambda state, **_kwargs: state,
    )

    result = snowflake_bronze_runtime.run_snowflake_bronze_scripts(
        {
            "target_warehouse": "snowflake",
            "bronze_generation_results": [
                {
                    "table": "claims",
                    "database_name": "insurance",
                    "schema_name": "dbo",
                    "code_generation_format": "dbt",
                }
            ],
        },
        load_only=True,
    )

    assert loaded == ["claims"]
    assert result["snowflake_bronze_source_load_status"] == "COMPLETED"
    assert result["snowflake_bronze_execution_status"] == "SKIPPED_DBT_CODEGEN_ONLY"
    assert result["snowflake_bronze_execution_results"] == []


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
            self.closed = False

        def execute(self, sql):
            self.sql.append(sql)

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
    bulk_calls = []

    def fake_bulk_write(connection, **kwargs):
        bulk_calls.append((connection, kwargs))
        return len(kwargs["rows"])

    monkeypatch.setattr(snowflake_bronze_runtime, "_write_pandas_batch", fake_bulk_write)

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
    assert len(bulk_calls) == 2
    assert all(call[0] is fake_snowflake_conn for call in bulk_calls)
    assert bulk_calls[0][1]["table"] == "claim_payment_indemnity"
    assert any("rows_loaded=4" in message for message in progress_messages)
    assert fake_source_conn.closed is True
    assert fake_snowflake_conn.cursor_instance.closed is True

    metadata_source_conn = FakeSourceConnection()
    metadata_snowflake_conn = FakeSnowflakeConnection()
    monkeypatch.setattr(
        snowflake_bronze_runtime,
        "get_client_connection",
        lambda _database_name: metadata_source_conn,
    )
    snowflake_bronze_runtime.load_azure_sql_table_to_snowflake(
        {
            "table": "claim_payment_indemnity",
            "database_name": "insurance",
            "schema_name": "dbo",
            "snowflake_landing_database": "ATHENA",
            "snowflake_landing_schema": "BRONZE",
            "snowflake_landing_table": "raw_claim_payment_indemnity",
            "metadata_runtime": True,
        },
        metadata_snowflake_conn,
        run_id="runtime-123",
    )

    assert any(
        sql.startswith(
            'CREATE OR REPLACE TEMPORARY TABLE "ATHENA"."BRONZE"."raw_claim_payment_indemnity"'
        )
        for sql in metadata_snowflake_conn.cursor_instance.sql
    )


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
