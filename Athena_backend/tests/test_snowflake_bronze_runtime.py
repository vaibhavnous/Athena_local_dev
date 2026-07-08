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
    sql_path.write_text("CREATE SCHEMA IF NOT EXISTS \"ATHENA_DB\".\"BRONZE\";\nSELECT 1;", encoding="utf-8")

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
    monkeypatch.delenv("ATHENA_SNOWFLAKE_BRONZE_LOAD_SOURCE", raising=False)
    monkeypatch.setattr(snowflake_bronze_runtime, "_snowflake_connect", lambda: fake_conn)

    result = snowflake_bronze_runtime.run_snowflake_bronze_scripts(
        {
            "target_warehouse": "snowflake",
            "bronze_generation_results": [{"table": "claims", "script_path": str(sql_path)}],
        }
    )

    assert result["snowflake_bronze_execution_status"] == "COMPLETED"
    assert result["snowflake_bronze_execution_results"][0]["statement_count"] == 2
    assert fake_conn.sql[0][0].startswith("CREATE SCHEMA")
    assert fake_conn.closed is True


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
