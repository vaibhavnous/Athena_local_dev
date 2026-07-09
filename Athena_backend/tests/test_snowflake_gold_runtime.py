from __future__ import annotations

import uuid
from pathlib import Path

from services import pipeline_runtime
from services import snowflake_gold_runtime


def _gold_sql() -> str:
    return """CREATE SCHEMA IF NOT EXISTS "ATHENA_DB"."GOLD";
CREATE TABLE IF NOT EXISTS "ATHENA_DB"."GOLD"."fact_total_claims" (
    "claim_status" VARCHAR,
    "total_claims_value" FLOAT,
    "kpi_name" VARCHAR,
    "gold_run_id" VARCHAR,
    "gold_processed_timestamp" TIMESTAMP_NTZ,
    "gold_upsert_key" VARCHAR
);
MERGE INTO "ATHENA_DB"."GOLD"."fact_total_claims" AS target
USING (
    WITH aggregate_data AS (
        SELECT
        "claim_status" AS "claim_status",
        COUNT(*) AS "total_claims_value"
        FROM "ATHENA_DB"."SILVER"."silver_claim_information"
        GROUP BY "claim_status"
    )
    SELECT
        "claim_status",
        "total_claims_value",
        'Total Claims' AS "kpi_name",
        'run-1' AS "gold_run_id",
        CURRENT_TIMESTAMP() AS "gold_processed_timestamp",
        MD5(CONCAT_WS('||', 'Total Claims', COALESCE(TO_VARCHAR("claim_status"), '__NULL__'))) AS "gold_upsert_key"
    FROM aggregate_data
) AS source
ON target."gold_upsert_key" = source."gold_upsert_key"
WHEN MATCHED THEN UPDATE SET
        target."claim_status" = source."claim_status",
        target."total_claims_value" = source."total_claims_value",
        target."kpi_name" = source."kpi_name",
        target."gold_run_id" = source."gold_run_id",
        target."gold_processed_timestamp" = source."gold_processed_timestamp"
WHEN NOT MATCHED THEN INSERT (
        "claim_status", "total_claims_value", "kpi_name", "gold_run_id", "gold_processed_timestamp", "gold_upsert_key"
    )
    VALUES (
        source."claim_status", source."total_claims_value", source."kpi_name", source."gold_run_id", source."gold_processed_timestamp", source."gold_upsert_key"
    );"""


def test_snowflake_gold_runtime_is_disabled_by_default(monkeypatch):
    monkeypatch.delenv("ATHENA_EXECUTE_SNOWFLAKE_GOLD", raising=False)

    result = snowflake_gold_runtime.run_snowflake_gold_scripts(
        {"target_warehouse": "snowflake", "gold_generation_results": [{"kpi_name": "Total Claims"}]}
    )

    assert result["snowflake_gold_execution_status"] == "DISABLED"


def test_snowflake_gold_runtime_executes_generated_scripts(monkeypatch):
    workdir = Path.cwd() / ".tmp-tests" / f"snowflake_gold_{uuid.uuid4().hex}"
    workdir.mkdir(parents=True, exist_ok=True)
    script_path = workdir / "gold_total_claims.sql"
    script_path.write_text(_gold_sql(), encoding="utf-8")

    class FakeSnowflakeConnection:
        def __init__(self):
            self.sql = []
            self.closed = False

        def execute_string(self, sql, return_cursors=True):
            self.sql.append(sql)
            return [object(), object(), object()]

        def close(self):
            self.closed = True

    fake_conn = FakeSnowflakeConnection()
    monkeypatch.setenv("ATHENA_EXECUTE_SNOWFLAKE_GOLD", "true")
    monkeypatch.setattr(snowflake_gold_runtime, "_snowflake_connect", lambda: fake_conn)

    result = snowflake_gold_runtime.run_snowflake_gold_scripts(
        {
            "target_warehouse": "snowflake",
            "gold_generation_results": [
                {
                    "kpi_name": "Total Claims",
                    "source_table": "ATHENA_DB.SILVER.silver_claim_information",
                    "target_table": "ATHENA_DB.GOLD.fact_total_claims",
                    "script_path": str(script_path),
                }
            ],
        }
    )

    assert result["snowflake_gold_execution_status"] == "COMPLETED"
    assert result["snowflake_gold_execution_results"][0]["statement_count"] == 3
    assert any('MERGE INTO "ATHENA_DB"."GOLD"."fact_total_claims"' in sql for sql in fake_conn.sql)
    assert fake_conn.closed is True


def test_snowflake_gold_runtime_reports_external_progress(monkeypatch):
    workdir = Path.cwd() / ".tmp-tests" / f"snowflake_gold_progress_{uuid.uuid4().hex}"
    workdir.mkdir(parents=True, exist_ok=True)
    script_path = workdir / "gold_total_claims.sql"
    script_path.write_text(_gold_sql(), encoding="utf-8")
    progress_calls = []

    class FakeSnowflakeConnection:
        def execute_string(self, sql, return_cursors=True):
            return [object()]

        def close(self):
            pass

    def fake_progress(state, **kwargs):
        progress_calls.append(kwargs)
        return {**state, "external_execution": kwargs}

    monkeypatch.setenv("ATHENA_EXECUTE_SNOWFLAKE_GOLD", "true")
    monkeypatch.setattr(snowflake_gold_runtime, "_snowflake_connect", lambda: FakeSnowflakeConnection())
    monkeypatch.setattr(snowflake_gold_runtime, "save_external_execution_progress", fake_progress)

    result = snowflake_gold_runtime.run_snowflake_gold_scripts(
        {
            "run_id": "run-gold-progress",
            "target_warehouse": "snowflake",
            "gold_generation_results": [
                {
                    "kpi_name": "Total Claims",
                    "source_table": "ATHENA_DB.SILVER.silver_claim_information",
                    "target_table": "ATHENA_DB.GOLD.fact_total_claims",
                    "script_path": str(script_path),
                }
            ],
        }
    )

    assert result["snowflake_gold_execution_status"] == "COMPLETED"
    assert [call["status"] for call in progress_calls] == ["RUNNING", "RUNNING", "RUNNING", "COMPLETED"]
    assert progress_calls[1]["current_index"] == 1
    assert progress_calls[1]["total_count"] == 1
    assert "Snowflake Gold execution running" in progress_calls[1]["message"]


def test_snowflake_gold_runtime_rejects_databricks_sql():
    try:
        snowflake_gold_runtime.validate_snowflake_gold_script(
            {
                "source_table": "ATHENA_DB.SILVER.silver_claim_information",
                "target_table": "ATHENA_DB.GOLD.fact_total_claims",
                "script_body": _gold_sql() + "\nSELECT pyspark;",
            }
        )
    except ValueError as exc:
        assert "databricks/python token" in str(exc).lower()
    else:
        raise AssertionError("Databricks-style Snowflake Gold SQL should be rejected")


def test_gold_stage_executes_snowflake_gold_after_generation(monkeypatch):
    saved_states = []
    calls = []

    def fake_gold_generation(state):
        return {
            **state,
            "gold_generation_status": "COMPLETED",
            "gold_generation_results": [{"kpi_name": "Total Claims", "script_body": _gold_sql()}],
        }

    def fake_gold_execution(state):
        calls.append(state.copy())
        return {
            **state,
            "snowflake_gold_execution_status": "COMPLETED",
            "snowflake_gold_execution_results": [{"kpi_name": "Total Claims"}],
        }

    monkeypatch.setattr("nodes.gold_gen.gold_code_generation_node", fake_gold_generation)
    monkeypatch.setattr("services.snowflake_gold_runtime.run_snowflake_gold_scripts", fake_gold_execution)
    monkeypatch.setattr(pipeline_runtime, "save_checkpoint_state", lambda run_id, state: saved_states.append(state.copy()))

    result = pipeline_runtime._run_database_gold_stage({"run_id": "run-gold", "target_warehouse": "snowflake"})

    assert calls
    assert calls[0]["background_stage"] == "gold_code_execution"
    assert result["status"] == "PIPELINE_COMPLETED"
    assert result["snowflake_gold_execution_status"] == "COMPLETED"
    assert any(state.get("background_stage") == "gold_code_execution" for state in saved_states)
