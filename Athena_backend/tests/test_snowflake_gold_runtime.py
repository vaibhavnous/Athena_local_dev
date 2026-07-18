from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from services import pipeline_runtime
from services import snowflake_gold_runtime
from nodes.gold_gen import _canonicalize_snowflake_gold_identifiers, _require_snowflake_gold_structure, _validate_snowflake_gold_candidate


def test_gold_llm_candidate_rejects_noncanonical_silver_column_case():
    mapping = {
        "source_silver_table": "ATHENA_DB.SILVER.silver_claims",
        "measure": {"column": "PaidAmount", "aggregation": "AVG"},
    }
    sql = '''
CREATE SCHEMA IF NOT EXISTS "ATHENA_DB"."GOLD";
CREATE TABLE IF NOT EXISTS "ATHENA_DB"."GOLD"."fact_average" ("value" NUMBER);
MERGE INTO "ATHENA_DB"."GOLD"."fact_average" target
USING (SELECT AVG("PaidAmount") AS "value" FROM "ATHENA_DB"."SILVER"."silver_claims") source
ON 1 = 0 WHEN NOT MATCHED THEN INSERT ("value") VALUES (source."value");
'''

    try:
        _validate_snowflake_gold_candidate(sql, mapping, "ATHENA_DB.GOLD.fact_average")
    except ValueError as exc:
        assert "non-canonical Silver identifiers" in str(exc)
    else:
        raise AssertionError("Expected non-canonical Silver identifier casing to be rejected")


def test_gold_llm_candidate_repairs_canonical_silver_column_case():
    mapping = {
        "source_silver_table": "ATHENA_DB.SILVER.silver_claims",
        "measure": {"column": "PaidAmount", "aggregation": "AVG"},
    }
    sql = '''
CREATE SCHEMA IF NOT EXISTS "ATHENA_DB"."GOLD";
CREATE TABLE IF NOT EXISTS "ATHENA_DB"."GOLD"."fact_average" ("value" NUMBER);
MERGE INTO "ATHENA_DB"."GOLD"."fact_average" target
USING (SELECT AVG("PaidAmount") AS "value" FROM "ATHENA_DB"."SILVER"."silver_claims") source
ON 1 = 0 WHEN NOT MATCHED THEN INSERT ("value") VALUES (source."value");
'''

    repaired = _canonicalize_snowflake_gold_identifiers(sql, mapping)

    assert 'AVG("paidamount")' in repaired
    _validate_snowflake_gold_candidate(repaired, mapping, "ATHENA_DB.GOLD.fact_average")


def test_gold_llm_candidate_repairs_corrected_count_identifier():
    mapping = {
        "source_silver_table": "ATHENA_DB.SILVER.silver_policy_transactions",
        "measure": {"column": "RERERENCE_ID", "aggregation": "COUNT"},
    }
    sql = '''
CREATE SCHEMA IF NOT EXISTS "ATHENA_DB"."GOLD";
CREATE TABLE IF NOT EXISTS "ATHENA_DB"."GOLD"."fact_unique" ("value" NUMBER);
MERGE INTO "ATHENA_DB"."GOLD"."fact_unique" AS target
USING (SELECT COUNT(DISTINCT "RERERENCE_ID") AS "value" FROM "ATHENA_DB"."SILVER"."silver_policy_transactions") AS source
ON 1 = 0 WHEN NOT MATCHED THEN INSERT ("value") VALUES (source."value");
'''

    repaired = _canonicalize_snowflake_gold_identifiers(sql, mapping)

    assert 'COUNT(DISTINCT "reference_id")' in repaired
    _validate_snowflake_gold_candidate(repaired, mapping, "ATHENA_DB.GOLD.fact_unique")


def test_gold_llm_candidate_rejects_unknown_source_identifier():
    mapping = {
        "source_silver_table": "ATHENA_DB.SILVER.silver_policy_transactions",
        "measure": {"column": "RERERENCE_ID", "aggregation": "COUNT"},
    }
    sql = '''
CREATE SCHEMA IF NOT EXISTS "ATHENA_DB"."GOLD";
CREATE TABLE IF NOT EXISTS "ATHENA_DB"."GOLD"."fact_unique" ("value" NUMBER);
MERGE INTO "ATHENA_DB"."GOLD"."fact_unique" AS target
USING (SELECT COUNT(DISTINCT "CLAIM_NUMBER") AS "value" FROM "ATHENA_DB"."SILVER"."silver_policy_transactions") AS source
ON 1 = 0 WHEN NOT MATCHED THEN INSERT ("value") VALUES (source."value");
'''

    with pytest.raises(ValueError, match="non-contract Silver identifiers"):
        _validate_snowflake_gold_candidate(sql, mapping, "ATHENA_DB.GOLD.fact_unique")


def test_gold_llm_repair_preserves_output_alias_case():
    mapping = {
        "source_silver_table": "ATHENA_DB.SILVER.silver_claims",
        "measure": {"column": "PaidAmount", "aggregation": "AVG"},
    }
    sql = '''
WITH aggregate_data AS (
    SELECT AVG("PaidAmount") AS "PaidAmount"
    FROM "ATHENA_DB"."SILVER"."silver_claims"
)
SELECT "PaidAmount" FROM aggregate_data;
'''

    repaired = _canonicalize_snowflake_gold_identifiers(sql, mapping)

    assert 'AVG("paidamount") AS "PaidAmount"' in repaired
    assert 'SELECT "PaidAmount" FROM aggregate_data' in repaired


def test_gold_llm_rejects_comment_only_source_and_destructive_sql():
    mapping = {"source_silver_table": "ATHENA_DB.SILVER.silver_claims"}
    sql = '''
-- FROM "ATHENA_DB"."SILVER"."silver_claims"
MERGE INTO "ATHENA_DB"."GOLD"."fact_average" AS target
USING (SELECT * FROM "OTHER_DB"."SILVER"."claims") source ON 1 = 0
WHEN NOT MATCHED THEN INSERT DEFAULT VALUES;
DELETE FROM "ATHENA_DB"."SILVER"."silver_claims";
'''

    with pytest.raises(ValueError, match="approved Silver table"):
        _require_snowflake_gold_structure(sql, mapping, "ATHENA_DB.GOLD.fact_average")


def test_gold_llm_rejects_join_outside_approved_source():
    mapping = {"source_silver_table": "ATHENA_DB.SILVER.silver_claims"}
    sql = '''
MERGE INTO "ATHENA_DB"."GOLD"."fact_average" AS target
USING (
    SELECT COUNT(*) AS "value"
    FROM "ATHENA_DB"."SILVER"."silver_claims" AS claims
    JOIN "ATHENA_DB"."SILVER"."silver_payments" AS payments ON 1 = 1
) AS source ON 1 = 0
WHEN NOT MATCHED THEN INSERT DEFAULT VALUES;
'''

    with pytest.raises(ValueError, match="must not add joins"):
        _require_snowflake_gold_structure(sql, mapping, "ATHENA_DB.GOLD.fact_average")


def test_gold_runtime_rejects_destructive_stored_sql():
    sql = '''
MERGE INTO "ATHENA_DB"."GOLD"."fact_average" AS target
USING (SELECT * FROM "ATHENA_DB"."SILVER"."silver_claims") source ON 1 = 0
WHEN NOT MATCHED THEN INSERT DEFAULT VALUES;
DROP TABLE "ATHENA_DB"."GOLD"."fact_average";
'''

    with pytest.raises(ValueError, match="forbidden statement: DROP"):
        snowflake_gold_runtime._require_approved_snowflake_structure(
            sql,
            "ATHENA_DB.SILVER.silver_claims",
            "ATHENA_DB.GOLD.fact_average",
        )


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


def _dimension_sql() -> str:
    return """CREATE SCHEMA IF NOT EXISTS "ATHENA_DB"."GOLD";
CREATE TABLE IF NOT EXISTS "ATHENA_DB"."GOLD"."dim_claim" (
    "claim_key" VARCHAR,
    "natural_key_hash" VARCHAR,
    "attribute_hash" VARCHAR,
    "claim_status" VARCHAR,
    "is_current" BOOLEAN
);
MERGE INTO "ATHENA_DB"."GOLD"."dim_claim" AS target
USING (
    SELECT DISTINCT
        MD5(CONCAT_WS('||', COALESCE(TO_VARCHAR("claimstatus"), '__NULL__'))) AS "claim_key",
        MD5(CONCAT_WS('||', COALESCE(TO_VARCHAR("claimstatus"), '__NULL__'))) AS "natural_key_hash",
        MD5(CONCAT_WS('||', COALESCE(TO_VARCHAR("claimstatus"), '__NULL__'))) AS "attribute_hash",
        "claimstatus" AS "claim_status",
        TRUE AS "is_current"
    FROM "ATHENA_DB"."SILVER"."silver_claim_information"
) AS source
ON target."natural_key_hash" = source."natural_key_hash" AND target."is_current" = TRUE
WHEN MATCHED THEN UPDATE SET
        target."claim_status" = source."claim_status",
        target."attribute_hash" = source."attribute_hash",
        target."is_current" = source."is_current"
WHEN NOT MATCHED THEN INSERT (
        "claim_key", "natural_key_hash", "attribute_hash", "claim_status", "is_current"
    )
    VALUES (
        source."claim_key", source."natural_key_hash", source."attribute_hash", source."claim_status", source."is_current"
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
    dimension_script_path = workdir / "gold_dim_total_claims.sql"
    dimension_script_path.write_text(_dimension_sql(), encoding="utf-8")

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
                    "dimension_script_path": str(dimension_script_path),
                }
            ],
        }
    )

    assert result["snowflake_gold_execution_status"] == "COMPLETED"
    assert result["snowflake_gold_execution_results"][0]["statement_count"] == 3
    assert result["snowflake_gold_execution_results"][0]["dimension_statement_count"] == 3
    assert 'MERGE INTO "ATHENA_DB"."GOLD"."dim_claim"' in fake_conn.sql[0]
    assert any('MERGE INTO "ATHENA_DB"."GOLD"."fact_total_claims"' in sql for sql in fake_conn.sql)
    assert fake_conn.closed is True


def test_snowflake_gold_runtime_normalizes_timestamp_parse_for_existing_artifacts():
    sql = _gold_sql().replace(
        'GROUP BY "claim_status"',
        'GROUP BY DATE_TRUNC(\'month\', TRY_TO_TIMESTAMP_NTZ("paiddate"))',
    )
    sql = sql.replace(
        'COUNT(*) AS "total_claims_value"',
        'DATE_TRUNC(\'month\', TRY_TO_TIMESTAMP_NTZ("paiddate")) AS "period_start",\n        SUM(TRY_TO_DECIMAL("grossestimate")) AS "total_claims_value"',
    )

    class FakeSnowflakeConnection:
        def __init__(self):
            self.sql = ""

        def execute_string(self, sql, return_cursors=True):
            self.sql = sql
            return [object()]

    fake_conn = FakeSnowflakeConnection()

    snowflake_gold_runtime.execute_snowflake_gold_sql(
        {
            "kpi_name": "Total Claims",
            "source_table": "ATHENA_DB.SILVER.silver_claim_information",
            "target_table": "ATHENA_DB.GOLD.fact_total_claims",
            "script_body": sql,
        },
        fake_conn,
    )

    assert 'TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR("paiddate"))' in fake_conn.sql
    assert 'TRY_TO_TIMESTAMP_NTZ("paiddate")' not in fake_conn.sql
    assert 'TRY_TO_DECIMAL(TO_VARCHAR("grossestimate"))' in fake_conn.sql
    assert 'TRY_TO_DECIMAL("grossestimate")' not in fake_conn.sql
    assert (
        'ALTER TABLE "ATHENA_DB"."GOLD"."fact_total_claims" '
        'ADD COLUMN IF NOT EXISTS "claim_status" VARCHAR;'
    ) in fake_conn.sql


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


def test_snowflake_gold_catalog_preflight_rejects_missing_contract_column():
    class CatalogCursor:
        def execute(self, sql):
            self.sql = sql

        def fetchall(self):
            return [("claim_status",)]

    class CatalogConnection:
        def cursor(self):
            return CatalogCursor()

    try:
        snowflake_gold_runtime.validate_snowflake_gold_script(
            {
                "kpi_name": "Total Claims",
                "source_table": "ATHENA_DB.SILVER.silver_claim_information",
                "target_table": "ATHENA_DB.GOLD.fact_total_claims",
                "validation_columns": ["claim_status", "missing_measure"],
                "script_body": _gold_sql().replace(
                    '"claim_status" VARCHAR,',
                    '"claim_status" VARCHAR,\n    "missing_measure" FLOAT,',
                ),
            },
            catalog_connection=CatalogConnection(),
        )
    except ValueError as exc:
        assert "missing column(s): missing_measure" in str(exc)
    else:
        raise AssertionError("Gold catalog preflight should reject an unknown contract column")


def test_snowflake_gold_preflight_rejects_noncanonical_silver_case():
    class CatalogCursor:
        def execute(self, sql):
            self.sql = sql

        def fetchall(self):
            return [("inserteddate",)]

    class CatalogConnection:
        def cursor(self):
            return CatalogCursor()

    sql = _gold_sql().replace('"claim_status"', '"InsertedDate"')
    try:
        snowflake_gold_runtime.validate_snowflake_gold_script(
            {
                "kpi_name": "Total Claims",
                "source_table": "ATHENA_DB.SILVER.silver_claim_information",
                "target_table": "ATHENA_DB.GOLD.fact_total_claims",
                "validation_columns": ["inserteddate"],
                "script_body": sql,
            },
            catalog_connection=CatalogConnection(),
        )
    except ValueError as exc:
        assert "canonical Silver column" in str(exc)
    else:
        raise AssertionError("Gold preflight should reject case-sensitive Silver identifier drift")


def test_snowflake_dimension_catalog_preflight_checks_each_logical_source_table():
    calls = []

    class CatalogCursor:
        def execute(self, sql):
            calls.append(sql)

        def fetchall(self):
            return [("claim_status",), ("policy_type",)]

    class CatalogConnection:
        def cursor(self):
            return CatalogCursor()

    path = Path.cwd() / ".tmp-tests" / "shared-dimension-preflight.sql"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_dimension_sql().replace('"claim_status" VARCHAR,', '"claim_status" VARCHAR,\n    "policy_type" VARCHAR,'), encoding="utf-8")
    snowflake_gold_runtime.validate_snowflake_dimension_script(
        {
            "kpi_name": "Shared dimensions",
            "source_table": "ATHENA_DB.SILVER.silver_claim_information",
            "dimension_script_path": str(path),
            "dimension_contract": [
                {"logical_table": "claim_information", "columns": ["claim_status"]},
                {"logical_table": "policy_transactions", "columns": ["policy_type"]},
            ],
        },
        catalog_connection=CatalogConnection(),
    )
    assert any("silver_claim_information" in sql for sql in calls)
    assert any("silver_policy_transactions" in sql for sql in calls)


def test_gold_stage_waits_for_review_before_snowflake_execution(monkeypatch):
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

    assert not calls
    assert result["status"] == "HITL_WAIT"
    assert result["next_review_key"] == "gold_review"
    assert result["gold_review_artifact"]["items"]

    monkeypatch.setattr(pipeline_runtime, "load_checkpoint_state", lambda _run_id: result)
    completed = pipeline_runtime.submit_gold_review("run-gold", "APPROVED", result["gold_review_artifact"])

    assert calls
    assert calls[0]["background_stage"] == "gold_code_execution"
    assert completed["status"] == "PIPELINE_COMPLETED"
    assert completed["snowflake_gold_execution_status"] == "COMPLETED"
    assert any(state.get("background_stage") == "gold_code_execution" for state in saved_states)


def test_gold_stage_executes_databricks_gold_after_review(monkeypatch):
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
            "databricks_gold_execution_status": "COMPLETED",
            "databricks_gold_execution_results": [{"kpi_name": "Total Claims"}],
        }

    monkeypatch.setattr("nodes.gold_gen.gold_code_generation_node", fake_gold_generation)
    monkeypatch.setattr("services.databricks_runtime.databricks_gold_execution_enabled", lambda: True)
    monkeypatch.setattr("services.databricks_runtime.run_databricks_gold_scripts", fake_gold_execution)
    monkeypatch.setattr(pipeline_runtime, "save_checkpoint_state", lambda run_id, state: saved_states.append(state.copy()))

    result = pipeline_runtime._run_database_gold_stage({"run_id": "run-gold", "target_warehouse": "databricks"})

    assert not calls
    assert result["status"] == "HITL_WAIT"
    assert result["next_review_key"] == "gold_review"
    assert result["gold_review_artifact"]["items"]

    monkeypatch.setattr(pipeline_runtime, "load_checkpoint_state", lambda _run_id: result)
    completed = pipeline_runtime.submit_gold_review("run-gold", "APPROVED", result["gold_review_artifact"])

    assert calls
    assert calls[0]["background_stage"] == "gold_code_execution"
    assert completed["status"] == "PIPELINE_COMPLETED"
    assert completed["databricks_gold_execution_status"] == "COMPLETED"
    assert any(state.get("background_stage") == "gold_code_execution" for state in saved_states)
    assert saved_states[-1]["background_stage"] is None
    assert saved_states[-1]["status"] == "PIPELINE_COMPLETED"
