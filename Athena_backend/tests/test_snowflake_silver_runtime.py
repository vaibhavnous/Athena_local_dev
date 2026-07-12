from __future__ import annotations

import uuid
from pathlib import Path

from services import pipeline_runtime
from services import snowflake_silver_runtime


def _silver_sql(table: str = "claims") -> str:
    return f"""CREATE SCHEMA IF NOT EXISTS "ATHENA_DB"."SILVER";
CREATE TABLE IF NOT EXISTS "ATHENA_DB"."SILVER"."silver_{table}" (
    "claim_id" NUMBER(38,0),
    "silver_upsert_key" VARCHAR,
    "silver_run_id" VARCHAR,
    "silver_processed_timestamp" TIMESTAMP_NTZ
);
MERGE INTO "ATHENA_DB"."SILVER"."silver_{table}" AS target
USING (
    SELECT
        src."claim_id",
        SHA2(COALESCE(TO_VARCHAR(src."claim_id"), '__NULL__'), 256) AS "silver_upsert_key",
        'run-1' AS "silver_run_id",
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS "silver_processed_timestamp"
    FROM "ATHENA_DB"."BRONZE"."bronze_{table}" AS src
) AS source
ON target."silver_upsert_key" = source."silver_upsert_key"
WHEN MATCHED THEN UPDATE SET
    target."claim_id" = source."claim_id",
    target."silver_run_id" = source."silver_run_id",
    target."silver_processed_timestamp" = source."silver_processed_timestamp"
WHEN NOT MATCHED THEN INSERT (
    "claim_id",
    "silver_upsert_key",
    "silver_run_id",
    "silver_processed_timestamp"
)
VALUES (
    source."claim_id",
    source."silver_upsert_key",
    source."silver_run_id",
    source."silver_processed_timestamp"
);"""


def test_snowflake_silver_runtime_is_disabled_by_default(monkeypatch):
    monkeypatch.delenv("ATHENA_EXECUTE_SNOWFLAKE_SILVER", raising=False)

    result = snowflake_silver_runtime.run_snowflake_silver_scripts(
        {"target_warehouse": "snowflake", "silver_generation_results": [{"table": "claims"}]}
    )

    assert result["snowflake_silver_execution_status"] == "DISABLED"


def test_snowflake_silver_runtime_executes_only_approved_review_items(monkeypatch):
    workdir = Path.cwd() / ".tmp-tests" / f"snowflake_silver_{uuid.uuid4().hex}"
    workdir.mkdir(parents=True, exist_ok=True)
    claims_path = workdir / "silver_claims.sql"
    policy_path = workdir / "silver_policy.sql"
    claims_path.write_text(_silver_sql("claims"), encoding="utf-8")
    policy_path.write_text(_silver_sql("policy"), encoding="utf-8")

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
    monkeypatch.setenv("ATHENA_EXECUTE_SNOWFLAKE_SILVER", "true")
    monkeypatch.setattr(snowflake_silver_runtime, "_snowflake_connect", lambda: fake_conn)

    result = snowflake_silver_runtime.run_snowflake_silver_scripts(
        {
            "target_warehouse": "snowflake",
            "silver_generation_results": [
                {
                    "table": "claims",
                    "source_table": "ATHENA_DB.BRONZE.bronze_claims",
                    "target_table": "ATHENA_DB.SILVER.silver_claims",
                    "script_path": str(claims_path),
                },
                {
                    "table": "policy",
                    "source_table": "ATHENA_DB.BRONZE.bronze_policy",
                    "target_table": "ATHENA_DB.SILVER.silver_policy",
                    "script_path": str(policy_path),
                },
            ],
        },
        review_artifact={
            "items": [
                {
                    "table": "claims",
                    "target_table": "ATHENA_DB.SILVER.silver_claims",
                    "review_status": "APPROVED",
                },
                {
                    "table": "policy",
                    "target_table": "ATHENA_DB.SILVER.silver_policy",
                    "review_status": "REJECTED",
                },
            ]
        },
        approved_only=True,
    )

    assert result["snowflake_silver_execution_status"] == "COMPLETED"
    assert [item["table"] for item in result["snowflake_silver_execution_results"]] == ["claims"]
    assert result["snowflake_silver_execution_results"][0]["statement_count"] == 3
    assert any('MERGE INTO "ATHENA_DB"."SILVER"."silver_claims"' in sql for sql in fake_conn.sql)
    assert not any('MERGE INTO "ATHENA_DB"."SILVER"."silver_policy"' in sql for sql in fake_conn.sql)
    assert fake_conn.closed is True


def test_snowflake_silver_runtime_prefers_reviewed_script_body(monkeypatch):
    class FakeSnowflakeConnection:
        def __init__(self):
            self.sql = []

        def execute_string(self, sql, return_cursors=True):
            self.sql.append(sql)
            return [object()]

        def close(self):
            pass

    fake_conn = FakeSnowflakeConnection()
    monkeypatch.setenv("ATHENA_EXECUTE_SNOWFLAKE_SILVER", "true")
    monkeypatch.setattr(snowflake_silver_runtime, "_snowflake_connect", lambda: fake_conn)

    result = snowflake_silver_runtime.run_snowflake_silver_scripts(
        {
            "target_warehouse": "snowflake",
            "silver_generation_results": [
                {
                    "table": "claims",
                    "source_table": "ATHENA_DB.BRONZE.bronze_claims",
                    "target_table": "ATHENA_DB.SILVER.silver_claims",
                    "script_path": "missing.sql",
                }
            ],
        },
        review_artifact={
            "items": [
                {
                    "table": "claims",
                    "target_table": "ATHENA_DB.SILVER.silver_claims",
                    "source_table": "ATHENA_DB.BRONZE.bronze_claims",
                    "script_body": _silver_sql("claims") + "\n-- reviewed edit",
                    "review_status": "APPROVED",
                }
            ]
        },
        approved_only=True,
    )

    assert result["snowflake_silver_execution_status"] == "COMPLETED"
    assert "-- reviewed edit" in fake_conn.sql[0]


def test_snowflake_silver_review_uses_selected_subset_and_keep_all_pending_legacy():
    scripts = [
        {
            "table": "claims",
            "source_table": "ATHENA_DB.BRONZE.bronze_claims",
            "target_table": "ATHENA_DB.SILVER.silver_claims",
            "script_path": "claims.sql",
        },
        {
            "table": "policy",
            "source_table": "ATHENA_DB.BRONZE.bronze_policy",
            "target_table": "ATHENA_DB.SILVER.silver_policy",
            "script_path": "policy.sql",
        },
    ]

    selected = snowflake_silver_runtime._approved_review_scripts(
        {"silver_generation_results": scripts},
        {
            "items": [
                {"table": "claims", "target_table": "ATHENA_DB.SILVER.silver_claims", "review_status": "APPROVED"},
                {"table": "policy", "target_table": "ATHENA_DB.SILVER.silver_policy", "review_status": "PENDING"},
            ]
        },
    )
    legacy_all = snowflake_silver_runtime._approved_review_scripts(
        {"silver_generation_results": scripts},
        {
            "items": [
                {"table": "claims", "target_table": "ATHENA_DB.SILVER.silver_claims", "review_status": "PENDING"},
                {"table": "policy", "target_table": "ATHENA_DB.SILVER.silver_policy", "review_status": "PENDING"},
            ]
        },
    )

    assert [item["script_path"] for item in selected] == ["claims.sql"]
    assert [item["script_path"] for item in legacy_all] == ["claims.sql", "policy.sql"]


def test_snowflake_silver_runtime_rejects_databricks_sql():
    try:
        snowflake_silver_runtime.validate_snowflake_silver_script(
            {
                "source_table": "ATHENA_DB.BRONZE.bronze_claims",
                "target_table": "ATHENA_DB.SILVER.silver_claims",
                "script_body": (
                    'CREATE SCHEMA IF NOT EXISTS "ATHENA_DB"."SILVER"; '
                    'CREATE TABLE IF NOT EXISTS "ATHENA_DB"."SILVER"."silver_claims" ("x" VARCHAR); '
                    'MERGE INTO "ATHENA_DB"."SILVER"."silver_claims" AS target '
                    'USING (SELECT * FROM "ATHENA_DB"."BRONZE"."bronze_claims") AS source '
                    'ON target."x" = source."x" '
                    'WHEN MATCHED THEN UPDATE SET target."x" = source."x" '
                    'WHEN NOT MATCHED THEN INSERT ("x") VALUES (source."x"); '
                    'SELECT pyspark;'
                ),
            }
        )
    except ValueError as exc:
        assert "databricks/python token" in str(exc).lower()
    else:
        raise AssertionError("Databricks-style Snowflake Silver SQL should be rejected")


def test_snowflake_silver_catalog_preflight_rejects_missing_source_column():
    class CatalogCursor:
        def execute(self, sql):
            self.sql = sql

        def fetchall(self):
            return [("claim_id",), ("run_id",), ("ingestion_timestamp",), ("source_system",), ("source_table",)]

    class CatalogConnection:
        def cursor(self):
            return CatalogCursor()

    sql = _silver_sql("claims").replace('src."claim_id"', "GET_IGNORE_CASE(OBJECT_CONSTRUCT_KEEP_NULL(src.*), 'missing_id')")

    try:
        snowflake_silver_runtime.validate_snowflake_silver_script(
            {
                "table": "claims",
                "source_table": "ATHENA_DB.BRONZE.bronze_claims",
                "target_table": "ATHENA_DB.SILVER.silver_claims",
                "script_body": sql,
            },
            catalog_connection=CatalogConnection(),
        )
    except ValueError as exc:
        assert "missing column(s): missing_id" in str(exc)
    else:
        raise AssertionError("Silver catalog preflight should reject an unknown source column")


def test_snowflake_silver_catalog_preflight_rejects_quoted_case_mismatch():
    class CatalogCursor:
        def execute(self, sql):
            self.sql = sql

        def fetchall(self):
            return [("rererence_id",), ("run_id",), ("ingestion_timestamp",), ("source_system",), ("source_table",)]

    class CatalogConnection:
        def cursor(self):
            return CatalogCursor()

    sql = _silver_sql("claims").replace(
        '"ATHENA_DB"."BRONZE"."bronze_claims"',
        '"ATHENA_DB"."BRONZE"."bronze_policy_cover_level_transactions"',
    ).replace(
        '"ATHENA_DB"."SILVER"."silver_claims"',
        '"ATHENA_DB"."SILVER"."silver_policy_cover_level_transactions"',
    ).replace('src."claim_id"', 'src."RERERENCE_ID"')

    try:
        snowflake_silver_runtime.validate_snowflake_silver_script(
            {
                "table": "policy_cover_level_transactions",
                "source_table": "ATHENA_DB.BRONZE.bronze_policy_cover_level_transactions",
                "target_table": "ATHENA_DB.SILVER.silver_policy_cover_level_transactions",
                "script_body": sql,
            },
            catalog_connection=CatalogConnection(),
        )
    except ValueError as exc:
        assert "RERERENCE_ID" in str(exc)
    else:
        raise AssertionError("Quoted Snowflake source identifiers must match catalog case exactly")


def test_submit_gate5_review_executes_snowflake_silver_before_gold(monkeypatch):
    calls = []
    saved_states = []

    def fake_run(state, *, review_artifact=None, approved_only=False):
        calls.append((state, review_artifact, approved_only))
        return {
            **state,
            "snowflake_silver_execution_status": "COMPLETED",
            "snowflake_silver_execution_results": [{"table": "claims"}],
        }

    monkeypatch.setattr(pipeline_runtime, "load_checkpoint_state", lambda run_id: {
        "run_id": run_id,
        "target_warehouse": "snowflake",
        "silver_generation_results": [{"table": "claims"}],
    })
    monkeypatch.setattr(pipeline_runtime, "save_checkpoint_state", lambda run_id, state: saved_states.append(state.copy()))
    monkeypatch.setattr(pipeline_runtime, "ai_store_db_writer", lambda **_: None)
    monkeypatch.setattr(
        pipeline_runtime,
        "continue_database_pipeline",
        lambda run_id, start_stage_key, state: {"continued": start_stage_key, **state},
    )
    monkeypatch.setattr(
        "services.snowflake_silver_runtime.run_snowflake_silver_scripts",
        fake_run,
    )

    result = pipeline_runtime.submit_gate5_review(
        "run-gate5",
        action="APPROVED",
        review_artifact={"items": [{"table": "claims", "review_status": "APPROVED"}]},
    )

    assert calls
    assert calls[0][2] is True
    assert calls[0][0]["background_stage"] == "silver_code_execution"
    assert result["continued"] == "gold"
    assert result["snowflake_silver_execution_status"] == "COMPLETED"
    assert any(state.get("background_stage") == "silver_code_execution" for state in saved_states)
