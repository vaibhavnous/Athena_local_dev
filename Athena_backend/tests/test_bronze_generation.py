from __future__ import annotations

import json
import uuid
from pathlib import Path

from nodes import bronze_gen


def test_snowflake_bronze_script_uses_sql_patterns():
    script = bronze_gen.generate_snowflake_bronze_script(
        table="Claims",
        schema="dbo",
        database="insurance",
        run_id="run-1",
        bronze_catalog="ATHENA_DB",
        bronze_schema="BRONZE",
        table_metadata={
            "columns": [
                {"column_name": "ClaimID", "data_type": "int"},
                {"column_name": "ClaimDate", "data_type": "datetime2"},
                {"column_name": "Amount", "data_type": "decimal", "numeric_precision": 12, "numeric_scale": 2},
            ]
        },
    )

    assert "Expected runtime: Snowflake SQL" in script
    assert 'CREATE SCHEMA IF NOT EXISTS "ATHENA_DB"."BRONZE";' in script
    assert 'CREATE TABLE IF NOT EXISTS "ATHENA_DB"."BRONZE"."bronze_Claims"' in script
    assert 'TRY_CAST(src."ClaimID" AS NUMBER(38,0)) AS "claimid"' in script
    assert 'TRY_CAST(src."ClaimDate" AS TIMESTAMP_NTZ) AS "claimdate"' in script
    assert 'TRY_CAST(src."Amount" AS NUMBER(12,2)) AS "amount"' in script
    assert 'INSERT INTO "ATHENA_DB"."BRONZE"."bronze_Claims"' in script


def test_snowflake_bronze_generation_writes_sql_without_databricks_path(monkeypatch):
    monkeypatch.setenv("ATHENA_ENABLE_LLM_SNOWFLAKE_BRONZE_ENHANCEMENT", "false")
    workdir = Path.cwd() / ".tmp-tests" / f"bronze_{uuid.uuid4().hex}"
    workdir.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(workdir)
    monkeypatch.setattr(
        bronze_gen,
        "build_source_jdbc_url",
        lambda database_name=None: (_ for _ in ()).throw(AssertionError("Databricks JDBC path should not run")),
    )

    state = {
        "run_id": "run-snowflake",
        "target_warehouse": "snowflake",
        "bronze_catalog": "ATHENA_DB",
        "bronze_schema": "BRONZE",
        "certified_tables": [
            {"database_name": "insurance", "schema_name": "dbo", "table_name": "Claims"}
        ],
        "discovered_metadata": {
            "tables": [
                {
                    "table_name": "Claims",
                    "columns": [
                        {"column_name": "ClaimID", "data_type": "int"},
                    ],
                }
            ]
        },
    }

    result = bronze_gen.bronze_code_generation_node(state)

    script_path = Path(result["bronze_generation_results"][0]["script_path"])
    bundle_path = Path(result["bronze_generation_bundle_path"])
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))

    assert result["bronze_generation_status"] == "COMPLETED"
    assert result["bronze_generation_results"][0]["target_warehouse"] == "snowflake"
    assert result["bronze_generation_results"][0]["script_language"] == "sql"
    assert script_path.suffix == ".sql"
    assert script_path.parts[-3:] == ("snowflake", "bronze", script_path.name)
    assert "Expected runtime: Snowflake SQL" in script_path.read_text(encoding="utf-8")
    assert bundle["target_warehouse"] == "snowflake"


def test_snowflake_bronze_generation_can_use_llm_enhancement(monkeypatch):
    monkeypatch.setenv("ATHENA_ENABLE_LLM_SNOWFLAKE_BRONZE_ENHANCEMENT", "true")
    workdir = Path.cwd() / ".tmp-tests" / f"bronze_llm_{uuid.uuid4().hex}"
    workdir.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(workdir)

    calls = []

    def fake_enhance(sql, metadata):
        calls.append(metadata)
        return sql + "\n-- llm enhanced\n"

    monkeypatch.setattr(bronze_gen, "_enhance_snowflake_with_llm", fake_enhance)

    result = bronze_gen._generate_one_table(
        {
            "database_name": "insurance",
            "schema_name": "dbo",
            "table_name": "Claims",
        },
        run_id="run-llm",
        bronze_catalog="ATHENA_DB",
        bronze_schema="BRONZE",
        table_metadata={
            "columns": [
                {"column_name": "ClaimID", "data_type": "int"},
            ]
        },
        target_warehouse="snowflake",
    )

    assert calls
    assert result["llm_enhanced"] is True
    assert result["llm_enhancement_error"] is None


def test_snowflake_bronze_generation_skips_llm_by_default(monkeypatch):
    monkeypatch.delenv("ATHENA_ENABLE_LLM_SNOWFLAKE_BRONZE_ENHANCEMENT", raising=False)

    called = {"enhance": 0}

    def fail_if_called(sql, metadata):
        called["enhance"] += 1
        raise AssertionError("Snowflake LLM enhancement should be opt-in")

    monkeypatch.setattr(bronze_gen, "_enhance_snowflake_with_llm", fail_if_called)

    result = bronze_gen._generate_one_table(
        {
            "database_name": "insurance",
            "schema_name": "dbo",
            "table_name": "claims",
        },
        run_id="run-no-llm",
        bronze_catalog="main",
        bronze_schema="bronze",
        cast_rules={"claim_id": "int"},
        table_metadata={"columns": [{"column_name": "CLAIM_ID", "data_type": "int"}]},
        target_warehouse="snowflake",
    )

    assert called["enhance"] == 0
    assert result["llm_enhanced"] is False
    assert result["llm_enhancement_error"] is None
    assert "-- llm enhanced" not in Path(result["script_path"]).read_text(encoding="utf-8")
