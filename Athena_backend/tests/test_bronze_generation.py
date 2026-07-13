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
    monkeypatch.setenv("ATHENA_SNOWFLAKE_BRONZE_TABLE_ALLOWLIST", "*")
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
    assert result["bronze_generation_results"][0]["source_table"] == "insurance.dbo.Claims"
    assert result["bronze_generation_results"][0]["target_table"] == "ATHENA_DB.BRONZE.bronze_Claims"
    assert script_path.suffix == ".sql"
    assert script_path.parts[-3:] == ("snowflake", "bronze", script_path.name)
    assert "Expected runtime: Snowflake SQL" in script_path.read_text(encoding="utf-8")
    assert bundle["target_warehouse"] == "snowflake"


def test_bronze_generation_avoids_case_only_duplicate_source_tables(monkeypatch):
    monkeypatch.setenv("ATHENA_ENABLE_LLM_SNOWFLAKE_BRONZE_ENHANCEMENT", "false")
    monkeypatch.setenv("ATHENA_SNOWFLAKE_BRONZE_TABLE_ALLOWLIST", "*")
    workdir = Path.cwd() / ".tmp-tests" / f"bronze_case_{uuid.uuid4().hex}"
    workdir.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(workdir)

    state = {
        "run_id": "run-case",
        "target_warehouse": "snowflake",
        "bronze_catalog": "ATHENA_DB",
        "bronze_schema": "BRONZE",
        "certified_tables": [
            {"database_name": "insurance", "schema_name": "dbo", "table_name": "policy_cover_level_transactions_dup_del"},
            {"database_name": "insurance", "schema_name": "dbo", "table_name": "policy_cover_level_transactions_Dup_Del"},
        ],
        "discovered_metadata": {
            "tables": [
                {
                    "table_name": "policy_cover_level_transactions_dup_del",
                    "columns": [{"column_name": "PolicyID", "data_type": "int"}],
                }
            ]
        },
    }

    result = bronze_gen.bronze_code_generation_node(state)
    scripts = result["bronze_generation_results"]

    assert result["bronze_generation_status"] == "COMPLETED"
    assert [item["table"] for item in scripts] == ["policy_cover_level_transactions_dup_del"]
    assert Path(scripts[0]["script_path"]).exists()


def test_snowflake_bronze_generation_uses_selected_tables_without_default_allowlist(monkeypatch):
    monkeypatch.setenv("ATHENA_ENABLE_LLM_SNOWFLAKE_BRONZE_ENHANCEMENT", "false")
    monkeypatch.delenv("ATHENA_SNOWFLAKE_BRONZE_TABLE_ALLOWLIST", raising=False)
    monkeypatch.delenv("SNOWFLAKE_BRONZE_CATALOG", raising=False)
    monkeypatch.delenv("SNOWFLAKE_BRONZE_SCHEMA", raising=False)
    workdir = Path.cwd() / ".tmp-tests" / f"bronze_allowlist_{uuid.uuid4().hex}"
    workdir.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(workdir)

    state = {
        "run_id": "run-small-insurance",
        "target_warehouse": "snowflake",
        "bronze_catalog": "INSURANCE",
        "bronze_schema": "BRONZE",
        "certified_tables": [
            {"database_name": "insurance", "schema_name": "dbo", "table_name": "claim_information"},
            {"database_name": "insurance", "schema_name": "dbo", "table_name": "policy_cover_level_transactions"},
        ],
        "discovered_metadata": {
            "tables": [
                {"table_name": "claim_information", "columns": [{"column_name": "ClaimID", "data_type": "int"}]},
                {"table_name": "policy_cover_level_transactions", "columns": [{"column_name": "PolicyID", "data_type": "int"}]},
            ]
        },
    }

    result = bronze_gen.bronze_code_generation_node(state)
    scripts = sorted(result["bronze_generation_results"], key=lambda item: item["table"])
    script_sql = "\n".join(Path(item["script_path"]).read_text(encoding="utf-8") for item in scripts)

    assert result["bronze_generation_status"] == "COMPLETED"
    assert [item["table"] for item in scripts] == ["claim_information", "policy_cover_level_transactions"]
    assert {item["bronze_catalog"] for item in scripts} == {"ATHENA_DB"}
    assert {item["bronze_schema"] for item in scripts} == {"BRONZE"}
    assert 'CREATE SCHEMA IF NOT EXISTS "ATHENA_DB"."BRONZE"' in script_sql
    assert '"INSURANCE"."BRONZE"' not in script_sql
    assert result["bronze_generation_skipped_tables"] == []


def test_snowflake_bronze_generation_respects_optional_table_allowlist(monkeypatch):
    monkeypatch.setenv("ATHENA_ENABLE_LLM_SNOWFLAKE_BRONZE_ENHANCEMENT", "false")
    monkeypatch.setenv("ATHENA_SNOWFLAKE_BRONZE_TABLE_ALLOWLIST", "claim_information")
    workdir = Path.cwd() / ".tmp-tests" / f"bronze_allowlist_{uuid.uuid4().hex}"
    workdir.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(workdir)

    state = {
        "run_id": "run-explicit-allowlist",
        "target_warehouse": "snowflake",
        "certified_tables": [
            {"database_name": "insurance", "schema_name": "dbo", "table_name": "claim_information"},
            {"database_name": "insurance", "schema_name": "dbo", "table_name": "policy_cover_level_transactions"},
        ],
        "discovered_metadata": {
            "tables": [
                {"table_name": "claim_information", "columns": [{"column_name": "ClaimID", "data_type": "int"}]},
                {"table_name": "policy_cover_level_transactions", "columns": [{"column_name": "PolicyID", "data_type": "int"}]},
            ]
        },
    }

    result = bronze_gen.bronze_code_generation_node(state)

    assert result["bronze_generation_status"] == "COMPLETED"
    assert [item["table"] for item in result["bronze_generation_results"]] == ["claim_information"]
    assert [item["table_name"] for item in result["bronze_generation_skipped_tables"]] == ["policy_cover_level_transactions"]


def test_bronze_script_filename_is_safe_for_case_variant_tables():
    lower = bronze_gen._bronze_script_filename(
        run_id="run-case",
        database_name="insurance",
        schema_name="dbo",
        table_name="policy_cover_level_transactions_dup_del",
        extension="sql",
    )
    mixed = bronze_gen._bronze_script_filename(
        run_id="run-case",
        database_name="insurance",
        schema_name="dbo",
        table_name="policy_cover_level_transactions_Dup_Del",
        extension="sql",
    )

    assert lower.casefold() != mixed.casefold()


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


def test_snowflake_llm_enhancement_falls_back_when_target_drifted(monkeypatch):
    monkeypatch.setenv("ATHENA_ENABLE_LLM_SNOWFLAKE_BRONZE_ENHANCEMENT", "true")

    def wrong_target(sql, metadata):
        return sql.replace('"ATHENA_DB"."BRONZE"."bronze_claims"', '"OTHER_DB"."BRONZE"."bronze_claims"')

    monkeypatch.setattr(bronze_gen, "_enhance_snowflake_with_llm", wrong_target)

    result = bronze_gen._generate_one_table(
        {
            "database_name": "insurance",
            "schema_name": "dbo",
            "table_name": "claims",
        },
        run_id="run-target-check",
        bronze_catalog="ATHENA_DB",
        bronze_schema="BRONZE",
        table_metadata={"columns": [{"column_name": "CLAIM_ID", "data_type": "int"}]},
        target_warehouse="snowflake",
    )

    sql = Path(result["script_path"]).read_text(encoding="utf-8")
    assert result["llm_enhanced"] is False
    assert '"ATHENA_DB"."BRONZE"."bronze_claims"' in sql
    assert "expected target table" in str(result["llm_enhancement_error"]).lower()


def test_snowflake_validator_rejects_databricks_format():
    try:
        bronze_gen.validate_snowflake_bronze_sql(
            'CREATE SCHEMA IF NOT EXISTS "A"."B";\n'
            'CREATE TABLE IF NOT EXISTS "A"."B"."bronze_claims" ("run_id" VARCHAR, "ingestion_timestamp" TIMESTAMP_NTZ, "source_system" VARCHAR, "source_table" VARCHAR);\n'
            'INSERT INTO "A"."B"."bronze_claims" SELECT spark.read.format("jdbc"), CURRENT_TIMESTAMP(), \'x\', \'y\';'
        )
    except ValueError as exc:
        assert "databricks/python token" in str(exc).lower()
    else:
        raise AssertionError("Databricks-style Snowflake SQL should be rejected")


def test_snowflake_validator_allows_only_run_scoped_cleanup():
    target = '"A"."B"."bronze_claims"'
    sql = (
        'CREATE SCHEMA IF NOT EXISTS "A"."B";\n'
        f'CREATE TABLE IF NOT EXISTS {target} ("run_id" VARCHAR, "ingestion_timestamp" TIMESTAMP_NTZ, "source_system" VARCHAR, "source_table" VARCHAR);\n'
        f"DELETE FROM {target} WHERE \"run_id\" = 'run-1';\n"
        f"INSERT INTO {target} SELECT 'run-1', CURRENT_TIMESTAMP(), 'insurance', 'claims';"
    )

    bronze_gen.validate_snowflake_bronze_sql(sql, target_table=target)

    try:
        bronze_gen.validate_snowflake_bronze_sql(
            sql.replace('WHERE "run_id" = \'run-1\'', 'WHERE "source_system" = \'insurance\''),
            target_table=target,
        )
    except ValueError as exc:
        assert "delete" in str(exc).lower()
    else:
        raise AssertionError("Non-run-scoped cleanup should be rejected")


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
