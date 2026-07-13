from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from nodes import gold_gen
from nodes import silver_gen
from services import pipeline_runtime


def test_snowflake_gold_generation_writes_sql_from_contract(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_GOLD_CATALOG", "ATHENA_DB")
    monkeypatch.setenv("SNOWFLAKE_GOLD_SCHEMA", "GOLD")
    monkeypatch.setattr(gold_gen, "ai_store_db_writer", lambda **_: None)
    workdir = Path.cwd() / ".tmp-tests" / f"gold_snowflake_{uuid.uuid4().hex}"
    workdir.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(workdir)

    state = {
        "run_id": "run-snowflake-gold",
        "target_warehouse": "snowflake",
        "gold_generation_contract": {
            "run_id": "run-snowflake-gold",
            "status": "READY",
            "silver_tables": [
                {
                    "table": "claim_information",
                    "target_table": "ATHENA_DB.SILVER.silver_claim_information",
                    "column_count": 10,
                },
                {
                    "table": "policy_transactions",
                    "target_table": "ATHENA_DB.SILVER.silver_policy_transactions",
                    "column_count": 12,
                },
            ],
            "kpi_mappings": [
                {
                    "kpi_name": "Total Claims",
                    "source_silver_table": "ATHENA_DB.SILVER.silver_claim_information",
                    "measure": {
                        "table": "claim_information",
                        "column": "ClaimAmount",
                        "aggregation": "SUM",
                    },
                    "formula": {"status": "PROPOSED"},
                    "grouping_dimensions": [
                        {"table": "claim_information", "column": "ClaimStatus", "semantic_type": "DIMENSION"},
                        {"table": "policy_transactions", "column": "PolicyState", "semantic_type": "DIMENSION"},
                    ],
                    "time": {"grain": "month", "column": {"table": "claim_information", "column": "ClaimOpenDate"}},
                    "filters": [],
                    "join_paths": [],
                    "readiness": "READY",
                }
            ],
        },
    }

    result = gold_gen.gold_code_generation_node(state)
    script = result["gold_generation_results"][0]
    sql = Path(script["script_path"]).read_text(encoding="utf-8")
    loaded = pipeline_runtime.load_gold_scripts("run-snowflake-gold", result)

    assert result["gold_generation_status"] == "COMPLETED"
    assert script["script_language"] == "sql"
    assert script["target_warehouse"] == "snowflake"
    assert script["source_table"] == "ATHENA_DB.SILVER.silver_claim_information"
    assert script["target_table"] == "ATHENA_DB.GOLD.fact_total_claims"
    assert script["dimension_script_path"]
    assert Path(script["script_path"]).parts[-3:] == ("snowflake", "gold", Path(script["script_path"]).name)
    dim_sql = Path(script["dimension_script_path"]).read_text(encoding="utf-8")
    assert "CREATE SCHEMA IF NOT EXISTS \"ATHENA_DB\".\"GOLD\"" in sql
    assert "MERGE INTO \"ATHENA_DB\".\"GOLD\".\"fact_total_claims\" AS target" in sql
    assert "MERGE INTO \"ATHENA_DB\".\"GOLD\".\"DIM_CLAIM_INFORMATION\" AS target" in dim_sql
    assert "MERGE INTO \"ATHENA_DB\".\"GOLD\".\"DIM_POLICY_TRANSACTIONS\" AS target" in dim_sql
    assert "MERGE INTO \"ATHENA_DB\".\"GOLD\".\"FCT_CLAIM_INFORMATION\" AS target" in dim_sql
    assert "\"dim_policy\"" not in dim_sql
    assert "FROM \"ATHENA_DB\".\"SILVER\".\"silver_policy_transactions\"" in dim_sql
    assert 'ALTER TABLE "ATHENA_DB"."GOLD"."fact_total_claims" ADD COLUMN IF NOT EXISTS "ClaimStatus" VARCHAR;' in sql
    assert (
        'ALTER TABLE "ATHENA_DB"."GOLD"."fact_total_claims" ADD COLUMN IF NOT EXISTS '
        '"total_claims_value" FLOAT;'
    ) in sql
    assert "FROM \"ATHENA_DB\".\"SILVER\".\"silver_claim_information\"" in sql
    assert '"claimstatus" AS "ClaimStatus"' in sql
    assert '"policystate" AS "PolicyState"' not in sql
    assert 'TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR("claimopendate"))' in sql
    assert "SUM(TRY_TO_DECIMAL(TO_VARCHAR(\"claimamount\"))) AS \"total_claims_value\"" in sql
    assert 'TRY_TO_TIMESTAMP_NTZ("ClaimOpenDate")' not in sql
    assert 'TRY_TO_TIMESTAMP_NTZ("claimopendate")' not in sql
    assert 'TRY_TO_DECIMAL("ClaimAmount")' not in sql
    assert 'TRY_TO_DECIMAL("claimamount")' not in sql
    assert loaded["scripts"][0]["script_body"] == sql


def test_snowflake_gold_generation_uses_silver_canonical_column_names():
    mapping = {
        "kpi_name": "Reference Count",
        "source_silver_table": "ATHENA_DB.SILVER.silver_policy_transactions",
        "measure": {"table": "policy_transactions", "column": "RERERENCE_ID", "aggregation": "SUM"},
        "grouping_dimensions": [],
        "time": {},
        "filters": [],
        "join_paths": [],
        "readiness": "READY",
    }

    sql = gold_gen.generate_snowflake_gold_script(
        mapping=mapping,
        run_id="run-canonical-columns",
        gold_catalog="ATHENA_DB",
        gold_schema="GOLD",
    )

    assert 'TRY_TO_DECIMAL(TO_VARCHAR("reference_id"))' in sql
    assert 'TRY_TO_DECIMAL(TO_VARCHAR("RERERENCE_ID"))' not in sql


def test_gold_mapping_source_table_guard_caps_ranks_and_drops_bad_joins(monkeypatch):
    monkeypatch.setenv("ATHENA_GOLD_MAX_SOURCE_TABLES", "3")
    mapping = {
        "kpi_name": "Total Claims",
        "source_silver_table": "ATHENA_DB.SILVER.silver_claim_information",
        "measure": {"table": "claim_information", "column": "claim_amount", "aggregation": "SUM"},
        "grouping_dimensions": [
            {"table": "policy_transactions", "column": "policy_state", "semantic_type": "DIMENSION"},
            {"table": "measures", "column": "measure_name", "semantic_type": "DIMENSION"},
            {"table": "claim_payment_expenses", "column": "expense_type", "semantic_type": "DIMENSION"},
        ],
        "time": {"grain": "month", "column": {"table": "claim_information", "column": "claim_open_date"}},
        "join_paths": [
            {
                "left_table": "claim_information",
                "left_column": "policy_id",
                "right_table": "policy_transactions",
                "right_column": "policy_id",
                "certified": True,
                "confidence": 0.95,
            },
            {
                "left_table": "policy_transactions",
                "left_column": "measure_id",
                "right_table": "measures",
                "right_column": "measure_id",
                "certified": True,
                "confidence": 0.9,
            },
            {
                "left_table": "claim_information",
                "left_column": "claim_id",
                "right_table": "claim_payment_expenses",
                "right_column": "claim_id",
                "certified": False,
                "confidence": 0.1,
            },
            {"left_table": "broken", "right_table": "policy_transactions", "right_column": "policy_id"},
        ],
    }

    sanitized, guard = gold_gen._sanitize_gold_mapping(mapping)

    assert guard["max_source_tables"] == 3
    assert guard["kept_source_tables"] == ["claim_information", "policy_transactions", "measures"]
    assert guard["dropped_source_tables"] == ["claim_payment_expenses"]
    assert guard["dropped_malformed_join_paths"] == 1
    assert guard["dropped_join_paths"] == 1
    assert [path["right_table"] for path in sanitized["join_paths"]] == ["policy_transactions", "measures"]
    assert all("claim_payment_expenses" not in path.values() for path in sanitized["join_paths"])


def test_databricks_gold_script_uses_sanitized_join_paths(monkeypatch):
    monkeypatch.setenv("ATHENA_GOLD_MAX_SOURCE_TABLES", "3")
    monkeypatch.setattr(gold_gen, "ai_store_db_writer", lambda **_: None)
    workdir = Path.cwd() / ".tmp-tests" / f"gold_guard_{uuid.uuid4().hex}"
    workdir.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(workdir)

    state = {
        "run_id": "run-gold-guard",
        "target_warehouse": "databricks",
        "gold_generation_contract": {
            "run_id": "run-gold-guard",
            "status": "READY",
            "kpi_mappings": [
                {
                    "kpi_name": "Total Claims",
                    "source_silver_table": "silver.silver_claim_information",
                    "measure": {"table": "claim_information", "column": "claim_amount", "aggregation": "SUM"},
                    "formula": {"status": "PROPOSED"},
                    "grouping_dimensions": [
                        {"table": "policy_transactions", "column": "policy_state", "semantic_type": "DIMENSION"},
                        {"table": "measures", "column": "measure_name", "semantic_type": "DIMENSION"},
                        {"table": "claim_payment_expenses", "column": "expense_type", "semantic_type": "DIMENSION"},
                    ],
                    "time": {"grain": "month", "column": {"table": "claim_information", "column": "claim_open_date"}},
                    "join_paths": [
                        {
                            "left_table": "claim_information",
                            "left_column": "policy_id",
                            "right_table": "policy_transactions",
                            "right_column": "policy_id",
                            "certified": True,
                            "confidence": 0.95,
                        },
                        {
                            "left_table": "claim_information",
                            "left_column": "claim_id",
                            "right_table": "claim_payment_expenses",
                            "right_column": "claim_id",
                            "certified": False,
                            "confidence": 0.1,
                        },
                        {
                            "left_table": "policy_transactions",
                            "left_column": "measure_id",
                            "right_table": "measures",
                            "right_column": "measure_id",
                            "certified": True,
                            "confidence": 0.9,
                        },
                    ],
                    "filters": [],
                    "readiness": "READY",
                }
            ],
        },
    }

    result = gold_gen.gold_code_generation_node(state)
    script = result["gold_generation_results"][0]
    body = Path(script["script_path"]).read_text(encoding="utf-8")

    assert result["gold_generation_status"] == "COMPLETED"
    assert script["source_table_guard"]["kept_source_tables"] == [
        "claim_information",
        "policy_transactions",
        "measures",
    ]
    assert script["source_table_guard"]["dropped_source_tables"] == ["claim_payment_expenses"]
    assert script["source_table_guard"]["dropped_join_paths"] == 1
    assert "'right_table': 'policy_transactions'" in body
    assert "'right_table': 'measures'" in body
    assert "'right_table': 'claim_payment_expenses'" not in body


def test_gold_contract_includes_dimensions_from_certified_join_tables():
    results = [
        {
            "table": "claim_information",
            "source_table": "bronze.claim_information",
            "target_table": "silver.silver_claim_information",
            "column_count": 3,
        },
        {
            "table": "policy_transactions",
            "source_table": "bronze.policy_transactions",
            "target_table": "silver.silver_policy_transactions",
            "column_count": 2,
        },
    ]
    enriched_metadata = {
        "columns": [
            {"table_name": "claim_information", "column_name": "claim_amount", "semantic_type": "MEASURE", "is_measure": True},
            {"table_name": "claim_information", "column_name": "claim_status", "semantic_type": "DIMENSION"},
            {"table_name": "policy_transactions", "column_name": "policy_state", "semantic_type": "DIMENSION"},
            {"table_name": "claim_information", "column_name": "claim_open_date", "semantic_type": "DATE"},
        ],
        "certified_joins": [
            {
                "left_table": "claim_information",
                "left_column": "policy_id",
                "right_table": "policy_transactions",
                "right_column": "policy_id",
                "certified": True,
            }
        ],
    }
    state = {
        "run_id": "run-kimball-contract",
        "certified_kpis": [{"kpi_name": "Total Claims"}],
        "req_constraints": [],
    }

    contract = silver_gen._build_gold_generation_contract(
        state=state,
        results=results,
        enriched_metadata=enriched_metadata,
        generated_at="2026-07-11T00:00:00",
    )

    dimensions = contract["kpi_mappings"][0]["grouping_dimensions"]
    dimension_mappings = contract["dimension_mappings"]
    assert any(item["table"] == "claim_information" and item["column"] == "claim_status" for item in dimensions)
    assert any(item["table"] == "policy_transactions" and item["column"] == "policy_state" for item in dimensions)
    assert any(
        item["logical_table"] == "policy_transactions"
        and item["source_silver_table"] == "silver.silver_policy_transactions"
        and item["columns"] == ["policy_state"]
        for item in dimension_mappings
    )


def test_dimension_script_reads_joined_dimension_table():
    mapping = {
        "kpi_name": "Total Claims",
        "source_silver_table": "silver.silver_claim_information",
        "grouping_dimensions": [
            {"table": "claim_information", "column": "claim_status", "semantic_type": "DIMENSION"},
            {"table": "policy_transactions", "column": "policy_state", "semantic_type": "DIMENSION"},
        ],
    }

    script = gold_gen.generate_dimension_script(mapping, "gold")

    assert 'return f"{SILVER_SCHEMA}.silver_{logical_table}"' in script
    assert 'src = spark.table(dim_source_table)' in script


def test_kimball_plan_validation_accepts_certified_model_and_rejects_unknown_join():
    columns = [
        {"table_name": "claims", "column_name": "claim_amount", "semantic_type": "MEASURE", "is_measure": True},
        {"table_name": "claims", "column_name": "claim_status", "semantic_type": "DIMENSION"},
        {"table_name": "claims", "column_name": "claim_date", "semantic_type": "DATE"},
    ]
    joins = [{
        "left_table": "claims", "left_column": "policy_id",
        "right_table": "policies", "right_column": "policy_id",
        "certified": True,
    }]
    plan = {
        "measure": {"table": "claims", "column": "claim_amount", "aggregation": "SUM"},
        "dimensions": [{"table": "claims", "column": "claim_status", "semantic_type": "DIMENSION"}],
        "time": {"table": "claims", "column": "claim_date", "grain": "month"},
        "join_paths": joins,
    }

    validated = silver_gen._validate_kimball_plan(plan, columns=columns, certified_joins=joins)
    assert validated["measure"]["column"] == "claim_amount"
    assert validated["fact_grain"] == ["claim_status", "period_start"]

    invalid = {**plan, "join_paths": [{"left_table": "claims", "left_column": "x", "right_table": "policies", "right_column": "y"}]}
    with pytest.raises(ValueError, match="non-certified join"):
        silver_gen._validate_kimball_plan(invalid, columns=columns, certified_joins=joins)


def test_kimball_plan_resolves_candidate_ids_reversed_join_and_fact_grain():
    columns = [
        {"table_name": "claims", "column_name": "claim_amount", "semantic_type": "MEASURE", "is_measure": True},
        {"table_name": "claims", "column_name": "claim_status", "semantic_type": "DIMENSION"},
        {"table_name": "claims", "column_name": "claim_date", "semantic_type": "DATE"},
    ]
    joins = [{
        "left_table": "claims", "left_column": "policy_id",
        "right_table": "policies", "right_column": "policy_id",
        "certified": True,
    }]
    plan = {
        "measure_id": "M1",
        "aggregation": "SUM",
        "dimension_ids": ["D1"],
        "time_id": "T1",
        "time_grain": "month",
        "join_paths": [{
            "left_table": "policies", "left_column": "policy_id",
            "right_table": "claims", "right_column": "policy_id",
        }],
        "fact_grain": ["D1", "period_start"],
    }

    validated = silver_gen._validate_kimball_plan(plan, columns=columns, certified_joins=joins)

    assert validated["measure"] == {"table": "claims", "column": "claim_amount", "semantic_type": "MEASURE", "aggregation": "SUM"}
    assert validated["fact_grain"] == ["claim_status", "period_start"]
    assert validated["join_paths"][0]["left_table"] == "claims"
    assert validated["join_paths"][0]["left_column"] == "policy_id"
    assert validated["join_paths"][0]["right_table"] == "policies"
    assert validated["join_paths"][0]["right_column"] == "policy_id"


def test_kimball_plan_rejects_invalid_fact_grain():
    columns = [
        {"table_name": "claims", "column_name": "claim_amount", "semantic_type": "MEASURE"},
        {"table_name": "claims", "column_name": "claim_status", "semantic_type": "DIMENSION"},
    ]
    plan = {
        "measure": {"table": "claims", "column": "claim_amount", "aggregation": "SUM"},
        "dimensions": [{"table": "claims", "column": "claim_status"}],
        "fact_grain": ["wrong_column"],
    }

    with pytest.raises(ValueError, match="invalid fact grain"):
        silver_gen._validate_kimball_plan(plan, columns=columns, certified_joins=[])


def test_dimension_specs_use_source_table_grain_for_one_wide_source_table():
    mapping = {
        "source_silver_table": "silver.silver_policy_transactions",
        "measure": {"table": "policy_transactions"},
        "grouping_dimensions": [
            {"table": "policy_transactions", "column": "PRODUCT_NAME", "semantic_type": "DIMENSION"},
            {"table": "policy_transactions", "column": "AGENT_NAME", "semantic_type": "DIMENSION"},
            {"table": "policy_transactions", "column": "GEOG_STATE_NAME", "semantic_type": "DIMENSION"},
            {"table": "policy_transactions", "column": "CHANNEL_NAME", "semantic_type": "DIMENSION"},
        ],
    }

    specs = gold_gen._dimension_specs(mapping)

    assert {item["entity"] for item in specs} == {"policy_transactions"}
    assert specs[0]["columns"] == ["PRODUCT_NAME", "AGENT_NAME", "GEOG_STATE_NAME", "CHANNEL_NAME"]


def test_snowflake_source_table_mart_dedupes_dimensions_and_keeps_facts_row_grain():
    sql = gold_gen.generate_snowflake_source_table_mart_script(
        specs=[
            {
                "logical_table": "policy_transactions",
                "source_table": "ATHENA_DB.SILVER.silver_policy_transactions",
                "columns": ["product_name", "segment_name"],
                "source_columns": ["product_name", "segment_name"],
            }
        ],
        run_id="run-dim-dedupe",
        gold_catalog="ATHENA_DB",
        gold_schema="GOLD",
    )

    assert 'CREATE TABLE IF NOT EXISTS "ATHENA_DB"."GOLD"."DIM_POLICY_TRANSACTIONS" (' in sql
    assert 'SELECT DISTINCT' in sql
    assert 'TO_VARCHAR(src."product_name") AS "product_name"' in sql
    assert 'DELETE FROM "ATHENA_DB"."GOLD"."DIM_POLICY_TRANSACTIONS" WHERE "gold_run_id" = ' in sql
    assert "table-level dimension so DIM remains smaller than the row-grain FCT" in sql
    assert '>= (SELECT COUNT(*) FROM "ATHENA_DB"."SILVER"."silver_policy_transactions")' in sql
    assert 'CREATE TABLE IF NOT EXISTS "ATHENA_DB"."GOLD"."FCT_POLICY_TRANSACTIONS" LIKE "ATHENA_DB"."SILVER"."silver_policy_transactions";' in sql


def test_source_table_grain_skips_duplicate_deleted_auxiliary_tables():
    contract = {
        "silver_tables": [
            {
                "table": "policy_transactions",
                "target_table": "ATHENA_DB.SILVER.silver_policy_transactions",
            },
            {
                "table": "policy_cover_level_transactions_dup_del",
                "target_table": "ATHENA_DB.SILVER.silver_policy_cover_level_transactions_dup_del",
            },
        ]
    }

    specs = gold_gen._source_table_grain_specs(contract, [], {})

    assert [item["logical_table"] for item in specs] == ["policy_transactions"]
