from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from nodes import gold_gen
from nodes import silver_gen
from services import databricks_runtime
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
    assert '"FCT_CLAIM_INFORMATION"' not in dim_sql
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
            "silver_tables": [
                {"table": "claim_information", "target_table": "silver.silver_claim_information"},
                {"table": "policy_transactions", "target_table": "silver.silver_policy_transactions"},
                {"table": "measures", "target_table": "silver.silver_measures"},
                {"table": "claim_payment_expenses", "target_table": "silver.silver_claim_payment_expenses"},
            ],
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
    # Uncertified joins are removed at contract normalization before the source cap guard.
    assert script["source_table_guard"]["dropped_join_paths"] == 0
    assert "'right_table': 'policy_transactions'" in body
    assert "'right_table': 'measures'" in body
    assert "'right_source_table': 'silver.silver_policy_transactions'" in body
    assert "'right_source_table': 'silver.silver_measures'" in body
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


def test_snowflake_source_table_mart_dedupes_dimensions_without_fct_copies():
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
    assert "table-level dimension so DIM remains smaller than its Silver source" in sql
    assert '>= (SELECT COUNT(*) FROM "ATHENA_DB"."SILVER"."silver_policy_transactions")' in sql
    assert '"FCT_POLICY_TRANSACTIONS"' not in sql


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

    mappings = [{
        "source_silver_table": "ATHENA_DB.SILVER.silver_policy_transactions",
        "measure": {"table": "policy_transactions", "column": "premium", "aggregation": "SUM"},
        "readiness": "READY",
        "grouping_dimensions": [
            {
                "table": "policy_transactions",
                "column": "product_name",
                "semantic_type": "DIMENSION",
                "source_silver_table": "ATHENA_DB.SILVER.silver_policy_transactions",
            },
            {
                "table": "policy_cover_level_transactions_dup_del",
                "column": "coverage_name",
                "semantic_type": "DIMENSION",
                "source_silver_table": "ATHENA_DB.SILVER.silver_policy_cover_level_transactions_dup_del",
            },
        ],
    }]

    specs = gold_gen._source_table_grain_specs(contract, mappings, {})

    assert [item["logical_table"] for item in specs] == ["policy_transactions"]


def test_gold_contract_caps_dimensions_and_drops_unavailable_silver_joins(monkeypatch):
    monkeypatch.setenv("ATHENA_GOLD_MAX_DIMENSION_TABLES", "2")
    mapping = {
        "kpi_name": "Total Claims",
        "source_silver_table": "silver.silver_claims",
        "measure": {"table": "claims", "column": "ClaimAmount", "aggregation": "SUM"},
        "grouping_dimensions": [
            {"table": "claims", "column": "ClaimStatus", "semantic_type": "DIMENSION"},
            {"table": "policies", "column": "PolicyState", "semantic_type": "DIMENSION"},
            {"table": "agents", "column": "AgentName", "semantic_type": "DIMENSION"},
            {"table": "missing", "column": "Unknown", "semantic_type": "DIMENSION"},
        ],
        "join_paths": [
            {"left_table": "claims", "left_column": "PolicyID", "right_table": "policies", "right_column": "PolicyID", "certified": True},
            {"left_table": "claims", "left_column": "AgentID", "right_table": "agents", "right_column": "AgentID", "certified": True},
            {"left_table": "claims", "left_column": "MissingID", "right_table": "missing", "right_column": "MissingID", "certified": True},
        ],
        "time": {},
        "readiness": "READY",
    }

    constrained, warnings = silver_gen._constrain_gold_mapping(
        mapping,
        {
            "claims": "silver.silver_claims",
            "policies": "silver.silver_policies",
            "agents": "silver.silver_agents",
        },
    )

    assert constrained["measure"]["column"] == "claimamount"
    assert len(constrained["selected_dimension_tables"]) == 2
    assert len({item["table"] for item in constrained["grouping_dimensions"]}) <= 2
    assert all("missing" not in (join["left_table"], join["right_table"]) for join in constrained["join_paths"])
    assert all(join["left_source_table"].startswith("silver.silver_") for join in constrained["join_paths"])
    assert any("no Silver target exists" in warning for warning in warnings)


def test_databricks_gold_baseline_has_quality_guards_and_passes_hard_validation():
    mapping = {
        "kpi_name": "Average Claim Payment Amount",
        "source_silver_table": "silver.silver_claim_payment_indemnity",
        "measure": {
            "table": "claim_payment_indemnity",
            "column": "paidamount",
            "aggregation": "AVG",
        },
        "grouping_dimensions": [
            {
                "table": "claim_payment_indemnity",
                "column": "hospitalname",
                "semantic_type": "DIMENSION",
            }
        ],
        "time": {
            "grain": "month",
            "column": {"table": "claim_payment_indemnity", "column": "paiddate"},
        },
        "filters": [],
        "join_paths": [],
    }
    dimensions = gold_gen._dimension_specs(mapping)
    code = gold_gen.generate_gold_script(mapping=mapping, run_id="run-dq", gold_schema="gold")

    gold_gen._validate_databricks_gold_candidate(code, mapping, "gold", dimensions)

    assert "DQ_MAX_NULL_RATIO" in code
    assert "duplicate_key_exists" in code
    assert "NumericType" in code
    assert "source_age_days" in code
    assert "DQ_MAX_JOIN_MULTIPLIER" in code
    assert ".whenMatchedUpdateAll()" in code


def test_databricks_gold_hard_validation_rejects_hallucinated_dimension_and_append():
    mapping = {
        "kpi_name": "Total Claims",
        "source_silver_table": "silver.silver_claims",
        "measure": {"table": "claims", "column": "claimamount", "aggregation": "SUM"},
        "grouping_dimensions": [],
        "time": {},
        "filters": [],
        "join_paths": [],
    }
    candidate = '''
from pyspark.sql import functions as F
source = spark.table("silver.silver_claims")
invented = spark.table("gold.dim_agent")
result = source.agg(F.sum("claimamount").alias("total_claims_value"))
result.write.format("delta").mode("append").saveAsTable("gold.fact_total_claims")
'''

    with pytest.raises(ValueError, match="non-contract tables"):
        gold_gen._validate_databricks_gold_candidate(candidate, mapping, "gold", [])


def test_databricks_gold_execution_runs_dimensions_first_and_skips_blocked():
    scripts = databricks_runtime._scripts_for_layer(
        {
            "run_id": "run-order",
            "gold_generation_results": [
                {
                    "status": "APPROVED",
                    "script_path": "fact.py",
                    "script_body": "print('fact')",
                    "dimension_script_body": "print('dimensions')",
                    "target_table": "gold.fact_claims",
                },
                {
                    "status": "BLOCKED",
                    "script_path": None,
                    "target_table": "gold.fact_uncertified",
                },
            ],
        },
        "gold",
        None,
        False,
    )

    assert [script["target_table"] for script in scripts] == ["gold_dimensions", "gold.fact_claims"]
    assert scripts[0]["script_body"] == "print('dimensions')"


def test_gold_measure_scoring_rejects_operational_counters_for_money_kpis():
    columns = [
        {"table_name": "payments", "column_name": "updatenum", "semantic_type": "MEASURE", "is_measure": True},
        {"table_name": "payments", "column_name": "paidamount", "semantic_type": "MEASURE", "is_measure": True},
        {"table_name": "coverage", "column_name": "trans_num", "semantic_type": "MEASURE", "is_measure": True},
        {"table_name": "coverage", "column_name": "cover_sum_insured", "semantic_type": "MEASURE", "is_measure": True},
    ]

    payment = silver_gen._best_measure_for_kpi({"kpi_name": "Average Claim Payment Amount"}, columns)
    insured = silver_gen._best_measure_for_kpi({"kpi_name": "Total Sum Insured"}, columns)

    assert payment["column_name"] == "paidamount"
    assert insured["column_name"] == "cover_sum_insured"
    assert silver_gen._infer_aggregation("Claim Payment Frequency", payment) == "COUNT"
    assert silver_gen._infer_aggregation("Policy Transaction Type Distribution", insured) == "COUNT"


def test_databricks_gold_failure_persists_exact_script_and_stage(monkeypatch):
    monkeypatch.setenv("ATHENA_EXECUTE_DATABRICKS_GOLD", "true")
    monkeypatch.setattr(databricks_runtime, "_upload_support_files", lambda *_: None)
    monkeypatch.setattr(databricks_runtime, "_workspace_import_notebook", lambda *_: {})
    monkeypatch.setattr(databricks_runtime, "_submit_run", lambda *_args, **_kwargs: {"run_id": 42})
    monkeypatch.setattr(
        databricks_runtime,
        "_wait_for_run",
        lambda *_: {"run_id": 42, "result_state": "FAILED", "state_message": "workload failed"},
    )
    monkeypatch.setattr(databricks_runtime, "_run_failure_detail", lambda *_: "missing gold.dim_claims")
    saved = []

    def capture_progress(state, **kwargs):
        saved.append((state, kwargs))
        return state

    monkeypatch.setattr(databricks_runtime, "save_external_execution_progress", capture_progress)

    with pytest.raises(RuntimeError, match="missing gold.dim_claims"):
        databricks_runtime.run_databricks_gold_scripts(
            {
                "run_id": "run-failed-gold",
                "target_warehouse": "databricks",
                "gold_generation_results": [
                    {
                        "status": "APPROVED",
                        "script_body": "print('fact')",
                        "target_table": "gold.fact_claims",
                    }
                ],
            }
        )

    failed_state, failed_progress = saved[-1]
    assert failed_state["failed_background_stage"] == "gold_code_execution"
    assert failed_state["error"].endswith("missing gold.dim_claims")
    assert failed_progress["status"] == "FAILED"
    assert failed_progress["current_name"] == "gold_fact_claims"


def test_databricks_gold_llm_retries_then_uses_deterministic_fallback(monkeypatch):
    output_dir = Path.cwd() / ".tmp-tests" / f"gold_llm_fallback_{uuid.uuid4().hex}"
    output_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("ATHENA_GOLD_USE_LLM", "true")
    monkeypatch.setattr(gold_gen, "_gold_output_dir_for", lambda *_: str(output_dir))
    attempts = []

    def invalid_candidate(**kwargs):
        attempts.append(kwargs.get("validation_feedback"))
        return 'spark.table("gold.dim_invented")'

    monkeypatch.setattr(gold_gen, "llm_generate_gold_code", invalid_candidate)
    result = gold_gen._generate_one_mapping(
        {
            "kpi_name": "Total Claims",
            "source_silver_table": "silver.silver_claims",
            "measure": {"table": "claims", "column": "claimamount", "aggregation": "SUM"},
            "grouping_dimensions": [],
            "time": {},
            "filters": [],
            "join_paths": [],
            "readiness": "READY",
        },
        run_id="run-llm-fallback",
        gold_schema="gold",
        target_warehouse="databricks",
        use_domain_kb=False,
        dimension_contract=[],
        include_dimension=False,
    )

    body = Path(result["script_path"]).read_text(encoding="utf-8")
    assert attempts[0] is None
    assert "approved source or target" in attempts[1]
    assert result["generation_mode"] == "DETERMINISTIC_FALLBACK"
    assert result["fallback_reason"]
    assert "DQ_MAX_NULL_RATIO" in body
