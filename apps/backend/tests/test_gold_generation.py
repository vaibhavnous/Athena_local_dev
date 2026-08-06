from __future__ import annotations

import json
import time
import uuid
from pathlib import Path

import pytest

from nodes import gold_gen
from nodes import silver_gen
from services import databricks_runtime
from services import dbt_snowflake_runtime
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
    assert script["code_generation_format"] == "native"
    assert script["source_table"] == "ATHENA_DB.SILVER.silver_claim_information"
    assert script["target_table"] == "ATHENA_DB.GOLD.fact_total_claims"
    assert script["dimension_script_path"]
    assert script["script_body"] == sql
    assert Path(script["script_path"]).parts[-3:] == ("snowflake", "gold", Path(script["script_path"]).name)
    dim_sql = Path(script["dimension_script_path"]).read_text(encoding="utf-8")
    assert script["dimension_script_body"] == dim_sql
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
    assert loaded["scripts"][0]["script_body"].strip() == sql.strip()


def test_snowflake_dbt_gold_generation_writes_ref_model_without_native_dimensions(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_GOLD_CATALOG", "ATHENA_DB")
    monkeypatch.setenv("SNOWFLAKE_GOLD_SCHEMA", "GOLD")
    monkeypatch.setattr(gold_gen, "ai_store_db_writer", lambda **_: None)
    workdir = Path.cwd() / ".tmp-tests" / f"gold_snowflake_dbt_{uuid.uuid4().hex}"
    workdir.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(workdir)

    state = {
        "run_id": "run-snowflake-dbt-gold",
        "target_warehouse": "snowflake",
        "execution_engine": "dbt",
        "gold_generation_contract": {
            "run_id": "run-snowflake-dbt-gold",
            "status": "READY",
            "silver_tables": [
                {
                    "table": "claim_information",
                    "target_table": "ATHENA_DB.SILVER.silver_claim_information",
                    "column_count": 10,
                }
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
                        {
                            "table": "claim_information",
                            "column": "ClaimStatus",
                            "semantic_type": "DIMENSION",
                        }
                    ],
                    "time": {
                        "grain": "month",
                        "column": {"table": "claim_information", "column": "ClaimOpenDate"},
                    },
                    "filters": [],
                    "join_paths": [],
                    "readiness": "READY",
                }
            ],
        },
    }
    dbt_snowflake_runtime.write_snowflake_dbt_scaffold(state)
    silver_model_path = dbt_snowflake_runtime.dbt_model_path(
        state["run_id"],
        "silver",
        "silver_claim_information",
    )
    silver_model_path.parent.mkdir(parents=True, exist_ok=True)
    silver_model_path.write_text("select 1\n", encoding="utf-8")

    result = gold_gen.gold_code_generation_node(state)
    script = result["gold_generation_results"][0]
    model_path = Path(script["script_path"])
    sql = model_path.read_text(encoding="utf-8")
    bundle = json.loads(Path(result["gold_generation_bundle_path"]).read_text(encoding="utf-8"))

    assert result["gold_generation_status"] == "COMPLETED"
    assert result["snowflake_dbt_deploy_status"] == "NOT_APPLICABLE_CODEGEN_ONLY"
    assert result["snowflake_dbt_model_count"] == 2
    assert result["snowflake_dbt_validation"]["model_count"] == 2
    assert result["snowflake_dbt_validation"]["ref_count"] == 1
    assert Path(result["snowflake_dbt_artifact_path"]).is_dir()
    assert script["code_generation_format"] == "dbt"
    assert script["generation_mode"] == "SNOWFLAKE_DBT_SQL"
    assert script["dbt_model_name"] == "gold_total_claims"
    assert script["dbt_alias"] == "fact_total_claims"
    assert not script.get("dimension_script_path")
    assert bundle["dimension_script_count"] == 0
    assert model_path.parts[-3:] == ("models", "gold", "gold_total_claims.sql")
    assert "{{ config(" in sql
    assert "FROM {{ ref('silver_claim_information') }}" in sql
    assert "SUM(TRY_TO_DECIMAL(TO_VARCHAR(\"claimamount\"))) AS \"total_claims_value\"" in sql
    assert "MERGE INTO" not in sql
    assert "CREATE TABLE" not in sql
    assert not list(workdir.rglob("gold_dimensions*.sql"))


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


def test_gold_mapping_generation_parallelism_preserves_contract_order(monkeypatch):
    monkeypatch.setenv("ATHENA_GOLD_KPI_PARALLELISM", "3")
    calls = []

    def fake_generate(mapping, **kwargs):
        calls.append(mapping["kpi_name"])
        if mapping["kpi_name"] == "First KPI":
            time.sleep(0.05)
        return {"kpi_name": mapping["kpi_name"], "status": "APPROVED"}

    monkeypatch.setattr(gold_gen, "_generate_one_mapping", fake_generate)
    results = gold_gen._generate_gold_mapping_results(
        [
            {"kpi_name": "First KPI"},
            {"kpi_name": "Second KPI"},
            {"kpi_name": "Third KPI"},
        ],
        run_id="run-parallel",
        gold_schema="gold",
        gold_catalog="",
        target_warehouse="databricks",
        use_domain_kb=False,
        dimension_contract=[],
        dbt_codegen=False,
    )

    assert [item["kpi_name"] for item in results] == ["First KPI", "Second KPI", "Third KPI"]
    assert sorted(calls) == ["First KPI", "Second KPI", "Third KPI"]


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


def test_gold_python_artifact_validation_rejects_empty_rendered_contract():
    mapping = {
        "kpi_name": "Total Claims",
        "source_silver_table": "silver.silver_claim_information",
        "measure": {"table": "claim_information", "column": "claim_amount", "aggregation": "SUM"},
        "grouping_dimensions": [],
        "time": {},
        "filters": [],
        "join_paths": [],
        "readiness": "READY",
    }
    script = gold_gen.generate_gold_script(mapping=mapping, run_id="run-gold-contract", gold_schema="gold")
    malformed = script.replace("SOURCE_TABLE = 'silver.silver_claim_information'", "SOURCE_TABLE = ''")

    with pytest.raises(ValueError, match="empty required constants"):
        gold_gen._validate_databricks_gold_candidate(malformed, mapping, "gold", [])


@pytest.mark.parametrize(
    "corrupted",
    [
        'entity = key_column.removesuffix("_key")_best_effort_sql(',
        'cluster_columns = name for name in ["period_start"] if name in result.columns][:4]',
        "joined_logical_tables =",
        "dimension_context = to_json(struct(_[col(name) for name in dimensions]))",
        'try:\nspark.sql("CREATE SCHEMA IF NOT EXISTS gold")',
        'def _silver_table(logical_table):\n    return f".silver_"',
        're.search(r"(=<>!=>=<=><\\bIN\\b\\bLIKE\\b\\bIS\\b)", text)',
        'print(f"SUCCESS: Gold KPI generation completed for ")',
        'raise ValueError(f"Missing silver source table: ")',
    ],
)
def test_gold_python_artifact_validation_rejects_reported_corruption(corrupted):
    with pytest.raises((SyntaxError, ValueError)):
        gold_gen._validate_gold_python_artifact(corrupted, artifact_name="KPI")


def test_gold_sql_artifact_validation_rejects_incomplete_native_sql():
    with pytest.raises(ValueError, match="empty ALTER TABLE target"):
        gold_gen._validate_gold_sql_artifact("ALTER TABLE SET TAG x = 'y'", artifact_name="KPI")
    with pytest.raises(ValueError, match="unmatched parenthesis"):
        gold_gen._validate_gold_sql_artifact("SELECT COUNT((*) FROM source", artifact_name="KPI")


def test_gold_llm_is_constrained_for_complex_orchestration():
    assert gold_gen._gold_llm_skip_reason({"join_paths": [{"certified": True}]}, [])
    assert gold_gen._gold_llm_skip_reason({"grouping_dimensions": [{"column": "region"}]}, [])
    assert gold_gen._gold_llm_skip_reason({"filters": ["a=1", "b=2", "c=3", "d=4"]}, [])
    assert gold_gen._gold_llm_skip_reason({"filters": ["a=1"]}, []) == ""


def test_complex_gold_mapping_bypasses_llm_without_fallback(monkeypatch):
    output_dir = Path.cwd() / ".tmp-tests" / f"gold_constrained_{uuid.uuid4().hex}"
    output_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("ATHENA_GOLD_USE_LLM", "true")
    monkeypatch.setattr(gold_gen, "_gold_output_dir_for", lambda *_: str(output_dir))
    monkeypatch.setattr(
        gold_gen,
        "llm_generate_gold_code",
        lambda **_: pytest.fail("complex Gold orchestration must not call the LLM"),
    )

    result = gold_gen._generate_one_mapping(
        {
            "kpi_name": "Claims by Status",
            "source_silver_table": "silver.silver_claims",
            "measure": {"table": "claims", "column": "claim_amount", "aggregation": "SUM"},
            "grouping_dimensions": [{"table": "claims", "column": "claim_status"}],
            "time": {},
            "filters": [],
            "join_paths": [],
            "readiness": "READY",
        },
        run_id="run-constrained",
        gold_schema="gold",
        target_warehouse="databricks",
        use_domain_kb=False,
        dimension_contract=[],
        include_dimension=False,
    )

    assert result["generation_mode"] == "DETERMINISTIC_CONSTRAINED"
    assert result["llm_skip_reason"]
    assert result["fallback_reason"] is None


def test_gold_default_parallelism_remains_two(monkeypatch):
    monkeypatch.delenv("ATHENA_GOLD_KPI_PARALLELISM", raising=False)
    assert gold_gen._gold_kpi_parallelism() == 2


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


def test_kimball_plan_rejects_pii_unreachable_dimensions_and_many_to_many_joins():
    columns = [
        {"table_name": "claims", "column_name": "claim_amount", "semantic_type": "MEASURE", "data_type": "decimal"},
        {"table_name": "claims", "column_name": "claim_status", "semantic_type": "DIMENSION"},
        {"table_name": "customers", "column_name": "customer_segment", "semantic_type": "DIMENSION"},
        {"table_name": "customers", "column_name": "customer_name", "semantic_type": "DIMENSION", "is_pii_candidate": True},
    ]
    base = {
        "measure": {"table": "claims", "column": "claim_amount", "aggregation": "SUM"},
        "dimensions": [{"table": "customers", "column": "customer_segment"}],
        "fact_grain": ["customer_segment"],
    }

    candidates = silver_gen._kimball_candidates(columns, [])
    assert all(item["column"] != "customer_name" for item in candidates["dimensions"].values())
    with pytest.raises(ValueError, match="unreachable"):
        silver_gen._validate_kimball_plan(base, columns=columns, certified_joins=[])

    joins = [{
        "left_table": "claims",
        "left_column": "customer_id",
        "right_table": "customers",
        "right_column": "customer_id",
        "cardinality": "many_to_many",
        "certified": True,
    }]
    with pytest.raises(ValueError, match="many-to-many"):
        silver_gen._validate_kimball_plan(
            {**base, "join_paths": joins}, columns=columns, certified_joins=joins
        )

    pii_plan = {
        **base,
        "dimensions": [{"table": "customers", "column": "customer_name"}],
        "fact_grain": ["customer_name"],
        "join_paths": [{**joins[0], "cardinality": "many_to_one"}],
    }
    with pytest.raises(ValueError, match="PII dimension"):
        silver_gen._validate_kimball_plan(
            pii_plan,
            columns=columns,
            certified_joins=[{**joins[0], "cardinality": "many_to_one"}],
        )


def test_kimball_plan_rejects_non_numeric_sum_measure():
    columns = [{
        "table_name": "claims",
        "column_name": "claim_reference",
        "semantic_type": "MEASURE",
        "data_type": "varchar",
    }]
    plan = {
        "measure": {"table": "claims", "column": "claim_reference", "aggregation": "SUM"},
        "dimensions": [],
        "time": {},
        "fact_grain": ["claim_reference"],
    }

    with pytest.raises(ValueError, match="non-numeric"):
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


def test_databricks_gold_baseline_omits_runtime_dq_guards_and_passes_hard_validation():
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

    assert "DQ_MAX_" not in code
    assert "duplicate_key_exists" not in code
    assert "NumericType" in code
    assert "source_age_days" not in code
    assert "dimension key {key_column} unresolved" not in code
    assert ".whenMatchedUpdateAll()" in code


def test_databricks_gold_warns_through_unresolved_dimension_surrogate_keys():
    mapping = {
        "kpi_name": "Total Paid",
        "source_silver_table": "silver_schema.silver_claim_payment_indemnity",
        "measure": {
            "table": "claim_payment_indemnity",
            "column": "paidamount",
            "aggregation": "SUM",
        },
        "grouping_dimensions": [
            {
                "table": "claim_payment_indemnity",
                "column": "claimid",
                "semantic_type": "DIMENSION",
            }
        ],
        "time": {},
        "filters": [],
        "join_paths": [],
    }
    dimensions = gold_gen._dimension_specs(mapping)

    code = gold_gen.generate_gold_script(mapping=mapping, run_id="run-dim-key-dq", gold_schema="gold")

    gold_gen._validate_databricks_gold_candidate(code, mapping, "gold", dimensions)
    assert "DQ_MAX_DIMENSION_KEY_NULL_RATIO" not in code
    assert "dimension_key_columns.append(key_column)" in code
    assert "Gold dimension key {key_column} unresolved" not in code
    assert "check dimension natural keys and certified join paths" not in code


def test_databricks_gold_adds_governance_and_delta_quality_features():
    mapping = {
        "kpi_name": "Total Paid",
        "kpi_description": "Total certified claim payments",
        "source_silver_table": "silver.silver_claim_payment",
        "measure": {"table": "claim_payment", "column": "paid_amount", "aggregation": "SUM"},
        "grouping_dimensions": [
            {
                "table": "claim_payment",
                "column": "payment_status",
                "semantic_type": "DIMENSION",
                "source_silver_table": "silver.silver_claim_payment",
            }
        ],
        "time": {"grain": "month", "column": {"column": "paid_date"}},
    }

    code = gold_gen.generate_gold_script(mapping=mapping, run_id="run-governed", gold_schema="gold")

    compile(code, "<generated-gold>", "exec")
    assert "delta.enableChangeDataFeed" in code
    assert "SET TAGS ('layer' = 'gold', 'table_type' = 'fact')" in code
    assert "CHECK (gold_upsert_key IS NOT NULL)" in code
    assert "PRIMARY KEY (`gold_upsert_key`) NOT ENFORCED" in code
    assert 'f"FOREIGN KEY (`{key_column}`) REFERENCES gold.dim_{entity}' in code
    assert "CLUSTER BY" in code
    assert "Total certified claim payments" in code


def test_shared_dimensions_add_metadata_attributes_without_changing_natural_key():
    mappings = [{
        "kpi_name": "Total Paid",
        "source_silver_table": "silver.silver_claim_payment",
        "measure": {"table": "claim_payment", "column": "paid_amount", "aggregation": "SUM"},
        "grouping_dimensions": [{
            "table": "claim_payment",
            "column": "payment_status",
            "semantic_type": "DIMENSION",
            "source_silver_table": "silver.silver_claim_payment",
        }],
        "readiness": "READY",
    }]
    metadata = {"columns": [
        {"table_name": "claim_payment", "column_name": "payment_status", "semantic_type": "DIMENSION"},
        {"table_name": "claim_payment", "column_name": "payment_method", "semantic_type": "DIMENSION"},
        {"table_name": "claim_payment", "column_name": "customer_name", "semantic_type": "DIMENSION", "is_pii_candidate": True},
    ]}

    shared = gold_gen._shared_dimension_mapping(mappings, metadata)
    specs = gold_gen._dimension_specs(shared)
    code = gold_gen.generate_dimension_script(shared, "gold")

    assert specs[0]["columns"] == ["payment_status"]
    assert specs[0]["attribute_columns"] == ["payment_method"]
    assert "groupBy(*[col(name) for name in natural_columns]).agg" in code
    assert 'condition="target.attribute_hash <> source.attribute_hash"' in code
    assert "delta.enableChangeDataFeed" in code
    assert "customer_name" not in code


def test_databricks_generates_consolidated_kpi_fact():
    mappings = [
        {
            "kpi_name": "Total Paid",
            "source_silver_table": "silver.silver_claim_payment",
            "measure": {"column": "paid_amount", "aggregation": "SUM"},
            "readiness": "READY",
        },
        {
            "kpi_name": "Payment Count",
            "source_silver_table": "silver.silver_claim_payment",
            "measure": {"aggregation": "COUNT"},
            "readiness": "READY",
        },
    ]

    code = gold_gen.generate_consolidated_gold_script(
        mappings=mappings, run_id="run-consolidated", gold_schema="gold"
    )

    compile(code, "<generated-consolidated-gold>", "exec")
    assert "gold.fact_kpi_metrics" in code
    assert "gold.fact_total_paid" in code
    assert "gold.fact_payment_count" in code
    assert "dimension_key_context" in code
    assert "unionByName" in code
    assert "delta.enableChangeDataFeed" in code
    assert "CLUSTER BY (`period_start`, `kpi_name`)" in code


def test_databricks_gold_resolves_measure_column_case_insensitively():
    mapping = {
        "kpi_name": "Average Reserve Estimation Per Claim",
        "source_silver_table": "silver_schema.silver_indemnity_outstanding_estimates",
        "measure": {
            "table": "indemnity_outstanding_estimates",
            "column": "grossestimate",
            "aggregation": "AVG",
        },
        "grouping_dimensions": [
            {
                "table": "indemnity_outstanding_estimates",
                "column": "claimid",
                "semantic_type": "DIMENSION",
            }
        ],
        "time": {
            "grain": "month",
            "column": {
                "table": "indemnity_outstanding_estimates",
                "column": "inserteddate",
            },
        },
        "filters": [],
        "join_paths": [
            {
                "left_table": "indemnity_outstanding_estimates",
                "left_column": "claimid",
                "right_table": "claim_information",
                "right_column": "claimid",
                "certified": True,
            }
        ],
    }

    code = gold_gen.generate_gold_script(mapping=mapping, run_id="run-column-case", gold_schema="gold_schema")

    assert "columns_by_name = {name.casefold(): name for name in frame.columns}" in code
    assert "resolved_measure_column = _resolve_column(df, MEASURE_COLUMN)" in code
    assert "MEASURE_COLUMN = resolved_measure_column" in code
    assert "profile_dimensions =" not in code
    assert "resolved_base_column = _resolve_column(df, base_column)" in code
    assert "resolved_other_column = _resolve_column(other_df, other_column)" in code
    assert "TIME_COLUMN = _resolve_column(df, requested_time_column)" in code
    assert "date_trunc('month', col(TIME_COLUMN))" in code
    assert "avg(col(MEASURE_COLUMN))" in code
    assert "if MEASURE_COLUMN not in df.columns:" not in code


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


def test_databricks_gold_hard_validation_rejects_contract_drift_and_unsafe_imports():
    mapping = {
        "kpi_name": "Total Claims",
        "source_silver_table": "silver.silver_claims",
        "measure": {"table": "claims", "column": "claim_amount", "aggregation": "SUM"},
        "grouping_dimensions": [
            {"table": "claims", "column": "claim_status", "semantic_type": "DIMENSION"}
        ],
        "time": {"grain": "month", "column": {"table": "claims", "column": "claim_date"}},
        "filters": ["claim_status IS NOT NULL"],
        "join_paths": [],
    }
    dimensions = gold_gen._dimension_specs(mapping)
    baseline = gold_gen.generate_gold_script(mapping=mapping, run_id="run-hard", gold_schema="gold")

    drifted = baseline.replace("DIMENSION_COLUMNS = ['claim_status']", "DIMENSION_COLUMNS = []")
    with pytest.raises(ValueError, match="changed contract constants"):
        gold_gen._validate_databricks_gold_candidate(drifted, mapping, "gold", dimensions)

    unsafe = baseline.replace("import re", "import re\nimport os", 1)
    with pytest.raises(ValueError, match="non-approved modules"):
        gold_gen._validate_databricks_gold_candidate(unsafe, mapping, "gold", dimensions)

    changed_merge = baseline.replace(
        '"target.gold_upsert_key = source.gold_upsert_key"',
        '"target.kpi_name = source.kpi_name"',
    )
    with pytest.raises(ValueError, match="merge key"):
        gold_gen._validate_databricks_gold_candidate(changed_merge, mapping, "gold", dimensions)


def test_snowflake_gold_hard_validation_requires_grain_and_upsert_key():
    mapping = {
        "kpi_name": "Total Claims",
        "source_silver_table": "ATHENA_DB.SILVER.silver_claims",
        "measure": {"table": "claims", "column": "claim_amount", "aggregation": "SUM"},
        "grouping_dimensions": [
            {"table": "claims", "column": "claim_status", "semantic_type": "DIMENSION"}
        ],
        "time": {"grain": "month", "column": {"table": "claims", "column": "claim_date"}},
        "filters": [],
        "join_paths": [],
    }
    target = "ATHENA_DB.GOLD.fact_total_claims"
    baseline = gold_gen.generate_snowflake_gold_script(
        mapping=mapping,
        run_id="run-snowflake-hard",
        gold_catalog="ATHENA_DB",
        gold_schema="GOLD",
    )

    gold_gen._validate_snowflake_gold_candidate(baseline, mapping, target)
    changed_merge = baseline.replace(
        'target."gold_upsert_key" = source."gold_upsert_key"',
        'target."kpi_name" = source."kpi_name"',
    )
    with pytest.raises(ValueError, match="merge key"):
        gold_gen._validate_snowflake_gold_candidate(changed_merge, mapping, target)


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


def test_databricks_gold_review_filters_and_uses_reviewed_script_body():
    scripts = databricks_runtime._scripts_for_layer(
        {
            "run_id": "run-reviewed-gold",
            "target_warehouse": "databricks",
            "gold_generation_results": [
                {
                    "status": "APPROVED",
                    "script_path": "fact_one.py",
                    "script_body": "print('original one')",
                    "target_table": "gold.fact_one",
                },
                {
                    "status": "APPROVED",
                    "script_path": "fact_two.py",
                    "script_body": "print('original two')",
                    "target_table": "gold.fact_two",
                },
            ],
        },
        "gold",
        {
            "items": [
                {
                    "review_status": "PENDING",
                    "target_table": "gold.fact_one",
                    "script_body": "print('reviewed one')",
                },
                {
                    "review_status": "REJECTED",
                    "target_table": "gold.fact_two",
                    "script_body": "print('rejected two')",
                },
            ]
        },
        True,
    )

    assert [script["target_table"] for script in scripts] == ["gold.fact_one"]
    assert scripts[0]["script_body"] == "print('reviewed one')"


def test_databricks_gold_submits_approved_scripts_as_one_batch(monkeypatch):
    monkeypatch.setenv("ATHENA_EXECUTE_DATABRICKS_GOLD", "true")
    monkeypatch.delenv("ATHENA_DATABRICKS_GOLD_EXECUTION_MODE", raising=False)
    monkeypatch.delenv("ATHENA_DATABRICKS_EXECUTION_MODE", raising=False)
    monkeypatch.setattr(databricks_runtime, "_upload_support_files", lambda *_: None)
    monkeypatch.setattr(databricks_runtime, "_workspace_import_notebook", lambda *_: {})
    submitted = []
    monkeypatch.setattr(
        databricks_runtime,
        "_submit_run",
        lambda path, **kwargs: submitted.append((path, kwargs)) or {"run_id": 42},
    )
    monkeypatch.setattr(
        databricks_runtime,
        "_wait_for_run",
        lambda *_: {"run_id": 42, "result_state": "SUCCESS"},
    )
    monkeypatch.setattr(databricks_runtime, "_task_run_id", lambda *_: 42)
    monkeypatch.setattr(
        databricks_runtime,
        "_get_run_output",
        lambda *_: {
            "notebook_output": {
                "result": json.dumps({
                    "status": "SUCCESS",
                    "results": [
                        {"script_name": "gold_fact_one", "status": "SUCCESS"},
                        {"script_name": "gold_fact_two", "status": "SUCCESS"},
                    ],
                })
            }
        },
    )
    monkeypatch.setattr(databricks_runtime, "save_external_execution_progress", lambda state, **_: state)

    result = databricks_runtime.run_databricks_gold_scripts({
        "run_id": "run-batch-gold",
        "target_warehouse": "databricks",
        "gold_generation_results": [
            {"status": "APPROVED", "script_body": "print('one')", "target_table": "gold.fact_one"},
            {"status": "APPROVED", "script_body": "print('two')", "target_table": "gold.fact_two"},
            {"status": "BLOCKED", "target_table": "gold.fact_blocked"},
        ],
    })

    assert len(submitted) == 1
    assert submitted[0][1]["run_name"] == "Athena gold batch run-batch-gold"
    assert len(result["databricks_gold_execution_results"]) == 2


def test_databricks_gold_batch_refuses_unverifiable_output(monkeypatch):
    monkeypatch.setenv("ATHENA_EXECUTE_DATABRICKS_GOLD", "true")
    monkeypatch.delenv("ATHENA_DATABRICKS_GOLD_EXECUTION_MODE", raising=False)
    monkeypatch.delenv("ATHENA_DATABRICKS_EXECUTION_MODE", raising=False)
    monkeypatch.setattr(databricks_runtime, "_upload_support_files", lambda *_: None)
    monkeypatch.setattr(databricks_runtime, "_workspace_import_notebook", lambda *_: {})
    monkeypatch.setattr(databricks_runtime, "_submit_run", lambda *_args, **_kwargs: {"run_id": 42})
    monkeypatch.setattr(
        databricks_runtime,
        "_wait_for_run",
        lambda *_: {
            "run_id": 42,
            "result_state": "SUCCESS",
            "life_cycle_state": "TERMINATED",
            "run_page_url": "https://example.databricks/run/42",
        },
    )
    monkeypatch.setattr(databricks_runtime, "_task_run_id", lambda *_: 42)
    monkeypatch.setattr(databricks_runtime, "_get_run_output", lambda *_: (_ for _ in ()).throw(RuntimeError("output unavailable")))
    monkeypatch.setattr(databricks_runtime, "save_external_execution_progress", lambda state, **_: state)

    result = databricks_runtime.run_databricks_gold_scripts(
        {
            "run_id": "run-batch-gold-output-warning",
            "target_warehouse": "databricks",
            "gold_generation_results": [
                {"status": "APPROVED", "script_body": "print('one')", "target_table": "gold.fact_one"},
                {"status": "APPROVED", "script_body": "print('two')", "target_table": "gold.fact_two"},
            ],
        }
    )

    assert result["databricks_gold_execution_status"] == "COMPLETED"
    assert [item["status"] for item in result["databricks_gold_execution_results"]] == ["SUCCESS", "SUCCESS"]
    assert [item["verification_status"] for item in result["databricks_gold_execution_results"]] == ["UNVERIFIED", "UNVERIFIED"]
    assert "output unavailable" in result["databricks_gold_execution_results"][0]["warning"]


def test_databricks_batch_notebook_fails_the_job_when_any_script_fails():
    notebook = databricks_runtime._build_batch_driver_notebook(
        "gold",
        [{"status": "APPROVED", "script_body": "raise RuntimeError('boom')", "target_table": "gold.fact_one"}],
        workspace_dir="/Workspace/athena/run",
    )

    assert 'if _SUMMARY["status"] == "FAILED":' in notebook
    assert 'exec(compile(_item.get("script_text") or "", f"<athena:{_name}>", "exec"), _script_globals)' in notebook
    assert '_SCRIPTS_OK = builtins.sum(' in notebook
    assert "_SCRIPTS_OK >= _SCRIPTS_FAILED" in notebook
    assert "spark.catalog.tableExists(_target)" in notebook
    assert notebook.index('raise RuntimeError(json.dumps(_SUMMARY') < notebook.index("dbutils.notebook.exit")


def test_databricks_gold_batch_fifty_percent_success_completes_with_warnings(monkeypatch):
    monkeypatch.setenv("ATHENA_EXECUTE_DATABRICKS_GOLD", "true")
    monkeypatch.delenv("ATHENA_DATABRICKS_GOLD_ALLOW_PARTIAL_SUCCESS", raising=False)
    monkeypatch.setattr(databricks_runtime, "_upload_support_files", lambda *_: None)
    monkeypatch.setattr(databricks_runtime, "_workspace_import_notebook", lambda *_: {})
    monkeypatch.setattr(databricks_runtime, "_submit_run", lambda *_args, **_kwargs: {"run_id": 42})
    monkeypatch.setattr(
        databricks_runtime,
        "_wait_for_run",
        lambda *_: {"run_id": 42, "result_state": "SUCCESS", "life_cycle_state": "TERMINATED"},
    )
    monkeypatch.setattr(databricks_runtime, "_task_run_id", lambda *_: 42)
    monkeypatch.setattr(
        databricks_runtime,
        "_get_run_output",
        lambda *_: {
            "notebook_output": {
                "result": json.dumps(
                    {
                        "status": "COMPLETED_WITH_WARNINGS",
                        "results": [
                            {"script_name": "gold_fact_one", "status": "SUCCESS"},
                            {"script_name": "gold_fact_two", "status": "FAILED", "error": "bad generated SQL"},
                        ],
                    }
                )
            }
        },
    )
    saved = []

    def capture_progress(state, **kwargs):
        saved.append((state, kwargs))
        return state

    monkeypatch.setattr(databricks_runtime, "save_external_execution_progress", capture_progress)

    result = databricks_runtime.run_databricks_gold_scripts(
        {
            "run_id": "run-partial-gold",
            "target_warehouse": "databricks",
            "gold_generation_results": [
                {"status": "APPROVED", "script_body": "print('one')", "target_table": "gold.fact_one"},
                {"status": "APPROVED", "script_body": "print('two')", "target_table": "gold.fact_two"},
            ],
        }
    )

    assert result["databricks_gold_execution_status"] == "COMPLETED_WITH_WARNINGS"
    assert [item["script_name"] for item in result["databricks_gold_execution_failures"]] == ["gold_fact_two"]
    assert saved[-1][1]["status"] == "COMPLETED_WITH_WARNINGS"
    assert saved[-1][1]["completed_count"] == 1
    assert "gold_fact_two" in saved[-1][1]["message"]


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
    assert failed_progress["total_count"] == 1


def test_databricks_gold_batch_failure_persists_partial_results(monkeypatch):
    monkeypatch.setenv("ATHENA_EXECUTE_DATABRICKS_GOLD", "true")
    monkeypatch.delenv("ATHENA_DATABRICKS_GOLD_EXECUTION_MODE", raising=False)
    monkeypatch.delenv("ATHENA_DATABRICKS_EXECUTION_MODE", raising=False)
    monkeypatch.setattr(databricks_runtime, "_upload_support_files", lambda *_: None)
    monkeypatch.setattr(databricks_runtime, "_workspace_import_notebook", lambda *_: {})
    monkeypatch.setattr(databricks_runtime, "_submit_run", lambda *_args, **_kwargs: {"run_id": 42})
    monkeypatch.setattr(
        databricks_runtime,
        "_wait_for_run",
        lambda *_: {"run_id": 42, "result_state": "FAILED", "state_message": "workload failed"},
    )
    summary = {
        "status": "FAILED",
        "scripts_total": 3,
        "scripts_executed": 3,
        "scripts_ok": 1,
        "scripts_failed": 2,
        "results": [
            {"script_name": "gold_fact_one", "target_table": "gold.fact_one", "status": "SUCCESS"},
            {
                "script_name": "gold_fact_two",
                "target_table": "gold.fact_two",
                "status": "FAILED",
                "error": "missing gold.dim_claims",
            },
            {
                "script_name": "gold_fact_three",
                "target_table": "gold.fact_three",
                "status": "FAILED",
                "error": "missing gold.dim_policy",
            },
        ],
    }
    monkeypatch.setattr(
        databricks_runtime,
        "_run_failure_detail",
        lambda *_: f"RuntimeError: {json.dumps(summary)}",
    )
    saved = []

    def capture_progress(state, **kwargs):
        saved.append((state, kwargs))
        return state

    monkeypatch.setattr(databricks_runtime, "save_external_execution_progress", capture_progress)

    with pytest.raises(RuntimeError, match="gold_fact_two"):
        databricks_runtime.run_databricks_gold_scripts(
            {
                "run_id": "run-failed-gold-batch",
                "target_warehouse": "databricks",
                "gold_generation_results": [
                    {"status": "APPROVED", "script_body": "print('one')", "target_table": "gold.fact_one"},
                    {"status": "APPROVED", "script_body": "print('two')", "target_table": "gold.fact_two"},
                    {"status": "APPROVED", "script_body": "print('three')", "target_table": "gold.fact_three"},
                ],
            }
        )

    failed_state, failed_progress = saved[-1]
    results = failed_state["databricks_gold_execution_results"]
    assert [item["status"] for item in results] == ["SUCCESS", "FAILED", "FAILED"]
    assert failed_progress["completed_count"] == 1
    assert failed_progress["current_name"] == "gold_fact_two"
    assert failed_state["error"].endswith("missing gold.dim_claims")


def test_databricks_gold_llm_retries_then_uses_deterministic_fallback(monkeypatch):
    output_dir = Path.cwd() / ".tmp-tests" / f"gold_llm_fallback_{uuid.uuid4().hex}"
    output_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("ATHENA_GOLD_USE_LLM", "true")
    monkeypatch.setenv("ATHENA_GOLD_LLM_RETRY", "true")
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
    assert "dropped the approved source or target table" in attempts[1]
    assert result["generation_mode"] == "DETERMINISTIC_FALLBACK"
    assert result["fallback_reason"]
    assert "DQ_MAX_" not in body


def test_databricks_gold_llm_falls_back_without_retry_by_default(monkeypatch):
    output_dir = Path.cwd() / ".tmp-tests" / f"gold_llm_no_retry_{uuid.uuid4().hex}"
    output_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("ATHENA_GOLD_USE_LLM", "true")
    monkeypatch.delenv("ATHENA_GOLD_LLM_RETRY", raising=False)
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
        run_id="run-llm-no-retry",
        gold_schema="gold",
        target_warehouse="databricks",
        use_domain_kb=False,
        dimension_contract=[],
        include_dimension=False,
    )

    assert attempts == [None]
    assert result["generation_mode"] == "DETERMINISTIC_FALLBACK"
