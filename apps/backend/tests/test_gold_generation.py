from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest

from nodes import gold_gen
from nodes import silver_gen
from services import databricks_runtime
from services import dbt_snowflake_runtime
from services import pipeline_runtime


def test_metadata_gold_generation_emits_one_artifact_per_fact_and_dimension(monkeypatch):
    monkeypatch.setenv("GOLD_SCHEMA", "gold")
    monkeypatch.setattr(gold_gen, "ai_store_db_writer", lambda **_: None)
    workdir = Path.cwd() / ".tmp-tests" / f"gold_metadata_{uuid.uuid4().hex}"
    workdir.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(workdir)

    inputs = [{"object_name": "main.silver.silver_claims"}]
    dimension_plan = {
        "reference": {
            "artifact_kind": "DIMENSION", "name": "dim_claims", "target_table": "gold.dim_claims",
            "gold_ingestion_object_id": 301, "gold_ingestion_object_config_version": 3,
            "gold_ingestion_object_config_hash": "sha256:dim", "silver_to_gold_mapping_version": 31,
            "silver_to_gold_mapping_hash": "sha256:dim-map",
        },
        "object": {"target_table": "gold.dim_claims"},
        "inputs": inputs,
        "definition": {"artifact_kind": "DIMENSION", "logical_table": "claims"},
        "bundle": {"mappings": [
            {"source_object_name": "main.silver.silver_claims", "source_field_path": "claimid", "source_data_type": "BIGINT", "target_column_name": "claimid", "target_data_type": "BIGINT", "is_primary_key": True},
            {"source_object_name": "main.silver.silver_claims", "source_field_path": "claimstatus", "source_data_type": "STRING", "target_column_name": "claimstatus", "target_data_type": "STRING", "is_primary_key": False},
        ]},
    }
    fact_mapping = {
        "kpi_name": "Total Claims",
        "source_silver_table": "main.silver.silver_claims",
        "measure": {"table": "claims", "column": "claimamount", "aggregation": "SUM"},
        "formula": {"status": "PROPOSED"},
        "grouping_dimensions": [{"table": "claims", "column": "claimstatus", "semantic_type": "DIMENSION"}],
        "time": {"grain": "month", "column": None},
        "filters": [], "join_paths": [], "readiness": "READY",
    }
    fact_plan = {
        "reference": {
            "artifact_kind": "FACT", "name": "fact_total_claims", "target_table": "gold.fact_total_claims",
            "gold_ingestion_object_id": 302, "gold_ingestion_object_config_version": 3,
            "gold_ingestion_object_config_hash": "sha256:fact", "silver_to_gold_mapping_version": 32,
            "silver_to_gold_mapping_hash": "sha256:fact-map",
        },
        "object": {
            "target_table": "gold.fact_total_claims",
            "write_mode": "MERGE",
            "merge_keys_json": '["claimstatus"]',
        },
        "inputs": inputs,
        "definition": {"artifact_kind": "FACT", "mapping": fact_mapping},
        "bundle": {"mappings": [
            {
                "build_order": 20,
                "join_rules_json": "[]",
                "source_object_name": "main.silver.silver_claims",
                "source_field_path": "claimstatus",
                "target_column_name": "claimstatus",
                "target_data_type": "STRING",
                "transformation_rule": "IDENTITY",
            },
            {
                "build_order": 20,
                "join_rules_json": "[]",
                "source_object_name": "main.silver.silver_claims",
                "source_field_path": "claimamount",
                "target_column_name": "total_claims_value",
                "target_data_type": "DECIMAL(38,10)",
                "transformation_rule": "AGG_SUM",
            },
        ]},
    }
    monkeypatch.setattr(gold_gen, "_metadata_gold_plans", lambda _state: [dimension_plan, fact_plan])
    result = gold_gen.gold_code_generation_node({
        "run_id": "metadata-gold",
        "target_warehouse": "databricks",
        "gold_metadata_drafts": [dimension_plan["reference"], fact_plan["reference"]],
        "gold_generation_contract": {
            "status": "READY",
            "silver_tables": [{"table": "claims", "target_table": "main.silver.silver_claims"}],
            "kpi_mappings": [fact_mapping],
        },
    })

    assert result["gold_generation_status"] == "COMPLETED"
    assert [item["artifact_kind"] for item in result["gold_generation_results"]] == ["DIMENSION", "FACT"]
    assert {item["gold_ingestion_object_id"] for item in result["gold_generation_results"]} == {301, 302}
    assert len({item["script_path"] for item in result["gold_generation_results"]}) == 2
    assert all(item["source_table_guard"].get("dropped_source_tables") == [] for item in result["gold_generation_results"])
    generated_code = [Path(item["script_path"]).read_text(encoding="utf-8") for item in result["gold_generation_results"]]
    assert all('globals().get("ATHENA_RUNTIME_CONTEXT")' in code for code in generated_code)
    assert any("__ATHENA_LOGICAL_WORK_ID__" in code for code in generated_code)
    assert all('mode("errorifexists")' not in code for code in generated_code)
    assert all('limit(0).write.format("delta").mode("ignore")' in code for code in generated_code)


def test_metadata_gold_dbt_generation_uses_exact_plan_and_silver_ref(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_GOLD_CATALOG", "INSURANCE")
    monkeypatch.setenv("SNOWFLAKE_GOLD_SCHEMA", "GOLD")
    monkeypatch.setattr(gold_gen, "ai_store_db_writer", lambda **_: None)
    workdir = Path.cwd() / ".tmp-tests" / f"gold_metadata_dbt_{uuid.uuid4().hex}"
    workdir.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(workdir)

    reference = {
        "artifact_kind": "DIMENSION",
        "name": "dim_claims",
        "target_table": "INSURANCE.GOLD.dim_claims",
        "gold_ingestion_object_id": 301,
        "gold_ingestion_object_config_version": 3,
        "gold_ingestion_object_config_hash": "sha256:dim",
        "silver_to_gold_mapping_version": 31,
        "silver_to_gold_mapping_hash": "sha256:dim-map",
    }
    plan = {
        "reference": reference,
        "object": {"target_table": reference["target_table"]},
        "inputs": [{"object_name": "INSURANCE.SILVER.silver_claims"}],
        "definition": {"artifact_kind": "DIMENSION", "logical_table": "claims"},
        "bundle": {"mappings": [
            {
                "source_object_name": "INSURANCE.SILVER.silver_claims",
                "source_field_path": "claimid",
                "target_column_name": "claimid",
                "target_data_type": "NUMBER",
                "is_primary_key": True,
            },
            {
                "source_object_name": "INSURANCE.SILVER.silver_claims",
                "source_field_path": "claimstatus",
                "target_column_name": "claimstatus",
                "target_data_type": "VARCHAR",
                "is_primary_key": False,
            },
        ]},
    }
    monkeypatch.setattr(gold_gen, "_metadata_gold_plans", lambda _state: [plan])
    state = {
        "run_id": "metadata-dbt-gold",
        "target_warehouse": "snowflake",
        "execution_engine": "dbt",
        "gold_catalog": "INSURANCE",
        "gold_schema": "GOLD",
        "gold_metadata_drafts": [reference],
        "gold_generation_contract": {"status": "READY", "dimension_mappings": [{}]},
    }
    dbt_snowflake_runtime.write_snowflake_dbt_scaffold(state)
    silver_path = dbt_snowflake_runtime.dbt_model_path(
        state["run_id"], "silver", "silver_claims"
    )
    silver_path.parent.mkdir(parents=True, exist_ok=True)
    silver_path.write_text("select 1\n", encoding="utf-8")

    result = gold_gen.gold_code_generation_node(state)
    generated = result["gold_generation_results"][0]
    sql = Path(generated["script_path"]).read_text(encoding="utf-8")

    assert generated["code_generation_format"] == "dbt"
    assert generated["generation_mode"] == "METADATA_DBT_SQL"
    assert generated["dbt_alias"] == "dim_claims"
    assert "{{ ref('silver_claims') }}" in sql
    assert "CREATE TABLE" not in sql
    assert "MERGE INTO" not in sql


def test_metadata_gold_dbt_plan_reuses_exact_active_mapping(monkeypatch):
    from types import SimpleNamespace
    from services import metadata_selection

    pin = {
        "ingestion_object_id": 201,
        "config_version": 2,
        "config_hash": "silver-object",
        "mapping_version": 21,
        "mapping_hash": "silver-map",
        "object_name": "INSURANCE.SILVER.silver_claims",
    }

    class Repository:
        def get_ingestion_object(self, object_id, config_version):
            if (object_id, config_version) == (301, 3):
                return {
                    "ingestion_object_id": 301,
                    "config_version": 3,
                    "config_hash": "gold-object",
                    "target_table": "INSURANCE.GOLD.dim_claims",
                    "active_flag": False,
                    "is_current": False,
                }
            assert (object_id, config_version) == (201, 2)
            return {"config_version": 2, "config_hash": "silver-object"}

        def get_mapping_bundle(self, **kwargs):
            assert kwargs["require_active"] is None
            if kwargs["processing_stage"] == "SILVER_TO_GOLD":
                return {"mappings": [{
                    "build_order": 10,
                    "aggregation_rules_json": '{"artifact_kind":"DIMENSION"}',
                    "input_objects_json": json.dumps([pin]),
                }]}
            assert kwargs["processing_stage"] == "BRONZE_TO_SILVER"
            return {"mappings": []}

    monkeypatch.setattr(
        metadata_selection,
        "validated_metadata_selection",
        lambda _state: SimpleNamespace(repository=Repository()),
    )
    plans = gold_gen._metadata_gold_plans({
        "target_warehouse": "snowflake",
        "execution_engine": "dbt",
        "gold_metadata_drafts": [{
            "gold_ingestion_object_id": 301,
            "gold_ingestion_object_config_version": 3,
            "gold_ingestion_object_config_hash": "gold-object",
            "silver_to_gold_mapping_version": 31,
            "silver_to_gold_mapping_hash": "gold-map",
            "target_table": "INSURANCE.GOLD.dim_claims",
        }],
    })

    assert len(plans) == 1
    assert plans[0]["inputs"] == [pin]


def test_snowflake_metadata_fact_uses_exact_mapping_types_keys_and_write_mode() -> None:
    source = "ATHENA_DB.SILVER.silver_claims"
    plan = {
        "object": {
            "target_table": "ATHENA_DB.GOLD.fact_claims",
            "write_mode": "MERGE",
            "merge_keys_json": '["claimstatus"]',
        },
        "inputs": [{"object_name": source}],
        "bundle": {"mappings": [
            {
                "source_object_name": source,
                "source_field_path": "claimstatus",
                "target_column_name": "claimstatus",
                "target_data_type": "VARCHAR(80)",
                "transformation_rule": "GROUP_KEY",
                "join_rules_json": "[]",
            },
            {
                "source_object_name": source,
                "source_field_path": "claimamount",
                "target_column_name": "claim_total",
                "target_data_type": "DECIMAL(38,10)",
                "transformation_rule": "AGG_SUM",
                "join_rules_json": "[]",
            },
        ]},
    }

    sql = gold_gen._metadata_fact_code(plan, target_warehouse="snowflake")

    assert '"claimstatus" VARCHAR(80) NOT NULL' in sql
    assert '"claim_total" DECIMAL(38,10)' in sql
    assert 'SUM(s0."claimamount")' in sql
    assert 'target."claimstatus" = source."claimstatus"' in sql
    assert 's0."_logical_work_id" = $ATHENA_LOGICAL_WORK_ID' in sql
    assert 'FROM "ATHENA_DB"."SILVER"."silver_claims" AS s0' in sql
    assert 'MERGE INTO "ATHENA_DB"."GOLD"."fact_claims" AS target' in sql
    assert " FLOAT" not in sql


@pytest.mark.parametrize("dbt_compatible", [False, True])
def test_snowflake_metadata_global_count_fact_uses_snapshot_and_count_star(dbt_compatible) -> None:
    source = "ANALYTICS.SILVER.silver_events"
    plan = {
        "object": {
            "target_table": "ANALYTICS.GOLD.fact_event_count",
            "write_mode": "SNAPSHOT_REPLACE",
            "merge_keys_json": "[]",
        },
        "inputs": [{"object_name": source}],
        "bundle": {"mappings": [{
            "source_object_name": source,
            "source_field_path": "event_id",
            "target_column_name": "event_count_value",
            "target_data_type": "BIGINT",
            "transformation_rule": "AGG_COUNT",
            "join_rules_json": "[]",
        }]},
    }

    sql = gold_gen._metadata_fact_code(
        plan, target_warehouse="snowflake", dbt_compatible=dbt_compatible
    )

    assert "COUNT(*)" in sql
    assert "COUNT(s0" not in sql
    if dbt_compatible:
        assert 'materialized="table"' in sql
        assert "unique_key=" not in sql
        assert "incremental_strategy=" not in sql
    else:
        assert 'CREATE OR REPLACE TABLE "ANALYTICS"."GOLD"."fact_event_count" AS' in sql
        assert "MERGE INTO" not in sql


@pytest.mark.parametrize(
    ("warehouse", "dbt_compatible"),
    [("databricks", False), ("snowflake", False), ("snowflake", True)],
)
def test_metadata_factless_fact_uses_exact_keys_and_idempotent_merge(
    warehouse, dbt_compatible
) -> None:
    source = "ANALYTICS.SILVER.silver_events"
    plan = {
        "definition": {"artifact_kind": "FACT", "fact_type": "FACTLESS_ENTITY_COVERAGE"},
        "object": {
            "target_table": "ANALYTICS.GOLD.fact_events_coverage",
            "write_mode": "MERGE",
            "merge_keys_json": '["event_id"]',
            "validation_policy_json": '{}',
        },
        "inputs": [{"object_name": source}],
        "bundle": {"mappings": [{
            "source_object_name": source,
            "source_field_path": "event_id",
            "target_column_name": "event_id",
            "target_data_type": "BIGINT",
            "transformation_rule": "GROUP_KEY",
            "join_rules_json": "[]",
        }]},
    }

    code = gold_gen._metadata_fact_code(
        plan, target_warehouse=warehouse, dbt_compatible=dbt_compatible
    )

    assert "SELECT DISTINCT" in code
    assert "AGG_" not in code
    if warehouse == "databricks":
        assert "DeltaTable.forName" in code
        assert "KEYS = ['event_id']" in code
    elif dbt_compatible:
        assert 'materialized="incremental"' in code
        assert 'unique_key="event_id"' in code
        assert "{{ ref('silver_events') }}" in code
    else:
        assert 'MERGE INTO "ANALYTICS"."GOLD"."fact_events_coverage"' in code
        assert 'target."event_id" = source."event_id"' in code


def test_snowflake_metadata_dbt_fact_preserves_multi_input_refs() -> None:
    orders = "INSURANCE.SILVER.silver_orders"
    customers = "INSURANCE.SILVER.silver_customers"
    joins = json.dumps([{
        "left_source_table": orders,
        "right_source_table": customers,
        "left_column": "customerid",
        "right_column": "customerid",
        "join_type": "INNER",
    }])
    plan = {
        "object": {
            "target_table": "INSURANCE.GOLD.fact_orders",
            "write_mode": "MERGE",
            "merge_keys_json": '["status"]',
            "validation_policy_json": json.dumps({
                "rules": [{"rule_type": "MAX_JOIN_MULTIPLIER", "threshold_value": 1.05}]
            }),
        },
        "inputs": [{"object_name": orders}, {"object_name": customers}],
        "bundle": {"mappings": [
            {
                "source_object_name": orders,
                "source_field_path": "status",
                "target_column_name": "status",
                "target_data_type": "VARCHAR",
                "transformation_rule": "GROUP_KEY",
                "join_rules_json": joins,
            },
            {
                "source_object_name": orders,
                "source_field_path": "amount",
                "target_column_name": "total_amount",
                "target_data_type": "DECIMAL(38,10)",
                "transformation_rule": "AGG_SUM",
                "join_rules_json": joins,
            },
        ]},
    }

    sql = gold_gen._metadata_fact_code(
        plan, target_warehouse="snowflake", dbt_compatible=True
    )

    assert "{{ ref('silver_orders') }}" in sql
    assert "{{ ref('silver_customers') }}" in sql
    assert 'SUM(s0."amount")' in sql
    assert "_logical_work_id" not in sql
    assert "CREATE TABLE" not in sql
    assert "MERGE INTO" not in sql
    assert 'materialized="incremental"' in sql
    assert 'unique_key="status"' in sql
    assert '\\"status\\"' not in sql
    assert "__athena_join_guard" in sql
    assert "ATHENA_JOIN_MULTIPLIER_VALIDATION_FAILED" in sql
    assert "joined_count / root_count <= 1.05" in sql


def test_snowflake_metadata_native_fact_preserves_multi_input_qualified_joins() -> None:
    orders = "INSURANCE.SILVER.silver_orders"
    customers = "INSURANCE.SILVER.silver_customers"
    joins = json.dumps([{
        "left_source_table": orders,
        "right_source_table": customers,
        "left_column": "customerid",
        "right_column": "customerid",
        "join_type": "INNER",
    }])
    plan = {
        "object": {
            "target_table": "INSURANCE.GOLD.fact_orders",
            "write_mode": "MERGE",
            "merge_keys_json": '["status"]',
        },
        "inputs": [{"object_name": orders}, {"object_name": customers}],
        "bundle": {"mappings": [
            {
                "source_object_name": customers,
                "source_field_path": "status",
                "target_column_name": "status",
                "target_data_type": "VARCHAR",
                "transformation_rule": "GROUP_KEY",
                "join_rules_json": joins,
            },
            {
                "source_object_name": orders,
                "source_field_path": "amount",
                "target_column_name": "total_amount",
                "target_data_type": "DECIMAL(38,10)",
                "transformation_rule": "AGG_SUM",
                "join_rules_json": joins,
            },
        ]},
    }

    sql = gold_gen._metadata_fact_code(plan, target_warehouse="snowflake")

    assert 'FROM "INSURANCE"."SILVER"."silver_orders" AS s0' in sql
    assert 'INNER JOIN "INSURANCE"."SILVER"."silver_customers" AS s1' in sql
    assert 's0."customerid" = s1."customerid"' in sql
    assert 's0."_logical_work_id" = $ATHENA_LOGICAL_WORK_ID' in sql
    assert 's1."_logical_work_id" = $ATHENA_LOGICAL_WORK_ID' in sql
    assert 'MERGE INTO "INSURANCE"."GOLD"."fact_orders" AS target' in sql
    assert "<function" not in sql


def test_databricks_metadata_fact_reports_observed_join_multiplier() -> None:
    source = "main.silver.orders"
    plan = {
        "object": {
            "target_table": "main.gold.fact_orders",
            "write_mode": "MERGE",
            "merge_keys_json": '["status"]',
            "validation_policy_json": json.dumps({
                "schema_version": "1.0",
                "rules": [{"rule_type": "MAX_JOIN_MULTIPLIER", "threshold_value": 1.05}],
            }),
        },
        "inputs": [{"object_name": source}],
        "bundle": {"mappings": [
            {
                "source_object_name": source,
                "source_field_path": "status",
                "target_column_name": "status",
                "target_data_type": "STRING",
                "transformation_rule": "GROUP_KEY",
                "join_rules_json": "[]",
            },
            {
                "source_object_name": source,
                "source_field_path": "amount",
                "target_column_name": "amount",
                "target_data_type": "DECIMAL(38,10)",
                "transformation_rule": "AGG_SUM",
                "join_rules_json": "[]",
            },
        ]},
    }

    code = gold_gen._metadata_fact_code(plan, target_warehouse="databricks")

    assert "JOINED_COUNT_QUERY" in code
    assert "ROOT_COUNT_QUERY" in code
    assert '"rule_type": "MAX_JOIN_MULTIPLIER"' in code
    assert "observed_join_multiplier" in code
    assert "NOT_NULL_KEYS = []" in code
    assert 'if NOT_NULL_KEYS and mapped.filter(' in code
    assert 'mode("errorifexists")' not in code
    assert 'limit(0).write.format("delta").mode("ignore")' in code

    plan["object"]["validation_policy_json"] = json.dumps({
        "schema_version": "1.0",
        "rules": [{"rule_type": "KEYS_NOT_NULL", "columns": ["status"], "threshold_value": 0}],
    })
    strict_code = gold_gen._metadata_fact_code(plan, target_warehouse="databricks")

    assert "NOT_NULL_KEYS = ['status']" in strict_code
    assert 'if NOT_NULL_KEYS and mapped.filter(' in strict_code


def test_databricks_metadata_dimension_creates_then_merges_idempotently() -> None:
    code = gold_gen._metadata_dimension_code(
        {
            "object": {"target_table": "main.gold.dim_claims"},
            "bundle": {"mappings": [
                {
                    "source_object_name": "main.silver.claims",
                    "source_field_path": "claim_id",
                    "target_column_name": "claim_id",
                    "target_data_type": "BIGINT",
                    "is_primary_key": True,
                },
                {
                    "source_object_name": "main.silver.claims",
                    "source_field_path": "claim_status",
                    "target_column_name": "claim_status",
                    "target_data_type": "STRING",
                    "is_primary_key": False,
                },
            ]},
        },
        target_warehouse="databricks",
    )

    assert 'mode("errorifexists")' not in code
    assert 'limit(0).write.format("delta").mode("ignore")' in code
    assert ".whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()" in code


def test_snowflake_gold_returns_observed_join_multiplier_validation() -> None:
    from services import snowflake_gold_runtime

    orders = "ATHENA_DB.SILVER.silver_orders"
    customers = "ATHENA_DB.SILVER.silver_customers"
    join_rules = json.dumps([{
        "left_source_table": orders,
        "right_source_table": customers,
        "left_column": "customer_id",
        "right_column": "customer_id",
        "join_type": "INNER",
    }])

    class Cursor:
        description = [("status",), ("amount",)]

        def execute(self, sql):
            self.sql = sql

        def fetchone(self):
            if " JOIN " in self.sql:
                return (110,)
            return (100,)

        def close(self):
            pass

    class Connection:
        def cursor(self):
            return Cursor()

    results = snowflake_gold_runtime._blocking_validation_results(
        {
            "target_table": "ATHENA_DB.GOLD.fact_orders",
            "approved_source_tables": [orders, customers],
            "mapping_contract": [
                {
                    "source_object_name": orders,
                    "source_field_path": "amount",
                    "target_column_name": "amount",
                    "transformation_rule": "AGG_SUM",
                    "join_rules_json": join_rules,
                },
                {
                    "source_object_name": customers,
                    "source_field_path": "status",
                    "target_column_name": "status",
                    "transformation_rule": "GROUP_KEY",
                    "join_rules_json": join_rules,
                },
            ],
            "validation_policy": {
                "rules": [{"rule_type": "MAX_JOIN_MULTIPLIER", "threshold_value": 1.2}]
            },
        },
        Connection(),
    )

    assert results == [{
        "rule_type": "MAX_JOIN_MULTIPLIER",
        "observed_value": 1.1,
        "threshold_value": 1.2,
        "status": "PASSED",
    }]

    class InflatedCursor(Cursor):
        def fetchone(self):
            if " JOIN " in self.sql:
                return (200,)
            return (100,)

    class InflatedConnection:
        def cursor(self):
            return InflatedCursor()

    with pytest.raises(RuntimeError, match="pre-write validation failed"):
        snowflake_gold_runtime._prewrite_validation_results(
            {
                "approved_source_tables": [orders, customers],
                "mapping_contract": [
                    {
                        "source_object_name": orders,
                        "transformation_rule": "AGG_SUM",
                        "join_rules_json": join_rules,
                    }
                ],
                "validation_policy": {
                    "rules": [{"rule_type": "MAX_JOIN_MULTIPLIER", "threshold_value": 1.2}]
                },
            },
            InflatedConnection(),
        )


def test_snowflake_gold_returns_observed_key_validation() -> None:
    from services import snowflake_gold_runtime

    class Cursor:
        description = [("claim_key",), ("_logical_work_id",)]

        def execute(self, sql):
            self.sql = sql

        def fetchone(self):
            return (0,)

        def close(self):
            pass

    class Connection:
        def cursor(self):
            return Cursor()

    results = snowflake_gold_runtime._blocking_validation_results(
        {
            "target_table": "ATHENA_DB.GOLD.dim_claim",
            "merge_keys": ["claim_key"],
            "mapping_contract": [{"target_column_name": "claim_key"}],
            "validation_policy": {
                "rules": [
                    {"rule_type": "KEYS_NOT_NULL", "columns": ["claim_key"], "threshold_value": 0},
                    {"rule_type": "KEYS_UNIQUE", "columns": ["claim_key"], "threshold_value": 0},
                ]
            },
        },
        Connection(),
    )

    assert [result["rule_type"] for result in results] == ["KEYS_NOT_NULL", "KEYS_UNIQUE"]
    assert all(result["observed_value"] == 0 and result["status"] == "PASSED" for result in results)


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


def test_snowflake_gold_upsert_identity_excludes_the_measure_value():
    sql = gold_gen.generate_snowflake_gold_script(
        mapping={
            "kpi_name": "Order Value",
            "source_silver_table": "ANALYTICS.SILVER.silver_orders",
            "measure": {"table": "orders", "column": "amount", "aggregation": "SUM"},
            "grouping_dimensions": [
                {"table": "orders", "column": "status", "semantic_type": "DIMENSION"}
            ],
            "time": {},
            "filters": [],
            "join_paths": [],
            "readiness": "READY",
        },
        run_id="run-stable-grain",
        gold_catalog="ANALYTICS",
        gold_schema="GOLD",
    )

    upsert_line = next(line for line in sql.splitlines() if "MD5(CONCAT_WS" in line)
    assert '"status"' in upsert_line
    assert '"order_value_value"' not in upsert_line


def test_gold_mapping_source_table_guard_retains_all_certified_connected_inputs():
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

    assert guard["max_source_tables"] is None
    assert guard["kept_source_tables"] == ["claim_information", "policy_transactions", "measures"]
    assert guard["dropped_source_tables"] == ["claim_payment_expenses"]
    assert guard["dropped_malformed_join_paths"] == 1
    assert guard["dropped_join_paths"] == 1
    assert [path["right_table"] for path in sanitized["join_paths"]] == ["policy_transactions", "measures"]
    assert all("claim_payment_expenses" not in path.values() for path in sanitized["join_paths"])


def test_databricks_gold_script_uses_sanitized_join_paths(monkeypatch):
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


def test_gold_contract_includes_dimensions_from_certified_join_tables(monkeypatch):
    for key in silver_gen.KIMBALL_LLM_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    results = [
        {
            "table": "claim_information",
            "source_table": "bronze.claim_information",
            "target_table": "silver.silver_claim_information",
            "column_count": 3,
            "merge_keys": ["claim_id"],
        },
        {
            "table": "policy_transactions",
            "source_table": "bronze.policy_transactions",
            "target_table": "silver.silver_policy_transactions",
            "column_count": 2,
            "merge_keys": ["policy_id"],
        },
    ]
    enriched_metadata = {
        "columns": [
            {"table_name": "claim_information", "column_name": "claim_amount", "data_type": "decimal(18,2)", "semantic_type": "MEASURE", "is_measure": True},
            {"table_name": "claim_information", "column_name": "claim_status", "data_type": "varchar", "semantic_type": "DIMENSION"},
            {"table_name": "policy_transactions", "column_name": "policy_state", "data_type": "varchar", "semantic_type": "DIMENSION"},
            {"table_name": "claim_information", "column_name": "claim_open_date", "data_type": "date", "semantic_type": "DATE"},
            {"table_name": "claim_information", "column_name": "policy_id", "data_type": "bigint", "semantic_type": "ID"},
            {"table_name": "policy_transactions", "column_name": "policy_id", "data_type": "bigint", "semantic_type": "ID"},
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


def test_independent_gold_dimensions_keep_three_best_executable_entities():
    results = [
        {
            "table": name,
            "target_table": f"silver.silver_{name}",
            "merge_keys": [f"{name}_id"] if name != "unkeyed" else [],
        }
        for name in ("claims", "policies", "agents", "payments", "unkeyed")
    ]
    columns = [
        {
            "table_name": table,
            "column_name": f"attribute_{ordinal}",
            "semantic_type": "DIMENSION",
        }
        for table, count in (("claims", 4), ("policies", 3), ("agents", 2), ("payments", 1), ("unkeyed", 8))
        for ordinal in range(count)
    ]

    dimensions = silver_gen._independent_gold_dimensions(
        columns=columns,
        results=results,
        silver_tables={item["table"]: item["target_table"] for item in results},
        kpi_mappings=[],
    )

    assert [item["logical_table"] for item in dimensions] == ["claims", "policies", "agents"]


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
        {"table_name": "claims", "column_name": "claim_amount", "data_type": "decimal(18,2)", "semantic_type": "MEASURE", "is_measure": True},
        {"table_name": "claims", "column_name": "claim_status", "data_type": "varchar", "semantic_type": "DIMENSION"},
        {"table_name": "claims", "column_name": "claim_date", "data_type": "date", "semantic_type": "DATE"},
        {"table_name": "claims", "column_name": "policy_id", "data_type": "bigint", "semantic_type": "ID"},
        {"table_name": "policies", "column_name": "policy_id", "data_type": "bigint", "semantic_type": "ID"},
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
        {"table_name": "claims", "column_name": "claim_amount", "data_type": "decimal(18,2)", "semantic_type": "MEASURE", "is_measure": True},
        {"table_name": "claims", "column_name": "claim_status", "data_type": "varchar", "semantic_type": "DIMENSION"},
        {"table_name": "claims", "column_name": "claim_date", "data_type": "date", "semantic_type": "DATE"},
        {"table_name": "claims", "column_name": "policy_id", "data_type": "bigint", "semantic_type": "ID"},
        {"table_name": "policies", "column_name": "policy_id", "data_type": "bigint", "semantic_type": "ID"},
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
        {"table_name": "claims", "column_name": "claim_amount", "data_type": "decimal(18,2)", "semantic_type": "MEASURE"},
        {"table_name": "claims", "column_name": "claim_status", "data_type": "varchar", "semantic_type": "DIMENSION"},
    ]
    plan = {
        "measure": {"table": "claims", "column": "claim_amount", "aggregation": "SUM"},
        "dimensions": [{"table": "claims", "column": "claim_status"}],
        "fact_grain": ["wrong_column"],
    }

    with pytest.raises(ValueError, match="invalid fact grain"):
        silver_gen._validate_kimball_plan(plan, columns=columns, certified_joins=[])


def test_kimball_plan_rejects_incompatible_measure_and_join_types():
    columns = [
        {"table_name": "claims", "column_name": "claim_status", "data_type": "varchar", "semantic_type": "MEASURE"},
        {"table_name": "claims", "column_name": "policy_id", "data_type": "bigint", "semantic_type": "ID"},
        {"table_name": "policies", "column_name": "policy_id", "data_type": "varchar", "semantic_type": "ID"},
    ]
    join = {
        "left_table": "claims", "left_column": "policy_id",
        "right_table": "policies", "right_column": "policy_id", "certified": True,
    }

    with pytest.raises(ValueError, match="non-numeric measure"):
        silver_gen._validate_kimball_plan(
            {
                "measure": {"table": "claims", "column": "claim_status", "aggregation": "SUM"},
                "dimensions": [], "join_paths": [], "fact_grain": [],
            },
            columns=columns,
            certified_joins=[join],
        )

    with pytest.raises(ValueError, match="incompatible key datatypes"):
        silver_gen._validate_kimball_plan(
            {
                "measure": {"table": "claims", "column": "claim_status", "aggregation": "COUNT"},
                "dimensions": [], "join_paths": [join], "fact_grain": [],
            },
            columns=columns,
            certified_joins=[join],
        )


def test_guarded_kimball_failure_is_not_replaced_by_fallback(monkeypatch):
    monkeypatch.setenv("ATHENA_GOLD_KIMBALL_PLAN_USE_LLM", "true")
    monkeypatch.setattr(
        silver_gen,
        "_llm_kimball_plan",
        lambda **_kwargs: (_ for _ in ()).throw(ValueError("invalid candidate")),
    )
    contract = silver_gen._build_gold_generation_contract(
        state={"run_id": "run-no-fallback", "certified_kpis": [{"kpi_name": "Total Claim Amount"}]},
        results=[{
            "table": "claims", "source_table": "bronze.claims",
            "target_table": "silver.silver_claims", "column_count": 2,
            "merge_keys": ["claim_id"],
        }],
        enriched_metadata={"columns": [
            {"table_name": "claims", "column_name": "claim_amount", "data_type": "decimal(18,2)", "semantic_type": "MEASURE", "is_measure": True},
            {"table_name": "claims", "column_name": "claim_status", "data_type": "varchar", "semantic_type": "DIMENSION"},
        ]},
        generated_at="2026-08-10T00:00:00Z",
    )

    assert contract["kpi_mappings"][0]["readiness"] == "BLOCKED"
    assert contract["kpi_mappings"][0]["kimball_plan_source"] == "LLM_REJECTED"
    assert contract["dimension_mappings"][0]["logical_table"] == "claims"
    assert contract["factless_mappings"] == [{
        "fact_type": "FACTLESS_ENTITY_COVERAGE",
        "logical_table": "claims",
        "source_silver_table": "silver.silver_claims",
        "grain_columns": ["claim_id"],
        "readiness": "PENDING_EXACT_KEY_VALIDATION",
    }]
    assert contract["status"] == "READY_WITH_WARNINGS"
    assert "invalid candidate" in contract["kpi_mappings"][0]["mapping_validation_error"]


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


def test_gold_contract_retains_all_available_dimensions_and_drops_unavailable_silver_joins():
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
    assert constrained["selected_dimension_tables"] == ["agents", "claims", "policies"]
    assert {item["table"] for item in constrained["grouping_dimensions"]} == {"agents", "claims", "policies"}
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
    assert "profile_dimensions = list(dict.fromkeys(_resolve_columns(df, DIMENSION_COLUMNS)))" in code
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


def test_metadata_gold_batch_disables_legacy_partial_success(monkeypatch):
    monkeypatch.setenv("ATHENA_DATABRICKS_GOLD_ALLOW_PARTIAL_SUCCESS", "true")

    notebook = databricks_runtime._build_batch_driver_notebook(
        "gold",
        [{
            "gold_ingestion_object_id": 301,
            "status": "APPROVED",
            "script_body": "raise RuntimeError('boom')",
            "target_table": "gold.fact_one",
        }],
        workspace_dir="/Workspace/athena/run",
    )

    assert "_ALLOW_PARTIAL_SUCCESS = False" in notebook
    assert "_CONTINUE_ON_ERROR = False" in notebook


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


def test_databricks_metadata_batch_output_failure_persists_failed_progress(monkeypatch):
    monkeypatch.setenv("ATHENA_EXECUTE_DATABRICKS_GOLD", "true")
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
        lambda *_: {"notebook_output": {"result": json.dumps({
            "status": "FAILED",
            "results": [{
                "script_name": "gold_fact_one",
                "target_table": "gold.fact_one",
                "status": "FAILED",
                "error": "unsupported expression",
            }],
        })}},
    )
    saved = []

    def capture_progress(state, **kwargs):
        saved.append((state, kwargs))
        return state

    monkeypatch.setattr(databricks_runtime, "save_external_execution_progress", capture_progress)
    script = {
        "status": "APPROVED",
        "script_body": "raise RuntimeError('unsupported expression')",
        "target_table": "gold.fact_one",
        "gold_ingestion_object_id": 301,
        "metadata_runtime": True,
    }

    with pytest.raises(databricks_runtime.DatabricksBatchExecutionError, match="unsupported expression"):
        databricks_runtime.run_databricks_gold_scripts({
            "run_id": "design-run",
            "target_warehouse": "databricks",
            "metadata_runtime_batch": True,
            "metadata_runtime_context": {"queue_id": 1, "attempt_number": 1},
            "_metadata_runtime_scripts": [script],
        })

    failed_state, failed_progress = saved[-1]
    assert failed_state["failed_background_stage"] == "gold_code_execution"
    assert failed_progress["status"] == "FAILED"
    assert failed_progress["current_name"] == "gold_fact_one"
    assert failed_progress["current_target"] == "gold.fact_one"


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
