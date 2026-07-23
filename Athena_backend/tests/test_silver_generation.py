from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from nodes import silver_gen
from services import pipeline_runtime


def test_silver_llm_source_identifier_case_is_repaired():
    table_ref = {
        "database_name": "insurance",
        "schema_name": "dbo",
        "table_name": "claims",
        "bronze_table": "ATHENA_DB.BRONZE.bronze_claims",
        "silver_table": "ATHENA_DB.SILVER.silver_claims",
        "existing_script_path": None,
        "source_columns": [{"column_name": "claimid", "source_column_name": "claimid", "type": "VARCHAR"}],
    }

    repaired = silver_gen._canonicalize_snowflake_source_identifiers(
        'SELECT src."ClaimID", src."run_id" FROM "ATHENA_DB"."BRONZE"."bronze_claims" src',
        table_ref,
    )

    assert 'src."claimid"' in repaired
    assert 'src."run_id"' in repaired


def test_silver_llm_rejects_unsafe_snowflake_temporal_try_cast():
    table_ref = {
        "database_name": "insurance",
        "schema_name": "dbo",
        "table_name": "claims",
        "bronze_table": "ATHENA_DB.BRONZE.bronze_claims",
        "silver_table": "ATHENA_DB.SILVER.silver_claims",
        "existing_script_path": None,
        "source_columns": [{"column_name": "inserteddate", "source_column_name": "inserteddate", "type": "DATE"}],
    }
    sql = '''
CREATE TABLE "ATHENA_DB"."SILVER"."silver_claims" ("inserteddate" DATE);
MERGE INTO "ATHENA_DB"."SILVER"."silver_claims" target USING (
SELECT TRY_CAST(src."inserteddate" AS DATE) AS "inserteddate",
src."run_id" AS "run_id", src."ingestion_timestamp" AS "ingestion_timestamp",
src."source_system" AS "source_system", src."source_table" AS "source_table",
'key' AS "silver_upsert_key", 'run' AS "silver_run_id", CURRENT_TIMESTAMP AS "silver_processed_timestamp"
FROM "ATHENA_DB"."BRONZE"."bronze_claims" src) source ON 1 = 0;
'''

    with pytest.raises(ValueError, match="unsafe Snowflake temporal conversion"):
        silver_gen._validate_generated_silver_code(
            sql,
            table_ref=table_ref,
            enriched_columns=[{"column_name": "inserteddate"}],
            target_warehouse="snowflake",
        )


def test_silver_llm_rejects_direct_snowflake_temporal_conversion():
    table_ref = {
        "database_name": "insurance",
        "schema_name": "dbo",
        "table_name": "claims",
        "bronze_table": "ATHENA_DB.BRONZE.bronze_claims",
        "silver_table": "ATHENA_DB.SILVER.silver_claims",
        "existing_script_path": None,
        "source_columns": [{"column_name": "inserteddate", "source_column_name": "inserteddate", "type": "DATE"}],
    }
    sql = '''
CREATE TABLE "ATHENA_DB"."SILVER"."silver_claims" ("inserteddate" DATE);
MERGE INTO "ATHENA_DB"."SILVER"."silver_claims" target USING (
SELECT TRY_TO_TIMESTAMP_NTZ(src."inserteddate") AS "inserteddate",
src."run_id" AS "run_id", src."ingestion_timestamp" AS "ingestion_timestamp",
src."source_system" AS "source_system", src."source_table" AS "source_table",
'key' AS "silver_upsert_key", 'run' AS "silver_run_id", CURRENT_TIMESTAMP AS "silver_processed_timestamp"
FROM "ATHENA_DB"."BRONZE"."bronze_claims" src) source ON 1 = 0;
'''

    with pytest.raises(ValueError, match="unsafe Snowflake temporal conversion"):
        silver_gen._validate_generated_silver_code(
            sql,
            table_ref=table_ref,
            enriched_columns=[{"column_name": "inserteddate"}],
            target_warehouse="snowflake",
        )


def test_silver_llm_repairs_direct_snowflake_temporal_conversion():
    repaired = silver_gen._canonicalize_snowflake_temporal_conversions(
        'SELECT TRY_TO_TIMESTAMP_NTZ(src."inserteddate"), TRY_TO_DATE(src."paiddate")'
    )

    assert 'TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR(src."inserteddate"))' in repaired
    assert 'TRY_TO_DATE(TO_VARCHAR(src."paiddate"))' in repaired


def test_silver_llm_rejects_comment_only_source_and_destructive_sql():
    table_ref = {
        "database_name": "insurance",
        "schema_name": "dbo",
        "table_name": "claims",
        "bronze_table": "ATHENA_DB.BRONZE.bronze_claims",
        "silver_table": "ATHENA_DB.SILVER.silver_claims",
        "existing_script_path": None,
        "source_columns": [],
    }
    sql = '''
-- FROM "ATHENA_DB"."BRONZE"."bronze_claims" AS src
MERGE INTO "ATHENA_DB"."SILVER"."silver_claims" AS target
USING (SELECT * FROM "OTHER_DB"."BRONZE"."claims" AS src) source ON 1 = 0
WHEN NOT MATCHED THEN INSERT DEFAULT VALUES;
DROP TABLE "ATHENA_DB"."SILVER"."silver_claims";
'''

    with pytest.raises(ValueError, match="approved Bronze table"):
        silver_gen._require_snowflake_silver_structure(sql, table_ref)


def test_silver_table_resolution_ignores_existing_silver_outputs(monkeypatch):
    output_dir = Path.cwd() / ".tmp-tests" / f"silver_existing_{uuid.uuid4().hex}" / "silver"
    output_dir.mkdir(parents=True)
    stale_name = (
        "silver_transform_run_a_run_b_run_c_claim_payment_expenses.py"
    )
    (output_dir / stale_name).write_text("# stale output\n", encoding="utf-8")

    monkeypatch.setattr(silver_gen, "_silver_output_dir", lambda: str(output_dir))
    monkeypatch.setattr(silver_gen, "_load_bronze_bundle", lambda target_warehouse="databricks": {"scripts": []})

    refs = silver_gen._resolve_tables_for_silver(
        {
            "certified_tables": [
                {
                    "database_name": "insurance",
                    "schema_name": "dbo",
                    "table_name": "claim_payment_expenses",
                }
            ],
            "bronze_schema": "bronze",
            "silver_schema": "silver",
        }
    )

    assert [ref["table_name"] for ref in refs] == ["claim_payment_expenses"]


def test_silver_file_slug_caps_long_table_names():
    long_name = "018c963b_38fe_4567_b413_ae0f7dba5a68_" * 4 + "claim_payment_expenses"

    slug = silver_gen._file_slug(long_name)

    assert len(slug) <= 64
    assert slug.endswith("_" + silver_gen.hashlib.sha1(long_name.encode("utf-8")).hexdigest()[:8])


def test_reviewed_merge_keys_override_semantic_id_fallback():
    columns = [
        {"table_name": "claims", "column_name": "claim_id", "semantic_type": "ID", "is_join_key": False},
        {"table_name": "claims", "column_name": "policy_number", "semantic_type": "ID", "is_join_key": True},
    ]

    assert silver_gen._key_columns(columns) == ["policy_number"]


def test_gate4_review_clears_unselected_inferred_merge_keys():
    metadata = {
        "columns": [
            {"table_name": "claims", "column_name": "claim_id", "semantic_type": "ID", "is_join_key": True},
            {"table_name": "claims", "column_name": "policy_number", "semantic_type": "ID", "is_join_key": True},
        ]
    }
    review = {"feeds": [{"table": "claims", "primary_keys": ["policy_number"]}]}

    reviewed = pipeline_runtime._apply_gate4_merge_keys_to_metadata(metadata, review)
    columns_by_name = {item["column_name"]: item for item in reviewed["columns"]}

    assert columns_by_name["claim_id"]["is_join_key"] is False
    assert columns_by_name["policy_number"]["is_join_key"] is True
    assert silver_gen._key_columns(reviewed["columns"]) == ["policy_number"]


def test_snowflake_silver_generation_reads_bronze_and_uses_reviewed_merge_keys(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_BRONZE_CATALOG", "ATHENA_DB")
    monkeypatch.setenv("SNOWFLAKE_BRONZE_SCHEMA", "BRONZE")
    monkeypatch.setenv("SNOWFLAKE_SILVER_CATALOG", "ATHENA_DB")
    monkeypatch.setenv("SNOWFLAKE_SILVER_SCHEMA", "SILVER")
    monkeypatch.setattr(silver_gen, "ai_store_db_writer", lambda **_: None)
    workdir = Path.cwd() / ".tmp-tests" / f"silver_snowflake_{uuid.uuid4().hex}"
    workdir.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(workdir)

    state = {
        "run_id": "run-snowflake-silver",
        "target_warehouse": "snowflake",
        "bronze_generation_results": [
            {
                "run_id": "run-snowflake-silver",
                "table": "claim_information",
                "target_table": "ATHENA_DB.BRONZE.bronze_claim_information",
                "target_warehouse": "snowflake",
            }
        ],
        "enriched_metadata": {
            "gate4_reviewed_merge_keys": {"feeds": []},
            "columns": [
                {
                    "table_name": "claim_information",
                    "column_name": "claim_id",
                    "data_type": "int",
                    "semantic_type": "ID",
                    "is_join_key": True,
                },
                {
                    "table_name": "claim_information",
                    "column_name": "claim_amount",
                    "data_type": "decimal",
                    "numeric_precision": 12,
                    "numeric_scale": 2,
                },
            ],
        },
    }

    result = silver_gen.silver_code_generation_node(state)
    script = result["silver_generation_results"][0]
    sql = Path(script["script_path"]).read_text(encoding="utf-8")

    assert result["silver_generation_status"] == "COMPLETED"
    assert script["script_language"] == "sql"
    assert script["target_warehouse"] == "snowflake"
    assert script["source_table"] == "ATHENA_DB.BRONZE.bronze_claim_information"
    assert script["target_table"] == "ATHENA_DB.SILVER.silver_claim_information"
    assert script["merge_keys"] == ["claim_id"]
    assert Path(script["script_path"]).parts[-3:] == ("snowflake", "silver", Path(script["script_path"]).name)
    assert "-- Expected runtime: Snowflake SQL" in sql
    assert 'FROM "ATHENA_DB"."BRONZE"."bronze_claim_information" AS src' in sql
    assert 'MERGE INTO "ATHENA_DB"."SILVER"."silver_claim_information" AS target' in sql
    assert 'PARTITION BY "silver_upsert_key"' in sql
    assert "pyspark" not in sql.lower()


def test_snowflake_silver_uses_state_bronze_results_without_old_bundle_bleed(monkeypatch):
    monkeypatch.setattr(silver_gen, "_load_bronze_bundle", lambda target_warehouse="databricks": {
        "scripts": [
            {
                "table": "old_policy_table",
                "target_table": "ATHENA_DB.BRONZE.bronze_old_policy_table",
            }
        ]
    })

    refs = silver_gen._resolve_tables_for_silver(
        {
            "run_id": "run-current",
            "target_warehouse": "snowflake",
            "bronze_generation_results": [
                {
                    "table": "claim_information",
                    "target_table": "ATHENA_DB.BRONZE.bronze_claim_information",
                }
            ],
        }
    )

    assert [ref["table_name"] for ref in refs] == ["claim_information"]


def test_snowflake_silver_reads_actual_bronze_column_before_alias_correction(monkeypatch):
    table_ref = {
        "database_name": "insurance",
        "schema_name": "dbo",
        "table_name": "claim_information",
        "bronze_table": "ATHENA_DB.BRONZE.bronze_claim_information",
        "silver_table": "ATHENA_DB.SILVER.silver_claim_information",
        "existing_script_path": None,
        "source_columns": [],
    }

    sql = silver_gen.generate_snowflake_silver_script(
        table_ref=table_ref,
        enriched_columns=[
            {
                "table_name": "claim_information",
                "column_name": "rererence_id",
                "source_column_name": "rererence_id",
                "data_type": "varchar",
                "is_join_key": True,
            }
        ],
        run_id="run-correction",
    )

    assert "GET_IGNORE_CASE(OBJECT_CONSTRUCT_KEEP_NULL(src.*), 'rererence_id')" in sql
    assert 'AS "reference_id"' in sql
    assert 'src."reference_id"' not in sql


def test_snowflake_silver_uses_cast_not_try_cast_for_typed_bronze_columns():
    table_ref = {
        "database_name": "insurance",
        "schema_name": "dbo",
        "table_name": "claim_information",
        "bronze_table": "ATHENA_DB.BRONZE.bronze_claim_information",
        "silver_table": "ATHENA_DB.SILVER.silver_claim_information",
        "existing_script_path": None,
        "source_columns": [],
    }

    sql = silver_gen.generate_snowflake_silver_script(
        table_ref=table_ref,
        enriched_columns=[
            {
                "table_name": "claim_information",
                "column_name": "claim_open_date",
                "data_type": "datetime2",
            }
        ],
        run_id="run-cast",
    )

    assert "TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR(GET_IGNORE_CASE(OBJECT_CONSTRUCT_KEEP_NULL(src.*), 'claim_open_date')))" in sql
    assert 'TRY_CAST(' not in sql


def test_databricks_silver_uses_serverless_safe_try_cast():
    table_ref = {
        "database_name": "insurance",
        "schema_name": "dbo",
        "table_name": "claim_information",
        "bronze_table": "workspace.bronze.bronze_claim_information",
        "silver_table": "workspace.silver.silver_claim_information",
        "existing_script_path": None,
        "source_columns": [],
    }

    script = silver_gen.generate_silver_script(
        table_ref=table_ref,
        enriched_columns=[
            {
                "table_name": "claim_information",
                "column_name": "claim_open_date",
                "data_type": "datetime2",
            }
        ],
        run_id="run-cast",
    )

    assert "spark.databricks.delta.schema.autoMerge.enabled" not in script
    assert "try_cast(`{escaped_name}` AS {target_type})" in script


def test_databricks_silver_skips_duplicate_expected_output_columns():
    table_ref = {
        "database_name": "insurance",
        "schema_name": "dbo",
        "table_name": "policy_cover_level_transactions_dup_del",
        "bronze_table": "workspace.bronze.bronze_policy_cover_level_transactions_dup_del",
        "silver_table": "workspace.silver.silver_policy_cover_level_transactions_dup_del",
        "existing_script_path": None,
        "source_columns": [],
    }

    script = silver_gen.generate_silver_script(
        table_ref=table_ref,
        enriched_columns=[
            {"column_name": "cover_name", "data_type": "varchar"},
            {"column_name": "cover_name", "data_type": "varchar"},
            {"column_name": "detail_num", "data_type": "int"},
        ],
        run_id="run-duplicates",
    )

    assert "selected_output_columns = set()" in script
    assert "if expected_name in selected_output_columns:" in script
    assert "selected_output_columns.add(expected_name)" in script


def test_databricks_silver_merges_only_columns_shared_with_existing_target():
    table_ref = {
        "database_name": "insurance",
        "schema_name": "dbo",
        "table_name": "expenses_outstanding_estimates",
        "bronze_table": "workspace.bronze.bronze_expenses_outstanding_estimates",
        "silver_table": "workspace.silver.silver_expenses_outstanding_estimates",
        "existing_script_path": None,
        "source_columns": [],
    }

    script = silver_gen.generate_silver_script(
        table_ref=table_ref,
        enriched_columns=[
            {"column_name": "claimid", "data_type": "bigint", "is_join_key": True},
            {"column_name": "rererence_id", "data_type": "bigint"},
        ],
        run_id="run-target-schema-drift",
    )

    assert "common_columns = [" in script
    assert "if name in source_columns" in script
    assert "whenMatchedUpdate(set=update_assignments)" in script
    assert "whenNotMatchedInsert(values=insert_assignments)" in script
    assert "whenMatchedUpdateAll" not in script
    assert "whenNotMatchedInsertAll" not in script


def test_load_silver_scripts_prefers_snowflake_bundle_for_snowflake_run(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_BRONZE_CATALOG", "ATHENA_DB")
    monkeypatch.setenv("SNOWFLAKE_BRONZE_SCHEMA", "BRONZE")
    monkeypatch.setenv("SNOWFLAKE_SILVER_CATALOG", "ATHENA_DB")
    monkeypatch.setenv("SNOWFLAKE_SILVER_SCHEMA", "SILVER")
    monkeypatch.setattr(silver_gen, "ai_store_db_writer", lambda **_: None)
    workdir = Path.cwd() / ".tmp-tests" / f"silver_loader_{uuid.uuid4().hex}"
    workdir.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(workdir)

    state = {
        "run_id": "run-snowflake-loader",
        "target_warehouse": "snowflake",
        "bronze_generation_results": [
            {
                "run_id": "run-snowflake-loader",
                "table": "measures",
                "target_table": "ATHENA_DB.BRONZE.bronze_measures",
            }
        ],
        "enriched_metadata": {
            "columns": [
                {"table_name": "measures", "column_name": "measure_id", "data_type": "int", "is_join_key": True},
            ],
        },
    }

    checkpoint = silver_gen.silver_code_generation_node(state)
    loaded = pipeline_runtime.load_silver_scripts("run-snowflake-loader", checkpoint)

    assert len(loaded["scripts"]) == 1
    assert loaded["scripts"][0]["script_language"] == "sql"
    assert 'MERGE INTO "ATHENA_DB"."SILVER"."silver_measures"' in loaded["scripts"][0]["script_body"]


def test_snowflake_silver_generates_one_script_per_approved_bronze_result(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_BRONZE_CATALOG", "ATHENA_DB")
    monkeypatch.setenv("SNOWFLAKE_BRONZE_SCHEMA", "BRONZE")
    monkeypatch.setenv("SNOWFLAKE_SILVER_CATALOG", "ATHENA_DB")
    monkeypatch.setenv("SNOWFLAKE_SILVER_SCHEMA", "SILVER")
    monkeypatch.setattr(silver_gen, "ai_store_db_writer", lambda **_: None)
    workdir = Path.cwd() / ".tmp-tests" / f"silver_four_tables_{uuid.uuid4().hex}"
    workdir.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(workdir)

    state = {
        "run_id": "run-four-silver",
        "target_warehouse": "snowflake",
        "bronze_generation_results": [
            {
                "run_id": "run-four-silver",
                "table": table,
                "target_table": f"ATHENA_DB.BRONZE.bronze_{table}",
                "source_columns": [{"target": f"{table}_id", "type": "NUMBER(38,0)"}],
            }
            for table in ("claims", "policy", "payments", "coverage")
        ],
    }

    result = silver_gen.silver_code_generation_node(state)

    assert result["silver_generation_status"] == "COMPLETED"
    assert sorted(item["table"] for item in result["silver_generation_results"]) == [
        "claims",
        "coverage",
        "payments",
        "policy",
    ]
    assert len(result["silver_generation_results"]) == 4
