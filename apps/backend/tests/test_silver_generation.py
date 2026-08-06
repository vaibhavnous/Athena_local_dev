from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest

from nodes import bronze_gen, silver_gen
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


def test_metadata_silver_inputs_reload_exact_target_resident_draft(monkeypatch):
    from services import metadata_selection

    mapping_rows = [
        {
            "source_object_name": "main.bronze.bronze_claims",
            "target_table": "main.silver.silver_claims",
            "input_objects_json": json.dumps([{
                "ingestion_object_id": 101,
                "config_version": 2,
                "config_hash": "sha256:bronze-object",
                "mapping_version": 7,
                "mapping_hash": "sha256:bronze-mapping",
            }]),
            "source_field_path": "claimid",
            "source_data_type": "int",
            "target_column_name": "claimid",
            "target_data_type": "int",
            "is_primary_key": True,
            "transformation_rule": "CAST",
        },
        {
            "source_object_name": "main.bronze.bronze_claims",
            "target_table": "main.silver.silver_claims",
            "input_objects_json": json.dumps([{
                "ingestion_object_id": 101,
                "config_version": 2,
                "config_hash": "sha256:bronze-object",
                "mapping_version": 7,
                "mapping_hash": "sha256:bronze-mapping",
            }]),
            "source_field_path": "description",
            "source_data_type": "string",
            "target_column_name": "description",
            "target_data_type": "string",
            "is_primary_key": False,
            "transformation_rule": "TRIM_CAST",
        },
    ]

    class Repository:
        def get_ingestion_object(self, object_id, config_version):
            assert (object_id, config_version) == (202, 3)
            return {
                "ingestion_object_id": 202,
                "config_version": 3,
                "config_hash": "sha256:silver-object",
                "object_kind": "TRANSFORMATION",
                "processing_stage": "BRONZE_TO_SILVER",
                "target_table": "main.silver.silver_claims",
                "merge_keys_json": '["claimid"]',
                "active_flag": False,
                "is_current": False,
            }

        def get_active_ingestion_object(self, object_id):
            assert object_id == 101
            return {
                "ingestion_object_id": 101,
                "config_version": 2,
                "config_hash": "sha256:bronze-object",
                "target_bronze_table": "main.bronze.bronze_claims",
            }

        def get_mapping_bundle(self, **kwargs):
            if kwargs["processing_stage"] == "BRONZE_TO_SILVER":
                assert kwargs["mapping_version"] == 11
                assert kwargs["expected_hash"] == "sha256:silver-mapping"
                assert kwargs["require_active"] is False
                return {"mappings": mapping_rows}
            assert kwargs["ingestion_object_id"] == 101
            assert kwargs["mapping_version"] == 7
            assert kwargs["expected_hash"] == "sha256:bronze-mapping"
            assert kwargs["require_active"] is True
            return {"mappings": []}

    monkeypatch.setattr(
        metadata_selection,
        "validated_metadata_selection",
        lambda _state: type("Selection", (), {"repository": Repository()})(),
    )
    refs = silver_gen._metadata_tables_for_silver({
        "target_warehouse": "databricks",
        "bronze_generation_results": [{
            "silver_ingestion_object_id": 202,
            "silver_ingestion_object_config_version": 3,
            "silver_ingestion_object_config_hash": "sha256:silver-object",
            "bronze_to_silver_mapping_version": 11,
            "bronze_to_silver_mapping_hash": "sha256:silver-mapping",
        }],
    })

    assert len(refs) == 1
    assert refs[0]["bronze_table"] == "main.bronze.bronze_claims"
    assert refs[0]["silver_table"] == "main.silver.silver_claims"
    assert refs[0]["mapping_columns"][0]["is_join_key"] is True
    assert refs[0]["bronze_to_silver_mapping_hash"] == "sha256:silver-mapping"


def test_metadata_silver_templates_fail_closed_and_use_reviewed_keys() -> None:
    table_ref = {
        "database_name": "main",
        "schema_name": "bronze",
        "table_name": "claims",
        "bronze_table": "main.bronze.bronze_claims",
        "silver_table": "main.silver.silver_claims",
        "existing_script_path": None,
        "source_columns": [],
        "bronze_model_name": None,
    }
    columns = [
        {"column_name": "claimid", "source_column_name": "claimid", "data_type": "int", "type": "NUMBER(38,0)", "is_join_key": True},
        {"column_name": "description", "source_column_name": "description", "data_type": "string", "type": "VARCHAR", "is_join_key": False},
    ]

    databricks_code = silver_gen.generate_silver_script(
        table_ref=table_ref,
        enriched_columns=columns,
        run_id="design-run",
        strict_mapping=True,
    )
    snowflake_code = silver_gen.generate_snowflake_silver_script(
        table_ref=table_ref,
        enriched_columns=columns,
        run_id="design-run",
        silver_catalog="main",
        silver_schema="silver",
        strict_mapping=True,
    )

    assert "Missing approved mapped columns" in databricks_code
    assert "Approved Silver merge keys contain nulls" in databricks_code
    assert "Approved Silver merge keys are not unique" in databricks_code
    assert "target.`claimid` = source.`claimid`" in databricks_code
    assert "to_json(struct(" in databricks_code
    assert 'RUNTIME_CONTEXT = globals().get("ATHENA_RUNTIME_CONTEXT")' in databricks_code
    assert 'df = df.filter(col("_logical_work_id") == lit(LOGICAL_WORK_ID))' in databricks_code
    assert 'ON target."claimid" = source."claimid"' in snowflake_code
    assert "ARRAY_CONSTRUCT(\"claimid\")" in snowflake_code
    assert "APPROVED_SILVER_MERGE_KEYS_CONTAIN_NULLS" in snowflake_code
    assert "APPROVED_SILVER_MERGE_KEYS_ARE_NOT_UNIQUE" in snowflake_code
    assert 'src."_logical_work_id" = $ATHENA_LOGICAL_WORK_ID' in snowflake_code
    assert '$ATHENA_RUNTIME_RUN_ID AS "silver_run_id"' in snowflake_code


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


def test_databricks_silver_canonicalizes_uppercase_metadata_and_duplicate_reference_keys():
    table_ref = {
        "database_name": "insurance",
        "schema_name": "dbo",
        "table_name": "policy_cover_level_transactions",
        "bronze_table": "workspace.bronze.bronze_policy_cover_level_transactions",
        "silver_table": "workspace.silver.silver_policy_cover_level_transactions",
        "existing_script_path": None,
        "source_columns": [],
    }
    enriched_columns = [
        {"column_name": "COVER_NAME", "data_type": "varchar"},
        {"column_name": "RERERENCE_ID", "data_type": "bigint", "is_join_key": True},
        {"column_name": "rererence_id", "data_type": "bigint", "is_join_key": True},
    ]

    script = silver_gen.generate_silver_script(
        table_ref=table_ref,
        enriched_columns=enriched_columns,
        run_id="run-uppercase-metadata",
    )
    assignments = silver_gen._databricks_literal_assignments(script)

    assert assignments["EXPECTED_COLUMNS"] == ["cover_name", "reference_id"]
    assert assignments["KEY_COLUMNS"] == ["reference_id"]
    assert assignments["STRING_COLUMNS"] == ["cover_name"]
    assert assignments["CAST_RULES"] == {"reference_id": "bigint"}
    assert assignments["COLUMN_ALIASES"] == {"rererence_id": "reference_id"}
    assert "available_by_compact.get(compact_name(expected_name))" in script
    assert "col(actual_name).alias(expected_name)" in script

    twelve_columns = [
        {"column_name": f"COLUMN_{index}", "data_type": "varchar"}
        for index in range(11)
    ] + [{"column_name": "RERERENCE_ID", "data_type": "bigint"}]
    canonical_columns = silver_gen._canonicalize_databricks_columns(twelve_columns * 2)
    canonical_pairs = [
        (column["source_column_name"], column["column_name"])
        for column in canonical_columns
    ]

    assert len(canonical_columns) == 12
    assert len(canonical_pairs) == len(set(canonical_pairs))
    assert canonical_pairs[-1] == ("rererence_id", "reference_id")


def test_databricks_silver_rejects_uppercase_llm_column_contract_and_aliases():
    table_ref = {
        "database_name": "insurance",
        "schema_name": "dbo",
        "table_name": "policy_cover_level_transactions",
        "bronze_table": "workspace.bronze.bronze_policy_cover_level_transactions",
        "silver_table": "workspace.silver.silver_policy_cover_level_transactions",
        "existing_script_path": None,
        "source_columns": [],
    }
    enriched_columns = [
        {"column_name": "COVER_NAME", "data_type": "varchar"},
        {"column_name": "RERERENCE_ID", "data_type": "bigint", "is_join_key": True},
    ]
    safe_script = silver_gen.generate_silver_script(
        table_ref=table_ref,
        enriched_columns=enriched_columns,
        run_id="run-unsafe-llm",
    )
    uppercase_contract = (
        safe_script.replace(
            "EXPECTED_COLUMNS = ['cover_name', 'reference_id']",
            "EXPECTED_COLUMNS = ['COVER_NAME', 'RERERENCE_ID']",
        )
        .replace("KEY_COLUMNS = ['reference_id']", "KEY_COLUMNS = ['RERERENCE_ID']")
        .replace("STRING_COLUMNS = ['cover_name']", "STRING_COLUMNS = ['COVER_NAME']")
    )
    uppercase_aliases = safe_script.replace(
        "COLUMN_ALIASES = {'rererence_id': 'reference_id'}",
        "COLUMN_ALIASES = {'RERERENCE_ID': 'REFERENCE_ID'}",
    )

    with pytest.raises(ValueError, match="EXPECTED_COLUMNS"):
        silver_gen._validate_generated_silver_code(
            uppercase_contract,
            table_ref=table_ref,
            enriched_columns=enriched_columns,
            target_warehouse="databricks",
        )
    with pytest.raises(ValueError, match="COLUMN_ALIASES"):
        silver_gen._validate_generated_silver_code(
            uppercase_aliases,
            table_ref=table_ref,
            enriched_columns=enriched_columns,
            target_warehouse="databricks",
        )


@pytest.mark.parametrize(
    ("retry_is_safe", "expected_mode"),
    [(True, "LLM_RETRY"), (False, "DETERMINISTIC")],
)
def test_databricks_silver_validates_llm_retry_or_uses_deterministic_fallback(
    monkeypatch,
    retry_is_safe,
    expected_mode,
):
    table_ref = {
        "database_name": "insurance",
        "schema_name": "dbo",
        "table_name": "policy_cover_level_transactions",
        "bronze_table": "workspace.bronze.bronze_policy_cover_level_transactions",
        "silver_table": "workspace.silver.silver_policy_cover_level_transactions",
        "existing_script_path": None,
        "source_columns": [],
    }
    enriched_columns = [
        {
            "table_name": "policy_cover_level_transactions",
            "column_name": "COVER_NAME",
            "data_type": "varchar",
        },
        {
            "table_name": "policy_cover_level_transactions",
            "column_name": "RERERENCE_ID",
            "data_type": "bigint",
            "is_join_key": True,
        },
        {
            "table_name": "policy_cover_level_transactions",
            "column_name": "rererence_id",
            "data_type": "bigint",
            "is_join_key": True,
        },
    ]
    safe_script = silver_gen.generate_silver_script(
        table_ref=table_ref,
        enriched_columns=enriched_columns,
        run_id="run-llm-fallback",
    )
    unsafe_script = (
        safe_script.replace(
            "EXPECTED_COLUMNS = ['cover_name', 'reference_id']",
            "EXPECTED_COLUMNS = ['COVER_NAME', 'RERERENCE_ID']",
        )
        .replace("KEY_COLUMNS = ['reference_id']", "KEY_COLUMNS = ['RERERENCE_ID', 'REFERENCE_ID']")
        .replace("STRING_COLUMNS = ['cover_name']", "STRING_COLUMNS = ['COVER_NAME']")
        .replace(
            "COLUMN_ALIASES = {'rererence_id': 'reference_id'}",
            "COLUMN_ALIASES = {'RERERENCE_ID': 'REFERENCE_ID'}",
        )
    )
    responses = [unsafe_script, safe_script if retry_is_safe else unsafe_script]
    calls = []

    def fake_llm_generate_silver_code(**kwargs):
        calls.append(kwargs)
        return responses[len(calls) - 1]

    monkeypatch.setenv("ATHENA_SILVER_USE_LLM", "true")
    monkeypatch.setattr(silver_gen, "_llm_generate_silver_code", fake_llm_generate_silver_code)
    output_dir = Path.cwd() / ".tmp-tests" / f"silver_llm_fallback_{uuid.uuid4().hex}"
    output_dir.mkdir(parents=True)
    monkeypatch.setattr(silver_gen, "_silver_output_dir_for", lambda _: str(output_dir))

    result = silver_gen._generate_one_table(
        table_ref,
        enriched_metadata={"columns": enriched_columns},
        run_id="run-llm-fallback",
        silver_catalog="workspace",
        silver_schema="silver",
        target_warehouse="databricks",
        execution_engine="native",
    )
    persisted = Path(result["script_path"]).read_text(encoding="utf-8")
    assignments = silver_gen._databricks_literal_assignments(persisted)

    assert result["generation_mode"] == expected_mode
    assert result["column_count"] == 2
    assert result["merge_keys"] == ["reference_id"]
    assert assignments["EXPECTED_COLUMNS"] == ["cover_name", "reference_id"]
    assert assignments["KEY_COLUMNS"] == ["reference_id"]
    assert assignments["COLUMN_ALIASES"] == {"rererence_id": "reference_id"}
    assert len(calls) == 2
    assert calls[1]["validation_feedback"]


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


def test_snowflake_dbt_bronze_to_silver_dependency_and_rejected_cleanup(monkeypatch, tmp_path):
    monkeypatch.setenv("SNOWFLAKE_BRONZE_CATALOG", "ATHENA_DB")
    monkeypatch.setenv("SNOWFLAKE_BRONZE_SCHEMA", "BRONZE")
    monkeypatch.setenv("SNOWFLAKE_SILVER_CATALOG", "ATHENA_DB")
    monkeypatch.setenv("SNOWFLAKE_SILVER_SCHEMA", "SILVER")
    monkeypatch.delenv("ATHENA_SNOWFLAKE_BRONZE_TABLE_ALLOWLIST", raising=False)
    monkeypatch.setattr(silver_gen, "ai_store_db_writer", lambda **_: None)
    monkeypatch.chdir(tmp_path)

    bronze_state = {
        "run_id": "run-dbt-chain",
        "target_warehouse": "snowflake",
        "execution_engine": "dbt",
        "certified_tables": [
            {
                "database_name": "insurance",
                "schema_name": "dbo",
                "table_name": "ClaimInformation",
            }
        ],
        "discovered_metadata": {
            "tables": [
                {
                    "table_name": "ClaimInformation",
                    "columns": [{"column_name": "ClaimID", "data_type": "int"}],
                }
            ]
        },
    }
    bronze_result = bronze_gen.bronze_code_generation_node(bronze_state)

    silver_result = silver_gen.silver_code_generation_node(
        {
            **bronze_result,
            "enriched_metadata": {
                "columns": [
                    {
                        "table_name": "ClaimInformation",
                        "column_name": "claimid",
                        "data_type": "int",
                        "is_join_key": True,
                    }
                ]
            },
        }
    )
    silver_item = silver_result["silver_generation_results"][0]
    silver_model = Path(silver_item["script_path"])
    silver_sql = silver_model.read_text(encoding="utf-8")

    assert silver_model.name == "silver_claiminformation.sql"
    assert silver_item["code_generation_format"] == "dbt"
    assert silver_item["bronze_model_name"] == "bronze_claiminformation"
    assert silver_item["dbt_alias"] == "silver_ClaimInformation"
    assert "{{ ref('bronze_claiminformation') }}" in silver_sql
    assert """unique_key='"silver_upsert_key"'""" in silver_sql
    assert "MERGE INTO" not in silver_sql
    assert "CREATE TABLE" not in silver_sql

    reviewed_sql = silver_sql + "\n-- reviewed silver\n"
    reviewed = silver_gen.sync_snowflake_dbt_silver_review(
        "run-dbt-chain",
        [silver_item],
        {
            "items": [
                {
                    "entity": "ClaimInformation",
                    "review_status": "APPROVED",
                    "generated_silver_script": reviewed_sql,
                    "primary_keys": ["claimid"],
                }
            ]
        },
    )

    assert len(reviewed) == 1
    assert silver_model.read_text(encoding="utf-8") == reviewed_sql
    assert reviewed[0]["primary_keys"] == ["claimid"]
    silver_schema = Path(silver_result["snowflake_dbt_silver_schema_path"]).read_text(encoding="utf-8")
    assert "silver_claiminformation" in silver_schema
    assert "quote: true" in silver_schema

    rejected_review = silver_gen.sync_snowflake_dbt_silver_review(
        "run-dbt-chain",
        reviewed,
        {
            "items": [
                {
                    "entity": "ClaimInformation",
                    "review_status": "REJECTED",
                }
            ]
        },
    )

    assert rejected_review == []
    assert not silver_model.exists()
    assert "silver_claiminformation" not in Path(
        silver_result["snowflake_dbt_silver_schema_path"]
    ).read_text(encoding="utf-8")

    rejected = dict(bronze_result["bronze_generation_results"][0])
    rejected["status"] = "REJECTED"
    skipped = silver_gen.silver_code_generation_node(
        {
            **bronze_result,
            "bronze_generation_results": [rejected],
        }
    )

    assert skipped["silver_generation_status"] == "SKIPPED"
    assert not silver_model.exists()


def test_snowflake_dbt_silver_rejects_sanitized_model_name_collision(monkeypatch, tmp_path):
    monkeypatch.setenv("SNOWFLAKE_BRONZE_CATALOG", "ATHENA_DB")
    monkeypatch.setenv("SNOWFLAKE_BRONZE_SCHEMA", "BRONZE")
    monkeypatch.setenv("SNOWFLAKE_SILVER_CATALOG", "ATHENA_DB")
    monkeypatch.setenv("SNOWFLAKE_SILVER_SCHEMA", "SILVER")
    monkeypatch.chdir(tmp_path)

    with pytest.raises(ValueError, match="unique after sanitization"):
        silver_gen.silver_code_generation_node(
            {
                "run_id": "run-dbt-silver-collision",
                "target_warehouse": "snowflake",
                "execution_engine": "dbt",
                "bronze_generation_results": [
                    {
                        "table": "Claim-Information",
                        "status": "APPROVED",
                    },
                    {
                        "table": "Claim Information",
                        "status": "APPROVED",
                    },
                ],
            }
        )
