from nodes import bronze_gen
from sftp_nodes import bronze_code_generation
from sftp_nodes.bronze_code_generation import _resolve_databricks_source_path


def test_adls_entity_path_wins_over_shared_run_root(monkeypatch):
    monkeypatch.setenv("ADLS_ACCOUNT_URL", "https://atheastorage.dfs.core.windows.net")
    monkeypatch.setenv("ADLS_FILE_SYSTEM", "athena")

    path = _resolve_databricks_source_path(
        {
            "remote_path": "INSURANCE_SFTP/insurance/claim_information/claim_information.csv",
        },
        {
            "databricks_source_path": "abfss://athena@atheastorage.dfs.core.windows.net/INSURANCE_SFTP/insurance/",
        },
        "insurance",
        "claim_information",
        "adls_gen2",
    )

    assert path == (
        "abfss://athena@atheastorage.dfs.core.windows.net/"
        "INSURANCE_SFTP/insurance/claim_information/"
    )


def test_adls_snowflake_run_generates_reviewable_sql(monkeypatch):
    script_path = "generated_code/snowflake/bronze/bronze_claim_information.sql"
    monkeypatch.setattr(bronze_code_generation.Path, "read_text", lambda self, encoding: "SELECT 1;")
    monkeypatch.setattr(
        bronze_code_generation,
        "_approved_feeds_from_registry",
        lambda state: [{
            "feed_id": "insurance_claim_information",
            "source": "adls_gen2",
            "vendor": "insurance",
            "entity": "claim_information",
            "remote_path": "INSURANCE_SFTP/insurance/claim_information/claim_information.csv",
            "format": "csv",
            "status": "APPROVED",
        }],
    )
    monkeypatch.setattr(
        bronze_code_generation,
        "_approved_schema",
        lambda feed_id: {
            "schema_json": [{"column_name": "claim_id", "data_type": "NUMBER"}],
            "schema_fingerprint": "schema-1",
            "version": 1,
        },
    )
    monkeypatch.setattr(
        bronze_gen,
        "_generate_one_table",
        lambda *args, **kwargs: {
            "table": "claim_information",
            "database_name": "insurance",
            "schema_name": "dbo",
            "target_table": "main.bronze.bronze_claim_information",
            "source_columns": [{"source": "claim_id"}],
            "script_path": script_path,
            "target_warehouse": "snowflake",
            "script_language": "sql",
        },
    )
    monkeypatch.setattr(bronze_code_generation, "persist_bronze_execution_plan", lambda plan: None)
    monkeypatch.setattr(bronze_code_generation, "ai_store_db_writer", lambda **kwargs: None)
    monkeypatch.setattr(bronze_code_generation, "_write_bundle", lambda bundle: "generated_code/bronze/bundle.json")

    result = bronze_code_generation.sftp_bronze_code_generation_node({
        "run_id": "run-snowflake",
        "source": "adls_gen2",
        "target_warehouse": "snowflake",
    })

    plan = result["bronze_generation_results"][0]
    review = result["bronze_review_artifact"]["feeds"][0]
    assert plan["target_warehouse"] == "snowflake"
    assert plan["script_language"] == "sql"
    assert plan["artifact_path"] == script_path
    assert review["script_path"] == script_path
    assert review["table"] == "claim_information"
