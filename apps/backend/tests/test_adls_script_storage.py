from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from services import adls_script_storage


@pytest.fixture
def storage_root(request):
    root = Path(__file__).resolve().parent / ".tmp-adls-script-storage" / request.node.name
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True)
    try:
        yield root
    finally:
        if root.exists():
            shutil.rmtree(root)


def test_databricks_run_writes_only_generated_script_files(monkeypatch, storage_root: Path):
    tmp_path = storage_root
    code_root = tmp_path / "astra-data-app-code"
    monkeypatch.setenv("ATHENA_ADLS_CODE_ROOT", str(code_root))
    state = {
        "run_id": "run-123",
        "target_warehouse": "databricks",
        "status": "HITL_WAIT",
        "metadata_ddl_review": {
            "file_name": "metadata_schema.sql",
            "script_body": "create schema metadata_schema;\n",
        },
        "bronze_generation_results": [
            {"script_path": "claims_bronze.py", "script_body": "print('bronze')\n"}
        ],
        "silver_generation_results": [
            {"script_path": "claims_silver.py", "script_body": "print('silver')\n"}
        ],
        "gold_generation_results": [
            {"script_path": "claims_gold.py", "script_body": "print('gold')\n"}
        ],
    }

    for layer in ("metadata", "bronze", "silver", "gold"):
        adls_script_storage.persist_generated_scripts("run-123", state, layer)

    run_root = code_root / "databricks" / "run-123"
    assert (run_root / "metadata" / "metadata_schema.sql").read_text(encoding="utf-8") == "create schema metadata_schema;\n"
    assert (run_root / "bronze" / "claims_bronze.py").is_file()
    assert (run_root / "silver" / "claims_silver.py").is_file()
    assert (run_root / "gold" / "claims_gold.py").is_file()
    assert not list(code_root.rglob("*.json"))
    assert not (tmp_path / "run-history").exists()


def test_snowflake_native_uses_run_scoped_sql_folders(monkeypatch, storage_root: Path):
    tmp_path = storage_root
    code_root = tmp_path / "astra-data-app-code"
    monkeypatch.setenv("ATHENA_ADLS_CODE_ROOT", str(code_root))
    state = {
        "run_id": "run-native",
        "target_warehouse": "snowflake",
        "execution_engine": "native",
        "bronze_generation_results": [
            {"target_table": "ATHENA_DB.BRONZE.CLAIMS", "script_body": "create table claims as select 1;\n"}
        ],
    }

    adls_script_storage.persist_generated_scripts("run-native", state, "bronze")

    path = code_root / "snowflake" / "native" / "run-native" / "bronze" / "ATHENA_DB.BRONZE.CLAIMS.sql"
    assert path.read_text(encoding="utf-8").startswith("create table")


def test_snowflake_dbt_models_are_stored_under_layer(monkeypatch, storage_root: Path):
    tmp_path = storage_root
    code_root = tmp_path / "astra-data-app-code"
    monkeypatch.setenv("ATHENA_ADLS_CODE_ROOT", str(code_root))
    state = {
        "run_id": "run-dbt",
        "target_warehouse": "snowflake",
        "execution_engine": "dbt",
        "silver_generation_results": [
            {"script_path": "models/silver/stg_claims.sql", "script_body": "select * from bronze.claims\n"}
        ],
    }

    adls_script_storage.persist_generated_scripts("run-dbt", state, "silver")

    assert (code_root / "snowflake" / "dbt" / "run-dbt" / "models" / "silver" / "stg_claims.sql").is_file()


def test_loader_returns_the_persisted_file_body(monkeypatch, storage_root: Path):
    tmp_path = storage_root
    code_root = tmp_path / "astra-data-app-code"
    monkeypatch.setenv("ATHENA_ADLS_CODE_ROOT", str(code_root))
    persisted = {
        "run_id": "run-view",
        "target_warehouse": "databricks",
        "bronze_generation_results": [
            {"script_path": "claims.py", "script_body": "print('persisted')\n"}
        ],
    }
    adls_script_storage.persist_generated_scripts("run-view", persisted, "bronze")
    checkpoint = {
        **persisted,
        "bronze_generation_results": [
            {"script_path": "claims.py", "script_body": "print('checkpoint')\n"}
        ],
    }

    bundle = adls_script_storage.load_generated_scripts("run-view", checkpoint, "bronze")

    assert bundle["scripts"][0]["script_body"] == "print('persisted')\n"
    assert bundle["scripts"][0]["uri"].endswith("/databricks/run-view/bronze/claims.py")


def test_unconfigured_storage_is_a_noop(monkeypatch, storage_root: Path):
    tmp_path = storage_root
    monkeypatch.delenv("ATHENA_ADLS_CODE_ROOT", raising=False)

    adls_script_storage.persist_generated_scripts(
        "run-off",
        {"run_id": "run-off", "bronze_generation_results": [{"script_body": "print(1)"}]},
        "bronze",
    )

    assert list(tmp_path.iterdir()) == []
