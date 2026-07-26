from __future__ import annotations

import json
from pathlib import Path
import uuid

from utilis.generated_code_paths import generated_code_dir, generated_code_root, generated_run_dir
from utilis.runtime_paths import runtime_dir


def test_generated_code_root_uses_workspace_level_dir_when_cwd_is_backend(monkeypatch):
    backend_dir = Path(__file__).resolve().parents[1]
    monkeypatch.chdir(backend_dir)
    monkeypatch.delenv("ATHENA_GENERATED_CODE_ROOT", raising=False)
    monkeypatch.delenv("ATHENA_GENERATED_CODE_DIR", raising=False)

    assert generated_code_root() == backend_dir.parents[1] / "generated_code"
    assert generated_run_dir("snowflake", "run-1", "bronze") == (
        backend_dir.parents[1] / "generated_code" / "snowflake" / "run-1" / "bronze"
    )
    assert generated_run_dir("databricks", "run-1", "bronze") == (
        backend_dir.parents[1] / "generated_code" / "databricks" / "run-1" / "bronze"
    )


def test_generated_code_root_honors_explicit_env_override(monkeypatch):
    custom_root = Path.cwd() / ".tmp-tests" / f"generated_code_override_{uuid.uuid4().hex}" / "artifacts"
    custom_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("ATHENA_GENERATED_CODE_ROOT", str(custom_root))
    monkeypatch.setenv("ATHENA_GENERATED_CODE_DIR", str(Path.cwd() / "legacy"))

    assert generated_code_root() == custom_root.resolve()
    assert generated_code_dir("gold") == custom_root.resolve() / "gold"


def test_generated_code_root_honors_legacy_env_override(monkeypatch):
    custom_root = Path.cwd() / ".tmp-tests" / f"generated_code_legacy_{uuid.uuid4().hex}" / "artifacts"
    custom_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.delenv("ATHENA_GENERATED_CODE_ROOT", raising=False)
    monkeypatch.setenv("ATHENA_GENERATED_CODE_DIR", str(custom_root))

    assert generated_code_root() == custom_root.resolve()


def test_runtime_dir_uses_app_service_data_root(monkeypatch):
    monkeypatch.delenv("ATHENA_GENERATED_CODE_ROOT", raising=False)
    monkeypatch.delenv("ATHENA_GENERATED_CODE_DIR", raising=False)
    monkeypatch.setenv("ATHENA_APP_DATA_DIR", "/home/site/custom-data")

    original_exists = Path.exists

    def fake_exists(self):
        if str(self).replace("\\", "/") == "/home/site":
            return True
        return original_exists(self)

    monkeypatch.setattr(Path, "exists", fake_exists)

    resolved = runtime_dir("ATHENA_GENERATED_CODE_DIR", Path.cwd() / "generated_code", "generated_code")
    assert str(resolved).replace("\\", "/").endswith("/home/site/custom-data/generated_code")


def test_databricks_generation_dirs_are_run_scoped(monkeypatch):
    from nodes import bronze_gen, gold_gen, silver_gen

    custom_root = Path.cwd() / ".tmp-tests" / f"databricks_run_scoped_{uuid.uuid4().hex}"
    custom_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("ATHENA_GENERATED_CODE_ROOT", str(custom_root))

    assert Path(bronze_gen._bronze_output_dir_for("databricks", "run-1")) == custom_root.resolve() / "databricks" / "run-1" / "bronze"
    assert Path(silver_gen._silver_output_dir_for("databricks", "run-1")) == custom_root.resolve() / "databricks" / "run-1" / "silver"
    assert Path(silver_gen._gold_output_dir("databricks", "run-1")) == custom_root.resolve() / "databricks" / "run-1" / "gold"
    assert Path(gold_gen._gold_output_dir_for("databricks", "run-1")) == custom_root.resolve() / "databricks" / "run-1" / "gold"


def test_databricks_run_scoped_script_names_do_not_repeat_run_id():
    from nodes import bronze_gen, gold_gen, silver_gen

    run_id = "7cad2fc2-b465-401f-889c-58744f088e4d"
    run_slug = run_id.replace("-", "_")

    names = [
        bronze_gen._bronze_script_filename(
            run_id=run_id,
            database_name="insurance",
            schema_name="dbo",
            table_name="claim_information",
            extension="py",
            include_run_id=False,
        ),
        silver_gen._silver_script_filename(
            run_id=run_id,
            table_name="claim_information",
            extension="py",
            target_warehouse="databricks",
        ),
        gold_gen._gold_script_filename(
            prefix="gold_kpi",
            run_id=run_id,
            identifier="total_claims",
            extension="py",
            target_warehouse="databricks",
        ),
        gold_gen._gold_script_filename(
            prefix="gold_dimensions",
            run_id=run_id,
            extension="py",
            target_warehouse="databricks",
        ),
    ]

    assert names[0].startswith("bronze_ingest_claim_information_")
    assert names[0].endswith(".py")
    assert names[1:] == [
        "silver_transform_claim_information.py",
        "gold_kpi_total_claims.py",
        "gold_dimensions.py",
    ]
    assert all(run_id not in name and run_slug not in name for name in names)

    assert "run_snow" in silver_gen._silver_script_filename(
        run_id="run-snow",
        table_name="claim_information",
        extension="sql",
        target_warehouse="snowflake",
    )
    assert "run_snow" in gold_gen._gold_script_filename(
        prefix="gold_kpi",
        run_id="run-snow",
        identifier="total_claims",
        extension="sql",
        target_warehouse="snowflake",
    )


def test_databricks_loader_prefers_run_scoped_bundle_and_keeps_legacy_fallback(monkeypatch):
    from services import pipeline_runtime

    custom_root = Path.cwd() / ".tmp-tests" / f"databricks_loader_{uuid.uuid4().hex}"
    run_id = "run-db-loader"
    run_dir = custom_root / "databricks" / run_id / "bronze"
    legacy_dir = custom_root / "bronze"
    run_dir.mkdir(parents=True, exist_ok=True)
    legacy_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("ATHENA_GENERATED_CODE_ROOT", str(custom_root))

    run_script = run_dir / "bronze_run.py"
    legacy_script = legacy_dir / "bronze_legacy.py"
    run_script.write_text("print('run scoped')", encoding="utf-8")
    legacy_script.write_text("print('legacy')", encoding="utf-8")
    run_bundle = {
        "run_id": run_id,
        "generated_at": "2026-07-26T00:00:00",
        "scripts": [{"run_id": run_id, "table": "claims", "script_path": str(run_script)}],
    }
    legacy_bundle = {
        "run_id": run_id,
        "generated_at": "2026-07-25T00:00:00",
        "scripts": [{"run_id": run_id, "table": "claims", "script_path": str(legacy_script)}],
    }
    (run_dir / "run_db_loader_bronze_scripts.json").write_text(json.dumps(run_bundle), encoding="utf-8")
    (legacy_dir / "bronze_scripts.json").write_text(json.dumps(legacy_bundle), encoding="utf-8")

    loaded = pipeline_runtime.load_bronze_scripts(run_id, {"run_id": run_id, "target_warehouse": "databricks"})
    assert loaded["scripts"][0]["script_body"] == "print('run scoped')"

    (run_dir / "run_db_loader_bronze_scripts.json").unlink()
    loaded = pipeline_runtime.load_bronze_scripts(run_id, {"run_id": run_id, "target_warehouse": "databricks"})
    assert loaded["scripts"][0]["script_body"] == "print('legacy')"


def test_databricks_gold_contract_path_is_run_scoped(monkeypatch):
    from nodes import silver_gen

    custom_root = Path.cwd() / ".tmp-tests" / f"databricks_contract_{uuid.uuid4().hex}"
    custom_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("ATHENA_GENERATED_CODE_ROOT", str(custom_root))

    path = Path(silver_gen._write_gold_contract({"run_id": "run-contract", "target_warehouse": "databricks"}))

    assert path == custom_root.resolve() / "databricks" / "run-contract" / "gold" / "gold_generation_contract.json"
    assert path.exists()
