from pathlib import Path
import uuid

import pytest

from api.models import PipelineRunRequest
from services import dbt_snowflake_runtime


def _workdir(name: str) -> Path:
    path = Path.cwd() / ".tmp-tests" / f"{name}_{uuid.uuid4().hex}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def test_snowflake_dbt_generate_only_writes_deterministic_project(monkeypatch):
    workdir = _workdir("snowflake_dbt")
    monkeypatch.setattr(dbt_snowflake_runtime, "generated_run_dir", lambda target, run_id, *parts: workdir.joinpath(target, str(run_id), *parts))
    monkeypatch.setattr(dbt_snowflake_runtime, "_write_ai_store_summary", lambda state, payload: None)
    monkeypatch.delenv("SNOWFLAKE_PASSWORD", raising=False)

    state = dbt_snowflake_runtime.run_snowflake_dbt(
        {
            "run_id": "run-1",
            "target_warehouse": "snowflake",
            "execution_engine": "dbt",
            "dbt_deployment_mode": "generate_only",
            "gold_generation_results": [
                {
                    "status": "APPROVED",
                    "target_table": "ATHENA_DB.GOLD.fact_total_claims",
                }
            ],
        }
    )

    project_dir = Path(state["snowflake_dbt_artifact_path"])
    model_sql = (project_dir / "models" / "gold" / "publish_fact_total_claims.sql").read_text(encoding="utf-8")
    profiles_yml = (project_dir / "profiles.yml").read_text(encoding="utf-8")
    first_hash = state["snowflake_dbt_artifact_set_hash"]

    rerun_state = dbt_snowflake_runtime.run_snowflake_dbt(state)

    assert state["snowflake_dbt_status"] == "GENERATED"
    assert state["snowflake_dbt_deploy_status"] == "NOT_APPLICABLE_CODEGEN_ONLY"
    assert state["snowflake_dbt_model_count"] == 1
    assert 'from "ATHENA_DB"."GOLD"."fact_total_claims"' in model_sql
    assert "env_var('SNOWFLAKE_PASSWORD')" in profiles_yml
    assert "password:" in profiles_yml
    assert rerun_state["snowflake_dbt_artifact_set_hash"] == first_hash


def test_snowflake_dbt_codegen_ignores_legacy_deploy_mode(monkeypatch):
    workdir = _workdir("snowflake_dbt")
    monkeypatch.setattr(dbt_snowflake_runtime, "generated_run_dir", lambda target, run_id, *parts: workdir.joinpath(target, str(run_id), *parts))
    monkeypatch.setattr(dbt_snowflake_runtime, "_write_ai_store_summary", lambda state, payload: None)
    monkeypatch.setattr(dbt_snowflake_runtime.shutil, "which", lambda command: None)

    state = dbt_snowflake_runtime.run_snowflake_dbt(
        {
            "run_id": "run-2",
            "target_warehouse": "snowflake",
            "execution_engine": "dbt",
            "dbt_deployment_mode": "generate_and_deploy",
            "gold_generation_results": [{"status": "APPROVED", "target_table": "ATHENA_DB.GOLD.fact_claims"}],
        }
    )

    assert state["snowflake_dbt_status"] == "GENERATED"
    assert state["snowflake_dbt_deploy_status"] == "NOT_APPLICABLE_CODEGEN_ONLY"


def test_pipeline_request_rejects_dbt_for_non_snowflake():
    with pytest.raises(ValueError, match="dbt code generation is only supported"):
        PipelineRunRequest(
            brd_text="brd",
            target_warehouse="databricks",
            execution_engine="dbt",
        )


def test_pipeline_request_rejects_dbt_for_file_sources():
    with pytest.raises(ValueError, match="database sources"):
        PipelineRunRequest(
            brd_text="brd",
            source="adls_gen2",
            target_warehouse="snowflake",
            execution_engine="dbt",
        )
