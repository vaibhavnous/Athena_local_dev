from pathlib import Path
import uuid

import pytest

from api.models import PipelineRunRequest, ProjectRequest
from nodes import bronze_gen, silver_gen
from services import dbt_snowflake_runtime, pipeline_runtime


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
            "dbt_target_name": "TRUE",
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

    rerun_state = dbt_snowflake_runtime.run_snowflake_dbt({**state, "dbt_target_name": "true"})

    assert state["snowflake_dbt_status"] == "GENERATED"
    assert state["snowflake_dbt_deploy_status"] == "NOT_APPLICABLE_CODEGEN_ONLY"
    assert state["snowflake_dbt_validation_status"] == "STATIC_VALIDATED"
    assert state["snowflake_dbt_validation"]["validation_type"] == "static_dependencies"
    assert state["completion_mode"] == "codegen_only"
    assert state["snowflake_dbt_model_count"] == 1
    assert 'from "ATHENA_DB"."GOLD"."fact_total_claims"' in model_sql
    assert "env_var('SNOWFLAKE_PASSWORD')" in profiles_yml
    assert "password:" in profiles_yml
    assert 'target: "true"' in profiles_yml
    assert '    "true":' in profiles_yml
    assert rerun_state["snowflake_dbt_artifact_set_hash"] == first_hash
    assert rerun_state["snowflake_dbt_idempotency_key"] == state["snowflake_dbt_idempotency_key"]


def test_snowflake_dbt_preserves_physical_alias(monkeypatch):
    workdir = _workdir("snowflake_dbt_alias")
    monkeypatch.setattr(dbt_snowflake_runtime, "generated_run_dir", lambda target, run_id, *parts: workdir.joinpath(target, str(run_id), *parts))
    monkeypatch.setattr(dbt_snowflake_runtime, "_write_ai_store_summary", lambda state, payload: None)

    state = dbt_snowflake_runtime.run_snowflake_dbt(
        {
            "run_id": "run-alias",
            "target_warehouse": "snowflake",
            "execution_engine": "dbt",
            "gold_generation_results": [
                {
                    "status": "APPROVED",
                    "target_table": 'ATHENA_DB.GOLD."Fact Total Claims"',
                    "dbt_model_name": "fact_total_claims",
                    "dbt_alias": "Fact Total Claims",
                    "dbt_model_sql": "select 1 as claim_count",
                }
            ],
        }
    )

    project_dir = Path(state["snowflake_dbt_artifact_path"])
    schema_yml = (project_dir / "models" / "gold" / "schema.yml").read_text(encoding="utf-8")

    assert state["snowflake_dbt_models"][0]["alias"] == "Fact Total Claims"
    assert 'alias: "Fact Total Claims"' in schema_yml


def test_snowflake_dbt_gold_output_prefers_reviewed_script_body():
    outputs = dbt_snowflake_runtime._gold_outputs(
        {
            "gold_generation_results": [
                {
                    "target_table": "ATHENA_DB.GOLD.fact_total_claims",
                    "code_generation_format": "dbt",
                    "dbt_model_sql": "select -1 as stale_value",
                    "script_body": "select 42 as reviewed_value",
                }
            ]
        }
    )

    assert outputs[0]["model_sql"] == "select 42 as reviewed_value"


def test_snowflake_dbt_validation_rejects_unresolved_ref():
    project_dir = _workdir("snowflake_dbt_invalid_ref")
    model_dir = project_dir / "models" / "gold"
    model_dir.mkdir(parents=True)
    (model_dir / "fact_claims.sql").write_text(
        "select * from {{ ref('missing_silver_claims') }}\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"unresolved ref\(\) targets: missing_silver_claims"):
        dbt_snowflake_runtime._validate_project_dependencies(project_dir)


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


def test_dbt_target_name_respects_database_column_limit():
    oversized_target_name = "x" * 81

    with pytest.raises(ValueError, match="at most 80 characters"):
        PipelineRunRequest(dbt_target_name=oversized_target_name)

    with pytest.raises(ValueError, match="at most 80 characters"):
        ProjectRequest(
            name="project",
            description="description",
            connection_type="database",
            dbt_target_name=oversized_target_name,
        )


def test_dbt_reviews_skip_native_snowflake_execution(monkeypatch):
    saved_states = []
    continued_states = []
    reconciled_layers = []
    state = {
        "run_id": "run-dbt-reviews",
        "target_warehouse": "snowflake",
        "execution_engine": "dbt",
        "bronze_generation_results": [],
        "silver_generation_results": [],
    }

    monkeypatch.setattr(pipeline_runtime, "_pause_for_silver_merge_key_review", lambda _run_id, current: current)
    monkeypatch.setattr(pipeline_runtime, "ai_store_db_writer", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        bronze_gen,
        "sync_snowflake_dbt_bronze_review",
        lambda _run_id, results, _artifact: reconciled_layers.append("bronze") or results,
    )
    monkeypatch.setattr(
        silver_gen,
        "sync_snowflake_dbt_silver_review",
        lambda _run_id, results, _artifact: reconciled_layers.append("silver") or results,
    )
    monkeypatch.setattr(
        pipeline_runtime,
        "save_checkpoint_state_timed",
        lambda _run_id, current, **kwargs: saved_states.append(current.copy()),
    )
    monkeypatch.setattr(
        pipeline_runtime,
        "save_checkpoint_state",
        lambda _run_id, current: saved_states.append(current.copy()),
    )
    monkeypatch.setattr(
        pipeline_runtime,
        "continue_database_pipeline",
        lambda _run_id, **kwargs: continued_states.append(kwargs["state"].copy()) or kwargs["state"],
    )

    bronze_result = pipeline_runtime.submit_gate4_review(
        state["run_id"],
        checkpoint_state=state,
        review_artifact={"feeds": []},
    )
    monkeypatch.setattr(pipeline_runtime, "load_checkpoint_state", lambda _run_id: bronze_result)
    silver_result = pipeline_runtime.submit_gate5_review(
        state["run_id"],
        review_artifact={"items": []},
    )

    assert bronze_result["snowflake_bronze_execution_status"] == "SKIPPED_DBT_CODEGEN_ONLY"
    assert silver_result["snowflake_silver_execution_status"] == "SKIPPED_DBT_CODEGEN_ONLY"
    assert len(continued_states) == 2
    assert all(current.get("background_stage") is None for current in continued_states)
    assert reconciled_layers == ["bronze", "silver"]


def test_dbt_gold_review_finalizes_generation_without_native_execution(monkeypatch):
    saved_states = []
    generated_states = []
    checkpoint = {
        "run_id": "run-dbt-gold",
        "target_warehouse": "snowflake",
        "execution_engine": "dbt",
        "status": "HITL_WAIT",
        "next_review_key": "gold_review",
        "gold_generation_results": [
            {
                "target_table": "ATHENA_DB.GOLD.fact_total_claims",
                "script_body": "{{ ref('silver_claims') }}",
                "code_generation_format": "dbt",
            }
        ],
    }

    monkeypatch.setattr(pipeline_runtime, "load_checkpoint_state", lambda _run_id: checkpoint)
    monkeypatch.setattr(
        pipeline_runtime,
        "save_checkpoint_state",
        lambda _run_id, current: saved_states.append(current.copy()),
    )

    def fake_generate(current):
        generated_states.append(current.copy())
        return {
            **current,
            "snowflake_dbt_status": "GENERATED",
            "snowflake_dbt_deploy_status": "NOT_APPLICABLE_CODEGEN_ONLY",
        }

    monkeypatch.setattr(pipeline_runtime, "run_snowflake_dbt", fake_generate)

    result = pipeline_runtime.submit_gold_review(
        checkpoint["run_id"],
        review_artifact={"items": checkpoint["gold_generation_results"]},
    )

    assert generated_states[0]["background_stage"] == "snowflake_dbt_codegen"
    assert result["status"] == "PIPELINE_COMPLETED"
    assert result["snowflake_gold_execution_status"] == "SKIPPED_DBT_CODEGEN_ONLY"
    assert result["snowflake_dbt_status"] == "GENERATED"
    assert result["snowflake_dbt_deploy_status"] == "NOT_APPLICABLE_CODEGEN_ONLY"
    assert saved_states[-1]["status"] == "PIPELINE_COMPLETED"


def test_dbt_gold_review_clears_stale_validation_status_on_failure(monkeypatch):
    saved_states = []
    checkpoint = {
        "run_id": "run-dbt-validation-failure",
        "target_warehouse": "snowflake",
        "execution_engine": "dbt",
        "status": "HITL_WAIT",
        "snowflake_dbt_validation_status": "VALIDATED",
        "gold_generation_results": [
            {
                "target_table": "ATHENA_DB.GOLD.fact_claims",
                "dbt_model_name": "fact_claims",
                "dbt_model_sql": "{{ ref('missing_silver_model') }}",
                "code_generation_format": "dbt",
            }
        ],
    }

    monkeypatch.setattr(pipeline_runtime, "load_checkpoint_state", lambda _run_id: checkpoint)
    monkeypatch.setattr(
        pipeline_runtime,
        "save_checkpoint_state",
        lambda _run_id, current: saved_states.append(current.copy()),
    )
    monkeypatch.setattr(
        pipeline_runtime,
        "run_snowflake_dbt",
        lambda _state: (_ for _ in ()).throw(ValueError("unresolved dbt ref")),
    )

    with pytest.raises(ValueError, match="unresolved dbt ref"):
        pipeline_runtime.submit_gold_review(
            checkpoint["run_id"],
            review_artifact={"items": checkpoint["gold_generation_results"]},
        )

    assert saved_states[0]["snowflake_dbt_validation_status"] == "RUNNING"
    assert saved_states[-1]["status"] == "FAILED"
    assert saved_states[-1]["snowflake_dbt_validation_status"] == "FAILED"


def test_dbt_gold_review_emits_only_approved_edited_model(monkeypatch):
    workdir = _workdir("snowflake_dbt_review")
    checkpoint = {
        "run_id": "run-dbt-reviewed-model",
        "target_warehouse": "snowflake",
        "execution_engine": "dbt",
        "status": "HITL_WAIT",
        "next_review_key": "gold_review",
        "gold_generation_results": [
            {
                "kpi_name": "Keep KPI",
                "target_table": "ATHENA_DB.GOLD.fact_keep",
                "dbt_model_name": "fact_keep",
                "dbt_alias": "fact_keep",
                "dbt_model_sql": "select -1 as stale_value",
                "code_generation_format": "dbt",
            },
            {
                "target_table": "ATHENA_DB.GOLD.fact_drop",
                "dbt_model_name": "fact_drop",
                "dbt_model_sql": "select 0 as dropped_value",
                "code_generation_format": "dbt",
            },
        ],
    }
    review_artifact = {
        "items": [
            {
                "target_table": "ATHENA_DB.GOLD.fact_drop",
                "review_status": "REJECTED",
            },
            {
                "kpi_name": "Keep KPI",
                "target_table": "ATTACKER_DB.PUBLIC.hijacked",
                "review_status": "APPROVED",
                "script_body": "select 42 as reviewed_value",
                "code_generation_format": "native",
                "dbt_model_name": "hijacked",
                "dbt_alias": "hijacked",
                "execution_engine": "native",
            },
        ]
    }

    monkeypatch.setattr(
        dbt_snowflake_runtime,
        "generated_run_dir",
        lambda target, run_id, *parts: workdir.joinpath(target, str(run_id), *parts),
    )
    monkeypatch.setattr(dbt_snowflake_runtime, "_write_ai_store_summary", lambda state, payload: None)
    monkeypatch.setattr(pipeline_runtime, "load_checkpoint_state", lambda _run_id: checkpoint)
    monkeypatch.setattr(pipeline_runtime, "save_checkpoint_state", lambda _run_id, current: None)
    monkeypatch.setattr(pipeline_runtime, "run_snowflake_dbt", dbt_snowflake_runtime.run_snowflake_dbt)

    result = pipeline_runtime.submit_gold_review(
        checkpoint["run_id"],
        review_artifact=review_artifact,
    )

    project_dir = Path(result["snowflake_dbt_artifact_path"])
    assert (project_dir / "models" / "gold" / "fact_keep.sql").read_text(encoding="utf-8").strip() == (
        "select 42 as reviewed_value"
    )
    assert not (project_dir / "models" / "gold" / "fact_drop.sql").exists()
    assert [item["target_table"] for item in result["gold_generation_results"]] == [
        "ATHENA_DB.GOLD.fact_keep"
    ]
    assert result["gold_generation_results"][0]["code_generation_format"] == "dbt"
    assert result["gold_generation_results"][0]["dbt_model_name"] == "fact_keep"
    assert result["gold_generation_results"][0]["dbt_alias"] == "fact_keep"
    assert "execution_engine" not in result["gold_generation_results"][0]


def test_dbt_pipeline_steps_show_codegen_and_validation_state():
    steps = pipeline_runtime.build_pipeline_steps(
        source="database",
        checkpoint={
            "target_warehouse": "snowflake",
            "execution_engine": "dbt",
            "status": "RUNNING",
            "background_stage": "snowflake_dbt_codegen",
        },
        summary=[],
        pending_gate1=[],
        completed_gate1=[],
        nominated_tables=[],
        certified_tables=[],
        enriched_payload={},
        gate3_payload={},
        bronze_generation_completed=True,
        silver_generation_completed=True,
        gold_generation_completed=True,
    )
    by_key = {step["key"]: step for step in steps}

    assert by_key["bronze"]["label"] == "Bronze dbt Model Generation"
    assert by_key["bronze_code_execution"]["label"] == "Bronze dbt Models Ready"
    assert by_key["silver"]["label"] == "Silver dbt Model Generation"
    assert by_key["silver_code_execution"]["label"] == "Silver dbt Models Ready"
    assert by_key["gold"]["label"] == "Gold dbt Model Generation"
    assert by_key["gold_code_execution"]["label"] == "dbt Static Dependency Check"
    assert by_key["gold_code_execution"]["state"] == "RUNNING"
    assert "dbt parse/build and Snowflake execution are outside Astra" in by_key["gold_code_execution"]["detail"]


def test_dbt_gold_bundle_does_not_expose_native_dimension_companion():
    bundle = pipeline_runtime._scripts_from_checkpoint(
        {
            "run_id": "run-dbt-bundle",
            "gold_generation_results": [
                {
                    "target_table": "ATHENA_DB.GOLD.fact_total_claims",
                    "script_body": "{{ ref('silver_claims') }}",
                    "dimension_script_body": "select * from dimensions",
                    "dimension_script_path": "dimension.sql",
                    "code_generation_format": "dbt",
                }
            ],
        },
        "gold_generation_results",
        "gold",
    )

    assert len(bundle["scripts"]) == 1
    assert "dimension_script_body" not in bundle["scripts"][0]
    assert "dimension_script_path" not in bundle["scripts"][0]
