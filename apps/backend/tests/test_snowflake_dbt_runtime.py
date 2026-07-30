from pathlib import Path
from types import SimpleNamespace
import uuid

import pytest

from api.models import PipelineRunRequest, ProjectRequest
from nodes import bronze_gen, silver_gen
from services import databricks_runtime, dbt_snowflake_runtime, pipeline_runtime, snowflake_bronze_runtime


def _workdir(name: str) -> Path:
    path = Path.cwd() / ".tmp-tests" / f"{name}_{uuid.uuid4().hex}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def test_snowflake_dbt_generate_only_writes_deterministic_project(monkeypatch):
    workdir = _workdir("snowflake_dbt")
    monkeypatch.setattr(dbt_snowflake_runtime, "generated_run_dir", lambda target, run_id, *parts: workdir.joinpath(target, str(run_id), *parts))
    monkeypatch.setattr(dbt_snowflake_runtime, "_write_ai_store_summary", lambda state, payload: None)
    monkeypatch.delenv("SNOWFLAKE_PASSWORD", raising=False)
    monkeypatch.delenv("SNOWFLAKE_ROLE", raising=False)
    monkeypatch.delenv("SNOWFLAKE_DBT_ROLE", raising=False)
    monkeypatch.delenv("SNOWFLAKE_DBT_DATABASE", raising=False)
    monkeypatch.delenv("SNOWFLAKE_GOLD_CATALOG", raising=False)
    monkeypatch.delenv("SNOWFLAKE_DATABASE", raising=False)

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
    assert "password:" not in profiles_yml
    assert "account:" not in profiles_yml
    assert "user:" not in profiles_yml
    assert 'role: "ATHENA_DBT_ROLE"' in profiles_yml
    assert 'database: "ATHENA_DB"' in profiles_yml
    assert 'target: "true"' in profiles_yml
    assert '    "true":' in profiles_yml
    assert rerun_state["snowflake_dbt_artifact_set_hash"] == first_hash
    assert rerun_state["snowflake_dbt_idempotency_key"] == state["snowflake_dbt_idempotency_key"]


def test_execute_finalized_dbt_project_rejects_post_review_changes(monkeypatch):
    project_dir = _workdir("snowflake_dbt_frozen")
    model_path = project_dir / "models" / "bronze" / "bronze_claims.sql"
    model_path.parent.mkdir(parents=True)
    model_path.write_text("select 1 as claim_id\n", encoding="utf-8")
    reviewed_hash = dbt_snowflake_runtime._hash_project_files(project_dir)["artifact_set_hash"]
    state = {
        "run_id": "run-frozen-dbt",
        "target_warehouse": "snowflake",
        "execution_engine": "dbt",
        "dbt_deployment_mode": "generate_and_deploy",
        "snowflake_dbt_artifact_path": str(project_dir),
        "snowflake_dbt_artifact_set_hash": reviewed_hash,
    }
    monkeypatch.setattr(dbt_snowflake_runtime, "_write_ai_store_summary", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        dbt_snowflake_runtime,
        "_execute_snowflake_dbt",
        lambda current, _project_dir: {**current, "snowflake_dbt_status": "EXECUTED"},
    )

    result = dbt_snowflake_runtime.execute_finalized_snowflake_dbt_project(state)
    assert result["snowflake_dbt_status"] == "EXECUTED"

    model_path.write_text("select 2 as claim_id\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="changed after review"):
        dbt_snowflake_runtime.execute_finalized_snowflake_dbt_project(state)


def test_snowflake_dbt_build_keeps_bronze_silver_and_gold_in_one_project(monkeypatch):
    workdir = _workdir("snowflake_dbt_combined")
    monkeypatch.setattr(
        dbt_snowflake_runtime,
        "generated_run_dir",
        lambda target, run_id, *parts: workdir.joinpath(target, str(run_id), *parts),
    )
    monkeypatch.setattr(dbt_snowflake_runtime, "_write_ai_store_summary", lambda state, payload: None)

    state = {
        "run_id": "run-combined",
        "target_warehouse": "snowflake",
        "execution_engine": "dbt",
        "dbt_deployment_mode": "generate_only",
    }
    project_dir = dbt_snowflake_runtime.write_snowflake_dbt_scaffold(state)
    dbt_snowflake_runtime.write_snowflake_dbt_sources(
        state["run_id"],
        [
            {
                "source_name": "landing",
                "database": "ATHENA_DB",
                "schema": "LANDING",
                "table_name": "claims",
                "identifier": "CLAIMS",
            }
        ],
    )
    (project_dir / "models" / "bronze" / "bronze_claims.sql").parent.mkdir(parents=True)
    (project_dir / "models" / "bronze" / "bronze_claims.sql").write_text(
        "select * from {{ source('landing', 'claims') }}\n",
        encoding="utf-8",
    )
    (project_dir / "models" / "silver" / "silver_claims.sql").parent.mkdir(parents=True)
    (project_dir / "models" / "silver" / "silver_claims.sql").write_text(
        "select * from {{ ref('bronze_claims') }}\n",
        encoding="utf-8",
    )

    result = dbt_snowflake_runtime.run_snowflake_dbt(
        {
            **state,
            "gold_generation_results": [
                {
                    "status": "APPROVED",
                    "target_table": "ATHENA_DB.GOLD.fact_claims",
                    "code_generation_format": "dbt",
                    "dbt_model_name": "fact_claims",
                    "dbt_model_sql": "select count(*) as claim_count from {{ ref('silver_claims') }}",
                }
            ],
        }
    )

    assert result["snowflake_dbt_model_count"] == 3
    assert result["snowflake_dbt_validation"]["model_count"] == 3
    assert {
        path.relative_to(project_dir).as_posix()
        for path in project_dir.glob("models/*/*.sql")
    } == {
        "models/bronze/bronze_claims.sql",
        "models/silver/silver_claims.sql",
        "models/gold/fact_claims.sql",
    }


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


def test_gold_review_matching_does_not_cross_copy_models_with_shared_source():
    scripts = [
        {
            "target_table": "INSURANCE.GOLD.fact_average_claim_payment_amount",
            "source_table": "INSURANCE.SILVER.silver_claim_payment_indemnity",
            "kpi_name": "Average Claim Payment Amount",
            "script_body": "original average",
        },
        {
            "target_table": "INSURANCE.GOLD.fact_sum_of_service_tax_paid_per_service_provider",
            "source_table": "INSURANCE.SILVER.silver_claim_payment_indemnity",
            "kpi_name": "Sum of Service Tax Paid per Service Provider",
            "script_body": "original tax",
        },
    ]
    review_artifact = {
        "items": [
            {
                **scripts[0],
                "review_status": "APPROVED",
                "script_body": "reviewed average",
            },
            {
                **scripts[1],
                "review_status": "APPROVED",
                "script_body": "reviewed tax",
            },
        ]
    }

    filtered = databricks_runtime._filtered_scripts(scripts, review_artifact, "gold")

    assert [item["script_body"] for item in filtered] == ["reviewed average", "reviewed tax"]


def test_gold_review_matching_prefers_target_over_stale_script_path():
    scripts = [
        {
            "target_table": "INSURANCE.GOLD.fact_sum_of_service_tax_paid_per_service_provider",
            "kpi_name": "Sum of Service Tax Paid per Service Provider",
            "script_path": "models/gold/gold_average_claim_payment_amount.sql",
            "script_body": "corrupted average",
        }
    ]
    review_artifact = {
        "items": [
            {
                "target_table": "INSURANCE.GOLD.fact_average_claim_payment_amount",
                "kpi_name": "Average Claim Payment Amount",
                "script_path": "models/gold/gold_average_claim_payment_amount.sql",
                "review_status": "APPROVED",
                "script_body": "reviewed average",
            },
            {
                "target_table": "INSURANCE.GOLD.fact_sum_of_service_tax_paid_per_service_provider",
                "kpi_name": "Sum of Service Tax Paid per Service Provider",
                "script_path": "models/gold/gold_sum_of_service_tax_paid_per_service_provider.sql",
                "review_status": "APPROVED",
                "script_body": "reviewed tax",
            },
        ]
    }

    filtered = databricks_runtime._filtered_scripts(scripts, review_artifact, "gold")

    assert filtered[0]["script_body"] == "reviewed tax"


def test_snowflake_dbt_rejects_model_alias_that_disagrees_with_target(monkeypatch):
    workdir = _workdir("snowflake_dbt_alias_mismatch")
    monkeypatch.setattr(
        dbt_snowflake_runtime,
        "generated_run_dir",
        lambda target, run_id, *parts: workdir.joinpath(target, str(run_id), *parts),
    )
    monkeypatch.setattr(dbt_snowflake_runtime, "_write_ai_store_summary", lambda state, payload: None)

    with pytest.raises(ValueError, match="approved target requires"):
        dbt_snowflake_runtime.run_snowflake_dbt(
            {
                "run_id": "run-alias-mismatch",
                "target_warehouse": "snowflake",
                "execution_engine": "dbt",
                "gold_generation_results": [
                    {
                        "status": "APPROVED",
                        "target_table": "INSURANCE.GOLD.fact_service_tax",
                        "dbt_model_name": "gold_service_tax",
                        "dbt_alias": "fact_service_tax",
                        "dbt_model_sql": "{{ config(alias='fact_average_claim') }}\nselect 1",
                    }
                ],
            }
        )


def test_snowflake_dbt_rejects_duplicate_physical_aliases():
    project_dir = _workdir("snowflake_dbt_duplicate_alias")
    model_dir = project_dir / "models" / "gold"
    model_dir.mkdir(parents=True)
    (model_dir / "first.sql").write_text("{{ config(alias='fact_claims') }}\nselect 1\n", encoding="utf-8")
    (model_dir / "second.sql").write_text("{{ config(alias='fact_claims') }}\nselect 2\n", encoding="utf-8")

    with pytest.raises(ValueError, match="duplicate physical targets"):
        dbt_snowflake_runtime._validate_project_dependencies(project_dir)


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


def _enable_fake_snowflake_credentials(monkeypatch):
    for key, value in {
        "SNOWFLAKE_ACCOUNT": "account",
        "SNOWFLAKE_USER": "user",
        "SNOWFLAKE_PASSWORD": "secret",
        "SNOWFLAKE_WAREHOUSE": "warehouse",
    }.items():
        monkeypatch.setenv(key, value)


def test_snowflake_cli_command_supports_python_module_launcher(monkeypatch):
    monkeypatch.setenv(
        "ATHENA_SNOWFLAKE_CLI_COMMAND",
        "python -m snowflake.cli._app.__main__",
    )
    monkeypatch.setattr(
        dbt_snowflake_runtime.shutil,
        "which",
        lambda command: f"/tools/{command}",
    )

    assert dbt_snowflake_runtime._snowflake_cli_command() == [
        "/tools/python",
        "-m",
        "snowflake.cli._app.__main__",
    ]


def test_snowflake_dbt_project_name_is_readable_and_project_scoped():
    assert dbt_snowflake_runtime.dbt_project_object_name("Vialto Project") == "VIALTO_PROJECT_DBT"
    assert dbt_snowflake_runtime._native_project_name(
        {
            "project_id": "9c1e4c41-d9de-4a3b-b44a-dbd1e2629754",
            "dbt_project_object_name": "VIALTO_PROJECT_DBT",
        }
    ) == "VIALTO_PROJECT_DBT"
    assert dbt_snowflake_runtime._native_project_name(
        {"project_id": "9c1e4c41-d9de-4a3b-b44a-dbd1e2629754"}
    ) == "ATHENA_9C1E4C41_D9DE_4A3B_B44A_DBD1E2629754"


def test_snowflake_dbt_deploys_then_builds_inside_snowflake(monkeypatch):
    workdir = _workdir("snowflake_dbt")
    monkeypatch.setattr(dbt_snowflake_runtime, "generated_run_dir", lambda target, run_id, *parts: workdir.joinpath(target, str(run_id), *parts))
    monkeypatch.setattr(dbt_snowflake_runtime, "_write_ai_store_summary", lambda state, payload: None)
    monkeypatch.setattr(dbt_snowflake_runtime.shutil, "which", lambda command: "dbt")
    _enable_fake_snowflake_credentials(monkeypatch)
    commands = []

    def fake_run(command, **kwargs):
        commands.append(command)
        return SimpleNamespace(returncode=0, stdout=f"{command[2]} ok", stderr="")

    monkeypatch.setattr(dbt_snowflake_runtime.subprocess, "run", fake_run)

    state = dbt_snowflake_runtime.run_snowflake_dbt(
        {
            "run_id": "run-2",
            "target_warehouse": "snowflake",
            "execution_engine": "dbt",
            "dbt_deployment_mode": "generate_and_deploy",
            "gold_generation_results": [{"status": "APPROVED", "target_table": "ATHENA_DB.GOLD.fact_claims"}],
        }
    )

    assert [command[2] for command in commands] == ["deploy", "execute"]
    assert "--no-force" in commands[0]
    assert "--fail-fast" in commands[1]
    assert state["snowflake_dbt_status"] == "EXECUTED"
    assert state["snowflake_dbt_deploy_status"] == "COMPLETED"
    assert state["snowflake_dbt_validation_status"] == "DBT_VALIDATED"
    assert state["completion_mode"] == "dbt_executed"
    assert state["snowflake_dbt_project_fqn"] == "ATHENA_DB.PUBLIC.ATHENA_RUN_RUN_2"


def test_snowflake_dbt_failed_receipt_blocks_automatic_retry(monkeypatch):
    workdir = _workdir("snowflake_dbt_retry")
    monkeypatch.setattr(dbt_snowflake_runtime, "generated_run_dir", lambda target, run_id, *parts: workdir.joinpath(target, str(run_id), *parts))
    monkeypatch.setattr(dbt_snowflake_runtime, "_write_ai_store_summary", lambda state, payload: None)
    monkeypatch.setattr(dbt_snowflake_runtime.shutil, "which", lambda command: "dbt")
    _enable_fake_snowflake_credentials(monkeypatch)
    calls = []

    def fail_run(command, **kwargs):
        calls.append(command)
        return SimpleNamespace(returncode=2, stdout="", stderr="warehouse failure")

    monkeypatch.setattr(dbt_snowflake_runtime.subprocess, "run", fail_run)
    state = {
        "run_id": "run-retry",
        "target_warehouse": "snowflake",
        "execution_engine": "dbt",
        "dbt_deployment_mode": "generate_and_deploy",
        "gold_generation_results": [{"status": "APPROVED", "target_table": "ATHENA_DB.GOLD.fact_claims"}],
    }

    with pytest.raises(RuntimeError, match="exit code 2"):
        dbt_snowflake_runtime.run_snowflake_dbt(state)
    with pytest.raises(RuntimeError, match="Review Snowflake state before retrying"):
        dbt_snowflake_runtime.run_snowflake_dbt(state)

    assert len(calls) == 1


def test_pipeline_request_preserves_explicit_dbt_deploy_mode():
    payload = PipelineRunRequest(
        brd_text="brd",
        source="database",
        target_warehouse="snowflake",
        execution_engine="dbt",
        dbt_deployment_mode="generate_and_deploy",
    )

    assert payload.dbt_deployment_mode == "generate_and_deploy"


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


def test_dbt_deploy_mode_lands_bronze_sources_before_continuing(monkeypatch):
    calls = []
    state = {
        "run_id": "run-dbt-landing",
        "target_warehouse": "snowflake",
        "execution_engine": "dbt",
        "dbt_deployment_mode": "generate_and_deploy",
        "bronze_generation_results": [{"table": "claims", "status": "APPROVED"}],
    }
    monkeypatch.setattr(bronze_gen, "sync_snowflake_dbt_bronze_review", lambda _run_id, results, _artifact: results)
    monkeypatch.setattr(pipeline_runtime, "ai_store_db_writer", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(pipeline_runtime, "save_checkpoint_state_timed", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(pipeline_runtime, "save_checkpoint_state", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        pipeline_runtime,
        "continue_database_pipeline",
        lambda _run_id, **kwargs: kwargs["state"],
    )
    monkeypatch.setattr(
        snowflake_bronze_runtime,
        "run_snowflake_bronze_scripts",
        lambda current, **kwargs: calls.append(kwargs) or {
            **current,
            "snowflake_bronze_source_load_status": "COMPLETED",
        },
    )

    result = pipeline_runtime.submit_gate4_review(
        state["run_id"],
        checkpoint_state=state,
        review_artifact={"feeds": []},
    )

    assert calls == [{"review_artifact": {"feeds": []}, "approved_only": True, "load_only": True}]
    assert result["snowflake_bronze_source_load_status"] == "COMPLETED"
    assert result["snowflake_bronze_execution_status"] == "SKIPPED_DBT_CODEGEN_ONLY"


def _generation_first_dbt_state():
    return {
        "run_id": "run-generation-first-dbt",
        "source": "database",
        "target_warehouse": "snowflake",
        "execution_engine": "dbt",
        "dbt_deployment_mode": "generate_and_deploy",
        "database_flow_version": "generation_first_v1",
        "gate4": {"decision": "APPROVED"},
        "bronze_review_decision": "APPROVED",
        "bronze_review_artifact": {"feeds": [{"table": "claims", "review_status": "APPROVED"}]},
        "bronze_generation_results": [{"table": "claims", "code_generation_format": "dbt"}],
        "silver_merge_key_review_decision": "APPROVED",
        "gate5": {"decision": "APPROVED"},
        "silver_review_decision": "APPROVED",
        "silver_generation_results": [{"table": "claims", "code_generation_format": "dbt"}],
        "gold_review_decision": "APPROVED",
        "gold_review_artifact": {
            "items": [
                {
                    "target_table": "ATHENA_DB.GOLD.fact_claims",
                    "review_status": "APPROVED",
                }
            ]
        },
        "gold_generation_results": [
            {
                "target_table": "ATHENA_DB.GOLD.fact_claims",
                "code_generation_format": "dbt",
            }
        ],
    }


def test_generation_first_dbt_gold_review_finalizes_then_pauses(monkeypatch):
    checkpoint = _generation_first_dbt_state()
    saved_states = []
    monkeypatch.setattr(pipeline_runtime, "load_checkpoint_state", lambda _run_id: checkpoint)
    monkeypatch.setattr(
        pipeline_runtime,
        "save_checkpoint_state",
        lambda _run_id, current: saved_states.append(current.copy()),
    )
    monkeypatch.setattr(
        pipeline_runtime,
        "finalize_snowflake_dbt_project",
        lambda current: {
            **current,
            "snowflake_dbt_status": "GENERATED",
            "snowflake_dbt_deploy_status": "PENDING",
            "snowflake_dbt_validation_status": "STATIC_VALIDATED",
            "snowflake_dbt_artifact_path": "generated/dbt/run-generation-first-dbt",
            "snowflake_dbt_artifact_set_hash": "reviewed-hash",
        },
    )
    monkeypatch.setattr(
        pipeline_runtime,
        "run_snowflake_dbt",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("legacy dbt execution must not start")),
    )
    monkeypatch.setattr(
        snowflake_bronze_runtime,
        "run_snowflake_bronze_scripts",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("source landing must wait for the gate")),
    )

    result = pipeline_runtime.submit_gold_review(
        checkpoint["run_id"],
        review_artifact=checkpoint["gold_review_artifact"],
    )

    assert result["status"] == "PAUSED_FOR_STAGE_CONFIRMATION"
    assert result["execution_ready"] is True
    assert result["next_stage_key"] == "gold_code_execution"
    assert result["next_stage_label"] == "Code Execution"
    assert result["stage_confirmation"]["last_completed_stage_key"] == "gold_review"
    assert result["snowflake_dbt_artifact_set_hash"] == "reviewed-hash"
    assert "snowflake_bronze_source_load_status" not in result
    assert saved_states[-1]["status"] == "PAUSED_FOR_STAGE_CONFIRMATION"


def test_generation_first_generate_only_dbt_finishes_without_execution_gate(monkeypatch):
    checkpoint = {
        **_generation_first_dbt_state(),
        "dbt_deployment_mode": "generate_only",
    }
    monkeypatch.setattr(pipeline_runtime, "load_checkpoint_state", lambda _run_id: checkpoint)
    monkeypatch.setattr(pipeline_runtime, "save_checkpoint_state", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        pipeline_runtime,
        "finalize_snowflake_dbt_project",
        lambda current: {
            **current,
            "snowflake_dbt_status": "GENERATED",
            "snowflake_dbt_deploy_status": "NOT_APPLICABLE_CODEGEN_ONLY",
            "snowflake_dbt_validation_status": "STATIC_VALIDATED",
            "snowflake_dbt_artifact_path": "generated/dbt/run-generation-first-dbt",
            "snowflake_dbt_artifact_set_hash": "reviewed-hash",
            "completion_mode": "codegen_only",
        },
    )

    result = pipeline_runtime.submit_gold_review(
        checkpoint["run_id"],
        review_artifact=checkpoint["gold_review_artifact"],
    )

    assert result["status"] == "PIPELINE_COMPLETED"
    assert result["execution_ready"] is False
    assert result["snowflake_gold_execution_status"] == "SKIPPED_DBT_CODEGEN_ONLY"
    assert not result.get("stage_confirmation")


def test_generation_first_dbt_gate_lands_sources_then_executes_frozen_project(monkeypatch):
    state = {
        **_generation_first_dbt_state(),
        "execution_ready": True,
        "snowflake_dbt_status": "GENERATED",
        "snowflake_dbt_deploy_status": "PENDING",
        "snowflake_dbt_validation_status": "STATIC_VALIDATED",
        "snowflake_dbt_artifact_path": "generated/dbt/run-generation-first-dbt",
        "snowflake_dbt_artifact_set_hash": "reviewed-hash",
    }
    calls = []
    saved_states = []

    monkeypatch.setattr(
        snowflake_bronze_runtime,
        "run_snowflake_bronze_scripts",
        lambda current, **kwargs: (
            calls.append(("landing", kwargs))
            or {**current, "snowflake_bronze_source_load_status": "COMPLETED"}
        ),
    )
    monkeypatch.setattr(
        pipeline_runtime,
        "execute_finalized_snowflake_dbt_project",
        lambda current: (
            calls.append(("dbt", current["snowflake_dbt_artifact_set_hash"]))
            or {
                **current,
                "snowflake_dbt_status": "EXECUTED",
                "snowflake_dbt_deploy_status": "COMPLETED",
                "completion_mode": "dbt_executed",
            }
        ),
    )
    monkeypatch.setattr(pipeline_runtime, "load_checkpoint_state", lambda _run_id: {})
    monkeypatch.setattr(
        pipeline_runtime,
        "save_checkpoint_state_timed",
        lambda _run_id, current, **_kwargs: saved_states.append(current.copy()),
    )

    result = pipeline_runtime.execute_generation_first_snowflake_dbt(
        state["run_id"],
        state=state,
    )

    assert [call[0] for call in calls] == ["landing", "dbt"]
    assert calls[0][1] == {
        "review_artifact": state["bronze_review_artifact"],
        "approved_only": True,
        "load_only": True,
        "progress_stage_key": "gold_code_execution",
    }
    assert calls[1][1] == "reviewed-hash"
    assert result["status"] == "PIPELINE_COMPLETED"
    assert result["snowflake_gold_execution_status"] == "COMPLETED"
    assert result["execution_ready"] is False
    assert result["report_generation_status"] == "COMPLETED"
    assert result["run_report"]["outcome"] == "SUCCESS"
    assert [state["background_stage"] for state in saved_states[-2:]] == ["report_generation", None]
    assert saved_states[-1]["background_stage"] is None


def test_generation_first_dbt_retry_reuses_completed_source_landing(monkeypatch):
    state = {
        **_generation_first_dbt_state(),
        "execution_ready": True,
        "snowflake_bronze_source_load_status": "COMPLETED",
        "snowflake_dbt_validation_status": "STATIC_VALIDATED",
        "snowflake_dbt_artifact_path": "generated/dbt/run-generation-first-dbt",
        "snowflake_dbt_artifact_set_hash": "reviewed-hash",
    }
    monkeypatch.setattr(
        snowflake_bronze_runtime,
        "run_snowflake_bronze_scripts",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("completed source landing must not rerun")),
    )
    monkeypatch.setattr(
        pipeline_runtime,
        "execute_finalized_snowflake_dbt_project",
        lambda current: {**current, "snowflake_dbt_status": "EXECUTED"},
    )
    monkeypatch.setattr(pipeline_runtime, "load_checkpoint_state", lambda _run_id: {})
    monkeypatch.setattr(pipeline_runtime, "save_checkpoint_state_timed", lambda *_args, **_kwargs: None)

    result = pipeline_runtime.execute_generation_first_snowflake_dbt(
        state["run_id"],
        state=state,
    )

    assert result["status"] == "PIPELINE_COMPLETED"
    assert result["snowflake_bronze_source_load_status"] == "COMPLETED"


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


def test_dbt_gold_review_reports_completed_execution(monkeypatch):
    saved_states = []
    checkpoint = {
        "run_id": "run-dbt-deploy",
        "target_warehouse": "snowflake",
        "execution_engine": "dbt",
        "dbt_deployment_mode": "generate_and_deploy",
        "status": "HITL_WAIT",
        "next_review_key": "gold_review",
        "gold_generation_results": [{"target_table": "ATHENA_DB.GOLD.fact_claims"}],
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
        lambda current: {
            **current,
            "completion_mode": "dbt_executed",
            "snowflake_dbt_status": "EXECUTED",
            "snowflake_dbt_deploy_status": "COMPLETED",
        },
    )

    result = pipeline_runtime.submit_gold_review(
        checkpoint["run_id"],
        review_artifact={"items": checkpoint["gold_generation_results"]},
    )

    assert result["status"] == "PIPELINE_COMPLETED"
    assert result["snowflake_gold_execution_status"] == "COMPLETED"
    assert result["resume_message"] == "Snowflake dbt build completed."


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
            "dbt_deployment_mode": "generate_and_deploy",
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
    assert by_key["bronze_code_execution"]["label"] == "Bronze dbt Models Staged"
    assert by_key["silver"]["label"] == "Silver dbt Model Generation"
    assert by_key["silver_code_execution"]["label"] == "Silver dbt Models Staged"
    assert by_key["gold"]["label"] == "Gold dbt Model Generation"
    assert by_key["gold_code_execution"]["label"] == "dbt Project Build & Deployment"
    assert by_key["gold_code_execution"]["state"] == "RUNNING"
    assert "validated, deployed, and built in Snowflake" in by_key["gold_code_execution"]["detail"]


def test_generation_first_dbt_pipeline_steps_put_one_deployment_after_gold_review():
    steps = pipeline_runtime.build_pipeline_steps(
        source="database",
        checkpoint={
            **_generation_first_dbt_state(),
            "status": "PAUSED_FOR_STAGE_CONFIRMATION",
            "execution_ready": True,
            "snowflake_dbt_validation_status": "STATIC_VALIDATED",
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
    keys = [step["key"] for step in steps]
    by_key = {step["key"]: step for step in steps}

    assert keys.index("gold_review") < keys.index("gold_code_execution")
    assert "bronze_code_execution" not in keys
    assert "silver_code_execution" not in keys
    assert keys[-1] == "gold_code_execution"
    assert by_key["gold_code_execution"]["label"] == "Code Execution"


def test_generation_first_dbt_pipeline_steps_add_report_for_enabled_runs():
    steps = pipeline_runtime.build_pipeline_steps(
        source="database",
        checkpoint={
            **_generation_first_dbt_state(),
            "report_generation_enabled": True,
            "report_generation_status": "RUNNING",
            "status": "RUNNING",
            "background_stage": "report_generation",
            "snowflake_gold_execution_status": "COMPLETED",
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

    assert [step["key"] for step in steps][-2:] == ["gold_code_execution", "report_generation"]
    assert steps[-2]["state"] == "COMPLETED"
    assert steps[-1]["state"] == "RUNNING"


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
