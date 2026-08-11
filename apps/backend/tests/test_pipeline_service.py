from __future__ import annotations

from concurrent.futures import Future
from contextlib import nullcontext
import json
from pathlib import Path
import uuid

import pytest
from fastapi import HTTPException

from api.models import PipelineRunRequest
from api.services import pipeline_service
from services import pipeline_runtime


def test_metadata_setup_is_the_first_target_access_and_deploys_approved_snapshot(monkeypatch):
    from types import SimpleNamespace
    from services import metadata_contracts, metadata_repository, metadata_selection
    from utilis import generated_code_paths

    calls = []

    class TargetRepository:
        def execute(self, sql):
            calls.append(("ddl", sql))

        def preflight(self):
            calls.append(("preflight", None))

        def unit_of_work(self):
            return nullcontext(self)

        def upsert_source_system(self, payload):
            calls.append(("source", payload["source_system_id"]))

        def upsert_connection_draft(self, payload):
            calls.append(("connection", payload["connection_id"]))
            return payload

        def validate_and_activate_connection(self, connection_id, config_version, validator):
            calls.append(("activate_connection", (connection_id, config_version)))

        def deploy_configuration_snapshot(self, *, ingestion_objects, mappings):
            calls.append(("deploy", (len(ingestion_objects), len(mappings))))

    class DesignRepository:
        def get_active_ingestion_objects(self, object_ids):
            return {object_id: {"ingestion_object_id": object_id} for object_id in object_ids}

        def table(self, name):
            return name

        def query(self, _sql, _parameters):
            return [{"mapping_id": 1, "ingestion_object_id": 101}]

    artifact_path = Path(__file__)
    monkeypatch.setattr(metadata_repository, "metadata_repository_for_target", lambda **_: TargetRepository())
    monkeypatch.setattr(
        metadata_selection,
        "validated_metadata_selection",
        lambda _state: SimpleNamespace(
            repository=DesignRepository(),
            source_system={"source_system_id": 7},
            connection={"connection_id": 11, "config_version": 1},
        ),
    )
    monkeypatch.setattr(generated_code_paths, "resolve_generated_artifact_uri", lambda _uri: artifact_path)
    monkeypatch.setattr(metadata_contracts, "file_sha256", lambda _path: "artifact-hash")
    monkeypatch.setattr(metadata_contracts, "split_sql_statements", lambda _sql: ["CREATE SCHEMA", "CREATE TABLE"])
    monkeypatch.setattr(pipeline_runtime, "save_checkpoint_state_timed", lambda *_args, **_kwargs: None)

    result = pipeline_runtime._execute_metadata_setup({
        "run_id": "design-run",
        "target_warehouse": "databricks",
        "target_environment": "qa",
        "metadata_ddl_artifact": {
            "artifact_uri": "generated-code://metadata.sql",
            "artifact_hash": "artifact-hash",
        },
        "bronze_generation_results": [{
            "ingestion_object_id": 101,
            "metadata_activation_status": "ACTIVE",
        }],
    })

    assert [name for name, _ in calls[:3]] == ["ddl", "ddl", "preflight"]
    assert calls[-1] == ("deploy", (1, 1))
    assert result["metadata_setup_execution_status"] == "COMPLETED"


def test_pipeline_request_preserves_bigint_metadata_ids_from_json_strings():
    payload = PipelineRunRequest(
        brd_text="claims requirements",
        target_warehouse="databricks",
        target_environment="qa",
        source_system_id="7499026347042686646",
        source_connection_id="3358264270364792816",
    )

    assert payload.source_system_id == 7499026347042686646
    assert payload.source_connection_id == 3358264270364792816


def test_resume_payload_preserves_the_application_source_profile():
    payload = pipeline_service.seed_payload_from_checkpoint({
        "brd_text": "claims requirements",
        "source_system_id": 7499026347042686646,
        "source_connection_id": 3358264270364792816,
        "source_profile": "insurance_azure_sql",
        "target_warehouse": "databricks",
        "target_environment": "qa",
    })

    assert payload.source_profile == "insurance_azure_sql"


def test_validate_pipeline_result_rejects_non_dict():
    with pytest.raises(ValueError, match="invalid response object"):
        pipeline_service._validate_pipeline_result("bad")


def test_validate_pipeline_result_rejects_missing_state():
    with pytest.raises(ValueError, match="missing a valid result state"):
        pipeline_service._validate_pipeline_result({"result": "bad"})


def test_next_status_preserves_terminal_and_pause_states():
    assert pipeline_service._next_status("FAILED", pending_gate1=False, file_source=False) == "FAILED"
    assert pipeline_service._next_status("HITL_WAIT", pending_gate1=False, file_source=False) == "HITL_WAIT"
    assert pipeline_service._next_status("RUNNING", pending_gate1=True, file_source=False) == "RUNNING"


def test_next_status_derives_database_and_file_source_defaults():
    assert pipeline_service._next_status(None, pending_gate1=True, file_source=False) == "HITL_WAIT"
    assert pipeline_service._next_status(None, pending_gate1=False, file_source=True) == "COMPLETED"
    assert pipeline_service._next_status("done", pending_gate1=False, file_source=True) == "done"


def test_revised_flow_version_preserves_generation_first_v1_compatibility():
    base = {"source": "database", "target_warehouse": "databricks"}

    assert pipeline_runtime.generation_first_database_flow(
        {**base, "database_flow_version": "generation_first_v1"}
    )
    assert not pipeline_runtime.revised_metadata_database_flow(
        {**base, "database_flow_version": "generation_first_v1"}
    )
    assert pipeline_runtime.revised_metadata_database_flow(
        {**base, "database_flow_version": "generation_first_v2"}
    )


def _generation_first_state(target: str = "databricks"):
    return {
        "run_id": "run-generation-first",
        "source": "database",
        "target_warehouse": target,
        "execution_engine": "native",
        "database_flow_version": "generation_first_v1",
        "bronze_generation_status": "COMPLETED",
        "bronze_generation_results": [{"table": "claims", "script_body": "bronze"}],
        "bronze_review_artifact": {"feeds": [{"table": "claims", "review_status": "APPROVED"}]},
        "bronze_review_decision": "APPROVED",
        "gate4": {"decision": "APPROVED"},
        "silver_merge_key_review_decision": "APPROVED",
        "silver_generation_status": "COMPLETED",
        "silver_generation_results": [{"table": "claims", "script_body": "silver"}],
        "silver_review_artifact": {"items": [{"table": "claims", "review_status": "APPROVED"}]},
        "silver_review_decision": "APPROVED",
        "gate5": {"decision": "APPROVED"},
        "gold_generation_status": "COMPLETED",
        "gold_generation_results": [
            {
                "kpi_name": "Total Claims",
                "target_table": "main.gold.total_claims",
                "script_body": "gold",
            }
        ],
        "gold_review_artifact": {
            "items": [
                {
                    "kpi_name": "Total Claims",
                    "target_table": "main.gold.total_claims",
                    "review_status": "APPROVED",
                }
            ]
        },
        "gold_review_decision": "APPROVED",
    }


def test_generation_first_gold_review_pauses_at_start_execution(monkeypatch):
    state = {
        **_generation_first_state(),
        "failed_background_stage": "gold_review",
        "error": "stale failure",
    }
    saved = []
    monkeypatch.setattr(pipeline_runtime, "load_checkpoint_state", lambda _run_id: state)
    monkeypatch.setattr(
        pipeline_runtime,
        "save_checkpoint_state",
        lambda _run_id, checkpoint: saved.append(dict(checkpoint)),
    )

    result = pipeline_runtime.submit_gold_review(
        state["run_id"],
        "APPROVED",
        state["gold_review_artifact"],
    )

    assert result["status"] == "PAUSED_FOR_STAGE_CONFIRMATION"
    assert result["last_completed_stage_key"] == "gold_review"
    assert result["next_stage_key"] == "bronze_code_execution"
    assert result["execution_ready"] is True
    assert result["stage_confirmation"]["next_stage_label"] == "Metadata Setup Execution"
    assert result["failed_background_stage"] is None
    assert result["error"] is None
    assert not result.get("databricks_bronze_execution_status")
    assert saved[-1]["gold_generation_results"][0]["script_body"] == "gold"
    assert saved[-1]["gold_generation_results"][0]["review_status"] == "APPROVED"


def test_generation_first_execution_runs_layers_in_order_and_preserves_artifacts(monkeypatch):
    from services import databricks_runtime

    state = {**_generation_first_state(), "execution_ready": True}
    calls = []
    saved = []

    def runner(layer):
        def execute(checkpoint, **_kwargs):
            calls.append(layer)
            return {**checkpoint, f"databricks_{layer}_execution_status": "COMPLETED"}

        return execute

    monkeypatch.setattr(databricks_runtime, "run_databricks_bronze_scripts", runner("bronze"))
    monkeypatch.setattr(databricks_runtime, "run_databricks_silver_scripts", runner("silver"))
    monkeypatch.setattr(databricks_runtime, "run_databricks_gold_scripts", runner("gold"))
    monkeypatch.setattr(
        pipeline_runtime,
        "save_checkpoint_state_timed",
        lambda _run_id, checkpoint, **_kwargs: saved.append(dict(checkpoint)),
    )

    result = pipeline_runtime.execute_database_native_layers(state["run_id"], state=state)

    assert calls == ["bronze", "silver", "gold"]
    assert result["status"] == "PIPELINE_COMPLETED"
    assert result["execution_ready"] is False
    assert result["bronze_generation_results"] == state["bronze_generation_results"]
    assert result["silver_generation_results"] == state["silver_generation_results"]
    assert result["gold_generation_results"] == state["gold_generation_results"]
    assert [
        checkpoint["background_stage"]
        for checkpoint in saved
        if checkpoint.get("background_stage")
    ] == ["bronze_code_execution", "silver_code_execution", "gold_code_execution"]


def test_generation_first_retry_skips_completed_execution_layers(monkeypatch):
    from services import databricks_runtime

    state = {
        **_generation_first_state(),
        "execution_ready": True,
        "databricks_bronze_execution_status": "COMPLETED",
    }
    calls = []

    monkeypatch.setattr(
        databricks_runtime,
        "run_databricks_silver_scripts",
        lambda checkpoint, **_kwargs: (
            calls.append("silver")
            or {**checkpoint, "databricks_silver_execution_status": "COMPLETED"}
        ),
    )
    monkeypatch.setattr(
        databricks_runtime,
        "run_databricks_gold_scripts",
        lambda checkpoint, **_kwargs: (
            calls.append("gold")
            or {**checkpoint, "databricks_gold_execution_status": "COMPLETED"}
        ),
    )
    monkeypatch.setattr(
        databricks_runtime,
        "run_databricks_bronze_scripts",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("Bronze must not rerun")),
    )
    monkeypatch.setattr(pipeline_runtime, "save_checkpoint_state_timed", lambda *_args, **_kwargs: None)

    result = pipeline_runtime.execute_database_native_layers(
        state["run_id"],
        state=state,
        start_stage_key="bronze_code_execution",
    )

    assert calls == ["silver", "gold"]
    assert result["status"] == "PIPELINE_COMPLETED"
    assert pipeline_service.database_failed_stage_key(
        state["run_id"],
        {**state, "status": "FAILED", "failed_background_stage": "silver_code_execution"},
    ) == "silver_code_execution"


def test_generation_first_snowflake_native_uses_same_execution_order(monkeypatch):
    from services import (
        snowflake_bronze_runtime,
        snowflake_gold_runtime,
        snowflake_silver_runtime,
    )

    state = {**_generation_first_state("snowflake"), "execution_ready": True}
    calls = []

    monkeypatch.setattr(
        snowflake_bronze_runtime,
        "run_snowflake_bronze_scripts",
        lambda checkpoint, **_kwargs: (
            calls.append("bronze")
            or {**checkpoint, "snowflake_bronze_execution_status": "COMPLETED"}
        ),
    )
    monkeypatch.setattr(
        snowflake_silver_runtime,
        "run_snowflake_silver_scripts",
        lambda checkpoint, **_kwargs: (
            calls.append("silver")
            or {**checkpoint, "snowflake_silver_execution_status": "COMPLETED"}
        ),
    )
    monkeypatch.setattr(
        snowflake_gold_runtime,
        "run_snowflake_gold_scripts",
        lambda checkpoint: (
            calls.append("gold")
            or {**checkpoint, "snowflake_gold_execution_status": "COMPLETED"}
        ),
    )
    monkeypatch.setattr(pipeline_runtime, "save_checkpoint_state_timed", lambda *_args, **_kwargs: None)

    result = pipeline_runtime.execute_database_native_layers(state["run_id"], state=state)

    assert calls == ["bronze", "silver", "gold"]
    assert result["status"] == "PIPELINE_COMPLETED"


def test_generation_first_execution_is_fail_fast(monkeypatch):
    from services import databricks_runtime

    state = {**_generation_first_state(), "execution_ready": True}
    monkeypatch.setattr(
        databricks_runtime,
        "run_databricks_bronze_scripts",
        lambda checkpoint, **_kwargs: {
            **checkpoint,
            "databricks_bronze_execution_status": "FAILED",
        },
    )
    monkeypatch.setattr(
        databricks_runtime,
        "run_databricks_silver_scripts",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("Silver must not start")),
    )
    monkeypatch.setattr(pipeline_runtime, "load_checkpoint_state", lambda _run_id: state)
    monkeypatch.setattr(pipeline_runtime, "save_checkpoint_state_timed", lambda *_args, **_kwargs: None)

    with pytest.raises(RuntimeError, match="Bronze execution did not complete"):
        pipeline_runtime.execute_database_native_layers(state["run_id"], state=state)


@pytest.mark.parametrize(
    ("boundary", "artifact_key", "kept_execution_layers", "invalidated_generation_layers"),
    [
        ("gate4", "bronze_review_artifact", set(), {"silver", "gold"}),
        ("silver_merge_key_review", "silver_merge_key_review_artifact", set(), {"silver", "gold"}),
        ("gate5", "silver_review_artifact", {"bronze"}, {"gold"}),
        ("gold_review", "gold_review_artifact", {"bronze", "silver"}, set()),
    ],
)
def test_generation_first_review_resubmission_clears_ready_state_and_stale_downstream(
    monkeypatch,
    boundary,
    artifact_key,
    kept_execution_layers,
    invalidated_generation_layers,
):
    state = {
        **_generation_first_state(),
        "status": "PAUSED_FOR_STAGE_CONFIRMATION",
        "execution_ready": True,
        "awaiting_stage_confirmation": True,
        "stage_confirmation": {"awaiting_confirmation": True},
        "next_stage_key": "bronze_code_execution",
        "next_stage_label": "Bronze Target Execution",
        "databricks_bronze_execution_status": "COMPLETED",
        "databricks_silver_execution_status": "COMPLETED",
        "databricks_gold_execution_status": "COMPLETED",
        "bronze_execution_status": "COMPLETED",
        "silver_execution_status": "COMPLETED",
        "gold_execution_status": "COMPLETED",
        "bronze_runtime_validation_status": "COMPLETED",
        "silver_runtime_validation_status": "COMPLETED",
        "gold_runtime_validation_status": "COMPLETED",
        "report_generation_status": "COMPLETED",
        "run_report": {"generated_at": "2026-07-30T00:00:00Z"},
    }
    replacement_artifact = {"boundary": boundary, "items": [{"review_status": "REGENERATE"}]}
    monkeypatch.setattr(pipeline_runtime, "load_checkpoint_state", lambda _run_id: state)
    monkeypatch.setattr(pipeline_runtime, "save_checkpoint_state", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(pipeline_runtime, "save_checkpoint_state_timed", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(pipeline_runtime, "ai_store_db_writer", lambda **_kwargs: None)

    if boundary == "gate4":
        result = pipeline_runtime.submit_gate4_review(
            state["run_id"],
            "REGENERATE",
            replacement_artifact,
            checkpoint_state=state,
        )
    elif boundary == "silver_merge_key_review":
        result = pipeline_runtime.submit_silver_merge_key_review(
            state["run_id"],
            "REGENERATE",
            replacement_artifact,
        )
    elif boundary == "gate5":
        result = pipeline_runtime.submit_gate5_review(
            state["run_id"],
            "REGENERATE",
            replacement_artifact,
        )
    else:
        result = pipeline_runtime.submit_gold_review(
            state["run_id"],
            "REGENERATE",
            replacement_artifact,
        )

    assert result["status"] == "REGENERATE_REQUIRED"
    assert result["execution_ready"] is False
    assert result["awaiting_stage_confirmation"] is False
    assert result["stage_confirmation"] is None
    assert result["next_stage_key"] is None
    assert result["next_stage_label"] is None
    assert result[artifact_key] == replacement_artifact
    assert "report_generation_status" not in result
    assert "run_report" not in result
    for layer in ("bronze", "silver", "gold"):
        receipt_key = f"databricks_{layer}_execution_status"
        if layer in kept_execution_layers:
            assert result[receipt_key] == "COMPLETED"
            assert result[f"{layer}_execution_status"] == "COMPLETED"
        else:
            assert receipt_key not in result
            assert f"{layer}_execution_status" not in result
            assert f"{layer}_runtime_validation_status" not in result
    for layer in invalidated_generation_layers:
        assert f"{layer}_generation_results" not in result
    downstream_decisions = {
        "gate4": {
            "silver_merge_key_review_decision",
            "silver_review_decision",
            "gold_review_decision",
        },
        "silver_merge_key_review": {"silver_review_decision", "gold_review_decision"},
        "gate5": {"gold_review_decision"},
        "gold_review": set(),
    }[boundary]
    for decision_key in downstream_decisions:
        assert decision_key not in result


def test_legacy_review_state_is_not_invalidated():
    state = {
        "run_id": "run-legacy",
        "source": "database",
        "target_warehouse": "databricks",
        "execution_ready": True,
        "databricks_gold_execution_status": "COMPLETED",
    }

    assert pipeline_runtime._invalidate_generation_first_review_state(
        state,
        boundary="gold_review",
    ) is state


def test_generation_first_pipeline_steps_put_all_execution_after_gold_review():
    state = {
        **_generation_first_state(),
        "databricks_bronze_execution_status": "COMPLETED",
    }
    steps = pipeline_runtime.build_pipeline_steps(
        source="database",
        checkpoint=state,
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

    assert keys.index("gold_review") < keys.index("bronze_code_execution")
    assert keys[-3:] == [
        "bronze_code_execution",
        "silver_code_execution",
        "gold_code_execution",
    ]


def test_generation_first_old_execution_receipt_does_not_infer_generation_reviews():
    steps = pipeline_runtime.build_pipeline_steps(
        source="database",
        checkpoint={
            "source": "database",
            "target_warehouse": "databricks",
            "execution_engine": "native",
            "database_flow_version": "generation_first_v1",
            "databricks_bronze_execution_status": "COMPLETED",
        },
        summary=[],
        pending_gate1=[],
        completed_gate1=[],
        nominated_tables=[],
        certified_tables=[],
        enriched_payload={},
        gate3_payload={},
        bronze_generation_completed=False,
        silver_generation_completed=False,
        gold_generation_completed=False,
    )
    by_key = {step["key"]: step for step in steps}

    assert by_key["bronze_code_execution"]["complete"] is True
    assert by_key["bronze"]["complete"] is False
    assert by_key["silver"]["complete"] is False
    assert by_key["gold_review"]["complete"] is False


def test_start_pipeline_preserves_seeded_identity_before_first_stage(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        pipeline_runtime,
        "load_checkpoint_state",
        lambda run_id: {
            "run_id": run_id,
            "project_id": "project-1",
            "owner_email": "client@example.com",
            "created_by_email": "client@example.com",
        },
    )

    def interrupt_before_completion(run_id, *, start_stage_key, state):
        captured.update(state)
        raise RuntimeError("interrupted")

    monkeypatch.setattr(pipeline_runtime, "continue_database_pipeline", interrupt_before_completion)

    with pytest.raises(RuntimeError, match="interrupted"):
        pipeline_runtime.start_pipeline(
            run_id="run-owned",
            brd_text="BRD",
            source="database",
        )

    assert captured["project_id"] == "project-1"
    assert captured["owner_email"] == "client@example.com"
    assert captured["created_by_email"] == "client@example.com"


def test_gate2_scope_keeps_lookup_and_fk_dimension_tables():
    tables = [
        {"database_name": "insurance", "schema_name": "dbo", "table_name": "claim_information", "nomination_reason": "Dual Match (Keyword + Semantic)"},
        {"database_name": "insurance", "schema_name": "dbo", "table_name": "dim_policy", "nomination_reason": "Lookup Table Sweep (dim/ref/lkp)"},
        {"database_name": "insurance", "schema_name": "dbo", "table_name": "policy_type", "nomination_reason": "FK Resolution (related to nominated table)"},
        {
            "database_name": "insurance",
            "schema_name": "dbo",
            "table_name": "policy_history",
            "nomination_method": "FK Resolution (related to nominated table)",
            "nomination_reason": "Supporting table connected by a foreign key to a nominated KPI source",
        },
        {
            "database_name": "insurance",
            "schema_name": "dbo",
            "table_name": "policy_status",
            "nomination_reason": "Supporting table connected by a foreign key to a nominated KPI source",
        },
        {"database_name": "insurance", "schema_name": "dbo", "table_name": "audit_log", "nomination_reason": "Lookup Table Sweep (dim/ref/lkp)"},
    ]

    scoped = pipeline_runtime._gate2_execution_scope(tables, ["insurance.dbo.claim_information"])

    assert [item["table_name"] for item in scoped] == [
        "claim_information",
        "dim_policy",
        "policy_type",
        "policy_history",
        "policy_status",
    ]


def test_gate2_submission_recovers_nominations_from_run_checkpoint(monkeypatch):
    nominated = [{
        "database_name": "insurance",
        "schema_name": "dbo",
        "table_name": "claim_information",
        "nomination_reason": "Dual Match (Keyword + Semantic)",
    }]
    captured = {}
    monkeypatch.setattr(pipeline_runtime, "fetch_json_artifact", lambda *_: {})
    monkeypatch.setattr(
        pipeline_runtime,
        "load_checkpoint_state",
        lambda _: {
            "run_id": "run-retry-gate2",
            "status": "FAILED",
            "error": "stale artifact failure",
            "failed_background_stage": "gate2",
            "nominated_tables": nominated,
        },
    )

    def certify(state):
        captured["certification_input"] = state
        return {**state, "status": "GATE2_COMPLETE"}

    def continue_pipeline(run_id, *, start_stage_key, state):
        captured["continued"] = (run_id, start_stage_key, state)
        return state

    monkeypatch.setattr("nodes.hitl.hitl_table_review_node", certify)
    monkeypatch.setattr(pipeline_runtime, "save_checkpoint_state", lambda run_id, state: captured.update(saved=(run_id, state)))
    monkeypatch.setattr(pipeline_runtime, "continue_database_pipeline", continue_pipeline)

    result = pipeline_runtime.submit_gate2_review(
        "run-retry-gate2",
        ["insurance.dbo.claim_information"],
    )

    certification_input = captured["certification_input"]
    assert certification_input["certified_tables"] == nominated
    assert certification_input["human_table_decision"] == "COMPLETED"
    assert "error" not in certification_input
    assert "failed_background_stage" not in certification_input
    assert captured["continued"][:2] == ("run-retry-gate2", "discovery")
    assert result["status"] == "GATE2_COMPLETE"


def test_gate2_materializes_one_inactive_object_per_approved_table(monkeypatch):
    from types import SimpleNamespace
    from services import metadata_selection

    calls = []

    class Repository:
        def upsert_database_ingestion_object_draft(self, **kwargs):
            calls.append(kwargs)
            return {
                "ingestion_object_id": 100 + len(calls),
                "config_version": 1,
                "config_hash": "sha256:object",
            }

    monkeypatch.setattr(
        metadata_selection,
        "validated_metadata_selection",
        lambda _state: SimpleNamespace(
            repository=Repository(),
            connection={"config_version": 3, "config_hash": "sha256:connection"},
            uses_environment_source=True,
        ),
    )
    approved = [
        {"database_name": "insurance", "schema_name": "dbo", "table_name": "claims"},
        {"database_name": "insurance", "schema_name": "dbo", "table_name": "policy"},
    ]

    materialized = pipeline_runtime._materialize_gate2_ingestion_objects(
        {"source_system_id": 7, "source_connection_id": 11},
        approved,
    )

    assert [item["ingestion_object_id"] for item in materialized] == [101, 102]
    assert all(item["ingestion_object_config_version"] == 1 for item in materialized)
    assert [call["table"]["table_name"] for call in calls] == ["claims", "policy"]
    assert all(call["expected_connection_version"] == 3 for call in calls)
    assert all(call["allow_inactive_connection"] is True for call in calls)


def test_metadata_gate2_does_not_materialize_unapproved_support_tables(monkeypatch):
    tables = [
        {"database_name": "insurance", "schema_name": "dbo", "table_name": "claims"},
        {
            "database_name": "insurance",
            "schema_name": "dbo",
            "table_name": "dim_policy",
            "nomination_reason": "Supporting table connected by a foreign key to a nominated KPI source",
        },
    ]
    captured = {}
    state = {
        "run_id": "run-metadata-gate2",
        "source_system_id": 7,
        "source_connection_id": 11,
        "nominated_tables": tables,
    }
    monkeypatch.setattr(pipeline_runtime, "load_checkpoint_state", lambda _run_id: state)
    monkeypatch.setattr(pipeline_runtime, "fetch_json_artifact", lambda *_args: {})
    monkeypatch.setattr(
        pipeline_runtime,
        "_materialize_gate2_ingestion_objects",
        lambda _state, approved: captured.setdefault("approved", approved),
    )
    monkeypatch.setattr(
        "nodes.hitl.hitl_table_review_node",
        lambda review_state: {**review_state, "status": "GATE2_COMPLETE"},
    )
    monkeypatch.setattr(pipeline_runtime, "save_checkpoint_state", lambda *_args: None)
    monkeypatch.setattr(
        pipeline_runtime,
        "continue_database_pipeline",
        lambda _run_id, **kwargs: kwargs["state"],
    )

    pipeline_runtime.submit_gate2_review("run-metadata-gate2", ["insurance.dbo.claims"])

    assert [table["table_name"] for table in captured["approved"]] == ["claims"]


def test_bronze_metadata_is_selected_and_ordered_by_exact_mapping_bundle(monkeypatch):
    from types import SimpleNamespace
    from services import metadata_selection

    mappings = [
        {
            "source_field_path": "amount",
            "target_column_name": "amount",
            "target_data_type": "decimal(12,2)",
            "ordinal_position": 2,
        },
        {
            "source_field_path": "claim_id",
            "target_column_name": "claim_id",
            "target_data_type": "int",
            "ordinal_position": 1,
        },
    ]

    class Repository:
        def get_ingestion_object(self, _object_id, _version):
            return {
                "config_hash": "sha256:object",
                "target_bronze_table": "main.bronze.bronze_claims",
            }

        def get_mapping_bundle(self, **_kwargs):
            return {
                "ingestion_object_id": 101,
                "mapping_version": 9,
                "mapping_hash": "sha256:mapping",
                "mappings": mappings,
            }

    monkeypatch.setattr(
        metadata_selection,
        "validated_metadata_selection",
        lambda _state: SimpleNamespace(
            repository=Repository(),
            connection={
                "host_name": "source.database.windows.net",
                "port": 1433,
                "database_name": "insurance",
                "secrets_json": "{}",
                "config_json": "{}",
                "config_version": 1,
                "config_hash": "sha256:connection",
            },
        ),
    )
    state = {
        "source_system_id": 7,
        "source_connection_id": 11,
        "certified_tables": [
            {
                "ingestion_object_id": 101,
                "ingestion_object_config_version": 1,
                "ingestion_object_config_hash": "sha256:object",
                "source_to_bronze_mapping_version": 9,
                "source_to_bronze_mapping_hash": "sha256:mapping",
            }
        ],
        "discovered_metadata": {
            "tables": [
                {
                    "ingestion_object_id": 101,
                    "table_name": "claims",
                    "columns": [
                        {"column_name": "amount", "data_type": "decimal"},
                        {"column_name": "unused", "data_type": "varchar"},
                        {"column_name": "claim_id", "data_type": "int"},
                    ],
                }
            ]
        },
    }

    mapped = pipeline_runtime._mapping_driven_bronze_state(state)

    assert [column["column_name"] for column in mapped["discovered_metadata"]["tables"][0]["columns"]] == [
        "claim_id",
        "amount",
    ]


def test_merge_key_approval_materializes_exact_bronze_to_silver_draft(monkeypatch):
    from types import SimpleNamespace
    from services import metadata_selection

    captured = {}
    monkeypatch.setenv("SILVER_CATALOG", "main")
    monkeypatch.setenv("SILVER_SCHEMA", "silver")
    source_object = {
        "ingestion_object_id": 101,
        "source_system_id": 7,
        "config_version": 2,
        "config_hash": "sha256:bronze-object",
        "target_bronze_table": "main.bronze.bronze_claims",
        "active_flag": True,
        "is_current": True,
    }
    source_mapping = {
        "mapping_version": 9,
        "mapping_hash": "sha256:bronze-mapping",
        "mappings": [
            {
                "source_field_path": "ClaimID",
                "target_column_name": "claimid",
                "target_data_type": "int",
                "is_nullable": False,
                "ordinal_position": 1,
            },
            {
                "source_field_path": "Description",
                "target_column_name": "description",
                "target_data_type": "string",
                "is_nullable": True,
                "ordinal_position": 2,
            },
        ],
    }

    class Repository:
        def get_active_ingestion_object(self, object_id):
            assert object_id == 101
            return source_object

        def get_mapping_bundle(self, **kwargs):
            assert kwargs["mapping_version"] == 9
            assert kwargs["expected_hash"] == "sha256:bronze-mapping"
            assert kwargs["require_active"] is True
            return source_mapping

        def upsert_bronze_to_silver_draft(self, **kwargs):
            captured.update(kwargs)
            return {
                "ingestion_object": {
                    "ingestion_object_id": 202,
                    "config_version": 3,
                    "config_hash": "sha256:silver-object",
                },
                "mapping_bundle": {
                    "mapping_version": 11,
                    "mapping_hash": "sha256:silver-mapping",
                },
            }

    monkeypatch.setattr(
        metadata_selection,
        "validated_metadata_selection",
        lambda _state: SimpleNamespace(repository=Repository()),
    )
    result = pipeline_runtime._materialize_bronze_to_silver_metadata(
        {
            "target_warehouse": "databricks",
            "source_system_id": 7,
            "bronze_generation_results": [{
                "ingestion_object_id": 101,
                "mapping_version": 9,
                "mapping_hash": "sha256:bronze-mapping",
                "database_name": "ClaimsDB",
                "schema_name": "dbo",
                "table": "Claims",
            }],
        },
        {"feeds": [{
            "ingestion_object_id": 101,
            "database_name": "ClaimsDB",
            "schema_name": "dbo",
            "table": "Claims",
            "merge_keys": ["ClaimID"],
        }]},
    )

    silver = result["bronze_generation_results"][0]
    assert captured["target_silver_table"] == "main.silver.silver_Claims"
    assert captured["merge_keys"] == ["claimid"]
    assert captured["columns"][0]["source_field_path"] == "claimid"
    assert captured["columns"][1]["transformation_rule"] == "TRIM_CAST"
    assert silver["silver_ingestion_object_id"] == 202
    assert silver["bronze_to_silver_mapping_hash"] == "sha256:silver-mapping"


def test_snowflake_dbt_silver_uses_current_run_draft_not_active_databricks_metadata(monkeypatch):
    from types import SimpleNamespace
    from services import metadata_selection

    captured = {}
    draft = {
        "ingestion_object_id": 101,
        "source_system_id": 7,
        "config_version": 3,
        "config_hash": "sha256:snowflake-object",
        "target_bronze_table": "INSURANCE.BRONZE.bronze_claims",
        "active_flag": False,
        "is_current": False,
    }
    bundle = {
        "mapping_version": 19,
        "mapping_hash": "sha256:snowflake-mapping",
        "active_flag": False,
        "mappings": [{
            "target_column_name": "claimid",
            "target_data_type": "NUMBER",
            "is_nullable": False,
            "ordinal_position": 1,
        }],
    }

    class Repository:
        def get_ingestion_objects(self, refs, *, require_active):
            assert list(refs) == [{"ingestion_object_id": 101, "config_version": 3}]
            assert require_active is False
            return {(101, 3): draft}

        def get_mapping_bundles(self, refs):
            assert refs[0]["expected_target"] == "INSURANCE.BRONZE.bronze_claims"
            assert refs[0]["require_active"] is None
            return {(101, "SOURCE_TO_BRONZE", 19): bundle}

        def get_active_ingestion_object(self, _object_id):
            raise AssertionError("Snowflake dbt design must not read active Databricks metadata")

        def upsert_bronze_to_silver_draft(self, **kwargs):
            captured.update(kwargs)
            return {
                "ingestion_object": {
                    "ingestion_object_id": 202,
                    "config_version": 5,
                    "config_hash": "sha256:silver-object",
                },
                "mapping_bundle": {
                    "mapping_version": 29,
                    "mapping_hash": "sha256:silver-mapping",
                },
            }

    monkeypatch.setattr(
        metadata_selection,
        "validated_metadata_selection",
        lambda _state: SimpleNamespace(repository=Repository()),
    )
    result = pipeline_runtime._materialize_bronze_to_silver_metadata(
        {
            "target_warehouse": "snowflake",
            "execution_engine": "dbt",
            "source_system_id": 7,
            "bronze_generation_results": [{
                "ingestion_object_id": 101,
                "ingestion_object_config_version": 3,
                "mapping_version": 19,
                "mapping_hash": "sha256:snowflake-mapping",
                "metadata_activation_status": "PENDING_FINAL_DBT_PACKAGE",
                "target_table": "INSURANCE.BRONZE.bronze_claims",
                "database_name": "ClaimsDB",
                "schema_name": "dbo",
                "table": "Claims",
            }],
        },
        {"feeds": [{
            "ingestion_object_id": 101,
            "database_name": "ClaimsDB",
            "schema_name": "dbo",
            "table": "Claims",
            "merge_keys": ["ClaimID"],
        }]},
    )

    assert captured["source_object"] == draft
    assert captured["source_mapping"] == bundle
    assert captured["allow_inactive_source"] is True
    assert result["bronze_generation_results"][0]["silver_ingestion_object_id"] == 202


def test_metadata_merge_key_review_rejects_ambiguous_leaf_table(monkeypatch):
    from types import SimpleNamespace
    from services import metadata_selection

    monkeypatch.setattr(
        metadata_selection,
        "validated_metadata_selection",
        lambda _state: SimpleNamespace(repository=object()),
    )
    state = {
        "target_warehouse": "databricks",
        "bronze_generation_results": [
            {"ingestion_object_id": 101, "database_name": "db", "schema_name": "a", "table": "Claims"},
            {"ingestion_object_id": 102, "database_name": "db", "schema_name": "b", "table": "Claims"},
        ],
    }

    with pytest.raises(ValueError, match="unambiguously"):
        pipeline_runtime._materialize_bronze_to_silver_metadata(
            state,
            {"feeds": [{"table": "Claims", "merge_keys": ["ClaimID"]}]},
        )


def test_gate5_metadata_filter_uses_transformation_id_not_leaf_name() -> None:
    selected_object_id = 4_134_741_637_349_269_810
    results = [
        {"silver_ingestion_object_id": 201, "table": "claims", "target_table": "main.a.silver_claims"},
        {"silver_ingestion_object_id": selected_object_id, "table": "claims", "target_table": "main.b.silver_claims"},
    ]

    selected = pipeline_runtime._filter_silver_results_by_gate5_review(
        results,
        {
            "items": [
                {
                    "silver_ingestion_object_id": str(selected_object_id),
                    "table": "claims",
                    "review_status": "APPROVED",
                }
            ]
        },
    )

    assert [item["silver_ingestion_object_id"] for item in selected] == [selected_object_id]


def test_gate5_silver_artifact_is_hashed_and_activated_from_exact_draft(monkeypatch) -> None:
    from types import SimpleNamespace
    from services import metadata_selection

    artifact_root = Path.cwd() / ".tmp-tests" / f"gate5-silver-{uuid.uuid4().hex}"
    artifact = artifact_root / "silver" / "claims.py"
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact.write_text("print('approved silver')\n", encoding="utf-8")
    monkeypatch.setenv("ATHENA_GENERATED_CODE_DIR", str(artifact_root))
    captured = {}

    class Repository:
        def register_and_activate_bronze_to_silver_artifact(self, **kwargs):
            captured.update(kwargs)
            return {
                "ingestion_object": {"config_version": 4, "config_hash": "sha256:active"},
                "execution_spec": {**kwargs["execution_spec"], "processing_stage": "BRONZE_TO_SILVER"},
            }

    monkeypatch.setattr(
        metadata_selection,
        "validated_metadata_selection",
        lambda _state: SimpleNamespace(repository=Repository()),
    )
    state = {
        "run_id": "design-run",
        "target_warehouse": "databricks",
        "silver_review_artifact": {"items": [{
            "silver_ingestion_object_id": 202,
            "generated_silver_script": "print('approved silver')\n",
        }]},
        "silver_generation_results": [{
            "silver_ingestion_object_id": 202,
            "silver_ingestion_object_config_version": 3,
            "silver_ingestion_object_config_hash": "sha256:draft",
            "bronze_to_silver_mapping_version": 11,
            "bronze_to_silver_mapping_hash": "sha256:mapping",
            "script_path": str(artifact),
            "target_table": "main.silver.silver_claims",
            "code_generation_format": "native",
        }],
        "silver_transformation_objects": [{"ingestion_object_id": 202}],
    }

    attached = pipeline_runtime._attach_silver_execution_specs(state)
    activated = pipeline_runtime._activate_reviewed_silver_metadata(attached)

    assert captured["draft_config_version"] == 3
    assert captured["mapping_version"] == 11
    assert captured["execution_spec"]["mapping_version"] == 11
    assert activated["silver_generation_results"][0]["metadata_activation_status"] == "ACTIVE"
    assert activated["silver_transformation_objects"][0]["active_config_version"] == 4


def test_bronze_review_activates_one_set_based_stage_bundle(monkeypatch) -> None:
    from types import SimpleNamespace
    from services import metadata_selection

    class Repository:
        def __init__(self):
            self.calls = []

        def register_and_activate_artifacts(self, *, processing_stage, artifacts):
            items = list(artifacts)
            self.calls.append((processing_stage, items))
            return [{
                "ingestion_object": {
                    "ingestion_object_id": item["ingestion_object_id"],
                    "config_version": 2,
                    "config_hash": f"active-{item['ingestion_object_id']}",
                },
                "mapping_bundle": {},
                "execution_spec": item["execution_spec"],
            } for item in items]

    repository = Repository()
    monkeypatch.setattr(
        metadata_selection,
        "validated_metadata_selection",
        lambda _state: SimpleNamespace(repository=repository),
    )
    results = [{
        "ingestion_object_id": object_id,
        "ingestion_object_config_version": 1,
        "mapping_version": object_id + 10,
        "mapping_hash": f"mapping-{object_id}",
        "execution_spec": {"engine": "DATABRICKS_JOB"},
    } for object_id in (101, 102)]
    activated = pipeline_runtime._activate_reviewed_bronze_metadata({
        "source_system_id": 7,
        "bronze_generation_results": results,
        "certified_tables": [{"ingestion_object_id": 101}, {"ingestion_object_id": 102}],
    })

    assert len(repository.calls) == 1
    assert repository.calls[0][0] == "SOURCE_TO_BRONZE"
    assert len(repository.calls[0][1]) == 2
    assert [item["metadata_activation_status"] for item in activated["bronze_generation_results"]] == [
        "ACTIVE", "ACTIVE"
    ]


def test_gate5_metadata_rejects_edited_executable_code(monkeypatch) -> None:
    artifact_root = Path.cwd() / ".tmp-tests" / f"gate5-edited-{uuid.uuid4().hex}"
    artifact = artifact_root / "silver" / "claims.py"
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact.write_text("print('generated')\n", encoding="utf-8")
    monkeypatch.setenv("ATHENA_GENERATED_CODE_DIR", str(artifact_root))

    with pytest.raises(ValueError, match="cannot replace executable code"):
        pipeline_runtime._attach_silver_execution_specs({
            "run_id": "design-run",
            "target_warehouse": "databricks",
            "silver_review_artifact": {"items": [{
                "silver_ingestion_object_id": 202,
                "generated_silver_script": "print('edited')\n",
            }]},
            "silver_generation_results": [{
                "silver_ingestion_object_id": 202,
                "bronze_to_silver_mapping_version": 11,
                "script_path": str(artifact),
            }],
        })


@pytest.mark.parametrize(
    ("filters", "aggregation", "measure_type", "expected_kinds", "rejection_code", "expected_fact_type"),
    [
        ([], "SUM", "DECIMAL(18,2)", ["DIMENSION", "FACT"], None, "DECIMAL(38,10)"),
        ([], "MIN", "STRING", ["DIMENSION", "FACT"], None, "STRING"),
        (["Consistent identifiers across systems"], "SUM", "DECIMAL(18,2)", ["DIMENSION"], "UNSUPPORTED_FILTER", None),
        ([{"column": "claimstatus", "operator": "=", "value": "OPEN"}], "SUM", "DECIMAL(18,2)", ["DIMENSION"], "UNSUPPORTED_FILTER", None),
        ([], "RATIO", "DECIMAL(18,2)", ["DIMENSION"], "INVALID_AGGREGATION", None),
    ],
)
def test_gate5_materializes_independent_gold_fact_and_dimension_drafts(
    monkeypatch, filters, aggregation, measure_type, expected_kinds, rejection_code, expected_fact_type
) -> None:
    from types import SimpleNamespace
    from services import metadata_selection

    captured = []
    monkeypatch.setenv("GOLD_CATALOG", "main")
    monkeypatch.setenv("GOLD_SCHEMA", "gold")
    active = {
        "ingestion_object_id": 202,
        "source_system_id": 7,
        "config_version": 4,
        "config_hash": "sha256:silver-object",
        "processing_stage": "BRONZE_TO_SILVER",
        "target_table": "main.silver.silver_claims",
        "target_silver_table": "main.silver.silver_claims",
    }
    bundle = {
        "mapping_version": 11,
        "mapping_hash": "sha256:silver-mapping",
        "mappings": [
            {"target_column_name": "claimid", "target_data_type": "BIGINT", "is_primary_key": True, "is_nullable": False},
            {"target_column_name": "claimstatus", "target_data_type": "STRING", "is_primary_key": False, "is_nullable": True},
            {"target_column_name": "claimamount", "target_data_type": measure_type, "is_primary_key": False, "is_nullable": True},
        ],
    }

    class Repository:
        def get_mapping_bundle(self, **_kwargs):
            return bundle

        def get_active_ingestion_object(self, object_id):
            return active if object_id == 202 else None

        def upsert_silver_to_gold_draft(self, **kwargs):
            captured.append(kwargs)
            object_id = 300 + len(captured)
            return {
                "ingestion_object": {
                    "ingestion_object_id": object_id,
                    "config_version": 3,
                    "config_hash": f"sha256:gold-object-{object_id}",
                },
                "mapping_bundle": {
                    "mapping_version": object_id,
                    "mapping_hash": f"sha256:gold-mapping-{object_id}",
                },
            }

    monkeypatch.setattr(
        metadata_selection,
        "validated_metadata_selection",
        lambda _state: SimpleNamespace(repository=Repository()),
    )
    result = pipeline_runtime._materialize_silver_to_gold_metadata({
        "target_warehouse": "databricks",
        "silver_generation_results": [{
            "table": "claims",
            "target_table": "main.silver.silver_claims",
            "silver_ingestion_object_id": 202,
            "bronze_to_silver_mapping_version": 11,
            "bronze_to_silver_mapping_hash": "sha256:silver-mapping",
            "metadata_activation_status": "ACTIVE",
        }],
        "gold_generation_contract": {
            "dimension_mappings": [{
                "logical_table": "claims",
                "source_silver_table": "main.silver.silver_claims",
                "columns": ["claimstatus"],
            }],
            "kpi_mappings": [{
                "kpi_name": "Total Claims",
                "source_silver_table": "main.silver.silver_claims",
                "measure": {"table": "claims", "column": "claimamount", "aggregation": aggregation},
                "grouping_dimensions": [{"table": "claims", "column": "claimstatus", "semantic_type": "DIMENSION"}],
                "time": {"grain": "month", "column": None},
                "filters": filters,
                "join_paths": [],
                "readiness": "READY",
            }],
        },
    })

    assert [item["artifact_kind"] for item in result["gold_metadata_drafts"]] == expected_kinds
    assert [item["target_gold_table"] for item in captured] == [
        "main.gold.dim_claims",
        *(["main.gold.fact_total_claims"] if "FACT" in expected_kinds else []),
    ]
    assert captured[0]["merge_keys"] == ["claims_key"]
    assert captured[0]["columns"][0]["transformation_rule"] == "SURROGATE_KEY"
    assert captured[0]["columns"][0]["source_field_path"] == "claimid"
    assert {column["target_column_name"] for column in captured[0]["columns"][1:]} == {"claimid", "claimstatus"}
    if "FACT" in expected_kinds:
        assert captured[1]["merge_keys"] == ["claims_key"]
        assert captured[1]["columns"][0]["transformation_rule"] == "DIMENSION_KEY"
        assert captured[1]["columns"][0]["source_field_path"] == "claimid"
        aggregate = next(
            column for column in captured[1]["columns"]
            if str(column.get("transformation_rule") or "").startswith("AGG_")
        )
        assert aggregate["target_data_type"] == expected_fact_type
    assert [item["code"] for item in result["gold_metadata_rejections"]] == (
        [rejection_code] if rejection_code else []
    )


def test_gate5_persists_validated_snowflake_multi_input_gold(monkeypatch) -> None:
    from types import SimpleNamespace
    from services import metadata_selection

    captured = []
    active = {
        object_id: {
            "ingestion_object_id": object_id,
            "source_system_id": 7,
            "config_version": 4,
            "config_hash": f"sha256:object-{object_id}",
            "processing_stage": "BRONZE_TO_SILVER",
            "target_table": target,
            "target_silver_table": target,
        }
        for object_id, target in ((202, "ATHENA_DB.SILVER.silver_claims"), (203, "ATHENA_DB.SILVER.silver_policy"))
    }
    bundles = {
        object_id: {
            "mapping_version": object_id,
            "mapping_hash": f"sha256:mapping-{object_id}",
            "mappings": [
                {"target_column_name": "policyid", "target_data_type": "BIGINT", "is_primary_key": True, "is_nullable": False},
                {"target_column_name": "amount", "target_data_type": "DECIMAL(18,2)", "is_primary_key": False, "is_nullable": True},
            ],
        }
        for object_id in active
    }

    class Repository:
        def get_mapping_bundle(self, **kwargs):
            return bundles[int(kwargs["ingestion_object_id"])]

        def get_active_ingestion_object(self, object_id):
            return active.get(object_id)

        def upsert_silver_to_gold_draft(self, **kwargs):
            captured.append(kwargs)
            return {
                "ingestion_object": {
                    "ingestion_object_id": 301,
                    "config_version": 3,
                    "config_hash": "sha256:gold-object",
                },
                "mapping_bundle": {
                    "mapping_version": 31,
                    "mapping_hash": "sha256:gold-mapping",
                },
            }

    monkeypatch.setattr(metadata_selection, "validated_metadata_selection", lambda _state: SimpleNamespace(repository=Repository()))
    state = {
        "target_warehouse": "snowflake",
        "silver_generation_results": [
            {
                "table": logical,
                "target_table": active[object_id]["target_table"],
                "silver_ingestion_object_id": object_id,
                "bronze_to_silver_mapping_version": object_id,
                "bronze_to_silver_mapping_hash": f"sha256:mapping-{object_id}",
                "metadata_activation_status": "ACTIVE",
            }
            for object_id, logical in ((202, "claims"), (203, "policy"))
        ],
        "gold_generation_contract": {
            "dimension_mappings": [],
            "kpi_mappings": [{
                "kpi_name": "Claims by Policy",
                "measure": {"table": "claims", "column": "amount", "aggregation": "SUM"},
                "grouping_dimensions": [{"table": "policy", "column": "policyid", "semantic_type": "DIMENSION"}],
                "time": {"column": None},
                "filters": [],
                "join_paths": [{
                    "left_table": "claims", "left_column": "policyid",
                    "right_table": "policy", "right_column": "policyid", "certified": True,
                }],
                "readiness": "READY",
            }],
        },
    }

    result = pipeline_runtime._materialize_silver_to_gold_metadata(state)

    assert [item["artifact_kind"] for item in result["gold_metadata_drafts"]] == ["DIMENSION", "FACT"]
    assert [item["target_gold_table"] for item in captured] == [
        "INSURANCE.GOLD.dim_policy",
        "INSURANCE.GOLD.fact_claims_by_policy",
    ]
    assert captured[0]["merge_keys"] == ["policy_key"]
    assert captured[0]["columns"][0]["transformation_rule"] == "SURROGATE_KEY"
    assert len(captured[1]["inputs"]) == 2
    assert captured[1]["merge_keys"] == ["policy_key"]
    assert captured[1]["columns"][0]["transformation_rule"] == "DIMENSION_KEY"
    assert captured[1]["join_rules"] == [{
        "left_source_table": "ATHENA_DB.SILVER.silver_claims",
        "left_column": "policyid",
        "right_source_table": "ATHENA_DB.SILVER.silver_policy",
        "right_column": "policyid",
        "join_type": "INNER",
        "cardinality": None,
        "certified": True,
    }]


def test_gate5_materializes_factless_fact_and_logs_blocked_kpi(monkeypatch) -> None:
    from types import SimpleNamespace
    from services import metadata_selection

    captured = []
    active = {
        "ingestion_object_id": 202,
        "source_system_id": 7,
        "config_version": 4,
        "config_hash": "sha256:silver-object",
    }
    bundle = {
        "mapping_version": 11,
        "mapping_hash": "sha256:silver-mapping",
        "mappings": [
            {"target_column_name": "event_id", "target_data_type": "BIGINT", "is_primary_key": True},
            {"target_column_name": "status", "target_data_type": "STRING", "is_primary_key": False},
        ],
    }

    class Repository:
        def get_mapping_bundle(self, **_kwargs):
            return bundle

        def get_active_ingestion_object(self, object_id):
            return active if object_id == 202 else None

        def upsert_silver_to_gold_draft(self, **kwargs):
            captured.append(kwargs)
            return {
                "ingestion_object": {
                    "ingestion_object_id": 301,
                    "config_version": 3,
                    "config_hash": "sha256:gold-object",
                },
                "mapping_bundle": {
                    "mapping_version": 31,
                    "mapping_hash": "sha256:gold-mapping",
                },
            }

    monkeypatch.setattr(
        metadata_selection,
        "validated_metadata_selection",
        lambda _state: SimpleNamespace(repository=Repository()),
    )
    result = pipeline_runtime._materialize_silver_to_gold_metadata({
        "target_warehouse": "databricks",
        "silver_generation_results": [{
            "table": "events",
            "target_table": "main.silver.silver_events",
            "silver_ingestion_object_id": 202,
            "bronze_to_silver_mapping_version": 11,
            "bronze_to_silver_mapping_hash": "sha256:silver-mapping",
            "metadata_activation_status": "ACTIVE",
        }],
        "gold_generation_contract": {
            "dimension_mappings": [],
            "factless_mappings": [{
                "fact_type": "FACTLESS_ENTITY_COVERAGE",
                "logical_table": "events",
                "grain_columns": ["event_id"],
            }],
            "kpi_mappings": [{
                "kpi_name": "Uncomputable Ratio",
                "readiness": "BLOCKED",
            }],
        },
    })

    assert result["gold_metadata_materialization_status"] == "READY"
    assert result["gold_metadata_drafts"][0]["fact_type"] == "FACTLESS_ENTITY_COVERAGE"
    assert result["gold_metadata_rejections"][0]["code"] == "INCOMPLETE_KPI_CONTRACT"
    assert captured[0]["merge_keys"] == ["event_id"]
    assert captured[0]["columns"][0]["transformation_rule"] == "GROUP_KEY"
    assert captured[0]["write_mode"] == "MERGE"


def test_gate5_no_computable_gold_objects_is_nonfatal(monkeypatch) -> None:
    from types import SimpleNamespace
    from services import metadata_selection

    monkeypatch.setattr(
        metadata_selection,
        "validated_metadata_selection",
        lambda _state: SimpleNamespace(repository=object()),
    )
    result = pipeline_runtime._materialize_silver_to_gold_metadata({
        "target_warehouse": "databricks",
        "silver_generation_results": [],
        "gold_generation_contract": {
            "dimension_mappings": [],
            "factless_mappings": [],
            "kpi_mappings": [{"kpi_name": "Unavailable KPI", "readiness": "BLOCKED"}],
        },
    })

    assert result["gold_metadata_drafts"] == []
    assert result["gold_metadata_materialization_status"] == "SKIPPED_NOT_COMPUTABLE"
    assert result["gold_metadata_rejections"][0]["code"] == "INCOMPLETE_KPI_CONTRACT"


def test_gold_review_hashes_and_activates_exact_transformation_artifact(monkeypatch) -> None:
    from types import SimpleNamespace
    from services import metadata_selection

    artifact_root = Path.cwd() / ".tmp-tests" / f"gold-activation-{uuid.uuid4().hex}"
    artifact = artifact_root / "gold" / "fact_claims.py"
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact.write_text("print('approved gold')\n", encoding="utf-8")
    monkeypatch.setenv("ATHENA_GENERATED_CODE_DIR", str(artifact_root))
    captured = {}

    class Repository:
        def register_and_activate_silver_to_gold_artifact(self, **kwargs):
            captured.update(kwargs)
            return {
                "ingestion_object": {"config_version": 4, "config_hash": "sha256:active-gold"},
                "execution_spec": {**kwargs["execution_spec"], "processing_stage": "SILVER_TO_GOLD"},
            }

    monkeypatch.setattr(metadata_selection, "validated_metadata_selection", lambda _state: SimpleNamespace(repository=Repository()))
    state = {
        "run_id": "design-run",
        "target_warehouse": "databricks",
        "gold_review_artifact": {"items": [{
            "gold_ingestion_object_id": 302,
            "script_body": "print('approved gold')\n",
        }]},
        "gold_generation_results": [{
            "gold_ingestion_object_id": 302,
            "gold_ingestion_object_config_version": 3,
            "gold_ingestion_object_config_hash": "sha256:draft-gold",
            "silver_to_gold_mapping_version": 32,
            "silver_to_gold_mapping_hash": "sha256:gold-mapping",
            "script_path": str(artifact),
            "target_table": "gold.fact_claims",
        }],
        "gold_transformation_objects": [{"ingestion_object_id": 302}],
    }

    activated = pipeline_runtime._activate_reviewed_gold_metadata(
        pipeline_runtime._attach_gold_execution_specs(state)
    )

    assert captured["draft_config_version"] == 3
    assert captured["mapping_version"] == 32
    assert captured["execution_spec"]["mapping_version"] == 32
    assert activated["gold_generation_results"][0]["metadata_activation_status"] == "ACTIVE"
    assert activated["gold_transformation_objects"][0]["active_config_version"] == 4


def test_metadata_gold_review_filters_by_exact_object_id() -> None:
    results = [
        {"gold_ingestion_object_id": 301, "target_table": "gold.dim_claims"},
        {"gold_ingestion_object_id": 302, "target_table": "gold.fact_claims"},
    ]

    selected = pipeline_runtime._filter_gold_results_by_review(
        results,
        {"items": [{"gold_ingestion_object_id": 302, "review_status": "APPROVED"}]},
    )

    assert [item["gold_ingestion_object_id"] for item in selected] == [302]


def test_metadata_native_execution_confirmation_enqueues_only_bronze_roots(monkeypatch) -> None:
    from types import SimpleNamespace
    from services import metadata_selection

    captured = []

    class Repository:
        context = SimpleNamespace(platform="databricks", environment="qa")

        def enqueue_work(self, **kwargs):
            captured.append(kwargs)
            return {
                "queue_id": 900 + len(captured),
                "queue_status": "PENDING",
                "logical_work_id": kwargs["logical_work_id"],
            }

    monkeypatch.setattr(metadata_selection, "validated_target_metadata_selection", lambda _state: SimpleNamespace(repository=Repository()))
    monkeypatch.setattr(pipeline_runtime, "save_checkpoint_state_timed", lambda *_args, **_kwargs: None)
    state = {
        "run_id": "design-run",
        "target_warehouse": "databricks",
        "bronze_generation_results": [{
            "ingestion_object_id": 101, "target_table": "main.bronze.claims", "metadata_activation_status": "ACTIVE",
        }],
        "silver_generation_results": [{
            "silver_ingestion_object_id": 201, "target_table": "main.silver.claims", "metadata_activation_status": "ACTIVE",
        }],
        "gold_generation_results": [{
            "gold_ingestion_object_id": 301, "target_table": "main.gold.fact_claims", "metadata_activation_status": "ACTIVE",
        }],
    }

    queued = pipeline_runtime._enqueue_metadata_native_runtime(state)

    assert queued["status"] == "RUNTIME_QUEUED"
    assert [item["ingestion_object_id"] for item in queued["metadata_runtime_queue"]] == [101]
    assert [item["priority"] for item in captured] == [300]
    assert len({item["logical_work_id"] for item in captured}) == 1


def test_metadata_native_execution_is_drained_by_application_worker(monkeypatch) -> None:
    from types import SimpleNamespace
    from services import metadata_runtime_worker, metadata_selection

    logical_work_id = "logical-design-run"
    outcomes = [{
        "outcomes": [
            {"queue": {"queue_id": 1, "ingestion_object_id": 101}, "run": {"run_id": "runtime-1"}, "status": "SUCCESS"},
            {"queue": {"queue_id": 2, "ingestion_object_id": 201}, "run": {"run_id": "runtime-2"}, "status": "SUCCESS"},
            {"queue": {"queue_id": 3, "ingestion_object_id": 301}, "run": {"run_id": "runtime-3"}, "status": "SUCCESS"},
        ],
        "progress_state": {},
    }, None]
    calls = []

    class Repository:
        def queue_items_for_logical_work(self, value):
            assert value == logical_work_id
            return [
                {"queue_id": 1, "ingestion_object_id": 101, "queue_status": "SUCCESS"},
                {"queue_id": 2, "ingestion_object_id": 201, "queue_status": "SUCCESS"},
                {"queue_id": 3, "ingestion_object_id": 301, "queue_status": "SUCCESS"},
            ]

    def process(_repository, **kwargs):
        calls.append(kwargs)
        return outcomes.pop(0)

    monkeypatch.setattr(metadata_runtime_worker, "process_metadata_work_batch", process)
    monkeypatch.setattr(
        metadata_selection,
        "validated_target_metadata_selection",
        lambda _state: SimpleNamespace(repository=Repository()),
    )
    monkeypatch.setattr(pipeline_runtime, "save_checkpoint_state_timed", lambda *_args, **_kwargs: None)
    state = {
        "run_id": "design-run",
        "target_warehouse": "databricks",
        "metadata_runtime_queue": [{"logical_work_id": logical_work_id}],
        "bronze_generation_results": [{"ingestion_object_id": 101, "metadata_activation_status": "ACTIVE"}],
        "silver_generation_results": [{"silver_ingestion_object_id": 201, "metadata_activation_status": "ACTIVE"}],
        "gold_generation_results": [{"gold_ingestion_object_id": 301, "metadata_activation_status": "ACTIVE"}],
    }

    result = pipeline_runtime._execute_queued_metadata_native_runtime(state)

    assert result["status"] == "PIPELINE_COMPLETED"
    assert result["databricks_gold_execution_status"] == "COMPLETED"
    assert len(result["metadata_runtime_results"]) == 3
    assert all(call["logical_work_id"] == logical_work_id for call in calls)


def test_metadata_native_gold_one_of_ten_failure_completes_with_warning(monkeypatch) -> None:
    from types import SimpleNamespace
    from services import metadata_runtime_worker, metadata_selection

    logical_work_id = "logical-partial-gold"
    gold_ids = list(range(301, 311))
    queue_rows = [
        {"queue_id": 1, "ingestion_object_id": 101, "queue_status": "SUCCESS"},
        {"queue_id": 2, "ingestion_object_id": 201, "queue_status": "SUCCESS"},
        *[
            {
                "queue_id": object_id,
                "ingestion_object_id": object_id,
                "queue_status": "FAILED" if object_id == 310 else "SUCCESS",
            }
            for object_id in gold_ids
        ],
    ]
    batch = {
        "outcomes": [
            {
                "queue": row,
                "run": {"run_id": f"runtime-{row['queue_id']}"},
                "status": row["queue_status"],
            }
            for row in queue_rows
        ],
        "progress_state": {},
    }
    batches = [batch, None]

    class Repository:
        def queue_items_for_logical_work(self, value):
            assert value == logical_work_id
            return queue_rows

    monkeypatch.setattr(
        metadata_runtime_worker,
        "process_metadata_work_batch",
        lambda *_args, **_kwargs: batches.pop(0),
    )
    monkeypatch.setattr(
        metadata_selection,
        "validated_target_metadata_selection",
        lambda _state: SimpleNamespace(repository=Repository()),
    )
    monkeypatch.setattr(pipeline_runtime, "save_checkpoint_state_timed", lambda *_args, **_kwargs: None)
    state = {
        "run_id": "design-partial-gold",
        "target_warehouse": "databricks",
        "metadata_runtime_queue": [{"logical_work_id": logical_work_id}],
        "bronze_generation_results": [{"ingestion_object_id": 101, "metadata_activation_status": "ACTIVE"}],
        "silver_generation_results": [{"silver_ingestion_object_id": 201, "metadata_activation_status": "ACTIVE"}],
        "gold_generation_results": [
            {
                "gold_ingestion_object_id": object_id,
                "artifact_kind": "DIMENSION" if object_id <= 304 else "FACT",
                "metadata_activation_status": "ACTIVE",
            }
            for object_id in gold_ids
        ],
    }

    result = pipeline_runtime._execute_queued_metadata_native_runtime(state)

    assert result["status"] == "PIPELINE_COMPLETED"
    assert result["databricks_gold_execution_status"] == "COMPLETED_WITH_WARNINGS"
    assert result["gold_execution_summary"] == {
        "status": "COMPLETED_WITH_WARNINGS",
        "planned_count": 10,
        "successful_count": 9,
        "failed_count": 1,
        "successful_fact_count": 5,
        "successful_dimension_count": 4,
        "failed_object_ids": [310],
        "success_ratio": 0.9,
    }
    assert result["external_execution"]["message"].startswith(
        "Gold completed with warnings: 9/10 tables succeeded"
    )


@pytest.mark.parametrize("layer", ["bronze", "silver"])
@pytest.mark.parametrize("status", ["COMPLETED_WITH_WARNINGS", "SKIPPED", "HANDOFF_ONLY"])
def test_snowflake_upstream_native_layers_require_strict_completion(layer, status):
    assert pipeline_runtime._native_execution_completed(
        {f"snowflake_{layer}_execution_status": status}, "snowflake", layer
    ) is False


def test_snowflake_gold_native_warning_is_complete():
    assert pipeline_runtime._native_execution_completed(
        {"snowflake_gold_execution_status": "COMPLETED_WITH_WARNINGS"},
        "snowflake",
        "gold",
    ) is True


def test_snowflake_native_execution_uses_bounded_parallel_workers(monkeypatch) -> None:
    import threading
    from types import SimpleNamespace
    from services import metadata_runtime_worker, metadata_selection

    logical_work_id = "logical-snowflake-run"
    barrier = threading.Barrier(4)
    worker_ids = []

    class Repository:
        def queue_items_for_logical_work(self, _value):
            return [{"queue_id": 1, "ingestion_object_id": 101, "queue_status": "SUCCESS"}]

    def process(_repository, **kwargs):
        worker_ids.append(kwargs["worker_id"])
        barrier.wait(timeout=2)
        return None

    monkeypatch.setenv("ATHENA_SNOWFLAKE_NATIVE_WORKERS", "4")
    monkeypatch.setattr(metadata_runtime_worker, "process_next_metadata_work", process)
    monkeypatch.setattr(
        metadata_selection,
        "validated_target_metadata_selection",
        lambda _state: SimpleNamespace(repository=Repository()),
    )
    monkeypatch.setattr(pipeline_runtime, "save_checkpoint_state_timed", lambda *_args, **_kwargs: None)
    state = {
        "run_id": "design-snowflake",
        "target_warehouse": "snowflake",
        "metadata_runtime_queue": [{"logical_work_id": logical_work_id}],
        "bronze_generation_results": [
            {"ingestion_object_id": 101, "metadata_activation_status": "ACTIVE"}
        ],
    }

    result = pipeline_runtime._execute_queued_metadata_native_runtime(state)

    assert result["status"] == "PIPELINE_COMPLETED"
    assert len(worker_ids) == 4
    assert len(set(worker_ids)) == 4


def test_snowflake_native_progress_advances_design_run_between_layers() -> None:
    state = {
        "run_id": "design-snowflake",
        "target_warehouse": "snowflake",
        "bronze_generation_results": [
            {"ingestion_object_id": object_id, "metadata_activation_status": "ACTIVE"}
            for object_id in (101, 102)
        ],
        "silver_generation_results": [
            {"silver_ingestion_object_id": object_id, "metadata_activation_status": "ACTIVE"}
            for object_id in (201, 202)
        ],
        "gold_generation_results": [
            {"gold_ingestion_object_id": 301, "metadata_activation_status": "ACTIVE"}
        ],
    }
    outcomes = [
        {"ingestion_object_id": 101, "status": "SUCCESS"},
        {"ingestion_object_id": 102, "status": "SUCCESS"},
    ]

    silver_state, complete = pipeline_runtime._metadata_native_progress_state(state, outcomes)

    assert complete is False
    assert silver_state["snowflake_bronze_execution_status"] == "COMPLETED"
    assert silver_state["snowflake_silver_execution_status"] == "RUNNING"
    assert silver_state["snowflake_gold_execution_status"] == "PENDING"
    assert silver_state["background_stage"] == "silver_code_execution"

    outcomes.extend(
        [
            {"ingestion_object_id": 201, "status": "SUCCESS"},
            {"ingestion_object_id": 202, "status": "RECOVERED_SUCCESS"},
        ]
    )
    gold_state, complete = pipeline_runtime._metadata_native_progress_state(
        silver_state, outcomes
    )

    assert complete is False
    assert gold_state["snowflake_silver_execution_status"] == "COMPLETED"
    assert gold_state["snowflake_gold_execution_status"] == "RUNNING"
    assert gold_state["background_stage"] == "gold_code_execution"


def test_snowflake_native_execution_checkpoints_layer_transitions(monkeypatch) -> None:
    from types import SimpleNamespace
    from services import metadata_runtime_worker, metadata_selection

    logical_work_id = "logical-progress-run"
    outcomes = [
        {"queue": {"queue_id": 1, "ingestion_object_id": 101}, "run": {"run_id": "r1"}, "status": "SUCCESS"},
        {"queue": {"queue_id": 2, "ingestion_object_id": 201}, "run": {"run_id": "r2"}, "status": "SUCCESS"},
        {"queue": {"queue_id": 3, "ingestion_object_id": 301}, "run": {"run_id": "r3"}, "status": "SUCCESS"},
        None,
    ]
    saved = []

    class Repository:
        def queue_items_for_logical_work(self, _value):
            return [
                {"queue_id": queue_id, "ingestion_object_id": object_id, "queue_status": "SUCCESS"}
                for queue_id, object_id in ((1, 101), (2, 201), (3, 301))
            ]

    monkeypatch.setenv("ATHENA_SNOWFLAKE_NATIVE_WORKERS", "1")
    monkeypatch.setattr(
        metadata_runtime_worker,
        "process_next_metadata_work",
        lambda *_args, **_kwargs: outcomes.pop(0),
    )
    monkeypatch.setattr(
        metadata_selection,
        "validated_target_metadata_selection",
        lambda _state: SimpleNamespace(repository=Repository()),
    )
    monkeypatch.setattr(
        pipeline_runtime,
        "save_checkpoint_state_timed",
        lambda _run_id, checkpoint, **kwargs: saved.append((dict(checkpoint), kwargs.get("context"))),
    )
    state = {
        "run_id": "design-progress",
        "target_warehouse": "snowflake",
        "metadata_runtime_queue": [{"logical_work_id": logical_work_id}],
        "bronze_generation_results": [{"ingestion_object_id": 101, "metadata_activation_status": "ACTIVE"}],
        "silver_generation_results": [{"silver_ingestion_object_id": 201, "metadata_activation_status": "ACTIVE"}],
        "gold_generation_results": [{"gold_ingestion_object_id": 301, "metadata_activation_status": "ACTIVE"}],
    }

    pipeline_runtime._execute_queued_metadata_native_runtime(state)

    progress = [checkpoint for checkpoint, context in saved if context == "metadata_runtime:progress"]
    assert [checkpoint["background_stage"] for checkpoint in progress] == [
        "silver_code_execution",
        "gold_code_execution",
    ]
    assert progress[0]["snowflake_bronze_execution_status"] == "COMPLETED"
    assert progress[1]["snowflake_silver_execution_status"] == "COMPLETED"


def test_metadata_native_execution_fails_closed_when_an_active_artifact_is_missing(monkeypatch) -> None:
    from types import SimpleNamespace
    from services import metadata_runtime_worker, metadata_selection

    class Repository:
        def queue_items_for_logical_work(self, _value):
            return [{"queue_id": 1, "ingestion_object_id": 101, "queue_status": "SUCCESS"}]

    monkeypatch.setattr(metadata_runtime_worker, "process_metadata_work_batch", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        metadata_selection,
        "validated_target_metadata_selection",
        lambda _state: SimpleNamespace(repository=Repository()),
    )
    state = {
        "run_id": "design-run",
        "target_warehouse": "databricks",
        "metadata_runtime_queue": [{"logical_work_id": "logical-design-run"}],
        "bronze_generation_results": [{"ingestion_object_id": 101, "metadata_activation_status": "ACTIVE"}],
        "silver_generation_results": [{"silver_ingestion_object_id": 201, "metadata_activation_status": "ACTIVE"}],
    }

    with pytest.raises(RuntimeError, match="every active artifact"):
        pipeline_runtime._execute_queued_metadata_native_runtime(state)


def test_failed_kpi_artifact_does_not_open_empty_gate1():
    context = pipeline_runtime.build_pipeline_steps(
        source="database",
        checkpoint={"status": "FAILED", "failed_background_stage": "kpis"},
        summary=[{"artifact_type": "KPIS", "faithfulness_status": "FAILED"}],
        pending_gate1=[],
        completed_gate1=[],
        nominated_tables=[],
        certified_tables=[],
        enriched_payload={},
        gate3_payload={},
        bronze_generation_completed=False,
        silver_generation_completed=False,
        gold_generation_completed=False,
    )

    kpis = next(step for step in context if step["key"] == "kpis")
    gate1 = next(step for step in context if step["key"] == "gate1")
    assert kpis["state"] == "FAILED"
    assert gate1["state"] == "PENDING"


def test_silver_merge_key_resolution_node_builds_review_input():
    from nodes.silver_merge_key_resolution import silver_merge_key_resolution_node

    result = silver_merge_key_resolution_node({
        "run_id": "run-merge-keys",
        "bronze_review_artifact": {
            "feeds": [{"table": "claims", "primary_keys": ["claim_id"]}],
        },
    })

    assert result["silver_merge_key_resolution_status"] == "COMPLETED"
    assert result["silver_merge_key_resolution_artifact"]["feeds"][0]["merge_keys"] == ["claim_id"]
    assert result["silver_merge_key_resolution_artifact"]["feeds"][0]["review_status"] == "PENDING"


def test_silver_merge_key_resolution_derives_certified_keys_and_candidates():
    from nodes.silver_merge_key_resolution import silver_merge_key_resolution_node

    result = silver_merge_key_resolution_node({
        "run_id": "run-derived-merge-keys",
        "bronze_review_artifact": {
            "feeds": [
                {"table": "claims"},
                {"table": "claim_lines"},
            ],
        },
        "enriched_columns": [
            {"table_name": "claims", "column_name": "ClaimID", "is_primary_key": True, "is_join_key": True},
            {"table_name": "claims", "column_name": "PolicyID", "is_primary_key": False, "is_join_key": True},
            {"table_name": "claim_lines", "column_name": "ClaimID", "is_primary_key": False, "is_join_key": True},
        ],
    })

    artifact = result["silver_merge_key_resolution_artifact"]
    claims, claim_lines = artifact["feeds"]
    assert claims["merge_keys"] == ["ClaimID"]
    assert claims["merge_key_candidates"] == ["ClaimID", "PolicyID"]
    assert claims["merge_key_source"] == "semantic_enrichment_primary_key"
    assert claim_lines["merge_keys"] == []
    assert claim_lines["merge_key_candidates"] == ["ClaimID"]
    assert claim_lines["merge_key_resolution_status"] == "REVIEW_REQUIRED"
    assert artifact["resolved_count"] == 1
    assert artifact["review_required_count"] == 1


def test_file_merge_key_resolution_matches_semantic_entity():
    from nodes.silver_merge_key_resolution import silver_merge_key_resolution_node

    result = silver_merge_key_resolution_node({
        "run_id": "run-file-merge-keys",
        "source": "adls_gen2",
        "bronze_review_artifact": {
            "feeds": [{"feed_id": "insurance_claims", "entity": "claims"}],
        },
        "enriched_metadata": {
            "columns": [
                {"feed_id": "insurance_claims", "entity": "claims", "column_name": "ClaimID", "is_primary_key": True},
                {"feed_id": "insurance_claims", "entity": "claims", "column_name": "PolicyID", "is_join_key": True},
            ],
        },
    })

    feed = result["silver_merge_key_resolution_artifact"]["feeds"][0]
    assert feed["merge_keys"] == ["ClaimID"]
    assert feed["merge_key_candidates"] == ["ClaimID", "PolicyID"]


def test_reviewed_file_merge_keys_update_bronze_config():
    from services import pipeline_runtime

    results = pipeline_runtime._apply_reviewed_keys_to_bronze_results(
        [{
            "feed_id": "insurance_claims",
            "entity": "claims",
            "bronze_config": {"primary_keys": ["OldID"]},
        }],
        {"feeds": [{"entity": "claims", "merge_keys": ["ClaimID", "LineID"]}]},
    )

    assert results[0]["primary_keys"] == ["ClaimID", "LineID"]
    assert results[0]["bronze_config"]["primary_keys"] == ["ClaimID", "LineID"]


def test_silver_merge_key_review_rebuilds_legacy_empty_artifact():
    from services import pipeline_runtime

    artifact = pipeline_runtime._silver_merge_key_review_artifact({
        "run_id": "run-legacy-merge-keys",
        "bronze_review_artifact": {"feeds": [{"table": "claims"}]},
        "silver_merge_key_review_artifact": {
            "feeds": [{"table": "claims", "merge_keys": [], "primary_keys": []}],
        },
        "enriched_columns": [
            {"table_name": "claims", "column_name": "ClaimID", "is_primary_key": True},
        ],
    })

    assert artifact["feeds"][0]["merge_keys"] == ["ClaimID"]
    assert artifact["feeds"][0]["merge_key_source"] == "semantic_enrichment_primary_key"


def test_minimum_stage_runtime_uses_env(monkeypatch):
    from services import pipeline_runtime

    monkeypatch.setenv("ATHENA_MIN_STAGE_RUNTIME_SECONDS", "2.5")

    assert pipeline_runtime._minimum_stage_runtime_seconds() == 2.5


def test_minimum_stage_runtime_falls_back_for_bad_env(monkeypatch):
    from services import pipeline_runtime

    monkeypatch.setenv("ATHENA_MIN_STAGE_RUNTIME_SECONDS", "bad")

    assert pipeline_runtime._minimum_stage_runtime_seconds() == 10.0


def test_minimum_stage_runtime_skips_profiling_reviews_and_failures(monkeypatch):
    from services import pipeline_runtime

    sleeps = []
    monkeypatch.setenv("ATHENA_MIN_STAGE_RUNTIME_SECONDS", "10")
    monkeypatch.setattr(pipeline_runtime.time, "sleep", sleeps.append)
    monkeypatch.setattr(pipeline_runtime.time, "monotonic", lambda: 5.0)

    pipeline_runtime.wait_for_minimum_stage_runtime("requirements", 2.0, {"status": "RUNNING"})
    pipeline_runtime.wait_for_minimum_stage_runtime("profiling", 2.0, {"status": "RUNNING"})
    pipeline_runtime.wait_for_minimum_stage_runtime("gate1", 2.0, {"status": "RUNNING"})
    pipeline_runtime.wait_for_minimum_stage_runtime("enrichment", 2.0, {"status": "FAILED"})

    assert sleeps == [7.0]


def test_visible_stage_checkpoints_completion_before_wait(monkeypatch):
    from services import pipeline_runtime

    events = []
    monkeypatch.setattr(
        pipeline_runtime,
        "save_checkpoint_state_timed",
        lambda run_id, state, context: events.append((context, state.get("background_stage"))),
    )
    monkeypatch.setattr(
        pipeline_runtime,
        "wait_for_minimum_stage_runtime",
        lambda stage, started, state: events.append(("wait", state.get("background_stage"))),
    )

    result = pipeline_runtime.run_with_minimum_stage_runtime(
        "requirements",
        lambda state: {**state, "requirement_status": "COMPLETED"},
        {"run_id": "run-visible"},
    )

    assert events == [
        ("requirements:running", "requirements"),
        ("requirements:complete", None),
        ("wait", None),
    ]
    assert result["requirement_status"] == "COMPLETED"


def test_visible_stage_uses_file_source_labels(monkeypatch):
    from services import pipeline_runtime

    saved = []
    monkeypatch.setattr(
        pipeline_runtime,
        "save_checkpoint_state_timed",
        lambda run_id, state, context: saved.append(dict(state)),
    )
    monkeypatch.setattr(pipeline_runtime, "wait_for_minimum_stage_runtime", lambda *args, **kwargs: None)

    pipeline_runtime.run_with_minimum_stage_runtime(
        "discovery",
        lambda state: state,
        {"run_id": "run-adls", "source": "adls_gen2"},
    )

    assert saved[0]["resume_message"] == "Discover Source Objects is running."


def test_load_checkpoint_fields_uses_json_value_projection(monkeypatch):
    from services import pipeline_runtime

    recorded = {}

    class StubCursor:
        def execute(self, query, params):
            recorded["query"] = query
            recorded["params"] = params

        def fetchone(self):
            return ("database", "RUNNING")

    class StubConnection:
        def cursor(self):
            return StubCursor()

        def close(self):
            recorded["closed"] = True

    monkeypatch.setattr(pipeline_runtime, "get_connection", lambda: StubConnection())

    fields = pipeline_runtime.load_checkpoint_fields("run-fast", "source", "status")

    assert fields == {"source": "database", "status": "RUNNING"}
    assert "JSON_VALUE(full_state_json, '$.source')" in recorded["query"]
    assert "JSON_VALUE(full_state_json, '$.status')" in recorded["query"]
    assert recorded["params"] == ("run-fast",)
    assert recorded["closed"] is True


def test_checkpoint_persistence_redacts_nested_credentials_and_url_userinfo(monkeypatch):
    recorded = {}

    class StubCursor:
        def execute(self, query, parameters):
            recorded["parameters"] = parameters

    class StubConnection:
        def cursor(self):
            return StubCursor()

        def commit(self):
            recorded["committed"] = True

        def close(self):
            recorded["closed"] = True

    monkeypatch.setattr(pipeline_runtime, "get_connection", lambda: StubConnection())

    pipeline_runtime.save_checkpoint_state(
        "run-secret",
        {
            "status": "FAILED",
            "error": "https://source-user:source-password@example.test/path?token=token-value",
            "nested": {"client_secret": "client-value"},
            "source_runtime_connection": {
                "secrets": {
                    "username": {"scope": "source-scope", "key": "claims-user"},
                    "password": {"scope": "source-scope", "key": "claims-password"},
                }
            },
        },
    )

    persisted = json.loads(recorded["parameters"][1])
    rendered = json.dumps(persisted)
    assert "source-user" not in rendered
    assert "source-password" not in rendered
    assert "token-value" not in rendered
    assert "client-value" not in rendered
    assert rendered.count("[REDACTED]") >= 3
    assert persisted["source_runtime_connection"]["secrets"]["password"] == {
        "scope": "source-scope",
        "key": "claims-password",
    }
    assert recorded["committed"] is True
    assert recorded["closed"] is True


def test_list_runs_uses_lightweight_run_registry(monkeypatch):
    recorded = {"queries": []}

    class StubCursor:
        timeout = None

        def execute(self, query, *parameters):
            recorded["queries"].append(query)

        def fetchall(self):
            return [("run-fast", "2026-07-14T18:00:00")]

    class StubConnection:
        def cursor(self):
            return StubCursor()

        def close(self):
            recorded["closed"] = True

    monkeypatch.setattr(pipeline_runtime, "get_connection", lambda: StubConnection())

    runs = pipeline_runtime.list_runs(10)

    assert runs == [{
        "run_id": "run-fast",
        "last_activity": "2026-07-14T18:00:00",
        "checkpoint": {},
    }]
    assert "ORDER BY checkpoint_at" not in recorded["queries"][0]
    assert "brd_run_registry" in recorded["queries"][0]
    assert "kpi_checkpoints" not in recorded["queries"][0]
    assert all("ai_store" not in query for query in recorded["queries"])
    assert recorded["closed"] is True


def test_list_runs_filters_owner_before_top_limit(monkeypatch):
    recorded = {"queries": [], "parameters": []}

    class StubCursor:
        timeout = None

        def execute(self, query, *parameters):
            recorded["queries"].append(query)
            recorded["parameters"].append(parameters)

        def fetchall(self):
            if len(recorded["queries"]) == 1:
                return [("owned-run", "2026-07-27T10:00:00")]
            return [("owned-run", *([None] * 34))]

    class StubConnection:
        def cursor(self):
            return StubCursor()

        def close(self):
            pass

    monkeypatch.setattr(pipeline_runtime, "get_connection", lambda: StubConnection())

    runs = pipeline_runtime.list_runs(10, owner_email=" Client@Example.com ")

    assert runs[0]["run_id"] == "owned-run"
    assert "LOWER(COALESCE" in recorded["queries"][0]
    assert recorded["parameters"][0] == ("client@example.com",)


def test_list_runs_does_not_hydrate_checkpoint_payloads(monkeypatch):
    calls = []

    class StubCursor:
        timeout = None

        def execute(self, query, *parameters):
            calls.append(query)

        def fetchall(self):
            return [("run-visible", "2026-07-30T04:50:00")]

    class StubConnection:
        def cursor(self):
            return StubCursor()

        def close(self):
            pass

    monkeypatch.setattr(pipeline_runtime, "get_connection", lambda: StubConnection())

    runs = pipeline_runtime.list_runs(10)

    assert runs == [{
        "run_id": "run-visible",
        "last_activity": "2026-07-30T04:50:00",
        "checkpoint": {},
    }]
    assert "ORDER BY checkpoint_at" not in calls[0]
    assert len(calls) == 1
    assert "JSON_VALUE" not in calls[0]


def test_run_pipeline_background_database_flow_saves_completed(monkeypatch):
    saved = {}

    monkeypatch.setattr(pipeline_service, "load_checkpoint_state", lambda run_id: {"existing": True})
    monkeypatch.setattr(
        pipeline_service,
        "start_pipeline",
        lambda **kwargs: {"result": {"status": "COMPLETED", "source": "database", "payload": "ok"}},
    )
    monkeypatch.setattr(pipeline_service.api_utils, "is_file_source", lambda source: False)
    monkeypatch.setattr(pipeline_service, "get_pending_items", lambda run_id, gate: [{"id": "pending"}])
    monkeypatch.setattr(
        pipeline_service,
        "save_checkpoint_state",
        lambda run_id, state: saved.update({"run_id": run_id, "state": state}),
    )

    pipeline_service.run_pipeline_background(
        run_id="run-1",
        brd_text="brd",
        brd_filename="Customer BRD",
        source="database",
        source_databases=["db1"],
        sftp_entity="transactions",
        use_domain_kb=True,
        stage_confirmation_enabled=True,
    )

    assert saved["run_id"] == "run-1"
    assert saved["state"]["status"] == "COMPLETED"
    assert saved["state"]["payload"] == "ok"
    assert saved["state"]["brd_filename"] == "Customer BRD"


def test_run_pipeline_background_does_not_overwrite_abort(monkeypatch):
    saved = {}

    monkeypatch.setattr(pipeline_service, "load_checkpoint_state", lambda run_id: {"run_id": run_id, "status": "RUNNING"})

    def finish_after_abort(**kwargs):
        pipeline_runtime.ABORTED_RUNS.add(kwargs["run_id"])
        return {"result": {"status": "COMPLETED", "source": "database"}}

    monkeypatch.setattr(pipeline_service, "start_pipeline", finish_after_abort)
    monkeypatch.setattr(pipeline_service.api_utils, "is_file_source", lambda source: False)
    monkeypatch.setattr(
        pipeline_service,
        "save_checkpoint_state",
        lambda run_id, state: saved.update({"run_id": run_id, "state": state}),
    )

    try:
        pipeline_service.run_pipeline_background(
            run_id="run-abort-finished",
            brd_text="brd",
            brd_filename="Abort BRD",
            source="database",
            source_databases=None,
            sftp_entity="transactions",
            use_domain_kb=False,
            stage_confirmation_enabled=False,
        )
    finally:
        pipeline_runtime.clear_run_abort("run-abort-finished")

    assert saved["state"]["status"] == "ABORTED"
    assert saved["state"]["abort_requested"] is True


def test_run_pipeline_background_file_source_keeps_completed(monkeypatch):
    saved = {}

    monkeypatch.setattr(pipeline_service, "load_checkpoint_state", lambda run_id: {})
    monkeypatch.setattr(
        pipeline_service,
        "start_sftp_pipeline",
        lambda **kwargs: {"result": {"status": "COMPLETED", "source": "sftp"}},
    )
    monkeypatch.setattr(pipeline_service.api_utils, "is_file_source", lambda source: str(source).lower() == "sftp")
    monkeypatch.setattr(pipeline_service, "get_pending_items", lambda run_id, gate: [])
    monkeypatch.setattr(
        pipeline_service,
        "save_checkpoint_state",
        lambda run_id, state: saved.update({"state": state}),
    )

    pipeline_service.run_pipeline_background(
        run_id="run-2",
        brd_text="",
        brd_filename="File BRD",
        source="sftp",
        source_databases=None,
        sftp_entity="transactions",
        use_domain_kb=False,
        stage_confirmation_enabled=False,
    )

    assert saved["state"]["status"] == "COMPLETED"
    assert saved["state"]["source"] == "sftp"
    assert saved["state"]["brd_filename"] == "File BRD"


def test_run_pipeline_background_marks_failure(monkeypatch):
    failure = {}

    monkeypatch.setattr(pipeline_service, "load_checkpoint_state", lambda run_id: {"run_id": run_id})
    monkeypatch.setattr(
        pipeline_service,
        "start_pipeline",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    monkeypatch.setattr(pipeline_service.api_utils, "is_file_source", lambda source: False)
    monkeypatch.setattr(
        pipeline_service,
        "_mark_run_failed",
        lambda run_id, exc, stage: failure.update({"run_id": run_id, "error": str(exc), "stage": stage}),
    )

    with pytest.raises(RuntimeError, match="boom"):
        pipeline_service.run_pipeline_background(
            run_id="run-3",
            brd_text="brd",
            brd_filename="Broken BRD",
            source="database",
            source_databases=None,
            sftp_entity="transactions",
            use_domain_kb=False,
            stage_confirmation_enabled=True,
        )

    assert failure == {"run_id": "run-3", "error": "boom", "stage": "pipeline"}


def test_submit_pipeline_start_rejects_duplicate(monkeypatch):
    class PendingFuture:
        def done(self):
            return False

    payload = PipelineRunRequest(brd_text="brd", source="database")
    monkeypatch.setitem(pipeline_service.BACKGROUND_JOBS, "run-dup:pipeline", PendingFuture())

    try:
        with pytest.raises(HTTPException) as exc:
            pipeline_service.submit_pipeline_start("run-dup", payload)
        assert exc.value.status_code == 409
    finally:
        pipeline_service.BACKGROUND_JOBS.pop("run-dup:pipeline", None)


def test_submit_pipeline_start_rejects_when_background_capacity_full(monkeypatch):
    class PendingFuture:
        def done(self):
            return False

    keys = ["run-a:pipeline", "run-b:pipeline"]
    payload = PipelineRunRequest(brd_text="brd", source="database")
    monkeypatch.setattr(pipeline_runtime, "BACKGROUND_WORKER_COUNT", 2)
    for key in keys:
        monkeypatch.setitem(pipeline_service.BACKGROUND_JOBS, key, PendingFuture())

    try:
        with pytest.raises(HTTPException) as exc:
            pipeline_service.submit_pipeline_start("run-c", payload)
        assert exc.value.status_code == 429
    finally:
        for key in keys:
            pipeline_service.BACKGROUND_JOBS.pop(key, None)


def test_submit_pipeline_start_submits_and_registers_callback(monkeypatch):
    recorded = {}

    class StubFuture:
        def add_done_callback(self, callback):
            recorded["callback"] = callback

        def done(self):
            return False

    class StubExecutor:
        def submit(self, fn, **kwargs):
            recorded["fn"] = fn
            recorded["kwargs"] = kwargs
            return StubFuture()

    monkeypatch.setattr(pipeline_service, "BACKGROUND_EXECUTOR", StubExecutor())
    monkeypatch.setattr(pipeline_service.api_utils, "normalize_file_entity", lambda source, entity: "transactions")
    monkeypatch.setattr(pipeline_service.api_utils, "is_file_source", lambda source: False)

    payload = PipelineRunRequest(
        brd_text="brd",
        brd_filename="Claims BRD",
        source="database",
        database_name="db1",
        target_warehouse="snowflake",
        execution_engine="dbt",
        dbt_project_object_name="CLAIMS_DBT",
        dbt_target_name="astra_snowflake",
        dbt_threads=6,
        dbt_command_timeout_secs=900,
        compliance_enabled=True,
        compliance_domain="Insurance",
        compliance_countries=["US", "AU"],
    )
    pipeline_service.submit_pipeline_start("run-submit", payload)

    assert recorded["fn"] == pipeline_service.run_pipeline_background
    assert recorded["kwargs"]["run_id"] == "run-submit"
    assert recorded["kwargs"]["brd_filename"] == "Claims BRD"
    assert recorded["kwargs"]["source_databases"] == ["db1"]
    assert recorded["kwargs"]["stage_confirmation_enabled"] is False
    assert recorded["kwargs"]["target_warehouse"] == "snowflake"
    assert recorded["kwargs"]["execution_engine"] == "dbt"
    assert recorded["kwargs"]["dbt_deployment_mode"] == "generate_only"
    assert recorded["kwargs"]["dbt_project_object_name"] == "CLAIMS_DBT"
    assert recorded["kwargs"]["dbt_target_name"] == "astra_snowflake"
    assert recorded["kwargs"]["dbt_threads"] == 6
    assert recorded["kwargs"]["dbt_command_timeout_secs"] == 900
    assert recorded["kwargs"]["force_dbt_deploy"] is False
    assert recorded["kwargs"]["compliance_enabled"] is True
    assert recorded["kwargs"]["compliance_domain"] == "Insurance"
    assert recorded["kwargs"]["compliance_countries"] == ["US", "AU"]
    assert callable(recorded["callback"])
    pipeline_service.BACKGROUND_JOBS.pop("run-submit:pipeline", None)


def test_seed_payload_from_checkpoint_restores_dbt_generation_config():
    payload = pipeline_service.seed_payload_from_checkpoint(
        {
            "run_id": "run-restart",
            "project_id": "project-dbt",
            "brd_text": "requirements",
            "source": "database",
            "source_databases": ["insurance"],
            "target_warehouse": "snowflake",
            "execution_engine": "dbt",
            "dbt_deployment_mode": "generate_only",
            "dbt_project_object_name": "CLAIMS_DBT",
            "dbt_target_name": "astra_snowflake",
            "dbt_threads": 6,
            "dbt_command_timeout_secs": 900,
            "force_dbt_deploy": False,
        }
    )

    assert payload.project_id == "project-dbt"
    assert payload.database_name == "insurance"
    assert payload.target_warehouse == "snowflake"
    assert payload.execution_engine == "dbt"
    assert payload.dbt_deployment_mode == "generate_only"
    assert payload.dbt_project_object_name == "CLAIMS_DBT"
    assert payload.dbt_target_name == "astra_snowflake"
    assert payload.dbt_threads == 6
    assert payload.dbt_command_timeout_secs == 900
    assert payload.force_dbt_deploy is False


def test_continue_file_pipeline_job_rejects_invalid_state(monkeypatch):
    class BadGraph:
        def invoke(self, state):
            return "bad"

    monkeypatch.setattr(pipeline_service, "source_ingestion_graph", lambda: BadGraph())

    with pytest.raises(ValueError, match="invalid state"):
        pipeline_service.continue_file_pipeline_job("run-4", {"foo": "bar"})


def test_continue_database_dbt_codegen_reuses_saved_gold_review(monkeypatch):
    recorded = {}
    review_artifact = {"items": [{"script_key": "gold-model", "review_status": "APPROVED"}]}

    monkeypatch.setattr(
        pipeline_service,
        "submit_gold_review",
        lambda run_id, action, review_artifact: recorded.update(
            {
                "run_id": run_id,
                "action": action,
                "review_artifact": review_artifact,
            }
        )
        or {"status": "COMPLETED"},
    )
    monkeypatch.setattr(
        pipeline_service,
        "continue_database_pipeline",
        lambda *args, **kwargs: pytest.fail("dbt finalization must not rerun Gold generation"),
    )

    result = pipeline_service.continue_database_pipeline_job(
        "run-dbt-retry",
        "snowflake_dbt_codegen",
        {"gold_review_artifact": review_artifact},
    )

    assert result == {"status": "COMPLETED"}
    assert recorded == {
        "run_id": "run-dbt-retry",
        "action": "APPROVED",
        "review_artifact": review_artifact,
    }


def test_continue_generation_first_dbt_routes_gate_to_deployment(monkeypatch):
    state = {
        "run_id": "run-dbt-gate",
        "source": "database",
        "target_warehouse": "snowflake",
        "execution_engine": "dbt",
        "database_flow_version": "generation_first_v1",
    }
    recorded = {}
    monkeypatch.setattr(
        pipeline_service,
        "execute_generation_first_snowflake_dbt",
        lambda run_id, state: recorded.update({"run_id": run_id, "state": state}) or {"status": "PIPELINE_COMPLETED"},
    )
    monkeypatch.setattr(
        pipeline_service,
        "continue_database_pipeline",
        lambda *args, **kwargs: pytest.fail("the deployment gate must not re-enter generation"),
    )

    result = pipeline_service.continue_database_pipeline_job(
        state["run_id"],
        "gold_code_execution",
        state,
    )

    assert result == {"status": "PIPELINE_COMPLETED"}
    assert recorded == {"run_id": state["run_id"], "state": state}


def test_database_failed_stage_key_uses_context_fallback(monkeypatch):
    monkeypatch.setattr(
        pipeline_service,
        "get_run_context",
        lambda run_id: {"pipeline_steps": [{"key": "silver", "state": "FAILED"}]},
    )

    result = pipeline_service.database_failed_stage_key("run-5", {})

    assert result == "silver"


def test_database_failed_stage_key_maps_external_gold_execution_to_gold():
    assert pipeline_service.database_failed_stage_key(
        "run-gold-failed",
        {"failed_background_stage": "gold_code_execution"},
    ) == "gold"


def test_database_failed_stage_key_preserves_generation_first_dbt_deployment():
    assert pipeline_service.database_failed_stage_key(
        "run-dbt-deploy-failed",
        {
            "source": "database",
            "target_warehouse": "snowflake",
            "execution_engine": "dbt",
            "database_flow_version": "generation_first_v1",
            "failed_background_stage": "gold_code_execution",
        },
    ) == "gold_code_execution"


def test_database_failed_stage_key_preserves_snowflake_dbt_codegen():
    assert pipeline_service.database_failed_stage_key(
        "run-dbt-failed",
        {"failed_background_stage": "snowflake_dbt_codegen"},
    ) == "snowflake_dbt_codegen"


def test_database_failed_stage_key_maps_stale_silver_execution_to_gold_when_gold_exists():
    assert pipeline_service.database_failed_stage_key(
        "run-gold-failed",
        {
            "next_stage_key": "silver_code_execution",
            "gold_generation_completed": True,
        },
    ) == "gold"


def test_build_pipeline_steps_keeps_active_ingestion_running():
    from services import pipeline_runtime

    steps = pipeline_runtime.build_pipeline_steps(
        source="database",
        checkpoint={
            "status": "PROCESSING",
            "background_stage": "ingestion",
            "brd_text": "partial brd text already saved",
            "fingerprint": "partial-fingerprint",
        },
        summary=[],
        pending_gate1=[],
        completed_gate1=[],
        nominated_tables=[],
        certified_tables=[],
        enriched_payload={},
        gate3_payload={},
        bronze_generation_completed=False,
        silver_generation_completed=False,
        gold_generation_completed=False,
    )

    by_key = {step["key"]: step for step in steps}
    assert by_key["ingestion"]["state"] == "RUNNING"
    assert by_key["memory"]["state"] == "PENDING"


def test_active_bronze_execution_hides_stale_downstream_completion():
    from services import pipeline_runtime

    steps = pipeline_runtime.build_pipeline_steps(
        source="database",
        checkpoint={
            "status": "RUNNING",
            "target_warehouse": "snowflake",
            "background_stage": "bronze_code_execution",
            "snowflake_bronze_execution_status": "RUNNING",
            "silver_generation_status": "COMPLETED",
            "snowflake_silver_execution_status": "COMPLETED",
            "gold_generation_status": "COMPLETED",
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

    by_key = {step["key"]: step for step in steps}
    assert by_key["bronze_code_execution"]["state"] == "RUNNING"
    assert by_key["silver"]["state"] == "PENDING"
    assert by_key["gold_code_execution"]["state"] == "PENDING"


def test_databricks_gate4_does_not_mark_merge_key_review_complete():
    steps = pipeline_runtime.build_pipeline_steps(
        source="database",
        checkpoint={
            "status": "HITL_WAIT",
            "target_warehouse": "databricks",
            "bronze_review_decision": "APPROVED",
            "next_review_key": "silver_merge_key_review",
        },
        summary=[],
        pending_gate1=[],
        completed_gate1=[],
        nominated_tables=[],
        certified_tables=[],
        enriched_payload={},
        gate3_payload={},
        bronze_generation_completed=True,
        silver_generation_completed=False,
        gold_generation_completed=False,
    )

    by_key = {step["key"]: step for step in steps}
    assert by_key["silver_merge_key_resolution"]["state"] == "PENDING"
    assert by_key["silver_merge_key_review"]["state"] == "PENDING"
    assert by_key["silver"]["state"] == "PENDING"


def test_merge_key_resolution_completes_only_after_resolver_artifact_exists():
    steps = pipeline_runtime.build_pipeline_steps(
        source="database",
        checkpoint={
            "status": "HITL_WAIT",
            "bronze_review_decision": "APPROVED",
            "silver_merge_key_resolution_status": "COMPLETED",
            "silver_merge_key_resolution_artifact": {"feeds": [{"table": "claims", "merge_keys": ["ClaimID"]}]},
            "next_review_key": "silver_merge_key_review",
        },
        summary=[],
        pending_gate1=[],
        completed_gate1=[],
        nominated_tables=[],
        certified_tables=[],
        enriched_payload={},
        gate3_payload={},
        bronze_generation_completed=True,
        silver_generation_completed=False,
        gold_generation_completed=False,
    )
    steps = pipeline_runtime.apply_waiting_stage_state(steps, "silver_merge_key_review")

    by_key = {step["key"]: step for step in steps}
    assert by_key["silver_merge_key_resolution"]["state"] == "COMPLETED"
    assert by_key["silver_merge_key_review"]["state"] == "HITL_WAIT"


def test_merge_key_resolution_pauses_with_reviewable_artifact(monkeypatch):
    from nodes import silver_merge_key_resolution

    monkeypatch.setattr(
        silver_merge_key_resolution,
        "silver_merge_key_resolution_node",
        lambda state: {
            **state,
            "silver_merge_key_resolution_status": "COMPLETED",
            "silver_merge_key_resolution_artifact": {
                "feeds": [{"table": "claims", "merge_keys": ["claim_id"]}],
            },
        },
    )

    result = pipeline_runtime._pause_for_silver_merge_key_review(
        "run-auto-merge-keys",
        {
            "enriched_metadata": {
                "columns": [{"table_name": "claims", "column_name": "claim_id"}],
            },
        },
    )

    assert result["status"] == "HITL_WAIT"
    assert result["next_review_key"] == "silver_merge_key_review"
    assert result["silver_merge_key_review_decision"] is None
    assert result["gate_silver_merge_key_review"]["status"] == "PENDING"
    assert result["silver_merge_key_review_artifact"]["feeds"][0]["merge_keys"] == ["claim_id"]


def test_databricks_gold_generation_does_not_imply_execution_completion():
    from services import pipeline_runtime

    steps = pipeline_runtime.build_pipeline_steps(
        source="sftp",
        checkpoint={
            "status": "RUNNING",
            "target_warehouse": "databricks",
            "gold_generation_status": "COMPLETED",
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
    assert by_key["gold"]["state"] == "COMPLETED"
    assert by_key["gold_code_execution"]["state"] == "PENDING"


def test_databricks_gold_partial_success_completes_execution_step():
    from services import pipeline_runtime

    steps = pipeline_runtime.build_pipeline_steps(
        source="database",
        checkpoint={
            "status": "PIPELINE_COMPLETED",
            "target_warehouse": "databricks",
            "databricks_gold_execution_status": "COMPLETED_WITH_WARNINGS",
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

    gold_execution = next(step for step in steps if step["key"] == "gold_code_execution")
    assert gold_execution["complete"] is True
    assert gold_execution["state"] == "COMPLETED"


def test_file_source_pipeline_steps_match_the_six_ui_phases():
    from services import pipeline_runtime

    steps = pipeline_runtime.build_pipeline_steps(
        source="adls_gen2",
        checkpoint={
            "status": "RUNNING",
            "target_warehouse": "databricks",
            "background_stage": "bronze_code_execution",
            "databricks_bronze_execution_status": "RUNNING",
        },
        summary=[],
        pending_gate1=[],
        completed_gate1=[],
        nominated_tables=[],
        certified_tables=[],
        enriched_payload={},
        gate3_payload={},
        bronze_generation_completed=True,
        silver_generation_completed=False,
        gold_generation_completed=False,
    )

    keys = [step["key"] for step in steps]
    assert keys == [
        "ingestion", "memory", "requirements", "kpis", "gate1",
        "discovery", "nomination", "gate2", "schema", "profiling", "enrichment", "gate3",
        "pre_bronze_bootstrap_metadata", "plan_seal", "plan_freshness",
        "pre_bronze_metadata_codegen", "bronze", "gate4",
        "bronze_code_execution", "bronze_runtime_validation",
        "silver_merge_key_resolution", "silver_merge_key_review", "silver", "gate5",
        "silver_code_execution", "silver_runtime_validation",
        "gold", "gold_review", "gold_code_execution", "gold_runtime_validation",
        "final_publish", "finalize",
    ]
    by_key = {step["key"]: step for step in steps}
    assert by_key["bronze_code_execution"]["state"] == "RUNNING"
    assert by_key["bronze_runtime_validation"]["state"] == "PENDING"
    assert by_key["gold_code_execution"]["state"] == "PENDING"


def test_sftp_pull_does_not_count_as_databricks_bronze_execution():
    from services import pipeline_runtime

    steps = pipeline_runtime.build_pipeline_steps(
        source="sftp",
        checkpoint={
            "status": "RUNNING",
            "target_warehouse": "databricks",
            "sftp_pull_status": "COMPLETED",
            "bronze_ingestion_status": "COMPLETED",
        },
        summary=[],
        pending_gate1=[],
        completed_gate1=[],
        nominated_tables=[],
        certified_tables=[],
        enriched_payload={},
        gate3_payload={},
        bronze_generation_completed=True,
        silver_generation_completed=False,
        gold_generation_completed=False,
    )

    by_key = {step["key"]: step for step in steps}
    assert by_key["bronze_code_execution"]["state"] == "PENDING"


def test_later_stage_cannot_infer_bronze_execution_completion():
    from services import pipeline_runtime

    steps = pipeline_runtime.build_pipeline_steps(
        source="database",
        checkpoint={
            "status": "RUNNING",
            "target_warehouse": "snowflake",
            "background_stage": "silver_code_execution",
            "snowflake_silver_execution_status": "RUNNING",
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
        gold_generation_completed=False,
    )

    by_key = {step["key"]: step for step in steps}
    assert by_key["bronze_code_execution"]["state"] == "PENDING"


def test_review_artifacts_do_not_count_as_generated_or_executed_silver():
    from services.pipeline_runtime import generation_completed

    summary = [
        {"stage": "Silver Merge Key Review", "artifact_type": "SILVER_MERGE_KEY_REVIEW"},
        {"stage": "Silver Review", "artifact_type": "GATE5_SILVER_REVIEW"},
    ]

    assert generation_completed(summary, {}, "silver") is False
    assert generation_completed(summary, {"silver_generation_results": [{"script_path": "silver.sql"}]}, "silver") is True


def test_build_pipeline_steps_does_not_complete_in_progress_profiling():
    from services import pipeline_runtime

    steps = pipeline_runtime.build_pipeline_steps(
        source="database",
        checkpoint={
            "status": "RUNNING",
            "metadata_status": "COMPLETED",
            "column_profiling_status": "IN_PROGRESS",
        },
        summary=[],
        pending_gate1=[],
        completed_gate1=[],
        nominated_tables=[],
        certified_tables=[],
        enriched_payload={},
        gate3_payload={},
        bronze_generation_completed=False,
        silver_generation_completed=False,
        gold_generation_completed=False,
    )

    by_key = {step["key"]: step for step in steps}
    assert by_key["discovery"]["state"] == "COMPLETED"
    assert by_key["profiling"]["state"] == "RUNNING"
    assert by_key["enrichment"]["state"] == "PENDING"


def test_run_context_prefers_bronze_review_over_stale_stage_confirmation(monkeypatch):
    from services import pipeline_runtime

    checkpoint = {
        "run_id": "run-bronze",
        "source": "database",
        "status": "PAUSED_FOR_STAGE_CONFIRMATION",
        "stage_confirmation_enabled": True,
        "last_completed_stage_key": "bronze",
        "last_completed_stage_label": "Bronze Generation",
        "next_stage_key": "silver",
        "next_stage_label": "Silver Generation",
        "bronze_generation_status": "COMPLETED",
        "enrichment_review_status": "COMPLETED",
        "enrichment_review_artifact": {"approved_from_checkpoint": True},
    }

    monkeypatch.setattr(pipeline_runtime, "load_checkpoint_state", lambda run_id: checkpoint)
    monkeypatch.setattr(
        pipeline_runtime,
        "fetch_run_summary",
        lambda run_id: [{"stage": "bronze", "artifact_type": "BRONZE_GENERATION"}],
    )
    monkeypatch.setattr(pipeline_runtime, "get_pending_items", lambda run_id, gate: [])
    monkeypatch.setattr(pipeline_runtime, "get_completed_items", lambda run_id, gate: [])
    monkeypatch.setattr(
        pipeline_runtime,
        "fetch_json_artifact",
        lambda run_id, artifact: {"enrichment_artifact": {}} if artifact == "GATE3_APPROVED_ENRICHMENT" else {},
    )
    monkeypatch.setattr(
        pipeline_runtime,
        "load_bronze_scripts",
        lambda run_id, checkpoint=None: {"scripts": [{"script_body": "print('bronze')"}]},
    )
    monkeypatch.setattr(pipeline_runtime, "load_silver_scripts", lambda run_id, checkpoint=None: {"scripts": []})
    monkeypatch.setattr(pipeline_runtime, "load_gold_scripts", lambda run_id, checkpoint=None: {"scripts": []})

    context = pipeline_runtime.get_run_context("run-bronze")

    assert context["status"] == "HITL_WAIT"
    assert context["next_gate"] == 4
    assert context["stage_confirmation"] is None
    assert context["bronze"]["scripts"][0]["script_body"] == "print('bronze')"


def test_build_run_lineage_prefers_certified_fk_edges(monkeypatch):
    from services import pipeline_runtime

    checkpoint = {"run_id": "run-lineage", "gold_generation_contract": {}}
    monkeypatch.setattr(
        pipeline_runtime,
        "load_bronze_scripts",
        lambda run_id, checkpoint=None: {
            "scripts": [
                {"source": "insurance.dbo.claims", "target": "main.bronze.bronze_claims", "status": "APPROVED"},
            ]
        },
    )
    monkeypatch.setattr(
        pipeline_runtime,
        "load_silver_scripts",
        lambda run_id, checkpoint=None: {
            "scripts": [
                {"source_table": "main.bronze.bronze_claims", "target_table": "silver.silver_claims", "status": "APPROVED"},
            ]
        },
    )
    monkeypatch.setattr(
        pipeline_runtime,
        "load_gold_scripts",
        lambda run_id, checkpoint=None: {
            "scripts": [
                {
                    "source_table": "silver.silver_claims",
                    "target_table": "gold.fact_claim_count",
                    "dimension_script_path": "C:\\tmp\\gold_dim_claim_count.py",
                    "status": "APPROVED",
                },
            ]
        },
    )
    monkeypatch.setattr(
        pipeline_runtime,
        "fetch_json_artifact",
        lambda run_id, artifact: {
            "certified_joins": [
                {
                    "left_table": "claims",
                    "left_column": "policy_id",
                    "right_table": "policies",
                    "right_column": "policy_id",
                    "constraint_name": "fk_claims_policies",
                    "confidence": 1.0,
                    "certified": True,
                }
            ],
            "join_candidates": [
                {
                    "left_table": "claims",
                    "left_column": "agent_id",
                    "right_table": "agents",
                    "right_column": "agent_id",
                    "confidence": 0.55,
                }
            ],
        }
        if artifact == "ENRICHED_METADATA"
        else {},
    )

    payload = pipeline_runtime.build_run_lineage("run-lineage", checkpoint)

    edge_types = {edge["type"] for edge in payload["edges"]}
    assert {"pipeline", "fk", "heuristic"}.issubset(edge_types)
    fk_edges = [edge for edge in payload["edges"] if edge["type"] == "fk"]
    assert fk_edges[0]["constraint_name"] == "fk_claims_policies"
    assert payload["summary"]["fk_edge_count"] == 1
    assert payload["summary"]["heuristic_edge_count"] == 1


def test_build_run_lineage_renders_bronze_artifacts_with_generator_fields(monkeypatch):
    from services import pipeline_runtime

    checkpoint = {"run_id": "run-lineage-bronze-artifact", "gold_generation_contract": {}}
    monkeypatch.setattr(
        pipeline_runtime,
        "load_bronze_scripts",
        lambda run_id, checkpoint=None: {
            "scripts": [
                {
                    "database_name": "insurance",
                    "schema_name": "dbo",
                    "table": "claims",
                    "source_table": "insurance.dbo.claims",
                    "target_table": "ATHENA_DB.BRONZE.bronze_claims",
                    "script_body": "CREATE TABLE ...",
                }
            ]
        },
    )
    monkeypatch.setattr(pipeline_runtime, "load_silver_scripts", lambda run_id, checkpoint=None: {"scripts": []})
    monkeypatch.setattr(pipeline_runtime, "load_gold_scripts", lambda run_id, checkpoint=None: {"scripts": []})
    monkeypatch.setattr(pipeline_runtime, "fetch_json_artifact", lambda run_id, artifact: {})

    payload = pipeline_runtime.build_run_lineage("run-lineage-bronze-artifact", checkpoint)

    assert payload["summary"]["fallback"] is False
    assert payload["summary"]["source_count"] == 1
    assert payload["summary"]["bronze_count"] == 1

    normalized = pipeline_runtime._normalize_bronze_script(
        {
            "database_name": "insurance",
            "schema_name": "dbo",
            "table": "claims",
            "bronze_catalog": "ATHENA_DB",
            "bronze_schema": "BRONZE",
        }
    )
    assert normalized["source"] == "insurance.dbo.claims"
    assert normalized["target"] == "ATHENA_DB.BRONZE.bronze_claims"


def test_build_run_lineage_uses_checkpoint_fallback_when_scripts_missing(monkeypatch):
    from services import pipeline_runtime

    checkpoint = {
        "run_id": "run-lineage-fallback",
        "source": "adls_gen2",
        "file_feeds": [
            {
                "feed_id": "Vendor1_Deposit",
                "entity": "Deposit",
                "cloud_path": "abfss://athena@storage.dfs.core.windows.net/evention/vendor1/machine1/Deposit/",
            }
        ],
        "gold_generation_contract": {},
    }
    monkeypatch.setattr(pipeline_runtime, "load_bronze_scripts", lambda run_id, checkpoint=None: {"scripts": []})
    monkeypatch.setattr(pipeline_runtime, "load_silver_scripts", lambda run_id, checkpoint=None: {"scripts": []})
    monkeypatch.setattr(pipeline_runtime, "load_gold_scripts", lambda run_id, checkpoint=None: {"scripts": []})
    monkeypatch.setattr(pipeline_runtime, "fetch_json_artifact", lambda run_id, artifact: {})

    payload = pipeline_runtime.build_run_lineage("run-lineage-fallback", checkpoint)

    assert payload["summary"]["fallback"] is True
    assert payload["summary"]["mode"] == "checkpoint_fallback"
    assert payload["summary"]["source_count"] == 1
    assert payload["summary"]["bronze_count"] == 1
    assert payload["summary"]["silver_count"] == 1
    assert payload["summary"]["gold_count"] == 1
    assert [edge["type"] for edge in payload["edges"]] == ["pipeline", "pipeline", "pipeline"]


def test_build_run_lineage_database_fallback_uses_certified_tables(monkeypatch):
    from services import pipeline_runtime

    checkpoint = {
        "run_id": "run-lineage-db-fallback",
        "source": "database",
        "certified_tables": [
            {
                "source_schema": "dbo",
                "table_name": "claim_information",
            },
            {
                "source_schema": "dbo",
                "table_name": "expenses_outstanding_estimates",
            },
        ],
        "gold_generation_contract": {},
    }
    monkeypatch.setattr(pipeline_runtime, "load_bronze_scripts", lambda run_id, checkpoint=None: {"scripts": []})
    monkeypatch.setattr(pipeline_runtime, "load_silver_scripts", lambda run_id, checkpoint=None: {"scripts": []})
    monkeypatch.setattr(pipeline_runtime, "load_gold_scripts", lambda run_id, checkpoint=None: {"scripts": []})
    monkeypatch.setattr(pipeline_runtime, "fetch_json_artifact", lambda run_id, artifact: {})

    payload = pipeline_runtime.build_run_lineage("run-lineage-db-fallback", checkpoint)

    assert payload["summary"]["fallback"] is True
    assert payload["summary"]["source_count"] == 2
    assert payload["summary"]["bronze_count"] == 2
    assert payload["summary"]["silver_count"] == 2
    assert payload["summary"]["gold_count"] == 2
    assert any(node["name"] == "dbo.claim_information" for node in payload["nodes"])
    assert any(node["name"] == "main.bronze.bronze_claim_information" for node in payload["nodes"])


def test_run_context_converts_existing_pause_before_review_gate_to_hitl_wait(monkeypatch):
    from services import pipeline_runtime

    checkpoint = {
        "run_id": "run-gate3",
        "source": "database",
        "status": "PAUSED_FOR_STAGE_CONFIRMATION",
        "stage_confirmation_enabled": True,
        "last_completed_stage_key": "enrichment",
        "last_completed_stage_label": "Semantic Enrichment",
        "next_stage_key": "gate3",
        "next_stage_label": "Enrichment Review",
        "enriched_metadata": {"columns": [{"semantic_type": "MEASURE"}]},
    }

    monkeypatch.setattr(pipeline_runtime, "load_checkpoint_state", lambda run_id: checkpoint)
    monkeypatch.setattr(pipeline_runtime, "fetch_run_summary", lambda run_id: [])
    monkeypatch.setattr(pipeline_runtime, "get_pending_items", lambda run_id, gate: [])
    monkeypatch.setattr(pipeline_runtime, "get_completed_items", lambda run_id, gate: [])
    monkeypatch.setattr(
        pipeline_runtime,
        "fetch_json_artifact",
        lambda run_id, artifact: {},
    )

    context = pipeline_runtime.get_run_context("run-gate3")

    assert context["status"] == "HITL_WAIT"
    assert context["next_gate"] == 3
    assert context["stage_confirmation"] is None
    assert "Semantic Review is pending" in context["resume_message"]


def test_run_context_suppresses_stage_confirmation_when_background_stage_active(monkeypatch):
    from services import pipeline_runtime

    checkpoint = {
        "run_id": "run-active-enrichment",
        "source": "database",
        "status": "PAUSED_FOR_STAGE_CONFIRMATION",
        "background_stage": "enrichment",
        "stage_confirmation_enabled": True,
        "last_completed_stage_key": "profiling",
        "next_stage_key": "enrichment",
        "next_stage_label": "Semantic Enrichment",
    }

    monkeypatch.setattr(pipeline_runtime, "load_checkpoint_state", lambda run_id: checkpoint)
    monkeypatch.setattr(pipeline_runtime, "fetch_run_summary", lambda run_id: [])
    monkeypatch.setattr(pipeline_runtime, "get_pending_items", lambda run_id, gate: [])
    monkeypatch.setattr(pipeline_runtime, "get_completed_items", lambda run_id, gate: [])
    monkeypatch.setattr(pipeline_runtime, "fetch_json_artifact", lambda run_id, artifact: {})

    context = pipeline_runtime.get_run_context("run-active-enrichment")

    assert context["stage_confirmation"] is None
    assert context["current_pipeline_step"]["key"] == "enrichment"


def test_run_context_routes_stale_silver_stage_confirmation_to_missing_gate4(monkeypatch):
    from services import pipeline_runtime

    checkpoint = {
        "run_id": "run-silver-ready",
        "source": "database",
        "status": "PAUSED_FOR_STAGE_CONFIRMATION",
        "stage_confirmation_enabled": True,
        "last_completed_stage_key": "bronze",
        "last_completed_stage_label": "Bronze Generation",
        "next_stage_key": "silver",
        "next_stage_label": "Silver Generation",
        "bronze_generation_status": "COMPLETED",
        "silver_generation_status": "COMPLETED",
        "gate4": {"decision": "APPROVED"},
        "enrichment_review_status": "COMPLETED",
        "enrichment_review_artifact": {"approved_from_checkpoint": True},
    }

    monkeypatch.setattr(pipeline_runtime, "load_checkpoint_state", lambda run_id: checkpoint)
    monkeypatch.setattr(pipeline_runtime, "fetch_run_summary", lambda run_id: [])
    monkeypatch.setattr(pipeline_runtime, "get_pending_items", lambda run_id, gate: [])
    monkeypatch.setattr(pipeline_runtime, "get_completed_items", lambda run_id, gate: [])
    monkeypatch.setattr(
        pipeline_runtime,
        "fetch_json_artifact",
        lambda run_id, artifact: {"enrichment_artifact": {}} if artifact == "GATE3_APPROVED_ENRICHMENT" else {},
    )
    monkeypatch.setattr(
        pipeline_runtime,
        "load_bronze_scripts",
        lambda run_id, checkpoint=None: {"scripts": [{"script_body": "print('bronze')"}]},
    )
    monkeypatch.setattr(
        pipeline_runtime,
        "load_silver_scripts",
        lambda run_id, checkpoint=None: {"scripts": [{"script_body": "print('silver')"}]},
    )
    monkeypatch.setattr(pipeline_runtime, "load_gold_scripts", lambda run_id, checkpoint=None: {"scripts": []})

    context = pipeline_runtime.get_run_context("run-silver-ready")

    assert context["status"] == "HITL_WAIT"
    assert context["next_gate"] == 5
    assert context["stage_confirmation"] is None
    assert context["silver"]["scripts"][0]["script_body"] == "print('silver')"


def test_run_context_keeps_gate5_review_required_when_gold_artifact_exists(monkeypatch):
    from services import pipeline_runtime

    checkpoint = {
        "run_id": "run-gold-ready",
        "source": "database",
        "status": "PAUSED_FOR_STAGE_CONFIRMATION",
        "stage_confirmation_enabled": True,
        "last_completed_stage_key": "silver",
        "last_completed_stage_label": "Silver Generation",
        "next_stage_key": "gold",
        "next_stage_label": "Gold Generation",
        "silver_generation_status": "COMPLETED",
        "gold_generation_status": "COMPLETED",
        "gate4": {"decision": "APPROVED"},
        "gate5": {"decision": "APPROVED"},
        "next_review_key": "gold_review",
        "enrichment_review_status": "COMPLETED",
        "enrichment_review_artifact": {"approved_from_checkpoint": True},
    }

    monkeypatch.setattr(pipeline_runtime, "load_checkpoint_state", lambda run_id: checkpoint)
    monkeypatch.setattr(pipeline_runtime, "fetch_run_summary", lambda run_id: [])
    monkeypatch.setattr(pipeline_runtime, "get_pending_items", lambda run_id, gate: [])
    monkeypatch.setattr(pipeline_runtime, "get_completed_items", lambda run_id, gate: [])
    monkeypatch.setattr(
        pipeline_runtime,
        "fetch_json_artifact",
        lambda run_id, artifact: {"enrichment_artifact": {}} if artifact == "GATE3_APPROVED_ENRICHMENT" else {},
    )
    monkeypatch.setattr(pipeline_runtime, "load_bronze_scripts", lambda run_id, checkpoint=None: {"scripts": []})
    monkeypatch.setattr(
        pipeline_runtime,
        "load_silver_scripts",
        lambda run_id, checkpoint=None: {"scripts": [{"script_body": "print('silver')"}]},
    )
    monkeypatch.setattr(
        pipeline_runtime,
        "load_gold_scripts",
        lambda run_id, checkpoint=None: {"scripts": [{"script_body": "print('gold')"}]},
    )

    context = pipeline_runtime.get_run_context("run-gold-ready")

    assert context["status"] == "HITL_WAIT"
    assert context["next_review_key"] == "gold_review"
    assert context["stage_confirmation"] is None
    assert context["gold"]["scripts"][0]["script_body"] == "print('gold')"


@pytest.mark.parametrize(
    ("start_stage", "expected_gate"),
    [
        ("kpis", "gate1"),
        ("nomination", "gate2"),
        ("enrichment", "gate3"),
    ],
)
def test_database_continue_skips_stage_confirmation_before_review_gates(monkeypatch, start_stage, expected_gate):
    from services import pipeline_runtime

    visited = []
    saved_states = []

    def fake_runner(stage_key):
        def _run(state):
            visited.append(stage_key)
            if stage_key == expected_gate:
                return {"status": "HITL_WAIT", f"{stage_key}_status": "PENDING"}
            return {"status": "RUNNING"}

        return _run

    monkeypatch.setattr(pipeline_runtime, "_database_stage_runner", fake_runner)
    monkeypatch.setattr(pipeline_runtime, "save_checkpoint_state", lambda run_id, state: saved_states.append(dict(state)))
    monkeypatch.setattr(pipeline_runtime, "wait_for_minimum_stage_runtime", lambda *args, **kwargs: None)

    result = pipeline_runtime.continue_database_pipeline(
        "run-review",
        start_stage_key=start_stage,
        state={"run_id": "run-review", "stage_confirmation_enabled": True},
    )

    assert visited == [start_stage, expected_gate]
    assert result["status"] == "HITL_WAIT"
    assert result["last_completed_stage_key"] == expected_gate
    assert all(state.get("status") != "PAUSED_FOR_STAGE_CONFIRMATION" for state in saved_states)
    assert any(state.get("background_stage") == start_stage for state in saved_states)
    assert any(state.get("background_stage") == expected_gate for state in saved_states)


def test_database_continue_stops_before_next_stage_when_aborted(monkeypatch):
    visited = []

    monkeypatch.setattr(
        pipeline_runtime,
        "_database_stage_runner",
        lambda stage_key: lambda state: visited.append(stage_key) or {"status": "RUNNING"},
    )
    pipeline_runtime.ABORTED_RUNS.add("run-aborted")
    try:
        result = pipeline_runtime.continue_database_pipeline(
            "run-aborted",
            start_stage_key="ingestion",
            state={"run_id": "run-aborted", "status": "RUNNING"},
        )
    finally:
        pipeline_runtime.clear_run_abort("run-aborted")

    assert visited == []
    assert result["status"] == "ABORTED"
    assert result["abort_requested"] is True


def test_background_completion_callback_preserves_abort(monkeypatch):
    saved = []

    class CompletedAfterAbortExecutor:
        def submit(self, fn, *args):
            future = Future()
            pipeline_runtime.ABORTED_RUNS.add("run-callback-abort")
            future.set_result(fn(*args))
            return future

    monkeypatch.setattr(pipeline_runtime, "BACKGROUND_EXECUTOR", CompletedAfterAbortExecutor())
    monkeypatch.setattr(pipeline_runtime, "ensure_background_capacity_locked", lambda: None)
    monkeypatch.setattr(pipeline_runtime, "mark_run_processing", lambda run_id, stage: None)
    monkeypatch.setattr(
        pipeline_runtime,
        "load_checkpoint_state",
        lambda run_id: {"run_id": run_id, "status": "PROCESSING", "background_stage": "gate2"},
    )
    monkeypatch.setattr(
        pipeline_runtime,
        "save_checkpoint_state_timed",
        lambda run_id, state, context: saved.append((dict(state), context)),
    )

    try:
        pipeline_runtime.submit_background(
            "run-callback-abort",
            "gate2",
            lambda: {"status": "COMPLETED"},
        )
    finally:
        pipeline_runtime.clear_run_abort("run-callback-abort")
        pipeline_runtime.BACKGROUND_JOBS.pop("run-callback-abort:gate2", None)

    assert saved[-1][0]["status"] == "ABORTED"
    assert saved[-1][1] == "gate2:background_aborted"


def test_database_continue_clears_stale_failure_when_retrying(monkeypatch):
    from services import pipeline_runtime

    saved_states = []
    monkeypatch.setattr(
        pipeline_runtime,
        "_database_stage_runner",
        lambda _stage: lambda _state: {"status": "HITL_WAIT"},
    )
    monkeypatch.setattr(
        pipeline_runtime,
        "save_checkpoint_state",
        lambda run_id, state: saved_states.append(dict(state)),
    )

    pipeline_runtime.continue_database_pipeline(
        "run-retry",
        start_stage_key="bronze",
        state={
            "run_id": "run-retry",
            "error": "old failure",
            "error_type": "InterruptedRun",
            "error_message": "Backend restarted",
            "failed_stage": "bronze",
            "failed_stage_label": "Bronze Generation",
            "failed_background_stage": "bronze",
            "interrupted_by_backend_restart": True,
        },
    )

    assert saved_states[0]["error"] is None
    assert saved_states[0]["error_type"] is None
    assert saved_states[0]["error_message"] is None
    assert saved_states[0]["failed_stage"] is None
    assert saved_states[0]["failed_stage_label"] is None
    assert saved_states[0]["failed_background_stage"] is None
    assert saved_states[0]["interrupted_by_backend_restart"] is False


def test_mark_run_processing_moves_off_stale_review_gate(monkeypatch):
    from services import pipeline_runtime

    saved = []
    monkeypatch.setattr(
        pipeline_runtime,
        "load_checkpoint_state",
        lambda run_id: {
            "run_id": run_id,
            "status": "HITL_WAIT",
            "next_gate": 4,
            "next_review_key": "silver_merge_key_review",
            "stage_confirmation": {"awaiting_confirmation": True},
        },
    )
    monkeypatch.setattr(pipeline_runtime, "save_checkpoint_state", lambda run_id, state: saved.append(dict(state)))

    pipeline_runtime.mark_run_processing("run-transition", "silver")

    assert saved == [{
        "run_id": "run-transition",
        "status": "PROCESSING",
        "background_stage": "silver",
        "next_gate": None,
        "next_review_key": None,
        "stage_confirmation": None,
        "awaiting_stage_confirmation": False,
    }]


def test_job_done_callback_marks_failure_and_cleans_registry(monkeypatch):
    recorded = {}
    future = Future()
    future.set_exception(RuntimeError("job failed"))
    job_key = "run-6:pipeline"
    pipeline_service.BACKGROUND_JOBS[job_key] = future

    monkeypatch.setattr(
        pipeline_service,
        "_mark_run_failed",
        lambda run_id, exc, stage: recorded.update({"run_id": run_id, "error": str(exc), "stage": stage}),
    )

    callback = pipeline_service._job_done_callback("run-6", job_key, "pipeline")
    callback(future)

    assert recorded == {"run_id": "run-6", "error": "job failed", "stage": "pipeline"}
    assert job_key not in pipeline_service.BACKGROUND_JOBS


def test_gate4_review_filters_rejected_bronze_results_before_silver():
    from services import pipeline_runtime

    filtered = pipeline_runtime._filter_bronze_results_by_gate4_review(
        [
            {"database_name": "insurance", "schema_name": "dbo", "table": "claim_information"},
            {"database_name": "insurance", "schema_name": "dbo", "table": "policy_transactions"},
        ],
        {
            "feeds": [
                {
                    "database_name": "insurance",
                    "schema_name": "dbo",
                    "table": "claim_information",
                    "review_status": "APPROVED",
                },
                {
                    "database_name": "insurance",
                    "schema_name": "dbo",
                    "table": "policy_transactions",
                    "review_status": "REJECTED",
                },
            ]
        },
    )

    assert [item["table"] for item in filtered] == ["claim_information"]


def test_databricks_gate4_pauses_for_merge_key_review_after_execution(monkeypatch):
    from services import databricks_runtime

    saved = []
    monkeypatch.setattr(
        pipeline_runtime,
        "load_checkpoint_state",
        lambda run_id: {
            "run_id": run_id,
            "target_warehouse": "databricks",
            "bronze_generation_results": [{"table": "claims"}],
        },
    )
    monkeypatch.setattr(databricks_runtime, "databricks_bronze_execution_enabled", lambda: True)
    monkeypatch.setattr(
        databricks_runtime,
        "run_databricks_bronze_scripts",
        lambda state, **_: {**state, "databricks_bronze_execution_status": "COMPLETED"},
    )
    monkeypatch.setattr(
        pipeline_runtime,
        "_pause_for_silver_merge_key_review",
        lambda run_id, state: {**state, "status": "HITL_WAIT", "next_review_key": "silver_merge_key_review"},
    )
    monkeypatch.setattr(pipeline_runtime, "save_checkpoint_state_timed", lambda run_id, state, **_: saved.append(dict(state)))
    monkeypatch.setattr(pipeline_runtime, "ai_store_db_writer", lambda **_: None)
    result = pipeline_runtime.submit_gate4_review(
        "run-databricks-merge-review",
        action="APPROVED",
        review_artifact={"feeds": [{"table": "claims", "merge_keys": ["claim_id"]}]},
    )

    assert result["next_review_key"] == "silver_merge_key_review"
    assert saved[-1]["next_review_key"] == "silver_merge_key_review"


def test_database_databricks_gate4_refuses_disabled_execution(monkeypatch):
    from services import databricks_runtime

    monkeypatch.setattr(databricks_runtime, "databricks_bronze_execution_enabled", lambda: False)

    with pytest.raises(RuntimeError, match="execution is disabled"):
        pipeline_runtime.submit_gate4_review(
            "run-databricks-disabled",
            action="APPROVED",
            review_artifact={"feeds": [{"table": "claims", "merge_keys": ["claim_id"]}]},
            checkpoint_state={
                "run_id": "run-databricks-disabled",
                "target_warehouse": "databricks",
                "bronze_generation_results": [{"table": "claims"}],
            },
        )


def test_gate4_review_uses_provided_checkpoint_snapshot(monkeypatch):
    from services import databricks_runtime

    checkpoint = {
        "run_id": "run-gate4-snapshot",
        "target_warehouse": "databricks",
        "bronze_generation_results": [{"table": "claims"}],
    }

    monkeypatch.setattr(
        pipeline_runtime,
        "load_checkpoint_state",
        lambda run_id: (_ for _ in ()).throw(AssertionError("checkpoint should come from submitter")),
    )
    monkeypatch.setattr(
        pipeline_runtime,
        "_pause_for_silver_merge_key_review",
        lambda run_id, state: {**state, "status": "HITL_WAIT", "next_review_key": "silver_merge_key_review"},
    )
    monkeypatch.setattr(databricks_runtime, "databricks_bronze_execution_enabled", lambda: True)
    monkeypatch.setattr(
        databricks_runtime,
        "run_databricks_bronze_scripts",
        lambda state, **_: {**state, "databricks_bronze_execution_status": "COMPLETED"},
    )
    monkeypatch.setattr(pipeline_runtime, "save_checkpoint_state_timed", lambda *args, **kwargs: None)
    monkeypatch.setattr(pipeline_runtime, "ai_store_db_writer", lambda **_: None)

    result = pipeline_runtime.submit_gate4_review(
        "run-gate4-snapshot",
        action="APPROVED",
        review_artifact={"feeds": [{"table": "claims", "merge_keys": ["claim_id"]}]},
        checkpoint_state=checkpoint,
    )

    assert result["next_review_key"] == "silver_merge_key_review"


def test_gate4_review_uses_selected_bronze_subset_before_silver():
    from services import pipeline_runtime

    filtered = pipeline_runtime._filter_bronze_results_by_gate4_review(
        [
            {"database_name": "insurance", "schema_name": "dbo", "table": "claim_information"},
            {"database_name": "insurance", "schema_name": "dbo", "table": "policy_transactions"},
            {"database_name": "insurance", "schema_name": "dbo", "table": "claim_payment_indemnity"},
        ],
        {
            "feeds": [
                {
                    "database_name": "insurance",
                    "schema_name": "dbo",
                    "table": "claim_information",
                    "review_status": "APPROVED",
                },
                {
                    "database_name": "insurance",
                    "schema_name": "dbo",
                    "table": "policy_transactions",
                    "review_status": "PENDING",
                },
                {
                    "database_name": "insurance",
                    "schema_name": "dbo",
                    "table": "claim_payment_indemnity",
                    "review_status": "PENDING",
                },
            ]
        },
    )

    assert [item["table"] for item in filtered] == ["claim_information"]


def test_gate4_review_all_pending_preserves_legacy_all_bronze_selection():
    from services import pipeline_runtime

    filtered = pipeline_runtime._filter_bronze_results_by_gate4_review(
        [
            {"database_name": "insurance", "schema_name": "dbo", "table": "claim_information"},
            {"database_name": "insurance", "schema_name": "dbo", "table": "policy_transactions"},
        ],
        {
            "feeds": [
                {
                    "database_name": "insurance",
                    "schema_name": "dbo",
                    "table": "claim_information",
                    "review_status": "PENDING",
                },
                {
                    "database_name": "insurance",
                    "schema_name": "dbo",
                    "table": "policy_transactions",
                    "review_status": "PENDING",
                },
            ]
        },
    )

    assert [item["table"] for item in filtered] == ["claim_information", "policy_transactions"]


def test_gate5_review_filters_gold_contract_to_selected_silver_sources():
    from services import pipeline_runtime

    filtered_silver = pipeline_runtime._filter_silver_results_by_gate5_review(
        [
            {"table": "claims", "target_table": "ATHENA_DB.SILVER.silver_claims"},
            {"table": "policy", "target_table": "ATHENA_DB.SILVER.silver_policy"},
        ],
        {
            "items": [
                {"table": "claims", "target_table": "ATHENA_DB.SILVER.silver_claims", "review_status": "APPROVED"},
                {"table": "policy", "target_table": "ATHENA_DB.SILVER.silver_policy", "review_status": "PENDING"},
            ]
        },
    )
    contract = pipeline_runtime._filter_gold_contract_by_silver_results(
        {
            "kpi_mappings": [
                {"kpi_name": "Claim Count", "source_silver_table": "ATHENA_DB.SILVER.silver_claims"},
                {"kpi_name": "Policy Count", "source_silver_table": "ATHENA_DB.SILVER.silver_policy"},
            ],
            "warnings": [],
        },
        filtered_silver,
    )

    assert [item["table"] for item in filtered_silver] == ["claims"]
    assert [item["kpi_name"] for item in contract["kpi_mappings"]] == ["Claim Count"]
    assert "filtered out 1 KPI" in contract["warnings"][0]


def test_gate5_review_matches_table_only_approval_to_generated_silver_target():
    from services import pipeline_runtime

    filtered_silver = pipeline_runtime._filter_silver_results_by_gate5_review(
        [
            {
                "table": "claim_payment_indemnity",
                "source_table": "ATHENA_DB.BRONZE.bronze_claim_payment_indemnity",
                "target_table": "ATHENA_DB.SILVER.silver_claim_payment_indemnity",
            },
            {
                "table": "policy_transactions",
                "source_table": "ATHENA_DB.BRONZE.bronze_policy_transactions",
                "target_table": "ATHENA_DB.SILVER.silver_policy_transactions",
            },
        ],
        {
            "items": [
                {"table": "claim_payment_indemnity", "review_status": "APPROVED"},
                {"table": "policy_transactions", "review_status": "PENDING"},
            ]
        },
    )

    assert [item["table"] for item in filtered_silver] == ["claim_payment_indemnity"]


def test_finalized_snowflake_dbt_package_activates_each_metadata_layer(monkeypatch):
    from types import SimpleNamespace
    from services import metadata_selection

    calls = []

    class Repository:
        def register_and_activate_artifacts(self, *, processing_stage, artifacts):
            artifacts = list(artifacts)
            calls.append((processing_stage, artifacts))
            return [{
                "ingestion_object": {
                    "config_version": 20 + index,
                    "config_hash": f"active-{processing_stage}-{index}",
                },
                "execution_spec": artifact["execution_spec"],
            } for index, artifact in enumerate(artifacts)]

    monkeypatch.setattr(
        metadata_selection,
        "validated_metadata_selection",
        lambda _state: SimpleNamespace(repository=Repository()),
    )
    monkeypatch.setattr(
        pipeline_runtime,
        "_materialize_silver_to_gold_metadata",
        lambda current: {
            **current,
            "gold_metadata_drafts": [{
                "target_table": "INSURANCE.GOLD.fact_claims",
                "gold_ingestion_object_id": 301,
                "gold_ingestion_object_config_version": 3,
                "silver_to_gold_mapping_version": 31,
                "silver_to_gold_mapping_hash": "gold-map",
            }],
        },
    )
    monkeypatch.setattr(pipeline_runtime, "_attach_gold_execution_specs", lambda current: current)

    def spec(mapping_version):
        return {
            "contract_version": "1.0",
            "execution_mode": "GENERATED_ARTIFACT",
            "target_platform": "SNOWFLAKE",
            "engine": "SNOWFLAKE_DBT",
            "artifact_uri": "generated-code://model.sql",
            "entry_point": "model",
            "artifact_hash": "sha256:" + "1" * 64,
            "generator_version": "test",
            "mapping_version": mapping_version,
        }

    state = {
        "target_warehouse": "snowflake",
        "execution_engine": "dbt",
        "snowflake_dbt_validation_status": "STATIC_VALIDATED",
        "snowflake_dbt_artifact_set_hash": "a" * 64,
        "snowflake_dbt_idempotency_key": "package-1",
        "source_system_id": 7,
        "bronze_generation_results": [{
            "ingestion_object_id": 101, "ingestion_object_config_version": 1,
            "mapping_version": 11, "mapping_hash": "bronze-map", "execution_spec": spec(11),
        }],
        "certified_tables": [{"ingestion_object_id": 101}],
        "silver_generation_results": [{
            "silver_ingestion_object_id": 201, "silver_ingestion_object_config_version": 2,
            "bronze_to_silver_mapping_version": 21, "bronze_to_silver_mapping_hash": "silver-map",
            "execution_spec": spec(21),
        }],
        "silver_transformation_objects": [{"ingestion_object_id": 201}],
        "gold_generation_results": [{
            "gold_ingestion_object_id": 301, "gold_ingestion_object_config_version": 3,
            "silver_to_gold_mapping_version": 31, "silver_to_gold_mapping_hash": "gold-map",
            "target_table": "INSURANCE.GOLD.fact_claims", "execution_spec": spec(31),
        }],
        "gold_transformation_objects": [{"ingestion_object_id": 301}],
    }

    result = pipeline_runtime._activate_finalized_snowflake_dbt_metadata(state)

    assert [call[0] for call in calls] == [
        "SOURCE_TO_BRONZE", "BRONZE_TO_SILVER", "SILVER_TO_GOLD"
    ]
    assert all(
        item["metadata_activation_status"] == "ACTIVE"
        for layer in ("bronze", "silver", "gold")
        for item in result[f"{layer}_generation_results"]
    )
    assert all(
        artifact["execution_spec"]["dbt_package_hash"] == "a" * 64
        for _, artifacts in calls for artifact in artifacts
    )


def test_snowflake_dbt_gold_metadata_uses_exact_reviewed_silver_draft(monkeypatch):
    from types import SimpleNamespace
    from services import metadata_selection

    silver = {
        "ingestion_object_id": 201, "source_system_id": 7, "config_version": 2,
        "config_hash": "silver-object", "processing_stage": "BRONZE_TO_SILVER",
        "target_table": "INSURANCE.SILVER.silver_claims",
    }
    bundle = {
        "mapping_version": 21, "mapping_hash": "silver-map",
        "mappings": [{
            "target_column_name": "claimid", "target_data_type": "NUMBER",
            "is_primary_key": True, "is_nullable": False,
        }],
    }

    class Repository:
        def get_ingestion_objects(self, _refs, *, require_active):
            assert require_active is False
            return {(201, 2): silver}

        def get_mapping_bundles(self, refs):
            assert refs[0]["require_active"] is None
            return {(201, "BRONZE_TO_SILVER", 21): bundle}

        def upsert_silver_to_gold_draft(self, **_kwargs):
            return {
                "ingestion_object": {
                    "ingestion_object_id": 301, "config_version": 3, "config_hash": "gold-object"
                },
                "mapping_bundle": {"mapping_version": 31, "mapping_hash": "gold-map"},
            }

    monkeypatch.setattr(
        metadata_selection,
        "validated_metadata_selection",
        lambda _state: SimpleNamespace(repository=Repository()),
    )
    result = pipeline_runtime._materialize_silver_to_gold_metadata({
        "target_warehouse": "snowflake", "execution_engine": "dbt",
        "silver_generation_results": [{
            "table": "claims", "target_table": "INSURANCE.SILVER.silver_claims",
            "silver_ingestion_object_id": 201, "silver_ingestion_object_config_version": 2,
            "bronze_to_silver_mapping_version": 21, "bronze_to_silver_mapping_hash": "silver-map",
            "metadata_activation_status": "PENDING_FINAL_DBT_PACKAGE",
        }],
        "gold_generation_contract": {
            "dimension_mappings": [{
                "logical_table": "claims", "source_silver_table": "INSURANCE.SILVER.silver_claims",
                "columns": ["claimid"],
            }],
            "kpi_mappings": [],
        },
    })

    assert result["gold_metadata_drafts"][0]["gold_ingestion_object_id"] == 301
    assert result["gold_metadata_rejections"] == []


def test_snowflake_dbt_uses_one_control_attempt_for_the_project(monkeypatch):
    from types import SimpleNamespace
    from services import metadata_selection

    events = []

    class Repository:
        def enqueue_work(self, **kwargs):
            events.append(("enqueue", kwargs))
            return {"queue_id": 91, "queue_status": "PENDING", "logical_work_id": kwargs["logical_work_id"]}

        def claim_next_queue_item(self, **kwargs):
            events.append(("claim", kwargs))
            return {"queue_id": 91, "attempt_count": 1, "queue_status": "RUNNING"}

        def create_run_attempt(self, *_args, **_kwargs):
            events.append(("run", None))
            return {"run": {"run_id": "runtime-1", "queue_id": 91}, "metadata_snapshot_matches": True}

        def update_run_phase(self, *args, **kwargs):
            events.append(("phase", (args, kwargs)))

        def begin_queue_finalization(self, **kwargs):
            events.append(("finalizing", kwargs))

        def finalize_successful_run(self, **kwargs):
            events.append(("success", kwargs))

    repository = Repository()
    monkeypatch.setattr(
        metadata_selection,
        "validated_target_metadata_selection",
        lambda _state: SimpleNamespace(repository=repository),
    )
    state = {
        "run_id": "design-1", "source_system_id": 7,
        "snowflake_dbt_artifact_set_hash": "b" * 64,
        "snowflake_dbt_model_count": 3,
        "bronze_generation_results": [{"ingestion_object_id": 101, "metadata_activation_status": "ACTIVE"}],
        "silver_generation_results": [{"silver_ingestion_object_id": 201, "metadata_activation_status": "ACTIVE"}],
        "gold_generation_results": [{"gold_ingestion_object_id": 301, "metadata_activation_status": "ACTIVE"}],
    }

    control = pipeline_runtime._start_snowflake_dbt_control_attempt(state)
    pipeline_runtime._finish_snowflake_dbt_control_attempt(
        control, {**state, "snowflake_dbt_execution": {"status": "COMPLETED"}}
    )

    assert [event[0] for event in events].count("enqueue") == 1
    assert [event[0] for event in events].count("run") == 1
    assert [event[0] for event in events].count("success") == 1
    assert events[0][1]["work_scope"]["ingestion_object_ids"] == [101, 201, 301]
