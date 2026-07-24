from __future__ import annotations

from concurrent.futures import Future

import pytest
from fastapi import HTTPException

from api.models import PipelineRunRequest
from api.services import pipeline_service
from services import pipeline_runtime


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


def test_gate2_scope_keeps_lookup_and_fk_dimension_tables():
    tables = [
        {"database_name": "insurance", "schema_name": "dbo", "table_name": "claim_information", "nomination_reason": "Dual Match (Keyword + Semantic)"},
        {"database_name": "insurance", "schema_name": "dbo", "table_name": "dim_policy", "nomination_reason": "Lookup Table Sweep (dim/ref/lkp)"},
        {"database_name": "insurance", "schema_name": "dbo", "table_name": "policy_type", "nomination_reason": "FK Resolution (related to nominated table)"},
        {"database_name": "insurance", "schema_name": "dbo", "table_name": "audit_log", "nomination_reason": "Lookup Table Sweep (dim/ref/lkp)"},
    ]

    scoped = pipeline_runtime._gate2_execution_scope(tables, ["insurance.dbo.claim_information"])

    assert [item["table_name"] for item in scoped] == [
        "claim_information",
        "dim_policy",
        "policy_type",
    ]


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


def test_list_runs_uses_lightweight_checkpoint_index(monkeypatch):
    recorded = {}

    class StubCursor:
        timeout = None

        def execute(self, query):
            recorded["query"] = query

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
    assert "full_state_json" not in recorded["query"]
    assert "ai_store" not in recorded["query"]
    assert recorded["closed"] is True


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
    assert recorded["kwargs"]["compliance_enabled"] is True
    assert recorded["kwargs"]["compliance_domain"] == "Insurance"
    assert recorded["kwargs"]["compliance_countries"] == ["US", "AU"]
    assert callable(recorded["callback"])
    pipeline_service.BACKGROUND_JOBS.pop("run-submit:pipeline", None)


def test_continue_file_pipeline_job_rejects_invalid_state(monkeypatch):
    class BadGraph:
        def invoke(self, state):
            return "bad"

    monkeypatch.setattr(pipeline_service, "source_ingestion_graph", lambda: BadGraph())

    with pytest.raises(ValueError, match="invalid state"):
        pipeline_service.continue_file_pipeline_job("run-4", {"foo": "bar"})


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


def test_merge_key_resolution_auto_approves_and_continues_without_hitl(monkeypatch):
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

    assert result["status"] == "RUNNING"
    assert result["next_review_key"] is None
    assert result["silver_merge_key_review_decision"] == "APPROVED"
    assert result["gate_silver_merge_key_review"]["status"] == "COMPLETED"
    assert result["enriched_metadata"]["columns"][0]["is_join_key"] is True


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
        "pre_bronze_metadata_codegen", "pre_bronze_metadata_codegen_review", "bronze", "gate4",
        "runtime_bundle_handoff", "pre_bronze_runtime_config", "pre_bronze_validate_source",
        "pre_bronze_discover_source_objects", "pre_bronze_stage_to_landing",
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


def test_run_context_advances_stale_silver_stage_confirmation(monkeypatch):
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

    assert context["status"] == "PAUSED_FOR_STAGE_CONFIRMATION"
    assert context["stage_confirmation"]["last_completed_stage_key"] == "silver"
    assert context["stage_confirmation"]["next_stage_key"] == "gold"
    assert context["silver"]["scripts"][0]["script_body"] == "print('silver')"


def test_run_context_clears_stale_stage_confirmation_when_gold_complete(monkeypatch):
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

    assert context["status"] == "PIPELINE_COMPLETED"
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


def test_databricks_gate4_continues_to_silver_after_automatic_merge_key_resolution(monkeypatch):
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
    monkeypatch.setattr(databricks_runtime, "databricks_bronze_execution_enabled", lambda: False)
    monkeypatch.setattr(
        pipeline_runtime,
        "_pause_for_silver_merge_key_review",
        lambda run_id, state: {**state, "status": "RUNNING", "next_review_key": None},
    )
    monkeypatch.setattr(pipeline_runtime, "save_checkpoint_state_timed", lambda run_id, state, **_: saved.append(dict(state)))
    monkeypatch.setattr(pipeline_runtime, "ai_store_db_writer", lambda **_: None)
    monkeypatch.setattr(
        pipeline_runtime,
        "continue_database_pipeline",
        lambda run_id, start_stage_key, state: {**state, "continued_to": start_stage_key},
    )

    result = pipeline_runtime.submit_gate4_review(
        "run-databricks-merge-review",
        action="APPROVED",
        review_artifact={"feeds": [{"table": "claims", "merge_keys": ["claim_id"]}]},
    )

    assert result["continued_to"] == "silver"
    assert result["next_review_key"] is None
    assert saved[-1]["next_review_key"] is None


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
        lambda run_id, state: {**state, "status": "RUNNING", "next_review_key": None},
    )
    monkeypatch.setattr(databricks_runtime, "databricks_bronze_execution_enabled", lambda: False)
    monkeypatch.setattr(pipeline_runtime, "save_checkpoint_state_timed", lambda *args, **kwargs: None)
    monkeypatch.setattr(pipeline_runtime, "ai_store_db_writer", lambda **_: None)
    monkeypatch.setattr(
        pipeline_runtime,
        "continue_database_pipeline",
        lambda run_id, start_stage_key, state: {**state, "continued_to": start_stage_key},
    )

    result = pipeline_runtime.submit_gate4_review(
        "run-gate4-snapshot",
        action="APPROVED",
        review_artifact={"feeds": [{"table": "claims", "merge_keys": ["claim_id"]}]},
        checkpoint_state=checkpoint,
    )

    assert result["continued_to"] == "silver"
    assert result["next_review_key"] is None


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
