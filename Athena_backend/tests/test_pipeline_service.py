from __future__ import annotations

from concurrent.futures import Future

import pytest
from fastapi import HTTPException

from api.models import PipelineRunRequest
from api.services import pipeline_service


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


def test_minimum_stage_runtime_uses_env(monkeypatch):
    from services import pipeline_runtime

    monkeypatch.setenv("ATHENA_MIN_STAGE_RUNTIME_SECONDS", "2.5")

    assert pipeline_runtime._minimum_stage_runtime_seconds() == 2.5


def test_minimum_stage_runtime_falls_back_for_bad_env(monkeypatch):
    from services import pipeline_runtime

    monkeypatch.setenv("ATHENA_MIN_STAGE_RUNTIME_SECONDS", "bad")

    assert pipeline_runtime._minimum_stage_runtime_seconds() == 4.0


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

    payload = PipelineRunRequest(brd_text="brd", brd_filename="Claims BRD", source="database", database_name="db1")
    pipeline_service.submit_pipeline_start("run-submit", payload)

    assert recorded["fn"] == pipeline_service.run_pipeline_background
    assert recorded["kwargs"]["run_id"] == "run-submit"
    assert recorded["kwargs"]["brd_filename"] == "Claims BRD"
    assert recorded["kwargs"]["source_databases"] == ["db1"]
    assert recorded["kwargs"]["stage_confirmation_enabled"] is False
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


def test_run_context_preserves_stage_confirmation_status_after_bronze(monkeypatch):
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

    assert context["status"] == "PAUSED_FOR_STAGE_CONFIRMATION"
    assert context["stage_confirmation"]["last_completed_stage_key"] == "bronze"
    assert context["stage_confirmation"]["next_stage_key"] == "silver"
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
