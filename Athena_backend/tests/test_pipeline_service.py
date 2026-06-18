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
        source="database",
        source_databases=["db1"],
        sftp_entity="transactions",
        use_domain_kb=True,
        stage_confirmation_enabled=True,
    )

    assert saved["run_id"] == "run-1"
    assert saved["state"]["status"] == "COMPLETED"
    assert saved["state"]["payload"] == "ok"


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
        source="sftp",
        source_databases=None,
        sftp_entity="transactions",
        use_domain_kb=False,
        stage_confirmation_enabled=False,
    )

    assert saved["state"]["status"] == "COMPLETED"
    assert saved["state"]["source"] == "sftp"


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

    payload = PipelineRunRequest(brd_text="brd", source="database", database_name="db1")
    pipeline_service.submit_pipeline_start("run-submit", payload)

    assert recorded["fn"] == pipeline_service.run_pipeline_background
    assert recorded["kwargs"]["run_id"] == "run-submit"
    assert recorded["kwargs"]["source_databases"] == ["db1"]
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
