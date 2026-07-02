from concurrent.futures import TimeoutError as FutureTimeoutError
import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("ATHENA_DEMO_MODE", "false")

from api.main import app


client = TestClient(app)


def test_health_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["service"] == "athena-fastapi"
    embeddings = body["embeddings"]
    assert embeddings["enabled"] is False
    assert embeddings["mode"] == "disabled"


def test_sql_tcp_probe_failure_is_cached(monkeypatch):
    from utilis import db

    calls = {"probe": 0}

    def fake_probe(host, port, timeout_seconds):
        calls["probe"] += 1
        return TimeoutError("timed out")

    db._SQL_ENDPOINT_FAILURE_CACHE.clear()
    monkeypatch.setattr(db, "SQL_TCP_PROBE_ENABLED", True)
    monkeypatch.setattr(db, "SQL_FAIL_FAST_ON_TCP_PROBE", True)
    monkeypatch.setattr(db, "SQL_ENDPOINT_NEGATIVE_CACHE_SECONDS", 60)
    monkeypatch.setattr(db, "_probe_sql_endpoint", fake_probe)

    with pytest.raises(RuntimeError, match="SQL TCP probe failed"):
        db._connect_with_retry(
            "DRIVER={stub};",
            database_name="AdventureWorks2019",
            host="dataedge.database.windows.net",
            port=1433,
            role="pipeline",
        )

    with pytest.raises(RuntimeError, match="SQL TCP probe failed"):
        db._connect_with_retry(
            "DRIVER={stub};",
            database_name="AdventureWorks2019",
            host="dataedge.database.windows.net",
            port=1433,
            role="pipeline",
        )

    assert calls["probe"] == 1


def test_schema_embedding_is_skipped_when_embeddings_are_disabled(monkeypatch):
    from nodes import ingestion

    monkeypatch.setattr(
        ingestion,
        "execute_source_sql",
        lambda db, query, params: (_ for _ in ()).throw(AssertionError("schema SQL should not run")),
    )

    result = ingestion._embed_schema_metadata({
        "run_id": "run-schema-smoke",
        "status": "RUNNING",
        "source_databases": ["Insurance"],
    })

    assert result["schema_embedded"] is False
    assert result["schema_columns_count"] == 0


def test_pipeline_run_requires_brd_text_for_database_source():
    payload = {"source": "database", "brd_text": ""}
    response = client.post("/pipeline/run", json=payload)
    assert response.status_code == 400
    assert response.json()["detail"] == "brd_text is required"


def test_pipeline_run_requires_brd_text_for_file_source():
    response = client.post("/pipeline/run", json={"source": "sftp", "brd_text": ""})

    assert response.status_code == 400
    assert response.json()["detail"] == "brd_text is required"


def test_pipeline_run_starts_demo_progress_before_kpi_review(monkeypatch):
    from api.routers import pipeline_router

    monkeypatch.setattr(pipeline_router, "demo_enabled", lambda: True)
    monkeypatch.setattr(pipeline_router, "new_demo_run_id", lambda: "demo-run-1")

    recorded = {}

    def fake_demo_start_progress(run_id, segment):
        recorded["run_id"] = run_id
        recorded["segment"] = segment
        return {"run_id": run_id, "status": "PROCESSING"}

    monkeypatch.setattr(pipeline_router, "demo_start_progress", fake_demo_start_progress)

    response = client.post("/pipeline/run", json={"source": "database", "brd_text": "valid brd text"})

    assert response.status_code == 200
    assert response.json() == {"run_id": "demo-run-1", "status": "PROCESSING"}
    assert recorded == {"run_id": "demo-run-1", "segment": "start"}


def test_pipeline_run_accepts_file_source_with_brd_text(monkeypatch):
    saved = {}

    monkeypatch.setattr("services.pipeline_runtime.load_checkpoint_state", lambda run_id: None)
    monkeypatch.setattr(
        "services.pipeline_runtime.save_checkpoint_state",
        lambda run_id, state: saved.update({"run_id": run_id, "state": state}),
    )
    monkeypatch.setattr(
        "api.services.pipeline_service.submit_pipeline_start",
        lambda run_id, payload: saved.update({"submitted_run_id": run_id, "submitted_source": payload.source}),
    )

    response = client.post("/pipeline/run", json={"source": "sftp", "brd_text": "file source brd"})

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "RUNNING"
    assert saved["state"]["source"] == "sftp"
    assert saved["state"]["sftp_entity"] == "transactions"
    assert saved["submitted_run_id"] == body["run_id"]
    assert saved["submitted_source"] == "sftp"


def test_pipeline_run_returns_503_when_checkpoint_init_fails(monkeypatch):
    monkeypatch.setattr("services.pipeline_runtime.load_checkpoint_state", lambda run_id: None)
    monkeypatch.setattr(
        "services.pipeline_runtime.save_checkpoint_state",
        lambda run_id, state: (_ for _ in ()).throw(RuntimeError("db down")),
    )

    response = client.post("/pipeline/run", json={"source": "database", "brd_text": "valid brd text"})

    assert response.status_code == 503
    assert response.json()["detail"] == "Failed to initialize run checkpoint"


def test_upload_brd_creates_file(monkeypatch):
    monkeypatch.setattr("api.routers.pipeline_router.api_utils.ROOT_DIR", Path(__file__).resolve().parents[1])

    file_content = b"test content"
    response = client.post(
        "/pipeline/upload-brd",
        files={"file": ("sample.brd", file_content, "text/plain")},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["filename"] == "sample.brd"
    assert body["status"] == "uploaded"
    assert Path(body["path"]).exists()
    assert Path(body["path"]).read_bytes() == file_content


def test_upload_brd_rejects_large_file(monkeypatch):
    monkeypatch.setattr("api.routers.pipeline_router.api_utils.ROOT_DIR", Path(__file__).resolve().parents[1])

    response = client.post(
        "/pipeline/upload-brd",
        files={"file": ("large.brd", b"x" * (5 * 1024 * 1024 + 1), "text/plain")},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "File too large"


def test_pipeline_status_returns_404_for_missing_run(monkeypatch):
    monkeypatch.setattr("api.services.ui_service.ui_run", lambda run_id: {"status": "NOT_FOUND"})

    response = client.get("/pipeline/run-123/status")

    assert response.status_code == 404
    assert response.json()["detail"] == "Run not found: run-123"


def test_pipeline_status_shapes_running_response(monkeypatch):
    monkeypatch.setattr(
        "api.services.ui_service.ui_run",
        lambda run_id: {"status": "RUNNING", "run_id": run_id},
    )

    response = client.get("/pipeline/run-123/status")

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == "run-123"
    assert body["state"]["life_cycle_state"] == "RUNNING"
    assert body["state"]["result_state"] == "RUNNING"


def test_abort_run_persists_aborted_status(monkeypatch):
    saved = {}
    monkeypatch.setattr("services.pipeline_runtime.load_checkpoint_state", lambda run_id: {"run_id": run_id, "status": "RUNNING"})
    monkeypatch.setattr("services.pipeline_runtime.save_checkpoint_state", lambda run_id, state: saved.update({"run_id": run_id, "state": state}))

    response = client.post("/pipeline/run-123/abort")

    assert response.status_code == 200
    assert response.json() == {"run_id": "run-123", "status": "ABORTED"}
    assert saved["state"]["status"] == "ABORTED"


def test_continue_stage_requires_pending_stage(monkeypatch):
    monkeypatch.setattr("services.pipeline_runtime.load_checkpoint_state", lambda run_id: {})

    response = client.post("/pipeline/run-123/continue-stage", json={"auto_advance": False})

    assert response.status_code == 400
    assert response.json()["detail"] == "No next stage is pending confirmation for this run."


def test_continue_stage_rejects_file_source(monkeypatch):
    monkeypatch.setattr(
        "services.pipeline_runtime.load_checkpoint_state",
        lambda run_id: {"next_stage_key": "ingestion", "source": "sftp"},
    )

    response = client.post("/pipeline/run-123/continue-stage", json={"auto_advance": False})

    assert response.status_code == 400
    assert response.json()["detail"] == "Stage-by-stage confirmation is not enabled for file-source runs yet."


def test_continue_stage_submits_background_job(monkeypatch):
    recorded = {}
    checkpoint = {
        "run_id": "run-123",
        "status": "PAUSED_FOR_STAGE_CONFIRMATION",
        "next_stage_key": "enrichment",
        "source": "database",
    }

    monkeypatch.setattr("services.pipeline_runtime.load_checkpoint_state", lambda run_id: checkpoint)
    monkeypatch.setattr(
        "services.pipeline_runtime.save_checkpoint_state",
        lambda run_id, state: recorded.update({"saved_run_id": run_id, "saved_state": state}),
    )
    monkeypatch.setattr(
        "services.pipeline_runtime.submit_background",
        lambda run_id, stage, fn, *args: recorded.update(
            {"background_run_id": run_id, "background_stage": stage, "background_fn": fn.__name__, "background_args": args}
        ),
    )

    response = client.post("/pipeline/run-123/continue-stage", json={"auto_advance": False})

    assert response.status_code == 200
    assert response.json() == {
        "run_id": "run-123",
        "status": "SUBMITTED",
        "next_stage_key": "enrichment",
        "resume_message": "enrichment is running.",
    }
    assert recorded["saved_state"]["status"] == "PROCESSING"
    assert recorded["saved_state"]["background_stage"] == "enrichment"
    assert recorded["saved_state"]["stage_confirmation_enabled"] is True
    assert recorded["background_stage"] == "enrichment"
    assert recorded["background_fn"] == "continue_database_pipeline_job"
    assert recorded["background_args"][0:3] == ("run-123", "enrichment", recorded["saved_state"])
    assert recorded["background_args"][3] is False


def test_run_lineage_endpoint_returns_payload(monkeypatch):
    monkeypatch.setattr("services.pipeline_runtime.load_checkpoint_state", lambda run_id: {"run_id": run_id})
    monkeypatch.setattr(
        "services.pipeline_runtime.build_run_lineage",
        lambda run_id, checkpoint: {"run_id": run_id, "nodes": [{"id": "n1"}], "edges": [], "summary": {"fk_edge_count": 0}},
    )

    response = client.get("/run-lineage/run-123")

    assert response.status_code == 200
    assert response.json()["run_id"] == "run-123"
    assert response.json()["nodes"][0]["id"] == "n1"


def test_retry_failed_stage_rejects_non_failed_run(monkeypatch):
    monkeypatch.setattr("services.pipeline_runtime.load_checkpoint_state", lambda run_id: {"status": "RUNNING"})

    response = client.post("/pipeline/run-123/retry-failed-stage")

    assert response.status_code == 400
    assert response.json()["detail"] == "Only failed runs can retry a failed stage."


def test_retry_failed_stage_submits_file_resume(monkeypatch):
    recorded = {}
    checkpoint = {"status": "FAILED", "source": "sftp", "error": "boom"}

    monkeypatch.setattr("services.pipeline_runtime.load_checkpoint_state", lambda run_id: checkpoint)
    monkeypatch.setattr(
        "api.services.pipeline_service.clean_checkpoint_for_resume",
        lambda state: {"status": "RUNNING", "source": state["source"]},
    )
    monkeypatch.setattr(
        "services.pipeline_runtime.save_checkpoint_state",
        lambda run_id, state: recorded.update({"saved_run_id": run_id, "saved_state": state}),
    )
    monkeypatch.setattr(
        "services.pipeline_runtime.submit_background",
        lambda run_id, stage, fn, *args: recorded.update(
            {"background_run_id": run_id, "background_stage": stage, "background_fn": fn.__name__, "background_args": args}
        ),
    )

    response = client.post("/pipeline/run-123/retry-failed-stage")

    assert response.status_code == 200
    assert response.json() == {"run_id": "run-123", "status": "SUBMITTED", "action": "retry_failed_stage"}
    assert recorded["saved_state"]["status"] == "RUNNING"
    assert recorded["background_stage"] == "file_resume"
    assert recorded["background_fn"] == "continue_file_pipeline_job"


def test_runs_returns_empty_list_on_timeout(monkeypatch):
    class StubFuture:
        def result(self, timeout):
            raise FutureTimeoutError()

    class StubExecutor:
        def submit(self, fn, *args, **kwargs):
            return StubFuture()

    monkeypatch.setattr("api.routers.runs_router.RUN_LIST_EXECUTOR", StubExecutor())

    response = client.get("/runs")

    assert response.status_code == 200
    assert response.json() == []


def test_runs_skips_bad_rows_and_summary_failures(monkeypatch):
    class StubFuture:
        def result(self, timeout):
            return [{"run_id": "good-run"}, {"run_id": None}, {"run_id": "bad-run"}]

    class StubExecutor:
        def submit(self, fn, *args, **kwargs):
            return StubFuture()

    def fake_summary(run_id):
        if run_id == "bad-run":
            raise RuntimeError("summary failed")
        return {"run_id": run_id, "status": "SUCCESS"}

    monkeypatch.setenv("ATHENA_RUNS_FAST_SUMMARY", "false")
    monkeypatch.setattr("api.routers.runs_router.RUN_LIST_EXECUTOR", StubExecutor())
    monkeypatch.setattr("api.services.ui_service.ui_run_summary", fake_summary)

    response = client.get("/runs")

    assert response.status_code == 200
    payload = response.json()
    assert payload[0] == {"run_id": "good-run", "status": "SUCCESS"}
    assert payload[1]["run_id"] == "bad-run"
    assert payload[1]["status"] == "UNKNOWN"


def test_runs_uses_fast_checkpoint_summary_by_default(monkeypatch):
    monkeypatch.delenv("ATHENA_RUNS_FAST_SUMMARY", raising=False)
    monkeypatch.setattr(
        "services.pipeline_runtime.list_runs",
        lambda limit: [{"run_id": "run-fast", "last_activity": "2026-06-30T00:00:00Z"}],
    )
    monkeypatch.setattr(
        "services.pipeline_runtime.load_checkpoint_state",
        lambda run_id: {
            "run_id": run_id,
            "brd_filename": "fast-summary.docx",
            "source": "database",
            "status": "RUNNING",
            "next_gate": 2,
            "resume_message": "Table Review is pending.",
        },
    )

    def fail_full_summary(run_id):
        raise AssertionError("ui_run_summary should not be called by default /runs")

    monkeypatch.setattr("api.services.ui_service.ui_run_summary", fail_full_summary)

    response = client.get("/runs")

    assert response.status_code == 200
    payload = response.json()
    assert payload[0]["id"] == "run-fast"
    assert payload[0]["brd_filename"] == "fast-summary.docx"
    assert payload[0]["next_gate"] == 2
    assert payload[0]["resume_message"] == "Table Review is pending."


def test_run_detail_returns_fallback_on_failure(monkeypatch):
    monkeypatch.setattr(
        "api.services.ui_service.ui_run",
        lambda run_id, include_scripts=True: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    monkeypatch.setattr("services.pipeline_runtime.load_checkpoint_state", lambda run_id: {"status": "RUNNING"})

    response = client.get("/runs/run-123")

    assert response.status_code == 200
    payload = response.json()
    assert payload["run_id"] == "run-123"
    assert payload["status"] == "RUNNING"
    assert payload["checkpoint"] == {"status": "RUNNING"}


def test_settings_roundtrip():
    response = client.get("/settings")
    assert response.status_code == 200
    assert response.json()["provider"] == "azure_openai"

    payload = {"provider": "azure_openai", "budget": 42}
    response = client.put("/settings", json=payload)
    assert response.status_code == 200
    assert response.json()["budget"] == 42


def test_configuration_crud_endpoints():
    create_response = client.post("/configurations", json={"name": "custom"})
    assert create_response.status_code == 200
    created = create_response.json()
    assert created["name"] == "custom"
    assert created["id"]

    update_response = client.put(f"/configurations/{created['id']}", json={"name": "updated"})
    assert update_response.status_code == 200
    assert update_response.json() == {"name": "updated", "id": created["id"]}

    delete_response = client.delete(f"/configurations/{created['id']}")
    assert delete_response.status_code == 200
    assert delete_response.json() == {"id": created["id"], "deleted": True}
