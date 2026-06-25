from concurrent.futures import TimeoutError as FutureTimeoutError
from pathlib import Path

from fastapi.testclient import TestClient

from api.main import app


client = TestClient(app)


def test_health_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["service"] == "athena-fastapi"
    assert body["embeddings"]["ready"] is False
    assert body["embeddings"]["reason"] == "Semantic indexing is running in fallback mode"


def test_pipeline_run_requires_brd_text_for_database_source():
    payload = {"source": "database", "brd_text": ""}
    response = client.post("/pipeline/run", json=payload)
    assert response.status_code == 400
    assert response.json()["detail"] == "brd_text is required"


def test_pipeline_run_requires_brd_text_for_file_source():
    response = client.post("/pipeline/run", json={"source": "sftp", "brd_text": ""})

    assert response.status_code == 400
    assert response.json()["detail"] == "brd_text is required"


def test_pipeline_run_accepts_file_source_with_brd_text(monkeypatch):
    saved = {}

    monkeypatch.setattr("api.routers.pipeline_router.load_checkpoint_state", lambda run_id: None)
    monkeypatch.setattr(
        "api.routers.pipeline_router.save_checkpoint_state",
        lambda run_id, state: saved.update({"run_id": run_id, "state": state}),
    )
    monkeypatch.setattr(
        "api.routers.pipeline_router.submit_pipeline_start",
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
    monkeypatch.setattr("api.routers.pipeline_router.load_checkpoint_state", lambda run_id: None)
    monkeypatch.setattr(
        "api.routers.pipeline_router.save_checkpoint_state",
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
    monkeypatch.setattr("api.routers.pipeline_router.ui_run", lambda run_id: {"status": "NOT_FOUND"})

    response = client.get("/pipeline/run-123/status")

    assert response.status_code == 404
    assert response.json()["detail"] == "Run not found: run-123"


def test_pipeline_status_shapes_running_response(monkeypatch):
    monkeypatch.setattr(
        "api.routers.pipeline_router.ui_run",
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
    monkeypatch.setattr("api.routers.pipeline_router.load_checkpoint_state", lambda run_id: {"run_id": run_id, "status": "RUNNING"})
    monkeypatch.setattr("api.routers.pipeline_router.save_checkpoint_state", lambda run_id, state: saved.update({"run_id": run_id, "state": state}))

    response = client.post("/pipeline/run-123/abort")

    assert response.status_code == 200
    assert response.json() == {"run_id": "run-123", "status": "ABORTED"}
    assert saved["state"]["status"] == "ABORTED"


def test_continue_stage_requires_pending_stage(monkeypatch):
    monkeypatch.setattr("api.routers.pipeline_router.load_checkpoint_state", lambda run_id: {})

    response = client.post("/pipeline/run-123/continue-stage", json={"auto_advance": False})

    assert response.status_code == 400
    assert response.json()["detail"] == "No next stage is pending confirmation for this run."


def test_continue_stage_rejects_file_source(monkeypatch):
    monkeypatch.setattr(
        "api.routers.pipeline_router.load_checkpoint_state",
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

    monkeypatch.setattr("api.routers.pipeline_router.load_checkpoint_state", lambda run_id: checkpoint)
    monkeypatch.setattr(
        "api.routers.pipeline_router.save_checkpoint_state",
        lambda run_id, state: recorded.update({"saved_run_id": run_id, "saved_state": state}),
    )
    monkeypatch.setattr(
        "api.routers.pipeline_router.submit_background",
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


def test_retry_failed_stage_rejects_non_failed_run(monkeypatch):
    monkeypatch.setattr("api.routers.pipeline_router.load_checkpoint_state", lambda run_id: {"status": "RUNNING"})

    response = client.post("/pipeline/run-123/retry-failed-stage")

    assert response.status_code == 400
    assert response.json()["detail"] == "Only failed runs can retry a failed stage."


def test_retry_failed_stage_submits_file_resume(monkeypatch):
    recorded = {}
    checkpoint = {"status": "FAILED", "source": "sftp", "error": "boom"}

    monkeypatch.setattr("api.routers.pipeline_router.load_checkpoint_state", lambda run_id: checkpoint)
    monkeypatch.setattr(
        "api.routers.pipeline_router.clean_checkpoint_for_resume",
        lambda state: {"status": "RUNNING", "source": state["source"]},
    )
    monkeypatch.setattr(
        "api.routers.pipeline_router.save_checkpoint_state",
        lambda run_id, state: recorded.update({"saved_run_id": run_id, "saved_state": state}),
    )
    monkeypatch.setattr(
        "api.routers.pipeline_router.submit_background",
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

    monkeypatch.setattr("api.routers.runs_router.BACKGROUND_EXECUTOR", StubExecutor())

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

    monkeypatch.setattr("api.routers.runs_router.BACKGROUND_EXECUTOR", StubExecutor())
    monkeypatch.setattr("api.routers.runs_router.ui_run_summary", fake_summary)

    response = client.get("/runs")

    assert response.status_code == 200
    assert response.json() == [{"run_id": "good-run", "status": "SUCCESS"}]


def test_run_detail_returns_503_on_failure(monkeypatch):
    monkeypatch.setattr(
        "api.routers.runs_router.ui_run",
        lambda run_id, include_scripts=True: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    response = client.get("/runs/run-123")

    assert response.status_code == 503
    assert response.json()["detail"] == "Failed to fetch run"


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
