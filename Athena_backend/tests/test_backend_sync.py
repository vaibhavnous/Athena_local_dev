from __future__ import annotations

import os

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("ATHENA_DEMO_MODE", "false")

from api.main import app
from api.services.ui import run_ui_service


client = TestClient(app)


def test_ui_run_builds_cross_layer_payload(monkeypatch):
    context = {
        "summary": [{"stored_at": "2026-06-17T12:00:00+00:00", "token_count": 12, "cost_usd": 0.5}],
        "checkpoint": {"source": "database", "provider": "azure_openai", "deployment": "dep1"},
        "pipeline_steps": [{"key": "kpis", "label": "KPI Extraction", "state": "COMPLETED"}],
        "bronze": {"scripts": [{"id": 1}]},
        "silver": {"scripts": []},
        "gold": {"scripts": []},
        "nominated_tables": [{"table_name": "t1"}],
        "certified_tables": [],
        "enriched_metadata": {"columns": []},
        "enriched_columns": [],
        "enriched_joins": [],
        "semantic_counts": {},
        "pii_columns": [],
        "join_key_columns": [],
        "measure_columns": [],
        "feed_semantic_summary": [],
        "gate3_approved": False,
        "next_gate": 2,
        "resume_message": "Table Review is pending.",
        "stage_confirmation": None,
        "sftp_entity": None,
        "source_row_count": None,
        "source_columns": [],
    }
    summary = context["summary"]
    checkpoint = context["checkpoint"]

    monkeypatch.setattr(
        run_ui_service,
        "get_run_data",
        lambda run_id: ({}, context, summary, checkpoint),
    )
    monkeypatch.setattr(run_ui_service, "fetch_json_artifact", lambda run_id, artifact: {"business_objective": "Grow revenue"})
    monkeypatch.setattr(
        run_ui_service,
        "build_kpis",
        lambda run_id, checkpoint: ([{"id": "k1", "name": "Revenue"}], []),
    )
    monkeypatch.setattr(run_ui_service, "hitl_decisions", lambda run_id, context, hitl_rows=None: [])
    monkeypatch.setattr(
        run_ui_service,
        "ui_stages",
        lambda context, run_id: [{"key": "kpis", "status": "COMPLETED"}],
    )
    monkeypatch.setattr(run_ui_service, "display_run_name", lambda checkpoint, context=None: "athena_brd.txt")

    payload = run_ui_service.ui_run("run-ui", include_scripts=False)

    assert payload["run_id"] == "run-ui"
    assert payload["status"] == "HITL_WAIT"
    assert payload["requirements"]["business_objective"] == "Grow revenue"
    assert payload["kpis"][0]["name"] == "Revenue"
    assert payload["script_counts"]["bronze"] == 1
    assert payload["next_gate"] == 2


def test_display_run_name_prefers_submitted_brd_filename():
    from api.services.ui.shared import display_run_name

    assert display_run_name({"run_id": "run-name", "source": "database", "brd_filename": "Claims BRD"}) == "Claims BRD"
    assert display_run_name({"run_id": "run-name", "source": "database"}) == "run-name"


def test_exact_memory_match_does_not_preload_requirements_or_kpis():
    from nodes.memory_lookup import _apply_match_result

    state = {"run_id": "run-memory", "fingerprint": "fp1"}
    result = _apply_match_result(
        state,
        True,
        {"business_objective": "cached objective"},
        {"kpis": [{"kpi_name": "Cached KPI"}]},
        {"node": "test"},
    )

    assert result["memory_layer1"] is True
    assert result["memory_bypass"] is False
    assert result["memory_exact_requirements_found"] is True
    assert result["memory_exact_kpi_count"] == 1
    assert "req_business_objective" not in result
    assert "kpis" not in result
    assert "prior_kpis" not in result


def test_ui_status_prefers_background_stage_over_stale_stage_confirmation():
    context = {
        "checkpoint": {
            "status": "PAUSED_FOR_STAGE_CONFIRMATION",
            "background_stage": "enrichment",
        },
        "status": "PAUSED_FOR_STAGE_CONFIRMATION",
        "next_gate": None,
        "pending_gate1": [],
    }

    assert run_ui_service.status_from_context(context) == "RUNNING"


def test_ui_status_uses_reconciled_context_over_stale_checkpoint_pause():
    context = {
        "checkpoint": {
            "status": "PAUSED_FOR_STAGE_CONFIRMATION",
        },
        "status": "PIPELINE_COMPLETED",
        "stage_confirmation": None,
        "next_gate": None,
        "pending_gate1": [],
    }

    assert run_ui_service.status_from_context(context) == "SUCCESS"


def test_pipeline_status_endpoint_syncs_with_ui_service(monkeypatch):
    monkeypatch.setattr(
        "api.services.ui_service.ui_run",
        lambda run_id: {
            "status": "SUCCESS",
            "run_id": run_id,
            "stages": [{"key": "gold", "status": "COMPLETED"}],
        },
    )

    response = client.get("/pipeline/run-sync/status")

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == "run-sync"
    assert body["status"] == "SUCCESS"
    assert body["state"]["life_cycle_state"] == "TERMINATED"
    assert body["run"]["stages"][0]["key"] == "gold"


def test_run_detail_endpoint_returns_scripts_from_ui_layer(monkeypatch):
    monkeypatch.setattr(
        "api.services.ui_service.ui_run",
        lambda run_id, include_scripts=True: {
            "run_id": run_id,
            "status": "SUCCESS",
            "bronze": {"scripts": [{"name": "bronze.py"}]},
            "silver": {"scripts": []},
            "gold": {"scripts": []},
        },
    )

    response = client.get("/runs/run-sync")

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == "run-sync"
    assert body["bronze"]["scripts"][0]["name"] == "bronze.py"


def test_mocked_pipeline_progression_stays_in_sync(monkeypatch):
    state = {
        "run_id": "run-progress",
        "status": "HITL_WAIT",
        "next_gate": 1,
        "resume_message": "KPI Review is pending.",
    }
    decisions = []

    def fake_ui_run(run_id):
        return {
            "run_id": run_id,
            "status": state["status"],
            "next_gate": state["next_gate"],
            "resume_message": state["resume_message"],
            "stages": [{"key": "gate1", "status": state["status"]}],
        }

    def fake_update_hitl_item(queue_id, action, **kwargs):
        decisions.append((queue_id, action))
        state["status"] = "SUCCESS"
        state["next_gate"] = None
        state["resume_message"] = "Pipeline completed."

    monkeypatch.setattr("api.services.ui_service.ui_run", fake_ui_run)
    monkeypatch.setattr("utilis.db.update_hitl_item", fake_update_hitl_item)
    monkeypatch.setattr("api.services.kpi_service.maybe_resume_gate1", lambda run_id: None)

    before = client.get("/pipeline/run-progress/status")
    assert before.status_code == 200
    assert before.json()["status"] == "HITL_WAIT"
    assert before.json()["run"]["next_gate"] == 1

    approve = client.post("/kpi-reviews/run-progress:1:kpi-1/approve", json={})
    assert approve.status_code == 200
    assert approve.json()["status"] == "APPROVED"
    assert decisions == [("run-progress:1:kpi-1", "APPROVED")]

    after = client.get("/pipeline/run-progress/status")
    assert after.status_code == 200
    assert after.json()["status"] == "SUCCESS"
    assert after.json()["state"]["life_cycle_state"] == "TERMINATED"


def test_active_pipeline_status_uses_checkpoint_snapshot(monkeypatch):
    checkpoint = {
        "run_id": "run-active",
        "status": "RUNNING",
        "source": "database",
        "background_stage": "silver_code_execution",
        "silver_generation_status": "COMPLETED",
    }

    monkeypatch.setattr("services.pipeline_runtime.load_checkpoint_state", lambda run_id: checkpoint)
    monkeypatch.setattr(
        "api.services.ui_service.ui_run",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("active status should not hydrate full UI state")),
    )

    response = client.get("/pipeline/run-active/status")

    assert response.status_code == 200
    assert response.json()["run"]["status"] == "RUNNING"
    assert response.json()["run"]["pipeline_steps"]


def test_snowflake_bronze_review_submission_reports_execution_stage(monkeypatch):
    submitted = {}
    monkeypatch.setattr(
        "services.pipeline_runtime.load_checkpoint_state",
        lambda run_id: {"run_id": run_id, "source": "database", "target_warehouse": "snowflake"},
    )
    monkeypatch.setattr(
        "services.pipeline_runtime.submit_background",
        lambda run_id, stage, fn, *args: submitted.update({"run_id": run_id, "stage": stage}),
    )

    response = client.post(
        "/bronze-reviews/run-bronze-transition",
        json={"action": "APPROVED", "review_artifact": {"feeds": [{"table": "claims"}]}},
    )

    assert response.status_code == 200
    assert submitted == {"run_id": "run-bronze-transition", "stage": "bronze_code_execution"}


def test_databricks_bronze_review_reports_merge_key_stage_when_execution_disabled(monkeypatch):
    submitted = {}
    monkeypatch.setattr(
        "services.pipeline_runtime.load_checkpoint_state",
        lambda run_id: {"run_id": run_id, "source": "database", "target_warehouse": "databricks"},
    )
    monkeypatch.setattr("services.databricks_runtime.databricks_bronze_execution_enabled", lambda: False)
    monkeypatch.setattr(
        "services.pipeline_runtime.submit_background",
        lambda run_id, stage, fn, *args: submitted.update({"run_id": run_id, "stage": stage}),
    )

    response = client.post(
        "/bronze-reviews/run-databricks-merge-transition",
        json={"action": "APPROVED", "review_artifact": {"feeds": [{"table": "claims"}]}},
    )

    assert response.status_code == 200
    assert submitted == {"run_id": "run-databricks-merge-transition", "stage": "silver_merge_key_review"}


def test_silver_merge_review_submission_reports_generation_stage(monkeypatch):
    submitted = {}
    monkeypatch.setattr(
        "services.pipeline_runtime.submit_background",
        lambda run_id, stage, fn, *args: submitted.update({"run_id": run_id, "stage": stage}),
    )

    response = client.post(
        "/silver-merge-key-reviews/run-silver-transition",
        json={"action": "APPROVED", "review_artifact": {"feeds": [{"table": "claims"}]}},
    )

    assert response.status_code == 200
    assert submitted == {"run_id": "run-silver-transition", "stage": "silver"}


def test_hitl_batch_submit_returns_503_when_decision_persistence_fails(monkeypatch):
    def fail_update_hitl_item(*args, **kwargs):
        raise RuntimeError("pipeline database unavailable")

    monkeypatch.setattr("utilis.db.update_hitl_item", fail_update_hitl_item)

    response = client.post(
        "/hitl/run-db/decisions",
        json={"decisions": [{"kpi_id": "run-db:1:kpi-1", "decision": "APPROVED"}]},
    )

    assert response.status_code == 503
    assert "Failed to persist KPI decision" in response.json()["detail"]


def test_update_hitl_item_rejects_missing_item(monkeypatch):
    from utilis.db import update_hitl_item

    class Cursor:
        rowcount = 0

        def execute(self, *args, **kwargs):
            return None

    class Connection:
        def cursor(self):
            return Cursor()

        def commit(self):
            raise AssertionError("missing HITL item should not be committed")

        def close(self):
            return None

    monkeypatch.setattr("utilis.db.get_pipeline_connection", lambda: Connection())

    with pytest.raises(LookupError, match="HITL item not found"):
        update_hitl_item("missing-item", "APPROVED")
