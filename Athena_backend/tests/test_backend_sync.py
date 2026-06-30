from __future__ import annotations

import os

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
