from __future__ import annotations

from fastapi.testclient import TestClient

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


def test_pipeline_status_endpoint_syncs_with_ui_service(monkeypatch):
    monkeypatch.setattr(
        "api.routers.pipeline_router.ui_run",
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
        "api.routers.runs_router.ui_run",
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
