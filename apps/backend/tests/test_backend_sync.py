from __future__ import annotations

import os

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("ATHENA_DEMO_MODE", "false")

from api.main import app
from api.auth import AuthUser, get_current_user
from api.services.ui import run_ui_service

app.dependency_overrides[get_current_user] = lambda: AuthUser(
    uid="test-user", username="Test User", email="test@example.com", userType="Admin"
)

client = TestClient(app)


def test_ui_run_builds_cross_layer_payload(monkeypatch):
    context = {
        "summary": [{"stored_at": "2026-06-17T12:00:00+00:00", "token_count": 12, "cost_usd": 0.5}],
        "checkpoint": {
            "source": "database",
            "provider": "azure_openai",
            "deployment": "dep1",
            "execution_ready": False,
            "awaiting_stage_confirmation": False,
            "next_stage_key": None,
            "next_stage_label": None,
        },
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
    assert payload["execution_ready"] is False
    assert payload["awaiting_stage_confirmation"] is False
    assert payload["next_stage_key"] is None
    assert payload["next_stage_label"] is None


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


def test_ui_failed_stage_key_ignores_running_background_stage():
    from api.services.ui.shared import failed_stage_key

    assert failed_stage_key(
        {"status": "RUNNING", "background_stage": "profiling"},
        [{"key": "profiling", "state": "RUNNING"}],
    ) is None


def test_metadata_setup_failed_stage_has_display_label():
    from api import utils as api_utils

    assert api_utils.stage_label_from_key("metadata_setup_execution", "database") == "Metadata Setup Execution"


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


def test_pipeline_status_endpoint_uses_checkpoint_snapshot(monkeypatch):
    monkeypatch.setattr(
        "services.pipeline_runtime.load_checkpoint_state",
        lambda run_id: {
            "status": "SUCCESS",
            "run_id": run_id,
            "gold_generation_status": "COMPLETED",
        },
    )

    response = client.get("/pipeline/run-sync/status")

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == "run-sync"
    assert body["status"] == "SUCCESS"
    assert body["state"]["life_cycle_state"] == "TERMINATED"
    assert body["run"]["pipeline_steps"]


def test_pipeline_summary_status_returns_only_history_fields(monkeypatch):
    monkeypatch.setattr(
        "services.pipeline_runtime.load_checkpoint_fields",
        lambda run_id, *fields: {
            "run_id": run_id,
            "status": "FAILED",
            "brd_filename": "history-run",
            "error": "Stage failed",
        },
    )

    response = client.get("/pipeline/run-history/summary-status")

    assert response.status_code == 200
    body = response.json()
    assert body["run"]["status"] == "FAILED"
    assert body["run"]["brd_filename"] == "history-run"
    assert "pipeline_steps" not in body["run"]
    assert "checkpoint" not in body["run"]


def test_run_detail_endpoint_uses_checkpoint_and_loads_scripts_separately(monkeypatch):
    monkeypatch.setattr(
        "api.routers.runs_router.assert_run_access",
        lambda run_id, user, checkpoint=None: {
            "run_id": run_id,
            "status": "SUCCESS",
            "bronze_generation_status": "COMPLETED",
        },
    )

    response = client.get("/runs/run-sync")

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == "run-sync"
    assert body["status"] == "SUCCESS"
    assert body["bronze"]["scripts"] == []


def test_mocked_pipeline_progression_stays_in_sync(monkeypatch):
    state = {
        "run_id": "run-progress",
        "status": "HITL_WAIT",
        "next_gate": 1,
        "resume_message": "KPI Review is pending.",
    }
    decisions = []

    def fake_update_hitl_item(queue_id, action, **kwargs):
        decisions.append((queue_id, action))
        state["status"] = "SUCCESS"
        state["next_gate"] = None
        state["resume_message"] = "Pipeline completed."

    monkeypatch.setattr("services.pipeline_runtime.load_checkpoint_state", lambda run_id: dict(state))
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


@pytest.mark.parametrize(
    ("last_completed_stage_key", "waiting_stage_key", "next_gate", "next_review_key"),
    [
        ("gate1", "gate1", 1, None),
        ("gate2", "gate2", 2, None),
        ("gate3", "gate3", 3, None),
        ("bronze", "gate4", 4, None),
        ("silver", "gate5", 5, None),
        ("gold", "gold_review", None, "gold_review"),
    ],
)
def test_checkpoint_fallback_reconstructs_each_database_review_frontier(
    last_completed_stage_key,
    waiting_stage_key,
    next_gate,
    next_review_key,
):
    from api.routers.runs_router import _fallback_run_detail

    checkpoint = {
        "run_id": "run-review-frontier",
        "source": "database",
        "target_warehouse": "databricks",
        "execution_engine": "native",
        "database_flow_version": "generation_first_v1",
        "status": "HITL_WAIT",
        "last_completed_stage_key": last_completed_stage_key,
        "brd_text": "Build reviewed medallion tables.",
        "memory_lookup_status": "COMPLETED",
        "extracted_requirements": {"requirements": ["Create curated claims data."]},
        "extracted_kpis": [{"name": "Claim Count"}],
        "bronze_generation_status": (
            "COMPLETED"
            if last_completed_stage_key in {"bronze", "silver", "gold"}
            else None
        ),
        "silver_generation_status": (
            "COMPLETED"
            if last_completed_stage_key in {"silver", "gold"}
            else None
        ),
        "gold_generation_status": (
            "COMPLETED"
            if last_completed_stage_key == "gold"
            else None
        ),
    }

    detail = _fallback_run_detail("run-review-frontier", checkpoint)
    steps = detail["pipeline_steps"]
    waiting_index = next(
        index for index, step in enumerate(steps)
        if step["key"] == waiting_stage_key
    )

    assert detail["status"] == "HITL_WAIT"
    assert detail["next_gate"] == next_gate
    assert detail["next_review_key"] == next_review_key
    assert all(step["state"] == "COMPLETED" for step in steps[:waiting_index])
    assert steps[waiting_index]["state"] == "HITL_WAIT"
    assert all(step["state"] == "PENDING" for step in steps[waiting_index + 1:])


def test_checkpoint_fallback_preserves_silver_merge_key_review_frontier():
    from api.routers.runs_router import _fallback_run_detail

    detail = _fallback_run_detail(
        "run-merge-review",
        {
            "run_id": "run-merge-review",
            "source": "database",
            "target_warehouse": "snowflake",
            "execution_engine": "dbt",
            "dbt_deployment_mode": "generate_and_deploy",
            "database_flow_version": "generation_first_v1",
            "status": "HITL_WAIT",
            "last_completed_stage_key": "bronze",
            "next_review_key": "silver_merge_key_review",
            "bronze_generation_status": "COMPLETED",
        },
    )
    steps = detail["pipeline_steps"]
    waiting_index = next(
        index for index, step in enumerate(steps)
        if step["key"] == "silver_merge_key_review"
    )

    assert all(step["state"] == "COMPLETED" for step in steps[:waiting_index])
    assert steps[waiting_index]["state"] == "HITL_WAIT"
    assert all(step["state"] == "PENDING" for step in steps[waiting_index + 1:])


def test_checkpoint_fallback_does_not_add_gold_review_to_legacy_flow():
    from api.routers.runs_router import _fallback_run_detail

    detail = _fallback_run_detail(
        "run-legacy-gold",
        {
            "run_id": "run-legacy-gold",
            "source": "database",
            "target_warehouse": "databricks",
            "status": "HITL_WAIT",
            "last_completed_stage_key": "gold",
            "gold_generation_status": "COMPLETED",
        },
    )

    assert detail["next_review_key"] is None
    assert "gold_review" not in {
        step["key"] for step in detail["pipeline_steps"]
    }


def test_checkpoint_fallback_preserves_v2_gold_review_frontier():
    from api.routers.runs_router import _fallback_run_detail

    detail = _fallback_run_detail(
        "run-v2-gold-review",
        {
            "run_id": "run-v2-gold-review",
            "source": "database",
            "target_warehouse": "snowflake",
            "execution_engine": "native",
            "database_flow_version": "generation_first_v2",
            "status": "HITL_WAIT",
            "last_completed_stage_key": "gold",
            "gold_generation_status": "COMPLETED",
            "next_review_key": "gold_review",
        },
    )

    assert detail["status"] == "HITL_WAIT"
    assert detail["next_review_key"] == "gold_review"
    assert detail["generation_first_execution"] is True
    assert next(
        step for step in detail["pipeline_steps"] if step["key"] == "gold_review"
    )["state"] == "HITL_WAIT"


@pytest.mark.parametrize(
    ("source", "target", "execution_engine", "dbt_mode", "active_stage"),
    [
        ("database", "databricks", "native", None, "kpis"),
        ("database", "snowflake", "native", None, "kpis"),
        ("database", "snowflake", "dbt", "generate_and_deploy", "kpis"),
        ("sftp", "databricks", "native", None, "schema"),
        ("adls_gen2", "snowflake", "native", None, "schema"),
    ],
)
def test_checkpoint_fallback_shows_active_stage_for_supported_flows(
    source,
    target,
    execution_engine,
    dbt_mode,
    active_stage,
):
    from api.routers.runs_router import _fallback_run_detail

    detail = _fallback_run_detail(
        "run-active-frontier",
        {
            "run_id": "run-active-frontier",
            "source": source,
            "target_warehouse": target,
            "execution_engine": execution_engine,
            "dbt_deployment_mode": dbt_mode,
            "database_flow_version": (
                "generation_first_v1"
                if source == "database"
                else None
            ),
            "status": "RUNNING",
            "background_stage": active_stage,
        },
    )
    steps = detail["pipeline_steps"]
    active_index = next(
        index for index, step in enumerate(steps)
        if step["key"] == active_stage
    )

    assert all(step["state"] == "COMPLETED" for step in steps[:active_index])
    assert steps[active_index]["state"] == "RUNNING"
    assert all(step["state"] == "PENDING" for step in steps[active_index + 1:])


def test_checkpoint_fallback_keeps_just_completed_stage_successful():
    from api.routers.runs_router import _fallback_run_detail

    detail = _fallback_run_detail(
        "run-stage-complete",
        {
            "run_id": "run-stage-complete",
            "source": "database",
            "target_warehouse": "databricks",
            "execution_engine": "native",
            "database_flow_version": "generation_first_v1",
            "status": "RUNNING",
            "background_stage": None,
            "last_completed_stage_key": "requirements",
            "next_stage_key": "kpis",
        },
    )
    by_key = {step["key"]: step for step in detail["pipeline_steps"]}

    assert by_key["requirements"]["state"] == "COMPLETED"
    assert by_key["requirements"]["complete"] is True
    assert by_key["kpis"]["state"] == "PENDING"


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


def test_databricks_bronze_review_reports_execution_stage(monkeypatch):
    submitted = {}
    monkeypatch.setattr(
        "services.pipeline_runtime.load_checkpoint_state",
        lambda run_id: {"run_id": run_id, "source": "database", "target_warehouse": "databricks"},
    )
    monkeypatch.setattr(
        "services.pipeline_runtime.submit_background",
        lambda run_id, stage, fn, *args: submitted.update({"run_id": run_id, "stage": stage}),
    )

    response = client.post(
        "/bronze-reviews/run-databricks-merge-transition",
        json={"action": "APPROVED", "review_artifact": {"feeds": [{"table": "claims"}]}},
    )

    assert response.status_code == 200
    assert submitted == {"run_id": "run-databricks-merge-transition", "stage": "bronze_code_execution"}


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
