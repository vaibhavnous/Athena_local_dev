from __future__ import annotations

import json

from services import pipeline_runtime
from services.pipeline_runtime import (
    _interrupted_checkpoint_state,
    _restart_recovery_checkpoint_state,
    _restart_recovery_stage_key,
)


def test_interrupted_checkpoint_state_preserves_failed_stage_for_retry():
    state = {
        "run_id": "run-1",
        "status": "RUNNING",
        "background_stage": "silver_code_execution",
        "silver_generation_status": "COMPLETED",
    }

    recovered = _interrupted_checkpoint_state(state, "Backend process restarted while this run was active.")

    assert recovered["status"] == "FAILED"
    assert recovered["background_stage"] is None
    assert recovered["failed_background_stage"] == "silver_code_execution"
    assert recovered["interrupted_by_backend_restart"] is True
    assert "Retry Failed Stage" in recovered["resume_message"]


def test_interrupted_execution_between_layers_recovers_next_execution_stage():
    state = {
        "run_id": "run-generation-first",
        "status": "RUNNING",
        "database_flow_version": "generation_first_v1",
        "background_stage": None,
        "next_stage_key": "silver_code_execution",
        "databricks_bronze_execution_status": "COMPLETED",
    }

    recovered = _interrupted_checkpoint_state(state, "Backend process restarted while this run was active.")

    assert recovered["failed_background_stage"] == "silver_code_execution"


def test_restarted_profiling_checkpoint_can_resume_automatically():
    state = {
        "run_id": "run-profiling",
        "source": "database",
        "status": "RUNNING",
        "background_stage": "profiling",
        "column_profiling_status": "IN_PROGRESS",
    }

    assert _restart_recovery_stage_key(state) == "profiling"

    recovered = _restart_recovery_checkpoint_state(
        state,
        "Backend process restarted while this run was active.",
        "profiling",
    )

    assert recovered["status"] == "RUNNING"
    assert recovered["background_stage"] == "profiling"
    assert recovered["failed_background_stage"] is None
    assert recovered["error"] is None
    assert recovered["backend_restart_recovery_stage"] == "profiling"
    assert "resuming automatically" in recovered["resume_message"]


def test_startup_resubmits_recoverable_profiling_checkpoint(monkeypatch):
    rows = [
        (
            "run-profiling",
            json.dumps(
                {
                    "run_id": "run-profiling",
                    "source": "database",
                    "status": "RUNNING",
                    "background_stage": "profiling",
                }
            ),
        )
    ]
    saved = []
    submitted = []

    class Cursor:
        def execute(self, *_args, **_kwargs):
            return None

        def fetchall(self):
            return rows

    class Conn:
        def cursor(self):
            return Cursor()

        def close(self):
            return None

    monkeypatch.setattr(pipeline_runtime, "get_connection", lambda: Conn())
    monkeypatch.setattr(pipeline_runtime, "save_checkpoint_state", lambda run_id, state: saved.append((run_id, state)))
    monkeypatch.setattr(
        pipeline_runtime,
        "submit_background",
        lambda run_id, stage, fn, *args, **kwargs: submitted.append((run_id, stage, fn, args, kwargs)),
    )

    recovered = pipeline_runtime.mark_interrupted_background_runs_on_startup()

    assert recovered == 1
    assert saved[0][0] == "run-profiling"
    assert saved[0][1]["status"] == "RUNNING"
    assert saved[0][1]["background_stage"] == "profiling"
    assert saved[0][1]["backend_restart_recovery_stage"] == "profiling"
    assert submitted[0][0] == "run-profiling"
    assert submitted[0][1] == "profiling"
    assert submitted[0][4] == {"enforce_capacity": False}
