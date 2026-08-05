from __future__ import annotations

from services.pipeline_runtime import _finalize_interrupted_run_recovery, _interrupted_checkpoint_state


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


def test_recovery_recheck_does_not_fail_a_checkpoint_replaced_by_continuing_worker(monkeypatch):
    saved = []
    monkeypatch.setattr(
        "services.pipeline_runtime.load_checkpoint_state",
        lambda run_id: {"run_id": run_id, "status": "SUCCESS"},
    )
    monkeypatch.setattr("services.pipeline_runtime.save_checkpoint_state", lambda run_id, state: saved.append(state))

    changed = _finalize_interrupted_run_recovery("run-continued", "old-token", "restart")

    assert changed is False
    assert saved == []


def test_recovery_recheck_fails_only_untouched_active_checkpoint(monkeypatch):
    saved = []
    monkeypatch.setattr(
        "services.pipeline_runtime.load_checkpoint_state",
        lambda run_id: {
            "run_id": run_id,
            "status": "RUNNING",
            "background_stage": "gold_code_execution",
            "restart_recovery_token": "recovery-token",
        },
    )
    monkeypatch.setattr("services.pipeline_runtime.save_checkpoint_state", lambda run_id, state: saved.append(state))

    changed = _finalize_interrupted_run_recovery("run-stopped", "recovery-token", "restart")

    assert changed is True
    assert saved[0]["status"] == "FAILED"
    assert saved[0]["failed_background_stage"] == "gold_code_execution"
    assert "restart_recovery_token" not in saved[0]
