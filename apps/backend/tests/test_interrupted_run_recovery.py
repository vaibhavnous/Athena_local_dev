from __future__ import annotations

from services.pipeline_runtime import _interrupted_checkpoint_state


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
    assert recovered["self_healing_action"] == "MANUAL_REQUIRED"
    assert "Retry Failed Stage" in recovered["resume_message"]


def test_interrupted_checkpoint_state_auto_resume_for_idempotent_stage():
    state = {
        "run_id": "run-2",
        "status": "RUNNING",
        "background_stage": "discovery",
        "source": "database",
    }

    recovered = _interrupted_checkpoint_state(state, "Backend process restarted while this run was active.")

    assert recovered["failed_background_stage"] == "discovery"
    assert recovered["self_healing_action"] == "AUTO_RETRY"
    assert "Self-healing will replay" in recovered["resume_message"]
