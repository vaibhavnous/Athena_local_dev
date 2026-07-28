from __future__ import annotations

from typing import Any, Callable, Dict

from services.sftp_stage_registry import stage_spec
from state import Stage01State


COMPLETED_STAGE_STATUSES = {"COMPLETED", "COMPLETED_WITH_WARNINGS", "PASSED", "READY", "HANDOFF_ONLY"}


def execute_sftp_stage(
    state: Stage01State,
    stage_key: str,
    runner: Callable[[Stage01State], Stage01State],
) -> Stage01State:
    """Execute one canonical stage and persist both sides of its handoff."""
    from services.pipeline_runtime import save_checkpoint_state_timed

    spec = stage_spec(stage_key)
    run_id = str(state.get("run_id") or "").strip()
    if not run_id:
        raise ValueError(f"{stage_key} requires run_id")

    statuses = dict(state.get("stage_statuses") or {})
    statuses[stage_key] = "RUNNING"
    running_state: Stage01State = {
        **state,
        "status": "RUNNING",
        "current_stage": stage_key,
        "background_stage": stage_key,
        "stage_statuses": statuses,
        "pipeline_revision": int(state.get("pipeline_revision") or 0) + 1,
        "resume_message": f"{spec.label} is running.",
    }
    if spec.checkpoint_policy == "before_after":
        save_checkpoint_state_timed(run_id, running_state, context=f"{stage_key}:running")

    try:
        result = runner(running_state)
        if not isinstance(result, dict):
            raise ValueError(f"{stage_key} returned a non-dictionary state")
    except Exception as exc:
        failed_statuses = dict(statuses)
        failed_statuses[stage_key] = "FAILED"
        failed_errors = dict(state.get("stage_errors") or {})
        failed_errors[stage_key] = str(exc)
        failed_state: Stage01State = {
            **running_state,
            "status": "FAILED",
            "background_stage": None,
            "stage_statuses": failed_statuses,
            "stage_errors": failed_errors,
            "error": str(exc),
            "failed_background_stage": stage_key,
        }
        save_checkpoint_state_timed(run_id, failed_state, context=f"{stage_key}:failed")
        raise

    output_status = str(result.get(spec.status_field) or "").upper()
    if output_status not in COMPLETED_STAGE_STATUSES:
        error = str(result.get("error") or f"{stage_key} did not produce {spec.status_field}=COMPLETED")
        failed_statuses = dict(statuses)
        failed_statuses[stage_key] = "FAILED"
        failed_errors = dict(state.get("stage_errors") or {})
        failed_errors[stage_key] = error
        failed_state = {
            **running_state,
            **result,
            "status": "FAILED",
            "background_stage": None,
            "stage_statuses": failed_statuses,
            "stage_errors": failed_errors,
            "error": error,
            "failed_background_stage": stage_key,
        }
        save_checkpoint_state_timed(run_id, failed_state, context=f"{stage_key}:failed")
        return failed_state

    completed_statuses = dict(statuses)
    completed_statuses[stage_key] = "COMPLETED"
    completed_state: Stage01State = {
        **running_state,
        **result,
        "status": "RUNNING",
        "background_stage": None,
        "stage_statuses": completed_statuses,
        "pipeline_revision": int(running_state.get("pipeline_revision") or 0) + 1,
        "resume_message": f"{spec.label} completed.",
    }
    if spec.checkpoint_policy in {"complete", "before_after"}:
        save_checkpoint_state_timed(run_id, completed_state, context=f"{stage_key}:complete")
    return completed_state
