from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from services.pipeline_runtime import get_run_context, load_checkpoint_state
from utilis.logger import logger

from api import utils as api_utils


def status_from_context(context: Dict[str, Any]) -> str:
    checkpoint = context.get("checkpoint") or {}
    if checkpoint.get("background_stage"):
        return "RUNNING"
    status = str(context.get("status") or "UNKNOWN")
    if status in {"RUNNING", "PROCESSING", "PENDING", "SUBMITTED", "IN_PROGRESS"}:
        return "RUNNING"
    if status == "PAUSED_FOR_STAGE_CONFIRMATION" or (context.get("stage_confirmation") or {}).get("awaiting_confirmation"):
        return "PAUSED_FOR_STAGE_CONFIRMATION"
    if context.get("pending_gate1") or context.get("next_gate") in {1, 2, 3, 4, 5}:
        return "HITL_WAIT"
    if status in {"UNKNOWN", "NOT_FOUND"}:
        return "NOT_FOUND"
    if status == "HITL_WAIT":
        return "HITL_WAIT"
    if status == "ABORTED":
        return "ABORTED"
    if status in {"PIPELINE_COMPLETED", "COMPLETED"}:
        return "SUCCESS"
    if status == "FAILED":
        return "FAILED"
    return status


def display_run_name(checkpoint: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> str:
    if api_utils.is_file_source(checkpoint.get("source")):
        from services.sftp_runtime import build_sftp_display_name

        return (context or {}).get("display_name") or build_sftp_display_name(checkpoint)
    return checkpoint.get("brd_filename") or "athena_brd.txt"


def failed_stage_key(checkpoint: Dict[str, Any], pipeline_steps: List[Dict[str, Any]]) -> Optional[str]:
    return (
        checkpoint.get("failed_background_stage")
        or api_utils.stage_key(checkpoint.get("background_stage"))
        or api_utils.stage_key(checkpoint.get("last_completed_stage_key"))
        or next(
            (
                step.get("key")
                for step in pipeline_steps
                if str(step.get("state") or "").upper() == "FAILED"
            ),
            None,
        )
    )


def get_run_data(run_id: str) -> Tuple[Dict[str, Any], Dict[str, Any], List[Dict[str, Any]], Dict[str, Any]]:
    checkpoint_hint = load_checkpoint_state(run_id) or {}
    if api_utils.is_file_source(checkpoint_hint.get("source")):
        from services.sftp_runtime import get_sftp_run_context

        context = get_sftp_run_context(run_id)
    else:
        context = get_run_context(run_id)
    summary = context.get("summary") or []
    checkpoint = context.get("checkpoint") or checkpoint_hint
    logger.debug("Loaded run UI data run_id=%s source=%s", run_id, checkpoint.get("source"))
    return checkpoint_hint, context, summary, checkpoint
