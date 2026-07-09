from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from utilis.logger import logger


def save_external_execution_progress(
    state: Dict[str, Any],
    *,
    run_id: Any,
    layer: str,
    stage_key: str,
    status: str,
    total_count: int,
    completed_count: int,
    current_index: Optional[int] = None,
    current_name: Optional[str] = None,
    current_target: Optional[str] = None,
    message: Optional[str] = None,
) -> Dict[str, Any]:
    if not run_id:
        return state

    progress = {
        "platform": "snowflake",
        "layer": layer,
        "stage_key": stage_key,
        "status": status,
        "total_count": total_count,
        "completed_count": completed_count,
        "current_index": current_index,
        "current_name": current_name,
        "current_target": current_target,
        "message": message,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    updated = {
        **state,
        "run_id": run_id,
        "status": "RUNNING" if status == "RUNNING" else state.get("status", "RUNNING"),
        "background_stage": stage_key,
        "external_execution": progress,
        f"snowflake_{layer}_execution_status": status,
        f"snowflake_{layer}_execution_progress": progress,
        "resume_message": message or state.get("resume_message"),
    }

    try:
        from services.pipeline_runtime import save_checkpoint_state

        save_checkpoint_state(str(run_id), updated)
    except Exception as exc:
        logger.warning(
            "External execution progress checkpoint save failed: %s",
            exc,
            extra={"run_id": str(run_id), "node": stage_key, "stage": stage_key, "step_name": "progress_checkpoint_failed"},
        )
    return updated
