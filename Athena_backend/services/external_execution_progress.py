from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from utilis.logger import logger

_PROGRESS_SAVE_LOCK = threading.Lock()
_LAST_PROGRESS_SAVE_AT: Dict[str, float] = {}


def _progress_save_interval_seconds() -> float:
    raw = os.getenv("ATHENA_EXTERNAL_PROGRESS_SAVE_INTERVAL_SECONDS", "10")
    try:
        return max(0.0, float(raw))
    except ValueError:
        logger.warning("Invalid ATHENA_EXTERNAL_PROGRESS_SAVE_INTERVAL_SECONDS=%r; using 10 seconds", raw)
        return 10.0


def _should_save_progress(run_id: Any, layer: str, status: str) -> bool:
    if status != "RUNNING":
        return True
    interval = _progress_save_interval_seconds()
    if interval <= 0:
        return True
    key = f"{run_id}:{layer}"
    now = time.monotonic()
    with _PROGRESS_SAVE_LOCK:
        last_saved_at = _LAST_PROGRESS_SAVE_AT.get(key)
        if last_saved_at is not None and now - last_saved_at < interval:
            return False
        _LAST_PROGRESS_SAVE_AT[key] = now
    return True


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
        "background_stage": None if status == "COMPLETED" else stage_key,
        "failed_background_stage": None if status == "COMPLETED" else state.get("failed_background_stage"),
        "last_failed_stage_key": None if status == "COMPLETED" else state.get("last_failed_stage_key"),
        "error": None if status == "COMPLETED" else state.get("error"),
        "external_execution": progress,
        f"snowflake_{layer}_execution_status": status,
        f"snowflake_{layer}_execution_progress": progress,
        "resume_message": message or state.get("resume_message"),
    }

    # ponytail: keep exact logs, but avoid rewriting the large checkpoint on every fast per-script tick.
    if not _should_save_progress(run_id, layer, status):
        return updated

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
