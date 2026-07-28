from __future__ import annotations

from typing import Any, Dict, List, Optional

from api import utils as api_utils
from api.services.log_service import read_logs

UI_STAGE_LOG_LIMIT = 1000


def _metric_bucket(with_prompt_metadata: bool = True) -> Dict[str, Any]:
    bucket: Dict[str, Any] = {
        "tokens": 0,
        "cost": 0.0,
        "attempts": 0,
        "started_at": None,
        "completed_at": None,
    }
    if with_prompt_metadata:
        bucket["prompt_metadata"] = {"artifacts": []}
    return bucket


def _update_metric_times(bucket: Dict[str, Any], timestamp: Optional[str]) -> None:
    if timestamp and (not bucket["started_at"] or timestamp < bucket["started_at"]):
        bucket["started_at"] = timestamp
    if timestamp and (not bucket["completed_at"] or timestamp > bucket["completed_at"]):
        bucket["completed_at"] = timestamp


def _duration_seconds(started_at: Any, completed_at: Any) -> Optional[float]:
    start = api_utils.parse_iso(started_at)
    end = api_utils.parse_iso(completed_at)
    if not start or not end:
        return None
    try:
        return max(0.0, (end - start).total_seconds())
    except Exception:
        return None


def _stage_status(raw_state: Any) -> str:
    state = str(raw_state or "").upper()
    if state in {"COMPLETE", "COMPLETED"}:
        return "COMPLETED"
    if state == "FAILED":
        return "FAILED"
    if state in {"HITL_WAIT", "PAUSED_FOR_HITL"}:
        return "HITL_WAIT"
    if state in {"RUNNING", "IN_PROGRESS"}:
        return "RUNNING"
    return "PENDING"


def ui_stages(context: Dict[str, Any], run_id: str) -> List[Dict[str, Any]]:
    summary = context.get("summary") or []
    metrics: Dict[str, Dict[str, Any]] = {}

    for row in summary:
        key = api_utils.stage_key(row.get("stage")) or api_utils.stage_key(row.get("artifact_type"))
        if not key:
            continue
        bucket = metrics.setdefault(key, _metric_bucket(with_prompt_metadata=True))
        bucket["tokens"] += int(row.get("token_count") or 0)
        bucket["cost"] += float(row.get("cost_usd") or 0)
        bucket["attempts"] += int(row.get("retry_count") or 0)
        stored_at = api_utils.iso_or_none(row.get("stored_at"))
        _update_metric_times(bucket, stored_at)
        bucket["prompt_metadata"]["artifacts"].append(
            {
                "stage": row.get("stage"),
                "artifact_type": row.get("artifact_type"),
                "faithfulness_status": row.get("faithfulness_status"),
                "retry_count": row.get("retry_count"),
                "input_tokens": row.get("input_tokens"),
                "output_tokens": row.get("output_tokens"),
                "token_count": row.get("token_count"),
                "cost_usd": row.get("cost_usd"),
                "stored_at": stored_at,
            }
        )

    for log in read_logs(run_id, limit=UI_STAGE_LOG_LIMIT):
        key = api_utils.stage_key(log.get("stage")) or api_utils.stage_key(log.get("message"))
        if not key:
            continue
        bucket = metrics.setdefault(key, _metric_bucket(with_prompt_metadata=True))
        logged_at = log.get("logged_at")
        _update_metric_times(bucket, logged_at)
        if log.get("event_type") == "stage_end" and log.get("duration_seconds") is not None:
            bucket["duration_seconds"] = max(
                float(bucket.get("duration_seconds") or 0),
                float(log.get("duration_seconds") or 0),
            )

    return [
        {
            "id": f"stage_{index + 1:02d}",
            "key": step["key"],
            "name": step["label"],
            "status": _stage_status(step.get("state")),
            "tokens": (metrics.get(step["key"]) or {}).get("tokens", 0),
            "cost": (metrics.get(step["key"]) or {}).get("cost", 0.0),
            "attempts": (metrics.get(step["key"]) or {}).get("attempts", 0),
            "duration_seconds": (
                (metrics.get(step["key"]) or {}).get("duration_seconds")
                or _duration_seconds(
                    (metrics.get(step["key"]) or {}).get("started_at"),
                    (metrics.get(step["key"]) or {}).get("completed_at"),
                )
            ),
            "started_at": (metrics.get(step["key"]) or {}).get("started_at"),
            "completed_at": (metrics.get(step["key"]) or {}).get("completed_at"),
            "error": (context.get("checkpoint") or {}).get("error") if str(step.get("state") or "").upper() == "FAILED" else None,
            "prompt_metadata": (
                (metrics.get(step["key"]) or {}).get("prompt_metadata")
                if ((metrics.get(step["key"]) or {}).get("prompt_metadata", {}).get("artifacts"))
                else None
            ),
        }
        for index, step in enumerate(context.get("pipeline_steps", []))
    ]


def stage_metrics_from_summary(summary: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    metrics: Dict[str, Dict[str, Any]] = {}
    for row in summary:
        key = api_utils.stage_key(row.get("stage")) or api_utils.stage_key(row.get("artifact_type"))
        if not key:
            continue
        bucket = metrics.setdefault(key, _metric_bucket(with_prompt_metadata=False))
        bucket["tokens"] += int(row.get("token_count") or 0)
        bucket["cost"] += float(row.get("cost_usd") or 0)
        bucket["attempts"] += int(row.get("retry_count") or 0)
        _update_metric_times(bucket, api_utils.iso_or_none(row.get("stored_at")))
    return metrics


def summary_stage_list(
    *,
    checkpoint: Dict[str, Any],
    summary: List[Dict[str, Any]],
    pipeline_steps: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    metrics = stage_metrics_from_summary(summary)
    return [
        {
            "id": f"stage_{index + 1:02d}",
            "key": step["key"],
            "name": step["label"],
            "status": _stage_status(step.get("state")),
            "tokens": (metrics.get(step["key"]) or {}).get("tokens", 0),
            "cost": (metrics.get(step["key"]) or {}).get("cost", 0.0),
            "attempts": (metrics.get(step["key"]) or {}).get("attempts", 0),
            "duration_seconds": _duration_seconds(
                (metrics.get(step["key"]) or {}).get("started_at"),
                (metrics.get(step["key"]) or {}).get("completed_at"),
            ),
            "started_at": (metrics.get(step["key"]) or {}).get("started_at"),
            "completed_at": (metrics.get(step["key"]) or {}).get("completed_at"),
            "error": checkpoint.get("error") if str(step.get("state") or "").upper() == "FAILED" else None,
            "prompt_metadata": None,
        }
        for index, step in enumerate(pipeline_steps)
    ]
