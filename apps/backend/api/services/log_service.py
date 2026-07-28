from __future__ import annotations

import hashlib
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from api import utils as api_utils
from utilis.logger import PIPELINE_LOG_PATH, logger


def _parse_ts(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except Exception:
        return None


def tail_lines(path: Path, limit: int) -> List[str]:
    if limit <= 0 or not path.exists():
        return []

    with path.open("rb") as handle:
        handle.seek(0, os.SEEK_END)
        position = handle.tell()
        buffer = bytearray()
        newline_count = 0
        max_bytes = 2 * 1024 * 1024
        total_read = 0

        while position > 0 and newline_count <= limit and total_read < max_bytes:
            chunk_size = min(8192, position)
            position -= chunk_size
            handle.seek(position)
            chunk = handle.read(chunk_size)
            buffer[:0] = chunk
            total_read += chunk_size
            newline_count = buffer.count(b"\n")

    return buffer.decode("utf-8", errors="ignore").splitlines()[-limit:]


def _duration_from_message(message: Any) -> Optional[float]:
    match = re.search(r"duration_seconds=([0-9.]+)", str(message))
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def _event_type_from_message(message: Any) -> Optional[str]:
    normalized_message = str(message or "").strip().upper()
    if normalized_message.startswith("START"):
        return "stage_start"
    if normalized_message.startswith("END"):
        return "stage_end"
    return None


def _log_from_line(run_id: str, line: str, since_dt: Optional[datetime]) -> Optional[Dict[str, Any]]:
    if f'"run_id":"{run_id}"' not in line and f'"run_id": "{run_id}"' not in line:
        return None

    try:
        item = api_utils.json_loads(line)
    except Exception:
        return None

    if str(item.get("run_id") or "") != run_id:
        return None

    logged_at_raw = item.get("timestamp") or item.get("logged_at")
    logged_at_dt = _parse_ts(logged_at_raw)
    if since_dt and logged_at_dt and logged_at_dt <= since_dt:
        return None

    message = item.get("message", "")
    event_type = item.get("event_type") or _event_type_from_message(message)
    duration_seconds = item.get("duration_seconds")
    if duration_seconds is None:
        duration_seconds = _duration_from_message(message)

    stage = item.get("stage") or item.get("node") or item.get("module")
    step_name = item.get("step_name") or item.get("funcName")
    stable_log_id = hashlib.sha256(
        f"{run_id}|{logged_at_raw}|{stage}|{step_name}|{message}".encode("utf-8")
    ).hexdigest()

    return {
        "log_id": stable_log_id,
        "run_id": run_id,
        "notebook_name": item.get("node") or item.get("module"),
        "stage": stage,
        "step_name": step_name,
        "log_level": str(item.get("level") or "INFO").upper(),
        "message": message,
        "duration_seconds": duration_seconds,
        "event_type": event_type,
        "logged_at": logged_at_raw,
    }


def _collect_logs(run_id: str, raw_lines: List[str], since_dt: Optional[datetime]) -> List[Dict[str, Any]]:
    logs: List[Dict[str, Any]] = []
    for line in raw_lines:
        parsed = _log_from_line(run_id, line, since_dt)
        if parsed:
            logs.append(parsed)
    return logs


def read_logs(
    run_id: str,
    limit: int = 1000,
    since: Optional[str] = None,
) -> List[Dict[str, Any]]:
    log_path = PIPELINE_LOG_PATH
    if not log_path.exists():
        return []

    safe_limit = max(1, min(int(limit or 1000), 5000))
    since_dt = _parse_ts(since) if since else None

    raw_lines = tail_lines(log_path, min(max(safe_limit * 3, 1000), 5000))
    logs = _collect_logs(run_id, raw_lines, since_dt)

    # Older runs can fall outside the recent tail. On initial load, scan the
    # full local log file before telling the UI that no logs exist.
    if not logs and not since:
        try:
            all_lines = log_path.read_text(encoding="utf-8", errors="ignore").splitlines()
            logs = _collect_logs(run_id, all_lines, since_dt)
        except Exception:
            logger.exception("Failed to scan full pipeline log file for run_id=%s", run_id)
            return []

    return logs[-safe_limit:]
