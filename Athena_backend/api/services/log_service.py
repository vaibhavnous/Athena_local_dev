from __future__ import annotations

import hashlib
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from utilis.logger import PIPELINE_LOG_PATH

from api import utils as api_utils


def tail_lines(path: Path, limit: int) -> List[str]:
    if limit <= 0 or not path.exists():
        return []

    with path.open("rb") as handle:
        handle.seek(0, os.SEEK_END)
        position = handle.tell()
        buffer = bytearray()
        newline_count = 0

        while position > 0 and newline_count <= limit:
            chunk_size = min(8192, position)
            position -= chunk_size
            handle.seek(position)
            chunk = handle.read(chunk_size)
            buffer[:0] = chunk
            newline_count = buffer.count(b"\n")

    return buffer.decode("utf-8", errors="ignore").splitlines()[-limit:]


def read_logs(run_id: str, limit: int = 1000, since: Optional[str] = None) -> List[Dict[str, Any]]:
    log_path = PIPELINE_LOG_PATH
    if not log_path.exists():
        return []

    raw_lines = tail_lines(log_path, max(limit * 5, 2000))

    logs: List[Dict[str, Any]] = []
    for line in raw_lines:
        try:
            item = api_utils.json_loads(line)
        except Exception:
            continue
        if str(item.get("run_id") or "") != run_id:
            continue
        logged_at = item.get("timestamp") or item.get("logged_at")
        if since and logged_at and logged_at <= since:
            continue
        message = item.get("message", "")
        event_type = item.get("event_type")
        if not event_type:
            normalized_message = str(message).strip().upper()
            if normalized_message.startswith("START"):
                event_type = "stage_start"
            elif normalized_message.startswith("END"):
                event_type = "stage_end"
        duration_seconds = item.get("duration_seconds")
        if duration_seconds is None:
            duration_match = re.search(r"duration_seconds=([0-9.]+)", str(message))
            if duration_match:
                duration_seconds = float(duration_match.group(1))

        stage = item.get("stage") or item.get("node") or item.get("module")
        step_name = item.get("step_name") or item.get("funcName")
        stable_log_id = hashlib.sha256(
            "|".join(
                [
                    str(run_id),
                    str(logged_at or ""),
                    str(item.get("level", "INFO")),
                    str(stage or ""),
                    str(step_name or ""),
                    str(message or ""),
                    str(event_type or ""),
                ]
            ).encode("utf-8")
        ).hexdigest()
        logs.append(
            {
                "log_id": stable_log_id,
                "run_id": run_id,
                "notebook_name": item.get("node") or item.get("module"),
                "stage": stage,
                "step_name": step_name,
                "log_level": item.get("level", "INFO"),
                "message": message,
                "duration_seconds": duration_seconds,
                "event_type": event_type,
                "logged_at": logged_at,
            }
        )
    return logs[-limit:]
