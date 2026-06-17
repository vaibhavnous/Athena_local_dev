from __future__ import annotations

import hashlib
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from utilis.logger import PIPELINE_LOG_PATH, logger
from api import utils as api_utils


# -------------------------
# ✅ Helper: timestamp parse
# -------------------------
def _parse_ts(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except Exception:
        return None


# -------------------------
# ✅ Tail lines (optimized)
# -------------------------
def tail_lines(path: Path, limit: int) -> List[str]:
    if limit <= 0 or not path.exists():
        return []

    with path.open("rb") as handle:
        handle.seek(0, os.SEEK_END)
        position = handle.tell()
        buffer = bytearray()
        newline_count = 0

        # limit total read to avoid huge memory usage
        max_bytes = 2 * 1024 * 1024  # 2MB cap
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


# -------------------------
# ✅ Main log reader
# -------------------------
def read_logs(
    run_id: str,
    limit: int = 1000,
    since: Optional[str] = None,
) -> List[Dict[str, Any]]:
    log_path = PIPELINE_LOG_PATH

    if not log_path.exists():
        return []

    # ✅ reduce over-read size
    raw_lines = tail_lines(log_path, min(max(limit * 2, 500), 2000))

    logs: List[Dict[str, Any]] = []

    since_dt = _parse_ts(since) if since else None

    for line in raw_lines:

        # ✅ FAST FILTER before JSON parsing
        if f'"run_id":"{run_id}"' not in line and f'"run_id": "{run_id}"' not in line:
            continue

        try:
            item = api_utils.json_loads(line)
        except Exception:
            # optional debug log (kept quiet for performance)
            continue

        if str(item.get("run_id") or "") != run_id:
            continue

        logged_at_raw = item.get("timestamp") or item.get("logged_at")
        logged_at_dt = _parse_ts(logged_at_raw)

        # ✅ correct timestamp filtering
        if since_dt and logged_at_dt and logged_at_dt <= since_dt:
            continue

        message = item.get("message", "")
        event_type = item.get("event_type")

        # ✅ derive event type if missing
        if not event_type:
            normalized_message = str(message).strip().upper()
            if normalized_message.startswith("START"):
                event_type = "stage_start"
            elif normalized_message.startswith("END"):
                event_type = "stage_end"

        # ✅ optimized duration extraction
        duration_seconds = item.get("duration_seconds")
        if duration_seconds is None and "duration_seconds=" in str(message):
            match = re.search(r"duration_seconds=([0-9.]+)", str(message))
            if match:
                duration_seconds = float(match.group(1))

        stage = item.get("stage") or item.get("node") or item.get("module")
        step_name = item.get("step_name") or item.get("funcName")

        # ✅ stable but lighter hash
        stable_log_id = hashlib.sha256(
            f"{run_id}|{logged_at_raw}|{stage}|{step_name}|{message}".encode("utf-8")
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
                "logged_at": logged_at_raw,
            }
        )

        # ✅ EARLY EXIT (major performance win)
        if len(logs) >= limit:
            break

    return logs
