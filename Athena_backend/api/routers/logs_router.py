from typing import Any, Dict

from fastapi import APIRouter, HTTPException
from datetime import datetime

from api.services.log_service import read_logs
from utilis.logger import logger

router = APIRouter()


# -------------------------
# ✅ Discover Logs
# -------------------------
@router.post("/logs/discover/{run_id}")
def discover_logs(run_id: str) -> Dict[str, Any]:
    # No change — frontend safe
    return {"status": "completed", "runId": run_id}


# -------------------------
# ✅ Discover Logs Status
# -------------------------
@router.get("/logs/discover/{run_id}/status")
def discover_logs_status(run_id: str) -> Dict[str, Any]:
    # No change — frontend safe
    return {"status": "completed", "runId": run_id}


# -------------------------
# ✅ Get Logs
# -------------------------
@router.get("/logs/{run_id}")
def logs(run_id: str, limit: int = 300) -> Dict[str, Any]:

    # ✅ MUST FIX: Clamp limit to prevent abuse
    limit = min(max(limit, 1), 1000)

    logger.debug("Fetching logs", extra={"run_id": run_id, "limit": limit})

    try:
        logs = read_logs(run_id, limit=limit)
        return {"runId": run_id, "logs": logs}

    except Exception:
        logger.error("Failed to fetch logs", exc_info=True, extra={"run_id": run_id})
        return {"runId": run_id, "logs": []}


# -------------------------
# ✅ Get Logs Since Timestamp
# -------------------------
@router.get("/logs/{run_id}/since/{since_timestamp}")
def logs_since(run_id: str, since_timestamp: str, limit: int = 300) -> Dict[str, Any]:

    # ✅ MUST FIX: Clamp limit
    limit = min(max(limit, 1), 1000)

    # ✅ MUST FIX: Validate timestamp format
    try:
        datetime.fromisoformat(str(since_timestamp).replace("Z", "+00:00"))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid timestamp format")

    logger.debug(
        "Fetching logs since timestamp",
        extra={"run_id": run_id, "since": since_timestamp, "limit": limit},
    )

    try:
        logs = read_logs(run_id, limit=limit, since=since_timestamp)
        return {"runId": run_id, "logs": logs}

    except Exception:
        logger.error(
            "Failed to fetch logs since timestamp",
            exc_info=True,
            extra={"run_id": run_id, "since": since_timestamp},
        )
        return {"runId": run_id, "logs": []}