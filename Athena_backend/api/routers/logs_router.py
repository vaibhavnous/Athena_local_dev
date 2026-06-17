from typing import Any, Dict

from fastapi import APIRouter

from api.services.log_service import read_logs

router = APIRouter()


@router.post("/logs/discover/{run_id}")
def discover_logs(run_id: str) -> Dict[str, Any]:
    return {"status": "completed", "runId": run_id}


@router.get("/logs/discover/{run_id}/status")
def discover_logs_status(run_id: str) -> Dict[str, Any]:
    return {"status": "completed", "runId": run_id}


@router.get("/logs/{run_id}")
def logs(run_id: str, limit: int = 300) -> Dict[str, Any]:
    return {"runId": run_id, "logs": read_logs(run_id, limit=limit)}


@router.get("/logs/{run_id}/since/{since_timestamp}")
def logs_since(run_id: str, since_timestamp: str, limit: int = 300) -> Dict[str, Any]:
    return {"runId": run_id, "logs": read_logs(run_id, limit=limit, since=since_timestamp)}
