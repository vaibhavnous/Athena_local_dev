import os
from concurrent.futures import TimeoutError as FutureTimeoutError
from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException

from api.services.ui_service import ui_run, ui_run_summary
from services.pipeline_runtime import BACKGROUND_EXECUTOR, list_runs
from utilis.logger import logger

router = APIRouter()


@router.get("/runs")
def runs() -> List[Dict[str, Any]]:
    try:
        timeout_seconds = max(1, int(os.getenv("ATHENA_RUNS_ENDPOINT_TIMEOUT_SECONDS", "5")))
        future = BACKGROUND_EXECUTOR.submit(list_runs)
        rows = future.result(timeout=timeout_seconds)
        return [ui_run_summary(row["run_id"]) for row in rows]
    except FutureTimeoutError:
        logger.warning("GET /runs timed out while listing runs; returning empty list")
        return []
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/runs/{run_id}")
def run_detail(run_id: str) -> Dict[str, Any]:
    try:
        return ui_run(run_id, include_scripts=True)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
