import os
from concurrent.futures import TimeoutError as FutureTimeoutError
from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException

from api.services.ui_service import ui_run, ui_run_summary
from services.pipeline_runtime import BACKGROUND_EXECUTOR, list_runs
from utilis.logger import logger

router = APIRouter()


# -------------------------
# ✅ Runs List
# -------------------------
@router.get("/runs")
def runs() -> List[Dict[str, Any]]:
    try:
        # ✅ configurable timeout with safe minimum
        timeout_seconds = max(1, int(os.getenv("ATHENA_RUNS_ENDPOINT_TIMEOUT_SECONDS", "5")))
        run_limit = max(1, min(100, int(os.getenv("ATHENA_RUNS_LIST_LIMIT", "25"))))

        logger.debug("Fetching runs list", extra={"timeout_seconds": timeout_seconds, "limit": run_limit})

        future = BACKGROUND_EXECUTOR.submit(list_runs, run_limit)
        rows = future.result(timeout=timeout_seconds)

        results: List[Dict[str, Any]] = []

        for row in rows:
            run_id = row.get("run_id")
            if not run_id:
                continue  # ✅ safety against malformed data

            try:
                results.append(ui_run_summary(run_id))
            except Exception:
                # ✅ prevent single failure from breaking endpoint
                logger.warning(
                    "Failed to build run summary",
                    extra={"run_id": run_id},
                )
                continue

        return results

    except FutureTimeoutError:
        logger.warning("GET /runs timed out while listing runs; returning empty list")
        return []

    except Exception:
        logger.error("Failed to fetch runs", exc_info=True)
        raise HTTPException(status_code=503, detail="Failed to fetch runs")


# -------------------------
# ✅ Run Detail
# -------------------------
@router.get("/runs/{run_id}")
def run_detail(run_id: str) -> Dict[str, Any]:
    try:
        return ui_run(run_id, include_scripts=True)

    except Exception:
        logger.error(
            "Failed to fetch run detail",
            exc_info=True,
            extra={"run_id": run_id},
        )
        raise HTTPException(status_code=503, detail="Failed to fetch run")
