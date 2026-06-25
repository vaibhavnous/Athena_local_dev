import os
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException

from api.services.ui_service import ui_run, ui_run_summary
from services.pipeline_runtime import (
    BACKGROUND_EXECUTOR,
    list_runs,
    load_bronze_scripts,
    load_checkpoint_state,
    load_gold_scripts,
    load_silver_scripts,
)
from utilis.logger import logger

router = APIRouter()
RUN_SUMMARY_EXECUTOR = ThreadPoolExecutor(max_workers=max(1, int(os.getenv("ATHENA_RUN_SUMMARY_WORKERS", "2"))))


def _fallback_run_summary(row: Dict[str, Any]) -> Dict[str, Any]:
    run_id = str(row.get("run_id") or row.get("id") or "")
    return {
        "id": run_id,
        "run_id": run_id,
        "brd_filename": row.get("brd_filename") or run_id,
        "source": row.get("source") or "database",
        "status": row.get("status") or "UNKNOWN",
        "provider": row.get("provider") or "azure_openai",
        "deployment": row.get("deployment"),
        "started_at": row.get("started_at") or row.get("last_activity"),
        "completed_at": row.get("completed_at"),
        "cache_hit": "NONE",
        "cache_score": 0,
        "extraction_path": "ATHENA_GRAPH",
        "total_tokens": 0,
        "total_cost": 0,
        "stages": [],
        "next_gate": None,
        "resume_message": None,
        "stage_confirmation": None,
        "failed_stage_key": None,
        "failed_stage_label": None,
        "error": row.get("error"),
        "updated_at": row.get("updated_at") or row.get("last_activity"),
        "script_counts": {"bronze": 0, "silver": 0, "gold": 0},
        "sftp_entity": row.get("sftp_entity"),
        "source_row_count": row.get("source_row_count"),
        "source_columns": row.get("source_columns") or [],
    }


# -------------------------
# ✅ Runs List
# -------------------------
@router.get("/runs")
def runs() -> List[Dict[str, Any]]:
    try:
        # ✅ configurable timeout with safe minimum
        timeout_seconds = max(1, int(os.getenv("ATHENA_RUNS_ENDPOINT_TIMEOUT_SECONDS", "5")))
        run_limit = max(1, min(100, int(os.getenv("ATHENA_RUNS_LIST_LIMIT", "25"))))
        deadline = time.monotonic() + timeout_seconds

        logger.debug("Fetching runs list", extra={"timeout_seconds": timeout_seconds, "limit": run_limit})

        future = BACKGROUND_EXECUTOR.submit(list_runs, run_limit)
        rows = future.result(timeout=timeout_seconds)

        results: List[Dict[str, Any]] = []

        for row in rows:
            run_id = row.get("run_id")
            if not run_id:
                continue  # ✅ safety against malformed data

            if time.monotonic() >= deadline:
                logger.warning("GET /runs summary budget exhausted; returning fallback summary", extra={"run_id": run_id})
                results.append(_fallback_run_summary(row))
                continue

            try:
                remaining = max(0.1, deadline - time.monotonic())
                summary_future = RUN_SUMMARY_EXECUTOR.submit(ui_run_summary, run_id)
                results.append(summary_future.result(timeout=remaining))
            except FutureTimeoutError:
                logger.warning("GET /runs summary timed out; returning fallback summary", extra={"run_id": run_id})
                results.append(_fallback_run_summary(row))
            except Exception:
                # ✅ prevent single failure from breaking endpoint
                logger.warning(
                    "Failed to build run summary",
                    extra={"run_id": run_id},
                )
                results.append(_fallback_run_summary(row))

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


@router.get("/run-scripts/{run_id}")
def run_scripts(run_id: str) -> Dict[str, Any]:
    try:
        checkpoint = load_checkpoint_state(run_id) or {"run_id": run_id}
        return {
            "run_id": run_id,
            "bronze": load_bronze_scripts(run_id, checkpoint),
            "silver": load_silver_scripts(run_id, checkpoint),
            "gold": load_gold_scripts(run_id, checkpoint),
        }
    except Exception:
        logger.error(
            "Failed to fetch run scripts",
            exc_info=True,
            extra={"run_id": run_id},
        )
        raise HTTPException(status_code=503, detail="Failed to fetch run scripts")
