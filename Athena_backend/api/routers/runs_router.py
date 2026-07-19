import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException

from api.demo import (
    demo_enabled,
    demo_lineage,
    demo_run,
    demo_runs,
    demo_scripts,
)
from utilis.logger import logger

router = APIRouter()
RUN_LIST_EXECUTOR = ThreadPoolExecutor(max_workers=max(1, int(os.getenv("ATHENA_RUN_LIST_WORKERS", "2"))))
RUN_SUMMARY_EXECUTOR = ThreadPoolExecutor(max_workers=max(1, int(os.getenv("ATHENA_RUN_SUMMARY_WORKERS", "2"))))
RUN_DETAIL_EXECUTOR = ThreadPoolExecutor(max_workers=max(1, int(os.getenv("ATHENA_RUN_DETAIL_WORKERS", "2"))))


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
        "compliance_enabled": bool(row.get("compliance_enabled")),
        "compliance_assessment_id": row.get("compliance_assessment_id"),
        "compliance_assessment_status": row.get("compliance_assessment_status"),
        "compliance_review_status": row.get("compliance_review_status"),
    }


def _status_from_checkpoint(checkpoint: Dict[str, Any]) -> str:
    status = str(checkpoint.get("status") or "UNKNOWN").upper()
    if checkpoint.get("background_stage") or status in {"RUNNING", "PROCESSING", "PENDING", "SUBMITTED", "IN_PROGRESS"}:
        return "RUNNING"
    if checkpoint.get("next_gate") or checkpoint.get("next_review_key") or status in {"HITL_WAIT", "PAUSED_FOR_HITL"}:
        return "HITL_WAIT"
    if status == "PAUSED_FOR_STAGE_CONFIRMATION":
        return "PAUSED_FOR_STAGE_CONFIRMATION"
    if status in {"PIPELINE_COMPLETED", "COMPLETED", "SUCCESS"}:
        return "SUCCESS"
    if status == "FAILED":
        return "FAILED"
    if status == "ABORTED":
        return "ABORTED"
    return status


def _checkpoint_run_summary(row: Dict[str, Any]) -> Dict[str, Any]:
    from services.pipeline_runtime import load_checkpoint_state

    run_id = str(row.get("run_id") or row.get("id") or "")
    checkpoint = row.get("checkpoint")
    if isinstance(checkpoint, str):
        checkpoint = json.loads(checkpoint)
    if not isinstance(checkpoint, dict):
        checkpoint = load_checkpoint_state(run_id) or {}
    return {
        **_fallback_run_summary(row),
        "brd_filename": checkpoint.get("brd_filename") or checkpoint.get("display_name") or row.get("brd_filename") or run_id,
        "source": checkpoint.get("source") or row.get("source") or "database",
        "status": _status_from_checkpoint(checkpoint),
        "provider": checkpoint.get("provider") or row.get("provider") or "azure_openai",
        "deployment": checkpoint.get("deployment") or row.get("deployment"),
        "started_at": checkpoint.get("started_at") or row.get("started_at") or row.get("last_activity"),
        "completed_at": checkpoint.get("completed_at"),
        "next_gate": checkpoint.get("next_gate"),
        "next_review_key": checkpoint.get("next_review_key"),
        "resume_message": checkpoint.get("resume_message"),
        "stage_confirmation": checkpoint.get("stage_confirmation"),
        "failed_stage_key": checkpoint.get("failed_background_stage") or checkpoint.get("last_failed_stage_key"),
        "failed_stage_label": checkpoint.get("failed_stage_label"),
        "error": checkpoint.get("error") or row.get("error"),
        "updated_at": checkpoint.get("updated_at") or checkpoint.get("checkpoint_at") or row.get("last_activity"),
        "sftp_entity": checkpoint.get("sftp_entity") or row.get("sftp_entity"),
        "source_row_count": checkpoint.get("source_row_count") or row.get("source_row_count"),
        "source_columns": checkpoint.get("source_columns") or row.get("source_columns") or [],
        "compliance_enabled": bool(checkpoint.get("compliance_enabled")),
        "compliance_assessment_id": checkpoint.get("compliance_assessment_id"),
        "compliance_assessment_status": checkpoint.get("compliance_assessment_status"),
        "compliance_review_status": checkpoint.get("compliance_review_status"),
    }


def _fallback_run_detail(run_id: str, checkpoint: Dict[str, Any] | None = None) -> Dict[str, Any]:
    checkpoint = checkpoint or {}
    from services.pipeline_runtime import build_pipeline_steps

    bronze_completed = bool(
        checkpoint.get("bronze_generation_status") == "COMPLETED"
        or checkpoint.get("bronze_generation_results")
        or checkpoint.get("snowflake_bronze_execution_status") == "COMPLETED"
        or checkpoint.get("databricks_bronze_execution_status") == "COMPLETED"
    )
    silver_completed = bool(
        checkpoint.get("silver_generation_status") == "COMPLETED"
        or checkpoint.get("silver_generation_results")
        or checkpoint.get("snowflake_silver_execution_status") == "COMPLETED"
        or checkpoint.get("databricks_silver_execution_status") == "COMPLETED"
    )
    gold_completed = bool(
        str(checkpoint.get("gold_generation_status") or "").startswith("COMPLETED")
        or checkpoint.get("gold_generation_results")
        or checkpoint.get("background_stage") == "gold_code_execution"
        or str(checkpoint.get("snowflake_gold_execution_status") or "").upper() in {"RUNNING", "COMPLETED"}
        or str(checkpoint.get("databricks_gold_execution_status") or "").upper() in {"RUNNING", "COMPLETED"}
    )
    next_gate = None if gold_completed else checkpoint.get("next_gate")
    next_review_key = None if gold_completed else checkpoint.get("next_review_key")
    pipeline_steps = build_pipeline_steps(
        source=str(checkpoint.get("source") or "database"),
        checkpoint=checkpoint,
        summary=[],
        pending_gate1=[],
        completed_gate1=[],
        nominated_tables=checkpoint.get("nominated_tables") or [],
        certified_tables=checkpoint.get("certified_tables") or [],
        enriched_payload=checkpoint.get("enriched_metadata") or {},
        gate3_payload=checkpoint.get("enrichment_review_artifact") or {},
        bronze_generation_completed=bronze_completed,
        silver_generation_completed=silver_completed,
        gold_generation_completed=gold_completed,
    )
    waiting_gate_key = f"gate{next_gate}" if next_gate in {1, 2, 3, 4, 5} else None
    waiting_stage_key = str(next_review_key or waiting_gate_key or "") or None
    if waiting_stage_key:
        from services.pipeline_runtime import apply_waiting_stage_state

        pipeline_steps = apply_waiting_stage_state(pipeline_steps, waiting_stage_key)
    fallback_status = checkpoint.get("status")
    if checkpoint.get("background_stage"):
        fallback_status = "RUNNING"
    elif gold_completed and (
        str(checkpoint.get("snowflake_gold_execution_status") or "").upper() == "COMPLETED"
        or str(checkpoint.get("databricks_gold_execution_status") or "").upper() == "COMPLETED"
    ):
        fallback_status = "SUCCESS"
    elif gold_completed and str(fallback_status or "").upper() == "HITL_WAIT":
        fallback_status = "RUNNING"
    current_step = next(
        (
            step for step in pipeline_steps
            if str(step.get("state") or "").upper() in {"RUNNING", "HITL_WAIT", "FAILED"}
        ),
        None,
    )
    external_execution = checkpoint.get("external_execution") if isinstance(checkpoint.get("external_execution"), dict) else None
    if current_step and external_execution and external_execution.get("message"):
        current_step = {**current_step, "detail": external_execution.get("message")}

    return {
        **_fallback_run_summary(
            {
                "run_id": run_id,
                "brd_filename": checkpoint.get("brd_filename"),
                "source": checkpoint.get("source"),
                "status": fallback_status,
                "provider": checkpoint.get("provider"),
                "deployment": checkpoint.get("deployment"),
                "error": checkpoint.get("error"),
                "updated_at": checkpoint.get("updated_at") or checkpoint.get("checkpoint_at"),
                "sftp_entity": checkpoint.get("sftp_entity"),
                "source_row_count": checkpoint.get("source_row_count"),
                "source_columns": checkpoint.get("source_columns"),
            }
        ),
        "checkpoint": checkpoint,
        "stage_confirmation": checkpoint.get("stage_confirmation"),
        "next_gate": next_gate,
        "next_review_key": next_review_key,
        "resume_message": checkpoint.get("resume_message"),
        "pipeline_steps": pipeline_steps,
        "current_pipeline_step": current_step,
        "external_execution": external_execution,
        "bronze_generation_completed": bronze_completed,
        "silver_generation_completed": silver_completed,
        "gold_generation_completed": gold_completed,
        "candidate_feed": checkpoint.get("candidate_feed"),
        "candidate_feeds": checkpoint.get("candidate_feeds") or [],
        "compliance_enabled": bool(checkpoint.get("compliance_enabled")),
        "compliance_assessment_id": checkpoint.get("compliance_assessment_id"),
        "compliance_assessment_status": checkpoint.get("compliance_assessment_status"),
        "compliance_assessment_error": checkpoint.get("compliance_assessment_error"),
        "compliance_review_status": checkpoint.get("compliance_review_status"),
        "compliance_review": checkpoint.get("compliance_review") or {},
        "compliance_review_error": checkpoint.get("compliance_review_error"),
        "compliance_results": checkpoint.get("compliance_results") or {},
        "bronze": {"generated_at": None, "scripts": []},
        "silver": {"generated_at": None, "scripts": []},
        "gold": {"generated_at": None, "scripts": []},
    }


# -------------------------
# ✅ Runs List
# -------------------------
@router.get("/runs")
def runs() -> List[Dict[str, Any]]:
    if demo_enabled():
        return demo_runs()

    from services.pipeline_runtime import list_runs

    try:
        # ✅ configurable timeout with safe minimum
        timeout_seconds = max(1, int(os.getenv("ATHENA_RUNS_ENDPOINT_TIMEOUT_SECONDS", "5")))
        run_limit = max(1, min(100, int(os.getenv("ATHENA_RUNS_LIST_LIMIT", "10"))))
        fast_summary = str(os.getenv("ATHENA_RUNS_FAST_SUMMARY", "true")).lower() not in {"0", "false", "no"}
        deadline = time.monotonic() + timeout_seconds

        logger.debug("Fetching runs list", extra={"timeout_seconds": timeout_seconds, "limit": run_limit})

        future = RUN_LIST_EXECUTOR.submit(list_runs, run_limit)
        rows = future.result(timeout=timeout_seconds)

        results: List[Dict[str, Any]] = []

        if fast_summary:
            for row in rows:
                run_id = row.get("run_id")
                if not run_id:
                    continue
                try:
                    results.append(_checkpoint_run_summary(row))
                except Exception:
                    logger.warning("Failed to build checkpoint run summary; returning fallback summary", extra={"run_id": run_id})
                    results.append(_fallback_run_summary(row))
            return results

        for row in rows:
            run_id = row.get("run_id")
            if not run_id:
                continue  # ✅ safety against malformed data

            if time.monotonic() >= deadline:
                logger.warning("GET /runs summary budget exhausted; returning fallback summary", extra={"run_id": run_id})
                results.append(_fallback_run_summary(row))
                continue

            try:
                from api.services.ui_service import ui_run_summary

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
        logger.warning("GET /runs timed out while listing runs")
        try:
            future.cancel()
        except Exception:
            pass
        raise HTTPException(status_code=503, detail="Run list temporarily unavailable")

    except Exception:
        logger.error("Failed to fetch runs", exc_info=True)
        raise HTTPException(status_code=503, detail="Failed to fetch runs")


# -------------------------
# ✅ Run Detail
# -------------------------
@router.get("/runs/{run_id}")
def run_detail(run_id: str) -> Dict[str, Any]:
    if demo_enabled():
        return demo_run(run_id, include_scripts=True)

    from api.services.ui_service import ui_run
    from services.pipeline_runtime import load_checkpoint_state

    try:
        timeout_seconds = max(1, int(os.getenv("ATHENA_RUN_DETAIL_TIMEOUT_SECONDS", "8")))
        future = RUN_DETAIL_EXECUTOR.submit(ui_run, run_id, include_scripts=True)
        return future.result(timeout=timeout_seconds)
    except FutureTimeoutError:
        logger.warning("GET /runs/{run_id} detail timed out; returning fallback detail", extra={"run_id": run_id})
        try:
            checkpoint = load_checkpoint_state(run_id) or {}
        except Exception:
            checkpoint = {}
        return _fallback_run_detail(run_id, checkpoint)
    except Exception:
        logger.error(
            "Failed to fetch run detail",
            exc_info=True,
            extra={"run_id": run_id},
        )
        try:
            checkpoint = load_checkpoint_state(run_id) or {}
        except Exception:
            checkpoint = {}
        return _fallback_run_detail(run_id, checkpoint)


@router.get("/run-scripts/{run_id}")
def run_scripts(run_id: str) -> Dict[str, Any]:
    if demo_enabled():
        return {"run_id": run_id, **demo_scripts(run_id)}

    from services.pipeline_runtime import (
        load_bronze_scripts,
        load_checkpoint_state,
        load_gold_scripts,
        load_silver_scripts,
    )

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


@router.get("/run-lineage/{run_id}")
def run_lineage(run_id: str) -> Dict[str, Any]:
    if demo_enabled():
        return demo_lineage(run_id)

    from services.pipeline_runtime import build_run_lineage, load_checkpoint_state

    try:
        checkpoint = load_checkpoint_state(run_id) or {"run_id": run_id}
        return build_run_lineage(run_id, checkpoint)
    except Exception:
        logger.error(
            "Failed to build run lineage",
            exc_info=True,
            extra={"run_id": run_id},
        )
        raise HTTPException(status_code=503, detail="Failed to fetch run lineage")
