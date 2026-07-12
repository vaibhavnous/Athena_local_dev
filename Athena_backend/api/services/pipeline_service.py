from __future__ import annotations

import os
import time
from functools import lru_cache
from typing import Any, Dict, List, Optional

from fastapi import HTTPException

from services.pipeline_runtime import (
    BACKGROUND_EXECUTOR,
    BACKGROUND_JOBS,
    BACKGROUND_JOB_LOCK,
    continue_database_pipeline,
    ensure_background_capacity_locked,
    get_run_context,
    load_checkpoint_state,
    save_checkpoint_state,
    start_pipeline,
    submit_background,
)
from services.sftp_runtime import start_sftp_pipeline
from utilis.db import get_pending_items
from utilis.logger import logger

from api import utils as api_utils
from api.models import PipelineRunRequest

TERMINAL_STATUSES = {"ABORTED", "COMPLETED", "FAILED", "PIPELINE_COMPLETED", "SUCCESS"}
PAUSED_STATUSES = {"HITL_WAIT", "PAUSED_FOR_HITL", "PAUSED_FOR_STAGE_CONFIRMATION"}
ACTIVE_STATUSES = {"RUNNING", "PROCESSING", "SUBMITTED", "IN_PROGRESS"}


@lru_cache(maxsize=1)
def source_ingestion_graph():
    from source_ingestion_pipeline import build_source_ingestion_graph

    return build_source_ingestion_graph()


def _pipeline_timeout_seconds() -> int:
    return max(1, int(os.getenv("ATHENA_PIPELINE_JOB_TIMEOUT_SECONDS", "3600")))


def _validate_pipeline_result(result: Any) -> Dict[str, Any]:
    if not isinstance(result, dict):
        raise ValueError("Pipeline returned an invalid response object.")
    state = result.get("result")
    if not isinstance(state, dict):
        raise ValueError("Pipeline response is missing a valid result state.")
    return state


def _next_status(current_status: Optional[str], pending_gate1: bool, *, file_source: bool) -> str:
    status = str(current_status or "").upper()
    if status in TERMINAL_STATUSES or status in PAUSED_STATUSES or status in ACTIVE_STATUSES:
        return status
    if file_source:
        return current_status or "COMPLETED"
    return "HITL_WAIT" if pending_gate1 else (current_status or "COMPLETED")


def _mark_run_failed(run_id: str, exc: Exception, *, stage: str) -> None:
    logger.error("Pipeline job failed run_id=%s stage=%s", run_id, stage, exc_info=exc)
    checkpoint = load_checkpoint_state(run_id) or {"run_id": run_id}
    checkpoint.update(
        {
            "status": "FAILED",
            "error": str(exc),
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "failed_background_stage": stage,
            "failed_at": time.time(),
        }
    )
    save_checkpoint_state(run_id, checkpoint)


def _job_done_callback(run_id: str, job_key: str, stage: str):
    def _handle_done(done) -> None:
        try:
            exc = done.exception()
            if exc:
                _mark_run_failed(run_id, exc, stage=stage)
            else:
                logger.info("Pipeline background job completed run_id=%s stage=%s", run_id, stage)
        finally:
            with BACKGROUND_JOB_LOCK:
                if BACKGROUND_JOBS.get(job_key) is done:
                    BACKGROUND_JOBS.pop(job_key, None)

    return _handle_done


def run_pipeline_background(
    *,
    run_id: str,
    brd_text: str,
    brd_filename: Optional[str],
    source: Optional[str],
    source_databases: Optional[List[str]],
    sftp_entity: Optional[str],
    use_domain_kb: bool,
    stage_confirmation_enabled: bool,
    target_warehouse: str = "databricks",
) -> None:
    started_at = time.monotonic()
    try:
        logger.info("Pipeline background job started run_id=%s source=%s", run_id, source)
        existing_checkpoint = load_checkpoint_state(run_id) or {"run_id": run_id}
        if api_utils.is_file_source(source):
            result = start_sftp_pipeline(
                run_id=run_id,
                brd_text=brd_text,
                brd_filename=brd_filename,
                sftp_entity=sftp_entity,
                source=str(source or "sftp").lower(),
            )
        else:
            result = start_pipeline(
                brd_text=brd_text,
                brd_filename=brd_filename,
                source=source,
                source_databases=source_databases,
                sftp_entity=sftp_entity,
                run_id=run_id,
                use_domain_kb=use_domain_kb,
                stage_confirmation_enabled=stage_confirmation_enabled,
                target_warehouse=target_warehouse,
            )
        elapsed_seconds = time.monotonic() - started_at
        if elapsed_seconds > _pipeline_timeout_seconds():
            raise TimeoutError(f"Pipeline exceeded timeout after {elapsed_seconds:.1f} seconds.")

        state = _validate_pipeline_result(result)
        if brd_filename and not state.get("brd_filename"):
            state["brd_filename"] = brd_filename
        pending_gate1 = get_pending_items(run_id, 1)
        file_source = api_utils.is_file_source(state.get("source") or source)
        state["status"] = _next_status(state.get("status"), pending_gate1, file_source=file_source)
        save_checkpoint_state(run_id, {**existing_checkpoint, **state, "run_id": run_id})
        logger.info("Pipeline background job saved checkpoint run_id=%s status=%s", run_id, state.get("status"))
    except Exception as exc:
        _mark_run_failed(run_id, exc, stage="pipeline")
        raise


def submit_pipeline_start(run_id: str, payload: PipelineRunRequest) -> None:
    job_key = f"{run_id}:pipeline"
    source = str(payload.source or "database").lower()
    sftp_entity = api_utils.normalize_file_entity(source, payload.sftp_entity)
    use_domain_kb = False if api_utils.is_file_source(source) else bool(payload.use_domain_kb)
    with BACKGROUND_JOB_LOCK:
        if job_key in BACKGROUND_JOBS and not BACKGROUND_JOBS[job_key].done():
            logger.warning("Duplicate pipeline submission rejected run_id=%s", run_id)
            raise HTTPException(status_code=409, detail=f"Pipeline job already running for run_id={run_id}")

        ensure_background_capacity_locked()
        logger.info("Submitting pipeline background job run_id=%s source=%s", run_id, source)
        future = BACKGROUND_EXECUTOR.submit(
            run_pipeline_background,
            run_id=run_id,
            brd_text=payload.brd_text,
            brd_filename=payload.brd_filename,
            source=source,
            source_databases=payload.source_databases
            or ([payload.database_name] if payload.database_name else None),
            sftp_entity=sftp_entity,
            use_domain_kb=use_domain_kb,
            stage_confirmation_enabled=bool(
                payload.stage_confirmation_enabled if payload.stage_confirmation_enabled is not None else False
            ),
            target_warehouse=str(payload.target_warehouse or "databricks").lower(),
        )
        BACKGROUND_JOBS[job_key] = future

    future.add_done_callback(_job_done_callback(run_id, job_key, "pipeline"))


def normalized_source_databases(checkpoint: Dict[str, Any]) -> Optional[List[str]]:
    value = checkpoint.get("source_databases")
    if isinstance(value, list) and value:
        return value
    database_name = checkpoint.get("database_name")
    if database_name:
        return [database_name]
    return None


def seed_payload_from_checkpoint(checkpoint: Dict[str, Any]) -> PipelineRunRequest:
    source_databases = normalized_source_databases(checkpoint)
    database_name = source_databases[0] if source_databases else checkpoint.get("database_name")
    return PipelineRunRequest(
        brd_text=str(checkpoint.get("brd_text") or ""),
        brd_filename=checkpoint.get("brd_filename"),
        source=str(checkpoint.get("source") or "database"),
        provider=checkpoint.get("provider") or "azure_openai",
        deployment=checkpoint.get("deployment"),
        database_name=database_name,
        database_type=checkpoint.get("database_type"),
        target_warehouse=checkpoint.get("target_warehouse") or "databricks",
        source_databases=source_databases,
        sftp_entity=checkpoint.get("sftp_entity") or "transactions",
        use_domain_kb=bool(checkpoint.get("use_domain_kb")),
        stage_confirmation_enabled=checkpoint.get("stage_confirmation_enabled"),
    )


def clean_checkpoint_for_resume(checkpoint: Dict[str, Any]) -> Dict[str, Any]:
    cleaned = dict(checkpoint or {})
    cleaned["status"] = "RUNNING"
    cleaned["background_stage"] = None
    cleaned["failed_background_stage"] = None
    cleaned["last_failed_stage_key"] = checkpoint.get("failed_background_stage") or checkpoint.get("last_failed_stage_key")
    cleaned["error"] = None
    cleaned["resume_message"] = None
    cleaned["awaiting_stage_confirmation"] = False
    cleaned["retry_count"] = int(checkpoint.get("retry_count") or 0) + 1
    cleaned["resumed_at"] = time.time()
    return cleaned


def continue_database_pipeline_job(
    run_id: str,
    start_stage_key: str,
    state: Dict[str, Any],
    auto_advance: Optional[bool] = None,
) -> Dict[str, Any]:
    return continue_database_pipeline(
        run_id,
        start_stage_key=start_stage_key,
        state=state,
        auto_advance=auto_advance,
    )


def continue_file_pipeline_job(run_id: str, state: Dict[str, Any]) -> Dict[str, Any]:
    working_state = dict(state or {})
    working_state["run_id"] = run_id
    result = source_ingestion_graph().invoke(working_state)
    if not isinstance(result, dict):
        raise ValueError("File-source pipeline returned an invalid state.")
    return result


def database_failed_stage_key(run_id: str, checkpoint: Dict[str, Any]) -> Optional[str]:
    def _pipeline_stage(value: Any) -> Optional[str]:
        raw_stage = str(value or "").strip().lower()
        if raw_stage == "gold_code_execution":
            return "gold"
        if raw_stage == "silver_code_execution":
            if checkpoint.get("gold_generation_results") or checkpoint.get("gold_generation_completed"):
                return "gold"
            return "silver"
        stage = api_utils.stage_key(value)
        if stage == "gold_code_execution":
            return "gold"
        if stage == "silver_code_execution":
            # Gold is already generated only after Silver execution succeeds;
            # if an older checkpoint points here, retry the Gold stage safely.
            if checkpoint.get("gold_generation_results") or checkpoint.get("gold_generation_completed"):
                return "gold"
            return "silver"
        return stage

    if checkpoint.get("failed_background_stage"):
        return _pipeline_stage(checkpoint.get("failed_background_stage"))
    if checkpoint.get("background_stage"):
        return _pipeline_stage(checkpoint.get("background_stage"))

    next_stage_key = checkpoint.get("next_stage_key")
    if next_stage_key:
        return _pipeline_stage(next_stage_key)

    context = get_run_context(run_id)
    failed_step = next(
        (
            step.get("key")
            for step in (context.get("pipeline_steps") or [])
            if str(step.get("state") or "").upper() == "FAILED"
        ),
        None,
    )
    if failed_step:
        return _pipeline_stage(failed_step)
    return None
