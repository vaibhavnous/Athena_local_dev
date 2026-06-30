import os
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
import uuid
from pathlib import Path
from typing import Any, Dict

from fastapi import APIRouter, File, HTTPException, UploadFile

from api import utils as api_utils
from api.demo import demo_action, demo_enabled, demo_status, new_demo_run_id
from api.models import PipelineRunRequest, StageContinueRequest
from utilis.logger import logger

router = APIRouter()
RUN_STATUS_EXECUTOR = ThreadPoolExecutor(max_workers=max(1, int(os.getenv("ATHENA_RUN_STATUS_WORKERS", "2"))))


def _fallback_status_payload(run_id: str, status: str = "RUNNING", checkpoint: Dict[str, Any] | None = None) -> Dict[str, Any]:
    checkpoint = checkpoint or {}
    result_state = str(checkpoint.get("status") or status or "RUNNING")
    return {
        "run_id": run_id,
        "status": result_state,
        "state": {
            "life_cycle_state": (
                "TERMINATED"
                if result_state in {"SUCCESS", "FAILED", "ABORTED", "PIPELINE_COMPLETED", "COMPLETED"}
                else "RUNNING"
            ),
            "result_state": result_state,
        },
        "run": {
            "id": run_id,
            "run_id": run_id,
            "status": result_state,
            "source": checkpoint.get("source") or "database",
            "brd_filename": checkpoint.get("brd_filename") or run_id,
            "provider": checkpoint.get("provider") or "azure_openai",
            "deployment": checkpoint.get("deployment"),
            "stages": [],
            "next_gate": checkpoint.get("next_gate"),
            "resume_message": checkpoint.get("resume_message"),
            "stage_confirmation": checkpoint.get("stage_confirmation"),
            "failed_stage_key": checkpoint.get("failed_background_stage") or checkpoint.get("last_failed_stage_key"),
            "failed_stage_label": checkpoint.get("failed_stage_label"),
            "error": checkpoint.get("error"),
            "updated_at": checkpoint.get("updated_at") or checkpoint.get("checkpoint_at"),
        },
    }


# -------------------------
# ✅ Health
# -------------------------
@router.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok", "service": "athena-fastapi"}


# -------------------------
# ✅ Run Pipeline
# -------------------------
@router.post("/pipeline/run")
def run_pipeline(payload: PipelineRunRequest) -> Dict[str, Any]:
    if demo_enabled():
        run_id = new_demo_run_id()
        logger.info("Pipeline run requested", extra={"run_id": run_id})
        return {"run_id": run_id, "status": "HITL_WAIT"}

    from api.services.pipeline_service import submit_pipeline_start
    from services.pipeline_runtime import load_checkpoint_state, save_checkpoint_state

    source = str(payload.source or "database").lower()
    sftp_entity = api_utils.normalize_file_entity(source, payload.sftp_entity)

    if not payload.brd_text.strip():
        raise HTTPException(status_code=400, detail="brd_text is required")

    run_id = str(uuid.uuid4())

    logger.info("Pipeline run requested", extra={"run_id": run_id, "source": source})

    try:
        existing = load_checkpoint_state(run_id) or {"run_id": run_id}

        save_checkpoint_state(
            run_id,
            {
                **existing,
                "run_id": run_id,
                "status": existing.get("status") or "RUNNING",
                "brd_text": existing.get("brd_text") or payload.brd_text,
                "brd_filename": existing.get("brd_filename") or payload.brd_filename,
                "source": existing.get("source") or source,
                "provider": existing.get("provider") or payload.provider,
                "deployment": existing.get("deployment") or payload.deployment,
                "source_databases": existing.get("source_databases")
                or payload.source_databases
                or ([payload.database_name] if payload.database_name else None),
                "sftp_entity": existing.get("sftp_entity") or sftp_entity,
                "use_domain_kb": (
                    existing.get("use_domain_kb")
                    if existing.get("use_domain_kb") is not None
                    else False
                ),
                "stage_confirmation_enabled": (
                    existing.get("stage_confirmation_enabled")
                    if existing.get("stage_confirmation_enabled") is not None
                    else bool(payload.stage_confirmation_enabled if payload.stage_confirmation_enabled is not None else True)
                ),
            },
        )

    except Exception:
        logger.error("Failed to initialize checkpoint", exc_info=True, extra={"run_id": run_id})
        raise HTTPException(status_code=503, detail="Failed to initialize run checkpoint")

    submit_pipeline_start(run_id, payload)

    return {"run_id": run_id, "status": "RUNNING"}


# -------------------------
# ✅ Upload BRD (SAFE)
# -------------------------
@router.post("/pipeline/upload-brd")
async def upload_brd(file: UploadFile = File(...)) -> Dict[str, Any]:

    upload_dir = api_utils.ROOT_DIR / "uploads"
    upload_dir.mkdir(parents=True, exist_ok=True)

    path = upload_dir / Path(file.filename or "uploaded_brd").name

    # ✅ safe file read (limit size)
    content = await file.read()
    if len(content) > 5 * 1024 * 1024:  # 5MB limit
        raise HTTPException(status_code=400, detail="File too large")

    path.write_bytes(content)

    logger.info("BRD uploaded", extra={"file": path.name})

    return {"filename": path.name, "path": str(path), "status": "uploaded"}


# -------------------------
# ✅ Pipeline Status
# -------------------------
@router.get("/pipeline/{run_id}/status")
def pipeline_status(run_id: str) -> Dict[str, Any]:
    if demo_enabled():
        return demo_status(run_id)

    from api.services.ui_service import ui_run
    from services.pipeline_runtime import load_checkpoint_state

    try:
        timeout_seconds = max(1, int(os.getenv("ATHENA_STATUS_ENDPOINT_TIMEOUT_SECONDS", "5")))
        future = RUN_STATUS_EXECUTOR.submit(ui_run, run_id)
        run = future.result(timeout=timeout_seconds)
    except FutureTimeoutError:
        logger.warning("Pipeline status hydration timed out; returning fallback status", extra={"run_id": run_id})
        try:
            checkpoint = load_checkpoint_state(run_id) or {}
        except Exception:
            checkpoint = {}
        return _fallback_status_payload(run_id, checkpoint=checkpoint)
    except Exception:
        logger.error("Failed to fetch pipeline status", exc_info=True, extra={"run_id": run_id})
        raise HTTPException(status_code=503, detail="Failed to fetch run status")

    result_state = run["status"]

    if result_state == "NOT_FOUND":
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    return {
        "run_id": run_id,
        "status": result_state,
        "state": {
            "life_cycle_state": (
                "TERMINATED"
                if result_state in {"SUCCESS", "FAILED", "ABORTED"}
                else "RUNNING"
            ),
            "result_state": result_state,
        },
        "run": run,
    }


# -------------------------
# ✅ Abort Pipeline
# -------------------------
@router.post("/pipeline/{run_id}/abort")
def abort_run(run_id: str) -> Dict[str, Any]:
    if demo_enabled():
        return demo_action(run_id, status="ABORTED")

    from services.pipeline_runtime import load_checkpoint_state, save_checkpoint_state

    checkpoint = load_checkpoint_state(run_id) or {"run_id": run_id}
    checkpoint["status"] = "ABORTED"
    save_checkpoint_state(run_id, checkpoint)

    logger.warning("Pipeline aborted", extra={"run_id": run_id})

    return {"run_id": run_id, "status": "ABORTED"}


# -------------------------
# ✅ Continue Stage
# -------------------------
@router.post("/pipeline/{run_id}/continue-stage")
def continue_stage(run_id: str, payload: StageContinueRequest) -> Dict[str, Any]:
    if demo_enabled():
        gate = int((demo_status(run_id).get("run") or {}).get("next_gate") or 1)
        segment_by_gate = {1: "kpi", 2: "table", 3: "enrichment", 4: "bronze", 5: "silver"}
        return demo_action(
            run_id,
            segment=segment_by_gate.get(gate, "kpi"),
            next_stage_key="nomination" if gate == 1 else None,
            resume_message="Flow continued.",
        )

    from api.services.pipeline_service import continue_database_pipeline_job
    from services.pipeline_runtime import (
        load_checkpoint_state,
        save_checkpoint_state,
        submit_background,
    )

    checkpoint = load_checkpoint_state(run_id) or {}
    next_stage_key = checkpoint.get("next_stage_key")

    if not next_stage_key:
        raise HTTPException(
            status_code=400,
            detail="No next stage is pending confirmation for this run.",
        )

    if str(checkpoint.get("source") or "database").lower() in {"sftp", "adls_gen2"}:
        raise HTTPException(
            status_code=400,
            detail="Stage-by-stage confirmation is not enabled for file-source runs yet.",
        )

    stage_key = str(next_stage_key)
    auto_advance = bool(payload.auto_advance)
    resumed_state = {
        **checkpoint,
        "run_id": run_id,
        "status": "PROCESSING",
        "background_stage": stage_key,
        "awaiting_stage_confirmation": False,
        "stage_confirmation_enabled": not auto_advance,
        "resume_message": f"{stage_key} is running.",
    }
    save_checkpoint_state(run_id, resumed_state)

    submit_background(
        run_id,
        stage_key,
        continue_database_pipeline_job,
        run_id,
        stage_key,
        resumed_state,
        auto_advance,
    )

    logger.info("Stage continuation submitted", extra={"run_id": run_id, "stage": stage_key})

    return {
        "run_id": run_id,
        "status": "SUBMITTED",
        "next_stage_key": stage_key,
        "resume_message": resumed_state["resume_message"],
    }


# -------------------------
# ✅ Retry Failed Stage
# -------------------------
@router.post("/pipeline/{run_id}/retry-failed-stage")
def retry_failed_stage(run_id: str) -> Dict[str, Any]:
    if demo_enabled():
        return demo_action(run_id, action="retry_failed_stage")

    from api.services.pipeline_service import (
        clean_checkpoint_for_resume,
        continue_database_pipeline_job,
        continue_file_pipeline_job,
        database_failed_stage_key,
    )
    from services.pipeline_runtime import (
        load_checkpoint_state,
        save_checkpoint_state,
        submit_background,
    )

    checkpoint = load_checkpoint_state(run_id) or {}

    if str(checkpoint.get("status") or "").upper() != "FAILED":
        raise HTTPException(status_code=400, detail="Only failed runs can retry a failed stage.")

    source = str(checkpoint.get("source") or "database").lower()

    if api_utils.is_file_source(source):
        resumed_state = clean_checkpoint_for_resume(checkpoint)
        save_checkpoint_state(run_id, resumed_state)

        submit_background(run_id, "file_resume", continue_file_pipeline_job, run_id, resumed_state)

        return {
            "run_id": run_id,
            "status": "SUBMITTED",
            "action": "retry_failed_stage",
        }

    failed_stage_key = database_failed_stage_key(run_id, checkpoint)

    if not failed_stage_key:
        raise HTTPException(status_code=400, detail="No failed stage identified.")

    resumed_state = clean_checkpoint_for_resume(checkpoint)
    save_checkpoint_state(run_id, resumed_state)

    submit_background(
        run_id,
        failed_stage_key,
        continue_database_pipeline_job,
        run_id,
        failed_stage_key,
        resumed_state,
    )

    logger.info("Retrying failed stage", extra={"run_id": run_id, "stage": failed_stage_key})

    return {
        "run_id": run_id,
        "status": "SUBMITTED",
        "action": "retry_failed_stage",
        "start_stage_key": failed_stage_key,
    }


@router.post("/pipeline/{run_id}/resume-from-failure")
def resume_from_failure(run_id: str) -> Dict[str, Any]:
    return demo_action(run_id, action="resume_from_failure")


@router.post("/pipeline/{run_id}/restart")
def restart_run(run_id: str) -> Dict[str, Any]:
    return demo_action(run_id, action="restart")
