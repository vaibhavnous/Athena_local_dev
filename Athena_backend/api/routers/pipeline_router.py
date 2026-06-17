import uuid
from pathlib import Path
from typing import Any, Dict

from fastapi import APIRouter, File, HTTPException, UploadFile

from api import utils as api_utils
from api.models import PipelineRunRequest, StageContinueRequest
from api.services.pipeline_service import (
    clean_checkpoint_for_resume,
    continue_database_pipeline_job,
    continue_file_pipeline_job,
    database_failed_stage_key,
    seed_payload_from_checkpoint,
    submit_pipeline_start,
)
from api.services.ui_service import ui_run
from services.pipeline_runtime import continue_database_pipeline, load_checkpoint_state, save_checkpoint_state, submit_background


router = APIRouter()


@router.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok", "service": "athena-fastapi"}


@router.post("/pipeline/run")
def run_pipeline(payload: PipelineRunRequest) -> Dict[str, Any]:
    source = str(payload.source or "database").lower()
    sftp_entity = api_utils.normalize_file_entity(source, payload.sftp_entity)
    if not api_utils.is_file_source(source) and not payload.brd_text.strip():
        raise HTTPException(status_code=400, detail="brd_text is required")
    run_id = str(uuid.uuid4())
    use_domain_kb = False if api_utils.is_file_source(source) else bool(payload.use_domain_kb)

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
                "use_domain_kb": existing.get("use_domain_kb") if existing.get("use_domain_kb") is not None else use_domain_kb,
                "stage_confirmation_enabled": (
                    existing.get("stage_confirmation_enabled")
                    if existing.get("stage_confirmation_enabled") is not None
                    else bool(payload.stage_confirmation_enabled if payload.stage_confirmation_enabled is not None else True)
                ),
            },
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Failed to initialize run checkpoint: {exc}") from exc

    submit_pipeline_start(run_id, payload)
    return {"run_id": run_id, "status": "RUNNING"}


@router.post("/pipeline/upload-brd")
async def upload_brd(file: UploadFile = File(...)) -> Dict[str, Any]:
    upload_dir = api_utils.ROOT_DIR / "uploads"
    upload_dir.mkdir(parents=True, exist_ok=True)
    path = upload_dir / Path(file.filename or "uploaded_brd").name
    path.write_bytes(await file.read())
    return {"filename": path.name, "path": str(path), "status": "uploaded"}


@router.get("/pipeline/{run_id}/status")
def pipeline_status(run_id: str) -> Dict[str, Any]:
    run = ui_run(run_id)
    result_state = run["status"]
    if result_state == "NOT_FOUND":
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
    return {
        "run_id": run_id,
        "status": result_state,
        "state": {
            "life_cycle_state": "TERMINATED" if result_state in {"SUCCESS", "FAILED", "ABORTED"} else "RUNNING",
            "result_state": result_state,
        },
        "run": run,
    }


@router.post("/pipeline/{run_id}/abort")
def abort_run(run_id: str) -> Dict[str, Any]:
    checkpoint = load_checkpoint_state(run_id) or {"run_id": run_id}
    checkpoint["status"] = "ABORTED"
    save_checkpoint_state(run_id, checkpoint)
    return {"run_id": run_id, "status": "ABORTED"}


@router.post("/pipeline/{run_id}/continue-stage")
def continue_stage(run_id: str, payload: StageContinueRequest) -> Dict[str, Any]:
    checkpoint = load_checkpoint_state(run_id) or {}
    next_stage_key = checkpoint.get("next_stage_key")
    if not next_stage_key:
        raise HTTPException(status_code=400, detail="No next stage is pending confirmation for this run.")
    if str(checkpoint.get("source") or "database").lower() in {"sftp", "adls_gen2"}:
        raise HTTPException(status_code=400, detail="Stage-by-stage confirmation is not enabled for file-source runs yet.")

    result = continue_database_pipeline(
        run_id,
        start_stage_key=str(next_stage_key),
        state=checkpoint,
        auto_advance=bool(payload.auto_advance),
    )
    return {
        "run_id": run_id,
        "status": result.get("status") or "RUNNING",
        "next_stage_key": result.get("next_stage_key"),
        "resume_message": result.get("resume_message"),
    }


@router.post("/pipeline/{run_id}/retry-failed-stage")
def retry_failed_stage(run_id: str) -> Dict[str, Any]:
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
            "start_stage_key": checkpoint.get("failed_background_stage") or "file_resume",
        }

    failed_stage_key = database_failed_stage_key(run_id, checkpoint)
    if not failed_stage_key:
        raise HTTPException(status_code=400, detail="No failed stage could be identified for this run.")

    resumed_state = clean_checkpoint_for_resume(checkpoint)
    save_checkpoint_state(run_id, resumed_state)
    submit_background(run_id, failed_stage_key, continue_database_pipeline_job, run_id, failed_stage_key, resumed_state)
    return {
        "run_id": run_id,
        "status": "SUBMITTED",
        "action": "retry_failed_stage",
        "start_stage_key": failed_stage_key,
    }


@router.post("/pipeline/{run_id}/resume-from-failure")
def resume_from_failure(run_id: str) -> Dict[str, Any]:
    checkpoint = load_checkpoint_state(run_id) or {}
    if str(checkpoint.get("status") or "").upper() != "FAILED":
        raise HTTPException(status_code=400, detail="Only failed runs can resume from failure.")

    source = str(checkpoint.get("source") or "database").lower()
    if api_utils.is_file_source(source):
        resumed_state = clean_checkpoint_for_resume(checkpoint)
        save_checkpoint_state(run_id, resumed_state)
        submit_background(run_id, "file_resume", continue_file_pipeline_job, run_id, resumed_state)
        return {
            "run_id": run_id,
            "status": "SUBMITTED",
            "action": "resume_from_failure",
            "start_stage_key": checkpoint.get("failed_background_stage") or "file_resume",
        }

    start_stage_key = database_failed_stage_key(run_id, checkpoint) or checkpoint.get("next_stage_key") or "ingestion"
    resumed_state = clean_checkpoint_for_resume(checkpoint)
    save_checkpoint_state(run_id, resumed_state)
    submit_background(run_id, start_stage_key, continue_database_pipeline_job, run_id, start_stage_key, resumed_state)
    return {
        "run_id": run_id,
        "status": "SUBMITTED",
        "action": "resume_from_failure",
        "start_stage_key": start_stage_key,
    }


@router.post("/pipeline/{run_id}/restart")
def restart_pipeline(run_id: str) -> Dict[str, Any]:
    checkpoint = load_checkpoint_state(run_id) or {}
    if not checkpoint:
        raise HTTPException(status_code=404, detail=f"No checkpoint found for run: {run_id}")

    new_run_id = str(uuid.uuid4())
    payload = seed_payload_from_checkpoint(checkpoint)
    source = str(payload.source or "database").lower()
    sftp_entity = api_utils.normalize_file_entity(source, payload.sftp_entity)

    try:
        existing = load_checkpoint_state(new_run_id) or {"run_id": new_run_id}
        save_checkpoint_state(
            new_run_id,
            {
                **existing,
                "run_id": new_run_id,
                "status": "RUNNING",
                "brd_text": payload.brd_text,
                "brd_filename": payload.brd_filename,
                "source": source,
                "provider": payload.provider,
                "deployment": payload.deployment,
                "database_name": payload.database_name,
                "database_type": payload.database_type,
                "source_databases": payload.source_databases,
                "sftp_entity": sftp_entity,
                "stage_confirmation_enabled": bool(
                    payload.stage_confirmation_enabled if payload.stage_confirmation_enabled is not None else True
                ),
                "restarted_from_run_id": run_id,
            },
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Failed to initialize restarted run checkpoint: {exc}") from exc

    submit_pipeline_start(new_run_id, payload)
    return {
        "run_id": new_run_id,
        "status": "RUNNING",
        "action": "restart",
        "restarted_from_run_id": run_id,
    }
