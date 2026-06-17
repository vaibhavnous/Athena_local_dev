from __future__ import annotations

from typing import Any, Dict, List, Optional

from services.pipeline_runtime import (
    BACKGROUND_EXECUTOR,
    BACKGROUND_JOBS,
    BACKGROUND_JOB_LOCK,
    continue_database_pipeline,
    get_run_context,
    load_checkpoint_state,
    save_checkpoint_state,
    start_pipeline,
    submit_background,
)
from services.sftp_runtime import start_sftp_pipeline

from api import utils as api_utils
from api.models import PipelineRunRequest


def run_pipeline_background(
    *,
    run_id: str,
    brd_text: str,
    source: Optional[str],
    source_databases: Optional[List[str]],
    sftp_entity: Optional[str],
    use_domain_kb: bool,
    stage_confirmation_enabled: bool,
) -> None:
    try:
        existing_checkpoint = load_checkpoint_state(run_id) or {"run_id": run_id}
        if api_utils.is_file_source(source):
            result = start_sftp_pipeline(
                run_id=run_id,
                brd_text=brd_text,
                sftp_entity=sftp_entity,
                source=str(source or "sftp").lower(),
            )
        else:
            result = start_pipeline(
                brd_text=brd_text,
                source=source,
                source_databases=source_databases,
                sftp_entity=sftp_entity,
                run_id=run_id,
                use_domain_kb=use_domain_kb,
                stage_confirmation_enabled=stage_confirmation_enabled,
            )
        state = result.get("result") if isinstance(result, dict) else {}
        if isinstance(state, dict):
            from utilis.db import get_pending_items

            pending_gate1 = get_pending_items(run_id, 1)
            if api_utils.is_file_source(state.get("source") or source):
                state["status"] = state.get("status", "COMPLETED")
            elif state.get("status") not in {"PAUSED_FOR_STAGE_CONFIRMATION", "FAILED"}:
                state["status"] = "HITL_WAIT" if pending_gate1 else state.get("status", "COMPLETED")
            save_checkpoint_state(run_id, {**existing_checkpoint, **state, "run_id": run_id})
    except Exception as exc:
        checkpoint = load_checkpoint_state(run_id) or {"run_id": run_id}
        checkpoint.update({"status": "FAILED", "error": str(exc)})
        save_checkpoint_state(run_id, checkpoint)
        raise


def submit_pipeline_start(run_id: str, payload: PipelineRunRequest) -> None:
    job_key = f"{run_id}:pipeline"
    source = str(payload.source or "database").lower()
    sftp_entity = api_utils.normalize_file_entity(source, payload.sftp_entity)
    use_domain_kb = False if api_utils.is_file_source(source) else bool(payload.use_domain_kb)
    with BACKGROUND_JOB_LOCK:
        future = BACKGROUND_EXECUTOR.submit(
            run_pipeline_background,
            run_id=run_id,
            brd_text=payload.brd_text,
            source=source,
            source_databases=payload.source_databases
            or ([payload.database_name] if payload.database_name else None),
            sftp_entity=sftp_entity,
            use_domain_kb=use_domain_kb,
            stage_confirmation_enabled=bool(
                payload.stage_confirmation_enabled if payload.stage_confirmation_enabled is not None else True
            ),
        )
        BACKGROUND_JOBS[job_key] = future

    def _cleanup(done) -> None:
        with BACKGROUND_JOB_LOCK:
            if BACKGROUND_JOBS.get(job_key) is done:
                BACKGROUND_JOBS.pop(job_key, None)

    future.add_done_callback(_cleanup)


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
    cleaned["error"] = None
    cleaned["resume_message"] = None
    cleaned["awaiting_stage_confirmation"] = False
    return cleaned


def continue_database_pipeline_job(run_id: str, start_stage_key: str, state: Dict[str, Any]) -> Dict[str, Any]:
    return continue_database_pipeline(run_id, start_stage_key=start_stage_key, state=state)


def continue_file_pipeline_job(run_id: str, state: Dict[str, Any]) -> Dict[str, Any]:
    from source_ingestion_pipeline import build_source_ingestion_graph

    graph_app = build_source_ingestion_graph()
    working_state = dict(state or {})
    working_state["run_id"] = run_id
    result = graph_app.invoke(working_state)
    if not isinstance(result, dict):
        raise ValueError("File-source pipeline returned an invalid state.")
    return result


def database_failed_stage_key(run_id: str, checkpoint: Dict[str, Any]) -> Optional[str]:
    if checkpoint.get("failed_background_stage"):
        return str(checkpoint.get("failed_background_stage"))
    if checkpoint.get("background_stage"):
        return api_utils.stage_key(checkpoint.get("background_stage"))

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
        return str(failed_step)

    next_stage_key = checkpoint.get("next_stage_key")
    if next_stage_key:
        return str(next_stage_key)
    return None
