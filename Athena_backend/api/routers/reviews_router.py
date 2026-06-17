from typing import Any, Dict

from fastapi import APIRouter, HTTPException

from api import utils as api_utils
from api.models import Gate2DecisionPayload, Gate3DecisionPayload, GenericGateDecisionPayload
from api.services.ui_service import bronze_review_from_scripts, silver_review_from_scripts, ui_run
from services.pipeline_runtime import load_checkpoint_state, submit_background, submit_gate2_review, submit_gate3_review
from sftp_nodes.hitl import submit_sftp_gate2_review, submit_sftp_gate3_review, submit_sftp_gate4_review, submit_sftp_gate5_review

router = APIRouter()


@router.get("/table-reviews/{run_id}")
def table_reviews(run_id: str) -> Dict[str, Any]:
    try:
        run = ui_run(run_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    checkpoint = load_checkpoint_state(run_id) or {}
    return {
        "run_id": run_id,
        "source": run.get("source"),
        "next_gate": run.get("next_gate"),
        "resume_message": run.get("resume_message"),
        "nominated_tables": run.get("nominated_tables") or [],
        "certified_tables": run.get("certified_tables") or [],
        "candidate_feed": checkpoint.get("candidate_feed") if api_utils.is_file_source(run.get("source")) else None,
        "candidate_feeds": (checkpoint.get("candidate_feeds") or []) if api_utils.is_file_source(run.get("source")) else [],
    }


@router.post("/table-reviews/{run_id}")
def submit_table_reviews(run_id: str, payload: Gate2DecisionPayload) -> Dict[str, Any]:
    checkpoint = load_checkpoint_state(run_id) or {}
    if api_utils.is_file_source(checkpoint.get("source")):
        submit_background(run_id, "gate2", submit_sftp_gate2_review, run_id, True)
        return {"run_id": run_id, "status": "SUBMITTED", "approve": True}

    approved_tables = [item for item in payload.approved_tables if str(item).strip()]
    if not approved_tables:
        raise HTTPException(status_code=400, detail="At least one table must be approved for Table Review.")

    submit_background(run_id, "gate2", submit_gate2_review, run_id, approved_tables)
    return {"run_id": run_id, "status": "SUBMITTED", "approved_tables": approved_tables}


@router.get("/enrichment-reviews/{run_id}")
def enrichment_reviews(run_id: str) -> Dict[str, Any]:
    try:
        run = ui_run(run_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return {
        "run_id": run_id,
        "next_gate": run.get("next_gate"),
        "resume_message": run.get("resume_message"),
        "enriched_metadata": run.get("enriched_metadata") or {},
        "enriched_columns": run.get("enriched_columns") or [],
        "enriched_joins": run.get("enriched_joins") or [],
        "semantic_counts": run.get("semantic_counts") or {},
        "pii_columns": run.get("pii_columns") or [],
        "join_key_columns": run.get("join_key_columns") or [],
        "measure_columns": run.get("measure_columns") or [],
        "feed_semantic_summary": run.get("feed_semantic_summary") or [],
        "gate3_approved": run.get("gate3_approved") or False,
    }


@router.post("/enrichment-reviews/{run_id}")
def submit_enrichment_review(run_id: str, payload: Gate3DecisionPayload) -> Dict[str, Any]:
    checkpoint = load_checkpoint_state(run_id) or {}
    if api_utils.is_file_source(checkpoint.get("source")):
        submit_background(run_id, "gate3", submit_sftp_gate3_review, run_id, payload.approve)
        return {"run_id": run_id, "status": "SUBMITTED", "approve": payload.approve}
    submit_background(run_id, "gate3", submit_gate3_review, run_id, payload.approve)
    return {"run_id": run_id, "status": "SUBMITTED", "approve": payload.approve}


@router.get("/bronze-reviews/{run_id}")
def bronze_reviews(run_id: str) -> Dict[str, Any]:
    try:
        run = ui_run(run_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    checkpoint = load_checkpoint_state(run_id) or {}
    bronze_artifact = checkpoint.get("bronze_review_artifact") or run.get("bronze_review_artifact") or {}
    if not (bronze_artifact.get("feeds") or []):
        bronze_artifact = bronze_review_from_scripts(run_id, checkpoint)
    return {
        "run_id": run_id,
        "next_gate": run.get("next_gate"),
        "resume_message": run.get("resume_message"),
        "bronze_review_artifact": bronze_artifact,
    }


@router.post("/bronze-reviews/{run_id}")
def submit_bronze_reviews(run_id: str, payload: GenericGateDecisionPayload) -> Dict[str, Any]:
    submit_background(run_id, "gate4", submit_sftp_gate4_review, run_id, payload.action)
    return {"run_id": run_id, "status": "SUBMITTED", "action": payload.action}


@router.get("/silver-reviews/{run_id}")
def silver_reviews(run_id: str) -> Dict[str, Any]:
    try:
        run = ui_run(run_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    checkpoint = load_checkpoint_state(run_id) or {}
    silver_artifact = checkpoint.get("silver_review_artifact") or run.get("silver_review_artifact") or {}
    if not (silver_artifact.get("items") or []):
        silver_artifact = silver_review_from_scripts(run_id, checkpoint)
    return {
        "run_id": run_id,
        "next_gate": run.get("next_gate"),
        "resume_message": run.get("resume_message"),
        "silver_review_artifact": silver_artifact,
    }


@router.post("/silver-reviews/{run_id}")
def submit_silver_reviews(run_id: str, payload: GenericGateDecisionPayload) -> Dict[str, Any]:
    submit_background(run_id, "gate5", submit_sftp_gate5_review, run_id, payload.action)
    return {"run_id": run_id, "status": "SUBMITTED", "action": payload.action}
