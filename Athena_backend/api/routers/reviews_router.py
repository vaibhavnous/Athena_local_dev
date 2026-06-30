from typing import Any, Dict

from fastapi import APIRouter, HTTPException

from api import utils as api_utils
from api.demo import (
    demo_action,
    demo_bronze_review,
    demo_enabled,
    demo_enrichment_reviews,
    demo_silver_review,
    demo_table_reviews,
)
from api.models import Gate2DecisionPayload, Gate3DecisionPayload, GenericGateDecisionPayload
from utilis.logger import logger

router = APIRouter()


# -------------------------
# ✅ TABLE REVIEWS (GET)
# -------------------------
@router.get("/table-reviews/{run_id}")
def table_reviews(run_id: str) -> Dict[str, Any]:
    if demo_enabled():
        return demo_table_reviews(run_id)

    from api.services.ui_service import ui_run
    from services.pipeline_runtime import load_checkpoint_state

    try:
        run = ui_run(run_id)
    except Exception:
        logger.error("Failed to fetch table review", exc_info=True, extra={"run_id": run_id})
        raise HTTPException(status_code=503, detail="Failed to load table review")

    checkpoint = load_checkpoint_state(run_id) or {}

    return {
        "run_id": run_id,
        "source": run.get("source"),
        "next_gate": run.get("next_gate"),
        "resume_message": run.get("resume_message"),
        "nominated_tables": run.get("nominated_tables") or [],
        "certified_tables": run.get("certified_tables") or [],
        "candidate_feed": checkpoint.get("candidate_feed")
        if api_utils.is_file_source(run.get("source"))
        else None,
        "candidate_feeds": (checkpoint.get("candidate_feeds") or [])
        if api_utils.is_file_source(run.get("source"))
        else [],
    }


# -------------------------
# ✅ TABLE REVIEWS (POST)
# -------------------------
@router.post("/table-reviews/{run_id}")
def submit_table_reviews(run_id: str, payload: Gate2DecisionPayload) -> Dict[str, Any]:
    if demo_enabled():
        return demo_action(run_id, approved_tables=payload.approved_tables)

    from services.pipeline_runtime import (
        load_checkpoint_state,
        submit_background,
        submit_gate2_review,
    )
    from sftp_nodes.hitl import submit_sftp_gate2_review

    checkpoint = load_checkpoint_state(run_id) or {}

    logger.info("Submitting table review", extra={"run_id": run_id})

    if api_utils.is_file_source(checkpoint.get("source")):
        submit_background(run_id, "gate2", submit_sftp_gate2_review, run_id, True)
        return {"run_id": run_id, "status": "SUBMITTED", "approve": True}

    approved_tables = [item for item in payload.approved_tables if str(item).strip()]

    if not approved_tables:
        raise HTTPException(status_code=400, detail="At least one table must be approved for Table Review.")

    submit_background(run_id, "gate2", submit_gate2_review, run_id, approved_tables)

    return {"run_id": run_id, "status": "SUBMITTED", "approved_tables": approved_tables}


# -------------------------
# ✅ ENRICHMENT REVIEWS (GET)
# -------------------------
@router.get("/enrichment-reviews/{run_id}")
def enrichment_reviews(run_id: str) -> Dict[str, Any]:
    if demo_enabled():
        return demo_enrichment_reviews(run_id)

    from api.services.ui_service import ui_run

    try:
        run = ui_run(run_id)
    except Exception:
        logger.error("Failed to fetch enrichment review", exc_info=True, extra={"run_id": run_id})
        raise HTTPException(status_code=503, detail="Failed to load enrichment review")

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


# -------------------------
# ✅ ENRICHMENT REVIEWS (POST)
# -------------------------
@router.post("/enrichment-reviews/{run_id}")
def submit_enrichment_review(run_id: str, payload: Gate3DecisionPayload) -> Dict[str, Any]:
    if demo_enabled():
        return demo_action(run_id, approve=payload.approve)

    from services.pipeline_runtime import (
        load_checkpoint_state,
        submit_background,
        submit_gate3_review,
    )
    from sftp_nodes.hitl import submit_sftp_gate3_review

    checkpoint = load_checkpoint_state(run_id) or {}

    logger.info("Submitting enrichment review", extra={"run_id": run_id})

    if api_utils.is_file_source(checkpoint.get("source")):
        submit_background(run_id, "gate3", submit_sftp_gate3_review, run_id, payload.approve)
        return {"run_id": run_id, "status": "SUBMITTED", "approve": payload.approve}

    submit_background(run_id, "gate3", submit_gate3_review, run_id, payload.approve)

    return {"run_id": run_id, "status": "SUBMITTED", "approve": payload.approve}


# -------------------------
# ✅ BRONZE REVIEWS (GET)
# -------------------------
@router.get("/bronze-reviews/{run_id}")
def bronze_reviews(run_id: str) -> Dict[str, Any]:
    if demo_enabled():
        return demo_bronze_review(run_id)

    from api.services.ui_service import bronze_review_from_scripts, ui_run
    from services.pipeline_runtime import load_checkpoint_state

    try:
        run = ui_run(run_id)
    except Exception:
        logger.error("Failed to fetch bronze review", exc_info=True, extra={"run_id": run_id})
        raise HTTPException(status_code=503, detail="Failed to load bronze review")

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


# -------------------------
# ✅ BRONZE REVIEWS (POST)
# -------------------------
@router.post("/bronze-reviews/{run_id}")
def submit_bronze_reviews(run_id: str, payload: GenericGateDecisionPayload) -> Dict[str, Any]:
    if demo_enabled():
        return demo_action(run_id, action=payload.action)

    from services.pipeline_runtime import submit_background
    from sftp_nodes.hitl import submit_sftp_gate4_review

    logger.info("Submitting bronze review", extra={"run_id": run_id, "action": payload.action})

    submit_background(run_id, "gate4", submit_sftp_gate4_review, run_id, payload.action)

    return {"run_id": run_id, "status": "SUBMITTED", "action": payload.action}


# -------------------------
# ✅ SILVER REVIEWS (GET)
# -------------------------
@router.get("/silver-reviews/{run_id}")
def silver_reviews(run_id: str) -> Dict[str, Any]:
    if demo_enabled():
        return demo_silver_review(run_id)

    from api.services.ui_service import silver_review_from_scripts, ui_run
    from services.pipeline_runtime import load_checkpoint_state

    try:
        run = ui_run(run_id)
    except Exception:
        logger.error("Failed to fetch silver review", exc_info=True, extra={"run_id": run_id})
        raise HTTPException(status_code=503, detail="Failed to load silver review")

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


# -------------------------
# ✅ SILVER REVIEWS (POST)
# -------------------------
@router.post("/silver-reviews/{run_id}")
def submit_silver_reviews(run_id: str, payload: GenericGateDecisionPayload) -> Dict[str, Any]:
    if demo_enabled():
        return demo_action(run_id, action=payload.action)

    from services.pipeline_runtime import submit_background
    from sftp_nodes.hitl import submit_sftp_gate5_review

    logger.info("Submitting silver review", extra={"run_id": run_id, "action": payload.action})

    submit_background(run_id, "gate5", submit_sftp_gate5_review, run_id, payload.action)

    return {"run_id": run_id, "status": "SUBMITTED", "action": payload.action}
