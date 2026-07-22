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
from api.models import ComplianceReviewPayload, Gate2DecisionPayload, Gate3DecisionPayload, GenericGateDecisionPayload
from utilis.logger import logger

router = APIRouter()


def _compliance_review_decision(findings: list[Dict[str, Any]]) -> str:
    rejected_statuses = {"REJECTED", "EXCLUDED"}
    return "REJECTED" if any(str(item.get("status") or "").upper() in rejected_statuses for item in findings) else "APPROVED"


def _compliance_api_findings(findings: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    status_map = {
        "approved": "Approved",
        "modified": "Modified",
        "excluded": "Excluded",
        "rejected": "Excluded",
    }
    translated: list[Dict[str, Any]] = []
    for item in findings:
        api_item = {
            "table_name": item.get("table_name"),
            "column_name": item.get("column_name"),
            "status": status_map.get(str(item.get("status") or "").strip().lower(), "Approved"),
        }
        if "reviewer_comments" in item:
            api_item["reviewer_comments"] = item.get("reviewer_comments")
        translated.append(api_item)
    return translated


SECURITY_CONTROL_VALUES = {
    "encrypt": "Encrypt",
    "hash": "Hash",
    "mask": "Mask",
    "redact": "Redact",
    "tokenize": "Tokenize",
    "pseudonymize": "Pseudonymize",
    "anonymize": "Anonymize",
    "no_additional_control": "",
    "no additional control": "",
    "none": "",
    "": "",
}


def _normalize_security_control(value: Any) -> str:
    normalized = str(value or "").strip()
    key = normalized.replace("-", "_").replace(" ", "_").casefold()
    return SECURITY_CONTROL_VALUES.get(key, normalized if normalized in set(SECURITY_CONTROL_VALUES.values()) else "")


def _review_security_lookup(review: Dict[str, Any]) -> Dict[tuple[str, str], str]:
    lookup: Dict[tuple[str, str], str] = {}
    for item in review.get("column_evidence", []) if isinstance(review, dict) else []:
        table_name = str(item.get("table_name") or "").strip().casefold()
        column_name = str(item.get("column_name") or "").strip().casefold()
        control = _normalize_security_control(item.get("security_control"))
        if table_name and column_name and control:
            lookup[(table_name, column_name)] = control
    return lookup


def _findings_with_security_controls(findings: list[Dict[str, Any]], review: Dict[str, Any]) -> list[Dict[str, Any]]:
    lookup = _review_security_lookup(review)
    enriched: list[Dict[str, Any]] = []
    for item in findings:
        table_name = str(item.get("table_name") or "").strip()
        column_name = str(item.get("column_name") or "").strip()
        control = _normalize_security_control(item.get("security_control"))
        if not control:
            control = lookup.get((table_name.casefold(), column_name.casefold()), "")
        enriched.append({**item, "security_control": control})
    return enriched


def _security_policies_from_findings(findings: list[Dict[str, Any]]) -> Dict[str, Dict[str, str]]:
    policies: Dict[str, Dict[str, str]] = {}
    for item in findings:
        status = str(item.get("status") or "").strip().casefold()
        if status in {"rejected", "excluded"}:
            continue
        table_name = str(item.get("table_name") or "").strip()
        column_name = str(item.get("column_name") or "").strip().lower()
        control = _normalize_security_control(item.get("security_control"))
        if not table_name or not column_name or not control:
            continue
        policies.setdefault(table_name, {})[column_name] = control
    return policies


@router.get("/compliance-reviews/{run_id}")
def compliance_reviews(run_id: str) -> Dict[str, Any]:
    from services.compliance_client import fetch_review
    from services.pipeline_runtime import load_checkpoint_state, save_checkpoint_state

    try:
        checkpoint = load_checkpoint_state(run_id) or {}
        review = checkpoint.get("compliance_review")
        if checkpoint.get("compliance_enabled") and checkpoint.get("compliance_assessment_id") and not review:
            try:
                review = fetch_review({**checkpoint, "run_id": run_id})
                checkpoint.update(
                    {
                        "compliance_review_status": "READY",
                        "compliance_review": review,
                        "compliance_review_error": None,
                    }
                )
            except Exception as exc:
                checkpoint.update(
                    {
                        "compliance_review_status": "PENDING",
                        "compliance_review_error": str(exc),
                    }
                )
            save_checkpoint_state(run_id, checkpoint)
    except Exception:
        logger.error("Failed to fetch compliance review", exc_info=True, extra={"run_id": run_id})
        raise HTTPException(status_code=503, detail="Failed to load compliance review")

    return {
        "run_id": run_id,
        "compliance_enabled": bool(checkpoint.get("compliance_enabled")),
        "assessment_id": checkpoint.get("compliance_assessment_id"),
        "assessment_status": checkpoint.get("compliance_assessment_status"),
        "assessment_message": checkpoint.get("compliance_assessment_message"),
        "assessment_error": checkpoint.get("compliance_assessment_error"),
        "assessment_submitted_at": checkpoint.get("compliance_assessment_submitted_at"),
        "assessment_completed_at": checkpoint.get("compliance_assessment_completed_at"),
        "review_status": checkpoint.get("compliance_review_status"),
        "review_error": checkpoint.get("compliance_review_error"),
        "review": checkpoint.get("compliance_review") or {},
        "results": checkpoint.get("compliance_results") or {},
    }


@router.post("/compliance-reviews/{run_id}")
def submit_compliance_reviews(run_id: str, payload: ComplianceReviewPayload) -> Dict[str, Any]:
    from services.pipeline_runtime import load_checkpoint_state, resume_after_compliance_review, save_checkpoint_state, submit_background

    checkpoint = load_checkpoint_state(run_id) or {}
    if not checkpoint.get("compliance_enabled"):
        raise HTTPException(status_code=400, detail="Compliance is not enabled for this run.")
    if not checkpoint.get("compliance_assessment_id"):
        raise HTTPException(status_code=409, detail="Compliance assessment is not ready yet.")

    findings = [item.model_dump() for item in payload.findings]
    review = checkpoint.get("compliance_review") or {}
    if not findings:
        findings = [
            {
                "table_name": item.get("table_name"),
                "column_name": item.get("column_name"),
                "status": "Approved",
                "security_control": item.get("security_control"),
                "reviewer_comments": None,
            }
            for item in review.get("column_evidence", [])
            if item.get("table_name") and item.get("column_name")
        ]
    if not findings:
        raise HTTPException(status_code=400, detail="No compliance findings are available to review.")

    try:
        findings = _findings_with_security_controls(findings, review)
        security_policies = _security_policies_from_findings(findings)
        review_decision_payload = {
            "findings": _compliance_api_findings(findings),
            "overall_comments": payload.overall_comments,
        }
        review_decision = _compliance_review_decision(findings)
        decision = {
            "assessment_id": checkpoint.get("compliance_assessment_id"),
            "status": "completed",
            "message": "Compliance review stored in Athena.",
        }
        compliance_results = {
            "assessment_id": checkpoint.get("compliance_assessment_id"),
            "status": "completed",
            "column_evidence": findings,
            "compliance_evidence": (checkpoint.get("compliance_results") or {}).get("compliance_evidence")
            or (review.get("compliance_evidence") if isinstance(review, dict) else None)
            or {},
        }
        updated = {
            **checkpoint,
            "compliance_review_decision": review_decision,
            "compliance_review_decision_response": decision,
            "compliance_review_findings": findings,
            "compliance_review_submit_payload": review_decision_payload,
            "compliance_assessment_status": decision.get("status") or checkpoint.get("compliance_assessment_status"),
            "compliance_review_error": None,
            "compliance_results": compliance_results,
            "compliance_results_status": "completed",
            "compliance_results_error": None,
            "security_policies": security_policies,
            "column_security_policies": security_policies,
            "security_policy_source": "compliance_review",
        }
        updated["next_review_key"] = None
        updated["next_gate"] = None
        updated["resume_message"] = "Compliance Review submitted. Bronze generation is starting."
        save_checkpoint_state(run_id, updated)
        submit_background(run_id, "compliance_review", resume_after_compliance_review, run_id, updated)
    except HTTPException:
        raise
    except Exception:
        logger.error("Failed to submit compliance review", exc_info=True, extra={"run_id": run_id})
        raise HTTPException(status_code=503, detail="Failed to submit compliance review")

    return {
        "run_id": run_id,
        "status": updated.get("compliance_assessment_status"),
        "decision": updated.get("compliance_review_decision"),
        "results_status": updated.get("compliance_results_status"),
        "resume_status": "SUBMITTED",
        "security_policy_count": sum(len(columns) for columns in (updated.get("security_policies") or {}).values()),
    }


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
        checkpoint = load_checkpoint_state(run_id) or {}
        run = checkpoint
        if not (checkpoint.get("nominated_tables") or checkpoint.get("candidate_feed") or checkpoint.get("candidate_feeds")):
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
        return demo_action(run_id, segment="table", approved_tables=payload.approved_tables)

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

    from services.pipeline_runtime import load_checkpoint_state
    from api.services.ui_service import ui_run

    try:
        run = load_checkpoint_state(run_id) or {}
        if not run.get("enriched_metadata") and not run.get("enriched_columns"):
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
        return demo_action(run_id, segment="enrichment" if payload.approve else None, approve=payload.approve)

    from services.pipeline_runtime import (
        load_checkpoint_state,
        submit_background,
        submit_gate3_review,
    )
    from sftp_nodes.hitl import submit_sftp_gate3_review

    checkpoint = load_checkpoint_state(run_id) or {}

    logger.info("Submitting enrichment review", extra={"run_id": run_id})

    if api_utils.is_file_source(checkpoint.get("source")):
        submit_background(run_id, "gate3", submit_sftp_gate3_review, run_id, payload.approve, payload.enriched_metadata)
        return {"run_id": run_id, "status": "SUBMITTED", "approve": payload.approve}

    submit_background(run_id, "gate3", submit_gate3_review, run_id, payload.approve, payload.enriched_metadata)

    return {"run_id": run_id, "status": "SUBMITTED", "approve": payload.approve}


# -------------------------
# ✅ BRONZE REVIEWS (GET)
# -------------------------
@router.get("/bronze-reviews/{run_id}")
def bronze_reviews(run_id: str) -> Dict[str, Any]:
    if demo_enabled():
        return demo_bronze_review(run_id)

    from api.services.ui_service import bronze_review_from_scripts, normalize_bronze_review_artifact, ui_run
    from services.pipeline_runtime import load_checkpoint_state

    try:
        checkpoint = load_checkpoint_state(run_id) or {}
        run = checkpoint
        if not (checkpoint.get("bronze_review_artifact") or checkpoint.get("bronze_generation_results")):
            run = ui_run(run_id)
    except Exception:
        logger.error("Failed to fetch bronze review", exc_info=True, extra={"run_id": run_id})
        raise HTTPException(status_code=503, detail="Failed to load bronze review")

    bronze_artifact = checkpoint.get("bronze_review_artifact") or run.get("bronze_review_artifact") or {}

    if not (bronze_artifact.get("feeds") or []):
        bronze_artifact = bronze_review_from_scripts(run_id, checkpoint)
    bronze_artifact = normalize_bronze_review_artifact(bronze_artifact, checkpoint)

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
        return demo_action(run_id, segment="bronze" if payload.action == "APPROVED" else None, action=payload.action)

    from api.services.ui_service import bronze_review_from_scripts
    from services.pipeline_runtime import load_checkpoint_state, submit_background, submit_gate4_review
    from sftp_nodes.hitl import submit_sftp_gate4_review
    from services.databricks_runtime import databricks_bronze_execution_enabled

    logger.info("Submitting bronze review", extra={"run_id": run_id, "action": payload.action})

    checkpoint = load_checkpoint_state(run_id) or {}
    review_artifact = payload.review_artifact or {}
    if not (review_artifact.get("feeds") or []):
        review_artifact = checkpoint.get("bronze_review_artifact") or bronze_review_from_scripts(run_id, checkpoint) or {}
    if str(payload.action or "APPROVED").upper() == "APPROVED" and not (review_artifact.get("feeds") or []):
        raise HTTPException(status_code=409, detail="Bronze review is not ready yet. Generated Bronze scripts are still loading.")

    if api_utils.is_file_source(checkpoint.get("source")):
        stage = "bronze_code_execution" if str(payload.action).upper() == "APPROVED" else "gate4"
        submit_background(run_id, stage, submit_sftp_gate4_review, run_id, payload.action, review_artifact)
    else:
        stage = (
            "bronze_code_execution"
            if str(payload.action).upper() == "APPROVED"
            and (
                str(checkpoint.get("target_warehouse") or "").lower() == "snowflake"
                or (
                    str(checkpoint.get("target_warehouse") or "").lower() == "databricks"
                    and databricks_bronze_execution_enabled()
                )
            )
            else "silver_merge_key_review" if str(payload.action).upper() == "APPROVED"
            else "gate4"
        )
        submit_background(run_id, stage, submit_gate4_review, run_id, payload.action, review_artifact, checkpoint)

    return {"run_id": run_id, "status": "SUBMITTED", "action": payload.action}


# -------------------------
# ✅ SILVER REVIEWS (GET)
# -------------------------
@router.get("/silver-merge-key-reviews/{run_id}")
def silver_merge_key_reviews(run_id: str) -> Dict[str, Any]:
    from services.pipeline_runtime import _silver_merge_key_review_artifact, load_checkpoint_state

    try:
        run = load_checkpoint_state(run_id) or {}
    except Exception:
        logger.error("Failed to fetch Silver merge-key review", exc_info=True, extra={"run_id": run_id})
        raise HTTPException(status_code=503, detail="Failed to load Silver merge-key review")

    return {
        "run_id": run_id,
        "next_gate": run.get("next_gate"),
        "next_review_key": run.get("next_review_key"),
        "resume_message": run.get("resume_message"),
        "silver_merge_key_review_artifact": _silver_merge_key_review_artifact(run),
    }


@router.post("/silver-merge-key-reviews/{run_id}")
def submit_silver_merge_key_reviews(run_id: str, payload: GenericGateDecisionPayload) -> Dict[str, Any]:
    from services.pipeline_runtime import submit_background, submit_silver_merge_key_review

    logger.info("Submitting Silver merge-key review", extra={"run_id": run_id, "action": payload.action})
    stage = "silver" if str(payload.action).upper() == "APPROVED" else "silver_merge_key_review"
    submit_background(run_id, stage, submit_silver_merge_key_review, run_id, payload.action, payload.review_artifact)

    return {"run_id": run_id, "status": "SUBMITTED", "action": payload.action}


@router.get("/silver-reviews/{run_id}")
def silver_reviews(run_id: str) -> Dict[str, Any]:
    if demo_enabled():
        return demo_silver_review(run_id)

    from api.services.ui_service import silver_review_from_scripts, ui_run
    from services.pipeline_runtime import load_checkpoint_state

    try:
        checkpoint = load_checkpoint_state(run_id) or {}
        run = checkpoint
        if not (checkpoint.get("silver_review_artifact") or checkpoint.get("silver_generation_results")):
            run = ui_run(run_id)
    except Exception:
        logger.error("Failed to fetch silver review", exc_info=True, extra={"run_id": run_id})
        raise HTTPException(status_code=503, detail="Failed to load silver review")

    silver_artifact = checkpoint.get("silver_review_artifact") or run.get("silver_review_artifact") or {}

    generated_artifact = silver_review_from_scripts(run_id, checkpoint)
    if int(run.get("next_gate") or checkpoint.get("next_gate") or 0) == 5 and generated_artifact.get("items"):
        # A failed execution retry regenerates files before Gate 5. The generated
        # bundle is authoritative while review is pending; a checkpoint artifact
        # can still contain scripts from the previous attempt.
        silver_artifact = generated_artifact
    elif not (silver_artifact.get("items") or []):
        silver_artifact = generated_artifact

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
        return demo_action(run_id, segment="silver" if payload.action == "APPROVED" else None, action=payload.action)

    from services.pipeline_runtime import load_checkpoint_state, submit_background, submit_gate5_review
    from sftp_nodes.hitl import submit_sftp_gate5_review
    from services.databricks_runtime import databricks_silver_execution_enabled

    logger.info("Submitting silver review", extra={"run_id": run_id, "action": payload.action})

    checkpoint = load_checkpoint_state(run_id) or {}
    stage = (
        "silver_code_execution"
        if str(payload.action).upper() == "APPROVED"
        and (
            str(checkpoint.get("target_warehouse") or "").lower() == "snowflake"
            or (
                str(checkpoint.get("target_warehouse") or "").lower() == "databricks"
                and databricks_silver_execution_enabled()
            )
        )
        else "gold" if str(payload.action).upper() == "APPROVED"
        else "gate5"
    )
    if api_utils.is_file_source(checkpoint.get("source")):
        submit_background(run_id, stage, submit_sftp_gate5_review, run_id, payload.action, payload.review_artifact)
    else:
        submit_background(run_id, stage, submit_gate5_review, run_id, payload.action, payload.review_artifact)

    return {"run_id": run_id, "status": "SUBMITTED", "action": payload.action}


@router.get("/gold-reviews/{run_id}")
def gold_reviews(run_id: str) -> Dict[str, Any]:
    from services.pipeline_runtime import load_checkpoint_state

    run = load_checkpoint_state(run_id) or {}
    artifact = run.get("gold_review_artifact") or {
        "items": [item for item in run.get("gold_generation_results") or [] if isinstance(item, dict)]
    }
    return {
        "run_id": run_id,
        "next_review_key": run.get("next_review_key"),
        "resume_message": run.get("resume_message"),
        "gold_review_artifact": artifact,
    }


@router.post("/gold-reviews/{run_id}")
def submit_gold_reviews(run_id: str, payload: GenericGateDecisionPayload) -> Dict[str, Any]:
    from services.pipeline_runtime import submit_background, submit_gold_review

    logger.info("Submitting Gold review", extra={"run_id": run_id, "action": payload.action})
    submit_background(run_id, "gold_code_execution", submit_gold_review, run_id, payload.action, payload.review_artifact)
    return {"run_id": run_id, "status": "SUBMITTED", "action": payload.action}
