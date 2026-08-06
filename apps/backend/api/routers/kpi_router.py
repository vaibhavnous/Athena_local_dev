import json
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException

from api.auth import AuthUser, assert_run_access, get_current_user, has_request_user
from api.demo import demo_action, demo_enabled, demo_kpi_reviews, demo_start_progress
from api.models import HitlDecisionPayload
from utilis.logger import logger

router = APIRouter()


def _checkpoint_for_user(run_id: str, user: Any) -> Dict[str, Any]:
    return assert_run_access(run_id, user) if has_request_user(user) else {}


def _run_id_from_queue_id(queue_id: str) -> str:
    if ":1:" not in queue_id:
        raise HTTPException(status_code=400, detail="Invalid queue_id format")
    return queue_id.split(":1:", 1)[0]


# -------------------------
# KPI Reviews
# -------------------------
@router.get("/kpi-reviews/{run_id}")
def kpi_reviews(
    run_id: str,
    status: Optional[str] = None,
    user: AuthUser = Depends(get_current_user),
) -> Dict[str, Any]:
    if demo_enabled():
        return demo_kpi_reviews(run_id)

    from api.services.kpi_service import artifact_kpis, fetch_hitl_rows, map_kpi
    from services.pipeline_runtime import fetch_run_summary, load_checkpoint_fields

    checkpoint = _checkpoint_for_user(run_id, user)
    source = str((checkpoint or load_checkpoint_fields(run_id, "source")).get("source") or "database").lower()

    try:
        rows = fetch_hitl_rows(run_id, status=status, checkpoint=checkpoint)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    if not rows:
        rows = [map_kpi(kpi, run_id=run_id, source=source) for kpi in artifact_kpis(run_id)]
    if not rows:
        summary = fetch_run_summary(run_id)
        kpis_failed = any(
            str(row.get("artifact_type") or "").upper() == "KPIS"
            and str(row.get("faithfulness_status") or "").upper() == "FAILED"
            for row in summary
            if isinstance(row, dict)
        )
        if kpis_failed:
            raise HTTPException(
                status_code=409,
                detail="KPI extraction failed before review items were created. Retry KPI extraction for this run.",
            )

    rows = [
        {**row, "run_id": run_id, "source": source}
        for row in rows
        if str(row.get("run_id") or run_id) == str(run_id)
    ]

    return {
        "runId": run_id,
        "run_id": run_id,
        "source": source,
        "kpis": rows,
    }


@router.post("/kpi-reviews/{run_id}")
def create_kpi_review(
    run_id: str,
    payload: Dict[str, Any],
    user: AuthUser = Depends(get_current_user),
) -> Dict[str, Any]:
    name = str((payload or {}).get("name") or "").strip()
    definition = str((payload or {}).get("definition") or "").strip()
    if not name or not definition:
        raise HTTPException(status_code=400, detail="KPI name and description are required.")
    if len(name) > 250 or len(definition) > 5000:
        raise HTTPException(status_code=400, detail="KPI name or description is too long.")

    kpi = {
        "name": name,
        "kpi_name": name,
        "definition": definition,
        "kpi_description": definition,
        "category": str(payload.get("category") or "Business KPI"),
        "domain": str(payload.get("domain") or "Athena"),
        "derivation_type": "reviewer_authored",
        "grounding_status": "HUMAN_AUTHORED",
    }
    if demo_enabled():
        return {
            "id": f"{run_id}:1:manual-demo",
            "queue_id": f"{run_id}:1:manual-demo",
            "item_id": f"{run_id}:1:manual-demo",
            "run_id": run_id,
            "item_type": "KPI",
            "status": "PENDING_REVIEW",
            "name": name,
            "definition": definition,
            "kpi_detail": kpi,
        }

    from api.services.kpi_service import map_kpi
    from services.pipeline_runtime import load_checkpoint_state
    from utilis.db import get_pending_items, insert_hitl_queue_item

    checkpoint = _checkpoint_for_user(run_id, user) or load_checkpoint_state(run_id) or {}
    if not checkpoint:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
    if int(checkpoint.get("next_gate") or 0) != 1 and not get_pending_items(run_id, 1):
        raise HTTPException(status_code=409, detail="KPIs can only be added while KPI Review is pending.")

    try:
        item_id = insert_hitl_queue_item(run_id, kpi, gate_number=1)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Failed to add KPI to the review queue.") from exc

    logger.info("Reviewer-authored KPI added", extra={"run_id": run_id, "queue_id": item_id})
    return map_kpi(kpi, run_id=run_id, item_id=item_id, status="PENDING", source=checkpoint.get("source"))


# -------------------------
# Approve KPI
# -------------------------
@router.post("/kpi-reviews/{queue_id}/approve")
def approve_kpi(
    queue_id: str,
    payload: Dict[str, Any],
    user: AuthUser = Depends(get_current_user),
) -> Dict[str, Any]:
    if demo_enabled():
        run_id = queue_id.split(":1:", 1)[0] if ":1:" in queue_id else queue_id
        return {"queue_id": queue_id, "status": "APPROVED", "run": demo_start_progress(run_id, "kpi")}

    from api.services.kpi_service import maybe_resume_gate1
    from utilis.db import update_hitl_item

    if payload is None:
        raise HTTPException(status_code=400, detail="Invalid payload")

    run_id = _run_id_from_queue_id(queue_id)
    _checkpoint_for_user(run_id, user)

    update_hitl_item(queue_id, "APPROVED")

    logger.info("KPI approved", extra={"queue_id": queue_id})

    maybe_resume_gate1(run_id)

    return {"queue_id": queue_id, "status": "APPROVED"}


# -------------------------
# Reject KPI
# -------------------------
@router.post("/kpi-reviews/{queue_id}/reject")
def reject_kpi(
    queue_id: str,
    payload: Dict[str, Any],
    user: AuthUser = Depends(get_current_user),
) -> Dict[str, Any]:
    if demo_enabled():
        return {"queue_id": queue_id, "status": "REJECTED"}

    from api.services.kpi_service import maybe_resume_gate1
    from utilis.db import update_hitl_item

    if payload is None:
        raise HTTPException(status_code=400, detail="Invalid payload")

    run_id = _run_id_from_queue_id(queue_id)
    _checkpoint_for_user(run_id, user)

    update_hitl_item(queue_id, "REJECTED", rejection_reason=payload.get("rejection_reason"))

    logger.info("KPI rejected", extra={"queue_id": queue_id})

    maybe_resume_gate1(run_id)

    return {"queue_id": queue_id, "status": "REJECTED"}


# -------------------------
# Modify KPI
# -------------------------
@router.post("/kpi-reviews/{queue_id}/modify")
def modify_kpi(
    queue_id: str,
    payload: Dict[str, Any],
    user: AuthUser = Depends(get_current_user),
) -> Dict[str, Any]:
    if demo_enabled():
        run_id = queue_id.split(":1:", 1)[0] if ":1:" in queue_id else queue_id
        return {"queue_id": queue_id, "status": "APPROVED", "run": demo_start_progress(run_id, "kpi")}

    from api.services.kpi_service import maybe_resume_gate1
    from utilis.db import update_hitl_item

    if payload is None:
        raise HTTPException(status_code=400, detail="Invalid payload")

    run_id = _run_id_from_queue_id(queue_id)
    _checkpoint_for_user(run_id, user)

    update_hitl_item(
        queue_id,
        "APPROVED",
        edited_content=json.dumps(payload.get("edited_content") or {}),
    )

    logger.info("KPI modified", extra={"queue_id": queue_id})

    maybe_resume_gate1(run_id)

    return {"queue_id": queue_id, "status": "APPROVED"}


# -------------------------
# Bulk Action
# -------------------------
@router.post("/kpi-reviews/{run_id}/bulk")
def bulk_kpi_action(
    run_id: str,
    payload: Dict[str, Any],
    user: AuthUser = Depends(get_current_user),
) -> Dict[str, Any]:
    if demo_enabled():
        action = payload.get("action", "APPROVED") if payload else "APPROVED"
        return demo_action(run_id, status=action, segment="kpi" if action == "APPROVED" else None)

    from api.services.kpi_service import fetch_hitl_rows, maybe_resume_gate1
    from utilis.db import update_hitl_items_batch

    if payload is None:
        raise HTTPException(status_code=400, detail="Invalid payload")

    _checkpoint_for_user(run_id, user)
    rows = fetch_hitl_rows(run_id)
    action = str(payload.get("action") or "APPROVED").upper()
    if action not in {"APPROVED", "REJECTED"}:
        raise HTTPException(status_code=400, detail="Bulk KPI action must be APPROVED or REJECTED.")
    updates = [
        {
            "item_id": row["queue_id"],
            "status": action,
            "rejection_reason": payload.get("rejection_reason"),
        }
        for row in rows
        if not row.get("decision")
    ]
    try:
        update_hitl_items_batch(updates)
    except Exception as exc:
        raise HTTPException(status_code=409, detail="A KPI is no longer pending; no bulk decisions were saved.") from exc

    logger.info(
        "Bulk KPI action executed",
        extra={"run_id": run_id, "action": action, "updated_count": len(updates)},
    )

    maybe_resume_gate1(run_id)

    return {"run_id": run_id, "status": action, "updated_count": len(updates)}


# -------------------------
# HITL Queue (alias)
# -------------------------
@router.get("/hitl/{run_id}")
def hitl_queue(run_id: str, user: AuthUser = Depends(get_current_user)) -> Dict[str, Any]:
    return kpi_reviews(run_id, user=user)


# -------------------------
# Submit HITL Decisions
# -------------------------
@router.post("/hitl/{run_id}/decisions")
def submit_hitl_decisions(
    run_id: str,
    payload: HitlDecisionPayload,
    user: AuthUser = Depends(get_current_user),
) -> Dict[str, Any]:
    if demo_enabled():
        return demo_action(run_id, segment="kpi")

    from api.services.kpi_service import maybe_resume_gate1
    from utilis.db import update_hitl_items_batch

    _checkpoint_for_user(run_id, user)
    updates = []
    for decision in payload.decisions:
        if not str(decision.kpi_id or "").startswith(f"{run_id}:1:"):
            raise HTTPException(status_code=400, detail="KPI decision does not belong to this run.")

        status = str(decision.decision or "").upper()
        if status not in {"APPROVED", "EDITED", "REJECTED"}:
            raise HTTPException(status_code=400, detail="Unsupported KPI decision.")
        edited_content = None
        if status == "EDITED":
            edited = decision.edited_content or {
                "definition": decision.edited_definition,
                "kpi_description": decision.edited_definition,
                "notes": decision.notes,
            }
            edited_content = json.dumps(edited)
        updates.append(
            {
                "item_id": decision.kpi_id,
                "status": "REJECTED" if status == "REJECTED" else "APPROVED",
                "edited_content": edited_content,
                "rejection_reason": decision.notes if status == "REJECTED" else None,
            }
        )

    try:
        update_hitl_items_batch(updates)
    except Exception as exc:
        logger.warning("Failed to persist HITL decision batch", extra={"run_id": run_id, "error": str(exc)})
        raise HTTPException(status_code=409, detail="A KPI is no longer pending; no decisions were saved.") from exc

    logger.info("HITL decisions submitted", extra={"run_id": run_id, "updated_count": len(updates)})

    try:
        maybe_resume_gate1(run_id)
    except RuntimeError as exc:
        raise HTTPException(
            status_code=503,
            detail=f"KPI decisions were saved, but pipeline resume check failed: {exc}",
        ) from exc

    return {"run_id": run_id, "status": "SUBMITTED"}


# -------------------------
# All KPIs
# -------------------------
@router.get("/kpis")
def kpis(user: AuthUser = Depends(get_current_user)) -> List[Dict[str, Any]]:
    if demo_enabled():
        return demo_kpi_reviews("athena-insurance-run")["kpis"]

    from api.services.kpi_service import list_all_kpis

    return list_all_kpis(user=user)
