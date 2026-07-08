import json
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException

from api.demo import demo_action, demo_enabled, demo_kpi_reviews, demo_start_progress
from api.models import HitlDecisionPayload
from utilis.logger import logger

router = APIRouter()


# -------------------------
# ✅ KPI Reviews
# -------------------------
@router.get("/kpi-reviews/{run_id}")
def kpi_reviews(run_id: str, status: Optional[str] = None) -> Dict[str, Any]:
    if demo_enabled():
        return demo_kpi_reviews(run_id)

    from api.services.kpi_service import artifact_kpis, fetch_hitl_rows, map_kpi
    from services.pipeline_runtime import load_checkpoint_state

    checkpoint = load_checkpoint_state(run_id) or {}
    source = str(checkpoint.get("source") or "database").lower()

    try:
        rows = fetch_hitl_rows(run_id, status=status, checkpoint=checkpoint)  # ✅ reuse checkpoint
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    if not rows:
        rows = [map_kpi(kpi, run_id=run_id, source=source) for kpi in artifact_kpis(run_id)]

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


# -------------------------
# ✅ Approve KPI
# -------------------------
@router.post("/kpi-reviews/{queue_id}/approve")
def approve_kpi(queue_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    if demo_enabled():
        run_id = queue_id.split(":1:", 1)[0] if ":1:" in queue_id else queue_id
        return {"queue_id": queue_id, "status": "APPROVED", "run": demo_start_progress(run_id, "kpi")}

    from api.services.kpi_service import maybe_resume_gate1
    from utilis.db import update_hitl_item

    if payload is None:
        raise HTTPException(status_code=400, detail="Invalid payload")

    try:
        run_id = queue_id.split(":1:", 1)[0]
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid queue_id format")

    update_hitl_item(queue_id, "APPROVED")

    logger.info("KPI approved", extra={"queue_id": queue_id})

    maybe_resume_gate1(run_id)

    return {"queue_id": queue_id, "status": "APPROVED"}


# -------------------------
# ✅ Reject KPI
# -------------------------
@router.post("/kpi-reviews/{queue_id}/reject")
def reject_kpi(queue_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    if demo_enabled():
        return {"queue_id": queue_id, "status": "REJECTED"}

    from api.services.kpi_service import maybe_resume_gate1
    from utilis.db import update_hitl_item

    if payload is None:
        raise HTTPException(status_code=400, detail="Invalid payload")

    try:
        run_id = queue_id.split(":1:", 1)[0]
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid queue_id format")

    update_hitl_item(queue_id, "REJECTED", rejection_reason=payload.get("rejection_reason"))

    logger.info("KPI rejected", extra={"queue_id": queue_id})

    maybe_resume_gate1(run_id)

    return {"queue_id": queue_id, "status": "REJECTED"}


# -------------------------
# ✅ Modify KPI
# -------------------------
@router.post("/kpi-reviews/{queue_id}/modify")
def modify_kpi(queue_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    if demo_enabled():
        run_id = queue_id.split(":1:", 1)[0] if ":1:" in queue_id else queue_id
        return {"queue_id": queue_id, "status": "APPROVED", "run": demo_start_progress(run_id, "kpi")}

    from api.services.kpi_service import maybe_resume_gate1
    from utilis.db import update_hitl_item

    if payload is None:
        raise HTTPException(status_code=400, detail="Invalid payload")

    try:
        run_id = queue_id.split(":1:", 1)[0]
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid queue_id format")

    update_hitl_item(
        queue_id,
        "APPROVED",
        edited_content=json.dumps(payload.get("edited_content") or {}),
    )

    logger.info("KPI modified", extra={"queue_id": queue_id})

    maybe_resume_gate1(run_id)

    return {"queue_id": queue_id, "status": "APPROVED"}


# -------------------------
# ✅ Bulk Action
# -------------------------
@router.post("/kpi-reviews/{run_id}/bulk")
def bulk_kpi_action(run_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    if demo_enabled():
        action = payload.get("action", "APPROVED") if payload else "APPROVED"
        return demo_action(run_id, status=action, segment="kpi" if action == "APPROVED" else None)

    from api.services.kpi_service import fetch_hitl_rows, maybe_resume_gate1
    from utilis.db import update_hitl_item

    if payload is None:
        raise HTTPException(status_code=400, detail="Invalid payload")

    rows = fetch_hitl_rows(run_id)
    action = payload.get("action", "APPROVED")

    for row in rows:
        if row.get("decision"):
            continue
        try:
            update_hitl_item(
                row["queue_id"],
                action,
                rejection_reason=payload.get("rejection_reason"),
            )
        except Exception:
            logger.warning("Bulk update failed for KPI", extra={"queue_id": row.get("queue_id")})
            continue

    logger.info("Bulk KPI action executed", extra={"run_id": run_id, "action": action})

    maybe_resume_gate1(run_id)

    return {"run_id": run_id, "status": action}


# -------------------------
# ✅ HITL Queue (alias)
# -------------------------
@router.get("/hitl/{run_id}")
def hitl_queue(run_id: str) -> Dict[str, Any]:
    return kpi_reviews(run_id)


# -------------------------
# ✅ Submit HITL Decisions
# -------------------------
@router.post("/hitl/{run_id}/decisions")
def submit_hitl_decisions(run_id: str, payload: HitlDecisionPayload) -> Dict[str, Any]:
    if demo_enabled():
        return demo_action(run_id, segment="kpi")

    from api.services.kpi_service import maybe_resume_gate1
    from utilis.db import update_hitl_item

    for decision in payload.decisions:
        if not str(decision.kpi_id or "").startswith(f"{run_id}:"):
            raise HTTPException(status_code=400, detail="KPI decision does not belong to this run.")

        status = decision.decision.upper()

        try:
            if status == "EDITED":
                edited = {
                    "definition": decision.edited_definition,
                    "notes": decision.notes,
                }
                update_hitl_item(decision.kpi_id, "APPROVED", edited_content=json.dumps(edited))

            elif status == "REJECTED":
                update_hitl_item(decision.kpi_id, "REJECTED", rejection_reason=decision.notes)

            else:
                update_hitl_item(decision.kpi_id, "APPROVED")

        except Exception as exc:
            logger.warning(
                "Failed to process HITL decision",
                extra={"run_id": run_id, "kpi_id": decision.kpi_id, "error": str(exc)},
            )
            raise HTTPException(
                status_code=503,
                detail=f"Failed to persist KPI decision {decision.kpi_id}: {exc}",
            ) from exc

    logger.info("HITL decisions submitted", extra={"run_id": run_id})

    try:
        maybe_resume_gate1(run_id)
    except RuntimeError as exc:
        raise HTTPException(
            status_code=503,
            detail=f"KPI decisions were saved, but pipeline resume check failed: {exc}",
        ) from exc

    return {"run_id": run_id, "status": "SUBMITTED"}


# -------------------------
# ✅ All KPIs
# -------------------------
@router.get("/kpis")
def kpis() -> List[Dict[str, Any]]:
    if demo_enabled():
        return demo_kpi_reviews("athena-insurance-run")["kpis"]

    from api.services.kpi_service import list_all_kpis

    return list_all_kpis()
