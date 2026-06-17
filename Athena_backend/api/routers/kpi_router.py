import json
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException

from api.services.kpi_service import artifact_kpis, fetch_hitl_rows, list_all_kpis, map_kpi, maybe_resume_gate1
from api.services.pipeline_service import load_checkpoint_state
from api.models import HitlDecisionPayload
from utilis.db import update_hitl_item

router = APIRouter()


@router.get("/kpi-reviews/{run_id}")
def kpi_reviews(run_id: str, status: Optional[str] = None) -> Dict[str, Any]:
    checkpoint = load_checkpoint_state(run_id) or {}
    source = str(checkpoint.get("source") or "database").lower()
    try:
        rows = fetch_hitl_rows(run_id, status=status)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if not rows:
        rows = [map_kpi(kpi, run_id=run_id, source=source) for kpi in artifact_kpis(run_id)]
    rows = [
        {**row, "run_id": run_id, "source": source}
        for row in rows
        if str(row.get("run_id") or run_id) == str(run_id)
    ]
    return {"runId": run_id, "run_id": run_id, "source": source, "kpis": rows}


@router.post("/kpi-reviews/{queue_id}/approve")
def approve_kpi(queue_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    update_hitl_item(queue_id, "APPROVED")
    run_id = queue_id.split(":1:", 1)[0]
    maybe_resume_gate1(run_id)
    return {"queue_id": queue_id, "status": "APPROVED"}


@router.post("/kpi-reviews/{queue_id}/reject")
def reject_kpi(queue_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    update_hitl_item(queue_id, "REJECTED", rejection_reason=payload.get("rejection_reason"))
    run_id = queue_id.split(":1:", 1)[0]
    maybe_resume_gate1(run_id)
    return {"queue_id": queue_id, "status": "REJECTED"}


@router.post("/kpi-reviews/{queue_id}/modify")
def modify_kpi(queue_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    update_hitl_item(queue_id, "APPROVED", edited_content=json.dumps(payload.get("edited_content") or {}))
    run_id = queue_id.split(":1:", 1)[0]
    maybe_resume_gate1(run_id)
    return {"queue_id": queue_id, "status": "APPROVED"}


@router.post("/kpi-reviews/{run_id}/bulk")
def bulk_kpi_action(run_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    rows = fetch_hitl_rows(run_id)
    action = payload.get("action", "APPROVED")
    for row in rows:
        if row.get("decision"):
            continue
        update_hitl_item(row["queue_id"], action, rejection_reason=payload.get("rejection_reason"))
    maybe_resume_gate1(run_id)
    return {"run_id": run_id, "status": action}


@router.get("/hitl/{run_id}")
def hitl_queue(run_id: str) -> Dict[str, Any]:
    return kpi_reviews(run_id)


@router.post("/hitl/{run_id}/decisions")
def submit_hitl_decisions(run_id: str, payload: HitlDecisionPayload) -> Dict[str, Any]:
    for decision in payload.decisions:
        if not str(decision.kpi_id or "").startswith(f"{run_id}:"):
            raise HTTPException(status_code=400, detail="KPI decision does not belong to this run.")
        status = decision.decision.upper()
        if status == "EDITED":
            edited = {"definition": decision.edited_definition, "notes": decision.notes}
            update_hitl_item(decision.kpi_id, "APPROVED", edited_content=json.dumps(edited))
        elif status == "REJECTED":
            update_hitl_item(decision.kpi_id, "REJECTED", rejection_reason=decision.notes)
        else:
            update_hitl_item(decision.kpi_id, "APPROVED")
    maybe_resume_gate1(run_id)
    return {"run_id": run_id, "status": "SUBMITTED"}


@router.get("/kpis")
def kpis() -> List[Dict[str, Any]]:
    return list_all_kpis()
