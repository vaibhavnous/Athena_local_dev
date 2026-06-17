from __future__ import annotations

from typing import Any, Dict, List, Optional

from services.pipeline_runtime import fetch_json_artifact, list_runs, load_checkpoint_state, submit_background, submit_gate1_review
from sftp_nodes.hitl import submit_sftp_gate1_review
from utilis.db import get_pending_items

from api import utils as api_utils
from api.repositories.hitl_repository import fetch_hitl_rows as fetch_hitl_rows_raw


def artifact_kpis(run_id: str) -> List[Dict[str, Any]]:
    payload = fetch_json_artifact(run_id, "KPIS")
    kpis = payload.get("kpis") or payload.get("items") or payload.get("extracted_kpis") or []
    if isinstance(kpis, list):
        return kpis
    return []


def requirements_from_checkpoint(checkpoint: Dict[str, Any]) -> Dict[str, Any]:
    objective = checkpoint.get("req_business_objective")
    domains = checkpoint.get("req_data_domains") or []
    constraints = checkpoint.get("req_constraints") or []
    if not objective and not domains and not constraints:
        return {}
    return {
        "objective": objective,
        "business_objective": objective,
        "data_domains": domains,
        "reporting_frequency": checkpoint.get("req_reporting_frequency"),
        "target_audience": checkpoint.get("req_target_audience"),
        "constraints": constraints,
        "schema_valid": checkpoint.get("req_schema_valid"),
        "prompt_version": checkpoint.get("req_prompt_version"),
    }


def kpis_from_checkpoint(checkpoint: Dict[str, Any]) -> List[Dict[str, Any]]:
    for key in ("kpis", "prior_kpis", "extracted_kpis", "certified_kpis"):
        value = checkpoint.get(key) or []
        if isinstance(value, list) and value:
            return value
    return []


def map_kpi(
    kpi: Dict[str, Any],
    *,
    run_id: str,
    item_id: Optional[str] = None,
    status: str = "PENDING",
    source: Optional[str] = None,
) -> Dict[str, Any]:
    name = kpi.get("name") or kpi.get("kpi_name") or kpi.get("title") or "Unnamed KPI"
    definition = kpi.get("definition") or kpi.get("kpi_description") or kpi.get("description") or ""
    confidence = kpi.get("confidence") or kpi.get("ai_confidence_score") or 0
    return {
        "id": item_id or kpi.get("id") or name,
        "queue_id": item_id,
        "item_id": item_id,
        "item_type": "KPI",
        "gate_status": status,
        "decision": None if status == "PENDING" else status,
        "name": name,
        "definition": definition,
        "category": kpi.get("category") or kpi.get("domain") or "Business KPI",
        "domain": kpi.get("domain") or kpi.get("source_requirement_ref") or "Athena",
        "confidence": float(confidence or 0),
        "status": "PENDING_REVIEW" if status == "PENDING" else status,
        "grounded": str(kpi.get("grounding_status", "")).upper().endswith("PASSED"),
        "explicit": kpi.get("derivation_type") == "explicit",
        "kpi_detail": kpi,
        "run_id": run_id,
        "source": source or kpi.get("source"),
    }


def fetch_hitl_rows(run_id: str, status: Optional[str] = None) -> List[Dict[str, Any]]:
    checkpoint = load_checkpoint_state(run_id) or {}
    mapped = []
    for row in fetch_hitl_rows_raw(run_id, status=status):
        content = row.get("edited_content") or row.get("original_content") or {}
        item = map_kpi(
            content,
            run_id=run_id,
            item_id=row.get("item_id"),
            status=row.get("gate_status", "PENDING"),
            source=checkpoint.get("source"),
        )
        item.update(
            {
                "rejection_reason": row.get("rejection_reason"),
                "queued_at": row.get("queued_at"),
                "decided_at": row.get("decided_at"),
            }
        )
        mapped.append(item)
    return mapped


def maybe_resume_gate1(run_id: str) -> None:
    if get_pending_items(run_id, 1):
        return
    checkpoint = load_checkpoint_state(run_id) or {}
    if api_utils.is_file_source(checkpoint.get("source")):
        submit_background(run_id, "gate1", submit_sftp_gate1_review, run_id, True)
        return
    submit_background(run_id, "gate1", submit_gate1_review, run_id, [])


def list_all_kpis() -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for row in list_runs():
        run_id = row["run_id"]
        items.extend(map_kpi(kpi, run_id=run_id) for kpi in artifact_kpis(run_id))
    return items
