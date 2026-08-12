from __future__ import annotations

from nodes.semantic_enrichment import semantic_enrichment_node
from state import Stage01State


def sftp_semantic_enrichment_node(state: Stage01State) -> Stage01State:
    normalized = {
        **state,
        "discovered_metadata": state.get("discovered_metadata") or {
            "tables": state.get("file_schema_entries") or [],
            "columns": [
                column
                for table in state.get("file_schema_entries") or []
                for column in table.get("columns") or []
            ],
        },
    }
    return semantic_enrichment_node(normalized)


def sftp_gate3_node(state: Stage01State) -> Stage01State:
    decision = str(
        state.get("gate3_decision") or state.get("enrichment_review_decision") or ""
    ).upper()
    if decision == "REJECTED":
        return {**state, "status": "FAILED", "error": "Semantic Review was rejected."}
    if decision != "APPROVED":
        return {
            **state,
            "status": "HITL_WAIT",
            "next_gate": 3,
            "enrichment_review_status": "PENDING",
            "resume_message": "Semantic Review is pending.",
        }
    return {
        **state,
        "status": "IN_PROGRESS",
        "next_gate": None,
        "enrichment_review_status": "COMPLETED",
        "enrichment_review_decision": "APPROVED",
        "enrichment_review_artifact": state.get("enriched_metadata") or {},
    }
