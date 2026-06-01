from __future__ import annotations

from typing import Any, Dict, List, Optional

from source_ingestion_pipeline import build_source_ingestion_graph
from services.pipeline_runtime import (
    build_pipeline_steps,
    fetch_json_artifact,
    fetch_run_summary,
    load_bronze_scripts,
    load_checkpoint_state,
    load_gold_scripts,
    load_silver_scripts,
)


def build_sftp_display_name(checkpoint: Dict[str, Any]) -> str:
    # Backward-compat name: used by file-based sources (SFTP + ADLS Gen2).
    source = str(checkpoint.get("source") or "sftp").lower()
    prefix = "adls" if source == "adls_gen2" else "sftp"
    vendor = str(checkpoint.get("vendor") or "Vendor1")
    candidate_feeds = checkpoint.get("candidate_feeds") or []
    discovered_entities = sorted(
        {
            str(feed.get("entity") or "").lower()
            for feed in candidate_feeds
            if str(feed.get("entity") or "").strip()
        }
    )
    if len(discovered_entities) > 1:
        return f"{prefix}:{vendor}:{'+'.join(discovered_entities)}"
    entity = str(checkpoint.get("sftp_entity") or "transactions").lower()
    if entity == "both":
        return f"{prefix}:{vendor}:employee+transactions"
    return f"{prefix}:{vendor}:{entity}"


def apply_waiting_stage_state(steps: List[Dict[str, Any]], gate_key: Optional[str]) -> List[Dict[str, Any]]:
    if not gate_key:
        return steps
    for step in steps:
        if step.get("key") == gate_key:
            step["state"] = "HITL_WAIT"
            break
    return steps


def start_sftp_pipeline(
    *,
    run_id: str,
    brd_text: Optional[str] = None,
    sftp_entity: Optional[str] = None,
    source: str = "sftp",
) -> Dict[str, Any]:
    initial_state: Dict[str, Any] = {
        "brd_text": brd_text or "",
        "run_id": run_id,
        "metadata": {},
        "status": "PENDING",
        "source": str(source or "sftp").lower(),
        "sftp_entity": str(sftp_entity or "transactions").lower(),
    }
    graph_app = build_source_ingestion_graph()
    result = graph_app.invoke(initial_state)
    return {
        "run_id": run_id,
        "result": result,
    }


def get_sftp_run_context(run_id: str) -> Dict[str, Any]:
    checkpoint = load_checkpoint_state(run_id) or {}
    summary = fetch_run_summary(run_id)

    gate1_decision = (checkpoint.get("gate1") or {}).get("decision")
    gate2_decision = (checkpoint.get("gate2") or {}).get("decision")
    gate3_decision = str(checkpoint.get("enrichment_review_decision") or "").upper()
    candidate_feed = checkpoint.get("candidate_feed") or {}

    bronze_generation_completed = any(
        row.get("artifact_type") in {"BRONZE_GENERATION", "BRONZE_SCRIPTS"}
        or str(row.get("stage", "")).lower().startswith("bronze")
        for row in summary
    ) or checkpoint.get("bronze_generation_status") == "COMPLETED"
    silver_generation_completed = any(
        row.get("artifact_type") in {"SILVER_GENERATION", "SILVER_SCRIPTS"}
        or str(row.get("stage", "")).lower().startswith("silver")
        for row in summary
    ) or checkpoint.get("silver_generation_status") == "COMPLETED"
    gold_generation_completed = any(
        row.get("artifact_type") in {"GOLD_GENERATION", "GOLD_SCRIPTS"}
        or str(row.get("stage", "")).lower().startswith("gold")
        for row in summary
    ) or str(checkpoint.get("gold_generation_status") or "").startswith("COMPLETED")
    schema_discovery_completed = bool(
        any(row.get("artifact_type") == "SFTP_SCHEMA_SNAPSHOT" for row in summary)
        or checkpoint.get("metadata_status") == "COMPLETED"
    )
    column_profiling_completed = bool(
        any(row.get("artifact_type") == "SFTP_COLUMN_PROFILING" for row in summary)
        or checkpoint.get("column_profiling_status") == "COMPLETED"
    )
    enriched_payload = fetch_json_artifact(run_id, "ENRICHED_METADATA") or checkpoint.get("enriched_metadata") or {}
    gate3_payload = fetch_json_artifact(run_id, "GATE3_APPROVED_ENRICHMENT") or checkpoint.get("enrichment_review_artifact") or {}
    semantic_enrichment_completed = bool(
        any(row.get("artifact_type") == "ENRICHED_METADATA" for row in summary)
        or checkpoint.get("semantic_enrichment_status") == "COMPLETED"
        or enriched_payload
    )

    next_gate = None
    resume_message = None
    if gate1_decision in {None, ""}:
        next_gate = 1
        resume_message = "Gate 1 is pending. Review KPI items before continuing."
    elif gate1_decision == "APPROVED" and gate2_decision in {None, ""}:
        next_gate = 2
        candidate_feeds = checkpoint.get("candidate_feeds") or []
        entities = ", ".join(
            sorted(
                {
                    str(feed.get("entity") or "").strip()
                    for feed in candidate_feeds
                    if str(feed.get("entity") or "").strip()
                }
            )
        )
        feed_count = len(candidate_feeds) or (1 if candidate_feed else 0)
        if feed_count > 1 and entities:
            resume_message = f"Gate 2 is pending. Review {feed_count} discovered feeds ({entities}) before continuing."
        else:
            resume_message = "Gate 2 is pending. Review the discovered feed before continuing."
    elif gate2_decision == "APPROVED":
        if semantic_enrichment_completed and gate3_decision not in {"APPROVED", "REJECTED"}:
            next_gate = 3
            resume_message = "Gate 3 is pending. Review semantic enrichment before continuing."
        elif gate3_decision == "APPROVED" or gate3_payload:
            resume_message = "SFTP semantic enrichment is approved."
        elif column_profiling_completed:
            resume_message = "SFTP schema discovery and column profiling are complete."
        elif schema_discovery_completed:
            resume_message = "Schema discovery is complete. Column profiling is in progress."
        else:
            resume_message = "Gate 2 is complete."
    elif gate1_decision == "REJECTED":
        resume_message = "Gate 1 was rejected."
    elif gate2_decision == "REJECTED":
        resume_message = "Gate 2 was rejected."
    elif gate3_decision == "REJECTED":
        resume_message = "Gate 3 was rejected."
    elif not summary and not checkpoint:
        resume_message = "No stored state was found for this run ID."

    status = checkpoint.get("status") or "UNKNOWN"
    if gate3_decision == "APPROVED" or gate3_payload or bronze_generation_completed or silver_generation_completed or gold_generation_completed:
        status = "PIPELINE_COMPLETED"

    pipeline_steps = build_pipeline_steps(
        source=str(checkpoint.get("source") or "sftp").lower(),
        checkpoint=checkpoint,
        summary=summary,
        pending_gate1=[],
        completed_gate1=[],
        nominated_tables=[],
        certified_tables=[],
        enriched_payload=enriched_payload if isinstance(enriched_payload, dict) else {},
        gate3_payload=gate3_payload if isinstance(gate3_payload, dict) else {},
        bronze_generation_completed=bronze_generation_completed,
        silver_generation_completed=silver_generation_completed,
        gold_generation_completed=gold_generation_completed,
    )
    waiting_gate_key = "gate1" if next_gate == 1 else "gate2" if next_gate == 2 else "gate3" if next_gate == 3 else None
    pipeline_steps = apply_waiting_stage_state(pipeline_steps, waiting_gate_key)
    current_pipeline_step = next((step for step in pipeline_steps if step["state"] == "RUNNING"), None)
    if not current_pipeline_step and waiting_gate_key:
        current_pipeline_step = next((step for step in pipeline_steps if step["key"] == waiting_gate_key), None)
    if not current_pipeline_step and status == "PIPELINE_COMPLETED":
        current_pipeline_step = {
            "key": "completed",
            "label": "Pipeline Completed",
            "state": "COMPLETED",
            "detail": "All configured SFTP stages completed",
        }

    bronze = load_bronze_scripts(run_id, checkpoint) if bronze_generation_completed else {"generated_at": None, "scripts": []}
    silver = load_silver_scripts(run_id, checkpoint) if silver_generation_completed else {"generated_at": None, "scripts": []}
    gold = load_gold_scripts(run_id, checkpoint) if gold_generation_completed else {"generated_at": None, "scripts": []}

    return {
        "run_id": run_id,
        "checkpoint": checkpoint,
        "summary": summary,
        "pending_gate1": [],
        "completed_gate1": [],
        "nominated_tables": [],
        "certified_tables": [],
        "enriched_metadata": enriched_payload if isinstance(enriched_payload, dict) else {},
        "enriched_columns": enriched_payload.get("columns") or [],
        "enriched_joins": enriched_payload.get("joins") or [],
        "semantic_counts": enriched_payload.get("semantic_counts") or {},
        "pii_columns": [c for c in (enriched_payload.get("columns") or []) if c.get("is_pii")],
        "join_key_columns": [c for c in (enriched_payload.get("columns") or []) if c.get("is_primary_key")],
        "measure_columns": [c for c in (enriched_payload.get("columns") or []) if c.get("is_measure")],
        "gate3_approved": bool(gate3_payload or gate3_decision == "APPROVED"),
        "discovered_metadata": checkpoint.get("discovered_metadata") or {},
        "column_profiles": checkpoint.get("column_profiles") or {},
        "bronze_generation_completed": bronze_generation_completed,
        "silver_generation_completed": silver_generation_completed,
        "gold_generation_completed": gold_generation_completed,
        "bronze": bronze,
        "silver": silver,
        "gold": gold,
        "next_gate": next_gate,
        "resume_message": resume_message,
        "status": status,
        "pipeline_steps": pipeline_steps,
        "current_pipeline_step": current_pipeline_step,
        "candidate_feed": candidate_feed,
        "candidate_feeds": checkpoint.get("candidate_feeds") or [],
        "sftp_entity": checkpoint.get("sftp_entity") or "transactions",
        "source_row_count": checkpoint.get("source_row_count"),
        "source_columns": checkpoint.get("source_columns") or [],
        "display_name": build_sftp_display_name(checkpoint),
    }
