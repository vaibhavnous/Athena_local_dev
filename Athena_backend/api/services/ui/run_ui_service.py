from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from services.pipeline_runtime import (
    apply_waiting_stage_state,
    build_pipeline_steps,
    fetch_json_artifact,
    fetch_run_summary,
    generation_completed,
    load_checkpoint_fields,
    load_checkpoint_state,
)
from utilis.logger import logger

from api import utils as api_utils
from api.services.kpi_service import (
    artifact_kpis,
    fetch_hitl_rows,
    kpis_from_checkpoint,
    map_kpi,
    requirements_from_checkpoint,
)
from api.services.ui.shared import display_run_name, failed_stage_key, get_run_data, status_from_context
from api.services.ui.stage_ui_service import summary_stage_list, ui_stages


def _file_generation_flags(summary: List[Dict[str, Any]], checkpoint: Dict[str, Any]) -> Dict[str, bool]:
    artifact_types = {str(row.get("artifact_type") or "").upper() for row in summary if isinstance(row, dict)}
    return {
        "bronze_generation_completed": generation_completed(summary, checkpoint, "bronze"),
        "silver_generation_completed": generation_completed(summary, checkpoint, "silver"),
        "gold_generation_completed": generation_completed(summary, checkpoint, "gold"),
        "schema_discovery_completed": bool(
            "SFTP_SCHEMA_SNAPSHOT" in artifact_types
            or checkpoint.get("metadata_status") == "COMPLETED"
        ),
        "column_profiling_completed": bool(
            "SFTP_COLUMN_PROFILING" in artifact_types
            or checkpoint.get("column_profiling_status") == "COMPLETED"
        ),
    }


def _file_display_name(checkpoint: Dict[str, Any]) -> str:
    source = str(checkpoint.get("source") or "sftp").lower()
    prefix = "adls" if source == "adls_gen2" else "sftp"
    vendor = str(checkpoint.get("vendor") or "Vendor1")
    candidate_feeds = checkpoint.get("candidate_feeds") if isinstance(checkpoint.get("candidate_feeds"), list) else []
    discovered_entities = sorted(
        {
            str(feed.get("entity") or "").lower()
            for feed in candidate_feeds
            if isinstance(feed, dict) and str(feed.get("entity") or "").strip()
        }
    )
    if len(discovered_entities) > 1:
        return f"{prefix}:{vendor}:{'+'.join(discovered_entities)}"
    entity = str(checkpoint.get("sftp_entity") or "transactions").lower()
    if entity == "both":
        return f"{prefix}:{vendor}:employee+transactions"
    return f"{prefix}:{vendor}:{entity}"


def _file_next_gate_and_message(
    *,
    gate1_decision: Any,
    gate2_decision: Any,
    gate3_decision: str,
    gate4_decision: str,
    gate5_decision: str,
    feed_review_ready: bool,
    source_ingestion_completed: bool,
    semantic_enrichment_completed: bool,
    bronze_review_ready: bool,
    silver_review_ready: bool,
    gate3_payload: Dict[str, Any],
    column_profiling_completed: bool,
    schema_discovery_completed: bool,
) -> Dict[str, Any]:
    next_gate = None
    resume_message = None
    if gate1_decision in {None, ""}:
        next_gate = 1
        resume_message = "KPI Review is pending. Review KPI items before continuing."
    elif gate1_decision == "APPROVED" and gate2_decision in {None, ""}:
        if not source_ingestion_completed:
            resume_message = "Source ingestion is in progress. Feed review will open when source discovery completes."
        elif not feed_review_ready:
            resume_message = "Feed discovery is in progress. Feed review will open when discovery completes."
        else:
            next_gate = 2
            resume_message = "Feed Review is pending. Review discovered feeds before continuing."
    elif gate2_decision == "APPROVED":
        if semantic_enrichment_completed and gate3_decision not in {"APPROVED", "REJECTED"}:
            next_gate = 3
            resume_message = "Semantic Review is pending. Review semantic enrichment before continuing."
        elif (gate3_decision == "APPROVED" or gate3_payload) and bronze_review_ready and gate4_decision not in {"APPROVED", "REJECTED"}:
            next_gate = 4
            resume_message = "Bronze Review is pending. Review Bronze plan before ingestion."
        elif gate4_decision == "APPROVED" and silver_review_ready and gate5_decision not in {"APPROVED", "REJECTED"}:
            next_gate = 5
            resume_message = "Silver Review is pending. Review Silver plan before execution."
        elif gate5_decision == "APPROVED":
            resume_message = "Silver Review is complete."
        elif gate4_decision == "APPROVED":
            resume_message = "Bronze Review is complete."
        elif gate3_decision == "APPROVED" or gate3_payload:
            resume_message = "Semantic enrichment is approved."
        elif column_profiling_completed:
            resume_message = "Schema discovery and column profiling are complete."
        elif schema_discovery_completed:
            resume_message = "Schema discovery is complete. Column profiling is in progress."
        else:
            resume_message = "Feed Review is complete."
    elif gate1_decision == "REJECTED":
        resume_message = "KPI Review was rejected."
    elif gate2_decision == "REJECTED":
        resume_message = "Feed Review was rejected."
    elif gate3_decision == "REJECTED":
        resume_message = "Semantic Review was rejected."
    elif gate4_decision == "REJECTED":
        resume_message = "Bronze Review was rejected."
    elif gate5_decision == "REJECTED":
        resume_message = "Silver Review was rejected."
    return {"next_gate": next_gate, "resume_message": resume_message}


def _file_status(
    *,
    checkpoint: Dict[str, Any],
    next_gate: Optional[int],
    gate5_decision: str,
    gold_generation_completed: bool,
    silver_generation_completed: bool,
    gate1_decision: Any,
    gate2_decision: Any,
    source_ingestion_completed: bool,
    feed_review_ready: bool,
) -> str:
    status = checkpoint.get("status") or "UNKNOWN"
    if next_gate:
        return "HITL_WAIT"
    if checkpoint.get("background_stage"):
        return "RUNNING"
    if gate1_decision == "APPROVED" and gate2_decision in {None, ""} and (not source_ingestion_completed or not feed_review_ready):
        return "RUNNING"
    if gate5_decision == "APPROVED" or gold_generation_completed:
        return "PIPELINE_COMPLETED"
    if silver_generation_completed and gate5_decision not in {"APPROVED", "REJECTED"}:
        return "HITL_WAIT"
    return status


def _file_source_summary_context(run_id: str, checkpoint: Dict[str, Any], summary: List[Dict[str, Any]]) -> Dict[str, Any]:
    artifact_types = {
        str(row.get("artifact_type") or "").upper()
        for row in summary
        if isinstance(row, dict)
    }
    gate1_decision = (checkpoint.get("gate1") or {}).get("decision")
    gate2_decision = (checkpoint.get("gate2") or {}).get("decision")
    gate3_decision = str(checkpoint.get("enrichment_review_decision") or "").upper()
    gate4_decision = str((checkpoint.get("gate4") or {}).get("decision") or checkpoint.get("bronze_review_decision") or "").upper()
    gate5_decision = str((checkpoint.get("gate5") or {}).get("decision") or checkpoint.get("silver_review_decision") or "").upper()
    candidate_feed = checkpoint.get("candidate_feed") if isinstance(checkpoint.get("candidate_feed"), dict) else {}
    candidate_feeds = checkpoint.get("candidate_feeds") if isinstance(checkpoint.get("candidate_feeds"), list) else []
    source_ingestion_completed = checkpoint.get("source_ingestion_status") == "COMPLETED"
    feed_review_ready = bool(candidate_feeds) or bool(candidate_feed)

    generation_flags = _file_generation_flags(summary, checkpoint)
    semantic_enrichment_completed = bool(
        "ENRICHED_METADATA" in artifact_types
        or checkpoint.get("semantic_enrichment_status") == "COMPLETED"
        or checkpoint.get("enriched_metadata")
    )
    gate3_payload = (
        checkpoint.get("enrichment_review_artifact")
        if isinstance(checkpoint.get("enrichment_review_artifact"), dict)
        else {}
    )
    bronze_review_ready = bool(checkpoint.get("bronze_review_artifact") or checkpoint.get("bronze_generation_results"))
    silver_review_ready = bool(checkpoint.get("silver_review_artifact") or checkpoint.get("silver_generation_results"))

    gate_state = _file_next_gate_and_message(
        gate1_decision=gate1_decision,
        gate2_decision=gate2_decision,
        gate3_decision=gate3_decision,
        gate4_decision=gate4_decision,
        gate5_decision=gate5_decision,
        feed_review_ready=feed_review_ready,
        source_ingestion_completed=source_ingestion_completed,
        semantic_enrichment_completed=semantic_enrichment_completed,
        bronze_review_ready=bronze_review_ready,
        silver_review_ready=silver_review_ready,
        gate3_payload=gate3_payload,
        column_profiling_completed=generation_flags["column_profiling_completed"],
        schema_discovery_completed=generation_flags["schema_discovery_completed"],
    )
    next_gate = gate_state["next_gate"]

    status = _file_status(
        checkpoint=checkpoint,
        next_gate=next_gate,
        gate5_decision=gate5_decision,
        gold_generation_completed=generation_flags["gold_generation_completed"],
        silver_generation_completed=generation_flags["silver_generation_completed"],
        gate1_decision=gate1_decision,
        gate2_decision=gate2_decision,
        source_ingestion_completed=source_ingestion_completed,
        feed_review_ready=feed_review_ready,
    )

    pipeline_steps = build_pipeline_steps(
        source=str(checkpoint.get("source") or "sftp").lower(),
        checkpoint=checkpoint,
        summary=summary,
        pending_gate1=[],
        completed_gate1=[],
        nominated_tables=[],
        certified_tables=[],
        enriched_payload={"summary_only": True} if semantic_enrichment_completed else {},
        gate3_payload=gate3_payload,
        bronze_generation_completed=generation_flags["bronze_generation_completed"],
        silver_generation_completed=generation_flags["silver_generation_completed"],
        gold_generation_completed=generation_flags["gold_generation_completed"],
    )
    waiting_gate_key = f"gate{next_gate}" if next_gate in {1, 2, 3, 4, 5} else None
    pipeline_steps = apply_waiting_stage_state(pipeline_steps, waiting_gate_key)

    return {
        "checkpoint": checkpoint,
        "summary": summary,
        "status": status,
        "pipeline_steps": pipeline_steps,
        "next_gate": next_gate,
        "resume_message": gate_state["resume_message"],
        "stage_confirmation": None,
        "display_name": _file_display_name(checkpoint),
        "sftp_entity": checkpoint.get("sftp_entity") or "transactions",
        "source_row_count": checkpoint.get("source_row_count"),
        "source_columns": checkpoint.get("source_columns") if isinstance(checkpoint.get("source_columns"), list) else [],
        "bronze": {"generated_at": None, "scripts": []},
        "silver": {"generated_at": None, "scripts": []},
        "gold": {"generated_at": None, "scripts": []},
    }


def _summary_run_data(run_id: str) -> Tuple[Dict[str, Any], Dict[str, Any], List[Dict[str, Any]], Dict[str, Any]]:
    checkpoint = load_checkpoint_fields(run_id, "source")
    summary = fetch_run_summary(run_id)
    if api_utils.is_file_source(checkpoint.get("source")):
        full_checkpoint = load_checkpoint_state(run_id) or {}
        context = _file_source_summary_context(run_id, full_checkpoint, summary)
        return full_checkpoint, context, summary, full_checkpoint
    return get_run_data(run_id)


def build_kpis(run_id: str, checkpoint: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    source = str(checkpoint.get("source") or "database").lower()
    hitl_rows = fetch_hitl_rows(run_id)
    if hitl_rows:
        return hitl_rows, hitl_rows
    raw_kpis = artifact_kpis(run_id) or kpis_from_checkpoint(checkpoint)
    return [map_kpi(kpi, run_id=run_id, source=source) for kpi in raw_kpis], hitl_rows


def hitl_decisions(
    run_id: str,
    context: Dict[str, Any],
    hitl_rows: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    decisions: List[Dict[str, Any]] = []
    for row in hitl_rows or fetch_hitl_rows(run_id):
        if not row.get("decision"):
            continue
        decisions.append(
            {
                "id": row.get("id"),
                "gate": api_utils.gate_label(1),
                "type": "KPI",
                "name": row.get("name"),
                "definition": row.get("definition"),
                "decision": row.get("decision"),
                "rejection_reason": row.get("rejection_reason"),
                "reviewed_at": row.get("decided_at"),
            }
        )

    certified_tables = context.get("certified_tables") or []
    if certified_tables:
        decisions.append(
            {
                "id": f"{run_id}:gate2",
                "gate": api_utils.gate_label(2, source=str(context.get("checkpoint", {}).get("source") or "database")),
                "type": "Tables",
                "name": f"{len(certified_tables)} table(s) certified",
                "definition": ", ".join(
                    ".".join(str(table.get(part) or "") for part in ("database_name", "schema_name", "table_name")).strip(".")
                    for table in certified_tables[:5]
                ),
                "decision": "APPROVED",
                "reviewed_at": None,
            }
        )

    if context.get("gate3_approved"):
        decisions.append(
            {
                "id": f"{run_id}:gate3",
                "gate": api_utils.gate_label(3),
                "type": "Enrichment",
                "name": "Semantic enrichment approved",
                "definition": "Semantic tags, PII classifications, and join annotations approved.",
                "decision": "APPROVED",
                "reviewed_at": None,
            }
        )

    return decisions


def current_pipeline_step(context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    step = context.get("current_pipeline_step")
    if isinstance(step, dict) and step:
        return step
    for candidate in context.get("pipeline_steps") or []:
        if str(candidate.get("state") or candidate.get("status") or "").upper() in {"RUNNING", "HITL_WAIT", "PAUSED_FOR_HITL"}:
            return candidate
    return None


def build_ui_payload(
    *,
    run_id: str,
    context: Dict[str, Any],
    checkpoint: Dict[str, Any],
    summary: List[Dict[str, Any]],
    requirements: Dict[str, Any],
    kpis: List[Dict[str, Any]],
    hitl_rows: List[Dict[str, Any]],
    include_scripts: bool,
) -> Dict[str, Any]:
    status = status_from_context(context)
    pipeline_steps = context.get("pipeline_steps") or []
    run_failed_stage_key = failed_stage_key(checkpoint, pipeline_steps)
    failed_stage_label = api_utils.stage_label_from_key(run_failed_stage_key, checkpoint.get("source"))

    payload = {
        "id": run_id,
        "run_id": run_id,
        "brd_filename": display_run_name(checkpoint, context),
        "source": checkpoint.get("source") or "database",
        "status": status,
        "provider": checkpoint.get("provider") or "azure_openai",
        "deployment": checkpoint.get("deployment"),
        "started_at": summary[0].get("stored_at") if summary else None,
        "completed_at": summary[-1].get("stored_at") if status == "SUCCESS" and summary else None,
        "cache_hit": "L1_EXACT" if checkpoint.get("memory_layer1") else "L2_SEMANTIC" if checkpoint.get("memory_layer2") else "NONE",
        "cache_score": checkpoint.get("semantic_score") or 0,
        "extraction_path": checkpoint.get("extraction_path") or "ATHENA_GRAPH",
        "total_tokens": sum(int(row.get("token_count") or 0) for row in summary),
        "total_cost": sum(float(row.get("cost_usd") or 0) for row in summary),
        "stages": ui_stages(context, run_id),
        "pipeline_steps": pipeline_steps,
        "requirements": requirements,
        "kpis": kpis,
        "hitl_decisions": hitl_decisions(run_id, context, hitl_rows=hitl_rows),
        "nominated_tables": context.get("nominated_tables") or [],
        "certified_tables": context.get("certified_tables") or [],
        "enriched_metadata": context.get("enriched_metadata") or {},
        "enriched_columns": context.get("enriched_columns") or [],
        "enriched_joins": context.get("enriched_joins") or [],
        "semantic_counts": context.get("semantic_counts") or {},
        "pii_columns": context.get("pii_columns") or [],
        "join_key_columns": context.get("join_key_columns") or [],
        "measure_columns": context.get("measure_columns") or [],
        "feed_semantic_summary": context.get("feed_semantic_summary") or [],
        "gate3_approved": context.get("gate3_approved") or False,
        "next_gate": context.get("next_gate"),
        "next_review_key": context.get("next_review_key"),
        "resume_message": context.get("resume_message"),
        "stage_confirmation": context.get("stage_confirmation"),
        "current_pipeline_step": current_pipeline_step(context),
        "external_execution": context.get("external_execution"),
        "failed_stage_key": run_failed_stage_key,
        "failed_stage_label": failed_stage_label,
        "error": checkpoint.get("error"),
        "updated_at": summary[-1].get("stored_at") if summary else None,
        "databricks_run_id": run_id,
        "sftp_entity": context.get("sftp_entity") or checkpoint.get("sftp_entity"),
        "candidate_feed": (context.get("candidate_feed") or checkpoint.get("candidate_feed")) if api_utils.is_file_source(checkpoint.get("source")) else None,
        "candidate_feeds": (context.get("candidate_feeds") or checkpoint.get("candidate_feeds") or []) if api_utils.is_file_source(checkpoint.get("source")) else [],
        "source_row_count": context.get("source_row_count") or checkpoint.get("source_row_count"),
        "source_columns": context.get("source_columns") or checkpoint.get("source_columns") or [],
        "compliance_enabled": bool(checkpoint.get("compliance_enabled")),
        "compliance_assessment_id": checkpoint.get("compliance_assessment_id"),
        "compliance_assessment_status": checkpoint.get("compliance_assessment_status"),
        "compliance_assessment_error": checkpoint.get("compliance_assessment_error"),
        "compliance_review_status": checkpoint.get("compliance_review_status"),
        "compliance_review": checkpoint.get("compliance_review") or {},
        "compliance_review_error": checkpoint.get("compliance_review_error"),
        "compliance_results": checkpoint.get("compliance_results") or {},
    }
    if include_scripts:
        payload.update(
            {
                "bronze": context.get("bronze") or {"generated_at": None, "scripts": []},
                "silver": context.get("silver") or {"generated_at": None, "scripts": []},
                "gold": context.get("gold") or {"generated_at": None, "scripts": []},
                "bronze_generation_completed": context.get("bronze_generation_completed") or False,
                "silver_generation_completed": context.get("silver_generation_completed") or False,
                "gold_generation_completed": context.get("gold_generation_completed") or False,
            }
        )
    else:
        payload["script_counts"] = {
            "bronze": len((context.get("bronze") or {}).get("scripts") or []),
            "silver": len((context.get("silver") or {}).get("scripts") or []),
            "gold": len((context.get("gold") or {}).get("scripts") or []),
        }
    return payload


def ui_run_summary(run_id: str) -> Dict[str, Any]:
    _, context, summary, checkpoint = _summary_run_data(run_id)
    status = status_from_context(context)
    run_failed_stage_key = failed_stage_key(checkpoint, context.get("pipeline_steps") or [])
    failed_stage_label = api_utils.stage_label_from_key(run_failed_stage_key, checkpoint.get("source"))

    return {
        "id": run_id,
        "run_id": run_id,
        "brd_filename": display_run_name(checkpoint, context),
        "source": checkpoint.get("source") or "database",
        "status": status,
        "provider": checkpoint.get("provider") or "azure_openai",
        "deployment": checkpoint.get("deployment"),
        "started_at": summary[0].get("stored_at") if summary else None,
        "completed_at": summary[-1].get("stored_at") if status == "SUCCESS" and summary else None,
        "cache_hit": "L1_EXACT" if checkpoint.get("memory_layer1") else "L2_SEMANTIC" if checkpoint.get("memory_layer2") else "NONE",
        "cache_score": checkpoint.get("semantic_score") or 0,
        "extraction_path": checkpoint.get("extraction_path") or checkpoint.get("kpi_source") or "ATHENA_GRAPH",
        "total_tokens": sum(int(row.get("token_count") or 0) for row in summary),
        "total_cost": sum(float(row.get("cost_usd") or 0) for row in summary),
        "stages": summary_stage_list(
            checkpoint=checkpoint,
            summary=summary,
            pipeline_steps=context.get("pipeline_steps") or [],
        ),
        "pipeline_steps": context.get("pipeline_steps") or [],
        "next_gate": context.get("next_gate"),
        "next_review_key": context.get("next_review_key"),
        "resume_message": context.get("resume_message"),
        "stage_confirmation": context.get("stage_confirmation"),
        "current_pipeline_step": current_pipeline_step(context),
        "external_execution": context.get("external_execution"),
        "failed_stage_key": run_failed_stage_key,
        "failed_stage_label": failed_stage_label,
        "error": checkpoint.get("error"),
        "updated_at": checkpoint.get("checkpoint_at") or checkpoint.get("updated_at") or summary[-1].get("stored_at") if summary else None,
        "script_counts": {
            "bronze": len((context.get("bronze") or {}).get("scripts") or []),
            "silver": len((context.get("silver") or {}).get("scripts") or []),
            "gold": len((context.get("gold") or {}).get("scripts") or []),
        },
        "sftp_entity": context.get("sftp_entity"),
        "source_row_count": context.get("source_row_count"),
        "source_columns": context.get("source_columns") or [],
        "compliance_enabled": bool(checkpoint.get("compliance_enabled")),
        "compliance_assessment_id": checkpoint.get("compliance_assessment_id"),
        "compliance_assessment_status": checkpoint.get("compliance_assessment_status"),
        "compliance_review_status": checkpoint.get("compliance_review_status"),
    }


def ui_run(run_id: str, *, include_scripts: bool = False) -> Dict[str, Any]:
    logger.debug("Building UI payload run_id=%s include_scripts=%s", run_id, include_scripts)
    _, context, summary, checkpoint = get_run_data(run_id)
    requirements = fetch_json_artifact(run_id, "REQUIREMENTS") or requirements_from_checkpoint(checkpoint)
    kpis, hitl_rows = build_kpis(run_id, checkpoint)
    return build_ui_payload(
        run_id=run_id,
        context=context,
        checkpoint=checkpoint,
        summary=summary,
        requirements=requirements,
        kpis=kpis,
        hitl_rows=hitl_rows,
        include_scripts=include_scripts,
    )
