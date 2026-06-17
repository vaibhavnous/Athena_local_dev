from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from services.pipeline_runtime import fetch_json_artifact
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
        "resume_message": context.get("resume_message"),
        "stage_confirmation": context.get("stage_confirmation"),
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
    _, context, summary, checkpoint = get_run_data(run_id)
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
        "next_gate": context.get("next_gate"),
        "resume_message": context.get("resume_message"),
        "stage_confirmation": context.get("stage_confirmation"),
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
