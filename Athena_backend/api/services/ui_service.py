from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from services.pipeline_runtime import (
    fetch_json_artifact,
    get_run_context,
    load_bronze_scripts,
    load_checkpoint_state,
    load_silver_scripts,
)
from services.sftp_runtime import build_sftp_display_name, get_sftp_run_context

from api import utils as api_utils
from api.services.kpi_service import artifact_kpis, fetch_hitl_rows, kpis_from_checkpoint, map_kpi, requirements_from_checkpoint
from api.services.log_service import read_logs


def status_from_context(context: Dict[str, Any]) -> str:
    checkpoint = context.get("checkpoint") or {}
    if str(checkpoint.get("status") or "").upper() == "PAUSED_FOR_STAGE_CONFIRMATION":
        return "PAUSED_FOR_STAGE_CONFIRMATION"
    if context.get("pending_gate1") or context.get("next_gate") in {1, 2, 3, 4, 5}:
        return "HITL_WAIT"
    if checkpoint.get("background_stage"):
        return "RUNNING"
    status = str(context.get("status") or "UNKNOWN")
    if status in {"UNKNOWN", "NOT_FOUND"}:
        return "NOT_FOUND"
    if status in {"RUNNING", "PROCESSING", "PENDING"}:
        return "RUNNING"
    if status == "HITL_WAIT":
        return "HITL_WAIT"
    if status == "ABORTED":
        return "ABORTED"
    if status in {"PIPELINE_COMPLETED", "COMPLETED"}:
        return "SUCCESS"
    if status == "FAILED":
        return "FAILED"
    return status


def display_run_name(checkpoint: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> str:
    if api_utils.is_file_source(checkpoint.get("source")):
        return (context or {}).get("display_name") or build_sftp_display_name(checkpoint)
    return checkpoint.get("brd_filename") or "athena_brd.txt"


def bronze_review_from_scripts(run_id: str, checkpoint: Dict[str, Any]) -> Dict[str, Any]:
    bundle = load_bronze_scripts(run_id, checkpoint)
    scripts = bundle.get("scripts") or []
    if not scripts:
        return {}
    feeds = []
    for item in scripts:
        config_payload = item.get("bronze_config") or item.get("generated_bronze_config") or {}
        feeds.append(
            {
                "feed_summary": item.get("feed_summary") or f"{item.get('vendor') or 'Vendor'}.{item.get('entity') or 'Feed'}",
                "source_type": item.get("source_type") or config_payload.get("source_type") or checkpoint.get("source"),
                "vendor": item.get("vendor") or config_payload.get("vendor"),
                "entity": item.get("entity") or config_payload.get("entity"),
                "file_format": item.get("file_format") or config_payload.get("file_format"),
                "approved_schema": config_payload.get("schema_columns") or item.get("approved_schema") or [],
                "primary_keys": item.get("primary_keys") or config_payload.get("primary_keys") or [],
                "watermark_column": item.get("watermark_column") or config_payload.get("watermark_column"),
                "landing_path": item.get("landing_path") or config_payload.get("landing_path"),
                "target_table": item.get("target_table") or config_payload.get("target_table"),
                "bronze_output_path": item.get("bronze_output_path") or config_payload.get("bronze_output_path"),
                "checkpoint_path": item.get("checkpoint_path") or config_payload.get("checkpoint_path"),
                "schema_location": item.get("schema_location") or config_payload.get("schema_location"),
                "generated_bronze_config": item.get("generated_bronze_config") or config_payload,
                "generated_bronze_script": item.get("generated_bronze_script") or item.get("script_body") or "",
                "validation_checklist": item.get("validation_checklist") or [],
                "validation_issues": item.get("validation_issues") or [],
                "plan_valid": item.get("plan_valid", item.get("status") == "COMPLETED"),
                "review_status": item.get("review_status") or "PENDING",
            }
        )
    return {
        "run_id": run_id,
        "generated_at": bundle.get("generated_at") or checkpoint.get("bronze_generated_at"),
        "feeds": feeds,
    }


def silver_review_from_scripts(run_id: str, checkpoint: Dict[str, Any]) -> Dict[str, Any]:
    bundle = load_silver_scripts(run_id, checkpoint)
    scripts = bundle.get("scripts") or []
    if not scripts:
        return {}
    items = []
    for item in scripts:
        primary_keys = item.get("primary_keys") or []
        items.append(
            {
                "entity": item.get("entity") or item.get("table") or "Silver Item",
                "vendor": item.get("vendor"),
                "bronze_source": item.get("bronze_table") or item.get("source_table"),
                "silver_target": item.get("silver_table") or item.get("target_table"),
                "primary_keys": primary_keys,
                "watermark_column": item.get("watermark_column"),
                "transformations": [
                    "column rename (bronze -> business names)",
                    "type casting",
                    "deduplication",
                    "null audit",
                    "silver audit columns",
                ],
                "pii_masking_rules": item.get("pii_masking_rules") or [],
                "merge_strategy": "MERGE upsert" if primary_keys else "overwrite",
                "llm_enhanced": item.get("llm_enhanced", False),
                "generated_silver_script": item.get("generated_silver_script") or item.get("script_body") or "",
            }
        )
    return {
        "run_id": run_id,
        "generated_at": bundle.get("generated_at") or checkpoint.get("silver_generated_at"),
        "items": items,
    }


def ui_stages(context: Dict[str, Any], run_id: str) -> List[Dict[str, Any]]:
    summary = context.get("summary") or []
    metrics: Dict[str, Dict[str, Any]] = {}

    for row in summary:
        key = api_utils.stage_key(row.get("stage")) or api_utils.stage_key(row.get("artifact_type"))
        if not key:
            continue
        bucket = metrics.setdefault(
            key,
            {
                "tokens": 0,
                "cost": 0.0,
                "attempts": 0,
                "started_at": None,
                "completed_at": None,
                "prompt_metadata": {"artifacts": []},
            },
        )
        bucket["tokens"] += int(row.get("token_count") or 0)
        bucket["cost"] += float(row.get("cost_usd") or 0)
        bucket["attempts"] += int(row.get("retry_count") or 0)
        stored_at = api_utils.iso_or_none(row.get("stored_at"))
        if stored_at and (not bucket["started_at"] or stored_at < bucket["started_at"]):
            bucket["started_at"] = stored_at
        if stored_at and (not bucket["completed_at"] or stored_at > bucket["completed_at"]):
            bucket["completed_at"] = stored_at
        bucket["prompt_metadata"]["artifacts"].append(
            {
                "stage": row.get("stage"),
                "artifact_type": row.get("artifact_type"),
                "faithfulness_status": row.get("faithfulness_status"),
                "retry_count": row.get("retry_count"),
                "input_tokens": row.get("input_tokens"),
                "output_tokens": row.get("output_tokens"),
                "token_count": row.get("token_count"),
                "cost_usd": row.get("cost_usd"),
                "stored_at": stored_at,
            }
        )

    for log in read_logs(run_id, limit=5000):
        key = api_utils.stage_key(log.get("stage")) or api_utils.stage_key(log.get("message"))
        if not key:
            continue
        bucket = metrics.setdefault(
            key,
            {
                "tokens": 0,
                "cost": 0.0,
                "attempts": 0,
                "started_at": None,
                "completed_at": None,
                "prompt_metadata": {"artifacts": []},
            },
        )
        logged_at = log.get("logged_at")
        if logged_at and (not bucket["started_at"] or logged_at < bucket["started_at"]):
            bucket["started_at"] = logged_at
        if logged_at and (not bucket["completed_at"] or logged_at > bucket["completed_at"]):
            bucket["completed_at"] = logged_at
        if log.get("event_type") == "stage_end" and log.get("duration_seconds") is not None:
            bucket["duration_seconds"] = max(
                float(bucket.get("duration_seconds") or 0),
                float(log.get("duration_seconds") or 0),
            )

    stages = []
    for index, step in enumerate(context.get("pipeline_steps", [])):
        key = step["key"]
        bucket = metrics.get(key, {})
        raw_state = str(step.get("state") or "").upper()
        duration_seconds = bucket.get("duration_seconds")
        if duration_seconds is None and bucket.get("started_at") and bucket.get("completed_at"):
            try:
                start = datetime.fromisoformat(str(bucket["started_at"]).replace("Z", "+00:00"))
                end = datetime.fromisoformat(str(bucket["completed_at"]).replace("Z", "+00:00"))
                duration_seconds = max(0.0, (end - start).total_seconds())
            except Exception:
                duration_seconds = None

        stages.append(
            {
                "id": f"stage_{index + 1:02d}",
                "key": key,
                "name": step["label"],
                "status": (
                    "COMPLETED"
                    if raw_state in {"COMPLETE", "COMPLETED"}
                    else "FAILED"
                    if raw_state == "FAILED"
                    else "HITL_WAIT"
                    if raw_state in {"HITL_WAIT", "PAUSED_FOR_HITL"}
                    else "RUNNING"
                    if raw_state in {"RUNNING", "IN_PROGRESS"}
                    else "PENDING"
                ),
                "tokens": bucket.get("tokens", 0),
                "cost": bucket.get("cost", 0.0),
                "attempts": bucket.get("attempts", 0),
                "duration_seconds": duration_seconds,
                "started_at": bucket.get("started_at"),
                "completed_at": bucket.get("completed_at"),
                "error": context.get("checkpoint", {}).get("error") if raw_state == "FAILED" else None,
                "prompt_metadata": bucket.get("prompt_metadata") if bucket.get("prompt_metadata", {}).get("artifacts") else None,
            }
        )

    return stages


def stage_metrics_from_summary(summary: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    metrics: Dict[str, Dict[str, Any]] = {}
    for row in summary:
        key = api_utils.stage_key(row.get("stage")) or api_utils.stage_key(row.get("artifact_type"))
        if not key:
            continue
        bucket = metrics.setdefault(
            key,
            {"tokens": 0, "cost": 0.0, "attempts": 0, "started_at": None, "completed_at": None},
        )
        bucket["tokens"] += int(row.get("token_count") or 0)
        bucket["cost"] += float(row.get("cost_usd") or 0)
        bucket["attempts"] += int(row.get("retry_count") or 0)
        stored_at = api_utils.iso_or_none(row.get("stored_at"))
        if stored_at and (not bucket["started_at"] or stored_at < bucket["started_at"]):
            bucket["started_at"] = stored_at
        if stored_at and (not bucket["completed_at"] or stored_at > bucket["completed_at"]):
            bucket["completed_at"] = stored_at
    return metrics


def summary_stage_list(*, checkpoint: Dict[str, Any], summary: List[Dict[str, Any]], pipeline_steps: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    metrics = stage_metrics_from_summary(summary)
    stages: List[Dict[str, Any]] = []

    for index, step in enumerate(pipeline_steps):
        key = step["key"]
        bucket = metrics.get(key, {})
        duration_seconds = None
        if bucket.get("started_at") and bucket.get("completed_at"):
            try:
                start = datetime.fromisoformat(str(bucket["started_at"]).replace("Z", "+00:00"))
                end = datetime.fromisoformat(str(bucket["completed_at"]).replace("Z", "+00:00"))
                duration_seconds = max(0.0, (end - start).total_seconds())
            except Exception:
                duration_seconds = None

        raw_state = str(step.get("state") or "").upper()
        stages.append(
            {
                "id": f"stage_{index + 1:02d}",
                "key": key,
                "name": step["label"],
                "status": (
                    "COMPLETED"
                    if raw_state in {"COMPLETE", "COMPLETED"}
                    else "FAILED"
                    if raw_state == "FAILED"
                    else "HITL_WAIT"
                    if raw_state in {"HITL_WAIT", "PAUSED_FOR_HITL"}
                    else "RUNNING"
                    if raw_state in {"RUNNING", "IN_PROGRESS"}
                    else "PENDING"
                ),
                "tokens": bucket.get("tokens", 0),
                "cost": bucket.get("cost", 0.0),
                "attempts": bucket.get("attempts", 0),
                "duration_seconds": duration_seconds,
                "started_at": bucket.get("started_at"),
                "completed_at": bucket.get("completed_at"),
                "error": checkpoint.get("error") if raw_state == "FAILED" else None,
                "prompt_metadata": None,
            }
        )

    return stages


def ui_run_summary(run_id: str) -> Dict[str, Any]:
    checkpoint_hint = load_checkpoint_state(run_id) or {}
    context = get_sftp_run_context(run_id) if api_utils.is_file_source(checkpoint_hint.get("source")) else get_run_context(run_id)
    summary = context.get("summary") or []
    checkpoint = context.get("checkpoint") or checkpoint_hint
    status = status_from_context(context)
    failed_stage_key = (
        checkpoint.get("failed_background_stage")
        or api_utils.stage_key(checkpoint.get("background_stage"))
        or api_utils.stage_key(checkpoint.get("last_completed_stage_key"))
    )
    failed_stage_label = api_utils.stage_label_from_key(failed_stage_key, checkpoint.get("source"))

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
        "failed_stage_key": failed_stage_key,
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


def hitl_decisions(run_id: str, context: Dict[str, Any]) -> List[Dict[str, Any]]:
    decisions: List[Dict[str, Any]] = []
    for row in fetch_hitl_rows(run_id):
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


def ui_run(run_id: str, *, include_scripts: bool = False) -> Dict[str, Any]:
    checkpoint_hint = load_checkpoint_state(run_id) or {}
    context = get_sftp_run_context(run_id) if api_utils.is_file_source(checkpoint_hint.get("source")) else get_run_context(run_id)
    summary = context.get("summary") or []
    checkpoint = context.get("checkpoint") or {}
    requirements = fetch_json_artifact(run_id, "REQUIREMENTS") or requirements_from_checkpoint(checkpoint)
    raw_kpis = artifact_kpis(run_id) or kpis_from_checkpoint(checkpoint)
    hitl_rows = fetch_hitl_rows(run_id)
    kpis = hitl_rows or [map_kpi(kpi, run_id=run_id) for kpi in raw_kpis]
    status = status_from_context(context)
    failed_stage_key = (
        checkpoint.get("failed_background_stage")
        or api_utils.stage_key(checkpoint.get("background_stage"))
        or next(
            (step.get("key") for step in (context.get("pipeline_steps") or []) if str(step.get("state") or "").upper() == "FAILED"),
            None,
        )
    )
    failed_stage_label = api_utils.stage_label_from_key(failed_stage_key, checkpoint.get("source"))
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
        "hitl_decisions": hitl_decisions(run_id, context),
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
        "failed_stage_key": failed_stage_key,
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
