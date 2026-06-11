from __future__ import annotations

import json
import os
import re
import sys
import uuid
from concurrent.futures import TimeoutError as FutureTimeoutError
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.requests import Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from utilis.db import config, get_connection, get_pending_items, update_hitl_item
from utilis.logger import PIPELINE_LOG_PATH, logger
from services.pipeline_runtime import (
    BACKGROUND_EXECUTOR,
    BACKGROUND_JOBS,
    BACKGROUND_JOB_LOCK,
    build_pipeline_steps,
    continue_database_pipeline,
    fetch_json_artifact,
    fetch_run_summary,
    get_run_context,
    list_runs,
    load_bronze_scripts,
    load_checkpoint_state,
    load_silver_scripts,
    save_checkpoint_state,
    start_pipeline,
    submit_background,
    submit_gate1_review,
    submit_gate2_review,
    submit_gate3_review,
)
from services.sftp_runtime import get_sftp_run_context, start_sftp_pipeline
from services.sftp_runtime import build_sftp_display_name
from sftp_nodes.hitl import (
    submit_sftp_gate1_review,
    submit_sftp_gate2_review,
    submit_sftp_gate3_review,
    submit_sftp_gate4_review,
    submit_sftp_gate5_review,
)


app = FastAPI(title="Athena Backend API", version="1.0.0")

allowed_origins = [
    origin.strip()
    for origin in os.getenv(
        "ATHENA_CORS_ORIGINS",
        "http://localhost:3000,http://127.0.0.1:3000,http://localhost:3001,http://127.0.0.1:3001",
    ).split(",")
    if origin.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.exception("Unhandled API error on %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=500,
        content={
            "message": "Athena API failed while handling the request.",
            "detail": str(exc),
        },
    )


@app.exception_handler(HTTPException)
async def athena_http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    detail = exc.detail
    message = detail if isinstance(detail, str) else "Athena API request failed."
    return JSONResponse(
        status_code=exc.status_code,
        headers=getattr(exc, "headers", None),
        content={"message": message, "detail": detail},
    )


class PipelineRunRequest(BaseModel):
    brd_text: str = Field(default="")
    brd_filename: Optional[str] = None
    source: Optional[str] = "database"
    provider: Optional[str] = "azure_openai"
    deployment: Optional[str] = None
    budget: Optional[float] = None
    maxKpis: Optional[int] = None
    devMode: Optional[bool] = None
    database_name: Optional[str] = None
    database_type: Optional[str] = None
    source_databases: Optional[List[str]] = None
    sftp_entity: Optional[str] = "transactions"
    stage_confirmation_enabled: Optional[bool] = True


class StageContinueRequest(BaseModel):
    auto_advance: Optional[bool] = False


class HitlDecision(BaseModel):
    kpi_id: str
    decision: str
    reviewer: Optional[str] = None
    notes: Optional[str] = None
    edited_definition: Optional[str] = None


class HitlDecisionPayload(BaseModel):
    decisions: List[HitlDecision]


class Gate2DecisionPayload(BaseModel):
    approved_tables: List[str] = Field(default_factory=list)


class Gate3DecisionPayload(BaseModel):
    approve: bool = True


class GenericGateDecisionPayload(BaseModel):
    action: str = "APPROVED"


def _pipeline_schema() -> str:
    return (
        config["azure_sql"].get("pipeline_schema")
        or config["azure_sql"].get("schema_name")
        or "metadata"
    )


def _gate_label(gate: int, *, source: str = "database") -> str:
    if gate == 1:
        return "KPI Review"
    if gate == 2:
        return "Feed Review" if str(source or "").lower() in {"sftp", "adls_gen2"} else "Table Review"
    if gate == 3:
        return "Enrichment Review"
    if gate == 4:
        return "Bronze Review"
    if gate == 5:
        return "Silver Review"
    return f"Gate {gate}"


def _is_file_source(source: Optional[str]) -> bool:
    return str(source or "").lower() in {"sftp", "adls_gen2"}


def _json_loads(value: Any) -> Any:
    if not value:
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return value


def _run_pipeline_background(
    *,
    run_id: str,
    brd_text: str,
    source: Optional[str],
    source_databases: Optional[List[str]],
    sftp_entity: Optional[str],
    stage_confirmation_enabled: bool,
) -> None:
    try:
        existing_checkpoint = load_checkpoint_state(run_id) or {"run_id": run_id}
        if _is_file_source(source):
            result = start_sftp_pipeline(
                run_id=run_id,
                brd_text=brd_text,
                sftp_entity=sftp_entity,
                source=str(source or "sftp").lower(),
            )
        else:
            result = start_pipeline(
                brd_text=brd_text,
                source=source,
                source_databases=source_databases,
                sftp_entity=sftp_entity,
                run_id=run_id,
                stage_confirmation_enabled=stage_confirmation_enabled,
            )
        state = result.get("result") if isinstance(result, dict) else {}
        if isinstance(state, dict):
            pending_gate1 = get_pending_items(run_id, 1)
            if _is_file_source(state.get("source") or source):
                state["status"] = state.get("status", "COMPLETED")
            else:
                if state.get("status") not in {"PAUSED_FOR_STAGE_CONFIRMATION", "FAILED"}:
                    state["status"] = "HITL_WAIT" if pending_gate1 else state.get("status", "COMPLETED")
            save_checkpoint_state(run_id, {**existing_checkpoint, **state, "run_id": run_id})
    except Exception as exc:
        checkpoint = load_checkpoint_state(run_id) or {"run_id": run_id}
        checkpoint.update({"status": "FAILED", "error": str(exc)})
        save_checkpoint_state(run_id, checkpoint)
        raise


def _submit_pipeline_start(run_id: str, payload: PipelineRunRequest) -> None:
    job_key = f"{run_id}:pipeline"
    with BACKGROUND_JOB_LOCK:
        future = BACKGROUND_EXECUTOR.submit(
            _run_pipeline_background,
            run_id=run_id,
            brd_text=payload.brd_text,
            source=payload.source,
            source_databases=payload.source_databases
            or ([payload.database_name] if payload.database_name else None),
            sftp_entity=payload.sftp_entity,
            stage_confirmation_enabled=bool(payload.stage_confirmation_enabled if payload.stage_confirmation_enabled is not None else True),
        )
        BACKGROUND_JOBS[job_key] = future

    def _cleanup(done) -> None:
        with BACKGROUND_JOB_LOCK:
            if BACKGROUND_JOBS.get(job_key) is done:
                BACKGROUND_JOBS.pop(job_key, None)

    future.add_done_callback(_cleanup)


def _artifact_kpis(run_id: str) -> List[Dict[str, Any]]:
    payload = fetch_json_artifact(run_id, "KPIS")
    kpis = payload.get("kpis") or payload.get("items") or payload.get("extracted_kpis") or []
    if isinstance(kpis, list):
        return kpis
    return []


def _requirements_from_checkpoint(checkpoint: Dict[str, Any]) -> Dict[str, Any]:
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


def _kpis_from_checkpoint(checkpoint: Dict[str, Any]) -> List[Dict[str, Any]]:
    for key in ("kpis", "prior_kpis", "extracted_kpis", "certified_kpis"):
        value = checkpoint.get(key) or []
        if isinstance(value, list) and value:
            return value
    return []


def _map_kpi(kpi: Dict[str, Any], *, run_id: str, item_id: Optional[str] = None, status: str = "PENDING") -> Dict[str, Any]:
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
    }


def _fetch_hitl_rows(run_id: str, status: Optional[str] = None) -> List[Dict[str, Any]]:
    conn = get_connection()
    try:
        cursor = conn.cursor()
        status_clause = ""
        params: List[Any] = [run_id, 1]
        if status:
            status_clause = " AND gate_status = ?"
            params.append(status)
        cursor.execute(
            f"""
            SELECT item_id, gate_status, original_content, edited_content,
                   rejection_reason, queued_at, decided_at
            FROM [{_pipeline_schema()}].[hitl_review_queue]
            WHERE run_id = ? AND gate_number = ?{status_clause}
            ORDER BY queued_at
            """,
            params,
        )
        rows = cursor.fetchall()
    finally:
        conn.close()

    mapped = []
    for row in rows:
        content = _json_loads(row.edited_content) or _json_loads(row.original_content) or {}
        item = _map_kpi(content, run_id=run_id, item_id=row.item_id, status=row.gate_status)
        item.update(
            {
                "rejection_reason": row.rejection_reason,
                "queued_at": row.queued_at,
                "decided_at": row.decided_at,
            }
        )
        mapped.append(item)
    return mapped


def _status_from_context(context: Dict[str, Any]) -> str:
    checkpoint = context.get("checkpoint") or {}
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


def _display_run_name(checkpoint: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> str:
    if _is_file_source(checkpoint.get("source")):
        return (context or {}).get("display_name") or build_sftp_display_name(checkpoint)
    return checkpoint.get("brd_filename") or "athena_brd.txt"


def _bronze_review_from_scripts(run_id: str, checkpoint: Dict[str, Any]) -> Dict[str, Any]:
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


def _silver_review_from_scripts(run_id: str, checkpoint: Dict[str, Any]) -> Dict[str, Any]:
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


def _iso_or_none(value: Any) -> Optional[str]:
    if not value:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _stage_key(value: Any) -> Optional[str]:
    text = str(value or "").lower().replace("_", " ")
    if not text:
        return None
    if "feed discovery" in text or "candidate feed" in text:
        return "discovery"
    if "source ingestion" in text or "sftp source" in text:
        return "ingestion"
    if "ingestion" in text:
        return "ingestion"
    if "memory" in text:
        return "memory"
    if "requirement" in text or "req extract" in text:
        return "requirements"
    if "gate1" in text or "gate 1" in text or text == "hitl certification":
        return "gate1"
    if "kpi" in text and "hitl" not in text:
        return "kpis"
    if "nomination" in text or "table nomination" in text:
        return "nomination"
    if "gate2" in text or "gate 2" in text or "hitl table" in text:
        return "gate2"
    if "schema snapshot" in text or "sftp metadata discovery" in text:
        return "schema"
    if "metadata discovery" in text:
        return "discovery"
    if "column profiling" in text:
        return "profiling"
    if "semantic enrichment" in text:
        return "enrichment"
    if "gate3" in text or "gate 3" in text or "enrichment certification" in text:
        return "gate3"
    if "pre-bronze" in text or "bronze readiness" in text:
        return "pre_bronze"
    if "gate4" in text or "gate 4" in text or "bronze review" in text:
        return "gate4"
    if "sftp pull" in text:
        return "pull"
    if "bronze validation" in text:
        return "bronze_validation"
    if "bronze" in text:
        return "bronze"
    if "gate5" in text or "gate 5" in text or "silver review" in text:
        return "gate5"
    if "dq validation" in text:
        return "dq_validation"
    if "silver" in text:
        return "silver"
    if "gold" in text:
        return "gold"
    return None


def _ui_stages(context: Dict[str, Any], run_id: str) -> List[Dict[str, Any]]:
    summary = context.get("summary") or []
    metrics: Dict[str, Dict[str, Any]] = {}

    for row in summary:
        key = _stage_key(row.get("stage")) or _stage_key(row.get("artifact_type"))
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
        stored_at = _iso_or_none(row.get("stored_at"))
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

    for log in _read_logs(run_id, limit=5000):
        key = _stage_key(log.get("stage")) or _stage_key(log.get("message"))
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


def _stage_metrics_from_summary(summary: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    metrics: Dict[str, Dict[str, Any]] = {}

    for row in summary:
        key = _stage_key(row.get("stage")) or _stage_key(row.get("artifact_type"))
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
            },
        )
        bucket["tokens"] += int(row.get("token_count") or 0)
        bucket["cost"] += float(row.get("cost_usd") or 0)
        bucket["attempts"] += int(row.get("retry_count") or 0)
        stored_at = _iso_or_none(row.get("stored_at"))
        if stored_at and (not bucket["started_at"] or stored_at < bucket["started_at"]):
            bucket["started_at"] = stored_at
        if stored_at and (not bucket["completed_at"] or stored_at > bucket["completed_at"]):
            bucket["completed_at"] = stored_at

    return metrics


def _checkpoint_enriched_payload(checkpoint: Dict[str, Any]) -> Dict[str, Any]:
    payload = checkpoint.get("enriched_metadata") or checkpoint.get("enrichment_review_artifact") or {}
    if isinstance(payload, dict) and isinstance(payload.get("enrichment_artifact"), dict):
        return payload.get("enrichment_artifact") or {}
    return payload if isinstance(payload, dict) else {}


def _summary_stage_list(
    *,
    checkpoint: Dict[str, Any],
    summary: List[Dict[str, Any]],
    pipeline_steps: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    metrics = _stage_metrics_from_summary(summary)
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


def _summary_next_gate(
    *,
    checkpoint: Dict[str, Any],
    nominated_tables: List[Dict[str, Any]],
    certified_tables: List[Dict[str, Any]],
    enriched_payload: Dict[str, Any],
    gate3_approved: bool,
) -> Optional[int]:
    downstream_progress_exists = bool(
        nominated_tables
        or certified_tables
        or enriched_payload
        or gate3_approved
        or checkpoint.get("human_table_decision") == "COMPLETED"
        or checkpoint.get("enrichment_review_status") in {"COMPLETED", "PENDING"}
    )
    if nominated_tables and not certified_tables:
        return 2
    if enriched_payload and not gate3_approved:
        return 3
    checkpoint_status = str(checkpoint.get("status") or "").upper()
    if checkpoint_status == "HITL_WAIT" and not downstream_progress_exists:
        return 1
    return None


def _summary_resume_message(
    *,
    next_gate: Optional[int],
    checkpoint: Dict[str, Any],
    nominated_tables: List[Dict[str, Any]],
    certified_tables: List[Dict[str, Any]],
    enriched_payload: Dict[str, Any],
    gate3_approved: bool,
    summary: List[Dict[str, Any]],
) -> Optional[str]:
    if next_gate == 1:
        return "KPI Review is pending. Review the KPI items below."
    if next_gate == 2:
        return "Table Review is pending. Review and certify nominated tables below."
    if next_gate == 3:
        return "Enrichment Review is pending. Review enrichment details below."
    if next_gate == 4:
        return "Bronze Review is pending. Review Bronze plan before ingestion."
    if next_gate == 5:
        return "Silver Review is pending. Review Silver plan before execution."
    if gate3_approved:
        return "Enrichment Review is complete."
    if certified_tables and not enriched_payload:
        return "Table Review is certified. Downstream metadata/profiling/enrichment has not completed yet."
    if checkpoint.get("human_decision") == "COMPLETED" and not nominated_tables:
        return "KPI Review is certified. Table nomination has not completed yet."
    if not summary and not checkpoint:
        return "No stored state was found for this run ID."
    return None


def _summary_status(
    *,
    checkpoint: Dict[str, Any],
    next_gate: Optional[int],
    bronze_generation_completed: bool,
    silver_generation_completed: bool,
    gold_generation_completed: bool,
) -> str:
    if next_gate in {1, 2, 3, 4, 5}:
        return "HITL_WAIT"
    if checkpoint.get("background_stage"):
        return "RUNNING"

    status = str(
        checkpoint.get("status")
        or checkpoint.get("table_nomination_status")
        or checkpoint.get("enrichment_review_status")
        or "UNKNOWN"
    ).upper()

    if status in {"UNKNOWN", "NOT_FOUND"}:
        return "NOT_FOUND"
    if status == "ABORTED":
        return "ABORTED"
    if status == "FAILED":
        return "FAILED"
    if status in {"PIPELINE_COMPLETED", "COMPLETED"}:
        return "SUCCESS"
    if bronze_generation_completed or silver_generation_completed or gold_generation_completed:
        return "SUCCESS"
    if status in {"HITL_WAIT", "PAUSED_FOR_HITL"}:
        return "HITL_WAIT"
    if status in {"RUNNING", "PROCESSING", "SUBMITTED", "IN_PROGRESS", "GATE1_COMPLETE", "GATE2_COMPLETE", "GATE3_COMPLETE"}:
        return "RUNNING"
    return status


def _ui_run_summary(run_id: str) -> Dict[str, Any]:
    checkpoint_hint = load_checkpoint_state(run_id) or {}
    context = (
        get_sftp_run_context(run_id)
        if _is_file_source(checkpoint_hint.get("source"))
        else get_run_context(run_id)
    )
    summary = context.get("summary") or []
    checkpoint = context.get("checkpoint") or checkpoint_hint
    status = _status_from_context(context)

    return {
        "id": run_id,
        "run_id": run_id,
        "brd_filename": _display_run_name(checkpoint, context),
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
        "stages": _summary_stage_list(
            checkpoint=checkpoint,
            summary=summary,
            pipeline_steps=context.get("pipeline_steps") or [],
        ),
        "next_gate": context.get("next_gate"),
        "resume_message": context.get("resume_message"),
        "stage_confirmation": context.get("stage_confirmation"),
        "script_counts": {
            "bronze": len((context.get("bronze") or {}).get("scripts") or []),
            "silver": len((context.get("silver") or {}).get("scripts") or []),
            "gold": len((context.get("gold") or {}).get("scripts") or []),
        },
        "sftp_entity": context.get("sftp_entity"),
        "source_row_count": context.get("source_row_count"),
        "source_columns": context.get("source_columns") or [],
    }


def _hitl_decisions(run_id: str, context: Dict[str, Any]) -> List[Dict[str, Any]]:
    decisions: List[Dict[str, Any]] = []
    for row in _fetch_hitl_rows(run_id):
        if not row.get("decision"):
            continue
        decisions.append(
            {
                "id": row.get("id"),
                "gate": _gate_label(1),
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
                "gate": _gate_label(2, source=str(context.get("checkpoint", {}).get("source") or "database")),
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
                "gate": _gate_label(3),
                "type": "Enrichment",
                "name": "Semantic enrichment approved",
                "definition": "Semantic tags, PII classifications, and join annotations approved.",
                "decision": "APPROVED",
                "reviewed_at": None,
            }
        )

    return decisions


def _ui_run(run_id: str, *, include_scripts: bool = False) -> Dict[str, Any]:
    checkpoint_hint = load_checkpoint_state(run_id) or {}
    if _is_file_source(checkpoint_hint.get("source")):
        context = get_sftp_run_context(run_id)
    else:
        context = get_run_context(run_id)
    summary = context.get("summary") or []
    checkpoint = context.get("checkpoint") or {}
    total_tokens = sum(int(row.get("token_count") or 0) for row in summary)
    total_cost = sum(float(row.get("cost_usd") or 0) for row in summary)
    requirements = fetch_json_artifact(run_id, "REQUIREMENTS") or _requirements_from_checkpoint(checkpoint)
    raw_kpis = _artifact_kpis(run_id) or _kpis_from_checkpoint(checkpoint)
    hitl_rows = _fetch_hitl_rows(run_id)
    kpis = hitl_rows or [_map_kpi(kpi, run_id=run_id) for kpi in raw_kpis]
    status = _status_from_context(context)
    payload = {
        "id": run_id,
        "run_id": run_id,
        "brd_filename": _display_run_name(checkpoint, context),
        "source": checkpoint.get("source") or "database",
        "status": status,
        "provider": checkpoint.get("provider") or "azure_openai",
        "deployment": checkpoint.get("deployment"),
        "started_at": summary[0].get("stored_at") if summary else None,
        "completed_at": summary[-1].get("stored_at") if status == "SUCCESS" and summary else None,
        "cache_hit": "L1_EXACT" if checkpoint.get("memory_layer1") else "L2_SEMANTIC" if checkpoint.get("memory_layer2") else "NONE",
        "cache_score": checkpoint.get("semantic_score") or 0,
        "extraction_path": checkpoint.get("extraction_path") or "ATHENA_GRAPH",
        "total_tokens": total_tokens,
        "total_cost": total_cost,
        "stages": _ui_stages(context, run_id),
        "requirements": requirements,
        "kpis": kpis,
        "hitl_decisions": _hitl_decisions(run_id, context),
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
        "databricks_run_id": run_id,
        "sftp_entity": context.get("sftp_entity") or checkpoint.get("sftp_entity"),
        "candidate_feed": (context.get("candidate_feed") or checkpoint.get("candidate_feed")) if _is_file_source(checkpoint.get("source")) else None,
        "candidate_feeds": (context.get("candidate_feeds") or checkpoint.get("candidate_feeds") or []) if _is_file_source(checkpoint.get("source")) else [],
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
        payload.update(
            {
                "script_counts": {
                    "bronze": len((context.get("bronze") or {}).get("scripts") or []),
                    "silver": len((context.get("silver") or {}).get("scripts") or []),
                    "gold": len((context.get("gold") or {}).get("scripts") or []),
                }
            }
        )
    return payload


def _maybe_resume_gate1(run_id: str) -> None:
    if get_pending_items(run_id, 1):
        return
    checkpoint = load_checkpoint_state(run_id) or {}
    if _is_file_source(checkpoint.get("source")):
        submit_background(run_id, "gate1", submit_sftp_gate1_review, run_id, True)
        return
    submit_background(run_id, "gate1", submit_gate1_review, run_id, [])


def _tail_lines(path: Path, limit: int) -> List[str]:
    if limit <= 0 or not path.exists():
        return []

    with path.open("rb") as handle:
        handle.seek(0, os.SEEK_END)
        position = handle.tell()
        buffer = bytearray()
        newline_count = 0

        while position > 0 and newline_count <= limit:
            chunk_size = min(8192, position)
            position -= chunk_size
            handle.seek(position)
            chunk = handle.read(chunk_size)
            buffer[:0] = chunk
            newline_count = buffer.count(b"\n")

    return buffer.decode("utf-8", errors="ignore").splitlines()[-limit:]


def _read_logs(run_id: str, limit: int = 1000, since: Optional[str] = None) -> List[Dict[str, Any]]:
    log_path = PIPELINE_LOG_PATH
    if not log_path.exists():
        return []

    # This is called frequently by the UI. Keep the tail window bounded so we
    # don't repeatedly parse huge files on every poll.
    raw_lines = _tail_lines(log_path, max(limit * 5, 2000))

    logs: List[Dict[str, Any]] = []
    for line_number, line in enumerate(raw_lines):
        try:
            item = json.loads(line)
        except Exception:
            continue
        if str(item.get("run_id") or "") != run_id:
            continue
        logged_at = item.get("timestamp") or item.get("logged_at")
        if since and logged_at and logged_at <= since:
            continue
        message = item.get("message", "")
        event_type = item.get("event_type")
        if not event_type:
            normalized_message = str(message).strip().upper()
            if normalized_message.startswith("START"):
                event_type = "stage_start"
            elif normalized_message.startswith("END"):
                event_type = "stage_end"
        duration_seconds = item.get("duration_seconds")
        if duration_seconds is None:
            duration_match = re.search(r"duration_seconds=([0-9.]+)", str(message))
            if duration_match:
                duration_seconds = float(duration_match.group(1))

        stage = item.get("stage") or item.get("node") or item.get("module")
        step_name = item.get("step_name") or item.get("funcName")
        logs.append(
            {
                "log_id": f"{run_id}:{logged_at}:{line_number}:{item.get('level', 'INFO')}",
                "run_id": run_id,
                "notebook_name": item.get("node") or item.get("module"),
                "stage": stage,
                "step_name": step_name,
                "log_level": item.get("level", "INFO"),
                "message": message,
                "duration_seconds": duration_seconds,
                "event_type": event_type,
                "logged_at": logged_at,
            }
        )
    return logs[-limit:]


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok", "service": "athena-fastapi"}


@app.post("/pipeline/run")
def run_pipeline(payload: PipelineRunRequest) -> Dict[str, Any]:
    source = str(payload.source or "database").lower()
    if not _is_file_source(source) and not payload.brd_text.strip():
        raise HTTPException(status_code=400, detail="brd_text is required")
    run_id = str(uuid.uuid4())

    try:
        existing = load_checkpoint_state(run_id) or {"run_id": run_id}
        save_checkpoint_state(
            run_id,
            {
                **existing,
                "run_id": run_id,
                "status": existing.get("status") or "RUNNING",
                "brd_text": existing.get("brd_text") or payload.brd_text,
                "brd_filename": existing.get("brd_filename") or payload.brd_filename,
                "source": existing.get("source") or payload.source or "database",
                "provider": existing.get("provider") or payload.provider,
                "deployment": existing.get("deployment") or payload.deployment,
                "source_databases": existing.get("source_databases")
                or payload.source_databases
                or ([payload.database_name] if payload.database_name else None),
                "sftp_entity": existing.get("sftp_entity") or payload.sftp_entity or "transactions",
                "stage_confirmation_enabled": (
                    existing.get("stage_confirmation_enabled")
                    if existing.get("stage_confirmation_enabled") is not None
                    else bool(payload.stage_confirmation_enabled if payload.stage_confirmation_enabled is not None else True)
                ),
            },
        )
    except Exception as exc:
        logger.exception("Initial checkpoint save failed for run_id=%s", run_id)
        raise HTTPException(status_code=503, detail=f"Failed to initialize run checkpoint: {exc}") from exc

    _submit_pipeline_start(run_id, payload)
    # Keep backward-compatible semantics for the UI: a submitted run is effectively running.
    return {"run_id": run_id, "status": "RUNNING"}


@app.post("/pipeline/upload-brd")
async def upload_brd(file: UploadFile = File(...)) -> Dict[str, Any]:
    upload_dir = ROOT_DIR / "uploads"
    upload_dir.mkdir(parents=True, exist_ok=True)
    path = upload_dir / Path(file.filename or "uploaded_brd").name
    path.write_bytes(await file.read())
    return {"filename": path.name, "path": str(path), "status": "uploaded"}


@app.get("/pipeline/{run_id}/status")
def pipeline_status(run_id: str) -> Dict[str, Any]:
    run = _ui_run(run_id)
    result_state = run["status"]
    if result_state == "NOT_FOUND":
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
    return {
        "run_id": run_id,
        "status": result_state,
        "state": {
            "life_cycle_state": "TERMINATED" if result_state in {"SUCCESS", "FAILED", "ABORTED"} else "RUNNING",
            "result_state": result_state,
        },
        "run": run,
    }


@app.post("/pipeline/{run_id}/abort")
def abort_run(run_id: str) -> Dict[str, Any]:
    checkpoint = load_checkpoint_state(run_id) or {"run_id": run_id}
    checkpoint["status"] = "ABORTED"
    save_checkpoint_state(run_id, checkpoint)
    return {"run_id": run_id, "status": "ABORTED"}


@app.post("/pipeline/{run_id}/continue-stage")
def continue_stage(run_id: str, payload: StageContinueRequest) -> Dict[str, Any]:
    checkpoint = load_checkpoint_state(run_id) or {}
    next_stage_key = checkpoint.get("next_stage_key")
    if not next_stage_key:
        raise HTTPException(status_code=400, detail="No next stage is pending confirmation for this run.")
    if str(checkpoint.get("source") or "database").lower() in {"sftp", "adls_gen2"}:
        raise HTTPException(status_code=400, detail="Stage-by-stage confirmation is not enabled for file-source runs yet.")

    result = continue_database_pipeline(
        run_id,
        start_stage_key=str(next_stage_key),
        state=checkpoint,
        auto_advance=bool(payload.auto_advance),
    )
    return {
        "run_id": run_id,
        "status": result.get("status") or "RUNNING",
        "next_stage_key": result.get("next_stage_key"),
        "resume_message": result.get("resume_message"),
    }


@app.get("/runs")
def runs() -> List[Dict[str, Any]]:
    try:
        timeout_seconds = max(1, int(os.getenv("ATHENA_RUNS_ENDPOINT_TIMEOUT_SECONDS", "5")))
        future = BACKGROUND_EXECUTOR.submit(list_runs)
        rows = future.result(timeout=timeout_seconds)
        return [_ui_run_summary(row["run_id"]) for row in rows]
    except FutureTimeoutError:
        # Prevent the UI from hanging when the underlying Azure SQL query is slow/unreachable.
        logger.warning("GET /runs timed out while listing runs; returning empty list")
        return []
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/runs/{run_id}")
def run_detail(run_id: str) -> Dict[str, Any]:
    try:
        return _ui_run(run_id, include_scripts=True)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/table-reviews/{run_id}")
def table_reviews(run_id: str) -> Dict[str, Any]:
    try:
        run = _ui_run(run_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    checkpoint = load_checkpoint_state(run_id) or {}
    return {
        "run_id": run_id,
        "source": run.get("source"),
        "next_gate": run.get("next_gate"),
        "resume_message": run.get("resume_message"),
        "nominated_tables": run.get("nominated_tables") or [],
        "certified_tables": run.get("certified_tables") or [],
        "candidate_feed": checkpoint.get("candidate_feed") if _is_file_source(run.get("source")) else None,
        "candidate_feeds": (checkpoint.get("candidate_feeds") or []) if _is_file_source(run.get("source")) else [],
    }


@app.post("/table-reviews/{run_id}")
def submit_table_reviews(run_id: str, payload: Gate2DecisionPayload) -> Dict[str, Any]:
    checkpoint = load_checkpoint_state(run_id) or {}
    if _is_file_source(checkpoint.get("source")):
        submit_background(run_id, "gate2", submit_sftp_gate2_review, run_id, True)
        return {"run_id": run_id, "status": "SUBMITTED", "approve": True}

    approved_tables = [item for item in payload.approved_tables if str(item).strip()]
    if not approved_tables:
        raise HTTPException(status_code=400, detail="At least one table must be approved for Table Review.")

    submit_background(run_id, "gate2", submit_gate2_review, run_id, approved_tables)
    return {"run_id": run_id, "status": "SUBMITTED", "approved_tables": approved_tables}


@app.get("/enrichment-reviews/{run_id}")
def enrichment_reviews(run_id: str) -> Dict[str, Any]:
    try:
        run = _ui_run(run_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
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


@app.post("/enrichment-reviews/{run_id}")
def submit_enrichment_review(run_id: str, payload: Gate3DecisionPayload) -> Dict[str, Any]:
    checkpoint = load_checkpoint_state(run_id) or {}
    if _is_file_source(checkpoint.get("source")):
        submit_background(run_id, "gate3", submit_sftp_gate3_review, run_id, payload.approve)
        return {"run_id": run_id, "status": "SUBMITTED", "approve": payload.approve}
    submit_background(run_id, "gate3", submit_gate3_review, run_id, payload.approve)
    return {"run_id": run_id, "status": "SUBMITTED", "approve": payload.approve}


@app.get("/bronze-reviews/{run_id}")
def bronze_reviews(run_id: str) -> Dict[str, Any]:
    try:
        run = _ui_run(run_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    checkpoint = load_checkpoint_state(run_id) or {}
    bronze_artifact = checkpoint.get("bronze_review_artifact") or run.get("bronze_review_artifact") or {}
    if not (bronze_artifact.get("feeds") or []):
        bronze_artifact = _bronze_review_from_scripts(run_id, checkpoint)
    return {
        "run_id": run_id,
        "next_gate": run.get("next_gate"),
        "resume_message": run.get("resume_message"),
        "bronze_review_artifact": bronze_artifact,
    }


@app.post("/bronze-reviews/{run_id}")
def submit_bronze_reviews(run_id: str, payload: GenericGateDecisionPayload) -> Dict[str, Any]:
    submit_background(run_id, "gate4", submit_sftp_gate4_review, run_id, payload.action)
    return {"run_id": run_id, "status": "SUBMITTED", "action": payload.action}


@app.get("/silver-reviews/{run_id}")
def silver_reviews(run_id: str) -> Dict[str, Any]:
    try:
        run = _ui_run(run_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    checkpoint = load_checkpoint_state(run_id) or {}
    silver_artifact = checkpoint.get("silver_review_artifact") or run.get("silver_review_artifact") or {}
    if not (silver_artifact.get("items") or []):
        silver_artifact = _silver_review_from_scripts(run_id, checkpoint)
    return {
        "run_id": run_id,
        "next_gate": run.get("next_gate"),
        "resume_message": run.get("resume_message"),
        "silver_review_artifact": silver_artifact,
    }


@app.post("/silver-reviews/{run_id}")
def submit_silver_reviews(run_id: str, payload: GenericGateDecisionPayload) -> Dict[str, Any]:
    submit_background(run_id, "gate5", submit_sftp_gate5_review, run_id, payload.action)
    return {"run_id": run_id, "status": "SUBMITTED", "action": payload.action}


@app.get("/kpi-reviews/{run_id}")
def kpi_reviews(run_id: str, status: Optional[str] = None) -> Dict[str, Any]:
    try:
        rows = _fetch_hitl_rows(run_id, status=status)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if not rows:
        rows = [_map_kpi(kpi, run_id=run_id) for kpi in _artifact_kpis(run_id)]
    return {"runId": run_id, "run_id": run_id, "kpis": rows}


@app.post("/kpi-reviews/{queue_id}/approve")
def approve_kpi(queue_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    update_hitl_item(queue_id, "APPROVED")
    run_id = queue_id.split(":1:", 1)[0]
    _maybe_resume_gate1(run_id)
    return {"queue_id": queue_id, "status": "APPROVED"}


@app.post("/kpi-reviews/{queue_id}/reject")
def reject_kpi(queue_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    update_hitl_item(queue_id, "REJECTED", rejection_reason=payload.get("rejection_reason"))
    run_id = queue_id.split(":1:", 1)[0]
    _maybe_resume_gate1(run_id)
    return {"queue_id": queue_id, "status": "REJECTED"}


@app.post("/kpi-reviews/{queue_id}/modify")
def modify_kpi(queue_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    update_hitl_item(queue_id, "APPROVED", edited_content=json.dumps(payload.get("edited_content") or {}))
    run_id = queue_id.split(":1:", 1)[0]
    _maybe_resume_gate1(run_id)
    return {"queue_id": queue_id, "status": "APPROVED"}


@app.post("/kpi-reviews/{run_id}/bulk")
def bulk_kpi_action(run_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    rows = _fetch_hitl_rows(run_id)
    action = payload.get("action", "APPROVED")
    for row in rows:
        if row.get("decision"):
            continue
        update_hitl_item(row["queue_id"], action, rejection_reason=payload.get("rejection_reason"))
    _maybe_resume_gate1(run_id)
    return {"run_id": run_id, "status": action}


@app.get("/hitl/{run_id}")
def hitl_queue(run_id: str) -> Dict[str, Any]:
    return kpi_reviews(run_id)


@app.post("/hitl/{run_id}/decisions")
def submit_hitl_decisions(run_id: str, payload: HitlDecisionPayload) -> Dict[str, Any]:
    for decision in payload.decisions:
        status = decision.decision.upper()
        if status == "EDITED":
            edited = {"definition": decision.edited_definition, "notes": decision.notes}
            update_hitl_item(decision.kpi_id, "APPROVED", edited_content=json.dumps(edited))
        elif status == "REJECTED":
            update_hitl_item(decision.kpi_id, "REJECTED", rejection_reason=decision.notes)
        else:
            update_hitl_item(decision.kpi_id, "APPROVED")
    _maybe_resume_gate1(run_id)
    return {"run_id": run_id, "status": "SUBMITTED"}


@app.get("/kpis")
def kpis() -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for row in list_runs():
        run_id = row["run_id"]
        items.extend(_map_kpi(kpi, run_id=run_id) for kpi in _artifact_kpis(run_id))
    return items


@app.get("/analytics/cost")
def analytics_cost() -> List[Dict[str, Any]]:
    return []


@app.get("/settings")
def settings() -> Dict[str, Any]:
    return {
        "provider": "azure_openai",
        "azure_deployment": os.getenv("AZURE_OPENAI_DEPLOYMENT"),
        "budget": 5.0,
        "maxKpis": 25,
        "devMode": os.getenv("DEV_MODE", "").lower() in {"1", "true", "yes", "on"},
    }


@app.put("/settings")
def save_settings(data: Dict[str, Any]) -> Dict[str, Any]:
    return data


@app.get("/configurations")
def configurations() -> List[Dict[str, Any]]:
    db_conf = config["azure_sql"]
    return [
        {
            "id": "azure_sql_default",
            "name": "Default Azure SQL",
            "sourceType": "database",
            "dbType": "azure_sql",
            "host": db_conf.get("source_host"),
            "port": str(db_conf.get("port", 1433)),
            "databaseName": db_conf.get("source_database"),
            "schema": db_conf.get("source_schema"),
            "username": db_conf.get("source_username"),
            "driverClass": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "jdbcUrl": "",
        }
    ]


@app.post("/configurations")
def create_configuration(data: Dict[str, Any]) -> Dict[str, Any]:
    return {**data, "id": data.get("id") or str(uuid.uuid4())}


@app.put("/configurations/{config_id}")
def update_configuration(config_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
    return {**data, "id": config_id}


@app.delete("/configurations/{config_id}")
def delete_configuration(config_id: str) -> Dict[str, Any]:
    return {"id": config_id, "deleted": True}


@app.post("/logs/discover/{run_id}")
def discover_logs(run_id: str) -> Dict[str, Any]:
    return {"status": "completed", "runId": run_id}


@app.get("/logs/discover/{run_id}/status")
def discover_logs_status(run_id: str) -> Dict[str, Any]:
    return {"status": "completed", "runId": run_id}


@app.get("/logs/{run_id}")
def logs(run_id: str, limit: int = 300) -> Dict[str, Any]:
    return {"runId": run_id, "logs": _read_logs(run_id, limit=limit)}


@app.get("/logs/{run_id}/since/{since_timestamp}")
def logs_since(run_id: str, since_timestamp: str, limit: int = 300) -> Dict[str, Any]:
    return {"runId": run_id, "logs": _read_logs(run_id, limit=limit, since=since_timestamp)}
