from __future__ import annotations

import json
import os
import sys
import uuid
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
from utilis.logger import logger
from services.pipeline_runtime import (
    BACKGROUND_EXECUTOR,
    BACKGROUND_JOBS,
    BACKGROUND_JOB_LOCK,
    fetch_json_artifact,
    get_run_context,
    list_runs,
    load_checkpoint_state,
    save_checkpoint_state,
    start_pipeline,
    submit_background,
    submit_gate1_review,
    submit_gate2_review,
    submit_gate3_review,
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
    provider: Optional[str] = "azure_openai"
    deployment: Optional[str] = None
    budget: Optional[float] = None
    maxKpis: Optional[int] = None
    devMode: Optional[bool] = None
    database_name: Optional[str] = None
    database_type: Optional[str] = None
    source_databases: Optional[List[str]] = None


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


def _pipeline_schema() -> str:
    return (
        config["azure_sql"].get("pipeline_schema")
        or config["azure_sql"].get("schema_name")
        or "dbo"
    )


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
    source_databases: Optional[List[str]],
) -> None:
    try:
        result = start_pipeline(
            brd_text=brd_text,
            source_databases=source_databases,
            run_id=run_id,
        )
        state = result.get("result") if isinstance(result, dict) else {}
        if isinstance(state, dict):
            pending_gate1 = get_pending_items(run_id, 1)
            state["status"] = "HITL_WAIT" if pending_gate1 else state.get("status", "COMPLETED")
            save_checkpoint_state(run_id, {**state, "run_id": run_id})
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
            source_databases=payload.source_databases
            or ([payload.database_name] if payload.database_name else None),
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
    if context.get("pending_gate1"):
        return "HITL_WAIT"
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


def _ui_stages(context: Dict[str, Any]) -> List[Dict[str, Any]]:
    now = datetime.now(timezone.utc).isoformat()
    return [
        {
            "id": f"stage_{index + 1:02d}",
            "name": step["label"],
            "status": (
                "COMPLETED"
                if step["state"] == "complete"
                else "FAILED"
                if step["state"] == "failed"
                else "RUNNING"
                if step["state"] == "running"
                else "PENDING"
            ),
            "tokens": 0,
            "cost": 0,
            "attempts": 0,
            "started_at": now if step["state"] in {"complete", "running", "failed"} else None,
            "completed_at": now if step["state"] == "complete" else None,
            "error": context.get("checkpoint", {}).get("error") if step["state"] == "failed" else None,
            "prompt_metadata": None,
        }
        for index, step in enumerate(context.get("pipeline_steps", []))
    ]


def _ui_run(run_id: str, *, include_scripts: bool = False) -> Dict[str, Any]:
    context = get_run_context(run_id)
    summary = context.get("summary") or []
    checkpoint = context.get("checkpoint") or {}
    total_tokens = sum(int(row.get("token_count") or 0) for row in summary)
    total_cost = sum(float(row.get("cost_usd") or 0) for row in summary)
    requirements = fetch_json_artifact(run_id, "REQUIREMENTS") or _requirements_from_checkpoint(checkpoint)
    raw_kpis = _artifact_kpis(run_id) or _kpis_from_checkpoint(checkpoint)
    kpis = [_map_kpi(kpi, run_id=run_id) for kpi in raw_kpis]
    status = _status_from_context(context)
    payload = {
        "id": run_id,
        "run_id": run_id,
        "brd_filename": checkpoint.get("brd_filename") or "athena_brd.txt",
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
        "stages": _ui_stages(context),
        "requirements": requirements,
        "kpis": kpis,
        "nominated_tables": context.get("nominated_tables") or [],
        "certified_tables": context.get("certified_tables") or [],
        "enriched_metadata": context.get("enriched_metadata") or {},
        "enriched_columns": context.get("enriched_columns") or [],
        "enriched_joins": context.get("enriched_joins") or [],
        "semantic_counts": context.get("semantic_counts") or {},
        "pii_columns": context.get("pii_columns") or [],
        "join_key_columns": context.get("join_key_columns") or [],
        "measure_columns": context.get("measure_columns") or [],
        "gate3_approved": context.get("gate3_approved") or False,
        "next_gate": context.get("next_gate"),
        "resume_message": context.get("resume_message"),
        "databricks_run_id": run_id,
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
    log_path = ROOT_DIR / "pipeline_logs.json"
    if not log_path.exists():
        return []

    raw_lines = (
        log_path.read_text(encoding="utf-8").splitlines()
        if since
        else _tail_lines(log_path, max(limit * 10, 200))
    )

    logs: List[Dict[str, Any]] = []
    for line in raw_lines:
        try:
            item = json.loads(line)
        except Exception:
            continue
        if str(item.get("run_id") or "") != run_id:
            continue
        logged_at = item.get("timestamp") or item.get("logged_at")
        if since and logged_at and logged_at <= since:
            continue
        logs.append(
            {
                "log_id": f"{run_id}:{len(logs)}:{logged_at}",
                "run_id": run_id,
                "notebook_name": item.get("node") or item.get("module"),
                "stage": item.get("node") or item.get("stage"),
                "step_name": item.get("funcName"),
                "log_level": item.get("level", "INFO"),
                "message": item.get("message", ""),
                "duration_seconds": item.get("duration_seconds"),
                "logged_at": logged_at,
            }
        )
    return logs[-limit:]


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok", "service": "athena-fastapi"}


@app.post("/pipeline/run")
def run_pipeline(payload: PipelineRunRequest) -> Dict[str, Any]:
    if not payload.brd_text.strip():
        raise HTTPException(status_code=400, detail="brd_text is required")
    run_id = str(uuid.uuid4())
    try:
        save_checkpoint_state(
            run_id,
            {
                "run_id": run_id,
                "status": "RUNNING",
                "brd_text": payload.brd_text,
                "brd_filename": payload.brd_filename,
                "provider": payload.provider,
                "deployment": payload.deployment,
                "source_databases": payload.source_databases
                or ([payload.database_name] if payload.database_name else None),
            },
        )
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=(
                "Athena could not connect to the pipeline Azure SQL database. "
                "Check Azure SQL firewall access, ODBC driver, and credentials. "
                f"Original error: {exc}"
            ),
        ) from exc
    _submit_pipeline_start(run_id, payload)
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


@app.get("/runs")
def runs() -> List[Dict[str, Any]]:
    return [_ui_run(row["run_id"]) for row in list_runs()]


@app.get("/runs/{run_id}")
def run_detail(run_id: str) -> Dict[str, Any]:
    return _ui_run(run_id, include_scripts=True)


@app.get("/table-reviews/{run_id}")
def table_reviews(run_id: str) -> Dict[str, Any]:
    run = _ui_run(run_id)
    return {
        "run_id": run_id,
        "next_gate": run.get("next_gate"),
        "resume_message": run.get("resume_message"),
        "nominated_tables": run.get("nominated_tables") or [],
        "certified_tables": run.get("certified_tables") or [],
    }


@app.post("/table-reviews/{run_id}")
def submit_table_reviews(run_id: str, payload: Gate2DecisionPayload) -> Dict[str, Any]:
    approved_tables = [item for item in payload.approved_tables if str(item).strip()]
    if not approved_tables:
        raise HTTPException(status_code=400, detail="At least one table must be approved for Gate 2.")

    submit_background(run_id, "gate2", submit_gate2_review, run_id, approved_tables)
    return {"run_id": run_id, "status": "SUBMITTED", "approved_tables": approved_tables}


@app.get("/enrichment-reviews/{run_id}")
def enrichment_reviews(run_id: str) -> Dict[str, Any]:
    run = _ui_run(run_id)
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
        "gate3_approved": run.get("gate3_approved") or False,
    }


@app.post("/enrichment-reviews/{run_id}")
def submit_enrichment_review(run_id: str, payload: Gate3DecisionPayload) -> Dict[str, Any]:
    submit_background(run_id, "gate3", submit_gate3_review, run_id, payload.approve)
    return {"run_id": run_id, "status": "SUBMITTED", "approve": payload.approve}


@app.get("/kpi-reviews/{run_id}")
def kpi_reviews(run_id: str, status: Optional[str] = None) -> Dict[str, Any]:
    rows = _fetch_hitl_rows(run_id, status=status)
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
def logs(run_id: str, limit: int = 1000) -> Dict[str, Any]:
    return {"runId": run_id, "logs": _read_logs(run_id, limit=limit)}


@app.get("/logs/{run_id}/since/{since_timestamp}")
def logs_since(run_id: str, since_timestamp: str) -> Dict[str, Any]:
    return {"runId": run_id, "logs": _read_logs(run_id, since=since_timestamp)}
