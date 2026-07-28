from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from state import Stage01State
from utilis.db import config, get_pipeline_connection
from utilis.logger import logger

_BRONZE_PLAN_TABLE_READY = False
_BRONZE_PLAN_TABLE_LOCK = threading.Lock()


def _copy_state(state: Stage01State) -> Stage01State:
    return state.copy()


def _pipeline_schema() -> str:
    return config["azure_sql"]["pipeline_schema"]


def _latest_registry_feed(feed_id: str) -> Optional[Dict[str, Any]]:
    conn = get_pipeline_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT TOP 1 feed_id, vendor, entity, format, file_name, file_path, remote_path, status, source, approved_at
            FROM [{_pipeline_schema()}].[file_feed_registry]
            WHERE feed_id = ?
            ORDER BY updated_at DESC, created_at DESC
            """,
            feed_id,
        )
        row = cursor.fetchone()
        if not row:
            return None
        return {
            "feed_id": row.feed_id,
            "vendor": row.vendor,
            "entity": row.entity,
            "format": row.format,
            "file_name": row.file_name,
            "file_path": row.file_path,
            "remote_path": row.remote_path,
            "status": row.status,
            "source": row.source,
            "approved_at": getattr(row, "approved_at", None),
        }
    finally:
        conn.close()


def _latest_schema(feed_id: str) -> Optional[Dict[str, Any]]:
    conn = get_pipeline_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ?
              AND TABLE_NAME = 'file_feed_schema_registry'
            """,
            _pipeline_schema(),
        )
        schema_columns = {str(row.COLUMN_NAME).lower() for row in cursor.fetchall()}
        if "schema_status" not in schema_columns:
            logger.warning("file_feed_schema_registry has no schema_status column; source readiness blocked", extra={"feed_id": feed_id})
            return None
        status_filter = "AND UPPER(schema_status) = 'APPROVED'"
        cursor.execute(
            f"""
            SELECT TOP 1 feed_id, vendor, entity, format, schema_json, schema_fingerprint, version, discovered_at
            FROM [{_pipeline_schema()}].[file_feed_schema_registry]
            WHERE feed_id = ?
            {status_filter}
            ORDER BY version DESC, discovered_at DESC
            """,
            feed_id,
        )
        row = cursor.fetchone()
        if not row:
            return None
        try:
            payload = json.loads(row.schema_json) if row.schema_json else []
        except Exception:
            logger.exception("Failed to decode approved schema payload for feed_id=%s", feed_id)
            return None
        if not isinstance(payload, list):
            logger.warning("Approved schema payload is not a list for feed_id=%s", feed_id)
            return None
        return {
            "feed_id": row.feed_id,
            "vendor": row.vendor,
            "entity": row.entity,
            "format": row.format,
            "schema_json": payload,
            "schema_fingerprint": row.schema_fingerprint,
            "version": row.version,
            "discovered_at": row.discovered_at,
        }
    finally:
        conn.close()


def _resolve_approved_feeds(state: Stage01State) -> List[Dict[str, Any]]:
    feeds = [dict(feed) for feed in (state.get("candidate_feeds") or []) if isinstance(feed, dict)]
    if not feeds and isinstance(state.get("candidate_feed"), dict):
        feeds = [dict(state["candidate_feed"])]
    resolved: List[Dict[str, Any]] = []
    for feed in feeds:
        feed_id = str(feed.get("feed_id") or f"{feed.get('vendor')}_{feed.get('entity')}")
        registry_feed = _latest_registry_feed(feed_id) or {}
        merged = {**feed, **registry_feed}
        if str(merged.get("status") or "").upper() == "APPROVED":
            resolved.append(merged)
    return resolved


def _primary_keys_from_schema(schema_columns: List[Dict[str, Any]]) -> List[str]:
    explicit = [str(col.get("column_name") or "") for col in schema_columns if col.get("is_primary_key")]
    if explicit:
        return [col for col in explicit if col]
    fallback = [
        str(col.get("column_name") or "")
        for col in schema_columns
        if "id" in str(col.get("column_name") or "").lower()
    ]
    return [col for col in fallback if col]


def _watermark_column(schema_columns: List[Dict[str, Any]]) -> Optional[str]:
    for col in schema_columns:
        name = str(col.get("column_name") or "")
        lowered = name.lower()
        if any(token in lowered for token in ("modified", "updated", "timestamp", "date", "created")):
            return name
    return None


def _bronze_paths(feed: Dict[str, Any]) -> Dict[str, str]:
    vendor = str(feed.get("vendor") or "Vendor1")
    entity = str(feed.get("entity") or "unknown")
    landing_path = str(feed.get("landing_path") or state_safe_path(feed.get("file_path")) or f"/Volumes/sftp_landing/{vendor}/{entity}")
    bronze_output_path = f"dbfs:/pipelines/bronze/{vendor}/{entity}"
    checkpoint_path = f"dbfs:/pipelines/checkpoints/bronze/{vendor}/{entity}"
    return {
        "landing_path": landing_path,
        "bronze_output_path": bronze_output_path,
        "checkpoint_path": checkpoint_path,
    }


def state_safe_path(value: Any) -> str:
    return str(value or "").strip()


def _ensure_bronze_plan_table(cursor) -> None:
    global _BRONZE_PLAN_TABLE_READY
    if _BRONZE_PLAN_TABLE_READY:
        return
    with _BRONZE_PLAN_TABLE_LOCK:
        if _BRONZE_PLAN_TABLE_READY:
            return
        cursor.execute(
            f"""
            IF OBJECT_ID(N'[{_pipeline_schema()}].[bronze_execution_plan]', N'U') IS NULL
            BEGIN
                CREATE TABLE [{_pipeline_schema()}].[bronze_execution_plan] (
                    [id] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    [run_id] NVARCHAR(255) NOT NULL,
                    [feed_id] NVARCHAR(255) NOT NULL,
                    [vendor] NVARCHAR(255) NOT NULL,
                    [entity] NVARCHAR(255) NOT NULL,
                    [plan_json] NVARCHAR(MAX) NOT NULL,
                    [script_text] NVARCHAR(MAX) NOT NULL,
                    [config_json] NVARCHAR(MAX) NOT NULL,
                    [review_status] NVARCHAR(50) NOT NULL,
                    [created_at] DATETIME2(7) NOT NULL DEFAULT SYSUTCDATETIME(),
                    [updated_at] DATETIME2(7) NOT NULL DEFAULT SYSUTCDATETIME()
                );
            END
            """
        )
        _BRONZE_PLAN_TABLE_READY = True


def _persist_bronze_plan(plan: Dict[str, Any]) -> None:
    conn = get_pipeline_connection()
    try:
        cursor = conn.cursor()
        _ensure_bronze_plan_table(cursor)
        cursor.execute(
            f"""
            INSERT INTO [{_pipeline_schema()}].[bronze_execution_plan]
            (run_id, feed_id, vendor, entity, plan_json, script_text, config_json, review_status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            plan["run_id"],
            plan["feed_id"],
            plan["vendor"],
            plan["entity"],
            json.dumps(plan, default=str),
            plan["generated_bronze_script"],
            json.dumps(plan["bronze_config"], default=str),
            str(plan.get("review_status") or "PENDING"),
        )
        conn.commit()
    except Exception:
        logger.exception("Failed to persist bronze execution plan run_id=%s feed_id=%s", plan.get("run_id"), plan.get("feed_id"))
        raise
    finally:
        conn.close()


def source_access_readiness_check_node(state: Stage01State) -> Stage01State:
    new_state = _copy_state(state)
    if str(new_state.get("status") or "").upper() == "FAILED":
        return new_state

    logger.info("Running source access readiness check run_id=%s", new_state.get("run_id"))
    feeds = _resolve_approved_feeds(new_state)
    if not feeds:
        new_state["status"] = "FAILED"
        new_state["error"] = "Bronze readiness failed: no approved feeds found in file_feed_registry"
        return new_state

    reviewed_feeds: List[Dict[str, Any]] = []
    for feed in feeds:
        schema = _latest_schema(str(feed.get("feed_id") or "")) or {}
        schema_columns = schema.get("schema_json") or []
        if not schema_columns:
            new_state["status"] = "FAILED"
            new_state["error"] = f"Bronze readiness failed: no APPROVED schema snapshot found for {feed.get('feed_id')}"
            return new_state

        paths = _bronze_paths(feed)
        reviewed_feeds.append(
            {
                "feed_id": feed.get("feed_id"),
                "vendor": feed.get("vendor"),
                "entity": feed.get("entity"),
                "source_type": feed.get("source") or new_state.get("source"),
                "file_format": feed.get("format"),
                "approved_schema": schema_columns,
                "primary_keys": _primary_keys_from_schema(schema_columns),
                "watermark_column": _watermark_column(schema_columns),
                **paths,
            }
        )

    new_state["bronze_review_artifact"] = {
        "run_id": new_state.get("run_id"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "feeds": reviewed_feeds,
        "validation_checklist": [
            "Feed approved in registry",
            "Schema snapshot available",
            "Primary keys identified or reviewed",
            "Watermark column reviewed",
            "Landing and output paths confirmed",
            "Generated Bronze plan validated",
        ],
    }
    return new_state


def pre_bronze_readiness_check_node(state: Stage01State) -> Stage01State:
    return source_access_readiness_check_node(state)


def sftp_gate4_node(state: Stage01State) -> Stage01State:
    new_state = _copy_state(state)
    if str(new_state.get("status") or "").upper() == "FAILED":
        return new_state

    decision = str(new_state.get("bronze_review_decision") or "").strip().upper()
    if decision not in {"APPROVED", "REJECTED", "REGENERATE"}:
        new_state["gate4"] = {
            "gate": "gate4",
            "status": "PENDING",
            "decision": None,
            "reason": "Awaiting Bronze review",
            "payload_summary": new_state.get("bronze_review_artifact") or {},
        }
        new_state["status"] = "HITL_WAIT"
        return new_state

    if decision == "REJECTED":
        new_state["gate4"] = {"gate": "gate4", "status": "COMPLETED", "decision": "REJECTED"}
        new_state["status"] = "FAILED"
        new_state["error"] = "Gate 4 rejected Bronze review artifact"
        return new_state

    if decision == "REGENERATE":
        new_state["gate4"] = {"gate": "gate4", "status": "COMPLETED", "decision": "REGENERATE"}
        new_state["status"] = "REGENERATE_REQUIRED"
        return new_state

    new_state["gate4"] = {"gate": "gate4", "status": "COMPLETED", "decision": "APPROVED"}
    return new_state


def bronze_validation_node(state: Stage01State) -> Stage01State:
    new_state = _copy_state(state)
    if str(new_state.get("status") or "").upper() == "FAILED":
        return new_state
    bronze_artifact = new_state.get("bronze_review_artifact") or {}
    has_feeds = bool(bronze_artifact.get("feeds"))
    new_state["bronze_validation_status"] = "COMPLETED" if has_feeds else "FAILED"
    if not has_feeds:
        new_state["status"] = "FAILED"
        new_state["error"] = "Bronze validation failed: no reviewed Bronze feeds available"
        new_state["bronze_validation_error"] = new_state["error"]
    return new_state


def sftp_gate5_node(state: Stage01State) -> Stage01State:
    new_state = _copy_state(state)
    if str(new_state.get("status") or "").upper() == "FAILED":
        return new_state

    decision = str(new_state.get("silver_review_decision") or "").strip().upper()
    if decision not in {"APPROVED", "REJECTED", "REGENERATE"}:
        new_state["gate5"] = {
            "gate": "gate5",
            "status": "PENDING",
            "decision": None,
            "reason": "Awaiting Silver review",
            "payload_summary": new_state.get("silver_review_artifact") or {},
        }
        new_state["status"] = "HITL_WAIT"
        return new_state

    if decision == "REJECTED":
        new_state["gate5"] = {"gate": "gate5", "status": "COMPLETED", "decision": "REJECTED"}
        new_state["status"] = "FAILED"
        new_state["error"] = "Gate 5 rejected Silver review artifact"
        return new_state

    if decision == "REGENERATE":
        new_state["gate5"] = {"gate": "gate5", "status": "COMPLETED", "decision": "REGENERATE"}
        new_state["status"] = "REGENERATE_REQUIRED"
        return new_state

    new_state["gate5"] = {"gate": "gate5", "status": "COMPLETED", "decision": "APPROVED"}
    return new_state


def dq_validation_node(state: Stage01State) -> Stage01State:
    new_state = _copy_state(state)
    if str(new_state.get("status") or "").upper() == "FAILED":
        return new_state
    new_state["dq_validation_status"] = "SKIPPED"
    return new_state


def persist_bronze_execution_plan(
    feed_plan: Dict[str, Any],
    state: Optional[Stage01State] = None,
) -> None:
    _persist_bronze_plan(feed_plan)
