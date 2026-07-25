from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from state import Stage01State
from utilis.db import ai_store_db_writer, config, get_pipeline_connection
from utilis.logger import logger


def _copy_state(state: Stage01State) -> Stage01State:
    return state.copy()


def _schema_entries(state: Stage01State) -> List[Dict[str, Any]]:
    metadata = state.get("discovered_metadata") or {}
    return [dict(item) for item in (metadata.get("schema_registry") or [])]


def _profile_entries(state: Stage01State) -> List[Dict[str, Any]]:
    profiling = state.get("column_profiles") or {}
    return [dict(item) for item in (profiling.get("column_profiles") or [])]


def _profile_index(profile_rows: List[Dict[str, Any]]) -> Dict[Tuple[str, str], Dict[str, Any]]:
    index: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for row in profile_rows:
        key = (str(row.get("feed_id") or ""), str(row.get("column_name") or "").lower())
        index[key] = row
    return index


def _metric(profile_row: Dict[str, Any], name: str, default: Any = None) -> Any:
    metrics = profile_row.get("metrics_json") or {}
    return metrics.get(name, default)


def _semantic_type(column_name: str, data_type: str, profile_row: Dict[str, Any]) -> str:
    name = str(column_name or "").lower()
    dtype = str(data_type or "").lower()
    row_count = int(_metric(profile_row, "row_count", 0) or 0)
    null_count = int(_metric(profile_row, "null_count", 0) or 0)
    distinct_count = int(_metric(profile_row, "distinct_count", 0) or 0)

    if row_count > 0 and distinct_count == row_count and null_count == 0:
        return "PRIMARY_KEY"
    if any(token in name for token in ("email", "phone", "ssn", "aadhaar", "pan")):
        return "PII"
    if dtype in {"date", "datetime", "datetime2", "timestamp"}:
        return "DATE"
    if distinct_count <= 2 and row_count > 0:
        return "FLAG"
    if dtype in {"int", "bigint", "double", "float", "decimal", "numeric"} and distinct_count > 20:
        return "MEASURE"
    return "DIMENSION"


def _column_flags(semantic_type: str) -> Dict[str, bool]:
    return {
        "is_primary_key": semantic_type == "PRIMARY_KEY",
        "is_measure": semantic_type == "MEASURE",
        "is_dimension": semantic_type == "DIMENSION",
        "is_pii": semantic_type == "PII",
        "is_date": semantic_type == "DATE",
        "is_flag": semantic_type == "FLAG",
    }


def _persist_enriched_rows(rows: List[Dict[str, Any]], *, log_context: Dict[str, Any]) -> None:
    conn = get_pipeline_connection()
    table_schema = config["azure_sql"]["pipeline_schema"]
    try:
        cursor = conn.cursor()
        for row in rows:
            cursor.execute(
                f"""
                INSERT INTO [{table_schema}].[enriched_metadata]
                (feed_id, vendor, entity, column_name, semantic_type, approved, created_at, payload_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                row["feed_id"],
                row["vendor"],
                row["entity"],
                row["column_name"],
                row["semantic_type"],
                False,
                row["created_at"],
                json.dumps(row),
            )
        conn.commit()
    except Exception as exc:
        logger.warning("SFTP enriched metadata persistence skipped: %s", exc, extra=log_context)
    finally:
        conn.close()


def _schema_review_columns() -> List[str]:
    conn = get_pipeline_connection()
    table_schema = config["azure_sql"]["pipeline_schema"]
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ?
              AND TABLE_NAME = 'file_feed_schema_registry'
            """,
            table_schema,
        )
        return [str(row.COLUMN_NAME).lower() for row in cursor.fetchall()]
    finally:
        conn.close()


def _update_schema_review_status(
    rows: List[Dict[str, Any]],
    *,
    status: str,
    approved_by: str | None,
    rejection_reason: str | None,
    log_context: Dict[str, Any],
) -> None:
    if not rows:
        return

    available = set(_schema_review_columns())
    if "schema_status" not in available:
        logger.warning("Schema review columns missing on file_feed_schema_registry", extra=log_context)
        return

    conn = get_pipeline_connection()
    table_schema = config["azure_sql"]["pipeline_schema"]
    try:
        cursor = conn.cursor()
        approved_at = datetime.now(timezone.utc).isoformat()
        for row in rows:
            set_parts = ["schema_status = ?"]
            params: List[Any] = [status]
            if "approved_by" in available:
                set_parts.append("approved_by = ?")
                params.append(approved_by)
            if "approved_at" in available:
                set_parts.append("approved_at = ?")
                params.append(approved_at if status == "APPROVED" else None)
            if "rejection_reason" in available:
                set_parts.append("rejection_reason = ?")
                params.append(rejection_reason if status == "REJECTED" else None)

            params.extend(
                [
                    row["feed_id"],
                    row["schema_fingerprint"],
                ]
            )
            cursor.execute(
                f"""
                UPDATE [{table_schema}].[file_feed_schema_registry]
                SET {', '.join(set_parts)}
                WHERE feed_id = ?
                  AND schema_fingerprint = ?
                """,
                *params,
            )
        conn.commit()
    except Exception as exc:
        logger.warning("Schema review status persistence skipped: %s", exc, extra=log_context)
    finally:
        conn.close()


def sftp_semantic_enrichment_node(state: Stage01State) -> Stage01State:
    new_state = _copy_state(state)
    log_context = {
        "run_id": new_state.get("run_id", "unknown"),
        "node": "sftp_semantic_enrichment",
        "stage": "sftp_semantic_enrichment",
    }
    logger.info("SFTP semantic enrichment starting", extra={**log_context, "event_type": "stage_start"})

    if new_state.get("status") == "FAILED":
        return new_state

    schema_entries = _schema_entries(new_state)
    profile_rows = _profile_entries(new_state)
    if not schema_entries:
        new_state.update({
            "semantic_enrichment_status": "FAILED",
            "enrichment_review_error": "Schema snapshot is required before file semantic enrichment",
            "status": "FAILED",
            "error": "File semantic enrichment prerequisites missing",
        })
        return new_state

    profile_index = _profile_index(profile_rows)
    enriched_columns: List[Dict[str, Any]] = []
    joins: List[Dict[str, Any]] = []
    created_at = datetime.now(timezone.utc).isoformat()

    for feed in schema_entries:
        feed_id = str(feed.get("feed_id") or "")
        vendor = str(feed.get("vendor") or "")
        entity = str(feed.get("entity") or "")
        for column in feed.get("schema_json") or []:
            column_name = str(column.get("column_name") or "")
            data_type = str(column.get("data_type") or "string")
            profile_row = profile_index.get((feed_id, column_name.lower()), {})
            semantic_type = _semantic_type(column_name, data_type, profile_row)
            enriched = {
                "feed_id": feed_id,
                "vendor": vendor,
                "entity": entity,
                "column_name": column_name,
                "data_type": data_type,
                "semantic_type": semantic_type,
                **_column_flags(semantic_type),
                "approved": False,
                "created_at": created_at,
                "profile": profile_row.get("metrics_json") or {},
            }
            enriched_columns.append(enriched)

    counts: Dict[str, int] = {}
    for item in enriched_columns:
        key = str(item.get("semantic_type") or "UNKNOWN")
        counts[key] = counts.get(key, 0) + 1

    _persist_enriched_rows(enriched_columns, log_context=log_context)

    payload = {
        "run_id": new_state.get("run_id"),
        "fingerprint": new_state.get("fingerprint") or new_state.get("run_id"),
        "source": "FILE_SEMANTIC_ENRICHMENT",
        "enriched_at": created_at,
        "columns": enriched_columns,
        "joins": joins,
        "semantic_counts": counts,
        "schema_review_artifact": new_state.get("schema_review_artifact") or {},
    }

    ai_store_db_writer(
        run_id=str(new_state.get("run_id") or "unknown"),
        stage="File Semantic Enrichment",
        artifact_type="ENRICHED_METADATA",
        payload=payload,
        schema_version="FILE_ENRICHED_METADATA_v2",
        prompt_version="FILE_NB09A_v2",
        faithfulness_status="NOT_APPLICABLE",
        token_count=0,
        input_tokens=0,
        output_tokens=0,
        fingerprint=str(new_state.get("fingerprint") or new_state.get("run_id") or "unknown"),
    )

    new_state.update({
        "enriched_metadata": payload,
        "semantic_enrichment_status": "COMPLETED",
        "enrichment_review_status": "PENDING",
        "enrichment_review_decision": "PENDING",
        "semantic_tags_reviewed": False,
        "pii_classifications_reviewed": False,
        "join_key_annotations_reviewed": False,
        "status": "IN_PROGRESS",
    })
    logger.info("File semantic enrichment completed: columns=%d", len(enriched_columns), extra={**log_context, "event_type": "stage_end"})
    return new_state


def certify_sftp_gate3(run_id: str, enrichment_artifact: Dict[str, Any], fingerprint: str | None = None) -> None:
    ai_store_db_writer(
        run_id=run_id,
        stage="File Gate 3 Certification",
        artifact_type="GATE3_APPROVED_ENRICHMENT",
        payload={
            "fingerprint": fingerprint or run_id,
            "storage_fingerprint": f"{fingerprint or run_id}:GATE3_APPROVED_ENRICHMENT",
            "run_id": run_id,
            "enrichment_artifact": enrichment_artifact,
            "source": "FILE_HUMAN_CERTIFIED_ENRICHMENT",
        },
        schema_version="FILE_GATE3_v2",
        prompt_version="FILE_NB09B_v2",
        faithfulness_status="PASSED",
        token_count=0,
        input_tokens=0,
        output_tokens=0,
        fingerprint=fingerprint or run_id,
    )


def sftp_gate3_node(state: Stage01State) -> Stage01State:
    new_state = _copy_state(state)
    log_context = {
        "run_id": new_state.get("run_id", "unknown"),
        "node": "sftp_gate3",
        "stage": "sftp_gate3",
    }

    # Treat ADLS Gen2 as the same "file pipeline" as SFTP.
    if str(new_state.get("source") or "").lower() not in {"sftp", "adls_gen2"}:
        return new_state
    if str(new_state.get("status") or "").upper() == "FAILED":
        return new_state

    auto_mode = os.getenv("ATHENA_SFTP_HITL_AUTO", "").strip().lower() in {"1", "true", "yes", "on"}
    override = str(new_state.get("gate3_decision") or new_state.get("enrichment_review_decision") or "").strip().upper()
    artifact = new_state.get("enriched_metadata") or {}

    if not auto_mode and override not in {"APPROVED", "REJECTED"}:
        logger.info("SFTP Gate 3 pending review", extra={**log_context, "event_type": "stage_start"})
        new_state["enrichment_review_status"] = "PENDING"
        new_state["enrichment_review_decision"] = "PENDING"
        new_state["status"] = "HITL_WAIT"
        return new_state

    if override == "REJECTED":
        _update_schema_review_status(
            _schema_entries(new_state),
            status="REJECTED",
            approved_by="athena_gate3",
            rejection_reason="Rejected by reviewer",
            log_context=log_context,
        )
        new_state["enrichment_review_status"] = "FAILED"
        new_state["enrichment_review_decision"] = "REJECTED"
        new_state["enrichment_review_error"] = "Rejected by reviewer"
        new_state["status"] = "FAILED"
        return new_state

    certify_sftp_gate3(
        str(new_state.get("run_id") or "unknown"),
        artifact,
        str(new_state.get("fingerprint") or new_state.get("run_id") or "unknown"),
    )
    _update_schema_review_status(
        _schema_entries(new_state),
        status="APPROVED",
        approved_by="athena_gate3",
        rejection_reason=None,
        log_context=log_context,
    )
    new_state["enrichment_review_status"] = "COMPLETED"
    new_state["enrichment_review_decision"] = "APPROVED"
    new_state["semantic_tags_reviewed"] = True
    new_state["pii_classifications_reviewed"] = True
    new_state["join_key_annotations_reviewed"] = True
    new_state["enrichment_review_artifact"] = artifact
    new_state["status"] = "IN_PROGRESS"
    logger.info("SFTP Gate 3 approved", extra={**log_context, "event_type": "stage_end"})
    return new_state
