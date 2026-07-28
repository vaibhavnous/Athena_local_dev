from __future__ import annotations

import json
from typing import Any, Dict, List

from nodes.memory_lookup import memory_lookup_node
from state import Stage01State
from utilis.ai_store_writer import ai_store_db_writer
from utilis.db import config, get_pipeline_connection


def _schema() -> str:
    db_config = config.get("azure_sql", {})
    return db_config.get("pipeline_schema") or db_config.get("schema_name") or "metadata"


def _prior_file_context(state: Stage01State) -> Dict[str, List[Dict[str, Any]]]:
    if state.get("skip_db"):
        return {"feeds": [], "schemas": [], "artifacts": []}

    schema = _schema()
    source = str(state.get("source") or "").lower()
    run_id = str(state.get("run_id") or "")
    conn = get_pipeline_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT TOP 50 feed_id, vendor, entity, format, status, source, remote_path, file_path
            FROM [{schema}].[file_feed_registry]
            WHERE source = ?
            ORDER BY updated_at DESC
            """,
            source,
        )
        feeds = [
            {
                "feed_id": row[0],
                "vendor": row[1],
                "entity": row[2],
                "format": row[3],
                "status": row[4],
                "source": row[5],
                "remote_path": row[6],
                "file_path": row[7],
            }
            for row in cursor.fetchall()
        ]

        feed_ids = [str(feed["feed_id"]) for feed in feeds if feed.get("feed_id")]
        schemas: List[Dict[str, Any]] = []
        if feed_ids:
            placeholders = ", ".join("?" for _ in feed_ids)
            cursor.execute(
                f"""
                SELECT TOP 100 feed_id, schema_fingerprint, version, source_type, schema_json, discovered_at
                FROM [{schema}].[file_feed_schema_registry]
                WHERE feed_id IN ({placeholders})
                ORDER BY discovered_at DESC
                """,
                tuple(feed_ids),
            )
            schemas = [
                {
                    "feed_id": row[0],
                    "schema_fingerprint": row[1],
                    "version": row[2],
                    "source_type": row[3],
                    "schema": json.loads(row[4]) if row[4] else [],
                    "discovered_at": row[5].isoformat() if hasattr(row[5], "isoformat") else str(row[5] or ""),
                }
                for row in cursor.fetchall()
            ]

        cursor.execute(
            f"""
            SELECT TOP 50 run_id, artifact_type, stage, stored_at
            FROM [{schema}].[ai_store]
            WHERE run_id <> ?
              AND (
                artifact_type LIKE 'SFTP[_]%'
                OR artifact_type IN ('ENRICHED_METADATA', 'GATE3_APPROVED_ENRICHMENT')
              )
            ORDER BY stored_at DESC
            """,
            run_id,
        )
        artifacts = [
            {
                "run_id": row[0],
                "artifact_type": row[1],
                "stage": row[2],
                "stored_at": row[3].isoformat() if hasattr(row[3], "isoformat") else str(row[3] or ""),
            }
            for row in cursor.fetchall()
        ]
        return {"feeds": feeds, "schemas": schemas, "artifacts": artifacts}
    finally:
        conn.close()


def sftp_memory_check_node(state: Stage01State) -> Stage01State:
    if str(state.get("status") or "").upper() == "FAILED":
        return state

    result = memory_lookup_node(state)
    context = {
        "run_id": result.get("run_id"),
        "source": result.get("source"),
        "connection_id": result.get("connection_id"),
        "exact_match": bool(result.get("memory_layer1")),
        "semantic_match": bool(result.get("memory_layer2")),
        "context_kpis": result.get("context_kpis") or [],
        "rejected_kpis": result.get("rejected_kpis") or [],
        **_prior_file_context(result),
    }
    ai_store_db_writer(
        run_id=str(result.get("run_id") or ""),
        stage="SFTP Memory Check",
        artifact_type="SFTP_MEMORY_CONTEXT",
        payload={**context, "fingerprint": result.get("fingerprint") or result.get("run_id")},
        schema_version="1.0",
        prompt_version="deterministic-v1",
        faithfulness_status="PASSED",
    )
    return {
        **result,
        "sftp_memory_context": context,
        "memory_check_status": "COMPLETED",
    }
