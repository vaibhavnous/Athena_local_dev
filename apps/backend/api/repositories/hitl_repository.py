from __future__ import annotations

from typing import Any, Dict, List, Optional

from utilis.db import config, get_connection

from api import utils as api_utils


def pipeline_schema() -> str:
    return (
        config["azure_sql"].get("pipeline_schema")
        or config["azure_sql"].get("schema_name")
        or "metadata"
    )


def fetch_hitl_rows(run_id: str, status: Optional[str] = None) -> List[Dict[str, Any]]:
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
            FROM [{pipeline_schema()}].[hitl_review_queue]
            WHERE run_id = ? AND gate_number = ?{status_clause}
            ORDER BY queued_at
            """,
            params,
        )
        rows = cursor.fetchall()
    finally:
        conn.close()

    return [
        {
            "item_id": row.item_id,
            "gate_status": row.gate_status,
            "original_content": api_utils.json_loads(row.original_content) or {},
            "edited_content": api_utils.json_loads(row.edited_content) or {},
            "rejection_reason": row.rejection_reason,
            "queued_at": row.queued_at,
            "decided_at": row.decided_at,
        }
        for row in rows
    ]
