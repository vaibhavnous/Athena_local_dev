from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List


BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from utilis.db import get_pipeline_connection, config


def _pipeline_schema() -> str:
    return (
        config["azure_sql"].get("pipeline_schema")
        or config["azure_sql"].get("schema_name")
        or "metadata"
    )


def _fetch_all(cursor, sql: str, params: tuple[Any, ...]) -> List[Dict[str, Any]]:
    cursor.execute(sql, params)
    columns = [column[0] for column in cursor.description]
    rows = cursor.fetchall()
    return [dict(zip(columns, row)) for row in rows]


def _table_columns(cursor, schema: str, table: str) -> set[str]:
    cursor.execute(
        """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """,
        (schema, table),
    )
    return {str(row[0]) for row in cursor.fetchall()}


def _fetch_table_for_run(
    cursor,
    *,
    schema: str,
    table: str,
    run_id: str,
    preferred_order_columns: tuple[str, ...],
) -> List[Dict[str, Any]]:
    columns = _table_columns(cursor, schema, table)
    if not columns or "run_id" not in columns:
        return []

    order_column = next((column for column in preferred_order_columns if column in columns), None)
    order_clause = f" ORDER BY [{order_column}] ASC" if order_column else ""
    return _fetch_all(
        cursor,
        f"SELECT * FROM [{schema}].[{table}] WHERE run_id = ?{order_clause}",
        (run_id,),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch all persistent run data for a given run_id.")
    parser.add_argument("run_id", help="Run ID to fetch")
    args = parser.parse_args()

    schema = _pipeline_schema()
    run_id = args.run_id
    conn = get_pipeline_connection()
    try:
        cursor = conn.cursor()

        result: Dict[str, Any] = {
            "run_id": run_id,
            "schema": schema,
            "kpi_checkpoint": _fetch_all(
                cursor,
                f"""
                SELECT TOP 1
                    run_id,
                    checkpoint_at,
                    full_state_json
                FROM [{schema}].[kpi_checkpoints]
                WHERE run_id = ?
                ORDER BY checkpoint_at DESC
                """,
                (run_id,),
            ),
            "ai_store": _fetch_all(
                cursor,
                f"""
                SELECT
                    run_id,
                    fingerprint,
                    stage,
                    artifact_type,
                    schema_version,
                    prompt_version,
                    faithfulness_status,
                    faithfulness_warn_count,
                    retry_count,
                    token_count,
                    input_tokens,
                    output_tokens,
                    cost_usd,
                    stored_at,
                    payload
                FROM [{schema}].[ai_store]
                WHERE run_id = ?
                ORDER BY stored_at ASC
                """,
                (run_id,),
            ),
            "hitl_review_queue": _fetch_all(
                cursor,
                f"""
                SELECT *
                FROM [{schema}].[hitl_review_queue]
                WHERE run_id = ?
                ORDER BY {(
                    '[created_at]' if 'created_at' in _table_columns(cursor, schema, 'hitl_review_queue')
                    else '[queued_at]' if 'queued_at' in _table_columns(cursor, schema, 'hitl_review_queue')
                    else '[decided_at]' if 'decided_at' in _table_columns(cursor, schema, 'hitl_review_queue')
                    else '[id]' if 'id' in _table_columns(cursor, schema, 'hitl_review_queue')
                    else 'run_id'
                )} ASC
                """,
                (run_id,),
            ),
            "pipeline_run_log": _fetch_table_for_run(
                cursor,
                schema=schema,
                table="pipeline_run_log",
                run_id=run_id,
                preferred_order_columns=("created_at", "started_at", "completed_at", "id"),
            ),
            "brd_run_registry": _fetch_table_for_run(
                cursor,
                schema=schema,
                table="brd_run_registry",
                run_id=run_id,
                preferred_order_columns=("timestamp", "created_at", "id"),
            ),
            "bronze_execution_plan": _fetch_table_for_run(
                cursor,
                schema=schema,
                table="bronze_execution_plan",
                run_id=run_id,
                preferred_order_columns=("created_at", "updated_at", "id"),
            ),
        }

        print(json.dumps(result, indent=2, default=str))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
