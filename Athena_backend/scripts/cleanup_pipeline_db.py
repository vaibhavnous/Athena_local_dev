from __future__ import annotations

import argparse
import json
from typing import Iterable

from utilis.db import config, get_pipeline_connection


def _schema() -> str:
    return str(config["azure_sql"].get("pipeline_schema") or "metadata")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clean Athena pipeline DB rows from kpi_checkpoints, hitl_review_queue, and ai_store."
    )
    parser.add_argument("--run-id", action="append", default=[], help="Run id to delete. Repeatable.")
    parser.add_argument("--failed-only", action="store_true", help="Only select failed checkpoints.")
    parser.add_argument("--older-than-days", type=int, help="Select checkpoints older than this many days.")
    parser.add_argument("--include-ai-store", action="store_true", help="Also delete ai_store rows for selected run ids.")
    parser.add_argument("--execute", action="store_true", help="Actually delete rows. Omit for dry-run.")
    return parser.parse_args()


def _candidate_run_ids(cursor, *, run_ids: Iterable[str], failed_only: bool, older_than_days: int | None) -> list[str]:
    explicit = [str(run_id or "").strip() for run_id in run_ids if str(run_id or "").strip()]
    if explicit:
        return sorted(set(explicit))

    predicates = []
    params: list[object] = []
    if failed_only:
        predicates.append(
            "(JSON_VALUE(full_state_json, '$.status') = 'FAILED' "
            "OR NULLIF(JSON_VALUE(full_state_json, '$.failed_background_stage'), '') IS NOT NULL)"
        )
    if older_than_days is not None:
        predicates.append("checkpoint_at < DATEADD(day, ?, GETUTCDATE())")
        params.append(-abs(int(older_than_days)))

    if not predicates:
        raise SystemExit("Refusing broad cleanup. Provide --run-id, --failed-only, or --older-than-days.")

    schema = _schema()
    cursor.execute(
        f"""
        SELECT run_id
        FROM [{schema}].[kpi_checkpoints]
        WHERE {' AND '.join(predicates)}
        ORDER BY checkpoint_at
        """,
        params,
    )
    return [str(row[0]) for row in cursor.fetchall()]


def _count(cursor, table: str, run_ids: list[str]) -> int:
    if not run_ids:
        return 0
    schema = _schema()
    placeholders = ",".join("?" for _ in run_ids)
    cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}] WHERE run_id IN ({placeholders})", run_ids)
    return int(cursor.fetchone()[0] or 0)


def _delete(cursor, table: str, run_ids: list[str]) -> int:
    if not run_ids:
        return 0
    schema = _schema()
    placeholders = ",".join("?" for _ in run_ids)
    cursor.execute(f"DELETE FROM [{schema}].[{table}] WHERE run_id IN ({placeholders})", run_ids)
    return int(cursor.rowcount or 0)


def main() -> int:
    args = _parse_args()
    conn = get_pipeline_connection()
    try:
        cursor = conn.cursor()
        run_ids = _candidate_run_ids(
            cursor,
            run_ids=args.run_id,
            failed_only=bool(args.failed_only),
            older_than_days=args.older_than_days,
        )
        tables = ["hitl_review_queue", "kpi_checkpoints"]
        if args.include_ai_store:
            tables.insert(1, "ai_store")

        summary = {
            "mode": "EXECUTE" if args.execute else "DRY_RUN",
            "schema": _schema(),
            "run_ids": run_ids,
            "counts": {table: _count(cursor, table, run_ids) for table in tables},
        }
        print(json.dumps(summary, indent=2))

        if not args.execute:
            return 0

        deleted = {table: _delete(cursor, table, run_ids) for table in tables}
        conn.commit()
        print(json.dumps({"deleted": deleted}, indent=2))
        return 0
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
