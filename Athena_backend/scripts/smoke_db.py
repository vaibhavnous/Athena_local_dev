from __future__ import annotations

import sys
from pathlib import Path
from typing import Iterable, Set


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def _existing_tables(cursor, *, schema: str) -> Set[str]:
    cursor.execute(
        """
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ?
        """,
        (schema,),
    )
    return {row[0] for row in cursor.fetchall()}


def _check_required_tables(existing: Set[str], required: Iterable[str]) -> list[str]:
    return [name for name in required if name not in existing]


def main() -> int:
    # Import inside main so failures show a clean error message.
    from utilis.db import config, get_client_connection, get_pipeline_connection

    pipeline_schema = config["azure_sql"].get("pipeline_schema") or config["azure_sql"].get("schema_name") or "dbo"
    required_pipeline_tables = ("ai_store", "hitl_review_queue", "kpi_checkpoints")

    failures: list[str] = []

    print("== pipeline db ==")
    try:
        conn = get_pipeline_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        existing = _existing_tables(cur, schema=pipeline_schema)
        missing = _check_required_tables(existing, required_pipeline_tables)
        if missing:
            failures.append(f"pipeline schema '{pipeline_schema}' missing tables: {', '.join(missing)}")
            print(f"FAIL missing tables in schema '{pipeline_schema}': {', '.join(missing)}")
        else:
            print(f"ok (schema='{pipeline_schema}', tables ok)")
        conn.close()
    except Exception as exc:
        failures.append(f"pipeline connection failed: {type(exc).__name__}: {exc}")
        print(f"FAIL {type(exc).__name__}: {exc}")

    print("== source db ==")
    try:
        conn = get_client_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        print("ok")
        conn.close()
    except Exception as exc:
        failures.append(f"source connection failed: {type(exc).__name__}: {exc}")
        print(f"FAIL {type(exc).__name__}: {exc}")

    if failures:
        print("== summary ==")
        for failure in failures:
            print(f"- {failure}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
