from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from utilis.db import get_client_connection, get_pipeline_connection


def check_pipeline_db() -> None:
    print("Checking pipeline DB...")
    conn = get_pipeline_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT DB_NAME() AS db_name")
        row = cursor.fetchone()
        print(f"Pipeline DB OK: {row.db_name if hasattr(row, 'db_name') else row[0]}")
    finally:
        conn.close()


def check_source_db() -> None:
    print("Checking source DB...")
    conn = get_client_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT DB_NAME() AS db_name")
        row = cursor.fetchone()
        print(f"Source DB OK: {row.db_name if hasattr(row, 'db_name') else row[0]}")
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        check_pipeline_db()
    except Exception as exc:
        print(f"Pipeline DB FAILED: {exc}")

    try:
        check_source_db()
    except Exception as exc:
        print(f"Source DB FAILED: {exc}")
