from __future__ import annotations

import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.snowflake_bronze_runtime import _snowflake_connect


def quote(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


load_dotenv(".env")
conn = _snowflake_connect()
try:
    cursor = conn.cursor()
    try:
        for table in ["bronze_claim_information", "bronze_policy_transactions", "bronze_measures"]:
            cursor.execute(f"DESCRIBE TABLE {quote('ATHENA_DB')}.{quote('BRONZE')}.{quote(table)}")
            columns = [str(row[0]) for row in cursor.fetchall()]
            interesting = [name for name in columns if "ref" in name.lower() or "rer" in name.lower()]
            print(f"{table}: {interesting}")
    finally:
        cursor.close()
finally:
    conn.close()
