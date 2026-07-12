from __future__ import annotations

import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.snowflake_bronze_runtime import _snowflake_connect


TABLES = [
    "ATHENA_DB.SILVER.silver_claim_payment_indemnity",
    "ATHENA_DB.GOLD.fact_average_indemnity_outstanding_estimate_per_claim",
    "ATHENA_DB.GOLD.fact_total_number_of_unique_claims_processed",
]


def qname(value: str) -> str:
    return ".".join(f'"{part}"' for part in value.split("."))


def main() -> None:
    load_dotenv(".env")
    conn = _snowflake_connect()
    try:
        cur = conn.cursor()
        for table in TABLES:
            print(f"TABLE {table}")
            try:
                cur.execute(f"DESC TABLE {qname(table)}")
            except Exception as exc:
                print(f"DESC_ERROR {type(exc).__name__}: {exc}")
                continue
            rows = cur.fetchall()
            for row in rows:
                name = row[0]
                data_type = row[1]
                print(f"  {name}|{data_type}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
