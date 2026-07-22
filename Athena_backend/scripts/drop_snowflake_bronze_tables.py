from __future__ import annotations

import argparse
import os
from pathlib import Path

from cleanup_snowflake_layers import connect, load_dotenv, quote_ident


def bronze_target() -> tuple[str, str]:
    return (
        os.getenv("SNOWFLAKE_BRONZE_CATALOG") or os.getenv("SNOWFLAKE_DATABASE") or "ATHENA_DB",
        os.getenv("SNOWFLAKE_BRONZE_SCHEMA") or "BRONZE",
    )


def fetch_bronze_tables(cur, database: str, schema: str) -> list[str]:
    cur.execute(f"SHOW TABLES IN SCHEMA {quote_ident(database)}.{quote_ident(schema)}")
    return [str(row[1]) for row in cur.fetchall()]


def main() -> int:
    parser = argparse.ArgumentParser(description="Drop all tables from the configured Snowflake Bronze schema.")
    parser.add_argument("--execute", action="store_true", help="Actually drop tables. Omit for dry-run.")
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    load_dotenv(root / ".env")

    database, schema = bronze_target()
    conn = connect()
    dropped = 0
    try:
        cur = conn.cursor()
        tables = fetch_bronze_tables(cur, database, schema)
        if not tables:
            print(f"EMPTY {database}.{schema}")
        for table in tables:
            qualified = f"{quote_ident(database)}.{quote_ident(schema)}.{quote_ident(table)}"
            sql = f"DROP TABLE IF EXISTS {qualified}"
            if args.execute:
                cur.execute(sql)
                dropped += 1
                print(f"DROPPED TABLE {database}.{schema}.{table}")
            else:
                print(f"DRY_RUN {sql}")
        if args.execute:
            conn.commit()
    finally:
        conn.close()

    print(f"SUMMARY mode={'EXECUTE' if args.execute else 'DRY_RUN'} tables={len(tables)} dropped={dropped} target={database}.{schema}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
