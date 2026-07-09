from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nodes.silver_gen import silver_code_generation_node
from services.snowflake_bronze_runtime import _snowflake_connect
from services.snowflake_silver_runtime import run_snowflake_silver_scripts


def _quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _qualified_name(*parts: str) -> str:
    return ".".join(_quote_identifier(part) for part in parts if str(part or "").strip())


def _table_part(qualified: str) -> str:
    return str(qualified or "").split(".")[-1].strip().strip('"')


def _load_bronze_bundle() -> dict[str, Any]:
    path = Path("generated_code/snowflake/bronze/bronze_scripts.json")
    if not path.exists():
        raise FileNotFoundError(f"Missing latest Snowflake Bronze bundle: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _show_tables(conn: Any, database: str, schema: str) -> list[str]:
    cursor = conn.cursor()
    try:
        cursor.execute(f"SHOW TABLES IN SCHEMA {_qualified_name(database, schema)}")
        rows = cursor.fetchall()
    finally:
        cursor.close()
    return [str(row[1]) for row in rows if len(row) > 1]


def _count_table(conn: Any, table_name: str) -> int | None:
    parts = [part.strip().strip('"') for part in table_name.split(".") if part.strip()]
    if len(parts) != 3:
        return None
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {_qualified_name(*parts)}")
        row = cursor.fetchone()
        return int(row[0]) if row else None
    finally:
        cursor.close()


def main() -> int:
    load_dotenv(".env")
    os.environ["ATHENA_EXECUTE_SNOWFLAKE_SILVER"] = "true"

    bronze_db = os.getenv("SNOWFLAKE_BRONZE_CATALOG", "ATHENA_DB")
    bronze_schema = os.getenv("SNOWFLAKE_BRONZE_SCHEMA", "BRONZE")
    silver_db = os.getenv("SNOWFLAKE_SILVER_CATALOG", "ATHENA_DB")
    silver_schema = os.getenv("SNOWFLAKE_SILVER_SCHEMA", "SILVER")

    bundle = _load_bronze_bundle()
    bronze_scripts = [item for item in bundle.get("scripts") or [] if isinstance(item, dict)]
    if not bronze_scripts:
        raise ValueError("Latest Snowflake Bronze bundle has no scripts.")

    conn = _snowflake_connect()
    try:
        bronze_tables = _show_tables(conn, bronze_db, bronze_schema)
        bronze_table_keys = {name.casefold() for name in bronze_tables}
        print(f"BRONZE_TABLES {bronze_db}.{bronze_schema}: {', '.join(sorted(bronze_tables)) or '(none)'}")

        runnable_bronze_scripts = [
            script
            for script in bronze_scripts
            if f"bronze_{script.get('table')}".casefold() in bronze_table_keys
        ]
        skipped = [
            str(script.get("table"))
            for script in bronze_scripts
            if f"bronze_{script.get('table')}".casefold() not in bronze_table_keys
        ]
        if skipped:
            print(f"SKIPPED_NO_BRONZE_TABLE: {', '.join(skipped)}")
        if not runnable_bronze_scripts:
            raise ValueError("No generated Bronze scripts match existing Snowflake Bronze tables.")

        run_id = f"smoke_snowflake_silver_{bundle.get('run_id') or 'latest'}"
        silver_state = silver_code_generation_node(
            {
                "run_id": run_id,
                "target_warehouse": "snowflake",
                "bronze_generation_results": runnable_bronze_scripts,
                "enriched_metadata": {"columns": []},
            }
        )
        silver_scripts = [
            item for item in silver_state.get("silver_generation_results") or [] if isinstance(item, dict)
        ]
        print(f"GENERATED_SILVER_SCRIPTS: {len(silver_scripts)}")
        for script in silver_scripts:
            print(f"SILVER_SCRIPT {script.get('table')}: {script.get('source_table')} -> {script.get('target_table')}")

        execution_state = run_snowflake_silver_scripts(
            {
                **silver_state,
                "target_warehouse": "snowflake",
                "silver_generation_results": silver_scripts,
            }
        )
        print(f"EXECUTION_STATUS: {execution_state.get('snowflake_silver_execution_status')}")
        for result in execution_state.get("snowflake_silver_execution_results") or []:
            print(
                "EXECUTED "
                f"{result.get('table')} statements={result.get('statement_count')} target={result.get('target_table')}"
            )

        silver_tables = _show_tables(conn, silver_db, silver_schema)
        print(f"SILVER_TABLES {silver_db}.{silver_schema}: {', '.join(sorted(silver_tables)) or '(none)'}")
        for table in silver_tables:
            if table.startswith("silver_"):
                qualified = f"{silver_db}.{silver_schema}.{table}"
                print(f"SILVER_COUNT {qualified}: {_count_table(conn, qualified)}")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"SMOKE_FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        raise
