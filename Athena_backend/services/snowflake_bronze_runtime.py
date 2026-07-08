from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence
from urllib.parse import urlparse

from utilis.db import get_client_connection


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def snowflake_bronze_execution_enabled() -> bool:
    return _env_bool("ATHENA_EXECUTE_SNOWFLAKE_BRONZE", False)


def snowflake_bronze_source_load_enabled() -> bool:
    return _env_bool("ATHENA_SNOWFLAKE_BRONZE_LOAD_SOURCE", False)


def _normalize_account(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return raw
    parsed = urlparse(raw)
    if parsed.scheme and parsed.netloc:
        parts = [part for part in parsed.path.split("/") if part]
        if parsed.netloc == "app.snowflake.com" and len(parts) >= 2:
            return f"{parts[0]}-{parts[1]}"
        return parsed.netloc.split(".snowflakecomputing.com", 1)[0]
    return raw


def _get_snowflake_connector():
    try:
        import snowflake.connector

        return snowflake.connector
    except Exception as exc:
        raise RuntimeError(
            "snowflake-connector-python is unavailable. Install backend requirements before enabling "
            "ATHENA_EXECUTE_SNOWFLAKE_BRONZE."
        ) from exc


def _snowflake_connect():
    connector = _get_snowflake_connector()
    required = {
        "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER"),
        "SNOWFLAKE_PASSWORD": os.getenv("SNOWFLAKE_PASSWORD"),
        "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT"),
    }
    missing = [name for name, value in required.items() if not str(value or "").strip()]
    if missing:
        raise RuntimeError("Missing Snowflake configuration: " + ", ".join(missing))

    kwargs = {
        "user": required["SNOWFLAKE_USER"],
        "password": required["SNOWFLAKE_PASSWORD"],
        "account": _normalize_account(str(required["SNOWFLAKE_ACCOUNT"])),
        "autocommit": True,
    }
    for env_name, key in (
        ("SNOWFLAKE_WAREHOUSE", "warehouse"),
        ("SNOWFLAKE_ROLE", "role"),
        ("SNOWFLAKE_DATABASE", "database"),
        ("SNOWFLAKE_SCHEMA", "schema"),
    ):
        value = os.getenv(env_name)
        if str(value or "").strip():
            kwargs[key] = value
    return connector.connect(**kwargs)


def _snowflake_quote_identifier(value: str) -> str:
    cleaned = str(value or "").strip()
    if not cleaned:
        raise ValueError("Snowflake identifier cannot be empty.")
    return '"' + cleaned.replace('"', '""') + '"'


def _snowflake_qualified_name(*parts: str) -> str:
    return ".".join(_snowflake_quote_identifier(part) for part in parts if str(part or "").strip())


def _sqlserver_quote_identifier(value: str) -> str:
    cleaned = str(value or "").strip()
    if not cleaned:
        raise ValueError("SQL Server identifier cannot be empty.")
    return "[" + cleaned.replace("]", "]]") + "]"


def _source_select_sql(schema_name: str, table_name: str, limit: int) -> str:
    table_ref = f"{_sqlserver_quote_identifier(schema_name)}.{_sqlserver_quote_identifier(table_name)}"
    if limit > 0:
        return f"SELECT TOP ({limit}) * FROM {table_ref}"
    return f"SELECT * FROM {table_ref}"


def _batch_size() -> int:
    raw = os.getenv("ATHENA_SNOWFLAKE_BRONZE_BATCH_SIZE", "1000")
    try:
        return max(1, int(raw))
    except ValueError:
        return 1000


def _source_load_limit() -> int:
    raw = os.getenv("ATHENA_SNOWFLAKE_BRONZE_SOURCE_LOAD_LIMIT", "0")
    try:
        return max(0, int(raw))
    except ValueError:
        return 0


def _string_rows(rows: Iterable[Sequence[Any]]) -> List[tuple[Any, ...]]:
    return [tuple(None if value is None else str(value) for value in row) for row in rows]


def load_azure_sql_table_to_snowflake(script: Dict[str, Any], snowflake_conn: Any) -> Dict[str, Any]:
    database_name = str(script.get("database_name") or "insurance")
    schema_name = str(script.get("schema_name") or "dbo")
    table_name = str(script.get("table") or script.get("table_name") or "").strip()
    if not table_name:
        raise ValueError("Snowflake bronze source load is missing table name.")

    source_conn = get_client_connection(database_name)
    inserted_rows = 0
    try:
        source_cursor = source_conn.cursor()
        source_cursor.execute(_source_select_sql(schema_name, table_name, _source_load_limit()))
        columns = [str(column[0]) for column in source_cursor.description or []]
        if not columns:
            raise ValueError(f"Azure SQL returned no columns for {database_name}.{schema_name}.{table_name}.")

        landing_table = _snowflake_qualified_name(database_name, schema_name, table_name)
        column_defs = ", ".join(f"{_snowflake_quote_identifier(column)} VARCHAR" for column in columns)
        column_list = ", ".join(_snowflake_quote_identifier(column) for column in columns)
        placeholders = ", ".join(["%s"] * len(columns))

        # ponytail: source landing is raw VARCHAR; generated bronze SQL owns all typing via TRY_CAST.
        snowflake_cursor = snowflake_conn.cursor()
        try:
            snowflake_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {_snowflake_quote_identifier(database_name)}")
            snowflake_cursor.execute(
                f"CREATE SCHEMA IF NOT EXISTS {_snowflake_qualified_name(database_name, schema_name)}"
            )
            snowflake_cursor.execute(f"CREATE TABLE IF NOT EXISTS {landing_table} ({column_defs})")

            insert_sql = f"INSERT INTO {landing_table} ({column_list}) VALUES ({placeholders})"
            while True:
                rows = source_cursor.fetchmany(_batch_size())
                if not rows:
                    break
                values = _string_rows(rows)
                snowflake_cursor.executemany(insert_sql, values)
                inserted_rows += len(values)
        finally:
            snowflake_cursor.close()
    finally:
        source_conn.close()

    return {
        "source_table": f"{database_name}.{schema_name}.{table_name}",
        "snowflake_landing_table": f"{database_name}.{schema_name}.{table_name}",
        "rows_loaded": inserted_rows,
    }


def _read_sql_file(path_value: Any) -> str:
    path = Path(str(path_value or ""))
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"Generated Snowflake bronze SQL not found: {path}")
    return path.read_text(encoding="utf-8")


def validate_snowflake_bronze_script(script: Dict[str, Any]) -> str:
    from nodes.bronze_gen import _snowflake_qualified_name, validate_snowflake_bronze_sql

    sql = _read_sql_file(script.get("script_path"))
    table_name = str(script.get("table") or script.get("table_name") or "").strip()
    database_name = str(script.get("database_name") or "insurance")
    schema_name = str(script.get("schema_name") or "dbo")
    bronze_catalog = str(script.get("bronze_catalog") or os.getenv("BRONZE_CATALOG", "main"))
    bronze_schema = str(script.get("bronze_schema") or os.getenv("BRONZE_SCHEMA", "bronze"))
    validate_snowflake_bronze_sql(
        sql,
        source_table=_snowflake_qualified_name(database_name, schema_name, table_name) if table_name else None,
        target_table=_snowflake_qualified_name(bronze_catalog, bronze_schema, f"bronze_{table_name}") if table_name else None,
    )
    return sql


def execute_snowflake_sql_file(script: Dict[str, Any], snowflake_conn: Any) -> Dict[str, Any]:
    sql = validate_snowflake_bronze_script(script)
    cursors = snowflake_conn.execute_string(sql, return_cursors=True)
    statement_count = len(list(cursors or []))
    return {
        "table": script.get("table"),
        "script_path": script.get("script_path"),
        "statement_count": statement_count,
        "status": "COMPLETED",
    }


def run_snowflake_bronze_scripts(state: Dict[str, Any]) -> Dict[str, Any]:
    target_warehouse = str(state.get("target_warehouse") or "databricks").lower()
    if target_warehouse != "snowflake":
        return state
    if not snowflake_bronze_execution_enabled():
        return {**state, "snowflake_bronze_execution_status": "DISABLED"}

    scripts = [item for item in state.get("bronze_generation_results") or [] if isinstance(item, dict)]
    if not scripts:
        raise ValueError("Snowflake bronze execution enabled but no generated bronze scripts were found.")

    for script in scripts:
        validate_snowflake_bronze_script(script)

    load_source = snowflake_bronze_source_load_enabled()
    loaded_sources: List[Dict[str, Any]] = []
    executed_scripts: List[Dict[str, Any]] = []
    snowflake_conn = _snowflake_connect()
    try:
        for script in scripts:
            if load_source:
                loaded_sources.append(load_azure_sql_table_to_snowflake(script, snowflake_conn))
            executed_scripts.append(execute_snowflake_sql_file(script, snowflake_conn))
    finally:
        snowflake_conn.close()

    return {
        **state,
        "snowflake_bronze_execution_status": "COMPLETED",
        "snowflake_bronze_load_source_enabled": load_source,
        "snowflake_bronze_source_load_results": loaded_sources,
        "snowflake_bronze_execution_results": executed_scripts,
        "snowflake_bronze_executed_at": datetime.now(timezone.utc).isoformat(),
    }
