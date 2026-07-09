from __future__ import annotations

import os
import re
import csv
import io
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence
from urllib.parse import urlparse

from services.external_execution_progress import save_external_execution_progress
from utilis.db import get_client_connection
from utilis.logger import logger


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def snowflake_bronze_execution_enabled() -> bool:
    return _env_bool("ATHENA_EXECUTE_SNOWFLAKE_BRONZE", False)


def snowflake_bronze_source_load_enabled() -> bool:
    return _env_bool("ATHENA_SNOWFLAKE_BRONZE_LOAD_SOURCE", False)


def _source_mode() -> str:
    return str(os.getenv("ATHENA_SNOWFLAKE_BRONZE_SOURCE_MODE") or "azure_sql").strip().lower()


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


def _snowflake_string_literal(value: str) -> str:
    return "'" + str(value or "").replace("'", "''") + "'"


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
    raw = os.getenv("ATHENA_SNOWFLAKE_BRONZE_BATCH_SIZE", "5000")
    try:
        return max(1, int(raw))
    except ValueError:
        return 5000


def _progress_log_interval() -> int:
    raw = os.getenv("ATHENA_SNOWFLAKE_BRONZE_PROGRESS_EVERY_ROWS", "25000")
    try:
        return max(0, int(raw))
    except ValueError:
        return 25000


def _source_load_limit() -> int:
    raw = os.getenv("ATHENA_SNOWFLAKE_BRONZE_SOURCE_LOAD_LIMIT", "0")
    try:
        return max(0, int(raw))
    except ValueError:
        return 0


def _string_rows(rows: Iterable[Sequence[Any]]) -> List[tuple[Any, ...]]:
    return [tuple(None if value is None else str(value) for value in row) for row in rows]


def _table_name(script: Dict[str, Any]) -> str:
    table_name = str(script.get("table") or script.get("table_name") or script.get("entity") or "").strip()
    if not table_name:
        raise ValueError("Snowflake bronze source load is missing table name.")
    return table_name


def _database_name(script: Dict[str, Any]) -> str:
    return str(script.get("database_name") or "insurance").strip() or "insurance"


def _schema_name(script: Dict[str, Any]) -> str:
    return str(script.get("schema_name") or "dbo").strip() or "dbo"


def _log_context(run_id: Any, *, table: str | None = None, step_name: str = "snowflake_bronze") -> Dict[str, Any]:
    context = {
        "run_id": str(run_id or ""),
        "node": "bronze_code_execution",
        "stage": "bronze_code_execution",
        "step_name": step_name,
    }
    if table:
        context["table"] = table
    return context


def load_azure_sql_table_to_snowflake(
    script: Dict[str, Any],
    snowflake_conn: Any,
    *,
    run_id: Any = None,
) -> Dict[str, Any]:
    database_name = _database_name(script)
    schema_name = _schema_name(script)
    table_name = _table_name(script)

    source_conn = get_client_connection(database_name)
    inserted_rows = 0
    progress_every = _progress_log_interval()
    next_progress_log = progress_every
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
            snowflake_cursor.execute(f"CREATE OR REPLACE TABLE {landing_table} ({column_defs})")

            insert_sql = f"INSERT INTO {landing_table} ({column_list}) VALUES ({placeholders})"
            while True:
                rows = source_cursor.fetchmany(_batch_size())
                if not rows:
                    break
                values = _string_rows(rows)
                snowflake_cursor.executemany(insert_sql, values)
                inserted_rows += len(values)
                if progress_every and inserted_rows >= next_progress_log:
                    logger.info(
                        "Snowflake Bronze source load progress for %s: rows_loaded=%s",
                        f"{database_name}.{schema_name}.{table_name}",
                        inserted_rows,
                        extra=_log_context(run_id, table=table_name, step_name="source_load_progress"),
                    )
                    next_progress_log += progress_every
        finally:
            snowflake_cursor.close()
    finally:
        source_conn.close()

    return {
        "source_table": f"{database_name}.{schema_name}.{table_name}",
        "snowflake_landing_table": f"{database_name}.{schema_name}.{table_name}",
        "rows_loaded": inserted_rows,
    }


def _adls_stage_database() -> str:
    return str(os.getenv("SNOWFLAKE_ADLS_STAGE_DB") or os.getenv("BRONZE_CATALOG") or "ATHENA_DB").strip()


def _adls_stage_schema() -> str:
    return str(os.getenv("SNOWFLAKE_ADLS_STAGE_SCHEMA") or os.getenv("BRONZE_SCHEMA") or "BRONZE").strip()


def _adls_stage_name() -> str:
    return str(os.getenv("SNOWFLAKE_ADLS_STAGE_NAME") or "ADLS_INSURANCE_STAGE").strip()


def _adls_file_format_name() -> str:
    return str(os.getenv("SNOWFLAKE_ADLS_FILE_FORMAT") or "ADLS_CSV_FORMAT").strip()


def _adls_integration_name() -> str:
    return str(os.getenv("SNOWFLAKE_ADLS_INTEGRATION") or "ADLS_INSURANCE_INT").strip()


def _adls_stage_url() -> str:
    return str(
        os.getenv("SNOWFLAKE_ADLS_STAGE_URL")
        or "azure://atheastorage.blob.core.windows.net/athena/Insurance/"
    ).strip()


def _adls_folder_for_script(script: Dict[str, Any]) -> str:
    folder = str(script.get("adls_folder") or script.get("landing_path") or _table_name(script)).strip().strip("/")
    return folder


def _stage_ref(*, include_name: bool = True) -> str:
    parts = [_adls_stage_database(), _adls_stage_schema()]
    if include_name:
        parts.append(_adls_stage_name())
    return _snowflake_qualified_name(*parts)


def _file_format_ref() -> str:
    return _snowflake_qualified_name(_adls_stage_database(), _adls_stage_schema(), _adls_file_format_name())


def ensure_adls_stage(snowflake_conn: Any) -> Dict[str, Any]:
    stage_schema = _snowflake_qualified_name(_adls_stage_database(), _adls_stage_schema())
    cursor = snowflake_conn.cursor()
    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {_snowflake_quote_identifier(_adls_stage_database())}")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {stage_schema}")
        cursor.execute(
            f"""
CREATE FILE FORMAT IF NOT EXISTS {_file_format_ref()}
    TYPE = CSV
    PARSE_HEADER = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    NULL_IF = ('', 'NULL', 'null')
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
            """.strip()
        )
        cursor.execute(
            f"""
CREATE STAGE IF NOT EXISTS {_stage_ref()}
    URL = {_snowflake_string_literal(_adls_stage_url())}
    STORAGE_INTEGRATION = {_adls_integration_name()}
    FILE_FORMAT = {_file_format_ref()}
            """.strip()
        )
    finally:
        cursor.close()

    return {
        "stage": _stage_ref(),
        "file_format": _file_format_ref(),
        "stage_url": _adls_stage_url(),
        "storage_integration": _adls_integration_name(),
    }


def _source_columns_from_script_metadata(script: Dict[str, Any]) -> List[str]:
    columns: List[str] = []
    for column in script.get("source_columns") or script.get("approved_schema") or []:
        if isinstance(column, dict):
            name = column.get("source") or column.get("column_name") or column.get("name")
        else:
            name = column
        if str(name or "").strip():
            columns.append(str(name).strip())
    return columns


def _source_columns_from_sql(script: Dict[str, Any]) -> List[str]:
    sql = _read_sql_file(script.get("script_path"))
    return list(dict.fromkeys(re.findall(r'src\."([^"]+)"', sql)))


def _landing_columns(script: Dict[str, Any]) -> List[str]:
    columns = _source_columns_from_script_metadata(script) or _source_columns_from_sql(script)
    return list(dict.fromkeys(columns))


def load_adls_table_to_snowflake(script: Dict[str, Any], snowflake_conn: Any) -> Dict[str, Any]:
    database_name = _database_name(script)
    schema_name = _schema_name(script)
    table_name = _table_name(script)
    landing_table = _snowflake_qualified_name(database_name, schema_name, table_name)
    columns = _landing_columns(script)
    folder = _adls_folder_for_script(script)
    stage_path = f"@{_stage_ref()}/{folder}/"

    cursor = snowflake_conn.cursor()
    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {_snowflake_quote_identifier(database_name)}")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {_snowflake_qualified_name(database_name, schema_name)}")
        if columns:
            column_defs = ", ".join(f"{_snowflake_quote_identifier(column)} VARCHAR" for column in columns)
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {landing_table} ({column_defs})")
        else:
            # ponytail: when generated bronze uses src.*, let Snowflake infer landing columns from ADLS headers.
            cursor.execute(
                f"""
CREATE TABLE IF NOT EXISTS {landing_table}
USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => {_snowflake_string_literal(stage_path)},
            FILE_FORMAT => {_snowflake_string_literal(_file_format_ref())}
        )
    )
)
                """.strip()
            )
        cursor.execute(
            f"""
COPY INTO {landing_table}
FROM {stage_path}
FILE_FORMAT = (FORMAT_NAME = {_file_format_ref()})
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            """.strip()
        )
        copy_rows = cursor.fetchall() if getattr(cursor, "description", None) else []
    finally:
        cursor.close()

    return {
        "source_table": f"{database_name}.{schema_name}.{table_name}",
        "snowflake_landing_table": f"{database_name}.{schema_name}.{table_name}",
        "adls_stage_path": stage_path,
        "copy_result_count": len(copy_rows or []),
    }


def _adls_file_system() -> str:
    return str(os.getenv("ADLS_FILE_SYSTEM") or os.getenv("DLS_FILE_SYSTEM") or "athena").strip()


def _adls_account_url() -> str:
    raw = str(os.getenv("ADLS_ACCOUNT_URL") or "").strip()
    if raw:
        return raw
    account = str(os.getenv("ADLS_ACCOUNT_NAME") or "atheastorage").strip()
    return f"https://{account}.dfs.core.windows.net"


def _adls_source_root() -> str:
    return str(
        os.getenv("ADLS_SOURCE_ROOT")
        or os.getenv("ADLS_VENDOR_ROOT")
        or os.getenv("ADLS_PREFIX")
        or ""
    ).strip().strip("/")


def _adls_python_folder_for_script(script: Dict[str, Any]) -> str:
    folder = str(script.get("adls_folder") or script.get("landing_path") or _table_name(script)).strip().strip("/")
    root = _adls_source_root()
    return f"{root}/{folder}".strip("/") if root and not folder.startswith(root + "/") else folder


def _get_adls_file_system_client():
    try:
        from azure.identity import ClientSecretCredential
        from azure.storage.filedatalake import DataLakeServiceClient
    except Exception as exc:
        raise RuntimeError(
            "Azure ADLS libraries are unavailable. Install backend requirements before using "
            "ATHENA_SNOWFLAKE_BRONZE_SOURCE_MODE=adls_python."
        ) from exc

    required = {
        "AZURE_TENANT_ID": os.getenv("AZURE_TENANT_ID"),
        "AZURE_CLIENT_ID": os.getenv("AZURE_CLIENT_ID"),
        "AZURE_CLIENT_SECRET": os.getenv("AZURE_CLIENT_SECRET"),
    }
    missing = [name for name, value in required.items() if not str(value or "").strip()]
    if missing:
        raise RuntimeError("Missing ADLS service principal configuration: " + ", ".join(missing))

    credential = ClientSecretCredential(
        tenant_id=str(required["AZURE_TENANT_ID"]),
        client_id=str(required["AZURE_CLIENT_ID"]),
        client_secret=str(required["AZURE_CLIENT_SECRET"]),
    )
    service_client = DataLakeServiceClient(account_url=_adls_account_url(), credential=credential)
    return service_client.get_file_system_client(file_system=_adls_file_system())


def _adls_csv_paths(file_system_client: Any, folder: str) -> List[str]:
    if folder.lower().endswith((".csv", ".txt")):
        return [folder]
    paths = []
    try:
        for path in file_system_client.get_paths(path=folder, recursive=True):
            if getattr(path, "is_directory", False):
                continue
            name = str(getattr(path, "name", "") or "")
            if name.lower().endswith((".csv", ".txt")):
                paths.append(name)
    except Exception:
        candidate = f"{folder}.csv"
        try:
            file_system_client.get_file_client(candidate).get_file_properties()
            return [candidate]
        except Exception:
            raise
    if not paths:
        raise ValueError(f"No CSV/TXT files found in ADLS folder: {_adls_file_system()}/{folder}")
    return paths


def _download_adls_text(file_system_client: Any, path: str) -> str:
    file_client = file_system_client.get_file_client(path)
    payload = file_client.download_file().readall()
    if isinstance(payload, bytes):
        return payload.decode(os.getenv("ADLS_TEXT_ENCODING", "utf-8-sig"))
    return str(payload)


def _snowflake_insert_batch_size() -> int:
    raw = os.getenv("ATHENA_SNOWFLAKE_ADLS_INSERT_BATCH_SIZE", "1000")
    try:
        return max(1, int(raw))
    except ValueError:
        return 1000


def _adls_python_row_limit() -> int:
    raw = os.getenv("ATHENA_SNOWFLAKE_ADLS_ROW_LIMIT", "0")
    try:
        return max(0, int(raw))
    except ValueError:
        return 0


def _insert_rows(cursor: Any, insert_sql: str, rows: List[tuple[Any, ...]], inserted_rows: int) -> int:
    if not rows:
        return inserted_rows
    cursor.executemany(insert_sql, rows)
    return inserted_rows + len(rows)


def load_adls_python_table_to_snowflake(script: Dict[str, Any], snowflake_conn: Any) -> Dict[str, Any]:
    database_name = _database_name(script)
    schema_name = _schema_name(script)
    table_name = _table_name(script)
    landing_table = _snowflake_qualified_name(database_name, schema_name, table_name)
    folder = _adls_python_folder_for_script(script)
    file_system_client = _get_adls_file_system_client()
    paths = _adls_csv_paths(file_system_client, folder)
    row_limit = _adls_python_row_limit()

    cursor = snowflake_conn.cursor()
    inserted_rows = 0
    created_table = False
    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {_snowflake_quote_identifier(database_name)}")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {_snowflake_qualified_name(database_name, schema_name)}")

        for path in paths:
            reader = csv.DictReader(io.StringIO(_download_adls_text(file_system_client, path)))
            columns = [str(column or "").strip() for column in (reader.fieldnames or []) if str(column or "").strip()]
            if not columns:
                continue

            if not created_table:
                column_defs = ", ".join(f"{_snowflake_quote_identifier(column)} VARCHAR" for column in columns)
                cursor.execute(f"CREATE TABLE IF NOT EXISTS {landing_table} ({column_defs})")
                created_table = True

            column_list = ", ".join(_snowflake_quote_identifier(column) for column in columns)
            placeholders = ", ".join(["%s"] * len(columns))
            insert_sql = f"INSERT INTO {landing_table} ({column_list}) VALUES ({placeholders})"
            batch: List[tuple[Any, ...]] = []
            for row in reader:
                if row_limit and inserted_rows + len(batch) >= row_limit:
                    break
                batch.append(tuple(row.get(column) for column in columns))
                if len(batch) >= _snowflake_insert_batch_size():
                    inserted_rows = _insert_rows(cursor, insert_sql, batch, inserted_rows)
                    batch = []
            inserted_rows = _insert_rows(cursor, insert_sql, batch, inserted_rows)
            if row_limit and inserted_rows >= row_limit:
                break
    finally:
        cursor.close()

    if not created_table:
        raise ValueError(f"No header rows found in ADLS folder: {_adls_file_system()}/{folder}")

    return {
        "source_table": f"{database_name}.{schema_name}.{table_name}",
        "snowflake_landing_table": f"{database_name}.{schema_name}.{table_name}",
        "adls_file_system": _adls_file_system(),
        "adls_folder": folder,
        "files_loaded": len(paths),
        "rows_loaded": inserted_rows,
        "row_limit": row_limit,
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


def _script_key(script: Dict[str, Any]) -> str:
    return ".".join([_database_name(script), _schema_name(script), _table_name(script)])


def _casefold_script_key(script: Dict[str, Any]) -> str:
    return _script_key(script).casefold()


def _approved_review_scripts(state: Dict[str, Any], review_artifact: Dict[str, Any] | None) -> List[Dict[str, Any]]:
    scripts = [item for item in state.get("bronze_generation_results") or [] if isinstance(item, dict)]
    feeds = [item for item in (review_artifact or {}).get("feeds") or [] if isinstance(item, dict)]
    if not feeds:
        return scripts

    scripts_by_key = {_script_key(script): script for script in scripts}
    scripts_by_casefolded_key: Dict[str, Dict[str, Any] | None] = {}
    for script in scripts:
        key = _casefold_script_key(script)
        if key in scripts_by_casefolded_key:
            scripts_by_casefolded_key[key] = None
        else:
            scripts_by_casefolded_key[key] = script

    approved: List[Dict[str, Any]] = []
    for feed in feeds:
        if str(feed.get("review_status") or "").upper() != "APPROVED":
            continue
        key = _script_key(feed)
        script = scripts_by_key.get(key)
        if script is None:
            script = scripts_by_casefolded_key.get(_casefold_script_key(feed))
        if script is None:
            raise ValueError(f"Approved Bronze review item has no generated script: {key}")
        approved.append({**script, **feed})
    return approved


def run_snowflake_bronze_scripts(
    state: Dict[str, Any],
    *,
    review_artifact: Dict[str, Any] | None = None,
    approved_only: bool = False,
) -> Dict[str, Any]:
    run_id = state.get("run_id")
    target_warehouse = str(state.get("target_warehouse") or "databricks").lower()
    if target_warehouse != "snowflake":
        return state
    if not snowflake_bronze_execution_enabled():
        logger.info(
            "Snowflake Bronze execution disabled; generated scripts remain review artifacts",
            extra=_log_context(run_id, step_name="bronze_execution_disabled"),
        )
        return {**state, "snowflake_bronze_execution_status": "DISABLED"}

    scripts = _approved_review_scripts(state, review_artifact) if approved_only else [
        item for item in state.get("bronze_generation_results") or [] if isinstance(item, dict)
    ]
    if not scripts:
        raise ValueError("Snowflake bronze execution enabled but no approved generated bronze scripts were found.")

    for script in scripts:
        validate_snowflake_bronze_script(script)

    load_source = snowflake_bronze_source_load_enabled()
    source_mode = _source_mode()
    loaded_sources: List[Dict[str, Any]] = []
    executed_scripts: List[Dict[str, Any]] = []
    stage_key = "bronze_code_execution"
    logger.info(
        "Starting Snowflake Bronze execution in external Snowflake warehouse: total_tables=%d tables=%s source_load=%s source_mode=%s",
        len(scripts),
        ", ".join(_table_name(script) for script in scripts),
        load_source,
        source_mode,
        extra=_log_context(run_id, step_name="bronze_execution_start"),
    )
    state = save_external_execution_progress(
        state,
        run_id=run_id,
        layer="bronze",
        stage_key=stage_key,
        status="RUNNING",
        total_count=len(scripts),
        completed_count=0,
        message=f"Executing Bronze scripts in Snowflake: 0/{len(scripts)} completed.",
    )
    snowflake_conn = _snowflake_connect()
    try:
        if load_source and source_mode == "adls":
            logger.info(
                "Ensuring Snowflake ADLS stage and file format exist",
                extra=_log_context(run_id, step_name="ensure_adls_stage"),
            )
            ensure_adls_stage(snowflake_conn)
        for index, script in enumerate(scripts, start=1):
            table_name = _table_name(script)
            source_table = f"{_database_name(script)}.{_schema_name(script)}.{table_name}"
            if load_source:
                state = save_external_execution_progress(
                    state,
                    run_id=run_id,
                    layer="bronze",
                    stage_key=stage_key,
                    status="RUNNING",
                    total_count=len(scripts),
                    completed_count=len(executed_scripts),
                    current_index=index,
                    current_name=table_name,
                    current_target=source_table,
                    message=f"Loading Bronze source data into Snowflake: table {index}/{len(scripts)} ({source_table}).",
                )
                logger.info(
                    "Loading source table %d/%d %s into Snowflake landing using mode=%s; waiting for external load",
                    index,
                    len(scripts),
                    source_table,
                    source_mode,
                    extra=_log_context(run_id, table=table_name, step_name="source_load_start"),
                )
                load_started_at = time.monotonic()
                if source_mode == "adls":
                    load_result = load_adls_table_to_snowflake(script, snowflake_conn)
                elif source_mode in {"adls_python", "adls_service_principal"}:
                    load_result = load_adls_python_table_to_snowflake(script, snowflake_conn)
                else:
                    load_result = load_azure_sql_table_to_snowflake(script, snowflake_conn, run_id=run_id)
                load_elapsed_seconds = round(time.monotonic() - load_started_at, 2)
                loaded_sources.append(load_result)
                logger.info(
                    "Loaded source table %d/%d %s into %s rows=%s files=%s elapsed_seconds=%s",
                    index,
                    len(scripts),
                    source_table,
                    load_result.get("snowflake_landing_table"),
                    load_result.get("rows_loaded", load_result.get("copy_result_count")),
                    load_result.get("files_loaded"),
                    load_elapsed_seconds,
                    extra=_log_context(run_id, table=table_name, step_name="source_load_complete"),
                )
            target_table = f"{script.get('bronze_catalog') or os.getenv('BRONZE_CATALOG', 'main')}.{script.get('bronze_schema') or os.getenv('BRONZE_SCHEMA', 'bronze')}.bronze_{table_name}"
            state = save_external_execution_progress(
                state,
                run_id=run_id,
                layer="bronze",
                stage_key=stage_key,
                status="RUNNING",
                total_count=len(scripts),
                completed_count=len(executed_scripts),
                current_index=index,
                current_name=table_name,
                current_target=target_table,
                message=f"Snowflake Bronze execution running: table {index}/{len(scripts)} ({table_name}).",
            )
            logger.info(
                "Executing Snowflake Bronze script %d/%d for table %s target=%s; waiting for Snowflake to finish",
                index,
                len(scripts),
                source_table,
                target_table,
                extra=_log_context(run_id, table=table_name, step_name="bronze_script_execute_start"),
            )
            started_at = time.monotonic()
            execution_result = execute_snowflake_sql_file(script, snowflake_conn)
            elapsed_seconds = round(time.monotonic() - started_at, 2)
            executed_scripts.append(execution_result)
            logger.info(
                "Completed Snowflake Bronze script %d/%d for table %s statements=%s target=%s elapsed_seconds=%s",
                index,
                len(scripts),
                source_table,
                execution_result.get("statement_count"),
                target_table,
                elapsed_seconds,
                extra=_log_context(run_id, table=table_name, step_name="bronze_script_execute_complete"),
            )
            state = save_external_execution_progress(
                state,
                run_id=run_id,
                layer="bronze",
                stage_key=stage_key,
                status="RUNNING",
                total_count=len(scripts),
                completed_count=len(executed_scripts),
                current_index=index,
                current_name=table_name,
                current_target=target_table,
                message=f"Snowflake Bronze execution progress: {len(executed_scripts)}/{len(scripts)} completed.",
            )
    finally:
        snowflake_conn.close()

    logger.info(
        "Completed Snowflake Bronze external execution: completed_tables=%d total_tables=%d",
        len(executed_scripts),
        len(scripts),
        extra=_log_context(run_id, step_name="bronze_execution_complete"),
    )

    final_state = {
        **state,
        "snowflake_bronze_execution_status": "COMPLETED",
        "snowflake_bronze_load_source_enabled": load_source,
        "snowflake_bronze_source_mode": source_mode,
        "snowflake_bronze_source_load_results": loaded_sources,
        "snowflake_bronze_execution_results": executed_scripts,
        "snowflake_bronze_executed_at": datetime.now(timezone.utc).isoformat(),
    }
    return save_external_execution_progress(
        final_state,
        run_id=run_id,
        layer="bronze",
        stage_key=stage_key,
        status="COMPLETED",
        total_count=len(scripts),
        completed_count=len(executed_scripts),
        message=f"Snowflake Bronze execution completed: {len(executed_scripts)}/{len(scripts)} scripts finished.",
    )
