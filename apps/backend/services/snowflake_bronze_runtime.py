from __future__ import annotations

import os
import re
import json
import csv
import io
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence
from urllib.parse import urlparse
from xml.etree import ElementTree

from services.external_execution_progress import save_external_execution_progress
from services.metadata_contracts import validate_runtime_context
from utilis.db import get_client_connection
from utilis.logger import logger


class SnowflakeAmbiguousExecutionError(RuntimeError):
    retryable = True
    preserve_attempt = True


def is_snowflake_transient_error(exc: BaseException) -> bool:
    return type(exc).__name__ in {"OperationalError", "InterfaceError", "DatabaseError"}


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def snowflake_bronze_execution_enabled() -> bool:
    return _env_bool("ATHENA_EXECUTE_SNOWFLAKE_BRONZE", False)


def snowflake_bronze_source_load_enabled() -> bool:
    return _env_bool(
        "ATHENA_SNOWFLAKE_BRONZE_LOAD_SOURCE",
        _env_bool("ATHENA_SNOWFLAKE_BRONZE_SOURCE_LOAD", False),
    )


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


def _normalize_identifier(value: Any) -> str:
    cleaned = str(value or "").strip()
    if len(cleaned) >= 2 and cleaned[0] == cleaned[-1] and cleaned[0] in {'"', "'"}:
        return cleaned[1:-1].strip()
    return cleaned


def _get_snowflake_connector():
    try:
        import snowflake.connector

        return snowflake.connector
    except Exception as exc:
        raise RuntimeError(
            "snowflake-connector-python is unavailable. Install backend requirements before enabling "
            "ATHENA_EXECUTE_SNOWFLAKE_BRONZE."
        ) from exc


def _snowflake_connect(*, autocommit: bool = True):
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
        "autocommit": autocommit,
    }
    for env_name, key in (
        ("SNOWFLAKE_WAREHOUSE", "warehouse"),
        ("SNOWFLAKE_ROLE", "role"),
        ("SNOWFLAKE_DATABASE", "database"),
        ("SNOWFLAKE_SCHEMA", "schema"),
    ):
        value = os.getenv(env_name)
        if str(value or "").strip():
            kwargs[key] = _normalize_identifier(value)
    return connector.connect(**kwargs)


def configure_snowflake_runtime_session(connection: Any, state: Dict[str, Any]) -> Dict[str, Any] | None:
    raw = state.get("metadata_runtime_context")
    if not raw:
        return None
    context = validate_runtime_context(raw)
    query_tag = json.dumps(
        {
            "logical_work_id": context["logical_work_id"],
            "queue_id": context["queue_id"],
            "attempt_number": context.get("attempt_number", 0),
            "runtime_run_id": context.get("runtime_run_id"),
            "processing_stage": context["processing_stage"],
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    cursor = connection.cursor()
    try:
        cursor.execute("ALTER SESSION SET QUERY_TAG = %s", (query_tag,))
        cursor.execute("SET ATHENA_LOGICAL_WORK_ID = %s", (context["logical_work_id"],))
        cursor.execute("SET ATHENA_RUNTIME_RUN_ID = %s", (context.get("runtime_run_id") or "",))
    finally:
        cursor.close()
    return context


def reconcile_snowflake_resumed_attempt(connection: Any, state: Dict[str, Any]) -> None:
    raw = state.get("metadata_runtime_context")
    if not raw or not bool(raw.get("resumed_attempt")):
        return
    context = validate_runtime_context(raw)
    query_tag = json.dumps(
        {
            "logical_work_id": context["logical_work_id"],
            "queue_id": context["queue_id"],
            "attempt_number": context.get("attempt_number", 0),
            "runtime_run_id": context.get("runtime_run_id"),
            "processing_stage": context["processing_stage"],
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    deadline = time.monotonic() + max(30, int(os.getenv("ATHENA_SNOWFLAKE_RESUME_WAIT_SECONDS", "300")))
    while True:
        cursor = connection.cursor()
        try:
            cursor.execute(
                "SELECT COUNT(*) FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY("
                "END_TIME_RANGE_START=>DATEADD('hour', -24, CURRENT_TIMESTAMP()), RESULT_LIMIT=>10000)) "
                "WHERE QUERY_TAG = %s AND SESSION_ID <> CURRENT_SESSION() "
                "AND EXECUTION_STATUS IN ('RUNNING','QUEUED','RESUMING','BLOCKED')",
                (query_tag,),
            )
            active = int(cursor.fetchone()[0])
        finally:
            cursor.close()
        if active == 0:
            return
        if time.monotonic() >= deadline:
            raise TimeoutError("A prior Snowflake execution for this same queue attempt is still active.")
        time.sleep(2)


def _snowflake_quote_identifier(value: str) -> str:
    cleaned = _normalize_identifier(value)
    if not cleaned:
        raise ValueError("Snowflake identifier cannot be empty.")
    return '"' + cleaned.replace('"', '""') + '"'


def _use_existing_database(cursor: Any, database_name: str) -> None:
    # ponytail: deployment roles intentionally cannot create account-level databases.
    cursor.execute(f"USE DATABASE {_snowflake_quote_identifier(database_name)}")


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
    raw = os.getenv("ATHENA_SNOWFLAKE_BRONZE_BATCH_SIZE", "10000")
    try:
        return max(1, int(raw))
    except ValueError:
        return 10000


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


def _write_pandas_batch(
    connection: Any,
    *,
    database: str,
    schema: str,
    table: str,
    columns: Sequence[str],
    rows: Sequence[Sequence[Any]],
) -> int:
    """Load one bounded batch through Snowflake PUT/COPY on the existing session."""
    import pandas as pd
    from snowflake.connector.pandas_tools import write_pandas

    frame = pd.DataFrame.from_records(rows, columns=list(columns))
    success, _chunks, loaded_rows, _output = write_pandas(
        connection,
        frame,
        table_name=table,
        database=database,
        schema=schema,
        chunk_size=len(frame),
        bulk_upload_chunks=True,
        quote_identifiers=True,
        auto_create_table=False,
        overwrite=False,
    )
    if not success or int(loaded_rows) != len(frame):
        raise RuntimeError(
            f"Snowflake bulk source load wrote {loaded_rows} of {len(frame)} rows to "
            f"{database}.{schema}.{table}."
        )
    return int(loaded_rows)


def _table_name(script: Dict[str, Any]) -> str:
    table_name = str(script.get("table") or script.get("table_name") or script.get("entity") or "").strip()
    if not table_name:
        raise ValueError("Snowflake bronze source load is missing table name.")
    return table_name


def _database_name(script: Dict[str, Any]) -> str:
    return str(script.get("database_name") or "insurance").strip() or "insurance"


def _schema_name(script: Dict[str, Any]) -> str:
    return str(script.get("schema_name") or "dbo").strip() or "dbo"


def _landing_relation(script: Dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(script.get("snowflake_landing_database") or _database_name(script)).strip(),
        str(script.get("snowflake_landing_schema") or _schema_name(script)).strip(),
        str(script.get("snowflake_landing_table") or _table_name(script)).strip(),
    )


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

        landing_database, landing_schema, landing_name = _landing_relation(script)
        landing_table = _snowflake_qualified_name(landing_database, landing_schema, landing_name)
        column_defs = ", ".join(f"{_snowflake_quote_identifier(column)} VARCHAR" for column in columns)

        # ponytail: source landing is raw VARCHAR; generated bronze SQL owns all typing via TRY_CAST.
        snowflake_cursor = snowflake_conn.cursor()
        try:
            _use_existing_database(snowflake_cursor, landing_database)
            snowflake_cursor.execute(
                f"CREATE SCHEMA IF NOT EXISTS {_snowflake_qualified_name(landing_database, landing_schema)}"
            )
            table_kind = "TEMPORARY TABLE" if script.get("metadata_runtime") else "TABLE"
            snowflake_cursor.execute(
                f"CREATE OR REPLACE {table_kind} {landing_table} ({column_defs})"
            )

            while True:
                rows = source_cursor.fetchmany(_batch_size())
                if not rows:
                    break
                values = _string_rows(rows)
                inserted_rows += _write_pandas_batch(
                    snowflake_conn,
                    database=landing_database,
                    schema=landing_schema,
                    table=landing_name,
                    columns=columns,
                    rows=values,
                )
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
        "snowflake_landing_table": f"{landing_database}.{landing_schema}.{landing_name}",
        "rows_loaded": inserted_rows,
    }


def _adls_stage_database(*, adls_flow: bool = False) -> str:
    name = "ADLS_FLOW_STAGE_DB" if adls_flow else "SNOWFLAKE_ADLS_STAGE_DB"
    return str(os.getenv(name) or os.getenv("BRONZE_CATALOG") or "ATHENA_DB").strip()


def _adls_stage_schema(*, adls_flow: bool = False) -> str:
    name = "ADLS_FLOW_STAGE_SCHEMA" if adls_flow else "SNOWFLAKE_ADLS_STAGE_SCHEMA"
    return str(os.getenv(name) or os.getenv("BRONZE_SCHEMA") or "BRONZE").strip()


def _adls_stage_name(*, adls_flow: bool = False) -> str:
    name = "ADLS_FLOW_STAGE_NAME" if adls_flow else "SNOWFLAKE_ADLS_STAGE_NAME"
    return str(os.getenv(name) or "ADLS_INSURANCE_STAGE").strip()


def _adls_file_format_name(*, adls_flow: bool = False) -> str:
    name = "ADLS_FLOW_STAGE_FORMAT" if adls_flow else "SNOWFLAKE_ADLS_FILE_FORMAT"
    return str(os.getenv(name) or "ADLS_CSV_FORMAT").strip()


def _adls_integration_name() -> str:
    return str(os.getenv("SNOWFLAKE_ADLS_INTEGRATION") or "ADLS_INSURANCE_INT").strip()


def _adls_sas_token(*, adls_flow: bool = False) -> str:
    name = "ADLS_FLOW_SAS_TOKEN" if adls_flow else "SNOWFLAKE_ADLS_SAS_TOKEN"
    return str(os.getenv(name) or "").strip().lstrip("?")


def _adls_stage_url(*, adls_flow: bool = False) -> str:
    name = "ADLS_FLOW_STAGE_URL" if adls_flow else "SNOWFLAKE_ADLS_STAGE_URL"
    return str(
        os.getenv(name)
        or "azure://atheastorage.blob.core.windows.net/athena/Insurance/"
    ).strip()


def _adls_file_for_script(script: Dict[str, Any], *, adls_flow: bool = False) -> str:
    configured = str(script.get("adls_file") or "").strip().strip("/")
    if configured or not adls_flow:
        return configured or f"{_table_name(script)}.csv"

    def storage_path(value: Any) -> str:
        parsed = urlparse(str(value or "").strip())
        path = (parsed.path if parsed.scheme else str(value or "")).strip("/")
        filesystem, _, remainder = path.partition("/")
        return remainder if filesystem.casefold() == _adls_file_system().casefold() else path

    source_path = storage_path(script.get("adls_source_path") or script.get("adls_remote_path"))
    stage_root = storage_path(_adls_stage_url(adls_flow=True)).rstrip("/")
    if not source_path or not stage_root or not source_path.casefold().startswith(
        stage_root.casefold() + "/"
    ):
        raise ValueError("The approved ADLS source file is outside the configured Snowflake stage root.")
    return source_path[len(stage_root) + 1 :]


def _stage_ref(*, include_name: bool = True, adls_flow: bool = False) -> str:
    parts = [_adls_stage_database(adls_flow=adls_flow), _adls_stage_schema(adls_flow=adls_flow)]
    if include_name:
        parts.append(_adls_stage_name(adls_flow=adls_flow))
    return _snowflake_qualified_name(*parts)


def _file_format_ref(*, adls_flow: bool = False) -> str:
    return _snowflake_qualified_name(
        _adls_stage_database(adls_flow=adls_flow),
        _adls_stage_schema(adls_flow=adls_flow),
        _adls_file_format_name(adls_flow=adls_flow),
    )


def ensure_adls_stage(snowflake_conn: Any, *, adls_flow: bool = False) -> Dict[str, Any]:
    stage_schema = _snowflake_qualified_name(
        _adls_stage_database(adls_flow=adls_flow), _adls_stage_schema(adls_flow=adls_flow)
    )
    sas_token = _adls_sas_token(adls_flow=adls_flow)
    create_stage = "CREATE OR REPLACE STAGE" if sas_token else "CREATE STAGE IF NOT EXISTS"
    credentials = (
        f"CREDENTIALS = (AZURE_SAS_TOKEN = {_snowflake_string_literal(sas_token)})"
        if sas_token
        else f"STORAGE_INTEGRATION = {_adls_integration_name()}"
    )
    cursor = snowflake_conn.cursor()
    try:
        _use_existing_database(cursor, _adls_stage_database(adls_flow=adls_flow))
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {stage_schema}")
        cursor.execute(
            f"""
CREATE FILE FORMAT IF NOT EXISTS {_file_format_ref(adls_flow=adls_flow)}
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
{create_stage} {_stage_ref(adls_flow=adls_flow)}
    URL = {_snowflake_string_literal(_adls_stage_url(adls_flow=adls_flow))}
    {credentials}
    FILE_FORMAT = {_file_format_ref(adls_flow=adls_flow)}
            """.strip()
        )
    finally:
        cursor.close()

    return {
        "stage": _stage_ref(adls_flow=adls_flow),
        "file_format": _file_format_ref(adls_flow=adls_flow),
        "stage_url": _adls_stage_url(adls_flow=adls_flow),
        "credential_type": "sas" if sas_token else "storage_integration",
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


def load_adls_table_to_snowflake(
    script: Dict[str, Any], snowflake_conn: Any, *, adls_flow: bool = False
) -> Dict[str, Any]:
    database_name, schema_name, table_name = _landing_relation(script)
    landing_table = _snowflake_qualified_name(database_name, schema_name, table_name)
    columns = _landing_columns(script)
    adls_file = _adls_file_for_script(script, adls_flow=adls_flow)
    stage_path = f"@{_stage_ref(adls_flow=adls_flow)}"

    cursor = snowflake_conn.cursor()
    try:
        _use_existing_database(cursor, database_name)
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
            FILE_FORMAT => {_snowflake_string_literal(_file_format_ref(adls_flow=adls_flow))}
        )
    )
)
                """.strip()
            )
        cursor.execute(f"TRUNCATE TABLE {landing_table}")
        cursor.execute(
            f"""
COPY INTO {landing_table}
FROM {stage_path}
FILES = ({_snowflake_string_literal(adls_file)})
FILE_FORMAT = (FORMAT_NAME = {_file_format_ref(adls_flow=adls_flow)})
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
        "adls_file": adls_file,
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
    raw = str(
        script.get("adls_remote_path")
        or script.get("adls_source_path")
        or script.get("adls_folder")
        or script.get("landing_path")
        or _table_name(script)
    ).strip()
    parsed = urlparse(raw)
    folder = (parsed.path if parsed.scheme else raw).strip("/")
    if parsed.scheme in {"http", "https"}:
        filesystem, _, remainder = folder.partition("/")
        if filesystem.casefold() == _adls_file_system().casefold() and remainder:
            folder = remainder
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


def _adls_source_format(script: Dict[str, Any]) -> str:
    declared = str(script.get("adls_source_format") or "").strip().lower().lstrip(".")
    if declared:
        return declared
    return Path(str(script.get("source_file_name") or script.get("adls_source_path") or "")).suffix.lower().lstrip(".") or "csv"


def _adls_file_paths(file_system_client: Any, folder: str, file_format: str) -> List[str]:
    extensions = {
        "csv": (".csv",),
        "txt": (".txt",),
        "json": (".json", ".jsonl", ".ndjson"),
        "xml": (".xml",),
    }.get(file_format)
    if not extensions:
        raise ValueError(f"Unsupported ADLS-to-Snowflake source format: {file_format}")
    if folder.lower().endswith(extensions):
        return [folder]
    paths = []
    try:
        for path in file_system_client.get_paths(path=folder, recursive=True):
            if getattr(path, "is_directory", False):
                continue
            name = str(getattr(path, "name", "") or "")
            if name.lower().endswith(extensions):
                paths.append(name)
    except Exception:
        candidate = f"{folder}{extensions[0]}"
        try:
            file_system_client.get_file_client(candidate).get_file_properties()
            return [candidate]
        except Exception:
            raise
    if not paths:
        raise ValueError(
            f"No {file_format.upper()} files found in ADLS folder: {_adls_file_system()}/{folder}"
        )
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


def _json_scalar(value: Any) -> Any:
    return json.dumps(value, sort_keys=True) if isinstance(value, (dict, list)) else value


def _adls_records(payload: str, file_format: str, parser_options: Dict[str, Any]) -> List[Dict[str, Any]]:
    if file_format in {"csv", "txt"}:
        return [dict(row) for row in csv.DictReader(io.StringIO(payload))]
    if file_format == "json":
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            parsed = [json.loads(line) for line in payload.splitlines() if line.strip()]
        if isinstance(parsed, dict):
            list_values = [value for value in parsed.values() if isinstance(value, list)]
            parsed = list_values[0] if len(list_values) == 1 else [parsed]
        if not isinstance(parsed, list) or any(not isinstance(item, dict) for item in parsed):
            raise ValueError("ADLS JSON source must contain objects or an array of objects.")
        return [{str(key): _json_scalar(value) for key, value in item.items()} for item in parsed]
    root = ElementTree.fromstring(payload)
    row_tag = str(parser_options.get("rowTag") or parser_options.get("row_tag") or "").strip()
    elements = list(root.iter(row_tag)) if row_tag else list(root)
    if elements and elements[0] is root:
        elements = elements[1:]
    return [
        {
            str(child.tag).split("}")[-1]: child.text
            for child in list(element)
        }
        for element in elements
        if list(element)
    ]


def load_adls_python_table_to_snowflake(script: Dict[str, Any], snowflake_conn: Any) -> Dict[str, Any]:
    database_name, schema_name, table_name = _landing_relation(script)
    landing_table = _snowflake_qualified_name(database_name, schema_name, table_name)
    folder = _adls_python_folder_for_script(script)
    file_system_client = _get_adls_file_system_client()
    file_format = _adls_source_format(script)
    paths = _adls_file_paths(file_system_client, folder, file_format)
    parser_options = dict(script.get("adls_parser_options") or {})
    row_limit = _adls_python_row_limit()

    cursor = snowflake_conn.cursor()
    inserted_rows = 0
    created_table = False
    try:
        _use_existing_database(cursor, database_name)
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {_snowflake_qualified_name(database_name, schema_name)}")

        for path in paths:
            records = _adls_records(
                _download_adls_text(file_system_client, path), file_format, parser_options
            )
            columns = list(dict.fromkeys(
                str(column or "").strip()
                for record in records
                for column in record
                if str(column or "").strip()
            ))
            if not columns:
                continue

            if not created_table:
                column_defs = ", ".join(f"{_snowflake_quote_identifier(column)} VARCHAR" for column in columns)
                cursor.execute(f"CREATE TABLE IF NOT EXISTS {landing_table} ({column_defs})")
                cursor.execute(f"TRUNCATE TABLE {landing_table}")
                created_table = True

            column_list = ", ".join(_snowflake_quote_identifier(column) for column in columns)
            placeholders = ", ".join(["%s"] * len(columns))
            insert_sql = f"INSERT INTO {landing_table} ({column_list}) VALUES ({placeholders})"
            batch: List[tuple[Any, ...]] = []
            for row in records:
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
        "source_format": file_format,
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

    if script.get("execution_spec"):
        from utilis.generated_code_paths import verified_execution_artifact

        verified_path = verified_execution_artifact(script["execution_spec"], platform="snowflake")
        if Path(str(script.get("script_path") or "")).resolve() != verified_path:
            raise RuntimeError("Snowflake script_path does not match the registered execution artifact.")
        sql = verified_path.read_text(encoding="utf-8")
    else:
        sql = _read_sql_file(script.get("script_path"))
    table_name = str(script.get("table") or script.get("table_name") or "").strip()
    database_name = str(script.get("database_name") or "insurance")
    schema_name = str(script.get("schema_name") or "dbo")
    bronze_catalog = str(script.get("bronze_catalog") or os.getenv("BRONZE_CATALOG", "main"))
    bronze_schema = str(script.get("bronze_schema") or os.getenv("BRONZE_SCHEMA", "bronze"))
    landing_database = str(script.get("snowflake_landing_database") or database_name)
    landing_schema = str(script.get("snowflake_landing_schema") or schema_name)
    landing_table = str(script.get("snowflake_landing_table") or table_name)
    validate_snowflake_bronze_sql(
        sql,
        source_table=(
            _snowflake_qualified_name(landing_database, landing_schema, landing_table)
            if table_name else None
        ),
        target_table=_snowflake_qualified_name(bronze_catalog, bronze_schema, f"bronze_{table_name}") if table_name else None,
        metadata_driven=bool(script.get("metadata_runtime")),
    )
    return sql


def execute_snowflake_sql_file(script: Dict[str, Any], snowflake_conn: Any) -> Dict[str, Any]:
    sql = validate_snowflake_bronze_script(script)
    try:
        cursors = snowflake_conn.execute_string(sql, return_cursors=True)
    except Exception as exc:
        if is_snowflake_transient_error(exc):
            raise SnowflakeAmbiguousExecutionError(
                "Snowflake execution outcome is ambiguous; the same queue attempt must be resumed."
            ) from exc
        raise
    executed = list(cursors or [])
    statement_count = len(executed)
    query_id = next(
        (str(getattr(cursor, "sfqid", "") or "") for cursor in reversed(executed) if getattr(cursor, "sfqid", None)),
        "",
    )
    result = {
        "table": script.get("table"),
        "script_path": script.get("script_path"),
        "statement_count": statement_count,
        "status": "COMPLETED",
        "snowflake_query_id": query_id,
    }
    receipt = snowflake_target_commit_result(script, query_id)
    if receipt:
        target_parts = str(script.get("target_table") or "").split(".")
        if len(target_parts) != 3:
            raise RuntimeError("Metadata Snowflake Bronze is missing its exact target table.")
        cursor = snowflake_conn.cursor()
        try:
            cursor.execute(
                f"SELECT COUNT(*) FROM {_snowflake_qualified_name(*target_parts)} "
                'WHERE "_logical_work_id" = $ATHENA_LOGICAL_WORK_ID'
            )
            rows_written = int(cursor.fetchone()[0])
        finally:
            cursor.close()
        rows_read = script.get("source_rows_loaded")
        if rows_read is not None and int(rows_read) != rows_written:
            raise RuntimeError(
                "Snowflake Bronze row reconciliation failed for the queued logical work."
            )
        receipt.update({"rows_read": rows_read, "rows_written": rows_written})
        result["execution_result"] = receipt
    return result


def snowflake_target_commit_result(
    script: Dict[str, Any],
    query_id: str,
    *,
    validation_results: List[Dict[str, Any]] | None = None,
) -> Dict[str, Any] | None:
    raw_context = script.get("metadata_runtime_context")
    if not raw_context:
        return None
    context = validate_runtime_context(raw_context)
    if not str(query_id or "").strip():
        raise RuntimeError("Snowflake did not return a target commit query ID.")
    policy_rules = (script.get("validation_policy") or {}).get("rules") or []
    if policy_rules and validation_results is None:
        raise RuntimeError("Snowflake target did not return rule-level blocking-validation evidence.")
    return {
        "contract_version": "1.0",
        "status": "COMPLETED",
        "logical_work_id": context["logical_work_id"],
        "runtime_run_id": context.get("runtime_run_id"),
        "target_table": context["target_table"],
        "target_commit_id": str(query_id),
        "validation_status": "PASSED",
        "validation_policy_hash": context.get("validation_policy_hash"),
        "validation_results": list(validation_results or []),
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

    statuses = {str(feed.get("review_status") or "").upper() for feed in feeds}
    if not statuses.intersection({"APPROVED", "REJECTED"}):
        return scripts

    def matching_script(feed: Dict[str, Any]) -> Dict[str, Any] | None:
        key = _script_key(feed)
        script = scripts_by_key.get(key)
        if script is None:
            script = scripts_by_casefolded_key.get(_casefold_script_key(feed))
        return script

    approved: List[Dict[str, Any]] = []
    approved_feeds = [feed for feed in feeds if str(feed.get("review_status") or "").upper() == "APPROVED"]
    if approved_feeds:
        for feed in approved_feeds:
            script = matching_script(feed)
            if script is None:
                raise ValueError(f"Approved Bronze review item has no generated script: {_script_key(feed)}")
            approved.append({**script, **feed})
        return approved

    rejected_keys = {
        _casefold_script_key(feed)
        for feed in feeds
        if str(feed.get("review_status") or "").upper() == "REJECTED"
    }
    return [script for script in scripts if _casefold_script_key(script) not in rejected_keys]


def run_snowflake_bronze_scripts(
    state: Dict[str, Any],
    *,
    review_artifact: Dict[str, Any] | None = None,
    approved_only: bool = False,
    load_only: bool = False,
    progress_stage_key: str = "bronze_code_execution",
) -> Dict[str, Any]:
    run_id = state.get("run_id")
    target_warehouse = str(state.get("target_warehouse") or "databricks").lower()
    if target_warehouse != "snowflake":
        return state
    if not load_only and not snowflake_bronze_execution_enabled():
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

    if not load_only:
        for script in scripts:
            validate_snowflake_bronze_script(script)

    metadata_runtime = bool(state.get("metadata_runtime_context"))
    # Metadata JDBC Bronze always uses the already-validated deployment connector.
    load_source = True if metadata_runtime else snowflake_bronze_source_load_enabled()
    if load_only and not load_source:
        raise RuntimeError(
            "Native Snowflake dbt execution requires ATHENA_SNOWFLAKE_BRONZE_LOAD_SOURCE=true "
            "so source data is landed before dbt build."
        )
    adls_flow = str(state.get("source") or "database").lower() == "adls_gen2"
    configured_source_mode = (
        str(os.getenv("ADLS_FLOW_BRONZE_SOURCE_MODE") or "adls_python").strip().lower()
        if adls_flow
        else _source_mode()
    )
    source_mode = (
        configured_source_mode
        if configured_source_mode in {"adls", "adls_python", "adls_service_principal"}
        else "adls_python"
    ) if adls_flow else (
        "azure_sql" if metadata_runtime else configured_source_mode
    )
    if source_mode == "adls" and any(_adls_source_format(script) not in {"csv", "txt"} for script in scripts):
        # ponytail: the configured Snowflake stage has one CSV file format; use the
        # service-principal reader for mixed JSON/XML feeds until per-format stages exist.
        source_mode = "adls_python"
    loaded_sources: List[Dict[str, Any]] = []
    executed_scripts: List[Dict[str, Any]] = []
    stage_key = str(progress_stage_key or "bronze_code_execution")
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
        configure_snowflake_runtime_session(snowflake_conn, state)
        reconcile_snowflake_resumed_attempt(snowflake_conn, state)
        if load_source and source_mode == "adls":
            logger.info(
                "Ensuring Snowflake ADLS stage and file format exist",
                extra=_log_context(run_id, step_name="ensure_adls_stage"),
            )
            ensure_adls_stage(snowflake_conn, adls_flow=adls_flow)
        for index, script in enumerate(scripts, start=1):
            if script.get("metadata_runtime_context"):
                configure_snowflake_runtime_session(
                    snowflake_conn,
                    {"metadata_runtime_context": script["metadata_runtime_context"]},
                )
            table_name = _table_name(script)
            source_table = f"{_database_name(script)}.{_schema_name(script)}.{table_name}"
            load_result: Dict[str, Any] = {}
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
                    load_result = load_adls_table_to_snowflake(
                        script, snowflake_conn, adls_flow=adls_flow
                    )
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
                if load_only:
                    state = save_external_execution_progress(
                        state,
                        run_id=run_id,
                        layer="bronze",
                        stage_key=stage_key,
                        status="RUNNING",
                        total_count=len(scripts),
                        completed_count=len(loaded_sources),
                        current_index=index,
                        current_name=table_name,
                        current_target=source_table,
                        message=f"Snowflake source landing progress: {len(loaded_sources)}/{len(scripts)} completed.",
                    )
                    continue
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
            execution_result = execute_snowflake_sql_file(
                {
                    **script,
                    "source_rows_loaded": load_result.get("rows_loaded"),
                },
                snowflake_conn,
            )
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

    completed_count = len(loaded_sources) if load_only else len(executed_scripts)
    logger.info(
        "Completed Snowflake Bronze external %s: completed_tables=%d total_tables=%d",
        "source landing" if load_only else "execution",
        completed_count,
        len(scripts),
        extra=_log_context(run_id, step_name="bronze_execution_complete"),
    )

    final_state = {
        **state,
        "snowflake_bronze_execution_status": "SKIPPED_DBT_CODEGEN_ONLY" if load_only else "COMPLETED",
        "snowflake_bronze_source_load_status": "COMPLETED" if load_only else None,
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
        completed_count=completed_count,
        message=(
            f"Snowflake source landing completed: {completed_count}/{len(scripts)} tables loaded for dbt."
            if load_only
            else f"Snowflake Bronze execution completed: {completed_count}/{len(scripts)} scripts finished."
        ),
    )
