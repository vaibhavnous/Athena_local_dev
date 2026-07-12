from __future__ import annotations

import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from services.external_execution_progress import save_external_execution_progress
from services.snowflake_contract_validation import validate_catalog_columns
from services.snowflake_bronze_runtime import _snowflake_connect
from utilis.logger import logger


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def snowflake_gold_execution_enabled() -> bool:
    return _env_bool("ATHENA_EXECUTE_SNOWFLAKE_GOLD", False)


def _log_context(run_id: Any, *, table: str | None = None, step_name: str = "snowflake_gold") -> Dict[str, Any]:
    context = {
        "run_id": str(run_id or ""),
        "node": "gold_code_execution",
        "stage": "gold_code_execution",
        "step_name": step_name,
    }
    if table:
        context["table"] = table
    return context


def _read_sql(script: Dict[str, Any]) -> str:
    body = str(script.get("script_body") or script.get("generated_gold_script") or "").strip()
    if body:
        return body
    path = Path(str(script.get("script_path") or ""))
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"Generated Snowflake gold SQL not found: {path}")
    return path.read_text(encoding="utf-8")


def _quote_identifier(value: str) -> str:
    cleaned = str(value or "").strip()
    if not cleaned:
        raise ValueError("Snowflake identifier cannot be empty.")
    return '"' + cleaned.replace('"', '""') + '"'


def _qualified_name(value: str) -> str:
    return ".".join(_quote_identifier(part) for part in str(value or "").split(".") if part.strip())


def _table_name(script: Dict[str, Any]) -> str:
    target_table = str(script.get("target_table") or "").strip()
    if target_table:
        return target_table.split(".")[-1]
    return str(script.get("kpi_name") or script.get("table") or script.get("entity") or "").strip()


def _silver_output_column_name(value: Any) -> str:
    normalized = str(value or "").strip().strip('"').lower()
    return {"rererence_id": "reference_id"}.get(normalized, normalized)


def _validate_canonical_source_references(sql: str, required_columns: List[str], *, layer: str, context: str) -> None:
    quoted = {
        match.group(1).replace('""', '"')
        for match in re.finditer(r'"((?:""|[^"])*)"', str(sql or ""))
    }
    quoted_folded = {value.casefold(): value for value in quoted}
    wrong_case = [
        column
        for column in required_columns
        if column not in quoted and column.casefold() in quoted_folded
    ]
    if wrong_case:
        raise ValueError(
            f'{layer} preflight rejected {context}: SQL must reference canonical Silver column(s) exactly: '
            + ", ".join(sorted(set(wrong_case))[:10])
        )


def validate_snowflake_gold_script(script: Dict[str, Any], catalog_connection: Any = None) -> str:
    sql = _read_sql(script)
    sql = _normalize_snowflake_gold_sql(sql)
    normalized = sql.upper()
    missing = [
        keyword
        for keyword in ("CREATE SCHEMA", "CREATE TABLE", "MERGE INTO", "WHEN MATCHED", "WHEN NOT MATCHED")
        if keyword not in normalized
    ]
    if missing:
        raise ValueError(f"Snowflake gold SQL is missing required statements: {', '.join(missing)}")
    for token in ("PYSPARK", "SPARK.", "DELTA", "DATABRICKS"):
        if token in normalized:
            raise ValueError(f"Snowflake gold SQL contains Databricks/Python token: {token.lower()}")

    source_table = str(script.get("source_table") or "").strip()
    target_table = str(script.get("target_table") or "").strip()
    if source_table and _qualified_name(source_table) not in sql:
        raise ValueError(f"Snowflake gold SQL does not read from expected source table: {source_table}")
    if target_table and _qualified_name(target_table) not in sql:
        raise ValueError(f"Snowflake gold SQL does not write to expected target table: {target_table}")
    required_columns = [
        _silver_output_column_name(column)
        for column in script.get("validation_columns") or []
        if str(column).strip()
    ]
    missing_columns = [column for column in required_columns if column not in sql.lower()]
    if missing_columns:
        raise ValueError(f"Snowflake gold SQL is missing contract columns: {', '.join(missing_columns[:10])}")
    if catalog_connection is not None and source_table:
        _validate_canonical_source_references(
            sql,
            required_columns,
            layer="Gold",
            context=str(script.get("kpi_name") or target_table or source_table),
        )
        validate_catalog_columns(
            catalog_connection,
            table_ref=source_table,
            required_columns=required_columns,
            layer="Gold",
            context=str(script.get("kpi_name") or target_table or source_table),
        )
    return sql


def validate_snowflake_dimension_script(script: Dict[str, Any], catalog_connection: Any = None) -> str:
    path = Path(str(script.get("dimension_script_path") or ""))
    if not path.exists() or not path.is_file():
        return ""
    sql = _normalize_snowflake_gold_sql(path.read_text(encoding="utf-8"))
    normalized = sql.upper()
    missing = [
        keyword
        for keyword in ("CREATE SCHEMA", "CREATE TABLE", "MERGE INTO", "WHEN MATCHED", "WHEN NOT MATCHED")
        if keyword not in normalized
    ]
    if missing:
        raise ValueError(f"Snowflake gold dimension SQL is missing required statements: {', '.join(missing)}")
    for token in ("PYSPARK", "SPARK.", "DELTA", "DATABRICKS"):
        if token in normalized:
            raise ValueError(f"Snowflake gold dimension SQL contains Databricks/Python token: {token.lower()}")
    dimension_contract = script.get("dimension_contract") or []
    required_columns = {
        _silver_output_column_name(column)
        for spec in dimension_contract
        if isinstance(spec, dict)
        for column in spec.get("columns") or []
        if str(column).strip()
    }
    missing_columns = [column for column in sorted(required_columns) if column not in sql.lower()]
    if missing_columns:
        raise ValueError(f"Snowflake gold dimension SQL is missing contract columns: {', '.join(missing_columns[:10])}")
    source_table = str(script.get("source_table") or "").strip()
    if catalog_connection is not None and source_table:
        _validate_canonical_source_references(
            sql,
            sorted(required_columns),
            layer="Gold dimension",
            context=str(script.get("kpi_name") or source_table),
        )
        required_by_source: Dict[str, set[str]] = {}
        for spec in dimension_contract:
            if not isinstance(spec, dict):
                continue
            logical_table = str(spec.get("logical_table") or "").strip()
            spec_source = str(spec.get("source_table") or "").strip() or source_table
            parts = [part for part in spec_source.split(".") if part.strip()]
            if logical_table and len(parts) >= 3:
                spec_source = ".".join([parts[0], parts[1], f"silver_{logical_table}"])
            required_by_source.setdefault(spec_source, set()).update(
                _silver_output_column_name(column)
                for column in (spec.get("source_columns") or spec.get("columns") or [])
                if str(column).strip()
            )
        if not required_by_source:
            required_by_source[source_table] = set(required_columns)
        for spec_source, spec_columns in required_by_source.items():
            validate_catalog_columns(
                catalog_connection,
                table_ref=spec_source,
                required_columns=spec_columns,
                layer="Gold dimension",
                context=str(script.get("kpi_name") or spec_source),
            )
    return sql


def _normalize_snowflake_gold_sql(sql: str) -> str:
    # Existing reviewed artifacts may contain TRY_TO_TIMESTAMP_NTZ("date_col").
    # Snowflake rejects TRY_CAST from DATE to TIMESTAMP_NTZ, so parse via VARCHAR.
    sql = re.sub(
        r'TRY_TO_TIMESTAMP_NTZ\(\s*("(?:""|[^"])+")\s*\)',
        r"TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR(\1))",
        sql,
    )
    sql = re.sub(
        r'TRY_TO_DECIMAL\(\s*("(?:""|[^"])+")\s*\)',
        r"TRY_TO_DECIMAL(TO_VARCHAR(\1))",
        sql,
    )
    return _inject_gold_schema_evolution(sql)


def _inject_gold_schema_evolution(sql: str) -> str:
    if "ADD COLUMN IF NOT EXISTS" in sql.upper():
        return sql

    create_match = re.search(
        r'(CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+(?P<table>(?:"[^"]+"\.)*"[^"]+")\s*\(\s*(?P<columns>.*?)\n\);)',
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not create_match:
        return sql

    column_defs: List[str] = []
    for raw_line in create_match.group("columns").splitlines():
        column_def = raw_line.strip().rstrip(",")
        if re.match(r'^"(?:""|[^"])+"\s+', column_def):
            column_defs.append(column_def)
    if not column_defs:
        return sql

    table = create_match.group("table")
    alter_sql = "\n".join(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column_def};" for column_def in column_defs)
    insert_at = create_match.end()
    return f"{sql[:insert_at]}\n\n{alter_sql}{sql[insert_at:]}"


def execute_snowflake_gold_sql(script: Dict[str, Any], snowflake_conn: Any) -> Dict[str, Any]:
    dimension_sql = validate_snowflake_dimension_script(script, catalog_connection=snowflake_conn)
    dimension_statement_count = 0
    if dimension_sql:
        dimension_cursors = snowflake_conn.execute_string(dimension_sql, return_cursors=True)
        dimension_statement_count = len(list(dimension_cursors or []))

    sql = validate_snowflake_gold_script(script, catalog_connection=snowflake_conn)
    cursors = snowflake_conn.execute_string(sql, return_cursors=True)
    statement_count = len(list(cursors or []))
    return {
        "kpi_name": script.get("kpi_name"),
        "source_table": script.get("source_table"),
        "target_table": script.get("target_table"),
        "script_path": script.get("script_path"),
        "dimension_script_path": script.get("dimension_script_path"),
        "dimension_statement_count": dimension_statement_count,
        "statement_count": statement_count,
        "status": "COMPLETED",
    }


def run_snowflake_gold_scripts(state: Dict[str, Any]) -> Dict[str, Any]:
    run_id = state.get("run_id")
    target_warehouse = str(state.get("target_warehouse") or "databricks").lower()
    if target_warehouse != "snowflake":
        return state
    if not snowflake_gold_execution_enabled():
        logger.info(
            "Snowflake Gold execution disabled; generated scripts remain review artifacts",
            extra=_log_context(run_id, step_name="gold_execution_disabled"),
        )
        return {**state, "snowflake_gold_execution_status": "DISABLED"}

    scripts = [item for item in state.get("gold_generation_results") or [] if isinstance(item, dict) and item.get("script_path")]
    if not scripts:
        raise ValueError("Snowflake gold execution enabled but no generated gold scripts were found.")

    snowflake_conn = _snowflake_connect()
    try:
        for script in scripts:
            validate_snowflake_gold_script(script, catalog_connection=snowflake_conn)
            validate_snowflake_dimension_script(script, catalog_connection=snowflake_conn)

        logger.info(
            "Gold Snowflake contract preflight passed: kpis=%d",
            len(scripts),
            extra=_log_context(run_id, step_name="gold_contract_preflight_complete"),
        )

        executed_scripts: List[Dict[str, Any]] = []
        stage_key = "gold_code_execution"
        logger.info(
            "Starting Snowflake Gold execution in external Snowflake warehouse: total_kpis=%d kpis=%s",
            len(scripts),
            ", ".join(_table_name(script) for script in scripts),
            extra=_log_context(run_id, step_name="gold_execution_start"),
        )
        state = save_external_execution_progress(
            state,
            run_id=run_id,
            layer="gold",
            stage_key=stage_key,
            status="RUNNING",
            total_count=len(scripts),
            completed_count=0,
            message=f"Executing Gold scripts in Snowflake: 0/{len(scripts)} completed.",
        )
        for index, script in enumerate(scripts, start=1):
            table_name = _table_name(script)
            target_table = script.get("target_table")
            state = save_external_execution_progress(
                state,
                run_id=run_id,
                layer="gold",
                stage_key=stage_key,
                status="RUNNING",
                total_count=len(scripts),
                completed_count=len(executed_scripts),
                current_index=index,
                current_name=table_name,
                current_target=str(target_table or ""),
                message=f"Snowflake Gold execution running: KPI {index}/{len(scripts)} ({table_name}).",
            )
            logger.info(
                "Executing Snowflake Gold script %d/%d for %s target=%s; waiting for Snowflake to finish",
                index,
                len(scripts),
                table_name,
                target_table,
                extra=_log_context(run_id, table=table_name, step_name="gold_script_execute_start"),
            )
            started_at = time.monotonic()
            execution_result = execute_snowflake_gold_sql(script, snowflake_conn)
            elapsed_seconds = round(time.monotonic() - started_at, 2)
            executed_scripts.append(execution_result)
            logger.info(
                "Completed Snowflake Gold script %d/%d for %s statements=%s target=%s elapsed_seconds=%s",
                index,
                len(scripts),
                table_name,
                execution_result.get("statement_count"),
                execution_result.get("target_table"),
                elapsed_seconds,
                extra=_log_context(run_id, table=table_name, step_name="gold_script_execute_complete"),
            )
            state = save_external_execution_progress(
                state,
                run_id=run_id,
                layer="gold",
                stage_key=stage_key,
                status="RUNNING",
                total_count=len(scripts),
                completed_count=len(executed_scripts),
                current_index=index,
                current_name=table_name,
                current_target=str(execution_result.get("target_table") or target_table or ""),
                message=f"Snowflake Gold execution progress: {len(executed_scripts)}/{len(scripts)} completed.",
            )
    finally:
        snowflake_conn.close()

    logger.info(
        "Completed Snowflake Gold external execution: completed_kpis=%d total_kpis=%d",
        len(executed_scripts),
        len(scripts),
        extra=_log_context(run_id, step_name="gold_execution_complete"),
    )
    final_state = {
        **state,
        "snowflake_gold_execution_status": "COMPLETED",
        "snowflake_gold_execution_results": executed_scripts,
        "snowflake_gold_executed_at": datetime.now(timezone.utc).isoformat(),
    }
    return save_external_execution_progress(
        final_state,
        run_id=run_id,
        layer="gold",
        stage_key=stage_key,
        status="COMPLETED",
        total_count=len(scripts),
        completed_count=len(executed_scripts),
        message=f"Snowflake Gold execution completed: {len(executed_scripts)}/{len(scripts)} scripts finished.",
    )
