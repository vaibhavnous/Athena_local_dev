from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from services.external_execution_progress import save_external_execution_progress
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


def validate_snowflake_gold_script(script: Dict[str, Any]) -> str:
    sql = _read_sql(script)
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
    return sql


def execute_snowflake_gold_sql(script: Dict[str, Any], snowflake_conn: Any) -> Dict[str, Any]:
    sql = validate_snowflake_gold_script(script)
    cursors = snowflake_conn.execute_string(sql, return_cursors=True)
    statement_count = len(list(cursors or []))
    return {
        "kpi_name": script.get("kpi_name"),
        "source_table": script.get("source_table"),
        "target_table": script.get("target_table"),
        "script_path": script.get("script_path"),
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

    for script in scripts:
        validate_snowflake_gold_script(script)

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
    snowflake_conn = _snowflake_connect()
    try:
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
