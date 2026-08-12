from __future__ import annotations

import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from services.external_execution_progress import save_external_execution_progress
from services.snowflake_contract_validation import (
    extract_quoted_source_column_references,
    extract_source_column_references,
    validate_catalog_columns,
)
from services.snowflake_bronze_runtime import (
    _snowflake_connect,
    configure_snowflake_runtime_session,
    is_snowflake_transient_error,
    reconcile_snowflake_resumed_attempt,
    SnowflakeAmbiguousExecutionError,
    snowflake_target_commit_result,
)
from utilis.logger import logger


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def snowflake_silver_execution_enabled() -> bool:
    return _env_bool("ATHENA_EXECUTE_SNOWFLAKE_SILVER", False)


def _log_context(run_id: Any, *, table: str | None = None, step_name: str = "snowflake_silver") -> Dict[str, Any]:
    context = {
        "run_id": str(run_id or ""),
        "node": "silver_code_execution",
        "stage": "silver_code_execution",
        "step_name": step_name,
    }
    if table:
        context["table"] = table
    return context


def _read_sql(script: Dict[str, Any]) -> str:
    execution_spec = script.get("execution_spec")
    if execution_spec:
        from utilis.generated_code_paths import verified_execution_artifact

        verified_path = verified_execution_artifact(execution_spec, platform="snowflake")
        script_path = Path(str(script.get("script_path") or "")).resolve()
        if script_path != verified_path:
            raise RuntimeError("Snowflake Silver script_path does not match the registered execution artifact.")
        return verified_path.read_text(encoding="utf-8")
    body = str(script.get("script_body") or script.get("generated_silver_script") or "").strip()
    if body:
        return body
    path = Path(str(script.get("script_path") or ""))
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"Generated Snowflake silver SQL not found: {path}")
    return path.read_text(encoding="utf-8")


def _quote_identifier(value: str) -> str:
    cleaned = str(value or "").strip()
    if not cleaned:
        raise ValueError("Snowflake identifier cannot be empty.")
    return '"' + cleaned.replace('"', '""') + '"'


def _qualified_name(value: str) -> str:
    return ".".join(_quote_identifier(part) for part in str(value or "").split(".") if part.strip())


def _sql_without_comments(sql: str) -> str:
    from nodes.bronze_gen import _sql_without_comments as strip_comments

    return strip_comments(sql)


def _require_approved_snowflake_structure(sql: str, source_table: str, target_table: str) -> None:
    clean_sql = _sql_without_comments(sql)
    if source_table and not re.search(
        rf"(?:^|\s)FROM\s+{re.escape(_qualified_name(source_table))}\s+(?:AS\s+)?src\b",
        clean_sql,
        re.IGNORECASE,
    ):
        raise ValueError(f"Snowflake silver SQL must read the approved source table as src: {source_table}")
    if target_table and not re.search(
        rf"\bMERGE\s+INTO\s+{re.escape(_qualified_name(target_table))}\s+(?:AS\s+)?target\b",
        clean_sql,
        re.IGNORECASE,
    ):
        raise ValueError(f"Snowflake silver SQL must merge into the approved target table as target: {target_table}")
    forbidden = re.search(r"\b(DROP|TRUNCATE|DELETE|COPY|CALL|GRANT|REVOKE|USE|EXECUTE\s+IMMEDIATE)\b", clean_sql, re.IGNORECASE)
    if forbidden:
        raise ValueError(f"Snowflake silver SQL contains forbidden statement: {forbidden.group(1).upper()}")


def _table_name(script: Dict[str, Any]) -> str:
    return str(script.get("table") or script.get("table_name") or script.get("entity") or "").strip()


def validate_snowflake_silver_script(script: Dict[str, Any], catalog_connection: Any = None) -> str:
    sql = _read_sql(script)
    normalized = sql.upper()
    missing = [
        keyword
        for keyword in ("CREATE SCHEMA", "CREATE TABLE", "MERGE INTO", "WHEN MATCHED", "WHEN NOT MATCHED")
        if keyword not in normalized
    ]
    if missing:
        raise ValueError(f"Snowflake silver SQL is missing required statements: {', '.join(missing)}")
    for token in ("PYSPARK", "SPARK.", "DELTA", "DATABRICKS"):
        if token in normalized:
            raise ValueError(f"Snowflake silver SQL contains Databricks/Python token: {token.lower()}")

    source_table = str(script.get("source_table") or script.get("bronze_table") or "").strip()
    target_table = str(script.get("target_table") or script.get("silver_table") or "").strip()
    if source_table and _qualified_name(source_table) not in sql:
        raise ValueError(f"Snowflake silver SQL does not read from expected source table: {source_table}")
    if target_table and _qualified_name(target_table) not in sql:
        raise ValueError(f"Snowflake silver SQL does not write to expected target table: {target_table}")
    _require_approved_snowflake_structure(sql, source_table, target_table)
    if script.get("metadata_runtime") and (
        '"_logical_work_id"' not in _sql_without_comments(sql).casefold()
        or "$ATHENA_LOGICAL_WORK_ID" not in _sql_without_comments(sql).upper()
    ):
        raise ValueError("Metadata Snowflake Silver SQL does not isolate the queued logical work.")
    if catalog_connection is not None and source_table:
        validate_catalog_columns(
            catalog_connection,
            table_ref=source_table,
            required_columns=extract_source_column_references(sql),
            layer="Silver",
            context=_table_name(script) or target_table or source_table,
            exact_columns=extract_quoted_source_column_references(sql),
        )
    return sql


def _blocking_validation_results(script: Dict[str, Any], snowflake_conn: Any) -> List[Dict[str, Any]]:
    rules = (script.get("validation_policy") or {}).get("rules") or []
    if not rules:
        return []
    target = str(script.get("target_table") or script.get("silver_table") or "").strip()
    expected_columns = {
        str(row.get("target_column_name") or "")
        for row in (script.get("mapping_contract") or [])
        if str(row.get("target_column_name") or "")
    }
    cursor = snowflake_conn.cursor()
    try:
        cursor.execute(f"SELECT * FROM {_qualified_name(target)} LIMIT 0")
        target_columns = {str(item[0]) for item in (cursor.description or [])}
        results = []
        for rule in rules:
            rule_type = str(rule.get("rule_type") or rule.get("rule") or "").upper()
            threshold = rule.get("threshold_value", 0)
            if rule_type in {"MAPPED_COLUMNS_PRESENT", "TARGET_SCHEMA_MATCH"}:
                observed = len(expected_columns - target_columns)
            elif rule_type == "MERGE_KEYS_NOT_NULL":
                columns = [str(name) for name in (rule.get("columns") or script.get("merge_keys") or [])]
                if not columns or any(name not in target_columns for name in columns):
                    raise RuntimeError("Snowflake merge-key validation references unavailable columns.")
                predicate = " OR ".join(f"{_quote_identifier(name)} IS NULL" for name in columns)
                cursor.execute(
                    f"SELECT COUNT_IF({predicate}) FROM {_qualified_name(target)} "
                    'WHERE "_logical_work_id" = $ATHENA_LOGICAL_WORK_ID'
                )
                observed = int(cursor.fetchone()[0])
            else:
                raise RuntimeError(f"Unsupported Snowflake Silver validation rule: {rule_type}")
            status = "PASSED" if float(observed) <= float(threshold) else "FAILED"
            result = {
                "rule_type": rule_type,
                "observed_value": observed,
                "threshold_value": threshold,
                "status": status,
            }
            results.append(result)
            if status != "PASSED":
                raise RuntimeError(f"Snowflake Silver blocking validation failed: {result}")
        return results
    finally:
        cursor.close()


def execute_snowflake_silver_sql(script: Dict[str, Any], snowflake_conn: Any) -> Dict[str, Any]:
    sql = validate_snowflake_silver_script(script, catalog_connection=snowflake_conn)
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
        "table": _table_name(script),
        "source_table": script.get("source_table") or script.get("bronze_table"),
        "target_table": script.get("target_table") or script.get("silver_table"),
        "script_path": script.get("script_path"),
        "statement_count": statement_count,
        "status": "COMPLETED",
        "snowflake_query_id": query_id,
    }
    receipt = snowflake_target_commit_result(
        script,
        query_id,
        validation_results=_blocking_validation_results(script, snowflake_conn),
    )
    if receipt:
        result["execution_result"] = receipt
    return result


def _script_key(script: Dict[str, Any]) -> str:
    target = str(script.get("target_table") or script.get("silver_table") or "").strip()
    table = _table_name(script)
    return target or table


def _casefold_key(script: Dict[str, Any]) -> str:
    return _script_key(script).casefold()


def _approved_review_scripts(state: Dict[str, Any], review_artifact: Dict[str, Any] | None) -> List[Dict[str, Any]]:
    scripts = [item for item in state.get("silver_generation_results") or [] if isinstance(item, dict)]
    items = [item for item in (review_artifact or {}).get("items") or [] if isinstance(item, dict)]
    if not items:
        return scripts

    scripts_by_key = {_script_key(script): script for script in scripts}
    scripts_by_casefolded_key: Dict[str, Dict[str, Any] | None] = {}
    for script in scripts:
        key = _casefold_key(script)
        scripts_by_casefolded_key[key] = None if key in scripts_by_casefolded_key else script

    statuses = {str(item.get("review_status") or "").upper() for item in items}
    if not statuses.intersection({"APPROVED", "REJECTED"}):
        return scripts

    def matching_script(item: Dict[str, Any]) -> Dict[str, Any] | None:
        key = _script_key(item)
        script = scripts_by_key.get(key)
        if script is None:
            script = scripts_by_casefolded_key.get(_casefold_key(item))
        if script is None:
            table_name = _table_name(item)
            if table_name:
                matching = [candidate for candidate in scripts if _table_name(candidate).casefold() == table_name.casefold()]
                script = matching[0] if len(matching) == 1 else None
        return script

    approved: List[Dict[str, Any]] = []
    approved_items = [item for item in items if str(item.get("review_status") or "").upper() == "APPROVED"]
    if approved_items:
        for item in approved_items:
            script = matching_script(item)
            if script is None:
                raise ValueError(f"Approved Silver review item has no generated script: {_script_key(item)}")
            approved.append({**script, **item})
        return approved

    rejected_keys = {
        _casefold_key(item)
        for item in items
        if str(item.get("review_status") or "").upper() == "REJECTED"
    }
    return [script for script in scripts if _casefold_key(script) not in rejected_keys]


def run_snowflake_silver_scripts(
    state: Dict[str, Any],
    *,
    review_artifact: Dict[str, Any] | None = None,
    approved_only: bool = False,
) -> Dict[str, Any]:
    run_id = state.get("run_id")
    target_warehouse = str(state.get("target_warehouse") or "databricks").lower()
    if target_warehouse != "snowflake":
        return state
    if not snowflake_silver_execution_enabled():
        logger.info(
            "Snowflake Silver execution disabled; generated scripts remain review artifacts",
            extra=_log_context(run_id, step_name="silver_execution_disabled"),
        )
        return {**state, "snowflake_silver_execution_status": "DISABLED"}

    scripts = _approved_review_scripts(state, review_artifact) if approved_only else [
        item for item in state.get("silver_generation_results") or [] if isinstance(item, dict)
    ]
    if not scripts:
        raise ValueError("Snowflake silver execution enabled but no approved generated silver scripts were found.")

    snowflake_conn = _snowflake_connect()
    try:
        configure_snowflake_runtime_session(snowflake_conn, state)
        reconcile_snowflake_resumed_attempt(snowflake_conn, state)
        for script in scripts:
            validate_snowflake_silver_script(script, catalog_connection=snowflake_conn)

        logger.info(
            "Silver Snowflake contract preflight passed: tables=%d",
            len(scripts),
            extra=_log_context(run_id, step_name="silver_contract_preflight_complete"),
        )

        executed_scripts: List[Dict[str, Any]] = []
        stage_key = "silver_code_execution"
        logger.info(
            "Starting Snowflake Silver execution in external Snowflake warehouse: total_tables=%d tables=%s",
            len(scripts),
            ", ".join(_table_name(script) for script in scripts),
            extra=_log_context(run_id, step_name="silver_execution_start"),
        )
        state = save_external_execution_progress(
            state,
            run_id=run_id,
            layer="silver",
            stage_key=stage_key,
            status="RUNNING",
            total_count=len(scripts),
            completed_count=0,
            message=f"Executing Silver scripts in Snowflake: 0/{len(scripts)} completed.",
        )
        for index, script in enumerate(scripts, start=1):
            table_name = _table_name(script)
            target_table = script.get("target_table") or script.get("silver_table")
            state = save_external_execution_progress(
                state,
                run_id=run_id,
                layer="silver",
                stage_key=stage_key,
                status="RUNNING",
                total_count=len(scripts),
                completed_count=len(executed_scripts),
                current_index=index,
                current_name=table_name,
                current_target=str(target_table or ""),
                message=f"Snowflake Silver execution running: table {index}/{len(scripts)} ({table_name}).",
            )
            logger.info(
                "Executing Snowflake Silver script %d/%d for table %s target=%s; waiting for Snowflake to finish",
                index,
                len(scripts),
                table_name,
                target_table,
                extra=_log_context(run_id, table=table_name, step_name="silver_script_execute_start"),
            )
            started_at = time.monotonic()
            execution_result = execute_snowflake_silver_sql(script, snowflake_conn)
            elapsed_seconds = round(time.monotonic() - started_at, 2)
            executed_scripts.append(execution_result)
            logger.info(
                "Completed Snowflake Silver script %d/%d for table %s statements=%s target=%s elapsed_seconds=%s",
                index,
                len(scripts),
                table_name,
                execution_result.get("statement_count"),
                execution_result.get("target_table"),
                elapsed_seconds,
                extra=_log_context(run_id, table=table_name, step_name="silver_script_execute_complete"),
            )
            state = save_external_execution_progress(
                state,
                run_id=run_id,
                layer="silver",
                stage_key=stage_key,
                status="RUNNING",
                total_count=len(scripts),
                completed_count=len(executed_scripts),
                current_index=index,
                current_name=table_name,
                current_target=str(execution_result.get("target_table") or target_table or ""),
                message=f"Snowflake Silver execution progress: {len(executed_scripts)}/{len(scripts)} completed.",
            )
    finally:
        snowflake_conn.close()

    logger.info(
        "Completed Snowflake Silver external execution: completed_tables=%d total_tables=%d",
        len(executed_scripts),
        len(scripts),
        extra=_log_context(run_id, step_name="silver_execution_complete"),
    )
    final_state = {
        **state,
        "snowflake_silver_execution_status": "COMPLETED",
        "snowflake_silver_execution_results": executed_scripts,
        "snowflake_silver_executed_at": datetime.now(timezone.utc).isoformat(),
    }
    return save_external_execution_progress(
        final_state,
        run_id=run_id,
        layer="silver",
        stage_key=stage_key,
        status="COMPLETED",
        total_count=len(scripts),
        completed_count=len(executed_scripts),
        message=f"Snowflake Silver execution completed: {len(executed_scripts)}/{len(scripts)} scripts finished.",
    )
