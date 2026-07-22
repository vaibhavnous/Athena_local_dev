from __future__ import annotations

import json
import os
import re
import time
import uuid
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import HTTPException

from utilis.db import ai_store_db_writer, config, get_completed_items, get_connection, get_pending_items, timed_stage, update_hitl_items_batch
from utilis.generated_code_paths import generated_code_dir
from utilis.logger import logger


BACKGROUND_WORKER_COUNT = max(1, int(os.getenv("ATHENA_BACKGROUND_WORKERS", "2")))
BACKGROUND_EXECUTOR = ThreadPoolExecutor(max_workers=BACKGROUND_WORKER_COUNT)
BACKGROUND_JOBS: Dict[str, Future] = {}
BACKGROUND_JOB_LOCK = threading.Lock()
SCRIPT_BUNDLE_CACHE_LOCK = threading.Lock()
SCRIPT_BUNDLE_CACHE: Dict[str, Dict[str, Any]] = {}
ACTIVE_CHECKPOINT_STATUSES = {"RUNNING", "PROCESSING", "SUBMITTED", "IN_PROGRESS"}
GENERATION_ARTIFACT_TYPES = {
    "bronze": {"BRONZE_GENERATION", "BRONZE_SCRIPTS", "SFTP_BRONZE_GENERATION"},
    "silver": {"SILVER_GENERATION", "SILVER_SCRIPTS", "SFTP_SILVER_GENERATION"},
    "gold": {"GOLD_GENERATION", "GOLD_SCRIPTS", "SFTP_GOLD_GENERATION"},
}

DATABASE_STAGE_SEQUENCE = [
    ("ingestion", "BRD Ingest"),
    ("memory", "Memory Check"),
    ("requirements", "Requirement Extraction"),
    ("kpis", "KPI Extraction"),
    ("gate1", "KPI Review"),
    ("nomination", "Table Extraction"),
    ("gate2", "Table Review"),
    ("discovery", "Column Extraction"),
    ("profiling", "Column Profiling"),
    ("enrichment", "Semantic Enrichment"),
    ("gate3", "Semantic Review"),
    ("bronze", "Bronze Generation"),
    ("silver", "Silver Generation"),
    ("gold", "Gold Generation"),
]

DATABASE_STAGE_LABELS = dict(DATABASE_STAGE_SEQUENCE)
MINIMUM_RUNTIME_STAGE_KEYS = {
    "ingestion",
    "memory",
    "requirements",
    "kpis",
    "nomination",
    "discovery",
    "schema",
    "enrichment",
}


def _minimum_stage_runtime_seconds() -> float:
    raw = os.getenv("ATHENA_MIN_STAGE_RUNTIME_SECONDS", "10")
    try:
        return max(0.0, float(raw))
    except ValueError:
        logger.warning("Invalid ATHENA_MIN_STAGE_RUNTIME_SECONDS=%r; using 10 seconds", raw)
        return 10.0


def wait_for_minimum_stage_runtime(stage_key: str, started_at: float, state: Optional[Dict[str, Any]] = None) -> None:
    if stage_key not in MINIMUM_RUNTIME_STAGE_KEYS:
        return
    status = str((state or {}).get("status") or "").upper()
    if status in {"FAILED", "HITL_WAIT", "PAUSED_FOR_HITL", "PAUSED_FOR_STAGE_CONFIRMATION"}:
        return
    remaining = _minimum_stage_runtime_seconds() - (time.monotonic() - started_at)
    if remaining > 0:
        time.sleep(remaining)


def run_with_minimum_stage_runtime(stage_key: str, runner, state: Dict[str, Any]) -> Dict[str, Any]:
    started_at = time.monotonic()
    run_id = str(state.get("run_id") or "").strip()
    running_state = {
        **state,
        "status": "RUNNING",
        "background_stage": stage_key,
        "resume_message": f"{DATABASE_STAGE_LABELS.get(stage_key, stage_key).replace('_', ' ').title()} is running.",
    }
    if run_id:
        save_checkpoint_state_timed(run_id, running_state, context=f"{stage_key}:running")

    result = runner(running_state)
    if isinstance(result, dict):
        result = {
            **running_state,
            **result,
            "background_stage": None,
            "last_completed_stage_key": stage_key,
        }
        if run_id:
            save_checkpoint_state_timed(run_id, result, context=f"{stage_key}:complete")
        wait_for_minimum_stage_runtime(stage_key, started_at, result)
    return result


def _bundle_cache_token(path: Path) -> Optional[str]:
    try:
        stat = path.stat()
    except OSError:
        return None
    return f"{stat.st_mtime_ns}:{stat.st_size}"


def _load_script_bundle(path: Path) -> Dict[str, Any]:
    cache_key = str(path.resolve())
    cache_token = _bundle_cache_token(path)
    if cache_token is None:
        return {}

    with SCRIPT_BUNDLE_CACHE_LOCK:
        cached = SCRIPT_BUNDLE_CACHE.get(cache_key)
        if cached and cached.get("token") == cache_token:
            return dict(cached.get("bundle") or {})

    try:
        bundle = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.exception("Failed to load script bundle path=%s", path)
        return {}

    if not isinstance(bundle, dict):
        logger.warning("Ignoring malformed script bundle path=%s", path)
        return {}

    with SCRIPT_BUNDLE_CACHE_LOCK:
        SCRIPT_BUNDLE_CACHE[cache_key] = {"token": cache_token, "bundle": bundle}
    return dict(bundle)


def _gate_label(gate: int, *, source: str = "database") -> str:
    if gate == 1:
        return "KPI Review"
    if gate == 2:
        return "Feed Review" if str(source or "").lower() in {"sftp", "adls_gen2"} else "Table Review"
    if gate == 3:
        return "Semantic Review"
    if gate == 4:
        return "Bronze Review"
    if gate == 5:
        return "Silver Review"
    return f"Gate {gate}"


def _database_stage_index(stage_key: str) -> int:
    for index, (key, _) in enumerate(DATABASE_STAGE_SEQUENCE):
        if key == stage_key:
            return index
    return -1


def _database_next_stage_key(stage_key: str) -> Optional[str]:
    index = _database_stage_index(stage_key)
    if index < 0 or index + 1 >= len(DATABASE_STAGE_SEQUENCE):
        return None
    return DATABASE_STAGE_SEQUENCE[index + 1][0]


def _is_database_review_gate(stage_key: Optional[str]) -> bool:
    return str(stage_key or "") in {"gate1", "gate2", "gate3"}


def _pause_for_stage_confirmation(
    state: Dict[str, Any],
    *,
    run_id: str,
    completed_stage_key: str,
) -> Dict[str, Any]:
    next_stage_key = _database_next_stage_key(completed_stage_key)
    updated = {
        **state,
        "run_id": run_id,
        "status": "PAUSED_FOR_STAGE_CONFIRMATION",
        "awaiting_stage_confirmation": True,
        "last_completed_stage_key": completed_stage_key,
        "last_completed_stage_label": DATABASE_STAGE_LABELS.get(completed_stage_key, completed_stage_key),
        "next_stage_key": next_stage_key,
        "next_stage_label": DATABASE_STAGE_LABELS.get(next_stage_key, next_stage_key) if next_stage_key else None,
        "resume_message": (
            f"{DATABASE_STAGE_LABELS.get(completed_stage_key, completed_stage_key)} finished successfully. "
            f"Confirm before continuing to {DATABASE_STAGE_LABELS.get(next_stage_key, next_stage_key)}."
            if next_stage_key
            else f"{DATABASE_STAGE_LABELS.get(completed_stage_key, completed_stage_key)} finished successfully."
        ),
    }
    save_checkpoint_state(run_id, updated)
    return updated


def _database_stage_runner(stage_key: str):
    if stage_key == "ingestion":
        from nodes.ingestion import ingestion_node

        return ingestion_node
    if stage_key == "memory":
        from nodes.memory_lookup import memory_lookup_node

        return memory_lookup_node
    if stage_key == "requirements":
        from nodes.req_extraction import build_req_extraction_node

        return build_req_extraction_node(llm_provider="azure_openai")
    if stage_key == "kpis":
        from nodes.kpi_extraction import kpi_extraction_node

        return kpi_extraction_node
    if stage_key == "gate1":
        from nodes.hitl import hitl_review_node

        return hitl_review_node
    if stage_key == "nomination":
        from nodes.table_nomination import table_nomination_node

        return table_nomination_node
    if stage_key == "gate2":
        from nodes.hitl import hitl_table_review_node

        return hitl_table_review_node
    if stage_key == "discovery":
        from nodes.metadata_discovery import metadata_discovery_node

        return metadata_discovery_node
    if stage_key == "profiling":
        from nodes.column_profiling import column_profiling_node

        return column_profiling_node
    if stage_key == "enrichment":
        from nodes.semantic_enrichment import semantic_enrichment_node

        return semantic_enrichment_node
    if stage_key == "gate3":
        from nodes.hitl import build_hitl_enrichment_review_node

        return build_hitl_enrichment_review_node()
    if stage_key == "bronze":
        return _run_database_bronze_stage
    if stage_key == "silver":
        return _run_database_silver_stage
    if stage_key == "gold":
        return _run_database_gold_stage
    raise ValueError(f"Unsupported database stage: {stage_key}")


def _run_database_bronze_stage(state: Dict[str, Any]) -> Dict[str, Any]:
    from nodes.bronze_gen import bronze_code_generation_node

    result = bronze_code_generation_node(state)
    if str(result.get("bronze_generation_status") or "").upper() == "COMPLETED":
        return {
            **result,
            "status": "HITL_WAIT",
            "next_gate": 4,
            "resume_message": "Bronze Review is pending. Review generated Bronze scripts before Silver generation.",
        }
    return result


def _run_database_silver_stage(state: Dict[str, Any]) -> Dict[str, Any]:
    from nodes.silver_gen import silver_code_generation_node

    result = silver_code_generation_node(state)
    if str(result.get("silver_generation_status") or "").upper() == "COMPLETED":
        return {
            **result,
            "status": "HITL_WAIT",
            "next_gate": 5,
            "resume_message": "Silver Review is pending. Review generated Silver scripts before Gold generation.",
        }
    return result


def _run_database_gold_stage(state: Dict[str, Any]) -> Dict[str, Any]:
    from nodes.gold_gen import gold_code_generation_node

    result = gold_code_generation_node(state)
    if str(result.get("gold_generation_status") or "").upper().startswith("COMPLETED"):
        return {
            **result,
            "status": "HITL_WAIT",
            "background_stage": None,
            "next_gate": None,
            "next_review_key": "gold_review",
            "gold_review_artifact": {
                "items": [item for item in result.get("gold_generation_results") or [] if isinstance(item, dict)],
            },
            "resume_message": "Gold Review is pending. Review generated Gold scripts before execution.",
        }
    return result


def continue_database_pipeline(
    run_id: str,
    *,
    start_stage_key: str,
    state: Optional[Dict[str, Any]] = None,
    auto_advance: Optional[bool] = None,
) -> Dict[str, Any]:
    working_state = dict(state or load_checkpoint_state(run_id) or {"run_id": run_id})
    working_state["run_id"] = run_id

    if auto_advance is not None:
        working_state["stage_confirmation_enabled"] = not auto_advance
    stage_confirmation_enabled = bool(working_state.get("stage_confirmation_enabled"))

    current_stage_key = start_stage_key
    while current_stage_key:
        stage_started_at = time.monotonic()
        running_state = {
            **working_state,
            "run_id": run_id,
            "status": "RUNNING",
            "background_stage": current_stage_key,
            "awaiting_stage_confirmation": False,
            "error": None,
            "error_type": None,
            "error_message": None,
            "failed_stage": None,
            "failed_stage_label": None,
            "error_stage": None,
            "failed_background_stage": None,
            "interrupted_by_backend_restart": False,
            "resume_message": f"{DATABASE_STAGE_LABELS.get(current_stage_key, current_stage_key)} is running.",
        }
        logger.info(
            "START %s stage=%s",
            DATABASE_STAGE_LABELS.get(current_stage_key, current_stage_key),
            current_stage_key,
            extra={"run_id": run_id, "node": current_stage_key, "stage": current_stage_key, "event_type": "stage_start"},
        )
        save_checkpoint_state_timed(run_id, running_state, context=f"{current_stage_key}:running")
        working_state = running_state

        runner = _database_stage_runner(current_stage_key)
        result = runner(working_state)
        if not isinstance(result, dict):
            raise ValueError(f"Stage {current_stage_key} returned an invalid state.")

        working_state = {**working_state, **result, "run_id": run_id}
        logger.info(
            "END %s stage=%s status=%s duration_seconds=%.3f",
            DATABASE_STAGE_LABELS.get(current_stage_key, current_stage_key),
            current_stage_key,
            working_state.get("status"),
            time.monotonic() - stage_started_at,
            extra={
                "run_id": run_id,
                "node": current_stage_key,
                "stage": current_stage_key,
                "event_type": "stage_end",
                "duration_seconds": round(time.monotonic() - stage_started_at, 3),
            },
        )
        working_state["background_stage"] = None
        working_state["awaiting_stage_confirmation"] = False
        working_state["last_completed_stage_key"] = current_stage_key
        working_state["last_completed_stage_label"] = DATABASE_STAGE_LABELS.get(current_stage_key, current_stage_key)
        working_state["next_stage_key"] = _database_next_stage_key(current_stage_key)
        working_state["next_stage_label"] = DATABASE_STAGE_LABELS.get(working_state["next_stage_key"], working_state["next_stage_key"]) if working_state.get("next_stage_key") else None
        save_checkpoint_state_timed(run_id, working_state, context=f"{current_stage_key}:complete")

        if working_state.get("status") == "FAILED":
            return working_state
        if str(working_state.get("status") or "").upper() in {"HITL_WAIT", "PAUSED_FOR_HITL"}:
            return working_state

        wait_for_minimum_stage_runtime(current_stage_key, stage_started_at, working_state)

        if (
            stage_confirmation_enabled
            and working_state.get("next_stage_key")
            and not _is_database_review_gate(working_state.get("next_stage_key"))
        ):
            return _pause_for_stage_confirmation(
                working_state,
                run_id=run_id,
                completed_stage_key=current_stage_key,
            )

        current_stage_key = working_state.get("next_stage_key")

    working_state["status"] = working_state.get("status") or "PIPELINE_COMPLETED"
    save_checkpoint_state(run_id, working_state)
    return working_state


def _pipeline_schema() -> str:
    return (
        config["azure_sql"].get("pipeline_schema")
        or config["azure_sql"].get("schema_name")
        or "dbo"
    )


def _checkpoint_enriched_payload(checkpoint: Dict[str, Any]) -> Dict[str, Any]:
    payload = checkpoint.get("enriched_metadata") or checkpoint.get("enrichment_review_artifact") or {}
    if isinstance(payload, dict) and isinstance(payload.get("enrichment_artifact"), dict):
        return payload.get("enrichment_artifact") or {}
    return payload if isinstance(payload, dict) else {}


def fetch_json_artifact(run_id: str, artifact_type: str) -> Dict[str, Any]:
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT TOP 1 payload
            FROM [{_pipeline_schema()}].[ai_store]
            WHERE run_id = ? AND artifact_type = ?
            ORDER BY stored_at DESC
            """,
            (run_id, artifact_type),
        )
        row = cursor.fetchone()
        if not row or not row[0]:
            return {}
        return json.loads(row[0])
    finally:
        conn.close()


def fetch_run_summary(run_id: str) -> List[Dict[str, Any]]:
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT
                stage,
                artifact_type,
                faithfulness_status,
                retry_count,
                input_tokens,
                output_tokens,
                token_count,
                cost_usd,
                stored_at
            FROM [{_pipeline_schema()}].[ai_store]
            WHERE run_id = ?
            ORDER BY stored_at
            """,
            (run_id,),
        )
        rows = cursor.fetchall()
        return [
            {
                "stage": row[0],
                "artifact_type": row[1],
                "faithfulness_status": row[2],
                "retry_count": row[3],
                "input_tokens": row[4],
                "output_tokens": row[5],
                "token_count": row[6],
                "cost_usd": row[7],
                "stored_at": row[8],
            }
            for row in rows
        ]
    finally:
        conn.close()


def load_checkpoint_fields(run_id: str, *fields: str) -> Dict[str, Any]:
    safe_fields = [field for field in fields if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", field or "")]
    if not safe_fields:
        return {}

    select_list = ", ".join(
        f"JSON_VALUE(full_state_json, '$.{field}') AS [{field}]"
        for field in safe_fields
    )

    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT TOP 1 {select_list}
            FROM [{_pipeline_schema()}].[kpi_checkpoints]
            WHERE run_id = ?
            ORDER BY checkpoint_at DESC
            """,
            (run_id,),
        )
        row = cursor.fetchone()
        if not row:
            return {}
        return {
            field: row[index]
            for index, field in enumerate(safe_fields)
            if row[index] is not None
        }
    finally:
        conn.close()


def load_checkpoint_state(run_id: str) -> Optional[Dict[str, Any]]:
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT TOP 1 full_state_json
            FROM [{_pipeline_schema()}].[kpi_checkpoints]
            WHERE run_id = ?
            ORDER BY checkpoint_at DESC
            """,
            (run_id,),
        )
        row = cursor.fetchone()
        if not row or not row[0]:
            return None
        return json.loads(row[0])
    finally:
        conn.close()


def save_checkpoint_state(run_id: str, state: Dict[str, Any]) -> None:
    conn = get_connection()
    try:
        cursor = conn.cursor()
        state_json = json.dumps(state, default=str)
        cursor.execute(
            f"""
            MERGE [{_pipeline_schema()}].[kpi_checkpoints] AS target
            USING (VALUES (?)) AS source (run_id)
            ON target.run_id = source.run_id
            WHEN MATCHED THEN UPDATE SET full_state_json = ?, checkpoint_at = GETUTCDATE()
            WHEN NOT MATCHED THEN INSERT (run_id, full_state_json, checkpoint_at) VALUES (?, ?, GETUTCDATE());
            """,
            (run_id, state_json, run_id, state_json),
        )
        conn.commit()
    finally:
        conn.close()


def _checkpoint_slow_seconds() -> float:
    raw = os.getenv("ATHENA_CHECKPOINT_SLOW_SECONDS", "2")
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 2.0


def save_checkpoint_state_timed(run_id: str, state: Dict[str, Any], *, context: str) -> None:
    stage = state.get("background_stage") or state.get("failed_background_stage") or state.get("last_completed_stage_key")
    logger.info(
        "Saving checkpoint context=%s status=%s background_stage=%s",
        context,
        state.get("status"),
        stage,
        extra={"run_id": run_id, "node": stage or "checkpoint", "stage": stage or "checkpoint", "step_name": "checkpoint_save_start"},
    )
    started = time.perf_counter()
    try:
        save_checkpoint_state(run_id, state)
    except Exception:
        logger.exception(
            "Checkpoint save failed context=%s",
            context,
            extra={"run_id": run_id, "node": stage or "checkpoint", "stage": stage or "checkpoint", "step_name": "checkpoint_save_failed"},
        )
        raise

    elapsed = time.perf_counter() - started
    log = logger.warning if elapsed >= _checkpoint_slow_seconds() else logger.info
    log(
        "Checkpoint save finished context=%s elapsed_seconds=%.3f",
        context,
        elapsed,
        extra={
            "run_id": run_id,
            "node": stage or "checkpoint",
            "stage": stage or "checkpoint",
            "step_name": "checkpoint_save_complete",
            "duration_seconds": round(elapsed, 3),
        },
    )


def _interrupted_checkpoint_state(state: Dict[str, Any], reason: str) -> Dict[str, Any]:
    failed_stage = state.get("background_stage") or state.get("failed_background_stage") or state.get("last_failed_stage_key") or "pipeline"
    return {
        **state,
        "status": "FAILED",
        "background_stage": None,
        "failed_background_stage": failed_stage,
        "error": reason,
        "error_type": "InterruptedRun",
        "error_message": reason,
        "resume_message": "Backend restarted while this run was active. Use Retry Failed Stage or Resume from Failure.",
        "interrupted_by_backend_restart": True,
        "interrupted_at": time.time(),
    }


def mark_interrupted_background_runs_on_startup() -> int:
    if str(os.getenv("ATHENA_MARK_INTERRUPTED_RUNS_ON_STARTUP", "true")).lower() in {"0", "false", "no", "off"}:
        return 0

    limit = max(1, int(os.getenv("ATHENA_INTERRUPTED_RUN_RECOVERY_LIMIT", "50")))
    conn = get_connection()
    rows: List[tuple[str, str]] = []
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT TOP ({limit}) run_id, full_state_json
            FROM [{_pipeline_schema()}].[kpi_checkpoints]
            WHERE JSON_VALUE(full_state_json, '$.status') IN ('RUNNING', 'PROCESSING', 'SUBMITTED', 'IN_PROGRESS')
               OR NULLIF(JSON_VALUE(full_state_json, '$.background_stage'), '') IS NOT NULL
            ORDER BY checkpoint_at DESC
            """
        )
        rows = [(str(row[0]), str(row[1] or "")) for row in cursor.fetchall()]
    finally:
        conn.close()

    reason = "Backend process restarted while this run was active."
    recovered = 0
    for run_id, state_json in rows:
        try:
            state = json.loads(state_json) if state_json else {}
        except Exception:
            logger.exception("Skipping malformed interrupted checkpoint run_id=%s", run_id)
            continue

        status = str(state.get("status") or "").upper()
        if status not in ACTIVE_CHECKPOINT_STATUSES and not state.get("background_stage"):
            continue

        save_checkpoint_state(run_id, _interrupted_checkpoint_state({**state, "run_id": run_id}, reason))
        recovered += 1

    if recovered:
        logger.warning("Marked interrupted background runs as failed/retryable count=%s", recovered)
    return recovered


def mark_run_processing(run_id: str, stage: str) -> None:
    checkpoint = load_checkpoint_state(run_id) or {"run_id": run_id}
    checkpoint.update(
        {
            "run_id": run_id,
            "status": "PROCESSING",
            "background_stage": stage,
            "next_gate": None,
            "next_review_key": None,
            "stage_confirmation": None,
            "awaiting_stage_confirmation": False,
        }
    )
    save_checkpoint_state_timed(run_id, checkpoint, context=f"{stage}:processing")


def _active_background_job_count_locked() -> int:
    return sum(1 for future in BACKGROUND_JOBS.values() if future and not future.done())


def background_capacity_snapshot() -> Dict[str, int]:
    with BACKGROUND_JOB_LOCK:
        active = _active_background_job_count_locked()
    return {
        "workers": BACKGROUND_WORKER_COUNT,
        "active": active,
        "available": max(0, BACKGROUND_WORKER_COUNT - active),
    }


def ensure_background_capacity_locked() -> None:
    active = _active_background_job_count_locked()
    if active < BACKGROUND_WORKER_COUNT:
        return
    raise HTTPException(
        status_code=429,
        detail=(
            f"Backend background capacity is full: {active}/{BACKGROUND_WORKER_COUNT} active jobs. "
            "Wait for one run to pause/finish, then retry."
        ),
    )


def submit_background(run_id: str, stage: str, fn, *args) -> Future:
    job_key = f"{run_id}:{stage}"
    with BACKGROUND_JOB_LOCK:
        existing = BACKGROUND_JOBS.get(job_key)
        if existing and not existing.done():
            logger.info("Background %s already running for run_id=%s", stage, run_id)
            return existing

        ensure_background_capacity_locked()
        mark_run_processing(run_id, stage)
        future = BACKGROUND_EXECUTOR.submit(fn, *args)
        BACKGROUND_JOBS[job_key] = future

    def _record_background_result(done: Future) -> None:
        try:
            result = done.result()
            checkpoint = load_checkpoint_state(run_id) or {"run_id": run_id}
            if isinstance(result, dict):
                checkpoint.update(result)
            checkpoint.update({"run_id": run_id, "background_stage": None})
            if checkpoint.get("status") == "PROCESSING":
                checkpoint["status"] = checkpoint.get("semantic_enrichment_status") or checkpoint.get("table_nomination_status") or "COMPLETED"
            save_checkpoint_state_timed(run_id, checkpoint, context=f"{stage}:background_complete")
        except Exception as exc:
            logger.exception("Background %s failed for run_id=%s", stage, run_id)
            try:
                checkpoint = load_checkpoint_state(run_id) or {"run_id": run_id}
            except Exception:
                logger.exception("Failed to load checkpoint while recording background failure run_id=%s stage=%s", run_id, stage)
                checkpoint = {"run_id": run_id}
            checkpoint.update(
                {
                    "run_id": run_id,
                    "status": "FAILED",
                    "background_stage": None,
                    "failed_background_stage": checkpoint.get("failed_background_stage") or stage,
                    "error": str(exc),
                }
            )
            try:
                save_checkpoint_state_timed(run_id, checkpoint, context=f"{stage}:background_failed")
            except Exception:
                logger.exception("Failed to save background failure checkpoint run_id=%s stage=%s", run_id, stage)
        finally:
            with BACKGROUND_JOB_LOCK:
                if BACKGROUND_JOBS.get(job_key) is done:
                    BACKGROUND_JOBS.pop(job_key, None)

    future.add_done_callback(_record_background_result)
    return future


def list_runs(limit: int = 50) -> List[Dict[str, Any]]:
    conn = get_connection()
    try:
        cursor = conn.cursor()
        try:
            cursor.timeout = max(1, int(os.getenv("ATHENA_SQL_QUERY_TIMEOUT_SECONDS", "5")))
        except Exception:
            # Some drivers may not expose cursor timeout; ignore.
            pass
        # ponytail: history may briefly show an in-flight checkpoint; remove the
        # hint once the metadata database uses snapshot isolation.
        cursor.execute(
            f"""
            SELECT TOP ({limit}) run_id, checkpoint_at AS last_activity
            FROM [{_pipeline_schema()}].[kpi_checkpoints] WITH (READUNCOMMITTED)
            ORDER BY checkpoint_at DESC
            """
        )
        rows = cursor.fetchall()
        # ponytail: current runs are checkpoint-backed; querying four legacy
        # stores when this table is empty made an empty history wait for timeout.
        return [
            {
                "run_id": row[0],
                "last_activity": row[1],
                # Avoid loading the potentially large checkpoint blob for the list.
                # The selected run's detail request hydrates the complete state.
                "checkpoint": {},
            }
            for row in rows
            if row and row[0]
        ]
    except Exception as exc:
        # If the combined query is slow (missing indexes / large tables), fall back
        # to a cheaper query so the UI can still hydrate.
        try:
            cursor = conn.cursor()
            try:
                cursor.timeout = max(1, int(os.getenv("ATHENA_SQL_QUERY_TIMEOUT_SECONDS", "5")))
            except Exception:
                pass
            cursor.execute(
                f"""
                SELECT TOP ({limit}) run_id, MAX(checkpoint_at) AS last_activity
                FROM [{_pipeline_schema()}].[kpi_checkpoints] WITH (READUNCOMMITTED)
                GROUP BY run_id
                ORDER BY MAX(checkpoint_at) DESC
                """
            )
            rows = cursor.fetchall()
            return [
                {"run_id": row[0], "last_activity": row[1]}
                for row in rows
                if row and row[0]
            ]
        except Exception:
            raise
    finally:
        conn.close()


def _table_key(item: Dict[str, Any]) -> str:
    return f"{item.get('database_name', '')}.{item.get('schema_name', '')}.{item.get('table_name', '')}"


def _run_slug(run_id: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]+", "_", str(run_id or "run")).strip("_")[:48] or "run"


def _script_output_dirs(layer: str, target_warehouse: Optional[str] = None) -> List[Path]:
    default_dir = generated_code_dir(layer)
    if layer not in {"bronze", "silver", "gold"}:
        return [default_dir]

    snowflake_dir = generated_code_dir("snowflake", layer)
    if str(target_warehouse or "").lower() == "snowflake":
        return [snowflake_dir, default_dir]
    if target_warehouse is None:
        return [default_dir, snowflake_dir]
    return [default_dir]


def _script_bundle_path(layer: str, run_id: str, target_warehouse: Optional[str] = None) -> Path:
    output_dirs = _script_output_dirs(layer, target_warehouse)
    for output_dir in output_dirs:
        run_scoped = output_dir / f"{_run_slug(run_id)}_{layer}_scripts.json"
        if run_scoped.exists():
            return run_scoped
        latest = output_dir / f"{layer}_scripts.json"
        if latest.exists():
            return latest
    return output_dirs[0] / f"{layer}_scripts.json"


def _script_matches_run(
    *,
    item: Dict[str, Any],
    bundle_run_id: Any,
    requested_run_id: str,
    script_bodies: List[str],
) -> bool:
    item_run_id = item.get("run_id")
    if item_run_id:
        return str(item_run_id) == requested_run_id
    if bundle_run_id:
        return str(bundle_run_id) == requested_run_id
    return any(requested_run_id in body for body in script_bodies if body)


def _read_script_body(script_path_value: Any) -> str:
    script_path = Path(str(script_path_value or "")) if script_path_value else None
    if script_path and script_path.exists() and script_path.is_file():
        return script_path.read_text(encoding="utf-8")
    return ""


def _dedupe_scripts(scripts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: List[Dict[str, Any]] = []
    seen = set()
    for script in scripts:
        key = (
            script.get("script_path"),
            script.get("dimension_script_path"),
            script.get("target_table"),
            script.get("source_table"),
            script.get("table"),
            script.get("kpi_name"),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(script)
    return deduped


def _normalize_bronze_script(item: Dict[str, Any]) -> Dict[str, Any]:
    """Backfill lineage fields for Bronze bundles written before the schema fix."""
    row = dict(item)
    table = str(row.get("table") or row.get("table_name") or "").strip()
    source_table = str(row.get("source_table") or "").strip()
    if not source_table and table:
        source_table = ".".join(
            part for part in (row.get("database_name"), row.get("schema_name"), table) if str(part or "").strip()
        )
    target_table = str(row.get("target_table") or "").strip()
    if not target_table and table:
        target_table = ".".join(
            part
            for part in (
                row.get("bronze_catalog"),
                row.get("bronze_schema"),
                f"bronze_{table}",
            )
            if str(part or "").strip()
        )
    if source_table:
        row.setdefault("source_table", source_table)
        row.setdefault("source", source_table)
    if target_table:
        row.setdefault("target_table", target_table)
        row.setdefault("target", target_table)
    return row


def _scripts_from_checkpoint(
    checkpoint: Dict[str, Any],
    result_key: str,
    generated_at_key: str,
) -> Dict[str, Any]:
    scripts: List[Dict[str, Any]] = []
    for item in checkpoint.get(result_key) or []:
        script_body = str(item.get("script_body") or "").strip()
        if not script_body:
            script_body = _read_script_body(item.get("script_path"))
        dimension_script_body = _read_script_body(item.get("dimension_script_path"))
        row = _normalize_bronze_script({
            **item,
            "run_id": item.get("run_id") or checkpoint.get("run_id"),
            "script_body": script_body,
        }) if result_key == "bronze_generation_results" else {
            **item,
            "run_id": item.get("run_id") or checkpoint.get("run_id"),
            "script_body": script_body,
        }
        if dimension_script_body:
            row["dimension_script_body"] = dimension_script_body
        scripts.append(row)
    return {
        "run_id": checkpoint.get("run_id"),
        "generated_at": checkpoint.get(generated_at_key),
        "scripts": _dedupe_scripts(scripts),
    }


def load_bronze_scripts(run_id: str, checkpoint: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    checkpoint = checkpoint or {}
    bundle_path = _script_bundle_path("bronze", run_id, checkpoint.get("target_warehouse"))
    if not bundle_path.exists():
        return _scripts_from_checkpoint(checkpoint, "bronze_generation_results", "bronze_generated_at")

    bundle = _load_script_bundle(bundle_path)
    if not bundle:
        return _scripts_from_checkpoint(checkpoint, "bronze_generation_results", "bronze_generated_at")
    bundle_run_id = bundle.get("run_id")
    scripts: List[Dict[str, Any]] = []
    for item in bundle.get("scripts", []):
        if not isinstance(item, dict):
            continue
        script_body = str(item.get("script_body") or "").strip()
        if not script_body:
            script_body = _read_script_body(item.get("script_path"))
        if not _script_matches_run(
            item=item,
            bundle_run_id=bundle_run_id,
            requested_run_id=run_id,
            script_bodies=[script_body],
        ):
            continue
        scripts.append(_normalize_bronze_script({**item, "script_body": script_body}))

    if not scripts and checkpoint:
        return _scripts_from_checkpoint(checkpoint, "bronze_generation_results", "bronze_generated_at")

    return {
        "run_id": bundle_run_id,
        "generated_at": bundle.get("generated_at"),
        "scripts": _dedupe_scripts(scripts),
    }


def load_silver_scripts(run_id: str, checkpoint: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    checkpoint = checkpoint or {}
    bundle_path = _script_bundle_path("silver", run_id, checkpoint.get("target_warehouse"))
    if not bundle_path.exists():
        return _scripts_from_checkpoint(checkpoint, "silver_generation_results", "silver_generated_at")

    bundle = _load_script_bundle(bundle_path)
    if not bundle:
        return _scripts_from_checkpoint(checkpoint, "silver_generation_results", "silver_generated_at")
    bundle_run_id = bundle.get("run_id")
    scripts: List[Dict[str, Any]] = []
    for item in bundle.get("scripts", []):
        if not isinstance(item, dict):
            continue
        script_body = str(item.get("script_body") or "").strip()
        if not script_body:
            script_body = _read_script_body(item.get("script_path"))
        if not _script_matches_run(
            item=item,
            bundle_run_id=bundle_run_id,
            requested_run_id=run_id,
            script_bodies=[script_body],
        ):
            continue
        scripts.append(
            {
                **item,
                "script_body": script_body,
            }
        )

    if not scripts and checkpoint:
        return _scripts_from_checkpoint(checkpoint, "silver_generation_results", "silver_generated_at")

    return {
        "run_id": bundle_run_id,
        "generated_at": bundle.get("generated_at"),
        "scripts": _dedupe_scripts(scripts),
    }


def load_gold_scripts(run_id: str, checkpoint: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    checkpoint = checkpoint or {}
    bundle_path = _script_bundle_path("gold", run_id, checkpoint.get("target_warehouse"))
    if not bundle_path.exists():
        return _scripts_from_checkpoint(checkpoint, "gold_generation_results", "gold_generated_at")

    bundle = _load_script_bundle(bundle_path)
    if not bundle:
        return _scripts_from_checkpoint(checkpoint, "gold_generation_results", "gold_generated_at")
    bundle_run_id = bundle.get("run_id")
    scripts: List[Dict[str, Any]] = []
    for item in bundle.get("scripts", []):
        if not isinstance(item, dict):
            continue
        script_body = str(item.get("script_body") or "").strip()
        if not script_body:
            script_body = _read_script_body(item.get("script_path"))
        dimension_script_body = _read_script_body(item.get("dimension_script_path"))
        if not _script_matches_run(
            item=item,
            bundle_run_id=bundle_run_id,
            requested_run_id=run_id,
            script_bodies=[script_body, dimension_script_body],
        ):
            continue
        scripts.append(
            {
                **item,
                "script_body": script_body,
                "dimension_script_body": dimension_script_body,
            }
        )

    if not scripts and checkpoint:
        return _scripts_from_checkpoint(checkpoint, "gold_generation_results", "gold_generated_at")

    return {
        "run_id": bundle_run_id,
        "generated_at": bundle.get("generated_at"),
        "scripts": _dedupe_scripts(scripts),
    }


def _lineage_node_id(layer: str, name: str) -> str:
    safe_layer = re.sub(r"[^a-z0-9]+", "-", str(layer or "").lower()).strip("-") or "layer"
    safe_name = re.sub(r"[^a-z0-9_.:]+", "-", str(name or "").lower()).strip("-") or "node"
    return f"{safe_layer}:{safe_name}"


def _append_lineage_edge(
    edges: List[Dict[str, Any]],
    seen_edges: set[tuple[str, str, str]],
    *,
    source: str,
    target: str,
    edge_type: str,
    **metadata: Any,
) -> None:
    key = (source, target, edge_type)
    if key in seen_edges:
        return
    seen_edges.add(key)
    edges.append(
        {
            "id": f"{source}->{target}:{edge_type}",
            "source": source,
            "target": target,
            "type": edge_type,
            **metadata,
        }
    )


def _lineage_safe_entity(value: str, fallback: str = "source") -> str:
    raw = str(value or "").strip().strip("/\\")
    if not raw:
        return fallback
    name = re.split(r"[/\\]", raw)[-1] or raw
    if "." in name:
        name = name.rsplit(".", 1)[0]
    safe = re.sub(r"[^a-zA-Z0-9_]+", "_", name).strip("_").lower()
    return safe or fallback


def _lineage_table_name(item: Dict[str, Any]) -> str:
    schema = str(item.get("schema") or item.get("table_schema") or item.get("source_schema") or "").strip()
    table = str(
        item.get("table")
        or item.get("table_name")
        or item.get("source_table")
        or item.get("entity")
        or ""
    ).strip()
    if schema and table and "." not in table:
        return f"{schema}.{table}"
    return table or schema


def _checkpoint_lineage_sources(checkpoint: Dict[str, Any]) -> List[Dict[str, str]]:
    sources: List[Dict[str, str]] = []

    def add_source(
        *,
        source: Any,
        entity: Any = None,
        bronze: Any = None,
        silver: Any = None,
        gold: Any = None,
    ) -> None:
        source_name = str(source or "").strip()
        if not source_name:
            return
        entity_name = _lineage_safe_entity(str(entity or source_name), "source")
        row = {
            "source": source_name,
            "entity": entity_name,
            "bronze": str(bronze or "").strip(),
            "silver": str(silver or "").strip(),
            "gold": str(gold or "").strip(),
        }
        if not any(existing["source"] == row["source"] for existing in sources):
            sources.append(row)

    for item in checkpoint.get("bronze_generation_results") or []:
        if not isinstance(item, dict):
            continue
        config_payload = item.get("bronze_config") or item.get("generated_bronze_config") or {}
        add_source(
            source=(
                item.get("source")
                or item.get("source_table")
                or item.get("source_path")
                or config_payload.get("source_path")
                or config_payload.get("source_table")
            ),
            entity=item.get("entity") or item.get("table") or item.get("table_name"),
            bronze=item.get("target") or item.get("target_table") or config_payload.get("target_table"),
        )

    for item in checkpoint.get("silver_generation_results") or []:
        if not isinstance(item, dict):
            continue
        add_source(
            source=item.get("bronze_table") or item.get("source_table"),
            entity=item.get("entity") or item.get("table") or item.get("table_name"),
            bronze=item.get("bronze_table") or item.get("source_table"),
            silver=item.get("silver_table") or item.get("target_table"),
        )

    for feed in checkpoint.get("file_feeds") or []:
        if not isinstance(feed, dict):
            continue
        add_source(
            source=(
                feed.get("cloud_path")
                or feed.get("databricks_source_path")
                or feed.get("remote_path")
                or feed.get("feed_name")
                or feed.get("feed_id")
            ),
            entity=feed.get("entity") or feed.get("feed_name") or feed.get("feed_id"),
        )

    candidate_feed = checkpoint.get("candidate_feed")
    if isinstance(candidate_feed, dict):
        add_source(
            source=(
                candidate_feed.get("cloud_path")
                or candidate_feed.get("databricks_source_path")
                or candidate_feed.get("remote_path")
                or candidate_feed.get("feed_name")
                or candidate_feed.get("feed_id")
            ),
            entity=candidate_feed.get("entity") or candidate_feed.get("feed_name") or candidate_feed.get("feed_id"),
        )

    for table in (checkpoint.get("certified_tables") or checkpoint.get("nominated_tables") or []):
        if not isinstance(table, dict):
            continue
        table_name = _lineage_table_name(table)
        add_source(source=table_name, entity=table.get("table_name") or table.get("table") or table_name)

    if not sources:
        source_type = str(checkpoint.get("source") or "database").lower()
        entity = checkpoint.get("sftp_entity") or checkpoint.get("entity") or checkpoint.get("brd_filename") or "source"
        if source_type == "adls_gen2":
            source_name = checkpoint.get("databricks_source_path") or checkpoint.get("adls_source_root") or f"adls://{entity}"
        elif source_type == "sftp":
            source_name = checkpoint.get("landing_path") or f"sftp://{entity}"
        else:
            source_name = f"database://{entity}"
        add_source(source=source_name, entity=entity)

    return sources


def _append_checkpoint_lineage_fallback(
    *,
    run_id: str,
    checkpoint: Dict[str, Any],
    ensure_node,
    edges: List[Dict[str, Any]],
    seen_edges: set[tuple[str, str, str]],
) -> bool:
    sources = _checkpoint_lineage_sources(checkpoint)
    if not sources:
        return False

    source_type = str(checkpoint.get("source") or "database").lower()
    bronze_schema = str(checkpoint.get("bronze_schema") or "bronze")
    silver_schema = str(checkpoint.get("silver_schema") or "silver")
    gold_schema = str(checkpoint.get("gold_schema") or "gold")

    for item in sources:
        entity = item["entity"]
        source_name = item["source"]
        bronze_name = item["bronze"] or (
            f"{bronze_schema}.{entity}_raw" if source_type in {"sftp", "adls_gen2"} else f"main.{bronze_schema}.bronze_{entity}"
        )
        silver_name = item["silver"] or f"{silver_schema}.{entity}_clean"
        gold_name = item["gold"] or f"{gold_schema}.fact_{entity}"

        source_id = ensure_node("source", source_name, source_name, kind="source", fallback=True)
        bronze_id = ensure_node("bronze", bronze_name, bronze_name, kind="table", fallback=True)
        silver_id = ensure_node("silver", silver_name, silver_name, kind="table", fallback=True)
        gold_id = ensure_node("gold", gold_name, gold_name, kind="fact", fallback=True)

        _append_lineage_edge(
            edges,
            seen_edges,
            source=source_id,
            target=bronze_id,
            edge_type="pipeline",
            status=str(checkpoint.get("bronze_generation_status") or "DEMO_FALLBACK"),
            certified=False,
            fallback=True,
            run_id=run_id,
        )
        _append_lineage_edge(
            edges,
            seen_edges,
            source=bronze_id,
            target=silver_id,
            edge_type="pipeline",
            status=str(checkpoint.get("silver_generation_status") or "DEMO_FALLBACK"),
            certified=False,
            fallback=True,
            run_id=run_id,
        )
        _append_lineage_edge(
            edges,
            seen_edges,
            source=silver_id,
            target=gold_id,
            edge_type="pipeline",
            status=str(checkpoint.get("gold_generation_status") or "DEMO_FALLBACK"),
            certified=False,
            fallback=True,
            run_id=run_id,
        )

    return True


def build_run_lineage(run_id: str, checkpoint: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    checkpoint = checkpoint or load_checkpoint_state(run_id) or {"run_id": run_id}
    bronze = load_bronze_scripts(run_id, checkpoint)
    silver = load_silver_scripts(run_id, checkpoint)
    gold = load_gold_scripts(run_id, checkpoint)
    enriched_payload = fetch_json_artifact(run_id, "ENRICHED_METADATA") or _checkpoint_enriched_payload(checkpoint)
    gold_contract = fetch_json_artifact(run_id, "GOLD_GENERATION_CONTRACT") or checkpoint.get("gold_generation_contract") or {}

    nodes: List[Dict[str, Any]] = []
    edges: List[Dict[str, Any]] = []
    seen_nodes: set[str] = set()
    seen_edges: set[tuple[str, str, str]] = set()

    def ensure_node(layer: str, name: str, label: str, **metadata: Any) -> str:
        node_id = _lineage_node_id(layer, name)
        if node_id not in seen_nodes:
            seen_nodes.add(node_id)
            nodes.append(
                {
                    "id": node_id,
                    "layer": layer,
                    "name": name,
                    "label": label,
                    **metadata,
                }
            )
        return node_id

    for item in (bronze.get("scripts") or []):
        if not isinstance(item, dict):
            continue
        source_name = str(item.get("source") or item.get("source_table") or "")
        target_name = str(item.get("target") or item.get("target_table") or "")
        if not source_name or not target_name:
            continue
        source_id = ensure_node("source", source_name, source_name, kind="table")
        bronze_id = ensure_node("bronze", target_name, target_name, kind="table")
        _append_lineage_edge(
            edges,
            seen_edges,
            source=source_id,
            target=bronze_id,
            edge_type="pipeline",
            status=str(item.get("status") or "APPROVED"),
            certified=True,
        )

    for item in (silver.get("scripts") or []):
        if not isinstance(item, dict):
            continue
        source_name = str(item.get("source_table") or "")
        target_name = str(item.get("target_table") or "")
        if not source_name or not target_name:
            continue
        bronze_id = ensure_node("bronze", source_name, source_name, kind="table")
        silver_id = ensure_node("silver", target_name, target_name, kind="table")
        _append_lineage_edge(
            edges,
            seen_edges,
            source=bronze_id,
            target=silver_id,
            edge_type="pipeline",
            status=str(item.get("status") or "APPROVED"),
            certified=True,
        )

    for item in (gold.get("scripts") or []):
        if not isinstance(item, dict):
            continue
        source_name = str(item.get("source_table") or "")
        target_name = str(item.get("target_table") or "")
        if not source_name or not target_name:
            continue
        silver_id = ensure_node("silver", source_name, source_name, kind="table")
        gold_id = ensure_node("gold", target_name, target_name, kind="fact")
        _append_lineage_edge(
            edges,
            seen_edges,
            source=silver_id,
            target=gold_id,
            edge_type="pipeline",
            status=str(item.get("status") or "APPROVED"),
            certified=True,
        )
        dimension_script_path = str(item.get("dimension_script_path") or "")
        if dimension_script_path:
            base_name = os.path.basename(dimension_script_path).replace(".py", "")
            dim_id = ensure_node("gold", base_name, base_name, kind="dimension_script")
            _append_lineage_edge(
                edges,
                seen_edges,
                source=silver_id,
                target=dim_id,
                edge_type="dimension",
                status=str(item.get("status") or "APPROVED"),
                certified=True,
            )

    certified_joins = enriched_payload.get("certified_joins") if isinstance(enriched_payload, dict) else []
    for join in certified_joins or []:
        left_name = str(join.get("left_table") or "")
        right_name = str(join.get("right_table") or "")
        if not left_name or not right_name:
            continue
        left_id = ensure_node("logical", left_name, left_name, kind="logical_table")
        right_id = ensure_node("logical", right_name, right_name, kind="logical_table")
        _append_lineage_edge(
            edges,
            seen_edges,
            source=left_id,
            target=right_id,
            edge_type="fk",
            certified=True,
            source_column=join.get("left_column"),
            target_column=join.get("right_column"),
            constraint_name=join.get("constraint_name"),
            confidence=join.get("confidence"),
        )

    join_candidates = enriched_payload.get("join_candidates") if isinstance(enriched_payload, dict) else []
    for join in join_candidates or []:
        left_name = str(join.get("left_table") or "")
        right_name = str(join.get("right_table") or "")
        if not left_name or not right_name:
            continue
        left_id = ensure_node("logical", left_name, left_name, kind="logical_table")
        right_id = ensure_node("logical", right_name, right_name, kind="logical_table")
        _append_lineage_edge(
            edges,
            seen_edges,
            source=left_id,
            target=right_id,
            edge_type="heuristic",
            certified=False,
            source_column=join.get("left_column"),
            target_column=join.get("right_column"),
            confidence=join.get("confidence"),
        )

    for mapping in (gold_contract.get("kpi_mappings") or []):
        if not isinstance(mapping, dict):
            continue
        kpi_name = str(mapping.get("kpi_name") or "")
        source_table = str(mapping.get("source_silver_table") or "")
        if not kpi_name:
            continue
        kpi_id = ensure_node("kpi", kpi_name, kpi_name, kind="kpi", readiness=mapping.get("readiness"))
        if source_table:
            silver_id = ensure_node("silver", source_table, source_table, kind="table")
            _append_lineage_edge(
                edges,
                seen_edges,
                source=silver_id,
                target=kpi_id,
                edge_type="kpi",
                certified=bool(mapping.get("join_paths")),
                aggregation=(mapping.get("measure") or {}).get("aggregation"),
            )

    fallback_used = False
    if not any(node.get("layer") in {"source", "bronze", "silver", "gold"} for node in nodes):
        fallback_used = _append_checkpoint_lineage_fallback(
            run_id=run_id,
            checkpoint=checkpoint,
            ensure_node=ensure_node,
            edges=edges,
            seen_edges=seen_edges,
        )

    return {
        "run_id": run_id,
        "nodes": nodes,
        "edges": edges,
        "summary": {
            "source_count": sum(1 for node in nodes if node.get("layer") == "source"),
            "bronze_count": sum(1 for node in nodes if node.get("layer") == "bronze"),
            "silver_count": sum(1 for node in nodes if node.get("layer") == "silver"),
            "gold_count": sum(1 for node in nodes if node.get("layer") == "gold"),
            "fk_edge_count": sum(1 for edge in edges if edge.get("type") == "fk"),
            "heuristic_edge_count": sum(1 for edge in edges if edge.get("type") == "heuristic"),
            "fallback": fallback_used,
            "mode": "checkpoint_fallback" if fallback_used else "artifact_backed",
        },
    }


def build_pipeline_steps(
    *,
    source: str,
    checkpoint: Dict[str, Any],
    summary: List[Dict[str, Any]],
    pending_gate1: List[Dict[str, Any]],
    completed_gate1: List[Dict[str, Any]],
    nominated_tables: List[Dict[str, Any]],
    certified_tables: List[Dict[str, Any]],
    enriched_payload: Dict[str, Any],
    gate3_payload: Dict[str, Any],
    bronze_generation_completed: bool,
    silver_generation_completed: bool,
    gold_generation_completed: bool,
) -> List[Dict[str, str]]:
    source = str(source or "database").lower()
    artifact_types = {str(row.get("artifact_type") or "") for row in summary}
    stages = {str(row.get("stage") or "").lower() for row in summary}

    def artifact_failed(artifact_type: str) -> bool:
        target = str(artifact_type or "").upper()
        return any(
            str(row.get("artifact_type") or "").upper() == target
            and str(row.get("faithfulness_status") or "").upper() == "FAILED"
            for row in summary
            if isinstance(row, dict)
        )

    def has_stage(text: str) -> bool:
        needle = text.lower()
        return any(needle in stage for stage in stages)

    def status_completed(value: Any) -> bool:
        return str(value or "").upper() in {"COMPLETED", "COMPLETED_WITH_WARNINGS", "SKIPPED", "HANDOFF_ONLY"}

    if source in {"sftp", "adls_gen2"}:
        gate1_decision = (checkpoint.get("gate1") or {}).get("decision")
        gate2_decision = (checkpoint.get("gate2") or {}).get("decision")
        gate4_decision = (checkpoint.get("gate4") or {}).get("decision")
        gate5_decision = (checkpoint.get("gate5") or {}).get("decision")
        silver_merge_key_review_decision = str(checkpoint.get("silver_merge_key_review_decision") or "").upper()
        source_label = "ADLS Gen2" if source == "adls_gen2" else "SFTP"
        steps = [
            {
                "key": "ingestion",
                "label": "BRD Ingest",
                "complete": bool(
                    checkpoint.get("fingerprint")
                    or checkpoint.get("brd_text")
                    or artifact_types.intersection({"REQUIREMENTS", "REQUIREMENTS_WARN", "KPIS"})
                    or has_stage("req extract")
                    or has_stage("kpi")
                ),
                "detail": "BRD parsed and run created",
            },
            {
                "key": "requirements",
                "label": "Req Extract",
                "complete": bool(artifact_types.intersection({"REQUIREMENTS", "REQUIREMENTS_WARN"})),
                "detail": "Context requirements extracted",
            },
            {
                "key": "kpis",
                "label": "KPI Extract",
                "complete": bool(("KPIS" in artifact_types and not artifact_failed("KPIS")) or checkpoint.get("kpis")),
                "detail": "KPI candidates generated",
            },
            {
                "key": "gate1",
                "label": _gate_label(1, source=source),
                "complete": gate1_decision == "APPROVED",
                "detail": "KPI governance review",
            },
            {
                "key": "discovery",
                "label": "Feed Discovery",
                "complete": bool(
                    checkpoint.get("source_ingestion_status") == "COMPLETED"
                    or checkpoint.get("candidate_feed")
                    or checkpoint.get("candidate_feeds")
                ),
                "detail": f"{source_label} source scanned and candidate feeds identified",
            },
            {
                "key": "gate2",
                "label": _gate_label(2, source=source),
                "complete": gate2_decision == "APPROVED",
                "detail": "Feed governance review",
            },
            {
                "key": "schema",
                "label": "Schema Snapshot",
                "complete": bool("SFTP_SCHEMA_SNAPSHOT" in artifact_types or checkpoint.get("metadata_status") == "COMPLETED"),
                "detail": "Approved feed schema captured",
            },
            {
                "key": "profiling",
                "label": "Column Profiling",
                "complete": bool("SFTP_COLUMN_PROFILING" in artifact_types or checkpoint.get("column_profiling_status") == "COMPLETED"),
                "detail": "Sample-based feed profiling completed",
            },
            {
                "key": "enrichment",
                "label": "Semantic Enrichment",
                "complete": bool("ENRICHED_METADATA" in artifact_types or checkpoint.get("semantic_enrichment_status") == "COMPLETED"),
                "detail": "File-feed semantics classified",
            },
            {
                "key": "gate3",
                "label": _gate_label(3, source=source),
                "complete": bool("GATE3_APPROVED_ENRICHMENT" in artifact_types or checkpoint.get("enrichment_review_status") == "COMPLETED"),
                "detail": "Semantic enrichment review",
            },
            {
                "key": "bronze",
                "label": "Bronze Code Generation",
                "complete": bronze_generation_completed,
                "detail": "Bronze plan and script generated",
            },
            {
                "key": "gate4",
                "label": _gate_label(4, source=source),
                "complete": gate4_decision == "APPROVED",
                "detail": "Bronze review and merge-key resolution",
            },
            {
                "key": "bronze_code_execution",
                "label": "Bronze Code Execution",
                "complete": bool(
                    checkpoint.get("sftp_pull_status") == "COMPLETED"
                    or checkpoint.get("bronze_ingestion_status") in {"COMPLETED", "HANDOFF_ONLY"}
                    or checkpoint.get("bronze_validation_status") == "COMPLETED"
                ),
                "detail": "UI-only execution marker; generated Bronze code runs outside Athena",
            },
            {
                "key": "silver_merge_key_resolution",
                "label": "Silver Merge Key Resolution",
                "complete": bool(
                    checkpoint.get("silver_merge_key_resolution_status") == "COMPLETED"
                    or checkpoint.get("silver_merge_key_resolution_artifact")
                ),
                "detail": "Merge keys resolved from certified semantic metadata",
            },
            {
                "key": "silver_merge_key_review",
                "label": "Silver Merge Key Review",
                "complete": bool(silver_merge_key_review_decision == "APPROVED"),
                "detail": "Reviewed merge keys approved before Silver generation",
            },
            {
                "key": "silver",
                "label": "Silver Code Generation",
                "complete": silver_generation_completed,
                "detail": "Silver transformation script generated",
            },
            {
                "key": "gate5",
                "label": _gate_label(5, source=source),
                "complete": gate5_decision == "APPROVED",
                "detail": "Silver review",
            },
            {
                "key": "silver_code_execution",
                "label": "Silver Code Execution",
                "complete": bool(
                    str(checkpoint.get("target_warehouse") or "").lower() == "snowflake"
                    and checkpoint.get("snowflake_silver_execution_status") == "COMPLETED"
                ) or bool(
                    str(checkpoint.get("target_warehouse") or "").lower() == "databricks"
                    and checkpoint.get("databricks_silver_execution_status") == "COMPLETED"
                ) or bool(
                    str(checkpoint.get("target_warehouse") or "").lower() not in {"snowflake", "databricks"}
                    and (checkpoint.get("dq_validation_status") in {"COMPLETED", "SKIPPED"} or gate5_decision == "APPROVED")
                ),
                "detail": (
                    "Approved Silver scripts are executed in Snowflake before Gold generation."
                    if str(checkpoint.get("target_warehouse") or "").lower() == "snowflake"
                    else "Approved Silver scripts are executed in Databricks before Gold generation."
                    if str(checkpoint.get("target_warehouse") or "").lower() == "databricks"
                    else "UI-only execution marker; generated Silver code runs outside Athena"
                ),
            },
            {
                "key": "gold",
                "label": "Gold Code Generation",
                "complete": gold_generation_completed,
                "detail": "Gold KPI generation completed",
            },
            {
                "key": "gold_code_execution",
                "label": "Gold Code Execution",
                "complete": bool(
                    str(checkpoint.get("target_warehouse") or "").lower() == "snowflake"
                    and checkpoint.get("snowflake_gold_execution_status") == "COMPLETED"
                ) or bool(
                    str(checkpoint.get("target_warehouse") or "").lower() == "databricks"
                    and checkpoint.get("databricks_gold_execution_status") == "COMPLETED"
                ) or bool(
                    str(checkpoint.get("target_warehouse") or "").lower() not in {"snowflake", "databricks"}
                    and gold_generation_completed
                ),
                "detail": (
                    "Generated Gold scripts are executed in Snowflake after Gold generation."
                    if str(checkpoint.get("target_warehouse") or "").lower() == "snowflake"
                    else "Generated Gold scripts are executed in Databricks after Gold generation."
                    if str(checkpoint.get("target_warehouse") or "").lower() == "databricks"
                    else "UI-only execution marker; generated Gold code runs outside Athena"
                ),
            },
        ]
    else:
        gate4_decision = str((checkpoint.get("gate4") or {}).get("decision") or checkpoint.get("bronze_review_decision") or "").upper()
        silver_merge_key_review_decision = str(checkpoint.get("silver_merge_key_review_decision") or "").upper()
        silver_merge_key_review_complete = silver_merge_key_review_decision == "APPROVED"
        steps = [
        {
            "key": "ingestion",
            "label": "Ingestion",
            "complete": bool(checkpoint.get("fingerprint") or checkpoint.get("brd_text") or summary),
            "detail": "BRD parsed and run created",
        },
        {
            "key": "memory",
            "label": "Memory Lookup",
            "complete": bool(
                checkpoint.get("memory_layer1")
                or checkpoint.get("memory_layer2")
                or has_stage("memory")
                or artifact_types.intersection({"REQUIREMENTS", "REQUIREMENTS_WARN", "KPIS"})
            ),
            "detail": "Exact/semantic memory checked",
        },
        {
            "key": "domain_knowledge",
            "label": "Domain Knowledge Check",
            "complete": bool(checkpoint.get("use_domain_kb")),
            "detail": "Reusable domain terminology checked",
        },
        {
            "key": "requirements",
            "label": "Req Extract",
            "complete": bool(artifact_types.intersection({"REQUIREMENTS", "REQUIREMENTS_WARN"})),
            "detail": "Business requirements extracted",
        },
        {
            "key": "kpis",
            "label": "KPI Extract",
            "complete": bool(("KPIS" in artifact_types and not artifact_failed("KPIS")) or pending_gate1 or completed_gate1),
            "detail": "KPI candidates generated",
        },
        {
            "key": "gate1",
            "label": _gate_label(1, source=source),
            "complete": bool("GATE1_CERTIFIED_KPIS" in artifact_types or (completed_gate1 and not pending_gate1)),
            "detail": "Human KPI certification",
        },
        {
            "key": "nomination",
            "label": "Table Extraction",
            "complete": bool("TABLE_NOMINATIONS" in artifact_types or nominated_tables),
            "detail": "Candidate tables selected",
        },
        {
            "key": "gate2",
            "label": _gate_label(2, source=source),
            "complete": bool("GATE2_CERTIFIED_TABLES" in artifact_types or certified_tables),
            "detail": "Human table certification",
        },
        {
            "key": "discovery",
            "label": "Column Extraction",
            "complete": bool("DISCOVERED_METADATA" in artifact_types or status_completed(checkpoint.get("metadata_status"))),
            "detail": "Table metadata discovered",
        },
        {
            "key": "profiling",
            "label": "Column Profiling",
            "complete": bool("COLUMN_PROFILES" in artifact_types or status_completed(checkpoint.get("column_profiling_status"))),
            "detail": "Column profiles generated",
        },
        {
            "key": "enrichment",
            "label": "Semantic Enrichment",
            "complete": bool("ENRICHED_METADATA" in artifact_types or enriched_payload or status_completed(checkpoint.get("semantic_enrichment_status"))),
            "detail": "Semantic metadata enriched",
        },
        {
            "key": "gate3",
            "label": _gate_label(3, source=source),
            "complete": bool("GATE3_APPROVED_ENRICHMENT" in artifact_types or gate3_payload),
            "detail": "Human enrichment approval",
        },
        {
            "key": "bronze",
            "label": "Bronze Code Generation",
            "complete": bool(bronze_generation_completed),
            "detail": "Bronze scripts generated",
        },
        {
            "key": "gate4",
            "label": _gate_label(4, source=source),
            "complete": bool(gate4_decision == "APPROVED"),
            "detail": "Bronze review and merge-key resolution",
        },
            {
                "key": "bronze_code_execution",
                "label": "Bronze Code Execution",
                "complete": bool(
                    str(checkpoint.get("target_warehouse") or "").lower() == "snowflake"
                    and checkpoint.get("snowflake_bronze_execution_status") == "COMPLETED"
                ) or bool(
                    str(checkpoint.get("target_warehouse") or "").lower() == "databricks"
                    and checkpoint.get("databricks_bronze_execution_status") == "COMPLETED"
                ),
                "detail": (
                    "Approved Bronze scripts are executed in Snowflake before Silver generation."
                    if str(checkpoint.get("target_warehouse") or "").lower() == "snowflake"
                    else "Approved Bronze scripts are executed in Databricks before Silver generation."
                    if str(checkpoint.get("target_warehouse") or "").lower() == "databricks"
                    else "UI-only execution marker; generated Bronze code runs outside Athena"
                ),
            },
        {
            "key": "silver_merge_key_resolution",
            "label": "Silver Merge Key Resolution",
            "complete": bool(
                checkpoint.get("silver_merge_key_resolution_status") == "COMPLETED"
                or checkpoint.get("silver_merge_key_resolution_artifact")
            ),
            "detail": "Merge keys resolved from certified semantic metadata",
        },
        {
            "key": "silver_merge_key_review",
            "label": "Silver Merge Key Review",
            "complete": bool(silver_merge_key_review_complete),
            "detail": "Reviewed merge keys approved before Silver generation",
        },
        {
            "key": "silver",
            "label": "Silver Code Generation",
            "complete": bool(silver_generation_completed),
            "detail": "Silver transformation scripts generated",
        },
        {
            "key": "gate5",
            "label": _gate_label(5, source=source),
            "complete": bool((checkpoint.get("gate5") or {}).get("decision") == "APPROVED" or checkpoint.get("silver_review_decision") == "APPROVED"),
            "detail": "Silver review",
        },
            {
                "key": "silver_code_execution",
                "label": "Silver Code Execution",
                "complete": bool(
                    str(checkpoint.get("target_warehouse") or "").lower() == "snowflake"
                    and checkpoint.get("snowflake_silver_execution_status") == "COMPLETED"
                ) or bool(
                    str(checkpoint.get("target_warehouse") or "").lower() == "databricks"
                    and checkpoint.get("databricks_silver_execution_status") == "COMPLETED"
                ),
                "detail": (
                    "Approved Silver scripts are executed in Snowflake before Gold generation."
                    if str(checkpoint.get("target_warehouse") or "").lower() == "snowflake"
                    else "Approved Silver scripts are executed in Databricks before Gold generation."
                    if str(checkpoint.get("target_warehouse") or "").lower() == "databricks"
                    else "UI-only execution marker; generated Silver code runs outside Athena"
                ),
            },
        {
            "key": "gold",
            "label": "Gold Code Generation",
            "complete": bool(gold_generation_completed),
            "detail": "Gold KPI scripts generated",
        },
            {
                "key": "gold_code_execution",
                "label": "Gold Code Execution",
                "complete": bool(
                    str(checkpoint.get("target_warehouse") or "").lower() == "snowflake"
                    and checkpoint.get("snowflake_gold_execution_status") == "COMPLETED"
                ) or bool(
                    str(checkpoint.get("target_warehouse") or "").lower() == "databricks"
                    and checkpoint.get("databricks_gold_execution_status") == "COMPLETED"
                ),
                "detail": (
                    "Generated Gold scripts are executed in Snowflake after Gold generation."
                    if str(checkpoint.get("target_warehouse") or "").lower() == "snowflake"
                    else "Generated Gold scripts are executed in Databricks after Gold generation."
                    if str(checkpoint.get("target_warehouse") or "").lower() == "databricks"
                    else "UI-only execution marker; generated Gold code runs outside Athena"
                ),
            },
        ]

    checkpoint_status = str(checkpoint.get("status") or "").upper()
    pipeline_is_active = bool(
        checkpoint.get("background_stage")
        or checkpoint_status in {"RUNNING", "PROCESSING", "SUBMITTED", "IN_PROGRESS"}
    )

    active_stage_key = str(checkpoint.get("background_stage") or "")
    external_execution = checkpoint.get("external_execution") if isinstance(checkpoint.get("external_execution"), dict) else {}
    external_message = str(external_execution.get("message") or "").strip()

    execution_completion = {
        step["key"]: bool(step.get("complete"))
        for step in steps
        if step.get("key") in {"bronze_code_execution", "silver_code_execution", "gold_code_execution"}
    }

    active_index = next((index for index, step in enumerate(steps) if step.get("key") == active_stage_key), None) if active_stage_key else None

    if active_index is not None:
        for index, step in enumerate(steps):
            if index < active_index:
                step["complete"] = True
                step["state"] = "COMPLETED"
            elif step.get("key") == active_stage_key:
                step["complete"] = False
                step["state"] = "RUNNING"
                if external_message:
                    step["detail"] = external_message
            else:
                # ponytail: this UI is linear; persisted downstream artifacts may be
                # stale during retry, so the active checkpoint owns the visible frontier.
                step["complete"] = False
                step["state"] = "PENDING"

    else:
        last_complete_index = -1
        for index, step in enumerate(steps):
            if step["complete"]:
                last_complete_index = index

        # The pipeline is linear for this UI. If a downstream node completed, every
        # upstream node must have already run even if its individual artifact was not
        # persisted with the exact name this page checks.
        for index, step in enumerate(steps):
            if index <= last_complete_index:
                step["complete"] = True

        # Assign states: COMPLETE, RUNNING only while the backend is actively
        # processing, otherwise keep incomplete steps pending until a gate/runtime
        # explicitly marks one active.
        first_incomplete_seen = False
        for step in steps:
            if step["complete"]:
                step["state"] = "COMPLETED"
            elif pipeline_is_active and not first_incomplete_seen:
                step["state"] = "RUNNING"
                first_incomplete_seen = True
            else:
                step["state"] = "PENDING"

    # ponytail: downstream progress is not proof that an execution ran; only
    # executor-owned status (or a real external handoff result) can complete it.
    for step in steps:
        key = step.get("key")
        if key in execution_completion and not execution_completion[key] and key != active_stage_key:
            step["complete"] = False
            step["state"] = "PENDING"

    # If pipeline failed, mark the failed step
    if checkpoint.get("status") == "FAILED":
        failed_key = (
            checkpoint.get("failed_background_stage")
            or checkpoint.get("last_failed_stage_key")
            or checkpoint.get("failed_stage")
        )
        failed_step = next((step for step in steps if step.get("key") == failed_key), None)
        if failed_step:
            failed_step["complete"] = False
            failed_step["state"] = "FAILED"
        else:
            for step in steps:
                if step["state"] == "RUNNING":
                    step["state"] = "FAILED"
                    break
    # If all steps are complete, ensure at least one shows as completed
    elif all(step["complete"] for step in steps):
        for step in reversed(steps):
            if step["state"] != "COMPLETED":
                step["state"] = "COMPLETED"

    return steps


def generation_completed(summary: List[Dict[str, Any]], checkpoint: Dict[str, Any], layer: str) -> bool:
    layer = str(layer or "").lower()
    artifact_types = {
        str(row.get("artifact_type") or "").upper()
        for row in summary
        if isinstance(row, dict)
    }
    status = str(checkpoint.get(f"{layer}_generation_status") or "").upper()
    return bool(
        artifact_types.intersection(GENERATION_ARTIFACT_TYPES.get(layer, set()))
        or status == "COMPLETED"
        or status.startswith("COMPLETED_")
        or checkpoint.get(f"{layer}_generation_results")
    )


def apply_waiting_stage_state(steps: List[Dict[str, Any]], gate_key: Optional[str]) -> List[Dict[str, Any]]:
    if not gate_key:
        return steps
    waiting_index = None
    for index, step in enumerate(steps):
        if step.get("key") == gate_key:
            step["state"] = "HITL_WAIT"
            step["complete"] = False
            waiting_index = index
            break
    if waiting_index is None:
        return steps
    for index, step in enumerate(steps):
        if index > waiting_index:
            step["state"] = "PENDING"
            step["complete"] = False
    return steps


def get_run_context(run_id: str) -> Dict[str, Any]:
    checkpoint = load_checkpoint_state(run_id) or {}
    source_value = str(checkpoint.get("source") or "database").lower()
    summary = fetch_run_summary(run_id)
    pending_gate1 = get_pending_items(run_id, 1)
    completed_gate1 = get_completed_items(run_id, 1)
    kpi_artifact_failed = any(
        str(row.get("artifact_type") or "").upper() == "KPIS"
        and str(row.get("faithfulness_status") or "").upper() == "FAILED"
        for row in summary
        if isinstance(row, dict)
    )
    checkpoint_kpis = any(
        isinstance(checkpoint.get(key), list) and bool(checkpoint.get(key))
        for key in ("kpis", "prior_kpis", "extracted_kpis", "certified_kpis")
    )
    kpi_review_unavailable = bool(kpi_artifact_failed and not pending_gate1 and not completed_gate1 and not checkpoint_kpis)
    if kpi_review_unavailable:
        checkpoint = {
            **checkpoint,
            "status": "FAILED",
            "failed_background_stage": "kpis",
            "error": checkpoint.get("error") or "KPI extraction failed before review items were created.",
        }
    nominations_payload = fetch_json_artifact(run_id, "TABLE_NOMINATIONS")
    nominated_tables = (
        nominations_payload.get("nominations", [])
        or checkpoint.get("nominated_tables")
        or []
    )
    gate2_payload = fetch_json_artifact(run_id, "GATE2_CERTIFIED_TABLES")
    certified_tables = (
        gate2_payload.get("certified_tables", [])
        or checkpoint.get("certified_tables")
        or []
    )
    enriched_payload = fetch_json_artifact(run_id, "ENRICHED_METADATA") or _checkpoint_enriched_payload(checkpoint)
    gate3_payload = fetch_json_artifact(run_id, "GATE3_APPROVED_ENRICHMENT")
    if not gate3_payload and checkpoint.get("enrichment_review_status") == "COMPLETED":
        gate3_payload = checkpoint.get("enrichment_review_artifact") or {"approved_from_checkpoint": True}
    downstream_progress_exists = bool(
        nominated_tables
        or certified_tables
        or enriched_payload
        or gate3_payload
        or checkpoint.get("human_table_decision") == "COMPLETED"
        or checkpoint.get("enrichment_review_status") in {"COMPLETED", "PENDING"}
    )
    if downstream_progress_exists and completed_gate1:
        pending_gate1 = []

    # For SFTP runs, the feed review replaces table nomination.
    # Ensure we don't render DB-table review panels for SFTP runs.
    if source_value in {"sftp", "adls_gen2"}:
        nominated_tables = []
        certified_tables = []
        pending_gate1 = []  # SFTP gate1 is tracked via checkpoint.gate1, not SQL queue.
        completed_gate1 = []
    bronze_generation_completed = generation_completed(summary, checkpoint, "bronze")
    silver_generation_completed = generation_completed(summary, checkpoint, "silver")
    gold_generation_completed = generation_completed(summary, checkpoint, "gold")
    bronze = load_bronze_scripts(run_id, checkpoint) if gate3_payload or bronze_generation_completed else {"generated_at": None, "scripts": []}
    silver = load_silver_scripts(run_id, checkpoint) if silver_generation_completed else {"generated_at": None, "scripts": []}
    gold = load_gold_scripts(run_id, checkpoint) if gold_generation_completed else {"generated_at": None, "scripts": []}

    enriched_columns = enriched_payload.get("columns", []) if isinstance(enriched_payload, dict) else []
    enriched_joins = enriched_payload.get("joins", []) if isinstance(enriched_payload, dict) else []
    semantic_counts: Dict[str, int] = {}
    pii_columns: List[Dict[str, Any]] = []
    join_key_columns: List[Dict[str, Any]] = []
    measure_columns: List[Dict[str, Any]] = []
    for column in enriched_columns:
        semantic_type = str(column.get("semantic_type") or "UNKNOWN")
        semantic_counts[semantic_type] = semantic_counts.get(semantic_type, 0) + 1
        if column.get("is_pii_candidate") or column.get("is_pii"):
            pii_columns.append(column)
        if semantic_type in {"ID", "SURROGATE_KEY"} or column.get("is_join_key"):
            join_key_columns.append(column)
        if column.get("is_measure"):
            measure_columns.append(column)

    known_stage_completion = {
        "gate1": bool(completed_gate1 and not pending_gate1),
        "gate2": bool(certified_tables),
        "enrichment": bool(enriched_payload or checkpoint.get("semantic_enrichment_status") == "COMPLETED"),
        "gate3": bool(gate3_payload),
        "bronze": bool(bronze_generation_completed),
        "silver": bool(silver_generation_completed),
        "gold": bool(gold_generation_completed),
    }

    def _known_stage_completed(stage_key: Optional[str]) -> bool:
        return bool(known_stage_completion.get(str(stage_key or "")))

    def _latest_known_completed_stage_at_or_after(stage_key: Optional[str]) -> Optional[str]:
        start_index = _database_stage_index(str(stage_key or ""))
        if start_index < 0:
            return None
        latest_stage_key = None
        for candidate_key, _ in DATABASE_STAGE_SEQUENCE[start_index:]:
            if _known_stage_completed(candidate_key):
                latest_stage_key = candidate_key
        return latest_stage_key

    def _stage_confirmation_after(completed_stage_key: str) -> Optional[Dict[str, Any]]:
        next_stage_key = _database_next_stage_key(completed_stage_key)
        if not next_stage_key or _known_stage_completed(next_stage_key) or _is_database_review_gate(next_stage_key):
            return None
        completed_stage_label = DATABASE_STAGE_LABELS.get(completed_stage_key, completed_stage_key)
        next_stage_label = DATABASE_STAGE_LABELS.get(next_stage_key, next_stage_key)
        return {
            "enabled": bool(checkpoint.get("stage_confirmation_enabled")),
            "awaiting_confirmation": True,
            "last_completed_stage_key": completed_stage_key,
            "last_completed_stage_label": completed_stage_label,
            "next_stage_key": next_stage_key,
            "next_stage_label": next_stage_label,
            "resume_message": (
                f"{completed_stage_label} finished successfully. "
                f"Confirm before continuing to {next_stage_label}."
            ),
        }

    next_gate = None
    next_review_key = checkpoint.get("next_review_key")
    resume_message = None
    if source_value in {"sftp", "adls_gen2"}:
        gate1_decision = (checkpoint.get("gate1") or {}).get("decision")
        gate2_decision = (checkpoint.get("gate2") or {}).get("decision")
        if kpi_review_unavailable:
            resume_message = checkpoint["error"]
        elif gate1_decision in {None, ""}:
            next_gate = 1
            resume_message = "KPI Review is pending. Review KPI items before continuing."
        elif gate1_decision == "APPROVED" and (gate2_decision in {None, ""}):
            next_gate = 2
            resume_message = "Feed Review is pending. Review the discovered feed before continuing."
        elif gate2_decision == "APPROVED":
            resume_message = "Feed Review is complete."
        elif gate1_decision == "REJECTED":
            resume_message = "KPI Review was rejected."
        elif gate2_decision == "REJECTED":
            resume_message = "Feed Review was rejected."
    elif kpi_review_unavailable:
        resume_message = checkpoint["error"]
    elif pending_gate1:
        next_gate = 1
        resume_message = "KPI Review is pending. Review the KPI items below."
    elif nominated_tables and not certified_tables:
        next_gate = 2
        resume_message = "Table Review is pending. Review and certify nominated tables below."
    elif enriched_payload and not gate3_payload:
        next_gate = 3
        resume_message = "Semantic Review is pending. Review enriched column metadata below."
    elif gate3_payload:
        resume_message = "Semantic Review is complete."
    elif certified_tables and not enriched_payload:
        resume_message = "Table Review is certified. Column Extraction has not completed yet."
    elif completed_gate1 and not nominated_tables:
        resume_message = "KPI Review is certified. Table Extraction has not completed yet."
    elif not summary and not checkpoint:
        resume_message = "No stored state was found for this run ID."

    if next_review_key:
        next_gate = None
        resume_message = checkpoint.get("resume_message") or "Silver Merge Key Review is pending. Review merge keys before Silver generation."

    # Recover stale checkpoints that say HITL_WAIT but do not carry next_gate.
    # The durable ai_store artifacts are the source of truth for UI review routing.
    if source_value not in {"sftp", "adls_gen2"} and not next_gate and not next_review_key:
        gate4_decision = str((checkpoint.get("gate4") or {}).get("decision") or checkpoint.get("bronze_review_decision") or "").upper()
        gate5_decision = str((checkpoint.get("gate5") or {}).get("decision") or checkpoint.get("silver_review_decision") or "").upper()
        if completed_gate1 and not pending_gate1 and nominated_tables and not certified_tables:
            next_gate = 2
            resume_message = "Table Review is pending. Review and certify nominated tables below."
        elif certified_tables and enriched_payload and not gate3_payload:
            next_gate = 3
            resume_message = "Semantic Review is pending. Review enriched column metadata below."
        elif gate3_payload and bronze_generation_completed and gate4_decision not in {"APPROVED", "REJECTED"}:
            next_gate = 4
            resume_message = "Bronze Review is pending. Review generated Bronze scripts before Silver generation."
        elif silver_generation_completed and gate5_decision not in {"APPROVED", "REJECTED"}:
            next_gate = 5
            resume_message = "Silver Review is pending. Review generated Silver scripts before Gold generation."

    stage_confirmation = None
    active_background_stage = bool(checkpoint.get("background_stage"))
    paused_for_stage_confirmation = (
        checkpoint.get("status") == "PAUSED_FOR_STAGE_CONFIRMATION"
        and not active_background_stage
    )
    paused_before_review_gate = False
    stale_stage_confirmation_completed = False
    if paused_for_stage_confirmation:
        target_stage_key = str(checkpoint.get("next_stage_key") or "")
        if target_stage_key and _known_stage_completed(target_stage_key):
            stale_stage_confirmation_completed = True
            completed_stage_key = _latest_known_completed_stage_at_or_after(target_stage_key)
            next_stage_key = _database_next_stage_key(completed_stage_key) if completed_stage_key else None
            if next_stage_key and _is_database_review_gate(next_stage_key):
                paused_before_review_gate = True
                gate_map = {"gate1": 1, "gate2": 2, "gate3": 3}
                next_gate = gate_map.get(str(next_stage_key))
                resume_message = (
                    f"{DATABASE_STAGE_LABELS.get(next_stage_key, next_stage_key)} "
                    "is pending. Review the generated artifacts before continuing."
                )
            elif completed_stage_key and next_stage_key:
                stage_confirmation = _stage_confirmation_after(completed_stage_key)
                if stage_confirmation:
                    resume_message = stage_confirmation["resume_message"]
        elif target_stage_key and _is_database_review_gate(target_stage_key):
            paused_before_review_gate = True

    if paused_before_review_gate:
        gate_map = {"gate1": 1, "gate2": 2, "gate3": 3}
        review_stage_key = str(checkpoint.get("next_stage_key") or "")
        if not next_gate:
            next_gate = gate_map.get(review_stage_key)
        if not resume_message:
            resume_message = (
                f"{checkpoint.get('next_stage_label') or DATABASE_STAGE_LABELS.get(review_stage_key, 'Review')} "
                "is pending. Review the generated artifacts before continuing."
            )
    elif paused_for_stage_confirmation and not stage_confirmation and not stale_stage_confirmation_completed:
        stage_confirmation = {
            "enabled": bool(checkpoint.get("stage_confirmation_enabled")),
            "awaiting_confirmation": True,
            "last_completed_stage_key": checkpoint.get("last_completed_stage_key"),
            "last_completed_stage_label": checkpoint.get("last_completed_stage_label"),
            "next_stage_key": checkpoint.get("next_stage_key"),
            "next_stage_label": checkpoint.get("next_stage_label"),
        }
        if checkpoint.get("resume_message"):
            resume_message = checkpoint.get("resume_message")

    gold_execution_progress_exists = bool(
        checkpoint.get("background_stage") == "gold_code_execution"
        or str(checkpoint.get("snowflake_gold_execution_status") or "").upper() in {"RUNNING", "COMPLETED"}
        or str(checkpoint.get("databricks_gold_execution_status") or "").upper() in {"RUNNING", "COMPLETED"}
        or str(checkpoint.get("status") or "").upper() == "PIPELINE_COMPLETED"
    )
    if gold_execution_progress_exists:
        next_gate = None
        next_review_key = None

    status = checkpoint.get("status") or checkpoint.get("table_nomination_status") or checkpoint.get("enrichment_review_status") or "UNKNOWN"
    if paused_before_review_gate:
        status = "HITL_WAIT"
    elif stale_stage_confirmation_completed and not stage_confirmation:
        status = "PIPELINE_COMPLETED"
    can_promote_to_completed = str(status or "").upper() not in {
        "HITL_WAIT",
        "PAUSED_FOR_HITL",
        "PAUSED_FOR_STAGE_CONFIRMATION",
        "PROCESSING",
        "RUNNING",
        "SUBMITTED",
        "FAILED",
        "ABORTED",
    }
    if can_promote_to_completed and (
        checkpoint.get("bronze_generation_status") == "COMPLETED"
        or checkpoint.get("databricks_bronze_execution_status") == "COMPLETED"
        or checkpoint.get("snowflake_bronze_execution_status") == "COMPLETED"
    ):
        status = "PIPELINE_COMPLETED"
    if can_promote_to_completed and gate3_payload and bronze_generation_completed:
        status = "PIPELINE_COMPLETED"
    if can_promote_to_completed and (
        silver_generation_completed
        or checkpoint.get("databricks_silver_execution_status") == "COMPLETED"
        or checkpoint.get("snowflake_silver_execution_status") == "COMPLETED"
    ):
        status = "PIPELINE_COMPLETED"
    if can_promote_to_completed and (
        gold_generation_completed
        or checkpoint.get("databricks_gold_execution_status") == "COMPLETED"
        or checkpoint.get("snowflake_gold_execution_status") == "COMPLETED"
    ):
        status = "PIPELINE_COMPLETED"
    if (
        not checkpoint.get("background_stage")
        and str(status or "").upper() in {"RUNNING", "PROCESSING", "SUBMITTED", "IN_PROGRESS"}
        and (
            str(checkpoint.get("databricks_gold_execution_status") or "").upper() == "COMPLETED"
            or str(checkpoint.get("snowflake_gold_execution_status") or "").upper() == "COMPLETED"
        )
    ):
        status = "PIPELINE_COMPLETED"
    pipeline_steps = build_pipeline_steps(
        source=str(checkpoint.get("source") or "database"),
        checkpoint=checkpoint,
        summary=summary,
        pending_gate1=pending_gate1,
        completed_gate1=completed_gate1,
        nominated_tables=nominated_tables,
        certified_tables=certified_tables,
        enriched_payload=enriched_payload,
        gate3_payload=gate3_payload,
        bronze_generation_completed=bronze_generation_completed,
        silver_generation_completed=silver_generation_completed,
        gold_generation_completed=gold_generation_completed,
    )
    waiting_gate_key = (
        "gate1" if next_gate == 1
        else "gate2" if next_gate == 2
        else "gate3" if next_gate == 3
        else "gate4" if next_gate == 4
        else "gate5" if next_gate == 5
        else None
    )
    waiting_stage_key = str(
        "gold_code_execution" if next_review_key == "gold_review" else next_review_key or waiting_gate_key or ""
    ) or None
    pipeline_steps = apply_waiting_stage_state(pipeline_steps, waiting_stage_key)
    if checkpoint.get("status") == "PAUSED_FOR_STAGE_CONFIRMATION" and checkpoint.get("next_stage_key"):
        for step in pipeline_steps:
            if step.get("key") == checkpoint.get("next_stage_key") and step.get("state") == "PENDING":
                step["detail"] = f"Waiting for confirmation before {checkpoint.get('next_stage_label') or step.get('label')}."
                break
    current_pipeline_step = next((step for step in pipeline_steps if str(step.get("state")).upper() == "RUNNING"), None)
    if not current_pipeline_step and waiting_stage_key:
        current_pipeline_step = next((step for step in pipeline_steps if step["key"] == waiting_stage_key), None)
    if not current_pipeline_step and status == "PIPELINE_COMPLETED":
        current_pipeline_step = {
            "key": "completed",
            "label": "Pipeline Completed",
            "state": "complete",
            "detail": "All backend stages completed",
        }

    return {
        "run_id": run_id,
        "checkpoint": checkpoint,
        "summary": summary,
        "pending_gate1": pending_gate1,
        "completed_gate1": completed_gate1,
        "nominated_tables": nominated_tables,
        "certified_tables": certified_tables,
        "enriched_metadata": enriched_payload,
        "enriched_columns": enriched_columns,
        "enriched_joins": enriched_joins,
        "semantic_counts": semantic_counts,
        "pii_columns": pii_columns,
        "join_key_columns": join_key_columns,
        "measure_columns": measure_columns,
        "gate3_approved": bool(gate3_payload),
        "bronze_generation_completed": bronze_generation_completed,
        "silver_generation_completed": silver_generation_completed,
        "gold_generation_completed": gold_generation_completed,
        "bronze": bronze,
        "silver": silver,
        "gold": gold,
        "next_gate": next_gate,
        "next_review_key": next_review_key,
        "resume_message": resume_message,
        "stage_confirmation": stage_confirmation,
        "status": status,
        "pipeline_steps": pipeline_steps,
        "current_pipeline_step": current_pipeline_step,
        "external_execution": checkpoint.get("external_execution"),
    }


def start_pipeline(
    *,
    brd_text: Optional[str] = None,
    input_path: Optional[str] = None,
    brd_filename: Optional[str] = None,
    source: Optional[str] = None,
    source_databases: Optional[List[str]] = None,
    sftp_entity: Optional[str] = None,
    run_id: Optional[str] = None,
    use_domain_kb: bool = False,
    stage_confirmation_enabled: bool = False,
    compliance_enabled: bool = False,
    compliance_domain: str = "Insurance",
    compliance_countries: Optional[List[str]] = None,
    target_warehouse: str = "databricks",
) -> Dict[str, Any]:
    run_id = run_id or str(uuid.uuid4())
    default_source_db = config["azure_sql"].get("source_database") or "insurance"
    source_value = str(source or "database").lower()
    file_sources = {"sftp", "adls_gen2"}
    initial_state: Dict[str, Any] = {
        "brd_text": brd_text or input_path or "",
        "brd_filename": brd_filename,
        "run_id": run_id,
        "metadata": {},
        "status": "PENDING",
        "source": source_value,
        "sftp_entity": str(sftp_entity or "transactions").lower(),
        "source_databases": source_databases or [default_source_db],
        "use_domain_kb": bool(use_domain_kb),
        "stage_confirmation_enabled": bool(stage_confirmation_enabled),
        "compliance_enabled": bool(compliance_enabled),
        "compliance_domain": compliance_domain or "Insurance",
        "compliance_countries": compliance_countries or ["US"],
        "target_warehouse": str(target_warehouse or "databricks").lower(),
    }

    if source_value in file_sources:
        from services.sftp_runtime import start_sftp_pipeline

        result = start_sftp_pipeline(
            run_id=run_id,
            brd_text=initial_state["brd_text"],
            sftp_entity=initial_state["sftp_entity"],
            source=source_value,
        ).get("result")
    else:
        result = continue_database_pipeline(
            run_id,
            start_stage_key="ingestion",
            state=initial_state,
        )

    return {
        "run_id": run_id,
        "result": result,
    }


def submit_gate1_review(run_id: str, decisions: List[Dict[str, str]]) -> Dict[str, Any]:
    from nodes.hitl import hitl_review_node

    pending = get_pending_items(run_id, 1)
    existing_nomination = fetch_json_artifact(run_id, "TABLE_NOMINATIONS")
    if not pending and existing_nomination.get("nominations"):
        checkpoint = load_checkpoint_state(run_id) or {"run_id": run_id}
        return {
            **checkpoint,
            "run_id": run_id,
            "nominated_tables": existing_nomination.get("nominations", []),
            "table_nomination_status": checkpoint.get("table_nomination_status") or "PENDING",
        }

    decision_map = {item["item_id"]: item for item in decisions}
    batch_updates: List[Dict[str, Optional[str]]] = []

    for item in pending:
        decision = decision_map.get(item["item_id"])
        action = (decision or {}).get("action", "APPROVED").upper()
        kpi = item["kpi"]

        if action == "REJECTED":
            batch_updates.append(
                {
                    "item_id": item["item_id"],
                    "status": "REJECTED",
                    "edited_content": None,
                    "rejection_reason": (decision or {}).get("reason", ""),
                }
            )
            continue

        if action == "EDITED":
            edited = kpi.copy()
            edited["kpi_name"] = (decision or {}).get("name", kpi.get("kpi_name", ""))
            edited["kpi_description"] = (decision or {}).get("description", kpi.get("kpi_description", ""))
            batch_updates.append(
                {
                    "item_id": item["item_id"],
                    "status": "APPROVED",
                    "edited_content": json.dumps(edited),
                    "rejection_reason": None,
                }
            )
            continue

        batch_updates.append(
            {
                "item_id": item["item_id"],
                "status": "APPROVED",
                "edited_content": None,
                "rejection_reason": None,
            }
        )

    update_hitl_items_batch(batch_updates)

    certified = get_completed_items(run_id, 1)
    resumed_input = load_checkpoint_state(run_id) or {"run_id": run_id}
    resumed_input["human_decision"] = "COMPLETED"
    resumed_input["certified_kpis"] = [item["kpi"] for item in certified]
    with timed_stage("gate1_hitl_certification", run_id=run_id, node="api"):
        resumed = hitl_review_node(resumed_input)
    if resumed.get("status") == "FAILED":
        raise ValueError(resumed.get("error", "KPI Review certification failed."))

    save_checkpoint_state(run_id, resumed)

    return continue_database_pipeline(run_id, start_stage_key="nomination", state=resumed)


def _gate2_execution_scope(tables: List[Dict[str, Any]], approved_keys: List[str]) -> List[Dict[str, Any]]:
    approved_key_set = set(approved_keys)
    approved = [item for item in tables if _table_key(item) in approved_key_set]

    # Dimension/lookup candidates are supporting inputs to the approved facts.
    # Keep them in the execution scope even when the reviewer selected only the
    # fact tables; otherwise they disappear before Bronze/Silver generation.
    dimension_prefixes = ("dim_", "ref_", "lkp_", "lookup_", "code_", "type_")
    dimension_reasons = {"FK Resolution (related to nominated table)"}
    fact_keys = {_table_key(item) for item in approved}
    if fact_keys:
        approved_keys_seen = set(fact_keys)
        for item in tables:
            table_name = str(item.get("table_name") or item.get("table") or "").strip().lower()
            reason = str(item.get("nomination_reason") or "").strip()
            if (table_name.startswith(dimension_prefixes) or reason in dimension_reasons) and _table_key(item) not in approved_keys_seen:
                approved.append(item)
                approved_keys_seen.add(_table_key(item))

    if not approved:
        raise ValueError("At least one table must be approved for Table Review.")
    return approved


def submit_gate2_review(run_id: str, approved_keys: List[str]) -> Dict[str, Any]:
    from nodes.hitl import hitl_table_review_node

    tables = fetch_json_artifact(run_id, "TABLE_NOMINATIONS").get("nominations", []) or []
    approved = _gate2_execution_scope(tables, approved_keys)

    resumed_input = load_checkpoint_state(run_id) or {"run_id": run_id}
    resumed_input["human_table_decision"] = "COMPLETED"
    resumed_input["certified_tables"] = approved
    with timed_stage("gate2_hitl_certification", run_id=run_id, node="api"):
        resumed = hitl_table_review_node(resumed_input)
    if resumed.get("status") == "FAILED":
        raise ValueError(resumed.get("error", "Table Review certification failed."))
    save_checkpoint_state(run_id, resumed)

    return continue_database_pipeline(run_id, start_stage_key="discovery", state=resumed)


def submit_gate3_review(run_id: str, approve: bool, enriched_metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    from nodes.hitl import build_hitl_enrichment_review_node
    from services.compliance_client import attach_review_result

    checkpoint_state = load_checkpoint_state(run_id) or {}
    metadata = enriched_metadata or fetch_json_artifact(run_id, "ENRICHED_METADATA") or _checkpoint_enriched_payload(checkpoint_state)
    if not metadata:
        raise ValueError("No enriched metadata found for this run.")

    enrichment_node = build_hitl_enrichment_review_node()
    state: Dict[str, Any] = {
        "run_id": run_id,
        "enriched_metadata": metadata,
        "semantic_tags_reviewed": approve,
        "pii_classifications_reviewed": approve,
        "join_key_annotations_reviewed": approve,
        "enrichment_review_decision": "APPROVED" if approve else "REJECTED",
    }
    with timed_stage("gate3_hitl_certification", run_id=run_id, node="api"):
        result = enrichment_node(state)
    if result.get("enrichment_review_status") != "COMPLETED":
        return result

    certified_tables = (
        fetch_json_artifact(run_id, "GATE2_CERTIFIED_TABLES").get("certified_tables", [])
        or metadata.get("certified_tables")
        or checkpoint_state.get("certified_tables")
        or []
    )
    if not certified_tables:
        raise ValueError("Bronze generation skipped: no Table Review certified tables found.")

    bronze_state: Dict[str, Any] = {
        **checkpoint_state,
        **result,
        "run_id": run_id,
        "enriched_metadata": metadata,
        "fingerprint": metadata.get("fingerprint") or checkpoint_state.get("fingerprint") or run_id,
        "certified_tables": certified_tables,
        "discovered_metadata": fetch_json_artifact(run_id, "DISCOVERED_METADATA") or checkpoint_state.get("discovered_metadata") or {},
        "bronze_catalog": os.getenv("BRONZE_CATALOG", "main"),
        "bronze_schema": os.getenv("BRONZE_SCHEMA", "bronze"),
        "silver_catalog": os.getenv("SILVER_CATALOG", os.getenv("BRONZE_CATALOG", "main")),
        "silver_schema": os.getenv("SILVER_SCHEMA", "silver"),
        "gold_schema": os.getenv("GOLD_SCHEMA", "gold"),
    }
    bronze_state.update(attach_review_result(bronze_state))
    if str(bronze_state.get("target_warehouse") or "").lower() == "snowflake":
        bronze_state["gold_catalog"] = os.getenv("SNOWFLAKE_GOLD_CATALOG") or os.getenv("SNOWFLAKE_SILVER_CATALOG") or "ATHENA_DB"
        bronze_state["gold_schema"] = os.getenv("SNOWFLAKE_GOLD_SCHEMA", "GOLD")
    return continue_database_pipeline(run_id, start_stage_key="bronze", state=bronze_state)


def _apply_gate4_merge_keys_to_metadata(metadata: Dict[str, Any], review_artifact: Dict[str, Any]) -> Dict[str, Any]:
    feeds = review_artifact.get("feeds") or []
    if not feeds or not isinstance(metadata, dict):
        return metadata

    keys_by_table = {
        str(feed.get("table") or feed.get("entity") or feed.get("table_name") or feed.get("target_table") or "").split(".")[-1].strip().lower(): {
            str(key).strip().lower()
            for key in (feed.get("primary_keys") or feed.get("merge_keys") or [])
            if str(key).strip()
        }
        for feed in feeds
    }
    columns = []
    for column in metadata.get("columns") or []:
        if not isinstance(column, dict):
            columns.append(column)
            continue
        table_name = str(column.get("table_name") or "").strip().lower()
        column_name = str(column.get("column_name") or "").strip().lower()
        reviewed_keys = keys_by_table.get(table_name) or set()
        if reviewed_keys and column_name in reviewed_keys:
            columns.append({**column, "is_join_key": True, "semantic_type": column.get("semantic_type") or "ID"})
        elif reviewed_keys:
            columns.append({**column, "is_join_key": False})
        else:
            columns.append(column)
    return {**metadata, "columns": columns, "gate4_reviewed_merge_keys": review_artifact}


def _silver_merge_key_review_artifact(checkpoint: Dict[str, Any]) -> Dict[str, Any]:
    artifact = checkpoint.get("silver_merge_key_review_artifact")
    feeds = artifact.get("feeds") if isinstance(artifact, dict) else []
    has_selected_keys = any(
        isinstance(feed, dict) and (feed.get("merge_keys") or feed.get("primary_keys"))
        for feed in feeds
    )
    has_enriched_shape = bool(feeds) and all(
        isinstance(feed, dict) and "merge_key_source" in feed and "merge_key_candidates" in feed
        for feed in feeds
    )
    if has_selected_keys or has_enriched_shape:
        return artifact

    from nodes.silver_merge_key_resolution import silver_merge_key_resolution_node

    resolved = silver_merge_key_resolution_node(checkpoint)
    return resolved.get("silver_merge_key_resolution_artifact") or {"run_id": checkpoint.get("run_id"), "feeds": []}


def _pause_for_silver_merge_key_review(run_id: str, state: Dict[str, Any]) -> Dict[str, Any]:
    from nodes.silver_merge_key_resolution import silver_merge_key_resolution_node

    started_at = time.monotonic()
    logger.info(
        "START Silver Merge Key Resolution",
        extra={"run_id": run_id, "node": "silver_merge_key_resolution", "stage": "silver_merge_key_resolution", "event_type": "stage_start"},
    )
    try:
        resolved_state = silver_merge_key_resolution_node({**state, "run_id": run_id})
    except Exception:
        logger.exception(
            "FAILED Silver Merge Key Resolution",
            extra={"run_id": run_id, "node": "silver_merge_key_resolution", "stage": "silver_merge_key_resolution", "event_type": "stage_error"},
        )
        raise
    artifact = resolved_state.get("silver_merge_key_resolution_artifact") or {"run_id": run_id, "feeds": []}
    logger.info(
        "END Silver Merge Key Resolution feeds=%d duration_seconds=%.3f",
        len(artifact.get("feeds") or []),
        time.monotonic() - started_at,
        extra={
            "run_id": run_id,
            "node": "silver_merge_key_resolution",
            "stage": "silver_merge_key_resolution",
            "event_type": "stage_end",
            "feed_count": len(artifact.get("feeds") or []),
            "duration_seconds": round(time.monotonic() - started_at, 3),
        },
    )
    return {
        **resolved_state,
        "run_id": run_id,
        "status": "HITL_WAIT",
        "background_stage": None,
        "next_gate": None,
        "next_review_key": "silver_merge_key_review",
        "silver_merge_key_review_artifact": artifact,
        "resume_message": "Silver Merge Key Review is pending. Review merge keys before Silver generation.",
    }


def _filter_bronze_results_by_gate4_review(
    bronze_results: List[Dict[str, Any]],
    review_artifact: Dict[str, Any],
) -> List[Dict[str, Any]]:
    feeds = [feed for feed in (review_artifact or {}).get("feeds") or [] if isinstance(feed, dict)]
    if not feeds:
        return bronze_results

    approved_keys = {
        (
            str(feed.get("database_name") or "").strip().casefold(),
            str(feed.get("schema_name") or "").strip().casefold(),
            str(feed.get("table") or feed.get("table_name") or feed.get("entity") or "").strip().casefold(),
        )
        for feed in feeds
        if str(feed.get("review_status") or "").upper() == "APPROVED"
    }
    approved_tables = {
        key[2]
        for key in approved_keys
        if key[2]
    }
    rejected_keys = {
        (
            str(feed.get("database_name") or "").strip().casefold(),
            str(feed.get("schema_name") or "").strip().casefold(),
            str(feed.get("table") or feed.get("table_name") or feed.get("entity") or "").strip().casefold(),
        )
        for feed in feeds
        if str(feed.get("review_status") or "").upper() == "REJECTED"
    }
    rejected_tables = {
        key[2]
        for key in rejected_keys
        if key[2]
    }
    if not approved_tables and not rejected_tables:
        return bronze_results

    filtered: List[Dict[str, Any]] = []
    for result in bronze_results:
        table_name = str(result.get("table") or result.get("table_name") or result.get("entity") or "").strip().casefold()
        full_key = (
            str(result.get("database_name") or "").strip().casefold(),
            str(result.get("schema_name") or "").strip().casefold(),
            table_name,
        )
        if approved_tables:
            if full_key in approved_keys or table_name in approved_tables:
                filtered.append(result)
        elif full_key not in rejected_keys and table_name not in rejected_tables:
            filtered.append(result)
    return filtered


def _silver_review_keys(item: Dict[str, Any]) -> set[str]:
    keys: set[str] = set()
    for field in ("target_table", "silver_table", "source_table", "bronze_table", "table", "table_name", "entity"):
        value = str(item.get(field) or "").strip()
        if not value:
            continue
        folded = value.casefold()
        keys.add(folded)
        simple = value.split(".")[-1].strip('"').casefold()
        if simple:
            keys.add(simple)
            for prefix in ("silver_", "bronze_"):
                if simple.startswith(prefix):
                    keys.add(simple[len(prefix):])
    return keys


def _filter_silver_results_by_gate5_review(
    silver_results: List[Dict[str, Any]],
    review_artifact: Dict[str, Any],
) -> List[Dict[str, Any]]:
    items = [item for item in (review_artifact or {}).get("items") or [] if isinstance(item, dict)]
    if not items:
        return silver_results

    approved_items = [item for item in items if str(item.get("review_status") or "").upper() == "APPROVED"]
    rejected_items = [item for item in items if str(item.get("review_status") or "").upper() == "REJECTED"]
    if not approved_items and not rejected_items:
        return silver_results

    def matches(result: Dict[str, Any], review_item: Dict[str, Any]) -> bool:
        return bool(_silver_review_keys(result) & _silver_review_keys(review_item))

    filtered: List[Dict[str, Any]] = []
    for result in silver_results:
        if approved_items:
            if any(matches(result, item) for item in approved_items):
                filtered.append(result)
        elif not any(matches(result, item) for item in rejected_items):
            filtered.append(result)
    return filtered


def _filter_gold_contract_by_silver_results(contract: Dict[str, Any], silver_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(contract, dict) or not contract:
        return contract

    allowed_sources = {
        str(item.get("target_table") or item.get("silver_table") or "").strip().casefold()
        for item in silver_results
        if str(item.get("target_table") or item.get("silver_table") or "").strip()
    }
    warnings = list(contract.get("warnings") or [])
    if not allowed_sources:
        dropped = len(contract.get("kpi_mappings") or [])
        if dropped:
            warnings.append(f"Gold scope filtered out {dropped} KPI mapping(s) because no Silver source was approved for execution.")
            return {**contract, "kpi_mappings": [], "warnings": warnings}
        return contract

    mappings = [
        mapping
        for mapping in contract.get("kpi_mappings") or []
        if str(mapping.get("source_silver_table") or "").strip().casefold() in allowed_sources
    ]
    dropped = len(contract.get("kpi_mappings") or []) - len(mappings)
    if dropped:
        warnings.append(f"Gold scope filtered out {dropped} KPI mapping(s) because their Silver source was not approved for execution.")
    return {**contract, "kpi_mappings": mappings, "warnings": warnings}


def submit_gate4_review(
    run_id: str,
    action: str = "APPROVED",
    review_artifact: Optional[Dict[str, Any]] = None,
    checkpoint_state: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    checkpoint_state = checkpoint_state or load_checkpoint_state(run_id) or {"run_id": run_id}
    decision = str(action or "APPROVED").upper()
    final_state = {
        **checkpoint_state,
        "run_id": run_id,
        "bronze_review_decision": decision,
        "bronze_review_artifact": review_artifact or checkpoint_state.get("bronze_review_artifact") or {},
        "gate4": {"gate": "gate4", "status": "COMPLETED", "decision": decision},
    }

    if decision == "REJECTED":
        final_state["status"] = "FAILED"
        final_state["error"] = "Gate 4 rejected Bronze review artifact"
    elif decision == "REGENERATE":
        final_state["status"] = "REGENERATE_REQUIRED"
    elif decision == "APPROVED":
        enriched = final_state.get("enrichment_review_artifact") or final_state.get("enriched_metadata") or {}
        if isinstance(enriched, dict) and "enrichment_artifact" in enriched:
            enriched = enriched.get("enrichment_artifact") or {}
        reviewed_metadata = _apply_gate4_merge_keys_to_metadata(enriched, final_state["bronze_review_artifact"])
        final_state["enriched_metadata"] = reviewed_metadata
        final_state["enrichment_review_artifact"] = reviewed_metadata
        final_state["bronze_generation_results"] = _filter_bronze_results_by_gate4_review(
            [item for item in final_state.get("bronze_generation_results") or [] if isinstance(item, dict)],
            final_state["bronze_review_artifact"],
        )
        target_warehouse = str(final_state.get("target_warehouse") or "").lower()
        if target_warehouse == "snowflake":
            from services.snowflake_bronze_runtime import run_snowflake_bronze_scripts

            execution_state = {
                **final_state,
                "status": "RUNNING",
                "background_stage": "bronze_code_execution",
                "next_gate": None,
                "resume_message": "Executing approved Bronze scripts in Snowflake.",
            }
            save_checkpoint_state_timed(run_id, execution_state, context="bronze_code_execution:running")
            try:
                final_state = run_snowflake_bronze_scripts(
                    execution_state,
                    review_artifact=execution_state["bronze_review_artifact"],
                    approved_only=True,
                )
                if final_state.get("snowflake_bronze_execution_status") != "COMPLETED":
                    raise RuntimeError(
                        "Snowflake Bronze execution did not complete; refusing to continue to Silver."
                    )
            except Exception as exc:
                failed_state = {
                    **execution_state,
                    "status": "FAILED",
                    "background_stage": "bronze_code_execution",
                    "failed_background_stage": "bronze_code_execution",
                    "error": str(exc),
                }
                save_checkpoint_state_timed(run_id, failed_state, context="bronze_code_execution:failed")
                raise
            final_state["background_stage"] = None
        elif target_warehouse == "databricks":
            from services.databricks_runtime import databricks_bronze_execution_enabled, run_databricks_bronze_scripts

            if databricks_bronze_execution_enabled():
                execution_state = {
                    **final_state,
                    "status": "RUNNING",
                    "background_stage": "bronze_code_execution",
                    "next_gate": None,
                    "resume_message": "Executing approved Bronze scripts in Databricks.",
                }
                save_checkpoint_state_timed(run_id, execution_state, context="bronze_code_execution:running")
                try:
                    final_state = run_databricks_bronze_scripts(
                        execution_state,
                        review_artifact=execution_state["bronze_review_artifact"],
                        approved_only=True,
                    )
                    if final_state.get("databricks_bronze_execution_status") != "COMPLETED":
                        raise RuntimeError(
                            "Databricks Bronze execution did not complete; refusing to continue to Silver."
                        )
                except Exception as exc:
                    failed_state = {
                        **execution_state,
                        "status": "FAILED",
                        "background_stage": "bronze_code_execution",
                        "failed_background_stage": "bronze_code_execution",
                        "error": str(exc),
                    }
                    save_checkpoint_state_timed(run_id, failed_state, context="bronze_code_execution:failed")
                    raise
                final_state["background_stage"] = None
        final_state = _pause_for_silver_merge_key_review(run_id, final_state)
        ai_store_db_writer(
            run_id=run_id,
            stage="Bronze Review",
            artifact_type="GATE4_BRONZE_REVIEW",
            payload={
                "run_id": run_id,
                "decision": decision,
                "review_artifact": final_state["bronze_review_artifact"],
            },
            schema_version="GATE4_v1",
            prompt_version="UI_REVIEWER_v1",
            faithfulness_status="PASSED",
            token_count=0,
            input_tokens=0,
            output_tokens=0,
            fingerprint=str(final_state.get("fingerprint") or run_id),
        )
        save_checkpoint_state_timed(run_id, final_state, context="gate4:complete")
        if final_state.get("next_review_key") == "silver_merge_key_review":
            return final_state
        return continue_database_pipeline(run_id, start_stage_key="silver", state=final_state)

    save_checkpoint_state(run_id, final_state)
    return final_state


def submit_silver_merge_key_review(run_id: str, action: str = "APPROVED", review_artifact: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    checkpoint_state = load_checkpoint_state(run_id) or {"run_id": run_id}
    decision = str(action or "APPROVED").upper()
    artifact = review_artifact or _silver_merge_key_review_artifact(checkpoint_state)
    final_state = {
        **checkpoint_state,
        "run_id": run_id,
        "silver_merge_key_review_decision": decision,
        "silver_merge_key_review_artifact": artifact,
        "next_review_key": None,
        "gate_silver_merge_key_review": {
            "gate": "silver_merge_key_review",
            "status": "COMPLETED",
            "decision": decision,
        },
    }

    if decision == "REJECTED":
        final_state["status"] = "FAILED"
        final_state["error"] = "Silver Merge Key Review rejected merge keys"
    elif decision == "REGENERATE":
        final_state["status"] = "REGENERATE_REQUIRED"
        final_state["resume_message"] = "Silver Merge Key Review requested regeneration before Silver generation."
    elif decision == "APPROVED":
        enriched = final_state.get("enrichment_review_artifact") or final_state.get("enriched_metadata") or {}
        if isinstance(enriched, dict) and "enrichment_artifact" in enriched:
            enriched = enriched.get("enrichment_artifact") or {}
        reviewed_metadata = _apply_gate4_merge_keys_to_metadata(enriched, artifact)
        final_state["enriched_metadata"] = reviewed_metadata
        final_state["enrichment_review_artifact"] = reviewed_metadata
        final_state["status"] = "RUNNING"
        final_state["next_gate"] = None
        final_state["resume_message"] = "Silver Merge Key Review approved. Silver generation is starting."

    ai_store_db_writer(
        run_id=run_id,
        stage="Silver Merge Key Review",
        artifact_type="SILVER_MERGE_KEY_REVIEW",
        payload={
            "run_id": run_id,
            "decision": decision,
            "review_artifact": artifact,
        },
        schema_version="SILVER_MERGE_KEY_REVIEW_v1",
        prompt_version="UI_REVIEWER_v1",
        faithfulness_status="PASSED" if decision == "APPROVED" else "WARN",
        token_count=0,
        input_tokens=0,
        output_tokens=0,
        fingerprint=str(final_state.get("fingerprint") or run_id),
    )
    save_checkpoint_state(run_id, final_state)
    if decision == "APPROVED":
        return continue_database_pipeline(run_id, start_stage_key="silver", state=final_state)
    return final_state


def submit_bronze_generation(run_id: str) -> Dict[str, Any]:
    from nodes.bronze_gen import bronze_code_generation_node

    checkpoint_state = load_checkpoint_state(run_id) or {"run_id": run_id}
    gate2_payload = fetch_json_artifact(run_id, "GATE2_CERTIFIED_TABLES")
    certified_tables = (
        gate2_payload.get("certified_tables", [])
        or checkpoint_state.get("certified_tables")
        or []
    )
    if not certified_tables:
        raise ValueError("Bronze generation failed: no Table Review certified tables found.")

    state: Dict[str, Any] = {
        **checkpoint_state,
        "run_id": run_id,
        "certified_tables": certified_tables,
        "discovered_metadata": fetch_json_artifact(run_id, "DISCOVERED_METADATA") or checkpoint_state.get("discovered_metadata") or {},
        "bronze_catalog": os.getenv("BRONZE_CATALOG", "main"),
        "bronze_schema": os.getenv("BRONZE_SCHEMA", "bronze"),
    }
    result = bronze_code_generation_node(state)
    final_state = {**checkpoint_state, **result, "run_id": run_id}
    if str(result.get("bronze_generation_status") or "").upper() == "COMPLETED":
        final_state.update(
            {
                "status": "HITL_WAIT",
                "next_gate": 4,
                "resume_message": "Bronze Review is pending. Review generated Bronze scripts before Silver generation.",
            }
        )
    save_checkpoint_state(run_id, final_state)
    return final_state


def submit_silver_generation(run_id: str) -> Dict[str, Any]:
    from nodes.silver_gen import silver_code_generation_node

    checkpoint_state = load_checkpoint_state(run_id) or {"run_id": run_id}
    metadata = fetch_json_artifact(run_id, "ENRICHED_METADATA")
    gate3_payload = fetch_json_artifact(run_id, "GATE3_APPROVED_ENRICHMENT")
    if not metadata and isinstance(gate3_payload, dict):
        metadata = gate3_payload.get("enrichment_artifact") or {}

    state: Dict[str, Any] = {
        **checkpoint_state,
        "run_id": run_id,
        "enriched_metadata": metadata,
        "silver_catalog": os.getenv("SILVER_CATALOG", os.getenv("BRONZE_CATALOG", "main")),
        "silver_schema": os.getenv("SILVER_SCHEMA", "silver"),
    }
    result = silver_code_generation_node(state)
    final_state = {**checkpoint_state, **result, "run_id": run_id}
    if str(result.get("silver_generation_status") or "").upper() == "COMPLETED":
        final_state.update(
            {
                "status": "HITL_WAIT",
                "next_gate": 5,
                "resume_message": "Silver Review is pending. Review generated Silver scripts before Gold generation.",
            }
        )
    save_checkpoint_state(run_id, final_state)
    return final_state


def submit_gate5_review(run_id: str, action: str = "APPROVED", review_artifact: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    checkpoint_state = load_checkpoint_state(run_id) or {"run_id": run_id}
    decision = str(action or "APPROVED").upper()
    final_state = {
        **checkpoint_state,
        "run_id": run_id,
        "silver_review_decision": decision,
        "silver_review_artifact": review_artifact or checkpoint_state.get("silver_review_artifact") or {},
        "gate5": {"gate": "gate5", "status": "COMPLETED", "decision": decision},
    }

    if decision == "REJECTED":
        final_state["status"] = "FAILED"
        final_state["error"] = "Gate 5 rejected Silver review artifact"
    elif decision == "REGENERATE":
        final_state["status"] = "REGENERATE_REQUIRED"
    elif decision == "APPROVED":
        final_state["status"] = "RUNNING"
        selected_silver_results = _filter_silver_results_by_gate5_review(
            [item for item in final_state.get("silver_generation_results") or [] if isinstance(item, dict)],
            final_state["silver_review_artifact"],
        )
        final_state["silver_generation_results"] = selected_silver_results
        final_state["gold_generation_contract"] = _filter_gold_contract_by_silver_results(
            final_state.get("gold_generation_contract") or {},
            selected_silver_results,
        )
        target_warehouse = str(final_state.get("target_warehouse") or "").lower()
        if target_warehouse == "snowflake":
            from services.snowflake_silver_runtime import run_snowflake_silver_scripts

            execution_state = {
                **final_state,
                "background_stage": "silver_code_execution",
                "next_gate": None,
                "resume_message": "Executing approved Silver scripts in Snowflake.",
            }
            save_checkpoint_state(run_id, execution_state)
            try:
                final_state = run_snowflake_silver_scripts(
                    execution_state,
                    review_artifact=execution_state["silver_review_artifact"],
                    approved_only=True,
                )
            except Exception as exc:
                failed_state = {
                    **execution_state,
                    "status": "FAILED",
                    "background_stage": "silver_code_execution",
                    "failed_background_stage": "silver_code_execution",
                    "error": str(exc),
                }
                save_checkpoint_state(run_id, failed_state)
                raise
            final_state["background_stage"] = None
        elif target_warehouse == "databricks":
            from services.databricks_runtime import databricks_silver_execution_enabled, run_databricks_silver_scripts

            if databricks_silver_execution_enabled():
                execution_state = {
                    **final_state,
                    "background_stage": "silver_code_execution",
                    "next_gate": None,
                    "resume_message": "Executing approved Silver scripts in Databricks.",
                }
                save_checkpoint_state(run_id, execution_state)
                try:
                    final_state = run_databricks_silver_scripts(
                        execution_state,
                        review_artifact=execution_state["silver_review_artifact"],
                        approved_only=True,
                    )
                    if final_state.get("databricks_silver_execution_status") != "COMPLETED":
                        raise RuntimeError(
                            "Databricks Silver execution did not complete; refusing to continue to Gold."
                        )
                except Exception as exc:
                    failed_state = {
                        **execution_state,
                        "status": "FAILED",
                        "background_stage": "silver_code_execution",
                        "failed_background_stage": "silver_code_execution",
                        "error": str(exc),
                    }
                    save_checkpoint_state(run_id, failed_state)
                    raise
                final_state["background_stage"] = None

    ai_store_db_writer(
        run_id=run_id,
        stage="Silver Review",
        artifact_type="GATE5_SILVER_REVIEW",
        payload={
            "run_id": run_id,
            "decision": decision,
            "review_artifact": final_state["silver_review_artifact"],
        },
        schema_version="GATE5_v1",
        prompt_version="UI_REVIEWER_v1",
        faithfulness_status="PASSED" if decision == "APPROVED" else "WARN",
        token_count=0,
        input_tokens=0,
        output_tokens=0,
        fingerprint=str(final_state.get("fingerprint") or run_id),
    )
    save_checkpoint_state(run_id, final_state)
    if decision == "APPROVED":
        return continue_database_pipeline(run_id, start_stage_key="gold", state=final_state)
    return final_state


def submit_gold_generation(run_id: str) -> Dict[str, Any]:
    from nodes.gold_gen import gold_code_generation_node

    checkpoint_state = load_checkpoint_state(run_id) or {"run_id": run_id}
    contract = (
        checkpoint_state.get("gold_generation_contract")
        or fetch_json_artifact(run_id, "GOLD_GENERATION_CONTRACT")
        or {}
    )
    state: Dict[str, Any] = {
        **checkpoint_state,
        "run_id": run_id,
        "gold_generation_contract": contract,
    }
    if str(checkpoint_state.get("target_warehouse") or "").lower() == "snowflake":
        state["gold_catalog"] = os.getenv("SNOWFLAKE_GOLD_CATALOG") or os.getenv("SNOWFLAKE_SILVER_CATALOG") or "ATHENA_DB"
        state["gold_schema"] = os.getenv("SNOWFLAKE_GOLD_SCHEMA", "GOLD")
    else:
        state["gold_schema"] = os.getenv("GOLD_SCHEMA", "gold")
    result = gold_code_generation_node(state)
    final_state = {**checkpoint_state, **result, "run_id": run_id}
    if str(result.get("gold_generation_status") or "").startswith("COMPLETED"):
        final_state.update(
            {
                "status": "HITL_WAIT",
                "background_stage": None,
                "next_gate": None,
                "next_review_key": "gold_review",
                "gold_review_artifact": {
                    "items": [item for item in result.get("gold_generation_results") or [] if isinstance(item, dict)],
                },
                "resume_message": "Gold Review is pending. Review generated Gold scripts before execution.",
            }
        )
    save_checkpoint_state(run_id, final_state)
    return final_state


def submit_gold_review(run_id: str, action: str = "APPROVED", review_artifact: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    checkpoint = load_checkpoint_state(run_id) or {"run_id": run_id}
    decision = str(action or "APPROVED").upper()
    final_state = {
        **checkpoint,
        "run_id": run_id,
        "gold_review_decision": decision,
        "gold_review_artifact": review_artifact or checkpoint.get("gold_review_artifact") or {},
        "next_review_key": None,
    }

    if decision == "REJECTED":
        final_state.update({"status": "FAILED", "error": "Gold Review rejected generated Gold scripts"})
    elif decision == "REGENERATE":
        final_state.update({"status": "REGENERATE_REQUIRED", "resume_message": "Gold Review requested regeneration."})
    elif decision == "APPROVED" and str(final_state.get("target_warehouse") or "").lower() == "snowflake":
        from services.snowflake_gold_runtime import run_snowflake_gold_scripts

        execution_state = {
            **final_state,
            "status": "RUNNING",
            "background_stage": "gold_code_execution",
            "resume_message": "Executing approved Gold scripts in Snowflake.",
        }
        save_checkpoint_state(run_id, execution_state)
        try:
            final_state = run_snowflake_gold_scripts(execution_state)
        except Exception as exc:
            failed_state = {
                **execution_state,
                "status": "FAILED",
                "failed_background_stage": "gold_code_execution",
                "error": str(exc),
            }
            save_checkpoint_state(run_id, failed_state)
            raise
        final_state.update({"status": "PIPELINE_COMPLETED", "background_stage": None, "next_review_key": None})
    elif decision == "APPROVED" and str(final_state.get("target_warehouse") or "").lower() == "databricks":
        from services.databricks_runtime import databricks_gold_execution_enabled, run_databricks_gold_scripts

        if databricks_gold_execution_enabled():
            execution_state = {
                **final_state,
                "status": "RUNNING",
                "background_stage": "gold_code_execution",
                "resume_message": "Executing approved Gold scripts in Databricks.",
            }
            save_checkpoint_state(run_id, execution_state)
            try:
                final_state = run_databricks_gold_scripts(
                    execution_state,
                    review_artifact=execution_state["gold_review_artifact"],
                    approved_only=True,
                )
            except Exception as exc:
                failed_state = {
                    **execution_state,
                    "status": "FAILED",
                    "failed_background_stage": "gold_code_execution",
                    "error": str(exc),
                }
                save_checkpoint_state(run_id, failed_state)
                raise
        final_state.update({"status": "PIPELINE_COMPLETED", "background_stage": None, "next_review_key": None})
    elif decision == "APPROVED":
        final_state.update({"status": "PIPELINE_COMPLETED", "background_stage": None})

    save_checkpoint_state(run_id, final_state)
    return final_state
