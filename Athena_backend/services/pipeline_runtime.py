from __future__ import annotations

import json
import os
import re
import uuid
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Optional

from utilis.db import config, get_completed_items, get_connection, get_pending_items, timed_stage, update_hitl_items_batch
from utilis.logger import logger


BACKGROUND_EXECUTOR = ThreadPoolExecutor(max_workers=int(os.getenv("ATHENA_BACKGROUND_WORKERS", "2")))
BACKGROUND_JOBS: Dict[str, Future] = {}
BACKGROUND_JOB_LOCK = threading.Lock()
SCRIPT_BUNDLE_CACHE_LOCK = threading.Lock()
SCRIPT_BUNDLE_CACHE: Dict[str, Dict[str, Any]] = {}

DATABASE_STAGE_SEQUENCE = [
    ("ingestion", "BRD Ingest"),
    ("memory", "Memory Check"),
    ("requirements", "Requirement Extraction"),
    ("kpis", "KPI Extraction"),
    ("gate1", "KPI Review"),
    ("nomination", "Table Nomination"),
    ("gate2", "Table Review"),
    ("discovery", "Metadata Discovery"),
    ("profiling", "Column Profiling"),
    ("enrichment", "Semantic Enrichment"),
    ("gate3", "Enrichment Review"),
    ("bronze", "Bronze Generation"),
    ("silver", "Silver Generation"),
    ("gold", "Gold Generation"),
]

DATABASE_STAGE_LABELS = dict(DATABASE_STAGE_SEQUENCE)


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
        return "Enrichment Review"
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
        from nodes.bronze_gen import bronze_code_generation_node

        return bronze_code_generation_node
    if stage_key == "silver":
        from nodes.silver_gen import silver_code_generation_node

        return silver_code_generation_node
    if stage_key == "gold":
        from nodes.gold_gen import gold_code_generation_node

        return gold_code_generation_node
    raise ValueError(f"Unsupported database stage: {stage_key}")


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
        runner = _database_stage_runner(current_stage_key)
        result = runner(working_state)
        if not isinstance(result, dict):
            raise ValueError(f"Stage {current_stage_key} returned an invalid state.")

        working_state = {**working_state, **result, "run_id": run_id}
        working_state["awaiting_stage_confirmation"] = False
        working_state["last_completed_stage_key"] = current_stage_key
        working_state["last_completed_stage_label"] = DATABASE_STAGE_LABELS.get(current_stage_key, current_stage_key)
        working_state["next_stage_key"] = _database_next_stage_key(current_stage_key)
        working_state["next_stage_label"] = DATABASE_STAGE_LABELS.get(working_state["next_stage_key"], working_state["next_stage_key"]) if working_state.get("next_stage_key") else None
        save_checkpoint_state(run_id, working_state)

        if working_state.get("status") == "FAILED":
            return working_state
        if str(working_state.get("status") or "").upper() in {"HITL_WAIT", "PAUSED_FOR_HITL"}:
            return working_state

        if stage_confirmation_enabled and working_state.get("next_stage_key"):
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


def mark_run_processing(run_id: str, stage: str) -> None:
    checkpoint = load_checkpoint_state(run_id) or {"run_id": run_id}
    checkpoint.update(
        {
            "run_id": run_id,
            "status": "PROCESSING",
            "background_stage": stage,
        }
    )
    save_checkpoint_state(run_id, checkpoint)


def submit_background(run_id: str, stage: str, fn, *args) -> Future:
    job_key = f"{run_id}:{stage}"
    with BACKGROUND_JOB_LOCK:
        existing = BACKGROUND_JOBS.get(job_key)
        if existing and not existing.done():
            logger.info("Background %s already running for run_id=%s", stage, run_id)
            return existing

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
            save_checkpoint_state(run_id, checkpoint)
        except Exception as exc:
            logger.exception("Background %s failed for run_id=%s", stage, run_id)
            checkpoint = load_checkpoint_state(run_id) or {"run_id": run_id}
            checkpoint.update(
                {
                    "run_id": run_id,
                    "status": "FAILED",
                    "background_stage": None,
                    "failed_background_stage": stage,
                    "error": str(exc),
                }
            )
            save_checkpoint_state(run_id, checkpoint)
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
        cursor.execute(
            f"""
            WITH ai_runs AS (
                SELECT run_id, MAX(stored_at) AS last_activity
                FROM [{_pipeline_schema()}].[ai_store]
                GROUP BY run_id
            ),
            queue_runs AS (
                SELECT run_id, MAX(COALESCE(decided_at, queued_at)) AS last_activity
                FROM [{_pipeline_schema()}].[hitl_review_queue]
                GROUP BY run_id
            ),
            checkpoint_runs AS (
                SELECT run_id, MAX(checkpoint_at) AS last_activity
                FROM [{_pipeline_schema()}].[kpi_checkpoints]
                GROUP BY run_id
            ),
            registry_runs AS (
                SELECT run_id, MAX(timestamp) AS last_activity
                FROM [{_pipeline_schema()}].[brd_run_registry]
                GROUP BY run_id
            ),
            combined AS (
                SELECT run_id, last_activity FROM ai_runs
                UNION
                SELECT run_id, last_activity FROM queue_runs
                UNION
                SELECT run_id, last_activity FROM checkpoint_runs
                UNION
                SELECT run_id, last_activity FROM registry_runs
            )
            SELECT TOP ({limit}) run_id, MAX(last_activity) AS last_activity
            FROM combined
            GROUP BY run_id
            ORDER BY MAX(last_activity) DESC
            """
        )
        rows = cursor.fetchall()
        return [
            {
                "run_id": row[0],
                "last_activity": row[1],
            }
            for row in rows
            if row[0]
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
                FROM [{_pipeline_schema()}].[kpi_checkpoints]
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


def _script_bundle_path(layer: str, run_id: str) -> Path:
    output_dir = Path(os.getcwd()) / "generated_code" / layer
    run_scoped = output_dir / f"{_run_slug(run_id)}_{layer}_scripts.json"
    return run_scoped if run_scoped.exists() else output_dir / f"{layer}_scripts.json"


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
        row = {
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
    bundle_path = _script_bundle_path("bronze", run_id)
    if not bundle_path.exists():
        return _scripts_from_checkpoint(checkpoint or {}, "bronze_generation_results", "bronze_generated_at")

    bundle = _load_script_bundle(bundle_path)
    if not bundle:
        return _scripts_from_checkpoint(checkpoint or {}, "bronze_generation_results", "bronze_generated_at")
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
        return _scripts_from_checkpoint(checkpoint, "bronze_generation_results", "bronze_generated_at")

    return {
        "run_id": bundle_run_id,
        "generated_at": bundle.get("generated_at"),
        "scripts": _dedupe_scripts(scripts),
    }


def load_silver_scripts(run_id: str, checkpoint: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    bundle_path = _script_bundle_path("silver", run_id)
    if not bundle_path.exists():
        return _scripts_from_checkpoint(checkpoint or {}, "silver_generation_results", "silver_generated_at")

    bundle = _load_script_bundle(bundle_path)
    if not bundle:
        return _scripts_from_checkpoint(checkpoint or {}, "silver_generation_results", "silver_generated_at")
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
    bundle_path = _script_bundle_path("gold", run_id)
    if not bundle_path.exists():
        return _scripts_from_checkpoint(checkpoint or {}, "gold_generation_results", "gold_generated_at")

    bundle = _load_script_bundle(bundle_path)
    if not bundle:
        return _scripts_from_checkpoint(checkpoint or {}, "gold_generation_results", "gold_generated_at")
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

    def has_stage(text: str) -> bool:
        needle = text.lower()
        return any(needle in stage for stage in stages)

    if source in {"sftp", "adls_gen2"}:
        gate1_decision = (checkpoint.get("gate1") or {}).get("decision")
        gate2_decision = (checkpoint.get("gate2") or {}).get("decision")
        gate4_decision = (checkpoint.get("gate4") or {}).get("decision")
        gate5_decision = (checkpoint.get("gate5") or {}).get("decision")
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
                "complete": bool("KPIS" in artifact_types or checkpoint.get("kpis")),
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
                "key": "pre_bronze",
                "label": "Pre-Bronze Readiness",
                "complete": bool(checkpoint.get("bronze_review_artifact")),
                "detail": "Bronze readiness inputs assembled",
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
                "detail": "Bronze review",
            },
            {
                "key": "pull",
                "label": "Source Handoff" if source == "adls_gen2" else "SFTP Pull",
                "complete": bool(
                    checkpoint.get("sftp_pull_status") == "COMPLETED"
                    or checkpoint.get("bronze_ingestion_status") == "HANDOFF_ONLY"
                ),
                "detail": "Approved ADLS source handed to generated Bronze script"
                if source == "adls_gen2"
                else "Approved SFTP files synchronized",
            },
            {
                "key": "bronze_validation",
                "label": "Bronze Validation",
                "complete": bool(checkpoint.get("bronze_validation_status") == "COMPLETED"),
                "detail": "Bronze readiness validated",
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
                "key": "dq_validation",
                "label": "DQ Validation",
                "complete": bool(checkpoint.get("dq_validation_status") == "COMPLETED" or checkpoint.get("dq_validation_status") == "SKIPPED"),
                "detail": "Data quality validation placeholder",
            },
            {
                "key": "gold",
                "label": "Gold Code Generation",
                "complete": gold_generation_completed,
                "detail": "Gold KPI generation completed",
            },
        ]
    else:
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
            "key": "requirements",
            "label": "Req Extract",
            "complete": bool(artifact_types.intersection({"REQUIREMENTS", "REQUIREMENTS_WARN"})),
            "detail": "Business requirements extracted",
        },
        {
            "key": "kpis",
            "label": "KPI Extract",
            "complete": bool("KPIS" in artifact_types or pending_gate1 or completed_gate1),
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
            "label": "Nomination",
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
            "label": "Metadata Discovery",
            "complete": bool("DISCOVERED_METADATA" in artifact_types or checkpoint.get("metadata_status")),
            "detail": "Table metadata discovered",
        },
        {
            "key": "profiling",
            "label": "Column Profiling",
            "complete": bool("COLUMN_PROFILES" in artifact_types or checkpoint.get("column_profiling_status")),
            "detail": "Column profiles generated",
        },
        {
            "key": "enrichment",
            "label": "Semantic Enrichment",
            "complete": bool("ENRICHED_METADATA" in artifact_types or enriched_payload or checkpoint.get("semantic_enrichment_status")),
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
            "label": "Bronze Scripts",
            "complete": bool(bronze_generation_completed),
            "detail": "Bronze scripts generated",
        },
        {
            "key": "silver",
            "label": "Silver Scripts",
            "complete": bool(silver_generation_completed),
            "detail": "Silver transformation scripts generated",
        },
        {
            "key": "gold",
            "label": "Gold Scripts",
            "complete": bool(gold_generation_completed),
            "detail": "Gold KPI scripts generated",
        },
        ]

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

    checkpoint_status = str(checkpoint.get("status") or "").upper()
    pipeline_is_active = bool(
        checkpoint.get("background_stage")
        or checkpoint_status in {"RUNNING", "PROCESSING", "SUBMITTED", "IN_PROGRESS"}
    )

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

    # If pipeline failed, mark the failed step
    if checkpoint.get("status") == "FAILED":
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
    if downstream_progress_exists:
        pending_gate1 = []

    # For SFTP runs, the feed review replaces table nomination.
    # Ensure we don't render DB-table review panels for SFTP runs.
    if source_value in {"sftp", "adls_gen2"}:
        nominated_tables = []
        certified_tables = []
        pending_gate1 = []  # SFTP gate1 is tracked via checkpoint.gate1, not SQL queue.
        completed_gate1 = []
    bronze_generation_completed = any(
        row.get("artifact_type") in {"BRONZE_GENERATION", "BRONZE_SCRIPTS"}
        or str(row.get("stage", "")).lower().startswith("bronze")
        for row in summary
    ) or checkpoint.get("bronze_generation_status") == "COMPLETED"
    silver_generation_completed = any(
        row.get("artifact_type") in {"SILVER_GENERATION", "SILVER_SCRIPTS"}
        or str(row.get("stage", "")).lower().startswith("silver")
        for row in summary
    ) or checkpoint.get("silver_generation_status") == "COMPLETED"
    gold_generation_completed = any(
        row.get("artifact_type") in {"GOLD_GENERATION", "GOLD_SCRIPTS"}
        or str(row.get("stage", "")).lower().startswith("gold")
        for row in summary
    ) or str(checkpoint.get("gold_generation_status") or "").startswith("COMPLETED")
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

    next_gate = None
    resume_message = None
    if source_value in {"sftp", "adls_gen2"}:
        gate1_decision = (checkpoint.get("gate1") or {}).get("decision")
        gate2_decision = (checkpoint.get("gate2") or {}).get("decision")
        if gate1_decision in {None, ""}:
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
    elif pending_gate1:
        next_gate = 1
        resume_message = "KPI Review is pending. Review the KPI items below."
    elif nominated_tables and not certified_tables:
        next_gate = 2
        resume_message = "Table Review is pending. Review and certify nominated tables below."
    elif enriched_payload and not gate3_payload:
        next_gate = 3
        resume_message = "Enrichment Review is pending. Review enrichment details below."
    elif gate3_payload:
        resume_message = "Enrichment Review is complete."
    elif certified_tables and not enriched_payload:
        resume_message = "Table Review is certified. Downstream metadata/profiling/enrichment has not completed yet."
    elif completed_gate1 and not nominated_tables:
        resume_message = "KPI Review is certified. Table nomination has not completed yet."
    elif not summary and not checkpoint:
        resume_message = "No stored state was found for this run ID."

    stage_confirmation = None
    if checkpoint.get("status") == "PAUSED_FOR_STAGE_CONFIRMATION":
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

    status = checkpoint.get("status") or checkpoint.get("table_nomination_status") or checkpoint.get("enrichment_review_status") or "UNKNOWN"
    if checkpoint.get("bronze_generation_status") == "COMPLETED":
        status = "PIPELINE_COMPLETED"
    if gate3_payload and bronze_generation_completed:
        status = "PIPELINE_COMPLETED"
    if silver_generation_completed:
        status = "PIPELINE_COMPLETED"
    if gold_generation_completed:
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
    waiting_gate_key = "gate1" if next_gate == 1 else "gate2" if next_gate == 2 else "gate3" if next_gate == 3 else None
    pipeline_steps = apply_waiting_stage_state(pipeline_steps, waiting_gate_key)
    if checkpoint.get("status") == "PAUSED_FOR_STAGE_CONFIRMATION" and checkpoint.get("next_stage_key"):
        for step in pipeline_steps:
            if step.get("key") == checkpoint.get("next_stage_key") and step.get("state") == "PENDING":
                step["detail"] = f"Waiting for confirmation before {checkpoint.get('next_stage_label') or step.get('label')}."
                break
    current_pipeline_step = next((step for step in pipeline_steps if str(step.get("state")).upper() == "RUNNING"), None)
    if not current_pipeline_step and waiting_gate_key:
        current_pipeline_step = next((step for step in pipeline_steps if step["key"] == waiting_gate_key), None)
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
        "resume_message": resume_message,
        "stage_confirmation": stage_confirmation,
        "status": status,
        "pipeline_steps": pipeline_steps,
        "current_pipeline_step": current_pipeline_step,
    }


def start_pipeline(
    *,
    brd_text: Optional[str] = None,
    input_path: Optional[str] = None,
    source: Optional[str] = None,
    source_databases: Optional[List[str]] = None,
    sftp_entity: Optional[str] = None,
    run_id: Optional[str] = None,
    use_domain_kb: bool = False,
    stage_confirmation_enabled: bool = True,
) -> Dict[str, Any]:
    run_id = run_id or str(uuid.uuid4())
    default_source_db = config["azure_sql"].get("source_database") or "insurance"
    source_value = str(source or "database").lower()
    file_sources = {"sftp", "adls_gen2"}
    initial_state: Dict[str, Any] = {
        "brd_text": brd_text or input_path or "",
        "run_id": run_id,
        "metadata": {},
        "status": "PENDING",
        "source": source_value,
        "sftp_entity": str(sftp_entity or "transactions").lower(),
        "source_databases": source_databases or [default_source_db],
        "use_domain_kb": bool(use_domain_kb),
        "stage_confirmation_enabled": bool(stage_confirmation_enabled),
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


def submit_gate2_review(run_id: str, approved_keys: List[str]) -> Dict[str, Any]:
    from nodes.hitl import hitl_table_review_node

    tables = fetch_json_artifact(run_id, "TABLE_NOMINATIONS").get("nominations", []) or []
    approved_key_set = set(approved_keys)
    approved = [item for item in tables if _table_key(item) in approved_key_set]

    if not approved:
        raise ValueError("At least one table must be approved for Table Review.")

    resumed_input = load_checkpoint_state(run_id) or {"run_id": run_id}
    resumed_input["human_table_decision"] = "COMPLETED"
    resumed_input["certified_tables"] = approved
    with timed_stage("gate2_hitl_certification", run_id=run_id, node="api"):
        resumed = hitl_table_review_node(resumed_input)
    if resumed.get("status") == "FAILED":
        raise ValueError(resumed.get("error", "Table Review certification failed."))
    save_checkpoint_state(run_id, resumed)

    return continue_database_pipeline(run_id, start_stage_key="discovery", state=resumed)


def submit_gate3_review(run_id: str, approve: bool) -> Dict[str, Any]:
    from nodes.hitl import build_hitl_enrichment_review_node

    checkpoint_state = load_checkpoint_state(run_id) or {}
    metadata = fetch_json_artifact(run_id, "ENRICHED_METADATA") or _checkpoint_enriched_payload(checkpoint_state)
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
    return continue_database_pipeline(run_id, start_stage_key="bronze", state=bronze_state)


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
    save_checkpoint_state(run_id, final_state)
    return result


def submit_silver_generation(run_id: str) -> Dict[str, Any]:
    from nodes.silver_gen import silver_code_generation_node
    from nodes.gold_gen import gold_code_generation_node

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
    gold_result = gold_code_generation_node({**checkpoint_state, **result, "run_id": run_id, "gold_schema": os.getenv("GOLD_SCHEMA", "gold")})
    final_state = {**checkpoint_state, **result, **gold_result, "run_id": run_id}
    if result.get("silver_generation_status") == "COMPLETED" or str(gold_result.get("gold_generation_status") or "").startswith("COMPLETED"):
        final_state["status"] = "PIPELINE_COMPLETED"
    save_checkpoint_state(run_id, final_state)
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
        "gold_schema": os.getenv("GOLD_SCHEMA", "gold"),
    }
    result = gold_code_generation_node(state)
    final_state = {**checkpoint_state, **result, "run_id": run_id}
    if str(result.get("gold_generation_status") or "").startswith("COMPLETED"):
        final_state["status"] = "PIPELINE_COMPLETED"
    save_checkpoint_state(run_id, final_state)
    return result
