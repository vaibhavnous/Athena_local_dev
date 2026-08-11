from __future__ import annotations

import json
import os
import re
import time
import uuid
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import HTTPException

from services.dbt_snowflake_runtime import (
    execute_finalized_snowflake_dbt_project,
    finalize_snowflake_dbt_project,
    run_snowflake_dbt,
    snowflake_dbt_enabled,
)
from utilis.db import (
    ai_store_db_writer,
    config,
    ensure_hitl_queue_items,
    get_completed_items,
    get_connection,
    get_hitl_items,
    get_pending_items,
    save_hitl_item_draft,
    timed_stage,
    update_hitl_items_batch,
)
from utilis.generated_code_paths import generated_code_dir
from utilis.logger import logger, redact_sensitive


BACKGROUND_WORKER_COUNT = max(1, int(os.getenv("ATHENA_BACKGROUND_WORKERS", "2")))
BACKGROUND_EXECUTOR = ThreadPoolExecutor(max_workers=BACKGROUND_WORKER_COUNT)
BACKGROUND_JOBS: Dict[str, Future] = {}
BACKGROUND_JOB_LOCK = threading.Lock()
ABORTED_RUNS: set[str] = set()
SCRIPT_BUNDLE_CACHE_LOCK = threading.Lock()
SCRIPT_BUNDLE_CACHE: Dict[str, Dict[str, Any]] = {}
ACTIVE_CHECKPOINT_STATUSES = {"RUNNING", "PROCESSING", "SUBMITTED", "IN_PROGRESS"}
SNOWFLAKE_COMPLETED_EXECUTION_STATUSES = {
    "COMPLETED",
    "COMPLETED_WITH_WARNINGS",
    "SKIPPED_DBT_CODEGEN_ONLY",
}
COMPLETED_EXECUTION_STATUSES = {"COMPLETED", "COMPLETED_WITH_WARNINGS", "SKIPPED", "HANDOFF_ONLY"}
REVIEW_CHECKPOINT_FIELDS = {
    "bronze_review_decision",
    "bronze_review_artifact",
    "gate4",
    "silver_merge_key_review_decision",
    "silver_merge_key_review_artifact",
    "silver_review_decision",
    "silver_review_artifact",
    "gate5",
    "gold_review_decision",
    "gold_review_artifact",
}
GENERATION_ARTIFACT_TYPES = {
    "bronze": {"BRONZE_GENERATION", "BRONZE_SCRIPTS", "SFTP_BRONZE_GENERATION"},
    "silver": {"SILVER_GENERATION", "SILVER_SCRIPTS", "SFTP_SILVER_GENERATION"},
    "gold": {"GOLD_GENERATION", "GOLD_SCRIPTS", "SFTP_GOLD_GENERATION"},
}
DATABASE_GENERATION_FIRST_FLOW_VERSION = "generation_first_v2"
LEGACY_DATABASE_GENERATION_FIRST_FLOW_VERSION = "generation_first_v1"


def _status_completed(value: Any) -> bool:
    return str(value or "").upper() in COMPLETED_EXECUTION_STATUSES


def _gold_partial_success_ratio() -> float:
    try:
        return min(1.0, max(0.5, float(os.getenv("ATHENA_GOLD_MIN_SUCCESS_RATIO", "0.9"))))
    except ValueError:
        return 0.9


def generation_first_database_flow(state: Dict[str, Any]) -> bool:
    return (
        str(state.get("database_flow_version") or "")
        in {
            DATABASE_GENERATION_FIRST_FLOW_VERSION,
            LEGACY_DATABASE_GENERATION_FIRST_FLOW_VERSION,
        }
        and str(state.get("source") or "database").lower() == "database"
        and str(state.get("target_warehouse") or "").lower() in {"databricks", "snowflake"}
    )


def revised_metadata_database_flow(state: Dict[str, Any]) -> bool:
    return str(state.get("database_flow_version") or "") == DATABASE_GENERATION_FIRST_FLOW_VERSION


def generation_first_native_database_flow(state: Dict[str, Any]) -> bool:
    return generation_first_database_flow(state) and not snowflake_dbt_enabled(state)


def generation_first_snowflake_dbt_flow(state: Dict[str, Any]) -> bool:
    return generation_first_database_flow(state) and snowflake_dbt_enabled(state)


def _invalidate_generation_first_review_state(
    state: Dict[str, Any],
    *,
    boundary: str,
) -> Dict[str, Any]:
    if not generation_first_database_flow(state):
        return state

    updated = dict(state)
    execution_layers = {
        "gate4": {"bronze", "silver", "gold"},
        "silver_merge_key_review": {"bronze", "silver", "gold"},
        "gate5": {"silver", "gold"},
        "gold_review": {"gold"},
    }.get(boundary, set())
    downstream_prefixes = {
        "gate4": (
            "silver_merge_key_resolution_",
            "silver_merge_key_review_",
            "silver_generation_",
            "silver_review_",
            "gold_contract_",
            "gold_generation_",
            "gold_review_",
        ),
        "silver_merge_key_review": (
            "silver_generation_",
            "silver_review_",
            "gold_contract_",
            "gold_generation_",
            "gold_review_",
        ),
        "gate5": (
            "gold_contract_",
            "gold_generation_",
            "gold_review_",
        ),
        "gold_review": (),
    }.get(boundary, ())
    downstream_exact = {
        "gate4": {
            "gate_silver_merge_key_review",
            "gate5",
            "gold_generation_contract",
        },
        "silver_merge_key_review": {
            "gate5",
            "gold_generation_contract",
        },
        "gate5": {"gold_generation_contract"},
        "gold_review": set(),
    }.get(boundary, set())
    invalidated_review_fields = {
        "gate4": {
            "silver_merge_key_review_decision",
            "silver_merge_key_review_artifact",
            "silver_review_decision",
            "silver_review_artifact",
            "gate5",
            "gold_review_decision",
            "gold_review_artifact",
        },
        "silver_merge_key_review": {
            "silver_review_decision",
            "silver_review_artifact",
            "gate5",
            "gold_review_decision",
            "gold_review_artifact",
        },
        "gate5": {"gold_review_decision", "gold_review_artifact"},
        "gold_review": set(),
    }.get(boundary, set())

    for key in list(updated):
        if generation_first_snowflake_dbt_flow(state) and key.startswith("snowflake_dbt_"):
            updated.pop(key, None)
            continue
        if (
            generation_first_snowflake_dbt_flow(state)
            and boundary == "gate4"
            and key.startswith("snowflake_bronze_source_load_")
        ):
            updated.pop(key, None)
            continue
        if key in downstream_exact or any(key.startswith(prefix) for prefix in downstream_prefixes):
            updated.pop(key, None)
            continue
        if any(
            key.startswith(f"{target}_{layer}_execution")
            for target in ("databricks", "snowflake")
            for layer in execution_layers
        ):
            updated.pop(key, None)
            continue
        if any(
            key == f"{layer}_execution_status"
            or key.startswith(f"{layer}_execution_")
            for layer in execution_layers
        ):
            updated.pop(key, None)
            continue
        if key in {f"{layer}_runtime_validation_status" for layer in execution_layers}:
            updated.pop(key, None)

    for key in (
        "final_publish_status",
        "finalization_status",
        "completion_mode",
        "report_generation_status",
        "run_report",
    ):
        updated.pop(key, None)
    # Explicit nulls distinguish intentional downstream invalidation from a
    # stale checkpoint that merely omitted an already-approved review.
    updated.update({key: None for key in invalidated_review_fields})
    updated.update(
        {
            "execution_ready": False,
            "awaiting_stage_confirmation": False,
            "stage_confirmation": None,
            "next_stage_key": None,
            "next_stage_label": None,
            "next_review_key": None,
            "next_gate": None,
            "last_completed_stage_key": None,
            "last_completed_stage_label": None,
            "background_stage": None,
            "failed_background_stage": None,
            "external_execution": None,
            "error": None,
            "error_type": None,
            "error_message": None,
            "completed_at": None,
        }
    )
    return updated


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
    ("metadata_ddl", "Metadata DDL Generation"),
    ("bronze", "Bronze Generation"),
    ("silver", "Silver Generation"),
    ("gold", "Gold Generation"),
]

DATABASE_STAGE_LABELS = dict(DATABASE_STAGE_SEQUENCE)
FILE_SOURCE_STAGE_LABELS = {
    "ingestion": "BRD Ingest",
    "requirements": "Requirement Extraction",
    "kpis": "KPI Extraction",
    "discovery": "Discover Source Objects",
    "schema": "Schema Snapshot",
    "enrichment": "Semantic Enrichment",
}
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


def is_run_aborted(run_id: str, state: Optional[Dict[str, Any]] = None) -> bool:
    return (
        run_id in ABORTED_RUNS
        or bool((state or {}).get("abort_requested"))
        or str((state or {}).get("status") or "").upper() == "ABORTED"
    )


def aborted_run_state(run_id: str, state: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {
        **(state or {}),
        "run_id": run_id,
        "status": "ABORTED",
        "abort_requested": True,
        "background_stage": None,
        "next_gate": None,
        "next_review_key": None,
        "stage_confirmation": None,
        "awaiting_stage_confirmation": False,
        "resume_message": "Run stopped by user.",
    }


def clear_run_abort(run_id: str) -> None:
    if run_id:
        with BACKGROUND_JOB_LOCK:
            ABORTED_RUNS.discard(run_id)


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
    stage_labels = FILE_SOURCE_STAGE_LABELS if str(state.get("source") or "").lower() in {"sftp", "adls_gen2"} else DATABASE_STAGE_LABELS
    running_state = {
        **state,
        "status": "RUNNING",
        "background_stage": stage_key,
        "resume_message": f"{stage_labels.get(stage_key, stage_key).replace('_', ' ').title()} is running.",
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
        return _run_database_nomination_stage
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
    if stage_key == "metadata_ddl":
        return _run_database_metadata_ddl_stage
    if stage_key == "bronze":
        return _run_database_bronze_stage
    if stage_key == "silver":
        return _run_database_silver_stage
    if stage_key == "gold":
        return _run_database_gold_stage
    raise ValueError(f"Unsupported database stage: {stage_key}")


def _run_database_nomination_stage(state: Dict[str, Any]) -> Dict[str, Any]:
    from nodes.table_nomination import table_nomination_node
    from services.metadata_selection import validated_metadata_selection

    selection = validated_metadata_selection(state)
    if selection:
        configured_database = str(selection.connection.get("database_name") or "").strip()
        requested = [str(value).strip() for value in state.get("source_databases") or [] if str(value).strip()]
        if requested and {value.casefold() for value in requested} != {configured_database.casefold()}:
            raise ValueError("Requested source_databases do not match the selected active connection.")
        state = {
            **state,
            "source_databases": [configured_database],
            "source_connection_config_version": selection.connection.get("config_version"),
            "source_connection_config_hash": selection.connection.get("config_hash"),
        }
    return table_nomination_node(state)


def _run_database_metadata_ddl_stage(state: Dict[str, Any]) -> Dict[str, Any]:
    """Generate target DDL without opening a target connection."""
    from services.metadata_contracts import TargetMetadataContext, file_sha256, render_ddl
    from utilis.generated_code_paths import generated_artifact_uri, generated_run_dir

    platform = str(state.get("target_warehouse") or "").strip().lower()
    environment = str(state.get("target_environment") or "").strip()
    namespace_variable = {
        "databricks": "ATHENA_DATABRICKS_METADATA_CATALOG",
        "snowflake": "ATHENA_SNOWFLAKE_METADATA_DATABASE",
    }.get(platform)
    if not namespace_variable:
        raise ValueError(f"Unsupported metadata DDL target: {platform!r}")
    namespace = str(os.getenv(namespace_variable) or "").strip()
    if not namespace:
        raise RuntimeError(f"{namespace_variable} is required for metadata DDL generation.")

    metadata_schema = "metadata_schema" if platform == "databricks" else "metadata"
    context = TargetMetadataContext(
        platform=platform,
        environment=environment,
        namespace=namespace,
        schema=metadata_schema,
    )
    output_dir = generated_run_dir(platform, state.get("run_id"), "metadata")
    output_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = output_dir / "metadata_schema.sql"
    temporary_path = artifact_path.with_suffix(".sql.tmp")
    ddl_text = render_ddl(context)
    temporary_path.write_text(ddl_text, encoding="utf-8")
    temporary_path.replace(artifact_path)
    artifact = {
        "schema_version": "1.0",
        "platform": platform,
        "environment": environment,
        "namespace": namespace,
        "schema": metadata_schema,
        "artifact_uri": generated_artifact_uri(artifact_path),
        "artifact_hash": file_sha256(artifact_path),
    }
    return {
        **state,
        "status": "HITL_WAIT",
        "background_stage": None,
        "next_gate": None,
        "next_review_key": "metadata_ddl_review",
        "metadata_ddl_generation_status": "COMPLETED",
        "metadata_ddl_review_status": "PENDING",
        "metadata_ddl_artifact": artifact,
        "metadata_ddl_review": {
            "title": "Target Metadata Schema DDL",
            "file_name": artifact_path.name,
            "script_language": "sql",
            "script_body": ddl_text,
            "artifact_hash": artifact["artifact_hash"],
            "review_status": "PENDING",
        },
        "resume_message": "Metadata DDL Review is pending. Review the generated target schema before Bronze generation.",
    }


def _run_database_bronze_stage(state: Dict[str, Any]) -> Dict[str, Any]:
    from nodes.bronze_gen import bronze_code_generation_node

    result = bronze_code_generation_node(_mapping_driven_bronze_state(state))
    if state.get("source_system_id") is not None:
        result = _attach_bronze_execution_specs(result)
    if str(result.get("bronze_generation_status") or "").upper() == "COMPLETED":
        return {
            **result,
            "status": "HITL_WAIT",
            "next_gate": 4,
            "resume_message": "Bronze Review is pending. Review generated Bronze scripts before Silver generation.",
        }
    return result


def _attach_bronze_execution_specs(state: Dict[str, Any]) -> Dict[str, Any]:
    from pathlib import Path

    from services.metadata_contracts import file_sha256, validate_execution_spec
    from utilis.generated_code_paths import generated_artifact_uri

    certified_by_source = {
        f"{table.get('database_name')}.{table.get('schema_name')}.{table.get('table_name')}".casefold(): table
        for table in state.get("certified_tables") or []
    }
    platform = str(state.get("target_warehouse") or "").strip().lower()
    enriched_results = []
    for result in state.get("bronze_generation_results") or []:
        source_key = str(result.get("source_table") or "").casefold()
        certified = certified_by_source.get(source_key)
        if not certified:
            raise ValueError(f"Generated Bronze artifact has no certified ingestion object: {source_key}")
        path = Path(str(result.get("script_path") or ""))
        engine = (
            "SNOWFLAKE_DBT"
            if platform == "snowflake" and str(result.get("code_generation_format") or "").lower() == "dbt"
            else "SNOWFLAKE_SQL"
            if platform == "snowflake"
            else "DATABRICKS_JOB"
        )
        spec = validate_execution_spec(
            {
                "contract_version": "1.0",
                "execution_mode": "GENERATED_ARTIFACT",
                "target_platform": platform.upper(),
                "engine": engine,
                "artifact_uri": generated_artifact_uri(path),
                "entry_point": str(result.get("dbt_model_name") or "script"),
                "artifact_hash": file_sha256(path),
                "generator_version": "astra-codegen-1.0.0",
                "mapping_version": int(certified["source_to_bronze_mapping_version"]),
                "deployment_id": str(state.get("run_id") or ""),
                "connection_id": int(state["source_connection_id"]),
                "connection_config_version": int(state["source_connection_config_version"]),
                "connection_config_hash": str(state["source_connection_config_hash"]),
                "runtime_context_contract_version": "1.0",
                "idempotency_identity": "logical_work_id",
                "source_resource": {
                    "database": result.get("database_name"),
                    "schema": result.get("schema_name"),
                    "table": result.get("table"),
                },
                "landing_resource": (
                    {
                        "database": result.get("snowflake_landing_database"),
                        "schema": result.get("snowflake_landing_schema"),
                        "table": result.get("snowflake_landing_table"),
                    }
                    if platform == "snowflake"
                    else None
                ),
            },
            platform=platform,
        )
        enriched_results.append(
            {
                **result,
                "ingestion_object_id": int(certified["ingestion_object_id"]),
                "ingestion_object_config_version": int(certified["ingestion_object_config_version"]),
                "ingestion_object_config_hash": str(certified["ingestion_object_config_hash"]),
                "mapping_version": int(certified["source_to_bronze_mapping_version"]),
                "mapping_hash": str(certified["source_to_bronze_mapping_hash"]),
                "execution_spec": spec,
            }
        )
    return {**state, "bronze_generation_results": enriched_results}


def _register_and_activate_artifact_bundle(
    repository: Any, *, processing_stage: str, artifacts: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    bulk = getattr(repository, "register_and_activate_artifacts", None)
    if callable(bulk):
        return bulk(processing_stage=processing_stage, artifacts=artifacts)
    method_name = {
        "SOURCE_TO_BRONZE": "register_and_activate_source_to_bronze_artifact",
        "BRONZE_TO_SILVER": "register_and_activate_bronze_to_silver_artifact",
        "SILVER_TO_GOLD": "register_and_activate_silver_to_gold_artifact",
    }[processing_stage]
    single = getattr(repository, method_name)
    return [single(**artifact) for artifact in artifacts]


def _activate_reviewed_bronze_metadata(
    state: Dict[str, Any], *, activate_finalized_dbt: bool = False
) -> Dict[str, Any]:
    from services.metadata_selection import validated_metadata_selection

    if state.get("source_system_id") is None:
        return state
    selection = validated_metadata_selection(state)
    if not selection:
        raise ValueError("Bronze activation requires a valid target metadata selection.")
    generated_results = list(state.get("bronze_generation_results") or [])
    activatable = [
        result for result in generated_results
        if activate_finalized_dbt
        or str((result.get("execution_spec") or {}).get("engine") or "").upper() != "SNOWFLAKE_DBT"
    ]
    activated_by_object = {}
    if activatable:
        activated_by_object = {
            int(result["ingestion_object_id"]): activated
            for result, activated in zip(
                activatable,
                _register_and_activate_artifact_bundle(
                    selection.repository,
                    processing_stage="SOURCE_TO_BRONZE",
                    artifacts=[{
                        "draft_config_version": int(result["ingestion_object_config_version"]),
                        "ingestion_object_id": int(result["ingestion_object_id"]),
                        "mapping_version": int(result["mapping_version"]),
                        "mapping_hash": str(result["mapping_hash"]),
                        "execution_spec": result["execution_spec"],
                    } for result in activatable],
                ),
            )
        }
    activated_results = []
    active_versions: Dict[int, tuple[int, str]] = {}
    for result in generated_results:
        if (
            not activate_finalized_dbt
            and str((result.get("execution_spec") or {}).get("engine") or "").upper() == "SNOWFLAKE_DBT"
        ):
            activated_results.append({**result, "metadata_activation_status": "PENDING_FINAL_DBT_PACKAGE"})
            continue
        activated = activated_by_object[int(result["ingestion_object_id"])]
        active_object = activated["ingestion_object"]
        active_versions[int(result["ingestion_object_id"])] = (
            int(active_object["config_version"]),
            str(active_object["config_hash"]),
        )
        activated_results.append(
            {
                **result,
                "active_ingestion_object_config_version": int(active_object["config_version"]),
                "active_ingestion_object_config_hash": str(active_object["config_hash"]),
                "execution_spec": activated["execution_spec"],
                "metadata_activation_status": "ACTIVE",
            }
        )
    certified_tables = []
    for table in state.get("certified_tables") or []:
        active = active_versions.get(int(table.get("ingestion_object_id") or 0))
        certified_tables.append(
            {
                **table,
                **(
                    {
                        "active_ingestion_object_config_version": active[0],
                        "active_ingestion_object_config_hash": active[1],
                    }
                    if active
                    else {}
                ),
            }
        )
    return {**state, "bronze_generation_results": activated_results, "certified_tables": certified_tables}


def _attach_silver_execution_specs(state: Dict[str, Any]) -> Dict[str, Any]:
    from services.metadata_contracts import canonical_json_hash, file_sha256, validate_execution_spec
    from utilis.generated_code_paths import generated_artifact_uri

    platform = str(state.get("target_warehouse") or "").strip().lower()
    objects_by_id = {
        int(item.get("ingestion_object_id") or 0): item
        for item in state.get("silver_transformation_objects") or []
        if isinstance(item, dict)
    }
    review_by_object = {
        int(item.get("silver_ingestion_object_id") or 0): item
        for item in (state.get("silver_review_artifact") or {}).get("items") or []
        if isinstance(item, dict) and item.get("silver_ingestion_object_id") is not None
    }
    results = []
    for result in state.get("silver_generation_results") or []:
        object_id = int(result.get("silver_ingestion_object_id") or 0)
        transformation = objects_by_id.get(object_id) or {}
        validation_policy = json.loads(str(transformation.get("validation_policy_json") or "{}"))
        path = Path(str(result.get("script_path") or ""))
        reviewed = review_by_object.get(object_id) or {}
        reviewed_body = str(reviewed.get("generated_silver_script") or reviewed.get("script_body") or "")
        if reviewed_body and reviewed_body != path.read_text(encoding="utf-8"):
            raise ValueError(
                "Metadata-driven Silver review cannot replace executable code; update the approved mapping and regenerate."
            )
        engine = (
            "SNOWFLAKE_DBT"
            if platform == "snowflake" and str(result.get("code_generation_format") or "").lower() == "dbt"
            else "SNOWFLAKE_SQL"
            if platform == "snowflake"
            else "DATABRICKS_JOB"
        )
        spec = validate_execution_spec(
            {
                "contract_version": "1.0",
                "execution_mode": "GENERATED_ARTIFACT",
                "target_platform": platform.upper(),
                "engine": engine,
                "artifact_uri": generated_artifact_uri(path),
                "entry_point": str(result.get("dbt_model_name") or "script"),
                "artifact_hash": file_sha256(path),
                "generator_version": "astra-codegen-1.0.0",
                "mapping_version": int(result["bronze_to_silver_mapping_version"]),
                "deployment_id": str(state.get("run_id") or ""),
                "embedded_blocking_validation": True,
                "validation_policy_hash": canonical_json_hash(validation_policy),
            },
            platform=platform,
        )
        results.append({**result, "execution_spec": spec})
    return {**state, "silver_generation_results": results}


def _activate_reviewed_silver_metadata(
    state: Dict[str, Any], *, activate_finalized_dbt: bool = False
) -> Dict[str, Any]:
    from services.metadata_selection import validated_metadata_selection

    selection = validated_metadata_selection(state)
    if not selection:
        raise ValueError("Silver activation requires a valid target metadata selection.")
    generated_results = list(state.get("silver_generation_results") or [])
    activatable = [
        result for result in generated_results
        if activate_finalized_dbt
        or str((result.get("execution_spec") or {}).get("engine") or "").upper() != "SNOWFLAKE_DBT"
    ]
    activated_by_object = {}
    if activatable:
        activated_by_object = {
            int(result["silver_ingestion_object_id"]): activated
            for result, activated in zip(
                activatable,
                _register_and_activate_artifact_bundle(
                    selection.repository,
                    processing_stage="BRONZE_TO_SILVER",
                    artifacts=[{
                        "draft_config_version": int(result["silver_ingestion_object_config_version"]),
                        "ingestion_object_id": int(result["silver_ingestion_object_id"]),
                        "mapping_version": int(result["bronze_to_silver_mapping_version"]),
                        "mapping_hash": str(result["bronze_to_silver_mapping_hash"]),
                        "execution_spec": result["execution_spec"],
                    } for result in activatable],
                ),
            )
        }
    active_versions: Dict[int, tuple[int, str]] = {}
    results = []
    for result in generated_results:
        if (
            not activate_finalized_dbt
            and str((result.get("execution_spec") or {}).get("engine") or "").upper() == "SNOWFLAKE_DBT"
        ):
            results.append({**result, "metadata_activation_status": "PENDING_FINAL_DBT_PACKAGE"})
            continue
        activated = activated_by_object[int(result["silver_ingestion_object_id"])]
        active_object = activated["ingestion_object"]
        object_id = int(result["silver_ingestion_object_id"])
        active_versions[object_id] = (
            int(active_object["config_version"]),
            str(active_object["config_hash"]),
        )
        results.append({
            **result,
            "active_silver_ingestion_object_config_version": int(active_object["config_version"]),
            "active_silver_ingestion_object_config_hash": str(active_object["config_hash"]),
            "execution_spec": activated["execution_spec"],
            "metadata_activation_status": "ACTIVE",
        })
    objects = [
        {
            **item,
            **(
                {
                    "active_config_version": active_versions[int(item["ingestion_object_id"])][0],
                    "active_config_hash": active_versions[int(item["ingestion_object_id"])][1],
                }
                if int(item.get("ingestion_object_id") or 0) in active_versions
                else {}
            ),
        }
        for item in state.get("silver_transformation_objects") or []
        if isinstance(item, dict)
    ]
    return {**state, "silver_generation_results": results, "silver_transformation_objects": objects}


def _attach_gold_execution_specs(state: Dict[str, Any]) -> Dict[str, Any]:
    from services.metadata_contracts import canonical_json_hash, file_sha256, validate_execution_spec
    from utilis.generated_code_paths import generated_artifact_uri

    platform = str(state.get("target_warehouse") or "").strip().lower()
    review_by_object = {
        int(item.get("gold_ingestion_object_id") or 0): item
        for item in (state.get("gold_review_artifact") or {}).get("items") or []
        if isinstance(item, dict) and item.get("gold_ingestion_object_id") is not None
    }
    results = []
    objects_by_id = {
        int(item.get("ingestion_object_id") or 0): item
        for item in state.get("gold_transformation_objects") or []
        if isinstance(item, dict)
    }
    for result in state.get("gold_generation_results") or []:
        object_id = int(result.get("gold_ingestion_object_id") or 0)
        transformation = objects_by_id.get(object_id) or {}
        validation_policy = json.loads(str(transformation.get("validation_policy_json") or "{}"))
        path = Path(str(result.get("script_path") or ""))
        reviewed = review_by_object.get(object_id) or {}
        reviewed_body = str(reviewed.get("generated_gold_script") or reviewed.get("script_body") or "")
        if (
            reviewed_body
            and str(result.get("code_generation_format") or "").lower() != "dbt"
            and reviewed_body != path.read_text(encoding="utf-8")
        ):
            raise ValueError(
                "Metadata-driven Gold review cannot replace executable code; update the approved mapping and regenerate."
            )
        engine = (
            "SNOWFLAKE_DBT"
            if platform == "snowflake" and str(result.get("code_generation_format") or "").lower() == "dbt"
            else "SNOWFLAKE_SQL"
            if platform == "snowflake"
            else "DATABRICKS_JOB"
        )
        spec = validate_execution_spec(
            {
                "contract_version": "1.0",
                "execution_mode": "GENERATED_ARTIFACT",
                "target_platform": platform.upper(),
                "engine": engine,
                "artifact_uri": generated_artifact_uri(path),
                "entry_point": str(result.get("dbt_model_name") or "script"),
                "artifact_hash": file_sha256(path),
                "generator_version": "astra-codegen-1.0.0",
                "mapping_version": int(result["silver_to_gold_mapping_version"]),
                "deployment_id": str(state.get("run_id") or ""),
                "embedded_blocking_validation": True,
                "validation_policy_hash": canonical_json_hash(validation_policy),
            },
            platform=platform,
        )
        results.append({**result, "execution_spec": spec})
    return {**state, "gold_generation_results": results}


def _activate_reviewed_gold_metadata(
    state: Dict[str, Any], *, activate_finalized_dbt: bool = False
) -> Dict[str, Any]:
    from services.metadata_selection import validated_metadata_selection

    selection = validated_metadata_selection(state)
    if not selection:
        raise ValueError("Gold activation requires a valid target metadata selection.")
    generated_results = list(state.get("gold_generation_results") or [])
    activatable = [
        result for result in generated_results
        if activate_finalized_dbt
        or str((result.get("execution_spec") or {}).get("engine") or "").upper() != "SNOWFLAKE_DBT"
    ]
    activated_by_object = {
        int(result["gold_ingestion_object_id"]): activated
        for result, activated in zip(
            activatable,
            _register_and_activate_artifact_bundle(
                selection.repository,
                processing_stage="SILVER_TO_GOLD",
                artifacts=[{
                    "draft_config_version": int(result["gold_ingestion_object_config_version"]),
                    "ingestion_object_id": int(result["gold_ingestion_object_id"]),
                    "mapping_version": int(result["silver_to_gold_mapping_version"]),
                    "mapping_hash": str(result["silver_to_gold_mapping_hash"]),
                    "execution_spec": result["execution_spec"],
                } for result in activatable],
            ) if activatable else [],
        )
    }
    active_versions: Dict[int, tuple[int, str]] = {}
    results = []
    for result in generated_results:
        if (
            not activate_finalized_dbt
            and str((result.get("execution_spec") or {}).get("engine") or "").upper() == "SNOWFLAKE_DBT"
        ):
            results.append({**result, "metadata_activation_status": "PENDING_FINAL_DBT_PACKAGE"})
            continue
        activated = activated_by_object[int(result["gold_ingestion_object_id"])]
        active_object = activated["ingestion_object"]
        object_id = int(result["gold_ingestion_object_id"])
        active_versions[object_id] = (int(active_object["config_version"]), str(active_object["config_hash"]))
        results.append({
            **result,
            "active_gold_ingestion_object_config_version": int(active_object["config_version"]),
            "active_gold_ingestion_object_config_hash": str(active_object["config_hash"]),
            "execution_spec": activated["execution_spec"],
            "metadata_activation_status": "ACTIVE",
        })
    objects = [
        {
            **item,
            **(
                {
                    "active_config_version": active_versions[int(item["ingestion_object_id"])][0],
                    "active_config_hash": active_versions[int(item["ingestion_object_id"])][1],
                }
                if int(item.get("ingestion_object_id") or 0) in active_versions
                else {}
            ),
        }
        for item in state.get("gold_transformation_objects") or []
        if isinstance(item, dict)
    ]
    return {**state, "gold_generation_results": results, "gold_transformation_objects": objects}


def _activate_finalized_snowflake_dbt_metadata(state: Dict[str, Any]) -> Dict[str, Any]:
    """Bind one reviewed dbt package to its exact model drafts before activation."""
    from services.metadata_contracts import validate_execution_spec

    if not snowflake_dbt_enabled(state):
        return state
    package_hash = str(state.get("snowflake_dbt_artifact_set_hash") or "").strip()
    package_id = str(state.get("snowflake_dbt_idempotency_key") or "").strip()
    if not re.fullmatch(r"[0-9a-f]{64}", package_hash) or not package_id:
        raise RuntimeError("The finalized Snowflake dbt package identity is missing or invalid.")
    if str(state.get("snowflake_dbt_validation_status") or "").upper() != "STATIC_VALIDATED":
        raise RuntimeError("Snowflake dbt metadata cannot activate before static project validation passes.")

    bound = dict(state)
    for layer in ("bronze", "silver"):
        key = f"{layer}_generation_results"
        results = []
        for result in bound.get(key) or []:
            if not isinstance(result, dict):
                continue
            spec = dict(result.get("execution_spec") or {})
            if str(spec.get("engine") or "").upper() != "SNOWFLAKE_DBT":
                raise RuntimeError(f"The finalized dbt package contains a non-dbt {layer} artifact.")
            spec.update({"dbt_package_hash": package_hash, "dbt_package_id": package_id})
            results.append({**result, "execution_spec": validate_execution_spec(spec, platform="snowflake")})
        bound[key] = results

    bound = _activate_reviewed_bronze_metadata(bound, activate_finalized_dbt=True)
    bound = _activate_reviewed_silver_metadata(bound, activate_finalized_dbt=True)
    refreshed = _materialize_silver_to_gold_metadata(bound)
    drafts_by_target = {
        str(item.get("target_table") or "").casefold(): item
        for item in refreshed.get("gold_metadata_drafts") or []
        if isinstance(item, dict)
    }
    refreshed_results = []
    for result in bound.get("gold_generation_results") or []:
        target = str(result.get("target_table") or "").casefold()
        draft = drafts_by_target.get(target)
        if not draft:
            raise RuntimeError(f"The finalized dbt package has no refreshed Gold metadata for {target}.")
        refreshed_results.append({**result, **draft})
    bound = {
        **refreshed,
        "gold_generation_results": refreshed_results,
    }
    bound = _attach_gold_execution_specs(bound)
    bound["gold_generation_results"] = [{
        **result,
        "execution_spec": validate_execution_spec(
            {
                **dict(result.get("execution_spec") or {}),
                "dbt_package_hash": package_hash,
                "dbt_package_id": package_id,
            },
            platform="snowflake",
        ),
    } for result in bound.get("gold_generation_results") or []]
    return _activate_reviewed_gold_metadata(bound, activate_finalized_dbt=True)


def _mapping_driven_bronze_state(
    state: Dict[str, Any], *, _selection: Any = None
) -> Dict[str, Any]:
    if state.get("source_system_id") is None:
        return state
    from services.metadata_contracts import validate_identifier
    from services.metadata_selection import validated_metadata_selection

    selection = _selection or validated_metadata_selection(state)
    if not selection:
        raise ValueError("Metadata-enabled Bronze generation requires a valid target metadata selection.")
    if _selection is None and callable(getattr(selection.repository, "unit_of_work", None)):
        with selection.repository.unit_of_work():
            return _mapping_driven_bronze_state(state, _selection=selection)
    certified_tables = state.get("certified_tables") or []
    discovered = dict(state.get("discovered_metadata") or {})
    discovered_by_object = {
        int(table["ingestion_object_id"]): table
        for table in discovered.get("tables") or []
        if table.get("ingestion_object_id") is not None
    }
    mapped_discovery = []
    loaded_bundles = []
    pinned_target_namespace: Optional[tuple[str, str]] = None
    object_refs = [
        {
            "ingestion_object_id": int(item.get("ingestion_object_id") or 0),
            "config_version": int(item.get("ingestion_object_config_version") or 0),
        }
        for item in certified_tables
    ]
    load_objects = getattr(selection.repository, "get_ingestion_objects", None)
    objects = load_objects(object_refs) if callable(load_objects) else {
        (item["ingestion_object_id"], item["config_version"]): selection.repository.get_ingestion_object(
            item["ingestion_object_id"], item["config_version"]
        )
        for item in object_refs
    }
    bundle_refs = []
    for certified in certified_tables:
        object_id = int(certified.get("ingestion_object_id") or 0)
        config_version = int(certified.get("ingestion_object_config_version") or 0)
        ingestion_object = objects.get((object_id, config_version))
        if not ingestion_object:
            raise ValueError(f"Missing ingestion-object draft for object {object_id}/{config_version}.")
        bundle_refs.append({
            "ingestion_object_id": object_id,
            "processing_stage": "SOURCE_TO_BRONZE",
            "mapping_version": int(certified.get("source_to_bronze_mapping_version") or 0),
            "expected_hash": str(certified.get("source_to_bronze_mapping_hash") or ""),
            "expected_target": str(ingestion_object.get("target_bronze_table") or "").strip(),
            "require_active": None,
        })
    load_bundles = getattr(selection.repository, "get_mapping_bundles", None)
    bundles = load_bundles(bundle_refs) if callable(load_bundles) else {}
    for certified in certified_tables:
        object_id = int(certified.get("ingestion_object_id") or 0)
        config_version = int(certified.get("ingestion_object_config_version") or 0)
        ingestion_object = objects.get((object_id, config_version))
        if not ingestion_object:
            raise ValueError(f"Missing ingestion-object draft for object {object_id}/{config_version}.")
        if str(ingestion_object.get("config_hash") or "") != str(
            certified.get("ingestion_object_config_hash") or ""
        ):
            raise ValueError(f"Ingestion-object configuration hash mismatch for object {object_id}.")
        target_table = str(ingestion_object.get("target_bronze_table") or "").strip()
        mapping_version = int(certified.get("source_to_bronze_mapping_version") or 0)
        bundle = bundles.get((object_id, "SOURCE_TO_BRONZE", mapping_version)) or selection.repository.get_mapping_bundle(
                ingestion_object_id=object_id,
                processing_stage="SOURCE_TO_BRONZE",
                mapping_version=mapping_version,
                expected_hash=str(certified.get("source_to_bronze_mapping_hash") or ""),
                expected_target=target_table,
                require_active=None,
            )
        loaded_bundles.append(bundle)
        source_table = discovered_by_object.get(object_id)
        if not source_table:
            raise ValueError(f"Missing approved Source-to-Bronze mapping context for object {object_id}.")
        target_parts = [validate_identifier(part, label="Bronze target identifier") for part in target_table.split(".")]
        if len(target_parts) != 3 or target_parts[2].casefold() != f"bronze_{source_table.get('table_name') or ''}".casefold():
            raise ValueError(f"Unsupported pinned Bronze target for object {object_id}: {target_table!r}")
        target_namespace = (target_parts[0], target_parts[1])
        if pinned_target_namespace and tuple(part.casefold() for part in pinned_target_namespace) != tuple(
            part.casefold() for part in target_namespace
        ):
            raise ValueError("One Bronze generation batch cannot span multiple target catalog/schema pairs.")
        pinned_target_namespace = target_namespace
        original_columns = {
            str(column.get("column_name") or "").casefold(): column
            for column in source_table.get("columns") or []
        }
        mapped_columns = []
        for mapping in sorted(bundle.get("mappings") or [], key=lambda item: int(item.get("ordinal_position") or 0)):
            source_name = str(mapping.get("source_field_path") or "")
            original = original_columns.get(source_name.casefold())
            if not original:
                raise ValueError(f"Mapped source column was not discovered: {source_name}")
            mapped_columns.append(
                {
                    **original,
                    "ordinal_position": mapping.get("ordinal_position"),
                    "bronze_target_name": mapping.get("target_column_name"),
                    "bronze_target_type": mapping.get("target_data_type"),
                }
            )
        mapped_discovery.append({**source_table, "target_bronze_table": target_table, "columns": mapped_columns})
    if not pinned_target_namespace:
        raise ValueError("Metadata-enabled Bronze generation requires at least one pinned target.")
    return {
        **state,
        "bronze_catalog": pinned_target_namespace[0],
        "bronze_schema": pinned_target_namespace[1],
        "source_to_bronze_mapping_bundles": loaded_bundles,
        "source_runtime_connection": {
            "host_name": selection.connection.get("host_name"),
            "port": selection.connection.get("port"),
            "database_name": selection.connection.get("database_name"),
            "secrets": json.loads(str(selection.connection.get("secrets_json") or "{}")),
            "config": json.loads(str(selection.connection.get("config_json") or "{}")),
            "config_version": selection.connection.get("config_version"),
            "config_hash": selection.connection.get("config_hash"),
        },
        "discovered_metadata": {**discovered, "tables": mapped_discovery},
    }


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
        if is_run_aborted(run_id, working_state):
            return aborted_run_state(run_id, working_state)
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
        if is_run_aborted(run_id):
            return aborted_run_state(run_id, working_state)

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
    persisted_state = redact_sensitive(dict(state))
    now = datetime.now(timezone.utc).isoformat()
    persisted_state["updated_at"] = now
    normalized_status = str(persisted_state.get("status") or "").upper()
    if normalized_status in {"SUCCESS", "COMPLETED", "PIPELINE_COMPLETED", "FAILED", "ABORTED", "CANCELLED", "CANCELED"}:
        persisted_state["completed_at"] = persisted_state.get("completed_at") or now

    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT TOP 1 full_state_json
            FROM [{_pipeline_schema()}].[kpi_checkpoints] WITH (UPDLOCK, HOLDLOCK)
            WHERE run_id = ?
            """,
            (run_id,),
        )
        current = cursor.fetchone()
        if current and current[0]:
            current_state = json.loads(current[0])
            for field in REVIEW_CHECKPOINT_FIELDS:
                if field not in persisted_state and field in current_state:
                    persisted_state[field] = current_state[field]
        state_json = json.dumps(persisted_state, default=str)
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


def abort_background_run(run_id: str, checkpoint: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    aborted = aborted_run_state(run_id, checkpoint or load_checkpoint_state(run_id) or {})
    aborted["aborted_at"] = time.time()
    with BACKGROUND_JOB_LOCK:
        ABORTED_RUNS.add(run_id)
        futures = [
            future
            for job_key, future in BACKGROUND_JOBS.items()
            if job_key == run_id or job_key.startswith(f"{run_id}:")
        ]
        save_checkpoint_state(run_id, aborted)

    cancelled = sum(1 for future in futures if future.cancel())
    logger.warning(
        "Pipeline abort requested",
        extra={"run_id": run_id, "jobs_found": len(futures), "jobs_cancelled": cancelled},
    )
    return aborted


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
    failed_stage = (
        state.get("background_stage")
        or state.get("failed_background_stage")
        or state.get("last_failed_stage_key")
        or state.get("next_stage_key")
        or "pipeline"
    )
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
    if is_run_aborted(run_id, checkpoint):
        raise HTTPException(status_code=409, detail="Run has been aborted.")
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
        if is_run_aborted(run_id):
            raise HTTPException(status_code=409, detail="Run has been aborted.")
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
            with BACKGROUND_JOB_LOCK:
                checkpoint = load_checkpoint_state(run_id) or {"run_id": run_id}
                if is_run_aborted(run_id, checkpoint):
                    save_checkpoint_state_timed(
                        run_id,
                        aborted_run_state(run_id, checkpoint),
                        context=f"{stage}:background_aborted",
                    )
                else:
                    if isinstance(result, dict):
                        checkpoint.update(result)
                    checkpoint.update({"run_id": run_id, "background_stage": None})
                    if checkpoint.get("status") == "PROCESSING":
                        checkpoint["status"] = "RUNNING"
                    save_checkpoint_state_timed(run_id, checkpoint, context=f"{stage}:background_complete")
        except Exception as exc:
            logger.exception("Background %s failed for run_id=%s", stage, run_id)
            with BACKGROUND_JOB_LOCK:
                try:
                    checkpoint = load_checkpoint_state(run_id) or {"run_id": run_id}
                except Exception:
                    logger.exception("Failed to load checkpoint while recording background failure run_id=%s stage=%s", run_id, stage)
                    checkpoint = {"run_id": run_id}
                if is_run_aborted(run_id, checkpoint):
                    save_checkpoint_state_timed(
                        run_id,
                        aborted_run_state(run_id, checkpoint),
                        context=f"{stage}:background_aborted",
                    )
                else:
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


def list_runs(
    limit: int = 50,
    *,
    owner_email: Optional[str] = None,
    project_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    safe_limit = max(1, min(1000, int(limit or 50)))
    filters: List[str] = []
    parameters: List[str] = []
    if owner_email:
        filters.append(
            """LOWER(COALESCE(
                NULLIF(JSON_VALUE(full_state_json, '$.owner_email'), ''),
                NULLIF(JSON_VALUE(full_state_json, '$.created_by_email'), ''),
                NULLIF(JSON_VALUE(full_state_json, '$.submitted_by_email'), ''),
                NULLIF(JSON_VALUE(full_state_json, '$.user_email'), '')
            )) = ?"""
        )
        parameters.append(str(owner_email).strip().lower())
    if project_id:
        filters.append("JSON_VALUE(full_state_json, '$.project_id') = ?")
        parameters.append(str(project_id).strip())
    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    # Run history must not scan the large checkpoint JSON payload table just to
    # discover run IDs.  The ingestion pipeline already maintains the compact
    # run registry for this purpose (the same split used by the Athena app).
    # Checkpoint JSON is projected only for the small set of selected run IDs.
    if filters:
        index_query = f"""
            SELECT TOP ({safe_limit}) registry.run_id, registry.[timestamp] AS last_activity
            FROM [{_pipeline_schema()}].[brd_run_registry] AS registry WITH (READUNCOMMITTED)
            INNER JOIN [{_pipeline_schema()}].[kpi_checkpoints] AS cp WITH (READUNCOMMITTED)
                ON cp.run_id = registry.run_id
            WHERE {' AND '.join(condition.replace('full_state_json', 'cp.full_state_json') for condition in filters)}
            ORDER BY registry.[timestamp] DESC
        """
    else:
        index_query = f"""
            SELECT TOP ({safe_limit}) run_id, [timestamp] AS last_activity
            FROM [{_pipeline_schema()}].[brd_run_registry] WITH (READUNCOMMITTED)
            ORDER BY [timestamp] DESC
        """

    conn = get_connection()
    try:
        cursor = conn.cursor()
        try:
            cursor.timeout = max(1, int(os.getenv("ATHENA_SQL_QUERY_TIMEOUT_SECONDS", "5")))
        except Exception:
            # Some drivers may not expose cursor timeout; ignore.
            pass
        cursor.execute(index_query, *parameters)
        index_rows = cursor.fetchall()
        base_rows = []
        for row in index_rows:
            if not row or not row[0]:
                continue
            base_rows.append({
                "run_id": row[0],
                "last_activity": row[1],
                "checkpoint": {},
            })
        base_rows.sort(
            key=lambda row: str(row.get("last_activity") or ""),
            reverse=True,
        )
        if not base_rows:
            return []

        return [
            {
                **row,
                "checkpoint": row.get("checkpoint") or {},
            }
            for row in base_rows
        ]
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


def _is_dbt_codegen_script(item: Dict[str, Any]) -> bool:
    return str(item.get("code_generation_format") or "").strip().lower() == "dbt"


def _without_dbt_dimension_fields(item: Dict[str, Any]) -> Dict[str, Any]:
    if not _is_dbt_codegen_script(item):
        return item
    row = dict(item)
    row.pop("dimension_script_path", None)
    row.pop("dimension_script_body", None)
    row.pop("dimension_body", None)
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
        dimension_script_body = (
            ""
            if result_key == "gold_generation_results" and _is_dbt_codegen_script(item)
            else _read_script_body(item.get("dimension_script_path"))
        )
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
        if result_key == "gold_generation_results":
            row = _without_dbt_dimension_fields(row)
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
        dimension_script_body = (
            "" if _is_dbt_codegen_script(item) else _read_script_body(item.get("dimension_script_path"))
        )
        if not _script_matches_run(
            item=item,
            bundle_run_id=bundle_run_id,
            requested_run_id=run_id,
            script_bodies=[script_body, dimension_script_body],
        ):
            continue
        scripts.append(
            _without_dbt_dimension_fields({
                **item,
                "script_body": script_body,
                "dimension_script_body": dimension_script_body,
            })
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
    dbt_codegen = source == "database" and snowflake_dbt_enabled(checkpoint)
    dbt_deploy = dbt_codegen and str(checkpoint.get("dbt_deployment_mode") or "").lower() == "generate_and_deploy"

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
                "key": "memory",
                "label": "Memory Check",
                "complete": checkpoint.get("memory_lookup_status") == "COMPLETED",
                "detail": "Exact and semantic BRD memory checked",
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
                "label": "Discover Source Objects",
                "complete": bool(
                    checkpoint.get("source_ingestion_status") == "COMPLETED"
                    or checkpoint.get("candidate_feed")
                    or checkpoint.get("candidate_feeds")
                ),
                "detail": f"{source_label} source scanned and candidate feeds identified",
            },
            {
                "key": "nomination",
                "label": "Feed Nomination",
                "complete": bool(
                    checkpoint.get("table_nomination_status") == "COMPLETED"
                    or checkpoint.get("nominated_tables")
                ),
                "detail": "Approved feed candidates nominated for metadata extraction",
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
                "key": "pre_bronze_bootstrap_metadata",
                "label": "Bootstrap Metadata",
                "complete": checkpoint.get("metadata_bootstrap_status") == "COMPLETED",
                "detail": "Approved feed schemas loaded into the metadata manifest",
            },
            {
                "key": "plan_seal",
                "label": "Seal Approved Plan",
                "complete": checkpoint.get("plan_seal_status") == "COMPLETED",
                "detail": "Approved KPI, feed, schema, and enrichment inputs sealed",
            },
            {
                "key": "plan_freshness",
                "label": "Validate Plan Freshness",
                "complete": checkpoint.get("freshness_check_status") == "COMPLETED",
                "detail": "Current backend plan checked against its approved seal",
            },
            {
                "key": "pre_bronze_metadata_codegen",
                "label": "Metadata Code Generation",
                "complete": checkpoint.get("metadata_codegen_status") == "COMPLETED",
                "detail": "Canonical feed metadata and runtime manifest generated",
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
                    str(checkpoint.get("target_warehouse") or "").lower() == "snowflake"
                    and checkpoint.get("snowflake_bronze_execution_status") == "COMPLETED"
                ) or bool(
                    str(checkpoint.get("target_warehouse") or "").lower() == "databricks"
                    and checkpoint.get("databricks_bronze_execution_status") == "COMPLETED"
                ),
                "detail": "Approved Bronze code executed by the selected target runtime",
            },
            {
                "key": "bronze_runtime_validation",
                "label": "Bronze Runtime Validation",
                "complete": checkpoint.get("bronze_runtime_validation_status") == "COMPLETED"
                or checkpoint.get("snowflake_bronze_execution_status") == "COMPLETED"
                or checkpoint.get("databricks_bronze_execution_status") == "COMPLETED",
                "detail": "Target execution completion validated; full Bronze DQ remains a future runtime step",
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
                "label": "Silver Code Review",
                "complete": gate5_decision == "APPROVED",
                "detail": "Silver code reviewed before target execution",
            },
            {
                "key": "silver_code_execution",
                "label": "Silver Code Execution",
                "complete": bool(
                    str(checkpoint.get("target_warehouse") or "").lower() == "snowflake"
                    and checkpoint.get("snowflake_silver_execution_status") in SNOWFLAKE_COMPLETED_EXECUTION_STATUSES
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
                "key": "silver_runtime_validation",
                "label": "Silver Runtime Validation",
                "complete": checkpoint.get("silver_runtime_validation_status") == "COMPLETED"
                or checkpoint.get("snowflake_silver_execution_status") == "COMPLETED"
                or checkpoint.get("databricks_silver_execution_status") == "COMPLETED",
                "detail": "Target execution completion validated; full Silver DQ remains a future runtime step",
            },
            {
                "key": "gold",
                "label": "Gold Code Generation",
                "complete": gold_generation_completed,
                "detail": "Gold KPI generation completed",
            },
            {
                "key": "gold_review",
                "label": "Gold Code Review",
                "complete": str(checkpoint.get("gold_review_decision") or "").upper() == "APPROVED",
                "detail": "Generated Gold scripts approved for target execution",
            },
            {
                "key": "gold_code_execution",
                "label": "Gold Code Execution",
                "complete": bool(
                    str(checkpoint.get("target_warehouse") or "").lower() == "snowflake"
                    and checkpoint.get("snowflake_gold_execution_status") in SNOWFLAKE_COMPLETED_EXECUTION_STATUSES
                ) or bool(
                    str(checkpoint.get("target_warehouse") or "").lower() == "databricks"
                    and _status_completed(checkpoint.get("databricks_gold_execution_status"))
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
            {
                "key": "gold_runtime_validation",
                "label": "Gold Runtime Validation",
                "complete": checkpoint.get("gold_runtime_validation_status") == "COMPLETED"
                or _status_completed(checkpoint.get("snowflake_gold_execution_status"))
                or _status_completed(checkpoint.get("databricks_gold_execution_status")),
                "detail": "Target execution completion validated; full Gold DQ remains a future runtime step",
            },
            {
                "key": "final_publish",
                "label": "Final Publish (Target Gate 5)",
                "complete": checkpoint.get("final_publish_status") == "COMPLETED"
                or _status_completed(checkpoint.get("snowflake_gold_execution_status"))
                or _status_completed(checkpoint.get("databricks_gold_execution_status")),
                "detail": "Reviewed Gold outputs reached the selected target",
            },
            {
                "key": "finalize",
                "label": "Finalize Run",
                "complete": checkpoint.get("finalization_status") == "COMPLETED"
                or str(checkpoint.get("status") or "").upper() == "PIPELINE_COMPLETED",
                "detail": "Run closed after reviewed Gold code executes successfully",
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
            "complete": bool("DISCOVERED_METADATA" in artifact_types or _status_completed(checkpoint.get("metadata_status"))),
            "detail": "Table metadata discovered",
        },
        {
            "key": "profiling",
            "label": "Column Profiling",
            "complete": bool("COLUMN_PROFILES" in artifact_types or _status_completed(checkpoint.get("column_profiling_status"))),
            "detail": "Column profiles generated",
        },
        {
            "key": "enrichment",
            "label": "Semantic Enrichment",
            "complete": bool("ENRICHED_METADATA" in artifact_types or enriched_payload or _status_completed(checkpoint.get("semantic_enrichment_status"))),
            "detail": "Semantic metadata enriched",
        },
        {
            "key": "gate3",
            "label": _gate_label(3, source=source),
            "complete": bool("GATE3_APPROVED_ENRICHMENT" in artifact_types or gate3_payload),
            "detail": "Human enrichment approval",
        },
        {
            "key": "metadata_ddl",
            "label": "Metadata DDL Generation",
            "complete": checkpoint.get("metadata_ddl_generation_status") == "COMPLETED",
            "detail": "Target-specific metadata schema DDL generated without target access",
        },
        {
            "key": "metadata_ddl_review",
            "label": "Metadata DDL Review",
            "complete": checkpoint.get("metadata_ddl_review_status") == "COMPLETED",
            "detail": "Generated target metadata schema reviewed before Bronze generation",
        },
        {
            "key": "bronze",
            "label": "Bronze dbt Model Generation" if dbt_codegen else "Bronze Code Generation",
            "complete": bool(bronze_generation_completed),
            "detail": "Bronze dbt models generated" if dbt_codegen else "Bronze scripts generated",
        },
        {
            "key": "gate4",
            "label": _gate_label(4, source=source),
            "complete": bool(gate4_decision == "APPROVED"),
            "detail": "Bronze review and merge-key resolution",
        },
            {
                "key": "bronze_code_execution",
                "label": (
                    "Bronze dbt Models Staged"
                    if dbt_deploy
                    else "Bronze dbt Models Ready"
                    if dbt_codegen
                    else "Bronze Code Execution"
                ),
                "complete": bool(
                    str(checkpoint.get("target_warehouse") or "").lower() == "snowflake"
                    and checkpoint.get("snowflake_bronze_execution_status") in SNOWFLAKE_COMPLETED_EXECUTION_STATUSES
                ) or bool(
                    str(checkpoint.get("target_warehouse") or "").lower() == "databricks"
                    and checkpoint.get("databricks_bronze_execution_status") == "COMPLETED"
                ),
                "detail": (
                    "Bronze dbt models were added to the combined project and will deploy with the final dbt build."
                    if dbt_deploy
                    else "Bronze dbt models were added to the generated project."
                    if dbt_codegen
                    else "Approved Bronze scripts are executed in Snowflake before Silver generation."
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
            "label": "Silver dbt Model Generation" if dbt_codegen else "Silver Code Generation",
            "complete": bool(silver_generation_completed),
            "detail": "Silver dbt models generated" if dbt_codegen else "Silver transformation scripts generated",
        },
        {
            "key": "gate5",
            "label": _gate_label(5, source=source),
            "complete": bool((checkpoint.get("gate5") or {}).get("decision") == "APPROVED" or checkpoint.get("silver_review_decision") == "APPROVED"),
            "detail": "Silver review",
        },
            {
                "key": "silver_code_execution",
                "label": (
                    "Silver dbt Models Staged"
                    if dbt_deploy
                    else "Silver dbt Models Ready"
                    if dbt_codegen
                    else "Silver Code Execution"
                ),
                "complete": bool(
                    str(checkpoint.get("target_warehouse") or "").lower() == "snowflake"
                    and checkpoint.get("snowflake_silver_execution_status") in SNOWFLAKE_COMPLETED_EXECUTION_STATUSES
                ) or bool(
                    str(checkpoint.get("target_warehouse") or "").lower() == "databricks"
                    and checkpoint.get("databricks_silver_execution_status") == "COMPLETED"
                ),
                "detail": (
                    "Silver dbt models were added to the combined project and will deploy with the final dbt build."
                    if dbt_deploy
                    else "Silver dbt models were added to the generated project."
                    if dbt_codegen
                    else "Approved Silver scripts are executed in Snowflake before Gold generation."
                    if str(checkpoint.get("target_warehouse") or "").lower() == "snowflake"
                    else "Approved Silver scripts are executed in Databricks before Gold generation."
                    if str(checkpoint.get("target_warehouse") or "").lower() == "databricks"
                    else "UI-only execution marker; generated Silver code runs outside Athena"
                ),
            },
        {
            "key": "gold",
            "label": "Gold dbt Model Generation" if dbt_codegen else "Gold Code Generation",
            "complete": bool(gold_generation_completed),
            "detail": "Gold dbt KPI models generated" if dbt_codegen else "Gold KPI scripts generated",
        },
            {
                "key": "gold_code_execution",
                "label": (
                    "dbt Project Build & Deployment"
                    if dbt_deploy
                    else "dbt Static Dependency Check"
                    if dbt_codegen
                    else "Gold Code Execution"
                ),
                "complete": bool(
                    str(checkpoint.get("target_warehouse") or "").lower() == "snowflake"
                    and checkpoint.get("snowflake_gold_execution_status") in SNOWFLAKE_COMPLETED_EXECUTION_STATUSES
                ) or bool(
                    str(checkpoint.get("target_warehouse") or "").lower() == "databricks"
                    and _status_completed(checkpoint.get("databricks_gold_execution_status"))
                ),
                "detail": (
                    "The combined Bronze, Silver, and Gold dbt project is validated, deployed, and built in Snowflake."
                    if dbt_deploy
                    else "Bronze, Silver, and Gold dbt model dependencies were checked and packaged."
                    if dbt_codegen
                    else "Generated Gold scripts are executed in Snowflake after Gold generation."
                    if str(checkpoint.get("target_warehouse") or "").lower() == "snowflake"
                    else "Generated Gold scripts are executed in Databricks after Gold generation."
                    if str(checkpoint.get("target_warehouse") or "").lower() == "databricks"
                    else "UI-only execution marker; generated Gold code runs outside Athena"
                ),
            },
        ]
        if generation_first_database_flow(checkpoint):
            execution_steps = {
                step["key"]: step
                for step in steps
                if step.get("key") in {
                    "bronze_code_execution",
                    "silver_code_execution",
                    "gold_code_execution",
                }
            }
            steps = [
                step
                for step in steps
                if step.get("key") not in execution_steps
            ]
            steps.append(
                {
                    "key": "gold_review",
                    "label": "Gold Code Review",
                    "complete": str(checkpoint.get("gold_review_decision") or "").upper() == "APPROVED",
                    "detail": (
                        "Generated dbt project approved before deployment and build starts"
                        if generation_first_snowflake_dbt_flow(checkpoint)
                        else "Generated Gold scripts approved before target execution starts"
                    ),
                }
            )
            steps.append(
                {
                    "key": "metadata_setup_execution",
                    "label": "Metadata Setup Execution",
                    "complete": checkpoint.get("metadata_setup_execution_status") == "COMPLETED",
                    "detail": "Generated metadata DDL is verified and executed on the selected target",
                }
            )
            execution_layers = (
                (("gold",) if dbt_deploy else ())
                if generation_first_snowflake_dbt_flow(checkpoint)
                else ("bronze", "silver", "gold")
            )
            for layer in execution_layers:
                execution_step = execution_steps[f"{layer}_code_execution"]
                if generation_first_snowflake_dbt_flow(checkpoint):
                    execution_step["label"] = "Code Execution"
                    execution_step["detail"] = (
                        "Approved source data is landed, then the frozen dbt project is deployed and built in Snowflake"
                    )
                else:
                    execution_step["label"] = f"{layer.capitalize()} Target Execution"
                    execution_step["detail"] = (
                        f"Approved {layer.capitalize()} scripts execute after all code generation and reviews complete"
                    )
                steps.append(execution_step)
            if checkpoint.get("report_generation_enabled") and (
                dbt_deploy
                or (
                    revised_metadata_database_flow(checkpoint)
                    and generation_first_native_database_flow(checkpoint)
                )
            ):
                steps.append(
                    {
                        "key": "report_generation",
                        "label": "Report Generation",
                        "complete": str(checkpoint.get("report_generation_status") or "").upper()
                        in {"COMPLETED", "COMPLETED_WITH_WARNINGS"},
                        "detail": "Professional run summary assembled from the existing pipeline checkpoint",
                    }
                )

    checkpoint_status = str(checkpoint.get("status") or "").upper()
    pipeline_is_active = bool(
        checkpoint.get("background_stage")
        or checkpoint_status in {"RUNNING", "PROCESSING", "SUBMITTED", "IN_PROGRESS"}
    )

    active_stage_key = str(checkpoint.get("background_stage") or "")
    if active_stage_key == "snowflake_dbt_codegen":
        active_stage_key = (
            "gold_review"
            if generation_first_snowflake_dbt_flow(checkpoint)
            else "gold_code_execution"
        )
    external_execution = checkpoint.get("external_execution") if isinstance(checkpoint.get("external_execution"), dict) else {}
    external_message = str(external_execution.get("message") or "").strip()

    executor_owned_completion = {
        step["key"]: bool(step.get("complete"))
        for step in steps
        if step.get("key") in {
            "bronze_code_execution",
            "bronze_runtime_validation",
            "silver_code_execution",
            "silver_runtime_validation",
            "gold_code_execution",
        }
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
        generation_first_flow = generation_first_database_flow(checkpoint)
        execution_stage_keys = {stage_key for _, stage_key in NATIVE_EXECUTION_STAGES}
        for index, step in enumerate(steps):
            if step["complete"] and not (
                generation_first_flow and step.get("key") in execution_stage_keys
            ):
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

    if external_message and str(external_execution.get("layer") or "").lower() == "gold":
        gold_execution_step = next(
            (step for step in steps if step.get("key") == "gold_code_execution"),
            None,
        )
        if gold_execution_step:
            gold_execution_step["detail"] = external_message

    # ponytail: downstream progress is not proof that an execution ran; only
    # executor-owned status (or a real external handoff result) can complete it.
    for step in steps:
        key = step.get("key")
        if key in executor_owned_completion and not executor_owned_completion[key] and key != active_stage_key:
            step["complete"] = False
            step["state"] = "PENDING"

    # If pipeline failed, mark the failed step
    if checkpoint.get("status") == "FAILED":
        failed_key = (
            checkpoint.get("failed_background_stage")
            or checkpoint.get("last_failed_stage_key")
            or checkpoint.get("failed_stage")
        )
        if failed_key == "snowflake_dbt_codegen":
            failed_key = "gold_code_execution"
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
        if index < waiting_index:
            # ponytail: the UI flow is linear; reaching a review proves every
            # upstream stage completed. Replace this with dependency edges if
            # the visible pipeline ever supports branching.
            step["state"] = "COMPLETED"
            step["complete"] = True
        elif index > waiting_index:
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
            **(
                checkpoint.get("stage_confirmation")
                if isinstance(checkpoint.get("stage_confirmation"), dict)
                else {}
            ),
            "enabled": bool(
                checkpoint.get("execution_ready")
                or checkpoint.get("stage_confirmation_enabled")
            ),
            "awaiting_confirmation": True,
            "last_completed_stage_key": checkpoint.get("last_completed_stage_key"),
            "last_completed_stage_label": checkpoint.get("last_completed_stage_label"),
            "next_stage_key": checkpoint.get("next_stage_key"),
            "next_stage_label": checkpoint.get("next_stage_label"),
        }
        if checkpoint.get("resume_message"):
            resume_message = checkpoint.get("resume_message")

    # ponytail: a real review gate always wins over a stale generic confirmation.
    if paused_for_stage_confirmation and (next_gate or next_review_key):
        paused_before_review_gate = True
        stage_confirmation = None

    gold_execution_progress_exists = bool(
        checkpoint.get("background_stage") == "gold_code_execution"
        or str(checkpoint.get("snowflake_gold_execution_status") or "").upper()
        in {"RUNNING", *SNOWFLAKE_COMPLETED_EXECUTION_STATUSES}
        or str(checkpoint.get("databricks_gold_execution_status") or "").upper() == "RUNNING"
        or _status_completed(checkpoint.get("databricks_gold_execution_status"))
        or str(checkpoint.get("status") or "").upper() == "PIPELINE_COMPLETED"
    )
    if gold_execution_progress_exists:
        next_gate = None
        next_review_key = None

    status = checkpoint.get("status") or checkpoint.get("table_nomination_status") or checkpoint.get("enrichment_review_status") or "UNKNOWN"
    target_warehouse = str(checkpoint.get("target_warehouse") or "").strip().lower()
    target_gold_completed = (
        target_warehouse == "databricks"
        and _status_completed(checkpoint.get("databricks_gold_execution_status"))
    ) or (
        target_warehouse == "snowflake"
        and checkpoint.get("snowflake_gold_execution_status") in SNOWFLAKE_COMPLETED_EXECUTION_STATUSES
    )
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
        or checkpoint.get("snowflake_bronze_execution_status") in SNOWFLAKE_COMPLETED_EXECUTION_STATUSES
    ):
        status = "PIPELINE_COMPLETED"
    if can_promote_to_completed and gate3_payload and bronze_generation_completed:
        status = "PIPELINE_COMPLETED"
    if can_promote_to_completed and (
        silver_generation_completed
        or checkpoint.get("databricks_silver_execution_status") == "COMPLETED"
        or checkpoint.get("snowflake_silver_execution_status") in SNOWFLAKE_COMPLETED_EXECUTION_STATUSES
    ):
        status = "PIPELINE_COMPLETED"
    if can_promote_to_completed and (
        gold_generation_completed
        or _status_completed(checkpoint.get("databricks_gold_execution_status"))
        or checkpoint.get("snowflake_gold_execution_status") in SNOWFLAKE_COMPLETED_EXECUTION_STATUSES
    ):
        status = "PIPELINE_COMPLETED"
    if (
        str(status or "").upper() == "PIPELINE_COMPLETED"
        and target_warehouse in {"databricks", "snowflake"}
        and not target_gold_completed
    ):
        status = "HITL_WAIT" if gold_generation_completed else "RUNNING"
    if (
        not checkpoint.get("background_stage")
        and str(status or "").upper() in {"RUNNING", "PROCESSING", "SUBMITTED", "IN_PROGRESS"}
        and (
            _status_completed(checkpoint.get("databricks_gold_execution_status"))
            or str(checkpoint.get("snowflake_gold_execution_status") or "").upper()
            in SNOWFLAKE_COMPLETED_EXECUTION_STATUSES
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
        next_review_key or waiting_gate_key or ""
        if source_value in {"sftp", "adls_gen2"}
        else (
            "gold_review"
            if generation_first_database_flow(checkpoint)
            else "gold_code_execution"
        )
        if next_review_key == "gold_review"
        else next_review_key or waiting_gate_key or ""
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
        "database_flow_version": checkpoint.get("database_flow_version"),
        "generation_first_execution": generation_first_database_flow(checkpoint),
        "report_generation_enabled": bool(checkpoint.get("report_generation_enabled")),
        "report_generation_status": checkpoint.get("report_generation_status"),
        "run_report": checkpoint.get("run_report") or {},
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
    domain_profile: Optional[str] = None,
    knowledge_base_id: Optional[str] = None,
    stage_confirmation_enabled: bool = False,
    compliance_enabled: bool = False,
    compliance_domain: str = "Insurance",
    compliance_countries: Optional[List[str]] = None,
    target_warehouse: str = "databricks",
    target_environment: Optional[str] = None,
    source_system_id: Optional[int] = None,
    source_connection_id: Optional[int] = None,
    source_profile: Optional[str] = None,
    execution_engine: str = "native",
    dbt_deployment_mode: str = "generate_only",
    dbt_project_object_name: Optional[str] = None,
    dbt_target_name: Optional[str] = None,
    dbt_threads: Optional[int] = None,
    dbt_command_timeout_secs: Optional[int] = None,
    force_dbt_deploy: bool = False,
) -> Dict[str, Any]:
    run_id = run_id or str(uuid.uuid4())
    default_source_db = config["azure_sql"].get("source_database") or "insurance"
    source_value = str(source or "database").lower()
    file_sources = {"sftp", "adls_gen2"}
    seeded_state = load_checkpoint_state(run_id) or {}
    initial_state: Dict[str, Any] = {
        **seeded_state,
        "brd_text": brd_text or input_path or "",
        "brd_filename": brd_filename,
        "run_id": run_id,
        "metadata": dict(seeded_state.get("metadata") or {}),
        "status": "PENDING",
        "source": source_value,
        "sftp_entity": str(sftp_entity or "transactions").lower(),
        "source_databases": source_databases or [default_source_db],
        "use_domain_kb": bool(use_domain_kb),
        "domain_profile": domain_profile,
        "knowledge_base_id": knowledge_base_id,
        "stage_confirmation_enabled": bool(stage_confirmation_enabled),
        "compliance_enabled": bool(compliance_enabled),
        "compliance_domain": compliance_domain or "Insurance",
        "compliance_countries": compliance_countries or ["US"],
        "target_warehouse": str(target_warehouse or "databricks").lower(),
        "target_environment": target_environment,
        "source_system_id": source_system_id,
        "source_connection_id": source_connection_id,
        "source_profile": source_profile,
        "execution_engine": str(execution_engine or "native").lower(),
        "dbt_deployment_mode": str(dbt_deployment_mode or "generate_only").lower(),
        "dbt_project_object_name": dbt_project_object_name,
        "dbt_target_name": dbt_target_name,
        "dbt_threads": dbt_threads,
        "dbt_command_timeout_secs": dbt_command_timeout_secs,
        "force_dbt_deploy": bool(force_dbt_deploy),
        "report_generation_enabled": bool(
            seeded_state.get("report_generation_enabled")
            or (
                source_value == "database"
                and str(target_warehouse or "").lower() == "snowflake"
                and str(execution_engine or "").lower() == "dbt"
                and str(dbt_deployment_mode or "").lower() == "generate_and_deploy"
            )
        ),
    }
    if (
        not initial_state.get("database_flow_version")
        and not seeded_state
        and source_value == "database"
    ):
        initial_state["database_flow_version"] = DATABASE_GENERATION_FIRST_FLOW_VERSION
    if (
        revised_metadata_database_flow(initial_state)
        and generation_first_native_database_flow(initial_state)
    ):
        initial_state["report_generation_enabled"] = True

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
    dimension_methods = {
        "FK Resolution (related to nominated table)",
        "Supporting table connected by a foreign key to a nominated KPI source",
    }
    fact_keys = {_table_key(item) for item in approved}
    if fact_keys:
        approved_keys_seen = set(fact_keys)
        for item in tables:
            table_name = str(item.get("table_name") or item.get("table") or "").strip().lower()
            method = str(item.get("nomination_method") or item.get("nomination_reason") or "").strip()
            if (table_name.startswith(dimension_prefixes) or method in dimension_methods) and _table_key(item) not in approved_keys_seen:
                approved.append(item)
                approved_keys_seen.add(_table_key(item))

    if not approved:
        raise ValueError("At least one table must be approved for Table Review.")
    return approved


def _materialize_gate2_ingestion_objects(
    state: Dict[str, Any],
    approved: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    from services.metadata_selection import validated_metadata_selection

    selection = validated_metadata_selection(state)
    if not selection:
        return approved
    materialized = []
    from services.metadata_contracts import validate_identifier

    platform = str(state.get("target_warehouse") or "").lower()
    if platform == "snowflake":
        bronze_catalog = os.getenv("SNOWFLAKE_BRONZE_CATALOG", "ATHENA_DB")
        bronze_schema = os.getenv("SNOWFLAKE_BRONZE_SCHEMA", "BRONZE")
    else:
        bronze_catalog = os.getenv("BRONZE_CATALOG", "main")
        bronze_schema = os.getenv("BRONZE_SCHEMA", "bronze")
    bronze_catalog = validate_identifier(bronze_catalog, label="Bronze catalog")
    bronze_schema = validate_identifier(bronze_schema, label="Bronze schema")
    requests = []
    for table in approved:
        bronze_table = validate_identifier(f"bronze_{table.get('table_name') or ''}", label="Bronze table")
        target_bronze_table = f"{bronze_catalog}.{bronze_schema}.{bronze_table}"
        requests.append({"table": table, "target_bronze_table": target_bronze_table})
    bulk_upsert = getattr(selection.repository, "upsert_database_ingestion_object_drafts", None)
    if callable(bulk_upsert):
        ingestion_objects = bulk_upsert(
            source_system_id=int(state["source_system_id"]),
            connection_id=int(state["source_connection_id"]),
            expected_connection_version=int(selection.connection["config_version"]),
            expected_connection_hash=str(selection.connection["config_hash"]),
            allow_inactive_connection=bool(getattr(selection, "uses_environment_source", False)),
            requests=requests,
        )
    else:
        ingestion_objects = [
            selection.repository.upsert_database_ingestion_object_draft(
                source_system_id=int(state["source_system_id"]),
                connection_id=int(state["source_connection_id"]),
                table=request["table"],
                expected_connection_version=int(selection.connection["config_version"]),
                expected_connection_hash=str(selection.connection["config_hash"]),
                target_bronze_table=request["target_bronze_table"],
                allow_inactive_connection=bool(getattr(selection, "uses_environment_source", False)),
            )
            for request in requests
        ]
    for request, ingestion_object in zip(requests, ingestion_objects):
        table = request["table"]
        materialized.append(
            {
                **table,
                "ingestion_object_id": int(ingestion_object["ingestion_object_id"]),
                "ingestion_object_config_version": int(ingestion_object["config_version"]),
                "ingestion_object_config_hash": str(ingestion_object["config_hash"]),
                "target_bronze_table": request["target_bronze_table"],
            }
        )
    return materialized


def _materialize_source_to_bronze_mappings(
    state: Dict[str, Any],
    approved_metadata: Dict[str, Any],
    certified_tables: List[Dict[str, Any]],
    *,
    _selection: Any = None,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    from services.metadata_selection import validated_metadata_selection

    selection = _selection or validated_metadata_selection(state)
    if not selection:
        return certified_tables, []
    if _selection is None and callable(getattr(selection.repository, "unit_of_work", None)):
        with selection.repository.unit_of_work():
            return _materialize_source_to_bronze_mappings(
                state,
                approved_metadata,
                certified_tables,
                _selection=selection,
            )
    columns_by_object: Dict[int, List[Dict[str, Any]]] = {}
    for column in approved_metadata.get("columns") or []:
        if isinstance(column, dict) and column.get("ingestion_object_id") is not None:
            columns_by_object.setdefault(int(column["ingestion_object_id"]), []).append(column)

    mapped_tables: List[Dict[str, Any]] = []
    bundles: List[Dict[str, Any]] = []
    references = [
        {
            "ingestion_object_id": int(table["ingestion_object_id"]),
            "config_version": int(table["ingestion_object_config_version"]),
        }
        for table in certified_tables
        if table.get("ingestion_object_id") is not None and table.get("ingestion_object_config_version") is not None
    ]
    load_objects = getattr(selection.repository, "get_ingestion_objects", None)
    objects = load_objects(references) if callable(load_objects) else {}
    for table in certified_tables:
        ingestion_object_id = table.get("ingestion_object_id")
        config_version = table.get("ingestion_object_config_version")
        if ingestion_object_id is None or config_version is None:
            raise ValueError("Metadata-enabled Bronze mapping requires ingestion-object lineage for every table.")
        ingestion_object = objects.get((int(ingestion_object_id), int(config_version))) or selection.repository.get_ingestion_object(
            int(ingestion_object_id), int(config_version)
        )
        if not ingestion_object:
            raise ValueError(f"Ingestion-object draft not found: {ingestion_object_id}/{config_version}")
        bundle = selection.repository.upsert_source_to_bronze_mapping_draft(
            ingestion_object=ingestion_object,
            columns=columns_by_object.get(int(ingestion_object_id), []),
        )
        bundles.append(bundle)
        mapped_tables.append(
            {
                **table,
                "source_to_bronze_mapping_version": bundle["mapping_version"],
                "source_to_bronze_mapping_hash": bundle["mapping_hash"],
            }
        )
    return mapped_tables, bundles


def submit_gate2_review(run_id: str, approved_keys: List[str]) -> Dict[str, Any]:
    from nodes.hitl import hitl_table_review_node

    checkpoint_state = load_checkpoint_state(run_id) or {}
    tables = (
        fetch_json_artifact(run_id, "TABLE_NOMINATIONS").get("nominations", [])
        or checkpoint_state.get("nominated_tables")
        or []
    )
    if checkpoint_state.get("source_system_id") is not None:
        approved_key_set = set(approved_keys)
        approved = [item for item in tables if _table_key(item) in approved_key_set]
        if not approved:
            raise ValueError("At least one table must be explicitly approved for Table Review.")
    else:
        approved = _gate2_execution_scope(tables, approved_keys)
    approved = _materialize_gate2_ingestion_objects(checkpoint_state, approved)

    resumed_input = dict(checkpoint_state or {"run_id": run_id})
    resumed_input.pop("error", None)
    resumed_input.pop("failed_background_stage", None)
    resumed_input["human_table_decision"] = "COMPLETED"
    resumed_input["certified_tables"] = approved
    resumed_input["ingestion_objects"] = [
        {
            "ingestion_object_id": table["ingestion_object_id"],
            "config_version": table["ingestion_object_config_version"],
            "config_hash": table.get("ingestion_object_config_hash"),
            "database_name": table.get("database_name"),
            "schema_name": table.get("schema_name"),
            "table_name": table.get("table_name"),
        }
        for table in approved
        if table.get("ingestion_object_id") is not None
    ]
    with timed_stage("gate2_hitl_certification", run_id=run_id, node="api"):
        resumed = hitl_table_review_node(resumed_input)
    if resumed.get("status") == "FAILED":
        raise ValueError(resumed.get("error", "Table Review certification failed."))
    save_checkpoint_state(run_id, resumed)

    return continue_database_pipeline(run_id, start_stage_key="discovery", state=resumed)


def ensure_gate3_review_queue(
    run_id: str,
    enriched_metadata: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Materialize Gate 3 table reviews for new and pre-existing runs."""
    from nodes.hitl import build_gate3_review_items

    existing_rows = get_hitl_items(run_id, 3)
    if existing_rows:
        return existing_rows
    if fetch_json_artifact(run_id, "GATE3_APPROVED_ENRICHMENT"):
        return []
    checkpoint = load_checkpoint_state(run_id) or {}
    metadata = enriched_metadata or fetch_json_artifact(run_id, "ENRICHED_METADATA") or _checkpoint_enriched_payload(checkpoint)
    if not metadata:
        return []
    ensure_hitl_queue_items(run_id, build_gate3_review_items(run_id, metadata), gate_number=3)
    return get_hitl_items(run_id, 3)


def save_gate3_review_draft(
    run_id: str,
    item_id: str,
    edited_content: Dict[str, Any],
    expected_revision: Optional[str] = None,
) -> Dict[str, Any]:
    from nodes.hitl import validate_gate3_table_edit

    rows = ensure_gate3_review_queue(run_id)
    row = next((item for item in rows if item["item_id"] == item_id), None)
    if not row:
        raise LookupError(f"Gate 3 review item not found: {item_id}")
    normalized = validate_gate3_table_edit(row.get("original_content") or {}, edited_content)
    revision = save_hitl_item_draft(run_id, 3, item_id, normalized, expected_revision)
    return {"item_id": item_id, "edited_content": normalized, "revision": revision, "status": "SAVED"}


def record_gate3_review_decisions(run_id: str, decisions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Persist a complete Gate 3 decision set in one transaction."""
    from nodes.hitl import validate_gate3_table_edit

    rows = ensure_gate3_review_queue(run_id)
    rows_by_id = {row["item_id"]: row for row in rows}
    decisions_by_id = {str(item.get("item_id") or ""): item for item in decisions}
    if not rows or len(decisions_by_id) != len(decisions) or set(decisions_by_id) != set(rows_by_id):
        raise ValueError("A decision is required for every Gate 3 review item.")

    updates: List[Dict[str, Optional[str]]] = []
    for item_id, decision in decisions_by_id.items():
        action = str(decision.get("decision") or "").upper()
        if action not in {"APPROVED", "REJECTED"}:
            raise ValueError(f"Unsupported Gate 3 decision for {item_id}: {action}")
        row = rows_by_id[item_id]
        edited = row.get("edited_content")
        if action == "APPROVED":
            validate_gate3_table_edit(row.get("original_content") or {}, edited or row.get("original_content") or {})
        reason = str(decision.get("rejection_reason") or "").strip()
        if action == "REJECTED" and not reason:
            raise ValueError(f"A rejection reason is required for Gate 3 item {item_id}.")
        current_status = str(row.get("gate_status") or "PENDING").upper()
        if current_status != "PENDING":
            status_matches = (
                (action == "REJECTED" and current_status == "REJECTED")
                or (action == "APPROVED" and current_status in {"APPROVED", "EDITED"})
            )
            reason_matches = action != "REJECTED" or reason == str(row.get("rejection_reason") or "").strip()
            if status_matches and reason_matches:
                continue
            raise RuntimeError(f"Gate 3 item is already decided differently: {item_id}")
        updates.append({
            "item_id": item_id,
            "status": "REJECTED" if action == "REJECTED" else "EDITED" if edited else "APPROVED",
            "edited_content": json.dumps(edited) if edited else None,
            "rejection_reason": reason if action == "REJECTED" else None,
        })

    if updates:
        update_hitl_items_batch(updates)
    return get_hitl_items(run_id, 3)


def submit_gate3_review(
    run_id: str,
    approve: bool,
    enriched_metadata: Optional[Dict[str, Any]] = None,
    use_persisted_review: bool = False,
) -> Dict[str, Any]:
    from nodes.hitl import apply_gate3_review_rows, build_hitl_enrichment_review_node
    from services.compliance_client import attach_review_result

    checkpoint_state = load_checkpoint_state(run_id) or {}
    metadata = enriched_metadata or fetch_json_artifact(run_id, "ENRICHED_METADATA") or _checkpoint_enriched_payload(checkpoint_state)
    if not metadata:
        raise ValueError("No enriched metadata found for this run.")
    if checkpoint_state.get("source_system_id") is not None:
        if enriched_metadata is not None and not use_persisted_review:
            raise ValueError("Metadata-enabled enrichment approval must use persisted Gate 3 review decisions.")
        use_persisted_review = True
    if use_persisted_review:
        review_rows = get_hitl_items(run_id, 3)
        if not review_rows or any(str(row.get("gate_status") or "").upper() == "PENDING" for row in review_rows):
            raise ValueError("Gate 3 review decisions are incomplete.")
        approve = not any(str(row.get("gate_status") or "").upper() == "REJECTED" for row in review_rows)
        if approve:
            metadata = apply_gate3_review_rows(metadata, review_rows)

    enrichment_node = build_hitl_enrichment_review_node()
    state: Dict[str, Any] = {
        "run_id": run_id,
        "fingerprint": metadata.get("fingerprint") or checkpoint_state.get("fingerprint") or run_id,
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
    certified_tables, mapping_bundles = _materialize_source_to_bronze_mappings(
        checkpoint_state,
        metadata,
        certified_tables,
    )

    bronze_state: Dict[str, Any] = {
        **checkpoint_state,
        **result,
        "run_id": run_id,
        "enriched_metadata": metadata,
        "fingerprint": metadata.get("fingerprint") or checkpoint_state.get("fingerprint") or run_id,
        "certified_tables": certified_tables,
        "source_to_bronze_mapping_bundles": mapping_bundles,
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
    return continue_database_pipeline(run_id, start_stage_key="metadata_ddl", state=bronze_state)


def _apply_gate4_merge_keys_to_metadata(metadata: Dict[str, Any], review_artifact: Dict[str, Any]) -> Dict[str, Any]:
    feeds = review_artifact.get("feeds") or []
    if not feeds or not isinstance(metadata, dict):
        return metadata

    keys_by_table = {
        str(
            feed.get("table") or feed.get("entity") or feed.get("table_name")
            or feed.get("feed_id") or feed.get("target_table") or ""
        ).split(".")[-1].strip().lower(): {
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
        table_name = str(
            column.get("table_name") or column.get("table") or column.get("entity") or column.get("feed_id") or ""
        ).split(".")[-1].strip().lower()
        column_name = str(column.get("column_name") or "").strip().lower()
        reviewed_keys = keys_by_table.get(table_name) or set()
        if reviewed_keys and column_name in reviewed_keys:
            columns.append({**column, "is_join_key": True, "semantic_type": column.get("semantic_type") or "ID"})
        elif reviewed_keys:
            columns.append({**column, "is_join_key": False})
        else:
            columns.append(column)
    return {**metadata, "columns": columns, "gate4_reviewed_merge_keys": review_artifact}


def _apply_reviewed_keys_to_bronze_results(
    bronze_results: List[Dict[str, Any]],
    review_artifact: Dict[str, Any],
) -> List[Dict[str, Any]]:
    reviewed = {
        str(
            feed.get("table") or feed.get("table_name") or feed.get("entity")
            or feed.get("feed_id") or feed.get("target_table") or ""
        ).split(".")[-1].strip().casefold(): [
            str(key).strip()
            for key in (feed.get("merge_keys") or feed.get("primary_keys") or [])
            if str(key).strip()
        ]
        for feed in (review_artifact.get("feeds") or [])
        if isinstance(feed, dict)
    }
    results = []
    for result in bronze_results:
        table_key = str(
            result.get("table") or result.get("table_name") or result.get("entity")
            or result.get("feed_id") or result.get("target_table") or ""
        ).split(".")[-1].strip().casefold()
        if table_key not in reviewed:
            results.append(result)
            continue
        bronze_config = dict(result.get("bronze_config") or result.get("generated_bronze_config") or {})
        bronze_config["primary_keys"] = reviewed[table_key]
        results.append({
            **result,
            "primary_keys": reviewed[table_key],
            "merge_keys": reviewed[table_key],
            "bronze_config": bronze_config,
            "generated_bronze_config": bronze_config,
        })
    return results


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
        "silver_merge_key_review_decision": None,
        "silver_merge_key_review_artifact": artifact,
        "gate_silver_merge_key_review": {
            "gate": "silver_merge_key_review",
            "status": "PENDING",
            "decision": None,
        },
        "resume_message": "Silver Merge Key Review is pending. Review selected keys and candidates before Silver generation.",
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

    if any(result.get("silver_ingestion_object_id") is not None for result in silver_results):
        results_by_id = {
            int(result["silver_ingestion_object_id"]): result
            for result in silver_results
            if result.get("silver_ingestion_object_id") is not None
        }
        reviewed_ids = []
        for item in approved_items or rejected_items:
            object_id = int(item.get("silver_ingestion_object_id") or 0)
            if object_id not in results_by_id:
                raise ValueError("Gate 5 metadata review must identify the exact Silver transformation object.")
            reviewed_ids.append(object_id)
        if len(reviewed_ids) != len(set(reviewed_ids)):
            raise ValueError("Gate 5 contains duplicate Silver transformation-object decisions.")
        selected_ids = set(reviewed_ids)
        return (
            [result for object_id, result in results_by_id.items() if object_id in selected_ids]
            if approved_items
            else [result for object_id, result in results_by_id.items() if object_id not in selected_ids]
        )

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


def _filter_gold_results_by_review(
    gold_results: List[Dict[str, Any]], review_artifact: Dict[str, Any]
) -> List[Dict[str, Any]]:
    if not any(result.get("gold_ingestion_object_id") is not None for result in gold_results):
        from services.databricks_runtime import _filtered_scripts

        return _filtered_scripts(gold_results, review_artifact, "gold")
    items = [item for item in (review_artifact or {}).get("items") or [] if isinstance(item, dict)]
    if not items:
        return gold_results
    approved = [item for item in items if str(item.get("review_status") or "").upper() == "APPROVED"]
    rejected = [item for item in items if str(item.get("review_status") or "").upper() == "REJECTED"]
    if not approved and not rejected:
        return gold_results
    results_by_id = {int(item["gold_ingestion_object_id"]): item for item in gold_results}
    reviewed_ids = [int(item.get("gold_ingestion_object_id") or 0) for item in (approved or rejected)]
    if len(reviewed_ids) != len(set(reviewed_ids)) or any(object_id not in results_by_id for object_id in reviewed_ids):
        raise ValueError("Gold review must identify each exact transformation object once.")
    selected = set(reviewed_ids)
    return (
        [item for object_id, item in results_by_id.items() if object_id in selected]
        if approved
        else [item for object_id, item in results_by_id.items() if object_id not in selected]
    )


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

    silver_targets = {
        str(item.get("table") or item.get("table_name") or "").split(".")[-1].casefold().removeprefix("silver_"):
        str(item.get("target_table") or item.get("silver_table") or "").strip().casefold()
        for item in silver_results
        if str(item.get("target_table") or item.get("silver_table") or "").strip()
    }

    def mapping_sources(mapping: Dict[str, Any]) -> set[str]:
        sources = {str(mapping.get("source_silver_table") or "").strip().casefold()}
        for dimension in mapping.get("grouping_dimensions") or []:
            if not isinstance(dimension, dict):
                continue
            sources.add(
                str(dimension.get("source_silver_table") or silver_targets.get(
                    str(dimension.get("table") or "").split(".")[-1].casefold().removeprefix("silver_"), ""
                )).strip().casefold()
            )
        time_column = (mapping.get("time") or {}).get("column")
        if isinstance(time_column, dict):
            sources.add(str(silver_targets.get(
                str(time_column.get("table") or "").split(".")[-1].casefold().removeprefix("silver_"), ""
            )).strip().casefold())
        for join in mapping.get("join_paths") or []:
            if not isinstance(join, dict):
                continue
            for side in ("left", "right"):
                sources.add(str(join.get(f"{side}_source_table") or silver_targets.get(
                    str(join.get(f"{side}_table") or "").split(".")[-1].casefold().removeprefix("silver_"), ""
                )).strip().casefold())
        sources.discard("")
        return sources

    mappings = [
        mapping
        for mapping in contract.get("kpi_mappings") or []
        if isinstance(mapping, dict)
        and mapping_sources(mapping)
        and mapping_sources(mapping).issubset(allowed_sources)
    ]
    dropped = len(contract.get("kpi_mappings") or []) - len(mappings)
    if dropped:
        warnings.append(f"Gold scope filtered out {dropped} KPI mapping(s) because their Silver source was not approved for execution.")
    return {**contract, "kpi_mappings": mappings, "warnings": warnings}


def _materialize_silver_to_gold_metadata(
    state: Dict[str, Any], *, _selection: Any = None
) -> Dict[str, Any]:
    """Persist only computable Gold facts/dimensions after Silver approval."""
    from services.metadata_contracts import normalize_bronze_column_name, validate_identifier
    from services.metadata_selection import validated_metadata_selection

    selection = _selection or validated_metadata_selection(state)
    if not selection:
        raise ValueError("Silver-to-Gold metadata requires a valid target selection.")
    if _selection is None and callable(getattr(selection.repository, "unit_of_work", None)):
        with selection.repository.unit_of_work():
            return _materialize_silver_to_gold_metadata(state, _selection=selection)
    dbt_codegen = snowflake_dbt_enabled(state)

    platform = str(state.get("target_warehouse") or "").lower()
    if platform == "snowflake":
        gold_catalog = validate_identifier(
            os.getenv("SNOWFLAKE_GOLD_CATALOG") or os.getenv("SNOWFLAKE_SILVER_CATALOG") or "ATHENA_DB",
            label="Gold catalog",
        )
        gold_schema = validate_identifier(os.getenv("SNOWFLAKE_GOLD_SCHEMA", "GOLD"), label="Gold schema")
        target_prefix = f"{gold_catalog}.{gold_schema}"
    else:
        gold_catalog = validate_identifier(
            os.getenv("GOLD_CATALOG") or os.getenv("SILVER_CATALOG") or os.getenv("BRONZE_CATALOG") or "main",
            label="Gold catalog",
        )
        gold_schema = validate_identifier(os.getenv("GOLD_SCHEMA", "gold"), label="Gold schema")
        target_prefix = f"{gold_catalog}.{gold_schema}"

    approved_silver = [
        result for result in state.get("silver_generation_results") or []
        if isinstance(result, dict)
        and result.get("metadata_activation_status") in (
            {"ACTIVE", "PENDING_FINAL_DBT_PACKAGE"} if dbt_codegen else {"ACTIVE"}
        )
    ]
    pending_dbt_inputs = [
        result for result in approved_silver
        if result.get("metadata_activation_status") == "PENDING_FINAL_DBT_PACKAGE"
    ]
    if pending_dbt_inputs and len(pending_dbt_inputs) != len(approved_silver):
        raise RuntimeError("Snowflake dbt Silver metadata cannot mix draft and active package inputs.")
    using_dbt_drafts = bool(pending_dbt_inputs)
    silver_ids = [int(result["silver_ingestion_object_id"]) for result in approved_silver]
    silver_object_refs = [{
        "ingestion_object_id": int(result["silver_ingestion_object_id"]),
        "config_version": int(result["silver_ingestion_object_config_version"]),
    } for result in approved_silver] if using_dbt_drafts else []
    load_objects = getattr(selection.repository, "get_ingestion_objects", None)
    reviewed_silver = (
        load_objects(silver_object_refs, require_active=False)
        if using_dbt_drafts and callable(load_objects)
        else {}
    )
    load_active = getattr(selection.repository, "get_active_ingestion_objects", None)
    active_silver = load_active(silver_ids) if not using_dbt_drafts and callable(load_active) else {}
    silver_bundle_refs = [{
        "ingestion_object_id": int(result["silver_ingestion_object_id"]),
        "processing_stage": "BRONZE_TO_SILVER",
        "mapping_version": int(result["bronze_to_silver_mapping_version"]),
        "expected_hash": str(result["bronze_to_silver_mapping_hash"]),
        "expected_target": str(result.get("target_table") or result.get("target_silver_table") or ""),
        "require_active": None if using_dbt_drafts else True,
    } for result in approved_silver]
    load_bundles = getattr(selection.repository, "get_mapping_bundles", None)
    active_bundles = load_bundles(silver_bundle_refs) if callable(load_bundles) else {}
    silver_by_logical: Dict[str, Dict[str, Any]] = {}
    for result in approved_silver:
        if not isinstance(result, dict) or result.get("metadata_activation_status") not in (
            {"ACTIVE", "PENDING_FINAL_DBT_PACKAGE"} if dbt_codegen else {"ACTIVE"}
        ):
            continue
        logical = str(result.get("table") or result.get("table_name") or "").split(".")[-1].casefold()
        logical = logical.removeprefix("silver_")
        if not logical or logical in silver_by_logical:
            raise ValueError("Approved Silver results contain an ambiguous logical table identity.")
        object_id = int(result["silver_ingestion_object_id"])
        target = str(result.get("target_table") or result.get("target_silver_table") or "")
        mapping_version = int(result["bronze_to_silver_mapping_version"])
        bundle = active_bundles.get((object_id, "BRONZE_TO_SILVER", mapping_version)) or selection.repository.get_mapping_bundle(
            ingestion_object_id=object_id,
            processing_stage="BRONZE_TO_SILVER",
            mapping_version=mapping_version,
            expected_hash=str(result["bronze_to_silver_mapping_hash"]),
            expected_target=target,
            require_active=False if using_dbt_drafts else True,
        )
        active = (
            reviewed_silver.get((object_id, int(result["silver_ingestion_object_config_version"])))
            if using_dbt_drafts
            else active_silver.get(object_id) or selection.repository.get_active_ingestion_object(object_id)
        )
        if not active:
            raise ValueError(f"Reviewed Silver transformation object not found: {object_id}")
        columns = {
            str(row.get("target_column_name") or "").casefold(): row
            for row in bundle["mappings"]
        }
        silver_by_logical[logical] = {
            "result": result,
            "object": active,
            "bundle": bundle,
            "columns": columns,
            "input": {
                "ingestion_object_id": object_id,
                "config_version": int(active["config_version"]),
                "config_hash": str(active["config_hash"]),
                "mapping_version": int(bundle["mapping_version"]),
                "mapping_hash": str(bundle["mapping_hash"]),
            },
            "target": target,
        }

    contract = dict(state.get("gold_generation_contract") or {})
    rejections: List[Dict[str, Any]] = []
    validation_warnings: List[Dict[str, Any]] = []
    drafts: List[Dict[str, Any]] = []
    objects: List[Dict[str, Any]] = []
    bundles: List[Dict[str, Any]] = []
    used_targets: set[str] = set()

    def reject(kind: str, name: str, code: str, detail: str) -> None:
        rejections.append({"object_kind": kind, "name": name, "code": code, "detail": detail})

    def source(logical: Any) -> Optional[Dict[str, Any]]:
        name = str(logical or "").split(".")[-1].casefold().removeprefix("silver_")
        return silver_by_logical.get(name)

    def source_column(item: Dict[str, Any], column_name: Any) -> Optional[Dict[str, Any]]:
        return item["columns"].get(normalize_bronze_column_name(column_name).casefold())

    def type_family(value: Any) -> str:
        base = re.split(r"[<(]", str(value or "").upper(), maxsplit=1)[0].strip()
        if any(token in base for token in ("INT", "DECIMAL", "NUMERIC", "NUMBER", "FLOAT", "DOUBLE", "REAL")):
            return "NUMERIC"
        if any(token in base for token in ("CHAR", "STRING", "TEXT", "VARCHAR")):
            return "STRING"
        if any(token in base for token in ("DATE", "TIME")):
            return "TEMPORAL"
        return base

    def compatible_join_types(left: Any, right: Any) -> bool:
        return bool(type_family(left)) and type_family(left) == type_family(right)

    for raw in contract.get("dimension_mappings") or []:
        if not isinstance(raw, dict):
            continue
        logical = str(raw.get("logical_table") or "").strip()
        name = f"dim_{normalize_bronze_column_name(logical)}"
        item = source(logical)
        if not item:
            reject("DIMENSION", name, "MISSING_SILVER_INPUT", f"No approved Silver object exists for {logical}.")
            continue
        key_rows = [row for row in item["bundle"]["mappings"] if bool(row.get("is_primary_key"))]
        if not key_rows:
            reject("DIMENSION", name, "MISSING_BUSINESS_KEY", f"No reviewed business key exists for {logical}.")
            continue
        requested = list(dict.fromkeys([*(raw.get("columns") or []), *[row["target_column_name"] for row in key_rows]]))
        mapped = []
        for ordinal, column_name in enumerate(requested, 1):
            row = source_column(item, column_name)
            if not row:
                validation_warnings.append({
                    "object_kind": "DIMENSION",
                    "name": name,
                    "code": "DROPPED_OPTIONAL_FIELD",
                    "detail": f"{logical}.{column_name} is not in the active Silver mapping.",
                })
                continue
            target_column = normalize_bronze_column_name(row["target_column_name"])
            mapped.append({
                "source_object_name": item["target"],
                "source_field_path": str(row["target_column_name"]),
                "source_data_type": str(row["target_data_type"]),
                "target_column_name": target_column,
                "target_data_type": str(row["target_data_type"]),
                "is_nullable": bool(row.get("is_nullable", True)),
                "is_primary_key": bool(row.get("is_primary_key")),
                "ordinal_position": ordinal,
                "transformation_rule": "IDENTITY",
            })
        if not mapped:
            continue
        target = f"{target_prefix}.{name}"
        keys = [normalize_bronze_column_name(row["target_column_name"]) for row in key_rows]
        definition = {"artifact_kind": "DIMENSION", "logical_table": logical, "columns": requested}
        created = selection.repository.upsert_silver_to_gold_draft(
            source_system_id=int(item["object"]["source_system_id"]),
            target_gold_table=target,
            inputs=[item["input"]],
            columns=mapped,
            merge_keys=keys,
            join_rules=[],
            definition=definition,
            build_order=10,
            validation_policy={"fail_on_null_key": True, "fail_on_duplicate_key": True, "fail_on_schema_mismatch": True},
            allow_inactive_inputs=using_dbt_drafts,
        )
        used_targets.add(target.casefold())
        drafts.append({
            "artifact_kind": "DIMENSION",
            "name": name,
            "gold_ingestion_object_id": int(created["ingestion_object"]["ingestion_object_id"]),
            "gold_ingestion_object_config_version": int(created["ingestion_object"]["config_version"]),
            "gold_ingestion_object_config_hash": str(created["ingestion_object"]["config_hash"]),
            "silver_to_gold_mapping_version": int(created["mapping_bundle"]["mapping_version"]),
            "silver_to_gold_mapping_hash": str(created["mapping_bundle"]["mapping_hash"]),
            "target_table": target,
        })
        objects.append(created["ingestion_object"])
        bundles.append(created["mapping_bundle"])

    for raw in contract.get("factless_mappings") or []:
        if not isinstance(raw, dict):
            continue
        logical = str(raw.get("logical_table") or "").strip()
        name = f"fact_{normalize_bronze_column_name(logical)}_coverage"
        item = source(logical)
        if not item:
            reject("FACT", name, "MISSING_SILVER_INPUT", f"No approved Silver object exists for {logical}.")
            continue
        key_rows = [row for row in item["bundle"]["mappings"] if bool(row.get("is_primary_key"))]
        if not key_rows:
            reject("FACT", name, "MISSING_BUSINESS_KEY", f"No reviewed business grain exists for {logical}.")
            continue
        mapped = [{
            "source_object_name": item["target"],
            "source_field_path": str(row["target_column_name"]),
            "source_data_type": str(row["target_data_type"]),
            "target_column_name": normalize_bronze_column_name(row["target_column_name"]),
            "target_data_type": str(row["target_data_type"]),
            "is_nullable": False,
            "is_primary_key": True,
            "ordinal_position": ordinal,
            "transformation_rule": "GROUP_KEY",
        } for ordinal, row in enumerate(key_rows, 1)]
        keys = [str(row["target_column_name"]) for row in mapped]
        target = f"{target_prefix}.{name}"
        if target.casefold() in used_targets:
            reject("FACT", name, "DUPLICATE_GOLD_TARGET", f"More than one Gold object resolves to {target}.")
            continue
        definition = {
            "artifact_kind": "FACT",
            "fact_type": "FACTLESS_ENTITY_COVERAGE",
            "logical_table": logical,
            "grain_columns": keys,
        }
        created = selection.repository.upsert_silver_to_gold_draft(
            source_system_id=int(item["object"]["source_system_id"]),
            target_gold_table=target,
            inputs=[item["input"]],
            columns=mapped,
            merge_keys=keys,
            join_rules=[],
            definition=definition,
            build_order=20,
            write_mode="MERGE",
            validation_policy={
                "fail_on_missing_input": True,
                "fail_on_null_key": True,
                "fail_on_duplicate_key": True,
                "fail_on_schema_mismatch": True,
            },
            allow_inactive_inputs=using_dbt_drafts,
        )
        used_targets.add(target.casefold())
        drafts.append({
            "artifact_kind": "FACT",
            "fact_type": "FACTLESS_ENTITY_COVERAGE",
            "name": name,
            "gold_ingestion_object_id": int(created["ingestion_object"]["ingestion_object_id"]),
            "gold_ingestion_object_config_version": int(created["ingestion_object"]["config_version"]),
            "gold_ingestion_object_config_hash": str(created["ingestion_object"]["config_hash"]),
            "silver_to_gold_mapping_version": int(created["mapping_bundle"]["mapping_version"]),
            "silver_to_gold_mapping_hash": str(created["mapping_bundle"]["mapping_hash"]),
            "target_table": target,
        })
        objects.append(created["ingestion_object"])
        bundles.append(created["mapping_bundle"])

    for raw in contract.get("kpi_mappings") or []:
        if not isinstance(raw, dict):
            continue
        kpi_name = str(raw.get("kpi_name") or "KPI").strip()
        name = f"fact_{normalize_bronze_column_name(kpi_name)}"
        if str(raw.get("readiness") or "").upper() == "BLOCKED":
            reject("FACT", name, "INCOMPLETE_KPI_CONTRACT", "The approved KPI contract is marked BLOCKED.")
            continue
        filters = list(raw.get("filters") or [])
        if filters:
            reject("FACT", name, "UNSUPPORTED_FILTER", "Executable Gold filters require a validated structured expression contract.")
            continue
        measure = dict(raw.get("measure") or {})
        measure_input = source(measure.get("table") or raw.get("source_silver_table"))
        if not measure_input:
            reject("FACT", name, "MISSING_SILVER_INPUT", "The KPI measure source is not an approved Silver object.")
            continue
        logical_inputs = {str(measure.get("table") or "").casefold()}
        logical_inputs.update(str(dim.get("table") or "").casefold() for dim in raw.get("grouping_dimensions") or [] if isinstance(dim, dict))
        time_column = (raw.get("time") or {}).get("column")
        if isinstance(time_column, dict):
            logical_inputs.add(str(time_column.get("table") or "").casefold())
        joins = [join for join in raw.get("join_paths") or [] if isinstance(join, dict) and join.get("certified") is True]
        logical_inputs.update(str(join.get(side) or "").casefold() for join in joins for side in ("left_table", "right_table"))
        logical_inputs.discard("")
        resolved_inputs = [source(logical) for logical in sorted(logical_inputs)]
        if any(item is None for item in resolved_inputs):
            reject("FACT", name, "MISSING_SILVER_INPUT", "One or more KPI inputs are not approved active Silver objects.")
            continue
        inputs_by_id = {
            int(item["input"]["ingestion_object_id"]): item
            for item in [measure_input, *[value for value in resolved_inputs if value is not None]]
        }
        inputs = list(inputs_by_id.values())
        if len(inputs) > 1 and len(joins) < len(inputs) - 1:
            reject("FACT", name, "DISCONNECTED_JOIN_GRAPH", "Certified joins do not connect every required Silver input.")
            continue
        join_rules = []
        join_valid = True
        join_edges: set[tuple[str, str]] = set()
        for join in joins:
            left = source(join.get("left_table"))
            right = source(join.get("right_table"))
            left_row = source_column(left, join.get("left_column")) if left else None
            right_row = source_column(right, join.get("right_column")) if right else None
            if not left or not right or not left_row or not right_row:
                reject("FACT", name, "UNCERTIFIED_JOIN", "A certified join does not match the active Silver column contracts.")
                join_valid = False
                break
            join_type = str(join.get("join_type") or "INNER").upper()
            edge = tuple(sorted((left["target"].casefold(), right["target"].casefold())))
            if (
                join_type not in {"INNER", "LEFT"}
                or edge[0] == edge[1]
                or edge in join_edges
                or not compatible_join_types(left_row.get("target_data_type"), right_row.get("target_data_type"))
            ):
                reject("FACT", name, "INVALID_JOIN_GRAPH", "A certified join has an unsupported type, duplicate edge, or incompatible key datatype.")
                join_valid = False
                break
            join_edges.add(edge)
            join_rules.append({
                "left_source_table": left["target"],
                "left_column": str(left_row["target_column_name"]),
                "right_source_table": right["target"],
                "right_column": str(right_row["target_column_name"]),
                "join_type": join_type,
                "cardinality": join.get("cardinality"),
                "certified": True,
            })
        if not join_valid:
            continue
        ordered_sources = {measure_input["target"].casefold()}
        pending_rules = list(join_rules)
        while pending_rules:
            progressed = False
            for rule in list(pending_rules):
                left = str(rule["left_source_table"]).casefold()
                right = str(rule["right_source_table"]).casefold()
                join_type = str(rule.get("join_type") or "INNER").upper()
                if left in ordered_sources and right not in ordered_sources:
                    ordered_sources.add(right)
                elif join_type == "INNER" and right in ordered_sources and left not in ordered_sources:
                    ordered_sources.add(left)
                else:
                    continue
                pending_rules.remove(rule)
                progressed = True
            if not progressed:
                reject("FACT", name, "INVALID_JOIN_GRAPH", "Certified joins cannot be ordered safely from the KPI measure input.")
                join_valid = False
                break
        if not join_valid:
            continue
        required_nodes = {item["target"].casefold() for item in inputs}
        graph = {node: set() for node in required_nodes}
        for left, right in join_edges:
            graph[left].add(right)
            graph[right].add(left)
        visited: set[str] = set()
        pending = [measure_input["target"].casefold()]
        while pending:
            node = pending.pop()
            if node in visited:
                continue
            visited.add(node)
            pending.extend(graph.get(node, set()) - visited)
        if visited != required_nodes or len(join_edges) != max(0, len(required_nodes) - 1):
            reject("FACT", name, "DISCONNECTED_JOIN_GRAPH", "Certified joins must form one unambiguous graph rooted at the measure input.")
            continue
        mapped: List[Dict[str, Any]] = []
        keys: List[str] = []
        seen_targets: set[str] = set()

        def add_output(input_item: Dict[str, Any], field: Any, target_field: Any, rule: str, target_type: Optional[str] = None) -> bool:
            row = source_column(input_item, field)
            target_name = normalize_bronze_column_name(target_field)
            if not row or target_name.casefold() in seen_targets:
                return False
            seen_targets.add(target_name.casefold())
            mapped.append({
                "source_object_name": input_item["target"],
                "source_field_path": str(row["target_column_name"]),
                "source_data_type": str(row["target_data_type"]),
                "target_column_name": target_name,
                "target_data_type": target_type or str(row["target_data_type"]),
                "is_nullable": bool(row.get("is_nullable", True)),
                "is_primary_key": False,
                "ordinal_position": len(mapped) + 1,
                "transformation_rule": rule,
            })
            return True

        invalid_field = False
        for dimension in raw.get("grouping_dimensions") or []:
            if not isinstance(dimension, dict):
                continue
            input_item = source(dimension.get("table") or measure.get("table"))
            target_name = normalize_bronze_column_name(dimension.get("column"))
            if not input_item or not add_output(input_item, dimension.get("column"), target_name, "IDENTITY"):
                reject("FACT", name, "MISSING_SOURCE_FIELD", f"Gold dimension field {dimension.get('column')} is invalid or duplicated.")
                invalid_field = True
                break
            keys.append(target_name)
        if invalid_field:
            continue
        if isinstance(time_column, dict):
            time_input = source(time_column.get("table") or measure.get("table"))
            grain = str((raw.get("time") or {}).get("grain") or "month").upper()
            if grain not in {"DAY", "WEEK", "MONTH", "QUARTER", "YEAR"}:
                reject("FACT", name, "INVALID_GRAIN", f"Unsupported Gold time grain: {grain}.")
                continue
            time_row = source_column(time_input, time_column.get("column")) if time_input else None
            if not time_row or type_family(time_row.get("target_data_type")) != "TEMPORAL":
                reject("FACT", name, "INVALID_GRAIN", "The Gold time grain requires a temporal Silver column.")
                continue
            if not add_output(time_input, time_column.get("column"), "period_start", f"DATE_TRUNC_{grain}", "TIMESTAMP"):
                reject("FACT", name, "INVALID_GRAIN", "The Gold time grain column is not present in active Silver metadata.")
                continue
            keys.append("period_start")
        aggregation = str(measure.get("aggregation") or "SUM").upper()
        if aggregation not in {"SUM", "AVG", "MIN", "MAX", "COUNT"}:
            reject("FACT", name, "INVALID_AGGREGATION", f"Unsupported Gold aggregation: {aggregation}.")
            continue
        measure_row = source_column(measure_input, measure.get("column"))
        if aggregation != "COUNT" and not measure_row:
            reject("FACT", name, "MISSING_SOURCE_FIELD", "The KPI measure column is not present in active Silver metadata.")
            continue
        if aggregation == "COUNT" and not measure_row:
            measure_row = next(iter(measure_input["columns"].values()), None)
        measure_family = type_family((measure_row or {}).get("target_data_type"))
        if aggregation in {"SUM", "AVG"} and measure_family != "NUMERIC":
            reject("FACT", name, "INVALID_AGGREGATION", f"{aggregation} requires a numeric Silver measure.")
            continue
        if aggregation in {"MIN", "MAX"} and measure_family not in {"NUMERIC", "STRING", "TEMPORAL"}:
            reject("FACT", name, "INVALID_AGGREGATION", f"{aggregation} requires a comparable Silver measure.")
            continue
        target_measure_type = (
            "BIGINT"
            if aggregation == "COUNT"
            else str(measure_row.get("target_data_type") or "")
            if aggregation in {"MIN", "MAX"}
            else "DECIMAL(38,10)"
        )
        value_name = f"{normalize_bronze_column_name(kpi_name)}_value"
        if not measure_row or not add_output(
            measure_input,
            measure_row["target_column_name"],
            value_name,
            f"AGG_{aggregation}",
            target_measure_type,
        ):
            reject("FACT", name, "INVALID_AGGREGATION", "The KPI aggregation could not be represented by the approved mapping.")
            continue
        target = f"{target_prefix}.{name}"
        if target.casefold() in used_targets:
            reject("FACT", name, "DUPLICATE_GOLD_TARGET", f"More than one Gold object resolves to {target}.")
            continue
        created = selection.repository.upsert_silver_to_gold_draft(
            source_system_id=int(measure_input["object"]["source_system_id"]),
            target_gold_table=target,
            inputs=[item["input"] for item in inputs],
            columns=mapped,
            merge_keys=keys,
            join_rules=join_rules,
            definition={"artifact_kind": "FACT", "mapping": raw},
            build_order=20,
            write_mode="MERGE" if keys else "SNAPSHOT_REPLACE",
            validation_policy={
                "fail_on_missing_input": True,
                "fail_on_join_multiplier": True,
                "fail_on_schema_mismatch": True,
                "max_join_multiplier": float(os.getenv("ATHENA_GOLD_MAX_JOIN_MULTIPLIER", "1.05")),
            },
            allow_inactive_inputs=using_dbt_drafts,
        )
        used_targets.add(target.casefold())
        drafts.append({
            "artifact_kind": "FACT",
            "name": name,
            "gold_ingestion_object_id": int(created["ingestion_object"]["ingestion_object_id"]),
            "gold_ingestion_object_config_version": int(created["ingestion_object"]["config_version"]),
            "gold_ingestion_object_config_hash": str(created["ingestion_object"]["config_hash"]),
            "silver_to_gold_mapping_version": int(created["mapping_bundle"]["mapping_version"]),
            "silver_to_gold_mapping_hash": str(created["mapping_bundle"]["mapping_hash"]),
            "target_table": target,
        })
        objects.append(created["ingestion_object"])
        bundles.append(created["mapping_bundle"])

    materialization_status = "READY" if drafts else "SKIPPED_NOT_COMPUTABLE"
    if rejections:
        logger.warning(
            "Gold metadata validation rejected %d object(s); %d executable object(s) remain: %s",
            len(rejections),
            len(drafts),
            json.dumps(rejections, default=str),
        )
    if validation_warnings:
        logger.warning(
            "Gold metadata validation retained executable objects with %d warning(s): %s",
            len(validation_warnings),
            json.dumps(validation_warnings, default=str),
        )
    return {
        **state,
        "gold_metadata_drafts": drafts,
        "gold_transformation_objects": objects,
        "silver_to_gold_mapping_bundles": bundles,
        "gold_metadata_rejections": rejections,
        "gold_metadata_warnings": validation_warnings,
        "gold_metadata_materialization_status": materialization_status,
        "gold_catalog": gold_catalog,
        "gold_schema": f"{gold_catalog}.{gold_schema}" if platform == "databricks" else gold_schema,
    }


def submit_metadata_ddl_review(run_id: str) -> Dict[str, Any]:
    """Approve the generated DDL checkpoint and continue to Bronze generation."""
    checkpoint = load_checkpoint_state(run_id) or {"run_id": run_id}
    if is_run_aborted(run_id, checkpoint):
        return aborted_run_state(run_id, checkpoint)
    if not checkpoint.get("metadata_ddl_artifact") or not checkpoint.get("metadata_ddl_review"):
        raise RuntimeError("Metadata DDL review is not ready.")
    reviewed = {
        **checkpoint,
        "status": "RUNNING",
        "background_stage": None,
        "next_review_key": None,
        "metadata_ddl_review_status": "COMPLETED",
        "metadata_ddl_review": {
            **checkpoint["metadata_ddl_review"],
            "review_status": "APPROVED",
        },
        "resume_message": "Metadata DDL review completed. Bronze generation is starting.",
    }
    save_checkpoint_state_timed(run_id, reviewed, context="metadata_ddl_review:complete")
    return continue_database_pipeline(run_id, start_stage_key="bronze", state=reviewed)


def submit_gate4_review(
    run_id: str,
    action: str = "APPROVED",
    review_artifact: Optional[Dict[str, Any]] = None,
    checkpoint_state: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    checkpoint_state = checkpoint_state or load_checkpoint_state(run_id) or {"run_id": run_id}
    if is_run_aborted(run_id, checkpoint_state):
        return aborted_run_state(run_id, checkpoint_state)
    decision = str(action or "APPROVED").upper()
    current_review_artifact = review_artifact or checkpoint_state.get("bronze_review_artifact") or {}
    if decision != "APPROVED":
        checkpoint_state = _invalidate_generation_first_review_state(
            checkpoint_state,
            boundary="gate4",
        )
    final_state = {
        **checkpoint_state,
        "run_id": run_id,
        "bronze_review_decision": decision,
        "bronze_review_artifact": current_review_artifact,
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
        target_warehouse = str(final_state.get("target_warehouse") or "").lower()
        bronze_results = [
            item
            for item in final_state.get("bronze_generation_results") or []
            if isinstance(item, dict)
        ]
        if target_warehouse == "snowflake" and snowflake_dbt_enabled(final_state):
            from nodes.bronze_gen import sync_snowflake_dbt_bronze_review

            bronze_results = sync_snowflake_dbt_bronze_review(
                run_id,
                bronze_results,
                final_state["bronze_review_artifact"],
            )
        final_state["bronze_generation_results"] = _filter_bronze_results_by_gate4_review(
            bronze_results,
            final_state["bronze_review_artifact"],
        )
        if final_state.get("source_system_id") is not None:
            final_state = _activate_reviewed_bronze_metadata(_attach_bronze_execution_specs(final_state))
        if target_warehouse == "snowflake" and snowflake_dbt_enabled(final_state):
            if (
                str(final_state.get("dbt_deployment_mode") or "generate_only").lower() == "generate_and_deploy"
                and not generation_first_snowflake_dbt_flow(final_state)
            ):
                from services.snowflake_bronze_runtime import run_snowflake_bronze_scripts

                landing_state = {
                    **final_state,
                    "status": "RUNNING",
                    "background_stage": "bronze_code_execution",
                    "resume_message": "Landing approved source data in Snowflake before the native dbt build.",
                }
                save_checkpoint_state_timed(run_id, landing_state, context="snowflake_dbt_source_landing:running")
                try:
                    final_state = run_snowflake_bronze_scripts(
                        landing_state,
                        review_artifact=landing_state["bronze_review_artifact"],
                        approved_only=True,
                        load_only=True,
                    )
                except Exception as exc:
                    failed_state = {
                        **landing_state,
                        "status": "FAILED",
                        "failed_background_stage": "bronze_code_execution",
                        "snowflake_bronze_source_load_status": "FAILED",
                        "error": str(exc),
                    }
                    save_checkpoint_state_timed(run_id, failed_state, context="snowflake_dbt_source_landing:failed")
                    raise
            final_state.update(
                {
                    "snowflake_bronze_execution_status": "SKIPPED_DBT_CODEGEN_ONLY",
                    "background_stage": None,
                    "resume_message": "Bronze dbt models generated; continuing to Silver generation.",
                }
            )
        elif not generation_first_native_database_flow(final_state) and target_warehouse == "snowflake":
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
                if is_run_aborted(run_id, final_state):
                    return aborted_run_state(run_id, final_state)
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
        elif not generation_first_native_database_flow(final_state) and target_warehouse == "databricks":
            from services.databricks_runtime import databricks_bronze_execution_enabled, run_databricks_bronze_scripts

            if not databricks_bronze_execution_enabled():
                raise RuntimeError(
                    "Databricks Bronze execution is disabled; refusing to continue to merge-key review or Silver."
                )
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
                if is_run_aborted(run_id, final_state):
                    return aborted_run_state(run_id, final_state)
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
    if decision != "APPROVED":
        checkpoint_state = _invalidate_generation_first_review_state(
            checkpoint_state,
            boundary="silver_merge_key_review",
        )
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
        final_state["bronze_generation_results"] = _apply_reviewed_keys_to_bronze_results(
            [item for item in final_state.get("bronze_generation_results") or [] if isinstance(item, dict)],
            artifact,
        )
        if final_state.get("source_system_id") is not None:
            final_state = _materialize_bronze_to_silver_metadata(final_state, artifact)
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
        if str(final_state.get("source") or "").lower() in {"sftp", "adls_gen2"}:
            from sftp_nodes.review_gates import sftp_gate5_node
            from sftp_nodes.silver_code_generation import sftp_silver_code_generation_node

            silver_state = sftp_silver_code_generation_node(final_state)
            silver_status = str(silver_state.get("silver_generation_status") or "").upper()
            silver_items = ((silver_state.get("silver_review_artifact") or {}).get("items") or [])
            if silver_status not in {"COMPLETED", "PARTIAL"} or not silver_items:
                blocked_state = {
                    **silver_state,
                    "status": "FAILED",
                    "error": silver_state.get("silver_generation_error")
                    or "Silver generation did not produce a review artifact after merge-key approval.",
                }
                save_checkpoint_state(run_id, blocked_state)
                return blocked_state
            gate5_state = sftp_gate5_node(silver_state)
            save_checkpoint_state(run_id, gate5_state)
            return gate5_state
        return continue_database_pipeline(run_id, start_stage_key="silver", state=final_state)
    return final_state


def _materialize_bronze_to_silver_metadata(
    state: Dict[str, Any], review_artifact: Dict[str, Any], *, _selection: Any = None
) -> Dict[str, Any]:
    from services.metadata_contracts import normalize_bronze_column_name, validate_identifier
    from services.metadata_selection import validated_metadata_selection

    selection = _selection or validated_metadata_selection(state)
    if not selection:
        raise ValueError("Bronze-to-Silver metadata requires a valid target selection.")
    if _selection is None and callable(getattr(selection.repository, "unit_of_work", None)):
        with selection.repository.unit_of_work():
            return _materialize_bronze_to_silver_metadata(
                state, review_artifact, _selection=selection
            )
    platform = str(state.get("target_warehouse") or "").lower()
    if platform == "snowflake":
        silver_catalog = validate_identifier(os.getenv("SNOWFLAKE_SILVER_CATALOG", "ATHENA_DB"), label="Silver catalog")
        silver_schema = validate_identifier(os.getenv("SNOWFLAKE_SILVER_SCHEMA", "SILVER"), label="Silver schema")
    else:
        silver_catalog = validate_identifier(
            os.getenv("SILVER_CATALOG", os.getenv("BRONZE_CATALOG", "main")), label="Silver catalog"
        )
        silver_schema = validate_identifier(os.getenv("SILVER_SCHEMA", "silver"), label="Silver schema")
    reviewed_feeds = [feed for feed in review_artifact.get("feeds") or [] if isinstance(feed, dict)]
    bronze_results = [item for item in state.get("bronze_generation_results") or [] if isinstance(item, dict)]

    def result_identity(item: Dict[str, Any]) -> tuple[str, str, str]:
        return (
            str(item.get("database_name") or "").strip().casefold(),
            str(item.get("schema_name") or "").strip().casefold(),
            str(item.get("table") or item.get("table_name") or "").split(".")[-1].strip().casefold(),
        )

    def reviewed_feed_for(item: Dict[str, Any]) -> Dict[str, Any]:
        object_id = int(item.get("ingestion_object_id") or 0)
        by_object = [feed for feed in reviewed_feeds if int(feed.get("ingestion_object_id") or 0) == object_id]
        if len(by_object) == 1:
            return by_object[0]
        identity = result_identity(item)
        exact = [
            feed for feed in reviewed_feeds
            if (
                str(feed.get("database_name") or "").strip().casefold(),
                str(feed.get("schema_name") or "").strip().casefold(),
                str(feed.get("table") or feed.get("table_name") or feed.get("entity") or "")
                .split(".")[-1].strip().casefold(),
            ) == identity
        ]
        if len(exact) == 1:
            return exact[0]
        same_leaf_results = [candidate for candidate in bronze_results if result_identity(candidate)[2] == identity[2]]
        leaf = [
            feed for feed in reviewed_feeds
            if not feed.get("database_name") and not feed.get("schema_name")
            and str(feed.get("table") or feed.get("table_name") or feed.get("entity") or "")
            .split(".")[-1].strip().casefold() == identity[2]
        ]
        if len(same_leaf_results) == 1 and len(leaf) == 1:
            return leaf[0]
        raise ValueError(
            f"Silver merge-key review does not identify ingestion object {object_id} unambiguously."
        )

    reviewed_bronze = [(bronze, reviewed_feed_for(bronze)) for bronze in bronze_results]
    results = []
    bundles = []
    objects = []
    dbt_codegen = snowflake_dbt_enabled(state)
    pending_dbt = [
        item for item in bronze_results
        if item.get("metadata_activation_status") == "PENDING_FINAL_DBT_PACKAGE"
    ]
    if pending_dbt and (not dbt_codegen or len(pending_dbt) != len(bronze_results)):
        raise RuntimeError("Snowflake dbt Bronze metadata cannot mix draft and active package inputs.")
    using_dbt_drafts = bool(pending_dbt)
    bronze_ids = [int(item.get("ingestion_object_id") or 0) for item in bronze_results]
    bronze_object_refs = [{
        "ingestion_object_id": int(item["ingestion_object_id"]),
        "config_version": int(item["ingestion_object_config_version"]),
    } for item in bronze_results] if using_dbt_drafts else []
    load_objects = getattr(selection.repository, "get_ingestion_objects", None)
    reviewed_bronze_objects = (
        load_objects(bronze_object_refs, require_active=False)
        if using_dbt_drafts and callable(load_objects)
        else {}
    )
    load_active = getattr(selection.repository, "get_active_ingestion_objects", None)
    active_bronze = load_active(bronze_ids) if not using_dbt_drafts and callable(load_active) else {}
    bronze_bundle_refs = []
    for bronze in bronze_results:
        object_id = int(bronze.get("ingestion_object_id") or 0)
        source_object = (
            reviewed_bronze_objects.get((object_id, int(bronze["ingestion_object_config_version"])))
            if using_dbt_drafts
            else active_bronze.get(object_id) or selection.repository.get_active_ingestion_object(object_id)
        )
        if not source_object:
            raise ValueError(f"Reviewed Bronze ingestion object not found: {object_id}")
        bronze_bundle_refs.append({
            "ingestion_object_id": object_id,
            "processing_stage": "SOURCE_TO_BRONZE",
            "mapping_version": int(bronze["mapping_version"]),
            "expected_hash": str(bronze["mapping_hash"]),
            "expected_target": str(bronze.get("target_table") or source_object["target_bronze_table"]),
            "require_active": None if using_dbt_drafts else True,
        })
    load_bundles = getattr(selection.repository, "get_mapping_bundles", None)
    active_source_bundles = load_bundles(bronze_bundle_refs) if callable(load_bundles) else {}
    for bronze, feed in reviewed_bronze:
        object_id = int(bronze.get("ingestion_object_id") or 0)
        source_object = (
            reviewed_bronze_objects.get((object_id, int(bronze["ingestion_object_config_version"])))
            if using_dbt_drafts
            else active_bronze.get(object_id) or selection.repository.get_active_ingestion_object(object_id)
        )
        if not source_object:
            raise ValueError(f"Reviewed Bronze ingestion object not found: {object_id}")
        mapping_version = int(bronze["mapping_version"])
        source_mapping = active_source_bundles.get((object_id, "SOURCE_TO_BRONZE", mapping_version)) or selection.repository.get_mapping_bundle(
            ingestion_object_id=object_id,
            processing_stage="SOURCE_TO_BRONZE",
            mapping_version=mapping_version,
            expected_hash=str(bronze["mapping_hash"]),
            expected_target=str(bronze.get("target_table") or source_object["target_bronze_table"]),
            require_active=None if using_dbt_drafts else True,
        )
        table_name = str(bronze.get("table") or "")
        merge_keys = [
            normalize_bronze_column_name(key)
            for key in feed.get("merge_keys") or feed.get("primary_keys") or []
            if str(key or "").strip()
        ]
        if not merge_keys:
            raise ValueError(f"Silver merge keys were not approved for {table_name}.")
        columns = []
        for mapping in source_mapping["mappings"]:
            bronze_name = str(mapping.get("target_column_name") or "")
            target_type = str(mapping.get("target_data_type") or "")
            columns.append({
                "source_field_path": bronze_name,
                "source_data_type": target_type,
                "target_column_name": normalize_bronze_column_name(bronze_name),
                "target_data_type": target_type,
                "is_nullable": mapping.get("is_nullable", True),
                "ordinal_position": mapping.get("ordinal_position"),
                "transformation_rule": "TRIM_CAST"
                if re.match(r"^(?:VAR)?CHAR|^STRING|^TEXT", target_type, re.IGNORECASE)
                else "CAST",
            })
        target_table = f"{silver_catalog}.{silver_schema}.silver_{validate_identifier(table_name, label='Silver table')}"
        created = selection.repository.upsert_bronze_to_silver_draft(
            source_system_id=int(source_object["source_system_id"]),
            source_object=source_object,
            source_mapping=source_mapping,
            target_silver_table=target_table,
            merge_keys=merge_keys,
            columns=columns,
            allow_inactive_source=using_dbt_drafts,
        )
        transformation = created["ingestion_object"]
        bundle = created["mapping_bundle"]
        objects.append(transformation)
        bundles.append(bundle)
        results.append({
            **bronze,
            "silver_ingestion_object_id": int(transformation["ingestion_object_id"]),
            "silver_ingestion_object_config_version": int(transformation["config_version"]),
            "silver_ingestion_object_config_hash": str(transformation["config_hash"]),
            "bronze_to_silver_mapping_version": int(bundle["mapping_version"]),
            "bronze_to_silver_mapping_hash": str(bundle["mapping_hash"]),
            "target_silver_table": target_table,
        })
    return {
        **state,
        "bronze_generation_results": results,
        "silver_transformation_objects": objects,
        "bronze_to_silver_mapping_bundles": bundles,
        "silver_catalog": silver_catalog,
        "silver_schema": silver_schema,
    }


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
    if is_run_aborted(run_id, checkpoint_state):
        return aborted_run_state(run_id, checkpoint_state)
    decision = str(action or "APPROVED").upper()
    current_review_artifact = review_artifact or checkpoint_state.get("silver_review_artifact") or {}
    if decision != "APPROVED":
        checkpoint_state = _invalidate_generation_first_review_state(
            checkpoint_state,
            boundary="gate5",
        )
    final_state = {
        **checkpoint_state,
        "run_id": run_id,
        "silver_review_decision": decision,
        "silver_review_artifact": current_review_artifact,
        "gate5": {"gate": "gate5", "status": "COMPLETED", "decision": decision},
    }

    if decision == "REJECTED":
        final_state["status"] = "FAILED"
        final_state["error"] = "Gate 5 rejected Silver review artifact"
    elif decision == "REGENERATE":
        final_state["status"] = "REGENERATE_REQUIRED"
    elif decision == "APPROVED":
        final_state["status"] = "RUNNING"
        target_warehouse = str(final_state.get("target_warehouse") or "").lower()
        silver_results = [
            item
            for item in final_state.get("silver_generation_results") or []
            if isinstance(item, dict)
        ]
        if target_warehouse == "snowflake" and snowflake_dbt_enabled(final_state):
            from nodes.silver_gen import sync_snowflake_dbt_silver_review

            silver_results = sync_snowflake_dbt_silver_review(
                run_id,
                silver_results,
                final_state["silver_review_artifact"],
            )
        selected_silver_results = _filter_silver_results_by_gate5_review(
            silver_results,
            final_state["silver_review_artifact"],
        )
        final_state["silver_generation_results"] = selected_silver_results
        if any(result.get("silver_ingestion_object_id") is not None for result in selected_silver_results):
            final_state = _activate_reviewed_silver_metadata(_attach_silver_execution_specs(final_state))
            selected_silver_results = final_state["silver_generation_results"]
        final_state["gold_generation_contract"] = _filter_gold_contract_by_silver_results(
            final_state.get("gold_generation_contract") or {},
            selected_silver_results,
        )
        if any(result.get("silver_ingestion_object_id") is not None for result in selected_silver_results):
            final_state = _materialize_silver_to_gold_metadata(final_state)
        if target_warehouse == "snowflake" and snowflake_dbt_enabled(final_state):
            final_state.update(
                {
                    "snowflake_silver_execution_status": "SKIPPED_DBT_CODEGEN_ONLY",
                    "background_stage": None,
                    "resume_message": "Silver dbt models generated; continuing to Gold generation.",
                }
            )
        elif not generation_first_native_database_flow(final_state) and target_warehouse == "snowflake":
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
                if is_run_aborted(run_id, final_state):
                    return aborted_run_state(run_id, final_state)
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
        elif not generation_first_native_database_flow(final_state) and target_warehouse == "databricks":
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
                    if is_run_aborted(run_id, final_state):
                        return aborted_run_state(run_id, final_state)
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
            "gold_metadata_drafts": final_state.get("gold_metadata_drafts") or [],
            "gold_metadata_rejections": final_state.get("gold_metadata_rejections") or [],
            "gold_metadata_warnings": final_state.get("gold_metadata_warnings") or [],
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
        dbt_codegen = snowflake_dbt_enabled(final_state)
        final_state.update(
            {
                "status": "HITL_WAIT",
                "background_stage": None,
                "next_gate": None,
                "next_review_key": "gold_review",
                "gold_review_artifact": {
                    "items": [item for item in result.get("gold_generation_results") or [] if isinstance(item, dict)],
                },
                "resume_message": (
                    "Gold Review is pending. Review generated Gold dbt models before finalizing the project."
                    if dbt_codegen
                    else "Gold Review is pending. Review generated Gold scripts before execution."
                ),
            }
        )
    save_checkpoint_state(run_id, final_state)
    return final_state


NATIVE_EXECUTION_STAGES = (
    ("bronze", "bronze_code_execution"),
    ("silver", "silver_code_execution"),
    ("gold", "gold_code_execution"),
)


def _metadata_runtime_object_ids(state: Dict[str, Any]) -> List[int]:
    return sorted({
        int(item.get(object_key) or 0)
        for result_key, object_key in (
            ("bronze_generation_results", "ingestion_object_id"),
            ("silver_generation_results", "silver_ingestion_object_id"),
            ("gold_generation_results", "gold_ingestion_object_id"),
        )
        for item in state.get(result_key) or []
        if isinstance(item, dict)
        and item.get("metadata_activation_status") == "ACTIVE"
        and int(item.get(object_key) or 0) > 0
    })


def _execute_metadata_setup(state: Dict[str, Any]) -> Dict[str, Any]:
    """Execute the reviewed DDL and deploy missing runtime config versions."""
    from services.metadata_contracts import file_sha256, split_sql_statements
    from services.metadata_repository import metadata_repository_for_target
    from services.metadata_selection import validated_metadata_selection
    from services.source_connection_validation import validate_deployment_database_binding
    from utilis.generated_code_paths import resolve_generated_artifact_uri

    artifact = state.get("metadata_ddl_artifact") or {}
    if not artifact:
        # ponytail: in-flight generation_first_v1 runs created before this stage
        # get one execution-boundary migration path; new runs always generate it
        # before Bronze and persist the same artifact contract.
        state = _run_database_metadata_ddl_stage(state)
        artifact = state.get("metadata_ddl_artifact") or {}
    artifact_uri = str(artifact.get("artifact_uri") or "")
    artifact_path = resolve_generated_artifact_uri(artifact_uri)
    if file_sha256(artifact_path) != str(artifact.get("artifact_hash") or ""):
        raise RuntimeError("Metadata DDL artifact failed its SHA-256 verification.")

    platform = str(state.get("target_warehouse") or "").strip().lower()
    environment = str(state.get("target_environment") or "").strip()
    repository = metadata_repository_for_target(platform=platform, environment=environment)
    running_state = {
        **state,
        "status": "RUNNING",
        "background_stage": "metadata_setup_execution",
        "next_stage_key": "metadata_setup_execution",
        "next_stage_label": "Metadata Setup Execution",
        "metadata_setup_execution_status": "RUNNING",
        "resume_message": "Creating and validating target metadata before layer execution.",
    }
    save_checkpoint_state_timed(
        str(state.get("run_id") or ""), running_state, context="metadata_setup_execution:running"
    )

    for statement in split_sql_statements(artifact_path.read_text(encoding="utf-8")):
        repository.execute(statement)
    repository.preflight()

    design = validated_metadata_selection(state)
    if not design:
        raise ValueError("Metadata setup requires an application design metadata selection.")
    object_ids = _metadata_runtime_object_ids(state)
    if not object_ids:
        raise RuntimeError("Metadata setup found no approved active ingestion objects.")
    active_objects = list(design.repository.get_active_ingestion_objects(object_ids).values())
    id_parameters = {f"object_id_{index}": object_id for index, object_id in enumerate(object_ids)}
    mappings = design.repository.query(
        f"SELECT * FROM {design.repository.table('cfg_mapping')} "
        "WHERE active_flag = :active_flag AND is_current = :is_current AND ingestion_object_id IN ("
        + ", ".join(f":object_id_{index}" for index in range(len(object_ids)))
        + ")",
        {**id_parameters, "active_flag": True, "is_current": True},
    )
    if not mappings:
        raise RuntimeError("Metadata setup found no approved active mapping rows.")

    with repository.unit_of_work():
        repository.upsert_source_system(design.source_system)
        connection = repository.upsert_connection_draft(design.connection)
        repository.validate_and_activate_connection(
            int(connection["connection_id"]),
            int(connection["config_version"]),
            lambda payload: validate_deployment_database_binding(payload, target_platform=platform),
        )
        repository.deploy_configuration_snapshot(
            ingestion_objects=active_objects,
            mappings=mappings,
        )

    completed_state = {
        **running_state,
        "background_stage": None,
        "last_completed_stage_key": "metadata_setup_execution",
        "last_completed_stage_label": "Metadata Setup Execution",
        "next_stage_key": "bronze_code_execution" if platform != "snowflake" or not snowflake_dbt_enabled(state) else "gold_code_execution",
        "next_stage_label": "Bronze Target Execution" if platform != "snowflake" or not snowflake_dbt_enabled(state) else "Code Execution",
        "metadata_setup_execution_status": "COMPLETED",
        "metadata_setup_artifact_hash": str(artifact["artifact_hash"]),
        "resume_message": "Target metadata is ready. Starting approved layer execution.",
    }
    save_checkpoint_state_timed(
        str(state.get("run_id") or ""), completed_state, context="metadata_setup_execution:complete"
    )
    return completed_state


def _enqueue_metadata_native_runtime(state: Dict[str, Any]) -> Dict[str, Any]:
    from services.metadata_contracts import canonical_json_hash
    from services.metadata_selection import validated_target_metadata_selection

    selection = validated_target_metadata_selection(state)
    if not selection:
        raise ValueError("Metadata runtime queueing requires a valid target selection.")
    requested_by = str(state.get("requested_by") or state.get("user_email") or "design-pipeline")
    queued = []
    batch_logical_work_id = canonical_json_hash({
        "design_run_id": str(state.get("run_id") or ""),
        "target_platform": selection.repository.context.platform,
        "target_environment": selection.repository.context.environment,
        "work_scope": state.get("runtime_work_scope") or {},
    })
    # Runtime queues only roots; successful workers release metadata-pinned dependants.
    layers = (("SOURCE_TO_BRONZE", "bronze_generation_results", "ingestion_object_id", 300),)
    requests = []
    for stage, result_key, object_key, priority in layers:
        for result in state.get(result_key) or []:
            if not isinstance(result, dict) or result.get("metadata_activation_status") != "ACTIVE":
                continue
            object_id = int(result.get(object_key) or 0)
            work_scope = {
                "design_run_id": str(state.get("run_id") or ""),
                "processing_stage": stage,
                "target_table": result.get("target_table"),
            }
            requests.append({
                "ingestion_object_id": object_id, "trigger_type": "MANUAL",
                "work_scope": work_scope, "requested_by": requested_by, "priority": priority,
                "logical_work_id": batch_logical_work_id, "processing_stage": stage,
            })
    enqueue_many = getattr(selection.repository, "enqueue_work_batch", None)
    items = enqueue_many(requests) if callable(enqueue_many) else [
        selection.repository.enqueue_work(**{
            key: value for key, value in request.items() if key != "processing_stage"
        })
        for request in requests
    ]
    for request, item in zip(requests, items):
            object_id = int(request["ingestion_object_id"])
            queued.append({
                "queue_id": int(item["queue_id"]),
                "ingestion_object_id": object_id,
                "processing_stage": request["processing_stage"],
                "queue_status": str(item.get("queue_status") or ""),
                "logical_work_id": str(item.get("logical_work_id") or ""),
            })
    if not queued:
        raise RuntimeError("No active metadata artifacts were available for runtime queueing.")
    queued_state = {
        **state,
        "status": "RUNTIME_QUEUED",
        "execution_ready": False,
        "awaiting_stage_confirmation": False,
        "stage_confirmation": None,
        "background_stage": None,
        "metadata_runtime_queue": queued,
        "resume_message": f"Queued {len(queued)} metadata runtime work item(s) on the selected target.",
    }
    save_checkpoint_state_timed(str(state.get("run_id") or ""), queued_state, context="metadata_runtime:queued")
    return queued_state


def _metadata_native_progress_state(
    state: Dict[str, Any], completed: List[Dict[str, Any]]
) -> tuple[Dict[str, Any], bool]:
    """Project child runtime outcomes onto the design run watched by the UI."""
    layer_specs = (
        ("bronze", "bronze_generation_results", "ingestion_object_id"),
        ("silver", "silver_generation_results", "silver_ingestion_object_id"),
        ("gold", "gold_generation_results", "gold_ingestion_object_id"),
    )
    successful_ids = {
        int(item.get("ingestion_object_id") or 0)
        for item in completed
        if str(item.get("status") or "").upper() in {"SUCCESS", "RECOVERED_SUCCESS"}
    }
    expected_by_layer = {
        layer: {
            int(item.get(object_key) or 0)
            for item in state.get(result_key) or []
            if isinstance(item, dict)
            and item.get("metadata_activation_status") == "ACTIVE"
            and int(item.get(object_key) or 0) > 0
        }
        for layer, result_key, object_key in layer_specs
    }
    completion = {
        layer: not expected or expected.issubset(successful_ids)
        for layer, expected in expected_by_layer.items()
    }
    active_layer = next(
        (layer for layer, _result_key, _object_key in layer_specs if not completion[layer]),
        None,
    )
    if active_layer is None:
        return {**state, "metadata_runtime_results": list(completed)}, True

    labels = {
        "bronze": "Bronze Target Execution",
        "silver": "Silver Target Execution",
        "gold": "Gold Target Execution",
    }
    updated = {**state, "status": "RUNNING", "metadata_runtime_results": list(completed)}
    last_completed_layer = None
    for layer, _result_key, _object_key in layer_specs:
        if layer == active_layer:
            break
        if expected_by_layer[layer] and completion[layer]:
            last_completed_layer = layer
    for layer, _result_key, _object_key in layer_specs:
        expected = expected_by_layer[layer]
        completed_count = len(expected & successful_ids)
        status = "COMPLETED" if completion[layer] else "RUNNING" if layer == active_layer else "PENDING"
        progress = {
            "platform": "snowflake",
            "layer": layer,
            "stage_key": f"{layer}_code_execution",
            "status": status,
            "total_count": len(expected),
            "completed_count": completed_count,
            "message": f"Snowflake {layer.capitalize()} target execution: {completed_count}/{len(expected)} completed.",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        updated[f"snowflake_{layer}_execution_status"] = status
        updated[f"snowflake_{layer}_execution_progress"] = progress
    active_progress = updated[f"snowflake_{active_layer}_execution_progress"]
    updated.update(
        {
            "background_stage": f"{active_layer}_code_execution",
            "external_execution": active_progress,
            "next_stage_key": f"{active_layer}_code_execution",
            "next_stage_label": labels[active_layer],
            "resume_message": active_progress["message"],
        }
    )
    if last_completed_layer:
        updated.update(
            {
                "last_completed_stage_key": f"{last_completed_layer}_code_execution",
                "last_completed_stage_label": labels[last_completed_layer],
            }
        )
    return updated, False


def _execute_queued_metadata_native_runtime(state: Dict[str, Any]) -> Dict[str, Any]:
    from services.metadata_runtime_worker import process_metadata_work_batch, process_next_metadata_work
    from services.metadata_selection import validated_target_metadata_selection

    selection = validated_target_metadata_selection(state)
    if not selection:
        raise ValueError("Metadata runtime execution requires a valid target selection.")
    logical_ids = {
        str(item.get("logical_work_id") or "").strip()
        for item in state.get("metadata_runtime_queue") or []
        if isinstance(item, dict) and str(item.get("logical_work_id") or "").strip()
    }
    if len(logical_ids) != 1:
        raise RuntimeError("Metadata runtime queue must contain exactly one logical work identity.")
    logical_work_id = logical_ids.pop()
    worker_id = f"design:{state.get('run_id')}:{uuid.uuid4()}"
    completed: List[Dict[str, Any]] = []
    progress_state = dict(state)
    target = str(state.get("target_warehouse") or "").lower()

    while True:
        worker_error: Optional[BaseException] = None
        if target == "databricks":
            batch = process_metadata_work_batch(
                selection.repository,
                worker_id=worker_id,
                logical_work_id=logical_work_id,
                progress_state=progress_state,
            )
            if batch is None:
                break
            progress_state = dict(batch.get("progress_state") or progress_state)
            progress_state.pop("_metadata_runtime_scripts", None)
            progress_state.pop("metadata_runtime_context", None)
            outcomes = batch.get("outcomes") or []
        else:
            try:
                worker_count = max(
                    1,
                    min(8, int(os.getenv("ATHENA_SNOWFLAKE_NATIVE_WORKERS", "4"))),
                )
            except ValueError:
                worker_count = 4
            # ponytail: queue claims remain atomic and per-object; only independent ready work runs concurrently.
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                futures = [
                    executor.submit(
                        process_next_metadata_work,
                        selection.repository,
                        worker_id=f"{worker_id}:{index}",
                        logical_work_id=logical_work_id,
                    )
                    for index in range(worker_count)
                ]
                outcomes = []
                for future in futures:
                    try:
                        outcome = future.result()
                    except BaseException as exc:
                        worker_error = worker_error or exc
                    else:
                        if outcome is not None:
                            outcomes.append(outcome)
            if not outcomes and not worker_error:
                break
        for outcome in outcomes:
            queue_item = outcome.get("queue") or {}
            runtime_run = outcome.get("run") or {}
            completed.append({
                "queue_id": queue_item.get("queue_id"),
                "ingestion_object_id": queue_item.get("ingestion_object_id"),
                "runtime_run_id": runtime_run.get("run_id"),
                "status": outcome.get("status"),
            })
        if target == "snowflake":
            if worker_error:
                # Project durable queue successes before surfacing a downstream
                # validation failure; the UI must not retain an older 4/7 view.
                completed = [
                    {
                        "queue_id": item.get("queue_id"),
                        "ingestion_object_id": item.get("ingestion_object_id"),
                        "runtime_run_id": item.get("run_id"),
                        "status": "SUCCESS",
                    }
                    for item in selection.repository.queue_items_for_logical_work(logical_work_id)
                    if str(item.get("queue_status") or "").upper() == "SUCCESS"
                ]
            progress_state, execution_complete = _metadata_native_progress_state(
                progress_state, completed
            )
            if not execution_complete or worker_error:
                save_checkpoint_state_timed(
                    str(state.get("run_id") or ""),
                    progress_state,
                    context="metadata_runtime:progress",
                )
        if worker_error:
            raise worker_error

    queue_items = selection.repository.queue_items_for_logical_work(logical_work_id)
    expected_object_ids = {
        int(item.get(object_key) or 0)
        for result_key, object_key in (
            ("bronze_generation_results", "ingestion_object_id"),
            ("silver_generation_results", "silver_ingestion_object_id"),
            ("gold_generation_results", "gold_ingestion_object_id"),
        )
        for item in state.get(result_key) or []
        if isinstance(item, dict)
        and item.get("metadata_activation_status") == "ACTIVE"
        and int(item.get(object_key) or 0) > 0
    }
    successful_object_ids = {
        int(item.get("ingestion_object_id") or 0)
        for item in queue_items
        if str(item.get("queue_status") or "").upper() == "SUCCESS"
    }
    failed = [item for item in queue_items if str(item.get("queue_status") or "").upper() == "FAILED"]
    incomplete = [
        item for item in queue_items
        if str(item.get("queue_status") or "").upper() not in {"SUCCESS", "FAILED"}
    ]
    gold_results = [
        item
        for item in state.get("gold_generation_results") or []
        if isinstance(item, dict)
        and item.get("metadata_activation_status") == "ACTIVE"
        and int(item.get("gold_ingestion_object_id") or 0) > 0
    ]
    gold_by_id = {
        int(item["gold_ingestion_object_id"]): item
        for item in gold_results
    }
    gold_object_ids = set(gold_by_id)
    failed_ids = {int(item.get("ingestion_object_id") or 0) for item in failed}
    failed_gold_ids = failed_ids & gold_object_ids
    failed_non_gold = [
        item for item in failed
        if int(item.get("ingestion_object_id") or 0) not in gold_object_ids
    ]
    missing = expected_object_ids - successful_object_ids - failed_gold_ids
    if failed_non_gold:
        raise RuntimeError(
            "Metadata target execution failed for queue item(s): "
            + ", ".join(str(item.get("queue_id")) for item in failed_non_gold)
        )
    if incomplete or missing:
        raise RuntimeError(
            "Metadata target execution did not reach a terminal success state for every active artifact."
        )

    successful_gold_ids = gold_object_ids & successful_object_ids
    planned_fact_ids = {
        object_id for object_id, item in gold_by_id.items()
        if str(item.get("artifact_kind") or "FACT").upper() == "FACT"
    }
    planned_dimension_ids = gold_object_ids - planned_fact_ids
    gold_success_ratio = (
        len(successful_gold_ids) / len(gold_object_ids)
        if gold_object_ids
        else 1.0
    )
    gold_completed_with_warnings = bool(failed_gold_ids)
    if gold_completed_with_warnings and (
        not successful_gold_ids or gold_success_ratio < _gold_partial_success_ratio()
        or (planned_fact_ids and not (planned_fact_ids & successful_gold_ids))
        or (planned_dimension_ids and not (planned_dimension_ids & successful_gold_ids))
    ):
        failed_queue_ids = [
            str(item.get("queue_id"))
            for item in failed
            if int(item.get("ingestion_object_id") or 0) in failed_gold_ids
        ]
        raise RuntimeError(
            "Gold metadata execution did not meet the approved success-coverage threshold; failed queue item(s): "
            + ", ".join(failed_queue_ids)
        )

    def count_gold(kind: str, object_ids: set[int]) -> int:
        return sum(
            1
            for object_id in object_ids
            if str(gold_by_id[object_id].get("artifact_kind") or "FACT").upper() == kind
        )

    gold_summary = {
        "status": "COMPLETED_WITH_WARNINGS" if gold_completed_with_warnings else "COMPLETED",
        "planned_count": len(gold_object_ids),
        "successful_count": len(successful_gold_ids),
        "failed_count": len(failed_gold_ids),
        "successful_fact_count": count_gold("FACT", successful_gold_ids),
        "successful_dimension_count": count_gold("DIMENSION", successful_gold_ids),
        "failed_object_ids": sorted(failed_gold_ids),
        "success_ratio": round(gold_success_ratio, 4),
    }
    gold_message = (
        f"Gold completed with warnings: {len(successful_gold_ids)}/{len(gold_object_ids)} tables succeeded; "
        f"{len(failed_gold_ids)} table(s) remain failed for retry."
        if gold_completed_with_warnings
        else f"Gold target execution completed: {len(successful_gold_ids)}/{len(gold_object_ids)} tables succeeded."
    )

    final_state = {
        **progress_state,
        "status": (
            "RUNNING"
            if revised_metadata_database_flow(progress_state)
            else "PIPELINE_COMPLETED"
        ),
        "execution_ready": False,
        "background_stage": None,
        "failed_background_stage": None,
        "last_completed_stage_key": "gold_code_execution",
        "last_completed_stage_label": "Gold Target Execution",
        "next_stage_key": None,
        "next_stage_label": None,
        "metadata_runtime_results": completed,
        f"{target}_bronze_execution_status": "COMPLETED",
        f"{target}_silver_execution_status": "COMPLETED",
        f"{target}_gold_execution_status": gold_summary["status"],
        "gold_execution_summary": gold_summary,
        "external_execution": {
            "platform": target,
            "layer": "gold",
            "stage_key": "gold_code_execution",
            "status": gold_summary["status"],
            "total_count": len(gold_object_ids),
            "completed_count": len(successful_gold_ids),
            "failed_count": len(failed_gold_ids),
            "message": gold_message,
        },
        "resume_message": gold_message,
    }
    if revised_metadata_database_flow(final_state):
        return _complete_run_with_report(
            final_state,
            running_message=f"{gold_message} Generating the pipeline run report.",
            completed_message=f"{gold_message} Run report completed.",
            context="metadata_runtime",
        )
    save_checkpoint_state_timed(
        str(state.get("run_id") or ""),
        final_state,
        context="metadata_runtime:complete",
    )
    return final_state


def _native_execution_completed(state: Dict[str, Any], target_warehouse: str, layer: str) -> bool:
    status = state.get(f"{target_warehouse}_{layer}_execution_status")
    if target_warehouse == "snowflake":
        return (
            str(status or "").upper() in SNOWFLAKE_COMPLETED_EXECUTION_STATUSES
            if layer == "gold"
            else str(status or "").upper() == "COMPLETED"
        )
    return _status_completed(status)


def _database_native_execution_validation_errors(state: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    source = str(state.get("source") or "database").lower()
    target_warehouse = str(state.get("target_warehouse") or "").lower()
    if source != "database":
        errors.append("source must be database")
    if not generation_first_native_database_flow(state):
        errors.append("run is not a generation-first native database flow")
    if target_warehouse not in {"databricks", "snowflake"}:
        errors.append("target warehouse must be Databricks or Snowflake")
    if snowflake_dbt_enabled(state):
        errors.append("Snowflake dbt execution uses its existing Gold Review finalization flow")

    review_decisions = {
        "Bronze Review": str(
            (state.get("gate4") or {}).get("decision")
            or state.get("bronze_review_decision")
            or ""
        ).upper(),
        "Silver Merge Key Review": str(state.get("silver_merge_key_review_decision") or "").upper(),
        "Silver Review": str(
            (state.get("gate5") or {}).get("decision")
            or state.get("silver_review_decision")
            or ""
        ).upper(),
        "Gold Review": str(state.get("gold_review_decision") or "").upper(),
    }
    errors.extend(
        f"{label} is not approved"
        for label, decision in review_decisions.items()
        if decision != "APPROVED"
    )
    errors.extend(
        f"{layer.capitalize()} generation artifacts are missing"
        for layer in ("bronze", "silver", "gold")
        if not [
            item
            for item in state.get(f"{layer}_generation_results") or []
            if isinstance(item, dict)
        ]
    )
    return errors


def execute_database_native_layers(
    run_id: str,
    *,
    state: Optional[Dict[str, Any]] = None,
    start_stage_key: str = "bronze_code_execution",
) -> Dict[str, Any]:
    working_state = dict(state or load_checkpoint_state(run_id) or {"run_id": run_id})
    working_state["run_id"] = run_id
    validation_errors = _database_native_execution_validation_errors(working_state)
    if validation_errors:
        raise RuntimeError(
            "Database target execution is not ready: " + "; ".join(validation_errors) + "."
        )
    metadata_runtime = bool(
        working_state.get("source_system_id") is not None
        and _metadata_runtime_object_ids(working_state)
    )
    if metadata_runtime:
        if revised_metadata_database_flow(working_state):
            try:
                working_state = _execute_metadata_setup(working_state)
            except Exception as exc:
                failed_state = {
                    **working_state,
                    "status": "FAILED",
                    "execution_ready": True,
                    "background_stage": "metadata_setup_execution",
                    "failed_background_stage": "metadata_setup_execution",
                    "next_stage_key": "metadata_setup_execution",
                    "next_stage_label": "Metadata Setup Execution",
                    "metadata_setup_execution_status": "FAILED",
                    "error": str(exc),
                    "resume_message": "Target metadata setup failed. Retry safely reuses the verified DDL and existing configuration versions.",
                }
                save_checkpoint_state_timed(
                    run_id, failed_state, context="metadata_setup_execution:failed"
                )
                raise
        return _execute_queued_metadata_native_runtime(
            _enqueue_metadata_native_runtime(working_state)
        )

    target_warehouse = str(working_state.get("target_warehouse") or "").lower()
    stage_keys = [stage_key for _, stage_key in NATIVE_EXECUTION_STAGES]
    if start_stage_key not in stage_keys:
        raise ValueError(f"Unsupported database execution stage: {start_stage_key}")
    start_index = stage_keys.index(start_stage_key)

    # ponytail: completed layer receipts are the idempotency boundary; a retry
    # resumes at the failed layer without rerunning successful target mutations.
    for prerequisite_layer, _ in NATIVE_EXECUTION_STAGES[:start_index]:
        if not _native_execution_completed(working_state, target_warehouse, prerequisite_layer):
            raise RuntimeError(
                f"{prerequisite_layer.capitalize()} execution must complete before "
                f"{NATIVE_EXECUTION_STAGES[start_index][0].capitalize()} execution can resume."
            )

    for index, (layer, stage_key) in enumerate(NATIVE_EXECUTION_STAGES[start_index:], start=start_index):
        if _native_execution_completed(working_state, target_warehouse, layer):
            continue
        if is_run_aborted(run_id, working_state):
            return aborted_run_state(run_id, working_state)

        execution_state = {
            **working_state,
            "status": "RUNNING",
            "execution_ready": True,
            "background_stage": stage_key,
            "next_stage_key": stage_key,
            "next_stage_label": f"{layer.capitalize()} Target Execution",
            "awaiting_stage_confirmation": False,
            "stage_confirmation": None,
            "failed_background_stage": None,
            "error": None,
            "resume_message": f"Executing approved {layer.capitalize()} scripts in {target_warehouse.capitalize()}.",
        }
        save_checkpoint_state_timed(run_id, execution_state, context=f"{stage_key}:running")

        try:
            if target_warehouse == "databricks":
                from services.databricks_runtime import (
                    run_databricks_bronze_scripts,
                    run_databricks_gold_scripts,
                    run_databricks_silver_scripts,
                )

                runners = {
                    "bronze": run_databricks_bronze_scripts,
                    "silver": run_databricks_silver_scripts,
                    "gold": run_databricks_gold_scripts,
                }
                result = runners[layer](
                    execution_state,
                    review_artifact=execution_state.get(f"{layer}_review_artifact") or {},
                    approved_only=True,
                )
            else:
                from services.snowflake_bronze_runtime import run_snowflake_bronze_scripts
                from services.snowflake_gold_runtime import run_snowflake_gold_scripts
                from services.snowflake_silver_runtime import run_snowflake_silver_scripts

                if layer == "bronze":
                    result = run_snowflake_bronze_scripts(
                        execution_state,
                        review_artifact=execution_state.get("bronze_review_artifact") or {},
                        approved_only=True,
                    )
                elif layer == "silver":
                    result = run_snowflake_silver_scripts(
                        execution_state,
                        review_artifact=execution_state.get("silver_review_artifact") or {},
                        approved_only=True,
                    )
                else:
                    result = run_snowflake_gold_scripts(execution_state)

            working_state = {**execution_state, **result, "run_id": run_id}
            if is_run_aborted(run_id, working_state):
                return aborted_run_state(run_id, working_state)
            if not _native_execution_completed(working_state, target_warehouse, layer):
                status = working_state.get(f"{target_warehouse}_{layer}_execution_status")
                raise RuntimeError(
                    f"{target_warehouse.capitalize()} {layer.capitalize()} execution "
                    f"did not complete (status={status or 'missing'})."
                )
        except Exception as exc:
            latest_checkpoint = load_checkpoint_state(run_id) or {}
            failed_state = {
                **execution_state,
                **working_state,
                **latest_checkpoint,
                "status": "FAILED",
                "execution_ready": True,
                "background_stage": stage_key,
                "failed_background_stage": stage_key,
                "next_stage_key": stage_key,
                "next_stage_label": f"{layer.capitalize()} Target Execution",
                "error": str(exc),
                "resume_message": (
                    f"{layer.capitalize()} target execution failed. "
                    "Retry resumes from this layer without regenerating approved code."
                ),
            }
            save_checkpoint_state_timed(run_id, failed_state, context=f"{stage_key}:failed")
            raise

        next_stage = NATIVE_EXECUTION_STAGES[index + 1] if index + 1 < len(NATIVE_EXECUTION_STAGES) else None
        working_state.update(
            {
                "status": "RUNNING" if next_stage else "PIPELINE_COMPLETED",
                "execution_ready": bool(next_stage),
                "background_stage": None,
                "last_completed_stage_key": stage_key,
                "last_completed_stage_label": f"{layer.capitalize()} Target Execution",
                "next_stage_key": next_stage[1] if next_stage else None,
                "next_stage_label": f"{next_stage[0].capitalize()} Target Execution" if next_stage else None,
                "stage_confirmation": None,
                f"{layer}_runtime_validation_status": "COMPLETED",
                "resume_message": (
                    f"{layer.capitalize()} target execution completed. "
                    f"Continuing to {next_stage[0].capitalize()} target execution."
                    if next_stage
                    else "Bronze, Silver, and Gold target execution completed."
                ),
            }
        )
        save_checkpoint_state_timed(run_id, working_state, context=f"{stage_key}:complete")
        if not next_stage and revised_metadata_database_flow(working_state):
            return _complete_run_with_report(
                working_state,
                running_message="Gold target execution completed. Generating the pipeline run report.",
                completed_message="Bronze, Silver, and Gold target execution and run report completed.",
                context="native_execution",
            )

    return working_state


def _database_dbt_execution_validation_errors(state: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    if not generation_first_snowflake_dbt_flow(state):
        errors.append("run is not a generation-first Snowflake dbt flow")
    if str(state.get("dbt_deployment_mode") or "generate_only").lower() != "generate_and_deploy":
        errors.append("Snowflake dbt deployment was not requested")

    review_decisions = {
        "Bronze Review": str(
            (state.get("gate4") or {}).get("decision")
            or state.get("bronze_review_decision")
            or ""
        ).upper(),
        "Silver Merge Key Review": str(state.get("silver_merge_key_review_decision") or "").upper(),
        "Silver Review": str(
            (state.get("gate5") or {}).get("decision")
            or state.get("silver_review_decision")
            or ""
        ).upper(),
        "Gold Review": str(state.get("gold_review_decision") or "").upper(),
    }
    errors.extend(
        f"{label} is not approved"
        for label, decision in review_decisions.items()
        if decision != "APPROVED"
    )
    errors.extend(
        f"{layer.capitalize()} generation artifacts are missing"
        for layer in ("bronze", "silver", "gold")
        if not [
            item
            for item in state.get(f"{layer}_generation_results") or []
            if isinstance(item, dict)
        ]
    )
    if str(state.get("snowflake_dbt_validation_status") or "").upper() != "STATIC_VALIDATED":
        errors.append("Snowflake dbt project has not passed static validation")
    if not str(state.get("snowflake_dbt_artifact_path") or "").strip():
        errors.append("finalized Snowflake dbt project is missing")
    if not str(state.get("snowflake_dbt_artifact_set_hash") or "").strip():
        errors.append("finalized Snowflake dbt project hash is missing")
    if state.get("source_system_id") is not None:
        package_hash = str(state.get("snowflake_dbt_artifact_set_hash") or "")
        for layer in ("bronze", "silver", "gold"):
            artifacts = [
                item for item in state.get(f"{layer}_generation_results") or []
                if isinstance(item, dict)
            ]
            if any(
                item.get("metadata_activation_status") != "ACTIVE"
                or str((item.get("execution_spec") or {}).get("engine") or "").upper() != "SNOWFLAKE_DBT"
                or str((item.get("execution_spec") or {}).get("dbt_package_hash") or "") != package_hash
                for item in artifacts
            ):
                errors.append(f"{layer.capitalize()} dbt metadata is not active for the finalized package")
    return errors


def _start_snowflake_dbt_control_attempt(
    state: Dict[str, Any], *, _selection: Any = None
) -> Optional[Dict[str, Any]]:
    """Create one fenced control attempt for the already-finalized dbt package."""
    if state.get("source_system_id") is None:
        return None
    from services.metadata_contracts import canonical_json_hash
    from services.metadata_selection import validated_target_metadata_selection

    if (
        _selection is None
        and str(state.get("target_warehouse") or "").strip()
        and revised_metadata_database_flow(state)
        and str(state.get("metadata_setup_execution_status") or "").upper() != "COMPLETED"
    ):
        state = _execute_metadata_setup(state)
    selection = _selection or validated_target_metadata_selection(state)
    if not selection:
        raise ValueError("Snowflake dbt execution requires a valid metadata selection.")
    if _selection is None and callable(getattr(selection.repository, "unit_of_work", None)):
        with selection.repository.unit_of_work():
            return _start_snowflake_dbt_control_attempt(state, _selection=selection)
    gold_ids = sorted({
        int(item.get("gold_ingestion_object_id") or 0)
        for item in state.get("gold_generation_results") or []
        if isinstance(item, dict)
        and item.get("metadata_activation_status") == "ACTIVE"
        and int(item.get("gold_ingestion_object_id") or 0) > 0
    })
    if not gold_ids:
        raise RuntimeError("Snowflake dbt execution has no active Gold metadata object.")
    package_object_ids = sorted({
        int(item.get(object_key) or 0)
        for layer, object_key in (
            ("bronze", "ingestion_object_id"),
            ("silver", "silver_ingestion_object_id"),
            ("gold", "gold_ingestion_object_id"),
        )
        for item in state.get(f"{layer}_generation_results") or []
        if isinstance(item, dict)
        and item.get("metadata_activation_status") == "ACTIVE"
        and int(item.get(object_key) or 0) > 0
    })
    package_hash = str(state.get("snowflake_dbt_artifact_set_hash") or "")
    logical_work_id = canonical_json_hash({
        "design_run_id": str(state.get("run_id") or ""),
        "target_platform": "snowflake",
        "execution_engine": "dbt",
        "package_hash": package_hash,
    })
    queue_item = selection.repository.enqueue_work(
        ingestion_object_id=gold_ids[0],
        trigger_type="MANUAL",
        work_scope={
            "design_run_id": str(state.get("run_id") or ""),
            "execution_unit": "SNOWFLAKE_DBT_PROJECT",
            "package_hash": package_hash,
            "ingestion_object_ids": package_object_ids,
        },
        requested_by=str(state.get("requested_by") or state.get("user_email") or "design-pipeline"),
        priority=100,
        logical_work_id=logical_work_id,
    )
    if str(queue_item.get("queue_status") or "").upper() == "SUCCESS":
        return {"repository": selection.repository, "queue": queue_item, "completed": True}

    worker_id = f"dbt:{state.get('run_id')}:{uuid.uuid4()}"
    lease_seconds = max(300, int(state.get("dbt_command_timeout_secs") or 900) + 300)
    claimed = selection.repository.claim_next_queue_item(
        worker_id=worker_id,
        lease_seconds=lease_seconds,
        logical_work_id=logical_work_id,
    )
    if not claimed:
        raise RuntimeError("The Snowflake dbt package is already claimed or is waiting for its retry window.")
    context = selection.repository.create_run_attempt(
        claimed, pipeline_name="snowflake_dbt", worker_id=worker_id
    )
    if not context.get("metadata_snapshot_matches", True):
        raise RuntimeError("The Snowflake dbt queue snapshot no longer matches active metadata.")
    return {
        "repository": selection.repository,
        "queue": claimed,
        "run": context["run"],
        "worker_id": worker_id,
        "lease_seconds": lease_seconds,
        "attempt_number": int(claimed.get("attempt_count") or 0),
        "error_stage": "READ",
        "completed": False,
    }


def _finish_snowflake_dbt_control_attempt(
    control: Optional[Dict[str, Any]], state: Dict[str, Any], *, _within_unit: bool = False
) -> None:
    if not control or control.get("completed"):
        return
    repository = control["repository"]
    if not _within_unit and callable(getattr(repository, "unit_of_work", None)):
        with repository.unit_of_work():
            return _finish_snowflake_dbt_control_attempt(
                control, state, _within_unit=True
            )
    queue_item = control["queue"]
    run = control["run"]
    worker_id = control["worker_id"]
    receipt = state.get("snowflake_dbt_execution") or {}
    receipt_status = str(receipt.get("status") or "").upper()
    if receipt_status not in {"COMPLETED", "COMPLETED_WITH_WARNINGS"}:
        raise RuntimeError("Snowflake dbt did not produce a completed execution receipt.")
    gold_summary = receipt.get("gold_execution_summary") or {}
    for failed_model in gold_summary.get("results") or []:
        if not isinstance(failed_model, dict) or failed_model.get("status") != "FAILED":
            continue
        repository.record_run_error(
            run=run,
            error_stage="WRITE",
            error=RuntimeError(str(failed_model.get("error") or "Gold dbt model failed.")),
            retryable=False,
            detail={
                "execution_unit": "SNOWFLAKE_DBT_MODEL",
                "model_name": failed_model.get("model_name"),
                "partial_success": True,
            },
            worker_id=worker_id,
        )
    package_hash = str(state.get("snowflake_dbt_artifact_set_hash") or "")
    repository.update_run_phase(
        str(run["run_id"]),
        "TARGET_WRITTEN",
        queue_id=int(queue_item["queue_id"]),
        worker_id=worker_id,
        target_write_id=f"dbt:{package_hash}",
        target_commit_status="COMMITTED",
        validation_status=("PASSED_WITH_WARNINGS" if receipt_status == "COMPLETED_WITH_WARNINGS" else "PASSED"),
        validation_summary_json=json.dumps(
            {
                "execution_unit": "SNOWFLAKE_DBT_PROJECT",
                "package_hash": package_hash,
                "receipt_status": receipt_status,
                "model_count": state.get("snowflake_dbt_model_count"),
                "gold_execution_summary": gold_summary,
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        watermark_commit_status="SKIPPED",
    )
    repository.begin_queue_finalization(
        queue_id=int(queue_item["queue_id"]), worker_id=worker_id
    )
    repository.finalize_successful_run(
        run_id=str(run["run_id"]),
        queue_id=int(queue_item["queue_id"]),
        worker_id=worker_id,
    )


def _fail_snowflake_dbt_control_attempt(
    control: Optional[Dict[str, Any]], error: BaseException, *, _within_unit: bool = False
) -> None:
    if not control or control.get("completed") or not control.get("run"):
        return
    retryable = isinstance(error, (ConnectionError, TimeoutError)) or type(error).__name__ in {
        "OperationalError", "InterfaceError"
    }
    repository = control["repository"]
    if not _within_unit and callable(getattr(repository, "unit_of_work", None)):
        with repository.unit_of_work():
            return _fail_snowflake_dbt_control_attempt(
                control, error, _within_unit=True
            )
    run = control["run"]
    repository.record_run_error(
        run=run,
        error_stage=str(control.get("error_stage") or "WRITE"),
        error=error,
        retryable=retryable,
        detail={"execution_unit": "SNOWFLAKE_DBT_PROJECT"},
        worker_id=control["worker_id"],
    )
    repository.finalize_failed_run(
        run=run,
        worker_id=control["worker_id"],
        retryable=retryable,
        message=str(error),
    )


def build_run_report(state: Dict[str, Any]) -> Dict[str, Any]:
    """Build a display-ready report exclusively from the persisted run state."""
    generated_at = datetime.now(timezone.utc).isoformat()
    enriched = state.get("enriched_metadata") if isinstance(state.get("enriched_metadata"), dict) else {}
    columns = [
        item for item in enriched.get("columns") or state.get("enriched_columns") or []
        if isinstance(item, dict)
    ]
    tables = [
        item for item in state.get("certified_tables") or state.get("nominated_tables") or []
        if isinstance(item, dict)
    ]
    kpis = [
        item for item in state.get("certified_kpis") or state.get("extracted_kpis") or state.get("kpis") or []
        if isinstance(item, dict)
    ]

    def table_name(item: Dict[str, Any]) -> str:
        return str(
            item.get("table_name")
            or item.get("table")
            or item.get("entity")
            or item.get("source_table")
            or "Unassigned"
        ).split(".")[-1]

    table_rows: Dict[str, Dict[str, Any]] = {}
    for table in tables:
        name = table_name(table)
        table_rows[name.casefold()] = {
            "name": name,
            "database": table.get("database_name") or table.get("database"),
            "schema": table.get("schema_name") or table.get("schema"),
            "columns": [],
        }
    for column in columns:
        name = table_name(column)
        row = table_rows.setdefault(
            name.casefold(),
            {
                "name": name,
                "database": column.get("database_name") or column.get("database"),
                "schema": column.get("schema_name") or column.get("schema"),
                "columns": [],
            },
        )
        row["columns"].append(
            {
                "name": column.get("column_name") or column.get("name") or "Unnamed column",
                "data_type": column.get("data_type") or column.get("type") or "UNKNOWN",
                "semantic_type": column.get("semantic_type") or "UNKNOWN",
                "is_key": bool(
                    column.get("is_join_key")
                    or column.get("is_primary_key")
                    or str(column.get("semantic_type") or "").upper() in {"ID", "SURROGATE_KEY"}
                ),
                "is_pii": bool(column.get("is_pii") or column.get("is_pii_candidate")),
                "is_measure": bool(column.get("is_measure")),
            }
        )

    artifacts = []
    for layer in ("bronze", "silver", "gold"):
        for artifact in state.get(f"{layer}_generation_results") or []:
            if not isinstance(artifact, dict):
                continue
            artifacts.append(
                {
                    "layer": layer,
                    "name": (
                        artifact.get("dbt_model_name")
                        or artifact.get("dbt_alias")
                        or str(artifact.get("target_table") or artifact.get("table") or "Generated model").split(".")[-1]
                    ),
                    "target": artifact.get("target_table"),
                    "format": artifact.get("code_generation_format") or artifact.get("script_language") or "dbt",
                    "status": artifact.get("review_status") or artifact.get("status") or "APPROVED",
                }
            )

    report_tables = sorted(table_rows.values(), key=lambda item: str(item.get("name") or "").casefold())
    source_databases = state.get("source_databases")
    source_database = state.get("database_name") or (
        source_databases[0] if isinstance(source_databases, list) and source_databases else source_databases
    )
    target = str(state.get("target_warehouse") or "snowflake").lower()
    execution_engine = str(state.get("execution_engine") or "native").lower()
    is_dbt = execution_engine == "dbt"
    execution_summary = (
        {
            "kind": "deployment",
            "status": state.get("snowflake_dbt_deploy_status") or state.get("snowflake_dbt_status") or "COMPLETED",
            "validation_status": state.get("snowflake_dbt_validation_status"),
            "artifact_set_hash": state.get("snowflake_dbt_artifact_set_hash"),
            "completion_mode": state.get("completion_mode") or "dbt_executed",
        }
        if is_dbt
        else {
            "kind": "execution",
            "status": state.get(f"{target}_gold_execution_status") or "COMPLETED",
            "validation_status": state.get("gold_runtime_validation_status"),
            "completion_mode": "native_execution",
            "bronze_status": state.get(f"{target}_bronze_execution_status"),
            "silver_status": state.get(f"{target}_silver_execution_status"),
            "gold_status": state.get(f"{target}_gold_execution_status"),
        }
    )
    return {
        "version": 1,
        "generated_at": generated_at,
        "title": "Pipeline Run Report",
        "outcome": "SUCCESS",
        "run": {
            "id": state.get("run_id"),
            "name": state.get("project_name") or state.get("brd_filename") or state.get("run_id"),
            "source": state.get("source") or "database",
            "source_database": source_database,
            "target": target,
            "execution_engine": execution_engine,
            "deployment_mode": (
                state.get("dbt_deployment_mode") or "generate_and_deploy"
                if is_dbt
                else "native_execution"
            ),
            "started_at": state.get("started_at"),
            "completed_at": generated_at,
        },
        "metrics": {
            "tables": len(report_tables),
            "columns": sum(len(item["columns"]) for item in report_tables),
            "kpis": len(kpis),
            "artifacts": len(artifacts),
            "pii_columns": sum(1 for column in columns if column.get("is_pii") or column.get("is_pii_candidate")),
            "key_columns": sum(
                1 for column in columns
                if column.get("is_join_key")
                or column.get("is_primary_key")
                or str(column.get("semantic_type") or "").upper() in {"ID", "SURROGATE_KEY"}
            ),
        },
        "artifacts": artifacts,
        "tables": report_tables,
        "kpis": [
            {
                "name": item.get("kpi_name") or item.get("name") or "Unnamed KPI",
                "description": item.get("kpi_description") or item.get("description"),
                "formula": item.get("formula") or item.get("business_formula") or item.get("calculation"),
                "target": item.get("target_table") or item.get("output_table"),
            }
            for item in kpis
        ],
        "reviews": {
            "kpi": str((state.get("gate1") or {}).get("decision") or state.get("human_decision") or "APPROVED"),
            "tables": str((state.get("gate2") or {}).get("decision") or state.get("human_table_decision") or "APPROVED"),
            "semantics": str(state.get("enrichment_review_decision") or state.get("enrichment_review_status") or "APPROVED"),
            "bronze": str(state.get("bronze_review_decision") or (state.get("gate4") or {}).get("decision") or "APPROVED"),
            "merge_keys": str(state.get("silver_merge_key_review_decision") or "APPROVED"),
            "silver": str(state.get("silver_review_decision") or (state.get("gate5") or {}).get("decision") or "APPROVED"),
            "gold": str(state.get("gold_review_decision") or "APPROVED"),
        },
        "deployment": execution_summary,
    }


def _complete_run_with_report(
    state: Dict[str, Any],
    *,
    running_message: str,
    completed_message: str,
    context: str,
) -> Dict[str, Any]:
    """Finalize a successful generation-first run with its persisted report."""
    run_id = str(state.get("run_id") or "")
    working_state = {
        **state,
        "status": "RUNNING",
        "execution_ready": False,
        "background_stage": "report_generation",
        "failed_background_stage": None,
        "next_stage_key": None,
        "next_stage_label": None,
        "stage_confirmation": None,
        "report_generation_enabled": True,
        "report_generation_status": "RUNNING",
        "resume_message": running_message,
    }
    save_checkpoint_state_timed(
        run_id,
        working_state,
        context=f"{context}:report_generation:running",
    )
    try:
        run_report = build_run_report(working_state)
        report_status = "COMPLETED"
    except Exception:
        logger.exception(
            "Run report generation failed after successful target execution",
            extra={"run_id": run_id, "stage": "report_generation"},
        )
        run_report = {
            "version": 1,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "title": "Pipeline Run Report",
            "outcome": "SUCCESS",
            "warning": "Detailed report sections could not be assembled.",
            "run": {
                "id": run_id,
                "target": state.get("target_warehouse"),
                "execution_engine": state.get("execution_engine"),
            },
            "metrics": {},
            "artifacts": [],
            "tables": [],
            "kpis": [],
            "reviews": {},
            "deployment": {"status": "COMPLETED"},
        }
        report_status = "COMPLETED_WITH_WARNINGS"

    working_state.update(
        {
            "status": "PIPELINE_COMPLETED",
            "background_stage": None,
            "last_completed_stage_key": "report_generation",
            "last_completed_stage_label": "Report Generation",
            "report_generation_status": report_status,
            "run_report": run_report,
            "resume_message": completed_message,
        }
    )
    save_checkpoint_state_timed(
        run_id,
        working_state,
        context="report_generation:complete",
    )
    return working_state


def execute_generation_first_snowflake_dbt(
    run_id: str,
    *,
    state: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    working_state = dict(state or load_checkpoint_state(run_id) or {"run_id": run_id})
    working_state["run_id"] = run_id
    validation_errors = _database_dbt_execution_validation_errors(working_state)
    if validation_errors:
        raise RuntimeError(
            "Snowflake dbt deployment is not ready: " + "; ".join(validation_errors) + "."
        )

    if (
        working_state.get("source_system_id") is not None
        and revised_metadata_database_flow(working_state)
    ):
        try:
            working_state = _execute_metadata_setup(working_state)
        except Exception as exc:
            failed_state = {
                **working_state,
                "status": "FAILED",
                "execution_ready": True,
                "background_stage": "metadata_setup_execution",
                "failed_background_stage": "metadata_setup_execution",
                "next_stage_key": "metadata_setup_execution",
                "next_stage_label": "Metadata Setup Execution",
                "metadata_setup_execution_status": "FAILED",
                "error": str(exc),
                "resume_message": (
                    "Target metadata setup failed. Retry safely reuses the verified DDL "
                    "and existing configuration versions."
                ),
            }
            save_checkpoint_state_timed(
                run_id, failed_state, context="metadata_setup_execution:failed"
            )
            raise

    stage_key = "gold_code_execution"
    execution_state = {
        **working_state,
        "status": "RUNNING",
        "execution_ready": True,
        "background_stage": stage_key,
        "next_stage_key": stage_key,
        "next_stage_label": "Code Execution",
        "awaiting_stage_confirmation": False,
        "stage_confirmation": None,
        "failed_background_stage": None,
        "error": None,
        "resume_message": "Landing approved source data before Snowflake dbt deployment.",
    }
    save_checkpoint_state_timed(run_id, execution_state, context="snowflake_dbt_deployment:running")

    control: Optional[Dict[str, Any]] = None
    try:
        control = _start_snowflake_dbt_control_attempt(execution_state)
        if str(execution_state.get("snowflake_bronze_source_load_status") or "").upper() != "COMPLETED":
            from services.snowflake_bronze_runtime import run_snowflake_bronze_scripts

            landed_state = run_snowflake_bronze_scripts(
                execution_state,
                review_artifact=execution_state.get("bronze_review_artifact") or {},
                approved_only=True,
                load_only=True,
                progress_stage_key=stage_key,
            )
            working_state = {**execution_state, **landed_state, "run_id": run_id}
            if str(working_state.get("snowflake_bronze_source_load_status") or "").upper() != "COMPLETED":
                raise RuntimeError("Snowflake source landing did not complete before dbt deployment.")
            working_state.update(
                {
                    "status": "RUNNING",
                    "background_stage": stage_key,
                    "resume_message": "Deploying the frozen dbt project and running dbt build in Snowflake.",
                }
            )
            save_checkpoint_state_timed(run_id, working_state, context="snowflake_dbt_source_landing:complete")

        if control and not control.get("completed"):
            control["error_stage"] = "WRITE"
            if int(control.get("attempt_number") or 0) > 1:
                # ponytail: full-load dbt models are idempotent, so a fenced retry may replace a failed receipt.
                working_state["force_dbt_deploy"] = True
            control["repository"].heartbeat_queue_item(
                queue_id=int(control["queue"]["queue_id"]),
                worker_id=control["worker_id"],
                lease_seconds=int(control["lease_seconds"]),
            )
            control["repository"].update_run_phase(
                str(control["run"]["run_id"]),
                "TARGET_SUBMITTED",
                queue_id=int(control["queue"]["queue_id"]),
                worker_id=control["worker_id"],
                target_write_id=f"dbt:{working_state.get('snowflake_dbt_artifact_set_hash')}",
                target_commit_status="SUBMITTED",
            )
        deployed_state = execute_finalized_snowflake_dbt_project(working_state)
        _finish_snowflake_dbt_control_attempt(control, deployed_state)
        gold_execution_status = str(
            deployed_state.get("snowflake_gold_execution_status") or "COMPLETED"
        ).upper()
        gold_execution_summary = deployed_state.get("snowflake_dbt_execution_summary") or {}
        gold_execution_message = str(gold_execution_summary.get("message") or "").strip()
        working_state = {
            **working_state,
            **deployed_state,
            "run_id": run_id,
            "execution_ready": False,
            "failed_background_stage": None,
            "last_completed_stage_key": stage_key,
            "last_completed_stage_label": "Code Execution",
            "next_stage_key": None,
            "next_stage_label": None,
            "stage_confirmation": None,
            "snowflake_gold_execution_status": gold_execution_status,
            "gold_runtime_validation_status": gold_execution_status,
            "external_execution": {
                "status": gold_execution_status,
                "layer": "gold",
                "total_count": gold_execution_summary.get("planned_count"),
                "completed_count": gold_execution_summary.get("completed_count"),
                "failed_count": gold_execution_summary.get("failed_count"),
                "message": gold_execution_message,
            },
        }
        return _complete_run_with_report(
            working_state,
            running_message="Deployment completed. Generating the pipeline run report.",
            completed_message=(
                f"{gold_execution_message} Run report completed."
                if gold_execution_message
                else "Snowflake dbt deployment, build, and run report completed."
            ),
            context="snowflake_dbt_deployment",
        )
    except Exception as exc:
        try:
            _fail_snowflake_dbt_control_attempt(control, exc)
        except Exception:
            logger.exception(
                "Snowflake dbt control finalization failed",
                extra={"run_id": run_id, "stage": "gold_execution"},
            )
        latest_checkpoint = load_checkpoint_state(run_id) or {}
        failed_state = {
            **execution_state,
            **working_state,
            **latest_checkpoint,
            "status": "FAILED",
            "execution_ready": True,
            "background_stage": stage_key,
            "failed_background_stage": stage_key,
            "next_stage_key": stage_key,
            "next_stage_label": "Code Execution",
            "error": str(exc),
            "resume_message": (
                "Snowflake dbt deployment or build failed. Retry reuses completed source landing "
                "and the frozen reviewed project."
            ),
        }
        save_checkpoint_state_timed(run_id, failed_state, context="snowflake_dbt_deployment:failed")
        raise


def submit_gold_review(run_id: str, action: str = "APPROVED", review_artifact: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    checkpoint = load_checkpoint_state(run_id) or {"run_id": run_id}
    if is_run_aborted(run_id, checkpoint):
        return aborted_run_state(run_id, checkpoint)
    decision = str(action or "APPROVED").upper()
    current_review_artifact = review_artifact or checkpoint.get("gold_review_artifact") or {}
    if decision != "APPROVED":
        checkpoint = _invalidate_generation_first_review_state(
            checkpoint,
            boundary="gold_review",
        )
    final_state = {
        **checkpoint,
        "run_id": run_id,
        "gold_review_decision": decision,
        "gold_review_artifact": current_review_artifact,
        "next_review_key": None,
        "failed_background_stage": None,
        "error": None,
    }

    metadata_gold = False
    if decision == "APPROVED":
        final_state["gold_generation_results"] = _filter_gold_results_by_review(
            [item for item in final_state.get("gold_generation_results") or [] if isinstance(item, dict)],
            final_state["gold_review_artifact"],
        )
        metadata_gold = any(
            item.get("gold_ingestion_object_id") is not None
            for item in final_state["gold_generation_results"]
        )
        if metadata_gold and not snowflake_dbt_enabled(final_state):
            final_state = _activate_reviewed_gold_metadata(_attach_gold_execution_specs(final_state))

    if decision == "REJECTED":
        final_state.update({"status": "FAILED", "error": "Gold Review rejected generated Gold scripts"})
    elif decision == "REGENERATE":
        final_state.update({"status": "REGENERATE_REQUIRED", "resume_message": "Gold Review requested regeneration."})
    elif (
        decision == "APPROVED"
        and str(final_state.get("target_warehouse") or "").lower() == "snowflake"
        and snowflake_dbt_enabled(final_state)
        and generation_first_snowflake_dbt_flow(final_state)
    ):
        generation_state = {
            **final_state,
            "status": "RUNNING",
            "background_stage": "snowflake_dbt_codegen",
            "snowflake_dbt_validation_status": "RUNNING",
            "resume_message": "Finalizing and validating the reviewed Snowflake dbt project.",
        }
        save_checkpoint_state(run_id, generation_state)
        try:
            final_state = finalize_snowflake_dbt_project(generation_state)
            if metadata_gold:
                final_state = _attach_gold_execution_specs(final_state)
                final_state = _activate_finalized_snowflake_dbt_metadata(final_state)
        except Exception as exc:
            failed_state = {
                **generation_state,
                "status": "FAILED",
                "failed_background_stage": "snowflake_dbt_codegen",
                "snowflake_dbt_validation_status": "FAILED",
                "error": str(exc),
            }
            save_checkpoint_state(run_id, failed_state)
            raise

        if str(final_state.get("dbt_deployment_mode") or "generate_only").lower() != "generate_and_deploy":
            final_state.update(
                {
                    "status": "PIPELINE_COMPLETED",
                    "background_stage": None,
                    "execution_ready": False,
                    "snowflake_gold_execution_status": "SKIPPED_DBT_CODEGEN_ONLY",
                    "resume_message": "Snowflake dbt project generated and statically validated. Execution was not requested.",
                }
            )
        else:
            validation_errors = _database_dbt_execution_validation_errors(final_state)
            if validation_errors:
                error = "Approved dbt project is not ready for deployment: " + "; ".join(validation_errors) + "."
                final_state.update({"status": "FAILED", "error": error})
                save_checkpoint_state(run_id, final_state)
                raise RuntimeError(error)
            final_state.update(
                {
                    "status": "PAUSED_FOR_STAGE_CONFIRMATION",
                    "execution_ready": True,
                    "background_stage": None,
                    "awaiting_stage_confirmation": True,
                    "last_completed_stage_key": "gold_review",
                    "last_completed_stage_label": "Gold Code Review",
                    "next_stage_key": "gold_code_execution",
                    "next_stage_label": "Metadata Setup Execution",
                    "resume_message": "All dbt models are reviewed and frozen. Start target metadata setup and deployment when ready.",
                    "stage_confirmation": {
                        "enabled": True,
                        "awaiting_confirmation": True,
                        "last_completed_stage_key": "gold_review",
                        "last_completed_stage_label": "Gold Code Review",
                        "next_stage_key": "gold_code_execution",
                        "next_stage_label": "Metadata Setup Execution",
                        "resume_message": "All dbt models are reviewed and frozen. Start target metadata setup and deployment when ready.",
                    },
                }
            )
        save_checkpoint_state(run_id, final_state)
    elif (
        decision == "APPROVED"
        and str(final_state.get("target_warehouse") or "").lower() == "snowflake"
        and snowflake_dbt_enabled(final_state)
    ):
        generation_state = {
            **final_state,
            "status": "RUNNING",
            "background_stage": "snowflake_dbt_codegen",
            "snowflake_dbt_validation_status": "RUNNING",
            "resume_message": "Finalizing the generated Snowflake dbt project.",
        }
        save_checkpoint_state(run_id, generation_state)
        try:
            final_state = run_snowflake_dbt(generation_state)
        except Exception as exc:
            failed_state = {
                **generation_state,
                "status": "FAILED",
                "failed_background_stage": "snowflake_dbt_codegen",
                "snowflake_dbt_validation_status": "FAILED",
                "error": str(exc),
            }
            save_checkpoint_state(run_id, failed_state)
            raise
        final_state.update(
            {
                "status": "PIPELINE_COMPLETED",
                "background_stage": None,
                "next_review_key": None,
                "snowflake_gold_execution_status": (
                    "COMPLETED"
                    if final_state.get("completion_mode") == "dbt_executed"
                    else "SKIPPED_DBT_CODEGEN_ONLY"
                ),
                "resume_message": (
                    "Snowflake dbt build completed."
                    if final_state.get("completion_mode") == "dbt_executed"
                    else "Snowflake dbt project generated. Execution was not requested."
                ),
            }
        )
    elif decision == "APPROVED" and generation_first_native_database_flow(final_state):
        validation_errors = _database_native_execution_validation_errors(final_state)
        if validation_errors:
            error = "Approved code is not ready for target execution: " + "; ".join(validation_errors) + "."
            final_state.update({"status": "FAILED", "error": error})
            save_checkpoint_state(run_id, final_state)
            raise RuntimeError(error)
        final_state.update(
            {
                "status": "PAUSED_FOR_STAGE_CONFIRMATION",
                "execution_ready": True,
                "background_stage": None,
                "awaiting_stage_confirmation": True,
                "last_completed_stage_key": "gold_review",
                "last_completed_stage_label": "Gold Code Review",
                "next_stage_key": "bronze_code_execution",
                "next_stage_label": "Metadata Setup Execution",
                "resume_message": "All generated code is approved. Start target metadata setup and execution when ready.",
                "stage_confirmation": {
                    "enabled": True,
                    "awaiting_confirmation": True,
                    "last_completed_stage_key": "gold_review",
                    "last_completed_stage_label": "Gold Code Review",
                    "next_stage_key": "bronze_code_execution",
                    "next_stage_label": "Metadata Setup Execution",
                    "resume_message": "All generated code is approved. Start target metadata setup and execution when ready.",
                },
            }
        )
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
            if is_run_aborted(run_id, final_state):
                return aborted_run_state(run_id, final_state)
            if (
                str(final_state.get("snowflake_gold_execution_status") or "").upper()
                not in SNOWFLAKE_COMPLETED_EXECUTION_STATUSES
            ):
                raise RuntimeError(
                    "Snowflake Gold execution did not complete; refusing to complete the pipeline."
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
    elif decision == "APPROVED" and str(final_state.get("target_warehouse") or "").lower() == "databricks":
        from services.databricks_runtime import databricks_gold_execution_enabled, run_databricks_gold_scripts

        if not databricks_gold_execution_enabled():
            error = "Databricks Gold execution is disabled; refusing to complete the pipeline."
            save_checkpoint_state(
                run_id,
                {
                    **final_state,
                    "status": "FAILED",
                    "failed_background_stage": "gold_code_execution",
                    "error": error,
                },
            )
            raise RuntimeError(error)
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
            if is_run_aborted(run_id, final_state):
                return aborted_run_state(run_id, final_state)
            if not _status_completed(final_state.get("databricks_gold_execution_status")):
                raise RuntimeError(
                    "Databricks Gold execution did not complete; refusing to complete the pipeline."
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
        error = f"Unsupported target warehouse for Gold execution: {final_state.get('target_warehouse') or 'missing'}"
        save_checkpoint_state(run_id, {**final_state, "status": "FAILED", "error": error})
        raise RuntimeError(error)

    save_checkpoint_state(run_id, final_state)
    return final_state
