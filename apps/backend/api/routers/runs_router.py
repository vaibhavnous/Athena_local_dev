import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from fastapi import APIRouter, Depends, HTTPException

from api.auth import AuthUser, assert_run_access, get_current_user
from api.demo import (
    demo_enabled,
    demo_lineage,
    demo_run,
    demo_runs,
    demo_scripts,
)
from utilis.logger import logger

router = APIRouter()
RUN_LIST_EXECUTOR = ThreadPoolExecutor(max_workers=max(1, int(os.getenv("ATHENA_RUN_LIST_WORKERS", "2"))))
RUN_SUMMARY_EXECUTOR = ThreadPoolExecutor(max_workers=max(1, int(os.getenv("ATHENA_RUN_SUMMARY_WORKERS", "2"))))
RUN_LIST_RETRY_LOCK = threading.Lock()
RUN_LIST_RETRY_AFTER = 0.0
RUN_LIST_RETRY_DELAY_SECONDS = max(30, int(os.getenv("ATHENA_RUNS_RETRY_DELAY_SECONDS", "120")))
RUN_ID_PATTERN = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-"
    r"[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
)
GENERATION_FIRST_DATABASE_FLOW_VERSIONS = {"generation_first_v1", "generation_first_v2"}


def _is_generation_first_database_run(payload: Dict[str, Any]) -> bool:
    return str(payload.get("database_flow_version") or "") in GENERATION_FIRST_DATABASE_FLOW_VERSIONS


def _as_bool(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _utc_timestamp(value: Any) -> Any:
    """Serialize timezone-less SQL datetimes as UTC, not browser-local time."""
    if not isinstance(value, datetime):
        return value
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _with_project_metadata(payload: Dict[str, Any], user: AuthUser) -> Dict[str, Any]:
    project_id = str(payload.get("project_id") or "").strip()
    # The checkpoint already carries the project snapshot used by Run History.
    # Avoid another remote project-table lookup on every status/detail refresh.
    if not project_id or payload.get("project_name"):
        return payload
    try:
        from api.auth import load_project_for_user

        project = load_project_for_user(project_id, user)
    except Exception:
        logger.warning("Unable to enrich run with project metadata", extra={"project_id": project_id})
        return payload
    return {
        **payload,
        "project_name": payload.get("project_name") or project.get("name"),
        "project_description": payload.get("project_description") or project.get("description"),
        "database_type": payload.get("database_type") or payload.get("db_type") or project.get("db_type"),
        "database_name": payload.get("database_name") or project.get("database_name"),
        "use_domain_knowledge_base": (
            _as_bool(payload.get("use_domain_knowledge_base"))
            or _as_bool(project.get("use_domain_knowledge_base"))
        ),
        "domain_profile": payload.get("domain_profile") or project.get("domain_profile"),
        "knowledge_base_id": payload.get("knowledge_base_id") or project.get("knowledge_base_id"),
    }


def _fallback_run_summary(row: Dict[str, Any]) -> Dict[str, Any]:
    run_id = str(row.get("run_id") or row.get("id") or "")
    return {
        "id": run_id,
        "run_id": run_id,
        "project_id": row.get("project_id"),
        "project_name": row.get("project_name"),
        "project_description": row.get("project_description"),
        "brd_filename": row.get("brd_filename") or run_id,
        "source": row.get("source") or "database",
        "database_type": row.get("database_type") or row.get("db_type"),
        "database_name": row.get("database_name"),
        "use_domain_knowledge_base": _as_bool(row.get("use_domain_knowledge_base")),
        "domain_profile": row.get("domain_profile"),
        "knowledge_base_id": row.get("knowledge_base_id"),
        "target_warehouse": row.get("target_warehouse"),
        "execution_engine": row.get("execution_engine") or "native",
        "dbt_deployment_mode": row.get("dbt_deployment_mode") or "generate_only",
        "database_flow_version": row.get("database_flow_version"),
        "generation_first_execution": _is_generation_first_database_run(row),
        "execution_ready": row.get("execution_ready"),
        "awaiting_stage_confirmation": row.get("awaiting_stage_confirmation"),
        "next_stage_key": row.get("next_stage_key"),
        "next_stage_label": row.get("next_stage_label"),
        "status": row.get("status") or "UNKNOWN",
        "provider": row.get("provider") or "azure_openai",
        "deployment": row.get("deployment"),
        "started_at": _utc_timestamp(row.get("started_at")),
        "completed_at": _utc_timestamp(row.get("completed_at")),
        "cache_hit": "NONE",
        "cache_score": 0,
        "extraction_path": "ATHENA_GRAPH",
        "total_tokens": 0,
        "total_cost": 0,
        "stages": [],
        "next_gate": None,
        "resume_message": None,
        "stage_confirmation": None,
        "failed_stage_key": None,
        "failed_stage_label": None,
        "error": row.get("error"),
        "updated_at": _utc_timestamp(row.get("updated_at") or row.get("last_activity")),
        "script_counts": {"bronze": 0, "silver": 0, "gold": 0},
        "sftp_entity": row.get("sftp_entity"),
        "source_row_count": row.get("source_row_count"),
        "source_columns": row.get("source_columns") or [],
        "compliance_enabled": bool(row.get("compliance_enabled")),
        "compliance_assessment_id": row.get("compliance_assessment_id"),
        "compliance_assessment_status": row.get("compliance_assessment_status"),
        "compliance_review_status": row.get("compliance_review_status"),
        "hydration_fallback": True,
        "status_authoritative": False,
    }


def _local_run_history(limit: int) -> List[Dict[str, Any]]:
    """Build a non-blocking recent-run index from the structured local log.

    This is intentionally a history-list fallback only. Selecting a run still
    loads its authoritative checkpoint/status from the API.
    """
    log_path = Path(__file__).resolve().parents[2] / "pipeline_logs.json"
    if not log_path.exists():
        return []

    runs: Dict[str, Dict[str, Any]] = {}
    try:
        with log_path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                try:
                    item = json.loads(raw_line)
                except (TypeError, ValueError):
                    continue
                run_id = str(item.get("run_id") or "").strip()
                if not RUN_ID_PATTERN.fullmatch(run_id):
                    continue

                timestamp = item.get("timestamp")
                row = runs.setdefault(
                    run_id,
                    {
                        "run_id": run_id,
                        "started_at": timestamp,
                        "last_activity": timestamp,
                        "source": item.get("source") or "database",
                        "status": "UNKNOWN",
                    },
                )
                if timestamp:
                    row["last_activity"] = timestamp
                if item.get("source"):
                    row["source"] = item["source"]

                message = str(item.get("message") or "")
                status_match = re.search(r"\bstatus=([A-Za-z_]+)", message)
                if status_match:
                    row["status"] = _status_from_checkpoint({"status": status_match.group(1)})
                lowered = message.lower()
                if "pipeline aborted" in lowered:
                    row["status"] = "ABORTED"
                elif "pipeline completed" in lowered or "pipeline complete" in lowered:
                    row["status"] = "SUCCESS"
                elif "pipeline failed" in lowered:
                    row["status"] = "FAILED"
    except OSError:
        logger.warning("Unable to read local run history fallback", exc_info=True)
        return []

    ordered = sorted(
        runs.values(),
        key=lambda row: str(row.get("last_activity") or ""),
        reverse=True,
    )
    return [_fallback_run_summary(row) for row in ordered[:limit]]


def _status_from_checkpoint(checkpoint: Dict[str, Any]) -> str:
    status = str(checkpoint.get("status") or "UNKNOWN").upper()
    if checkpoint.get("background_stage") or status in {"RUNNING", "PROCESSING", "PENDING", "SUBMITTED", "IN_PROGRESS"}:
        return "RUNNING"
    if checkpoint.get("next_gate") or checkpoint.get("next_review_key") or status in {"HITL_WAIT", "PAUSED_FOR_HITL"}:
        return "HITL_WAIT"
    if status == "PAUSED_FOR_STAGE_CONFIRMATION":
        return "PAUSED_FOR_STAGE_CONFIRMATION"
    if status in {"PIPELINE_COMPLETED", "COMPLETED", "SUCCESS"}:
        return "SUCCESS"
    if status == "FAILED":
        return "FAILED"
    if status == "ABORTED":
        return "ABORTED"
    return status


def _checkpoint_run_summary(row: Dict[str, Any]) -> Dict[str, Any]:
    from services.pipeline_runtime import load_checkpoint_state

    run_id = str(row.get("run_id") or row.get("id") or "")
    checkpoint = row.get("checkpoint")
    if isinstance(checkpoint, str):
        checkpoint = json.loads(checkpoint)
    if not isinstance(checkpoint, dict):
        checkpoint = load_checkpoint_state(run_id) or {}
    return {
        **_fallback_run_summary(row),
        "hydration_fallback": False,
        "status_authoritative": True,
        "brd_filename": checkpoint.get("brd_filename") or checkpoint.get("display_name") or row.get("brd_filename") or run_id,
        "source": checkpoint.get("source") or row.get("source") or "database",
        "project_id": checkpoint.get("project_id") or row.get("project_id"),
        "project_name": checkpoint.get("project_name") or row.get("project_name"),
        "project_description": checkpoint.get("project_description") or row.get("project_description"),
        "database_type": checkpoint.get("database_type") or checkpoint.get("db_type") or row.get("database_type") or row.get("db_type"),
        "database_name": checkpoint.get("database_name") or row.get("database_name"),
        "use_domain_knowledge_base": _as_bool(checkpoint.get("use_domain_knowledge_base")),
        "domain_profile": checkpoint.get("domain_profile") or row.get("domain_profile"),
        "knowledge_base_id": checkpoint.get("knowledge_base_id") or row.get("knowledge_base_id"),
        "target_warehouse": checkpoint.get("target_warehouse") or row.get("target_warehouse"),
        "execution_engine": checkpoint.get("execution_engine") or row.get("execution_engine") or "native",
        "dbt_deployment_mode": checkpoint.get("dbt_deployment_mode") or row.get("dbt_deployment_mode") or "generate_only",
        "database_flow_version": checkpoint.get("database_flow_version"),
        "generation_first_execution": _is_generation_first_database_run(checkpoint),
        "report_generation_enabled": bool(checkpoint.get("report_generation_enabled")),
        "report_generation_status": checkpoint.get("report_generation_status"),
        "execution_ready": checkpoint.get("execution_ready"),
        "awaiting_stage_confirmation": checkpoint.get("awaiting_stage_confirmation"),
        "next_stage_key": checkpoint.get("next_stage_key"),
        "next_stage_label": checkpoint.get("next_stage_label"),
        "status": _status_from_checkpoint(checkpoint),
        "provider": checkpoint.get("provider") or row.get("provider") or "azure_openai",
        "deployment": checkpoint.get("deployment") or row.get("deployment"),
        "started_at": _utc_timestamp(checkpoint.get("started_at") or row.get("started_at")),
        "completed_at": _utc_timestamp(checkpoint.get("completed_at")),
        "next_gate": checkpoint.get("next_gate"),
        "next_review_key": checkpoint.get("next_review_key"),
        "resume_message": checkpoint.get("resume_message"),
        "stage_confirmation": checkpoint.get("stage_confirmation"),
        "failed_stage_key": checkpoint.get("failed_background_stage") or checkpoint.get("last_failed_stage_key"),
        "failed_stage_label": checkpoint.get("failed_stage_label"),
        "error": checkpoint.get("error") or row.get("error"),
        "updated_at": _utc_timestamp(
            checkpoint.get("updated_at")
            or checkpoint.get("checkpoint_at")
            or row.get("last_activity")
        ),
        "sftp_entity": checkpoint.get("sftp_entity") or row.get("sftp_entity"),
        "source_row_count": checkpoint.get("source_row_count") or row.get("source_row_count"),
        "source_columns": checkpoint.get("source_columns") or row.get("source_columns") or [],
        "compliance_enabled": bool(checkpoint.get("compliance_enabled")),
        "compliance_assessment_id": checkpoint.get("compliance_assessment_id"),
        "compliance_assessment_status": checkpoint.get("compliance_assessment_status"),
        "compliance_review_status": checkpoint.get("compliance_review_status"),
    }


def _fallback_run_detail(run_id: str, checkpoint: Dict[str, Any] | None = None) -> Dict[str, Any]:
    checkpoint = checkpoint or {}
    from services.pipeline_runtime import (
        apply_waiting_stage_state,
        build_pipeline_steps,
    )

    generation_first = _is_generation_first_database_run(checkpoint)
    bronze_completed = bool(
        checkpoint.get("bronze_generation_status") == "COMPLETED"
        or checkpoint.get("bronze_generation_results")
        or (
            not generation_first
            and (
                checkpoint.get("snowflake_bronze_execution_status") == "COMPLETED"
                or checkpoint.get("databricks_bronze_execution_status") == "COMPLETED"
            )
        )
    )
    silver_completed = bool(
        checkpoint.get("silver_generation_status") == "COMPLETED"
        or checkpoint.get("silver_generation_results")
        or (
            not generation_first
            and (
                checkpoint.get("snowflake_silver_execution_status") == "COMPLETED"
                or checkpoint.get("databricks_silver_execution_status") == "COMPLETED"
            )
        )
    )
    gold_completed = bool(
        str(checkpoint.get("gold_generation_status") or "").startswith("COMPLETED")
        or checkpoint.get("gold_generation_results")
        or (
            not generation_first
            and (
                checkpoint.get("background_stage") == "gold_code_execution"
                or str(checkpoint.get("snowflake_gold_execution_status") or "").upper() in {"RUNNING", "COMPLETED", "COMPLETED_WITH_WARNINGS"}
                or str(checkpoint.get("databricks_gold_execution_status") or "").upper() in {"RUNNING", "COMPLETED", "COMPLETED_WITH_WARNINGS"}
            )
        )
    )
    next_gate = None if gold_completed else checkpoint.get("next_gate")
    next_review_key = (
        checkpoint.get("next_review_key")
        if generation_first
        else None if gold_completed
        else checkpoint.get("next_review_key")
    )
    checkpoint_status = str(checkpoint.get("status") or "").upper()
    last_completed_stage_key = str(checkpoint.get("last_completed_stage_key") or "")
    if checkpoint_status in {"HITL_WAIT", "PAUSED_FOR_HITL", "PENDING_REVIEW"}:
        if not next_gate and not next_review_key:
            next_gate = {
                "gate1": 1,
                "gate2": 2,
                "gate3": 3,
                "bronze": 4,
                "gate4": 4,
                "silver": 5,
                "gate5": 5,
            }.get(last_completed_stage_key)
        if (
            generation_first
            and not next_gate
            and not next_review_key
            and last_completed_stage_key == "gold"
        ):
            next_review_key = "gold_review"
    pipeline_steps = build_pipeline_steps(
        source=str(checkpoint.get("source") or "database"),
        checkpoint=checkpoint,
        summary=[],
        pending_gate1=[],
        completed_gate1=[],
        nominated_tables=checkpoint.get("nominated_tables") or [],
        certified_tables=checkpoint.get("certified_tables") or [],
        enriched_payload=checkpoint.get("enriched_metadata") or {},
        gate3_payload=checkpoint.get("enrichment_review_artifact") or {},
        bronze_generation_completed=bronze_completed,
        silver_generation_completed=silver_completed,
        gold_generation_completed=gold_completed,
    )
    if not checkpoint.get("background_stage") and last_completed_stage_key:
        completed_index = next(
            (
                index
                for index, step in enumerate(pipeline_steps)
                if step.get("key") == last_completed_stage_key
            ),
            None,
        )
        if completed_index is not None:
            for step in pipeline_steps[:completed_index + 1]:
                step["state"] = "COMPLETED"
                step["complete"] = True
    waiting_gate_key = f"gate{next_gate}" if next_gate in {1, 2, 3, 4, 5} else None
    waiting_stage_key = str(next_review_key or waiting_gate_key or "") or None
    if waiting_stage_key:
        pipeline_steps = apply_waiting_stage_state(pipeline_steps, waiting_stage_key)
    fallback_status = checkpoint.get("status")
    if checkpoint.get("background_stage"):
        fallback_status = "RUNNING"
    elif gold_completed and (
        str(checkpoint.get("snowflake_gold_execution_status") or "").upper() in {"COMPLETED", "COMPLETED_WITH_WARNINGS"}
        or str(checkpoint.get("databricks_gold_execution_status") or "").upper() in {"COMPLETED", "COMPLETED_WITH_WARNINGS"}
    ):
        fallback_status = "SUCCESS"
    elif not generation_first and gold_completed and str(fallback_status or "").upper() == "HITL_WAIT":
        fallback_status = "RUNNING"
    current_step = next(
        (
            step for step in pipeline_steps
            if str(step.get("state") or "").upper() in {"RUNNING", "HITL_WAIT", "FAILED"}
        ),
        None,
    )
    external_execution = checkpoint.get("external_execution") if isinstance(checkpoint.get("external_execution"), dict) else None
    if current_step and external_execution and external_execution.get("message"):
        current_step = {**current_step, "detail": external_execution.get("message")}

    checkpoint_snapshot = {
        key: checkpoint.get(key)
        for key in (
            "run_id",
            "status",
            "background_stage",
            "next_gate",
            "next_review_key",
            "next_stage_key",
            "next_stage_label",
            "resume_message",
            "updated_at",
            "completed_at",
        )
        if key in checkpoint
    }

    return {
        **_fallback_run_summary(
            {
                "run_id": run_id,
                "project_id": checkpoint.get("project_id"),
                "brd_filename": checkpoint.get("brd_filename"),
                "source": checkpoint.get("source"),
                "target_warehouse": checkpoint.get("target_warehouse"),
                "execution_engine": checkpoint.get("execution_engine"),
                "dbt_deployment_mode": checkpoint.get("dbt_deployment_mode"),
                "database_flow_version": checkpoint.get("database_flow_version"),
                "status": fallback_status,
                "provider": checkpoint.get("provider"),
                "deployment": checkpoint.get("deployment"),
                "error": checkpoint.get("error"),
                "started_at": checkpoint.get("started_at"),
                "completed_at": checkpoint.get("completed_at"),
                "updated_at": checkpoint.get("updated_at") or checkpoint.get("checkpoint_at"),
                "project_name": checkpoint.get("project_name"),
                "project_description": checkpoint.get("project_description"),
                "database_type": checkpoint.get("database_type") or checkpoint.get("db_type"),
                "database_name": checkpoint.get("database_name"),
                "use_domain_knowledge_base": checkpoint.get("use_domain_knowledge_base"),
                "domain_profile": checkpoint.get("domain_profile"),
                "knowledge_base_id": checkpoint.get("knowledge_base_id"),
                "sftp_entity": checkpoint.get("sftp_entity"),
                "source_row_count": checkpoint.get("source_row_count"),
                "source_columns": checkpoint.get("source_columns"),
            }
        ),
        "hydration_fallback": True,
        "status_authoritative": bool(checkpoint),
        "checkpoint": checkpoint_snapshot,
        "execution_ready": checkpoint.get("execution_ready"),
        "awaiting_stage_confirmation": checkpoint.get("awaiting_stage_confirmation"),
        "next_stage_key": checkpoint.get("next_stage_key"),
        "next_stage_label": checkpoint.get("next_stage_label"),
        "stage_confirmation": checkpoint.get("stage_confirmation"),
        "next_gate": next_gate,
        "next_review_key": next_review_key,
        "resume_message": checkpoint.get("resume_message"),
        "pipeline_steps": pipeline_steps,
        "current_pipeline_step": current_step,
        "external_execution": external_execution,
        "bronze_generation_completed": bronze_completed,
        "silver_generation_completed": silver_completed,
        "gold_generation_completed": gold_completed,
        "report_generation_enabled": bool(checkpoint.get("report_generation_enabled")),
        "report_generation_status": checkpoint.get("report_generation_status"),
        "run_report": checkpoint.get("run_report") or {},
        "candidate_feed": checkpoint.get("candidate_feed"),
        "candidate_feeds": checkpoint.get("candidate_feeds") or [],
        "compliance_enabled": bool(checkpoint.get("compliance_enabled")),
        "compliance_assessment_id": checkpoint.get("compliance_assessment_id"),
        "compliance_assessment_status": checkpoint.get("compliance_assessment_status"),
        "compliance_assessment_error": checkpoint.get("compliance_assessment_error"),
        "compliance_review_status": checkpoint.get("compliance_review_status"),
        "compliance_review": checkpoint.get("compliance_review") or {},
        "compliance_review_error": checkpoint.get("compliance_review_error"),
        "compliance_results": checkpoint.get("compliance_results") or {},
        "bronze": {"generated_at": None, "scripts": []},
        "silver": {"generated_at": None, "scripts": []},
        "gold": {"generated_at": None, "scripts": []},
    }


# -------------------------
# ✅ Runs List
# -------------------------
@router.get("/runs")
def runs(user: AuthUser = Depends(get_current_user)) -> List[Dict[str, Any]]:
    global RUN_LIST_RETRY_AFTER

    if demo_enabled():
        return demo_runs()

    from services.pipeline_runtime import list_runs

    try:
        # ✅ configurable timeout with safe minimum
        timeout_seconds = max(1, int(os.getenv("ATHENA_RUNS_ENDPOINT_TIMEOUT_SECONDS", "10")))
        run_limit = max(1, min(100, int(os.getenv("ATHENA_RUNS_LIST_LIMIT", "10"))))
        fast_summary = str(os.getenv("ATHENA_RUNS_FAST_SUMMARY", "true")).lower() not in {"0", "false", "no"}
        deadline = time.monotonic() + timeout_seconds

        logger.debug("Fetching runs list", extra={"timeout_seconds": timeout_seconds, "limit": run_limit})

        with RUN_LIST_RETRY_LOCK:
            retry_after = RUN_LIST_RETRY_AFTER
        if user.user_type == "Admin" and time.monotonic() < retry_after:
            return _local_run_history(run_limit)

        list_kwargs = {} if user.user_type == "Admin" else {"owner_email": user.email}
        future = RUN_LIST_EXECUTOR.submit(list_runs, run_limit, **list_kwargs)
        rows = future.result(timeout=timeout_seconds)
        with RUN_LIST_RETRY_LOCK:
            RUN_LIST_RETRY_AFTER = 0.0

        results: List[Dict[str, Any]] = []

        if fast_summary:
            for row in rows:
                run_id = row.get("run_id")
                if not run_id:
                    continue
                try:
                    results.append(_checkpoint_run_summary(row))
                except Exception:
                    logger.warning("Failed to build checkpoint run summary; returning fallback summary", extra={"run_id": run_id})
                    results.append(_fallback_run_summary(row))
            if user.user_type == "Admin":
                local_by_id = {
                    str(item.get("run_id")): item
                    for item in _local_run_history(max(run_limit, len(results)))
                }
                results = [
                    {
                        **summary,
                        "status": (
                            local_by_id.get(str(summary.get("run_id")), {}).get("status")
                            if str(summary.get("status") or "UNKNOWN").upper() == "UNKNOWN"
                            else summary.get("status")
                        ) or "UNKNOWN",
                        "started_at": summary.get("started_at")
                        or local_by_id.get(str(summary.get("run_id")), {}).get("started_at"),
                    }
                    for summary in results
                ]
            return results

        for row in rows:
            run_id = row.get("run_id")
            if not run_id:
                continue  # ✅ safety against malformed data

            if time.monotonic() >= deadline:
                logger.warning("GET /runs summary budget exhausted; returning fallback summary", extra={"run_id": run_id})
                results.append(_fallback_run_summary(row))
                continue

            try:
                from api.services.ui_service import ui_run_summary

                remaining = max(0.1, deadline - time.monotonic())
                summary_future = RUN_SUMMARY_EXECUTOR.submit(ui_run_summary, run_id)
                results.append(summary_future.result(timeout=remaining))
            except FutureTimeoutError:
                logger.warning("GET /runs summary timed out; returning fallback summary", extra={"run_id": run_id})
                results.append(_fallback_run_summary(row))
            except Exception:
                # ✅ prevent single failure from breaking endpoint
                logger.warning(
                    "Failed to build run summary",
                    extra={"run_id": run_id},
                )
                results.append(_fallback_run_summary(row))

        return results

    except FutureTimeoutError:
        logger.warning("GET /runs timed out while listing runs")
        try:
            future.cancel()
        except Exception:
            pass
        if user.user_type == "Admin":
            with RUN_LIST_RETRY_LOCK:
                RUN_LIST_RETRY_AFTER = time.monotonic() + RUN_LIST_RETRY_DELAY_SECONDS
            return _local_run_history(run_limit)
        raise HTTPException(status_code=503, detail="Run list temporarily unavailable")

    except Exception:
        logger.error("Failed to fetch runs", exc_info=True)
        if user.user_type == "Admin":
            with RUN_LIST_RETRY_LOCK:
                RUN_LIST_RETRY_AFTER = time.monotonic() + RUN_LIST_RETRY_DELAY_SECONDS
            return _local_run_history(run_limit)
        raise HTTPException(status_code=503, detail="Failed to fetch runs")


# -------------------------
# ✅ Run Detail
# -------------------------
@router.get("/runs/{run_id}")
def run_detail(run_id: str, user: AuthUser = Depends(get_current_user)) -> Dict[str, Any]:
    if demo_enabled():
        return demo_run(run_id, include_scripts=True)

    try:
        checkpoint = assert_run_access(run_id, user)
        # Run History needs the persisted state immediately. Generated scripts
        # are loaded by /run-scripts only when the user opens the code viewer.
        return _with_project_metadata(_fallback_run_detail(run_id, checkpoint), user)
    except HTTPException:
        raise
    except Exception:
        logger.error(
            "Failed to fetch run detail",
            exc_info=True,
            extra={"run_id": run_id},
        )
        try:
            checkpoint = assert_run_access(run_id, user, checkpoint=load_checkpoint_state(run_id) or {})
        except HTTPException:
            raise
        except Exception:
            checkpoint = {}
        return _with_project_metadata(_fallback_run_detail(run_id, checkpoint), user)


@router.get("/run-scripts/{run_id}")
def run_scripts(run_id: str, user: AuthUser = Depends(get_current_user)) -> Dict[str, Any]:
    if demo_enabled():
        return {"run_id": run_id, **demo_scripts(run_id)}

    from services.pipeline_runtime import (
        load_bronze_scripts,
        load_checkpoint_state,
        load_gold_scripts,
        load_metadata_script,
        load_silver_scripts,
    )
    from services.adls_script_storage import adls_script_storage_configured

    try:
        checkpoint = assert_run_access(run_id, user, checkpoint=load_checkpoint_state(run_id) or {})
        return {
            "run_id": run_id,
            "script_source": "adls" if adls_script_storage_configured() else "local",
            "metadata": load_metadata_script(run_id, checkpoint),
            "bronze": load_bronze_scripts(run_id, checkpoint),
            "silver": load_silver_scripts(run_id, checkpoint),
            "gold": load_gold_scripts(run_id, checkpoint),
        }
    except HTTPException:
        raise
    except Exception:
        logger.error(
            "Failed to fetch run scripts",
            exc_info=True,
            extra={"run_id": run_id},
        )
        raise HTTPException(status_code=503, detail="Failed to fetch run scripts")


@router.get("/run-lineage/{run_id}")
def run_lineage(run_id: str, user: AuthUser = Depends(get_current_user)) -> Dict[str, Any]:
    if demo_enabled():
        return demo_lineage(run_id)

    from services.pipeline_runtime import build_run_lineage, load_checkpoint_state

    try:
        checkpoint = assert_run_access(run_id, user, checkpoint=load_checkpoint_state(run_id) or {})
        return build_run_lineage(run_id, checkpoint)
    except HTTPException:
        raise
    except Exception:
        logger.error(
            "Failed to build run lineage",
            exc_info=True,
            extra={"run_id": run_id},
        )
        raise HTTPException(status_code=503, detail="Failed to fetch run lineage")
