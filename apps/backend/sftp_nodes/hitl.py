from __future__ import annotations

from typing import Any, Dict, Iterable, Optional


def _approved_feeds(
    candidates: Iterable[Dict[str, Any]], approved_keys: Optional[Iterable[str]]
) -> list[Dict[str, Any]]:
    feeds = [dict(item) for item in candidates if isinstance(item, dict)]
    keys = {str(item).strip().casefold() for item in approved_keys or [] if str(item).strip()}
    if not keys:
        return feeds
    approved = [
        feed
        for feed in feeds
        if {
            str(feed.get("feed_id") or "").casefold(),
            str(feed.get("source_path") or "").casefold(),
            str(feed.get("table_name") or feed.get("entity") or "").casefold(),
            ".".join(
                str(part).strip()
                for part in (
                    feed.get("database_name") or feed.get("database"),
                    feed.get("schema_name") or feed.get("schema"),
                    feed.get("table_name") or feed.get("entity"),
                )
                if str(part or "").strip()
            ).casefold(),
        }
        & keys
    ]
    if not approved:
        raise ValueError("None of the selected Feed Review items match discovered ADLS files.")
    return approved


def submit_sftp_gate1_review(run_id: str, approve: bool = True) -> Dict[str, Any]:
    from services.pipeline_runtime import load_checkpoint_state, save_checkpoint_state
    from sftp_nodes.feed_nomination import sftp_feed_nomination_node
    from sftp_nodes.source_ingestion import source_ingestion_node
    from utilis.db import get_completed_items

    checkpoint = load_checkpoint_state(run_id) or {"run_id": run_id}
    if not approve:
        rejected = {
            **checkpoint,
            "status": "FAILED",
            "gate1_decision": "REJECTED",
            "gate1": {"decision": "REJECTED", "status": "COMPLETED"},
            "error": "KPI Review was rejected.",
        }
        save_checkpoint_state(run_id, rejected)
        return rejected
    certified_kpis = [
        item["kpi"]
        for item in get_completed_items(run_id, 1)
        if isinstance(item.get("kpi"), dict) and item["kpi"]
    ]
    if not certified_kpis:
        raise ValueError("Approved KPI Review produced no certified KPIs.")
    discovered = source_ingestion_node(
        {
            **checkpoint,
            "human_decision": "COMPLETED",
            "certified_kpis": certified_kpis,
            "gate1_decision": "APPROVED",
            "gate1": {"decision": "APPROVED", "status": "COMPLETED"},
        }
    )
    if discovered.get("status") == "FAILED":
        save_checkpoint_state(run_id, discovered)
        return discovered
    nominated = sftp_feed_nomination_node(discovered)
    waiting = {
        **nominated,
        "status": "HITL_WAIT",
        "next_gate": 2,
        "gate2": {"decision": None, "status": "PENDING"},
        "resume_message": "Feed Review is pending. Approve the discovered ADLS source files.",
    }
    save_checkpoint_state(run_id, waiting)
    return waiting


def submit_sftp_gate2_review(
    run_id: str,
    approve: bool = True,
    approved_keys: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    from services.file_metadata import persist_file_design
    from services.pipeline_runtime import (
        load_checkpoint_state,
        run_with_minimum_stage_runtime,
        save_checkpoint_state,
    )
    from sftp_nodes.column_profiling import sftp_column_profiling_node
    from sftp_nodes.metadata_discovery import file_metadata_discovery_node
    from sftp_nodes.semantic_enrichment import sftp_semantic_enrichment_node
    from utilis.logger import logger

    checkpoint = load_checkpoint_state(run_id) or {"run_id": run_id}
    if not approve:
        rejected = {
            **checkpoint,
            "status": "FAILED",
            "gate2_decision": "REJECTED",
            "gate2": {"decision": "REJECTED", "status": "COMPLETED"},
            "error": "Feed Review was rejected.",
        }
        save_checkpoint_state(run_id, rejected)
        return rejected
    certified = _approved_feeds(checkpoint.get("nominated_tables") or [], approved_keys)
    working = {
        **checkpoint,
        "status": "IN_PROGRESS",
        "gate2_decision": "APPROVED",
        "gate2": {"decision": "APPROVED", "status": "COMPLETED"},
        "certified_tables": certified,
        "human_table_decision": "COMPLETED",
    }
    for stage_key, stage_label, runner in (
        ("discovery", "Column Extraction", file_metadata_discovery_node),
        ("profiling", "Column Profiling", sftp_column_profiling_node),
        ("enrichment", "Semantic Enrichment", sftp_semantic_enrichment_node),
    ):
        logger.info(
            "START %s",
            stage_label,
            extra={"run_id": run_id, "node": stage_key, "stage": stage_key, "event_type": "stage_start"},
        )
        working = run_with_minimum_stage_runtime(stage_key, runner, working)
        if str(working.get("status") or "").upper() == "FAILED":
            logger.error(
                "FAILED %s: %s",
                stage_label,
                working.get("error") or working.get(f"{stage_key}_error") or "unknown error",
                extra={"run_id": run_id, "node": stage_key, "stage": stage_key, "event_type": "stage_error"},
            )
            save_checkpoint_state(run_id, working)
            return working
        logger.info(
            "END %s",
            stage_label,
            extra={"run_id": run_id, "node": stage_key, "stage": stage_key, "event_type": "stage_end"},
        )
    working = persist_file_design(
        working,
        (working.get("discovered_metadata") or {}).get("tables") or working.get("certified_tables") or [],
    )
    waiting = {
        **working,
        "status": "HITL_WAIT",
        "next_gate": 3,
        "enrichment_review_status": "PENDING",
        "resume_message": "Semantic Review is pending.",
    }
    save_checkpoint_state(run_id, waiting)
    return waiting


def submit_sftp_gate3_review(
    run_id: str,
    approve: bool = True,
    enriched_metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    from services.pipeline_runtime import (
        _persist_generated_layer,
        _run_database_metadata_ddl_stage,
        load_checkpoint_state,
        save_checkpoint_state,
    )

    checkpoint = load_checkpoint_state(run_id) or {"run_id": run_id}
    if not approve:
        rejected = {
            **checkpoint,
            "status": "FAILED",
            "enrichment_review_decision": "REJECTED",
            "error": "Semantic Review was rejected.",
        }
        save_checkpoint_state(run_id, rejected)
        return rejected
    approved = {
        **checkpoint,
        "enriched_metadata": enriched_metadata or checkpoint.get("enriched_metadata") or {},
        "enrichment_review_artifact": enriched_metadata
        or checkpoint.get("enriched_metadata")
        or {},
        "enrichment_review_decision": "APPROVED",
        "enrichment_review_status": "COMPLETED",
        "next_gate": None,
        "status": "RUNNING",
    }
    generated = _run_database_metadata_ddl_stage(approved)
    save_checkpoint_state(run_id, generated)
    _persist_generated_layer(run_id, generated, "metadata")
    return generated


def submit_sftp_gate4_review(
    run_id: str,
    action: str = "APPROVED",
    review_artifact: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    from services.pipeline_runtime import load_checkpoint_state, save_checkpoint_state, submit_gate4_review
    from sftp_nodes.silver_merge_key_resolution import adls_silver_merge_key_resolution_node

    checkpoint = load_checkpoint_state(run_id) or {"run_id": run_id}
    reviewed = submit_gate4_review(run_id, action, review_artifact, checkpoint)
    if (
        str(action or "").upper() == "APPROVED"
        and str(reviewed.get("source") or "").lower() == "adls_gen2"
        and reviewed.get("next_review_key") == "silver_merge_key_review"
    ):
        reviewed = adls_silver_merge_key_resolution_node(reviewed)
        save_checkpoint_state(run_id, reviewed)
    return reviewed


def submit_sftp_silver_merge_key_review(
    run_id: str,
    action: str = "APPROVED",
    review_artifact: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    from services.pipeline_runtime import load_checkpoint_state, submit_silver_merge_key_review
    from sftp_nodes.silver_merge_key_resolution import validate_adls_merge_key_review

    checkpoint = load_checkpoint_state(run_id) or {"run_id": run_id}
    artifact = review_artifact or checkpoint.get("silver_merge_key_review_artifact") or {}
    if str(action or "").upper() == "APPROVED":
        artifact = validate_adls_merge_key_review(checkpoint, artifact)
    return submit_silver_merge_key_review(run_id, action, artifact)


def submit_sftp_gate5_review(
    run_id: str,
    action: str = "APPROVED",
    review_artifact: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    from services.pipeline_runtime import submit_gate5_review

    return submit_gate5_review(run_id, action, review_artifact)
