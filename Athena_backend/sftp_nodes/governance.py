from __future__ import annotations

import importlib.util
import os
from pathlib import Path
from typing import Any, Dict

from sftp_nodes.hitl import hitl_controller
from state import Stage01State
from utilis.logger import logger


def _load_feed_discovery_node():
    module_path = Path(__file__).resolve().parent / "feed discovery.py"
    spec = importlib.util.spec_from_file_location("sftp_feed_discovery", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load feed discovery module: {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if not hasattr(module, "feed_discovery_node"):
        raise RuntimeError("feed_discovery_node not found in sftp feed discovery module")
    return module.feed_discovery_node


def sftp_gate1_node(state: Stage01State) -> Stage01State:
    new_state: Dict[str, Any] = dict(state)
    # Treat ADLS Gen2 as the same "file pipeline" as SFTP.
    if str(new_state.get("source") or "").lower() not in {"sftp", "adls_gen2"}:
        return new_state
    log_context = {
        "run_id": new_state.get("run_id", "unknown"),
        "node": "sftp_gate1",
        "stage": "sftp_gate1",
    }

    auto_mode = os.getenv("ATHENA_SFTP_HITL_AUTO", "").strip().lower() in {"1", "true", "yes", "on"}
    override = str(new_state.get("gate1_decision") or "").strip().upper()
    if not auto_mode and override not in {"APPROVED", "REJECTED"}:
        logger.info(
            "SFTP Gate 1 pending review for %d KPI(s)",
            len(new_state.get("kpis") or []),
            extra={**log_context, "event_type": "stage_start"},
        )
        new_state["gate1"] = {
            "gate": "gate1",
            "status": "PENDING",
            "decision": None,
            "reason": "Awaiting Gate 1 submission",
            "payload_summary": {"kpi_count": len(new_state.get("kpis") or [])},
        }
        new_state["status"] = "HITL_WAIT"
        return new_state

    if override in {"APPROVED", "REJECTED"}:
        result = {
            "gate": "gate1",
            "status": "COMPLETED",
            "decision": override,
            "reason": "Submitted via API",
            "payload_summary": {"kpi_count": len(new_state.get("kpis") or [])},
        }
    else:
        result = hitl_controller.decide("gate1", {"kpis": new_state.get("kpis") or []})

    new_state["gate1"] = result
    logger.info(
        "SFTP Gate 1 decision=%s reason=%s",
        result.get("decision"),
        result.get("reason"),
        extra={**log_context, "event_type": "stage_end"},
    )

    if result.get("decision") == "APPROVED":
        new_state["human_decision"] = "COMPLETED"
        return new_state

    new_state["status"] = "FAILED"
    new_state["error"] = f"Gate 1 rejected: {result.get('reason')}"
    return new_state


def sftp_feed_discovery_node(state: Stage01State) -> Stage01State:
    new_state: Dict[str, Any] = dict(state)
    if str(new_state.get("source") or "").lower() not in {"sftp", "adls_gen2"}:
        return new_state
    log_context = {
        "run_id": new_state.get("run_id", "unknown"),
        "node": "sftp_feed_discovery",
        "stage": "sftp_feed_discovery",
    }
    logger.info("SFTP feed discovery starting", extra={**log_context, "event_type": "stage_start"})
    if str(new_state.get("sftp_entity") or "transactions").lower() == "both":
        files = new_state.get("sftp_files") or []
        candidate_feeds = []
        feed_node = _load_feed_discovery_node()
        for file_path in files:
            item_state = dict(new_state)
            item_state["file_path"] = file_path
            discovered = feed_node(item_state)
            if discovered.get("candidate_feed"):
                candidate_feeds.append(discovered["candidate_feed"])
        candidate_feeds = sorted(candidate_feeds, key=lambda item: str(item.get("entity") or ""))
        new_state["candidate_feeds"] = candidate_feeds
        total_rows = sum(int(feed.get("sample_row_count") or 0) for feed in candidate_feeds)
        all_columns = sorted(
            {
                str(column).lower()
                for feed in candidate_feeds
                for column in (feed.get("columns") or [])
                if str(column).strip()
            }
        )
        all_primary_keys = sorted(
            {
                str(column).lower()
                for feed in candidate_feeds
                for column in (feed.get("primary_keys") or [])
                if str(column).strip()
            }
        )
        all_measures = sorted(
            {
                str(column).lower()
                for feed in candidate_feeds
                for column in (feed.get("measures") or [])
                if str(column).strip()
            }
        )
        file_names = [str(feed.get("file_name")) for feed in candidate_feeds if str(feed.get("file_name") or "").strip()]
        entities = [str(feed.get("entity")) for feed in candidate_feeds if str(feed.get("entity") or "").strip()]
        new_state["candidate_feed"] = {
            "feed_id": "Vendor1_both",
            "vendor": "Vendor1",
            "entity": "both",
            "semantic_type": "multi-feed",
            "format": "mixed",
            "file_name": ", ".join(file_names),
            "file_names": file_names,
            "entities": entities,
            "feed_count": len(candidate_feeds),
            "sample_row_count": total_rows,
            "columns": all_columns,
            "primary_keys": all_primary_keys,
            "measures": all_measures,
            "source": str(new_state.get("source") or "sftp"),
            "status": "CANDIDATE",
        }
        logger.info(
            "SFTP feed discovery completed for both feeds: feeds=%d rows=%d",
            len(candidate_feeds),
            total_rows,
            extra={**log_context, "event_type": "stage_end"},
        )
        return new_state

    feed_node = _load_feed_discovery_node()
    discovered = feed_node(new_state)
    candidate_feed = discovered.get("candidate_feed") or {}
    logger.info(
        "SFTP feed discovery completed: entity=%s format=%s rows=%s",
        candidate_feed.get("entity"),
        candidate_feed.get("format"),
        candidate_feed.get("sample_row_count"),
        extra={**log_context, "event_type": "stage_end"},
    )
    return discovered


def sftp_gate2_node(state: Stage01State) -> Stage01State:
    new_state: Dict[str, Any] = dict(state)
    if str(new_state.get("source") or "").lower() not in {"sftp", "adls_gen2"}:
        return new_state
    log_context = {
        "run_id": new_state.get("run_id", "unknown"),
        "node": "sftp_gate2",
        "stage": "sftp_gate2",
    }

    auto_mode = os.getenv("ATHENA_SFTP_HITL_AUTO", "").strip().lower() in {"1", "true", "yes", "on"}
    override = str(new_state.get("gate2_decision") or "").strip().upper()

    if not auto_mode and override not in {"APPROVED", "REJECTED"}:
        logger.info("SFTP Gate 2 pending review", extra={**log_context, "event_type": "stage_start"})
        new_state["gate2"] = {
            "gate": "gate2",
            "status": "PENDING",
            "decision": None,
            "reason": "Awaiting Gate 2 submission",
            "payload_summary": (new_state.get("candidate_feed") or {}),
        }
        new_state["status"] = "HITL_WAIT"
        return new_state

    if override in {"APPROVED", "REJECTED"}:
        result = {
            "gate": "gate2",
            "status": "COMPLETED",
            "decision": override,
            "reason": "Submitted via API",
            "payload_summary": (new_state.get("candidate_feed") or {}),
        }
    else:
        payload = new_state.get("candidate_feed") or {}
        result = hitl_controller.decide("gate2", payload)

    new_state["gate2"] = result
    logger.info(
        "SFTP Gate 2 decision=%s reason=%s",
        result.get("decision"),
        result.get("reason"),
        extra={**log_context, "event_type": "stage_end"},
    )
    if result.get("decision") == "APPROVED":
        return new_state

    new_state["status"] = "FAILED"
    new_state["error"] = f"Gate 2 rejected: {result.get('reason')}"
    return new_state
