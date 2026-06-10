from __future__ import annotations

import importlib.util
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from sftp_nodes.hitl import hitl_controller
from state import Stage01State
from utilis.db import config, get_pipeline_connection
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


def _normalize_feed_registry_entry(feed: Dict[str, Any], status: str = "DISCOVERED") -> Dict[str, Any]:
    feed_id = str(feed.get("feed_id") or f"{feed.get('vendor')}_{feed.get('entity')}").strip()
    return {
        "feed_id": feed_id,
        "vendor": str(feed.get("vendor") or "unknown").strip(),
        "entity": str(feed.get("entity") or "unknown").strip(),
        "format": str(feed.get("format") or "unknown").strip(),
        "file_name": str(feed.get("file_name") or "").strip(),
        "file_path": str(feed.get("file_path") or "").strip(),
        "remote_path": str(feed.get("remote_path") or feed.get("file_path") or "").strip(),
        "status": status,
        "source": str(feed.get("source") or "sftp").strip(),
        "last_modified_at": datetime.now(timezone.utc).isoformat(),
        "approved_at": None,
    }


def _persist_file_feed_registry(feeds: List[Dict[str, Any]], status: str = "DISCOVERED") -> None:
    if not feeds:
        return

    db_conf = config["azure_sql"]
    schema = db_conf["pipeline_schema"]
    conn = get_pipeline_connection()
    try:
        cursor = conn.cursor()
        for feed in feeds:
            entry = _normalize_feed_registry_entry(feed, status=status)
            cursor.execute(
                f"SELECT 1 FROM [{schema}].[file_feed_registry] WHERE feed_id = ?",
                entry["feed_id"],
            )
            existing = cursor.fetchone()
            if existing:
                cursor.execute(
                    f"""
                    UPDATE [{schema}].[file_feed_registry]
                    SET vendor = ?,
                        entity = ?,
                        format = ?,
                        file_name = ?,
                        file_path = ?,
                        remote_path = ?,
                        status = CASE
                            WHEN UPPER(status) = 'APPROVED' AND ? = 'DISCOVERED' THEN status
                            ELSE ?
                        END,
                        source = ?,
                        last_modified_at = ?,
                        updated_at = SYSUTCDATETIME()
                    WHERE feed_id = ?
                    """,
                    entry["vendor"],
                    entry["entity"],
                    entry["format"],
                    entry["file_name"],
                    entry["file_path"],
                    entry["remote_path"],
                    entry["status"],
                    entry["status"],
                    entry["source"],
                    entry["last_modified_at"],
                    entry["feed_id"],
                )
            else:
                cursor.execute(
                    f"""
                    INSERT INTO [{schema}].[file_feed_registry]
                    (feed_id, vendor, entity, format, file_name, file_path, remote_path, status, source, last_modified_at, approved_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    entry["feed_id"],
                    entry["vendor"],
                    entry["entity"],
                    entry["format"],
                    entry["file_name"],
                    entry["file_path"],
                    entry["remote_path"],
                    entry["status"],
                    entry["source"],
                    entry["last_modified_at"],
                    entry["approved_at"],
                )
        conn.commit()
    except Exception as exc:
        logger.warning("SFTP feed registry persistence skipped: %s", exc)
    finally:
        conn.close()


def _mark_registry_feeds_approved(feeds: List[Dict[str, Any]]) -> None:
    if not feeds:
        return
    db_conf = config["azure_sql"]
    schema = db_conf["pipeline_schema"]
    conn = get_pipeline_connection()
    try:
        cursor = conn.cursor()
        approved_at = datetime.now(timezone.utc).isoformat()
        for feed in feeds:
            feed_id = str(feed.get("feed_id") or f"{feed.get('vendor')}_{feed.get('entity')}").strip()
            cursor.execute(
                f"""
                UPDATE [{schema}].[file_feed_registry]
                SET status = 'APPROVED', approved_at = ?, updated_at = SYSUTCDATETIME()
                WHERE feed_id = ?
                """,
                approved_at,
                feed_id,
            )
        conn.commit()
    except Exception as exc:
        logger.warning("SFTP feed approval persistence skipped: %s", exc)
    finally:
        conn.close()


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
    if len(new_state.get("sftp_files") or []) > 1 or str(new_state.get("sftp_entity") or "transactions").lower() in {"both", "auto", "multi"}:
        files = new_state.get("sftp_files") or []
        candidate_feeds = []
        feed_node = _load_feed_discovery_node()
        for file_path in files:
            item_state = dict(new_state)
            item_state["file_path"] = file_path
            discovered = feed_node(item_state)
            if discovered.get("candidate_feed"):
                candidate_feeds.append(discovered["candidate_feed"])
        unique_candidate_feeds = []
        seen_feed_keys = set()
        for feed in candidate_feeds:
            entity = str(feed.get("entity") or "").strip().lower()
            file_format = str(feed.get("format") or "").strip().lower()
            feed_key = (entity, file_format)
            if entity and feed_key not in seen_feed_keys:
                seen_feed_keys.add(feed_key)
                unique_candidate_feeds.append(feed)
        candidate_feeds = unique_candidate_feeds
        candidate_feeds = sorted(candidate_feeds, key=lambda item: str(item.get("entity") or ""))
        new_state["candidate_feeds"] = candidate_feeds
        _persist_file_feed_registry(candidate_feeds, status="DISCOVERED")
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
            "entity": "multi",
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
    _persist_file_feed_registry([candidate_feed], status="DISCOVERED")
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
        feeds = [dict(feed) for feed in (new_state.get("candidate_feeds") or []) if isinstance(feed, dict)]
        if not feeds and isinstance(new_state.get("candidate_feed"), dict):
            feeds = [dict(new_state["candidate_feed"])]
        _mark_registry_feeds_approved(feeds)
        return new_state

    new_state["status"] = "FAILED"
    new_state["error"] = f"Gate 2 rejected: {result.get('reason')}"
    return new_state
