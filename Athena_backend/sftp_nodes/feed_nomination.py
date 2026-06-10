from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

from state import Stage01State
from utilis.db import config, get_pipeline_connection
from utilis.logger import logger


def _normalize_table_name(feed: Dict[str, Any]) -> str:
    entity = str(feed.get("entity") or "").strip()
    table_name = str(feed.get("table_name") or entity or feed.get("file_name") or "unknown").strip()
    return table_name.replace(" ", "_").lower()


def _nominate_feed(feed: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "database_name": str(feed.get("database_name") or "insurance").strip(),
        "schema_name": str(feed.get("schema_name") or "bronze").strip(),
        "table_name": _normalize_table_name(feed),
        "vendor": str(feed.get("vendor") or "Vendor1").strip(),
        "entity": str(feed.get("entity") or "").strip(),
        "format": str(feed.get("format") or "csv").strip(),
        "file_path": str(feed.get("file_path") or feed.get("file_name") or "").strip(),
        "source": str(feed.get("source") or "sftp").strip(),
        "status": "NOMINATED",
    }


def _normalize_feed_registry_entry(feed: Dict[str, Any], status: str = "NOMINATED") -> Dict[str, Any]:
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


def _persist_nominated_feeds(feeds: List[Dict[str, Any]]) -> None:
    if not feeds:
        return

    db_conf = config["azure_sql"]
    schema = db_conf["pipeline_schema"]
    conn = get_pipeline_connection()
    try:
        cursor = conn.cursor()
        for feed in feeds:
            entry = _normalize_feed_registry_entry(feed, status="NOMINATED")
            cursor.execute(
                f"SELECT 1 FROM [{schema}].[file_feed_registry] WHERE feed_id = ?",
                entry["feed_id"],
            )
            existing = cursor.fetchone()
            if existing:
                cursor.execute(
                    f"""
                    UPDATE [{schema}].[file_feed_registry]
                    SET vendor = ?, entity = ?, format = ?, file_name = ?, file_path = ?, remote_path = ?, status = ?, source = ?, last_modified_at = ?, updated_at = SYSUTCDATETIME()
                    WHERE feed_id = ?
                    """,
                    entry["vendor"],
                    entry["entity"],
                    entry["format"],
                    entry["file_name"],
                    entry["file_path"],
                    entry["remote_path"],
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
        logger.warning("SFTP feed nomination registry persistence skipped: %s", exc)
    finally:
        conn.close()


def sftp_feed_nomination_node(state: Stage01State) -> Stage01State:
    new_state = state.copy()
    log_context = {
        "run_id": new_state.get("run_id", "unknown"),
        "node": "sftp_feed_nomination",
        "stage": "feed_nomination",
    }

    candidate_feeds = new_state.get("candidate_feeds") or []
    candidate_feed = new_state.get("candidate_feed") or {}
    nominated_tables: List[Dict[str, Any]] = []

    if candidate_feeds:
        nominated_tables = [_nominate_feed(feed) for feed in candidate_feeds if isinstance(feed, dict)]
    elif isinstance(candidate_feed, dict) and candidate_feed:
        nominated_tables = [_nominate_feed(candidate_feed)]

    if not nominated_tables:
        logger.warning(
            "SFTP feed nomination found no candidate feeds",
            extra={**log_context, "event_type": "stage_warning"},
        )
        new_state["table_nomination_status"] = "SKIPPED"
        new_state["table_nomination_error"] = "No candidate feeds available for nomination"
        return new_state

    logger.info(
        "SFTP feed nomination completed: nominated_tables=%d",
        len(nominated_tables),
        extra={**log_context, "event_type": "stage_end"},
    )

    _persist_nominated_feeds(nominated_tables)

    new_state["nominated_tables"] = nominated_tables
    new_state["table_nomination_status"] = "COMPLETED"
    new_state["table_nomination_error"] = None
    new_state["human_table_decision"] = "COMPLETED"
    return new_state
