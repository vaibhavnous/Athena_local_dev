from __future__ import annotations

from typing import Any, Dict, List

from state import Stage01State
from utilis.ai_store_writer import ai_store_db_writer


def _nominate(feed: Dict[str, Any]) -> Dict[str, Any]:
    return {
        **feed,
        "database_name": str(feed.get("database_name") or "insurance"),
        "schema_name": str(feed.get("schema_name") or "source"),
        "table_name": str(feed.get("table_name") or feed.get("entity") or "").strip().lower(),
        "format": str(feed.get("file_format") or feed.get("format") or "").lower(),
        "status": "NOMINATED",
    }


def sftp_feed_nomination_node(state: Stage01State) -> Stage01State:
    feeds: List[Dict[str, Any]] = [
        _nominate(item)
        for item in state.get("candidate_feeds") or []
        if isinstance(item, dict)
    ]
    if not feeds:
        return {
            **state,
            "status": "FAILED",
            "table_nomination_status": "FAILED",
            "table_nomination_error": "No ADLS source files are available for nomination.",
        }
    ai_store_db_writer(
        run_id=str(state.get("run_id") or ""),
        stage="Feed Nomination",
        artifact_type="TABLE_NOMINATIONS",
        payload={"nominations": feeds},
        schema_version="FILE_NOMINATION_v1",
        prompt_version="DETERMINISTIC_NOMINATION_v1",
        faithfulness_status="PASSED",
        token_count=0,
        input_tokens=0,
        output_tokens=0,
        fingerprint=str(state.get("fingerprint") or state.get("run_id") or ""),
    )
    return {
        **state,
        "nominated_tables": feeds,
        "table_nomination_status": "COMPLETED",
        "table_nomination_error": None,
    }
