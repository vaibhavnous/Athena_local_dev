from __future__ import annotations

from typing import Any, Dict

from services.adls_source import discover_files, source_base_path
from state import Stage01State
from utilis.ai_store_writer import ai_store_db_writer
from utilis.logger import logger


def source_ingestion_node(state: Stage01State) -> Stage01State:
    new_state: Dict[str, Any] = dict(state)
    if str(new_state.get("source") or "").lower() != "adls_gen2":
        return {
            **new_state,
            "status": "FAILED",
            "source_ingestion_status": "FAILED",
            "error": "The replacement file-source flow supports source=adls_gen2 only.",
        }
    try:
        feeds = discover_files()
        ai_store_db_writer(
            run_id=str(new_state.get("run_id") or ""),
            stage="Source Object Discovery",
            artifact_type="ADLS_SOURCE_CANDIDATES",
            payload={"source_root": source_base_path(), "candidate_feeds": feeds},
            schema_version="ADLS_SOURCE_CANDIDATES_v1",
            prompt_version="DETERMINISTIC_DISCOVERY_v1",
            faithfulness_status="PASSED",
            token_count=0,
            input_tokens=0,
            output_tokens=0,
            fingerprint=str(new_state.get("fingerprint") or new_state.get("run_id") or ""),
        )
        return {
            **new_state,
            "status": "IN_PROGRESS",
            "source_ingestion_status": "COMPLETED",
            "source_root": source_base_path(),
            "candidate_feeds": feeds,
            "candidate_feed": feeds[0] if len(feeds) == 1 else {},
            "source_file_count": len(feeds),
            "sftp_entity": "auto",
            "vendor": "Insurance",
        }
    except Exception as exc:
        logger.exception("ADLS source discovery failed", extra={"run_id": new_state.get("run_id")})
        return {
            **new_state,
            "status": "FAILED",
            "source_ingestion_status": "FAILED",
            "error": f"ADLS source discovery failed: {exc}",
        }
