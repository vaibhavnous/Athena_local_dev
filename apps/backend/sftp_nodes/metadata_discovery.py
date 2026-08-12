from __future__ import annotations

from services.adls_source import infer_schemas
from state import Stage01State
from utilis.ai_store_writer import ai_store_db_writer


def file_metadata_discovery_node(state: Stage01State) -> Stage01State:
    approved = state.get("certified_tables") or state.get("nominated_tables") or []
    try:
        tables = infer_schemas(approved)
    except Exception as exc:
        return {
            **state,
            "status": "FAILED",
            "metadata_status": "FAILED",
            "metadata_error": f"ADLS schema inference failed: {exc}",
        }
    columns = [column for table in tables for column in table.get("columns") or []]
    payload = {"tables": tables, "columns": columns}
    ai_store_db_writer(
        run_id=str(state.get("run_id") or ""),
        stage="Metadata Discovery",
        artifact_type="DISCOVERED_METADATA",
        payload=payload,
        schema_version="ADLS_DISCOVERED_METADATA_v1",
        prompt_version="DETERMINISTIC_SCHEMA_INFERENCE_v1",
        faithfulness_status="PASSED",
        token_count=0,
        input_tokens=0,
        output_tokens=0,
        fingerprint=str(state.get("fingerprint") or state.get("run_id") or ""),
    )
    return {
        **state,
        "status": "IN_PROGRESS",
        "metadata_status": "COMPLETED",
        "discovered_metadata": payload,
        "file_schema_entries": tables,
        "certified_tables": tables,
    }
