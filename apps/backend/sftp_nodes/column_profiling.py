from __future__ import annotations

from state import Stage01State
from utilis.ai_store_writer import ai_store_db_writer


def sftp_column_profiling_node(state: Stage01State) -> Stage01State:
    tables = (state.get("discovered_metadata") or {}).get("tables") or []
    profiles = []
    for table in tables:
        for column in table.get("columns") or []:
            sample_count = int(column.get("sample_count") or 0)
            null_count = int(column.get("null_count") or 0)
            profiles.append(
                {
                    **column,
                    "feed_id": table.get("feed_id"),
                    "entity": table.get("entity"),
                    "null_percentage": round((null_count / sample_count) * 100, 4) if sample_count else 0.0,
                    "profile_status": "COMPLETED",
                }
            )
    if not profiles:
        return {
            **state,
            "status": "FAILED",
            "column_profiling_status": "FAILED",
            "column_profiling_error": "No inferred ADLS columns are available for profiling.",
        }
    ai_store_db_writer(
        run_id=str(state.get("run_id") or ""),
        stage="Column Profiling",
        artifact_type="COLUMN_PROFILES",
        payload={"profiles": profiles},
        schema_version="ADLS_COLUMN_PROFILES_v1",
        prompt_version="DETERMINISTIC_PROFILE_v1",
        faithfulness_status="PASSED",
        token_count=0,
        input_tokens=0,
        output_tokens=0,
        fingerprint=str(state.get("fingerprint") or state.get("run_id") or ""),
    )
    return {
        **state,
        "column_profiles": profiles,
        "column_profiling_status": "COMPLETED",
        "column_profiling_error": None,
    }
