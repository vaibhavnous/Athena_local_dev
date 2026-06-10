from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List

import pandas as pd

from state import Stage01State
from utilis.db import ai_store_db_writer, config, get_pipeline_connection
from utilis.logger import logger


def _copy_state(state: Stage01State) -> Stage01State:
    return state.copy()


def _profile_limit() -> int:
    import os
    return max(1, int(os.getenv("ATHENA_SFTP_PROFILE_SAMPLE_ROWS", "10000")))


def _approved_feeds_from_state(state: Stage01State) -> List[Dict[str, Any]]:
    metadata = state.get("discovered_metadata") or {}
    registry = metadata.get("schema_registry") or []
    if registry:
        return [dict(item) for item in registry]
    feeds = state.get("candidate_feeds") or []
    if feeds:
        return [dict(feed) for feed in feeds]
    candidate = state.get("candidate_feed")
    return [dict(candidate)] if isinstance(candidate, dict) and candidate else []


def _read_sample_dataframe(file_path: str, file_format: str) -> pd.DataFrame:
    limit = _profile_limit()
    lower_format = str(file_format or "").lower()
    if lower_format == "csv":
        return pd.read_csv(file_path, nrows=limit)
    if lower_format == "json":
        with open(file_path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, list):
            return pd.DataFrame(payload[:limit])
        return pd.DataFrame([payload])
    if lower_format == "xml":
        return pd.read_xml(file_path).head(limit)
    raise ValueError(f"Unsupported file format for profiling: {file_format}")


def _json_safe(value: Any) -> Any:
    if pd.isna(value):
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, (int, float, str, bool)) or value is None:
        return value
    return str(value)


def _infer_type(series: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(series):
        return "boolean"
    if pd.api.types.is_numeric_dtype(series):
        return "numeric"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "timestamp"
    return "string"


def _top_values(series: pd.Series, limit: int = 5) -> List[Dict[str, Any]]:
    values = [value for value in series.dropna().tolist()]
    counts = Counter(_json_safe(value) for value in values)
    top = counts.most_common(limit)
    return [{"value": value, "count": count} for value, count in top]


def _string_metrics(series: pd.Series) -> Dict[str, Any]:
    non_null = series.dropna().astype(str)
    if non_null.empty:
        return {"min_length": None, "max_length": None, "avg_length": None}
    lengths = non_null.str.len()
    return {
        "min_length": int(lengths.min()),
        "max_length": int(lengths.max()),
        "avg_length": round(float(lengths.mean()), 3),
    }


def _numeric_metrics(series: pd.Series) -> Dict[str, Any]:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return {"min": None, "max": None, "avg": None}
    return {
        "min": _json_safe(numeric.min()),
        "max": _json_safe(numeric.max()),
        "avg": round(float(numeric.mean()), 6),
    }


def _datetime_metrics(series: pd.Series) -> Dict[str, Any]:
    dt = pd.to_datetime(series, errors="coerce").dropna()
    if dt.empty:
        return {"min": None, "max": None}
    return {"min": _json_safe(dt.min()), "max": _json_safe(dt.max())}


def _profile_column(feed: Dict[str, Any], frame: pd.DataFrame, column_name: str) -> Dict[str, Any]:
    series = frame[column_name]
    inferred_type = _infer_type(series)
    row_count = int(len(frame))
    null_count = int(series.isna().sum())
    distinct_count = int(series.nunique(dropna=True))
    metrics: Dict[str, Any] = {
        "row_count": row_count,
        "null_count": null_count,
        "distinct_count": distinct_count,
        "inferred_type": inferred_type,
        "top_values": _top_values(series),
    }

    if inferred_type == "numeric":
        metrics.update(_numeric_metrics(series))
    elif inferred_type == "timestamp":
        metrics.update(_datetime_metrics(series))
    else:
        metrics.update(_string_metrics(series))

    return {
        "feed_id": str(feed.get("feed_id") or f"{feed.get('vendor')}_{feed.get('entity')}"),
        "vendor": feed.get("vendor"),
        "entity": feed.get("entity"),
        "column_name": column_name,
        "metrics_json": metrics,
        "profiled_at": datetime.now(timezone.utc).isoformat(),
        "source_format": feed.get("format"),
    }


def _persist_column_profile_rows(rows: List[Dict[str, Any]], *, log_context: Dict[str, Any]) -> None:
    conn = get_pipeline_connection()
    table_schema = config["azure_sql"]["pipeline_schema"]
    try:
        cursor = conn.cursor()
        for row in rows:
            cursor.execute(
                f"""
                INSERT INTO [{table_schema}].[column_profiles]
                (feed_id, vendor, entity, column_name, metrics_json, profiled_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                row["feed_id"],
                row["vendor"],
                row["entity"],
                row["column_name"],
                json.dumps(row["metrics_json"]),
                row["profiled_at"],
            )
        conn.commit()
    except Exception as exc:
        logger.warning("SFTP column profile persistence skipped: %s", exc, extra=log_context)
    finally:
        conn.close()


def sftp_column_profiling_node(state: Stage01State) -> Stage01State:
    new_state = _copy_state(state)
    log_context = {
        "run_id": new_state.get("run_id", "unknown"),
        "node": "sftp_column_profiling",
        "stage": "sftp_column_profiling",
    }
    logger.info("SFTP column profiling starting", extra={**log_context, "event_type": "stage_start"})

    if new_state.get("status") == "FAILED":
        return new_state

    feeds = _approved_feeds_from_state(new_state)
    if not feeds:
        new_state["column_profiling_status"] = "SKIPPED"
        new_state["column_profiling_error"] = "No approved file-source feeds available for profiling"
        return new_state

    fingerprint = str(new_state.get("fingerprint") or new_state.get("run_id") or "unknown")
    profile_rows: List[Dict[str, Any]] = []
    feed_results: List[Dict[str, Any]] = []
    skipped_feeds: List[Dict[str, Any]] = []

    try:
        for feed in feeds:
            file_path = str(feed.get("local_file_path") or feed.get("sample_file_path") or feed.get("file_path") or "").strip()
            if not file_path:
                skipped_feeds.append(
                    {
                        "feed_id": feed.get("feed_id"),
                        "vendor": feed.get("vendor"),
                        "entity": feed.get("entity"),
                        "reason": "No local sample file available for profiling",
                    }
                )
                continue
            frame = _read_sample_dataframe(file_path, str(feed.get("format") or "unknown"))
            column_names = list(frame.columns)
            for column_name in column_names:
                profile_rows.append(_profile_column(feed, frame, column_name))
            feed_results.append(
                {
                    "feed_id": feed.get("feed_id"),
                    "vendor": feed.get("vendor"),
                    "entity": feed.get("entity"),
                    "row_count": int(len(frame)),
                    "column_count": len(column_names),
                    "sample_file": file_path,
                }
            )

        _persist_column_profile_rows(profile_rows, log_context=log_context)
        payload = {
            "fingerprint": fingerprint,
            "run_id": new_state.get("run_id"),
            "feed_count": len(feed_results),
            "profile_row_count": len(profile_rows),
            "feed_results": feed_results,
            "skipped_feeds": skipped_feeds,
            "column_profiles": profile_rows,
            "sample_limit": _profile_limit(),
            "profiling_strategy": "file_sample_profile_v1",
        }
        ai_store_db_writer(
            run_id=str(new_state.get("run_id") or "unknown"),
            stage="SFTP Column Profiling",
            artifact_type="SFTP_COLUMN_PROFILING",
            payload=payload,
            schema_version="SFTP_COLUMN_PROFILING_v1",
            prompt_version="DETERMINISTIC_FILE_PROFILE_v1",
            faithfulness_status="NOT_APPLICABLE",
            token_count=0,
            input_tokens=0,
            output_tokens=0,
            fingerprint=fingerprint,
        )

        new_state.update(
            {
                "column_profiles": payload,
                "column_profiling_status": "COMPLETED" if profile_rows else "SKIPPED",
                "column_profiling_error": None,
                "status": "IN_PROGRESS",
            }
        )
        logger.info("SFTP column profiling completed: feeds=%d profiles=%d", len(feed_results), len(profile_rows), extra={**log_context, "event_type": "stage_end"})
        return new_state
    except Exception as exc:
        new_state.update(
            {
                "column_profiling_status": "FAILED",
                "column_profiling_error": str(exc),
                "status": "FAILED",
                "error": f"SFTP column profiling failed: {exc}",
            }
        )
        logger.error("SFTP column profiling failed: %s", exc, extra=log_context)
        return new_state
