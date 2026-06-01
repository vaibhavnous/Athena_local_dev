from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from state import Stage01State
from utilis.db import ai_store_db_writer, config, get_pipeline_connection
from utilis.logger import logger


def _copy_state(state: Stage01State) -> Stage01State:
    return state.copy()


def _schema_root() -> Path:
    configured = os.getenv("ATHENA_SFTP_SCHEMA_ROOT", "").strip()
    if configured:
        return Path(configured)
    return Path(__file__).resolve().parents[1] / "schemas"


def _sample_limit() -> int:
    return max(1, int(os.getenv("ATHENA_SFTP_SCHEMA_SAMPLE_ROWS", "10000")))


def _approved_feeds_from_state(state: Stage01State) -> List[Dict[str, Any]]:
    feeds = state.get("candidate_feeds") or []
    if feeds:
        return [dict(feed) for feed in feeds]
    candidate = state.get("candidate_feed")
    return [dict(candidate)] if isinstance(candidate, dict) and candidate else []


def _logical_source_path(source_type: str, vendor: str, entity: str) -> str:
    if source_type == "adls_gen2":
        return f"abfss://.../{vendor}/{entity}/"
    return f"/Volumes/.../sftp_landing/{vendor}/{entity}/"


def _declared_schema_candidates(vendor: str, entity: str) -> List[Path]:
    root = _schema_root()
    return [
        root / vendor / f"{entity}.json",
        root / f"{vendor}_{entity}.json",
        root / f"{entity}.json",
    ]


def _load_declared_schema(vendor: str, entity: str) -> Optional[List[Dict[str, Any]]]:
    for path in _declared_schema_candidates(vendor, entity):
        if not path.exists():
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        fields = payload.get("fields") if isinstance(payload, dict) else payload
        if not isinstance(fields, list):
            raise ValueError(f"Declared schema at {path} must be a list or have a fields list")

        normalized: List[Dict[str, Any]] = []
        for item in fields:
            if not isinstance(item, dict):
                continue
            normalized.append(
                {
                    "column_name": str(item.get("column_name") or item.get("name") or "").strip(),
                    "data_type": str(item.get("data_type") or item.get("type") or "string").strip().lower(),
                    "nullable": bool(item.get("nullable", True)),
                }
            )
        normalized = [item for item in normalized if item["column_name"]]
        if not normalized:
            raise ValueError(f"Declared schema at {path} resolved to zero columns")
        return normalized
    return None


def _read_sample_dataframe(file_path: str, file_format: str) -> pd.DataFrame:
    limit = _sample_limit()
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
        return pd.read_xml(file_path)
    raise ValueError(f"Unsupported file format for schema extraction: {file_format}")


def _normalize_dtype(dtype: Any) -> str:
    text = str(dtype).lower()
    if "int" in text:
        return "int"
    if "float" in text or "double" in text:
        return "double"
    if "bool" in text:
        return "boolean"
    if "datetime" in text or text.startswith("date"):
        return "timestamp"
    return "string"


def _infer_schema(file_path: str, file_format: str) -> List[Dict[str, Any]]:
    sample = _read_sample_dataframe(file_path, file_format)
    if sample.empty and len(sample.columns) == 0:
        raise ValueError("Schema inference produced no columns")
    return [
        {
            "column_name": str(column),
            "data_type": _normalize_dtype(sample[column].dtype),
            "nullable": bool(sample[column].isnull().any()),
        }
        for column in sample.columns
    ]


def _schema_strategy(file_format: str, declared_schema: Optional[List[Dict[str, Any]]]) -> str:
    lower_format = str(file_format or "").lower()
    if lower_format == "xml" and not declared_schema:
        raise ValueError("XML feed requires declared schema but none was found")
    if declared_schema:
        return "declared"
    if lower_format in {"csv", "json"}:
        return "infer"
    raise ValueError(f"Unsupported schema strategy for format={file_format}")


def _schema_fingerprint(schema_columns: List[Dict[str, Any]]) -> str:
    schema_str = json.dumps(schema_columns, sort_keys=True)
    return hashlib.md5(schema_str.encode("utf-8")).hexdigest()


def _persist_schema_registry_entry(entry: Dict[str, Any], *, log_context: Dict[str, Any]) -> int:
    conn = get_pipeline_connection()
    table_schema = config["azure_sql"]["pipeline_schema"]
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT TOP 1 schema_fingerprint, version
            FROM [{table_schema}].[file_feed_schema_registry]
            WHERE feed_id = ?
            ORDER BY version DESC, discovered_at DESC
            """,
            entry["feed_id"],
        )
        row = cursor.fetchone()
        latest_fingerprint = str(row.schema_fingerprint) if row and getattr(row, "schema_fingerprint", None) is not None else None
        latest_version = int(row.version) if row and getattr(row, "version", None) is not None else 0

        if latest_fingerprint == entry["schema_fingerprint"]:
            return latest_version or 1

        version = latest_version + 1
        cursor.execute(
            f"""
            INSERT INTO [{table_schema}].[file_feed_schema_registry]
            (feed_id, vendor, entity, format, schema_json, schema_fingerprint, version, discovered_at, source_type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            entry["feed_id"],
            entry["vendor"],
            entry["entity"],
            entry["format"],
            json.dumps(entry["schema_json"]),
            entry["schema_fingerprint"],
            version,
            entry["discovered_at"],
            entry["source_type"],
        )
        conn.commit()
        return version
    except Exception as exc:
        logger.warning("SFTP schema registry persistence skipped: %s", exc, extra=log_context)
        return int(entry.get("version") or 1)
    finally:
        conn.close()


def sftp_metadata_discovery_node(state: Stage01State) -> Stage01State:
    new_state = _copy_state(state)
    log_context = {
        "run_id": new_state.get("run_id", "unknown"),
        "node": "sftp_metadata_discovery",
        "stage": "sftp_metadata_discovery",
    }
    logger.info("SFTP metadata discovery starting", extra={**log_context, "event_type": "stage_start"})

    if new_state.get("status") == "FAILED":
        return new_state

    feeds = _approved_feeds_from_state(new_state)
    if not feeds:
        new_state["metadata_status"] = "SKIPPED"
        new_state["metadata_error"] = "No approved SFTP feeds available for schema discovery"
        return new_state

    fingerprint = str(new_state.get("fingerprint") or new_state.get("run_id") or "unknown")
    discovered_at = datetime.now(timezone.utc).isoformat()
    schema_registry_entries: List[Dict[str, Any]] = []

    try:
        for feed in feeds:
            vendor = str(feed.get("vendor") or new_state.get("vendor") or "Vendor1")
            entity = str(feed.get("entity") or "unknown")
            file_format = str(feed.get("format") or "unknown").lower()
            file_path = str(feed.get("file_path") or "").strip()
            if not file_path:
                raise ValueError(f"Missing file_path for feed {vendor}.{entity}")

            declared_schema = _load_declared_schema(vendor, entity)
            strategy = _schema_strategy(file_format, declared_schema)
            normalized_schema = declared_schema if declared_schema is not None else _infer_schema(file_path, file_format)
            normalized_schema = [
                {
                    "column_name": item["column_name"],
                    "data_type": str(item.get("data_type") or "string").lower(),
                    "nullable": bool(item.get("nullable", True)),
                    "source_format": file_format,
                }
                for item in normalized_schema
            ]
            schema_fingerprint = _schema_fingerprint(normalized_schema)
            entry = {
                "feed_id": str(feed.get("feed_id") or f"{vendor}_{entity}"),
                "vendor": vendor,
                "entity": entity,
                "format": file_format,
                "schema_json": normalized_schema,
                "schema_fingerprint": schema_fingerprint,
                "version": 1,
                "discovered_at": discovered_at,
                "source_type": str(feed.get("source") or new_state.get("source") or "sftp"),
                "source_path": _logical_source_path(str(feed.get("source") or new_state.get("source") or "sftp"), vendor, entity),
                "local_file_path": file_path,
                "schema_strategy": strategy,
            }
            entry["version"] = _persist_schema_registry_entry(entry, log_context=log_context)
            schema_registry_entries.append(entry)

        payload = {
            "fingerprint": fingerprint,
            "run_id": new_state.get("run_id"),
            "feed_count": len(schema_registry_entries),
            "discovered_at": discovered_at,
            "schema_registry": schema_registry_entries,
        }
        ai_store_db_writer(
            run_id=str(new_state.get("run_id") or "unknown"),
            stage="SFTP Metadata Discovery",
            artifact_type="SFTP_SCHEMA_SNAPSHOT",
            payload=payload,
            schema_version="SFTP_SCHEMA_SNAPSHOT_v1",
            prompt_version="DETERMINISTIC_FILE_SCHEMA_v1",
            faithfulness_status="NOT_APPLICABLE",
            token_count=0,
            input_tokens=0,
            output_tokens=0,
            fingerprint=fingerprint,
        )

        new_state.update(
            {
                "discovered_metadata": payload,
                "metadata_status": "COMPLETED",
                "metadata_error": None,
                "status": "IN_PROGRESS",
            }
        )
        logger.info("SFTP metadata discovery completed: feeds=%d", len(schema_registry_entries), extra={**log_context, "event_type": "stage_end"})
        return new_state
    except Exception as exc:
        new_state.update(
            {
                "metadata_status": "FAILED",
                "metadata_error": str(exc),
                "status": "FAILED",
                "error": f"SFTP metadata discovery failed: {exc}",
            }
        )
        logger.error("SFTP metadata discovery failed: %s", exc, extra=log_context)
        return new_state
