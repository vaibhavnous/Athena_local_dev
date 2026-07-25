from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from state import Stage01State
from utilis.db import ai_store_db_writer, config, get_pipeline_connection
from utilis.logger import logger


FILE_SOURCE_TYPES = {"sftp", "adls_gen2", "adls", "file_source", "file-source"}


def _copy_state(state: Stage01State) -> Stage01State:
    return state.copy()


def _pipeline_schema() -> str:
    return config["azure_sql"]["pipeline_schema"]


def _sample_limit() -> int:
    return max(1, int(os.getenv("ATHENA_SFTP_SCHEMA_SAMPLE_ROWS", "1000")))


def _source_type(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"adls", "abfs", "abfss", "blob"}:
        return "adls_gen2"
    if raw in FILE_SOURCE_TYPES:
        return raw
    return "sftp"


def _state_feed_ids(state: Stage01State) -> List[str]:
    feed_ids: List[str] = []
    for feed in state.get("candidate_feeds") or []:
        if isinstance(feed, dict):
            feed_id = str(feed.get("feed_id") or "").strip()
            if feed_id:
                feed_ids.append(feed_id)
    candidate_feed = state.get("candidate_feed")
    if isinstance(candidate_feed, dict):
        feed_id = str(candidate_feed.get("feed_id") or "").strip()
        if feed_id:
            feed_ids.append(feed_id)
    return list(dict.fromkeys(feed_ids))


def _state_feed_map(state: Stage01State) -> Dict[str, Dict[str, Any]]:
    feed_map: Dict[str, Dict[str, Any]] = {}
    for feed in state.get("candidate_feeds") or []:
        if isinstance(feed, dict):
            feed_id = str(feed.get("feed_id") or "").strip()
            if feed_id:
                feed_map[feed_id] = dict(feed)
    candidate_feed = state.get("candidate_feed")
    if isinstance(candidate_feed, dict):
        feed_id = str(candidate_feed.get("feed_id") or "").strip()
        if feed_id and feed_id not in feed_map:
            feed_map[feed_id] = dict(candidate_feed)
    return feed_map


def _approved_registry_feeds(state: Stage01State) -> List[Dict[str, Any]]:
    feed_ids = _state_feed_ids(state)
    source_type = _source_type(state.get("source"))
    conn = get_pipeline_connection()
    try:
        cursor = conn.cursor()
        params: List[Any] = [source_type]
        filters = [
            "UPPER(status) = 'APPROVED'",
            "LOWER(source) = ?",
        ]
        if feed_ids:
            placeholders = ", ".join("?" for _ in feed_ids)
            filters.append(f"feed_id IN ({placeholders})")
            params.extend(feed_ids)

        cursor.execute(
            f"""
            WITH ranked AS (
                SELECT
                    feed_id,
                    vendor,
                    entity,
                    format,
                    file_name,
                    file_path,
                    remote_path,
                    status,
                    source,
                    approved_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY feed_id
                        ORDER BY updated_at DESC, created_at DESC
                    ) AS rn
                FROM [{_pipeline_schema()}].[file_feed_registry]
                WHERE {' AND '.join(filters)}
            )
            SELECT
                feed_id,
                vendor,
                entity,
                format,
                file_name,
                file_path,
                remote_path,
                status,
                source,
                approved_at
            FROM ranked
            WHERE rn = 1
            """,
            *params,
        )
        rows = cursor.fetchall()
    finally:
        conn.close()

    state_feeds = _state_feed_map(state)
    merged: List[Dict[str, Any]] = []
    for row in rows:
        registry_feed = {
            "feed_id": row.feed_id,
            "vendor": row.vendor,
            "entity": row.entity,
            "format": row.format,
            "file_name": row.file_name,
            "file_path": row.file_path,
            "remote_path": row.remote_path,
            "status": row.status,
            "source": _source_type(row.source),
            "approved_at": getattr(row, "approved_at", None),
        }
        merged.append({**registry_feed, **state_feeds.get(str(row.feed_id), {})})
    return merged


def _resolve_existing_path(value: Any) -> Optional[str]:
    candidate = str(value or "").strip()
    if not candidate:
        return None
    path = Path(candidate)
    return str(path) if path.exists() else None


def _landing_candidates(feed: Dict[str, Any], state: Stage01State) -> Iterable[str]:
    yield str(feed.get("local_file_path") or "").strip()
    yield str(feed.get("sample_file_path") or "").strip()
    yield str(feed.get("file_path") or "").strip()

    source_file_mappings = state.get("source_file_mappings") or []
    feed_id = str(feed.get("feed_id") or "").strip()
    entity = str(feed.get("entity") or "").strip().lower()
    for item in source_file_mappings:
        if not isinstance(item, dict):
            continue
        mapped_feed_id = str(item.get("feed_id") or "").strip()
        mapped_entity = str(item.get("entity") or "").strip().lower()
        if feed_id and mapped_feed_id == feed_id:
            yield str(item.get("local_file_path") or item.get("file_path") or "").strip()
        elif entity and mapped_entity == entity:
            yield str(item.get("local_file_path") or item.get("file_path") or "").strip()

    for item in state.get("pulled_files") or []:
        yield str(item or "").strip()

    landing_path = str(state.get("landing_path") or "").strip()
    if landing_path:
        yield landing_path


def _select_sample_file(feed: Dict[str, Any], state: Stage01State) -> Optional[str]:
    for candidate in _landing_candidates(feed, state):
        existing = _resolve_existing_path(candidate)
        if existing and Path(existing).is_file():
            return existing
    return None


def _source_path(feed: Dict[str, Any], state: Stage01State) -> str:
    for value in (
        feed.get("source_path"),
        feed.get("remote_path"),
        feed.get("cloud_path"),
        feed.get("databricks_source_path"),
        feed.get("file_path"),
        state.get("databricks_source_path"),
        state.get("landing_path"),
    ):
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _infer_scalar_type(values: Sequence[Any], column_name: str = "") -> str:
    lowered_name = str(column_name or "").lower()

    if "version" in lowered_name or re.search(r"(^|_)fw(__|_|$)", lowered_name):
        return "string"

    if lowered_name.endswith("__date") or lowered_name.endswith("_date") or lowered_name.endswith("accountingdate"):
        return "date"

    if lowered_name.endswith("__time") or lowered_name.endswith("_time"):
        return "timestamp"

    if "mode" in lowered_name:
        return "long"

    cleaned = [value for value in values if value not in (None, "")]
    if not cleaned:
        return "string"

    lower_values = {str(value).strip().lower() for value in cleaned}
    if lower_values and lower_values.issubset({"true", "false", "0", "1", "yes", "no"}):
        return "long"

    try:
        for value in cleaned:
            int(str(value).strip())
        return "long"
    except Exception:
        pass

    try:
        for value in cleaned:
            float(str(value).strip())
        return "double"
    except Exception:
        pass

    date_only_patterns = [
        r"^\d{4}-\d{1,2}-\d{1,2}$",
        r"^\d{1,2}/\d{1,2}/\d{4}$",
        r"^\d{1,2}-\d{1,2}-\d{4}$",
    ]
    if all(
        any(re.match(pattern, str(value).strip()) for pattern in date_only_patterns)
        for value in cleaned
    ):
        dates = pd.to_datetime(pd.Series(cleaned), errors="coerce")
        if int(dates.notna().sum()) == len(cleaned):
            return "date"

    timestamps = pd.to_datetime(pd.Series(cleaned), errors="coerce")
    if int(timestamps.notna().sum()) == len(cleaned):
        return "timestamp"

    return "string"


def _normalize_columns(columns: List[Tuple[str, Sequence[Any]]], source_format: str) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for name, values in columns:
        column_name = str(name or "").strip()
        if not column_name:
            continue
        normalized.append(
            {
                "column_name": column_name,
                "data_type": _infer_scalar_type(values, column_name),
                "nullable": any(value in (None, "") for value in values),
                "source_format": source_format,
            }
        )
    return normalized


def _infer_csv_schema(file_path: str) -> List[Dict[str, Any]]:
    frame = pd.read_csv(file_path, nrows=_sample_limit())
    return _normalize_columns(
        [(str(column), frame[column].tolist()) for column in frame.columns],
        "csv",
    )


def _flatten_json_records(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        records = [item for item in payload if isinstance(item, dict)]
        return records[: _sample_limit()]
    if isinstance(payload, dict):
        nested_records = [value for value in payload.values() if isinstance(value, list) and value and isinstance(value[0], dict)]
        if nested_records:
            return list(nested_records[0])[: _sample_limit()]
        return [payload]
    return []


def _infer_json_schema(file_path: str) -> List[Dict[str, Any]]:
    with open(file_path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    records = _flatten_json_records(payload)
    if not records:
        return []
    keys = sorted({str(key) for record in records for key in record.keys()})
    columns = [(key, [record.get(key) for record in records]) for key in keys]
    return _normalize_columns(columns, "json")


def _xml_row_elements(root: ET.Element) -> List[ET.Element]:
    direct_children = list(root)
    if not direct_children:
        return []
    tag_counts: Dict[str, int] = {}
    for child in direct_children:
        tag = child.tag.split("}")[-1]
        tag_counts[tag] = tag_counts.get(tag, 0) + 1
    row_tag = max(tag_counts.items(), key=lambda item: item[1])[0]
    return [child for child in direct_children if child.tag.split("}")[-1] == row_tag]


def _xml_node_to_dict(element: ET.Element) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        f"_{key}": value for key, value in element.attrib.items()
    }
    children = list(element)
    text = (element.text or "").strip()
    if not children:
        payload["value"] = text
    for child in children:
        tag = child.tag.split("}")[-1]
        value = _xml_node_to_dict(child)
        if tag in payload:
            if not isinstance(payload[tag], list):
                payload[tag] = [payload[tag]]
            payload[tag].append(value)
        else:
            payload[tag] = value
    return payload


def _flatten_xml_element(element: ET.Element, prefix: str = "") -> Dict[str, Any]:
    tag = element.tag.split("}")[-1]
    current = f"{prefix}_{tag}" if prefix else tag
    flattened: Dict[str, Any] = {}

    for attr_name, attr_value in element.attrib.items():
        flattened[f"{current}__{attr_name}"] = attr_value

    children = list(element)
    text = (element.text or "").strip()
    if not children:
        flattened[current] = text

    child_tags = [child.tag.split("}")[-1] for child in children]
    repeated = {name for name in child_tags if child_tags.count(name) > 1}

    for child in children:
        child_tag = child.tag.split("}")[-1]
        if child_tag in repeated:
            key = f"{current}_{child_tag}"
            existing_items: List[Any] = []
            if key in flattened:
                try:
                    loaded = json.loads(str(flattened[key]))
                    existing_items = loaded if isinstance(loaded, list) else [loaded]
                except Exception:
                    existing_items = [flattened[key]]
            existing_items.append(_xml_node_to_dict(child))
            flattened[key] = json.dumps(existing_items, sort_keys=True)
            continue
        flattened.update(_flatten_xml_element(child, current))

    return flattened


def _infer_xml_schema(file_path: str) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    tree = ET.parse(file_path)
    root = tree.getroot()
    row_elements = _xml_row_elements(root)
    root_tag = root.tag.split("}")[-1]
    if row_elements and len({child.tag.split("}")[-1] for child in list(root)}) < len(list(root)):
        row_tag = row_elements[0].tag.split("}")[-1]
        sample_rows = row_elements[: _sample_limit()]
    else:
        row_tag = root_tag
        sample_rows = [root]

    flattened_rows = [_flatten_xml_element(row) for row in sample_rows]
    if not flattened_rows:
        return [], row_tag
    field_names = sorted(
        {
            key
            for row in flattened_rows
            for key in row.keys()
        }
    )
    columns = []
    for field in field_names:
        values = [row.get(field) for row in flattened_rows]
        columns.append((field, values))
    return _normalize_columns(columns, "xml"), row_tag


def _schema_fingerprint(schema_columns: List[Dict[str, Any]], row_tag: Optional[str]) -> str:
    payload = {"row_tag": row_tag, "columns": schema_columns}
    return hashlib.md5(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def _table_columns() -> List[str]:
    conn = get_pipeline_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ?
              AND TABLE_NAME = 'file_feed_schema_registry'
            """,
            _pipeline_schema(),
        )
        return [str(row.COLUMN_NAME) for row in cursor.fetchall()]
    finally:
        conn.close()


def _persist_schema_registry_entry(entry: Dict[str, Any], *, log_context: Dict[str, Any]) -> int:
    table_columns = set(_table_columns())
    conn = get_pipeline_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT TOP 1 schema_fingerprint, version
            FROM [{_pipeline_schema()}].[file_feed_schema_registry]
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
        insert_values = {
            "feed_id": entry["feed_id"],
            "vendor": entry["vendor"],
            "entity": entry["entity"],
            "format": entry["format"],
            "source_type": entry["source_type"],
            "schema_json": json.dumps(entry["schema_json"]),
            "schema_fingerprint": entry["schema_fingerprint"],
            "version": version,
            "row_tag": entry.get("row_tag"),
            "sample_file_path": entry.get("sample_file_path"),
            "source_path": entry.get("source_path"),
            "schema_status": entry.get("schema_status") or "PENDING_REVIEW",
            "discovered_at": entry["discovered_at"],
        }
        ordered_columns = [column for column in insert_values if column in table_columns]
        placeholders = ", ".join("?" for _ in ordered_columns)
        cursor.execute(
            f"""
            INSERT INTO [{_pipeline_schema()}].[file_feed_schema_registry]
            ({', '.join(f'[{column}]' for column in ordered_columns)})
            VALUES ({placeholders})
            """,
            *[insert_values[column] for column in ordered_columns],
        )
        conn.commit()
        return version
    except Exception as exc:
        logger.warning("File schema registry persistence skipped: %s", exc, extra=log_context)
        return int(entry.get("version") or 1)
    finally:
        conn.close()


def _placeholder_entry(feed: Dict[str, Any], state: Stage01State, discovered_at: str) -> Dict[str, Any]:
    source_type = _source_type(feed.get("source") or state.get("source"))
    return {
        "feed_id": str(feed.get("feed_id") or f"{feed.get('vendor')}_{feed.get('entity')}"),
        "vendor": str(feed.get("vendor") or state.get("vendor") or "Vendor1"),
        "entity": str(feed.get("entity") or "unknown"),
        "format": str(feed.get("format") or "unknown").lower(),
        "source_type": source_type,
        "schema_json": [],
        "schema_fingerprint": _schema_fingerprint([], None),
        "version": 1,
        "row_tag": str(feed.get("row_tag") or "").strip() or None,
        "sample_file_path": None,
        "source_path": _source_path(feed, state),
        "schema_status": "PENDING_REVIEW",
        "discovered_at": discovered_at,
        "sample_mode": "cloud_placeholder",
        "review_message": "Cloud source schema could not be inferred locally. Review and confirm schema before approval.",
    }


def _discover_schema_for_feed(feed: Dict[str, Any], state: Stage01State, discovered_at: str) -> Dict[str, Any]:
    source_type = _source_type(feed.get("source") or state.get("source"))
    file_format = str(feed.get("format") or "unknown").lower()
    sample_file_path = _select_sample_file(feed, state)

    if not sample_file_path:
        if source_type == "adls_gen2":
            return _placeholder_entry(feed, state, discovered_at)
        raise ValueError(f"Missing local sample file for feed {feed.get('feed_id')}")

    row_tag: Optional[str] = None
    if file_format == "csv":
        schema_columns = _infer_csv_schema(sample_file_path)
    elif file_format == "json":
        schema_columns = _infer_json_schema(sample_file_path)
    elif file_format == "xml":
        schema_columns, row_tag = _infer_xml_schema(sample_file_path)
    else:
        raise ValueError(f"Unsupported file format for schema extraction: {file_format}")

    return {
        "feed_id": str(feed.get("feed_id") or f"{feed.get('vendor')}_{feed.get('entity')}"),
        "vendor": str(feed.get("vendor") or state.get("vendor") or "Vendor1"),
        "entity": str(feed.get("entity") or "unknown"),
        "format": file_format,
        "source_type": source_type,
        "schema_json": schema_columns,
        "schema_fingerprint": _schema_fingerprint(schema_columns, row_tag),
        "version": 1,
        "row_tag": row_tag or str(feed.get("row_tag") or "").strip() or None,
        "sample_file_path": sample_file_path,
        "local_file_path": sample_file_path,
        "file_path": sample_file_path,
        "source_path": _source_path(feed, state),
        "schema_status": "PENDING_REVIEW",
        "discovered_at": discovered_at,
        "sample_mode": "local_sample",
        "review_message": "Schema inferred from local sample file and awaits Gate 3 approval.",
    }


def file_metadata_discovery_node(state: Stage01State) -> Stage01State:
    new_state = _copy_state(state)
    log_context = {
        "run_id": new_state.get("run_id", "unknown"),
        "node": "file_metadata_discovery",
        "stage": "file_metadata_discovery",
    }
    logger.info("File metadata discovery starting", extra={**log_context, "event_type": "stage_start"})

    if str(new_state.get("status") or "").upper() == "FAILED":
        return new_state

    feeds = _approved_registry_feeds(new_state)
    if not feeds:
        new_state.update(
            {
                "metadata_discovery_status": "SKIPPED",
                "metadata_status": "SKIPPED",
                "metadata_error": "No approved file-source feeds found in file_feed_registry",
                "schema_registry_results": [],
                "schema_review_artifact": {
                    "run_id": new_state.get("run_id"),
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "feeds": [],
                    "message": "No approved file-source feeds found in file_feed_registry",
                },
            }
        )
        return new_state

    fingerprint = str(new_state.get("fingerprint") or new_state.get("run_id") or "unknown")
    discovered_at = datetime.now(timezone.utc).isoformat()
    schema_registry_results: List[Dict[str, Any]] = []

    try:
        for feed in feeds:
            entry = _discover_schema_for_feed(feed, new_state, discovered_at)
            entry["version"] = _persist_schema_registry_entry(entry, log_context=log_context)
            schema_registry_results.append(entry)

        artifact = {
            "run_id": new_state.get("run_id"),
            "generated_at": discovered_at,
            "feed_count": len(schema_registry_results),
            "feeds": schema_registry_results,
        }
        payload = {
            "fingerprint": fingerprint,
            "run_id": new_state.get("run_id"),
            "feed_count": len(schema_registry_results),
            "discovered_at": discovered_at,
            "schema_registry": schema_registry_results,
        }
        ai_store_db_writer(
            run_id=str(new_state.get("run_id") or "unknown"),
            stage="File Metadata Discovery",
            artifact_type="FILE_SCHEMA_SNAPSHOT",
            payload=payload,
            schema_version="FILE_SCHEMA_SNAPSHOT_v2",
            prompt_version="DETERMINISTIC_FILE_SCHEMA_v2",
            faithfulness_status="NOT_APPLICABLE",
            token_count=0,
            input_tokens=0,
            output_tokens=0,
            fingerprint=fingerprint,
        )

        new_state.update(
            {
                "discovered_metadata": payload,
                "metadata_discovery_status": "COMPLETED",
                "metadata_status": "COMPLETED",
                "metadata_error": None,
                "schema_registry_results": schema_registry_results,
                "schema_review_artifact": artifact,
                "status": "IN_PROGRESS",
            }
        )
        logger.info("File metadata discovery completed: feeds=%d", len(schema_registry_results), extra={**log_context, "event_type": "stage_end"})
        return new_state
    except Exception as exc:
        new_state.update(
            {
                "metadata_discovery_status": "FAILED",
                "metadata_status": "FAILED",
                "metadata_error": str(exc),
                "status": "FAILED",
                "error": f"File metadata discovery failed: {exc}",
            }
        )
        logger.error("File metadata discovery failed: %s", exc, extra=log_context)
        return new_state


def sftp_metadata_discovery_node(state: Stage01State) -> Stage01State:
    return file_metadata_discovery_node(state)


def adls_metadata_discovery_node(state: Stage01State) -> Stage01State:
    return file_metadata_discovery_node(state)
