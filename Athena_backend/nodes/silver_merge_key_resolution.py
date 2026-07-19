"""Resolve Silver merge-key candidates before the combined human review gate."""

from __future__ import annotations

from typing import Any, Dict, List

from state import Stage01State


def _table_name(value: Any) -> str:
    name = str(value or "").split(".")[-1].strip('"').casefold()
    for prefix in ("bronze_", "silver_"):
        if name.startswith(prefix):
            return name[len(prefix):]
    return name


def _enriched_columns(state: Stage01State) -> List[Dict[str, Any]]:
    candidates = [state.get("enriched_columns")]
    for field in ("enriched_metadata", "enrichment_review_artifact"):
        payload = state.get(field)
        if not isinstance(payload, dict):
            continue
        candidates.append(payload.get("columns"))
        nested = payload.get("enrichment_artifact")
        if isinstance(nested, dict):
            candidates.append(nested.get("columns"))
    for columns in candidates:
        if isinstance(columns, list) and columns:
            return [column for column in columns if isinstance(column, dict)]
    return []


def _dedupe(values: List[Any]) -> List[str]:
    seen = set()
    result = []
    for value in values:
        key = str(value or "").strip()
        folded = key.casefold()
        if not key or folded in seen:
            continue
        seen.add(folded)
        result.append(key)
    return result


def silver_merge_key_resolution_node(state: Stage01State) -> Stage01State:
    """Build the backend artifact consumed by the single merge-key review gate."""
    bronze_artifact = state.get("bronze_review_artifact") or state.get("gate4_reviewed_merge_keys") or {}
    columns_by_table: Dict[str, List[Dict[str, Any]]] = {}
    for column in _enriched_columns(state):
        columns_by_table.setdefault(_table_name(column.get("table_name") or column.get("table")), []).append(column)

    feeds = []
    for feed in (bronze_artifact.get("feeds") if isinstance(bronze_artifact, dict) else []) or []:
        if not isinstance(feed, dict):
            continue
        table = _table_name(
            feed.get("table") or feed.get("table_name") or feed.get("entity") or feed.get("target_table")
        )
        table_columns = columns_by_table.get(table) or []
        primary_candidates = _dedupe([
            column.get("column_name") or column.get("name")
            for column in table_columns
            if column.get("is_primary_key")
        ])
        join_candidates = _dedupe([
            column.get("column_name") or column.get("name")
            for column in table_columns
            if column.get("is_join_key")
        ])
        existing_keys = _dedupe(list(feed.get("merge_keys") or feed.get("primary_keys") or []))
        keys = existing_keys or primary_candidates
        candidates = _dedupe(primary_candidates + join_candidates)
        source = (
            "bronze_review"
            if existing_keys
            else "semantic_enrichment_primary_key"
            if primary_candidates
            else "semantic_enrichment_candidates"
            if candidates
            else "unresolved"
        )
        feeds.append(
            {
                **feed,
                "merge_keys": keys,
                "primary_keys": keys,
                "merge_key_candidates": candidates,
                "merge_key_source": source,
                "merge_key_resolution_status": "RESOLVED" if keys else "REVIEW_REQUIRED",
                "review_status": "PENDING",
                "review_type": "silver_merge_key",
            }
        )
    resolved_count = sum(1 for feed in feeds if feed.get("merge_keys"))
    artifact: Dict[str, Any] = {
        "run_id": state.get("run_id"),
        "feeds": feeds,
        "resolved_count": resolved_count,
        "review_required_count": len(feeds) - resolved_count,
    }
    return {
        **state,
        "silver_merge_key_resolution_status": "COMPLETED",
        "silver_merge_key_resolution_artifact": artifact,
    }
