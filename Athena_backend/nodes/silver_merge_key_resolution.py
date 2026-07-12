"""Resolve Silver merge-key candidates before the combined human review gate."""

from __future__ import annotations

from typing import Any, Dict

from state import Stage01State


def silver_merge_key_resolution_node(state: Stage01State) -> Stage01State:
    """Build the backend artifact consumed by the single merge-key review gate."""
    bronze_artifact = state.get("bronze_review_artifact") or state.get("gate4_reviewed_merge_keys") or {}
    feeds = []
    for feed in (bronze_artifact.get("feeds") if isinstance(bronze_artifact, dict) else []) or []:
        if not isinstance(feed, dict):
            continue
        keys = feed.get("merge_keys") or feed.get("primary_keys") or []
        feeds.append(
            {
                **feed,
                "merge_keys": list(keys),
                "primary_keys": list(keys),
                "review_status": "PENDING",
                "review_type": "silver_merge_key",
            }
        )
    artifact: Dict[str, Any] = {"run_id": state.get("run_id"), "feeds": feeds}
    return {
        **state,
        "silver_merge_key_resolution_status": "COMPLETED",
        "silver_merge_key_resolution_artifact": artifact,
    }
