from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from services.adls_source import profile_merge_key, profile_merge_key_candidates
from services.metadata_contracts import normalize_bronze_column_name
from state import Stage01State
from utilis.ai_store_writer import ai_store_db_writer
from utilis.logger import logger


_SOURCE_CONTRACT_KEYS = {"policy_transactions": ["reference_id"]}


class MergeKeyProposal(BaseModel):
    table: str
    merge_keys: List[str] = Field(min_length=1, max_length=8)
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: str = Field(min_length=1, max_length=1000)


class MergeKeyProposalBundle(BaseModel):
    feeds: List[MergeKeyProposal]


def _table_name(value: Any) -> str:
    name = str(value or "").split(".")[-1].strip('"').casefold()
    for prefix in ("bronze_", "silver_"):
        if name.startswith(prefix):
            return name[len(prefix) :]
    return name


def _feed_table(feed: Dict[str, Any]) -> str:
    return _table_name(
        feed.get("table")
        or feed.get("table_name")
        or feed.get("entity")
        or feed.get("feed_id")
        or feed.get("target_table")
    )


def _canonical_source_columns(source: Dict[str, Any]) -> Dict[str, str]:
    return {
        normalize_bronze_column_name(column.get("column_name") or column.get("source_field_path")): str(
            column.get("column_name") or column.get("source_field_path") or ""
        )
        for column in source.get("columns") or []
        if isinstance(column, dict)
        and str(column.get("column_name") or column.get("source_field_path") or "").strip()
    }


def _profile_canonical_key(source: Dict[str, Any], keys: List[str]) -> Dict[str, Any]:
    available = _canonical_source_columns(source)
    evidence = profile_merge_key(source, [available[key] for key in keys])
    return {**evidence, "columns": keys}


def apply_adls_source_contract_merge_keys(
    state: Stage01State, artifact: Dict[str, Any]
) -> Dict[str, Any]:
    """Keep ADLS contract keys authoritative in persisted and UI review artifacts."""
    certified_by_table = {
        _table_name(table.get("table_name") or table.get("entity")): dict(table)
        for table in state.get("certified_tables") or []
        if isinstance(table, dict)
    }
    feeds = []
    for raw_feed in artifact.get("feeds") or []:
        if not isinstance(raw_feed, dict):
            continue
        feed = dict(raw_feed)
        table = _feed_table(feed)
        contract_keys = _SOURCE_CONTRACT_KEYS.get(table)
        source = certified_by_table.get(table)
        if contract_keys and source:
            available = _canonical_source_columns(source)
            if all(key in available for key in contract_keys):
                keys = list(contract_keys)
                feed.update(
                    merge_keys=keys,
                    primary_keys=keys,
                    merge_key_source="adls_source_contract_default",
                    merge_key_resolution_status="RESOLVED",
                    merge_key_reasoning="ADLS policy transaction source contract uses reference_id.",
                )
        feeds.append(feed)
    return {**artifact, "feeds": feeds}


def _strip_fences(value: str) -> str:
    match = re.search(r"```(?:json)?\s*(.*?)```", str(value or ""), re.IGNORECASE | re.DOTALL)
    return (match.group(1) if match else str(value or "")).strip()


def _positive_float(name: str, default: float) -> float:
    try:
        return min(1.0, max(0.0, float(os.getenv(name, str(default)))))
    except ValueError:
        return default


def _proposal_context(
    state: Stage01State,
    feeds: List[Dict[str, Any]],
    candidate_sets_by_table: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, Any]:
    enriched = state.get("enrichment_review_artifact") or state.get("enriched_metadata") or {}
    if isinstance(enriched, dict) and isinstance(enriched.get("enrichment_artifact"), dict):
        enriched = enriched.get("enrichment_artifact") or {}
    semantic_by_column = {
        (
            _table_name(column.get("table_name") or column.get("table") or column.get("entity")),
            str(column.get("column_name") or "").casefold(),
        ): column
        for column in (enriched.get("columns") if isinstance(enriched, dict) else []) or []
        if isinstance(column, dict)
    }
    profiles_by_table: Dict[str, List[Dict[str, Any]]] = {}
    for profile in state.get("column_profiles") or []:
        if not isinstance(profile, dict):
            continue
        table = _table_name(
            profile.get("table_name")
            or profile.get("table")
            or profile.get("entity")
            or profile.get("feed_id")
        )
        if table:
            semantic = semantic_by_column.get(
                (table, str(profile.get("column_name") or "").casefold())
            ) or {}
            profiles_by_table.setdefault(table, []).append(
                {
                    "column_name": profile.get("column_name"),
                    "data_type": profile.get("data_type"),
                    "semantic_type": semantic.get("semantic_type") or profile.get("semantic_type"),
                    "sample_count": profile.get("sample_count"),
                    "null_count": profile.get("null_count"),
                    "distinct_count": profile.get("distinct_count"),
                    "null_percentage": profile.get("null_percentage"),
                    "is_join_key": semantic.get("is_join_key", profile.get("is_join_key")),
                    "is_primary_key": semantic.get("is_primary_key", profile.get("is_primary_key")),
                }
            )
    return {
        "feeds": [
            {
                "table": _feed_table(feed),
                "suggested_candidates": feed.get("merge_key_candidates") or [],
                "columns": profiles_by_table.get(_feed_table(feed)) or [],
                "validated_candidate_keys": candidate_sets_by_table.get(_feed_table(feed)) or [],
            }
            for feed in feeds
        ]
    }


def _validate_proposals(
    proposals: MergeKeyProposalBundle,
    unresolved: List[Dict[str, Any]],
    certified_by_table: Dict[str, Dict[str, Any]],
    candidate_sets_by_table: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, Dict[str, Any]]:
    expected = {_feed_table(feed) for feed in unresolved}
    proposed = {_table_name(item.table): item for item in proposals.feeds}
    if set(proposed) != expected or len(proposals.feeds) != len(expected):
        raise ValueError(
            f"LLM merge-key coverage mismatch: missing={sorted(expected - set(proposed))} "
            f"unexpected={sorted(set(proposed) - expected)}"
        )
    validated: Dict[str, Dict[str, Any]] = {}
    for table, proposal in proposed.items():
        source = certified_by_table.get(table)
        if not source:
            raise ValueError(f"LLM merge-key proposal has no approved ADLS feed: {table}")
        available = {
            str(column.get("column_name") or "").casefold(): str(column.get("column_name") or "")
            for column in source.get("columns") or []
            if isinstance(column, dict) and str(column.get("column_name") or "").strip()
        }
        missing = [key for key in proposal.merge_keys if key.casefold() not in available]
        if missing:
            raise ValueError(f"LLM proposed unknown columns for {table}: {', '.join(missing)}")
        canonical_keys = [available[key.casefold()] for key in proposal.merge_keys]
        evidence = next(
            (
                candidate
                for candidate in candidate_sets_by_table.get(table) or []
                if [str(key).casefold() for key in candidate.get("columns") or []]
                == [key.casefold() for key in canonical_keys]
            ),
            None,
        )
        if evidence is None:
            raise ValueError(
                f"Proposed merge key for {table} is not one of the data-validated candidate sets."
            )
        validated[table] = {
            "merge_keys": canonical_keys,
            "confidence": proposal.confidence,
            "reasoning": proposal.reasoning,
            "profile_evidence": evidence,
        }
    return validated


def _propose_with_llm(
    state: Stage01State,
    unresolved: List[Dict[str, Any]],
    certified_by_table: Dict[str, Dict[str, Any]],
    candidate_sets_by_table: Dict[str, List[Dict[str, Any]]],
    *,
    llm: Any = None,
) -> Dict[str, Dict[str, Any]]:
    if llm is None:
        from nodes.req_extraction import get_llm

        llm = get_llm(
            provider=os.getenv("ATHENA_ADLS_MERGE_KEY_LLM_PROVIDER", os.getenv("ATHENA_LLM_PROVIDER", "azure_openai")),
            model=os.getenv("ATHENA_ADLS_MERGE_KEY_LLM_MODEL"),
            temperature=0.0,
            request_timeout=float(os.getenv("ATHENA_ADLS_MERGE_KEY_LLM_TIMEOUT_SECONDS", "45")),
            max_retries=0,
        )
    context = _proposal_context(state, unresolved, candidate_sets_by_table)
    prompt = (
        "You are selecting stable Silver MERGE keys for approved file feeds. "
        "Return JSON only with shape {\"feeds\":[{\"table\":str,\"merge_keys\":[str],"
        "\"confidence\":0..1,\"reasoning\":str}]}. Return exactly one item per input table. "
        "Choose merge_keys exactly from one validated_candidate_keys.columns list for that table; "
        "those sets already passed source-sample completeness and uniqueness checks. Prefer the smallest "
        "stable business key. A low-cardinality sequence or version column may be required in a composite key. "
        "A join key is only a candidate, not proof of uniqueness. Use a composite key when necessary. "
        "Do not use audit timestamps, free text, measures, or volatile dates unless required as part of a "
        "documented versioned business identity. Every feed must receive a non-empty proposal.\n\n"
        + json.dumps(context, ensure_ascii=False)
    )
    max_attempts = max(1, int(os.getenv("ADLS_MERGE_KEY_LLM_ATTEMPTS", "3")))
    last_error = ""
    for attempt in range(1, max_attempts + 1):
        request = prompt
        if last_error:
            request += f"\n\nPREVIOUS PROPOSAL FAILED HARD VALIDATION:\n{last_error[:1200]}\nReturn corrected JSON."
        try:
            response = llm.invoke(request)
            payload = json.loads(_strip_fences(str(getattr(response, "content", response))))
            proposals = MergeKeyProposalBundle.model_validate(payload)
            return _validate_proposals(
                proposals,
                unresolved,
                certified_by_table,
                candidate_sets_by_table,
            )
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            logger.warning(
                "ADLS merge-key LLM attempt %d/%d failed: %s",
                attempt,
                max_attempts,
                last_error[:500],
                extra={"run_id": state.get("run_id"), "node": "adls_silver_merge_key_resolution"},
            )
    raise RuntimeError(f"ADLS merge-key proposal failed after {max_attempts} attempts: {last_error}")


def adls_silver_merge_key_resolution_node(
    state: Stage01State, *, llm: Any = None
) -> Stage01State:
    """Resolve file keys without changing the database-source resolver."""
    from nodes.silver_merge_key_resolution import silver_merge_key_resolution_node

    resolved = silver_merge_key_resolution_node(state)
    artifact = dict(resolved.get("silver_merge_key_resolution_artifact") or {})
    feeds = [dict(feed) for feed in artifact.get("feeds") or [] if isinstance(feed, dict)]
    certified_by_table = {
        _table_name(table.get("table_name") or table.get("entity")): dict(table)
        for table in state.get("certified_tables") or []
        if isinstance(table, dict)
    }
    proposals: Dict[str, Dict[str, Any]] = {}
    resolution_errors: Dict[str, str] = {}
    candidate_sets_by_table: Dict[str, List[Dict[str, Any]]] = {}
    for feed in feeds:
        table = _feed_table(feed)
        contract_keys = _SOURCE_CONTRACT_KEYS.get(table)
        source = certified_by_table.get(table)
        if not contract_keys or not source:
            continue
        available = _canonical_source_columns(source)
        if all(key in available for key in contract_keys):
            canonical_keys = list(contract_keys)
            evidence = _profile_canonical_key(source, canonical_keys)
            candidate_sets_by_table[table] = [{**evidence, "columns": canonical_keys}]
            proposals[table] = {
                "merge_keys": canonical_keys,
                "confidence": 1.0,
                "reasoning": "ADLS policy transaction source contract uses reference_id.",
                "profile_evidence": evidence,
                "source": "adls_source_contract_default",
            }
        else:
            resolution_errors[table] = "The policy_transactions feed does not contain reference_id."

    unresolved = [
        feed for feed in feeds
        if _feed_table(feed) not in proposals
        and not (feed.get("merge_keys") or feed.get("primary_keys"))
    ]
    if not unresolved and not proposals:
        return resolved
    for feed in unresolved:
        table = _feed_table(feed)
        source = certified_by_table.get(table)
        if not source:
            resolution_errors[table] = "Approved ADLS source metadata is unavailable."
            continue
        try:
            candidate_sets = profile_merge_key_candidates(
                source,
                preferred_columns=feed.get("merge_key_candidates") or [],
            )
            candidate_sets_by_table[table] = candidate_sets
            if not candidate_sets:
                resolution_errors[table] = "No complete and unique key candidate was found in the bounded source sample."
                continue
            proposals.update(_propose_with_llm(
                state,
                [feed],
                certified_by_table,
                {table: candidate_sets},
                llm=llm,
            ))
        except Exception as exc:
            resolution_errors[table] = str(exc)
            logger.warning(
                "ADLS merge-key resolution remains pending for %s: %s",
                table,
                str(exc)[:500],
                extra={"run_id": state.get("run_id"), "node": "adls_silver_merge_key_resolution"},
            )
    updated_feeds = []
    for feed in feeds:
        proposal = proposals.get(_feed_table(feed))
        if not proposal:
            table = _feed_table(feed)
            candidate_sets = candidate_sets_by_table.get(table) or []
            candidate_columns = _dedupe_candidate_columns(feed, candidate_sets)
            updated_feeds.append({
                **feed,
                "merge_key_candidates": candidate_columns,
                "merge_key_candidate_sets": candidate_sets,
                "merge_key_resolution_error": resolution_errors.get(table),
            })
            continue
        keys = proposal["merge_keys"]
        table = _feed_table(feed)
        candidate_sets = candidate_sets_by_table.get(table) or []
        updated_feeds.append(
            {
                **feed,
                "merge_keys": keys,
                "primary_keys": keys,
                "merge_key_candidates": _dedupe_candidate_columns(feed, candidate_sets),
                "merge_key_candidate_sets": candidate_sets,
                "merge_key_source": proposal.get("source") or "adls_llm_profile_validated",
                "merge_key_resolution_status": "RESOLVED",
                "merge_key_confidence": proposal["confidence"],
                "merge_key_reasoning": proposal["reasoning"],
                "merge_key_profile_evidence": proposal["profile_evidence"],
            }
        )
    updated_artifact = apply_adls_source_contract_merge_keys(state, {
        **artifact,
        "feeds": updated_feeds,
        "resolved_count": sum(1 for feed in updated_feeds if feed.get("merge_keys")),
        "review_required_count": sum(1 for feed in updated_feeds if not feed.get("merge_keys")),
    })
    ai_store_db_writer(
        run_id=str(state.get("run_id") or ""),
        stage="Silver Merge Key Resolution",
        artifact_type="ADLS_MERGE_KEY_PROPOSALS",
        payload=updated_artifact,
        schema_version="ADLS_MERGE_KEY_PROPOSALS_v2",
        prompt_version="ADLS_LLM_PROFILE_VALIDATED_v2",
        faithfulness_status="WARN" if resolution_errors else "PASSED",
        token_count=0,
        input_tokens=0,
        output_tokens=0,
        fingerprint=str(state.get("fingerprint") or state.get("run_id") or ""),
    )
    return {
        **resolved,
        "silver_merge_key_resolution_artifact": updated_artifact,
        "silver_merge_key_review_artifact": updated_artifact,
    }


def _dedupe_candidate_columns(
    feed: Dict[str, Any], candidate_sets: List[Dict[str, Any]]
) -> List[str]:
    seen = set()
    result = []
    values = list(feed.get("merge_key_candidates") or [])
    values.extend(
        column
        for candidate in candidate_sets
        for column in candidate.get("columns") or []
    )
    for value in values:
        column = str(value or "").strip()
        if column and column.casefold() not in seen:
            seen.add(column.casefold())
            result.append(column)
    return result


def validate_adls_merge_key_review(
    state: Stage01State, review_artifact: Dict[str, Any]
) -> Dict[str, Any]:
    """Reject incomplete or ungrounded ADLS merge-key approvals at the API boundary."""
    expected_artifact = state.get("silver_merge_key_review_artifact") or {}
    expected_feeds = [feed for feed in expected_artifact.get("feeds") or [] if isinstance(feed, dict)]
    review_artifact = apply_adls_source_contract_merge_keys(state, review_artifact)
    submitted_feeds = [feed for feed in review_artifact.get("feeds") or [] if isinstance(feed, dict)]
    expected = {_feed_table(feed) for feed in expected_feeds}
    submitted = {_feed_table(feed): dict(feed) for feed in submitted_feeds}
    if not expected or set(submitted) != expected or len(submitted_feeds) != len(expected):
        raise ValueError("ADLS Silver merge-key review must submit every expected feed exactly once.")
    certified_by_table = {
        _table_name(table.get("table_name") or table.get("entity")): dict(table)
        for table in state.get("certified_tables") or []
        if isinstance(table, dict)
    }
    validated_feeds = []
    for table in sorted(expected):
        feed = submitted[table]
        keys = [str(key).strip() for key in (feed.get("merge_keys") or feed.get("primary_keys") or []) if str(key).strip()]
        if not keys:
            raise ValueError(f"Approved Silver merge keys are missing for {table}.")
        source = certified_by_table.get(table)
        if not source:
            raise ValueError(f"Approved ADLS source metadata is missing for {table}.")
        available = _canonical_source_columns(source)
        submitted_keys = [normalize_bronze_column_name(key) for key in keys]
        missing = [key for key in submitted_keys if key not in available]
        if missing:
            raise ValueError(f"Approved Silver merge keys do not exist for {table}: {', '.join(missing)}")
        canonical_keys = submitted_keys
        evidence = _profile_canonical_key(source, canonical_keys)
        if evidence["completeness_ratio"] < _positive_float("ADLS_MERGE_KEY_MIN_SAMPLE_COMPLETENESS", 1.0):
            raise ValueError(f"Approved Silver merge keys contain nulls in the source sample for {table}.")
        if evidence["uniqueness_ratio"] < _positive_float("ADLS_MERGE_KEY_MIN_SAMPLE_UNIQUENESS", 0.98):
            raise ValueError(f"Approved Silver merge keys are not unique in the source sample for {table}.")
        validated_feeds.append(
            {
                **feed,
                "merge_keys": canonical_keys,
                "primary_keys": canonical_keys,
                "merge_key_profile_evidence": evidence,
                "review_status": "APPROVED",
            }
        )
    return {**review_artifact, "feeds": validated_feeds}
