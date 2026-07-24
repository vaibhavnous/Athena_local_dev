from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List

from state import Stage01State
from utilis.ai_store_writer import ai_store_db_writer


CONTRACT_VERSION = "sftp-canonical-v1"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical_hash(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _write_artifact(state: Stage01State, *, stage: str, artifact_type: str, payload: Dict[str, Any]) -> None:
    ai_store_db_writer(
        run_id=str(state.get("run_id") or ""),
        stage=stage,
        artifact_type=artifact_type,
        payload={
            **payload,
            "fingerprint": state.get("fingerprint") or state.get("run_id"),
        },
        schema_version="1.0",
        prompt_version="deterministic-v1",
        faithfulness_status="PASSED",
    )


def _approved_feeds(state: Stage01State) -> List[Dict[str, Any]]:
    reviewed = (state.get("bronze_review_artifact") or {}).get("feeds") or []
    if reviewed:
        return [dict(feed) for feed in reviewed if isinstance(feed, dict)]

    candidates = state.get("candidate_feeds") or []
    return [
        dict(feed)
        for feed in candidates
        if isinstance(feed, dict)
        and str(feed.get("status") or "APPROVED").upper() in {"APPROVED", "CERTIFIED", "ENABLED"}
    ]


def _approved_enrichment(state: Stage01State) -> Dict[str, Any]:
    artifact = state.get("enrichment_review_artifact") or state.get("enriched_metadata") or {}
    if isinstance(artifact, dict) and isinstance(artifact.get("enrichment_artifact"), dict):
        return dict(artifact["enrichment_artifact"])
    return dict(artifact) if isinstance(artifact, dict) else {}


def _approved_kpis(state: Stage01State) -> List[Dict[str, Any]]:
    return [
        dict(kpi)
        for kpi in (state.get("certified_kpis") or state.get("kpis") or [])
        if isinstance(kpi, dict)
    ]


def _schema_contract(feeds: Iterable[Dict[str, Any]], state: Stage01State) -> List[Dict[str, Any]]:
    registry_by_feed = {
        str(item.get("feed_id") or ""): item
        for item in (state.get("schema_registry_results") or [])
        if isinstance(item, dict)
    }
    contracts = []
    for feed in feeds:
        feed_id = str(feed.get("feed_id") or "")
        registry = registry_by_feed.get(feed_id) or {}
        columns = (
            feed.get("approved_schema")
            or feed.get("schema")
            or registry.get("schema")
            or registry.get("schema_json")
            or []
        )
        contracts.append(
            {
                "feed_id": feed_id,
                "entity": feed.get("entity"),
                "format": feed.get("file_format") or feed.get("format"),
                "columns": columns,
                "schema_fingerprint": feed.get("schema_fingerprint") or registry.get("schema_fingerprint") or _canonical_hash(columns),
            }
        )
    return contracts


def sftp_metadata_bootstrap_node(state: Stage01State) -> Stage01State:
    if str(state.get("status") or "").upper() == "FAILED":
        return state

    feeds = _approved_feeds(state)
    schemas = _schema_contract(feeds, state)
    enrichment = _approved_enrichment(state)
    kpis = _approved_kpis(state)
    missing = []
    if not feeds:
        missing.append("approved feeds")
    if not schemas or any(not schema.get("columns") for schema in schemas):
        missing.append("approved schemas")
    if not enrichment:
        missing.append("approved semantic enrichment")
    if not kpis:
        missing.append("approved KPIs")
    if missing:
        return {
            **state,
            "status": "FAILED",
            "metadata_bootstrap_status": "FAILED",
            "error": f"Metadata bootstrap blocked: missing {', '.join(missing)}.",
        }

    manifest = {
        "run_id": state.get("run_id"),
        "connection_lock": {
            "connection_id": state.get("connection_id"),
            "source": state.get("source"),
            "target_warehouse": state.get("target_warehouse"),
        },
        "feeds": feeds,
        "schemas": schemas,
        "enrichment": enrichment,
        "kpis": kpis,
        "created_at": _utc_now(),
        "contract_version": CONTRACT_VERSION,
    }
    _write_artifact(
        state,
        stage="SFTP Metadata Bootstrap",
        artifact_type="SFTP_METADATA_BOOTSTRAP",
        payload=manifest,
    )
    return {
        **state,
        "metadata_bootstrap": manifest,
        "metadata_bootstrap_status": "COMPLETED",
    }


def _plan_components(state: Stage01State) -> Dict[str, Any]:
    bootstrap = state.get("metadata_bootstrap") or {}
    enrichment = bootstrap.get("enrichment") or _approved_enrichment(state)
    schemas = bootstrap.get("schemas") or _schema_contract(_approved_feeds(state), state)
    kpis = bootstrap.get("kpis") or _approved_kpis(state)
    merge_keys = {
        str(schema.get("feed_id") or ""): [
            str(column.get("name") or column.get("column_name"))
            for column in (schema.get("columns") or [])
            if isinstance(column, dict)
            and (
                column.get("is_primary_key")
                or str(column.get("semantic_type") or column.get("role") or "").upper() in {"PRIMARY_KEY", "IDENTIFIER"}
            )
        ]
        for schema in schemas
    }
    dq_rules = state.get("dq_rules") or enrichment.get("dq_rules") or {}
    connection = bootstrap.get("connection_lock") or {
        "connection_id": state.get("connection_id"),
        "source": state.get("source"),
        "target_warehouse": state.get("target_warehouse"),
    }
    return {
        "schema": schemas,
        "enrichment": enrichment,
        "kpis": kpis,
        "merge_keys": merge_keys,
        "dq": dq_rules,
        "connection": connection,
        "utilities": {"contract_version": CONTRACT_VERSION},
    }


def sftp_plan_seal_node(state: Stage01State) -> Stage01State:
    if str(state.get("status") or "").upper() == "FAILED":
        return state
    if state.get("metadata_bootstrap_status") != "COMPLETED":
        return {
            **state,
            "status": "FAILED",
            "plan_seal_status": "FAILED",
            "error": "Plan seal blocked: metadata bootstrap is not complete.",
        }

    components = _plan_components(state)
    hashes = {name: _canonical_hash(value) for name, value in components.items()}
    seal = {
        "run_id": state.get("run_id"),
        "hashes": hashes,
        "sealed_plan_hash": _canonical_hash(hashes),
        "sealed_at": _utc_now(),
        "contract_version": CONTRACT_VERSION,
    }
    _write_artifact(state, stage="SFTP Plan Seal", artifact_type="SFTP_SEALED_PLAN", payload=seal)
    return {
        **state,
        "sealed_plan": seal,
        "plan_seal_status": "COMPLETED",
    }


def sftp_freshness_check_node(state: Stage01State) -> Stage01State:
    if str(state.get("status") or "").upper() == "FAILED":
        return state

    seal = state.get("sealed_plan") or {}
    expected = seal.get("hashes") or {}
    if not expected:
        return {
            **state,
            "status": "FAILED",
            "freshness_check_status": "FAILED",
            "error": "Freshness check blocked: no sealed plan is available.",
        }

    current = {name: _canonical_hash(value) for name, value in _plan_components(state).items()}
    stale = sorted(name for name, value in expected.items() if current.get(name) != value)
    manifest = {
        "run_id": state.get("run_id"),
        "sealed_plan_hash": seal.get("sealed_plan_hash"),
        "expected_hashes": expected,
        "current_hashes": current,
        "stale_components": stale,
        "checked_at": _utc_now(),
    }
    if stale:
        return {
            **state,
            "status": "FAILED",
            "freshness_manifest": manifest,
            "freshness_check_status": "FAILED",
            "error": f"Freshness check failed for: {', '.join(stale)}.",
        }

    _write_artifact(
        state,
        stage="SFTP Freshness Check",
        artifact_type="SFTP_FRESHNESS_MANIFEST",
        payload=manifest,
    )
    return {
        **state,
        "freshness_manifest": manifest,
        "freshness_check_status": "COMPLETED",
    }


def _column_name(column: Dict[str, Any]) -> str:
    return str(column.get("name") or column.get("column_name") or "").strip()


def sftp_metadata_codegen_node(state: Stage01State) -> Stage01State:
    if str(state.get("status") or "").upper() == "FAILED":
        return state
    if state.get("freshness_check_status") != "COMPLETED":
        return {
            **state,
            "status": "FAILED",
            "metadata_codegen_status": "FAILED",
            "error": "Metadata code generation blocked: freshness check is not complete.",
        }

    bootstrap = state.get("metadata_bootstrap") or {}
    enrichment = bootstrap.get("enrichment") or {}
    enriched_columns = enrichment.get("columns") or enrichment.get("enriched_columns") or []
    enrichment_by_column = {
        (
            str(item.get("feed_id") or item.get("entity") or ""),
            _column_name(item),
        ): item
        for item in enriched_columns
        if isinstance(item, dict) and _column_name(item)
    }

    source_mappings = []
    target_rules = []
    for schema in bootstrap.get("schemas") or []:
        feed_id = str(schema.get("feed_id") or "")
        columns = [column for column in (schema.get("columns") or []) if isinstance(column, dict)]
        mappings = []
        merge_keys = []
        for column in columns:
            source_name = _column_name(column)
            enriched = (
                enrichment_by_column.get((feed_id, source_name))
                or enrichment_by_column.get((str(schema.get("entity") or ""), source_name))
                or {}
            )
            role = str(enriched.get("semantic_type") or enriched.get("role") or column.get("semantic_type") or "ATTRIBUTE").upper()
            if column.get("is_primary_key") or role in {"PRIMARY_KEY", "IDENTIFIER"}:
                merge_keys.append(source_name)
            mappings.append(
                {
                    "source_column": source_name,
                    "target_column": str(enriched.get("target_column") or source_name).lower(),
                    "data_type": enriched.get("data_type") or column.get("data_type") or column.get("type"),
                    "semantic_role": role,
                    "pii": bool(enriched.get("pii") or enriched.get("is_pii")),
                    "transform": enriched.get("transform") or "identity",
                }
            )
        if not mappings or not merge_keys:
            return {
                **state,
                "status": "FAILED",
                "metadata_codegen_status": "FAILED",
                "error": f"Metadata code generation blocked for {feed_id}: mapping coverage or merge keys are missing.",
            }
        source_mappings.append({"feed_id": feed_id, "columns": mappings})
        target_rules.append(
            {
                "feed_id": feed_id,
                "target_table": f"{str(schema.get('entity') or feed_id).lower()}_silver",
                "merge_keys": sorted(set(merge_keys)),
                "dq_rules": state.get("dq_rules") or {},
            }
        )

    gold_models = [
        {
            "model_name": str(kpi.get("kpi_name") or kpi.get("name") or f"kpi_{index + 1}").lower().replace(" ", "_"),
            "definition": kpi.get("definition"),
            "measure": kpi.get("measure") or kpi.get("measures"),
            "dimensions": kpi.get("dimensions") or [],
            "date_column": kpi.get("date_column"),
            "aggregation": kpi.get("aggregation"),
            "grain": kpi.get("grain"),
        }
        for index, kpi in enumerate(bootstrap.get("kpis") or [])
    ]
    if not gold_models:
        return {
            **state,
            "status": "FAILED",
            "metadata_codegen_status": "FAILED",
            "error": "Metadata code generation blocked: no approved KPI can produce a Gold model.",
        }

    artifact = {
        "run_id": state.get("run_id"),
        "source_mapping": source_mappings,
        "target_table_rule": target_rules,
        "gold_model_config": gold_models,
        "sealed_plan_hash": (state.get("sealed_plan") or {}).get("sealed_plan_hash"),
        "generated_at": _utc_now(),
    }
    _write_artifact(
        state,
        stage="SFTP Metadata Code Generation",
        artifact_type="SFTP_METADATA_CODEGEN",
        payload=artifact,
    )
    return {
        **state,
        "metadata_codegen_artifact": artifact,
        "metadata_codegen_status": "COMPLETED",
    }
