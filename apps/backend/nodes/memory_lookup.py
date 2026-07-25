from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional, Tuple

from nodes.ingestion import _chunk_and_embed, finalize_ingestion_after_memory
from state import Stage01State
from utilis.db import artifact_storage_fingerprint, config, get_pipeline_connection
from utilis.env import load_backend_env
from utilis.embeddings import get_embedding_model
from utilis.logger import logger

load_backend_env()
db_conf = config.get("azure_sql", {})
db_schema = db_conf.get("schema_name", "dbo")
pinecone_conf = config.get("pinecone", {})


def _copy_state(state: Stage01State) -> Stage01State:
    return state.copy()


def _log_context(state: Stage01State) -> dict:
    return {
        "run_id": state.get("run_id", "unknown"),
        "node": "memory_lookup",
        "fingerprint": state.get("fingerprint"),
    }


def _fetch_latest_payload(fingerprint: str, artifact_types: Tuple[str, ...]) -> Optional[Dict[str, Any]]:
    conn = get_pipeline_connection()
    try:
        cursor = conn.cursor()
        pair_conditions = " OR ".join("(fingerprint = ? AND artifact_type = ?)" for _ in artifact_types)
        legacy_placeholders = ", ".join("?" for _ in artifact_types)
        params: List[Any] = []
        for artifact_type in artifact_types:
            params.extend([artifact_storage_fingerprint(fingerprint, artifact_type), artifact_type])
        params.extend([fingerprint, *artifact_types])

        cursor.execute(
            f"""
            SELECT TOP 1 payload
            FROM [{db_schema}].[ai_store]
            WHERE ({pair_conditions})
               OR (fingerprint = ? AND artifact_type IN ({legacy_placeholders}))
            ORDER BY stored_at DESC
            """,
            tuple(params),
        )
        row = cursor.fetchone()
        if row:
            return json.loads(row[0])
    finally:
        conn.close()
    return None


def _fetch_exact_match(fingerprint: str, state: Stage01State) -> Tuple[bool, Dict[str, Any], Dict[str, Any]]:
    if state.get("skip_db", False):
        logger.info("DB skipped (test mode)")
        return False, {}, {}

    requirements_payload = _fetch_latest_payload(
        fingerprint,
        ("REQUIREMENTS", "REQUIREMENTS_WARN"),
    ) or {}
    kpis_payload = _fetch_latest_payload(
        fingerprint,
        ("KPIS",),
    ) or {}

    has_requirements = bool(requirements_payload.get("business_objective"))
    has_kpis = bool(kpis_payload.get("kpis"))
    return has_requirements or has_kpis, requirements_payload, kpis_payload


def _fetch_context_kpis(embeddings: List[float], top_k: int = 3) -> List[Dict[str, Any]]:
    api_key = pinecone_conf.get("api_key") or os.getenv("PINECONE_API_KEY")
    index_name = pinecone_conf.get("index_name") or os.getenv("PINECONE_INDEX_NAME") or "ai-store-index"
    if not api_key or not index_name:
        return []

    try:
        from pinecone import Pinecone

        result = Pinecone(api_key=api_key).Index(index_name).query(
            vector=embeddings,
            top_k=max(1, int(top_k)),
            include_metadata=True,
            namespace="global",
        )
    except Exception as exc:
        logger.warning("Semantic memory lookup query failed: %s", exc, extra={"node": "memory_lookup"})
        return []

    matches = getattr(result, "matches", None)
    if matches is None and isinstance(result, dict):
        matches = result.get("matches", [])

    rows: List[Dict[str, Any]] = []
    for match in matches or []:
        metadata = getattr(match, "metadata", None)
        if metadata is None and isinstance(match, dict):
            metadata = match.get("metadata")
        if isinstance(metadata, dict):
            rows.append(metadata)
    return rows


def _fetch_rejected_kpis(fingerprint: str, limit: int = 10) -> List[str]:
    rejected = []
    conn = get_pipeline_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT TOP (?) payload
            FROM [{db_schema}].[ai_store]
            WHERE fingerprint NOT IN (?, ?)
              AND artifact_type = 'KPIS'
              AND (faithfulness_status = 'FAILED' OR cost_usd = 0)
            ORDER BY stored_at DESC
            """,
            (limit, fingerprint, artifact_storage_fingerprint(fingerprint, "KPIS")),
        )
        for row in cursor.fetchall():
            payload = json.loads(row[0])
            kpis = payload.get("kpis", [])
            rejected.extend([kpi["kpi_name"] for kpi in kpis if kpi.get("ai_confidence_score", 1.0) < 0.3])
    finally:
        conn.close()
    return rejected[:20]


def _apply_match_result(
    state: Stage01State,
    is_match: bool,
    requirements_payload: Dict[str, Any],
    kpis_payload: Dict[str, Any],
    log_context: dict,
) -> Stage01State:
    new_state = state.copy()
    if is_match:
        stored_kpis = kpis_payload.get("kpis", [])
        logger.info(
        "Layer 1 hit: exact match found; LLM extraction still required (has_requirements=%s has_kpis=%s)",
            bool(requirements_payload),
            bool(stored_kpis),
            extra=log_context,
        )
        new_state.update({
            "memory_layer1": True,
            "memory_bypass": False,
            "memory_exact_requirements_found": bool(requirements_payload),
            "memory_exact_kpis_found": bool(stored_kpis),
            "memory_exact_kpi_count": len(stored_kpis),
            "status": "EXACT_MATCH_FOUND_LLM_REQUIRED",
        })
    else:
        logger.info("Layer 1 miss", extra=log_context)
        new_state.update({
            "memory_layer1": False,
            "memory_bypass": False,
            "status": "NO_EXACT_MATCH",
        })
    return new_state


def _run_semantic_lookup(state: Stage01State, log_context: dict) -> Stage01State:
    new_state = _copy_state(state)
    model = get_embedding_model(log_context=log_context)
    if model is None:
        logger.info("Semantic memory lookup deferred; embedding provider unavailable", extra=log_context)
        new_state["memory_layer2"] = False
        return new_state

    query_text = str(new_state.get("brd_text") or new_state.get("context_text") or "").strip()
    if not query_text:
        new_state["memory_layer2"] = False
        return new_state

    embeddings = model.embed_query(query_text)
    context_kpis = _fetch_context_kpis(embeddings, top_k=3)
    rejected_kpis = _fetch_rejected_kpis(str(new_state.get("fingerprint") or ""), limit=10)

    new_state["context_kpis"] = context_kpis
    new_state["rejected_kpis"] = rejected_kpis
    new_state["memory_layer2"] = bool(context_kpis)
    return new_state


def memory_lookup_node(state: Stage01State) -> Stage01State:
    new_state = _copy_state(state)
    log_context = _log_context(new_state)

    logger.info("START memory_lookup + KPI memory", extra=log_context)

    if new_state.get("status") == "FAILED":
        return new_state

    is_match, requirements_payload, kpis_payload = _fetch_exact_match(new_state.get("fingerprint"), new_state)
    new_state = _apply_match_result(new_state, is_match, requirements_payload, kpis_payload, log_context)

    new_state["memory_layer2"] = False
    if not new_state.get("memory_bypass", False):
        new_state = _run_semantic_lookup(new_state, log_context)

    if not new_state.get("memory_bypass", False):
        logger.info("RUNNING EMBEDDING", extra=log_context)
        new_state = _chunk_and_embed(new_state)
        new_state = finalize_ingestion_after_memory(new_state)

    logger.info(
        "END memory_lookup: layer1=%s layer2=%s exact_kpi_n=%d context_n=%d rejected_n=%d",
        new_state.get("memory_layer1"),
        new_state.get("memory_layer2"),
        int(new_state.get("memory_exact_kpi_count") or 0),
        len(new_state.get("context_kpis", [])),
        len(new_state.get("rejected_kpis", [])),
        extra=log_context,
    )
    return new_state
