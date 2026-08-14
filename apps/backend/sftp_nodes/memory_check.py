from __future__ import annotations

from nodes.ingestion import _chunk_and_embed, finalize_ingestion_after_memory
from nodes.memory_lookup import _fetch_context_kpis
from state import Stage01State
from utilis.embeddings import get_embedding_model
from utilis.logger import logger


def _semantic_context(state: Stage01State, log_context: dict) -> Stage01State:
    updated = state.copy()
    model = get_embedding_model(log_context=log_context)
    query_text = str(updated.get("brd_text") or updated.get("context_text") or "").strip()
    if model is None or not query_text:
        updated["memory_layer2"] = False
        updated["context_kpis"] = []
        return updated

    context_kpis = _fetch_context_kpis(model.embed_query(query_text), top_k=3)
    updated["context_kpis"] = context_kpis
    updated["memory_layer2"] = bool(context_kpis)
    return updated


def sftp_memory_check_node(state: Stage01State) -> Stage01State:
    """Run bounded ADLS memory lookup without the legacy ai_store JSON scan."""
    updated = state.copy()
    log_context = {
        "run_id": updated.get("run_id", "unknown"),
        "node": "memory_lookup",
        "fingerprint": updated.get("fingerprint"),
    }
    logger.info("START ADLS memory_lookup + KPI memory", extra=log_context)
    if updated.get("status") == "FAILED":
        return updated

    # ponytail: exact ai_store lookup never bypasses ADLS extraction, so avoid its
    # unindexed legacy JSON scan. Add an indexed logical fingerprint if exact reuse
    # later becomes functional rather than informational.
    updated.update({
        "memory_layer1": False,
        "memory_bypass": False,
        "memory_exact_requirements_found": False,
        "memory_exact_kpis_found": False,
        "memory_exact_kpi_count": 0,
        "rejected_kpis": [],
        "status": "NO_EXACT_MATCH",
    })
    updated = _semantic_context(updated, log_context)
    logger.info("RUNNING ADLS EMBEDDING", extra=log_context)
    updated = _chunk_and_embed(updated)
    updated = finalize_ingestion_after_memory(updated)
    logger.info(
        "END ADLS memory_lookup: layer1=%s layer2=%s context_n=%d",
        updated.get("memory_layer1"),
        updated.get("memory_layer2"),
        len(updated.get("context_kpis", [])),
        extra=log_context,
    )
    return updated
