from __future__ import annotations

import json
import os
import sys
import uuid
from pathlib import Path
from typing import Any

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from pinecone import Pinecone

from nodes import ingestion, kpi_extraction, memory_lookup, table_nomination
from utilis import domain_kb
from utilis.embeddings import get_embedding_model, get_embedding_provider_name
from utilis.env import load_backend_env


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _match_metadata(match: Any) -> dict[str, Any]:
    if isinstance(match, dict):
        return match.get("metadata", {}) or {}
    return getattr(match, "metadata", {}) or {}


def _index(pc: Pinecone, name: str):
    return pc.Index(name)


def _seed_schema_vector(model: Any, pc: Pinecone, vector_id: str, database_name: str) -> None:
    text = "Table claims contains column claim_amount used for insurance claim severity KPI"
    vector = model.embed_query(text)
    _index(pc, os.getenv("PINECONE_SCHEMA_INDEX_NAME") or "metadata").upsert(
        vectors=[
            {
                "id": vector_id,
                "values": vector,
                "metadata": {
                    "database_name": database_name,
                    "schema_name": "dbo",
                    "table_name": "claims",
                    "column_name": "claim_amount",
                    "type": "schema",
                },
            }
        ],
        namespace="schema",
    )


def _delete_schema_vector(pc: Pinecone, vector_id: str) -> None:
    try:
        _index(pc, os.getenv("PINECONE_SCHEMA_INDEX_NAME") or "metadata").delete(ids=[vector_id], namespace="schema")
    except Exception:
        pass


def test_ingestion_chunk_and_embed(pc: Pinecone) -> dict[str, Any]:
    print("Running ingestion._chunk_and_embed...", flush=True)
    run_id = f"smoke-ingestion-{uuid.uuid4()}"
    state = {
        "run_id": run_id,
        "brd_text": "Insurance claim severity KPI requires claim amount, policy id, and settlement delay analysis.",
        "fingerprint": run_id,
        "status": "RUNNING",
    }
    result = ingestion._chunk_and_embed(state)
    ok = result.get("brd_embedded") is True
    try:
        _index(pc, os.getenv("PINECONE_INDEX_NAME") or "ai-store-index").delete(filter={"run_id": run_id}, namespace="global")
    except Exception:
        pass
    if not ok:
        raise RuntimeError(f"ingestion._chunk_and_embed did not mark brd_embedded=true: {result}")
    return {"node": "ingestion._chunk_and_embed", "status": "ok", "brd_embedded": True}


def test_schema_consumers(model: Any, pc: Pinecone) -> list[dict[str, Any]]:
    print("Running table_nomination and kpi_extraction schema consumers...", flush=True)
    vector_id = f"smoke-schema-{uuid.uuid4()}"
    database_name = f"smokedb_{uuid.uuid4().hex[:8]}"
    _seed_schema_vector(model, pc, vector_id, database_name)
    try:
        table_results = table_nomination._semantic_search(
            "claim severity amount KPI",
            [database_name],
        )
        if not table_results:
            raise RuntimeError("table_nomination._semantic_search returned no semantic matches")

        kpi_results = kpi_extraction._fetch_relevant_schema(
            "claim severity amount KPI",
            [database_name],
            top_k=5,
        )
        if not kpi_results:
            raise RuntimeError("kpi_extraction._fetch_relevant_schema returned no schema rows")

        return [
            {
                "node": "table_nomination._semantic_search",
                "status": "ok",
                "matches": len(table_results),
                "first_table": table_results[0].get("table_name"),
            },
            {
                "node": "kpi_extraction._fetch_relevant_schema",
                "status": "ok",
                "matches": len(kpi_results),
                "first_table": kpi_results[0].get("table_name"),
            },
        ]
    finally:
        _delete_schema_vector(pc, vector_id)


def test_memory_lookup(model: Any, pc: Pinecone) -> dict[str, Any]:
    print("Running memory_lookup._run_semantic_lookup...", flush=True)
    text = f"Unique semantic memory smoke {uuid.uuid4()} claim ratio KPI"
    fingerprint = f"smoke-memory-{uuid.uuid4()}"
    vector_id = f"{fingerprint}_chunk_0"
    index_name = os.getenv("PINECONE_INDEX_NAME") or "ai-store-index"
    _index(pc, index_name).upsert(
        vectors=[
            {
                "id": vector_id,
                "values": model.embed_query(text),
                "metadata": {"fingerprint": fingerprint, "artifact_type": "BRD", "source": "smoke"},
            }
        ],
        namespace="global",
    )

    original_context = memory_lookup._fetch_context_kpis
    original_rejected = memory_lookup._fetch_rejected_kpis
    memory_lookup._fetch_context_kpis = lambda embeddings, top_k=3: [{"kpi_name": "Smoke KPI"}]
    memory_lookup._fetch_rejected_kpis = lambda fingerprint, limit=10: []
    try:
        result = memory_lookup._run_semantic_lookup(
            {"run_id": fingerprint, "fingerprint": fingerprint, "brd_text": text},
            {"run_id": fingerprint, "node": "memory_lookup_smoke"},
        )
    finally:
        memory_lookup._fetch_context_kpis = original_context
        memory_lookup._fetch_rejected_kpis = original_rejected
        try:
            _index(pc, index_name).delete(ids=[vector_id], namespace="global")
        except Exception:
            pass

    if result.get("memory_layer2") is not True:
        raise RuntimeError(f"memory_lookup._run_semantic_lookup did not set memory_layer2=true: {result}")
    return {
        "node": "memory_lookup._run_semantic_lookup",
        "status": "ok",
        "memory_layer2": True,
        "context_kpis": len(result.get("context_kpis", [])),
    }


def test_domain_kb() -> dict[str, Any]:
    print("Running domain_kb upsert/load...", flush=True)
    kb_id = f"SMOKE_KB_{uuid.uuid4().hex[:8]}"
    namespace = kb_id
    row_id = f"smoke-kb-{uuid.uuid4()}"
    row = {
        "kb_row_id": row_id,
        "knowledge_base_id": kb_id,
        "domain_profile": "Insurance",
        "kb_content_type": domain_kb.KB_CONTENT_TABLE,
        "database_name": "smokedb",
        "schema_name": "dbo",
        "table_name": "claims",
        "column_name": "",
        "embedding_text": "Insurance claims table includes claim amount and settlement delay KPI context",
        "prompt_context": "Use claims.claim_amount for claim severity analytics.",
        "is_active": True,
    }
    result = domain_kb.upsert_kb_rows_to_pinecone(
        [row],
        index_name=os.getenv("PINECONE_KNOWLEDGE_BASE_INDEX_NAME") or "knowledgebase",
        namespace=namespace,
        refresh=False,
    )
    previous_enabled = os.environ.get("ATHENA_USE_DOMAIN_KB")
    previous_namespace = os.environ.get("PINECONE_KNOWLEDGE_BASE_NAMESPACE")
    os.environ["ATHENA_USE_DOMAIN_KB"] = "true"
    os.environ["PINECONE_KNOWLEDGE_BASE_NAMESPACE"] = namespace
    try:
        loaded = domain_kb.load_domain_kb(
            query_text="claim amount severity analytics",
            top_k=3,
            max_chars=1000,
            content_types=[domain_kb.KB_CONTENT_TABLE],
            knowledge_base_id=kb_id,
        )
    finally:
        if previous_enabled is None:
            os.environ.pop("ATHENA_USE_DOMAIN_KB", None)
        else:
            os.environ["ATHENA_USE_DOMAIN_KB"] = previous_enabled
        if previous_namespace is None:
            os.environ.pop("PINECONE_KNOWLEDGE_BASE_NAMESPACE", None)
        else:
            os.environ["PINECONE_KNOWLEDGE_BASE_NAMESPACE"] = previous_namespace
    try:
        _index(Pinecone(api_key=_required("PINECONE_API_KEY")), os.getenv("PINECONE_KNOWLEDGE_BASE_INDEX_NAME") or "knowledgebase").delete(
            ids=[row_id],
            namespace=namespace,
        )
    except Exception:
        pass

    if result.get("rows_upserted") != 1:
        raise RuntimeError(f"domain_kb.upsert_kb_rows_to_pinecone did not upsert one row: {result}")
    if loaded.get("rows_retrieved", 0) < 1:
        raise RuntimeError(f"domain_kb.load_domain_kb did not retrieve smoke row: {loaded}")
    return {
        "node": "domain_kb.upsert/load",
        "status": "ok",
        "rows_upserted": result.get("rows_upserted"),
        "rows_retrieved": loaded.get("rows_retrieved"),
    }


def main() -> int:
    load_backend_env()
    print("Initializing embedding provider...", flush=True)
    model = get_embedding_model(log_context={"node": "embedding_node_smoke"})
    if model is None:
        raise RuntimeError("Embedding provider is unavailable")
    pc = Pinecone(api_key=_required("PINECONE_API_KEY"))

    results = [
        test_ingestion_chunk_and_embed(pc),
        *test_schema_consumers(model, pc),
        test_memory_lookup(model, pc),
        test_domain_kb(),
    ]
    print(
        json.dumps(
            {
                "status": "ok",
                "provider": get_embedding_provider_name(),
                "embedding_dimensions": len(model.embed_query("dimension check")),
                "results": results,
                "note": "Schema embedding is protected by a unit test that verifies scoped database deletes instead of namespace-wide deletes.",
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, indent=2), file=sys.stderr)
        raise SystemExit(1)
