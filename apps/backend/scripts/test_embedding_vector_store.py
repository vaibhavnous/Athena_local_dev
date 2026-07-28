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

from utilis.embeddings import get_embedding_model, get_embedding_provider_name
from utilis.env import load_backend_env


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _as_dict(value: Any) -> dict[str, Any]:
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if isinstance(value, dict):
        return value
    return {}


def _index_dimension(pc: Pinecone, index_name: str) -> int | None:
    details = _as_dict(pc.describe_index(index_name))
    dimension = details.get("dimension")
    return int(dimension) if dimension is not None else None


def _matches(result: Any) -> list[Any]:
    matches = getattr(result, "matches", None)
    if matches is None and isinstance(result, dict):
        matches = result.get("matches", [])
    return list(matches or [])


def _match_value(match: Any, key: str, default: Any = None) -> Any:
    if isinstance(match, dict):
        return match.get(key, default)
    return getattr(match, key, default)


def _test_index(pc: Pinecone, index_name: str, vector: list[float]) -> dict[str, Any]:
    namespace = "athena-embedding-smoke-test"
    vector_id = f"smoke-{uuid.uuid4()}"
    index = pc.Index(index_name)
    dimension = _index_dimension(pc, index_name)

    if dimension is not None and dimension != len(vector):
        raise RuntimeError(
            f"Pinecone index {index_name!r} dimension mismatch: "
            f"index={dimension}, embedding={len(vector)}"
        )

    index.upsert(
        vectors=[
            {
                "id": vector_id,
                "values": vector,
                "metadata": {"source": "athena_embedding_smoke_test"},
            }
        ],
        namespace=namespace,
    )
    result = index.query(
        vector=vector,
        top_k=1,
        include_metadata=True,
        namespace=namespace,
    )

    try:
        index.delete(ids=[vector_id], namespace=namespace)
    except Exception:
        pass

    matches = _matches(result)
    if not matches:
        raise RuntimeError(f"Pinecone index {index_name!r} returned no matches after upsert")

    top = matches[0]
    return {
        "index": index_name,
        "dimension": dimension,
        "top_match_id": _match_value(top, "id"),
        "top_score": float(_match_value(top, "score", 0.0) or 0.0),
    }


def main() -> int:
    load_backend_env()

    model = get_embedding_model(log_context={"node": "embedding_vector_store_test"})
    if model is None:
        raise RuntimeError("No embedding provider is available")

    vector = list(model.embed_query("semantic matching for table nomination in athena"))
    if not vector:
        raise RuntimeError("Embedding provider returned an empty vector")

    pc = Pinecone(api_key=_required("PINECONE_API_KEY"))
    index_names = [
        os.getenv("PINECONE_INDEX_NAME", "ai-store-index").strip() or "ai-store-index",
        os.getenv("PINECONE_SCHEMA_INDEX_NAME", "").strip(),
    ]
    index_names = list(dict.fromkeys(name for name in index_names if name))

    results = [_test_index(pc, index_name, vector) for index_name in index_names]
    print(
        json.dumps(
            {
                "status": "ok",
                "provider": get_embedding_provider_name(),
                "embedding": {
                    "deployment": os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT"),
                    "dimensions": len(vector),
                },
                "pinecone": results,
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
