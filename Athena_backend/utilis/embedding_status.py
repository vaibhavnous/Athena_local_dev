from __future__ import annotations

from functools import lru_cache
from typing import Any, Dict


@lru_cache(maxsize=1)
def get_embedding_runtime_status(probe_models: bool = True) -> Dict[str, Any]:
    return {
        "blocked": True,
        "env_enabled": False,
        "pinecone_configured": False,
        "provider": None,
        "provider_configured": False,
        "azure_embedding_configured": False,
        "openai_embedding_configured": False,
        "sentence_transformer_available": False,
        "langchain_embedding_available": False,
        "ready": False,
        "reason": "Embedding feature is blocked",
    }


def reset_embedding_runtime_status_cache() -> None:
    get_embedding_runtime_status.cache_clear()
