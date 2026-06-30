from __future__ import annotations

import os
from functools import lru_cache
from typing import Any, Dict

from utilis.embeddings import (
    get_embedding_model,
    get_embedding_provider_config,
    get_embedding_provider_name,
    reset_embedding_model_cache,
)
from utilis.env import load_backend_env


def _env_enabled(name: str, default: str = "false") -> bool:
    return str(os.getenv(name, default)).strip().lower() in {"1", "true", "yes", "on"}


@lru_cache(maxsize=1)
def get_embedding_runtime_status(probe_models: bool = True) -> Dict[str, Any]:
    load_backend_env()

    env_enabled = _env_enabled("ATHENA_ENABLE_EMBEDDINGS")
    provider_config = get_embedding_provider_config()
    status: Dict[str, Any] = {
        "env_enabled": env_enabled,
        "pinecone_configured": bool(os.getenv("PINECONE_API_KEY")),
        "provider": None,
        "provider_configured": False,
        "azure_embedding_configured": provider_config["azure_configured"],
        "openai_embedding_configured": provider_config["openai_configured"],
        "sentence_transformer_available": False,
        "langchain_embedding_available": False,
        "ready": False,
    }

    if not env_enabled:
        status["reason"] = "Semantic indexing is running in fallback mode"
        return status

    if not probe_models:
        status["provider_configured"] = bool(
            provider_config["azure_configured"]
            or provider_config["openai_configured"]
            or provider_config["local_configured"]
        )
        status["provider"] = (
            "azure_openai"
            if provider_config["azure_configured"]
            else "openai"
            if provider_config["openai_configured"]
            else "local_huggingface"
        )
        status["ready"] = status["pinecone_configured"] and status["provider_configured"]
        status["langchain_embedding_available"] = status["provider_configured"]
        status["reason"] = "Lightweight health check; semantic model probing is deferred"
        return status

    model = get_embedding_model(log_context={"node": "embedding_status"})
    provider = get_embedding_provider_name()
    status["provider"] = provider
    status["provider_configured"] = bool(
        provider_config["azure_configured"]
        or provider_config["openai_configured"]
        or provider_config["local_configured"]
    )
    status["langchain_embedding_available"] = model is not None
    status["sentence_transformer_available"] = provider == "local_huggingface"

    status["ready"] = (
        status["pinecone_configured"]
        and status["langchain_embedding_available"]
    )
    if not status["ready"] and "reason" not in status:
        status["reason"] = "Embedding provider or Pinecone configuration is unavailable"

    return status


def reset_embedding_runtime_status_cache() -> None:
    get_embedding_runtime_status.cache_clear()
    reset_embedding_model_cache()
