from __future__ import annotations

import os
from functools import lru_cache
from typing import Any, Dict

from utilis.embeddings import (
    get_embedding_model,
    get_embedding_provider_config,
    get_embedding_provider_name,
)


@lru_cache(maxsize=1)
def get_embedding_runtime_status(probe_models: bool = True) -> Dict[str, Any]:
    config = get_embedding_provider_config()
    provider = get_embedding_provider_name()
    provider_error = None

    if probe_models and not config["blocked"] and config["enabled"] and provider is None:
        try:
            if get_embedding_model() is not None:
                provider = get_embedding_provider_name()
        except Exception as exc:
            provider_error = str(exc)

    provider_configured = bool(
        config["azure_configured"]
        or config["openai_configured"]
        or config["allow_local_fallback"]
    )
    if not provider and not probe_models:
        if config["azure_configured"]:
            provider = "azure_openai"
        elif config["openai_configured"]:
            provider = "openai"
        elif config["allow_local_fallback"]:
            provider = "local_huggingface"
    ready = bool(config["enabled"] and provider_configured) if not probe_models else bool(provider)

    status: Dict[str, Any] = {
        "blocked": config["blocked"],
        "env_enabled": config["enabled"],
        "pinecone_configured": bool(os.getenv("PINECONE_API_KEY")),
        "provider": provider,
        "provider_configured": provider_configured,
        "azure_embedding_configured": config["azure_configured"],
        "openai_embedding_configured": config["openai_configured"],
        "sentence_transformer_available": bool(config["allow_local_fallback"]),
        "langchain_embedding_available": bool(config["allow_local_fallback"]),
        "ready": ready,
    }

    if provider_error:
        status["provider_error"] = provider_error

    if config["blocked"]:
        status["reason"] = "Embedding feature is blocked"
    elif not config["enabled"]:
        status["reason"] = "Semantic indexing is disabled by environment"
    elif ready:
        status["reason"] = "Embedding provider is ready" if probe_models else "Embedding provider configuration present"
    else:
        status["reason"] = "Embedding provider configuration is unavailable"

    return status


def reset_embedding_runtime_status_cache() -> None:
    get_embedding_runtime_status.cache_clear()
