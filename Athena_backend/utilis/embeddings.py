from __future__ import annotations

import io
import os
from contextlib import redirect_stderr, redirect_stdout
from typing import Any, Dict, Optional

from utilis.env import load_backend_env
from utilis.logger import logger


load_backend_env()

DEV_MODE = os.getenv("DEV_MODE", "").strip().lower() in {"1", "true", "yes", "on", "dev"}

_EMBEDDING_MODEL: Optional[Any] = None
_EMBEDDING_PROVIDER: Optional[str] = None


class _OpenAIEmbeddingAdapter:
    def __init__(
        self,
        *,
        client: Any,
        model_name: str,
    ) -> None:
        self._client = client
        self._model_name = model_name

    def embed_query(self, text: str) -> list[float]:
        return self.embed_documents([text])[0]

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        response = self._client.embeddings.create(
            model=self._model_name,
            input=texts,
        )
        rows = sorted(response.data, key=lambda item: item.index)
        return [list(item.embedding) for item in rows]


def _env_enabled(name: str, default: str = "false") -> bool:
    return str(os.getenv(name, default)).strip().lower() in {"1", "true", "yes", "on"}


def _normalize_azure_endpoint(endpoint: str | None) -> str | None:
    if not endpoint:
        return endpoint
    normalized = endpoint.strip().rstrip("/")
    suffix = "/openai/v1"
    if normalized.lower().endswith(suffix):
        return normalized[: -len(suffix)].rstrip("/")
    return normalized


def get_embedding_provider_config() -> Dict[str, Any]:
    azure_endpoint = _normalize_azure_endpoint(
        os.getenv("AZURE_OPENAI_EMBEDDING_ENDPOINT") or os.getenv("AZURE_OPENAI_ENDPOINT")
    )
    azure_api_key = os.getenv("AZURE_OPENAI_EMBEDDING_API_KEY") or os.getenv("AZURE_OPENAI_API_KEY")
    azure_deployment = (
        os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT")
        or os.getenv("AZURE_OPENAI_EMBEDDING_MODEL")
        or os.getenv("AZURE_OPENAI_DEPLOYMENT")
    )
    openai_api_key = os.getenv("OPENAI_API_KEY")
    openai_model = os.getenv("OPENAI_EMBEDDING_MODEL") or os.getenv("OPENAI_MODEL")
    return {
        "enabled": _env_enabled("ATHENA_ENABLE_EMBEDDINGS"),
        "allow_local_fallback": _env_enabled("ATHENA_ALLOW_LOCAL_EMBEDDING_FALLBACK"),
        "azure_configured": bool(azure_endpoint and azure_api_key and azure_deployment),
        "azure_endpoint": azure_endpoint,
        "azure_api_key": azure_api_key,
        "azure_deployment": azure_deployment,
        "azure_model": os.getenv("AZURE_OPENAI_EMBEDDING_MODEL") or azure_deployment,
        "azure_api_version": (
            os.getenv("AZURE_OPENAI_EMBEDDING_API_VERSION")
            or os.getenv("AZURE_OPENAI_API_VERSION")
            or "2024-12-01-preview"
        ),
        "openai_configured": bool(openai_api_key and openai_model),
        "openai_api_key": openai_api_key,
        "openai_model": openai_model or "text-embedding-3-small",
        "local_configured": False,
    }


def _log_probe(message: str, log_context: Optional[dict], level: str = "info", *args: Any) -> None:
    extra = log_context or {"node": "embedding_provider"}
    getattr(logger, level)(message, *args, extra=extra)


def _build_azure_embedding_model(config: Dict[str, Any], log_context: Optional[dict]) -> Optional[Any]:
    if not config["azure_configured"]:
        return None
    try:
        from openai import AzureOpenAI

        _log_probe(
            "Initializing Azure OpenAI embedding model (%s)",
            log_context,
            "info",
            config["azure_deployment"],
        )
        client = AzureOpenAI(
            azure_endpoint=config["azure_endpoint"],
            api_key=config["azure_api_key"],
            api_version=config["azure_api_version"],
            timeout=10.0,
            max_retries=1,
        )
        model = _OpenAIEmbeddingAdapter(client=client, model_name=config["azure_deployment"])
        model.embed_query("athena embedding healthcheck")
        return model
    except Exception as exc:
        _log_probe("Azure OpenAI embeddings unavailable: %s", log_context, "warning", exc)
        return None


def _build_openai_embedding_model(config: Dict[str, Any], log_context: Optional[dict]) -> Optional[Any]:
    if not config["openai_configured"]:
        return None
    try:
        from openai import OpenAI

        _log_probe(
            "Initializing OpenAI embedding model (%s)",
            log_context,
            "info",
            config["openai_model"],
        )
        client = OpenAI(
            api_key=config["openai_api_key"],
            timeout=10.0,
            max_retries=1,
        )
        model = _OpenAIEmbeddingAdapter(client=client, model_name=config["openai_model"])
        model.embed_query("athena embedding healthcheck")
        return model
    except Exception as exc:
        _log_probe("OpenAI embeddings unavailable: %s", log_context, "warning", exc)
        return None


def _build_local_embedding_model(log_context: Optional[dict]) -> Optional[Any]:
    try:
        from langchain_huggingface import HuggingFaceEmbeddings

        _log_probe("Initializing local embedding model", log_context)
        os.environ["TRANSFORMERS_NO_ADVISE"] = "1"
        os.environ["TOKENIZERS_PARALLELISM"] = "false"

        kwargs = {
            "model_name": "sentence-transformers/all-MiniLM-L6-v2",
            "model_kwargs": {"local_files_only": True, "trust_remote_code": False},
            "encode_kwargs": {"normalize_embeddings": False},
        }
        if DEV_MODE:
            model = HuggingFaceEmbeddings(**kwargs)
            model.embed_query("athena embedding healthcheck")
            return model

        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            model = HuggingFaceEmbeddings(**kwargs)
            model.embed_query("athena embedding healthcheck")
        return model
    except Exception as exc:
        _log_probe("Local embeddings unavailable: %s", log_context, "warning", exc)
        return None


def get_embedding_model(*, log_context: Optional[dict] = None) -> Optional[Any]:
    global _EMBEDDING_MODEL, _EMBEDDING_PROVIDER

    if _EMBEDDING_MODEL is not None:
        return _EMBEDDING_MODEL

    config = get_embedding_provider_config()
    if not config["enabled"]:
        _log_probe("Semantic indexing deferred; embeddings are disabled", log_context)
        return None

    azure_model = _build_azure_embedding_model(config, log_context)
    if azure_model is not None:
        _EMBEDDING_MODEL = azure_model
        _EMBEDDING_PROVIDER = "azure_openai"
        return _EMBEDDING_MODEL

    if config["azure_configured"] and not config["allow_local_fallback"]:
        _EMBEDDING_PROVIDER = None
        _log_probe(
            "Semantic indexing deferred; Azure embeddings are configured but unavailable",
            log_context,
            "warning",
        )
        return None

    openai_model = _build_openai_embedding_model(config, log_context)
    if openai_model is not None:
        _EMBEDDING_MODEL = openai_model
        _EMBEDDING_PROVIDER = "openai"
        return _EMBEDDING_MODEL

    if not config["allow_local_fallback"]:
        _EMBEDDING_PROVIDER = None
        _log_probe("Semantic indexing deferred; local embedding fallback is disabled", log_context, "warning")
        return None

    local_model = _build_local_embedding_model(log_context)
    if local_model is not None:
        _EMBEDDING_MODEL = local_model
        _EMBEDDING_PROVIDER = "local_huggingface"
        return _EMBEDDING_MODEL

    _EMBEDDING_PROVIDER = None
    _log_probe("Semantic indexing deferred; no embedding provider is available", log_context, "warning")
    return None


def get_embedding_provider_name() -> Optional[str]:
    return _EMBEDDING_PROVIDER


def reset_embedding_model_cache() -> None:
    global _EMBEDDING_MODEL, _EMBEDDING_PROVIDER
    _EMBEDDING_MODEL = None
    _EMBEDDING_PROVIDER = None
