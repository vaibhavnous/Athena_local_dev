from __future__ import annotations

import io
import os
from contextlib import redirect_stderr, redirect_stdout
from functools import lru_cache
from typing import Any, Dict

from utilis.env import load_backend_env


HF_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
ST_MODEL_NAME = "all-MiniLM-L6-v2"


def _env_enabled(name: str, default: str = "false") -> bool:
    return str(os.getenv(name, default)).strip().lower() in {"1", "true", "yes", "on"}


@lru_cache(maxsize=1)
def get_embedding_runtime_status() -> Dict[str, Any]:
    load_backend_env()

    env_enabled = _env_enabled("ATHENA_ENABLE_EMBEDDINGS")
    status: Dict[str, Any] = {
        "env_enabled": env_enabled,
        "pinecone_configured": bool(os.getenv("PINECONE_API_KEY")),
        "sentence_transformer_available": False,
        "langchain_embedding_available": False,
        "ready": False,
    }

    if not env_enabled:
        status["reason"] = "ATHENA_ENABLE_EMBEDDINGS is disabled"
        return status

    try:
        from sentence_transformers import SentenceTransformer

        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            model = SentenceTransformer(ST_MODEL_NAME, local_files_only=True)
            model.encode("athena embedding healthcheck")
        status["sentence_transformer_available"] = True
    except Exception as exc:
        status["sentence_transformer_error"] = str(exc)

    try:
        from langchain_huggingface import HuggingFaceEmbeddings

        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            model = HuggingFaceEmbeddings(
                model_name=HF_MODEL_NAME,
                model_kwargs={"local_files_only": True, "trust_remote_code": False},
                encode_kwargs={"normalize_embeddings": False},
            )
            model.embed_query("athena embedding healthcheck")
        status["langchain_embedding_available"] = True
    except Exception as exc:
        status["langchain_embedding_error"] = str(exc)

    status["ready"] = (
        status["pinecone_configured"]
        and status["sentence_transformer_available"]
        and status["langchain_embedding_available"]
    )
    if not status["ready"] and "reason" not in status:
        status["reason"] = "Local embedding model or Pinecone configuration is unavailable"

    return status


def reset_embedding_runtime_status_cache() -> None:
    get_embedding_runtime_status.cache_clear()
