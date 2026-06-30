from __future__ import annotations

import os
import sys

from utilis.embeddings import get_embedding_model, get_embedding_provider_name
from utilis.env import load_backend_env


def _env_enabled(name: str, default: str = "false") -> bool:
    return str(os.getenv(name, default)).strip().lower() in {"1", "true", "yes", "on"}


def _preload_required() -> bool:
    return _env_enabled("ATHENA_EMBEDDING_PRELOAD_REQUIRED")


def _fail(message: str) -> int:
    if _preload_required():
        print(message, file=sys.stderr)
        return 1

    print(f"{message} Continuing without warmed embedding cache.", file=sys.stderr)
    return 0


def main() -> int:
    load_backend_env()

    if not _env_enabled("ATHENA_ENABLE_EMBEDDINGS"):
        print("Semantic model preload not requested; using fallback mode")
        return 0

    try:
        model = get_embedding_model(log_context={"node": "preload_embeddings"})
        if model is None:
            return _fail("Embedding preload failed: no provider is available")
        print(f"Embedding provider ready: {get_embedding_provider_name() or 'unknown'}")
    except Exception as exc:
        return _fail(f"Embedding preload failed: {exc}")

    print("Embedding preload completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
