from __future__ import annotations

import os
import sys

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
        print("Embedding preload skipped: ATHENA_ENABLE_EMBEDDINGS is disabled")
        return 0

    try:
        from sentence_transformers import SentenceTransformer
        from langchain_huggingface import HuggingFaceEmbeddings
    except Exception as exc:
        return _fail(f"Embedding preload failed: dependency import error: {exc}")

    hf_home = os.getenv("HF_HOME") or os.getenv("SENTENCE_TRANSFORMERS_HOME")
    if hf_home:
        print(f"Embedding cache directory: {hf_home}")

    try:
        print("Preloading SentenceTransformer model...")
        st_model = SentenceTransformer("all-MiniLM-L6-v2", local_files_only=False)
        st_model.encode("athena embedding warmup")

        print("Preloading LangChain HuggingFace embeddings...")
        lc_model = HuggingFaceEmbeddings(
            model_name="sentence-transformers/all-MiniLM-L6-v2",
            model_kwargs={"local_files_only": False, "trust_remote_code": False},
            encode_kwargs={"normalize_embeddings": False},
        )
        lc_model.embed_query("athena embedding warmup")
    except Exception as exc:
        return _fail(f"Embedding preload failed: {exc}")

    print("Embedding preload completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
