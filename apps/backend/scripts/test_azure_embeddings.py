from __future__ import annotations

import os
import sys
from pathlib import Path

from openai import AzureOpenAI

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from utilis.env import load_backend_env


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _normalize_endpoint(endpoint: str) -> str:
    endpoint = endpoint.strip().rstrip("/")
    suffix = "/openai/v1"
    if endpoint.lower().endswith(suffix):
        return endpoint[: -len(suffix)].rstrip("/")
    return endpoint


def main() -> int:
    load_backend_env()

    endpoint = _normalize_endpoint(_required("AZURE_OPENAI_EMBEDDING_ENDPOINT"))
    api_key = _required("AZURE_OPENAI_EMBEDDING_API_KEY")
    deployment = (
        os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT", "").strip()
        or _required("AZURE_OPENAI_EMBEDDING_MODEL")
    )
    api_version = (
        os.getenv("AZURE_OPENAI_EMBEDDING_API_VERSION", "").strip()
        or "2024-12-01-preview"
    )

    client = AzureOpenAI(
        azure_endpoint=endpoint,
        api_key=api_key,
        api_version=api_version,
        timeout=10.0,
        max_retries=1,
    )

    response = client.embeddings.create(
        model=deployment,
        input=["athena embedding healthcheck"],
    )
    first = response.data[0].embedding if response.data else []

    print("Embedding probe succeeded")
    print(f"endpoint={endpoint}")
    print(f"deployment={deployment}")
    print(f"api_version={api_version}")
    print(f"vector_count={len(response.data)}")
    print(f"dimensions={len(first)}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Embedding probe failed: {exc}", file=sys.stderr)
        raise
