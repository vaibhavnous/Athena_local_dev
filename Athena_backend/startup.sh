#!/bin/bash
set -e

echo "========== Athena backend startup =========="
echo "Timestamp: $(date -u)"
echo "PWD: $(pwd)"
echo "Python: $(python --version 2>&1)"
echo "Port: ${PORT:-8000}"

if [ -d "/home/site/wwwroot/antenv" ]; then
  echo "Activating Oryx virtual environment..."
  . /home/site/wwwroot/antenv/bin/activate
else
  echo "WARNING: Oryx virtual environment not found at /home/site/wwwroot/antenv"
fi

if [ -z "${HF_HOME:-}" ] && [ -d "/home/site" ]; then
  export HF_HOME="/home/site/huggingface"
fi

if [ -z "${SENTENCE_TRANSFORMERS_HOME:-}" ] && [ -n "${HF_HOME:-}" ]; then
  export SENTENCE_TRANSFORMERS_HOME="${HF_HOME}"
fi

if [ -n "${HF_HOME:-}" ]; then
  mkdir -p "${HF_HOME}"
  echo "HF_HOME: ${HF_HOME}"
fi

if [ "${ATHENA_PRELOAD_EMBEDDINGS,,}" = "true" ] || [ "${ATHENA_PRELOAD_EMBEDDINGS}" = "1" ] || [ "${ATHENA_PRELOAD_EMBEDDINGS,,}" = "yes" ] || [ "${ATHENA_PRELOAD_EMBEDDINGS,,}" = "on" ]; then
  echo "Preloading embedding model cache..."
  if ! python scripts/preload_embeddings.py; then
    echo "Embedding preload failed and ATHENA_EMBEDDING_PRELOAD_REQUIRED is enabled; aborting startup."
    exit 1
  fi
else
  echo "Embedding preload skipped at startup; ATHENA_PRELOAD_EMBEDDINGS=${ATHENA_PRELOAD_EMBEDDINGS:-false}"
fi

echo "Import smoke test:"
python - <<'PY'
import importlib
import traceback

try:
    module = importlib.import_module("api.main")
    app = getattr(module, "app", None)
    print(f"api.main import succeeded; app_present={app is not None}")
except Exception:
    print("api.main import failed")
    traceback.print_exc()
    raise
PY

echo "Starting Uvicorn..."
exec python -m uvicorn api.main:app --host 0.0.0.0 --port "${PORT:-8000}" --log-level debug
