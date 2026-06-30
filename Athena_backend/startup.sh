#!/bin/bash
set -e

echo "========== Athena backend startup =========="
echo "Timestamp: $(date -u)"
echo "PWD: $(pwd)"
echo "Python: $(python --version 2>&1)"
echo "Port: ${PORT:-8000}"

export ATHENA_BLOCK_EMBEDDINGS=true
export ATHENA_ENABLE_EMBEDDINGS=false
export ATHENA_PRELOAD_EMBEDDINGS=false
export ATHENA_ALLOW_LOCAL_EMBEDDING_FALLBACK=false
export ATHENA_USE_DOMAIN_KB=false
echo "Embedding feature blocked for lightweight runtime"

if [ -d "/home/site/wwwroot/antenv" ]; then
  echo "Activating Oryx virtual environment..."
  . /home/site/wwwroot/antenv/bin/activate
else
  echo "WARNING: Oryx virtual environment not found at /home/site/wwwroot/antenv"
fi

echo "Embedding preload disabled."

if [ "${ATHENA_STARTUP_IMPORT_SMOKE,,}" = "true" ] || [ "${ATHENA_STARTUP_IMPORT_SMOKE}" = "1" ]; then
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
else
  echo "Import smoke test skipped; ATHENA_STARTUP_IMPORT_SMOKE=${ATHENA_STARTUP_IMPORT_SMOKE:-false}"
fi

echo "Starting Uvicorn..."
exec python -m uvicorn api.main:app --host 0.0.0.0 --port "${PORT:-8000}" --log-level debug
