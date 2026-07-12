#!/bin/bash
set -e

echo "========== Athena backend startup =========="
echo "Timestamp: $(date -u)"
echo "PWD: $(pwd)"
echo "Python: $(python --version 2>&1)"
echo "Port: ${PORT:-8000}"

export ATHENA_BLOCK_EMBEDDINGS="${ATHENA_BLOCK_EMBEDDINGS:-true}"
export ATHENA_ENABLE_EMBEDDINGS="${ATHENA_ENABLE_EMBEDDINGS:-false}"
export ATHENA_PRELOAD_EMBEDDINGS="${ATHENA_PRELOAD_EMBEDDINGS:-false}"
export ATHENA_ALLOW_LOCAL_EMBEDDING_FALLBACK="${ATHENA_ALLOW_LOCAL_EMBEDDING_FALLBACK:-false}"
export ATHENA_USE_DOMAIN_KB="${ATHENA_USE_DOMAIN_KB:-false}"
export ATHENA_BACKGROUND_WORKERS="${ATHENA_BACKGROUND_WORKERS:-1}"
export BRONZE_MAX_WORKERS="${BRONZE_MAX_WORKERS:-3}"
export SILVER_MAX_WORKERS="${SILVER_MAX_WORKERS:-3}"
export COLUMN_PROFILING_MAX_WORKERS="${COLUMN_PROFILING_MAX_WORKERS:-3}"
echo "Embedding config: blocked=${ATHENA_BLOCK_EMBEDDINGS} enabled=${ATHENA_ENABLE_EMBEDDINGS} preload=${ATHENA_PRELOAD_EMBEDDINGS} domain_kb=${ATHENA_USE_DOMAIN_KB}"
echo "Worker config: web=1 background=${ATHENA_BACKGROUND_WORKERS} bronze=${BRONZE_MAX_WORKERS} silver=${SILVER_MAX_WORKERS} profiling=${COLUMN_PROFILING_MAX_WORKERS}"

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

echo "Starting Gunicorn..."
# ponytail: one web process is intentional until background job claiming is distributed.
exec gunicorn -k uvicorn.workers.UvicornWorker api.main:app \
  --bind "0.0.0.0:${PORT:-8000}" \
  --workers 1 \
  --timeout "${GUNICORN_TIMEOUT_SECONDS:-600}" \
  --graceful-timeout 30 \
  --keep-alive 5 \
  --log-level info \
  --access-logfile - \
  --error-logfile -
