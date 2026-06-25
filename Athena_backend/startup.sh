#!/bin/bash
set -e

echo "========== Athena backend startup =========="
echo "Timestamp: $(date -u)"
echo "User: $(whoami)"
echo "PWD: $(pwd)"
echo "Python: $(python --version 2>&1)"
echo "Port: ${PORT:-8000}"

echo "Top-level files:"
ls -la

echo "API package:"
ls -la api || true

echo "Selected environment flags:"
env | sort | grep -E '^(AZURE_SQL_HOST|AZURE_SQL_PIPELINE_DATABASE|AZURE_SQL_SOURCE_HOST|AZURE_SQL_SOURCE_DATABASE|AZURE_OPENAI_ENDPOINT|AZURE_OPENAI_DEPLOYMENT|ATHENA_CORS_ORIGINS|PINECONE_INDEX_NAME|ADLS_ACCOUNT_URL|SCM_DO_BUILD_DURING_DEPLOYMENT|PORT)=' || true

echo "Installing Python dependencies..."
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

echo "Installed package checks:"
python -m pip show fastapi uvicorn pyodbc azure-identity azure-storage-file-datalake pinecone || true

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
