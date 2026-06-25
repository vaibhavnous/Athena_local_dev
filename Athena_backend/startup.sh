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
