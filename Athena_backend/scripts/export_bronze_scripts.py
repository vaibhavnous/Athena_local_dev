from __future__ import annotations

from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from services.pipeline_runtime import load_checkpoint_state


def _safe_name(value: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "_" for ch in str(value or "")).strip("_") or "unknown"


def export_bronze_scripts(run_id: str) -> list[Path]:
    checkpoint = load_checkpoint_state(run_id) or {}
    items = checkpoint.get("bronze_generation_results") or []
    output_dir = Path(__file__).resolve().parents[1] / "generated_code" / "bronze"
    output_dir.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        entity = _safe_name(item.get("entity") or "bronze")
        body = str(item.get("generated_bronze_script") or "").strip()
        if not body:
            continue
        path = output_dir / f"{_safe_name(run_id)}_{entity}_bronze.py"
        path.write_text(body + "\n", encoding="utf-8")
        written.append(path)
    return written


if __name__ == "__main__":
    RUN_ID = "b5f2cd29-4958-4285-a3fe-f6153ceadd9b"
    for path in export_bronze_scripts(RUN_ID):
        print(path)
