from __future__ import annotations

import os
import re
from pathlib import Path

from utilis.runtime_paths import app_service_data_dir


def _local_generated_code_root(cwd: Path) -> Path:
    if cwd.name.casefold() == "backend" and cwd.parent.name.casefold() == "apps":
        return cwd.parents[1] / "generated_code"
    if cwd.name.casefold() == "athena_backend" and cwd.parent.exists():
        return cwd.parent / "generated_code"
    return cwd / "generated_code"


def generated_code_root() -> Path:
    cwd = Path.cwd().resolve()
    configured = str(os.getenv("ATHENA_GENERATED_CODE_ROOT") or os.getenv("ATHENA_GENERATED_CODE_DIR") or "").strip()
    if configured:
        return Path(configured).expanduser().resolve()

    data_dir = app_service_data_dir()
    if data_dir:
        return data_dir / "generated_code"

    return _local_generated_code_root(cwd).resolve()


def generated_code_dir(*parts: str) -> Path:
    path = generated_code_root()
    for part in parts:
        cleaned = str(part or "").strip()
        if cleaned:
            path = path / cleaned
    return path


def generated_run_slug(run_id: object) -> str:
    slug = re.sub(r"[^A-Za-z0-9_-]+", "_", str(run_id or "run").strip()).strip("_")
    return slug[:96] or "run"


def generated_run_dir(target_warehouse: str, run_id: object, *parts: str) -> Path:
    return generated_code_dir(str(target_warehouse or "databricks").lower(), generated_run_slug(run_id), *parts)
