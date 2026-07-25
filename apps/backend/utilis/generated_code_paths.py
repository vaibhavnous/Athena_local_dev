from __future__ import annotations

import os
from pathlib import Path

from utilis.runtime_paths import runtime_dir


def generated_code_root() -> Path:
    cwd = Path.cwd().resolve()
    local_default = cwd.parent / "generated_code" if cwd.name.casefold() == "athena_backend" and cwd.parent.exists() else cwd / "generated_code"
    return runtime_dir("ATHENA_GENERATED_CODE_DIR", local_default, "generated_code")


def generated_code_dir(*parts: str) -> Path:
    path = generated_code_root()
    for part in parts:
        cleaned = str(part or "").strip()
        if cleaned:
            path = path / cleaned
    return path
