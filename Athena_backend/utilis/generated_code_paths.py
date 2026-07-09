from __future__ import annotations

import os
from pathlib import Path


def generated_code_root() -> Path:
    configured = str(os.getenv("ATHENA_GENERATED_CODE_DIR") or "").strip()
    if configured:
        return Path(configured).expanduser().resolve()

    cwd = Path.cwd().resolve()
    if cwd.name.casefold() == "athena_backend" and cwd.parent.exists():
        return cwd.parent / "generated_code"
    return cwd / "generated_code"


def generated_code_dir(*parts: str) -> Path:
    path = generated_code_root()
    for part in parts:
        cleaned = str(part or "").strip()
        if cleaned:
            path = path / cleaned
    return path
