from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def write_json_file(path: str | Path, payload: Any, **dump_options: Any) -> str:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    dump_options.setdefault("default", str)
    text = json.dumps(payload, **dump_options)
    temporary = target.with_name(f"{target.name}.tmp")
    temporary.write_text(text, encoding="utf-8")
    temporary.replace(target)
    return str(target)
