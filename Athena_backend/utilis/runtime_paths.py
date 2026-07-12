from __future__ import annotations

import os
from pathlib import Path


def app_service_data_dir() -> Path | None:
    """Return Azure App Service's persistent data root when running there."""
    if not Path("/home/site").exists():
        return None
    return Path(os.getenv("ATHENA_APP_DATA_DIR") or "/home/site/data").expanduser().resolve()


def runtime_dir(env_name: str, local_default: Path, azure_child: str) -> Path:
    configured = str(os.getenv(env_name) or "").strip()
    if configured:
        return Path(configured).expanduser().resolve()

    data_dir = app_service_data_dir()
    if data_dir:
        return data_dir / azure_child

    return local_default.resolve()
