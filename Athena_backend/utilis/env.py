from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv


def load_backend_env() -> None:
    """Load local backend environment files before SDK clients are created."""
    backend_root = Path(__file__).resolve().parents[1]

    for env_file in (backend_root / ".env", backend_root / ".myenv"):
        load_dotenv(env_file, override=False)

    load_dotenv(override=False)
