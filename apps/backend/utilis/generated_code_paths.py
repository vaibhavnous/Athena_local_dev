from __future__ import annotations

import os
import re
from pathlib import Path

from utilis.runtime_paths import runtime_dir


def generated_code_root() -> Path:
    cwd = Path.cwd().resolve()
    backend_parent = cwd.parents[1] if cwd.parent.name.casefold() == "apps" else cwd.parent
    local_default = (
        backend_parent / "generated_code"
        if cwd.name.casefold() in {"athena_backend", "backend"} and cwd.parent.exists()
        else cwd / "generated_code"
    )
    return runtime_dir("ATHENA_GENERATED_CODE_DIR", local_default, "generated_code")


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


def generated_artifact_uri(path: object) -> str:
    root = generated_code_root().resolve()
    artifact = Path(str(path or "")).resolve()
    try:
        relative = artifact.relative_to(root)
    except ValueError as exc:
        raise ValueError("Generated artifact must be stored under ATHENA_GENERATED_CODE_DIR.") from exc
    if not artifact.is_file():
        raise ValueError(f"Generated artifact does not exist: {artifact}")
    return "generated-code://" + relative.as_posix()


def resolve_generated_artifact_uri(uri: object) -> Path:
    value = str(uri or "").strip()
    prefix = "generated-code://"
    if not value.startswith(prefix):
        raise ValueError("Only generated-code:// artifact URIs are allowed.")
    relative = Path(value.removeprefix(prefix))
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError("Generated artifact URI escapes the configured artifact root.")
    resolved = (generated_code_root().resolve() / relative).resolve()
    try:
        resolved.relative_to(generated_code_root().resolve())
    except ValueError as exc:
        raise ValueError("Generated artifact URI escapes the configured artifact root.") from exc
    return resolved


def verified_execution_artifact(spec_value: object, *, platform: str) -> Path:
    from services.metadata_contracts import file_sha256, validate_execution_spec

    spec = validate_execution_spec(spec_value, platform=platform)
    path = resolve_generated_artifact_uri(spec["artifact_uri"])
    if not path.is_file() or file_sha256(path) != spec["artifact_hash"]:
        raise RuntimeError("Generated execution artifact is missing or failed SHA-256 verification.")
    return path
