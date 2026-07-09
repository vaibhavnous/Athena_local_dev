from __future__ import annotations

from pathlib import Path
import uuid

from utilis.generated_code_paths import generated_code_dir, generated_code_root


def test_generated_code_root_uses_workspace_level_dir_when_cwd_is_backend(monkeypatch):
    backend_dir = Path(__file__).resolve().parents[1]
    monkeypatch.chdir(backend_dir)
    monkeypatch.delenv("ATHENA_GENERATED_CODE_DIR", raising=False)

    assert generated_code_root() == backend_dir.parent / "generated_code"
    assert generated_code_dir("snowflake", "bronze") == backend_dir.parent / "generated_code" / "snowflake" / "bronze"


def test_generated_code_root_honors_explicit_env_override(monkeypatch):
    custom_root = Path.cwd() / ".tmp-tests" / f"generated_code_override_{uuid.uuid4().hex}" / "artifacts"
    custom_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("ATHENA_GENERATED_CODE_DIR", str(custom_root))

    assert generated_code_root() == custom_root.resolve()
    assert generated_code_dir("gold") == custom_root.resolve() / "gold"
