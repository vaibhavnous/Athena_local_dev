from __future__ import annotations

from types import SimpleNamespace

import pytest

from utilis import env


class FakeSecretClient:
    def __init__(self, values: dict[str, str], failure: Exception | None = None) -> None:
        self.values = values
        self.failure = failure
        self.requests: list[str] = []

    def get_secret(self, name: str):
        self.requests.append(name)
        if self.failure:
            raise self.failure
        return SimpleNamespace(value=self.values[name])


def test_key_vault_hydration_preserves_overrides_and_maps_secret_names(monkeypatch: pytest.MonkeyPatch) -> None:
    for env_name in env.KEY_VAULT_SECRET_ENV_MAPPING.values():
        monkeypatch.delenv(env_name, raising=False)
    monkeypatch.setenv("SNOWFLAKE_USER", "deployment-override")
    client = FakeSecretClient({name: f"value-for-{name}" for name in env.KEY_VAULT_SECRET_ENV_MAPPING})

    loaded = env._hydrate_secret_environment(client)

    assert "SNOWFLAKE_USER" not in loaded
    assert "SNOWFLAKE-USER" not in client.requests
    assert env.os.environ["SNOWFLAKE_USER"] == "deployment-override"
    assert env.os.environ["AZURE_SQL_USERNAME"] == "value-for-az-astra-data-username"


def test_key_vault_hydration_is_atomic_on_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    for env_name in env.KEY_VAULT_SECRET_ENV_MAPPING.values():
        monkeypatch.delenv(env_name, raising=False)

    with pytest.raises(RuntimeError, match="Unable to load Key Vault secret"):
        env._hydrate_secret_environment(FakeSecretClient({}, RuntimeError("denied")))

    assert not any(env.os.environ.get(name) for name in env.KEY_VAULT_SECRET_ENV_MAPPING.values())


def test_key_vault_url_rejects_non_https_origins() -> None:
    with pytest.raises(RuntimeError, match="valid HTTPS URL"):
        env._validate_vault_url("http://astra-dev.vault.azure.net/")
