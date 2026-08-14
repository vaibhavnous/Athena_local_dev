from __future__ import annotations

import logging
import os
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv


logger = logging.getLogger(__name__)

KEY_VAULT_SECRET_ENV_MAPPING = {
    "az-astra-data-username": "AZURE_SQL_USERNAME",
    "az-astra-data-pwd": "AZURE_SQL_PASSWORD",
    "az-src-insurance-username": "AZURE_SQL_SOURCE_USERNAME",
    "az-src-insurance-pwd": "AZURE_SQL_SOURCE_PASSWORD",
    "AZURE-OPENAI-API-KEY": "AZURE_OPENAI_API_KEY",
    "AZURE-OPENAI-EMBEDDING-API-KEY": "AZURE_OPENAI_EMBEDDING_API_KEY",
    "DATABRICKS-TOKEN": "DATABRICKS_TOKEN",
    "PINECONE-API-KEY": "PINECONE_API_KEY",
    "SNOWFLAKE-ACCOUNT": "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE-PASSWORD": "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE-USER": "SNOWFLAKE_USER",
}

_key_vault_loaded = False


def _is_enabled(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _validate_vault_url(vault_url: str) -> str:
    parsed = urlparse(vault_url)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise RuntimeError("AZURE_KEY_VAULT_URL must be a valid HTTPS URL")
    if parsed.path not in {"", "/"} or parsed.params or parsed.query or parsed.fragment:
        raise RuntimeError("AZURE_KEY_VAULT_URL must contain only the vault origin")
    return vault_url.rstrip("/")


def _hydrate_secret_environment(secret_client) -> tuple[str, ...]:
    pending = {
        secret_name: env_name
        for secret_name, env_name in KEY_VAULT_SECRET_ENV_MAPPING.items()
        if not os.environ.get(env_name)
    }
    loaded: dict[str, str] = {}

    for secret_name, env_name in pending.items():
        try:
            value = secret_client.get_secret(secret_name).value
        except Exception as exc:
            raise RuntimeError(
                f"Unable to load Key Vault secret '{secret_name}' for '{env_name}'"
            ) from exc
        if not value:
            raise RuntimeError(f"Key Vault secret '{secret_name}' is empty")
        loaded[env_name] = value

    # Apply only after every read succeeds so startup cannot use a partial secret set.
    os.environ.update(loaded)
    return tuple(loaded)


def _load_key_vault_environment() -> None:
    global _key_vault_loaded
    if _key_vault_loaded:
        return

    if not _is_enabled(os.getenv("ATHENA_KEY_VAULT_ENABLED")):
        _key_vault_loaded = True
        return

    configured_vault_url = str(os.getenv("AZURE_KEY_VAULT_URL") or "").strip()
    if not configured_vault_url:
        raise RuntimeError("AZURE_KEY_VAULT_URL is required when ATHENA_KEY_VAULT_ENABLED=true")
    vault_url = _validate_vault_url(configured_vault_url)

    missing_env_names = [
        env_name for env_name in KEY_VAULT_SECRET_ENV_MAPPING.values() if not os.environ.get(env_name)
    ]
    if not missing_env_names:
        _key_vault_loaded = True
        return

    tenant_id = str(os.getenv("AZURE_TENANT_ID") or "").strip()
    client_id = str(os.getenv("AZURE_CLIENT_ID") or "").strip()
    client_secret = str(os.getenv("AZURE_CLIENT_SECRET") or "").strip()
    service_principal_values = (tenant_id, client_id, client_secret)
    if any(service_principal_values) and not all(service_principal_values):
        raise RuntimeError(
            "AZURE_TENANT_ID, AZURE_CLIENT_ID, and AZURE_CLIENT_SECRET must all be set "
            "for service-principal authentication"
        )

    from azure.identity import ClientSecretCredential, DefaultAzureCredential
    from azure.keyvault.secrets import SecretClient

    credential = (
        ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
        if all(service_principal_values)
        else DefaultAzureCredential(exclude_interactive_browser_credential=True)
    )
    try:
        with SecretClient(vault_url=vault_url, credential=credential) as secret_client:
            loaded_env_names = _hydrate_secret_environment(secret_client)
    finally:
        credential.close()

    _key_vault_loaded = True
    logger.info("Loaded %d application secrets from Azure Key Vault", len(loaded_env_names))


def load_backend_env() -> None:
    """Load local settings and approved Key Vault secrets before clients are created."""
    backend_root = Path(__file__).resolve().parents[1]

    for env_file in (backend_root / ".env", backend_root / ".myenv"):
        load_dotenv(env_file, override=False)

    load_dotenv(override=False)
    _load_key_vault_environment()
