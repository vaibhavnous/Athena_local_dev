import os
import socket
import uuid
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from fastapi import APIRouter, HTTPException

router = APIRouter()

SENSITIVE_KEY_NAMES = {"password", "secret", "apikey", "api_key", "token", "access_token", "refresh_token", "client_secret"}


def _is_sensitive_key(key: str) -> bool:
    compact = "".join(ch for ch in str(key).lower() if ch.isalnum())
    return compact in {name.replace("_", "") for name in SENSITIVE_KEY_NAMES} or compact.endswith(("password", "secret", "token"))


def _redact_sensitive(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: "" if _is_sensitive_key(key) else _redact_sensitive(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_redact_sensitive(item) for item in value]
    return value


def _field(data: Dict[str, Any], *keys: str, default: str = "") -> str:
    for key in keys:
        value = data.get(key)
        if value is not None:
            return str(value).strip()
    return default


def _coerce_port(value: str, default: int = 1433) -> int:
    raw = str(value or default).strip()
    try:
        port = int(raw)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Port must be a number") from exc
    if port < 1 or port > 65535:
        raise HTTPException(status_code=400, detail="Port must be between 1 and 65535")
    return port


def _configuration_id(data: Dict[str, Any]) -> Any:
    config_id = data.get("id")
    if isinstance(config_id, (str, int)) and str(config_id).strip():
        return config_id
    return str(uuid.uuid4())


def _load_env_if_available() -> None:
    try:
        from utilis.env import load_backend_env

        load_backend_env()
    except Exception:
        return


def _configured_source_matches(host: str, port: int, database_name: str) -> bool:
    _load_env_if_available()

    configured_host = str(os.getenv("AZURE_SQL_SOURCE_HOST") or os.getenv("AZURE_SQL_HOST") or "").strip().lower()
    configured_database = str(os.getenv("AZURE_SQL_SOURCE_DATABASE") or "").strip().lower()
    has_credentials = bool(
        str(os.getenv("AZURE_SQL_SOURCE_USERNAME") or "").strip()
        and str(os.getenv("AZURE_SQL_SOURCE_PASSWORD") or "").strip()
    )
    if not has_credentials or not configured_host:
        return False
    try:
        configured_port = _coerce_port(os.getenv("AZURE_SQL_PORT", "1433"))
    except HTTPException:
        return False
    return (
        host.strip().lower() == configured_host
        and int(port) == configured_port
        and (not database_name or database_name.strip().lower() == configured_database)
    )


def _tcp_probe(host: str, port: int) -> Optional[Exception]:
    timeout_seconds = max(0.1, min(float(os.getenv("ATHENA_SQL_TCP_PROBE_TIMEOUT_SECONDS", "2")), 2.0))
    try:
        with socket.create_connection((host, port), timeout=timeout_seconds):
            return None
    except OSError as exc:
        return exc


def _test_database_configuration(data: Dict[str, Any]) -> Dict[str, Any]:
    host = _field(data, "host")
    if not host:
        raise HTTPException(status_code=400, detail="Host is required")

    port = _coerce_port(_field(data, "port", default="1433"))
    database_name = _field(data, "database_name", "databaseName")
    db_type = _field(data, "db_type", "dbType", default="azure_sql").lower()

    if not _field(data, "username"):
        raise HTTPException(status_code=400, detail="Username is required")

    # ponytail: compatibility check is TCP-only for the env-configured Azure SQL source; add persisted-provider probes later.
    if db_type == "azure_sql" and _configured_source_matches(host, port, database_name):
        error = _tcp_probe(host, port)
        if error:
            raise HTTPException(status_code=503, detail=f"Configured Azure SQL endpoint is not reachable: {error}")
        return {"success": True, "message": "Configured Azure SQL endpoint is reachable."}

    return {
        "success": True,
        "message": "Configuration accepted; live connection test is not enabled for this source.",
    }


def _test_data_lake_configuration(data: Dict[str, Any]) -> Dict[str, Any]:
    integration_type = _field(data, "integration_type", "integrationType", default="SFTP").upper()
    if integration_type == "API":
        base_url = _field(data, "base_url", "baseUrl")
        parsed = urlparse(base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise HTTPException(status_code=400, detail="Valid base URL is required")
    else:
        if not _field(data, "base_path", "basePath"):
            raise HTTPException(status_code=400, detail="Base path is required")
        if not _field(data, "directory_name", "directoryName"):
            raise HTTPException(status_code=400, detail="Directory name is required")

    return {
        "success": True,
        "message": "Configuration accepted; live data lake test is not enabled.",
    }


@router.get("/settings")
def settings() -> Dict[str, Any]:
    return {
        "provider": "azure_openai",
        "azure_deployment": os.getenv("AZURE_OPENAI_DEPLOYMENT"),
        "budget": 5.0,
        "maxKpis": 25,
        "devMode": os.getenv("DEV_MODE", "").lower() in {"1", "true", "yes", "on"},
    }


@router.put("/settings")
def save_settings(data: Dict[str, Any]) -> Dict[str, Any]:
    return data


@router.get("/configurations")
def configurations() -> List[Dict[str, Any]]:
    from utilis.db import config

    db_conf = config.get("azure_sql", {})
    return [
        {
            "id": "azure_sql_default",
            "name": "Default Azure SQL",
            "sourceType": "database",
            "dbType": "azure_sql",
            "host": db_conf.get("source_host"),
            "port": str(db_conf.get("port", 1433)),
            "databaseName": db_conf.get("source_database"),
            "schema": db_conf.get("source_schema"),
            "username": db_conf.get("source_username"),
            "driverClass": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "jdbcUrl": "",
        }
    ]


@router.post("/configurations")
def create_configuration(data: Dict[str, Any]) -> Dict[str, Any]:
    return {**_redact_sensitive(data), "id": _configuration_id(data)}


@router.post("/configurations/test")
def test_configuration(data: Dict[str, Any]) -> Dict[str, Any]:
    source_type = _field(data, "source_type", "sourceType", default="database").lower()
    if source_type == "data_lake":
        return _test_data_lake_configuration(data)
    return _test_database_configuration(data)


@router.put("/configurations/{config_id}")
def update_configuration(config_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
    return {**_redact_sensitive(data), "id": config_id}


@router.delete("/configurations/{config_id}")
def delete_configuration(config_id: str) -> Dict[str, Any]:
    return {"id": config_id, "deleted": True}
