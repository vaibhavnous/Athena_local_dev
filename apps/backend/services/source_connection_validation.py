from __future__ import annotations

import json
from typing import Any, Mapping
from urllib.parse import quote


def validate_deployment_database_binding(
    connection: Mapping[str, Any], *, target_platform: str = "snowflake"
) -> None:
    """Fail when active metadata no longer describes this deployment's source endpoint."""
    from utilis.db import config

    source = config["azure_sql"]
    expected = {
        "host_name": str(source.get("source_host") or "").strip().casefold(),
        "port": int(source.get("port") or 0),
        "database_name": str(source.get("source_database") or "").strip().casefold(),
    }
    actual = {
        "host_name": str(connection.get("host_name") or "").strip().casefold(),
        "port": int(connection.get("port") or 0),
        "database_name": str(connection.get("database_name") or "").strip().casefold(),
    }
    if actual != expected:
        raise ValueError("The JDBC metadata endpoint does not match this deployment's configured source database.")
    if str(connection.get("auth_type") or "").upper() != "BASIC":
        raise ValueError("The existing database source utility supports BASIC authentication only.")

    references = json.loads(str(connection.get("secrets_json") or "{}"))
    expected_keys = {"username": "AZURE_SQL_SOURCE_USERNAME", "password": "AZURE_SQL_SOURCE_PASSWORD"}
    for logical_name, environment_key in expected_keys.items():
        reference = references.get(logical_name) or {}
        scope = str(reference.get("scope") or "").strip()
        key = str(reference.get("key") or "").strip()
        if str(target_platform).lower() == "databricks":
            if not scope or scope.upper() == "DEPLOYMENT_ENV" or not key:
                raise ValueError(f"{logical_name} must reference a Databricks secret scope and key.")
        elif scope.upper() != "DEPLOYMENT_ENV" or key != environment_key:
            raise ValueError(
                f"{logical_name} must reference DEPLOYMENT_ENV/{environment_key} for the current database connector."
            )


def _validate_databricks_secret_keys(connection: Mapping[str, Any]) -> None:
    from services.databricks_runtime import _request_json

    references = json.loads(str(connection.get("secrets_json") or "{}"))
    by_scope: dict[str, set[str]] = {}
    for reference in references.values():
        by_scope.setdefault(str(reference["scope"]), set()).add(str(reference["key"]))
    for scope, required_keys in by_scope.items():
        payload = _request_json("GET", f"/api/2.0/secrets/list?scope={quote(scope, safe='')}")
        available = {str(item.get("key") or "") for item in payload.get("secrets") or []}
        missing = sorted(required_keys - available)
        if missing:
            raise RuntimeError(f"Databricks secret scope {scope!r} is missing configured keys: {', '.join(missing)}")


def validate_deployment_database_connection(
    connection: Mapping[str, Any], *, target_platform: str = "snowflake"
) -> None:
    """Validate binding and prove that the deployment source can be queried."""
    from utilis.db import get_client_connection

    validate_deployment_database_binding(connection, target_platform=target_platform)
    if str(target_platform).lower() == "databricks":
        _validate_databricks_secret_keys(connection)
    connection_handle = None
    try:
        connection_handle = get_client_connection(str(connection.get("database_name") or ""))
        cursor = connection_handle.cursor()
        cursor.execute("SELECT 1")
        if not cursor.fetchone():
            raise RuntimeError("Source connectivity validation returned no result.")
    except Exception:
        raise RuntimeError("Source database connectivity validation failed.") from None
    finally:
        if connection_handle is not None:
            connection_handle.close()
