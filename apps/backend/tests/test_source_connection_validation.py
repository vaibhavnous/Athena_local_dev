from __future__ import annotations

import json

import pytest

from services.source_connection_validation import validate_deployment_database_connection


class FakeCursor:
    def __init__(self) -> None:
        self.sql = None

    def execute(self, sql: str) -> None:
        self.sql = sql

    def fetchone(self):
        return (1,)


class FakeConnection:
    def __init__(self) -> None:
        self.cursor_value = FakeCursor()
        self.closed = False

    def cursor(self) -> FakeCursor:
        return self.cursor_value

    def close(self) -> None:
        self.closed = True


def _metadata_connection() -> dict:
    return {
        "host_name": "source.database.windows.net",
        "port": 1433,
        "database_name": "ClaimsDB",
        "auth_type": "BASIC",
        "secrets_json": json.dumps(
            {
                "username": {"scope": "DEPLOYMENT_ENV", "key": "AZURE_SQL_SOURCE_USERNAME"},
                "password": {"scope": "DEPLOYMENT_ENV", "key": "AZURE_SQL_SOURCE_PASSWORD"},
            }
        ),
    }


def test_database_connection_validation_reuses_existing_source_client(monkeypatch: pytest.MonkeyPatch) -> None:
    from utilis import db

    handle = FakeConnection()
    monkeypatch.setitem(
        db.config,
        "azure_sql",
        {
            **db.config["azure_sql"],
            "source_host": "source.database.windows.net",
            "port": 1433,
            "source_database": "ClaimsDB",
        },
    )
    monkeypatch.setattr(db, "get_client_connection", lambda database: handle)

    validate_deployment_database_connection(_metadata_connection())

    assert handle.cursor_value.sql == "SELECT 1"
    assert handle.closed is True


def test_database_connection_validation_rejects_metadata_endpoint_drift(monkeypatch: pytest.MonkeyPatch) -> None:
    from utilis import db

    monkeypatch.setitem(
        db.config,
        "azure_sql",
        {
            **db.config["azure_sql"],
            "source_host": "source.database.windows.net",
            "port": 1433,
            "source_database": "ClaimsDB",
        },
    )
    connection = _metadata_connection()
    connection["database_name"] = "AnotherDatabase"

    with pytest.raises(ValueError, match="does not match"):
        validate_deployment_database_connection(connection)


def test_databricks_connection_requires_and_validates_secret_scope_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    from services import databricks_runtime
    from utilis import db

    handle = FakeConnection()
    monkeypatch.setitem(
        db.config,
        "azure_sql",
        {
            **db.config["azure_sql"],
            "source_host": "source.database.windows.net",
            "port": 1433,
            "source_database": "ClaimsDB",
        },
    )
    monkeypatch.setattr(db, "get_client_connection", lambda _database: handle)
    monkeypatch.setattr(
        databricks_runtime,
        "_request_json",
        lambda method, path: {
            "secrets": [{"key": "claims-db-username"}, {"key": "claims-db-password"}]
        },
    )
    connection = _metadata_connection()
    connection["secrets_json"] = json.dumps(
        {
            "username": {"scope": "astra-qa-source-secrets", "key": "claims-db-username"},
            "password": {"scope": "astra-qa-source-secrets", "key": "claims-db-password"},
        }
    )

    validate_deployment_database_connection(connection, target_platform="databricks")

    assert handle.cursor_value.sql == "SELECT 1"
