from utilis import db
from utilis.db import _sql_error_hint


def test_sql_error_hint_classifies_odbc_tls_failure_before_network_code():
    message = (
        "('08001', '[08001] [Microsoft][ODBC Driver 18 for SQL Server]"
        "Encryption not supported on the client. SSL Provider: "
        "No credentials are available in the security package')"
    )

    hint = _sql_error_hint(
        Exception(message),
        role="pipeline",
        host="dataedge.database.windows.net",
        port=1433,
        database_name="AdventureWorks2019",
    )

    assert hint.startswith("SQL TLS/client encryption failed")
    assert "Likely connectivity issue" not in hint


def test_driver_candidates_skip_uninstalled_fallback_driver(monkeypatch):
    class FakePyodbc:
        @staticmethod
        def drivers():
            return ["ODBC Driver 18 for SQL Server"]

    monkeypatch.setattr(db, "_get_pyodbc", lambda: FakePyodbc)
    monkeypatch.setitem(db.config["azure_sql"], "driver", "ODBC Driver 18 for SQL Server")

    assert db._driver_candidates() == ["ODBC Driver 18 for SQL Server"]


def test_source_jdbc_url_does_not_embed_credentials(monkeypatch):
    monkeypatch.setitem(db.config["azure_sql"], "source_host", "example.database.windows.net")
    monkeypatch.setitem(db.config["azure_sql"], "port", 1433)
    monkeypatch.setitem(db.config["azure_sql"], "source_username", "user")
    monkeypatch.setitem(db.config["azure_sql"], "source_password", "secret")

    url = db.build_source_jdbc_url("insurance")

    assert "databaseName=insurance" in url
    assert "user=" not in url.lower()
    assert "password=" not in url.lower()
    assert "pwd=" not in url.lower()


def test_source_jdbc_url_honors_trust_server_certificate(monkeypatch):
    monkeypatch.setitem(db.config["azure_sql"], "source_host", "example.database.windows.net")
    monkeypatch.setitem(db.config["azure_sql"], "port", 1433)
    monkeypatch.setitem(db.config["azure_sql"], "trust_server_certificate", "yes")

    url = db.build_source_jdbc_url("insurance")

    assert "trustServerCertificate=true" in url


def test_bad_env_helpers_fall_back(monkeypatch):
    monkeypatch.setenv("ATHENA_TEST_BAD_INT", "nope")

    assert db._env_int("ATHENA_TEST_BAD_INT", 7) == 7
    assert db._sql_identifier("dbo;DROP TABLE x", "dbo", env_name="TEST_SCHEMA") == "dbo"
