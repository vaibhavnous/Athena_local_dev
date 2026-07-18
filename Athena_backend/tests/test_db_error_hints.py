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
