from utilis import db
from utilis.db import _sql_error_hint


def test_artifact_storage_keys_isolate_runs_without_changing_legacy_keys():
    legacy = db.artifact_storage_fingerprint("same-input", "TABLE_NOMINATIONS")
    first_run = db.artifact_storage_fingerprint(
        "same-input",
        "TABLE_NOMINATIONS",
        run_id="run-1",
    )
    second_run = db.artifact_storage_fingerprint(
        "same-input",
        "TABLE_NOMINATIONS",
        run_id="run-2",
    )

    assert len({legacy, first_run, second_run}) == 3
    assert legacy == db.artifact_storage_fingerprint("same-input", "TABLE_NOMINATIONS")


def test_ai_store_writer_uses_run_scoped_atomic_upsert(monkeypatch):
    executed = {}

    class Cursor:
        def execute(self, sql, *params):
            executed["sql"] = sql
            executed["params"] = params

    class Connection:
        def cursor(self):
            return Cursor()

        def commit(self):
            executed["committed"] = True

        def close(self):
            executed["closed"] = True

    monkeypatch.setattr(db, "get_pipeline_connection", Connection)

    db.ai_store_db_writer(
        run_id="run-isolated",
        stage="Table Nomination",
        artifact_type="TABLE_NOMINATIONS",
        payload={"fingerprint": "same-input", "nominations": []},
        schema_version="v1",
        prompt_version="v1",
        faithfulness_status="PASSED",
    )

    expected_key = db.artifact_storage_fingerprint(
        "same-input",
        "TABLE_NOMINATIONS",
        run_id="run-isolated",
    )
    assert executed["params"][0] == expected_key
    assert "MERGE" in executed["sql"]
    assert "HOLDLOCK" in executed["sql"]
    matched_update = executed["sql"].split("WHEN MATCHED", 1)[1].split("WHEN NOT MATCHED", 1)[0]
    assert "run_id" not in matched_update
    assert executed["committed"] is True
    assert executed["closed"] is True


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
