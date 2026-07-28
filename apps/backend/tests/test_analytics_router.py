from api.auth import AuthUser
from api.routers import analytics_router


class Cursor:
    def __init__(self):
        self.query = ""
        self.parameters = ()

    def execute(self, query, *parameters):
        self.query = str(query)
        self.parameters = parameters

    def fetchall(self):
        return [("2026-07-27", 0.1, 0.2, 0.3, 42)]


class Connection:
    def __init__(self):
        self.cursor_instance = Cursor()
        self.closed = False

    def cursor(self):
        return self.cursor_instance

    def close(self):
        self.closed = True


def test_client_cost_analytics_is_scoped_to_checkpoint_owner(monkeypatch):
    connection = Connection()
    monkeypatch.setattr(analytics_router, "config", {"azure_sql": {"pipeline_schema": "metadata"}})
    monkeypatch.setattr(analytics_router, "get_connection", lambda: connection)

    result = analytics_router.analytics_cost(
        AuthUser(uid="client", username="Client", email="client@example.com", userType="Client")
    )

    assert "checkpoint.run_id = ai_store.run_id" in connection.cursor_instance.query
    assert "$.owner_email" in connection.cursor_instance.query
    assert connection.cursor_instance.parameters == ("client@example.com",)
    assert result[0]["totalCost"] == 0.3
    assert connection.closed is True
