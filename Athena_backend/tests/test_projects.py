from fastapi.testclient import TestClient
import pytest

from api.auth import AuthUser, get_current_user
from api.main import app
from api.models import ProjectRequest
from api.routers import projects_router


client = TestClient(app)


def test_project_create_uses_authenticated_owner(monkeypatch):
    captured = {}
    previous_override = app.dependency_overrides.get(get_current_user)
    app.dependency_overrides[get_current_user] = lambda: AuthUser(
        uid="owner", username="Owner", email="owner@astra.local", userType="Client"
    )
    monkeypatch.setattr(
        projects_router.repository,
        "create",
        lambda project: captured.update(project) or {"id": "project-1", **project},
    )

    response = client.post(
        "/projects",
        json={
            "name": "Claims",
            "description": "Claims pipeline",
            "target": "Snowflake",
            "connection_type": "database",
            "db_type": "azure_sql",
            "database_name": "insurance",
            "execution_engine": "dbt",
            "dbt_deployment_mode": "generate_only",
        },
    )

    assert response.status_code == 201
    assert captured["owner_email"] == "owner@astra.local"
    assert captured["execution_engine"] == "dbt"
    assert captured["dbt_deployment_mode"] == "generate_only"
    assert response.json()["id"] == "project-1"
    if previous_override:
        app.dependency_overrides[get_current_user] = previous_override
    else:
        app.dependency_overrides.pop(get_current_user, None)


def test_project_list_is_scoped_to_client_owner(monkeypatch):
    captured = {}
    previous_override = app.dependency_overrides.get(get_current_user)
    app.dependency_overrides[get_current_user] = lambda: AuthUser(
        uid="owner", username="Owner", email="owner@astra.local", userType="Client"
    )
    monkeypatch.setattr(
        projects_router.repository,
        "list_projects",
        lambda owner_email=None: captured.update({"owner_email": owner_email}) or [],
    )

    response = client.get("/projects")

    assert response.status_code == 200
    assert captured["owner_email"] == "owner@astra.local"
    if previous_override:
        app.dependency_overrides[get_current_user] = previous_override
    else:
        app.dependency_overrides.pop(get_current_user, None)


def test_project_run_keeps_project_id_in_checkpoint(monkeypatch):
    from api.routers import pipeline_router

    saved = {}
    monkeypatch.setattr("services.pipeline_runtime.load_checkpoint_state", lambda run_id: None)
    monkeypatch.setattr("services.pipeline_runtime.save_checkpoint_state", lambda run_id, state: saved.update(state))

    pipeline_router._seed_run_checkpoint(
        "run-1",
        pipeline_router.PipelineRunRequest(
            project_id="project-1", brd_text="requirements", source="database", use_domain_kb=True
        ),
    )

    assert saved["project_id"] == "project-1"
    assert saved["use_domain_kb"] is True


def test_project_request_rejects_dbt_for_data_lake_project():
    with pytest.raises(ValueError, match="database projects"):
        ProjectRequest(
            name="Claims",
            description="Claims pipeline",
            target="Snowflake",
            connection_type="data_lake",
            integration_type="SFTP",
            execution_engine="dbt",
        )


def test_project_repository_splits_dbt_column_migration_batches(monkeypatch):
    class Cursor:
        def __init__(self):
            self.queries = []

        def execute(self, query, *parameters):
            self.queries.append(str(query))

    class Connection:
        def __init__(self):
            self.cursor_instance = Cursor()

        def cursor(self):
            return self.cursor_instance

        def commit(self):
            pass

        def close(self):
            pass

    connection = Connection()
    monkeypatch.setattr(projects_router.repository, "_ready", False)
    monkeypatch.setattr(
        "api.repositories.project_repository.get_pipeline_connection",
        lambda: connection,
    )

    projects_router.repository.ensure_table()

    migration_batches = [query for query in connection.cursor_instance.queries if "COL_LENGTH" in query]
    constraint_batches = [query for query in connection.cursor_instance.queries if "sys.check_constraints" in query]
    assert len(migration_batches) == 6
    assert len(constraint_batches) == 2
    assert all("CHECK (execution_engine" not in query for query in migration_batches)
