from fastapi.testclient import TestClient

from api.auth import AuthUser, get_current_user
from api.main import app
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
        },
    )

    assert response.status_code == 201
    assert captured["owner_email"] == "owner@astra.local"
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
