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
            "execution_engine": "dbt",
            "dbt_target_name": "astra_snowflake",
            "dbt_threads": 6,
            "dbt_command_timeout_secs": 900,
        },
    )

    assert response.status_code == 201
    assert captured["owner_email"] == "owner@astra.local"
    assert captured["execution_engine"] == "dbt"
    assert captured["dbt_deployment_mode"] == "generate_and_deploy"
    assert captured["dbt_project_object_name"] == "CLAIMS_DBT"
    assert captured["dbt_target_name"] == "astra_snowflake"
    assert captured["dbt_threads"] == 6
    assert captured["dbt_command_timeout_secs"] == 900
    assert captured["force_dbt_deploy"] is False
    assert response.json()["id"] == "project-1"
    if previous_override:
        app.dependency_overrides[get_current_user] = previous_override
    else:
        app.dependency_overrides.pop(get_current_user, None)


def test_native_project_update_does_not_create_dbt_object_name():
    from api.routers.projects_router import _payload

    payload = _payload(
        projects_router.ProjectRequest(
            name="Renamed Claims",
            description="Claims pipeline",
            target="Snowflake",
            connection_type="database",
            database_name="insurance",
        ),
        "owner@astra.local",
        {
            "dbt_project_object_name": None,
        },
    )

    assert payload["dbt_project_object_name"] is None


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


def test_project_delete_returns_empty_204(monkeypatch):
    previous_override = app.dependency_overrides.get(get_current_user)
    app.dependency_overrides[get_current_user] = lambda: AuthUser(
        uid="owner", username="Owner", email="owner@astra.local", userType="Client"
    )
    monkeypatch.setattr(
        projects_router.repository,
        "find",
        lambda project_id: {"id": project_id, "owner_email": "owner@astra.local"},
    )
    monkeypatch.setattr(projects_router.repository, "delete", lambda project_id: True)

    response = client.delete("/projects/project-1")

    assert response.status_code == 204
    assert response.content == b""
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
            project_id="project-1",
            dbt_project_object_name="CLAIMS_DBT",
            brd_text="requirements",
            source="database",
            target_warehouse="snowflake",
            execution_engine="dbt",
            dbt_target_name="astra_snowflake",
            dbt_threads=6,
            dbt_command_timeout_secs=900,
            use_domain_kb=True,
        ),
    )

    assert saved["project_id"] == "project-1"
    assert saved["dbt_project_object_name"] == "CLAIMS_DBT"
    assert saved["execution_engine"] == "dbt"
    assert saved["dbt_deployment_mode"] == "generate_only"
    assert saved["dbt_target_name"] == "astra_snowflake"
    assert saved["dbt_threads"] == 6
    assert saved["dbt_command_timeout_secs"] == 900
    assert saved["force_dbt_deploy"] is False
    assert saved["use_domain_kb"] is True

    saved.clear()
    pipeline_router._seed_run_checkpoint(
        "run-native",
        pipeline_router.PipelineRunRequest(
            project_id="project-native",
            brd_text="requirements",
            source="database",
            target_warehouse="snowflake",
            execution_engine="native",
        ),
    )

    assert saved["project_id"] == "project-native"
    assert saved["execution_engine"] == "native"
    assert saved["dbt_deployment_mode"] == "generate_only"
    assert saved["dbt_project_object_name"] is None


def test_project_execution_config_is_server_authoritative():
    from api.routers import pipeline_router

    stale_payload = pipeline_router.PipelineRunRequest(
        project_id="project-dbt",
        brd_text="requirements",
        source="sftp",
        target_warehouse="databricks",
    )
    dbt_payload = pipeline_router._with_project_execution_config(
        stale_payload,
        {
            "id": "project-dbt",
            "target": "Snowflake",
            "connection_type": "database",
            "database_name": "insurance",
            "db_type": "azure_sql",
            "execution_engine": "dbt",
            "dbt_project_object_name": "CLAIMS_DBT",
            "dbt_target_name": "astra_snowflake",
            "dbt_threads": 6,
            "dbt_command_timeout_secs": 900,
        },
    )

    assert dbt_payload.source == "database"
    assert dbt_payload.target_warehouse == "snowflake"
    assert dbt_payload.database_name == "insurance"
    assert dbt_payload.database_type == "azure_sql"
    assert dbt_payload.execution_engine == "dbt"
    assert dbt_payload.dbt_deployment_mode == "generate_and_deploy"
    assert dbt_payload.dbt_project_object_name == "CLAIMS_DBT"
    assert dbt_payload.dbt_target_name == "astra_snowflake"
    assert dbt_payload.dbt_threads == 6
    assert dbt_payload.dbt_command_timeout_secs == 900
    assert dbt_payload.force_dbt_deploy is False

    adls_dbt_payload = pipeline_router._with_project_execution_config(
        pipeline_router.PipelineRunRequest(
            project_id="project-adls-dbt",
            brd_text="requirements",
            source="adls_gen2",
            target_warehouse="databricks",
        ),
        {
            "id": "project-adls-dbt",
            "target": "Snowflake",
            "connection_type": "data_lake",
            "integration_type": "ADLS",
            "execution_engine": "dbt",
            "dbt_project_object_name": "ADLS_DBT",
        },
    )

    assert adls_dbt_payload.source == "adls_gen2"
    assert adls_dbt_payload.target_warehouse == "snowflake"
    assert adls_dbt_payload.execution_engine == "dbt"
    assert adls_dbt_payload.dbt_deployment_mode == "generate_and_deploy"
    assert adls_dbt_payload.dbt_project_object_name == "ADLS_DBT"

    native_payload = pipeline_router._with_project_execution_config(
        pipeline_router.PipelineRunRequest(
            project_id="project-native",
            brd_text="requirements",
            source="database",
            target_warehouse="snowflake",
            execution_engine="dbt",
            dbt_target_name="untrusted",
        ),
        {
            "id": "project-native",
            "target": "Snowflake",
            "connection_type": "database",
            "execution_engine": "native",
        },
    )

    assert native_payload.target_warehouse == "snowflake"
    assert native_payload.source == "database"
    assert native_payload.execution_engine == "native"
    assert native_payload.dbt_deployment_mode == "generate_only"
    assert native_payload.dbt_project_object_name is None
    assert native_payload.dbt_target_name is None


def test_snowflake_database_projects_preserve_selected_engine():
    from api.repositories.project_repository import ProjectRepository

    native_project = ProjectRepository._with_execution_defaults(
        {
            "target": "Snowflake",
            "connection_type": "database",
            "execution_engine": "native",
            "dbt_deployment_mode": "generate_only",
        }
    )
    dbt_project = ProjectRepository._with_execution_defaults(
        {
            "target": "Snowflake",
            "connection_type": "database",
            "execution_engine": "dbt",
            "dbt_deployment_mode": "generate_only",
        }
    )

    assert native_project["execution_engine"] == "native"
    assert native_project["dbt_deployment_mode"] == "generate_only"
    assert dbt_project["execution_engine"] == "dbt"
    assert dbt_project["dbt_deployment_mode"] == "generate_and_deploy"


def test_snowflake_adls_projects_preserve_selected_engine():
    from api.repositories.project_repository import ProjectRepository

    project = ProjectRepository._with_execution_defaults(
        {
            "target": "Snowflake",
            "connection_type": "data_lake",
            "integration_type": "ADLS",
            "execution_engine": "dbt",
        }
    )

    assert project["execution_engine"] == "dbt"
    assert project["dbt_deployment_mode"] == "generate_and_deploy"
