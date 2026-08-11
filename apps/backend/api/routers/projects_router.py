from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Response, status

from api.auth import AuthUser, get_current_user
from api.models import ProjectRequest
from api.repositories.project_repository import ProjectRepository

router = APIRouter(prefix="/projects", tags=["Projects"])
repository = ProjectRepository()


def _payload(
    request: ProjectRequest,
    owner_email: str,
    current: dict[str, Any] | None = None,
) -> dict[str, Any]:
    data = request.model_dump()
    data["name"] = data["name"].strip()
    data["description"] = data["description"].strip()
    data["target"] = data["target"].strip().title()
    data["status"] = data["status"].strip().upper()
    data["connection_type"] = data["connection_type"].strip().lower()
    data["execution_engine"] = str(data.get("execution_engine") or "native").strip().lower()
    data["dbt_deployment_mode"] = str(
        data.get("dbt_deployment_mode") or "generate_only"
    ).strip().lower()
    data["owner_email"] = owner_email.lower()
    if not data["name"] or not data["description"]:
        raise HTTPException(status_code=400, detail="Project name and description are required")
    if data["target"] not in {"Databricks", "Snowflake", "Fabric"}:
        raise HTTPException(status_code=400, detail="Unsupported target warehouse")
    if data["status"] not in {"ACTIVE", "ARCHIVED"}:
        raise HTTPException(status_code=400, detail="Unsupported project status")
    if data["connection_type"] not in {"database", "data_lake"}:
        raise HTTPException(status_code=400, detail="Source type must be database or data_lake")
    snowflake_database_target = (
        data["target"] == "Snowflake" and data["connection_type"] == "database"
    )
    if not snowflake_database_target:
        data["execution_engine"] = "native"
        data["dbt_deployment_mode"] = "generate_only"
        data["dbt_target_name"] = None
        data["dbt_threads"] = None
        data["dbt_command_timeout_secs"] = None
        data["force_dbt_deploy"] = False
        data["dbt_project_object_name"] = None
    elif data["execution_engine"] == "dbt":
        from services.dbt_snowflake_runtime import dbt_project_object_name

        data["dbt_deployment_mode"] = "generate_and_deploy"
        data["dbt_project_object_name"] = (
            (current or {}).get("dbt_project_object_name")
            or dbt_project_object_name(data["name"])
        )
    else:
        data["dbt_deployment_mode"] = "generate_only"
        data["dbt_target_name"] = None
        data["dbt_threads"] = None
        data["dbt_command_timeout_secs"] = None
        data["force_dbt_deploy"] = False
        data["dbt_project_object_name"] = None
    return data


def _owned_project(project_id: str, user: AuthUser) -> dict[str, Any]:
    project = repository.find(project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    if user.user_type != "Admin" and project["owner_email"].lower() != user.email.lower():
        raise HTTPException(status_code=403, detail="Project access denied")
    return project


@router.get("")
def list_projects(user: AuthUser = Depends(get_current_user)) -> list[dict[str, Any]]:
    if user.user_type == "Admin":
        return repository.list_projects()
    return repository.list_projects(owner_email=user.email)


@router.get("/{project_id}")
def get_project(project_id: str, user: AuthUser = Depends(get_current_user)) -> dict[str, Any]:
    return _owned_project(project_id, user)


@router.post("", status_code=201)
def create_project(request: ProjectRequest, user: AuthUser = Depends(get_current_user)) -> dict[str, Any]:
    return repository.create(_payload(request, user.email))


@router.put("/{project_id}")
def update_project(project_id: str, request: ProjectRequest, user: AuthUser = Depends(get_current_user)) -> dict[str, Any]:
    current = _owned_project(project_id, user)
    project = repository.update(project_id, _payload(request, current["owner_email"], current))
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    return project


@router.delete("/{project_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_project(project_id: str, user: AuthUser = Depends(get_current_user)) -> Response:
    _owned_project(project_id, user)
    if not repository.delete(project_id):
        raise HTTPException(status_code=404, detail="Project not found")
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get("/{project_id}/runs")
def project_runs(project_id: str, user: AuthUser = Depends(get_current_user)) -> list[dict[str, Any]]:
    _owned_project(project_id, user)
    from services.pipeline_runtime import list_runs, load_checkpoint_fields_many

    indexed_runs = list_runs(limit=200, project_id=project_id)
    run_ids = [str(item.get("run_id") or "") for item in indexed_runs if item.get("run_id")]
    checkpoints = load_checkpoint_fields_many(
        run_ids,
        "project_id",
        "brd_filename",
        "status",
        "created_at",
        "started_at",
        "updated_at",
        "completed_at",
        "error",
        "error_message",
        "failed_background_stage",
    )

    return [
        {**item, **checkpoints.get(run_id, {}), "run_id": run_id}
        for item in indexed_runs
        if (run_id := str(item.get("run_id") or ""))
        and str(checkpoints.get(run_id, {}).get("project_id") or "") == project_id
    ]
