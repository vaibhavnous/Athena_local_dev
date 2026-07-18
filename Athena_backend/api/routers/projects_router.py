from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from api.auth import AuthUser, get_current_user
from api.models import ProjectRequest
from api.repositories.project_repository import ProjectRepository

router = APIRouter(prefix="/projects", tags=["Projects"])
repository = ProjectRepository()


def _payload(request: ProjectRequest, owner_email: str) -> dict[str, Any]:
    data = request.model_dump()
    data["name"] = data["name"].strip()
    data["description"] = data["description"].strip()
    data["target"] = data["target"].strip().title()
    data["status"] = data["status"].strip().upper()
    data["connection_type"] = data["connection_type"].strip().lower()
    data["owner_email"] = owner_email.lower()
    if not data["name"] or not data["description"]:
        raise HTTPException(status_code=400, detail="Project name and description are required")
    if data["target"] not in {"Databricks", "Snowflake", "Fabric"}:
        raise HTTPException(status_code=400, detail="Unsupported target warehouse")
    if data["status"] not in {"ACTIVE", "ARCHIVED"}:
        raise HTTPException(status_code=400, detail="Unsupported project status")
    if data["connection_type"] not in {"database", "data_lake"}:
        raise HTTPException(status_code=400, detail="Source type must be database or data_lake")
    return data


def _owned_project(project_id: str, user: AuthUser) -> dict[str, Any]:
    project = repository.find(project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    if user.user_type != "Admin" and project["owner_email"].lower() != user.email.lower():
        raise HTTPException(status_code=403, detail="Project access denied")
    return project


@router.get("")
def list_projects(_: AuthUser = Depends(get_current_user)) -> list[dict[str, Any]]:
    return repository.list_projects()


@router.get("/{project_id}")
def get_project(project_id: str, user: AuthUser = Depends(get_current_user)) -> dict[str, Any]:
    return _owned_project(project_id, user)


@router.post("", status_code=201)
def create_project(request: ProjectRequest, user: AuthUser = Depends(get_current_user)) -> dict[str, Any]:
    return repository.create(_payload(request, user.email))


@router.put("/{project_id}")
def update_project(project_id: str, request: ProjectRequest, user: AuthUser = Depends(get_current_user)) -> dict[str, Any]:
    current = _owned_project(project_id, user)
    project = repository.update(project_id, _payload(request, current["owner_email"]))
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    return project


@router.delete("/{project_id}", status_code=204)
def delete_project(project_id: str, user: AuthUser = Depends(get_current_user)) -> None:
    _owned_project(project_id, user)
    if not repository.delete(project_id):
        raise HTTPException(status_code=404, detail="Project not found")


@router.get("/{project_id}/runs")
def project_runs(project_id: str, user: AuthUser = Depends(get_current_user)) -> list[dict[str, Any]]:
    _owned_project(project_id, user)
    from services.pipeline_runtime import list_runs, load_checkpoint_state

    matches = []
    for item in list_runs(limit=200):
        run_id = str(item.get("run_id") or "")
        checkpoint = load_checkpoint_state(run_id) or {}
        if str(checkpoint.get("project_id") or "") == project_id:
            matches.append({**item, **checkpoint, "run_id": run_id})
    return matches
