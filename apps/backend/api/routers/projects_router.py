from __future__ import annotations

import os
import threading
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Response, status

from api.auth import AuthUser, get_current_user
from api.models import ProjectRequest
from api.repositories.project_repository import ProjectRepository
from utilis.logger import logger

router = APIRouter(prefix="/projects", tags=["Projects"])
repository = ProjectRepository()
PROJECT_RUNS_EXECUTOR = ThreadPoolExecutor(max_workers=max(1, int(os.getenv("ATHENA_PROJECT_RUN_WORKERS", "2"))))
PROJECT_RUNS_LOCK = threading.Lock()
PROJECT_RUNS_FUTURES: dict[str, Future] = {}
PROJECT_RUNS_CACHE: dict[str, list[dict[str, Any]]] = {}
PROJECT_RUNS_TIMEOUT_SECONDS = max(1, int(os.getenv("ATHENA_PROJECT_RUN_TIMEOUT_SECONDS", "12")))


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


def _load_project_runs(project_id: str) -> list[dict[str, Any]]:
    from services.pipeline_runtime import load_project_run_history

    return load_project_run_history(project_id, limit=200)


def _cached_project_runs(project_id: str) -> list[dict[str, Any]] | None:
    with PROJECT_RUNS_LOCK:
        cached = PROJECT_RUNS_CACHE.get(project_id)
    if not cached:
        return None
    return [dict(row) for row in cached]


def _project_runs_future(project_id: str) -> Future:
    with PROJECT_RUNS_LOCK:
        existing = PROJECT_RUNS_FUTURES.get(project_id)
        if existing and not existing.done():
            return existing
        future = PROJECT_RUNS_EXECUTOR.submit(_load_project_runs, project_id)
        PROJECT_RUNS_FUTURES[project_id] = future

    def store_result(done: Future) -> None:
        try:
            rows = done.result()
        except Exception:
            rows = None
        with PROJECT_RUNS_LOCK:
            if rows is not None:
                PROJECT_RUNS_CACHE[project_id] = [dict(row) for row in rows]
            if PROJECT_RUNS_FUTURES.get(project_id) is done:
                PROJECT_RUNS_FUTURES.pop(project_id, None)

    future.add_done_callback(store_result)
    return future


@router.get("/{project_id}/runs")
def project_runs(project_id: str, user: AuthUser = Depends(get_current_user)) -> list[dict[str, Any]]:
    _owned_project(project_id, user)
    future = _project_runs_future(project_id)
    try:
        return future.result(timeout=PROJECT_RUNS_TIMEOUT_SECONDS)
    except FutureTimeoutError:
        stale = _cached_project_runs(project_id)
        if stale is not None:
            logger.warning("Project run history refresh timed out; returning cached rows", extra={"project_id": project_id})
            return stale
        logger.warning("Project run history timed out", extra={"project_id": project_id})
        raise HTTPException(status_code=503, detail="Project run history is still loading. Please retry shortly.")
    except Exception:
        stale = _cached_project_runs(project_id)
        if stale is not None:
            logger.exception("Project run history refresh failed; returning cached rows", extra={"project_id": project_id})
            return stale
        logger.exception("Project run history failed", extra={"project_id": project_id})
        raise HTTPException(status_code=503, detail="Project run history is temporarily unavailable.")
