from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional

from services.metadata_repository import MetadataRepository, metadata_repository_for_target
from services.metadata_contracts import validate_jdbc_connection
from services.source_connection_validation import validate_deployment_database_binding


@dataclass(frozen=True)
class ValidatedMetadataSelection:
    repository: MetadataRepository
    source_system: Dict[str, Any]
    connection: Dict[str, Any]
    uses_environment_source: bool = False


def environment_source_fallback_enabled() -> bool:
    return str(os.getenv("ATHENA_METADATA_ALLOW_ENV_SOURCE_FALLBACK") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def metadata_source_options(
    *, platform: str, environment: str, project_id: Optional[str] = None
) -> List[Dict[str, Any]]:
    repository = metadata_repository_for_target(platform=platform, environment=environment)
    repository.preflight()
    fallback_enabled = environment_source_fallback_enabled()
    sources = repository.query(
        f"SELECT * FROM {repository.table('cfg_source_system')} WHERE active_flag = :active_flag "
        "ORDER BY source_system_name",
        {"active_flag": True},
    )
    connections = repository.query(
        f"SELECT * FROM {repository.table('cfg_connection')} "
        "WHERE connection_type = :connection_type ORDER BY connection_id, config_version DESC",
        {"connection_type": "JDBC"},
    )
    latest_connections: Dict[int, Dict[str, Any]] = {}
    for connection in connections:
        connection_id = int(connection.get("connection_id") or 0)
        if connection_id and connection_id not in latest_connections:
            latest_connections[connection_id] = connection

    grouped: Dict[int, List[Dict[str, Any]]] = {}
    for connection in latest_connections.values():
        active = str(connection.get("active_flag") or "").lower() in {"1", "true"}
        current = str(connection.get("is_current") or "").lower() in {"1", "true"}
        if not (active and current) and not fallback_enabled:
            continue
        normalized = validate_jdbc_connection(connection)
        if normalized["config_hash"] != str(connection.get("config_hash") or ""):
            raise ValueError("A configured source connection has an invalid configuration hash.")
        allowed_projects = set(json.loads(normalized["config_json"])["allowed_project_ids"])
        if "*" not in allowed_projects and str(project_id or "").strip() not in allowed_projects:
            continue
        source_system_id = int(connection.get("source_system_id") or 0)
        grouped.setdefault(source_system_id, []).append(
            {
                # JSON clients cannot safely represent arbitrary BIGINT identifiers as numbers.
                "connection_id": str(connection["connection_id"]),
                "connection_name": str(connection.get("connection_name") or ""),
                "connection_type": str(connection.get("connection_type") or ""),
                "database_name": str(connection.get("database_name") or ""),
                "config_version": int(connection.get("config_version") or 0),
                "active": active and current,
                "design_time_fallback": not (active and current),
            }
        )

    return [
        {
            "source_system_id": str(source["source_system_id"]),
            "source_system_name": str(source.get("source_system_name") or ""),
            "connections": sorted(
                grouped[int(source["source_system_id"])],
                key=lambda item: item["connection_name"].casefold(),
            ),
        }
        for source in sources
        if int(source.get("source_system_id") or 0) in grouped
    ]


def validated_metadata_selection(state: Mapping[str, Any]) -> Optional[ValidatedMetadataSelection]:
    source_system_id = state.get("source_system_id")
    connection_id = state.get("source_connection_id")
    if source_system_id is None and connection_id is None:
        return None
    if source_system_id is None or connection_id is None:
        raise ValueError("source_system_id and source_connection_id must be supplied together.")

    repository = metadata_repository_for_target(
        platform=str(state.get("target_warehouse") or ""),
        environment=str(state.get("target_environment") or ""),
    )
    repository.preflight()
    source_system = repository.get_source_system(int(source_system_id))
    expected_version = state.get("source_connection_config_version")
    connection = (
        repository.get_connection(int(connection_id), int(expected_version))
        if expected_version is not None
        else repository.get_active_connection(int(connection_id))
    )
    uses_environment_source = False
    if not connection and expected_version is None and environment_source_fallback_enabled():
        connection = repository.get_latest_connection(int(connection_id))
        uses_environment_source = connection is not None
    if not source_system or not connection:
        raise ValueError("The selected metadata source system or connection is not active.")
    if str(source_system.get("active_flag") or "").strip().lower() not in {"1", "true", "yes"} and source_system.get("active_flag") is not True:
        raise ValueError("The selected source system is inactive.")
    connection_is_active = (
        str(connection.get("active_flag") or "").lower() in {"1", "true"}
        and str(connection.get("is_current") or "").lower() in {"1", "true"}
    )
    if not connection_is_active and not environment_source_fallback_enabled():
        raise ValueError("The selected connection version is no longer active/current.")
    uses_environment_source = uses_environment_source or not connection_is_active
    if int(connection.get("source_system_id") or 0) != int(source_system_id):
        raise ValueError("The selected connection belongs to a different source system.")
    normalized = validate_jdbc_connection(connection)
    if normalized["config_hash"] != str(connection.get("config_hash") or ""):
        raise ValueError("The active connection configuration hash is invalid.")
    expected_hash = str(state.get("source_connection_config_hash") or "")
    if expected_hash and expected_hash != normalized["config_hash"]:
        raise ValueError("The connection configuration changed after table nomination.")
    validate_deployment_database_binding(
        connection,
        target_platform=str(state.get("target_warehouse") or ""),
    )
    access = set(json.loads(normalized["config_json"])["allowed_project_ids"])
    project_id = str(state.get("project_id") or "").strip()
    if "*" not in access and project_id not in access:
        raise PermissionError("The selected project is not authorized to use this source connection.")
    return ValidatedMetadataSelection(repository, source_system, connection, uses_environment_source)
