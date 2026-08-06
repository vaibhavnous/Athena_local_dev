from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional

from services.metadata_repository import MetadataRepository, metadata_repository_for_target
from services.metadata_contracts import validate_jdbc_connection
from services.source_connection_validation import validate_deployment_database_binding


@dataclass(frozen=True)
class ValidatedMetadataSelection:
    repository: MetadataRepository
    source_system: Dict[str, Any]
    connection: Dict[str, Any]


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
    if not source_system or not connection:
        raise ValueError("The selected metadata source system or connection is not active.")
    if str(source_system.get("active_flag") or "").strip().lower() not in {"1", "true", "yes"} and source_system.get("active_flag") is not True:
        raise ValueError("The selected source system is inactive.")
    if not (str(connection.get("active_flag") or "").lower() in {"1", "true"} and str(connection.get("is_current") or "").lower() in {"1", "true"}):
        raise ValueError("The selected connection version is no longer active/current.")
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
    return ValidatedMetadataSelection(repository, source_system, connection)
