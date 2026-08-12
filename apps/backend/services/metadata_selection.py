from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional

from services.metadata_repository import MetadataRepository, metadata_repository_for_target
from services.metadata_contracts import validate_connection
from services.source_connection_validation import (
    validate_deployment_adls_binding,
    validate_deployment_database_binding,
)


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
    del platform, environment, project_id
    from services.database_source_catalog import database_source_options

    return database_source_options()


def validated_metadata_selection(state: Mapping[str, Any]) -> Optional[ValidatedMetadataSelection]:
    source_system_id = state.get("source_system_id")
    connection_id = state.get("source_connection_id")
    if source_system_id is None and connection_id is None:
        return None
    if source_system_id is None or connection_id is None:
        raise ValueError("source_system_id and source_connection_id must be supplied together.")

    platform = str(state.get("target_warehouse") or "").strip().lower()
    environment = str(state.get("target_environment") or "").strip()
    if str(state.get("database_flow_version") or "") != "generation_first_v2":
        repository = metadata_repository_for_target(platform=platform, environment=environment)
        return _validated_metadata_selection(state, repository)
    from services.application_metadata_repository import application_metadata_repository
    if str(state.get("source") or "database").lower() == "adls_gen2":
        from services.adls_source import source_catalog

        source_system, connection = source_catalog(platform=platform)
        if int(source_system_id) != int(source_system["source_system_id"]) or int(connection_id) != int(
            connection["connection_id"]
        ):
            raise ValueError("The selected ADLS metadata source does not match the configured source root.")
    else:
        from services.database_source_catalog import selected_database_source

        source_system, connection = selected_database_source(
            source_system_id=source_system_id,
            connection_id=connection_id,
            platform=platform,
        )
    selected_profile = str(state.get("source_profile") or "").strip()
    expected_profile = "insurance_adls" if str(state.get("source") or "").lower() == "adls_gen2" else "insurance_azure_sql"
    if selected_profile and selected_profile != expected_profile:
        raise ValueError("The selected source profile does not match the application source catalog.")
    repository = application_metadata_repository(
        platform=platform,
        environment=environment,
        source_system=source_system,
        connection=connection,
    )
    unit_of_work = getattr(repository, "unit_of_work", None)
    if callable(unit_of_work):
        with unit_of_work():
            return _validated_metadata_selection(state, repository)
    return _validated_metadata_selection(state, repository)


def validated_target_metadata_selection(state: Mapping[str, Any]) -> Optional[ValidatedMetadataSelection]:
    """Resolve the target repository only after target execution has started."""
    source_system_id = state.get("source_system_id")
    connection_id = state.get("source_connection_id")
    if source_system_id is None and connection_id is None:
        return None
    if source_system_id is None or connection_id is None:
        raise ValueError("source_system_id and source_connection_id must be supplied together.")

    platform = str(state.get("target_warehouse") or "").strip().lower()
    environment = str(state.get("target_environment") or "").strip()
    if str(state.get("source") or "database").lower() == "adls_gen2":
        from services.adls_source import source_catalog

        source_system, connection = source_catalog(platform=platform)
        if int(source_system_id) != int(source_system["source_system_id"]) or int(connection_id) != int(
            connection["connection_id"]
        ):
            raise ValueError("The selected ADLS metadata source does not match the configured source root.")
    else:
        from services.database_source_catalog import selected_database_source

        source_system, connection = selected_database_source(
            source_system_id=source_system_id,
            connection_id=connection_id,
            platform=platform,
        )
    repository = metadata_repository_for_target(platform=platform, environment=environment)
    repository.preflight()
    return ValidatedMetadataSelection(repository, source_system, connection, True)


def _validated_metadata_selection(
    state: Mapping[str, Any], repository: MetadataRepository
) -> ValidatedMetadataSelection:
    source_system_id = state["source_system_id"]
    connection_id = state["source_connection_id"]
    repository.preflight()
    source_system = repository.get_source_system(int(source_system_id))
    expected_version = state.get("source_connection_config_version")
    connection = (
        repository.get_connection(int(connection_id), int(expected_version))
        if expected_version is not None
        else repository.get_active_connection(int(connection_id))
    )
    uses_environment_source = bool(getattr(repository, "uses_environment_source", False))
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
    normalized = validate_connection(connection)
    if normalized["config_hash"] != str(connection.get("config_hash") or ""):
        raise ValueError("The active connection configuration hash is invalid.")
    expected_hash = str(state.get("source_connection_config_hash") or "")
    if expected_hash and expected_hash != normalized["config_hash"]:
        raise ValueError("The connection configuration changed after table nomination.")
    if str(connection.get("connection_type") or "").upper() == "ADLS":
        validate_deployment_adls_binding(connection)
    else:
        validate_deployment_database_binding(
            connection,
            target_platform=str(state.get("target_warehouse") or ""),
        )
    access = set(json.loads(normalized["config_json"])["allowed_project_ids"])
    project_id = str(state.get("project_id") or "").strip()
    if "*" not in access and project_id not in access:
        raise PermissionError("The selected project is not authorized to use this source connection.")
    return ValidatedMetadataSelection(repository, source_system, connection, uses_environment_source)
