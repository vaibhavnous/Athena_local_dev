from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Mapping

from services.metadata_contracts import validate_jdbc_connection


# ponytail: this is the temporary single-source catalog requested for rollout.
# Replace this constant with the planned administration page when a second source
# is onboarded; the API/run contract below does not need to change.
DATABASE_SOURCE_SYSTEM_ID = 7499026347042686646
DATABASE_CONNECTION_ID = 3358264270364792816
DATABASE_SOURCE_PROFILE = "insurance_azure_sql"


def database_source_options() -> List[Dict[str, Any]]:
    return [
        {
            "source_system_id": str(DATABASE_SOURCE_SYSTEM_ID),
            "source_system_name": "Insurance",
            "source_profile": DATABASE_SOURCE_PROFILE,
            "connections": [
                {
                    "connection_id": str(DATABASE_CONNECTION_ID),
                    "connection_name": "Insurance Azure SQL QA",
                    "connection_type": "JDBC",
                    "database_name": "insurance",
                    "source_profile": DATABASE_SOURCE_PROFILE,
                    "config_version": 1,
                    "active": True,
                    "design_time_fallback": True,
                }
            ],
        }
    ]


def database_source_contract(*, platform: str) -> tuple[Dict[str, Any], Dict[str, Any]]:
    normalized_platform = str(platform or "").strip().lower()
    if normalized_platform not in {"databricks", "snowflake"}:
        raise ValueError(f"Unsupported database target: {platform!r}")

    from utilis.db import config

    source_config = config["azure_sql"]
    if normalized_platform == "databricks":
        scope = str(os.getenv("DATABRICKS_SOURCE_SECRET_SCOPE") or "dataedge-secrets").strip()
        secrets = {
            "username": {
                "scope": scope,
                "key": str(os.getenv("DATABRICKS_SOURCE_USERNAME_SECRET_KEY") or "azure-sql-username").strip(),
            },
            "password": {
                "scope": scope,
                "key": str(os.getenv("DATABRICKS_SOURCE_PASSWORD_SECRET_KEY") or "azure-sql-password").strip(),
            },
        }
    else:
        secrets = {
            "username": {"scope": "DEPLOYMENT_ENV", "key": "AZURE_SQL_SOURCE_USERNAME"},
            "password": {"scope": "DEPLOYMENT_ENV", "key": "AZURE_SQL_SOURCE_PASSWORD"},
        }

    source_system = {
        "source_system_id": DATABASE_SOURCE_SYSTEM_ID,
        "source_system_name": "Insurance",
        "business_domain": "Insurance",
        "description": "Temporary application-configured database source",
        "active_flag": True,
    }
    connection = validate_jdbc_connection(
        {
            "connection_id": DATABASE_CONNECTION_ID,
            "source_system_id": DATABASE_SOURCE_SYSTEM_ID,
            "connection_name": "Insurance Azure SQL QA",
            "connection_type": "JDBC",
            "connection_contract_name": "JDBC_CONNECTION",
            "connection_schema_version": "1.0",
            "host_name": str(source_config.get("source_host") or "dataedge.database.windows.net"),
            "port": int(source_config.get("port") or 1433),
            "database_name": str(source_config.get("source_database") or "insurance"),
            "auth_type": "BASIC",
            "secrets_json": json.dumps(secrets, sort_keys=True),
            "config_json": json.dumps(
                {
                    "allowed_project_ids": ["*"],
                    "encrypt": True,
                    "jdbc_driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                },
                sort_keys=True,
            ),
            "config_version": 1,
            "is_current": True,
            "active_flag": True,
        }
    )
    return source_system, connection


def selected_database_source(
    *, source_system_id: Any, connection_id: Any, platform: str
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    if int(source_system_id or 0) != DATABASE_SOURCE_SYSTEM_ID:
        raise ValueError("The selected source system is not present in the application source catalog.")
    if int(connection_id or 0) != DATABASE_CONNECTION_ID:
        raise ValueError("The selected source connection is not present in the application source catalog.")
    return database_source_contract(platform=platform)

