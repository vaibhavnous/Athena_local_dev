import os
import uuid
from typing import Any, Dict, List

from fastapi import APIRouter

router = APIRouter()


@router.get("/settings")
def settings() -> Dict[str, Any]:
    return {
        "provider": "azure_openai",
        "azure_deployment": os.getenv("AZURE_OPENAI_DEPLOYMENT"),
        "budget": 5.0,
        "maxKpis": 25,
        "devMode": os.getenv("DEV_MODE", "").lower() in {"1", "true", "yes", "on"},
    }


@router.put("/settings")
def save_settings(data: Dict[str, Any]) -> Dict[str, Any]:
    return data


@router.get("/configurations")
def configurations() -> List[Dict[str, Any]]:
    from utilis.db import config

    db_conf = config["azure_sql"]
    return [
        {
            "id": "azure_sql_default",
            "name": "Default Azure SQL",
            "sourceType": "database",
            "dbType": "azure_sql",
            "host": db_conf.get("source_host"),
            "port": str(db_conf.get("port", 1433)),
            "databaseName": db_conf.get("source_database"),
            "schema": db_conf.get("source_schema"),
            "username": db_conf.get("source_username"),
            "driverClass": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "jdbcUrl": "",
        }
    ]


@router.post("/configurations")
def create_configuration(data: Dict[str, Any]) -> Dict[str, Any]:
    return {**data, "id": data.get("id") or str(uuid.uuid4())}


@router.put("/configurations/{config_id}")
def update_configuration(config_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
    return {**data, "id": config_id}


@router.delete("/configurations/{config_id}")
def delete_configuration(config_id: str) -> Dict[str, Any]:
    return {"id": config_id, "deleted": True}
