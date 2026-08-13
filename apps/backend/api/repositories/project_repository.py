from __future__ import annotations

import re
import threading
import uuid
from typing import Any

from utilis.db import config, get_pipeline_connection


class ProjectRepository:
    """Azure SQL persistence for governed pipeline projects."""

    _table_name = "astra_projects"

    def __init__(self) -> None:
        self._ready = False
        self._ready_lock = threading.Lock()

    @property
    def schema(self) -> str:
        schema = str(config["azure_sql"].get("pipeline_schema") or "metadata")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
            raise RuntimeError("AZURE_SQL_PIPELINE_SCHEMA contains invalid characters")
        return schema

    @property
    def table(self) -> str:
        return f"[{self.schema}].[{self._table_name}]"

    def ensure_table(self) -> None:
        if self._ready:
            return
        with self._ready_lock:
            if self._ready:
                return
            connection = get_pipeline_connection()
            try:
                cursor = connection.cursor()
                cursor.execute(
                    f"""
                    IF OBJECT_ID(N'{self.table}', N'U') IS NULL
                    BEGIN
                        CREATE TABLE {self.table} (
                            id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
                            name NVARCHAR(255) NOT NULL,
                            description NVARCHAR(MAX) NOT NULL,
                            [target] NVARCHAR(50) NOT NULL,
                            status NVARCHAR(20) NOT NULL,
                            owner_email NVARCHAR(255) NOT NULL,
                            connection_type NVARCHAR(100) NOT NULL,
                            connection_name NVARCHAR(255) NULL,
                            db_type NVARCHAR(100) NULL,
                            database_name NVARCHAR(255) NULL,
                            integration_type NVARCHAR(50) NULL,
                            data_lake_type NVARCHAR(50) NULL,
                            data_lake_name NVARCHAR(255) NULL,
                            use_domain_knowledge_base BIT NOT NULL,
                            domain_profile NVARCHAR(100) NULL,
                            knowledge_base_id NVARCHAR(255) NULL,
                            execution_engine NVARCHAR(20) NOT NULL CONSTRAINT DF_astra_projects_execution_engine DEFAULT 'native',
                            dbt_deployment_mode NVARCHAR(40) NOT NULL CONSTRAINT DF_astra_projects_dbt_deployment_mode DEFAULT 'generate_only',
                            dbt_project_object_name NVARCHAR(80) NULL,
                            dbt_target_name NVARCHAR(80) NULL,
                            dbt_threads INT NULL,
                            dbt_command_timeout_secs INT NULL,
                            force_dbt_deploy BIT NOT NULL CONSTRAINT DF_astra_projects_force_dbt_deploy DEFAULT 0,
                            created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
                            updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
                            CONSTRAINT CK_astra_projects_target CHECK ([target] IN ('Databricks', 'Snowflake', 'Fabric')),
                            CONSTRAINT CK_astra_projects_status CHECK (status IN ('ACTIVE', 'ARCHIVED')),
                            CONSTRAINT CK_astra_projects_execution_engine CHECK (execution_engine IN ('native', 'dbt')),
                            CONSTRAINT CK_astra_projects_dbt_deployment_mode CHECK (dbt_deployment_mode IN ('generate_only', 'generate_and_deploy'))
                        )
                    END
                    """
                )
                for column_sql in (
                    f"IF COL_LENGTH(N'{self.table}', N'execution_engine') IS NULL ALTER TABLE {self.table} ADD execution_engine NVARCHAR(20) NOT NULL CONSTRAINT DF_astra_projects_execution_engine DEFAULT 'native'",
                    f"IF COL_LENGTH(N'{self.table}', N'dbt_deployment_mode') IS NULL ALTER TABLE {self.table} ADD dbt_deployment_mode NVARCHAR(40) NOT NULL CONSTRAINT DF_astra_projects_dbt_deployment_mode DEFAULT 'generate_only'",
                    f"IF COL_LENGTH(N'{self.table}', N'dbt_project_object_name') IS NULL ALTER TABLE {self.table} ADD dbt_project_object_name NVARCHAR(80) NULL",
                    f"IF COL_LENGTH(N'{self.table}', N'dbt_target_name') IS NULL ALTER TABLE {self.table} ADD dbt_target_name NVARCHAR(80) NULL",
                    f"IF COL_LENGTH(N'{self.table}', N'dbt_threads') IS NULL ALTER TABLE {self.table} ADD dbt_threads INT NULL",
                    f"IF COL_LENGTH(N'{self.table}', N'dbt_command_timeout_secs') IS NULL ALTER TABLE {self.table} ADD dbt_command_timeout_secs INT NULL",
                    f"IF COL_LENGTH(N'{self.table}', N'force_dbt_deploy') IS NULL ALTER TABLE {self.table} ADD force_dbt_deploy BIT NOT NULL CONSTRAINT DF_astra_projects_force_dbt_deploy DEFAULT 0",
                ):
                    cursor.execute(column_sql)
                cursor.execute(
                    f"""
                    IF NOT EXISTS (
                        SELECT 1
                        FROM sys.indexes
                        WHERE name = N'UX_astra_projects_dbt_project_object_name'
                          AND object_id = OBJECT_ID(N'{self.table}')
                    )
                        CREATE UNIQUE INDEX UX_astra_projects_dbt_project_object_name
                        ON {self.table}(dbt_project_object_name)
                        WHERE dbt_project_object_name IS NOT NULL
                    """
                )
                cursor.execute(
                    f"""
                    IF NOT EXISTS (
                        SELECT 1
                        FROM sys.check_constraints c
                        JOIN sys.tables t ON c.parent_object_id = t.object_id
                        JOIN sys.schemas s ON t.schema_id = s.schema_id
                        WHERE s.name = N'{self.schema}'
                          AND t.name = N'{self._table_name}'
                          AND c.name = N'CK_astra_projects_execution_engine'
                    )
                        ALTER TABLE {self.table} ADD CONSTRAINT CK_astra_projects_execution_engine CHECK (execution_engine IN ('native', 'dbt'))
                    """
                )
                cursor.execute(
                    f"""
                    IF NOT EXISTS (
                        SELECT 1
                        FROM sys.check_constraints c
                        JOIN sys.tables t ON c.parent_object_id = t.object_id
                        JOIN sys.schemas s ON t.schema_id = s.schema_id
                        WHERE s.name = N'{self.schema}'
                          AND t.name = N'{self._table_name}'
                          AND c.name = N'CK_astra_projects_dbt_deployment_mode'
                    )
                        ALTER TABLE {self.table} ADD CONSTRAINT CK_astra_projects_dbt_deployment_mode CHECK (dbt_deployment_mode IN ('generate_only', 'generate_and_deploy'))
                    """
                )
                connection.commit()
                self._ready = True
            finally:
                connection.close()

    def list_projects(self, owner_email: str | None = None) -> list[dict[str, Any]]:
        if owner_email:
            return self._query(
                f"{self._select()} WHERE LOWER(owner_email) = LOWER(?) ORDER BY updated_at DESC, created_at DESC",
                owner_email,
            )
        return self._query(f"{self._select()} ORDER BY updated_at DESC, created_at DESC")

    def find(self, project_id: str) -> dict[str, Any] | None:
        rows = self._query(f"{self._select()} WHERE id = ?", project_id)
        return rows[0] if rows else None

    def create(self, project: dict[str, Any]) -> dict[str, Any]:
        self.ensure_table()
        project_id = str(uuid.uuid4())
        project = dict(project)
        project_object_name = str(project.get("dbt_project_object_name") or "").strip()
        if project_object_name and self._query(
            f"{self._select()} WHERE UPPER(dbt_project_object_name) = UPPER(?)",
            project_object_name,
        ):
            compact_id = project_id.replace("-", "")[:8].upper()
            base_name = (
                project_object_name[:-4]
                if project_object_name.upper().endswith("_DBT")
                else project_object_name
            )
            project["dbt_project_object_name"] = f"{base_name[:67]}_{compact_id}_DBT"
        fields = self._fields(project)
        connection = get_pipeline_connection()
        try:
            connection.cursor().execute(
                f"""
                INSERT INTO {self.table}
                  (id, name, description, [target], status, owner_email, connection_type,
                   connection_name, db_type, database_name, integration_type, data_lake_type,
                   data_lake_name, use_domain_knowledge_base, domain_profile, knowledge_base_id,
                   execution_engine, dbt_deployment_mode, dbt_project_object_name,
                   dbt_target_name, dbt_threads,
                   dbt_command_timeout_secs, force_dbt_deploy)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                project_id,
                *fields,
            )
            connection.commit()
        finally:
            connection.close()
        return self.find(project_id) or {"id": project_id, **project}

    def update(self, project_id: str, project: dict[str, Any]) -> dict[str, Any] | None:
        self.ensure_table()
        connection = get_pipeline_connection()
        try:
            cursor = connection.cursor()
            cursor.execute(
                f"""
                UPDATE {self.table} SET
                  name = ?, description = ?, [target] = ?, status = ?, owner_email = ?,
                  connection_type = ?, connection_name = ?, db_type = ?, database_name = ?,
                  integration_type = ?, data_lake_type = ?, data_lake_name = ?,
                  use_domain_knowledge_base = ?, domain_profile = ?, knowledge_base_id = ?,
                  execution_engine = ?, dbt_deployment_mode = ?, dbt_project_object_name = ?,
                  dbt_target_name = ?,
                  dbt_threads = ?, dbt_command_timeout_secs = ?, force_dbt_deploy = ?,
                  updated_at = SYSUTCDATETIME()
                WHERE id = ?
                """,
                *self._fields(project),
                project_id,
            )
            connection.commit()
        finally:
            connection.close()
        return self.find(project_id)

    def delete(self, project_id: str) -> bool:
        self.ensure_table()
        connection = get_pipeline_connection()
        try:
            cursor = connection.cursor()
            cursor.execute(f"DELETE FROM {self.table} WHERE id = ?", project_id)
            deleted = cursor.rowcount > 0
            connection.commit()
            return deleted
        finally:
            connection.close()

    @staticmethod
    def _fields(project: dict[str, Any]) -> tuple[Any, ...]:
        project = ProjectRepository._with_execution_defaults(project)
        return (
            project["name"], project["description"], project["target"], project["status"],
            project["owner_email"], project["connection_type"], project.get("connection_name"),
            project.get("db_type"), project.get("database_name"), project.get("integration_type"),
            project.get("data_lake_type"), project.get("data_lake_name"),
            bool(project.get("use_domain_knowledge_base")), project.get("domain_profile"),
            project.get("knowledge_base_id"),
            project.get("execution_engine") or "native",
            project.get("dbt_deployment_mode") or "generate_only",
            project.get("dbt_project_object_name"),
            project.get("dbt_target_name"), project.get("dbt_threads"),
            project.get("dbt_command_timeout_secs"), bool(project.get("force_dbt_deploy")),
        )

    @staticmethod
    def _with_execution_defaults(project: dict[str, Any]) -> dict[str, Any]:
        project = dict(project)
        snowflake_supported_target = (
            str(project.get("target") or "").strip().lower() == "snowflake"
            and (
                str(project.get("connection_type") or "").strip().lower() == "database"
                or (
                    str(project.get("connection_type") or "").strip().lower() == "data_lake"
                    and str(project.get("integration_type") or "").strip().lower() == "adls"
                )
            )
        )
        selected_engine = str(project.get("execution_engine") or "native").strip().lower()
        project["execution_engine"] = (
            "dbt" if snowflake_supported_target and selected_engine == "dbt" else "native"
        )
        project["dbt_deployment_mode"] = (
            "generate_and_deploy"
            if project["execution_engine"] == "dbt"
            else "generate_only"
        )
        if project["execution_engine"] != "dbt":
            project["force_dbt_deploy"] = False
        return project

    def _select(self) -> str:
        return f"""
            SELECT CONVERT(NVARCHAR(36), id) AS id, name, description, [target], status,
                   owner_email, connection_type, connection_name, db_type, database_name,
                   integration_type, data_lake_type, data_lake_name,
                   CAST(use_domain_knowledge_base AS BIT) AS use_domain_knowledge_base,
                   domain_profile, knowledge_base_id, execution_engine, dbt_deployment_mode,
                   dbt_project_object_name, dbt_target_name, dbt_threads, dbt_command_timeout_secs,
                   CAST(force_dbt_deploy AS BIT) AS force_dbt_deploy,
                   created_at, updated_at
            FROM {self.table}
        """

    def _query(self, query: str, *parameters: Any) -> list[dict[str, Any]]:
        self.ensure_table()
        connection = get_pipeline_connection()
        try:
            cursor = connection.cursor()
            cursor.execute(query, *parameters)
            columns = [column[0] for column in cursor.description]
            return [
                self._with_execution_defaults(dict(zip(columns, row)))
                for row in cursor.fetchall()
            ]
        finally:
            connection.close()
