from __future__ import annotations

import re
import threading
from typing import Any

from utilis.db import config, get_pipeline_connection


class AuthRepository:
    """Azure SQL persistence for Astra accounts."""

    _table_name = "astra_users"

    def __init__(self) -> None:
        self._ready = False
        self._ready_lock = threading.Lock()

    @property
    def table(self) -> str:
        schema = str(config["azure_sql"].get("pipeline_schema") or "metadata")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
            raise RuntimeError("AZURE_SQL_PIPELINE_SCHEMA contains invalid characters")
        return f"[{schema}].[{self._table_name}]"

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
                            uid UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
                            username NVARCHAR(255) NOT NULL,
                            email NVARCHAR(255) NOT NULL,
                            password_hash NVARCHAR(255) NOT NULL,
                            user_type NVARCHAR(20) NOT NULL,
                            is_active BIT NOT NULL CONSTRAINT DF_astra_users_active DEFAULT 1,
                            token_version INT NOT NULL CONSTRAINT DF_astra_users_token_version DEFAULT 1,
                            created_at DATETIME2 NOT NULL CONSTRAINT DF_astra_users_created DEFAULT SYSUTCDATETIME(),
                            updated_at DATETIME2 NOT NULL CONSTRAINT DF_astra_users_updated DEFAULT SYSUTCDATETIME(),
                            CONSTRAINT CK_astra_users_type CHECK (user_type IN ('Admin', 'Client'))
                        );
                        CREATE UNIQUE INDEX UX_astra_users_email ON {self.table}(email);
                    END
                    """
                )
                connection.commit()
                self._ready = True
            finally:
                connection.close()

    def find_by_email(self, email: str) -> dict[str, Any] | None:
        return self._fetch_one(
            f"""
            SELECT CONVERT(NVARCHAR(36), uid) AS uid, username, email,
                   password_hash, user_type, CAST(is_active AS BIT) AS is_active,
                   token_version
            FROM {self.table} WHERE email = ?
            """,
            email,
        )

    def find_by_uid(self, uid: str) -> dict[str, Any] | None:
        return self._fetch_one(
            f"""
            SELECT CONVERT(NVARCHAR(36), uid) AS uid, username, email,
                   password_hash, user_type, CAST(is_active AS BIT) AS is_active,
                   token_version
            FROM {self.table} WHERE uid = ?
            """,
            uid,
        )

    def list_users(self) -> list[dict[str, Any]]:
        return self._fetch_all(
            f"""
            SELECT CONVERT(NVARCHAR(36), uid) AS uid, username, email,
                   user_type, CAST(is_active AS BIT) AS is_active, token_version
            FROM {self.table}
            ORDER BY CASE WHEN user_type = 'Admin' THEN 0 ELSE 1 END, username
            """
        )

    def create_user(
        self,
        *,
        uid: str,
        username: str,
        email: str,
        password_hash: str,
        user_type: str,
    ) -> dict[str, Any]:
        connection = get_pipeline_connection()
        try:
            cursor = connection.cursor()
            cursor.execute(
                f"""
                INSERT INTO {self.table} (uid, username, email, password_hash, user_type)
                VALUES (?, ?, ?, ?, ?)
                """,
                uid,
                username,
                email,
                password_hash,
                user_type,
            )
            connection.commit()
        finally:
            connection.close()
        user = self.find_by_uid(uid)
        if not user:
            raise RuntimeError("Created account could not be loaded")
        return user

    def update_user(
        self,
        uid: str,
        *,
        username: str,
        email: str,
        user_type: str,
        password_hash: str | None,
    ) -> dict[str, Any] | None:
        password_sql = ", password_hash = ?" if password_hash else ""
        parameters: list[Any] = [username, email, user_type]
        if password_hash:
            parameters.append(password_hash)
        parameters.append(uid)
        connection = get_pipeline_connection()
        try:
            cursor = connection.cursor()
            cursor.execute(
                f"""
                UPDATE {self.table}
                SET username = ?, email = ?, user_type = ?{password_sql},
                    token_version = token_version + 1, updated_at = SYSUTCDATETIME()
                WHERE uid = ?
                """,
                *parameters,
            )
            connection.commit()
        finally:
            connection.close()
        return self.find_by_uid(uid)

    def set_active(self, uid: str, is_active: bool) -> dict[str, Any] | None:
        connection = get_pipeline_connection()
        try:
            cursor = connection.cursor()
            cursor.execute(
                f"""
                UPDATE {self.table}
                SET is_active = ?, token_version = token_version + 1,
                    updated_at = SYSUTCDATETIME()
                WHERE uid = ?
                """,
                is_active,
                uid,
            )
            connection.commit()
        finally:
            connection.close()
        return self.find_by_uid(uid)

    def delete_user(self, uid: str) -> bool:
        connection = get_pipeline_connection()
        try:
            cursor = connection.cursor()
            cursor.execute(f"DELETE FROM {self.table} WHERE uid = ?", uid)
            deleted = cursor.rowcount > 0
            connection.commit()
            return deleted
        finally:
            connection.close()

    def _fetch_one(self, query: str, *parameters: Any) -> dict[str, Any] | None:
        rows = self._query(query, *parameters)
        return rows[0] if rows else None

    def _fetch_all(self, query: str, *parameters: Any) -> list[dict[str, Any]]:
        return self._query(query, *parameters)

    def _query(self, query: str, *parameters: Any) -> list[dict[str, Any]]:
        connection = get_pipeline_connection()
        try:
            cursor = connection.cursor()
            cursor.execute(query, *parameters)
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            connection.close()
