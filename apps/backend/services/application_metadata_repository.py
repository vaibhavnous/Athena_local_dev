from __future__ import annotations

import re
import threading
from contextlib import contextmanager
from typing import Any, Dict, Iterable, List, Mapping, Optional

from services.metadata_contracts import (
    METADATA_TABLES,
    TargetMetadataContext,
    expected_columns,
    validate_identifier,
)
from services.metadata_repository import MetadataRepository, _PARAMETER


_DESIGN_TABLES = {"cfg_ingestion_object", "cfg_mapping"}


class ApplicationMetadataRepository(MetadataRepository):
    """Persist design metadata in the application Azure SQL database."""

    uses_environment_source = True
    max_bind_parameters = 2000

    def __init__(
        self,
        *,
        platform: str,
        environment: str,
        source_system: Mapping[str, Any],
        connection: Mapping[str, Any],
    ) -> None:
        # The physical Azure SQL database is intentionally target-independent.
        # platform remains the selected code dialect used by mapping normalization.
        super().__init__(
            TargetMetadataContext(
                platform=platform,
                environment=environment,
                namespace="application",
                schema="metadata",
            )
        )
        self.source_system = dict(source_system)
        self.connection = dict(connection)
        self._transaction = threading.local()

    def table(self, table_name: str) -> str:
        if table_name not in METADATA_TABLES:
            raise ValueError(f"Unsupported metadata table: {table_name!r}")
        from utilis.db import config

        schema = validate_identifier(
            config["azure_sql"].get("pipeline_schema") or "metadata",
            label="application metadata schema",
        )
        return f"[{schema}].[{table_name}]"

    def bootstrap(self) -> None:
        raise RuntimeError("Application metadata tables are deployment prerequisites, not runtime target DDL.")

    def preflight(self) -> None:
        from utilis.db import config

        schema = validate_identifier(
            config["azure_sql"].get("pipeline_schema") or "metadata",
            label="application metadata schema",
        )
        rows = self.query(
            "SELECT LOWER(TABLE_NAME) AS table_name, LOWER(COLUMN_NAME) AS column_name "
            "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = :schema "
            "AND TABLE_NAME IN ('cfg_ingestion_object', 'cfg_mapping')",
            {"schema": schema},
        )
        actual: Dict[str, set[str]] = {}
        for row in rows:
            actual.setdefault(str(row["table_name"]).lower(), set()).add(
                str(row["column_name"]).lower()
            )
        expected = expected_columns()
        errors = []
        for table_name in sorted(_DESIGN_TABLES):
            missing = expected[table_name] - actual.get(table_name, set())
            unexpected = actual.get(table_name, set()) - expected[table_name]
            if missing:
                errors.append(f"{table_name} missing columns: {', '.join(sorted(missing))}")
            if unexpected:
                errors.append(f"{table_name} has unexpected columns: {', '.join(sorted(unexpected))}")
        if errors:
            raise RuntimeError("Application metadata schema drift: " + "; ".join(errors))

    def get_source_system(self, source_system_id: int) -> Optional[Dict[str, Any]]:
        return dict(self.source_system) if int(source_system_id) == int(self.source_system["source_system_id"]) else None

    def get_source_system_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        return (
            dict(self.source_system)
            if str(name or "").casefold() == str(self.source_system.get("source_system_name") or "").casefold()
            else None
        )

    def get_connection(self, connection_id: int, config_version: Optional[int] = None) -> Optional[Dict[str, Any]]:
        if int(connection_id) != int(self.connection["connection_id"]):
            return None
        if config_version is not None and int(config_version) != int(self.connection.get("config_version") or 0):
            return None
        return dict(self.connection)

    def get_active_connection(self, connection_id: int) -> Optional[Dict[str, Any]]:
        return self.get_connection(connection_id)

    def get_latest_connection(self, connection_id: int) -> Optional[Dict[str, Any]]:
        return self.get_connection(connection_id)

    def _transaction_connection(self) -> Any:
        return getattr(self._transaction, "connection", None)

    @contextmanager
    def unit_of_work(self):
        existing = self._transaction_connection()
        if existing is not None:
            yield self
            return

        from utilis.db import get_pipeline_connection

        connection = get_pipeline_connection()
        self._transaction.connection = connection
        try:
            yield self
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            del self._transaction.connection
            connection.close()

    @staticmethod
    def _adapt(sql: str, parameters: Optional[Mapping[str, Any]]) -> tuple[str, List[Any]]:
        values = parameters or {}
        ordered: List[Any] = []

        def replace(match: re.Match[str]) -> str:
            name = match.group(1)
            if name not in values:
                raise ValueError(f"Missing SQL parameter: {name}")
            ordered.append(values[name])
            return "?"

        adapted = _PARAMETER.sub(replace, sql)
        adapted = re.sub(r"\bCURRENT_TIMESTAMP\(\)", "CURRENT_TIMESTAMP", adapted, flags=re.IGNORECASE)
        adapted = re.sub(r"\bTRUE\b", "1", adapted, flags=re.IGNORECASE)
        adapted = re.sub(r"\bFALSE\b", "0", adapted, flags=re.IGNORECASE)
        if adapted.lstrip().upper().startswith("MERGE") and not adapted.rstrip().endswith(";"):
            adapted = adapted.rstrip() + ";"
        return adapted, ordered

    def execute(self, sql: str, parameters: Optional[Mapping[str, Any]] = None) -> None:
        adapted, ordered = self._adapt(sql, parameters)
        connection = self._transaction_connection()
        owns_connection = connection is None
        if owns_connection:
            from utilis.db import get_pipeline_connection

            connection = get_pipeline_connection()
        cursor = None
        try:
            cursor = connection.cursor()
            cursor.execute(adapted, tuple(ordered))
            if owns_connection:
                connection.commit()
        except Exception:
            if owns_connection:
                connection.rollback()
            raise
        finally:
            if cursor is not None:
                cursor.close()
            if owns_connection:
                connection.close()

    def execute_batch(self, statements: Iterable[tuple[str, Mapping[str, Any]]]) -> None:
        with self.unit_of_work():
            for sql, parameters in statements:
                self.execute(sql, parameters)

    def query(self, sql: str, parameters: Optional[Mapping[str, Any]] = None) -> List[Dict[str, Any]]:
        adapted, ordered = self._adapt(sql, parameters)
        connection = self._transaction_connection()
        owns_connection = connection is None
        if owns_connection:
            from utilis.db import get_pipeline_connection

            connection = get_pipeline_connection()
        cursor = None
        try:
            cursor = connection.cursor()
            cursor.execute(adapted, tuple(ordered))
            columns = [str(column[0]).lower() for column in (cursor.description or [])]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            if cursor is not None:
                cursor.close()
            if owns_connection:
                connection.close()


def application_metadata_repository(
    *,
    platform: str,
    environment: str,
    source_system: Mapping[str, Any],
    connection: Mapping[str, Any],
) -> ApplicationMetadataRepository:
    return ApplicationMetadataRepository(
        platform=platform,
        environment=environment,
        source_system=source_system,
        connection=connection,
    )
