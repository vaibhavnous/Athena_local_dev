from __future__ import annotations

import json
import os
import re
import threading
import time
import uuid
from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional

from services.metadata_contracts import (
    canonical_json_hash,
    bronze_target_data_type,
    METADATA_TABLES,
    TargetMetadataContext,
    render_ddl,
    split_sql_statements,
    stable_bigint,
    normalize_bronze_column_name,
    validate_identifier,
    validate_jdbc_connection,
    validate_runtime_context,
    validate_schema_columns,
    validate_snowflake_logical_work_filters,
)
from utilis.logger import redact_sensitive, redact_sensitive_text


_PARAMETER = re.compile(r":([A-Za-z_][A-Za-z0-9_]*)")


def _as_bool(value: Any) -> bool:
    return value is True or str(value or "").strip().lower() in {"1", "true", "yes"}


def _validate_source_resource_binding(obj: Mapping[str, Any], source: Any) -> None:
    object_parts = str(obj.get("object_name") or "").split(".")
    expected = {
        "database": object_parts[-3] if len(object_parts) >= 3 else "",
        "schema": str(obj.get("database_schema") or (object_parts[-2] if len(object_parts) >= 2 else "")),
        "table": str(obj.get("table_name") or (object_parts[-1] if object_parts else "")),
    }
    if not isinstance(source, Mapping) or any(
        not value or str(source.get(name) or "").casefold() != value.casefold()
        for name, value in expected.items()
    ):
        raise ValueError("The execution artifact source resource does not match the ingestion object.")


def _validate_snowflake_transformation_contract(sql: str, obj: Mapping[str, Any]) -> None:
    from nodes.bronze_gen import _snowflake_qualified_name, _sql_without_comments

    clean_sql = _sql_without_comments(sql)
    target = str(obj.get("target_table") or obj.get("target_bronze_table") or "").strip()
    target_parts = target.split(".")
    if len(target_parts) != 3 or _snowflake_qualified_name(*target_parts).casefold() not in clean_sql.casefold():
        raise ValueError("Snowflake transformation SQL does not write to its exact configured target.")
    try:
        dependencies = json.loads(str(obj.get("dependency_objects_json") or "{}"))["dependencies"]
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError("Snowflake transformation activation requires valid dependency pins.") from exc
    expected_sources = {
        _snowflake_qualified_name(*str(item.get("object_name") or "").split("."))
        for item in dependencies
        if isinstance(item, Mapping) and len(str(item.get("object_name") or "").split(".")) == 3
    }
    actual_sources = {
        match.group(1)
        for match in re.finditer(
            r'\b(?:FROM|JOIN)\s+((?:"(?:""|[^"])+"\.){2}"(?:""|[^"])+")',
            clean_sql,
            flags=re.IGNORECASE,
        )
    }
    if not expected_sources or {item.casefold() for item in actual_sources} != {
        item.casefold() for item in expected_sources
    }:
        raise ValueError("Snowflake transformation SQL does not use exactly its pinned input objects.")
    validate_snowflake_logical_work_filters(clean_sql, expected_sources)


def _validate_snowflake_registered_artifact(
    artifact_path: Any,
    obj: Mapping[str, Any],
    spec: Mapping[str, Any],
    processing_stage: str,
    target_table: str,
) -> None:
    engine = str(spec.get("engine") or "").upper()
    if engine == "SNOWFLAKE_DBT":
        if (
            not re.fullmatch(r"[0-9a-f]{64}", str(spec.get("dbt_package_hash") or ""))
            or not str(spec.get("dbt_package_id") or "").strip()
            or artifact_path.suffix.lower() != ".sql"
        ):
            raise ValueError("Snowflake dbt activation requires a finalized package and SQL model artifact.")
        return
    if engine != "SNOWFLAKE_SQL":
        raise ValueError("Snowflake artifact activation received an unsupported execution engine.")

    from nodes.bronze_gen import _snowflake_qualified_name, validate_snowflake_bronze_sql

    sql = artifact_path.read_text(encoding="utf-8")
    if processing_stage == "SOURCE_TO_BRONZE":
        landing = spec.get("landing_resource")
        if not isinstance(landing, dict):
            raise ValueError("Snowflake Bronze activation requires a pinned landing resource.")
        target_parts = target_table.split(".")
        if len(target_parts) != 3:
            raise ValueError("Snowflake Bronze activation requires a three-part target table.")
        validate_snowflake_bronze_sql(
            sql,
            source_table=_snowflake_qualified_name(
                landing["database"], landing["schema"], landing["table"]
            ),
            target_table=_snowflake_qualified_name(*target_parts),
            metadata_driven=True,
        )
    else:
        _validate_snowflake_transformation_contract(sql, obj)


class MetadataRepository(ABC):
    def __init__(self, context: TargetMetadataContext) -> None:
        self.context = context

    @contextmanager
    def unit_of_work(self):
        yield self

    @abstractmethod
    def execute(self, sql: str, parameters: Optional[Mapping[str, Any]] = None) -> None:
        raise NotImplementedError

    @abstractmethod
    def query(self, sql: str, parameters: Optional[Mapping[str, Any]] = None) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def execute_batch(self, statements: Iterable[tuple[str, Mapping[str, Any]]]) -> None:
        for sql, parameters in statements:
            self.execute(sql, parameters)

    def deploy_configuration_snapshot(
        self,
        *,
        ingestion_objects: Iterable[Mapping[str, Any]],
        mappings: Iterable[Mapping[str, Any]],
    ) -> None:
        """Idempotently deploy an approved application metadata snapshot to a target."""
        object_rows = [dict(row) for row in ingestion_objects]
        mapping_rows = [dict(row) for row in mappings]
        self._deploy_configuration_rows(
            table_name="cfg_ingestion_object",
            rows=object_rows,
            key_columns=("ingestion_object_id", "config_version"),
            hash_column="config_hash",
        )
        self._deploy_configuration_rows(
            table_name="cfg_mapping",
            rows=mapping_rows,
            key_columns=("mapping_id", "mapping_version"),
            hash_column="mapping_hash",
        )

    def _deploy_configuration_rows(
        self,
        *,
        table_name: str,
        rows: List[Dict[str, Any]],
        key_columns: tuple[str, ...],
        hash_column: str,
    ) -> None:
        if not rows:
            return
        columns = tuple(rows[0])
        if any(tuple(row) != columns for row in rows):
            raise ValueError(f"{table_name} snapshot rows do not share one schema contract.")
        if any(column not in columns for column in (*key_columns, hash_column)):
            raise ValueError(f"{table_name} snapshot is missing its immutable identity contract.")
        lookup_where, lookup_parameters = self._where_pairs(
            [{column: row[column] for column in key_columns} for row in rows],
            prefix="existing_deploy_",
        )
        existing_rows = self.query(
            f"SELECT {', '.join((*key_columns, hash_column))} FROM {self.table(table_name)} "
            f"WHERE {lookup_where}",
            lookup_parameters,
        )
        existing = {
            tuple(row.get(column) for column in key_columns): str(row.get(hash_column) or "")
            for row in existing_rows
        }
        for row in rows:
            key = tuple(row[column] for column in key_columns)
            if key in existing and existing[key] != str(row.get(hash_column) or ""):
                raise RuntimeError(f"{table_name} target contains a conflicting immutable version.")
        rows = [row for row in rows if tuple(row[column] for column in key_columns) not in existing]
        if not rows:
            return
        update_columns = tuple(column for column in columns if column not in key_columns and column != "created_at")
        for offset in range(0, len(rows), 50):
            chunk = rows[offset : offset + 50]
            _, source, parameters = self._source_rows(chunk, prefix=f"deploy_{offset}_")
            join = " AND ".join(f"target.{column} = source.{column}" for column in key_columns)
            updates = ", ".join(f"target.{column} = source.{column}" for column in update_columns)
            self.execute(
                f"MERGE INTO {self.table(table_name)} AS target USING ({source}) AS source "
                f"ON {join} "
                f"WHEN MATCHED AND target.{hash_column} = source.{hash_column} THEN UPDATE SET {updates} "
                f"WHEN NOT MATCHED THEN INSERT ({', '.join(columns)}) "
                f"VALUES ({', '.join('source.' + column for column in columns)})",
                parameters,
            )

        where, parameters = self._where_pairs(
            [{column: row[column] for column in key_columns} for row in rows],
            prefix="verify_deploy_",
        )
        saved = self.query(
            f"SELECT {', '.join((*key_columns, hash_column))} FROM {self.table(table_name)} WHERE {where}",
            parameters,
        )
        expected = {
            tuple(row[column] for column in key_columns): str(row.get(hash_column) or "")
            for row in rows
        }
        actual = {
            tuple(row.get(column) for column in key_columns): str(row.get(hash_column) or "")
            for row in saved
        }
        if actual != expected:
            raise RuntimeError(f"{table_name} target deployment failed its immutable postcondition.")

    @staticmethod
    def _source_rows(
        rows: Iterable[Mapping[str, Any]], *, prefix: str
    ) -> tuple[tuple[str, ...], str, Dict[str, Any]]:
        materialized = [dict(row) for row in rows]
        if not materialized:
            raise ValueError("A bulk metadata operation requires at least one row.")
        names = tuple(materialized[0])
        if any(tuple(row) != names for row in materialized):
            raise ValueError("Bulk metadata rows must use one consistent contract.")
        parameters = {
            f"{prefix}{index}_{name}": value
            for index, row in enumerate(materialized)
            for name, value in row.items()
        }
        source = " UNION ALL ".join(
            "SELECT "
            + ", ".join(f":{prefix}{index}_{name} AS {name}" for name in names)
            for index in range(len(materialized))
        )
        return names, source, parameters

    @staticmethod
    def _where_pairs(
        pairs: Iterable[Mapping[str, Any]], *, prefix: str
    ) -> tuple[str, Dict[str, Any]]:
        materialized = [dict(pair) for pair in pairs]
        if not materialized:
            raise ValueError("A bulk metadata lookup requires at least one key.")
        parameters: Dict[str, Any] = {}
        clauses = []
        for index, pair in enumerate(materialized):
            clause = []
            for name, value in pair.items():
                parameter = f"{prefix}{index}_{name}"
                parameters[parameter] = value
                clause.append(f"{name} = :{parameter}")
            clauses.append("(" + " AND ".join(clause) + ")")
        return " OR ".join(clauses), parameters

    def table(self, table_name: str) -> str:
        if table_name not in METADATA_TABLES:
            raise ValueError(f"Unsupported metadata table: {table_name!r}")
        if self.context.platform == "databricks":
            return f"`{self.context.namespace}`.`{self.context.schema}`.`{table_name}`"
        return f'"{self.context.namespace.upper()}"."{self.context.schema.upper()}"."{table_name.upper()}"'

    def bootstrap(self) -> None:
        for statement in split_sql_statements(render_ddl(self.context)):
            self.execute(statement)
        self.preflight()

    def preflight(self) -> None:
        if self.context.platform == "databricks":
            namespace = validate_identifier(self.context.namespace, label="metadata catalog")
            rows = self.query(
                f"SELECT table_name, column_name FROM `{namespace}`.information_schema.columns "
                "WHERE table_schema = :schema",
                {"schema": self.context.schema},
            )
        else:
            namespace = validate_identifier(self.context.namespace, label="metadata database")
            rows = self.query(
                f'SELECT table_name, column_name FROM "{namespace.upper()}".information_schema.columns '
                "WHERE table_schema = :schema",
                {"schema": self.context.schema.upper()},
            )
        actual: Dict[str, set[str]] = {}
        for row in rows:
            table_name = str(row.get("table_name") or "").lower()
            column_name = str(row.get("column_name") or "").lower()
            if table_name and column_name:
                actual.setdefault(table_name, set()).add(column_name)
        validate_schema_columns(actual)

    def get_source_system(self, source_system_id: int) -> Optional[Dict[str, Any]]:
        rows = self.query(
            f"SELECT * FROM {self.table('cfg_source_system')} WHERE source_system_id = :source_system_id",
            {"source_system_id": int(source_system_id)},
        )
        return rows[0] if rows else None

    def get_source_system_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        rows = self.query(
            f"SELECT * FROM {self.table('cfg_source_system')} WHERE LOWER(source_system_name) = :name",
            {"name": str(name).strip().lower()},
        )
        return rows[0] if rows else None

    def upsert_source_system(self, payload: Mapping[str, Any]) -> Dict[str, Any]:
        name = str(payload.get("source_system_name") or "").strip()
        if not name:
            raise ValueError("source_system_name is required.")
        source_system_id = int(payload.get("source_system_id") or stable_bigint("source_system", name))
        existing = self.get_source_system(source_system_id)
        existing_name = self.get_source_system_by_name(name)
        if existing and str(existing.get("source_system_name") or "").casefold() != name.casefold():
            raise RuntimeError("Deterministic source_system_id collision detected.")
        if existing_name and int(existing_name.get("source_system_id") or 0) != source_system_id:
            raise ValueError("source_system_name is already registered with another ID.")

        values = {
            "source_system_id": source_system_id,
            "source_system_name": name,
            "business_domain": payload.get("business_domain"),
            "owner_name": payload.get("owner_name"),
            "owner_email": payload.get("owner_email"),
            "description": payload.get("description"),
            "active_flag": _as_bool(payload.get("active_flag", True)),
        }
        self.execute(
            f"""
            MERGE INTO {self.table('cfg_source_system')} AS target
            USING (SELECT
                :source_system_id AS source_system_id,
                :source_system_name AS source_system_name,
                :business_domain AS business_domain,
                :owner_name AS owner_name,
                :owner_email AS owner_email,
                :description AS description,
                :active_flag AS active_flag
            ) AS source
            ON target.source_system_id = source.source_system_id
            WHEN MATCHED AND LOWER(target.source_system_name) = LOWER(source.source_system_name) THEN UPDATE SET
                source_system_name = source.source_system_name,
                business_domain = source.business_domain,
                owner_name = source.owner_name,
                owner_email = source.owner_email,
                description = source.description,
                active_flag = source.active_flag,
                updated_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT (
                source_system_id, source_system_name, business_domain, owner_name,
                owner_email, description, active_flag, created_at, updated_at
            ) VALUES (
                source.source_system_id, source.source_system_name, source.business_domain,
                source.owner_name, source.owner_email, source.description, source.active_flag,
                CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
            )
            """,
            values,
        )
        saved = self.get_source_system(source_system_id)
        if not saved:
            raise RuntimeError("Source-system upsert did not produce a readable row.")
        if str(saved.get("source_system_name") or "").casefold() != name.casefold():
            raise RuntimeError("Deterministic source_system_id collision detected.")
        return saved

    def get_connection(self, connection_id: int, config_version: Optional[int] = None) -> Optional[Dict[str, Any]]:
        version_filter = " AND config_version = :config_version" if config_version is not None else (
            " AND active_flag = :active_flag AND is_current = :is_current"
        )
        order_by = " ORDER BY config_version DESC" if config_version is None else ""
        parameters: Dict[str, Any] = {"connection_id": int(connection_id)}
        if config_version is not None:
            parameters["config_version"] = int(config_version)
        else:
            parameters.update({"active_flag": True, "is_current": True})
        rows = self.query(
            f"SELECT * FROM {self.table('cfg_connection')} "
            f"WHERE connection_id = :connection_id{version_filter}{order_by}",
            parameters,
        )
        if len(rows) > 1:
            raise RuntimeError("Duplicate connection versions were found.")
        return rows[0] if rows else None

    def get_connections(
        self, references: Iterable[Mapping[str, Any]]
    ) -> Dict[tuple[int, int], Dict[str, Any]]:
        keys = [
            {"connection_id": int(item["connection_id"]), "config_version": int(item["config_version"])}
            for item in references
        ]
        if not keys:
            return {}
        expected = {(item["connection_id"], item["config_version"]) for item in keys}
        if len(expected) != len(keys):
            raise ValueError("Bulk connection references must be unique.")
        where, parameters = self._where_pairs(keys, prefix="connection")
        rows = self.query(f"SELECT * FROM {self.table('cfg_connection')} WHERE {where}", parameters)
        result = {
            (int(row.get("connection_id") or 0), int(row.get("config_version") or 0)): row
            for row in rows
        }
        if len(result) != len(rows) or set(result) != expected:
            raise ValueError("One or more exact connection versions were not found.")
        return result

    def get_active_connection(self, connection_id: int) -> Optional[Dict[str, Any]]:
        rows = self.query(
            f"SELECT * FROM {self.table('cfg_connection')} "
            "WHERE connection_id = :connection_id AND active_flag = :active_flag "
            "AND is_current = :is_current ORDER BY config_version DESC",
            {"connection_id": int(connection_id), "active_flag": True, "is_current": True},
        )
        if len(rows) > 1:
            raise RuntimeError("Multiple active/current connection versions were found.")
        return rows[0] if rows else None

    def get_latest_connection(self, connection_id: int) -> Optional[Dict[str, Any]]:
        rows = self.query(
            f"SELECT * FROM {self.table('cfg_connection')} "
            "WHERE connection_id = :connection_id ORDER BY config_version DESC LIMIT 1",
            {"connection_id": int(connection_id)},
        )
        return rows[0] if rows else None

    def get_ingestion_object(self, ingestion_object_id: int, config_version: int) -> Optional[Dict[str, Any]]:
        rows = self.query(
            f"SELECT * FROM {self.table('cfg_ingestion_object')} "
            "WHERE ingestion_object_id = :ingestion_object_id AND config_version = :config_version",
            {"ingestion_object_id": int(ingestion_object_id), "config_version": int(config_version)},
        )
        if len(rows) > 1:
            raise RuntimeError("Duplicate ingestion-object versions were found.")
        return rows[0] if rows else None

    def get_active_ingestion_object(self, ingestion_object_id: int) -> Optional[Dict[str, Any]]:
        rows = self.query(
            f"SELECT * FROM {self.table('cfg_ingestion_object')} "
            "WHERE ingestion_object_id = :ingestion_object_id AND active_flag = :active_flag "
            "AND is_current = :is_current ORDER BY config_version DESC",
            {"ingestion_object_id": int(ingestion_object_id), "active_flag": True, "is_current": True},
        )
        if len(rows) > 1:
            raise RuntimeError("Multiple active/current ingestion-object versions were found.")
        return rows[0] if rows else None

    def get_ingestion_objects(
        self, references: Iterable[Mapping[str, Any]], *, require_active: Optional[bool] = None
    ) -> Dict[tuple[int, int], Dict[str, Any]]:
        keys = [
            {
                "ingestion_object_id": int(reference["ingestion_object_id"]),
                "config_version": int(reference["config_version"]),
            }
            for reference in references
        ]
        if not keys:
            return {}
        expected = {(item["ingestion_object_id"], item["config_version"]) for item in keys}
        if len(expected) != len(keys):
            raise ValueError("Bulk ingestion-object references must be unique.")
        where, parameters = self._where_pairs(keys, prefix="obj")
        lifecycle = ""
        if require_active is not None:
            lifecycle = " AND active_flag = :active_flag AND is_current = :is_current"
            parameters.update({"active_flag": require_active, "is_current": require_active})
        rows = self.query(
            f"SELECT * FROM {self.table('cfg_ingestion_object')} WHERE ({where}){lifecycle}",
            parameters,
        )
        result: Dict[tuple[int, int], Dict[str, Any]] = {}
        for row in rows:
            key = (int(row.get("ingestion_object_id") or 0), int(row.get("config_version") or 0))
            if key in result:
                raise RuntimeError("Duplicate ingestion-object versions were found.")
            result[key] = row
        if set(result) != expected:
            raise ValueError("One or more exact ingestion-object versions were not found.")
        return result

    def get_active_ingestion_objects(
        self, ingestion_object_ids: Iterable[int]
    ) -> Dict[int, Dict[str, Any]]:
        object_ids = [int(value) for value in ingestion_object_ids]
        if not object_ids:
            return {}
        if len(set(object_ids)) != len(object_ids):
            raise ValueError("Bulk active ingestion-object identifiers must be unique.")
        parameters = {f"object_id_{index}": value for index, value in enumerate(object_ids)}
        rows = self.query(
            f"SELECT * FROM {self.table('cfg_ingestion_object')} WHERE active_flag = :active_flag "
            "AND is_current = :is_current AND ingestion_object_id IN ("
            + ", ".join(f":object_id_{index}" for index in range(len(object_ids)))
            + ")",
            {**parameters, "active_flag": True, "is_current": True},
        )
        result: Dict[int, Dict[str, Any]] = {}
        for row in rows:
            object_id = int(row.get("ingestion_object_id") or 0)
            if object_id in result:
                raise RuntimeError("Multiple active/current ingestion-object versions were found.")
            result[object_id] = row
        if set(result) != set(object_ids):
            raise ValueError("One or more active ingestion objects were not found.")
        return result

    def upsert_connection_draft(self, payload: Mapping[str, Any]) -> Dict[str, Any]:
        normalized = validate_jdbc_connection(payload)
        source_system_id = int(normalized["source_system_id"])
        source_system = self.get_source_system(source_system_id)
        if not source_system or not _as_bool(source_system.get("active_flag")):
            raise ValueError("An active cfg_source_system row is required before connection onboarding.")

        name = str(normalized["connection_name"]).strip()
        connection_id = int(normalized.get("connection_id") or stable_bigint("connection", source_system_id, name))
        config_version = int(normalized.get("config_version") or 1)
        if config_version < 1:
            raise ValueError("config_version must be a positive integer.")
        values = {
            **normalized,
            "connection_id": connection_id,
            "config_version": config_version,
            "is_current": False,
            "active_flag": False,
        }
        columns = (
            "connection_id", "source_system_id", "connection_name", "connection_type",
            "connection_contract_name", "connection_schema_version", "host_name", "port",
            "base_path", "base_url", "database_name", "auth_type", "secret_scope", "secret_key",
            "secrets_json", "config_json", "config_hash", "config_version", "effective_from",
            "effective_to", "is_current", "active_flag",
        )
        self.execute(
            f"""
            MERGE INTO {self.table('cfg_connection')} AS target
            USING (SELECT {', '.join(':' + column + ' AS ' + column for column in columns)}) AS source
            ON target.connection_id = source.connection_id
               AND target.config_version = source.config_version
            WHEN NOT MATCHED THEN INSERT (
                {', '.join(columns)}, created_at, updated_at
            ) VALUES (
                {', '.join('source.' + column for column in columns)},
                CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
            )
            """,
            {column: values.get(column) for column in columns},
        )
        saved = self.get_connection(connection_id, config_version)
        if not saved:
            raise RuntimeError("Connection draft insert did not produce a readable row.")
        if str(saved.get("config_hash") or "") != normalized["config_hash"]:
            raise ValueError("An existing connection version is immutable; increment config_version.")
        return saved

    def validate_and_activate_connection(
        self,
        connection_id: int,
        config_version: int,
        validator: Callable[[Mapping[str, Any]], None],
    ) -> Dict[str, Any]:
        draft = self.get_connection(connection_id, config_version)
        if not draft:
            raise ValueError("Connection draft was not found.")
        if draft.get("effective_from") and not (
            _as_bool(draft.get("active_flag")) and _as_bool(draft.get("is_current"))
        ):
            raise ValueError("An expired connection version cannot be reactivated.")
        source_system = self.get_source_system(int(draft.get("source_system_id") or 0))
        if not source_system or not _as_bool(source_system.get("active_flag")):
            raise ValueError("The connection's source system is inactive.")
        validator(draft)
        self.execute(
            f"""
            UPDATE {self.table('cfg_connection')}
            SET is_current = CASE WHEN config_version = :config_version THEN :enabled ELSE :disabled END,
                active_flag = CASE WHEN config_version = :config_version THEN :enabled ELSE :disabled END,
                effective_from = CASE
                    WHEN config_version = :config_version THEN COALESCE(effective_from, CURRENT_TIMESTAMP())
                    ELSE effective_from
                END,
                effective_to = CASE
                    WHEN config_version = :config_version THEN NULL
                    ELSE CURRENT_TIMESTAMP()
                END,
                updated_at = CURRENT_TIMESTAMP()
            WHERE connection_id = :connection_id
              AND (config_version = :config_version OR is_current = :enabled)
            """,
            {
                "connection_id": int(connection_id),
                "config_version": int(config_version),
                "enabled": True,
                "disabled": False,
            },
        )
        active = self.get_active_connection(connection_id)
        if not active or int(active.get("config_version") or 0) != int(config_version):
            raise RuntimeError("Connection activation postcondition failed.")
        if str(active.get("config_hash") or "") != str(draft.get("config_hash") or ""):
            raise RuntimeError("Connection activation changed the validated configuration.")
        return active

    def upsert_database_ingestion_object_draft(
        self,
        *,
        source_system_id: int,
        connection_id: int,
        table: Mapping[str, Any],
        config_version: Optional[int] = None,
        expected_connection_version: Optional[int] = None,
        expected_connection_hash: Optional[str] = None,
        target_bronze_table: Optional[str] = None,
        allow_inactive_connection: bool = False,
    ) -> Dict[str, Any]:
        source_system = self.get_source_system(source_system_id)
        connection = self.get_active_connection(connection_id)
        if not connection and allow_inactive_connection:
            connection = self.get_latest_connection(connection_id)
        if not source_system or not _as_bool(source_system.get("active_flag")):
            raise ValueError("The selected source system is missing or inactive.")
        if not connection:
            raise ValueError("The selected connection is missing, inactive, or not current.")
        if int(connection.get("source_system_id") or 0) != int(source_system_id):
            raise ValueError("The selected connection belongs to a different source system.")
        if str(connection.get("connection_type") or "").upper() != "JDBC":
            raise ValueError("Database ingestion requires a JDBC connection.")
        if expected_connection_version is not None and int(connection.get("config_version") or 0) != int(
            expected_connection_version
        ):
            raise ValueError("The active connection version changed after table nomination.")
        if expected_connection_hash and str(connection.get("config_hash") or "") != str(expected_connection_hash):
            raise ValueError("The active connection configuration changed after table nomination.")

        database = str(table.get("database_name") or connection.get("database_name") or "").strip()
        schema = str(table.get("schema_name") or table.get("database_schema") or "").strip()
        table_name = str(table.get("table_name") or table.get("table") or "").strip()
        if not database or not schema or not table_name:
            raise ValueError("Approved database tables require database_name, schema_name, and table_name.")
        validate_identifier(database, label="source database")
        validate_identifier(schema, label="source schema")
        validate_identifier(table_name, label="source table")
        if database.casefold() != str(connection.get("database_name") or "").strip().casefold():
            raise ValueError("The approved table database does not match the selected connection.")
        ingestion_object_id = int(
            stable_bigint(
                "ingestion_object",
                source_system_id,
                connection_id,
                database,
                schema,
                table_name,
                "SOURCE_TO_BRONZE",
            )
        )
        executable = {
            "source_system_id": int(source_system_id),
            "connection_id": int(connection_id),
            "database_name": database,
            "database_schema": schema,
            "table_name": table_name,
            "processing_stage": "SOURCE_TO_BRONZE",
            "load_type": "FULL",
            "write_mode": "APPEND",
            "target_bronze_table": str(target_bronze_table or "").strip(),
        }
        config_hash = canonical_json_hash(executable)
        if config_version is None:
            config_version = ((int(config_hash.removeprefix("sha256:")[:8], 16) & ((1 << 29) - 1)) * 2) + 1
        if int(config_version) < 1 or int(config_version) % 2 == 0:
            raise ValueError("Draft config_version must be a positive odd content version; executable versions are even.")
        values = {
            "source_system_id": executable["source_system_id"],
            "connection_id": executable["connection_id"],
            "database_schema": executable["database_schema"],
            "table_name": executable["table_name"],
            "processing_stage": executable["processing_stage"],
            "load_type": executable["load_type"],
            "ingestion_object_id": ingestion_object_id,
            "object_kind": "INGESTION",
            "ingestion_type": "DATABASE",
            "source_layer": "SOURCE",
            "target_layer": "BRONZE",
            "object_name": f"{database}.{schema}.{table_name}",
            "object_type": "TABLE",
            "source_resource_type": "TABLE",
            "container_format": "NONE",
            "target_bronze_table": executable["target_bronze_table"],
            "write_mode": executable["write_mode"],
            "config_hash": config_hash,
            "config_version": int(config_version),
            "is_current": False,
            "active_flag": False,
        }
        target_collisions = self.query(
            f"SELECT ingestion_object_id FROM {self.table('cfg_ingestion_object')} "
            "WHERE LOWER(target_bronze_table) = :target_bronze_table "
            "AND ingestion_object_id <> :ingestion_object_id",
            {
                "target_bronze_table": str(values["target_bronze_table"]).casefold(),
                "ingestion_object_id": ingestion_object_id,
            },
        )
        if target_collisions:
            raise ValueError("The derived Bronze target is already assigned to another ingestion object.")
        columns = tuple(values)
        self.execute(
            f"""
            MERGE INTO {self.table('cfg_ingestion_object')} AS target
            USING (SELECT {', '.join(':' + column + ' AS ' + column for column in columns)}) AS source
            ON target.ingestion_object_id = source.ingestion_object_id
               AND target.config_version = source.config_version
            WHEN NOT MATCHED THEN INSERT (
                {', '.join(columns)}, created_at, updated_at
            ) VALUES (
                {', '.join('source.' + column for column in columns)},
                CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
            )
            """,
            values,
        )
        rows = self.query(
            f"SELECT * FROM {self.table('cfg_ingestion_object')} "
            "WHERE ingestion_object_id = :ingestion_object_id AND config_version = :config_version",
            {"ingestion_object_id": ingestion_object_id, "config_version": int(config_version)},
        )
        if len(rows) != 1:
            if not rows:
                raise RuntimeError("Ingestion-object draft upsert did not produce a readable row.")
            raise RuntimeError("Duplicate ingestion-object versions were found.")
        saved = rows[0]
        if not saved:
            raise RuntimeError("Ingestion-object draft upsert did not produce a readable row.")
        if str(saved.get("config_hash") or "") != values["config_hash"]:
            raise ValueError("An existing ingestion-object version is immutable; increment config_version.")
        return saved

    def upsert_database_ingestion_object_drafts(
        self,
        *,
        source_system_id: int,
        connection_id: int,
        requests: Iterable[Mapping[str, Any]],
        expected_connection_version: Optional[int] = None,
        expected_connection_hash: Optional[str] = None,
        allow_inactive_connection: bool = False,
    ) -> List[Dict[str, Any]]:
        requested = [dict(item) for item in requests]
        if not requested:
            return []
        source_system = self.get_source_system(source_system_id)
        connection = self.get_active_connection(connection_id)
        if not connection and allow_inactive_connection:
            connection = self.get_latest_connection(connection_id)
        if not source_system or not _as_bool(source_system.get("active_flag")):
            raise ValueError("The selected source system is missing or inactive.")
        if not connection:
            raise ValueError("The selected connection is missing, inactive, or not current.")
        if int(connection.get("source_system_id") or 0) != int(source_system_id):
            raise ValueError("The selected connection belongs to a different source system.")
        if str(connection.get("connection_type") or "").upper() != "JDBC":
            raise ValueError("Database ingestion requires a JDBC connection.")
        if expected_connection_version is not None and int(connection.get("config_version") or 0) != int(expected_connection_version):
            raise ValueError("The active connection version changed after table nomination.")
        if expected_connection_hash and str(connection.get("config_hash") or "") != str(expected_connection_hash):
            raise ValueError("The active connection configuration changed after table nomination.")

        rows = []
        for request in requested:
            table = dict(request.get("table") or {})
            database = str(table.get("database_name") or connection.get("database_name") or "").strip()
            schema = str(table.get("schema_name") or table.get("database_schema") or "").strip()
            table_name = str(table.get("table_name") or table.get("table") or "").strip()
            if not database or not schema or not table_name:
                raise ValueError("Approved database tables require database_name, schema_name, and table_name.")
            validate_identifier(database, label="source database")
            validate_identifier(schema, label="source schema")
            validate_identifier(table_name, label="source table")
            if database.casefold() != str(connection.get("database_name") or "").strip().casefold():
                raise ValueError("The approved table database does not match the selected connection.")
            object_id = stable_bigint(
                "ingestion_object", source_system_id, connection_id, database, schema, table_name, "SOURCE_TO_BRONZE"
            )
            executable = {
                "source_system_id": int(source_system_id),
                "connection_id": int(connection_id),
                "database_name": database,
                "database_schema": schema,
                "table_name": table_name,
                "processing_stage": "SOURCE_TO_BRONZE",
                "load_type": "FULL",
                "write_mode": "APPEND",
                "target_bronze_table": str(request.get("target_bronze_table") or "").strip(),
            }
            config_hash = canonical_json_hash(executable)
            config_version = request.get("config_version")
            if config_version is None:
                config_version = ((int(config_hash.removeprefix("sha256:")[:8], 16) & ((1 << 29) - 1)) * 2) + 1
            if int(config_version) < 1 or int(config_version) % 2 == 0:
                raise ValueError("Draft config_version must be a positive odd content version; executable versions are even.")
            rows.append({
                "source_system_id": int(source_system_id),
                "connection_id": int(connection_id),
                "database_schema": schema,
                "table_name": table_name,
                "processing_stage": "SOURCE_TO_BRONZE",
                "load_type": "FULL",
                "ingestion_object_id": int(object_id),
                "object_kind": "INGESTION",
                "ingestion_type": "DATABASE",
                "source_layer": "SOURCE",
                "target_layer": "BRONZE",
                "object_name": f"{database}.{schema}.{table_name}",
                "object_type": "TABLE",
                "source_resource_type": "TABLE",
                "container_format": "NONE",
                "target_bronze_table": executable["target_bronze_table"],
                "write_mode": "APPEND",
                "config_hash": config_hash,
                "config_version": int(config_version),
                "is_current": False,
                "active_flag": False,
            })
        identities = {(row["ingestion_object_id"], row["config_version"]) for row in rows}
        targets = {str(row["target_bronze_table"]).casefold(): row["ingestion_object_id"] for row in rows}
        if len(identities) != len(rows) or len(targets) != len(rows):
            raise ValueError("Approved database tables contain duplicate object or Bronze target identities.")
        target_parameters = {f"target_{index}": target for index, target in enumerate(targets)}
        collisions = self.query(
            f"SELECT ingestion_object_id, target_bronze_table FROM {self.table('cfg_ingestion_object')} "
            "WHERE LOWER(target_bronze_table) IN ("
            + ", ".join(f":target_{index}" for index in range(len(targets)))
            + ")",
            target_parameters,
        )
        if any(
            targets.get(str(row.get("target_bronze_table") or "").casefold())
            != int(row.get("ingestion_object_id") or 0)
            for row in collisions
        ):
            raise ValueError("A derived Bronze target is already assigned to another ingestion object.")
        names, source, parameters = self._source_rows(rows, prefix="draft")
        self.execute(
            f"MERGE INTO {self.table('cfg_ingestion_object')} AS target USING ({source}) AS source "
            "ON target.ingestion_object_id = source.ingestion_object_id AND target.config_version = source.config_version "
            f"WHEN NOT MATCHED THEN INSERT ({', '.join(names)}, created_at, updated_at) VALUES ("
            + ", ".join("source." + name for name in names)
            + ", CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())",
            parameters,
        )
        persisted = self.get_ingestion_objects([
            {"ingestion_object_id": row["ingestion_object_id"], "config_version": row["config_version"]}
            for row in rows
        ], require_active=False)
        results = []
        for row in rows:
            saved = persisted[(row["ingestion_object_id"], row["config_version"])]
            if str(saved.get("config_hash") or "") != row["config_hash"]:
                raise ValueError("An existing ingestion-object version is immutable; increment config_version.")
            results.append(saved)
        return results

    def upsert_source_to_bronze_mapping_draft(
        self,
        *,
        ingestion_object: Mapping[str, Any],
        columns: Iterable[Mapping[str, Any]],
    ) -> Dict[str, Any]:
        ingestion_object_id = int(ingestion_object["ingestion_object_id"])
        object_config_version = int(ingestion_object.get("config_version") or 0)
        object_config_hash = str(ingestion_object.get("config_hash") or "").strip()
        if (
            object_config_version < 1
            or not object_config_hash
            or str(ingestion_object.get("object_kind") or "").upper() != "INGESTION"
            or str(ingestion_object.get("ingestion_type") or "").upper() != "DATABASE"
            or str(ingestion_object.get("processing_stage") or "").upper() != "SOURCE_TO_BRONZE"
            or _as_bool(ingestion_object.get("active_flag"))
            or _as_bool(ingestion_object.get("is_current"))
        ):
            raise ValueError("Source-to-Bronze mappings require an exact inactive database ingestion-object draft.")
        source_object = str(ingestion_object.get("object_name") or "").strip()
        target_table = str(ingestion_object.get("target_bronze_table") or "").strip()
        if not source_object or not target_table:
            raise ValueError("The ingestion-object draft requires source and Bronze target identities.")
        normalized = []
        for ordinal, column in enumerate(columns, start=1):
            source_field = str(column.get("column_name") or column.get("source_field_path") or "").strip()
            source_type = str(column.get("data_type_full") or column.get("data_type") or "").strip()
            if not source_field or not source_type:
                raise ValueError("Source-to-Bronze mappings require column name and source datatype.")
            ordinal_position = int(column.get("ordinal_position") or ordinal)
            if ordinal_position < 1:
                raise ValueError("Source-to-Bronze mapping ordinals must be positive.")
            normalized.append(
                {
                    "source_field_path": source_field,
                    "source_data_type": source_type,
                    "target_column_name": normalize_bronze_column_name(source_field),
                    "target_data_type": bronze_target_data_type(self.context.platform, column),
                    "is_nullable": bool(column.get("is_nullable", True)),
                    "is_array": False,
                    "is_primary_key": bool(column.get("is_primary_key")),
                    "ordinal_position": ordinal_position,
                }
            )
        normalized.sort(key=lambda item: (item["ordinal_position"], item["source_field_path"].casefold()))
        if not normalized:
            raise ValueError("At least one approved column is required for a mapping bundle.")
        reserved_targets = {"run_id", "ingestion_timestamp", "source_system", "source_table"}
        collisions = sorted(
            {item["target_column_name"] for item in normalized} & reserved_targets
        )
        if collisions:
            raise ValueError("Source-to-Bronze mappings collide with reserved lineage columns: " + ", ".join(collisions))
        for field in ("source_field_path", "target_column_name"):
            identities = [str(item[field]).casefold() for item in normalized]
            if len(identities) != len(set(identities)):
                raise ValueError(f"Source-to-Bronze mappings contain duplicate {field} values.")
        ordinals = [int(item["ordinal_position"]) for item in normalized]
        if len(ordinals) != len(set(ordinals)):
            raise ValueError("Source-to-Bronze mappings contain duplicate ordinal positions.")
        input_objects = json.dumps(
            [{
                "ingestion_object_id": ingestion_object_id,
                "config_version": object_config_version,
                "config_hash": object_config_hash,
            }],
            sort_keys=True,
            separators=(",", ":"),
        )
        contract = {
            "source_layer": "SOURCE",
            "target_layer": "BRONZE",
            "mapping_group": f"SOURCE_TO_BRONZE:{ingestion_object_id}:{object_config_version}",
            "input_objects_json": input_objects,
            "transformation_rule": "CAST",
            "transformation_language": "SQL" if self.context.platform == "snowflake" else "PYSPARK_EXPR",
        }
        bundle_hash = canonical_json_hash(
            {
                "ingestion_object_id": ingestion_object_id,
                "object_config_version": object_config_version,
                "object_config_hash": object_config_hash,
                "processing_stage": "SOURCE_TO_BRONZE",
                "source_object": source_object,
                "target_table": target_table,
                "contract": contract,
                "columns": normalized,
            }
        )
        mapping_version = int(bundle_hash.removeprefix("sha256:")[:8], 16) & ((1 << 31) - 1) or 1
        mapping_group = contract["mapping_group"]
        bundle_rows = []
        for column in normalized:
            bundle_rows.append({
                "mapping_id": stable_bigint(
                    "mapping",
                    ingestion_object_id,
                    "SOURCE_TO_BRONZE",
                    column["source_field_path"],
                    column["target_column_name"],
                ),
                "ingestion_object_id": ingestion_object_id,
                "processing_stage": "SOURCE_TO_BRONZE",
                "source_layer": "SOURCE",
                "target_layer": "BRONZE",
                "source_object_name": source_object,
                "target_object_name": target_table,
                "target_table": target_table,
                "mapping_group": mapping_group,
                "input_objects_json": input_objects,
                "build_order": 1,
                **column,
                "transformation_rule": contract["transformation_rule"],
                "transformation_language": contract["transformation_language"],
                "mapping_hash": bundle_hash,
                "mapping_version": mapping_version,
                "is_current": False,
                "active_flag": False,
            })
        names = tuple(bundle_rows[0])
        parameters = {
            f"r{row_index}_{name}": value
            for row_index, row in enumerate(bundle_rows)
            for name, value in row.items()
        }
        source_rows = " UNION ALL ".join(
            "SELECT " + ", ".join(
                f":r{row_index}_{name} AS {name}" for name in names
            )
            for row_index in range(len(bundle_rows))
        )
        statement = f"""
            MERGE INTO {self.table('cfg_mapping')} AS target
            USING ({source_rows}) AS source
            ON target.mapping_id = source.mapping_id AND target.mapping_version = source.mapping_version
            WHEN NOT MATCHED THEN INSERT (
                {', '.join(names)}, created_at, updated_at
            ) VALUES (
                {', '.join('source.' + name for name in names)}, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
            )
        """
        attempts = 3 if self.context.platform == "databricks" else 1
        for attempt in range(attempts):
            try:
                self.execute(statement, parameters)
                break
            except RuntimeError as exc:
                retryable = any(token in str(exc).casefold() for token in ("concurrent", "conflict", "429", "throttl"))
                if not retryable or attempt + 1 == attempts:
                    raise
                time.sleep(0.2 * (attempt + 1))
        return self.get_mapping_bundle(
            ingestion_object_id=ingestion_object_id,
            processing_stage="SOURCE_TO_BRONZE",
            mapping_version=mapping_version,
            expected_hash=bundle_hash,
            expected_target=target_table,
            require_active=None,
        )

    def get_mapping_bundle(
        self,
        *,
        ingestion_object_id: int,
        processing_stage: str,
        mapping_version: int,
        expected_hash: str,
        expected_target: str,
        require_active: Optional[bool],
    ) -> Dict[str, Any]:
        rows = self.query(
            f"SELECT * FROM {self.table('cfg_mapping')} "
            "WHERE ingestion_object_id = :ingestion_object_id AND processing_stage = :processing_stage "
            "AND mapping_version = :mapping_version ORDER BY ordinal_position, source_field_path",
            {
                "ingestion_object_id": int(ingestion_object_id),
                "processing_stage": str(processing_stage),
                "mapping_version": int(mapping_version),
            },
        )
        return self._validate_mapping_bundle_rows(
            rows,
            ingestion_object_id=ingestion_object_id,
            processing_stage=processing_stage,
            mapping_version=mapping_version,
            expected_hash=expected_hash,
            expected_target=expected_target,
            require_active=require_active,
        )

    def _validate_mapping_bundle_rows(
        self,
        rows: List[Dict[str, Any]],
        *,
        ingestion_object_id: int,
        processing_stage: str,
        mapping_version: int,
        expected_hash: str,
        expected_target: str,
        require_active: Optional[bool],
        transformations: Optional[Mapping[tuple[int, int], Mapping[str, Any]]] = None,
    ) -> Dict[str, Any]:
        if not rows:
            raise ValueError("The exact mapping bundle was not found.")
        # None accepts either consistent lifecycle state for idempotent design-time reuse.
        for row in rows:
            if (
                str(row.get("mapping_hash") or "") != str(expected_hash)
                or str(row.get("target_table") or "").casefold() != str(expected_target).casefold()
                or str(row.get("processing_stage") or "").upper() != str(processing_stage).upper()
            ):
                raise RuntimeError("The persisted mapping bundle does not match the pinned contract.")
        lifecycle_states = {
            (_as_bool(row.get("active_flag")), _as_bool(row.get("is_current")))
            for row in rows
        }
        if len(lifecycle_states) != 1 or any(active != current for active, current in lifecycle_states):
            raise RuntimeError("The persisted mapping bundle has an inconsistent lifecycle state.")
        active = next(iter(lifecycle_states))[0]
        if require_active is not None and active != require_active:
            raise RuntimeError("The persisted mapping bundle does not match the pinned contract.")
        stage = str(processing_stage).upper()
        if stage == "SOURCE_TO_BRONZE":
            try:
                pinned_object = json.loads(str(rows[0].get("input_objects_json") or "[]"))[0]
            except (IndexError, TypeError, ValueError, json.JSONDecodeError) as exc:
                raise RuntimeError("The mapping bundle has an invalid ingestion-object pin.") from exc
            canonical_columns = [
                {
                    "source_field_path": str(row.get("source_field_path") or ""),
                    "source_data_type": str(row.get("source_data_type") or ""),
                    "target_column_name": str(row.get("target_column_name") or ""),
                    "target_data_type": str(row.get("target_data_type") or ""),
                    "is_nullable": _as_bool(row.get("is_nullable")),
                    "is_array": _as_bool(row.get("is_array")),
                    "is_primary_key": _as_bool(row.get("is_primary_key")),
                    "ordinal_position": int(row.get("ordinal_position") or 0),
                }
                for row in rows
            ]
            recomputed_hash = canonical_json_hash(
                {
                    "ingestion_object_id": int(ingestion_object_id),
                    "object_config_version": int(pinned_object.get("config_version") or 0),
                    "object_config_hash": str(pinned_object.get("config_hash") or ""),
                    "processing_stage": "SOURCE_TO_BRONZE",
                    "source_object": str(rows[0].get("source_object_name") or ""),
                    "target_table": str(rows[0].get("target_table") or ""),
                    "contract": {
                        "source_layer": str(rows[0].get("source_layer") or ""),
                        "target_layer": str(rows[0].get("target_layer") or ""),
                        "mapping_group": str(rows[0].get("mapping_group") or ""),
                        "input_objects_json": str(rows[0].get("input_objects_json") or ""),
                        "transformation_rule": str(rows[0].get("transformation_rule") or ""),
                        "transformation_language": str(rows[0].get("transformation_language") or ""),
                    },
                    "columns": canonical_columns,
                }
            )
            if recomputed_hash != str(expected_hash):
                raise RuntimeError("The persisted mapping bundle failed its content-hash check.")
        elif stage == "BRONZE_TO_SILVER":
            try:
                config_version = int(str(rows[0].get("mapping_group") or "").rsplit(":", 1)[-1])
                transformation = (transformations or {}).get((int(ingestion_object_id), config_version))
                if transformation is None:
                    transformation = self.get_ingestion_object(ingestion_object_id, config_version)
                input_objects = json.loads(str(rows[0].get("input_objects_json") or "[]"))
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise RuntimeError("The Bronze-to-Silver mapping bundle has invalid version pins.") from exc
            if (
                not transformation
                or str(transformation.get("processing_stage") or "").upper() != stage
                or str(transformation.get("target_table") or "").casefold() != str(expected_target).casefold()
                or len(input_objects) != 1
            ):
                raise RuntimeError("The Bronze-to-Silver mapping bundle has invalid object pins.")
            canonical_columns = [
                {
                    "source_field_path": str(row.get("source_field_path") or ""),
                    "source_data_type": str(row.get("source_data_type") or ""),
                    "target_column_name": str(row.get("target_column_name") or ""),
                    "target_data_type": str(row.get("target_data_type") or ""),
                    "is_nullable": _as_bool(row.get("is_nullable")),
                    "ordinal_position": int(row.get("ordinal_position") or 0),
                    "transformation_rule": str(row.get("transformation_rule") or ""),
                }
                for row in rows
            ]
            recomputed_hash = canonical_json_hash(
                {
                    "ingestion_object_id": int(ingestion_object_id),
                    "object_config_version": config_version,
                    "object_config_hash": str(transformation.get("config_hash") or ""),
                    "processing_stage": stage,
                    "source_object": str(rows[0].get("source_object_name") or ""),
                    "target_table": str(rows[0].get("target_table") or ""),
                    "input_objects_json": str(rows[0].get("input_objects_json") or ""),
                    "merge_keys": [
                        str(row.get("target_column_name") or "")
                        for row in rows
                        if _as_bool(row.get("is_primary_key"))
                    ],
                    "columns": canonical_columns,
                }
            )
            if recomputed_hash != str(expected_hash):
                raise RuntimeError("The persisted Bronze-to-Silver bundle failed its content-hash check.")
        elif stage == "SILVER_TO_GOLD":
            try:
                config_version = int(str(rows[0].get("mapping_group") or "").rsplit(":", 1)[-1])
                transformation = (transformations or {}).get((int(ingestion_object_id), config_version))
                if transformation is None:
                    transformation = self.get_ingestion_object(ingestion_object_id, config_version)
                input_objects = json.loads(str(rows[0].get("input_objects_json") or "[]"))
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise RuntimeError("The Silver-to-Gold mapping bundle has invalid version pins.") from exc
            if (
                not transformation
                or str(transformation.get("processing_stage") or "").upper() != stage
                or str(transformation.get("target_table") or "").casefold() != str(expected_target).casefold()
                or not input_objects
            ):
                raise RuntimeError("The Silver-to-Gold mapping bundle has invalid object pins.")
            canonical_columns = [
                {
                    "source_object_name": str(row.get("source_object_name") or ""),
                    "source_field_path": str(row.get("source_field_path") or ""),
                    "source_data_type": str(row.get("source_data_type") or ""),
                    "target_column_name": str(row.get("target_column_name") or ""),
                    "target_data_type": str(row.get("target_data_type") or ""),
                    "is_nullable": _as_bool(row.get("is_nullable")),
                    "is_primary_key": _as_bool(row.get("is_primary_key")),
                    "ordinal_position": int(row.get("ordinal_position") or 0),
                    "transformation_rule": str(row.get("transformation_rule") or ""),
                }
                for row in rows
            ]
            recomputed_hash = canonical_json_hash({
                "ingestion_object_id": int(ingestion_object_id),
                "object_config_version": config_version,
                "object_config_hash": str(transformation.get("config_hash") or ""),
                "processing_stage": stage,
                "target_table": str(rows[0].get("target_table") or ""),
                "input_objects_json": str(rows[0].get("input_objects_json") or ""),
                "join_rules_json": str(rows[0].get("join_rules_json") or ""),
                "aggregation_rules_json": str(rows[0].get("aggregation_rules_json") or ""),
                "build_order": int(rows[0].get("build_order") or 0),
                "columns": canonical_columns,
            })
            if recomputed_hash != str(expected_hash):
                raise RuntimeError("The persisted Silver-to-Gold bundle failed its content-hash check.")
        source_names = [str(row.get("source_field_path") or "").casefold() for row in rows]
        # A Gold source field may legitimately feed both a grain column and an aggregate.
        source_identities = source_names if stage != "SILVER_TO_GOLD" else []
        target_names = [str(row.get("target_column_name") or "").casefold() for row in rows]
        ordinals = [int(row.get("ordinal_position") or 0) for row in rows]
        common_fields = [
            "source_layer", "target_layer", "target_object_name", "target_table",
            "mapping_group", "input_objects_json",
        ]
        if stage != "SILVER_TO_GOLD":
            common_fields.append("source_object_name")
        else:
            common_fields.extend(("join_rules_json", "aggregation_rules_json", "build_order"))
        if (
            len(source_identities) != len(set(source_identities))
            or len(target_names) != len(set(target_names))
            or len(ordinals) != len(set(ordinals))
            or any(ordinal < 1 for ordinal in ordinals)
            or any(any(str(row.get(field) or "") != str(rows[0].get(field) or "") for field in common_fields) for row in rows)
        ):
            raise RuntimeError("The persisted mapping bundle contains duplicate or invalid fields.")
        return {
            "ingestion_object_id": int(ingestion_object_id),
            "processing_stage": str(processing_stage),
            "mapping_version": int(mapping_version),
            "mapping_hash": str(expected_hash),
            "active_flag": active,
            "mappings": rows,
        }

    def get_mapping_bundles(
        self, references: Iterable[Mapping[str, Any]]
    ) -> Dict[tuple[int, str, int], Dict[str, Any]]:
        requested = [dict(reference) for reference in references]
        if not requested:
            return {}
        keys = [
            {
                "ingestion_object_id": int(item["ingestion_object_id"]),
                "processing_stage": str(item["processing_stage"]).upper(),
                "mapping_version": int(item["mapping_version"]),
            }
            for item in requested
        ]
        expected_keys = {
            (item["ingestion_object_id"], item["processing_stage"], item["mapping_version"])
            for item in keys
        }
        if len(expected_keys) != len(keys):
            raise ValueError("Bulk mapping-bundle references must be unique.")
        where, parameters = self._where_pairs(keys, prefix="mapping")
        rows = self.query(
            f"SELECT * FROM {self.table('cfg_mapping')} WHERE {where} "
            "ORDER BY ingestion_object_id, processing_stage, mapping_version, ordinal_position, source_field_path",
            parameters,
        )
        grouped: Dict[tuple[int, str, int], List[Dict[str, Any]]] = {}
        for row in rows:
            key = (
                int(row.get("ingestion_object_id") or 0),
                str(row.get("processing_stage") or "").upper(),
                int(row.get("mapping_version") or 0),
            )
            grouped.setdefault(key, []).append(row)
        if set(grouped) != expected_keys:
            raise ValueError("One or more exact mapping bundles were not found.")

        transformation_refs = []
        for key, bundle_rows in grouped.items():
            if key[1] == "SOURCE_TO_BRONZE":
                continue
            try:
                config_version = int(str(bundle_rows[0].get("mapping_group") or "").rsplit(":", 1)[-1])
            except ValueError as exc:
                raise RuntimeError("A transformation mapping bundle has an invalid object pin.") from exc
            transformation_refs.append(
                {"ingestion_object_id": key[0], "config_version": config_version}
            )
        unique_transformation_refs = {
            (item["ingestion_object_id"], item["config_version"]): item
            for item in transformation_refs
        }
        transformations = (
            self.get_ingestion_objects(unique_transformation_refs.values())
            if unique_transformation_refs
            else {}
        )
        result: Dict[tuple[int, str, int], Dict[str, Any]] = {}
        for item in requested:
            key = (
                int(item["ingestion_object_id"]),
                str(item["processing_stage"]).upper(),
                int(item["mapping_version"]),
            )
            result[key] = self._validate_mapping_bundle_rows(
                grouped[key],
                ingestion_object_id=key[0],
                processing_stage=key[1],
                mapping_version=key[2],
                expected_hash=str(item["expected_hash"]),
                expected_target=str(item["expected_target"]),
                require_active=item.get("require_active"),
                transformations=transformations,
            )
        return result

    def upsert_bronze_to_silver_draft(
        self,
        *,
        source_system_id: int,
        source_object: Mapping[str, Any],
        source_mapping: Mapping[str, Any],
        target_silver_table: str,
        merge_keys: Iterable[str],
        columns: Iterable[Mapping[str, Any]],
    ) -> Dict[str, Any]:
        source_object_id = int(source_object.get("ingestion_object_id") or 0)
        persisted_source = self.get_active_ingestion_object(source_object_id)
        if not persisted_source:
            raise ValueError("Bronze-to-Silver requires the active Bronze ingestion object.")
        source_target = str(persisted_source.get("target_bronze_table") or "").strip()
        target_table = str(target_silver_table or "").strip()
        keys = [normalize_bronze_column_name(key) for key in merge_keys if str(key or "").strip()]
        if not source_target or not target_table or not keys:
            raise ValueError("Bronze-to-Silver requires a Bronze source, Silver target, and reviewed merge keys.")
        if len(keys) != len(set(keys)):
            raise ValueError("Silver merge keys must be unique.")
        if int(persisted_source.get("source_system_id") or 0) != int(source_system_id):
            raise ValueError("The active Bronze object belongs to a different source system.")
        persisted_mapping = self.get_mapping_bundle(
            ingestion_object_id=source_object_id,
            processing_stage="SOURCE_TO_BRONZE",
            mapping_version=int(source_mapping.get("mapping_version") or 0),
            expected_hash=str(source_mapping.get("mapping_hash") or ""),
            expected_target=source_target,
            require_active=True,
        )
        target_parts = target_table.split(".")
        if len(target_parts) not in {2, 3}:
            raise ValueError("Silver target must be schema.table or catalog.schema.table.")
        for part in target_parts:
            validate_identifier(part, label="Silver target identifier")

        normalized_columns = sorted(
            [{
                "source_field_path": str(column.get("source_field_path") or ""),
                "source_data_type": str(column.get("source_data_type") or ""),
                "target_column_name": normalize_bronze_column_name(column.get("target_column_name")),
                "target_data_type": str(column.get("target_data_type") or ""),
                "is_nullable": bool(column.get("is_nullable", True)),
                "ordinal_position": int(column.get("ordinal_position") or 0),
                "transformation_rule": str(column.get("transformation_rule") or "CAST").upper(),
            } for column in columns],
            key=lambda item: (int(item.get("ordinal_position") or 0), str(item.get("source_field_path") or "")),
        )
        for column in normalized_columns:
            if column["target_column_name"] in keys:
                column["is_nullable"] = False
        if not normalized_columns:
            raise ValueError("Bronze-to-Silver requires at least one approved column mapping.")
        source_names = [column["source_field_path"].casefold() for column in normalized_columns]
        target_names = [column["target_column_name"].casefold() for column in normalized_columns]
        ordinals = [column["ordinal_position"] for column in normalized_columns]
        if (
            any(not name for name in source_names + target_names)
            or any(not column["source_data_type"] or not column["target_data_type"] for column in normalized_columns)
            or any(column["transformation_rule"] not in {"IDENTITY", "CAST", "TRIM_CAST"} for column in normalized_columns)
            or len(source_names) != len(set(source_names))
            or len(target_names) != len(set(target_names))
            or len(ordinals) != len(set(ordinals))
            or any(ordinal < 1 for ordinal in ordinals)
        ):
            raise ValueError("Bronze-to-Silver mappings contain missing, duplicate, or invalid columns.")
        if not set(keys).issubset(set(target_names)):
            raise ValueError("Every reviewed Silver merge key must be present in the mapping bundle.")
        active_source_columns = {
            str(row.get("target_column_name") or "").casefold(): str(row.get("target_data_type") or "")
            for row in persisted_mapping["mappings"]
        }
        if any(
            column["source_field_path"].casefold() not in active_source_columns
            or active_source_columns[column["source_field_path"].casefold()].casefold()
            != column["source_data_type"].casefold()
            for column in normalized_columns
        ):
            raise ValueError("Bronze-to-Silver mappings do not match the active Bronze contract.")

        transformation_object_id = stable_bigint(
            "ingestion_object", source_system_id, source_target, target_table, "BRONZE_TO_SILVER"
        )
        dependencies = json.dumps(
            {
                "condition": "ALL_SUCCESS",
                "dependencies": [{
                    "ingestion_object_id": source_object_id,
                    "config_version": int(persisted_source["config_version"]),
                    "config_hash": str(persisted_source["config_hash"]),
                    "mapping_version": int(persisted_mapping["mapping_version"]),
                    "mapping_hash": str(persisted_mapping["mapping_hash"]),
                    "object_name": source_target,
                    "required_stage": "SOURCE_TO_BRONZE",
                    "wait_for_successful_commit": True,
                }],
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        executable = {
            "source_system_id": int(source_system_id),
            "object_kind": "TRANSFORMATION",
            "processing_stage": "BRONZE_TO_SILVER",
            "source_layer": "BRONZE",
            "target_layer": "SILVER",
            "object_name": target_table,
            "target_table": target_table,
            "object_type": "TABLE",
            "source_resource_type": "TABLE",
            "schema_evolution_policy": "FAIL",
            "load_type": "FULL",
            "write_mode": "MERGE",
            "merge_keys_json": json.dumps(keys, separators=(",", ":")),
            "dedupe_keys_json": json.dumps(keys, separators=(",", ":")),
            "delete_strategy": "IGNORE",
            "dependency_objects_json": dependencies,
            "validation_policy_json": json.dumps(
                {
                    "schema_version": "1.0",
                    "rules": [
                        {"rule_type": "MAPPED_COLUMNS_PRESENT", "threshold_value": 0, "threshold_unit": "COUNT", "failure_action": "FAIL_RUN", "stop_watermark_on_failure": True},
                        {"rule_type": "MERGE_KEYS_NOT_NULL", "columns": keys, "threshold_value": 0, "threshold_unit": "COUNT", "failure_action": "FAIL_RUN", "stop_watermark_on_failure": True},
                        {"rule_type": "TARGET_SCHEMA_MATCH", "threshold_value": 0, "threshold_unit": "COUNT", "failure_action": "FAIL_RUN", "stop_watermark_on_failure": True},
                    ],
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
        }
        config_hash = canonical_json_hash(executable)
        config_version = ((int(config_hash.removeprefix("sha256:")[:8], 16) & ((1 << 29) - 1)) * 2) + 1
        object_values = {
            "ingestion_object_id": transformation_object_id,
            **executable,
            "target_silver_table": target_table,
            "config_hash": config_hash,
            "config_version": config_version,
            "is_current": False,
            "active_flag": False,
        }
        collisions = self.query(
            f"SELECT ingestion_object_id FROM {self.table('cfg_ingestion_object')} "
            "WHERE LOWER(target_table) = :target_table AND ingestion_object_id <> :ingestion_object_id",
            {"target_table": target_table.casefold(), "ingestion_object_id": transformation_object_id},
        )
        if collisions:
            raise ValueError("The Silver target is already assigned to another ingestion object.")
        names = tuple(object_values)
        self.execute(
            f"""
            MERGE INTO {self.table('cfg_ingestion_object')} AS target
            USING (SELECT {', '.join(':' + name + ' AS ' + name for name in names)}) AS source
            ON target.ingestion_object_id = source.ingestion_object_id
               AND target.config_version = source.config_version
            WHEN NOT MATCHED THEN INSERT ({', '.join(names)}, created_at, updated_at)
            VALUES ({', '.join('source.' + name for name in names)}, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
            """,
            object_values,
        )
        transformation_object = self.get_ingestion_object(transformation_object_id, config_version)
        if not transformation_object or str(transformation_object.get("config_hash") or "") != config_hash:
            raise RuntimeError("Bronze-to-Silver transformation-object upsert failed.")

        input_objects_json = json.dumps(
            [{
                "ingestion_object_id": source_object_id,
                "config_version": int(persisted_source["config_version"]),
                "config_hash": str(persisted_source["config_hash"]),
                "mapping_version": int(persisted_mapping["mapping_version"]),
                "mapping_hash": str(persisted_mapping["mapping_hash"]),
                "object_name": source_target,
            }],
            sort_keys=True,
            separators=(",", ":"),
        )
        bundle_contract = {
            "ingestion_object_id": transformation_object_id,
            "object_config_version": config_version,
            "object_config_hash": config_hash,
            "processing_stage": "BRONZE_TO_SILVER",
            "source_object": source_target,
            "target_table": target_table,
            "input_objects_json": input_objects_json,
            "merge_keys": keys,
            "columns": normalized_columns,
        }
        mapping_hash = canonical_json_hash(bundle_contract)
        mapping_version = int(mapping_hash.removeprefix("sha256:")[:8], 16) & ((1 << 31) - 1) or 1
        mapping_group = f"BRONZE_TO_SILVER:{transformation_object_id}:{config_version}"
        rows = []
        for column in normalized_columns:
            target_column = normalize_bronze_column_name(column.get("target_column_name"))
            rows.append({
                "mapping_id": stable_bigint(
                    "mapping", transformation_object_id, "BRONZE_TO_SILVER",
                    column.get("source_field_path"), target_column,
                ),
                "ingestion_object_id": transformation_object_id,
                "processing_stage": "BRONZE_TO_SILVER",
                "source_layer": "BRONZE",
                "target_layer": "SILVER",
                "source_object_name": source_target,
                "target_object_name": target_table,
                "target_table": target_table,
                "mapping_group": mapping_group,
                "input_objects_json": input_objects_json,
                "build_order": 1,
                "source_field_path": str(column.get("source_field_path") or ""),
                "source_data_type": str(column.get("source_data_type") or ""),
                "target_column_name": target_column,
                "target_data_type": str(column.get("target_data_type") or ""),
                "is_nullable": bool(column.get("is_nullable", True)),
                "is_array": False,
                "is_primary_key": target_column in keys,
                "transformation_rule": str(column.get("transformation_rule") or "CAST"),
                "transformation_language": "NONE",
                "validation_rule_json": json.dumps({"rule_type": "NOT_NULL" if target_column in keys else "NONE"}),
                "severity": "CRITICAL" if target_column in keys else "INFO",
                "failure_action": "FAIL_RUN" if target_column in keys else "WARN",
                "stop_watermark_on_failure": target_column in keys,
                "ordinal_position": int(column.get("ordinal_position") or 0),
                "mapping_hash": mapping_hash,
                "mapping_version": mapping_version,
                "is_current": False,
                "active_flag": False,
            })
        row_names = tuple(rows[0])
        parameters = {
            f"r{index}_{name}": value
            for index, row in enumerate(rows)
            for name, value in row.items()
        }
        source_rows = " UNION ALL ".join(
            "SELECT " + ", ".join(f":r{index}_{name} AS {name}" for name in row_names)
            for index in range(len(rows))
        )
        self.execute(
            f"""
            MERGE INTO {self.table('cfg_mapping')} AS target
            USING ({source_rows}) AS source
            ON target.mapping_id = source.mapping_id AND target.mapping_version = source.mapping_version
            WHEN NOT MATCHED THEN INSERT ({', '.join(row_names)}, created_at, updated_at)
            VALUES ({', '.join('source.' + name for name in row_names)}, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
            """,
            parameters,
        )
        persisted = self.get_mapping_bundle(
            ingestion_object_id=transformation_object_id,
            processing_stage="BRONZE_TO_SILVER",
            mapping_version=mapping_version,
            expected_hash=mapping_hash,
            expected_target=target_table,
            require_active=None,
        )
        if len(persisted["mappings"]) != len(rows):
            raise RuntimeError("Bronze-to-Silver mapping bundle is incomplete.")
        return {"ingestion_object": transformation_object, "mapping_bundle": persisted}

    def upsert_silver_to_gold_draft(
        self,
        *,
        source_system_id: int,
        target_gold_table: str,
        inputs: Iterable[Mapping[str, Any]],
        columns: Iterable[Mapping[str, Any]],
        merge_keys: Iterable[str],
        join_rules: Iterable[Mapping[str, Any]],
        definition: Mapping[str, Any],
        build_order: int,
        write_mode: str = "MERGE",
        validation_policy: Optional[Mapping[str, Any]] = None,
        allow_inactive_inputs: bool = False,
    ) -> Dict[str, Any]:
        target_table = str(target_gold_table or "").strip()
        target_parts = target_table.split(".")
        if len(target_parts) not in {2, 3}:
            raise ValueError("Gold target must be schema.table or catalog.schema.table.")
        for part in target_parts:
            validate_identifier(part, label="Gold target identifier")
        input_pins = []
        source_columns: Dict[str, Dict[str, str]] = {}
        for requested in inputs:
            object_id = int(requested.get("ingestion_object_id") or 0)
            active = (
                self.get_ingestion_object(object_id, int(requested.get("config_version") or 0))
                if allow_inactive_inputs
                else self.get_active_ingestion_object(object_id)
            )
            if (
                not active
                or str(active.get("processing_stage") or "").upper() != "BRONZE_TO_SILVER"
                or int(active.get("config_version") or 0) != int(requested.get("config_version") or 0)
                or str(active.get("config_hash") or "") != str(requested.get("config_hash") or "")
                or _as_bool(active.get("active_flag")) == bool(allow_inactive_inputs)
                or _as_bool(active.get("is_current")) == bool(allow_inactive_inputs)
            ):
                lifecycle = "inactive reviewed" if allow_inactive_inputs else "active"
                raise ValueError(f"Silver-to-Gold requires exact {lifecycle} Silver input objects.")
            source_target = str(active.get("target_silver_table") or active.get("target_table") or "")
            bundle = self.get_mapping_bundle(
                ingestion_object_id=object_id,
                processing_stage="BRONZE_TO_SILVER",
                mapping_version=int(requested.get("mapping_version") or 0),
                expected_hash=str(requested.get("mapping_hash") or ""),
                expected_target=source_target,
                require_active=None if allow_inactive_inputs else True,
            )
            input_pins.append({
                "ingestion_object_id": object_id,
                "config_version": int(active["config_version"]),
                "config_hash": str(active["config_hash"]),
                "mapping_version": int(bundle["mapping_version"]),
                "mapping_hash": str(bundle["mapping_hash"]),
                "object_name": source_target,
                "required_stage": "BRONZE_TO_SILVER",
            })
            source_columns[source_target.casefold()] = {
                str(row.get("target_column_name") or "").casefold(): str(row.get("target_data_type") or "")
                for row in bundle["mappings"]
            }
        input_pins.sort(key=lambda item: (str(item["object_name"]).casefold(), int(item["ingestion_object_id"])))
        if not input_pins:
            raise ValueError("Silver-to-Gold requires at least one active Silver input.")
        input_objects_json = json.dumps(input_pins, sort_keys=True, separators=(",", ":"))
        joins = [dict(rule) for rule in join_rules]
        allowed_inputs = set(source_columns)
        seen_edges: set[tuple[str, str]] = set()
        graph = {name: set() for name in allowed_inputs}

        def type_family(value: Any) -> str:
            base = re.split(r"[<(]", str(value or "").upper(), 1)[0].strip()
            if any(token in base for token in ("INT", "DECIMAL", "NUMERIC", "NUMBER", "FLOAT", "DOUBLE", "REAL")):
                return "NUMERIC"
            if any(token in base for token in ("CHAR", "STRING", "TEXT", "VARCHAR")):
                return "STRING"
            if any(token in base for token in ("DATE", "TIME")):
                return "TEMPORAL"
            return base

        for rule in joins:
            left = str(rule.get("left_source_table") or "").casefold()
            right = str(rule.get("right_source_table") or "").casefold()
            left_column = str(rule.get("left_column") or "").casefold()
            right_column = str(rule.get("right_column") or "").casefold()
            join_type = str(rule.get("join_type") or "INNER").upper()
            edge = tuple(sorted((left, right)))
            if left not in allowed_inputs or right not in allowed_inputs:
                raise ValueError("Gold join rules reference an input outside the approved Silver set.")
            if (
                left == right
                or edge in seen_edges
                or join_type not in {"INNER", "LEFT"}
                or not left_column
                or not right_column
                or left_column not in source_columns[left]
                or right_column not in source_columns[right]
                or type_family(source_columns[left][left_column]) != type_family(source_columns[right][right_column])
            ):
                raise ValueError("Gold join rules contain an invalid, duplicate, or type-incompatible edge.")
            seen_edges.add(edge)
            graph[left].add(right)
            graph[right].add(left)
        if len(allowed_inputs) > 1:
            visited: set[str] = set()
            pending = [next(iter(allowed_inputs))]
            while pending:
                node = pending.pop()
                if node in visited:
                    continue
                visited.add(node)
                pending.extend(graph[node] - visited)
            if visited != allowed_inputs or len(seen_edges) != len(allowed_inputs) - 1:
                raise ValueError("Gold join rules must form one unambiguous connected input graph.")
        join_rules_json = json.dumps(joins, sort_keys=True, separators=(",", ":"))
        artifact_kind = str(definition.get("artifact_kind") or definition.get("object_type") or "").upper()
        if artifact_kind and artifact_kind not in {"FACT", "DIMENSION"}:
            raise ValueError("Gold definition artifact_kind must be FACT or DIMENSION.")
        aggregation_rules_json = json.dumps(dict(definition), sort_keys=True, separators=(",", ":"))
        normalized_columns = sorted(
            [{
                "source_object_name": str(column.get("source_object_name") or ""),
                "source_field_path": str(column.get("source_field_path") or ""),
                "source_data_type": str(column.get("source_data_type") or ""),
                "target_column_name": normalize_bronze_column_name(column.get("target_column_name")),
                "target_data_type": str(column.get("target_data_type") or ""),
                "is_nullable": bool(column.get("is_nullable", True)),
                "is_primary_key": bool(column.get("is_primary_key")),
                "ordinal_position": int(column.get("ordinal_position") or 0),
                "transformation_rule": str(column.get("transformation_rule") or "IDENTITY").upper(),
            } for column in columns],
            key=lambda item: (item["ordinal_position"], item["target_column_name"]),
        )
        if not normalized_columns:
            raise ValueError("Silver-to-Gold requires at least one output mapping.")
        targets = [column["target_column_name"].casefold() for column in normalized_columns]
        ordinals = [column["ordinal_position"] for column in normalized_columns]
        if len(targets) != len(set(targets)) or len(ordinals) != len(set(ordinals)) or any(value < 1 for value in ordinals):
            raise ValueError("Silver-to-Gold contains duplicate or invalid output mappings.")
        for column in normalized_columns:
            available = source_columns.get(column["source_object_name"].casefold()) or {}
            rule = column["transformation_rule"]
            if (
                available.get(column["source_field_path"].casefold(), "").casefold()
                != column["source_data_type"].casefold()
                or not (
                    rule in {"IDENTITY", "GROUP_KEY"}
                    or re.fullmatch(r"AGG_(SUM|AVG|MIN|MAX|COUNT)", rule)
                    or re.fullmatch(r"DATE_TRUNC_(DAY|WEEK|MONTH|QUARTER|YEAR)", rule)
                )
            ):
                raise ValueError("Silver-to-Gold mappings do not match an active Silver input contract.")
        aggregate_count = sum(
            1 for column in normalized_columns if column["transformation_rule"].startswith("AGG_")
        )
        if (artifact_kind == "FACT" and aggregate_count != 1) or (
            artifact_kind == "DIMENSION" and aggregate_count
        ):
            raise ValueError("Gold FACT mappings require exactly one aggregate; DIMENSION mappings allow none.")
        keys = [normalize_bronze_column_name(key) for key in merge_keys if str(key or "").strip()]
        mode = str(write_mode or "MERGE").strip().upper()
        if mode not in {"MERGE", "SNAPSHOT_REPLACE"}:
            raise ValueError("Gold write mode must be MERGE or SNAPSHOT_REPLACE.")
        if len(keys) != len(set(keys)) or (mode == "MERGE" and not keys):
            raise ValueError("Gold MERGE keys must be non-empty and unique.")
        for column in normalized_columns:
            if column["target_column_name"] in keys:
                column.update({"is_primary_key": True, "is_nullable": False})
        transformation_object_id = stable_bigint("ingestion_object", source_system_id, target_table, "SILVER_TO_GOLD")
        requested_policy = dict(
            validation_policy or {"fail_on_missing_input": True, "fail_on_schema_mismatch": True}
        )
        if not isinstance(requested_policy.get("rules"), list):
            rules = []
            if requested_policy.get("fail_on_missing_input"):
                rules.append({"rule_type": "INPUTS_PRESENT", "failure_action": "FAIL_RUN"})
            if requested_policy.get("fail_on_schema_mismatch"):
                rules.append({"rule_type": "TARGET_SCHEMA_MATCH", "failure_action": "FAIL_RUN"})
            if requested_policy.get("fail_on_null_key"):
                rules.append({
                    "rule_type": "KEYS_NOT_NULL",
                    "columns": keys,
                    "threshold_value": 0,
                    "threshold_unit": "COUNT",
                    "failure_action": "FAIL_RUN",
                })
            if requested_policy.get("fail_on_duplicate_key"):
                rules.append({
                    "rule_type": "KEYS_UNIQUE",
                    "columns": keys,
                    "threshold_value": 0,
                    "threshold_unit": "COUNT",
                    "failure_action": "FAIL_RUN",
                })
            if requested_policy.get("fail_on_join_multiplier"):
                threshold = requested_policy.get("max_join_multiplier")
                if not isinstance(threshold, (int, float)) or float(threshold) < 1.0:
                    raise ValueError("Gold join-multiplier validation requires max_join_multiplier >= 1.0.")
                rules.append({
                    "rule_type": "MAX_JOIN_MULTIPLIER",
                    "threshold_value": float(threshold),
                    "threshold_unit": "RATIO",
                    "failure_action": "FAIL_RUN",
                })
            requested_policy = {"schema_version": "1.0", "rules": rules}
        dependencies = json.dumps(
            {"condition": "ALL_SUCCESS", "dependencies": [{**pin, "wait_for_successful_commit": True} for pin in input_pins]},
            sort_keys=True,
            separators=(",", ":"),
        )
        executable = {
            "source_system_id": int(source_system_id),
            "object_kind": "TRANSFORMATION",
            "processing_stage": "SILVER_TO_GOLD",
            "source_layer": "SILVER",
            "target_layer": "GOLD",
            "object_name": target_table,
            "object_type": "TABLE",
            "source_resource_type": "TABLE",
            "schema_evolution_policy": "FAIL",
            "load_type": "FULL",
            "target_gold_table": target_table,
            "target_table": target_table,
            "write_mode": mode,
            "merge_keys_json": json.dumps(keys, separators=(",", ":")),
            "dedupe_keys_json": json.dumps(keys, separators=(",", ":")),
            "delete_strategy": "IGNORE",
            "dependency_objects_json": dependencies,
            "validation_policy_json": json.dumps(
                requested_policy,
                sort_keys=True,
                separators=(",", ":"),
            ),
        }
        config_hash = canonical_json_hash(executable)
        config_version = ((int(config_hash.removeprefix("sha256:")[:8], 16) & ((1 << 29) - 1)) * 2) + 1
        object_values = {
            "ingestion_object_id": transformation_object_id,
            **executable,
            "config_hash": config_hash,
            "config_version": config_version,
            "is_current": False,
            "active_flag": False,
        }
        collisions = self.query(
            f"SELECT ingestion_object_id FROM {self.table('cfg_ingestion_object')} WHERE LOWER(target_table) = :target_table "
            "AND ingestion_object_id <> :ingestion_object_id",
            {"target_table": target_table.casefold(), "ingestion_object_id": transformation_object_id},
        )
        if collisions:
            raise ValueError("The Gold target is already assigned to another ingestion object.")
        names = tuple(object_values)
        self.execute(
            f"MERGE INTO {self.table('cfg_ingestion_object')} AS target USING (SELECT "
            + ", ".join(":" + name + " AS " + name for name in names)
            + ") AS source ON target.ingestion_object_id = source.ingestion_object_id AND target.config_version = source.config_version "
            + f"WHEN NOT MATCHED THEN INSERT ({', '.join(names)}, created_at, updated_at) VALUES ("
            + ", ".join("source." + name for name in names)
            + ", CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())",
            object_values,
        )
        transformation = self.get_ingestion_object(transformation_object_id, config_version)
        if not transformation or str(transformation.get("config_hash") or "") != config_hash:
            raise RuntimeError("Silver-to-Gold transformation-object upsert failed.")
        bundle_contract = {
            "ingestion_object_id": transformation_object_id,
            "object_config_version": config_version,
            "object_config_hash": config_hash,
            "processing_stage": "SILVER_TO_GOLD",
            "target_table": target_table,
            "input_objects_json": input_objects_json,
            "join_rules_json": join_rules_json,
            "aggregation_rules_json": aggregation_rules_json,
            "build_order": int(build_order),
            "columns": normalized_columns,
        }
        mapping_hash = canonical_json_hash(bundle_contract)
        mapping_version = int(mapping_hash.removeprefix("sha256:")[:8], 16) & ((1 << 31) - 1) or 1
        mapping_group = f"SILVER_TO_GOLD:{transformation_object_id}:{config_version}"
        rows = []
        for column in normalized_columns:
            rows.append({
                "mapping_id": stable_bigint("mapping", transformation_object_id, "SILVER_TO_GOLD", column["source_object_name"], column["source_field_path"], column["target_column_name"]),
                "ingestion_object_id": transformation_object_id,
                "processing_stage": "SILVER_TO_GOLD",
                "source_layer": "SILVER",
                "target_layer": "GOLD",
                "source_object_name": column["source_object_name"],
                "target_object_name": target_table,
                "target_table": target_table,
                "mapping_group": mapping_group,
                "transformation_group": str(definition.get("transformation_group") or target_table),
                "input_objects_json": input_objects_json,
                "join_rules_json": join_rules_json,
                "aggregation_rules_json": aggregation_rules_json,
                "build_order": int(build_order),
                **{key: value for key, value in column.items() if key != "source_object_name"},
                "transformation_language": "NONE",
                "mapping_hash": mapping_hash,
                "mapping_version": mapping_version,
                "is_current": False,
                "active_flag": False,
            })
        row_names = tuple(rows[0])
        parameters = {f"r{index}_{name}": value for index, row in enumerate(rows) for name, value in row.items()}
        source_rows = " UNION ALL ".join(
            "SELECT " + ", ".join(f":r{index}_{name} AS {name}" for name in row_names)
            for index in range(len(rows))
        )
        self.execute(
            f"MERGE INTO {self.table('cfg_mapping')} AS target USING ({source_rows}) AS source "
            "ON target.mapping_id = source.mapping_id AND target.mapping_version = source.mapping_version "
            f"WHEN NOT MATCHED THEN INSERT ({', '.join(row_names)}, created_at, updated_at) VALUES ("
            + ", ".join("source." + name for name in row_names)
            + ", CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())",
            parameters,
        )
        persisted = self.get_mapping_bundle(
            ingestion_object_id=transformation_object_id,
            processing_stage="SILVER_TO_GOLD",
            mapping_version=mapping_version,
            expected_hash=mapping_hash,
            expected_target=target_table,
            require_active=None,
        )
        if len(persisted["mappings"]) != len(rows):
            raise RuntimeError("Silver-to-Gold mapping bundle is incomplete.")
        return {"ingestion_object": transformation, "mapping_bundle": persisted}

    def register_and_activate_source_to_bronze_artifact(
        self,
        *,
        draft_config_version: int,
        ingestion_object_id: int,
        mapping_version: int,
        mapping_hash: str,
        execution_spec: Mapping[str, Any],
    ) -> Dict[str, Any]:
        return self._register_and_activate_artifact(
            processing_stage="SOURCE_TO_BRONZE",
            draft_config_version=draft_config_version,
            ingestion_object_id=ingestion_object_id,
            mapping_version=mapping_version,
            mapping_hash=mapping_hash,
            execution_spec=execution_spec,
        )

    def register_and_activate_bronze_to_silver_artifact(
        self,
        *,
        draft_config_version: int,
        ingestion_object_id: int,
        mapping_version: int,
        mapping_hash: str,
        execution_spec: Mapping[str, Any],
    ) -> Dict[str, Any]:
        return self._register_and_activate_artifact(
            processing_stage="BRONZE_TO_SILVER",
            draft_config_version=draft_config_version,
            ingestion_object_id=ingestion_object_id,
            mapping_version=mapping_version,
            mapping_hash=mapping_hash,
            execution_spec=execution_spec,
        )

    def register_and_activate_silver_to_gold_artifact(
        self,
        *,
        draft_config_version: int,
        ingestion_object_id: int,
        mapping_version: int,
        mapping_hash: str,
        execution_spec: Mapping[str, Any],
    ) -> Dict[str, Any]:
        return self._register_and_activate_artifact(
            processing_stage="SILVER_TO_GOLD",
            draft_config_version=draft_config_version,
            ingestion_object_id=ingestion_object_id,
            mapping_version=mapping_version,
            mapping_hash=mapping_hash,
            execution_spec=execution_spec,
        )

    def _register_and_activate_artifact(
        self,
        *,
        processing_stage: str,
        draft_config_version: int,
        ingestion_object_id: int,
        mapping_version: int,
        mapping_hash: str,
        execution_spec: Mapping[str, Any],
    ) -> Dict[str, Any]:
        from services.metadata_contracts import validate_execution_spec
        from utilis.generated_code_paths import verified_execution_artifact

        draft = self.get_ingestion_object(ingestion_object_id, draft_config_version)
        if (
            not draft
            or str(draft.get("processing_stage") or "").upper() != processing_stage
            or _as_bool(draft.get("active_flag"))
            or _as_bool(draft.get("is_current"))
        ):
            raise ValueError("Artifact registration requires the exact inactive ingestion-object draft.")
        target_table = str(draft.get("target_table") or draft.get("target_bronze_table") or "").strip()
        try:
            bundle = self.get_mapping_bundle(
                ingestion_object_id=ingestion_object_id,
                processing_stage=processing_stage,
                mapping_version=mapping_version,
                expected_hash=mapping_hash,
                expected_target=target_table,
                require_active=False,
            )
        except RuntimeError:
            bundle = self.get_mapping_bundle(
                ingestion_object_id=ingestion_object_id,
                processing_stage=processing_stage,
                mapping_version=mapping_version,
                expected_hash=mapping_hash,
                expected_target=target_table,
                require_active=True,
            )
        spec = validate_execution_spec(execution_spec, platform=self.context.platform)
        if processing_stage == "SOURCE_TO_BRONZE":
            _validate_source_resource_binding(draft, spec.get("source_resource"))
        if not str(os.getenv("ATHENA_GENERATED_CODE_DIR") or "").strip():
            raise RuntimeError("ATHENA_GENERATED_CODE_DIR must identify durable shared storage before metadata activation.")
        artifact_path = verified_execution_artifact(spec, platform=self.context.platform)
        if self.context.platform == "snowflake":
            _validate_snowflake_registered_artifact(
                artifact_path, draft, spec, processing_stage, target_table
            )
        if int(spec["mapping_version"]) != int(mapping_version):
            raise ValueError("The execution artifact was generated from a different mapping version.")
        spec = {
            **spec,
            "design_config_version": int(draft_config_version),
            "design_config_hash": str(draft.get("config_hash") or ""),
            "mapping_hash": str(mapping_hash),
            "processing_stage": processing_stage,
        }
        execution_spec_json = json.dumps(spec, sort_keys=True, separators=(",", ":"))
        config_hash = canonical_json_hash(
            {
                "base_config_hash": str(draft.get("config_hash") or ""),
                "execution_spec": spec,
                "target_table": target_table,
                "write_mode": str(draft.get("write_mode") or ""),
            }
        )
        executable_version = ((int(config_hash.removeprefix("sha256:")[:8], 16) & ((1 << 29) - 1)) * 2) + 2
        copy_fields = (
            "source_system_id", "connection_id", "object_kind", "ingestion_type",
            "processing_stage", "source_layer", "target_layer", "object_name", "object_type",
            "source_resource_type", "payload_format", "container_format", "source_path",
            "file_pattern", "database_schema", "table_name", "query_text", "endpoint_path",
            "http_method", "request_headers_json", "request_params_json", "request_body_template",
            "response_root_path", "pagination_type", "pagination_config_json", "parser_options_json",
            "normalization_options_json", "schema_inference_policy", "schema_evolution_policy",
            "load_type", "watermark_column", "boundary_operator", "tie_breaker_columns_json",
            "sort_columns_json", "lookback_interval", "checkpoint_type", "target_bronze_table",
            "target_silver_table", "target_gold_table", "target_table", "write_mode",
            "merge_keys_json", "dedupe_keys_json", "partition_columns_json", "delete_strategy",
            "scd_config_json", "dependency_objects_json", "validation_policy_json",
        )
        values = {"ingestion_object_id": int(ingestion_object_id)}
        values.update({name: draft.get(name) for name in copy_fields if draft.get(name) is not None})
        values.update({
            "execution_spec_json": execution_spec_json,
            "config_hash": config_hash,
            "config_version": executable_version,
            "is_current": False,
            "active_flag": False,
        })
        names = tuple(values)
        self.execute(
            f"""
            MERGE INTO {self.table('cfg_ingestion_object')} AS target
            USING (SELECT {', '.join(':' + name + ' AS ' + name for name in names)}) AS source
            ON target.ingestion_object_id = source.ingestion_object_id
               AND target.config_version = source.config_version
            WHEN NOT MATCHED THEN INSERT (
                {', '.join(names)}, created_at, updated_at
            ) VALUES (
                {', '.join('source.' + name for name in names)}, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
            )
            """,
            values,
        )
        executable = self.get_ingestion_object(ingestion_object_id, executable_version)
        if not executable or str(executable.get("config_hash") or "") != config_hash:
            raise RuntimeError("Executable ingestion-object registration postcondition failed.")

        activation_statements = [
            (f"""
            UPDATE {self.table('cfg_mapping')}
            SET active_flag = CASE WHEN mapping_version = :mapping_version THEN :enabled ELSE :disabled END,
                is_current = CASE WHEN mapping_version = :mapping_version THEN :enabled ELSE :disabled END,
                effective_from = CASE WHEN mapping_version = :mapping_version
                    THEN COALESCE(effective_from, CURRENT_TIMESTAMP()) ELSE effective_from END,
                effective_to = CASE WHEN mapping_version = :mapping_version THEN NULL ELSE CURRENT_TIMESTAMP() END,
                updated_at = CURRENT_TIMESTAMP()
            WHERE ingestion_object_id = :ingestion_object_id AND processing_stage = :processing_stage
              AND (mapping_version = :mapping_version OR is_current = :enabled)
            """, {
                "ingestion_object_id": int(ingestion_object_id),
                "processing_stage": processing_stage,
                "mapping_version": int(mapping_version),
                "enabled": True,
                "disabled": False,
            }),
            (f"""
            UPDATE {self.table('cfg_ingestion_object')}
            SET active_flag = CASE WHEN config_version = :config_version THEN :enabled ELSE :disabled END,
                is_current = CASE WHEN config_version = :config_version THEN :enabled ELSE :disabled END,
                effective_from = CASE WHEN config_version = :config_version
                    THEN COALESCE(effective_from, CURRENT_TIMESTAMP()) ELSE effective_from END,
                effective_to = CASE WHEN config_version = :config_version THEN NULL ELSE CURRENT_TIMESTAMP() END,
                updated_at = CURRENT_TIMESTAMP()
            WHERE ingestion_object_id = :ingestion_object_id
              AND (config_version = :config_version OR is_current = :enabled)
            """, {
                "ingestion_object_id": int(ingestion_object_id),
                "config_version": executable_version,
                "enabled": True,
                "disabled": False,
            }),
        ]
        self.execute_batch(activation_statements)
        active_object = self.get_active_ingestion_object(ingestion_object_id)
        active_bundle = self.get_mapping_bundle(
            ingestion_object_id=ingestion_object_id,
            processing_stage=processing_stage,
            mapping_version=mapping_version,
            expected_hash=mapping_hash,
            expected_target=target_table,
            require_active=True,
        )
        if not active_object or int(active_object.get("config_version") or 0) != executable_version:
            raise RuntimeError("Executable ingestion-object activation postcondition failed.")
        return {"ingestion_object": active_object, "mapping_bundle": active_bundle, "execution_spec": spec}

    def register_and_activate_artifacts(
        self, *, processing_stage: str, artifacts: Iterable[Mapping[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Register and activate one reviewed stage bundle with set-based metadata I/O."""
        from services.metadata_contracts import validate_execution_spec
        from utilis.generated_code_paths import verified_execution_artifact

        stage = str(processing_stage or "").upper()
        requested = [dict(item) for item in artifacts]
        if not requested:
            return []
        object_ids = [int(item["ingestion_object_id"]) for item in requested]
        if len(set(object_ids)) != len(object_ids):
            raise ValueError("A stage activation bundle cannot contain the same ingestion object twice.")
        draft_refs = [
            {
                "ingestion_object_id": int(item["ingestion_object_id"]),
                "config_version": int(item["draft_config_version"]),
            }
            for item in requested
        ]
        drafts = self.get_ingestion_objects(draft_refs, require_active=False)
        mapping_refs = []
        for item in requested:
            draft = drafts[(int(item["ingestion_object_id"]), int(item["draft_config_version"]))]
            target_table = str(draft.get("target_table") or draft.get("target_bronze_table") or "").strip()
            if str(draft.get("processing_stage") or "").upper() != stage:
                raise ValueError("Artifact registration requires one exact-stage inactive draft bundle.")
            mapping_refs.append({
                "ingestion_object_id": int(item["ingestion_object_id"]),
                "processing_stage": stage,
                "mapping_version": int(item["mapping_version"]),
                "expected_hash": str(item["mapping_hash"]),
                "expected_target": target_table,
                "require_active": None,
            })
        bundles = self.get_mapping_bundles(mapping_refs)

        copy_fields = (
            "source_system_id", "connection_id", "object_kind", "ingestion_type",
            "processing_stage", "source_layer", "target_layer", "object_name", "object_type",
            "source_resource_type", "payload_format", "container_format", "source_path",
            "file_pattern", "database_schema", "table_name", "query_text", "endpoint_path",
            "http_method", "request_headers_json", "request_params_json", "request_body_template",
            "response_root_path", "pagination_type", "pagination_config_json", "parser_options_json",
            "normalization_options_json", "schema_inference_policy", "schema_evolution_policy",
            "load_type", "watermark_column", "boundary_operator", "tie_breaker_columns_json",
            "sort_columns_json", "lookback_interval", "checkpoint_type", "target_bronze_table",
            "target_silver_table", "target_gold_table", "target_table", "write_mode",
            "merge_keys_json", "dedupe_keys_json", "partition_columns_json", "delete_strategy",
            "scd_config_json", "dependency_objects_json", "validation_policy_json",
        )
        executable_rows = []
        prepared = []
        for item in requested:
            object_id = int(item["ingestion_object_id"])
            draft_version = int(item["draft_config_version"])
            mapping_version = int(item["mapping_version"])
            mapping_hash = str(item["mapping_hash"])
            draft = drafts[(object_id, draft_version)]
            target_table = str(draft.get("target_table") or draft.get("target_bronze_table") or "").strip()
            bundle_key = (object_id, stage, mapping_version)
            bundle = bundles[bundle_key]
            spec = validate_execution_spec(item["execution_spec"], platform=self.context.platform)
            if stage == "SOURCE_TO_BRONZE":
                _validate_source_resource_binding(draft, spec.get("source_resource"))
            if not str(os.getenv("ATHENA_GENERATED_CODE_DIR") or "").strip():
                raise RuntimeError("ATHENA_GENERATED_CODE_DIR must identify durable shared storage before metadata activation.")
            artifact_path = verified_execution_artifact(spec, platform=self.context.platform)
            if self.context.platform == "snowflake":
                _validate_snowflake_registered_artifact(
                    artifact_path, draft, spec, stage, target_table
                )
            if int(spec["mapping_version"]) != mapping_version:
                raise ValueError("The execution artifact was generated from a different mapping version.")
            spec = {
                **spec,
                "design_config_version": draft_version,
                "design_config_hash": str(draft.get("config_hash") or ""),
                "mapping_hash": mapping_hash,
                "processing_stage": stage,
            }
            config_hash = canonical_json_hash({
                "base_config_hash": str(draft.get("config_hash") or ""),
                "execution_spec": spec,
                "target_table": target_table,
                "write_mode": str(draft.get("write_mode") or ""),
            })
            executable_version = ((int(config_hash.removeprefix("sha256:")[:8], 16) & ((1 << 29) - 1)) * 2) + 2
            row = {"ingestion_object_id": object_id}
            row.update({name: draft.get(name) for name in copy_fields})
            row.update({
                "execution_spec_json": json.dumps(spec, sort_keys=True, separators=(",", ":")),
                "config_hash": config_hash,
                "config_version": executable_version,
                "is_current": False,
                "active_flag": False,
            })
            executable_rows.append(row)
            prepared.append({
                "ingestion_object_id": object_id,
                "mapping_version": mapping_version,
                "mapping_hash": mapping_hash,
                "target_table": target_table,
                "config_version": executable_version,
                "config_hash": config_hash,
                "spec": spec,
                "bundle": bundle,
            })

        names, source, parameters = self._source_rows(executable_rows, prefix="exec")
        self.execute(
            f"MERGE INTO {self.table('cfg_ingestion_object')} AS target USING ({source}) AS source "
            "ON target.ingestion_object_id = source.ingestion_object_id "
            "AND target.config_version = source.config_version "
            f"WHEN NOT MATCHED THEN INSERT ({', '.join(names)}, created_at, updated_at) VALUES ("
            + ", ".join("source." + name for name in names)
            + ", CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())",
            parameters,
        )

        activation_rows = [
            {
                "ingestion_object_id": item["ingestion_object_id"],
                "processing_stage": stage,
                "mapping_version": item["mapping_version"],
                "config_version": item["config_version"],
            }
            for item in prepared
        ]
        _, activation_source, activation_parameters = self._source_rows(activation_rows, prefix="active")
        self.execute(
            f"MERGE INTO {self.table('cfg_mapping')} AS target USING ({activation_source}) AS source "
            "ON target.ingestion_object_id = source.ingestion_object_id "
            "AND target.processing_stage = source.processing_stage "
            "WHEN MATCHED AND (target.mapping_version = source.mapping_version OR target.is_current = :enabled) "
            "THEN UPDATE SET active_flag = CASE WHEN target.mapping_version = source.mapping_version THEN :enabled ELSE :disabled END, "
            "is_current = CASE WHEN target.mapping_version = source.mapping_version THEN :enabled ELSE :disabled END, "
            "effective_from = CASE WHEN target.mapping_version = source.mapping_version THEN COALESCE(target.effective_from, CURRENT_TIMESTAMP()) ELSE target.effective_from END, "
            "effective_to = CASE WHEN target.mapping_version = source.mapping_version THEN NULL ELSE CURRENT_TIMESTAMP() END, "
            "updated_at = CURRENT_TIMESTAMP()",
            {**activation_parameters, "enabled": True, "disabled": False},
        )
        self.execute(
            f"MERGE INTO {self.table('cfg_ingestion_object')} AS target USING ({activation_source}) AS source "
            "ON target.ingestion_object_id = source.ingestion_object_id "
            "WHEN MATCHED AND (target.config_version = source.config_version OR target.is_current = :enabled) "
            "THEN UPDATE SET active_flag = CASE WHEN target.config_version = source.config_version THEN :enabled ELSE :disabled END, "
            "is_current = CASE WHEN target.config_version = source.config_version THEN :enabled ELSE :disabled END, "
            "effective_from = CASE WHEN target.config_version = source.config_version THEN COALESCE(target.effective_from, CURRENT_TIMESTAMP()) ELSE target.effective_from END, "
            "effective_to = CASE WHEN target.config_version = source.config_version THEN NULL ELSE CURRENT_TIMESTAMP() END, "
            "updated_at = CURRENT_TIMESTAMP()",
            {**activation_parameters, "enabled": True, "disabled": False},
        )

        active_objects = self.get_active_ingestion_objects(object_ids)
        active_bundles = self.get_mapping_bundles([
            {
                "ingestion_object_id": item["ingestion_object_id"],
                "processing_stage": stage,
                "mapping_version": item["mapping_version"],
                "expected_hash": item["mapping_hash"],
                "expected_target": item["target_table"],
                "require_active": True,
            }
            for item in prepared
        ])
        results = []
        for item in prepared:
            active = active_objects[item["ingestion_object_id"]]
            if (
                int(active.get("config_version") or 0) != item["config_version"]
                or str(active.get("config_hash") or "") != item["config_hash"]
            ):
                raise RuntimeError("Bulk executable activation postcondition failed.")
            results.append({
                "ingestion_object": active,
                "mapping_bundle": active_bundles[(item["ingestion_object_id"], stage, item["mapping_version"])],
                "execution_spec": item["spec"],
            })
        return results

    def get_active_mapping_reference(self, ingestion_object_id: int, processing_stage: str) -> Dict[str, Any]:
        rows = self.query(
            f"SELECT mapping_version, mapping_hash FROM {self.table('cfg_mapping')} "
            "WHERE ingestion_object_id = :ingestion_object_id AND processing_stage = :processing_stage "
            "AND active_flag = :active_flag AND is_current = :is_current",
            {
                "ingestion_object_id": int(ingestion_object_id),
                "processing_stage": str(processing_stage).upper(),
                "active_flag": True,
                "is_current": True,
            },
        )
        references = {
            (int(row.get("mapping_version") or 0), str(row.get("mapping_hash") or ""))
            for row in rows
        }
        if len(references) != 1:
            raise RuntimeError("Runtime requires exactly one active mapping version and hash.")
        mapping_version, mapping_hash = next(iter(references))
        return {"mapping_version": mapping_version, "mapping_hash": mapping_hash}

    def _runtime_snapshot(
        self,
        obj: Mapping[str, Any],
        mapping: Mapping[str, Any],
        *,
        connection: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        try:
            spec = json.loads(str(obj.get("execution_spec_json") or ""))
        except json.JSONDecodeError as exc:
            raise ValueError("The executable ingestion object has an invalid execution specification.") from exc
        if (
            int(spec.get("mapping_version") or 0) != int(mapping.get("mapping_version") or 0)
            or str(spec.get("mapping_hash") or "") != str(mapping.get("mapping_hash") or "")
            or str(spec.get("processing_stage") or "").upper()
            != str(obj.get("processing_stage") or "").upper()
        ):
            raise RuntimeError("The execution artifact does not match the active mapping contract.")
        if str(obj.get("processing_stage") or "").upper() == "SOURCE_TO_BRONZE" and (
            str(spec.get("runtime_context_contract_version") or "") != "1.0"
            or str(spec.get("idempotency_identity") or "") != "logical_work_id"
        ):
            raise RuntimeError("The Bronze artifact does not implement the required runtime/idempotency contract.")
        if str(obj.get("processing_stage") or "").upper() == "SOURCE_TO_BRONZE":
            _validate_source_resource_binding(obj, spec.get("source_resource"))
        connection_pin = None
        if obj.get("connection_id") is not None:
            resolved_connection = connection or self.get_active_connection(int(obj["connection_id"]))
            if not resolved_connection:
                raise ValueError("The runtime source connection is not active.")
            connection_pin = {
                "connection_id": int(resolved_connection["connection_id"]),
                "config_version": int(resolved_connection["config_version"]),
                "config_hash": str(resolved_connection.get("config_hash") or ""),
            }
            if (
                int(spec.get("connection_id") or 0) != connection_pin["connection_id"]
                or int(spec.get("connection_config_version") or 0)
                != connection_pin["config_version"]
                or str(spec.get("connection_config_hash") or "")
                != connection_pin["config_hash"]
            ):
                raise RuntimeError(
                    "The execution artifact is not bound to the active source-connection version."
                )
        snapshot = {
            "ingestion_object_id": int(obj["ingestion_object_id"]),
            "config_version": int(obj.get("config_version") or 0),
            "config_hash": str(obj.get("config_hash") or ""),
            "mapping_version": int(mapping.get("mapping_version") or 0),
            "mapping_hash": str(mapping.get("mapping_hash") or ""),
            "connection": connection_pin,
            "artifact_hash": str(spec.get("artifact_hash") or ""),
        }
        return {"snapshot": snapshot, "metadata_snapshot_id": canonical_json_hash(snapshot)}

    def enqueue_work(
        self,
        *,
        ingestion_object_id: int,
        trigger_type: str,
        work_scope: Mapping[str, Any],
        requested_by: str,
        priority: int = 0,
        max_attempts: int = 3,
        logical_work_id: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        retry_policy: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        obj = self.get_active_ingestion_object(int(ingestion_object_id))
        if not obj or not str(obj.get("execution_spec_json") or "").strip():
            raise ValueError("Queue work requires an active executable ingestion object.")
        if (
            str(obj.get("load_type") or "FULL").upper() != "FULL"
            or str(obj.get("watermark_column") or "").strip()
            or str(obj.get("checkpoint_type") or "").strip()
        ):
            raise ValueError("The database-source runtime currently accepts FULL/stateless objects only.")
        stage = str(obj.get("processing_stage") or "").upper()
        mapping = self.get_active_mapping_reference(int(ingestion_object_id), stage)
        scope = dict(work_scope or {})
        snapshot = self._runtime_snapshot(obj, mapping)
        logical_id = str(logical_work_id or canonical_json_hash({
            "ingestion_object_id": int(ingestion_object_id),
            "work_scope": scope,
        })).strip()
        dedupe_key = str(idempotency_key or canonical_json_hash({
            "logical_work_id": logical_id,
            "ingestion_object_id": int(ingestion_object_id),
        })).strip()
        queue_id = stable_bigint("queue", dedupe_key)
        try:
            validation_policy = json.loads(str(obj.get("validation_policy_json") or "{}"))
        except json.JSONDecodeError as exc:
            raise ValueError("The active validation policy is invalid JSON.") from exc
        runtime_context = validate_runtime_context({
            "contract_version": "1.0",
            "logical_work_id": logical_id,
            "queue_id": queue_id,
            "ingestion_object_id": int(ingestion_object_id),
            "processing_stage": stage,
            "load_type": "FULL",
            "source_object": obj.get("object_name"),
            "target_table": obj.get("target_table") or obj.get("target_bronze_table"),
            "config_version": int(obj.get("config_version") or 0),
            "mapping_version": int(mapping.get("mapping_version") or 0),
            "attempt_number": 0,
            "validation_policy_hash": (
                canonical_json_hash(validation_policy) if validation_policy else None
            ),
        })
        scope.update({"_metadata_snapshot": snapshot["snapshot"], "runtime_context": runtime_context})
        scope_json = json.dumps(scope, sort_keys=True, separators=(",", ":"))
        metadata_snapshot_id = snapshot["metadata_snapshot_id"]
        values = {
            "queue_id": queue_id,
            "ingestion_object_id": int(ingestion_object_id),
            "trigger_type": str(trigger_type or "MANUAL").upper(),
            "queue_status": "PENDING",
            "priority": int(priority),
            "logical_work_id": logical_id,
            "idempotency_key": dedupe_key,
            "work_scope_json": scope_json,
            "requested_start_boundary": scope.get("start_boundary"),
            "requested_end_boundary": scope.get("end_boundary"),
            "partition_spec_json": json.dumps(scope.get("partition") or {}, sort_keys=True, separators=(",", ":")),
            "batch_id": scope.get("batch_id"),
            "requested_by": str(requested_by or "system"),
            "manual_override_json": json.dumps(scope.get("manual_override") or {}, sort_keys=True, separators=(",", ":")),
            "attempt_count": 0,
            "max_attempts": max(1, int(max_attempts)),
            "retry_policy_json": json.dumps(dict(retry_policy or {}), sort_keys=True, separators=(",", ":")),
            "metadata_snapshot_id": metadata_snapshot_id,
            "message": "Queued",
        }
        names = tuple(values)
        self.execute(
            f"MERGE INTO {self.table('ctl_ingestion_queue')} AS target USING (SELECT "
            + ", ".join(f":{name} AS {name}" for name in names)
            + ") AS source ON target.idempotency_key = source.idempotency_key "
            + f"WHEN NOT MATCHED THEN INSERT ({', '.join(names)}, requested_at) VALUES ("
            + ", ".join(f"source.{name}" for name in names)
            + ", CURRENT_TIMESTAMP())",
            values,
        )
        rows = self.query(
            f"SELECT * FROM {self.table('ctl_ingestion_queue')} WHERE idempotency_key = :idempotency_key",
            {"idempotency_key": dedupe_key},
        )
        if (
            len(rows) != 1
            or int(rows[0].get("queue_id") or 0) != queue_id
            or int(rows[0].get("ingestion_object_id") or 0) != int(ingestion_object_id)
            or str(rows[0].get("logical_work_id") or "") != logical_id
            or str(rows[0].get("work_scope_json") or "") != scope_json
        ):
            raise RuntimeError("Queue idempotency-key collision or duplicate row detected.")
        return rows[0]

    def enqueue_work_batch(self, requests: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
        requested = [dict(item) for item in requests]
        if not requested:
            return []
        object_ids = [int(item["ingestion_object_id"]) for item in requested]
        if len(set(object_ids)) != len(object_ids):
            raise ValueError("A runtime enqueue bundle cannot contain the same ingestion object twice.")
        objects = self.get_active_ingestion_objects(object_ids)
        mapping_refs = []
        specs = {}
        for object_id in object_ids:
            obj = objects[object_id]
            if not str(obj.get("execution_spec_json") or "").strip():
                raise ValueError("Queue work requires an active executable ingestion object.")
            if (
                str(obj.get("load_type") or "FULL").upper() != "FULL"
                or str(obj.get("watermark_column") or "").strip()
                or str(obj.get("checkpoint_type") or "").strip()
            ):
                raise ValueError("The database-source runtime currently accepts FULL/stateless objects only.")
            try:
                spec = json.loads(str(obj["execution_spec_json"]))
            except json.JSONDecodeError as exc:
                raise ValueError("The executable ingestion object has an invalid execution specification.") from exc
            specs[object_id] = spec
            mapping_refs.append({
                "ingestion_object_id": object_id,
                "processing_stage": str(obj.get("processing_stage") or "").upper(),
                "mapping_version": int(spec.get("mapping_version") or 0),
                "expected_hash": str(spec.get("mapping_hash") or ""),
                "expected_target": str(obj.get("target_table") or obj.get("target_bronze_table") or ""),
                "require_active": True,
            })
        mappings = self.get_mapping_bundles(mapping_refs)
        connections = {
            connection_id: self.get_active_connection(connection_id)
            for connection_id in {
                int(obj["connection_id"])
                for obj in objects.values()
                if obj.get("connection_id") is not None
            }
        }
        rows = []
        for request in requested:
            object_id = int(request["ingestion_object_id"])
            obj = objects[object_id]
            stage = str(obj.get("processing_stage") or "").upper()
            spec = specs[object_id]
            mapping_version = int(spec.get("mapping_version") or 0)
            mapping = mappings[(object_id, stage, mapping_version)]
            connection = connections.get(int(obj["connection_id"])) if obj.get("connection_id") is not None else None
            snapshot = self._runtime_snapshot(obj, mapping, connection=connection)
            scope = dict(request.get("work_scope") or {})
            logical_id = str(request.get("logical_work_id") or canonical_json_hash({
                "ingestion_object_id": object_id, "work_scope": scope,
            })).strip()
            dedupe_key = str(request.get("idempotency_key") or canonical_json_hash({
                "logical_work_id": logical_id, "ingestion_object_id": object_id,
            })).strip()
            queue_id = stable_bigint("queue", dedupe_key)
            try:
                validation_policy = json.loads(str(obj.get("validation_policy_json") or "{}"))
            except json.JSONDecodeError as exc:
                raise ValueError("The active validation policy is invalid JSON.") from exc
            runtime_context = validate_runtime_context({
                "contract_version": "1.0", "logical_work_id": logical_id, "queue_id": queue_id,
                "ingestion_object_id": object_id, "processing_stage": stage, "load_type": "FULL",
                "source_object": obj.get("object_name"),
                "target_table": obj.get("target_table") or obj.get("target_bronze_table"),
                "config_version": int(obj.get("config_version") or 0),
                "mapping_version": int(mapping.get("mapping_version") or 0), "attempt_number": 0,
                "validation_policy_hash": canonical_json_hash(validation_policy) if validation_policy else None,
            })
            scope.update({"_metadata_snapshot": snapshot["snapshot"], "runtime_context": runtime_context})
            scope_json = json.dumps(scope, sort_keys=True, separators=(",", ":"))
            rows.append({
                "queue_id": queue_id, "ingestion_object_id": object_id,
                "trigger_type": str(request.get("trigger_type") or "MANUAL").upper(),
                "queue_status": "PENDING", "priority": int(request.get("priority") or 0),
                "logical_work_id": logical_id, "idempotency_key": dedupe_key,
                "work_scope_json": scope_json, "requested_start_boundary": scope.get("start_boundary"),
                "requested_end_boundary": scope.get("end_boundary"),
                "partition_spec_json": json.dumps(scope.get("partition") or {}, sort_keys=True, separators=(",", ":")),
                "batch_id": scope.get("batch_id"), "requested_by": str(request.get("requested_by") or "system"),
                "manual_override_json": json.dumps(scope.get("manual_override") or {}, sort_keys=True, separators=(",", ":")),
                "attempt_count": 0, "max_attempts": max(1, int(request.get("max_attempts") or 3)),
                "retry_policy_json": json.dumps(dict(request.get("retry_policy") or {}), sort_keys=True, separators=(",", ":")),
                "metadata_snapshot_id": snapshot["metadata_snapshot_id"], "message": "Queued",
            })
        names, source, parameters = self._source_rows(rows, prefix="queue")
        self.execute(
            f"MERGE INTO {self.table('ctl_ingestion_queue')} AS target USING ({source}) AS source "
            "ON target.idempotency_key = source.idempotency_key "
            f"WHEN NOT MATCHED THEN INSERT ({', '.join(names)}, requested_at) VALUES ("
            + ", ".join("source." + name for name in names)
            + ", CURRENT_TIMESTAMP())",
            parameters,
        )
        key_parameters = {f"key_{index}": row["idempotency_key"] for index, row in enumerate(rows)}
        persisted = self.query(
            f"SELECT * FROM {self.table('ctl_ingestion_queue')} WHERE idempotency_key IN ("
            + ", ".join(f":key_{index}" for index in range(len(rows)))
            + ")",
            key_parameters,
        )
        by_key = {str(row.get("idempotency_key") or ""): row for row in persisted}
        if len(by_key) != len(rows):
            raise RuntimeError("Bulk queue upsert did not produce one row per idempotency key.")
        results = []
        for expected in rows:
            saved = by_key.get(expected["idempotency_key"])
            if (
                not saved
                or int(saved.get("queue_id") or 0) != expected["queue_id"]
                or int(saved.get("ingestion_object_id") or 0) != expected["ingestion_object_id"]
                or str(saved.get("logical_work_id") or "") != expected["logical_work_id"]
                or str(saved.get("work_scope_json") or "") != expected["work_scope_json"]
            ):
                raise RuntimeError("Queue idempotency-key collision or duplicate row detected.")
            results.append(saved)
        return results

    def claim_next_queue_item(
        self,
        *,
        worker_id: str,
        lease_seconds: int = 300,
        logical_work_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        worker = str(worker_id or "").strip()
        if not worker:
            raise ValueError("worker_id is required.")
        logical_id = str(logical_work_id or "").strip()
        scope_sql = " AND logical_work_id = :logical_work_id" if logical_id else ""
        query_values = {
            "pending": "PENDING",
            "retry_wait": "RETRY_WAIT",
            "running": "RUNNING",
            "finalizing": "FINALIZING",
        }
        if logical_id:
            query_values["logical_work_id"] = logical_id
        candidate_rows = self.query(
            f"SELECT * FROM {self.table('ctl_ingestion_queue')} WHERE ("
            "(queue_status = :finalizing AND lease_expires_at <= CURRENT_TIMESTAMP()) OR "
            "(queue_status = :running AND lease_expires_at <= CURRENT_TIMESTAMP() "
            "AND (last_heartbeat_at IS NULL OR last_heartbeat_at <= lease_expires_at)) OR "
            "(attempt_count < max_attempts AND (queue_status = :pending "
            "OR (queue_status = :retry_wait AND (next_retry_at IS NULL OR next_retry_at <= CURRENT_TIMESTAMP())) "
            ")))"
            f"{scope_sql} "
            "ORDER BY priority DESC, requested_at ASC LIMIT 1",
            query_values,
        )
        if not candidate_rows:
            return None
        candidate = candidate_rows[0]
        queue_id = int(candidate["queue_id"])
        lease_duration = max(30, int(lease_seconds))
        expires_at = (datetime.now(timezone.utc) + timedelta(seconds=lease_duration)).isoformat()
        lease_expression = (
            f"DATEADD(second, {lease_duration}, CURRENT_TIMESTAMP())"
            if self.context.platform == "snowflake"
            else "CAST(:lease_expires_at AS TIMESTAMP)"
        )
        self.execute(
            f"UPDATE {self.table('ctl_ingestion_queue')} SET queue_status = :new_status, "
            "claimed_by_worker_id = :worker_id, lease_acquired_at = CURRENT_TIMESTAMP(), "
            f"lease_expires_at = {lease_expression}, last_heartbeat_at = CURRENT_TIMESTAMP(), "
            "attempt_count = CASE WHEN queue_status IN (:finalizing, :running) THEN attempt_count "
            "ELSE COALESCE(attempt_count, 0) + 1 END, "
            "started_at = COALESCE(started_at, CURRENT_TIMESTAMP()), message = :message WHERE queue_id = :queue_id AND ("
            "(queue_status = :finalizing AND lease_expires_at <= CURRENT_TIMESTAMP()) OR "
            "(queue_status = :running AND lease_expires_at <= CURRENT_TIMESTAMP() "
            "AND (last_heartbeat_at IS NULL OR last_heartbeat_at <= lease_expires_at)) OR "
            "(attempt_count < max_attempts AND (queue_status = :pending "
            "OR (queue_status = :retry_wait AND (next_retry_at IS NULL OR next_retry_at <= CURRENT_TIMESTAMP())) "
            ")))",
            {
                "new_status": "RUNNING",
                "worker_id": worker,
                "lease_expires_at": expires_at,
                "message": "Claimed",
                "queue_id": queue_id,
                "pending": "PENDING",
                "retry_wait": "RETRY_WAIT",
                "running": "RUNNING",
                "finalizing": "FINALIZING",
            },
        )
        claimed = self.query(
            f"SELECT * FROM {self.table('ctl_ingestion_queue')} WHERE queue_id = :queue_id "
            "AND queue_status = :status AND claimed_by_worker_id = :worker_id",
            {"queue_id": queue_id, "status": "RUNNING", "worker_id": worker},
        )
        return claimed[0] if len(claimed) == 1 else None

    def claim_queue_items(
        self,
        *,
        worker_id: str,
        lease_seconds: int = 300,
        logical_work_id: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Conditionally claim one ready processing-stage bundle in three SQL round trips."""
        worker = str(worker_id or "").strip()
        if not worker:
            raise ValueError("worker_id is required.")
        logical_id = str(logical_work_id or "").strip()
        scope_sql = " AND logical_work_id = :logical_work_id" if logical_id else ""
        values: Dict[str, Any] = {
            "pending": "PENDING", "retry_wait": "RETRY_WAIT",
            "running": "RUNNING", "finalizing": "FINALIZING",
        }
        if logical_id:
            values["logical_work_id"] = logical_id
        candidates = self.query(
            f"SELECT * FROM {self.table('ctl_ingestion_queue')} WHERE ("
            "(queue_status = :finalizing AND lease_expires_at <= CURRENT_TIMESTAMP()) OR "
            "(queue_status = :running AND lease_expires_at <= CURRENT_TIMESTAMP() "
            "AND (last_heartbeat_at IS NULL OR last_heartbeat_at <= lease_expires_at)) OR "
            "(attempt_count < max_attempts AND (queue_status = :pending OR "
            "(queue_status = :retry_wait AND (next_retry_at IS NULL OR next_retry_at <= CURRENT_TIMESTAMP())))))"
            f"{scope_sql} ORDER BY priority DESC, requested_at ASC LIMIT {max(1, min(1000, int(limit)))}",
            values,
        )
        if not candidates:
            return []

        def stage_of(candidate: Mapping[str, Any]) -> str:
            try:
                scope = json.loads(str(candidate.get("work_scope_json") or "{}"))
            except json.JSONDecodeError:
                return ""
            return str(
                (scope.get("runtime_context") or {}).get("processing_stage")
                or scope.get("processing_stage")
                or ""
            ).upper()

        selected_stage = stage_of(candidates[0])
        selected = [candidate for candidate in candidates if stage_of(candidate) == selected_stage]
        queue_ids = [int(item["queue_id"]) for item in selected]
        queue_parameters = {f"queue_id_{index}": queue_id for index, queue_id in enumerate(queue_ids)}
        lease_duration = max(30, int(lease_seconds))
        expires_at = (datetime.now(timezone.utc) + timedelta(seconds=lease_duration)).isoformat()
        lease_expression = (
            f"DATEADD(second, {lease_duration}, CURRENT_TIMESTAMP())"
            if self.context.platform == "snowflake"
            else "CAST(:lease_expires_at AS TIMESTAMP)"
        )
        self.execute(
            f"UPDATE {self.table('ctl_ingestion_queue')} SET queue_status = :new_status, "
            "claimed_by_worker_id = :worker_id, lease_acquired_at = CURRENT_TIMESTAMP(), "
            f"lease_expires_at = {lease_expression}, last_heartbeat_at = CURRENT_TIMESTAMP(), "
            "attempt_count = CASE WHEN queue_status IN (:finalizing, :running) THEN attempt_count "
            "ELSE COALESCE(attempt_count, 0) + 1 END, started_at = COALESCE(started_at, CURRENT_TIMESTAMP()), "
            "message = :message WHERE queue_id IN ("
            + ", ".join(f":queue_id_{index}" for index in range(len(queue_ids)))
            + ") AND ((queue_status = :finalizing AND lease_expires_at <= CURRENT_TIMESTAMP()) OR "
            "(queue_status = :running AND lease_expires_at <= CURRENT_TIMESTAMP() "
            "AND (last_heartbeat_at IS NULL OR last_heartbeat_at <= lease_expires_at)) OR "
            "(attempt_count < max_attempts AND (queue_status = :pending OR "
            "(queue_status = :retry_wait AND (next_retry_at IS NULL OR next_retry_at <= CURRENT_TIMESTAMP())))))",
            {
                **queue_parameters, "new_status": "RUNNING", "worker_id": worker,
                "lease_expires_at": expires_at, "message": "Claimed", "pending": "PENDING",
                "retry_wait": "RETRY_WAIT", "running": "RUNNING", "finalizing": "FINALIZING",
            },
        )
        claimed = self.query(
            f"SELECT * FROM {self.table('ctl_ingestion_queue')} WHERE queue_id IN ("
            + ", ".join(f":queue_id_{index}" for index in range(len(queue_ids)))
            + ") AND queue_status = :status AND claimed_by_worker_id = :worker_id",
            {**queue_parameters, "status": "RUNNING", "worker_id": worker},
        )
        order = {queue_id: index for index, queue_id in enumerate(queue_ids)}
        return sorted(claimed, key=lambda row: order[int(row["queue_id"])])

    def queue_items_for_logical_work(self, logical_work_id: str) -> List[Dict[str, Any]]:
        logical_id = str(logical_work_id or "").strip()
        if not logical_id:
            raise ValueError("logical_work_id is required.")
        return self.query(
            f"SELECT * FROM {self.table('ctl_ingestion_queue')} "
            "WHERE logical_work_id = :logical_work_id ORDER BY requested_at, queue_id",
            {"logical_work_id": logical_id},
        )

    def heartbeat_queue_item(self, *, queue_id: int, worker_id: str, lease_seconds: int = 300) -> None:
        lease_duration = max(30, int(lease_seconds))
        expires_at = (datetime.now(timezone.utc) + timedelta(seconds=lease_duration)).isoformat()
        lease_expression = (
            f"DATEADD(second, {lease_duration}, CURRENT_TIMESTAMP())"
            if self.context.platform == "snowflake"
            else "CAST(:lease_expires_at AS TIMESTAMP)"
        )
        self.execute(
            f"UPDATE {self.table('ctl_ingestion_queue')} SET last_heartbeat_at = CURRENT_TIMESTAMP(), "
            f"lease_expires_at = {lease_expression} WHERE queue_id = :queue_id "
            "AND queue_status = :status AND claimed_by_worker_id = :worker_id AND lease_expires_at > CURRENT_TIMESTAMP()",
            {"lease_expires_at": expires_at, "queue_id": int(queue_id), "status": "RUNNING", "worker_id": str(worker_id)},
        )
        rows = self.query(
            f"SELECT queue_id FROM {self.table('ctl_ingestion_queue')} WHERE queue_id = :queue_id "
            "AND queue_status = :status AND claimed_by_worker_id = :worker_id AND lease_expires_at > CURRENT_TIMESTAMP()",
            {"queue_id": int(queue_id), "status": "RUNNING", "worker_id": str(worker_id)},
        )
        if len(rows) != 1:
            raise RuntimeError("Queue lease was lost before heartbeat renewal.")

    def heartbeat_queue_items(
        self, *, queue_ids: Iterable[int], worker_id: str, lease_seconds: int = 300
    ) -> None:
        ids = [int(value) for value in queue_ids]
        if not ids:
            return
        if len(set(ids)) != len(ids):
            raise ValueError("Queue heartbeat identifiers must be unique.")
        lease_duration = max(30, int(lease_seconds))
        expires_at = (datetime.now(timezone.utc) + timedelta(seconds=lease_duration)).isoformat()
        lease_expression = (
            f"DATEADD(second, {lease_duration}, CURRENT_TIMESTAMP())"
            if self.context.platform == "snowflake"
            else "CAST(:lease_expires_at AS TIMESTAMP)"
        )
        parameters = {f"queue_id_{index}": value for index, value in enumerate(ids)}
        placeholders = ", ".join(f":queue_id_{index}" for index in range(len(ids)))
        self.execute(
            f"UPDATE {self.table('ctl_ingestion_queue')} SET last_heartbeat_at = CURRENT_TIMESTAMP(), "
            f"lease_expires_at = {lease_expression} WHERE queue_id IN ({placeholders}) "
            "AND queue_status = :status AND claimed_by_worker_id = :worker_id "
            "AND lease_expires_at > CURRENT_TIMESTAMP()",
            {**parameters, "lease_expires_at": expires_at, "status": "RUNNING", "worker_id": str(worker_id)},
        )
        owned = self.query(
            f"SELECT queue_id FROM {self.table('ctl_ingestion_queue')} WHERE queue_id IN ({placeholders}) "
            "AND queue_status = :status AND claimed_by_worker_id = :worker_id "
            "AND lease_expires_at > CURRENT_TIMESTAMP()",
            {**parameters, "status": "RUNNING", "worker_id": str(worker_id)},
        )
        if {int(row["queue_id"]) for row in owned} != set(ids):
            raise RuntimeError("One or more queue leases were lost before heartbeat renewal.")

    def begin_queue_finalization(self, *, queue_id: int, worker_id: str) -> None:
        self.execute(
            f"UPDATE {self.table('ctl_ingestion_queue')} SET queue_status = :finalizing, message = :message "
            "WHERE queue_id = :queue_id AND queue_status = :running "
            "AND claimed_by_worker_id = :worker_id AND lease_expires_at > CURRENT_TIMESTAMP()",
            {
                "finalizing": "FINALIZING",
                "message": "Finalizing verified target commit",
                "queue_id": int(queue_id),
                "running": "RUNNING",
                "worker_id": str(worker_id),
            },
        )
        rows = self.query(
            f"SELECT queue_id FROM {self.table('ctl_ingestion_queue')} WHERE queue_id = :queue_id "
            "AND queue_status = :finalizing AND claimed_by_worker_id = :worker_id",
            {"queue_id": int(queue_id), "finalizing": "FINALIZING", "worker_id": str(worker_id)},
        )
        if len(rows) != 1:
            raise RuntimeError("Queue ownership was lost before target-commit finalization.")

    def begin_queue_finalizations(self, *, queue_ids: Iterable[int], worker_id: str) -> None:
        ids = [int(value) for value in queue_ids]
        if not ids:
            return
        if len(set(ids)) != len(ids):
            raise ValueError("Queue finalization identifiers must be unique.")
        parameters = {f"queue_id_{index}": value for index, value in enumerate(ids)}
        placeholders = ", ".join(f":queue_id_{index}" for index in range(len(ids)))
        self.execute(
            f"UPDATE {self.table('ctl_ingestion_queue')} SET queue_status = :finalizing, message = :message "
            f"WHERE queue_id IN ({placeholders}) AND queue_status = :running "
            "AND claimed_by_worker_id = :worker_id AND lease_expires_at > CURRENT_TIMESTAMP()",
            {
                **parameters, "finalizing": "FINALIZING", "message": "Finalizing verified target commit",
                "running": "RUNNING", "worker_id": str(worker_id),
            },
        )
        owned = self.query(
            f"SELECT queue_id FROM {self.table('ctl_ingestion_queue')} WHERE queue_id IN ({placeholders}) "
            "AND queue_status = :finalizing AND claimed_by_worker_id = :worker_id "
            "AND lease_expires_at > CURRENT_TIMESTAMP()",
            {**parameters, "finalizing": "FINALIZING", "worker_id": str(worker_id)},
        )
        if {int(row["queue_id"]) for row in owned} != set(ids):
            raise RuntimeError("Queue ownership was lost before bulk target-commit finalization.")

    def create_run_attempt(
        self, queue_item: Mapping[str, Any], *, pipeline_name: str, worker_id: str
    ) -> Dict[str, Any]:
        queue_id = int(queue_item.get("queue_id") or 0)
        try:
            scope = json.loads(str(queue_item.get("work_scope_json") or "{}"))
            pin = scope["_metadata_snapshot"]
            runtime_context = validate_runtime_context(scope["runtime_context"])
        except (KeyError, TypeError, json.JSONDecodeError) as exc:
            raise ValueError("The queue item does not contain a valid immutable metadata snapshot.") from exc
        object_id = int(queue_item.get("ingestion_object_id") or 0)
        if (
            int(pin.get("ingestion_object_id") or 0) != object_id
            or int(runtime_context["ingestion_object_id"]) != object_id
            or int(runtime_context["queue_id"]) != queue_id
            or str(runtime_context["logical_work_id"]) != str(queue_item.get("logical_work_id") or "")
        ):
            raise RuntimeError("The queue metadata snapshot belongs to a different ingestion object.")
        obj = self.get_ingestion_object(object_id, int(pin.get("config_version") or 0))
        if not obj:
            raise ValueError("The exact queued ingestion-object version no longer exists.")
        expected_target = str(obj.get("target_table") or obj.get("target_bronze_table") or "")
        if (
            str(runtime_context.get("processing_stage") or "").upper()
            != str(obj.get("processing_stage") or "").upper()
            or str(runtime_context.get("target_table") or "") != expected_target
            or int(runtime_context.get("config_version") or 0)
            != int(obj.get("config_version") or 0)
            or int(runtime_context.get("mapping_version") or 0)
            != int(pin.get("mapping_version") or 0)
            or (
                runtime_context.get("source_object") is not None
                and str(runtime_context.get("source_object") or "")
                != str(obj.get("object_name") or "")
            )
        ):
            raise RuntimeError("The runtime context does not match its immutable metadata snapshot.")
        mapping = self.get_mapping_bundle(
            ingestion_object_id=object_id,
            processing_stage=str(obj.get("processing_stage") or ""),
            mapping_version=int(pin.get("mapping_version") or 0),
            expected_hash=str(pin.get("mapping_hash") or ""),
            expected_target=expected_target,
            require_active=_as_bool(obj.get("active_flag")) and _as_bool(obj.get("is_current")),
        )
        connection = None
        connection_pin = pin.get("connection")
        if obj.get("connection_id") is not None:
            if not isinstance(connection_pin, dict):
                raise RuntimeError("The queue metadata snapshot is missing its source-connection pin.")
            connection = self.get_connection(
                int(connection_pin.get("connection_id") or 0),
                int(connection_pin.get("config_version") or 0),
            )
            if not connection or str(connection.get("config_hash") or "") != str(
                connection_pin.get("config_hash") or ""
            ):
                raise RuntimeError("The queued source-connection snapshot failed validation.")
            if self.context.platform == "snowflake":
                from services.source_connection_validation import validate_deployment_database_binding

                validate_deployment_database_binding(connection, target_platform="snowflake")
        snapshot_id = self._runtime_snapshot(obj, mapping, connection=connection)["metadata_snapshot_id"]
        queued_snapshot_id = str(queue_item.get("metadata_snapshot_id") or "")
        snapshot_matches = snapshot_id == queued_snapshot_id
        ownership = self.query(
            f"SELECT queue_id FROM {self.table('ctl_ingestion_queue')} WHERE queue_id = :queue_id "
            "AND queue_status = :status AND claimed_by_worker_id = :worker_id "
            "AND attempt_count = :attempt_number AND lease_expires_at > CURRENT_TIMESTAMP()",
            {
                "queue_id": queue_id,
                "status": "RUNNING",
                "worker_id": str(worker_id),
                "attempt_number": int(queue_item.get("attempt_count") or 0),
            },
        )
        if len(ownership) != 1:
            raise RuntimeError("Queue ownership was lost before run-attempt creation.")
        attempt_number = int(queue_item.get("attempt_count") or 0)
        existing_attempts = self.query(
            f"SELECT * FROM {self.table('ctl_run')} WHERE queue_id = :queue_id "
            "AND attempt_number = :attempt_number "
            "ORDER BY created_at DESC LIMIT 2",
            {
                "queue_id": queue_id,
                "attempt_number": attempt_number,
            },
        )
        if len(existing_attempts) > 1:
            raise RuntimeError("Multiple run records exist for one queue attempt.")
        if existing_attempts:
            existing = existing_attempts[0]
            if (
                int(existing.get("ingestion_object_id") or 0) != object_id
                or int(existing.get("ingestion_object_config_version") or 0)
                != int(obj.get("config_version") or 0)
                or int(existing.get("mapping_version") or 0)
                != int(mapping.get("mapping_version") or 0)
                or str(existing.get("logical_work_id") or "")
                != str(queue_item.get("logical_work_id") or "")
            ):
                raise RuntimeError("The resumable run attempt does not match its immutable queue snapshot.")
            existing_status = str(existing.get("status") or "").upper()
            if existing_status == "FAILED":
                self.execute(
                    f"UPDATE {self.table('ctl_run')} SET status = :running, end_time = NULL, "
                    "recovery_action = :recovery_action WHERE run_id = :run_id AND status = :failed "
                    f"AND EXISTS (SELECT 1 FROM {self.table('ctl_ingestion_queue')} AS queue_item "
                    "WHERE queue_item.queue_id = :queue_id AND queue_item.queue_status = :running "
                    "AND queue_item.attempt_count = :attempt_number AND queue_item.claimed_by_worker_id = :worker_id "
                    "AND queue_item.lease_expires_at > CURRENT_TIMESTAMP())",
                    {
                        "running": "RUNNING",
                        "failed": "FAILED",
                        "recovery_action": "RESUME_PARTIAL_CONTROL_FINALIZATION",
                        "run_id": str(existing["run_id"]),
                        "queue_id": queue_id,
                        "attempt_number": attempt_number,
                        "worker_id": str(worker_id),
                    },
                )
                refreshed = self.query(
                    f"SELECT * FROM {self.table('ctl_run')} WHERE run_id = :run_id AND status = :status",
                    {"run_id": str(existing["run_id"]), "status": "RUNNING"},
                )
                if len(refreshed) != 1:
                    raise RuntimeError("Partially finalized run attempt could not be resumed safely.")
                existing = refreshed[0]
            elif existing_status != "RUNNING":
                raise RuntimeError("The existing queue attempt is not safely resumable.")
            return {
                "run": existing,
                "ingestion_object": obj,
                "mapping": mapping,
                "runtime_context": {
                    **runtime_context,
                    "attempt_number": attempt_number,
                    "runtime_run_id": str(existing["run_id"]),
                },
                "metadata_snapshot_matches": snapshot_matches,
                "active_metadata_snapshot_id": snapshot_id,
                "resumed_attempt": True,
            }
        run_id = str(uuid.uuid4())
        values = {
            "run_id": run_id,
            "queue_id": queue_id,
            "attempt_number": attempt_number,
            "logical_work_id": str(queue_item.get("logical_work_id") or ""),
            "idempotency_key": str(queue_item.get("idempotency_key") or ""),
            "ingestion_object_id": int(obj["ingestion_object_id"]),
            "source_system_id": obj.get("source_system_id"),
            "connection_id": obj.get("connection_id"),
            "ingestion_type": obj.get("ingestion_type"),
            "processing_stage": obj.get("processing_stage"),
            "source_layer": obj.get("source_layer"),
            "target_layer": obj.get("target_layer"),
            "target_table": obj.get("target_table") or obj.get("target_bronze_table"),
            "write_mode": obj.get("write_mode"),
            "pipeline_name": str(pipeline_name or "metadata_worker"),
            "metadata_snapshot_id": queued_snapshot_id,
            "connection_config_version": None,
            "ingestion_object_config_version": int(obj["config_version"]),
            "mapping_version": int(mapping["mapping_version"]),
            "metadata_hash": canonical_json_hash({"snapshot": snapshot_id, "work_scope": queue_item.get("work_scope_json")}),
            "source_boundary_hash": canonical_json_hash({"work_scope": queue_item.get("work_scope_json")}),
            "status": "RUNNING",
            "current_phase": "CONFIG_RESOLVED",
            "phase_status_json": json.dumps({"CONFIG_RESOLVED": "SUCCESS"}, separators=(",", ":")),
            "target_commit_status": "NOT_STARTED",
            "validation_status": "NOT_STARTED",
            "watermark_commit_status": "NOT_STARTED",
        }
        if obj.get("connection_id") is not None:
            values["connection_config_version"] = int(connection["config_version"])
        names = tuple(values)
        self.execute_batch([
            (
                f"INSERT INTO {self.table('ctl_run')} ({', '.join(names)}, start_time, created_at) VALUES ("
                + ", ".join(f":{name}" for name in names)
                + ", CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())",
                values,
            ),
            (
                f"UPDATE {self.table('ctl_ingestion_queue')} SET run_id = :run_id WHERE queue_id = :queue_id "
                "AND queue_status = :status AND attempt_count = :attempt_number "
                "AND claimed_by_worker_id = :worker_id AND lease_expires_at > CURRENT_TIMESTAMP()",
                {
                    "run_id": run_id,
                    "queue_id": queue_id,
                    "status": "RUNNING",
                    "attempt_number": values["attempt_number"],
                    "worker_id": str(worker_id),
                },
            ),
        ])
        rows = self.query(f"SELECT * FROM {self.table('ctl_run')} WHERE run_id = :run_id", {"run_id": run_id})
        queue_rows = self.query(
            f"SELECT queue_id FROM {self.table('ctl_ingestion_queue')} WHERE queue_id = :queue_id "
            "AND run_id = :run_id AND queue_status = :status AND claimed_by_worker_id = :worker_id",
            {"queue_id": queue_id, "run_id": run_id, "status": "RUNNING", "worker_id": str(worker_id)},
        )
        if len(rows) != 1 or len(queue_rows) != 1:
            raise RuntimeError("Run-attempt creation postcondition failed.")
        return {
            "run": rows[0],
            "ingestion_object": obj,
            "mapping": mapping,
            "runtime_context": {
                **runtime_context,
                "attempt_number": int(queue_item.get("attempt_count") or 0),
                "runtime_run_id": run_id,
            },
            "metadata_snapshot_matches": snapshot_matches,
            "active_metadata_snapshot_id": snapshot_id,
        }

    def create_run_attempts(
        self,
        queue_items: Iterable[Mapping[str, Any]],
        *,
        pipeline_name: str,
        worker_id: str,
    ) -> List[Dict[str, Any]]:
        """Create a normal, first-pass run bundle; resumable attempts retain the single-item path."""
        requested = [dict(item) for item in queue_items]
        if not requested:
            return []
        parsed = []
        for queue_item in requested:
            queue_id = int(queue_item.get("queue_id") or 0)
            try:
                scope = json.loads(str(queue_item.get("work_scope_json") or "{}"))
                pin = scope["_metadata_snapshot"]
                runtime_context = validate_runtime_context(scope["runtime_context"])
            except (KeyError, TypeError, json.JSONDecodeError) as exc:
                raise ValueError("A queue item does not contain a valid immutable metadata snapshot.") from exc
            object_id = int(queue_item.get("ingestion_object_id") or 0)
            if (
                int(pin.get("ingestion_object_id") or 0) != object_id
                or int(runtime_context["ingestion_object_id"]) != object_id
                or int(runtime_context["queue_id"]) != queue_id
                or str(runtime_context["logical_work_id"]) != str(queue_item.get("logical_work_id") or "")
            ):
                raise RuntimeError("A queue metadata snapshot belongs to a different ingestion object.")
            parsed.append({
                "queue": queue_item, "scope": scope, "pin": pin,
                "runtime_context": runtime_context, "object_id": object_id,
            })
        object_refs = [{
            "ingestion_object_id": item["object_id"],
            "config_version": int(item["pin"].get("config_version") or 0),
        } for item in parsed]
        objects = self.get_ingestion_objects(object_refs)
        mapping_refs = []
        connection_refs = {}
        for item in parsed:
            obj = objects[(item["object_id"], int(item["pin"].get("config_version") or 0))]
            target = str(obj.get("target_table") or obj.get("target_bronze_table") or "")
            context = item["runtime_context"]
            if (
                str(context.get("processing_stage") or "").upper() != str(obj.get("processing_stage") or "").upper()
                or str(context.get("target_table") or "") != target
                or int(context.get("config_version") or 0) != int(obj.get("config_version") or 0)
                or int(context.get("mapping_version") or 0) != int(item["pin"].get("mapping_version") or 0)
                or (
                    context.get("source_object") is not None
                    and str(context.get("source_object") or "") != str(obj.get("object_name") or "")
                )
            ):
                raise RuntimeError("A runtime context does not match its immutable metadata snapshot.")
            mapping_refs.append({
                "ingestion_object_id": item["object_id"],
                "processing_stage": str(obj.get("processing_stage") or ""),
                "mapping_version": int(item["pin"].get("mapping_version") or 0),
                "expected_hash": str(item["pin"].get("mapping_hash") or ""),
                "expected_target": target,
                "require_active": _as_bool(obj.get("active_flag")) and _as_bool(obj.get("is_current")),
            })
            connection_pin = item["pin"].get("connection")
            if obj.get("connection_id") is not None:
                if not isinstance(connection_pin, dict):
                    raise RuntimeError("A queue metadata snapshot is missing its source-connection pin.")
                key = (int(connection_pin.get("connection_id") or 0), int(connection_pin.get("config_version") or 0))
                connection_refs[key] = {"connection_id": key[0], "config_version": key[1]}
        mappings = self.get_mapping_bundles(mapping_refs)
        connections = self.get_connections(connection_refs.values()) if connection_refs else {}
        contexts = []
        for item in parsed:
            obj = objects[(item["object_id"], int(item["pin"].get("config_version") or 0))]
            stage = str(obj.get("processing_stage") or "").upper()
            mapping_version = int(item["pin"].get("mapping_version") or 0)
            mapping = mappings[(item["object_id"], stage, mapping_version)]
            connection = None
            connection_pin = item["pin"].get("connection")
            if obj.get("connection_id") is not None:
                key = (int(connection_pin.get("connection_id") or 0), int(connection_pin.get("config_version") or 0))
                connection = connections.get(key)
                if not connection or str(connection.get("config_hash") or "") != str(connection_pin.get("config_hash") or ""):
                    raise RuntimeError("A queued source-connection snapshot failed validation.")
                if self.context.platform == "snowflake":
                    from services.source_connection_validation import validate_deployment_database_binding

                    validate_deployment_database_binding(connection, target_platform="snowflake")
            snapshot_id = self._runtime_snapshot(obj, mapping, connection=connection)["metadata_snapshot_id"]
            contexts.append({
                **item, "obj": obj, "mapping": mapping, "connection": connection,
                "snapshot_id": snapshot_id,
                "snapshot_matches": snapshot_id == str(item["queue"].get("metadata_snapshot_id") or ""),
            })
        ownership_keys = [{
            "queue_id": int(item["queue"]["queue_id"]),
            "attempt_count": int(item["queue"].get("attempt_count") or 0),
        } for item in contexts]
        ownership_where, ownership_parameters = self._where_pairs(ownership_keys, prefix="owned")
        ownership = self.query(
            f"SELECT queue_id FROM {self.table('ctl_ingestion_queue')} WHERE ({ownership_where}) "
            "AND queue_status = :running AND claimed_by_worker_id = :worker_id "
            "AND lease_expires_at > CURRENT_TIMESTAMP()",
            {**ownership_parameters, "running": "RUNNING", "worker_id": str(worker_id)},
        )
        if {int(row["queue_id"]) for row in ownership} != {
            int(item["queue"]["queue_id"]) for item in contexts
        }:
            raise RuntimeError("Queue ownership was lost before bulk run-attempt creation.")
        attempt_keys = [{
            "queue_id": int(item["queue"]["queue_id"]),
            "attempt_number": int(item["queue"].get("attempt_count") or 0),
        } for item in contexts]
        attempts_where, attempts_parameters = self._where_pairs(attempt_keys, prefix="attempt")
        existing = self.query(
            f"SELECT queue_id FROM {self.table('ctl_run')} WHERE {attempts_where}", attempts_parameters
        )
        if existing:
            return [
                self.create_run_attempt(item["queue"], pipeline_name=pipeline_name, worker_id=worker_id)
                for item in contexts
            ]
        run_rows = []
        for item in contexts:
            queue_item = item["queue"]
            obj = item["obj"]
            mapping = item["mapping"]
            run_id = str(uuid.uuid4())
            run_rows.append({
                "run_id": run_id, "queue_id": int(queue_item["queue_id"]),
                "attempt_number": int(queue_item.get("attempt_count") or 0),
                "logical_work_id": str(queue_item.get("logical_work_id") or ""),
                "idempotency_key": str(queue_item.get("idempotency_key") or ""),
                "ingestion_object_id": int(obj["ingestion_object_id"]),
                "source_system_id": obj.get("source_system_id"), "connection_id": obj.get("connection_id"),
                "ingestion_type": obj.get("ingestion_type"), "processing_stage": obj.get("processing_stage"),
                "source_layer": obj.get("source_layer"), "target_layer": obj.get("target_layer"),
                "target_table": obj.get("target_table") or obj.get("target_bronze_table"),
                "write_mode": obj.get("write_mode"), "pipeline_name": str(pipeline_name or "metadata_worker"),
                "metadata_snapshot_id": str(queue_item.get("metadata_snapshot_id") or ""),
                "connection_config_version": int(item["connection"]["config_version"]) if item["connection"] else None,
                "ingestion_object_config_version": int(obj["config_version"]),
                "mapping_version": int(mapping["mapping_version"]),
                "metadata_hash": canonical_json_hash({
                    "snapshot": item["snapshot_id"], "work_scope": queue_item.get("work_scope_json"),
                }),
                "source_boundary_hash": canonical_json_hash({"work_scope": queue_item.get("work_scope_json")}),
                "status": "RUNNING", "current_phase": "CONFIG_RESOLVED",
                "phase_status_json": json.dumps({"CONFIG_RESOLVED": "SUCCESS"}, separators=(",", ":")),
                "target_commit_status": "NOT_STARTED", "validation_status": "NOT_STARTED",
                "watermark_commit_status": "NOT_STARTED",
            })
        names, source, source_parameters = self._source_rows(run_rows, prefix="run")
        self.execute(
            f"MERGE INTO {self.table('ctl_run')} AS target USING ({source}) AS source "
            "ON target.queue_id = source.queue_id AND target.attempt_number = source.attempt_number "
            f"WHEN NOT MATCHED THEN INSERT ({', '.join(names)}, start_time, created_at) VALUES ("
            + ", ".join("source." + name for name in names)
            + ", CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())",
            source_parameters,
        )
        self.execute(
            f"MERGE INTO {self.table('ctl_ingestion_queue')} AS target USING ({source}) AS source "
            "ON target.queue_id = source.queue_id AND target.attempt_count = source.attempt_number "
            "AND target.queue_status = :running AND target.claimed_by_worker_id = :worker_id "
            "AND target.lease_expires_at > CURRENT_TIMESTAMP() "
            "WHEN MATCHED THEN UPDATE SET run_id = source.run_id",
            {**source_parameters, "running": "RUNNING", "worker_id": str(worker_id)},
        )
        run_parameters = {f"run_id_{index}": row["run_id"] for index, row in enumerate(run_rows)}
        saved_runs = self.query(
            f"SELECT * FROM {self.table('ctl_run')} WHERE run_id IN ("
            + ", ".join(f":run_id_{index}" for index in range(len(run_rows)))
            + ")",
            run_parameters,
        )
        saved_by_id = {str(row["run_id"]): row for row in saved_runs}
        queue_links = self.query(
            f"SELECT queue_id, run_id FROM {self.table('ctl_ingestion_queue')} WHERE run_id IN ("
            + ", ".join(f":run_id_{index}" for index in range(len(run_rows)))
            + ") AND queue_status = :running AND claimed_by_worker_id = :worker_id",
            {**run_parameters, "running": "RUNNING", "worker_id": str(worker_id)},
        )
        if len(saved_by_id) != len(run_rows) or len(queue_links) != len(run_rows):
            raise RuntimeError("Bulk run-attempt creation postcondition failed.")
        results = []
        for item, run_row in zip(contexts, run_rows):
            results.append({
                "run": saved_by_id[run_row["run_id"]], "ingestion_object": item["obj"],
                "mapping": item["mapping"],
                "runtime_context": {
                    **item["runtime_context"],
                    "attempt_number": int(item["queue"].get("attempt_count") or 0),
                    "runtime_run_id": run_row["run_id"],
                },
                "metadata_snapshot_matches": item["snapshot_matches"],
                "active_metadata_snapshot_id": item["snapshot_id"],
            })
        return results

    def recover_committed_queue_item(self, *, queue_id: int, worker_id: str) -> Optional[Dict[str, Any]]:
        rows = self.query(
            f"SELECT * FROM {self.table('ctl_run')} WHERE queue_id = :queue_id "
            "AND target_commit_status = :committed AND validation_status IN (:passed, :warning) "
            "AND watermark_commit_status IN (:watermark_committed, :skipped) "
            "ORDER BY attempt_number DESC LIMIT 1",
            {
                "queue_id": int(queue_id),
                "committed": "COMMITTED",
                "passed": "PASSED",
                "warning": "WARNING",
                "watermark_committed": "COMMITTED",
                "skipped": "SKIPPED",
            },
        )
        if not rows:
            return None
        run = rows[0]
        if str(run.get("status") or "") == "RUNNING":
            self.finalize_successful_run(
                run_id=str(run["run_id"]),
                queue_id=int(queue_id),
                worker_id=str(worker_id),
            )
        elif str(run.get("status") or "") == "SUCCESS":
            self.execute(
                f"UPDATE {self.table('ctl_ingestion_queue')} SET queue_status = :success, run_id = :run_id, "
                "completed_at = CURRENT_TIMESTAMP(), lease_expires_at = NULL, claimed_by_worker_id = NULL, "
                "message = :message WHERE queue_id = :queue_id AND queue_status = :running "
                "AND claimed_by_worker_id = :worker_id",
                {
                    "success": "SUCCESS",
                    "run_id": str(run["run_id"]),
                    "message": "Recovered after committed target",
                    "queue_id": int(queue_id),
                    "running": "RUNNING",
                    "worker_id": str(worker_id),
                },
            )
        else:
            return None
        queue = self.query(
            f"SELECT queue_status FROM {self.table('ctl_ingestion_queue')} WHERE queue_id = :queue_id",
            {"queue_id": int(queue_id)},
        )
        if len(queue) != 1 or str(queue[0].get("queue_status") or "") != "SUCCESS":
            raise RuntimeError("Committed queue recovery postcondition failed.")
        return run

    def recover_committed_queue_items(
        self, *, queue_ids: Iterable[int], worker_id: str
    ) -> Dict[int, Dict[str, Any]]:
        ids = [int(value) for value in queue_ids]
        if not ids:
            return {}
        parameters = {f"queue_id_{index}": value for index, value in enumerate(ids)}
        rows = self.query(
            f"SELECT queue_id FROM {self.table('ctl_run')} WHERE queue_id IN ("
            + ", ".join(f":queue_id_{index}" for index in range(len(ids)))
            + ") AND target_commit_status = :committed AND validation_status IN (:passed, :warning) "
            "AND watermark_commit_status IN (:watermark_committed, :skipped)",
            {
                **parameters, "committed": "COMMITTED", "passed": "PASSED", "warning": "WARNING",
                "watermark_committed": "COMMITTED", "skipped": "SKIPPED",
            },
        )
        recovered = {}
        for queue_id in {int(row["queue_id"]) for row in rows}:
            run = self.recover_committed_queue_item(queue_id=queue_id, worker_id=worker_id)
            if run:
                recovered[queue_id] = run
        return recovered

    def assert_runtime_dependencies(
        self, ingestion_object: Mapping[str, Any], *, logical_work_id: str
    ) -> None:
        raw = str(ingestion_object.get("dependency_objects_json") or "").strip()
        if not raw:
            return
        try:
            contract = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError("The active dependency contract is invalid.") from exc
        dependencies = contract.get("dependencies") if isinstance(contract, dict) else None
        if not isinstance(dependencies, list):
            raise RuntimeError("The active dependency contract must contain a dependencies array.")
        for dependency in dependencies:
            if not isinstance(dependency, dict):
                raise RuntimeError("The active dependency contract contains an invalid entry.")
            rows = self.query(
                f"SELECT run_id FROM {self.table('ctl_run')} WHERE ingestion_object_id = :ingestion_object_id "
                "AND ingestion_object_config_version = :config_version AND mapping_version = :mapping_version "
                "AND logical_work_id = :logical_work_id AND status = :success AND target_commit_status = :committed "
                "ORDER BY created_at DESC LIMIT 1",
                {
                    "ingestion_object_id": int(dependency.get("ingestion_object_id") or 0),
                    "config_version": int(dependency.get("config_version") or 0),
                    "mapping_version": int(dependency.get("mapping_version") or 0),
                    "logical_work_id": str(logical_work_id or ""),
                    "success": "SUCCESS",
                    "committed": "COMMITTED",
                },
            )
            if len(rows) != 1:
                raise RuntimeError(
                    f"Runtime dependency is not committed: ingestion_object_id={dependency.get('ingestion_object_id')}"
                )

    def assert_runtime_dependencies_batch(
        self, ingestion_objects: Iterable[Mapping[str, Any]], *, logical_work_id: str
    ) -> None:
        required = {}
        for ingestion_object in ingestion_objects:
            raw = str(ingestion_object.get("dependency_objects_json") or "").strip()
            if not raw:
                continue
            try:
                dependencies = json.loads(raw).get("dependencies")
            except json.JSONDecodeError as exc:
                raise RuntimeError("An active dependency contract is invalid.") from exc
            if not isinstance(dependencies, list):
                raise RuntimeError("An active dependency contract must contain a dependencies array.")
            for dependency in dependencies:
                if not isinstance(dependency, dict):
                    raise RuntimeError("An active dependency contract contains an invalid entry.")
                key = (
                    int(dependency.get("ingestion_object_id") or 0),
                    int(dependency.get("config_version") or 0),
                    int(dependency.get("mapping_version") or 0),
                )
                required[key] = {
                    "ingestion_object_id": key[0],
                    "ingestion_object_config_version": key[1],
                    "mapping_version": key[2],
                }
        if not required:
            return
        where, parameters = self._where_pairs(required.values(), prefix="runtime_dependency")
        rows = self.query(
            f"SELECT ingestion_object_id, ingestion_object_config_version, mapping_version "
            f"FROM {self.table('ctl_run')} WHERE ({where}) AND logical_work_id = :logical_work_id "
            "AND status = :success AND target_commit_status = :committed",
            {
                **parameters, "logical_work_id": str(logical_work_id or ""),
                "success": "SUCCESS", "committed": "COMMITTED",
            },
        )
        committed = {
            (
                int(row.get("ingestion_object_id") or 0),
                int(row.get("ingestion_object_config_version") or 0),
                int(row.get("mapping_version") or 0),
            )
            for row in rows
        }
        missing = set(required) - committed
        if missing:
            raise RuntimeError(f"Runtime dependencies are not committed: {sorted(missing)}")

    def enqueue_ready_downstream(
        self,
        *,
        completed_object: Mapping[str, Any],
        logical_work_id: str,
        parent_work_scope: Mapping[str, Any],
        requested_by: str = "metadata-runtime-worker",
    ) -> List[Dict[str, Any]]:
        next_stage = {
            "SOURCE_TO_BRONZE": "BRONZE_TO_SILVER",
            "BRONZE_TO_SILVER": "SILVER_TO_GOLD",
        }.get(str(completed_object.get("processing_stage") or "").upper())
        if not next_stage:
            return []
        candidates = self.query(
            f"SELECT * FROM {self.table('cfg_ingestion_object')} WHERE processing_stage = :processing_stage "
            "AND active_flag = :active_flag AND is_current = :is_current",
            {"processing_stage": next_stage, "active_flag": True, "is_current": True},
        )
        completed_id = int(completed_object.get("ingestion_object_id") or 0)
        queued: List[Dict[str, Any]] = []
        for candidate in candidates:
            try:
                dependency_contract = json.loads(str(candidate.get("dependency_objects_json") or "{}"))
                dependencies = dependency_contract["dependencies"]
            except (KeyError, TypeError, json.JSONDecodeError) as exc:
                raise RuntimeError("An active downstream object has an invalid dependency contract.") from exc
            if not isinstance(dependencies, list) or not dependencies:
                raise RuntimeError("An active downstream object has no dependency pins.")
            if completed_id not in {
                int(dependency.get("ingestion_object_id") or 0)
                for dependency in dependencies
                if isinstance(dependency, dict)
            }:
                continue
            ready = True
            for dependency in dependencies:
                rows = self.query(
                    f"SELECT run_id FROM {self.table('ctl_run')} WHERE ingestion_object_id = :ingestion_object_id "
                    "AND ingestion_object_config_version = :config_version AND mapping_version = :mapping_version "
                    "AND logical_work_id = :logical_work_id AND status = :success "
                    "AND target_commit_status = :committed ORDER BY created_at DESC LIMIT 1",
                    {
                        "ingestion_object_id": int(dependency.get("ingestion_object_id") or 0),
                        "config_version": int(dependency.get("config_version") or 0),
                        "mapping_version": int(dependency.get("mapping_version") or 0),
                        "logical_work_id": str(logical_work_id),
                        "success": "SUCCESS",
                        "committed": "COMMITTED",
                    },
                )
                if len(rows) != 1:
                    ready = False
                    break
            if not ready:
                continue
            scope = {
                key: value
                for key, value in dict(parent_work_scope or {}).items()
                if key not in {"_metadata_snapshot", "runtime_context", "processing_stage", "target_table"}
            }
            scope.update({
                "processing_stage": next_stage,
                "target_table": candidate.get("target_table") or candidate.get("target_bronze_table"),
            })
            queued.append(
                self.enqueue_work(
                    ingestion_object_id=int(candidate["ingestion_object_id"]),
                    trigger_type="DEPENDENCY_SUCCESS",
                    work_scope=scope,
                    requested_by=requested_by,
                    priority=200 if next_stage == "BRONZE_TO_SILVER" else 100,
                    logical_work_id=str(logical_work_id),
                )
            )
        return queued

    def enqueue_ready_downstream_batch(
        self, completed: Iterable[Mapping[str, Any]], *, requested_by: str = "metadata-runtime-worker"
    ) -> List[Dict[str, Any]]:
        items = [dict(item) for item in completed]
        if not items:
            return []
        completed_objects = [dict(item["completed_object"]) for item in items]
        stages = {str(obj.get("processing_stage") or "").upper() for obj in completed_objects}
        logical_ids = {str(item.get("logical_work_id") or "") for item in items}
        if len(stages) != 1 or len(logical_ids) != 1:
            raise ValueError("A downstream-release bundle must share one stage and logical work identity.")
        next_stage = {
            "SOURCE_TO_BRONZE": "BRONZE_TO_SILVER",
            "BRONZE_TO_SILVER": "SILVER_TO_GOLD",
        }.get(next(iter(stages)))
        if not next_stage:
            return []
        logical_work_id = next(iter(logical_ids))
        candidates = self.query(
            f"SELECT * FROM {self.table('cfg_ingestion_object')} WHERE processing_stage = :processing_stage "
            "AND active_flag = :active_flag AND is_current = :is_current",
            {"processing_stage": next_stage, "active_flag": True, "is_current": True},
        )
        completed_scopes = {
            int(item["completed_object"].get("ingestion_object_id") or 0): dict(item.get("parent_work_scope") or {})
            for item in items
        }
        candidate_dependencies = {}
        dependency_keys = {}
        for candidate in candidates:
            try:
                dependencies = json.loads(str(candidate.get("dependency_objects_json") or "{}"))["dependencies"]
            except (KeyError, TypeError, json.JSONDecodeError) as exc:
                raise RuntimeError("An active downstream object has an invalid dependency contract.") from exc
            if not isinstance(dependencies, list) or not dependencies:
                raise RuntimeError("An active downstream object has no dependency pins.")
            if not any(int(item.get("ingestion_object_id") or 0) in completed_scopes for item in dependencies):
                continue
            candidate_id = int(candidate["ingestion_object_id"])
            candidate_dependencies[candidate_id] = (candidate, dependencies)
            for dependency in dependencies:
                key = (
                    int(dependency.get("ingestion_object_id") or 0),
                    int(dependency.get("config_version") or 0),
                    int(dependency.get("mapping_version") or 0),
                )
                dependency_keys[key] = {
                    "ingestion_object_id": key[0],
                    "ingestion_object_config_version": key[1],
                    "mapping_version": key[2],
                }
        if not candidate_dependencies:
            return []
        where, parameters = self._where_pairs(dependency_keys.values(), prefix="dependency")
        successful = self.query(
            f"SELECT ingestion_object_id, ingestion_object_config_version, mapping_version "
            f"FROM {self.table('ctl_run')} WHERE ({where}) AND logical_work_id = :logical_work_id "
            "AND status = :success AND target_commit_status = :committed",
            {
                **parameters, "logical_work_id": logical_work_id,
                "success": "SUCCESS", "committed": "COMMITTED",
            },
        )
        successful_keys = {
            (
                int(row.get("ingestion_object_id") or 0),
                int(row.get("ingestion_object_config_version") or 0),
                int(row.get("mapping_version") or 0),
            )
            for row in successful
        }
        requests = []
        for candidate, dependencies in candidate_dependencies.values():
            required = {
                (
                    int(item.get("ingestion_object_id") or 0),
                    int(item.get("config_version") or 0),
                    int(item.get("mapping_version") or 0),
                )
                for item in dependencies
            }
            if not required.issubset(successful_keys):
                continue
            parent_scope = next(
                (
                    completed_scopes[int(item.get("ingestion_object_id") or 0)]
                    for item in dependencies
                    if int(item.get("ingestion_object_id") or 0) in completed_scopes
                ),
                {},
            )
            scope = {
                key: value for key, value in parent_scope.items()
                if key not in {"_metadata_snapshot", "runtime_context", "processing_stage", "target_table"}
            }
            scope.update({
                "processing_stage": next_stage,
                "target_table": candidate.get("target_table") or candidate.get("target_bronze_table"),
            })
            requests.append({
                "ingestion_object_id": int(candidate["ingestion_object_id"]),
                "trigger_type": "DEPENDENCY_SUCCESS", "work_scope": scope,
                "requested_by": requested_by,
                "priority": 200 if next_stage == "BRONZE_TO_SILVER" else 100,
                "logical_work_id": logical_work_id,
            })
        return self.enqueue_work_batch(requests)

    def release_ready_downstream_from_successes(
        self, *, logical_work_id: Optional[str] = None, limit: int = 100
    ) -> List[Dict[str, Any]]:
        logical_id = str(logical_work_id or "").strip()
        recovery_stage = None
        if logical_id:
            queued = self.queue_items_for_logical_work(logical_id)
            queued_stages = set()
            queued_gold_ids = set()
            for item in queued:
                try:
                    scope = json.loads(str(item.get("work_scope_json") or "{}"))
                except json.JSONDecodeError:
                    continue
                item_stage = str(
                    (scope.get("runtime_context") or {}).get("processing_stage")
                    or scope.get("processing_stage")
                    or ""
                ).upper()
                queued_stages.add(item_stage)
                if item_stage == "SILVER_TO_GOLD":
                    queued_gold_ids.add(int(item.get("ingestion_object_id") or 0))
            if "SILVER_TO_GOLD" in queued_stages:
                active_gold = self.query(
                    f"SELECT ingestion_object_id FROM {self.table('cfg_ingestion_object')} "
                    "WHERE processing_stage = :processing_stage AND active_flag = :active_flag "
                    "AND is_current = :is_current",
                    {"processing_stage": "SILVER_TO_GOLD", "active_flag": True, "is_current": True},
                )
                if {int(item["ingestion_object_id"]) for item in active_gold}.issubset(queued_gold_ids):
                    return []
            recovery_stage = "BRONZE_TO_SILVER" if "BRONZE_TO_SILVER" in queued_stages else "SOURCE_TO_BRONZE"
        scope_sql = " AND runtime_run.logical_work_id = :logical_work_id" if logical_id else ""
        stage_sql = " AND completed_object.processing_stage = :recovery_stage" if recovery_stage else ""
        parameters: Dict[str, Any] = {"success": "SUCCESS", "committed": "COMMITTED"}
        if logical_id:
            parameters["logical_work_id"] = logical_id
        if recovery_stage:
            parameters["recovery_stage"] = recovery_stage
        rows = self.query(
            f"SELECT runtime_run.ingestion_object_id, runtime_run.ingestion_object_config_version, "
            f"runtime_run.logical_work_id, queue_item.work_scope_json "
            f"FROM {self.table('ctl_run')} AS runtime_run "
            f"JOIN {self.table('ctl_ingestion_queue')} AS queue_item "
            "ON queue_item.queue_id = runtime_run.queue_id "
            f"JOIN {self.table('cfg_ingestion_object')} AS completed_object "
            "ON completed_object.ingestion_object_id = runtime_run.ingestion_object_id "
            "AND completed_object.config_version = runtime_run.ingestion_object_config_version "
            "WHERE runtime_run.status = :success AND runtime_run.target_commit_status = :committed "
            f"{scope_sql}{stage_sql} "
            f"ORDER BY runtime_run.created_at DESC LIMIT {max(1, min(1000, int(limit)))}",
            parameters,
        )
        released: List[Dict[str, Any]] = []
        for row in rows:
            completed = self.get_ingestion_object(
                int(row.get("ingestion_object_id") or 0),
                int(row.get("ingestion_object_config_version") or 0),
            )
            if not completed:
                raise RuntimeError("A successful runtime attempt references missing immutable metadata.")
            try:
                scope = json.loads(str(row.get("work_scope_json") or "{}"))
            except json.JSONDecodeError as exc:
                raise RuntimeError("A successful runtime attempt has invalid work-scope JSON.") from exc
            released.extend(
                self.enqueue_ready_downstream(
                    completed_object=completed,
                    logical_work_id=str(row.get("logical_work_id") or ""),
                    parent_work_scope=scope,
                )
            )
        return released

    def update_run_phase(
        self,
        run_id: str,
        phase: str,
        *,
        queue_id: Optional[int] = None,
        worker_id: Optional[str] = None,
        **fields: Any,
    ) -> None:
        allowed = {
            "rows_read", "rows_written", "files_processed", "bytes_processed", "execution_time_seconds",
            "target_write_id", "target_commit_status", "validation_status", "validation_summary_json",
            "watermark_before", "watermark_after", "watermark_commit_status", "recovery_action",
        }
        unknown = set(fields) - allowed
        if unknown:
            raise ValueError(f"Unsupported ctl_run update fields: {sorted(unknown)}")
        if (queue_id is None) != (worker_id is None):
            raise ValueError("queue_id and worker_id must be supplied together for a fenced phase update.")
        ownership_sql = ""
        ownership_values: Dict[str, Any] = {}
        if queue_id is not None:
            ownership_sql = (
                f" AND EXISTS (SELECT 1 FROM {self.table('ctl_ingestion_queue')} AS queue_item "
                "WHERE queue_item.queue_id = :queue_id AND queue_item.claimed_by_worker_id = :worker_id "
                "AND queue_item.queue_status IN (:running, :finalizing) "
                "AND queue_item.lease_expires_at > CURRENT_TIMESTAMP())"
            )
            ownership_values = {
                "queue_id": int(queue_id),
                "worker_id": str(worker_id),
                "running": "RUNNING",
                "finalizing": "FINALIZING",
            }
        current = self.query(
            f"SELECT phase_status_json FROM {self.table('ctl_run')} WHERE run_id = :run_id "
            f"AND status = :status{ownership_sql}",
            {"run_id": str(run_id), "status": "RUNNING", **ownership_values},
        )
        if len(current) != 1:
            raise RuntimeError("The RUNNING attempt was not found for phase update.")
        try:
            phase_status = json.loads(str(current[0].get("phase_status_json") or "{}"))
        except json.JSONDecodeError as exc:
            raise RuntimeError("The run phase-status contract is invalid.") from exc
        phase_status[str(phase).upper()] = "SUCCESS"
        values = {name: value for name, value in fields.items()}
        values.update({
            "run_id": str(run_id),
            "current_phase": str(phase).upper(),
            "phase_status_json": json.dumps(phase_status, sort_keys=True, separators=(",", ":")),
        })
        assignments = ["current_phase = :current_phase", "phase_status_json = :phase_status_json"] + [f"{name} = :{name}" for name in fields]
        self.execute(
            f"UPDATE {self.table('ctl_run')} SET {', '.join(assignments)} WHERE run_id = :run_id "
            f"AND status = :status{ownership_sql}",
            {**values, "status": "RUNNING", **ownership_values},
        )

    def update_run_phases(
        self,
        *,
        phase: str,
        updates: Iterable[Mapping[str, Any]],
        worker_id: str,
    ) -> None:
        requested = [dict(item) for item in updates]
        if not requested:
            return
        allowed = {
            "rows_read", "rows_written", "files_processed", "bytes_processed", "execution_time_seconds",
            "target_write_id", "target_commit_status", "validation_status", "validation_summary_json",
            "watermark_before", "watermark_after", "watermark_commit_status", "recovery_action",
        }
        field_names = tuple(key for key in requested[0] if key not in {"run_id", "queue_id"})
        if set(field_names) - allowed or any(
            tuple(key for key in item if key not in {"run_id", "queue_id"}) != field_names
            for item in requested
        ):
            raise ValueError("Bulk run-phase updates must use one supported field contract.")
        keys = [
            {"run_id": str(item["run_id"]), "queue_id": int(item["queue_id"])}
            for item in requested
        ]
        where, parameters = self._where_pairs(keys, prefix="phase")
        where = where.replace("run_id =", "runtime_run.run_id =").replace(
            "queue_id =", "runtime_run.queue_id ="
        )
        current = self.query(
            f"SELECT runtime_run.run_id, runtime_run.phase_status_json FROM {self.table('ctl_run')} AS runtime_run "
            f"JOIN {self.table('ctl_ingestion_queue')} AS queue_item ON queue_item.queue_id = runtime_run.queue_id "
            f"WHERE ({where}) AND runtime_run.status = :running "
            "AND queue_item.claimed_by_worker_id = :worker_id "
            "AND queue_item.queue_status IN (:running, :finalizing) "
            "AND queue_item.lease_expires_at > CURRENT_TIMESTAMP()",
            {**parameters, "running": "RUNNING", "finalizing": "FINALIZING", "worker_id": str(worker_id)},
        )
        phase_by_run = {}
        for row in current:
            try:
                status = json.loads(str(row.get("phase_status_json") or "{}"))
            except json.JSONDecodeError as exc:
                raise RuntimeError("A run phase-status contract is invalid.") from exc
            status[str(phase).upper()] = "SUCCESS"
            phase_by_run[str(row["run_id"])] = json.dumps(status, sort_keys=True, separators=(",", ":"))
        if set(phase_by_run) != {item["run_id"] for item in keys}:
            raise RuntimeError("One or more RUNNING attempts were not found for bulk phase update.")
        rows = []
        for item in requested:
            rows.append({
                "run_id": str(item["run_id"]),
                "queue_id": int(item["queue_id"]),
                "current_phase": str(phase).upper(),
                "phase_status_json": phase_by_run[str(item["run_id"])],
                **{name: item.get(name) for name in field_names},
            })
        _, source, source_parameters = self._source_rows(rows, prefix="runphase")
        assignments = [
            "current_phase = source.current_phase",
            "phase_status_json = source.phase_status_json",
            *[f"{name} = source.{name}" for name in field_names],
        ]
        self.execute(
            f"MERGE INTO {self.table('ctl_run')} AS target USING (SELECT source.* FROM ({source}) AS source "
            f"JOIN {self.table('ctl_ingestion_queue')} AS queue_item ON queue_item.queue_id = source.queue_id "
            "WHERE queue_item.claimed_by_worker_id = :worker_id "
            "AND queue_item.queue_status IN (:running, :finalizing) "
            "AND queue_item.lease_expires_at > CURRENT_TIMESTAMP()) AS source "
            "ON target.run_id = source.run_id AND target.status = :running "
            f"WHEN MATCHED THEN UPDATE SET {', '.join(assignments)}",
            {**source_parameters, "worker_id": str(worker_id), "running": "RUNNING", "finalizing": "FINALIZING"},
        )
        verified = self.query(
            f"SELECT run_id FROM {self.table('ctl_run')} WHERE run_id IN ("
            + ", ".join(f":run_id_{index}" for index in range(len(rows)))
            + ") AND current_phase = :phase AND status = :running",
            {
                **{f"run_id_{index}": row["run_id"] for index, row in enumerate(rows)},
                "phase": str(phase).upper(), "running": "RUNNING",
            },
        )
        if {str(row["run_id"]) for row in verified} != {row["run_id"] for row in rows}:
            raise RuntimeError("Bulk run-phase update postcondition failed.")

    @staticmethod
    def _safe_error_text(value: Any, limit: int) -> str:
        return redact_sensitive_text(value, limit)

    @classmethod
    def _redact_sensitive(cls, value: Any, *, key: str = "") -> Any:
        return redact_sensitive(value, key=key)

    def record_run_error(
        self,
        *,
        run: Mapping[str, Any],
        error_stage: str,
        error: BaseException,
        retryable: bool,
        detail: Optional[Mapping[str, Any]] = None,
        worker_id: Optional[str] = None,
    ) -> str:
        error_id = str(uuid.uuid4())
        safe_detail = self._safe_error_text(
            json.dumps(self._redact_sensitive(dict(detail or {})), default=str), 12000
        )
        values = {
            "error_id": error_id,
            "run_id": str(run.get("run_id") or ""),
            "queue_id": int(run.get("queue_id") or 0),
            "attempt_number": int(run.get("attempt_number") or 0),
            "ingestion_object_id": int(run.get("ingestion_object_id") or 0),
            "source_system_id": run.get("source_system_id"),
            "connection_id": run.get("connection_id"),
            "error_stage": str(error_stage or "FINALIZE").upper(),
            "error_phase": str(run.get("current_phase") or error_stage or "FINALIZE").upper(),
            "error_code": type(error).__name__,
            "error_message": self._safe_error_text(error, 2000),
            "error_detail": safe_detail,
            "severity": "ERROR",
            "retryable_flag": bool(retryable),
            "retry_action": "RETRY" if retryable else "FIX_CONFIGURATION",
        }
        names = tuple(values)
        if worker_id is None:
            self.execute(
                f"INSERT INTO {self.table('ctl_error_log')} ({', '.join(names)}, error_time) VALUES ("
                + ", ".join(f":{name}" for name in names)
                + ", CURRENT_TIMESTAMP())",
                values,
            )
        else:
            self.execute(
                f"INSERT INTO {self.table('ctl_error_log')} ({', '.join(names)}, error_time) SELECT "
                + ", ".join(f":{name}" for name in names)
                + f", CURRENT_TIMESTAMP() WHERE EXISTS (SELECT 1 FROM {self.table('ctl_ingestion_queue')} "
                "WHERE queue_id = :owned_queue_id AND claimed_by_worker_id = :worker_id "
                "AND queue_status IN (:running, :finalizing) AND lease_expires_at > CURRENT_TIMESTAMP())",
                {
                    **values,
                    "owned_queue_id": int(run.get("queue_id") or 0),
                    "worker_id": str(worker_id),
                    "running": "RUNNING",
                    "finalizing": "FINALIZING",
                },
            )
        return error_id

    def finalize_failed_run(
        self, *, run: Mapping[str, Any], worker_id: str, retryable: bool, message: str
    ) -> None:
        attempts = int(run.get("attempt_number") or 0)
        queue_id = int(run.get("queue_id") or 0)
        queue_rows = self.query(
            f"SELECT max_attempts FROM {self.table('ctl_ingestion_queue')} WHERE queue_id = :queue_id",
            {"queue_id": queue_id},
        )
        max_attempts = int(queue_rows[0].get("max_attempts") or attempts) if queue_rows else attempts
        queue_status = "RETRY_WAIT" if retryable and attempts < max_attempts else "FAILED"
        retry_at = (datetime.now(timezone.utc) + timedelta(minutes=min(60, 2 ** max(0, attempts - 1)))).isoformat()
        ownership = (
            f"EXISTS (SELECT 1 FROM {self.table('ctl_ingestion_queue')} AS queue_item "
            "WHERE queue_item.queue_id = :queue_id AND queue_item.claimed_by_worker_id = :worker_id "
            "AND queue_item.queue_status = :running AND queue_item.lease_expires_at > CURRENT_TIMESTAMP())"
        )
        self.execute_batch([
            (
                f"UPDATE {self.table('ctl_run')} SET status = :status, end_time = CURRENT_TIMESTAMP(), "
                "execution_time_seconds = TIMESTAMPDIFF(SECOND, start_time, CURRENT_TIMESTAMP()), "
                f"recovery_action = :recovery_action WHERE run_id = :run_id AND status = :running AND {ownership}",
                {
                    "status": "FAILED",
                    "recovery_action": "RETRY" if queue_status == "RETRY_WAIT" else "MANUAL",
                    "run_id": run["run_id"],
                    "queue_id": queue_id,
                    "worker_id": str(worker_id),
                    "running": "RUNNING",
                },
            ),
            (
                f"UPDATE {self.table('ctl_ingestion_queue')} SET queue_status = :queue_status, "
                "next_retry_at = CAST(:next_retry_at AS TIMESTAMP), completed_at = CASE WHEN :queue_status = :failed THEN CURRENT_TIMESTAMP() ELSE NULL END, "
                "lease_expires_at = NULL, claimed_by_worker_id = NULL, message = :message "
                "WHERE queue_id = :queue_id AND queue_status = :running "
                "AND claimed_by_worker_id = :worker_id AND lease_expires_at > CURRENT_TIMESTAMP()",
                {"queue_status": queue_status, "next_retry_at": retry_at, "failed": "FAILED", "message": self._safe_error_text(message, 2000), "queue_id": queue_id, "worker_id": str(worker_id), "running": "RUNNING"},
            ),
        ])

    def release_queue_for_same_attempt_resume(
        self, *, queue_id: int, worker_id: str, message: str
    ) -> None:
        """Release an ambiguous target submission without creating a new attempt."""
        self.execute(
            f"UPDATE {self.table('ctl_ingestion_queue')} SET claimed_by_worker_id = NULL, "
            "lease_expires_at = CURRENT_TIMESTAMP(), last_heartbeat_at = CURRENT_TIMESTAMP(), message = :message "
            "WHERE queue_id = :queue_id AND queue_status = :running "
            "AND claimed_by_worker_id = :worker_id AND lease_expires_at > CURRENT_TIMESTAMP()",
            {
                "message": self._safe_error_text(message, 2000),
                "queue_id": int(queue_id),
                "running": "RUNNING",
                "worker_id": str(worker_id),
            },
        )

    def get_watermark(self, ingestion_object_id: int) -> Optional[Dict[str, Any]]:
        rows = self.query(
            f"SELECT * FROM {self.table('ctl_watermark')} WHERE ingestion_object_id = :ingestion_object_id",
            {"ingestion_object_id": int(ingestion_object_id)},
        )
        if len(rows) > 1:
            raise RuntimeError("More than one watermark row exists for the ingestion object.")
        return rows[0] if rows else None

    def initialize_watermark(self, ingestion_object_id: int, initial_value: Optional[str] = None) -> Dict[str, Any]:
        obj = self.get_active_ingestion_object(int(ingestion_object_id))
        stateful = bool(obj) and (
            str(obj.get("load_type") or "").upper() in {"INCREMENTAL", "CDC", "CURSOR"}
            or bool(str(obj.get("watermark_column") or "").strip())
            or bool(str(obj.get("checkpoint_type") or "").strip())
        )
        if not stateful:
            raise ValueError("Watermarks may be initialized only for active stateful ingestion objects.")
        values = {
            "watermark_id": stable_bigint("watermark", int(ingestion_object_id)),
            "ingestion_object_id": int(ingestion_object_id),
            "watermark_type": str(obj.get("checkpoint_type") or "TIMESTAMP").upper(),
            "watermark_column": obj.get("watermark_column"),
            "last_watermark_value": initial_value,
            "committed_watermark_value": initial_value,
            "watermark_version": 0,
            "commit_status": "COMMITTED",
            "boundary_operator": str(obj.get("boundary_operator") or ">").upper(),
            "checkpoint_state_json": json.dumps({}, separators=(",", ":")),
        }
        names = tuple(values)
        self.execute(
            f"MERGE INTO {self.table('ctl_watermark')} AS target USING (SELECT "
            + ", ".join(f":{name} AS {name}" for name in names)
            + ") AS source ON target.ingestion_object_id = source.ingestion_object_id "
            + f"WHEN NOT MATCHED THEN INSERT ({', '.join(names)}, committed_at, updated_at) VALUES ("
            + ", ".join(f"source.{name}" for name in names)
            + ", CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())",
            values,
        )
        saved = self.get_watermark(int(ingestion_object_id))
        if not saved or int(saved.get("watermark_id") or 0) != int(values["watermark_id"]):
            raise RuntimeError("Watermark initialization postcondition failed.")
        return saved

    def stage_watermark_candidate(self, *, run_id: str, candidate_value: str, expected_version: int) -> None:
        runs = self.query(
            f"SELECT target_commit_status, validation_status FROM {self.table('ctl_run')} WHERE run_id = :run_id",
            {"run_id": str(run_id)},
        )
        if len(runs) != 1 or str(runs[0].get("target_commit_status") or "") != "COMMITTED" or str(
            runs[0].get("validation_status") or ""
        ) not in {"PASSED", "WARNING"}:
            raise RuntimeError("Watermark staging requires a committed target write and successful blocking validation.")
        self.execute(
            f"UPDATE {self.table('ctl_watermark')} SET candidate_watermark_value = :candidate, "
            "candidate_run_id = :run_id, candidate_created_at = CURRENT_TIMESTAMP(), commit_status = :status, updated_at = CURRENT_TIMESTAMP() "
            "WHERE ingestion_object_id = (SELECT ingestion_object_id FROM " + self.table('ctl_run') + " WHERE run_id = :run_id) "
            "AND watermark_version = :expected_version AND (candidate_run_id IS NULL OR candidate_run_id = :run_id)",
            {"candidate": str(candidate_value), "run_id": str(run_id), "status": "CANDIDATE_STAGED", "expected_version": int(expected_version)},
        )
        rows = self.query(
            f"SELECT candidate_run_id FROM {self.table('ctl_watermark')} WHERE candidate_run_id = :run_id "
            "AND watermark_version = :expected_version",
            {"run_id": str(run_id), "expected_version": int(expected_version)},
        )
        if len(rows) != 1:
            raise RuntimeError("Watermark candidate compare-and-swap failed.")
        self.update_run_phase(run_id, "TARGET_WRITTEN", watermark_after=str(candidate_value), watermark_commit_status="CANDIDATE_STAGED")

    def commit_watermark_candidate(self, *, run_id: str, expected_version: int) -> None:
        self.execute(
            f"UPDATE {self.table('ctl_watermark')} SET committed_watermark_value = candidate_watermark_value, "
            "last_watermark_value = candidate_watermark_value, last_successful_run_id = :run_id, "
            "watermark_version = watermark_version + 1, commit_status = :committed, committed_at = CURRENT_TIMESTAMP(), "
            "candidate_watermark_value = NULL, candidate_run_id = NULL, candidate_created_at = NULL, updated_at = CURRENT_TIMESTAMP() "
            "WHERE candidate_run_id = :run_id AND watermark_version = :expected_version AND commit_status = :staged",
            {"run_id": str(run_id), "expected_version": int(expected_version), "committed": "COMMITTED", "staged": "CANDIDATE_STAGED"},
        )
        rows = self.query(
            f"SELECT watermark_version, last_successful_run_id FROM {self.table('ctl_watermark')} "
            "WHERE last_successful_run_id = :run_id AND watermark_version = :new_version",
            {"run_id": str(run_id), "new_version": int(expected_version) + 1},
        )
        if len(rows) != 1:
            self.update_run_phase(run_id, "TARGET_WRITTEN", watermark_commit_status="FAILED")
            raise RuntimeError("Watermark commit compare-and-swap failed.")
        self.update_run_phase(run_id, "WATERMARK_COMMITTED", watermark_commit_status="COMMITTED")

    def finalize_successful_run(self, *, run_id: str, queue_id: int, worker_id: str) -> None:
        runs = self.query(f"SELECT * FROM {self.table('ctl_run')} WHERE run_id = :run_id", {"run_id": str(run_id)})
        if len(runs) != 1:
            raise RuntimeError("Runtime finalization could not reload the run attempt.")
        run = runs[0]
        if str(run.get("target_commit_status") or "") != "COMMITTED":
            raise RuntimeError("A run cannot succeed before the target commit is verified.")
        if str(run.get("validation_status") or "") not in {"PASSED", "WARNING"}:
            raise RuntimeError("A run cannot succeed before blocking validation passes.")
        if str(run.get("watermark_commit_status") or "") not in {"COMMITTED", "SKIPPED"}:
            raise RuntimeError("A stateful run cannot succeed before its watermark commits.")
        ownership = (
            f"EXISTS (SELECT 1 FROM {self.table('ctl_ingestion_queue')} AS queue_item "
            "WHERE queue_item.queue_id = :queue_id AND queue_item.run_id = :run_id "
            "AND queue_item.claimed_by_worker_id = :worker_id "
            "AND queue_item.queue_status IN (:running, :finalizing) "
            "AND queue_item.lease_expires_at > CURRENT_TIMESTAMP())"
        )
        self.execute_batch([
            (
                f"UPDATE {self.table('ctl_run')} SET status = :success, current_phase = :phase, "
                "end_time = CURRENT_TIMESTAMP(), execution_time_seconds = TIMESTAMPDIFF(SECOND, start_time, CURRENT_TIMESTAMP()) "
                f"WHERE run_id = :run_id AND status = :running AND {ownership}",
                {
                    "success": "SUCCESS",
                    "phase": "FINALIZED",
                    "run_id": str(run_id),
                    "queue_id": int(queue_id),
                    "worker_id": str(worker_id),
                    "running": "RUNNING",
                    "finalizing": "FINALIZING",
                },
            ),
            (
                f"UPDATE {self.table('ctl_ingestion_queue')} SET queue_status = :success, completed_at = CURRENT_TIMESTAMP(), "
                "lease_expires_at = NULL, claimed_by_worker_id = NULL, message = :message "
                "WHERE queue_id = :queue_id AND run_id = :run_id "
                "AND queue_status IN (:running, :finalizing) AND claimed_by_worker_id = :worker_id "
                "AND lease_expires_at > CURRENT_TIMESTAMP()",
                {
                    "success": "SUCCESS",
                    "message": "Completed",
                    "queue_id": int(queue_id),
                    "run_id": str(run_id),
                    "running": "RUNNING",
                    "finalizing": "FINALIZING",
                    "worker_id": str(worker_id),
                },
            ),
        ])
        final_run = self.query(
            f"SELECT status FROM {self.table('ctl_run')} WHERE run_id = :run_id",
            {"run_id": str(run_id)},
        )
        final_queue = self.query(
            f"SELECT queue_status FROM {self.table('ctl_ingestion_queue')} WHERE queue_id = :queue_id AND run_id = :run_id",
            {"queue_id": int(queue_id), "run_id": str(run_id)},
        )
        if (
            len(final_run) != 1
            or str(final_run[0].get("status") or "") != "SUCCESS"
            or len(final_queue) != 1
            or str(final_queue[0].get("queue_status") or "") != "SUCCESS"
        ):
            raise RuntimeError("Run/queue success finalization postcondition failed.")

    def finalize_successful_runs(
        self, *, attempts: Iterable[Mapping[str, Any]], worker_id: str
    ) -> None:
        requested = [
            {"run_id": str(item["run_id"]), "queue_id": int(item["queue_id"])}
            for item in attempts
        ]
        if not requested:
            return
        if len({item["run_id"] for item in requested}) != len(requested):
            raise ValueError("Bulk success finalization requires unique run attempts.")
        where, parameters = self._where_pairs(requested, prefix="final")
        where = where.replace("run_id =", "runtime_run.run_id =").replace(
            "queue_id =", "runtime_run.queue_id ="
        )
        rows = self.query(
            f"SELECT runtime_run.run_id, runtime_run.target_commit_status, runtime_run.validation_status, "
            f"runtime_run.watermark_commit_status FROM {self.table('ctl_run')} AS runtime_run "
            f"JOIN {self.table('ctl_ingestion_queue')} AS queue_item ON queue_item.queue_id = runtime_run.queue_id "
            f"WHERE ({where}) AND runtime_run.status = :running "
            "AND queue_item.run_id = runtime_run.run_id AND queue_item.claimed_by_worker_id = :worker_id "
            "AND queue_item.queue_status IN (:running, :finalizing) "
            "AND queue_item.lease_expires_at > CURRENT_TIMESTAMP()",
            {
                **parameters, "running": "RUNNING", "finalizing": "FINALIZING",
                "worker_id": str(worker_id),
            },
        )
        if len(rows) != len(requested) or any(
            str(row.get("target_commit_status") or "") != "COMMITTED"
            or str(row.get("validation_status") or "") not in {"PASSED", "WARNING"}
            or str(row.get("watermark_commit_status") or "") not in {"COMMITTED", "SKIPPED"}
            for row in rows
        ):
            raise RuntimeError("A run cannot succeed before commit, validation, and checkpoint postconditions pass.")
        _, source, source_parameters = self._source_rows(requested, prefix="success")
        owned_source = (
            f"SELECT source.* FROM ({source}) AS source JOIN {self.table('ctl_ingestion_queue')} AS queue_item "
            "ON queue_item.queue_id = source.queue_id AND queue_item.run_id = source.run_id "
            "WHERE queue_item.claimed_by_worker_id = :worker_id "
            "AND queue_item.queue_status IN (:running, :finalizing) "
            "AND queue_item.lease_expires_at > CURRENT_TIMESTAMP()"
        )
        self.execute(
            f"MERGE INTO {self.table('ctl_run')} AS target USING ({owned_source}) AS source "
            "ON target.run_id = source.run_id AND target.status = :running "
            "WHEN MATCHED THEN UPDATE SET status = :success, current_phase = :phase, "
            "end_time = CURRENT_TIMESTAMP(), execution_time_seconds = TIMESTAMPDIFF(SECOND, target.start_time, CURRENT_TIMESTAMP())",
            {
                **source_parameters, "worker_id": str(worker_id), "running": "RUNNING",
                "finalizing": "FINALIZING", "success": "SUCCESS", "phase": "FINALIZED",
            },
        )
        self.execute(
            f"MERGE INTO {self.table('ctl_ingestion_queue')} AS target USING ({source}) AS source "
            "ON target.queue_id = source.queue_id AND target.run_id = source.run_id "
            "AND target.queue_status IN (:running, :finalizing) "
            "AND target.claimed_by_worker_id = :worker_id AND target.lease_expires_at > CURRENT_TIMESTAMP() "
            "WHEN MATCHED THEN UPDATE SET queue_status = :success, completed_at = CURRENT_TIMESTAMP(), "
            "lease_expires_at = NULL, claimed_by_worker_id = NULL, message = :message",
            {
                **source_parameters, "worker_id": str(worker_id), "running": "RUNNING",
                "finalizing": "FINALIZING", "success": "SUCCESS", "message": "Completed",
            },
        )
        verified = self.query(
            f"SELECT runtime_run.run_id FROM {self.table('ctl_run')} AS runtime_run "
            f"JOIN {self.table('ctl_ingestion_queue')} AS queue_item ON queue_item.queue_id = runtime_run.queue_id "
            f"WHERE ({where}) AND runtime_run.status = :success AND queue_item.queue_status = :success "
            "AND queue_item.run_id = runtime_run.run_id",
            {**parameters, "success": "SUCCESS"},
        )
        if {str(row["run_id"]) for row in verified} != {item["run_id"] for item in requested}:
            raise RuntimeError("Bulk run/queue success finalization postcondition failed.")


class DatabricksMetadataRepository(MetadataRepository):
    def __init__(self, context: TargetMetadataContext, *, warehouse_id: Optional[str] = None) -> None:
        super().__init__(context)
        self.warehouse_id = str(warehouse_id or os.getenv("DATABRICKS_SQL_WAREHOUSE_ID") or "").strip()
        if not self.warehouse_id:
            raise RuntimeError("DATABRICKS_SQL_WAREHOUSE_ID is required for metadata operations.")

    @staticmethod
    def _parameters(parameters: Optional[Mapping[str, Any]]) -> List[Dict[str, str]]:
        result = []
        for name, value in (parameters or {}).items():
            item: Dict[str, str] = {"name": name}
            if value is not None:
                if isinstance(value, bool):
                    item.update({"value": "true" if value else "false", "type": "BOOLEAN"})
                elif isinstance(value, int):
                    item.update({"value": str(value), "type": "BIGINT"})
                elif isinstance(value, float):
                    item.update({"value": str(value), "type": "DOUBLE"})
                else:
                    item.update({"value": str(value), "type": "STRING"})
            result.append(item)
        return result

    @staticmethod
    def _decode(value: Any, type_name: str) -> Any:
        if value is None:
            return None
        kind = str(type_name or "").upper()
        if kind == "BOOLEAN":
            return str(value).strip().lower() == "true"
        if kind in {"BYTE", "SHORT", "INT", "INTEGER", "LONG", "BIGINT"}:
            return int(value)
        if kind in {"FLOAT", "DOUBLE"}:
            return float(value)
        return value

    def _statement(self, sql: str, parameters: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
        from services.databricks_runtime import _request_json

        response = _request_json(
            "POST",
            "/api/2.0/sql/statements",
            {
                "warehouse_id": self.warehouse_id,
                "statement": sql.strip(),
                "catalog": self.context.namespace,
                "format": "JSON_ARRAY",
                "disposition": "INLINE",
                "wait_timeout": "10s",
                "on_wait_timeout": "CONTINUE",
                "parameters": self._parameters(parameters),
            },
        )
        statement_id = str(response.get("statement_id") or "")
        deadline = time.monotonic() + 120
        while str((response.get("status") or {}).get("state") or "").upper() in {"PENDING", "RUNNING"}:
            if not statement_id or time.monotonic() >= deadline:
                raise TimeoutError("Databricks metadata statement timed out.")
            time.sleep(1)
            response = _request_json("GET", f"/api/2.0/sql/statements/{statement_id}")
        status = response.get("status") or {}
        state = str(status.get("state") or "").upper()
        if state != "SUCCEEDED":
            error = status.get("error") or {}
            code = str(error.get("error_code") or state or "FAILED")
            raise RuntimeError(f"Databricks metadata statement failed: {code}")
        return response

    def execute(self, sql: str, parameters: Optional[Mapping[str, Any]] = None) -> None:
        self._statement(sql, parameters)

    def query(self, sql: str, parameters: Optional[Mapping[str, Any]] = None) -> List[Dict[str, Any]]:
        response = self._statement(sql, parameters)
        manifest_columns = (((response.get("manifest") or {}).get("schema") or {}).get("columns") or [])
        columns = [str(column.get("name") or "").lower() for column in manifest_columns]
        types = [str(column.get("type_name") or "") for column in manifest_columns]
        rows = ((response.get("result") or {}).get("data_array") or [])
        return [
            dict(zip(columns, (self._decode(value, kind) for value, kind in zip(row, types))))
            for row in rows
        ]


class SnowflakeMetadataRepository(MetadataRepository):
    def __init__(self, context: TargetMetadataContext) -> None:
        super().__init__(context)
        self._transaction = threading.local()

    def _transaction_connection(self) -> Any:
        return getattr(self._transaction, "connection", None)

    @contextmanager
    def unit_of_work(self):
        existing = self._transaction_connection()
        if existing is not None:
            yield self
            return

        from services.snowflake_bronze_runtime import _snowflake_connect

        connection = _snowflake_connect(autocommit=False)
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

    def _within_unit_of_work(self, operation: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        with self.unit_of_work():
            return operation(*args, **kwargs)

    def upsert_database_ingestion_object_drafts(
        self, *args: Any, **kwargs: Any
    ) -> List[Dict[str, Any]]:
        return self._within_unit_of_work(
            super().upsert_database_ingestion_object_drafts, *args, **kwargs
        )

    def upsert_source_to_bronze_mapping_draft(
        self, *args: Any, **kwargs: Any
    ) -> Dict[str, Any]:
        return self._within_unit_of_work(
            super().upsert_source_to_bronze_mapping_draft, *args, **kwargs
        )

    def upsert_bronze_to_silver_draft(
        self, *args: Any, **kwargs: Any
    ) -> Dict[str, Any]:
        return self._within_unit_of_work(
            super().upsert_bronze_to_silver_draft, *args, **kwargs
        )

    def upsert_silver_to_gold_draft(
        self, *args: Any, **kwargs: Any
    ) -> Dict[str, Any]:
        return self._within_unit_of_work(
            super().upsert_silver_to_gold_draft, *args, **kwargs
        )

    def register_and_activate_artifacts(
        self, *args: Any, **kwargs: Any
    ) -> List[Dict[str, Any]]:
        return self._within_unit_of_work(
            super().register_and_activate_artifacts, *args, **kwargs
        )

    def _adapt(self, sql: str, parameters: Optional[Mapping[str, Any]]) -> tuple[str, List[Any]]:
        values = parameters or {}
        ordered: List[Any] = []

        def replace(match: re.Match[str]) -> str:
            name = match.group(1)
            if name not in values:
                raise ValueError(f"Missing SQL parameter: {name}")
            ordered.append(values[name])
            return "%s"

        return _PARAMETER.sub(replace, sql), ordered

    def execute(self, sql: str, parameters: Optional[Mapping[str, Any]] = None) -> None:
        from services.snowflake_bronze_runtime import _snowflake_connect

        adapted, ordered = self._adapt(sql, parameters)
        connection = self._transaction_connection() or _snowflake_connect(autocommit=False)
        owns_connection = self._transaction_connection() is None
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
            close_cursor = getattr(cursor, "close", None)
            if callable(close_cursor):
                close_cursor()
            if owns_connection:
                connection.close()

    def execute_batch(self, statements: Iterable[tuple[str, Mapping[str, Any]]]) -> None:
        from services.snowflake_bronze_runtime import _snowflake_connect

        connection = self._transaction_connection() or _snowflake_connect(autocommit=False)
        owns_connection = self._transaction_connection() is None
        cursor = None
        try:
            cursor = connection.cursor()
            for sql, parameters in statements:
                adapted, ordered = self._adapt(sql, parameters)
                cursor.execute(adapted, tuple(ordered))
            if owns_connection:
                connection.commit()
        except Exception:
            if owns_connection:
                connection.rollback()
            raise
        finally:
            close_cursor = getattr(cursor, "close", None)
            if callable(close_cursor):
                close_cursor()
            if owns_connection:
                connection.close()

    def query(self, sql: str, parameters: Optional[Mapping[str, Any]] = None) -> List[Dict[str, Any]]:
        from services.snowflake_bronze_runtime import _snowflake_connect

        adapted, ordered = self._adapt(sql, parameters)
        connection = self._transaction_connection() or _snowflake_connect(autocommit=False)
        owns_connection = self._transaction_connection() is None
        cursor = None
        try:
            cursor = connection.cursor()
            cursor.execute(adapted, tuple(ordered))
            columns = [str(column[0]).lower() for column in (cursor.description or [])]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            close_cursor = getattr(cursor, "close", None)
            if callable(close_cursor):
                close_cursor()
            if owns_connection:
                connection.rollback()
                connection.close()

    def enqueue_work(self, **kwargs: Any) -> Dict[str, Any]:
        from services.snowflake_bronze_runtime import _snowflake_connect

        if self._transaction_connection() is not None:
            return super().enqueue_work(**kwargs)
        connection = _snowflake_connect(autocommit=False)
        self._transaction.connection = connection
        try:
            cursor = connection.cursor()
            cursor.execute(
                f"UPDATE {self.table('cfg_ingestion_object')} SET updated_at = updated_at "
                "WHERE ingestion_object_id = %s AND active_flag = TRUE AND is_current = TRUE",
                (int(kwargs.get("ingestion_object_id") or 0),),
            )
            result = super().enqueue_work(**kwargs)
            connection.commit()
            return result
        except Exception:
            connection.rollback()
            raise
        finally:
            del self._transaction.connection
            connection.close()

    def enqueue_work_batch(self, requests: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
        materialized = [dict(item) for item in requests]
        if self._transaction_connection() is not None:
            return super().enqueue_work_batch(materialized)
        from services.snowflake_bronze_runtime import _snowflake_connect

        connection = _snowflake_connect(autocommit=False)
        self._transaction.connection = connection
        try:
            cursor = connection.cursor()
            for object_id in sorted({int(item.get("ingestion_object_id") or 0) for item in materialized}):
                cursor.execute(
                    f"UPDATE {self.table('cfg_ingestion_object')} SET updated_at = updated_at "
                    "WHERE ingestion_object_id = %s AND active_flag = TRUE AND is_current = TRUE",
                    (object_id,),
                )
            result = super().enqueue_work_batch(materialized)
            connection.commit()
            return result
        except Exception:
            connection.rollback()
            raise
        finally:
            del self._transaction.connection
            connection.close()


def metadata_repository(
    *,
    platform: str,
    environment: str,
    namespace: str,
    schema: str = "metadata",
) -> MetadataRepository:
    configured_environment = str(os.getenv("ATHENA_TARGET_ENVIRONMENT") or "").strip()
    if not configured_environment:
        raise RuntimeError("ATHENA_TARGET_ENVIRONMENT is required for target metadata operations.")
    if configured_environment.casefold() != str(environment or "").strip().casefold():
        raise ValueError("The selected target environment is not served by this deployment.")
    context = TargetMetadataContext(
        platform=platform,
        environment=environment,
        namespace=namespace,
        schema=schema,
    )
    if context.platform == "databricks":
        return DatabricksMetadataRepository(context)
    return SnowflakeMetadataRepository(context)


def metadata_repository_for_target(*, platform: str, environment: str) -> MetadataRepository:
    normalized = str(platform or "").strip().lower()
    variable = {
        "databricks": "ATHENA_DATABRICKS_METADATA_CATALOG",
        "snowflake": "ATHENA_SNOWFLAKE_METADATA_DATABASE",
    }.get(normalized)
    if not variable:
        raise ValueError(f"Unsupported metadata target: {platform!r}")
    namespace = str(os.getenv(variable) or "").strip()
    if not namespace:
        raise RuntimeError(f"{variable} is required for target metadata operations.")
    return metadata_repository(
        platform=normalized,
        environment=environment,
        namespace=namespace,
        schema="metadata",
    )
