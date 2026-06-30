"""
Deterministic metadata discovery node for LangGraph.

This node deep-crawls Azure SQL metadata for nominated tables and produces a
column-level JSON artifact that can be used later for SQL generation.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from typing import Any, Dict, List, Literal, Optional, TypedDict
import os

import pyodbc
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import StateGraph

from state import Stage01State
from utilis.db import ai_store_db_writer, get_client_connection
from utilis.logger import logger


class NominatedTable(TypedDict):
    database_name: str
    schema_name: str
    table_name: str


class ColumnMetadata(TypedDict):
    column_name: str
    data_type: str
    data_type_full: str
    is_nullable: bool
    ordinal_position: int
    character_maximum_length: Optional[int]
    numeric_precision: Optional[int]
    numeric_scale: Optional[int]
    datetime_precision: Optional[int]
    collation_name: Optional[str]
    column_default: Optional[str]


class TableMetadata(TypedDict, total=False):
    database_name: str
    schema_name: str
    table_name: str
    table_status: Literal["COMPLETED", "FAILED"]
    column_count: int
    columns: List[ColumnMetadata]
    primary_keys: List[Dict[str, Any]]
    unique_keys: List[Dict[str, Any]]
    foreign_keys: List[Dict[str, Any]]
    error: str


class DiscoveredMetadataPayload(TypedDict):
    fingerprint: str
    storage_fingerprint: str
    run_id: str
    certified_kpis: List[Any]
    table_count: int
    successful_table_count: int
    failed_table_count: int
    primary_keys: List[Dict[str, Any]]
    foreign_keys: List[Dict[str, Any]]
    table_relationships: List[Dict[str, Any]]
    tables: List[TableMetadata]


def _copy_state(state: Stage01State) -> Stage01State:
    return state.copy()


def _resolve_tables_for_discovery(state: Stage01State) -> List[NominatedTable]:
    raw_tables = state.get("certified_tables") or state.get("nominated_tables") or []
    resolved: List[NominatedTable] = []

    for item in raw_tables:
        if not isinstance(item, dict):
            continue

        database_name = str(item.get("database_name") or "").strip()
        schema_name = str(item.get("schema_name") or "dbo").strip()
        table_name = str(item.get("table_name") or "").strip()

        if not database_name or not table_name:
            continue

        resolved.append(
            {
                "database_name": database_name,
                "schema_name": schema_name,
                "table_name": table_name,
            }
        )

    return resolved


def get_azure_sql_connection(database_name: str) -> pyodbc.Connection:
    return get_client_connection(database_name)

#normalize raw sql data types to more user-friendly formats, e.g. varchar(255) instead of just varchar
def _format_data_type(
    data_type: str,
    character_maximum_length: Optional[int],
    numeric_precision: Optional[int],
    numeric_scale: Optional[int],
    datetime_precision: Optional[int],
) -> str:
    normalized = data_type.lower()

    if normalized in {"char", "varchar", "nchar", "nvarchar", "binary", "varbinary"}:
        if character_maximum_length is None:
            return data_type
        if character_maximum_length == -1:
            return f"{data_type}(MAX)"
        return f"{data_type}({character_maximum_length})"

    if normalized in {"decimal", "numeric"}:
        if numeric_precision is None:
            return data_type
        if numeric_scale is None:
            return f"{data_type}({numeric_precision})"
        return f"{data_type}({numeric_precision},{numeric_scale})"

    if normalized in {"datetime2", "datetimeoffset", "time"} and datetime_precision is not None:
        return f"{data_type}({datetime_precision})"

    return data_type


def _fetch_table_columns(cursor: pyodbc.Cursor, schema_name: str, table_name: str) -> List[ColumnMetadata]:
    query = """
        SELECT
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            IS_NULLABLE,
            ORDINAL_POSITION,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            DATETIME_PRECISION,
            COLLATION_NAME,
            COLUMN_DEFAULT
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ?
          AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
    """

    cursor.execute(query, (schema_name, table_name))
    rows = cursor.fetchall()

    columns: List[ColumnMetadata] = []
    for row in rows:
        data_type = str(row.DATA_TYPE)
        char_len = int(row.CHARACTER_MAXIMUM_LENGTH) if row.CHARACTER_MAXIMUM_LENGTH is not None else None
        num_precision = int(row.NUMERIC_PRECISION) if row.NUMERIC_PRECISION is not None else None
        num_scale = int(row.NUMERIC_SCALE) if row.NUMERIC_SCALE is not None else None
        dt_precision = int(row.DATETIME_PRECISION) if row.DATETIME_PRECISION is not None else None

        columns.append(
            {
                "column_name": str(row.COLUMN_NAME),
                "data_type": data_type,
                "data_type_full": _format_data_type(
                    data_type=data_type,
                    character_maximum_length=char_len,
                    numeric_precision=num_precision,
                    numeric_scale=num_scale,
                    datetime_precision=dt_precision,
                ),
                "is_nullable": str(row.IS_NULLABLE).upper() == "YES",
                "ordinal_position": int(row.ORDINAL_POSITION),
                "character_maximum_length": char_len,
                "numeric_precision": num_precision,
                "numeric_scale": num_scale,
                "datetime_precision": dt_precision,
                "collation_name": str(row.COLLATION_NAME) if row.COLLATION_NAME is not None else None,
                "column_default": str(row.COLUMN_DEFAULT) if row.COLUMN_DEFAULT is not None else None,
            }
        )

    return columns


def _fetch_key_constraints(
    cursor: pyodbc.Cursor,
    schema_name: str,
    table_name: str,
    constraint_type: str,
) -> List[Dict[str, Any]]:
    query = """
        SELECT
            tc.CONSTRAINT_NAME,
            kcu.COLUMN_NAME,
            kcu.ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
           AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
           AND tc.TABLE_NAME = kcu.TABLE_NAME
        WHERE tc.TABLE_SCHEMA = ?
          AND tc.TABLE_NAME = ?
          AND tc.CONSTRAINT_TYPE = ?
        ORDER BY tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
    """
    cursor.execute(query, (schema_name, table_name, constraint_type))
    return [
        {
            "constraint_name": str(row.CONSTRAINT_NAME),
            "column_name": str(row.COLUMN_NAME),
            "ordinal_position": int(row.ORDINAL_POSITION),
        }
        for row in cursor.fetchall()
    ]


def _fetch_foreign_keys(cursor: pyodbc.Cursor, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
    query = """
        SELECT
            fk.name AS constraint_name,
            src_schema.name AS source_schema_name,
            src_table.name AS source_table_name,
            src_col.name AS source_column_name,
            ref_schema.name AS referenced_schema_name,
            ref_table.name AS referenced_table_name,
            ref_col.name AS referenced_column_name,
            fkc.constraint_column_id AS ordinal_position,
            fk.delete_referential_action_desc AS delete_rule,
            fk.update_referential_action_desc AS update_rule
        FROM sys.foreign_keys fk
        JOIN sys.foreign_key_columns fkc
            ON fk.object_id = fkc.constraint_object_id
        JOIN sys.tables src_table
            ON fk.parent_object_id = src_table.object_id
        JOIN sys.schemas src_schema
            ON src_table.schema_id = src_schema.schema_id
        JOIN sys.columns src_col
            ON src_col.object_id = src_table.object_id
           AND src_col.column_id = fkc.parent_column_id
        JOIN sys.tables ref_table
            ON fk.referenced_object_id = ref_table.object_id
        JOIN sys.schemas ref_schema
            ON ref_table.schema_id = ref_schema.schema_id
        JOIN sys.columns ref_col
            ON ref_col.object_id = ref_table.object_id
           AND ref_col.column_id = fkc.referenced_column_id
        WHERE src_schema.name = ?
          AND src_table.name = ?
        ORDER BY fk.name, fkc.constraint_column_id
    """
    cursor.execute(query, (schema_name, table_name))
    return [
        {
            "constraint_name": str(row.constraint_name),
            "source_schema_name": str(row.source_schema_name),
            "source_table_name": str(row.source_table_name),
            "source_column_name": str(row.source_column_name),
            "referenced_schema_name": str(row.referenced_schema_name),
            "referenced_table_name": str(row.referenced_table_name),
            "referenced_column_name": str(row.referenced_column_name),
            "ordinal_position": int(row.ordinal_position),
            "delete_rule": str(row.delete_rule),
            "update_rule": str(row.update_rule),
        }
        for row in cursor.fetchall()
    ]


def _build_table_relationships(
    *,
    database_name: str,
    foreign_keys: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in foreign_keys:
        grouped[str(row["constraint_name"])].append(row)

    relationships: List[Dict[str, Any]] = []
    for constraint_name, rows in grouped.items():
        ordered = sorted(rows, key=lambda item: int(item.get("ordinal_position") or 0))
        first = ordered[0]
        relationships.append(
            {
                "relationship_id": f"{database_name}.{first['source_schema_name']}.{first['source_table_name']}:{constraint_name}",
                "constraint_name": constraint_name,
                "relationship_type": "FOREIGN_KEY",
                "cardinality": "MANY_TO_ONE",
                "source": "METADATA_DISCOVERY",
                "confidence": 1.0,
                "certified": True,
                "database_name": database_name,
                "source_schema_name": first["source_schema_name"],
                "source_table_name": first["source_table_name"],
                "referenced_schema_name": first["referenced_schema_name"],
                "referenced_table_name": first["referenced_table_name"],
                "column_mapping": [
                    {
                        "source_column_name": row["source_column_name"],
                        "referenced_column_name": row["referenced_column_name"],
                        "ordinal_position": row["ordinal_position"],
                    }
                    for row in ordered
                ],
                "update_rule": first["update_rule"],
                "delete_rule": first["delete_rule"],
            }
        )
    return relationships


def _close_connections(connections: Iterable[pyodbc.Connection]) -> None:
    for connection in connections:
        try:
            connection.close()
        except Exception:
            logger.warning("Azure SQL connection close failed", extra={"node": "metadata_discovery"})

#payload generation for metadata discovery and hitl certification nodes
def _persist_discovered_metadata(
    *,
    run_id: str,
    fingerprint: str,
    certified_kpis: List[Any],
    primary_keys: List[Dict[str, Any]],
    foreign_keys: List[Dict[str, Any]],
    table_relationships: List[Dict[str, Any]],
    tables: List[TableMetadata],
) -> None:
    payload: DiscoveredMetadataPayload = {
        "fingerprint": fingerprint,
        "storage_fingerprint": f"{fingerprint}:DISCOVERED_METADATA",
        "run_id": run_id,
        "certified_kpis": certified_kpis,
        "table_count": len(tables),
        "successful_table_count": sum(1 for table in tables if table["table_status"] == "COMPLETED"),
        "failed_table_count": sum(1 for table in tables if table["table_status"] == "FAILED"),
        "primary_keys": primary_keys,
        "foreign_keys": foreign_keys,
        "table_relationships": table_relationships,
        "tables": tables,
    }

    ai_store_db_writer(
        run_id=run_id,
        stage="Metadata Discovery",
        artifact_type="DISCOVERED_METADATA",
        payload=payload,
        schema_version="MetadataDiscovery_v1",
        prompt_version="DETERMINISTIC_SQL_METADATA_v1",
        faithfulness_status="PASSED",
        token_count=0,
        input_tokens=0,
        output_tokens=0,
        fingerprint=fingerprint,
    )


def metadata_discovery_node(state: Stage01State) -> Stage01State:
    new_state = _copy_state(state)
    log_context = {
        "run_id": new_state.get("run_id", "unknown"),
        "node": "metadata_discovery",
    }

    logger.info("START metadata_discovery_node", extra=log_context)

    if new_state.get("status") == "FAILED":
        logger.warning("Skipping metadata discovery because pipeline status is FAILED", extra=log_context)
        return new_state

    nominated_tables = _resolve_tables_for_discovery(new_state)
    if not nominated_tables:
        logger.info("Skipping metadata discovery because no nominated tables are available", extra=log_context)
        return new_state

    run_id = str(new_state.get("run_id") or "unknown")
    fingerprint = str(new_state.get("fingerprint") or run_id)
    certified_kpis = list(new_state.get("certified_kpis") or [])

    tables_metadata: List[TableMetadata] = []
    discovered_primary_keys: List[Dict[str, Any]] = []
    discovered_foreign_keys: List[Dict[str, Any]] = []
    discovered_relationships: List[Dict[str, Any]] = []
    connections: Dict[str, pyodbc.Connection] = {}

    try:
        for table_ref in nominated_tables:
            database_name = table_ref["database_name"]
            schema_name = table_ref["schema_name"]
            table_name = table_ref["table_name"]

            try:
                connection = connections.get(database_name)
                if connection is None:
                    connection = get_azure_sql_connection(database_name)
                    connections[database_name] = connection

                cursor = connection.cursor()
                columns = _fetch_table_columns(cursor, schema_name, table_name)
                if not columns:
                    raise ValueError(
                        f"No column metadata found for {database_name}.{schema_name}.{table_name}. "
                        "Table may not exist, schema may be wrong, or access may be blocked."
                    )

                primary_keys = _fetch_key_constraints(cursor, schema_name, table_name, "PRIMARY KEY")
                unique_keys = _fetch_key_constraints(cursor, schema_name, table_name, "UNIQUE")
                foreign_keys = _fetch_foreign_keys(cursor, schema_name, table_name)
                table_relationships = _build_table_relationships(
                    database_name=database_name,
                    foreign_keys=foreign_keys,
                )

                discovered_primary_keys.extend(
                    {
                        "database_name": database_name,
                        "schema_name": schema_name,
                        "table_name": table_name,
                        **item,
                    }
                    for item in primary_keys
                )
                discovered_foreign_keys.extend(
                    {
                        "database_name": database_name,
                        **item,
                    }
                    for item in foreign_keys
                )
                discovered_relationships.extend(table_relationships)

                tables_metadata.append(
                    {
                        "database_name": database_name,
                        "schema_name": schema_name,
                        "table_name": table_name,
                        "table_status": "COMPLETED",
                        "column_count": len(columns),
                        "columns": columns,
                        "primary_keys": primary_keys,
                        "unique_keys": unique_keys,
                        "foreign_keys": foreign_keys,
                    }
                )

                logger.info(
                    "Metadata discovered for %s.%s.%s (%d columns)",
                    database_name,
                    schema_name,
                    table_name,
                    len(columns),
                    extra=log_context,
                )
            except Exception as exc:
                logger.warning(
                    "Metadata discovery failed for %s.%s.%s: %s",
                    database_name,
                    schema_name,
                    table_name,
                    exc,
                    extra=log_context,
                )
                tables_metadata.append(
                    {
                        "database_name": database_name,
                        "schema_name": schema_name,
                        "table_name": table_name,
                        "table_status": "FAILED",
                        "column_count": 0,
                        "columns": [],
                        "error": str(exc),
                    }
                )
    except Exception as exc:
        logger.error("Metadata discovery failed: %s", exc, extra=log_context)
        new_state.update(
            {
                "metadata_status": "FAILED",
                "metadata_error": str(exc),
            }
        )
        return new_state
    finally:
        _close_connections(connections.values())

    try:
        _persist_discovered_metadata(
            run_id=run_id,
            fingerprint=fingerprint,
            certified_kpis=certified_kpis,
            primary_keys=discovered_primary_keys,
            foreign_keys=discovered_foreign_keys,
            table_relationships=discovered_relationships,
            tables=tables_metadata,
        )
    except Exception as exc:
        logger.warning("Metadata artifact persistence failed: %s", exc, extra=log_context)
        new_state.update(
            {
                "metadata_status": "FAILED",
                "metadata_error": f"Metadata extracted but persistence failed: {exc}",
                "discovered_metadata": {
                    "certified_kpis": certified_kpis,
                    "primary_keys": discovered_primary_keys,
                    "foreign_keys": discovered_foreign_keys,
                    "table_relationships": discovered_relationships,
                    "tables": tables_metadata,
                },
            }
        )
        return new_state

    success_count = sum(1 for table in tables_metadata if table["table_status"] == "COMPLETED")
    failed_count = sum(1 for table in tables_metadata if table["table_status"] == "FAILED")

    if success_count == 0 and failed_count > 0:
        metadata_status = "FAILED"
        metadata_error = "Metadata discovery failed for all selected tables."
    elif failed_count > 0:
        metadata_status = "COMPLETED_WITH_WARNINGS"
        metadata_error = f"Metadata discovery failed for {failed_count} table(s)."
    else:
        metadata_status = "COMPLETED"
        metadata_error = None

    new_state.update(
        {
            "discovered_metadata": {
                "certified_kpis": certified_kpis,
                "primary_keys": discovered_primary_keys,
                "foreign_keys": discovered_foreign_keys,
                "table_relationships": discovered_relationships,
                "tables": tables_metadata,
            },
            "primary_keys": discovered_primary_keys,
            "foreign_keys": discovered_foreign_keys,
            "table_relationships": discovered_relationships,
            "metadata_status": metadata_status,
            "metadata_error": metadata_error,
        }
    )

    logger.info(
        "END metadata_discovery_node: tables=%d success=%d failed=%d",
        len(tables_metadata),
        sum(1 for table in tables_metadata if table["table_status"] == "COMPLETED"),
        sum(1 for table in tables_metadata if table["table_status"] == "FAILED"),
        extra=log_context,
    )
    return new_state


def build_metadata_discovery_graph() -> StateGraph:
    graph = StateGraph(Stage01State)
    graph.add_node("metadata_discovery", metadata_discovery_node)
    graph.set_entry_point("metadata_discovery")
    graph.set_finish_point("metadata_discovery")
    return graph


def compile_metadata_discovery_graph():
    return build_metadata_discovery_graph().compile(checkpointer=MemorySaver())
