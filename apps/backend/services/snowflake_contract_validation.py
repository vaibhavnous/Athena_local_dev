"""Catalog-aware validation for generated Snowflake Silver and Gold contracts."""

from __future__ import annotations

import re
from typing import Any, Iterable, Set


_SOURCE_REFERENCE_RE = re.compile(
    r"\bsrc\s*\.\s*(?:\"((?:\"\"|[^\"])*)\"|([A-Za-z_][A-Za-z0-9_$]*))",
    flags=re.IGNORECASE,
)
_GET_IGNORE_CASE_RE = re.compile(
    r"GET_IGNORE_CASE\s*\(.*?,\s*'((?:''|[^'])*)'",
    flags=re.IGNORECASE | re.DOTALL,
)


def quote_qualified_name(value: str) -> str:
    parts = [part.strip().strip('"') for part in str(value or '').split('.') if part.strip()]
    if len(parts) not in {2, 3}:
        raise ValueError(f"Snowflake table must be database.schema.table: {value}")
    return '.'.join('"' + part.replace('"', '""') + '"' for part in parts)


def extract_source_column_references(sql: str) -> Set[str]:
    references = {
        (quoted or bare or '').replace('""', '"').strip().casefold()
        for quoted, bare in _SOURCE_REFERENCE_RE.findall(str(sql or ''))
    }
    references.update(
        value.replace("''", "'").strip().casefold()
        for value in _GET_IGNORE_CASE_RE.findall(str(sql or ''))
    )
    return {value for value in references if value and value != '*'}


def extract_quoted_source_column_references(sql: str) -> Set[str]:
    return {
        (quoted or '').replace('""', '"').strip()
        for quoted, _ in _SOURCE_REFERENCE_RE.findall(str(sql or ''))
        if quoted and quoted.strip()
    }


def catalog_columns(connection: Any, table_ref: str) -> Set[str]:
    cursor_factory = getattr(connection, 'cursor', None)
    if not callable(cursor_factory):
        # Test doubles and dry-run callers may intentionally omit a catalog cursor.
        return set()
    cursor = cursor_factory()
    cursor.execute(f'DESC TABLE {quote_qualified_name(table_ref)}')
    rows = cursor.fetchall() or []
    return {str(row[0]).strip().strip('"') for row in rows if row and str(row[0]).strip()}


def validate_catalog_columns(
    connection: Any,
    *,
    table_ref: str,
    required_columns: Iterable[str],
    layer: str,
    context: str,
    exact_columns: Iterable[str] = (),
) -> None:
    required = {str(column).strip().strip('"').casefold() for column in required_columns if str(column).strip()}
    if not required:
        return
    try:
        available = catalog_columns(connection, table_ref)
    except Exception as exc:
        raise ValueError(f"{layer} preflight could not inspect source table {table_ref}: {exc}") from exc
    if not available:
        return
    available_folded = {column.casefold() for column in available}
    missing = sorted(required - available_folded)
    exact_missing = sorted({str(column).strip() for column in exact_columns if str(column).strip()} - available)
    missing = sorted(set(missing) | set(exact_missing))
    if missing:
        raise ValueError(
            f"{layer} preflight rejected {context}: source table {table_ref} is missing column(s): {', '.join(missing[:20])}"
        )
