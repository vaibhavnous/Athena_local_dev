from __future__ import annotations

import json
import os
import re
import xml.etree.ElementTree as ET
from itertools import combinations
from io import BytesIO
from pathlib import PurePosixPath
from typing import Any, Dict, Iterable, List, Mapping

import pandas as pd

from services.metadata_contracts import canonical_json_hash, stable_bigint


SUPPORTED_FILE_FORMATS = {"csv", "json", "xml"}


def _env(name: str, default: str = "") -> str:
    return str(os.getenv(name) or default).strip()


def account_url() -> str:
    return _env("ADLS_ACCOUNT_URL", "https://atheastorage.dfs.core.windows.net").rstrip("/")


def file_system() -> str:
    value = _env("ADLS_FILE_SYSTEM", "athena")
    if not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])?", value):
        raise ValueError("ADLS_FILE_SYSTEM is not a valid container name.")
    return value


def source_root() -> str:
    value = _env("ADLS_SOURCE_ROOT", "INSURANCE_SFTP/insurance").strip("/")
    parts = PurePosixPath(value).parts
    if not value or any(part in {"", ".", ".."} for part in parts):
        raise ValueError("ADLS_SOURCE_ROOT must be a non-empty canonical path.")
    return "/".join(parts)


def source_base_path() -> str:
    account = account_url().split("://", 1)[-1].split(".", 1)[0]
    return f"abfss://{file_system()}@{account}.dfs.core.windows.net/{source_root()}/"


def _credential():
    try:
        from azure.identity import ClientSecretCredential, DefaultAzureCredential
    except ImportError as exc:  # pragma: no cover - deployment prerequisite
        raise RuntimeError("azure-identity is required for ADLS discovery.") from exc

    tenant = _env("AZURE_TENANT_ID")
    client = _env("AZURE_CLIENT_ID")
    secret = _env("AZURE_CLIENT_SECRET")
    supplied = [bool(tenant), bool(client), bool(secret)]
    if any(supplied) and not all(supplied):
        raise RuntimeError(
            "AZURE_TENANT_ID, AZURE_CLIENT_ID, and AZURE_CLIENT_SECRET must be supplied together."
        )
    if all(supplied):
        return ClientSecretCredential(tenant_id=tenant, client_id=client, client_secret=secret)
    return DefaultAzureCredential(
        exclude_interactive_browser_credential=True,
        exclude_shared_token_cache_credential=True,
        exclude_visual_studio_code_credential=True,
    )


def file_system_client():
    try:
        from azure.storage.filedatalake import DataLakeServiceClient
    except ImportError as exc:  # pragma: no cover - deployment prerequisite
        raise RuntimeError("azure-storage-file-datalake is required for ADLS discovery.") from exc
    return DataLakeServiceClient(account_url=account_url(), credential=_credential()).get_file_system_client(
        file_system()
    )


def _format(path: str) -> str:
    return PurePosixPath(path).suffix.lower().lstrip(".")


def _canonical_path(path: str) -> str:
    normalized = str(PurePosixPath(str(path or "").lstrip("/")))
    root = source_root()
    if normalized != root and not normalized.startswith(root + "/"):
        raise ValueError(f"ADLS path is outside the configured source root: {path!r}")
    return normalized


def _abfss(path: str) -> str:
    return source_base_path().removesuffix(source_root() + "/") + _canonical_path(path)


def discover_files() -> List[Dict[str, Any]]:
    client = file_system_client()
    maximum = max(1, int(_env("ADLS_MAX_DISCOVERED_FILES", "10000")))
    discovered: List[Dict[str, Any]] = []
    for item in client.get_paths(path=source_root(), recursive=True):
        if getattr(item, "is_directory", False):
            continue
        path = _canonical_path(str(item.name))
        file_format = _format(path)
        if file_format not in SUPPORTED_FILE_FORMATS:
            continue
        parent = PurePosixPath(path).parent.name
        entity = re.sub(r"[^a-zA-Z0-9_]+", "_", parent).strip("_").lower()
        if not entity:
            raise ValueError(f"Cannot derive an entity name from ADLS path: {path}")
        discovered.append(
            {
                "feed_id": f"adls:{path.casefold()}",
                "source": "adls_gen2",
                "source_resource_type": "FILE",
                "entity": entity,
                "database_name": "insurance",
                "schema_name": "source",
                "table_name": entity,
                "file_name": PurePosixPath(path).name,
                "file_format": file_format,
                "format": file_format,
                "remote_path": "/" + path,
                "source_path": _abfss(path),
                "landing_path": _abfss(path),
                "databricks_source_path": _abfss(path),
                "cloud_path": _abfss(path),
                "file_size": int(getattr(item, "content_length", 0) or 0),
                "etag": str(getattr(item, "etag", "") or "").strip('"'),
                "last_modified": (
                    getattr(item, "last_modified", None).isoformat()
                    if getattr(item, "last_modified", None)
                    else None
                ),
                "status": "DISCOVERED",
            }
        )
        if len(discovered) > maximum:
            raise RuntimeError(f"ADLS discovery exceeded ADLS_MAX_DISCOVERED_FILES={maximum}.")
    discovered.sort(key=lambda item: item["source_path"].casefold())
    identities = [item["source_path"].casefold() for item in discovered]
    if len(identities) != len(set(identities)):
        raise RuntimeError("ADLS discovery produced duplicate canonical source paths.")
    if not discovered:
        raise FileNotFoundError(
            f"No CSV, JSON, or XML files were found under {source_base_path()}"
        )
    return discovered


def _download_sample(path: str) -> bytes:
    maximum = max(1024, int(_env("ADLS_SCHEMA_SAMPLE_BYTES", str(4 * 1024 * 1024))))
    downloader = file_system_client().get_file_client(_canonical_path(path)).download_file(
        offset=0,
        length=maximum,
    )
    return downloader.readall()


def _dataframe(content: bytes, file_format: str) -> tuple[pd.DataFrame, Dict[str, Any]]:
    maximum_rows = max(1, int(_env("ADLS_SCHEMA_SAMPLE_ROWS", "1000")))
    if file_format == "csv":
        frame = pd.read_csv(BytesIO(content), nrows=maximum_rows)
        return frame, {"header": True, "inferSchema": False}
    if file_format == "json":
        try:
            frame = pd.read_json(BytesIO(content), lines=True)
            mode = "json_lines"
        except ValueError:
            payload = json.loads(content.decode("utf-8-sig"))
            records = payload if isinstance(payload, list) else [payload]
            frame = pd.json_normalize(records)
            mode = "document"
        return frame.head(maximum_rows), {"mode": mode, "multiline": mode == "document"}
    if file_format == "xml":
        root = ET.fromstring(content)
        children = list(root)
        if not children:
            raise ValueError("XML source must contain at least one record element.")
        row_tag = children[0].tag.rsplit("}", 1)[-1]
        frame = pd.read_xml(BytesIO(content))
        return frame.head(maximum_rows), {"rowTag": row_tag}
    raise ValueError(f"Unsupported ADLS payload format: {file_format}")


def _data_type(series: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(series.dtype):
        return "boolean"
    if pd.api.types.is_integer_dtype(series.dtype):
        return "bigint"
    if pd.api.types.is_float_dtype(series.dtype):
        return "double"
    if pd.api.types.is_datetime64_any_dtype(series.dtype):
        return "timestamp"
    return "string"


def infer_schema(feed: Mapping[str, Any]) -> Dict[str, Any]:
    source_path = str(feed.get("remote_path") or feed.get("source_path") or "")
    file_format = str(feed.get("file_format") or feed.get("format") or "").lower()
    content = _download_sample(source_path)
    frame, parser_options = _dataframe(content, file_format)
    if frame.empty and not list(frame.columns):
        raise ValueError(f"Source file has no readable schema: {feed.get('source_path')}")
    columns = []
    for ordinal, name in enumerate(frame.columns, start=1):
        series = frame[name]
        non_null = series.dropna()
        columns.append(
            {
                "database_name": feed.get("database_name") or "insurance",
                "schema_name": feed.get("schema_name") or "source",
                "table_name": feed.get("table_name") or feed.get("entity"),
                "column_name": str(name),
                "data_type": _data_type(series),
                "data_type_full": _data_type(series),
                "is_nullable": bool(series.isna().any()),
                "ordinal_position": ordinal,
                "sample_count": int(len(series)),
                "null_count": int(series.isna().sum()),
                "distinct_count": int(non_null.astype(str).nunique()) if not non_null.empty else 0,
            }
        )
    return {
        **dict(feed),
        "parser_options": parser_options,
        "parser_options_json": json.dumps(parser_options, sort_keys=True),
        "sample_row_count": int(len(frame)),
        "columns": columns,
        "schema_status": "INFERRED",
    }


def infer_schemas(feeds: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    return [infer_schema(feed) for feed in feeds]


def profile_merge_key(feed: Mapping[str, Any], columns: Iterable[str]) -> Dict[str, Any]:
    """Validate a proposed file key against the same bounded sample used for inference."""
    proposed = [str(column).strip() for column in columns if str(column).strip()]
    if not proposed or len({column.casefold() for column in proposed}) != len(proposed):
        raise ValueError("A proposed ADLS merge key must contain unique, non-empty columns.")
    source_path = str(feed.get("remote_path") or feed.get("source_path") or "")
    file_format = str(feed.get("file_format") or feed.get("format") or "").lower()
    frame, _ = _dataframe(_download_sample(source_path), file_format)
    actual_by_name = {str(column).casefold(): str(column) for column in frame.columns}
    missing = [column for column in proposed if column.casefold() not in actual_by_name]
    if missing:
        raise ValueError(f"Proposed ADLS merge-key columns do not exist: {', '.join(missing)}")
    actual = [actual_by_name[column.casefold()] for column in proposed]
    sample_rows = int(len(frame))
    if sample_rows <= 0:
        raise ValueError("ADLS merge-key validation requires at least one sampled row.")
    complete = _complete_key_rows(frame, actual)
    complete_rows = int(len(complete))
    distinct_rows = int(len(complete.astype(str).drop_duplicates())) if complete_rows else 0
    return {
        "columns": proposed,
        "sample_rows": sample_rows,
        "complete_rows": complete_rows,
        "distinct_rows": distinct_rows,
        "completeness_ratio": round(complete_rows / sample_rows, 6),
        "uniqueness_ratio": round(distinct_rows / complete_rows, 6) if complete_rows else 0.0,
        "validation_scope": "bounded_source_sample",
    }


def _complete_key_rows(frame: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    selected = frame[list(columns)].copy()
    for column in selected.columns:
        if pd.api.types.is_object_dtype(selected[column].dtype) or pd.api.types.is_string_dtype(selected[column].dtype):
            selected[column] = selected[column].replace(r"^\s*$", pd.NA, regex=True)
    return selected.dropna(how="any")


def profile_merge_key_candidates(
    feed: Mapping[str, Any],
    *,
    preferred_columns: Iterable[str] = (),
) -> List[Dict[str, Any]]:
    """Find bounded, data-valid key candidates without assuming source-specific names."""
    source_path = str(feed.get("remote_path") or feed.get("source_path") or "")
    file_format = str(feed.get("file_format") or feed.get("format") or "").lower()
    frame, _ = _dataframe(_download_sample(source_path), file_format)
    if frame.empty:
        return []

    max_columns = max(1, int(_env("ADLS_MERGE_KEY_MAX_CANDIDATE_COLUMNS", "14")))
    max_width = max(1, int(_env("ADLS_MERGE_KEY_MAX_WIDTH", "4")))
    max_results = max(1, int(_env("ADLS_MERGE_KEY_MAX_VALIDATED_CANDIDATES", "32")))
    minimum_uniqueness = float(_env("ADLS_MERGE_KEY_MIN_SAMPLE_UNIQUENESS", "0.98"))
    minimum_completeness = float(_env("ADLS_MERGE_KEY_MIN_SAMPLE_COMPLETENESS", "1.0"))
    preferred = {str(column).casefold() for column in preferred_columns if str(column).strip()}

    ranked_columns = []
    for raw_name in frame.columns:
        name = str(raw_name)
        series = frame[raw_name]
        complete = series.replace(r"^\s*$", pd.NA, regex=True).dropna() if (
            pd.api.types.is_object_dtype(series.dtype) or pd.api.types.is_string_dtype(series.dtype)
        ) else series.dropna()
        completeness = len(complete) / len(frame)
        if completeness < minimum_completeness:
            continue
        uniqueness = complete.astype(str).nunique() / max(1, len(complete))
        normalized = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
        normalized = re.sub(r"[^a-z0-9]+", "_", normalized.casefold())
        identity_hint = bool(re.search(r"(^|_)(id|key|code|number|num|no|seq|sequence|version|revision|reference|ref)($|_)", normalized))
        score = (
            (10.0 if name.casefold() in preferred else 0.0)
            + (4.0 if identity_hint else 0.0)
            + (3.0 * uniqueness)
        )
        ranked_columns.append((score, uniqueness, name))
    ranked_columns.sort(key=lambda item: (-item[0], -item[1], item[2].casefold()))
    candidate_columns = [item[2] for item in ranked_columns[:max_columns]]

    validated = []
    for width in range(1, min(max_width, len(candidate_columns)) + 1):
        for keys in combinations(candidate_columns, width):
            complete = _complete_key_rows(frame, keys)
            complete_rows = int(len(complete))
            distinct_rows = int(len(complete.astype(str).drop_duplicates())) if complete_rows else 0
            completeness = complete_rows / len(frame)
            uniqueness = distinct_rows / complete_rows if complete_rows else 0.0
            if completeness < minimum_completeness or uniqueness < minimum_uniqueness:
                continue
            ordered_keys = [str(column) for column in frame.columns if str(column) in keys]
            validated.append(
                {
                    "columns": ordered_keys,
                    "sample_rows": int(len(frame)),
                    "complete_rows": complete_rows,
                    "distinct_rows": distinct_rows,
                    "completeness_ratio": round(completeness, 6),
                    "uniqueness_ratio": round(uniqueness, 6),
                    "validation_scope": "bounded_source_sample",
                    "candidate_score": round(sum(
                        next(item[0] for item in ranked_columns if item[2] == key)
                        for key in ordered_keys
                    ) / len(ordered_keys), 6),
                }
            )
    validated.sort(
        key=lambda item: (
            len(item["columns"]),
            -item["candidate_score"],
            [column.casefold() for column in item["columns"]],
        )
    )
    by_width = {
        width: [item for item in validated if len(item["columns"]) == width]
        for width in range(1, max_width + 1)
    }
    diverse = []
    offset = 0
    while len(diverse) < max_results and any(offset < len(items) for items in by_width.values()):
        for width in sorted(by_width):
            items = by_width[width]
            if offset < len(items):
                diverse.append(items[offset])
                if len(diverse) == max_results:
                    break
        offset += 1
    return diverse


def source_catalog(*, platform: str) -> tuple[Dict[str, Any], Dict[str, Any]]:
    source_system_id = int(stable_bigint("source_system", "adls", account_url(), file_system()))
    connection_id = int(stable_bigint("connection", source_system_id, source_base_path()))
    source_system = {
        "source_system_id": source_system_id,
        "source_system_name": "Insurance ADLS",
        "business_domain": "Insurance",
        "owner_name": "Data Platform",
        "owner_email": "",
        "description": "Approved ADLS file source",
        "active_flag": True,
    }
    connection = {
        "connection_id": connection_id,
        "source_system_id": source_system_id,
        "connection_name": "Insurance ADLS",
        "connection_type": "ADLS",
        "connection_contract_name": "ADLS_CONNECTION",
        "connection_schema_version": "1.0",
        "host_name": account_url().split("://", 1)[-1],
        "port": 443,
        "base_path": source_base_path(),
        "base_url": account_url(),
        "database_name": file_system(),
        "auth_type": "SERVICE_PRINCIPAL",
        "secrets_json": json.dumps(
            {
                "tenant_id": {"scope": "DEPLOYMENT_ENV", "key": "AZURE_TENANT_ID"},
                "client_id": {"scope": "DEPLOYMENT_ENV", "key": "AZURE_CLIENT_ID"},
                "client_secret": {"scope": "DEPLOYMENT_ENV", "key": "AZURE_CLIENT_SECRET"},
            },
            sort_keys=True,
        ),
        "config_json": json.dumps(
            {
                "allowed_extensions": sorted(SUPPORTED_FILE_FORMATS),
                "allowed_project_ids": ["*"],
                "source_root": source_root(),
                "target_platform": str(platform or "databricks").lower(),
            },
            sort_keys=True,
        ),
        "config_version": 1,
        "is_current": True,
        "active_flag": True,
    }
    connection["config_hash"] = canonical_json_hash(
        {
            key: connection.get(key)
            for key in (
                "source_system_id",
                "connection_name",
                "connection_type",
                "connection_contract_name",
                "connection_schema_version",
                "base_path",
                "base_url",
                "auth_type",
                "secrets_json",
                "config_json",
            )
        }
    )
    return source_system, connection
