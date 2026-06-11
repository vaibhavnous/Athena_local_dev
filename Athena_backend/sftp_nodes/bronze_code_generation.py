from __future__ import annotations

import ast
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from state import Stage01State
from utilis.ai_store_writer import ai_store_db_writer
from utilis.db import config, get_pipeline_connection
from utilis.logger import logger
from sftp_nodes.review_gates import persist_bronze_execution_plan


BRONZE_OUTPUT_DIR = os.path.join(os.getcwd(), "generated_code", "bronze")


def _run_slug(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]+", "_", str(value or "run")).strip("_")[:64] or "run"


def _safe_sql_name(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_]+", "_", str(value or "")).strip("_").lower()
    return cleaned or "unknown"


def _pipeline_schema() -> str:
    return config["azure_sql"]["pipeline_schema"]


def _is_file_source(source: str) -> bool:
    value = str(source or "").lower()
    return value in {"sftp", "adls_gen2"} or any(
        token in value for token in ("adls", "abfs", "abfss", "blob", "s3")
    )


def _is_databricks_readable_path(path: str) -> bool:
    value = str(path or "").strip()
    return value.startswith(("dbfs:/", "/Volumes/", "abfss://", "s3://"))


def _adls_abfss_from_remote_path(remote_path: str) -> str:
    account_url = str(os.getenv("ADLS_ACCOUNT_URL") or "").strip()
    file_system = str(os.getenv("ADLS_FILE_SYSTEM") or "").strip()
    if _looks_like_local_path(remote_path):
        return ""
    if not account_url or not file_system:
        return ""

    account = (
        account_url.replace("https://", "")
        .replace("http://", "")
        .split(".", 1)[0]
        .strip()
    )
    normalized = str(remote_path or "").strip().lstrip("/")
    if not account or not normalized:
        return ""
    return f"abfss://{file_system}@{account}.dfs.core.windows.net/{normalized}"


def _adls_abfss_from_root(entity: str) -> str:
    account_url = str(os.getenv("ADLS_ACCOUNT_URL") or "").strip()
    file_system = str(os.getenv("ADLS_FILE_SYSTEM") or "").strip()
    source_root = (
        str(os.getenv("ADLS_SOURCE_ROOT") or os.getenv("ADLS_VENDOR_ROOT") or "evention/vendor1/machine1")
        .strip()
        .strip("/")
    )
    if not account_url or not file_system or not source_root:
        return ""

    account = (
        account_url.replace("https://", "")
        .replace("http://", "")
        .split(".", 1)[0]
        .strip()
    )
    entity_part = str(entity or "").strip().strip("/")
    full_path = f"{source_root}/{entity_part}/" if entity_part else f"{source_root}/"
    return f"abfss://{file_system}@{account}.dfs.core.windows.net/{full_path}"


def _looks_like_local_path(path: str) -> bool:
    value = str(path or "").strip()
    if not value:
        return False

    # Windows path: C:\Users\...
    if re.match(r"^[a-zA-Z]:\\", value):
        return True

    # Common local/backend paths
    local_tokens = [
        "\\Users\\",
        "/Users/",
        "/home/",
        "Athena_backend/uploads",
        "uploads\\",
        "uploads/",
    ]
    return any(token in value for token in local_tokens)


def _first_present(*values: Any) -> str:
    for value in values:
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _resolve_databricks_source_path(
    feed: Dict[str, Any],
    state: Stage01State,
    vendor: str,
    entity: str,
    source_type: str,
) -> str:
    """
    Resolve the path that should appear inside the generated Databricks script.

    Do not prefer local file_path unless it is already Databricks-readable.
    """

    candidates = [
        feed.get("databricks_source_path"),
        feed.get("landing_path"),
        feed.get("cloud_path"),
        state.get("databricks_source_path"),
        state.get("landing_path"),
    ]

    for candidate in candidates:
        candidate_str = str(candidate or "").strip()
        if candidate_str and _is_databricks_readable_path(candidate_str):
            return candidate_str

    # Use file_path only if it is already cloud/Databricks-readable.
    file_path = str(feed.get("file_path") or "").strip()
    if file_path and _is_databricks_readable_path(file_path):
        return file_path

    # For ADLS feeds, registry rows may only have a remote_path. Rebuild a valid
    # abfss:// URI from the configured account and file system.
    remote_path = str(feed.get("remote_path") or "").strip()
    if source_type != "sftp" and remote_path:
        abfss_path = _adls_abfss_from_remote_path(remote_path)
        if abfss_path:
            remote_name = Path(remote_path).name
            if remote_name and "." in remote_name:
                return abfss_path.rsplit("/", 1)[0].rstrip("/") + "/"
            return abfss_path.rstrip("/") + "/"

    if source_type != "sftp":
        root_path = _adls_abfss_from_root(entity)
        if root_path:
            return root_path.rstrip("/") + "/"

    # Safe fallback for generated review script.
    # Gate 4 will still mark invalid if this path does not exist later.
    if source_type == "sftp":
        return f"/Volumes/<catalog>/<schema>/<volume>/sftp_landing/{vendor}/{entity}/"

    return f"/Volumes/<catalog>/<schema>/<volume>/adls_landing/{vendor}/{entity}/"


def _approved_feeds_from_registry(state: Stage01State) -> List[Dict[str, Any]]:
    feeds: List[Dict[str, Any]] = []

    candidate_feeds = state.get("candidate_feeds") or []
    if isinstance(candidate_feeds, list):
        feeds.extend([dict(feed) for feed in candidate_feeds if isinstance(feed, dict)])

    candidate_feed = state.get("candidate_feed")
    if isinstance(candidate_feed, dict) and candidate_feed:
        feeds.append(dict(candidate_feed))

    feed_ids = [
        str(feed.get("feed_id") or "").strip()
        for feed in feeds
        if str(feed.get("feed_id") or "").strip()
    ]
    feed_ids = list(dict.fromkeys(feed_ids))

    if not feed_ids:
        return []

    placeholders = ", ".join("?" for _ in feed_ids)
    conn = get_pipeline_connection()

    try:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            WITH ranked AS (
                SELECT
                    feed_id,
                    vendor,
                    entity,
                    format,
                    file_name,
                    file_path,
                    remote_path,
                    status,
                    source,
                    approved_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY feed_id
                        ORDER BY updated_at DESC, created_at DESC
                    ) AS rn
                FROM [{_pipeline_schema()}].[file_feed_registry]
                WHERE feed_id IN ({placeholders})
            )
            SELECT
                feed_id,
                vendor,
                entity,
                format,
                file_name,
                file_path,
                remote_path,
                status,
                source,
                approved_at
            FROM ranked
            WHERE rn = 1
              AND UPPER(status) = 'APPROVED'
            """,
            *feed_ids,
        )

        rows = cursor.fetchall()

        return [
            {
                "feed_id": row.feed_id,
                "vendor": row.vendor,
                "entity": row.entity,
                "format": row.format,
                "file_name": row.file_name,
                "file_path": row.file_path,
                "remote_path": row.remote_path,
                "status": row.status,
                "source": row.source,
                "approved_at": getattr(row, "approved_at", None),
            }
            for row in rows
        ]
    finally:
        conn.close()


def _schema_status_column() -> Optional[str]:
    """
    Detect an approval/status column on file_feed_schema_registry.

    This makes the code safer if the table currently uses schema_status,
    review_status, approval_status, or status.
    """

    possible_columns = ["schema_status", "review_status", "approval_status", "status"]
    conn = get_pipeline_connection()

    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ?
              AND TABLE_NAME = 'file_feed_schema_registry'
            """,
            _pipeline_schema(),
        )
        existing = {str(row.COLUMN_NAME).lower() for row in cursor.fetchall()}

        for col in possible_columns:
            if col.lower() in existing:
                return col

        return None
    finally:
        conn.close()


def _approved_schema(feed_id: str) -> Optional[Dict[str, Any]]:
    status_col = _schema_status_column()
    if not status_col:
        logger.warning("file_feed_schema_registry has no schema approval column; approved schema lookup blocked", extra={"feed_id": feed_id})
        return None

    status_filter = f"AND UPPER({status_col}) = 'APPROVED'"

    conn = get_pipeline_connection()

    try:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT TOP 1
                feed_id,
                vendor,
                entity,
                format,
                row_tag,
                schema_json,
                schema_fingerprint,
                version,
                discovered_at
            FROM [{_pipeline_schema()}].[file_feed_schema_registry]
            WHERE feed_id = ?
            {status_filter}
            ORDER BY version DESC, discovered_at DESC
            """,
            feed_id,
        )

        row = cursor.fetchone()

        if not row:
            return None

        return {
            "feed_id": row.feed_id,
            "vendor": row.vendor,
            "entity": row.entity,
            "format": row.format,
            "row_tag": getattr(row, "row_tag", None),
            "schema_json": json.loads(row.schema_json) if row.schema_json else [],
            "schema_fingerprint": row.schema_fingerprint,
            "version": row.version,
            "discovered_at": row.discovered_at,
        }
    finally:
        conn.close()


def _primary_keys(schema_columns: List[Dict[str, Any]]) -> List[str]:
    explicit = [
        str(col.get("column_name") or "")
        for col in schema_columns
        if col.get("is_primary_key")
    ]

    if explicit:
        return [col for col in explicit if col]

    inferred = [
        str(col.get("column_name") or "")
        for col in schema_columns
        if "id" in str(col.get("column_name") or "").lower()
    ]

    return [col for col in inferred if col]


def _watermark_column(schema_columns: List[Dict[str, Any]]) -> Optional[str]:
    for col in schema_columns:
        name = str(col.get("column_name") or "")
        lowered = name.lower()

        if any(token in lowered for token in ("modified", "updated", "timestamp", "date", "created")):
            return name

    return None


def _xml_row_tag(feed: Dict[str, Any], schema: Dict[str, Any], state: Stage01State, entity: str) -> str:
    row_tag = _first_present(
        feed.get("row_tag"),
        feed.get("rowTag"),
        schema.get("row_tag"),
        schema.get("rowTag"),
        state.get("row_tag"),
        state.get("rowTag"),
    )

    if row_tag and str(row_tag).strip().lower() != str(entity or "").strip().lower():
        return row_tag

    schema_columns = schema.get("schema_json") or []
    for col in schema_columns:
        value = _first_present(col.get("row_tag"), col.get("rowTag"))
        if value and str(value).strip().lower() != str(entity or "").strip().lower():
            return value

    return "RemoteMessage"


def _bronze_config(feed: Dict[str, Any], schema: Dict[str, Any], state: Stage01State) -> Dict[str, Any]:
    vendor = str(feed.get("vendor") or schema.get("vendor") or "Vendor1")
    entity = str(feed.get("entity") or schema.get("entity") or "unknown")

    vendor_safe = _safe_sql_name(vendor)
    entity_safe = _safe_sql_name(entity)

    bronze_schema = str(state.get("bronze_schema") or os.getenv("BRONZE_SCHEMA", "bronze"))
    bronze_schema_safe = _safe_sql_name(bronze_schema)
    volume_catalog = str(os.getenv("DATABRICKS_VOLUME_CATALOG", "main"))
    volume_schema = str(os.getenv("DATABRICKS_VOLUME_SCHEMA", bronze_schema_safe))
    volume_name = str(os.getenv("DATABRICKS_VOLUME_NAME", "pipeline_artifacts"))

    source_type = str(feed.get("source") or state.get("source") or "sftp").lower()
    file_format = str(feed.get("format") or schema.get("format") or "csv").lower()
    schema_columns = schema.get("schema_json") or []

    landing_path = _resolve_databricks_source_path(
        feed=feed,
        state=state,
        vendor=vendor,
        entity=entity,
        source_type=source_type,
    )

    return {
        "feed_id": feed["feed_id"],
        "vendor": vendor,
        "entity": entity,
        "vendor_safe": vendor_safe,
        "entity_safe": entity_safe,
        "source_type": source_type,
        "file_format": file_format,
        "row_tag": _xml_row_tag(feed, schema, state, entity) if file_format == "xml" else None,
        "landing_path": landing_path,
        "original_file_path": str(feed.get("file_path") or ""),
        "remote_path": str(feed.get("remote_path") or ""),
        "bronze_output_path": f"/Volumes/{volume_catalog}/{volume_schema}/{volume_name}/tables/bronze/{vendor_safe}/{entity_safe}",
        "checkpoint_path": f"/Volumes/{volume_catalog}/{volume_schema}/{volume_name}/checkpoints/bronze/{vendor_safe}/{entity_safe}",
        "schema_location": f"/Volumes/{volume_catalog}/{volume_schema}/{volume_name}/schemas/bronze/{vendor_safe}/{entity_safe}",
        "target_table": f"{bronze_schema_safe}.{vendor_safe}_{entity_safe}_raw",
        "schema_columns": schema_columns,
        "schema_version": schema.get("version"),
        "schema_fingerprint": schema.get("schema_fingerprint"),
        "primary_keys": _primary_keys(schema_columns),
        "watermark_column": _watermark_column(schema_columns),
        "validation_checklist": [
            "Feed approved in file_feed_registry",
            "Schema approved in file_feed_schema_registry",
            "Source path is Databricks-readable",
            "Primary keys reviewed",
            "Watermark column reviewed",
            "Landing and output paths confirmed",
            "Generated Bronze script compiles",
            "No forbidden patterns found in generated script",
        ],
    }


def _effective_xml_row_tag(config_json: Dict[str, Any]) -> str:
    row_tag = str(config_json.get("row_tag") or "").strip()
    entity = str(config_json.get("entity") or "").strip()

    if row_tag and row_tag.lower() != entity.lower():
        return row_tag

    return "RemoteMessage"


def _is_probably_integer_xml_column(column_name: str) -> bool:
    lowered = str(column_name or "").lower()
    terminal = lowered.rsplit("_", 1)[-1]

    if terminal in {"user", "iskit", "endshiftdeposit", "total"}:
        return True

    integer_tokens = (
        "transactionid",
        "userid",
        "userlevel",
        "groupid",
        "machineid",
        "_nop",
        "_id",
        "_n",
        "_value",
        "_valid",
        "counting",
        "counted",
        "count",
        "template",
        "bag",
    )
    return any(token in lowered for token in integer_tokens)


def _spark_cast_type(dtype: str, file_format: str = "", column_name: str = "") -> str:
    value = str(dtype or "string").lower()
    lowered_name = str(column_name or "").lower()

    if str(file_format or "").lower() == "xml":
        if "version" in lowered_name or re.search(r"(^|_)fw(__|_|$)", lowered_name):
            return "string"

        if lowered_name.endswith("authenticationmode"):
            return "string"

        if lowered_name.endswith("__date") or lowered_name.endswith("_date") or lowered_name.endswith("accountingdate"):
            return "date"

        if lowered_name.endswith("__time") or lowered_name.endswith("_time"):
            return "timestamp"

        if "mode" in lowered_name or value in {"boolean", "bool"}:
            return "long"

        if value in {"integer", "int", "bigint", "long"}:
            return "long"

        if value == "double" and _is_probably_integer_xml_column(lowered_name):
            return "long"

    mapping = {
        "integer": "long",
        "int": "long",
        "bigint": "long",
        "long": "long",
        "double": "double",
        "float": "float",
        "decimal": "decimal(38,18)",
        "numeric": "decimal(38,18)",
        "boolean": "long",
        "bool": "long",
        "timestamp": "timestamp",
        "datetime": "timestamp",
        "date": "date",
        "string": "string",
        "str": "string",
    }

    return mapping.get(value, "string")


def _flatten_xml_column_name(raw_name: str, row_tag: str) -> str:
    name = str(raw_name or "").strip()
    root = str(row_tag or "RemoteMessage").strip() or "RemoteMessage"

    if not name:
        return name

    if name == root or name.startswith(f"{root}_"):
        return name

    parts = [part for part in name.split(".") if part]
    output = root

    for part in parts:
        if part.startswith("_"):
            output += part
        else:
            output += f"_{part}"

    return output


def _evention_xml_column_names(raw_name: str, row_tag: str) -> List[str]:
    name = str(raw_name or "").strip()
    root = str(row_tag or "RemoteMessage").strip() or "RemoteMessage"
    lowered = name.lower()

    if not name:
        return []

    if name == root or name.startswith(f"{root}_"):
        return [name]

    evention_map = {
        "currency": [f"{root}_Details__Currency"],
        "total": [f"{root}_Details_total"],
        "countings": [
            f"{root}_Details_countings__valid",
            f"{root}_Details_countings_counted",
        ],
        "count": [
            f"{root}_contentAfter_count",
            f"{root}_contentBefore_count",
            f"{root}_destDetails_count",
        ],
    }

    if lowered in evention_map:
        return evention_map[lowered]

    return [_flatten_xml_column_name(name, root)]


def _evention_xml_supplemental_columns(row_tag: str) -> List[tuple[str, str]]:
    root = str(row_tag or "RemoteMessage").strip() or "RemoteMessage"

    return [
        (f"{root}__CustomerCode", "string"),
        (f"{root}__Date", "date"),
        (f"{root}__DeviceID", "string"),
        (f"{root}__NOP", "long"),
        (f"{root}__TestMode", "long"),
        (f"{root}__Time", "timestamp"),
        (f"{root}__operation", "string"),
        (f"{root}_Details_countings_counted", "string"),
        (f"{root}_contentAfter_count", "string"),
        (f"{root}_contentBefore_count", "string"),
        (f"{root}_destDetails_count", "string"),
    ]


def _expected_schema_literals(
    schema_columns: List[Dict[str, Any]],
    file_format: str = "",
    row_tag: str = "",
) -> tuple[str, str]:
    expected_columns: List[str] = []
    expected_types: Dict[str, str] = {}
    is_xml = str(file_format or "").lower() == "xml"

    for col in schema_columns:
        raw_name = str(col.get("column_name") or "").strip()

        if not raw_name:
            continue

        names = _evention_xml_column_names(raw_name, row_tag) if is_xml else [raw_name]

        for name in names:
            if name not in expected_columns:
                expected_columns.append(name)

            expected_types[name] = _spark_cast_type(
                str(col.get("data_type") or "string"),
                file_format=file_format,
                column_name=name,
            )

    if is_xml:
        for name, dtype in _evention_xml_supplemental_columns(row_tag):
            if name not in expected_columns:
                expected_columns.append(name)
            expected_types[name] = dtype

    return json.dumps(expected_columns, indent=4), json.dumps(expected_types, indent=4)


def _generate_script(config_json: Dict[str, Any], run_id: str, pipeline_version: str) -> str:
    file_format = str(config_json.get("file_format") or "csv").lower()
    source_feed = f"{config_json['vendor']}/{config_json['entity']}"
    row_tag = (
        _effective_xml_row_tag(config_json)
        if file_format == "xml"
        else str(config_json.get("row_tag") or config_json.get("entity") or "row")
    )

    expected_columns_literal, expected_types_literal = _expected_schema_literals(
        config_json.get("schema_columns") or [],
        file_format=file_format,
        row_tag=row_tag,
    )

    script = f'''
import json
import os
import re
import xml.etree.ElementTree as ET
from io import BytesIO

import pandas as pd
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, MapType, StructType

RUN_ID = {json.dumps(run_id)}
PIPELINE_VERSION = {json.dumps(pipeline_version or "v1")}
SOURCE_TYPE = {json.dumps(config_json["source_type"])}
SOURCE_FEED = {json.dumps(source_feed)}
FILE_FORMAT = {json.dumps(file_format)}
ROW_TAG = {json.dumps(row_tag)}
SOURCE_PATH = {json.dumps(config_json["landing_path"])}
TARGET_TABLE = {json.dumps(config_json["target_table"])}
SCHEMA_LOCATION = {json.dumps(config_json["schema_location"])}
CHECKPOINT_PATH = {json.dumps(config_json["checkpoint_path"])}
SECRET_SCOPE = "dataedge-secrets"
TENANT_KEY = "adls-tenant-id"
CLIENT_ID_KEY = "adls-client-id"
CLIENT_SECRET_KEY = "adls-client-secret"
DEBUG = False
EXPECTED_COLUMNS = {expected_columns_literal}
EXPECTED_TYPES = {expected_types_literal}

print(f"Starting Bronze ingestion for {{SOURCE_FEED}}")
print(f"Source path: {{SOURCE_PATH}}")
print(f"Target table: {{TARGET_TABLE}}")


def _secret(key):
    return dbutils.secrets.get(SECRET_SCOPE, key)


def _parse_abfss_path(path):
    match = re.match(r"^abfss://([^@]+)@([^.]+)\\.dfs\\.core\\.windows\\.net/(.*)$", path.strip())
    if not match:
        raise ValueError(f"SOURCE_PATH must be an abfss:// ADLS Gen2 path, got: {{path}}")
    file_system, account_name, source_prefix = match.groups()
    return file_system, account_name, source_prefix.strip("/")


def _file_system_client():
    file_system, account_name, _ = _parse_abfss_path(SOURCE_PATH)
    credential = ClientSecretCredential(
        tenant_id=_secret(TENANT_KEY),
        client_id=_secret(CLIENT_ID_KEY),
        client_secret=_secret(CLIENT_SECRET_KEY),
    )
    service_client = DataLakeServiceClient(
        account_url=f"https://{{account_name}}.dfs.core.windows.net",
        credential=credential,
    )
    return service_client.get_file_system_client(file_system)


def _list_source_files(file_system_client):
    _, _, source_prefix = _parse_abfss_path(SOURCE_PATH)
    paths = list(file_system_client.get_paths(path=source_prefix, recursive=False))
    source_files = [
        item
        for item in paths
        if not getattr(item, "is_directory", False)
        and str(getattr(item, "name", "")).lower().endswith(f".{{FILE_FORMAT}}")
    ]
    if not source_files:
        raise ValueError(f"No .{{FILE_FORMAT}} files found under {{SOURCE_PATH}}")
    return source_files


def _download_file(file_system_client, file_path):
    return file_system_client.get_file_client(file_path).download_file().readall()


def _xml_strings_from_bytes(payload):
    root = ET.fromstring(payload)
    if root.tag.split("}}")[-1] == ROW_TAG:
        return [ET.tostring(root, encoding="unicode")]
    rows = [node for node in root.iter() if node.tag.split("}}")[-1] == ROW_TAG]
    return [ET.tostring(node, encoding="unicode") for node in rows]


def _safe_col_name(value):
    cleaned = re.sub(r"[^A-Za-z0-9_]+", "_", str(value or ""))
    return cleaned if re.search(r"[A-Za-z0-9]", cleaned) else "unknown"


def _quote_path(path):
    return ".".join(f"`{{part}}`" for part in path.split("."))


def _flatten_columns(schema, prefix, path):
    columns = []
    for field in schema.fields:
        child_path = f"{{path}}.{{field.name}}"
        output_name = _safe_col_name(f"{{prefix}}_{{field.name}}")
        dtype = field.dataType
        if isinstance(dtype, StructType):
            columns.extend(_flatten_columns(dtype, output_name, child_path))
        elif isinstance(dtype, (ArrayType, MapType)):
            columns.append(F.to_json(F.col(_quote_path(child_path))).alias(output_name))
        else:
            columns.append(F.col(_quote_path(child_path)).alias(output_name))
    return columns


def _project_column(available_columns, name, target_type):
    if name in available_columns:
        return F.col(f"`{{name}}`").cast(target_type).alias(name)
    return F.lit(None).cast(target_type).alias(name)


def _project_bronze_df(source_df):
    available_columns = set(source_df.columns)
    expected_set = set(EXPECTED_COLUMNS)
    extra_names = sorted(available_columns - expected_set)
    if DEBUG and extra_names:
        print("Extra source columns not in approved schema:", extra_names)
    projected = [
        _project_column(available_columns, name, EXPECTED_TYPES.get(name, "string"))
        for name in EXPECTED_COLUMNS
    ]
    extra_columns = [
        F.col(f"`{{name}}`")
        for name in extra_names
    ]
    return source_df.select(*(projected + extra_columns)) if projected or extra_columns else source_df


fs_client = _file_system_client()
_source_files = _list_source_files(fs_client)
_source_file_names = ", ".join([os.path.basename(item.name) for item in _source_files])

if FILE_FORMAT == "csv":
    frames = [pd.read_csv(BytesIO(_download_file(fs_client, item.name))) for item in _source_files]
    pdf = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
    df = spark.createDataFrame(pdf)
elif FILE_FORMAT == "json":
    records = []
    for item in _source_files:
        payload = json.loads(_download_file(fs_client, item.name).decode("utf-8"))
        if isinstance(payload, list):
            records.extend([row for row in payload if isinstance(row, dict)])
        elif isinstance(payload, dict):
            nested_rows = [
                value for value in payload.values()
                if isinstance(value, list) and value and isinstance(value[0], dict)
            ]
            if nested_rows:
                records.extend(nested_rows[0])
            else:
                records.append(payload)
    if not records:
        raise ValueError(f"No JSON records found under {{SOURCE_PATH}}")
    df = spark.createDataFrame(pd.DataFrame(records))
elif FILE_FORMAT == "xml":
    xml_rows = []
    for item in _source_files:
        xml_rows.extend(_xml_strings_from_bytes(_download_file(fs_client, item.name)))
    if not xml_rows:
        raise ValueError(f"No <{{ROW_TAG}}> records found under {{SOURCE_PATH}}")
    xml_df = spark.createDataFrame([(text,) for text in xml_rows], ["_xml"])
    xml_schema = spark.range(1).select(
        F.schema_of_xml(F.lit(xml_rows[0]), {{"rowTag": ROW_TAG}}).alias("schema")
    ).first()["schema"]
    parsed_df = xml_df.select(F.from_xml(F.col("_xml"), xml_schema, {{"rowTag": ROW_TAG}}).alias(ROW_TAG))
    flattened_columns = _flatten_columns(parsed_df.schema[ROW_TAG].dataType, ROW_TAG, ROW_TAG)
    df = parsed_df.select(*flattened_columns)
else:
    raise ValueError(f"Unsupported FILE_FORMAT: {{FILE_FORMAT}}")

if df.limit(1).count() == 0:
    raise ValueError(f"No records found in Bronze source path: {{SOURCE_PATH}}")

if DEBUG:
    print("Available columns:", df.columns)

bronze_df = _project_bronze_df(df)

bronze_df = (
    bronze_df
    .withColumn("_run_id", F.lit(RUN_ID))
    .withColumn("_ingestion_timestamp", F.current_timestamp())
    .withColumn("_source_system", F.lit(SOURCE_TYPE))
    .withColumn("_source_feed", F.lit(SOURCE_FEED))
    .withColumn("_source_file_path", F.lit(SOURCE_PATH))
    .withColumn("_source_file_name", F.lit(_source_file_names))
    .withColumn("_file_modification_time", F.lit(None).cast("timestamp"))
    .withColumn("_pipeline_version", F.lit(PIPELINE_VERSION))
    .withColumn("_rescued_data", F.lit(None).cast("string"))
)

target_schema = ".".join(TARGET_TABLE.split(".")[:-1])
if target_schema:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {{target_schema}}")

(
    bronze_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

row_count = spark.table(TARGET_TABLE).count()
print(f"Bronze ingestion completed: {{TARGET_TABLE}}")
print(f"Total rows now in target: {{row_count:,}}")
'''.strip()

    return script


def _validate_plan(plan: Dict[str, Any]) -> List[str]:
    issues: List[str] = []
    cfg = plan.get("bronze_config") or {}

    landing_path = str(cfg.get("landing_path") or "")
    original_file_path = str(cfg.get("original_file_path") or "")

    if not landing_path:
        issues.append("landing_path_missing")
    elif not _is_databricks_readable_path(landing_path):
        issues.append("invalid_databricks_source_path")

    if (
        original_file_path
        and _looks_like_local_path(original_file_path)
        and str(cfg.get("source_type") or "").lower() == "sftp"
    ):
        issues.append("registry_file_path_is_local_backend_path")

    if not cfg.get("target_table"):
        issues.append("target_table_missing")

    if cfg.get("file_format") == "xml" and not cfg.get("row_tag"):
        issues.append("xml_row_tag_missing")

    if (
        cfg.get("file_format") == "xml"
        and str(cfg.get("row_tag") or "").strip().lower()
        == str(cfg.get("entity") or "").strip().lower()
    ):
        issues.append("xml_row_tag_equals_entity_likely_wrong")

    script = plan.get("generated_bronze_script") or ""

    if not script:
        issues.append("script_missing")
        return issues

    try:
        compile(script, "<bronze_script>", "exec")
        tree = ast.parse(script)

        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name) and node.func.id in {"eval", "exec"}:
                    issues.append(f"blocked_call:{node.func.id}")

            if isinstance(node, ast.Name) and node.id in {"subprocess"}:
                issues.append(f"blocked_name:{node.id}")

            if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
                if node.value.id == "os" and node.attr == "system":
                    issues.append("blocked_os_system")
    except SyntaxError as exc:
        issues.append(f"syntax_error:{exc.lineno}:{exc.msg}")
    except Exception as exc:
        issues.append(f"ast_scan_failed:{exc}")

    return issues


def _write_plan_artifact(plan: Dict[str, Any]) -> str:
    os.makedirs(BRONZE_OUTPUT_DIR, exist_ok=True)

    path = (
        Path(BRONZE_OUTPUT_DIR)
        / f"{_run_slug(plan['run_id'])}_{_run_slug(plan['feed_id'])}_bronze_plan.json"
    )

    path.write_text(json.dumps(plan, indent=2, default=str), encoding="utf-8")

    return str(path)


def _write_bundle(bundle: Dict[str, Any]) -> str:
    os.makedirs(BRONZE_OUTPUT_DIR, exist_ok=True)

    path = Path(BRONZE_OUTPUT_DIR) / f"{_run_slug(bundle['run_id'])}_bronze_scripts.json"
    path.write_text(json.dumps(bundle, indent=2, default=str), encoding="utf-8")

    return str(path)


def sftp_bronze_code_generation_node(state: Stage01State) -> Stage01State:
    new_state = state.copy()

    if str(new_state.get("status") or "").upper() == "FAILED":
        return new_state

    run_id = str(
        state.get("run_id")
        or f"sftp_bronze_{datetime.now(timezone.utc).timestamp()}"
    )
    pipeline_version = str(state.get("pipeline_version") or "v1")

    approved_feeds = [
        feed
        for feed in _approved_feeds_from_registry(state)
        if _is_file_source(str(feed.get("source") or state.get("source") or ""))
    ]

    if not approved_feeds:
        new_state["bronze_generation_status"] = "FAILED"
        new_state["status"] = "FAILED"
        new_state["bronze_generation_error"] = "No approved file-source feeds found in file_feed_registry"
        return new_state

    plans: List[Dict[str, Any]] = []

    for feed in approved_feeds:
        schema = _approved_schema(str(feed.get("feed_id") or ""))

        if not schema:
            new_state["bronze_generation_status"] = "FAILED"
            new_state["status"] = "FAILED"
            new_state["bronze_generation_error"] = (
                f"No APPROVED schema snapshot found for feed {feed.get('feed_id')}"
            )
            return new_state

        config_json = _bronze_config(feed, schema, state)
        script_text = _generate_script(config_json, run_id, pipeline_version)

        plan = {
            "run_id": run_id,
            "feed_id": feed["feed_id"],
            "vendor": config_json.get("vendor"),
            "entity": config_json.get("entity"),
            "source_type": config_json["source_type"],
            "file_format": config_json.get("file_format"),
            "feed_summary": {
                "file_name": feed.get("file_name"),
                "file_path": feed.get("file_path"),
                "remote_path": feed.get("remote_path"),
                "format": feed.get("format"),
                "status": feed.get("status"),
            },
            "bronze_config": config_json,
            "generated_bronze_config": config_json,
            "generated_bronze_script": script_text,
            "schema_version": config_json.get("schema_version"),
            "schema_fingerprint": config_json.get("schema_fingerprint"),
            "primary_keys": config_json.get("primary_keys"),
            "watermark_column": config_json.get("watermark_column"),
            "landing_path": config_json.get("landing_path"),
            "target_table": config_json.get("target_table"),
            "bronze_output_path": config_json.get("bronze_output_path"),
            "checkpoint_path": config_json.get("checkpoint_path"),
            "schema_location": config_json.get("schema_location"),
            "validation_checklist": config_json.get("validation_checklist"),
            "review_status": "PENDING",
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

        plan["validation_issues"] = _validate_plan(plan)
        plan["plan_valid"] = len(plan["validation_issues"]) == 0
        plan["artifact_path"] = _write_plan_artifact(plan)

        persist_bronze_execution_plan(plan)
        plans.append(plan)

    bundle = {
        "run_id": run_id,
        "fingerprint": str(state.get("fingerprint") or run_id),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "plan_count": len(plans),
        "scripts": [
            {
                "run_id": run_id,
                "feed_id": plan["feed_id"],
                "entity": plan["entity"],
                "script_path": plan["artifact_path"],
                "script_body": plan["generated_bronze_script"],
                "status": "COMPLETED" if plan["plan_valid"] else "INVALID",
                "validation_issues": plan["validation_issues"],
            }
            for plan in plans
        ],
    }

    bundle_path = _write_bundle(bundle)

    ai_store_db_writer(
        run_id=run_id,
        stage="SFTP Bronze Code Generation",
        artifact_type="SFTP_BRONZE_GENERATION",
        payload={**bundle, "plans": plans},
        schema_version="SFTP_BRONZE_GENERATION_v3",
        prompt_version="SFTP_BRONZE_REVIEWABLE_v3",
        faithfulness_status="PASSED" if all(plan["plan_valid"] for plan in plans) else "NEEDS_REVIEW",
        token_count=0,
        input_tokens=0,
        output_tokens=0,
    )

    bronze_review_artifact = {
        "run_id": run_id,
        "generated_at": bundle["generated_at"],
        "feeds": [
            {
                "feed_summary": plan["feed_summary"],
                "source_type": plan["source_type"],
                "vendor": plan["vendor"],
                "entity": plan["entity"],
                "file_format": plan["file_format"],
                "approved_schema": plan["bronze_config"]["schema_columns"],
                "schema_version": plan["schema_version"],
                "schema_fingerprint": plan["schema_fingerprint"],
                "primary_keys": plan["primary_keys"],
                "watermark_column": plan["watermark_column"],
                "landing_path": plan["landing_path"],
                "target_table": plan["target_table"],
                "bronze_output_path": plan["bronze_output_path"],
                "checkpoint_path": plan["checkpoint_path"],
                "schema_location": plan["schema_location"],
                "generated_bronze_config": plan["generated_bronze_config"],
                "generated_bronze_script": plan["generated_bronze_script"],
                "validation_checklist": plan["validation_checklist"],
                "validation_issues": plan["validation_issues"],
                "plan_valid": plan["plan_valid"],
                "review_status": plan["review_status"],
            }
            for plan in plans
        ],
    }

    new_state.update(
        {
            "bronze_generation_status": "COMPLETED"
            if all(plan["plan_valid"] for plan in plans)
            else "NEEDS_REVIEW",
            "bronze_generation_error": None,
            "bronze_generated_at": bundle["generated_at"],
            "bronze_generation_results": plans,
            "bronze_execution_plan": {**bundle, "plans": plans},
            "bronze_review_artifact": bronze_review_artifact,
            "bronze_generation_bundle_path": bundle_path,
        }
    )

    return new_state
