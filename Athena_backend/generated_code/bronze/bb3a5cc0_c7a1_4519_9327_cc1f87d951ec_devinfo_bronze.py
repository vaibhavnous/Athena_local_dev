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

RUN_ID = "bb3a5cc0-c7a1-4519-9327-cc1f87d951ec"
PIPELINE_VERSION = "v1"
SOURCE_TYPE = "adls_gen2"
SOURCE_FEED = "Vendor1/DevInfo"
FILE_FORMAT = "xml"
ROW_TAG = "RemoteMessage"
SOURCE_PATH = "abfss://athena@atheastorage.dfs.core.windows.net/evention/vendor1/machine1/DevInfo/"
TARGET_TABLE = "bronze.vendor1_devinfo_raw"
SCHEMA_LOCATION = "/Volumes/main/bronze/pipeline_artifacts/schemas/bronze/vendor1/devinfo"
CHECKPOINT_PATH = "/Volumes/main/bronze/pipeline_artifacts/checkpoints/bronze/vendor1/devinfo"
SECRET_SCOPE = "dataedge-secrets"
TENANT_KEY = "tenant-id"
CLIENT_ID_KEY = "client-id"
CLIENT_SECRET_KEY = "client-secret"

print(f"Starting Bronze ingestion for {SOURCE_FEED}")
print(f"Source path: {SOURCE_PATH}")
print(f"Target table: {TARGET_TABLE}")


def _secret(key):
    return dbutils.secrets.get(SECRET_SCOPE, key)


def _parse_abfss_path(path):
    match = re.match(r"^abfss://([^@]+)@([^.]+)\.dfs\.core\.windows\.net/(.*)$", path.strip())
    if not match:
        raise ValueError(f"SOURCE_PATH must be an abfss:// ADLS Gen2 path, got: {path}")
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
        account_url=f"https://{account_name}.dfs.core.windows.net",
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
        and str(getattr(item, "name", "")).lower().endswith(f".{FILE_FORMAT}")
    ]
    if not source_files:
        raise ValueError(f"No .{FILE_FORMAT} files found under {SOURCE_PATH}")
    return source_files


def _download_file(file_system_client, file_path):
    return file_system_client.get_file_client(file_path).download_file().readall()


def _xml_strings_from_bytes(payload):
    root = ET.fromstring(payload)
    if root.tag.split("}")[-1] == ROW_TAG:
        return [ET.tostring(root, encoding="unicode")]
    rows = [node for node in root.iter() if node.tag.split("}")[-1] == ROW_TAG]
    return [ET.tostring(node, encoding="unicode") for node in rows]


def _safe_col_name(value):
    cleaned = re.sub(r"[^A-Za-z0-9_]+", "_", str(value or ""))
    return cleaned if re.search(r"[A-Za-z0-9]", cleaned) else "unknown"


def _quote_path(path):
    return ".".join(f"`{part}`" for part in path.split("."))


def _flatten_columns(schema, prefix, path):
    columns = []
    for field in schema.fields:
        child_path = f"{path}.{field.name}"
        output_name = _safe_col_name(f"{prefix}_{field.name}")
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
        return F.col(f"`{name}`").cast(target_type).alias(name)
    return F.lit(None).cast(target_type).alias(name)


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
        raise ValueError(f"No JSON records found under {SOURCE_PATH}")
    df = spark.createDataFrame(pd.DataFrame(records))
elif FILE_FORMAT == "xml":
    xml_rows = []
    for item in _source_files:
        xml_rows.extend(_xml_strings_from_bytes(_download_file(fs_client, item.name)))
    if not xml_rows:
        raise ValueError(f"No <{ROW_TAG}> records found under {SOURCE_PATH}")
    xml_df = spark.createDataFrame([(text,) for text in xml_rows], ["_xml"])
    xml_schema = spark.range(1).select(
        F.schema_of_xml(F.lit(xml_rows[0]), {"rowTag": ROW_TAG}).alias("schema")
    ).first()["schema"]
    parsed_df = xml_df.select(F.from_xml(F.col("_xml"), xml_schema, {"rowTag": ROW_TAG}).alias(ROW_TAG))
    flattened_columns = _flatten_columns(parsed_df.schema[ROW_TAG].dataType, ROW_TAG, ROW_TAG)
    df = parsed_df.select(*flattened_columns)
else:
    raise ValueError(f"Unsupported FILE_FORMAT: {FILE_FORMAT}")

if df.limit(1).count() == 0:
    raise ValueError(f"No records found in Bronze source path: {SOURCE_PATH}")

available_columns = set(df.columns)
bronze_df = df.select(
    _project_column(available_columns, "RemoteMessage_BDM_RecFw_version", "string"),
    _project_column(available_columns, "RemoteMessage_BDM_RecTemplates_Template", "string"),
    _project_column(available_columns, "RemoteMessage_BDM_fw_version", "timestamp"),
    _project_column(available_columns, "RemoteMessage_BDM_machineId", "long"),
    _project_column(available_columns, "RemoteMessage_BDM_model_name", "string"),
    _project_column(available_columns, "RemoteMessage_BDM_model_sn", "string"),
    _project_column(available_columns, "RemoteMessage_BDM_stocksConfig_stock_L2", "long"),
    _project_column(available_columns, "RemoteMessage_BDM_stocksConfig_stock_L3", "long"),
    _project_column(available_columns, "RemoteMessage_BDM_stocksConfig_stock_L4B", "long"),
    _project_column(available_columns, "RemoteMessage_BDM_stocksConfig_stock_capacity", "long"),
    _project_column(available_columns, "RemoteMessage_BDM_stocksConfig_stock_denom_curr", "string"),
    _project_column(available_columns, "RemoteMessage_BDM_stocksConfig_stock_denom_rollSize", "string"),
    _project_column(available_columns, "RemoteMessage_BDM_stocksConfig_stock_denom_value", "string"),
    _project_column(available_columns, "RemoteMessage_BDM_stocksConfig_stock_exceedings", "long"),
    _project_column(available_columns, "RemoteMessage_BDM_stocksConfig_stock_id", "long"),
    _project_column(available_columns, "RemoteMessage_BDM_stocksConfig_stock_recycle", "long"),
    _project_column(available_columns, "RemoteMessage_BDM_videoPack_version", "string"),
    _project_column(available_columns, "RemoteMessage_CustomerCode", "string"),
    _project_column(available_columns, "RemoteMessage_Date", "timestamp"),
    _project_column(available_columns, "RemoteMessage_DeviceID", "string"),
    _project_column(available_columns, "RemoteMessage_NOP", "long"),
    _project_column(available_columns, "RemoteMessage_TestMode", "long"),
    _project_column(available_columns, "RemoteMessage_Time", "timestamp"),
    _project_column(available_columns, "RemoteMessage_operation", "string"),
    _project_column(available_columns, "RemoteMessage_os", "string"),
    _project_column(available_columns, "RemoteMessage_software_sw_name", "string"),
    _project_column(available_columns, "RemoteMessage_software_sw_version", "string")
)

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
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")

(
    bronze_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

row_count = spark.table(TARGET_TABLE).count()
print(f"Bronze ingestion completed: {TARGET_TABLE}")
print(f"Total rows now in target: {row_count:,}")


