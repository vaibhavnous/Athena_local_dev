from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

RUN_ID = "538e43c8-8174-495b-b706-ccc3647dcaa6"
PIPELINE_VERSION = "v1"
SOURCE_TYPE = "adls_gen2"
SOURCE_FEED = "Vendor1/DevInfo"
FILE_FORMAT = "xml"
ROW_TAG = "RemoteMessage"
SOURCE_PATH = "abfss://athena@atheastorage.dfs.core.windows.net/evention/vendor1/machine1/DevInfo/"
TARGET_TABLE = "bronze.vendor1_devinfo_raw"
SCHEMA_LOCATION = "dbfs:/pipelines/schemas/bronze/vendor1/devinfo"
CHECKPOINT_PATH = "dbfs:/pipelines/checkpoints/bronze/vendor1/devinfo"

print(f"Starting Bronze ingestion for {SOURCE_FEED}")
print(f"Source path: {SOURCE_PATH}")
print(f"Target table: {TARGET_TABLE}")

if FILE_FORMAT == "csv":
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .csv(SOURCE_PATH)
    )
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
    df = (
        spark.read
        .format("xml")
        .option("rowTag", ROW_TAG)
        .option("mode", "PERMISSIVE")
        .load(SOURCE_PATH)
    )
else:
    raise ValueError(f"Unsupported FILE_FORMAT: {FILE_FORMAT}")

if df.limit(1).count() == 0:
    raise ValueError(f"No records found in Bronze source path: {SOURCE_PATH}")

bronze_df = df.select(
    F.col("RemoteMessage_BDM_RecFw_version").cast("string").alias("RemoteMessage_BDM_RecFw_version"),
    F.col("RemoteMessage_BDM_RecTemplates_Template").cast("string").alias("RemoteMessage_BDM_RecTemplates_Template"),
    F.col("RemoteMessage_BDM_fw_version").cast("timestamp").alias("RemoteMessage_BDM_fw_version"),
    F.col("RemoteMessage_BDM_machineId").cast("boolean").alias("RemoteMessage_BDM_machineId"),
    F.col("RemoteMessage_BDM_model_name").cast("string").alias("RemoteMessage_BDM_model_name"),
    F.col("RemoteMessage_BDM_model_sn").cast("string").alias("RemoteMessage_BDM_model_sn"),
    F.col("RemoteMessage_BDM_stocksConfig_stock_L2").cast("boolean").alias("RemoteMessage_BDM_stocksConfig_stock_L2"),
    F.col("RemoteMessage_BDM_stocksConfig_stock_L3").cast("boolean").alias("RemoteMessage_BDM_stocksConfig_stock_L3"),
    F.col("RemoteMessage_BDM_stocksConfig_stock_L4B").cast("boolean").alias("RemoteMessage_BDM_stocksConfig_stock_L4B"),
    F.col("RemoteMessage_BDM_stocksConfig_stock_capacity").cast("int").alias("RemoteMessage_BDM_stocksConfig_stock_capacity"),
    F.col("RemoteMessage_BDM_stocksConfig_stock_denom_curr").cast("string").alias("RemoteMessage_BDM_stocksConfig_stock_denom_curr"),
    F.col("RemoteMessage_BDM_stocksConfig_stock_denom_rollSize").cast("string").alias("RemoteMessage_BDM_stocksConfig_stock_denom_rollSize"),
    F.col("RemoteMessage_BDM_stocksConfig_stock_denom_value").cast("string").alias("RemoteMessage_BDM_stocksConfig_stock_denom_value"),
    F.col("RemoteMessage_BDM_stocksConfig_stock_exceedings").cast("boolean").alias("RemoteMessage_BDM_stocksConfig_stock_exceedings"),
    F.col("RemoteMessage_BDM_stocksConfig_stock_id").cast("int").alias("RemoteMessage_BDM_stocksConfig_stock_id"),
    F.col("RemoteMessage_BDM_stocksConfig_stock_recycle").cast("boolean").alias("RemoteMessage_BDM_stocksConfig_stock_recycle"),
    F.col("RemoteMessage_BDM_videoPack_version").cast("string").alias("RemoteMessage_BDM_videoPack_version"),
    F.col("RemoteMessage_CustomerCode").cast("string").alias("RemoteMessage_CustomerCode"),
    F.col("RemoteMessage_Date").cast("timestamp").alias("RemoteMessage_Date"),
    F.col("RemoteMessage_DeviceID").cast("string").alias("RemoteMessage_DeviceID"),
    F.col("RemoteMessage_NOP").cast("int").alias("RemoteMessage_NOP"),
    F.col("RemoteMessage_TestMode").cast("boolean").alias("RemoteMessage_TestMode"),
    F.col("RemoteMessage_Time").cast("timestamp").alias("RemoteMessage_Time"),
    F.col("RemoteMessage_operation").cast("string").alias("RemoteMessage_operation"),
    F.col("RemoteMessage_os").cast("string").alias("RemoteMessage_os"),
    F.col("RemoteMessage_software_sw_name").cast("string").alias("RemoteMessage_software_sw_name"),
    F.col("RemoteMessage_software_sw_version").cast("string").alias("RemoteMessage_software_sw_version")
)

bronze_df = (
    bronze_df
    .withColumn("_run_id", F.lit(RUN_ID))
    .withColumn("_ingestion_timestamp", F.current_timestamp())
    .withColumn("_source_system", F.lit(SOURCE_TYPE))
    .withColumn("_source_feed", F.lit(SOURCE_FEED))
    .withColumn("_source_file_path", F.input_file_name())
    .withColumn("_source_file_name", F.element_at(F.split(F.input_file_name(), "/"), -1))
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

# Optional Auto Loader pattern for production:
# (
#     spark.readStream
#     .format("cloudFiles")
#     .option("cloudFiles.format", FILE_FORMAT)
#     .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
#     .load(SOURCE_PATH)
#     .withColumn("_run_id", F.lit(RUN_ID))
#     .withColumn("_ingestion_timestamp", F.current_timestamp())
#     .withColumn("_source_system", F.lit(SOURCE_TYPE))
#     .withColumn("_source_feed", F.lit(SOURCE_FEED))
#     .withColumn("_source_file_path", F.input_file_name())
#     .writeStream
#     .format("delta")
#     .option("checkpointLocation", CHECKPOINT_PATH)
#     .trigger(availableNow=True)
#     .toTable(TARGET_TABLE)
# )


