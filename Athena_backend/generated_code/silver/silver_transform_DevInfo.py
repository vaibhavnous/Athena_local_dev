from pyspark.sql import functions as F

# ============================================================
# Silver Transformation: Vendor1/DevInfo
# ============================================================

SOURCE_TABLE = "bronze.vendor1_devinfo_raw"
TARGET_TABLE = "silver.vendor1_devinfo_clean"

print(f"Starting Silver transformation for {SOURCE_TABLE} -> {TARGET_TABLE}")

# ============================================================
# 1. Read Bronze source table
# ============================================================
source_df = spark.table(SOURCE_TABLE)

if source_df.limit(1).count() == 0:
    raise ValueError(f"Bronze source table is empty: {SOURCE_TABLE}")

print(f"Bronze source rows: {source_df.count():,}")

# ============================================================
# 1b. Incremental filter (read only new Bronze rows)
# ============================================================
try:
    _last_watermark = spark.sql("SELECT MAX(_silver_processed_at) FROM silver.vendor1_devinfo_clean").first()[0]
    if _last_watermark:
        source_df = source_df.filter(F.col("_ingestion_timestamp") > F.lit(_last_watermark))
        print(f"Incremental: reading rows after {_last_watermark}")
except Exception:
    print("No existing Silver table found - full load")

# ============================================================
# 2. Select, rename, and cast columns to business-friendly names
# ============================================================
df = source_df.select(
    F.col("`RemoteMessage_BDM_RecFw`").cast("string").alias("bdm_rec_fw"),
    F.col("`RemoteMessage_BDM_RecFw__version`").cast("string").alias("bdm_rec_fw_version"),
    F.col("`RemoteMessage_BDM_RecTemplates_Template`").cast("string").alias("bdm_rec_templates_template"),
    F.col("`RemoteMessage_BDM_bagsConfig_bag`").cast("string").alias("bdm_bags_config_bag"),
    F.col("`RemoteMessage_BDM_fw`").cast("string").alias("bdm_fw"),
    F.col("`RemoteMessage_BDM_fw__version`").cast("string").alias("bdm_fw_version"),
    F.col("`RemoteMessage_BDM_machineId`").cast("long").alias("bdm_machine_id"),
    F.col("`RemoteMessage_BDM_model`").cast("long").alias("bdm_model"),
    F.col("`RemoteMessage_BDM_model__name`").cast("long").alias("bdm_model_name"),
    F.col("`RemoteMessage_BDM_model__sn`").cast("long").alias("bdm_model_sn"),
    F.col("`RemoteMessage_BDM_stocksConfig_stock`").cast("string").alias("bdm_stocks_config_stock"),
    F.col("`RemoteMessage_BDM_videoPack`").cast("string").alias("bdm_video_pack"),
    F.col("`RemoteMessage_BDM_videoPack__version`").cast("string").alias("bdm_video_pack_version"),
    F.col("`RemoteMessage_CDM`").cast("string").alias("cdm"),
    F.col("`RemoteMessage_DDM`").cast("string").alias("ddm"),
    F.col("`RemoteMessage__CustomerCode`").cast("string").alias("customer_code"),
    F.col("`RemoteMessage__Date`").cast("date").alias("date"),
    F.col("`RemoteMessage__DeviceID`").cast("string").alias("device_id"),
    F.col("`RemoteMessage__NOP`").cast("long").alias("nop"),
    F.col("`RemoteMessage__TestMode`").cast("long").alias("test_mode"),
    F.col("`RemoteMessage__Time`").cast("timestamp").alias("time"),
    F.col("`RemoteMessage__operation`").cast("string").alias("operation"),
    F.col("`RemoteMessage_os`").cast("string").alias("os"),
    F.col("`RemoteMessage_software_sw`").cast("string").alias("software_sw"),
    F.col("`RemoteMessage_software_sw__name`").cast("string").alias("software_sw_name"),
    F.col("`RemoteMessage_software_sw__version`").cast("string").alias("software_sw_version"),
    F.col("`RemoteMessage_Details_countings_counted`").cast("string").alias("details_countings_counted"),
    F.col("`RemoteMessage_contentAfter_count`").cast("string").alias("content_after_count"),
    F.col("`RemoteMessage_contentBefore_count`").cast("string").alias("content_before_count"),
    F.col("`RemoteMessage_destDetails_count`").cast("string").alias("dest_details_count")
)

# ============================================================
# 3. Deduplicate on primary keys (keep latest by ingestion time)
# ============================================================
from pyspark.sql.window import Window

_dedup_window = Window.partitionBy("bdm_machine_id", "bdm_video_pack", "bdm_video_pack_version", "device_id").orderBy(F.col("_ingestion_timestamp").desc())
df = df.withColumn("_row_num", F.row_number().over(_dedup_window)).filter(F.col("_row_num") == 1).drop("_row_num")
print(f"After dedup: {df.count():,} rows")

# ============================================================
# 5. Data quality checks
# ============================================================
_total_rows = df.count()
_null_counts = df.select([F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns]).first()

print(f"Total rows: {_total_rows:,}")
_dq_issues = {col: count for col, count in _null_counts.asDict().items() if count and count > 0}
if _dq_issues:
    print(f"Columns with NULLs: {_dq_issues}")
else:
    print("No NULL values detected.")

# Add Silver audit columns
df = (
    df
    .withColumn("_silver_processed_at", F.current_timestamp())
    .withColumn("_bronze_source_table", F.lit(SOURCE_TABLE))
)

# ============================================================
# 6. Write Silver table (MERGE upsert)
# ============================================================
df.createOrReplaceTempView("_silver_updates")

spark.sql("""CREATE TABLE IF NOT EXISTS silver.vendor1_devinfo_clean
    USING DELTA
    AS SELECT * FROM _silver_updates WHERE 1=0
""")

spark.sql("""MERGE INTO silver.vendor1_devinfo_clean AS target
    USING _silver_updates AS source
    ON target.bdm_machine_id = source.bdm_machine_id AND target.bdm_video_pack = source.bdm_video_pack AND target.bdm_video_pack_version = source.bdm_video_pack_version AND target.device_id = source.device_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

row_count = spark.table("silver.vendor1_devinfo_clean").count()
print(f"Silver transformation completed: silver.vendor1_devinfo_clean")
print(f"Total rows in Silver: {row_count:,}")