from pyspark.sql import functions as F

# ============================================================
# Silver Transformation: Vendor1/Deposit
# ============================================================

SOURCE_TABLE = "bronze.vendor1_deposit_raw"
TARGET_TABLE = "silver.vendor1_deposit_clean"

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
    _last_watermark = spark.sql("SELECT MAX(_silver_processed_at) FROM silver.vendor1_deposit_clean").first()[0]
    if _last_watermark:
        source_df = source_df.filter(F.col("_ingestion_timestamp") > F.lit(_last_watermark))
        print(f"Incremental: reading rows after {_last_watermark}")
except Exception:
    print("No existing Silver table found - full load")

# ============================================================
# 2. Select, rename, and cast columns to business-friendly names
# ============================================================
df = source_df.select(
    F.col("`RemoteMessage_Conciliation`").cast("string").alias("conciliation"),
    F.col("`RemoteMessage_Details__Currency`").cast("string").alias("details_currency"),
    F.col("`RemoteMessage_Details_countings__valid`").cast("long").alias("details_countings_valid"),
    F.col("`RemoteMessage_Details_countings_counted`").cast("string").alias("details_countings_counted"),
    F.col("`RemoteMessage_Details_total`").cast("long").alias("details_total"),
    F.col("`RemoteMessage_L2Countings`").cast("string").alias("l2_countings"),
    F.col("`RemoteMessage_L3Countings`").cast("string").alias("l3_countings"),
    F.col("`RemoteMessage_L4bCountings`").cast("string").alias("l4b_countings"),
    F.col("`RemoteMessage_TransactionID`").cast("long").alias("transaction_id"),
    F.col("`RemoteMessage_User`").cast("long").alias("user"),
    F.col("`RemoteMessage_UserAlternateID`").cast("string").alias("user_alternate_id"),
    F.col("`RemoteMessage_UserLevel`").cast("long").alias("user_level"),
    F.col("`RemoteMessage_UserName`").cast("string").alias("user_name"),
    F.col("`RemoteMessage__CustomerCode`").cast("string").alias("customer_code"),
    F.col("`RemoteMessage__Date`").cast("date").alias("date"),
    F.col("`RemoteMessage__DeviceID`").cast("string").alias("device_id"),
    F.col("`RemoteMessage__NOP`").cast("long").alias("nop"),
    F.col("`RemoteMessage__TestMode`").cast("long").alias("test_mode"),
    F.col("`RemoteMessage__Time`").cast("timestamp").alias("time"),
    F.col("`RemoteMessage__operation`").cast("string").alias("operation"),
    F.col("`RemoteMessage_accountingDate`").cast("date").alias("accounting_date"),
    F.col("`RemoteMessage_authenticationMode`").cast("string").alias("authentication_mode"),
    F.col("`RemoteMessage_cheques`").cast("string").alias("cheques"),
    F.col("`RemoteMessage_contentAfter_count`").cast("string").alias("content_after_count"),
    F.col("`RemoteMessage_contentBefore_count`").cast("string").alias("content_before_count"),
    F.col("`RemoteMessage_destDetails_count`").cast("string").alias("dest_details_count"),
    F.col("`RemoteMessage_endShiftDeposit`").cast("long").alias("end_shift_deposit"),
    F.col("`RemoteMessage_groupID`").cast("long").alias("group_id"),
    F.col("`RemoteMessage_groupName`").cast("string").alias("group_name"),
    F.col("`RemoteMessage_isKit`").cast("long").alias("is_kit"),
    F.col("`RemoteMessage_offlineMachines`").cast("string").alias("offline_machines"),
    F.col("`RemoteMessage_tickets`").cast("string").alias("tickets")
)

# ============================================================
# 3. Deduplicate on primary keys (keep latest by ingestion time)
# ============================================================
from pyspark.sql.window import Window

_dedup_window = Window.partitionBy("details_countings_valid", "transaction_id", "user_alternate_id", "device_id", "group_id").orderBy(F.col("_ingestion_timestamp").desc())
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

spark.sql("""CREATE TABLE IF NOT EXISTS silver.vendor1_deposit_clean
    USING DELTA
    AS SELECT * FROM _silver_updates WHERE 1=0
""")

spark.sql("""MERGE INTO silver.vendor1_deposit_clean AS target
    USING _silver_updates AS source
    ON target.details_countings_valid = source.details_countings_valid AND target.transaction_id = source.transaction_id AND target.user_alternate_id = source.user_alternate_id AND target.device_id = source.device_id AND target.group_id = source.group_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

row_count = spark.table("silver.vendor1_deposit_clean").count()
print(f"Silver transformation completed: silver.vendor1_deposit_clean")
print(f"Total rows in Silver: {row_count:,}")