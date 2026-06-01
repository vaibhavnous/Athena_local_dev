
"""
AUTO-GENERATED SILVER TRANSFORMATION SCRIPT

Source table: bronze.bronze_claim_payment_indemnity
Target table: silver.silver_claim_payment_indemnity
Expected runtime: Spark / Databricks with Delta support

POC rule: generated bronze scripts are treated as proof that bronze tables exist.
Runtime checks below still fail clearly if the Databricks table is missing.

DO NOT EDIT MANUALLY
"""

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, concat_ws, current_timestamp, lit, sha2, trim, when

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

try:
    spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
except Exception:
    print("Could not create schema 'silver' in the current catalog")

RUN_ID = "3d8b1e79-22a2-436e-ac3f-937e63e85a73"
SOURCE_TABLE = "bronze.bronze_claim_payment_indemnity"
TARGET_TABLE = "silver.silver_claim_payment_indemnity"
TEMP_VIEW = "silver_src_claim_payment_indemnity"

EXPECTED_COLUMNS = ['paymentid', 'updatenum', 'paiddate', 'paidamount', 'servicetax', 'claimid', 'payeeid', 'payeename', 'payeetype', 'serviceproviderid', 'serviceprovidername', 'serviceprovidertypename', 'paymentmodeid', 'paymentmodename', 'surveytype', 'garageid', 'garagename', 'gcgaragecity', 'garagetypeid', 'garagetypename', 'hospitalid', 'hospitalname', 'hospitaltype']
STRING_COLUMNS = ['payeename', 'payeetype', 'serviceprovidername', 'serviceprovidertypename', 'paymentmodeid', 'paymentmodename', 'surveytype', 'garagename', 'gcgaragecity', 'garagetypename', 'hospitalname', 'hospitaltype']
PII_COLUMNS = []
KEY_COLUMNS = []
CAST_RULES = {'paymentid': 'bigint', 'updatenum': 'int', 'paiddate': 'date', 'paidamount': 'decimal(38,10)', 'servicetax': 'decimal(38,10)', 'claimid': 'bigint', 'payeeid': 'bigint', 'serviceproviderid': 'bigint', 'garageid': 'bigint', 'garagetypeid': 'bigint', 'hospitalid': 'bigint'}
COLUMN_ALIASES = {}

if not spark.catalog.tableExists(SOURCE_TABLE):
    raise ValueError(f"Missing bronze source table: {SOURCE_TABLE}")

df = spark.table(SOURCE_TABLE)

if df.limit(1).count() == 0:
    raise ValueError(f"Bronze source table has no rows: {SOURCE_TABLE}")

available_columns = set(df.columns)
for old_name, new_name in COLUMN_ALIASES.items():
    if old_name in available_columns and new_name not in available_columns:
        df = df.withColumnRenamed(old_name, new_name)

available_columns = set(df.columns)
metadata_columns = [
    name for name in ["run_id", "ingestion_timestamp", "source_system", "source_table"]
    if name in available_columns
]

def compact_name(name):
    return str(name).lower().replace("_", "")

available_by_compact = {
    compact_name(name): name
    for name in df.columns
}

if EXPECTED_COLUMNS:
    select_expressions = []
    missing_columns = []
    for expected_name in EXPECTED_COLUMNS:
        actual_name = available_by_compact.get(compact_name(expected_name))
        if actual_name:
            select_expressions.append(col(actual_name).alias(expected_name))
        else:
            missing_columns.append(expected_name)
else:
    select_expressions = [
        col(name)
        for name in df.columns
        if name not in metadata_columns
    ]
    missing_columns = []

if not select_expressions:
    raise ValueError(
        f"No expected business columns found in {SOURCE_TABLE}. "
        f"Available columns: {df.columns}"
    )

metadata_expressions = [col(name) for name in metadata_columns]
df = df.select(*select_expressions, *metadata_expressions)

if missing_columns:
    print(f"WARNING: Missing expected columns in {SOURCE_TABLE}: {missing_columns}")

for column_name in STRING_COLUMNS:
    if column_name in df.columns:
        df = df.withColumn(
            column_name,
            when(trim(col(column_name)) == "", None).otherwise(trim(col(column_name)))
        )

for column_name, target_type in CAST_RULES.items():
    if column_name in df.columns:
        df = df.withColumn(column_name, col(column_name).cast(target_type))

for column_name in PII_COLUMNS:
    if column_name in df.columns:
        df = df.withColumn(column_name, col(column_name).cast("string"))

dedup_keys = [column_name for column_name in KEY_COLUMNS if column_name in df.columns]
business_columns = [
    name for name in df.columns
    if name not in metadata_columns
]
hash_columns = dedup_keys or business_columns
if not hash_columns:
    raise ValueError(f"No columns available to build Silver upsert key for {TARGET_TABLE}")

df = df.withColumn(
    "silver_upsert_key",
    sha2(
        concat_ws(
            "||",
            *[coalesce(col(name).cast("string"), lit("__NULL__")) for name in hash_columns]
        ),
        256,
    ),
)

if dedup_keys:
    df = df.dropDuplicates(["silver_upsert_key"])
else:
    df = df.dropDuplicates(["silver_upsert_key"])

df = (
    df
    .withColumn("silver_run_id", lit(RUN_ID))
    .withColumn("silver_processed_timestamp", current_timestamp())
)

df.createOrReplaceTempView(TEMP_VIEW)

create_table_sql = (
    f"CREATE TABLE IF NOT EXISTS {TARGET_TABLE} "
    f"USING DELTA "
    f"AS SELECT * FROM {TEMP_VIEW} WHERE 1 = 0"
)
spark.sql(create_table_sql)

target_columns = set(spark.table(TARGET_TABLE).columns)
if "silver_upsert_key" not in target_columns:
    spark.sql(f"ALTER TABLE {TARGET_TABLE} ADD COLUMNS (silver_upsert_key STRING)")

delta_target = DeltaTable.forName(spark, TARGET_TABLE)
(
    delta_target.alias("target")
    .merge(
        df.alias("source"),
        "target.silver_upsert_key = source.silver_upsert_key",
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print(f"SUCCESS: Silver upsert completed for {TARGET_TABLE}")
