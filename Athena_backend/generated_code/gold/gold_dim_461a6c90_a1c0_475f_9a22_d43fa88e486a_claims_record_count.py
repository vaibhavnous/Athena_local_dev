
"""
AUTO-GENERATED GOLD DIMENSION SCRIPT

KPI context: Claims Record Count
Source table: silver.silver_policy_transactions
Expected runtime: Spark / Databricks with Delta support

DO NOT EDIT MANUALLY
"""

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, concat_ws, current_timestamp, lit, sha2, to_timestamp

spark = SparkSession.builder.getOrCreate()

try:
    spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
except Exception:
    print("Could not create schema 'gold' in the current catalog")

SOURCE_TABLE = 'silver.silver_policy_transactions'
DIMENSIONS = [{'entity': 'policy', 'source_table': 'silver.silver_policy_transactions', 'logical_table': 'policy_transactions', 'columns': ['POLICY_TRANSACTION_TYPE', 'SEGMENT_NAME', 'BUSINESS_DIVISION_NAME', 'AGEN_T_CATEGORY_NAME', 'CHANNEL_NAME']}, {'entity': 'product', 'source_table': 'silver.silver_policy_transactions', 'logical_table': 'policy_transactions', 'columns': ['PRODUCT_NAME', 'PRODUCT_GROUP_NAME']}, {'entity': 'agent', 'source_table': 'silver.silver_policy_transactions', 'logical_table': 'policy_transactions', 'columns': ['AGENT_NAME', 'AGENT_SUB_CATEGORY_NAME']}]

if not SOURCE_TABLE or not spark.catalog.tableExists(SOURCE_TABLE):
    raise ValueError(f"Missing dimension source table: {SOURCE_TABLE}")

src = spark.table(SOURCE_TABLE)

def _hash_columns(df, columns):
    expressions = [coalesce(col(name).cast("string"), lit("__NULL__")) for name in columns if name in df.columns]
    if not expressions:
        return sha2(lit("__ALL__"), 256)
    return sha2(concat_ws("||", *expressions), 256)

for dim in DIMENSIONS:
    entity = dim["entity"]
    target_table = "gold.dim_" + entity
    key_column = entity + "_key"
    natural_columns = [name for name in dim.get("columns", []) if name in src.columns]

    if not natural_columns:
        print(f"WARNING: Skipping dimension {target_table} because no source columns are available")
        continue

    staged = src.select(*[col(name) for name in natural_columns]).dropDuplicates()
    staged = (
        staged
        .withColumn("natural_key_hash", _hash_columns(staged, natural_columns))
        .withColumn("attribute_hash", _hash_columns(staged, natural_columns))
        .withColumn(key_column, sha2(col("natural_key_hash"), 256))
        .withColumn("effective_from", current_timestamp())
        .withColumn("effective_to", to_timestamp(lit("9999-12-31 23:59:59")))
        .withColumn("is_current", lit(1))
    )

    if not spark.catalog.tableExists(target_table):
        (
            staged.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target_table)
        )
        print(f"SUCCESS: Created dimension {target_table}")
        continue

    current_dim = spark.table(target_table).filter(col("is_current") == 1)
    changed = (
        staged.alias("s")
        .join(current_dim.alias("d"), col("s.natural_key_hash") == col("d.natural_key_hash"), "left")
        .filter(col("d.natural_key_hash").isNull() | (col("s.attribute_hash") != col("d.attribute_hash")))
        .select("s.*")
    )

    delta_target = DeltaTable.forName(spark, target_table)
    (
        delta_target.alias("d")
        .merge(
            changed.alias("s"),
            "d.natural_key_hash = s.natural_key_hash AND d.is_current = 1 AND d.attribute_hash <> s.attribute_hash",
        )
        .whenMatchedUpdate(set={
            "effective_to": "current_timestamp()",
            "is_current": "0",
        })
        .execute()
    )

    (
        changed.write
        .format("delta")
        .mode("append")
        .saveAsTable(target_table)
    )

    print(f"SUCCESS: SCD2 dimension merge completed for {target_table}")
