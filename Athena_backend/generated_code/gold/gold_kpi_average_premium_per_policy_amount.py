
"""
AUTO-GENERATED GOLD KPI SCRIPT

KPI: Average Premium per Policy Amount
Source table: silver.silver_policy_cover_level_transactions
Target table: gold.fact_average_premium_per_policy_amount
Expected runtime: Spark / Databricks with Delta support

DO NOT EDIT MANUALLY
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, current_timestamp, date_trunc, expr, lit, max, min, sum

spark = SparkSession.builder.getOrCreate()

try:
    spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
except Exception:
    print("Could not create schema 'gold' in the current catalog")

RUN_ID = '9504e06c-3bfb-4b63-8a2f-5f25223b2149'
KPI_NAME = 'Average Premium per Policy Amount'
SOURCE_TABLE = 'silver.silver_policy_cover_level_transactions'
TARGET_TABLE = 'gold.fact_average_premium_per_policy_amount'
VALUE_COLUMN = 'average_premium_per_policy_amount_value'
SILVER_SCHEMA = 'silver'
SOURCE_LOGICAL_TABLE = 'policy_cover_level_transactions'
MEASURE_COLUMN = 'PREMIUM'
MEASURE_AGGREGATION = 'AVG'
DIMENSION_COLUMNS = ['BEGIN_DATE', 'END_DATE', 'POLICY_ISSUED_DATE', 'PaidDate', 'InsertedDate', 'COVER_NAME', 'GEOG_STATE_NAME', 'COVER_GROUP_IDENTIFIER_NAME', 'GEOG_ZONE', 'COVERAGE_CATEGORY']
DIMENSION_SPECS = [{'entity': 'coverage', 'source_table': 'silver.silver_policy_cover_level_transactions', 'logical_table': 'policy_cover_level_transactions', 'columns': ['COVER_NAME', 'COVER_GROUP_IDENTIFIER_NAME', 'COVERAGE_CATEGORY']}, {'entity': 'region', 'source_table': 'silver.silver_policy_cover_level_transactions', 'logical_table': 'policy_cover_level_transactions', 'columns': ['GEOG_STATE_NAME', 'GEOG_ZONE']}]
TIME_COLUMN = 'BEGIN_DATE'
TIME_GRAIN = 'month'
BUSINESS_FILTERS = ['Consistent identifiers across systems', 'No transformations at bronze layer']
JOIN_PATHS = [{'left_table': 'policy_transactions', 'left_column': 'RERERENCE_ID', 'right_table': 'policy_cover_level_transactions', 'right_column': 'RERERENCE_ID', 'join_type': 'INNER', 'cardinality': 'MANY_TO_ONE', 'confidence': 0.8, 'certified': False}, {'left_table': 'policy_cover_level_transactions', 'left_column': 'RERERENCE_ID', 'right_table': 'policy_transactions', 'right_column': 'RERERENCE_ID', 'join_type': 'INNER', 'cardinality': 'MANY_TO_ONE', 'confidence': 0.8, 'certified': False}]

if not spark.catalog.tableExists(SOURCE_TABLE):
    raise ValueError(f"Missing silver source table: {SOURCE_TABLE}")

df = spark.table(SOURCE_TABLE)

if df.limit(1).count() == 0:
    raise ValueError(f"Silver source table has no rows: {SOURCE_TABLE}")

def _silver_table(logical_table):
    return f"{SILVER_SCHEMA}.silver_{logical_table}"

def _sql_like_filter(condition):
    text = str(condition or "").strip()
    if not text or len(text) > 500:
        return False
    return bool(__import__("re").search(r"(=|<>|!=|>=|<=|>|<|\bIN\b|\bLIKE\b|\bIS\b)", text, __import__("re").IGNORECASE))

for condition in BUSINESS_FILTERS:
    if _sql_like_filter(condition):
        df = df.filter(expr(str(condition)))
    else:
        print(f"WARNING: Skipping non-SQL business filter: {condition}")

joined_logical_tables = {SOURCE_LOGICAL_TABLE}
for index, path in enumerate(JOIN_PATHS):
    left_table = str(path.get("left_table") or "")
    right_table = str(path.get("right_table") or "")
    left_column = str(path.get("left_column") or "")
    right_column = str(path.get("right_column") or "")
    join_type = str(path.get("join_type") or "left").lower()
    if join_type == "inner" and not path.get("certified"):
        join_type = "left"

    if not left_table or not right_table or not left_column or not right_column:
        continue

    if left_table in joined_logical_tables and right_table not in joined_logical_tables:
        other_table = right_table
        base_column = left_column
        other_column = right_column
    elif right_table in joined_logical_tables and left_table not in joined_logical_tables:
        other_table = left_table
        base_column = right_column
        other_column = left_column
    else:
        continue

    other_silver_table = _silver_table(other_table)
    if not spark.catalog.tableExists(other_silver_table):
        print(f"WARNING: Missing join-path table: {other_silver_table}")
        continue
    if base_column not in df.columns:
        print(f"WARNING: Missing join-path base column: {base_column}")
        continue

    other_df = spark.table(other_silver_table)
    if other_column not in other_df.columns:
        print(f"WARNING: Missing join-path other column: {other_column} in {other_silver_table}")
        continue
    rename_map = {
        name: f"{other_table}__{name}"
        for name in other_df.columns
        if name in df.columns and name != other_column
    }
    for old_name, new_name in rename_map.items():
        other_df = other_df.withColumnRenamed(old_name, new_name)
    df = df.join(other_df, df[base_column] == other_df[other_column], join_type)
    joined_logical_tables.add(other_table)

available_columns = set(df.columns)
missing_dimensions = [name for name in DIMENSION_COLUMNS if name not in available_columns]
if missing_dimensions:
    print(f"WARNING: Dropping missing gold dimensions: {missing_dimensions}")

group_columns = []
dimension_raw_columns = set()
for dim in DIMENSION_SPECS:
    entity = dim["entity"]
    target_dim_table = "gold.dim_" + entity
    key_column = entity + "_key"
    natural_columns = [name for name in dim.get("columns", []) if name in df.columns]
    if not natural_columns:
        continue
    dimension_raw_columns.update(natural_columns)
    if spark.catalog.tableExists(target_dim_table):
        dim_df = spark.table(target_dim_table).filter(col("is_current") == 1)
        join_columns = [name for name in natural_columns if name in dim_df.columns]
        if join_columns and key_column in dim_df.columns:
            df = df.join(dim_df.select(*join_columns, key_column), join_columns, "left")
            group_columns.append(col(key_column))
        else:
            print(f"WARNING: Dimension {target_dim_table} is missing required natural/key columns")
    else:
        print(f"WARNING: Dimension table {target_dim_table} does not exist; using raw attributes")
        group_columns.extend([col(name) for name in natural_columns])

group_columns.extend([
    col(name)
    for name in DIMENSION_COLUMNS
    if name in set(df.columns) and name not in dimension_raw_columns
])

if TIME_COLUMN and TIME_COLUMN in available_columns:
    group_columns.append(date_trunc('month', col('BEGIN_DATE')).alias('period_start'))
elif TIME_COLUMN:
    print(f"WARNING: Gold time column '{TIME_COLUMN}' is missing from {SOURCE_TABLE}")

if MEASURE_AGGREGATION != "COUNT" and MEASURE_COLUMN not in available_columns:
    raise ValueError(f"Gold measure column '{MEASURE_COLUMN}' is missing from {SOURCE_TABLE}")

agg_expr = avg(col('PREMIUM')).alias('average_premium_per_policy_amount_value')

if group_columns:
    result = df.groupBy(*group_columns).agg(agg_expr)
else:
    result = df.agg(agg_expr)

result = (
    result
    .withColumn("kpi_name", lit(KPI_NAME))
    .withColumn("gold_run_id", lit(RUN_ID))
    .withColumn("gold_processed_timestamp", current_timestamp())
)

if spark.catalog.tableExists(TARGET_TABLE):
    writer = result.write.format("delta").mode("append")
else:
    writer = result.write.format("delta").mode("overwrite").option("overwriteSchema", "true")

if "period_start" in result.columns:
    writer = writer.partitionBy("period_start")

writer.saveAsTable(TARGET_TABLE)

print(f"SUCCESS: Gold KPI generation completed for {TARGET_TABLE}")
