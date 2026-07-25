"""
Silver Code Generation (POC MODE)

Generates standalone Databricks/Spark scripts from generated bronze metadata and
semantic enrichment. In demo mode, generated bronze scripts are treated as proof
that bronze tables exist.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, TypedDict

from state import Stage01State
from utilis.db import ai_store_db_writer
from utilis.generated_code_paths import generated_code_dir
from utilis.logger import logger


SILVER_MAX_WORKERS = int(os.environ.get("SILVER_MAX_WORKERS", "4"))
SILVER_LLM_ENV_KEYS = ("ATHENA_SILVER_USE_LLM", "USE_LLM")
KIMBALL_LLM_ENV_KEYS = ("ATHENA_GOLD_KIMBALL_PLAN_USE_LLM", "ATHENA_GOLD_USE_LLM", "USE_LLM")
DEFAULT_MAX_GOLD_DIMENSION_TABLES = 2


class SilverTableRef(TypedDict):
    database_name: str
    schema_name: str
    table_name: str
    bronze_table: str
    silver_table: str
    existing_script_path: str | None
    source_columns: List[Dict[str, Any]]


def _silver_output_dir() -> str:
    return str(generated_code_dir("silver"))


def _silver_output_dir_for(target_warehouse: str = "databricks") -> str:
    warehouse = str(target_warehouse or "databricks").lower()
    if warehouse == "snowflake":
        return str(generated_code_dir("snowflake", "silver"))
    return _silver_output_dir()


def _run_slug(run_id: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]+", "_", str(run_id or "run")).strip("_")[:48] or "run"


def _file_slug(value: str, max_length: int = 64) -> str:
    slug = re.sub(r"[^a-zA-Z0-9_]+", "_", str(value or "table")).strip("_") or "table"
    if len(slug) <= max_length:
        return slug
    digest = hashlib.sha1(slug.encode("utf-8")).hexdigest()[:8]
    return f"{slug[: max_length - 9].rstrip('_')}_{digest}"


def _gold_output_dir() -> str:
    return str(generated_code_dir("gold"))


def _bronze_bundle_path(target_warehouse: str = "databricks") -> str:
    if str(target_warehouse or "").lower() == "snowflake":
        snowflake_path = str(generated_code_dir("snowflake", "bronze", "bronze_scripts.json"))
        if os.path.exists(snowflake_path):
            return snowflake_path
    return str(generated_code_dir("bronze", "bronze_scripts.json"))


def _silver_readme_path(target_warehouse: str = "databricks") -> str:
    return os.path.join(_silver_output_dir_for(target_warehouse), "README.md")


def _silver_ui_path(target_warehouse: str = "databricks") -> str:
    return os.path.join(_silver_output_dir_for(target_warehouse), "index.html")


def _validate_python(code: str) -> None:
    compile(code, "<silver_generated>", "exec")


def _llm_enabled_for_silver() -> bool:
    return any(str(os.getenv(key, "")).lower() in {"1", "true", "yes", "on"} for key in SILVER_LLM_ENV_KEYS)


def _llm_enabled_for_kimball_plan() -> bool:
    return any(str(os.getenv(key, "")).lower() in {"1", "true", "yes", "on"} for key in KIMBALL_LLM_ENV_KEYS)


def _llm_generate_silver_code(
    *,
    table_ref: SilverTableRef,
    enriched_columns: List[Dict[str, Any]],
    deterministic_code: str,
    target_warehouse: str,
    validation_feedback: str = "",
) -> str:
    from nodes.req_extraction import get_llm

    language = "Snowflake SQL" if str(target_warehouse).lower() == "snowflake" else "Databricks PySpark"
    canonical_source_columns = sorted(
        {
            str(item.get("source_column_name") or item.get("column_name") or "").strip()
            for item in table_ref.get("source_columns") or []
            if isinstance(item, dict) and str(item.get("source_column_name") or item.get("column_name") or "").strip()
        }
        | {"run_id", "ingestion_timestamp", "source_system", "source_table"}
    )
    retry_context = (
        f"\nA previous candidate was rejected by the hard validator for this reason:\n{validation_feedback}\n"
        "Correct that exact violation without changing transformation semantics.\n"
        if validation_feedback
        else ""
    )
    prompt = f"""Generate production {language} Silver transformation code.
Return only executable code. Preserve the source/target tables, audit columns,
type normalization, deduplication, and merge/upsert behavior from the baseline.
Do not invent columns or change the target table.
For quoted src column references, use only the exact case-sensitive identifiers
in Canonical source columns. Copy identifiers exactly; do not restore source-system casing.
For Snowflake temporal conversion, use TRY_TO_DATE or TRY_TO_TIMESTAMP_NTZ over
TO_VARCHAR(source); never TRY_CAST a DATE/TIMESTAMP expression to another temporal type.
{retry_context}

Source table: {table_ref['bronze_table']}
Target table: {table_ref['silver_table']}
Canonical source columns: {json.dumps(canonical_source_columns)}
Reviewed columns: {json.dumps(enriched_columns, default=str)}

BASELINE:
{deterministic_code}
""".strip()
    llm = get_llm(
        provider=os.getenv("ATHENA_SILVER_LLM_PROVIDER", os.getenv("ATHENA_LLM_PROVIDER", "azure_openai")),
        model=os.getenv("ATHENA_SILVER_LLM_MODEL"),
        temperature=0.0,
    )
    response = llm.invoke(prompt)
    content = getattr(response, "content", response)
    text = str(content).strip()
    match = re.search(r"```(?:python|sql)?\s*(.*?)```", text, flags=re.IGNORECASE | re.DOTALL)
    return match.group(1).strip() if match else text


def _canonicalize_snowflake_source_identifiers(code: str, table_ref: SilverTableRef) -> str:
    canonical = {
        str(item.get("source_column_name") or item.get("column_name") or "").strip()
        for item in table_ref.get("source_columns") or []
        if isinstance(item, dict) and str(item.get("source_column_name") or item.get("column_name") or "").strip()
    }
    canonical.update({"run_id", "ingestion_timestamp", "source_system", "source_table"})
    by_casefold = {column.casefold(): column for column in canonical}

    def replace(match: re.Match[str]) -> str:
        identifier = match.group(1).replace('""', '"')
        replacement = by_casefold.get(identifier.casefold())
        if replacement is None:
            return match.group(0)
        return f'src."{replacement.replace(chr(34), chr(34) * 2)}"'

    return re.sub(r'\bsrc\s*\.\s*"((?:""|[^"])*)"', replace, str(code or ""), flags=re.IGNORECASE)


def _canonicalize_snowflake_temporal_conversions(code: str) -> str:
    """Make direct source temporal parsing explicit so Snowflake never coerces DATE values implicitly."""
    return re.sub(
        r'\b(TRY_TO_(?:DATE|TIMESTAMP_NTZ))\s*\(\s*(src\s*\.\s*"(?:""|[^"])*")\s*\)',
        r'\1(TO_VARCHAR(\2))',
        str(code or ""),
        flags=re.IGNORECASE,
    )


def _sql_without_comments(code: str) -> str:
    return re.sub(r"--[^\n]*|/\*.*?\*/", "", str(code or ""), flags=re.DOTALL)


def _require_snowflake_silver_structure(code: str, table_ref: SilverTableRef) -> None:
    source = _snowflake_qualified_name(*str(table_ref["bronze_table"]).split("."))
    target = _snowflake_qualified_name(*str(table_ref["silver_table"]).split("."))
    sql = _sql_without_comments(code)
    if not re.search(rf"(?:^|\s)FROM\s+{re.escape(source)}\s+(?:AS\s+)?src\b", sql, re.IGNORECASE):
        raise ValueError("LLM Silver SQL must read the approved Bronze table as src")
    if not re.search(rf"\bMERGE\s+INTO\s+{re.escape(target)}\s+(?:AS\s+)?target\b", sql, re.IGNORECASE):
        raise ValueError("LLM Silver SQL must merge into the approved Silver table as target")
    forbidden = re.search(r"\b(DROP|TRUNCATE|DELETE|COPY|CALL|GRANT|REVOKE|USE|EXECUTE\s+IMMEDIATE)\b", sql, re.IGNORECASE)
    if forbidden:
        raise ValueError(f"LLM Silver SQL contains forbidden statement: {forbidden.group(1).upper()}")


def _validate_generated_silver_code(
    code: str,
    *,
    table_ref: SilverTableRef,
    enriched_columns: List[Dict[str, Any]],
    target_warehouse: str,
) -> None:
    normalized = str(code or "").lower()
    normalized_identifiers = normalized.replace('"', "")
    source_table = str(table_ref["bronze_table"]).lower()
    target_table = str(table_ref["silver_table"]).lower()
    if source_table not in normalized_identifiers or target_table not in normalized_identifiers:
        raise ValueError("LLM Silver code changed the approved source or target table")

    required_columns = {_normalized_column_name(column) for column in enriched_columns if _normalized_column_name(column)}
    required_columns.update({
        "run_id",
        "ingestion_timestamp",
        "source_system",
        "source_table",
        "silver_upsert_key",
        "silver_run_id",
        "silver_processed_timestamp",
    })
    missing = [column for column in sorted(required_columns) if column not in normalized]
    if missing:
        raise ValueError(f"LLM Silver code dropped required columns: {', '.join(missing[:10])}")

    if str(target_warehouse or "").lower() == "snowflake":
        upper = normalized.upper()
        if "CREATE TABLE" not in upper or "MERGE INTO" not in upper:
            raise ValueError("LLM Silver SQL must contain CREATE TABLE and MERGE INTO")
        if re.search(r"TRY_CAST\s*\([^)]*\bAS\s+(?:DATE|TIMESTAMP(?:_NTZ)?)(?:\s*\([^)]*\))?\s*\)", code, re.IGNORECASE) or re.search(
            r"TRY_TO_(?:DATE|TIMESTAMP_NTZ)\s*\(\s*(?:src\s*\.|GET_IGNORE_CASE)",
            code,
            re.IGNORECASE,
        ):
            raise ValueError(
                "LLM Silver SQL used unsafe Snowflake temporal conversion; use TRY_TO_DATE/TRY_TO_TIMESTAMP_NTZ over TO_VARCHAR"
            )
        _require_snowflake_silver_structure(code, table_ref)
        physical_source_columns = {
            str(item.get("source_column_name") or item.get("column_name") or "").strip()
            for item in table_ref.get("source_columns") or []
            if isinstance(item, dict) and str(item.get("source_column_name") or item.get("column_name") or "").strip()
        }
        physical_source_columns.update({"run_id", "ingestion_timestamp", "source_system", "source_table"})
        if physical_source_columns:
            quoted_source_columns = {
                match.group(1).replace('""', '"')
                for match in re.finditer(r'\bsrc\s*\.\s*"((?:""|[^"])*)"', str(code), flags=re.IGNORECASE)
            }
            invalid_source_columns = sorted(quoted_source_columns - physical_source_columns)
            if invalid_source_columns:
                raise ValueError(
                    "LLM Silver SQL used source identifiers that do not match the Bronze contract: "
                    + ", ".join(invalid_source_columns[:10])
                )
    else:
        _validate_python(code)


def _load_bronze_bundle(target_warehouse: str = "databricks") -> Dict[str, Any]:
    path = _bronze_bundle_path(target_warehouse)
    if not os.path.exists(path):
        return {"scripts": []}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _existing_silver_script_refs(silver_schema: str) -> List[SilverTableRef]:
    output_dir = _silver_output_dir()
    if not os.path.isdir(output_dir):
        return []

    refs: List[SilverTableRef] = []
    for file_name in sorted(os.listdir(output_dir)):
        match = re.fullmatch(r"silver_transform_(.+)\.py", file_name)
        if not match:
            continue
        table_name = match.group(1)
        refs.append(
            {
                "database_name": "unknown",
                "schema_name": "unknown",
                "table_name": table_name,
                "bronze_table": f"bronze.bronze_{table_name}",
                "silver_table": f"{silver_schema}.silver_{table_name}",
                "existing_script_path": os.path.join(output_dir, file_name),
                "source_columns": [],
            }
        )
    return refs


def _table_name_from_ref(item: Dict[str, Any]) -> str:
    return str(item.get("table") or item.get("table_name") or "").strip()


def _snowflake_quote_identifier(value: str) -> str:
    cleaned = str(value or "").strip()
    if not cleaned:
        raise ValueError("Snowflake identifier cannot be empty.")
    return '"' + cleaned.replace('"', '""') + '"'


def _snowflake_qualified_name(*parts: str) -> str:
    return ".".join(_snowflake_quote_identifier(part) for part in parts if str(part or "").strip())


def _snowflake_string_literal(value: str) -> str:
    return "'" + str(value or "").replace("'", "''") + "'"


def _snowflake_bronze_catalog() -> str:
    return str(os.getenv("SNOWFLAKE_BRONZE_CATALOG") or "ATHENA_DB").strip() or "ATHENA_DB"


def _snowflake_bronze_schema() -> str:
    return str(os.getenv("SNOWFLAKE_BRONZE_SCHEMA") or "BRONZE").strip() or "BRONZE"


def _snowflake_silver_catalog() -> str:
    return str(os.getenv("SNOWFLAKE_SILVER_CATALOG") or _snowflake_bronze_catalog()).strip() or "ATHENA_DB"


def _snowflake_silver_schema() -> str:
    return str(os.getenv("SNOWFLAKE_SILVER_SCHEMA") or "SILVER").strip() or "SILVER"


def _resolve_tables_for_silver(state: Stage01State) -> List[SilverTableRef]:
    target_warehouse = str(state.get("target_warehouse") or "databricks").lower()
    bronze_results = list(state.get("bronze_generation_results") or [])
    if not bronze_results:
        bronze_results.extend(_load_bronze_bundle(target_warehouse).get("scripts", []))
    if not bronze_results:
        bronze_results.extend(state.get("certified_tables") or [])
        discovered = state.get("discovered_metadata") or {}
        if isinstance(discovered, dict):
            bronze_results.extend(discovered.get("tables", []) or [])

    bronze_catalog = str(state.get("bronze_catalog") or "main")
    bronze_schema = str(state.get("bronze_schema") or "bronze")
    silver_catalog = str(state.get("silver_catalog") or bronze_catalog)
    silver_schema = str(state.get("silver_schema") or "silver")
    if target_warehouse == "snowflake":
        bronze_catalog = _snowflake_bronze_catalog()
        bronze_schema = _snowflake_bronze_schema()
        silver_catalog = _snowflake_silver_catalog()
        silver_schema = _snowflake_silver_schema()
    resolved_by_table: Dict[str, SilverTableRef] = {}

    for item in bronze_results:
        if not isinstance(item, dict):
            continue
        table_name = _table_name_from_ref(item)
        if not table_name:
            continue
        extension = "sql" if target_warehouse == "snowflake" else "py"
        script_path = os.path.join(
            _silver_output_dir_for(target_warehouse),
            f"silver_transform_{_run_slug(str(state.get('run_id') or 'run'))}_{_file_slug(table_name)}.{extension}",
        )
        bronze_table = (
            str(item.get("target_table") or "").strip()
            if target_warehouse == "snowflake" and item.get("target_table")
            else ""
        )
        if not bronze_table:
            bronze_table = (
                f"{bronze_catalog}.{bronze_schema}.bronze_{table_name}"
                if target_warehouse == "snowflake"
                else f"{bronze_schema}.bronze_{table_name}"
            )
        silver_table = (
            f"{silver_catalog}.{silver_schema}.silver_{table_name}"
            if target_warehouse == "snowflake"
            else f"{silver_schema}.silver_{table_name}"
        )
        resolved_by_table[table_name.lower()] = {
            "database_name": str(item.get("database_name") or "insurance"),
            "schema_name": str(item.get("schema_name") or "dbo"),
            "table_name": table_name,
            "bronze_table": bronze_table,
            "silver_table": silver_table,
            "existing_script_path": script_path if os.path.exists(script_path) else None,
            "source_columns": [
                {
                    "column_name": str(column.get("target") or column.get("source") or ""),
                    "source_column_name": str(column.get("target") or column.get("source") or ""),
                    "type": str(column.get("type") or ""),
                }
                for column in item.get("source_columns") or []
                if isinstance(column, dict)
            ],
        }

    return list(resolved_by_table.values())


def _columns_for_table(enriched_metadata: Dict[str, Any], table_name: str) -> List[Dict[str, Any]]:
    columns = enriched_metadata.get("columns", []) if isinstance(enriched_metadata, dict) else []
    return [
        column
        for column in columns
        if str(column.get("table_name") or "").strip().lower() == table_name.lower()
    ]


def _safe_python_list(values: List[str]) -> str:
    return repr([value for value in values if value])


def _datatype_cast(data_type: str) -> str | None:
    normalized = data_type.lower().strip()
    if normalized in {"int", "integer", "smallint", "tinyint"}:
        return "int"
    if normalized in {"bigint"}:
        return "bigint"
    if normalized in {"float", "real"}:
        return "double"
    if normalized in {"decimal", "numeric", "money", "smallmoney"}:
        return "decimal(38,10)"
    if normalized in {"date"}:
        return "date"
    if normalized in {"datetime", "datetime2", "smalldatetime", "timestamp"}:
        return "timestamp"
    if normalized in {"bit", "boolean"}:
        return "boolean"
    return None


def _key_columns(enriched_columns: List[Dict[str, Any]]) -> List[str]:
    reviewed = [
        _normalized_column_name(column)
        for column in enriched_columns
        if column.get("is_join_key") is True
    ]
    if reviewed:
        return reviewed
    return [
        _normalized_column_name(column)
        for column in enriched_columns
        if str(column.get("semantic_type") or "") in {"ID", "SURROGATE_KEY"}
    ]


COLUMN_NAME_CORRECTIONS = {
    "rererence_id": "reference_id",
}


def _normalized_column_name(column: Dict[str, Any]) -> str:
    normalized = str(column.get("column_name") or "").strip().lower()
    return COLUMN_NAME_CORRECTIONS.get(normalized, normalized)


def _source_column_name(column: Dict[str, Any]) -> str:
    source_name = str(column.get("source_column_name") or column.get("source") or "").strip().lower()
    if source_name:
        return source_name
    return str(column.get("column_name") or "").strip().lower()


def generate_silver_script(
    *,
    table_ref: SilverTableRef,
    enriched_columns: List[Dict[str, Any]],
    run_id: str,
    silver_catalog: str = "main",
    silver_schema: str = "silver",
) -> str:
    table_name = table_ref["table_name"]
    bronze_table = table_ref["bronze_table"]
    silver_table = table_ref["silver_table"]

    source_columns = [_normalized_column_name(column) for column in enriched_columns]
    source_columns = [column for column in source_columns if column]
    string_columns = [
        _normalized_column_name(column)
        for column in enriched_columns
        if str(column.get("data_type") or "").lower() in {"varchar", "nvarchar", "text", "char", "nchar"}
    ]
    pii_columns = [
        _normalized_column_name(column)
        for column in enriched_columns
        if column.get("is_pii_candidate") or column.get("is_pii") or column.get("semantic_type") == "PII"
    ]
    key_columns = _key_columns(enriched_columns)
    cast_rules = {
        _normalized_column_name(column): _datatype_cast(str(column.get("data_type") or ""))
        for column in enriched_columns
    }
    cast_rules = {key: value for key, value in cast_rules.items() if key and value}
    column_aliases = {
        bad_name: good_name
        for bad_name, good_name in COLUMN_NAME_CORRECTIONS.items()
        if bad_name in {str(column.get("column_name") or "").strip().lower() for column in enriched_columns}
    }

    return f'''
"""
AUTO-GENERATED SILVER TRANSFORMATION SCRIPT

Source table: {bronze_table}
Target table: {silver_table}
Expected runtime: Spark / Databricks with Delta support

POC rule: generated bronze scripts are treated as proof that bronze tables exist.
Runtime checks below still fail clearly if the Databricks table is missing.

DO NOT EDIT MANUALLY
"""

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, concat_ws, current_timestamp, expr, lit, row_number, sha2, trim, when
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

try:
    spark.sql("CREATE SCHEMA IF NOT EXISTS {silver_schema}")
except Exception:
    print("Could not create schema '{silver_schema}' in the current catalog")

RUN_ID = "{run_id}"
SOURCE_TABLE = "{bronze_table}"
TARGET_TABLE = "{silver_table}"
TEMP_VIEW = "silver_src_{table_name}"

EXPECTED_COLUMNS = {_safe_python_list(source_columns)}
STRING_COLUMNS = {_safe_python_list(string_columns)}
PII_COLUMNS = {_safe_python_list(pii_columns)}
KEY_COLUMNS = {_safe_python_list(key_columns)}
CAST_RULES = {repr(cast_rules)}
COLUMN_ALIASES = {repr(column_aliases)}

def _try_cast_column(column_name, target_type):
    escaped_name = column_name.replace("`", "``")
    return expr(f"try_cast(`{{escaped_name}}` AS {{target_type}})")

if not spark.catalog.tableExists(SOURCE_TABLE):
    raise ValueError(f"Missing bronze source table: {{SOURCE_TABLE}}")

df = spark.table(SOURCE_TABLE)

if df.limit(1).count() == 0:
    raise ValueError(f"Bronze source table has no rows: {{SOURCE_TABLE}}")

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

available_by_compact = {{
    compact_name(name): name
    for name in df.columns
}}

if EXPECTED_COLUMNS:
    select_expressions = []
    missing_columns = []
    selected_output_columns = set()
    for expected_name in EXPECTED_COLUMNS:
        if expected_name in selected_output_columns:
            continue
        actual_name = available_by_compact.get(compact_name(expected_name))
        if actual_name:
            select_expressions.append(col(actual_name).alias(expected_name))
            selected_output_columns.add(expected_name)
        else:
            missing_columns.append(expected_name)
else:
    selected_output_columns = set()
    select_expressions = [
        col(name)
        for name in df.columns
        if name not in metadata_columns
    ]
    selected_output_columns.update(name for name in df.columns if name not in metadata_columns)
    missing_columns = []

if not select_expressions:
    raise ValueError(
        f"No expected business columns found in {{SOURCE_TABLE}}. "
        f"Available columns: {{df.columns}}"
    )

metadata_expressions = [col(name) for name in metadata_columns if name not in selected_output_columns]
df = df.select(*select_expressions, *metadata_expressions)

if missing_columns:
    print(f"WARNING: Missing expected columns in {{SOURCE_TABLE}}: {{missing_columns}}")

for column_name in STRING_COLUMNS:
    if column_name in df.columns:
        df = df.withColumn(
            column_name,
            when(trim(col(column_name)) == "", None).otherwise(trim(col(column_name)))
        )

for column_name, target_type in CAST_RULES.items():
    if column_name in df.columns:
        df = df.withColumn(column_name, _try_cast_column(column_name, target_type))

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
    raise ValueError(f"No columns available to build Silver upsert key for {{TARGET_TABLE}}")

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

dedup_order_columns = [
    name for name in df.columns
    if any(token in name.lower() for token in ["updated", "modified", "effective", "inserted", "created", "timestamp", "date"])
]
dedup_order_columns.extend([
    name for name in ["ingestion_timestamp", "run_id"]
    if name in df.columns
])
dedup_order_columns = list(dict.fromkeys(dedup_order_columns))
if dedup_keys and dedup_order_columns:
    window_spec = Window.partitionBy(*dedup_keys).orderBy(
        *[col(name).desc_nulls_last() for name in dedup_order_columns]
    )
    df = (
        df
        .withColumn("_silver_row_number", row_number().over(window_spec))
        .filter(col("_silver_row_number") == 1)
        .drop("_silver_row_number")
    )
elif dedup_keys:
    df = df.dropDuplicates(dedup_keys)
else:
    df = df.dropDuplicates(["silver_upsert_key"])

df = (
    df
    .withColumn("silver_run_id", lit(RUN_ID))
    .withColumn("silver_processed_timestamp", current_timestamp())
)

df.createOrReplaceTempView(TEMP_VIEW)

create_table_sql = (
    f"CREATE TABLE IF NOT EXISTS {{TARGET_TABLE}} "
    f"USING DELTA "
    f"AS SELECT * FROM {{TEMP_VIEW}} WHERE 1 = 0"
)
spark.sql(create_table_sql)

target_columns = set(spark.table(TARGET_TABLE).columns)
if "silver_upsert_key" not in target_columns:
    spark.sql(f"ALTER TABLE {{TARGET_TABLE}} ADD COLUMNS (silver_upsert_key STRING)")

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

print(f"SUCCESS: Silver upsert completed for {{TARGET_TABLE}}")
'''


def _snowflake_type_from_metadata(column: Dict[str, Any]) -> str:
    explicit_type = str(column.get("type") or "").strip()
    if explicit_type:
        return explicit_type
    data_type = str(column.get("data_type") or "").strip().lower()
    precision = column.get("numeric_precision")
    scale = column.get("numeric_scale")
    max_length = column.get("character_maximum_length") or column.get("max_length")

    if data_type in {"int", "integer", "smallint", "tinyint", "bigint"}:
        return "NUMBER(38,0)"
    if data_type in {"bit", "boolean"}:
        return "BOOLEAN"
    if data_type in {"float", "real", "double"}:
        return "FLOAT"
    if data_type in {"decimal", "numeric", "number", "money", "smallmoney"}:
        if precision and scale is not None:
            return f"NUMBER({min(int(precision), 38)},{int(scale)})"
        return "NUMBER(38,10)"
    if data_type == "date":
        return "DATE"
    if data_type in {"datetime", "datetime2", "smalldatetime", "datetimeoffset", "time", "timestamp"}:
        return "TIMESTAMP_NTZ"
    if data_type in {"binary", "varbinary"}:
        return "BINARY"
    if data_type in {"varchar", "nvarchar", "char", "nchar", "text", "ntext", "string"}:
        try:
            length = int(max_length)
            if 0 < length <= 16777216:
                return f"VARCHAR({length})"
        except Exception:
            pass
    return "VARCHAR"


def _snowflake_column_expr(column: Dict[str, Any]) -> str:
    target_name = _normalized_column_name(column)
    source_name = _source_column_name(column)
    source_ref = f"GET_IGNORE_CASE(OBJECT_CONSTRUCT_KEEP_NULL(src.*), {_snowflake_string_literal(source_name)})"
    data_type = str(column.get("data_type") or "").strip().lower()
    target_type = _snowflake_type_from_metadata(column)

    if data_type in {"varchar", "nvarchar", "char", "nchar", "text", "ntext", "string"}:
        expression = f"NULLIF(TRIM(TO_VARCHAR({source_ref})), '')"
    elif target_type == "VARCHAR":
        expression = source_ref
    else:
        expression = _snowflake_variant_cast_expr(source_ref, target_type)
    return f"{expression} AS {_snowflake_quote_identifier(target_name)}"


def _snowflake_variant_cast_expr(source_ref: str, target_type: str) -> str:
    normalized = str(target_type or "").strip().upper()
    number_match = re.fullmatch(r"NUMBER\((\d+),\s*(\d+)\)", normalized)
    if number_match:
        return f"TRY_TO_DECIMAL(TO_VARCHAR({source_ref}), {number_match.group(1)}, {number_match.group(2)})"
    if normalized.startswith("NUMBER"):
        return f"TRY_TO_NUMBER(TO_VARCHAR({source_ref}))"
    if normalized == "FLOAT":
        return f"TRY_TO_DOUBLE(TO_VARCHAR({source_ref}))"
    if normalized == "BOOLEAN":
        return f"TRY_TO_BOOLEAN(TO_VARCHAR({source_ref}))"
    if normalized == "DATE":
        text_value = f"TO_VARCHAR({source_ref})"
        return f"COALESCE(TRY_TO_DATE({text_value}), TO_DATE(TRY_TO_TIMESTAMP_NTZ({text_value})))"
    if normalized == "TIMESTAMP_NTZ":
        return f"TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR({source_ref}))"
    if normalized == "BINARY":
        return f"CAST({source_ref} AS BINARY)"
    return f"CAST({source_ref} AS {target_type})"


def _snowflake_hash_expr(columns: List[str]) -> str:
    if not columns:
        return "SHA2('__NO_BUSINESS_COLUMNS__', 256)"
    parts = ",\n            ".join(
        f"COALESCE(TO_VARCHAR({_snowflake_quote_identifier(column)}), '__NULL__')" for column in columns
    )
    return f"SHA2(CONCAT_WS('||',\n            {parts}\n        ), 256)"


def generate_snowflake_silver_script(
    *,
    table_ref: SilverTableRef,
    enriched_columns: List[Dict[str, Any]],
    run_id: str,
    silver_catalog: str = "ATHENA_DB",
    silver_schema: str = "SILVER",
) -> str:
    table_name = table_ref["table_name"]
    source_table = _snowflake_qualified_name(*str(table_ref["bronze_table"]).split("."))
    target_table = _snowflake_qualified_name(*str(table_ref["silver_table"]).split("."))
    target_schema = _snowflake_qualified_name(silver_catalog, silver_schema)

    business_columns = []
    seen_columns: set[str] = set()
    for column in enriched_columns:
        column_name = _normalized_column_name(column)
        if column_name and column_name not in seen_columns:
            business_columns.append(column)
            seen_columns.add(column_name)

    if not business_columns:
        business_columns = [{"column_name": table_name, "data_type": "varchar"}]

    business_column_names = [_normalized_column_name(column) for column in business_columns]
    key_columns = [column for column in _key_columns(business_columns) if column in business_column_names]
    hash_columns = key_columns or business_column_names
    column_defs = ",\n    ".join(
        f"{_snowflake_quote_identifier(_normalized_column_name(column))} {_snowflake_type_from_metadata(column)}"
        for column in business_columns
    )
    business_selects = ",\n        ".join(_snowflake_column_expr(column) for column in business_columns)
    business_insert_columns = ",\n    ".join(_snowflake_quote_identifier(column) for column in business_column_names)
    all_insert_columns = ",\n    ".join(
        [
            business_insert_columns,
            '"run_id"',
            '"ingestion_timestamp"',
            '"source_system"',
            '"source_table"',
            '"silver_upsert_key"',
            '"silver_run_id"',
            '"silver_processed_timestamp"',
        ]
    )
    update_assignments = ",\n        ".join(
        f'target.{_snowflake_quote_identifier(column)} = source.{_snowflake_quote_identifier(column)}'
        for column in business_column_names
    )
    update_assignments = ",\n        ".join(
        [
            update_assignments,
            'target."run_id" = source."run_id"',
            'target."ingestion_timestamp" = source."ingestion_timestamp"',
            'target."source_system" = source."source_system"',
            'target."source_table" = source."source_table"',
            'target."silver_run_id" = source."silver_run_id"',
            'target."silver_processed_timestamp" = source."silver_processed_timestamp"',
        ]
    )
    insert_values = ",\n    ".join(f"source.{column}" for column in all_insert_columns.split(",\n    "))
    hash_expr = _snowflake_hash_expr(hash_columns)
    order_expr = 'COALESCE("ingestion_timestamp", "silver_processed_timestamp") DESC NULLS LAST'

    return f"""-- AUTO-GENERATED SILVER TRANSFORMATION SCRIPT
-- Source table: {table_ref["bronze_table"]}
-- Target table: {table_ref["silver_table"]}
-- Expected runtime: Snowflake SQL
-- Merge keys: {", ".join(key_columns) if key_columns else "business column hash fallback"}
-- DO NOT EDIT MANUALLY

CREATE SCHEMA IF NOT EXISTS {target_schema};

CREATE TABLE IF NOT EXISTS {target_table} (
    {column_defs},
    "run_id" VARCHAR,
    "ingestion_timestamp" TIMESTAMP_NTZ,
    "source_system" VARCHAR,
    "source_table" VARCHAR,
    "silver_upsert_key" VARCHAR,
    "silver_run_id" VARCHAR,
    "silver_processed_timestamp" TIMESTAMP_NTZ
);

MERGE INTO {target_table} AS target
USING (
    WITH normalized AS (
        SELECT
        {business_selects},
        src."run_id" AS "run_id",
        src."ingestion_timestamp" AS "ingestion_timestamp",
        src."source_system" AS "source_system",
        src."source_table" AS "source_table",
        {_snowflake_string_literal(run_id)} AS "silver_run_id",
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS "silver_processed_timestamp"
        FROM {source_table} AS src
    ),
    keyed AS (
        SELECT
            *,
            {hash_expr} AS "silver_upsert_key"
        FROM normalized
    )
    SELECT *
    FROM keyed
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY "silver_upsert_key"
        ORDER BY {order_expr}
    ) = 1
) AS source
ON target."silver_upsert_key" = source."silver_upsert_key"
WHEN MATCHED THEN UPDATE SET
        {update_assignments}
WHEN NOT MATCHED THEN INSERT (
    {all_insert_columns}
)
VALUES (
    {insert_values}
);
"""


def _generate_one_table(
    table_ref: SilverTableRef,
    *,
    enriched_metadata: Dict[str, Any],
    run_id: str,
    silver_catalog: str,
    silver_schema: str,
    target_warehouse: str = "databricks",
) -> Dict[str, object]:
    table_name = table_ref["table_name"]
    enriched_columns = _columns_for_table(enriched_metadata, table_name)
    if not enriched_columns and str(target_warehouse or "").lower() == "snowflake":
        enriched_columns = table_ref.get("source_columns") or []
    merge_keys = _key_columns(enriched_columns)

    if str(target_warehouse or "").lower() == "snowflake":
        code = generate_snowflake_silver_script(
            table_ref=table_ref,
            enriched_columns=enriched_columns,
            run_id=run_id,
            silver_catalog=silver_catalog,
            silver_schema=silver_schema,
        )
        script_language = "sql"
        extension = "sql"
        merge_strategy = "Snowflake MERGE on silver_upsert_key built from reviewed merge keys"
    else:
        code = generate_silver_script(
            table_ref=table_ref,
            enriched_columns=enriched_columns,
            run_id=run_id,
            silver_catalog=silver_catalog,
            silver_schema=silver_schema,
        )
        _validate_python(code)
        script_language = "python"
        extension = "py"
        merge_strategy = "Delta MERGE on silver_upsert_key built from reviewed merge keys"

    generation_mode = "DETERMINISTIC"
    if _llm_enabled_for_silver():
        try:
            candidate = _llm_generate_silver_code(
                table_ref=table_ref,
                enriched_columns=enriched_columns,
                deterministic_code=code,
                target_warehouse=target_warehouse,
            )
            repaired_candidate = (
                _canonicalize_snowflake_temporal_conversions(
                    _canonicalize_snowflake_source_identifiers(candidate, table_ref)
                )
                if str(target_warehouse or "").lower() == "snowflake"
                else candidate
            )
            _validate_generated_silver_code(
                repaired_candidate,
                table_ref=table_ref,
                enriched_columns=enriched_columns,
                target_warehouse=target_warehouse,
            )
            code = repaired_candidate
            generation_mode = "LLM_REPAIRED" if repaired_candidate != candidate else "LLM"
        except Exception as first_exc:
            try:
                retry_candidate = _llm_generate_silver_code(
                    table_ref=table_ref,
                    enriched_columns=enriched_columns,
                    deterministic_code=code,
                    target_warehouse=target_warehouse,
                    validation_feedback=str(first_exc),
                )
                repaired_retry = (
                    _canonicalize_snowflake_temporal_conversions(
                        _canonicalize_snowflake_source_identifiers(retry_candidate, table_ref)
                    )
                    if str(target_warehouse or "").lower() == "snowflake"
                    else retry_candidate
                )
                _validate_generated_silver_code(
                    repaired_retry,
                    table_ref=table_ref,
                    enriched_columns=enriched_columns,
                    target_warehouse=target_warehouse,
                )
                code = repaired_retry
                generation_mode = "LLM_RETRY_REPAIRED" if repaired_retry != retry_candidate else "LLM_RETRY"
            except Exception as retry_exc:
                logger.warning(
                    "Silver LLM generation and validation-feedback retry failed; using deterministic fallback: %s",
                    retry_exc,
                )

    output_dir = _silver_output_dir_for(target_warehouse)
    os.makedirs(output_dir, exist_ok=True)
    script_path = os.path.join(output_dir, f"silver_transform_{_run_slug(run_id)}_{_file_slug(table_name)}.{extension}")
    with open(script_path, "w", encoding="utf-8") as f:
        f.write(code)

    return {
        "run_id": run_id,
        "table": table_name,
        "database_name": table_ref["database_name"],
        "schema_name": table_ref["schema_name"],
        "source_table": table_ref["bronze_table"],
        "target_table": table_ref["silver_table"],
        "column_count": len(enriched_columns),
        "merge_keys": merge_keys,
        "primary_keys": merge_keys,
        "merge_key_source": "reviewed_gate4" if enriched_metadata.get("gate4_reviewed_merge_keys") else "semantic_enrichment",
        "merge_strategy": merge_strategy,
        "script_language": script_language,
        "generation_mode": generation_mode,
        "llm_enabled": _llm_enabled_for_silver(),
        "target_warehouse": str(target_warehouse or "databricks").lower(),
        "status": "APPROVED",
        "script_path": script_path,
    }


def _write_silver_readme(
    *,
    results: List[Dict[str, object]],
    generated_at: str,
    target_warehouse: str = "databricks",
) -> str:
    lines = [
        "# Silver Scripts",
        "",
        f"Generated at: `{generated_at}`",
        f"Script count: `{len(results)}`",
        "",
        "| Source Bronze | Target Silver | Columns | Script | Status |",
        "| --- | --- | ---: | --- | --- |",
    ]
    for item in sorted(results, key=lambda row: str(row.get("table", ""))):
        script_path = str(item.get("script_path") or "")
        script_name = os.path.basename(script_path) if script_path else "-"
        lines.append(
            f"| `{item.get('source_table')}` | `{item.get('target_table')}` | "
            f"`{item.get('column_count', 0)}` | [{script_name}]({script_path}) | `{item.get('status')}` |"
        )

    readme_path = _silver_readme_path(target_warehouse)
    with open(readme_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    return readme_path


def _write_silver_ui(
    *,
    results: List[Dict[str, object]],
    generated_at: str,
    target_warehouse: str = "databricks",
) -> str:
    rows: List[Dict[str, str]] = []
    for item in sorted(results, key=lambda row: str(row.get("table", ""))):
        script_path = str(item.get("script_path") or "")
        script_body = ""
        if script_path and os.path.exists(script_path):
            with open(script_path, "r", encoding="utf-8") as f:
                script_body = f.read()
        rows.append(
            {
                "table": str(item.get("table") or ""),
                "source_table": str(item.get("source_table") or ""),
                "target_table": str(item.get("target_table") or ""),
                "column_count": str(item.get("column_count") or 0),
                "status": str(item.get("status") or "-"),
                "script_body": script_body,
            }
        )

    payload = json.dumps(rows)
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Silver Scripts Viewer</title>
  <style>
    body {{ margin: 0; font-family: Segoe UI, Tahoma, sans-serif; background: #eef4f1; color: #1f2937; }}
    main {{ width: min(1100px, calc(100vw - 32px)); margin: 28px auto; }}
    .hero, .card {{ background: white; border: 1px solid #d9e4df; border-radius: 14px; padding: 18px; margin-bottom: 14px; }}
    input {{ width: 100%; padding: 11px; border: 1px solid #ccd8d3; border-radius: 8px; margin: 12px 0; }}
    pre {{ white-space: pre-wrap; word-break: break-word; background: #f8fafc; border: 1px solid #d9e4df; border-radius: 8px; padding: 14px; overflow: auto; }}
    .meta {{ color: #667085; }}
  </style>
</head>
<body>
  <main>
    <section class="hero">
      <h1>Silver Scripts Viewer</h1>
      <p class="meta">Generated at: {generated_at} | Scripts: {len(rows)}</p>
      <input id="search" type="search" placeholder="Search silver scripts..." />
    </section>
    <section id="list"></section>
  </main>
  <script>
    const rows = {payload};
    const list = document.getElementById("list");
    const search = document.getElementById("search");
    function escapeHtml(value) {{
      return String(value).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
    }}
    function render() {{
      const query = search.value.trim().toLowerCase();
      const filtered = rows.filter((row) => [row.table, row.source_table, row.target_table].join(" ").toLowerCase().includes(query));
      list.innerHTML = filtered.map((row) => `
        <article class="card">
          <h3>${{row.table}}</h3>
          <p class="meta">Source: ${{row.source_table}} | Target: ${{row.target_table}} | Columns: ${{row.column_count}} | Status: ${{row.status}}</p>
          <pre><code>${{escapeHtml(row.script_body)}}</code></pre>
        </article>
      `).join("");
    }}
    search.addEventListener("input", render);
    render();
  </script>
</body>
</html>
"""

    ui_path = _silver_ui_path(target_warehouse)
    with open(ui_path, "w", encoding="utf-8") as f:
        f.write(html)
    return ui_path


def _tokens(value: str) -> set[str]:
    return {
        token
        for token in re.split(r"[^a-z0-9]+", str(value or "").lower())
        if len(token) > 2
    }


def _column_tokens(column: Dict[str, Any]) -> set[str]:
    return _tokens(
        " ".join(
            str(column.get(key) or "")
            for key in (
                "table_name",
                "column_name",
                "business_description",
                "suggested_display_name",
                "semantic_type",
            )
        )
    )


def _extract_kpi_name(kpi: Dict[str, Any]) -> str:
    return str(kpi.get("kpi_name") or kpi.get("name") or kpi.get("title") or "").strip()


def _infer_aggregation(kpi_name: str, column: Dict[str, Any] | None) -> str:
    name = kpi_name.lower()
    if any(word in name for word in ("average", "avg", "mean")):
        return "AVG"
    if any(word in name for word in ("count", "number", "volume", "frequency", "distribution")):
        return "COUNT"
    if any(word in name for word in ("rate", "ratio", "percent", "percentage")):
        return "RATIO"
    if column:
        policy = column.get("aggregation_policy") or {}
        recommended = policy.get("recommended_aggregations") or []
        if recommended:
            return str(recommended[0])
        suggested = column.get("suggested_aggregation")
        if suggested and suggested != "NONE":
            return str(suggested)
    return "SUM"


def _score_column_for_kpi(kpi: Dict[str, Any], column: Dict[str, Any]) -> int:
    kpi_text = " ".join(str(kpi.get(key) or "") for key in ("kpi_name", "name", "kpi_description", "description"))
    overlap = _tokens(kpi_text).intersection(_column_tokens(column))
    score = len(overlap) * 10
    semantic = str(column.get("semantic_type") or "")
    if column.get("is_measure") or semantic == "MEASURE":
        score += 5
    if semantic in {"ID", "SURROGATE_KEY", "PII", "HIGH_CARD_TEXT"}:
        score -= 5
    column_name = str(column.get("column_name") or "").lower()
    compact_name = re.sub(r"[^a-z0-9]+", "", column_name)
    kpi_tokens = _tokens(kpi_text)
    score += sum(6 for token in kpi_tokens if token in compact_name)
    monetary_intent = {"amount", "premium", "insured", "estimate", "cost", "expense", "payment", "paid"}
    monetary_columns = ("amount", "premium", "insured", "estimate", "cost", "expense", "paid", "gross")
    if kpi_tokens & monetary_intent:
        score += 25 if any(token in compact_name for token in monetary_columns) else -25
    if any(token in compact_name for token in ("updatenum", "rownumber", "sequence", "version")):
        score -= 50
    return score


def _best_measure_for_kpi(kpi: Dict[str, Any], columns: List[Dict[str, Any]]) -> Dict[str, Any] | None:
    candidates = [
        column for column in columns
        if column.get("is_measure")
        or str(column.get("semantic_type") or "") in {"MEASURE", "FLAG"}
        or (column.get("aggregation_policy") or {}).get("allowed")
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda column: _score_column_for_kpi(kpi, column))


def _dimension_scope_tables(joins: List[Dict[str, Any]], measure_table: str | None) -> set[str]:
    if not measure_table:
        return set()
    scoped = {str(measure_table)}
    changed = True
    while changed:
        changed = False
        for join in joins or []:
            left = str(join.get("left_table") or "")
            right = str(join.get("right_table") or "")
            if left in scoped and right and right not in scoped:
                scoped.add(right)
                changed = True
            if right in scoped and left and left not in scoped:
                scoped.add(left)
                changed = True
    return scoped


def _dimension_columns(
    columns: List[Dict[str, Any]],
    measure_table: str | None,
    joins: List[Dict[str, Any]] | None = None,
) -> List[Dict[str, str]]:
    scoped_tables = _dimension_scope_tables(joins or [], measure_table)
    dimensions: List[Dict[str, str]] = []
    for column in columns:
        semantic = str(column.get("semantic_type") or "")
        if semantic not in {"DIMENSION", "DATE", "FLAG"}:
            continue
        table_name = str(column.get("table_name") or "")
        if scoped_tables and table_name not in scoped_tables and semantic != "DATE":
            continue
        dimensions.append(
            {
                "table": table_name,
                "column": str(column.get("column_name") or ""),
                "semantic_type": semantic,
            }
        )
    return dimensions[:12]


def _max_gold_dimension_tables() -> int:
    try:
        return max(0, int(os.getenv("ATHENA_GOLD_MAX_DIMENSION_TABLES", str(DEFAULT_MAX_GOLD_DIMENSION_TABLES))))
    except ValueError:
        return DEFAULT_MAX_GOLD_DIMENSION_TABLES


def _canonical_gold_column(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    return COLUMN_NAME_CORRECTIONS.get(normalized, normalized)


def _constrain_gold_mapping(
    mapping: Dict[str, Any], silver_tables: Dict[str, str]
) -> tuple[Dict[str, Any], List[str]]:
    """Keep Gold lineage executable against registered Silver outputs."""
    warnings: List[str] = []
    measure = dict(mapping.get("measure") or {})
    measure_table = str(measure.get("table") or "").strip().casefold()
    source_table = silver_tables.get(measure_table)
    if not source_table:
        return {**mapping, "source_silver_table": None, "readiness": "BLOCKED"}, [
            f"Gold measure table '{measure_table or 'unknown'}' has no generated Silver target."
        ]

    measure["column"] = _canonical_gold_column(measure.get("column"))
    if measure.get("aggregation") != "COUNT" and measure.get("column"):
        measure["expression"] = f"{measure.get('aggregation')}({measure['column']})"

    dimensions: List[Dict[str, Any]] = []
    table_scores: Dict[str, float] = {}
    for dimension in mapping.get("grouping_dimensions") or []:
        if not isinstance(dimension, dict):
            continue
        table = str(dimension.get("table") or measure_table).strip().casefold()
        if table not in silver_tables:
            warnings.append(f"Dropped Gold dimension from '{table}' because no Silver target exists.")
            continue
        normalized = {
            **dimension,
            "table": table,
            "column": _canonical_gold_column(dimension.get("column")),
            "source_silver_table": silver_tables[table],
        }
        dimensions.append(normalized)
        if str(dimension.get("semantic_type") or "").upper() != "DATE":
            table_scores[table] = table_scores.get(table, 0.0) + 100.0

    valid_joins: List[Dict[str, Any]] = []
    for join in mapping.get("join_paths") or []:
        if not isinstance(join, dict) or not join.get("certified"):
            continue
        left = str(join.get("left_table") or "").strip().casefold()
        right = str(join.get("right_table") or "").strip().casefold()
        if left not in silver_tables or right not in silver_tables:
            warnings.append(f"Dropped Gold join '{left}->{right}' because a Silver target is unavailable.")
            continue
        try:
            confidence = float(join.get("confidence") or 0)
        except (TypeError, ValueError):
            confidence = 0.0
        table_scores[left] = table_scores.get(left, 0.0) + confidence * 10
        table_scores[right] = table_scores.get(right, 0.0) + confidence * 10
        valid_joins.append({
            **join,
            "left_table": left,
            "left_column": _canonical_gold_column(join.get("left_column")),
            "left_source_table": silver_tables[left],
            "right_table": right,
            "right_column": _canonical_gold_column(join.get("right_column")),
            "right_source_table": silver_tables[right],
            "certified": True,
        })

    ranked_dimension_tables = sorted(table_scores, key=lambda table: (-table_scores[table], table))
    kept_dimension_tables = set(ranked_dimension_tables[:_max_gold_dimension_tables()])
    dropped_dimension_tables = [table for table in ranked_dimension_tables if table not in kept_dimension_tables]
    if dropped_dimension_tables:
        warnings.append(f"Gold dimension table cap applied; dropped {', '.join(dropped_dimension_tables)}.")
    dimensions = [
        item
        for item in dimensions
        if str(item.get("semantic_type") or "").upper() == "DATE"
        or str(item.get("table") or "").casefold() in kept_dimension_tables
    ]
    allowed_join_tables = {measure_table, *kept_dimension_tables}
    valid_joins = [
        join
        for join in valid_joins
        if join["left_table"] in allowed_join_tables and join["right_table"] in allowed_join_tables
    ]

    time_info = dict(mapping.get("time") or {})
    time_column = time_info.get("column")
    if isinstance(time_column, dict):
        time_table = str(time_column.get("table") or measure_table).strip().casefold()
        if time_table in silver_tables:
            time_info["column"] = {
                **time_column,
                "table": time_table,
                "column": _canonical_gold_column(time_column.get("column")),
            }
        else:
            time_info["column"] = None
            warnings.append(f"Dropped Gold time column from '{time_table}' because no Silver target exists.")

    fact_grain = [str(item.get("column") or "") for item in dimensions if item.get("column")]
    if time_info.get("column"):
        fact_grain.append("period_start")
    return {
        **mapping,
        "source_silver_table": source_table,
        "measure": measure,
        "grouping_dimensions": dimensions,
        "time": time_info,
        "join_paths": valid_joins,
        "fact_grain": list(dict.fromkeys(fact_grain)),
        "dimension_table_limit": _max_gold_dimension_tables(),
        "selected_dimension_tables": sorted(kept_dimension_tables),
    }, warnings


def _time_column(columns: List[Dict[str, Any]], measure_table: str | None) -> Dict[str, str] | None:
    date_columns = [
        column for column in columns
        if str(column.get("semantic_type") or "") in {"DATE", "AUDIT_TIMESTAMP"}
        and (not measure_table or column.get("table_name") == measure_table)
    ]
    if not date_columns:
        date_columns = [column for column in columns if str(column.get("semantic_type") or "") == "DATE"]
    if not date_columns:
        return None
    preferred = max(date_columns, key=lambda column: int("date" in str(column.get("column_name") or "").lower()))
    return {
        "table": str(preferred.get("table_name") or ""),
        "column": str(preferred.get("column_name") or ""),
    }


def _time_grain(state: Stage01State) -> str:
    frequency = str(state.get("req_reporting_frequency") or "").lower()
    if "day" in frequency or "daily" in frequency:
        return "day"
    if "week" in frequency or "weekly" in frequency:
        return "week"
    if "quarter" in frequency or "quarterly" in frequency:
        return "quarter"
    if "year" in frequency or "annual" in frequency:
        return "year"
    return "month"


def _join_paths_for_table(joins: List[Dict[str, Any]], table_name: str | None) -> List[Dict[str, Any]]:
    if not table_name:
        return []
    return [
        {
            "left_table": join.get("left_table"),
            "left_column": join.get("left_column"),
            "right_table": join.get("right_table"),
            "right_column": join.get("right_column"),
            "join_type": join.get("join_type", "INNER"),
            "cardinality": join.get("cardinality"),
            "confidence": join.get("confidence"),
            "source": join.get("source"),
            "constraint_name": join.get("constraint_name"),
            "certified": bool(join.get("certified")),
        }
        for join in joins
        if join.get("left_table") == table_name or join.get("right_table") == table_name
    ]


def _silver_tables_by_name(results: List[Dict[str, object]]) -> Dict[str, str]:
    return {
        str(item.get("table") or "").lower(): str(item.get("target_table") or "")
        for item in results
        if item.get("table")
    }


def _dimension_mappings_from_kpis(kpi_mappings: List[Dict[str, Any]], silver_tables: Dict[str, str]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for mapping in kpi_mappings:
        if not isinstance(mapping, dict):
            continue
        kpi_name = str(mapping.get("kpi_name") or "")
        for dimension in mapping.get("grouping_dimensions") or []:
            if not isinstance(dimension, dict):
                continue
            if str(dimension.get("semantic_type") or "").upper() == "DATE":
                continue
            table = str(dimension.get("table") or "").strip()
            column = str(dimension.get("column") or "").strip()
            if not table or not column:
                continue
            row = grouped.setdefault(
                table.lower(),
                {
                    "logical_table": table,
                    "source_silver_table": silver_tables.get(table.lower()),
                    "columns": [],
                    "consumed_by_kpis": [],
                },
            )
            if column not in row["columns"]:
                row["columns"].append(column)
            if kpi_name and kpi_name not in row["consumed_by_kpis"]:
                row["consumed_by_kpis"].append(kpi_name)
    return sorted(grouped.values(), key=lambda item: str(item.get("logical_table") or ""))


def _extract_json_object(value: Any) -> Dict[str, Any]:
    text = str(value or "").strip()
    match = re.search(r"```(?:json)?\s*(.*?)```", text, flags=re.IGNORECASE | re.DOTALL)
    parsed = json.loads(match.group(1).strip() if match else text)
    if not isinstance(parsed, dict):
        raise ValueError("Kimball plan must be a JSON object")
    return parsed


def _kimball_candidates(columns: List[Dict[str, Any]], certified_joins: List[Dict[str, Any]]) -> Dict[str, Dict[str, Dict[str, Any]]]:
    measures: Dict[str, Dict[str, Any]] = {}
    dimensions: Dict[str, Dict[str, Any]] = {}
    times: Dict[str, Dict[str, Any]] = {}
    joins: Dict[str, Dict[str, Any]] = {}
    for column in columns:
        if not isinstance(column, dict):
            continue
        candidate = {
            "table": str(column.get("table_name") or ""),
            "column": str(column.get("column_name") or ""),
            "semantic_type": str(column.get("semantic_type") or "").upper(),
        }
        if not candidate["table"] or not candidate["column"]:
            continue
        if candidate["semantic_type"] in {"MEASURE", "FLAG"}:
            measures[f"M{len(measures) + 1}"] = candidate
        if candidate["semantic_type"] in {"DIMENSION", "DATE", "FLAG"}:
            dimensions[f"D{len(dimensions) + 1}"] = candidate
        if candidate["semantic_type"] in {"DATE", "AUDIT_TIMESTAMP"}:
            times[f"T{len(times) + 1}"] = candidate
    for join in certified_joins:
        if not isinstance(join, dict) or not join.get("certified"):
            continue
        joins[f"J{len(joins) + 1}"] = {
            key: join.get(key)
            for key in ("left_table", "left_column", "right_table", "right_column", "join_type", "cardinality", "confidence")
        }
    return {"measures": measures, "dimensions": dimensions, "times": times, "joins": joins}


def _kimball_plan_prompt(
    *,
    kpi: Dict[str, Any],
    mapping: Dict[str, Any],
    columns: List[Dict[str, Any]],
    certified_joins: List[Dict[str, Any]],
    validation_feedback: str = "",
) -> str:
    candidates = _kimball_candidates(columns, certified_joins)
    retry_context = (
        f"\nPrevious plan rejected: {validation_feedback}\nCorrect only that violation.\n"
        if validation_feedback
        else ""
    )
    shape = '{"measure_id":"M1","aggregation":"SUM|AVG|MIN|MAX|COUNT","dimension_ids":["D1"],"time_id":"T1|null","time_grain":"day|week|month|quarter|year","join_ids":["J1"],"fact_grain":["D1","period_start"]}'
    return (
        "Design a Kimball Gold model. Return only JSON matching this exact shape. "
        "Use only candidate IDs. Do not invent IDs, columns, joins, or aggregations. "
        "fact_grain must include every selected dimension ID and include period_start only when time_id is set."
        + retry_context
        + "\nKPI=" + json.dumps(kpi, default=str)
        + "\nCURRENT_MAPPING=" + json.dumps(mapping, default=str)
        + "\nCANDIDATES=" + json.dumps(candidates, default=str)
    )


def _normalize_kimball_plan(plan: Dict[str, Any], *, columns: List[Dict[str, Any]], certified_joins: List[Dict[str, Any]]) -> Dict[str, Any]:
    candidates = _kimball_candidates(columns, certified_joins)
    index = {(str(c.get("table_name") or "").casefold(), str(c.get("column_name") or "").casefold()): c for c in columns if isinstance(c, dict)}

    def canonical_column(item: Dict[str, Any]) -> Dict[str, Any]:
        meta = index.get((str(item.get("table") or "").casefold(), str(item.get("column") or "").casefold()))
        return {
            "table": str((meta or item).get("table_name") or item.get("table") or ""),
            "column": str((meta or item).get("column_name") or item.get("column") or ""),
            "semantic_type": str((meta or item).get("semantic_type") or item.get("semantic_type") or "").upper(),
        }

    measure_input = plan.get("measure") or candidates["measures"].get(str(plan.get("measure_id") or ""), {})
    dimensions_input = plan.get("dimensions") if isinstance(plan.get("dimensions"), list) else [
        candidates["dimensions"].get(str(identifier) or "", {}) for identifier in plan.get("dimension_ids") or []
    ]
    time_input = plan.get("time") or candidates["times"].get(str(plan.get("time_id") or ""), {})
    join_lookup = {
        tuple(str(join.get(key) or "").casefold() for key in ("left_table", "left_column", "right_table", "right_column")): join
        for join in candidates["joins"].values()
    }
    joins_input = plan.get("join_paths") if isinstance(plan.get("join_paths"), list) else [
        candidates["joins"].get(str(identifier) or "", {}) for identifier in plan.get("join_ids") or []
    ]
    normalized_joins = []
    for join in joins_input:
        signature = tuple(str(join.get(key) or "").casefold() for key in ("left_table", "left_column", "right_table", "right_column"))
        reverse = (signature[2], signature[3], signature[0], signature[1])
        normalized_joins.append(join_lookup.get(signature) or join_lookup.get(reverse) or join)

    normalized_dimensions = [canonical_column(item) for item in dimensions_input if isinstance(item, dict)]
    normalized_time = canonical_column(time_input) if isinstance(time_input, dict) and time_input else {}
    if normalized_time:
        normalized_time["grain"] = str((time_input or {}).get("grain") or plan.get("time_grain") or "").lower()
    fact_grain = list(plan.get("fact_grain") or [])
    if fact_grain and any(str(value).upper().startswith("D") for value in fact_grain):
        fact_grain = [
            candidates["dimensions"].get(str(value), {}).get("column", value)
            for value in fact_grain
        ]
    if not fact_grain:
        fact_grain = [item["column"] for item in normalized_dimensions]
        if normalized_time:
            fact_grain.append("period_start")
    return {
        "measure": {**canonical_column(measure_input), "aggregation": str(plan.get("aggregation") or measure_input.get("aggregation") or "").upper()},
        "dimensions": normalized_dimensions,
        "time": normalized_time,
        "join_paths": normalized_joins,
        "fact_grain": fact_grain,
    }


def _validate_kimball_plan(plan: Dict[str, Any], *, columns: List[Dict[str, Any]], certified_joins: List[Dict[str, Any]]) -> Dict[str, Any]:
    plan = _normalize_kimball_plan(plan, columns=columns, certified_joins=certified_joins)
    index = {(str(c.get("table_name") or "").casefold(), str(c.get("column_name") or "").casefold()): c for c in columns if isinstance(c, dict)}
    measure = plan.get("measure") or {}
    measure_meta = index.get((str(measure.get("table") or "").casefold(), str(measure.get("column") or "").casefold()))
    if not measure_meta or str(measure_meta.get("semantic_type") or "").upper() not in {"MEASURE", "FLAG"}:
        raise ValueError("Kimball plan selected an invalid measure")
    if str(measure.get("aggregation") or "").upper() not in {"SUM", "AVG", "MIN", "MAX", "COUNT"}:
        raise ValueError("Kimball plan selected an unsupported aggregation")
    dimensions = plan.get("dimensions") or []
    if not isinstance(dimensions, list) or len(dimensions) > 12:
        raise ValueError("Kimball plan has an invalid dimension list")
    for item in dimensions:
        meta = index.get((str(item.get("table") or "").casefold(), str(item.get("column") or "").casefold()))
        if not meta or str(meta.get("semantic_type") or "").upper() not in {"DIMENSION", "DATE", "FLAG"}:
            raise ValueError("Kimball plan selected an invalid dimension")
    time = plan.get("time") or {}
    if time:
        meta = index.get((str(time.get("table") or "").casefold(), str(time.get("column") or "").casefold()))
        if not meta or str(meta.get("semantic_type") or "").upper() not in {"DATE", "AUDIT_TIMESTAMP"}:
            raise ValueError("Kimball plan selected an invalid time column")
        if str(time.get("grain") or "").lower() not in {"day", "week", "month", "quarter", "year"}:
            raise ValueError("Kimball plan selected an invalid time grain")
    certified = {tuple(str(j.get(k) or "").casefold() for k in ("left_table", "left_column", "right_table", "right_column")) for j in certified_joins if isinstance(j, dict)}
    for join in plan.get("join_paths") or []:
        signature = tuple(str(join.get(k) or "").casefold() for k in ("left_table", "left_column", "right_table", "right_column"))
        if signature not in certified:
            raise ValueError("Kimball plan selected a non-certified join")
    expected_grain = {str(item.get("column") or "") for item in dimensions}
    if time:
        expected_grain.add("period_start")
    actual_grain = [str(item or "") for item in plan.get("fact_grain") or []]
    if not actual_grain or len(actual_grain) != len(set(actual_grain)) or set(actual_grain) != expected_grain:
        raise ValueError("Kimball plan has an invalid fact grain")
    return plan


def _apply_kimball_plan(mapping: Dict[str, Any], plan: Dict[str, Any]) -> Dict[str, Any]:
    result = dict(mapping)
    aggregation = str(plan["measure"]["aggregation"]).upper()
    result["measure"] = {**(mapping.get("measure") or {}), **plan["measure"], "aggregation": aggregation}
    result["measure"]["expression"] = f"{aggregation}({plan['measure']['column']})" if aggregation != "COUNT" else "COUNT(*)"
    result["formula"] = {"type": "single_measure", "status": "PROPOSED"}
    result["grouping_dimensions"] = list(plan.get("dimensions") or [])
    if plan.get("time"):
        result["time"] = dict(plan["time"])
    result["join_paths"] = [{**join, "certified": True} for join in plan.get("join_paths") or []]
    result["fact_grain"] = list(plan.get("fact_grain") or [])
    result["kimball_plan"] = plan
    result["kimball_plan_source"] = "LLM_VALIDATED"
    return result


def _llm_kimball_plan(
    *,
    kpi: Dict[str, Any],
    mapping: Dict[str, Any],
    columns: List[Dict[str, Any]],
    certified_joins: List[Dict[str, Any]],
    validation_feedback: str = "",
) -> Dict[str, Any]:
    from nodes.req_extraction import get_llm
    llm = get_llm(provider=os.getenv("ATHENA_GOLD_LLM_PROVIDER", os.getenv("ATHENA_LLM_PROVIDER", "azure_openai")), model=os.getenv("ATHENA_GOLD_KIMBALL_PLAN_MODEL") or os.getenv("ATHENA_GOLD_LLM_MODEL"), temperature=0.0)
    response = llm.invoke(_kimball_plan_prompt(kpi=kpi, mapping=mapping, columns=columns, certified_joins=certified_joins, validation_feedback=validation_feedback))
    plan = _extract_json_object(getattr(response, "content", response))
    validated = _validate_kimball_plan(plan, columns=columns, certified_joins=certified_joins)
    selected = validated.get("measure") or {}
    selected_meta = next(
        (
            column
            for column in columns
            if str(column.get("table_name") or "").casefold() == str(selected.get("table") or "").casefold()
            and str(column.get("column_name") or "").casefold() == str(selected.get("column") or "").casefold()
        ),
        None,
    )
    best = _best_measure_for_kpi(kpi, columns)
    if selected_meta and best and _score_column_for_kpi(kpi, selected_meta) < _score_column_for_kpi(kpi, best):
        raise ValueError(
            "Kimball plan selected a weaker KPI measure than the deterministic semantic match: "
            f"{selected.get('column')} < {best.get('column_name')}"
        )
    expected_aggregation = _infer_aggregation(_extract_kpi_name(kpi), selected_meta)
    if expected_aggregation != "RATIO" and str(selected.get("aggregation") or "").upper() != expected_aggregation:
        raise ValueError(
            f"Kimball plan changed KPI aggregation from {expected_aggregation} "
            f"to {selected.get('aggregation')}"
        )
    return validated


def _build_gold_generation_contract(
    *,
    state: Stage01State,
    results: List[Dict[str, object]],
    enriched_metadata: Dict[str, Any],
    generated_at: str,
) -> Dict[str, Any]:
    columns = enriched_metadata.get("columns", []) if isinstance(enriched_metadata, dict) else []
    if isinstance(enriched_metadata, dict):
        joins = enriched_metadata.get("certified_joins") or enriched_metadata.get("joins", [])
        fallback_joins = enriched_metadata.get("join_candidates") or []
    else:
        joins = []
        fallback_joins = []
    joins = [
        join
        for join in joins
        if isinstance(join, dict) and join.get("certified") is True
    ]
    certified_kpis = state.get("certified_kpis") or enriched_metadata.get("certified_kpis") or []
    silver_tables = _silver_tables_by_name(results)
    warnings: List[str] = []
    kpi_mappings: List[Dict[str, Any]] = []

    for kpi in certified_kpis:
        if not isinstance(kpi, dict):
            continue
        kpi_name = _extract_kpi_name(kpi)
        measure = _best_measure_for_kpi(kpi, columns)
        measure_table = str((measure or {}).get("table_name") or "")
        measure_column = str((measure or {}).get("column_name") or "")
        aggregation = _infer_aggregation(kpi_name, measure)
        date_column = _time_column(columns, measure_table)
        join_paths = _join_paths_for_table(joins, measure_table)
        dimensions = _dimension_columns(columns, measure_table, join_paths)

        if not measure:
            warnings.append(f"No measure column mapped for KPI '{kpi_name}'.")
        if measure_table and measure_table.lower() not in silver_tables:
            warnings.append(f"KPI '{kpi_name}' maps to table '{measure_table}', but no silver script is registered for that table.")
        if aggregation == "RATIO":
            warnings.append(f"KPI '{kpi_name}' needs numerator/denominator formula certification before gold SQL is production-safe.")
        if not join_paths:
            heuristic_paths = _join_paths_for_table(fallback_joins, measure_table)
            if heuristic_paths:
                warnings.append(f"KPI '{kpi_name}' has heuristic join candidates, but no certified FK-backed joins.")

        mapping = {
                "kpi_name": kpi_name,
                "kpi_description": kpi.get("kpi_description") or kpi.get("description"),
                "source_silver_table": silver_tables.get(measure_table.lower()) if measure_table else None,
                "measure": {
                    "table": measure_table or None,
                    "column": measure_column or None,
                    "aggregation": aggregation,
                    "expression": (
                        f"{aggregation}({measure_column})"
                        if measure_column and aggregation not in {"RATIO", "COUNT"}
                        else ("COUNT(*)" if aggregation == "COUNT" else None)
                    ),
                    "confidence": _score_column_for_kpi(kpi, measure) if measure else 0,
                },
                "formula": {
                    "type": "derived" if aggregation == "RATIO" else "single_measure",
                    "status": "NEEDS_CERTIFICATION" if aggregation == "RATIO" or not measure else "PROPOSED",
                },
                "grouping_dimensions": dimensions,
                "time": {
                    "grain": _time_grain(state),
                    "column": date_column,
                },
                "filters": list(state.get("req_constraints") or []),
                "join_paths": join_paths,
                "readiness": "BLOCKED" if not measure else "READY_WITH_WARNINGS" if join_paths else "READY",
            }
        if _llm_enabled_for_kimball_plan() and measure:
            try:
                plan = _llm_kimball_plan(kpi=kpi, mapping=mapping, columns=columns, certified_joins=joins)
                mapping = _apply_kimball_plan(mapping, plan)
            except Exception as first_exc:
                try:
                    plan = _llm_kimball_plan(
                        kpi=kpi,
                        mapping=mapping,
                        columns=columns,
                        certified_joins=joins,
                        validation_feedback=str(first_exc),
                    )
                    mapping = _apply_kimball_plan(mapping, plan)
                    mapping["kimball_plan_source"] = "LLM_RETRY_VALIDATED"
                except Exception as retry_exc:
                    mapping["kimball_plan_source"] = "DETERMINISTIC_FALLBACK"
                    warnings.append(f"KPI '{kpi_name}' Kimball LLM plan rejected; deterministic plan retained: {retry_exc}")
                    logger.warning("Kimball plan rejected for KPI %s; deterministic fallback retained: %s", kpi_name, retry_exc)
        else:
            mapping["kimball_plan_source"] = "DETERMINISTIC"
        mapping, mapping_warnings = _constrain_gold_mapping(mapping, silver_tables)
        warnings.extend(f"KPI '{kpi_name}': {warning}" for warning in mapping_warnings)
        kpi_mappings.append(mapping)

    status = "READY"
    if warnings:
        status = "READY_WITH_WARNINGS"
    if kpi_mappings and all(item["readiness"] == "BLOCKED" for item in kpi_mappings):
        status = "FAILED"
    if not kpi_mappings:
        status = "SKIPPED"
        warnings.append("No certified KPIs found for gold contract generation.")

    return {
        "run_id": state.get("run_id"),
        "fingerprint": state.get("fingerprint") or state.get("run_id"),
        "generated_at": generated_at,
        "status": status,
        "silver_tables": [
            {
                "table": item.get("table"),
                "source_table": item.get("source_table"),
                "target_table": item.get("target_table"),
                "column_count": item.get("column_count"),
            }
            for item in sorted(results, key=lambda row: str(row.get("table", "")))
        ],
        "dimension_mappings": _dimension_mappings_from_kpis(kpi_mappings, silver_tables),
        "kpi_mappings": kpi_mappings,
        "kimball_plan_enabled": _llm_enabled_for_kimball_plan(),
        "available_joins": joins,
        "join_candidates": fallback_joins,
        "warnings": sorted(set(warnings)),
        "next_gate_required": "GOLD_CONTRACT_REVIEW",
    }


def _write_gold_contract(contract: Dict[str, Any]) -> str:
    os.makedirs(_gold_output_dir(), exist_ok=True)
    path = os.path.join(_gold_output_dir(), "gold_generation_contract.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(contract, f, indent=2)
    return path


def _persist_generation_artifacts(
    *,
    state: Stage01State,
    silver_bundle: Dict[str, Any],
    gold_contract: Dict[str, Any],
) -> None:
    run_id = str(state.get("run_id") or "SILVER_POC_RUN_001")
    fingerprint = str(state.get("fingerprint") or run_id)
    ai_store_db_writer(
        run_id=run_id,
        stage="Silver Code Generation",
        artifact_type="SILVER_GENERATION",
        payload=silver_bundle,
        schema_version="SilverGeneration_v1",
        prompt_version="DETERMINISTIC_SPARK_SILVER_v1",
        faithfulness_status="PASSED",
        token_count=0,
        input_tokens=0,
        output_tokens=0,
        fingerprint=fingerprint,
    )
    ai_store_db_writer(
        run_id=run_id,
        stage="Gold Contract Generation",
        artifact_type="GOLD_GENERATION_CONTRACT",
        payload=gold_contract,
        schema_version="GoldGenerationContract_v1",
        prompt_version="HEURISTIC_GOLD_CONTRACT_v1",
        faithfulness_status="PASSED" if gold_contract.get("status") != "FAILED" else "WARN",
        token_count=0,
        input_tokens=0,
        output_tokens=0,
        fingerprint=fingerprint,
    )


def silver_code_generation_node(state: Stage01State) -> Stage01State:
    new_state = state.copy()
    target_warehouse = str(state.get("target_warehouse") or "databricks").lower()
    table_refs = _resolve_tables_for_silver(state)

    if not table_refs:
        new_state["silver_generation_status"] = "SKIPPED"
        new_state["silver_generation_error"] = "No bronze generation results or bronze bundle found."
        return new_state

    enriched_metadata = (
        state.get("enrichment_review_artifact")
        or state.get("enriched_metadata")
        or {}
    )
    if isinstance(enriched_metadata, dict) and "enrichment_artifact" in enriched_metadata:
        enriched_metadata = enriched_metadata.get("enrichment_artifact") or {}

    run_id = str(state.get("run_id") or "SILVER_POC_RUN_001")
    silver_catalog = str(state.get("silver_catalog") or state.get("bronze_catalog") or "main")
    silver_schema = str(state.get("silver_schema") or "silver")
    if target_warehouse == "snowflake":
        silver_catalog = _snowflake_silver_catalog()
        silver_schema = _snowflake_silver_schema()

    results: List[Dict[str, object]] = []
    with ThreadPoolExecutor(max_workers=SILVER_MAX_WORKERS) as executor:
        futures = [
            executor.submit(
                _generate_one_table,
                table_ref,
                enriched_metadata=enriched_metadata,
                run_id=run_id,
                silver_catalog=silver_catalog,
                silver_schema=silver_schema,
                target_warehouse=target_warehouse,
            )
            for table_ref in table_refs
        ]
        for future in as_completed(futures):
            results.append(future.result())

    generated_at = datetime.utcnow().isoformat()
    bundle = {
        "run_id": run_id,
        "generated_at": generated_at,
        "script_count": len(results),
        "target_warehouse": target_warehouse,
        "llm_enabled": _llm_enabled_for_silver(),
        "scripts": results,
    }

    output_dir = _silver_output_dir_for(target_warehouse)
    os.makedirs(output_dir, exist_ok=True)
    bundle_path = os.path.join(output_dir, f"{_run_slug(run_id)}_silver_scripts.json")
    latest_bundle_path = os.path.join(output_dir, "silver_scripts.json")
    with open(bundle_path, "w", encoding="utf-8") as f:
        json.dump(bundle, f, indent=2)
    with open(latest_bundle_path, "w", encoding="utf-8") as f:
        json.dump(bundle, f, indent=2)

    readme_path = _write_silver_readme(
        results=results,
        generated_at=generated_at,
        target_warehouse=target_warehouse,
    )
    ui_path = _write_silver_ui(
        results=results,
        generated_at=generated_at,
        target_warehouse=target_warehouse,
    )
    gold_contract = _build_gold_generation_contract(
        state=state,
        results=results,
        enriched_metadata=enriched_metadata,
        generated_at=generated_at,
    )
    gold_contract_path = _write_gold_contract(gold_contract)
    try:
        _persist_generation_artifacts(state=state, silver_bundle=bundle, gold_contract=gold_contract)
    except Exception as exc:
        logger.warning("Generation artifact persistence failed: %s", exc, extra={"run_id": run_id, "node": "silver_generation"})

    new_state["silver_generation_status"] = "COMPLETED"
    new_state["silver_generation_error"] = None
    new_state["silver_generated_at"] = generated_at
    new_state["silver_generation_results"] = results
    new_state["silver_generation_bundle_path"] = bundle_path
    new_state["silver_generation_readme_path"] = readme_path
    new_state["silver_generation_ui_path"] = ui_path
    new_state["gold_contract_status"] = gold_contract["status"]
    new_state["gold_contract_error"] = "; ".join(gold_contract["warnings"]) if gold_contract["warnings"] else None
    new_state["gold_generation_contract"] = gold_contract
    new_state["gold_contract_bundle_path"] = gold_contract_path
    new_state["status"] = "PIPELINE_COMPLETED"

    logger.info(
        "Silver generation completed: %d scripts target_warehouse=%s",
        len(results),
        target_warehouse,
        extra={"run_id": run_id, "node": "silver_generation"},
    )
    return new_state
