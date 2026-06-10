from __future__ import annotations

import json
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from langchain_core.messages import HumanMessage, SystemMessage

from nodes.req_extraction import get_llm
from state import Stage01State
from utilis.ai_store_writer import ai_store_db_writer
from utilis.logger import logger

SILVER_OUTPUT_DIR = os.path.join(os.getcwd(), "generated_code", "silver")
SILVER_LLM_ENABLED = os.getenv("ATHENA_ENABLE_LLM_SFTP_SILVER", "false").lower() in {"1", "true", "yes", "on"}
SILVER_LLM_TIMEOUT_SECONDS = int(os.getenv("ATHENA_SFTP_SILVER_LLM_TIMEOUT_SECONDS", "60"))


def _run_slug(run_id: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]+", "_", str(run_id or "run")).strip("_")[:48] or "run"


def _safe_sql_name(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_]+", "_", str(value or "")).strip("_").lower()
    return cleaned or "unknown"


def _resolve_sftp_bronze_results(state: Stage01State) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    bronze_results = state.get("bronze_generation_results") or []
    if isinstance(bronze_results, list):
        results.extend([item for item in bronze_results if isinstance(item, dict)])
    return results


def _resolve_bronze_table(bronze_result: Dict[str, Any], bronze_schema: str) -> str:
    """
    Resolve the actual Bronze table name from the Bronze generation result.
    The Bronze generator uses pattern: {schema}.{vendor}_{entity}_raw
    NOT: {schema}.bronze_{entity}
    """
    bronze_config = bronze_result.get("bronze_config") or {}
    target_table = bronze_config.get("target_table") or bronze_result.get("target_table")
    if target_table and str(target_table).strip():
        return str(target_table).strip()

    vendor = _safe_sql_name(str(bronze_result.get("vendor") or "vendor1"))
    entity = _safe_sql_name(str(bronze_result.get("entity") or "unknown"))
    return f"{bronze_schema}.{vendor}_{entity}_raw"


def _resolve_bronze_columns(bronze_result: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Get the actual Bronze column names + types from bronze_config.
    These are the FLATTENED names (e.g. RemoteMessage_TransactionID).
    """
    script_body = bronze_result.get("generated_bronze_script") or ""
    if script_body and "EXPECTED_COLUMNS" in script_body:
        try:
            match = re.search(r"EXPECTED_COLUMNS\s*=\s*(\[.*?\])", script_body, re.DOTALL)
            if match:
                columns_list = json.loads(match.group(1).replace("'", '"'))
                types_match = re.search(r"EXPECTED_TYPES\s*=\s*(\{.*?\})", script_body, re.DOTALL)
                types_dict = {}
                if types_match:
                    types_dict = json.loads(types_match.group(1).replace("'", '"'))
                return [
                    {"name": col, "type": types_dict.get(col, "string")}
                    for col in columns_list
                ]
        except (json.JSONDecodeError, AttributeError):
            pass

    bronze_config = bronze_result.get("bronze_config") or bronze_result.get("generated_bronze_config") or {}
    schema_columns = bronze_config.get("schema_columns") or []
    return [
        {
            "name": str(col.get("column_name") or "").strip(),
            "type": str(col.get("data_type") or "string").lower(),
        }
        for col in schema_columns
        if str(col.get("column_name") or "").strip()
    ]


def _resolve_primary_keys(bronze_result: Dict[str, Any]) -> List[str]:
    bronze_config = bronze_result.get("bronze_config") or {}
    keys = bronze_config.get("primary_keys") or bronze_result.get("primary_keys") or []
    return [str(k).strip() for k in keys if str(k).strip()]


def _resolve_watermark_column(bronze_result: Dict[str, Any]) -> Optional[str]:
    bronze_config = bronze_result.get("bronze_config") or {}
    wm = bronze_config.get("watermark_column") or bronze_result.get("watermark_column")
    return str(wm).strip() if wm else None


def _resolve_pii_columns(state: Stage01State) -> List[str]:
    enriched = state.get("enriched_metadata") or {}
    if not isinstance(enriched, dict):
        return []
    return [
        str(col.get("column_name") or "").strip()
        for col in (enriched.get("columns") or [])
        if col.get("is_pii") and str(col.get("column_name") or "").strip()
    ]


def _business_column_name(bronze_col: str, row_tag: str = "RemoteMessage") -> str:
    """
    Convert Bronze flattened column name to business-friendly Silver name.
    RemoteMessage_TransactionID -> transaction_id
    RemoteMessage__CustomerCode -> customer_code
    RemoteMessage_Details__Currency -> details_currency
    """
    name = bronze_col
    prefix = f"{row_tag}_"
    if name.startswith(prefix):
        name = name[len(prefix):]

    name = re.sub(r"__+", "_", name)
    name = name.lstrip("_")
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    return name.lower().strip("_")


def _script_output_path(entity: str) -> str:
    os.makedirs(SILVER_OUTPUT_DIR, exist_ok=True)
    return os.path.join(SILVER_OUTPUT_DIR, f"silver_transform_{_run_slug(entity)}.py")


def _build_silver_script(
    entity: str,
    vendor: str,
    bronze_table: str,
    silver_schema: str,
    bronze_columns: List[Dict[str, str]],
    primary_keys: List[str],
    watermark_column: Optional[str],
    pii_columns: List[str],
    row_tag: str = "RemoteMessage",
) -> str:
    vendor_safe = _safe_sql_name(vendor)
    entity_safe = _safe_sql_name(entity)
    silver_table = f"{silver_schema}.{vendor_safe}_{entity_safe}_clean"

    column_renames: List[tuple] = []
    for col in bronze_columns:
        bronze_name = col["name"]
        silver_name = _business_column_name(bronze_name, row_tag)
        col_type = col["type"]
        column_renames.append((bronze_name, silver_name, col_type))

    select_lines = []
    for bronze_name, silver_name, col_type in column_renames:
        select_lines.append(
            f'    F.col("`{bronze_name}`").cast("{col_type}").alias("{silver_name}")'
        )
    select_block = ",\n".join(select_lines)

    dedup_keys_silver = []
    for pk in primary_keys:
        for bronze_name, silver_name, _ in column_renames:
            if bronze_name == pk or pk.lower() in bronze_name.lower():
                dedup_keys_silver.append(silver_name)
                break

    if dedup_keys_silver:
        partition_cols = ", ".join([f'"{k}"' for k in dedup_keys_silver])
        dedup_block = f"""
# ============================================================
# 3. Deduplicate on primary keys (keep latest by ingestion time)
# ============================================================
from pyspark.sql.window import Window

_dedup_window = Window.partitionBy({partition_cols}).orderBy(F.col("_ingestion_timestamp").desc())
df = df.withColumn("_row_num", F.row_number().over(_dedup_window)).filter(F.col("_row_num") == 1).drop("_row_num")
print(f"After dedup: {{df.count():,}} rows")
"""
    else:
        dedup_block = """
# ============================================================
# 3. Deduplicate (no primary keys - drop exact duplicates)
# ============================================================
df = df.dropDuplicates()
print(f"After dedup: {{df.count():,}} rows")
"""

    pii_block = ""
    if pii_columns:
        pii_lines = []
        for pii_col in pii_columns:
            for bronze_name, silver_name, _ in column_renames:
                if pii_col.lower() in bronze_name.lower():
                    pii_lines.append(f'    .withColumn("{silver_name}", F.sha2(F.col("{silver_name}").cast("string"), 256))')
                    break
        if pii_lines:
            pii_mask_code = "\n".join(pii_lines)
            pii_block = f"""
# ============================================================
# 4. PII Masking (SHA-256 hash)
# ============================================================
df = (
    df
{pii_mask_code}
)
print("PII columns masked.")
"""

    incremental_block = ""
    if watermark_column:
        incremental_block = f"""
# ============================================================
# 1b. Incremental filter (read only new Bronze rows)
# ============================================================
try:
    _last_watermark = spark.sql("SELECT MAX(_silver_processed_at) FROM {silver_table}").first()[0]
    if _last_watermark:
        source_df = source_df.filter(F.col("_ingestion_timestamp") > F.lit(_last_watermark))
        print(f"Incremental: reading rows after {{_last_watermark}}")
except Exception:
    print("No existing Silver table found - full load")
"""

    if dedup_keys_silver:
        merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in dedup_keys_silver])
        write_block = f"""
# ============================================================
# 6. Write Silver table (MERGE upsert)
# ============================================================
df.createOrReplaceTempView("_silver_updates")

spark.sql(\"\"\"CREATE TABLE IF NOT EXISTS {silver_table}
    USING DELTA
    AS SELECT * FROM _silver_updates WHERE 1=0
\"\"\")

spark.sql(\"\"\"MERGE INTO {silver_table} AS target
    USING _silver_updates AS source
    ON {merge_condition}
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
\"\"\")

row_count = spark.table("{silver_table}").count()
print(f"Silver transformation completed: {silver_table}")
print(f"Total rows in Silver: {{row_count:,}}")
"""
    else:
        write_block = f"""
# ============================================================
# 6. Write Silver table (overwrite - no primary keys for merge)
# ============================================================
(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("{silver_table}")
)

row_count = spark.table("{silver_table}").count()
print(f"Silver transformation completed: {silver_table}")
print(f"Total rows in Silver: {{row_count:,}}")
"""

    script = f'''from pyspark.sql import functions as F

# ============================================================
# Silver Transformation: {vendor}/{entity}
# ============================================================

SOURCE_TABLE = "{bronze_table}"
TARGET_TABLE = "{silver_table}"

print(f"Starting Silver transformation for {{SOURCE_TABLE}} -> {{TARGET_TABLE}}")

# ============================================================
# 1. Read Bronze source table
# ============================================================
source_df = spark.table(SOURCE_TABLE)

if source_df.limit(1).count() == 0:
    raise ValueError(f"Bronze source table is empty: {{SOURCE_TABLE}}")

print(f"Bronze source rows: {{source_df.count():,}}")
{incremental_block}
# ============================================================
# 2. Select, rename, and cast columns to business-friendly names
# ============================================================
df = source_df.select(
{select_block}
)
{dedup_block}{pii_block}
# ============================================================
# 5. Data quality checks
# ============================================================
_total_rows = df.count()
_null_counts = df.select([F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns]).first()

print(f"Total rows: {{_total_rows:,}}")
_dq_issues = {{col: count for col, count in _null_counts.asDict().items() if count and count > 0}}
if _dq_issues:
    print(f"Columns with NULLs: {{_dq_issues}}")
else:
    print("No NULL values detected.")

# Add Silver audit columns
df = (
    df
    .withColumn("_silver_processed_at", F.current_timestamp())
    .withColumn("_bronze_source_table", F.lit(SOURCE_TABLE))
)
{write_block}'''

    return script.strip()


def _llm_prompt(code: str, entity: str, bronze_table: str, silver_table: str) -> str:
    return f"""
You are a senior Spark data engineer. Improve this Silver transformation script.

Entity: {entity}
Bronze table: {bronze_table}
Silver table: {silver_table}

Requirements:
- Preserve the same source and target tables.
- Use PySpark and Delta.
- Do NOT use .rdd (not available on serverless Spark Connect).
- Do NOT use SparkSession.builder.getOrCreate() (already available).
- Add data quality checks where appropriate.
- Keep merge/upsert logic if present.
- Return only valid Python code.

Current script:
{code}
""".strip()


def _enhance_with_llm(code: str, entity: str, bronze_table: str, silver_table: str) -> str:
    if not SILVER_LLM_ENABLED:
        return code

    provider = os.getenv("ATHENA_LLM_PROVIDER", "azure_openai")
    model = os.getenv("ATHENA_SFTP_SILVER_LLM_MODEL")
    llm = get_llm(provider=provider, model=model, temperature=0.0, request_timeout=SILVER_LLM_TIMEOUT_SECONDS)
    prompt = _llm_prompt(code, entity, bronze_table, silver_table)
    response = llm.invoke([
        SystemMessage(content="You are a senior Spark data engineer. Return only valid Python code. Do NOT use .rdd or SparkSession.builder.getOrCreate()."),
        HumanMessage(content=prompt),
    ])

    enhanced = str(response.content).strip()

    try:
        compile(enhanced, "<silver_llm_output>", "exec")
    except SyntaxError as exc:
        logger.warning("LLM Silver output failed syntax check: %s", exc, extra={"entity": entity})
        return code

    forbidden = [".rdd", "SparkSession.builder", "spark.sparkContext", "eval(", "exec(", "subprocess"]
    for pattern in forbidden:
        if pattern in enhanced:
            logger.warning("LLM Silver output contains forbidden pattern: %s", pattern, extra={"entity": entity})
            return code

    return enhanced


def _write_bundle(bundle: Dict[str, Any], run_id: str) -> str:
    os.makedirs(SILVER_OUTPUT_DIR, exist_ok=True)
    bundle_path = os.path.join(SILVER_OUTPUT_DIR, f"{_run_slug(run_id)}_silver_scripts.json")
    with open(bundle_path, "w", encoding="utf-8") as f:
        json.dump(bundle, f, indent=2, default=str)
    return bundle_path


def _write_readme(results: List[Dict[str, Any]], generated_at: str) -> str:
    os.makedirs(SILVER_OUTPUT_DIR, exist_ok=True)
    path = os.path.join(SILVER_OUTPUT_DIR, "README.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"# Silver Scripts\n\nGenerated at: {generated_at}\n\n")
        f.write(f"Total scripts: {len(results)}\n\n")
        for item in results:
            f.write(f"- **{item.get('entity')}**: {item.get('status')} ")
            if item.get("llm_enhanced"):
                f.write("(LLM enhanced)")
            f.write("\n")
    return path


def sftp_silver_code_generation_node(state: Stage01State) -> Stage01State:
    new_state = state.copy()
    run_id = str(state.get("run_id") or f"sftp_silver_{datetime.now(timezone.utc).timestamp()}")
    bronze_schema = str(state.get("bronze_schema") or os.getenv("BRONZE_SCHEMA", "bronze"))
    silver_schema = str(state.get("silver_schema") or os.getenv("SILVER_SCHEMA", "silver"))

    bronze_results = _resolve_sftp_bronze_results(state)
    if not bronze_results:
        new_state["silver_generation_status"] = "SKIPPED"
        new_state["silver_generation_error"] = "No Bronze generation results available for Silver generation."
        return new_state

    seen_entities = set()
    unique_results = []
    for result in bronze_results:
        entity = str(result.get("entity") or "").strip().lower()
        if entity and entity not in seen_entities:
            seen_entities.add(entity)
            unique_results.append(result)

    if not unique_results:
        new_state["silver_generation_status"] = "SKIPPED"
        new_state["silver_generation_error"] = "Unable to resolve Bronze entities for Silver generation."
        return new_state

    pii_columns = _resolve_pii_columns(state)

    results: List[Dict[str, Any]] = []
    max_workers = int(os.getenv("ATHENA_SFTP_SILVER_PLAN_WORKERS", "4"))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                _generate_one_entity,
                bronze_result=result,
                run_id=run_id,
                bronze_schema=bronze_schema,
                silver_schema=silver_schema,
                pii_columns=pii_columns,
                state=state,
            )
            for result in unique_results
        ]
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as exc:
                logger.error("Silver generation failed: %s", exc)
                results.append({"run_id": run_id, "entity": "unknown", "status": "FAILED", "error": str(exc)})

    generated_at = datetime.now(timezone.utc).isoformat()
    bundle = {
        "run_id": run_id,
        "fingerprint": str(state.get("fingerprint") or run_id),
        "generated_at": generated_at,
        "script_count": len(results),
        "scripts": results,
    }
    bundle_path = _write_bundle(bundle, run_id)
    readme_path = _write_readme(results, generated_at)

    ai_store_db_writer(
        run_id=run_id,
        stage="SFTP Silver Code Generation",
        artifact_type="SFTP_SILVER_GENERATION",
        payload=bundle,
        schema_version="SFTP_SILVER_GENERATION_v2",
        prompt_version="SFTP_SILVER_v2",
        faithfulness_status="PASSED" if all(r.get("status") == "COMPLETED" for r in results) else "NEEDS_REVIEW",
        token_count=0,
        input_tokens=0,
        output_tokens=0,
    )

    new_state.update({
        "silver_generation_status": "COMPLETED" if all(r.get("status") == "COMPLETED" for r in results) else "PARTIAL",
        "silver_generation_error": None,
        "silver_generated_at": generated_at,
        "silver_generation_results": results,
        "silver_generation_bundle_path": bundle_path,
        "silver_generation_readme_path": readme_path,
        "silver_review_artifact": {
            "run_id": run_id,
            "generated_at": generated_at,
            "items": [
                {
                    "entity": item.get("entity"),
                    "vendor": item.get("vendor"),
                    "bronze_source": item.get("bronze_table"),
                    "silver_target": item.get("silver_table"),
                    "primary_keys": item.get("primary_keys"),
                    "watermark_column": item.get("watermark_column"),
                    "transformations": [
                        "column rename (bronze -> business names)",
                        "type casting",
                        "deduplication",
                        "null audit",
                        "silver audit columns",
                    ],
                    "pii_masking_rules": [f"SHA-256 hash on {col}" for col in pii_columns] if pii_columns else [],
                    "merge_strategy": "MERGE upsert" if item.get("primary_keys") else "overwrite",
                    "llm_enhanced": item.get("llm_enhanced", False),
                    "generated_silver_script": Path(item["script_path"]).read_text(encoding="utf-8")
                    if item.get("script_path") and os.path.exists(item.get("script_path", ""))
                    else "",
                }
                for item in results
                if item.get("status") == "COMPLETED"
            ],
        },
    })
    return new_state


def _generate_one_entity(
    bronze_result: Dict[str, Any],
    run_id: str,
    bronze_schema: str,
    silver_schema: str,
    pii_columns: List[str],
    state: Stage01State,
) -> Dict[str, Any]:
    entity = str(bronze_result.get("entity") or "unknown").strip()
    vendor = str(bronze_result.get("vendor") or "Vendor1").strip()

    bronze_table = _resolve_bronze_table(bronze_result, bronze_schema)
    bronze_columns = _resolve_bronze_columns(bronze_result)
    primary_keys = _resolve_primary_keys(bronze_result)
    watermark_column = _resolve_watermark_column(bronze_result)

    bronze_config = bronze_result.get("bronze_config") or {}
    row_tag = str(bronze_config.get("row_tag") or "RemoteMessage")

    script_path = _script_output_path(entity)

    code = _build_silver_script(
        entity=entity,
        vendor=vendor,
        bronze_table=bronze_table,
        silver_schema=silver_schema,
        bronze_columns=bronze_columns,
        primary_keys=primary_keys,
        watermark_column=watermark_column,
        pii_columns=pii_columns,
        row_tag=row_tag,
    )

    llm_error = None
    llm_enhanced = False

    try:
        silver_table = f"{silver_schema}.{_safe_sql_name(vendor)}_{_safe_sql_name(entity)}_clean"
        enhanced = _enhance_with_llm(code, entity, bronze_table, silver_table)
        if enhanced and enhanced != code:
            code = enhanced
            llm_enhanced = True
    except Exception as exc:
        llm_error = str(exc)
        logger.warning("Silver LLM enhancement failed: %s", exc, extra={"run_id": run_id, "node": "sftp_silver_code_generation"})

    with open(script_path, "w", encoding="utf-8") as f:
        f.write(code)

    return {
        "run_id": run_id,
        "entity": entity,
        "vendor": vendor,
        "bronze_table": bronze_table,
        "silver_table": f"{silver_schema}.{_safe_sql_name(vendor)}_{_safe_sql_name(entity)}_clean",
        "script_path": script_path,
        "llm_enhanced": llm_enhanced,
        "llm_error": llm_error,
        "primary_keys": primary_keys,
        "watermark_column": watermark_column,
        "bronze_columns_count": len(bronze_columns),
        "status": "COMPLETED",
    }
