from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from langchain_core.messages import HumanMessage, SystemMessage

from nodes.req_extraction import get_llm
from state import Stage01State
from utilis.ai_store_writer import ai_store_db_writer
from utilis.logger import logger

GOLD_OUTPUT_DIR = os.path.join(os.getcwd(), "generated_code", "gold")
GOLD_LLM_ENABLED = os.getenv("ATHENA_ENABLE_LLM_SFTP_GOLD", "false").lower() in {"1", "true", "yes", "on"}
GOLD_LLM_TIMEOUT_SECONDS = int(os.getenv("ATHENA_SFTP_GOLD_LLM_TIMEOUT_SECONDS", "60"))


def _run_slug(run_id: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]+", "_", str(run_id or "run")).strip("_")[:48] or "run"


def _resolve_sftp_silver_inputs(state: Stage01State) -> List[Dict[str, str]]:
    inputs: List[Dict[str, str]] = []
    silver_results = state.get("silver_generation_results") or []
    for item in silver_results:
        if isinstance(item, dict):
            entity = str(item.get("entity") or "").strip().lower()
            source_table = str(item.get("silver_table") or item.get("target_table") or "").strip()
            if entity and source_table and not any(existing["entity"] == entity for existing in inputs):
                inputs.append({"entity": entity, "source_table": source_table})
    return inputs


def _script_output_path(entity: str, run_id: str) -> str:
    os.makedirs(GOLD_OUTPUT_DIR, exist_ok=True)
    return os.path.join(GOLD_OUTPUT_DIR, f"gold_fact_{_run_slug(run_id)}_{_run_slug(entity)}.py")


def _spark_col_expr(column_name: str) -> str:
    escaped = str(column_name or "").replace("`", "``").replace("\\", "\\\\").replace('"', '\\"')
    return f'F.col("`{escaped}`")'


def _build_gold_script(source_table: str, target_table: str, columns: List[str]) -> str:
    select_columns = ", ".join([_spark_col_expr(col) for col in columns])
    select_block = f"df.select({select_columns})" if select_columns else "df"

    return f"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import count, current_timestamp

spark = SparkSession.builder.getOrCreate()

SOURCE_TABLE = r\"{source_table}\"
TARGET_TABLE = r\"{target_table}\"

print(f\"Loading Silver table {{SOURCE_TABLE}} for Gold KPI generation\")
df = spark.table(SOURCE_TABLE)
if df.limit(1).count() == 0:
    raise ValueError(f'Silver source table is empty: {{SOURCE_TABLE}}')

summary = {select_block}
result = summary.agg(count('*').alias('record_count')).withColumn('gold_generated_at', current_timestamp())
result.write.format('delta').mode('overwrite').saveAsTable(TARGET_TABLE)
print(f\"Gold script completed for {{TARGET_TABLE}}\")
""".strip()


def _llm_prompt(code: str, entity: str, source_table: str, target_table: str) -> str:
    return f"""
You are a senior Spark data engineer. Improve this Gold KPI script for an SFTP pipeline.

Entity: {entity}
Source Silver table: {source_table}
Target Gold table: {target_table}

Requirements:
- Preserve the same source and target tables.
- Generate valid PySpark code.
- Add metadata columns and incremental Delta write behavior if possible.
- Return only the Python code.

Current script:
{code}
""".strip()


def _enhance_with_llm(code: str, entity: str, source_table: str, target_table: str) -> str:
    if not GOLD_LLM_ENABLED:
        return code

    provider = os.getenv("ATHENA_LLM_PROVIDER", "azure_openai")
    model = os.getenv("ATHENA_SFTP_GOLD_LLM_MODEL")
    llm = get_llm(provider=provider, model=model, temperature=0.0, request_timeout=GOLD_LLM_TIMEOUT_SECONDS)
    prompt = _llm_prompt(code, entity, source_table, target_table)
    response = llm.invoke([SystemMessage(content="You are a senior Spark data engineer. Return only valid Python code."), HumanMessage(content=prompt)])
    return str(response.content).strip()


def _write_bundle(bundle: Dict[str, Any], run_id: str) -> str:
    os.makedirs(GOLD_OUTPUT_DIR, exist_ok=True)
    bundle_path = os.path.join(GOLD_OUTPUT_DIR, f"{_run_slug(run_id)}_gold_scripts.json")
    with open(bundle_path, "w", encoding="utf-8") as f:
        json.dump(bundle, f, indent=2)
    return bundle_path


def _write_readme(results: List[Dict[str, Any]], generated_at: str) -> str:
    os.makedirs(GOLD_OUTPUT_DIR, exist_ok=True)
    path = os.path.join(GOLD_OUTPUT_DIR, "README.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"Gold scripts generated at {generated_at}.\nGenerated scripts: {len(results)}\n")
    return path


def _generate_one_entity(
    entity: str,
    run_id: str,
    source_table: str,
    target_table: str,
    columns: List[str],
) -> Dict[str, Any]:
    script_path = _script_output_path(entity, run_id)
    code = _build_gold_script(source_table=source_table, target_table=target_table, columns=columns)
    llm_error = None
    llm_enhanced = False

    try:
        enhanced = _enhance_with_llm(code, entity, source_table, target_table)
        if enhanced and enhanced != code:
            code = enhanced
            llm_enhanced = True
    except Exception as exc:
        llm_error = str(exc)
        logger.warning("Gold LLM enhancement failed: %s", exc, extra={"run_id": run_id, "node": "sftp_gold_code_generation"})

    with open(script_path, "w", encoding="utf-8") as f:
        f.write(code)

    return {
        "run_id": run_id,
        "entity": entity,
        "source_table": source_table,
        "target_table": target_table,
        "gold_table": target_table,
        "script_path": script_path,
        "llm_enhanced": llm_enhanced,
        "llm_error": llm_error,
        "status": "COMPLETED",
    }


def sftp_gold_code_generation_node(state: Stage01State) -> Stage01State:
    new_state = state.copy()
    run_id = str(state.get("run_id") or f"sftp_gold_{datetime.utcnow().timestamp()}")
    gold_catalog = str(
        state.get("gold_catalog")
        or os.getenv("GOLD_CATALOG")
        or state.get("silver_catalog")
        or os.getenv("SILVER_CATALOG")
        or state.get("bronze_catalog")
        or os.getenv("BRONZE_CATALOG", "workspace")
    )
    gold_schema = str(state.get("gold_schema") or os.getenv("GOLD_SCHEMA", "gold"))
    gold_namespace = gold_schema if "." in gold_schema else f"{gold_catalog}.{gold_schema}"

    silver_inputs = _resolve_sftp_silver_inputs(state)
    if not silver_inputs:
        new_state["gold_generation_status"] = "SKIPPED"
        new_state["gold_generation_error"] = "No executed Silver table mappings are available for Gold generation."
        return new_state

    columns: List[str] = []
    enriched = state.get("enriched_metadata") or {}
    if isinstance(enriched, dict):
        for column in enriched.get("columns", []) or []:
            name = str(column.get("column_name") or "").strip()
            if name and name not in columns:
                columns.append(name)

    results: List[Dict[str, Any]] = []
    for item in silver_inputs:
        entity = item["entity"]
        results.append(
            _generate_one_entity(
                entity,
                run_id,
                item["source_table"],
                f"{gold_namespace}.gold_{entity}",
                columns,
            )
        )

    generated_at = datetime.utcnow().isoformat()
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
        stage="SFTP Gold Code Generation",
        artifact_type="SFTP_GOLD_GENERATION",
        payload=bundle,
        schema_version="SFTP_GOLD_GENERATION_v1",
        prompt_version="SFTP_GOLD_v1",
        faithfulness_status="PASSED",
        token_count=0,
        input_tokens=0,
        output_tokens=0,
    )

    new_state.update({
        "gold_generation_status": "COMPLETED",
        "gold_generation_error": None,
        "gold_catalog": gold_catalog,
        "gold_schema": gold_schema,
        "gold_generated_at": generated_at,
        "gold_generation_results": results,
        "gold_generation_bundle_path": bundle_path,
        "gold_generation_readme_path": readme_path,
    })
    return new_state
