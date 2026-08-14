from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List

from services.metadata_contracts import normalize_bronze_column_name
from utilis.logger import logger


def _logical_table(value: Any) -> str:
    name = str(value or "").split(".")[-1].strip('"').casefold()
    return name.removeprefix("silver_")


def _safe_identifier(value: Any, fallback: str) -> str:
    name = re.sub(r"[^a-zA-Z0-9_]+", "_", str(value or "").strip().lower())
    name = re.sub(r"_+", "_", name).strip("_") or fallback
    return f"{fallback}_{name}" if name[0].isdigit() else name


def _validated_identifier(value: Any, label: str) -> str:
    name = str(value or "").strip()
    if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*){1,2}", name):
        raise ValueError(f"ADLS factless Gold {label} is not a qualified identifier: {name!r}")
    return name


def _dedupe(values: Iterable[Any]) -> List[str]:
    seen = set()
    result = []
    for value in values:
        name = _safe_identifier(value, "column")
        if name not in seen:
            seen.add(name)
            result.append(name)
    return result


def _canonical_columns(values: Iterable[Any]) -> List[str]:
    return _dedupe(normalize_bronze_column_name(value) for value in values)


def _factless_script(
    *, run_id: str, kpi_name: str, source_table: str, target_table: str,
    grain_columns: List[str],
) -> str:
    return f'''# Generated ADLS Gold KPI fact.
from delta.tables import DeltaTable
from pyspark.sql import functions as F

RUN_ID = {run_id!r}
KPI_NAME = {kpi_name!r}
SOURCE_TABLE = {source_table!r}
TARGET_TABLE = {target_table!r}
GRAIN_COLUMNS = {grain_columns!r}

source_columns = set(spark.table(SOURCE_TABLE).columns)
missing = [column for column in GRAIN_COLUMNS if column not in source_columns]
if missing:
    raise ValueError(f"Factless Gold grain columns are missing from {{SOURCE_TABLE}}: {{missing}}")

factless = (
    spark.table(SOURCE_TABLE)
    .select(*GRAIN_COLUMNS)
    .dropna(subset=GRAIN_COLUMNS)
    .dropDuplicates(GRAIN_COLUMNS)
    .withColumn("kpi_name", F.lit(KPI_NAME))
    .withColumn("pipeline_run_id", F.lit(RUN_ID))
    .withColumn("gold_processed_timestamp", F.current_timestamp())
)

merge_condition = " AND ".join(
    [f"target.`{{column}}` = source.`{{column}}`" for column in GRAIN_COLUMNS]
    + ["target.kpi_name = source.kpi_name", "target.pipeline_run_id = source.pipeline_run_id"]
)

if spark.catalog.tableExists(TARGET_TABLE):
    (
        DeltaTable.forName(spark, TARGET_TABLE).alias("target")
        .merge(factless.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    factless.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)
'''


def _dimension_script(
    *, run_id: str, source_table: str, target_table: str,
    grain_columns: List[str], attributes: List[str],
) -> str:
    selected = _dedupe([*grain_columns, *attributes])
    return f'''# Generated ADLS Gold dimension with deterministic version-preserving rows.
from delta.tables import DeltaTable
from pyspark.sql import functions as F

RUN_ID = {run_id!r}
SOURCE_TABLE = {source_table!r}
TARGET_TABLE = {target_table!r}
GRAIN_COLUMNS = {grain_columns!r}
SELECTED_COLUMNS = {selected!r}

source_columns = set(spark.table(SOURCE_TABLE).columns)
missing = [column for column in SELECTED_COLUMNS if column not in source_columns]
if missing:
    raise ValueError(f"Gold dimension columns are missing from {{SOURCE_TABLE}}: {{missing}}")

dimension = (
    spark.table(SOURCE_TABLE)
    .select(*SELECTED_COLUMNS)
    .dropna(subset=GRAIN_COLUMNS)
    .dropDuplicates(SELECTED_COLUMNS)
    .withColumn(
        "dimension_row_hash",
        F.sha2(F.concat_ws("||", *[F.coalesce(F.col(column).cast("string"), F.lit("<NULL>")) for column in SELECTED_COLUMNS]), 256),
    )
    .withColumn("pipeline_run_id", F.lit(RUN_ID))
    .withColumn("gold_processed_timestamp", F.current_timestamp())
)

if spark.catalog.tableExists(TARGET_TABLE):
    (
        DeltaTable.forName(spark, TARGET_TABLE).alias("target")
        .merge(dimension.alias("source"), "target.dimension_row_hash = source.dimension_row_hash")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    dimension.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)
'''


def _snowflake_relation(value: Any) -> str:
    return ".".join(f'"{part}"' for part in _validated_identifier(value, "relation").split("."))


def _snowflake_fact_select(
    *, run_id: str, kpi_name: str, source_table: str, grain_columns: List[str]
) -> str:
    grain = ",\n    ".join(f'"{column}"' for column in grain_columns)
    return f'''SELECT DISTINCT
    {grain},
    {shared_sql_literal(kpi_name)} AS "kpi_name",
    {shared_sql_literal(run_id)} AS "pipeline_run_id",
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS "gold_processed_timestamp"
FROM {_snowflake_relation(source_table)}
WHERE {" AND ".join(f'"{column}" IS NOT NULL' for column in grain_columns)}'''


def _snowflake_dimension_select(
    *, run_id: str, source_table: str, grain_columns: List[str], attributes: List[str]
) -> str:
    columns = _dedupe([*grain_columns, *attributes])
    selected = ",\n    ".join(f'"{column}"' for column in columns)
    return f'''SELECT DISTINCT
    {selected},
    {shared_sql_literal(run_id)} AS "pipeline_run_id",
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS "gold_processed_timestamp"
FROM {_snowflake_relation(source_table)}
WHERE {" AND ".join(f'"{column}" IS NOT NULL' for column in grain_columns)}'''


def shared_sql_literal(value: Any) -> str:
    return "'" + str(value or "").replace("'", "''") + "'"


def _snowflake_identifier(value: Any, label: str) -> str:
    name = str(value or "").strip()
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"ADLS Snowflake Gold {label} is not a valid identifier: {name!r}")
    return name


def _snowflake_native_script(
    *, source_table: str, target_table: str, select_sql: str, key_columns: List[str]
) -> str:
    target = _snowflake_relation(target_table)
    source = _snowflake_relation(source_table)
    target_schema = ".".join(target.split(".")[:-1])
    keys = [*key_columns, "pipeline_run_id"]
    merge_on = " AND ".join(f'target."{key}" = source."{key}"' for key in keys)
    return f'''CREATE SCHEMA IF NOT EXISTS {target_schema};
CREATE TABLE IF NOT EXISTS {target} AS
SELECT * FROM ({select_sql}) WHERE 1 = 0;
MERGE INTO {target} AS target
USING ({select_sql}) AS source
ON {merge_on}
WHEN MATCHED THEN UPDATE ALL BY NAME
WHEN NOT MATCHED THEN INSERT ALL BY NAME;'''


def _snowflake_gold_fallbacks(
    state: Dict[str, Any], generated: Dict[str, Any], *, dbt_codegen: bool
) -> Dict[str, Any]:
    from nodes import gold_gen as shared

    contract = dict(state.get("gold_generation_contract") or {})
    run_id = str(state.get("run_id") or contract.get("run_id") or "GOLD_RUN")
    catalog = _snowflake_identifier(
        state.get("gold_catalog") or os.getenv("SNOWFLAKE_GOLD_CATALOG", "INSURANCE"), "database"
    )
    schema = _snowflake_identifier(
        state.get("gold_schema") or os.getenv("SNOWFLAKE_GOLD_SCHEMA", "GOLD"), "gold"
    )
    output_dir = shared._gold_output_dir_for("snowflake")
    os.makedirs(output_dir, exist_ok=True)
    results = [dict(item) for item in generated.get("gold_generation_results") or [] if isinstance(item, dict)]
    factless_by_table = {
        _logical_table(item.get("logical_table") or item.get("source_silver_table")): dict(item)
        for item in contract.get("factless_mappings") or [] if isinstance(item, dict)
    }
    mappings = {
        str(item.get("kpi_name") or "").casefold(): dict(item)
        for item in contract.get("kpi_mappings") or []
        if isinstance(item, dict) and str(item.get("kpi_name") or "").strip()
    }
    if not results:
        results = [
            {"kpi_name": item.get("kpi_name"), "status": "BLOCKED"}
            for item in contract.get("kpi_mappings") or [] if isinstance(item, dict)
        ]

    final_results: List[Dict[str, Any]] = []
    fact_count = 0
    for result in results:
        if str(result.get("status") or "").upper() != "BLOCKED":
            final_results.append(result)
            continue
        kpi_name = str(result.get("kpi_name") or "KPI")
        mapping = mappings.get(kpi_name.casefold()) or {}
        logical = _logical_table(
            (mapping.get("measure") or {}).get("table")
            or mapping.get("source_silver_table") or result.get("source_table")
        )
        factless = factless_by_table.get(logical) or {}
        grain = _canonical_columns(factless.get("grain_columns") or [])
        source_table = factless.get("source_silver_table") or result.get("source_table")
        if not grain or not source_table:
            final_results.append(result)
            continue
        source_table = _validated_identifier(source_table, "source table")
        model_name = f"fact_{_safe_identifier(kpi_name, 'kpi')}"
        target_table = _validated_identifier(f"{catalog}.{schema}.{model_name}", "target table")
        select_sql = _snowflake_fact_select(
            run_id=run_id, kpi_name=kpi_name, source_table=source_table, grain_columns=grain
        )
        body = select_sql if dbt_codegen else _snowflake_native_script(
            source_table=source_table, target_table=target_table, select_sql=select_sql,
            key_columns=[*grain, "kpi_name"],
        )
        path = os.path.join(output_dir, f"{model_name}.sql")
        with open(path, "w", encoding="utf-8") as file:
            file.write(body)
        final_results.append({
            **result, "run_id": run_id, "status": "APPROVED", "reason": None,
            "source_table": source_table, "target_table": target_table,
            "script_path": path, "script_body": body, "script_language": "sql",
            "target_warehouse": "snowflake", "code_generation_format": "dbt" if dbt_codegen else "native",
            "dbt_model_name": model_name if dbt_codegen else None,
            "dbt_alias": model_name if dbt_codegen else None,
            "artifact_kind": "FACT", "fact_type": "FACTLESS_KPI_EVENT",
            "generation_mode": "ADLS_KPI_FACT_DETERMINISTIC", "grain_columns": grain,
        })
        fact_count += 1

    dimension_count = 0
    targets = {str(item.get("target_table") or "").casefold() for item in final_results}
    for mapping in contract.get("dimension_mappings") or []:
        if not isinstance(mapping, dict):
            continue
        logical = _logical_table(mapping.get("logical_table") or mapping.get("source_silver_table"))
        factless = factless_by_table.get(logical) or {}
        grain = _canonical_columns(factless.get("grain_columns") or [])
        source_table = factless.get("source_silver_table") or mapping.get("source_silver_table")
        attributes = _canonical_columns(mapping.get("columns") or [])
        if not grain or not source_table or not attributes:
            continue
        source_table = _validated_identifier(source_table, "dimension source table")
        model_name = f"dim_{_safe_identifier(logical, 'dimension')}"
        target_table = _validated_identifier(f"{catalog}.{schema}.{model_name}", "dimension target table")
        if target_table.casefold() in targets:
            continue
        targets.add(target_table.casefold())
        select_sql = _snowflake_dimension_select(
            run_id=run_id, source_table=source_table, grain_columns=grain, attributes=attributes
        )
        body = select_sql if dbt_codegen else _snowflake_native_script(
            source_table=source_table, target_table=target_table, select_sql=select_sql,
            key_columns=grain,
        )
        path = os.path.join(output_dir, f"{model_name}.sql")
        with open(path, "w", encoding="utf-8") as file:
            file.write(body)
        final_results.append({
            "run_id": run_id, "kpi_name": f"Dimension: {logical}", "status": "APPROVED",
            "source_table": source_table, "target_table": target_table,
            "script_path": path, "script_body": body, "script_language": "sql",
            "target_warehouse": "snowflake", "code_generation_format": "dbt" if dbt_codegen else "native",
            "dbt_model_name": model_name if dbt_codegen else None,
            "dbt_alias": model_name if dbt_codegen else None,
            "artifact_kind": "DIMENSION", "generation_mode": "ADLS_DIMENSION_DETERMINISTIC",
            "grain_columns": grain,
        })
        dimension_count += 1

    if not fact_count and not dimension_count:
        return generated
    fallback = {**generated, "gold_generation_results": final_results}
    if dbt_codegen:
        from services.dbt_snowflake_runtime import build_snowflake_dbt_artifacts
        fallback = {**fallback, **build_snowflake_dbt_artifacts({**state, **fallback})}
    return {
        **fallback, "status": "HITL_WAIT", "gold_generation_status": "COMPLETED",
        "gold_generation_error": None, "adls_factless_fact_count": fact_count,
        "adls_dimension_count": dimension_count, "resume_message": None,
    }


def gold_code_generation_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """Add safe ADLS factless facts without changing database Gold generation."""
    from nodes import gold_gen as shared

    if str(state.get("source") or "").lower() != "adls_gen2":
        return shared.gold_code_generation_node(state)
    target = str(state.get("target_warehouse") or "databricks").lower()
    if target == "snowflake":
        contract = dict(state.get("gold_generation_contract") or {})
        if contract:
            # Bronze and Silver already materialize canonical aliases. Reuse the
            # shared normalizer before either native or dbt Gold consumes them.
            contract["kpi_mappings"] = shared._normalize_contract_mappings(
                contract,
                canonicalize_columns=True,
            )
            state = {**state, "gold_generation_contract": contract}
        dbt_codegen = str(state.get("execution_engine") or "native").lower() == "dbt"
        try:
            generated = shared.gold_code_generation_node(state)
        except ValueError as exc:
            if not dbt_codegen or "no approved Gold target tables" not in str(exc):
                raise
            generated = {"gold_generation_results": [], "gold_generation_status": "FAILED"}
        return _snowflake_gold_fallbacks(state, generated, dbt_codegen=dbt_codegen)
    if target != "databricks":
        return shared.gold_code_generation_node(state)

    generated = shared.gold_code_generation_node(state)
    contract = dict(state.get("gold_generation_contract") or {})
    results = [dict(item) for item in generated.get("gold_generation_results") or [] if isinstance(item, dict)]
    if not results:
        return generated

    run_id = str(state.get("run_id") or contract.get("run_id") or "GOLD_RUN")
    gold_schema = _safe_identifier(state.get("gold_schema") or os.getenv("GOLD_SCHEMA", "gold"), "gold")
    output_dir = shared._gold_output_dir_for("databricks")
    os.makedirs(output_dir, exist_ok=True)
    factless_by_table = {
        _logical_table(item.get("logical_table") or item.get("source_silver_table")): dict(item)
        for item in contract.get("factless_mappings") or []
        if isinstance(item, dict)
    }
    kpi_by_name = {
        str(item.get("kpi_name") or "").casefold(): dict(item)
        for item in contract.get("kpi_mappings") or []
        if isinstance(item, dict) and str(item.get("kpi_name") or "").strip()
    }

    fallback_count = 0
    final_results = []
    for result in results:
        if str(result.get("status") or "").upper() != "BLOCKED":
            final_results.append(result)
            continue
        kpi_name = str(result.get("kpi_name") or "KPI")
        mapping = kpi_by_name.get(kpi_name.casefold()) or {}
        logical = _logical_table(
            (mapping.get("measure") or {}).get("table")
            or mapping.get("source_silver_table")
            or result.get("source_table")
        )
        factless = factless_by_table.get(logical) or {}
        grain = _canonical_columns(factless.get("grain_columns") or [])
        source_table = factless.get("source_silver_table") or result.get("source_table")
        if not grain or not source_table:
            final_results.append(result)
            continue
        source_table = _validated_identifier(source_table, "source table")
        kpi_id = _safe_identifier(kpi_name, "kpi")
        target_table = _validated_identifier(f"{gold_schema}.fact_{kpi_id}", "target table")
        code = _factless_script(
            run_id=run_id,
            kpi_name=kpi_name,
            source_table=source_table,
            target_table=target_table,
            grain_columns=grain,
        )
        compile(code, f"fact_{kpi_id}.py", "exec")
        path = os.path.join(output_dir, f"gold_fact_{shared._run_slug(run_id)}_{kpi_id}.py")
        with open(path, "w", encoding="utf-8") as file:
            file.write(code)
        final_results.append({
            **result,
            "status": "APPROVED",
            "reason": None,
            "source_table": source_table,
            "target_table": target_table,
            "script_path": path,
            "script_body": code,
            "script_language": "python",
            "artifact_kind": "FACT",
            "fact_type": "FACTLESS_KPI_EVENT",
            "generation_mode": "ADLS_KPI_FACT_DETERMINISTIC",
            "grain_columns": grain,
        })
        fallback_count += 1

    dimension_count = 0
    dimension_targets = set()
    for mapping in contract.get("dimension_mappings") or []:
        if not isinstance(mapping, dict):
            continue
        logical = _logical_table(mapping.get("logical_table") or mapping.get("source_silver_table"))
        factless = factless_by_table.get(logical) or {}
        grain = _canonical_columns(factless.get("grain_columns") or [])
        source_table = factless.get("source_silver_table") or mapping.get("source_silver_table")
        attributes = _canonical_columns(mapping.get("columns") or [])
        if not grain or not source_table or not attributes:
            continue
        source_table = _validated_identifier(source_table, "dimension source table")
        target_table = _validated_identifier(f"{gold_schema}.dim_{_safe_identifier(logical, 'dimension')}", "dimension target table")
        if target_table in dimension_targets:
            continue
        dimension_targets.add(target_table)
        # ponytail: retain natural-key grain until the contract certifies
        # cross-feed joins; inventing surrogate relationships would corrupt facts.
        code = _dimension_script(
            run_id=run_id,
            source_table=source_table,
            target_table=target_table,
            grain_columns=grain,
            attributes=attributes,
        )
        compile(code, f"dim_{logical}.py", "exec")
        path = os.path.join(output_dir, f"gold_dimension_{shared._run_slug(run_id)}_{_safe_identifier(logical, 'dimension')}.py")
        with open(path, "w", encoding="utf-8") as file:
            file.write(code)
        final_results.append({
            "run_id": run_id,
            "kpi_name": f"Dimension: {logical}",
            "status": "APPROVED",
            "source_table": source_table,
            "target_table": target_table,
            "script_path": path,
            "script_body": code,
            "script_language": "python",
            "target_warehouse": "databricks",
            "code_generation_format": "native",
            "artifact_kind": "DIMENSION",
            "generation_mode": "ADLS_DIMENSION_DETERMINISTIC",
            "grain_columns": grain,
        })
        dimension_count += 1

    if not fallback_count:
        return generated

    generated_at = datetime.now(timezone.utc).isoformat()
    bundle = {
        "generated_at": generated_at,
        "script_count": sum(1 for item in final_results if item.get("script_path")),
        "dimension_script_count": dimension_count,
        "blocked_count": sum(1 for item in final_results if item.get("status") == "BLOCKED"),
        "contract_status": contract.get("status"),
        "target_warehouse": "databricks",
        "llm_enabled": False,
        "scripts": final_results,
    }
    bundle_path = shared._write_bundle(
        generated_at=generated_at,
        results=final_results,
        contract=contract,
        target_warehouse="databricks",
    )
    readme_path = shared._write_readme(
        generated_at=generated_at,
        results=final_results,
        target_warehouse="databricks",
    )
    ui_path = shared._write_ui(
        generated_at=generated_at,
        results=final_results,
        target_warehouse="databricks",
    )
    try:
        shared._persist_gold_generation(state=state, bundle=bundle)
    except Exception as exc:
        logger.warning(
            "ADLS factless Gold artifact persistence failed: %s",
            exc,
            extra={"run_id": run_id, "node": "adls_gold_generation"},
        )
    blocked_count = bundle["blocked_count"]
    status = "COMPLETED_WITH_WARNINGS" if blocked_count else "COMPLETED"
    logger.info(
        "ADLS Gold generation completed: %d KPI facts, %d dimensions, %d blocked",
        fallback_count,
        dimension_count,
        blocked_count,
        extra={"run_id": run_id, "node": "adls_gold_generation"},
    )
    return {
        **generated,
        "status": "PIPELINE_COMPLETED",
        "gold_generation_status": status,
        "gold_generation_error": (
            f"Generated {fallback_count} KPI factless fact(s); {blocked_count} mapping(s) remain blocked."
            if blocked_count
            else None
        ),
        "gold_generated_at": generated_at,
        "gold_generation_results": final_results,
        "gold_generation_bundle_path": bundle_path,
        "gold_generation_readme_path": readme_path,
        "gold_generation_ui_path": ui_path,
        "adls_factless_fact_count": fallback_count,
        "adls_dimension_count": dimension_count,
        "resume_message": None,
    }
