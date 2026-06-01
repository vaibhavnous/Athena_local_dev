"""
Silver Code Generation (POC MODE)

Generates standalone Databricks/Spark scripts from generated bronze metadata and
semantic enrichment. In demo mode, generated bronze scripts are treated as proof
that bronze tables exist.
"""

from __future__ import annotations

import ast
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, TypedDict

from state import Stage01State
from utilis.db import ai_store_db_writer
from utilis.logger import logger


SILVER_MAX_WORKERS = int(os.environ.get("SILVER_MAX_WORKERS", "4"))


class SilverTableRef(TypedDict):
    database_name: str
    schema_name: str
    table_name: str
    bronze_table: str
    silver_table: str
    existing_script_path: str | None


def _silver_output_dir() -> str:
    return os.path.join(os.getcwd(), "generated_code", "silver")


def _run_slug(run_id: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]+", "_", str(run_id or "run")).strip("_")[:48] or "run"


def _gold_output_dir() -> str:
    return os.path.join(os.getcwd(), "generated_code", "gold")


def _bronze_bundle_path() -> str:
    return os.path.join(os.getcwd(), "generated_code", "bronze", "bronze_scripts.json")


def _silver_readme_path() -> str:
    return os.path.join(_silver_output_dir(), "README.md")


def _silver_ui_path() -> str:
    return os.path.join(_silver_output_dir(), "index.html")


def _validate_python(code: str) -> None:
    compile(code, "<silver_generated>", "exec")
    ast.parse(code)


def _load_bronze_bundle() -> Dict[str, Any]:
    path = _bronze_bundle_path()
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
            }
        )
    return refs


def _table_name_from_ref(item: Dict[str, Any]) -> str:
    return str(item.get("table") or item.get("table_name") or "").strip()


def _resolve_tables_for_silver(state: Stage01State) -> List[SilverTableRef]:
    bronze_results = list(state.get("bronze_generation_results") or [])
    bronze_results.extend(_load_bronze_bundle().get("scripts", []))
    bronze_results.extend(state.get("certified_tables") or [])
    discovered = state.get("discovered_metadata") or {}
    if isinstance(discovered, dict):
        bronze_results.extend(discovered.get("tables", []) or [])

    bronze_schema = str(state.get("bronze_schema") or "bronze")
    silver_schema = str(state.get("silver_schema") or "silver")
    resolved_by_table: Dict[str, SilverTableRef] = {}

    for item in bronze_results:
        if not isinstance(item, dict):
            continue
        table_name = _table_name_from_ref(item)
        if not table_name:
            continue
        script_path = os.path.join(_silver_output_dir(), f"silver_transform_{table_name}.py")
        resolved_by_table[table_name.lower()] = {
            "database_name": str(item.get("database_name") or "insurance"),
            "schema_name": str(item.get("schema_name") or "dbo"),
            "table_name": table_name,
            "bronze_table": f"{bronze_schema}.bronze_{table_name}",
            "silver_table": f"{silver_schema}.silver_{table_name}",
            "existing_script_path": script_path if os.path.exists(script_path) else None,
        }

    for ref in _existing_silver_script_refs(silver_schema):
        resolved_by_table.setdefault(ref["table_name"].lower(), ref)

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


COLUMN_NAME_CORRECTIONS = {
    "rererence_id": "reference_id",
}


def _normalized_column_name(column: Dict[str, Any]) -> str:
    normalized = str(column.get("column_name") or "").strip().lower()
    return COLUMN_NAME_CORRECTIONS.get(normalized, normalized)


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
    key_columns = [
        _normalized_column_name(column)
        for column in enriched_columns
        if column.get("is_join_key") or str(column.get("semantic_type") or "") in {"ID", "SURROGATE_KEY"}
    ]
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
from pyspark.sql.functions import coalesce, col, concat_ws, current_timestamp, lit, sha2, trim, when

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

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
        f"No expected business columns found in {{SOURCE_TABLE}}. "
        f"Available columns: {{df.columns}}"
    )

metadata_expressions = [col(name) for name in metadata_columns]
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


def _generate_one_table(
    table_ref: SilverTableRef,
    *,
    enriched_metadata: Dict[str, Any],
    run_id: str,
    silver_catalog: str,
    silver_schema: str,
) -> Dict[str, object]:
    table_name = table_ref["table_name"]
    enriched_columns = _columns_for_table(enriched_metadata, table_name)

    code = generate_silver_script(
        table_ref=table_ref,
        enriched_columns=enriched_columns,
        run_id=run_id,
        silver_catalog=silver_catalog,
        silver_schema=silver_schema,
    )
    _validate_python(code)

    output_dir = _silver_output_dir()
    os.makedirs(output_dir, exist_ok=True)
    script_path = os.path.join(output_dir, f"silver_transform_{_run_slug(run_id)}_{table_name}.py")
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
        "status": "APPROVED",
        "script_path": script_path,
    }


def _write_silver_readme(*, results: List[Dict[str, object]], generated_at: str) -> str:
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

    readme_path = _silver_readme_path()
    with open(readme_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    return readme_path


def _write_silver_ui(*, results: List[Dict[str, object]], generated_at: str) -> str:
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

    ui_path = _silver_ui_path()
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
    if any(word in name for word in ("count", "number", "volume")):
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


def _dimension_columns(columns: List[Dict[str, Any]], measure_table: str | None) -> List[Dict[str, str]]:
    dimensions: List[Dict[str, str]] = []
    for column in columns:
        semantic = str(column.get("semantic_type") or "")
        if semantic not in {"DIMENSION", "DATE", "FLAG"}:
            continue
        if measure_table and column.get("table_name") != measure_table and semantic != "DATE":
            continue
        dimensions.append(
            {
                "table": str(column.get("table_name") or ""),
                "column": str(column.get("column_name") or ""),
                "semantic_type": semantic,
            }
        )
    return dimensions[:12]


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


def _build_gold_generation_contract(
    *,
    state: Stage01State,
    results: List[Dict[str, object]],
    enriched_metadata: Dict[str, Any],
    generated_at: str,
) -> Dict[str, Any]:
    columns = enriched_metadata.get("columns", []) if isinstance(enriched_metadata, dict) else []
    joins = enriched_metadata.get("joins", []) if isinstance(enriched_metadata, dict) else []
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
        dimensions = _dimension_columns(columns, measure_table)
        join_paths = _join_paths_for_table(joins, measure_table)

        if not measure:
            warnings.append(f"No measure column mapped for KPI '{kpi_name}'.")
        if measure_table and measure_table.lower() not in silver_tables:
            warnings.append(f"KPI '{kpi_name}' maps to table '{measure_table}', but no silver script is registered for that table.")
        if aggregation == "RATIO":
            warnings.append(f"KPI '{kpi_name}' needs numerator/denominator formula certification before gold SQL is production-safe.")
        if join_paths and not any(path["certified"] for path in join_paths):
            warnings.append(f"KPI '{kpi_name}' has join candidates, but none are certified.")

        kpi_mappings.append(
            {
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
        )

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
        "kpi_mappings": kpi_mappings,
        "available_joins": joins,
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
        "scripts": results,
    }

    os.makedirs(_silver_output_dir(), exist_ok=True)
    bundle_path = os.path.join(_silver_output_dir(), f"{_run_slug(run_id)}_silver_scripts.json")
    latest_bundle_path = os.path.join(_silver_output_dir(), "silver_scripts.json")
    with open(bundle_path, "w", encoding="utf-8") as f:
        json.dump(bundle, f, indent=2)
    with open(latest_bundle_path, "w", encoding="utf-8") as f:
        json.dump(bundle, f, indent=2)

    readme_path = _write_silver_readme(results=results, generated_at=generated_at)
    ui_path = _write_silver_ui(results=results, generated_at=generated_at)
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

    logger.info("Silver generation completed: %d scripts", len(results), extra={"run_id": run_id, "node": "silver_generation"})
    return new_state
