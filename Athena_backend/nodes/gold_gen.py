"""
Gold Code Generation

Generates Databricks/Spark KPI aggregate scripts from the certified gold
generation contract produced after silver generation.
"""

from __future__ import annotations

import ast
import json
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Tuple

from state import Stage01State
from utilis.db import ai_store_db_writer
from utilis.domain_kb import get_domain_kb_config, load_domain_kb
from utilis.generated_code_paths import generated_code_dir
from utilis.logger import logger


USE_LLM_ENV_KEYS = ("ATHENA_GOLD_USE_LLM", "USE_LLM")
DEFAULT_MAX_GOLD_SOURCE_TABLES = 3
DEFAULT_MAX_GOLD_DIMENSION_TABLES = 2
SILVER_COLUMN_NAME_CORRECTIONS = {
    "rererence_id": "reference_id",
}


def _gold_output_dir_for(target_warehouse: str = "databricks") -> str:
    if str(target_warehouse or "").lower() == "snowflake":
        return str(generated_code_dir("snowflake", "gold"))
    return str(generated_code_dir("gold"))


def _gold_output_dir() -> str:
    return _gold_output_dir_for("databricks")


def _run_slug(run_id: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_]+", "_", str(run_id or "run")).strip("_")
    return cleaned[:48] or "run"


def _contract_path() -> str:
    return os.path.join(_gold_output_dir(), "gold_generation_contract.json")


def _bundle_path(target_warehouse: str = "databricks") -> str:
    return os.path.join(_gold_output_dir_for(target_warehouse), "gold_scripts.json")


def _run_bundle_path(run_id: Any, target_warehouse: str = "databricks") -> str:
    return os.path.join(_gold_output_dir_for(target_warehouse), f"{_run_slug(str(run_id or 'run'))}_gold_scripts.json")


def _readme_path(target_warehouse: str = "databricks") -> str:
    return os.path.join(_gold_output_dir_for(target_warehouse), "README.md")


def _ui_path(target_warehouse: str = "databricks") -> str:
    return os.path.join(_gold_output_dir_for(target_warehouse), "index.html")


def _validate_python(code: str) -> None:
    compile(code, "<gold_generated>", "exec")


def _databricks_contract_columns(mapping: Dict[str, Any], dimension_contract: List[Dict[str, Any]]) -> set[str]:
    columns = set(_allowed_llm_source_columns(mapping))
    for path in mapping.get("join_paths") or []:
        if isinstance(path, dict):
            columns.update(
                _silver_output_column_name(path.get(name))
                for name in ("left_column", "right_column")
                if path.get(name)
            )
    for spec in dimension_contract:
        columns.update(_silver_output_column_name(name) for name in spec.get("source_columns") or spec.get("columns") or [])
        entity = _safe_identifier(spec.get("entity"), "dimension")
        columns.add(f"{entity}_key")
    columns.update(
        {
            "period_start",
            "gold_run_id",
            "kpi_name",
            "gold_processed_timestamp",
            "gold_upsert_key",
            "silver_upsert_key",
            "is_current",
            "natural_key_hash",
            "attribute_hash",
            "count",
            "entity",
            "source_age_days",
            "*",
        }
    )
    return {str(column).casefold() for column in columns if str(column).strip()}


def _databricks_contract_tables(
    mapping: Dict[str, Any], gold_schema: str, dimension_contract: List[Dict[str, Any]]
) -> set[str]:
    tables = {str(mapping.get("source_silver_table") or "").casefold()}
    for path in mapping.get("join_paths") or []:
        if not isinstance(path, dict):
            continue
        for name in ("left_source_table", "right_source_table"):
            if path.get(name):
                tables.add(str(path[name]).casefold())
    tables.update(
        f"{gold_schema}.dim_{_safe_identifier(spec.get('entity'), 'dimension')}".casefold()
        for spec in dimension_contract
    )
    tables.add(
        _target_fact_table(
            gold_schema,
            _safe_identifier(str(mapping.get("kpi_name") or "kpi"), "kpi"),
        ).casefold()
    )
    return {table for table in tables if table}


def _candidate_string_literals(tree: ast.AST) -> List[str]:
    values: List[str] = []
    constants: Dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                constants[node.targets[0].id] = node.value.value
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            values.append(node.value)
        elif isinstance(node, ast.JoinedStr):
            parts: List[str] = []
            for part in node.values:
                if isinstance(part, ast.Constant):
                    parts.append(str(part.value))
                elif isinstance(part, ast.FormattedValue) and isinstance(part.value, ast.Name):
                    parts.append(constants.get(part.value.id, ""))
            values.append("".join(parts))
    return values


def _validate_databricks_gold_candidate(
    code: str,
    mapping: Dict[str, Any],
    gold_schema: str,
    dimension_contract: List[Dict[str, Any]],
) -> None:
    _validate_python(code)
    tree = ast.parse(code)
    normalized = str(code or "").casefold()
    source_table = str(mapping.get("source_silver_table") or "").casefold()
    target_table = _target_fact_table(
        gold_schema,
        _safe_identifier(str(mapping.get("kpi_name") or "kpi"), "kpi"),
    ).casefold()
    missing = [value for value in (source_table, target_table) if value and value not in normalized]
    if missing:
        raise ValueError("LLM Gold code dropped the approved source or target table")

    literals = _candidate_string_literals(tree)
    approved_tables = _databricks_contract_tables(mapping, gold_schema, dimension_contract)
    referenced_tables = {
        value.casefold()
        for value in literals
        if re.fullmatch(r"[a-zA-Z0-9_]+\.(?:silver_|dim_|fact_)[a-zA-Z0-9_]+", value.strip())
    }
    unknown_tables = sorted(referenced_tables - approved_tables)
    if unknown_tables:
        raise ValueError("LLM Gold code referenced non-contract tables: " + ", ".join(unknown_tables[:10]))

    allowed_columns = _databricks_contract_columns(mapping, dimension_contract)
    referenced_columns: set[str] = set()
    column_calls = {"col", "column", "groupby", "select", "sum", "avg", "min", "max", "count", "countdistinct"}
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            name = ""
            if isinstance(node.func, ast.Name):
                name = node.func.id.casefold()
            elif isinstance(node.func, ast.Attribute):
                name = node.func.attr.casefold()
            if name in column_calls:
                referenced_columns.update(
                    str(arg.value).split(".")[-1].strip("`").casefold()
                    for arg in node.args
                    if isinstance(arg, ast.Constant) and isinstance(arg.value, str)
                )
        elif isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Constant) and isinstance(node.slice.value, str):
            referenced_columns.add(str(node.slice.value).split(".")[-1].strip("`").casefold())
    unknown_columns = sorted(referenced_columns - allowed_columns)
    if unknown_columns:
        raise ValueError("LLM Gold code referenced non-contract columns: " + ", ".join(unknown_columns[:10]))

    measure = mapping.get("measure") or {}
    aggregation = str(measure.get("aggregation") or "SUM").casefold()
    measure_column = _silver_output_column_name(measure.get("column")).casefold()
    if aggregation not in normalized or (aggregation != "count" and measure_column not in normalized):
        raise ValueError("LLM Gold code changed the certified measure or aggregation")
    if re.search(r"\.mode\(\s*['\"]append['\"]\s*\)", code, re.IGNORECASE):
        raise ValueError("LLM Gold code must be idempotent; append-only writes are forbidden")
    required_markers = (
        "gold_upsert_key",
        ".merge(",
        "whenmatchedupdateall",
        "whennotmatchedinsertall",
        "tableexists",
    )
    if any(marker not in normalized for marker in required_markers):
        raise ValueError("LLM Gold code must preserve deterministic MERGE/upsert behavior")
    if "dq_max_null_ratio" not in normalized or "silver_upsert_key" not in normalized:
        raise ValueError("LLM Gold code must preserve the runtime data-quality guards")
    forbidden = re.search(r"\b(drop|truncate|delete|grant|revoke)\b", normalized)
    if forbidden:
        raise ValueError(f"LLM Gold code contains forbidden operation: {forbidden.group(1).upper()}")


def _safe_identifier(value: str, fallback: str = "kpi") -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_]+", "_", str(value or "").strip().lower())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    if not cleaned:
        cleaned = fallback
    if cleaned[0].isdigit():
        cleaned = f"{fallback}_{cleaned}"
    return cleaned


def _snowflake_quote_identifier(value: str) -> str:
    text = str(value or "").strip().strip('"')
    return '"' + text.replace('"', '""') + '"'


def _snowflake_silver_source_identifier(value: str) -> str:
    return _snowflake_quote_identifier(_silver_output_column_name(value))


def _snowflake_qualified_name(*parts: str) -> str:
    return ".".join(_snowflake_quote_identifier(part) for part in parts if str(part or "").strip())


def _snowflake_string_literal(value: Any) -> str:
    return "'" + str(value or "").replace("'", "''") + "'"


def _snowflake_gold_catalog() -> str:
    return str(os.getenv("SNOWFLAKE_GOLD_CATALOG") or os.getenv("SNOWFLAKE_SILVER_CATALOG") or "ATHENA_DB").strip() or "ATHENA_DB"


def _snowflake_gold_schema() -> str:
    return str(os.getenv("SNOWFLAKE_GOLD_SCHEMA") or "GOLD").strip() or "GOLD"


def _target_warehouse(state: Stage01State) -> str:
    return str(state.get("target_warehouse") or "databricks").lower()


def _load_contract(state: Stage01State) -> Dict[str, Any]:
    
    
    contract = state.get("gold_generation_contract") or {}
    if contract:
        return contract

    path = str(state.get("gold_contract_bundle_path") or _contract_path())
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    return {}


def _literal(value: Any) -> str:
    return repr(value)


def _result_column_name(kpi_name: str) -> str:
    return f"{_safe_identifier(kpi_name, 'kpi')}_value"


def _silver_output_column_name(value: Any) -> str:
    """Return the physical Silver column name used by both Gold runtimes."""
    normalized = str(value or "").strip().strip('"').lower()
    return SILVER_COLUMN_NAME_CORRECTIONS.get(normalized, normalized)


def _date_grain_expr(grain: str, source_column: str) -> str:
    grain = str(grain or "month").lower()
    if grain not in {"day", "week", "month", "quarter", "year"}:
        grain = "month"
    return f"date_trunc('{grain}', col({source_column!r})).alias('period_start')"


def _measure_expression(measure: Dict[str, Any], value_alias: str) -> str:
    column = str(measure.get("column") or "").strip()
    aggregation = str(measure.get("aggregation") or "SUM").upper()
    if aggregation == "COUNT":
        return f"count(lit(1)).alias({value_alias!r})"
    if aggregation == "AVG":
        return f"avg(col({column!r})).alias({value_alias!r})"
    if aggregation == "MIN":
        return f"min(col({column!r})).alias({value_alias!r})"
    if aggregation == "MAX":
        return f"max(col({column!r})).alias({value_alias!r})"
    return (
        f"sum(coalesce(col({column!r}).cast('decimal(38,10)'), "
        f"lit(0).cast('decimal(38,10)'))).alias({value_alias!r})"
    )


def _env_float(name: str, default: float, minimum: float = 0.0) -> float:
    try:
        return max(minimum, float(os.getenv(name, str(default))))
    except ValueError:
        return default


def _env_int(name: str, default: int, minimum: int = 0) -> int:
    try:
        return max(minimum, int(os.getenv(name, str(default))))
    except ValueError:
        return default


def _llm_enabled_for_gold() -> bool:
    return any(str(os.getenv(key, "")).lower() in {"1", "true", "yes", "on"} for key in USE_LLM_ENV_KEYS)


def _extract_code_block(value: str) -> str:
    text = str(value or "").strip()
    match = re.search(r"```(?:python)?\s*(.*?)```", text, flags=re.IGNORECASE | re.DOTALL)
    return match.group(1).strip() if match else text


def _silver_schema_from_source(source_table: str) -> str:
    parts = str(source_table or "").split(".")
    return parts[0] if len(parts) == 2 else "silver"


def _logical_table_from_silver(source_table: str) -> str:
    name = str(source_table or "").split(".")[-1]
    return name.removeprefix("silver_")


def _dimension_entity_for_column(column: str, table: str | None = None) -> str:
    table_entity = _safe_identifier(table or "", "dimension")
    if table_entity != "dimension":
        return table_entity

    column_text = str(column or "").lower()
    table_text = str(table or "").lower()
    direct_matches = {
        "customer": ("customer", "client", "insured", "account"),
        "product": ("product", "sku", "item"),
        "claim": ("claim",),
        "coverage": ("coverage", "cover"),
        "policy": ("policy", "pol_"),
        "agent": ("agent", "broker", "producer"),
        "channel": ("channel", "distribution"),
        "segment": ("segment",),
        "branch": ("branch", "office"),
        "region": ("region", "geog", "state", "zone", "territory", "country"),
    }
    # Column semantics identify the business entity more precisely than the
    # physical source table; policy_transactions can contain product, agent,
    # channel, region, and policy attributes side by side.
    for entity, tokens in direct_matches.items():
        if any(token in column_text for token in tokens):
            return entity
    for entity, tokens in direct_matches.items():
        if any(token in table_text for token in tokens):
            return entity

    cleaned = _safe_identifier(column or table or "dimension", "dimension")
    cleaned = re.sub(r"_(name|desc|description|category|type|code|id|identifier)$", "", cleaned)
    return cleaned or _safe_identifier(table or "dimension", "dimension")


def _dimension_specs(mapping: Dict[str, Any]) -> List[Dict[str, Any]]:
    grouped: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for item in mapping.get("grouping_dimensions", []) or []:
        if not isinstance(item, dict):
            continue
        semantic = str(item.get("semantic_type") or "").upper()
        if semantic == "DATE":
            continue
        column = str(item.get("column") or "").strip()
        if not column:
            continue
        table = str(item.get("table") or (mapping.get("measure") or {}).get("table") or "").strip()
        entity = _dimension_entity_for_column(column, table)
        key = (table, entity)
        explicit_source_table = str(item.get("source_silver_table") or "").strip()
        source_table = explicit_source_table or str(mapping.get("source_silver_table") or "").strip()
        source_parts = [part for part in source_table.split(".") if part.strip()]
        if not explicit_source_table and table and len(source_parts) >= 3:
            source_table = ".".join([source_parts[0], source_parts[1], f"silver_{table}"])
        spec = grouped.setdefault(
            key,
            {
                "entity": entity,
                "source_table": source_table,
                "logical_table": table,
                "columns": [],
                "source_columns": [],
            },
        )
        if column not in spec["columns"]:
            spec["columns"].append(column)
            spec["source_columns"].append(_silver_output_column_name(column))
    return list(grouped.values())


def _mapping_source_columns(mapping: Dict[str, Any]) -> set[str]:
    source_logical_table = _logical_table_name(mapping.get("source_silver_table"))
    columns: set[str] = set()
    measure = mapping.get("measure") or {}
    measure_column = str(measure.get("column") or "").strip()
    aggregation = str(measure.get("aggregation") or "").upper()
    if measure_column and aggregation != "COUNT":
        columns.add(_silver_output_column_name(measure_column))
    for item in mapping.get("grouping_dimensions") or []:
        if not isinstance(item, dict):
            continue
        table = _logical_table_name(item.get("table"))
        if not table or not source_logical_table or table == source_logical_table:
            column = str(item.get("column") or "").strip()
            if column:
                columns.add(_silver_output_column_name(column))
    time_info = mapping.get("time") or {}
    time_column = time_info.get("column") if isinstance(time_info, dict) else None
    if isinstance(time_column, dict):
        time_table = _logical_table_name(time_column.get("table"))
        if not time_table or not source_logical_table or time_table == source_logical_table:
            column = str(time_column.get("column") or "").strip()
            if column:
                columns.add(_silver_output_column_name(column))
    return columns


def _allowed_llm_source_columns(mapping: Dict[str, Any]) -> set[str]:
    columns = set(_mapping_source_columns(mapping))
    measure = mapping.get("measure") or {}
    measure_column = str(measure.get("column") or "").strip()
    if measure_column:
        columns.add(_silver_output_column_name(measure_column))
    for item in mapping.get("grouping_dimensions") or []:
        if isinstance(item, dict) and item.get("column"):
            columns.add(_silver_output_column_name(item["column"]))
    time_info = mapping.get("time") or {}
    time_column = time_info.get("column") if isinstance(time_info, dict) else None
    if isinstance(time_column, dict) and time_column.get("column"):
        columns.add(_silver_output_column_name(time_column["column"]))
    for path in mapping.get("join_paths") or []:
        if isinstance(path, dict):
            columns.update(
                _silver_output_column_name(path[name])
                for name in ("left_column", "right_column")
                if path.get(name)
            )
    return columns


def _shared_dimension_mapping(mappings: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Build one idempotent dimension contract for the whole Gold run."""
    dimensions: List[Dict[str, Any]] = []
    seen: set[Tuple[str, str]] = set()
    table_scores: Dict[str, int] = {}
    source_table = ""
    for mapping in mappings:
        if not isinstance(mapping, dict) or not _usable_mapping(mapping):
            continue
        source_table = source_table or str(mapping.get("source_silver_table") or "")
        for item in mapping.get("grouping_dimensions") or []:
            if not isinstance(item, dict) or str(item.get("semantic_type") or "").upper() == "DATE":
                continue
            table = str(item.get("table") or "").strip()
            column = str(item.get("column") or "").strip()
            if table:
                table_scores[table.casefold()] = table_scores.get(table.casefold(), 0) + 1
            key = (table.casefold(), column.casefold())
            if table and column and key not in seen:
                dimensions.append(item)
                seen.add(key)
    ranked_tables = sorted(table_scores, key=lambda table: (-table_scores[table], table))
    kept_tables = set(ranked_tables[:_max_gold_dimension_tables()])
    return {
        "kpi_name": "Shared Gold Dimensions",
        "source_silver_table": source_table,
        "grouping_dimensions": [
            item for item in dimensions if str(item.get("table") or "").casefold() in kept_tables
        ],
    }


def _gold_dimension_columns_for_table(enriched_metadata: Dict[str, Any], table_name: str) -> List[str]:
    columns = enriched_metadata.get("columns", []) if isinstance(enriched_metadata, dict) else []
    selected: List[str] = []
    seen: set[str] = set()
    for column in columns:
        if not isinstance(column, dict):
            continue
        if str(column.get("table_name") or "").casefold() != str(table_name or "").casefold():
            continue
        if str(column.get("semantic_type") or "").upper() != "DIMENSION":
            continue
        if column.get("is_pii_candidate") or column.get("is_primary_key"):
            continue
        name = _silver_output_column_name(column.get("column_name"))
        if name and name not in seen:
            selected.append(name)
            seen.add(name)
    return selected


def _source_table_grain_specs(
    contract: Dict[str, Any],
    mappings: List[Dict[str, Any]],
    enriched_metadata: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    enriched_metadata = enriched_metadata or {}
    specs: Dict[str, Dict[str, Any]] = {}
    for spec in _dimension_specs(_shared_dimension_mapping(mappings)):
        source_table = str(spec.get("source_table") or "").strip()
        logical_table = str(spec.get("logical_table") or _logical_table_from_silver(source_table)).strip()
        if logical_table.casefold().endswith("_dup_del"):
            continue
        if source_table and logical_table:
            dimension_columns = _gold_dimension_columns_for_table(enriched_metadata, logical_table)
            if not dimension_columns:
                dimension_columns = list(spec.get("source_columns") or [])
            specs.setdefault(
                logical_table.casefold(),
                {
                    "entity": _safe_identifier(logical_table, "dimension"),
                    "source_table": source_table,
                    "logical_table": logical_table,
                    "columns": dimension_columns,
                    "source_columns": dimension_columns,
                    "grain": "source_table",
                },
            )
    return list(specs.values())


def _target_dim_table(gold_schema: str, entity: str) -> str:
    return f"{gold_schema}.dim_{_safe_identifier(entity, 'dimension')}"


def _target_fact_table(gold_schema: str, kpi_id: str) -> str:
    return f"{gold_schema}.fact_{kpi_id}"


def _snowflake_target_fact_table(gold_catalog: str, gold_schema: str, kpi_id: str) -> str:
    return f"{gold_catalog}.{gold_schema}.fact_{kpi_id}"


def _llm_prompt(
    mapping: Dict[str, Any],
    run_id: str,
    gold_schema: str,
    dimension_contract: List[Dict[str, Any]],
    baseline: str,
    validation_feedback: str = "",
    domain_reference_context: str = "",
) -> str:
    measure = mapping.get("measure") or {}
    time_info = mapping.get("time") or {}
    prompt_parts = [
        "Generate production Databricks PySpark code for a Gold KPI fact table.",
        "Return only executable Python code.",
        "",
        f"KPI Name: {mapping.get('kpi_name')}",
        f"Run ID: {run_id}",
        f"Gold schema: {gold_schema}",
        f"Source Table: {mapping.get('source_silver_table')}",
        f"Measure: column={measure.get('column')}, aggregation={measure.get('aggregation')}",
        f"Dimensions: {json.dumps(mapping.get('grouping_dimensions') or [], default=str)}",
        f"Time grain: {time_info.get('grain')}",
        f"Filters: {json.dumps(mapping.get('filters') or [], default=str)}",
        f"Join paths: {json.dumps(mapping.get('join_paths') or [], default=str)}",
        f"Allowed source columns: {json.dumps(sorted(_allowed_llm_source_columns(mapping)))}",
        f"Generated dimension contract: {json.dumps(dimension_contract, default=str)}",
        f"Target table: {_target_fact_table(gold_schema, _safe_identifier(str(mapping.get('kpi_name') or 'kpi'), 'kpi'))}",
    ]
    if domain_reference_context:
        prompt_parts.extend(["", "DOMAIN REFERENCE MODEL:", domain_reference_context])
    prompt_parts.extend(
        [
            "",
            "Instructions to LLM:",
            "- Generate PySpark code.",
            "- Use groupBy + aggregation.",
            "- Apply date_trunc to create period_start when a time column exists.",
            "- Add metadata columns gold_run_id, kpi_name, and gold_processed_timestamp.",
            "- Follow Kimball star schema principles.",
            "- Join current dim_<name> tables and use surrogate keys in the fact table.",
            "- Only join dimension tables and keys listed in Generated dimension contract.",
            "- Preserve the baseline runtime data-quality guards and idempotent Delta MERGE.",
            "- Never use append-only writes or invent source columns, tables, or surrogate keys.",
        ]
    )
    if validation_feedback:
        prompt_parts.extend(["", "Previous candidate validation error:", validation_feedback, "Correct that exact error."])
    prompt_parts.extend(["", "VALIDATED BASELINE:", baseline])
    return "\n".join(prompt_parts)


def llm_generate_gold_code(
    mapping: Dict[str, Any],
    run_id: str,
    gold_schema: str,
    dimension_contract: List[Dict[str, Any]],
    validation_feedback: str = "",
    domain_reference_context: str = "",
) -> str:
    baseline = generate_gold_script(mapping=mapping, run_id=run_id, gold_schema=gold_schema)
    prompt = _llm_prompt(
        mapping,
        run_id,
        gold_schema,
        dimension_contract,
        baseline,
        validation_feedback,
        domain_reference_context,
    )
    provider = os.getenv("ATHENA_GOLD_LLM_PROVIDER", "azure_openai")
    model = os.getenv("ATHENA_GOLD_LLM_MODEL")
    from nodes.req_extraction import get_llm

    llm = get_llm(provider=provider, model=model, temperature=0.0)
    response = llm.invoke(prompt)
    content = getattr(response, "content", response)
    return _extract_code_block(str(content))


def llm_generate_snowflake_gold_code(
    mapping: Dict[str, Any],
    run_id: str,
    gold_catalog: str,
    gold_schema: str,
    validation_feedback: str = "",
) -> str:
    """Ask the model to improve Snowflake SQL, then validate/fallback upstream."""
    deterministic = generate_snowflake_gold_script(
        mapping=mapping,
        run_id=run_id,
        gold_catalog=gold_catalog,
        gold_schema=gold_schema,
    )
    canonical_source_columns = sorted(_mapping_source_columns(mapping))
    retry_context = (
        f"\nA previous candidate was rejected by the hard validator for this reason:\n{validation_feedback}\n"
        "Correct that exact violation without changing KPI semantics.\n"
        if validation_feedback
        else ""
    )
    prompt = f"""Generate production Snowflake SQL for this Gold KPI.
Return only SQL. Preserve the exact source and target tables, dimensional groupings,
metadata columns, and MERGE/upsert behavior from the baseline. Do not use Python,
Spark, or Databricks syntax. Do not invent columns.
Use only exact case-sensitive identifiers from Canonical Silver columns when reading
the source table. Copy those identifiers exactly from the baseline.
{retry_context}

KPI: {mapping.get('kpi_name')}
Canonical Silver columns: {json.dumps(canonical_source_columns)}
Mapping: {json.dumps(mapping, default=str)}

BASELINE:
{deterministic}
""".strip()
    provider = os.getenv("ATHENA_GOLD_LLM_PROVIDER", os.getenv("ATHENA_LLM_PROVIDER", "azure_openai"))
    from nodes.req_extraction import get_llm

    llm = get_llm(provider=provider, model=os.getenv("ATHENA_GOLD_LLM_MODEL"), temperature=0.0)
    response = llm.invoke(prompt)
    content = getattr(response, "content", response)
    text = str(content).strip()
    match = re.search(r"```(?:sql)?\s*(.*?)```", text, flags=re.IGNORECASE | re.DOTALL)
    candidate = match.group(1).strip() if match else text
    normalized = candidate.upper()
    if "CREATE TABLE" not in normalized or "MERGE INTO" not in normalized:
        raise ValueError("LLM Gold SQL must contain CREATE TABLE and MERGE INTO")
    if any(token in normalized for token in ("SPARK.", "PYSPARK", "DATABRICKS")):
        raise ValueError("LLM Gold SQL returned non-Snowflake syntax")
    return candidate


def _snowflake_source_select_region(code: str, mapping: Dict[str, Any]) -> str:
    source_table = _snowflake_qualified_name(*str(mapping.get("source_silver_table") or "").split("."))
    source_match = re.search(rf"(?:^|\s)FROM\s+{re.escape(source_table)}(?=\s|\)|,|$)", str(code or ""), re.IGNORECASE)
    if not source_match:
        return ""

    select_start = str(code).upper().rfind("SELECT", 0, source_match.start())
    if select_start < 0:
        return ""
    return str(code)[select_start:source_match.start()]


def _canonicalize_snowflake_gold_identifiers(code: str, mapping: Dict[str, Any]) -> str:
    by_casefold = {column.casefold(): column for column in _allowed_llm_source_columns(mapping)}
    by_casefold.update({raw.casefold(): canonical for raw, canonical in SILVER_COLUMN_NAME_CORRECTIONS.items()})

    source_region = _snowflake_source_select_region(code, mapping)
    if not source_region:
        return str(code or "")
    select_start = str(code).find(source_region)
    region_end = select_start + len(source_region)
    prefix = str(code)[:select_start]
    suffix = str(code)[region_end:]

    def replace(match: re.Match[str]) -> str:
        if re.search(r"\bAS\s*$", source_region[:match.start()], re.IGNORECASE):
            return match.group(0)
        identifier = match.group(1).replace('""', '"')
        replacement = by_casefold.get(identifier.casefold())
        if replacement is None:
            return match.group(0)
        return '"' + replacement.replace('"', '""') + '"'

    return prefix + re.sub(r'"((?:""|[^"])*)"', replace, source_region) + suffix


def _snowflake_source_identifier_references(code: str, mapping: Dict[str, Any]) -> set[str]:
    source_region = _snowflake_source_select_region(code, mapping)
    if not source_region:
        return set()
    references = set()
    for match in re.finditer(r'"((?:""|[^"])*)"', source_region):
        if re.search(r"\bAS\s*$", source_region[:match.start()], re.IGNORECASE):
            continue
        references.add(match.group(1).replace('""', '"'))
    return references


def _sql_without_comments(code: str) -> str:
    return re.sub(r"--[^\n]*|/\*.*?\*/", "", str(code or ""), flags=re.DOTALL)


def _require_snowflake_gold_structure(code: str, mapping: Dict[str, Any], target_table: str) -> None:
    source = _snowflake_qualified_name(*str(mapping.get("source_silver_table") or "").split("."))
    target = _snowflake_qualified_name(*str(target_table or "").split("."))
    sql = _sql_without_comments(code)
    if not re.search(rf"(?:^|\s)FROM\s+{re.escape(source)}(?=\s|\)|,|$)", sql, re.IGNORECASE):
        raise ValueError("LLM Gold SQL must read the approved Silver table")
    if not re.search(rf"\bMERGE\s+INTO\s+{re.escape(target)}\s+(?:AS\s+)?target\b", sql, re.IGNORECASE):
        raise ValueError("LLM Gold SQL must merge into the approved Gold table as target")
    # Gold facts are generated from one certified Silver contract. Dimensions are
    # built separately, so an LLM-added join can only broaden that contract.
    if re.search(r"\bJOIN\b", sql, re.IGNORECASE):
        raise ValueError("LLM Gold SQL must not add joins outside the approved Silver source")
    forbidden = re.search(r"\b(DROP|TRUNCATE|DELETE|COPY|CALL|GRANT|REVOKE|USE|EXECUTE\s+IMMEDIATE)\b", sql, re.IGNORECASE)
    if forbidden:
        raise ValueError(f"LLM Gold SQL contains forbidden statement: {forbidden.group(1).upper()}")


def _validate_snowflake_gold_candidate(code: str, mapping: Dict[str, Any], target_table: str) -> None:
    normalized = str(code or "").lower()
    normalized_identifiers = normalized.replace('"', "")
    source_table = str(mapping.get("source_silver_table") or "").lower()
    required = {source_table, str(target_table).lower()}
    required.update(_mapping_source_columns(mapping))
    missing = [token for token in sorted(required) if token and token not in normalized_identifiers]
    if missing:
        raise ValueError(f"LLM Gold SQL dropped required contract fields: {', '.join(missing[:10])}")
    _require_snowflake_gold_structure(code, mapping, target_table)
    quoted = {
        match.group(1).replace('""', '"')
        for match in re.finditer(r'"((?:""|[^"])*)"', str(code or ""))
    }
    wrong_case = [column for column in _mapping_source_columns(mapping) if column not in quoted]
    if wrong_case:
        raise ValueError(
            "LLM Gold SQL used non-canonical Silver identifiers: " + ", ".join(sorted(wrong_case)[:10])
        )
    allowed_source_columns = {column.casefold() for column in _allowed_llm_source_columns(mapping)}
    unknown_source_refs = sorted(
        {
            reference
            for reference in _snowflake_source_identifier_references(code, mapping)
            if reference.casefold() not in allowed_source_columns
        }
    )
    if unknown_source_refs:
        raise ValueError(
            "LLM Gold SQL referenced non-contract Silver identifiers: "
            + ", ".join(unknown_source_refs[:10])
        )
    upper = normalized.upper()
    if "CREATE SCHEMA" not in upper or "CREATE TABLE" not in upper or "MERGE INTO" not in upper:
        raise ValueError("LLM Gold SQL is missing required DDL or MERGE statements")
    aggregation = str((mapping.get("measure") or {}).get("aggregation") or "SUM").upper()
    if aggregation in {"SUM", "AVG", "MIN", "MAX", "COUNT"} and f"{aggregation}(" not in upper:
        raise ValueError(f"LLM Gold SQL does not preserve the required {aggregation} aggregation")


def _usable_mapping(mapping: Dict[str, Any]) -> bool:
    measure = mapping.get("measure") or {}
    formula = mapping.get("formula") or {}
    if mapping.get("readiness") == "BLOCKED":
        return False
    if formula.get("status") == "NEEDS_CERTIFICATION":
        return False
    if not mapping.get("source_silver_table"):
        return False
    aggregation = str(measure.get("aggregation") or "").upper()
    if aggregation != "COUNT" and not measure.get("column"):
        return False
    return True


def _max_gold_source_tables() -> int:
    raw_value = str(os.getenv("ATHENA_GOLD_MAX_SOURCE_TABLES") or DEFAULT_MAX_GOLD_SOURCE_TABLES)
    try:
        return max(1, int(raw_value))
    except ValueError:
        return DEFAULT_MAX_GOLD_SOURCE_TABLES


def _max_gold_dimension_tables() -> int:
    raw_value = str(os.getenv("ATHENA_GOLD_MAX_DIMENSION_TABLES") or DEFAULT_MAX_GOLD_DIMENSION_TABLES)
    try:
        return max(0, int(raw_value))
    except ValueError:
        return DEFAULT_MAX_GOLD_DIMENSION_TABLES


def _logical_table_name(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return _logical_table_from_silver(text)


def _normalize_contract_mappings(
    contract: Dict[str, Any], *, canonicalize_columns: bool
) -> List[Dict[str, Any]]:
    silver_by_name: Dict[str, str] = {}
    silver_by_target: Dict[str, str] = {}
    for item in contract.get("silver_tables") or []:
        if not isinstance(item, dict):
            continue
        target = str(item.get("target_table") or "").strip()
        logical = str(item.get("table") or _logical_table_from_silver(target)).strip().casefold()
        if target and logical:
            silver_by_name[logical] = target
            silver_by_target[target.casefold()] = target

    normalized_mappings: List[Dict[str, Any]] = []
    for raw_mapping in contract.get("kpi_mappings") or []:
        if not isinstance(raw_mapping, dict):
            continue
        mapping = dict(raw_mapping)
        measure = dict(mapping.get("measure") or {})
        measure_table = _logical_table_name(measure.get("table"))
        requested_source = str(mapping.get("source_silver_table") or "").strip()
        source_table = silver_by_target.get(requested_source.casefold()) or silver_by_name.get(measure_table)
        if not source_table:
            normalized_mappings.append({**mapping, "source_silver_table": None, "readiness": "BLOCKED"})
            continue
        mapping["source_silver_table"] = source_table
        if canonicalize_columns and measure.get("column"):
            measure["column"] = _silver_output_column_name(measure["column"])
        mapping["measure"] = measure

        dimensions: List[Dict[str, Any]] = []
        for dimension in mapping.get("grouping_dimensions") or []:
            if not isinstance(dimension, dict):
                continue
            table = _logical_table_name(dimension.get("table") or measure_table)
            dimension_source = silver_by_name.get(table)
            if not dimension_source:
                continue
            column = dimension.get("column")
            dimensions.append({
                **dimension,
                "table": table,
                "column": _silver_output_column_name(column) if canonicalize_columns else column,
                "source_silver_table": dimension_source,
            })
        mapping["grouping_dimensions"] = dimensions

        joins: List[Dict[str, Any]] = []
        for path in mapping.get("join_paths") or []:
            if not isinstance(path, dict) or path.get("certified") is not True:
                continue
            left = _logical_table_name(path.get("left_table"))
            right = _logical_table_name(path.get("right_table"))
            if left not in silver_by_name or right not in silver_by_name:
                continue
            joins.append({
                **path,
                "left_table": left,
                "left_column": _silver_output_column_name(path.get("left_column")) if canonicalize_columns else path.get("left_column"),
                "left_source_table": silver_by_name[left],
                "right_table": right,
                "right_column": _silver_output_column_name(path.get("right_column")) if canonicalize_columns else path.get("right_column"),
                "right_source_table": silver_by_name[right],
            })
        mapping["join_paths"] = joins

        time_info = dict(mapping.get("time") or {})
        if canonicalize_columns and isinstance(time_info.get("column"), dict):
            time_info["column"] = {
                **time_info["column"],
                "column": _silver_output_column_name(time_info["column"].get("column")),
            }
        mapping["time"] = time_info
        normalized_mappings.append(mapping)
    return normalized_mappings


def _bump_score(scores: Dict[str, float], table: Any, points: float) -> None:
    name = _logical_table_name(table)
    if name:
        scores[name] = scores.get(name, 0.0) + points


def _mapping_source_table_scores(mapping: Dict[str, Any]) -> Dict[str, float]:
    measure = mapping.get("measure") or {}
    time_info = mapping.get("time") or {}
    time_column = time_info.get("column") if isinstance(time_info, dict) else {}
    scores: Dict[str, float] = {}

    _bump_score(scores, mapping.get("source_silver_table"), 10_000)
    _bump_score(scores, measure.get("table"), 5_000)
    if isinstance(time_column, dict):
        _bump_score(scores, time_column.get("table"), 300)

    for dimension in mapping.get("grouping_dimensions") or []:
        if not isinstance(dimension, dict):
            continue
        _bump_score(scores, dimension.get("table"), 120)
        if str(dimension.get("semantic_type") or "").upper() == "DATE":
            _bump_score(scores, dimension.get("table"), 60)

    for path in mapping.get("join_paths") or []:
        if not isinstance(path, dict):
            continue
        if not all(str(path.get(key) or "").strip() for key in ("left_table", "right_table", "left_column", "right_column")):
            continue
        try:
            confidence = float(path.get("confidence") or 0)
        except (TypeError, ValueError):
            confidence = 0.0
        join_score = 25 + min(max(confidence, 0.0), 1.0) * 50
        if path.get("certified"):
            join_score += 75
        _bump_score(scores, path.get("left_table"), join_score)
        _bump_score(scores, path.get("right_table"), join_score)

    return scores


def _sanitize_gold_mapping(mapping: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    max_tables = _max_gold_source_tables()
    scores = _mapping_source_table_scores(mapping)
    ranked_tables = sorted(scores, key=lambda table: (-scores[table], table))
    kept_tables = set(ranked_tables[:max_tables])
    dropped_tables = [table for table in ranked_tables if table not in kept_tables]

    original_join_paths = [path for path in mapping.get("join_paths") or [] if isinstance(path, dict)]
    valid_join_paths: List[Dict[str, Any]] = []
    malformed_count = 0

    for path in original_join_paths:
        left_table = _logical_table_name(path.get("left_table"))
        right_table = _logical_table_name(path.get("right_table"))
        left_column = str(path.get("left_column") or "").strip()
        right_column = str(path.get("right_column") or "").strip()
        if not left_table or not right_table or not left_column or not right_column:
            malformed_count += 1
            continue
        if left_table in kept_tables and right_table in kept_tables:
            valid_join_paths.append({**path, "left_table": left_table, "right_table": right_table})

    warnings: List[str] = []
    if malformed_count:
        warnings.append(f"Dropped {malformed_count} malformed Gold join path(s).")
    if dropped_tables:
        warnings.append(
            f"Gold source table cap applied: kept {', '.join(ranked_tables[:max_tables])}; "
            f"dropped {', '.join(dropped_tables)}."
        )
    if original_join_paths and not valid_join_paths and len(kept_tables) <= 1:
        warnings.append("Gold join paths were not usable after validation; generating from the primary Silver table only.")

    dimension_tables: List[str] = []
    for item in mapping.get("grouping_dimensions") or []:
        if not isinstance(item, dict) or str(item.get("semantic_type") or "").upper() == "DATE":
            continue
        table = _logical_table_name(item.get("table"))
        if table and table not in dimension_tables:
            dimension_tables.append(table)
    max_dimensions = _max_gold_dimension_tables()
    kept_dimension_tables = set(
        sorted(dimension_tables, key=lambda table: (-scores.get(table, 0.0), table))[:max_dimensions]
    )
    dropped_dimension_tables = [table for table in dimension_tables if table not in kept_dimension_tables]
    grouping_dimensions = [
        item
        for item in mapping.get("grouping_dimensions") or []
        if isinstance(item, dict)
        and (
            str(item.get("semantic_type") or "").upper() == "DATE"
            or _logical_table_name(item.get("table")) in kept_dimension_tables
        )
    ]
    if dropped_dimension_tables:
        warnings.append(
            f"Gold dimension table cap applied: dropped {', '.join(dropped_dimension_tables)}."
        )

    guard = {
        "max_source_tables": max_tables,
        "max_dimension_tables": max_dimensions,
        "ranked_source_tables": ranked_tables,
        "kept_source_tables": [table for table in ranked_tables if table in kept_tables],
        "dropped_source_tables": dropped_tables,
        "kept_dimension_tables": [table for table in dimension_tables if table in kept_dimension_tables],
        "dropped_dimension_tables": dropped_dimension_tables,
        "dropped_malformed_join_paths": malformed_count,
        "dropped_join_paths": max(0, len(original_join_paths) - len(valid_join_paths) - malformed_count),
        "warnings": warnings,
    }
    return {
        **mapping,
        "grouping_dimensions": grouping_dimensions,
        "join_paths": valid_join_paths,
        "_gold_source_table_guard": guard,
    }, guard


def generate_dimension_script(mapping: Dict[str, Any], gold_schema: str) -> str:
    kpi_name = str(mapping.get("kpi_name") or "KPI")
    source_table = str(mapping.get("source_silver_table") or "")
    specs = _dimension_specs(mapping)
    silver_schema = _silver_schema_from_source(source_table)

    return f'''
"""
AUTO-GENERATED GOLD DIMENSION SCRIPT

KPI context: {kpi_name}
Source table: {source_table}
Expected runtime: Spark / Databricks with Delta support

DO NOT EDIT MANUALLY
"""

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, concat_ws, current_timestamp, lit, sha2, to_timestamp

spark = SparkSession.builder.getOrCreate()

try:
    spark.sql("CREATE SCHEMA IF NOT EXISTS {gold_schema}")
except Exception:
    print("Could not create schema '{gold_schema}' in the current catalog")

SOURCE_TABLE = {source_table!r}
SILVER_SCHEMA = {silver_schema!r}
DIMENSIONS = {_literal(specs)}

if not SOURCE_TABLE:
    raise ValueError("Missing dimension source table.")

def _source_table(dim):
    logical_table = str(dim.get("logical_table") or "").strip()
    if logical_table:
        return f"{{SILVER_SCHEMA}}.silver_{{logical_table}}"
    return SOURCE_TABLE

def _hash_columns(df, columns):
    expressions = [coalesce(col(name).cast("string"), lit("__NULL__")) for name in columns if name in df.columns]
    if not expressions:
        return sha2(lit("__ALL__"), 256)
    return sha2(concat_ws("||", *expressions), 256)

for dim in DIMENSIONS:
    entity = dim["entity"]
    target_table = "{gold_schema}.dim_" + entity
    key_column = entity + "_key"
    dim_source_table = _source_table(dim)

    if not spark.catalog.tableExists(dim_source_table):
        print(f"WARNING: Skipping dimension {{target_table}} because source table is missing: {{dim_source_table}}")
        continue

    src = spark.table(dim_source_table)
    natural_columns = [name for name in dim.get("columns", []) if name in src.columns]

    if not natural_columns:
        print(f"WARNING: Skipping dimension {{target_table}} because no source columns are available")
        continue

    staged = src.select(*[col(name) for name in natural_columns]).dropDuplicates()
    source_count = src.count()
    dimension_count = staged.count()
    if source_count and dimension_count >= source_count:
        print(
            f"WARNING: Skipping dimension {{target_table}} because its cardinality "
            f"({{dimension_count}}) is not smaller than the source ({{source_count}})"
        )
        continue
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
        print(f"SUCCESS: Created dimension {{target_table}}")
        continue

    current_dim = spark.table(target_table).filter(col("is_current") == 1)
    new_rows = (
        staged.alias("s")
        .join(
            current_dim.select("natural_key_hash").alias("d"),
            col("s.natural_key_hash") == col("d.natural_key_hash"),
            "left_anti",
        )
        .select("s.*")
    )

    (
        new_rows.write
        .format("delta")
        .mode("append")
        .saveAsTable(target_table)
    )

    print(f"SUCCESS: Dimension merge completed for {{target_table}}")
'''


def generate_gold_script(
    *,
    mapping: Dict[str, Any],
    run_id: str,
    gold_schema: str,
) -> str:
    kpi_name = str(mapping.get("kpi_name") or "KPI")
    kpi_id = _safe_identifier(kpi_name, "kpi")
    source_table = str(mapping["source_silver_table"])
    target_table = _target_fact_table(gold_schema, kpi_id)
    value_alias = _result_column_name(kpi_name)
    measure = mapping.get("measure") or {}
    measure_column = str(measure.get("column") or "")
    measure_aggregation = str(measure.get("aggregation") or "SUM").upper()
    dimensions = [
        item for item in mapping.get("grouping_dimensions", [])
        if isinstance(item, dict) and item.get("column")
    ]
    time_info = mapping.get("time") or {}
    time_column = (time_info.get("column") or {}).get("column") if isinstance(time_info.get("column"), dict) else None
    time_grain = str(time_info.get("grain") or "month")
    filters = mapping.get("filters") or []
    join_paths = mapping.get("join_paths") or []
    dimension_specs = _dimension_specs(mapping)
    silver_schema = _silver_schema_from_source(source_table)
    source_logical_table = _logical_table_from_silver(source_table)
    dq_max_null_ratio = min(1.0, _env_float("ATHENA_GOLD_MAX_MEASURE_NULL_RATIO", 0.2))
    dq_max_dimension_cardinality = _env_int("ATHENA_GOLD_MAX_DIMENSION_CARDINALITY", 1_000_000, 1)
    dq_max_source_age_days = _env_int("ATHENA_GOLD_MAX_SOURCE_AGE_DAYS", 0)
    dq_max_join_multiplier = _env_float("ATHENA_GOLD_MAX_JOIN_MULTIPLIER", 1.05, 1.0)

    dimension_columns = []
    seen_dimensions = set()
    for item in dimensions:
        column = str(item.get("column") or "").strip()
        if not column or column in seen_dimensions:
            continue
        seen_dimensions.add(column)
        dimension_columns.append(column)

    return f'''
"""
AUTO-GENERATED GOLD KPI SCRIPT

KPI: {kpi_name}
Source table: {source_table}
Target table: {target_table}
Expected runtime: Spark / Databricks with Delta support

DO NOT EDIT MANUALLY
"""

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import approx_count_distinct, avg, coalesce, col, concat_ws, count, current_date, current_timestamp, datediff, date_trunc, expr, lit, max, min, sha2, sum, to_date
from pyspark.sql.types import DateType, NumericType, TimestampType

spark = SparkSession.builder.getOrCreate()

try:
    spark.sql("CREATE SCHEMA IF NOT EXISTS {gold_schema}")
except Exception:
    print("Could not create schema '{gold_schema}' in the current catalog")

RUN_ID = {run_id!r}
KPI_NAME = {kpi_name!r}
SOURCE_TABLE = {source_table!r}
TARGET_TABLE = {target_table!r}
VALUE_COLUMN = {value_alias!r}
SILVER_SCHEMA = {silver_schema!r}
SOURCE_LOGICAL_TABLE = {source_logical_table!r}
MEASURE_COLUMN = {measure_column!r}
MEASURE_AGGREGATION = {measure_aggregation!r}
DIMENSION_COLUMNS = {_literal(dimension_columns)}
DIMENSION_SPECS = {_literal(dimension_specs)}
TIME_COLUMN = {time_column!r}
TIME_GRAIN = {time_grain!r}
BUSINESS_FILTERS = {_literal(filters)}
JOIN_PATHS = {_literal(join_paths)}
DQ_MAX_NULL_RATIO = {dq_max_null_ratio!r}
DQ_MAX_DIMENSION_CARDINALITY = {dq_max_dimension_cardinality!r}
DQ_MAX_SOURCE_AGE_DAYS = {dq_max_source_age_days!r}
DQ_MAX_JOIN_MULTIPLIER = {dq_max_join_multiplier!r}

if not spark.catalog.tableExists(SOURCE_TABLE):
    raise ValueError(f"Missing silver source table: {{SOURCE_TABLE}}")

df = spark.table(SOURCE_TABLE)

source_row_count = df.count()
if source_row_count == 0:
    raise ValueError(f"Silver source table has no rows: {{SOURCE_TABLE}}")

if "silver_upsert_key" in df.columns:
    duplicate_key_exists = (
        df.groupBy("silver_upsert_key")
        .count()
        .filter(col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicate_key_exists:
        raise ValueError(f"Duplicate silver_upsert_key values found in {{SOURCE_TABLE}}")

if MEASURE_AGGREGATION != "COUNT":
    if MEASURE_COLUMN not in df.columns:
        raise ValueError(f"Gold measure column '{{MEASURE_COLUMN}}' is missing from {{SOURCE_TABLE}}")
    measure_field = next(field for field in df.schema.fields if field.name == MEASURE_COLUMN)
    if MEASURE_AGGREGATION in {{"SUM", "AVG"}} and not isinstance(measure_field.dataType, NumericType):
        raise TypeError(
            f"Gold {{MEASURE_AGGREGATION}} requires a numeric column; "
            f"{{SOURCE_TABLE}}.{{MEASURE_COLUMN}} is {{measure_field.dataType.simpleString()}}"
        )
    measure_null_count = df.filter(col(MEASURE_COLUMN).isNull()).count()
    measure_null_ratio = measure_null_count / source_row_count
    if measure_null_ratio > DQ_MAX_NULL_RATIO:
        raise ValueError(
            f"Gold measure null ratio {{measure_null_ratio:.2%}} exceeds "
            f"{{DQ_MAX_NULL_RATIO:.2%}} for {{SOURCE_TABLE}}.{{MEASURE_COLUMN}}"
        )

profile_dimensions = list(dict.fromkeys(name for name in DIMENSION_COLUMNS if name in df.columns))
if profile_dimensions:
    cardinalities = df.agg(
        *[approx_count_distinct(col(name)).alias(name) for name in profile_dimensions]
    ).first().asDict()
    oversized = {{name: value for name, value in cardinalities.items() if value > DQ_MAX_DIMENSION_CARDINALITY}}
    if oversized:
        raise ValueError(f"Gold dimension cardinality exceeds limit: {{oversized}}")

if TIME_COLUMN and TIME_COLUMN in df.columns:
    time_field = next(field for field in df.schema.fields if field.name == TIME_COLUMN)
    if not isinstance(time_field.dataType, (DateType, TimestampType)):
        raise TypeError(
            f"Gold time column {{SOURCE_TABLE}}.{{TIME_COLUMN}} must be date/timestamp, "
            f"not {{time_field.dataType.simpleString()}}"
        )
    if DQ_MAX_SOURCE_AGE_DAYS > 0:
        source_age_days = df.select(
            datediff(current_date(), max(to_date(col(TIME_COLUMN)))).alias("source_age_days")
        ).first()["source_age_days"]
        if source_age_days is None or source_age_days > DQ_MAX_SOURCE_AGE_DAYS:
            raise ValueError(
                f"Gold source freshness failed for {{SOURCE_TABLE}}.{{TIME_COLUMN}}: "
                f"age={{source_age_days}} days, limit={{DQ_MAX_SOURCE_AGE_DAYS}}"
            )

def _silver_table(logical_table):
    return f"{{SILVER_SCHEMA}}.silver_{{logical_table}}"

def _sql_like_filter(condition):
    text = str(condition or "").strip()
    if not text or len(text) > 500:
        return False
    return bool(__import__("re").search(r"(=|<>|!=|>=|<=|>|<|\\bIN\\b|\\bLIKE\\b|\\bIS\\b)", text, __import__("re").IGNORECASE))

for condition in BUSINESS_FILTERS:
    if _sql_like_filter(condition):
        df = df.filter(expr(str(condition)))
    else:
        print(f"WARNING: Skipping non-SQL business filter: {{condition}}")

joined_logical_tables = {{SOURCE_LOGICAL_TABLE}}
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
        other_silver_table = str(path.get("right_source_table") or _silver_table(other_table))
    elif right_table in joined_logical_tables and left_table not in joined_logical_tables:
        other_table = left_table
        base_column = right_column
        other_column = left_column
        other_silver_table = str(path.get("left_source_table") or _silver_table(other_table))
    else:
        continue

    if not spark.catalog.tableExists(other_silver_table):
        print(f"WARNING: Missing join-path table: {{other_silver_table}}")
        continue
    if base_column not in df.columns:
        print(f"WARNING: Missing join-path base column: {{base_column}}")
        continue

    other_df = spark.table(other_silver_table)
    if other_column not in other_df.columns:
        print(f"WARNING: Missing join-path other column: {{other_column}} in {{other_silver_table}}")
        continue
    rename_map = {{
        name: f"{{other_table}}__{{name}}"
        for name in other_df.columns
        if name in df.columns and name != other_column
    }}
    for old_name, new_name in rename_map.items():
        other_df = other_df.withColumnRenamed(old_name, new_name)
    pre_join_count = df.count()
    joined_df = df.join(other_df, df[base_column] == other_df[other_column], join_type)
    joined_count = joined_df.count()
    if pre_join_count and joined_count > pre_join_count * DQ_MAX_JOIN_MULTIPLIER:
        raise ValueError(
            f"Gold join {{left_table}} -> {{right_table}} multiplied rows "
            f"from {{pre_join_count}} to {{joined_count}}"
        )
    df = joined_df
    joined_logical_tables.add(other_table)

available_columns = set(df.columns)
missing_dimensions = [name for name in DIMENSION_COLUMNS if name not in available_columns]
if missing_dimensions:
    print(f"WARNING: Dropping missing gold dimensions: {{missing_dimensions}}")

group_columns = []
dimension_raw_columns = set()
for dim in DIMENSION_SPECS:
    entity = dim["entity"]
    target_dim_table = "{gold_schema}.dim_" + entity
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
            print(f"WARNING: Dimension {{target_dim_table}} is missing required natural/key columns")
    else:
        print(f"WARNING: Dimension table {{target_dim_table}} does not exist; using raw attributes")
        group_columns.extend([col(name) for name in natural_columns])

group_columns.extend([
    col(name)
    for name in DIMENSION_COLUMNS
    if name in set(df.columns) and name not in dimension_raw_columns
])

if TIME_COLUMN and TIME_COLUMN in available_columns:
    group_columns.append({_date_grain_expr(time_grain, time_column)})
elif TIME_COLUMN:
    print(f"WARNING: Gold time column '{{TIME_COLUMN}}' is missing from {{SOURCE_TABLE}}")

if MEASURE_AGGREGATION != "COUNT" and MEASURE_COLUMN not in available_columns:
    raise ValueError(f"Gold measure column '{{MEASURE_COLUMN}}' is missing from {{SOURCE_TABLE}}")

agg_expr = {_measure_expression(measure, value_alias)}

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

grain_columns = [
    name for name in result.columns
    if name not in {{VALUE_COLUMN, "gold_processed_timestamp", "gold_run_id"}}
]
result = result.withColumn(
    "gold_upsert_key",
    sha2(
        concat_ws(
            "||",
            *[coalesce(col(name).cast("string"), lit("__NULL__")) for name in grain_columns]
        ),
        256,
    ),
)

if spark.catalog.tableExists(TARGET_TABLE):
    delta_target = DeltaTable.forName(spark, TARGET_TABLE)
    (
        delta_target.alias("target")
        .merge(
            result.alias("source"),
            "target.gold_upsert_key = source.gold_upsert_key",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    writer = result.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if "period_start" in result.columns:
        writer = writer.partitionBy("period_start")
    writer.saveAsTable(TARGET_TABLE)

print(f"SUCCESS: Gold KPI generation completed for {{TARGET_TABLE}}")
'''


def _snowflake_measure_expression(measure: Dict[str, Any], value_alias: str) -> str:
    column = str(measure.get("column") or "").strip()
    quoted_alias = _snowflake_quote_identifier(value_alias)
    aggregation = str(measure.get("aggregation") or "SUM").upper()
    if aggregation == "COUNT":
        return f"COUNT(*) AS {quoted_alias}"
    quoted_column = _snowflake_silver_source_identifier(column)
    numeric_expr = f"TRY_TO_DECIMAL(TO_VARCHAR({quoted_column}))"
    if aggregation == "AVG":
        return f"AVG({numeric_expr}) AS {quoted_alias}"
    if aggregation == "MIN":
        return f"MIN({numeric_expr}) AS {quoted_alias}"
    if aggregation == "MAX":
        return f"MAX({numeric_expr}) AS {quoted_alias}"
    return f"SUM({numeric_expr}) AS {quoted_alias}"


def _snowflake_grain_expr(grain: str, source_column: str) -> str:
    grain = str(grain or "month").lower()
    if grain not in {"day", "week", "month", "quarter", "year"}:
        grain = "month"
    source_expr = _snowflake_silver_source_identifier(source_column)
    return f"DATE_TRUNC('{grain}', TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR({source_expr})))"


def _snowflake_dimension_source_table(source_table: str, logical_table: str) -> str:
    parts = [part for part in str(source_table or "").split(".") if part.strip()]
    if logical_table and len(parts) >= 3:
        return ".".join([parts[0], parts[1], f"silver_{logical_table}"])
    return source_table


def generate_snowflake_source_table_mart_script(
    *,
    specs: List[Dict[str, Any]],
    run_id: str,
    gold_catalog: str,
    gold_schema: str,
) -> str:
    if not specs:
        return ""

    statements: List[str] = [f"CREATE SCHEMA IF NOT EXISTS {_snowflake_qualified_name(gold_catalog, gold_schema)};"]
    for spec in specs:
        source_table = str(spec.get("source_table") or "").strip()
        entity = _safe_identifier(str(spec.get("logical_table") or spec.get("entity") or ""), "dimension")
        if not source_table or not entity:
            continue
        source_qname = _snowflake_qualified_name(*source_table.split("."))
        dimension_columns = list(
            dict.fromkeys(
                _silver_output_column_name(column)
                for column in spec.get("source_columns") or spec.get("columns") or []
                if str(column).strip()
            )
        )
        dim_target = _snowflake_qualified_name(gold_catalog, gold_schema, f"DIM_{entity.upper()}")
        dim_key = f"{entity}_key"
        dim_column_defs = [(dim_key, "VARCHAR"), *[(column, "VARCHAR") for column in dimension_columns]]
        dim_column_defs.extend(
            [
                ("gold_run_id", "VARCHAR"),
                ("gold_processed_timestamp", "TIMESTAMP_NTZ"),
                ("gold_upsert_key", "VARCHAR"),
            ]
        )
        create_dim_columns = ",\n    ".join(
            f"{_snowflake_quote_identifier(name)} {data_type}" for name, data_type in dim_column_defs
        )
        alter_dim_columns = "\n".join(
            f"ALTER TABLE {dim_target} ADD COLUMN IF NOT EXISTS {_snowflake_quote_identifier(name)} {data_type};"
            for name, data_type in dim_column_defs
        )
        if dimension_columns:
            distinct_select = ",\n        ".join(
                f"TO_VARCHAR(src.{_snowflake_quote_identifier(column)}) AS {_snowflake_quote_identifier(column)}"
                for column in dimension_columns
            )
            natural_parts = ",\n            ".join(
                f"COALESCE(TO_VARCHAR({_snowflake_quote_identifier(column)}), '__NULL__')"
                for column in dimension_columns
            )
            natural_expr = f"MD5(CONCAT_WS('||',\n            {natural_parts}\n        ))"
            distinct_cte = f"""distinct_dimensions AS (
        SELECT DISTINCT
        {distinct_select}
        FROM {source_qname} AS src
            )"""
            source_dimension_columns = ",\n        " + ",\n        ".join(_snowflake_quote_identifier(column) for column in dimension_columns)
            update_dimension_columns = [dim_key, *dimension_columns, "gold_run_id", "gold_processed_timestamp"]
            insert_dimension_columns = [dim_key, *dimension_columns, "gold_run_id", "gold_processed_timestamp", "gold_upsert_key"]
        else:
            natural_expr = "MD5('__ALL__')"
            distinct_cte = f"""distinct_dimensions AS (
        SELECT 1 AS "__dimension_row"
        FROM {source_qname} AS src
        QUALIFY ROW_NUMBER() OVER (ORDER BY 1) = 1
    )"""
            source_dimension_columns = ""
            update_dimension_columns = [dim_key, "gold_run_id", "gold_processed_timestamp"]
            insert_dimension_columns = [dim_key, "gold_run_id", "gold_processed_timestamp", "gold_upsert_key"]
        update_dim_assignments = ",\n        ".join(
            f"target.{_snowflake_quote_identifier(column)} = source.{_snowflake_quote_identifier(column)}"
            for column in update_dimension_columns
            if column != dim_key
        )
        insert_dim_columns = [_snowflake_quote_identifier(column) for column in insert_dimension_columns]
        insert_dim_values = [f"source.{column}" for column in insert_dim_columns]
        statements.append(
            f"""
CREATE TABLE IF NOT EXISTS {dim_target} (
    {create_dim_columns}
);

{alter_dim_columns}

DELETE FROM {dim_target} WHERE "gold_run_id" = {_snowflake_string_literal(run_id)};

MERGE INTO {dim_target} AS target
USING (
    WITH {distinct_cte}
    SELECT
        {natural_expr} AS {_snowflake_quote_identifier(dim_key)}{source_dimension_columns},
        {_snowflake_string_literal(run_id)} AS "gold_run_id",
        CURRENT_TIMESTAMP() AS "gold_processed_timestamp",
        {natural_expr} AS "gold_upsert_key"
    FROM distinct_dimensions
) AS source
ON target."gold_upsert_key" = source."gold_upsert_key"
WHEN MATCHED THEN UPDATE SET
        {update_dim_assignments}
WHEN NOT MATCHED THEN INSERT (
        {", ".join(insert_dim_columns)}
    )
    VALUES (
        {", ".join(insert_dim_values)}
    );

-- ponytail: if every descriptive combination is row-unique, collapse to a
-- table-level dimension so DIM remains smaller than its Silver source.
DELETE FROM {dim_target}
WHERE "gold_run_id" = {_snowflake_string_literal(run_id)}
  AND (SELECT COUNT(*) FROM {dim_target} WHERE "gold_run_id" = {_snowflake_string_literal(run_id)})
      >= (SELECT COUNT(*) FROM {source_qname});

INSERT INTO {dim_target} (
        {_snowflake_quote_identifier(dim_key)}, "gold_run_id", "gold_processed_timestamp", "gold_upsert_key"
    )
SELECT
        MD5('__ALL__') AS {_snowflake_quote_identifier(dim_key)},
        {_snowflake_string_literal(run_id)} AS "gold_run_id",
        CURRENT_TIMESTAMP() AS "gold_processed_timestamp",
        MD5('__ALL__') AS "gold_upsert_key"
WHERE NOT EXISTS (
        SELECT 1 FROM {dim_target} WHERE "gold_run_id" = {_snowflake_string_literal(run_id)}
    )
  AND (SELECT COUNT(*) FROM {source_qname}) > 1;
""".strip()
        )

    return "\n\n".join(statements) + "\n"


def generate_snowflake_dimension_script(
    *,
    mapping: Dict[str, Any],
    run_id: str,
    gold_catalog: str,
    gold_schema: str,
) -> str:
    specs = _dimension_specs(mapping)
    if not specs:
        return ""

    source_table = str(mapping.get("source_silver_table") or "")
    statements: List[str] = [f"CREATE SCHEMA IF NOT EXISTS {_snowflake_qualified_name(gold_catalog, gold_schema)};"]
    for spec in specs:
        entity = _safe_identifier(str(spec.get("entity") or "dimension"), "dimension")
        columns = list(dict.fromkeys(str(column).strip() for column in spec.get("columns") or [] if str(column).strip()))
        source_columns = list(
            dict.fromkeys(
                _silver_output_column_name(column)
                for column in spec.get("source_columns") or columns
                if str(column).strip()
            )
        )
        if not columns:
            continue

        target_table = _snowflake_qualified_name(gold_catalog, gold_schema, f"dim_{entity}")
        source_qname = _snowflake_qualified_name(
            *_snowflake_dimension_source_table(
                str(spec.get("source_table") or source_table), str(spec.get("logical_table") or "")
            ).split(".")
        )
        key_column = f"{entity}_key"
        dimension_columns = [(key_column, "VARCHAR"), ("natural_key_hash", "VARCHAR"), ("attribute_hash", "VARCHAR")]
        dimension_columns.extend((column, "VARCHAR") for column in columns)
        dimension_columns.extend(
            [
                ("effective_from", "TIMESTAMP_NTZ"),
                ("effective_to", "TIMESTAMP_NTZ"),
                ("is_current", "BOOLEAN"),
                ("gold_run_id", "VARCHAR"),
                ("gold_processed_timestamp", "TIMESTAMP_NTZ"),
            ]
        )
        create_columns = ",\n    ".join(
            f"{_snowflake_quote_identifier(name)} {data_type}" for name, data_type in dimension_columns
        )
        alter_columns = "\n".join(
            f"ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS {_snowflake_quote_identifier(name)} {data_type};"
            for name, data_type in dimension_columns
        )
        natural_parts = [
            f"COALESCE(TO_VARCHAR({_snowflake_silver_source_identifier(column)}), '__NULL__')"
            for column in source_columns
        ]
        natural_expr = f"MD5(CONCAT_WS('||', {', '.join(natural_parts)}))"
        select_columns = ",\n        ".join(
            f"{_snowflake_silver_source_identifier(source_column)} AS {_snowflake_quote_identifier(column)}"
            for column, source_column in zip(columns, source_columns)
        )
        insert_columns = [_snowflake_quote_identifier(name) for name, _ in dimension_columns]
        update_columns = [
            name
            for name, _ in dimension_columns
            if name not in {key_column, "natural_key_hash", "effective_from"}
        ]
        update_assignments = ",\n        ".join(
            f"target.{_snowflake_quote_identifier(name)} = source.{_snowflake_quote_identifier(name)}"
            for name in update_columns
        )
        insert_values = [f"source.{column}" for column in insert_columns]

        statements.append(
            f"""
CREATE TABLE IF NOT EXISTS {target_table} (
    {create_columns}
);

{alter_columns}

MERGE INTO {target_table} AS target
USING (
    SELECT DISTINCT
        {natural_expr} AS {_snowflake_quote_identifier(key_column)},
        {natural_expr} AS "natural_key_hash",
        {natural_expr} AS "attribute_hash",
        {select_columns},
        CURRENT_TIMESTAMP() AS "effective_from",
        TO_TIMESTAMP_NTZ('9999-12-31 23:59:59') AS "effective_to",
        TRUE AS "is_current",
        {_snowflake_string_literal(run_id)} AS "gold_run_id",
        CURRENT_TIMESTAMP() AS "gold_processed_timestamp"
    FROM {source_qname}
) AS source
ON target."natural_key_hash" = source."natural_key_hash" AND target."is_current" = TRUE
WHEN MATCHED THEN UPDATE SET
        {update_assignments}
WHEN NOT MATCHED THEN INSERT (
        {", ".join(insert_columns)}
    )
    VALUES (
        {", ".join(insert_values)}
    );
""".strip()
        )

    return "\n\n".join(statements) + "\n"


def generate_snowflake_gold_script(
    *,
    mapping: Dict[str, Any],
    run_id: str,
    gold_catalog: str,
    gold_schema: str,
) -> str:
    kpi_name = str(mapping.get("kpi_name") or "KPI")
    kpi_id = _safe_identifier(kpi_name, "kpi")
    source_table = str(mapping["source_silver_table"])
    target_table = _snowflake_target_fact_table(gold_catalog, gold_schema, kpi_id)
    value_alias = _result_column_name(kpi_name)
    measure = mapping.get("measure") or {}
    source_logical_table = _logical_table_name(source_table)
    dimensions = [
        str(item.get("column") or "").strip()
        for item in mapping.get("grouping_dimensions", []) or []
        if isinstance(item, dict)
        and str(item.get("column") or "").strip()
        and (
            not _logical_table_name(item.get("table"))
            or not source_logical_table
            or _logical_table_name(item.get("table")) == source_logical_table
        )
    ]
    dimension_columns = list(dict.fromkeys(dimensions))[:12]
    time_info = mapping.get("time") or {}
    time_column_info = time_info.get("column") if isinstance(time_info, dict) else None
    time_column = None
    if isinstance(time_column_info, dict):
        time_table = _logical_table_name(time_column_info.get("table"))
        if not time_table or not source_logical_table or time_table == source_logical_table:
            time_column = time_column_info.get("column")
    time_grain = str(time_info.get("grain") or "month")

    select_clauses: List[str] = []
    group_exprs: List[str] = []
    table_columns: List[Tuple[str, str]] = []
    for column in dimension_columns:
        source_quoted = _snowflake_silver_source_identifier(column)
        alias_quoted = _snowflake_quote_identifier(column)
        select_clauses.append(f"{source_quoted} AS {alias_quoted}")
        group_exprs.append(source_quoted)
        table_columns.append((column, "VARCHAR"))

    if time_column:
        period_expr = _snowflake_grain_expr(time_grain, str(time_column))
        select_clauses.append(f"{period_expr} AS \"period_start\"")
        group_exprs.append(period_expr)
        table_columns.append(("period_start", "TIMESTAMP_NTZ"))

    select_clauses.append(_snowflake_measure_expression(measure, value_alias))
    table_columns.append((value_alias, "FLOAT"))
    metadata_columns = [
        ("kpi_name", "VARCHAR"),
        ("gold_run_id", "VARCHAR"),
        ("gold_processed_timestamp", "TIMESTAMP_NTZ"),
        ("gold_upsert_key", "VARCHAR"),
    ]
    all_table_columns = [*table_columns, *metadata_columns]
    target_qname = _snowflake_qualified_name(*target_table.split("."))
    source_qname = _snowflake_qualified_name(*source_table.split("."))

    create_columns = ",\n    ".join(
        f"{_snowflake_quote_identifier(name)} {data_type}" for name, data_type in all_table_columns
    )
    alter_columns = "\n".join(
        f"ALTER TABLE {target_qname} ADD COLUMN IF NOT EXISTS {_snowflake_quote_identifier(name)} {data_type};"
        for name, data_type in all_table_columns
    )

    aggregate_select = ",\n        ".join(select_clauses)
    group_by_clause = f"\n    GROUP BY {', '.join(group_exprs)}" if group_exprs else ""
    final_columns = [name for name, _ in table_columns]
    upsert_parts = [
        _snowflake_string_literal(kpi_name),
        *[f"COALESCE(TO_VARCHAR({_snowflake_quote_identifier(name)}), '__NULL__')" for name in final_columns],
    ]
    upsert_expr = f"MD5(CONCAT_WS('||', {', '.join(upsert_parts)}))"

    insert_columns = [
        *[_snowflake_quote_identifier(name) for name in final_columns],
        '"kpi_name"',
        '"gold_run_id"',
        '"gold_processed_timestamp"',
        '"gold_upsert_key"',
    ]
    update_assignments = ",\n        ".join(
        f"target.{column} = source.{column}" for column in insert_columns if column != '"gold_upsert_key"'
    )
    insert_values = [f"source.{column}" for column in insert_columns]

    return f"""-- AUTO-GENERATED GOLD KPI SCRIPT
-- KPI: {kpi_name}
-- Source table: {source_table}
-- Target table: {target_table}
-- Expected runtime: Snowflake SQL
-- DO NOT EDIT MANUALLY

CREATE SCHEMA IF NOT EXISTS {_snowflake_qualified_name(gold_catalog, gold_schema)};

CREATE TABLE IF NOT EXISTS {target_qname} (
    {create_columns}
);

{alter_columns}

MERGE INTO {target_qname} AS target
USING (
    WITH aggregate_data AS (
        SELECT
        {aggregate_select}
        FROM {source_qname}{group_by_clause}
    )
    SELECT
        {", ".join(_snowflake_quote_identifier(name) for name in final_columns)},
        {_snowflake_string_literal(kpi_name)} AS "kpi_name",
        {_snowflake_string_literal(run_id)} AS "gold_run_id",
        CURRENT_TIMESTAMP() AS "gold_processed_timestamp",
        {upsert_expr} AS "gold_upsert_key"
    FROM aggregate_data
) AS source
ON target."gold_upsert_key" = source."gold_upsert_key"
WHEN MATCHED THEN UPDATE SET
        {update_assignments}
WHEN NOT MATCHED THEN INSERT (
        {", ".join(insert_columns)}
    )
    VALUES (
        {", ".join(insert_values)}
    );
"""


def _generate_one_mapping(
    mapping: Dict[str, Any],
    *,
    run_id: str,
    gold_schema: str,
    target_warehouse: str,
    gold_catalog: str = "",
    use_domain_kb: bool,
    dimension_contract: List[Dict[str, Any]] | None = None,
    include_dimension: bool = True,
) -> Dict[str, Any]:
    mapping, source_table_guard = _sanitize_gold_mapping(mapping)
    dimension_contract = dimension_contract or []
    kpi_name = str(mapping.get("kpi_name") or "KPI")
    kpi_id = _safe_identifier(kpi_name, "kpi")
    is_snowflake = str(target_warehouse or "").lower() == "snowflake"
    target_table = (
        _snowflake_target_fact_table(gold_catalog, gold_schema, kpi_id)
        if is_snowflake
        else _target_fact_table(gold_schema, kpi_id)
    )
    kb_cfg = get_domain_kb_config()
    use_domain_kb = bool(use_domain_kb) and kb_cfg.enabled
    if use_domain_kb:
        kb_query_parts = [
            kpi_name,
            str(mapping.get("source_silver_table") or ""),
            json.dumps(mapping.get("measure") or {}, default=str),
            json.dumps(mapping.get("grouping_dimensions") or [], default=str),
            json.dumps(mapping.get("join_paths") or [], default=str),
        ]
        kb_result = load_domain_kb(
            query_text=" ".join(kb_query_parts),
            top_k=kb_cfg.top_k_gold,
            max_chars=kb_cfg.max_chars_gold,
            content_types=None,
        )
    else:
        kb_result = {"context_text": "", "rows_retrieved": 0, "chars_injected": 0, "knowledge_base_id": kb_cfg.knowledge_base_id}

    if not _usable_mapping(mapping):
        return {
            "run_id": run_id,
            "kpi_name": kpi_name,
            "status": "BLOCKED",
            "reason": "Gold contract mapping is incomplete or requires formula certification.",
            "source_table": mapping.get("source_silver_table"),
            "target_table": target_table,
            "script_path": None,
            "dimension_script_path": None,
            "script_language": "sql" if is_snowflake else "python",
            "target_warehouse": str(target_warehouse or "databricks").lower(),
            "source_table_guard": source_table_guard,
            "domain_knowledge_base": {
                "enabled": use_domain_kb,
                "knowledge_base_id": kb_result.get("knowledge_base_id"),
                "rows_retrieved": kb_result.get("rows_retrieved", 0),
                "chars_injected": kb_result.get("chars_injected", 0),
            },
        }

    llm_requested = _llm_enabled_for_gold()
    generation_mode = "LLM" if llm_requested else "DETERMINISTIC"
    fallback_reason = None
    if is_snowflake and llm_requested:
        try:
            code = llm_generate_snowflake_gold_code(
                mapping=mapping,
                run_id=run_id,
                gold_catalog=gold_catalog,
                gold_schema=gold_schema,
            )
            repaired_code = _canonicalize_snowflake_gold_identifiers(code, mapping)
            generation_mode = "LLM_REPAIRED" if repaired_code != code else "LLM"
            code = repaired_code
            _validate_snowflake_gold_candidate(code, mapping, target_table)
        except Exception as first_exc:
            try:
                retry_code = llm_generate_snowflake_gold_code(
                    mapping=mapping,
                    run_id=run_id,
                    gold_catalog=gold_catalog,
                    gold_schema=gold_schema,
                    validation_feedback=str(first_exc),
                )
                repaired_retry = _canonicalize_snowflake_gold_identifiers(retry_code, mapping)
                _validate_snowflake_gold_candidate(repaired_retry, mapping, target_table)
                code = repaired_retry
                generation_mode = "LLM_RETRY_REPAIRED" if repaired_retry != retry_code else "LLM_RETRY"
            except Exception as retry_exc:
                fallback_reason = f"Snowflake Gold LLM generation failed: {first_exc}; retry failed: {retry_exc}"
                logger.warning(
                    "Gold Snowflake LLM generation and validation-feedback retry failed; deterministic fallback will be used: %s",
                    retry_exc,
                )
                code = generate_snowflake_gold_script(
                    mapping=mapping,
                    run_id=run_id,
                    gold_catalog=gold_catalog,
                    gold_schema=gold_schema,
                )
                generation_mode = "SNOWFLAKE_SQL_FALLBACK"
    elif is_snowflake:
        code = generate_snowflake_gold_script(
            mapping=mapping,
            run_id=run_id,
            gold_catalog=gold_catalog,
            gold_schema=gold_schema,
        )
        generation_mode = "SNOWFLAKE_SQL"
    elif llm_requested:
        try:
            code = llm_generate_gold_code(
                mapping=mapping,
                run_id=run_id,
                gold_schema=gold_schema,
                dimension_contract=dimension_contract,
                domain_reference_context=str(kb_result.get("context_text") or ""),
            )
            _validate_databricks_gold_candidate(code, mapping, gold_schema, dimension_contract)
        except Exception as first_exc:
            try:
                retry_code = llm_generate_gold_code(
                    mapping=mapping,
                    run_id=run_id,
                    gold_schema=gold_schema,
                    dimension_contract=dimension_contract,
                    validation_feedback=str(first_exc),
                    domain_reference_context=str(kb_result.get("context_text") or ""),
                )
                _validate_databricks_gold_candidate(retry_code, mapping, gold_schema, dimension_contract)
                code = retry_code
                generation_mode = "LLM_RETRY"
            except Exception as retry_exc:
                fallback_reason = f"Databricks Gold LLM generation failed: {first_exc}; retry failed: {retry_exc}"
                code = generate_gold_script(mapping=mapping, run_id=run_id, gold_schema=gold_schema)
                generation_mode = "DETERMINISTIC_FALLBACK"
                logger.warning(
                    "Gold Databricks LLM generation and validation-feedback retry failed; deterministic fallback will be used: %s",
                    retry_exc,
                    extra={"run_id": run_id, "node": "gold_generation", "kpi_name": kpi_name},
                )
    else:
        code = generate_gold_script(mapping=mapping, run_id=run_id, gold_schema=gold_schema)
    if not is_snowflake:
        _validate_python(code)

    if include_dimension:
        if is_snowflake:
            dimension_code = generate_snowflake_dimension_script(
                mapping=mapping,
                run_id=run_id,
                gold_catalog=gold_catalog,
                gold_schema=gold_schema,
            )
        else:
            dimension_code = generate_dimension_script(mapping=mapping, gold_schema=gold_schema)
    else:
        dimension_code = ""
    if dimension_code:
        if not is_snowflake:
            _validate_python(dimension_code)

    output_dir = _gold_output_dir_for(target_warehouse)
    os.makedirs(output_dir, exist_ok=True)
    extension = "sql" if is_snowflake else "py"
    script_path = os.path.join(output_dir, f"gold_kpi_{_run_slug(run_id)}_{kpi_id}.{extension}")
    with open(script_path, "w", encoding="utf-8") as f:
        f.write(code)
    dimension_script_path = None
    if dimension_code:
        dimension_extension = "sql" if is_snowflake else "py"
        dimension_script_path = os.path.join(output_dir, f"gold_dim_{_run_slug(run_id)}_{kpi_id}.{dimension_extension}")
        with open(dimension_script_path, "w", encoding="utf-8") as f:
            f.write(dimension_code)

    return {
        "run_id": run_id,
        "kpi_name": kpi_name,
        "status": "APPROVED",
        "source_table": mapping.get("source_silver_table"),
        "target_table": target_table,
        "script_path": script_path,
        "dimension_script_path": dimension_script_path,
        "script_language": "sql" if is_snowflake else "python",
        "target_warehouse": str(target_warehouse or "databricks").lower(),
        "generation_mode": generation_mode,
        "fallback_reason": fallback_reason,
        "time_grain": (mapping.get("time") or {}).get("grain"),
        "validation_columns": sorted(_mapping_source_columns(mapping)),
        "dimension_count": len(mapping.get("grouping_dimensions") or []),
        "kimball_dimension_count": len(_dimension_specs(mapping)),
        "dimension_contract": _dimension_specs(mapping),
        "join_count": len(mapping.get("join_paths") or []),
        "source_table_guard": source_table_guard,
        "domain_knowledge_base": {
            "enabled": use_domain_kb,
            "knowledge_base_id": kb_result.get("knowledge_base_id"),
            "rows_retrieved": kb_result.get("rows_retrieved", 0),
            "chars_injected": kb_result.get("chars_injected", 0),
        },
    }


def _write_bundle(
    *,
    generated_at: str,
    results: List[Dict[str, Any]],
    contract: Dict[str, Any],
    target_warehouse: str = "databricks",
) -> str:
    dimension_paths = sorted({str(item.get("dimension_script_path")) for item in results if item.get("dimension_script_path")})
    bundle = {
        "run_id": contract.get("run_id"),
        "generated_at": generated_at,
        "script_count": sum(1 for item in results if item.get("script_path")),
        "dimension_script_count": len(dimension_paths),
        "dimension_script_paths": dimension_paths,
        "blocked_count": sum(1 for item in results if item.get("status") == "BLOCKED"),
        "contract_status": contract.get("status"),
        "target_warehouse": str(target_warehouse or "databricks").lower(),
        "llm_enabled": _llm_enabled_for_gold(),
        "scripts": results,
    }
    os.makedirs(_gold_output_dir_for(target_warehouse), exist_ok=True)
    path = _bundle_path(target_warehouse)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(bundle, f, indent=2)
    run_path = _run_bundle_path(contract.get("run_id"), target_warehouse)
    with open(run_path, "w", encoding="utf-8") as f:
        json.dump(bundle, f, indent=2)
    return path


def _write_readme(
    *,
    generated_at: str,
    results: List[Dict[str, Any]],
    target_warehouse: str = "databricks",
) -> str:
    lines = [
        "# Gold Scripts",
        "",
        f"Generated at: `{generated_at}`",
        f"Generated scripts: `{sum(1 for item in results if item.get('script_path'))}`",
        f"Blocked mappings: `{sum(1 for item in results if item.get('status') == 'BLOCKED')}`",
        "",
        "| KPI | Source Silver | Target Gold | Status | Fact Script | Dimension Script | Mode |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in sorted(results, key=lambda row: str(row.get("kpi_name", ""))):
        script_path = str(item.get("script_path") or "")
        script_name = os.path.basename(script_path) if script_path else "-"
        script_link = f"[{script_name}]({script_path})" if script_path else "-"
        dimension_path = str(item.get("dimension_script_path") or "")
        dimension_name = os.path.basename(dimension_path) if dimension_path else "-"
        dimension_link = f"[{dimension_name}]({dimension_path})" if dimension_path else "-"
        lines.append(
            f"| `{item.get('kpi_name')}` | `{item.get('source_table')}` | "
            f"`{item.get('target_table')}` | `{item.get('status')}` | {script_link} | "
            f"{dimension_link} | `{item.get('generation_mode') or '-'}` |"
        )

    path = _readme_path(target_warehouse)
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    return path


def _write_ui(
    *,
    generated_at: str,
    results: List[Dict[str, Any]],
    target_warehouse: str = "databricks",
) -> str:
    rows: List[Dict[str, str]] = []
    for item in sorted(results, key=lambda row: str(row.get("kpi_name", ""))):
        script_path = str(item.get("script_path") or "")
        script_body = ""
        if script_path and os.path.exists(script_path):
            with open(script_path, "r", encoding="utf-8") as f:
                script_body = f.read()
        dimension_path = str(item.get("dimension_script_path") or "")
        dimension_body = ""
        if dimension_path and os.path.exists(dimension_path):
            with open(dimension_path, "r", encoding="utf-8") as f:
                dimension_body = f.read()
        rows.append(
            {
                "kpi_name": str(item.get("kpi_name") or ""),
                "source_table": str(item.get("source_table") or ""),
                "target_table": str(item.get("target_table") or ""),
                "status": str(item.get("status") or ""),
                "reason": str(item.get("reason") or ""),
                "generation_mode": str(item.get("generation_mode") or ""),
                "fallback_reason": str(item.get("fallback_reason") or ""),
                "script_body": script_body,
                "dimension_body": dimension_body,
            }
        )

    payload = json.dumps(rows)
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Gold Scripts Viewer</title>
  <style>
    body {{ margin: 0; font-family: Segoe UI, Tahoma, sans-serif; background: #f3f5f7; color: #1f2937; }}
    main {{ width: min(1120px, calc(100vw - 32px)); margin: 28px auto; }}
    .hero, .card {{ background: white; border: 1px solid #d8dee4; border-radius: 8px; padding: 18px; margin-bottom: 14px; }}
    input {{ width: 100%; padding: 11px; border: 1px solid #cbd5e1; border-radius: 6px; margin: 12px 0; }}
    pre {{ white-space: pre-wrap; word-break: break-word; background: #f8fafc; border: 1px solid #d8dee4; border-radius: 6px; padding: 14px; overflow: auto; }}
    .meta {{ color: #667085; }}
    .badge {{ display: inline-block; padding: 3px 8px; border-radius: 999px; background: #e5e7eb; font-size: 12px; }}
  </style>
</head>
<body>
  <main>
    <section class="hero">
      <h1>Gold Scripts Viewer</h1>
      <p class="meta">Generated at: {generated_at} | Mappings: {len(rows)}</p>
      <input id="search" type="search" placeholder="Search gold scripts..." />
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
      const filtered = rows.filter((row) => [row.kpi_name, row.source_table, row.target_table, row.status].join(" ").toLowerCase().includes(query));
      list.innerHTML = filtered.map((row) => `
        <article class="card">
          <h3>${{escapeHtml(row.kpi_name)}} <span class="badge">${{escapeHtml(row.status)}}</span></h3>
          <p class="meta">Source: ${{escapeHtml(row.source_table)}} | Target: ${{escapeHtml(row.target_table)}} | Mode: ${{escapeHtml(row.generation_mode || "-")}}</p>
          ${{row.reason ? `<p class="meta">${{escapeHtml(row.reason)}}</p>` : ""}}
          ${{row.fallback_reason ? `<p class="meta">${{escapeHtml(row.fallback_reason)}}</p>` : ""}}
          ${{row.dimension_body ? `<h4>Dimension Script</h4><pre><code>${{escapeHtml(row.dimension_body)}}</code></pre>` : ""}}
          ${{row.script_body ? `<h4>Fact Script</h4><pre><code>${{escapeHtml(row.script_body)}}</code></pre>` : ""}}
        </article>
      `).join("");
    }}
    search.addEventListener("input", render);
    render();
  </script>
</body>
</html>
"""
    path = _ui_path(target_warehouse)
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    return path


def _persist_gold_generation(*, state: Stage01State, bundle: Dict[str, Any]) -> None:
    run_id = str(state.get("run_id") or "GOLD_RUN")
    fingerprint = str(state.get("fingerprint") or run_id)
    ai_store_db_writer(
        run_id=run_id,
        stage="Gold Code Generation",
        artifact_type="GOLD_GENERATION",
        payload=bundle,
        schema_version="GoldGeneration_v1",
        prompt_version="HYBRID_KIMBALL_SPARK_GOLD_v1" if _llm_enabled_for_gold() else "DETERMINISTIC_KIMBALL_SPARK_GOLD_v1",
        faithfulness_status="PASSED",
        token_count=0,
        input_tokens=0,
        output_tokens=0,
        fingerprint=fingerprint,
    )


def gold_code_generation_node(state: Stage01State) -> Stage01State:
    new_state = state.copy()
    contract = _load_contract(state)
    target_warehouse = _target_warehouse(state)
    mappings = _normalize_contract_mappings(
        contract,
        canonicalize_columns=target_warehouse == "databricks",
    )

    if not contract:
        new_state["gold_generation_status"] = "SKIPPED"
        new_state["gold_generation_error"] = "No gold generation contract found."
        return new_state

    if not mappings:
        new_state["gold_generation_status"] = "SKIPPED"
        new_state["gold_generation_error"] = "Gold contract has no KPI mappings."
        return new_state

    run_id = str(state.get("run_id") or contract.get("run_id") or "GOLD_RUN")
    if target_warehouse == "snowflake":
        gold_catalog = str(state.get("gold_catalog") or _snowflake_gold_catalog())
        gold_schema = str(state.get("gold_schema") or _snowflake_gold_schema())
    else:
        gold_catalog = str(state.get("gold_catalog") or "")
        gold_schema = str(state.get("gold_schema") or os.getenv("GOLD_SCHEMA", "gold"))
    generated_at = datetime.utcnow().isoformat()
    shared_dimension_mapping = _shared_dimension_mapping(mappings)
    databricks_dimension_contract = (
        _dimension_specs(shared_dimension_mapping) if target_warehouse == "databricks" else []
    )

    results = [
        _generate_one_mapping(
            mapping,
            run_id=run_id,
            gold_schema=gold_schema,
            gold_catalog=gold_catalog,
            target_warehouse=target_warehouse,
            use_domain_kb=bool(state.get("use_domain_kb")),
            dimension_contract=databricks_dimension_contract,
            include_dimension=False,
        )
        for mapping in mappings
        if isinstance(mapping, dict)
    ]

    # ponytail: one shared mart artifact avoids generating/executing the same
    # source-table grain DIM/FCT tables once per KPI.
    enriched_metadata = (
        state.get("enrichment_review_artifact")
        or state.get("enriched_metadata")
        or {}
    )
    if isinstance(enriched_metadata, dict) and "enrichment_artifact" in enriched_metadata:
        enriched_metadata = enriched_metadata.get("enrichment_artifact") or {}
    source_table_grain_specs = _source_table_grain_specs(contract, mappings, enriched_metadata)
    shared_dimension_code = ""
    if target_warehouse == "snowflake" and source_table_grain_specs:
        shared_dimension_code = generate_snowflake_source_table_mart_script(
            specs=source_table_grain_specs,
            run_id=run_id,
            gold_catalog=gold_catalog,
            gold_schema=gold_schema,
        )
    elif shared_dimension_mapping.get("grouping_dimensions"):
        if target_warehouse == "snowflake":
            shared_dimension_code = generate_snowflake_dimension_script(
                mapping=shared_dimension_mapping,
                run_id=run_id,
                gold_catalog=gold_catalog,
                gold_schema=gold_schema,
            )
        else:
            shared_dimension_code = generate_dimension_script(shared_dimension_mapping, gold_schema)
            _validate_python(shared_dimension_code)

    shared_dimension_path = None
    if shared_dimension_code:
        output_dir = _gold_output_dir_for(target_warehouse)
        os.makedirs(output_dir, exist_ok=True)
        dimension_extension = "sql" if target_warehouse == "snowflake" else "py"
        shared_dimension_path = os.path.join(
            output_dir, f"gold_dimensions_{_run_slug(run_id)}.{dimension_extension}"
        )
        with open(shared_dimension_path, "w", encoding="utf-8") as f:
            f.write(shared_dimension_code)
        for item in results:
            if item.get("status") == "APPROVED":
                item["dimension_script_path"] = shared_dimension_path
                dimension_contract = (
                    source_table_grain_specs
                    if target_warehouse == "snowflake" and source_table_grain_specs
                    else _dimension_specs(shared_dimension_mapping)
                )
                item["dimension_contract"] = dimension_contract
                item["kimball_dimension_count"] = len(dimension_contract)
                break

    bundle = {
        "generated_at": generated_at,
        "script_count": sum(1 for item in results if item.get("script_path")),
        "dimension_script_count": sum(1 for item in results if item.get("dimension_script_path")),
        "dimension_script_path": shared_dimension_path,
        "blocked_count": sum(1 for item in results if item.get("status") == "BLOCKED"),
        "contract_status": contract.get("status"),
        "target_warehouse": target_warehouse,
        "llm_enabled": _llm_enabled_for_gold(),
        "scripts": results,
    }
    bundle_path = _write_bundle(
        generated_at=generated_at,
        results=results,
        contract=contract,
        target_warehouse=target_warehouse,
    )
    readme_path = _write_readme(generated_at=generated_at, results=results, target_warehouse=target_warehouse)
    ui_path = _write_ui(generated_at=generated_at, results=results, target_warehouse=target_warehouse)

    try:
        _persist_gold_generation(state=state, bundle=bundle)
    except Exception as exc:
        logger.warning("Gold generation artifact persistence failed: %s", exc, extra={"run_id": run_id, "node": "gold_generation"})

    generated_count = bundle["script_count"]
    blocked_count = bundle["blocked_count"]
    if generated_count == 0 and blocked_count > 0:
        status = "FAILED"
        error = "All gold mappings are blocked."
    elif blocked_count:
        status = "COMPLETED_WITH_WARNINGS"
        error = f"{blocked_count} gold mapping(s) blocked."
    else:
        status = "COMPLETED"
        error = None

    new_state["gold_generation_status"] = status
    new_state["gold_generation_error"] = error
    new_state["gold_generated_at"] = generated_at
    new_state["gold_generation_results"] = results
    new_state["gold_generation_bundle_path"] = bundle_path
    new_state["gold_generation_readme_path"] = readme_path
    new_state["gold_generation_ui_path"] = ui_path
    new_state["gold_catalog"] = gold_catalog
    new_state["status"] = "PIPELINE_COMPLETED" if status != "FAILED" else "FAILED"

    logger.info(
        "Gold generation completed: %d scripts, %d blocked target_warehouse=%s",
        generated_count,
        blocked_count,
        target_warehouse,
        extra={"run_id": run_id, "node": "gold_generation"},
    )
    return new_state
