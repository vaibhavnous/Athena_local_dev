

from __future__ import annotations

import ast
import json
import os
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, TypedDict

from langchain_core.messages import HumanMessage, SystemMessage

from nodes.req_extraction import get_llm
from state import Stage01State
from utilis.db import build_source_jdbc_url
from utilis.logger import logger


# ------------------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------------------

BRONZE_MAX_WORKERS = int(os.environ.get("BRONZE_MAX_WORKERS", "4"))
BRONZE_LLM_MAX_PROMPT_CHARS = int(os.environ.get("BRONZE_LLM_MAX_PROMPT_CHARS", "45000"))
BRONZE_LLM_TIMEOUT_SECONDS = int(os.environ.get("BRONZE_LLM_TIMEOUT_SECONDS", "60"))
BRONZE_LLM_SYSTEM_MSG = (
    "You are a senior Spark data engineer. Return only production-ready Python code. "
    "Do not include markdown fences or explanations."
)
SNOWFLAKE_BRONZE_LLM_SYSTEM_MSG = (
    "You are a senior Snowflake data engineer. Return only production-ready Snowflake SQL. "
    "Do not include markdown fences or explanations."
)

DANGEROUS_SQL_KEYWORDS = {
    "DELETE",
    "MERGE",
    "TRUNCATE",
    "UPDATE",
    "ALTER",
}
DESTRUCTIVE_SNOWFLAKE_SQL_KEYWORDS = {
    "ALTER",
    "DELETE",
    "DROP",
    "MERGE",
    "TRUNCATE",
    "UPDATE",
}


class BronzeTableRef(TypedDict):
    database_name: str
    schema_name: str
    table_name: str


def _normalize_bronze_column_name(column_name: str) -> str:
    return str(column_name or "").strip().lower()


def _spark_cast_type(column: Dict[str, Any]) -> str | None:
    data_type = str(column.get("data_type") or "").strip().lower()
    precision = column.get("numeric_precision")
    scale = column.get("numeric_scale")

    if data_type in {"date", "datetime", "datetime2", "smalldatetime", "datetimeoffset", "time", "timestamp"}:
        return "timestamp"
    if data_type in {"int", "integer", "smallint", "tinyint"}:
        return "int"
    if data_type == "bigint":
        return "bigint"
    if data_type in {"bit", "boolean"}:
        return "boolean"
    if data_type in {"float", "real"}:
        return "double"
    if data_type in {"decimal", "numeric", "money", "smallmoney"}:
        if precision and scale is not None:
            safe_precision = min(int(precision), 38)
            return f"decimal({safe_precision},{int(scale)})"
        return "decimal(38,10)"
    return None


def _metadata_tables(state: Stage01State) -> List[Dict[str, Any]]:
    discovered = state.get("discovered_metadata") or {}
    if isinstance(discovered, dict):
        return discovered.get("tables", []) or []
    return []


def _cast_rules_for_table(state: Stage01State, table_name: str) -> Dict[str, str]:
    rules: Dict[str, str] = {}
    for table in _metadata_tables(state):
        if str(table.get("table_name") or "").lower() != table_name.lower():
            continue
        for column in table.get("columns", []) or []:
            column_name = _normalize_bronze_column_name(str(column.get("column_name") or ""))
            cast_type = _spark_cast_type(column)
            if column_name and cast_type:
                rules[column_name] = cast_type
        break
    return rules


def _metadata_for_table(state: Stage01State, table_name: str) -> Dict[str, Any]:
    for table in _metadata_tables(state):
        if str(table.get("table_name") or "").lower() == table_name.lower():
            return table
    return {}


# ------------------------------------------------------------------------------
# OUTPUT DIR
# ------------------------------------------------------------------------------

def _bronze_output_dir() -> str:
    return os.path.join(os.getcwd(), "generated_code", "bronze")


def _bronze_output_dir_for(target_warehouse: str = "databricks") -> str:
    warehouse = str(target_warehouse or "databricks").lower()
    if warehouse == "snowflake":
        return os.path.join(os.getcwd(), "generated_code", "snowflake", "bronze")
    return _bronze_output_dir()


def _run_slug(run_id: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]+", "_", str(run_id or "run")).strip("_")[:48] or "run"


def _bronze_readme_path(target_warehouse: str = "databricks") -> str:
    return os.path.join(_bronze_output_dir_for(target_warehouse), "README.md")


def _bronze_ui_path(target_warehouse: str = "databricks") -> str:
    return os.path.join(_bronze_output_dir_for(target_warehouse), "index.html")


def _resolve_tables_for_bronze(state: Stage01State) -> List[BronzeTableRef]:
    raw_tables = state.get("certified_tables") or state.get("nominated_tables") or []
    resolved: List[BronzeTableRef] = []

    for item in raw_tables:
        if isinstance(item, dict):
            database_name = str(item.get("database_name") or "").strip()
            schema_name = str(item.get("schema_name") or "dbo").strip()
            table_name = str(item.get("table_name") or "").strip()
        else:
            database_name = ""
            schema_name = "dbo"
            table_name = str(item or "").strip()

        if not table_name:
            continue

        resolved.append(
            {
                "database_name": database_name or "insurance",
                "schema_name": schema_name or "dbo",
                "table_name": table_name,
            }
        )

    return resolved


# ------------------------------------------------------------------------------
# HARD VALIDATION
# ------------------------------------------------------------------------------

def _validate_python(code: str) -> None:
    compile(code, "<bronze_generated>", "exec")
    ast.parse(code)


def _detect_dangerous_sql(code: str) -> None:
    upper = code.upper()
    for kw in DANGEROUS_SQL_KEYWORDS:
        if f"{kw} " in upper:
            raise ValueError(f"Dangerous SQL keyword detected: {kw}")


def _validate_snowflake_sql(sql: str) -> None:
    upper = str(sql or "").upper()
    required = ("CREATE SCHEMA", "CREATE TABLE", "INSERT INTO")
    missing = [token for token in required if token not in upper]
    if missing:
        raise ValueError(f"Snowflake bronze SQL is missing required statements: {', '.join(missing)}")
    for keyword in DESTRUCTIVE_SNOWFLAKE_SQL_KEYWORDS:
        if re.search(rf"\b{keyword}\b", upper):
            raise ValueError(f"Disallowed Snowflake SQL keyword detected: {keyword}")


def _strip_code_fences(raw: str) -> str:
    raw = raw.strip()
    if raw.startswith("```"):
        raw = "\n".join(raw.split("\n")[1:]).rsplit("```", 1)[0].strip()
    return raw


def _llm_enabled_for_bronze() -> bool:
    return os.getenv("ATHENA_ENABLE_LLM_BRONZE_ENHANCEMENT", "false").lower() in {"1", "true", "yes", "on"}


def _llm_enabled_for_snowflake_bronze() -> bool:
    return os.getenv("ATHENA_ENABLE_LLM_SNOWFLAKE_BRONZE_ENHANCEMENT", "false").lower() in {"1", "true", "yes", "on"}


def _enhance_with_llm(code: str, metadata: Dict[str, Any]) -> str:
    provider = os.getenv("ATHENA_LLM_PROVIDER", "azure_openai")
    model = os.getenv("ATHENA_BRONZE_LLM_MODEL")
    llm = get_llm(
        provider=provider,
        model=model,
        temperature=0.0,
        request_timeout=BRONZE_LLM_TIMEOUT_SECONDS,
    )

    prompt = f"""
Enhance this deterministic Spark Bronze ingestion script.

Metadata:
{json.dumps(metadata, indent=2, default=str)}

Requirements:
- Preserve the same source table and target table.
- Preserve JDBC loading and Delta append behavior.
- Normalize column names deterministically.
- Improve safe casts, date parsing, null handling, and ingestion metadata where metadata supports it.
- Add concise comments only where they clarify non-obvious logic.
- Do not generate DELETE, UPDATE, MERGE, TRUNCATE, or ALTER statements.
- Do not remove existing validation behavior.
- Return only a complete Python script.

Current script:
{code}
""".strip()

    if len(prompt) > BRONZE_LLM_MAX_PROMPT_CHARS:
        raise ValueError(
            f"Bronze LLM enhancement prompt too large: {len(prompt)} chars > {BRONZE_LLM_MAX_PROMPT_CHARS}"
        )

    response = llm.invoke(
        [
            SystemMessage(content=BRONZE_LLM_SYSTEM_MSG),
            HumanMessage(content=prompt),
        ]
    )
    enhanced = _strip_code_fences(str(response.content))
    if not enhanced:
        raise ValueError("Bronze LLM enhancement returned empty code")
    return enhanced


def _maybe_enhance_with_llm(code: str, metadata: Dict[str, Any]) -> tuple[str, bool, str | None]:
    if not _llm_enabled_for_bronze():
        return code, False, None

    try:
        enhanced = _enhance_with_llm(code, metadata)
        _validate_python(enhanced)
        _detect_dangerous_sql(enhanced)
        return enhanced, True, None
    except Exception as exc:
        logger.warning(
            "Bronze LLM enhancement failed; using deterministic template: %s",
            exc,
            extra={"node": "bronze_gen", "pass": "llm_enhancement"},
        )
        return code, False, str(exc)[:500]


def _enhance_snowflake_with_llm(sql: str, metadata: Dict[str, Any]) -> str:
    provider = os.getenv("ATHENA_LLM_PROVIDER", "azure_openai")
    model = os.getenv("ATHENA_SNOWFLAKE_BRONZE_LLM_MODEL") or os.getenv("ATHENA_BRONZE_LLM_MODEL")
    llm = get_llm(
        provider=provider,
        model=model,
        temperature=0.0,
        request_timeout=BRONZE_LLM_TIMEOUT_SECONDS,
    )

    prompt = f"""
Enhance this deterministic Snowflake Bronze ingestion SQL.

Metadata:
{json.dumps(metadata, indent=2, default=str)}

Requirements:
- Return only complete Snowflake SQL.
- Preserve the same source and target objects.
- Preserve the INSERT INTO load pattern.
- Preserve audit columns run_id, ingestion_timestamp, source_system, and source_table.
- Keep statements idempotent where possible.
- Use Snowflake-native safe casts and timestamp handling.
- Do not generate DROP, DELETE, UPDATE, MERGE, TRUNCATE, or ALTER statements.
- Do not add explanations or markdown fences.

Current SQL:
{sql}
""".strip()

    if len(prompt) > BRONZE_LLM_MAX_PROMPT_CHARS:
        raise ValueError(
            f"Snowflake Bronze LLM enhancement prompt too large: {len(prompt)} chars > {BRONZE_LLM_MAX_PROMPT_CHARS}"
        )

    response = llm.invoke(
        [
            SystemMessage(content=SNOWFLAKE_BRONZE_LLM_SYSTEM_MSG),
            HumanMessage(content=prompt),
        ]
    )
    enhanced = _strip_code_fences(str(response.content))
    if not enhanced:
        raise ValueError("Snowflake Bronze LLM enhancement returned empty SQL")
    return enhanced


def _maybe_enhance_snowflake_with_llm(sql: str, metadata: Dict[str, Any]) -> tuple[str, bool, str | None]:
    if not _llm_enabled_for_snowflake_bronze():
        return sql, False, None

    try:
        enhanced = _enhance_snowflake_with_llm(sql, metadata)
        _validate_snowflake_sql(enhanced)
        return enhanced, True, None
    except Exception as exc:
        logger.warning(
            "Snowflake Bronze LLM enhancement failed; using deterministic template: %s",
            exc,
            extra={"node": "bronze_gen", "pass": "snowflake_llm_enhancement"},
        )
        return sql, False, str(exc)[:500]


def _write_bronze_readme(
    *,
    results: List[Dict[str, object]],
    generated_at: str,
    bronze_catalog: str,
    bronze_schema: str,
    target_warehouse: str = "databricks",
) -> str:
    lines = [
        "# Bronze Scripts",
        "",
        f"Generated at: `{generated_at}`",
        f"Script count: `{len(results)}`",
        "",
        "## Catalog",
        "",
        "| Source | Target | Script | Status |",
        "| --- | --- | --- | --- |",
    ]

    for item in sorted(results, key=lambda row: (str(row.get("database_name", "")), str(row.get("schema_name", "")), str(row.get("table", "")))):
        database_name = str(item.get("database_name") or "insurance")
        schema_name = str(item.get("schema_name") or "dbo")
        table_name = str(item.get("table") or "")
        script_path = str(item.get("script_path") or "")
        script_name = os.path.basename(script_path) if script_path else "-"
        source_name = f"`{database_name}.{schema_name}.{table_name}`"
        target_name = f"`{bronze_catalog}.{bronze_schema}.bronze_{table_name}`"
        script_link = f"[{script_name}]({script_path})" if script_path else "-"
        status = f"`{item.get('status', '-')}`"
        lines.append(f"| {source_name} | {target_name} | {script_link} | {status} |")

    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- Open this file instead of opening each generated script manually.",
            "- Use the script links above to jump directly to a specific bronze ingestion file.",
        ]
    )

    readme_path = _bronze_readme_path(target_warehouse)
    os.makedirs(os.path.dirname(readme_path), exist_ok=True)
    with open(readme_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    return readme_path


def _write_bronze_ui(
    *,
    results: List[Dict[str, object]],
    generated_at: str,
    bronze_catalog: str,
    bronze_schema: str,
    target_warehouse: str = "databricks",
) -> str:
    rows: List[Dict[str, str]] = []
    for item in sorted(results, key=lambda row: (str(row.get("database_name", "")), str(row.get("schema_name", "")), str(row.get("table", "")))):
        table_name = str(item.get("table") or "")
        script_path = str(item.get("script_path") or "")
        script_body = ""
        if script_path and os.path.exists(script_path):
            with open(script_path, "r", encoding="utf-8") as f:
                script_body = f.read()
        rows.append(
            {
                "source": f"{str(item.get('database_name') or 'insurance')}.{str(item.get('schema_name') or 'dbo')}.{table_name}",
                "target": f"{bronze_catalog}.{bronze_schema}.bronze_{table_name}",
                "script_name": os.path.basename(script_path),
                "script_path": script_path,
                "script_body": script_body,
                "status": str(item.get("status") or "-"),
            }
        )

    payload = json.dumps(rows)
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Bronze Scripts Viewer</title>
  <style>
    :root {{
      --bg: #f5efe6;
      --panel: #fffaf3;
      --ink: #1f2937;
      --muted: #6b7280;
      --line: #e7dccb;
      --accent: #0f766e;
      --accent-soft: #dff5f2;
      --shadow: 0 18px 45px rgba(31, 41, 55, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", Tahoma, sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(15, 118, 110, 0.12), transparent 28%),
        linear-gradient(180deg, #fbf6ee 0%, var(--bg) 100%);
      min-height: 100vh;
    }}
    .shell {{
      width: min(1100px, calc(100vw - 32px));
      margin: 32px auto;
      background: rgba(255, 250, 243, 0.9);
      border: 1px solid var(--line);
      border-radius: 24px;
      box-shadow: var(--shadow);
      overflow: hidden;
    }}
    .hero {{
      padding: 28px 32px 20px;
      border-bottom: 1px solid var(--line);
      background:
        linear-gradient(135deg, rgba(15, 118, 110, 0.08), rgba(255, 250, 243, 0.92)),
        repeating-linear-gradient(135deg, transparent 0, transparent 14px, rgba(231, 220, 203, 0.3) 14px, rgba(231, 220, 203, 0.3) 15px);
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: clamp(28px, 3vw, 40px);
      letter-spacing: -0.04em;
    }}
    .sub {{
      color: var(--muted);
      margin: 0;
      max-width: 760px;
    }}
    .meta {{
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      margin-top: 18px;
    }}
    .pill {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 8px 12px;
      font-size: 13px;
    }}
    .toolbar {{
      display: grid;
      grid-template-columns: 1fr 180px;
      gap: 14px;
      padding: 20px 32px;
      border-bottom: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.45);
    }}
    input, select {{
      width: 100%;
      padding: 12px 14px;
      border-radius: 14px;
      border: 1px solid var(--line);
      background: #fffdf9;
      color: var(--ink);
      font: inherit;
      outline: none;
    }}
    input:focus, select:focus {{
      border-color: var(--accent);
      box-shadow: 0 0 0 4px rgba(15, 118, 110, 0.12);
    }}
    .list {{
      padding: 24px 32px 32px;
      display: grid;
      gap: 16px;
    }}
    .card {{
      padding: 18px;
      border: 1px solid var(--line);
      border-radius: 18px;
      background: var(--panel);
      display: grid;
      gap: 10px;
    }}
    .row {{
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: start;
      flex-wrap: wrap;
    }}
    .source {{
      margin: 0;
      font-size: 18px;
      font-weight: 700;
      letter-spacing: -0.02em;
    }}
    .target {{
      margin: 0;
      color: var(--muted);
      font-size: 14px;
    }}
    .tag {{
      background: var(--accent-soft);
      color: var(--accent);
      border-radius: 999px;
      padding: 7px 10px;
      font-size: 12px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }}
    .empty {{
      display: none;
      padding: 8px 32px 32px;
      color: var(--muted);
    }}
    @media (max-width: 720px) {{
      .shell {{ width: min(100vw - 16px, 1100px); margin: 8px auto; border-radius: 18px; }}
      .hero, .toolbar, .list {{ padding-left: 18px; padding-right: 18px; }}
      .toolbar {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <main class="shell">
    <section class="hero">
      <h1>Bronze Scripts Viewer</h1>
      <p class="sub">A simple HTML page for reading generated bronze ingestion scripts in one place. Search by source table, target table, or script name.</p>
      <div class="meta">
        <span class="pill">Generated at: {generated_at}</span>
        <span class="pill">Scripts: {len(rows)}</span>
        <span class="pill">Target schema: {bronze_catalog}.{bronze_schema}</span>
      </div>
    </section>

    <section class="toolbar">
      <input id="search" type="search" placeholder="Search claim, policy, bronze_, dbo..." />
      <select id="status">
        <option value="">All statuses</option>
        <option value="APPROVED">APPROVED</option>
      </select>
    </section>

    <section id="list" class="list"></section>
    <p id="empty" class="empty">No bronze scripts match the current filter.</p>
  </main>

  <script>
    const rows = {payload};
    const list = document.getElementById("list");
    const empty = document.getElementById("empty");
    const search = document.getElementById("search");
    const status = document.getElementById("status");

    function render() {{
      const query = search.value.trim().toLowerCase();
      const selectedStatus = status.value;
      const filtered = rows.filter((row) => {{
        const haystack = [row.source, row.target, row.script_name].join(" ").toLowerCase();
        const queryMatch = !query || haystack.includes(query);
        const statusMatch = !selectedStatus || row.status === selectedStatus;
        return queryMatch && statusMatch;
      }});

      list.innerHTML = filtered.map((row) => `
        <article class="card">
          <div class="row">
            <div>
              <p class="source">${{row.source}}</p>
              <p class="target">Target: ${{row.target}}</p>
            </div>
            <span class="tag">${{row.status}}</span>
          </div>
          <pre style="margin:0; overflow:auto; background:#fffdf9; border:1px solid var(--line); border-radius:14px; padding:16px; font-size:13px; line-height:1.5;"><code>${{escapeHtml(row.script_body)}}</code></pre>
        </article>
      `).join("");

      empty.style.display = filtered.length ? "none" : "block";
    }}

    function escapeHtml(value) {{
      return String(value)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;");
    }}

    search.addEventListener("input", render);
    status.addEventListener("change", render);
    render();
  </script>
</body>
</html>
"""

    ui_path = _bronze_ui_path(target_warehouse)
    os.makedirs(os.path.dirname(ui_path), exist_ok=True)
    with open(ui_path, "w", encoding="utf-8") as f:
        f.write(html)

    return ui_path


# ------------------------------------------------------------------------------
# BRONZE SCRIPT TEMPLATE (POC‑LOCKED)
# ------------------------------------------------------------------------------

def _snowflake_quote_identifier(value: str) -> str:
    cleaned = str(value or "").strip()
    if not cleaned:
        raise ValueError("Snowflake identifier cannot be empty.")
    return '"' + cleaned.replace('"', '""') + '"'


def _snowflake_qualified_name(*parts: str) -> str:
    return ".".join(_snowflake_quote_identifier(part) for part in parts if str(part or "").strip())


def _snowflake_string_literal(value: str) -> str:
    return "'" + str(value or "").replace("'", "''") + "'"


def _snowflake_type_from_metadata(column: Dict[str, Any]) -> str:
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


def _snowflake_type_from_spark_cast(cast_type: str) -> str:
    normalized = str(cast_type or "").strip().lower()
    decimal_match = re.fullmatch(r"decimal\((\d+),\s*(\d+)\)", normalized)
    if decimal_match:
        return f"NUMBER({min(int(decimal_match.group(1)), 38)},{int(decimal_match.group(2))})"
    if normalized in {"int", "integer", "bigint", "smallint", "tinyint"}:
        return "NUMBER(38,0)"
    if normalized in {"double", "float", "real"}:
        return "FLOAT"
    if normalized == "boolean":
        return "BOOLEAN"
    if normalized == "date":
        return "DATE"
    if normalized == "timestamp":
        return "TIMESTAMP_NTZ"
    return "VARCHAR"


def _snowflake_columns(
    *,
    table_metadata: Dict[str, Any] | None,
    cast_rules: Dict[str, str] | None,
) -> List[Dict[str, str]]:
    columns: List[Dict[str, str]] = []
    seen: Dict[str, int] = {}
    metadata_columns = (table_metadata or {}).get("columns") or []

    for column in metadata_columns:
        original_name = str(column.get("column_name") or "").strip()
        if not original_name:
            continue
        normalized_name = _normalize_bronze_column_name(original_name)
        if normalized_name in seen:
            seen[normalized_name] += 1
            normalized_name = f"{normalized_name}_{seen[normalized_name]}"
        else:
            seen[normalized_name] = 0
        columns.append(
            {
                "source": original_name,
                "target": normalized_name,
                "type": _snowflake_type_from_metadata(column),
            }
        )

    if columns:
        return columns

    for column_name, cast_type in sorted((cast_rules or {}).items()):
        normalized_name = _normalize_bronze_column_name(column_name)
        if not normalized_name:
            continue
        columns.append(
            {
                "source": column_name,
                "target": normalized_name,
                "type": _snowflake_type_from_spark_cast(cast_type),
            }
        )
    return columns


def generate_snowflake_bronze_script(
    *,
    table: str,
    schema: str = "dbo",
    database: str = "insurance",
    run_id: str = "BRONZE_RUN",
    bronze_catalog: str = "main",
    bronze_schema: str = "bronze",
    cast_rules: Dict[str, str] | None = None,
    table_metadata: Dict[str, Any] | None = None,
) -> str:
    source_table = _snowflake_qualified_name(database, schema, table)
    target_table = _snowflake_qualified_name(bronze_catalog, bronze_schema, f"bronze_{table}")
    target_schema = _snowflake_qualified_name(bronze_catalog, bronze_schema)
    columns = _snowflake_columns(table_metadata=table_metadata, cast_rules=cast_rules)

    if columns:
        table_columns = ",\n    ".join(
            f"{_snowflake_quote_identifier(column['target'])} {column['type']}" for column in columns
        )
        insert_columns = ",\n    ".join(_snowflake_quote_identifier(column["target"]) for column in columns)
        select_columns = ",\n    ".join(
            f"TRY_CAST(src.{_snowflake_quote_identifier(column['source'])} AS {column['type']}) AS {_snowflake_quote_identifier(column['target'])}"
            for column in columns
        )
        create_table = f"""CREATE TABLE IF NOT EXISTS {target_table} (
    {table_columns},
    "run_id" VARCHAR,
    "ingestion_timestamp" TIMESTAMP_NTZ,
    "source_system" VARCHAR,
    "source_table" VARCHAR
);"""
        insert_sql = f"""INSERT INTO {target_table} (
    {insert_columns},
    "run_id",
    "ingestion_timestamp",
    "source_system",
    "source_table"
)
SELECT
    {select_columns},
    {_snowflake_string_literal(run_id)} AS "run_id",
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS "ingestion_timestamp",
    {_snowflake_string_literal(database)} AS "source_system",
    {_snowflake_string_literal(table)} AS "source_table"
FROM {source_table} AS src;"""
    else:
        create_table = f"""CREATE TABLE IF NOT EXISTS {target_table} AS
SELECT
    src.*,
    {_snowflake_string_literal(run_id)} AS "run_id",
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS "ingestion_timestamp",
    {_snowflake_string_literal(database)} AS "source_system",
    {_snowflake_string_literal(table)} AS "source_table"
FROM {source_table} AS src
WHERE 1 = 0;"""
        insert_sql = f"""INSERT INTO {target_table}
SELECT
    src.*,
    {_snowflake_string_literal(run_id)} AS "run_id",
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS "ingestion_timestamp",
    {_snowflake_string_literal(database)} AS "source_system",
    {_snowflake_string_literal(table)} AS "source_table"
FROM {source_table} AS src;"""

    return f"""-- AUTO-GENERATED BRONZE INGESTION SCRIPT
-- Source: {database}.{schema}.{table}
-- Expected runtime: Snowflake SQL
-- Target table: {bronze_catalog}.{bronze_schema}.bronze_{table}
-- DO NOT EDIT MANUALLY

CREATE SCHEMA IF NOT EXISTS {target_schema};

{create_table}

{insert_sql}
"""


def generate_bronze_script(
    *,
    table: str,
    schema: str = "dbo",
    database: str = "insurance",
    run_id: str = "BRONZE_RUN",
    bronze_catalog: str = "main",
    bronze_schema: str = "bronze",
    source_jdbc_url: str | None = None,
    cast_rules: Dict[str, str] | None = None,
) -> str:
    if not source_jdbc_url:
        raise ValueError(f"Missing source JDBC URL for {database}.{schema}.{table}.")

    cast_rules = cast_rules or {}

    return f'''
"""
AUTO-GENERATED BRONZE INGESTION SCRIPT

Source: {database}.{schema}.{table}
Expected runtime: Spark / Databricks with Delta support
Target table: {bronze_catalog}.{bronze_schema}.bronze_{table}

DO NOT EDIT MANUALLY
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

spark = SparkSession.builder.getOrCreate()

# ------------------------------------------------------------------------------
# Databricks catalog/schema setup
# ------------------------------------------------------------------------------

try:
    spark.sql("CREATE SCHEMA IF NOT EXISTS {bronze_schema}")
except Exception:
    print("Could not create schema '{bronze_schema}' in the current catalog")

RUN_ID = {run_id!r}
DEFAULT_SOURCE_JDBC_URL = {source_jdbc_url!r}
SOURCE_JDBC_URL_ENV = "ATHENA_SOURCE_JDBC_URL"
SOURCE_JDBC_URL = os.getenv(SOURCE_JDBC_URL_ENV) or os.getenv("SOURCE_JDBC_URL") or DEFAULT_SOURCE_JDBC_URL
if not SOURCE_JDBC_URL:
    raise RuntimeError(f"Missing source JDBC URL. Set {{SOURCE_JDBC_URL_ENV}} or SOURCE_JDBC_URL at runtime.")

TARGET_TABLE = "{bronze_schema}.bronze_{table}"
TEMP_VIEW = "bronze_src_{table}"
CAST_RULES = {repr(cast_rules)}
DATE_COLUMN_HINTS = ("date", "_dt", "timestamp", "created_at", "updated_at", "modified_at")
RECREATE_TARGET_ON_SCHEMA_CONFLICT = True

df = (
    spark.read.format("jdbc")
    .option("url", SOURCE_JDBC_URL)
    .option("dbtable", "{schema}.{table}")
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .load()
)

if not df.schema or not df.schema.fields:
    raise ValueError("Source read returned an empty schema for {database}.{schema}.{table}.")

normalized_columns = []
seen_columns = {{}}
for original_name in df.columns:
    normalized_name = original_name.lower()
    if normalized_name in seen_columns:
        seen_columns[normalized_name] += 1
        normalized_name = f"{{normalized_name}}_{{seen_columns[normalized_name]}}"
    else:
        seen_columns[normalized_name] = 0
    normalized_columns.append(col(original_name).alias(normalized_name))

df = df.select(*normalized_columns)

for column_name, target_type in CAST_RULES.items():
    if column_name in df.columns:
        df = df.withColumn(column_name, col(column_name).cast(target_type))

for column_name in df.columns:
    lower_name = column_name.lower()
    if column_name in CAST_RULES:
        continue
    if any(hint in lower_name for hint in DATE_COLUMN_HINTS):
        df = df.withColumn(column_name, col(column_name).cast("timestamp"))

df = (
    df
    .withColumn("run_id", lit(RUN_ID))
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_system", lit("{database}"))
    .withColumn("source_table", lit("{table}"))
)

df.createOrReplaceTempView(TEMP_VIEW)

if spark.catalog.tableExists(TARGET_TABLE):
    target_schema = {{
        field.name.lower(): field.dataType.simpleString().lower()
        for field in spark.table(TARGET_TABLE).schema.fields
    }}
    incoming_schema = {{
        field.name.lower(): field.dataType.simpleString().lower()
        for field in df.schema.fields
    }}
    schema_conflicts = [
        (name, target_schema[name], incoming_type)
        for name, incoming_type in incoming_schema.items()
        if name in target_schema and target_schema[name] != incoming_type
    ]

    if schema_conflicts:
        conflict_text = ", ".join(
            f"{{name}}: target={{target_type}}, incoming={{incoming_type}}"
            for name, target_type, incoming_type in schema_conflicts
        )
        if RECREATE_TARGET_ON_SCHEMA_CONFLICT:
            print(f"Recreating {{TARGET_TABLE}} due to schema conflicts: {{conflict_text}}")
            spark.sql(f"DROP TABLE IF EXISTS {{TARGET_TABLE}}")
        else:
            raise ValueError(f"Schema conflicts detected for {{TARGET_TABLE}}: {{conflict_text}}")

create_table_sql = (
    f"CREATE TABLE IF NOT EXISTS {{TARGET_TABLE}} "
    f"USING DELTA "
    f"AS SELECT * FROM {{TEMP_VIEW}} WHERE 1 = 0"
)
spark.sql(create_table_sql)

(
    df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print(f"SUCCESS: Bronze ingestion completed for {{TARGET_TABLE}}")
'''
# ------------------------------------------------------------------------------
# PER-TABLE GENERATION
# ------------------------------------------------------------------------------

def _generate_one_table(
    table_ref: BronzeTableRef,
    *,
    run_id: str,
    source_jdbc_url: str | None = None,
    bronze_catalog: str = "main",
    bronze_schema: str = "bronze",
    cast_rules: Dict[str, str] | None = None,
    table_metadata: Dict[str, Any] | None = None,
    target_warehouse: str = "databricks",
) -> Dict[str, object]:
    database_name = table_ref["database_name"]
    schema_name = table_ref["schema_name"]
    table_name = table_ref["table_name"]
    target_warehouse = str(target_warehouse or "databricks").lower()

    if target_warehouse == "snowflake":
        code = generate_snowflake_bronze_script(
            table=table_name,
            schema=schema_name,
            database=database_name,
            run_id=run_id,
            bronze_catalog=bronze_catalog,
            bronze_schema=bronze_schema,
            cast_rules=cast_rules or {},
            table_metadata=table_metadata or {},
        )
        enhancement_metadata = {
            "source_table": table_ref,
            "target_table": f"{bronze_catalog}.{bronze_schema}.bronze_{table_name}",
            "cast_rules": cast_rules or {},
            "table_metadata": table_metadata or {},
            "target_warehouse": "snowflake",
        }
        code, llm_enhanced, llm_error = _maybe_enhance_snowflake_with_llm(code, enhancement_metadata)
        _validate_snowflake_sql(code)
        extension = "sql"
    else:
        resolved_source_jdbc_url = source_jdbc_url or build_source_jdbc_url(database_name)

        code = generate_bronze_script(
            table=table_name,
            schema=schema_name,
            database=database_name,
            run_id=run_id,
            bronze_catalog=bronze_catalog,
            bronze_schema=bronze_schema,
            source_jdbc_url=resolved_source_jdbc_url,
            cast_rules=cast_rules or {},
        )

        enhancement_metadata = {
            "source_table": table_ref,
            "target_table": f"{bronze_catalog}.{bronze_schema}.bronze_{table_name}",
            "cast_rules": cast_rules or {},
            "table_metadata": table_metadata or {},
        }
        code, llm_enhanced, llm_error = _maybe_enhance_with_llm(code, enhancement_metadata)

        _validate_python(code)
        _detect_dangerous_sql(code)
        extension = "py"

    output_dir = _bronze_output_dir_for(target_warehouse)
    os.makedirs(output_dir, exist_ok=True)

    script_path = os.path.join(output_dir, f"bronze_ingest_{_run_slug(run_id)}_{table_name}.{extension}")

    with open(script_path, "w", encoding="utf-8") as f:
        f.write(code)

    return {
        "run_id": run_id,
        "table": table_name,
        "database_name": database_name,
        "schema_name": schema_name,
        "status": "APPROVED",
        "cast_rule_count": len(cast_rules or {}),
        "llm_enhanced": llm_enhanced,
        "llm_enhancement_error": llm_error,
        "target_warehouse": target_warehouse,
        "script_language": "sql" if target_warehouse == "snowflake" else "python",
        "script_path": script_path,
    }
# ------------------------------------------------------------------------------
# LANGGRAPH NODE
# ------------------------------------------------------------------------------

def bronze_code_generation_node(state: Stage01State) -> Stage01State:
    """
    Generates Bronze ingestion scripts for Gate 2 certified tables.
    """

    new_state = state.copy()

    results: List[Dict[str, object]] = []
    run_id = str(state.get("run_id") or "BRONZE_RUN")
    bronze_catalog = state.get("bronze_catalog") or "main"
    bronze_schema = state.get("bronze_schema") or "bronze"
    target_warehouse = str(state.get("target_warehouse") or "databricks").lower()

    table_refs = _resolve_tables_for_bronze(state)

    if not table_refs:
        new_state["bronze_generation_status"] = "SKIPPED"
        new_state["bronze_generation_error"] = "No certified_tables or nominated_tables available for Bronze generation."
        return new_state

    source_jdbc_url = state.get("source_jdbc_url")
    with ThreadPoolExecutor(max_workers=BRONZE_MAX_WORKERS) as executor:
        futures = [
            executor.submit(
                _generate_one_table,
                table_ref,
                run_id=run_id,
                source_jdbc_url=source_jdbc_url,
                bronze_catalog=bronze_catalog,
                bronze_schema=bronze_schema,
                cast_rules=_cast_rules_for_table(state, table_ref["table_name"]),
                table_metadata=_metadata_for_table(state, table_ref["table_name"]),
                target_warehouse=target_warehouse,
            )
            for table_ref in table_refs
        ]

        for f in as_completed(futures):
            results.append(f.result())

    # Write bundle summary
    bundle = {
        "run_id": run_id,
        "generated_at": datetime.utcnow().isoformat(),
        "source_database": table_refs[0]["database_name"],
        "target_warehouse": target_warehouse,
        "script_count": len(results),
        "scripts": results,
    }

    output_dir = _bronze_output_dir_for(target_warehouse)
    os.makedirs(output_dir, exist_ok=True)
    bundle_path = os.path.join(output_dir, f"{_run_slug(run_id)}_bronze_scripts.json")
    latest_bundle_path = os.path.join(output_dir, "bronze_scripts.json")
    with open(bundle_path, "w", encoding="utf-8") as f:
        json.dump(bundle, f, indent=2)
    with open(latest_bundle_path, "w", encoding="utf-8") as f:
        json.dump(bundle, f, indent=2)

    readme_path = _write_bronze_readme(
        results=results,
        generated_at=bundle["generated_at"],
        bronze_catalog=bronze_catalog,
        bronze_schema=bronze_schema,
        target_warehouse=target_warehouse,
    )
    ui_path = _write_bronze_ui(
        results=results,
        generated_at=bundle["generated_at"],
        bronze_catalog=bronze_catalog,
        bronze_schema=bronze_schema,
        target_warehouse=target_warehouse,
    )

    new_state["bronze_generation_status"] = "COMPLETED"
    new_state["bronze_generation_error"] = None
    new_state["bronze_generated_at"] = bundle["generated_at"]
    new_state["bronze_generation_results"] = results
    new_state["bronze_generation_bundle_path"] = bundle_path
    new_state["bronze_generation_readme_path"] = readme_path
    new_state["bronze_generation_ui_path"] = ui_path
    new_state["status"] = "PIPELINE_COMPLETED"

    return new_state
