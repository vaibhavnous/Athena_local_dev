from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Iterable

from dotenv import load_dotenv


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from services import databricks_runtime


def _clean(value: Any) -> str:
    return str(value or "").strip().strip("`").strip('"')


def _run_slug(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]+", "_", str(value or "cleanup")).strip("_")[:48] or "cleanup"


def _target(layer: str, catalog: str, schema: str, *, prefixes: Iterable[str]) -> dict[str, Any]:
    return {
        "layer": layer,
        "catalog": _clean(catalog),
        "schema": _clean(schema),
        "prefixes": [prefix for prefix in prefixes if prefix],
    }


def _default_targets() -> list[dict[str, Any]]:
    bronze_catalog = os.getenv("BRONZE_CATALOG", "main")
    silver_catalog = os.getenv("SILVER_CATALOG", bronze_catalog)
    gold_catalog = os.getenv("GOLD_CATALOG") or os.getenv("SILVER_CATALOG") or os.getenv("BRONZE_CATALOG") or ""
    return [
        _target("bronze", bronze_catalog, os.getenv("BRONZE_SCHEMA", "bronze"), prefixes=("bronze_",)),
        _target("silver", silver_catalog, os.getenv("SILVER_SCHEMA", "silver"), prefixes=("silver_",)),
        _target("gold", gold_catalog, os.getenv("GOLD_SCHEMA", "gold"), prefixes=("fact_", "dim_")),
    ]


def _parse_target(value: str) -> dict[str, Any]:
    parts = [_clean(part) for part in str(value or "").split(".")]
    if len(parts) == 1 and all(parts):
        catalog, schema = "", parts[0]
    elif len(parts) == 2 and all(parts):
        catalog, schema = parts[0], parts[1]
    else:
        raise ValueError(f"Invalid --target {value!r}; expected SCHEMA or CATALOG.SCHEMA")
    layer = schema.lower()
    prefixes = {
        "bronze": ("bronze_",),
        "silver": ("silver_",),
        "gold": ("fact_", "dim_"),
    }.get(layer, ())
    return _target(layer, catalog, schema, prefixes=prefixes)


def _selected_targets(layers: list[str], extra_targets: list[str], *, only_targets: bool = False) -> list[dict[str, Any]]:
    requested_layers = {layer.lower() for layer in layers} if layers else {"bronze", "silver", "gold"}
    targets = [] if only_targets else [target for target in _default_targets() if target["layer"] in requested_layers]
    targets.extend(_parse_target(item) for item in extra_targets)

    unique: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for target in targets:
        key = (target["layer"], target["catalog"].casefold(), target["schema"].casefold())
        if key in seen:
            continue
        seen.add(key)
        unique.append(target)
    return unique


def _cleanup_notebook(targets: list[dict[str, Any]], *, execute: bool, include_all: bool) -> str:
    payload = json.dumps(targets)
    return f'''# Databricks notebook source
import json

TARGETS = json.loads({payload!r})
EXECUTE = {execute!r}
INCLUDE_ALL = {include_all!r}

def quote_part(value):
    value = str(value or "").strip().strip("`")
    if not value:
        return ""
    return "`" + value.replace("`", "``") + "`"

def object_name(row, *names):
    for name in names:
        if hasattr(row, name):
            value = getattr(row, name)
            if value:
                return str(value)
        try:
            value = row[name]
            if value:
                return str(value)
        except Exception:
            pass
    values = list(row.asDict().values()) if hasattr(row, "asDict") else []
    return str(values[1]) if len(values) > 1 else ""

def should_drop(name, prefixes):
    return INCLUDE_ALL or any(str(name).lower().startswith(str(prefix).lower()) for prefix in prefixes)

def schema_fqn(target):
    catalog = quote_part(target.get("catalog"))
    schema = quote_part(target.get("schema"))
    return ".".join(part for part in (catalog, schema) if part)

results = []

for target in TARGETS:
    schema_name = schema_fqn(target)
    prefixes = target.get("prefixes") or []
    try:
        table_rows = spark.sql(f"SHOW TABLES IN {{schema_name}}").collect()
    except Exception as exc:
        results.append({{"target": schema_name, "status": "MISSING_OR_UNREADABLE", "error": str(exc)}})
        continue

    objects = []
    for row in table_rows:
        if getattr(row, "isTemporary", False):
            continue
        name = object_name(row, "tableName", "table_name")
        if name and should_drop(name, prefixes):
            objects.append(("TABLE", name))

    try:
        view_rows = spark.sql(f"SHOW VIEWS IN {{schema_name}}").collect()
        for row in view_rows:
            if getattr(row, "isTemporary", False):
                continue
            name = object_name(row, "viewName", "view_name", "tableName", "table_name")
            if name and should_drop(name, prefixes):
                objects.append(("VIEW", name))
    except Exception:
        pass

    seen = set()
    deduped = []
    for object_type, name in objects:
        key = (object_type, name.lower())
        if key not in seen:
            seen.add(key)
            deduped.append((object_type, name))

    for object_type, name in deduped:
        qualified = f"{{schema_name}}.{{quote_part(name)}}"
        sql = f"DROP {{object_type}} IF EXISTS {{qualified}}"
        if EXECUTE:
            spark.sql(sql)
        results.append({{
            "target": schema_name,
            "object_type": object_type,
            "object_name": name,
            "sql": sql,
            "status": "DROPPED" if EXECUTE else "DRY_RUN",
        }})

    if not deduped:
        results.append({{"target": schema_name, "status": "EMPTY_OR_NO_MATCH", "prefixes": prefixes}})

summary = {{
    "mode": "EXECUTE" if EXECUTE else "DRY_RUN",
    "include_all": INCLUDE_ALL,
    "targets": TARGETS,
    "objects": results,
    "object_count": sum(1 for item in results if item.get("object_name")),
}}
dbutils.notebook.exit(json.dumps(summary, default=str))
'''


def _parse_output(run_state: dict[str, Any]) -> dict[str, Any]:
    output_run_id = databricks_runtime._task_run_id(run_state)
    output = databricks_runtime._get_run_output(output_run_id) if output_run_id is not None else {}
    result = (output.get("notebook_output") or {}).get("result")
    if not result:
        return {"raw_output": output}
    return json.loads(result)


def main() -> int:
    parser = argparse.ArgumentParser(description="Dry-run or drop generated Athena Databricks Bronze/Silver/Gold tables/views.")
    parser.add_argument("--execute", action="store_true", help="Actually drop matching Databricks objects. Omit for dry-run.")
    parser.add_argument("--layer", action="append", choices=("bronze", "silver", "gold"), default=[], help="Layer to clean. Repeatable. Defaults to all layers.")
    parser.add_argument("--target", action="append", default=[], help="Extra CATALOG.SCHEMA or SCHEMA to clean. Repeatable.")
    parser.add_argument("--only-targets", action="store_true", help="Clean only --target values; do not add configured default layer schemas.")
    parser.add_argument("--include-all", action="store_true", help="Drop every table/view in each target schema instead of generated-name prefixes only.")
    parser.add_argument("--run-id", default="manual-cleanup", help="Label used for the temporary Databricks cleanup notebook path.")
    args = parser.parse_args()

    load_dotenv(REPO_ROOT / ".env", override=False)

    targets = _selected_targets(args.layer, args.target, only_targets=args.only_targets)
    if not targets:
        raise RuntimeError("No Databricks cleanup targets selected.")

    notebook_path = f"{databricks_runtime._workspace_root()}/{_run_slug(args.run_id)}/maintenance/__athena_cleanup_databricks_layers"
    databricks_runtime._workspace_import_notebook(
        notebook_path,
        _cleanup_notebook(targets, execute=args.execute, include_all=args.include_all),
    )
    run_payload = databricks_runtime._submit_run(
        notebook_path,
        run_name=f"Athena Databricks cleanup {'execute' if args.execute else 'dry-run'}",
    )
    run_state = databricks_runtime._wait_for_run(int(run_payload["run_id"]))
    if str(run_state.get("result_state") or "").upper() not in {"SUCCESS", "COMPLETED"}:
        raise RuntimeError(databricks_runtime._run_failure_detail(run_state))

    print(json.dumps(_parse_output(run_state), indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
