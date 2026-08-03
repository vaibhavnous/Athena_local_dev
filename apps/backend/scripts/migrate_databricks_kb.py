from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List


BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from utilis.env import load_backend_env

load_backend_env()

from services import databricks_runtime
from utilis.domain_kb import KB_TARGETS, upsert_kb_rows_to_pinecone


DEFAULT_SOURCE_VIEW = "workspace.athena.domain_kb_pinecone_migration_vw"
REQUIRED_COLUMNS = {
    "kb_row_id",
    "knowledge_base_id",
    "domain_profile",
    "kb_content_type",
    "embedding_text",
    "prompt_context",
    "is_active",
}
OPTIONAL_COLUMNS = {
    "database_name",
    "schema_name",
    "table_name",
    "column_name",
    "kpi_name",
    "gold_rule_json",
    "rule_type",
    "rule_value",
    "confidence",
}


def _qualified_name(value: str) -> str:
    name = str(value or "").strip()
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError("Databricks KB source must be a three-part catalog.schema.view name")
    return name


def _export_notebook(source_view: str, max_rows: int) -> str:
    return f'''# Databricks notebook source
import json
from pyspark.sql import functions as F

source_view = {source_view!r}
df = spark.table(source_view)
required = {sorted(REQUIRED_COLUMNS)!r}
optional = {sorted(OPTIONAL_COLUMNS)!r}
missing = [name for name in required if name not in df.columns]
if missing:
    raise ValueError("Migration view is missing required columns: " + ", ".join(missing))

df = df.filter(F.coalesce(F.col("is_active").cast("boolean"), F.lit(False)))
active_count = df.count()
if active_count > {max_rows}:
    raise ValueError(f"Migration view has {{active_count}} active rows; configured export limit is {max_rows}")

selected = [name for name in required + optional if name in df.columns]
rows = [row.asDict(recursive=True) for row in df.select(*selected).collect()]
dbutils.notebook.exit(json.dumps({{"source_view": source_view, "active_count": active_count, "rows": rows}}, default=str))
'''


def export_active_kb_rows(source_view: str, max_rows: int = 5000) -> List[Dict[str, Any]]:
    source_view = _qualified_name(source_view)
    notebook_path = "/Shared/Athena/kb_migration/export_domain_kb_pinecone_migration"
    databricks_runtime._workspace_import_notebook(
        notebook_path,
        _export_notebook(source_view, max(1, int(max_rows))),
    )
    submitted = databricks_runtime._submit_run(
        notebook_path,
        run_name="Athena Databricks to Pinecone KB export",
    )
    run_id = int(submitted["run_id"])
    state = databricks_runtime._wait_for_run(run_id)
    if state.get("result_state") != "SUCCESS":
        output = databricks_runtime._get_run_output(databricks_runtime._task_run_id(state))
        detail = str(output.get("error") or state.get("state_message") or state.get("result_state"))
        raise RuntimeError(
            f"Databricks KB export failed (run_id={run_id}): {detail}"
        )
    output = databricks_runtime._get_run_output(databricks_runtime._task_run_id(state))
    raw_result = str((output.get("notebook_output") or {}).get("result") or "")
    if not raw_result:
        raise RuntimeError("Databricks KB export returned no notebook result")
    payload = json.loads(raw_result)
    rows = payload.get("rows") or []
    return [dict(row) for row in rows if isinstance(row, dict)]


def validate_and_group_rows(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped = {kb_id: [] for kb_id in KB_TARGETS}
    seen_ids: set[str] = set()
    for position, row in enumerate(rows, start=1):
        missing = sorted(name for name in REQUIRED_COLUMNS if name not in row)
        if missing:
            raise ValueError(f"Migration row {position} is missing: {', '.join(missing)}")
        row_id = str(row.get("kb_row_id") or "").strip()
        kb_id = str(row.get("knowledge_base_id") or "").strip()
        if not row_id or not str(row.get("embedding_text") or "").strip():
            raise ValueError(f"Migration row {position} has an empty ID or embedding text")
        if row_id in seen_ids:
            raise ValueError(f"Duplicate migration kb_row_id: {row_id}")
        if kb_id not in grouped:
            raise ValueError(f"Unsupported migration knowledge_base_id: {kb_id}")
        seen_ids.add(row_id)
        grouped[kb_id].append(row)
    return grouped


def _summary(grouped: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
    return {
        kb_id: {
            "rows": len(rows),
            "content_types": dict(sorted(Counter(str(row.get("kb_content_type")) for row in rows).items())),
        }
        for kb_id, rows in grouped.items()
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate the normalized Databricks domain KB view to Pinecone.")
    parser.add_argument(
        "--source-view",
        default=os.getenv("DATABRICKS_KB_SOURCE_TABLE", DEFAULT_SOURCE_VIEW),
    )
    parser.add_argument("--knowledge-base-id", choices=sorted(KB_TARGETS), action="append")
    parser.add_argument("--max-rows", type=int, default=5000)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-refresh", action="store_true")
    args = parser.parse_args()

    rows = export_active_kb_rows(args.source_view, args.max_rows)
    grouped = validate_and_group_rows(rows)
    selected = set(args.knowledge_base_id or KB_TARGETS)
    grouped = {kb_id: kb_rows for kb_id, kb_rows in grouped.items() if kb_id in selected}
    result: Dict[str, Any] = {
        "source_view": _qualified_name(args.source_view),
        "dry_run": args.dry_run,
        "summary": _summary(grouped),
    }
    if not args.dry_run:
        migrations = {}
        for kb_id, kb_rows in grouped.items():
            if not kb_rows:
                migrations[kb_id] = {"rows_upserted": 0, "skipped": "no active rows"}
                continue
            domain, index_name, _ = KB_TARGETS[kb_id]
            migrations[kb_id] = upsert_kb_rows_to_pinecone(
                kb_rows,
                index_name=index_name,
                namespace=kb_id,
                knowledge_base_id=kb_id,
                domain_profile=domain,
                refresh=not args.no_refresh,
            )
        result["migrations"] = migrations
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
