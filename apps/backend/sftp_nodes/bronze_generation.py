from __future__ import annotations

import json
import os
import re
from pathlib import Path
from pathlib import PurePosixPath
from typing import Any, Dict, Mapping
from urllib.parse import urlsplit


_PARQUET_BRANCH = '''elif FILE_FORMAT == "parquet":
    df = spark.read.format("parquet").load(SOURCE_PATH)
else:
    raise ValueError(f"Unsupported FILE_FORMAT: {FILE_FORMAT}")'''

def _file_by_source_table(state: Mapping[str, Any]) -> Dict[str, Mapping[str, Any]]:
    return {
        f"{item.get('database_name')}.{item.get('schema_name')}.{item.get('table_name')}".casefold(): item
        for item in state.get("certified_tables") or []
        if isinstance(item, Mapping)
    }


def add_xml_reader(script: str, row_tag: str) -> str:
    if _PARQUET_BRANCH not in script:
        raise RuntimeError("Generated Bronze template changed; XML reader could not be added.")
    replacement = f'''elif FILE_FORMAT == "parquet":
    df = spark.read.format("parquet").load(SOURCE_PATH)
elif FILE_FORMAT == "xml":
    df = spark.read.format("xml").option("rowTag", {row_tag!r}).load(SOURCE_PATH)
else:
    raise ValueError(f"Unsupported FILE_FORMAT: {{FILE_FORMAT}}")'''
    updated = script.replace(_PARQUET_BRANCH, replacement, 1)
    compile(updated, "<generated-adls-bronze>", "exec")
    return updated


def _volume_root() -> str:
    root = str(
        os.getenv("DATABRICKS_ADLS_BRONZE_VOLUME")
        or "/Volumes/workspace/bronze_schema/vol_bronze"
    ).strip().rstrip("/")
    parts = PurePosixPath(root).parts
    if len(parts) != 5 or parts[:2] != ("/", "Volumes") or any(not part for part in parts[2:]):
        raise ValueError("DATABRICKS_ADLS_BRONZE_VOLUME must be /Volumes/<catalog>/<schema>/<volume>.")
    return root


def _path_slug(value: Any, fallback: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "_", str(value or "")).strip("._-") or fallback


def volume_source_path(run_id: str, source: Mapping[str, Any]) -> str:
    source_path = str(source.get("source_path") or source.get("landing_path") or "").strip()
    remote_path = str(source.get("remote_path") or "").strip()
    file_name = str(source.get("file_name") or PurePosixPath(urlsplit(source_path).path or remote_path).name).strip()
    if not source_path or not file_name:
        raise ValueError(f"Approved ADLS source identity is incomplete: {source!r}")
    del run_id  # Stable cache paths are intentionally shared by every pipeline run.
    file_format = _path_slug(
        source.get("file_format") or source.get("format") or PurePosixPath(file_name).suffix.lstrip("."),
        "file",
    ).lower()
    return f"{_volume_root()}/{file_format}/{_path_slug(file_name, 'source_file')}"


def use_volume_source(script: str, original_path: str, volume_path: str) -> str:
    if original_path not in script:
        raise RuntimeError("Generated Bronze template changed; original ADLS source path was not found.")
    updated = script.replace(original_path, volume_path)
    if f"SOURCE_PATH = {volume_path!r}" not in updated:
        raise RuntimeError("Generated Bronze template changed; Volume source path could not be activated.")
    compile(updated, "<generated-adls-bronze>", "exec")
    return updated


def add_metadata_runtime_identity(script: str) -> str:
    """Scope cached-file Bronze writes to the queue's logical work identity."""
    updated, identity_count = re.subn(
        r"RUN_ID = ['\"][^'\"]+['\"]\n",
        '''RUNTIME_CONTEXT = globals().get("ATHENA_RUNTIME_CONTEXT")
if not isinstance(RUNTIME_CONTEXT, dict):
    raise RuntimeError("Metadata Bronze execution requires ATHENA_RUNTIME_CONTEXT")
RUN_ID = str(RUNTIME_CONTEXT.get("runtime_run_id") or "")
LOGICAL_WORK_ID = str(RUNTIME_CONTEXT.get("logical_work_id") or "")
if not RUN_ID or not LOGICAL_WORK_ID:
    raise RuntimeError("Metadata runtime context is missing run or logical-work identity")
''',
        script,
        count=1,
    )
    lineage_marker = '.withColumn("source_table", lit('
    lineage_start = updated.find(lineage_marker)
    if lineage_start < 0:
        raise RuntimeError("Generated Bronze template changed; source lineage was not found.")
    lineage_end = updated.find("\n)", lineage_start)
    if lineage_end < 0:
        raise RuntimeError("Generated Bronze template changed; source lineage was incomplete.")
    updated = (
        updated[:lineage_end]
        + '\n    .withColumn("_logical_work_id", lit(LOGICAL_WORK_ID))'
        + updated[lineage_end:]
    )
    write_marker = '''    .mode("append")
    .option("mergeSchema", "true")'''
    write_replacement = '''    .mode("overwrite")
    .option("replaceWhere", f"`_logical_work_id` = '{LOGICAL_WORK_ID.replace(chr(39), chr(39) * 2)}'")
    .option("mergeSchema", "true")'''
    if identity_count != 1 or updated.count(write_marker) != 1:
        raise RuntimeError("Generated Bronze template changed; metadata runtime identity could not be added.")
    updated = updated.replace(write_marker, write_replacement, 1)
    compile(updated, "<generated-adls-bronze>", "exec")
    return updated


def bronze_code_generation_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """Reuse native generation and add ADLS-only runtime details."""
    from nodes.bronze_gen import bronze_code_generation_node as generate

    result = generate(state)
    files = _file_by_source_table(state)
    databricks = str(state.get("target_warehouse") or "databricks").lower() == "databricks"
    activated_paths: set[str] = set()
    for artifact in result.get("bronze_generation_results") or []:
        source = files.get(str(artifact.get("source_table") or "").casefold())
        original_path = str(
            (source or {}).get("source_path")
            or (source or {}).get("landing_path")
            or artifact.get("source_path")
            or artifact.get("landing_path")
            or ""
        ).strip()
        if not source or not original_path:
            raise ValueError(f"Approved ADLS source path is missing: {artifact.get('source_table')}")
        path = Path(str(artifact.get("script_path") or ""))
        runtime_path = None
        if databricks:
            runtime_path = volume_source_path(str(state.get("run_id") or "run"), source)
            if runtime_path.casefold() in activated_paths:
                raise ValueError(f"Multiple approved ADLS files resolve to one Volume path: {runtime_path}")
            activated_paths.add(runtime_path.casefold())
            script = path.read_text(encoding="utf-8")
            script = use_volume_source(script, original_path, runtime_path)
            script = add_metadata_runtime_identity(script)
            if str((source or {}).get("file_format") or "").lower() == "xml":
                row_tag = str(((source or {}).get("parser_options") or {}).get("rowTag") or "").strip()
                if not row_tag:
                    raise ValueError(f"Approved XML source is missing its inferred rowTag: {artifact.get('source_table')}")
                script = add_xml_reader(script, row_tag)
            path.write_text(script, encoding="utf-8")
        artifact.update({
            "adls_source_path": original_path,
            "adls_remote_path": str(source.get("remote_path") or original_path),
            "adls_source_etag": str(source.get("etag") or "").strip('"'),
            "adls_source_size": int(source.get("file_size") or 0),
            "adls_source_format": str(source.get("file_format") or source.get("format") or "").lower(),
            "adls_parser_options": dict(source.get("parser_options") or {}),
            "source_file_name": str(source.get("file_name") or PurePosixPath(urlsplit(original_path).path).name),
            "volume_source_path": runtime_path,
        })

    bundle_path = Path(str(result.get("bronze_generation_bundle_path") or ""))
    if bundle_path.is_file():
        bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
        bundle["scripts"] = result.get("bronze_generation_results") or []
        bundle_path.write_text(json.dumps(bundle, indent=2), encoding="utf-8")
    return result
