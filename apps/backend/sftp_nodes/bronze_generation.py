from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Mapping


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


def bronze_code_generation_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """Reuse native generation and add the one file format it does not yet emit."""
    from nodes.bronze_gen import bronze_code_generation_node as generate

    result = generate(state)
    files = _file_by_source_table(state)
    for artifact in result.get("bronze_generation_results") or []:
        source = files.get(str(artifact.get("source_table") or "").casefold())
        if str((source or {}).get("file_format") or "").lower() != "xml":
            continue
        row_tag = str(((source or {}).get("parser_options") or {}).get("rowTag") or "").strip()
        if not row_tag:
            raise ValueError(f"Approved XML source is missing its inferred rowTag: {artifact.get('source_table')}")
        path = Path(str(artifact.get("script_path") or ""))
        script = path.read_text(encoding="utf-8")
        path.write_text(add_xml_reader(script, row_tag), encoding="utf-8")
    return result
