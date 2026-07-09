from __future__ import annotations

from typing import Any, Dict

from services.pipeline_runtime import load_bronze_scripts, load_silver_scripts
from utilis.logger import logger


def _load_script_bundle(loader, bundle_name: str, run_id: str, checkpoint: Dict[str, Any]) -> Dict[str, Any]:
    try:
        return loader(run_id, checkpoint) or {}
    except Exception:
        logger.exception("Failed to load %s scripts for run_id=%s", bundle_name, run_id)
        return {}


def _map_bronze_feed(item: Dict[str, Any], checkpoint: Dict[str, Any]) -> Dict[str, Any]:
    config_payload = item.get("bronze_config") or item.get("generated_bronze_config") or {}
    return {
        "feed_summary": item.get("feed_summary") or f"{item.get('vendor') or 'Vendor'}.{item.get('entity') or 'Feed'}",
        "source_type": item.get("source_type") or config_payload.get("source_type") or checkpoint.get("source"),
        "vendor": item.get("vendor") or config_payload.get("vendor"),
        "entity": item.get("entity") or config_payload.get("entity") or item.get("table"),
        "table": item.get("table") or item.get("table_name"),
        "database_name": item.get("database_name"),
        "schema_name": item.get("schema_name"),
        "script_path": item.get("script_path"),
        "target_warehouse": item.get("target_warehouse"),
        "script_language": item.get("script_language"),
        "source_columns": item.get("source_columns") or [],
        "file_format": item.get("file_format") or config_payload.get("file_format"),
        "approved_schema": config_payload.get("schema_columns") or item.get("approved_schema") or [],
        "primary_keys": item.get("primary_keys") or config_payload.get("primary_keys") or [],
        "watermark_column": item.get("watermark_column") or config_payload.get("watermark_column"),
        "landing_path": item.get("landing_path") or config_payload.get("landing_path"),
        "target_table": item.get("target_table") or config_payload.get("target_table"),
        "bronze_output_path": item.get("bronze_output_path") or config_payload.get("bronze_output_path"),
        "checkpoint_path": item.get("checkpoint_path") or config_payload.get("checkpoint_path"),
        "schema_location": item.get("schema_location") or config_payload.get("schema_location"),
        "generated_bronze_config": item.get("generated_bronze_config") or config_payload,
        "generated_bronze_script": item.get("generated_bronze_script") or item.get("script_body") or "",
        "validation_checklist": item.get("validation_checklist") or [],
        "validation_issues": item.get("validation_issues") or [],
        "plan_valid": item.get("plan_valid", item.get("status") == "COMPLETED"),
        "review_status": item.get("review_status") or "PENDING",
    }


def bronze_review_from_scripts(run_id: str, checkpoint: Dict[str, Any]) -> Dict[str, Any]:
    bundle = _load_script_bundle(load_bronze_scripts, "bronze", run_id, checkpoint)
    scripts = bundle.get("scripts") or []
    if not scripts:
        return {}
    return {
        "run_id": run_id,
        "generated_at": bundle.get("generated_at") or checkpoint.get("bronze_generated_at"),
        "feeds": [_map_bronze_feed(item, checkpoint) for item in scripts],
    }


def _map_silver_item(item: Dict[str, Any]) -> Dict[str, Any]:
    primary_keys = item.get("primary_keys") or []
    return {
        "entity": item.get("entity") or item.get("table") or "Silver Item",
        "vendor": item.get("vendor"),
        "bronze_source": item.get("bronze_table") or item.get("source_table"),
        "silver_target": item.get("silver_table") or item.get("target_table"),
        "primary_keys": primary_keys,
        "watermark_column": item.get("watermark_column"),
        "transformations": [
            "column rename (bronze -> business names)",
            "type casting",
            "deduplication",
            "null audit",
            "silver audit columns",
        ],
        "pii_masking_rules": item.get("pii_masking_rules") or [],
        "merge_strategy": "MERGE upsert" if primary_keys else "overwrite",
        "llm_enhanced": item.get("llm_enhanced", False),
        "generated_silver_script": item.get("generated_silver_script") or item.get("script_body") or "",
    }


def silver_review_from_scripts(run_id: str, checkpoint: Dict[str, Any]) -> Dict[str, Any]:
    bundle = _load_script_bundle(load_silver_scripts, "silver", run_id, checkpoint)
    scripts = bundle.get("scripts") or []
    if not scripts:
        return {}
    return {
        "run_id": run_id,
        "generated_at": bundle.get("generated_at") or checkpoint.get("silver_generated_at"),
        "items": [_map_silver_item(item) for item in scripts],
    }
