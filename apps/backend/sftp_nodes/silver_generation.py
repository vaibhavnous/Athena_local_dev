from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict

from services import dbt_snowflake_runtime
from utilis.logger import logger


def silver_code_generation_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """Generate ADLS Silver artifacts with the shared strict code generators."""
    from nodes import silver_gen as shared
    from services.file_metadata import prepare_file_silver_generation

    prepared = prepare_file_silver_generation(
        state,
        state.get("silver_merge_key_review_artifact") or {},
    )
    table_refs = prepared.get("silver_generation_table_refs")
    if not isinstance(table_refs, list) or not table_refs:
        raise ValueError("ADLS Silver generation requires approved file mapping references.")
    if any(not isinstance(item, dict) or not item.get("metadata_driven") for item in table_refs):
        raise ValueError("ADLS Silver generation requires strict metadata-driven table references.")

    new_state = dict(prepared)
    target_warehouse = str(prepared.get("target_warehouse") or "databricks").lower()
    execution_engine = dbt_snowflake_runtime.resolve_execution_engine(prepared)
    dbt_codegen = dbt_snowflake_runtime.snowflake_dbt_enabled(prepared)
    run_id = str(prepared.get("run_id") or "SILVER_POC_RUN_001")
    dbt_project_path = None
    if dbt_codegen:
        dbt_project_path = str(dbt_snowflake_runtime.write_snowflake_dbt_scaffold(prepared))
        shared._refresh_snowflake_dbt_models(run_id)
        shared._assert_unique_dbt_model_names(
            [f"silver_{table_ref['table_name']}" for table_ref in table_refs],
            layer="silver",
        )

    enriched_metadata = prepared.get("enrichment_review_artifact") or prepared.get("enriched_metadata") or {}
    if isinstance(enriched_metadata, dict) and "enrichment_artifact" in enriched_metadata:
        enriched_metadata = enriched_metadata.get("enrichment_artifact") or {}

    silver_catalog = str(prepared.get("silver_catalog") or prepared.get("bronze_catalog") or "main")
    silver_schema = str(prepared.get("silver_schema") or "silver")
    if target_warehouse == "snowflake":
        silver_catalog = shared._snowflake_silver_catalog()
        silver_schema = shared._snowflake_silver_schema()

    results = []
    with ThreadPoolExecutor(max_workers=shared.SILVER_MAX_WORKERS) as executor:
        futures = [
            executor.submit(
                shared._generate_one_table,
                table_ref,
                enriched_metadata=enriched_metadata,
                run_id=run_id,
                silver_catalog=silver_catalog,
                silver_schema=silver_schema,
                target_warehouse=target_warehouse,
                execution_engine=execution_engine,
            )
            for table_ref in table_refs
        ]
        for future in as_completed(futures):
            results.append(future.result())

    generated_at = datetime.now(timezone.utc).isoformat()
    bundle = {
        "run_id": run_id,
        "generated_at": generated_at,
        "script_count": len(results),
        "target_warehouse": target_warehouse,
        "execution_engine": execution_engine,
        "code_generation_format": "dbt" if dbt_codegen else "native",
        "dbt_project_path": dbt_project_path,
        "llm_enabled": False,
        "scripts": results,
    }
    output_dir = shared._silver_output_dir_for(target_warehouse)
    os.makedirs(output_dir, exist_ok=True)
    bundle_path = os.path.join(output_dir, f"{shared._run_slug(run_id)}_silver_scripts.json")
    latest_bundle_path = os.path.join(output_dir, "silver_scripts.json")
    for path in (bundle_path, latest_bundle_path):
        with open(path, "w", encoding="utf-8") as file:
            json.dump(bundle, file, indent=2)

    dbt_schema_path = None
    if dbt_codegen:
        dbt_schema_path = str(shared._write_snowflake_silver_dbt_metadata(run_id, results))
    readme_path = shared._write_silver_readme(
        results=results,
        generated_at=generated_at,
        target_warehouse=target_warehouse,
    )
    ui_path = shared._write_silver_ui(
        results=results,
        generated_at=generated_at,
        target_warehouse=target_warehouse,
    )
    gold_contract = shared._build_gold_generation_contract(
        state=prepared,
        results=results,
        enriched_metadata=enriched_metadata,
        generated_at=generated_at,
    )
    gold_contract_path = shared._write_gold_contract(gold_contract)
    try:
        shared._persist_generation_artifacts(
            state=prepared,
            silver_bundle=bundle,
            gold_contract=gold_contract,
        )
    except Exception as exc:
        logger.warning(
            "ADLS Silver artifact persistence failed: %s",
            exc,
            extra={"run_id": run_id, "node": "adls_silver_generation"},
        )

    new_state.update(
        {
            "silver_generation_status": "COMPLETED",
            "silver_generation_error": None,
            "silver_generated_at": generated_at,
            "silver_generation_results": results,
            "silver_generation_bundle_path": bundle_path,
            "silver_generation_readme_path": readme_path,
            "silver_generation_ui_path": ui_path,
            "gold_contract_status": gold_contract["status"],
            "gold_contract_error": "; ".join(gold_contract["warnings"])
            if gold_contract["warnings"]
            else None,
            "gold_generation_contract": gold_contract,
            "gold_contract_bundle_path": gold_contract_path,
            "status": "PIPELINE_COMPLETED",
        }
    )
    if dbt_codegen:
        new_state["snowflake_dbt_artifact_path"] = dbt_project_path
        new_state["snowflake_dbt_silver_schema_path"] = dbt_schema_path
    logger.info(
        "ADLS Silver generation completed: %d scripts target_warehouse=%s",
        len(results),
        target_warehouse,
        extra={"run_id": run_id, "node": "adls_silver_generation"},
    )
    return new_state
