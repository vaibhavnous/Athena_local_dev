from __future__ import annotations

from pathlib import Path
import uuid

import pytest

from services import databricks_runtime, pipeline_runtime, sftp_runtime
from sftp_nodes import bronze_code_generation, bronze_ingestion, gold_code_generation, hitl


def test_file_source_layer_refuses_disabled_databricks_execution(monkeypatch):
    monkeypatch.setattr(pipeline_runtime, "save_checkpoint_state", lambda *_: None)
    monkeypatch.setattr(databricks_runtime, "databricks_execution_enabled", lambda _layer: False)

    with pytest.raises(RuntimeError, match="execution is disabled"):
        hitl._execute_reviewed_layer(
            "run-1",
            {"run_id": "run-1", "target_warehouse": "databricks"},
            "bronze",
            {"feeds": []},
        )


def test_file_source_layer_requires_completed_databricks_execution(monkeypatch):
    monkeypatch.setattr(pipeline_runtime, "save_checkpoint_state", lambda *_: None)
    monkeypatch.setattr(databricks_runtime, "databricks_execution_enabled", lambda _layer: True)
    monkeypatch.setattr(
        databricks_runtime,
        "run_databricks_bronze_scripts",
        lambda state, **_: {**state, "databricks_bronze_execution_status": "FAILED"},
    )

    with pytest.raises(RuntimeError, match="did not complete"):
        hitl._execute_reviewed_layer(
            "run-2",
            {"run_id": "run-2", "target_warehouse": "databricks"},
            "bronze",
            {"feeds": []},
        )


def test_gate4_executes_bronze_before_merge_key_review(monkeypatch):
    from sftp_nodes import review_gates

    order = []
    checkpoint = {
        "run_id": "run-gate4",
        "source": "adls_gen2",
        "target_warehouse": "databricks",
        "bronze_generation_results": [{"entity": "claims"}],
        "bronze_review_artifact": {"feeds": []},
    }
    monkeypatch.setattr(pipeline_runtime, "load_checkpoint_state", lambda _run_id: checkpoint)
    monkeypatch.setattr(pipeline_runtime, "save_checkpoint_state", lambda *_: None)
    monkeypatch.setattr(review_gates, "sftp_gate4_node", lambda state: state)
    monkeypatch.setattr(
        hitl,
        "_execute_reviewed_layer",
        lambda _run_id, state, layer, _artifact: order.append(f"execute_{layer}") or state,
    )
    monkeypatch.setattr(
        review_gates,
        "bronze_validation_node",
        lambda state: order.append("validate_bronze") or state,
    )
    monkeypatch.setattr(
        pipeline_runtime,
        "_pause_for_silver_merge_key_review",
        lambda _run_id, state: order.append("resolve_merge_keys") or {
            **state,
            "status": "HITL_WAIT",
            "next_review_key": "silver_merge_key_review",
        },
    )

    result = hitl.submit_sftp_gate4_review("run-gate4")

    assert order == ["execute_bronze", "validate_bronze", "resolve_merge_keys"]
    assert result["status"] == "HITL_WAIT"
    assert result["next_review_key"] == "silver_merge_key_review"


def test_file_bronze_generation_uses_autoloader_checkpoint():
    script = bronze_code_generation._generate_script(
        {
            "source_type": "adls_gen2",
            "vendor": "Insurance",
            "entity": "claim_information",
            "file_format": "csv",
            "row_tag": None,
            "landing_path": "abfss://athena@account.dfs.core.windows.net/INSURANCE_SFTP/insurance/",
            "target_table": "workspace.bronze.insurance_claim_information_raw",
            "schema_location": "/Volumes/workspace/bronze/pipeline_artifacts/schemas/bronze/insurance/claim_information",
            "checkpoint_path": "/Volumes/workspace/bronze/pipeline_artifacts/checkpoints/bronze/insurance/claim_information",
            "schema_columns": [{"column_name": "ClaimID", "data_type": "integer"}],
        },
        run_id="run-autoloader",
        pipeline_version="v1",
    )

    assert ".readStream" in script
    assert "cloudFiles" in script
    assert "cloudFiles.schemaLocation" in script
    assert "checkpointLocation" in script
    assert ".trigger(availableNow=True)" in script


def test_bronze_ingestion_prefers_manifest_rows(monkeypatch):
    landing = Path.cwd() / ".tmp-unit-metadata-driven" / uuid.uuid4().hex / "landing"
    landing.mkdir(parents=True)
    ready_file = landing / "claims.csv"
    ready_file.write_text("ClaimID\n1\n", encoding="utf-8")
    updated_statuses = []

    monkeypatch.setattr(
        bronze_ingestion,
        "_manifest_ready_rows",
        lambda feed_id, run_id: [
            {
                "manifest_id": 42,
                "file_path": str(ready_file),
                "file_name": "claims.csv",
                "file_size": ready_file.stat().st_size,
            }
        ],
    )
    monkeypatch.setattr(
        bronze_ingestion,
        "_update_manifest_bronze_status",
        lambda manifest_ids, status: updated_statuses.append((manifest_ids, status)),
    )
    monkeypatch.setattr(bronze_ingestion, "_log_bronze_file_ingestion", lambda **_: None)

    result = bronze_ingestion.sftp_bronze_ingestion_node(
        {
            "run_id": "run-manifest",
            "landing_path": str(landing),
            "candidate_feed": {"feed_id": "feed-1", "vendor": "Insurance", "entity": "claims"},
            "pulled_files": [],
            "files_pulled": 0,
        }
    )

    assert result["bronze_ingestion_status"] == "READY_FOR_BRONZE"
    assert result["bronze_ready_files"] == [str(ready_file)]
    assert result["bronze_ready_manifest"][0]["manifest_id"] == 42
    assert updated_statuses == [([42], "READY_FOR_BRONZE")]


def test_file_merge_key_approval_generates_silver(monkeypatch):
    from sftp_nodes import review_gates, silver_code_generation

    checkpoint = {
        "run_id": "run-file-merge-review",
        "source": "adls_gen2",
        "bronze_generation_results": [{"entity": "claims", "bronze_config": {"primary_keys": []}}],
        "enriched_metadata": {"columns": [{"entity": "claims", "column_name": "ClaimID"}]},
    }
    monkeypatch.setattr(pipeline_runtime, "load_checkpoint_state", lambda _run_id: checkpoint)
    monkeypatch.setattr(pipeline_runtime, "save_checkpoint_state", lambda *_: None)
    monkeypatch.setattr(pipeline_runtime, "ai_store_db_writer", lambda **_: None)
    monkeypatch.setattr(
        silver_code_generation,
        "sftp_silver_code_generation_node",
        lambda state: {
            **state,
            "silver_generation_status": "COMPLETED",
            "silver_review_artifact": {"items": [{"entity": "claims"}]},
        },
    )
    monkeypatch.setattr(review_gates, "sftp_gate5_node", lambda state: {**state, "status": "HITL_WAIT", "next_gate": 5})

    result = pipeline_runtime.submit_silver_merge_key_review(
        "run-file-merge-review",
        action="APPROVED",
        review_artifact={"feeds": [{"entity": "claims", "merge_keys": ["ClaimID"]}]},
    )

    assert result["next_gate"] == 5
    assert result["bronze_generation_results"][0]["bronze_config"]["primary_keys"] == ["ClaimID"]


def test_gate5_executes_silver_before_gold_generation(monkeypatch):
    from sftp_nodes import gold_code_generation as gold_node
    from sftp_nodes import review_gates

    order = []
    checkpoint = {
        "run_id": "run-gate5",
        "source": "adls_gen2",
        "target_warehouse": "databricks",
        "silver_review_artifact": {"items": [{"entity": "claims"}]},
    }
    monkeypatch.setattr(pipeline_runtime, "load_checkpoint_state", lambda _run_id: checkpoint)
    monkeypatch.setattr(pipeline_runtime, "save_checkpoint_state", lambda *_: None)
    monkeypatch.setattr(review_gates, "sftp_gate5_node", lambda state: state)
    monkeypatch.setattr(
        hitl,
        "_execute_reviewed_layer",
        lambda _run_id, state, layer, _artifact: order.append(f"execute_{layer}") or state,
    )
    monkeypatch.setattr(
        review_gates,
        "dq_validation_node",
        lambda state: order.append("validate_silver") or state,
    )
    def generate_gold(state):
        order.append("generate_gold")
        return {
            **state,
            "gold_generation_status": "COMPLETED",
            "gold_generation_results": [{"entity": "claims"}],
        }

    monkeypatch.setattr(gold_node, "sftp_gold_code_generation_node", generate_gold)

    result = hitl.submit_sftp_gate5_review("run-gate5")

    assert order == ["execute_silver", "validate_silver", "generate_gold"]
    assert result["status"] == "HITL_WAIT"
    assert result["next_review_key"] == "gold_review"


def test_generation_alone_never_completes_file_source_run():
    status = sftp_runtime._compute_status(
        checkpoint={
            "target_warehouse": "databricks",
            "gold_generation_status": "COMPLETED",
        },
        next_gate=None,
        gate5_decision="APPROVED",
        gold_generation_completed=True,
        silver_generation_completed=True,
        gate1_decision="APPROVED",
        gate2_decision="APPROVED",
        source_ingestion_completed=True,
        feed_review_ready=True,
    )

    assert status != "PIPELINE_COMPLETED"


def test_sftp_gold_uses_exact_silver_table_and_qualified_target(monkeypatch):
    output_dir = Path.cwd() / ".tmp-tests" / f"sftp_gold_{uuid.uuid4().hex}"
    output_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(gold_code_generation, "GOLD_OUTPUT_DIR", str(output_dir))
    monkeypatch.setattr(gold_code_generation, "ai_store_db_writer", lambda **_: None)
    monkeypatch.setattr(gold_code_generation, "GOLD_LLM_ENABLED", False)

    state = gold_code_generation.sftp_gold_code_generation_node(
        {
            "run_id": "run-qualified",
            "bronze_catalog": "workspace",
            "silver_catalog": "workspace",
            "gold_catalog": "workspace",
            "gold_schema": "gold",
            "silver_generation_results": [
                {
                    "entity": "claims",
                    "silver_table": "workspace.silver.vendor1_claims_clean",
                }
            ],
        }
    )

    result = state["gold_generation_results"][0]
    script = Path(result["script_path"]).read_text(encoding="utf-8")
    assert result["source_table"] == "workspace.silver.vendor1_claims_clean"
    assert result["target_table"] == "workspace.gold.gold_claims"
    assert 'SOURCE_TABLE = r"workspace.silver.vendor1_claims_clean"' in script
    assert 'TARGET_TABLE = r"workspace.gold.gold_claims"' in script
