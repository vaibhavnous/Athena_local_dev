"""
Refined HITL Controller (Clean + UI-friendly)
"""

from datetime import datetime
from typing import Any, Dict, Optional


def _run_visible_stage(stage_key: str, runner, state: Dict[str, Any]) -> Dict[str, Any]:
    from services.pipeline_runtime import run_with_minimum_stage_runtime

    return run_with_minimum_stage_runtime(stage_key, runner, state)


class HITLController:

    def __init__(self, mode="auto"):
        self.mode = mode

    # Main entry point
    def decide(self, gate_name, payload):

        result = self._init_result(gate_name, payload)

        if self.mode == "auto":
            decision, reason = self._auto_decision(gate_name, payload)
        elif self.mode == "manual":
            decision, reason = self._manual_decision(gate_name, payload)
        else:
            raise ValueError("Invalid HITL mode")

        # finalize result
        result["decision"] = decision
        result["reason"] = reason
        result["status"] = "COMPLETED"
        result["updated_at"] = self._now()

        return result

    # Initial result object
    def _init_result(self, gate_name, payload):
        return {
            "gate": gate_name,
            "status": "IN_PROGRESS",      # lifecycle state
            "decision": None,             # APPROVED / REJECTED
            "reason": None,               # explanation
            "created_at": self._now(),
            "updated_at": None,
            "payload_summary": self._summarize_payload(payload)
        }

    # Auto decision router
    def _auto_decision(self, gate_name, payload):

        if gate_name == "gate1":
            return self._gate1_rules(payload)

        elif gate_name == "gate2":
            return self._gate2_rules(payload)

        return "REJECTED", "Unknown gate"

    # Manual mode (for future UI integration)
    def _manual_decision(self, gate_name, payload):

        print(f"\nHITL Required: {gate_name}")
        print(payload)

        decision = input("Approve? (yes/no): ").strip().lower()

        if decision == "yes":
            return "APPROVED", "Approved by user"
        else:
            return "REJECTED", "Rejected by user"

    # Gate 1 rules
    def _gate1_rules(self, payload):

        kpis = payload.get("kpis", [])

        if not kpis:
            return "REJECTED", "No KPIs detected"

        return "APPROVED", f"{len(kpis)} KPIs validated"

    # Gate 2 rules
    def _gate2_rules(self, payload):

        entity = payload.get("entity")
        file_format = payload.get("format")
        rows = payload.get("sample_row_count", 0)

        if entity in [None, "unknown"]:
            return "REJECTED", "Entity not identified from folder"

        if file_format not in ["csv", "json"]:
            return "REJECTED", f"Unsupported format: {file_format}"

        if rows <= 0:
            return "REJECTED", "Dataset is empty"

        return "APPROVED", f"Feed valid ({rows} sample rows)"

    # Payload summary (UI safe)
    def _summarize_payload(self, payload):

        if not payload:
            return {}

        summary = {}

        # only expose useful keys
        for key in ["entity", "format", "sample_row_count"]:
            if key in payload:
                summary[key] = payload[key]

        if "kpis" in payload:
            summary["kpi_count"] = len(payload["kpis"])

        return summary

    # Timestamp
    def _now(self):
        return datetime.utcnow().isoformat()


# Global instance
hitl_controller = HITLController(mode="auto")


def _apply_reviewed_bronze_artifact_to_results(state: Dict[str, Any]) -> Dict[str, Any]:
    artifact = state.get("bronze_review_artifact") or {}
    feeds = artifact.get("feeds") or []
    if not feeds:
        return state

    reviewed_by_entity = {
        str(feed.get("entity") or feed.get("feed_name") or feed.get("table_name") or "").strip().lower(): feed
        for feed in feeds
        if str(feed.get("entity") or feed.get("feed_name") or feed.get("table_name") or "").strip()
    }
    results = []
    for item in state.get("bronze_generation_results") or []:
        if not isinstance(item, dict):
            continue
        key = str(item.get("entity") or item.get("feed_name") or item.get("table_name") or "").strip().lower()
        reviewed = reviewed_by_entity.get(key)
        if reviewed:
            config = dict(item.get("bronze_config") or item.get("generated_bronze_config") or {})
            reviewed_keys = reviewed.get("primary_keys") or reviewed.get("merge_keys") or []
            config["primary_keys"] = reviewed_keys
            item = {
                **item,
                "primary_keys": reviewed_keys,
                "merge_keys": reviewed_keys,
                "bronze_config": config,
                "generated_bronze_config": reviewed.get("generated_bronze_config") or config,
                "generated_bronze_script": reviewed.get("generated_bronze_script") or item.get("generated_bronze_script"),
            }
        results.append(item)
    return {**state, "bronze_generation_results": results}


def _execute_reviewed_layer(
    run_id: str,
    state: Dict[str, Any],
    layer: str,
    review_artifact: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    from services.pipeline_runtime import save_checkpoint_state

    target = str(state.get("target_warehouse") or "").strip().lower()
    execution_state = {
        **state,
        "status": "RUNNING",
        "background_stage": f"{layer}_code_execution",
        "next_gate": None,
        "resume_message": f"Executing approved {layer.capitalize()} scripts in {target.capitalize()}.",
    }
    save_checkpoint_state(run_id, execution_state)

    if target == "databricks":
        from services.databricks_runtime import (
            databricks_execution_enabled,
            run_databricks_bronze_scripts,
            run_databricks_silver_scripts,
        )

        if not databricks_execution_enabled(layer):
            raise RuntimeError(
                f"Databricks {layer.capitalize()} execution is disabled; refusing to generate the next layer."
            )
        runner = {
            "bronze": run_databricks_bronze_scripts,
            "silver": run_databricks_silver_scripts,
        }[layer]
        result = runner(execution_state, review_artifact=review_artifact, approved_only=True)
        status = result.get(f"databricks_{layer}_execution_status")
    elif target == "snowflake":
        if layer == "bronze":
            from services.snowflake_bronze_runtime import run_snowflake_bronze_scripts as runner
        else:
            from services.snowflake_silver_runtime import run_snowflake_silver_scripts as runner
        result = runner(execution_state, review_artifact=review_artifact, approved_only=True)
        status = result.get(f"snowflake_{layer}_execution_status")
    else:
        raise RuntimeError(f"Unsupported target warehouse for {layer.capitalize()} execution: {target or 'missing'}")

    if str(status or "").upper() != "COMPLETED":
        raise RuntimeError(
            f"{target.capitalize()} {layer.capitalize()} execution did not complete; refusing to generate the next layer."
        )
    return {**result, "background_stage": None}


def submit_sftp_gate1_review(run_id: str, approve: bool = True) -> Dict[str, Any]:
    from services.pipeline_runtime import load_checkpoint_state, save_checkpoint_state
    from sftp_nodes.governance import sftp_feed_discovery_node, sftp_gate1_node, sftp_gate2_node
    from sftp_nodes.source_ingestion import source_ingestion_node

    checkpoint_state = load_checkpoint_state(run_id) or {"run_id": run_id}
    checkpoint_state["gate1_decision"] = "APPROVED" if approve else "REJECTED"

    gate1_state = sftp_gate1_node(checkpoint_state)
    if gate1_state.get("status") == "FAILED":
        save_checkpoint_state(run_id, gate1_state)
        return gate1_state

    source_state = _run_visible_stage("discovery", source_ingestion_node, gate1_state)
    if source_state.get("status") == "FAILED":
        save_checkpoint_state(run_id, source_state)
        return source_state

    discovered_state = sftp_feed_discovery_node(source_state)
    gate2_state = sftp_gate2_node(discovered_state)
    save_checkpoint_state(run_id, gate2_state)
    return gate2_state


def submit_sftp_gate2_review(run_id: str, approve: bool = True) -> Dict[str, Any]:
    from services.pipeline_runtime import load_checkpoint_state, save_checkpoint_state
    from sftp_nodes.bronze_code_generation import sftp_bronze_code_generation_node
    from sftp_nodes.column_profiling import sftp_column_profiling_node
    from sftp_nodes.feed_nomination import sftp_feed_nomination_node
    from sftp_nodes.governance import sftp_gate2_node
    from sftp_nodes.metadata_discovery import file_metadata_discovery_node
    from sftp_nodes.review_gates import source_access_readiness_check_node, sftp_gate4_node
    from sftp_nodes.semantic_enrichment import sftp_gate3_node, sftp_semantic_enrichment_node

    checkpoint_state = load_checkpoint_state(run_id) or {"run_id": run_id}
    checkpoint_state["gate2_decision"] = "APPROVED" if approve else "REJECTED"

    nomination_state = sftp_feed_nomination_node(checkpoint_state)
    gate2_state = sftp_gate2_node(nomination_state)
    if gate2_state.get("status") == "FAILED":
        save_checkpoint_state(run_id, gate2_state)
        return gate2_state

    metadata_state = _run_visible_stage("schema", file_metadata_discovery_node, gate2_state)
    profiling_state = sftp_column_profiling_node(metadata_state)
    enriched_state = _run_visible_stage("enrichment", sftp_semantic_enrichment_node, profiling_state)
    gate3_state = sftp_gate3_node(enriched_state)
    if gate3_state.get("status") == "HITL_WAIT":
        save_checkpoint_state(run_id, gate3_state)
        return gate3_state
    if gate3_state.get("status") == "FAILED":
        save_checkpoint_state(run_id, gate3_state)
        return gate3_state

    readiness_state = source_access_readiness_check_node(gate3_state)
    bronze_code_state = sftp_bronze_code_generation_node(readiness_state)
    if bronze_code_state.get("bronze_generation_status") == "FAILED" or bronze_code_state.get("status") == "FAILED":
        save_checkpoint_state(run_id, bronze_code_state)
        return bronze_code_state

    gate4_state = sftp_gate4_node(bronze_code_state)
    save_checkpoint_state(run_id, gate4_state)
    return gate4_state


def submit_sftp_gate3_review(run_id: str, approve: bool = True, enriched_metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    from services.pipeline_runtime import load_checkpoint_state, save_checkpoint_state
    from sftp_nodes.bronze_code_generation import sftp_bronze_code_generation_node
    from sftp_nodes.review_gates import source_access_readiness_check_node, sftp_gate4_node
    from sftp_nodes.semantic_enrichment import sftp_gate3_node

    checkpoint_state = load_checkpoint_state(run_id) or {"run_id": run_id}
    checkpoint_state["gate3_decision"] = "APPROVED" if approve else "REJECTED"
    checkpoint_state["enrichment_review_decision"] = "APPROVED" if approve else "REJECTED"
    if enriched_metadata:
        checkpoint_state["enriched_metadata"] = enriched_metadata
    gate3_state = sftp_gate3_node(checkpoint_state)
    if gate3_state.get("status") == "HITL_WAIT":
        save_checkpoint_state(run_id, gate3_state)
        return gate3_state
    if gate3_state.get("status") == "FAILED":
        save_checkpoint_state(run_id, gate3_state)
        return gate3_state

    readiness_state = source_access_readiness_check_node(gate3_state)
    bronze_code_state = sftp_bronze_code_generation_node(readiness_state)
    if bronze_code_state.get("bronze_generation_status") == "FAILED" or bronze_code_state.get("status") == "FAILED":
        save_checkpoint_state(run_id, bronze_code_state)
        return bronze_code_state

    gate4_state = sftp_gate4_node({**bronze_code_state, "bronze_review_decision": None})
    if gate4_state.get("status") == "HITL_WAIT" or gate4_state.get("status") == "FAILED":
        save_checkpoint_state(run_id, gate4_state)
        return gate4_state

    # If a prior checkpoint already contains a decision, preserve it; otherwise
    # stop here and let the UI present Gate 4 for Bronze review.
    if str((gate4_state.get("gate4") or {}).get("decision") or "").upper() != "APPROVED":
        save_checkpoint_state(run_id, gate4_state)
        return gate4_state

    save_checkpoint_state(run_id, gate4_state)
    return submit_sftp_gate4_review(
        run_id,
        action="APPROVED",
        review_artifact=gate4_state.get("bronze_review_artifact"),
    )


def submit_sftp_gate4_review(run_id: str, action: str = "APPROVED", review_artifact: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    from services.pipeline_runtime import load_checkpoint_state, save_checkpoint_state
    from sftp_nodes.bronze_code_generation import sftp_bronze_code_generation_node
    from sftp_nodes.review_gates import bronze_validation_node, sftp_gate4_node
    from sftp_nodes.bronze_ingestion import sftp_bronze_ingestion_node
    from sftp_nodes.sftp_pull import sftp_pull_node
    from sftp_nodes.silver_code_generation import sftp_silver_code_generation_node
    from sftp_nodes.review_gates import sftp_gate5_node

    checkpoint_state = load_checkpoint_state(run_id) or {"run_id": run_id}
    checkpoint_state["bronze_review_decision"] = str(action or "APPROVED").upper()
    if review_artifact:
        checkpoint_state["bronze_review_artifact"] = review_artifact
    gate4_state = sftp_gate4_node(checkpoint_state)
    if gate4_state.get("status") == "REGENERATE_REQUIRED":
        regenerated = sftp_bronze_code_generation_node({**checkpoint_state, "bronze_review_decision": None})
        resumed = sftp_gate4_node(regenerated)
        save_checkpoint_state(run_id, resumed)
        return resumed
    if gate4_state.get("status") in {"FAILED", "HITL_WAIT"}:
        save_checkpoint_state(run_id, gate4_state)
        return gate4_state

    source_type = str(gate4_state.get("source") or "").lower()
    post_gate4 = gate4_state
    if source_type == "sftp":
        post_gate4 = sftp_pull_node(post_gate4)
        if post_gate4.get("status") == "FAILED":
            save_checkpoint_state(run_id, post_gate4)
            return post_gate4
        bronze_state = sftp_bronze_ingestion_node(post_gate4)
    else:
        bronze_state = {
            **post_gate4,
            "bronze_ingestion_status": "HANDOFF_ONLY",
            "bronze_handoff_status": "READY_FOR_DATABRICKS_REVIEWED_SCRIPT",
        }
    reviewed_state = _apply_reviewed_bronze_artifact_to_results(bronze_state)
    try:
        executed_state = _execute_reviewed_layer(
            run_id,
            reviewed_state,
            "bronze",
            reviewed_state.get("bronze_review_artifact"),
        )
    except Exception as exc:
        failed_state = {
            **reviewed_state,
            "status": "FAILED",
            "background_stage": "bronze_code_execution",
            "failed_background_stage": "bronze_code_execution",
            "error": str(exc),
        }
        save_checkpoint_state(run_id, failed_state)
        raise

    validated = bronze_validation_node(executed_state)
    if validated.get("status") == "FAILED":
        save_checkpoint_state(run_id, validated)
        return validated

    silver_state = sftp_silver_code_generation_node(validated)
    silver_status = str(silver_state.get("silver_generation_status") or "").upper()
    silver_items = ((silver_state.get("silver_review_artifact") or {}).get("items") or [])
    if silver_status not in {"COMPLETED", "PARTIAL"} or not silver_items:
        blocked_state = {
            **silver_state,
            "status": "FAILED",
            "error": silver_state.get("silver_generation_error") or "Silver generation did not produce a review artifact after Gate 4 approval.",
        }
        save_checkpoint_state(run_id, blocked_state)
        return blocked_state

    gate5_state = sftp_gate5_node(silver_state)
    save_checkpoint_state(run_id, gate5_state)
    return gate5_state


def submit_sftp_gate5_review(run_id: str, action: str = "APPROVED", review_artifact: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    from services.pipeline_runtime import load_checkpoint_state, save_checkpoint_state
    from sftp_nodes.gold_code_generation import sftp_gold_code_generation_node
    from sftp_nodes.review_gates import dq_validation_node, sftp_gate5_node
    from sftp_nodes.silver_code_generation import sftp_silver_code_generation_node

    checkpoint_state = load_checkpoint_state(run_id) or {"run_id": run_id}
    checkpoint_state["silver_review_decision"] = str(action or "APPROVED").upper()
    if review_artifact:
        checkpoint_state["silver_review_artifact"] = review_artifact
    gate5_state = sftp_gate5_node(checkpoint_state)
    if gate5_state.get("status") == "REGENERATE_REQUIRED":
        regenerated = sftp_silver_code_generation_node({**checkpoint_state, "silver_review_decision": None})
        resumed = sftp_gate5_node(regenerated)
        save_checkpoint_state(run_id, resumed)
        return resumed
    if gate5_state.get("status") in {"FAILED", "HITL_WAIT"}:
        save_checkpoint_state(run_id, gate5_state)
        return gate5_state

    try:
        executed_state = _execute_reviewed_layer(
            run_id,
            gate5_state,
            "silver",
            gate5_state.get("silver_review_artifact"),
        )
    except Exception as exc:
        failed_state = {
            **gate5_state,
            "status": "FAILED",
            "background_stage": "silver_code_execution",
            "failed_background_stage": "silver_code_execution",
            "error": str(exc),
        }
        save_checkpoint_state(run_id, failed_state)
        raise

    dq_state = dq_validation_node(executed_state)
    gold_state = sftp_gold_code_generation_node(dq_state)
    if str(gold_state.get("gold_generation_status") or "").upper() == "COMPLETED":
        gold_state.update(
            {
                "status": "HITL_WAIT",
                "background_stage": None,
                "next_gate": None,
                "next_review_key": "gold_review",
                "gold_review_artifact": {
                    "items": [
                        item
                        for item in gold_state.get("gold_generation_results") or []
                        if isinstance(item, dict)
                    ],
                },
                "resume_message": "Gold Review is pending. Review generated Gold scripts before execution.",
            }
        )
    save_checkpoint_state(run_id, gold_state)
    return gold_state
