"""
Refined HITL Controller (Clean + UI-friendly)
"""

from datetime import datetime
from typing import Any, Dict


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


def submit_sftp_gate1_review(run_id: str, approve: bool = True) -> Dict[str, Any]:
    from services.pipeline_runtime import load_checkpoint_state, save_checkpoint_state
    from sftp_nodes.governance import sftp_feed_discovery_node, sftp_gate1_node, sftp_gate2_node

    checkpoint_state = load_checkpoint_state(run_id) or {"run_id": run_id}
    checkpoint_state["gate1_decision"] = "APPROVED" if approve else "REJECTED"

    gate1_state = sftp_gate1_node(checkpoint_state)
    if gate1_state.get("status") == "FAILED":
        save_checkpoint_state(run_id, gate1_state)
        return gate1_state

    discovered_state = sftp_feed_discovery_node(gate1_state)
    gate2_state = sftp_gate2_node(discovered_state)
    save_checkpoint_state(run_id, gate2_state)
    return gate2_state


def submit_sftp_gate2_review(run_id: str, approve: bool = True) -> Dict[str, Any]:
    from services.pipeline_runtime import load_checkpoint_state, save_checkpoint_state
    from sftp_nodes.column_profiling import sftp_column_profiling_node
    from sftp_nodes.governance import sftp_gate2_node
    from sftp_nodes.metadata_discovery import sftp_metadata_discovery_node
    from sftp_nodes.semantic_enrichment import sftp_gate3_node, sftp_semantic_enrichment_node

    checkpoint_state = load_checkpoint_state(run_id) or {"run_id": run_id}
    checkpoint_state["gate2_decision"] = "APPROVED" if approve else "REJECTED"
    gate2_state = sftp_gate2_node(checkpoint_state)
    if gate2_state.get("status") == "FAILED":
        save_checkpoint_state(run_id, gate2_state)
        return gate2_state

    metadata_state = sftp_metadata_discovery_node(gate2_state)
    profiling_state = sftp_column_profiling_node(metadata_state)
    enriched_state = sftp_semantic_enrichment_node(profiling_state)
    gate3_state = sftp_gate3_node(enriched_state)
    save_checkpoint_state(run_id, gate3_state)
    return gate3_state


def submit_sftp_gate3_review(run_id: str, approve: bool = True) -> Dict[str, Any]:
    from services.pipeline_runtime import load_checkpoint_state, save_checkpoint_state
    from sftp_nodes.semantic_enrichment import sftp_gate3_node

    checkpoint_state = load_checkpoint_state(run_id) or {"run_id": run_id}
    checkpoint_state["gate3_decision"] = "APPROVED" if approve else "REJECTED"
    checkpoint_state["enrichment_review_decision"] = "APPROVED" if approve else "REJECTED"
    result = sftp_gate3_node(checkpoint_state)
    save_checkpoint_state(run_id, result)
    return result
