from __future__ import annotations

from state import Stage01State


def sftp_gate1_node(state: Stage01State) -> Stage01State:
    decision = str(state.get("gate1_decision") or "").upper()
    if decision == "REJECTED":
        return {**state, "status": "FAILED", "gate1": {"decision": "REJECTED"}}
    if decision != "APPROVED":
        return {
            **state,
            "status": "HITL_WAIT",
            "next_gate": 1,
            "gate1": {"decision": None, "status": "PENDING"},
            "resume_message": "KPI Review is pending.",
        }
    return {**state, "status": "IN_PROGRESS", "next_gate": None, "gate1": {"decision": "APPROVED"}}


def sftp_feed_discovery_node(state: Stage01State) -> Stage01State:
    return state


def sftp_gate2_node(state: Stage01State) -> Stage01State:
    decision = str(state.get("gate2_decision") or "").upper()
    if decision == "REJECTED":
        return {**state, "status": "FAILED", "gate2": {"decision": "REJECTED"}}
    if decision != "APPROVED":
        return {
            **state,
            "status": "HITL_WAIT",
            "next_gate": 2,
            "gate2": {"decision": None, "status": "PENDING"},
            "resume_message": "Feed Review is pending.",
        }
    return {**state, "status": "IN_PROGRESS", "next_gate": None, "gate2": {"decision": "APPROVED"}}
