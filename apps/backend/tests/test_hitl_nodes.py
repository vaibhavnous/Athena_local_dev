from __future__ import annotations

from nodes.hitl import (
    build_hitl_enrichment_review_node,
    hitl_review_node,
    hitl_table_review_node,
)


def test_gate1_pending_sets_hitl_wait():
    result = hitl_review_node({"run_id": "run-1", "human_decision": "PENDING"})

    assert result["status"] == "HITL_WAIT"
    assert result["human_decision"] == "PENDING"


def test_gate2_pending_sets_hitl_wait():
    result = hitl_table_review_node({"run_id": "run-1", "human_table_decision": "PENDING"})

    assert result["status"] == "HITL_WAIT"
    assert result["human_table_decision"] == "PENDING"
    assert result["next_gate"] == 2
    assert result["next_review_key"] is None
    assert "Table Review is pending" in result["resume_message"]


def test_gate3_pending_sets_hitl_wait():
    node = build_hitl_enrichment_review_node()
    result = node({"run_id": "run-1"})

    assert result["status"] == "HITL_WAIT"
    assert result["enrichment_review_status"] == "PENDING"
    assert result["enrichment_review_decision"] == "PENDING"
    assert result["next_gate"] == 3
    assert result["next_review_key"] is None
    assert "Semantic Review is pending" in result["resume_message"]


def test_gate3_rejected_sets_failed_even_without_review_flags():
    node = build_hitl_enrichment_review_node()
    result = node({"run_id": "run-1", "enrichment_review_decision": "REJECTED"})

    assert result["status"] == "FAILED"
    assert result["enrichment_review_status"] == "FAILED"
    assert result["next_gate"] is None
    assert result["next_review_key"] is None
