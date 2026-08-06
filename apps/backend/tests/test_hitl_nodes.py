from __future__ import annotations

from copy import deepcopy

import pytest

from nodes.hitl import (
    apply_gate3_review_rows,
    build_gate3_review_items,
    build_hitl_enrichment_review_node,
    hitl_review_node,
    hitl_table_review_node,
    validate_gate3_table_edit,
)


def test_gate1_pending_sets_hitl_wait():
    result = hitl_review_node({"run_id": "run-1", "human_decision": "PENDING"})

    assert result["status"] == "HITL_WAIT"
    assert result["human_decision"] == "PENDING"
    assert result["next_gate"] == 1
    assert result["next_review_key"] is None
    assert result["resume_message"] == "KPI Review is pending. Review the extracted KPIs before continuing."


def test_gate2_pending_sets_hitl_wait():
    result = hitl_table_review_node({"run_id": "run-1", "human_table_decision": "PENDING"})

    assert result["status"] == "HITL_WAIT"
    assert result["human_table_decision"] == "PENDING"
    assert result["next_gate"] == 2
    assert result["next_review_key"] is None


def test_gate3_pending_sets_hitl_wait():
    node = build_hitl_enrichment_review_node()
    result = node({"run_id": "run-1"})

    assert result["status"] == "HITL_WAIT"
    assert result["next_gate"] == 3
    assert result["next_review_key"] is None
    assert result["enrichment_review_status"] == "PENDING"


def test_gate3_rejection_is_terminal():
    node = build_hitl_enrichment_review_node()
    result = node({"run_id": "run-1", "enrichment_review_decision": "REJECTED"})

    assert result["status"] == "FAILED"
    assert result["next_gate"] is None
    assert result["enrichment_review_status"] == "FAILED"


def test_gate3_review_preserves_qualified_identity_and_noneditable_metadata():
    artifact = {
        "run_id": "run-1",
        "fingerprint": "fingerprint-1",
        "columns": [
            {
                "database_name": "sales",
                "schema_name": "claims",
                "table_name": "events",
                "column_name": "amount",
                "data_type": "decimal(18,2)",
                "suggested_display_name": "amount",
                "business_description": "Monetary amount recorded for the claim event",
                "semantic_type": "MEASURE",
                "is_measure": True,
                "is_dimension": False,
                "is_pii_candidate": False,
                "synonyms": ["claim value"],
                "confidence": 0.82,
            },
            {
                "database_name": "archive",
                "schema_name": "claims",
                "table_name": "events",
                "column_name": "event_code",
                "data_type": "varchar(20)",
                "suggested_display_name": "event_code",
                "business_description": "Business code identifying the archived claim event",
                "semantic_type": "DIMENSION",
                "is_measure": False,
                "is_dimension": True,
                "is_pii_candidate": False,
                "confidence": 0.78,
            },
        ],
    }

    items = build_gate3_review_items("run-1", artifact)
    assert len(items) == 2
    assert {item["content"]["qualified_table_name"] for item in items} == {
        "sales.claims.events",
        "archive.claims.events",
    }

    first = items[0]["content"]
    edited = deepcopy(first)
    edited["columns"][0]["suggested_display_name"] = "approved_claim_amount"
    merged = validate_gate3_table_edit(first, edited)
    assert merged["columns"][0]["data_type"] == "decimal(18,2)"
    assert merged["columns"][0]["synonyms"] == ["claim value"]

    with pytest.raises(ValueError, match="renamed"):
        renamed = deepcopy(edited)
        renamed["columns"][0]["column_name"] = "different_column"
        validate_gate3_table_edit(first, renamed)

    reviewed = apply_gate3_review_rows(artifact, [
        {"gate_status": "EDITED", "original_content": first, "edited_content": merged},
        {"gate_status": "APPROVED", "original_content": items[1]["content"], "edited_content": None},
    ])
    assert reviewed["columns"][0]["suggested_display_name"] == "approved_claim_amount"
    assert reviewed["columns"][0]["enrichment_source"] == "HUMAN_REVIEW"
    assert reviewed["columns"][0]["confidence"] == 1.0
    assert reviewed["columns"][1]["database_name"] == "archive"


def test_gate3_review_requires_pii_type_for_aadhaar_candidate():
    original = {
        "table_name": "claims",
        "columns": [{
            "column_name": "S_AADHAAR_ATTACHED",
            "suggested_display_name": "Aadhaar Attached",
            "business_description": "Indicates whether Aadhaar documentation is attached to the claim.",
            "semantic_type": "FLAG",
            "is_measure": False,
            "is_dimension": True,
            "is_pii_candidate": True,
            "pii_type": None,
        }],
    }

    with pytest.raises(ValueError, match="S_AADHAAR_ATTACHED: PII type is required"):
        validate_gate3_table_edit(original, original)
