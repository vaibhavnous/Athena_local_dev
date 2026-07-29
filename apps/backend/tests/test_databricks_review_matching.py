from __future__ import annotations

import pytest

from api.services.ui.review_ui_service import _map_silver_item
from services import databricks_runtime


def _silver_script(table: str) -> dict:
    return {
        "table": table,
        "source_table": f"ATHENA.BRONZE.bronze_{table}",
        "target_table": f"ATHENA.SILVER.silver_{table}",
        "script_path": f"generated/silver_{table}.py",
        "script_body": f"generated {table}",
    }


def test_legacy_silver_review_aliases_match_current_generated_identity():
    scripts = [_silver_script("claims"), _silver_script("policies")]
    review_artifact = {
        "items": [
            {
                "entity": "claims",
                "bronze_source": "ATHENA.BRONZE.bronze_claims",
                "silver_target": "ATHENA.SILVER.silver_claims",
                "review_status": "APPROVED",
                "generated_silver_script": "reviewed claims",
            },
            {
                "entity": "policies",
                "bronze_source": "ATHENA.BRONZE.bronze_policies",
                "silver_target": "ATHENA.SILVER.silver_policies",
                "review_status": "APPROVED",
                "generated_silver_script": "reviewed policies",
            },
        ]
    }

    filtered = databricks_runtime._filtered_scripts(scripts, review_artifact, "silver")

    assert [item["table"] for item in filtered] == ["claims", "policies"]
    assert [item["generated_silver_script"] for item in filtered] == [
        "reviewed claims",
        "reviewed policies",
    ]


def test_new_silver_review_mapper_preserves_canonical_identity_and_aliases():
    mapped = _map_silver_item(
        {
            "table": "claims",
            "entity": "Claims display",
            "source_table": "ATHENA.BRONZE.bronze_claims",
            "target_table": "ATHENA.SILVER.silver_claims",
            "script_path": "generated/silver_claims.py",
        }
    )

    assert mapped["table"] == "claims"
    assert mapped["source_table"] == "ATHENA.BRONZE.bronze_claims"
    assert mapped["target_table"] == "ATHENA.SILVER.silver_claims"
    assert mapped["script_path"] == "generated/silver_claims.py"
    assert mapped["entity"] == "Claims display"
    assert mapped["bronze_source"] == mapped["source_table"]
    assert mapped["silver_target"] == mapped["target_table"]


def test_silver_rejected_item_is_excluded_while_pending_legacy_item_remains():
    scripts = [_silver_script("claims"), _silver_script("policies")]
    review_artifact = {
        "items": [
            {"entity": "claims", "review_status": "REJECTED"},
            {
                "entity": "policies",
                "review_status": "PENDING",
                "generated_silver_script": "pending reviewed policies",
            },
        ]
    }

    filtered = databricks_runtime._filtered_scripts(scripts, review_artifact, "silver")

    assert [item["table"] for item in filtered] == ["policies"]
    assert filtered[0]["generated_silver_script"] == "pending reviewed policies"


def test_target_identity_wins_over_stale_review_script_path():
    scripts = [_silver_script("claims"), _silver_script("policies")]
    review_artifact = {
        "items": [
            {
                "silver_target": "ATHENA.SILVER.silver_claims",
                "script_path": "generated/silver_policies.py",
                "review_status": "APPROVED",
                "generated_silver_script": "reviewed claims",
            },
            {
                "silver_target": "ATHENA.SILVER.silver_policies",
                "script_path": "generated/silver_claims.py",
                "review_status": "APPROVED",
                "generated_silver_script": "reviewed policies",
            },
        ]
    }

    filtered = databricks_runtime._filtered_scripts(scripts, review_artifact, "silver")

    assert [item["generated_silver_script"] for item in filtered] == [
        "reviewed claims",
        "reviewed policies",
    ]


def test_shared_gold_source_uses_stronger_kpi_identity_and_source_only_fails_closed():
    scripts = [
        {
            "kpi_name": "Average Claim",
            "source_table": "ATHENA.SILVER.silver_claims",
            "target_table": "ATHENA.GOLD.fact_average_claim",
            "script_body": "generated average",
        },
        {
            "kpi_name": "Claim Count",
            "source_table": "ATHENA.SILVER.silver_claims",
            "target_table": "ATHENA.GOLD.fact_claim_count",
            "script_body": "generated count",
        },
    ]
    compatible = {
        "items": [
            {
                "kpi_name": "Average Claim",
                "bronze_source": "ATHENA.SILVER.silver_claims",
                "review_status": "APPROVED",
                "script_body": "reviewed average",
            },
            {
                "kpi_name": "Claim Count",
                "bronze_source": "ATHENA.SILVER.silver_claims",
                "review_status": "APPROVED",
                "script_body": "reviewed count",
            },
        ]
    }
    ambiguous = {
        "items": [
            {
                "bronze_source": "ATHENA.SILVER.silver_claims",
                "review_status": "APPROVED",
                "script_body": "must not cross-copy",
            }
        ]
    }

    filtered = databricks_runtime._filtered_scripts(scripts, compatible, "gold")

    assert [item["script_body"] for item in filtered] == ["reviewed average", "reviewed count"]
    assert databricks_runtime._filtered_scripts(scripts, ambiguous, "gold") == []


def test_zero_identity_matches_reports_generated_and_approved_counts(monkeypatch):
    monkeypatch.setattr(databricks_runtime, "databricks_execution_enabled", lambda _layer: True)
    state = {
        "run_id": "run-zero-match",
        "target_warehouse": "databricks",
        "silver_generation_results": [_silver_script("claims")],
    }

    with pytest.raises(
        ValueError,
        match=r"zero script identities matched .*generated_count=1, approved_count=1, matched_count=0",
    ):
        databricks_runtime.run_databricks_silver_scripts(
            state,
            review_artifact={
                "items": [
                    {
                        "entity": "missing",
                        "silver_target": "ATHENA.SILVER.silver_missing",
                        "review_status": "APPROVED",
                    }
                ]
            },
            approved_only=True,
        )
