from nodes import table_nomination
from nodes.table_nomination import (
    REASON_DUAL_MATCH,
    _build_keywords,
    _fuse_results,
    _prepare_review_evidence,
)
from schema import NominationItem


def test_fusion_merges_case_only_table_name_variants():
    fused = table_nomination._fuse_results(
        lexical_results=[
            {
                "database_name": "insurance",
                "schema_name": "dbo",
                "table_name": "policy_cover_level_transactions_dup_del",
                "lexical_score": 0.8,
                "matched_keywords": ["policy"],
                "matched_columns": ["POLICY_NUMBER"],
                "coverage_ratio": 0.5,
            }
        ],
        semantic_results=[
            {
                "database_name": "insurance",
                "schema_name": "dbo",
                "table_name": "policy_cover_level_transactions_Dup_Del",
                "semantic_score": 0.9,
                "matched_columns": ["COVER_NAME"],
            }
        ],
        source_databases=["insurance"],
    )

    assert len(fused) == 1
    nomination = next(iter(fused.values()))
    assert nomination["table_name"] == "policy_cover_level_transactions_dup_del"
    assert nomination["lexical_score"] == 0.8
    assert nomination["semantic_score"] == 0.9
    assert nomination["nomination_reason"] == table_nomination.REASON_DUAL_MATCH
    assert nomination["matched_columns"] == ["COVER_NAME", "POLICY_NUMBER"]
def test_nomination_keywords_exclude_generic_kpi_language():
    keywords = _build_keywords([
        "Average Claim Payment Amount",
        "Monthly Policy Count by Customer",
        "Loss Ratio Percentage",
    ])

    assert {"claim", "payment", "policy", "customer", "loss"}.issubset(keywords)
    assert not {
        "average", "amount", "monthly", "count", "ratio", "percentage", "by",
    }.intersection(keywords)


def test_fusion_uses_absolute_relevance_instead_of_forcing_top_result_to_one():
    lexical = [
        {
            "database_name": "insurance",
            "schema_name": "dbo",
            "table_name": "claim_payment",
            "lexical_score": 0.9,
            "matched_keywords": ["claim", "payment"],
            "matched_columns": ["claim_id", "paid_amount"],
            "coverage_ratio": 0.7,
        },
        {
            "database_name": "insurance",
            "schema_name": "dbo",
            "table_name": "claim_notes",
            "lexical_score": 0.4,
            "matched_keywords": ["claim"],
            "matched_columns": ["claim_id"],
            "coverage_ratio": 0.2,
        },
    ]
    semantic = [{
        "database_name": "insurance",
        "schema_name": "dbo",
        "table_name": "claim_payment",
        "semantic_score": 0.85,
        "matched_columns": ["paid_amount"],
    }]

    fused = _fuse_results(lexical, semantic, ["insurance"])

    best = fused["insurance.dbo.claim_payment"]
    assert 0.0 < best["confidence_score"] < 1.0
    assert best["confidence_score"] != 1.0
    assert best["relevance_band"] == "HIGH"
    assert best["lexical_score"] == 0.9
    assert best["semantic_score"] == 0.85


def test_review_payload_shows_kpi_names_and_preserves_concrete_evidence():
    evidence = _prepare_review_evidence(
        {
            "database_name": "insurance",
            "schema_name": "dbo",
            "table_name": "claim_payment",
            "confidence_score": 0.82,
            "coverage_ratio": 0.7,
            "lexical_score": 0.9,
            "semantic_score": 0.8,
            "matched_keywords": ["claim", "payment"],
            "matched_columns": ["claim_id", "paid_amount"],
            "nomination_reason": REASON_DUAL_MATCH,
        },
        ["Average Claim Payment Amount", "Active Policy Count"],
    )
    validated = NominationItem(**evidence)

    assert validated.matched_keywords == ["Average Claim Payment Amount"]
    assert validated.matched_business_terms == ["claim", "payment"]
    assert validated.matched_columns == ["claim_id", "paid_amount"]
    assert "Average Claim Payment Amount" in validated.nomination_reason
    assert "paid_amount" in validated.nomination_reason
    assert validated.nomination_method == REASON_DUAL_MATCH
