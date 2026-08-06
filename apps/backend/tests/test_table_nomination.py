from nodes import table_nomination
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
    keywords = table_nomination._build_keywords([
        "Average Claim Payment Amount",
        "Monthly Policy Count by Customer",
        "Loss Ratio Percentage",
    ])

    assert {"claim", "payment", "policy", "customer", "loss"}.issubset(keywords)
    assert not {
        "average", "amount", "monthly", "count", "ratio", "percentage", "by",
    }.intersection(keywords)


def test_nomination_keywords_preserve_generic_only_lexical_fallback():
    assert table_nomination._build_keywords(["Monthly Average Count"]) == [
        "average",
        "count",
        "monthly",
    ]


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

    fused = table_nomination._fuse_results(lexical, semantic, ["insurance"])

    best = fused["insurance.dbo.claim_payment"]
    assert 0.0 < best["confidence_score"] < 1.0
    assert best["relevance_band"] == "HIGH"
    assert best["lexical_score"] == 0.9
    assert best["semantic_score"] == 0.85


def test_review_payload_is_deterministic_and_preserves_concrete_evidence():
    raw = {
        "database_name": "insurance",
        "schema_name": "dbo",
        "table_name": "claim_payment",
        "confidence_score": 0.82,
        "coverage_ratio": 0.7,
        "lexical_score": 0.9,
        "semantic_score": 1.2,
        "matched_keywords": ["payment", "claim", "claim"],
        "matched_columns": ["paid_amount", "claim_id", "paid_amount"],
        "nomination_reason": table_nomination.REASON_DUAL_MATCH,
    }
    kpis = ["Average Claim Payment Amount", "Active Policy Count"]

    evidence = table_nomination._prepare_review_evidence(raw, kpis)
    validated = NominationItem(**evidence)

    assert evidence == table_nomination._prepare_review_evidence(raw, kpis)
    assert validated.matched_keywords == ["Average Claim Payment Amount"]
    assert validated.matched_business_terms == ["claim", "payment"]
    assert validated.matched_columns == ["claim_id", "paid_amount"]
    assert validated.semantic_score == 1.0
    assert "Average Claim Payment Amount" in validated.nomination_reason
    assert "paid_amount" in validated.nomination_reason
    assert validated.nomination_method == table_nomination.REASON_DUAL_MATCH


def test_supporting_table_payload_keeps_legacy_method_and_new_display_reason():
    evidence = table_nomination._prepare_review_evidence(
        {
            "database_name": "insurance",
            "schema_name": "dbo",
            "table_name": "policy_type",
            "confidence_score": table_nomination.SCORE_FK_RESOLVED,
            "nomination_reason": table_nomination.REASON_FK_RESOLVED,
            "matched_keywords": [],
        },
        ["Active Policy Count"],
    )

    assert evidence["nomination_method"] == table_nomination.REASON_FK_RESOLVED
    assert evidence["nomination_reason"] == table_nomination.DISPLAY_REASON_FK_RESOLVED
    assert evidence["relevance_band"] == "HIGH"


def test_legacy_nomination_payload_remains_valid():
    legacy = NominationItem(
        database_name="insurance",
        schema_name="dbo",
        table_name="claims",
        confidence_score=0.9,
        coverage_ratio=0.5,
        matched_keywords=["claim"],
        nomination_reason=table_nomination.REASON_LEXICAL_ONLY,
    )

    assert legacy.matched_business_terms == []
    assert legacy.matched_columns == []
    assert legacy.lexical_score == 0.0
    assert legacy.semantic_score == 0.0
    assert legacy.relevance_band == "LOW"
    assert legacy.nomination_method == ""
