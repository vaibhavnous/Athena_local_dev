from nodes import table_nomination


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
