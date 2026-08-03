from __future__ import annotations

import json
from types import SimpleNamespace

import nodes.semantic_enrichment as semantic


class FakeLLM:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def invoke(self, messages, config=None):
        self.calls.append({"messages": messages, "config": config})
        if not self.responses:
            raise AssertionError("FakeLLM exhausted")
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return SimpleNamespace(content=json.dumps(response))


def _llm_column(column_name: str, semantic_type: str, **overrides):
    return {
        "database_name": "insurance",
        "schema_name": "dbo",
        "table_name": "claims",
        "column_name": column_name,
        "business_description": f"Business meaning for {column_name} in insurance claim reporting.",
        "semantic_type": semantic_type,
        "suggested_display_name": semantic._business_display_name(column_name),
        "is_pii_candidate": False,
        "pii_type": None,
        "synonyms": [],
        "confidence": 0.95,
        **overrides,
    }


def _column(column_name: str, data_type: str = "varchar", **overrides):
    return {
        "database_name": "insurance",
        "schema_name": "dbo",
        "table_name": "claims",
        "column_name": column_name,
        "data_type": data_type,
        **overrides,
    }


def test_rules_recognize_camel_case_ids_and_keep_pii_orthogonal():
    identifier = semantic.rule_based_semantic_classification(_column("ClaimID", "int"))
    company = semantic.rule_based_semantic_classification(_column("CompanyName"))
    email = semantic.rule_based_semantic_classification(_column("CustomerEmail"))

    assert identifier["semantic_type"] == "ID"
    assert identifier["is_join_key"] is True
    assert identifier["needs_llm"] is False
    assert company["is_pii_candidate"] is False
    assert email["is_pii_candidate"] is True


def test_business_display_names_humanize_technical_identifiers():
    assert semantic._business_display_name("Claim_ID") == "Claim ID"
    assert semantic._business_display_name("claimAmount") == "Claim Amount"
    assert semantic._business_display_name("CUSTOMER_PII_FLAG") == "Customer PII Flag"


def test_rules_flag_incomplete_aadhaar_pii_for_human_review():
    column = semantic._fallback_enrichment(_column("S_AADHAAR_ATTACHED", "bit"), source="RULES")

    assert column["semantic_type"] == "FLAG"
    assert column["is_pii_candidate"] is True
    assert column["pii_type"] is None
    assert column["needs_review"] is True


def test_llm_batch_retries_incomplete_output_and_never_sends_raw_samples():
    columns = [
        _column("ClaimAmount", "decimal", top_samples=[{"value": "secret@example.com", "count": 2}]),
        _column("ClaimStatus", "int", cardinality=4, total_rows=1000),
    ]
    incomplete = {"enriched_columns": [_llm_column("ClaimAmount", "MEASURE")]}
    complete = {
        "enriched_columns": [
            _llm_column("ClaimAmount", "MEASURE"),
            _llm_column("ClaimStatus", "DIMENSION"),
        ]
    }
    llm = FakeLLM([incomplete, complete])

    results = semantic._enrich_batch(
        columns,
        {"business_objective": "Analyze claims", "data_domains": ["insurance"]},
        llm,
        semantic.TokenAccumulator(),
        max_retries=1,
        batch_label="test",
    )

    assert [result["column_name"] for result in results] == ["ClaimAmount", "ClaimStatus"]
    assert len(llm.calls) == 2
    prompt = llm.calls[0]["messages"][1].content
    assert "secret@example.com" not in prompt
    assert "pattern=A{6}@A{7}.A{3}" in prompt


def test_node_uses_real_llm_for_ambiguous_columns_and_preserves_certified_keys(monkeypatch):
    llm = FakeLLM(
        [
            {
                "enriched_columns": [
                    _llm_column("ClaimAmount", "MEASURE", synonyms=["paid amount"]),
                    _llm_column("ClaimStatus", "DIMENSION", synonyms=["claim state"]),
                ]
            }
        ]
    )
    persisted = {}
    monkeypatch.setattr(semantic, "get_llm", lambda **_: llm)
    monkeypatch.setattr(semantic, "ai_store_db_writer", lambda **kwargs: persisted.update(kwargs))

    state = {
        "run_id": "run-1",
        "fingerprint": "fp-1",
        "req_business_objective": "Track claim performance",
        "req_data_domains": ["insurance"],
        "discovered_metadata": {
            "primary_keys": [
                {
                    "database_name": "insurance",
                    "schema_name": "dbo",
                    "table_name": "claims",
                    "column_name": "ClaimID",
                    "constraint_name": "PK_claims",
                }
            ],
            "foreign_keys": [],
            "table_relationships": [],
            "tables": [
                {
                    "database_name": "insurance",
                    "schema_name": "dbo",
                    "table_name": "claims",
                    "columns": [
                        {"column_name": "ClaimID", "data_type": "int"},
                        {"column_name": "ClaimAmount", "data_type": "decimal"},
                        {"column_name": "ClaimStatus", "data_type": "int"},
                    ],
                }
            ],
        },
        "column_profiles": {
            "column_profiles": [
                {**_column("ClaimID", "int"), "profile_tier": "MEASURE", "cardinality": 1000, "total_rows": 1000},
                {**_column("ClaimAmount", "decimal"), "profile_tier": "MEASURE", "cardinality": 900, "total_rows": 1000},
                {**_column("ClaimStatus", "int"), "profile_tier": "MEASURE", "cardinality": 4, "total_rows": 1000},
            ]
        },
        "certified_tables": [{"database_name": "insurance", "schema_name": "dbo", "table_name": "claims"}],
    }

    result = semantic.semantic_enrichment_node(state)
    columns = {column["column_name"]: column for column in result["enriched_metadata"]["columns"]}

    assert columns["ClaimID"]["semantic_type"] == "ID"
    assert columns["ClaimID"]["suggested_display_name"] == "Claim ID"
    assert columns["ClaimID"]["is_join_key"] is True
    assert columns["ClaimID"]["enrichment_source"] == "RULES"
    assert columns["ClaimAmount"]["semantic_type"] == "MEASURE"
    assert columns["ClaimAmount"]["is_measure"] is True
    assert columns["ClaimStatus"]["semantic_type"] == "DIMENSION"
    assert columns["ClaimStatus"]["is_dimension"] is True
    assert result["enriched_metadata"]["enrichment_source_counts"] == {"RULES": 1, "LLM": 2}
    assert persisted["schema_version"] == "SemanticEnrichment_v2"
    assert persisted["prompt_version"] == semantic.SEMANTIC_PROMPT_VERSION


def test_node_falls_back_without_dropping_columns_when_llm_fails(monkeypatch):
    monkeypatch.setattr(semantic, "get_llm", lambda **_: FakeLLM([RuntimeError("offline")] * 3))
    monkeypatch.setattr(semantic, "ai_store_db_writer", lambda **_: None)
    state = {
        "run_id": "run-2",
        "fingerprint": "fp-2",
        "discovered_metadata": {
            "primary_keys": [],
            "foreign_keys": [],
            "table_relationships": [],
            "tables": [
                {
                    "database_name": "insurance",
                    "schema_name": "dbo",
                    "table_name": "claims",
                    "columns": [{"column_name": "ClaimAmount", "data_type": "decimal"}],
                }
            ],
        },
        "column_profiles": {"column_profiles": []},
    }

    result = semantic.semantic_enrichment_node(state)
    column = result["enriched_metadata"]["columns"][0]

    assert column["semantic_type"] == "MEASURE"
    assert column["enrichment_source"] == "RULES_FALLBACK"
    assert column["needs_review"] is True
    assert result["semantic_enrichment_status"] == "COMPLETED"


def test_profiles_are_matched_by_qualified_identity(monkeypatch):
    monkeypatch.setenv("SEMANTIC_LLM_ENABLED", "false")
    monkeypatch.setattr(semantic, "ai_store_db_writer", lambda **_: None)
    tables = [
        {
            "database_name": "insurance",
            "schema_name": schema_name,
            "table_name": "claims",
            "columns": [{"column_name": "StatusCode", "data_type": "varchar"}],
        }
        for schema_name in ("current", "archive")
    ]
    profiles = [
        {
            "database_name": "insurance",
            "schema_name": "current",
            "table_name": "claims",
            "column_name": "StatusCode",
            "data_type": "varchar",
            "profile_tier": "DIMENSION",
            "cardinality": 4,
            "total_rows": 1000,
        },
        {
            "database_name": "insurance",
            "schema_name": "archive",
            "table_name": "claims",
            "column_name": "StatusCode",
            "data_type": "varchar",
            "profile_tier": "DIMENSION",
            "cardinality": 1000,
            "total_rows": 1000,
        },
    ]
    result = semantic.semantic_enrichment_node(
        {
            "run_id": "run-qualified",
            "fingerprint": "fp-qualified",
            "discovered_metadata": {
                "primary_keys": [],
                "foreign_keys": [],
                "table_relationships": [],
                "tables": tables,
            },
            "column_profiles": {"column_profiles": profiles},
        }
    )

    by_schema = {
        column["schema_name"]: column
        for column in result["enriched_metadata"]["columns"]
    }
    assert by_schema["current"]["semantic_type"] == "DIMENSION"
    assert by_schema["archive"]["semantic_type"] == "HIGH_CARD_TEXT"
