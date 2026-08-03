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


def test_rules_distinguish_sensitive_values_from_presence_flags():
    for column in (
        semantic._fallback_enrichment(_column("S_AADHAAR_ATTACHED", "bit"), source="RULES"),
        semantic._fallback_enrichment(
            _column("IS_AADHAAR_ATTACHED", "varchar", profile_tier="FLAG"),
            source="RULES",
        ),
    ):
        assert column["semantic_type"] == "FLAG"
        assert column["is_pii_candidate"] is False
        assert column["pii_type"] is None
        assert column["needs_review"] is False

    identifier = semantic._fallback_enrichment(_column("AADHAAR_NUMBER"), source="RULES")
    assert identifier["is_pii_candidate"] is True
    assert identifier["needs_review"] is True


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
        {
            "business_objective": "Analyze claims",
            "data_domains": ["insurance"],
            "domain_knowledge_context": "Claims use settlement-specific insurance terminology.",
        },
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
    assert "Claims use settlement-specific insurance terminology." in prompt


def test_node_audits_kb_context_only_when_sent_to_llm(monkeypatch):
    llm = FakeLLM([{"enriched_columns": [_llm_column("ClaimAmount", "MEASURE")]}])
    kb_config = SimpleNamespace(
        enabled=True,
        knowledge_base_id="PC_Insurance_V1",
        domain_profile="Insurance",
        index_name="insurancekb",
        top_k_enrichment=8,
        max_chars_enrichment=5000,
    )
    monkeypatch.setattr(semantic, "get_domain_kb_config", lambda **_: kb_config)
    monkeypatch.setattr(
        semantic,
        "load_domain_kb",
        lambda **_: {
            "context_text": "Insurance KB measure guidance",
            "rows_retrieved": 2,
            "chars_injected": 29,
            "knowledge_base_id": kb_config.knowledge_base_id,
            "index_name": kb_config.index_name,
            "content_types": ["MEASURE_PATTERN"],
        },
    )
    monkeypatch.setattr(semantic, "get_llm", lambda **_: llm)
    monkeypatch.setattr(semantic, "ai_store_db_writer", lambda **_: None)

    result = semantic.semantic_enrichment_node({
        "run_id": "run-kb",
        "fingerprint": "fp-kb",
        "use_domain_kb": True,
        "knowledge_base_id": kb_config.knowledge_base_id,
        "domain_profile": kb_config.domain_profile,
        "discovered_metadata": {
            "primary_keys": [],
            "foreign_keys": [],
            "table_relationships": [],
            "tables": [{
                "database_name": "insurance",
                "schema_name": "dbo",
                "table_name": "claims",
                "columns": [{"column_name": "ClaimAmount", "data_type": "decimal"}],
            }],
        },
        "column_profiles": {"column_profiles": []},
    })

    audit = result["enriched_metadata"]["domain_knowledge_base"]
    assert "Insurance KB measure guidance" in llm.calls[0]["messages"][1].content
    assert audit["rows_retrieved"] == 2
    assert audit["chars_retrieved"] == len("Insurance KB measure guidance")
    assert audit["chars_injected"] == len("Insurance KB measure guidance")


def test_kb_audit_does_not_claim_injection_when_semantic_llm_is_disabled(monkeypatch):
    kb_config = SimpleNamespace(
        enabled=True,
        knowledge_base_id="PC_Insurance_V1",
        domain_profile="Insurance",
        index_name="insurancekb",
        top_k_enrichment=8,
        max_chars_enrichment=5000,
    )
    monkeypatch.setenv("SEMANTIC_LLM_ENABLED", "false")
    monkeypatch.setattr(semantic, "get_domain_kb_config", lambda **_: kb_config)
    monkeypatch.setattr(
        semantic,
        "load_domain_kb",
        lambda **_: {
            "context_text": "Retrieved but unused KB context",
            "rows_retrieved": 1,
            "knowledge_base_id": kb_config.knowledge_base_id,
            "index_name": kb_config.index_name,
        },
    )
    monkeypatch.setattr(semantic, "ai_store_db_writer", lambda **_: None)

    result = semantic.semantic_enrichment_node({
        "run_id": "run-kb-disabled-llm",
        "fingerprint": "fp-kb-disabled-llm",
        "use_domain_kb": True,
        "discovered_metadata": {
            "primary_keys": [],
            "foreign_keys": [],
            "table_relationships": [],
            "tables": [{
                "database_name": "insurance",
                "schema_name": "dbo",
                "table_name": "claims",
                "columns": [{"column_name": "ClaimAmount", "data_type": "decimal"}],
            }],
        },
        "column_profiles": {"column_profiles": []},
    })

    audit = result["enriched_metadata"]["domain_knowledge_base"]
    assert audit["chars_retrieved"] == len("Retrieved but unused KB context")
    assert audit["chars_injected"] == 0


def test_llm_retries_display_name_collision_with_rule_enrichment(monkeypatch):
    duplicate = _llm_column(
        "CHANNEL_GROUP_NAME",
        "DIMENSION",
        table_name="policy_transactions",
        suggested_display_name="Channel Group ID",
    )
    corrected = {**duplicate, "suggested_display_name": "Channel Group Name"}
    llm = FakeLLM([
        {"enriched_columns": [duplicate]},
        {"enriched_columns": [corrected]},
    ])
    monkeypatch.setattr(semantic, "get_llm", lambda **_: llm)
    monkeypatch.setattr(semantic, "ai_store_db_writer", lambda **_: None)

    result = semantic.semantic_enrichment_node({
        "run_id": "run-distinct-display-names",
        "fingerprint": "fp-distinct-display-names",
        "discovered_metadata": {
            "primary_keys": [],
            "foreign_keys": [],
            "table_relationships": [],
            "tables": [{
                "database_name": "insurance",
                "schema_name": "dbo",
                "table_name": "policy_transactions",
                "columns": [
                    {"column_name": "CHANNEL_GROUP_ID", "data_type": "int"},
                    {"column_name": "CHANNEL_GROUP_NAME", "data_type": "varchar"},
                ],
            }],
        },
        "column_profiles": {"column_profiles": [
            {**_column("CHANNEL_GROUP_ID", "int", table_name="policy_transactions"), "profile_tier": "ID"},
            {**_column("CHANNEL_GROUP_NAME", table_name="policy_transactions"), "profile_tier": "DIMENSION"},
        ]},
    })

    columns = {column["column_name"]: column for column in result["enriched_metadata"]["columns"]}
    assert len(llm.calls) == 2
    assert columns["CHANNEL_GROUP_ID"]["suggested_display_name"] == "Channel Group ID"
    assert columns["CHANNEL_GROUP_NAME"]["suggested_display_name"] == "Channel Group Name"


def test_final_display_name_guard_repairs_cross_source_collision():
    columns = [
        {**_column("CHANNEL_GROUP_ID", "int", table_name="policy_transactions"), "suggested_display_name": "Channel Group ID"},
        {**_column("CHANNEL_GROUP_NAME", table_name="policy_transactions"), "suggested_display_name": "Channel Group ID", "confidence": 0.95},
    ]

    semantic._ensure_unique_display_names(columns)

    assert columns[0]["suggested_display_name"] == "Channel Group ID"
    assert columns[1]["suggested_display_name"] == "Channel Group Name"
    assert columns[1]["display_name_source"] == "COLUMN_NAME_FALLBACK"
    assert columns[1]["needs_review"] is True


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
