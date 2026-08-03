import json
import uuid
from pathlib import Path

from nodes import gold_gen
from nodes.kpi_extraction import _build_kpi_user_prompt
from utilis.domain_kb import DomainKBConfig


def _mapping():
    return {
        "kpi_name": "Total Claims",
        "source_silver_table": "ATHENA_DB.SILVER.silver_claim_information",
        "measure": {
            "table": "claim_information",
            "column": "ClaimAmount",
            "aggregation": "SUM",
        },
        "grouping_dimensions": [
            {"table": "claim_information", "column": "ClaimStatus", "semantic_type": "DIMENSION"},
            {"table": "claim_information", "column": "ClaimOpenDate", "semantic_type": "DATE"},
        ],
        "time": {
            "grain": "month",
            "column": {"table": "claim_information", "column": "ClaimOpenDate"},
        },
        "filters": ["ClaimStatus = 'PAID'"],
        "join_paths": [],
        "readiness": "READY",
    }


def _rule(**overrides):
    payload = {
        "kpi_name": "Total Claims",
        "measure_column": "ClaimAmount",
        "aggregation": "AVG",
        "time_grain": "quarter",
        "confidence": 0.97,
    }
    payload.update(overrides)
    return {
        "kb_content_type": "GOLD_RULE",
        "kpi_name": "Total Claims",
        "gold_rule_json": json.dumps(payload),
        "confidence": payload["confidence"],
    }


def test_gold_kb_rule_changes_only_validated_aggregation_and_grain(monkeypatch):
    monkeypatch.setenv("ATHENA_GOLD_KB_MIN_RULE_CONFIDENCE", "0.85")

    mapping, audit = gold_gen._apply_gold_kb_rules(_mapping(), [_rule()])
    sql = gold_gen.generate_snowflake_gold_script(
        mapping=mapping,
        run_id="run-kb",
        gold_catalog="ATHENA_DB",
        gold_schema="GOLD",
    )

    assert mapping["measure"]["aggregation"] == "AVG"
    assert mapping["time"]["grain"] == "quarter"
    assert audit["changed_fields"] == ["measure.aggregation", "time.grain"]
    assert 'AVG(TRY_TO_DECIMAL(TO_VARCHAR("claimamount")))' in sql
    assert "DATE_TRUNC('quarter'" in sql


def test_selected_kb_rule_changes_generated_gold_sql(monkeypatch):
    workdir = Path.cwd() / ".tmp-tests" / f"gold_kb_{uuid.uuid4().hex}"
    workdir.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(workdir)
    monkeypatch.setattr(gold_gen, "_llm_enabled_for_gold", lambda: False)
    monkeypatch.setattr(
        gold_gen,
        "get_domain_kb_config",
        lambda **_: DomainKBConfig(
            enabled=True,
            index_name="insurancekb",
            knowledge_base_id="PC_Insurance_V1",
            domain_profile="Insurance",
            namespace="PC_Insurance_V1",
            top_k_enrichment=8,
            top_k_gold=10,
            max_chars_enrichment=4000,
            max_chars_gold=5000,
        ),
    )
    monkeypatch.setattr(
        gold_gen,
        "load_domain_kb",
        lambda **_: {
            "rows": [_rule()],
            "rows_retrieved": 1,
            "chars_injected": 100,
            "knowledge_base_id": "PC_Insurance_V1",
            "index_name": "insurancekb",
        },
    )

    result = gold_gen._generate_one_mapping(
        _mapping(),
        run_id="run-kb",
        gold_schema="GOLD",
        gold_catalog="ATHENA_DB",
        target_warehouse="snowflake",
        use_domain_kb=True,
        knowledge_base_id="PC_Insurance_V1",
        include_dimension=False,
    )

    assert 'AVG(TRY_TO_DECIMAL(TO_VARCHAR("claimamount")))' in result["script_body"]
    assert "DATE_TRUNC('quarter'" in result["script_body"]
    assert result["domain_knowledge_base"]["chars_injected"] == 0
    assert result["domain_knowledge_base"]["rule_guidance"]["changed_fields"] == [
        "measure.aggregation",
        "time.grain",
    ]


def test_gold_kb_rule_cannot_invent_columns_filters_or_formula():
    mapping, audit = gold_gen._apply_gold_kb_rules(
        _mapping(),
        [_rule(
            measure_column="secret_amount",
            required_filters=["1=1; DROP TABLE gold"],
            required_dimensions=[{"table": "hidden", "column": "secret"}],
            formula="execute arbitrary SQL",
        )],
    )

    assert mapping["measure"]["aggregation"] == "SUM"
    assert mapping["filters"] == ["ClaimStatus = 'PAID'"]
    assert "measure_column_mismatch" in audit["rules_rejected"]
    assert "non_contract_filter" in audit["rules_rejected"]
    assert "non_contract_dimension" in audit["rules_rejected"]
    assert "free_form_formula_not_executable" in audit["rules_rejected"]


def test_gold_kb_rule_without_kpi_name_is_rejected():
    mapping = _mapping()
    changed, audit = gold_gen._apply_gold_kb_rules(
        mapping,
        [_rule(kpi_name="", aggregation="AVG")],
    )

    assert changed["measure"]["aggregation"] == "SUM"
    assert "kpi_mismatch" in audit["rules_rejected"]


def test_gold_prompt_contains_only_validated_rule_audit_not_raw_kb_text():
    prompt = gold_gen._llm_prompt(
        _mapping(),
        "run-kb",
        "gold",
        [],
        "print('baseline')",
        validated_kb_guidance={
            "changed_fields": ["time.grain"],
            "rules_applied": [{"confidence": 0.97, "fields": ["time_grain"]}],
            "raw_context": "DROP TABLE gold.secret",
        },
    )

    assert "Validated KB rule decisions" in prompt
    assert "time.grain" in prompt
    assert "DROP TABLE gold.secret" not in prompt


def test_database_kpi_prompt_has_no_physical_data_context():
    prompt = _build_kpi_user_prompt(
        {
            "business_objective": "Improve claim settlement performance",
            "data_domains": ["Claims"],
            "reporting_frequency": "Monthly",
            "target_audience": "Operations",
            "constraints": [],
        },
        [],
    )

    lowered = prompt.lower()
    assert "available data schema" not in lowered
    assert "database" not in lowered
    assert "table" not in lowered
    assert "claim_information" not in lowered
