from services.sftp_stage_registry import SFTP_STAGE_KEYS, SFTP_STAGE_SPECS, phase_templates, stage_spec
from services.sftp_stage_execution import execute_sftp_stage
from sftp_nodes import design_governance
from sftp_nodes import memory_check


def test_canonical_sftp_stage_registry_matches_approved_thirty_stage_flow():
    assert len(SFTP_STAGE_SPECS) == 30
    assert len(set(SFTP_STAGE_KEYS)) == 30
    assert SFTP_STAGE_KEYS == (
        "brd_ingestion",
        "memory_check",
        "requirements",
        "kpi_extraction",
        "gate1",
        "feed_discovery",
        "feed_nomination",
        "gate2",
        "metadata_discovery",
        "column_profiling",
        "semantic_enrichment",
        "gate3",
        "metadata_bootstrap",
        "plan_seal",
        "freshness_check",
        "metadata_codegen",
        "gate4_metadata",
        "dab_bundle",
        "runtime_config",
        "validate_source",
        "discover_source_objects",
        "stage_to_landing",
        "bronze_autoloader",
        "bronze_dq",
        "bronze_to_silver",
        "silver_dq",
        "silver_to_gold",
        "gold_dq",
        "gate5_publish",
        "finalize",
    )


def test_every_stage_has_one_status_handoff_and_phase():
    assert all(stage.status_field and stage.phase_id and stage.phase_label for stage in SFTP_STAGE_SPECS)
    assert [phase["id"] for phase in phase_templates()] == [
        "phase-1",
        "phase-2",
        "phase-3",
        "phase-4",
        "phase-5",
        "phase-6",
    ]
    assert sum(len(phase["keys"]) for phase in phase_templates()) == 30
    assert stage_spec("gate4_metadata").artifact_type == "SFTP_GATE4_METADATA_DECISION"


def test_sftp_memory_check_writes_real_context_artifact(monkeypatch):
    written = {}
    monkeypatch.setattr(
        memory_check,
        "memory_lookup_node",
        lambda state: {
            **state,
            "memory_layer1": True,
            "memory_layer2": False,
            "context_kpis": [{"name": "Claim Count"}],
        },
    )
    monkeypatch.setattr(
        memory_check,
        "_prior_file_context",
        lambda state: {
            "feeds": [{"feed_id": "insurance_claims"}],
            "schemas": [{"feed_id": "insurance_claims", "version": 2}],
            "artifacts": [{"artifact_type": "SFTP_SCHEMA_SNAPSHOT"}],
        },
    )
    monkeypatch.setattr(memory_check, "ai_store_db_writer", lambda **kwargs: written.update(kwargs))

    result = memory_check.sftp_memory_check_node({
        "run_id": "run-memory",
        "source": "adls_gen2",
        "fingerprint": "fp-memory",
    })

    assert result["memory_check_status"] == "COMPLETED"
    assert result["sftp_memory_context"]["feeds"][0]["feed_id"] == "insurance_claims"
    assert written["artifact_type"] == "SFTP_MEMORY_CONTEXT"
    assert written["payload"]["fingerprint"] == "fp-memory"


def _approved_design_state():
    return {
        "run_id": "run-design",
        "fingerprint": "fp-design",
        "source": "adls_gen2",
        "connection_id": "insurance-adls",
        "target_warehouse": "databricks",
        "candidate_feeds": [
            {
                "feed_id": "insurance_claims",
                "entity": "claims",
                "format": "csv",
                "status": "APPROVED",
                "approved_schema": [
                    {"name": "claim_id", "data_type": "string", "is_primary_key": True},
                    {"name": "amount", "data_type": "decimal"},
                ],
            },
        ],
        "enrichment_review_artifact": {
            "columns": [
                {"feed_id": "insurance_claims", "column_name": "claim_id", "semantic_type": "IDENTIFIER"},
                {"feed_id": "insurance_claims", "column_name": "amount", "semantic_type": "MEASURE"},
            ],
        },
        "certified_kpis": [
            {
                "kpi_name": "Total Claim Amount",
                "definition": "Sum of claim amount",
                "measure": "amount",
                "aggregation": "sum",
                "grain": "claim",
            },
        ],
    }


def test_design_governance_nodes_seal_validate_and_generate_real_metadata(monkeypatch):
    artifacts = []
    monkeypatch.setattr(
        design_governance,
        "ai_store_db_writer",
        lambda **kwargs: artifacts.append(kwargs["artifact_type"]),
    )

    bootstrapped = design_governance.sftp_metadata_bootstrap_node(_approved_design_state())
    sealed = design_governance.sftp_plan_seal_node(bootstrapped)
    fresh = design_governance.sftp_freshness_check_node(sealed)
    generated = design_governance.sftp_metadata_codegen_node(fresh)

    assert generated["metadata_bootstrap_status"] == "COMPLETED"
    assert generated["plan_seal_status"] == "COMPLETED"
    assert generated["freshness_check_status"] == "COMPLETED"
    assert generated["metadata_codegen_status"] == "COMPLETED"
    assert generated["metadata_codegen_artifact"]["source_mapping"][0]["columns"][0]["source_column"] == "claim_id"
    assert generated["metadata_codegen_artifact"]["target_table_rule"][0]["merge_keys"] == ["claim_id"]
    assert generated["metadata_codegen_artifact"]["gold_model_config"][0]["aggregation"] == "sum"
    assert artifacts == [
        "SFTP_METADATA_BOOTSTRAP",
        "SFTP_SEALED_PLAN",
        "SFTP_FRESHNESS_MANIFEST",
        "SFTP_METADATA_CODEGEN",
    ]


def test_freshness_check_blocks_changed_approved_schema(monkeypatch):
    monkeypatch.setattr(design_governance, "ai_store_db_writer", lambda **kwargs: None)
    bootstrapped = design_governance.sftp_metadata_bootstrap_node(_approved_design_state())
    sealed = design_governance.sftp_plan_seal_node(bootstrapped)
    changed_bootstrap = {
        **sealed["metadata_bootstrap"],
        "schemas": [
            {
                **sealed["metadata_bootstrap"]["schemas"][0],
                "columns": [
                    *sealed["metadata_bootstrap"]["schemas"][0]["columns"],
                    {"name": "unexpected_column", "data_type": "string"},
                ],
            },
        ],
    }

    result = design_governance.sftp_freshness_check_node({
        **sealed,
        "metadata_bootstrap": changed_bootstrap,
    })

    assert result["freshness_check_status"] == "FAILED"
    assert result["status"] == "FAILED"
    assert result["freshness_manifest"]["stale_components"] == ["schema"]


def test_stage_execution_refuses_to_complete_without_named_handoff(monkeypatch):
    from services import pipeline_runtime

    saved = []
    monkeypatch.setattr(
        pipeline_runtime,
        "save_checkpoint_state_timed",
        lambda run_id, state, context: saved.append((context, dict(state))),
    )

    result = execute_sftp_stage(
        {"run_id": "run-contract", "source": "sftp"},
        "plan_seal",
        lambda state: {**state, "some_other_status": "COMPLETED"},
    )

    assert result["status"] == "FAILED"
    assert result["stage_statuses"]["plan_seal"] == "FAILED"
    assert "plan_seal_status=COMPLETED" in result["error"]
    assert [context for context, _ in saved] == ["plan_seal:running", "plan_seal:failed"]
