from __future__ import annotations

import sys
import types
from pathlib import Path

from sftp_nodes import governance, source_ingestion
from services import sftp_runtime


def test_start_sftp_pipeline_normalizes_entity_and_invokes_graph(monkeypatch):
    captured = {}

    class StubGraph:
        def invoke(self, state):
            captured["state"] = state
            return {"status": "COMPLETED", "source": state["source"]}

    monkeypatch.setattr(sftp_runtime, "_get_graph", lambda: StubGraph())

    result = sftp_runtime.start_sftp_pipeline(
        run_id="run-sftp",
        brd_text="test",
        sftp_entity="invalid",
        source="sftp",
    )

    assert result["run_id"] == "run-sftp"
    assert result["result"]["status"] == "COMPLETED"
    assert captured["state"]["sftp_entity"] == "transactions"


def test_start_sftp_pipeline_uses_auto_entity_for_adls(monkeypatch):
    captured = {}

    class StubGraph:
        def invoke(self, state):
            captured["state"] = state
            return state

    monkeypatch.setattr(sftp_runtime, "_get_graph", lambda: StubGraph())

    sftp_runtime.start_sftp_pipeline(
        run_id="run-adls",
        brd_text="test",
        sftp_entity="employee",
        source="adls_gen2",
    )

    assert captured["state"]["sftp_entity"] == "auto"


def test_adls_source_ingestion_uses_demo_fallback_when_live_read_fails(monkeypatch, tmp_path):
    class StubCredential:
        pass

    class StubServiceClient:
        def __init__(self, *args, **kwargs):
            pass

        def get_file_system_client(self, file_system):
            return object()

    azure_module = types.ModuleType("azure")
    identity_module = types.ModuleType("azure.identity")
    storage_module = types.ModuleType("azure.storage")
    datalake_module = types.ModuleType("azure.storage.filedatalake")
    identity_module.DefaultAzureCredential = StubCredential
    datalake_module.DataLakeServiceClient = StubServiceClient
    azure_module.identity = identity_module
    azure_module.storage = storage_module
    storage_module.filedatalake = datalake_module

    monkeypatch.setitem(sys.modules, "azure", azure_module)
    monkeypatch.setitem(sys.modules, "azure.identity", identity_module)
    monkeypatch.setitem(sys.modules, "azure.storage", storage_module)
    monkeypatch.setitem(sys.modules, "azure.storage.filedatalake", datalake_module)
    monkeypatch.setattr(source_ingestion, "_discover_adls_child_paths", lambda fs: (_ for _ in ()).throw(RuntimeError("auth failed")))
    monkeypatch.setattr(source_ingestion, "ADLS_FILE_SYSTEM", "athena")
    monkeypatch.setattr(source_ingestion, "ADLS_SOURCE_ROOT", "evention/vendor1/machine1")
    monkeypatch.setattr(source_ingestion, "ADLS_VENDOR_NAME", "Vendor1")

    result = source_ingestion.source_ingestion_node({"run_id": "run-adls", "source": "adls_gen2"})

    assert result["status"] != "FAILED"
    assert result["source_ingestion_status"] == "COMPLETED"
    assert result["adls_demo_fallback"] is True
    assert result["sftp_entity"] == "auto"
    assert {feed["entity"] for feed in result["candidate_feeds"]} == {"employee", "transactions"}
    assert all(feed["source"] == "adls_gen2" for feed in result["candidate_feeds"])
    assert all(Path(path).exists() for path in result["sftp_files"])
    assert result["source_row_count"] == 4


def test_feed_registry_persistence_skips_connection_failures(monkeypatch):
    monkeypatch.setattr(
        governance,
        "get_pipeline_connection",
        lambda: (_ for _ in ()).throw(RuntimeError("sql firewall")),
    )

    governance._persist_file_feed_registry(
        [{"feed_id": "Vendor1_Deposit", "vendor": "Vendor1", "entity": "Deposit", "source": "adls_gen2"}]
    )
    governance._mark_registry_feeds_approved(
        [{"feed_id": "Vendor1_Deposit", "vendor": "Vendor1", "entity": "Deposit"}]
    )


def test_safe_fetch_returns_empty_dict_on_error(monkeypatch):
    monkeypatch.setattr(
        sftp_runtime,
        "fetch_json_artifact",
        lambda run_id, artifact_name: (_ for _ in ()).throw(RuntimeError("db fail")),
    )

    assert sftp_runtime._safe_fetch("run-1", "ENRICHED_METADATA") == {}


def test_get_sftp_run_context_builds_expected_gate_and_status(monkeypatch):
    checkpoint = {
        "run_id": "run-ctx",
        "source": "sftp",
        "gate1": {"decision": "APPROVED"},
        "gate2": {"decision": "APPROVED"},
        "candidate_feeds": [{"feed_id": "f1", "vendor": "Vendor1", "entity": "transactions"}],
        "sftp_entity": "transactions",
        "source_columns": ["transaction_id"],
        "source_row_count": 42,
    }
    summary = [
        {"artifact_type": "SFTP_SCHEMA_SNAPSHOT", "stage": "schema snapshot"},
        {"artifact_type": "SFTP_COLUMN_PROFILING", "stage": "column profiling"},
        {"artifact_type": "ENRICHED_METADATA", "stage": "semantic enrichment"},
    ]
    enriched_payload = {
        "columns": [
            {
                "feed_id": "f1",
                "vendor": "Vendor1",
                "entity": "transactions",
                "semantic_type": "ID",
                "is_pii": True,
                "is_primary_key": True,
                "is_measure": False,
            }
        ],
        "joins": [{"left": "a", "right": "b"}],
        "semantic_counts": {"ID": 1},
    }

    monkeypatch.setattr(sftp_runtime, "load_checkpoint_state", lambda run_id: checkpoint)
    monkeypatch.setattr(sftp_runtime, "fetch_run_summary", lambda run_id: summary)
    monkeypatch.setattr(
        sftp_runtime,
        "fetch_json_artifact",
        lambda run_id, artifact: (
            enriched_payload if artifact == "ENRICHED_METADATA" else {}
        ),
    )
    monkeypatch.setattr(
        sftp_runtime,
        "build_pipeline_steps",
        lambda **kwargs: [
            {"key": "gate1", "label": "KPI Review", "state": "COMPLETED"},
            {"key": "gate2", "label": "Feed Review", "state": "COMPLETED"},
            {"key": "gate3", "label": "Enrichment Review", "state": "PENDING"},
        ],
    )
    monkeypatch.setattr(sftp_runtime, "load_bronze_scripts", lambda run_id, checkpoint=None: {"generated_at": None, "scripts": []})
    monkeypatch.setattr(sftp_runtime, "load_silver_scripts", lambda run_id, checkpoint=None: {"generated_at": None, "scripts": []})
    monkeypatch.setattr(sftp_runtime, "load_gold_scripts", lambda run_id, checkpoint=None: {"generated_at": None, "scripts": []})

    context = sftp_runtime.get_sftp_run_context("run-ctx")

    assert context["next_gate"] == 3
    assert context["status"] == "HITL_WAIT"
    assert context["gate3_approved"] is False
    assert context["pii_columns"][0]["feed_id"] == "f1"
    assert context["join_key_columns"][0]["feed_id"] == "f1"
    assert context["feed_semantic_summary"][0]["column_count"] == 1
    assert context["source_row_count"] == 42


def test_get_sftp_run_context_does_not_open_gate2_before_source_discovery(monkeypatch):
    checkpoint = {
        "run_id": "run-pregate2",
        "source": "adls_gen2",
        "gate1": {"decision": "APPROVED"},
        "sftp_entity": "auto",
        "brd_text": "valid brd",
    }

    monkeypatch.setattr(sftp_runtime, "load_checkpoint_state", lambda run_id: checkpoint)
    monkeypatch.setattr(sftp_runtime, "fetch_run_summary", lambda run_id: [])
    monkeypatch.setattr(sftp_runtime, "fetch_json_artifact", lambda run_id, artifact: {})
    monkeypatch.setattr(
        sftp_runtime,
        "build_pipeline_steps",
        lambda **kwargs: [
            {"key": "ingestion", "label": "BRD Ingest", "state": "COMPLETED"},
            {"key": "gate1", "label": "KPI Review", "state": "COMPLETED"},
            {"key": "discovery", "label": "Feed Discovery", "state": "PENDING"},
            {"key": "gate2", "label": "Feed Review", "state": "PENDING"},
        ],
    )

    context = sftp_runtime.get_sftp_run_context("run-pregate2")

    assert context["next_gate"] is None
    assert context["status"] == "RUNNING"
    assert "Feed review will open" in context["resume_message"]


def test_get_sftp_run_context_handles_script_loader_failure(monkeypatch):
    checkpoint = {
        "run_id": "run-script",
        "source": "sftp",
        "gate1": {"decision": "APPROVED"},
        "gate2": {"decision": "APPROVED"},
        "enrichment_review_decision": "APPROVED",
        "gate4": {"decision": "APPROVED"},
        "gate5": {"decision": "APPROVED"},
        "gold_generation_status": "COMPLETED",
    }
    summary = [{"artifact_type": "GOLD_SCRIPTS", "stage": "gold generation"}]

    monkeypatch.setattr(sftp_runtime, "load_checkpoint_state", lambda run_id: checkpoint)
    monkeypatch.setattr(sftp_runtime, "fetch_run_summary", lambda run_id: summary)
    monkeypatch.setattr(sftp_runtime, "fetch_json_artifact", lambda run_id, artifact: {})
    monkeypatch.setattr(
        sftp_runtime,
        "build_pipeline_steps",
        lambda **kwargs: [{"key": "gold", "label": "Gold", "state": "COMPLETED"}],
    )
    monkeypatch.setattr(
        sftp_runtime,
        "load_gold_scripts",
        lambda run_id, checkpoint=None: (_ for _ in ()).throw(RuntimeError("script load failed")),
    )

    context = sftp_runtime.get_sftp_run_context("run-script")

    assert context["status"] == "PIPELINE_COMPLETED"
    assert context["gold"] == {"generated_at": None, "scripts": []}


def test_generation_flags_treat_checkpoint_script_results_as_completed():
    flags = sftp_runtime._compute_generation_flags(
        [],
        {
            "bronze_generation_results": [{"script_body": "bronze"}],
            "silver_generation_results": [{"script_body": "silver"}],
            "gold_generation_results": [{"script_body": "gold"}],
        },
    )

    assert flags["bronze_generation_completed"] is True
    assert flags["silver_generation_completed"] is True
    assert flags["gold_generation_completed"] is True


def test_build_sftp_display_name_uses_discovered_entities():
    name = sftp_runtime.build_sftp_display_name(
        {
            "source": "sftp",
            "vendor": "VendorX",
            "candidate_feeds": [
                {"entity": "employee"},
                {"entity": "transactions"},
            ],
        }
    )

    assert name == "sftp:VendorX:employee+transactions"
