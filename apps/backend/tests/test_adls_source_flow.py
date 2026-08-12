from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from services import adls_source, file_metadata, pipeline_runtime, sftp_runtime
from services.metadata_contracts import validate_connection
from services.metadata_repository import MetadataRepository
from sftp_nodes.bronze_generation import add_xml_reader
from sftp_nodes.hitl import _approved_feeds, submit_sftp_gate1_review, submit_sftp_gate2_review


class _Paths:
    def get_paths(self, **_kwargs):
        now = datetime.now(timezone.utc)
        return [
            SimpleNamespace(name="INSURANCE_SFTP/insurance/claims", is_directory=True),
            SimpleNamespace(
                name="INSURANCE_SFTP/insurance/claims/claims.csv",
                is_directory=False,
                content_length=10,
                etag='"csv-etag"',
                last_modified=now,
            ),
            SimpleNamespace(
                name="INSURANCE_SFTP/insurance/policies/policies.json",
                is_directory=False,
                content_length=20,
                etag='"json-etag"',
                last_modified=now,
            ),
            SimpleNamespace(
                name="INSURANCE_SFTP/insurance/payments/payments.xml",
                is_directory=False,
                content_length=30,
                etag='"xml-etag"',
                last_modified=now,
            ),
            SimpleNamespace(
                name="INSURANCE_SFTP/insurance/control/_SUCCESS",
                is_directory=False,
                content_length=0,
                etag="",
                last_modified=now,
            ),
        ]


def test_adls_discovery_is_one_object_per_supported_physical_file(monkeypatch):
    monkeypatch.setattr(adls_source, "file_system_client", lambda: _Paths())
    files = adls_source.discover_files()

    assert [item["file_format"] for item in files] == ["csv", "xml", "json"]
    assert len({item["source_path"] for item in files}) == 3
    assert all(item["source_path"].startswith("abfss://athena@atheastorage") for item in files)


def test_schema_inference_handles_csv_json_and_xml(monkeypatch):
    payloads = {
        "csv": b"claim_id,amount\n1,10.5\n2,20.0\n",
        "json": b'{"policy_id": 1, "active": true}\n{"policy_id": 2, "active": false}\n',
        "xml": b"<rows><row><payment_id>1</payment_id><amount>2.5</amount></row></rows>",
    }
    monkeypatch.setattr(
        adls_source,
        "_download_sample",
        lambda path: payloads[path.rsplit(".", 1)[-1]],
    )

    inferred = [
        adls_source.infer_schema(
            {
                "remote_path": f"/INSURANCE_SFTP/insurance/{fmt}/sample.{fmt}",
                "source_path": f"abfss://athena@atheastorage.dfs.core.windows.net/sample.{fmt}",
                "file_format": fmt,
                "entity": fmt,
                "table_name": fmt,
            }
        )
        for fmt in ("csv", "json", "xml")
    ]

    assert all(item["schema_status"] == "INFERRED" for item in inferred)
    assert all(item["columns"] for item in inferred)
    assert inferred[-1]["parser_options"] == {"rowTag": "row"}


def test_adls_merge_key_profile_validates_composite_uniqueness(monkeypatch):
    monkeypatch.setattr(
        adls_source,
        "_download_sample",
        lambda _path: b"claim_id,update_num,amount\n1,1,10\n1,2,20\n2,1,30\n",
    )
    evidence = adls_source.profile_merge_key(
        {"remote_path": "/root/claims.csv", "file_format": "csv"},
        ["claim_id", "update_num"],
    )

    assert evidence["sample_rows"] == 3
    assert evidence["completeness_ratio"] == 1.0
    assert evidence["uniqueness_ratio"] == 1.0


def test_adls_merge_key_candidates_find_dynamic_versioned_composite(monkeypatch):
    monkeypatch.setattr(
        adls_source,
        "_download_sample",
        lambda _path: b"record,revision,description\n1,1,a\n1,2,b\n2,1,c\n",
    )

    candidates = adls_source.profile_merge_key_candidates(
        {"remote_path": "/root/arbitrary.csv", "file_format": "csv"},
        preferred_columns=["record"],
    )

    assert ["record", "revision"] in [candidate["columns"] for candidate in candidates]
    assert all(candidate["uniqueness_ratio"] >= 0.98 for candidate in candidates)


def test_xml_bronze_reader_uses_inferred_row_tag():
    script = '''if FILE_FORMAT == "csv":
    pass
elif FILE_FORMAT == "parquet":
    df = spark.read.format("parquet").load(SOURCE_PATH)
else:
    raise ValueError(f"Unsupported FILE_FORMAT: {FILE_FORMAT}")'''

    generated = add_xml_reader(script, "claim")

    assert '.format("xml").option("rowTag", \'claim\')' in generated


def test_adls_design_persists_only_source_file_objects(monkeypatch):
    captured = {"objects": [], "mappings": []}

    class Repository:
        @contextmanager
        def unit_of_work(self):
            yield self

        def table(self, name):
            return name

        def execute(self, *_args, **_kwargs):
            return None

        def query(self, *_args, **_kwargs):
            return [
                {
                    "ingestion_object_id": row["ingestion_object_id"],
                    "config_version": row["config_version"],
                    "config_hash": row["config_hash"],
                }
                for row in captured["objects"]
            ]

    repository = Repository()
    monkeypatch.setattr(
        file_metadata,
        "validated_metadata_selection",
        lambda _state: SimpleNamespace(repository=repository),
    )
    monkeypatch.setattr(
        file_metadata,
        "_merge_rows",
        lambda _repository, table, rows, _key: captured[
            "objects" if table == "cfg_ingestion_object" else "mappings"
        ].extend(rows),
    )
    state = {
        "source": "adls_gen2",
        "target_warehouse": "databricks",
        "source_system_id": 11,
        "source_connection_id": 22,
    }
    files = [
        {
            "source_path": f"abfss://athena@account.dfs.core.windows.net/root/{entity}/{entity}.csv",
            "file_name": f"{entity}.csv",
            "file_format": "csv",
            "entity": entity,
            "table_name": entity,
            "columns": [
                {
                    "column_name": "id",
                    "data_type": "bigint",
                    "ordinal_position": 1,
                    "is_nullable": False,
                }
            ],
        }
        for entity in ("claims", "policies")
    ]

    result = file_metadata.persist_file_design(state, files)

    assert result["file_ingestion_object_count"] == 2
    assert len(captured["objects"]) == 2
    assert all(row["ingestion_type"] == "FILE" for row in captured["objects"])
    assert all(row["object_type"] == "FILE" for row in captured["objects"])
    assert {row["processing_stage"] for row in captured["mappings"]} == {
        "SOURCE_TO_BRONZE",
        "BRONZE_TO_SILVER",
        "SILVER_TO_GOLD",
    }
    assert {row["ingestion_object_id"] for row in captured["mappings"]} == {
        row["ingestion_object_id"] for row in captured["objects"]
    }


def test_adls_source_mapping_passes_existing_content_hash_validator():
    state = {
        "target_warehouse": "databricks",
        "source_system_id": 11,
        "source_connection_id": 22,
    }
    table = {
        "source_path": "abfss://athena@account/root/claims/claims.csv",
        "file_format": "csv",
        "table_name": "claims",
        "columns": [
            {
                "column_name": "claim_id",
                "data_type": "bigint",
                "ordinal_position": 1,
                "is_nullable": False,
            }
        ],
    }
    ingestion_object = file_metadata._object_row(state, table)
    rows = [
        row
        for row in file_metadata._mapping_rows(state, table, ingestion_object)
        if row["processing_stage"] == "SOURCE_TO_BRONZE"
    ]

    bundle = MetadataRepository._validate_mapping_bundle_rows(
        object(),
        rows,
        ingestion_object_id=ingestion_object["ingestion_object_id"],
        processing_stage="SOURCE_TO_BRONZE",
        mapping_version=rows[0]["mapping_version"],
        expected_hash=rows[0]["mapping_hash"],
        expected_target=ingestion_object["target_bronze_table"],
        require_active=True,
    )

    assert bundle["mapping_hash"] == rows[0]["mapping_hash"]


def test_adls_flow_is_generation_first_without_changing_database_predicates():
    assert pipeline_runtime.generation_first_native_database_flow(
        {
            "source": "adls_gen2",
            "database_flow_version": "generation_first_v2",
            "target_warehouse": "databricks",
            "execution_engine": "native",
        }
    )
    assert pipeline_runtime.generation_first_native_database_flow(
        {
            "source": "database",
            "database_flow_version": "generation_first_v2",
            "target_warehouse": "databricks",
            "execution_engine": "native",
        }
    )
    assert not pipeline_runtime.generation_first_database_flow(
        {
            "source": "database",
            "database_flow_version": "",
            "target_warehouse": "databricks",
        }
    )


def test_adls_pipeline_steps_use_database_generation_first_projection():
    steps = pipeline_runtime.build_pipeline_steps(
        source="adls_gen2",
        checkpoint={
            "source": "adls_gen2",
            "database_flow_version": "generation_first_v2",
            "target_warehouse": "databricks",
            "execution_engine": "native",
        },
        summary=[],
        pending_gate1=[],
        completed_gate1=[],
        nominated_tables=[],
        certified_tables=[],
        enriched_payload={},
        gate3_payload={},
        bronze_generation_completed=False,
        silver_generation_completed=False,
        gold_generation_completed=False,
    )
    keys = [step["key"] for step in steps]

    assert "metadata_ddl" in keys
    assert "metadata_ddl_review" in keys
    assert "gold_review" in keys
    assert keys.index("gold_review") < keys.index("bronze_code_execution")
    assert not {
        "schema",
        "pre_bronze_bootstrap_metadata",
        "plan_seal",
        "plan_freshness",
        "pre_bronze_metadata_codegen",
        "bronze_runtime_validation",
    }.intersection(keys)


def test_adls_connection_contract_contains_references_not_secret_values():
    _, connection = adls_source.source_catalog(platform="databricks")
    validated = validate_connection(connection)

    assert validated["connection_type"] == "ADLS"
    assert "AZURE_CLIENT_SECRET" in validated["secrets_json"]
    assert "client_secret\":" not in validated["config_json"]


def test_adls_start_reuses_checkpointed_database_stage_orchestrator(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        sftp_runtime,
        "load_checkpoint_state",
        lambda _run_id: {"owner_email": "owner@example.com"},
    )
    monkeypatch.setattr(
        adls_source,
        "source_catalog",
        lambda **_kwargs: (
            {"source_system_id": 11},
            {"connection_id": 22, "config_version": 1, "config_hash": "hash"},
        ),
    )

    def continue_pipeline(run_id, *, start_stage_key, state):
        captured.update({"run_id": run_id, "start": start_stage_key, "state": state})
        return {**state, "status": "HITL_WAIT", "next_gate": 1}

    monkeypatch.setattr(sftp_runtime, "continue_database_pipeline", continue_pipeline)

    result = sftp_runtime.start_sftp_pipeline(run_id="run-adls", source="adls_gen2")

    assert captured["start"] == "ingestion"
    assert captured["state"]["owner_email"] == "owner@example.com"
    assert captured["state"]["database_flow_version"] == "generation_first_v2"
    assert result["result"]["next_gate"] == 1


def test_adls_table_review_accepts_shared_qualified_table_key():
    feed = {
        "database_name": "insurance",
        "schema_name": "source",
        "table_name": "claims",
        "source_path": "abfss://athena@atheastorage/insurance/claims.csv",
    }

    assert _approved_feeds([feed], ["insurance.source.claims"]) == [feed]


def test_adls_gate1_carries_approved_kpis_into_certified_state(monkeypatch):
    from services import pipeline_runtime
    from sftp_nodes import feed_nomination, source_ingestion
    from utilis import db

    saved = {}
    monkeypatch.setattr(
        pipeline_runtime,
        "load_checkpoint_state",
        lambda _run_id: {"run_id": "run-adls", "source": "adls_gen2"},
    )
    monkeypatch.setattr(
        pipeline_runtime,
        "save_checkpoint_state",
        lambda _run_id, state: saved.update(state),
    )
    monkeypatch.setattr(
        db,
        "get_completed_items",
        lambda _run_id, _gate: [{"kpi": {"kpi_name": "Claim Count"}}],
    )
    monkeypatch.setattr(
        source_ingestion,
        "source_ingestion_node",
        lambda state: {**state, "status": "IN_PROGRESS", "candidate_feeds": [{"entity": "claims"}]},
    )
    monkeypatch.setattr(
        feed_nomination,
        "sftp_feed_nomination_node",
        lambda state: {**state, "nominated_tables": state["candidate_feeds"]},
    )

    result = submit_sftp_gate1_review("run-adls")

    assert result["certified_kpis"] == [{"kpi_name": "Claim Count"}]
    assert saved["certified_kpis"] == [{"kpi_name": "Claim Count"}]


def test_adls_uses_only_source_specific_gate_in_shared_stage_orchestrator():
    from nodes.hitl import hitl_review_node
    from sftp_nodes.governance import sftp_gate1_node

    assert pipeline_runtime._database_stage_runner(
        "gate1", {"source": "adls_gen2"}
    ) is sftp_gate1_node
    assert pipeline_runtime._database_stage_runner(
        "gate1", {"source": "database"}
    ) is hitl_review_node


def test_adls_gate2_checkpoints_each_shared_metadata_stage(monkeypatch):
    from sftp_nodes import column_profiling, metadata_discovery, semantic_enrichment

    calls = []
    checkpoint = {
        "run_id": "run-adls",
        "source": "adls_gen2",
        "nominated_tables": [{"table_name": "claims", "source_path": "claims.csv"}],
    }
    monkeypatch.setattr(pipeline_runtime, "load_checkpoint_state", lambda _run_id: checkpoint)
    monkeypatch.setattr(pipeline_runtime, "save_checkpoint_state", lambda *_args: None)
    monkeypatch.setattr(
        pipeline_runtime,
        "run_with_minimum_stage_runtime",
        lambda key, runner, state: calls.append(key) or runner(state),
    )
    monkeypatch.setattr(
        metadata_discovery,
        "file_metadata_discovery_node",
        lambda state: {**state, "metadata_status": "COMPLETED", "discovered_metadata": {"tables": []}},
    )
    monkeypatch.setattr(
        column_profiling,
        "sftp_column_profiling_node",
        lambda state: {**state, "column_profiling_status": "COMPLETED"},
    )
    monkeypatch.setattr(
        semantic_enrichment,
        "sftp_semantic_enrichment_node",
        lambda state: {**state, "semantic_enrichment_status": "COMPLETED"},
    )
    monkeypatch.setattr(file_metadata, "persist_file_design", lambda state, _tables: state)

    result = submit_sftp_gate2_review("run-adls")

    assert calls == ["discovery", "profiling", "enrichment"]
    assert result["status"] == "HITL_WAIT"


def test_adls_silver_reuses_source_objects_and_persists_reviewed_mapping(monkeypatch):
    captured = []
    state = {
        "run_id": "run-adls",
        "source": "adls_gen2",
        "target_warehouse": "databricks",
        "source_system_id": 11,
        "source_connection_id": 22,
    }
    table = {
        "source_path": "abfss://athena@account/root/claims/claims.csv",
        "file_format": "csv",
        "database_name": "insurance",
        "schema_name": "source",
        "table_name": "claims",
        "columns": [
            {"column_name": "claim_id", "data_type": "bigint", "ordinal_position": 1},
            {"column_name": "amount", "data_type": "decimal", "ordinal_position": 2},
        ],
    }
    ingestion_object = file_metadata._object_row(state, table)
    certified = {
        **table,
        "ingestion_object_id": ingestion_object["ingestion_object_id"],
        "ingestion_object_config_version": ingestion_object["config_version"],
        "ingestion_object_config_hash": ingestion_object["config_hash"],
    }

    class Repository:
        @contextmanager
        def unit_of_work(self):
            yield self

        def get_ingestion_object(self, object_id, config_version):
            assert object_id == ingestion_object["ingestion_object_id"]
            assert config_version == ingestion_object["config_version"]
            return ingestion_object

    monkeypatch.setattr(
        file_metadata,
        "validated_metadata_selection",
        lambda _state: SimpleNamespace(repository=Repository()),
    )
    monkeypatch.setattr(
        file_metadata,
        "_merge_rows",
        lambda _repository, table_name, rows, _key: captured.append((table_name, rows)),
    )
    prepared = file_metadata.prepare_file_silver_generation(
        {
            **state,
            "certified_tables": [certified],
            "bronze_generation_results": [
                {
                    "ingestion_object_id": ingestion_object["ingestion_object_id"],
                    "source_table": "insurance.source.claims",
                }
            ],
        },
        {"feeds": [{"table": "claims", "merge_keys": ["claim_id"]}]},
    )

    assert [name for name, _rows in captured] == ["cfg_mapping"]
    assert all(row["processing_stage"] == "BRONZE_TO_SILVER" for row in captured[0][1])
    assert [row["target_column_name"] for row in captured[0][1] if row["is_primary_key"]] == ["claim_id"]
    assert prepared["silver_generation_table_refs"][0]["metadata_driven"] is True
    assert prepared["silver_generation_table_refs"][0]["mapping_columns"][0]["is_join_key"] is True
    assert "silver_ingestion_object_id" not in prepared["silver_generation_table_refs"][0]


def test_silver_stage_dispatch_is_source_scoped(monkeypatch):
    from nodes import silver_gen
    from sftp_nodes import silver_generation

    monkeypatch.setattr(
        silver_gen,
        "silver_code_generation_node",
        lambda state: {**state, "generator": "database", "silver_generation_status": "SKIPPED"},
    )
    monkeypatch.setattr(
        silver_generation,
        "silver_code_generation_node",
        lambda state: {**state, "generator": "adls", "silver_generation_status": "SKIPPED"},
    )

    assert pipeline_runtime._run_database_silver_stage({"source": "database"})["generator"] == "database"
    assert pipeline_runtime._run_database_silver_stage({"source": "adls_gen2"})["generator"] == "adls"


def test_adls_silver_orchestration_stays_outside_database_node(monkeypatch):
    from unittest.mock import mock_open

    from nodes import silver_gen
    from sftp_nodes import silver_generation

    table_ref = {
        "database_name": "insurance",
        "schema_name": "source",
        "table_name": "claims",
        "bronze_table": "workspace.bronze.bronze_claims",
        "silver_table": "workspace.silver.silver_claims",
        "existing_script_path": None,
        "source_columns": [],
        "mapping_columns": [{"column_name": "claim_id", "is_join_key": True}],
        "metadata_driven": True,
        "bronze_model_name": None,
    }
    monkeypatch.setattr(
        file_metadata,
        "prepare_file_silver_generation",
        lambda state, _artifact: {**state, "silver_generation_table_refs": [table_ref]},
    )
    monkeypatch.setattr(
        silver_generation.dbt_snowflake_runtime,
        "resolve_execution_engine",
        lambda _state: "native",
    )
    monkeypatch.setattr(
        silver_generation.dbt_snowflake_runtime,
        "snowflake_dbt_enabled",
        lambda _state: False,
    )
    monkeypatch.setattr(
        silver_gen,
        "_generate_one_table",
        lambda ref, **_kwargs: {
            "table": ref["table_name"],
            "source_table": ref["bronze_table"],
            "target_table": ref["silver_table"],
            "generation_mode": "DETERMINISTIC",
            "llm_enabled": False,
        },
    )
    captured_bundles = []
    monkeypatch.setattr(silver_gen, "_silver_output_dir_for", lambda _target: "ignored")
    monkeypatch.setattr(silver_generation.os, "makedirs", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("builtins.open", mock_open())
    monkeypatch.setattr(
        silver_generation.json,
        "dump",
        lambda payload, _file, **_kwargs: captured_bundles.append(payload),
    )
    monkeypatch.setattr(silver_gen, "_write_silver_readme", lambda **_kwargs: "readme")
    monkeypatch.setattr(silver_gen, "_write_silver_ui", lambda **_kwargs: "ui")
    monkeypatch.setattr(
        silver_gen,
        "_build_gold_generation_contract",
        lambda **_kwargs: {"status": "COMPLETED", "warnings": []},
    )
    monkeypatch.setattr(silver_gen, "_write_gold_contract", lambda _contract: "gold-contract")
    monkeypatch.setattr(silver_gen, "_persist_generation_artifacts", lambda **_kwargs: None)

    result = silver_generation.silver_code_generation_node(
        {
            "run_id": "run-adls",
            "source": "adls_gen2",
            "target_warehouse": "databricks",
            "silver_merge_key_review_artifact": {"feeds": []},
        }
    )
    assert result["silver_generation_status"] == "COMPLETED"
    assert result["silver_generation_results"][0]["generation_mode"] == "DETERMINISTIC"
    assert captured_bundles[0]["llm_enabled"] is False


def test_adls_merge_key_resolution_uses_llm_only_for_unresolved_files(monkeypatch):
    from sftp_nodes import silver_merge_key_resolution

    class Llm:
        def invoke(self, _prompt):
            return SimpleNamespace(
                content='{"feeds":[{"table":"claim_information","merge_keys":["ClaimID","UpdateNum"],'
                '"confidence":0.96,"reasoning":"Versioned claim identity"}]}'
            )

    monkeypatch.setattr(
        silver_merge_key_resolution,
        "profile_merge_key_candidates",
        lambda _feed, preferred_columns=(): [{
            "columns": ["ClaimID", "UpdateNum"],
            "sample_rows": 100,
            "complete_rows": 100,
            "distinct_rows": 100,
            "completeness_ratio": 1.0,
            "uniqueness_ratio": 1.0,
            "validation_scope": "bounded_source_sample",
            "candidate_score": 10.0,
        }],
    )
    monkeypatch.setattr(silver_merge_key_resolution, "ai_store_db_writer", lambda **_kwargs: None)
    state = {
        "run_id": "run-adls",
        "source": "adls_gen2",
        "bronze_review_artifact": {
            "feeds": [{"table": "claim_information", "entity": "claim_information", "primary_keys": []}]
        },
        "enriched_metadata": {
            "columns": [
                {"table_name": "claim_information", "column_name": "ClaimID", "is_join_key": True, "is_primary_key": False},
                {"table_name": "claim_information", "column_name": "UpdateNum", "is_join_key": False, "is_primary_key": False},
            ]
        },
        "column_profiles": [
            {"table_name": "claim_information", "column_name": "ClaimID", "sample_count": 100, "distinct_count": 50},
            {"table_name": "claim_information", "column_name": "UpdateNum", "sample_count": 100, "distinct_count": 10},
        ],
        "certified_tables": [
            {
                "table_name": "claim_information",
                "remote_path": "/root/claim_information.csv",
                "file_format": "csv",
                "columns": [{"column_name": "ClaimID"}, {"column_name": "UpdateNum"}],
            }
        ],
    }

    result = silver_merge_key_resolution.adls_silver_merge_key_resolution_node(state, llm=Llm())
    feed = result["silver_merge_key_review_artifact"]["feeds"][0]

    assert feed["merge_keys"] == ["ClaimID", "UpdateNum"]
    assert feed["merge_key_source"] == "adls_llm_profile_validated"
    assert feed["merge_key_profile_evidence"]["uniqueness_ratio"] == 1.0


def test_adls_merge_key_resolution_keeps_successful_feeds_when_one_has_no_key(monkeypatch):
    from sftp_nodes import silver_merge_key_resolution

    class Llm:
        def invoke(self, _prompt):
            return SimpleNamespace(
                content='{"feeds":[{"table":"claims","merge_keys":["ClaimID"],'
                '"confidence":0.95,"reasoning":"Validated business identifier"}]}'
            )

    def candidates(feed, preferred_columns=()):
        if feed["table_name"] == "notes":
            return []
        return [{
            "columns": ["ClaimID"],
            "sample_rows": 10,
            "complete_rows": 10,
            "distinct_rows": 10,
            "completeness_ratio": 1.0,
            "uniqueness_ratio": 1.0,
            "validation_scope": "bounded_source_sample",
            "candidate_score": 10.0,
        }]

    monkeypatch.setattr(silver_merge_key_resolution, "profile_merge_key_candidates", candidates)
    monkeypatch.setattr(silver_merge_key_resolution, "ai_store_db_writer", lambda **_kwargs: None)
    state = {
        "run_id": "run-adls",
        "source": "adls_gen2",
        "bronze_review_artifact": {"feeds": [{"table": "claims"}, {"table": "notes"}]},
        "enriched_metadata": {"columns": [
            {"table_name": "claims", "column_name": "ClaimID", "is_join_key": True},
            {"table_name": "notes", "column_name": "Text"},
        ]},
        "column_profiles": [
            {"table_name": "claims", "column_name": "ClaimID", "sample_count": 10, "distinct_count": 10},
            {"table_name": "notes", "column_name": "Text", "sample_count": 10, "distinct_count": 3},
        ],
        "certified_tables": [
            {"table_name": "claims", "columns": [{"column_name": "ClaimID"}]},
            {"table_name": "notes", "columns": [{"column_name": "Text"}]},
        ],
    }

    result = silver_merge_key_resolution.adls_silver_merge_key_resolution_node(state, llm=Llm())
    feeds = {feed["table"]: feed for feed in result["silver_merge_key_review_artifact"]["feeds"]}

    assert feeds["claims"]["merge_keys"] == ["ClaimID"]
    assert feeds["notes"].get("merge_keys") in (None, [])
    assert "No complete and unique key candidate" in feeds["notes"]["merge_key_resolution_error"]


def test_adls_merge_key_review_rejects_approved_empty_keys():
    from sftp_nodes.silver_merge_key_resolution import validate_adls_merge_key_review

    state = {
        "silver_merge_key_review_artifact": {"feeds": [{"table": "claims"}]},
        "certified_tables": [{"table_name": "claims", "columns": [{"column_name": "ClaimID"}]}],
    }
    with pytest.raises(ValueError, match="missing for claims"):
        validate_adls_merge_key_review(
            state,
            {"feeds": [{"table": "claims", "review_status": "APPROVED", "merge_keys": []}]},
        )


def test_adls_gold_adds_contract_driven_factless_facts_and_dimensions(monkeypatch):
    from unittest.mock import mock_open

    from nodes import gold_gen as shared_gold
    from sftp_nodes import gold_generation

    monkeypatch.setattr(
        shared_gold,
        "gold_code_generation_node",
        lambda _state: {
            "status": "FAILED",
            "gold_generation_status": "FAILED",
            "gold_generation_results": [{
                "kpi_name": "Claim Count",
                "status": "BLOCKED",
                "reason": "KPI formula is not certified.",
                "source_table": "workspace.silver.silver_claims",
            }],
        },
    )
    monkeypatch.setattr(shared_gold, "_gold_output_dir_for", lambda _target: "generated")
    monkeypatch.setattr(shared_gold, "_write_bundle", lambda **_kwargs: "bundle.json")
    monkeypatch.setattr(shared_gold, "_write_readme", lambda **_kwargs: "README.md")
    monkeypatch.setattr(shared_gold, "_write_ui", lambda **_kwargs: "index.html")
    monkeypatch.setattr(shared_gold, "_persist_gold_generation", lambda **_kwargs: None)
    monkeypatch.setattr(gold_generation.os, "makedirs", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("builtins.open", mock_open())

    result = gold_generation.gold_code_generation_node({
        "run_id": "adls-run",
        "source": "adls_gen2",
        "target_warehouse": "databricks",
        "gold_schema": "gold",
        "gold_generation_contract": {
            "run_id": "adls-run",
            "kpi_mappings": [{"kpi_name": "Claim Count", "measure": {"table": "claims"}}],
            "factless_mappings": [{
                "logical_table": "claims",
                "source_silver_table": "workspace.silver.silver_claims",
                "grain_columns": ["claim_id"],
            }],
            "dimension_mappings": [{
                "logical_table": "claims",
                "source_silver_table": "workspace.silver.silver_claims",
                "columns": ["claim_status"],
            }],
        },
    })

    assert result["gold_generation_status"] == "COMPLETED"
    assert result["gold_generation_error"] is None
    assert result["adls_factless_fact_count"] == 1
    assert result["adls_dimension_count"] == 1
    assert len(result["gold_generation_results"]) == 2
    fact = next(item for item in result["gold_generation_results"] if item["artifact_kind"] == "FACT")
    dimension = next(item for item in result["gold_generation_results"] if item["artifact_kind"] == "DIMENSION")
    assert fact["target_table"] == "gold.fact_claim_count"
    assert fact["computability_status"] == "NOT_COMPUTABLE"
    assert "fallback_reason" not in fact
    assert "KPI_NAME = 'Claim Count'" in fact["script_body"]
    assert dimension["target_table"] == "gold.dim_claims"
    assert all("ingestion_object_id" not in item for item in result["gold_generation_results"])


def test_adls_gold_wrapper_delegates_database_sources_unchanged(monkeypatch):
    from nodes import gold_gen as shared_gold
    from sftp_nodes import gold_generation

    expected = {"gold_generation_status": "DATABASE_SENTINEL"}
    monkeypatch.setattr(shared_gold, "gold_code_generation_node", lambda state: expected)

    assert gold_generation.gold_code_generation_node({"source": "database"}) is expected
