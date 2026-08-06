from __future__ import annotations

import json
import logging
import uuid
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

import pytest

from services.metadata_contracts import TargetMetadataContext
from services.metadata_contracts import validate_jdbc_connection
from services.metadata_repository import (
    DatabricksMetadataRepository,
    MetadataRepository,
    SnowflakeMetadataRepository,
    metadata_repository_for_target,
)
from utilis.logger import SecretRedactionFilter


class StubMetadataRepository(MetadataRepository):
    def __init__(self) -> None:
        super().__init__(TargetMetadataContext("databricks", "qa", "athena"))
        self.executed: List[tuple[str, Dict[str, Any]]] = []
        self.saved: Optional[Dict[str, Any]] = None
        self.objects: Dict[tuple[int, int], Dict[str, Any]] = {}
        self.mappings: List[Dict[str, Any]] = []
        self.source = {"source_system_id": 7, "active_flag": True}
        self.connection = {
            "connection_id": 11,
            "source_system_id": 7,
            "connection_type": "JDBC",
            "database_name": "ClaimsDB",
            "config_version": 1,
            "config_hash": "sha256:test",
            "active_flag": True,
            "is_current": True,
        }

    def execute(self, sql: str, parameters: Optional[Mapping[str, Any]] = None) -> None:
        values = dict(parameters or {})
        self.executed.append((sql, values))
        if "cfg_ingestion_object" in sql:
            if "UPDATE" in sql:
                for (object_id, version), row in self.objects.items():
                    if object_id == int(values["ingestion_object_id"]):
                        selected = version == int(values["config_version"])
                        row.update({"active_flag": selected, "is_current": selected})
            else:
                key = (int(values["ingestion_object_id"]), int(values["config_version"]))
                self.objects.setdefault(key, dict(values))
                self.saved = self.objects[key]
        if "cfg_mapping" in sql:
            if "UPDATE" in sql:
                for row in self.mappings:
                    if row["ingestion_object_id"] == int(values["ingestion_object_id"]):
                        selected = row["mapping_version"] == int(values["mapping_version"])
                        row.update({"active_flag": selected, "is_current": selected})
            else:
                row_indexes = sorted({int(key.split("_", 1)[0][1:]) for key in values if key.startswith("r")})
                for row_index in row_indexes:
                    prefix = f"r{row_index}_"
                    row = {key.removeprefix(prefix): value for key, value in values.items() if key.startswith(prefix)}
                    if not any(
                        saved["mapping_id"] == row["mapping_id"] and saved["mapping_version"] == row["mapping_version"]
                        for saved in self.mappings
                    ):
                        self.mappings.append(row)
        if "UPDATE" in sql and "cfg_connection" in sql:
            self.connection.update({"active_flag": True, "is_current": True})

    def query(self, sql: str, parameters: Optional[Mapping[str, Any]] = None) -> List[Dict[str, Any]]:
        if "cfg_ingestion_object" in sql:
            object_id = int((parameters or {}).get("ingestion_object_id") or 0)
            if "config_version =" in sql:
                row = self.objects.get((object_id, int((parameters or {})["config_version"])))
                return [dict(row)] if row else []
            return [
                dict(row)
                for (saved_id, _), row in self.objects.items()
                if saved_id == object_id and row.get("active_flag") and row.get("is_current")
            ]
        if "cfg_mapping" in sql:
            version = int((parameters or {})["mapping_version"])
            object_id = int((parameters or {}).get("ingestion_object_id") or 0)
            return [
                dict(row)
                for row in self.mappings
                if row["mapping_version"] == version and row["ingestion_object_id"] == object_id
            ]
        return []

    def get_source_system(self, source_system_id: int) -> Optional[Dict[str, Any]]:
        return dict(self.source) if source_system_id == 7 else None

    def get_active_connection(self, connection_id: int) -> Optional[Dict[str, Any]]:
        return (
            dict(self.connection)
            if connection_id == 11 and self.connection["active_flag"] and self.connection["is_current"]
            else None
        )

    def get_connection(self, connection_id: int, config_version: Optional[int] = None) -> Optional[Dict[str, Any]]:
        return dict(self.connection) if connection_id == 11 and config_version == 1 else None


def test_gate2_database_object_is_inactive_idempotent_source_to_bronze_draft() -> None:
    repository = StubMetadataRepository()
    table = {"database_name": "ClaimsDB", "schema_name": "dbo", "table_name": "Claims"}

    first = repository.upsert_database_ingestion_object_draft(
        source_system_id=7,
        connection_id=11,
        table=table,
        target_bronze_table="main.bronze.bronze_Claims",
    )
    second = repository.upsert_database_ingestion_object_draft(
        source_system_id=7,
        connection_id=11,
        table=table,
        target_bronze_table="main.bronze.bronze_Claims",
    )

    assert first["ingestion_object_id"] == second["ingestion_object_id"]
    assert first["processing_stage"] == "SOURCE_TO_BRONZE"
    assert first["object_name"] == "ClaimsDB.dbo.Claims"
    assert first["active_flag"] is False
    assert first["is_current"] is False
    assert "database_name AS database_name" not in repository.executed[0][0]


def test_gate2_object_rejects_cross_source_connection() -> None:
    repository = StubMetadataRepository()
    repository.connection["source_system_id"] = 99

    with pytest.raises(ValueError, match="different source system"):
        repository.upsert_database_ingestion_object_draft(
            source_system_id=7,
            connection_id=11,
            table={"database_name": "ClaimsDB", "schema_name": "dbo", "table_name": "Claims"},
        )


def test_gate2_object_requires_complete_source_identity() -> None:
    repository = StubMetadataRepository()

    with pytest.raises(ValueError, match="database_name, schema_name, and table_name"):
        repository.upsert_database_ingestion_object_draft(
            source_system_id=7,
            connection_id=11,
            table={"schema_name": "dbo"},
        )


def test_databricks_result_decoder_preserves_boolean_and_numeric_types() -> None:
    assert DatabricksMetadataRepository._decode("false", "BOOLEAN") is False
    assert DatabricksMetadataRepository._decode("42", "BIGINT") == 42
    assert DatabricksMetadataRepository._decode("1.5", "DOUBLE") == 1.5
    assert DatabricksMetadataRepository._decode(None, "STRING") is None


def test_connection_activates_only_after_validator_succeeds() -> None:
    repository = StubMetadataRepository()
    repository.connection.update({"active_flag": False, "is_current": False})
    validated = []

    active = repository.validate_and_activate_connection(11, 1, lambda row: validated.append(row["connection_id"]))

    assert validated == [11]
    assert active["active_flag"] is True
    assert active["is_current"] is True


def test_connection_remains_inactive_when_validator_fails() -> None:
    repository = StubMetadataRepository()
    repository.connection.update({"active_flag": False, "is_current": False})

    def fail(_: Mapping[str, Any]) -> None:
        raise RuntimeError("connectivity failed")

    with pytest.raises(RuntimeError, match="connectivity failed"):
        repository.validate_and_activate_connection(11, 1, fail)
    assert repository.connection["active_flag"] is False


def test_target_repository_uses_server_authorized_namespace(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ATHENA_TARGET_ENVIRONMENT", "qa")
    monkeypatch.setenv("ATHENA_DATABRICKS_METADATA_CATALOG", "athena_qa")
    monkeypatch.setenv("DATABRICKS_SQL_WAREHOUSE_ID", "warehouse-id")

    repository = metadata_repository_for_target(platform="databricks", environment="qa")

    assert repository.context.namespace == "athena_qa"
    with pytest.raises(ValueError, match="not served"):
        metadata_repository_for_target(platform="databricks", environment="prod")


def test_source_to_bronze_mapping_bundle_is_deterministic_and_inactive() -> None:
    repository = StubMetadataRepository()
    ingestion_object = {
        "ingestion_object_id": 123,
        "config_version": 1,
        "config_hash": "sha256:object",
        "object_kind": "INGESTION",
        "ingestion_type": "DATABASE",
        "processing_stage": "SOURCE_TO_BRONZE",
        "active_flag": False,
        "is_current": False,
        "object_name": "ClaimsDB.dbo.Claims",
        "target_bronze_table": "main.bronze.bronze_Claims",
    }
    columns = [
        {"column_name": "claim_id", "data_type": "int", "ordinal_position": 1, "is_primary_key": True},
        {
            "column_name": "amount",
            "data_type": "decimal",
            "data_type_full": "decimal(12,2)",
            "numeric_precision": 12,
            "numeric_scale": 2,
            "ordinal_position": 2,
        },
    ]

    first = repository.upsert_source_to_bronze_mapping_draft(
        ingestion_object=ingestion_object,
        columns=columns,
    )
    second = repository.upsert_source_to_bronze_mapping_draft(
        ingestion_object=ingestion_object,
        columns=list(reversed(columns)),
    )

    assert first["mapping_version"] == second["mapping_version"]
    assert first["mapping_hash"] == second["mapping_hash"]
    assert len(first["mappings"]) == 2
    assert all(row["active_flag"] is False for row in first["mappings"])
    assert first["mappings"][0]["processing_stage"] == "SOURCE_TO_BRONZE"
    assert len([sql for sql, _ in repository.executed if "cfg_mapping" in sql]) == 2
    assert "UNION ALL" in next(sql for sql, _ in repository.executed if "cfg_mapping" in sql)
    assert [row["target_column_name"] for row in first["mappings"]] == ["claim_id", "amount"]
    assert [row["target_data_type"] for row in first["mappings"]] == ["int", "decimal(12,2)"]


def test_source_to_bronze_mapping_rejects_normalization_collision_before_write() -> None:
    repository = StubMetadataRepository()
    ingestion_object = {
        "ingestion_object_id": 123,
        "config_version": 1,
        "config_hash": "sha256:object",
        "object_kind": "INGESTION",
        "ingestion_type": "DATABASE",
        "processing_stage": "SOURCE_TO_BRONZE",
        "active_flag": False,
        "is_current": False,
        "object_name": "ClaimsDB.dbo.Claims",
        "target_bronze_table": "main.bronze.bronze_Claims",
    }

    with pytest.raises(ValueError, match="duplicate source_field_path"):
        repository.upsert_source_to_bronze_mapping_draft(
            ingestion_object=ingestion_object,
            columns=[
                {"column_name": "ClaimID", "data_type": "int", "ordinal_position": 1},
                {"column_name": "claimid", "data_type": "int", "ordinal_position": 2},
            ],
        )

    assert not any("cfg_mapping" in sql for sql, _ in repository.executed)


def test_source_to_bronze_mapping_rejects_reserved_lineage_column() -> None:
    repository = StubMetadataRepository()
    ingestion_object = {
        "ingestion_object_id": 123,
        "config_version": 1,
        "config_hash": "sha256:object",
        "object_kind": "INGESTION",
        "ingestion_type": "DATABASE",
        "processing_stage": "SOURCE_TO_BRONZE",
        "active_flag": False,
        "is_current": False,
        "object_name": "ClaimsDB.dbo.Claims",
        "target_bronze_table": "main.bronze.bronze_Claims",
    }

    with pytest.raises(ValueError, match="reserved lineage columns: run_id"):
        repository.upsert_source_to_bronze_mapping_draft(
            ingestion_object=ingestion_object,
            columns=[{"column_name": "Run ID", "data_type": "varchar", "ordinal_position": 1}],
        )


def test_large_mapping_bundle_uses_one_atomic_statement() -> None:
    repository = StubMetadataRepository()
    ingestion_object = {
        "ingestion_object_id": 123,
        "config_version": 1,
        "config_hash": "sha256:object",
        "object_kind": "INGESTION",
        "ingestion_type": "DATABASE",
        "processing_stage": "SOURCE_TO_BRONZE",
        "active_flag": False,
        "is_current": False,
        "object_name": "ClaimsDB.dbo.Claims",
        "target_bronze_table": "main.bronze.bronze_Claims",
    }

    bundle = repository.upsert_source_to_bronze_mapping_draft(
        ingestion_object=ingestion_object,
        columns=[
            {"column_name": f"column_{index}", "data_type": "int", "ordinal_position": index}
            for index in range(1, 102)
        ],
    )

    writes = [(sql, parameters) for sql, parameters in repository.executed if "cfg_mapping" in sql]
    assert len(writes) == 1
    assert writes[0][0].count("SELECT") == 101
    assert len(bundle["mappings"]) == 101


def _active_bronze_contract(repository: StubMetadataRepository):
    draft = repository.upsert_database_ingestion_object_draft(
        source_system_id=7,
        connection_id=11,
        table={"database_name": "ClaimsDB", "schema_name": "dbo", "table_name": "Claims"},
        target_bronze_table="main.bronze.bronze_claims",
    )
    bundle = repository.upsert_source_to_bronze_mapping_draft(
        ingestion_object=draft,
        columns=[
            {"column_name": "ClaimID", "data_type": "int", "ordinal_position": 1},
            {"column_name": "Description", "data_type": "varchar", "ordinal_position": 2},
        ],
    )
    repository.objects[(draft["ingestion_object_id"], draft["config_version"])].update(
        {"active_flag": True, "is_current": True}
    )
    for row in repository.mappings:
        row.update({"active_flag": True, "is_current": True})
    return repository.get_active_ingestion_object(draft["ingestion_object_id"]), repository.get_mapping_bundle(
        ingestion_object_id=draft["ingestion_object_id"],
        processing_stage="SOURCE_TO_BRONZE",
        mapping_version=bundle["mapping_version"],
        expected_hash=bundle["mapping_hash"],
        expected_target=draft["target_bronze_table"],
        require_active=True,
    )


def test_bronze_to_silver_draft_is_exact_inactive_and_idempotent() -> None:
    repository = StubMetadataRepository()
    source_object, source_mapping = _active_bronze_contract(repository)
    columns = [
        {
            "source_field_path": row["target_column_name"],
            "source_data_type": row["target_data_type"],
            "target_column_name": row["target_column_name"],
            "target_data_type": row["target_data_type"],
            "is_nullable": row["is_nullable"],
            "ordinal_position": row["ordinal_position"],
            "transformation_rule": "TRIM_CAST" if row["target_column_name"] == "description" else "CAST",
        }
        for row in source_mapping["mappings"]
    ]

    first = repository.upsert_bronze_to_silver_draft(
        source_system_id=7,
        source_object=source_object,
        source_mapping=source_mapping,
        target_silver_table="main.silver.silver_claims",
        merge_keys=["claimid"],
        columns=columns,
    )
    second = repository.upsert_bronze_to_silver_draft(
        source_system_id=7,
        source_object=source_object,
        source_mapping=source_mapping,
        target_silver_table="main.silver.silver_claims",
        merge_keys=["claimid"],
        columns=columns,
    )

    assert first["ingestion_object"]["ingestion_object_id"] == second["ingestion_object"]["ingestion_object_id"]
    assert first["ingestion_object"]["object_kind"] == "TRANSFORMATION"
    assert first["ingestion_object"]["processing_stage"] == "BRONZE_TO_SILVER"
    assert first["ingestion_object"]["schema_evolution_policy"] == "FAIL"
    assert first["ingestion_object"]["active_flag"] is False
    assert first["mapping_bundle"]["active_flag"] is False
    assert [row["is_primary_key"] for row in first["mapping_bundle"]["mappings"]] == [True, False]
    assert [row["transformation_rule"] for row in first["mapping_bundle"]["mappings"]] == ["CAST", "TRIM_CAST"]
    dependency = json.loads(first["ingestion_object"]["dependency_objects_json"])["dependencies"][0]
    assert dependency["mapping_hash"] == source_mapping["mapping_hash"]


def test_bronze_to_silver_rejects_unknown_key_before_creating_transform() -> None:
    repository = StubMetadataRepository()
    source_object, source_mapping = _active_bronze_contract(repository)

    with pytest.raises(ValueError, match="merge key must be present"):
        repository.upsert_bronze_to_silver_draft(
            source_system_id=7,
            source_object=source_object,
            source_mapping=source_mapping,
            target_silver_table="main.silver.silver_claims",
            merge_keys=["missing_key"],
            columns=[
                {
                    "source_field_path": row["target_column_name"],
                    "source_data_type": row["target_data_type"],
                    "target_column_name": row["target_column_name"],
                    "target_data_type": row["target_data_type"],
                    "ordinal_position": row["ordinal_position"],
                }
                for row in source_mapping["mappings"]
            ],
        )

    assert len(repository.objects) == 1


def test_bronze_to_silver_reload_detects_mapping_tampering() -> None:
    repository = StubMetadataRepository()
    source_object, source_mapping = _active_bronze_contract(repository)
    created = repository.upsert_bronze_to_silver_draft(
        source_system_id=7,
        source_object=source_object,
        source_mapping=source_mapping,
        target_silver_table="main.silver.silver_claims",
        merge_keys=["claimid"],
        columns=[
            {
                "source_field_path": row["target_column_name"],
                "source_data_type": row["target_data_type"],
                "target_column_name": row["target_column_name"],
                "target_data_type": row["target_data_type"],
                "ordinal_position": row["ordinal_position"],
            }
            for row in source_mapping["mappings"]
        ],
    )
    next(
        row
        for row in repository.mappings
        if row["ingestion_object_id"] == created["ingestion_object"]["ingestion_object_id"]
    )["target_data_type"] = "string"

    with pytest.raises(RuntimeError, match="content-hash"):
        repository.get_mapping_bundle(
            ingestion_object_id=created["ingestion_object"]["ingestion_object_id"],
            processing_stage="BRONZE_TO_SILVER",
            mapping_version=created["mapping_bundle"]["mapping_version"],
            expected_hash=created["mapping_bundle"]["mapping_hash"],
            expected_target="main.silver.silver_claims",
            require_active=False,
        )


def test_gate5_activates_exact_silver_mapping_and_verified_artifact(monkeypatch) -> None:
    from services.metadata_contracts import file_sha256
    from utilis.generated_code_paths import generated_artifact_uri

    artifact_root = Path.cwd() / ".tmp-tests" / f"silver-activation-{uuid.uuid4().hex}"
    artifact = artifact_root / "silver" / "claims.py"
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact.write_text("print('silver claims')\n", encoding="utf-8")
    monkeypatch.setenv("ATHENA_GENERATED_CODE_DIR", str(artifact_root))
    repository = StubMetadataRepository()
    source_object, source_mapping = _active_bronze_contract(repository)
    draft = repository.upsert_bronze_to_silver_draft(
        source_system_id=7,
        source_object=source_object,
        source_mapping=source_mapping,
        target_silver_table="main.silver.silver_claims",
        merge_keys=["claimid"],
        columns=[
            {
                "source_field_path": row["target_column_name"],
                "source_data_type": row["target_data_type"],
                "target_column_name": row["target_column_name"],
                "target_data_type": row["target_data_type"],
                "ordinal_position": row["ordinal_position"],
            }
            for row in source_mapping["mappings"]
        ],
    )

    activated = repository.register_and_activate_bronze_to_silver_artifact(
        draft_config_version=draft["ingestion_object"]["config_version"],
        ingestion_object_id=draft["ingestion_object"]["ingestion_object_id"],
        mapping_version=draft["mapping_bundle"]["mapping_version"],
        mapping_hash=draft["mapping_bundle"]["mapping_hash"],
        execution_spec={
            "contract_version": "1.0",
            "execution_mode": "GENERATED_ARTIFACT",
            "target_platform": "DATABRICKS",
            "engine": "DATABRICKS_JOB",
            "artifact_uri": generated_artifact_uri(artifact),
            "entry_point": "script",
            "artifact_hash": file_sha256(artifact),
            "generator_version": "test",
            "mapping_version": draft["mapping_bundle"]["mapping_version"],
        },
    )

    assert activated["ingestion_object"]["active_flag"] is True
    assert activated["ingestion_object"]["target_silver_table"] == "main.silver.silver_claims"
    assert activated["mapping_bundle"]["active_flag"] is True
    assert activated["execution_spec"]["processing_stage"] == "BRONZE_TO_SILVER"


def test_silver_to_gold_draft_pins_active_inputs_and_structured_rules() -> None:
    repository = StubMetadataRepository()
    source_object, source_mapping = _active_bronze_contract(repository)
    silver = repository.upsert_bronze_to_silver_draft(
        source_system_id=7,
        source_object=source_object,
        source_mapping=source_mapping,
        target_silver_table="main.silver.silver_claims",
        merge_keys=["claimid"],
        columns=[
            {
                "source_field_path": row["target_column_name"],
                "source_data_type": row["target_data_type"],
                "target_column_name": row["target_column_name"],
                "target_data_type": row["target_data_type"],
                "ordinal_position": row["ordinal_position"],
            }
            for row in source_mapping["mappings"]
        ],
    )
    silver_id = silver["ingestion_object"]["ingestion_object_id"]
    repository.objects[(silver_id, silver["ingestion_object"]["config_version"])].update(
        {"active_flag": True, "is_current": True}
    )
    for row in repository.mappings:
        if row["ingestion_object_id"] == silver_id:
            row.update({"active_flag": True, "is_current": True})

    gold = repository.upsert_silver_to_gold_draft(
        source_system_id=7,
        target_gold_table="main.gold.fact_claim_count",
        inputs=[{
            "ingestion_object_id": silver_id,
            "config_version": silver["ingestion_object"]["config_version"],
            "config_hash": silver["ingestion_object"]["config_hash"],
            "mapping_version": silver["mapping_bundle"]["mapping_version"],
            "mapping_hash": silver["mapping_bundle"]["mapping_hash"],
        }],
        columns=[
            {
                "source_object_name": "main.silver.silver_claims",
                "source_field_path": "claimid",
                "source_data_type": "int",
                "target_column_name": "claimid",
                "target_data_type": "int",
                "ordinal_position": 1,
                "is_primary_key": True,
                "transformation_rule": "GROUP_KEY",
            },
            {
                "source_object_name": "main.silver.silver_claims",
                "source_field_path": "claimid",
                "source_data_type": "int",
                "target_column_name": "claim_count",
                "target_data_type": "BIGINT",
                "ordinal_position": 2,
                "transformation_rule": "AGG_COUNT",
            },
        ],
        merge_keys=["claimid"],
        join_rules=[],
        definition={
            "schema_version": "1.0",
            "object_type": "FACT",
            "transformation_group": "claim_count",
            "measures": [{"operator": "COUNT", "target_column": "claim_count"}],
        },
        build_order=20,
        validation_policy={
            "fail_on_missing_input": True,
            "fail_on_schema_mismatch": True,
            "fail_on_null_key": True,
            "fail_on_duplicate_key": True,
        },
    )

    assert gold["ingestion_object"]["processing_stage"] == "SILVER_TO_GOLD"
    assert gold["ingestion_object"]["active_flag"] is False
    assert gold["mapping_bundle"]["mappings"][0]["build_order"] == 20
    inputs = json.loads(gold["mapping_bundle"]["mappings"][0]["input_objects_json"])
    assert inputs[0]["mapping_hash"] == silver["mapping_bundle"]["mapping_hash"]
    rule_types = {
        rule["rule_type"]
        for rule in json.loads(gold["ingestion_object"]["validation_policy_json"])["rules"]
    }
    assert rule_types == {"INPUTS_PRESENT", "TARGET_SCHEMA_MATCH", "KEYS_NOT_NULL", "KEYS_UNIQUE"}


def test_runtime_error_sanitizer_removes_credentials() -> None:
    safe = MetadataRepository._safe_error_text(
        "Authorization: Bearer abc.def password=hunter2 token=secret-value", 2000
    )

    assert "abc.def" not in safe
    assert "hunter2" not in safe
    assert "secret-value" not in safe
    assert safe.count("[REDACTED]") >= 2


def test_runtime_error_redaction_handles_nested_secrets_and_url_userinfo() -> None:
    redacted = MetadataRepository._redact_sensitive(
        {
            "password": "hunter2",
            "nested": {"client_secret": "client-value"},
            "url": "https://source-user:source-password@example.test/path",
        }
    )
    text = json.dumps(redacted)

    assert "hunter2" not in text
    assert "client-value" not in text
    assert "source-user" not in text
    assert "source-password" not in text
    assert text.count("[REDACTED]") >= 3


def test_log_boundary_redacts_message_fields_and_traceback() -> None:
    try:
        raise RuntimeError("password=hunter2 https://url-user:url-pass@example.test/?sig=signature-value")
    except RuntimeError:
        record = logging.LogRecord(
            "athena",
            logging.ERROR,
            __file__,
            1,
            "token=%s",
            ("token-value",),
            __import__("sys").exc_info(),
        )
    record.client_secret = "client-value"

    assert SecretRedactionFilter().filter(record) is True
    rendered = record.getMessage() + str(record.client_secret) + str(record.exc_text)
    assert "hunter2" not in rendered
    assert "token-value" not in rendered
    assert "signature-value" not in rendered
    assert "client-value" not in rendered
    assert "url-user" not in rendered
    assert "url-pass" not in rendered


def test_watermark_candidate_cannot_stage_before_target_commit() -> None:
    class Repository(StubMetadataRepository):
        def query(self, sql, parameters=None):
            if "target_commit_status" in sql:
                return [{"target_commit_status": "STARTED", "validation_status": "PASSED"}]
            return super().query(sql, parameters)

    repository = Repository()

    with pytest.raises(RuntimeError, match="committed target write"):
        repository.stage_watermark_candidate(run_id="run-1", candidate_value="200", expected_version=1)

    assert not any("ctl_watermark" in sql for sql, _ in repository.executed)


def test_queue_claim_uses_conditional_lease_ownership() -> None:
    class Repository(StubMetadataRepository):
        def __init__(self):
            super().__init__()
            self.claimed = False

        def query(self, sql, parameters=None):
            if "ctl_ingestion_queue" not in sql:
                return super().query(sql, parameters)
            if "ORDER BY priority" in sql:
                return [] if self.claimed else [{"queue_id": 91}]
            if "claimed_by_worker_id" in sql:
                return [{"queue_id": 91, "queue_status": "RUNNING", "claimed_by_worker_id": "worker-a"}] if self.claimed else []
            return []

        def execute(self, sql, parameters=None):
            super().execute(sql, parameters)
            if "ctl_ingestion_queue" in sql and "claimed_by_worker_id = :worker_id" in sql:
                self.claimed = True

    repository = Repository()

    claimed = repository.claim_next_queue_item(worker_id="worker-a", lease_seconds=60)
    no_second_claim = repository.claim_next_queue_item(worker_id="worker-b", lease_seconds=60)

    assert claimed["queue_id"] == 91
    assert no_second_claim is None
    claim_sql = next(sql for sql, _ in repository.executed if "ctl_ingestion_queue" in sql)
    assert "attempt_count < max_attempts" in claim_sql
    assert "lease_expires_at <= CURRENT_TIMESTAMP()" in claim_sql
    assert "queue_status IN (:finalizing, :running)" in claim_sql
    running_reclaim = claim_sql.split("(attempt_count < max_attempts", 1)[0]
    assert "queue_status = :running" in running_reclaim


def test_full_enqueue_persists_exact_runtime_context_and_rejects_incremental() -> None:
    class Repository(StubMetadataRepository):
        def __init__(self):
            super().__init__()
            self.queue = None
            self.runtime_object = {
                "ingestion_object_id": 101,
                "config_version": 2,
                "config_hash": "object-hash",
                "processing_stage": "SOURCE_TO_BRONZE",
                "object_name": "ClaimsDB.dbo.Claims",
                "target_bronze_table": "main.bronze.bronze_claims",
                "load_type": "FULL",
                "active_flag": True,
                "is_current": True,
                "execution_spec_json": json.dumps({
                    "mapping_version": 3,
                    "mapping_hash": "mapping-hash",
                    "processing_stage": "SOURCE_TO_BRONZE",
                    "artifact_hash": "artifact-hash",
                        "runtime_context_contract_version": "1.0",
                        "idempotency_identity": "logical_work_id",
                        "source_resource": {
                            "database": "ClaimsDB",
                            "schema": "dbo",
                            "table": "Claims",
                        },
                    }),
            }
            self.runtime_mapping = {
                "mapping_version": 3,
                "mapping_hash": "mapping-hash",
            }

        def get_active_ingestion_object(self, ingestion_object_id):
            return dict(self.runtime_object) if int(ingestion_object_id) == 101 else None

        def get_active_mapping_reference(self, ingestion_object_id, processing_stage):
            return dict(self.runtime_mapping)

        def execute(self, sql, parameters=None):
            super().execute(sql, parameters)
            if "ctl_ingestion_queue" in sql and "MERGE INTO" in sql:
                self.queue = dict(parameters or {})

        def query(self, sql, parameters=None):
            if "ctl_ingestion_queue" in sql and "idempotency_key" in sql:
                return [dict(self.queue)] if self.queue else []
            return super().query(sql, parameters)

    repository = Repository()
    queued = repository.enqueue_work(
        ingestion_object_id=101,
        trigger_type="MANUAL",
        work_scope={"design_run_id": "design-1"},
        requested_by="tester",
        logical_work_id="logical-1",
    )
    runtime_context = json.loads(queued["work_scope_json"])["runtime_context"]

    assert runtime_context == {
        "contract_version": "1.0",
        "logical_work_id": "logical-1",
        "queue_id": queued["queue_id"],
        "ingestion_object_id": 101,
        "processing_stage": "SOURCE_TO_BRONZE",
        "load_type": "FULL",
        "source_object": "ClaimsDB.dbo.Claims",
        "target_table": "main.bronze.bronze_claims",
        "config_version": 2,
        "mapping_version": 3,
        "attempt_number": 0,
        "validation_policy_hash": None,
        "runtime_run_id": None,
    }

    repository.runtime_object["load_type"] = "INCREMENTAL"
    with pytest.raises(ValueError, match="FULL/stateless"):
        repository.enqueue_work(
            ingestion_object_id=101,
            trigger_type="MANUAL",
            work_scope={},
            requested_by="tester",
        )


def test_downstream_release_waits_for_every_pinned_dependency_and_deduplicates() -> None:
    class Repository(StubMetadataRepository):
        def __init__(self):
            super().__init__()
            self.succeeded = {101}
            self.enqueued = {}
            self.candidate = {
                "ingestion_object_id": 201,
                "processing_stage": "BRONZE_TO_SILVER",
                "target_table": "main.silver.claims",
                "dependency_objects_json": json.dumps({
                    "dependencies": [
                        {"ingestion_object_id": 101, "config_version": 1, "mapping_version": 1},
                        {"ingestion_object_id": 102, "config_version": 1, "mapping_version": 1},
                    ]
                }),
            }

        def query(self, sql, parameters=None):
            if "cfg_ingestion_object" in sql and "processing_stage = :processing_stage" in sql:
                return [dict(self.candidate)]
            if "ctl_run" in sql and "ingestion_object_config_version" in sql:
                dependency_id = int((parameters or {})["ingestion_object_id"])
                return [{"run_id": f"run-{dependency_id}"}] if dependency_id in self.succeeded else []
            return super().query(sql, parameters)

        def enqueue_work(self, **values):
            key = (int(values["ingestion_object_id"]), str(values["logical_work_id"]))
            self.enqueued.setdefault(key, {
                "queue_id": len(self.enqueued) + 1,
                "queue_status": "PENDING",
                "logical_work_id": key[1],
            })
            return dict(self.enqueued[key])

    repository = Repository()
    completed = {"ingestion_object_id": 101, "processing_stage": "SOURCE_TO_BRONZE"}

    assert repository.enqueue_ready_downstream(
        completed_object=completed,
        logical_work_id="logical-1",
        parent_work_scope={"design_run_id": "design-1"},
    ) == []

    repository.succeeded.add(102)
    first = repository.enqueue_ready_downstream(
        completed_object=completed,
        logical_work_id="logical-1",
        parent_work_scope={"design_run_id": "design-1"},
    )
    repository.enqueue_ready_downstream(
        completed_object=completed,
        logical_work_id="logical-1",
        parent_work_scope={"design_run_id": "design-1"},
    )

    assert first[0]["logical_work_id"] == "logical-1"
    assert len(repository.enqueued) == 1


@pytest.mark.parametrize("existing_status", ["RUNNING", "FAILED"])
def test_reclaimed_running_queue_resumes_the_same_runtime_attempt(existing_status) -> None:
    class Repository(StubMetadataRepository):
        def __init__(self):
            super().__init__()
            self.runtime_object = {
                "ingestion_object_id": 101,
                "config_version": 2,
                "config_hash": "object-hash",
                "processing_stage": "SOURCE_TO_BRONZE",
                "object_name": "ClaimsDB.dbo.Claims",
                "target_bronze_table": "main.bronze.bronze_claims",
                "execution_spec_json": json.dumps({
                    "mapping_version": 3,
                    "mapping_hash": "mapping-hash",
                    "processing_stage": "SOURCE_TO_BRONZE",
                    "artifact_hash": "artifact-hash",
                    "runtime_context_contract_version": "1.0",
                    "idempotency_identity": "logical_work_id",
                    "source_resource": {
                        "database": "ClaimsDB",
                        "schema": "dbo",
                        "table": "Claims",
                    },
                }),
            }
            self.runtime_mapping = {"mapping_version": 3, "mapping_hash": "mapping-hash"}
            self.existing = {
                "run_id": "runtime-1",
                "queue_id": 91,
                "attempt_number": 1,
                "logical_work_id": "logical-1",
                "ingestion_object_id": 101,
                "ingestion_object_config_version": 2,
                "mapping_version": 3,
                "status": existing_status,
            }

        def execute(self, sql, parameters=None):
            super().execute(sql, parameters)
            if "RESUME_PARTIAL_CONTROL_FINALIZATION" in str(parameters or {}):
                self.existing["status"] = "RUNNING"

        def get_ingestion_object(self, ingestion_object_id, config_version=None):
            return dict(self.runtime_object)

        def get_mapping_bundle(self, **_kwargs):
            return dict(self.runtime_mapping)

        def query(self, sql, parameters=None):
            if "claimed_by_worker_id" in sql and "attempt_count = :attempt_number" in sql:
                return [{"queue_id": 91}]
            if "ctl_run" in sql and "attempt_number = :attempt_number" in sql:
                return [dict(self.existing)]
            if "ctl_run" in sql and "run_id = :run_id AND status = :status" in sql:
                return [dict(self.existing)] if self.existing["status"] == parameters["status"] else []
            return super().query(sql, parameters)

    repository = Repository()
    snapshot = repository._runtime_snapshot(
        repository.runtime_object, repository.runtime_mapping
    )
    runtime_context = {
        "contract_version": "1.0",
        "logical_work_id": "logical-1",
        "queue_id": 91,
        "ingestion_object_id": 101,
        "processing_stage": "SOURCE_TO_BRONZE",
        "load_type": "FULL",
        "source_object": "ClaimsDB.dbo.Claims",
        "target_table": "main.bronze.bronze_claims",
        "config_version": 2,
        "mapping_version": 3,
        "attempt_number": 0,
    }
    queue_item = {
        "queue_id": 91,
        "ingestion_object_id": 101,
        "attempt_count": 1,
        "logical_work_id": "logical-1",
        "metadata_snapshot_id": snapshot["metadata_snapshot_id"],
        "work_scope_json": json.dumps({
            "_metadata_snapshot": snapshot["snapshot"],
            "runtime_context": runtime_context,
        }),
    }

    resumed = repository.create_run_attempt(
        queue_item, pipeline_name="metadata-worker", worker_id="worker-2"
    )

    assert resumed["resumed_attempt"] is True
    assert resumed["run"]["run_id"] == "runtime-1"
    assert resumed["runtime_context"]["attempt_number"] == 1
    assert not any("INSERT INTO" in sql and "ctl_run" in sql for sql, _ in repository.executed)
    if existing_status == "FAILED":
        assert any("RESUME_PARTIAL_CONTROL_FINALIZATION" in str(values) for _, values in repository.executed)


def test_runtime_snapshot_rejects_artifact_for_another_source_table() -> None:
    repository = StubMetadataRepository()
    obj = {
        "ingestion_object_id": 101,
        "config_version": 2,
        "config_hash": "object-hash",
        "processing_stage": "SOURCE_TO_BRONZE",
        "object_name": "ClaimsDB.dbo.Claims",
        "execution_spec_json": json.dumps({
            "mapping_version": 3,
            "mapping_hash": "mapping-hash",
            "processing_stage": "SOURCE_TO_BRONZE",
            "artifact_hash": "artifact-hash",
            "runtime_context_contract_version": "1.0",
            "idempotency_identity": "logical_work_id",
            "source_resource": {"database": "ClaimsDB", "schema": "dbo", "table": "Policies"},
        }),
    }

    with pytest.raises(ValueError, match="source resource"):
        repository._runtime_snapshot(obj, {"mapping_version": 3, "mapping_hash": "mapping-hash"})


def test_run_phase_update_is_fenced_by_current_queue_lease() -> None:
    class Repository(StubMetadataRepository):
        def query(self, sql, parameters=None):
            if "phase_status_json" in sql:
                return [{"phase_status_json": "{}"}]
            return super().query(sql, parameters)

    repository = Repository()
    repository.update_run_phase(
        "run-1", "TARGET_SUBMITTED", queue_id=91, worker_id="worker-a",
        target_commit_status="SUBMITTED",
    )

    sql, values = repository.executed[-1]
    assert "EXISTS (SELECT 1" in sql
    assert "claimed_by_worker_id = :worker_id" in sql
    assert "lease_expires_at > CURRENT_TIMESTAMP()" in sql
    assert values["queue_id"] == 91


def test_snowflake_enqueue_serializes_on_ingestion_object_in_one_transaction(monkeypatch) -> None:
    from services import snowflake_bronze_runtime

    calls = []

    class Cursor:
        def execute(self, sql, parameters=None):
            calls.append(("execute", sql, parameters))

    class Connection:
        def cursor(self):
            return Cursor()

        def commit(self):
            calls.append(("commit",))

        def rollback(self):
            calls.append(("rollback",))

        def close(self):
            calls.append(("close",))

    connection = Connection()
    repository = SnowflakeMetadataRepository(TargetMetadataContext("snowflake", "qa", "ATHENA"))
    monkeypatch.setattr(snowflake_bronze_runtime, "_snowflake_connect", lambda **_kwargs: connection)

    def base_enqueue(self, **kwargs):
        assert self._transaction_connection() is connection
        return {"queue_id": 7, **kwargs}

    monkeypatch.setattr(MetadataRepository, "enqueue_work", base_enqueue)

    result = repository.enqueue_work(ingestion_object_id=101, trigger_type="MANUAL")

    assert result["queue_id"] == 7
    assert "SET updated_at = updated_at" in calls[0][1]
    assert calls[0][2] == (101,)
    assert [call[0] for call in calls[-2:]] == ["commit", "close"]


def test_reviewed_artifact_creates_and_activates_a_new_executable_version(tmp_path, monkeypatch) -> None:
    from services.metadata_contracts import file_sha256
    from utilis.generated_code_paths import generated_artifact_uri

    monkeypatch.setenv("ATHENA_GENERATED_CODE_DIR", str(tmp_path))
    artifact = tmp_path / "bronze" / "claims.py"
    artifact.parent.mkdir()
    artifact.write_text("print('claims')\n", encoding="utf-8")
    repository = StubMetadataRepository()
    draft = repository.upsert_database_ingestion_object_draft(
        source_system_id=7,
        connection_id=11,
        table={"database_name": "ClaimsDB", "schema_name": "dbo", "table_name": "Claims"},
        target_bronze_table="main.bronze.bronze_Claims",
    )
    bundle = repository.upsert_source_to_bronze_mapping_draft(
        ingestion_object=draft,
        columns=[{"column_name": "ClaimID", "data_type": "int", "ordinal_position": 1}],
    )

    activated = repository.register_and_activate_source_to_bronze_artifact(
        draft_config_version=int(draft["config_version"]),
        ingestion_object_id=int(draft["ingestion_object_id"]),
        mapping_version=int(bundle["mapping_version"]),
        mapping_hash=str(bundle["mapping_hash"]),
        execution_spec={
            "contract_version": "1.0",
            "execution_mode": "GENERATED_ARTIFACT",
            "target_platform": "DATABRICKS",
            "engine": "DATABRICKS_JOB",
            "artifact_uri": generated_artifact_uri(artifact),
            "entry_point": "script",
            "artifact_hash": file_sha256(artifact),
            "generator_version": "test",
            "mapping_version": bundle["mapping_version"],
            "source_resource": {"database": "ClaimsDB", "schema": "dbo", "table": "Claims"},
        },
    )

    assert activated["ingestion_object"]["config_version"] % 2 == 0
    assert activated["ingestion_object"]["active_flag"] is True
    assert activated["mapping_bundle"]["active_flag"] is True
    assert json.loads(activated["ingestion_object"]["execution_spec_json"])["design_config_version"] == draft["config_version"]

    replacement = tmp_path / "bronze" / "claims_v2.py"
    replacement.write_text("print('claims v2')\n", encoding="utf-8")
    second_spec = {
        **activated["execution_spec"],
        "artifact_uri": generated_artifact_uri(replacement),
        "artifact_hash": file_sha256(replacement),
        "deployment_id": "second-design-run",
    }
    reactivated = repository.register_and_activate_source_to_bronze_artifact(
        draft_config_version=int(draft["config_version"]),
        ingestion_object_id=int(draft["ingestion_object_id"]),
        mapping_version=int(bundle["mapping_version"]),
        mapping_hash=str(bundle["mapping_hash"]),
        execution_spec=second_spec,
    )

    assert reactivated["ingestion_object"]["config_version"] != activated["ingestion_object"]["config_version"]
    assert reactivated["ingestion_object"]["config_version"] % 2 == 0


def test_metadata_selection_revalidates_pinned_connection_and_project_access(monkeypatch: pytest.MonkeyPatch) -> None:
    from services import metadata_selection

    connection = validate_jdbc_connection(
        {
            "source_system_id": 7,
            "connection_name": "claims-db",
            "host_name": "db.internal",
            "port": 1433,
            "database_name": "ClaimsDB",
            "auth_type": "BASIC",
            "secrets_json": {
                "username": {"scope": "DEPLOYMENT_ENV", "key": "AZURE_SQL_SOURCE_USERNAME"},
                "password": {"scope": "DEPLOYMENT_ENV", "key": "AZURE_SQL_SOURCE_PASSWORD"},
            },
            "config_json": {"allowed_project_ids": ["project-1"]},
        }
    )
    connection.update({"connection_id": 11, "config_version": 2, "active_flag": True, "is_current": True})

    class Repository:
        def preflight(self):
            return None

        def get_source_system(self, _source_id):
            return {"source_system_id": 7, "active_flag": True}

        def get_connection(self, _connection_id, _version):
            return dict(connection)

        def get_active_connection(self, _connection_id):
            raise AssertionError("Pinned selection must use the exact version")

    monkeypatch.setattr(metadata_selection, "metadata_repository_for_target", lambda **_: Repository())
    monkeypatch.setattr(metadata_selection, "validate_deployment_database_binding", lambda _row, **_kwargs: None)

    selected = metadata_selection.validated_metadata_selection(
        {
            "target_warehouse": "databricks",
            "target_environment": "qa",
            "source_system_id": 7,
            "source_connection_id": 11,
            "source_connection_config_version": 2,
            "source_connection_config_hash": connection["config_hash"],
            "project_id": "project-1",
        }
    )

    assert selected is not None
    assert selected.connection["config_version"] == 2
