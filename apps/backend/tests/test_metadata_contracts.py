from __future__ import annotations

import json

import pytest

from services.metadata_contracts import (
    METADATA_TABLES,
    TargetMetadataContext,
    expected_columns,
    render_ddl,
    stable_bigint,
    validate_jdbc_connection,
    validate_execution_result,
    validate_runtime_context,
    validate_schema_columns,
    validate_secret_references,
)


def test_full_runtime_context_and_execution_result_are_work_identity_pinned() -> None:
    context = validate_runtime_context({
        "contract_version": "1.0",
        "logical_work_id": "work-123",
        "queue_id": 10,
        "ingestion_object_id": 20,
        "processing_stage": "SOURCE_TO_BRONZE",
        "load_type": "FULL",
        "target_table": "main.bronze.claims",
        "config_version": 2,
        "mapping_version": 3,
        "runtime_run_id": "runtime-1",
    })
    result = validate_execution_result(
        {
            "contract_version": "1.0",
            "status": "COMPLETED",
            "logical_work_id": "work-123",
            "runtime_run_id": "runtime-1",
            "target_table": "main.bronze.claims",
            "target_commit_id": "target-run-456",
            "validation_status": "PASSED",
        },
        runtime_context=context,
    )

    assert result["target_commit_id"] == "target-run-456"
    with pytest.raises(ValueError, match="FULL loads only"):
        validate_runtime_context({**context, "load_type": "INCREMENTAL"})
    with pytest.raises(ValueError, match="does not match"):
        validate_execution_result(
            {**result, "logical_work_id": "other-work"}, runtime_context=context
        )


def test_execution_spec_requires_allowlisted_platform_artifact(tmp_path, monkeypatch) -> None:
    from services.metadata_contracts import file_sha256, validate_execution_spec
    from utilis.generated_code_paths import (
        generated_artifact_uri,
        resolve_generated_artifact_uri,
        verified_execution_artifact,
    )

    monkeypatch.setenv("ATHENA_GENERATED_CODE_DIR", str(tmp_path))
    artifact = tmp_path / "bronze_claims.py"
    artifact.write_text("print('ok')\n", encoding="utf-8")
    uri = generated_artifact_uri(artifact)
    spec = validate_execution_spec(
        {
            "contract_version": "1.0",
            "execution_mode": "GENERATED_ARTIFACT",
            "target_platform": "DATABRICKS",
            "engine": "DATABRICKS_JOB",
            "artifact_uri": uri,
            "entry_point": "script",
            "artifact_hash": file_sha256(artifact),
            "generator_version": "test",
            "mapping_version": 1,
        },
        platform="databricks",
    )

    assert resolve_generated_artifact_uri(spec["artifact_uri"]) == artifact.resolve()
    assert verified_execution_artifact(spec, platform="databricks") == artifact.resolve()
    artifact.write_text("print('tampered')\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="SHA-256"):
        verified_execution_artifact(spec, platform="databricks")
    with pytest.raises(ValueError, match="escapes"):
        resolve_generated_artifact_uri("generated-code://../secret.py")


def test_platform_ddls_preserve_exact_eight_table_contract() -> None:
    databricks = expected_columns("databricks")
    snowflake = expected_columns("snowflake")

    assert set(databricks) == set(METADATA_TABLES)
    assert snowflake == databricks
    assert "execution_spec_json" in databricks["cfg_ingestion_object"]
    assert "logical_work_id" in databricks["ctl_ingestion_queue"]
    assert "watermark_version" in databricks["ctl_watermark"]


def test_target_context_rejects_unsupported_or_unsafe_targets() -> None:
    with pytest.raises(ValueError, match="Unsupported metadata target"):
        TargetMetadataContext("fabric", "qa", "warehouse")
    with pytest.raises(ValueError, match="Invalid namespace"):
        TargetMetadataContext("databricks", "qa", "catalog; DROP TABLE x")
    with pytest.raises(ValueError, match="authoritative metadata schema"):
        TargetMetadataContext("databricks", "qa", "catalog", "custom")


def test_render_ddl_uses_validated_target_namespace() -> None:
    context = TargetMetadataContext("snowflake", "qa", "ATHENA_QA")
    sql = render_ddl(context)

    assert "__TARGET_DATABASE__" not in sql
    assert "ATHENA_QA.METADATA.CFG_SOURCE_SYSTEM" in sql


def test_stable_bigint_is_deterministic_and_namespaced() -> None:
    first = stable_bigint("connection", 10, "Claims")

    assert first == stable_bigint("connection", 10, "claims")
    assert first != stable_bigint("source_system", 10, "claims")
    assert 0 < first < 2**63


def test_secret_contract_accepts_references_and_rejects_values() -> None:
    refs = validate_secret_references(
        {"username": {"scope": "source-secrets", "key": "claims-user"}}
    )
    assert refs["username"]["key"] == "claims-user"

    with pytest.raises(ValueError, match="secret value field"):
        validate_secret_references(
            {"password": {"scope": "source-secrets", "key": "claims-password", "value": "bad"}}
        )
    with pytest.raises(ValueError, match="at least one"):
        validate_secret_references({})


def test_jdbc_contract_hash_is_canonical_and_contains_no_secret_values() -> None:
    payload = {
        "source_system_id": 42,
        "connection_name": "claims-db",
        "host_name": "db.internal",
        "port": 1433,
        "database_name": "claims",
        "auth_type": "BASIC",
        "secrets_json": {
            "username": {"scope": "source-secrets", "key": "claims-user"},
            "password": {"scope": "source-secrets", "key": "claims-password"},
        },
        "config_json": {
            "query_timeout_seconds": 60,
            "fetch_size": 1000,
            "allowed_project_ids": ["project-claims"],
        },
    }
    reversed_payload = dict(payload)
    reversed_payload["config_json"] = {
        "allowed_project_ids": ["project-claims"],
        "fetch_size": 1000,
        "query_timeout_seconds": 60,
    }

    first = validate_jdbc_connection(payload)
    second = validate_jdbc_connection(reversed_payload)

    assert first["config_hash"] == second["config_hash"]
    assert json.loads(first["secrets_json"])["password"] == {
        "key": "claims-password",
        "scope": "source-secrets",
    }
    assert first["secret_scope"] is None
    assert first["secret_key"] is None


def test_jdbc_contract_rejects_credentials_in_config_json() -> None:
    payload = {
        "source_system_id": 42,
        "connection_name": "claims-db",
        "host_name": "db.internal",
        "port": 1433,
        "database_name": "claims",
        "auth_type": "BASIC",
        "secrets_json": {
            "username": {"scope": "source-secrets", "key": "claims-user"},
            "password": {"scope": "source-secrets", "key": "claims-password"},
        },
        "config_json": {"nested": {"password": "cleartext"}},
    }

    with pytest.raises(ValueError, match="forbidden credential field"):
        validate_jdbc_connection(payload)

    payload["config_json"] = {"made_up_option": True}
    with pytest.raises(ValueError, match="Unsupported JDBC config_json fields"):
        validate_jdbc_connection(payload)

    payload["config_json"] = {
        "allowed_project_ids": ["project-claims"],
        "jdbc_url_template": "jdbc:sqlserver://db;user=admin;password=DUMMY_SECRET",
    }
    with pytest.raises(ValueError, match="must not contain credentials"):
        validate_jdbc_connection(payload)


def test_schema_preflight_reports_drift() -> None:
    actual = expected_columns()
    actual["cfg_ingestion_object"] = actual["cfg_ingestion_object"] - {"execution_spec_json"}

    with pytest.raises(RuntimeError, match="execution_spec_json"):
        validate_schema_columns(actual)
