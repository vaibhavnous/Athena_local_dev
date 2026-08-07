from __future__ import annotations

import json
import base64
import re
import uuid
from pathlib import Path

import pytest

from nodes import bronze_gen


def test_snowflake_bronze_script_uses_sql_patterns():
    script = bronze_gen.generate_snowflake_bronze_script(
        table="Claims",
        schema="dbo",
        database="insurance",
        run_id="run-1",
        bronze_catalog="ATHENA_DB",
        bronze_schema="BRONZE",
        table_metadata={
            "columns": [
                {"column_name": "ClaimID", "data_type": "int"},
                {"column_name": "ClaimDate", "data_type": "datetime2"},
                {"column_name": "Amount", "data_type": "decimal", "numeric_precision": 12, "numeric_scale": 2},
            ]
        },
    )

    assert "Expected runtime: Snowflake SQL" in script
    assert 'CREATE SCHEMA IF NOT EXISTS "ATHENA_DB"."BRONZE";' in script
    assert 'CREATE TABLE IF NOT EXISTS "ATHENA_DB"."BRONZE"."bronze_Claims"' in script
    assert 'TRY_CAST(src."ClaimID" AS NUMBER(38,0)) AS "claimid"' in script
    assert 'TRY_CAST(src."ClaimDate" AS TIMESTAMP_NTZ) AS "claimdate"' in script
    assert 'TRY_CAST(src."Amount" AS NUMBER(12,2)) AS "amount"' in script
    assert 'INSERT INTO "ATHENA_DB"."BRONZE"."bronze_Claims"' in script


def test_metadata_snowflake_bronze_replaces_one_logical_work_atomically():
    script = bronze_gen.generate_snowflake_bronze_script(
        table="Claims",
        schema="dbo",
        database="insurance",
        bronze_catalog="ATHENA_DB",
        bronze_schema="BRONZE",
        table_metadata={"columns": [{"column_name": "ClaimID", "data_type": "int"}]},
        metadata_driven=True,
    )

    assert '"_logical_work_id" VARCHAR' in script
    assert '$ATHENA_RUNTIME_RUN_ID AS "run_id"' in script
    assert '$ATHENA_LOGICAL_WORK_ID AS "_logical_work_id"' in script
    assert 'DELETE FROM "ATHENA_DB"."BRONZE"."bronze_Claims" WHERE "_logical_work_id" = $ATHENA_LOGICAL_WORK_ID;' in script
    assert "BEGIN TRANSACTION;" in script
    assert script.rstrip().endswith("COMMIT;")

    bronze_gen.validate_snowflake_bronze_sql(
        script,
        source_table='"insurance"."dbo"."Claims"',
        target_table='"ATHENA_DB"."BRONZE"."bronze_Claims"',
        metadata_driven=True,
    )


def test_metadata_snowflake_validator_rejects_legacy_run_scoped_artifact():
    legacy = bronze_gen.generate_snowflake_bronze_script(
        table="Claims",
        schema="dbo",
        database="insurance",
        bronze_catalog="ATHENA_DB",
        bronze_schema="BRONZE",
        table_metadata={"columns": [{"column_name": "ClaimID", "data_type": "int"}]},
    )

    with pytest.raises(ValueError, match="transaction/idempotency contract"):
        bronze_gen.validate_snowflake_bronze_sql(
            legacy,
            source_table='"insurance"."dbo"."Claims"',
            target_table='"ATHENA_DB"."BRONZE"."bronze_Claims"',
            metadata_driven=True,
        )


def test_metadata_snowflake_validator_does_not_accept_contract_tokens_in_comments():
    legacy = bronze_gen.generate_snowflake_bronze_script(
        table="Claims",
        schema="dbo",
        database="insurance",
        bronze_catalog="ATHENA_DB",
        bronze_schema="BRONZE",
        table_metadata={"columns": [{"column_name": "ClaimID", "data_type": "int"}]},
    )
    bypass = legacy + """
-- BEGIN TRANSACTION; COMMIT; "_logical_work_id" $ATHENA_LOGICAL_WORK_ID $ATHENA_RUNTIME_RUN_ID
/* DELETE FROM "ATHENA_DB"."BRONZE"."bronze_Claims"
   WHERE "_logical_work_id" = $ATHENA_LOGICAL_WORK_ID; */
"""

    with pytest.raises(ValueError, match="transaction/idempotency contract"):
        bronze_gen.validate_snowflake_bronze_sql(
            bypass,
            source_table='"insurance"."dbo"."Claims"',
            target_table='"ATHENA_DB"."BRONZE"."bronze_Claims"',
            metadata_driven=True,
        )


def test_metadata_snowflake_generation_preserves_runtime_contract_and_skips_llm(monkeypatch):
    monkeypatch.setenv("ATHENA_ENABLE_LLM_SNOWFLAKE_BRONZE_ENHANCEMENT", "true")
    monkeypatch.setattr(
        bronze_gen,
        "_enhance_snowflake_with_llm",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("LLM must not run")),
    )

    result = bronze_gen._generate_one_table(
        {"database_name": "insurance", "schema_name": "dbo", "table_name": "claims"},
        run_id="run-metadata-snowflake",
        bronze_catalog="ATHENA_DB",
        bronze_schema="BRONZE",
        target_warehouse="snowflake",
        table_metadata={"columns": [{"column_name": "ClaimID", "data_type": "int"}]},
        metadata_driven=True,
    )
    script = Path(result["script_path"]).read_text(encoding="utf-8")

    assert result["llm_enhanced"] is False
    assert result["snowflake_landing_database"] == "ATHENA_DB"
    assert result["snowflake_landing_schema"]
    assert result["snowflake_landing_table"] == "raw_claims"
    assert (
        f'FROM "ATHENA_DB"."{result["snowflake_landing_schema"]}"."raw_claims" AS src;'
        in script
    )
    assert '"_logical_work_id" VARCHAR' in script
    assert '$ATHENA_RUNTIME_RUN_ID AS "run_id"' in script
    assert '$ATHENA_LOGICAL_WORK_ID AS "_logical_work_id"' in script


def test_snowflake_bronze_execution_spec_pins_source_and_landing_resources():
    from services import pipeline_runtime

    result = bronze_gen._generate_one_table(
        {"database_name": "insurance", "schema_name": "dbo", "table_name": "claims"},
        run_id="run-pinned-snowflake",
        bronze_catalog="ATHENA_DB",
        bronze_schema="BRONZE",
        target_warehouse="snowflake",
        table_metadata={"columns": [{"column_name": "ClaimID", "data_type": "int"}]},
        metadata_driven=True,
    )
    attached = pipeline_runtime._attach_bronze_execution_specs({
        "run_id": "run-pinned-snowflake",
        "target_warehouse": "snowflake",
        "source_connection_id": 11,
        "source_connection_config_version": 2,
        "source_connection_config_hash": "connection-hash",
        "bronze_generation_results": [result],
        "certified_tables": [{
            "database_name": "insurance",
            "schema_name": "dbo",
            "table_name": "claims",
            "ingestion_object_id": 101,
            "ingestion_object_config_version": 2,
            "ingestion_object_config_hash": "object-hash",
            "source_to_bronze_mapping_version": 3,
            "source_to_bronze_mapping_hash": "mapping-hash",
        }],
    })
    spec = attached["bronze_generation_results"][0]["execution_spec"]

    assert spec["source_resource"] == {
        "database": "insurance", "schema": "dbo", "table": "claims"
    }
    assert spec["landing_resource"] == {
        "database": result["snowflake_landing_database"],
        "schema": result["snowflake_landing_schema"],
        "table": "raw_claims",
    }


def test_databricks_bronze_script_uses_catalog_qualified_target():
    script = bronze_gen.generate_bronze_script(
        table="Claims",
        schema="dbo",
        database="insurance",
        run_id="run-1",
        bronze_catalog="workspace",
        bronze_schema="bronze",
        source_jdbc_url="jdbc:sqlserver://example",
        cast_rules={"claimid": "int"},
    )

    assert 'spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")' in script
    assert 'TARGET_TABLE = "workspace.bronze.bronze_Claims"' in script
    assert "try_cast(`" in script


def test_databricks_bronze_script_keeps_security_optional():
    script = bronze_gen.generate_bronze_script(
        table="Claims",
        schema="dbo",
        database="insurance",
        run_id="run-1",
        bronze_catalog="workspace",
        bronze_schema="bronze",
        source_jdbc_url="jdbc:sqlserver://example",
    )

    assert "from security_control import" not in script
    assert "apply_security_controls(" not in script


def test_databricks_bronze_script_can_embed_security_controls():
    script = bronze_gen.generate_bronze_script(
        assessment_id="assessment-1",
        policies={"ClaimID": "Hash", "Email": "Mask"},
        table="Claims",
        schema="dbo",
        database="insurance",
        run_id="run-1",
        bronze_catalog="workspace",
        bronze_schema="bronze",
        source_jdbc_url="jdbc:sqlserver://example",
    )

    assert "from security_control import apply_security_controls, SecurityControlType" in script
    assert "SECURITY_ASSESSMENT_ID = 'assessment-1'" in script
    assert "'claimid': 'Hash'" in script
    assert "'email': 'Mask'" in script
    assert "df = apply_security_controls(" in script


def test_databricks_bronze_script_can_use_adls_landing_path():
    script = bronze_gen.generate_bronze_script(
        table="Claims",
        schema="dbo",
        database="insurance",
        run_id="run-1",
        bronze_catalog="workspace",
        bronze_schema="bronze",
        landing_path="abfss://raw@acct.dfs.core.windows.net/vendor/claims/",
        file_format="csv",
        source_type="adls_gen2",
        cast_rules={"claimid": "int"},
    )

    assert 'SOURCE_PATH = \'abfss://raw@acct.dfs.core.windows.net/vendor/claims/\'' in script
    assert 'FILE_FORMAT = \'csv\'' in script
    assert 'spark.read.format("csv")' in script
    assert '.option("dbtable",' not in script
    assert 'source_system", lit("adls_gen2")' in script


def test_databricks_runtime_prefers_script_body_for_plan_artifacts():
    from services import databricks_runtime

    script = databricks_runtime._read_script_text(
        {
            "script_path": "generated_code/bronze/run_feed_bronze_plan.json",
            "script_body": "print('real script')",
        }
    )
    name = databricks_runtime._script_name(
        {
            "script_path": "generated_code/bronze/run_feed_bronze_plan.json",
            "script_body": "print('real script')",
            "target_table": "workspace.bronze.vendor1_transactions_raw",
        }
    )

    assert script == "print('real script')"
    assert name == "workspace_bronze_vendor1_transactions_raw"


def test_databricks_layers_default_to_batch_execution(monkeypatch):
    from services import databricks_runtime

    monkeypatch.delenv("ATHENA_DATABRICKS_BRONZE_EXECUTION_MODE", raising=False)
    monkeypatch.delenv("ATHENA_DATABRICKS_SILVER_EXECUTION_MODE", raising=False)
    monkeypatch.delenv("ATHENA_DATABRICKS_GOLD_EXECUTION_MODE", raising=False)
    monkeypatch.delenv("ATHENA_DATABRICKS_EXECUTION_MODE", raising=False)

    assert databricks_runtime._databricks_execution_mode("bronze") == "batch"
    assert databricks_runtime._databricks_execution_mode("silver") == "batch"
    assert databricks_runtime._databricks_execution_mode("gold") == "batch"


def test_databricks_batch_driver_keeps_separate_script_targets(monkeypatch):
    from services import databricks_runtime

    monkeypatch.delenv("ATHENA_DATABRICKS_CONTINUE_ON_ERROR", raising=False)
    notebook = databricks_runtime._build_batch_driver_notebook(
        "bronze",
        [
            {
                "target_table": "workspace.bronze.customer_raw",
                "script_body": 'spark.sql("CREATE TABLE IF NOT EXISTS workspace.bronze.customer_raw(id INT)")',
            },
            {
                "target_table": "workspace.bronze.orders_raw",
                "script_body": 'spark.sql("CREATE TABLE IF NOT EXISTS workspace.bronze.orders_raw(id INT)")',
            },
        ],
        workspace_dir="/Workspace/athena/run-1",
    )

    encoded = re.search(r'b64decode\("([^"]+)"\)', notebook).group(1)
    payload = json.loads(base64.b64decode(encoded).decode("utf-8"))

    assert [item["target_table"] for item in payload] == [
        "workspace.bronze.customer_raw",
        "workspace.bronze.orders_raw",
    ]
    assert "dbutils.notebook.exit" in notebook
    assert "exec(compile" in notebook


def test_databricks_batch_driver_injects_metadata_runtime_context():
    from services import databricks_runtime

    runtime_context = {
        "contract_version": "1.0",
        "logical_work_id": "logical-1",
        "queue_id": 91,
        "runtime_run_id": "runtime-1",
    }
    notebook = databricks_runtime._build_batch_driver_notebook(
        "bronze",
        [{"target_table": "main.bronze.claims", "script_body": "print('claims')"}],
        workspace_dir="/Workspace/athena/runtime-1",
        runtime_context=runtime_context,
    )
    encoded = re.search(
        r'_RUNTIME_CONTEXT = json.loads\(base64\.b64decode\("([^"]+)"\)', notebook
    ).group(1)

    assert json.loads(base64.b64decode(encoded).decode("utf-8")) == runtime_context
    assert '_script_globals["ATHENA_RUNTIME_CONTEXT"] = dict(_ITEM_RUNTIME_CONTEXT)' in notebook


def test_databricks_metadata_batch_embeds_one_runtime_context_per_script():
    from services import databricks_runtime

    notebook = databricks_runtime._build_batch_driver_notebook(
        "bronze",
        [
            {
                "target_table": "main.bronze.claims",
                "script_body": "print('claims')",
                "metadata_runtime_context": {"queue_id": 1, "runtime_run_id": "run-1"},
            },
            {
                "target_table": "main.bronze.policy",
                "script_body": "print('policy')",
                "metadata_runtime_context": {"queue_id": 2, "runtime_run_id": "run-2"},
            },
        ],
        workspace_dir="/Workspace/athena/design-1",
        metadata_runtime_batch=True,
    )
    encoded = re.search(
        r'_SCRIPT_ITEMS = json.loads\(base64\.b64decode\("([^"]+)"\)', notebook
    ).group(1)
    items = json.loads(base64.b64decode(encoded).decode("utf-8"))

    assert [item["runtime_context"]["runtime_run_id"] for item in items] == ["run-1", "run-2"]
    assert "_CONTINUE_ON_ERROR = True" in notebook
    assert "_METADATA_RUNTIME_BATCH = True" in notebook
    assert "if not _METADATA_RUNTIME_BATCH:" in notebook
    assert 'spark.databricks.delta.commitInfo.userMetadata' not in notebook
    assert '_version_after != _version_before + 1' in notebook
    assert '"target_commit_id": f"delta:{_target}:v{_history[\'version\']}"' in notebook
    assert '_result["execution_result"] = _execution_result' in notebook


def test_databricks_submit_uses_stable_platform_idempotency_token(monkeypatch):
    from services import databricks_runtime

    captured = {}
    monkeypatch.setattr(databricks_runtime, "_cluster_spec", lambda: {"existing_cluster_id": "cluster-1"})
    monkeypatch.setattr(
        databricks_runtime,
        "_request_json",
        lambda method, path, payload: captured.update(
            {"method": method, "path": path, "payload": payload}
        ) or {"run_id": 42},
    )

    result = databricks_runtime._submit_run(
        "/Workspace/athena/runtime-1",
        run_name="metadata runtime",
        idempotency_token="athena-metadata-91",
    )

    assert result["run_id"] == 42
    assert captured["payload"]["idempotency_token"] == "athena-metadata-91"


def test_databricks_submit_retries_ambiguous_response_with_same_token(monkeypatch):
    from services import databricks_runtime

    attempts = []

    def request_json(_method, _path, payload):
        attempts.append(dict(payload))
        if len(attempts) < 3:
            raise databricks_runtime.DatabricksTransientError("response lost")
        return {"run_id": 42}

    monkeypatch.setattr(databricks_runtime, "_request_json", request_json)
    monkeypatch.setattr(databricks_runtime.time, "sleep", lambda _seconds: None)

    assert databricks_runtime._submit_run(
        "/Shared/job", run_name="metadata", idempotency_token="athena-metadata-91-2"
    ) == {"run_id": 42}
    assert [item["idempotency_token"] for item in attempts] == [
        "athena-metadata-91-2", "athena-metadata-91-2", "athena-metadata-91-2"
    ]


def test_metadata_databricks_batch_uses_queue_attempt_token_and_returns_receipt(monkeypatch):
    from services import databricks_runtime

    submitted = {}
    receipts = []
    monkeypatch.setattr(databricks_runtime, "_upload_support_files", lambda *_args: None)
    monkeypatch.setattr(databricks_runtime, "_workspace_import_notebook", lambda *_args: {})
    monkeypatch.setattr(
        databricks_runtime,
        "_submit_run",
        lambda _path, **kwargs: submitted.update(kwargs) or {"run_id": 42},
    )
    monkeypatch.setattr(
        databricks_runtime,
        "_wait_for_run",
        lambda _run_id: {
            "run_id": 42,
            "result_state": "SUCCESS",
            "life_cycle_state": "TERMINATED",
        },
    )
    monkeypatch.setattr(databricks_runtime, "_task_run_id", lambda _state: 42)
    monkeypatch.setattr(
        databricks_runtime,
        "_get_run_output",
        lambda _run_id: {"notebook_output": {"result": json.dumps({
            "status": "COMPLETED",
            "results": [{
                "script_name": "bronze_claims",
                "target_table": "main.bronze.claims",
                "status": "SUCCESS",
                "verification_status": "VERIFIED",
            }],
        })}},
    )
    monkeypatch.setattr(
        databricks_runtime,
        "save_external_execution_progress",
        lambda state, **_kwargs: state,
    )

    result = databricks_runtime._execute_databricks_stage_batch(
        {
            "run_id": "runtime-1",
            "metadata_runtime_context": {
                "queue_id": 91,
                "attempt_number": 2,
                "logical_work_id": "logical-1",
            },
        },
        layer="bronze",
        scripts=[{"target_table": "main.bronze.claims", "script_body": "print('claims')"}],
        on_submitted=receipts.append,
    )

    assert submitted["idempotency_token"] == "athena-metadata-91-2"
    assert receipts == ["42"]
    assert result["databricks_bronze_execution_results"][0]["databricks_run_id"] == 42


def test_databricks_task_run_id_expands_parent_submit_run(monkeypatch):
    from services import databricks_runtime

    monkeypatch.setattr(
        databricks_runtime,
        "_request_json",
        lambda method, path: {"tasks": [{"run_id": 456}]} if "run_id=123" in path else {},
    )

    assert databricks_runtime._task_run_id({"run_id": 123}) == 456


def test_metadata_databricks_submission_receipt_failure_preserves_attempt(monkeypatch):
    from services import databricks_runtime

    waited = []
    monkeypatch.setattr(databricks_runtime, "_upload_support_files", lambda *_args: None)
    monkeypatch.setattr(databricks_runtime, "_workspace_import_notebook", lambda *_args: {})
    monkeypatch.setattr(databricks_runtime, "_submit_run", lambda *_args, **_kwargs: {"run_id": 42})
    monkeypatch.setattr(databricks_runtime, "_wait_for_run", lambda run_id: waited.append(run_id))
    monkeypatch.setattr(databricks_runtime, "save_external_execution_progress", lambda state, **_: state)

    with pytest.raises(databricks_runtime.DatabricksAmbiguousSubmissionError) as raised:
        databricks_runtime._execute_databricks_stage_batch(
            {
                "run_id": "runtime-1",
                "metadata_runtime_context": {
                    "queue_id": 91,
                    "attempt_number": 2,
                    "logical_work_id": "logical-1",
                },
            },
            layer="bronze",
            scripts=[{"target_table": "main.bronze.claims", "script_body": "print('claims')"}],
            on_submitted=lambda _run_id: (_ for _ in ()).throw(ConnectionError("control unavailable")),
        )

    assert raised.value.preserve_attempt is True
    assert "Databricks run 42" in str(raised.value)
    assert waited == []


def test_file_bronze_generation_uses_tolerant_databricks_casts():
    from sftp_nodes import bronze_code_generation

    script = bronze_code_generation._generate_script(
        {
            "source_type": "adls_gen2",
            "source_feed": "Vendor.Feed",
            "vendor": "vendor",
            "entity": "feed",
            "file_format": "csv",
            "landing_path": "abfss://raw@example.dfs.core.windows.net/vendor/feed/",
            "target_table": "workspace.bronze.bronze_feed",
            "schema_location": "/tmp/schema",
            "checkpoint_path": "/tmp/checkpoint",
            "expected_columns": ["paiddate", "paidamount"],
            "expected_types": {"paiddate": "timestamp", "paidamount": "double"},
        },
        run_id="run-1",
        pipeline_version="v1",
    )

    assert "try_cast(`{escaped_name}` AS {target_type})" in script


def test_snowflake_bronze_generation_writes_sql_without_databricks_path(monkeypatch):
    monkeypatch.setenv("ATHENA_ENABLE_LLM_SNOWFLAKE_BRONZE_ENHANCEMENT", "false")
    monkeypatch.setenv("ATHENA_SNOWFLAKE_BRONZE_TABLE_ALLOWLIST", "*")
    monkeypatch.setenv("SNOWFLAKE_BRONZE_CATALOG", "ATHENA_DB")
    monkeypatch.setenv("SNOWFLAKE_BRONZE_SCHEMA", "BRONZE")
    workdir = Path.cwd() / ".tmp-tests" / f"bronze_{uuid.uuid4().hex}"
    workdir.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(workdir)
    monkeypatch.setattr(
        bronze_gen,
        "build_source_jdbc_url",
        lambda database_name=None: (_ for _ in ()).throw(AssertionError("Databricks JDBC path should not run")),
    )

    state = {
        "run_id": "run-snowflake",
        "target_warehouse": "snowflake",
        "bronze_catalog": "ATHENA_DB",
        "bronze_schema": "BRONZE",
        "certified_tables": [
            {"database_name": "insurance", "schema_name": "dbo", "table_name": "Claims"}
        ],
        "discovered_metadata": {
            "tables": [
                {
                    "table_name": "Claims",
                    "columns": [
                        {"column_name": "ClaimID", "data_type": "int"},
                    ],
                }
            ]
        },
    }

    result = bronze_gen.bronze_code_generation_node(state)

    script_path = Path(result["bronze_generation_results"][0]["script_path"])
    bundle_path = Path(result["bronze_generation_bundle_path"])
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))

    assert result["bronze_generation_status"] == "COMPLETED"
    assert result["bronze_generation_results"][0]["target_warehouse"] == "snowflake"
    assert result["bronze_generation_results"][0]["script_language"] == "sql"
    assert result["bronze_generation_results"][0]["source_table"] == "insurance.dbo.Claims"
    assert result["bronze_generation_results"][0]["target_table"] == "ATHENA_DB.BRONZE.bronze_Claims"
    assert script_path.suffix == ".sql"
    assert script_path.parts[-3:] == ("snowflake", "bronze", script_path.name)
    assert "Expected runtime: Snowflake SQL" in script_path.read_text(encoding="utf-8")
    assert bundle["target_warehouse"] == "snowflake"


def test_bronze_generation_copies_security_helper_when_enabled(monkeypatch):
    monkeypatch.setenv("ATHENA_ENABLE_LLM_BRONZE_ENHANCEMENT", "false")
    workdir = Path.cwd() / ".tmp-tests" / f"bronze_security_{uuid.uuid4().hex}"
    workdir.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(workdir)
    copied = []

    monkeypatch.setattr(bronze_gen, "build_source_jdbc_url", lambda database_name=None: "jdbc:sqlserver://example")
    monkeypatch.setattr(bronze_gen, "copy_security_control_module", lambda output_dir: copied.append(output_dir) or str(Path(output_dir) / "security_control.py"))

    state = {
        "run_id": "run-security",
        "target_warehouse": "databricks",
        "bronze_catalog": "workspace",
        "bronze_schema": "bronze",
        "compliance_assessment_id": "assessment-1",
        "security_policies": {
            "Claims": {
                "ClaimID": "Hash",
                "Email": "Mask",
            }
        },
        "certified_tables": [
            {"database_name": "insurance", "schema_name": "dbo", "table_name": "Claims"}
        ],
    }

    result = bronze_gen.bronze_code_generation_node(state)
    script_path = Path(result["bronze_generation_results"][0]["script_path"])
    script = script_path.read_text(encoding="utf-8")

    assert copied
    assert result["bronze_generation_results"][0]["security_enabled"] is True
    assert result["bronze_generation_results"][0]["assessment_id"] == "assessment-1"
    assert result["bronze_generation_results"][0]["security_policy_columns"] == ["claimid", "email"]
    assert "apply_security_controls(" in script


def test_bronze_generation_uses_adls_landing_path_without_jdbc(monkeypatch):
    monkeypatch.setenv("ATHENA_ENABLE_LLM_BRONZE_ENHANCEMENT", "false")
    workdir = Path.cwd() / ".tmp-tests" / f"bronze_adls_{uuid.uuid4().hex}"
    workdir.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(workdir)
    monkeypatch.setattr(
        bronze_gen,
        "build_source_jdbc_url",
        lambda database_name=None: (_ for _ in ()).throw(AssertionError("JDBC path should not run for ADLS bronze")),
    )

    state = {
        "run_id": "run-adls",
        "target_warehouse": "databricks",
        "source": "adls_gen2",
        "bronze_catalog": "workspace",
        "bronze_schema": "bronze",
        "candidate_feed": {
            "entity": "Claims",
            "source": "adls_gen2",
            "landing_path": "abfss://raw@acct.dfs.core.windows.net/vendor/claims/",
            "file_format": "json",
        },
        "certified_tables": [
            {"database_name": "insurance", "schema_name": "dbo", "table_name": "Claims"}
        ],
    }

    result = bronze_gen.bronze_code_generation_node(state)
    script_path = Path(result["bronze_generation_results"][0]["script_path"])
    script = script_path.read_text(encoding="utf-8")

    assert result["bronze_generation_status"] == "COMPLETED"
    assert 'SOURCE_PATH = \'abfss://raw@acct.dfs.core.windows.net/vendor/claims/\'' in script
    assert 'FILE_FORMAT = \'json\'' in script
    assert 'spark.read.format("json").load(SOURCE_PATH)' in script


def test_bronze_generation_avoids_case_only_duplicate_source_tables(monkeypatch):
    monkeypatch.setenv("ATHENA_ENABLE_LLM_SNOWFLAKE_BRONZE_ENHANCEMENT", "false")
    monkeypatch.setenv("ATHENA_SNOWFLAKE_BRONZE_TABLE_ALLOWLIST", "*")
    workdir = Path.cwd() / ".tmp-tests" / f"bronze_case_{uuid.uuid4().hex}"
    workdir.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(workdir)

    state = {
        "run_id": "run-case",
        "target_warehouse": "snowflake",
        "bronze_catalog": "ATHENA_DB",
        "bronze_schema": "BRONZE",
        "certified_tables": [
            {"database_name": "insurance", "schema_name": "dbo", "table_name": "policy_cover_level_transactions_dup_del"},
            {"database_name": "insurance", "schema_name": "dbo", "table_name": "policy_cover_level_transactions_Dup_Del"},
        ],
        "discovered_metadata": {
            "tables": [
                {
                    "table_name": "policy_cover_level_transactions_dup_del",
                    "columns": [{"column_name": "PolicyID", "data_type": "int"}],
                }
            ]
        },
    }

    result = bronze_gen.bronze_code_generation_node(state)
    scripts = result["bronze_generation_results"]

    assert result["bronze_generation_status"] == "COMPLETED"
    assert [item["table"] for item in scripts] == ["policy_cover_level_transactions_dup_del"]
    assert Path(scripts[0]["script_path"]).exists()


def test_snowflake_bronze_generation_uses_selected_tables_without_default_allowlist(monkeypatch):
    monkeypatch.setenv("ATHENA_ENABLE_LLM_SNOWFLAKE_BRONZE_ENHANCEMENT", "false")
    monkeypatch.delenv("ATHENA_SNOWFLAKE_BRONZE_TABLE_ALLOWLIST", raising=False)
    monkeypatch.delenv("SNOWFLAKE_BRONZE_CATALOG", raising=False)
    monkeypatch.delenv("SNOWFLAKE_BRONZE_SCHEMA", raising=False)
    workdir = Path.cwd() / ".tmp-tests" / f"bronze_allowlist_{uuid.uuid4().hex}"
    workdir.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(workdir)

    state = {
        "run_id": "run-small-insurance",
        "target_warehouse": "snowflake",
        "bronze_catalog": "INSURANCE",
        "bronze_schema": "BRONZE",
        "certified_tables": [
            {"database_name": "insurance", "schema_name": "dbo", "table_name": "claim_information"},
            {"database_name": "insurance", "schema_name": "dbo", "table_name": "policy_cover_level_transactions"},
        ],
        "discovered_metadata": {
            "tables": [
                {"table_name": "claim_information", "columns": [{"column_name": "ClaimID", "data_type": "int"}]},
                {"table_name": "policy_cover_level_transactions", "columns": [{"column_name": "PolicyID", "data_type": "int"}]},
            ]
        },
    }

    result = bronze_gen.bronze_code_generation_node(state)
    scripts = sorted(result["bronze_generation_results"], key=lambda item: item["table"])
    script_sql = "\n".join(Path(item["script_path"]).read_text(encoding="utf-8") for item in scripts)

    assert result["bronze_generation_status"] == "COMPLETED"
    assert [item["table"] for item in scripts] == ["claim_information", "policy_cover_level_transactions"]
    assert {item["bronze_catalog"] for item in scripts} == {"ATHENA_DB"}
    assert {item["bronze_schema"] for item in scripts} == {"BRONZE"}
    assert 'CREATE SCHEMA IF NOT EXISTS "ATHENA_DB"."BRONZE"' in script_sql
    assert '"INSURANCE"."BRONZE"' not in script_sql
    assert result["bronze_generation_skipped_tables"] == []


def test_snowflake_bronze_generation_respects_optional_table_allowlist(monkeypatch):
    monkeypatch.setenv("ATHENA_ENABLE_LLM_SNOWFLAKE_BRONZE_ENHANCEMENT", "false")
    monkeypatch.setenv("ATHENA_SNOWFLAKE_BRONZE_TABLE_ALLOWLIST", "claim_information")
    workdir = Path.cwd() / ".tmp-tests" / f"bronze_allowlist_{uuid.uuid4().hex}"
    workdir.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(workdir)

    state = {
        "run_id": "run-explicit-allowlist",
        "target_warehouse": "snowflake",
        "certified_tables": [
            {"database_name": "insurance", "schema_name": "dbo", "table_name": "claim_information"},
            {"database_name": "insurance", "schema_name": "dbo", "table_name": "policy_cover_level_transactions"},
        ],
        "discovered_metadata": {
            "tables": [
                {"table_name": "claim_information", "columns": [{"column_name": "ClaimID", "data_type": "int"}]},
                {"table_name": "policy_cover_level_transactions", "columns": [{"column_name": "PolicyID", "data_type": "int"}]},
            ]
        },
    }

    result = bronze_gen.bronze_code_generation_node(state)

    assert result["bronze_generation_status"] == "COMPLETED"
    assert [item["table"] for item in result["bronze_generation_results"]] == ["claim_information"]
    assert [item["table_name"] for item in result["bronze_generation_skipped_tables"]] == ["policy_cover_level_transactions"]


def test_bronze_script_filename_is_safe_for_case_variant_tables():
    lower = bronze_gen._bronze_script_filename(
        run_id="run-case",
        database_name="insurance",
        schema_name="dbo",
        table_name="policy_cover_level_transactions_dup_del",
        extension="sql",
    )
    mixed = bronze_gen._bronze_script_filename(
        run_id="run-case",
        database_name="insurance",
        schema_name="dbo",
        table_name="policy_cover_level_transactions_Dup_Del",
        extension="sql",
    )

    assert lower.casefold() != mixed.casefold()


def test_snowflake_bronze_generation_can_use_llm_enhancement(monkeypatch):
    monkeypatch.setenv("ATHENA_ENABLE_LLM_SNOWFLAKE_BRONZE_ENHANCEMENT", "true")
    workdir = Path.cwd() / ".tmp-tests" / f"bronze_llm_{uuid.uuid4().hex}"
    workdir.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(workdir)

    calls = []

    def fake_enhance(sql, metadata):
        calls.append(metadata)
        return sql + "\n-- llm enhanced\n"

    monkeypatch.setattr(bronze_gen, "_enhance_snowflake_with_llm", fake_enhance)

    result = bronze_gen._generate_one_table(
        {
            "database_name": "insurance",
            "schema_name": "dbo",
            "table_name": "Claims",
        },
        run_id="run-llm",
        bronze_catalog="ATHENA_DB",
        bronze_schema="BRONZE",
        table_metadata={
            "columns": [
                {"column_name": "ClaimID", "data_type": "int"},
            ]
        },
        target_warehouse="snowflake",
    )

    assert calls
    assert result["llm_enhanced"] is True
    assert result["llm_enhancement_error"] is None


def test_metadata_bronze_artifact_never_embeds_environment_jdbc_credentials(monkeypatch):
    monkeypatch.setattr(
        bronze_gen,
        "build_source_jdbc_url",
        lambda _database: "jdbc:sqlserver://source;user=secret-user;password=secret-password",
    )

    result = bronze_gen._generate_one_table(
        {"database_name": "insurance", "schema_name": "dbo", "table_name": "claims"},
        run_id="run-metadata-secret-free",
        bronze_catalog="main",
        bronze_schema="bronze",
        target_warehouse="databricks",
        table_metadata={
            "columns": [
                {
                    "column_name": "ClaimID",
                    "bronze_target_name": "claimid",
                    "bronze_target_type": "bigint",
                }
            ]
        },
        metadata_driven=True,
    )
    code = Path(result["script_path"]).read_text(encoding="utf-8")

    assert "secret-user" not in code
    assert "secret-password" not in code
    assert "DEFAULT_SOURCE_JDBC_URL = None" in code
    assert 'SOURCE_JDBC_URL_ENV = "ATHENA_SOURCE_JDBC_URL"' in code
    assert "MAPPED_COLUMNS = [{'source': 'ClaimID', 'target': 'claimid', 'type': 'bigint'}]" in code
    assert "DROP TABLE" not in code


def test_metadata_databricks_bronze_projects_only_the_approved_mapping(monkeypatch):
    result = bronze_gen._generate_one_table(
        {"database_name": "insurance", "schema_name": "dbo", "table_name": "claims"},
        run_id="run-metadata-projection",
        bronze_catalog="main",
        bronze_schema="bronze",
        target_warehouse="databricks",
        table_metadata={
            "columns": [
                {
                    "column_name": "ClaimID",
                    "bronze_target_name": "claimid",
                    "bronze_target_type": "bigint",
                },
                {
                    "column_name": "ClaimAmount",
                    "bronze_target_name": "claimamount",
                    "bronze_target_type": "decimal(12,2)",
                },
            ]
        },
        metadata_driven=True,
    )

    code = Path(result["script_path"]).read_text(encoding="utf-8")
    assert "'source': 'ClaimID'" in code
    assert "'source': 'ClaimAmount'" in code
    assert "source_by_name[item[\"source\"].casefold()]" in code
    assert "Mapped source columns are missing" in code
    assert 'RUNTIME_CONTEXT = globals().get("ATHENA_RUNTIME_CONTEXT")' in code
    assert '.withColumn("_logical_work_id", lit(LOGICAL_WORK_ID))' in code
    assert '.mode("overwrite")' in code
    assert '.option("replaceWhere", f"`_logical_work_id` = \'{logical_work_literal}\'")' in code
    assert "DROP TABLE" not in code


def test_metadata_databricks_bronze_resolves_source_credentials_from_secret_scope():
    result = bronze_gen._generate_one_table(
        {"database_name": "insurance", "schema_name": "dbo", "table_name": "claims"},
        run_id="run-secret-scope",
        bronze_catalog="main",
        bronze_schema="bronze",
        target_warehouse="databricks",
        table_metadata={
            "columns": [
                {"column_name": "ClaimID", "bronze_target_name": "claimid", "bronze_target_type": "bigint"}
            ]
        },
        runtime_connection={
            "host_name": "source.database.windows.net",
            "port": 1433,
            "database_name": "insurance",
            "secrets": {
                "username": {"scope": "astra-qa-source-secrets", "key": "claims-db-username"},
                "password": {"scope": "astra-qa-source-secrets", "key": "claims-db-password"},
            },
            "config": {},
        },
        metadata_driven=True,
    )

    code = Path(result["script_path"]).read_text(encoding="utf-8")
    assert 'dbutils.secrets.get(scope=username_ref["scope"], key=username_ref["key"])' in code
    assert "astra-qa-source-secrets" in code
    assert "claims-db-password" in code
    assert "secret-password" not in code


def test_snowflake_llm_enhancement_falls_back_when_target_drifted(monkeypatch):
    monkeypatch.setenv("ATHENA_ENABLE_LLM_SNOWFLAKE_BRONZE_ENHANCEMENT", "true")

    def wrong_target(sql, metadata):
        return sql.replace('"ATHENA_DB"."BRONZE"."bronze_claims"', '"OTHER_DB"."BRONZE"."bronze_claims"')

    monkeypatch.setattr(bronze_gen, "_enhance_snowflake_with_llm", wrong_target)

    result = bronze_gen._generate_one_table(
        {
            "database_name": "insurance",
            "schema_name": "dbo",
            "table_name": "claims",
        },
        run_id="run-target-check",
        bronze_catalog="ATHENA_DB",
        bronze_schema="BRONZE",
        table_metadata={"columns": [{"column_name": "CLAIM_ID", "data_type": "int"}]},
        target_warehouse="snowflake",
    )

    sql = Path(result["script_path"]).read_text(encoding="utf-8")
    assert result["llm_enhanced"] is False
    assert '"ATHENA_DB"."BRONZE"."bronze_claims"' in sql
    assert "expected target table" in str(result["llm_enhancement_error"]).lower()


def test_snowflake_validator_rejects_databricks_format():
    try:
        bronze_gen.validate_snowflake_bronze_sql(
            'CREATE SCHEMA IF NOT EXISTS "A"."B";\n'
            'CREATE TABLE IF NOT EXISTS "A"."B"."bronze_claims" ("run_id" VARCHAR, "ingestion_timestamp" TIMESTAMP_NTZ, "source_system" VARCHAR, "source_table" VARCHAR);\n'
            'INSERT INTO "A"."B"."bronze_claims" SELECT spark.read.format("jdbc"), CURRENT_TIMESTAMP(), \'x\', \'y\';'
        )
    except ValueError as exc:
        assert "databricks/python token" in str(exc).lower()
    else:
        raise AssertionError("Databricks-style Snowflake SQL should be rejected")


def test_snowflake_validator_allows_only_run_scoped_cleanup():
    target = '"A"."B"."bronze_claims"'
    sql = (
        'CREATE SCHEMA IF NOT EXISTS "A"."B";\n'
        f'CREATE TABLE IF NOT EXISTS {target} ("run_id" VARCHAR, "ingestion_timestamp" TIMESTAMP_NTZ, "source_system" VARCHAR, "source_table" VARCHAR);\n'
        f"DELETE FROM {target} WHERE \"run_id\" = 'run-1';\n"
        f"INSERT INTO {target} SELECT 'run-1', CURRENT_TIMESTAMP(), 'insurance', 'claims';"
    )

    bronze_gen.validate_snowflake_bronze_sql(sql, target_table=target)

    try:
        bronze_gen.validate_snowflake_bronze_sql(
            sql.replace('WHERE "run_id" = \'run-1\'', 'WHERE "source_system" = \'insurance\''),
            target_table=target,
        )
    except ValueError as exc:
        assert "delete" in str(exc).lower()
    else:
        raise AssertionError("Non-run-scoped cleanup should be rejected")


def test_snowflake_bronze_generation_skips_llm_by_default(monkeypatch):
    monkeypatch.delenv("ATHENA_ENABLE_LLM_SNOWFLAKE_BRONZE_ENHANCEMENT", raising=False)

    called = {"enhance": 0}

    def fail_if_called(sql, metadata):
        called["enhance"] += 1
        raise AssertionError("Snowflake LLM enhancement should be opt-in")

    monkeypatch.setattr(bronze_gen, "_enhance_snowflake_with_llm", fail_if_called)

    result = bronze_gen._generate_one_table(
        {
            "database_name": "insurance",
            "schema_name": "dbo",
            "table_name": "claims",
        },
        run_id="run-no-llm",
        bronze_catalog="main",
        bronze_schema="bronze",
        cast_rules={"claim_id": "int"},
        table_metadata={"columns": [{"column_name": "CLAIM_ID", "data_type": "int"}]},
        target_warehouse="snowflake",
    )

    assert called["enhance"] == 0
    assert result["llm_enhanced"] is False
    assert result["llm_enhancement_error"] is None
    assert "-- llm enhanced" not in Path(result["script_path"]).read_text(encoding="utf-8")


def test_snowflake_dbt_bronze_refreshes_models_from_current_tables(monkeypatch, tmp_path):
    monkeypatch.setenv("SNOWFLAKE_BRONZE_CATALOG", "ATHENA_DB")
    monkeypatch.setenv("SNOWFLAKE_BRONZE_SCHEMA", "BRONZE")
    monkeypatch.setenv("SNOWFLAKE_RAW_SCHEMA", "RAW")
    monkeypatch.delenv("ATHENA_SNOWFLAKE_BRONZE_TABLE_ALLOWLIST", raising=False)
    monkeypatch.chdir(tmp_path)

    state = {
        "run_id": "run-dbt-refresh",
        "target_warehouse": "snowflake",
        "execution_engine": "dbt",
        "certified_tables": [
            {
                "database_name": "insurance",
                "schema_name": "dbo",
                "table_name": "ClaimInformation",
            }
        ],
        "discovered_metadata": {
            "tables": [
                {
                    "table_name": "ClaimInformation",
                    "columns": [{"column_name": "ClaimID", "data_type": "int"}],
                }
            ]
        },
    }

    first = bronze_gen.bronze_code_generation_node(state)
    first_result = first["bronze_generation_results"][0]
    first_model = Path(first_result["script_path"])
    first_sql = first_model.read_text(encoding="utf-8")

    assert first_model.name == "bronze_claiminformation.sql"
    assert first_result["code_generation_format"] == "dbt"
    assert first_result["dbt_alias"] == "bronze_ClaimInformation"
    assert "{{ source('athena_db_raw', 'raw_claiminformation') }}" in first_sql
    assert first_result["snowflake_landing_database"] == "ATHENA_DB"
    assert first_result["snowflake_landing_schema"] == "RAW"
    assert first_result["snowflake_landing_table"] == "raw_ClaimInformation"
    assert "CREATE TABLE" not in first_sql
    sources_yml = Path(first["snowflake_dbt_sources_path"]).read_text(encoding="utf-8")
    assert 'database: "ATHENA_DB"' in sources_yml
    assert 'schema: "RAW"' in sources_yml
    assert 'identifier: "raw_ClaimInformation"' in sources_yml

    reviewed_sql = first_sql + "\n-- reviewed bronze\n"
    reviewed = bronze_gen.sync_snowflake_dbt_bronze_review(
        "run-dbt-refresh",
        [first_result],
        {
            "feeds": [
                {
                    "database_name": "insurance",
                    "schema_name": "dbo",
                    "table_name": "ClaimInformation",
                    "review_status": "APPROVED",
                    "approved_schema": [
                        {
                            "column_name": "claimid",
                            "description": "Reviewed claim identifier",
                        }
                    ],
                    "generated_bronze_script": reviewed_sql,
                    "primary_keys": ["claimid"],
                }
            ]
        },
    )

    assert len(reviewed) == 1
    assert first_model.read_text(encoding="utf-8") == reviewed_sql
    assert reviewed[0]["primary_keys"] == ["claimid"]
    assert "Reviewed claim identifier" in Path(
        first["snowflake_dbt_bronze_schema_path"]
    ).read_text(encoding="utf-8")

    renamed = bronze_gen.bronze_code_generation_node(
        {
            **state,
            "certified_tables": [
                {
                    "database_name": "insurance",
                    "schema_name": "dbo",
                    "table_name": "PolicyDetail",
                }
            ],
            "discovered_metadata": {
                "tables": [
                    {
                        "table_name": "PolicyDetail",
                        "columns": [{"column_name": "PolicyID", "data_type": "int"}],
                    }
                ]
            },
        }
    )
    renamed_model = Path(renamed["bronze_generation_results"][0]["script_path"])

    assert not first_model.exists()
    assert renamed_model.name == "bronze_policydetail.sql"

    rejected = bronze_gen.sync_snowflake_dbt_bronze_review(
        "run-dbt-refresh",
        renamed["bronze_generation_results"],
        {
            "feeds": [
                {
                    "database_name": "insurance",
                    "schema_name": "dbo",
                    "table_name": "PolicyDetail",
                    "review_status": "REJECTED",
                }
            ]
        },
    )

    assert rejected == []
    assert not renamed_model.exists()
    assert "policydetail" not in Path(
        renamed["snowflake_dbt_sources_path"]
    ).read_text(encoding="utf-8")
    assert "bronze_policydetail" not in Path(
        renamed["snowflake_dbt_bronze_schema_path"]
    ).read_text(encoding="utf-8")

    empty = bronze_gen.bronze_code_generation_node(
        {
            **state,
            "certified_tables": [],
            "nominated_tables": [],
            "discovered_metadata": {},
        }
    )

    assert empty["bronze_generation_status"] == "SKIPPED"
    assert not renamed_model.exists()
    assert Path(empty["snowflake_dbt_sources_path"]).exists()
    assert Path(empty["snowflake_dbt_bronze_schema_path"]).exists()


def test_snowflake_dbt_bronze_landing_schema_defaults_to_bronze(monkeypatch, tmp_path):
    monkeypatch.delenv("SNOWFLAKE_RAW_SCHEMA", raising=False)
    monkeypatch.chdir(tmp_path)

    result = bronze_gen._generate_one_table(
        run_id="run-dbt-legacy-landing",
        table_ref={
            "database_name": "insurance",
            "schema_name": "dbo",
            "table_name": "Claims",
        },
        bronze_catalog="ATHENA_DB",
        bronze_schema="BRONZE",
        target_warehouse="snowflake",
        execution_engine="dbt",
    )

    assert result["snowflake_landing_schema"] == "BRONZE"
    assert "{{ source('athena_db_bronze', 'raw_claims') }}" in Path(
        result["script_path"]
    ).read_text(encoding="utf-8")


def test_snowflake_dbt_bronze_rejects_sanitized_model_name_collision(monkeypatch, tmp_path):
    monkeypatch.delenv("ATHENA_SNOWFLAKE_BRONZE_TABLE_ALLOWLIST", raising=False)
    monkeypatch.chdir(tmp_path)

    with pytest.raises(ValueError, match="unique after sanitization"):
        bronze_gen.bronze_code_generation_node(
            {
                "run_id": "run-dbt-bronze-collision",
                "target_warehouse": "snowflake",
                "execution_engine": "dbt",
                "certified_tables": [
                    {
                        "database_name": "insurance",
                        "schema_name": "dbo",
                        "table_name": "Claim-Information",
                    },
                    {
                        "database_name": "insurance",
                        "schema_name": "dbo",
                        "table_name": "Claim Information",
                    },
                ],
            }
        )
