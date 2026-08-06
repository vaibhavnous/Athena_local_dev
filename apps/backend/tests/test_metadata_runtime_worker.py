from __future__ import annotations

import json

import pytest

from services import metadata_runtime_worker


def completed_databricks_gold():
    return {
        "databricks_gold_execution_status": "COMPLETED",
        "databricks_gold_execution_results": [
            {
                "status": "SUCCESS",
                "verification_status": "VERIFIED",
                "databricks_run_id": 456,
                "execution_result": {
                    "contract_version": "1.0",
                    "status": "COMPLETED",
                    "logical_work_id": "logical-work",
                    "runtime_run_id": "runtime-run",
                    "target_table": "main.gold.fact_claims",
                    "target_commit_id": "delta:main.gold.fact_claims:v12",
                    "rows_written": 10,
                    "validation_status": "PASSED",
                },
            }
        ],
    }


def test_databricks_execution_receipt_must_bind_exact_delta_target():
    result = completed_databricks_gold()
    result["databricks_gold_execution_results"][0]["execution_result"]["target_commit_id"] = (
        "delta:main.gold.other_fact:v12"
    )

    with pytest.raises(RuntimeError, match="exact Delta target commit"):
        metadata_runtime_worker._assert_execution_completed(
            result,
            {
                "processing_stage": "SILVER_TO_GOLD",
                "execution_spec_json": '{"target_platform":"DATABRICKS"}',
            },
            {
                "logical_work_id": "logical-work",
                "runtime_run_id": "runtime-run",
                "target_table": "main.gold.fact_claims",
            },
        )


class Repository:
    def __init__(self, *, stateful: bool = False, snapshot_matches: bool = True) -> None:
        self.calls = []
        self.queue = {
            "queue_id": 10,
            "ingestion_object_id": 20,
            "attempt_count": 1,
            "work_scope_json": "{}",
        }
        self.run = {
            "run_id": "runtime-run",
            "queue_id": 10,
            "attempt_number": 1,
            "ingestion_object_id": 20,
            "idempotency_key": "work-key",
            "logical_work_id": "logical-work",
        }
        self.obj = {
            "ingestion_object_id": 20,
            "processing_stage": "SILVER_TO_GOLD",
            "target_table": "main.gold.fact_claims",
            "execution_spec_json": '{"target_platform":"DATABRICKS"}',
            "watermark_column": "updated_at" if stateful else None,
        }
        self.snapshot_matches = snapshot_matches

    def claim_next_queue_item(self, **kwargs):
        self.calls.append(("claim", kwargs))
        return self.queue

    def release_ready_downstream_from_successes(self):
        self.calls.append(("release", None))
        return []

    def create_run_attempt(self, *_args, **_kwargs):
        self.calls.append(("create_run", None))
        return {
            "run": self.run,
            "ingestion_object": self.obj,
            "mapping": {},
            "runtime_context": {
                "contract_version": "1.0",
                "logical_work_id": "logical-work",
                "queue_id": 10,
                "ingestion_object_id": 20,
                "processing_stage": self.obj["processing_stage"],
                "load_type": "FULL",
                "target_table": self.obj["target_table"],
                "config_version": 2,
                "mapping_version": 3,
                "runtime_run_id": "runtime-run",
            },
            "metadata_snapshot_matches": self.snapshot_matches,
        }

    def recover_committed_queue_item(self, **_kwargs):
        self.calls.append(("recover", None))
        return None

    def assert_runtime_dependencies(self, _obj, **_kwargs):
        self.calls.append(("dependencies", None))

    def heartbeat_queue_item(self, **_kwargs):
        self.calls.append(("heartbeat", None))

    def update_run_phase(self, run_id, phase, **fields):
        self.calls.append(("phase", run_id, phase, fields))

    def begin_queue_finalization(self, **_kwargs):
        self.calls.append(("begin_finalize", None))

    def finalize_successful_run(self, **_kwargs):
        self.calls.append(("success", None))

    def enqueue_ready_downstream(self, **_kwargs):
        self.calls.append(("enqueue_downstream", None))
        return []

    def record_run_error(self, **kwargs):
        self.calls.append(("error", kwargs["error_stage"], kwargs["retryable"]))

    def finalize_failed_run(self, **kwargs):
        self.calls.append(("failed", kwargs["retryable"]))

    def release_queue_for_same_attempt_resume(self, **_kwargs):
        self.calls.append(("resume_same_attempt", None))


def test_snowflake_bronze_runtime_state_uses_pinned_source_and_landing_resources():
    state = metadata_runtime_worker._runtime_state(
        {"run_id": "runtime-1"},
        {
            "ingestion_object_id": 20,
            "processing_stage": "SOURCE_TO_BRONZE",
            "target_bronze_table": "ATHENA.BRONZE.BRONZE_CLAIMS",
        },
        {
            "target_platform": "SNOWFLAKE",
            "source_resource": {"database": "ClaimsDB", "schema": "dbo", "table": "Claims"},
            "landing_resource": {"database": "ATHENA", "schema": "BRONZE", "table": "raw_Claims"},
        },
        "claims.sql",
        {"logical_work_id": "logical-1"},
    )
    artifact = state["bronze_generation_results"][0]

    assert artifact["metadata_runtime"] is True
    assert (artifact["database_name"], artifact["schema_name"], artifact["table"]) == (
        "ClaimsDB", "dbo", "Claims"
    )
    assert artifact["snowflake_landing_table"] == "raw_Claims"


def test_worker_commits_control_state_only_after_verified_target_execution(monkeypatch):
    repository = Repository()
    monkeypatch.setattr(
        metadata_runtime_worker, "_execute_registered_artifact", lambda *_: completed_databricks_gold()
    )

    result = metadata_runtime_worker.process_next_metadata_work(repository, worker_id="worker-1")

    call_names = [call[0] for call in repository.calls]
    assert result["status"] == "SUCCESS"
    assert call_names == [
        "release", "claim", "recover", "create_run", "dependencies", "heartbeat", "heartbeat",
        "begin_finalize", "phase", "success", "enqueue_downstream"
    ]
    phase = next(call for call in repository.calls if call[0] == "phase")
    assert phase[2] == "TARGET_WRITTEN"
    assert phase[3]["target_commit_status"] == "COMMITTED"
    assert phase[3]["watermark_commit_status"] == "SKIPPED"


def test_worker_logs_and_finalizes_failure_without_advancing_state(monkeypatch):
    repository = Repository()

    def fail(*_args):
        raise TimeoutError("token=do-not-log")

    monkeypatch.setattr(metadata_runtime_worker, "_execute_registered_artifact", fail)

    with pytest.raises(TimeoutError):
        metadata_runtime_worker.process_next_metadata_work(repository, worker_id="worker-1")

    call_names = [call[0] for call in repository.calls]
    assert "phase" not in call_names
    assert call_names[-2:] == ["error", "failed"]
    assert repository.calls[-2][2] is True
    assert repository.calls[-1][1] is True


def test_worker_leaves_committed_run_recoverable_when_final_status_update_fails(monkeypatch):
    repository = Repository()
    monkeypatch.setattr(
        metadata_runtime_worker, "_execute_registered_artifact", lambda *_: completed_databricks_gold()
    )

    def fail_final_status(**_kwargs):
        repository.calls.append(("success_failed", None))
        raise ConnectionError("control finalization unavailable")

    repository.finalize_successful_run = fail_final_status

    with pytest.raises(ConnectionError, match="control finalization"):
        metadata_runtime_worker.process_next_metadata_work(repository, worker_id="worker-1")

    call_names = [call[0] for call in repository.calls]
    assert "phase" in call_names
    assert "error" in call_names
    assert "failed" not in call_names


def test_worker_preserves_verified_commit_when_begin_finalization_fails(monkeypatch):
    repository = Repository()
    monkeypatch.setattr(
        metadata_runtime_worker, "_execute_registered_artifact", lambda *_: completed_databricks_gold()
    )
    repository.begin_queue_finalization = lambda **_kwargs: (_ for _ in ()).throw(
        ConnectionError("control transition unavailable")
    )

    with pytest.raises(ConnectionError, match="control transition"):
        metadata_runtime_worker.process_next_metadata_work(repository, worker_id="worker-1")

    assert "error" in [call[0] for call in repository.calls]
    assert "failed" not in [call[0] for call in repository.calls]


def test_ambiguous_submission_releases_same_attempt_without_marking_failure(monkeypatch):
    class Ambiguous(RuntimeError):
        retryable = True
        preserve_attempt = True

    repository = Repository()
    monkeypatch.setattr(
        metadata_runtime_worker,
        "_execute_registered_artifact",
        lambda *_: (_ for _ in ()).throw(Ambiguous("submit response lost")),
    )

    with pytest.raises(Ambiguous):
        metadata_runtime_worker.process_next_metadata_work(repository, worker_id="worker-1")

    assert [call[0] for call in repository.calls][-2:] == ["error", "resume_same_attempt"]
    assert "failed" not in [call[0] for call in repository.calls]


def test_worker_fails_closed_for_stateful_artifact_without_checkpoint_protocol(monkeypatch):
    repository = Repository(stateful=True)
    monkeypatch.setattr(
        metadata_runtime_worker,
        "_execute_registered_artifact",
        lambda *_: (_ for _ in ()).throw(AssertionError("stateful artifact must not execute")),
    )

    with pytest.raises(RuntimeError, match="checkpoint-output protocol"):
        metadata_runtime_worker.process_next_metadata_work(repository, worker_id="worker-1")

    assert [call[0] for call in repository.calls][-2:] == ["error", "failed"]


def test_worker_recovers_committed_attempt_without_reexecuting(monkeypatch):
    repository = Repository()
    repository.recover_committed_queue_item = lambda **_kwargs: repository.run
    monkeypatch.setattr(
        metadata_runtime_worker,
        "_execute_registered_artifact",
        lambda *_: (_ for _ in ()).throw(AssertionError("committed target must not execute again")),
    )

    result = metadata_runtime_worker.process_next_metadata_work(repository, worker_id="worker-1")

    assert result["status"] == "RECOVERED_SUCCESS"
    assert [call[0] for call in repository.calls] == ["release", "claim"]


def test_worker_records_permanent_failure_for_stale_metadata_snapshot(monkeypatch):
    repository = Repository(snapshot_matches=False)
    monkeypatch.setattr(
        metadata_runtime_worker,
        "_execute_registered_artifact",
        lambda *_: (_ for _ in ()).throw(AssertionError("stale metadata must not execute")),
    )

    with pytest.raises(RuntimeError, match="no longer the active"):
        metadata_runtime_worker.process_next_metadata_work(repository, worker_id="worker-1")

    assert [call[0] for call in repository.calls][-2:] == ["error", "failed"]
    assert repository.calls[-2][2] is False


def test_worker_rejects_disabled_target_execution(monkeypatch):
    repository = Repository()
    monkeypatch.setattr(
        metadata_runtime_worker,
        "_execute_registered_artifact",
        lambda *_: {"databricks_gold_execution_status": "DISABLED"},
    )

    with pytest.raises(RuntimeError, match="did not complete"):
        metadata_runtime_worker.process_next_metadata_work(repository, worker_id="worker-1")

    assert not any(call[0] == "phase" for call in repository.calls)
    assert [call[0] for call in repository.calls][-2:] == ["error", "failed"]


def test_worker_requires_blocking_validation_evidence(monkeypatch):
    repository = Repository()
    repository.obj["validation_policy_json"] = '{"rules":[{"rule":"NOT_NULL"}]}'
    monkeypatch.setattr(
        metadata_runtime_worker, "_execute_registered_artifact", lambda *_: completed_databricks_gold()
    )

    with pytest.raises(RuntimeError, match="validation evidence"):
        metadata_runtime_worker.process_next_metadata_work(repository, worker_id="worker-1")

    assert not any(call[0] == "phase" for call in repository.calls)


def test_worker_accepts_hash_pinned_embedded_validation(monkeypatch):
    from services.metadata_contracts import canonical_json_hash

    repository = Repository()
    policy = {
        "schema_version": "1.0",
        "rules": [{"rule_type": "TARGET_SCHEMA_MATCH", "threshold_value": 0}],
    }
    repository.obj["validation_policy_json"] = json.dumps(policy)
    repository.obj["execution_spec_json"] = json.dumps({
        "target_platform": "DATABRICKS",
        "embedded_blocking_validation": True,
        "validation_policy_hash": canonical_json_hash(policy),
    })
    completed = completed_databricks_gold()
    completed["databricks_gold_execution_results"][0]["execution_result"].update({
        "validation_policy_hash": canonical_json_hash(policy),
        "validation_results": [
            {
                "rule_type": "TARGET_SCHEMA_MATCH",
                "observed_value": 0,
                "threshold_value": 0,
                "status": "PASSED",
            }
        ],
    })
    monkeypatch.setattr(
        metadata_runtime_worker, "_execute_registered_artifact", lambda *_: completed
    )

    result = metadata_runtime_worker.process_next_metadata_work(repository, worker_id="worker-1")

    assert result["status"] == "SUCCESS"


def test_worker_executes_full_source_to_bronze_with_runtime_context(monkeypatch):
    repository = Repository()
    repository.obj["processing_stage"] = "SOURCE_TO_BRONZE"
    repository.obj["target_table"] = "main.bronze.claims"
    monkeypatch.setattr(
        metadata_runtime_worker,
        "_execute_registered_artifact",
        lambda *_: {
            "databricks_bronze_execution_status": "COMPLETED",
            "databricks_bronze_execution_results": [
                {
                    "status": "SUCCESS",
                    "verification_status": "VERIFIED",
                    "databricks_run_id": 789,
                    "execution_result": {
                        "contract_version": "1.0",
                        "status": "COMPLETED",
                        "logical_work_id": "logical-work",
                        "runtime_run_id": "runtime-run",
                        "target_table": "main.bronze.claims",
                        "target_commit_id": "delta:main.bronze.claims:v3",
                        "validation_status": "PASSED",
                    },
                }
            ],
        },
    )

    result = metadata_runtime_worker.process_next_metadata_work(repository, worker_id="worker-1")

    assert result["status"] == "SUCCESS"
