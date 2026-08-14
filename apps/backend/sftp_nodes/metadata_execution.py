from __future__ import annotations

import json
import threading
import uuid
from typing import Any, Dict, List, Mapping


def _artifacts_by_object(state: Mapping[str, Any], key: str) -> Dict[int, Dict[str, Any]]:
    artifacts = {
        int(item.get("ingestion_object_id") or 0): dict(item)
        for item in state.get(key) or []
        if isinstance(item, Mapping) and int(item.get("ingestion_object_id") or 0) > 0
    }
    if not artifacts:
        raise RuntimeError(f"ADLS metadata execution found no source-bound {key}.")
    return artifacts


def _runtime_scripts(
    contexts: List[Dict[str, Any]], artifacts: Dict[int, Dict[str, Any]]
) -> List[Dict[str, Any]]:
    scripts = []
    for item in contexts:
        object_id = int(item["obj"]["ingestion_object_id"])
        artifact = artifacts.get(object_id)
        if not artifact:
            raise RuntimeError(f"ADLS runtime artifact is missing for source object {object_id}.")
        scripts.append({
            **artifact,
            "metadata_runtime": True,
            "metadata_runtime_context": dict(item["runtime_context"]),
        })
    return scripts


def _fail_claims(
    repository: Any,
    contexts: List[Dict[str, Any]],
    worker_id: str,
    exc: BaseException,
    *,
    layer: str,
) -> None:
    from services.metadata_runtime_worker import _retryable_execution_error

    retryable = _retryable_execution_error(exc)
    for item in contexts:
        try:
            repository.record_run_error(
                run={**item["run"], "current_phase": f"{layer.upper()}_WRITE"},
                error_stage="WRITE",
                error=exc,
                retryable=retryable,
                detail={"operation": f"ADLS_{layer.upper()}_EXECUTION"},
                worker_id=worker_id,
            )
            repository.finalize_failed_run(
                run=item["run"], worker_id=worker_id, retryable=retryable, message=str(exc)
            )
        except Exception:
            pass


def _finalize_claims(
    repository: Any,
    contexts: List[Dict[str, Any]],
    worker_id: str,
    *,
    platform: str,
    bronze_result: Mapping[str, Any],
    silver_result: Mapping[str, Any],
    gold_result: Mapping[str, Any],
) -> None:
    prefix = "databricks" if platform == "databricks" else "snowflake"

    def by_run(result: Mapping[str, Any], layer: str) -> Dict[str, Dict[str, Any]]:
        return {
            str((item.get("execution_result") or {}).get("runtime_run_id") or item.get("runtime_run_id") or ""): dict(item)
            for item in result.get(f"{prefix}_{layer}_execution_results") or []
            if isinstance(item, Mapping)
            and str((item.get("execution_result") or {}).get("runtime_run_id") or item.get("runtime_run_id") or "")
        }

    bronze_results = by_run(bronze_result, "bronze")
    silver_results = by_run(silver_result, "silver")
    gold_results = [
        dict(item)
        for item in gold_result.get(f"{prefix}_gold_execution_results") or []
        if isinstance(item, Mapping)
    ]
    failed_gold = [
        item for item in gold_results
        if str(item.get("status") or "").upper() in {"FAILED", "ERROR"}
    ]
    gold_status = str(gold_result.get(f"{prefix}_gold_execution_status") or "").upper()
    if failed_gold and gold_status != "COMPLETED_WITH_WARNINGS":
        raise RuntimeError("ADLS Gold execution contains failed target artifacts.")
    updates = []
    attempts = []
    for item in contexts:
        run = item["run"]
        queue = item["queue"]
        runtime_run_id = str(run["run_id"])
        layer_evidence = {}
        for layer, results in (("bronze", bronze_results), ("silver", silver_results)):
            execution = results.get(runtime_run_id) or {}
            evidence = dict(execution.get("execution_result") or {})
            if (
                str(execution.get("status") or "").upper() not in {"SUCCESS", "COMPLETED"}
                or not evidence.get("target_commit_id")
                or str(evidence.get("runtime_run_id") or "") != runtime_run_id
                or str(evidence.get("logical_work_id") or "")
                != str(item["runtime_context"].get("logical_work_id") or "")
                or str(evidence.get("validation_status") or "").upper() != "PASSED"
            ):
                raise RuntimeError(
                    f"ADLS {layer.capitalize()} execution returned invalid commit evidence for "
                    f"source object {item['obj']['ingestion_object_id']}."
                )
            layer_evidence[layer] = evidence
        silver_evidence = layer_evidence["silver"]
        updates.append({
            "run_id": str(run["run_id"]),
            "queue_id": int(queue["queue_id"]),
            "rows_read": layer_evidence["bronze"].get("rows_read"),
            "rows_written": silver_evidence.get("rows_written"),
            "target_write_id": str(silver_evidence["target_commit_id"]),
            "target_commit_status": "COMMITTED",
            "validation_status": "PASSED",
            "validation_summary_json": json.dumps(
                {
                    "bronze": layer_evidence["bronze"],
                    "silver": silver_evidence,
                    "gold": {
                        "status": str(gold_result.get(f"{prefix}_gold_execution_status") or ""),
                        "artifact_count": len(gold_results),
                        "results": gold_results,
                    },
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
            "watermark_commit_status": "SKIPPED",
        })
        attempts.append({"run_id": str(run["run_id"]), "queue_id": int(queue["queue_id"])})
    repository.begin_queue_finalizations(
        queue_ids=[int(item["queue"]["queue_id"]) for item in contexts], worker_id=worker_id
    )
    repository.update_run_phases(phase="TARGET_WRITTEN", updates=updates, worker_id=worker_id)
    repository.finalize_successful_runs(attempts=attempts, worker_id=worker_id)


def execute_adls_metadata_native_runtime(state: Dict[str, Any]) -> Dict[str, Any]:
    """Keep source-file queue work open through Bronze, Silver, and Gold execution."""
    from services.metadata_selection import validated_target_metadata_selection
    from services.pipeline_runtime import (
        _complete_run_with_report,
        _enqueue_metadata_native_runtime,
        save_checkpoint_state_timed,
    )

    platform = str(state.get("target_warehouse") or "databricks").lower()
    if platform not in {"databricks", "snowflake"}:
        raise ValueError("ADLS metadata execution supports Databricks or Snowflake native targets.")
    if platform == "databricks":
        from services.databricks_runtime import (
            run_databricks_bronze_scripts as run_bronze,
            run_databricks_gold_scripts as run_gold,
            run_databricks_silver_scripts as run_silver,
        )
    else:
        from services.snowflake_bronze_runtime import run_snowflake_bronze_scripts as run_bronze
        from services.snowflake_gold_runtime import run_snowflake_gold_scripts as run_gold
        from services.snowflake_silver_runtime import run_snowflake_silver_scripts as run_silver

    queued_state = _enqueue_metadata_native_runtime(state)
    selection = validated_target_metadata_selection(queued_state)
    if not selection:
        raise ValueError("ADLS metadata execution requires a valid target selection.")
    repository = selection.repository
    logical_ids = {
        str(item.get("logical_work_id") or "")
        for item in queued_state.get("metadata_runtime_queue") or []
        if isinstance(item, Mapping) and str(item.get("logical_work_id") or "")
    }
    if len(logical_ids) != 1:
        raise RuntimeError("ADLS runtime queue must contain one logical work identity.")
    logical_work_id = logical_ids.pop()
    worker_id = f"adls:{state.get('run_id')}:{uuid.uuid4()}"
    with repository.unit_of_work():
        claimed = repository.claim_queue_items(
            worker_id=worker_id, lease_seconds=600,
            logical_work_id=logical_work_id, limit=100,
        )
        if len(claimed) != len(queued_state.get("metadata_runtime_queue") or []):
            for queue_item in claimed:
                repository.release_queue_for_same_attempt_resume(
                    queue_id=int(queue_item["queue_id"]),
                    worker_id=worker_id,
                    message="ADLS runtime did not atomically claim the complete source-file batch.",
                )
            raise RuntimeError("ADLS runtime could not claim every queued source-file work item.")
        attempts = repository.create_run_attempts(
            claimed, pipeline_name="adls_metadata_runtime", worker_id=worker_id
        )
    contexts = [
        {
            "queue": queue,
            "run": attempt["run"],
            "obj": attempt["ingestion_object"],
            "runtime_context": {
                **attempt["runtime_context"],
                "resumed_attempt": bool(attempt.get("resumed_attempt")),
            },
        }
        for queue, attempt in zip(claimed, attempts)
    ]
    stop = threading.Event()
    heartbeat_error: List[BaseException] = []

    def heartbeat() -> None:
        while not stop.wait(60):
            try:
                repository.heartbeat_queue_items(
                    queue_ids=[int(item["queue"]["queue_id"]) for item in contexts],
                    worker_id=worker_id, lease_seconds=600,
                )
            except BaseException as exc:
                heartbeat_error.append(exc)
                stop.set()

    thread = threading.Thread(target=heartbeat, name="adls-metadata-leases", daemon=True)
    thread.start()
    active_layer = "bronze"

    def submitted(layer: str):
        def record(receipt: str) -> None:
            if not str(receipt or "").strip():
                raise RuntimeError(f"{platform.capitalize()} {layer} returned no submission receipt.")
            repository.update_run_phases(
                phase="TARGET_SUBMITTED",
                worker_id=worker_id,
                updates=[{
                    "run_id": str(item["run"]["run_id"]),
                    "queue_id": int(item["queue"]["queue_id"]),
                    "target_write_id": f"{layer}:{receipt}",
                    "target_commit_status": "SUBMITTED",
                } for item in contexts],
            )
        return record

    try:
        bronze_scripts = _runtime_scripts(
            contexts, _artifacts_by_object(queued_state, "bronze_generation_results")
        )
        bronze_state = {
            **queued_state,
            "status": "RUNNING",
            "execution_ready": True,
            "background_stage": "bronze_code_execution",
            "metadata_runtime_batch": True,
            "metadata_runtime_context": dict(contexts[0]["runtime_context"]),
            "_metadata_runtime_scripts": bronze_scripts,
        }
        save_checkpoint_state_timed(
            str(state.get("run_id") or ""), bronze_state, context="adls_metadata_bronze:running"
        )
        if platform == "databricks":
            bronze_result = run_bronze(
                bronze_state, approved_only=False, on_submitted=submitted("bronze")
            )
        else:
            submitted("bronze")(f"{state.get('run_id')}:bronze")
            bronze_result = run_bronze(bronze_state, approved_only=False)
        if heartbeat_error:
            raise RuntimeError("ADLS queue lease was lost during Bronze execution.") from heartbeat_error[0]

        active_layer = "silver"
        silver_scripts = _runtime_scripts(
            contexts, _artifacts_by_object(queued_state, "silver_generation_results")
        )
        silver_state = {
            **queued_state,
            **bronze_result,
            "status": "RUNNING",
            "execution_ready": True,
            "background_stage": "silver_code_execution",
            "metadata_runtime_batch": True,
            "allow_partial_stage_success": platform == "databricks",
            "metadata_runtime_context": dict(contexts[0]["runtime_context"]),
            "_metadata_runtime_scripts": silver_scripts,
        }
        save_checkpoint_state_timed(
            str(state.get("run_id") or ""), silver_state, context="adls_metadata_silver:running"
        )
        if platform == "databricks":
            silver_result = run_silver(
                silver_state, approved_only=False, on_submitted=submitted("silver")
            )
        else:
            submitted("silver")(f"{state.get('run_id')}:silver")
            silver_result = run_silver(silver_state, approved_only=False)
        if heartbeat_error:
            raise RuntimeError("ADLS queue lease was lost during Silver execution.") from heartbeat_error[0]

        silver_results = {
            str((result.get("execution_result") or {}).get("runtime_run_id") or result.get("runtime_run_id") or ""): result
            for result in silver_result.get(f"{platform}_silver_execution_results") or []
            if isinstance(result, Mapping)
        }
        successful_contexts = [
            item for item in contexts
            if str(silver_results.get(str(item["run"]["run_id"]), {}).get("status") or "").upper()
            in {"SUCCESS", "COMPLETED"}
        ]
        failed_contexts = [item for item in contexts if item not in successful_contexts]
        if failed_contexts:
            logger.warning(
                "ADLS Silver completed with warnings: %d/%d scripts succeeded; continuing to Gold.",
                len(successful_contexts),
                len(contexts),
                extra={
                    "run_id": state.get("run_id"),
                    "node": "silver_partial_success",
                    "stage": "silver_code_execution",
                },
            )

        active_layer = "gold"
        gold_state = {
            **queued_state,
            **bronze_result,
            **silver_result,
            "status": "RUNNING",
            "execution_ready": True,
            "background_stage": "gold_code_execution",
        }
        gold_state.pop("_metadata_runtime_scripts", None)
        gold_state.pop("metadata_runtime_context", None)
        gold_state.pop("metadata_runtime_batch", None)
        save_checkpoint_state_timed(
            str(state.get("run_id") or ""), gold_state, context="adls_gold_execution:running"
        )
        if platform == "databricks":
            gold_result = run_gold(
                gold_state,
                review_artifact=gold_state.get("gold_review_artifact") or {},
                approved_only=True,
                on_submitted=submitted("gold"),
            )
        else:
            submitted("gold")(f"{state.get('run_id')}:gold")
            gold_result = run_gold(gold_state)
        if heartbeat_error:
            raise RuntimeError("ADLS queue lease was lost during Gold execution.") from heartbeat_error[0]
        with repository.unit_of_work():
            _finalize_claims(
                repository,
                successful_contexts,
                worker_id,
                platform=platform,
                bronze_result=bronze_result,
                silver_result=silver_result,
                gold_result=gold_result,
            )
            if failed_contexts:
                _fail_claims(
                    repository,
                    failed_contexts,
                    worker_id,
                    RuntimeError("ADLS Silver script failed; successful feeds continued to Gold."),
                    layer="silver",
                )
    except BaseException as exc:
        with repository.unit_of_work():
            _fail_claims(repository, contexts, worker_id, exc, layer=active_layer)
        raise
    finally:
        stop.set()
        thread.join(timeout=5)

    return _complete_run_with_report(
        {
            **gold_state,
            **gold_result,
            "metadata_runtime_results": [item["run"] for item in contexts],
        },
        running_message="Gold target execution completed. Generating the pipeline run report.",
        completed_message="Bronze, Silver, and Gold target execution and run report completed.",
        context="adls_native_execution",
    )
