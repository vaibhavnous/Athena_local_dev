from __future__ import annotations

import json
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, Optional

from services.metadata_repository import MetadataRepository


def _runtime_state(
    run: Dict[str, Any],
    obj: Dict[str, Any],
    execution_spec: Dict[str, Any],
    artifact_path: str,
    runtime_context: Dict[str, Any],
    mapping: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    try:
        validation_policy = json.loads(str(obj.get("validation_policy_json") or "{}"))
        merge_keys = json.loads(str(obj.get("merge_keys_json") or "[]"))
    except json.JSONDecodeError as exc:
        raise RuntimeError("Runtime object contains invalid validation or merge-key JSON.") from exc
    artifact = {
        "status": "APPROVED",
        "target_table": obj.get("target_table") or obj.get("target_bronze_table"),
        "script_path": artifact_path,
        "execution_spec": execution_spec,
        "ingestion_object_id": obj["ingestion_object_id"],
        "write_mode": obj.get("write_mode"),
        "metadata_runtime": True,
        "metadata_runtime_context": runtime_context,
        "validation_policy": validation_policy,
        "mapping_contract": list((mapping or {}).get("mappings") or []),
        "merge_keys": merge_keys,
    }
    source_resource = execution_spec.get("source_resource")
    landing_resource = execution_spec.get("landing_resource")
    if isinstance(source_resource, dict):
        artifact.update({
            "database_name": source_resource.get("database"),
            "schema_name": source_resource.get("schema"),
            "table": source_resource.get("table"),
        })
    if isinstance(landing_resource, dict):
        artifact.update({
            "snowflake_landing_database": landing_resource.get("database"),
            "snowflake_landing_schema": landing_resource.get("schema"),
            "snowflake_landing_table": landing_resource.get("table"),
        })
    stage = str(obj.get("processing_stage") or "").upper()
    if stage != "SOURCE_TO_BRONZE":
        try:
            dependencies = json.loads(str(obj.get("dependency_objects_json") or "{}"))["dependencies"]
        except (KeyError, TypeError, json.JSONDecodeError) as exc:
            raise RuntimeError("Transformation runtime requires valid dependency pins.") from exc
        approved_sources = [
            str(item.get("object_name") or "").strip()
            for item in dependencies
            if isinstance(item, dict) and str(item.get("object_name") or "").strip()
        ]
        if not approved_sources:
            raise RuntimeError("Transformation runtime has no approved source objects.")
        artifact.update({
            "source_table": approved_sources[0],
            "approved_source_tables": approved_sources,
        })
    if stage == "SOURCE_TO_BRONZE":
        target_parts = str(artifact.get("target_table") or "").split(".")
        if len(target_parts) == 3:
            artifact.update({
                "bronze_catalog": target_parts[0],
                "bronze_schema": target_parts[1],
            })
    result_key = {
        "SOURCE_TO_BRONZE": "bronze_generation_results",
        "BRONZE_TO_SILVER": "silver_generation_results",
        "SILVER_TO_GOLD": "gold_generation_results",
    }.get(stage)
    if not result_key:
        raise ValueError(f"Unsupported runtime processing stage: {stage}")
    return {
        "run_id": run["run_id"],
        "target_warehouse": str(execution_spec["target_platform"]).lower(),
        "metadata_runtime_context": runtime_context,
        result_key: [artifact],
        f"{result_key.removesuffix('_generation_results')}_review_artifact": {"items": [artifact]},
    }


def _execute_registered_artifact(
    run: Dict[str, Any],
    obj: Dict[str, Any],
    runtime_context: Dict[str, Any],
    mapping: Optional[Dict[str, Any]] = None,
    on_submitted=None,
) -> Dict[str, Any]:
    state, stage, platform = _registered_artifact_state(
        run, obj, runtime_context, mapping=mapping
    )
    if platform == "DATABRICKS":
        from services.databricks_runtime import (
            run_databricks_bronze_scripts,
            run_databricks_gold_scripts,
            run_databricks_silver_scripts,
        )

        runner = {
            "SOURCE_TO_BRONZE": run_databricks_bronze_scripts,
            "BRONZE_TO_SILVER": run_databricks_silver_scripts,
            "SILVER_TO_GOLD": run_databricks_gold_scripts,
        }[stage]
        return runner(
            state,
            review_artifact=next(
                value for key, value in state.items() if key.endswith("_review_artifact")
            ),
            approved_only=True,
            on_submitted=on_submitted,
        )
    if platform == "SNOWFLAKE":
        if stage == "SOURCE_TO_BRONZE":
            from services.snowflake_bronze_runtime import run_snowflake_bronze_scripts

            return run_snowflake_bronze_scripts(state, review_artifact=state["bronze_review_artifact"], approved_only=True)
        if stage == "BRONZE_TO_SILVER":
            from services.snowflake_silver_runtime import run_snowflake_silver_scripts

            return run_snowflake_silver_scripts(state, review_artifact=state["silver_review_artifact"], approved_only=True)
        from services.snowflake_gold_runtime import run_snowflake_gold_scripts

        return run_snowflake_gold_scripts(state)
    raise ValueError(f"Unsupported runtime target platform: {platform}")


def _registered_artifact_state(
    run: Dict[str, Any],
    obj: Dict[str, Any],
    runtime_context: Dict[str, Any],
    mapping: Optional[Dict[str, Any]] = None,
) -> tuple[Dict[str, Any], str, str]:
    from utilis.generated_code_paths import verified_execution_artifact

    try:
        execution_spec = json.loads(str(obj.get("execution_spec_json") or ""))
    except json.JSONDecodeError as exc:
        raise RuntimeError("The active execution specification is invalid.") from exc
    platform = str(execution_spec.get("target_platform") or "").upper()
    artifact_path = verified_execution_artifact(execution_spec, platform=platform.lower())
    state = _runtime_state(
        run, obj, execution_spec, str(artifact_path), runtime_context, mapping=mapping
    )
    stage = str(obj.get("processing_stage") or "").upper()
    return state, stage, platform


def _assert_execution_completed(
    result: Dict[str, Any], obj: Dict[str, Any], runtime_context: Dict[str, Any]
) -> Dict[str, Any]:
    """Accept only explicit, per-artifact target completion evidence."""
    from services.metadata_contracts import validate_execution_result
    try:
        execution_spec = json.loads(str(obj.get("execution_spec_json") or ""))
    except json.JSONDecodeError as exc:
        raise RuntimeError("The active execution specification is invalid.") from exc
    platform = str(execution_spec.get("target_platform") or "").lower()
    layer = {
        "SOURCE_TO_BRONZE": "bronze",
        "BRONZE_TO_SILVER": "silver",
        "SILVER_TO_GOLD": "gold",
    }.get(str(obj.get("processing_stage") or "").upper())
    if platform not in {"databricks", "snowflake"} or not layer:
        raise RuntimeError("Runtime completion evidence has an unsupported target or processing stage.")
    status_key = f"{platform}_{layer}_execution_status"
    results_key = f"{platform}_{layer}_execution_results"
    if str(result.get(status_key) or "").upper() != "COMPLETED":
        raise RuntimeError(f"Target execution did not complete successfully: {status_key}={result.get(status_key)!r}.")
    items = [item for item in result.get(results_key) or [] if isinstance(item, dict)]
    if len(items) != 1:
        raise RuntimeError("Metadata runtime requires completion evidence for exactly one registered artifact.")
    item_status = str(items[0].get("status") or "").upper()
    expected_status = "SUCCESS" if platform == "databricks" else "COMPLETED"
    if item_status != expected_status:
        raise RuntimeError(f"Registered artifact execution was not successful: status={item_status or 'MISSING'}.")
    if platform == "databricks" and str(items[0].get("verification_status") or "").upper() != "VERIFIED":
        raise RuntimeError("Databricks execution output was not independently verified.")
    execution_result = validate_execution_result(
        items[0].get("execution_result"), runtime_context=runtime_context
    )
    if platform == "databricks":
        expected_commit = rf"delta:{re.escape(str(runtime_context.get('target_table') or ''))}:v[0-9]+"
        if not re.fullmatch(expected_commit, str(execution_result.get("target_commit_id") or ""), re.IGNORECASE):
            raise RuntimeError("Databricks execution receipt is not bound to the exact Delta target commit.")
    return {
        "platform": platform,
        "layer": layer,
        "artifact_result": items[0],
        "execution_result": execution_result,
    }


def _assert_blocking_validation(
    result: Dict[str, Any], obj: Dict[str, Any], execution_result: Dict[str, Any]
) -> Dict[str, Any]:
    from services.metadata_contracts import canonical_json_hash

    try:
        policy = json.loads(str(obj.get("validation_policy_json") or "{}"))
        spec = json.loads(str(obj.get("execution_spec_json") or "{}"))
    except json.JSONDecodeError as exc:
        raise RuntimeError("The active validation or execution contract is invalid.") from exc
    if not policy:
        return {"status": "PASSED", "policy": "NONE"}
    rule_results = execution_result.get("validation_results")
    configured_rules = policy.get("rules") if isinstance(policy, dict) else None
    if not isinstance(configured_rules, list) or not configured_rules:
        raise RuntimeError("Blocking validation policy must contain an explicit non-empty rules array.")
    expected_rules = {
        str(rule.get("rule_type") or rule.get("rule") or "").upper(): rule
        for rule in configured_rules
        if isinstance(rule, dict) and str(rule.get("rule_type") or rule.get("rule") or "").strip()
    }
    returned_rules = {
        str(rule.get("rule_type") or rule.get("rule") or "").upper(): rule
        for rule in (rule_results or [])
        if isinstance(rule, dict) and str(rule.get("rule_type") or rule.get("rule") or "").strip()
    }
    if (
        spec.get("embedded_blocking_validation") is True
        and str(spec.get("validation_policy_hash") or "") == canonical_json_hash(policy)
        and str(execution_result.get("validation_policy_hash") or "")
        == canonical_json_hash(policy)
        and isinstance(rule_results, list)
        and len(expected_rules) == len(configured_rules)
        and set(returned_rules) == set(expected_rules)
        and len(returned_rules) == len(rule_results)
        and all(
            str(rule.get("status") or "").upper() == "PASSED"
            and rule.get("observed_value") is not None
            and rule.get("threshold_value") == expected_rules[name].get("threshold_value", 0)
            for name, rule in returned_rules.items()
        )
    ):
        return {
            "status": "PASSED",
            "policy": "EMBEDDED_IN_VERIFIED_ARTIFACT",
            "policy_hash": canonical_json_hash(policy),
            "rule_results": rule_results,
        }
    raise RuntimeError("Blocking validation evidence is missing or did not pass.")


def _execute_with_lease_heartbeat(
    repository: MetadataRepository,
    *,
    run: Dict[str, Any],
    obj: Dict[str, Any],
    runtime_context: Dict[str, Any],
    mapping: Dict[str, Any],
    queue_id: int,
    worker_id: str,
    lease_seconds: int,
    on_submitted: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    stop = threading.Event()
    heartbeat_error: list[BaseException] = []
    interval = max(5.0, min(60.0, max(30, int(lease_seconds)) / 3.0))

    def renew() -> None:
        while not stop.wait(interval):
            try:
                repository.heartbeat_queue_item(
                    queue_id=queue_id, worker_id=worker_id, lease_seconds=lease_seconds
                )
            except BaseException as exc:  # the owner must fail closed if its lease cannot renew
                heartbeat_error.append(exc)
                stop.set()

    thread = threading.Thread(target=renew, name=f"metadata-lease-{queue_id}", daemon=True)
    thread.start()
    try:
        result = _execute_registered_artifact(run, obj, runtime_context, mapping, on_submitted)
    finally:
        stop.set()
        thread.join(timeout=min(5.0, interval))
    if heartbeat_error:
        raise RuntimeError("Queue lease was lost during target execution.") from heartbeat_error[0]
    repository.heartbeat_queue_item(
        queue_id=queue_id, worker_id=worker_id, lease_seconds=lease_seconds
    )
    return result


def _retryable_execution_error(exc: BaseException) -> bool:
    return (
        bool(getattr(exc, "retryable", False))
        or isinstance(exc, (ConnectionError, TimeoutError))
        or type(exc).__name__ in {"OperationalError", "InterfaceError"}
        or "dependency is not committed" in str(exc).lower()
    )


def _execute_registered_artifact_batch(
    prepared: list[Dict[str, Any]],
    progress_state: Dict[str, Any],
    on_submitted: Callable[[str], None],
) -> Dict[str, Any]:
    from services.databricks_runtime import (
        run_databricks_bronze_scripts,
        run_databricks_gold_scripts,
        run_databricks_silver_scripts,
    )

    states: list[Dict[str, Any]] = []
    stages: set[str] = set()
    for item in prepared:
        state, stage, platform = _registered_artifact_state(
            item["run"], item["obj"], item["runtime_context"], mapping=item["mapping"]
        )
        if platform != "DATABRICKS":
            raise ValueError("Metadata batch execution is supported only for Databricks.")
        states.append(state)
        stages.add(stage)
    if len(stages) != 1:
        raise RuntimeError("A Databricks metadata batch must contain exactly one processing stage.")

    stage = stages.pop()
    layer, result_key, runner = {
        "SOURCE_TO_BRONZE": ("bronze", "bronze_generation_results", run_databricks_bronze_scripts),
        "BRONZE_TO_SILVER": ("silver", "silver_generation_results", run_databricks_silver_scripts),
        "SILVER_TO_GOLD": ("gold", "gold_generation_results", run_databricks_gold_scripts),
    }[stage]
    scripts = [script for state in states for script in state.get(result_key) or []]
    if len(scripts) != len(prepared):
        raise RuntimeError("Every claimed metadata item must resolve to exactly one registered artifact.")
    batch_state = {
        **progress_state,
        "run_id": str(progress_state.get("run_id") or ""),
        "target_warehouse": "databricks",
        "metadata_runtime_batch": True,
        "metadata_runtime_context": dict(prepared[0]["runtime_context"]),
        "_metadata_runtime_scripts": scripts,
    }
    return runner(batch_state, approved_only=False, on_submitted=on_submitted)


def process_metadata_work_batch(
    repository: MetadataRepository,
    *,
    worker_id: str,
    progress_state: Dict[str, Any],
    lease_seconds: int = 300,
    logical_work_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Execute all currently ready Databricks items in one serverless submission."""
    from services.databricks_runtime import DatabricksBatchExecutionError

    prepared: list[Dict[str, Any]] = []
    outcomes: list[Dict[str, Any]] = []
    stage: Optional[str] = None
    claim_many = getattr(repository, "claim_queue_items", None)
    if callable(claim_many):
        claimed_items = claim_many(
            worker_id=worker_id,
            lease_seconds=lease_seconds,
            logical_work_id=logical_work_id,
            limit=100,
        )
    else:
        claimed_items = []
        while True:
            queue_item = repository.claim_next_queue_item(
                worker_id=worker_id,
                lease_seconds=lease_seconds,
                logical_work_id=logical_work_id,
            )
            if not queue_item:
                break
            claimed_items.append(queue_item)
    if not claimed_items and repository.release_ready_downstream_from_successes(logical_work_id=logical_work_id):
        if callable(claim_many):
            claimed_items = claim_many(
                worker_id=worker_id,
                lease_seconds=lease_seconds,
                logical_work_id=logical_work_id,
                limit=100,
            )
        else:
            while True:
                queue_item = repository.claim_next_queue_item(
                    worker_id=worker_id,
                    lease_seconds=lease_seconds,
                    logical_work_id=logical_work_id,
                )
                if not queue_item:
                    break
                claimed_items.append(queue_item)

    recover_many = getattr(repository, "recover_committed_queue_items", None)
    recovered_by_queue = recover_many(
        queue_ids=[int(item["queue_id"]) for item in claimed_items], worker_id=worker_id
    ) if callable(recover_many) and claimed_items else {}
    unrecovered = []
    for queue_item in claimed_items:
        queue_id = int(queue_item["queue_id"])
        recovered = recovered_by_queue.get(queue_id) if callable(recover_many) else repository.recover_committed_queue_item(
            queue_id=queue_id, worker_id=worker_id
        )
        if recovered:
            outcomes.append({"queue": queue_item, "run": recovered, "status": "RECOVERED_SUCCESS"})
            continue
        unrecovered.append(queue_item)
    create_many = getattr(repository, "create_run_attempts", None)
    contexts = create_many(
        unrecovered, pipeline_name="metadata_runtime_worker", worker_id=worker_id
    ) if callable(create_many) and unrecovered else [
        repository.create_run_attempt(
            queue_item, pipeline_name="metadata_runtime_worker", worker_id=worker_id
        )
        for queue_item in unrecovered
    ]
    for queue_item, context in zip(unrecovered, contexts):
        run = context["run"]
        obj = context["ingestion_object"]
        item_stage = str(obj.get("processing_stage") or "").upper()
        if stage is None:
            stage = item_stage
        if item_stage != stage:
            raise RuntimeError("Ready metadata work unexpectedly crossed a processing-stage boundary.")
        if not context.get("metadata_snapshot_matches", True):
            raise RuntimeError("Queued metadata snapshot is no longer the active executable configuration.")
        if obj.get("watermark_column") or str(obj.get("checkpoint_type") or "").strip():
            raise RuntimeError(
                "Stateful metadata execution requires the generated artifact checkpoint-output protocol, which is not configured."
            )
        runtime_context = {
            **context["runtime_context"],
            "resumed_attempt": bool(context.get("resumed_attempt")),
        }
        prepared.append({
            "queue": queue_item,
            "run": run,
            "obj": obj,
            "mapping": context["mapping"],
            "runtime_context": runtime_context,
        })

    if prepared:
        assert_many = getattr(repository, "assert_runtime_dependencies_batch", None)
        if callable(assert_many):
            assert_many(
                [item["obj"] for item in prepared],
                logical_work_id=str(prepared[0]["run"].get("logical_work_id") or ""),
            )
        else:
            for item in prepared:
                repository.assert_runtime_dependencies(
                    item["obj"], logical_work_id=str(item["run"].get("logical_work_id") or "")
                )
        heartbeat_many = getattr(repository, "heartbeat_queue_items", None)
        if callable(heartbeat_many):
            heartbeat_many(
                queue_ids=[int(item["queue"]["queue_id"]) for item in prepared],
                worker_id=worker_id,
                lease_seconds=lease_seconds,
            )
        else:
            for item in prepared:
                repository.heartbeat_queue_item(
                    queue_id=int(item["queue"]["queue_id"]),
                    worker_id=worker_id,
                    lease_seconds=lease_seconds,
                )

    if not prepared:
        return {"outcomes": outcomes, "progress_state": progress_state} if outcomes else None

    stop = threading.Event()
    heartbeat_error: list[BaseException] = []
    interval = max(5.0, min(60.0, max(30, int(lease_seconds)) / 3.0))

    def renew() -> None:
        while not stop.wait(interval):
            try:
                heartbeat_many = getattr(repository, "heartbeat_queue_items", None)
                if callable(heartbeat_many):
                    heartbeat_many(
                        queue_ids=[int(item["queue"]["queue_id"]) for item in prepared],
                        worker_id=worker_id,
                        lease_seconds=lease_seconds,
                    )
                else:
                    def heartbeat(item: Dict[str, Any]) -> None:
                        repository.heartbeat_queue_item(
                            queue_id=int(item["queue"]["queue_id"]),
                            worker_id=worker_id,
                            lease_seconds=lease_seconds,
                        )

                    with ThreadPoolExecutor(max_workers=min(4, len(prepared))) as executor:
                        list(executor.map(heartbeat, prepared))
            except BaseException as exc:
                heartbeat_error.append(exc)
                stop.set()

    thread = threading.Thread(target=renew, name="metadata-batch-leases", daemon=True)
    thread.start()
    batch_results: list[Dict[str, Any]] = []
    batch_error: Optional[BaseException] = None

    def on_submitted(target_write_id: str) -> None:
        if not str(target_write_id or "").strip():
            raise RuntimeError("The target platform did not return an execution receipt.")

        update_many = getattr(repository, "update_run_phases", None)
        if callable(update_many):
            update_many(
                phase="TARGET_SUBMITTED",
                worker_id=worker_id,
                updates=[{
                    "run_id": str(item["run"]["run_id"]),
                    "queue_id": int(item["queue"]["queue_id"]),
                    "target_write_id": str(target_write_id),
                    "target_commit_status": "SUBMITTED",
                } for item in prepared],
            )
        else:
            def mark_submitted(item: Dict[str, Any]) -> None:
                repository.update_run_phase(
                    str(item["run"]["run_id"]),
                    "TARGET_SUBMITTED",
                    queue_id=int(item["queue"]["queue_id"]),
                    worker_id=worker_id,
                    target_write_id=str(target_write_id),
                    target_commit_status="SUBMITTED",
                )

            with ThreadPoolExecutor(max_workers=min(4, len(prepared))) as executor:
                list(executor.map(mark_submitted, prepared))

    try:
        try:
            result = _execute_registered_artifact_batch(prepared, progress_state, on_submitted)
            layer = {"SOURCE_TO_BRONZE": "bronze", "BRONZE_TO_SILVER": "silver", "SILVER_TO_GOLD": "gold"}[stage or ""]
            batch_results = [
                item for item in result.get(f"databricks_{layer}_execution_results") or []
                if isinstance(item, dict)
            ]
            progress_state = result
        except DatabricksBatchExecutionError as exc:
            batch_error = exc
            batch_results = list(exc.results)
        except BaseException as exc:
            batch_error = exc
    finally:
        stop.set()
        thread.join(timeout=min(5.0, interval))

    results_by_run = {
        str(item.get("runtime_run_id") or ""): item
        for item in batch_results
        if str(item.get("runtime_run_id") or "")
    }
    if batch_error and bool(getattr(batch_error, "preserve_attempt", False)):
        for item in prepared:
            repository.record_run_error(
                run=item["run"],
                error_stage="WRITE",
                error=batch_error,
                retryable=True,
                detail={"operation": "DATABRICKS_BATCH_SUBMISSION"},
                worker_id=worker_id,
            )
            repository.release_queue_for_same_attempt_resume(
                queue_id=int(item["queue"]["queue_id"]),
                worker_id=worker_id,
                message=str(batch_error),
            )
        raise batch_error
    layer = {"SOURCE_TO_BRONZE": "bronze", "BRONZE_TO_SILVER": "silver", "SILVER_TO_GOLD": "gold"}[stage or ""]

    bulk_finalization = all(
        str((results_by_run.get(str(item["run"]["run_id"])) or {}).get("status") or "").upper() == "SUCCESS"
        for item in prepared
    ) and not batch_error and not heartbeat_error
    if bulk_finalization:
        verified_items = []
        try:
            for item in prepared:
                queue_item = item["queue"]
                run = item["run"]
                obj = item["obj"]
                script_result = results_by_run[str(run["run_id"])]
                result = {
                    f"databricks_{layer}_execution_status": "COMPLETED",
                    f"databricks_{layer}_execution_results": [script_result],
                }
                execution_evidence = _assert_execution_completed(result, obj, item["runtime_context"])
                validation_evidence = _assert_blocking_validation(
                    result, obj, execution_evidence["execution_result"]
                )
                verified_items.append({
                    **item,
                    "phase_update": {
                        "run_id": str(run["run_id"]),
                        "queue_id": int(queue_item["queue_id"]),
                        "rows_read": execution_evidence["execution_result"].get("rows_read"),
                        "rows_written": execution_evidence["execution_result"].get("rows_written"),
                        "target_write_id": str(execution_evidence["execution_result"]["target_commit_id"]),
                        "target_commit_status": "COMMITTED",
                        "validation_status": "PASSED",
                        "validation_summary_json": json.dumps(
                            {"target_execution": execution_evidence, "blocking_validation": validation_evidence},
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                        "watermark_commit_status": "SKIPPED",
                    },
                })
        except BaseException:
            bulk_finalization = False

    if bulk_finalization:
        begin_many = getattr(repository, "begin_queue_finalizations", None)
        update_many = getattr(repository, "update_run_phases", None)
        finalize_many = getattr(repository, "finalize_successful_runs", None)
        if callable(begin_many) and callable(update_many) and callable(finalize_many):
            begin_many(
                queue_ids=[int(item["queue"]["queue_id"]) for item in verified_items],
                worker_id=worker_id,
            )
            update_many(
                phase="TARGET_WRITTEN",
                updates=[item["phase_update"] for item in verified_items],
                worker_id=worker_id,
            )
            finalize_many(
                attempts=[{
                    "run_id": str(item["run"]["run_id"]),
                    "queue_id": int(item["queue"]["queue_id"]),
                } for item in verified_items],
                worker_id=worker_id,
            )

            downstream_status = "COMPLETED"
            try:
                release_many = getattr(repository, "enqueue_ready_downstream_batch", None)
                if callable(release_many):
                    release_many(completed=[{
                        "completed_object": item["obj"],
                        "logical_work_id": str(item["run"].get("logical_work_id") or ""),
                        "parent_work_scope": json.loads(str(item["queue"].get("work_scope_json") or "{}")),
                    } for item in verified_items])
                else:
                    for item in verified_items:
                        repository.enqueue_ready_downstream(
                            completed_object=item["obj"],
                            logical_work_id=str(item["run"].get("logical_work_id") or ""),
                            parent_work_scope=json.loads(str(item["queue"].get("work_scope_json") or "{}")),
                        )
            except Exception as release_error:
                downstream_status = "PENDING_RECOVERY"
                for item in verified_items:
                    repository.record_run_error(
                        run=item["run"], error_stage="FINALIZE", error=release_error,
                        retryable=True, detail={"operation": "DOWNSTREAM_RELEASE"},
                    )
            outcomes.extend({
                "queue": item["queue"], "run": item["run"], "status": "SUCCESS",
                "downstream_release_status": downstream_status,
            } for item in verified_items)
            return {"outcomes": outcomes, "progress_state": progress_state}

    def finalize(item: Dict[str, Any]) -> Dict[str, Any]:
        queue_item = item["queue"]
        run = item["run"]
        obj = item["obj"]
        runtime_context = item["runtime_context"]
        target_committed = False
        try:
            if heartbeat_error:
                raise RuntimeError("Queue lease was lost during target execution.") from heartbeat_error[0]
            script_result = results_by_run.get(str(run["run_id"]))
            if not script_result:
                raise RuntimeError(str(batch_error or "Databricks batch omitted this artifact result."))
            if str(script_result.get("status") or "").upper() != "SUCCESS":
                raise RuntimeError(str(script_result.get("error") or batch_error or "Databricks artifact execution failed."))
            result = {
                f"databricks_{layer}_execution_status": "COMPLETED",
                f"databricks_{layer}_execution_results": [script_result],
            }
            execution_evidence = _assert_execution_completed(result, obj, runtime_context)
            validation_evidence = _assert_blocking_validation(
                result, obj, execution_evidence["execution_result"]
            )
            target_committed = True
            repository.begin_queue_finalization(
                queue_id=int(queue_item["queue_id"]), worker_id=worker_id
            )
            repository.update_run_phase(
                str(run["run_id"]),
                "TARGET_WRITTEN",
                queue_id=int(queue_item["queue_id"]),
                worker_id=worker_id,
                rows_read=execution_evidence["execution_result"].get("rows_read"),
                rows_written=execution_evidence["execution_result"].get("rows_written"),
                target_write_id=str(execution_evidence["execution_result"]["target_commit_id"]),
                target_commit_status="COMMITTED",
                validation_status="PASSED",
                validation_summary_json=json.dumps(
                    {"target_execution": execution_evidence, "blocking_validation": validation_evidence},
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                watermark_commit_status="SKIPPED",
            )
            repository.finalize_successful_run(
                run_id=str(run["run_id"]),
                queue_id=int(queue_item["queue_id"]),
                worker_id=worker_id,
            )
            downstream_release_status = "COMPLETED"
            try:
                repository.enqueue_ready_downstream(
                    completed_object=obj,
                    logical_work_id=str(run.get("logical_work_id") or ""),
                    parent_work_scope=json.loads(str(queue_item.get("work_scope_json") or "{}")),
                )
            except Exception as release_error:
                downstream_release_status = "PENDING_RECOVERY"
                repository.record_run_error(
                    run=run,
                    error_stage="FINALIZE",
                    error=release_error,
                    retryable=True,
                    detail={"operation": "DOWNSTREAM_RELEASE"},
                )
            return {
                "queue": queue_item,
                "run": run,
                "status": "SUCCESS",
                "downstream_release_status": downstream_release_status,
            }
        except BaseException as exc:
            retryable = _retryable_execution_error(exc)
            repository.record_run_error(
                run=run,
                error_stage="FINALIZE" if target_committed else "WRITE",
                error=exc,
                retryable=retryable,
                detail={"ingestion_object_id": run.get("ingestion_object_id")},
                worker_id=worker_id,
            )
            if not target_committed:
                repository.finalize_failed_run(
                    run=run,
                    worker_id=worker_id,
                    retryable=retryable,
                    message=str(exc),
                )
            return {"queue": queue_item, "run": run, "status": "FAILED", "error": str(exc)}

    # ponytail: Databricks control rows are independent; bounded parallelism removes per-row API latency.
    with ThreadPoolExecutor(max_workers=min(4, len(prepared))) as executor:
        outcomes.extend(executor.map(finalize, prepared))

    return {"outcomes": outcomes, "progress_state": progress_state}


def process_next_metadata_work(
    repository: MetadataRepository,
    *,
    worker_id: str,
    lease_seconds: int = 300,
    logical_work_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Claim and execute one stateless, registered metadata work item."""
    queue_item = repository.claim_next_queue_item(
        worker_id=worker_id,
        lease_seconds=lease_seconds,
        logical_work_id=logical_work_id,
    )
    if not queue_item:
        repository.release_ready_downstream_from_successes(logical_work_id=logical_work_id)
        queue_item = repository.claim_next_queue_item(
            worker_id=worker_id,
            lease_seconds=lease_seconds,
            logical_work_id=logical_work_id,
        )
        if not queue_item:
            return None
    recovered = repository.recover_committed_queue_item(
        queue_id=int(queue_item["queue_id"]), worker_id=worker_id
    )
    if recovered:
        return {"queue": queue_item, "run": recovered, "status": "RECOVERED_SUCCESS"}
    context: Optional[Dict[str, Any]] = None
    target_committed = False
    error_stage = "FINALIZE"
    try:
        context = repository.create_run_attempt(
            queue_item, pipeline_name="metadata_runtime_worker", worker_id=worker_id
        )
        run = context["run"]
        obj = context["ingestion_object"]
        runtime_context = {
            **context["runtime_context"],
            "resumed_attempt": bool(context.get("resumed_attempt")),
        }
        error_stage = "VALIDATE"
        if not context.get("metadata_snapshot_matches", True):
            raise RuntimeError("Queued metadata snapshot is no longer the active executable configuration.")
        repository.assert_runtime_dependencies(obj, logical_work_id=str(run.get("logical_work_id") or ""))
        if obj.get("watermark_column") or str(obj.get("checkpoint_type") or "").strip():
            raise RuntimeError(
                "Stateful metadata execution requires the generated artifact checkpoint-output protocol, which is not configured."
            )
        repository.heartbeat_queue_item(
            queue_id=int(queue_item["queue_id"]), worker_id=worker_id, lease_seconds=lease_seconds
        )
        error_stage = "WRITE"
        receipt = {"value": ""}

        def on_submitted(target_write_id: str) -> None:
            if not str(target_write_id or "").strip():
                raise RuntimeError("The target platform did not return an execution receipt.")
            receipt["value"] = str(target_write_id)
            repository.update_run_phase(
                str(run["run_id"]),
                "TARGET_SUBMITTED",
                queue_id=int(queue_item["queue_id"]),
                worker_id=worker_id,
                target_write_id=receipt["value"],
                target_commit_status="SUBMITTED",
            )

        result = _execute_with_lease_heartbeat(
            repository,
            run=run,
            obj=obj,
            runtime_context=runtime_context,
            mapping=context["mapping"],
            queue_id=int(queue_item["queue_id"]),
            worker_id=worker_id,
            lease_seconds=lease_seconds,
            on_submitted=on_submitted,
        )
        execution_evidence = _assert_execution_completed(result, obj, runtime_context)
        validation_evidence = _assert_blocking_validation(
            result, obj, execution_evidence["execution_result"]
        )
        # A verified target receipt is durable even if the control-plane update below fails.
        target_committed = True
        error_stage = "FINALIZE"
        repository.begin_queue_finalization(
            queue_id=int(queue_item["queue_id"]), worker_id=worker_id
        )
        repository.update_run_phase(
            str(run["run_id"]),
            "TARGET_WRITTEN",
            queue_id=int(queue_item["queue_id"]),
            worker_id=worker_id,
            rows_read=execution_evidence["execution_result"].get("rows_read"),
            rows_written=execution_evidence["execution_result"].get("rows_written"),
            target_write_id=str(execution_evidence["execution_result"]["target_commit_id"]),
            target_commit_status="COMMITTED",
            validation_status="PASSED",
            validation_summary_json=json.dumps(
                {"target_execution": execution_evidence, "blocking_validation": validation_evidence},
                sort_keys=True,
                separators=(",", ":"),
            ),
            watermark_commit_status="SKIPPED",
        )
        repository.finalize_successful_run(
            run_id=str(run["run_id"]),
            queue_id=int(queue_item["queue_id"]),
            worker_id=worker_id,
        )
        downstream_release_status = "COMPLETED"
        try:
            repository.enqueue_ready_downstream(
                completed_object=obj,
                logical_work_id=str(run.get("logical_work_id") or ""),
                parent_work_scope=json.loads(str(queue_item.get("work_scope_json") or "{}")),
            )
        except Exception as release_error:
            downstream_release_status = "PENDING_RECOVERY"
            repository.record_run_error(
                run=run,
                error_stage="FINALIZE",
                error=release_error,
                retryable=True,
                detail={"operation": "DOWNSTREAM_RELEASE"},
            )
        return {
            "queue": queue_item,
            "run": run,
            "execution": result,
            "status": "SUCCESS",
            "downstream_release_status": downstream_release_status,
        }
    except Exception as exc:
        if context:
            run = context["run"]
            retryable = (
                bool(getattr(exc, "retryable", False))
                or isinstance(exc, (ConnectionError, TimeoutError))
                or type(exc).__name__ in {"OperationalError", "InterfaceError"}
                or "dependency is not committed" in str(exc).lower()
            )
            repository.record_run_error(
                run=run,
                error_stage=error_stage,
                error=exc,
                retryable=retryable,
                detail={"ingestion_object_id": run.get("ingestion_object_id")},
                worker_id=worker_id,
            )
            if bool(getattr(exc, "preserve_attempt", False)):
                repository.release_queue_for_same_attempt_resume(
                    queue_id=int(queue_item["queue_id"]),
                    worker_id=worker_id,
                    message=str(exc),
                )
                raise
            if target_committed:
                raise
            repository.finalize_failed_run(
                run=run,
                worker_id=worker_id,
                retryable=retryable,
                message=str(exc),
            )
        raise
