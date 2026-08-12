from __future__ import annotations

import base64
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional
from urllib import error, request

from services.external_execution_progress import save_external_execution_progress
from utilis.logger import logger


class DatabricksTransientError(RuntimeError):
    retryable = True


class DatabricksAmbiguousSubmissionError(DatabricksTransientError):
    preserve_attempt = True


class DatabricksBatchExecutionError(RuntimeError):
    def __init__(self, message: str, results: List[Dict[str, Any]]) -> None:
        super().__init__(message)
        self.results = results


def _env_flag(*names: str) -> bool:
    for name in names:
        value = os.getenv(name)
        if value is None:
            continue
        if str(value).strip().lower() in {"1", "true", "yes", "on"}:
            return True
    return False


def databricks_execution_enabled(layer: str) -> bool:
    layer_name = str(layer or "").strip().lower()
    if not layer_name:
        return False
    return _env_flag(f"ATHENA_EXECUTE_DATABRICKS_{layer_name.upper()}")


def databricks_bronze_execution_enabled() -> bool:
    return databricks_execution_enabled("bronze")


def databricks_silver_execution_enabled() -> bool:
    return databricks_execution_enabled("silver")


def databricks_gold_execution_enabled() -> bool:
    return databricks_execution_enabled("gold")


def _gold_partial_success_enabled(layer: str) -> bool:
    if str(layer or "").strip().lower() != "gold":
        return False
    return str(os.getenv("ATHENA_DATABRICKS_GOLD_ALLOW_PARTIAL_SUCCESS", "true")).strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }


def _api_base() -> str:
    host = str(os.getenv("DATABRICKS_HOST") or "").strip().rstrip("/")
    if not host:
        raise RuntimeError("DATABRICKS_HOST is required to execute Databricks jobs.")
    if not host.startswith("http://") and not host.startswith("https://"):
        host = f"https://{host}"
    return host


def _auth_token() -> str:
    token = str(os.getenv("DATABRICKS_TOKEN") or "").strip()
    if not token:
        raise RuntimeError("DATABRICKS_TOKEN is required to execute Databricks jobs.")
    return token


def _databricks_timeout_seconds() -> int:
    raw = os.getenv("ATHENA_DATABRICKS_REQUEST_TIMEOUT_SECONDS", "180")
    try:
        return max(1, int(raw))
    except ValueError:
        return 180


def _databricks_poll_interval_seconds() -> float:
    raw = os.getenv("ATHENA_DATABRICKS_POLL_INTERVAL_SECONDS", "10")
    try:
        return max(1.0, float(raw))
    except ValueError:
        return 10.0


def _databricks_run_timeout_seconds() -> int:
    raw = os.getenv("ATHENA_DATABRICKS_RUN_TIMEOUT_SECONDS", "7200")
    try:
        return max(1, int(raw))
    except ValueError:
        return 7200


def _databricks_execution_mode(layer: str) -> str:
    layer_name = str(layer or "").strip().upper()
    raw = (
        os.getenv(f"ATHENA_DATABRICKS_{layer_name}_EXECUTION_MODE")
        or os.getenv("ATHENA_DATABRICKS_EXECUTION_MODE")
        or "batch"
    )
    mode = str(raw or "").strip().lower()
    return mode if mode in {"batch", "per_script"} else "batch"


def _workspace_root() -> str:
    return str(os.getenv("DATABRICKS_WORKSPACE_ROOT") or "/Shared/Athena").rstrip("/")


def _workspace_path(layer: str, run_id: str, script_name: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9_]+", "_", str(run_id or "run")).strip("_")[:48] or "run"
    layer_name = str(layer or "stage").strip().lower()
    stem = re.sub(r"[^a-zA-Z0-9_]+", "_", Path(script_name).stem).strip("_")[:128] or "script"
    return f"{_workspace_root()}/{slug}/{layer_name}/{stem}"


def _request_json(method: str, path: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"{_api_base()}{path}"
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    req = request.Request(
        url,
        data=data,
        method=method.upper(),
        headers={
            "Authorization": f"Bearer {_auth_token()}",
            "Content-Type": "application/json",
        },
    )
    try:
        with request.urlopen(req, timeout=_databricks_timeout_seconds()) as response:
            body = response.read().decode("utf-8").strip()
            return json.loads(body) if body else {}
    except error.HTTPError as exc:
        message = exc.read().decode("utf-8", "ignore").strip()
        exception_type = DatabricksTransientError if exc.code == 429 or exc.code >= 500 else RuntimeError
        raise exception_type(
            f"Databricks API request failed ({method} {path}): {message or exc.reason}"
        ) from exc
    except (error.URLError, TimeoutError) as exc:
        raise DatabricksTransientError(
            f"Databricks API request failed ({method} {path}): {exc}"
        ) from exc
    except Exception as exc:
        raise RuntimeError(f"Databricks API request failed ({method} {path}): {exc}") from exc


def _workspace_import_notebook(notebook_path: str, content: str) -> Dict[str, Any]:
    parent_path = str(Path(notebook_path).parent).replace("\\", "/")
    if parent_path and parent_path not in {".", "/"}:
        _request_json("POST", "/api/2.0/workspace/mkdirs", {"path": parent_path})
    encoded = base64.b64encode(content.encode("utf-8")).decode("ascii")
    return _request_json(
        "POST",
        "/api/2.0/workspace/import",
        {
            "path": notebook_path,
            "format": "SOURCE",
            "language": "PYTHON",
            "overwrite": True,
            "content": encoded,
        },
    )


def _workspace_import_file(file_path: str, content: str) -> Dict[str, Any]:
    parent_path = str(Path(file_path).parent).replace("\\", "/")
    if parent_path and parent_path not in {".", "/"}:
        _request_json("POST", "/api/2.0/workspace/mkdirs", {"path": parent_path})
    encoded = base64.b64encode(content.encode("utf-8")).decode("ascii")
    return _request_json(
        "POST",
        "/api/2.0/workspace/import",
        {
            "path": file_path,
            "format": "RAW",
            "overwrite": True,
            "content": encoded,
        },
    )


def _databricks_compute_mode() -> str:
    mode = str(os.getenv("ATHENA_DATABRICKS_COMPUTE_MODE") or "classic").strip().lower()
    return mode or "classic"


def _cluster_spec() -> Dict[str, Any]:
    if _databricks_compute_mode() == "serverless":
        return {}

    existing_cluster_id = str(os.getenv("DATABRICKS_CLUSTER_ID") or os.getenv("DATABRICKS_EXISTING_CLUSTER_ID") or "").strip()
    if existing_cluster_id:
        return {"existing_cluster_id": existing_cluster_id}

    spark_version = str(os.getenv("DATABRICKS_NEW_CLUSTER_SPARK_VERSION") or "").strip()
    node_type_id = str(os.getenv("DATABRICKS_NEW_CLUSTER_NODE_TYPE_ID") or "").strip()
    if spark_version and node_type_id:
        new_cluster: Dict[str, Any] = {
            "spark_version": spark_version,
            "node_type_id": node_type_id,
            "num_workers": int(os.getenv("DATABRICKS_NEW_CLUSTER_NUM_WORKERS", "1")),
        }
        autoscale_min = os.getenv("DATABRICKS_NEW_CLUSTER_AUTOSCALE_MIN_WORKERS")
        autoscale_max = os.getenv("DATABRICKS_NEW_CLUSTER_AUTOSCALE_MAX_WORKERS")
        if autoscale_min and autoscale_max:
            new_cluster.pop("num_workers", None)
            new_cluster["autoscale"] = {
                "min_workers": int(autoscale_min),
                "max_workers": int(autoscale_max),
            }
        return {"new_cluster": new_cluster}

    raise RuntimeError(
        "Databricks execution requires DATABRICKS_CLUSTER_ID or DATABRICKS_NEW_CLUSTER_SPARK_VERSION/"
        "DATABRICKS_NEW_CLUSTER_NODE_TYPE_ID."
    )


def _submit_run(
    notebook_path: str, *, run_name: str, idempotency_token: str = ""
) -> Dict[str, Any]:
    task: Dict[str, Any] = {
        "task_key": "athena",
        "notebook_task": {"notebook_path": notebook_path},
    }
    task.update(_cluster_spec())
    payload = {
        "run_name": run_name,
        "tasks": [task],
        "timeout_seconds": _databricks_run_timeout_seconds(),
    }
    if idempotency_token:
        payload["idempotency_token"] = str(idempotency_token)[-64:]
    attempts = 3 if idempotency_token else 1
    for attempt in range(attempts):
        try:
            return _request_json("POST", "/api/2.0/jobs/runs/submit", payload)
        except DatabricksTransientError as exc:
            if attempt + 1 == attempts:
                raise DatabricksAmbiguousSubmissionError(
                    "Databricks submission outcome is ambiguous; the same queue attempt must be resumed."
                ) from exc
            time.sleep(0.25 * (attempt + 1))
    raise AssertionError("unreachable")


def _get_run(run_id: int) -> Dict[str, Any]:
    return _request_json("GET", f"/api/2.0/jobs/runs/get?run_id={run_id}")


def _get_run_output(run_id: int) -> Dict[str, Any]:
    return _request_json("GET", f"/api/2.1/jobs/runs/get-output?run_id={run_id}")


def _normalise_state(run_payload: Dict[str, Any]) -> Dict[str, Any]:
    run_state = run_payload.get("state") or {}
    life_cycle_state = str(run_state.get("life_cycle_state") or "").upper()
    result_state = str(run_state.get("result_state") or "").upper()
    state_message = str(run_state.get("state_message") or "").strip()
    return {
        "life_cycle_state": life_cycle_state,
        "result_state": result_state,
        "state_message": state_message,
        "run_page_url": run_payload.get("run_page_url"),
        "cluster_id": run_payload.get("cluster_instance", {}).get("cluster_id") if isinstance(run_payload.get("cluster_instance"), dict) else None,
    }


def _wait_for_run(run_id: int) -> Dict[str, Any]:
    started_at = time.monotonic()
    while True:
        payload = _get_run(run_id)
        state = _normalise_state(payload)
        if state["life_cycle_state"] == "TERMINATED":
            return {**payload, **state}
        if time.monotonic() - started_at > _databricks_run_timeout_seconds():
            raise TimeoutError(f"Databricks run {run_id} exceeded timeout.")
        time.sleep(_databricks_poll_interval_seconds())


def _task_run_id(run_state: Dict[str, Any]) -> Optional[int]:
    tasks = run_state.get("tasks")
    if isinstance(tasks, list) and tasks:
        task_run_id = tasks[0].get("run_id") if isinstance(tasks[0], dict) else None
        if task_run_id is not None:
            return int(task_run_id)
    run_id = run_state.get("run_id")
    if run_id is None:
        return None
    try:
        expanded = _request_json("GET", f"/api/2.1/jobs/runs/get?run_id={int(run_id)}")
        expanded_tasks = expanded.get("tasks")
        if isinstance(expanded_tasks, list) and expanded_tasks:
            task_run_id = expanded_tasks[0].get("run_id") if isinstance(expanded_tasks[0], dict) else None
            if task_run_id is not None:
                return int(task_run_id)
    except Exception:
        pass
    return int(run_id)


def _read_script_text(script: Dict[str, Any]) -> str:
    execution_spec = script.get("execution_spec")
    if execution_spec:
        from utilis.generated_code_paths import verified_execution_artifact

        verified_path = verified_execution_artifact(execution_spec, platform="databricks")
        script_path = Path(str(script.get("script_path") or "")).resolve()
        if script_path != verified_path:
            raise RuntimeError("Databricks script_path does not match the registered execution artifact.")
        return verified_path.read_text(encoding="utf-8")
    script_body = str(script.get("script_body") or "").strip()
    if script_body:
        script_path_value = str(script.get("script_path") or "").strip()
        if not script_path_value or Path(script_path_value).suffix.lower() not in {".py", ".sql"}:
            return script_body
    script_path = str(script.get("script_path") or "").strip()
    if script_path:
        path = Path(script_path)
        if path.exists():
            return path.read_text(encoding="utf-8")
    if script_body:
        return script_body
    raise ValueError("Databricks execution requires a script_path or script_body.")


def _script_name(script: Dict[str, Any]) -> str:
    target_table = str(script.get("target_table") or script.get("table") or script.get("table_name") or script.get("entity") or "script").strip()
    if target_table:
        script_path_value = str(script.get("script_path") or "").strip()
        if not script_path_value or Path(script_path_value).suffix.lower() not in {".py", ".sql"}:
            return re.sub(r"[^a-zA-Z0-9_]+", "_", target_table).strip("_") or "script"
    script_path = str(script.get("script_path") or "").strip()
    if script_path:
        return Path(script_path).stem
    return re.sub(r"[^a-zA-Z0-9_]+", "_", target_table).strip("_") or "script"


_REVIEW_IDENTITY_FAMILIES = (
    (700, ("target_table", "silver_target", "silver_table")),
    (600, ("dbt_model_name", "dbt_alias")),
    (500, ("kpi_name",)),
    (400, ("table", "table_name", "entity")),
    (300, ("source_table", "bronze_source", "bronze_table")),
    (200, ("script_name",)),
    (100, ("script_path",)),
)


def _script_target_table(script: Dict[str, Any]) -> str:
    target = str(
        script.get("target_table")
        or script.get("silver_table")
        or script.get("gold_table")
        or script.get("bronze_table")
        or ""
    ).strip()
    if target:
        return target
    match = re.search(
        r"(?m)^\s*TARGET_TABLE\s*=\s*r?[\"']([^\"']+)[\"']",
        _read_script_text(script),
    )
    return str(match.group(1)).strip() if match else ""


def _target_verification_code(target_table: str) -> str:
    encoded_target = json.dumps(str(target_table or ""))
    return f"""

_ATHENA_TARGET_TABLE = {encoded_target}
if not _ATHENA_TARGET_TABLE:
    raise RuntimeError("Generated script did not declare a target table.")
if not spark.catalog.tableExists(_ATHENA_TARGET_TABLE):
    raise RuntimeError(f"Target table was not created: {{_ATHENA_TARGET_TABLE}}")
_ATHENA_TARGET_ROW_COUNT = spark.table(_ATHENA_TARGET_TABLE).limit(1).count()
if _ATHENA_TARGET_ROW_COUNT < 1:
    raise RuntimeError(f"Target table is empty: {{_ATHENA_TARGET_TABLE}}")
""".rstrip()


def _script_keys(script: Dict[str, Any]) -> set[str]:
    keys: set[str] = set()
    for field in ("table", "table_name", "entity", "target_table", "source_table", "silver_table", "bronze_table", "kpi_name", "script_name"):
        value = str(script.get(field) or "").strip()
        if not value:
            continue
        folded = value.casefold()
        keys.add(folded)
        simple = value.split(".")[-1].strip('"').casefold()
        if simple:
            keys.add(simple)
            for prefix in ("bronze_", "silver_", "gold_"):
                if simple.startswith(prefix):
                    keys.add(simple[len(prefix):])
    script_path = str(script.get("script_path") or "").strip()
    if script_path:
        keys.add(script_path.casefold())
        stem = Path(script_path).stem.casefold()
        if stem:
            keys.add(stem)
    return keys


def _review_match_score(script: Dict[str, Any], item: Dict[str, Any]) -> int:
    for score, fields in _REVIEW_IDENTITY_FAMILIES:
        script_values = {
            str(script.get(field) or "").strip().casefold()
            for field in fields
            if str(script.get(field) or "").strip()
        }
        item_values = {
            str(item.get(field) or "").strip().casefold()
            for field in fields
            if str(item.get(field) or "").strip()
        }
        if script_values & item_values:
            return score
    return 0


def _unambiguous_review_matches(
    scripts: List[Dict[str, Any]],
    candidates: List[Dict[str, Any]],
) -> tuple[Dict[int, Dict[str, Any]], set[int]]:
    selected: Dict[int, tuple[int, int]] = {}
    ambiguous_scripts: set[int] = set()
    for script_index, script in enumerate(scripts):
        ranked = [
            (_review_match_score(script, item), item_index)
            for item_index, item in enumerate(candidates)
        ]
        best_score = max((score for score, _ in ranked), default=0)
        best_items = [item_index for score, item_index in ranked if score == best_score and score > 0]
        if len(best_items) == 1:
            selected[script_index] = (best_score, best_items[0])
        elif best_items:
            ambiguous_scripts.add(script_index)

    scripts_by_item: Dict[int, List[tuple[int, int]]] = {}
    for script_index, (score, item_index) in selected.items():
        scripts_by_item.setdefault(item_index, []).append((score, script_index))

    matches: Dict[int, Dict[str, Any]] = {}
    for item_index, ranked_scripts in scripts_by_item.items():
        best_score = max(score for score, _ in ranked_scripts)
        best_scripts = [
            script_index
            for score, script_index in ranked_scripts
            if score == best_score
        ]
        if len(best_scripts) == 1:
            matches[best_scripts[0]] = candidates[item_index]
        else:
            ambiguous_scripts.update(best_scripts)
        ambiguous_scripts.update(
            script_index
            for score, script_index in ranked_scripts
            if score < best_score
        )
    return matches, ambiguous_scripts


_REVIEW_EDITABLE_SCRIPT_FIELDS = {
    "review_status",
    "reviewer_comments",
    "script_body",
    "script_path",
    "generated_bronze_script",
    "generated_silver_script",
    "generated_gold_script",
    "dbt_model_sql",
    "dbt_model_body",
    "dimension_script_body",
    "dimension_script_path",
}


def _reviewed_script(script: Dict[str, Any], item: Dict[str, Any]) -> Dict[str, Any]:
    # Review identifiers select a generated script; they must not replace its execution metadata.
    review_edits = {key: value for key, value in item.items() if key in _REVIEW_EDITABLE_SCRIPT_FIELDS}
    return {**script, **review_edits}


def _filtered_scripts(scripts: List[Dict[str, Any]], review_artifact: Optional[Dict[str, Any]], layer: str) -> List[Dict[str, Any]]:
    if not review_artifact:
        return scripts

    review_items = review_artifact.get("feeds") if layer == "bronze" else review_artifact.get("items")
    if not isinstance(review_items, list):
        return scripts

    approved_items = [item for item in review_items if isinstance(item, dict) and str(item.get("review_status") or "").upper() == "APPROVED"]
    rejected_items = [item for item in review_items if isinstance(item, dict) and str(item.get("review_status") or "").upper() == "REJECTED"]
    if not approved_items and not rejected_items:
        return scripts

    if approved_items:
        matches, _ = _unambiguous_review_matches(scripts, approved_items)
        return [
            _reviewed_script(script, matches[index])
            for index, script in enumerate(scripts)
            if index in matches
        ]
    if rejected_items:
        matches, ambiguous_scripts = _unambiguous_review_matches(scripts, rejected_items)
        review_matches, _ = _unambiguous_review_matches(scripts, review_items)
        return [
            _reviewed_script(script, review_matches[index]) if index in review_matches else script
            for index, script in enumerate(scripts)
            if index not in matches and index not in ambiguous_scripts
        ]
    matches, _ = _unambiguous_review_matches(scripts, review_items)
    return [
        _reviewed_script(script, matches[index]) if index in matches else script
        for index, script in enumerate(scripts)
    ]


def _generated_scripts_for_layer(state: Dict[str, Any], layer: str) -> List[Dict[str, Any]]:
    runtime_scripts = state.get("_metadata_runtime_scripts")
    scripts = (
        [item for item in runtime_scripts if isinstance(item, dict)]
        if isinstance(runtime_scripts, list)
        else [item for item in (state.get(f"{layer}_generation_results") or []) if isinstance(item, dict)]
    )
    if not scripts:
        from services.pipeline_runtime import load_bronze_scripts, load_gold_scripts, load_silver_scripts

        loader_map = {
            "bronze": load_bronze_scripts,
            "silver": load_silver_scripts,
            "gold": load_gold_scripts,
        }
        loader = loader_map.get(layer)
        if loader:
            bundle = loader(str(state.get("run_id") or ""), state)
            scripts = [item for item in (bundle.get("scripts") or []) if isinstance(item, dict)]
    return scripts


def _scripts_for_layer(
    state: Dict[str, Any],
    layer: str,
    review_artifact: Optional[Dict[str, Any]],
    approved_only: bool,
    *,
    generated_scripts: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    layer = str(layer or "").strip().lower()
    scripts = list(
        generated_scripts
        if generated_scripts is not None
        else _generated_scripts_for_layer(state, layer)
    )
    if approved_only:
        scripts = _filtered_scripts(scripts, review_artifact, layer)
    if layer == "gold":
        approved_scripts = [
            script
            for script in scripts
            if str(script.get("status") or "APPROVED").upper() in {"APPROVED", "COMPLETED", "SUCCESS"}
            and (script.get("script_body") or script.get("script_path"))
        ]
        dimension_scripts: List[Dict[str, Any]] = []
        seen_dimension_paths: set[str] = set()
        for script in approved_scripts:
            dimension_path = str(script.get("dimension_script_path") or "").strip()
            dimension_body = str(script.get("dimension_script_body") or "").strip()
            if not dimension_body and dimension_path:
                path = Path(dimension_path)
                if path.exists():
                    dimension_body = path.read_text(encoding="utf-8")
            dimension_key = dimension_path or dimension_body
            if not dimension_body or dimension_key in seen_dimension_paths:
                continue
            seen_dimension_paths.add(dimension_key)
            dimension_scripts.append(
                {
                    "run_id": script.get("run_id") or state.get("run_id"),
                    "status": "APPROVED",
                    "script_path": dimension_path,
                    "script_body": dimension_body,
                    "target_table": "gold_dimensions",
                    "kpi_name": "Gold Dimensions",
                }
            )
        scripts = dimension_scripts + approved_scripts
    return scripts


def _batch_driver_path(layer: str, run_id: str) -> str:
    return _workspace_path(layer, run_id, f"__athena_{layer}_driver")


def _workspace_dir(path: str) -> str:
    return str(Path(path).parent).replace("\\", "/")


def _script_support_files(script: Dict[str, Any]) -> List[Dict[str, str]]:
    script_text = _read_script_text(script)
    script_path = str(script.get("script_path") or "").strip()
    if "from security_control import" not in script_text:
        return []
    if not script_path:
        return []
    local_helper = Path(script_path).with_name("security_control.py")
    if not local_helper.exists():
        return []
    return [
        {
            "name": "security_control.py",
            "content": local_helper.read_text(encoding="utf-8"),
        }
    ]


def _upload_support_files(workspace_dir: str, scripts: List[Dict[str, Any]]) -> None:
    uploaded: set[str] = set()
    for script in scripts:
        for support_file in _script_support_files(script):
            target_path = f"{workspace_dir}/{support_file['name']}"
            if target_path in uploaded:
                continue
            _workspace_import_file(target_path, support_file["content"])
            uploaded.add(target_path)


def _build_batch_driver_notebook(
    layer: str,
    scripts: List[Dict[str, Any]],
    *,
    workspace_dir: str,
    runtime_context: Optional[Dict[str, Any]] = None,
    metadata_runtime_batch: bool = False,
) -> str:
    script_items = [
        {
            "script_name": _script_name(script),
            "script_path": str(script.get("script_path") or "").strip(),
            "target_table": _script_target_table(script),
            "script_text": _read_script_text(script),
            "validation_policy": script.get("validation_policy") or {},
            "mapping_contract": script.get("mapping_contract") or [],
            "merge_keys": script.get("merge_keys") or [],
            "approved_source_tables": script.get("approved_source_tables") or [],
            "runtime_context": script.get("metadata_runtime_context") or {},
        }
        for script in scripts
    ]
    encoded = base64.b64encode(json.dumps(script_items).encode("utf-8")).decode("ascii")
    context_encoded = base64.b64encode(
        json.dumps(dict(runtime_context or {}), sort_keys=True).encode("utf-8")
    ).decode("ascii")
    metadata_driven = any(
        script.get("metadata_runtime") or script.get("gold_ingestion_object_id") is not None
        for script in scripts
    )
    allow_partial_success = _gold_partial_success_enabled(layer) and not metadata_driven
    continue_on_error = metadata_runtime_batch or allow_partial_success or _env_flag(
        f"ATHENA_DATABRICKS_{str(layer or '').upper()}_CONTINUE_ON_ERROR",
        "ATHENA_DATABRICKS_CONTINUE_ON_ERROR",
    )
    return f'''# Databricks notebook source
import base64
import builtins
import json
import sys
import time
import traceback

from delta.tables import DeltaTable

_SCRIPT_ITEMS = json.loads(base64.b64decode("{encoded}").decode("utf-8"))
_RUNTIME_CONTEXT = json.loads(base64.b64decode("{context_encoded}").decode("utf-8"))
_CONTINUE_ON_ERROR = {str(continue_on_error)}
_ALLOW_PARTIAL_SUCCESS = {str(allow_partial_success)}
_METADATA_RUNTIME_BATCH = {str(metadata_runtime_batch)}
_RESULTS = []
_WORKSPACE_DIR = "{workspace_dir}"
if _WORKSPACE_DIR not in sys.path:
    sys.path.append(_WORKSPACE_DIR)

for _index, _item in enumerate(_SCRIPT_ITEMS, start=1):
    _started = time.time()
    _name = _item.get("script_name") or f"script_{{_index}}"
    _target = str(_item.get("target_table") or "").strip()
    _ITEM_RUNTIME_CONTEXT = _item.get("runtime_context") or _RUNTIME_CONTEXT
    try:
        _target_existed_before = False
        _version_before = None
        if _ITEM_RUNTIME_CONTEXT and _target and _target.casefold() != "gold_dimensions":
            _target_existed_before = spark.catalog.tableExists(_target)
            if _target_existed_before:
                _before = DeltaTable.forName(spark, _target).history(1).select("version").first()
                if not _before:
                    raise RuntimeError(f"Delta history is unavailable before executing {{_target}}")
                _version_before = int(_before["version"])
        _script_globals = dict(globals())
        _script_globals["__name__"] = f"__athena_{{_name}}"
        _script_globals["__file__"] = _item.get("script_path") or f"<athena:{{_name}}>"
        _script_globals["ATHENA_RUNTIME_CONTEXT"] = dict(_ITEM_RUNTIME_CONTEXT)
        exec(compile(_item.get("script_text") or "", f"<athena:{{_name}}>", "exec"), _script_globals)
        _verification_status = "UNVERIFIED"
        _verification_warning = None
        if _target and _target.casefold() != "gold_dimensions":
            try:
                _target_exists = spark.catalog.tableExists(_target)
            except Exception as _verify_exc:
                _verification_warning = f"Target verification unavailable for {{_target}}: {{_verify_exc}}"
            else:
                if not _target_exists:
                    raise RuntimeError(f"Expected Databricks target table was not created: {{_target}}")
                _verification_status = "VERIFIED"
        _validation_results = []
        _policy_rules = (_item.get("validation_policy") or {{}}).get("rules") or []
        if _policy_rules:
            if not _target or _target.casefold() == "gold_dimensions":
                raise RuntimeError("Blocking validation requires one concrete target table")
            _target_df = spark.table(_target)
            _target_columns = set(_target_df.columns)
            _expected_columns = {{
                str(_row.get("target_column_name") or "")
                for _row in (_item.get("mapping_contract") or [])
                if str(_row.get("target_column_name") or "")
            }}
            _scoped_target = _target_df
            if "_logical_work_id" in _target_columns:
                _logical_literal = str(_ITEM_RUNTIME_CONTEXT.get("logical_work_id") or "").replace("'", "''")
                _scoped_target = _target_df.filter(f"`_logical_work_id` = '{{_logical_literal}}'")
            _script_rule_results = {{
                str(_row.get("rule_type") or _row.get("rule") or "").upper(): _row
                for _row in (_script_globals.get("ATHENA_VALIDATION_RESULTS") or [])
                if isinstance(_row, dict)
            }}
            for _rule in _policy_rules:
                if not isinstance(_rule, dict):
                    raise RuntimeError("Blocking validation policy contains an invalid rule")
                _rule_type = str(_rule.get("rule_type") or _rule.get("rule") or "").upper()
                _threshold = _rule.get("threshold_value", 0)
                if _rule_type in {{"MAPPED_COLUMNS_PRESENT", "TARGET_SCHEMA_MATCH"}}:
                    _observed = len(_expected_columns - _target_columns)
                elif _rule_type == "MERGE_KEYS_NOT_NULL":
                    _columns = [str(_name) for _name in (_rule.get("columns") or _item.get("merge_keys") or [])]
                    if not _columns or any(_name not in _target_columns for _name in _columns):
                        raise RuntimeError("Merge-key validation references unavailable target columns")
                    _predicate = " OR ".join(f"`{{_name.replace('`', '``')}}` IS NULL" for _name in _columns)
                    _observed = _scoped_target.filter(_predicate).count()
                elif _rule_type == "KEYS_NOT_NULL":
                    _columns = [str(_name) for _name in (_rule.get("columns") or _item.get("merge_keys") or [])]
                    if not _columns or any(_name not in _target_columns for _name in _columns):
                        raise RuntimeError("Gold key validation references unavailable target columns")
                    _predicate = " OR ".join(f"`{{_name.replace('`', '``')}}` IS NULL" for _name in _columns)
                    _observed = _scoped_target.filter(_predicate).count()
                elif _rule_type == "KEYS_UNIQUE":
                    _columns = [str(_name) for _name in (_rule.get("columns") or _item.get("merge_keys") or [])]
                    if not _columns or any(_name not in _target_columns for _name in _columns):
                        raise RuntimeError("Gold key validation references unavailable target columns")
                    _observed = _scoped_target.groupBy(*_columns).count().filter("count > 1").count()
                elif _rule_type == "INPUTS_PRESENT":
                    _observed = sum(
                        1 for _source in (_item.get("approved_source_tables") or [])
                        if not spark.catalog.tableExists(str(_source))
                    )
                elif _rule_type == "MAX_JOIN_MULTIPLIER":
                    _reported = _script_rule_results.get(_rule_type)
                    if not _reported or _reported.get("observed_value") is None:
                        raise RuntimeError("Gold artifact did not report its observed join multiplier")
                    _observed = float(_reported["observed_value"])
                else:
                    raise RuntimeError(f"Unsupported blocking validation rule: {{_rule_type}}")
                _status = "PASSED" if float(_observed) <= float(_threshold) else "FAILED"
                _result = {{
                    "rule_type": _rule_type,
                    "observed_value": _observed,
                    "threshold_value": _threshold,
                    "status": _status,
                }}
                _validation_results.append(_result)
                if _status != "PASSED":
                    raise RuntimeError(f"Blocking validation failed: {{_result}}")
        _execution_result = None
        if _ITEM_RUNTIME_CONTEXT and _target and _target.casefold() != "gold_dimensions":
            _history = DeltaTable.forName(spark, _target).history(1).select(
                "version", "operationMetrics"
            ).first()
            if not _history:
                raise RuntimeError(f"Delta history is unavailable after executing {{_target}}")
            _version_after = int(_history["version"])
            if _target_existed_before and _version_after != _version_before + 1:
                raise RuntimeError(
                    f"Expected exactly one Delta commit for {{_target}}; "
                    f"history advanced from {{_version_before}} to {{_version_after}}"
                )
            if not _target_existed_before and _version_after < 0:
                raise RuntimeError(f"No Delta commit was created for {{_target}}")
            _metrics = dict(_history["operationMetrics"] or {{}})
            _rows_written = _metrics.get("numOutputRows")
            if _rows_written is None:
                _rows_written = sum(
                    int(_metrics.get(_key) or 0)
                    for _key in ("numTargetRowsInserted", "numTargetRowsUpdated")
                )
            _execution_result = {{
                "contract_version": "1.0",
                "status": "COMPLETED",
                "logical_work_id": _ITEM_RUNTIME_CONTEXT.get("logical_work_id"),
                "runtime_run_id": _ITEM_RUNTIME_CONTEXT.get("runtime_run_id"),
                "target_table": _target,
                "target_commit_id": f"delta:{{_target}}:v{{_history['version']}}",
                "rows_written": int(_rows_written or 0),
                "validation_status": "PASSED",
                "validation_policy_hash": _ITEM_RUNTIME_CONTEXT.get("validation_policy_hash"),
                "validation_results": _validation_results,
            }}
        _result = {{
            "script_name": _name,
            "script_path": _item.get("script_path"),
            "target_table": _target or None,
            "runtime_run_id": _ITEM_RUNTIME_CONTEXT.get("runtime_run_id"),
            "queue_id": _ITEM_RUNTIME_CONTEXT.get("queue_id"),
            "status": "SUCCESS",
            "verification_status": _verification_status,
            "elapsed_seconds": round(time.time() - _started, 2),
        }}
        if _verification_warning:
            _result["verification_warning"] = _verification_warning
        if _execution_result:
            _result["execution_result"] = _execution_result
        _RESULTS.append(_result)
    except Exception as _exc:
        _RESULTS.append({{
            "script_name": _name,
            "script_path": _item.get("script_path"),
            "target_table": _item.get("target_table"),
            "runtime_run_id": _ITEM_RUNTIME_CONTEXT.get("runtime_run_id"),
            "queue_id": _ITEM_RUNTIME_CONTEXT.get("queue_id"),
            "status": "FAILED",
            "error": str(_exc),
            "traceback": traceback.format_exc()[-12000:],
            "elapsed_seconds": round(time.time() - _started, 2),
        }})
        if not _CONTINUE_ON_ERROR:
            break

_SCRIPTS_OK = builtins.sum(1 for _r in _RESULTS if _r.get("status") == "SUCCESS")
_SCRIPTS_FAILED = builtins.sum(1 for _r in _RESULTS if _r.get("status") == "FAILED")
_PARTIAL_SUCCESS = _ALLOW_PARTIAL_SUCCESS and _SCRIPTS_FAILED > 0 and _SCRIPTS_OK >= _SCRIPTS_FAILED
_SUMMARY = {{
    "status": "COMPLETED_WITH_WARNINGS" if _PARTIAL_SUCCESS else "FAILED" if _SCRIPTS_FAILED else "SUCCESS",
    "scripts_total": builtins.len(_SCRIPT_ITEMS),
    "scripts_executed": builtins.len(_RESULTS),
    "scripts_ok": _SCRIPTS_OK,
    "scripts_failed": _SCRIPTS_FAILED,
    "results": _RESULTS,
}}

if _SUMMARY["status"] == "FAILED":
    if not _METADATA_RUNTIME_BATCH:
        raise RuntimeError(json.dumps(_SUMMARY, default=str))

dbutils.notebook.exit(json.dumps(_SUMMARY, default=str))
'''


def _parse_notebook_result(output_payload: Dict[str, Any]) -> Dict[str, Any]:
    notebook_output = output_payload.get("notebook_output") if isinstance(output_payload, dict) else None
    result = notebook_output.get("result") if isinstance(notebook_output, dict) else None
    if not result:
        return {}
    try:
        parsed = json.loads(result)
    except json.JSONDecodeError:
        return {"raw_result": result}
    return parsed if isinstance(parsed, dict) else {"raw_result": parsed}


def _parse_batch_summary_from_error(value: Any) -> Dict[str, Any]:
    text = str(value or "")
    decoder = json.JSONDecoder()
    for index, char in enumerate(text):
        if char != "{":
            continue
        try:
            parsed, _ = decoder.raw_decode(text[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict) and isinstance(parsed.get("results"), list):
            return parsed
    return {}


def _safe_progress_value(value: Any) -> Any:
    from services.metadata_repository import MetadataRepository

    return MetadataRepository._redact_sensitive(value)


def _annotate_batch_results(
    results: List[Dict[str, Any]],
    *,
    notebook_path: str,
    run_state: Dict[str, Any],
) -> List[Dict[str, Any]]:
    return [
        {
            **item,
            "workspace_path": notebook_path,
            "databricks_run_id": run_state.get("run_id"),
            "run_page_url": run_state.get("run_page_url"),
            "cluster_id": run_state.get("cluster_id"),
            "compute_mode": _databricks_compute_mode(),
            "life_cycle_state": run_state.get("life_cycle_state"),
            "result_state": run_state.get("result_state"),
            "state_message": run_state.get("state_message"),
        }
        for item in results
    ]


def _successful_batch_results_from_scripts(
    scripts: List[Dict[str, Any]],
    *,
    notebook_path: str,
    run_state: Dict[str, Any],
    warning: Optional[str] = None,
) -> List[Dict[str, Any]]:
    return [
        {
            "script_name": _script_name(script),
            "script_path": str(script.get("script_path") or "").strip(),
            "target_table": script.get("target_table") or script.get("silver_table") or script.get("bronze_table"),
            "status": "SUCCESS",
            "verification_status": "UNVERIFIED",
            "workspace_path": notebook_path,
            "databricks_run_id": run_state.get("run_id"),
            "run_page_url": run_state.get("run_page_url"),
            "cluster_id": run_state.get("cluster_id"),
            "compute_mode": _databricks_compute_mode(),
            "life_cycle_state": run_state.get("life_cycle_state"),
            "result_state": run_state.get("result_state"),
            "state_message": run_state.get("state_message"),
            **({"warning": warning} if warning else {}),
        }
        for script in scripts
    ]


def _run_failure_detail(run_state: Dict[str, Any]) -> str:
    detail = str(run_state.get("state_message") or run_state.get("result_state") or "unknown error")
    try:
        output_run_id = _task_run_id(run_state)
        output = _get_run_output(output_run_id) if output_run_id is not None else {}
        return str(output.get("error") or output.get("error_trace") or detail)[:4000]
    except Exception:
        return detail


def _execute_databricks_stage_batch(
    state: Dict[str, Any],
    *,
    layer: str,
    scripts: List[Dict[str, Any]],
    on_submitted: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    run_id = str(state.get("run_id") or "")
    notebook_path = _batch_driver_path(layer, run_id)
    workspace_dir = _workspace_dir(notebook_path)
    _upload_support_files(workspace_dir, scripts)
    runtime_context = dict(state.get("metadata_runtime_context") or {})
    _workspace_import_notebook(
        notebook_path,
        _build_batch_driver_notebook(
            layer,
            scripts,
            workspace_dir=workspace_dir,
            runtime_context=runtime_context,
            metadata_runtime_batch=bool(state.get("metadata_runtime_batch")),
        ),
    )

    state = save_external_execution_progress(
        state,
        run_id=run_id,
        platform="databricks",
        layer=layer,
        stage_key=f"{layer}_code_execution",
        status="RUNNING",
        total_count=len(scripts),
        completed_count=0,
        message=f"Executing {layer.capitalize()} scripts in one Databricks batch job.",
    )

    started_at = time.monotonic()
    run_payload = _submit_run(
        notebook_path,
        run_name=f"Athena {layer} batch {run_id}",
        idempotency_token=(
            f"athena-metadata-{runtime_context['queue_id']}-{runtime_context.get('attempt_number', 0)}"
            if runtime_context else ""
        ),
    )
    if on_submitted:
        submitted_run_id = str(run_payload.get("run_id") or "")
        try:
            on_submitted(submitted_run_id)
        except Exception as exc:
            if runtime_context:
                raise DatabricksAmbiguousSubmissionError(
                    f"Databricks run {submitted_run_id} was accepted but its control receipt could not be persisted; "
                    "resume the same queue attempt."
                ) from exc
            raise
    run_state = _wait_for_run(int(run_payload.get("run_id")))
    elapsed_seconds = round(time.monotonic() - started_at, 2)
    if str(run_state.get("result_state") or "").upper() not in {"SUCCESS", "COMPLETED"}:
        detail = _run_failure_detail(run_state)
        summary = _parse_batch_summary_from_error(detail)
        partial_results = _annotate_batch_results(
            [item for item in (summary.get("results") or []) if isinstance(item, dict)],
            notebook_path=notebook_path,
            run_state=run_state,
        )
        partial_results = _safe_progress_value(partial_results)
        failed_results = [item for item in partial_results if str(item.get("status") or "").upper() == "FAILED"]
        first_failed = failed_results[0] if failed_results else {}
        completed_count = sum(1 for item in partial_results if str(item.get("status") or "").upper() == "SUCCESS")
        failure = str(_safe_progress_value(
            f"Databricks {layer} batch execution failed for {first_failed.get('script_name')}: "
            f"{first_failed.get('error') or 'unknown error'}"
            if first_failed
            else f"Databricks {layer} batch execution failed: {detail}"
        ))
        save_external_execution_progress(
            {
                **state,
                "status": "FAILED",
                "failed_background_stage": f"{layer}_code_execution",
                "error": failure,
                f"databricks_{layer}_execution_results": partial_results,
            },
            run_id=run_id,
            platform="databricks",
            layer=layer,
            stage_key=f"{layer}_code_execution",
            status="FAILED",
            total_count=len(scripts),
            completed_count=completed_count,
            current_index=len(partial_results) or None,
            current_name=first_failed.get("script_name"),
            current_target=first_failed.get("target_table"),
            message=failure,
        )
        if state.get("metadata_runtime_batch"):
            raise DatabricksBatchExecutionError(failure, partial_results)
        raise RuntimeError(failure)

    output_warning = None
    try:
        output_run_id = _task_run_id(run_state)
        output = _get_run_output(output_run_id) if output_run_id is not None else {}
        summary = _parse_notebook_result(output)
    except Exception as exc:
        output_warning = f"Databricks run succeeded, but Athena could not read notebook output: {exc}"
        logger.warning(
            output_warning,
            extra={"run_id": run_id, "node": f"{layer}_output_read_warning", "stage": f"{layer}_code_execution"},
        )
        summary = {}
    results = [item for item in (summary.get("results") or []) if isinstance(item, dict)]
    executed_scripts = (
        _annotate_batch_results(results, notebook_path=notebook_path, run_state=run_state)
        if results
        else _successful_batch_results_from_scripts(
            scripts,
            notebook_path=notebook_path,
            run_state=run_state,
            warning=output_warning or "Databricks run succeeded, but notebook output did not include per-script results.",
        )
    )
    failed = [item for item in executed_scripts if str(item.get("status") or "").upper() == "FAILED"]
    succeeded_count = sum(1 for item in executed_scripts if str(item.get("status") or "").upper() == "SUCCESS")
    metadata_driven = any(
        script.get("metadata_runtime") or script.get("gold_ingestion_object_id") is not None
        for script in scripts
    )
    partial_success = (
        _gold_partial_success_enabled(layer)
        and not metadata_driven
        and bool(failed)
        and succeeded_count >= len(failed)
    )
    if failed and not partial_success:
        first = failed[0]
        failure = (
            f"Databricks {layer} batch execution failed for {first.get('script_name')}: "
            f"{first.get('error') or 'unknown error'}"
        )
        save_external_execution_progress(
            {
                **state,
                "status": "FAILED",
                "failed_background_stage": f"{layer}_code_execution",
                "error": failure,
                f"databricks_{layer}_execution_results": executed_scripts,
            },
            run_id=run_id,
            platform="databricks",
            layer=layer,
            stage_key=f"{layer}_code_execution",
            status="FAILED",
            total_count=len(scripts),
            completed_count=succeeded_count,
            current_index=executed_scripts.index(first) + 1,
            current_name=first.get("script_name"),
            current_target=first.get("target_table"),
            message=failure,
        )
        if state.get("metadata_runtime_batch"):
            raise DatabricksBatchExecutionError(failure, executed_scripts)
        raise RuntimeError(failure)

    execution_status = "COMPLETED_WITH_WARNINGS" if partial_success else "COMPLETED"
    failed_names = [str(item.get("script_name") or item.get("target_table") or "unknown script") for item in failed]
    message = (
        f"Databricks Gold completed with warnings: {succeeded_count}/{len(scripts)} scripts succeeded; "
        f"failed scripts: {', '.join(failed_names)}."
        if partial_success
        else f"Databricks {layer.capitalize()} batch execution completed: "
        f"{succeeded_count}/{len(scripts)} scripts finished in {elapsed_seconds}s."
    )
    final_state = {
        **state,
        f"databricks_{layer}_execution_status": execution_status,
        f"databricks_{layer}_execution_results": executed_scripts,
        f"databricks_{layer}_execution_failures": failed,
        f"databricks_{layer}_executed_at": datetime.now(timezone.utc).isoformat(),
    }
    return save_external_execution_progress(
        final_state,
        run_id=run_id,
        platform="databricks",
        layer=layer,
        stage_key=f"{layer}_code_execution",
        status=execution_status,
        total_count=len(scripts),
        completed_count=succeeded_count,
        message=message,
    )


def _execute_databricks_stage(
    state: Dict[str, Any],
    *,
    layer: str,
    review_artifact: Optional[Dict[str, Any]] = None,
    approved_only: bool = False,
    on_submitted: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    run_id = str(state.get("run_id") or "")
    target_warehouse = str(state.get("target_warehouse") or "databricks").lower()
    if target_warehouse != "databricks":
        return state
    if not databricks_execution_enabled(layer):
        logger.info(
            "Databricks %s execution disabled; generated scripts remain review artifacts",
            layer,
            extra={"run_id": run_id, "node": f"{layer}_execution_disabled", "stage": f"{layer}_code_execution"},
        )
        return {**state, f"databricks_{layer}_execution_status": "DISABLED"}

    generated_scripts = _generated_scripts_for_layer(state, layer)
    identity_filtered_scripts = (
        _filtered_scripts(generated_scripts, review_artifact, layer)
        if approved_only
        else generated_scripts
    )
    identity_matched_count = len(identity_filtered_scripts)
    scripts = _scripts_for_layer(
        state,
        layer,
        None,
        False,
        generated_scripts=identity_filtered_scripts,
    )
    if not scripts:
        if not generated_scripts:
            raise ValueError(
                f"Databricks {layer} execution enabled but no generated scripts were found "
                "(generated_count=0)."
            )
        review_items = (
            (review_artifact or {}).get("feeds")
            if layer == "bronze"
            else (review_artifact or {}).get("items")
        )
        approved_count = sum(
            1
            for item in review_items or []
            if isinstance(item, dict)
            and str(item.get("review_status") or "").upper() == "APPROVED"
        )
        if approved_only and approved_count and identity_matched_count == 0:
            raise ValueError(
                f"Databricks {layer} execution found generated and approved review items, "
                "but zero script identities matched "
                f"(generated_count={len(generated_scripts)}, "
                f"approved_count={approved_count}, matched_count=0)."
            )
        raise ValueError(
            f"Databricks {layer} execution has no executable scripts after review filtering "
            f"(generated_count={len(generated_scripts)}, approved_count={approved_count}, "
            f"matched_count={identity_matched_count})."
        )

    metadata_runtime = bool(state.get("metadata_runtime_context"))
    if metadata_runtime or _databricks_execution_mode(layer) == "batch":
        return _execute_databricks_stage_batch(
            state, layer=layer, scripts=scripts, on_submitted=on_submitted
        )

    state = save_external_execution_progress(
        state,
        run_id=run_id,
        platform="databricks",
        layer=layer,
        stage_key=f"{layer}_code_execution",
        status="RUNNING",
        total_count=len(scripts),
        completed_count=0,
        message=f"Executing {layer.capitalize()} scripts in Databricks: 0/{len(scripts)} completed.",
    )

    executed_scripts: List[Dict[str, Any]] = []
    for index, script in enumerate(scripts, start=1):
        script_path = str(script.get("script_path") or "").strip()
        script_name = _script_name(script)
        notebook_path = _workspace_path(layer, run_id, script_name)
        target_table = _script_target_table(script)
        script_text = f"{_read_script_text(script)}\n{_target_verification_code(target_table)}"
        logger.info(
            "Submitting Databricks %s script %d/%d for %s",
            layer,
            index,
            len(scripts),
            script_name,
            extra={"run_id": run_id, "node": f"{layer}_script_submit_start", "stage": f"{layer}_code_execution"},
        )
        _upload_support_files(_workspace_dir(notebook_path), [script])
        _workspace_import_notebook(notebook_path, script_text)
        state = save_external_execution_progress(
            state,
            run_id=run_id,
            platform="databricks",
            layer=layer,
            stage_key=f"{layer}_code_execution",
            status="RUNNING",
            total_count=len(scripts),
            completed_count=len(executed_scripts),
            current_index=index,
            current_name=script_name,
            current_target=target_table,
            message=f"Databricks {layer.capitalize()} execution running: table {index}/{len(scripts)} ({script_name}).",
        )
        started_at = time.monotonic()
        run_payload = _submit_run(notebook_path, run_name=f"Athena {layer} {run_id}")
        if on_submitted:
            on_submitted(str(run_payload.get("run_id") or ""))
        run_state = _wait_for_run(int(run_payload.get("run_id")))
        elapsed_seconds = round(time.monotonic() - started_at, 2)
        if str(run_state.get("result_state") or "").upper() not in {"SUCCESS", "COMPLETED"}:
            failure = str(_safe_progress_value(
                f"Databricks {layer} execution failed for {script_name}: {_run_failure_detail(run_state)}"
            ))
            save_external_execution_progress(
                {
                    **state,
                    "status": "FAILED",
                    "failed_background_stage": f"{layer}_code_execution",
                    "error": failure,
                    f"databricks_{layer}_execution_results": executed_scripts,
                },
                run_id=run_id,
                platform="databricks",
                layer=layer,
                stage_key=f"{layer}_code_execution",
                status="FAILED",
                total_count=len(scripts),
                completed_count=len(executed_scripts),
                current_index=index,
                current_name=script_name,
                current_target=target_table,
                message=failure,
            )
            raise RuntimeError(failure)
        executed_scripts.append(
            {
                "script_name": script_name,
                "script_path": script_path,
                "workspace_path": notebook_path,
                "databricks_run_id": run_state.get("run_id"),
                "run_page_url": run_state.get("run_page_url"),
                "cluster_id": run_state.get("cluster_id"),
                "compute_mode": _databricks_compute_mode(),
                "life_cycle_state": run_state.get("life_cycle_state"),
                "result_state": run_state.get("result_state"),
                "state_message": run_state.get("state_message"),
                "elapsed_seconds": elapsed_seconds,
                "target_table": target_table,
                "target_verified": True,
                "target_row_count_at_least": 1,
            }
        )
        state = save_external_execution_progress(
            state,
            run_id=run_id,
            platform="databricks",
            layer=layer,
            stage_key=f"{layer}_code_execution",
            status="RUNNING",
            total_count=len(scripts),
            completed_count=len(executed_scripts),
            current_index=index,
            current_name=script_name,
            current_target=target_table,
            message=f"Databricks {layer.capitalize()} execution progress: {len(executed_scripts)}/{len(scripts)} completed.",
        )

    final_state = {
        **state,
        f"databricks_{layer}_execution_status": "COMPLETED",
        f"databricks_{layer}_execution_results": executed_scripts,
        f"databricks_{layer}_executed_at": datetime.now(timezone.utc).isoformat(),
    }
    return save_external_execution_progress(
        final_state,
        run_id=run_id,
        platform="databricks",
        layer=layer,
        stage_key=f"{layer}_code_execution",
        status="COMPLETED",
        total_count=len(scripts),
        completed_count=len(executed_scripts),
        message=f"Databricks {layer.capitalize()} execution completed: {len(executed_scripts)}/{len(scripts)} scripts finished.",
    )


def run_databricks_bronze_scripts(
    state: Dict[str, Any],
    *,
    review_artifact: Dict[str, Any] | None = None,
    approved_only: bool = False,
    on_submitted: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    return _execute_databricks_stage(
        state, layer="bronze", review_artifact=review_artifact,
        approved_only=approved_only, on_submitted=on_submitted,
    )


def run_databricks_silver_scripts(
    state: Dict[str, Any],
    *,
    review_artifact: Dict[str, Any] | None = None,
    approved_only: bool = False,
    on_submitted: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    return _execute_databricks_stage(
        state, layer="silver", review_artifact=review_artifact,
        approved_only=approved_only, on_submitted=on_submitted,
    )


def run_databricks_gold_scripts(
    state: Dict[str, Any],
    *,
    review_artifact: Dict[str, Any] | None = None,
    approved_only: bool = False,
    on_submitted: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    return _execute_databricks_stage(
        state, layer="gold", review_artifact=review_artifact,
        approved_only=approved_only, on_submitted=on_submitted,
    )
