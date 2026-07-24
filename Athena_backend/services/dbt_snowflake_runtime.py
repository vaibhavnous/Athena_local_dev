from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from services.external_execution_progress import save_external_execution_progress
from utilis.generated_code_paths import generated_code_dir
from utilis.logger import logger


DBT_STAGE_KEY = "snowflake_dbt_deploy"
DBT_ARTIFACT_TYPE = "SNOWFLAKE_DBT_DEPLOYMENT"
DBT_SCHEMA_VERSION = "1.0"
_VALID_ENGINES = {"native", "dbt"}
_VALID_MODES = {"generate_only", "generate_and_deploy"}
_SECRET_ENV_NAMES = {
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE",
    "AZURE_CLIENT_SECRET",
}


def _choice(value: Any, allowed: set[str], default: str) -> str:
    normalized = str(value or default).strip().lower()
    return normalized if normalized in allowed else default


def resolve_execution_engine(state: Dict[str, Any]) -> str:
    return _choice(state.get("execution_engine") or os.getenv("ATHENA_SNOWFLAKE_EXECUTION_ENGINE"), _VALID_ENGINES, "native")


def resolve_dbt_deployment_mode(state: Dict[str, Any]) -> str:
    return _choice(
        state.get("dbt_deployment_mode") or os.getenv("ATHENA_SNOWFLAKE_DBT_DEPLOYMENT_MODE"),
        _VALID_MODES,
        "generate_only",
    )


def snowflake_dbt_enabled(state: Dict[str, Any]) -> bool:
    if str(state.get("target_warehouse") or "").strip().lower() != "snowflake":
        return False
    return resolve_execution_engine(state) == "dbt" or resolve_dbt_deployment_mode(state) != "generate_only"


def _run_slug(run_id: Any) -> str:
    slug = re.sub(r"[^A-Za-z0-9_-]+", "_", str(run_id or "manual").strip())
    return slug.strip("_") or "manual"


def _safe_name(value: Any, *, prefix: str = "model") -> str:
    name = re.sub(r"[^A-Za-z0-9_]+", "_", str(value or "").strip().lower()).strip("_")
    if not name:
        name = prefix
    if name[0].isdigit():
        name = f"{prefix}_{name}"
    return name[:80]


def _split_qualified_name(value: Any) -> List[str]:
    text = str(value or "").strip()
    if not text:
        return []
    parts: List[str] = []
    token: List[str] = []
    in_quote = False
    index = 0
    while index < len(text):
        char = text[index]
        if char == '"':
            if in_quote and index + 1 < len(text) and text[index + 1] == '"':
                token.append('"')
                index += 2
                continue
            in_quote = not in_quote
            index += 1
            continue
        if char == "." and not in_quote:
            part = "".join(token).strip()
            if part:
                parts.append(part)
            token = []
            index += 1
            continue
        token.append(char)
        index += 1
    part = "".join(token).strip()
    if part:
        parts.append(part)
    return parts


def _quote_identifier(value: Any) -> str:
    cleaned = str(value or "").strip()
    if not cleaned:
        raise ValueError("Snowflake identifier cannot be empty.")
    return '"' + cleaned.replace('"', '""') + '"'


def _qualified_relation(value: Any) -> str:
    parts = _split_qualified_name(value)
    if not parts:
        raise ValueError("Snowflake relation cannot be empty.")
    return ".".join(_quote_identifier(part) for part in parts)


def _default_dbt_database() -> str:
    return (
        os.getenv("SNOWFLAKE_DBT_DATABASE")
        or os.getenv("SNOWFLAKE_GOLD_CATALOG")
        or os.getenv("SNOWFLAKE_DATABASE")
        or "ATHENA_DB"
    )


def _default_dbt_schema() -> str:
    return os.getenv("SNOWFLAKE_DBT_SCHEMA") or "DBT"


def _dbt_threads(state: Dict[str, Any]) -> int:
    raw = state.get("dbt_threads") or os.getenv("ATHENA_SNOWFLAKE_DBT_THREADS") or 4
    try:
        return min(32, max(1, int(raw)))
    except (TypeError, ValueError):
        return 4


def _dbt_timeout_seconds(state: Dict[str, Any]) -> int:
    raw = state.get("dbt_command_timeout_secs") or os.getenv("ATHENA_SNOWFLAKE_DBT_COMMAND_TIMEOUT_SECONDS") or 3600
    try:
        return min(86_400, max(60, int(raw)))
    except (TypeError, ValueError):
        return 3600


def _project_dir(run_id: Any) -> Path:
    return generated_code_dir("snowflake", "dbt", _run_slug(run_id))


def _reset_generated_dir(path: Path, *, root: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    root_resolved = root.resolve()
    path_resolved = path.resolve()
    if root_resolved != path_resolved and root_resolved not in path_resolved.parents:
        raise ValueError(f"Refusing to clean dbt path outside project root: {path}")
    for child in path.iterdir():
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()


def _gold_outputs(state: Dict[str, Any]) -> List[Dict[str, str]]:
    outputs: List[Dict[str, str]] = []
    seen: set[str] = set()
    for item in state.get("gold_generation_results") or []:
        if not isinstance(item, dict):
            continue
        status = str(item.get("status") or item.get("generation_status") or "APPROVED").strip().upper()
        if status.startswith(("BLOCKED", "FAILED", "SKIPPED", "ERROR")):
            continue
        target_table = str(item.get("target_table") or "").strip()
        if not target_table:
            continue
        relation = _qualified_relation(target_table)
        if relation in seen:
            continue
        seen.add(relation)
        base_name = _split_qualified_name(target_table)[-1]
        model_name = _safe_name(f"publish_{base_name}", prefix="publish")
        alias = _safe_name(f"dbt_{base_name}", prefix="dbt")
        outputs.append(
            {
                "target_table": target_table,
                "source_relation": relation,
                "model_name": model_name,
                "alias": alias,
            }
        )
    if not outputs:
        raise ValueError("Snowflake dbt was requested but no approved Gold target tables were found.")
    return outputs


def _render_project_yml() -> str:
    return """name: athena_snowflake_dbt
version: 1.0.0
config-version: 2
profile: athena_snowflake

model-paths: ["models"]
target-path: "target"
clean-targets: ["target", "dbt_packages", "logs"]

models:
  athena_snowflake_dbt:
    gold:
      +materialized: "{{ env_var('ATHENA_SNOWFLAKE_DBT_MATERIALIZATION', 'view') }}"
"""


def _render_profiles_yml(state: Dict[str, Any]) -> str:
    target_name = _safe_name(state.get("dbt_target_name") or os.getenv("ATHENA_SNOWFLAKE_DBT_TARGET_NAME") or "dev", prefix="target")
    return f"""athena_snowflake:
  target: {target_name}
  outputs:
    {target_name}:
      type: snowflake
      account: "{{{{ env_var('SNOWFLAKE_ACCOUNT') }}}}"
      user: "{{{{ env_var('SNOWFLAKE_USER') }}}}"
      password: "{{{{ env_var('SNOWFLAKE_PASSWORD') }}}}"
      role: "{{{{ env_var('SNOWFLAKE_ROLE', '') }}}}"
      database: "{{{{ env_var('SNOWFLAKE_DBT_DATABASE', '{_default_dbt_database()}') }}}}"
      warehouse: "{{{{ env_var('SNOWFLAKE_WAREHOUSE') }}}}"
      schema: "{{{{ env_var('SNOWFLAKE_DBT_SCHEMA', '{_default_dbt_schema()}') }}}}"
      threads: {_dbt_threads(state)}
      client_session_keep_alive: false
"""


def _render_model(model: Dict[str, str]) -> str:
    target_table = model["target_table"].replace("*/", "* /")
    return f"""{{{{ config(alias='{model["alias"]}') }}}}

-- Generated by Athena after Gold approval.
-- Source Gold relation: {target_table}
select *
from {model["source_relation"]}
"""


def _render_schema_yml(models: List[Dict[str, str]]) -> str:
    lines = ["version: 2", "", "models:"]
    for model in models:
        description = f"Athena dbt publish wrapper for Snowflake Gold relation {model['target_table']}"
        lines.extend(
            [
                f"  - name: {model['model_name']}",
                f"    description: {json.dumps(description)}",
                "    config:",
                f"      alias: {model['alias']}",
            ]
        )
    return "\n".join(lines) + "\n"


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8", newline="\n")


def _hash_project_files(project_dir: Path) -> Dict[str, Any]:
    excluded_dirs = {"target", "logs", "dbt_packages"}
    excluded_files = {"snowflake_dbt_summary.json"}
    hasher = hashlib.sha256()
    files: List[Dict[str, str]] = []
    for path in sorted(project_dir.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(project_dir)
        if any(part in excluded_dirs for part in relative.parts) or relative.name in excluded_files:
            continue
        data = path.read_bytes()
        file_hash = hashlib.sha256(data).hexdigest()
        rel_posix = relative.as_posix()
        hasher.update(rel_posix.encode("utf-8"))
        hasher.update(b"\0")
        hasher.update(file_hash.encode("ascii"))
        hasher.update(b"\0")
        files.append({"path": rel_posix, "sha256": file_hash})
    return {"artifact_set_hash": hasher.hexdigest(), "files": files}


def build_snowflake_dbt_artifacts(state: Dict[str, Any]) -> Dict[str, Any]:
    run_id = state.get("run_id")
    project_dir = _project_dir(run_id)
    model_dir = project_dir / "models" / "gold"
    _reset_generated_dir(model_dir, root=project_dir)

    models = _gold_outputs(state)
    _write_text(project_dir / "dbt_project.yml", _render_project_yml())
    _write_text(project_dir / "profiles.yml", _render_profiles_yml(state))
    _write_text(model_dir / "schema.yml", _render_schema_yml(models))
    for model in models:
        _write_text(model_dir / f"{model['model_name']}.sql", _render_model(model))

    hash_payload = _hash_project_files(project_dir)
    idempotency_key = hashlib.sha256(
        json.dumps(
            {
                "platform": "snowflake",
                "stage": DBT_STAGE_KEY,
                "target_name": state.get("dbt_target_name") or os.getenv("ATHENA_SNOWFLAKE_DBT_TARGET_NAME") or "dev",
                "database": _default_dbt_database(),
                "schema": _default_dbt_schema(),
                "artifact_set_hash": hash_payload["artifact_set_hash"],
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()

    summary = {
        "run_id": run_id,
        "project_dir": str(project_dir),
        "model_count": len(models),
        "models": models,
        "artifact_set_hash": hash_payload["artifact_set_hash"],
        "artifact_files": hash_payload["files"],
        "idempotency_key": idempotency_key,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    _write_text(project_dir / "snowflake_dbt_summary.json", json.dumps(summary, indent=2, sort_keys=True))
    return {
        **state,
        "snowflake_dbt_artifact_path": str(project_dir),
        "snowflake_dbt_artifact_set_hash": hash_payload["artifact_set_hash"],
        "snowflake_dbt_idempotency_key": idempotency_key,
        "snowflake_dbt_model_count": len(models),
        "snowflake_dbt_models": models,
    }


def _redact(text: str) -> str:
    redacted = str(text or "")
    for key in _SECRET_ENV_NAMES:
        value = os.getenv(key)
        if value and len(value) >= 4:
            redacted = redacted.replace(value, f"<redacted:{key}>")
        redacted = re.sub(rf"({re.escape(key)}\s*=\s*)\S+", rf"\1<redacted:{key}>", redacted, flags=re.IGNORECASE)
    redacted = re.sub(r"(?i)(password|passcode|private_key|token|secret)(\s*[:=]\s*)\S+", r"\1\2<redacted>", redacted)
    return redacted[-8000:]


def _required_deploy_env() -> List[str]:
    return [name for name in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_WAREHOUSE") if not os.getenv(name)]


def _dbt_command_name(command: List[str]) -> str:
    return " ".join(part for part in command if not str(part).startswith(str(Path.cwd())))


def _run_dbt_command(command: List[str], *, project_dir: Path, timeout_seconds: int) -> Dict[str, Any]:
    started_at = time.monotonic()
    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(project_dir)
    completed = subprocess.run(
        command,
        cwd=str(project_dir),
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        check=False,
    )
    elapsed_seconds = round(time.monotonic() - started_at, 2)
    stdout = _redact(completed.stdout)
    stderr = _redact(completed.stderr)
    result = {
        "command": _dbt_command_name(command),
        "returncode": completed.returncode,
        "elapsed_seconds": elapsed_seconds,
        "stdout_tail": stdout,
        "stderr_tail": stderr,
    }
    if completed.returncode != 0:
        message = stderr or stdout or f"return code {completed.returncode}"
        raise RuntimeError(f"Snowflake dbt command failed: {command[2] if len(command) > 2 else command[0]}: {message}")
    return result


def _dbt_target_artifacts(project_dir: Path) -> Dict[str, Dict[str, Any]]:
    artifacts: Dict[str, Dict[str, Any]] = {}
    for name in ("manifest.json", "run_results.json", "catalog.json"):
        path = project_dir / "target" / name
        if not path.exists() or not path.is_file():
            continue
        data = path.read_bytes()
        artifacts[name] = {"path": str(path), "sha256": hashlib.sha256(data).hexdigest(), "size_bytes": len(data)}
    return artifacts


def _write_ai_store_summary(state: Dict[str, Any], payload: Dict[str, Any]) -> None:
    run_id = str(state.get("run_id") or "")
    if not run_id:
        return
    try:
        from utilis.db import ai_store_db_writer

        ai_store_db_writer(
            run_id,
            "snowflake_dbt",
            DBT_ARTIFACT_TYPE,
            payload,
            DBT_SCHEMA_VERSION,
            "deterministic-dbt-snowflake-v1",
            str(payload.get("status") or "COMPLETED"),
            fingerprint=str(payload.get("idempotency_key") or payload.get("artifact_set_hash") or run_id),
        )
    except Exception as exc:
        logger.warning(
            "Snowflake dbt audit write failed: %s",
            exc,
            extra={"run_id": run_id, "node": DBT_STAGE_KEY, "stage": DBT_STAGE_KEY, "step_name": "dbt_audit_write_failed"},
        )


def _with_dbt_progress(
    state: Dict[str, Any],
    *,
    status: str,
    total_count: int,
    completed_count: int,
    current_name: Optional[str] = None,
    message: str,
) -> Dict[str, Any]:
    progress_status = "COMPLETED" if status.startswith("SKIPPED") else status
    updated = save_external_execution_progress(
        state,
        run_id=state.get("run_id"),
        platform="snowflake",
        layer="dbt_deploy",
        stage_key=DBT_STAGE_KEY,
        status=progress_status,
        total_count=total_count,
        completed_count=completed_count,
        current_name=current_name,
        message=message,
    )
    progress = updated.get("external_execution")
    return {
        **updated,
        "snowflake_dbt_deploy_status": status,
        "snowflake_dbt_deploy_progress": progress,
    }


def run_snowflake_dbt(state: Dict[str, Any]) -> Dict[str, Any]:
    if not snowflake_dbt_enabled(state):
        return state

    run_id = state.get("run_id")
    mode = resolve_dbt_deployment_mode(state)
    state = build_snowflake_dbt_artifacts(state)
    project_dir = Path(str(state["snowflake_dbt_artifact_path"]))
    artifact_payload = {
        "status": "GENERATED",
        "mode": mode,
        "project_dir": str(project_dir),
        "model_count": state.get("snowflake_dbt_model_count"),
        "artifact_set_hash": state.get("snowflake_dbt_artifact_set_hash"),
        "idempotency_key": state.get("snowflake_dbt_idempotency_key"),
    }

    if mode == "generate_only":
        final_state = {
            **state,
            "snowflake_dbt_deploy_status": "SKIPPED_GENERATE_ONLY",
            "snowflake_dbt_generated_at": datetime.now(timezone.utc).isoformat(),
        }
        _write_ai_store_summary(final_state, {**artifact_payload, "status": "SKIPPED_GENERATE_ONLY"})
        return _with_dbt_progress(
            final_state,
            status="SKIPPED_GENERATE_ONLY",
            total_count=int(state.get("snowflake_dbt_model_count") or 0),
            completed_count=0,
            message="Snowflake dbt project generated; deployment mode is generate_only.",
        )

    try:
        force = bool(state.get("force_dbt_deploy"))
        if (
            not force
            and state.get("snowflake_dbt_deploy_status") == "COMPLETED"
            and state.get("snowflake_dbt_last_deployed_key") == state.get("snowflake_dbt_idempotency_key")
        ):
            return {
                **state,
                "snowflake_dbt_deploy_status": "SKIPPED_ALREADY_DEPLOYED",
                "resume_message": "Snowflake dbt deployment skipped because this artifact set was already deployed.",
            }

        missing_env = _required_deploy_env()
        if missing_env:
            raise RuntimeError(f"Snowflake dbt deployment is missing required environment variables: {', '.join(missing_env)}")

        dbt_bin = shutil.which("dbt")
        if not dbt_bin:
            raise RuntimeError(
                "Snowflake dbt deployment requested but dbt CLI is not installed. "
                "Install optional backend dependencies with Athena_backend/requirements-dbt.txt."
            )

        target_name = _safe_name(state.get("dbt_target_name") or os.getenv("ATHENA_SNOWFLAKE_DBT_TARGET_NAME") or "dev", prefix="target")
        timeout_seconds = _dbt_timeout_seconds(state)
        commands = [
            [dbt_bin, "--no-use-colors", "parse", "--profiles-dir", str(project_dir), "--project-dir", str(project_dir), "--target", target_name],
            [dbt_bin, "--no-use-colors", "run", "--profiles-dir", str(project_dir), "--project-dir", str(project_dir), "--target", target_name],
        ]

        working_state = _with_dbt_progress(
            {**state, "status": "RUNNING", "background_stage": DBT_STAGE_KEY},
            status="RUNNING",
            total_count=len(commands),
            completed_count=0,
            current_name="parse",
            message="Deploying generated Snowflake dbt project.",
        )
        results: List[Dict[str, Any]] = []
        for index, command in enumerate(commands, start=1):
            command_result = _run_dbt_command(command, project_dir=project_dir, timeout_seconds=timeout_seconds)
            results.append(command_result)
            working_state = _with_dbt_progress(
                working_state,
                status="RUNNING",
                total_count=len(commands),
                completed_count=len(results),
                current_name=command[2] if len(command) > 2 else "dbt",
                message=f"Snowflake dbt deployment progress: {len(results)}/{len(commands)} commands completed.",
            )

        target_artifacts = _dbt_target_artifacts(project_dir)
        completed_at = datetime.now(timezone.utc).isoformat()
        final_payload = {
            **artifact_payload,
            "status": "COMPLETED",
            "completed_at": completed_at,
            "commands": results,
            "target_artifacts": target_artifacts,
        }
        final_state = {
            **working_state,
            "snowflake_dbt_deploy_status": "COMPLETED",
            "snowflake_dbt_deploy_results": results,
            "snowflake_dbt_target_artifacts": target_artifacts,
            "snowflake_dbt_last_deployed_key": state.get("snowflake_dbt_idempotency_key"),
            "snowflake_dbt_deployed_at": completed_at,
        }
        _write_text(project_dir / "snowflake_dbt_deploy_result.json", json.dumps(final_payload, indent=2, sort_keys=True))
        _write_ai_store_summary(final_state, final_payload)
        logger.info(
            "Snowflake dbt deployment completed run_id=%s models=%s artifact_hash=%s",
            run_id,
            state.get("snowflake_dbt_model_count"),
            state.get("snowflake_dbt_artifact_set_hash"),
            extra={"run_id": str(run_id or ""), "node": DBT_STAGE_KEY, "stage": DBT_STAGE_KEY, "step_name": "dbt_deploy_complete"},
        )
        return _with_dbt_progress(
            final_state,
            status="COMPLETED",
            total_count=len(commands),
            completed_count=len(commands),
            message="Snowflake dbt deployment completed.",
        )
    except Exception as exc:
        failed_state = {
            **state,
            "status": "FAILED",
            "background_stage": None,
            "failed_background_stage": DBT_STAGE_KEY,
            "snowflake_dbt_deploy_status": "FAILED",
            "error": str(exc),
            "error_type": type(exc).__name__,
            "error_message": str(exc),
        }
        failed_payload = {**artifact_payload, "status": "FAILED", "error_type": type(exc).__name__, "error_message": str(exc)}
        _write_ai_store_summary(failed_state, failed_payload)
        try:
            failed_state = _with_dbt_progress(
                failed_state,
                status="FAILED",
                total_count=2,
                completed_count=0,
                message="Snowflake dbt deployment failed.",
            )
        except Exception:
            pass
        setattr(exc, "athena_state", failed_state)
        raise
