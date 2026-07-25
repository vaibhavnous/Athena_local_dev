from __future__ import annotations

import hashlib
import json
import os
import re
import time
from typing import Any, Dict


IDEMPOTENT_DATABASE_STAGES = {
    "ingestion",
    "memory",
    "requirements",
    "kpis",
    "nomination",
    "discovery",
    "profiling",
    "enrichment",
    "bronze",
    "silver",
    "gold",
}
NON_REPLAYABLE_STAGES = {
    "gate1",
    "gate2",
    "gate3",
    "gate4",
    "gate5",
    "silver_merge_key_review",
    "gold_review",
    "bronze_code_execution",
    "silver_code_execution",
    "gold_code_execution",
    "file_resume",
    "pipeline",
}
TRANSIENT_PATTERNS = (
    r"\btimeout\b",
    r"\btimed out\b",
    r"temporar(?:y|ily)",
    r"connection (?:reset|aborted|closed|refused)",
    r"transport-level error",
    r"\b08s01\b",
    r"\b40001\b",
    r"\b40613\b",
    r"\bhyt00\b",
    r"service unavailable",
    r"\b(?:502|503|504)\b",
    r"throttl",
    r"too many requests",
    r"rate limit",
)
RESOURCE_PATTERNS = (
    r"capacity is full",
    r"cluster .*terminated",
    r"warehouse .*suspend",
    r"warehouse .*not .*running",
)
AUTH_PATTERNS = (
    r"login failed",
    r"unauthorized",
    r"forbidden",
    r"access denied",
    r"permission denied",
    r"invalid token",
    r"expired token",
    r"credential",
)
DATA_CONTRACT_PATTERNS = (
    r"validation",
    r"missing column",
    r"unknown column",
    r"schema mismatch",
    r"not waiting for gate",
    r"unsupported",
    r"forbidden statement",
    r"destructive",
)
EVENT_LIMIT = 100


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, *, minimum: int = 0, maximum: int = 10) -> int:
    try:
        return min(maximum, max(minimum, int(os.getenv(name, str(default)))))
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float, *, minimum: float = 0.0, maximum: float = 60.0) -> float:
    try:
        return min(maximum, max(minimum, float(os.getenv(name, str(default)))))
    except (TypeError, ValueError):
        return default


def enabled() -> bool:
    return _env_bool("ATHENA_SELF_HEALING_ENABLED", True)


def auto_resume_interrupted_enabled() -> bool:
    return enabled() and _env_bool("ATHENA_SELF_HEAL_AUTO_RESUME_INTERRUPTED", True)


def max_attempts() -> int:
    return _env_int("ATHENA_SELF_HEAL_MAX_ATTEMPTS", 2, minimum=1, maximum=5)


def retry_delay_seconds(attempt_number: int) -> float:
    base = _env_float("ATHENA_SELF_HEAL_BACKOFF_BASE_SECONDS", 1.0, minimum=0.0, maximum=30.0)
    cap = _env_float("ATHENA_SELF_HEAL_BACKOFF_MAX_SECONDS", 8.0, minimum=0.0, maximum=120.0)
    return min(cap, base * (2 ** max(0, attempt_number - 1)))


def is_stage_replayable(stage_key: Any) -> bool:
    stage = str(stage_key or "").strip().lower()
    if stage in NON_REPLAYABLE_STAGES:
        return False
    return stage in IDEMPOTENT_DATABASE_STAGES


def replay_key(run_id: Any, stage_key: Any, state: Dict[str, Any]) -> str:
    payload = {
        "run_id": str(run_id or state.get("run_id") or ""),
        "stage_key": str(stage_key or ""),
        "project_id": state.get("project_id"),
        "source": state.get("source"),
        "target_warehouse": state.get("target_warehouse"),
        "execution_engine": state.get("execution_engine"),
        "source_databases": state.get("source_databases"),
        "database_name": state.get("database_name"),
        "brd_filename": state.get("brd_filename"),
        "last_completed_stage_key": state.get("last_completed_stage_key"),
    }
    data = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def _self_healing(state: Dict[str, Any]) -> Dict[str, Any]:
    current = state.get("self_healing")
    return dict(current) if isinstance(current, dict) else {}


def append_event(state: Dict[str, Any], *, stage_key: Any, event: str, **details: Any) -> Dict[str, Any]:
    healing = _self_healing(state)
    events = healing.get("events")
    if not isinstance(events, list):
        events = []
    events.append(
        {
            "event": event,
            "stage_key": str(stage_key or ""),
            "at": time.time(),
            **{key: value for key, value in details.items() if value is not None},
        }
    )
    healing["events"] = events[-EVENT_LIMIT:]
    return {**state, "self_healing": healing}


def begin_stage_attempt(state: Dict[str, Any], *, run_id: Any, stage_key: Any) -> Dict[str, Any]:
    if not enabled():
        return state
    stage = str(stage_key or "").strip().lower()
    healing = _self_healing(state)
    attempts = healing.get("stage_attempts")
    if not isinstance(attempts, dict):
        attempts = {}
    attempt_number = int(attempts.get(stage) or 0) + 1
    attempts[stage] = attempt_number
    replayable = is_stage_replayable(stage)
    healing.update(
        {
            "enabled": True,
            "status": "RUNNING",
            "stage_key": stage,
            "stage_attempts": attempts,
            "current_attempt": attempt_number,
            "current_replay_key": replay_key(run_id, stage, state),
            "replay_contract": {
                "stage_key": stage,
                "idempotent": replayable,
                "max_attempts": max_attempts(),
            },
        }
    )
    updated = {**state, "self_healing": healing, "self_healing_status": "RUNNING"}
    return append_event(updated, stage_key=stage, event="stage_attempt_started", attempt=attempt_number, replayable=replayable)


def classify_failure(error: Any, *, stage_key: Any = None) -> Dict[str, Any]:
    message = str(error or "").strip()
    normalized = message.lower()
    category = "UNKNOWN"
    retryable = False
    recovery_action = "manual_review"

    def _matches(patterns: tuple[str, ...]) -> bool:
        return any(re.search(pattern, normalized, re.IGNORECASE) for pattern in patterns)

    if _matches(AUTH_PATTERNS):
        category = "AUTH_OR_PERMISSION"
        recovery_action = "fix_credentials_or_permissions"
    elif _matches(DATA_CONTRACT_PATTERNS):
        category = "DATA_CONTRACT_OR_VALIDATION"
        recovery_action = "regenerate_or_review_contract"
    elif _matches(RESOURCE_PATTERNS):
        category = "RESOURCE_CAPACITY"
        retryable = True
        recovery_action = "retry_after_capacity_recovers"
    elif _matches(TRANSIENT_PATTERNS):
        category = "TRANSIENT_CONNECTIVITY"
        retryable = True
        recovery_action = "retry_same_stage"
    elif str(stage_key or "").strip().lower() in {"interrupted", "backend_restart"} or "backend process restarted" in normalized:
        category = "RUNTIME_INTERRUPTION"
        retryable = True
        recovery_action = "replay_idempotent_stage"

    return {
        "category": category,
        "retryable": retryable,
        "message": message,
        "recovery_action": recovery_action,
    }


def decide_recovery(state: Dict[str, Any], *, stage_key: Any, failure: Dict[str, Any]) -> Dict[str, Any]:
    stage = str(stage_key or "").strip().lower()
    healing = _self_healing(state)
    attempts = healing.get("stage_attempts") if isinstance(healing.get("stage_attempts"), dict) else {}
    circuit_breakers = healing.get("circuit_breakers") if isinstance(healing.get("circuit_breakers"), dict) else {}
    circuit = circuit_breakers.get(stage) if isinstance(circuit_breakers.get(stage), dict) else {}
    attempt_number = int(attempts.get(stage) or healing.get("current_attempt") or 1)
    replayable = is_stage_replayable(stage)
    retryable = bool(failure.get("retryable"))
    max_retry_attempts = max_attempts()

    if circuit.get("open"):
        action = "DEAD_LETTER"
        reason = f"Circuit breaker is open for stage '{stage}'."
    elif not enabled():
        action = "MANUAL_REQUIRED"
        reason = "Self-healing is disabled."
    elif not replayable:
        action = "MANUAL_REQUIRED"
        reason = f"Stage '{stage}' is not safe for automatic replay."
    elif not retryable:
        action = "MANUAL_REQUIRED"
        reason = f"Failure category {failure.get('category')} is not automatically retryable."
    elif attempt_number < max_retry_attempts:
        action = "AUTO_RETRY"
        reason = f"Retrying transient failure for idempotent stage '{stage}'."
    else:
        action = "DEAD_LETTER"
        reason = f"Stage '{stage}' exhausted {max_retry_attempts} self-healing attempt(s)."

    return {
        "action": action,
        "reason": reason,
        "stage_key": stage,
        "attempt": attempt_number,
        "max_attempts": max_retry_attempts,
        "retry_delay_seconds": retry_delay_seconds(attempt_number),
        "replayable": replayable,
        "retryable": retryable,
        "category": failure.get("category"),
        "recovery_action": failure.get("recovery_action"),
        "circuit_breaker_open": action == "DEAD_LETTER",
    }


def apply_failure_metadata(state: Dict[str, Any], *, stage_key: Any, error: Any) -> Dict[str, Any]:
    failure = classify_failure(error, stage_key=stage_key)
    decision = decide_recovery(state, stage_key=stage_key, failure=failure)
    healing = _self_healing(state)
    healing.update(
        {
            "status": decision["action"],
            "last_failure": failure,
            "last_decision": decision,
        }
    )
    if decision["action"] == "DEAD_LETTER":
        circuit_breakers = healing.get("circuit_breakers") if isinstance(healing.get("circuit_breakers"), dict) else {}
        circuit_breakers[str(stage_key or "").strip().lower()] = {
            "open": True,
            "opened_at": time.time(),
            "reason": decision["reason"],
            "category": failure["category"],
        }
        healing["circuit_breakers"] = circuit_breakers
    updated = {
        **state,
        "self_healing": healing,
        "self_healing_status": decision["action"],
        "self_healing_action": decision["action"],
        "self_healing_retryable": bool(decision["retryable"] and decision["replayable"]),
        "self_healing_attempt": decision["attempt"],
        "root_cause_category": failure["category"],
        "root_cause_detail": failure["message"],
        "recommended_recovery_action": decision["recovery_action"],
        "circuit_breaker_open": bool(decision["circuit_breaker_open"]),
    }
    if decision["action"] == "DEAD_LETTER":
        updated.update(
            {
                "dead_lettered": True,
                "dead_letter_stage": str(stage_key or ""),
                "dead_letter_reason": decision["reason"],
                "dead_letter_at": time.time(),
            }
        )
    return append_event(
        updated,
        stage_key=stage_key,
        event="stage_failure_classified",
        category=failure["category"],
        action=decision["action"],
        attempt=decision["attempt"],
        reason=decision["reason"],
    )


def should_retry(state: Dict[str, Any]) -> bool:
    decision = (_self_healing(state).get("last_decision") or {})
    return decision.get("action") == "AUTO_RETRY"


def should_auto_resume_interrupted(state: Dict[str, Any]) -> bool:
    if not auto_resume_interrupted_enabled():
        return False
    stage = state.get("failed_background_stage") or state.get("background_stage")
    return is_stage_replayable(stage) and str(state.get("source") or "database").lower() not in {"sftp", "adls_gen2"}


def clear_for_manual_resume(state: Dict[str, Any]) -> Dict[str, Any]:
    healing = _self_healing(state)
    healing["status"] = "MANUAL_RESUME"
    healing.pop("last_decision", None)
    circuit_breakers = healing.get("circuit_breakers") if isinstance(healing.get("circuit_breakers"), dict) else {}
    for breaker in circuit_breakers.values():
        if isinstance(breaker, dict):
            breaker["open"] = False
            breaker["closed_at"] = time.time()
            breaker["closed_by"] = "manual_resume"
    healing["circuit_breakers"] = circuit_breakers
    updated = {
        **state,
        "self_healing": healing,
        "self_healing_status": "MANUAL_RESUME",
        "self_healing_action": None,
        "self_healing_retryable": None,
        "dead_lettered": False,
        "dead_letter_stage": None,
        "dead_letter_reason": None,
        "dead_letter_at": None,
        "root_cause_category": None,
        "root_cause_detail": None,
        "recommended_recovery_action": None,
        "circuit_breaker_open": False,
    }
    return append_event(updated, stage_key=state.get("failed_background_stage"), event="manual_resume_requested")
