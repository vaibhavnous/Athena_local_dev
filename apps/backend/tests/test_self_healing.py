from __future__ import annotations

from services import self_healing


def test_self_healing_retries_transient_idempotent_stage(monkeypatch):
    monkeypatch.setenv("ATHENA_SELF_HEAL_MAX_ATTEMPTS", "2")
    monkeypatch.setenv("ATHENA_SELF_HEAL_BACKOFF_BASE_SECONDS", "0")

    state = self_healing.begin_stage_attempt({"run_id": "run-1"}, run_id="run-1", stage_key="discovery")
    failed = self_healing.apply_failure_metadata(
        state,
        stage_key="discovery",
        error="HYT00 timeout while reading catalog",
    )

    assert failed["root_cause_category"] == "TRANSIENT_CONNECTIVITY"
    assert failed["self_healing_action"] == "AUTO_RETRY"
    assert self_healing.should_retry(failed) is True


def test_self_healing_dead_letters_after_exhausted_attempts(monkeypatch):
    monkeypatch.setenv("ATHENA_SELF_HEAL_MAX_ATTEMPTS", "2")
    monkeypatch.setenv("ATHENA_SELF_HEAL_BACKOFF_BASE_SECONDS", "0")

    state = self_healing.begin_stage_attempt({"run_id": "run-1"}, run_id="run-1", stage_key="profiling")
    failed = self_healing.apply_failure_metadata(state, stage_key="profiling", error="connection reset by peer")
    retry_state = self_healing.begin_stage_attempt(failed, run_id="run-1", stage_key="profiling")
    exhausted = self_healing.apply_failure_metadata(retry_state, stage_key="profiling", error="connection reset by peer")

    assert exhausted["self_healing_action"] == "DEAD_LETTER"
    assert exhausted["dead_lettered"] is True
    assert exhausted["dead_letter_stage"] == "profiling"
    assert exhausted["circuit_breaker_open"] is True
    assert self_healing.should_retry(exhausted) is False


def test_self_healing_manual_resume_clears_dead_letter_state(monkeypatch):
    monkeypatch.setenv("ATHENA_SELF_HEAL_MAX_ATTEMPTS", "1")
    state = self_healing.begin_stage_attempt({"run_id": "run-1"}, run_id="run-1", stage_key="kpis")
    failed = self_healing.apply_failure_metadata(state, stage_key="kpis", error="timeout")

    cleaned = self_healing.clear_for_manual_resume(failed)

    assert cleaned["dead_lettered"] is False
    assert cleaned["circuit_breaker_open"] is False
    assert cleaned["self_healing_status"] == "MANUAL_RESUME"


def test_self_healing_does_not_auto_replay_external_execution():
    state = self_healing.begin_stage_attempt({"run_id": "run-1"}, run_id="run-1", stage_key="gold_code_execution")
    failed = self_healing.apply_failure_metadata(
        state,
        stage_key="gold_code_execution",
        error="503 service unavailable",
    )

    assert failed["root_cause_category"] == "TRANSIENT_CONNECTIVITY"
    assert failed["self_healing_action"] == "MANUAL_REQUIRED"
    assert self_healing.should_retry(failed) is False
