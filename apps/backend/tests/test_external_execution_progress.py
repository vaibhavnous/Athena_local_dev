import sys
import types

from services import external_execution_progress


def test_running_progress_checkpoint_saves_are_throttled(monkeypatch):
    saved = []
    pipeline_runtime = types.ModuleType("services.pipeline_runtime")
    pipeline_runtime.save_checkpoint_state = lambda run_id, state: saved.append((run_id, state))

    monkeypatch.setitem(sys.modules, "services.pipeline_runtime", pipeline_runtime)
    monkeypatch.setattr(external_execution_progress, "_LAST_PROGRESS_SAVE_AT", {})
    monkeypatch.setenv("ATHENA_EXTERNAL_PROGRESS_SAVE_INTERVAL_SECONDS", "60")

    state = {"run_id": "run-1"}
    state = external_execution_progress.save_external_execution_progress(
        state,
        run_id="run-1",
        layer="bronze",
        stage_key="bronze_code_execution",
        status="RUNNING",
        total_count=2,
        completed_count=0,
    )
    state = external_execution_progress.save_external_execution_progress(
        state,
        run_id="run-1",
        layer="bronze",
        stage_key="bronze_code_execution",
        status="RUNNING",
        total_count=2,
        completed_count=1,
    )
    external_execution_progress.save_external_execution_progress(
        state,
        run_id="run-1",
        layer="bronze",
        stage_key="bronze_code_execution",
        status="COMPLETED",
        total_count=2,
        completed_count=2,
    )

    assert len(saved) == 2
    assert saved[0][1]["snowflake_bronze_execution_status"] == "RUNNING"
    assert saved[1][1]["snowflake_bronze_execution_status"] == "COMPLETED"
    assert saved[1][1]["background_stage"] is None
    assert saved[1][1]["failed_background_stage"] is None


def test_completed_gold_progress_marks_pipeline_completed(monkeypatch):
    saved = []
    pipeline_runtime = types.ModuleType("services.pipeline_runtime")
    pipeline_runtime.save_checkpoint_state = lambda run_id, state: saved.append((run_id, state))

    monkeypatch.setitem(sys.modules, "services.pipeline_runtime", pipeline_runtime)

    state = external_execution_progress.save_external_execution_progress(
        {"run_id": "run-gold", "status": "RUNNING", "background_stage": "gold_code_execution"},
        run_id="run-gold",
        platform="databricks",
        layer="gold",
        stage_key="gold_code_execution",
        status="COMPLETED",
        total_count=1,
        completed_count=1,
    )

    assert state["status"] == "PIPELINE_COMPLETED"
    assert state["background_stage"] is None
    assert state["databricks_gold_execution_status"] == "COMPLETED"
    assert saved[-1][1]["status"] == "PIPELINE_COMPLETED"
