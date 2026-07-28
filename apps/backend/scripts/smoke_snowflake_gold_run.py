from __future__ import annotations

import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nodes.gold_gen import gold_code_generation_node
from services.pipeline_runtime import load_checkpoint_state, save_checkpoint_state
from services.snowflake_bronze_runtime import _snowflake_connect
from services.snowflake_gold_runtime import execute_snowflake_gold_sql


DEFAULT_RUN_ID = "10d7da39-b6b6-4298-a190-4bc1a255d73c"


def main() -> None:
    run_id = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_RUN_ID
    load_dotenv(".env")
    state = load_checkpoint_state(run_id) or {"run_id": run_id}
    state["target_warehouse"] = "snowflake"
    refreshed = gold_code_generation_node(state)
    save_checkpoint_state(run_id, refreshed)

    scripts = [
        item
        for item in refreshed.get("gold_generation_results") or []
        if isinstance(item, dict) and item.get("status") == "APPROVED" and item.get("script_path")
    ]
    print(f"REGENERATED run_id={run_id} approved_executable={len(scripts)}")

    executed_results = []
    conn = _snowflake_connect()
    try:
        for script in scripts:
            result = execute_snowflake_gold_sql(script, conn)
            executed_results.append(result)
            print(
                "EXECUTED "
                f"kpi={result['kpi_name']} "
                f"statements={result['statement_count']} "
                f"target={result['target_table']}"
            )
    finally:
        conn.close()

    save_checkpoint_state(
        run_id,
        {
            **refreshed,
            "status": "PIPELINE_COMPLETED",
            "background_stage": None,
            "next_gate": None,
            "next_review_key": None,
            "error": None,
            "snowflake_gold_execution_status": "COMPLETED",
            "snowflake_gold_execution_results": executed_results,
        },
    )


if __name__ == "__main__":
    main()
