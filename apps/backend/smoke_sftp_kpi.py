from __future__ import annotations

import json
import sys
import uuid

from services.pipeline_runtime import start_pipeline


def main() -> int:
    run_id = str(uuid.uuid4())
    print(f"[smoke] run_id={run_id}")

    try:
        result = start_pipeline(run_id=run_id, source="sftp", brd_text="")
    except Exception as exc:
        print(f"[smoke] start_pipeline raised: {exc}")
        return 2

    state = result.get("result") if isinstance(result, dict) else None
    if not isinstance(state, dict):
        print("[smoke] no state returned")
        return 3

    status = state.get("status")
    kpis = state.get("kpis") or []
    print(f"[smoke] status={status}")
    print(f"[smoke] kpi_count={len(kpis) if isinstance(kpis, list) else 'n/a'}")

    # Print a compact view of the first few KPI names if present.
    if isinstance(kpis, list) and kpis:
        names = []
        for item in kpis[:5]:
            if isinstance(item, dict):
                names.append(item.get("kpi_name") or item.get("name") or item.get("title") or "unknown")
        print("[smoke] sample_kpis=" + json.dumps(names))

    # Helpful debug markers for why we might have stopped early.
    if status == "FAILED":
        print(f"[smoke] error={state.get('error')}")
    print(f"[smoke] source_ingestion_status={state.get('source_ingestion_status')}")
    print(f"[smoke] context_text_len={len(state.get('context_text') or '')}")
    if state.get("source_columns"):
        print("[smoke] source_columns=" + json.dumps(state.get("source_columns")))
    if state.get("source_row_count") is not None:
        print(f"[smoke] source_row_count={state.get('source_row_count')}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
