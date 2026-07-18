from __future__ import annotations

import json
import os
import sys
import uuid
from pathlib import Path

from dotenv import load_dotenv


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from nodes import bronze_gen
from services import databricks_runtime


def main() -> int:
    load_dotenv(REPO_ROOT / ".env", override=False)

    run_id = f"smoke-security-{uuid.uuid4().hex[:10]}"
    state = {
        "run_id": run_id,
        "target_warehouse": "databricks",
        "bronze_catalog": "workspace",
        "bronze_schema": "bronze",
        "compliance_assessment_id": "assessment-smoke",
        "security_policies": {
            "Claims": {
                "Email": "Mask",
                "ClaimID": "Hash",
            }
        },
        "certified_tables": [
            {
                "database_name": "insurance",
                "schema_name": "dbo",
                "table_name": "Claims",
            }
        ],
        # ponytail: smoke uses a dummy JDBC URL because the helper import should fail before source access.
        "source_jdbc_url": "jdbc:sqlserver://example.invalid:1433;databaseName=insurance;encrypt=true;trustServerCertificate=true",
    }

    generated = bronze_gen.bronze_code_generation_node(state)
    results = generated.get("bronze_generation_results") or []
    if not results:
        raise RuntimeError("Bronze generation produced no scripts.")

    artifact = results[0]
    script_path = Path(str(artifact["script_path"]))
    helper_path = script_path.parent / "security_control.py"
    script_text = script_path.read_text(encoding="utf-8")

    print(
        json.dumps(
            {
                "run_id": run_id,
                "script_path": str(script_path),
                "security_enabled": artifact.get("security_enabled"),
                "assessment_id": artifact.get("assessment_id"),
                "security_policy_columns": artifact.get("security_policy_columns"),
                "helper_exists_locally": helper_path.exists(),
                "script_imports_helper": "from security_control import apply_security_controls, SecurityControlType" in script_text,
                "script_calls_helper": "apply_security_controls(" in script_text,
            },
            indent=2,
        )
    )

    executed = databricks_runtime.run_databricks_bronze_scripts(generated)
    print(
        json.dumps(
            {
                "execution_status": executed.get("databricks_bronze_execution_status"),
                "execution_results": executed.get("databricks_bronze_execution_results"),
            },
            indent=2,
            default=str,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
