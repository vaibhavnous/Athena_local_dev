from __future__ import annotations

import json
import re
import sys
import uuid
from pathlib import Path

from dotenv import load_dotenv


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from nodes.bronze_gen import copy_security_control_module
from services import databricks_runtime


def _slug(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]+", "_", value).strip("_").lower()


def main() -> int:
    load_dotenv(REPO_ROOT / ".env", override=False)

    run_id = f"smoke-compliance-{uuid.uuid4().hex[:10]}"
    table_name = f"athena_security_smoke_{_slug(run_id)}"
    target_table = f"workspace.bronze.{table_name}"
    output_dir = REPO_ROOT.parent / "generated_code" / "bronze" / _slug(run_id)
    output_dir.mkdir(parents=True, exist_ok=True)
    copy_security_control_module(str(output_dir))

    script_path = output_dir / "security_smoke.py"
    script = f'''
from pyspark.sql import Row
from security_control import apply_security_controls, SecurityControlType

TARGET_TABLE = "{target_table}"

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")
spark.sql(f"DROP TABLE IF EXISTS {{TARGET_TABLE}}")

df = spark.createDataFrame([
    Row(claimid="C-1001", email="vaibhav@example.com", amount=1250.0),
    Row(claimid="C-1002", email="admin@example.com", amount=500.0),
])

secured_df = apply_security_controls(
    assessment_id="assessment-smoke",
    table_name="{table_name}",
    dataframe=df,
    policies={{
        "claimid": SecurityControlType.HASH,
        "email": SecurityControlType.MASK,
    }},
)

secured_df.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)
rows = [row.asDict() for row in spark.table(TARGET_TABLE).orderBy("amount").collect()]
print("ATHENA_SECURITY_SMOKE_ROWS=" + str(rows))
'''
    script_path.write_text(script.strip() + "\n", encoding="utf-8")

    state = {
        "run_id": run_id,
        "target_warehouse": "databricks",
        "bronze_generation_results": [
            {
                "run_id": run_id,
                "table": table_name,
                "target_table": target_table,
                "script_path": str(script_path),
                "target_warehouse": "databricks",
                "script_language": "python",
                "security_enabled": True,
                "assessment_id": "assessment-smoke",
                "security_policy_columns": ["claimid", "email"],
            }
        ],
    }

    print(json.dumps({"run_id": run_id, "target_table": target_table, "script_path": str(script_path)}, indent=2))
    result = databricks_runtime.run_databricks_bronze_scripts(state)
    print(json.dumps({
        "execution_status": result.get("databricks_bronze_execution_status"),
        "execution_results": result.get("databricks_bronze_execution_results"),
        "target_table": target_table,
    }, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
