from __future__ import annotations

import argparse
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
from services.pipeline_runtime import load_bronze_scripts


def _slug(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]+", "_", value).strip("_").lower()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Apply security controls to existing Databricks Bronze tables."
    )
    parser.add_argument(
        "--source-run-id",
        default="",
        help="Run ID whose Bronze artifact should provide the table list.",
    )
    args = parser.parse_args()
    load_dotenv(REPO_ROOT / ".env", override=False)

    run_id = f"smoke-existing-bronze-{uuid.uuid4().hex[:10]}"
    source_run_id = args.source_run_id.strip()
    artifact_scripts = load_bronze_scripts(source_run_id).get("scripts", []) if source_run_id else []
    artifact_tables = []
    for item in artifact_scripts:
        target = str(item.get("target_table") or "").strip()
        table_name = target.split(".")[-1].strip("`\"[]") if target else ""
        if table_name:
            artifact_tables.append(table_name)

    output_dir = REPO_ROOT.parent / "generated_code" / "bronze" / _slug(run_id)
    output_dir.mkdir(parents=True, exist_ok=True)
    copy_security_control_module(str(output_dir))

    script_path = output_dir / "security_existing_bronze_smoke.py"
    artifact_tables_json = json.dumps(sorted(set(artifact_tables)))
    script = f'''
import json
import re

from security_control import apply_security_controls, SecurityControlType

SOURCE_SCHEMA = "workspace.bronze"
TARGET_PREFIX = "secured_"
ASSESSMENT_ID = "assessment-smoke-existing-bronze"
MAX_TABLES = 2
ARTIFACT_TABLES = {artifact_tables_json}

MASK_PATTERNS = (
    "name", "email", "mail", "phone", "mobile", "address", "aadhaar",
    "aadhar", "pan", "ssn", "passport", "account", "ifsc",
)
AUDIT_COLUMNS = {{"run_id", "ingestion_timestamp", "source_system", "source_table"}}
HASH_PATTERNS = (
    "claimid", "policytransid", "payeeid", "hospitalid", "garageid",
    "serviceproviderid", "agent_id", "customerid", "memberid",
)

def _policy_for_column(column_name):
    normalized = re.sub(r"[^a-z0-9]+", "", column_name.lower())
    if any(pattern in normalized for pattern in MASK_PATTERNS):
        return SecurityControlType.MASK
    if any(pattern in normalized for pattern in HASH_PATTERNS):
        return SecurityControlType.HASH
    return None

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {{SOURCE_SCHEMA}}")

catalog_tables = [
    row.tableName
    for row in spark.sql(f"SHOW TABLES IN {{SOURCE_SCHEMA}}").collect()
    if not row.isTemporary
    and row.tableName.startswith("bronze_")
    and not row.tableName.startswith(TARGET_PREFIX)
]
catalog_set = set(catalog_tables)
tables = [table for table in ARTIFACT_TABLES if table in catalog_set]
if not tables:
    tables = catalog_tables

results = []
for table_name in sorted(tables):
    source_table = f"{{SOURCE_SCHEMA}}.{{table_name}}"
    df = spark.table(source_table)
    policies = {{
        column_name: control
        for column_name in df.columns
        if (control := _policy_for_column(column_name)) is not None
    }}
    if not policies:
        fallback_columns = [column for column in df.columns if column.lower() not in AUDIT_COLUMNS][:2]
        policies = {{column: SecurityControlType.MASK for column in fallback_columns}}
    if not policies:
        continue

    target_table = f"{{SOURCE_SCHEMA}}.{{TARGET_PREFIX}}{{table_name}}"
    secured_df = apply_security_controls(
        assessment_id=ASSESSMENT_ID,
        table_name=table_name,
        dataframe=df,
        policies=policies,
    )
    spark.sql(f"DROP TABLE IF EXISTS {{target_table}}")
    secured_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
    sample = [row.asDict() for row in spark.table(target_table).limit(3).collect()]
    results.append({{
        "source_table": source_table,
        "target_table": target_table,
        "policy_columns": {{name: control.value for name, control in policies.items()}},
        "row_count": spark.table(target_table).count(),
        "sample": sample,
    }})
    if len(results) >= MAX_TABLES:
        break

if not results:
    raise RuntimeError(f"No transformable columns found in {{SOURCE_SCHEMA}} bronze tables. artifact_tables={{ARTIFACT_TABLES}} catalog_tables={{catalog_tables}}")

dbutils.notebook.exit(json.dumps({{
    "assessment_id": ASSESSMENT_ID,
    "source_run_id": "{source_run_id}",
    "artifact_tables": ARTIFACT_TABLES,
    "secured_tables": results,
}}, default=str))
'''
    script_path.write_text(script.strip() + "\n", encoding="utf-8")

    state = {
        "run_id": run_id,
        "target_warehouse": "databricks",
        "bronze_generation_results": [
            {
                "run_id": run_id,
                "table": "existing_bronze_security_smoke",
                "target_table": "workspace.bronze.secured_*",
                "script_path": str(script_path),
                "target_warehouse": "databricks",
                "script_language": "python",
                "security_enabled": True,
                "assessment_id": "assessment-smoke-existing-bronze",
            }
        ],
    }

    print(json.dumps({
        "run_id": run_id,
        "source_run_id": source_run_id,
        "artifact_tables": artifact_tables,
        "script_path": str(script_path),
    }, indent=2))
    result = databricks_runtime.run_databricks_bronze_scripts(state)
    print(json.dumps({
        "execution_status": result.get("databricks_bronze_execution_status"),
        "execution_results": result.get("databricks_bronze_execution_results"),
    }, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
