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


def _artifact_tables(source_run_id: str) -> list[str]:
    scripts = load_bronze_scripts(source_run_id).get("scripts", []) if source_run_id else []
    tables: list[str] = []
    for item in scripts:
        target = str(item.get("target_table") or "").strip()
        table_name = target.split(".")[-1].strip("`\"[]") if target else ""
        if table_name:
            tables.append(table_name)
    return sorted(set(tables))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Apply security controls to Databricks Bronze tables from a Bronze artifact."
    )
    parser.add_argument("--source-run-id", required=True, help="Pipeline run_id whose Bronze artifact provides tables.")
    parser.add_argument("--max-tables", type=int, default=2, help="Maximum Bronze tables to transform.")
    args = parser.parse_args()

    load_dotenv(REPO_ROOT / ".env", override=False)

    source_run_id = args.source_run_id.strip()
    run_id = f"security-bronze-{uuid.uuid4().hex[:10]}"
    tables = _artifact_tables(source_run_id)
    if not tables:
        raise RuntimeError(f"No Bronze tables found in artifact for run_id={source_run_id}")

    output_dir = REPO_ROOT.parent / "generated_code" / "bronze_security" / _slug(run_id)
    output_dir.mkdir(parents=True, exist_ok=True)
    copy_security_control_module(str(output_dir))

    script_path = output_dir / "apply_security_to_bronze_tables.py"
    artifact_tables_json = json.dumps(tables)
    max_tables = max(1, int(args.max_tables))
    script = f'''
import json
import re

from security_control import apply_security_controls, SecurityControlType

ASSESSMENT_ID = "assessment-existing-bronze"
ARTIFACT_TABLES = {artifact_tables_json}
MAX_TABLES = {max_tables}
AUDIT_COLUMNS = {{"run_id", "ingestion_timestamp", "source_system", "source_table"}}

MASK_PATTERNS = (
    "name", "email", "mail", "phone", "mobile", "address", "aadhaar",
    "aadhar", "pan", "ssn", "passport", "account", "ifsc",
)
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

def _quote(identifier):
    return "`" + identifier.replace("`", "``") + "`"

catalog_tables = []
table_locations = {{}}
catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
for catalog_name in catalogs:
    try:
        schemas = [row.databaseName for row in spark.sql(f"SHOW SCHEMAS IN {{_quote(catalog_name)}}").collect()]
    except Exception:
        continue
    for schema_name in schemas:
        schema_fqn = f"{{_quote(catalog_name)}}.{{_quote(schema_name)}}"
        try:
            rows = spark.sql(f"SHOW TABLES IN {{schema_fqn}}").collect()
        except Exception:
            continue
        for row in rows:
            if row.isTemporary:
                continue
            table_name = row.tableName
            catalog_tables.append(f"{{catalog_name}}.{{schema_name}}.{{table_name}}")
            if table_name not in table_locations:
                table_locations[table_name] = (catalog_name, schema_name)

tables = [table for table in ARTIFACT_TABLES if table in table_locations]

results = []
for table_name in tables:
    catalog_name, schema_name = table_locations[table_name]
    source_table = f"{{_quote(catalog_name)}}.{{_quote(schema_name)}}.{{_quote(table_name)}}"
    target_table = f"{{_quote(catalog_name)}}.{{_quote(schema_name)}}.{{_quote(table_name + '_secured')}}"
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

    secured_df = apply_security_controls(
        assessment_id=ASSESSMENT_ID,
        table_name=table_name,
        dataframe=df,
        policies=policies,
    )
    spark.sql(f"DROP TABLE IF EXISTS {{target_table}}")
    secured_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
    results.append({{
        "source_table": source_table,
        "target_table": target_table,
        "policy_columns": {{name: control.value for name, control in policies.items()}},
        "row_count": spark.table(target_table).count(),
    }})
    if len(results) >= MAX_TABLES:
        break

if not results:
    raise RuntimeError(f"No transformable Bronze tables found. artifact_tables={{ARTIFACT_TABLES}} catalog_tables={{catalog_tables}}")

print("ATHENA_SECURITY_RESULT=" + json.dumps({{
    "assessment_id": ASSESSMENT_ID,
    "source_run_id": "{source_run_id}",
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
                "table": "apply_security_to_bronze_tables",
                "target_table": "workspace.bronze.<bronze_table>_secured",
                "script_path": str(script_path),
                "target_warehouse": "databricks",
                "script_language": "python",
                "security_enabled": True,
                "assessment_id": "assessment-existing-bronze",
            }
        ],
    }

    print(json.dumps({
        "run_id": run_id,
        "source_run_id": source_run_id,
        "artifact_tables": tables,
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
