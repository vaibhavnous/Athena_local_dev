from __future__ import annotations

import os
import json
import time
import uuid
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


DEMO_RUN_ID = "run_a3f8c2"
DEMO_COMPLETED_RUN_ID = "run_b7e1d3"
DEMO_ASSET_RUN_ID = "33e4af14-9875-4866-b15c-e2e39835154e"
ROOT_DIR = Path(__file__).resolve().parents[1]
GENERATED_CODE_DIR = ROOT_DIR / "generated_code"


def demo_enabled() -> bool:
    return str(os.getenv("ATHENA_DEMO_MODE", "true")).strip().lower() in {"1", "true", "yes", "on"}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(minutes_ago: int) -> str:
    return (_now() - timedelta(minutes=minutes_ago)).isoformat()


def _stage(key: str, label: str, status: str, index: int) -> Dict[str, Any]:
    completed = status in {"COMPLETED", "SUCCESS", "HITL_WAIT"}
    return {
        "id": key,
        "key": key,
        "name": label,
        "label": label,
        "status": status,
        "state": status,
        "tokens": 2400 + index * 375 if completed else 0,
        "cost": round((2400 + index * 375) / 100000, 4) if completed else 0,
        "attempts": 1 if completed else 0,
        "started_at": _iso(18 - index) if completed else None,
        "completed_at": _iso(17 - index) if completed and status != "HITL_WAIT" else None,
        "error": None,
        "prompt_metadata": {"model": "gpt-4.1", "temperature": 0.0} if completed else None,
    }


def _load_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _asset_bundle(layer: str) -> Dict[str, Any]:
    filename = f"{DEMO_ASSET_RUN_ID.replace('-', '_')}_{layer}_scripts.json"
    return _load_json(GENERATED_CODE_DIR / layer / filename)


def _generated_script_body(layer: str, script_path: Any) -> str:
    if not script_path:
        return ""
    filename = Path(str(script_path).replace("\\", "/")).name
    candidates = [
        GENERATED_CODE_DIR / layer / filename,
        GENERATED_CODE_DIR
        / layer
        / filename.replace(DEMO_RUN_ID.replace("-", "_"), DEMO_ASSET_RUN_ID.replace("-", "_")).replace(
            DEMO_COMPLETED_RUN_ID.replace("-", "_"),
            DEMO_ASSET_RUN_ID.replace("-", "_"),
        ),
    ]
    for path in candidates:
        try:
            return path.read_text(encoding="utf-8")
        except Exception:
            continue
    return ""


def _with_run_id(value: Any, run_id: str) -> Any:
    if isinstance(value, dict):
        return {key: _with_run_id(item, run_id) for key, item in value.items()}
    if isinstance(value, list):
        return [_with_run_id(item, run_id) for item in value]
    if isinstance(value, str):
        return value.replace(DEMO_ASSET_RUN_ID, run_id).replace(DEMO_ASSET_RUN_ID.replace("-", "_"), run_id.replace("-", "_"))
    return value


def _gold_bundle() -> Dict[str, Any]:
    return _asset_bundle("gold")


def _bronze_bundle() -> Dict[str, Any]:
    return _asset_bundle("bronze")


def _silver_bundle() -> Dict[str, Any]:
    return _asset_bundle("silver")


def _fallback_kpis() -> List[Dict[str, Any]]:
    names = [
        ("Total Claims Processed Count", "Count of processed claims by policy, product, channel, and month.", "Claims"),
        ("Average Claim Processing Time (Days)", "Average elapsed days between claim registration and payment activity.", "Claims"),
        ("Total Outstanding Claims Count", "Number of open outstanding claim reserve records by period.", "Claims"),
        ("Total Premium Collected Amount", "Total collected premium amount across cover-level policy transactions.", "Premium"),
        ("Average Premium Per Policy Amount", "Average premium value per policy transaction and product segment.", "Premium"),
        ("Average Coverage Sum Insured Amount", "Average insured coverage value by cover, region, and policy period.", "Coverage"),
        ("Total Expenses Incurred Amount", "Total allocated claim expense amount for operational cost reporting.", "Claims Finance"),
        ("Total Risk Sum Insured Amount", "Total risk sum insured amount across policy transactions.", "Risk"),
    ]
    return [
        {
            "id": f"{DEMO_RUN_ID}:1:{index}",
            "queue_id": f"{DEMO_RUN_ID}:1:{index}",
            "item_id": f"{DEMO_RUN_ID}:1:{index}",
            "item_type": "KPI",
            "name": name,
            "kpi_name": name,
            "definition": definition,
            "kpi_description": definition,
            "category": category,
            "domain": "P&C Insurance Analytics",
            "confidence": round(0.96 - index * 0.015, 2),
            "ai_confidence_score": round(0.96 - index * 0.015, 2),
            "status": "PENDING_REVIEW",
            "gate_status": "PENDING",
            "decision": None,
            "grounded": True,
            "explicit": index < 5,
            "source_requirement_ref": "Insurance management dashboard metrics",
            "run_id": DEMO_RUN_ID,
            "source": "database",
        }
        for index, (name, definition, category) in enumerate(names)
    ]


def _kpis_from_gold_bundle() -> List[Dict[str, Any]]:
    scripts = (_gold_bundle().get("scripts") or [])[:8]
    if not scripts:
        return _fallback_kpis()
    rows: List[Dict[str, Any]] = []
    for index, script in enumerate(scripts):
        name = str(script.get("kpi_name") or f"Insurance KPI {index + 1}")
        source_table = str(script.get("source_table") or "silver.insurance")
        target_table = str(script.get("target_table") or "gold.insurance_kpi")
        definition = f"Builds {target_table} from {source_table} for P&C insurance analytics."
        rows.append(
            {
                "id": f"{DEMO_RUN_ID}:1:{index}",
                "queue_id": f"{DEMO_RUN_ID}:1:{index}",
                "item_id": f"{DEMO_RUN_ID}:1:{index}",
                "item_type": "KPI",
                "name": name,
                "kpi_name": name,
                "definition": definition,
                "kpi_description": definition,
                "category": "Insurance Analytics",
                "domain": "P&C Insurance Analytics",
                "confidence": round(0.97 - index * 0.01, 2),
                "ai_confidence_score": round(0.97 - index * 0.01, 2),
                "status": "PENDING_REVIEW",
                "gate_status": "PENDING",
                "decision": None,
                "grounded": True,
                "explicit": True,
                "source_requirement_ref": "Insurance management dashboard metrics",
                "source_table": source_table,
                "target_table": target_table,
                "generation_mode": script.get("generation_mode") or "DETERMINISTIC",
                "run_id": DEMO_RUN_ID,
                "source": "database",
            }
        )
    return rows


DEMO_KPIS: List[Dict[str, Any]] = _kpis_from_gold_bundle()


def demo_requirements() -> Dict[str, Any]:
    return {
        "objective": "Build a comprehensive insurance analytics dashboard to track policy performance, claims processing efficiency, and customer retention metrics.",
        "business_objective": "Build a comprehensive insurance analytics dashboard to track policy performance, claims processing efficiency, and customer retention metrics.",
        "data_domains": ["Policy Management", "Claims Processing", "Customer Data", "Financial Data", "Actuarial Data"],
        "reporting_frequency": "Daily with monthly aggregations",
        "target_audience": "Insurance Operations Team, Actuarial Department, C-Suite",
        "constraints": [
            "GDPR compliance for EU customer data",
            "Data retention max 7 years",
            "Real-time claims under 5s SLA",
            "SOX compliance for financial metrics",
        ],
        "schema_valid": True,
        "faithfulness_score": 0.94,
        "retry_count": 0,
        "prompt_version": "ATHENA_INSURANCE_v3",
    }


def demo_stages() -> List[Dict[str, Any]]:
    return [
        _stage("ingestion", "BRD Ingestion", "COMPLETED", 1),
        _stage("memory", "Memory Intelligence", "COMPLETED", 2),
        _stage("requirements", "Requirement Extraction", "COMPLETED", 3),
        _stage("kpis", "KPI Extraction", "COMPLETED", 4),
        _stage("gate1", "KPI Review", "HITL_WAIT", 5),
        _stage("nomination", "Table Extraction", "PENDING", 6),
        _stage("gate2", "Table Review", "PENDING", 7),
        _stage("discovery", "Column Extraction", "PENDING", 8),
        _stage("profiling", "Column Extraction", "PENDING", 9),
        _stage("enrichment", "Column Extraction", "PENDING", 10),
        _stage("gate3", "Column Review", "PENDING", 11),
        _stage("bronze", "Bronze Code Generation", "PENDING", 12),
        _stage("gate4", "Bronze Review", "PENDING", 13),
        _stage("silver", "Silver Code Generation", "PENDING", 14),
        _stage("gate5", "Silver Review", "PENDING", 15),
        _stage("gold", "Gold Code Generation", "PENDING", 16),
    ]


def demo_tables() -> List[Dict[str, Any]]:
    bronze_scripts = _bronze_bundle().get("scripts") or []
    rows = []
    for index, item in enumerate(bronze_scripts[:8]):
        table_name = item.get("table") or f"insurance_table_{index + 1}"
        rows.append(
            {
                "id": f"{DEMO_RUN_ID}:table:{index}",
                "database_name": item.get("database_name") or "insurance",
                "schema_name": item.get("schema_name") or "dbo",
                "table_name": table_name,
                "logical_name": str(table_name).replace("_", " ").title(),
                "score": round(0.97 - index * 0.025, 2),
                "confidence": round(0.97 - index * 0.025, 2),
                "semantic_score": round(0.97 - index * 0.025, 2),
                "confidence_score": round(0.97 - index * 0.025, 2),
                "status": "PENDING_REVIEW",
                "reason": "Required for claim, policy, premium, reserve, and coverage KPI computation.",
                "matched_kpis": [kpi["kpi_name"] for kpi in DEMO_KPIS[index % max(1, len(DEMO_KPIS)): index % max(1, len(DEMO_KPIS)) + 2]],
                "selected": True,
            }
        )
    if rows:
        return rows
    return [
        {"database_name": "insurance", "schema_name": "dbo", "table_name": "policy_transactions", "score": 0.97},
        {"database_name": "insurance", "schema_name": "dbo", "table_name": "claim_information", "score": 0.94},
        {"database_name": "insurance", "schema_name": "dbo", "table_name": "claim_payment_indemnity", "score": 0.91},
        {"database_name": "insurance", "schema_name": "dbo", "table_name": "policy_cover_level_transactions", "score": 0.89},
    ]


def demo_enriched_columns() -> List[Dict[str, Any]]:
    table_columns = {
        "policy_transactions": [
            "RERERENCE_ID", "POLICY_NUMBER", "POLICY_TRANSACTION_TYPE", "BEGIN_DATE", "END_DATE",
            "POLICY_ISSUED_DATE", "PRODUCT_NAME", "PRODUCT_GROUP_NAME", "SEGMENT_NAME",
            "BUSINESS_DIVISION_NAME", "AGENT_NAME", "AGEN_T_CATEGORY_NAME", "AGENT_SUB_CATEGORY_NAME",
            "CHANNEL_NAME", "BRANCH_OFFICE_NAME", "CUSTOMER_IDENTIFIER", "TOTAL_RISK_SUM_INSURED_AMOUNT",
            "POLICY_STATUS", "POLICY_TENURE_MONTHS", "RENEWAL_INDICATOR", "WRITTEN_PREMIUM_AMOUNT",
            "NET_PREMIUM_AMOUNT", "COMMISSION_AMOUNT", "UNDERWRITING_YEAR", "CUSTOMER_TIER",
            "PAYMENT_FREQUENCY", "POLICY_SOURCE_SYSTEM", "POLICY_EFFECTIVE_STATUS", "RISK_CATEGORY",
            "INSURED_AGE_BAND", "POLICY_POSTAL_REGION", "LOAD_TIMESTAMP", "SOURCE_BATCH_ID",
        ],
        "claim_information": [
            "RERERENCE_ID", "CLAIM_NUMBER", "CLAIM_STATUS", "CLAIM_REGISTERED_DATE", "CLAIM_CLOSED_DATE",
            "LOSS_DATE", "POLICY_NUMBER", "PRODUCT_NAME", "CLAIM_TYPE", "CLAIM_CAUSE",
            "CUSTOMER_IDENTIFIER", "CLAIM_HANDLER", "CLAIM_REGION", "CLAIM_SEVERITY", "CLAIM_CHANNEL",
            "FIRST_NOTICE_DATE", "ADJUDICATION_DATE", "SETTLEMENT_DATE", "CLAIM_AGE_DAYS",
            "FRAUD_SUSPECTED_FLAG", "LITIGATION_FLAG", "CATASTROPHE_EVENT_CODE", "LOSS_LOCATION_STATE",
            "COVERAGE_CODE", "CLAIM_SOURCE_SYSTEM", "LOAD_TIMESTAMP",
        ],
        "claim_payment_indemnity": [
            "RERERENCE_ID", "CLAIM_NUMBER", "PAYMENT_DATE", "PAID_AMOUNT", "PAYMENT_STATUS",
            "PAYMENT_TYPE", "CURRENCY_CODE", "APPROVED_BY", "PAYEE_TYPE", "RECOVERY_FLAG",
            "PAYMENT_METHOD", "PAYMENT_BATCH_ID", "RESERVE_RELEASE_AMOUNT", "TAX_WITHHELD_AMOUNT",
            "NET_PAYMENT_AMOUNT", "PAYMENT_APPROVAL_DATE", "PAYMENT_SOURCE_SYSTEM", "LOAD_TIMESTAMP",
        ],
        "claim_payment_expenses": [
            "RERERENCE_ID", "CLAIM_NUMBER", "EXPENSE_DATE", "EXPENSE_AMOUNT", "EXPENSE_TYPE",
            "VENDOR_NAME", "PAYMENT_STATUS", "CURRENCY_CODE", "APPROVED_BY", "COST_CENTER",
            "INVOICE_NUMBER", "INVOICE_DATE", "SERVICE_CATEGORY", "NET_EXPENSE_AMOUNT",
            "TAX_AMOUNT", "PAYMENT_BATCH_ID", "EXPENSE_SOURCE_SYSTEM", "LOAD_TIMESTAMP",
        ],
        "policy_cover_level_transactions": [
            "RERERENCE_ID", "POLICY_NUMBER", "COVER_NAME", "COVER_GROUP_IDENTIFIER_NAME",
            "PREMIUM_AMOUNT", "SUM_INSURED_AMOUNT", "GEOG_STATE_NAME", "COVER_START_DATE",
            "COVER_END_DATE", "DEDUCTIBLE_AMOUNT", "COVER_STATUS", "COVER_LIMIT_AMOUNT",
            "COVER_PREMIUM_TAX_AMOUNT", "RISK_CLASS_CODE", "COVER_SOURCE_SYSTEM", "LOAD_TIMESTAMP",
        ],
        "policy_cover_level_transactions_dup_del": [
            "RERERENCE_ID", "POLICY_NUMBER", "COVER_NAME", "PREMIUM_AMOUNT", "SUM_INSURED_AMOUNT",
            "DUPLICATE_GROUP_ID", "DEDUP_SEQUENCE", "COVER_STATUS", "DUPLICATE_REASON_CODE",
            "SURVIVOR_RECORD_FLAG", "DEDUP_RULE_VERSION", "LOAD_TIMESTAMP", "SOURCE_BATCH_ID",
        ],
        "expenses_outstanding_estimates": [
            "RERERENCE_ID", "CLAIM_NUMBER", "OUTSTANDING_AMOUNT", "RESERVE_DATE", "RESERVE_STATUS",
            "RESERVE_CATEGORY", "RESERVE_CHANGE_AMOUNT", "RESERVE_REASON_CODE", "ESTIMATOR_ID",
            "ESTIMATE_SOURCE_SYSTEM", "LOAD_TIMESTAMP",
        ],
        "indemnity_outstanding_estimates": [
            "RERERENCE_ID", "CLAIM_NUMBER", "RESERVE_AMOUNT", "RESERVE_DATE", "RESERVE_STATUS",
            "INDEMNITY_CATEGORY", "RESERVE_CHANGE_AMOUNT", "RESERVE_REASON_CODE", "ESTIMATOR_ID",
            "ESTIMATE_SOURCE_SYSTEM", "LOAD_TIMESTAMP",
        ],
    }
    scripts = _silver_bundle().get("scripts") or []
    counts = {str(item.get("table")): int(item.get("column_count") or 0) for item in scripts}
    rows: List[Dict[str, Any]] = []
    for table, seed_columns in table_columns.items():
        target_count = max(len(seed_columns), counts.get(table, len(seed_columns)))
        expanded_columns = list(seed_columns)
        while len(expanded_columns) < target_count:
            expanded_columns.append(f"{table.upper()}_SOURCE_ATTRIBUTE_{len(expanded_columns) + 1}")
        for column in expanded_columns[:target_count]:
            semantic_type = _semantic_type_for_column(column)
            is_measure = semantic_type == "MEASURE"
            is_pii = semantic_type == "PII"
            rows.append(
                {
                    "table_name": table,
                    "column_name": column,
                    "name": column,
                    "suggested_display_name": column.replace("_", " ").title(),
                    "semantic_type": semantic_type,
                    "business_description": _semantic_description(table, column, semantic_type),
                    "business_definition": _semantic_description(table, column, semantic_type),
                    "enrichment_source": "semantic_enrichment_llm" if len(rows) % 4 == 0 else "rules_and_catalog",
                    "is_measure": is_measure,
                    "is_dimension": semantic_type in {"DIMENSION", "ID", "DATE", "FLAG", "PII"},
                    "is_pii_candidate": is_pii,
                    "confidence": round(max(0.72, 0.98 - (len(rows) % 18) * 0.012), 2),
                    "status": "PENDING_REVIEW",
                }
            )
    return rows


def _semantic_type_for_column(column_name: str) -> str:
    name = column_name.upper()
    if "CUSTOMER" in name:
        return "PII"
    if "LOAD_TIMESTAMP" in name:
        return "AUDIT_TIMESTAMP"
    if name.endswith("_ID") or "NUMBER" in name or "RERERENCE" in name or "BATCH_ID" in name:
        return "ID"
    if "DATE" in name:
        return "DATE"
    if any(token in name for token in ("AMOUNT", "PREMIUM", "SUM_INSURED", "RESERVE", "PAID", "TENURE", "AGE_DAYS")):
        return "MEASURE"
    if any(token in name for token in ("STATUS", "FLAG", "TYPE", "INDICATOR")):
        return "FLAG"
    return "DIMENSION"


def _semantic_description(table_name: str, column_name: str, semantic_type: str) -> str:
    table_label = table_name.replace("_", " ")
    column_label = column_name.replace("_", " ").title()
    if semantic_type == "MEASURE":
        return f"{column_label} is a quantitative insurance metric from {table_label} used for KPI aggregation and Gold facts."
    if semantic_type == "ID":
        return f"{column_label} is a business or technical key from {table_label} used for joins, deduplication, and lineage."
    if semantic_type == "DATE":
        return f"{column_label} is a lifecycle date from {table_label} used for period, aging, and SLA analysis."
    if semantic_type == "PII":
        return f"{column_label} is a privacy-sensitive identifier from {table_label} requiring review before downstream exposure."
    if semantic_type == "AUDIT_TIMESTAMP":
        return f"{column_label} is an audit timestamp from {table_label} used for ingestion traceability."
    if semantic_type == "FLAG":
        return f"{column_label} is a lifecycle/status signal from {table_label} used for segmentation and filtering."
    return f"{column_label} is a descriptive insurance dimension from {table_label} used for slicing and reporting."


def demo_feed_semantic_summary() -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for column in demo_enriched_columns():
        grouped.setdefault(str(column["table_name"]), []).append(column)
    return [
        {
            "feed_id": table_name,
            "vendor": "insurance",
            "entity": table_name,
            "format": "sql_table",
            "table_name": table_name,
            "column_count": len(columns),
            "pii_count": sum(1 for column in columns if column.get("is_pii_candidate")),
            "join_key_count": sum(1 for column in columns if column.get("semantic_type") == "ID"),
            "measure_count": sum(1 for column in columns if column.get("is_measure")),
            "semantic_counts": {
                "ID": sum(1 for column in columns if column.get("semantic_type") == "ID"),
                "MEASURE": sum(1 for column in columns if column.get("semantic_type") == "MEASURE"),
                "DIMENSION": sum(1 for column in columns if column.get("semantic_type") == "DIMENSION"),
                "PII": sum(1 for column in columns if column.get("semantic_type") == "PII"),
            },
            "enriched_columns": columns,
            "table_summary": f"{table_name} semantic labels prepared for column review.",
            "sample_row_count": 12840 - index * 730,
        }
        for index, (table_name, columns) in enumerate(grouped.items())
    ]


def _semantic_summary(columns: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "tables": len({str(column.get("table_name")) for column in columns if column.get("table_name")}),
        "columns": len(columns),
        "joins": 3,
        "measures": sum(1 for column in columns if column.get("is_measure")),
        "dimensions": sum(1 for column in columns if column.get("is_dimension")),
        "pii": sum(1 for column in columns if column.get("is_pii_candidate")),
        "dates": sum(1 for column in columns if column.get("semantic_type") in {"DATE", "AUDIT_TIMESTAMP"}),
        "ids": sum(1 for column in columns if column.get("semantic_type") == "ID"),
        "flags": sum(1 for column in columns if column.get("semantic_type") == "FLAG"),
    }


def _qualified_columns(columns: List[Dict[str, Any]], predicate) -> List[str]:
    return [
        f"{column.get('table_name')}.{column.get('column_name')}"
        for column in columns
        if column.get("table_name") and column.get("column_name") and predicate(column)
    ]


def demo_run(run_id: Optional[str] = None, *, include_scripts: bool = False) -> Dict[str, Any]:
    run_id = run_id or DEMO_RUN_ID
    is_completed = run_id == DEMO_COMPLETED_RUN_ID
    scripts = demo_scripts(run_id)
    tables = demo_tables()
    enriched_columns = demo_enriched_columns()
    feed_semantic_summary = demo_feed_semantic_summary()
    stages = demo_stages()
    if is_completed:
        stages = [_stage(s["key"], s["label"], "COMPLETED", i + 1) for i, s in enumerate(stages)]
    payload: Dict[str, Any] = {
        "id": run_id,
        "run_id": run_id,
        "brd_filename": "Insurance_BRD_v3.txt" if not is_completed else "Sales_Dashboard_BRD.txt",
        "source": "database",
        "status": "SUCCESS" if is_completed else "HITL_WAIT",
        "provider": "azure_openai",
        "deployment": "gpt-4o-athena",
        "started_at": _iso(45 if is_completed else 8),
        "completed_at": _iso(30) if is_completed else None,
        "cache_hit": "L1_EXACT" if is_completed else "L2_FUZZY",
        "cache_score": 1.0 if is_completed else 0.947,
        "extraction_path": "CACHED_L1" if is_completed else "CACHED_L2",
        "total_tokens": 31200 if is_completed else 48320,
        "total_cost": 0.78 if is_completed else 1.24,
        "stages": stages,
        "pipeline_steps": stages,
        "requirements": demo_requirements(),
        "kpis": deepcopy(DEMO_KPIS),
        "hitl_decisions": [],
        "nominated_tables": tables,
        "certified_tables": [],
        "enriched_metadata": {
            "domain": "P&C insurance",
            "tables": len(tables),
            "columns": len(enriched_columns),
            "summary": "Policy, claim, premium, reserve, coverage, and payment columns are prepared for Bronze/Silver/Gold generation.",
        },
        "enriched_columns": enriched_columns,
        "enriched_joins": [
            {"left": "policy_transactions.RERERENCE_ID", "right": "claim_information.RERERENCE_ID", "join_type": "LEFT"},
            {"left": "policy_transactions.RERERENCE_ID", "right": "claim_payment_indemnity.RERERENCE_ID", "join_type": "LEFT"},
            {"left": "policy_transactions.RERERENCE_ID", "right": "policy_cover_level_transactions.RERERENCE_ID", "join_type": "LEFT"},
        ],
        "semantic_counts": {**_semantic_summary(enriched_columns), "kpis": len(DEMO_KPIS)},
        "pii_columns": _qualified_columns(enriched_columns, lambda column: column.get("is_pii_candidate")),
        "join_key_columns": _qualified_columns(enriched_columns, lambda column: column.get("semantic_type") == "ID"),
        "measure_columns": _qualified_columns(enriched_columns, lambda column: column.get("is_measure")),
        "feed_semantic_summary": feed_semantic_summary,
        "next_gate": None if is_completed else 1,
        "resume_message": "Run completed." if is_completed else "KPI Review is ready. Validate the extracted KPIs before the pipeline continues.",
        "stage_confirmation": None,
        "failed_stage_key": None,
        "failed_stage_label": None,
        "error": None,
        "updated_at": _iso(1),
        "databricks_run_id": run_id,
        "script_counts": {
            "bronze": len((scripts.get("bronze") or {}).get("scripts") or []),
            "silver": len((scripts.get("silver") or {}).get("scripts") or []),
            "gold": len((scripts.get("gold") or {}).get("scripts") or []),
        } if is_completed else {"bronze": 0, "silver": 0, "gold": 0},
        "gold_generation_completed": is_completed,
        "gold_generation_status": "COMPLETED" if is_completed else "PENDING",
    }
    if is_completed:
        payload["bronze_review_artifact"] = demo_bronze_review(run_id)["bronze_review_artifact"]
        payload["silver_review_artifact"] = demo_silver_review(run_id)["silver_review_artifact"]
    if include_scripts and is_completed:
        payload.update(scripts)
    return payload


def demo_runs() -> List[Dict[str, Any]]:
    active = demo_run(DEMO_RUN_ID)
    completed = demo_run(DEMO_COMPLETED_RUN_ID)
    return [active, completed]


def demo_status(run_id: str) -> Dict[str, Any]:
    run = demo_run(run_id)
    terminal = run["status"] in {"SUCCESS", "FAILED", "ABORTED"}
    return {
        "run_id": run_id,
        "status": run["status"],
        "state": {"life_cycle_state": "TERMINATED" if terminal else "RUNNING", "result_state": run["status"]},
        "run": run,
    }


def demo_kpi_reviews(run_id: str) -> Dict[str, Any]:
    rows = [{**kpi, "run_id": run_id, "id": kpi["id"].replace(DEMO_RUN_ID, run_id), "queue_id": kpi["queue_id"].replace(DEMO_RUN_ID, run_id), "item_id": kpi["item_id"].replace(DEMO_RUN_ID, run_id)} for kpi in DEMO_KPIS]
    return {"runId": run_id, "run_id": run_id, "source": "database", "kpis": rows}


def demo_table_reviews(run_id: str) -> Dict[str, Any]:
    return {
        "run_id": run_id,
        "source": "database",
        "next_gate": 2,
        "resume_message": "Table review is ready.",
        "nominated_tables": demo_run(run_id)["nominated_tables"],
        "certified_tables": [],
        "candidate_feed": None,
        "candidate_feeds": [],
    }


def demo_enrichment_reviews(run_id: str) -> Dict[str, Any]:
    enriched_columns = demo_enriched_columns()
    feed_semantic_summary = demo_feed_semantic_summary()
    return {
        "run_id": run_id,
        "next_gate": 3,
        "resume_message": "Column enrichment review is ready.",
        "queue_id": f"{run_id}-semantic-enrichment",
        "entity": "insurance_column_enrichment",
        "table_name": "Insurance Column Review",
        "table_summary": "Semantic enrichment prepared claim, policy, premium, reserve, payment, and coverage columns for review.",
        "enriched_metadata": {
            "policy_transactions": "Policy transaction grain with product, channel, dates, and risk values.",
            "claim_information": "Claim lifecycle and claim identifier attributes.",
            "claim_payment_indemnity": "Paid indemnity measures joined by reference id.",
            "policy_cover_level_transactions": "Coverage and premium measures by cover and geography.",
        },
        "enriched_columns": enriched_columns,
        "enriched_joins": [
            {"left": "policy_transactions.RERERENCE_ID", "right": "claim_information.RERERENCE_ID", "join_type": "LEFT", "confidence": 0.92},
            {"left": "policy_transactions.RERERENCE_ID", "right": "claim_payment_indemnity.RERERENCE_ID", "join_type": "LEFT", "confidence": 0.89},
            {"left": "policy_transactions.RERERENCE_ID", "right": "policy_cover_level_transactions.RERERENCE_ID", "join_type": "LEFT", "confidence": 0.91},
        ],
        "semantic_counts": _semantic_summary(enriched_columns),
        "pii_columns": _qualified_columns(enriched_columns, lambda column: column.get("is_pii_candidate")),
        "join_key_columns": _qualified_columns(enriched_columns, lambda column: column.get("semantic_type") == "ID"),
        "measure_columns": _qualified_columns(enriched_columns, lambda column: column.get("is_measure")),
        "feed_semantic_summary": feed_semantic_summary,
        "gate3_approved": False,
    }


def demo_bronze_review(run_id: str) -> Dict[str, Any]:
    bundle = _with_run_id(_bronze_bundle(), run_id)
    feeds = []
    for index, item in enumerate(bundle.get("scripts") or []):
        script_body = _generated_script_body("bronze", item.get("script_path"))
        table_name = item.get("table") or f"bronze_table_{index + 1}"
        feeds.append(
            {
                **item,
                "entity": table_name,
                "feed_name": table_name,
                "table_name": table_name,
                "target_table": f"bronze.bronze_{table_name}",
                "generated_bronze_script": script_body,
                "script_body": script_body,
                "status": item.get("status") or "APPROVED",
                "queued_at": _iso(8 - min(index, 5)),
            }
        )
    return {
        "run_id": run_id,
        "next_gate": 4,
        "resume_message": "Bronze plan is ready.",
        "bronze_review_artifact": {
            "source_database": bundle.get("source_database") or "insurance",
            "generated_at": bundle.get("generated_at") or _iso(4),
            "feeds": feeds,
        },
    }


def demo_silver_review(run_id: str) -> Dict[str, Any]:
    bundle = _with_run_id(_silver_bundle(), run_id)
    items = [
        {
            "script_name": "merge_key_policy_reference",
            "entity": "policy_transactions",
            "table_name": "policy_transactions",
            "source_table": "bronze.bronze_policy_transactions",
            "target_table": "silver.silver_policy_transactions",
            "merge_strategy": "deterministic_business_key",
            "merge_key_source": "RERERENCE_ID + POLICY_NUMBER",
            "candidate_keys": ["RERERENCE_ID", "POLICY_NUMBER", "POLICY_TRANSACTION_TYPE"],
            "selected_merge_key": ["RERERENCE_ID", "POLICY_NUMBER"],
            "confidence_score": 0.96,
            "status": "PENDING_REVIEW",
            "queued_at": _iso(5),
            "script_body": "\n".join(
                [
                    "# Silver merge-key review",
                    'source_table = "bronze.bronze_policy_transactions"',
                    'target_table = "silver.silver_policy_transactions"',
                    'selected_merge_key = ["RERERENCE_ID", "POLICY_NUMBER"]',
                    'merge_strategy = "dedupe on selected business key, keep latest policy issue timestamp"',
                ]
            ),
        },
        {
            "script_name": "merge_key_claim_reference",
            "entity": "claim_information",
            "table_name": "claim_information",
            "source_table": "bronze.bronze_claim_information",
            "target_table": "silver.silver_claim_information",
            "merge_strategy": "claim_reference_key",
            "merge_key_source": "RERERENCE_ID + CLAIM_NUMBER",
            "candidate_keys": ["RERERENCE_ID", "CLAIM_NUMBER"],
            "selected_merge_key": ["RERERENCE_ID", "CLAIM_NUMBER"],
            "confidence_score": 0.94,
            "status": "PENDING_REVIEW",
            "queued_at": _iso(4),
            "script_body": "\n".join(
                [
                    "# Silver merge-key review",
                    'source_table = "bronze.bronze_claim_information"',
                    'target_table = "silver.silver_claim_information"',
                    'selected_merge_key = ["RERERENCE_ID", "CLAIM_NUMBER"]',
                    'merge_strategy = "dedupe claim rows before downstream claim-payment joins"',
                ]
            ),
        },
    ]
    for index, item in enumerate(bundle.get("scripts") or []):
        script_body = _generated_script_body("silver", item.get("script_path"))
        table_name = item.get("table") or f"silver_table_{index + 1}"
        items.append(
            {
                **item,
                "entity": table_name,
                "table_name": table_name,
                "script_name": f"silver_{table_name}",
                "generated_silver_script": script_body,
                "script_body": script_body,
                "status": item.get("status") or "APPROVED",
                "queued_at": _iso(3 - min(index, 2)),
            }
        )
    return {
        "run_id": run_id,
        "next_gate": 5,
        "resume_message": "Silver plan is ready.",
        "silver_review_artifact": {
            "generated_at": bundle.get("generated_at") or _iso(3),
            "items": items,
        },
    }


def demo_scripts(run_id: str) -> Dict[str, Any]:
    bronze = _with_run_id(_bronze_bundle(), run_id) or {"generated_at": _iso(4), "scripts": []}
    silver = _with_run_id(_silver_bundle(), run_id) or {"generated_at": _iso(3), "scripts": []}
    gold = _with_run_id(_gold_bundle(), run_id) or {"generated_at": _iso(2), "scripts": []}

    for item in bronze.get("scripts") or []:
        body = _generated_script_body("bronze", item.get("script_path"))
        if body:
            item["script_body"] = body
            item["generated_bronze_script"] = body

    for item in silver.get("scripts") or []:
        body = _generated_script_body("silver", item.get("script_path"))
        if body:
            item["script_body"] = body
            item["generated_silver_script"] = body

    for item in gold.get("scripts") or []:
        body = _generated_script_body("gold", item.get("script_path"))
        dimension_body = _generated_script_body("gold", item.get("dimension_script_path"))
        if body:
            item["script_body"] = body
        if dimension_body:
            item["dimension_body"] = dimension_body

    return {"bronze": bronze, "silver": silver, "gold": gold}


def demo_lineage(run_id: str) -> Dict[str, Any]:
    tables = [row["table_name"] for row in demo_tables()]
    gold_scripts = (demo_scripts(run_id).get("gold") or {}).get("scripts") or []
    semantic_by_table = {feed["entity"]: feed for feed in demo_feed_semantic_summary()}
    table_roles = {
        "policy_transactions": "Policy transaction fact source",
        "claim_information": "Claim lifecycle source",
        "claim_payment_indemnity": "Indemnity payment source",
        "claim_payment_expenses": "Expense payment source",
        "policy_cover_level_transactions": "Coverage and premium source",
        "policy_cover_level_transactions_dup_del": "Coverage duplicate-resolution source",
        "expenses_outstanding_estimates": "Expense reserve source",
        "indemnity_outstanding_estimates": "Indemnity reserve source",
    }
    nodes: List[Dict[str, Any]] = []
    edges: List[Dict[str, Any]] = []

    for table in tables:
        semantic = semantic_by_table.get(table) or {}
        column_count = int(semantic.get("column_count") or 0)
        sample_row_count = int(semantic.get("sample_row_count") or 0)
        source_id = f"source:{table}"
        bronze_id = f"bronze:{table}"
        silver_id = f"silver:{table}"
        nodes.extend(
            [
                {
                    "id": source_id,
                    "name": f"insurance.dbo.{table}",
                    "label": f"insurance.dbo.{table}",
                    "layer": "source",
                    "table": table,
                    "database": "insurance",
                    "schema": "dbo",
                    "role": table_roles.get(table, "Insurance source table"),
                    "column_count": column_count,
                    "sample_row_count": sample_row_count,
                    "semantic_counts": semantic.get("semantic_counts") or {},
                },
                {
                    "id": bronze_id,
                    "name": f"main.bronze.bronze_{table}",
                    "label": f"main.bronze.bronze_{table}",
                    "layer": "bronze",
                    "table": table,
                    "database": "main",
                    "schema": "bronze",
                    "role": "Raw landing table with audit metadata and source fidelity",
                    "column_count": column_count + 4,
                    "source_table": f"insurance.dbo.{table}",
                },
                {
                    "id": silver_id,
                    "name": f"main.silver.silver_{table}",
                    "label": f"main.silver.silver_{table}",
                    "layer": "silver",
                    "table": table,
                    "database": "main",
                    "schema": "silver",
                    "role": "Curated table with merge key, casts, dedupe, and semantic labels",
                    "column_count": column_count + 3,
                    "source_table": f"main.bronze.bronze_{table}",
                    "merge_key": _lineage_merge_key(table),
                },
            ]
        )
        edges.extend(
            [
                {
                    "id": f"pipeline:source:{table}:bronze",
                    "source": source_id,
                    "target": bronze_id,
                    "type": "pipeline",
                    "operation": "bronze_ingest",
                    "description": "Source table lands into Bronze with no business transformation.",
                },
                {
                    "id": f"pipeline:bronze:{table}:silver",
                    "source": bronze_id,
                    "target": silver_id,
                    "type": "pipeline",
                    "operation": "silver_transform",
                    "description": "Bronze table is cast, deduplicated, and assigned merge keys.",
                },
            ]
        )

    for index, script in enumerate(gold_scripts):
        kpi_name = script.get("kpi_name") or f"Gold KPI {index + 1}"
        source_table = str(script.get("source_table") or "").replace("silver.silver_", "")
        gold_id = f"gold:{str(kpi_name).lower().replace(' ', '_')}"
        nodes.append(
            {
                "id": gold_id,
                "name": script.get("target_table") or f"main.gold.{kpi_name}",
                "label": script.get("target_table") or kpi_name,
                "layer": "gold",
                "kpi_name": kpi_name,
                "role": "Gold KPI fact output",
                "source_table": script.get("source_table"),
                "target_table": script.get("target_table"),
                "time_grain": script.get("time_grain") or "month",
                "dimension_count": script.get("dimension_count") or 0,
                "join_count": script.get("join_count") or 0,
            }
        )
        if source_table:
            edges.append(
                {
                    "id": f"pipeline:silver:{source_table}:gold:{index}",
                    "source": f"silver:{source_table}",
                    "target": gold_id,
                    "type": "pipeline",
                    "operation": "gold_aggregation",
                    "description": f"Silver table feeds {kpi_name} Gold KPI generation.",
                }
            )

    relationship_edges = [
        ("policy_transactions", "claim_information", "RERERENCE_ID", "RERERENCE_ID", "fk_policy_claim_reference", "fk", 0.96, "Policy-to-claim lifecycle linkage"),
        ("policy_transactions", "claim_payment_indemnity", "RERERENCE_ID", "RERERENCE_ID", "fk_policy_indemnity_payment", "fk", 0.94, "Policy-to-indemnity payment linkage"),
        ("policy_transactions", "claim_payment_expenses", "RERERENCE_ID", "RERERENCE_ID", "fk_policy_expense_payment", "fk", 0.93, "Policy-to-expense payment linkage"),
        ("policy_transactions", "policy_cover_level_transactions", "RERERENCE_ID", "RERERENCE_ID", "heuristic_policy_cover_reference", "heuristic", 0.88, "Coverage-level premium rollup candidate"),
        ("claim_information", "expenses_outstanding_estimates", "RERERENCE_ID", "RERERENCE_ID", "heuristic_claim_expense_reserve", "heuristic", 0.84, "Claim-to-expense reserve candidate"),
        ("claim_information", "indemnity_outstanding_estimates", "RERERENCE_ID", "RERERENCE_ID", "heuristic_claim_indemnity_reserve", "heuristic", 0.85, "Claim-to-indemnity reserve candidate"),
    ]
    for source_table, target_table, source_column, target_column, name, edge_type, confidence, description in relationship_edges:
        edges.append(
            {
                "id": f"{edge_type}:{source_table}:{target_table}",
                "source": f"source:{source_table}",
                "target": f"source:{target_table}",
                "type": edge_type,
                "source_column": source_column,
                "target_column": target_column,
                "constraint_name": name,
                "confidence": confidence,
                "description": description,
            }
        )

    return {
        "run_id": run_id,
        "nodes": nodes,
        "edges": edges,
        "summary": {
            "source_count": len(tables),
            "bronze_count": len(tables),
            "silver_count": len(tables),
            "gold_count": len(gold_scripts),
            "fk_edge_count": 3,
            "heuristic_edge_count": 3,
            "kpi_count": len(DEMO_KPIS),
        },
    }


def _lineage_merge_key(table_name: str) -> List[str]:
    keys = {
        "policy_transactions": ["RERERENCE_ID", "POLICY_NUMBER"],
        "claim_information": ["RERERENCE_ID", "CLAIM_NUMBER"],
        "claim_payment_indemnity": ["RERERENCE_ID", "CLAIM_NUMBER", "PAYMENT_DATE"],
        "claim_payment_expenses": ["RERERENCE_ID", "CLAIM_NUMBER", "EXPENSE_DATE"],
        "policy_cover_level_transactions": ["RERERENCE_ID", "POLICY_NUMBER", "COVER_NAME"],
        "policy_cover_level_transactions_dup_del": ["RERERENCE_ID", "POLICY_NUMBER", "COVER_NAME", "DEDUP_SEQUENCE"],
        "expenses_outstanding_estimates": ["RERERENCE_ID", "CLAIM_NUMBER", "RESERVE_DATE"],
        "indemnity_outstanding_estimates": ["RERERENCE_ID", "CLAIM_NUMBER", "RESERVE_DATE"],
    }
    return keys.get(table_name, ["RERERENCE_ID"])


def demo_logs(run_id: str, limit: int = 300) -> List[Dict[str, Any]]:
    messages = [
        ("ingestion", "INFO", "Received P&C insurance BRD and assigned run context"),
        ("ingestion", "INFO", "Normalized policy, claim, premium, reserve, coverage, and expense requirements"),
        ("memory", "INFO", "Checked prior run memory for matching insurance analytics patterns"),
        ("memory", "INFO", "Reused approved P&C insurance context for table and KPI grounding"),
        ("requirements", "INFO", "Extracted reporting objective, audience, constraints, and KPI families"),
        ("requirements", "INFO", "Validated daily operations and monthly regulatory reporting cadence"),
        ("kpis", "INFO", f"Generated {len(DEMO_KPIS)} grounded insurance KPIs"),
        ("kpis", "INFO", "Mapped KPIs to claims, policy, premium, coverage, expenses, and reserve domains"),
        ("gate1", "INFO", "KPI Review is ready for approval"),
    ]
    if run_id == DEMO_COMPLETED_RUN_ID:
        messages.extend(
            [
                ("nomination", "INFO", f"Certified {len(demo_tables())} source table nominations from insurance.dbo"),
                ("discovery", "INFO", "Discovered source columns for policy, claim, payment, reserve, and coverage tables"),
                ("profiling", "INFO", "Profiled keys, dates, measures, nullability, and cardinality"),
                ("enrichment", "INFO", f"Approved {len(demo_enriched_columns())} semantic column classifications"),
                ("bronze", "INFO", f"Generated {len((_bronze_bundle().get('scripts') or []))} Bronze ingestion artifacts"),
                ("silver", "INFO", f"Generated {len((_silver_bundle().get('scripts') or []))} Silver transformation artifacts"),
                ("gold", "INFO", f"Generated {len((_gold_bundle().get('scripts') or []))} Gold KPI generation artifacts"),
            ]
        )
    rows = [
        {"timestamp": _iso(max(1, 34 - i * 2)), "stage": stage, "level": level, "message": message, "run_id": run_id}
        for i, (stage, level, message) in enumerate(messages)
    ]
    return rows[-limit:]


def demo_action(run_id: str, status: str = "SUBMITTED", **extra: Any) -> Dict[str, Any]:
    return {"run_id": run_id, "status": status, **extra}


def new_demo_run_id() -> str:
    return f"athena-{uuid.uuid4()}"
