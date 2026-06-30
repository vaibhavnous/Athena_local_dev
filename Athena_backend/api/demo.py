from __future__ import annotations

import os
import time
import uuid
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional


DEMO_RUN_ID = "athena-insurance-run"


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
        "state": "WAITING" if status == "HITL_WAIT" else status,
        "tokens": 2400 + index * 375 if completed else 0,
        "cost": round((2400 + index * 375) / 100000, 4) if completed else 0,
        "attempts": 1 if completed else 0,
        "started_at": _iso(18 - index) if completed else None,
        "completed_at": _iso(17 - index) if completed and status != "HITL_WAIT" else None,
        "error": None,
        "prompt_metadata": {"model": "gpt-4.1", "temperature": 0.0} if completed else None,
    }


DEMO_KPIS: List[Dict[str, Any]] = [
    {
        "id": f"{DEMO_RUN_ID}:1:0",
        "queue_id": f"{DEMO_RUN_ID}:1:0",
        "item_id": f"{DEMO_RUN_ID}:1:0",
        "item_type": "KPI",
        "name": "Claims Cycle Time",
        "kpi_name": "Claims Cycle Time",
        "definition": "Average hours from claim intake to first adjudication decision.",
        "kpi_description": "Average hours from claim intake to first adjudication decision.",
        "category": "Claims",
        "domain": "Claims Processing",
        "confidence": 0.94,
        "ai_confidence_score": 0.94,
        "status": "PENDING_REVIEW",
        "gate_status": "PENDING",
        "decision": None,
        "grounded": True,
        "explicit": True,
        "source_requirement_ref": "Claims processing efficiency",
        "run_id": DEMO_RUN_ID,
        "source": "database",
    },
    {
        "id": f"{DEMO_RUN_ID}:1:1",
        "queue_id": f"{DEMO_RUN_ID}:1:1",
        "item_id": f"{DEMO_RUN_ID}:1:1",
        "item_type": "KPI",
        "name": "Policy Renewal Rate",
        "kpi_name": "Policy Renewal Rate",
        "definition": "Percentage of eligible policies renewed within the renewal window.",
        "kpi_description": "Percentage of eligible policies renewed within the renewal window.",
        "category": "Policy",
        "domain": "Policy Management",
        "confidence": 0.91,
        "ai_confidence_score": 0.91,
        "status": "PENDING_REVIEW",
        "gate_status": "PENDING",
        "decision": None,
        "grounded": True,
        "explicit": False,
        "source_requirement_ref": "Retention analytics",
        "run_id": DEMO_RUN_ID,
        "source": "database",
    },
    {
        "id": f"{DEMO_RUN_ID}:1:2",
        "queue_id": f"{DEMO_RUN_ID}:1:2",
        "item_id": f"{DEMO_RUN_ID}:1:2",
        "item_type": "KPI",
        "name": "Loss Ratio",
        "kpi_name": "Loss Ratio",
        "definition": "Claims paid divided by earned premium for the selected period.",
        "kpi_description": "Claims paid divided by earned premium for the selected period.",
        "category": "Financial",
        "domain": "Actuarial",
        "confidence": 0.89,
        "ai_confidence_score": 0.89,
        "status": "PENDING_REVIEW",
        "gate_status": "PENDING",
        "decision": None,
        "grounded": True,
        "explicit": True,
        "source_requirement_ref": "Regulatory and actuarial reporting",
        "run_id": DEMO_RUN_ID,
        "source": "database",
    },
]


def demo_requirements() -> Dict[str, Any]:
    return {
        "objective": "Build an insurance analytics foundation for claims, policy, premium, and renewal reporting.",
        "business_objective": "Build an insurance analytics foundation for claims, policy, premium, and renewal reporting.",
        "data_domains": ["Claims Processing", "Policy Management", "Premium Billing", "Customer Retention"],
        "reporting_frequency": "Daily operational dashboards with monthly regulatory summaries",
        "target_audience": "Claims operations, underwriting, actuarial, and executive teams",
        "constraints": ["No raw-data transformation in bronze", "Auditable KPI lineage", "Fast lightweight runtime"],
        "schema_valid": True,
        "prompt_version": "DEMO_v1",
    }


def demo_stages() -> List[Dict[str, Any]]:
    return [
        _stage("ingestion", "BRD Ingest", "COMPLETED", 1),
        _stage("requirements", "Requirement Extraction", "COMPLETED", 2),
        _stage("kpis", "KPI Extraction", "COMPLETED", 3),
        _stage("gate1", "KPI Review", "HITL_WAIT", 4),
        _stage("nomination", "Table Nomination", "PENDING", 5),
        _stage("enrichment", "Semantic Enrichment", "PENDING", 6),
        _stage("bronze", "Bronze Generation", "PENDING", 7),
        _stage("silver", "Silver Generation", "PENDING", 8),
        _stage("gold", "Gold Generation", "PENDING", 9),
    ]


def demo_run(run_id: Optional[str] = None, *, include_scripts: bool = False) -> Dict[str, Any]:
    run_id = run_id or DEMO_RUN_ID
    payload: Dict[str, Any] = {
        "id": run_id,
        "run_id": run_id,
        "brd_filename": "Insurance_Analytics_BRD.txt",
        "source": "database",
        "status": "HITL_WAIT",
        "provider": "azure_openai",
        "deployment": "gpt-4.1",
        "started_at": _iso(18),
        "completed_at": None,
        "cache_hit": "L1_EXACT",
        "cache_score": 1.0,
        "extraction_path": "ATHENA_GRAPH",
        "total_tokens": 12875,
        "total_cost": 0.19,
        "stages": demo_stages(),
        "requirements": demo_requirements(),
        "kpis": deepcopy(DEMO_KPIS),
        "hitl_decisions": [],
        "nominated_tables": [
            {"database_name": "insurance", "schema_name": "dbo", "table_name": "claims", "score": 0.94},
            {"database_name": "insurance", "schema_name": "dbo", "table_name": "policies", "score": 0.91},
        ],
        "certified_tables": [],
        "enriched_metadata": {},
        "enriched_columns": [],
        "semantic_counts": {},
        "next_gate": 1,
        "resume_message": "KPI Review is ready. Approve KPIs to continue the flow.",
        "stage_confirmation": None,
        "failed_stage_key": None,
        "failed_stage_label": None,
        "error": None,
        "updated_at": _iso(1),
        "databricks_run_id": run_id,
        "script_counts": {"bronze": 2, "silver": 2, "gold": 1},
    }
    if include_scripts:
        payload.update(demo_scripts(run_id))
    return payload


def demo_runs() -> List[Dict[str, Any]]:
    active = demo_run(DEMO_RUN_ID)
    completed = {**demo_run("athena-completed-run"), "status": "SUCCESS", "next_gate": None, "resume_message": "Run completed."}
    completed["stages"] = [_stage(s["key"], s["label"], "COMPLETED", i + 1) for i, s in enumerate(demo_stages())]
    completed["completed_at"] = _iso(5)
    return [active, completed]


def demo_status(run_id: str) -> Dict[str, Any]:
    run = demo_run(run_id)
    return {
        "run_id": run_id,
        "status": run["status"],
        "state": {"life_cycle_state": "RUNNING", "result_state": run["status"]},
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
    return {
        "run_id": run_id,
        "next_gate": 3,
        "resume_message": "Enrichment review is ready.",
        "enriched_metadata": {"claims": "Claim header and lifecycle facts", "policies": "Policy coverage and renewal attributes"},
        "enriched_columns": [{"table_name": "claims", "column_name": "claim_status", "semantic_type": "status"}],
        "enriched_joins": [{"left": "claims.policy_id", "right": "policies.policy_id"}],
        "semantic_counts": {"tables": 2, "columns": 12, "joins": 1},
        "pii_columns": ["policies.customer_id"],
        "join_key_columns": ["policy_id"],
        "measure_columns": ["claim_amount", "earned_premium"],
        "feed_semantic_summary": [],
        "gate3_approved": False,
    }


def demo_bronze_review(run_id: str) -> Dict[str, Any]:
    return {
        "run_id": run_id,
        "next_gate": 4,
        "resume_message": "Bronze plan is ready.",
        "bronze_review_artifact": {
            "feeds": [
                {"name": "claims", "target_table": "bronze.claims_raw", "mode": "append"},
                {"name": "policies", "target_table": "bronze.policies_raw", "mode": "append"},
            ]
        },
    }


def demo_silver_review(run_id: str) -> Dict[str, Any]:
    return {
        "run_id": run_id,
        "next_gate": 5,
        "resume_message": "Silver plan is ready.",
        "silver_review_artifact": {
            "items": [
                {"name": "claims_curated", "source": "bronze.claims_raw", "checks": ["dedupe", "status_normalization"]},
                {"name": "policy_claim_summary", "source": "bronze.policies_raw", "checks": ["policy_join"]},
            ]
        },
    }


def demo_scripts(run_id: str) -> Dict[str, Any]:
    return {
        "bronze": {"generated_at": _iso(4), "scripts": ["CREATE TABLE bronze.claims_raw (...);", "CREATE TABLE bronze.policies_raw (...);"]},
        "silver": {"generated_at": _iso(3), "scripts": ["CREATE TABLE silver.claims_curated AS SELECT ...;", "CREATE TABLE silver.policy_claim_summary AS SELECT ...;"]},
        "gold": {"generated_at": _iso(2), "scripts": ["CREATE TABLE gold.claims_kpi_dashboard AS SELECT ...;"]},
    }


def demo_lineage(run_id: str) -> Dict[str, Any]:
    return {
        "run_id": run_id,
        "nodes": [
            {"id": "brd", "label": "BRD"},
            {"id": "bronze", "label": "Bronze"},
            {"id": "silver", "label": "Silver"},
            {"id": "gold", "label": "Gold KPIs"},
        ],
        "edges": [
            {"source": "brd", "target": "bronze"},
            {"source": "bronze", "target": "silver"},
            {"source": "silver", "target": "gold"},
        ],
    }


def demo_logs(run_id: str, limit: int = 300) -> List[Dict[str, Any]]:
    messages = [
        ("ingestion", "BRD ingested"),
        ("requirements", "Requirements extracted"),
        ("kpis", "KPIs generated"),
        ("gate1", "Waiting for KPI review"),
    ]
    rows = [
        {"timestamp": _iso(12 - i * 2), "stage": stage, "level": "INFO", "message": message, "run_id": run_id}
        for i, (stage, message) in enumerate(messages)
    ]
    return rows[-limit:]


def demo_action(run_id: str, status: str = "SUBMITTED", **extra: Any) -> Dict[str, Any]:
    return {"run_id": run_id, "status": status, **extra}


def new_demo_run_id() -> str:
    return f"athena-{uuid.uuid4()}"
