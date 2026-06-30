from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Optional


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def json_loads(value: Any) -> Any:
    if not value:
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return value


def gate_label(gate: int, *, source: str = "database") -> str:
    if gate == 1:
        return "KPI Review"
    if gate == 2:
        return "Feed Review" if str(source or "").lower() in {"sftp", "adls_gen2"} else "Table Review"
    if gate == 3:
        return "Semantic Review"
    if gate == 4:
        return "Bronze Review"
    if gate == 5:
        return "Silver Review"
    return f"Gate {gate}"


def is_file_source(source: Optional[str]) -> bool:
    return str(source or "").lower() in {"sftp", "adls_gen2"}


def normalize_file_entity(source: Optional[str], sftp_entity: Optional[str]) -> str:
    source_value = str(source or "").lower()
    entity = str(sftp_entity or "").lower().strip()
    if source_value == "adls_gen2":
        return "auto"
    if source_value == "sftp":
        return entity if entity in {"transactions", "employee", "both"} else "transactions"
    return entity or "transactions"


def iso_or_none(value: Any) -> Optional[str]:
    if not value:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def parse_iso(value: Any) -> Optional[datetime]:
    text = iso_or_none(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(str(text).replace("Z", "+00:00"))
    except Exception:
        return None


def stage_key(value: Any) -> Optional[str]:
    text = str(value or "").lower().replace("_", " ")
    if not text:
        return None
    if "feed discovery" in text or "candidate feed" in text:
        return "discovery"
    if "source ingestion" in text or "sftp source" in text:
        return "ingestion"
    if "ingestion" in text:
        return "ingestion"
    if "memory" in text:
        return "memory"
    if "domain knowledge" in text or "domain kb" in text:
        return "domain_knowledge"
    if "requirement" in text or "req extract" in text:
        return "requirements"
    if "gate1" in text or "gate 1" in text or text == "hitl certification":
        return "gate1"
    if "kpi" in text and "hitl" not in text:
        return "kpis"
    if "nomination" in text or "table nomination" in text:
        return "nomination"
    if "gate2" in text or "gate 2" in text or "hitl table" in text:
        return "gate2"
    if "schema snapshot" in text or "sftp metadata discovery" in text:
        return "schema"
    if "metadata discovery" in text:
        return "discovery"
    if "column profiling" in text:
        return "profiling"
    if "semantic enrichment" in text:
        return "enrichment"
    if "gate3" in text or "gate 3" in text or "semantic review" in text or "enrichment certification" in text:
        return "gate3"
    if "pre-bronze" in text or "bronze readiness" in text:
        return "pre_bronze"
    if "gate4" in text or "gate 4" in text or "bronze review" in text:
        return "gate4"
    if "sftp pull" in text:
        return "pull"
    if "bronze validation" in text:
        return "bronze_validation"
    if "bronze" in text:
        return "bronze"
    if "gate5" in text or "gate 5" in text or "silver review" in text:
        return "gate5"
    if "dq validation" in text:
        return "dq_validation"
    if "silver" in text:
        return "silver"
    if "gold" in text:
        return "gold"
    return None


def stage_label_from_key(key: Optional[str], source: Optional[str] = None) -> Optional[str]:
    if not key:
        return None
    labels = {
        "ingestion": "BRD Ingest" if not is_file_source(source) else "Ingestion",
        "memory": "Memory Check",
        "domain_knowledge": "Domain Knowledge Check",
        "domain_kb": "Domain Knowledge Check",
        "requirements": "Requirement Extraction",
        "kpis": "KPI Extraction",
        "gate1": gate_label(1, source=str(source or "database")),
        "nomination": "Table Nomination",
        "gate2": gate_label(2, source=str(source or "database")),
        "discovery": "Metadata Discovery",
        "schema": "Schema Snapshot",
        "profiling": "Column Profiling",
        "enrichment": "Semantic Enrichment",
        "gate3": gate_label(3, source=str(source or "database")),
        "pre_bronze": "Pre-Bronze Readiness",
        "bronze": "Bronze Generation" if not is_file_source(source) else "Bronze Code Generation",
        "gate4": gate_label(4, source=str(source or "database")),
        "pull": "Source Handoff" if str(source or "").lower() == "adls_gen2" else "SFTP Pull",
        "bronze_validation": "Bronze Validation",
        "silver": "Silver Generation" if not is_file_source(source) else "Silver Code Generation",
        "gate5": gate_label(5, source=str(source or "database")),
        "dq_validation": "DQ Validation",
        "gold": "Gold Generation" if not is_file_source(source) else "Gold Code Generation",
    }
    return labels.get(str(key), str(key))
