"""
NB09 — Semantic Enrichment Node

Responsibilities:
- Column semantic classification (rule-first, LLM optional)
- Explicit join metadata capture (declarative, not executed)
- Explicit aggregation policy capture (rules only)
"""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, Set, Tuple
from datetime import datetime, timezone

from langgraph.graph import StateGraph
from langgraph.checkpoint.memory import MemorySaver

from state import Stage01State
from utilis.logger import logger
from utilis.db import ai_store_db_writer
from utilis.domain_kb import (
    KB_CONTENT_MEASURE,
    KB_CONTENT_PII,
    KB_CONTENT_TABLE,
    get_domain_kb_config,
    load_domain_kb,
)


# ------------------------------------------------------------------------------------
# Semantic Types
# ------------------------------------------------------------------------------------

SemanticType = Literal[
    "MEASURE",
    "DIMENSION",
    "DATE",
    "ID",
    "PII",
    "FLAG",
    "HIGH_CARD_TEXT",
    "AUDIT_TIMESTAMP",
    "SURROGATE_KEY",
    "UNKNOWN",
]

AggType = Literal["SUM", "AVG", "COUNT", "MIN", "MAX", "NONE"]


# ------------------------------------------------------------------------------------
# RULE-BASED SEMANTIC CLASSIFICATION
# ------------------------------------------------------------------------------------

def rule_based_semantic_classification(column: Dict[str, Any]) -> Dict[str, Any]:
    name = str(column.get("column_name", "")).lower()
    data_type = str(column.get("data_type", "")).lower()
    cardinality = column.get("cardinality")
    is_primary_key = bool(column.get("is_primary_key"))
    is_foreign_key = bool(column.get("is_foreign_key"))

    semantic: SemanticType = "UNKNOWN"
    suggested_agg: AggType = "NONE"

    if is_primary_key:
        semantic = "SURROGATE_KEY" if name == "id" else "ID"
    elif is_foreign_key or name.endswith("_id") or name == "id":
        semantic = "ID"
    elif name.startswith(("is_", "has_")) or data_type == "bit":
        semantic = "FLAG"
        suggested_agg = "COUNT"
    elif name in {"created_at", "updated_at", "modified_at"}:
        semantic = "AUDIT_TIMESTAMP"
    elif data_type in {"date", "datetime", "datetime2"}:
        semantic = "DATE"
    elif data_type in {"int", "bigint", "decimal", "numeric", "float"}:
        semantic = "MEASURE"
        suggested_agg = "SUM"
    elif data_type in {"varchar", "nvarchar", "text"}:
        semantic = "DIMENSION"
        if cardinality and cardinality > 1000:
            semantic = "HIGH_CARD_TEXT"

    is_pii = any(
        k in name for k in ("email", "phone", "mobile", "ssn", "pan", "aadhaar")
    )

    return {
        "semantic_type": semantic,
        "is_measure": semantic == "MEASURE",
        "is_dimension": semantic in {"DIMENSION", "HIGH_CARD_TEXT"},
        "is_pii_candidate": is_pii,
        "suggested_aggregation": suggested_agg,
        "needs_llm": semantic in {"UNKNOWN", "MEASURE", "DIMENSION"},
        "is_join_key": is_primary_key or is_foreign_key or semantic in {"ID", "SURROGATE_KEY"},
    }


# ------------------------------------------------------------------------------------
# AGGREGATION POLICY (DECLARATIVE)
# ------------------------------------------------------------------------------------

def build_aggregation_policy(column: Dict[str, Any]) -> Dict[str, Any]:
    semantic = column.get("semantic_type")
    data_type = str(column.get("data_type", "")).lower()
    cardinality = column.get("cardinality")

    policy = {
        "allowed": False,
        "recommended_aggregations": [],
        "forbidden_aggregations": [],
        "requires_deduplication": False,
        "confidence": 0.9,
    }

    if semantic == "MEASURE":
        policy["allowed"] = True
        policy["recommended_aggregations"] = ["SUM"]
        policy["forbidden_aggregations"] = ["COUNT"]
        if cardinality and cardinality > 1_000_000:
            policy["requires_deduplication"] = True

    elif semantic == "FLAG":
        policy["allowed"] = True
        policy["recommended_aggregations"] = ["COUNT"]
        policy["forbidden_aggregations"] = ["SUM", "AVG"]

    elif semantic in {"ID", "SURROGATE_KEY"}:
        policy["allowed"] = False
        policy["confidence"] = 1.0

    elif semantic in {"DATE", "AUDIT_TIMESTAMP"}:
        policy["allowed"] = False

    return policy


# ------------------------------------------------------------------------------------
# OPTIONAL LLM ENRICHMENT (ABSTRACTED)
# ------------------------------------------------------------------------------------

def llm_enrich_column(column: Dict[str, Any], domain_context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Placeholder for LLM enrichment (Azure OpenAI / OpenAI).
    """
    display = column["column_name"].replace("_", " ").title()

    kb_context = str(domain_context.get("domain_knowledge_context") or "").strip()
    description_suffix = "analytics"
    if kb_context:
        description_suffix = "analytics using the configured domain knowledge base"

    return {
        "business_description": f"{display} used for {domain_context.get('business_objective') or description_suffix}.",
        "suggested_display_name": display,
        "suggested_join_key_for": None,
    }


# ------------------------------------------------------------------------------------
# JOIN DISCOVERY (SAFE, RULE-BASED)
# ------------------------------------------------------------------------------------

def _relationship_signature(join: Dict[str, Any]) -> Tuple[str, str, str, str]:
    return (
        str(join.get("left_table") or ""),
        str(join.get("left_column") or ""),
        str(join.get("right_table") or ""),
        str(join.get("right_column") or ""),
    )


def metadata_backed_joins(relationships: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    joins: List[Dict[str, Any]] = []
    for relationship in relationships:
        source_table = str(relationship.get("source_table_name") or "")
        referenced_table = str(relationship.get("referenced_table_name") or "")
        constraint_name = str(relationship.get("constraint_name") or "")
        for mapping in relationship.get("column_mapping", []) or []:
            joins.append(
                {
                    "left_table": source_table,
                    "left_column": mapping.get("source_column_name"),
                    "right_table": referenced_table,
                    "right_column": mapping.get("referenced_column_name"),
                    "cardinality": relationship.get("cardinality", "MANY_TO_ONE"),
                    "join_type": "INNER",
                    "confidence": relationship.get("confidence", 1.0),
                    "source": "FOREIGN_KEY",
                    "constraint_name": constraint_name,
                    "relationship_id": relationship.get("relationship_id"),
                    "certified": True,
                }
            )
    return joins


def discover_joins(tables: List[Dict[str, Any]], existing_joins: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    joins: List[Dict[str, Any]] = []
    index: Dict[str, List[Dict[str, Any]]] = {}
    existing_signatures: Set[Tuple[str, str, str, str]] = {
        _relationship_signature(join)
        for join in (existing_joins or [])
    }

    for table in tables:
        for col in table["columns"]:
            if col.get("semantic_type") in {"ID", "SURROGATE_KEY"}:
                index.setdefault(col["column_name"], []).append({
                    "table": table["table_name"],
                    "column": col["column_name"],
                    "cardinality": col.get("cardinality"),
                })

    for col_name, refs in index.items():
        if len(refs) < 2:
            continue

        for left in refs:
            for right in refs:
                if left["table"] == right["table"]:
                    continue

                candidate = {
                    "left_table": left["table"],
                    "left_column": left["column"],
                    "right_table": right["table"],
                    "right_column": right["column"],
                    "cardinality": "MANY_TO_ONE",
                    "join_type": "INNER",
                    "confidence": 0.55,
                    "source": "HEURISTIC",
                    "certified": False,
                }
                if _relationship_signature(candidate) in existing_signatures:
                    continue
                joins.append(candidate)

    return joins


# ------------------------------------------------------------------------------------
# COLUMN ENRICHMENT ORCHESTRATION
# ------------------------------------------------------------------------------------

def enrich_column(column: Dict[str, Any], domain_context: Dict[str, Any]) -> Dict[str, Any]:
    enriched = {**column}

    rule_result = rule_based_semantic_classification(column)
    enriched.update(rule_result)

    if column.get("embedding_version") != "ENRICHED" and rule_result["needs_llm"]:
        enriched.update(llm_enrich_column(column, domain_context))
        enriched["enrichment_source"] = "LLM"
    else:
        enriched["enrichment_source"] = "RULES"

    enriched["aggregation_policy"] = build_aggregation_policy(enriched)
    return enriched


# ------------------------------------------------------------------------------------
# LANGGRAPH NODE
# ------------------------------------------------------------------------------------

def semantic_enrichment_node(state: Stage01State) -> Stage01State:
    logger.info(
        "START Semantic Enrichment tables=%d use_domain_kb=%s",
        len((state.get("discovered_metadata") or {}).get("tables", [])),
        bool(state.get("use_domain_kb")),
        extra={"run_id": state.get("run_id"), "node": "semantic_enrichment", "stage": "enrichment", "event_type": "node_start"},
    )
    new_state = state.copy()
    discovered = state.get("discovered_metadata", {})
    profiling = state.get("column_profiles", {})
    kb_cfg = get_domain_kb_config()
    use_domain_kb = bool(state.get("use_domain_kb")) and kb_cfg.enabled

    table_names = []
    column_names = []
    for table in discovered.get("tables", []):
        table_names.append(str(table.get("table_name") or ""))
        for col in table.get("columns", []):
            column_names.append(str(col.get("column_name") or ""))

    if use_domain_kb:
        kb_result = load_domain_kb(
            query_text=" ".join(table_names + column_names),
            top_k=kb_cfg.top_k_enrichment,
            max_chars=kb_cfg.max_chars_enrichment,
            content_types=[KB_CONTENT_TABLE, KB_CONTENT_PII, KB_CONTENT_MEASURE],
        )
    else:
        kb_result = {"context_text": "", "rows_retrieved": 0, "chars_injected": 0, "knowledge_base_id": kb_cfg.knowledge_base_id}

    domain_context = {
        "business_objective": state.get("business_objective"),
        "data_domains": state.get("data_domains"),
        "domain_knowledge_context": kb_result.get("context_text", ""),
    }

    discovered_relationships = discovered.get("table_relationships", []) if isinstance(discovered, dict) else []
    primary_keys = discovered.get("primary_keys", []) if isinstance(discovered, dict) else []
    foreign_keys = discovered.get("foreign_keys", []) if isinstance(discovered, dict) else []
    primary_key_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
    foreign_key_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for item in primary_keys:
        primary_key_map[(str(item.get("table_name") or ""), str(item.get("column_name") or ""))] = item
    for item in foreign_keys:
        foreign_key_map[(str(item.get("source_table_name") or ""), str(item.get("source_column_name") or ""))] = item

    enriched_columns: List[Dict[str, Any]] = []
    enriched_tables: List[Dict[str, Any]] = []

    for table in discovered.get("tables", []):
        cols = []
        for col in table.get("columns", []):
            profile = next(
                (p for p in profiling.get("column_profiles", [])
                 if p["column_name"] == col["column_name"]
                 and p["table_name"] == table["table_name"]),
                {},
            )
            pk_info = primary_key_map.get((str(table.get("table_name") or ""), str(col.get("column_name") or "")), {})
            fk_info = foreign_key_map.get((str(table.get("table_name") or ""), str(col.get("column_name") or "")), {})
            merged = {
                **col,
                **profile,
                "table_name": table["table_name"],
                "is_primary_key": bool(pk_info),
                "primary_key_constraint_name": pk_info.get("constraint_name"),
                "is_foreign_key": bool(fk_info),
                "foreign_key_constraint_name": fk_info.get("constraint_name"),
                "references_table_name": fk_info.get("referenced_table_name"),
                "references_column_name": fk_info.get("referenced_column_name"),
            }
            enriched = enrich_column(merged, domain_context)
            cols.append(enriched)
            enriched_columns.append(enriched)

        enriched_tables.append({
            "table_name": table["table_name"],
            "columns": cols,
        })

    certified_joins = metadata_backed_joins(discovered_relationships)
    heuristic_joins = discover_joins(enriched_tables, existing_joins=certified_joins)
    joins = certified_joins + heuristic_joins
    semantic_counts: Dict[str, int] = {}
    for column in enriched_columns:
        semantic_type = str(column.get("semantic_type") or "UNKNOWN")
        semantic_counts[semantic_type] = semantic_counts.get(semantic_type, 0) + 1

    payload = {
        "run_id": state.get("run_id"),
        "fingerprint": state.get("fingerprint"),
        "certified_tables": state.get("certified_tables", []),
        "enriched_at": datetime.now(timezone.utc).isoformat(),
        "domain_knowledge_base": {
            "enabled": use_domain_kb,
            "knowledge_base_id": kb_result.get("knowledge_base_id"),
            "rows_retrieved": kb_result.get("rows_retrieved", 0),
            "chars_injected": kb_result.get("chars_injected", 0),
            "content_types": kb_result.get("content_types"),
        },
        "columns": enriched_columns,
        "semantic_counts": semantic_counts,
        "table_relationships": discovered_relationships,
        "certified_joins": certified_joins,
        "join_candidates": heuristic_joins,
        "joins": joins,
    }

    ai_store_db_writer(
        run_id=state.get("run_id"),
        stage="Semantic Enrichment",
        artifact_type="ENRICHED_METADATA",
        payload=payload,
        schema_version="SemanticEnrichment_v1",
        prompt_version="NB09_SEMANTIC_ENRICHMENT_v1",
        faithfulness_status="NOT_APPLICABLE",
        token_count=0,
        input_tokens=0,
        output_tokens=0,
        fingerprint=state.get("fingerprint"),
    )

    new_state["enriched_metadata"] = payload
    new_state["certified_joins"] = certified_joins
    new_state["join_candidates"] = heuristic_joins
    new_state["table_relationships"] = discovered_relationships
    new_state["semantic_enrichment_status"] = "COMPLETED"
    logger.info(
        "END Semantic Enrichment tables=%d columns=%d certified_joins=%d heuristic_joins=%d kb_enabled=%s",
        len(enriched_tables),
        len(enriched_columns),
        len(certified_joins),
        len(heuristic_joins),
        use_domain_kb,
        extra={"run_id": state.get("run_id"), "node": "semantic_enrichment", "stage": "enrichment", "event_type": "node_end"},
    )
    return new_state


# ------------------------------------------------------------------------------------
# GRAPH BUILDER
# ------------------------------------------------------------------------------------

def build_semantic_enrichment_graph() -> StateGraph:
    graph = StateGraph(Stage01State)
    graph.add_node("semantic_enrichment", semantic_enrichment_node)
    graph.set_entry_point("semantic_enrichment")
    graph.set_finish_point("semantic_enrichment")
    return graph


def compile_semantic_enrichment_graph():
    return build_semantic_enrichment_graph().compile(checkpointer=MemorySaver())
