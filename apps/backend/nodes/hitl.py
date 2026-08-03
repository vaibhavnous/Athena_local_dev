"""
HITL review nodes for LangGraph.
Placed after interrupts. Certify artifacts to `ai_store` when review is completed.
"""

import hashlib
from typing import Any, Callable, Dict, List, Tuple

from state import Stage01State
from utilis.db import ai_store_db_writer, ensure_hitl_queue_items
from utilis.logger import logger


GATE3_ALLOWED_SEMANTIC_TYPES = {
    "MEASURE", "DIMENSION", "ID", "SURROGATE_KEY", "DATE",
    "AUDIT_TIMESTAMP", "PII", "FLAG", "HIGH_CARD_TEXT", "UNKNOWN",
}
GATE3_DIMENSION_TYPES = {"DIMENSION", "DATE", "FLAG"}


def _column_identity(column: Dict[str, Any]) -> Tuple[str, str, str, str]:
    return tuple(
        str(column.get(field) or "").strip().casefold()
        for field in ("database_name", "schema_name", "table_name", "column_name")
    )


def _qualified_table_name(column: Dict[str, Any]) -> str:
    table_name = str(column.get("table_name") or column.get("table") or "").strip()
    if "." in table_name:
        return table_name
    return ".".join(
        part for part in (
            str(column.get("database_name") or "").strip(),
            str(column.get("schema_name") or "").strip(),
            table_name,
        ) if part
    )


def build_gate3_review_items(run_id: str, enrichment_artifact: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Build one stable, lossless Gate 3 queue item per qualified table."""
    grouped: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = {}
    table_labels: Dict[Tuple[str, str, str], str] = {}
    for column in enrichment_artifact.get("columns") or []:
        if not isinstance(column, dict):
            raise ValueError("Semantic enrichment contains a non-object column.")
        identity = _column_identity(column)
        if not identity[2] or not identity[3]:
            raise ValueError("Every semantic column requires table_name and column_name.")
        table_key = identity[:3]
        grouped.setdefault(table_key, []).append(dict(column))
        table_labels[table_key] = _qualified_table_name(column)

    summaries = enrichment_artifact.get("table_summaries") or {}
    items: List[Dict[str, Any]] = []
    for table_key, columns in grouped.items():
        qualified_name = table_labels[table_key]
        leaf_name = str(columns[0].get("table_name") or "").strip()
        digest = hashlib.sha256("|".join(table_key).encode("utf-8")).hexdigest()[:24]
        items.append({
            "item_id": f"{run_id}:3:{digest}",
            "content": {
                "database_name": str(columns[0].get("database_name") or "").strip(),
                "schema_name": str(columns[0].get("schema_name") or "").strip(),
                "table_name": leaf_name,
                "qualified_table_name": qualified_name,
                "table_summary": summaries.get(qualified_name) or summaries.get(leaf_name) or "",
                "columns": columns,
            },
        })
    return items


def validate_gate3_table_edit(
    original_content: Dict[str, Any],
    edited_content: Dict[str, Any],
) -> Dict[str, Any]:
    """Validate editable fields and merge them over immutable semantic metadata."""
    if not isinstance(edited_content, dict):
        raise ValueError("Semantic review draft must be an object.")
    original_columns = original_content.get("columns") or []
    edited_columns = edited_content.get("columns") or []
    if not original_columns or not isinstance(edited_columns, list):
        raise ValueError("Semantic review draft must contain the original table columns.")

    original_by_name: Dict[str, Dict[str, Any]] = {}
    for column in original_columns:
        name = str(column.get("column_name") or "").strip().casefold()
        if not name or name in original_by_name:
            raise ValueError("Original semantic metadata contains duplicate or unnamed columns.")
        original_by_name[name] = column

    edited_by_name: Dict[str, Dict[str, Any]] = {}
    for column in edited_columns:
        if not isinstance(column, dict):
            raise ValueError("Every edited semantic column must be an object.")
        name = str(column.get("column_name") or "").strip().casefold()
        if not name or name in edited_by_name:
            raise ValueError("Edited semantic metadata contains duplicate or unnamed columns.")
        edited_by_name[name] = column
    if set(edited_by_name) != set(original_by_name):
        raise ValueError("Columns cannot be added, removed, or renamed during semantic review.")

    merged_columns: List[Dict[str, Any]] = []
    display_names: set[str] = set()
    for name, original in original_by_name.items():
        edited = edited_by_name[name]
        for field in ("database_name", "schema_name", "table_name", "column_name", "data_type"):
            if field in edited and str(edited.get(field) or "").strip().casefold() != str(original.get(field) or "").strip().casefold():
                raise ValueError(f"{field} cannot be changed during semantic review.")

        display_name = str(edited.get("suggested_display_name") or "").strip()
        description = str(edited.get("business_description") or "").strip()
        semantic_type = str(edited.get("semantic_type") or "UNKNOWN").strip().upper()
        if not display_name or len(display_name) > 256:
            raise ValueError(f"{original['column_name']}: display name is required and must be at most 256 characters.")
        display_key = display_name.casefold()
        if display_key in display_names:
            raise ValueError(f"Duplicate display name in table: {display_name}")
        display_names.add(display_key)
        if not description or len(description) > 1000:
            raise ValueError(f"{original['column_name']}: business description is required and must be at most 1000 characters.")
        if semantic_type not in GATE3_ALLOWED_SEMANTIC_TYPES:
            raise ValueError(f"{original['column_name']}: unsupported semantic type '{semantic_type}'.")

        expected_measure = semantic_type == "MEASURE"
        expected_dimension = semantic_type in GATE3_DIMENSION_TYPES
        if "is_measure" in edited and bool(edited.get("is_measure")) != expected_measure:
            raise ValueError(f"{original['column_name']}: measure flag must match semantic type {semantic_type}.")
        if "is_dimension" in edited and bool(edited.get("is_dimension")) != expected_dimension:
            raise ValueError(f"{original['column_name']}: dimension flag must match semantic type {semantic_type}.")

        is_pii = semantic_type == "PII" or bool(edited.get("is_pii_candidate"))
        pii_type = str(edited.get("pii_type") or "").strip()
        if is_pii and (not pii_type or pii_type == "-"):
            raise ValueError(f"{original['column_name']}: PII type is required when PII is selected.")

        merged_columns.append({
            **original,
            "suggested_display_name": display_name,
            "business_description": description,
            "semantic_type": semantic_type,
            "is_measure": expected_measure,
            "is_dimension": expected_dimension,
            "is_pii_candidate": is_pii,
            "pii_type": pii_type if is_pii else None,
        })

    original_table = str(original_content.get("table_name") or "").strip()
    qualified_table = str(original_content.get("qualified_table_name") or original_table).strip()
    edited_table = str(edited_content.get("table_name") or qualified_table).strip()
    if edited_table.casefold() not in {original_table.casefold(), qualified_table.casefold()}:
        raise ValueError("table_name cannot be changed during semantic review.")
    table_summary = str(edited_content.get("table_summary") or original_content.get("table_summary") or "").strip()
    if len(table_summary) > 4000:
        raise ValueError("Table summary must be at most 4000 characters.")
    return {**original_content, "table_summary": table_summary, "columns": merged_columns}


def apply_gate3_review_rows(
    enrichment_artifact: Dict[str, Any],
    rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Apply approved queue content to the full artifact without losing non-editable fields."""
    reviewed_columns: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    summaries = dict(enrichment_artifact.get("table_summaries") or {})
    for row in rows:
        if str(row.get("gate_status") or "").upper() not in {"APPROVED", "EDITED"}:
            raise ValueError("Every Gate 3 item must be approved before certification.")
        original = row.get("original_content") or {}
        edited = row.get("edited_content")
        effective = validate_gate3_table_edit(original, edited or original)
        human_edited = bool(edited)
        for column in effective["columns"]:
            reviewed = {**column, "human_review_status": "EDITED" if human_edited else "APPROVED"}
            if human_edited:
                reviewed.update({"confidence": 1.0, "enrichment_source": "HUMAN_REVIEW", "needs_review": False})
            key = _column_identity(reviewed)
            if key in reviewed_columns:
                raise ValueError("Gate 3 review contains duplicate qualified columns.")
            reviewed_columns[key] = reviewed
        qualified_name = str(effective.get("qualified_table_name") or effective.get("table_name") or "").strip()
        if qualified_name:
            summaries[qualified_name] = effective.get("table_summary") or ""

    base_columns = enrichment_artifact.get("columns") or []
    base_keys = [_column_identity(column) for column in base_columns]
    if len(base_keys) != len(set(base_keys)) or set(base_keys) != set(reviewed_columns):
        raise ValueError("Gate 3 review does not exactly cover the enriched metadata columns.")
    certified_columns = [reviewed_columns[key] for key in base_keys]
    semantic_counts: Dict[str, int] = {}
    source_counts: Dict[str, int] = {}
    for column in certified_columns:
        semantic_type = str(column.get("semantic_type") or "UNKNOWN")
        enrichment_source = str(column.get("enrichment_source") or "UNKNOWN")
        semantic_counts[semantic_type] = semantic_counts.get(semantic_type, 0) + 1
        source_counts[enrichment_source] = source_counts.get(enrichment_source, 0) + 1
    return {
        **enrichment_artifact,
        "columns": certified_columns,
        "table_summaries": summaries,
        "semantic_counts": semantic_counts,
        "enrichment_source_counts": source_counts,
        "quality_summary": {
            **(enrichment_artifact.get("quality_summary") or {}),
            "columns_total": len(certified_columns),
            "columns_needing_review": sum(1 for column in certified_columns if column.get("needs_review")),
        },
        "gate3_review_summary": {
            "tables_reviewed": len(rows),
            "columns_reviewed": len(certified_columns),
            "tables_edited": sum(1 for row in rows if row.get("edited_content")),
            "pii_columns_certified": sum(1 for column in certified_columns if column.get("is_pii_candidate")),
            "measure_columns_certified": sum(1 for column in certified_columns if column.get("is_measure")),
        },
    }


def certify_hitl_enrichment(run_id: str, enrichment_artifact: dict, fingerprint: str | None = None) -> None:
    ai_store_db_writer(
        run_id=run_id,
        stage="HITL Enrichment Certification",
        artifact_type="GATE3_APPROVED_ENRICHMENT",
        payload={
            "fingerprint": fingerprint or run_id,
            "storage_fingerprint": f"{fingerprint or run_id}:GATE3_APPROVED_ENRICHMENT",
            "run_id": run_id,
            "enrichment_artifact": enrichment_artifact,
            "source": "HUMAN_CERTIFIED_ENRICHMENT",
        },
        schema_version="GATE3_v1",
        prompt_version="NB09B_ENRICHMENT_REVIEW_v1",
        faithfulness_status="PASSED",
        token_count=0,
        input_tokens=0,
        output_tokens=0,
        fingerprint=fingerprint or run_id,
    )


def build_hitl_enrichment_review_node() -> Callable[[Stage01State], Stage01State]:
    """
    Gate 3 HITL review for semantic enrichment.
    Auto-approves in dev mode. In prod, waits for reviewed flags/decision in state.
    """
    import os

    def hitl_enrichment_review_node(state: Stage01State) -> Stage01State:
        log_context = {"run_id": state.get("run_id", "unknown"), "node": "enrichment_review"}
        dev_mode = os.getenv("DEV_MODE", "").strip().lower() in {"1", "true", "yes", "on", "dev"}

        explicit_decision = str(state.get("enrichment_review_decision") or "").upper()
        if dev_mode and explicit_decision not in {"APPROVED", "REJECTED"}:
            logger.info("[DEV] Auto-approving enrichment review", extra=log_context)
            new_state = state.copy()
            new_state["status"] = "GATE3_COMPLETE"
            new_state["next_gate"] = None
            new_state["next_review_key"] = None
            artifact = state.get("enriched_metadata")
            new_state["enrichment_review_status"] = "COMPLETED"
            new_state["semantic_tags_reviewed"] = True
            new_state["pii_classifications_reviewed"] = True
            new_state["join_key_annotations_reviewed"] = True
            new_state["enrichment_review_decision"] = "APPROVED"
            new_state["enrichment_review_artifact"] = artifact
            certify_hitl_enrichment(state["run_id"], artifact, state.get("fingerprint"))
            return new_state

        if explicit_decision == "REJECTED":
            logger.warning("Enrichment review rejected by human", extra=log_context)
            new_state = state.copy()
            new_state["status"] = "FAILED"
            new_state["next_gate"] = None
            new_state["next_review_key"] = None
            new_state["enrichment_review_status"] = "FAILED"
            new_state["enrichment_review_error"] = "Rejected by reviewer"
            new_state["resume_message"] = "Semantic Review was rejected."
            return new_state

        if not state.get("semantic_tags_reviewed") or not state.get("pii_classifications_reviewed"):
            artifact = state.get("enriched_metadata") or {}
            if artifact:
                ensure_hitl_queue_items(
                    state["run_id"],
                    build_gate3_review_items(state["run_id"], artifact),
                    gate_number=3,
                )
            logger.info("Enrichment review pending human validation", extra=log_context)
            new_state = state.copy()
            new_state["status"] = "HITL_WAIT"
            new_state["next_gate"] = 3
            new_state["next_review_key"] = None
            new_state["enrichment_review_status"] = "PENDING"
            new_state["enrichment_review_decision"] = "PENDING"
            new_state["resume_message"] = "Semantic Review is pending. Review the enriched metadata before continuing."
            return new_state

        if state.get("enrichment_review_decision") == "APPROVED":
            artifact = state.get("enriched_metadata")
            certify_hitl_enrichment(state["run_id"], artifact, state.get("fingerprint"))
            new_state = state.copy()
            new_state["status"] = "GATE3_COMPLETE"
            new_state["next_gate"] = None
            new_state["next_review_key"] = None
            new_state["enrichment_review_status"] = "COMPLETED"
            new_state["enrichment_review_artifact"] = artifact
            new_state["resume_message"] = "Semantic Review approved. Bronze generation is starting."
            return new_state

        new_state = state.copy()
        new_state["status"] = "HITL_WAIT"
        new_state["next_gate"] = 3
        new_state["next_review_key"] = None
        new_state["enrichment_review_status"] = "PENDING"
        new_state["enrichment_review_decision"] = "PENDING"
        new_state["resume_message"] = "Semantic Review is pending. Review the enriched metadata before continuing."
        return new_state

    return hitl_enrichment_review_node


def certify_hitl_result(run_id: str, certified_kpis: List[Dict], fingerprint: str | None = None) -> None:
    ai_store_db_writer(
        run_id=run_id,
        stage="HITL Certification",
        artifact_type="GATE1_CERTIFIED_KPIS",
        payload={
            "fingerprint": fingerprint or run_id,
            "storage_fingerprint": f"{fingerprint or run_id}:GATE1_CERTIFIED_KPIS",
            "run_id": run_id,
            "certified_kpi_count": len(certified_kpis),
            "certified_kpis": certified_kpis,
            "source": "HUMAN_CERTIFIED",
        },
        schema_version="GATE1_v1",
        prompt_version="CLI_REVIEWER_v1",
        faithfulness_status="PASSED",
        token_count=0,
        input_tokens=0,
        output_tokens=0,
        fingerprint=fingerprint or run_id,
    )


def certify_hitl_tables(run_id: str, certified_tables: List[Dict], fingerprint: str | None = None) -> None:
    ai_store_db_writer(
        run_id=run_id,
        stage="HITL Table Certification",
        artifact_type="GATE2_CERTIFIED_TABLES",
        payload={
            "fingerprint": fingerprint or run_id,
            "storage_fingerprint": f"{fingerprint or run_id}:GATE2_CERTIFIED_TABLES",
            "run_id": run_id,
            "certified_table_count": len(certified_tables),
            "certified_tables": certified_tables,
            "source": "HUMAN_CERTIFIED_TABLES",
        },
        schema_version="GATE2_v1",
        prompt_version="CLI_REVIEWER_v1",
        faithfulness_status="PASSED",
        token_count=0,
        input_tokens=0,
        output_tokens=0,
        fingerprint=fingerprint or run_id,
    )


def build_hitl_review_node() -> Callable[[Stage01State], Stage01State]:
    def hitl_review_node(state: Stage01State) -> Stage01State:
        log_context = {"run_id": state.get("run_id", "unknown"), "node": "hitl_review"}
        human_decision = state.get("human_decision")

        if human_decision != "COMPLETED":
            logger.info("HITL review pending human decision", extra=log_context)
            new_state = state.copy()
            new_state["status"] = "HITL_WAIT"
            new_state["human_decision"] = human_decision or "PENDING"
            new_state["next_gate"] = 1
            new_state["next_review_key"] = None
            new_state["resume_message"] = "KPI Review is pending. Review the extracted KPIs before continuing."
            return new_state

        certified_kpis = state.get("certified_kpis")
        if not certified_kpis:
            logger.warning("No certified KPIs found despite COMPLETED decision", extra=log_context)
            return {**state, "status": "FAILED", "error": "No certified KPIs", "next_gate": None}

        run_id = state["run_id"]
        fingerprint = state.get("fingerprint", run_id)
        certify_hitl_result(run_id, certified_kpis, fingerprint)

        logger.info("HITL certified %d KPIs to ai_store", len(certified_kpis), extra=log_context)
        new_state = state.copy()
        new_state["status"] = "GATE1_COMPLETE"
        new_state["next_gate"] = None
        new_state["next_review_key"] = None
        new_state["resume_message"] = "KPI Review approved. Table extraction is starting."
        return new_state

    return hitl_review_node


def build_hitl_table_review_node() -> Callable[[Stage01State], Stage01State]:
    def hitl_table_review_node(state: Stage01State) -> Stage01State:
        log_context = {"run_id": state.get("run_id", "unknown"), "node": "hitl_table_review"}
        human_table_decision = state.get("human_table_decision")

        if human_table_decision != "COMPLETED":
            logger.info("HITL table review pending human decision", extra=log_context)
            new_state = state.copy()
            new_state["status"] = "HITL_WAIT"
            new_state["human_table_decision"] = human_table_decision or "PENDING"
            new_state["next_gate"] = 2
            new_state["next_review_key"] = None
            new_state["resume_message"] = "Table Review is pending. Review the nominated tables before continuing."
            return new_state

        certified_tables = state.get("certified_tables")
        if not certified_tables:
            logger.warning("No certified tables found despite COMPLETED decision", extra=log_context)
            return {**state, "status": "FAILED", "error": "No certified tables after Gate 2", "next_gate": None}

        run_id = state["run_id"]
        fingerprint = state.get("fingerprint", run_id)
        certify_hitl_tables(run_id, certified_tables, fingerprint)

        logger.info("HITL certified %d tables to ai_store", len(certified_tables), extra=log_context)
        new_state = state.copy()
        new_state["status"] = "GATE2_COMPLETE"
        new_state["next_gate"] = None
        new_state["next_review_key"] = None
        new_state["resume_message"] = "Table Review approved. Column extraction is starting."
        return new_state

    return hitl_table_review_node


hitl_review_node = build_hitl_review_node()
hitl_table_review_node = build_hitl_table_review_node()
