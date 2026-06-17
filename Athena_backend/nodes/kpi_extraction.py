from __future__ import annotations

import json
import os
from typing import Any, Callable, Dict, List, Optional

import pydantic
from pydantic import BaseModel, Field
from langchain_core.callbacks.base import BaseCallbackHandler
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage
from pinecone import Pinecone

from nodes.ingestion import _embedding_model
from state import Stage01State
from schema import KPISchema, KPISchemaItem, DerivationType
from utilis.logger import logger
from utilis.db import ai_store_db_writer, config as db_config, insert_hitl_queue_items, get_pipeline_connection
from nodes.req_extraction import get_llm, compute_cost_usd, TokenAccumulator, _strip_fences, handoff_validator

SYSTEM_PROMPT_KPI = """You are a KPI analyst specialized in data-driven systems.

Extract ONLY high-quality KPIs that can be computed from data.

STRICT RULES:
1. KPI MUST be measurable using data columns.
   - Must include measurable terms like:
     time, rate, count, amount, ratio, percentage, frequency, duration
2. KPI MUST be actionable and computable.
   - Avoid abstract concepts.
3. REJECT KPIs like:
   - data stability
   - data coverage
   - data governance
   - platform performance
4. If KPI is abstract, CONVERT it into measurable form.
5. Output MUST be JSON array with max 10 KPIs.
6. Each KPI must be unique, specific, and tied to measurable data.

FORMAT:
[
  {{
    "kpi_name": "...",
    "kpi_description": "...",
    "ai_confidence_score": 0.0,
    "derivation_type": "explicit" | "implicit",
    "source_requirement_ref": "..."
  }}
]"""

MEASURABLE_TERMS = {
    "time", "rate", "count", "amount", "ratio", "percentage",
    "frequency", "duration", "total", "average", "avg", "volume",
}
ABSTRACT_KPI_PHRASES = {
    "data stability", "data coverage", "data governance", "platform performance",
}


def _build_requirements(state: Stage01State) -> Dict[str, Any]:
    """Consolidate req_* fields into requirements dict."""
    return {
        "business_objective": state.get("req_business_objective", ""),
        "data_domains": state.get("req_data_domains", []),
        "reporting_frequency": state.get("req_reporting_frequency", ""),
        "target_audience": state.get("req_target_audience", ""),
        "constraints": state.get("req_constraints", []),
    }


def _resolve_source_databases(state: Stage01State) -> List[str]:
    source_databases = state.get("source_databases")
    if source_databases:
        return source_databases

    default_db = (
        db_config.get("azure_sql", {}).get("source_database")
        or db_config.get("azure_sql", {}).get("target_catalog")
        or db_config.get("azure_sql", {}).get("database_name")
    )
    return [default_db] if default_db else []


def _fetch_relevant_schema(brd_text: str, source_databases: List[str], top_k: int = 10) -> List[Dict[str, Any]]:
    if not brd_text.strip() or not source_databases or _embedding_model is None:
        return []

    try:
        pc = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
        index = pc.Index("metadata")
        query_vector = _embedding_model.embed_query(brd_text[:4000])
        results = index.query(
            vector=query_vector,
            top_k=top_k,
            include_metadata=True,
            namespace="schema",
        )
    except Exception as exc:
        logger.warning("Schema grounding lookup failed: %s", exc, extra={"node": "kpi_extraction"})
        return []

    source_set = {db.lower() for db in source_databases}
    schema_map: Dict[str, Dict[str, Any]] = {}
    matches = getattr(results, "matches", None)
    if matches is None and isinstance(results, dict):
        matches = results.get("matches", [])
    matches = matches or []

    for match in matches:
        meta = getattr(match, "metadata", {}) or {}
        db = str(meta.get("database_name", "")).lower()
        if db not in source_set:
            continue

        table = str(meta.get("table_name", "")).lower()
        schema = str(meta.get("schema_name", "dbo")).lower()
        key = f"{db}.{schema}.{table}"
        row = schema_map.setdefault(
            key,
            {
                "database_name": db,
                "schema_name": schema,
                "table_name": table,
                "columns": set(),
            },
        )
        if meta.get("column_name"):
            row["columns"].add(str(meta["column_name"]))

    grounded_schema: List[Dict[str, Any]] = []
    for row in schema_map.values():
        grounded_schema.append({**row, "columns": sorted(row["columns"])[:8]})
    return grounded_schema[:top_k]


def _format_schema_context(schema_rows: List[Dict[str, Any]]) -> str:
    if not schema_rows:
        return "Available Data Schema:\n- No schema context found"

    lines = ["Available Data Schema:"]
    for row in schema_rows:
        columns = ", ".join(row.get("columns", [])[:8]) or "no columns captured"
        lines.append(f"- {row['table_name']} ({columns})")
    return "\n".join(lines)


def _format_file_source_context(state: Stage01State, context_text: str) -> str:
    source = str(state.get("source") or "").lower()
    if source not in {"sftp", "adls_gen2"}:
        return ""

    columns = [str(column) for column in (state.get("source_columns") or []) if str(column).strip()]
    row_count = state.get("source_row_count")
    source_label = "ADLS Gen2" if source == "adls_gen2" else "SFTP"
    lines = [f"{source_label} file-source context:"]
    if row_count is not None:
        lines.append(f"- Rows ingested: {row_count}")
    if columns:
        lines.append("- Available columns: " + ", ".join(columns))
    if state.get("candidate_feeds"):
        feeds = [
            str(feed.get("entity") or feed.get("feed_id") or "").strip()
            for feed in state.get("candidate_feeds") or []
            if isinstance(feed, dict)
        ]
        feeds = [feed for feed in feeds if feed]
        if feeds:
            lines.append("- Discovered feeds: " + ", ".join(feeds))
    if context_text:
        lines.append("- Dataset summary: " + context_text[:1200])
    return "\n".join(lines)


def _coerce_kpi_payload(raw: Any) -> List[Dict[str, Any]]:
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for key in ("kpis", "items", "results"):
            value = raw.get(key)
            if isinstance(value, list):
                return value
    raise ValueError("KPI response must be a JSON array or an object containing a kpis array")


def _fallback_kpis_from_columns(state: Stage01State) -> List[Dict[str, Any]]:
    source = str(state.get("source") or "").lower()
    if source not in {"sftp", "adls_gen2"}:
        return []

    columns = {str(column).strip().lower(): str(column).strip() for column in (state.get("source_columns") or [])}
    if not columns:
        return []

    kpis: List[Dict[str, Any]] = [
        {
            "kpi_name": "Total Record Count",
            "kpi_description": "Count of records ingested from the file source for the current pipeline run.",
            "ai_confidence_score": 0.68,
            "derivation_type": "implicit",
            "source_requirement_ref": "source_columns",
            "grounding": "STRONG",
        }
    ]

    if "currency" in columns:
        kpis.append({
            "kpi_name": "Currency Distribution Count",
            "kpi_description": f"Count of records grouped by {columns['currency']} to monitor transaction volume by currency.",
            "ai_confidence_score": 0.66,
            "derivation_type": "implicit",
            "source_requirement_ref": columns["currency"],
            "grounding": "STRONG",
        })

    date_column = next((original for lower, original in columns.items() if "date" in lower), None)
    if date_column:
        kpis.append({
            "kpi_name": "Daily Record Count",
            "kpi_description": f"Count of records grouped by {date_column} to track daily transaction volume.",
            "ai_confidence_score": 0.66,
            "derivation_type": "implicit",
            "source_requirement_ref": date_column,
            "grounding": "STRONG",
        })

    user_column = next((columns[key] for key in ("user", "username") if key in columns), None)
    if user_column:
        kpis.append({
            "kpi_name": "Unique User Count",
            "kpi_description": f"Count of distinct {user_column} values represented in the ingested file source.",
            "ai_confidence_score": 0.64,
            "derivation_type": "implicit",
            "source_requirement_ref": user_column,
            "grounding": "STRONG",
        })

    return kpis[:5]


def _is_measurable_kpi(kpi: Dict[str, Any]) -> bool:
    text = f"{kpi.get('kpi_name', '')} {kpi.get('kpi_description', '')}".lower()
    if any(phrase in text for phrase in ABSTRACT_KPI_PHRASES):
        return False
    return any(term in text for term in MEASURABLE_TERMS)


def _grounding_check(kpis: List[Dict], requirements: Dict[str, Any], brd_text: str) -> List[Dict]:
    """Filter KPIs with weak grounding to requirements/BRD."""
    import re

    req_text = " ".join([str(v) for v in requirements.values() if isinstance(v, (str, list))])
    all_text = f"{req_text} {brd_text}".lower()
    grounded = []
    for kpi in kpis:
        if not _is_measurable_kpi(kpi):
            logger.warning("Dropped KPI (not measurable enough): %s", kpi["kpi_name"])
            continue
        kpi_lower = f"{kpi['kpi_name']} {kpi['kpi_description']}".lower()
        keywords = [w for w in kpi_lower.split() if len(w) > 2]
        grounding_score = any(re.search(rf"\\b{re.escape(w)}\\b", all_text) for w in keywords)
        if grounding_score or kpi["ai_confidence_score"] >= 0.6:
            kpi["grounding"] = "STRONG" if grounding_score else "WEAK"
            grounded.append(kpi)
        else:
            logger.warning("Dropped KPI (no grounding): %s", kpi["kpi_name"])
    return grounded


def _remove_duplicates_and_rejected(kpis: List[Dict], rejected_kpis: List[str]) -> List[Dict]:
    """Deduplicate by kpi_name, filter rejected."""
    seen = set()
    unique = []
    for kpi in kpis:
        name = kpi["kpi_name"].lower()
        if name not in seen and kpi["kpi_name"] not in rejected_kpis:
            seen.add(name)
            unique.append(kpi)
    return unique


def build_kpi_extraction_node(
    llm_provider: str = "azure_openai",
    max_retries: int = 3,
) -> Callable[[Stage01State], Stage01State]:
    def kpi_extraction_node(state: Stage01State) -> Stage01State:
        tokens_used = 0
        cost_usd = 0.0
        attempts = 0
        source = "LLM"
        token_acc = None

        log_context = {"run_id": state.get("run_id", "unknown"), "node": "kpi_extraction"}

        if state.get("status") == "FAILED":
            logger.warning("Skipping kpi_extraction: status=FAILED", extra=log_context)
            return state

        handoff_validator("KPI Extraction", state, [
            "run_id", "context_text", "fingerprint",
            "req_business_objective", "req_data_domains",
        ])

        run_id = state["run_id"]
        fingerprint = state["fingerprint"]
        context_text = state["context_text"]
        requirements = _build_requirements(state)
        source_databases = _resolve_source_databases(state)
        relevant_schema = _fetch_relevant_schema(context_text, source_databases, top_k=10)
        schema_context = _format_schema_context(relevant_schema)
        file_source_context = _format_file_source_context(state, context_text)

        if state.get("memory_layer1", False) and state.get("prior_kpis"):
            kpis = state["prior_kpis"][:25]
            source = "MEMORY_LAYER1"
            logger.info("PATH A: Using MEMORY_LAYER1 prior_kpis (n=%d)", len(kpis), extra=log_context)
        else:
            llm = get_llm(provider=llm_provider)
            token_acc = TokenAccumulator()
            last_error = None
            kpis = []
            rejected_kpis = state.get("rejected_kpis", [])

            for attempt in range(max_retries + 1):
                user_prompt = f"""Requirements: {json.dumps(requirements, indent=2)}
{schema_context}
{file_source_context}
Rejected KPI names: {json.dumps(rejected_kpis)}
Extract KPIs ONLY based on the available schema or file-source columns above.
For file sources, every KPI must reference at least one available column when possible."""

                if attempt > 0:
                    user_prompt += f"\n\nPREV ERROR: {last_error}. Fix & ensure valid JSON array."

                if attempt == max_retries:
                    user_prompt += "\nFINAL: Force at least 3 valid KPIs."

                logger.info("KPI LLM attempt %d/%d (path=%s)", attempt + 1, max_retries + 1, source, extra=log_context)

                try:
                    response = llm.invoke(
                        [SystemMessage(content=SYSTEM_PROMPT_KPI), HumanMessage(content=user_prompt)],
                        config={"callbacks": [token_acc]},
                    )

                    raw_json = _strip_fences(response.content)
                    parsed_list = _coerce_kpi_payload(json.loads(raw_json))
                    kpis_parsed = [
                        KPISchemaItem.model_validate(kpi).model_dump(mode="json")
                        for kpi in parsed_list
                    ]

                    kpis_final = _remove_duplicates_and_rejected(kpis_parsed, rejected_kpis)
                    kpis_final = kpis_final[:10]
                    kpis_final = _grounding_check(kpis_final, requirements, context_text)

                    if len(kpis_final) == 0 and attempt == max_retries:
                        obj_name = requirements["business_objective"] or "Default Primary Objective"
                        kpis_final = [{
                            "kpi_name": obj_name[:50],
                            "kpi_description": "Primary objective metric",
                            "ai_confidence_score": 0.5,
                            "derivation_type": "implicit",
                            "source_requirement_ref": "business_objective",
                            "grounding": "WEAK",
                        }]

                    kpis = kpis_final
                    tokens_used = token_acc.total
                    cost_usd = compute_cost_usd(token_acc.total_input, token_acc.total_output)
                    attempts = attempt + 1
                    break

                except (json.JSONDecodeError, pydantic.ValidationError, Exception) as exc:
                    last_error = str(exc)[:300]
                    logger.warning("Attempt %d failed: %s", attempt + 1, last_error, extra=log_context)

            if not kpis:
                fallback_kpis = _fallback_kpis_from_columns(state)
                if fallback_kpis:
                    logger.warning(
                        "Using deterministic file-source KPI fallback after LLM extraction failure: n_kpis=%d",
                        len(fallback_kpis),
                        extra=log_context,
                    )
                    kpis = fallback_kpis
                    source = "FILE_SOURCE_FALLBACK"
                    attempts = max_retries + 1
                    tokens_used = token_acc.total if token_acc else 0
                    cost_usd = compute_cost_usd(token_acc.total_input, token_acc.total_output) if token_acc else 0.0

            if not kpis:
                logger.error("KPI extraction FAILED after %d attempts", max_retries + 1, extra=log_context)
                payload = {
                    "fingerprint": fingerprint,
                    "run_id": run_id,
                    "kpi_count": 0,
                    "source": source,
                    "error": last_error,
                }
                ai_store_db_writer(
                    run_id=run_id,
                    stage="KPI Extraction",
                    artifact_type="KPIS",
                    payload=payload,
                    schema_version="KPISchema_v1",
                    prompt_version="PROMPT_KPI_v1",
                    faithfulness_status="FAILED",
                    retry_count=max_retries,
                    token_count=tokens_used,
                    input_tokens=0,
                    output_tokens=0,
                    fingerprint=fingerprint,
                )
                return {**state, "status": "FAILED", "error": "KPI extraction failed"}

        ai_payload = {
            "fingerprint": fingerprint,
            "run_id": run_id,
            "kpi_count": len(kpis),
            "kpis": kpis,
            "cost_usd": cost_usd,
            "source": source,
        }
        ai_store_db_writer(
            run_id=run_id,
            stage="KPI Extraction",
            artifact_type="KPIS",
            payload=ai_payload,
            schema_version="KPISchema_v1",
            prompt_version="PROMPT_KPI_v1",
            faithfulness_status="PASSED",
            retry_count=max(attempts - 1, 0),
            token_count=tokens_used,
            input_tokens=token_acc.total_input if token_acc else 0,
            output_tokens=token_acc.total_output if token_acc else 0,
            fingerprint=fingerprint,
        )
        logger.info("Skipping kpi_memory insert; current schema does not support detailed KPI columns", extra=log_context)

        new_state = state.copy()
        new_state.update({
            "kpis": kpis,
            "kpi_source": source,
            "kpi_tokens_used": tokens_used,
            "kpi_cost_usd": cost_usd,
            "kpi_attempts": attempts,
            "source_databases": source_databases,
        })

        logger.info("KPI Extraction success: source=%s n_kpis=%d cost=$%.6f", source, len(kpis), cost_usd, extra=log_context)

        # Queue for HITL review only once per run. If the queue already exists
        # or Gate 1 was already certified, do not recreate review items.
        from utilis.db import get_completed_items, get_pending_items

        existing_pending_gate1 = get_pending_items(run_id, 1)
        existing_completed_gate1 = get_completed_items(run_id, 1)

        new_state["extracted_kpis"] = kpis.copy()

        if state.get("human_decision") == "COMPLETED" or existing_completed_gate1:
            new_state["human_decision"] = "COMPLETED"
            if not new_state.get("certified_kpis"):
                new_state["certified_kpis"] = [item["kpi"] for item in existing_completed_gate1]
            logger.info(
                "KPI review already certified for run_id=%s; skipping Gate 1 queue creation",
                run_id,
                extra=log_context,
            )
        elif existing_pending_gate1:
            new_state["human_decision"] = "PENDING"
            logger.info(
                "Existing Gate 1 queue found for run_id=%s; skipping duplicate KPI review queue creation",
                run_id,
                extra=log_context,
            )
        else:
            insert_hitl_queue_items(run_id, kpis, gate_number=1)
            new_state["human_decision"] = "PENDING"

        # Checkpoint full state to Azure SQL DB (KPI stage only).
        # Merge with existing checkpoint so startup metadata like source/brd_filename
        # is not lost when this node persists its partial state.
        try:
            from services.pipeline_runtime import load_checkpoint_state, save_checkpoint_state

            existing_checkpoint = load_checkpoint_state(new_state["run_id"]) or {"run_id": new_state["run_id"]}
            save_checkpoint_state(new_state["run_id"], {**existing_checkpoint, **new_state, "run_id": new_state["run_id"]})
            logger.info("KPI checkpoint saved to DB for run_id=%s", new_state["run_id"], extra=log_context)
        except Exception as e:
            logger.warning("Checkpoint save failed (non-critical): %s", e, extra=log_context)

        return new_state

    return kpi_extraction_node


_KPI_EXTRACTION_NODE: Optional[Callable[[Stage01State], Stage01State]] = None


def kpi_extraction_node(state: Stage01State) -> Stage01State:
    global _KPI_EXTRACTION_NODE
    if _KPI_EXTRACTION_NODE is None:
        _KPI_EXTRACTION_NODE = build_kpi_extraction_node()
    return _KPI_EXTRACTION_NODE(state)
