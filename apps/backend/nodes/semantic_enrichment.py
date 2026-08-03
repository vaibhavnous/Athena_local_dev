"""
NB09 — Semantic Enrichment Node

Responsibilities:
- Column semantic classification (rule-first, LLM optional)
- Explicit join metadata capture (declarative, not executed)
- Explicit aggregation policy capture (rules only)
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
import os
import re
from typing import Any, Dict, List, Literal, Optional, Set, Tuple

from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.graph import StateGraph
from langgraph.checkpoint.memory import MemorySaver
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from nodes.column_profiling import DATE_TYPES, NUMERIC_TYPES, TEXT_TYPES
from nodes.req_extraction import TokenAccumulator, _strip_fences, compute_cost_usd, get_llm
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
    "DEFAULT",
    "UNKNOWN",
]

AggType = Literal["SUM", "AVG", "COUNT", "MIN", "MAX", "NONE"]

LLMSemanticType = Literal[
    "MEASURE",
    "DIMENSION",
    "DATE",
    "ID",
    "FLAG",
    "HIGH_CARD_TEXT",
    "AUDIT_TIMESTAMP",
    "UNKNOWN",
]

SEMANTIC_PROMPT_VERSION = "PROMPT_ENR_v2"
_SEMANTIC_SYSTEM_MESSAGE = (
    "You are a precise senior data analyst. Return only valid JSON matching the requested schema."
)
_DISPLAY_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 _&()/.-]*$")
_GENERIC_DESCRIPTION_PREFIXES = (
    "this column",
    "this field",
    "a column",
    "column stores",
    "column contains",
)


class LLMEnrichedColumn(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    database_name: str
    schema_name: str
    table_name: str
    column_name: str
    business_description: str = Field(min_length=10, max_length=1000)
    semantic_type: LLMSemanticType
    suggested_display_name: str = Field(min_length=2, max_length=128)
    is_pii_candidate: bool = False
    pii_type: Optional[str] = Field(default=None, max_length=100)
    synonyms: List[str] = Field(default_factory=list, max_length=10)
    confidence: float = Field(default=0.7, ge=0.0, le=1.0)

    @field_validator("suggested_display_name")
    @classmethod
    def validate_display_name(cls, value: str) -> str:
        if not _DISPLAY_NAME_RE.fullmatch(value):
            raise ValueError("suggested_display_name must be a readable business label")
        return value

    @field_validator("business_description")
    @classmethod
    def validate_description(cls, value: str) -> str:
        if value.casefold().startswith(_GENERIC_DESCRIPTION_PREFIXES):
            raise ValueError("business_description must describe the column's business meaning")
        return value

    @field_validator("synonyms")
    @classmethod
    def normalize_synonyms(cls, values: List[str]) -> List[str]:
        normalized: List[str] = []
        seen: Set[str] = set()
        for value in values:
            clean = str(value or "").strip()
            key = clean.casefold()
            if clean and key not in seen:
                seen.add(key)
                normalized.append(clean[:100])
        return normalized

    @model_validator(mode="after")
    def validate_pii_fields(self):
        if self.is_pii_candidate and not str(self.pii_type or "").strip():
            raise ValueError("pii_type is required when is_pii_candidate is true")
        if not self.is_pii_candidate:
            self.pii_type = None
        return self


class EnrichmentBatchResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enriched_columns: List[LLMEnrichedColumn]

    @model_validator(mode="after")
    def reject_duplicate_columns(self):
        keys = [_column_key(column.model_dump()) for column in self.enriched_columns]
        if len(keys) != len(set(keys)):
            raise ValueError("Duplicate columns in semantic enrichment output")
        return self


def _positive_int_env(name: str, default: int) -> int:
    try:
        return max(1, int(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        logger.warning("Invalid %s; using default=%d", name, default, extra={"node": "semantic_enrichment"})
        return default


def _confidence(value: Any, default: float) -> float:
    try:
        return min(1.0, max(0.0, float(value)))
    except (TypeError, ValueError):
        return default


def _snake_case(value: str) -> str:
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", str(value or "").strip())
    value = re.sub(r"[^A-Za-z0-9]+", "_", value).strip("_").lower()
    if not value:
        return "unknown_column"
    if value[0].isdigit():
        value = f"column_{value}"
    return value


def _business_display_name(value: str) -> str:
    """Convert technical identifiers into readable labels while preserving common acronyms."""
    text = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", str(value or "").strip())
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", text)
    tokens = re.sub(r"[^A-Za-z0-9]+", " ", text).strip().split()
    acronyms = {"api", "db", "dob", "id", "ip", "kpi", "pii", "sku", "sql", "uri", "url"}
    label = " ".join(token.upper() if token.casefold() in acronyms else token.capitalize() for token in tokens)
    return label or "Unknown Column"


def _column_key(column: Dict[str, Any]) -> Tuple[str, str, str, str]:
    return tuple(
        str(column.get(field) or "").strip().casefold()
        for field in ("database_name", "schema_name", "table_name", "column_name")
    )


def _masked_value_shape(value: Any) -> Optional[str]:
    """Describe sample structure without sending source values to the LLM."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "boolean"
    text = str(value)
    if not text:
        return "empty"
    shaped = re.sub(
        r"[A-Za-z]+|\d+",
        lambda match: f"{'A' if match.group(0)[0].isalpha() else '9'}{{{len(match.group(0))}}}",
        text,
    )
    shaped = re.sub(r"[^A{}0-9@._:+\-/]", "?", shaped)
    return f"length={len(text)} pattern={shaped[:120]}"


def _masked_sample_shapes(samples: Any) -> List[str]:
    shapes: List[str] = []
    for item in samples if isinstance(samples, list) else []:
        value = item
        if isinstance(item, dict):
            value = next(
                (
                    candidate
                    for key, candidate in item.items()
                    if str(key).casefold() not in {"count", "frequency", "occurrences"}
                ),
                None,
            )
        shape = _masked_value_shape(value)
        if shape and shape not in shapes:
            shapes.append(shape)
        if len(shapes) == 5:
            break
    return shapes


# ------------------------------------------------------------------------------------
# RULE-BASED SEMANTIC CLASSIFICATION
# ------------------------------------------------------------------------------------

def rule_based_semantic_classification(column: Dict[str, Any]) -> Dict[str, Any]:
    name = _snake_case(str(column.get("column_name") or ""))
    name_tokens = set(name.split("_"))
    data_type = str(column.get("data_type") or "").strip().lower()
    cardinality = column.get("cardinality")
    total_rows = column.get("total_rows")
    is_primary_key = bool(column.get("is_primary_key"))
    is_foreign_key = bool(column.get("is_foreign_key"))
    profile_tier = str(column.get("profile_tier") or "").upper()

    semantic: SemanticType = "UNKNOWN"
    suggested_agg: AggType = "NONE"
    needs_llm = True

    if is_primary_key:
        semantic = "SURROGATE_KEY" if name == "id" else "ID"
        needs_llm = False
    elif is_foreign_key or profile_tier == "ID" or name.endswith("_id") or name == "id":
        semantic = "ID"
        needs_llm = False
    elif name.startswith(("is_", "has_")) or data_type in {"bit", "boolean", "bool"}:
        semantic = "FLAG"
        suggested_agg = "COUNT"
        needs_llm = False
    elif profile_tier == "AUDIT" or name in {
        "created_at",
        "updated_at",
        "modified_at",
        "created_date",
        "updated_date",
        "modified_date",
        "load_timestamp",
        "ingested_at",
    }:
        semantic = "AUDIT_TIMESTAMP"
        needs_llm = False
    elif data_type in DATE_TYPES or profile_tier == "DATE":
        semantic = "DATE"
    elif data_type in NUMERIC_TYPES or profile_tier == "MEASURE":
        semantic = "MEASURE"
        suggested_agg = "SUM"
    elif data_type in TEXT_TYPES or profile_tier in {"DIMENSION", "HIGH_CARD_TEXT"}:
        semantic = "DIMENSION"
        high_cardinality_threshold = _positive_int_env(
            "COLUMN_PROFILING_HIGH_CARDINALITY_THRESHOLD",
            100,
        )
        if (
            profile_tier == "HIGH_CARD_TEXT"
            or isinstance(cardinality, int)
            and cardinality > high_cardinality_threshold
            and (not total_rows or cardinality / max(int(total_rows), 1) >= 0.8)
        ):
            semantic = "HIGH_CARD_TEXT"

    is_pii = bool(
        name_tokens.intersection(
            {
                "email",
                "phone",
                "mobile",
                "ssn",
                "aadhaar",
                "passport",
                "iban",
            }
        )
        or "social_security" in name
        or name == "pan"
        or name.endswith("_pan")
        or name in {"first_name", "last_name", "full_name", "date_of_birth", "dob"}
    )

    return {
        "semantic_type": semantic,
        "is_measure": semantic == "MEASURE",
        "is_dimension": semantic in {"DIMENSION", "DATE", "FLAG"},
        "is_pii_candidate": is_pii,
        "suggested_aggregation": suggested_agg,
        "needs_llm": needs_llm,
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
# LLM ENRICHMENT
# ------------------------------------------------------------------------------------

SEMANTIC_ENRICHMENT_PROMPT = """You are enriching database column metadata for enterprise analytics.

BUSINESS CONTEXT:
{domain_context}

COLUMNS:
{columns_json}

Return ONLY this JSON object:
{{
  "enriched_columns": [
    {{
      "database_name": "<exact database_name>",
      "schema_name": "<exact schema_name>",
      "table_name": "<exact table_name>",
      "column_name": "<exact column_name>",
      "business_description": "<specific 1-2 sentence business meaning>",
      "semantic_type": "<MEASURE|DIMENSION|DATE|ID|FLAG|HIGH_CARD_TEXT|AUDIT_TIMESTAMP|UNKNOWN>",
      "suggested_display_name": "<readable Title Case business label, for example Claim ID>",
      "is_pii_candidate": <true|false>,
      "pii_type": "<category or null>",
      "synonyms": ["<business alias>"],
      "confidence": <number from 0.0 to 1.0>
    }}
  ]
}}

Rules:
- Return exactly one result for every input column and no other columns.
- Preserve all four identity fields exactly.
- Use profile evidence to distinguish measures from numeric identifiers and category codes.
- PII is independent of semantic_type. Do not use PII as semantic_type.
- HIGH_CARD_TEXT is descriptive text unsuitable for ordinary grouping.
- Do not infer joins or keys without explicit evidence in the input.
- Descriptions must be business-specific; do not start with 'this column' or 'this field'.
- Never reconstruct or repeat source sample values from the supplied masked patterns.
- Return only JSON, with no markdown fences or explanation.
"""


def _column_context(column: Dict[str, Any]) -> Dict[str, Any]:
    total_rows = column.get("total_rows")
    cardinality = column.get("cardinality")
    uniqueness_ratio = None
    if isinstance(total_rows, int) and total_rows > 0 and isinstance(cardinality, int):
        uniqueness_ratio = round(cardinality / total_rows, 6)
    return {
        "database_name": str(column.get("database_name") or ""),
        "schema_name": str(column.get("schema_name") or ""),
        "table_name": str(column.get("table_name") or ""),
        "column_name": str(column.get("column_name") or ""),
        "data_type": str(column.get("data_type") or "unknown"),
        "is_nullable": column.get("is_nullable"),
        "profile_tier": column.get("profile_tier"),
        "null_rate": column.get("null_rate"),
        "cardinality": cardinality,
        "total_rows": total_rows,
        "uniqueness_ratio": uniqueness_ratio,
        "sample_shapes": _masked_sample_shapes(column.get("top_samples")),
        "is_primary_key": bool(column.get("is_primary_key")),
        "is_foreign_key": bool(column.get("is_foreign_key")),
        "references_table_name": column.get("references_table_name"),
        "references_column_name": column.get("references_column_name"),
        "rule_hint": column.get("semantic_type"),
    }


def _domain_context_text(domain_context: Dict[str, Any]) -> str:
    domains = domain_context.get("data_domains") or []
    if not isinstance(domains, list):
        domains = [domains]
    return json.dumps(
        {
            "business_objective": str(domain_context.get("business_objective") or "Enterprise analytics")[:1000],
            "data_domains": [str(value)[:100] for value in domains[:10]],
        },
        ensure_ascii=False,
    )


def _validate_batch_coverage(
    result: EnrichmentBatchResult,
    columns: List[Dict[str, Any]],
) -> Dict[Tuple[str, str, str, str], LLMEnrichedColumn]:
    expected = {_column_key(column): column for column in columns}
    actual = {_column_key(column.model_dump()): column for column in result.enriched_columns}
    missing = set(expected) - set(actual)
    unexpected = set(actual) - set(expected)
    if missing or unexpected:
        raise ValueError(
            f"LLM output identity mismatch: missing={sorted(missing)[:5]} "
            f"unexpected={sorted(unexpected)[:5]}"
        )
    if len(actual) != len(columns):
        raise ValueError("LLM output column count does not match input")
    display_names: Set[Tuple[str, str, str, str]] = set()
    for column in result.enriched_columns:
        display_key = (
            column.database_name.casefold(),
            column.schema_name.casefold(),
            column.table_name.casefold(),
            _business_display_name(column.suggested_display_name).casefold(),
        )
        if display_key in display_names:
            raise ValueError(
                f"Duplicate suggested_display_name '{column.suggested_display_name}' in table "
                f"{column.database_name}.{column.schema_name}.{column.table_name}"
            )
        display_names.add(display_key)
    return actual


def _enrich_batch(
    columns: List[Dict[str, Any]],
    domain_context: Dict[str, Any],
    llm: Any,
    token_accumulator: TokenAccumulator,
    *,
    max_retries: int,
    batch_label: str,
) -> List[Dict[str, Any]]:
    prompt = SEMANTIC_ENRICHMENT_PROMPT.format(
        domain_context=_domain_context_text(domain_context),
        columns_json=json.dumps([_column_context(column) for column in columns], ensure_ascii=False),
    )
    last_error: Optional[str] = None
    for attempt in range(max_retries + 1):
        retry_prompt = prompt
        if last_error:
            retry_prompt += (
                "\nPREVIOUS OUTPUT FAILED VALIDATION:\n"
                f"{last_error[:1000]}\nCorrect the output and return every requested column."
            )
        try:
            response = llm.invoke(
                [
                    SystemMessage(content=_SEMANTIC_SYSTEM_MESSAGE),
                    HumanMessage(content=retry_prompt),
                ],
                config={"callbacks": [token_accumulator]},
            )
            parsed = json.loads(_strip_fences(str(response.content)))
            validated = EnrichmentBatchResult.model_validate(parsed)
            by_key = _validate_batch_coverage(validated, columns)
            logger.info(
                "Semantic LLM batch %s completed columns=%d attempt=%d",
                batch_label,
                len(columns),
                attempt + 1,
                extra={"node": "semantic_enrichment"},
            )
            return [by_key[_column_key(column)].model_dump() for column in columns]
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            logger.warning(
                "Semantic LLM batch %s attempt %d/%d failed: %s",
                batch_label,
                attempt + 1,
                max_retries + 1,
                last_error[:500],
                extra={"node": "semantic_enrichment"},
            )
    raise RuntimeError(
        f"Semantic LLM batch {batch_label} failed after {max_retries + 1} attempts: {last_error}"
    )


def _llm_batches(columns: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    max_tables = _positive_int_env("SEMANTIC_ENRICH_MAX_TABLES_PER_BATCH", 5)
    token_threshold = _positive_int_env("SEMANTIC_ENRICH_TOKEN_THRESHOLD", 80_000)
    tables: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = {}
    for column in columns:
        table_key = _column_key(column)[:3]
        tables.setdefault(table_key, []).append(column)

    batches: List[List[Dict[str, Any]]] = []
    current: List[Dict[str, Any]] = []
    current_tables = 0
    current_tokens = 800
    for table_columns in tables.values():
        table_tokens = max(200, len(json.dumps([_column_context(column) for column in table_columns])) // 4)
        if current and (current_tables >= max_tables or current_tokens + table_tokens > token_threshold):
            batches.append(current)
            current = []
            current_tables = 0
            current_tokens = 800
        current.extend(table_columns)
        current_tables += 1
        current_tokens += table_tokens
    if current:
        batches.append(current)
    return batches


def _normalized_enrichment(
    column: Dict[str, Any],
    enrichment: Dict[str, Any],
    *,
    source: str,
    needs_review: bool,
) -> Dict[str, Any]:
    merged = {**column, **enrichment}
    for identity_field in ("database_name", "schema_name", "table_name", "column_name"):
        merged[identity_field] = column.get(identity_field)
    semantic = str(merged.get("semantic_type") or "UNKNOWN").strip().upper()
    if semantic == "DEFAULT":
        semantic = "UNKNOWN"
    if semantic not in {
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
    }:
        semantic = "UNKNOWN"
        needs_review = True

    pii_candidate = bool(merged.get("is_pii_candidate")) or semantic == "PII"
    pii_type = str(merged.get("pii_type") or "").strip()
    if pii_candidate and (not pii_type or pii_type == "-"):
        needs_review = True
    display_name = _business_display_name(
        str(merged.get("suggested_display_name") or column.get("column_name") or "unknown_column")
    )
    description = str(merged.get("business_description") or "").strip()
    if len(description) < 10:
        table_name = str(column.get("table_name") or "source table")
        description = f"{display_name} from {table_name} used for business analysis and reporting."
        needs_review = True

    raw_synonyms = merged.get("synonyms") or []
    if isinstance(raw_synonyms, str):
        try:
            parsed_synonyms = json.loads(raw_synonyms)
            raw_synonyms = parsed_synonyms if isinstance(parsed_synonyms, list) else [raw_synonyms]
        except json.JSONDecodeError:
            raw_synonyms = [raw_synonyms]
    synonyms: List[str] = []
    seen_synonyms: Set[str] = set()
    for value in raw_synonyms:
        clean = str(value or "").strip()
        key = clean.casefold()
        if clean and key not in seen_synonyms:
            seen_synonyms.add(key)
            synonyms.append(clean[:100])

    normalized = {
        **merged,
        "semantic_type": semantic,
        "business_description": description,
        "suggested_display_name": display_name,
        "is_measure": semantic == "MEASURE",
        "is_dimension": semantic in {"DIMENSION", "DATE", "FLAG"},
        "is_pii_candidate": pii_candidate,
        "pii_type": pii_type if pii_candidate and pii_type and pii_type != "-" else None,
        "synonyms": synonyms[:10],
        "is_join_key": bool(merged.get("is_primary_key"))
        or bool(merged.get("is_foreign_key"))
        or semantic in {"ID", "SURROGATE_KEY"},
        "needs_llm": False,
        "needs_review": needs_review,
        "confidence": _confidence(merged.get("confidence"), 0.55 if needs_review else 0.95),
        "enrichment_source": source,
    }
    normalized["suggested_aggregation"] = (
        "SUM" if semantic == "MEASURE" else "COUNT" if semantic == "FLAG" else "NONE"
    )
    normalized["aggregation_policy"] = build_aggregation_policy(normalized)
    return normalized


def _fallback_enrichment(column: Dict[str, Any], *, source: str = "RULES_FALLBACK") -> Dict[str, Any]:
    rules = rule_based_semantic_classification(column)
    semantic = str(rules.get("semantic_type") or "UNKNOWN")
    display_name = _business_display_name(str(column.get("column_name") or ""))
    table_name = str(column.get("table_name") or "source table")
    descriptions = {
        "ID": f"Business identifier used to identify or relate {table_name} records.",
        "SURROGATE_KEY": f"System-generated key that uniquely identifies {table_name} records.",
        "FLAG": f"Boolean business indicator for {display_name} in {table_name}.",
        "AUDIT_TIMESTAMP": f"System audit timestamp for lifecycle tracking of {table_name} records.",
    }
    return _normalized_enrichment(
        column,
        {
            **rules,
            "suggested_display_name": display_name,
            "business_description": column.get("business_description")
            or column.get("column_description")
            or descriptions.get(semantic),
            "synonyms": column.get("synonyms") or [],
            "confidence": 0.55 if rules.get("needs_llm") else 0.98,
        },
        source=source,
        needs_review=bool(rules.get("needs_llm")),
    )


def llm_enrich_column(
    column: Dict[str, Any],
    domain_context: Dict[str, Any],
    llm: Any = None,
) -> Dict[str, Any]:
    """Backward-compatible single-column entry point backed by the real LLM."""
    token_accumulator = TokenAccumulator()
    model = llm or get_llm(
        provider=os.getenv("SEMANTIC_LLM_PROVIDER", "azure_openai"),
        request_timeout=_positive_int_env("SEMANTIC_LLM_TIMEOUT_SECONDS", 120),
        max_retries=0,
    )
    result = _enrich_batch(
        [column],
        domain_context,
        model,
        token_accumulator,
        max_retries=_positive_int_env("SEMANTIC_ENRICH_MAX_RETRIES", 2),
        batch_label="single_column",
    )[0]
    return _normalized_enrichment(
        column,
        result,
        source="LLM",
        needs_review=float(result.get("confidence") or 0.0) < 0.75,
    )


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
    rule_result = rule_based_semantic_classification(column)
    if column.get("embedding_version") == "ENRICHED" and (
        column.get("business_description") or column.get("column_description")
    ):
        return _normalized_enrichment(
            column,
            {
                **rule_result,
                "semantic_type": column.get("semantic_type")
                or column.get("prior_semantic_type")
                or rule_result.get("semantic_type"),
                "business_description": column.get("business_description")
                or column.get("column_description"),
                "suggested_display_name": column.get("suggested_display_name")
                or column.get("logical_column_name")
                or column.get("column_name"),
                "synonyms": column.get("synonyms") or [],
                "confidence": column.get("confidence") or 0.95,
            },
            source="CACHE",
            needs_review=False,
        )
    if not rule_result["needs_llm"]:
        return _fallback_enrichment({**column, **rule_result}, source="RULES")
    try:
        return llm_enrich_column({**column, **rule_result}, domain_context)
    except Exception as exc:
        logger.warning(
            "Single-column semantic LLM failed; using deterministic fallback: %s",
            str(exc)[:500],
            extra={"node": "semantic_enrichment"},
        )
        return _fallback_enrichment({**column, **rule_result})


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
    discovered = state.get("discovered_metadata") or {}
    profiling = state.get("column_profiles") or {}
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
        "business_objective": state.get("req_business_objective") or state.get("business_objective"),
        "data_domains": state.get("req_data_domains") or state.get("data_domains"),
        "domain_knowledge_context": kb_result.get("context_text", ""),
    }

    discovered_relationships = discovered.get("table_relationships", []) if isinstance(discovered, dict) else []
    primary_keys = discovered.get("primary_keys", []) if isinstance(discovered, dict) else []
    foreign_keys = discovered.get("foreign_keys", []) if isinstance(discovered, dict) else []
    primary_key_map: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    foreign_key_map: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    loose_primary_keys: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    loose_foreign_keys: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for item in primary_keys:
        primary_key_map[_column_key(item)] = item
        loose_primary_keys.setdefault(
            (
                str(item.get("table_name") or "").casefold(),
                str(item.get("column_name") or "").casefold(),
            ),
            [],
        ).append(item)
    for item in foreign_keys:
        foreign_key_map[
            _column_key(
                {
                    "database_name": item.get("database_name"),
                    "schema_name": item.get("source_schema_name"),
                    "table_name": item.get("source_table_name"),
                    "column_name": item.get("source_column_name"),
                }
            )
        ] = item
        loose_foreign_keys.setdefault(
            (
                str(item.get("source_table_name") or "").casefold(),
                str(item.get("source_column_name") or "").casefold(),
            ),
            [],
        ).append(item)

    profile_rows = profiling.get("column_profiles", []) if isinstance(profiling, dict) else []
    profile_map: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {
        _column_key(profile): profile
        for profile in profile_rows
        if isinstance(profile, dict)
    }
    loose_profiles: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for profile in profile_rows:
        if not isinstance(profile, dict):
            continue
        loose_key = (
            str(profile.get("table_name") or "").casefold(),
            str(profile.get("column_name") or "").casefold(),
        )
        loose_profiles.setdefault(loose_key, []).append(profile)

    merged_columns: List[Dict[str, Any]] = []

    for table in discovered.get("tables", []):
        for col in table.get("columns", []):
            identity = {
                "database_name": table.get("database_name"),
                "schema_name": table.get("schema_name"),
                "table_name": table.get("table_name"),
                "column_name": col.get("column_name"),
            }
            identity_key = _column_key(identity)
            profile = profile_map.get(identity_key, {})
            if not profile:
                compatible = loose_profiles.get((identity_key[2], identity_key[3]), [])
                if len(compatible) == 1:
                    # Backward compatibility for profile artifacts written before qualified identities.
                    profile = compatible[0]
            pk_info = primary_key_map.get(identity_key, {})
            fk_info = foreign_key_map.get(identity_key, {})
            if not pk_info:
                compatible = loose_primary_keys.get((identity_key[2], identity_key[3]), [])
                if len(compatible) == 1:
                    pk_info = compatible[0]
            if not fk_info:
                compatible = loose_foreign_keys.get((identity_key[2], identity_key[3]), [])
                if len(compatible) == 1:
                    fk_info = compatible[0]
            merged_columns.append(
                {
                    **col,
                    **profile,
                    **identity,
                    "is_primary_key": bool(pk_info),
                    "primary_key_constraint_name": pk_info.get("constraint_name"),
                    "is_foreign_key": bool(fk_info),
                    "foreign_key_constraint_name": fk_info.get("constraint_name"),
                    "references_table_name": fk_info.get("referenced_table_name"),
                    "references_column_name": fk_info.get("referenced_column_name"),
                }
            )

    enriched_by_key: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    llm_candidates: List[Dict[str, Any]] = []
    for column in merged_columns:
        rules = rule_based_semantic_classification(column)
        candidate = {**column, **rules}
        if column.get("embedding_version") == "ENRICHED" and (
            column.get("business_description") or column.get("column_description")
        ):
            enriched_by_key[_column_key(column)] = _normalized_enrichment(
                candidate,
                {
                    "semantic_type": column.get("semantic_type")
                    or column.get("prior_semantic_type")
                    or rules.get("semantic_type"),
                    "business_description": column.get("business_description")
                    or column.get("column_description"),
                    "suggested_display_name": column.get("suggested_display_name")
                    or column.get("logical_column_name")
                    or column.get("column_name"),
                    "synonyms": column.get("synonyms") or [],
                    "confidence": column.get("confidence") or 0.95,
                },
                source="CACHE",
                needs_review=False,
            )
        elif rules["needs_llm"]:
            llm_candidates.append(candidate)
        else:
            enriched_by_key[_column_key(column)] = _fallback_enrichment(candidate, source="RULES")

    token_accumulator = TokenAccumulator()
    llm_errors: List[str] = []
    max_retries = _positive_int_env("SEMANTIC_ENRICH_MAX_RETRIES", 2)
    llm_enabled = os.getenv("SEMANTIC_LLM_ENABLED", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    model = None
    if llm_candidates and llm_enabled:
        try:
            model = get_llm(
                provider=os.getenv("SEMANTIC_LLM_PROVIDER", "azure_openai"),
                request_timeout=_positive_int_env("SEMANTIC_LLM_TIMEOUT_SECONDS", 120),
                max_retries=0,
            )
        except Exception as exc:
            llm_errors.append(f"LLM initialization failed: {type(exc).__name__}: {exc}"[:1000])
            logger.error(llm_errors[-1], extra={"run_id": state.get("run_id"), "node": "semantic_enrichment"})

    if model is not None:
        for batch_index, batch in enumerate(_llm_batches(llm_candidates), start=1):
            label = f"batch_{batch_index}"
            try:
                results = _enrich_batch(
                    batch,
                    domain_context,
                    model,
                    token_accumulator,
                    max_retries=max_retries,
                    batch_label=label,
                )
                for column, result in zip(batch, results):
                    confidence = float(result.get("confidence") or 0.0)
                    enriched_by_key[_column_key(column)] = _normalized_enrichment(
                        column,
                        result,
                        source="LLM",
                        needs_review=confidence < 0.75,
                    )
            except Exception as exc:
                message = f"{label}: {type(exc).__name__}: {exc}"[:1000]
                llm_errors.append(message)
                logger.error(
                    "Semantic enrichment %s; using rule fallback for %d columns",
                    message[:500],
                    len(batch),
                    extra={"run_id": state.get("run_id"), "node": "semantic_enrichment"},
                )
                for column in batch:
                    enriched_by_key[_column_key(column)] = _fallback_enrichment(column)
    else:
        if llm_candidates and not llm_enabled:
            llm_errors.append("Semantic LLM disabled by SEMANTIC_LLM_ENABLED")
        for column in llm_candidates:
            enriched_by_key[_column_key(column)] = _fallback_enrichment(column)

    enriched_columns = [enriched_by_key[_column_key(column)] for column in merged_columns]
    enriched_tables = [
        {
            "database_name": table.get("database_name"),
            "schema_name": table.get("schema_name"),
            "table_name": table.get("table_name"),
            "columns": [
                enriched_by_key[_column_key(column)]
                for column in merged_columns
                if _column_key(column)[:3]
                == _column_key(
                    {
                        "database_name": table.get("database_name"),
                        "schema_name": table.get("schema_name"),
                        "table_name": table.get("table_name"),
                    }
                )[:3]
            ],
        }
        for table in discovered.get("tables", [])
    ]

    certified_joins = metadata_backed_joins(discovered_relationships)
    heuristic_joins = discover_joins(enriched_tables, existing_joins=certified_joins)
    joins = certified_joins + heuristic_joins
    semantic_counts: Dict[str, int] = {}
    source_counts: Dict[str, int] = {}
    for column in enriched_columns:
        semantic_type = str(column.get("semantic_type") or "UNKNOWN")
        semantic_counts[semantic_type] = semantic_counts.get(semantic_type, 0) + 1
        source = str(column.get("enrichment_source") or "UNKNOWN")
        source_counts[source] = source_counts.get(source, 0) + 1

    llm_cost_usd = compute_cost_usd(
        token_accumulator.total_input,
        token_accumulator.total_output,
    )
    needs_review_count = sum(1 for column in enriched_columns if column.get("needs_review"))

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
        "enrichment_source_counts": source_counts,
        "quality_summary": {
            "columns_total": len(enriched_columns),
            "columns_needing_review": needs_review_count,
            "llm_batches_failed": len(llm_errors),
            "llm_errors": llm_errors,
        },
        "llm_metrics": {
            "enabled": llm_enabled,
            "provider": os.getenv("SEMANTIC_LLM_PROVIDER", "azure_openai"),
            "input_tokens": token_accumulator.total_input,
            "output_tokens": token_accumulator.total_output,
            "token_count": token_accumulator.total,
            "cost_usd": llm_cost_usd,
            "prompt_version": SEMANTIC_PROMPT_VERSION,
        },
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
        schema_version="SemanticEnrichment_v2",
        prompt_version=SEMANTIC_PROMPT_VERSION,
        faithfulness_status="NOT_APPLICABLE",
        token_count=token_accumulator.total,
        input_tokens=token_accumulator.total_input,
        output_tokens=token_accumulator.total_output,
        fingerprint=state.get("fingerprint"),
    )

    new_state["enriched_metadata"] = payload
    new_state["certified_joins"] = certified_joins
    new_state["join_candidates"] = heuristic_joins
    new_state["table_relationships"] = discovered_relationships
    new_state["semantic_enrichment_status"] = "COMPLETED"
    logger.info(
        "END Semantic Enrichment tables=%d columns=%d certified_joins=%d heuristic_joins=%d "
        "llm_columns=%d review_required=%d tokens=%d kb_enabled=%s",
        len(enriched_tables),
        len(enriched_columns),
        len(certified_joins),
        len(heuristic_joins),
        source_counts.get("LLM", 0),
        needs_review_count,
        token_accumulator.total,
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
