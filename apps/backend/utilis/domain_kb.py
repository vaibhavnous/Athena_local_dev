from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence

from utilis.db import config, execute_source_sql
from utilis.embeddings import get_embedding_model
from utilis.env import load_backend_env
from utilis.logger import logger


load_backend_env()

KB_CONTENT_TABLE = "TABLE_DEFINITION"
KB_CONTENT_FK = "FK_PATTERN"
KB_CONTENT_PII = "PII_PATTERN"
KB_CONTENT_MEASURE = "MEASURE_PATTERN"
KB_CONTENT_GOLD_RULE = "GOLD_RULE"

DEFAULT_KB_INDEX_NAME = "knowledgebase"
DEFAULT_KB_ID = "PC_Insurance_V1"
DEFAULT_DOMAIN_PROFILE = "Insurance"
KB_TARGETS = {
    "PC_Insurance_V1": ("Insurance", "insurancekb", "PINECONE_INSURANCE_KB_INDEX_NAME"),
    "BASEL_DW_V1": ("Basel", "beselkb", "PINECONE_BASEL_KB_INDEX_NAME"),
}

@dataclass(frozen=True)
class DomainKBConfig:
    enabled: bool
    index_name: str
    knowledge_base_id: str
    domain_profile: str
    namespace: str
    top_k_enrichment: int
    top_k_gold: int
    max_chars_enrichment: int
    max_chars_gold: int


def _env_enabled(name: str, default: str = "false") -> bool:
    return str(os.getenv(name, default)).strip().lower() in {"1", "true", "yes", "on"}


def _confidence(value: Any) -> float:
    try:
        return min(1.0, max(0.0, float(value or 0.0)))
    except (TypeError, ValueError):
        return 0.0


def _metadata_json(value: Any, max_chars: int = 4000) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value, separators=(",", ":"), default=str)[:max_chars]
    return str(value or "")[:max_chars]


def get_domain_kb_config(
    *,
    knowledge_base_id: Optional[str] = None,
    domain_profile: Optional[str] = None,
) -> DomainKBConfig:
    explicit_kb = str(knowledge_base_id or "").strip()
    kb_id = explicit_kb or os.getenv("ATHENA_KB_ID", DEFAULT_KB_ID).strip() or DEFAULT_KB_ID
    target = KB_TARGETS.get(kb_id)
    if explicit_kb and target is None:
        raise ValueError(f"Unsupported knowledge_base_id: {kb_id}")
    default_domain, routed_index, index_env = target or (
        DEFAULT_DOMAIN_PROFILE,
        DEFAULT_KB_INDEX_NAME,
        "PINECONE_KNOWLEDGE_BASE_INDEX_NAME",
    )
    index_name = (
        os.getenv(index_env)
        or (routed_index if explicit_kb else None)
        or os.getenv("PINECONE_KNOWLEDGE_BASE_INDEX_NAME")
        or os.getenv("PINECONE_KB_INDEX_NAME")
        or routed_index
    )
    return DomainKBConfig(
        enabled=_env_enabled("ATHENA_USE_DOMAIN_KB"),
        index_name=index_name.strip(),
        knowledge_base_id=kb_id,
        domain_profile=(
            str(domain_profile or "").strip()
            or (default_domain if explicit_kb else os.getenv("ATHENA_DOMAIN_PROFILE", default_domain).strip())
            or default_domain
        ),
        namespace=os.getenv("PINECONE_KNOWLEDGE_BASE_NAMESPACE", kb_id).strip() or kb_id,
        top_k_enrichment=max(1, int(os.getenv("ATHENA_KB_TOP_K_ENRICHMENT", "8"))),
        top_k_gold=max(1, int(os.getenv("ATHENA_KB_TOP_K_GOLD", "10"))),
        max_chars_enrichment=max(500, int(os.getenv("ATHENA_KB_MAX_CHARS_ENRICHMENT", "4000"))),
        max_chars_gold=max(500, int(os.getenv("ATHENA_KB_MAX_CHARS_GOLD", "5000"))),
    )


def _pinecone_index(index_name: str):
    from pinecone import Pinecone

    api_key = os.getenv("PINECONE_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("Missing required environment variable: PINECONE_API_KEY")
    return Pinecone(api_key=api_key).Index(index_name)


def _pinecone_index_description(index_name: str) -> Dict[str, Any]:
    from pinecone import Pinecone

    api_key = os.getenv("PINECONE_API_KEY", "").strip()
    if not api_key:
        return {}
    details = Pinecone(api_key=api_key).describe_index(index_name)
    if hasattr(details, "to_dict"):
        return details.to_dict()
    return details if isinstance(details, dict) else {}


def _index_uses_integrated_embedding(index_name: str) -> bool:
    return bool(_pinecone_index_description(index_name).get("embed"))


def _index_dimension(index_name: str) -> Optional[int]:
    value = _pinecone_index_description(index_name).get("dimension")
    try:
        return max(1, int(value)) if value is not None else None
    except (TypeError, ValueError):
        return None


def _stable_id(*parts: Any) -> str:
    raw = "|".join(str(part or "").strip().lower() for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _normalize_tokens(value: str) -> List[str]:
    return [token for token in "".join(ch if ch.isalnum() else " " for ch in value.lower()).split() if token]


def _is_numeric_type(data_type: str) -> bool:
    return str(data_type or "").lower() in {
        "bigint",
        "decimal",
        "float",
        "int",
        "money",
        "numeric",
        "real",
        "smallint",
        "smallmoney",
        "tinyint",
    }


def _is_measure_column(column_name: str, data_type: str) -> bool:
    name = str(column_name or "").lower()
    if not _is_numeric_type(data_type):
        return False
    if name.endswith(("_id", "_key")) or name in {"id", "key"}:
        return False
    if any(token in name for token in ("date", "time", "year", "month", "day")):
        return False
    return True


def _is_pii_column(column_name: str) -> bool:
    name = str(column_name or "").lower()
    pii_tokens = (
        "address",
        "birth",
        "dob",
        "email",
        "gender",
        "mobile",
        "name",
        "phone",
        "ssn",
        "zip",
    )
    return any(token in name for token in pii_tokens)


def _row_value(row: Any, name: str, default: Any = None) -> Any:
    return getattr(row, name, default)


def _source_database(database_name: Optional[str] = None) -> str:
    return database_name or str(config["azure_sql"].get("source_database") or "insurance")


def _source_schema(schema_name: Optional[str] = None) -> str:
    return schema_name or str(config["azure_sql"].get("source_schema") or "dbo")


def extract_schema_knowledge(database_name: Optional[str] = None, schema_name: Optional[str] = None) -> Dict[str, Any]:
    db = _source_database(database_name)
    schema = _source_schema(schema_name)

    columns = execute_source_sql(
        db,
        """
        SELECT
            TABLE_SCHEMA,
            TABLE_NAME,
            COLUMN_NAME,
            DATA_TYPE,
            ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ?
        ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
        """,
        (schema,),
    )

    foreign_keys = execute_source_sql(
        db,
        """
        SELECT
            sch1.name AS source_schema,
            tab1.name AS source_table,
            col1.name AS source_column,
            sch2.name AS referenced_schema,
            tab2.name AS referenced_table,
            col2.name AS referenced_column
        FROM sys.foreign_key_columns fkc
        INNER JOIN sys.tables tab1 ON tab1.object_id = fkc.parent_object_id
        INNER JOIN sys.schemas sch1 ON tab1.schema_id = sch1.schema_id
        INNER JOIN sys.columns col1 ON col1.column_id = fkc.parent_column_id AND col1.object_id = tab1.object_id
        INNER JOIN sys.tables tab2 ON tab2.object_id = fkc.referenced_object_id
        INNER JOIN sys.schemas sch2 ON tab2.schema_id = sch2.schema_id
        INNER JOIN sys.columns col2 ON col2.column_id = fkc.referenced_column_id AND col2.object_id = tab2.object_id
        WHERE sch1.name = ?
        ORDER BY sch1.name, tab1.name, col1.name
        """,
        (schema,),
    )

    return {
        "database_name": db,
        "schema_name": schema,
        "columns": columns,
        "foreign_keys": foreign_keys,
    }


def create_kb_from_schema(
    *,
    database_name: Optional[str] = None,
    schema_name: Optional[str] = None,
    knowledge_base_id: Optional[str] = None,
    domain_profile: Optional[str] = None,
) -> List[Dict[str, Any]]:
    cfg = get_domain_kb_config()
    kb_id = knowledge_base_id or cfg.knowledge_base_id
    domain = domain_profile or cfg.domain_profile
    schema_data = extract_schema_knowledge(database_name=database_name, schema_name=schema_name)
    db = schema_data["database_name"]
    schema = schema_data["schema_name"]

    tables: Dict[str, List[Dict[str, str]]] = {}
    for row in schema_data["columns"]:
        table_name = str(_row_value(row, "TABLE_NAME", "")).strip()
        column_name = str(_row_value(row, "COLUMN_NAME", "")).strip()
        data_type = str(_row_value(row, "DATA_TYPE", "")).strip()
        if not table_name or not column_name:
            continue
        tables.setdefault(table_name, []).append({"column_name": column_name, "data_type": data_type})

    kb_rows: List[Dict[str, Any]] = []

    for table_name, columns in sorted(tables.items()):
        column_summary = ", ".join(f"{col['column_name']} ({col['data_type']})" for col in columns)
        table_ref = f"{db}.{schema}.{table_name}"
        kb_rows.append(
            {
                "kb_row_id": _stable_id(kb_id, KB_CONTENT_TABLE, table_ref),
                "knowledge_base_id": kb_id,
                "domain_profile": domain,
                "kb_content_type": KB_CONTENT_TABLE,
                "database_name": db,
                "schema_name": schema,
                "table_name": table_name,
                "column_name": "",
                "embedding_text": f"{domain} table {table_ref} contains columns {column_summary}",
                "prompt_context": f"{table_ref} contains columns: {column_summary}.",
                "is_active": True,
            }
        )

        for col in columns:
            column_name = col["column_name"]
            data_type = col["data_type"]
            if _is_measure_column(column_name, data_type):
                kb_rows.append(
                    {
                        "kb_row_id": _stable_id(kb_id, KB_CONTENT_MEASURE, table_ref, column_name),
                        "knowledge_base_id": kb_id,
                        "domain_profile": domain,
                        "kb_content_type": KB_CONTENT_MEASURE,
                        "database_name": db,
                        "schema_name": schema,
                        "table_name": table_name,
                        "column_name": column_name,
                        "embedding_text": f"{domain} measure column {table_ref}.{column_name} type {data_type}",
                        "prompt_context": f"{table_ref}.{column_name} is a numeric measure candidate ({data_type}).",
                        "is_active": True,
                    }
                )
            if _is_pii_column(column_name):
                kb_rows.append(
                    {
                        "kb_row_id": _stable_id(kb_id, KB_CONTENT_PII, table_ref, column_name),
                        "knowledge_base_id": kb_id,
                        "domain_profile": domain,
                        "kb_content_type": KB_CONTENT_PII,
                        "database_name": db,
                        "schema_name": schema,
                        "table_name": table_name,
                        "column_name": column_name,
                        "embedding_text": f"{domain} pii candidate column {table_ref}.{column_name} type {data_type}",
                        "prompt_context": f"{table_ref}.{column_name} is a PII candidate and should be handled carefully.",
                        "is_active": True,
                    }
                )

    for row in schema_data["foreign_keys"]:
        source_table = str(_row_value(row, "source_table", "")).strip()
        source_column = str(_row_value(row, "source_column", "")).strip()
        referenced_table = str(_row_value(row, "referenced_table", "")).strip()
        referenced_column = str(_row_value(row, "referenced_column", "")).strip()
        source_schema = str(_row_value(row, "source_schema", schema)).strip() or schema
        referenced_schema = str(_row_value(row, "referenced_schema", schema)).strip() or schema
        if not source_table or not source_column or not referenced_table or not referenced_column:
            continue
        source_ref = f"{db}.{source_schema}.{source_table}.{source_column}"
        referenced_ref = f"{db}.{referenced_schema}.{referenced_table}.{referenced_column}"
        kb_rows.append(
            {
                "kb_row_id": _stable_id(kb_id, KB_CONTENT_FK, source_ref, referenced_ref),
                "knowledge_base_id": kb_id,
                "domain_profile": domain,
                "kb_content_type": KB_CONTENT_FK,
                "database_name": db,
                "schema_name": source_schema,
                "table_name": source_table,
                "column_name": source_column,
                "embedding_text": f"{domain} foreign key relationship {source_ref} references {referenced_ref}",
                "prompt_context": f"Join pattern: {source_ref} -> {referenced_ref}.",
                "is_active": True,
            }
        )

    return kb_rows


def compute_kb_fingerprint(kb_rows: Iterable[Dict[str, Any]], knowledge_base_id: Optional[str] = None) -> str:
    kb_id = knowledge_base_id or get_domain_kb_config().knowledge_base_id
    ids = sorted(
        str(row.get("kb_row_id"))
        for row in kb_rows
        if row.get("knowledge_base_id") == kb_id and row.get("is_active", True)
    )
    return hashlib.sha256(",".join(ids).encode("utf-8")).hexdigest()


def upsert_kb_rows_to_pinecone(
    kb_rows: Sequence[Dict[str, Any]],
    *,
    index_name: Optional[str] = None,
    namespace: Optional[str] = None,
    knowledge_base_id: Optional[str] = None,
    domain_profile: Optional[str] = None,
    refresh: bool = True,
) -> Dict[str, Any]:
    row_kb_ids = {str(row.get("knowledge_base_id") or "").strip() for row in kb_rows}
    row_kb_ids.discard("")
    if len(row_kb_ids) > 1:
        raise ValueError("A Pinecone upsert batch must contain exactly one knowledge_base_id")
    kb_id = str(knowledge_base_id or next(iter(row_kb_ids), "")).strip() or None
    cfg = get_domain_kb_config(
        knowledge_base_id=kb_id,
        domain_profile=domain_profile,
    )
    target_index_name = index_name or cfg.index_name
    target_namespace = namespace or cfg.namespace

    active_rows = [row for row in kb_rows if row.get("is_active", True)]
    if not active_rows:
        return {
            "rows_upserted": 0,
            "kb_hash": compute_kb_fingerprint(kb_rows, cfg.knowledge_base_id),
            "index_name": target_index_name,
            "namespace": target_namespace,
            "knowledge_base_id": cfg.knowledge_base_id,
        }

    index = _pinecone_index(target_index_name)
    uses_integrated_embedding = _index_uses_integrated_embedding(target_index_name)
    if refresh:
        try:
            index.delete(filter={"knowledge_base_id": {"$eq": cfg.knowledge_base_id}}, namespace=target_namespace)
        except Exception as exc:
            logger.warning("Domain KB refresh delete skipped: %s", exc)

    if uses_integrated_embedding:
        pinecone_records = []
        for row in active_rows:
            pinecone_records.append(
                {
                    "_id": str(row["kb_row_id"]),
                    "text": str(row["embedding_text"]),
                    "knowledge_base_id": str(row["knowledge_base_id"]),
                    "domain_profile": str(row["domain_profile"]),
                    "kb_content_type": str(row["kb_content_type"]),
                    "database_name": str(row.get("database_name") or ""),
                    "schema_name": str(row.get("schema_name") or ""),
                    "table_name": str(row.get("table_name") or ""),
                    "column_name": str(row.get("column_name") or ""),
                    "embedding_text": str(row["embedding_text"])[:2000],
                    "prompt_context": str(row["prompt_context"])[:2000],
                    "is_active": bool(row.get("is_active", True)),
                    "kpi_name": str(row.get("kpi_name") or "")[:200],
                    "gold_rule_json": _metadata_json(row.get("gold_rule_json")),
                    "rule_type": str(row.get("rule_type") or "")[:100],
                    "rule_value": str(row.get("rule_value") or "")[:1000],
                    "confidence": _confidence(row.get("confidence")),
                }
            )
        for offset in range(0, len(pinecone_records), 96):
            index.upsert_records(records=pinecone_records[offset:offset + 96], namespace=target_namespace)
        return {
            "rows_upserted": len(pinecone_records),
            "kb_hash": compute_kb_fingerprint(active_rows, cfg.knowledge_base_id),
            "index_name": target_index_name,
            "namespace": target_namespace,
            "knowledge_base_id": cfg.knowledge_base_id,
            "integrated_embedding": True,
        }

    model = get_embedding_model(
        log_context={"node": "domain_kb", "stage": "index"},
        dimensions=_index_dimension(target_index_name),
    )
    if model is None:
        raise RuntimeError("Domain KB embedding model is unavailable")

    texts = [str(row["embedding_text"]) for row in active_rows]
    vectors = model.embed_documents(texts)
    pinecone_vectors = []
    for row, vector in zip(active_rows, vectors):
        metadata = {
            "knowledge_base_id": str(row["knowledge_base_id"]),
            "domain_profile": str(row["domain_profile"]),
            "kb_content_type": str(row["kb_content_type"]),
            "database_name": str(row.get("database_name") or ""),
            "schema_name": str(row.get("schema_name") or ""),
            "table_name": str(row.get("table_name") or ""),
            "column_name": str(row.get("column_name") or ""),
            "embedding_text": str(row["embedding_text"])[:2000],
            "prompt_context": str(row["prompt_context"])[:2000],
            "is_active": bool(row.get("is_active", True)),
            "kpi_name": str(row.get("kpi_name") or "")[:200],
            "gold_rule_json": _metadata_json(row.get("gold_rule_json")),
            "rule_type": str(row.get("rule_type") or "")[:100],
            "rule_value": str(row.get("rule_value") or "")[:1000],
            "confidence": _confidence(row.get("confidence")),
        }
        pinecone_vectors.append({"id": str(row["kb_row_id"]), "values": vector, "metadata": metadata})

    for offset in range(0, len(pinecone_vectors), 100):
        index.upsert(vectors=pinecone_vectors[offset:offset + 100], namespace=target_namespace)
    return {
        "rows_upserted": len(pinecone_vectors),
        "kb_hash": compute_kb_fingerprint(active_rows, cfg.knowledge_base_id),
        "index_name": target_index_name,
        "namespace": target_namespace,
        "knowledge_base_id": cfg.knowledge_base_id,
        "integrated_embedding": False,
    }


def build_and_upsert_client_db_kb(
    *,
    database_name: Optional[str] = None,
    schema_name: Optional[str] = None,
    knowledge_base_id: Optional[str] = None,
    domain_profile: Optional[str] = None,
    index_name: Optional[str] = None,
    namespace: Optional[str] = None,
    refresh: bool = True,
) -> Dict[str, Any]:
    cfg = get_domain_kb_config(
        knowledge_base_id=knowledge_base_id,
        domain_profile=domain_profile,
    )
    kb_rows = create_kb_from_schema(
        database_name=database_name,
        schema_name=schema_name,
        knowledge_base_id=cfg.knowledge_base_id,
        domain_profile=cfg.domain_profile,
    )
    result = upsert_kb_rows_to_pinecone(
        kb_rows,
        index_name=index_name or cfg.index_name,
        namespace=namespace or cfg.namespace,
        knowledge_base_id=cfg.knowledge_base_id,
        domain_profile=cfg.domain_profile,
        refresh=refresh,
    )
    result["rows_generated"] = len(kb_rows)
    return result


def load_domain_kb(
    *,
    query_text: str,
    top_k: int,
    max_chars: int,
    content_types: Optional[Sequence[str]] = None,
    knowledge_base_id: Optional[str] = None,
    domain_profile: Optional[str] = None,
) -> Dict[str, Any]:
    cfg = get_domain_kb_config(
        knowledge_base_id=knowledge_base_id,
        domain_profile=domain_profile,
    )
    kb_id = cfg.knowledge_base_id
    if not cfg.enabled:
        return {
            "context_text": "",
            "rows_retrieved": 0,
            "chars_injected": 0,
            "knowledge_base_id": kb_id,
            "index_name": cfg.index_name,
        }

    filter_payload: Dict[str, Any] = {
        "knowledge_base_id": {"$eq": kb_id},
        "is_active": {"$eq": True},
    }
    if content_types:
        filter_payload["kb_content_type"] = {"$in": list(content_types)}

    try:
        index = _pinecone_index(cfg.index_name)
        if _index_uses_integrated_embedding(cfg.index_name):
            response = index.search(
                namespace=cfg.namespace,
                top_k=max(1, int(top_k)),
                inputs={"text": str(query_text or "domain knowledge")},
                filter=filter_payload,
                fields=[
                    "prompt_context",
                    "knowledge_base_id",
                    "kb_content_type",
                    "table_name",
                    "column_name",
                    "kpi_name",
                    "gold_rule_json",
                    "rule_type",
                    "rule_value",
                    "confidence",
                ],
            )
            result = getattr(response, "result", None)
            if result is None and isinstance(response, dict):
                result = response.get("result", {})
            matches = getattr(result, "hits", None)
            if matches is None and isinstance(result, dict):
                matches = result.get("hits", [])
        else:
            model = get_embedding_model(
                log_context={"node": "domain_kb", "stage": "query"},
                dimensions=_index_dimension(cfg.index_name),
            )
            if model is None:
                return {
                    "context_text": "",
                    "rows_retrieved": 0,
                    "chars_injected": 0,
                    "knowledge_base_id": kb_id,
                    "index_name": cfg.index_name,
                }
            vector = model.embed_query(str(query_text or "domain knowledge"))
            response = index.query(
                vector=vector,
                top_k=max(1, int(top_k)),
                namespace=cfg.namespace,
                include_metadata=True,
                filter=filter_payload,
            )
            matches = getattr(response, "matches", None)
            if matches is None and isinstance(response, dict):
                matches = response.get("matches", [])
        matches = matches or []
        contexts: List[str] = []
        rows = []
        for match in matches:
            metadata = getattr(match, "metadata", None) or match.get("metadata", {}) or {}
            if not metadata:
                fields = getattr(match, "fields", None) or match.get("fields", {}) or {}
                metadata = fields
            prompt_context = str(metadata.get("prompt_context") or "").strip()
            if prompt_context:
                contexts.append(prompt_context)
                rows.append(metadata)

        context_text = "\n".join(contexts)[: max(0, int(max_chars))]
        return {
            "context_text": context_text,
            "rows_retrieved": len(rows),
            "chars_injected": len(context_text),
            "knowledge_base_id": kb_id,
            "index_name": cfg.index_name,
            "content_types": list(content_types) if content_types else None,
            "rows": rows,
        }
    except Exception as exc:
        logger.warning("Domain KB retrieval skipped: %s", exc)
        return {
            "context_text": "",
            "rows_retrieved": 0,
            "chars_injected": 0,
            "knowledge_base_id": kb_id,
            "index_name": cfg.index_name,
            "error": str(exc),
        }


def keyword_rank_kb_rows(
    kb_rows: Sequence[Dict[str, Any]],
    *,
    query_text: str,
    top_k: int,
    max_chars: int,
    content_types: Optional[Sequence[str]] = None,
    knowledge_base_id: Optional[str] = None,
) -> Dict[str, Any]:
    kb_id = knowledge_base_id or get_domain_kb_config().knowledge_base_id
    query_tokens = set(_normalize_tokens(query_text))
    allowed_types = set(content_types or [])

    rows = [
        row
        for row in kb_rows
        if row.get("knowledge_base_id") == kb_id
        and row.get("is_active", True)
        and (not allowed_types or row.get("kb_content_type") in allowed_types)
    ]
    rows.sort(
        key=lambda row: sum(1 for token in query_tokens if token in str(row.get("embedding_text", "")).lower()),
        reverse=True,
    )
    selected = rows[: max(1, int(top_k))]
    context_text = "\n".join(str(row.get("prompt_context") or "") for row in selected)[: max(0, int(max_chars))]
    return {"context_text": context_text, "rows_retrieved": len(selected), "chars_injected": len(context_text)}
