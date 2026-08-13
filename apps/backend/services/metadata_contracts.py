from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional


METADATA_TABLES = (
    "cfg_source_system",
    "cfg_connection",
    "cfg_ingestion_object",
    "cfg_mapping",
    "ctl_ingestion_queue",
    "ctl_run",
    "ctl_error_log",
    "ctl_watermark",
)
SUPPORTED_METADATA_TARGETS = {"databricks", "snowflake"}
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
_BACKEND_ROOT = Path(__file__).resolve().parents[1]
# ponytail: deployment is flat under the backend root; the second path preserves monorepo development.
_DDL_ROOT = next(
    (
        path
        for path in (
            _BACKEND_ROOT / "prereq" / "metadata",
            _BACKEND_ROOT.parents[1] / "prereq" / "metadata",
        )
        if path.is_dir()
    ),
    _BACKEND_ROOT / "prereq" / "metadata",
)
CANONICAL_COLUMN_NAME_CORRECTIONS = {
    "agen_t_category_name": "agent_category_name",
    "claimid": "claim_id",
    "garageid": "garage_id",
    "garagetypeid": "garage_type_id",
    "hospitalid": "hospital_id",
    "paidamount": "paid_amount",
    "paiddate": "paid_date",
    "paymentid": "payment_id",
    "rererence_id": "reference_id",
    "servicetax": "service_tax",
    "updatenum": "update_num",
}


@dataclass(frozen=True)
class TargetMetadataContext:
    platform: str
    environment: str
    namespace: str
    schema: str = "metadata"

    def __post_init__(self) -> None:
        platform = str(self.platform or "").strip().lower()
        if platform not in SUPPORTED_METADATA_TARGETS:
            raise ValueError(f"Unsupported metadata target: {self.platform!r}")
        for label, value in (
            ("environment", self.environment),
            ("namespace", self.namespace),
            ("schema", self.schema),
        ):
            validate_identifier(value, label=label)
        allowed_schemas = {"metadata", "metadata_schema"} if platform == "databricks" else {"metadata"}
        if str(self.schema).strip().lower() not in allowed_schemas:
            raise ValueError(f"Unsupported {platform} metadata schema: {self.schema!r}")
        object.__setattr__(self, "platform", platform)


def validate_identifier(value: Any, *, label: str = "identifier") -> str:
    cleaned = str(value or "").strip()
    if not _IDENTIFIER.fullmatch(cleaned):
        raise ValueError(f"Invalid {label}: {value!r}")
    return cleaned


def stable_bigint(namespace: str, *identity_parts: Any) -> int:
    """Return a stable positive 63-bit ID; repositories still check collisions."""
    identity = "\x1f".join(str(part or "").strip().casefold() for part in identity_parts)
    if not identity.replace("\x1f", ""):
        raise ValueError("A stable ID requires a non-empty logical identity.")
    digest = hashlib.sha256(f"{namespace}\x1e{identity}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") & ((1 << 63) - 1) or 1


def canonical_json_hash(payload: Mapping[str, Any]) -> str:
    normalized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return "sha256:" + hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def normalize_bronze_column_name(value: Any) -> str:
    normalized = re.sub(r"[^a-z0-9_]+", "_", str(value or "").strip().lower()).strip("_")
    if normalized and normalized[0].isdigit():
        normalized = "col_" + normalized
    return CANONICAL_COLUMN_NAME_CORRECTIONS.get(normalized, normalized)


def bronze_target_data_type(platform: str, column: Mapping[str, Any]) -> str:
    """Resolve the physical Bronze type once so metadata and code generation agree."""
    target = str(platform or "").strip().lower()
    data_type = str(column.get("data_type") or "").strip().lower()
    precision = column.get("numeric_precision")
    scale = column.get("numeric_scale")
    max_length = column.get("character_maximum_length") or column.get("max_length")

    if target == "snowflake":
        if data_type in {"int", "integer", "smallint", "tinyint", "bigint"}:
            return "NUMBER(38,0)"
        if data_type in {"bit", "boolean"}:
            return "BOOLEAN"
        if data_type in {"float", "real", "double"}:
            return "FLOAT"
        if data_type in {"decimal", "numeric", "number", "money", "smallmoney"}:
            if precision and scale is not None:
                return f"NUMBER({min(int(precision), 38)},{int(scale)})"
            return "NUMBER(38,10)"
        if data_type == "date":
            return "DATE"
        if data_type in {"datetime", "datetime2", "smalldatetime", "datetimeoffset", "time", "timestamp"}:
            return "TIMESTAMP_NTZ"
        if data_type in {"binary", "varbinary"}:
            return "BINARY"
        if data_type in {"varchar", "nvarchar", "char", "nchar", "text", "ntext", "string"}:
            try:
                length = int(max_length)
                if 0 < length <= 16777216:
                    return f"VARCHAR({length})"
            except (TypeError, ValueError):
                pass
        return "VARCHAR"

    if target != "databricks":
        raise ValueError(f"Unsupported Bronze target platform: {platform!r}")
    if data_type in {"int", "integer", "smallint", "tinyint"}:
        return "int"
    if data_type == "bigint":
        return "bigint"
    if data_type in {"bit", "boolean"}:
        return "boolean"
    if data_type in {"float", "real", "double"}:
        return "double"
    if data_type in {"decimal", "numeric", "money", "smallmoney"}:
        if precision and scale is not None:
            return f"decimal({min(int(precision), 38)},{int(scale)})"
        return "decimal(38,10)"
    if data_type == "date":
        return "date"
    if data_type in {"datetime", "datetime2", "smalldatetime", "datetimeoffset", "time", "timestamp"}:
        return "timestamp"
    if data_type in {"binary", "varbinary"}:
        return "binary"
    return "string"


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def validate_execution_spec(value: Any, *, platform: str) -> Dict[str, Any]:
    spec = parse_json_object(value, field_name="execution_spec_json", required=True)
    normalized_platform = str(platform or "").strip().upper()
    required = {
        "contract_version",
        "execution_mode",
        "target_platform",
        "engine",
        "artifact_uri",
        "entry_point",
        "artifact_hash",
        "generator_version",
        "mapping_version",
    }
    missing = sorted(required - set(spec))
    if missing:
        raise ValueError("execution_spec_json is missing: " + ", ".join(missing))
    if spec["contract_version"] != "1.0" or spec["execution_mode"] != "GENERATED_ARTIFACT":
        raise ValueError("Only the GENERATED_ARTIFACT/1.0 execution contract is supported.")
    if str(spec["target_platform"]).upper() != normalized_platform:
        raise ValueError("execution_spec_json target_platform does not match the selected target.")
    allowed_engines = {
        "DATABRICKS": {"DATABRICKS_JOB"},
        "SNOWFLAKE": {"SNOWFLAKE_SQL", "SNOWFLAKE_DBT"},
    }
    if str(spec["engine"]).upper() not in allowed_engines.get(normalized_platform, set()):
        raise ValueError("execution_spec_json engine is not allowed for the selected target.")
    if not str(spec["artifact_uri"]).startswith("generated-code://"):
        raise ValueError("execution_spec_json artifact_uri is not allow-listed.")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", str(spec["artifact_hash"])):
        raise ValueError("execution_spec_json artifact_hash must be a SHA-256 value.")
    validate_identifier(spec["entry_point"], label="execution entry point")
    try:
        spec["mapping_version"] = int(spec["mapping_version"])
    except (TypeError, ValueError) as exc:
        raise ValueError("execution_spec_json mapping_version must be an integer.") from exc
    if spec["mapping_version"] < 1:
        raise ValueError("execution_spec_json mapping_version must be positive.")
    for resource_name in ("source_resource", "landing_resource"):
        resource = spec.get(resource_name)
        if resource is None:
            continue
        if not isinstance(resource, dict) or set(resource) != {"database", "schema", "table"}:
            raise ValueError(
                f"execution_spec_json {resource_name} must contain database, schema, and table."
            )
        spec[resource_name] = {
            key: validate_identifier(resource.get(key), label=f"{resource_name} {key}")
            for key in ("database", "schema", "table")
        }
    return spec


def validate_runtime_context(value: Any) -> Dict[str, Any]:
    context = parse_json_object(value, field_name="runtime_context", required=True)
    required = {
        "contract_version",
        "logical_work_id",
        "queue_id",
        "ingestion_object_id",
        "processing_stage",
        "load_type",
        "target_table",
        "config_version",
        "mapping_version",
    }
    missing = sorted(required - set(context))
    if missing:
        raise ValueError("runtime_context is missing: " + ", ".join(missing))
    if context["contract_version"] != "1.0":
        raise ValueError("Only runtime_context contract version 1.0 is supported.")
    if str(context["load_type"] or "").upper() != "FULL":
        raise ValueError("The database-source runtime currently supports FULL loads only.")
    stage = str(context["processing_stage"] or "").upper()
    if stage not in {"SOURCE_TO_BRONZE", "BRONZE_TO_SILVER", "SILVER_TO_GOLD"}:
        raise ValueError("runtime_context processing_stage is unsupported.")
    for field in ("logical_work_id", "target_table"):
        context[field] = str(context[field] or "").strip()
        if not context[field]:
            raise ValueError(f"runtime_context {field} is required.")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9:._-]{0,127}", context["logical_work_id"]):
        raise ValueError("runtime_context logical_work_id has an unsafe format.")
    for field in ("queue_id", "ingestion_object_id", "config_version", "mapping_version"):
        try:
            context[field] = int(context[field])
        except (TypeError, ValueError) as exc:
            raise ValueError(f"runtime_context {field} must be an integer.") from exc
        if context[field] < 1:
            raise ValueError(f"runtime_context {field} must be positive.")
    try:
        context["attempt_number"] = int(context.get("attempt_number") or 0)
    except (TypeError, ValueError) as exc:
        raise ValueError("runtime_context attempt_number must be an integer.") from exc
    if context["attempt_number"] < 0:
        raise ValueError("runtime_context attempt_number cannot be negative.")
    context["processing_stage"] = stage
    context["load_type"] = "FULL"
    context["runtime_run_id"] = str(context.get("runtime_run_id") or "").strip() or None
    return context


def validate_execution_result(value: Any, *, runtime_context: Mapping[str, Any]) -> Dict[str, Any]:
    result = parse_json_object(value, field_name="execution_result", required=True)
    required = {
        "contract_version",
        "status",
        "logical_work_id",
        "runtime_run_id",
        "target_table",
        "target_commit_id",
        "validation_status",
    }
    missing = sorted(required - set(result))
    if missing:
        raise ValueError("execution_result is missing: " + ", ".join(missing))
    if result["contract_version"] != "1.0" or str(result["status"] or "").upper() != "COMPLETED":
        raise ValueError("execution_result must be a COMPLETED version 1.0 contract.")
    if str(result["logical_work_id"] or "") != str(runtime_context.get("logical_work_id") or ""):
        raise ValueError("execution_result logical_work_id does not match the queued work.")
    if str(result["runtime_run_id"] or "") != str(runtime_context.get("runtime_run_id") or ""):
        raise ValueError("execution_result runtime_run_id does not match the current attempt.")
    if str(result["target_table"] or "").casefold() != str(runtime_context.get("target_table") or "").casefold():
        raise ValueError("execution_result target_table does not match the queued target.")
    if str(result["validation_status"] or "").upper() != "PASSED":
        raise ValueError("execution_result blocking validation did not pass.")
    result["target_commit_id"] = str(result["target_commit_id"] or "").strip()
    if not result["target_commit_id"]:
        raise ValueError("execution_result target_commit_id is required.")
    for field in ("rows_read", "rows_written"):
        if field not in result or result[field] is None:
            continue
        try:
            result[field] = int(result[field])
        except (TypeError, ValueError) as exc:
            raise ValueError(f"execution_result {field} must be an integer.") from exc
        if result[field] < 0:
            raise ValueError(f"execution_result {field} cannot be negative.")
    result["status"] = "COMPLETED"
    result["validation_status"] = "PASSED"
    return result


def parse_json_object(value: Any, *, field_name: str, required: bool = False) -> Dict[str, Any]:
    if value in (None, ""):
        if required:
            raise ValueError(f"{field_name} is required.")
        return {}
    if isinstance(value, Mapping):
        result = dict(value)
    else:
        try:
            result = json.loads(str(value))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{field_name} must contain valid JSON.") from exc
    if not isinstance(result, dict):
        raise ValueError(f"{field_name} must be a JSON object.")
    return result


def validate_secret_references(value: Any, *, required: bool = True) -> Dict[str, Dict[str, str]]:
    payload = parse_json_object(value, field_name="secrets_json", required=required)
    forbidden = {"value", "secret", "password", "token", "private_key", "client_secret"}
    validated: Dict[str, Dict[str, str]] = {}
    for logical_name, reference in payload.items():
        if not isinstance(reference, Mapping):
            raise ValueError(f"Secret reference {logical_name!r} must be an object.")
        lowered = {str(key).casefold() for key in reference}
        if lowered & forbidden:
            raise ValueError(f"Secret reference {logical_name!r} contains a secret value field.")
        if lowered - {"scope", "key"}:
            raise ValueError(f"Secret reference {logical_name!r} contains unsupported fields.")
        scope = str(reference.get("scope") or "").strip()
        key = str(reference.get("key") or "").strip()
        if not scope or not key:
            raise ValueError(f"Secret reference {logical_name!r} requires scope and key.")
        validated[str(logical_name)] = {"scope": scope, "key": key}
    if required and not validated:
        raise ValueError("secrets_json requires at least one secret reference.")
    return validated


def _reject_secret_values(value: Any, *, path: str = "config_json") -> None:
    forbidden = {"password", "passwd", "secret", "token", "private_key", "client_secret", "authorization"}
    if isinstance(value, Mapping):
        for key, child in value.items():
            if str(key).casefold() in forbidden:
                raise ValueError(f"{path} contains forbidden credential field {key!r}.")
            _reject_secret_values(child, path=f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_secret_values(child, path=f"{path}[{index}]")


def _validate_jdbc_config(value: Any) -> Dict[str, Any]:
    config = parse_json_object(value, field_name="config_json")
    _reject_secret_values(config)
    allowed = {
        "jdbc_driver",
        "jdbc_url_template",
        "fetch_size",
        "query_timeout_seconds",
        "partition_column",
        "lower_bound",
        "upper_bound",
        "num_partitions",
        "ssl_mode",
        "encrypt",
        "trust_server_certificate",
        "allowed_project_ids",
    }
    unknown = set(config) - allowed
    if unknown:
        raise ValueError("Unsupported JDBC config_json fields: " + ", ".join(sorted(unknown)))
    for name in ("fetch_size", "query_timeout_seconds", "num_partitions"):
        if name in config:
            try:
                config[name] = int(config[name])
            except (TypeError, ValueError) as exc:
                raise ValueError(f"config_json.{name} must be an integer.") from exc
            if config[name] <= 0:
                raise ValueError(f"config_json.{name} must be positive.")
    for name in ("jdbc_driver", "jdbc_url_template", "partition_column", "ssl_mode"):
        if name in config and not isinstance(config[name], str):
            raise ValueError(f"config_json.{name} must be a string.")
    url_template = str(config.get("jdbc_url_template") or "")
    if re.search(r"(?i)(?:password|pwd|user|token|secret|authorization)\s*=|://[^/@:]+:[^/@]+@", url_template):
        raise ValueError("config_json.jdbc_url_template must not contain credentials or credential placeholders.")
    allowed_projects = config.get("allowed_project_ids")
    if not isinstance(allowed_projects, list) or not allowed_projects or any(
        not isinstance(project_id, str) or not project_id.strip() for project_id in allowed_projects
    ):
        raise ValueError("config_json.allowed_project_ids must be a non-empty string array; use '*' explicitly for shared access.")
    config["allowed_project_ids"] = sorted({project_id.strip() for project_id in allowed_projects})
    return config


def validate_jdbc_connection(payload: Mapping[str, Any]) -> Dict[str, Any]:
    normalized = dict(payload)
    required = ("source_system_id", "connection_name", "host_name", "port", "database_name", "auth_type")
    missing = [name for name in required if normalized.get(name) in (None, "")]
    if missing:
        raise ValueError("Missing JDBC connection fields: " + ", ".join(missing))
    if str(normalized.get("connection_type") or "JDBC").upper() != "JDBC":
        raise ValueError("Database-first onboarding requires connection_type=JDBC.")
    normalized["connection_type"] = "JDBC"
    try:
        normalized["port"] = int(normalized["port"])
    except (TypeError, ValueError) as exc:
        raise ValueError("port must be an integer.") from exc
    if not 1 <= normalized["port"] <= 65535:
        raise ValueError("port must be between 1 and 65535.")
    normalized["connection_contract_name"] = str(
        normalized.get("connection_contract_name") or "JDBC_CONNECTION"
    )
    normalized["connection_schema_version"] = str(normalized.get("connection_schema_version") or "1.0")
    if normalized["connection_contract_name"] != "JDBC_CONNECTION" or normalized["connection_schema_version"] != "1.0":
        raise ValueError("Only the JDBC_CONNECTION/1.0 contract is supported by the database-first connector.")
    auth_type = str(normalized.get("auth_type") or "").strip().upper()
    auth_contracts = {
        "BASIC": {"username", "password"},
        "TOKEN": {"token"},
        "OAUTH": {"client_id", "client_secret"},
        "SERVICE_PRINCIPAL": {"client_id", "client_secret", "tenant_id"},
        "KEY": {"username", "private_key"},
        "MANAGED_IDENTITY": set(),
    }
    if auth_type not in auth_contracts:
        raise ValueError(f"Unsupported JDBC auth_type: {auth_type!r}")
    references = validate_secret_references(
        normalized.get("secrets_json"), required=bool(auth_contracts[auth_type])
    )
    missing_refs = auth_contracts[auth_type] - set(references)
    if missing_refs:
        raise ValueError(f"{auth_type} requires secret references: {', '.join(sorted(missing_refs))}")
    config = _validate_jdbc_config(normalized.get("config_json"))
    normalized["auth_type"] = auth_type
    normalized["secret_scope"] = None
    normalized["secret_key"] = None
    normalized["secrets_json"] = json.dumps(references, sort_keys=True)
    normalized["config_json"] = json.dumps(config, sort_keys=True)
    executable = {
        key: normalized.get(key)
        for key in (
            "source_system_id",
            "connection_name",
            "connection_type",
            "connection_contract_name",
            "connection_schema_version",
            "host_name",
            "port",
            "database_name",
            "auth_type",
            "secrets_json",
            "config_json",
        )
    }
    normalized["config_hash"] = canonical_json_hash(executable)
    return normalized


def validate_adls_connection(payload: Mapping[str, Any]) -> Dict[str, Any]:
    normalized = dict(payload)
    required = ("source_system_id", "connection_name", "base_url", "base_path", "auth_type")
    missing = [name for name in required if normalized.get(name) in (None, "")]
    if missing:
        raise ValueError("Missing ADLS connection fields: " + ", ".join(missing))
    if str(normalized.get("connection_type") or "").upper() != "ADLS":
        raise ValueError("File-source onboarding requires connection_type=ADLS.")
    normalized["connection_type"] = "ADLS"
    normalized["connection_contract_name"] = str(
        normalized.get("connection_contract_name") or "ADLS_CONNECTION"
    )
    normalized["connection_schema_version"] = str(
        normalized.get("connection_schema_version") or "1.0"
    )
    if (
        normalized["connection_contract_name"] != "ADLS_CONNECTION"
        or normalized["connection_schema_version"] != "1.0"
    ):
        raise ValueError("Only the ADLS_CONNECTION/1.0 contract is supported.")
    base_url = str(normalized["base_url"]).strip().rstrip("/")
    base_path = str(normalized["base_path"]).strip().rstrip("/") + "/"
    if not re.fullmatch(r"https://[a-z0-9-]+\.dfs\.core\.windows\.net", base_url):
        raise ValueError("ADLS base_url must use the Azure Data Lake dfs endpoint.")
    if not re.fullmatch(
        r"abfss://[a-z0-9-]+@[a-z0-9-]+\.dfs\.core\.windows\.net/.+/",
        base_path,
    ):
        raise ValueError("ADLS base_path must be a canonical abfss directory URI.")
    auth_type = str(normalized.get("auth_type") or "").strip().upper()
    required_references = {
        "SERVICE_PRINCIPAL": {"tenant_id", "client_id", "client_secret"},
        "MANAGED_IDENTITY": set(),
    }
    if auth_type not in required_references:
        raise ValueError(f"Unsupported ADLS auth_type: {auth_type!r}")
    references = validate_secret_references(
        normalized.get("secrets_json"), required=bool(required_references[auth_type])
    )
    missing_refs = required_references[auth_type] - set(references)
    if missing_refs:
        raise ValueError(f"{auth_type} requires secret references: {', '.join(sorted(missing_refs))}")
    config = parse_json_object(normalized.get("config_json"), field_name="config_json", required=True)
    _reject_secret_values(config)
    allowed = {"allowed_extensions", "allowed_project_ids", "source_root", "target_platform"}
    unknown = set(config) - allowed
    if unknown:
        raise ValueError("Unsupported ADLS config_json fields: " + ", ".join(sorted(unknown)))
    extensions = config.get("allowed_extensions")
    if not isinstance(extensions, list) or not extensions:
        raise ValueError("config_json.allowed_extensions must be a non-empty string array.")
    normalized_extensions = sorted({str(item).strip().lower().lstrip(".") for item in extensions})
    if not set(normalized_extensions) <= {"csv", "json", "xml"}:
        raise ValueError("ADLS allowed_extensions may contain only csv, json, and xml.")
    allowed_projects = config.get("allowed_project_ids")
    if not isinstance(allowed_projects, list) or not allowed_projects or any(
        not isinstance(project_id, str) or not project_id.strip() for project_id in allowed_projects
    ):
        raise ValueError("config_json.allowed_project_ids must be a non-empty string array.")
    config["allowed_extensions"] = normalized_extensions
    config["allowed_project_ids"] = sorted({item.strip() for item in allowed_projects})
    normalized.update(
        {
            "base_url": base_url,
            "base_path": base_path,
            "auth_type": auth_type,
            "host_name": normalized.get("host_name") or base_url.split("://", 1)[-1],
            "port": int(normalized.get("port") or 443),
            "secrets_json": json.dumps(references, sort_keys=True),
            "config_json": json.dumps(config, sort_keys=True),
        }
    )
    executable = {
        key: normalized.get(key)
        for key in (
            "source_system_id",
            "connection_name",
            "connection_type",
            "connection_contract_name",
            "connection_schema_version",
            "base_url",
            "base_path",
            "auth_type",
            "secrets_json",
            "config_json",
        )
    }
    normalized["config_hash"] = canonical_json_hash(executable)
    return normalized


def validate_connection(payload: Mapping[str, Any]) -> Dict[str, Any]:
    if str(payload.get("connection_type") or "JDBC").upper() == "ADLS":
        return validate_adls_connection(payload)
    return validate_jdbc_connection(payload)


def ddl_path(platform: str) -> Path:
    normalized = str(platform or "").strip().lower()
    if normalized not in SUPPORTED_METADATA_TARGETS:
        raise ValueError(f"Unsupported metadata target: {platform!r}")
    return _DDL_ROOT / f"{normalized}.sql"


def render_ddl(context: TargetMetadataContext) -> str:
    sql = ddl_path(context.platform).read_text(encoding="utf-8")
    placeholder = "__TARGET_CATALOG__" if context.platform == "databricks" else "__TARGET_DATABASE__"
    return sql.replace(placeholder, validate_identifier(context.namespace, label="metadata namespace"))


def split_sql_statements(sql: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    in_single_quote = False
    for char in sql:
        if char == "'":
            in_single_quote = not in_single_quote
        if char == ";" and not in_single_quote:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
        else:
            current.append(char)
    tail = "".join(current).strip()
    if tail:
        statements.append(tail)
    return statements


def validate_snowflake_logical_work_filters(sql: str, expected_sources: Iterable[str]) -> None:
    """Require every approved Snowflake input alias to be scoped in an ON/WHERE predicate."""
    qualified = r'(?:(?:"(?:""|[^"])+")\.){2}"(?:""|[^"])+"'
    source_matches = list(
        re.finditer(
            rf'\b(?:FROM|JOIN)\s+(?P<table>{qualified})(?:\s+(?:AS\s+)?(?P<alias>"(?:""|[^"])+"|[A-Za-z_][A-Za-z0-9_]*))?',
            str(sql or ""),
            flags=re.IGNORECASE,
        )
    )
    expected = {str(item).casefold() for item in expected_sources if str(item).strip()}
    actual = {match.group("table").casefold() for match in source_matches}
    if not expected or actual != expected:
        raise ValueError("Snowflake transformation SQL does not use exactly its pinned input objects.")
    reserved = {"where", "join", "on", "group", "order", "having", "qualify", "union", "limit"}
    for match in source_matches:
        alias = str(match.group("alias") or "").strip()
        if not alias or alias.casefold().strip('"') in reserved:
            raise ValueError("Every Snowflake transformation input must have an explicit alias.")
        alias_ref = re.escape(alias)
        column_ref = rf'{alias_ref}\s*\.\s*"_logical_work_id"'
        equality = rf'(?:{column_ref}\s*=\s*\$ATHENA_LOGICAL_WORK_ID|\$ATHENA_LOGICAL_WORK_ID\s*=\s*{column_ref})'
        clause = (
            rf'\b(?:WHERE|ON|AND)\b'
            rf'(?:(?!\b(?:SELECT|FROM|JOIN|WHERE|GROUP|ORDER|HAVING|QUALIFY|UNION|MERGE|WHEN)\b)[\s\S]){{0,1000}}?'
            rf'{equality}'
        )
        if not re.search(clause, str(sql or ""), flags=re.IGNORECASE):
            raise ValueError("Snowflake transformation SQL does not isolate every input to the queued logical work.")


def expected_columns(platform: str = "databricks") -> Dict[str, set[str]]:
    sql = ddl_path(platform).read_text(encoding="utf-8")
    pattern = re.compile(
        r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+[^.]+\.(?:metadata|metadata_schema)\.([A-Za-z_]+)\s*\((.*?)\)\s*(?:USING\s+DELTA)?\s*;",
        re.IGNORECASE | re.DOTALL,
    )
    tables: Dict[str, set[str]] = {}
    for table_name, body in pattern.findall(sql):
        columns = set()
        for line in body.splitlines():
            match = re.match(r"\s*([A-Za-z_][A-Za-z0-9_]*)\s+", line)
            if match:
                columns.add(match.group(1).lower())
        tables[table_name.lower()] = columns
    return tables


def validate_schema_columns(actual: Mapping[str, Iterable[str]]) -> None:
    expected = expected_columns()
    normalized_actual = {
        str(table).lower(): {str(column).lower() for column in columns}
        for table, columns in actual.items()
    }
    errors = []
    for table in METADATA_TABLES:
        if table not in normalized_actual:
            errors.append(f"missing table {table}")
            continue
        missing = expected[table] - normalized_actual[table]
        if missing:
            errors.append(f"{table} missing columns: {', '.join(sorted(missing))}")
        unexpected = normalized_actual[table] - expected[table]
        if unexpected:
            errors.append(f"{table} has unexpected columns: {', '.join(sorted(unexpected))}")
    if errors:
        raise RuntimeError("Metadata schema drift: " + "; ".join(errors))
