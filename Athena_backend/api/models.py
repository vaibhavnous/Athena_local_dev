import json
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


MAX_BRD_TEXT_LENGTH = 5 * 1024 * 1024
MAX_REVIEW_ITEMS = 500
REVIEW_STATUS_VALUES = {"APPROVED", "REJECTED", "PENDING", "MODIFIED", "EXCLUDED"}
REVIEW_ITEM_ID_FIELDS = {
    "id",
    "item_id",
    "feed_id",
    "table",
    "table_name",
    "entity",
    "source_table",
    "target_table",
    "bronze_table",
    "silver_table",
    "script_name",
    "name",
    "kpi_name",
    "view_name",
}

PipelineSource = Literal["database", "sftp", "adls_gen2", "rdbms"]
TargetWarehouse = Literal["databricks", "snowflake", "fabric"]
ExecutionEngine = Literal["native", "dbt"]
DbtDeploymentMode = Literal["generate_only", "generate_and_deploy"]
ReviewAction = Literal["APPROVED", "REJECTED", "REGENERATE"]
BulkKpiAction = Literal["APPROVED", "REJECTED"]
HitlDecisionAction = Literal["APPROVED", "EDITED", "REJECTED"]


class StrictApiModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class FlexibleApiPayload(BaseModel):
    model_config = ConfigDict(extra="allow", str_strip_whitespace=True)

    def to_payload(self) -> Dict[str, Any]:
        payload = dict(self.model_extra or {})
        payload.update(self.model_dump(exclude_none=True))
        return payload


def _upper(value: Any) -> Any:
    return value.strip().upper() if isinstance(value, str) else value


def _lower(value: Any) -> Any:
    return value.strip().lower() if isinstance(value, str) else value


def _bounded_json_object(value: Dict[str, Any], *, field_name: str, max_bytes: int = 100_000) -> Dict[str, Any]:
    try:
        encoded = json.dumps(value, default=str)
    except Exception as exc:
        raise ValueError(f"{field_name} must be JSON serializable") from exc
    if len(encoded.encode("utf-8")) > max_bytes:
        raise ValueError(f"{field_name} is too large")
    return value


class PipelineRunRequest(StrictApiModel):
    project_id: Optional[str] = None
    brd_text: str = Field(default="", max_length=MAX_BRD_TEXT_LENGTH)
    brd_filename: Optional[str] = Field(default=None, max_length=512)
    source: PipelineSource = "database"
    provider: Optional[str] = Field(default="azure_openai", max_length=80)
    deployment: Optional[str] = Field(default=None, max_length=128)
    budget: Optional[float] = Field(default=None, ge=0, le=10_000)
    maxKpis: Optional[int] = Field(default=None, ge=1, le=25)
    devMode: Optional[bool] = None
    use_domain_kb: Optional[bool] = False
    database_name: Optional[str] = Field(default=None, max_length=128)
    database_type: Optional[str] = Field(default=None, max_length=80)
    target_warehouse: TargetWarehouse = "databricks"
    execution_engine: ExecutionEngine = "native"
    dbt_deployment_mode: DbtDeploymentMode = "generate_only"
    dbt_target_name: Optional[str] = Field(default=None, max_length=80)
    dbt_threads: Optional[int] = Field(default=None, ge=1, le=32)
    dbt_command_timeout_secs: Optional[int] = Field(default=None, ge=60, le=86_400)
    force_dbt_deploy: Optional[bool] = False
    source_databases: Optional[List[str]] = Field(default=None, max_length=20)
    sftp_entity: Optional[Literal["transactions", "employee", "both", "auto"]] = "transactions"
    stage_confirmation_enabled: Optional[bool] = False
    compliance_enabled: Optional[bool] = False
    compliance_domain: Optional[str] = Field(default="Insurance", max_length=80)
    compliance_countries: Optional[List[str]] = Field(default_factory=lambda: ["US"], max_length=20)

    @field_validator("source", "target_warehouse", "execution_engine", "dbt_deployment_mode", "sftp_entity", mode="before")
    @classmethod
    def normalize_lowercase(cls, value: Any) -> Any:
        return _lower(value)

    @field_validator("source_databases", "compliance_countries", mode="after")
    @classmethod
    def strip_string_lists(cls, value: Optional[List[str]]) -> Optional[List[str]]:
        if value is None:
            return None
        cleaned = [str(item).strip() for item in value if str(item).strip()]
        return cleaned or None

    @model_validator(mode="after")
    def validate_dbt_target(self) -> "PipelineRunRequest":
        dbt_requested = self.execution_engine == "dbt" or self.dbt_deployment_mode != "generate_only"
        if dbt_requested and self.target_warehouse != "snowflake":
            raise ValueError("dbt execution is only supported when target_warehouse='snowflake'")
        if dbt_requested and self.source not in {"database", "rdbms"}:
            raise ValueError("Snowflake dbt execution is only supported for database sources in this branch")
        return self


class ProjectRequest(StrictApiModel):
    name: str = Field(..., max_length=200)
    description: str = Field(..., max_length=5000)
    target: str = Field(default="Databricks", max_length=40)
    status: str = Field(default="ACTIVE", max_length=40)
    connection_type: str = Field(..., max_length=40)
    connection_name: Optional[str] = Field(default=None, max_length=200)
    db_type: Optional[str] = Field(default=None, max_length=80)
    database_name: Optional[str] = Field(default=None, max_length=128)
    integration_type: Optional[str] = Field(default=None, max_length=80)
    data_lake_type: Optional[str] = Field(default=None, max_length=80)
    data_lake_name: Optional[str] = Field(default=None, max_length=200)
    use_domain_knowledge_base: bool = False
    domain_profile: Optional[str] = Field(default=None, max_length=200)
    knowledge_base_id: Optional[str] = Field(default=None, max_length=200)
    execution_engine: ExecutionEngine = "native"
    dbt_deployment_mode: DbtDeploymentMode = "generate_only"
    dbt_target_name: Optional[str] = Field(default=None, max_length=80)
    dbt_threads: Optional[int] = Field(default=None, ge=1, le=32)
    dbt_command_timeout_secs: Optional[int] = Field(default=None, ge=60, le=86_400)
    force_dbt_deploy: Optional[bool] = False

    @field_validator("execution_engine", "dbt_deployment_mode", mode="before")
    @classmethod
    def normalize_dbt_choices(cls, value: Any) -> Any:
        return _lower(value)

    @model_validator(mode="after")
    def validate_dbt_project_target(self) -> "ProjectRequest":
        dbt_requested = self.execution_engine == "dbt" or self.dbt_deployment_mode != "generate_only"
        if dbt_requested and str(self.target or "").strip().lower() != "snowflake":
            raise ValueError("dbt execution is only supported when target='Snowflake'")
        if dbt_requested and str(self.connection_type or "").strip().lower() != "database":
            raise ValueError("Snowflake dbt execution is only supported for database projects")
        return self


class StageContinueRequest(StrictApiModel):
    auto_advance: Optional[bool] = False


class HitlDecision(StrictApiModel):
    kpi_id: str = Field(..., min_length=1, max_length=256)
    decision: HitlDecisionAction
    reviewer: Optional[str] = Field(default=None, max_length=200)
    notes: Optional[str] = Field(default=None, max_length=2000)
    edited_definition: Optional[str] = Field(default=None, max_length=5000)
    edited_content: Optional[Dict[str, Any]] = None

    @field_validator("decision", mode="before")
    @classmethod
    def normalize_decision(cls, value: Any) -> Any:
        return _upper(value)

    @field_validator("edited_content", mode="after")
    @classmethod
    def validate_edited_content(cls, value: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        return _bounded_json_object(value, field_name="edited_content") if value is not None else value


class HitlDecisionPayload(StrictApiModel):
    decisions: List[HitlDecision] = Field(..., min_length=1, max_length=200)


class Gate2DecisionPayload(StrictApiModel):
    approved_tables: List[str] = Field(default_factory=list, max_length=200)

    @field_validator("approved_tables", mode="after")
    @classmethod
    def normalize_tables(cls, value: List[str]) -> List[str]:
        return [str(item).strip() for item in value if str(item).strip()]


class Gate3DecisionPayload(StrictApiModel):
    approve: bool = True
    enriched_metadata: Optional[Dict[str, Any]] = None

    @field_validator("enriched_metadata", mode="after")
    @classmethod
    def validate_enriched_metadata(cls, value: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        return _bounded_json_object(value, field_name="enriched_metadata", max_bytes=1_000_000) if value is not None else value


class GenericGateDecisionPayload(StrictApiModel):
    action: ReviewAction
    review_artifact: Optional[Dict[str, Any]] = None

    @field_validator("action", mode="before")
    @classmethod
    def normalize_action(cls, value: Any) -> Any:
        return _upper(value)

    @model_validator(mode="after")
    def validate_review_artifact(self) -> "GenericGateDecisionPayload":
        artifact = self.review_artifact
        if artifact is None:
            return self
        _bounded_json_object(artifact, field_name="review_artifact", max_bytes=2_000_000)
        for key in ("feeds", "items"):
            if key not in artifact:
                continue
            items = artifact.get(key)
            if items is None:
                artifact[key] = []
                continue
            if not isinstance(items, list):
                raise ValueError(f"review_artifact.{key} must be a list")
            if len(items) > MAX_REVIEW_ITEMS:
                raise ValueError(f"review_artifact.{key} cannot exceed {MAX_REVIEW_ITEMS} entries")
            for index, item in enumerate(items):
                if not isinstance(item, dict):
                    raise ValueError(f"review_artifact.{key}[{index}] must be an object")
                status = item.get("review_status")
                if status is not None:
                    normalized_status = str(status).strip().upper()
                    if normalized_status not in REVIEW_STATUS_VALUES:
                        raise ValueError(f"review_artifact.{key}[{index}].review_status is unsupported")
                    item["review_status"] = normalized_status
                    if not any(str(item.get(field) or "").strip() for field in REVIEW_ITEM_ID_FIELDS):
                        raise ValueError(f"review_artifact.{key}[{index}] must include a stable item identifier")
        return self


class KpiCreatePayload(StrictApiModel):
    name: str = Field(..., min_length=1, max_length=250)
    definition: str = Field(..., min_length=1, max_length=5000)
    category: Optional[str] = Field(default="Business KPI", max_length=120)
    domain: Optional[str] = Field(default="Athena", max_length=120)


class KpiActionPayload(StrictApiModel):
    reviewer_id: Optional[str] = Field(default=None, max_length=200)


class KpiRejectPayload(KpiActionPayload):
    rejection_reason: Optional[str] = Field(default=None, max_length=2000)


class KpiModifyPayload(KpiActionPayload):
    edited_content: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("edited_content", mode="after")
    @classmethod
    def validate_edited_content(cls, value: Dict[str, Any]) -> Dict[str, Any]:
        return _bounded_json_object(value, field_name="edited_content")


class KpiBulkActionPayload(KpiRejectPayload):
    action: BulkKpiAction = "APPROVED"

    @field_validator("action", mode="before")
    @classmethod
    def normalize_action(cls, value: Any) -> Any:
        return _upper(value)


class ComplianceReviewFinding(StrictApiModel):
    table_name: str = Field(..., min_length=1, max_length=256)
    column_name: str = Field(..., min_length=1, max_length=256)
    status: str = Field(default="Approved", max_length=40)
    reviewer_comments: Optional[str] = Field(default=None, max_length=2000)

    @field_validator("status", mode="after")
    @classmethod
    def validate_status(cls, value: str) -> str:
        normalized = str(value or "").strip().lower()
        if normalized not in {"approved", "modified", "excluded", "rejected"}:
            raise ValueError("Compliance finding status must be Approved, Modified, Excluded, or Rejected")
        return value


class ComplianceReviewPayload(StrictApiModel):
    findings: List[ComplianceReviewFinding] = Field(default_factory=list, max_length=1000)
    overall_comments: Optional[str] = Field(default=None, max_length=5000)


class SettingsPayload(FlexibleApiPayload):
    provider: Optional[str] = Field(default=None, max_length=80)
    azure_deployment: Optional[str] = Field(default=None, max_length=128)
    budget: Optional[float] = Field(default=None, ge=0, le=10_000)
    maxKpis: Optional[int] = Field(default=None, ge=1, le=25)
    devMode: Optional[bool] = None


class ConfigurationPayload(FlexibleApiPayload):
    @model_validator(mode="after")
    def validate_known_shape(self) -> "ConfigurationPayload":
        data = self.to_payload()
        source_type = str(data.get("source_type") or data.get("sourceType") or "database").strip().lower()
        if source_type not in {"database", "data_lake"}:
            raise ValueError("sourceType must be database or data_lake")
        if source_type == "database":
            port = data.get("port")
            if port not in {None, ""}:
                try:
                    port_value = int(str(port).strip())
                except ValueError as exc:
                    raise ValueError("port must be a number") from exc
                if port_value < 1 or port_value > 65535:
                    raise ValueError("port must be between 1 and 65535")
        return self
