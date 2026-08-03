from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


ExecutionEngine = Literal["native", "dbt"]
DbtDeploymentMode = Literal["generate_only", "generate_and_deploy"]


class PipelineRunRequest(BaseModel):
    project_id: Optional[str] = None
    dbt_project_object_name: Optional[str] = Field(default=None, max_length=80)
    brd_text: str = Field(default="")
    brd_filename: Optional[str] = None
    source: Optional[str] = "database"
    provider: Optional[str] = "azure_openai"
    deployment: Optional[str] = None
    budget: Optional[float] = None
    maxKpis: Optional[int] = None
    devMode: Optional[bool] = None
    use_domain_kb: Optional[bool] = False
    domain_profile: Optional[str] = Field(default=None, max_length=80)
    knowledge_base_id: Optional[str] = Field(default=None, max_length=80)
    database_name: Optional[str] = None
    database_type: Optional[str] = None
    target_warehouse: Optional[str] = "databricks"
    execution_engine: ExecutionEngine = "native"
    dbt_deployment_mode: DbtDeploymentMode = "generate_only"
    dbt_target_name: Optional[str] = Field(default=None, max_length=80)
    dbt_threads: Optional[int] = Field(default=None, ge=1, le=32)
    dbt_command_timeout_secs: Optional[int] = Field(default=None, ge=60, le=86_400)
    force_dbt_deploy: Optional[bool] = False
    source_databases: Optional[List[str]] = None
    sftp_entity: Optional[str] = "transactions"
    stage_confirmation_enabled: Optional[bool] = False
    compliance_enabled: Optional[bool] = False
    compliance_domain: Optional[str] = "Insurance"
    compliance_countries: Optional[List[str]] = Field(default_factory=lambda: ["US"])

    @field_validator("source", "target_warehouse", "execution_engine", "dbt_deployment_mode", mode="before")
    @classmethod
    def normalize_dbt_choices(cls, value: Any) -> Any:
        return value.strip().lower() if isinstance(value, str) else value

    @model_validator(mode="after")
    def validate_dbt_target(self) -> "PipelineRunRequest":
        if self.use_domain_kb:
            from utilis.domain_kb import get_domain_kb_config

            kb_config = get_domain_kb_config(
                knowledge_base_id=self.knowledge_base_id,
                domain_profile=self.domain_profile,
            )
            self.knowledge_base_id = kb_config.knowledge_base_id
            self.domain_profile = kb_config.domain_profile
        if self.execution_engine != "dbt":
            self.dbt_deployment_mode = "generate_only"
            self.force_dbt_deploy = False
            return self
        if self.target_warehouse != "snowflake":
            raise ValueError("dbt code generation is only supported when target_warehouse='snowflake'")
        if self.source not in {"database", "rdbms"}:
            raise ValueError("Snowflake dbt code generation is only supported for database sources in this branch")
        if self.dbt_deployment_mode != "generate_and_deploy":
            self.force_dbt_deploy = False
        return self


class ProjectRequest(BaseModel):
    name: str
    description: str
    target: str = "Databricks"
    status: str = "ACTIVE"
    connection_type: str
    connection_name: Optional[str] = None
    db_type: Optional[str] = None
    database_name: Optional[str] = None
    integration_type: Optional[str] = None
    data_lake_type: Optional[str] = None
    data_lake_name: Optional[str] = None
    use_domain_knowledge_base: bool = False
    domain_profile: Optional[str] = None
    knowledge_base_id: Optional[str] = None
    execution_engine: ExecutionEngine = "native"
    dbt_deployment_mode: DbtDeploymentMode = "generate_only"
    dbt_target_name: Optional[str] = Field(default=None, max_length=80)
    dbt_threads: Optional[int] = Field(default=None, ge=1, le=32)
    dbt_command_timeout_secs: Optional[int] = Field(default=None, ge=60, le=86_400)
    force_dbt_deploy: Optional[bool] = False

    @field_validator("execution_engine", "dbt_deployment_mode", mode="before")
    @classmethod
    def normalize_dbt_choices(cls, value: Any) -> Any:
        return value.strip().lower() if isinstance(value, str) else value

    @model_validator(mode="after")
    def validate_dbt_project_target(self) -> "ProjectRequest":
        if self.use_domain_knowledge_base:
            from utilis.domain_kb import get_domain_kb_config

            kb_config = get_domain_kb_config(
                knowledge_base_id=self.knowledge_base_id,
                domain_profile=self.domain_profile,
            )
            self.knowledge_base_id = kb_config.knowledge_base_id
            self.domain_profile = kb_config.domain_profile
        if self.execution_engine != "dbt":
            self.dbt_deployment_mode = "generate_only"
            self.force_dbt_deploy = False
            return self
        if str(self.target or "").strip().lower() != "snowflake":
            raise ValueError("dbt code generation is only supported when target='Snowflake'")
        if str(self.connection_type or "").strip().lower() != "database":
            raise ValueError("Snowflake dbt code generation is only supported for database projects")
        self.dbt_deployment_mode = "generate_and_deploy"
        return self


class StageContinueRequest(BaseModel):
    auto_advance: Optional[bool] = False


class HitlDecision(BaseModel):
    kpi_id: str
    decision: str
    reviewer: Optional[str] = None
    notes: Optional[str] = None
    edited_definition: Optional[str] = None
    edited_content: Optional[Dict[str, Any]] = None


class HitlDecisionPayload(BaseModel):
    decisions: List[HitlDecision]


class Gate2DecisionPayload(BaseModel):
    approved_tables: List[str] = Field(default_factory=list)


class Gate3DecisionPayload(BaseModel):
    approve: bool = True
    enriched_metadata: Optional[Dict[str, Any]] = None


class GenericGateDecisionPayload(BaseModel):
    action: str = "APPROVED"
    review_artifact: Optional[Dict[str, Any]] = None


class ComplianceReviewFinding(BaseModel):
    table_name: str
    column_name: str
    status: str = "Approved"
    reviewer_comments: Optional[str] = None


class ComplianceReviewPayload(BaseModel):
    findings: List[ComplianceReviewFinding] = Field(default_factory=list)
    overall_comments: Optional[str] = None
