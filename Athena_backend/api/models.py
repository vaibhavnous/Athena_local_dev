from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class PipelineRunRequest(BaseModel):
    brd_text: str = Field(default="")
    brd_filename: Optional[str] = None
    source: Optional[str] = "database"
    provider: Optional[str] = "azure_openai"
    deployment: Optional[str] = None
    budget: Optional[float] = None
    maxKpis: Optional[int] = None
    devMode: Optional[bool] = None
    use_domain_kb: Optional[bool] = False
    database_name: Optional[str] = None
    database_type: Optional[str] = None
    target_warehouse: Optional[str] = "databricks"
    source_databases: Optional[List[str]] = None
    sftp_entity: Optional[str] = "transactions"
    stage_confirmation_enabled: Optional[bool] = False
    compliance_enabled: Optional[bool] = False
    compliance_domain: Optional[str] = "Insurance"
    compliance_countries: Optional[List[str]] = Field(default_factory=lambda: ["US"])


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
