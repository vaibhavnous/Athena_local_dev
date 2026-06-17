from typing import List, Optional

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
    source_databases: Optional[List[str]] = None
    sftp_entity: Optional[str] = "transactions"
    stage_confirmation_enabled: Optional[bool] = True


class StageContinueRequest(BaseModel):
    auto_advance: Optional[bool] = False


class HitlDecision(BaseModel):
    kpi_id: str
    decision: str
    reviewer: Optional[str] = None
    notes: Optional[str] = None
    edited_definition: Optional[str] = None


class HitlDecisionPayload(BaseModel):
    decisions: List[HitlDecision]


class Gate2DecisionPayload(BaseModel):
    approved_tables: List[str] = Field(default_factory=list)


class Gate3DecisionPayload(BaseModel):
    approve: bool = True


class GenericGateDecisionPayload(BaseModel):
    action: str = "APPROVED"
