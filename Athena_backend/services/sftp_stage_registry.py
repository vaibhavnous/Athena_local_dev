from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Tuple


@dataclass(frozen=True)
class SftpStageSpec:
    key: str
    notebook: str
    label: str
    phase_id: str
    phase_label: str
    status_field: str
    artifact_type: Optional[str] = None
    gate: Optional[int] = None
    checkpoint_policy: str = "none"


_PHASES = {
    "phase-1": "Discovery & Requirement Intelligence",
    "phase-2": "Feed & Metadata Intelligence",
    "phase-3": "Metadata Bootstrap & Source Validation",
    "phase-4": "Bronze Layer (Ingestion & DQ)",
    "phase-5": "Silver Layer (Transformation & DQ)",
    "phase-6": "Gold Layer & Deployment",
}


def _stage(
    key: str,
    notebook: str,
    label: str,
    phase_id: str,
    status_field: str,
    artifact_type: Optional[str] = None,
    gate: Optional[int] = None,
    checkpoint_policy: str = "none",
) -> SftpStageSpec:
    return SftpStageSpec(
        key=key,
        notebook=notebook,
        label=label,
        phase_id=phase_id,
        phase_label=_PHASES[phase_id],
        status_field=status_field,
        artifact_type=artifact_type,
        gate=gate,
        checkpoint_policy=checkpoint_policy,
    )


# This is the canonical execution order from the approved SFTP/ADLS design.
# A stage may appear in the UI only when it exists in this registry and is
# backed by a real graph node.
SFTP_STAGE_SPECS: Tuple[SftpStageSpec, ...] = (
    _stage("brd_ingestion", "SFTP_NB01_DL_BRD_Ingestion.py", "BRD Ingest", "phase-1", "brd_ingestion_status", "SFTP_BRD_INGESTION", checkpoint_policy="complete"),
    _stage("memory_check", "SFTP_NB02_Memory_Check.py", "Memory Check", "phase-1", "memory_check_status", "SFTP_MEMORY_CONTEXT"),
    _stage("requirements", "SFTP_NB03_Requirements.py", "Requirement Extraction", "phase-1", "requirements_status", "SFTP_REQUIREMENTS"),
    _stage("kpi_extraction", "SFTP_NB04_KPI_Extract.py", "KPI Extraction", "phase-1", "kpi_extraction_status", "SFTP_KPI_EXTRACTION"),
    _stage("gate1", "SFTP_NB05_HITL_Gate1.py", "KPI Review", "phase-1", "gate1_status", "SFTP_APPROVED_KPIS", 1, "complete"),
    _stage("feed_discovery", "SFTP_NB06_Feed_Discovery.py", "Discover Source Objects", "phase-2", "feed_discovery_status", "SFTP_FEED_DISCOVERY"),
    _stage("feed_nomination", "SFTP_NB06b_Feed_Nomination.py", "Feed Nomination", "phase-2", "feed_nomination_status", "SFTP_FEED_NOMINATION"),
    _stage("gate2", "SFTP_NB06c_HITL_Gate2.py", "Feed Review", "phase-2", "gate2_status", "SFTP_GATE2_DECISION", 2, "complete"),
    _stage("metadata_discovery", "SFTP_NB07_Metadata_Discovery.py", "Schema Snapshot", "phase-2", "metadata_discovery_status", "SFTP_SCHEMA_SNAPSHOT"),
    _stage("column_profiling", "SFTP_NB08_Column_Profiling.py", "Column Profiling", "phase-2", "column_profiling_status", "SFTP_COLUMN_PROFILING"),
    _stage("semantic_enrichment", "SFTP_NB09_Semantic_Enrichment.py", "Semantic Enrichment", "phase-2", "semantic_enrichment_status", "ENRICHED_METADATA"),
    _stage("gate3", "SFTP_NB09b_HITL_Gate3.ipynb", "Semantic Review", "phase-2", "gate3_status", "GATE3_APPROVED_ENRICHMENT", 3, "complete"),
    _stage("metadata_bootstrap", "SFTP_NB10_Metadata_Bootstrap.py", "Bootstrap Metadata", "phase-3", "metadata_bootstrap_status", "SFTP_METADATA_BOOTSTRAP"),
    _stage("plan_seal", "SFTP_NB09c_Plan_Seal_Check.py", "Seal Approved Plan", "phase-3", "plan_seal_status", "SFTP_SEALED_PLAN"),
    _stage("freshness_check", "SFTP_NB07b_Freshness_Check.py", "Validate Plan Freshness", "phase-3", "freshness_check_status", "SFTP_FRESHNESS_MANIFEST"),
    _stage("metadata_codegen", "SFTP_NB10b_Metadata_Codegen.py", "Metadata Code Generation", "phase-3", "metadata_codegen_status", "SFTP_METADATA_CODEGEN"),
    _stage("gate4_metadata", "SFTP_NB10c_HITL_Gate4_Metadata.py", "Metadata Code Review", "phase-3", "gate4_metadata_status", "SFTP_GATE4_METADATA_DECISION", 4, "complete"),
    _stage("dab_bundle", "SFTP_NB22_Generate_DAB_Bundle.py", "Generate Runtime Bundle", "phase-3", "dab_bundle_status", "SFTP_DAB_BUNDLE", checkpoint_policy="complete"),
    _stage("runtime_config", "SFTP_NB11_Load_Runtime_Config.py", "Prepare Runtime Configuration", "phase-4", "runtime_config_status", "SFTP_RUNTIME_CONFIG"),
    _stage("validate_source", "SFTP_NB12_Validate_Source.py", "Validate Source Access", "phase-4", "source_validation_status", "SFTP_SOURCE_VALIDATION"),
    _stage("discover_source_objects", "SFTP_NB13_Discover_Source_Objects.py", "Discover Runtime Objects", "phase-4", "source_object_discovery_status", "SFTP_RUNTIME_OBJECT_DISCOVERY"),
    _stage("stage_to_landing", "SFTP_NB14_Stage_To_Landing.py", "Stage Files to Landing", "phase-4", "stage_to_landing_status", "SFTP_LANDING_MANIFEST", checkpoint_policy="before_after"),
    _stage("bronze_autoloader", "SFTP_NB15_Bronze_AutoLoader.ipynb", "Bronze Ingestion", "phase-4", "bronze_execution_status", "SFTP_BRONZE_EXECUTION", checkpoint_policy="before_after"),
    _stage("bronze_dq", "SFTP_NB16_Bronze_DQ.py", "Bronze Data Quality", "phase-4", "bronze_dq_status", "SFTP_BRONZE_DQ", checkpoint_policy="complete"),
    _stage("bronze_to_silver", "SFTP_NB17_Bronze_To_Silver_Merge.py", "Silver Transformation", "phase-5", "silver_execution_status", "SFTP_SILVER_EXECUTION", checkpoint_policy="before_after"),
    _stage("silver_dq", "SFTP_NB18_Silver_DQ.py", "Silver Data Quality", "phase-5", "silver_dq_status", "SFTP_SILVER_DQ", checkpoint_policy="complete"),
    _stage("silver_to_gold", "SFTP_NB19_Silver_To_Gold_Merge.py", "Gold Model Build", "phase-6", "gold_execution_status", "SFTP_GOLD_EXECUTION", checkpoint_policy="before_after"),
    _stage("gold_dq", "SFTP_NB20_Gold_DQ.py", "Gold Data Quality", "phase-6", "gold_dq_status", "SFTP_GOLD_DQ", checkpoint_policy="complete"),
    _stage("gate5_publish", "SFTP_NB20b_HITL_Gate5.py", "Final Publish Review", "phase-6", "gate5_publish_status", "SFTP_GATE5_PUBLISH_DECISION", 5, "complete"),
    _stage("finalize", "SFTP_NB21_Finalize_Run.py", "Finalize Run", "phase-6", "finalization_status", "SFTP_RUN_FINALIZATION", checkpoint_policy="complete"),
)

SFTP_STAGE_BY_KEY: Dict[str, SftpStageSpec] = {stage.key: stage for stage in SFTP_STAGE_SPECS}
SFTP_STAGE_KEYS: Tuple[str, ...] = tuple(stage.key for stage in SFTP_STAGE_SPECS)


def stage_spec(stage_key: str) -> SftpStageSpec:
    try:
        return SFTP_STAGE_BY_KEY[stage_key]
    except KeyError as exc:
        raise ValueError(f"Unknown SFTP/ADLS stage: {stage_key}") from exc


def phase_templates() -> Tuple[Dict[str, object], ...]:
    return tuple(
        {
            "id": phase_id,
            "label": phase_label,
            "keys": tuple(stage.key for stage in SFTP_STAGE_SPECS if stage.phase_id == phase_id),
        }
        for phase_id, phase_label in _PHASES.items()
    )
