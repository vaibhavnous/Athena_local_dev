import pytest

from api.models import PipelineRunRequest, ProjectRequest
from utilis.domain_kb import get_domain_kb_config


def test_explicit_kb_ids_route_to_separate_pinecone_indexes(monkeypatch):
    monkeypatch.setenv("PINECONE_KNOWLEDGE_BASE_INDEX_NAME", "legacy-global-index")

    insurance = get_domain_kb_config(knowledge_base_id="PC_Insurance_V1")
    basel = get_domain_kb_config(knowledge_base_id="BASEL_DW_V1")

    assert (insurance.domain_profile, insurance.index_name) == ("Insurance", "insurancekb")
    assert (basel.domain_profile, basel.index_name) == ("Basel", "beselkb")


def test_selected_kb_index_can_be_overridden_per_domain(monkeypatch):
    monkeypatch.setenv("PINECONE_BASEL_KB_INDEX_NAME", "baselkb-test")

    config = get_domain_kb_config(knowledge_base_id="BASEL_DW_V1")

    assert config.index_name == "baselkb-test"
    assert config.namespace == "BASEL_DW_V1"


def test_pipeline_request_rejects_unknown_selected_kb():
    with pytest.raises(ValueError, match="Unsupported knowledge_base_id"):
        PipelineRunRequest(
            brd_text="requirements",
            use_domain_kb=True,
            knowledge_base_id="unknown-kb",
        )


def test_pipeline_request_normalizes_selected_kb():
    request = PipelineRunRequest(
        brd_text="requirements",
        use_domain_kb=True,
        knowledge_base_id="BASEL_DW_V1",
    )

    assert request.knowledge_base_id == "BASEL_DW_V1"
    assert request.domain_profile == "Basel"


def test_project_request_normalizes_selected_kb():
    project = ProjectRequest(
        name="Basel reporting",
        description="Regulatory reporting",
        connection_type="database",
        use_domain_knowledge_base=True,
        knowledge_base_id="BASEL_DW_V1",
    )

    assert project.knowledge_base_id == "BASEL_DW_V1"
    assert project.domain_profile == "Basel"
