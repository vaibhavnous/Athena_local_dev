import pytest

from api.models import PipelineRunRequest, ProjectRequest
from utilis import domain_kb
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


def test_legacy_run_without_explicit_kb_keeps_legacy_routing(monkeypatch):
    monkeypatch.setenv("PINECONE_KNOWLEDGE_BASE_INDEX_NAME", "legacy-global-index")
    monkeypatch.setenv("PINECONE_KNOWLEDGE_BASE_NAMESPACE", "legacy-namespace")

    config = get_domain_kb_config()

    assert config.index_name == "legacy-global-index"
    assert config.namespace == "legacy-namespace"


def test_explicit_kb_rejects_mismatched_domain():
    with pytest.raises(ValueError, match="does not match"):
        get_domain_kb_config(
            knowledge_base_id="BASEL_DW_V1",
            domain_profile="Insurance",
        )


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


def test_pipeline_request_without_selected_kb_preserves_legacy_selection():
    request = PipelineRunRequest(brd_text="requirements", use_domain_kb=True)

    assert request.knowledge_base_id is None
    assert request.domain_profile == "Insurance"


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


def test_refresh_delete_failure_stops_before_upsert(monkeypatch):
    class FailingIndex:
        def delete(self, **kwargs):
            raise RuntimeError("delete unavailable")

    monkeypatch.setattr(domain_kb, "_pinecone_index", lambda _: FailingIndex())
    monkeypatch.setattr(domain_kb, "_index_uses_integrated_embedding", lambda _: True)

    with pytest.raises(RuntimeError, match="could not clear"):
        domain_kb.upsert_kb_rows_to_pinecone(
            [{
                "kb_row_id": "row-1",
                "knowledge_base_id": "PC_Insurance_V1",
                "domain_profile": "Insurance",
                "kb_content_type": "TABLE_DEFINITION",
                "embedding_text": "claim table",
                "prompt_context": "claim table context",
                "is_active": True,
            }],
            knowledge_base_id="PC_Insurance_V1",
            refresh=True,
        )
