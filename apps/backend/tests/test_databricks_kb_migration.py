import pytest

from scripts.migrate_databricks_kb import _qualified_name, validate_and_group_rows


def _row(row_id="1", kb_id="PC_Insurance_V1"):
    return {
        "kb_row_id": row_id,
        "knowledge_base_id": kb_id,
        "domain_profile": "Insurance",
        "kb_content_type": "TABLE_DEFINITION",
        "embedding_text": "claim table",
        "prompt_context": "claim table context",
        "is_active": True,
    }


def test_qualified_name_requires_safe_three_part_name():
    assert _qualified_name("workspace.athena.domain_kb_pinecone_migration_vw")
    with pytest.raises(ValueError):
        _qualified_name("workspace.athena.view; DROP TABLE claims")


def test_validate_and_group_rows_routes_both_kbs():
    grouped = validate_and_group_rows([_row(), _row("2", "BASEL_DW_V1")])
    assert len(grouped["PC_Insurance_V1"]) == 1
    assert len(grouped["BASEL_DW_V1"]) == 1


def test_validate_and_group_rows_rejects_duplicate_ids():
    with pytest.raises(ValueError, match="Duplicate"):
        validate_and_group_rows([_row(), _row()])
