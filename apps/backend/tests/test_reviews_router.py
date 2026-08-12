from api.auth import AuthUser
from api.routers.reviews_router import (
    _compliance_api_findings,
    _compliance_review_decision,
    _review_artifact_for_user,
)


def test_compliance_review_decision_reflects_rejected_columns():
    assert _compliance_review_decision([{"status": "Approved"}]) == "APPROVED"
    assert _compliance_review_decision([{"status": "Approved"}, {"status": "Rejected"}]) == "REJECTED"
    assert _compliance_review_decision([{"status": "Excluded"}]) == "REJECTED"


def test_compliance_api_findings_translate_ui_rejected_to_api_excluded():
    assert _compliance_api_findings([{"status": "Rejected", "table_name": "claims", "column_name": "ssn"}]) == [
        {"status": "Excluded", "table_name": "claims", "column_name": "ssn"}
    ]


def test_clients_cannot_submit_executable_review_code():
    client = AuthUser(
        uid="client-1",
        username="Client",
        email="client@example.com",
        userType="Client",
    )
    artifact = {
        "items": [{
            "table": "claims",
            "review_status": "APPROVED",
            "script_body": "UPDATE secrets SET value = 'pwned'",
            "script_path": "other-run.sql",
            "dbt_model_sql": "select * from secrets",
            "dbt_model_body": "select * from other_secrets",
        }]
    }

    assert _review_artifact_for_user(artifact, client) == {
        "items": [{"table": "claims", "review_status": "APPROVED"}]
    }
    assert _review_artifact_for_user(
        artifact,
        client.model_copy(update={"user_type": "Admin"}),
    ) == artifact


def test_metadata_driven_review_strips_executable_code_for_admins_too():
    admin = AuthUser(
        uid="admin-1",
        username="Admin",
        email="admin@example.com",
        userType="Admin",
    )
    artifact = {
        "items": [{
            "silver_ingestion_object_id": "4134741637349269810",
            "mapping_version": 7,
            "review_status": "APPROVED",
            "generated_silver_script": "print('changed')",
            "script_body": "print('changed')",
        }]
    }

    assert _review_artifact_for_user(artifact, admin, strip_executable=True) == {
        "items": [{
            "silver_ingestion_object_id": "4134741637349269810",
            "mapping_version": 7,
            "review_status": "APPROVED",
        }]
    }
