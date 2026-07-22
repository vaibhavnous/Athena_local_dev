from api.routers.reviews_router import (
    _compliance_api_findings,
    _compliance_review_decision,
    _findings_with_security_controls,
    _security_policies_from_findings,
)


def test_compliance_review_decision_reflects_rejected_columns():
    assert _compliance_review_decision([{"status": "Approved"}]) == "APPROVED"
    assert _compliance_review_decision([{"status": "Approved"}, {"status": "Rejected"}]) == "REJECTED"
    assert _compliance_review_decision([{"status": "Excluded"}]) == "REJECTED"


def test_compliance_api_findings_translate_ui_rejected_to_api_excluded():
    assert _compliance_api_findings([{"status": "Rejected", "table_name": "claims", "column_name": "ssn"}]) == [
        {"status": "Excluded", "table_name": "claims", "column_name": "ssn"}
    ]


def test_compliance_api_findings_do_not_send_internal_security_control():
    assert _compliance_api_findings(
        [
            {
                "status": "Approved",
                "table_name": "claims",
                "column_name": "email",
                "security_control": "Mask",
                "reviewer_comments": "ok",
            }
        ]
    ) == [
        {
            "status": "Approved",
            "table_name": "claims",
            "column_name": "email",
            "reviewer_comments": "ok",
        }
    ]


def test_security_policies_are_derived_from_approved_compliance_controls():
    findings = [
        {"status": "Approved", "table_name": "claims", "column_name": "Email", "security_control": "Mask"},
        {"status": "Modified", "table_name": "claims", "column_name": "SSN", "security_control": "Hash"},
        {"status": "Rejected", "table_name": "claims", "column_name": "DOB", "security_control": "Redact"},
        {"status": "Approved", "table_name": "claims", "column_name": "Name", "security_control": "No_Additional_Control"},
    ]

    assert _security_policies_from_findings(findings) == {
        "claims": {
            "email": "Mask",
            "ssn": "Hash",
        }
    }


def test_missing_security_control_is_filled_from_review_evidence():
    findings = [{"status": "Approved", "table_name": "claims", "column_name": "Email"}]
    review = {"column_evidence": [{"table_name": "claims", "column_name": "email", "security_control": "Mask"}]}

    enriched = _findings_with_security_controls(findings, review)

    assert enriched[0]["security_control"] == "Mask"


def test_compliance_review_fetch_error_returns_pending_status(monkeypatch):
    from api.routers import reviews_router

    saved = {}
    monkeypatch.setattr(
        "services.pipeline_runtime.load_checkpoint_state",
        lambda run_id: {
            "run_id": run_id,
            "compliance_enabled": True,
            "compliance_assessment_id": "assessment-1",
            "compliance_assessment_status": "created",
        },
    )
    monkeypatch.setattr("services.pipeline_runtime.save_checkpoint_state", lambda run_id, state: saved.update(state))
    monkeypatch.setattr(
        "services.compliance_client.fetch_review",
        lambda state: (_ for _ in ()).throw(RuntimeError("review not ready")),
    )

    response = reviews_router.compliance_reviews("run-1")

    assert response["review_status"] == "PENDING"
    assert response["review_error"] == "review not ready"
    assert saved["compliance_review_status"] == "PENDING"


def test_compliance_review_submit_persists_locally_without_external_callback(monkeypatch):
    from api.models import ComplianceReviewFinding, ComplianceReviewPayload
    from api.routers import reviews_router

    saved = {}
    submitted = {}

    monkeypatch.setattr(
        "services.pipeline_runtime.load_checkpoint_state",
        lambda run_id: {
            "run_id": run_id,
            "compliance_enabled": True,
            "compliance_assessment_id": "assessment-1",
            "compliance_assessment_status": "pending_review",
            "compliance_review": {
                "column_evidence": [
                    {"table_name": "claims", "column_name": "Email", "security_control": "Mask"}
                ]
            },
        },
    )
    monkeypatch.setattr("services.pipeline_runtime.save_checkpoint_state", lambda run_id, state: saved.update(state))
    monkeypatch.setattr(
        "services.pipeline_runtime.submit_background",
        lambda run_id, stage, fn, *args: submitted.update({"run_id": run_id, "stage": stage}),
    )
    response = reviews_router.submit_compliance_reviews(
        "run-1",
        ComplianceReviewPayload(
            findings=[
                ComplianceReviewFinding(
                    table_name="claims",
                    column_name="Email",
                    status="Approved",
                    security_control=None,
                )
            ],
            overall_comments="approved for demo",
        ),
    )

    assert response["resume_status"] == "SUBMITTED"
    assert response["security_policy_count"] == 1
    assert submitted["stage"] == "compliance_review"
    assert saved["security_policies"] == {"claims": {"email": "Mask"}}
    assert saved["compliance_review_findings"][0]["security_control"] == "Mask"
    assert saved["compliance_review_error"] is None
    assert saved["compliance_results_status"] == "completed"
    assert saved["compliance_results"]["column_evidence"][0]["security_control"] == "Mask"


def test_pending_silver_review_prefers_regenerated_script_bundle(monkeypatch):
    from api.routers import reviews_router

    monkeypatch.setattr(
        "services.pipeline_runtime.load_checkpoint_state",
        lambda run_id: {
            "run_id": run_id,
            "status": "HITL_WAIT",
            "next_gate": 5,
            "silver_review_artifact": {
                "items": [{"entity": "claims", "generated_silver_script": "stale"}]
            },
            "silver_generation_results": [{"table": "claims", "script_path": "new.py"}],
        },
    )
    monkeypatch.setattr(
        "api.services.ui_service.silver_review_from_scripts",
        lambda run_id, checkpoint: {
            "run_id": run_id,
            "items": [{"entity": "claims", "generated_silver_script": "regenerated"}],
        },
    )

    response = reviews_router.silver_reviews("run-silver-retry")

    assert response["silver_review_artifact"]["items"][0]["generated_silver_script"] == "regenerated"
