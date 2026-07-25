from api.routers.reviews_router import _compliance_api_findings, _compliance_review_decision


def test_compliance_review_decision_reflects_rejected_columns():
    assert _compliance_review_decision([{"status": "Approved"}]) == "APPROVED"
    assert _compliance_review_decision([{"status": "Approved"}, {"status": "Rejected"}]) == "REJECTED"
    assert _compliance_review_decision([{"status": "Excluded"}]) == "REJECTED"


def test_compliance_api_findings_translate_ui_rejected_to_api_excluded():
    assert _compliance_api_findings([{"status": "Rejected", "table_name": "claims", "column_name": "ssn"}]) == [
        {"status": "Excluded", "table_name": "claims", "column_name": "ssn"}
    ]
