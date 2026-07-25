from services import compliance_client


def test_build_assessment_payload_uses_profile_artifacts():
    payload = compliance_client.build_assessment_payload(
        {
            "run_id": "run-1",
            "brd_text": "Claims compliance BRD",
            "brd_filename": "claims.txt",
            "compliance_domain": "Insurance",
            "compliance_countries": ["US", "AU"],
        },
        {
            "column_profiles": [
                {
                    "table_name": "claim_information",
                    "column_name": "claim_id",
                    "data_type": "varchar",
                    "profile_tier": "ID",
                    "top_samples": [{"value": "CLM001"}, {"value": "CLM002"}],
                }
            ]
        },
    )

    assert payload["filename"] == "claims.txt"
    assert payload["domain"] == "Insurance"
    assert payload["countries"] == ["US", "AU"]
    assert payload["metadata"] == [
        {
            "table_name": "claim_information",
            "column_name": "claim_id",
            "data_type": "varchar",
            "description": "Column claim_id from table claim_information discovered during Athena profiling.",
            "sample_values": ["CLM001", "CLM002"],
            "tags": ["ID"],
        }
    ]


def test_build_assessment_payload_caps_metadata(monkeypatch):
    monkeypatch.setenv("COMPLIANCE_MAX_METADATA_COLUMNS", "2")

    payload = compliance_client.build_assessment_payload(
        {"brd_text": "Claims compliance BRD"},
        {
            "column_profiles": [
                {
                    "table_name": "claims",
                    "column_name": f"column_{index}",
                    "data_type": "varchar",
                    "top_samples": [{"value": f"value-{index}"}],
                }
                for index in range(4)
            ]
        },
    )

    assert [item["column_name"] for item in payload["metadata"]] == ["column_0", "column_1"]


def test_attach_assessment_result_skips_when_disabled(monkeypatch):
    monkeypatch.setattr(
        compliance_client,
        "create_assessment",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("should not call")),
    )

    assert compliance_client.attach_assessment_result({"compliance_enabled": False}, {}) == {}


def test_attach_assessment_result_submits_background_job(monkeypatch):
    recorded = {}

    class StubExecutor:
        def submit(self, fn, *args):
            recorded["fn"] = fn
            recorded["args"] = args

    monkeypatch.setattr(compliance_client, "COMPLIANCE_EXECUTOR", StubExecutor())

    result = compliance_client.attach_assessment_result(
        {"compliance_enabled": True, "run_id": "run-1"},
        {"column_profiles": [{"column_name": "claim_id"}]},
    )

    assert result["compliance_assessment_status"] == "SUBMITTED"
    assert result["compliance_assessment_error"] is None
    assert recorded["fn"] == compliance_client._create_assessment_background


def test_attach_review_result_records_review(monkeypatch):
    monkeypatch.setattr(
        compliance_client,
        "fetch_review",
        lambda _state: {"assessment_id": "assessment-1", "column_evidence": []},
    )

    result = compliance_client.attach_review_result(
        {
            "compliance_enabled": True,
            "compliance_assessment_id": "assessment-1",
        }
    )

    assert result["compliance_review_status"] == "READY"
    assert result["compliance_review"]["assessment_id"] == "assessment-1"
    assert result["compliance_review_error"] is None
