from services import compliance_client
from urllib.error import HTTPError


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
    monkeypatch.setenv("COMPLIANCE_CACHED_RESULT_ENABLED", "false")
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
    monkeypatch.setenv("COMPLIANCE_CACHED_RESULT_ENABLED", "false")
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


def test_create_assessment_retries_transient_server_error(monkeypatch):
    monkeypatch.setenv("COMPLIANCE_CACHED_RESULT_ENABLED", "false")
    attempts = []

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return b'{"assessment_id":"assessment-1","status":"created"}'

    def flaky_urlopen(*_args, **_kwargs):
        attempts.append(1)
        if len(attempts) == 1:
            raise HTTPError("https://example.test/api/assessments", 500, "Internal Server Error", None, None)
        return FakeResponse()

    monkeypatch.setattr(compliance_client, "urlopen", flaky_urlopen)
    monkeypatch.setattr(compliance_client, "time", type("T", (), {"sleep": lambda *_args: None, "time": lambda: 1})())
    monkeypatch.setattr(compliance_client, "ai_store_db_writer", lambda **_kwargs: None)
    monkeypatch.setenv("COMPLIANCE_ASSESSMENT_RETRIES", "1")

    result = compliance_client.create_assessment(
        {"run_id": "run-1", "brd_text": "Claims compliance BRD"},
        {"column_profiles": [{"table_name": "claims", "column_name": "email"}]},
    )

    assert result["assessment_id"] == "assessment-1"
    assert len(attempts) == 2


def test_cached_completed_result_populates_review_without_auto_approval_when_live_fails(monkeypatch):
    monkeypatch.setattr(
        compliance_client,
        "_load_cached_result",
        lambda: {
            "assessment_id": "cached-assessment-1",
            "status": "completed",
            "compliance_evidence": {"domain": "Insurance"},
            "column_evidence": [
                {
                    "table_name": "claims",
                    "column_name": "claim_amount",
                    "security_control": "Pseudonymize",
                }
            ],
        },
    )
    monkeypatch.setattr(compliance_client, "create_assessment", lambda *_args: {"assessment_id": "live-assessment-1", "status": "pending_review"})
    monkeypatch.setattr(compliance_client, "fetch_review", lambda *_args: (_ for _ in ()).throw(RuntimeError("live unavailable")))

    result = compliance_client.ensure_review_result(
        {"compliance_enabled": True, "run_id": "run-1"},
        {"column_profiles": []},
    )

    assert result["compliance_assessment_id"] == "cached-assessment-1"
    assert result["compliance_review_status"] == "READY"
    assert result["compliance_results_status"] == "completed"
    assert result["security_policies"] == {"claims": {"claim_amount": "Pseudonymize"}}
    assert "compliance_review_decision" not in result


def test_cached_result_is_scoped_to_current_profiled_columns(monkeypatch):
    monkeypatch.setattr(
        compliance_client,
        "_load_cached_result",
        lambda: {
            "assessment_id": "cached-assessment-1",
            "status": "completed",
            "column_evidence": [
                {
                    "table_name": "customer_profile",
                    "column_name": "FIRST_NAME",
                    "security_control": "Pseudonymize",
                },
                {
                    "table_name": "underwriting",
                    "column_name": "ANNUAL_INCOME",
                    "security_control": "Pseudonymize",
                },
            ],
        },
    )
    monkeypatch.setattr(compliance_client, "create_assessment", lambda *_args: {"assessment_id": "live-assessment-1", "status": "pending_review"})
    monkeypatch.setattr(compliance_client, "fetch_review", lambda *_args: (_ for _ in ()).throw(RuntimeError("live unavailable")))

    result = compliance_client.ensure_review_result(
        {"compliance_enabled": True, "run_id": "run-1"},
        {
            "column_profiles": [
                {
                    "table_name": "customer_address",
                    "column_name": "FIRST_NAME",
                }
            ]
        },
    )

    assert result["compliance_review"]["column_evidence"] == [
        {
            "table_name": "customer_address",
            "column_name": "FIRST_NAME",
            "security_control": "Pseudonymize",
        }
    ]
    assert result["security_policies"] == {"customer_address": {"first_name": "Pseudonymize"}}


def test_ensure_review_result_uses_live_review_before_cached_result(monkeypatch):
    monkeypatch.setattr(
        compliance_client,
        "_load_cached_result",
        lambda: {
            "assessment_id": "cached-assessment-1",
            "status": "completed",
            "column_evidence": [
                {
                    "table_name": "cached_table",
                    "column_name": "cached_column",
                    "security_control": "Pseudonymize",
                }
            ],
        },
    )
    monkeypatch.setattr(compliance_client, "create_assessment", lambda *_args: {"assessment_id": "live-assessment-1", "status": "pending_review"})
    monkeypatch.setattr(
        compliance_client,
        "fetch_review",
        lambda _state: {
            "assessment_id": "live-assessment-1",
            "column_evidence": [
                {
                    "table_name": "live_table",
                    "column_name": "live_column",
                    "security_control": "Mask",
                }
            ],
        },
    )

    result = compliance_client.ensure_review_result(
        {"compliance_enabled": True, "run_id": "run-1"},
        {"column_profiles": []},
    )

    assert result["compliance_assessment_id"] == "live-assessment-1"
    assert result["compliance_review"]["assessment_id"] == "live-assessment-1"
    assert result.get("security_policies") is None


def test_cached_completed_result_used_when_live_create_fails(monkeypatch):
    monkeypatch.setattr(
        compliance_client,
        "_load_cached_result",
        lambda: {
            "assessment_id": "cached-assessment-1",
            "status": "completed",
            "column_evidence": [],
        },
    )
    monkeypatch.setattr(compliance_client, "_json_request", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("down")))
    monkeypatch.setattr(compliance_client, "ai_store_db_writer", lambda **_kwargs: None)

    result = compliance_client.create_assessment(
        {"run_id": "run-1", "brd_text": "Claims compliance BRD"},
        {"column_profiles": [{"table_name": "claims", "column_name": "claim_amount"}]},
    )

    assert result["assessment_id"] == "cached-assessment-1"
    assert result["status"] == "completed"
