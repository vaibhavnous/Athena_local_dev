from __future__ import annotations

import uuid

import pytest
from fastapi import HTTPException

from api.auth import AuthService, AuthUser, CreateUserRequest, assert_run_access, assert_run_gate_open


class FakeAuthRepository:
    def __init__(self) -> None:
        self.users: dict[str, dict] = {}

    def ensure_table(self) -> None:
        return None

    def find_by_email(self, email: str):
        return next((user for user in self.users.values() if user["email"] == email), None)

    def find_by_uid(self, uid: str):
        return self.users.get(uid)

    def list_users(self):
        return list(self.users.values())

    def create_user(self, *, uid, username, email, password_hash, user_type):
        user = {
            "uid": uid,
            "username": username,
            "email": email,
            "password_hash": password_hash,
            "user_type": user_type,
            "is_active": True,
            "token_version": 1,
        }
        self.users[uid] = user
        return user

    def update_user(self, uid, *, username, email, user_type, password_hash):
        user = self.users.get(uid)
        if not user:
            return None
        user.update(username=username, email=email, user_type=user_type)
        if password_hash:
            user["password_hash"] = password_hash
        user["token_version"] += 1
        return user

    def set_active(self, uid, is_active):
        user = self.users.get(uid)
        if not user:
            return None
        user["is_active"] = is_active
        user["token_version"] += 1
        return user

    def delete_user(self, uid):
        return self.users.pop(uid, None) is not None


@pytest.fixture
def auth(monkeypatch):
    monkeypatch.setenv("ASTRA_AUTH_EMAIL", "admin@astra.local")
    monkeypatch.setenv("ASTRA_AUTH_USERNAME", "Primary Admin")
    monkeypatch.setenv("ASTRA_AUTH_PASSWORD", "AdminPass!234")
    monkeypatch.setenv("ASTRA_JWT_SECRET", "test-secret-that-is-at-least-32-bytes-long")
    repository = FakeAuthRepository()
    return AuthService(repository), repository


def test_login_issues_token_that_resolves_current_user(auth):
    service, _ = auth

    session = service.login(" ADMIN@ASTRA.LOCAL ", "AdminPass!234")
    current_user = service.authenticate_token(session.access_token)

    assert current_user.email == "admin@astra.local"
    assert current_user.user_type == "Admin"
    assert session.expires_in == 3600


def test_disabling_account_immediately_invalidates_existing_token(auth):
    service, repository = auth
    session = service.login("admin@astra.local", "AdminPass!234")
    user = repository.find_by_email("admin@astra.local")
    user["is_active"] = False
    user["token_version"] += 1

    with pytest.raises(HTTPException) as exc:
        service.authenticate_token(session.access_token)

    assert exc.value.status_code == 401


def test_only_primary_admin_can_create_accounts(auth):
    service, _ = auth
    request = CreateUserRequest(
        username="Client User",
        email="client@example.com",
        password="ClientPass!234",
        userType="Client",
    )
    client = AuthUser(
        uid=str(uuid.uuid4()),
        username="Client",
        email="client@example.com",
        userType="Client",
    )

    with pytest.raises(HTTPException) as exc:
        service.create_user(request, client)

    assert exc.value.status_code == 403


def test_legacy_primary_admin_password_can_bootstrap(monkeypatch):
    monkeypatch.setenv("ASTRA_AUTH_EMAIL", "admin@astra.local")
    monkeypatch.setenv("ASTRA_AUTH_USERNAME", "Primary Admin")
    monkeypatch.setenv("ASTRA_AUTH_PASSWORD", "admin123")
    monkeypatch.setenv("ASTRA_JWT_SECRET", "test-secret-that-is-at-least-32-bytes-long")
    service = AuthService(FakeAuthRepository())

    session = service.login("admin@astra.local", "admin123")

    assert session.user.can_manage_accounts is True


def test_existing_primary_admin_password_syncs_from_env(monkeypatch):
    monkeypatch.setenv("ASTRA_AUTH_EMAIL", "admin@astra.local")
    monkeypatch.setenv("ASTRA_AUTH_USERNAME", "Primary Admin")
    monkeypatch.setenv("ASTRA_AUTH_PASSWORD", "NewAdminPass!234")
    monkeypatch.setenv("ASTRA_JWT_SECRET", "test-secret-that-is-at-least-32-bytes-long")
    repository = FakeAuthRepository()
    repository.create_user(
        uid=str(uuid.uuid4()),
        username="Primary Admin",
        email="admin@astra.local",
        password_hash=AuthService._hash_password("OldAdminPass!234"),
        user_type="Admin",
    )
    service = AuthService(repository)

    session = service.login("admin@astra.local", "NewAdminPass!234")

    assert session.user.can_manage_accounts is True


def test_client_cannot_access_another_users_run():
    client = AuthUser(
        uid=str(uuid.uuid4()),
        username="Client",
        email="client@example.com",
        userType="Client",
    )

    with pytest.raises(HTTPException) as exc:
        assert_run_access("run-1", client, checkpoint={"run_id": "run-1", "owner_email": "other@example.com"})

    assert exc.value.status_code == 403


def test_unowned_legacy_run_is_not_client_accessible_by_default(monkeypatch):
    monkeypatch.delenv("ATHENA_ALLOW_LEGACY_UNOWNED_RUNS", raising=False)
    client = AuthUser(
        uid=str(uuid.uuid4()),
        username="Client",
        email="client@example.com",
        userType="Client",
    )

    with pytest.raises(HTTPException) as exc:
        assert_run_access("run-legacy", client, checkpoint={"run_id": "run-legacy"})

    assert exc.value.status_code == 403


def test_review_gate_replay_is_rejected():
    admin = AuthUser(
        uid=str(uuid.uuid4()),
        username="Admin",
        email="admin@astra.local",
        userType="Admin",
    )

    with pytest.raises(HTTPException) as exc:
        assert_run_gate_open(
            "run-1",
            admin,
            checkpoint={
                "run_id": "run-1",
                "status": "HITL_WAIT",
                "next_gate": 4,
                "bronze_review_decision": "APPROVED",
            },
            gate_number=4,
        )

    assert exc.value.status_code == 409


def test_stale_gate2_checkpoint_with_nominated_tables_is_open():
    admin = AuthUser(
        uid=str(uuid.uuid4()),
        username="Admin",
        email="admin@astra.local",
        userType="Admin",
    )
    checkpoint = {
        "run_id": "run-gate2",
        "status": "HITL_WAIT",
        "human_table_decision": "PENDING",
        "nominated_tables": [{"database_name": "insurance", "schema_name": "dbo", "table_name": "claims"}],
    }

    result = assert_run_gate_open("run-gate2", admin, checkpoint=checkpoint, gate_number=2)

    assert result is checkpoint


def test_gate2_fallback_does_not_open_active_checkpoint():
    admin = AuthUser(
        uid=str(uuid.uuid4()),
        username="Admin",
        email="admin@astra.local",
        userType="Admin",
    )

    with pytest.raises(HTTPException) as exc:
        assert_run_gate_open(
            "run-gate2",
            admin,
            checkpoint={
                "run_id": "run-gate2",
                "status": "RUNNING",
                "human_table_decision": "PENDING",
                "nominated_tables": [{"database_name": "insurance", "schema_name": "dbo", "table_name": "claims"}],
            },
            gate_number=2,
        )

    assert exc.value.status_code == 409
    assert "not waiting for gate 2" in exc.value.detail


def test_gate2_review_replay_is_rejected_from_human_decision():
    admin = AuthUser(
        uid=str(uuid.uuid4()),
        username="Admin",
        email="admin@astra.local",
        userType="Admin",
    )

    with pytest.raises(HTTPException) as exc:
        assert_run_gate_open(
            "run-gate2",
            admin,
            checkpoint={
                "run_id": "run-gate2",
                "status": "HITL_WAIT",
                "next_gate": 2,
                "human_table_decision": "COMPLETED",
                "certified_tables": [{"database_name": "insurance", "schema_name": "dbo", "table_name": "claims"}],
            },
            gate_number=2,
        )

    assert exc.value.status_code == 409
    assert "already been decided" in exc.value.detail


def test_stale_gate3_checkpoint_with_enriched_metadata_is_open():
    admin = AuthUser(
        uid=str(uuid.uuid4()),
        username="Admin",
        email="admin@astra.local",
        userType="Admin",
    )
    checkpoint = {
        "run_id": "run-gate3",
        "status": "HITL_WAIT",
        "enrichment_review_status": "PENDING",
        "enrichment_review_decision": "PENDING",
        "enriched_metadata": {"columns": [{"table_name": "claims", "column_name": "claim_id"}]},
    }

    result = assert_run_gate_open("run-gate3", admin, checkpoint=checkpoint, gate_number=3)

    assert result is checkpoint


def test_gate3_fallback_does_not_open_active_checkpoint():
    admin = AuthUser(
        uid=str(uuid.uuid4()),
        username="Admin",
        email="admin@astra.local",
        userType="Admin",
    )

    with pytest.raises(HTTPException) as exc:
        assert_run_gate_open(
            "run-gate3",
            admin,
            checkpoint={
                "run_id": "run-gate3",
                "status": "RUNNING",
                "enrichment_review_status": "PENDING",
                "enrichment_review_decision": "PENDING",
                "enriched_metadata": {"columns": [{"table_name": "claims", "column_name": "claim_id"}]},
            },
            gate_number=3,
        )

    assert exc.value.status_code == 409
    assert "not waiting for gate 3" in exc.value.detail


def test_gate3_review_replay_is_rejected_from_enrichment_decision():
    admin = AuthUser(
        uid=str(uuid.uuid4()),
        username="Admin",
        email="admin@astra.local",
        userType="Admin",
    )

    with pytest.raises(HTTPException) as exc:
        assert_run_gate_open(
            "run-gate3",
            admin,
            checkpoint={
                "run_id": "run-gate3",
                "status": "HITL_WAIT",
                "next_gate": 3,
                "enrichment_review_status": "COMPLETED",
                "enrichment_review_decision": "APPROVED",
                "enrichment_review_artifact": {"columns": []},
            },
            gate_number=3,
        )

    assert exc.value.status_code == 409
    assert "already been decided" in exc.value.detail


def test_stale_gate4_checkpoint_with_bronze_artifact_is_open():
    admin = AuthUser(
        uid=str(uuid.uuid4()),
        username="Admin",
        email="admin@astra.local",
        userType="Admin",
    )
    checkpoint = {
        "run_id": "run-gate4",
        "status": "HITL_WAIT",
        "bronze_review_artifact": {"feeds": [{"entity": "claims", "review_status": "PENDING"}]},
    }

    result = assert_run_gate_open("run-gate4", admin, checkpoint=checkpoint, gate_number=4)

    assert result is checkpoint


def test_gate4_fallback_does_not_open_active_checkpoint():
    admin = AuthUser(
        uid=str(uuid.uuid4()),
        username="Admin",
        email="admin@astra.local",
        userType="Admin",
    )

    with pytest.raises(HTTPException) as exc:
        assert_run_gate_open(
            "run-gate4",
            admin,
            checkpoint={
                "run_id": "run-gate4",
                "status": "RUNNING",
                "bronze_review_artifact": {"feeds": [{"entity": "claims", "review_status": "PENDING"}]},
            },
            gate_number=4,
        )

    assert exc.value.status_code == 409
    assert "not waiting for gate 4" in exc.value.detail


def test_stale_gate5_checkpoint_with_silver_artifact_is_open():
    admin = AuthUser(
        uid=str(uuid.uuid4()),
        username="Admin",
        email="admin@astra.local",
        userType="Admin",
    )
    checkpoint = {
        "run_id": "run-gate5",
        "status": "HITL_WAIT",
        "silver_review_artifact": {"items": [{"entity": "claims", "review_status": "PENDING"}]},
    }

    result = assert_run_gate_open("run-gate5", admin, checkpoint=checkpoint, gate_number=5)

    assert result is checkpoint


def test_stale_silver_merge_key_checkpoint_with_artifact_is_open():
    admin = AuthUser(
        uid=str(uuid.uuid4()),
        username="Admin",
        email="admin@astra.local",
        userType="Admin",
    )
    checkpoint = {
        "run_id": "run-merge-key",
        "status": "HITL_WAIT",
        "silver_merge_key_review_artifact": {"feeds": [{"entity": "claims", "merge_keys": ["claim_id"]}]},
    }

    result = assert_run_gate_open(
        "run-merge-key",
        admin,
        checkpoint=checkpoint,
        review_key="silver_merge_key_review",
    )

    assert result is checkpoint


def test_stale_gold_checkpoint_with_artifact_is_open():
    admin = AuthUser(
        uid=str(uuid.uuid4()),
        username="Admin",
        email="admin@astra.local",
        userType="Admin",
    )
    checkpoint = {
        "run_id": "run-gold",
        "status": "HITL_WAIT",
        "gold_review_artifact": {"items": [{"name": "claims_kpi", "review_status": "PENDING"}]},
    }

    result = assert_run_gate_open("run-gold", admin, checkpoint=checkpoint, review_key="gold_review")

    assert result is checkpoint
