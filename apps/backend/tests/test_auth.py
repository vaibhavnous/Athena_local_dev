from __future__ import annotations

import uuid

import pytest
from fastapi import HTTPException

from api.auth import AuthService, AuthUser, CreateUserRequest, UpdateUserRequest, assert_run_access, get_current_user


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


def test_auth_can_be_temporarily_disabled(monkeypatch):
    monkeypatch.setenv("ATHENA_AUTH_DISABLED", "true")

    user = get_current_user(credentials=None, service=None)

    assert user.uid == "temporary-auth-disabled-user"
    assert user.user_type == "Client"
    assert user.can_manage_accounts is False


def test_login_issues_token_that_resolves_current_user(auth):
    service, _ = auth

    session = service.login(" ADMIN@ASTRA.LOCAL ", "AdminPass!234")
    current_user = service.authenticate_token(session.access_token)

    assert current_user.email == "admin@astra.local"
    assert current_user.user_type == "Admin"
    assert session.expires_in == 3600


def test_active_session_can_be_renewed(auth):
    service, _ = auth
    session = service.login("admin@astra.local", "AdminPass!234")
    current_user = service.authenticate_token(session.access_token)

    renewed = service.refresh(current_user)

    assert renewed.expires_in == 3600
    assert service.authenticate_token(renewed.access_token) == current_user


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


def test_existing_account_email_cannot_change(auth):
    service, repository = auth
    admin = service.login("admin@astra.local", "AdminPass!234").user
    client = service.create_user(
        CreateUserRequest(
            username="Client User",
            email="client@example.com",
            password="ClientPass!234",
            userType="Client",
        ),
        admin,
    )

    with pytest.raises(HTTPException) as exc:
        service.update_user(client.uid, UpdateUserRequest(email="new-client@example.com"), admin)

    assert exc.value.status_code == 400
    assert repository.find_by_uid(client.uid)["email"] == "client@example.com"


def test_weak_primary_admin_password_cannot_bootstrap(monkeypatch):
    monkeypatch.setenv("ASTRA_AUTH_EMAIL", "admin@astra.local")
    monkeypatch.setenv("ASTRA_AUTH_USERNAME", "Primary Admin")
    monkeypatch.setenv("ASTRA_AUTH_PASSWORD", "admin123")
    monkeypatch.setenv("ASTRA_JWT_SECRET", "test-secret-that-is-at-least-32-bytes-long")
    service = AuthService(FakeAuthRepository())

    with pytest.raises(HTTPException) as exc:
        service.login("admin@astra.local", "admin123")

    assert exc.value.status_code == 400


def test_existing_primary_admin_password_is_not_reset_from_env(monkeypatch):
    monkeypatch.setenv("ASTRA_AUTH_EMAIL", "admin@astra.local")
    monkeypatch.setenv("ASTRA_AUTH_USERNAME", "Primary Admin")
    monkeypatch.setenv("ASTRA_AUTH_PASSWORD", "admin123")
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

    session = service.login("admin@astra.local", "OldAdminPass!234")

    assert session.user.can_manage_accounts is True
    with pytest.raises(HTTPException) as exc:
        service.login("admin@astra.local", "admin123")
    assert exc.value.status_code == 401


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
