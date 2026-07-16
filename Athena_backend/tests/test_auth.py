from __future__ import annotations

import uuid

import pytest
from fastapi import HTTPException

from api.auth import AuthService, AuthUser, CreateUserRequest


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
