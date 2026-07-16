from __future__ import annotations

import os
import re
import threading
import uuid
from datetime import datetime, timedelta, timezone
from typing import Literal

import bcrypt
import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, ConfigDict, Field

from api.repositories.auth_repository import AuthRepository

UserType = Literal["Admin", "Client"]
bearer_scheme = HTTPBearer(auto_error=False)


class AuthUser(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    uid: str
    username: str
    email: str
    user_type: UserType = Field(alias="userType")
    is_active: bool = Field(default=True, alias="isActive")
    can_manage_accounts: bool = Field(default=False, alias="canManageAccounts")


class LoginRequest(BaseModel):
    email: str
    password: str


class CreateUserRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    username: str
    email: str
    password: str
    user_type: UserType = Field(alias="userType")


class UpdateUserRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    username: str | None = None
    email: str | None = None
    password: str | None = None
    user_type: UserType | None = Field(default=None, alias="userType")


class UpdateUserStatusRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    is_active: bool = Field(alias="isActive")


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "Bearer"
    expires_in: int
    user: AuthUser


class UsersResponse(BaseModel):
    users: list[AuthUser]


class UserResponse(BaseModel):
    user: AuthUser


class AuthService:
    def __init__(self, repository: AuthRepository | None = None) -> None:
        self.repository = repository or AuthRepository()
        self._ready = False
        self._ready_lock = threading.Lock()

    def login(self, email: str, password: str) -> LoginResponse:
        self._ensure_ready()
        user = self.repository.find_by_email(self._normalize_email(email))
        password_matches = False
        if user:
            try:
                password_matches = bcrypt.checkpw(
                    password.encode("utf-8"), user["password_hash"].encode("utf-8")
                )
            except ValueError:
                password_matches = False
        if not user or not password_matches:
            raise HTTPException(status_code=401, detail="Invalid email or password")
        if not user["is_active"]:
            raise HTTPException(status_code=401, detail="Account is disabled")

        expires_in = self._token_ttl_seconds()
        now = datetime.now(timezone.utc)
        payload = {
            "sub": user["uid"],
            "ver": user["token_version"],
            "iat": now,
            "exp": now + timedelta(seconds=expires_in),
            "iss": self._issuer,
            "aud": self._audience,
        }
        token = jwt.encode(payload, self._jwt_secret(), algorithm="HS256")
        return LoginResponse(
            access_token=token,
            expires_in=expires_in,
            user=self._public_user(user),
        )

    def authenticate_token(self, token: str) -> AuthUser:
        self._ensure_ready()
        try:
            payload = jwt.decode(
                token,
                self._jwt_secret(),
                algorithms=["HS256"],
                issuer=self._issuer,
                audience=self._audience,
            )
        except jwt.PyJWTError as exc:
            raise HTTPException(status_code=401, detail="Invalid or expired session") from exc

        uid = str(payload.get("sub") or "")
        user = self.repository.find_by_uid(uid) if uid else None
        if not user or not user["is_active"] or int(payload.get("ver", -1)) != int(user["token_version"]):
            raise HTTPException(status_code=401, detail="Invalid or expired session")
        return self._public_user(user)

    def list_users(self, requester: AuthUser) -> list[AuthUser]:
        self._assert_primary_admin(requester)
        return [self._public_user(user) for user in self.repository.list_users()]

    def create_user(self, request: CreateUserRequest, requester: AuthUser) -> AuthUser:
        self._assert_primary_admin(requester)
        username = self._validate_username(request.username)
        email = self._normalize_email(request.email)
        password = self._validate_password(request.password)
        if self.repository.find_by_email(email):
            raise HTTPException(status_code=409, detail="Email is already registered")
        user = self.repository.create_user(
            uid=str(uuid.uuid4()),
            username=username,
            email=email,
            password_hash=self._hash_password(password),
            user_type=request.user_type,
        )
        return self._public_user(user)

    def update_user(self, uid: str, request: UpdateUserRequest, requester: AuthUser) -> AuthUser:
        self._assert_primary_admin(requester)
        current = self.repository.find_by_uid(uid)
        if not current:
            raise HTTPException(status_code=404, detail="User not found")

        username = self._validate_username(request.username) if request.username is not None else current["username"]
        email = self._normalize_email(request.email) if request.email is not None else current["email"]
        user_type = request.user_type or current["user_type"]
        if self._is_primary_admin(self._public_user(current)):
            if email != self._primary_admin_email or user_type != "Admin":
                raise HTTPException(status_code=403, detail="Primary admin identity and role cannot be changed")
        duplicate = self.repository.find_by_email(email)
        if duplicate and duplicate["uid"] != uid:
            raise HTTPException(status_code=409, detail="Email is already registered")
        password_hash = self._hash_password(self._validate_password(request.password)) if request.password else None
        updated = self.repository.update_user(
            uid,
            username=username,
            email=email,
            user_type=user_type,
            password_hash=password_hash,
        )
        if not updated:
            raise HTTPException(status_code=404, detail="User not found")
        return self._public_user(updated)

    def set_user_active(self, uid: str, is_active: bool, requester: AuthUser) -> AuthUser:
        self._assert_primary_admin(requester)
        current = self.repository.find_by_uid(uid)
        if not current:
            raise HTTPException(status_code=404, detail="User not found")
        if self._is_primary_admin(self._public_user(current)) and not is_active:
            raise HTTPException(status_code=403, detail="Primary admin cannot be disabled")
        updated = self.repository.set_active(uid, is_active)
        if not updated:
            raise HTTPException(status_code=404, detail="User not found")
        return self._public_user(updated)

    def delete_user(self, uid: str, requester: AuthUser) -> None:
        self._assert_primary_admin(requester)
        current = self.repository.find_by_uid(uid)
        if not current:
            raise HTTPException(status_code=404, detail="User not found")
        if self._is_primary_admin(self._public_user(current)):
            raise HTTPException(status_code=403, detail="Primary admin cannot be deleted")
        self.repository.delete_user(uid)

    def _ensure_ready(self) -> None:
        if self._ready:
            return
        with self._ready_lock:
            if self._ready:
                return
            self.repository.ensure_table()
            email = os.getenv("ASTRA_AUTH_EMAIL", "").strip()
            username = os.getenv("ASTRA_AUTH_USERNAME", "").strip()
            password = os.getenv("ASTRA_AUTH_PASSWORD", "")
            if not all((email, username, password)):
                raise RuntimeError(
                    "Set ASTRA_AUTH_EMAIL, ASTRA_AUTH_USERNAME, and ASTRA_AUTH_PASSWORD before using authentication"
                )
            normalized_email = self._normalize_email(email)
            if not self.repository.find_by_email(normalized_email):
                try:
                    self.repository.create_user(
                        uid=str(uuid.uuid4()),
                        username=self._validate_username(username),
                        email=normalized_email,
                        # The bootstrap credential is operator-controlled and may be a legacy password.
                        password_hash=self._hash_password(password),
                        user_type="Admin",
                    )
                except Exception:
                    # ponytail: tolerate two app workers racing to seed the same unique admin.
                    if not self.repository.find_by_email(normalized_email):
                        raise
            self._jwt_secret()
            self._ready = True

    def _public_user(self, user: dict) -> AuthUser:
        auth_user = AuthUser(
            uid=str(user["uid"]),
            username=str(user["username"]),
            email=str(user["email"]),
            userType=user["user_type"],
            isActive=bool(user["is_active"]),
        )
        auth_user.can_manage_accounts = self._is_primary_admin(auth_user)
        return auth_user

    @staticmethod
    def _validate_username(value: str) -> str:
        username = value.strip()
        if len(username) < 2 or not re.search(r"[A-Za-z]", username):
            raise HTTPException(status_code=400, detail="User name must contain at least two characters and one letter")
        if len(username) > 255:
            raise HTTPException(status_code=400, detail="User name is too long")
        return username

    @staticmethod
    def _normalize_email(value: str) -> str:
        email = value.strip().lower()
        if len(email) > 255 or not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", email):
            raise HTTPException(status_code=400, detail="Email is invalid")
        return email

    @staticmethod
    def _validate_password(value: str) -> str:
        if len(value) < 12 or not re.search(r"[A-Za-z]", value) or not re.search(r"\d", value) or not re.search(r"[^A-Za-z0-9]", value):
            raise HTTPException(
                status_code=400,
                detail="Password must be at least 12 characters and include a letter, number, and special character",
            )
        if len(value.encode("utf-8")) > 72:
            raise HTTPException(status_code=400, detail="Password must be at most 72 bytes")
        return value

    @staticmethod
    def _hash_password(value: str) -> str:
        return bcrypt.hashpw(value.encode("utf-8"), bcrypt.gensalt(rounds=12)).decode("utf-8")

    def _assert_primary_admin(self, user: AuthUser) -> None:
        if not self._is_primary_admin(user):
            raise HTTPException(status_code=403, detail="Only the primary admin can manage accounts")

    def _is_primary_admin(self, user: AuthUser) -> bool:
        return user.user_type == "Admin" and user.email.lower() == self._primary_admin_email

    @property
    def _primary_admin_email(self) -> str:
        return self._normalize_email(os.getenv("ASTRA_AUTH_EMAIL", ""))

    @property
    def _issuer(self) -> str:
        return os.getenv("ASTRA_JWT_ISSUER", "astra-api")

    @property
    def _audience(self) -> str:
        return os.getenv("ASTRA_JWT_AUDIENCE", "astra-frontend")

    @staticmethod
    def _token_ttl_seconds() -> int:
        try:
            ttl = int(os.getenv("ASTRA_JWT_EXPIRES_IN_SECONDS", "3600"))
        except ValueError as exc:
            raise RuntimeError("ASTRA_JWT_EXPIRES_IN_SECONDS must be an integer") from exc
        if not 300 <= ttl <= 86400:
            raise RuntimeError("ASTRA_JWT_EXPIRES_IN_SECONDS must be between 300 and 86400")
        return ttl

    @staticmethod
    def _jwt_secret() -> str:
        secret = os.getenv("ASTRA_JWT_SECRET", "")
        if len(secret.encode("utf-8")) < 32:
            raise RuntimeError("ASTRA_JWT_SECRET must contain at least 32 bytes")
        return secret


_auth_service = AuthService()


def get_auth_service() -> AuthService:
    return _auth_service


def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
    service: AuthService = Depends(get_auth_service),
) -> AuthUser:
    if not credentials or credentials.scheme.lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return service.authenticate_token(credentials.credentials)


def get_primary_admin(
    user: AuthUser = Depends(get_current_user),
    service: AuthService = Depends(get_auth_service),
) -> AuthUser:
    service._assert_primary_admin(user)
    return user


def get_admin(user: AuthUser = Depends(get_current_user)) -> AuthUser:
    if user.user_type != "Admin":
        raise HTTPException(status_code=403, detail="Administrator access required")
    return user
