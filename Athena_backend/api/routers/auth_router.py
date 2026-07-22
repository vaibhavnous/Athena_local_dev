from fastapi import APIRouter, Depends, Response, status

from api.auth import (
    AuthService,
    AuthUser,
    CreateUserRequest,
    LoginRequest,
    LoginResponse,
    UpdateUserRequest,
    UpdateUserStatusRequest,
    UserResponse,
    UsersResponse,
    get_auth_service,
    get_current_user,
    get_primary_admin,
)

router = APIRouter(prefix="/auth", tags=["Authentication"])


@router.post("/login", response_model=LoginResponse)
def login(request: LoginRequest, service: AuthService = Depends(get_auth_service)) -> LoginResponse:
    return service.login(request.email, request.password)


@router.get("/me", response_model=AuthUser)
def me(user: AuthUser = Depends(get_current_user)) -> AuthUser:
    return user


@router.get("/users", response_model=UsersResponse)
def users(
    admin: AuthUser = Depends(get_primary_admin),
    service: AuthService = Depends(get_auth_service),
) -> UsersResponse:
    return UsersResponse(users=service.list_users(admin))


@router.post("/users", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
def create_user(
    request: CreateUserRequest,
    admin: AuthUser = Depends(get_primary_admin),
    service: AuthService = Depends(get_auth_service),
) -> UserResponse:
    return UserResponse(user=service.create_user(request, admin))


@router.patch("/users/{uid}", response_model=UserResponse)
def update_user(
    uid: str,
    request: UpdateUserRequest,
    admin: AuthUser = Depends(get_primary_admin),
    service: AuthService = Depends(get_auth_service),
) -> UserResponse:
    return UserResponse(user=service.update_user(uid, request, admin))


@router.patch("/users/{uid}/status", response_model=UserResponse)
def update_user_status(
    uid: str,
    request: UpdateUserStatusRequest,
    admin: AuthUser = Depends(get_primary_admin),
    service: AuthService = Depends(get_auth_service),
) -> UserResponse:
    return UserResponse(user=service.set_user_active(uid, request.is_active, admin))


@router.delete("/users/{uid}", status_code=status.HTTP_204_NO_CONTENT, response_class=Response)
def delete_user(
    uid: str,
    admin: AuthUser = Depends(get_primary_admin),
    service: AuthService = Depends(get_auth_service),
) -> Response:
    service.delete_user(uid, admin)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
