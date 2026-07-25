from __future__ import annotations

import os
import re
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from api.routers.analytics_router import router as analytics_router
from api.routers.auth_router import router as auth_router
from api.routers.config_router import router as config_router
from api.routers.kpi_router import router as kpi_router
from api.routers.logs_router import router as logs_router
from api.routers.pipeline_router import router as pipeline_router
from api.routers.projects_router import router as projects_router
from api.routers.reviews_router import router as reviews_router
from api.routers.runs_router import router as runs_router
from api.auth import get_admin, get_current_user
from utilis.embedding_status import get_embedding_runtime_status
from utilis.logger import logger


PRODUCTION_ENV_VALUES = {"prod", "production"}
DEV_CORS_ORIGINS = (
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://localhost:3002",
    "http://127.0.0.1:3002",
    "https://ashy-mud-0abca9a00.7.azurestaticapps.net",
)
SECRET_TEXT_PATTERN = re.compile(
    r"([\"']?\b(password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key|private[_-]?key|connection[_-]?string)\b"
    r"[\"']?\s*[:=]\s*[\"']?)([^\"'\s,;}]+)",
    re.IGNORECASE,
)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _is_production() -> bool:
    for name in ("ATHENA_ENV", "APP_ENV", "ENVIRONMENT", "ENV"):
        if os.getenv(name, "").strip().lower() in PRODUCTION_ENV_VALUES:
            return True
    return False


def _csv_env(name: str, default: str) -> list[str]:
    raw = os.getenv(name, default)
    return [item.strip() for item in raw.split(",") if item.strip()]


def _api_docs_url(path: str) -> str | None:
    return path if _env_bool("ATHENA_ENABLE_API_DOCS", not _is_production()) else None


def _redact_text(value: str) -> str:
    return SECRET_TEXT_PATTERN.sub(lambda match: f"{match.group(1)}[redacted]", value)


def _redact_detail(value):
    if isinstance(value, dict):
        return {key: _redact_detail(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_redact_detail(item) for item in value]
    if isinstance(value, str):
        return _redact_text(value)
    return value


@asynccontextmanager
async def lifespan(app: FastAPI):
    embedding_status = get_embedding_runtime_status(probe_models=False)
    logger.info("Athena API service started")
    logger.info(
        "Embeddings status | blocked=%s enabled=%s ready=%s provider=%s",
        embedding_status.get("blocked"),
        embedding_status.get("env_enabled"),
        embedding_status.get("ready"),
        embedding_status.get("provider"),
    )
    try:
        from services.pipeline_runtime import mark_interrupted_background_runs_on_startup

        mark_interrupted_background_runs_on_startup()
    except Exception:
        logger.exception("Interrupted run recovery failed during startup")
    try:
        yield
    finally:
        logger.info("Athena API service stopped")


app = FastAPI(
    title="Athena Backend API",
    version="1.0.0",
    docs_url=_api_docs_url("/docs"),
    redoc_url=_api_docs_url("/redoc"),
    openapi_url=_api_docs_url("/openapi.json"),
    lifespan=lifespan,
)


def get_allowed_origins() -> list[str]:
    origins = os.getenv("ATHENA_CORS_ORIGINS")
    if origins is None:
        if _is_production():
            logger.warning("ATHENA_CORS_ORIGINS is unset in production; browser CORS access is disabled.")
            return []
        origins = ",".join(DEV_CORS_ORIGINS)
    allowed = [origin.strip().rstrip("/") for origin in origins.split(",") if origin.strip()]
    if _is_production() and "*" in allowed:
        logger.warning("Wildcard CORS origin ignored in production.")
        allowed = [origin for origin in allowed if origin != "*"]
    return allowed


def _cors_methods() -> list[str]:
    default = "GET,POST,PUT,PATCH,DELETE,OPTIONS" if _is_production() else "*"
    return _csv_env("ATHENA_CORS_ALLOW_METHODS", default)


def _cors_headers() -> list[str]:
    default = "Authorization,Content-Type,Accept,X-Requested-With" if _is_production() else "*"
    return _csv_env("ATHENA_CORS_ALLOW_HEADERS", default)


app.add_middleware(
    CORSMiddleware,
    allow_origins=get_allowed_origins(),
    allow_credentials=True,
    allow_methods=_cors_methods(),
    allow_headers=_cors_headers(),
)


@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("Referrer-Policy", os.getenv("ATHENA_REFERRER_POLICY", "no-referrer"))
    response.headers.setdefault("X-Frame-Options", os.getenv("ATHENA_FRAME_OPTIONS", "DENY"))
    if _is_production() and _env_bool("ATHENA_ENABLE_HSTS", True):
        response.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
    return response


@app.get("/health", tags=["Health"])
async def health_check():
    embedding_status = get_embedding_runtime_status(probe_models=False)
    try:
        from services.pipeline_runtime import background_capacity_snapshot

        background_capacity = background_capacity_snapshot()
    except Exception:
        logger.exception("Failed to collect background capacity")
        background_capacity = {"workers": 0, "active": 0, "available": 0}
    return {
        "status": "ok",
        "service": "athena-fastapi",
        "background_capacity": background_capacity,
        "embeddings": {
            **embedding_status,
            "enabled": embedding_status["env_enabled"],
            "mode": "blocked" if embedding_status["blocked"] else ("enabled" if embedding_status["env_enabled"] else "disabled"),
        },
    }


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    if _is_production():
        logger.error("Unhandled error | %s %s | Error type: %s", request.method, request.url.path, exc.__class__.__name__)
    else:
        logger.exception(
            "Unhandled error | %s %s | Error: %s",
            request.method,
            request.url.path,
            _redact_text(str(exc)),
        )

    return JSONResponse(
        status_code=500,
        content={
            "message": "Internal server error",
            "detail": "An unexpected error occurred.",
        },
    )


@app.exception_handler(HTTPException)
async def athena_http_exception_handler(
    request: Request, exc: HTTPException
) -> JSONResponse:
    if _is_production() and exc.status_code >= 500:
        detail = "Internal server error"
        response_detail: object = "An unexpected error occurred."
    else:
        detail = _redact_text(exc.detail) if isinstance(exc.detail, str) else "Request failed"
        response_detail = _redact_detail(exc.detail)

    return JSONResponse(
        status_code=exc.status_code,
        headers=getattr(exc, "headers", None),
        content={
            "message": detail,
            "detail": response_detail,
        },
    )


protected = [Depends(get_current_user)]
app.include_router(auth_router)
app.include_router(pipeline_router, dependencies=protected)
app.include_router(runs_router, dependencies=protected)
app.include_router(reviews_router, dependencies=protected)
app.include_router(kpi_router, dependencies=protected)
app.include_router(analytics_router, dependencies=protected)
app.include_router(config_router, dependencies=[Depends(get_admin)])
app.include_router(logs_router, dependencies=protected)
app.include_router(projects_router)


# Mount static files for React SPA (frontend)
# This serves the React build files on all routes not matched by API routers
static_path = os.path.join(os.path.dirname(__file__), "..", "..", "static")
if os.path.exists(static_path):
    app.mount("/", StaticFiles(directory=static_path, html=True), name="static")
