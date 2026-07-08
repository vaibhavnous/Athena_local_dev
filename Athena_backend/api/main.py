from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from api.routers.analytics_router import router as analytics_router
from api.routers.config_router import router as config_router
from api.routers.kpi_router import router as kpi_router
from api.routers.logs_router import router as logs_router
from api.routers.pipeline_router import router as pipeline_router
from api.routers.reviews_router import router as reviews_router
from api.routers.runs_router import router as runs_router
from utilis.embedding_status import get_embedding_runtime_status
from utilis.logger import logger


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
        yield
    finally:
        logger.info("Athena API service stopped")


app = FastAPI(
    title="Athena Backend API",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan,
)


def get_allowed_origins() -> list[str]:
    origins = os.getenv(
        "ATHENA_CORS_ORIGINS",
        "http://localhost:3000,http://127.0.0.1:3000,https://ashy-mud-0abca9a00.7.azurestaticapps.net",
    )
    return [origin.strip().rstrip("/") for origin in origins.split(",") if origin.strip()]


app.add_middleware(
    CORSMiddleware,
    allow_origins=get_allowed_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health", tags=["Health"])
async def health_check():
    embedding_status = get_embedding_runtime_status(probe_models=False)
    return {
        "status": "ok",
        "service": "athena-fastapi",
        "embeddings": {
            **embedding_status,
            "enabled": embedding_status["env_enabled"],
            "mode": "blocked" if embedding_status["blocked"] else ("enabled" if embedding_status["env_enabled"] else "disabled"),
        },
    }


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.exception(
        "Unhandled error | %s %s | Error: %s",
        request.method,
        request.url.path,
        str(exc),
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
    detail = exc.detail if isinstance(exc.detail, str) else "Request failed"

    return JSONResponse(
        status_code=exc.status_code,
        headers=getattr(exc, "headers", None),
        content={
            "message": detail,
            "detail": exc.detail,
        },
    )


app.include_router(pipeline_router)
app.include_router(runs_router)
app.include_router(reviews_router)
app.include_router(kpi_router)
app.include_router(analytics_router)
app.include_router(config_router)
app.include_router(logs_router)


# Mount static files for React SPA (frontend)
# This serves the React build files on all routes not matched by API routers
static_path = os.path.join(os.path.dirname(__file__), "..", "..", "static")
if os.path.exists(static_path):
    app.mount("/", StaticFiles(directory=static_path, html=True), name="static")
