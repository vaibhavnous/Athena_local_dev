from __future__ import annotations

import os
import logging

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# ✅ FIXED imports (adjust based on your real structure)
from routers import (
    analytics_router,
    config_router,
    kpi_router,
    logs_router,
    pipeline_router,
    reviews_router,
    runs_router,
)

from utils.logger import logger  # ✅ fixed typo (utilis → utils)


# ✅ App initialization
app = FastAPI(
    title="Athena Backend API",
    version="1.0.0",
    docs_url="/docs",                 # keep explicit
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)


# ✅ CORS Configuration (safe + flexible)
def get_allowed_origins() -> list[str]:
    origins = os.getenv(
        "ATHENA_CORS_ORIGINS",
        "http://localhost:3000,http://127.0.0.1:3000"
    )
    return [origin.strip() for origin in origins.split(",") if origin.strip()]


app.add_middleware(
    CORSMiddleware,
    allow_origins=get_allowed_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ✅ Health Check (critical for deployment platforms)
@app.get("/health", tags=["Health"])
async def health_check():
    return {"status": "ok"}


# ✅ Global Exception Handler (safe for production)
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
            "detail": "An unexpected error occurred."
        },
    )


# ✅ HTTP Exception Handler (clean response)
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


# ✅ Router registration (grouped, predictable, scalable)
app.include_router(pipeline_router, prefix="/pipeline", tags=["Pipeline"])
app.include_router(runs_router, prefix="/runs", tags=["Runs"])
app.include_router(reviews_router, prefix="/reviews", tags=["Reviews"])
app.include_router(kpi_router, prefix="/kpi", tags=["KPI"])
app.include_router(analytics_router, prefix="/analytics", tags=["Analytics"])
app.include_router(config_router, prefix="/config", tags=["Config"])
app.include_router(logs_router, prefix="/logs", tags=["Logs"])


# ✅ Optional: Startup / Shutdown hooks (safe placeholders)
@app.on_event("startup")
async def startup_event():
    logger.info("Athena API service started")


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Athena API service stopped")
