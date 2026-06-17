from __future__ import annotations

import os

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.requests import Request
from fastapi.responses import JSONResponse

from api.routers import (
    analytics_router,
    config_router,
    kpi_router,
    logs_router,
    pipeline_router,
    reviews_router,
    runs_router,
)
from utilis.logger import logger


app = FastAPI(title="Athena Backend API", version="1.0.0")

allowed_origins = [
    origin.strip()
    for origin in os.getenv(
        "ATHENA_CORS_ORIGINS",
        "http://localhost:3000,http://127.0.0.1:3000,http://localhost:3001,http://127.0.0.1:3001",
    ).split(",")
    if origin.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.exception("Unhandled API error on %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=500,
        content={
            "message": "Athena API failed while handling the request.",
            "detail": str(exc),
        },
    )


@app.exception_handler(HTTPException)
async def athena_http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    detail = exc.detail
    message = detail if isinstance(detail, str) else "Athena API request failed."
    return JSONResponse(
        status_code=exc.status_code,
        headers=getattr(exc, "headers", None),
        content={"message": message, "detail": detail},
    )


app.include_router(pipeline_router)
app.include_router(runs_router)
app.include_router(reviews_router)
app.include_router(kpi_router)
app.include_router(analytics_router)
app.include_router(config_router)
app.include_router(logs_router)
