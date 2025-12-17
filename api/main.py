"""
TRR Backend API - FastAPI application.

Provides endpoints for:
- Browsing shows, seasons, episodes, and cast
- Submitting surveys with instant live results
- Episode discussion threads, posts, and reactions
- Direct messages (1:1 DMs)
- Real-time WebSocket updates
"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routers import discussions, dms, shows, surveys, ws
from api.realtime.broker import init_broker, shutdown_broker

logger = logging.getLogger(__name__)


def get_cors_origins() -> list[str]:
    """
    Get CORS allowed origins from environment.
    Set CORS_ALLOW_ORIGINS as comma-separated list of origins.
    Example: CORS_ALLOW_ORIGINS=https://therealityreport.com,https://app.therealityreport.com
    """
    origins_str = os.getenv("CORS_ALLOW_ORIGINS", "")
    if not origins_str:
        return []
    return [origin.strip() for origin in origins_str.split(",") if origin.strip()]


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler for startup/shutdown events."""
    # Startup
    logger.info("Starting up TRR Backend API...")
    await init_broker()
    yield
    # Shutdown
    logger.info("Shutting down TRR Backend API...")
    await shutdown_broker()


app = FastAPI(
    title="The Reality Report API",
    description="Backend API for The Reality Report - reality TV data and surveys",
    version="0.1.0",
    lifespan=lifespan,
)

# CORS configuration
# Set CORS_ALLOW_ORIGINS env var with comma-separated origins for production
# If no origins configured, allows all origins but disables credentials (safer default)
cors_origins = get_cors_origins()
allow_credentials = len(cors_origins) > 0  # Only allow credentials with explicit origins

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins if cors_origins else ["*"],
    allow_credentials=allow_credentials,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# Include routers
app.include_router(shows.router, prefix="/api/v1")
app.include_router(surveys.router, prefix="/api/v1")
app.include_router(discussions.router, prefix="/api/v1")
app.include_router(dms.router, prefix="/api/v1")
app.include_router(ws.router, prefix="/api/v1")


@app.get("/")
def root():
    """Health check endpoint."""
    return {"status": "ok", "service": "trr-backend"}


@app.get("/health")
def health():
    """Health check endpoint."""
    return {"status": "healthy"}
