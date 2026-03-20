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

import asyncio
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from urllib.parse import urlparse

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware

from api.realtime.broker import init_broker, shutdown_broker
from trr_backend.observability import (
    CONTENT_TYPE_LATEST,
    bind_trace_id,
    configure_runtime_observability,
    metrics_available,
    record_http_request,
    render_metrics,
    reset_trace_id,
)

configure_runtime_observability(service_name="trr-backend-api")

logger = logging.getLogger(__name__)


def _env_flag(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw not in {"0", "false", "no", "off"}


def _cast_screentime_stale_sweeper_enabled() -> bool:
    return _env_flag("CAST_SCREENTIME_STALE_SWEEPER_ENABLED", False)


def _cast_screentime_stale_sweeper_interval_seconds() -> int:
    raw = (os.getenv("CAST_SCREENTIME_STALE_SWEEPER_INTERVAL_SECONDS") or "").strip()
    try:
        if raw:
            return max(int(raw), 60)
    except ValueError:
        return 300
    return 300


async def _run_cast_screentime_stale_sweeper(stop_event: asyncio.Event) -> None:
    from api.routers import admin_cast_screentime

    interval_seconds = _cast_screentime_stale_sweeper_interval_seconds()
    while not stop_event.is_set():
        try:
            reconciled = await asyncio.to_thread(admin_cast_screentime.reconcile_stale_runs_once)
            if reconciled:
                logger.warning(
                    "[cast-screentime] reconciled stale runs count=%s stale_after_seconds=%s",
                    len(reconciled),
                    admin_cast_screentime._stale_after_seconds(),
                )
        except Exception:
            logger.exception("[cast-screentime] stale-run sweeper failed")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
        except TimeoutError:
            continue


def _validate_startup_config() -> None:
    """Validate high-impact service env configuration with actionable logs."""
    screenalytics_api_url = (os.getenv("SCREENALYTICS_API_URL") or "").strip()
    admin_shared_secret = (os.getenv("TRR_INTERNAL_ADMIN_SHARED_SECRET") or "").strip()
    service_token = (os.getenv("SCREENALYTICS_SERVICE_TOKEN") or "").strip()

    if screenalytics_api_url:
        parsed = urlparse(screenalytics_api_url)
        if not parsed.scheme or not parsed.netloc:
            logger.warning(
                "[startup-config] SCREENALYTICS_API_URL looks malformed: %s; only legacy outbound Screenalytics HTTP "
                "flows depend on this setting",
                screenalytics_api_url,
            )
    else:
        logger.info(
            "[startup-config] SCREENALYTICS_API_URL not set; only legacy outbound Screenalytics HTTP flows are "
            "disabled. Admin image-analysis stays on the backend-owned vision runtime."
        )

    if not admin_shared_secret:
        logger.warning("[startup-config] TRR_INTERNAL_ADMIN_SHARED_SECRET missing; admin proxy auth may fail")
    if not service_token:
        logger.warning(
            "[startup-config] SCREENALYTICS_SERVICE_TOKEN missing; only /api/v1/screenalytics auth-protected "
            "requests are affected and backend startup continues normally"
        )


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
    _validate_startup_config()
    await init_broker()
    stale_sweeper_stop: asyncio.Event | None = None
    stale_sweeper_task: asyncio.Task[None] | None = None
    if _cast_screentime_stale_sweeper_enabled():
        stale_sweeper_stop = asyncio.Event()
        stale_sweeper_task = asyncio.create_task(_run_cast_screentime_stale_sweeper(stale_sweeper_stop))
        logger.info(
            "[startup-config] cast screentime stale-run sweeper enabled interval_seconds=%s",
            _cast_screentime_stale_sweeper_interval_seconds(),
        )
    yield
    # Shutdown
    logger.info("Shutting down TRR Backend API...")
    if stale_sweeper_stop is not None:
        stale_sweeper_stop.set()
    if stale_sweeper_task is not None:
        try:
            await asyncio.wait_for(stale_sweeper_task, timeout=5)
        except TimeoutError:
            stale_sweeper_task.cancel()
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


@app.middleware("http")
async def observability_middleware(request: Request, call_next):
    incoming_trace = (
        str(request.headers.get("x-trace-id") or "").strip() or str(request.headers.get("x-request-id") or "").strip()
    )
    trace_id = incoming_trace or uuid.uuid4().hex
    trace_token = bind_trace_id(trace_id)
    request.state.trace_id = trace_id
    start = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        elapsed = time.perf_counter() - start
        route = getattr(request.scope.get("route"), "path", request.url.path)
        record_http_request(request.method, route, 500, elapsed)
        reset_trace_id(trace_token)
        raise
    elapsed = time.perf_counter() - start
    route = getattr(request.scope.get("route"), "path", request.url.path)
    record_http_request(request.method, route, response.status_code, elapsed)
    response.headers.setdefault("x-trace-id", trace_id)
    response.headers.setdefault("x-request-id", trace_id)
    reset_trace_id(trace_token)
    return response


# Include routers
from api.routers import (  # noqa: E402
    admin_asset_batch_jobs,
    admin_asset_flags,
    admin_brands,
    admin_cast,
    admin_cast_photos,
    admin_cast_screentime,
    admin_fandom_sync,
    admin_image_counts,
    admin_media_assets,
    admin_nbcumv,
    admin_operations,
    admin_person_images,
    admin_scrape,
    admin_show_bravo,
    admin_show_icons,
    admin_show_images,
    admin_show_links,
    admin_show_news,
    admin_show_roles,
    admin_show_sync,
    admin_socialblade,
    discussions,
    dms,
    screenalytics,
    screenalytics_runs_v2,
    shows,
    socials,
    surveys,
    ws,
)

app.include_router(shows.router, prefix="/api/v1")
app.include_router(surveys.router, prefix="/api/v1")
app.include_router(discussions.router, prefix="/api/v1")
app.include_router(dms.router, prefix="/api/v1")
app.include_router(ws.router, prefix="/api/v1")
app.include_router(screenalytics.router, prefix="/api/v1")
app.include_router(screenalytics_runs_v2.router, prefix="/api/v1")
app.include_router(admin_asset_flags.router, prefix="/api/v1")
app.include_router(admin_asset_batch_jobs.router, prefix="/api/v1")
app.include_router(admin_cast.router, prefix="/api/v1")
app.include_router(admin_cast_screentime.router, prefix="/api/v1")
app.include_router(admin_cast_photos.router, prefix="/api/v1")
app.include_router(admin_brands.router, prefix="/api/v1")
app.include_router(admin_fandom_sync.router, prefix="/api/v1")
app.include_router(admin_image_counts.router, prefix="/api/v1")
app.include_router(admin_media_assets.router, prefix="/api/v1")
app.include_router(admin_operations.router, prefix="/api/v1")
app.include_router(admin_show_icons.router, prefix="/api/v1")
app.include_router(admin_show_links.router, prefix="/api/v1")
app.include_router(admin_show_links.fandom_router, prefix="/api/v1")
app.include_router(admin_show_roles.router, prefix="/api/v1")
app.include_router(admin_person_images.router, prefix="/api/v1")
app.include_router(admin_nbcumv.router, prefix="/api/v1")
app.include_router(admin_show_bravo.router, prefix="/api/v1")
app.include_router(admin_show_images.router, prefix="/api/v1")
app.include_router(admin_show_news.router, prefix="/api/v1")
app.include_router(admin_scrape.router, prefix="/api/v1")
app.include_router(admin_socialblade.router, prefix="/api/v1")
app.include_router(admin_show_sync.router, prefix="/api/v1")
app.include_router(socials.router, prefix="/api/v1")


@app.get("/")
def root():
    """Health check endpoint."""
    return {"status": "ok", "service": "trr-backend"}


@app.get("/health")
def health():
    """Health check endpoint."""
    return {"status": "healthy"}


@app.get("/metrics")
def metrics() -> Response:
    """Prometheus metrics endpoint."""
    if not metrics_available():
        return Response(status_code=404, content="metrics_unavailable\n", media_type="text/plain")
    return Response(content=render_metrics(), media_type=CONTENT_TYPE_LATEST)
