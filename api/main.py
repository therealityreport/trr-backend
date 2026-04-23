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

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.realtime.broker import init_broker, shutdown_broker
from trr_backend.db import pg
from trr_backend.db.connection import log_database_resolution_summary, resolve_database_url_candidate_details
from trr_backend.db.pg import DatabaseServiceUnavailableError, database_service_unavailable_detail
from trr_backend.middleware.request_timeout import RequestTimeoutMiddleware
from trr_backend.observability import (
    CONTENT_TYPE_LATEST,
    bind_trace_id,
    configure_runtime_observability,
    metrics_available,
    record_http_request,
    render_metrics,
    reset_trace_id,
)
from trr_backend.security.jwt import (
    describe_supabase_jwt_context,
    expected_supabase_issuer,
    expected_supabase_project_ref,
)

configure_runtime_observability(service_name="trr-backend-api")

logger = logging.getLogger(__name__)

_LOCAL_RUNTIME_MARKERS = frozenset({"local", "dev", "development", "test"})


def _env_flag(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw not in {"0", "false", "no", "off"}


def _is_local_or_dev_runtime() -> bool:
    if os.getenv("CI") or os.getenv("GITHUB_ACTIONS"):
        return True
    values = {
        str(os.getenv("APP_ENV") or "").strip().lower(),
        str(os.getenv("ENVIRONMENT") or "").strip().lower(),
        str(os.getenv("TRR_ENV") or "").strip().lower(),
        str(os.getenv("TRR_ENVIRONMENT") or "").strip().lower(),
        str(os.getenv("PYTHON_ENV") or "").strip().lower(),
    }
    if values & _LOCAL_RUNTIME_MARKERS:
        return True
    raw_local = str(os.getenv("TRR_LOCAL_DEV") or "").strip().lower()
    return raw_local in {"1", "true", "yes", "on"}


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
    admin_shared_secret = (os.getenv("TRR_INTERNAL_ADMIN_SHARED_SECRET") or "").strip()
    supabase_jwt_secret = (os.getenv("SUPABASE_JWT_SECRET") or "").strip()
    log_database_resolution_summary()
    winner = next(iter(resolve_database_url_candidate_details()), None)

    if not winner:
        raise RuntimeError(
            "No database URL candidates available.\n"
            "Set TRR_DB_URL to your Supabase session-pooler connection string.\n"
            "Optionally set TRR_DB_FALLBACK_URL for controlled failover."
        )

    winner_connection_class = str(winner.get("connection_class") or "")
    winner_source = str(winner.get("source") or "")
    is_local = _is_local_or_dev_runtime()

    # Log structured startup fields
    logger.info(
        "[startup-config] db_winner source=%s connection_class=%s is_local=%s",
        winner_source,
        winner_connection_class,
        is_local,
    )

    # Fail-fast for invalid runtime lanes — only session and local are allowed.
    invalid_lanes = {"direct", "unknown", "other", "pooler", "transaction"}

    if winner_connection_class in invalid_lanes:
        raise RuntimeError(
            f"Invalid runtime connection lane: {winner_connection_class}\n"
            f"Only session-mode pooler (:5432) and local Postgres are supported.\n"
            f"Use session-mode pooler (:5432) via TRR_DB_URL.\n"
            f"Winner source: {winner_source}"
        )

    if winner_connection_class == "session":
        raw_minconn = (os.getenv("TRR_DB_POOL_MINCONN") or "").strip()
        raw_maxconn = (os.getenv("TRR_DB_POOL_MAXCONN") or "").strip()
        try:
            minconn = int(raw_minconn) if raw_minconn else None
        except ValueError:
            minconn = None
        try:
            maxconn = int(raw_maxconn) if raw_maxconn else None
        except ValueError:
            maxconn = None
        if (minconn is not None and minconn > 4) or (maxconn is not None and maxconn > 16):
            logger.warning(
                "[startup-config] oversized_session_pool_override detected for "
                "Supavisor session mode: TRR_DB_POOL_MINCONN=%s TRR_DB_POOL_MAXCONN=%s",
                raw_minconn or "<unset>",
                raw_maxconn or "<unset>",
            )
        if _env_flag("SOCIAL_QUEUE_ENABLED", False):
            logger.warning(
                "[startup-config] session_pooler_with_social_queue_enabled; keep "
                "local worker lanes and DB pool sizing conservative when using "
                "pooler.supabase.com:5432"
            )

    missing_required: list[str] = []
    if not admin_shared_secret:
        missing_required.append("TRR_INTERNAL_ADMIN_SHARED_SECRET")
    if not supabase_jwt_secret:
        missing_required.append("SUPABASE_JWT_SECRET")

    if missing_required and _is_local_or_dev_runtime():
        logger.warning(
            "[startup-config] local/dev runtime missing auth env(s): %s",
            ", ".join(missing_required),
        )
    elif missing_required:
        raise RuntimeError("Missing required auth env(s) for deployed runtime: " + ", ".join(missing_required))

    supabase_project_ref = expected_supabase_project_ref()
    supabase_issuer = expected_supabase_issuer()
    if supabase_project_ref and supabase_issuer:
        logger.info(
            "[startup-config] supabase_jwt project_ref=%s issuer=%s",
            supabase_project_ref,
            supabase_issuer,
        )
    for warning in describe_supabase_jwt_context():
        logger.warning("[startup-config] supabase_jwt %s", warning)


def _prewarm_database_pool() -> None:
    try:
        with pg.db_read_connection(label="startup-prewarm") as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        logger.info("[startup-config] database pool prewarmed")
    except Exception:
        logger.exception("[startup-config] database pool prewarm failed")


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
    await asyncio.to_thread(_prewarm_database_pool)
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

app.add_middleware(RequestTimeoutMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins if cors_origins else ["*"],
    allow_credentials=allow_credentials,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)


@app.exception_handler(DatabaseServiceUnavailableError)
async def database_service_unavailable_exception_handler(
    request: Request, exc: DatabaseServiceUnavailableError
) -> JSONResponse:
    payload = database_service_unavailable_detail(exc)
    logger.warning(
        "[api] database_service_unavailable path=%s reason=%s",
        request.url.path,
        payload.get("reason"),
    )
    return JSONResponse(status_code=503, content={"detail": payload})


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
    admin_bravotv_images,
    admin_cast,
    admin_cast_photos,
    admin_cast_screentime,
    admin_covered_shows,
    admin_face_references,
    admin_fandom_sync,
    admin_image_counts,
    admin_media_assets,
    admin_nbcumv,
    admin_networks_streaming_reads,
    admin_operations,
    admin_people_reads,
    admin_person_images,
    admin_person_profile,
    admin_recent_people,
    admin_reddit_reads,
    admin_scrape,
    admin_show_bravo,
    admin_show_icons,
    admin_show_images,
    admin_show_links,
    admin_show_news,
    admin_show_reads,
    admin_show_roles,
    admin_show_sync,
    admin_social_posts,
    admin_socialblade,
    discussions,
    dms,
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
app.include_router(admin_asset_flags.router, prefix="/api/v1")
app.include_router(admin_asset_batch_jobs.router, prefix="/api/v1")
app.include_router(admin_bravotv_images.router, prefix="/api/v1")
app.include_router(admin_cast.router, prefix="/api/v1")
app.include_router(admin_cast_screentime.router, prefix="/api/v1")
app.include_router(admin_cast_photos.router, prefix="/api/v1")
app.include_router(admin_brands.router, prefix="/api/v1")
app.include_router(admin_covered_shows.router, prefix="/api/v1")
app.include_router(admin_fandom_sync.router, prefix="/api/v1")
app.include_router(admin_face_references.router, prefix="/api/v1")
app.include_router(admin_image_counts.router, prefix="/api/v1")
app.include_router(admin_media_assets.router, prefix="/api/v1")
app.include_router(admin_networks_streaming_reads.router, prefix="/api/v1")
app.include_router(admin_operations.router, prefix="/api/v1")
app.include_router(admin_people_reads.router, prefix="/api/v1")
app.include_router(admin_recent_people.router, prefix="/api/v1")
app.include_router(admin_reddit_reads.router, prefix="/api/v1")
app.include_router(admin_show_reads.router, prefix="/api/v1")
app.include_router(admin_show_icons.router, prefix="/api/v1")
app.include_router(admin_show_links.router, prefix="/api/v1")
app.include_router(admin_show_links.fandom_router, prefix="/api/v1")
app.include_router(admin_show_roles.router, prefix="/api/v1")
app.include_router(admin_person_images.router, prefix="/api/v1")
app.include_router(admin_person_profile.router, prefix="/api/v1")
app.include_router(admin_nbcumv.router, prefix="/api/v1")
app.include_router(admin_show_bravo.router, prefix="/api/v1")
app.include_router(admin_show_images.router, prefix="/api/v1")
app.include_router(admin_show_news.router, prefix="/api/v1")
app.include_router(admin_scrape.router, prefix="/api/v1")
app.include_router(admin_social_posts.router, prefix="/api/v1")
app.include_router(admin_socialblade.router, prefix="/api/v1")
app.include_router(admin_show_sync.router, prefix="/api/v1")
app.include_router(socials.router, prefix="/api/v1")


@app.get("/")
def root():
    """Health check endpoint."""
    return {"status": "ok", "service": "trr-backend"}


@app.get("/health")
def health():
    """DB-aware readiness probe.

    Returns 200 when the database is reachable, 503 when degraded.
    """
    try:
        with pg.db_read_connection(label="health-probe", pool_name="health") as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return {
            "status": "healthy",
            "service": "trr-backend",
            "database": "connected",
        }
    except Exception:
        logger.warning("[health] readiness probe failed", exc_info=True)
        return JSONResponse(
            status_code=503,
            content={
                "status": "degraded",
                "service": "trr-backend",
                "database": "unreachable",
            },
        )


@app.get("/health/live")
def health_live():
    """Lightweight liveness probe. No DB check."""
    return {"status": "alive", "service": "trr-backend"}


@app.get("/metrics")
def metrics() -> Response:
    """Prometheus metrics endpoint."""
    if not metrics_available():
        return Response(status_code=404, content="metrics_unavailable\n", media_type="text/plain")
    return Response(content=render_metrics(), media_type=CONTENT_TYPE_LATEST)
