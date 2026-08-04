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
from pathlib import Path

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - python-dotenv is expected locally, optional in slim runtimes.
    load_dotenv = None  # type: ignore[assignment]

if load_dotenv is not None:
    load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.auth import InternalAdminUser
from api.realtime.broker import broker_runtime_status, init_broker, shutdown_broker
from trr_backend.db import pg
from trr_backend.db.connection import (
    DIRECT_DB_ENV,
    FALLBACK_DB_ENV,
    SESSION_DB_ENV,
    TRANSACTION_DB_ENV,
    log_database_resolution_summary,
    resolve_database_url_candidate_details,
    transaction_flight_test_enabled,
)
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
from trr_backend.problem import problem_response
from trr_backend.security.jwt import (
    describe_supabase_jwt_context,
    expected_supabase_issuer,
    expected_supabase_project_ref,
)
from trr_backend.socials.read_models.account_profile.common import instagram_comment_rollup_health

configure_runtime_observability(service_name="trr-backend-api")

logger = logging.getLogger(__name__)

_LOCAL_RUNTIME_MARKERS = frozenset({"local", "dev", "development", "test"})
_DB_POOL_LANE_LABELS = {
    "default": "backend_default_pool",
    "health": "health_pool",
    "social_profile": "social_profile_pool",
    "social_control": "social_control_pool",
    "social_progress": "social_progress_pool",
}


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


def _database_url_lane_label(candidate: dict[str, object] | None) -> str:
    if not candidate:
        return "missing_database_url"
    source = str(candidate.get("source") or "")
    connection_class = str(candidate.get("connection_class") or "")
    host_class = str(candidate.get("host_class") or "")
    if source == DIRECT_DB_ENV or connection_class == "direct":
        return "direct_url"
    if source == FALLBACK_DB_ENV:
        return "local_fallback" if host_class == "local" else "fallback_url"
    if source == SESSION_DB_ENV or connection_class == "session":
        return "pooler_url"
    if source == TRANSACTION_DB_ENV or connection_class == "transaction":
        return "transaction_pooler_url"
    if connection_class == "local" or host_class == "local":
        return "local_fallback"
    return connection_class or host_class or "unknown_database_url"


def _database_operator_lane_snapshot(*, pool_name: str) -> dict[str, object]:
    candidate = next(iter(resolve_database_url_candidate_details()), None)
    return {
        "url_lane": _database_url_lane_label(candidate),
        "url_source": str(candidate.get("source") or "") if candidate else None,
        "connection_class": str(candidate.get("connection_class") or "") if candidate else None,
        "host_class": str(candidate.get("host_class") or "") if candidate else None,
        "pool_name": pool_name,
        "pool_lane": _DB_POOL_LANE_LABELS.get(pool_name, "unknown_pool"),
    }


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


def _positive_int_env(name: str, default: int, *, minimum: int = 1) -> int:
    raw = (os.getenv(name) or "").strip()
    try:
        if raw:
            return max(int(raw), minimum)
    except ValueError:
        return default
    return default


def _modal_runtime_scheduler_enabled() -> bool:
    if not _env_flag("TRR_MODAL_RUNTIME_SCHEDULER_ENABLED", False):
        return False
    try:
        from trr_backend.modal_dispatch import modal_dispatch_enabled

        return modal_dispatch_enabled()
    except Exception:
        logger.exception("[modal-runtime-scheduler] enabled check failed")
        return False


def _modal_maintenance_owner_required() -> bool:
    if _is_local_or_dev_runtime():
        return _env_flag("TRR_MODAL_MAINTENANCE_OWNER_REQUIRED", False)
    return True


def _modal_maintenance_owner_names() -> list[str]:
    owners: list[str] = []
    if _env_flag("TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED", False):
        owners.append("modal_singleton_cron")
    if _env_flag("TRR_MODAL_RUNTIME_SCHEDULER_ENABLED", False):
        owners.append("api_runtime_scheduler")
    return owners


def _modal_maintenance_owner_fix_message() -> str:
    return (
        "Set exactly one owner variable: TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED=1 "
        "for Modal singleton cron maintenance, or TRR_MODAL_RUNTIME_SCHEDULER_ENABLED=1 "
        "for the API runtime scheduler fallback. Keep TRR_MODAL_MAINTENANCE_OWNER_REQUIRED=1. "
        "Then update the runtime secret with: cd TRR-Backend && "
        "python3.11 scripts/modal/prepare_named_secrets.py --apply"
    )


def _validate_modal_maintenance_owner_config() -> str | None:
    if not _modal_maintenance_owner_required():
        return None
    owners = _modal_maintenance_owner_names()
    if not owners:
        raise RuntimeError("Modal maintenance has no active owner. " + _modal_maintenance_owner_fix_message())
    if len(owners) > 1:
        raise RuntimeError(
            "Modal maintenance has duplicate active owners: "
            + ", ".join(owners)
            + ". Current owner variables: "
            + f"TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED={os.getenv('TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED')!r}, "
            + f"TRR_MODAL_RUNTIME_SCHEDULER_ENABLED={os.getenv('TRR_MODAL_RUNTIME_SCHEDULER_ENABLED')!r}. "
            + _modal_maintenance_owner_fix_message()
        )
    return owners[0]


def _modal_heartbeat_interval_seconds() -> int:
    return _positive_int_env("TRR_MODAL_RUNTIME_HEARTBEAT_INTERVAL_SECONDS", 60, minimum=30)


def _modal_social_recovery_interval_seconds() -> int:
    return _positive_int_env("TRR_MODAL_RUNTIME_SOCIAL_RECOVERY_INTERVAL_SECONDS", 120, minimum=60)


def _modal_stale_worker_cleanup_interval_seconds() -> int:
    return _positive_int_env("TRR_MODAL_RUNTIME_STALE_WORKER_CLEANUP_INTERVAL_SECONDS", 24 * 60 * 60, minimum=60)


def _run_modal_executor_heartbeat_once() -> dict[str, object]:
    from trr_backend.modal_dispatch import modal_heartbeat_function_name, spawn_modal_maintenance_function

    return spawn_modal_maintenance_function(
        function_name=modal_heartbeat_function_name(),
        log_label="modal executor heartbeat",
        dispatcher_name="admin",
        kwargs={"heartbeat_source": "backend_runtime_scheduler"},
    )


def _run_modal_social_recovery_once() -> dict[str, object]:
    from trr_backend.modal_dispatch import modal_social_recovery_function_name, spawn_modal_maintenance_function
    from trr_backend.socials.platforms import SOCIAL_SUPPORTED_PLATFORMS

    return spawn_modal_maintenance_function(
        function_name=modal_social_recovery_function_name(),
        log_label="modal social recovery",
        dispatcher_name="social",
        supported_platforms=list(SOCIAL_SUPPORTED_PLATFORMS),
    )


def _run_modal_stale_worker_cleanup_once() -> dict[str, object]:
    from trr_backend.modal_dispatch import modal_stale_worker_cleanup_function_name, spawn_modal_maintenance_function
    from trr_backend.socials.platforms import SOCIAL_SUPPORTED_PLATFORMS

    return spawn_modal_maintenance_function(
        function_name=modal_stale_worker_cleanup_function_name(),
        log_label="modal stale worker cleanup",
        dispatcher_name="social",
        supported_platforms=list(SOCIAL_SUPPORTED_PLATFORMS),
    )


async def _run_modal_runtime_scheduler_loop(
    *,
    stop_event: asyncio.Event,
    task_name: str,
    interval_seconds: int,
    run_once,
) -> None:
    while not stop_event.is_set():
        try:
            result = await asyncio.to_thread(run_once)
            logger.info("[modal-runtime-scheduler] %s result=%s", task_name, result)
        except Exception:
            logger.exception("[modal-runtime-scheduler] %s failed", task_name)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
        except TimeoutError:
            continue


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
    _validate_modal_maintenance_owner_config()
    _validate_cors_config()
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

    transaction_flight_allowed = (
        winner_connection_class == "transaction"
        and winner_source == TRANSACTION_DB_ENV
        and transaction_flight_test_enabled()
    )
    direct_local_allowed = winner_connection_class == "direct" and winner_source == DIRECT_DB_ENV and is_local
    direct_source_allowed = winner_source != DIRECT_DB_ENV or is_local

    # Fail-fast for invalid runtime lanes. Transaction mode is allowed only for
    # an explicit flight test using TRR_DB_TRANSACTION_URL, never implicitly via
    # the compatibility TRR_DB_URL.
    invalid_lanes = {"unknown", "other", "pooler"}

    if (
        winner_connection_class in invalid_lanes
        or (not direct_source_allowed)
        or (winner_connection_class == "direct" and not direct_local_allowed)
        or (winner_connection_class == "transaction" and not transaction_flight_allowed)
    ):
        raise RuntimeError(
            f"Invalid runtime connection lane: {winner_connection_class}\n"
            "Only session-mode pooler (:5432), local Postgres, local TRR_DB_DIRECT_URL, "
            "and explicit transaction flight tests are supported.\n"
            f"Use session-mode pooler (:5432) via TRR_DB_SESSION_URL or TRR_DB_URL. "
            f"Local direct database runs must use {DIRECT_DB_ENV}. "
            f"Transaction tests must use {TRANSACTION_DB_ENV} with TRR_DB_TRANSACTION_FLIGHT_TEST=1.\n"
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


def _cors_allow_origins_for_runtime(cors_origins: list[str] | None = None) -> list[str]:
    origins = cors_origins if cors_origins is not None else get_cors_origins()
    if origins:
        return origins
    if _is_local_or_dev_runtime():
        return ["*"]
    return []


def _validate_cors_config() -> None:
    if get_cors_origins() or _is_local_or_dev_runtime():
        return
    raise RuntimeError(
        "Missing CORS_ALLOW_ORIGINS for deployed runtime. "
        "Set it to a comma-separated list of trusted app origins before starting the API."
    )


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
    modal_scheduler_stop: asyncio.Event | None = None
    modal_scheduler_tasks: list[asyncio.Task[None]] = []
    if _cast_screentime_stale_sweeper_enabled():
        stale_sweeper_stop = asyncio.Event()
        stale_sweeper_task = asyncio.create_task(_run_cast_screentime_stale_sweeper(stale_sweeper_stop))
        logger.info(
            "[startup-config] cast screentime stale-run sweeper enabled interval_seconds=%s",
            _cast_screentime_stale_sweeper_interval_seconds(),
        )
    if _modal_runtime_scheduler_enabled():
        modal_scheduler_stop = asyncio.Event()
        modal_scheduler_specs = [
            ("executor_heartbeat", _modal_heartbeat_interval_seconds(), _run_modal_executor_heartbeat_once),
            ("social_recovery", _modal_social_recovery_interval_seconds(), _run_modal_social_recovery_once),
            (
                "stale_worker_cleanup",
                _modal_stale_worker_cleanup_interval_seconds(),
                _run_modal_stale_worker_cleanup_once,
            ),
        ]
        for task_name, interval_seconds, run_once in modal_scheduler_specs:
            modal_scheduler_tasks.append(
                asyncio.create_task(
                    _run_modal_runtime_scheduler_loop(
                        stop_event=modal_scheduler_stop,
                        task_name=task_name,
                        interval_seconds=interval_seconds,
                        run_once=run_once,
                    )
                )
            )
        logger.info(
            "[startup-config] modal runtime scheduler enabled heartbeat_interval_seconds=%s "
            "social_recovery_interval_seconds=%s stale_worker_cleanup_interval_seconds=%s",
            _modal_heartbeat_interval_seconds(),
            _modal_social_recovery_interval_seconds(),
            _modal_stale_worker_cleanup_interval_seconds(),
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
    if modal_scheduler_stop is not None:
        modal_scheduler_stop.set()
    for task in modal_scheduler_tasks:
        try:
            await asyncio.wait_for(task, timeout=5)
        except TimeoutError:
            task.cancel()
    await shutdown_broker()


app = FastAPI(
    title="The Reality Report API",
    description="Backend API for The Reality Report - reality TV data and surveys",
    version="0.1.0",
    lifespan=lifespan,
)

# CORS configuration.
# Set CORS_ALLOW_ORIGINS env var with comma-separated origins for deployed runtime.
# Local/dev may fall back to wildcard origins with credentials disabled.
cors_origins = get_cors_origins()
allow_credentials = len(cors_origins) > 0  # Only allow credentials with explicit origins

app.add_middleware(RequestTimeoutMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_allow_origins_for_runtime(cors_origins),
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
    return problem_response(
        request,
        code=str(payload.get("code") or "DATABASE_SERVICE_UNAVAILABLE"),
        status=503,
        message=str(payload.get("message") or "Database service unavailable."),
        retryable=bool(payload.get("retryable", True)),
        extra={
            "reason": payload.get("reason"),
            "retry_after_ms": payload.get("retry_after_ms"),
        },
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
    admin_bravotv_images,
    admin_cast,
    admin_cast_photos,
    admin_cast_screentime,
    admin_covered_shows,
    admin_face_references,
    admin_fandom_sync,
    admin_image_counts,
    admin_media_assets,
    admin_media_links,
    admin_nbcumv,
    admin_networks_streaming_reads,
    admin_operations,
    admin_people_reads,
    admin_person_external_ids,
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
    admin_social_completion,
    admin_social_posts,
    admin_socialblade,
    discussions,
    dms,
    shows,
    socials,
    surveys,
    ws,
)
from api.routers.v2 import admin_media as admin_media_v2  # noqa: E402
from api.routers.v2 import admin_people_reads as admin_people_reads_v2  # noqa: E402
from api.routers.v2 import core_cast_credit_reads as core_cast_credit_reads_v2  # noqa: E402
from api.routers.v2 import core_show_reads as core_show_reads_v2  # noqa: E402
from api.routers.v2 import covered_shows as covered_shows_v2  # noqa: E402
from api.routers.v2 import external_ids as external_ids_v2  # noqa: E402
from api.routers.v2 import identities as identities_v2  # noqa: E402
from api.routers.v2 import networks_streaming as networks_streaming_v2  # noqa: E402
from api.routers.v2 import person_media as person_media_v2  # noqa: E402
from api.routers.v2 import recent_people as recent_people_v2  # noqa: E402
from api.routers.v2 import season_cast_survey_roles as season_cast_survey_roles_v2  # noqa: E402
from api.routers.v2 import show_slugs as show_slugs_v2  # noqa: E402

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
app.include_router(admin_media_links.router, prefix="/api/v1")
app.include_router(admin_networks_streaming_reads.router, prefix="/api/v1")
app.include_router(admin_operations.router, prefix="/api/v1")
app.include_router(admin_people_reads.router, prefix="/api/v1")
app.include_router(admin_person_external_ids.router, prefix="/api/v1")
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
app.include_router(admin_social_completion.router, prefix="/api/v1")
app.include_router(admin_social_posts.router, prefix="/api/v1")
app.include_router(admin_socialblade.router, prefix="/api/v1")
app.include_router(admin_show_sync.router, prefix="/api/v1")
app.include_router(socials.router, prefix="/api/v1")
app.include_router(identities_v2.router, prefix="/api/v2")
app.include_router(networks_streaming_v2.router, prefix="/api/v2")
app.include_router(covered_shows_v2.router, prefix="/api/v2")
app.include_router(core_cast_credit_reads_v2.router, prefix="/api/v2")
app.include_router(core_show_reads_v2.router, prefix="/api/v2")
app.include_router(admin_media_v2.router, prefix="/api/v2")
app.include_router(admin_people_reads_v2.router, prefix="/api/v2")
app.include_router(recent_people_v2.router, prefix="/api/v2")
app.include_router(external_ids_v2.router, prefix="/api/v2")
app.include_router(show_slugs_v2.router, prefix="/api/v2")
app.include_router(person_media_v2.router, prefix="/api/v2")
app.include_router(season_cast_survey_roles_v2.router, prefix="/api/v2")


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
            "database_lane": _database_operator_lane_snapshot(pool_name="health"),
        }
    except Exception as exc:
        detail = database_service_unavailable_detail(exc)
        lane = _database_operator_lane_snapshot(pool_name="health")
        logger.warning("[health] readiness probe failed", exc_info=True)
        return JSONResponse(
            status_code=503,
            content={
                "status": "degraded",
                "service": "trr-backend",
                "database": "unreachable",
                "database_lane": lane,
                "reason": detail.get("reason"),
                "message": detail.get("message"),
                "retryable": detail.get("retryable"),
                "retry_after_ms": detail.get("retry_after_ms"),
            },
        )


@app.get("/health/live")
def health_live():
    """Lightweight liveness probe. No DB check."""
    return {"status": "alive", "service": "trr-backend"}


@app.get("/health/db-pressure")
def health_db_pressure() -> dict[str, str]:
    """Public-safe DB pressure status. Does not expose pool topology."""
    return pg.local_pool_pressure_summary()


def _permission_blocked_db_activity(error: Exception) -> bool:
    code = str(getattr(error, "pgcode", "") or "").strip()
    message = str(error).lower()
    return code in {"42501"} or "permission denied" in message or "insufficient privilege" in message


def _db_activity_unavailable(error: Exception) -> dict[str, object]:
    return {
        "status": "unavailable",
        "reason": "permission_blocked" if _permission_blocked_db_activity(error) else "unavailable",
        "error_type": type(error).__name__,
        "holders": [],
    }


def _db_activity_holder_snapshot() -> dict[str, object]:
    """Return grouped pg_stat_activity holder counts without query text."""
    try:
        with pg.db_read_connection(label="admin-db-pressure", pool_name="health") as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      COALESCE(NULLIF(application_name, ''), 'unknown') AS application_name,
                      COALESCE(NULLIF(usename, ''), 'unknown') AS role,
                      COALESCE(NULLIF(state, ''), 'unknown') AS state,
                      COALESCE(NULLIF(client_addr::text, ''), 'local') AS client_addr,
                      COUNT(*)::int AS holder_count
                    FROM pg_stat_activity
                    WHERE datname = current_database()
                    GROUP BY 1, 2, 3, 4
                    ORDER BY holder_count DESC, application_name ASC, role ASC, state ASC, client_addr ASC
                    LIMIT 50
                    """
                )
                rows = cur.fetchall()
        holders = [
            {
                "application_name": str(row[0] or "unknown"),
                "role": str(row[1] or "unknown"),
                "state": str(row[2] or "unknown"),
                "client_addr": str(row[3] or "local"),
                "holder_count": int(row[4] or 0),
            }
            for row in rows
        ]
        return {
            "status": "available",
            "reason": "ok",
            "grouped_by": ["application_name", "role", "state", "client_addr"],
            "holders": holders,
        }
    except Exception as exc:  # noqa: BLE001
        logger.warning("[health] db activity holder snapshot unavailable", exc_info=True)
        return _db_activity_unavailable(exc)


@app.get("/admin/health/db-pressure")
def admin_health_db_pressure(_: InternalAdminUser = None) -> dict[str, object]:
    """Internal DB pressure details for admin/operator diagnostics."""
    snapshot = pg.local_pool_pressure_snapshot()
    snapshot["db_activity"] = _db_activity_holder_snapshot()
    snapshot["operator_failure_lanes"] = {
        "database": _database_operator_lane_snapshot(pool_name="default"),
        "health": _database_operator_lane_snapshot(pool_name="health"),
        "social_profile": _database_operator_lane_snapshot(pool_name="social_profile"),
        "social_control": _database_operator_lane_snapshot(pool_name="social_control"),
        "social_progress": _database_operator_lane_snapshot(pool_name="social_progress"),
        "auth": {
            "lane": "auth",
            "signals": ["401", "403", "AUTH_REQUIRED", "FORBIDDEN"],
        },
        "modal": {
            "lane": "modal_deployment_state",
            "readiness_command": "cd TRR-Backend && .venv/bin/python scripts/modal/verify_modal_readiness.py",
        },
    }
    return snapshot


@app.get("/admin/health/instagram-comment-rollups")
@app.get("/api/v1/admin/health/instagram-comment-rollups")
def admin_health_instagram_comment_rollups(sample_limit: int = 25, _: InternalAdminUser = None) -> dict[str, object]:
    """Internal exact-count health check for persisted Instagram comment rollups."""
    return instagram_comment_rollup_health(sample_limit=sample_limit)


@app.get("/health/runtime")
async def health_runtime() -> dict[str, object]:
    """DB-free runtime snapshot for local control-plane diagnostics."""
    from trr_backend.socials.control_plane.background_tasks import background_task_snapshot

    return {
        "status": "alive",
        "service": "trr-backend",
        "background_tasks": background_task_snapshot(),
        "realtime": broker_runtime_status(),
    }


@app.get("/metrics")
def metrics() -> Response:
    """Prometheus metrics endpoint."""
    if not metrics_available():
        return Response(status_code=404, content="metrics_unavailable\n", media_type="text/plain")
    return Response(content=render_metrics(), media_type=CONTENT_TYPE_LATEST)
