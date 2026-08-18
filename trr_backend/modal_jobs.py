"""Modal app for on-demand TRR backend long-running jobs."""

from __future__ import annotations

import logging
import math
import os
import pathlib
import socket
import sys
import time
import uuid
from datetime import UTC, datetime
from importlib.metadata import version as package_version
from typing import Any, Final, cast

from trr_backend.observability import configure_runtime_observability
from trr_backend.socials.platforms import SOCIAL_SUPPORTED_PLATFORMS


def _build_modal_stub_module():
    class _ModalImage:
        @classmethod
        def debian_slim(cls, **_kwargs):
            return cls()

        def pip_install_from_requirements(self, *_args, **_kwargs):
            return self

        def add_local_python_source(self, *_args, **_kwargs):
            return self

        def add_local_file(self, *_args, **_kwargs):
            return self

        def add_local_dir(self, *_args, **_kwargs):
            return self

        def apt_install(self, *_args, **_kwargs):
            return self

        def pip_install(self, *_args, **_kwargs):
            return self

        def run_commands(self, *_args, **_kwargs):
            return self

    class _ModalSecret:
        @staticmethod
        def from_name(name: str):
            return {"named": name}

        @staticmethod
        def from_dotenv(path: pathlib.Path):
            return {"dotenv": str(path)}

    class _ModalCron:
        def __init__(self, expression: str, *, timezone: str | None = None):
            self.expression = expression
            self.timezone = timezone

    class _ModalApp:
        def __init__(self, *_args, **_kwargs):
            return

        def function(self, *args, **kwargs):
            def _decorator(func):
                func.local = func
                func.get_raw_f = lambda: func
                func._modal_function_args = args
                func._modal_function_options = dict(kwargs)
                return func

            return _decorator

    class _ModalModule:
        Image = _ModalImage
        Secret = _ModalSecret
        Cron = _ModalCron
        App = _ModalApp

        @staticmethod
        def asgi_app(*_args, **_kwargs):
            def _decorator(func):
                return func

            return _decorator

    return _ModalModule()


def _modal_module_is_usable(module: object) -> bool:
    return all(hasattr(module, attribute) for attribute in ("Image", "Secret", "Cron", "App", "asgi_app"))


def _load_modal_module():
    try:
        import modal as imported_modal
    except ModuleNotFoundError:  # pragma: no cover - exercised by local/test imports without modal installed
        return _build_modal_stub_module()
    if not _modal_module_is_usable(imported_modal):
        return _build_modal_stub_module()
    return imported_modal


modal: Any = _load_modal_module()
logger = logging.getLogger(__name__)

_BACKEND_ROOT = pathlib.Path(__file__).resolve().parents[1]
_APP_NAME = str(os.getenv("TRR_MODAL_APP_NAME") or "trr-backend-jobs").strip() or "trr-backend-jobs"
_TIMEZONE = str(os.getenv("TRR_MODAL_TIMEZONE") or "America/New_York").strip() or "America/New_York"
_API_FUNCTION_NAME = str(os.getenv("TRR_MODAL_API_FUNCTION") or "serve_backend_api").strip() or "serve_backend_api"
_API_LABEL = str(os.getenv("TRR_MODAL_API_LABEL") or "trr-backend-api").strip() or "trr-backend-api"
_API_MIN_CONTAINERS = max(0, int(os.getenv("TRR_MODAL_API_MIN_CONTAINERS", "0")))
_API_MAX_CONTAINERS = max(1, int(os.getenv("TRR_MODAL_API_MAX_CONTAINERS", "8")))
_SOCIAL_CONCURRENCY_LIMIT = max(1, int(os.getenv("TRR_MODAL_SOCIAL_JOB_CONCURRENCY_LIMIT", "8")))
_SOCIAL_COMMENTS_CONCURRENCY_LIMIT = max(
    1,
    int(os.getenv("TRR_MODAL_SOCIAL_COMMENTS_JOB_CONCURRENCY_LIMIT", "4")),
)
_SOCIAL_COMMENTS_RECOVERY_CONCURRENCY_LIMIT = max(
    1,
    int(os.getenv("TRR_MODAL_SOCIAL_COMMENTS_RECOVERY_JOB_CONCURRENCY_LIMIT", "4")),
)

# Phase 2 (DB headroom): guard against scaling comments containers past the DB
# session-pool ceiling. Each container against the Supavisor SESSION pooler pins
# 1:1 Postgres backends; per-container demand = default pool + the social_control
# named pool (both Modal-clamped). The budget (operator-set after reading the
# dashboard pool size) bounds total comments-lane session demand. 0 = disabled.
SOCIAL_INSTAGRAM_COMMENTS_DB_SESSION_BUDGET_ENV = "SOCIAL_INSTAGRAM_COMMENTS_DB_SESSION_BUDGET"


def comments_db_session_budget_status() -> dict[str, int | bool]:
    """Compute comments-lane DB session demand vs the configured budget.

    sessions_per_worker = default pool + social_control named pool (the only named
    pool the comments path opens), both Modal session-pooler clamped. worker_cap
    covers the comments + recovery container caps. A budget <= 0 disables the
    check (within_budget=True)."""
    from trr_backend.db.pg import (
        DEFAULT_MODAL_NAMED_SESSION_POOLER_MAXCONN,
        DEFAULT_MODAL_SESSION_POOLER_MAXCONN,
    )

    sessions_per_worker = DEFAULT_MODAL_SESSION_POOLER_MAXCONN + DEFAULT_MODAL_NAMED_SESSION_POOLER_MAXCONN
    worker_cap = _SOCIAL_COMMENTS_CONCURRENCY_LIMIT + _SOCIAL_COMMENTS_RECOVERY_CONCURRENCY_LIMIT
    demand = worker_cap * sessions_per_worker
    try:
        budget = int(os.getenv(SOCIAL_INSTAGRAM_COMMENTS_DB_SESSION_BUDGET_ENV, "0"))
    except ValueError:
        budget = 0
    return {
        "sessions_per_worker": sessions_per_worker,
        "worker_cap": worker_cap,
        "demand": demand,
        "budget": budget,
        "within_budget": budget <= 0 or demand <= budget,
    }


def _log_comments_db_session_budget() -> None:
    status = comments_db_session_budget_status()
    if not status["within_budget"]:
        logger.error(
            "[comments-db-session-budget] comments-lane session demand %s exceeds "
            "%s=%s (worker_cap=%s x sessions_per_worker=%s). Lower the comments/recovery "
            "container caps or raise the Supavisor session pool before scaling.",
            status["demand"],
            SOCIAL_INSTAGRAM_COMMENTS_DB_SESSION_BUDGET_ENV,
            status["budget"],
            status["worker_cap"],
            status["sessions_per_worker"],
        )


_SOCIAL_MEDIA_CONCURRENCY_LIMIT = max(
    1,
    int(os.getenv("TRR_MODAL_SOCIAL_MEDIA_JOB_CONCURRENCY_LIMIT", "1")),
)
_SOCIAL_RECOVERY_CONCURRENCY_LIMIT = max(1, int(os.getenv("TRR_MODAL_SOCIAL_RECOVERY_CONCURRENCY_LIMIT", "1")))
_SOCIAL_PENDING_LAUNCH_RECOVERY_LIMIT = max(
    1,
    int(os.getenv("TRR_MODAL_SOCIAL_PENDING_LAUNCH_RECOVERY_LIMIT", "10")),
)
_ADMIN_KEEP_WARM = max(0, int(os.getenv("TRR_MODAL_ADMIN_KEEP_WARM", "0")))
_ADMIN_CONCURRENCY_LIMIT = max(1, int(os.getenv("TRR_MODAL_ADMIN_OPERATION_CONCURRENCY_LIMIT", "8")))
_GOOGLE_NEWS_CONCURRENCY_LIMIT = max(1, int(os.getenv("TRR_MODAL_GOOGLE_NEWS_CONCURRENCY_LIMIT", "4")))
_REDDIT_REFRESH_CONCURRENCY_LIMIT = max(1, int(os.getenv("TRR_MODAL_REDDIT_REFRESH_CONCURRENCY_LIMIT", "2")))
_VISION_CONCURRENCY_LIMIT = max(1, int(os.getenv("TRR_MODAL_VISION_CONCURRENCY_LIMIT", "4")))
_CAST_SCREENTIME_CONCURRENCY_LIMIT = max(1, int(os.getenv("TRR_MODAL_CAST_SCREENTIME_CONCURRENCY_LIMIT", "2")))
_CAST_SCREENTIME_SUBTITLE_CONCURRENCY_LIMIT = max(
    1,
    int(os.getenv("TRR_MODAL_CAST_SCREENTIME_SUBTITLE_CONCURRENCY_LIMIT", "2")),
)
_SOCIALBLADE_CONCURRENCY_LIMIT = max(1, int(os.getenv("TRR_MODAL_SOCIALBLADE_CONCURRENCY_LIMIT", "3")))
_STALE_WORKER_CLEANUP_CONCURRENCY_LIMIT = max(
    1,
    int(os.getenv("TRR_MODAL_STALE_WORKER_CLEANUP_CONCURRENCY_LIMIT", "1")),
)
_ADMIN_OPERATION_TIMEOUT_SECONDS = max(5 * 60, int(os.getenv("TRR_MODAL_ADMIN_OPERATION_TIMEOUT_SECONDS", "2700")))
_GOOGLE_NEWS_TIMEOUT_SECONDS = max(5 * 60, int(os.getenv("TRR_MODAL_GOOGLE_NEWS_TIMEOUT_SECONDS", "1800")))
_REDDIT_REFRESH_TIMEOUT_SECONDS = max(15 * 60, int(os.getenv("TRR_MODAL_REDDIT_REFRESH_TIMEOUT_SECONDS", "7200")))
_VISION_TIMEOUT_SECONDS = max(5 * 60, int(os.getenv("TRR_MODAL_VISION_TIMEOUT_SECONDS", "1200")))
_CAST_SCREENTIME_TIMEOUT_SECONDS = max(
    30 * 60,
    int(os.getenv("TRR_MODAL_CAST_SCREENTIME_TIMEOUT_SECONDS", str(2 * 60 * 60))),
)
_CAST_SCREENTIME_SUBTITLE_TIMEOUT_SECONDS = max(
    30 * 60,
    int(os.getenv("TRR_MODAL_CAST_SCREENTIME_SUBTITLE_TIMEOUT_SECONDS", str(2 * 60 * 60))),
)
_SOCIALBLADE_TIMEOUT_SECONDS = max(5 * 60, int(os.getenv("TRR_MODAL_SOCIALBLADE_TIMEOUT_SECONDS", "900")))
_STALE_WORKER_CLEANUP_TIMEOUT_SECONDS = max(
    60,
    int(os.getenv("TRR_MODAL_STALE_WORKER_CLEANUP_TIMEOUT_SECONDS", "300")),
)
_STALE_WORKER_CLEANUP_AFTER_SECONDS = max(
    24 * 60 * 60,
    int(os.getenv("SOCIAL_WORKER_HEARTBEAT_CLEANUP_AFTER_SECONDS", str(7 * 24 * 60 * 60))),
)
_DEFAULT_RUNTIME_SECRET_NAME = "trr-backend-runtime"
_DEFAULT_SOCIAL_SECRET_NAME = "trr-social-auth"
_LOCAL_RUNTIME_MARKERS: Final[frozenset[str]] = frozenset({"local", "dev", "development", "test"})
_INSTAGRAM_PUBLIC_FIRST_MODE_ALIASES: Final[frozenset[str]] = frozenset(
    {"", "public", "public-first", "public_first", "no_login", "nologin"}
)
_SOCIAL_IMAGE_LOCAL_FILES: Final[tuple[tuple[str, str], ...]] = (
    (str(_BACKEND_ROOT / "scripts" / "_sync_common.py"), "/root/scripts/_sync_common.py"),
    (str(_BACKEND_ROOT / "scripts" / "socials" / "__init__.py"), "/root/scripts/socials/__init__.py"),
    (
        str(_BACKEND_ROOT / "scripts" / "socials" / "refresh_cookies.py"),
        "/root/scripts/socials/refresh_cookies.py",
    ),
    (
        str(_BACKEND_ROOT / "scripts" / "socials" / "youtube" / "__init__.py"),
        "/root/scripts/socials/youtube/__init__.py",
    ),
    (str(_BACKEND_ROOT / "scripts" / "socials" / "youtube" / "scrape.py"), "/root/scripts/socials/youtube/scrape.py"),
)
_SOCIAL_IMAGE_LOCAL_DIRS: Final[tuple[tuple[str, str], ...]] = (
    (str(_BACKEND_ROOT / "scripts" / "sync"), "/root/scripts/sync"),
    (str(_BACKEND_ROOT / "scripts" / "socials" / "instagram"), "/root/scripts/socials/instagram"),
    (str(_BACKEND_ROOT / "scripts" / "socials" / "tiktok"), "/root/scripts/socials/tiktok"),
    (str(_BACKEND_ROOT / "scripts" / "socials" / "twitter"), "/root/scripts/socials/twitter"),
)
_LEAN_IMAGE_LOCAL_FILES: Final[tuple[tuple[str, str], ...]] = (
    (str(_BACKEND_ROOT / "scripts" / "__init__.py"), "/root/scripts/__init__.py"),
    (str(_BACKEND_ROOT / "scripts" / "_sync_common.py"), "/root/scripts/_sync_common.py"),
)
_LEAN_IMAGE_LOCAL_DIRS: Final[tuple[tuple[str, str], ...]] = (
    (str(_BACKEND_ROOT / "scripts" / "sync"), "/root/scripts/sync"),
)
_SOCIAL_BROWSER_APT_PACKAGES: Final[tuple[str, ...]] = (
    "libnss3",
    "libnspr4",
    "libatk1.0-0",
    "libatk-bridge2.0-0",
    "libcups2",
    "libdrm2",
    "libxkbcommon0",
    "libxcomposite1",
    "libxdamage1",
    "libxfixes3",
    "libxrandr2",
    "libgbm1",
    "libpango-1.0-0",
    "libcairo2",
    "libasound2",
    "libatspi2.0-0",
)
_SOCIAL_BROWSER_SETUP_COMMANDS: Final[tuple[str, ...]] = (
    "playwright install chromium",
    # P0-2/P0-3: The Scrapling comments lane uses StealthyFetcher (Patchright-
    # backed in v0.4+). Scrapling v0.4.9 refreshed browsers and fingerprints,
    # and upstream recommends forcing the asset refresh after upgrades.
    "scrapling install --force",
)


def _instagram_public_first_mode_enabled() -> bool:
    return str(os.getenv("SOCIAL_INSTAGRAM_SCRAPE_MODE") or "public_first").strip().lower() in (
        _INSTAGRAM_PUBLIC_FIRST_MODE_ALIASES
    )


_MODAL_LEAN_REQUIREMENTS: Final = _BACKEND_ROOT / "requirements.modal.lean.lock.txt"
_MODAL_BROWSER_REQUIREMENTS: Final = _BACKEND_ROOT / "requirements.modal.browser.lock.txt"
_MODAL_VISION_REQUIREMENTS: Final = _BACKEND_ROOT / "requirements.modal.vision.lock.txt"
_SOCIAL_IMAGE_PIP_PACKAGES: Final[tuple[str, ...]] = ()
_INSTAGRAM_PAYLOAD_READ_MODE_ENV: Final = "SOCIAL_INSTAGRAM_PAYLOAD_READ_MODE"
_MODAL_INSTAGRAM_PAYLOAD_READ_MODE_ENV: Final = "TRR_MODAL_INSTAGRAM_PAYLOAD_READ_MODE"
_INSTAGRAM_PAYLOAD_READ_MODES: Final[frozenset[str]] = frozenset({"legacy", "compare", "sidecar"})
_INSTAGRAM_PAYLOAD_COMPARE_SAMPLE_RATE_ENV: Final = "SOCIAL_INSTAGRAM_PAYLOAD_COMPARE_SAMPLE_RATE"
_MODAL_INSTAGRAM_PAYLOAD_COMPARE_SAMPLE_RATE_ENV: Final = "TRR_MODAL_INSTAGRAM_PAYLOAD_COMPARE_SAMPLE_RATE"
_DEFAULT_INSTAGRAM_PAYLOAD_COMPARE_SAMPLE_RATE: Final = 0.1
_CANONICAL_MODAL_RUNTIME_DEFAULTS: Final[dict[str, str]] = {
    "TRR_RUNTIME_CAPACITY_CONTEXT": "hosted_modal",
    "TRR_DB_POOL_MINCONN": "1",
    "TRR_DB_POOL_MAXCONN": "2",
    "TRR_SOCIAL_PROFILE_DB_POOL_MINCONN": "1",
    "TRR_SOCIAL_PROFILE_DB_POOL_MAXCONN": "1",
    "TRR_SOCIAL_CONTROL_DB_POOL_MINCONN": "1",
    "TRR_SOCIAL_CONTROL_DB_POOL_MAXCONN": "1",
    "TRR_SOCIAL_PROGRESS_DB_POOL_MINCONN": "1",
    "TRR_SOCIAL_PROGRESS_DB_POOL_MAXCONN": "1",
    "TRR_HEALTH_DB_POOL_MINCONN": "1",
    "TRR_HEALTH_DB_POOL_MAXCONN": "1",
    "TRR_DB_POOL_CLOSE_AFTER_RETURN": "1",
    "TRR_DB_POOL_ACQUIRE_ATTEMPTS": "30",
    "TRR_DB_POOL_ACQUIRE_SLEEP_MS": "200",
    "TRR_JOB_PLANE_MODE": "remote",
    "TRR_LONG_JOB_ENFORCE_REMOTE": "1",
    "TRR_REMOTE_EXECUTOR": "modal",
    "TRR_MODAL_ENABLED": "1",
    "TRR_MODAL_APP_NAME": _APP_NAME,
    "TRR_MODAL_API_FUNCTION": _API_FUNCTION_NAME,
    "TRR_MODAL_API_LABEL": _API_LABEL,
    "TRR_MODAL_MAINTENANCE_OWNER_REQUIRED": "1",
    "TRR_MODAL_ADMIN_OPERATION_FUNCTION": "run_admin_operation_v2",
    "TRR_MODAL_GOOGLE_NEWS_FUNCTION": "run_google_news_sync",
    "TRR_MODAL_REDDIT_REFRESH_FUNCTION": "run_reddit_refresh",
    "TRR_MODAL_SOCIAL_JOB_FUNCTION": "run_social_job",
    "TRR_MODAL_SOCIAL_POSTS_JOB_FUNCTION": "run_social_posts_job",
    "TRR_MODAL_SOCIAL_MEDIA_JOB_FUNCTION": "run_social_media_job",
    "TRR_MODAL_SOCIAL_COMMENTS_JOB_FUNCTION": "run_social_comments_job",
    "TRR_MODAL_SOCIAL_COMMENTS_RECOVERY_JOB_FUNCTION": "run_social_comments_recovery_job",
    "TRR_MODAL_SOCIAL_RECOVERY_FUNCTION": "sweep_social_dispatch_queue",
    "TRR_MODAL_SOCIAL_AUTH_PROBE_FUNCTION": "probe_social_remote_auth",
    "TRR_MODAL_INSTAGRAM_PUBLIC_HISTORY_PROBE_FUNCTION": "probe_instagram_public_history",
    "TRR_MODAL_GETTY_REMOTE_PROBE_FUNCTION": "probe_getty_remote_access",
    "TRR_MODAL_VISION_FUNCTION": "run_admin_vision",
    "TRR_MODAL_CAST_SCREENTIME_FUNCTION": "run_cast_screentime_analysis",
    "TRR_MODAL_CAST_SCREENTIME_SUBTITLE_FUNCTION": "run_cast_screentime_subtitle_extraction",
    "TRR_MODAL_SOCIALBLADE_FUNCTION": "run_socialblade_scrape",
    "TRR_MODAL_STALE_WORKER_CLEANUP_FUNCTION": "purge_stale_social_worker_heartbeats",
    "TRR_MODAL_SOCIAL_JOB_CONCURRENCY_LIMIT": "8",
    # Phase 4 (GATED — do not raise until the gate passes): comments container cap.
    # This is the REAL throughput-scaling lever (each container = its own egress +
    # rate-pace slot), unlike PER_POST_CONCURRENCY below. To flip: raise "4" toward
    # the target (e.g. "6"/"8") ONLY AFTER (1) the transaction-pooler flight test
    # is validated (Phase 2) and (2) comments_db_session_budget_status() shows the
    # new cap stays within_budget. Each added container pins default-pool(2) +
    # social_control(1) = 3 session-pooler backends; recovery cap scales with it.
    "TRR_MODAL_SOCIAL_COMMENTS_JOB_CONCURRENCY_LIMIT": "4",
    "TRR_MODAL_SOCIAL_COMMENTS_RECOVERY_JOB_CONCURRENCY_LIMIT": "4",
    "TRR_MODAL_SOCIAL_MEDIA_JOB_CONCURRENCY_LIMIT": "1",
    "TRR_MODAL_SOCIAL_RECOVERY_CONCURRENCY_LIMIT": "1",
    "TRR_MODAL_GOOGLE_NEWS_CONCURRENCY_LIMIT": "4",
    "TRR_MODAL_REDDIT_REFRESH_CONCURRENCY_LIMIT": "2",
    "TRR_MODAL_VISION_CONCURRENCY_LIMIT": "4",
    "TRR_MODAL_CAST_SCREENTIME_CONCURRENCY_LIMIT": "2",
    "TRR_MODAL_CAST_SCREENTIME_SUBTITLE_CONCURRENCY_LIMIT": "2",
    "TRR_MODAL_CAST_SCREENTIME_SUBTITLE_TIMEOUT_SECONDS": "7200",
    "TRR_MODAL_SOCIALBLADE_CONCURRENCY_LIMIT": "3",
    "SOCIAL_MODAL_DISPATCH_LIMIT": "12",
    "SOCIAL_INSTAGRAM_POSTS_USE_STICKY_PROXY": "true",
    "SOCIAL_INSTAGRAM_POSTS_ANONYMOUS_ENABLED": "false",
    "SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER": "decodo",
    "SOCIAL_INSTAGRAM_COMMENTS_FORCE_ROTATING_PROXY": "true",
    "SOCIAL_INSTAGRAM_COMMENTS_USE_STICKY_PROXY": "false",
    "SOCIAL_INSTAGRAM_COMMENTS_PROXY_SESSION_TTL_SECONDS": "600",
    # SocialBlade profile scrapes route through the Decodo residential proxy so the
    # live lane no longer depends on an unproxied Modal egress that Cloudflare 1020s.
    # The browser warmup must pass Scrapling's supported proxy= option and let
    # Scrapling solve SocialBlade's Cloudflare interstitial. Sticky Decodo usernames
    # are intentionally off here because the TRR Decodo gateway rejects the generated
    # session-scoped username with 407.
    "SOCIALBLADE_PROXY_PROVIDER": "decodo",
    "SOCIALBLADE_USE_STICKY_PROXY": "false",
    "SOCIALBLADE_PROXY_SESSION_TTL_SECONDS": "600",
    "SOCIALBLADE_SCRAPLING_SOLVE_CLOUDFLARE": "true",
    "INSTAGRAM_BROWSER_NETWORK_POLICY_ENABLED": "true",
    "INSTAGRAM_BROWSER_BLOCK_STATIC_ASSETS": "true",
    "INSTAGRAM_BROWSER_DISABLE_EXTRA_RESOURCES": "true",
    "INSTAGRAM_BROWSER_NETWORK_POLICY_REPORT_ONLY": "false",
    "SOCIAL_WORKER_POOL_COMMENTS": "4",
    "SOCIAL_WORKER_POOL_SHARED_ACCOUNT_DISCOVERY": "3",
    "SOCIAL_WORKER_POOL_SHARED_ACCOUNT_POSTS": "8",
    "SOCIAL_SHARED_ACCOUNT_POSTS_PLATFORM_CAP_INSTAGRAM": "2",
    "SOCIAL_WORKER_POOL_MEDIA_MIRROR": "1",
    "SOCIAL_WORKER_POOL_COMMENT_MEDIA_MIRROR": "1",
    "SOCIAL_MIRROR_PLATFORM_CAP": "1",
    "SOCIAL_CATALOG_RUN_IN_FLIGHT_CAP": "8",
    "SOCIAL_POSTS_COMMENTS_PLATFORM_CAP_INSTAGRAM": "4",
    "SOCIAL_INSTAGRAM_COMMENTS_PROFILE_SHARD_COUNT": "8",
    "SOCIAL_INSTAGRAM_COMMENTS_MAX_SHARD_COUNT": "1000",
    "SOCIAL_INSTAGRAM_COMMENTS_GLOBAL_RATE_LIMIT_MODE": "advisory",
    # Phase 4 (GATED): in-job per-post parallelism. SECONDARY lever — all posts in
    # a job share one sticky egress + one rate slot, so raising this alone yields
    # little until container-count fan-out (above) is in play; it runs in-process
    # against the SAME clamped per-container pool, so it adds NO pooler sessions.
    # Safe to raise toward the cap of 8 once per-egress pacing (Phase 1/3) is live
    # and a benchmark shows req/s gains without a 429 increase. Keep at "1" until then.
    "SOCIAL_INSTAGRAM_COMMENTS_PER_POST_CONCURRENCY": "1",
    # Throughput (Phase 1): raise the public-relay GraphQL page size from the
    # baked default 12 to the clamp ceiling 50 (~4x fewer requests per comment).
    # Safe because the fetcher downgrades to 12 and retries the same cursor when a
    # doc_id rejects the larger ``first`` (comments_scrapling.fetcher
    # _try_page_size_downgrade / _try_child_page_size_downgrade). Reversible: unset
    # or set back to "12".
    "SOCIAL_INSTAGRAM_COMMENTS_COAUTHOR_GRAPHQL_PAGE_SIZE": "50",
    "SOCIAL_INSTAGRAM_COMMENTS_COAUTHOR_GRAPHQL_CHILD_PAGE_SIZE": "50",
    # Keep automatic account-backed escalation disabled. Public incomplete
    # comment targets should go through the public-to-public recovery lane first;
    # authenticated/proxy follow-up remains an explicit operator action.
    "SOCIAL_INSTAGRAM_COMMENTS_AUTO_AUTH_FALLBACK": "0",
    # Comment-completeness fleet determinism: pin the "scrape all comments"
    # knobs. The deadlines are BOUNDED per-attempt CHECKPOINTS (not give-ups):
    # each attempt scrapes a chunk, persists the partial + records a resume
    # cursor, marks the shard retryable, and resumes from the cursor next attempt.
    # Combined with gap-tolerance=0 + retry-until-complete this converges to ALL
    # comments while making durable incremental progress and freeing workers
    # between chunks. Do NOT set these to 0/unbounded: that holds everything in
    # memory and persists nothing until post-completion, starving the worker pool
    # and losing all work on a timeout/crash. Gap tolerances at 0 force
    # exhaustion-based terminal classification; raised stall/refill/min-gap +
    # checkpoint caps keep the long tail recoverable. See comments_scrapling.*.
    "SOCIAL_INSTAGRAM_COMMENT_PAGINATION_MAX_SECONDS": "300",
    "SOCIAL_INSTAGRAM_REPLY_PAGINATION_MAX_SECONDS": "180",
    "SOCIAL_INSTAGRAM_REPLY_TAIL_TOTAL_MAX_SECONDS_PER_POST": "180",
    "SOCIAL_INSTAGRAM_COMMENTS_TERMINAL_GAP_MAX": "0",
    "SOCIAL_INSTAGRAM_COMMENTS_TERMINAL_GAP_RATIO": "0",
    "SOCIAL_INSTAGRAM_COMMENTS_RECONCILABLE_GAP_MAX": "0",
    "SOCIAL_INSTAGRAM_COMMENTS_RECONCILABLE_GAP_RATIO": "0",
    "SOCIAL_INSTAGRAM_COMMENTS_HIDDEN_UNAVAILABLE_GAP_MAX": "0",
    "SOCIAL_INSTAGRAM_COMMENTS_INCOMPLETE_STALL_ATTEMPTS": "8",
    "SOCIAL_INSTAGRAM_COMMENTS_AUTO_AUTH_FALLBACK_MIN_GAP": "1",
    "SOCIAL_INSTAGRAM_COMMENTS_AUTO_REFILL_LIMIT": "100",
    "SOCIAL_INSTAGRAM_REPLY_CHECKPOINT_MAX_ITEMS": "1000",
    "SOCIAL_INSTAGRAM_TOP_LEVEL_CHECKPOINT_MAX_ITEMS": "1000",
    "SOCIAL_INSTAGRAM_COMMENTS_REVEAL_HIDDEN_WITHOUT_EXPECTED": "1",
    "SOCIAL_INSTAGRAM_COMMENTS_SINGLE_SESSION_RENDERED_DEADLINE_SECONDS": "300",
    "SOCIAL_THREADS_POSTS_SCRAPLING_ENABLED": "true",
    "SOCIAL_THREADS_POSTS_PROXY_PROVIDER": "decodo",
    "SOCIAL_TIKTOK_POSTS_PROXY_PROVIDER": "decodo",
    "SOCIAL_TIKTOK_COMMENT_FETCH_TIMEOUT_SECONDS": "180",
    "SOCIAL_PLATFORM_CAP_PER_ACCOUNT_SCALING": "false",
    "TRR_ADMIN_IMAGE_EXECUTION_BACKEND": "modal",
    "SOCIAL_QUEUE_ENABLED": "true",
}
# Operator-tunable subset of _CANONICAL_MODAL_RUNTIME_DEFAULTS. For these keys
# the canonical literal is a DEFAULT (applied via setdefault) so an operator can
# override it through the Modal secret or environment. Everything NOT in this set
# stays unconditionally pinned — in particular every DB-pool size and worker/
# container concurrency cap, which are Supavisor session-budget safety clamps
# (see comments_db_session_budget_status). Do not add a *_DB_POOL_*,
# *_CONCURRENCY_LIMIT, or SOCIAL_WORKER_POOL_* key here without the DB-session-
# budget analysis.
_OPERATOR_TUNABLE_RUNTIME_DEFAULT_KEYS: Final[frozenset[str]] = frozenset(
    {
        "SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER",
        "SOCIALBLADE_PROXY_PROVIDER",
        "SOCIAL_THREADS_POSTS_PROXY_PROVIDER",
        "SOCIAL_INSTAGRAM_COMMENTS_COAUTHOR_GRAPHQL_PAGE_SIZE",
        "SOCIAL_INSTAGRAM_COMMENTS_COAUTHOR_GRAPHQL_CHILD_PAGE_SIZE",
    }
)


def _build_lean_image_base(
    *,
    apt_packages: tuple[str, ...] = (),
    image_factory: Any | None = None,
):
    factory = image_factory or modal.Image
    image = factory.debian_slim(python_version="3.11")
    if apt_packages:
        image = image.apt_install(*apt_packages)
    image = image.pip_install_from_requirements(str(_MODAL_LEAN_REQUIREMENTS))
    image = image.add_local_python_source("api", "trr_backend")
    for local_path, remote_path in _LEAN_IMAGE_LOCAL_FILES:
        image = image.add_local_file(local_path, remote_path=remote_path)
    for local_path, remote_path in _LEAN_IMAGE_LOCAL_DIRS:
        image = image.add_local_dir(local_path, remote_path=remote_path)
    return image


def _build_media_image_base(*, image_factory: Any | None = None):
    """Build the lean backend runtime with FFmpeg/FFprobe for media extraction."""

    return _build_lean_image_base(apt_packages=("ffmpeg",), image_factory=image_factory)


def _build_social_image_base(*, include_browser_runtime: bool = False, image_factory: Any | None = None):
    factory = image_factory or modal.Image
    image = factory.debian_slim(python_version="3.11")
    if include_browser_runtime:
        image = image.apt_install(*_SOCIAL_BROWSER_APT_PACKAGES)
    image = image.pip_install_from_requirements(str(_MODAL_BROWSER_REQUIREMENTS))
    if _SOCIAL_IMAGE_PIP_PACKAGES:
        image = image.pip_install(*_SOCIAL_IMAGE_PIP_PACKAGES)
    if include_browser_runtime:
        image = image.run_commands(*_SOCIAL_BROWSER_SETUP_COMMANDS)
    image = image.add_local_python_source("api", "trr_backend")
    for local_path, remote_path in _SOCIAL_IMAGE_LOCAL_FILES:
        image = image.add_local_file(local_path, remote_path=remote_path)
    for local_path, remote_path in _SOCIAL_IMAGE_LOCAL_DIRS:
        image = image.add_local_dir(local_path, remote_path=remote_path)
    return image


def _modal_capacity_metadata(
    *,
    worker_family: str,
    function_name: str,
    image_family: str,
    timeout_seconds: int,
    max_containers: int,
    min_containers: int = 0,
) -> dict[str, object]:
    return {
        "worker_family": worker_family,
        "modal_app": _APP_NAME,
        "modal_function": function_name,
        "image_family": image_family,
        "timeout_seconds": int(timeout_seconds),
        "min_containers": int(min_containers),
        "max_containers": int(max_containers),
    }


def modal_completion_evidence_contract() -> dict[str, object]:
    """Return the non-secret completion evidence expected for Modal-affecting work."""
    return {
        "local_verification": [
            "python3.11 -m pytest tests/api/test_health.py tests/test_modal_jobs.py -q",
            ".venv/bin/python scripts/modal/verify_modal_readiness.py --json",
        ],
        "modal_update_status_required": True,
        "required_completion_fields": [
            "local_verification_status",
            "modal_update_status",
            "modal_readiness_status",
            "blocker",
        ],
        "blocker_required_when_not_updated": True,
        "readiness_command": "cd TRR-Backend && .venv/bin/python scripts/modal/verify_modal_readiness.py",
    }


_image = _build_lean_image_base()
_media_image = _build_media_image_base()

_vision_image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install("libgl1", "libglib2.0-0")
    .pip_install_from_requirements(
        str(_MODAL_VISION_REQUIREMENTS),
        extra_index_url="https://download.pytorch.org/whl/cpu",
    )
    .add_local_python_source("api", "trr_backend")
)

_browser_image = _build_social_image_base(include_browser_runtime=True)
_FUNCTION_IMAGE_BINDINGS: Final[dict[str, object]] = {
    "serve_backend_api": _image,
    "run_admin_operation": _image,
    "run_admin_operation_v2": _image,
    "run_google_news_sync": _image,
    "run_reddit_refresh": _image,
    "probe_reddit_refresh_runtime": _image,
    "probe_admin_operation_runtime": _image,
    "probe_google_news_runtime": _image,
    "run_social_job": _browser_image,
    "run_social_posts_job": _browser_image,
    "run_social_media_job": _browser_image,
    "run_social_comments_job": _browser_image,
    "run_social_comments_recovery_job": _browser_image,
    "run_socialblade_scrape": _browser_image,
    "probe_socialblade_runtime": _browser_image,
    "probe_browser_image_runtime": _browser_image,
    "heartbeat_remote_executors": _image,
    "sync_nbcumv_official_images": _image,
    "purge_stale_social_worker_heartbeats": _image,
    "run_admin_vision": _vision_image,
    "run_cast_screentime_analysis": _vision_image,
    "run_cast_screentime_subtitle_extraction": _media_image,
    "probe_admin_vision_runtime": _vision_image,
}


def _env_flag(name: str, *, default: bool = False) -> bool:
    raw = str(os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _modal_always_on_schedules_enabled() -> bool:
    return _env_flag("TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED", default=False)


def _modal_runtime_scheduler_owner_enabled() -> bool:
    return _env_flag("TRR_MODAL_RUNTIME_SCHEDULER_ENABLED", default=False)


def _modal_maintenance_owner_required() -> bool:
    if _is_local_or_dev_runtime():
        return _env_flag("TRR_MODAL_MAINTENANCE_OWNER_REQUIRED", default=False)
    return True


def _modal_maintenance_owner_names() -> list[str]:
    owners: list[str] = []
    if _modal_always_on_schedules_enabled():
        owners.append("modal_singleton_cron")
    if _modal_runtime_scheduler_owner_enabled():
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


def _modal_cron_schedule_kwargs(expression: str) -> dict[str, object]:
    if not _modal_always_on_schedules_enabled():
        return {}
    return {"schedule": modal.Cron(expression, timezone=_TIMEZONE)}


def _runtime_secret_name() -> str:
    return str(os.getenv("TRR_MODAL_RUNTIME_SECRET_NAME") or os.getenv("TRR_MODAL_SECRET_NAME") or "").strip()


def _social_secret_name() -> str:
    return str(os.getenv("TRR_MODAL_SOCIAL_SECRET_NAME") or "").strip()


def _is_local_or_dev_runtime() -> bool:
    runtime_markers = (
        os.getenv("APP_ENV"),
        os.getenv("ENV"),
        os.getenv("ENVIRONMENT"),
        os.getenv("TRR_ENV"),
        os.getenv("TRR_ENVIRONMENT"),
        os.getenv("WORKSPACE_ENV"),
    )
    normalized = {str(value or "").strip().lower() for value in runtime_markers if str(value or "").strip()}
    if normalized & _LOCAL_RUNTIME_MARKERS:
        return True
    if _env_flag("TRR_LOCAL_DEV") or _env_flag("TRR_MODAL_ALLOW_DOTENV_FALLBACK"):
        return True
    if "pytest" in sys.modules:
        return True
    if os.getenv("PYTEST_CURRENT_TEST"):
        return True
    return False


def _require_named_secrets() -> bool:
    return _env_flag("TRR_MODAL_ENABLED", default=False) and not _is_local_or_dev_runtime()


def _allow_dotenv_secret_fallback() -> bool:
    return _env_flag("TRR_MODAL_ALLOW_DOTENV_FALLBACK", default=False) and _is_local_or_dev_runtime()


def _api_custom_domains() -> list[str] | None:
    raw = str(os.getenv("TRR_MODAL_API_CUSTOM_DOMAINS") or "").strip()
    if not raw:
        return None
    domains = [segment.strip() for segment in raw.split(",") if segment.strip()]
    return domains or None


def _resolve_modal_secrets() -> list[Any]:
    explicit_runtime_secret_name = _runtime_secret_name()
    explicit_social_secret_name = _social_secret_name()

    if explicit_runtime_secret_name and explicit_social_secret_name:
        return [
            modal.Secret.from_name(explicit_runtime_secret_name),
            modal.Secret.from_name(explicit_social_secret_name),
        ]

    if explicit_runtime_secret_name or explicit_social_secret_name:
        missing = []
        if not explicit_runtime_secret_name:
            missing.append("TRR_MODAL_RUNTIME_SECRET_NAME")
        if not explicit_social_secret_name:
            missing.append("TRR_MODAL_SOCIAL_SECRET_NAME")
        raise RuntimeError(
            f"Modal secret configuration is partial. Set both named secrets or neither. Missing: {', '.join(missing)}"
        )

    if _allow_dotenv_secret_fallback():
        return [modal.Secret.from_dotenv(_BACKEND_ROOT)]

    if _env_flag("TRR_MODAL_ENABLED", default=False):
        return [
            modal.Secret.from_name(_DEFAULT_RUNTIME_SECRET_NAME),
            modal.Secret.from_name(_DEFAULT_SOCIAL_SECRET_NAME),
        ]

    if _is_local_or_dev_runtime():
        return [modal.Secret.from_dotenv(_BACKEND_ROOT)]

    # Keep production/staging deploys deterministic even when the secret name env vars
    # are not present inside the remote import environment.
    return [
        modal.Secret.from_name(_DEFAULT_RUNTIME_SECRET_NAME),
        modal.Secret.from_name(_DEFAULT_SOCIAL_SECRET_NAME),
    ]


def _inject_modal_runtime_defaults() -> None:
    for key, value in _CANONICAL_MODAL_RUNTIME_DEFAULTS.items():
        if key in _OPERATOR_TUNABLE_RUNTIME_DEFAULT_KEYS:
            os.environ.setdefault(key, value)
        else:
            os.environ[key] = value
    if _env_flag("TRR_PREVIEW_READ_ONLY", default=False):
        # The isolated preview never owns queue consumers or dispatch. Keep this
        # explicit after canonical defaults, which otherwise pin the queue on.
        os.environ["SOCIAL_QUEUE_ENABLED"] = "false"
    if (os.getenv("OBJECT_STORAGE_ACCESS_KEY_ID") or "").strip() and (
        os.getenv("OBJECT_STORAGE_SECRET_ACCESS_KEY") or ""
    ).strip():
        os.environ.pop("OBJECT_STORAGE_PROFILE", None)


def _modal_api_runtime_env() -> dict[str, str]:
    """Return explicit, non-secret API rollout controls for Modal containers."""
    read_mode = str(os.getenv(_MODAL_INSTAGRAM_PAYLOAD_READ_MODE_ENV) or "legacy").strip().lower()
    if read_mode not in _INSTAGRAM_PAYLOAD_READ_MODES:
        allowed = ", ".join(sorted(_INSTAGRAM_PAYLOAD_READ_MODES))
        raise RuntimeError(f"{_MODAL_INSTAGRAM_PAYLOAD_READ_MODE_ENV} must be one of: {allowed}")
    if read_mode != "compare":
        sample_rate = 0.0
    else:
        raw_sample_rate = str(os.getenv(_MODAL_INSTAGRAM_PAYLOAD_COMPARE_SAMPLE_RATE_ENV) or "").strip()
        try:
            sample_rate = float(raw_sample_rate) if raw_sample_rate else _DEFAULT_INSTAGRAM_PAYLOAD_COMPARE_SAMPLE_RATE
        except ValueError as exc:
            raise RuntimeError(
                f"{_MODAL_INSTAGRAM_PAYLOAD_COMPARE_SAMPLE_RATE_ENV} must be greater than 0 and at most 1"
            ) from exc
        if not math.isfinite(sample_rate) or not 0.0 < sample_rate <= 1.0:
            raise RuntimeError(
                f"{_MODAL_INSTAGRAM_PAYLOAD_COMPARE_SAMPLE_RATE_ENV} must be greater than 0 and at most 1"
            )
    return {
        _INSTAGRAM_PAYLOAD_READ_MODE_ENV: read_mode,
        _INSTAGRAM_PAYLOAD_COMPARE_SAMPLE_RATE_ENV: format(sample_rate, "g"),
    }


def _worker_id(worker_family: str) -> str:
    normalized = str(worker_family or "worker").strip().lower().replace(" ", "-")
    return f"modal:{normalized}:{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


def _worker_started(family: str, **metadata: object) -> float:
    started_at = time.monotonic()
    logger.info(
        "[modal_worker_start] family=%s app=%s function=%s metadata=%s",
        family,
        _APP_NAME,
        str(metadata.pop("function_name", "") or ""),
        metadata,
    )
    return started_at


def _worker_finished(family: str, started_at: float, *, result_status: str, **metadata: object) -> None:
    elapsed_ms = int((time.monotonic() - started_at) * 1000)
    logger.info(
        "[modal_worker_finish] family=%s app=%s status=%s elapsed_ms=%s metadata=%s",
        family,
        _APP_NAME,
        result_status,
        elapsed_ms,
        metadata,
    )


def _worker_failed(family: str, started_at: float, *, failure_class: str, **metadata: object) -> None:
    elapsed_ms = int((time.monotonic() - started_at) * 1000)
    logger.exception(
        "[modal_worker_failed] family=%s app=%s failure_class=%s elapsed_ms=%s metadata=%s",
        family,
        _APP_NAME,
        failure_class,
        elapsed_ms,
        metadata,
    )


def _close_db_pools_after_worker(worker_family: str, **metadata: object) -> None:
    try:
        from trr_backend.db import pg

        pg.close_pool()
    except Exception:  # noqa: BLE001
        logger.exception("[modal_worker_db_cleanup_failed] family=%s metadata=%s", worker_family, metadata)


_secrets = _resolve_modal_secrets()
_inject_modal_runtime_defaults()
# Phase 2: surface a comments-lane DB session-budget breach at container startup
# (after canonical defaults are injected) so an unsafe concurrency/container-cap
# config is visible in logs before it can exhaust the Supavisor session pool.
_log_comments_db_session_budget()
configure_runtime_observability(service_name="trr-backend-modal-jobs")

app = modal.App(_APP_NAME, image=_image)


@app.function(
    name="probe_browser_image_runtime",
    image=_FUNCTION_IMAGE_BINDINGS["probe_browser_image_runtime"],
    retries=0,
    timeout=2 * 60,
)
def probe_browser_image_runtime() -> dict[str, object]:
    """Prove the browser image can import and launch Chromium without state."""
    # Keep this probe deliberately state-free: no Secret, database, Volume, Queue,
    # cookie, external URL, or browser storage is involved.
    started = time.monotonic()
    try:
        # Importing the concrete fetcher catches Scrapling/Patchright integration
        # drift without constructing a session or making a network request.
        from patchright.sync_api import sync_playwright as patchright_sync_playwright
        from playwright.sync_api import sync_playwright
        from scrapling.fetchers import StealthyFetcher

        _ = StealthyFetcher, patchright_sync_playwright
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                browser_version = str(browser.version)
            finally:
                # A Playwright Browser must be closed before its owning
                # sync_playwright context shuts down. Closing it afterward
                # attempts to use an event loop that Playwright already ended.
                browser.close()
        return {
            "healthy": True,
            "reason": "ok",
            "worker_family": "browser_image",
            "state_free": True,
            "timestamp": datetime.now(UTC).isoformat(),
            "elapsed_ms": round((time.monotonic() - started) * 1000, 1),
            "versions": {
                "python": sys.version.split()[0],
                "modal": package_version("modal"),
                "scrapling": package_version("scrapling"),
                "playwright": package_version("playwright"),
                "patchright": package_version("patchright"),
                "chromium": browser_version,
            },
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "healthy": False,
            "reason": "browser_image_probe_failed",
            "state_free": True,
            "timestamp": datetime.now(UTC).isoformat(),
            "elapsed_ms": round((time.monotonic() - started) * 1000, 1),
            "error_class": type(exc).__name__,
            "error": str(exc),
        }


@app.function(
    name=_API_FUNCTION_NAME,
    secrets=_secrets,
    timeout=60 * 60,
    min_containers=_API_MIN_CONTAINERS,
    max_containers=_API_MAX_CONTAINERS,
    env=_modal_api_runtime_env(),
)
@modal.asgi_app(label=_API_LABEL, custom_domains=_api_custom_domains())
def serve_backend_api():
    from api.main import app as fastapi_app

    return fastapi_app


def _execute_admin_operation(operation_id: str, operation_type: str) -> dict[str, object]:
    from trr_backend.pipeline.admin_operation_bootstrap import register_admin_operation_providers
    from trr_backend.pipeline.admin_operations import (
        claim_and_execute_operation,
        wait_for_sub_operation_dependencies,
    )

    register_admin_operation_providers()
    worker_id = _worker_id("admin-operation")
    started_at = _worker_started(
        "admin_operations",
        function_name="run_admin_operation_v2",
        operation_id=operation_id,
        operation_type=operation_type,
        worker_id=worker_id,
    )
    try:
        if not wait_for_sub_operation_dependencies(operation_id):
            result = {
                "operation_id": operation_id,
                "operation_type": operation_type,
                "claimed": False,
                "worker_id": worker_id,
                "reason": "dependency_not_satisfied",
                "worker_family": "admin_operations",
            }
            _worker_finished("admin_operations", started_at, result_status="skipped", **result)
            return result

        claimed = claim_and_execute_operation(
            operation_id=operation_id,
            worker_id=worker_id,
            operation_types=[operation_type],
        )
        result = {
            "operation_id": operation_id,
            "operation_type": operation_type,
            "claimed": claimed,
            "worker_id": worker_id,
            "worker_family": "admin_operations",
        }
        _worker_finished(
            "admin_operations",
            started_at,
            result_status="completed" if claimed else "skipped",
            **result,
        )
        return result
    except Exception as exc:
        _worker_failed(
            "admin_operations",
            started_at,
            failure_class=type(exc).__name__,
            operation_id=operation_id,
            operation_type=operation_type,
            worker_id=worker_id,
        )
        raise
    finally:
        _close_db_pools_after_worker("admin_operations", operation_id=operation_id, worker_id=worker_id)


def _admin_operation_runtime_probe_payload() -> dict[str, object]:
    from trr_backend.pipeline import admin_operations as admin_operations_module

    return {
        "healthy": True,
        "reason": "ok",
        "worker_family": "admin_operations",
        "supports_claim_by_id": hasattr(admin_operations_module, "claim_and_execute_operation"),
        "supports_claim_heartbeat": hasattr(admin_operations_module.admin_operations, "heartbeat_operation_claim"),
        "execution_backend": "modal",
    }


@app.function(
    name="probe_admin_operation_runtime",
    image=_FUNCTION_IMAGE_BINDINGS["probe_admin_operation_runtime"],
    secrets=_secrets,
    retries=0,
    timeout=60,
    max_containers=1,
)
def probe_admin_operation_runtime() -> dict[str, object]:
    return _admin_operation_runtime_probe_payload()


def _google_news_runtime_probe_payload() -> dict[str, object]:
    from api.routers import admin_show_news

    return {
        "healthy": True,
        "reason": "ok",
        "worker_family": "google_news",
        "supports_claim_by_id": hasattr(admin_show_news, "claim_and_execute_google_news_sync_job"),
        "supports_heartbeat": hasattr(admin_show_news, "_touch_google_news_sync_job_heartbeat"),
        "execution_backend": "modal",
    }


@app.function(
    name="probe_google_news_runtime",
    image=_FUNCTION_IMAGE_BINDINGS["probe_google_news_runtime"],
    secrets=_secrets,
    retries=0,
    timeout=60,
    max_containers=1,
)
def probe_google_news_runtime() -> dict[str, object]:
    return _google_news_runtime_probe_payload()


def _vision_runtime_probe_payload() -> dict[str, object]:
    from trr_backend.vision import people_count_engine

    return {
        "healthy": True,
        "reason": "ok",
        "worker_family": "admin_vision",
        "supports_single": hasattr(people_count_engine, "compute_people_count"),
        "supports_batch": hasattr(people_count_engine, "compute_people_count_batch"),
        "execution_backend": "modal",
    }


@app.function(
    name="probe_admin_vision_runtime",
    image=_FUNCTION_IMAGE_BINDINGS["probe_admin_vision_runtime"],
    secrets=_secrets,
    retries=0,
    timeout=2 * 60,
    max_containers=1,
)
def probe_admin_vision_runtime() -> dict[str, object]:
    return _vision_runtime_probe_payload()


def _socialblade_runtime_probe_payload() -> dict[str, object]:
    from trr_backend.socials.socialblade.auth import load_socialblade_cookies_from_sources
    from trr_backend.socials.socialblade.service import sanitize_socialblade_platform

    cookies = load_socialblade_cookies_from_sources()
    return {
        "healthy": True,
        "reason": "ok",
        "worker_family": "socialblade",
        "has_cookie_source": bool(cookies),
        "supports_instagram": sanitize_socialblade_platform("instagram") == "instagram",
        "execution_backend": "modal",
    }


@app.function(
    name="probe_socialblade_runtime",
    image=_FUNCTION_IMAGE_BINDINGS["probe_socialblade_runtime"],
    secrets=_secrets,
    retries=0,
    timeout=2 * 60,
    max_containers=1,
)
def probe_socialblade_runtime() -> dict[str, object]:
    return _socialblade_runtime_probe_payload()


@app.function(
    name="run_admin_operation",
    image=_FUNCTION_IMAGE_BINDINGS["run_admin_operation"],
    secrets=_secrets,
    retries=0,
    timeout=_ADMIN_OPERATION_TIMEOUT_SECONDS,
    min_containers=_ADMIN_KEEP_WARM,
    max_containers=_ADMIN_CONCURRENCY_LIMIT,
)
def run_admin_operation(operation_id: str, operation_type: str) -> dict[str, object]:
    return _execute_admin_operation(operation_id, operation_type)


@app.function(
    name="run_admin_operation_v2",
    image=_FUNCTION_IMAGE_BINDINGS["run_admin_operation_v2"],
    secrets=_secrets,
    retries=0,
    timeout=_ADMIN_OPERATION_TIMEOUT_SECONDS,
    min_containers=_ADMIN_KEEP_WARM,
    max_containers=_ADMIN_CONCURRENCY_LIMIT,
)
def run_admin_operation_v2(operation_id: str, operation_type: str) -> dict[str, object]:
    return _execute_admin_operation(operation_id, operation_type)


def _show_season_media_watch_lease_seconds() -> int:
    return max(30, min(int(os.getenv("TRR_MEDIA_WATCH_LEASE_SECONDS", "180")), 3600))


def _show_season_media_watch_dispatch_limit() -> int:
    return max(1, min(int(os.getenv("TRR_MEDIA_WATCH_DISPATCH_LIMIT", "4")), 16))


def _show_season_media_watch_retry_seconds(watch: dict[str, object]) -> int:
    """Use bounded, deterministic jitter after persisted failures.

    The service owns the final fenced write.  Supplying its cadence before the
    run begins keeps that final write atomic with the run journal, while a
    successful run naturally resets to the configured cadence.
    """
    cadence = max(1, int(cast("int | str", watch.get("poll_interval_seconds") or 60)))
    failures = max(0, int(cast("int | str", watch.get("consecutive_failures") or 0)))
    if not failures:
        return cadence
    maximum = max(cadence, min(int(os.getenv("TRR_MEDIA_WATCH_MAX_RETRY_SECONDS", "3600")), 86_400))
    exponential = min(maximum, cadence * (2 ** min(failures, 10)))
    seed = sum(ord(char) for char in f"{watch.get('id', '')}:{watch.get('lease_fence', '')}") % 21
    return max(1, min(maximum, int(exponential * (0.90 + (seed / 100)))))


@app.function(
    name="run_show_season_media_watch_worker",
    image=_image,
    secrets=_secrets,
    retries=0,
    timeout=60 * 60,
    max_containers=16,
)
def run_show_season_media_watch_worker(
    watch: dict[str, object],
    lease_owner: str,
    lease_fence: int,
    backfill: bool | None = None,
) -> dict[str, object]:
    """Run one already-claimed watch; never attempt an inline lease fallback."""
    from trr_backend.media.watchers.service import run_show_season_media_watch
    from trr_backend.repositories import media_watchers

    watch_id = str(watch.get("id") or "").strip()
    started_at = _worker_started(
        "show_season_media_watch",
        function_name="run_show_season_media_watch_worker",
        watch_id=watch_id,
        lease_fence=lease_fence,
    )
    try:
        if not watch_id or not str(lease_owner or "").strip():
            raise ValueError("watch_id and lease_owner are required")
        if not media_watchers.heartbeat_lease(
            watch_id=watch_id,
            lease_owner=lease_owner,
            lease_fence=int(lease_fence),
            lease_seconds=_show_season_media_watch_lease_seconds(),
        ):
            result = {"watch_id": watch_id, "lease_fence": int(lease_fence), "status": "fenced"}
            _worker_finished("show_season_media_watch", started_at, result_status="fenced", **result)
            return result
        effective_watch = dict(watch)
        effective_watch["poll_interval_seconds"] = _show_season_media_watch_retry_seconds(effective_watch)
        result = run_show_season_media_watch(
            effective_watch,
            lease_owner=lease_owner,
            lease_fence=int(lease_fence),
            backfill=backfill,
        )
        payload = {
            "watch_id": watch_id,
            "lease_fence": int(lease_fence),
            "status": result.status,
            "run_id": result.run_id,
            "summary": result.summary,
            "continuation": result.continuation,
            "error": result.error,
        }
        _worker_finished("show_season_media_watch", started_at, result_status=result.status, **payload)
        return payload
    except Exception as exc:
        _worker_failed(
            "show_season_media_watch",
            started_at,
            failure_class=type(exc).__name__,
            watch_id=watch_id,
            lease_fence=lease_fence,
        )
        raise
    finally:
        _close_db_pools_after_worker("show_season_media_watch", watch_id=watch_id, lease_fence=lease_fence)


def _poll_due_show_season_media_watches_impl() -> dict[str, object]:
    """Singleton minute poller that claims a bounded number of due watches."""
    owner_config = _validate_modal_maintenance_owner_config()
    if owner_config not in {"modal_singleton_cron", "api_runtime_scheduler"}:
        return {
            "status": "disabled",
            "reason": "maintenance_scheduler_not_owner",
            "maintenance_owner": owner_config,
            "claimed": 0,
            "dispatched": 0,
        }

    from trr_backend.repositories import media_watchers

    owner = _worker_id("show-season-media-poller")
    claimed: list[dict[str, object]] = []
    dispatch_errors: list[str] = []
    started_at = _worker_started(
        "show_season_media_poller",
        function_name="poll_due_show_season_media_watches",
        dispatch_limit=_show_season_media_watch_dispatch_limit(),
    )
    try:
        for _ in range(_show_season_media_watch_dispatch_limit()):
            watch = media_watchers.claim_due_watch(
                lease_owner=owner,
                lease_seconds=_show_season_media_watch_lease_seconds(),
            )
            if watch is None:
                break
            claimed.append(dict(watch))
        for watch in claimed:
            try:
                run_show_season_media_watch_worker.spawn(
                    watch=watch,
                    lease_owner=owner,
                    lease_fence=int(cast("int | str", watch["lease_fence"])),
                    backfill=None,
                )
            except Exception as exc:  # Lease expiry is the safe recovery path; never run inline.
                logger.exception("[show-season-media-poller] worker dispatch failed watch_id=%s", watch.get("id"))
                dispatch_errors.append(f"{watch.get('id')}: {type(exc).__name__}")
        payload = {
            "status": "completed" if not dispatch_errors else "partial",
            "claimed": len(claimed),
            "dispatched": len(claimed) - len(dispatch_errors),
            "dispatch_errors": dispatch_errors,
            "lease_owner": owner,
        }
        _worker_finished("show_season_media_poller", started_at, result_status=str(payload["status"]), **payload)
        return payload
    except Exception as exc:
        _worker_failed("show_season_media_poller", started_at, failure_class=type(exc).__name__)
        raise
    finally:
        _close_db_pools_after_worker("show_season_media_poller")


@app.function(
    name="poll_due_show_season_media_watches",
    image=_image,
    secrets=_secrets,
    retries=0,
    timeout=5 * 60,
    max_containers=1,
)
def poll_due_show_season_media_watches() -> dict[str, object]:
    """On-demand entrypoint; the minute heartbeat owns the durable clock."""
    return _poll_due_show_season_media_watches_impl()


@app.function(
    image=_FUNCTION_IMAGE_BINDINGS["run_google_news_sync"],
    secrets=_secrets,
    retries=0,
    timeout=_GOOGLE_NEWS_TIMEOUT_SECONDS,
    max_containers=_GOOGLE_NEWS_CONCURRENCY_LIMIT,
)
def run_google_news_sync(job_id: str) -> dict[str, object]:
    from api.routers.admin_show_news import claim_and_execute_google_news_sync_job

    worker_id = _worker_id("google-news")
    started_at = _worker_started(
        "google_news",
        function_name="run_google_news_sync",
        job_id=job_id,
        worker_id=worker_id,
    )
    try:
        claimed = claim_and_execute_google_news_sync_job(job_id=job_id, worker_id=worker_id)
        result = {
            "job_id": job_id,
            "claimed": claimed,
            "worker_id": worker_id,
            "worker_family": "google_news",
        }
        _worker_finished("google_news", started_at, result_status="completed" if claimed else "skipped", **result)
        return result
    except Exception as exc:
        _worker_failed("google_news", started_at, failure_class=type(exc).__name__, job_id=job_id, worker_id=worker_id)
        raise
    finally:
        _close_db_pools_after_worker("google_news", job_id=job_id, worker_id=worker_id)


@app.function(
    image=_FUNCTION_IMAGE_BINDINGS["run_reddit_refresh"],
    secrets=_secrets,
    retries=0,
    timeout=_REDDIT_REFRESH_TIMEOUT_SECONDS,
    max_containers=_REDDIT_REFRESH_CONCURRENCY_LIMIT,
)
def run_reddit_refresh(run_id: str) -> dict[str, object]:
    from trr_backend.repositories.reddit_refresh import execute_refresh_run

    worker_id = _worker_id("reddit-refresh")
    started_at = _worker_started(
        "reddit_refresh",
        function_name="run_reddit_refresh",
        run_id=run_id,
        worker_id=worker_id,
    )
    try:
        result = execute_refresh_run(run_id, worker_id=worker_id)
        payload: dict[str, object] = {
            "run_id": run_id,
            "status": str(result.get("status") or ""),
            "worker_id": worker_id,
            "worker_family": "reddit_refresh",
        }
        _worker_finished("reddit_refresh", started_at, result_status=str(payload["status"] or "completed"), **payload)
        return payload
    except Exception as exc:
        _worker_failed(
            "reddit_refresh",
            started_at,
            failure_class=type(exc).__name__,
            run_id=run_id,
            worker_id=worker_id,
        )
        raise
    finally:
        _close_db_pools_after_worker("reddit_refresh", run_id=run_id, worker_id=worker_id)


def _reddit_runtime_probe_payload() -> dict[str, object]:
    from trr_backend.repositories.reddit_refresh import REDDIT_USER_AGENT_DEFAULT

    client_id = (os.getenv("REDDIT_CLIENT_ID") or "").strip()
    client_secret = (os.getenv("REDDIT_CLIENT_SECRET") or "").strip()
    user_agent = (os.getenv("REDDIT_USER_AGENT") or "").strip()
    missing_env = []
    if not client_id:
        missing_env.append("REDDIT_CLIENT_ID")
    if not client_secret:
        missing_env.append("REDDIT_CLIENT_SECRET")
    warnings = []
    if not user_agent:
        warnings.append("REDDIT_USER_AGENT")
    healthy = not missing_env
    return {
        "healthy": healthy,
        "reason": "ok" if healthy else "reddit_oauth_missing",
        "missing_env": missing_env,
        "warnings": warnings,
        "supports_oauth": healthy,
        "user_agent_configured": bool(user_agent),
        "uses_default_user_agent": not bool(user_agent),
        "effective_user_agent": user_agent or REDDIT_USER_AGENT_DEFAULT,
    }


@app.function(
    image=_FUNCTION_IMAGE_BINDINGS["probe_reddit_refresh_runtime"],
    secrets=_secrets,
    retries=0,
    timeout=60,
    max_containers=1,
)
def probe_reddit_refresh_runtime() -> dict[str, object]:
    return _reddit_runtime_probe_payload()


@app.function(
    name=str(os.getenv("TRR_MODAL_SOCIAL_AUTH_PROBE_FUNCTION") or "probe_social_remote_auth").strip()
    or "probe_social_remote_auth",
    image=_FUNCTION_IMAGE_BINDINGS["run_social_job"],
    secrets=_secrets,
    retries=0,
    timeout=5 * 60,
)
def probe_social_remote_auth(platform: str) -> dict[str, object]:
    from trr_backend.socials.control_plane import probe_remote_auth_health

    return probe_remote_auth_health(platform)


@app.function(
    name=str(os.getenv("TRR_MODAL_INSTAGRAM_POSTS_AUTH_PROBE_FUNCTION") or "probe_instagram_posts_auth").strip()
    or "probe_instagram_posts_auth",
    image=_FUNCTION_IMAGE_BINDINGS["run_social_posts_job"],
    secrets=_secrets,
    retries=0,
    timeout=5 * 60,
)
def probe_instagram_posts_auth(account_handle: str) -> dict[str, object]:
    if _instagram_public_first_mode_enabled():
        return {
            "platform": "instagram",
            "account_handle": str(account_handle or "").strip().lower().lstrip("@"),
            "ready": True,
            "execution_backend": "modal",
            "status": "public",
            "result": "public",
            "instagram_scrape_mode": "public_first",
            "auth_state": "public",
            "proxy_state": "none",
            "auth_probe_skipped": True,
            "fallback_policy": {
                "auth_fallback": "requires_approval",
                "proxy_fallback": "requires_approval",
            },
        }
    from trr_backend.socials.pipelines.account_catalog.launch import _probe_instagram_posts_endpoint_for_launch

    payload = dict(_probe_instagram_posts_endpoint_for_launch(account_handle=account_handle))
    status = str(payload.get("status") or payload.get("result") or "").strip().lower()
    payload.update(
        {
            "platform": "instagram",
            "ready": status == "valid",
            "execution_backend": "modal",
        }
    )
    return payload


@app.function(
    name=str(os.getenv("TRR_MODAL_INSTAGRAM_COMMENTS_AUTH_PROBE_FUNCTION") or "probe_instagram_comments_auth").strip()
    or "probe_instagram_comments_auth",
    image=_FUNCTION_IMAGE_BINDINGS["run_social_comments_job"],
    secrets=_secrets,
    retries=0,
    timeout=5 * 60,
)
def probe_instagram_comments_auth(
    account_handle: str,
    shortcode: str,
    strict_authenticated: bool = False,
) -> dict[str, object]:
    strict_authenticated = bool(strict_authenticated)
    if _instagram_public_first_mode_enabled() and not strict_authenticated:
        return {
            "platform": "instagram",
            "account_handle": str(account_handle or "").strip().lower().lstrip("@"),
            "shortcode": str(shortcode or "").strip(),
            "ready": True,
            "public_ready": True,
            "authenticated_ready": False,
            "execution_backend": "modal",
            "status": "public",
            "result": "public",
            "instagram_scrape_mode": "public_first",
            "auth_state": "public",
            "proxy_state": "none",
            "auth_probe_skipped": True,
            "auth_required_for_hidden_comments": True,
            "comments_auth_blocker": "strict_authenticated_probe_not_requested",
            "operator_action": "Run a strict Instagram comments auth probe before launching hidden-comments recovery.",
            "fallback_policy": {
                "auth_fallback": "requires_approval",
                "proxy_fallback": "requires_approval",
            },
        }
    from trr_backend.socials.pipelines.comments.instagram import _probe_instagram_comments_endpoint_for_launch

    payload = dict(
        _probe_instagram_comments_endpoint_for_launch(
            account_handle=account_handle,
            shortcode=shortcode,
        )
    )
    status = str(payload.get("status") or payload.get("result") or "").strip().lower()
    authenticated_ready = status == "valid"
    reason = str(payload.get("reason") or "").strip().lower()
    rate_limited = reason in {"http_429", "rate_limited"} or "429" in reason or status == "rate_limited"
    try:
        cooldown_seconds = int(float(os.getenv("TRR_MODAL_INSTAGRAM_COMMENTS_AUTH_RATE_LIMIT_CACHE_SECONDS") or "300"))
    except ValueError:
        cooldown_seconds = 300
    cooldown_seconds = min(max(cooldown_seconds, 1), 1_800)
    payload.update(
        {
            "platform": "instagram",
            "account_handle": account_handle,
            "ready": authenticated_ready,
            "public_ready": True,
            "authenticated_ready": authenticated_ready,
            "execution_backend": "modal",
            "auth_probe_skipped": False,
            "auth_required_for_hidden_comments": not authenticated_ready,
            "comments_auth_blocker": None
            if authenticated_ready
            else str(payload.get("reason") or status or "comments_auth_probe_failed").strip()
            or "comments_auth_probe_failed",
            "rate_limited": rate_limited,
            "cooldown_recommended_seconds": cooldown_seconds if rate_limited else None,
            "operator_action": None
            if authenticated_ready
            else (
                f"Instagram comments auth probe is rate-limited. Wait at least {cooldown_seconds} seconds, "
                "then rerun the strict comments auth probe before launching hidden-comments recovery."
                if rate_limited
                else "Repair Instagram comments auth, then rerun the strict comments auth probe."
            ),
        }
    )
    return payload


@app.function(
    name=str(os.getenv("TRR_MODAL_INSTAGRAM_PUBLIC_HISTORY_PROBE_FUNCTION") or "probe_instagram_public_history").strip()
    or "probe_instagram_public_history",
    image=_FUNCTION_IMAGE_BINDINGS["run_social_comments_job"],
    secrets=_secrets,
    retries=0,
    timeout=int(os.getenv("TRR_MODAL_INSTAGRAM_PUBLIC_HISTORY_PROBE_TIMEOUT_SECONDS") or str(2 * 60 * 60)),
    max_containers=1,
)
def probe_instagram_public_history(
    account_handle: str = "bravotv",
    until_date: str = "2025-01-01",
    target_years: str = "2025,2026",
    max_pages: int = 0,
    continue_after_boundary: bool = True,
    sample_details_per_page: int = 2,
    sample_comments_per_page: int = 1,
    comments_mode: str = "sampled",
    details_mode: str = "sampled",
    resume: bool = False,
    state_file: str | None = None,
    state_payload: dict[str, object] | None = None,
    output_file: str | None = None,
    scrub_public_env: bool = True,
) -> dict[str, object]:
    import json
    from dataclasses import asdict
    from datetime import date
    from pathlib import Path

    from trr_backend.socials.instagram.public_probe import (
        AUTH_ENV_VARS,
        COOKIE_ENV_VARS,
        DECODO_ENV_VARS,
        PROXY_ENV_VARS,
        PROXY_PROVIDER_ENV_VARS,
        PublicProbeConfig,
        parse_target_years,
        run_public_probe,
    )

    normalized_account = str(account_handle or "").strip().lower().lstrip("@") or "bravotv"
    probe_state_file = Path(state_file or f"/tmp/trr-{normalized_account}-public-probe-state.json")
    probe_output_file = Path(output_file or f"/tmp/trr-{normalized_account}-public-probe-output.json")
    if isinstance(state_payload, dict) and state_payload:
        probe_state_file.parent.mkdir(parents=True, exist_ok=True)
        probe_state_file.write_text(json.dumps(state_payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    scrubbed_env: dict[str, str | None] = {}
    scrubbed_names = [
        *COOKIE_ENV_VARS,
        *DECODO_ENV_VARS,
        *PROXY_ENV_VARS,
        *AUTH_ENV_VARS,
        *PROXY_PROVIDER_ENV_VARS,
    ]
    if scrub_public_env:
        for name in scrubbed_names:
            if name in os.environ:
                scrubbed_env[name] = os.environ.pop(name)
        os.environ["SOCIAL_INSTAGRAM_POSTS_PROXY_PROVIDER"] = "none"
        os.environ["SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER"] = "none"
    try:
        result = run_public_probe(
            PublicProbeConfig(
                account=normalized_account,
                until_date=date.fromisoformat(str(until_date)),
                target_years=parse_target_years(target_years),
                max_pages=max_pages,
                continue_after_boundary=bool(continue_after_boundary),
                sample_details_per_page=sample_details_per_page,
                sample_comments_per_page=sample_comments_per_page,
                comments_mode=comments_mode,
                details_mode=details_mode,
                state_file=probe_state_file,
                output=probe_output_file,
                resume=resume,
                strict_public=True,
                fail_if_cookies=True,
                fail_if_decodo=True,
                retry_profile="patient",
            )
        )
        payload = asdict(result)
        payload["execution_backend"] = "modal"
        payload["output_file"] = str(probe_output_file)
        payload["modal_public_env_scrubbed"] = sorted(scrubbed_env)
        if probe_state_file.exists():
            try:
                payload["state_payload"] = json.loads(probe_state_file.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                payload["state_payload"] = None
        return payload
    finally:
        if scrub_public_env:
            for name in PROXY_PROVIDER_ENV_VARS:
                os.environ.pop(name, None)
            for name, value in scrubbed_env.items():
                if value is not None:
                    os.environ[name] = value


@app.function(
    name=str(os.getenv("TRR_MODAL_GETTY_REMOTE_PROBE_FUNCTION") or "probe_getty_remote_access").strip()
    or "probe_getty_remote_access",
    image=_FUNCTION_IMAGE_BINDINGS["run_social_job"],
    secrets=_secrets,
    retries=0,
    timeout=5 * 60,
)
def probe_getty_remote_access() -> dict[str, object]:
    from trr_backend.integrations.getty_local_prefetch import probe_getty_remote_access as _probe_getty_remote_access

    return _probe_getty_remote_access()


def _execute_social_job(job_id: str, *, worker_prefix: str) -> dict[str, object]:
    from trr_backend.socials.control_plane import claim_and_process_social_job

    worker_id = f"{worker_prefix}:{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
    try:
        result = claim_and_process_social_job(job_id=job_id, worker_id=worker_id)
        return {
            "job_id": job_id,
            "claimed": bool(result.get("claimed")),
            "worker_id": worker_id,
            "worker_family": "social",
            "job": result.get("job"),
        }
    finally:
        _close_db_pools_after_worker("social", job_id=job_id, worker_id=worker_id)


@app.function(
    name=str(os.getenv("TRR_MODAL_SOCIAL_POSTS_JOB_FUNCTION") or "run_social_posts_job").strip()
    or "run_social_posts_job",
    image=_FUNCTION_IMAGE_BINDINGS["run_social_posts_job"],
    secrets=_secrets,
    retries=0,
    timeout=2 * 60 * 60,
    max_containers=_SOCIAL_CONCURRENCY_LIMIT,
)
def run_social_posts_job(job_id: str) -> dict[str, object]:
    return _execute_social_job(job_id, worker_prefix="modal:social-posts")


@app.function(
    name=str(os.getenv("TRR_MODAL_SOCIAL_MEDIA_JOB_FUNCTION") or "run_social_media_job").strip()
    or "run_social_media_job",
    image=_FUNCTION_IMAGE_BINDINGS["run_social_media_job"],
    secrets=_secrets,
    retries=0,
    timeout=2 * 60 * 60,
    max_containers=_SOCIAL_MEDIA_CONCURRENCY_LIMIT,
)
def run_social_media_job(job_id: str) -> dict[str, object]:
    return _execute_social_job(job_id, worker_prefix="modal:social-media")


@app.function(
    name=str(os.getenv("TRR_MODAL_SOCIAL_COMMENTS_JOB_FUNCTION") or "run_social_comments_job").strip()
    or "run_social_comments_job",
    image=_FUNCTION_IMAGE_BINDINGS["run_social_comments_job"],
    secrets=_secrets,
    retries=0,
    timeout=2 * 60 * 60,
    max_containers=_SOCIAL_COMMENTS_CONCURRENCY_LIMIT,
)
def run_social_comments_job(job_id: str) -> dict[str, object]:
    return _execute_social_job(job_id, worker_prefix="modal:social-comments")


@app.function(
    name=str(os.getenv("TRR_MODAL_SOCIAL_COMMENTS_RECOVERY_JOB_FUNCTION") or "run_social_comments_recovery_job").strip()
    or "run_social_comments_recovery_job",
    image=_FUNCTION_IMAGE_BINDINGS["run_social_comments_recovery_job"],
    secrets=_secrets,
    retries=0,
    timeout=2 * 60 * 60,
    max_containers=_SOCIAL_COMMENTS_RECOVERY_CONCURRENCY_LIMIT,
)
def run_social_comments_recovery_job(job_id: str) -> dict[str, object]:
    return _execute_social_job(job_id, worker_prefix="modal:social-comments-recovery")


@app.function(
    image=_FUNCTION_IMAGE_BINDINGS["run_social_job"],
    secrets=_secrets,
    retries=0,
    timeout=2 * 60 * 60,
    max_containers=_SOCIAL_CONCURRENCY_LIMIT,
)
def run_social_job(job_id: str) -> dict[str, object]:
    return _execute_social_job(job_id, worker_prefix="modal:social")


def _recover_stale_pending_social_catalog_launches(
    *,
    limit: int = _SOCIAL_PENDING_LAUNCH_RECOVERY_LIMIT,
) -> dict[str, object]:
    """Finalize catalog launches stuck in launch_state=pending/finalizing with no jobs.

    Catalog launches reserve a durable run row first and finalize on a non-durable
    daemon thread; if the backend dies in between, the run stays queued forever unless
    a status read happens to trigger recovery. This sweep-side pass proactively invokes
    the same idempotent, advisory-locked recovery entrypoint for stale candidates.
    """
    import trr_backend.socials.social_season_analytics_impl as social_core

    safe_limit = max(1, min(int(limit), 50))
    grace_seconds = max(
        0,
        int(getattr(social_core, "_CATALOG_LAUNCH_FINALIZING_RECOVERY_GRACE_SECONDS", 120)),
    )
    rows = social_core.pg.fetch_all(
        """
        select
          r.id::text as run_id,
          coalesce(r.config, '{}'::jsonb) as config
        from social.scrape_runs r
        where coalesce(r.config->>'pipeline_ingest_mode', '') = %s
          and (
            lower(coalesce(r.config->>'launch_state', '')) in ('pending', 'finalizing')
            or lower(coalesce(r.config->>'launch_task_resolution_pending', 'false')) = 'true'
          )
          and r.created_at <= now() - make_interval(secs => %s)
          and not exists (select 1 from social.scrape_jobs j where j.run_id = r.id)
        order by r.created_at asc
        limit %s
        """,
        [social_core.SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE, grace_seconds, safe_limit],
    )
    recovered_run_ids: list[str] = []
    failed_run_ids: list[str] = []
    for row in rows:
        run_id = str(row.get("run_id") or "").strip()
        run_config = social_core._metadata_dict(row.get("config"))
        platforms = social_core._as_text_list(run_config.get("platforms") or [])
        accounts = social_core._as_text_list(run_config.get("accounts_override") or [])
        if not run_id or not platforms or not accounts:
            continue
        try:
            recovery = social_core.recover_pending_social_account_catalog_launch(
                platform=platforms[0],
                account_handle=accounts[0],
                run_id=run_id,
            )
        except Exception:  # noqa: BLE001 - one bad candidate must not block the rest
            failed_run_ids.append(run_id)
            logger.warning(
                "sweep_social_dispatch_queue: pending catalog launch recovery failed run_id=%s",
                run_id,
                exc_info=True,
            )
            continue
        if bool(recovery.get("recovered")):
            recovered_run_ids.append(run_id)
    return {
        "scanned": len(rows),
        "recovered": len(recovered_run_ids),
        "recovered_run_ids": recovered_run_ids,
        "failed_run_ids": failed_run_ids,
    }


@app.function(
    image=_image,
    secrets=_secrets,
    retries=0,
    timeout=15 * 60,
    max_containers=_SOCIAL_RECOVERY_CONCURRENCY_LIMIT,
    **_modal_cron_schedule_kwargs("*/2 * * * *"),
)
def sweep_social_dispatch_queue() -> dict[str, object]:
    _validate_modal_maintenance_owner_config()
    from trr_backend.socials.control_plane import recover_and_dispatch_due_social_jobs

    started_at = _worker_started(
        "social_recovery",
        function_name="sweep_social_dispatch_queue",
    )
    try:
        result = recover_and_dispatch_due_social_jobs()
        try:
            launch_recovery = _recover_stale_pending_social_catalog_launches()
        except Exception:  # noqa: BLE001 - recovery is best-effort; never fail the sweep
            logger.warning(
                "sweep_social_dispatch_queue: pending catalog launch recovery sweep failed",
                exc_info=True,
            )
            launch_recovery = {"status": "error"}
        result["pending_launch_recovery"] = launch_recovery
        _worker_finished(
            "social_recovery",
            started_at,
            result_status=str(result.get("status") or "completed"),
            recovered=result.get("recovered"),
            dispatched=result.get("dispatched"),
            pending_launches_recovered=launch_recovery.get("recovered"),
        )
        return result
    except Exception as exc:
        _worker_failed("social_recovery", started_at, failure_class=type(exc).__name__)
        raise
    finally:
        _close_db_pools_after_worker("social_recovery")


@app.function(
    image=_image,
    secrets=_secrets,
    retries=0,
    timeout=5 * 60,
    max_containers=1,
    # Daily at 13:30 in the configured TRR timezone (America/New_York by default).
    **_modal_cron_schedule_kwargs("30 13 * * *"),
)
def poll_decodo_proxy_usage() -> dict[str, object]:
    """Daily Decodo proxy usage poll + budget threshold alert.

    No-op (and reported as such) unless DECODO_API_* credentials are configured. When
    daily usage exceeds DECODO_DAILY_BUDGET_GB / DECODO_DAILY_BUDGET_USD it logs a
    WARNING (shipped to Better Stack) and, if available, a Sentry message. Fail-open:
    poll/parse errors are caught inside ``poll_decodo_usage`` and never crash the cron.
    """
    _validate_modal_maintenance_owner_config()
    from trr_backend.socials.decodo_usage import poll_decodo_usage

    started_at = _worker_started(
        "decodo_usage",
        function_name="poll_decodo_proxy_usage",
    )
    try:
        result = poll_decodo_usage()
        _worker_finished(
            "decodo_usage",
            started_at,
            result_status=str(result.get("status") or "completed"),
            alert=result.get("alert"),
            used_gb=result.get("used_gb"),
            used_usd=result.get("used_usd"),
        )
        return result
    except Exception as exc:
        # Defensive: poll_decodo_usage is already fail-open, but never let the cron crash.
        _worker_failed("decodo_usage", started_at, failure_class=type(exc).__name__)
        return {"status": "error", "reason": type(exc).__name__, "alert": False}


@app.function(
    image=_FUNCTION_IMAGE_BINDINGS["sync_nbcumv_official_images"],
    secrets=_secrets,
    retries=0,
    timeout=15 * 60,
    max_containers=1,
    # Daily at 14:15 in the configured TRR timezone.
    **_modal_cron_schedule_kwargs("15 14 * * *"),
)
def sync_nbcumv_official_images() -> dict[str, object]:
    _validate_modal_maintenance_owner_config()
    from api.routers.admin_show_sync import run_official_images_auto_sync

    started_at = _worker_started(
        "nbcumv_official_images",
        function_name="sync_nbcumv_official_images",
    )
    try:
        result = run_official_images_auto_sync()
        _worker_finished(
            "nbcumv_official_images",
            started_at,
            result_status=str(result.get("status") or "completed"),
            scanned=result.get("scanned"),
            changed=result.get("changed"),
            skipped=result.get("skipped"),
            failed=result.get("failed"),
        )
        return result
    except Exception as exc:
        _worker_failed("nbcumv_official_images", started_at, failure_class=type(exc).__name__)
        raise
    finally:
        _close_db_pools_after_worker("nbcumv_official_images")


@app.function(
    image=_FUNCTION_IMAGE_BINDINGS["heartbeat_remote_executors"],
    secrets=_secrets,
    retries=0,
    timeout=10 * 60,
    max_containers=1,
    **_modal_cron_schedule_kwargs("* * * * *"),
)
def heartbeat_remote_executors(heartbeat_source: str = "backend_runtime_scheduler") -> dict[str, object]:
    _validate_modal_maintenance_owner_config()
    try:
        _poll_due_show_season_media_watches_impl()
    except Exception:
        # Keep the pre-existing executor heartbeat independent while making the
        # media poll visible in its own structured worker-failure logs.
        logger.exception("[executor-heartbeat] show-season media poll failed")
    from trr_backend.modal_dispatch import _record_dispatcher_heartbeat
    from trr_backend.socials.control_plane import get_worker_auth_capabilities, is_queue_enabled

    started_at = _worker_started(
        "executor_heartbeat",
        function_name="heartbeat_remote_executors",
    )
    try:
        metadata = {
            "dispatch_enabled": True,
            "heartbeat_source": str(heartbeat_source or "backend_runtime_scheduler"),
            "heartbeat_call_id": f"heartbeat:{uuid.uuid4().hex[:8]}",
        }
        _record_dispatcher_heartbeat(
            dispatcher_name="admin",
            status="idle",
            metadata_updates={
                **metadata,
                "modal_capacity": _modal_capacity_metadata(
                    worker_family="admin_operations",
                    function_name="run_admin_operation_v2",
                    image_family="lean",
                    timeout_seconds=_ADMIN_OPERATION_TIMEOUT_SECONDS,
                    min_containers=_ADMIN_KEEP_WARM,
                    max_containers=_ADMIN_CONCURRENCY_LIMIT,
                ),
            },
        )
        _record_dispatcher_heartbeat(
            dispatcher_name="google-news",
            status="idle",
            metadata_updates={
                **metadata,
                "modal_capacity": _modal_capacity_metadata(
                    worker_family="google_news",
                    function_name="run_google_news_sync",
                    image_family="lean",
                    timeout_seconds=_GOOGLE_NEWS_TIMEOUT_SECONDS,
                    max_containers=_GOOGLE_NEWS_CONCURRENCY_LIMIT,
                ),
            },
        )
        _record_dispatcher_heartbeat(
            dispatcher_name="reddit",
            status="idle",
            metadata_updates={
                **metadata,
                "modal_capacity": _modal_capacity_metadata(
                    worker_family="reddit_refresh",
                    function_name="run_reddit_refresh",
                    image_family="lean",
                    timeout_seconds=_REDDIT_REFRESH_TIMEOUT_SECONDS,
                    max_containers=_REDDIT_REFRESH_CONCURRENCY_LIMIT,
                ),
            },
        )
        if is_queue_enabled():
            social_metadata = {
                **metadata,
                "auth_capabilities": get_worker_auth_capabilities(),
                "modal_capacity": _modal_capacity_metadata(
                    worker_family="social",
                    function_name="run_social_job",
                    image_family="browser",
                    timeout_seconds=2 * 60 * 60,
                    max_containers=_SOCIAL_CONCURRENCY_LIMIT,
                ),
                "modal_capacity_by_function": [
                    _modal_capacity_metadata(
                        worker_family="social_posts",
                        function_name="run_social_posts_job",
                        image_family="browser",
                        timeout_seconds=2 * 60 * 60,
                        max_containers=_SOCIAL_CONCURRENCY_LIMIT,
                    ),
                    _modal_capacity_metadata(
                        worker_family="social_media",
                        function_name="run_social_media_job",
                        image_family="browser",
                        timeout_seconds=2 * 60 * 60,
                        max_containers=_SOCIAL_MEDIA_CONCURRENCY_LIMIT,
                    ),
                    _modal_capacity_metadata(
                        worker_family="social_comments",
                        function_name="run_social_comments_job",
                        image_family="browser",
                        timeout_seconds=2 * 60 * 60,
                        max_containers=_SOCIAL_COMMENTS_CONCURRENCY_LIMIT,
                    ),
                    _modal_capacity_metadata(
                        worker_family="social_comments_recovery",
                        function_name="run_social_comments_recovery_job",
                        image_family="browser",
                        timeout_seconds=2 * 60 * 60,
                        max_containers=_SOCIAL_COMMENTS_RECOVERY_CONCURRENCY_LIMIT,
                    ),
                    _modal_capacity_metadata(
                        worker_family="socialblade",
                        function_name="run_socialblade_scrape",
                        image_family="browser",
                        timeout_seconds=_SOCIALBLADE_TIMEOUT_SECONDS,
                        max_containers=_SOCIALBLADE_CONCURRENCY_LIMIT,
                    ),
                    _modal_capacity_metadata(
                        worker_family="admin_vision",
                        function_name="run_admin_vision",
                        image_family="vision",
                        timeout_seconds=_VISION_TIMEOUT_SECONDS,
                        max_containers=_VISION_CONCURRENCY_LIMIT,
                    ),
                ],
            }
            _record_dispatcher_heartbeat(
                dispatcher_name="social",
                status="idle",
                metadata_updates=social_metadata,
                supported_platforms=list(SOCIAL_SUPPORTED_PLATFORMS),
            )
        else:
            social_metadata = None
        _worker_finished(
            "executor_heartbeat",
            started_at,
            result_status="completed",
            social_queue_enabled=bool(social_metadata),
        )
        return {
            "ok": True,
            "social_auth_capabilities": social_metadata.get("auth_capabilities") if social_metadata else None,
        }
    except Exception as exc:
        _worker_failed("executor_heartbeat", started_at, failure_class=type(exc).__name__)
        raise
    finally:
        _close_db_pools_after_worker("executor_heartbeat")


@app.function(
    image=_FUNCTION_IMAGE_BINDINGS["purge_stale_social_worker_heartbeats"],
    secrets=_secrets,
    retries=0,
    timeout=_STALE_WORKER_CLEANUP_TIMEOUT_SECONDS,
    max_containers=_STALE_WORKER_CLEANUP_CONCURRENCY_LIMIT,
    **_modal_cron_schedule_kwargs("17 4 * * *"),
)
def purge_stale_social_worker_heartbeats() -> dict[str, object]:
    _validate_modal_maintenance_owner_config()
    from collections import Counter

    from trr_backend.db import pg

    started_at = _worker_started(
        "social_worker_heartbeat_cleanup",
        function_name="purge_stale_social_worker_heartbeats",
        stale_after_seconds=_STALE_WORKER_CLEANUP_AFTER_SECONDS,
    )
    try:
        active_clause = (
            "status in ('starting', 'idle', 'working') and last_seen_at >= now() - (%s * interval '1 second')"
        )
        snapshot_row = (
            pg.fetch_one(
                f"""
                select
                  count(*) filter (where {active_clause})::int as active_workers,
                  count(*)::int as total_workers
                from social.scrape_workers
                """,
                [_STALE_WORKER_CLEANUP_AFTER_SECONDS],
            )
            or {}
        )
        deleted_rows = pg.execute_returning(
            """
            delete from social.scrape_workers
            where last_seen_at < now() - (%s * interval '1 second')
            returning worker_id, status
            """,
            [_STALE_WORKER_CLEANUP_AFTER_SECONDS],
        )
        deleted_by_status = Counter(str(row.get("status") or "unknown") for row in deleted_rows or [])
        total_workers_before = int(snapshot_row.get("total_workers") or 0)
        deleted_workers = len(deleted_rows or [])
        result = {
            "stale_after_seconds": _STALE_WORKER_CLEANUP_AFTER_SECONDS,
            "active_workers": int(snapshot_row.get("active_workers") or 0),
            "total_workers_before": total_workers_before,
            "total_workers_after": max(0, total_workers_before - deleted_workers),
            "deleted_workers": deleted_workers,
            "deleted_by_status": dict(sorted(deleted_by_status.items())),
            "reason": None,
        }
        _worker_finished(
            "social_worker_heartbeat_cleanup",
            started_at,
            result_status="completed",
            deleted_workers=result.get("deleted_workers"),
            total_workers_before=result.get("total_workers_before"),
            total_workers_after=result.get("total_workers_after"),
        )
        return {
            **result,
            "worker_family": "social_worker_heartbeat_cleanup",
            "cleanup_policy": "delete_rows_older_than_threshold",
        }
    except Exception as exc:
        _worker_failed(
            "social_worker_heartbeat_cleanup",
            started_at,
            failure_class=type(exc).__name__,
            stale_after_seconds=_STALE_WORKER_CLEANUP_AFTER_SECONDS,
        )
        raise
    finally:
        _close_db_pools_after_worker(
            "social_worker_heartbeat_cleanup",
            stale_after_seconds=_STALE_WORKER_CLEANUP_AFTER_SECONDS,
        )


@app.function(
    image=_FUNCTION_IMAGE_BINDINGS["run_admin_vision"],
    secrets=_secrets,
    retries=0,
    timeout=_VISION_TIMEOUT_SECONDS,
    max_containers=_VISION_CONCURRENCY_LIMIT,
)
def run_admin_vision(payload: dict[str, object], batch: bool = False) -> dict[str, object]:
    from trr_backend.vision.people_count_engine import (
        VisionEngineError,
        VisionEngineUnavailableError,
        compute_people_count,
        compute_people_count_batch,
    )

    started_at = _worker_started(
        "admin_vision",
        function_name="run_admin_vision",
        batch=batch,
        target_count=len(payload) if batch and isinstance(payload, list) else 1,
    )
    try:
        result = compute_people_count_batch(payload) if batch else compute_people_count(payload)
        _worker_finished("admin_vision", started_at, result_status="completed", batch=batch)
        return result
    except VisionEngineUnavailableError as exc:
        _worker_finished("admin_vision", started_at, result_status="unavailable", batch=batch)
        return {
            "error": str(exc),
            "retry_after_s": int(exc.retry_after_s),
            "unavailable": True,
        }
    except VisionEngineError as exc:
        _worker_finished(
            "admin_vision",
            started_at,
            result_status="failed",
            batch=batch,
            failure_class=type(exc).__name__,
        )
        return {
            "error": str(exc),
            "unavailable": False,
        }
    finally:
        _close_db_pools_after_worker("admin_vision", batch=batch)


@app.function(
    image=_FUNCTION_IMAGE_BINDINGS["run_cast_screentime_analysis"],
    secrets=_secrets,
    retries=0,
    timeout=_CAST_SCREENTIME_TIMEOUT_SECONDS,
    max_containers=_CAST_SCREENTIME_CONCURRENCY_LIMIT,
)
def run_cast_screentime_analysis(run_id: str) -> dict[str, object]:
    from trr_backend.services import retained_cast_screentime_runtime

    normalized_run_id = str(run_id or "").strip()
    started_at = _worker_started(
        "cast_screentime",
        function_name="run_cast_screentime_analysis",
        run_id=normalized_run_id,
    )
    try:
        result = retained_cast_screentime_runtime.run_screentime_analysis(normalized_run_id)
        _worker_finished(
            "cast_screentime",
            started_at,
            result_status=str(result.get("status") or "completed"),
            run_id=normalized_run_id,
        )
        return result
    except Exception as exc:
        _worker_finished(
            "cast_screentime",
            started_at,
            result_status="failed",
            failure_class=type(exc).__name__,
            run_id=normalized_run_id,
        )
        raise
    finally:
        _close_db_pools_after_worker("cast_screentime", run_id=normalized_run_id)


@app.function(
    image=_FUNCTION_IMAGE_BINDINGS["run_cast_screentime_subtitle_extraction"],
    secrets=_secrets,
    # The service owns bounded failure persistence. A platform retry cannot
    # safely reclaim a row already marked running after a container crash;
    # stale work is reconciled back to failed for explicit operator retry.
    retries=0,
    timeout=_CAST_SCREENTIME_SUBTITLE_TIMEOUT_SECONDS,
    max_containers=_CAST_SCREENTIME_SUBTITLE_CONCURRENCY_LIMIT,
)
def run_cast_screentime_subtitle_extraction(
    video_asset_id: str,
    force: bool = False,
) -> dict[str, object]:
    from trr_backend.services import cast_screentime_subtitles

    normalized_video_asset_id = str(video_asset_id or "").strip()
    started_at = _worker_started(
        "cast_screentime_subtitles",
        function_name="run_cast_screentime_subtitle_extraction",
        video_asset_id=normalized_video_asset_id,
        force=bool(force),
    )
    try:
        result = cast_screentime_subtitles.extract_video_asset_subtitles(
            normalized_video_asset_id,
            force=bool(force),
        )
        _worker_finished(
            "cast_screentime_subtitles",
            started_at,
            result_status=str(result.get("status") or "completed"),
            video_asset_id=normalized_video_asset_id,
            force=bool(force),
        )
        return result
    except Exception as exc:
        _worker_finished(
            "cast_screentime_subtitles",
            started_at,
            result_status="failed",
            failure_class=type(exc).__name__,
            video_asset_id=normalized_video_asset_id,
            force=bool(force),
        )
        raise
    finally:
        _close_db_pools_after_worker(
            "cast_screentime_subtitles",
            video_asset_id=normalized_video_asset_id,
            force=bool(force),
        )


@app.function(
    image=_FUNCTION_IMAGE_BINDINGS["run_socialblade_scrape"],
    secrets=_secrets,
    retries=0,
    timeout=_SOCIALBLADE_TIMEOUT_SECONDS,
    max_containers=_SOCIALBLADE_CONCURRENCY_LIMIT,
)
def run_socialblade_scrape(
    handle: str,
    person_id: str | None = None,
    source: str = "person_page",
    force: bool = False,
    platform: str = "instagram",
    scrape_following: bool = True,
    source_scope: str = "network",
) -> dict[str, object]:
    from trr_backend.socials.socialblade.auth import load_socialblade_cookies_from_sources
    from trr_backend.socials.socialblade.scraper import scrape_socialblade
    from trr_backend.socials.socialblade.service import (
        normalize_socialblade_source_scope,
        refresh_and_persist_socialblade,
        sanitize_socialblade_platform,
        scrape_socialblade_then_following,
    )

    started_at = _worker_started(
        "socialblade",
        function_name="run_socialblade_scrape",
        handle=handle,
        person_id=person_id,
        source=source,
        platform=platform,
        scrape_following=scrape_following,
        source_scope=source_scope,
    )
    try:
        normalized_platform = sanitize_socialblade_platform(platform)
        normalized_source_scope = normalize_socialblade_source_scope(source_scope)
        cookies = load_socialblade_cookies_from_sources()

        def _scrape_primary(normalized_handle: str) -> dict[str, object]:
            return scrape_socialblade(
                normalized_handle,
                cookies,
                platform=normalized_platform,
                allow_login_fallback=False,
                allow_visible_browser_retry=False,
            )

        def _scrape_with_following_sidecar(safe_handle: str) -> dict[str, object]:
            return scrape_socialblade_then_following(
                _scrape_primary,
                handle=safe_handle,
                platform=normalized_platform,
                source=source,
                source_scope=normalized_source_scope,
                enabled=scrape_following,
            )

        result = refresh_and_persist_socialblade(
            person_id=person_id,
            handle=handle,
            scraper=_scrape_with_following_sidecar,
            source=source,
            force=force,
            platform=normalized_platform,
        )
        status = (
            str(result.get("status") or result.get("refresh_status") or "completed")
            if isinstance(result, dict)
            else "completed"
        )
        _worker_finished(
            "socialblade",
            started_at,
            result_status=status,
            handle=handle,
            person_id=person_id,
            platform=normalized_platform,
        )
        return result
    except Exception as exc:
        _worker_failed(
            "socialblade",
            started_at,
            failure_class=type(exc).__name__,
            handle=handle,
            person_id=person_id,
            platform=platform,
        )
        raise
    finally:
        _close_db_pools_after_worker("socialblade", handle=handle, person_id=person_id, platform=platform)
