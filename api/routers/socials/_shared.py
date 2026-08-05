# ruff: noqa: F401, I001
"""
API endpoints for social media scraping and analytics.

Provides endpoints to:
1. Scrape Instagram posts from profiles with filtering
2. Scrape TikTok posts from profiles with filtering
3. Search Twitter/X for tweets by hashtag/phrase
4. Scrape YouTube channel videos by keywords
5. Configure social accounts for shows/seasons/cast members
6. Retrieve cached social media data
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Lock, Thread
from time import monotonic as monotonic
from time import perf_counter
from typing import Any, Literal
from uuid import UUID

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel, Field, model_validator
from starlette.concurrency import run_in_threadpool

from api.auth import InternalAdminUser
from trr_backend.db.pg import database_service_unavailable_detail, is_database_service_unavailable_error
from trr_backend.job_plane import (
    canonical_execution_mode,
    execution_backend_canonical,
    execution_metadata,
    execution_owner_label,
    is_modal_remote_executor_enabled,
    is_remote_job_plane_enabled,
)
from trr_backend.modal_dispatch import (
    dispatch_reddit_refresh,
    get_modal_reddit_runtime_health,
    modal_app_name,
    modal_dispatch_ready,
    modal_environment_name,
    modal_execution_metadata,
    modal_reddit_refresh_function_name,
    modal_social_job_function_name,
    resolve_modal_function,
)
from trr_backend.observability import get_trace_id
from trr_backend.read_path_diagnostics import log_read_path
from trr_backend.repositories.twitter_standalone import persist_standalone_twitter_search
from trr_backend.socials.api.handlers import live_status as social_live_status
from trr_backend.socials.api.handlers import profile_reads as social_profile_reads
from trr_backend.socials.inline_ingest import (
    normalize_target_platforms as _normalize_inline_target_platforms,
)
from trr_backend.socials.inline_ingest import (
    run_inline_season_ingest_execution,
)
from trr_backend.socials.platforms import SOCIAL_SUPPORTED_PLATFORMS

from ._analytics_cache import (
    _ANALYTICS_CACHE,
    _ANALYTICS_CACHE_LOCK,
    _ANALYTICS_CACHE_MAX_ENTRIES,
    _ANALYTICS_CACHE_TTL_SECONDS,
    _COMMENTS_COVERAGE_CACHE,
    _COMMENTS_COVERAGE_CACHE_LOCK,
    _COVERAGE_CACHE_MAX_ENTRIES,
    _COVERAGE_CACHE_TTL_SECONDS,
    _MIRROR_COVERAGE_CACHE,
    _MIRROR_COVERAGE_CACHE_LOCK,
    _WEEK_LIVE_HEALTH_CACHE,
    _WEEK_LIVE_HEALTH_CACHE_LOCK,
    _WEEK_LIVE_HEALTH_CACHE_MAX_ENTRIES,
    _WEEK_LIVE_HEALTH_CACHE_TTL_SECONDS,
    WeekDetailSortDir,
    WeekDetailSortField,
    WeekSummaryInclude,
    _analytics_cache_key,
    _coverage_cache_window_key,
    _get_ttl_cached_payload,
    _get_week_detail_cached_payload,
    _get_week_summary_cached_payload,
    _set_ttl_cached_payload,
    _set_week_detail_cached_payload,
    _set_week_summary_cached_payload,
    _week_detail_cache_key,
    _week_live_health_cache_key,
    _week_summary_cache_key,
    invalidate_week_detail_cache,
)
from ._analytics_cache import (
    _clear_ttl_cache as _clear_ttl_cache,
)
from ._analytics_cache import (
    invalidate_week_summary_cache as invalidate_week_summary_cache,
)
from ._profile_cache import (
    _ACCOUNT_PROFILE_CACHE_MAX_ENTRIES,
    _ACCOUNT_PROFILE_CACHE_TTL_SECONDS,
    _ACCOUNT_PROFILE_COLLABORATORS_CACHE,
    _ACCOUNT_PROFILE_COLLABORATORS_CACHE_LOCK,
    _ACCOUNT_PROFILE_DASHBOARD_CACHE,
    _ACCOUNT_PROFILE_DASHBOARD_CACHE_LOCK,
    _ACCOUNT_PROFILE_HASHTAG_TIMELINE_CACHE,
    _ACCOUNT_PROFILE_HASHTAG_TIMELINE_CACHE_LOCK,
    _ACCOUNT_PROFILE_HASHTAGS_CACHE,
    _ACCOUNT_PROFILE_HASHTAGS_CACHE_LOCK,
    _ACCOUNT_PROFILE_POSTS_CACHE,
    _ACCOUNT_PROFILE_POSTS_CACHE_LOCK,
    _ACCOUNT_PROFILE_PROGRESS_CACHE_TTL_SECONDS,
    _ACCOUNT_PROFILE_SUMMARY_CACHE,
    _ACCOUNT_PROFILE_SUMMARY_CACHE_LOCK,
    _account_profile_cache_key,
    _clear_account_profile_caches,
    _resolve_account_profile_catalog_freshness,
    _resolve_account_profile_catalog_run_progress,
    _resolve_account_profile_singleflight,
)
from ._profile_cache import (
    _ACCOUNT_PROFILE_CATALOG_FRESHNESS_CACHE as _ACCOUNT_PROFILE_CATALOG_FRESHNESS_CACHE,
)
from ._profile_cache import (
    _ACCOUNT_PROFILE_CATALOG_FRESHNESS_CACHE_LOCK as _ACCOUNT_PROFILE_CATALOG_FRESHNESS_CACHE_LOCK,
)
from .analytics_read import (
    analytics_read_path_extra,
    page_week_detail_payload,
    parse_analytics_include,
    week_detail_cached_post_counts,
)



logger = logging.getLogger("api.routers.socials")


def _internal_error_response(_exc: Exception, *, status_code: int = 500) -> HTTPException:
    """Return a non-reflective client error after the caller logs context.

    Never surface raw internal exception text (DB/driver messages, hostnames,
    proxy/upstream detail) to clients. Callers do `raise _internal_error_response(exc) from exc`.
    """
    message = "Upstream request failed." if status_code == 502 else "Internal server error."
    return HTTPException(status_code=status_code, detail={"code": "INTERNAL_ERROR", "message": message})




SourceScopeParam = Literal["bravo", "network", "creator", "community", "news"]

INSTAGRAM_AUTH_REFRESH_CONFIRMATION = "I UNDERSTAND INSTAGRAM AUTH RISK"
INSTAGRAM_AUTH_REFRESH_WARNING = (
    "Manual Instagram auth can surface CAPTCHA, verification code, checkpoint, or account-lock prompts. "
    "Complete those steps yourself before confirming a validated-cookie sync."
)






def normalize_source_scope_param(value: str | None, *, default: str = "network") -> str:
    normalized = str(value or default).strip().lower() or default
    if normalized == "bravo":
        return "network"
    if normalized in {"network", "creator", "community", "news"}:
        return normalized
    raise ValueError(f"Unsupported source scope: {value}")


def preserve_source_scope_param(value: str | None, *, default: str = "network") -> str:
    normalized = str(value or default).strip().lower() or default
    if normalized in {"bravo", "network", "creator", "community", "news"}:
        return normalized
    raise ValueError(f"Unsupported source scope: {value}")

class SourceScopedRequest(BaseModel):
    source_scope: SourceScopeParam = Field(default="network")

    @model_validator(mode="after")
    def normalize_source_scope(self) -> SourceScopedRequest:
        self.source_scope = normalize_source_scope_param(self.source_scope)  # type: ignore[assignment]
        return self

def _raise_if_modal_social_dispatch_unresolvable(platform: str | None = None) -> None:
    resolution = resolve_modal_function(modal_social_job_function_name())
    if bool(resolution.get("resolved")):
        return
    platform_label = str(platform or "social").strip().lower() or "social"
    raise HTTPException(
        status_code=503,
        detail={
            "code": "SOCIAL_MODAL_DISPATCH_UNAVAILABLE",
            "message": (
                f"{platform_label.capitalize()} shared-account catalog dispatch is configured for Modal, "
                "but the configured Modal target could not be resolved."
            ),
            "reason": resolution.get("reason") or "modal_resolution_failed",
            "resolution_error": resolution.get("error"),
            "configured_app_name": resolution.get("app_name") or modal_app_name(),
            "configured_function_name": resolution.get("function_name") or modal_social_job_function_name(),
            "modal_environment": resolution.get("modal_environment") or modal_environment_name(),
            "required_execution_backend": "modal",
            "execution_mode": canonical_execution_mode(),
            "execution_owner": execution_owner_label(),
        },
    )

def _env_truthy(name: str) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}








# Default timeout (seconds) for inline dev-fallback execution.
# Configurable via SOCIAL_INLINE_EXECUTION_TIMEOUT_SECONDS env var.

async def _run_admin_repo_call(func: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Any:
    """Keep async admin routes from blocking the event loop on sync repository work."""
    return await run_in_threadpool(func, *args, **kwargs)





SocialExecutionMode = Literal["queued", "inline"]
SocialExecutionModeCanonical = Literal["queued", "inline", "inline_fallback"]
SocialExecutionModeLegacy = Literal["queue", "inline", "inline_fallback"]


def _resolve_social_execution_modes(
    *,
    queue_enabled: bool,
    used_inline_fallback: bool = False,
) -> tuple[SocialExecutionMode, SocialExecutionModeCanonical, SocialExecutionModeLegacy]:
    execution_mode: SocialExecutionMode = "queued" if queue_enabled else "inline"
    execution_mode_canonical: SocialExecutionModeCanonical = (
        "inline_fallback" if used_inline_fallback else execution_mode
    )
    execution_mode_legacy: SocialExecutionModeLegacy = (
        "queue"
        if execution_mode_canonical == "queued"
        else ("inline_fallback" if execution_mode_canonical == "inline_fallback" else "inline")
    )
    return execution_mode, execution_mode_canonical, execution_mode_legacy

def _social_execution_mode_deprecation_payload() -> dict[str, Any]:
    return {
        "field": "execution_mode_legacy",
        "message": "execution_mode_legacy is deprecated; use execution_mode_canonical.",
    }

def _start_runs_in_background(
    run_ids: list[str],
    background_tasks: BackgroundTasks,
    *,
    worker_prefix: str,
    stage: str | None = None,
    platform: str | None = None,
    supported_platforms: list[str] | None = None,
    metadata_updates: Mapping[str, Any] | None = None,
) -> None:
    from trr_backend.socials.control_plane import execute_run_with_inline_worker_registration

    def _runner() -> None:
        for index, run_id in enumerate(run_ids, start=1):
            worker_id = worker_prefix if len(run_ids) == 1 else f"{worker_prefix}:{index}"
            execute_run_with_inline_worker_registration(
                run_id,
                worker_id=worker_id,
                stage=stage,
                platform=platform,
                supported_platforms=supported_platforms,
                metadata_updates=metadata_updates,
            )

    if not run_ids:
        return
    background_tasks.add_task(_runner)

def _normalize_target_platforms(platforms: list[str] | None) -> list[str]:
    return _normalize_inline_target_platforms(platforms, supported_platforms=SOCIAL_SUPPORTED_PLATFORMS)

def _parse_platform_query(platforms: str | None) -> list[str] | None:
    if not platforms or not platforms.strip():
        return None

    requested = [item.strip().lower() for item in platforms.split(",") if item.strip()]
    if not requested:
        raise HTTPException(
            status_code=400,
            detail={"code": "INVALID_PLATFORM_FILTER", "message": "No valid platforms were provided"},
        )

    unsupported = sorted({item for item in requested if item not in SOCIAL_SUPPORTED_PLATFORMS})
    if unsupported:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "INVALID_PLATFORM_FILTER",
                "message": f"Unsupported platforms: {', '.join(unsupported)}",
            },
        )

    deduped: list[str] = []
    for platform in requested:
        if platform in deduped:
            continue
        deduped.append(platform)
    return deduped

def _value_error_to_bad_request(exc: ValueError) -> HTTPException:
    message = str(exc)
    if message.upper().startswith("INVALID_PLATFORM_FILTER"):
        return HTTPException(
            status_code=400,
            detail={"code": "INVALID_PLATFORM_FILTER", "message": message.split(":", 1)[-1].strip() or message},
        )
    return HTTPException(status_code=400, detail=message)

def _lookup_error_to_not_found(exc: LookupError) -> HTTPException:
    return HTTPException(status_code=404, detail=str(exc) or "Not found")


def _to_social_read_http_exception(error: Exception) -> HTTPException:
    if isinstance(error, ValueError):
        return _value_error_to_bad_request(error)
    if is_database_service_unavailable_error(error):
        return HTTPException(status_code=503, detail=database_service_unavailable_detail(error))
    return HTTPException(status_code=500, detail=str(error) or "Internal server error")


def _worker_health_detail(worker_health: Any) -> Any:
    return jsonable_encoder(worker_health) if worker_health is not None else None

def _remote_worker_unavailable_message(
    exc: Exception,
    *,
    default_message: str = (
        "Social ingest remote-worker ownership is enforced and no healthy worker is currently reporting heartbeats."
    ),
) -> str:
    exc_message = str(exc or "").strip()
    if not exc_message:
        return default_message
    return f"Social ingest remote-worker ownership is enforced. {exc_message.removesuffix('.')}."


def _is_local_or_dev_runtime() -> bool:
    runtime_markers = [
        os.getenv("APP_ENV"),
        os.getenv("ENV"),
        os.getenv("ENVIRONMENT"),
        os.getenv("TRR_ENV"),
        os.getenv("TRR_ENVIRONMENT"),
    ]
    normalized = {str(value or "").strip().lower() for value in runtime_markers if str(value or "").strip()}
    if normalized & {"local", "dev", "development", "test"}:
        return True

    if _env_truthy("TRR_LOCAL_DEV") or _env_truthy("SOCIAL_ALLOW_INLINE_DEV_FALLBACK"):
        return True

    if canonical_execution_mode() == "local" and not is_remote_job_plane_enabled():
        return True

    return False

def _can_use_local_catalog_inline_fallback(
    *,
    allow_inline_dev_fallback: bool,
    remote_plane_enforced: bool,
    requires_modal_executor: bool = False,
    explicit_local_preference: bool = False,
) -> bool:
    if not _is_local_or_dev_runtime():
        return False
    if requires_modal_executor and not explicit_local_preference:
        return False
    if _env_truthy("TRR_ALLOW_LOCAL_ADMIN_OPERATION_OVERRIDE"):
        return True
    return bool(allow_inline_dev_fallback) and not remote_plane_enforced

# Request/Response Models

# Endpoints

# TikTok Models

# TikTok Endpoints

# Twitter/X Models

# Twitter/X Endpoints

# YouTube Models

# YouTube Endpoints

# Facebook Models / Endpoints

# Threads Models / Endpoints

# ---------------------------------------------------------------------------
# Season social analytics (Bravo-first)
# ---------------------------------------------------------------------------

class CatalogRepairAuthRequest(BaseModel):
    allow_inline_dev_fallback: bool = Field(default=False)
    operator_confirmation: str | None = Field(default=None)
    allow_cookie_refresh: bool = Field(default=False)

InstagramCommentsLoadStrategy = Literal[
    "instagram_comments_endpoint_cursor",
    "cursor_api",
    "single_session_load_all",
    "public_relay",
]

def _require_instagram_auth_refresh_confirmation(platform: str, confirmation: str | None) -> None:
    normalized = str(platform or "").strip().lower()
    if normalized != "instagram":
        return
    if str(confirmation or "").strip() == INSTAGRAM_AUTH_REFRESH_CONFIRMATION:
        return
    raise HTTPException(
        status_code=400,
        detail={
            "code": "INSTAGRAM_AUTH_REFRESH_CONFIRMATION_REQUIRED",
            "message": INSTAGRAM_AUTH_REFRESH_WARNING,
            "required_confirmation": INSTAGRAM_AUTH_REFRESH_CONFIRMATION,
        },
    )

# ---------------------------------------------------------------------------
# Reddit flair auto-categorization
# ---------------------------------------------------------------------------

class AutoCategorizeFlairsResponse(BaseModel):
    categories: dict[str, str]
    matched: int
    total: int

# Intentionally exports private helpers for the decomposed route modules.
__all__ = [name for name in globals() if not name.startswith("__")]
