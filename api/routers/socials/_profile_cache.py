"""Route-local cache helpers for admin social account profile endpoints."""

from __future__ import annotations

import copy
import os
from collections.abc import Callable
from concurrent.futures import Future
from datetime import UTC, datetime
from threading import Lock
from typing import Any

from trr_backend.db.pg import database_service_unavailable_detail, is_database_service_unavailable_error

from ._analytics_cache import (
    _clear_ttl_cache,
    _get_ttl_cached_payload,
    _get_ttl_stale_payload,
    _set_ttl_cached_payload,
)

_ACCOUNT_PROFILE_CACHE_TTL_SECONDS = int(os.getenv("SOCIAL_ACCOUNT_PROFILE_CACHE_TTL_SECONDS", "600"))
_ACCOUNT_PROFILE_CACHE_MAX_ENTRIES = int(os.getenv("SOCIAL_ACCOUNT_PROFILE_CACHE_MAX_ENTRIES", "256"))
_ACCOUNT_PROFILE_PROGRESS_CACHE_TTL_SECONDS = int(
    os.getenv("SOCIAL_ACCOUNT_PROFILE_RUN_PROGRESS_CACHE_TTL_SECONDS", "3")
)
_ACCOUNT_PROFILE_SUMMARY_CACHE: dict[Any, tuple[float, dict[str, Any]]] = {}
_ACCOUNT_PROFILE_SUMMARY_CACHE_LOCK = Lock()
_ACCOUNT_PROFILE_DASHBOARD_CACHE: dict[Any, tuple[float, dict[str, Any]]] = {}
_ACCOUNT_PROFILE_DASHBOARD_CACHE_LOCK = Lock()
_ACCOUNT_PROFILE_PROGRESS_CACHE: dict[Any, tuple[float, dict[str, Any]]] = {}
_ACCOUNT_PROFILE_PROGRESS_CACHE_LOCK = Lock()
_ACCOUNT_PROFILE_POSTS_CACHE: dict[Any, tuple[float, dict[str, Any]]] = {}
_ACCOUNT_PROFILE_POSTS_CACHE_LOCK = Lock()
_ACCOUNT_PROFILE_HASHTAGS_CACHE: dict[Any, tuple[float, dict[str, Any]]] = {}
_ACCOUNT_PROFILE_HASHTAGS_CACHE_LOCK = Lock()
_ACCOUNT_PROFILE_HASHTAG_TIMELINE_CACHE: dict[Any, tuple[float, dict[str, Any]]] = {}
_ACCOUNT_PROFILE_HASHTAG_TIMELINE_CACHE_LOCK = Lock()
_ACCOUNT_PROFILE_COLLABORATORS_CACHE: dict[Any, tuple[float, dict[str, Any]]] = {}
_ACCOUNT_PROFILE_COLLABORATORS_CACHE_LOCK = Lock()
_ACCOUNT_PROFILE_CATALOG_FRESHNESS_CACHE: dict[Any, tuple[float, dict[str, Any]]] = {}
_ACCOUNT_PROFILE_CATALOG_FRESHNESS_CACHE_LOCK = Lock()
_ACCOUNT_PROFILE_SINGLEFLIGHT: dict[tuple[Any, ...], Future[dict[str, Any]]] = {}
_ACCOUNT_PROFILE_SINGLEFLIGHT_LOCK = Lock()


def _account_profile_cache_key(
    *,
    surface: str,
    platform: str,
    account_handle: str,
    page: int | None = None,
    page_size: int | None = None,
    search: str | None = None,
    window: str | None = None,
    comments_only: bool | None = None,
    comment_filter: str | None = None,
    sort_by: str | None = None,
    sort_dir: str | None = None,
    post_source_id: str | None = None,
    extra: tuple[Any, ...] | None = None,
) -> tuple[Any, ...]:
    return (
        surface,
        str(platform or "").strip().lower(),
        str(account_handle or "").strip().lower().lstrip("@"),
        page,
        page_size,
        str(search or "").strip().lower() or None,
        str(window or "").strip().lower() or None,
        None if comments_only is None else bool(comments_only),
        str(comment_filter or "").strip().lower() or None,
        str(sort_by or "").strip().lower() or None,
        str(sort_dir or "").strip().lower() or None,
        str(post_source_id or "").strip() or None,
        *(extra or ()),
    )


def _clear_account_profile_caches() -> None:
    _clear_ttl_cache(_ACCOUNT_PROFILE_SUMMARY_CACHE, _ACCOUNT_PROFILE_SUMMARY_CACHE_LOCK)
    _clear_ttl_cache(_ACCOUNT_PROFILE_DASHBOARD_CACHE, _ACCOUNT_PROFILE_DASHBOARD_CACHE_LOCK)
    _clear_ttl_cache(_ACCOUNT_PROFILE_PROGRESS_CACHE, _ACCOUNT_PROFILE_PROGRESS_CACHE_LOCK)
    _clear_ttl_cache(_ACCOUNT_PROFILE_POSTS_CACHE, _ACCOUNT_PROFILE_POSTS_CACHE_LOCK)
    _clear_ttl_cache(_ACCOUNT_PROFILE_HASHTAGS_CACHE, _ACCOUNT_PROFILE_HASHTAGS_CACHE_LOCK)
    _clear_ttl_cache(_ACCOUNT_PROFILE_HASHTAG_TIMELINE_CACHE, _ACCOUNT_PROFILE_HASHTAG_TIMELINE_CACHE_LOCK)
    _clear_ttl_cache(_ACCOUNT_PROFILE_COLLABORATORS_CACHE, _ACCOUNT_PROFILE_COLLABORATORS_CACHE_LOCK)
    _clear_ttl_cache(_ACCOUNT_PROFILE_CATALOG_FRESHNESS_CACHE, _ACCOUNT_PROFILE_CATALOG_FRESHNESS_CACHE_LOCK)
    with _ACCOUNT_PROFILE_SINGLEFLIGHT_LOCK:
        _ACCOUNT_PROFILE_SINGLEFLIGHT.clear()


def _resolve_account_profile_singleflight(
    cache_key: tuple[Any, ...],
    loader: Callable[[], dict[str, Any]],
    *,
    cache: dict[Any, tuple[float, dict[str, Any]]] | None = None,
    cache_lock: Lock | None = None,
    ttl_seconds: int | None = None,
    max_entries: int | None = None,
) -> dict[str, Any]:
    if cache is not None and cache_lock is not None:
        cached_payload = _get_ttl_cached_payload(cache, cache_lock, cache_key)
        if cached_payload is not None:
            return cached_payload

    with _ACCOUNT_PROFILE_SINGLEFLIGHT_LOCK:
        in_flight = _ACCOUNT_PROFILE_SINGLEFLIGHT.get(cache_key)
        if in_flight is None:
            in_flight = Future()
            _ACCOUNT_PROFILE_SINGLEFLIGHT[cache_key] = in_flight
            owns_loader = True
        else:
            owns_loader = False

    if not owns_loader:
        return copy.deepcopy(in_flight.result())

    try:
        payload = loader()
        resolved_payload = copy.deepcopy(payload)
        if cache is not None and cache_lock is not None and ttl_seconds is not None and max_entries is not None:
            _set_ttl_cached_payload(
                cache,
                cache_lock,
                cache_key,
                resolved_payload,
                ttl_seconds=ttl_seconds,
                max_entries=max_entries,
            )
        in_flight.set_result(copy.deepcopy(resolved_payload))
        return resolved_payload
    except Exception as exc:
        in_flight.set_exception(exc)
        raise
    finally:
        with _ACCOUNT_PROFILE_SINGLEFLIGHT_LOCK:
            if _ACCOUNT_PROFILE_SINGLEFLIGHT.get(cache_key) is in_flight:
                _ACCOUNT_PROFILE_SINGLEFLIGHT.pop(cache_key, None)


def _catalog_freshness_fallback_payload(
    *,
    platform: str,
    account_handle: str,
    error: Exception,
    stale_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    if stale_payload is not None:
        degraded_payload = copy.deepcopy(stale_payload)
        degraded_payload["stale"] = True
        degraded_payload["degraded"] = True
        degraded_payload["freshness_error"] = {
            "code": "CATALOG_FRESHNESS_REFRESH_FAILED",
            "message": str(error) or "Failed to refresh catalog freshness",
            "retryable": True,
        }
        return degraded_payload

    detail = database_service_unavailable_detail(error) if is_database_service_unavailable_error(error) else {}
    reason = str(detail.get("reason") or "database_unavailable")
    return {
        "platform": str(platform or "").strip().lower(),
        "account_handle": str(account_handle or "").strip().lower().lstrip("@"),
        "eligible": False,
        "reason": "catalog_freshness_unavailable",
        "checked_at": datetime.now(tz=UTC).isoformat(),
        "stored_total_posts": None,
        "live_total_posts_current": None,
        "delta_posts": 0,
        "needs_recent_sync": False,
        "latest_catalog_run_status": None,
        "active_run_status": None,
        "catalog_newest_post_at": None,
        "catalog_oldest_post_at": None,
        "has_resumable_frontier": False,
        "frontier_pages_scanned": None,
        "frontier_posts_checked": None,
        "degraded": True,
        "stale": False,
        "recent_runs_available": False,
        "freshness_error": {
            "code": "CATALOG_FRESHNESS_UNAVAILABLE",
            "reason": reason,
            "message": "Catalog freshness is temporarily unavailable; stored profile data can still be shown.",
            "retryable": True,
            "retry_after_ms": detail.get("retry_after_ms", 1000),
        },
    }


def _resolve_account_profile_catalog_freshness(
    *,
    platform: str,
    account_handle: str,
    force: bool,
    statement_timeout_ms: int,
    loader: Callable[..., dict[str, Any]],
) -> dict[str, Any]:
    cache_key = _account_profile_cache_key(
        surface="catalog-freshness",
        platform=platform,
        account_handle=account_handle,
        extra=(statement_timeout_ms,),
    )
    if force:
        return _resolve_account_profile_singleflight(
            (*cache_key, "force"),
            lambda: loader(
                platform=platform,
                account_handle=account_handle,
                statement_timeout_ms=statement_timeout_ms,
            ),
        )

    try:
        return _resolve_account_profile_singleflight(
            cache_key,
            lambda: loader(
                platform=platform,
                account_handle=account_handle,
                use_cached_live_total_only=True,
                statement_timeout_ms=statement_timeout_ms,
            ),
            cache=_ACCOUNT_PROFILE_CATALOG_FRESHNESS_CACHE,
            cache_lock=_ACCOUNT_PROFILE_CATALOG_FRESHNESS_CACHE_LOCK,
            ttl_seconds=_ACCOUNT_PROFILE_CACHE_TTL_SECONDS,
            max_entries=_ACCOUNT_PROFILE_CACHE_MAX_ENTRIES,
        )
    except (ValueError, LookupError):
        raise
    except Exception as exc:
        stale_payload = _get_ttl_stale_payload(
            _ACCOUNT_PROFILE_CATALOG_FRESHNESS_CACHE,
            _ACCOUNT_PROFILE_CATALOG_FRESHNESS_CACHE_LOCK,
            cache_key,
        )
        if stale_payload is not None or is_database_service_unavailable_error(exc):
            return _catalog_freshness_fallback_payload(
                platform=platform,
                account_handle=account_handle,
                error=exc,
                stale_payload=stale_payload,
            )
        raise


def _resolve_account_profile_catalog_run_progress(
    *,
    platform: str,
    account_handle: str,
    run_id: str,
    recent_log_limit: int,
    fast: bool,
    loader: Callable[..., dict[str, Any]],
) -> dict[str, Any]:
    cache_key = _account_profile_cache_key(
        surface="catalog-run-progress",
        platform=platform,
        account_handle=account_handle,
        extra=(str(run_id), recent_log_limit, "fast" if fast else "full"),
    )
    return _resolve_account_profile_singleflight(
        cache_key,
        lambda: loader(
            platform=platform,
            account_handle=account_handle,
            run_id=str(run_id),
            recent_log_limit=recent_log_limit,
            fast=fast,
        ),
        cache=_ACCOUNT_PROFILE_PROGRESS_CACHE,
        cache_lock=_ACCOUNT_PROFILE_PROGRESS_CACHE_LOCK,
        ttl_seconds=_ACCOUNT_PROFILE_PROGRESS_CACHE_TTL_SECONDS,
        max_entries=_ACCOUNT_PROFILE_CACHE_MAX_ENTRIES,
    )
