"""Route-local cache helpers for admin social analytics endpoints."""

from __future__ import annotations

import copy
import logging
import os
from datetime import UTC, datetime
from threading import Lock
from time import monotonic
from typing import Any, Literal

from trr_backend.socials.inline_ingest import (
    normalize_target_platforms as _normalize_target_platforms,
)
from trr_backend.socials.platforms import SOCIAL_SUPPORTED_PLATFORMS

logger = logging.getLogger("api.routers.socials")

WeekDetailSortField = Literal["engagement", "likes", "views", "comments_count", "shares", "retweets", "posted_at"]
WeekDetailSortDir = Literal["asc", "desc"]
WeekSummaryInclude = Literal["totals_only", "full"]

_WEEK_DETAIL_CACHE_TTL_SECONDS = int(os.getenv("WEEK_DETAIL_CACHE_TTL_SECONDS", "90"))
_WEEK_DETAIL_CACHE_MAX_ENTRIES = int(os.getenv("WEEK_DETAIL_CACHE_MAX_ENTRIES", "256"))
_WEEK_DETAIL_CACHE: dict[Any, tuple[float, dict[str, Any]]] = {}
_WEEK_DETAIL_CACHE_LOCK = Lock()
_WEEK_SUMMARY_CACHE_TTL_SECONDS = int(os.getenv("WEEK_SUMMARY_CACHE_TTL_SECONDS", str(_WEEK_DETAIL_CACHE_TTL_SECONDS)))
_WEEK_SUMMARY_CACHE_MAX_ENTRIES = int(os.getenv("WEEK_SUMMARY_CACHE_MAX_ENTRIES", "256"))
_WEEK_SUMMARY_CACHE: dict[Any, tuple[float, dict[str, Any]]] = {}
_WEEK_SUMMARY_CACHE_LOCK = Lock()
_ANALYTICS_CACHE_TTL_SECONDS = int(os.getenv("SOCIAL_ANALYTICS_CACHE_TTL_SECONDS", "20"))
_ANALYTICS_CACHE_MAX_ENTRIES = int(os.getenv("SOCIAL_ANALYTICS_CACHE_MAX_ENTRIES", "128"))
_ANALYTICS_CACHE: dict[Any, tuple[float, dict[str, Any]]] = {}
_ANALYTICS_CACHE_LOCK = Lock()
_WEEK_LIVE_HEALTH_CACHE_TTL_SECONDS = int(os.getenv("SOCIAL_WEEK_LIVE_HEALTH_CACHE_TTL_SECONDS", "8"))
_WEEK_LIVE_HEALTH_CACHE_MAX_ENTRIES = int(os.getenv("SOCIAL_WEEK_LIVE_HEALTH_CACHE_MAX_ENTRIES", "128"))
_WEEK_LIVE_HEALTH_CACHE: dict[Any, tuple[float, dict[str, Any]]] = {}
_WEEK_LIVE_HEALTH_CACHE_LOCK = Lock()
_COVERAGE_CACHE_TTL_SECONDS = int(os.getenv("SOCIAL_COVERAGE_CACHE_TTL_SECONDS", "20"))
_COVERAGE_CACHE_MAX_ENTRIES = int(os.getenv("SOCIAL_COVERAGE_CACHE_MAX_ENTRIES", "128"))
_COMMENTS_COVERAGE_CACHE: dict[Any, tuple[float, dict[str, Any]]] = {}
_COMMENTS_COVERAGE_CACHE_LOCK = Lock()
_MIRROR_COVERAGE_CACHE: dict[Any, tuple[float, dict[str, Any]]] = {}
_MIRROR_COVERAGE_CACHE_LOCK = Lock()


def _cache_datetime_key(value: datetime | None) -> str | None:
    if not isinstance(value, datetime):
        return None
    normalized = value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    return normalized.isoformat().replace("+00:00", "Z")


def _normalized_platform_key(platforms: list[str] | None) -> str:
    return ",".join(
        sorted(
            _normalize_target_platforms(
                platforms,
                supported_platforms=SOCIAL_SUPPORTED_PLATFORMS,
            )
        )
    )


def _week_detail_cache_key(
    *,
    season_id: str,
    week_index: int,
    source_scope: str,
    platforms: list[str] | None,
    timezone: str,
    max_comments_per_post: int,
    sort_field: WeekDetailSortField,
    sort_dir: WeekDetailSortDir,
    include_status: bool = True,
) -> tuple[Any, ...]:
    platform_key = _normalized_platform_key(platforms)
    return (
        season_id,
        week_index,
        source_scope.strip().lower(),
        platform_key,
        timezone,
        int(max_comments_per_post),
        sort_field,
        sort_dir,
        bool(include_status),
    )


def _week_summary_cache_key(
    *,
    season_id: str,
    week_index: int,
    source_scope: str,
    platforms: list[str] | None,
    timezone: str,
    include: WeekSummaryInclude,
    max_comments_per_post: int,
    sort_field: WeekDetailSortField,
    sort_dir: WeekDetailSortDir,
) -> tuple[Any, ...]:
    platform_key = _normalized_platform_key(platforms)
    return (
        season_id,
        week_index,
        source_scope.strip().lower(),
        platform_key,
        timezone,
        include,
        int(max_comments_per_post),
        sort_field,
        sort_dir,
    )


def _analytics_cache_key(
    *,
    season_id: str,
    source_scope: str,
    platforms: list[str] | None,
    timezone: str,
    week: int | None,
    include_rows: bool,
    include_flags: bool,
    include_schedule: bool,
    include_benchmark: bool,
) -> tuple[Any, ...]:
    platform_key = _normalized_platform_key(platforms)
    return (
        season_id,
        source_scope.strip().lower(),
        platform_key,
        timezone,
        int(week) if week is not None else None,
        bool(include_rows),
        bool(include_flags),
        bool(include_schedule),
        bool(include_benchmark),
    )


def _week_live_health_cache_key(
    *,
    season_id: str,
    week_index: int,
    source_scope: str,
    platforms: list[str] | None,
    timezone: str,
) -> tuple[Any, ...]:
    platform_key = _normalized_platform_key(platforms)
    return (
        season_id,
        int(week_index),
        source_scope.strip().lower(),
        platform_key,
        timezone,
    )


def _coverage_cache_window_key(
    *,
    season_id: str,
    source_scope: str,
    platforms: list[str] | None,
    timezone: str,
    date_start: datetime | None,
    date_end: datetime | None,
) -> tuple[Any, ...]:
    platform_key = _normalized_platform_key(platforms)
    return (
        season_id,
        source_scope.strip().lower(),
        platform_key,
        timezone,
        _cache_datetime_key(date_start),
        _cache_datetime_key(date_end),
    )


def _get_week_detail_cached_payload(cache_key: tuple[Any, ...]) -> dict[str, Any] | None:
    now = monotonic()
    with _WEEK_DETAIL_CACHE_LOCK:
        cached = _WEEK_DETAIL_CACHE.get(cache_key)
        if not cached:
            return None
        expires_at, payload = cached
        if expires_at <= now:
            _WEEK_DETAIL_CACHE.pop(cache_key, None)
            return None
        return copy.deepcopy(payload)


def _set_week_detail_cached_payload(cache_key: tuple[Any, ...], payload: dict[str, Any]) -> None:
    with _WEEK_DETAIL_CACHE_LOCK:
        _WEEK_DETAIL_CACHE[cache_key] = (monotonic() + _WEEK_DETAIL_CACHE_TTL_SECONDS, copy.deepcopy(payload))
        if len(_WEEK_DETAIL_CACHE) <= _WEEK_DETAIL_CACHE_MAX_ENTRIES:
            return
        items_by_expiry = sorted(
            _WEEK_DETAIL_CACHE.items(),
            key=lambda item: item[1][0],
        )
        for key, _ in items_by_expiry[:-_WEEK_DETAIL_CACHE_MAX_ENTRIES]:
            _WEEK_DETAIL_CACHE.pop(key, None)


def _get_week_summary_cached_payload(cache_key: tuple[Any, ...]) -> dict[str, Any] | None:
    now = monotonic()
    with _WEEK_SUMMARY_CACHE_LOCK:
        cached = _WEEK_SUMMARY_CACHE.get(cache_key)
        if not cached:
            return None
        expires_at, payload = cached
        if expires_at <= now:
            _WEEK_SUMMARY_CACHE.pop(cache_key, None)
            return None
        return copy.deepcopy(payload)


def _set_week_summary_cached_payload(cache_key: tuple[Any, ...], payload: dict[str, Any]) -> None:
    with _WEEK_SUMMARY_CACHE_LOCK:
        _WEEK_SUMMARY_CACHE[cache_key] = (monotonic() + _WEEK_SUMMARY_CACHE_TTL_SECONDS, copy.deepcopy(payload))
        if len(_WEEK_SUMMARY_CACHE) <= _WEEK_SUMMARY_CACHE_MAX_ENTRIES:
            return
        items_by_expiry = sorted(
            _WEEK_SUMMARY_CACHE.items(),
            key=lambda item: item[1][0],
        )
        for key, _ in items_by_expiry[:-_WEEK_SUMMARY_CACHE_MAX_ENTRIES]:
            _WEEK_SUMMARY_CACHE.pop(key, None)


def _get_ttl_cached_payload(
    cache: dict[Any, tuple[float, dict[str, Any]]],
    lock: Lock,
    cache_key: tuple[Any, ...],
) -> dict[str, Any] | None:
    now = monotonic()
    with lock:
        cached = cache.get(cache_key)
        if not cached:
            return None
        expires_at, payload = cached
        if expires_at <= now:
            return None
        return copy.deepcopy(payload)


def _get_ttl_stale_payload(
    cache: dict[Any, tuple[float, dict[str, Any]]],
    lock: Lock,
    cache_key: tuple[Any, ...],
) -> dict[str, Any] | None:
    with lock:
        cached = cache.get(cache_key)
        if not cached:
            return None
        _expires_at, payload = cached
        return copy.deepcopy(payload)


def _set_ttl_cached_payload(
    cache: dict[Any, tuple[float, dict[str, Any]]],
    lock: Lock,
    cache_key: tuple[Any, ...],
    payload: dict[str, Any],
    *,
    ttl_seconds: int,
    max_entries: int,
) -> None:
    if ttl_seconds <= 0:
        return
    with lock:
        cache[cache_key] = (monotonic() + ttl_seconds, copy.deepcopy(payload))
        if len(cache) <= max_entries:
            return
        items_by_expiry = sorted(
            cache.items(),
            key=lambda item: item[1][0],
        )
        for key, _ in items_by_expiry[:-max_entries]:
            cache.pop(key, None)


def _clear_ttl_cache(cache: dict[Any, tuple[float, dict[str, Any]]], lock: Lock) -> None:
    with lock:
        cache.clear()


def invalidate_week_summary_cache() -> None:
    with _WEEK_SUMMARY_CACHE_LOCK:
        _WEEK_SUMMARY_CACHE.clear()


def invalidate_week_detail_cache() -> None:
    """Clear in-memory week-detail and week-summary caches after ingest mutations."""
    with _WEEK_DETAIL_CACHE_LOCK:
        _WEEK_DETAIL_CACHE.clear()
    invalidate_week_summary_cache()
    _clear_ttl_cache(_ANALYTICS_CACHE, _ANALYTICS_CACHE_LOCK)
    _clear_ttl_cache(_WEEK_LIVE_HEALTH_CACHE, _WEEK_LIVE_HEALTH_CACHE_LOCK)
    _clear_ttl_cache(_COMMENTS_COVERAGE_CACHE, _COMMENTS_COVERAGE_CACHE_LOCK)
    _clear_ttl_cache(_MIRROR_COVERAGE_CACHE, _MIRROR_COVERAGE_CACHE_LOCK)


def _register_week_detail_cache_invalidator() -> None:
    try:
        from trr_backend.repositories.social_season_analytics import register_week_detail_cache_invalidator

        register_week_detail_cache_invalidator(invalidate_week_detail_cache)
    except Exception:  # noqa: BLE001
        logger.debug("Failed to register week-detail cache invalidator hook", exc_info=True)


_register_week_detail_cache_invalidator()
