"""Version-neutral networks/streaming cached read services."""

from __future__ import annotations

import os
import re
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from threading import Event, Lock
from typing import Any

from trr_backend.repositories import admin_networks_streaming_reads as repository
from trr_backend.repositories import brand_families

CachedPayloadBuilder = Callable[[], tuple[dict[str, Any], int]]


@dataclass
class _InFlightLoad:
    generation: int
    event: Event = field(default_factory=Event)
    result: tuple[dict[str, Any], int] | None = None
    error: Exception | None = None
    completed: bool = False
    invalidated: bool = False


class NetworksStreamingDetailNotFoundError(LookupError):
    """Raised when a detail lookup misses, with safe replacement suggestions."""

    def __init__(self, *, suggestions: list[dict[str, Any]], query_count: int) -> None:
        super().__init__("Networks/streaming entity not found")
        self.suggestions = suggestions
        self.query_count = query_count


def _cache_ttl_seconds(environment_name: str, default: int) -> int:
    raw = (os.getenv(environment_name) or str(default)).strip()
    try:
        return max(int(raw), 1)
    except ValueError:
        return default


_SUMMARY_CACHE_TTL_SECONDS = _cache_ttl_seconds(
    "TRR_ADMIN_NETWORKS_STREAMING_SUMMARY_BACKEND_CACHE_TTL_SECONDS",
    15,
)
_DETAIL_CACHE_TTL_SECONDS = _cache_ttl_seconds(
    "TRR_ADMIN_NETWORKS_STREAMING_DETAIL_BACKEND_CACHE_TTL_SECONDS",
    30,
)
_SUMMARY_CACHE_KEY = "summary"
_STATE_LOCK = Lock()
_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_INFLIGHT: dict[str, _InFlightLoad] = {}
_CACHE_GENERATION = 0


def _cache_get_locked(key: str) -> dict[str, Any] | None:
    now = time.monotonic()
    entry = _CACHE.get(key)
    if entry is None:
        return None
    expires_at, payload = entry
    if expires_at <= now:
        _CACHE.pop(key, None)
        return None
    return payload


def _get_or_build_cached_payload(
    key: str,
    *,
    ttl_seconds: int,
    builder: CachedPayloadBuilder,
) -> tuple[dict[str, Any], int, str]:
    while True:
        with _STATE_LOCK:
            cached = _cache_get_locked(key)
            if cached is not None:
                return cached, 0, "hit"
            inflight = _INFLIGHT.get(key)
            if inflight is None:
                inflight = _InFlightLoad(generation=_CACHE_GENERATION)
                _INFLIGHT[key] = inflight
                leader = True
            else:
                leader = False

        if leader:
            break

        completed = inflight.event.wait(timeout=max(ttl_seconds, 5))
        with _STATE_LOCK:
            if inflight.invalidated:
                continue
            if inflight.completed:
                if inflight.error is not None:
                    raise inflight.error
                if inflight.result is None:
                    raise RuntimeError("Networks/streaming in-flight read completed without an outcome")
                return inflight.result[0], 0, "deduped"
            if completed:
                continue
            if _INFLIGHT.get(key) is inflight:
                inflight.invalidated = True
                _INFLIGHT.pop(key, None)
                inflight.event.set()

    try:
        payload, query_count = builder()
        with _STATE_LOCK:
            inflight.result = (payload, query_count)
            inflight.completed = True
            if inflight.generation == _CACHE_GENERATION and not inflight.invalidated and _INFLIGHT.get(key) is inflight:
                _CACHE[key] = (time.monotonic() + ttl_seconds, payload)
        return payload, query_count, "miss"
    except Exception as error:
        with _STATE_LOCK:
            inflight.error = error
            inflight.completed = True
        raise
    finally:
        inflight.event.set()
        with _STATE_LOCK:
            if _INFLIGHT.get(key) is inflight:
                _INFLIGHT.pop(key, None)


def invalidate_networks_streaming_cache() -> None:
    """Invalidate summary/detail payloads and detach every older in-flight load."""

    global _CACHE_GENERATION

    with _STATE_LOCK:
        _CACHE_GENERATION += 1
        _CACHE.clear()
        inflight_loads = list(_INFLIGHT.values())
        for inflight in inflight_loads:
            inflight.invalidated = True
        _INFLIGHT.clear()
    for inflight in inflight_loads:
        inflight.event.set()


def invalidate_networks_streaming_summary_cache() -> None:
    """Backward-compatible invalidation entrypoint used by the v1 route."""

    invalidate_networks_streaming_cache()


def get_networks_streaming_summary() -> tuple[dict[str, Any], int, str]:
    return _get_or_build_cached_payload(
        _SUMMARY_CACHE_KEY,
        ttl_seconds=_SUMMARY_CACHE_TTL_SECONDS,
        builder=repository.get_networks_streaming_summary,
    )


def _normalize_entity_key(value: str | None) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).casefold()


def _normalize_entity_slug(value: str | None) -> str:
    normalized = str(value or "").strip().casefold().replace("&", " and ")
    normalized = re.sub(r"[^a-z0-9]+", "-", normalized)
    return re.sub(r"-{2,}", "-", normalized).strip("-")


def _detail_cache_key(*, entity_type: str, entity_key: str, entity_slug: str) -> str:
    return f"detail:{entity_type}:{entity_key}:{entity_slug}"


def _build_networks_streaming_detail(
    *,
    entity_type: str,
    entity_key: str,
    entity_slug: str,
) -> tuple[dict[str, Any], int]:
    detail, query_count = repository.get_networks_streaming_detail(
        entity_type=entity_type,
        entity_key=entity_key or None,
        entity_slug=entity_slug or None,
    )
    if detail is None:
        suggestions, suggestions_query_count = repository.get_networks_streaming_suggestions(
            entity_type=entity_type,
            entity_key=entity_key or None,
            entity_slug=entity_slug or None,
        )
        raise NetworksStreamingDetailNotFoundError(
            suggestions=suggestions,
            query_count=query_count + suggestions_query_count,
        )

    family = None
    family_suggestions = brand_families.list_family_suggestions().get("rows", [])
    shared_links: list[dict[str, Any]] = []
    wikipedia_show_urls: list[dict[str, Any]] = []
    if entity_type in {"network", "streaming"}:
        family = brand_families.get_family_by_entity(
            entity_type=entity_type,
            entity_key=str(detail["entity_key"]),
        )
        if family:
            family_id = str(family.get("id") or "")
            shared_links = brand_families.list_family_links(
                family_id=family_id,
                active_only=True,
            ).get("rows", [])
            wikipedia_show_urls = brand_families.list_family_wikipedia_show_links(
                family_id=family_id,
                limit=500,
            ).get("rows", [])

    return (
        {
            **detail,
            "family": family,
            "family_suggestions": family_suggestions,
            "shared_links": shared_links,
            "wikipedia_show_urls": wikipedia_show_urls,
        },
        query_count,
    )


def get_networks_streaming_detail(
    *,
    entity_type: str,
    entity_key: str | None = None,
    entity_slug: str | None = None,
) -> tuple[dict[str, Any], int, str]:
    normalized_type = str(entity_type or "").strip().casefold()
    normalized_key = _normalize_entity_key(entity_key)
    normalized_slug = _normalize_entity_slug(entity_slug)
    cache_key = _detail_cache_key(
        entity_type=normalized_type,
        entity_key=normalized_key,
        entity_slug=normalized_slug,
    )
    return _get_or_build_cached_payload(
        cache_key,
        ttl_seconds=_DETAIL_CACHE_TTL_SECONDS,
        builder=lambda: _build_networks_streaming_detail(
            entity_type=normalized_type,
            entity_key=normalized_key,
            entity_slug=normalized_slug,
        ),
    )
