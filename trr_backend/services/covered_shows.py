"""Version-neutral covered-shows service and shared read cache."""

from __future__ import annotations

import os
import time
from threading import Lock
from typing import Any, Literal

from trr_backend.repositories import covered_shows as covered_shows_repo
from trr_backend.services import networks_streaming_reads as networks_streaming_reads_service

CacheStatus = Literal["hit", "miss"]

_CACHE_LOCK = Lock()
_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_CACHE_TTL_SECONDS = max(int(os.getenv("TRR_ADMIN_COVERED_SHOWS_BACKEND_CACHE_TTL_SECONDS", "30")), 1)


def _cache_get(key: str) -> dict[str, Any] | None:
    now = time.monotonic()
    with _CACHE_LOCK:
        entry = _CACHE.get(key)
        if not entry:
            return None
        expires_at, payload = entry
        if expires_at <= now:
            _CACHE.pop(key, None)
            return None
        return payload


def _cache_set(key: str, payload: dict[str, Any]) -> None:
    with _CACHE_LOCK:
        _CACHE[key] = (time.monotonic() + _CACHE_TTL_SECONDS, payload)


def invalidate_cache() -> None:
    with _CACHE_LOCK:
        _CACHE.clear()


def list_covered_shows() -> tuple[dict[str, Any], int, CacheStatus]:
    cached = _cache_get("list")
    if cached is not None:
        return cached, 0, "hit"

    shows, query_count = covered_shows_repo.list_covered_shows()
    payload = {"shows": shows}
    _cache_set("list", payload)
    return payload, query_count, "miss"


def get_covered_show(show_id: str) -> tuple[dict[str, Any] | None, int, CacheStatus]:
    cache_key = f"show:{show_id}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached, 0, "hit"

    show, query_count = covered_shows_repo.get_covered_show(show_id)
    if show is not None:
        _cache_set(cache_key, show)
    return show, query_count, "miss"


def add_covered_show(*, show_id: str, show_name: str, actor_uid: str) -> tuple[dict[str, Any], int]:
    show, query_count = covered_shows_repo.add_covered_show(
        show_id=show_id,
        show_name=show_name,
        actor_uid=actor_uid,
    )
    invalidate_cache()
    networks_streaming_reads_service.invalidate_networks_streaming_cache()
    return show, query_count


def remove_covered_show(show_id: str) -> tuple[bool, int]:
    deleted, query_count = covered_shows_repo.remove_covered_show(show_id)
    if deleted:
        invalidate_cache()
        networks_streaming_reads_service.invalidate_networks_streaming_cache()
    return deleted, query_count
