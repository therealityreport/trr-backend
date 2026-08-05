"""Shared cache and singleflight state for backend person read models."""

from __future__ import annotations

import copy
import time
from collections.abc import Callable
from concurrent.futures import Future
from threading import Lock
from typing import Any

PersonReadLoader = Callable[[], tuple[dict[str, Any], int]]

_STATE_LOCK = Lock()
_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_INFLIGHT: dict[str, tuple[Future[dict[str, Any]], int]] = {}
_GENERATION = 0


def _cache_get_locked(key: str) -> dict[str, Any] | None:
    now = time.monotonic()
    entry = _CACHE.get(key)
    if not entry:
        return None
    expires_at, payload = entry
    if expires_at <= now:
        _CACHE.pop(key, None)
        return None
    return payload


def cache_get(key: str) -> dict[str, Any] | None:
    with _STATE_LOCK:
        return _cache_get_locked(key)


def invalidate_person_read_cache(*, person_id: str | None = None) -> None:
    """Invalidate person reads and detach older in-flight loads.

    The generation is intentionally global: a person-scoped write may make
    shared/derived person reads stale, and preventing an unrelated in-flight
    load from caching once is safer than allowing a pre-write result to land.
    """

    global _GENERATION

    prefixes = []
    if person_id:
        prefixes = [
            f"person:{person_id}:detail",
            f"person:{person_id}:cover-photo",
            f"person:{person_id}:gallery:",
        ]
    with _STATE_LOCK:
        _GENERATION += 1
        if not prefixes:
            _CACHE.clear()
            _INFLIGHT.clear()
            return
        for key in list(_CACHE):
            if any(key.startswith(prefix) for prefix in prefixes):
                _CACHE.pop(key, None)
        for key in list(_INFLIGHT):
            if any(key.startswith(prefix) for prefix in prefixes):
                _INFLIGHT.pop(key, None)


def resolve_person_read_singleflight(
    *,
    cache_key: str,
    ttl_seconds: int,
    loader: PersonReadLoader,
) -> tuple[dict[str, Any], int, str, str]:
    with _STATE_LOCK:
        cached = _cache_get_locked(cache_key)
        if cached is not None:
            return cached, 0, "hit", "none"

        in_flight = _INFLIGHT.get(cache_key)
        if in_flight is None:
            future: Future[dict[str, Any]] = Future()
            load_generation = _GENERATION
            _INFLIGHT[cache_key] = (future, load_generation)
            owns_loader = True
        else:
            future, load_generation = in_flight
            owns_loader = False

    if not owns_loader:
        return copy.deepcopy(future.result()), 0, "miss", "shared"

    try:
        payload, query_count = loader()
        resolved_payload = copy.deepcopy(payload)
        with _STATE_LOCK:
            if load_generation == _GENERATION and _INFLIGHT.get(cache_key) == (future, load_generation):
                _CACHE[cache_key] = (
                    time.monotonic() + ttl_seconds,
                    copy.deepcopy(resolved_payload),
                )
        future.set_result(copy.deepcopy(resolved_payload))
        return resolved_payload, query_count, "miss", "owner"
    except Exception as error:
        future.set_exception(error)
        raise
    finally:
        with _STATE_LOCK:
            if _INFLIGHT.get(cache_key) == (future, load_generation):
                _INFLIGHT.pop(cache_key, None)
