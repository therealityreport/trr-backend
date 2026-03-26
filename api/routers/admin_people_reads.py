from __future__ import annotations

import json
import logging
import os
import time
from threading import Lock
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.encoders import jsonable_encoder

from api.auth import InternalAdminUser
from trr_backend.repositories import admin_people_reads as people_repo

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/people", tags=["admin-people-reads"])

_CACHE_LOCK = Lock()
_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_RESOLVE_SLUG_CACHE_TTL_SECONDS = max(int(os.getenv("TRR_ADMIN_RESOLVE_SLUG_BACKEND_CACHE_TTL_SECONDS", "60")), 1)
_PERSON_DETAIL_CACHE_TTL_SECONDS = max(int(os.getenv("TRR_ADMIN_PERSON_DETAIL_BACKEND_CACHE_TTL_SECONDS", "15")), 1)
_PERSON_COVER_CACHE_TTL_SECONDS = max(int(os.getenv("TRR_ADMIN_PERSON_COVER_BACKEND_CACHE_TTL_SECONDS", "30")), 1)
_PERSON_GALLERY_CACHE_TTL_SECONDS = max(int(os.getenv("TRR_ADMIN_PERSON_GALLERY_BACKEND_CACHE_TTL_SECONDS", "10")), 1)


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


def _cache_set(key: str, payload: dict[str, Any], ttl_seconds: int) -> None:
    with _CACHE_LOCK:
        _CACHE[key] = (time.monotonic() + ttl_seconds, payload)


def invalidate_person_read_cache(*, person_id: str | None = None) -> None:
    prefixes = []
    if person_id:
        prefixes = [
            f"person:{person_id}:detail",
            f"person:{person_id}:cover-photo",
            f"person:{person_id}:gallery:",
        ]
    with _CACHE_LOCK:
        if not prefixes:
            _CACHE.clear()
            return
        for key in list(_CACHE.keys()):
            if any(key.startswith(prefix) for prefix in prefixes):
                _CACHE.pop(key, None)


def _payload_size_bytes(payload: dict[str, Any]) -> int:
    return len(json.dumps(jsonable_encoder(payload), separators=(",", ":"), default=str).encode("utf-8"))


def _log_read(route: str, *, query_count: int, payload: dict[str, Any], cache_status: str, started_at: float) -> None:
    logger.info(
        "[admin-people-read] route=%s latency_ms=%.1f payload_bytes=%s query_count=%s cache=%s",
        route,
        (time.perf_counter() - started_at) * 1000.0,
        _payload_size_bytes(payload),
        query_count,
        cache_status,
    )


@router.get("/resolve-slug")
def resolve_person_slug(
    slug: str = Query(min_length=1),
    show_id: str | None = Query(default=None),
    show_slug: str | None = Query(default=None),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    normalized_show_input = (show_id or show_slug or "").strip() or None
    cache_key = f"resolve:{slug.strip()}:{normalized_show_input or ''}"
    cached = _cache_get(cache_key)
    if cached is not None:
        _log_read("resolve-slug", query_count=0, payload=cached, cache_status="hit", started_at=started_at)
        return cached

    resolved, resolved_show_id, query_count = people_repo.resolve_person_slug(slug.strip(), normalized_show_input)
    if resolved is None:
        raise HTTPException(status_code=404, detail="person slug not found")
    payload = {"resolved": resolved, "show_id": resolved_show_id}
    _cache_set(cache_key, payload, _RESOLVE_SLUG_CACHE_TTL_SECONDS)
    _log_read("resolve-slug", query_count=query_count, payload=payload, cache_status="miss", started_at=started_at)
    return payload


@router.get("/{person_id}")
def get_person_detail(person_id: str, _: InternalAdminUser = None) -> dict[str, Any]:
    started_at = time.perf_counter()
    cache_key = f"person:{person_id}:detail"
    cached = _cache_get(cache_key)
    if cached is not None:
        _log_read("detail", query_count=0, payload=cached, cache_status="hit", started_at=started_at)
        return cached

    person, query_count = people_repo.get_person_detail(person_id)
    if person is None:
        raise HTTPException(status_code=404, detail="Person not found")
    payload = {"person": person}
    _cache_set(cache_key, payload, _PERSON_DETAIL_CACHE_TTL_SECONDS)
    _log_read("detail", query_count=query_count, payload=payload, cache_status="miss", started_at=started_at)
    return payload


@router.get("/{person_id}/cover-photo")
def get_person_cover_photo(person_id: str, _: InternalAdminUser = None) -> dict[str, Any]:
    started_at = time.perf_counter()
    cache_key = f"person:{person_id}:cover-photo"
    cached = _cache_get(cache_key)
    if cached is not None:
        _log_read("cover-photo", query_count=0, payload=cached, cache_status="hit", started_at=started_at)
        return cached

    cover_photo, query_count = people_repo.get_person_cover_photo(person_id)
    payload = {"coverPhoto": cover_photo}
    _cache_set(cache_key, payload, _PERSON_COVER_CACHE_TTL_SECONDS)
    _log_read("cover-photo", query_count=query_count, payload=payload, cache_status="miss", started_at=started_at)
    return payload


@router.get("/{person_id}/gallery")
def get_person_gallery(
    person_id: str,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    include_broken: bool = Query(default=False),
    sources: str | None = Query(default=None),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    normalized_sources = ",".join(
        sorted(value.strip().lower() for value in (sources or "").split(",") if value.strip())
    )
    cache_key = f"person:{person_id}:gallery:{limit}:{offset}:{1 if include_broken else 0}:{normalized_sources}"
    cached = _cache_get(cache_key)
    if cached is not None:
        _log_read("gallery", query_count=0, payload=cached, cache_status="hit", started_at=started_at)
        return cached

    payload, query_count = people_repo.get_person_gallery_page(
        person_id,
        limit=limit,
        offset=offset,
        include_broken=include_broken,
        sources=[value for value in normalized_sources.split(",") if value] or None,
    )
    _cache_set(cache_key, payload, _PERSON_GALLERY_CACHE_TTL_SECONDS)
    _log_read("gallery", query_count=query_count, payload=payload, cache_status="miss", started_at=started_at)
    return payload


@router.post("/{person_id}/cache/invalidate")
def invalidate_person_cache(person_id: str, _: InternalAdminUser = None) -> dict[str, bool]:
    invalidate_person_read_cache(person_id=person_id)
    logger.info("[admin-people-read] route=invalidate-cache person_id=%s", person_id)
    return {"success": True}
