from __future__ import annotations

import copy
import json
import logging
import os
import time
from concurrent.futures import Future
from threading import Lock
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder

from api.auth import InternalAdminUser
from trr_backend.db.pg import (
    database_service_unavailable_detail,
    is_database_service_unavailable_error,
)
from trr_backend.repositories import admin_people_reads as people_repo

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/people", tags=["admin-people-reads"])

_CACHE_LOCK = Lock()
_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_INFLIGHT_LOCK = Lock()
_INFLIGHT: dict[str, Future[dict[str, Any]]] = {}
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
    with _INFLIGHT_LOCK:
        if not prefixes:
            _INFLIGHT.clear()
            return
        for key in list(_INFLIGHT.keys()):
            if any(key.startswith(prefix) for prefix in prefixes):
                _INFLIGHT.pop(key, None)


def _payload_size_bytes(payload: dict[str, Any]) -> int:
    return len(json.dumps(jsonable_encoder(payload), separators=(",", ":"), default=str).encode("utf-8"))


def _log_read(
    route: str,
    *,
    query_count: int,
    payload: dict[str, Any],
    cache_status: str,
    singleflight_status: str,
    request_role: str,
    repo_ms: float,
    started_at: float,
) -> None:
    logger.info(
        (
            "[admin-people-read] route=%s latency_ms=%.1f repo_ms=%.1f "
            "payload_bytes=%s query_count=%s cache=%s singleflight=%s request_role=%s"
        ),
        route,
        (time.perf_counter() - started_at) * 1000.0,
        repo_ms,
        _payload_size_bytes(payload),
        query_count,
        cache_status,
        singleflight_status,
        request_role,
    )


def _request_role(request: Request) -> str:
    raw = str(request.headers.get("x-trr-admin-request-role") or "").strip().lower()
    return raw if raw in {"primary", "secondary", "polling"} else "unspecified"


def _resolve_people_read_singleflight(
    *,
    cache_key: str,
    ttl_seconds: int,
    loader: Any,
) -> tuple[dict[str, Any], int, str, str]:
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached, 0, "hit", "none"

    with _INFLIGHT_LOCK:
        in_flight = _INFLIGHT.get(cache_key)
        if in_flight is None:
            in_flight = Future()
            _INFLIGHT[cache_key] = in_flight
            owns_loader = True
        else:
            owns_loader = False

    if not owns_loader:
        return copy.deepcopy(in_flight.result()), 0, "miss", "shared"

    try:
        payload, query_count = loader()
        resolved_payload = copy.deepcopy(payload)
        _cache_set(cache_key, resolved_payload, ttl_seconds)
        in_flight.set_result(copy.deepcopy(resolved_payload))
        return resolved_payload, query_count, "miss", "owner"
    except Exception as exc:
        in_flight.set_exception(exc)
        raise
    finally:
        with _INFLIGHT_LOCK:
            if _INFLIGHT.get(cache_key) is in_flight:
                _INFLIGHT.pop(cache_key, None)


def _to_people_read_http_exception(error: Exception) -> HTTPException:
    if isinstance(error, HTTPException):
        return error
    if isinstance(error, ValueError):
        return HTTPException(status_code=400, detail=str(error))
    if is_database_service_unavailable_error(error):
        return HTTPException(status_code=503, detail=database_service_unavailable_detail(error))
    return HTTPException(status_code=500, detail=str(error) or "Internal server error")


@router.get("/resolve-slug")
def resolve_person_slug(
    request: Request,
    slug: str = Query(min_length=1),
    show_id: str | None = Query(default=None),
    show_slug: str | None = Query(default=None),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    request_role = _request_role(request)
    normalized_show_input = (show_id or show_slug or "").strip() or None
    cache_key = f"resolve:{slug.strip()}:{normalized_show_input or ''}"
    repo_started_at = time.perf_counter()
    try:
        payload, query_count, cache_status, singleflight_status = _resolve_people_read_singleflight(
            cache_key=cache_key,
            ttl_seconds=_RESOLVE_SLUG_CACHE_TTL_SECONDS,
            loader=lambda: _resolve_person_slug_payload(slug.strip(), normalized_show_input),
        )
    except Exception as error:
        raise _to_people_read_http_exception(error) from error
    _log_read(
        "resolve-slug",
        query_count=query_count,
        payload=payload,
        cache_status=cache_status,
        singleflight_status=singleflight_status,
        request_role=request_role,
        repo_ms=(time.perf_counter() - repo_started_at) * 1000.0,
        started_at=started_at,
    )
    return payload


@router.get("/{person_id}")
def get_person_detail(request: Request, person_id: str, _: InternalAdminUser = None) -> dict[str, Any]:
    started_at = time.perf_counter()
    request_role = _request_role(request)
    cache_key = f"person:{person_id}:detail"
    repo_started_at = time.perf_counter()
    try:
        payload, query_count, cache_status, singleflight_status = _resolve_people_read_singleflight(
            cache_key=cache_key,
            ttl_seconds=_PERSON_DETAIL_CACHE_TTL_SECONDS,
            loader=lambda: _get_person_detail_payload(person_id),
        )
    except Exception as error:
        raise _to_people_read_http_exception(error) from error
    _log_read(
        "detail",
        query_count=query_count,
        payload=payload,
        cache_status=cache_status,
        singleflight_status=singleflight_status,
        request_role=request_role,
        repo_ms=(time.perf_counter() - repo_started_at) * 1000.0,
        started_at=started_at,
    )
    return payload


@router.get("/{person_id}/cover-photo")
def get_person_cover_photo(request: Request, person_id: str, _: InternalAdminUser = None) -> dict[str, Any]:
    started_at = time.perf_counter()
    request_role = _request_role(request)
    cache_key = f"person:{person_id}:cover-photo"
    repo_started_at = time.perf_counter()
    try:
        payload, query_count, cache_status, singleflight_status = _resolve_people_read_singleflight(
            cache_key=cache_key,
            ttl_seconds=_PERSON_COVER_CACHE_TTL_SECONDS,
            loader=lambda: _get_person_cover_photo_payload(person_id),
        )
    except Exception as error:
        raise _to_people_read_http_exception(error) from error
    _log_read(
        "cover-photo",
        query_count=query_count,
        payload=payload,
        cache_status=cache_status,
        singleflight_status=singleflight_status,
        request_role=request_role,
        repo_ms=(time.perf_counter() - repo_started_at) * 1000.0,
        started_at=started_at,
    )
    return payload


@router.get("/{person_id}/gallery")
def get_person_gallery(
    request: Request,
    person_id: str,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    include_broken: bool = Query(default=False),
    include_total_count: bool = Query(default=True),
    sources: str | None = Query(default=None),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    request_role = _request_role(request)
    normalized_sources = ",".join(
        sorted(value.strip().lower() for value in (sources or "").split(",") if value.strip())
    )
    cache_key = (
        f"person:{person_id}:gallery:{limit}:{offset}:{1 if include_broken else 0}:{1 if include_total_count else 0}:"
        f"{normalized_sources}"
    )
    repo_started_at = time.perf_counter()
    try:
        payload, query_count, cache_status, singleflight_status = _resolve_people_read_singleflight(
            cache_key=cache_key,
            ttl_seconds=_PERSON_GALLERY_CACHE_TTL_SECONDS,
            loader=lambda: _get_person_gallery_payload(
                person_id,
                limit=limit,
                offset=offset,
                include_broken=include_broken,
                include_total_count=include_total_count,
                normalized_sources=normalized_sources,
            ),
        )
    except Exception as error:
        raise _to_people_read_http_exception(error) from error
    _log_read(
        "gallery",
        query_count=query_count,
        payload=payload,
        cache_status=cache_status,
        singleflight_status=singleflight_status,
        request_role=request_role,
        repo_ms=(time.perf_counter() - repo_started_at) * 1000.0,
        started_at=started_at,
    )
    return payload


@router.post("/{person_id}/cache/invalidate")
def invalidate_person_cache(person_id: str, _: InternalAdminUser = None) -> dict[str, bool]:
    invalidate_person_read_cache(person_id=person_id)
    logger.info("[admin-people-read] route=invalidate-cache person_id=%s", person_id)
    return {"success": True}


def _resolve_person_slug_payload(slug: str, normalized_show_input: str | None) -> tuple[dict[str, Any], int]:
    resolved, resolved_show_id, query_count = people_repo.resolve_person_slug(slug, normalized_show_input)
    if resolved is None:
        raise HTTPException(status_code=404, detail="person slug not found")
    return {"resolved": resolved, "show_id": resolved_show_id}, query_count


def _get_person_detail_payload(person_id: str) -> tuple[dict[str, Any], int]:
    person, query_count = people_repo.get_person_detail(person_id)
    if person is None:
        raise HTTPException(status_code=404, detail="Person not found")
    return {"person": person}, query_count


def _get_person_cover_photo_payload(person_id: str) -> tuple[dict[str, Any], int]:
    cover_photo, query_count = people_repo.get_person_cover_photo(person_id)
    return {"coverPhoto": cover_photo}, query_count


def _get_person_gallery_payload(
    person_id: str,
    *,
    limit: int,
    offset: int,
    include_broken: bool,
    include_total_count: bool,
    normalized_sources: str,
) -> tuple[dict[str, Any], int]:
    return people_repo.get_person_gallery_page(
        person_id,
        limit=limit,
        offset=offset,
        include_broken=include_broken,
        sources=[value for value in normalized_sources.split(",") if value] or None,
        include_total_count=include_total_count,
    )
