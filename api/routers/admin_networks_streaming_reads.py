from __future__ import annotations

import json
import logging
import os
import time
from threading import Event, Lock
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from api.auth import InternalAdminUser
from trr_backend.repositories import admin_networks_streaming_reads as networks_streaming_reads_repo
from trr_backend.repositories import brand_families

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/shows/networks-streaming", tags=["admin-networks-streaming-reads"])

_CACHE_LOCK = Lock()
_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_INFLIGHT_LOCK = Lock()
_INFLIGHT: dict[str, Event] = {}
_SUMMARY_CACHE_TTL_SECONDS = max(
    int(os.getenv("TRR_ADMIN_NETWORKS_STREAMING_SUMMARY_BACKEND_CACHE_TTL_SECONDS", "15")),
    1,
)
_DETAIL_CACHE_TTL_SECONDS = max(
    int(os.getenv("TRR_ADMIN_NETWORKS_STREAMING_DETAIL_BACKEND_CACHE_TTL_SECONDS", "30")),
    1,
)


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


def _get_or_build_cached_payload(
    key: str,
    *,
    ttl_seconds: int,
    builder: Any,
) -> tuple[dict[str, Any], int, str]:
    cached = _cache_get(key)
    if cached is not None:
        return cached, 0, "hit"

    leader = False
    event: Event | None = None
    with _INFLIGHT_LOCK:
        cached = _cache_get(key)
        if cached is not None:
            return cached, 0, "hit"
        event = _INFLIGHT.get(key)
        if event is None:
            event = Event()
            _INFLIGHT[key] = event
            leader = True

    if not leader and event is not None:
        event.wait(timeout=max(ttl_seconds, 5))
        cached = _cache_get(key)
        if cached is not None:
            return cached, 0, "deduped"

    if not leader:
        with _INFLIGHT_LOCK:
            event = _INFLIGHT.get(key)
            if event is None:
                event = Event()
                _INFLIGHT[key] = event
                leader = True

    if leader and event is not None:
        try:
            payload, query_count = builder()
            _cache_set(key, payload, ttl_seconds)
            return payload, query_count, "miss"
        finally:
            event.set()
            with _INFLIGHT_LOCK:
                if _INFLIGHT.get(key) is event:
                    _INFLIGHT.pop(key, None)

    cached = _cache_get(key)
    if cached is not None:
        return cached, 0, "deduped"
    payload, query_count = builder()
    _cache_set(key, payload, ttl_seconds)
    return payload, query_count, "miss"


def invalidate_networks_streaming_summary_cache() -> None:
    with _CACHE_LOCK:
        _CACHE.clear()
    with _INFLIGHT_LOCK:
        _INFLIGHT.clear()


def _payload_size_bytes(payload: dict[str, Any]) -> int:
    return len(json.dumps(jsonable_encoder(payload), separators=(",", ":"), default=str).encode("utf-8"))


def _log_read(route: str, *, query_count: int, payload: dict[str, Any], cache_status: str, started_at: float) -> None:
    logger.info(
        "[admin-networks-streaming-read] route=%s latency_ms=%.1f payload_bytes=%s query_count=%s cache=%s",
        route,
        (time.perf_counter() - started_at) * 1000.0,
        _payload_size_bytes(payload),
        query_count,
        cache_status,
    )


@router.get("/summary")
def get_networks_streaming_summary(_: InternalAdminUser = None) -> dict[str, Any]:
    started_at = time.perf_counter()
    payload, query_count, cache_status = _get_or_build_cached_payload(
        "summary",
        ttl_seconds=_SUMMARY_CACHE_TTL_SECONDS,
        builder=networks_streaming_reads_repo.get_networks_streaming_summary,
    )
    _log_read(
        "summary",
        query_count=query_count,
        payload=payload,
        cache_status=cache_status,
        started_at=started_at,
    )
    return payload


@router.post("/summary/cache/invalidate")
def invalidate_summary_cache(_: InternalAdminUser = None) -> dict[str, bool]:
    invalidate_networks_streaming_summary_cache()
    logger.info("[admin-networks-streaming-read] route=summary-invalidate-cache")
    return {"success": True}


@router.get("/detail")
def get_networks_streaming_detail(
    entity_type: str | None = Query(default=None),
    entity_key: str | None = Query(default=None),
    entity_slug: str | None = Query(default=None),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    normalized_type = str(entity_type or "").strip().lower()
    normalized_key = str(entity_key or "").strip()
    normalized_slug = str(entity_slug or "").strip()

    if normalized_type not in {"network", "streaming", "production"}:
        raise HTTPException(status_code=400, detail="entity_type must be network, streaming, or production")
    if not normalized_key and not normalized_slug:
        raise HTTPException(status_code=400, detail="entity_key or entity_slug is required")

    cache_key = f"detail:{normalized_type}:{normalized_key}:{normalized_slug}"

    def _build() -> tuple[dict[str, Any], int]:
        detail, query_count = networks_streaming_reads_repo.get_networks_streaming_detail(
            entity_type=normalized_type,
            entity_key=normalized_key or None,
            entity_slug=normalized_slug or None,
        )
        if detail is None:
            suggestions, _suggestions_query_count = networks_streaming_reads_repo.get_networks_streaming_suggestions(
                entity_type=normalized_type,
                entity_key=normalized_key or None,
                entity_slug=normalized_slug or None,
            )
            raise HTTPException(status_code=404, detail={"error": "not_found", "suggestions": suggestions})

        family = None
        family_suggestions = brand_families.list_family_suggestions().get("rows", [])
        shared_links: list[dict[str, Any]] = []
        wikipedia_show_urls: list[dict[str, Any]] = []
        if normalized_type in {"network", "streaming"}:
            family = brand_families.get_family_by_entity(entity_type=normalized_type, entity_key=detail["entity_key"])
            if family:
                family_id = str(family.get("id") or "")
                shared_links = brand_families.list_family_links(family_id=family_id, active_only=True).get("rows", [])
                wikipedia_show_urls = brand_families.list_family_wikipedia_show_links(
                    family_id=family_id,
                    limit=500,
                ).get("rows", [])

        payload = {
            **detail,
            "family": family,
            "family_suggestions": family_suggestions,
            "shared_links": shared_links,
            "wikipedia_show_urls": wikipedia_show_urls,
        }
        return payload, query_count

    try:
        payload, query_count, cache_status = _get_or_build_cached_payload(
            cache_key,
            ttl_seconds=_DETAIL_CACHE_TTL_SECONDS,
            builder=_build,
        )
    except HTTPException as exc:
        if exc.status_code == 404 and isinstance(exc.detail, dict):
            return_payload = exc.detail
            _log_read(
                "detail-not-found",
                query_count=0,
                payload=return_payload,
                cache_status="miss",
                started_at=started_at,
            )
            return JSONResponse(return_payload, status_code=404)
        raise
    _log_read(
        "detail",
        query_count=query_count,
        payload=payload,
        cache_status=cache_status,
        started_at=started_at,
    )
    return payload
