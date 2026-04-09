from __future__ import annotations

import base64
import json
import logging
import os
import time
from threading import Event, Lock
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Query
from fastapi.encoders import jsonable_encoder

from api.auth import InternalAdminUser
from trr_backend.repositories import admin_show_reads as show_reads_repo

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/trr-api", tags=["admin-show-reads"])

_CACHE_LOCK = Lock()
_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_INFLIGHT_LOCK = Lock()
_INFLIGHT: dict[str, Event] = {}
_SEARCH_CACHE_TTL_SECONDS = max(int(os.getenv("TRR_ADMIN_SEARCH_BACKEND_CACHE_TTL_SECONDS", "15")), 1)
_SHOWS_CACHE_TTL_SECONDS = max(int(os.getenv("TRR_ADMIN_SHOWS_BACKEND_CACHE_TTL_SECONDS", "15")), 1)
_RESOLVE_SLUG_CACHE_TTL_SECONDS = max(int(os.getenv("TRR_ADMIN_SHOW_RESOLVE_SLUG_BACKEND_CACHE_TTL_SECONDS", "60")), 1)
_PEOPLE_HOME_CACHE_TTL_SECONDS = max(int(os.getenv("TRR_ADMIN_PEOPLE_HOME_BACKEND_CACHE_TTL_SECONDS", "20")), 1)
_SHOW_DETAIL_CACHE_TTL_SECONDS = max(int(os.getenv("TRR_ADMIN_SHOW_DETAIL_BACKEND_CACHE_TTL_SECONDS", "30")), 1)
_SHOW_SEASONS_CACHE_TTL_SECONDS = max(int(os.getenv("TRR_ADMIN_SHOW_SEASONS_BACKEND_CACHE_TTL_SECONDS", "30")), 1)
_SHOW_ASSETS_CACHE_TTL_SECONDS = max(int(os.getenv("TRR_ADMIN_SHOW_ASSETS_BACKEND_CACHE_TTL_SECONDS", "30")), 1)
_SEASON_EPISODES_CACHE_TTL_SECONDS = max(int(os.getenv("TRR_ADMIN_SEASON_EPISODES_BACKEND_CACHE_TTL_SECONDS", "30")), 1)
_SHOW_CAST_CACHE_TTL_SECONDS = max(int(os.getenv("TRR_ADMIN_SHOW_CAST_BACKEND_CACHE_TTL_SECONDS", "45")), 1)
_SHOW_CREDITS_CACHE_TTL_SECONDS = max(int(os.getenv("TRR_ADMIN_SHOW_CREDITS_BACKEND_CACHE_TTL_SECONDS", "45")), 1)
_SEASON_CAST_CACHE_TTL_SECONDS = max(int(os.getenv("TRR_ADMIN_SEASON_CAST_BACKEND_CACHE_TTL_SECONDS", "45")), 1)
_SEASON_ASSETS_CACHE_TTL_SECONDS = max(int(os.getenv("TRR_ADMIN_SEASON_ASSETS_BACKEND_CACHE_TTL_SECONDS", "30")), 1)
_SEASON_BACKDROPS_CACHE_TTL_SECONDS = max(
    int(os.getenv("TRR_ADMIN_SEASON_BACKDROPS_BACKEND_CACHE_TTL_SECONDS", "30")),
    1,
)
_ASSET_CURSOR_PREFIX = "offset:"


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


def invalidate_show_read_cache(*, show_id: str | None = None) -> None:
    with _CACHE_LOCK:
        # A show detail mutation can change list/search/slug results as well, so clear the
        # full route cache rather than attempting partial key invalidation.
        _CACHE.clear()
    with _INFLIGHT_LOCK:
        _INFLIGHT.clear()


def invalidate_show_reads_cache() -> None:
    invalidate_show_read_cache()


def _encode_asset_cursor(offset: int) -> str | None:
    normalized_offset = max(0, int(offset))
    if normalized_offset <= 0:
        return None
    encoded = base64.urlsafe_b64encode(f"{_ASSET_CURSOR_PREFIX}{normalized_offset}".encode())
    return encoded.decode("ascii")


def _decode_asset_cursor(cursor: str | None) -> int:
    raw = str(cursor or "").strip()
    if not raw:
        return 0
    try:
        decoded = base64.urlsafe_b64decode(raw.encode("ascii")).decode("utf-8")
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail="Invalid asset cursor") from exc
    if not decoded.startswith(_ASSET_CURSOR_PREFIX):
        raise HTTPException(status_code=400, detail="Invalid asset cursor")
    offset_text = decoded.removeprefix(_ASSET_CURSOR_PREFIX).strip()
    if not offset_text.isdigit():
        raise HTTPException(status_code=400, detail="Invalid asset cursor")
    return max(0, int(offset_text))


def _build_asset_pagination(
    *,
    limit: int,
    offset: int,
    count: int,
    has_more: bool,
    full: bool,
    truncated: bool = False,
) -> dict[str, Any]:
    next_cursor = _encode_asset_cursor(offset + count) if has_more else None
    return {
        "limit": limit,
        "offset": offset,
        "count": count,
        "has_more": has_more,
        "next_cursor": next_cursor,
        "cursor": _encode_asset_cursor(offset),
        "full": full,
        "truncated": truncated,
    }


def _payload_size_bytes(payload: dict[str, Any]) -> int:
    return len(json.dumps(jsonable_encoder(payload), separators=(",", ":"), default=str).encode("utf-8"))


def _log_read(route: str, *, query_count: int, payload: dict[str, Any], cache_status: str, started_at: float) -> None:
    logger.info(
        "[admin-show-read] route=%s latency_ms=%.1f payload_bytes=%s query_count=%s cache=%s",
        route,
        (time.perf_counter() - started_at) * 1000.0,
        _payload_size_bytes(payload),
        query_count,
        cache_status,
    )


@router.get("/search")
def search_admin_show_reads(
    q: str = Query(..., min_length=3),
    limit: int = Query(8, ge=1, le=20),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    cache_key = f"search:{q.strip().lower()}:{limit}"
    payload, query_count, cache_status = _get_or_build_cached_payload(
        cache_key,
        ttl_seconds=_SEARCH_CACHE_TTL_SECONDS,
        builder=lambda: show_reads_repo.search_global(q.strip(), limit=limit),
    )
    _log_read("search", query_count=query_count, payload=payload, cache_status=cache_status, started_at=started_at)
    return payload


@router.get("/shows")
def list_admin_shows(
    q: str = Query(..., min_length=1),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    cache_key = f"shows:{q.strip().lower()}:{limit}:{offset}"

    def _build_shows_payload() -> tuple[dict[str, Any], int]:
        shows, query_count = show_reads_repo.search_shows(q.strip(), limit=limit, offset=offset)
        return (
            {
                "shows": shows,
                "pagination": {
                    "limit": limit,
                    "offset": offset,
                    "count": len(shows),
                },
            },
            query_count,
        )

    payload, query_count, cache_status = _get_or_build_cached_payload(
        cache_key,
        ttl_seconds=_SHOWS_CACHE_TTL_SECONDS,
        builder=_build_shows_payload,
    )
    _log_read("shows", query_count=query_count, payload=payload, cache_status=cache_status, started_at=started_at)
    return payload


@router.get("/shows/resolve-slug")
def resolve_admin_show_slug(slug: str = Query(..., min_length=1), _: InternalAdminUser = None) -> dict[str, Any]:
    started_at = time.perf_counter()
    cache_key = f"resolve-slug:{slug.strip().lower()}"

    def _build_resolved() -> tuple[dict[str, Any], int]:
        resolved, query_count = show_reads_repo.resolve_show_slug(slug.strip())
        if resolved is None:
            raise HTTPException(status_code=404, detail="show slug not found")
        return {"resolved": resolved}, query_count

    payload, query_count, cache_status = _get_or_build_cached_payload(
        cache_key,
        ttl_seconds=_RESOLVE_SLUG_CACHE_TTL_SECONDS,
        builder=_build_resolved,
    )
    _log_read(
        "resolve-slug",
        query_count=query_count,
        payload=payload,
        cache_status=cache_status,
        started_at=started_at,
    )
    return payload


@router.get("/people/home")
def get_people_home(
    limit: int = Query(12, ge=1, le=24),
    x_trr_admin_user_uid: str | None = Header(default=None, alias="X-TRR-Admin-User-Uid"),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    normalized_uid = (x_trr_admin_user_uid or "").strip() or None
    cache_key = f"people-home:{normalized_uid or ''}:{limit}"
    payload, query_count, cache_status = _get_or_build_cached_payload(
        cache_key,
        ttl_seconds=_PEOPLE_HOME_CACHE_TTL_SECONDS,
        builder=lambda: show_reads_repo.get_people_home(limit, firebase_uid=normalized_uid),
    )
    _log_read(
        "people-home",
        query_count=query_count,
        payload=payload,
        cache_status=cache_status,
        started_at=started_at,
    )
    return payload


@router.get("/shows/{show_id}")
def get_admin_show(show_id: str, _: InternalAdminUser = None) -> dict[str, Any]:
    started_at = time.perf_counter()
    cache_key = f"show:{show_id}"

    def _build_show_detail() -> tuple[dict[str, Any], int]:
        show, query_count = show_reads_repo.get_show_detail(show_id)
        if show is None:
            raise HTTPException(status_code=404, detail="Show not found")
        return {"show": show}, query_count

    payload, query_count, cache_status = _get_or_build_cached_payload(
        cache_key,
        ttl_seconds=_SHOW_DETAIL_CACHE_TTL_SECONDS,
        builder=_build_show_detail,
    )
    _log_read("detail", query_count=query_count, payload=payload, cache_status=cache_status, started_at=started_at)
    return payload


@router.get("/shows/{show_id}/assets")
def get_admin_show_assets(
    show_id: str,
    limit: int = Query(200, ge=1, le=500),
    offset: int = Query(0, ge=0),
    cursor: str | None = Query(default=None),
    full: bool = Query(False),
    sources: str | None = Query(None),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    normalized_sources = ",".join(value.strip().lower() for value in (sources or "").split(",") if value.strip())
    requested_offset = 0 if full else (_decode_asset_cursor(cursor) if cursor else offset)
    page_limit = min(limit, 500)
    request_limit = 5001 if full else page_limit + 1
    request_offset = requested_offset
    cache_key = f"show-assets:{show_id}:{request_limit}:{request_offset}:{1 if full else 0}:{normalized_sources}"

    def _build_show_assets_payload() -> tuple[dict[str, Any], int]:
        assets, query_count = show_reads_repo.get_show_assets(
            show_id,
            limit=request_limit,
            offset=request_offset,
            sources=[value for value in normalized_sources.split(",") if value] if normalized_sources else None,
            full=full,
        )
        visible_assets = assets[:5000] if full else assets[:page_limit]
        has_more = False if full else len(assets) > page_limit
        truncated = full and len(assets) > 5000
        return (
            {
                "assets": visible_assets,
                "pagination": _build_asset_pagination(
                    limit=5000 if full else page_limit,
                    offset=request_offset,
                    count=len(visible_assets),
                    has_more=has_more,
                    full=full,
                    truncated=truncated,
                ),
            },
            query_count,
        )

    payload, query_count, cache_status = _get_or_build_cached_payload(
        cache_key,
        ttl_seconds=_SHOW_ASSETS_CACHE_TTL_SECONDS,
        builder=_build_show_assets_payload,
    )
    _log_read(
        "show-assets",
        query_count=query_count,
        payload=payload,
        cache_status=cache_status,
        started_at=started_at,
    )
    return payload


@router.get("/shows/{show_id}/seasons")
def list_admin_show_seasons(
    show_id: str,
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    include_episode_signal: bool = Query(False),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    cache_key = f"show-seasons:{show_id}:{limit}:{offset}:{1 if include_episode_signal else 0}"

    def _build_show_seasons_payload() -> tuple[dict[str, Any], int]:
        seasons, query_count = show_reads_repo.get_show_seasons(
            show_id,
            limit=limit,
            offset=offset,
            include_episode_signal=include_episode_signal,
        )
        return (
            {
                "seasons": seasons,
                "pagination": {
                    "limit": limit,
                    "offset": offset,
                    "count": len(seasons),
                },
            },
            query_count,
        )

    payload, query_count, cache_status = _get_or_build_cached_payload(
        cache_key,
        ttl_seconds=_SHOW_SEASONS_CACHE_TTL_SECONDS,
        builder=_build_show_seasons_payload,
    )
    _log_read("seasons", query_count=query_count, payload=payload, cache_status=cache_status, started_at=started_at)
    return payload


@router.get("/shows/{show_id}/seasons/{season_number}/assets")
def get_admin_show_season_assets(
    show_id: str,
    season_number: int,
    limit: int = Query(200, ge=1, le=500),
    offset: int = Query(0, ge=0),
    cursor: str | None = Query(default=None),
    full: bool = Query(False),
    sources: str | None = Query(None),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    normalized_sources = ",".join(value.strip().lower() for value in (sources or "").split(",") if value.strip())
    requested_offset = 0 if full else (_decode_asset_cursor(cursor) if cursor else offset)
    page_limit = min(limit, 500)
    request_limit = 5001 if full else page_limit + 1
    request_offset = requested_offset
    cache_key = (
        f"season-assets:{show_id}:{season_number}:{request_limit}:{request_offset}:"
        f"{1 if full else 0}:{normalized_sources}"
    )

    def _build_season_assets_payload() -> tuple[dict[str, Any], int]:
        assets, query_count = show_reads_repo.get_show_season_assets(
            show_id,
            season_number,
            limit=request_limit,
            offset=request_offset,
            sources=[value for value in normalized_sources.split(",") if value] if normalized_sources else None,
            full=full,
        )
        visible_assets = assets[:5000] if full else assets[:page_limit]
        has_more = False if full else len(assets) > page_limit
        truncated = full and len(assets) > 5000
        return (
            {
                "assets": visible_assets,
                "pagination": _build_asset_pagination(
                    limit=5000 if full else page_limit,
                    offset=request_offset,
                    count=len(visible_assets),
                    has_more=has_more,
                    full=full,
                    truncated=truncated,
                ),
            },
            query_count,
        )

    payload, query_count, cache_status = _get_or_build_cached_payload(
        cache_key,
        ttl_seconds=_SEASON_ASSETS_CACHE_TTL_SECONDS,
        builder=_build_season_assets_payload,
    )
    _log_read(
        "season-assets",
        query_count=query_count,
        payload=payload,
        cache_status=cache_status,
        started_at=started_at,
    )
    return payload


@router.get("/seasons/{season_id}/episodes")
def list_admin_season_episodes(
    season_id: str,
    limit: int = Query(20, ge=1, le=250),
    offset: int = Query(0, ge=0),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    cache_key = f"season-episodes:{season_id}:{limit}:{offset}"

    def _build_season_episodes_payload() -> tuple[dict[str, Any], int]:
        episodes, query_count = show_reads_repo.get_season_episodes(season_id, limit=limit, offset=offset)
        return (
            {
                "episodes": episodes,
                "pagination": {
                    "limit": limit,
                    "offset": offset,
                    "count": len(episodes),
                },
            },
            query_count,
        )

    payload, query_count, cache_status = _get_or_build_cached_payload(
        cache_key,
        ttl_seconds=_SEASON_EPISODES_CACHE_TTL_SECONDS,
        builder=_build_season_episodes_payload,
    )
    _log_read("episodes", query_count=query_count, payload=payload, cache_status=cache_status, started_at=started_at)
    return payload


@router.get("/seasons/{season_id}/backdrops/unassigned")
def get_admin_unassigned_season_backdrops(
    season_id: str,
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    cache_key = f"season-unassigned-backdrops:{season_id}"

    def _build_unassigned_backdrops() -> tuple[dict[str, Any], int]:
        payload, query_count = show_reads_repo.get_unassigned_season_backdrops(season_id)
        if payload is None:
            raise HTTPException(status_code=404, detail="Season not found")
        return payload, query_count

    payload, query_count, cache_status = _get_or_build_cached_payload(
        cache_key,
        ttl_seconds=_SEASON_BACKDROPS_CACHE_TTL_SECONDS,
        builder=_build_unassigned_backdrops,
    )
    _log_read(
        "season-unassigned-backdrops",
        query_count=query_count,
        payload=payload,
        cache_status=cache_status,
        started_at=started_at,
    )
    return payload


@router.post("/seasons/{season_id}/backdrops/assign")
def assign_admin_season_backdrops(
    season_id: str,
    body: dict[str, Any],
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    requested_ids = body.get("media_asset_ids")
    media_asset_ids = (
        [value for value in requested_ids if isinstance(value, str)] if isinstance(requested_ids, list) else []
    )
    payload, query_count, show_id = show_reads_repo.assign_season_backdrops(season_id, media_asset_ids)
    if payload is None:
        raise HTTPException(status_code=404, detail="Season not found")
    if show_id:
        invalidate_show_read_cache(show_id=show_id)
    logger.info(
        "[admin-show-read] route=season-assign-backdrops query_count=%s show_id=%s season_id=%s assigned=%s",
        query_count,
        show_id,
        season_id,
        payload.get("assigned"),
    )
    return payload


@router.get("/shows/{show_id}/cast")
def get_admin_show_cast(
    show_id: str,
    limit: int = Query(20, ge=1, le=500),
    offset: int = Query(0, ge=0),
    min_episodes: int | None = Query(None, ge=0, alias="minEpisodes"),
    exclude_zero_episode_members: bool = Query(False),
    require_image: bool = Query(False, alias="requireImage"),
    roster_mode: str = Query("episode_evidence"),
    photo_fallback: str = Query("none"),
    include_photos: bool = Query(True),
    eligibility_mode: str = Query("default"),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    cache_key = (
        f"show-cast:{show_id}:{limit}:{offset}:{min_episodes if min_episodes is not None else ''}:"
        f"{1 if exclude_zero_episode_members else 0}:{1 if require_image else 0}:"
        f"{roster_mode}:{photo_fallback}:{eligibility_mode}:"
        f"{1 if include_photos else 0}"
    )

    payload, query_count, cache_status = _get_or_build_cached_payload(
        cache_key,
        ttl_seconds=_SHOW_CAST_CACHE_TTL_SECONDS,
        builder=lambda: show_reads_repo.get_show_cast(
            show_id,
            limit=limit,
            offset=offset,
            min_episodes=min_episodes,
            has_explicit_min_episodes=min_episodes is not None,
            exclude_zero_episode_members=exclude_zero_episode_members,
            require_image=require_image,
            roster_mode=roster_mode,
            photo_fallback=photo_fallback,
            include_photos=include_photos,
            eligibility_mode=eligibility_mode,
        ),
    )
    _log_read("show-cast", query_count=query_count, payload=payload, cache_status=cache_status, started_at=started_at)
    return payload


@router.get("/shows/{show_id}/credits")
def get_admin_show_credits(
    show_id: str,
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    cache_key = f"show-credits:{show_id}"
    payload, query_count, cache_status = _get_or_build_cached_payload(
        cache_key,
        ttl_seconds=_SHOW_CREDITS_CACHE_TTL_SECONDS,
        builder=lambda: show_reads_repo.get_show_credits(show_id),
    )
    _log_read(
        "show-credits",
        query_count=query_count,
        payload=payload,
        cache_status=cache_status,
        started_at=started_at,
    )
    return payload


@router.get("/shows/{show_id}/seasons/{season_number}/cast")
def get_admin_season_cast(
    show_id: str,
    season_number: int,
    limit: int = Query(500, ge=1, le=500),
    offset: int = Query(0, ge=0),
    include_archive_only: bool = Query(False),
    photo_fallback: str = Query("none"),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    cache_key = (
        f"season-cast:{show_id}:{season_number}:{limit}:{offset}:{1 if include_archive_only else 0}:{photo_fallback}"
    )

    payload, query_count, cache_status = _get_or_build_cached_payload(
        cache_key,
        ttl_seconds=_SEASON_CAST_CACHE_TTL_SECONDS,
        builder=lambda: show_reads_repo.get_season_cast(
            show_id,
            season_number,
            limit=limit,
            offset=offset,
            include_archive_only=include_archive_only,
            photo_fallback=photo_fallback,
        ),
    )
    _log_read(
        "season-cast",
        query_count=query_count,
        payload=payload,
        cache_status=cache_status,
        started_at=started_at,
    )
    return payload


@router.post("/shows/{show_id}/cache/invalidate")
def invalidate_admin_show_read_cache(show_id: str, _: InternalAdminUser = None) -> dict[str, bool]:
    invalidate_show_read_cache(show_id=show_id)
    logger.info("[admin-show-read] route=invalidate-cache show_id=%s", show_id)
    return {"success": True}
