from __future__ import annotations

import json
import logging
import os
import re
import time
from threading import Event, Lock
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from api.auth import InternalAdminUser
from trr_backend.repositories import admin_reddit_reads as reddit_reads_repo

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/reddit", tags=["admin-reddit-reads"])
UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)

_CACHE_LOCK = Lock()
_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_INFLIGHT_LOCK = Lock()
_INFLIGHT: dict[str, Event] = {}

_COMMUNITIES_CACHE_TTL_SECONDS = max(
    int(os.getenv("TRR_ADMIN_REDDIT_COMMUNITIES_BACKEND_CACHE_TTL_SECONDS", "15")),
    1,
)
_COMMUNITY_DETAIL_CACHE_TTL_SECONDS = max(
    int(os.getenv("TRR_ADMIN_REDDIT_COMMUNITY_DETAIL_BACKEND_CACHE_TTL_SECONDS", "30")),
    1,
)
_THREADS_CACHE_TTL_SECONDS = max(int(os.getenv("TRR_ADMIN_REDDIT_THREADS_BACKEND_CACHE_TTL_SECONDS", "15")), 1)
_THREAD_DETAIL_CACHE_TTL_SECONDS = max(
    int(os.getenv("TRR_ADMIN_REDDIT_THREAD_DETAIL_BACKEND_CACHE_TTL_SECONDS", "30")),
    1,
)
_STORED_POST_COUNTS_CACHE_TTL_SECONDS = max(
    int(os.getenv("TRR_ADMIN_REDDIT_STORED_POST_COUNTS_BACKEND_CACHE_TTL_SECONDS", "20")),
    1,
)
_ANALYTICS_SUMMARY_CACHE_TTL_SECONDS = max(
    int(os.getenv("TRR_ADMIN_REDDIT_ANALYTICS_SUMMARY_BACKEND_CACHE_TTL_SECONDS", "20")),
    1,
)
_STORED_POSTS_CACHE_TTL_SECONDS = max(
    int(os.getenv("TRR_ADMIN_REDDIT_STORED_POSTS_BACKEND_CACHE_TTL_SECONDS", "20")),
    1,
)
_ANALYTICS_POSTS_CACHE_TTL_SECONDS = max(
    int(os.getenv("TRR_ADMIN_REDDIT_ANALYTICS_POSTS_BACKEND_CACHE_TTL_SECONDS", "20")),
    1,
)
_POST_DETAIL_CACHE_TTL_SECONDS = max(
    int(os.getenv("TRR_ADMIN_REDDIT_POST_DETAIL_BACKEND_CACHE_TTL_SECONDS", "30")),
    1,
)
_RESOLVE_CACHE_TTL_SECONDS = max(int(os.getenv("TRR_ADMIN_REDDIT_RESOLVE_BACKEND_CACHE_TTL_SECONDS", "60")), 1)


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


def _payload_size_bytes(payload: dict[str, Any]) -> int:
    return len(json.dumps(jsonable_encoder(payload), separators=(",", ":"), default=str).encode("utf-8"))


def _log_read(route: str, *, query_count: int, payload: dict[str, Any], cache_status: str, started_at: float) -> None:
    logger.info(
        "[admin-reddit-read] route=%s latency_ms=%.1f payload_bytes=%s query_count=%s cache=%s",
        route,
        (time.perf_counter() - started_at) * 1000.0,
        _payload_size_bytes(payload),
        query_count,
        cache_status,
    )


def _scope_and_season(request_scope: str | None, season_id: str | None) -> tuple[str, str | None]:
    scope = "all" if (request_scope or "").strip().lower() == "all" else "season"
    return scope, season_id if season_id and scope == "season" else season_id


def _validate_uuid(value: str | None, field_name: str) -> None:
    if not value or not UUID_RE.fullmatch(value):
        raise HTTPException(status_code=400, detail=f"{field_name} must be a valid UUID")


def _normalize_detail_part(value: str | None) -> str | None:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return None
    if not re.fullmatch(r"[a-z0-9-]+", normalized):
        return None
    return normalized


def _parse_comments_limit(value: str | None) -> int | None:
    if not value:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    if parsed <= 0:
        return None
    return parsed


@router.get("/communities")
def list_communities(
    trr_show_id: str | None = Query(default=None),
    trr_season_id: str | None = Query(default=None),
    include_inactive: bool = Query(default=False),
    include_global_threads_for_season: bool = Query(default=True),
    include_assigned_threads: bool = Query(default=False),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    if trr_show_id is not None:
        _validate_uuid(trr_show_id, "trr_show_id")
    if trr_season_id is not None:
        _validate_uuid(trr_season_id, "trr_season_id")
    cache_key = (
        f"communities:{trr_show_id or ''}:{trr_season_id or ''}:{1 if include_inactive else 0}:"
        f"{1 if include_global_threads_for_season else 0}:{1 if include_assigned_threads else 0}"
    )

    def _build() -> tuple[dict[str, Any], int]:
        payload, query_count = reddit_reads_repo.list_reddit_communities(
            trr_show_id=trr_show_id,
            include_inactive=include_inactive,
            trr_season_id=trr_season_id,
            include_global_threads_for_season=include_global_threads_for_season,
            include_assigned_threads=include_assigned_threads,
        )
        return payload, query_count

    payload, query_count, cache_status = _get_or_build_cached_payload(
        cache_key,
        ttl_seconds=_COMMUNITIES_CACHE_TTL_SECONDS,
        builder=_build,
    )
    _log_read("communities", query_count=query_count, payload=payload, cache_status=cache_status, started_at=started_at)
    return payload


@router.get("/communities/{community_id}")
def get_community(community_id: str, _: InternalAdminUser = None) -> dict[str, Any]:
    started_at = time.perf_counter()
    _validate_uuid(community_id, "community_id")
    cache_key = f"community:{community_id}"

    def _build() -> tuple[dict[str, Any], int]:
        community, query_count = reddit_reads_repo.get_reddit_community_by_id(community_id)
        if community is None:
            raise HTTPException(status_code=404, detail="Community not found")
        return {"community": community}, query_count

    payload, query_count, cache_status = _get_or_build_cached_payload(
        cache_key,
        ttl_seconds=_COMMUNITY_DETAIL_CACHE_TTL_SECONDS,
        builder=_build,
    )
    _log_read("community", query_count=query_count, payload=payload, cache_status=cache_status, started_at=started_at)
    return payload


@router.get("/threads")
def list_threads(
    community_id: str | None = Query(default=None),
    trr_show_id: str | None = Query(default=None),
    trr_season_id: str | None = Query(default=None),
    include_global_threads_for_season: bool = Query(default=True),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    if community_id is not None:
        _validate_uuid(community_id, "community_id")
    if trr_show_id is not None:
        _validate_uuid(trr_show_id, "trr_show_id")
    if trr_season_id is not None:
        _validate_uuid(trr_season_id, "trr_season_id")
    cache_key = (
        f"threads:{community_id or ''}:{trr_show_id or ''}:{trr_season_id or ''}:"
        f"{1 if include_global_threads_for_season else 0}"
    )

    def _build() -> tuple[dict[str, Any], int]:
        return reddit_reads_repo.list_reddit_threads(
            community_id=community_id,
            trr_show_id=trr_show_id,
            trr_season_id=trr_season_id,
            include_global_threads_for_season=include_global_threads_for_season,
        )

    payload, query_count, cache_status = _get_or_build_cached_payload(
        cache_key,
        ttl_seconds=_THREADS_CACHE_TTL_SECONDS,
        builder=_build,
    )
    _log_read("threads", query_count=query_count, payload=payload, cache_status=cache_status, started_at=started_at)
    return payload


@router.get("/threads/{thread_id}")
def get_thread(thread_id: str, _: InternalAdminUser = None) -> dict[str, Any]:
    started_at = time.perf_counter()
    _validate_uuid(thread_id, "thread_id")
    cache_key = f"thread:{thread_id}"

    def _build() -> tuple[dict[str, Any], int]:
        thread, query_count = reddit_reads_repo.get_reddit_thread_by_id(thread_id)
        if thread is None:
            raise HTTPException(status_code=404, detail="Thread not found")
        return {"thread": thread}, query_count

    payload, query_count, cache_status = _get_or_build_cached_payload(
        cache_key,
        ttl_seconds=_THREAD_DETAIL_CACHE_TTL_SECONDS,
        builder=_build,
    )
    _log_read("thread", query_count=query_count, payload=payload, cache_status=cache_status, started_at=started_at)
    return payload


@router.get("/communities/{community_id}/stored-post-counts")
def get_stored_post_counts(
    community_id: str,
    season_id: str = Query(...),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    _validate_uuid(community_id, "community_id")
    _validate_uuid(season_id, "season_id")
    cache_key = f"stored-post-counts:{community_id}:{season_id}"

    def _build() -> tuple[dict[str, Any], int]:
        return reddit_reads_repo.get_stored_post_counts_by_community_and_season(community_id, season_id)

    payload, query_count, cache_status = _get_or_build_cached_payload(
        cache_key,
        ttl_seconds=_STORED_POST_COUNTS_CACHE_TTL_SECONDS,
        builder=_build,
    )
    _log_read(
        "stored-post-counts",
        query_count=query_count,
        payload=payload,
        cache_status=cache_status,
        started_at=started_at,
    )
    return payload


@router.get("/analytics/community/{community_id}/summary")
def get_analytics_summary(
    community_id: str,
    scope: str = Query(default="season"),
    season_id: str | None = Query(default=None),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    _validate_uuid(community_id, "community_id")
    normalized_scope, normalized_season_id = _scope_and_season(scope, season_id)
    if normalized_scope == "season" and not normalized_season_id:
        raise HTTPException(status_code=400, detail="season_id is required when scope=season")
    if normalized_season_id is not None:
        _validate_uuid(normalized_season_id, "season_id")
    cache_key = f"analytics-summary:{community_id}:{normalized_scope}:{normalized_season_id or ''}"

    def _build() -> tuple[dict[str, Any], int]:
        return reddit_reads_repo.get_reddit_community_analytics_summary(
            community_id=community_id,
            scope=normalized_scope,
            season_id=normalized_season_id,
        )

    payload, query_count, cache_status = _get_or_build_cached_payload(
        cache_key,
        ttl_seconds=_ANALYTICS_SUMMARY_CACHE_TTL_SECONDS,
        builder=_build,
    )
    _log_read(
        "analytics-summary",
        query_count=query_count,
        payload=payload,
        cache_status=cache_status,
        started_at=started_at,
    )
    return payload


@router.get("/communities/{community_id}/stored-posts")
def get_stored_posts(
    community_id: str,
    season_id: str = Query(...),
    container_key: str = Query(...),
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=200, ge=1, le=200),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    _validate_uuid(community_id, "community_id")
    _validate_uuid(season_id, "season_id")
    if not container_key.strip():
        raise HTTPException(status_code=400, detail="container_key is required")
    cache_key = f"stored-posts:{community_id}:{season_id}:{container_key}:{page}:{per_page}"

    def _build() -> tuple[dict[str, Any], int]:
        return reddit_reads_repo.get_stored_window_posts_by_community_and_season(
            community_id,
            season_id,
            container_key,
            page=page,
            per_page=per_page,
        )

    payload, query_count, cache_status = _get_or_build_cached_payload(
        cache_key,
        ttl_seconds=_STORED_POSTS_CACHE_TTL_SECONDS,
        builder=_build,
    )
    _log_read(
        "stored-posts",
        query_count=query_count,
        payload=payload,
        cache_status=cache_status,
        started_at=started_at,
    )
    return payload


@router.get("/analytics/community/{community_id}/posts")
def get_analytics_posts(
    community_id: str,
    scope: str = Query(default="season"),
    season_id: str | None = Query(default=None),
    container_key: str | None = Query(default=None),
    flair_key: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=50, ge=1, le=200),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    _validate_uuid(community_id, "community_id")
    normalized_scope, normalized_season_id = _scope_and_season(scope, season_id)
    if normalized_scope == "season" and not normalized_season_id:
        raise HTTPException(status_code=400, detail="season_id is required when scope=season")
    if normalized_season_id is not None:
        _validate_uuid(normalized_season_id, "season_id")
    cache_key = (
        f"analytics-posts:{community_id}:{normalized_scope}:{normalized_season_id or ''}:{container_key or ''}:"
        f"{flair_key or ''}:{page}:{per_page}"
    )

    def _build() -> tuple[dict[str, Any], int]:
        return reddit_reads_repo.get_reddit_community_analytics_posts(
            community_id=community_id,
            scope=normalized_scope,
            season_id=normalized_season_id,
            container_key=container_key,
            flair_key=flair_key,
            page=page,
            per_page=per_page,
        )

    payload, query_count, cache_status = _get_or_build_cached_payload(
        cache_key,
        ttl_seconds=_ANALYTICS_POSTS_CACHE_TTL_SECONDS,
        builder=_build,
    )
    _log_read(
        "analytics-posts",
        query_count=query_count,
        payload=payload,
        cache_status=cache_status,
        started_at=started_at,
    )
    return payload


@router.get("/communities/{community_id}/posts/resolve")
def resolve_post_detail(
    community_id: str,
    season_id: str = Query(...),
    window_key: str = Query(...),
    slug: str | None = Query(default=None),
    author: str | None = Query(default=None),
    post_id: str | None = Query(default=None),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    _validate_uuid(community_id, "community_id")
    _validate_uuid(season_id, "season_id")
    if not window_key.strip():
        raise HTTPException(status_code=400, detail="window_key is required")
    normalized_slug = _normalize_detail_part(slug)
    normalized_author = _normalize_detail_part(author)
    if not post_id and (not normalized_slug or not normalized_author):
        raise HTTPException(status_code=400, detail="slug and author are required when post_id is omitted")
    cache_key = (
        f"resolve:{community_id}:{season_id}:{window_key}:{normalized_slug or ''}:"
        f"{normalized_author or ''}:{post_id or ''}"
    )

    def _build() -> tuple[dict[str, Any], int]:
        container_key = _resolve_container_key_from_window_token(window_key)
        if not container_key:
            raise HTTPException(status_code=400, detail="window_key is required")
        resolved, query_count = reddit_reads_repo.resolve_reddit_post_detail_by_slug(
            community_id=community_id,
            season_id=season_id,
            container_key=container_key,
            title_slug=normalized_slug,
            author_slug=normalized_author,
            reddit_post_id=post_id,
        )
        if resolved is None:
            raise HTTPException(status_code=404, detail="Post not found for community, season, and window")
        return resolved, query_count

    payload, query_count, cache_status = _get_or_build_cached_payload(
        cache_key,
        ttl_seconds=_RESOLVE_CACHE_TTL_SECONDS,
        builder=_build,
    )
    _log_read("resolve", query_count=query_count, payload=payload, cache_status=cache_status, started_at=started_at)
    return payload


@router.get("/communities/{community_id}/posts/{post_id}/details")
def get_post_detail(
    community_id: str,
    post_id: str,
    season_id: str = Query(...),
    comments_limit: str | None = Query(default=None),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    _validate_uuid(community_id, "communityId")
    _validate_uuid(season_id, "season_id")
    normalized_post_id = str(post_id or "").strip()
    if not normalized_post_id:
        raise HTTPException(status_code=400, detail="postId is required")
    normalized_comments_limit = _parse_comments_limit(comments_limit)
    cache_key = f"post-detail:{community_id}:{season_id}:{normalized_post_id}:{normalized_comments_limit or ''}"

    def _build() -> tuple[dict[str, Any], int]:
        post, query_count = reddit_reads_repo.get_reddit_post_details_by_community_and_season(
            community_id=community_id,
            season_id=season_id,
            reddit_post_id=normalized_post_id,
            comments_limit=normalized_comments_limit,
        )
        if post is None:
            raise HTTPException(status_code=404, detail="Post not found for community and season")
        return {"post": post}, query_count

    try:
        payload, query_count, cache_status = _get_or_build_cached_payload(
            cache_key,
            ttl_seconds=_POST_DETAIL_CACHE_TTL_SECONDS,
            builder=_build,
        )
    except HTTPException as exc:
        if exc.status_code == 404:
            return JSONResponse({"error": str(exc.detail)}, status_code=404)
        raise
    _log_read("post-detail", query_count=query_count, payload=payload, cache_status=cache_status, started_at=started_at)
    return payload


def _resolve_container_key_from_window_token(value: str | None) -> str | None:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return None
    if normalized == "w0" or normalized == "period-preseason":
        return "period-preseason"
    if normalized == "w-postseason" or normalized == "period-postseason":
        return "period-postseason"
    episode_canonical = re.match(r"^e(\d+)$", normalized)
    if episode_canonical:
        return f"episode-{episode_canonical.group(1)}"
    episode_legacy = re.match(r"^w(\d+)$", normalized)
    if episode_legacy:
        return f"episode-{episode_legacy.group(1)}"
    episode_raw = re.match(r"^episode-(\d+)$", normalized)
    if episode_raw:
        return f"episode-{episode_raw.group(1)}"
    return None
