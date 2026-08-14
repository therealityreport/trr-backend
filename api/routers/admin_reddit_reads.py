from __future__ import annotations

import json
import logging
import os
import re
import time
from threading import Event, Lock
from typing import Any, cast
from urllib.parse import urlparse

from fastapi import APIRouter, Header, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from api.auth import InternalAdminUser
from trr_backend.repositories import admin_reddit_reads as reddit_reads_repo
from trr_backend.repositories import admin_reddit_sources as reddit_sources_repo

logger = logging.getLogger(__name__)
_INTERNAL_ADMIN_DEFAULT = cast(InternalAdminUser, None)

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


def _cache_clear() -> None:
    with _CACHE_LOCK:
        _CACHE.clear()


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


def _required_string(body: dict[str, Any], key: str) -> str:
    value = body.get(key)
    if not isinstance(value, str) or not value.strip():
        raise HTTPException(status_code=400, detail=f"{key} is required and must be a string")
    return value.strip()


def _optional_string_array(body: dict[str, Any], key: str) -> list[str] | None:
    if key not in body:
        return None
    value = body.get(key)
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise HTTPException(status_code=400, detail=f"{key} must be an array of strings")
    return value


def _validate_optional_object(body: dict[str, Any], key: str) -> dict[str, Any] | None:
    if key not in body:
        return None
    value = body.get(key)
    if not isinstance(value, dict):
        raise HTTPException(status_code=400, detail=f"{key} must be an object")
    return value


def _actor_uid(
    admin: dict[str, Any] | None,
    explicit_uid: str | None,
    explicit_email: str | None,
    explicit_id: str | None,
) -> str:
    for value in (
        explicit_uid,
        explicit_email,
        explicit_id,
        (admin or {}).get("admin_uid"),
        (admin or {}).get("admin_email"),
        (admin or {}).get("email"),
        (admin or {}).get("id"),
    ):
        normalized = str(value or "").strip()
        if normalized:
            return normalized
    return "admin"


def _is_unique_violation(error: Exception) -> bool:
    return getattr(error, "pgcode", None) == "23505" or getattr(error, "code", None) == "23505"


def _is_reddit_host(hostname: str) -> bool:
    host = hostname.lower()
    return host in {"reddit.com", "redd.it"} or host.endswith(".reddit.com") or host.endswith(".redd.it")


def _normalize_reddit_url(value: str, *, field_name: str) -> str:
    normalized = str(value or "").strip()
    try:
        parsed = urlparse(normalized)
        if not parsed.scheme or not parsed.netloc or not _is_reddit_host(parsed.hostname or ""):
            raise ValueError
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"{field_name} must be a valid Reddit URL") from exc
    return normalized


def _normalize_reddit_permalink(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        return None
    normalized = value.strip()
    if re.match(r"^https?://", normalized, re.I):
        return _normalize_reddit_url(normalized, field_name="permalink")
    if normalized.startswith("/"):
        return _normalize_reddit_url(f"https://www.reddit.com{normalized}", field_name="permalink")
    return _normalize_reddit_url(f"https://www.reddit.com/{normalized}", field_name="permalink")


def _optional_nonnegative_number(body: dict[str, Any], key: str) -> int | None:
    if key not in body:
        return None
    value = body.get(key)
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return 0
    return max(0, int(value))


def _require_season_belongs_to_show(season_id: str, show_id: str, detail: str) -> None:
    season_show_id, _query_count = reddit_sources_repo.get_season_show_id(season_id)
    if season_show_id != show_id:
        raise HTTPException(status_code=400, detail=detail)


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
    _: InternalAdminUser = _INTERNAL_ADMIN_DEFAULT,
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


@router.post("/communities", status_code=201)
def create_community(
    body: dict[str, Any],
    x_trr_admin_user_uid: str | None = Header(default=None, alias="X-TRR-Admin-User-Uid"),
    x_trr_admin_user_email: str | None = Header(default=None, alias="X-TRR-Admin-User-Email"),
    x_trr_admin_user_id: str | None = Header(default=None, alias="X-TRR-Admin-User-Id"),
    admin: InternalAdminUser = _INTERNAL_ADMIN_DEFAULT,
) -> dict[str, Any]:
    trr_show_id = _required_string(body, "trr_show_id")
    _validate_uuid(trr_show_id, "trr_show_id")
    _required_string(body, "trr_show_name")
    _required_string(body, "subreddit")
    for key in ("network_focus_targets", "franchise_focus_targets", "episode_title_patterns"):
        _optional_string_array(body, key)
    if "episode_required_flairs" in body:
        raise HTTPException(
            status_code=400,
            detail="episode_required_flairs is no longer supported; use analysis_all_flairs",
        )

    try:
        community, _query_count = reddit_sources_repo.create_reddit_community(
            payload=body,
            actor_uid=_actor_uid(admin or {}, x_trr_admin_user_uid, x_trr_admin_user_email, x_trr_admin_user_id),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        if _is_unique_violation(exc):
            raise HTTPException(status_code=409, detail="Community already exists for this show") from exc
        raise

    _cache_clear()
    return {"community": community}


@router.get("/communities/{community_id}")
def get_community(community_id: str, _: InternalAdminUser = _INTERNAL_ADMIN_DEFAULT) -> dict[str, Any]:
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


@router.patch("/communities/{community_id}")
def update_community(
    community_id: str,
    body: dict[str, Any],
    _: InternalAdminUser = _INTERNAL_ADMIN_DEFAULT,
) -> dict[str, Any]:
    _validate_uuid(community_id, "community_id")
    for key in (
        "analysis_flairs",
        "analysis_all_flairs",
        "network_focus_targets",
        "franchise_focus_targets",
        "episode_title_patterns",
    ):
        _optional_string_array(body, key)
    for key in ("post_flair_categories", "post_flair_assignments"):
        _validate_optional_object(body, key)
    if "episode_required_flairs" in body:
        raise HTTPException(
            status_code=400,
            detail="episode_required_flairs is no longer supported; use analysis_all_flairs",
        )
    if "subreddit" in body and not isinstance(body.get("subreddit"), str):
        raise HTTPException(status_code=400, detail="subreddit must be a string")

    try:
        community, _query_count = reddit_sources_repo.update_reddit_community(
            community_id=community_id,
            payload=body,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        if _is_unique_violation(exc):
            raise HTTPException(status_code=409, detail="Community already exists for this show") from exc
        raise

    if community is None:
        raise HTTPException(status_code=404, detail="Community not found")
    _cache_clear()
    return {"community": community}


@router.delete("/communities/{community_id}")
def delete_community(community_id: str, _: InternalAdminUser = _INTERNAL_ADMIN_DEFAULT) -> dict[str, bool]:
    _validate_uuid(community_id, "community_id")
    deleted, _query_count = reddit_sources_repo.delete_reddit_community(community_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Community not found")
    _cache_clear()
    return {"success": True}


@router.post("/communities/{community_id}/post-flairs")
@router.patch("/communities/{community_id}/post-flairs")
def update_community_post_flairs(
    community_id: str,
    body: dict[str, Any],
    _: InternalAdminUser = _INTERNAL_ADMIN_DEFAULT,
) -> dict[str, Any]:
    _validate_uuid(community_id, "community_id")
    post_flairs = _optional_string_array(body, "post_flairs")
    if post_flairs is None:
        raise HTTPException(status_code=400, detail="post_flairs must be an array of strings")
    post_flairs_updated_at = body.get("post_flairs_updated_at")
    if post_flairs_updated_at is not None and not isinstance(post_flairs_updated_at, str):
        raise HTTPException(status_code=400, detail="post_flairs_updated_at must be a string")
    community, _query_count = reddit_sources_repo.update_reddit_community_post_flairs(
        community_id=community_id,
        post_flairs=post_flairs,
        post_flairs_updated_at=post_flairs_updated_at,
    )
    if community is None:
        raise HTTPException(status_code=404, detail="Community not found")
    _cache_clear()
    return {
        "community": community,
        "flairs": community.get("post_flairs") if isinstance(community.get("post_flairs"), list) else post_flairs,
    }


@router.get("/threads")
def list_threads(
    community_id: str | None = Query(default=None),
    trr_show_id: str | None = Query(default=None),
    trr_season_id: str | None = Query(default=None),
    include_global_threads_for_season: bool = Query(default=True),
    _: InternalAdminUser = _INTERNAL_ADMIN_DEFAULT,
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


@router.post("/threads", status_code=201)
def create_thread(
    body: dict[str, Any],
    x_trr_admin_user_uid: str | None = Header(default=None, alias="X-TRR-Admin-User-Uid"),
    x_trr_admin_user_email: str | None = Header(default=None, alias="X-TRR-Admin-User-Email"),
    x_trr_admin_user_id: str | None = Header(default=None, alias="X-TRR-Admin-User-Id"),
    admin: InternalAdminUser = _INTERNAL_ADMIN_DEFAULT,
) -> dict[str, Any]:
    community_id = _required_string(body, "community_id")
    trr_show_id = _required_string(body, "trr_show_id")
    _required_string(body, "trr_show_name")
    reddit_post_id = _required_string(body, "reddit_post_id")
    title = _required_string(body, "title")
    url = _required_string(body, "url")
    _validate_uuid(community_id, "community_id")
    _validate_uuid(trr_show_id, "trr_show_id")
    if body.get("trr_season_id") is not None:
        if not isinstance(body.get("trr_season_id"), str):
            raise HTTPException(status_code=400, detail="trr_season_id must be a valid UUID")
        _validate_uuid(str(body.get("trr_season_id")), "trr_season_id")
    normalized_url = _normalize_reddit_url(url, field_name="url")
    normalized_permalink = _normalize_reddit_permalink(body.get("permalink"))
    if "source_kind" in body and body.get("source_kind") not in {"manual", "episode_discussion"}:
        raise HTTPException(status_code=400, detail="source_kind must be one of: manual, episode_discussion")

    community, _query_count = reddit_reads_repo.get_reddit_community_by_id(community_id)
    if community is None:
        raise HTTPException(status_code=404, detail="Community not found")
    if str(community.get("trr_show_id") or "") != trr_show_id:
        raise HTTPException(status_code=400, detail="trr_show_id does not match selected community")
    if isinstance(body.get("trr_season_id"), str):
        _require_season_belongs_to_show(
            str(body["trr_season_id"]),
            trr_show_id,
            "trr_season_id must belong to trr_show_id",
        )

    payload = {
        **body,
        "community_id": community_id,
        "trr_show_id": trr_show_id,
        "trr_show_name": community.get("trr_show_name") or body.get("trr_show_name"),
        "trr_season_id": body.get("trr_season_id") if isinstance(body.get("trr_season_id"), str) else None,
        "reddit_post_id": reddit_post_id,
        "title": title,
        "url": normalized_url,
        "permalink": normalized_permalink,
        "score": _optional_nonnegative_number(body, "score") if "score" in body else 0,
        "num_comments": _optional_nonnegative_number(body, "num_comments") if "num_comments" in body else 0,
        "author": body.get("author") if isinstance(body.get("author"), str) else None,
        "posted_at": body.get("posted_at") if isinstance(body.get("posted_at"), str) else None,
        "notes": body.get("notes") if isinstance(body.get("notes"), str) else None,
    }
    try:
        thread, _query_count = reddit_sources_repo.create_reddit_thread(
            payload=payload,
            actor_uid=_actor_uid(admin or {}, x_trr_admin_user_uid, x_trr_admin_user_email, x_trr_admin_user_id),
        )
    except ValueError as exc:
        if str(exc) == "Thread already exists in another community for this show":
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    _cache_clear()
    return {"thread": thread}


@router.get("/threads/{thread_id}")
def get_thread(thread_id: str, _: InternalAdminUser = _INTERNAL_ADMIN_DEFAULT) -> dict[str, Any]:
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


@router.patch("/threads/{thread_id}")
def update_thread(
    thread_id: str,
    body: dict[str, Any],
    _: InternalAdminUser = _INTERNAL_ADMIN_DEFAULT,
) -> dict[str, Any]:
    _validate_uuid(thread_id, "thread_id")
    if "community_id" in body:
        if not isinstance(body.get("community_id"), str):
            raise HTTPException(status_code=400, detail="community_id must be a valid UUID")
        _validate_uuid(str(body.get("community_id")), "community_id")
    if "trr_season_id" in body and body.get("trr_season_id") is not None:
        if not isinstance(body.get("trr_season_id"), str):
            raise HTTPException(status_code=400, detail="trr_season_id must be a valid UUID")
        _validate_uuid(str(body.get("trr_season_id")), "trr_season_id")
    if "url" in body:
        if not isinstance(body.get("url"), str):
            raise HTTPException(status_code=400, detail="url must be a string")
        body["url"] = _normalize_reddit_url(str(body["url"]), field_name="url")
    if "permalink" in body:
        body["permalink"] = _normalize_reddit_permalink(body.get("permalink"))
    if "source_kind" in body and body.get("source_kind") not in {"manual", "episode_discussion"}:
        raise HTTPException(status_code=400, detail="source_kind must be one of: manual, episode_discussion")

    existing_thread, _query_count = reddit_reads_repo.get_reddit_thread_by_id(thread_id)
    if existing_thread is None:
        raise HTTPException(status_code=404, detail="Thread not found")

    payload = dict(body)
    next_show_id = str(existing_thread.get("trr_show_id") or "")
    if isinstance(body.get("community_id"), str):
        target_community, _query_count = reddit_reads_repo.get_reddit_community_by_id(str(body["community_id"]))
        if target_community is None:
            raise HTTPException(status_code=404, detail="Target community not found")
        if str(target_community.get("trr_show_id") or "") != str(existing_thread.get("trr_show_id") or ""):
            raise HTTPException(
                status_code=400,
                detail="Cannot reassign thread to a community belonging to a different show",
            )
        next_show_id = str(target_community.get("trr_show_id") or "")
        payload["trr_show_id"] = target_community.get("trr_show_id")
        payload["trr_show_name"] = target_community.get("trr_show_name")

    if isinstance(body.get("trr_season_id"), str):
        _require_season_belongs_to_show(
            str(body["trr_season_id"]),
            next_show_id,
            "trr_season_id must belong to the thread show",
        )
    for key in ("score", "num_comments"):
        number_value = _optional_nonnegative_number(body, key)
        if number_value is not None:
            payload[key] = number_value

    try:
        thread, _query_count = reddit_sources_repo.update_reddit_thread(thread_id=thread_id, payload=payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if thread is None:
        raise HTTPException(status_code=404, detail="Thread not found")
    _cache_clear()
    return {"thread": thread}


@router.delete("/threads/{thread_id}")
def delete_thread(thread_id: str, _: InternalAdminUser = _INTERNAL_ADMIN_DEFAULT) -> dict[str, bool]:
    _validate_uuid(thread_id, "thread_id")
    deleted, _query_count = reddit_sources_repo.delete_reddit_thread(thread_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Thread not found")
    _cache_clear()
    return {"success": True}


@router.get("/communities/{community_id}/stored-post-counts")
def get_stored_post_counts(
    community_id: str,
    season_id: str = Query(...),
    _: InternalAdminUser = _INTERNAL_ADMIN_DEFAULT,
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
    _: InternalAdminUser = _INTERNAL_ADMIN_DEFAULT,
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
    _: InternalAdminUser = _INTERNAL_ADMIN_DEFAULT,
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
    _: InternalAdminUser = _INTERNAL_ADMIN_DEFAULT,
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
    _: InternalAdminUser = _INTERNAL_ADMIN_DEFAULT,
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
    _: InternalAdminUser = _INTERNAL_ADMIN_DEFAULT,
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
            return cast(dict[str, Any], JSONResponse({"error": str(exc.detail)}, status_code=404))
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
