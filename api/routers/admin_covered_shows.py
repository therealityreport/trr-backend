from __future__ import annotations

import json
import logging
import os
import time
from threading import Lock
from typing import Any

from fastapi import APIRouter, Header, HTTPException
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel

from api.auth import InternalAdminUser
from trr_backend.repositories import covered_shows as covered_shows_repo

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/covered-shows", tags=["admin-covered-shows"])

_CACHE_LOCK = Lock()
_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_CACHE_TTL_SECONDS = max(int(os.getenv("TRR_ADMIN_COVERED_SHOWS_BACKEND_CACHE_TTL_SECONDS", "30")), 1)


class CreateCoveredShowRequest(BaseModel):
    trr_show_id: str
    show_name: str


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


def invalidate_covered_shows_cache() -> None:
    with _CACHE_LOCK:
        _CACHE.clear()


def _payload_size_bytes(payload: dict[str, Any]) -> int:
    return len(json.dumps(jsonable_encoder(payload), separators=(",", ":"), default=str).encode("utf-8"))


def _log_read(route: str, *, query_count: int, payload: dict[str, Any], cache_status: str, started_at: float) -> None:
    logger.info(
        "[admin-covered-shows-read] route=%s latency_ms=%.1f payload_bytes=%s query_count=%s cache=%s",
        route,
        (time.perf_counter() - started_at) * 1000.0,
        _payload_size_bytes(payload),
        query_count,
        cache_status,
    )


def _actor_uid(admin: dict[str, Any], explicit_uid: str | None) -> str:
    normalized = str(explicit_uid or "").strip()
    if normalized:
        return normalized
    return str(admin.get("email") or admin.get("id") or "admin")


@router.get("")
def list_covered_shows(_: InternalAdminUser = None) -> dict[str, Any]:
    started_at = time.perf_counter()
    cached = _cache_get("list")
    if cached is not None:
        _log_read("list", query_count=0, payload=cached, cache_status="hit", started_at=started_at)
        return cached

    shows, query_count = covered_shows_repo.list_covered_shows()
    payload = {"shows": shows}
    _cache_set("list", payload)
    _log_read("list", query_count=query_count, payload=payload, cache_status="miss", started_at=started_at)
    return payload


@router.get("/{show_id}")
def get_covered_show(show_id: str, _: InternalAdminUser = None) -> dict[str, Any]:
    started_at = time.perf_counter()
    cache_key = f"show:{show_id}"
    cached = _cache_get(cache_key)
    if cached is not None:
        _log_read("detail", query_count=0, payload=cached, cache_status="hit", started_at=started_at)
        return cached

    show, query_count = covered_shows_repo.get_covered_show(show_id)
    if show is None:
        raise HTTPException(status_code=404, detail="Show not found in covered shows list")
    payload = {"show": show}
    _cache_set(cache_key, payload)
    _log_read("detail", query_count=query_count, payload=payload, cache_status="miss", started_at=started_at)
    return payload


@router.post("")
def create_covered_show(
    body: CreateCoveredShowRequest,
    x_trr_admin_user_uid: str | None = Header(default=None, alias="X-TRR-Admin-User-Uid"),
    admin: InternalAdminUser = None,
) -> dict[str, Any]:
    show_id = str(body.trr_show_id or "").strip()
    show_name = str(body.show_name or "").strip()
    if not show_id:
        raise HTTPException(status_code=400, detail="trr_show_id is required and must be a string")
    if not show_name:
        raise HTTPException(status_code=400, detail="show_name is required and must be a string")

    show, _query_count = covered_shows_repo.add_covered_show(
        show_id=show_id,
        show_name=show_name,
        actor_uid=_actor_uid(admin or {}, x_trr_admin_user_uid),
    )
    invalidate_covered_shows_cache()
    return {"show": show}


@router.delete("/{show_id}")
def delete_covered_show(show_id: str, _: InternalAdminUser = None) -> dict[str, bool]:
    deleted, _query_count = covered_shows_repo.remove_covered_show(show_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Show not found in covered shows list")
    invalidate_covered_shows_cache()
    return {"success": True}


@router.post("/cache/invalidate")
def invalidate_cache(_: InternalAdminUser = None) -> dict[str, bool]:
    invalidate_covered_shows_cache()
    logger.info("[admin-covered-shows-read] route=invalidate-cache")
    return {"success": True}
