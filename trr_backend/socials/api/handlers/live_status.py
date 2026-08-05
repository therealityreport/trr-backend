"""Live social ingest status handlers shared by admin HTTP and SSE routes."""

from __future__ import annotations

import copy
import logging
import os
from datetime import UTC, datetime
from threading import Lock
from time import monotonic
from typing import Any

from trr_backend.db.pg import is_database_service_unavailable_error

logger = logging.getLogger(__name__)

LIVE_STATUS_STREAM_INTERVAL_SECONDS = 5.0
LIVE_STATUS_STREAM_FETCH_TIMEOUT_SECONDS = float(os.getenv("SOCIAL_LIVE_STATUS_STREAM_FETCH_TIMEOUT_SECONDS", "8"))

_LIVE_STATUS_SNAPSHOT_TTL_SECONDS = float(os.getenv("SOCIAL_LIVE_STATUS_SNAPSHOT_TTL_SECONDS", "5"))
_LIVE_STATUS_SNAPSHOT_STALE_SECONDS = float(os.getenv("SOCIAL_LIVE_STATUS_SNAPSHOT_STALE_SECONDS", "30"))
_LIVE_STATUS_SEQUENCE = 0
_LIVE_STATUS_SNAPSHOT_LOCK = Lock()
_LIVE_STATUS_SNAPSHOT_CACHE: dict[str, Any] | None = None


def _next_live_status_sequence() -> int:
    global _LIVE_STATUS_SEQUENCE
    _LIVE_STATUS_SEQUENCE += 1
    return _LIVE_STATUS_SEQUENCE


def build_social_ingest_health_dot(status_payload: dict[str, Any]) -> dict[str, Any]:
    workers_payload = status_payload.get("workers") if isinstance(status_payload, dict) else {}
    queue_payload = status_payload.get("queue") if isinstance(status_payload, dict) else {}
    by_status = queue_payload.get("by_status") if isinstance(queue_payload, dict) else {}
    return {
        "queue_enabled": bool(status_payload.get("queue_enabled") if isinstance(status_payload, dict) else False),
        "workers": {
            "healthy": bool(workers_payload.get("healthy")) if isinstance(workers_payload, dict) else False,
            "healthy_workers": int(workers_payload.get("healthy_workers") or 0)
            if isinstance(workers_payload, dict)
            else 0,
            "shared_account_backfill_readiness": (
                workers_payload.get("shared_account_backfill_readiness") if isinstance(workers_payload, dict) else None
            ),
        },
        "queue": {
            "by_status": {
                "running": int(by_status.get("running") or 0) if isinstance(by_status, dict) else 0,
                "pending": int(by_status.get("pending") or 0) if isinstance(by_status, dict) else 0,
                "queued": int(by_status.get("queued") or 0) if isinstance(by_status, dict) else 0,
                "failed": int(by_status.get("failed") or 0) if isinstance(by_status, dict) else 0,
            },
        },
        "updated_at": datetime.now(UTC).isoformat(),
    }


def _build_live_status_payload_uncached() -> dict[str, Any]:
    import trr_backend.socials.control_plane.queue_status as queue_status_control_plane
    from trr_backend.repositories import admin_operations as admin_operations_repo

    try:
        queue_status = queue_status_control_plane.get_queue_status(
            include_recent_failures=False,
            include_stuck_jobs=False,
            include_runs_summary=False,
            summary_only=True,
            statement_timeout_ms=1000,
        )
    except Exception as exc:  # noqa: BLE001
        if is_database_service_unavailable_error(exc):
            raise
        logger.warning("Failed to build social live-status queue payload", exc_info=True)
        queue_status = {
            "queue_enabled": False,
            "workers": {"healthy": False, "healthy_workers": 0, "degraded": True},
            "queue": {"by_status": {}},
            "alerts": [
                {
                    "code": "live_status_queue_degraded",
                    "message": "Queue status read is degraded.",
                    "error_type": type(exc).__name__,
                }
            ],
            "degraded": True,
        }
    try:
        admin_operations = admin_operations_repo.get_admin_operations_health()
    except Exception as exc:  # noqa: BLE001
        if is_database_service_unavailable_error(exc):
            raise
        logger.warning("Failed to build social live-status admin-operations payload", exc_info=True)
        admin_operations = {
            "healthy": False,
            "degraded": True,
            "reason": "admin_operations_health_unavailable",
            "error_type": type(exc).__name__,
        }
    return {
        "health_dot": build_social_ingest_health_dot(queue_status),
        "queue_status": queue_status,
        "admin_operations": admin_operations,
        "generated_at": datetime.now(UTC).isoformat(),
        "sequence": _next_live_status_sequence(),
    }


def build_degraded_live_status_payload(reason: str, exc: Exception | None = None) -> dict[str, Any]:
    error_type = type(exc).__name__ if exc is not None else None
    return {
        "health_dot": build_social_ingest_health_dot(
            {
                "queue_enabled": False,
                "workers": {"healthy": False, "healthy_workers": 0},
                "queue": {"by_status": {}},
            }
        ),
        "queue_status": {
            "queue_enabled": False,
            "degraded": True,
            "reason": reason,
            **({"error_type": error_type} if error_type else {}),
        },
        "admin_operations": {
            "healthy": False,
            "degraded": True,
            "reason": reason,
            **({"error_type": error_type} if error_type else {}),
        },
        "generated_at": datetime.now(UTC).isoformat(),
        "sequence": _next_live_status_sequence(),
        "degraded": True,
        "degraded_reason": reason,
    }


def _live_status_payload_with_snapshot_metadata(
    payload: dict[str, Any],
    *,
    cache_status: str,
    fetched_at: float,
    stale: bool,
    refresh_error: Exception | None = None,
) -> dict[str, Any]:
    next_payload = copy.deepcopy(payload)
    cache_age_ms = max(0, int((monotonic() - fetched_at) * 1000))
    metadata: dict[str, Any] = {
        "cache_status": cache_status,
        "generated_at": str(next_payload.get("generated_at") or datetime.now(UTC).isoformat()),
        "cache_age_ms": cache_age_ms,
        "ttl_ms": int(_LIVE_STATUS_SNAPSHOT_TTL_SECONDS * 1000),
        "stale_if_error_ttl_ms": int(_LIVE_STATUS_SNAPSHOT_STALE_SECONDS * 1000),
        "stale": stale,
    }
    if refresh_error is not None:
        metadata["refresh_error"] = type(refresh_error).__name__
    next_payload["snapshot"] = metadata
    return next_payload


def build_live_status_payload() -> dict[str, Any]:
    """Return a cheap live-status snapshot shared by HTTP and SSE subscribers."""
    global _LIVE_STATUS_SNAPSHOT_CACHE

    now = monotonic()
    cached = _LIVE_STATUS_SNAPSHOT_CACHE
    if cached is not None:
        fetched_at = float(cached["fetched_at"])
        if now - fetched_at < _LIVE_STATUS_SNAPSHOT_TTL_SECONDS:
            return _live_status_payload_with_snapshot_metadata(
                cached["payload"],
                cache_status="hit",
                fetched_at=fetched_at,
                stale=False,
            )

    lock_acquired = _LIVE_STATUS_SNAPSHOT_LOCK.acquire(blocking=False)
    if not lock_acquired and cached is not None:
        fetched_at = float(cached["fetched_at"])
        if now - fetched_at <= _LIVE_STATUS_SNAPSHOT_TTL_SECONDS + _LIVE_STATUS_SNAPSHOT_STALE_SECONDS:
            return _live_status_payload_with_snapshot_metadata(
                cached["payload"],
                cache_status="stale-refreshing",
                fetched_at=fetched_at,
                stale=True,
            )
    if not lock_acquired:
        _LIVE_STATUS_SNAPSHOT_LOCK.acquire()
        lock_acquired = True
    try:
        cached = _LIVE_STATUS_SNAPSHOT_CACHE
        if cached is not None:
            fetched_at = float(cached["fetched_at"])
            if monotonic() - fetched_at < _LIVE_STATUS_SNAPSHOT_TTL_SECONDS:
                return _live_status_payload_with_snapshot_metadata(
                    cached["payload"],
                    cache_status="hit",
                    fetched_at=fetched_at,
                    stale=False,
                )

        try:
            payload = _build_live_status_payload_uncached()
        except Exception as exc:
            cached = _LIVE_STATUS_SNAPSHOT_CACHE
            if cached is not None:
                fetched_at = float(cached["fetched_at"])
                if now - fetched_at <= _LIVE_STATUS_SNAPSHOT_TTL_SECONDS + _LIVE_STATUS_SNAPSHOT_STALE_SECONDS:
                    return _live_status_payload_with_snapshot_metadata(
                        cached["payload"],
                        cache_status="stale",
                        fetched_at=fetched_at,
                        stale=True,
                        refresh_error=exc,
                    )
            raise

        fetched_at = monotonic()
        _LIVE_STATUS_SNAPSHOT_CACHE = {"payload": copy.deepcopy(payload), "fetched_at": fetched_at}
        return _live_status_payload_with_snapshot_metadata(
            payload,
            cache_status="miss" if cached is None else "refresh",
            fetched_at=fetched_at,
            stale=False,
        )
    finally:
        if lock_acquired:
            _LIVE_STATUS_SNAPSHOT_LOCK.release()
