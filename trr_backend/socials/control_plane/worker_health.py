"""Worker-heartbeat and queue-health surfaces for the social control plane.
This module owns the public worker-health/control-plane behavior. It still uses
`social_season_analytics_impl` as the private dependency provider for shared DB,
Modal, auth, normalization, and cache helpers while the monolith is shrinking.
"""

from __future__ import annotations

import copy
import json
import os
from collections.abc import Mapping
from typing import Any

from trr_backend.socials.control_plane.queue_status import _legacy_repo, get_queue_status

_core = _legacy_repo()


def is_queue_enabled() -> bool:
    raw = (os.getenv("SOCIAL_QUEUE_ENABLED") or "").strip().lower()
    if not raw:
        return False
    return raw in {"1", "true", "yes", "on"}


def get_worker_auth_capabilities(*, validate_instagram: bool = True) -> dict[str, Any]:
    instagram_cookies = _core._load_instagram_cookies_from_sources()
    tiktok_cookies = _core._load_tiktok_cookies_from_sources()
    twitter_cookies, twitter_bearer = _core._load_twitter_auth()
    twikit_creds = _core._load_twikit_credentials(twitter_cookies)
    facebook_cookies = _core._load_facebook_cookies_from_sources()
    threads_cookies = _core._load_threads_cookies_from_sources()
    instagram_validation = (
        _core._inspect_instagram_cookie_health(instagram_cookies)
        if validate_instagram
        else _core._instagram_cookie_schema_result(instagram_cookies)
    )
    instagram_authenticated = bool(instagram_validation.get("valid"))
    instagram_auth_reason = str(instagram_validation.get("reason") or "").strip() or None
    instagram_auth_detail = (
        dict(instagram_validation.get("detail") or {}) if isinstance(instagram_validation.get("detail"), dict) else None
    )
    return {
        "instagram_authenticated": instagram_authenticated,
        "instagram_auth_reason": None if instagram_authenticated else instagram_auth_reason,
        "instagram_auth_detail": None if instagram_authenticated else instagram_auth_detail,
        "tiktok_authenticated": bool(tiktok_cookies.get("sessionid") or tiktok_cookies.get("sid_tt")),
        "twitter_authenticated": bool(
            (twitter_cookies.get("auth_token") and twitter_cookies.get("ct0")) or twitter_bearer or twikit_creds
        ),
        "facebook_authenticated": bool(facebook_cookies.get("c_user") and facebook_cookies.get("xs")),
        "threads_authenticated": bool(threads_cookies.get("sessionid") and threads_cookies.get("csrftoken")),
    }


def probe_remote_auth_health(platform: str) -> dict[str, Any]:
    return _core.probe_remote_auth_health(platform)


def _clear_worker_health_caches() -> None:
    with _core._worker_health_cache_lock:
        _core._worker_health_cache = None
    with _core._queue_status_cache_lock:
        _core._queue_status_cache = None


def update_worker_heartbeat(*args: Any, **kwargs: Any) -> dict[str, Any] | None:
    worker_id = str(args[0] if args else kwargs.pop("worker_id", "")).strip()
    stage = kwargs.pop("stage", None)
    status = kwargs.pop("status", None)
    run_id = kwargs.pop("run_id", None)
    current_job_id = kwargs.pop("current_job_id", None)
    metadata = kwargs.pop("metadata", None)
    supported_platforms = kwargs.pop("supported_platforms", None)
    if kwargs:
        unexpected = ", ".join(sorted(kwargs))
        raise TypeError(f"unexpected worker heartbeat kwargs: {unexpected}")
    if not _core._worker_heartbeat_schema_ready():
        return None

    cleaned = sorted({p.strip().lower() for p in (supported_platforms or []) if p.strip()})
    normalized_platforms = cleaned if cleaned else None
    row = _core.pg.fetch_one(
        """
        insert into social.scrape_workers (
          worker_id,
          stage,
          status,
          run_id,
          current_job_id,
          metadata,
          started_at,
          last_seen_at,
          updated_at,
          supported_platforms
        )
        values (
          %s,
          %s,
          %s,
          %s,
          %s,
          %s::jsonb,
          now(),
          now(),
          now(),
          %s
        )
        on conflict (worker_id)
        do update set
          stage = excluded.stage,
          status = excluded.status,
          run_id = excluded.run_id,
          current_job_id = excluded.current_job_id,
          metadata = excluded.metadata,
          last_seen_at = now(),
          updated_at = now(),
          supported_platforms = CASE
            WHEN excluded.supported_platforms IS NOT NULL THEN excluded.supported_platforms
            ELSE social.scrape_workers.supported_platforms
          END
        returning
          worker_id,
          stage,
          status,
          run_id::text as run_id,
          current_job_id::text as current_job_id,
          metadata,
          started_at,
          last_seen_at,
          updated_at,
          supported_platforms
        """,
        [
            worker_id,
            _core._normalize_worker_stage(stage),
            _core._normalize_worker_status(status),
            run_id,
            current_job_id,
            json.dumps(metadata or {}),
            normalized_platforms,
        ],
    )
    _clear_worker_health_caches()
    return row


def mark_worker_stopped(*args: Any, **kwargs: Any) -> dict[str, Any] | None:
    worker_id = str(args[0] if args else kwargs.pop("worker_id", "")).strip()
    stage = kwargs.pop("stage", None)
    metadata = kwargs.pop("metadata", None)
    if kwargs:
        unexpected = ", ".join(sorted(kwargs))
        raise TypeError(f"unexpected worker stopped kwargs: {unexpected}")
    return update_worker_heartbeat(
        worker_id,
        stage=stage,
        status="stopped",
        run_id=None,
        current_job_id=None,
        metadata=metadata,
        supported_platforms=None,
    )


def _query_worker_health(*, stale_after_seconds: int | None = None) -> dict[str, Any]:
    return _core._query_worker_health(stale_after_seconds=stale_after_seconds)


def get_worker_health(*, stale_after_seconds: int | None = None) -> dict[str, Any]:
    modal_executor_reason: str | None = None
    modal_executor_enabled = is_queue_enabled() and _core.is_modal_remote_executor_enabled()
    if modal_executor_enabled:
        ready, reason = _core._modal_social_dispatch_ready()
        modal_executor_reason = None if ready else reason
    cache_ttl_seconds = _core._resolve_positive_int_env(
        "SOCIAL_WORKER_HEALTH_CACHE_TTL_SECONDS",
        _core.SOCIAL_WORKER_HEALTH_CACHE_TTL_SECONDS_DEFAULT,
        minimum=0,
    )
    if cache_ttl_seconds <= 0:
        payload = _query_worker_health(stale_after_seconds=stale_after_seconds)
        if modal_executor_enabled:
            return _core._build_modal_executor_health_payload(reason=modal_executor_reason)
        return payload

    if modal_executor_enabled:
        return _core._build_modal_executor_health_payload(reason=modal_executor_reason)

    executor_cache_context = (modal_executor_enabled, modal_executor_reason)
    now = _core.time_module.monotonic()
    with _core._worker_health_cache_lock:
        if _core._worker_health_cache is not None:
            cache_entry = _core._worker_health_cache
            cached_context = None
            if len(cache_entry) == 4:
                cached_at, cached_stale_after, cached_context, cached_payload = cache_entry
            else:
                cached_at, cached_stale_after, cached_payload = cache_entry
                if (
                    isinstance(cached_payload, Mapping)
                    and str(cached_payload.get("executor_backend") or "").strip().lower() == "modal"
                ):
                    cached_context = (True, None)
            if (
                cached_stale_after == stale_after_seconds
                and cached_context in {None, executor_cache_context}
                and (now - cached_at) < cache_ttl_seconds
            ):
                return copy.deepcopy(cached_payload)

    payload = _query_worker_health(stale_after_seconds=stale_after_seconds)
    if modal_executor_enabled:
        payload = _core._build_modal_executor_health_payload(reason=modal_executor_reason)
    else:
        payload["alerts"] = _core._build_worker_health_alerts(payload)
    with _core._worker_health_cache_lock:
        _core._worker_health_cache = (
            _core.time_module.monotonic(),
            stale_after_seconds,
            executor_cache_context,
            payload,
        )
    return copy.deepcopy(payload)


def _worker_uses_modal_backend(worker: Mapping[str, Any]) -> bool:
    worker_id = str(worker.get("worker_id") or "").strip().lower()
    metadata = _core._metadata_dict(worker.get("metadata"))
    execution_backend = str(metadata.get("execution_backend_canonical") or "").strip().lower()
    dispatcher_name = str(metadata.get("dispatcher_name") or "").strip().lower()
    return worker_id.startswith("modal:") or execution_backend == "modal" or dispatcher_name == "social"


def get_trusted_local_worker_health(
    *,
    platform: str | None = None,
    stale_after_seconds: int | None = None,
) -> dict[str, Any]:
    normalized_platform = _core._normalize_platform_name(platform) if platform else None
    base_payload = _query_worker_health(stale_after_seconds=stale_after_seconds)
    workers = [
        worker
        for worker in list(base_payload.get("workers") or [])
        if not _worker_uses_modal_backend(worker)
        and (
            normalized_platform is None
            or normalized_platform
            in {
                _core._normalize_platform_name(item)
                for item in (worker.get("supported_platforms") or [])
                if _core._normalize_platform_name(item)
            }
        )
    ]
    healthy_workers = sum(1 for worker in workers if bool(worker.get("is_healthy")))
    fresh_workers = sum(1 for worker in workers if bool(worker.get("is_fresh")))
    active_workers = sum(1 for worker in workers if str(worker.get("status") or "").strip().lower() == "working")
    payload = {
        **base_payload,
        "healthy": healthy_workers > 0,
        "healthy_workers": healthy_workers,
        "fresh_workers": fresh_workers,
        "stale_workers": max(0, len(workers) - fresh_workers),
        "stale_hidden_count": 0,
        "active_workers": active_workers,
        "total_workers": len(workers),
        "workers": workers,
        "executor_backend": "local",
        "required_worker_lane": _core.TRUSTED_LOCAL_WORKER_LANE,
        "platform_filter": normalized_platform,
    }
    if len(workers) <= 0:
        payload["reason"] = "no_local_workers"
    elif healthy_workers <= 0:
        payload["reason"] = "no_healthy_local_workers"
    else:
        payload["reason"] = None
    return payload


def get_worker_health_for_lane(
    *,
    required_worker_lane: str,
    platform: str | None = None,
    stale_after_seconds: int | None = None,
) -> dict[str, Any]:
    normalized_lane = _core._normalize_required_worker_lane(required_worker_lane)
    if not normalized_lane:
        return get_worker_health(stale_after_seconds=stale_after_seconds)
    if normalized_lane == _core.TRUSTED_LOCAL_WORKER_LANE:
        return get_trusted_local_worker_health(platform=platform, stale_after_seconds=stale_after_seconds)

    normalized_platform = _core._normalize_platform_name(platform) if platform else None
    base_payload = _query_worker_health(stale_after_seconds=stale_after_seconds)
    workers = []
    for worker in list(base_payload.get("workers") or []):
        metadata = _core._metadata_dict(worker.get("metadata"))
        worker_lane = _core._normalize_required_worker_lane(metadata.get("worker_lane"))
        if worker_lane != normalized_lane:
            continue
        supported_platforms = {
            _core._normalize_platform_name(item)
            for item in (worker.get("supported_platforms") or [])
            if _core._normalize_platform_name(item)
        }
        if normalized_platform is not None and normalized_platform not in supported_platforms:
            continue
        workers.append(worker)

    healthy_workers = sum(1 for worker in workers if bool(worker.get("is_healthy")))
    fresh_workers = sum(1 for worker in workers if bool(worker.get("is_fresh")))
    active_workers = sum(1 for worker in workers if str(worker.get("status") or "").strip().lower() == "working")
    payload = {
        **base_payload,
        "healthy": healthy_workers > 0,
        "healthy_workers": healthy_workers,
        "fresh_workers": fresh_workers,
        "stale_workers": max(0, len(workers) - fresh_workers),
        "stale_hidden_count": 0,
        "active_workers": active_workers,
        "total_workers": len(workers),
        "workers": workers,
        "executor_backend": "local",
        "required_worker_lane": normalized_lane,
        "platform_filter": normalized_platform,
    }
    if len(workers) <= 0:
        payload["reason"] = "no_lane_workers"
    elif healthy_workers <= 0:
        payload["reason"] = "no_healthy_lane_workers"
    else:
        payload["reason"] = None
    return payload


def get_worker_detail(worker_id: str, *, stale_after_seconds: int | None = None) -> dict[str, Any]:
    return _core.get_worker_detail(worker_id, stale_after_seconds=stale_after_seconds)


def purge_inactive_workers(*, stale_after_seconds: int | None = None) -> dict[str, Any]:
    stale_seconds = stale_after_seconds or _core._resolve_positive_int_env(
        "SOCIAL_WORKER_HEARTBEAT_STALE_SECONDS",
        _core.SOCIAL_WORKER_HEARTBEAT_STALE_SECONDS_DEFAULT,
        minimum=5,
    )
    if not _core._worker_heartbeat_schema_ready():
        return {
            "stale_after_seconds": stale_seconds,
            "active_workers": 0,
            "total_workers_before": 0,
            "total_workers_after": 0,
            "deleted_workers": 0,
            "reason": "worker_heartbeat_schema_missing",
        }

    active_clause = "status in ('starting', 'idle', 'working') and last_seen_at >= now() - (%s * interval '1 second')"
    snapshot_row = (
        _core.pg.fetch_one(
            f"""
            select
              count(*) filter (where {active_clause})::int as active_workers,
              count(*)::int as total_workers
            from social.scrape_workers
            """,
            [stale_seconds],
        )
        or {}
    )
    deleted_rows = _core.pg.execute_returning(
        f"""
        delete from social.scrape_workers
        where not ({active_clause})
        returning worker_id
        """,
        [stale_seconds],
    )

    active_workers = int(snapshot_row.get("active_workers") or 0)
    total_workers_before = int(snapshot_row.get("total_workers") or 0)
    deleted_workers = len(deleted_rows or [])
    with _core._worker_health_cache_lock:
        _core._worker_health_cache = None
    return {
        "stale_after_seconds": stale_seconds,
        "active_workers": active_workers,
        "total_workers_before": total_workers_before,
        "total_workers_after": max(0, total_workers_before - deleted_workers),
        "deleted_workers": deleted_workers,
        "reason": None,
    }


def assert_worker_available_when_queue_enabled(*args: Any, **kwargs: Any) -> dict[str, Any]:
    if args:
        raise TypeError("assert_worker_available_when_queue_enabled accepts keyword arguments only")
    required_worker_lane = kwargs.pop("required_worker_lane", None)
    required_execution_backend = kwargs.pop("required_execution_backend", None)
    platform = kwargs.pop("platform", None)
    if kwargs:
        unexpected = ", ".join(sorted(kwargs))
        raise TypeError(f"unexpected worker availability kwargs: {unexpected}")
    normalized_required_lane = _core._normalize_required_worker_lane(required_worker_lane)
    if not is_queue_enabled():
        return {
            "healthy": True,
            "healthy_workers": 0,
            "active_workers": 0,
            "total_workers": 0,
            "workers": [],
            "reason": "queue_disabled",
        }

    if normalized_required_lane:
        health = get_worker_health_for_lane(
            required_worker_lane=normalized_required_lane,
            platform=platform,
        )
        if bool(health.get("healthy")):
            return health
        raise _core.SocialWorkerUnavailableError(
            (
                "No healthy trusted-local social ingest workers are reporting heartbeats."
                if normalized_required_lane == _core.TRUSTED_LOCAL_WORKER_LANE
                else f"No healthy {normalized_required_lane} social ingest workers are reporting heartbeats."
            ),
            worker_health=health,
        )

    if str(required_execution_backend or "").strip().lower() == "modal":
        ready, reason = _core._modal_social_dispatch_ready()
        if not (_core.is_modal_remote_executor_enabled() and ready):
            payload = _core._build_modal_executor_health_payload(
                reason=reason or "modal_executor_required",
                platform=platform,
            )
            raise _core.SocialWorkerUnavailableError(
                "Modal social dispatch is required for this social ingest job.",
                worker_health=payload,
            )
        _core._touch_modal_social_dispatcher_heartbeat(
            metadata_updates={
                "dispatch_enabled": True,
                "last_dispatch_success_at": _core._iso(_core._now_utc()),
            }
        )
        payload = _core._build_modal_executor_health_payload(reason="modal_executor_ready", platform=platform)
        platform_readiness = _core._metadata_dict(payload.get("shared_account_backfill_readiness"))
        if platform and not bool(platform_readiness.get("ready")):
            normalized_platform = _core._normalize_platform_name(platform) or str(platform).strip().lower()
            raise _core.SocialWorkerUnavailableError(
                f"Modal social dispatch auth preflight is not ready for {normalized_platform}.",
                worker_health=payload,
            )
        return payload

    if _core.is_modal_remote_executor_enabled():
        ready, reason = _core._modal_social_dispatch_ready()
        if not ready:
            payload = _core._build_modal_executor_health_payload(reason=reason, platform=platform)
            raise _core.SocialWorkerUnavailableError(
                "Modal social dispatch is not configured for remote execution.",
                worker_health=payload,
            )
        _core._touch_modal_social_dispatcher_heartbeat(
            metadata_updates={
                "dispatch_enabled": True,
                "last_dispatch_success_at": _core._iso(_core._now_utc()),
            }
        )
        return _core._build_modal_executor_health_payload(reason="modal_executor_ready", platform=platform)

    health = get_worker_health()
    if bool(health.get("healthy")):
        return health

    reason = str(health.get("reason") or "no_healthy_workers")
    if reason == "worker_heartbeat_schema_missing":
        message = "Social worker heartbeat schema is missing. Apply migration 0130 before enabling queue mode."
    else:
        message = "No healthy social ingest workers are reporting heartbeats."
    raise _core.SocialWorkerUnavailableError(message, worker_health=health)


__all__ = [
    "assert_worker_available_when_queue_enabled",
    "get_queue_status",
    "get_worker_auth_capabilities",
    "get_worker_detail",
    "get_worker_health",
    "get_worker_health_for_lane",
    "get_trusted_local_worker_health",
    "is_queue_enabled",
    "mark_worker_stopped",
    "probe_remote_auth_health",
    "purge_inactive_workers",
    "update_worker_heartbeat",
]
