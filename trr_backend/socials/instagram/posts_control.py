"""Instagram posts_scrapling control-room entrypoints."""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Mapping
from typing import Any

from trr_backend.db import pg

logger = logging.getLogger(__name__)

_LOCAL_ROOM_NAMES: set[str] = set()
_LOCAL_ROOM_FUNCTIONS: dict[str, Any] = {}
_LEGACY_NAMESPACE: dict[str, Any] | None = None
_LEGACY_ORIGINALS: dict[str, Any] = {}
_MISSING = object()


def _configure_legacy_provider(
    namespace: dict[str, Any],
    originals: Mapping[str, Any],
) -> None:
    """Bind the supported monolith patch surface without importing it."""

    global _LEGACY_NAMESPACE, _LEGACY_ORIGINALS

    _LEGACY_NAMESPACE = namespace
    _LEGACY_ORIGINALS = dict(originals)


def _legacy_value(name: str, local_value: Any = _MISSING) -> Any:
    namespace = _LEGACY_NAMESPACE
    if namespace is not None and name in namespace:
        return namespace[name]
    if local_value is not _MISSING:
        return local_value
    raise RuntimeError(f"Instagram posts-control provider is not configured: {name}")


def _legacy_callable(name: str, local_impl: Any = _MISSING) -> Any:
    candidate = _legacy_value(name, local_impl)
    if not callable(candidate):
        raise TypeError(f"Instagram posts-control provider is not callable: {name}")
    return candidate


def _room_callable(name: str, local_impl: Any) -> Any:
    candidate = _legacy_value(name, None)
    if callable(candidate) and candidate is not _LEGACY_ORIGINALS.get(name):
        return candidate
    return local_impl


def _social_account_posts_scrapling_start_lock_key(platform: str, account_handle: str) -> int:
    """Per-(platform, account) advisory lock key preventing concurrent posts_scrapling starts."""
    normalize_platform = _legacy_callable("_normalize_social_account_profile_platform")
    normalize_account = _legacy_callable("_normalize_social_account_profile_handle")
    normalized_platform = normalize_platform(platform)
    normalized_account = normalize_account(account_handle)
    return int(
        hashlib.md5(f"posts-scrapling-start:{normalized_platform}:{normalized_account}".encode()).hexdigest()[:15],
        16,
    ) % (2**31)


def get_active_social_account_posts_scrapling_run(platform: str, account_handle: str) -> dict[str, Any] | None:
    """Return an active (queued/running) posts_scrapling run for this account, if any."""
    normalize_platform = _legacy_callable("_normalize_social_account_profile_platform")
    normalize_account = _legacy_callable("_normalize_social_account_profile_handle")
    normalized_platform = normalize_platform(platform)
    normalized_account = normalize_account(account_handle)
    expected_stage = (
        _legacy_value("INSTAGRAM_POSTS_SCRAPLING_STAGE")
        if normalized_platform == "instagram"
        else _legacy_value("TIKTOK_POSTS_SCRAPLING_STAGE")
    )
    pg_runtime = _legacy_value("pg", pg)
    row = pg_runtime.fetch_one(
        """
        select id::text as run_id, status
        from social.scrape_runs
        where status in ('queued', 'running')
          and config ->> 'platform' = %s
          and lower(config ->> 'account') = lower(%s)
          and config ->> 'stage' = %s
        order by created_at desc
        limit 1
        """,
        [normalized_platform, normalized_account, expected_stage],
    )
    return row


def start_instagram_posts_scrapling_scrape(
    *,
    account_handle: str,
    max_pages: int | None = None,
    fast_mode: bool = False,
    source_scope: str = "network",
    season_id: str | None = None,
    initiated_by: str | None = None,
    inline_worker_id: str | None = None,
) -> dict[str, Any]:
    """Enqueue a manual posts_scrapling scrape run for one Instagram account.

    Returns: {run_id, job_id, status, platform, account_handle, required_worker_lane}
    Raises: SocialIngestValidationError on bad args, SocialIngestConflictError on concurrent run.
    """
    normalize_account = _legacy_callable("_normalize_social_account_profile_handle")
    validation_error = _legacy_value("SocialIngestValidationError")
    conflict_error = _legacy_value("SocialIngestConflictError")
    assert_profile_exists = _legacy_callable("_assert_social_account_profile_exists")
    active_posts_auth_cooldown = _legacy_callable("_active_posts_auth_cooldown")
    is_queue_enabled_runtime = _legacy_callable("is_queue_enabled")
    assert_worker_available = _legacy_callable("assert_worker_available_when_queue_enabled")
    create_run = _legacy_callable("_create_run")
    create_job = _legacy_callable("_create_job")
    set_run_status = _legacy_callable("_set_run_status")
    dispatch_due_jobs = _legacy_callable("dispatch_due_social_jobs")
    pg_runtime = _legacy_value("pg", pg)
    logger_runtime = _legacy_value("logger", logger)
    posts_stage = _legacy_value("INSTAGRAM_POSTS_SCRAPLING_STAGE")
    worker_lane = _legacy_value("INSTAGRAM_POSTS_SCRAPLING_WORKER_LANE")
    normalized_platform = "instagram"
    normalized_account = normalize_account(account_handle)
    if not normalized_account:
        raise validation_error(
            "SOCIAL_POSTS_SCRAPLING_ACCOUNT_REQUIRED",
            "An Instagram account handle is required.",
        )
    assert_profile_exists(normalized_platform, normalized_account)
    cooldown = active_posts_auth_cooldown(normalized_platform, normalized_account)
    if cooldown is not None:
        raise conflict_error(
            "SOCIAL_POSTS_SCRAPLING_AUTH_COOLDOWN_ACTIVE",
            f"Posts_scrapling start is deferred for @{normalized_account} while Instagram auth cooldown is active.",
            detail={
                "platform": normalized_platform,
                "account_handle": normalized_account,
                "auth_cooldown": cooldown,
            },
        )

    lock_key = _social_account_posts_scrapling_start_lock_key(normalized_platform, normalized_account)
    lock_label = f"posts-scrapling-start-lock:instagram:{normalized_account[:48]}"
    with pg_runtime.db_connection(label=lock_label, pool_name="session_control") as lock_conn:
        with pg_runtime.db_cursor(conn=lock_conn, label=lock_label) as cur:
            lock_row = (
                pg_runtime.fetch_one_with_cursor(
                    cur,
                    "select pg_try_advisory_lock(%s) as locked",
                    [lock_key],
                )
                or {}
            )
        if not bool(lock_row.get("locked")):
            active_run_loader = _room_callable(
                "get_active_social_account_posts_scrapling_run",
                get_active_social_account_posts_scrapling_run,
            )
            active_run = active_run_loader(normalized_platform, normalized_account)
            raise conflict_error(
                "SOCIAL_POSTS_SCRAPLING_RUN_ALREADY_ACTIVE",
                f"Posts_scrapling run already active for @{normalized_account}.",
                detail=active_run or {"platform": normalized_platform, "account_handle": normalized_account},
            )
        try:
            active_run_loader = _room_callable(
                "get_active_social_account_posts_scrapling_run",
                get_active_social_account_posts_scrapling_run,
            )
            active_run = active_run_loader(normalized_platform, normalized_account)
            if active_run:
                raise conflict_error(
                    "SOCIAL_POSTS_SCRAPLING_RUN_ALREADY_ACTIVE",
                    f"Posts_scrapling run {active_run.get('run_id')} already active for @{normalized_account}.",
                    detail=active_run,
                )
            if is_queue_enabled_runtime():
                assert_worker_available(
                    required_worker_lane=worker_lane,
                    platform=normalized_platform,
                )

            run_status = "queued" if is_queue_enabled_runtime() else "running"
            job_status = "queued" if is_queue_enabled_runtime() else "pending"
            run_config = {
                "platform": normalized_platform,
                "account": normalized_account,
                "source_scope": source_scope,
                "stage": posts_stage,
                "max_pages": max_pages,
                "fast_mode": fast_mode,
                "season_id": season_id,
                "required_worker_lane": worker_lane,
                "ingest_mode": "posts_only",
            }
            run_id = create_run(
                None,
                source_scope=source_scope,
                initiated_by=initiated_by,
                config=run_config,
                status=run_status,
            )
            try:
                job_id = create_job(
                    None,
                    run_id=run_id,
                    platform=normalized_platform,
                    source_scope=source_scope,
                    job_type="posts",
                    stage=posts_stage,
                    config={**run_config, "account": normalized_account},
                    initiated_by=initiated_by,
                    status=job_status,
                    priority=105,
                    worker_id=inline_worker_id,
                    preclaim=bool(inline_worker_id),
                )
            except Exception:  # noqa: BLE001
                try:
                    set_run_status(run_id, "failed")
                except Exception:  # noqa: BLE001
                    logger_runtime.warning(
                        "[posts-scrapling-start] failed to mark orphaned instagram run as failed run_id=%s",
                        run_id,
                        exc_info=True,
                    )
                raise
            if is_queue_enabled_runtime():
                dispatch_due_jobs(run_id=run_id)
            return {
                "run_id": run_id,
                "job_id": job_id,
                "status": run_status,
                "job_status": job_status,
                "platform": normalized_platform,
                "account_handle": normalized_account,
                "required_worker_lane": worker_lane,
            }
        finally:
            try:
                with pg_runtime.db_cursor(conn=lock_conn, label=lock_label) as cur:
                    pg_runtime.fetch_one_with_cursor(
                        cur,
                        "select pg_advisory_unlock(%s) as unlocked",
                        [lock_key],
                    )
            except Exception:  # noqa: BLE001
                logger_runtime.debug(
                    "[posts-scrapling-start-lock] advisory unlock failed for instagram/%s",
                    normalized_account,
                    exc_info=True,
                )


_LOCAL_ROOM_NAMES = {
    "_social_account_posts_scrapling_start_lock_key",
    "get_active_social_account_posts_scrapling_run",
    "start_instagram_posts_scrapling_scrape",
}
_LOCAL_ROOM_FUNCTIONS = {_name: globals()[_name] for _name in _LOCAL_ROOM_NAMES}
__all__ = [
    "_social_account_posts_scrapling_start_lock_key",
    "get_active_social_account_posts_scrapling_run",
    "start_instagram_posts_scrapling_scrape",
]
