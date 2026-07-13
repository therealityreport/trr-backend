# ruff: noqa: F821, UP037
"""Instagram posts_scrapling control-room entrypoints."""

from __future__ import annotations

from typing import Any

import trr_backend.socials.social_season_analytics_impl as _core

_RESERVED_CORE_EXPORTS = {
    "__builtins__",
    "__cached__",
    "__doc__",
    "__file__",
    "__loader__",
    "__name__",
    "__package__",
    "__spec__",
    "_core",
    "_IMPORTED_CORE_NAMES",
    "_LOCAL_ROOM_NAMES",
    "_RESERVED_CORE_EXPORTS",
    "_sync_core_overrides",
}
_IMPORTED_CORE_NAMES: set[str] = set()
for _name, _value in _core.__dict__.items():
    if _name in _RESERVED_CORE_EXPORTS:
        continue
    globals()[_name] = _value
    _IMPORTED_CORE_NAMES.add(_name)
_LOCAL_ROOM_NAMES: set[str] = set()
_LOCAL_ROOM_FUNCTIONS: dict[str, Any] = {}
_CORE_ROOM_WRAPPERS: dict[str, Any] = {}


def _sync_core_overrides() -> None:
    for _name in _IMPORTED_CORE_NAMES - _LOCAL_ROOM_NAMES:
        if hasattr(_core, _name):
            globals()[_name] = getattr(_core, _name)


def _room_callable(name: str, local_impl: Any) -> Any:
    candidate = getattr(_core, name, None)
    if callable(candidate) and candidate is not _CORE_ROOM_WRAPPERS.get(name):
        return candidate
    return local_impl


def _social_account_posts_scrapling_start_lock_key(platform: str, account_handle: str) -> int:
    """Per-(platform, account) advisory lock key preventing concurrent posts_scrapling starts."""
    _sync_core_overrides()
    normalized_platform = _normalize_social_account_profile_platform(platform)
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    return int(
        hashlib.md5(f"posts-scrapling-start:{normalized_platform}:{normalized_account}".encode()).hexdigest()[:15],
        16,
    ) % (2**31)


def get_active_social_account_posts_scrapling_run(platform: str, account_handle: str) -> dict[str, Any] | None:
    """Return an active (queued/running) posts_scrapling run for this account, if any."""
    _sync_core_overrides()
    normalized_platform = _normalize_social_account_profile_platform(platform)
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    expected_stage = (
        INSTAGRAM_POSTS_SCRAPLING_STAGE if normalized_platform == "instagram" else TIKTOK_POSTS_SCRAPLING_STAGE
    )
    row = pg.fetch_one(
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
    _sync_core_overrides()
    normalized_platform = "instagram"
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    if not normalized_account:
        raise SocialIngestValidationError(
            "SOCIAL_POSTS_SCRAPLING_ACCOUNT_REQUIRED",
            "An Instagram account handle is required.",
        )
    _assert_social_account_profile_exists(normalized_platform, normalized_account)
    cooldown = _active_posts_auth_cooldown(normalized_platform, normalized_account)
    if cooldown is not None:
        raise SocialIngestConflictError(
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
    with pg.db_connection(label=lock_label, pool_name="session_control") as lock_conn:
        with pg.db_cursor(conn=lock_conn, label=lock_label) as cur:
            lock_row = pg.fetch_one_with_cursor(cur, "select pg_try_advisory_lock(%s) as locked", [lock_key]) or {}
        if not bool(lock_row.get("locked")):
            active_run_loader = _room_callable(
                "get_active_social_account_posts_scrapling_run",
                get_active_social_account_posts_scrapling_run,
            )
            active_run = active_run_loader(normalized_platform, normalized_account)
            raise SocialIngestConflictError(
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
                raise SocialIngestConflictError(
                    "SOCIAL_POSTS_SCRAPLING_RUN_ALREADY_ACTIVE",
                    f"Posts_scrapling run {active_run.get('run_id')} already active for @{normalized_account}.",
                    detail=active_run,
                )
            if is_queue_enabled():
                assert_worker_available_when_queue_enabled(
                    required_worker_lane=INSTAGRAM_POSTS_SCRAPLING_WORKER_LANE,
                    platform=normalized_platform,
                )

            run_status = "queued" if is_queue_enabled() else "running"
            job_status = "queued" if is_queue_enabled() else "pending"
            run_config = {
                "platform": normalized_platform,
                "account": normalized_account,
                "source_scope": source_scope,
                "stage": INSTAGRAM_POSTS_SCRAPLING_STAGE,
                "max_pages": max_pages,
                "fast_mode": fast_mode,
                "season_id": season_id,
                "required_worker_lane": INSTAGRAM_POSTS_SCRAPLING_WORKER_LANE,
                "ingest_mode": "posts_only",
            }
            run_id = _create_run(
                None,
                source_scope=source_scope,
                initiated_by=initiated_by,
                config=run_config,
                status=run_status,
            )
            try:
                job_id = _create_job(
                    None,
                    run_id=run_id,
                    platform=normalized_platform,
                    source_scope=source_scope,
                    job_type="posts",
                    stage=INSTAGRAM_POSTS_SCRAPLING_STAGE,
                    config={**run_config, "account": normalized_account},
                    initiated_by=initiated_by,
                    status=job_status,
                    priority=105,
                    worker_id=inline_worker_id,
                    preclaim=bool(inline_worker_id),
                )
            except Exception:  # noqa: BLE001
                try:
                    _set_run_status(run_id, "failed")
                except Exception:  # noqa: BLE001
                    logger.warning(
                        "[posts-scrapling-start] failed to mark orphaned instagram run as failed run_id=%s",
                        run_id,
                        exc_info=True,
                    )
                raise
            if is_queue_enabled():
                dispatch_due_social_jobs(run_id=run_id)
            return {
                "run_id": run_id,
                "job_id": job_id,
                "status": run_status,
                "job_status": job_status,
                "platform": normalized_platform,
                "account_handle": normalized_account,
                "required_worker_lane": INSTAGRAM_POSTS_SCRAPLING_WORKER_LANE,
            }
        finally:
            try:
                with pg.db_cursor(conn=lock_conn, label=lock_label) as cur:
                    pg.fetch_one_with_cursor(cur, "select pg_advisory_unlock(%s) as unlocked", [lock_key])
            except Exception:  # noqa: BLE001
                logger.debug(
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
_CORE_ROOM_WRAPPERS = {_name: getattr(_core, _name, None) for _name in _LOCAL_ROOM_NAMES}
__all__ = [
    "_social_account_posts_scrapling_start_lock_key",
    "get_active_social_account_posts_scrapling_run",
    "start_instagram_posts_scrapling_scrape",
]
