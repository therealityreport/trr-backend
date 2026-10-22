# ruff: noqa: F821, UP037
"""TikTok Scrapling lane launcher: enqueue a manual ``tiktok_posts_scrapling`` run for one
account, and the synchronous single-post detail refresh through yt-dlp.

Moved out of ``social_season_analytics_impl``; the legacy repository keeps
same-named wrappers that delegate through ``_LOCAL_ROOM_FUNCTIONS``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from trr_backend.socials.control_plane.run_lifecycle import _legacy_module as _published_legacy_module
from trr_backend.socials.provider_registry import (
    LateNamespaceProvider,
    LateProviderProxy,
    adopt_published,
    publish_module_slot,
)
from trr_backend.socials.tiktok.constants import TIKTOK_POSTS_SCRAPLING_STAGE, TIKTOK_POSTS_SCRAPLING_WORKER_LANE

if TYPE_CHECKING:
    from trr_backend.db import pg
    from trr_backend.socials.model_types import SeasonContext

    # Repository names resolved through the published provider namespace.
    class SocialIngestConflictError: ...

    class SocialIngestValidationError: ...

    def _assert_social_account_profile_exists(*args: Any, **kwargs: Any) -> Any: ...
    def _create_job(*args: Any, **kwargs: Any) -> Any: ...
    def _create_run(*args: Any, **kwargs: Any) -> Any: ...
    def _load_tiktok_cookies(*args: Any, **kwargs: Any) -> Any: ...
    def _normalize_social_account_profile_handle(*args: Any, **kwargs: Any) -> Any: ...
    def _resolve_tiktok_canonical_url(*args: Any, **kwargs: Any) -> Any: ...
    def _set_run_status(*args: Any, **kwargs: Any) -> Any: ...
    def _social_account_posts_scrapling_start_lock_key(*args: Any, **kwargs: Any) -> Any: ...
    def _upsert_tiktok_post(*args: Any, **kwargs: Any) -> Any: ...
    def assert_worker_available_when_queue_enabled(*args: Any, **kwargs: Any) -> Any: ...
    def dispatch_due_social_jobs(*args: Any, **kwargs: Any) -> Any: ...
    def get_active_social_account_posts_scrapling_run(*args: Any, **kwargs: Any) -> Any: ...
    def is_queue_enabled(*args: Any, **kwargs: Any) -> Any: ...

    logger: Any


# Room names are declared before the provider is built so that a late
# ``adopt_published`` (leaf imported after the repository published) still
# captures the repository wrappers for ``_room_callable``.
_LOCAL_ROOM_NAMES: set[str] = {
    "start_tiktok_posts_scrapling_scrape",
    "_refresh_tiktok_post_detail_sync",
}
_IMPORTED_CORE_NAMES: set[str] = set()
_LOCAL_ROOM_FUNCTIONS: dict[str, Any] = {}
_CORE_ROOM_WRAPPERS: dict[str, Any] = {}


_PROVIDER = LateNamespaceProvider(
    globals(),
    prefix="TIKTOK_LANE_START_PROVIDER",
    room_names=_LOCAL_ROOM_NAMES,
    imported_names=_IMPORTED_CORE_NAMES,
    room_wrappers=_CORE_ROOM_WRAPPERS,
    commit=publish_module_slot(globals(), "_core"),
)
_core: Any = LateProviderProxy(_PROVIDER)
_require_provider_ready = _PROVIDER.require
_configure_legacy_provider = _PROVIDER.configure
_sync_core_overrides = _PROVIDER.sync
_room_callable = _PROVIDER.room_callable
adopt_published(_PROVIDER, _published_legacy_module)
del _published_legacy_module


def start_tiktok_posts_scrapling_scrape(
    *,
    account_handle: str,
    max_pages: int | None = None,
    source_scope: str = "network",
    season_id: str | None = None,
    initiated_by: str | None = None,
    inline_worker_id: str | None = None,
) -> dict[str, Any]:
    """Enqueue a manual posts_scrapling scrape run for one TikTok account.

    Returns: {run_id, job_id, status, platform, account_handle, required_worker_lane}
    Raises: SocialIngestValidationError on bad args, SocialIngestConflictError on concurrent run.
    """
    _sync_core_overrides()
    normalized_platform = "tiktok"
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    if not normalized_account:
        raise SocialIngestValidationError(
            "SOCIAL_POSTS_SCRAPLING_ACCOUNT_REQUIRED",
            "A TikTok account handle is required.",
        )
    _assert_social_account_profile_exists(normalized_platform, normalized_account)

    lock_key = _social_account_posts_scrapling_start_lock_key(normalized_platform, normalized_account)
    lock_label = f"posts-scrapling-start-lock:tiktok:{normalized_account[:48]}"
    # Keep the recovery advisory lock out of the default query pool. Modal
    # social workers intentionally cap the default session-pooler connection at
    # one, and recovery finalization performs normal catalog queries while the
    # lock is held.
    with pg.db_connection(label=lock_label, pool_name="social_control") as lock_conn:
        with pg.db_cursor(conn=lock_conn, label=lock_label) as cur:
            lock_row = pg.fetch_one_with_cursor(cur, "select pg_try_advisory_lock(%s) as locked", [lock_key]) or {}
        if not bool(lock_row.get("locked")):
            active_run = get_active_social_account_posts_scrapling_run(normalized_platform, normalized_account)
            raise SocialIngestConflictError(
                "SOCIAL_POSTS_SCRAPLING_RUN_ALREADY_ACTIVE",
                f"Posts_scrapling run already active for @{normalized_account}.",
                detail=active_run or {"platform": normalized_platform, "account_handle": normalized_account},
            )
        try:
            active_run = get_active_social_account_posts_scrapling_run(normalized_platform, normalized_account)
            if active_run:
                raise SocialIngestConflictError(
                    "SOCIAL_POSTS_SCRAPLING_RUN_ALREADY_ACTIVE",
                    f"Posts_scrapling run {active_run.get('run_id')} already active for @{normalized_account}.",
                    detail=active_run,
                )
            if is_queue_enabled():
                assert_worker_available_when_queue_enabled(
                    required_worker_lane=TIKTOK_POSTS_SCRAPLING_WORKER_LANE,
                    platform=normalized_platform,
                )

            run_status = "queued" if is_queue_enabled() else "pending"
            run_config = {
                "platform": normalized_platform,
                "account": normalized_account,
                "source_scope": source_scope,
                "stage": TIKTOK_POSTS_SCRAPLING_STAGE,
                "max_pages": max_pages,
                "season_id": season_id,
                "required_worker_lane": TIKTOK_POSTS_SCRAPLING_WORKER_LANE,
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
                    stage=TIKTOK_POSTS_SCRAPLING_STAGE,
                    config={**run_config, "account": normalized_account},
                    initiated_by=initiated_by,
                    status=run_status,
                    priority=105,
                    worker_id=inline_worker_id,
                    preclaim=bool(inline_worker_id),
                )
            except Exception:  # noqa: BLE001
                try:
                    _set_run_status(run_id, "failed")
                except Exception:  # noqa: BLE001
                    logger.warning(
                        "[posts-scrapling-start] failed to mark orphaned tiktok run as failed run_id=%s",
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
                "platform": normalized_platform,
                "account_handle": normalized_account,
                "required_worker_lane": TIKTOK_POSTS_SCRAPLING_WORKER_LANE,
            }
        finally:
            try:
                with pg.db_cursor(conn=lock_conn, label=lock_label) as cur:
                    pg.fetch_one_with_cursor(cur, "select pg_advisory_unlock(%s) as unlocked", [lock_key])
            except Exception:  # noqa: BLE001
                logger.debug(
                    "[posts-scrapling-start-lock] advisory unlock failed for tiktok/%s",
                    normalized_account,
                    exc_info=True,
                )


def _refresh_tiktok_post_detail_sync(
    context: SeasonContext,
    *,
    source_id: str,
    account: str,
    row_json: dict[str, Any],
    detail_job_id: str | None,
) -> dict[str, Any]:
    _sync_core_overrides()
    from trr_backend.socials.tiktok import TikTokScrapeConfig, TikTokScraper

    scraper = TikTokScraper(cookies=_load_tiktok_cookies())
    username = str(row_json.get("username") or account or "").strip().lstrip("@")
    canonical_url = _resolve_tiktok_canonical_url(source_id=source_id, row_json=row_json, account=account)
    metadata = scraper._ytdlp_get_video_metadata(canonical_url)  # noqa: SLF001
    if not isinstance(metadata, dict):
        raise RuntimeError("tiktok_metadata_unavailable")
    config = TikTokScrapeConfig(username=username or str(account or "").strip().lstrip("@") or "unknown")
    post = scraper._parse_ytdlp_metadata(metadata, config)  # noqa: SLF001
    if not getattr(post, "video_id", ""):
        post.video_id = source_id
    with pg.db_connection() as conn:
        upserted = _upsert_tiktok_post(
            context,
            job_id=detail_job_id,
            account=account or getattr(post, "username", "") or username,
            post=post,
            conn=conn,
        )
    return {
        "status": "success" if upserted else "failed",
        "post_id": str((upserted or {}).get("id") or ""),
        "source": "yt_dlp_single_post",
    }


_LOCAL_ROOM_FUNCTIONS.update({_name: globals()[_name] for _name in _LOCAL_ROOM_NAMES})
__all__ = [
    "_refresh_tiktok_post_detail_sync",
    "start_tiktok_posts_scrapling_scrape",
]
