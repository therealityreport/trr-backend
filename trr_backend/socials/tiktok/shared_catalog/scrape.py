# ruff: noqa: F821, UP037
"""Shared-account TikTok catalog scrape: catalog assignment helpers, per-post hydration
(details, comments, media follow-ups), the partitioned scrape, and the account scrape
entrypoint.

Moved out of ``social_season_analytics_impl``; the legacy repository keeps
same-named wrappers that delegate through ``_LOCAL_ROOM_FUNCTIONS``.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Mapping, MutableMapping
from typing import TYPE_CHECKING, Any, cast

from trr_backend.socials.control_plane.run_lifecycle import _legacy_module as _published_legacy_module
from trr_backend.socials.provider_registry import (
    LateNamespaceProvider,
    LateProviderProxy,
    adopt_published,
    publish_module_slot,
)

if TYPE_CHECKING:
    from trr_backend.db import pg
    from trr_backend.socials.model_types import SeasonContext

    # Repository names resolved through the published provider namespace.
    CATALOG_FULL_HISTORY_CURSOR_PARTITION_STRATEGY: Any

    class _SavepointScope:
        def __init__(self, *, conn: Any | None, prefix: str, index: int) -> None: ...
        def __enter__(self) -> None: ...
        def __exit__(self, exc_type: Any, exc: Any, _tb: Any) -> bool: ...

    class _SharedTikTokCommentFetchTimeoutError(TimeoutError): ...

    def _apply_tiktok_profile_snapshot_to_post(*args: Any, **kwargs: Any) -> Any: ...
    def _bootstrap_shared_tiktok_account_context(*args: Any, **kwargs: Any) -> Any: ...
    def _coerce_dt(*args: Any, **kwargs: Any) -> Any: ...
    def _enqueue_platform_media_mirror_job(*args: Any, **kwargs: Any) -> Any: ...
    def _is_comment_fetch_complete(*args: Any, **kwargs: Any) -> Any: ...
    def _load_tiktok_comment_coverage_for_post(*args: Any, **kwargs: Any) -> Any: ...
    def _load_tiktok_cookies(*args: Any, **kwargs: Any) -> Any: ...
    def _mark_missing_comments_for_anchor(*args: Any, **kwargs: Any) -> Any: ...
    def _merge_comment_persist_stats(*args: Any, **kwargs: Any) -> Any: ...
    def _merge_tiktok_profile_snapshots(*args: Any, **kwargs: Any) -> Any: ...
    def _metadata_dict(*args: Any, **kwargs: Any) -> Any: ...
    def _new_comment_persist_stats(*args: Any, **kwargs: Any) -> Any: ...
    def _normalize_non_negative_int(*args: Any, **kwargs: Any) -> Any: ...
    def _prioritize_shared_tiktok_posts_for_comment_gaps(*args: Any, **kwargs: Any) -> Any: ...
    def _reconcile_post_comment_count(*args: Any, **kwargs: Any) -> Any: ...
    def _shared_account_expected_total_posts_from_config(*args: Any, **kwargs: Any) -> Any: ...
    def _shared_catalog_mode(*args: Any, **kwargs: Any) -> Any: ...
    def _shared_catalog_progress_interval_posts(*args: Any, **kwargs: Any) -> Any: ...
    def _shared_stage_post_limit(*args: Any, **kwargs: Any) -> Any: ...
    def _shared_tiktok_catalog_comment_delay_seconds(*args: Any, **kwargs: Any) -> Any: ...
    def _shared_tiktok_catalog_comments_enabled(*args: Any, **kwargs: Any) -> Any: ...
    def _shared_tiktok_catalog_direct_comment_override(*args: Any, **kwargs: Any) -> Any: ...
    def _shared_tiktok_catalog_effective_tasks(*args: Any, **kwargs: Any) -> Any: ...
    def _shared_tiktok_catalog_max_comments_per_post(*args: Any, **kwargs: Any) -> Any: ...
    def _shared_tiktok_comment_fetch_deadline(*args: Any, **kwargs: Any) -> Any: ...
    def _shared_tiktok_comment_fetch_timeout_seconds(*args: Any, **kwargs: Any) -> Any: ...
    def _update_platform_post_field(*args: Any, **kwargs: Any) -> Any: ...
    def _upsert_shared_catalog_post(*args: Any, **kwargs: Any) -> Any: ...
    def _upsert_tiktok_comment_tree(*args: Any, **kwargs: Any) -> Any: ...
    def _upsert_tiktok_post(*args: Any, **kwargs: Any) -> Any: ...
    def get_season_context(*args: Any, **kwargs: Any) -> Any: ...

    logger: Any


# Room names are declared before the provider is built so that a late
# ``adopt_published`` (leaf imported after the repository published) still
# captures the repository wrappers for ``_room_callable``.
_LOCAL_ROOM_NAMES: set[str] = {
    "_shared_tiktok_catalog_assignment_ids",
    "_shared_tiktok_catalog_context_from_row",
    "_apply_shared_tiktok_catalog_assignment_to_post",
    "_shared_tiktok_increment_counter",
    "_hydrate_shared_tiktok_catalog_post",
    "_scrape_shared_tiktok_posts_partitioned",
    "_scrape_shared_tiktok_posts",
}
_IMPORTED_CORE_NAMES: set[str] = set()
_LOCAL_ROOM_FUNCTIONS: dict[str, Any] = {}
_CORE_ROOM_WRAPPERS: dict[str, Any] = {}


_PROVIDER = LateNamespaceProvider(
    globals(),
    prefix="TIKTOK_SHARED_CATALOG_SCRAPE_PROVIDER",
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


def _shared_tiktok_catalog_assignment_ids(catalog_row: Mapping[str, Any] | None) -> tuple[str | None, str | None]:
    _sync_core_overrides()
    row = _metadata_dict(catalog_row)
    season_id = str(row.get("season_id") or row.get("assigned_season_id") or "").strip() or None
    show_id = str(row.get("show_id") or row.get("assigned_show_id") or "").strip() or None
    return season_id, show_id


def _shared_tiktok_catalog_context_from_row(
    catalog_row: Mapping[str, Any] | None,
    *,
    conn: Any | None = None,
) -> SeasonContext | None:
    _sync_core_overrides()
    season_id, _show_id = _room_callable(
        "_shared_tiktok_catalog_assignment_ids", _shared_tiktok_catalog_assignment_ids
    )(catalog_row)
    if not season_id:
        return None
    try:
        return get_season_context(season_id, conn=conn)
    except Exception:  # noqa: BLE001
        logger.debug("[tiktok] shared catalog row season context unavailable: %s", season_id, exc_info=True)
        return None


def _apply_shared_tiktok_catalog_assignment_to_post(
    *,
    post_row: Mapping[str, Any],
    catalog_row: Mapping[str, Any] | None,
    conn: Any | None = None,
) -> None:
    _sync_core_overrides()
    post_id = str(post_row.get("id") or "").strip()
    if not post_id:
        return
    season_id, show_id = _room_callable("_shared_tiktok_catalog_assignment_ids", _shared_tiktok_catalog_assignment_ids)(
        catalog_row
    )
    if season_id:
        _update_platform_post_field(platform="tiktok", post_id=post_id, field="season_id", value=season_id, conn=conn)
    if show_id:
        _update_platform_post_field(platform="tiktok", post_id=post_id, field="show_id", value=show_id, conn=conn)


def _shared_tiktok_increment_counter(
    counters: MutableMapping[str, int],
    key: str,
    amount: int = 1,
) -> None:
    counters[key] = int(counters.get(key) or 0) + max(0, int(amount or 0))


def _hydrate_shared_tiktok_catalog_post(
    *,
    scraper: Any,
    run_id: str | None,
    account_handle: str,
    config: Mapping[str, Any],
    job_id: str,
    post: Any,
    catalog_row: Mapping[str, Any] | None,
    counters: MutableMapping[str, int],
    comment_fail_reasons: Counter[str],
    progress_cb: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any] | None:
    _sync_core_overrides()
    tasks = _shared_tiktok_catalog_effective_tasks(config)
    if not tasks:
        return None

    source_scope = str(config.get("source_scope") or "network").strip().lower() or "network"
    should_materialize = bool(tasks & {"post_details", "comments", "media"})
    should_enqueue_media = "media" in tasks and not bool(config.get("details_refresh_skip_media_followups"))
    should_fetch_comments = _shared_tiktok_catalog_comments_enabled(config)
    materialized_row: dict[str, Any] | None = None
    context: SeasonContext | None = None
    video_id = str(getattr(post, "video_id", "") or "").strip()
    expected_comment_count = max(
        _normalize_non_negative_int(getattr(post, "comments", 0)),
        _normalize_non_negative_int(_metadata_dict(catalog_row).get("comments_count")),
    )
    pre_fetch_active_comments = 0

    if should_materialize:
        try:
            with pg.db_connection(label="tiktok-shared-catalog-materialize") as conn:
                context = _room_callable(
                    "_shared_tiktok_catalog_context_from_row", _shared_tiktok_catalog_context_from_row
                )(catalog_row, conn=conn)
                materialized_row = _upsert_tiktok_post(
                    context,
                    job_id=job_id,
                    account=account_handle,
                    post=post,
                    conn=conn,
                )
                if materialized_row:
                    _room_callable("_shared_tiktok_increment_counter", _shared_tiktok_increment_counter)(
                        counters, "materialized_posts_upserted"
                    )
                    _room_callable(
                        "_apply_shared_tiktok_catalog_assignment_to_post",
                        _apply_shared_tiktok_catalog_assignment_to_post,
                    )(
                        post_row=materialized_row,
                        catalog_row=catalog_row,
                        conn=conn,
                    )
                    expected_comment_count = max(
                        expected_comment_count,
                        _normalize_non_negative_int(materialized_row.get("comments_count")),
                    )
                    if should_fetch_comments:
                        existing_coverage = _load_tiktok_comment_coverage_for_post(
                            str(materialized_row.get("id") or ""),
                            conn=conn,
                        )
                        pre_fetch_active_comments = _normalize_non_negative_int(
                            existing_coverage.get("active_comments")
                        )
                    if should_enqueue_media:
                        mirror_job_id = _enqueue_platform_media_mirror_job(
                            context,
                            platform="tiktok",
                            run_id=run_id,
                            source_scope=source_scope,
                            account=account_handle,
                            post_row=materialized_row,
                            week_index=None,
                            parent_job_id=job_id,
                            conn=conn,
                        )
                        if mirror_job_id:
                            _room_callable("_shared_tiktok_increment_counter", _shared_tiktok_increment_counter)(
                                counters, "media_mirror_jobs_enqueued"
                            )
        except Exception:  # noqa: BLE001
            _room_callable("_shared_tiktok_increment_counter", _shared_tiktok_increment_counter)(
                counters, "materialized_post_errors"
            )
            logger.exception("[tiktok] Failed to materialize shared catalog post video_id=%s", video_id or "?")
            return None

    if not should_fetch_comments:
        return materialized_row
    if not materialized_row:
        _room_callable("_shared_tiktok_increment_counter", _shared_tiktok_increment_counter)(
            counters, "comment_posts_skipped_missing_materialized_post"
        )
        _room_callable("_shared_tiktok_increment_counter", _shared_tiktok_increment_counter)(
            counters, "incomplete_comment_fetches"
        )
        return None
    if pre_fetch_active_comments >= expected_comment_count:
        _room_callable("_shared_tiktok_increment_counter", _shared_tiktok_increment_counter)(
            counters, "comment_posts_skipped_complete"
        )
        if progress_cb:
            progress_cb(
                {
                    "phase": "skip_tiktok_comments_complete",
                    "scraped_comments": _normalize_non_negative_int(counters.get("comments_fetched")),
                    "comments_upserted": _normalize_non_negative_int(counters.get("comments_upserted")),
                    "current_source_id": video_id or None,
                }
            )
        return materialized_row

    max_comments_per_post = _shared_tiktok_catalog_max_comments_per_post(config)
    fetch_failed = False
    fail_reason = ""
    comments: list[Any] = []
    local_comment_stats = _new_comment_persist_stats()
    observed_comment_ids: set[str] = set()
    comment_fetch_timeout_seconds = _shared_tiktok_comment_fetch_timeout_seconds(config)

    def _emit_comment_fetch_progress(payload: dict[str, Any]) -> None:
        if not progress_cb:
            return
        fetched_for_post = _normalize_non_negative_int(payload.get("comments_fetched"))
        progress_cb(
            {
                "phase": str(payload.get("phase") or "fetch_tiktok_comments"),
                "scraped_comments": _normalize_non_negative_int(counters.get("comments_fetched")) + fetched_for_post,
                "comments_upserted": _normalize_non_negative_int(counters.get("comments_upserted")),
                "current_source_id": video_id or None,
            }
        )

    try:
        with _shared_tiktok_comment_fetch_deadline(comment_fetch_timeout_seconds, video_id=video_id):
            comments = scraper.fetch_comments(
                video_id,
                username=account_handle,
                max_comments=max_comments_per_post or None,
                fetch_replies=True,
                delay=_shared_tiktok_catalog_comment_delay_seconds(),
                fast_mode=True,
                progress_callback=_emit_comment_fetch_progress,
                max_replies_per_comment=max_comments_per_post or None,
                reply_fetch_deadline_seconds=comment_fetch_timeout_seconds,
            )
        fail_reason = str(
            getattr(scraper, "last_comment_fetch_reason", "") or getattr(scraper, "_last_api_fail_reason", "") or ""
        ).strip()
        if fail_reason:
            comment_fail_reasons[fail_reason] += 1
    except _SharedTikTokCommentFetchTimeoutError:
        fetch_failed = True
        fail_reason = "comment_fetch_timeout"
        comment_fail_reasons[fail_reason] += 1
        setter = getattr(scraper, "_set_comment_failure_reason", None)
        if callable(setter):
            setter(fail_reason)
        try:
            session = getattr(scraper, "session", None)
            if session is not None and hasattr(session, "close"):
                session.close()
        except Exception:  # noqa: BLE001
            logger.debug("[tiktok] Failed to close scraper session after comment timeout", exc_info=True)
        logger.warning(
            "[tiktok] Timed out fetching shared catalog comments video_id=%s after %ss",
            video_id or "?",
            comment_fetch_timeout_seconds,
        )
    except Exception:  # noqa: BLE001
        fetch_failed = True
        fail_reason = "fetch_exception"
        comment_fail_reasons[fail_reason] += 1
        logger.exception("[tiktok] Failed to fetch shared catalog comments video_id=%s", video_id or "?")

    if fetch_failed:
        _room_callable("_shared_tiktok_increment_counter", _shared_tiktok_increment_counter)(counters, "comment_errors")
        _room_callable("_shared_tiktok_increment_counter", _shared_tiktok_increment_counter)(
            counters, "incomplete_comment_fetches"
        )
        _room_callable("_shared_tiktok_increment_counter", _shared_tiktok_increment_counter)(
            counters, "comments_mark_missing_skipped"
        )
        return materialized_row

    post_db_id = str(materialized_row.get("id") or "").strip()
    try:
        with pg.db_connection(label="tiktok-shared-catalog-comments") as conn:
            with _SavepointScope(conn=conn, prefix="tiktok_shared_catalog_comments", index=1):
                for comment in comments:
                    _upsert_tiktok_comment_tree(
                        context,
                        job_id=job_id,
                        run_id=run_id,
                        account=account_handle,
                        post_id=post_db_id,
                        comment=comment,
                        source_scope=source_scope,
                        observed_comment_ids=observed_comment_ids,
                        persist_stats=local_comment_stats,
                        conn=conn,
                    )
                fetched_count = _normalize_non_negative_int(local_comment_stats.get("comments_upserted"))
                expected_count = max(expected_comment_count, pre_fetch_active_comments)
                is_complete = _is_comment_fetch_complete(
                    fetch_failed=False,
                    fail_reason=fail_reason,
                    auth_failed=bool(getattr(scraper, "comments_auth_failed", False)),
                    fetched_count=fetched_count,
                    max_comments_per_post=max_comments_per_post,
                    expected_count=expected_count,
                )
                if is_complete:
                    missing_marked = _mark_missing_comments_for_anchor(
                        platform="tiktok",
                        anchor_id=post_db_id,
                        observed_comment_ids=observed_comment_ids,
                        conn=conn,
                    )
                    _room_callable("_shared_tiktok_increment_counter", _shared_tiktok_increment_counter)(
                        counters, "comments_marked_missing", missing_marked
                    )
                    _reconcile_post_comment_count(platform="tiktok", post_db_id=post_db_id, conn=conn)
                else:
                    _room_callable("_shared_tiktok_increment_counter", _shared_tiktok_increment_counter)(
                        counters, "incomplete_comment_fetches"
                    )
                    _room_callable("_shared_tiktok_increment_counter", _shared_tiktok_increment_counter)(
                        counters, "comments_mark_missing_skipped"
                    )
                    if pre_fetch_active_comments > 0 and fetched_count < pre_fetch_active_comments:
                        _room_callable("_shared_tiktok_increment_counter", _shared_tiktok_increment_counter)(
                            counters, "comments_mark_missing_skipped_existing_gap"
                        )
    except Exception:  # noqa: BLE001
        _room_callable("_shared_tiktok_increment_counter", _shared_tiktok_increment_counter)(counters, "comment_errors")
        _room_callable("_shared_tiktok_increment_counter", _shared_tiktok_increment_counter)(
            counters, "incomplete_comment_fetches"
        )
        _room_callable("_shared_tiktok_increment_counter", _shared_tiktok_increment_counter)(
            counters, "comments_mark_missing_skipped"
        )
        logger.exception("[tiktok] Failed to persist shared catalog comments video_id=%s", video_id or "?")
        return materialized_row

    _merge_comment_persist_stats(cast(dict[str, int], counters), local_comment_stats)
    if progress_cb:
        progress_cb(
            {
                "phase": "persist_tiktok_comments",
                "scraped_comments": _normalize_non_negative_int(counters.get("comments_fetched")),
                "comments_upserted": _normalize_non_negative_int(counters.get("comments_upserted")),
                "current_source_id": video_id or None,
            }
        )
    return materialized_row


def _scrape_shared_tiktok_posts_partitioned(
    *,
    run_id: str | None,
    account_handle: str,
    config: Mapping[str, Any],
    progress_cb: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    _sync_core_overrides()
    from trr_backend.socials.tiktok import TikTokScrapeConfig

    context = _bootstrap_shared_tiktok_account_context(
        account_handle=account_handle,
        delay_seconds=0.35,
        direct_comment_api_enabled_override=_shared_tiktok_catalog_direct_comment_override(config),
    )
    scraper = context["scraper"]
    scrape_config = TikTokScrapeConfig(username=account_handle, hashtags=[], delay_seconds=0.35, max_pages=None)
    profile_snapshot = _merge_tiktok_profile_snapshots(
        context.get("profile_snapshot"),
        _metadata_dict(config.get("profile_snapshot")),
    )
    sec_uid = str(config.get("sec_uid") or profile_snapshot.get("sec_uid") or context.get("sec_uid") or "").strip()
    if not sec_uid:
        raise ValueError(f"Missing TikTok sec_uid partition metadata for @{account_handle}")
    cursor = max(0, int(config.get("cursor_start") or 0))
    end_cursor = max(0, int(config.get("cursor_end") or 0))
    total_posts = max(
        _normalize_non_negative_int(config.get("discovery_total_posts")),
        _normalize_non_negative_int(profile_snapshot.get("total_posts")),
        _normalize_non_negative_int(context.get("total_posts")),
    )
    last_cursor = cursor
    pages_scanned = 0
    posts_checked = 0
    rows: list[dict[str, Any]] = []
    persist_counters: dict[str, int] = {
        "posts_upserted": 0,
        "comments_upserted": 0,
        "materialized_posts_upserted": 0,
        "media_mirror_jobs_enqueued": 0,
    }
    comment_fail_reasons: Counter[str] = Counter()
    seen_cursors: set[int] = set()
    partition_completed = end_cursor <= 0
    error_code: str | None = None

    while True:
        pages_scanned += 1
        data = scraper.fetch_posts(account_handle, sec_uid, cursor, scrape_config.delay_seconds)
        if not data:
            if end_cursor > 0 and last_cursor != end_cursor:
                error_code = "tiktok_partition_cursor_end_not_reached"
            break
        items = list(data.get("itemList") or [])
        if not items:
            if end_cursor > 0 and last_cursor != end_cursor:
                error_code = "tiktok_partition_empty_page_before_cursor_end"
            break
        posts_checked += len(items)
        for item in items:
            post = _apply_tiktok_profile_snapshot_to_post(
                scraper._parse_post_item(item, scrape_config), profile_snapshot
            )
            row = _upsert_shared_catalog_post(
                platform="tiktok",
                run_id=run_id,
                account_handle=account_handle,
                post=post,
            )
            if row:
                rows.append(row)
                persist_counters["posts_upserted"] = len(rows)
                _room_callable("_hydrate_shared_tiktok_catalog_post", _hydrate_shared_tiktok_catalog_post)(
                    scraper=scraper,
                    run_id=run_id,
                    account_handle=account_handle,
                    config=config,
                    job_id=str(config.get("job_id") or "").strip(),
                    post=post,
                    catalog_row=row,
                    counters=persist_counters,
                    comment_fail_reasons=comment_fail_reasons,
                    progress_cb=progress_cb,
                )
        if progress_cb:
            progress_cb(
                {
                    "phase": "scrape_partition_page",
                    "pages_scanned": pages_scanned,
                    "posts_checked": posts_checked,
                    "matched_posts": len(rows),
                    "saved_posts": len(rows),
                    "total_posts": total_posts or None,
                }
            )
        next_cursor = int(data.get("cursor") or 0)
        has_more = bool(data.get("hasMore"))
        if end_cursor > 0 and next_cursor == end_cursor:
            partition_completed = True
            break
        if not has_more or next_cursor <= 0 or next_cursor in seen_cursors:
            if end_cursor > 0 and next_cursor != end_cursor:
                error_code = "tiktok_partition_cursor_end_not_reached"
            else:
                partition_completed = True
            break
        cursor = next_cursor
        last_cursor = cursor
        seen_cursors.add(cursor)
    retrieval_meta = {
        "retrieval_mode": "api_partition",
        "pages_scanned": pages_scanned,
        "posts_checked": posts_checked,
        "total_posts": total_posts or None,
        "partition_strategy": CATALOG_FULL_HISTORY_CURSOR_PARTITION_STRATEGY,
        "persist_counters": {
            **persist_counters,
            "posts_upserted": len(rows),
            "comments_upserted": _normalize_non_negative_int(persist_counters.get("comments_upserted")),
        },
        "profile_snapshot": profile_snapshot,
    }
    if comment_fail_reasons:
        retrieval_meta["comment_fetch_failures_by_reason"] = dict(comment_fail_reasons)
    for key in (
        "comments_fetched",
        "comments_marked_missing",
        "comments_mark_missing_skipped",
        "incomplete_comment_fetches",
        "comment_errors",
        "comment_posts_skipped_complete",
        "comment_posts_skipped_missing_materialized_post",
        "comments_mark_missing_skipped_existing_gap",
        "comment_media_mirror_jobs_enqueued",
        "comment_media_mirror_job_enqueue_errors",
        "materialized_post_errors",
    ):
        if _normalize_non_negative_int(persist_counters.get(key)):
            retrieval_meta[key] = _normalize_non_negative_int(persist_counters.get(key))
    endpoint_responses = _metadata_dict(
        _metadata_dict(getattr(scraper, "last_retrieval_meta", {}) or {}).get("endpoint_responses")
    )
    if endpoint_responses:
        retrieval_meta["endpoint_responses"] = endpoint_responses
    if error_code and not partition_completed:
        retrieval_meta["error_code"] = error_code
        retrieval_meta["error_class"] = "PartitionBoundaryNotReached"
        retrieval_meta["retryable"] = True
    return rows, retrieval_meta


def _scrape_shared_tiktok_posts(
    *,
    run_id: str | None,
    account_handle: str,
    config: Mapping[str, Any],
    job_id: str,
    progress_cb: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    _sync_core_overrides()
    from trr_backend.socials.tiktok import TikTokScrapeConfig, TikTokScraper

    if _shared_catalog_mode(config):
        from trr_backend.socials.tiktok.shared_catalog.public_listing import scrape_catalog

        return scrape_catalog(
            core=_core,
            hydrate=_room_callable("_hydrate_shared_tiktok_catalog_post", _hydrate_shared_tiktok_catalog_post),
            run_id=run_id,
            account_handle=account_handle,
            config=config,
            job_id=job_id,
            progress_cb=progress_cb,
        )

    scraper_kwargs: dict[str, Any] = {"cookies": _load_tiktok_cookies()}
    direct_comment_override = _shared_tiktok_catalog_direct_comment_override(config)
    if direct_comment_override is not None:
        scraper_kwargs["direct_comment_api_enabled_override"] = direct_comment_override
    scraper = TikTokScraper(**scraper_kwargs)
    expected_total_posts = max(
        _normalize_non_negative_int(config.get("discovery_total_posts")),
        _shared_account_expected_total_posts_from_config(
            config,
            platform="tiktok",
            account_handle=account_handle,
        ),
    )
    scrape_config = TikTokScrapeConfig(
        username=account_handle,
        hashtags=[],
        date_start=_coerce_dt(config.get("date_start")),
        date_end=_coerce_dt(config.get("date_end")),
        delay_seconds=0.35,
        max_pages=_shared_stage_post_limit(config),
        scrape_mode="ytdlp",
        ytdlp_max_videos_hint=expected_total_posts or None,
    )
    posts = scraper.scrape(scrape_config, progress_cb=progress_cb)
    retrieval_meta = dict(getattr(scraper, "last_retrieval_meta", {}) or {})
    profile_snapshot = _merge_tiktok_profile_snapshots(
        _metadata_dict(retrieval_meta).get("profile_snapshot"),
        _metadata_dict(config.get("profile_snapshot")),
    )
    rows: list[dict[str, Any]] = []
    if _shared_catalog_mode(config):
        persist_counters: dict[str, int] = {
            "posts_upserted": 0,
            "comments_upserted": 0,
            "materialized_posts_upserted": 0,
            "media_mirror_jobs_enqueued": 0,
        }
        comment_fail_reasons: Counter[str] = Counter()
        progress_interval = _shared_catalog_progress_interval_posts()
        pages_scanned = _normalize_non_negative_int(retrieval_meta.get("pages_scanned"))
        posts_checked = max(_normalize_non_negative_int(retrieval_meta.get("posts_checked")), len(posts))
        matched_posts = len(posts)
        total_posts = retrieval_meta.get("total_posts")
        posts, gap_priority_meta = _prioritize_shared_tiktok_posts_for_comment_gaps(
            posts,
            account_handle=account_handle,
            config=config,
        )
        if gap_priority_meta.get("enabled"):
            retrieval_meta["comment_gap_priority"] = gap_priority_meta

        def _emit_persist_progress(saved_posts: int, *, force: bool = False) -> None:
            if not progress_cb:
                return
            if not force and saved_posts > 0 and saved_posts % progress_interval != 0:
                return
            payload: dict[str, Any] = {
                "phase": "persist_catalog_posts",
                "pages_scanned": pages_scanned,
                "posts_checked": posts_checked,
                "matched_posts": matched_posts,
                "saved_posts": max(0, int(saved_posts)),
            }
            if total_posts is not None:
                payload["total_posts"] = _normalize_non_negative_int(total_posts)
            progress_cb(payload)

        _emit_persist_progress(0, force=True)
        for index, post in enumerate(posts, start=1):
            _apply_tiktok_profile_snapshot_to_post(post, profile_snapshot)
            row = _upsert_shared_catalog_post(
                platform="tiktok",
                run_id=run_id,
                account_handle=account_handle,
                post=post,
            )
            if row:
                rows.append(row)
                persist_counters["posts_upserted"] = len(rows)
                _room_callable("_hydrate_shared_tiktok_catalog_post", _hydrate_shared_tiktok_catalog_post)(
                    scraper=scraper,
                    run_id=run_id,
                    account_handle=account_handle,
                    config=config,
                    job_id=job_id,
                    post=post,
                    catalog_row=row,
                    counters=persist_counters,
                    comment_fail_reasons=comment_fail_reasons,
                    progress_cb=progress_cb,
                )
            _emit_persist_progress(len(rows), force=index == len(posts))
        retrieval_meta["persist_counters"] = {
            **persist_counters,
            "posts_upserted": len(rows),
            "comments_upserted": _normalize_non_negative_int(persist_counters.get("comments_upserted")),
        }
        if comment_fail_reasons:
            retrieval_meta["comment_fetch_failures_by_reason"] = dict(comment_fail_reasons)
        for key in (
            "comments_fetched",
            "comments_marked_missing",
            "comments_mark_missing_skipped",
            "incomplete_comment_fetches",
            "comment_errors",
            "comment_posts_skipped_missing_materialized_post",
            "comment_media_mirror_jobs_enqueued",
            "comment_media_mirror_job_enqueue_errors",
            "materialized_post_errors",
        ):
            if _normalize_non_negative_int(persist_counters.get(key)):
                retrieval_meta[key] = _normalize_non_negative_int(persist_counters.get(key))
    else:
        for post in posts:
            _apply_tiktok_profile_snapshot_to_post(post, profile_snapshot)
            row = _upsert_tiktok_post(None, job_id=job_id, account=account_handle, post=post)
            if row:
                rows.append(row)
    retrieval_meta["profile_snapshot"] = _merge_tiktok_profile_snapshots(
        _metadata_dict(retrieval_meta.get("profile_snapshot")),
        profile_snapshot,
    )
    retrieval_meta["total_posts"] = max(
        _normalize_non_negative_int(retrieval_meta.get("total_posts")),
        _normalize_non_negative_int(_metadata_dict(retrieval_meta.get("profile_snapshot")).get("total_posts")),
        len(rows),
    )
    return rows, retrieval_meta


_LOCAL_ROOM_FUNCTIONS.update({_name: globals()[_name] for _name in _LOCAL_ROOM_NAMES})
__all__ = [
    "_apply_shared_tiktok_catalog_assignment_to_post",
    "_hydrate_shared_tiktok_catalog_post",
    "_scrape_shared_tiktok_posts",
    "_scrape_shared_tiktok_posts_partitioned",
    "_shared_tiktok_catalog_assignment_ids",
    "_shared_tiktok_catalog_context_from_row",
    "_shared_tiktok_increment_counter",
]
