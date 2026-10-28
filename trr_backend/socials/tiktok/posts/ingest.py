# ruff: noqa: F821, UP037
"""TikTok season ingest stage (``_ingest_tiktok``): scrape posts and sounds for a season
account, persist posts/comments, run enrichment, and report stage metadata.

Moved out of ``social_season_analytics_impl``; the legacy repository keeps a
same-named wrapper that delegates through ``_LOCAL_ROOM_FUNCTIONS``. Repository
helpers are resolved through the published provider namespace at call time.
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime
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
    from trr_backend.socials.model_types import IngestOptions, SeasonContext

    # Repository names resolved through the published provider namespace.
    class CommentLifecycleSnapshot: ...

    class _SavepointScope:
        def __init__(self, *, conn: Any | None, prefix: str, index: int) -> None: ...
        def __enter__(self) -> None: ...
        def __exit__(self, exc_type: Any, exc: Any, _tb: Any) -> bool: ...

    def _as_text_list(*args: Any, **kwargs: Any) -> Any: ...
    def _build_existing_post_lookup(*args: Any, **kwargs: Any) -> Any: ...
    def _build_post_filter_text(*args: Any, **kwargs: Any) -> Any: ...
    def _build_progress_activity_payload(*args: Any, **kwargs: Any) -> Any: ...
    def _build_tiktok_sound_url(*args: Any, **kwargs: Any) -> Any: ...
    def _coerce_dt(*args: Any, **kwargs: Any) -> Any: ...
    def _comment_stats_payload(*args: Any, **kwargs: Any) -> Any: ...
    def _compute_tiktok_post_quality(*args: Any, **kwargs: Any) -> Any: ...
    def _compute_tiktok_velocity_metrics(*args: Any, **kwargs: Any) -> Any: ...
    def _decide_comment_refresh(*args: Any, **kwargs: Any) -> Any: ...
    def _effective_reply_limit(*args: Any, **kwargs: Any) -> Any: ...
    def _emit_job_progress(*args: Any, **kwargs: Any) -> Any: ...
    def _enqueue_platform_media_mirror_job(*args: Any, **kwargs: Any) -> Any: ...
    def _expected_comment_count_for_platform(*args: Any, **kwargs: Any) -> Any: ...
    def _extract_cast_mentions_from_text(*args: Any, **kwargs: Any) -> Any: ...
    def _extract_tiktok_sound_fields(*args: Any, **kwargs: Any) -> Any: ...
    def _fetch_tiktok_sound_snapshot(*args: Any, **kwargs: Any) -> Any: ...
    def _is_comment_fetch_complete(*args: Any, **kwargs: Any) -> Any: ...
    def _iso(*args: Any, **kwargs: Any) -> Any: ...
    def _load_comment_lifecycle_snapshots(*args: Any, **kwargs: Any) -> Any: ...
    def _load_existing_posts(*args: Any, **kwargs: Any) -> Any: ...
    def _load_ingest_checkpoint(*args: Any, **kwargs: Any) -> Any: ...
    def _load_show_cast_people(*args: Any, **kwargs: Any) -> Any: ...
    def _load_tiktok_cookies(*args: Any, **kwargs: Any) -> Any: ...
    def _mark_missing_comments_for_anchor(*args: Any, **kwargs: Any) -> Any: ...
    def _merge_comment_persist_stats(*args: Any, **kwargs: Any) -> Any: ...
    def _new_comment_persist_stats(*args: Any, **kwargs: Any) -> Any: ...
    def _new_job_progress_state(*args: Any, **kwargs: Any) -> Any: ...
    def _normalize_account_handle(*args: Any, **kwargs: Any) -> Any: ...
    def _normalize_non_negative_int(*args: Any, **kwargs: Any) -> Any: ...
    def _normalize_tiktok_sound_id(*args: Any, **kwargs: Any) -> Any: ...
    def _normalize_tiktok_sound_ids(*args: Any, **kwargs: Any) -> Any: ...
    def _now_utc(*args: Any, **kwargs: Any) -> Any: ...

    _parse_tiktok_time: Any

    def _persist_tiktok_comment_enrichment(*args: Any, **kwargs: Any) -> Any: ...
    def _persist_tiktok_post_enrichment(*args: Any, **kwargs: Any) -> Any: ...
    def _persist_tiktok_sound_snapshot(*args: Any, **kwargs: Any) -> Any: ...
    def _reconcile_post_comment_count(*args: Any, **kwargs: Any) -> Any: ...
    def _resolve_platform_delay_seconds(*args: Any, **kwargs: Any) -> Any: ...
    def _run_existing_post_details_refresh(*args: Any, **kwargs: Any) -> Any: ...
    def _save_ingest_checkpoint(*args: Any, **kwargs: Any) -> Any: ...
    def _target_source_ids_for_platform(*args: Any, **kwargs: Any) -> Any: ...
    def _text_contains_any_term(*args: Any, **kwargs: Any) -> Any: ...
    def _tiktok_catalog_comment_override_enabled(*args: Any, **kwargs: Any) -> Any: ...
    def _touch_job_heartbeat(*args: Any, **kwargs: Any) -> Any: ...
    def _trim_nested_comment_replies(*args: Any, **kwargs: Any) -> Any: ...
    def _upsert_tiktok_comment_tree(*args: Any, **kwargs: Any) -> Any: ...
    def _upsert_tiktok_post(*args: Any, **kwargs: Any) -> Any: ...

    logger: Any


# Room names are declared before the provider is built so that a late
# ``adopt_published`` (leaf imported after the repository published) still
# captures the repository wrappers for ``_room_callable``.
_LOCAL_ROOM_NAMES: set[str] = {
    "_ingest_tiktok",
}
_IMPORTED_CORE_NAMES: set[str] = set()
_LOCAL_ROOM_FUNCTIONS: dict[str, Any] = {}
_CORE_ROOM_WRAPPERS: dict[str, Any] = {}


_PROVIDER = LateNamespaceProvider(
    globals(),
    prefix="TIKTOK_POSTS_INGEST_PROVIDER",
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


def _ingest_tiktok(
    context: SeasonContext,
    *,
    run_id: str | None,
    account: str,
    hashtags: list[str],
    keywords: list[str],
    opts: IngestOptions,
    job_id: str,
    stage: str = "posts",
    job_config: dict[str, Any] | None = None,
) -> tuple[int, int, dict[str, Any]]:
    _sync_core_overrides()
    from trr_backend.socials.tiktok import TikTokScrapeConfig, TikTokScraper

    tiktok_cookies = _load_tiktok_cookies()
    direct_comment_api_override = (
        bool((job_config or {}).get("tiktok_direct_comment_api_override"))
        if _tiktok_catalog_comment_override_enabled()
        else False
    )
    scraper = TikTokScraper(
        cookies=tiktok_cookies,
        direct_comment_api_enabled_override=direct_comment_api_override if direct_comment_api_override else None,
    )

    retrieval_meta: dict[str, Any] = {
        "tiktok_direct_comment_api_override": direct_comment_api_override,
        "tiktok_direct_comment_api_override_enabled": _tiktok_catalog_comment_override_enabled(),
    }
    # Resolve comment availability once per stage. When the direct comment API
    # is parked, every fetch_comments call returns [] with "direct_api_parked";
    # calling it per post counted every post as a comment error, marked it
    # incomplete, and re-read all stored comments for enrichment.
    comments_api_check = getattr(scraper, "_direct_comment_api_experiment_enabled", None)
    tiktok_comments_available = bool(comments_api_check()) if callable(comments_api_check) else True
    if not tiktok_comments_available:
        retrieval_meta["comments_skipped_reason"] = "direct_api_parked"
    post_upsert_errors = 0
    normalized_creator_id = _normalize_account_handle(account) or str(account or "").strip().lower()
    requested_sound_ids = _normalize_tiktok_sound_ids(opts.sound_ids or [])
    discovered_sound_ids: set[str] = set()
    sound_pages_attempted = 0
    sound_pages_succeeded = 0
    sound_posts_ingested = 0
    latest_posted_at_seen: datetime | None = None
    checkpoint_cursor: str | None = None
    checkpoint_last_posted_at: datetime | None = None
    skipped_checkpoint = 0
    cast_people = _load_show_cast_people(context.show_id)
    cast_names = [str(item.get("name") or "").strip() for item in cast_people if str(item.get("name") or "").strip()]
    if stage == "posts" and opts.sync_strategy == "incremental" and normalized_creator_id:
        checkpoint = _load_ingest_checkpoint(platform="tiktok", creator_id=normalized_creator_id) or {}
        checkpoint_cursor = str(checkpoint.get("cursor") or "").strip() or None
        checkpoint_last_posted_at = _coerce_dt(checkpoint.get("last_posted_at"))
        retrieval_meta["checkpoint"] = {
            "platform": "tiktok",
            "creator_id": normalized_creator_id,
            "cursor": checkpoint_cursor,
            "last_posted_at": _iso(checkpoint_last_posted_at),
        }
    matched_post_count = 0
    scraped_post_count = 0
    scraped_comment_count = 0
    comment_errors = 0
    comment_fail_reasons: set[str] = set()
    comment_refresh_reasons: Counter[str] = Counter()
    missing_marked = 0
    incomplete_comment_fetches = 0
    comments_mark_missing_skipped = 0
    mirror_jobs_enqueued = 0
    mirror_job_enqueue_errors = 0
    comment_stats = _new_comment_persist_stats()
    skipped_keyword = 0
    total_scraped = 0
    progress_state = _new_job_progress_state()
    activity: dict[str, Any] = {
        "phase": f"{stage}_start",
        "pages_scanned": 0,
        "posts_checked": 0,
        "matched_posts": 0,
    }

    def _flush_progress(*, force: bool = False) -> None:
        _emit_job_progress(
            job_id=job_id,
            stage=stage,
            platform="tiktok",
            account=account,
            scraped_posts=scraped_post_count,
            scraped_comments=scraped_comment_count,
            posts_upserted=matched_post_count,
            comments_upserted=int(comment_stats.get("comments_upserted") or 0),
            activity=activity,
            progress_state=progress_state,
            force=force,
        )

    _flush_progress(force=True)

    if stage == "posts" and opts.ingest_mode == "details_refresh":
        return _run_existing_post_details_refresh(
            context,
            platform="tiktok",
            run_id=run_id,
            account=account,
            opts=opts,
            job_id=job_id,
            activity=activity,
            flush_progress=_flush_progress,
        )

    if stage == "comments" and not tiktok_comments_available:
        skipped_synced = 0
        retrieval_meta["source"] = "db"
        logger.info("[tiktok] Comments stage skipped for account=%s: direct comment API is parked", account)
    elif stage == "comments":
        target_source_ids = _target_source_ids_for_platform(opts, "tiktok")
        existing_posts = _load_existing_posts(
            "tiktok",
            context,
            account,
            opts.date_start,
            opts.date_end,
            source_ids=target_source_ids,
        )
        logger.info("[tiktok] Comments stage: %d existing posts from DB for account=%s", len(existing_posts), account)
        retrieval_meta["source"] = "db"
        snapshots = _load_comment_lifecycle_snapshots(
            [str(r["id"]) for r in existing_posts if r.get("id")],
            platform="tiktok",
        )
        skipped_synced = 0
        with pg.db_connection() as comments_conn:
            for post_index, row in enumerate(existing_posts, start=1):
                video_id = str(row.get("video_id") or "")
                post_db_id = str(row.get("id") or "")
                if not video_id:
                    continue
                expected = int(row.get("comments_count") or 0)
                decision = _decide_comment_refresh(
                    sync_strategy=opts.sync_strategy,
                    expected_count=expected,
                    snapshot=snapshots.get(post_db_id),
                    post_published_at=_coerce_dt(row.get("posted_at")),
                    refresh_policy=opts.comment_refresh_policy,
                )
                comment_refresh_reasons[decision.reason] += 1
                if not decision.should_refresh:
                    skipped_synced += 1
                    continue
                scraped_post_count += 1
                activity["phase"] = "comments_fetch"
                activity["posts_checked"] = scraped_post_count
                activity["matched_posts"] = matched_post_count
                if scraped_post_count % 10 == 0:
                    _touch_job_heartbeat(job_id)
                _flush_progress()
                fetch_failed = False
                comments: list[Any] = []
                fail_reason = ""
                observed_comment_ids: set[str] = set()
                local_comment_stats = _new_comment_persist_stats()
                try:
                    comments = scraper.fetch_comments(
                        video_id,
                        username=account,
                        max_comments=opts.max_comments_per_post,
                        fetch_replies=opts.fetch_replies,
                        delay=0.5,
                        max_replies_per_comment=_effective_reply_limit(opts),
                    )
                    _trim_nested_comment_replies(comments, max_replies_per_post=_effective_reply_limit(opts))
                    fail_reason = str(
                        getattr(scraper, "last_comment_fetch_reason", "")
                        or getattr(scraper, "_last_api_fail_reason", "")
                        or ""
                    )
                    if fail_reason:
                        comment_fail_reasons.add(fail_reason)
                        if not comments:
                            comment_errors += 1
                except Exception:
                    fetch_failed = True
                    comment_errors += 1
                    logger.exception(
                        "[tiktok] Failed to fetch comments for post %s (video_id=%s)",
                        post_db_id,
                        video_id,
                    )

                if not fetch_failed:
                    try:
                        with _SavepointScope(conn=comments_conn, prefix="tiktok_comments_stage", index=post_index):
                            for comment in comments:
                                _upsert_tiktok_comment_tree(
                                    context,
                                    job_id=job_id,
                                    run_id=run_id,
                                    account=account,
                                    post_id=post_db_id,
                                    comment=comment,
                                    source_scope=opts.source_scope,
                                    observed_comment_ids=observed_comment_ids,
                                    persist_stats=local_comment_stats,
                                    conn=comments_conn,
                                )

                            is_complete = _is_comment_fetch_complete(
                                fetch_failed=False,
                                fail_reason=fail_reason,
                                auth_failed=bool(getattr(scraper, "comments_auth_failed", False)),
                                fetched_count=len(comments),
                                max_comments_per_post=opts.max_comments_per_post,
                            )
                            if is_complete:
                                missing_marked += _mark_missing_comments_for_anchor(
                                    platform="tiktok",
                                    anchor_id=post_db_id,
                                    observed_comment_ids=observed_comment_ids,
                                    conn=comments_conn,
                                )
                                _reconcile_post_comment_count(
                                    platform="tiktok",
                                    post_db_id=post_db_id,
                                    conn=comments_conn,
                                )
                            else:
                                incomplete_comment_fetches += 1
                                comments_mark_missing_skipped += 1
                    except Exception:
                        fetch_failed = True
                        comment_errors += 1
                        logger.exception(
                            "[tiktok] Failed to persist comments for post %s (video_id=%s)",
                            post_db_id,
                            video_id,
                        )

                if fetch_failed:
                    incomplete_comment_fetches += 1
                    comments_mark_missing_skipped += 1
                    continue

                matched_post_count += 1
                activity["matched_posts"] = matched_post_count
                _merge_comment_persist_stats(comment_stats, local_comment_stats)
                scraped_comment_count = int(comment_stats.get("comments_fetched") or 0)
                _flush_progress()
        if skipped_synced:
            logger.info("[tiktok] Skipped %d posts whose comments are already synced", skipped_synced)
    else:
        effective_date_start = opts.date_start
        if checkpoint_last_posted_at is not None and (
            effective_date_start is None or checkpoint_last_posted_at > effective_date_start
        ):
            # Only use the checkpoint if it falls within the requested date range.
            # If the checkpoint is beyond opts.date_end (e.g. a newer week was synced
            # previously), honour the explicit date_start so we actually scrape posts
            # in the requested window instead of returning nothing.
            if opts.date_end is None or checkpoint_last_posted_at <= opts.date_end:
                effective_date_start = checkpoint_last_posted_at
            else:
                logger.info(
                    "[tiktok] Ignoring checkpoint (last_posted_at=%s) because it is "
                    "beyond the requested date_end=%s; using date_start=%s",
                    checkpoint_last_posted_at,
                    opts.date_end,
                    opts.date_start,
                )
        config = TikTokScrapeConfig(
            username=account,
            hashtags=hashtags,
            date_start=effective_date_start,
            date_end=opts.date_end,
            delay_seconds=_resolve_platform_delay_seconds(
                platform="tiktok",
                attempt_count=int((job_config or {}).get("_attempt_count") or 1),
                last_error_code=(job_config or {}).get("_last_error_code"),
                default=0.35,
            ),
            max_pages=None,
            scrape_mode="ytdlp",
            show_id=cast(Any, context.show_id),
            season_number=context.season_number,
        )
        logger.info("[tiktok] Scraping account=%s date_range=%s..%s", account, effective_date_start, opts.date_end)
        post_limit = opts.max_posts_per_target if opts.max_posts_per_target > 0 else None

        def _on_scrape_progress(payload: dict[str, Any]) -> None:
            nonlocal scraped_post_count
            activity["phase"] = str(payload.get("phase") or "scrape_posts")
            activity["pages_scanned"] = _normalize_non_negative_int(payload.get("pages_scanned"))
            activity["posts_checked"] = _normalize_non_negative_int(payload.get("posts_checked"))
            if payload.get("matched_posts") is not None:
                activity["matched_posts"] = _normalize_non_negative_int(payload.get("matched_posts"))
            scraped_post_count = max(scraped_post_count, _normalize_non_negative_int(payload.get("posts_checked")))
            _flush_progress()

        posts = scraper.scrape(config, progress_cb=_on_scrape_progress)
        retrieval_meta.update(dict(getattr(scraper, "last_retrieval_meta", {}) or {}))

        existing_post_lookup: dict[str, dict[str, Any]] = {}
        snapshots: dict[str, CommentLifecycleSnapshot] = {}
        if opts.sync_strategy == "incremental" and opts.max_comments_per_post > 0:
            existing_rows = _load_existing_posts("tiktok", context, account, opts.date_start, opts.date_end)
            existing_post_lookup = _build_existing_post_lookup("tiktok", existing_rows)
            snapshots = _load_comment_lifecycle_snapshots(
                [str(row.get("id")) for row in existing_post_lookup.values() if row.get("id")],
                platform="tiktok",
            )

        skipped_synced = 0
        with pg.db_connection() as post_comments_conn:
            for post_index, post in enumerate(posts, start=1):
                total_scraped += 1
                scraped_post_count = max(scraped_post_count, total_scraped)
                activity["phase"] = "posts_filter"
                activity["posts_checked"] = scraped_post_count
                post_created_at = _parse_tiktok_time(getattr(post, "create_time", None))
                if (
                    checkpoint_last_posted_at
                    and post_created_at
                    and post_created_at <= checkpoint_last_posted_at
                    and (opts.date_end is None or checkpoint_last_posted_at <= opts.date_end)
                ):
                    skipped_checkpoint += 1
                    _flush_progress()
                    continue
                description = str(getattr(post, "description", "") or "")
                filter_text = _build_post_filter_text(
                    primary_text=description,
                    hashtags=_as_text_list(getattr(post, "hashtags", []), strip_prefix="#"),
                    mentions=_as_text_list(getattr(post, "mentions", []), prefix="@", strip_prefix="@"),
                )
                if not _text_contains_any_term(text=filter_text, hashtags=hashtags, keywords=keywords):
                    skipped_keyword += 1
                    _flush_progress()
                    continue
                if post_limit and matched_post_count >= post_limit:
                    break

                existing_row = existing_post_lookup.get(str(getattr(post, "video_id", "") or ""))
                if existing_row and opts.max_comments_per_post > 0:
                    expected_comment_count = _expected_comment_count_for_platform(
                        "tiktok",
                        {"comments_count": int(getattr(post, "comments", 0) or 0)},
                    )
                    decision = _decide_comment_refresh(
                        sync_strategy=opts.sync_strategy,
                        expected_count=expected_comment_count,
                        snapshot=snapshots.get(str(existing_row.get("id") or "")),
                        post_published_at=_coerce_dt(existing_row.get("posted_at")),
                        refresh_policy=opts.comment_refresh_policy,
                    )
                    comment_refresh_reasons[decision.reason] += 1
                    if not decision.should_refresh:
                        skipped_synced += 1
                        activity["phase"] = "posts_skip_up_to_date"
                        _flush_progress()
                        continue

                upserted: dict[str, Any] | None = None
                try:
                    with pg.db_connection() as conn:
                        upserted = _upsert_tiktok_post(context, job_id=job_id, account=account, post=post, conn=conn)
                        if upserted:
                            hashtag_terms = _as_text_list(getattr(post, "hashtags", []), strip_prefix="#")
                            mention_terms = _as_text_list(getattr(post, "mentions", []), prefix="@", strip_prefix="@")
                            cast_mentions = _extract_cast_mentions_from_text(
                                _build_post_filter_text(
                                    primary_text=description,
                                    hashtags=hashtag_terms,
                                    mentions=mention_terms,
                                ),
                                cast_people,
                            )
                            quality_score, missing_fields, quality_flags = _compute_tiktok_post_quality(
                                description=description,
                                thumbnail_url=str(getattr(post, "thumbnail_url", "") or "").strip() or None,
                                hashtags=hashtag_terms,
                                mentions=mention_terms,
                                media_urls=[
                                    str(url).strip()
                                    for url in (getattr(post, "media_urls", []) or [])
                                    if str(url).strip()
                                ],
                                duration_seconds=_normalize_non_negative_int(getattr(post, "duration", 0)),
                            )
                            velocity_1h, velocity_24h, velocity_7d = _compute_tiktok_velocity_metrics(
                                views=_normalize_non_negative_int(getattr(post, "views", 0)),
                                posted_at=post_created_at,
                            )
                            _persist_tiktok_post_enrichment(
                                context=context,
                                run_id=run_id,
                                post_row=upserted,
                                description=description,
                                hashtags=hashtag_terms,
                                mentions=mention_terms,
                                keywords=keywords,
                                cast_mentions=cast_mentions,
                                quality_score=quality_score,
                                quality_missing_fields=missing_fields,
                                quality_flags=quality_flags,
                                velocity_1h=velocity_1h,
                                velocity_24h=velocity_24h,
                                velocity_7d=velocity_7d,
                                conn=conn,
                            )

                            raw_data = post.to_dict() if hasattr(post, "to_dict") else {}
                            if not isinstance(raw_data, dict):
                                raw_data = {}
                            sound_fields = _extract_tiktok_sound_fields(raw_data, post)
                            sound_id = str(sound_fields.get("sound_id") or "").strip()
                            if sound_id:
                                discovered_sound_ids.add(sound_id)
                                _persist_tiktok_sound_snapshot(
                                    sound_id=sound_id,
                                    snapshot={
                                        "title": sound_fields.get("sound_title"),
                                        "artist_name": sound_fields.get("sound_author"),
                                        "usage_count": _normalize_non_negative_int(
                                            sound_fields.get("sound_usage_count")
                                        ),
                                        "source_url": _build_tiktok_sound_url(sound_id),
                                        "posts": [
                                            {
                                                "platform_post_id": str(getattr(post, "video_id", "") or "").strip(),
                                                "creator_handle": str(getattr(post, "username", "") or "").strip()
                                                or None,
                                                "posted_at": _iso(post_created_at),
                                                "views": _normalize_non_negative_int(getattr(post, "views", 0)),
                                                "likes": _normalize_non_negative_int(getattr(post, "likes", 0)),
                                                "comments": _normalize_non_negative_int(getattr(post, "comments", 0)),
                                                "shares": _normalize_non_negative_int(getattr(post, "shares", 0)),
                                                "thumbnail_url": str(getattr(post, "thumbnail_url", "") or "").strip()
                                                or None,
                                                "caption": description,
                                            }
                                        ],
                                    },
                                    conn=conn,
                                )
                except Exception:
                    post_upsert_errors += 1
                    logger.exception(
                        "[tiktok] Failed to upsert post for video_id=%s",
                        getattr(post, "video_id", "?"),
                    )
                    _flush_progress()
                    continue

                if not upserted:
                    _flush_progress()
                    continue
                matched_post_count += 1
                if post_created_at and (latest_posted_at_seen is None or post_created_at > latest_posted_at_seen):
                    latest_posted_at_seen = post_created_at
                activity["phase"] = "posts_upsert"
                activity["matched_posts"] = matched_post_count
                _flush_progress()

                if stage == "posts":
                    try:
                        mirror_job_id = _enqueue_platform_media_mirror_job(
                            context,
                            platform="tiktok",
                            run_id=run_id,
                            source_scope=opts.source_scope,
                            account=account,
                            post_row=upserted,
                            week_index=None,
                            parent_job_id=job_id,
                            conn=None,
                        )
                        if mirror_job_id:
                            mirror_jobs_enqueued += 1
                    except Exception:  # noqa: BLE001
                        mirror_job_enqueue_errors += 1
                        logger.exception(
                            "[tiktok] Failed to enqueue media mirror job for post=%s video_id=%s",
                            str(upserted.get("id") or ""),
                            str(getattr(post, "video_id", "") or ""),
                        )

                if opts.max_comments_per_post > 0 and tiktok_comments_available:
                    comments: list[Any] = []
                    fail_reason = ""
                    fetch_failed = False
                    observed_comment_ids: set[str] = set()
                    local_comment_stats = _new_comment_persist_stats()
                    try:
                        comments = scraper.fetch_comments(
                            getattr(post, "video_id", ""),
                            username=account,
                            max_comments=opts.max_comments_per_post,
                            fetch_replies=opts.fetch_replies,
                            delay=0.5,
                            max_replies_per_comment=_effective_reply_limit(opts),
                        )
                        _trim_nested_comment_replies(comments, max_replies_per_post=_effective_reply_limit(opts))
                        fail_reason = str(
                            getattr(scraper, "last_comment_fetch_reason", "")
                            or getattr(scraper, "_last_api_fail_reason", "")
                            or ""
                        )
                        if fail_reason:
                            comment_fail_reasons.add(fail_reason)
                            if not comments:
                                comment_errors += 1
                    except Exception:
                        fetch_failed = True
                        comment_errors += 1
                        logger.exception(
                            "[tiktok] Failed to fetch comments for post %s (video_id=%s)",
                            upserted.get("id"),
                            getattr(post, "video_id", "?"),
                        )
                        incomplete_comment_fetches += 1
                        comments_mark_missing_skipped += 1

                    if not fetch_failed:
                        try:
                            with _SavepointScope(
                                conn=post_comments_conn,
                                prefix="tiktok_post_comments",
                                index=post_index,
                            ):
                                for comment in comments:
                                    _upsert_tiktok_comment_tree(
                                        context,
                                        job_id=job_id,
                                        run_id=run_id,
                                        account=account,
                                        post_id=str(upserted["id"]),
                                        comment=comment,
                                        source_scope=opts.source_scope,
                                        observed_comment_ids=observed_comment_ids,
                                        persist_stats=local_comment_stats,
                                        conn=post_comments_conn,
                                    )

                                is_complete = _is_comment_fetch_complete(
                                    fetch_failed=False,
                                    fail_reason=fail_reason,
                                    auth_failed=bool(getattr(scraper, "comments_auth_failed", False)),
                                    fetched_count=len(comments),
                                    max_comments_per_post=opts.max_comments_per_post,
                                )
                                if is_complete:
                                    missing_marked += _mark_missing_comments_for_anchor(
                                        platform="tiktok",
                                        anchor_id=str(upserted["id"]),
                                        observed_comment_ids=observed_comment_ids,
                                        conn=post_comments_conn,
                                    )
                                    _reconcile_post_comment_count(
                                        platform="tiktok",
                                        post_db_id=str(upserted["id"]),
                                        conn=post_comments_conn,
                                    )
                                else:
                                    incomplete_comment_fetches += 1
                                    comments_mark_missing_skipped += 1
                        except Exception:
                            fetch_failed = True
                            comment_errors += 1
                            logger.exception(
                                "[tiktok] Failed to persist comments for post %s (video_id=%s)",
                                upserted.get("id"),
                                getattr(post, "video_id", "?"),
                            )
                            incomplete_comment_fetches += 1
                            comments_mark_missing_skipped += 1

                    if not fetch_failed:
                        _merge_comment_persist_stats(comment_stats, local_comment_stats)
                        scraped_comment_count = int(comment_stats.get("comments_fetched") or 0)
                        try:
                            # Enrichment re-reads every stored comment for the post;
                            # skip it when this run persisted nothing new.
                            if int(local_comment_stats.get("comments_upserted") or 0) > 0:
                                _persist_tiktok_comment_enrichment(
                                    post_id=str(upserted["id"]),
                                    cast_names=cast_names,
                                )
                        except Exception:
                            logger.debug(
                                "[tiktok] Failed to persist comment enrichment for post %s",
                                str(upserted.get("id") or ""),
                                exc_info=True,
                            )
                        activity["phase"] = "comments_fetch"
                        _flush_progress()

        if stage == "posts":
            sound_ids_to_fetch: list[str] = []
            seen_sound_ids: set[str] = set()
            for candidate in [*requested_sound_ids, *sorted(discovered_sound_ids)]:
                normalized_sound_id = _normalize_tiktok_sound_id(candidate)
                if not normalized_sound_id or normalized_sound_id in seen_sound_ids:
                    continue
                seen_sound_ids.add(normalized_sound_id)
                sound_ids_to_fetch.append(normalized_sound_id)

            for sound_id in sound_ids_to_fetch:
                sound_pages_attempted += 1
                try:
                    snapshot = _fetch_tiktok_sound_snapshot(sound_id)
                    _persist_tiktok_sound_snapshot(sound_id=sound_id, snapshot=snapshot)
                    sound_pages_succeeded += 1
                    sound_posts_ingested += len(snapshot.get("posts") or [])
                except Exception:
                    logger.debug("[tiktok] Failed to fetch/persist sound snapshot sound_id=%s", sound_id, exc_info=True)

            if normalized_creator_id and (latest_posted_at_seen or checkpoint_cursor):
                _save_ingest_checkpoint(
                    platform="tiktok",
                    creator_id=normalized_creator_id,
                    cursor=checkpoint_cursor,
                    last_posted_at=latest_posted_at_seen or checkpoint_last_posted_at,
                    metadata={
                        "stage": stage,
                        "account": account,
                        "job_id": job_id,
                        "run_id": run_id,
                        "sound_ids_requested": requested_sound_ids,
                        "sound_ids_discovered": sorted(discovered_sound_ids),
                    },
                )

    activity["phase"] = f"{stage}_end"
    activity["matched_posts"] = matched_post_count
    _flush_progress(force=True)

    if skipped_synced:
        logger.info("[tiktok] Skipped %d posts already up-to-date", skipped_synced)
    if comment_errors:
        retrieval_meta["comment_errors"] = comment_errors
    if post_upsert_errors:
        retrieval_meta["post_upsert_errors"] = post_upsert_errors
    if comment_fail_reasons:
        retrieval_meta["comment_fail_reasons"] = sorted(comment_fail_reasons)
    if comment_refresh_reasons:
        retrieval_meta["comment_refresh_decisions"] = dict(comment_refresh_reasons)
    if missing_marked:
        retrieval_meta["comments_marked_missing"] = missing_marked
    if incomplete_comment_fetches:
        retrieval_meta["incomplete_comment_fetches"] = incomplete_comment_fetches
    if comments_mark_missing_skipped:
        retrieval_meta["comments_mark_missing_skipped"] = comments_mark_missing_skipped
    if mirror_jobs_enqueued:
        retrieval_meta["media_mirror_jobs_enqueued"] = mirror_jobs_enqueued
    if mirror_job_enqueue_errors:
        retrieval_meta["media_mirror_job_enqueue_errors"] = mirror_job_enqueue_errors
    retrieval_meta["comment_stats"] = _comment_stats_payload(comment_stats)
    if skipped_checkpoint:
        retrieval_meta["checkpoint_skipped_posts"] = skipped_checkpoint
    if requested_sound_ids:
        retrieval_meta["sound_ids_requested"] = requested_sound_ids
    if discovered_sound_ids:
        retrieval_meta["sound_ids_discovered"] = sorted(discovered_sound_ids)
    if sound_pages_attempted:
        retrieval_meta["sound_pages_attempted"] = sound_pages_attempted
    if sound_pages_succeeded:
        retrieval_meta["sound_pages_succeeded"] = sound_pages_succeeded
    if sound_posts_ingested:
        retrieval_meta["sound_posts_ingested"] = sound_posts_ingested
    if latest_posted_at_seen:
        retrieval_meta["latest_posted_at_seen"] = _iso(latest_posted_at_seen)
    if stage == "posts":
        retrieval_meta["posts_filter_counters"] = {
            "candidate_posts_found": total_scraped,
            "matched_posts": matched_post_count,
            "filtered_out_show_terms": skipped_keyword,
            "filtered_out_up_to_date": skipped_synced,
            "filtered_out_checkpoint": skipped_checkpoint,
            "filtered_out_total": skipped_keyword + skipped_synced + skipped_checkpoint,
        }
    retrieval_meta["scrape_counters"] = {"posts": scraped_post_count, "comments": scraped_comment_count}
    retrieval_meta["persist_counters"] = {
        "posts_upserted": matched_post_count,
        "comments_upserted": int(comment_stats.get("comments_upserted") or 0),
    }
    retrieval_meta["activity"] = _build_progress_activity_payload(activity, now=_now_utc())
    logger.info(
        "[tiktok] Done: scraped=%d matched=%d skipped_keyword=%d comments=%d comment_errors=%d",
        total_scraped,
        matched_post_count,
        skipped_keyword,
        scraped_comment_count,
        comment_errors,
    )
    return scraped_post_count, scraped_comment_count, retrieval_meta


_LOCAL_ROOM_FUNCTIONS.update({_name: globals()[_name] for _name in _LOCAL_ROOM_NAMES})
__all__ = [
    "_ingest_tiktok",
]
