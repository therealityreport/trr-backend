# ruff: noqa: F403, F405, UP037
"""Season analytics and media-mirror route surface."""

from __future__ import annotations

from fastapi import APIRouter

from ._shared import *
from ._surfaces import RouteRecord, routes_matching

router = APIRouter()

_WEEK_DETAIL_DEFAULT_MAX_COMMENTS_PER_POST = 0

_WEEK_DETAIL_DEFAULT_POST_LIMIT = 20

_WEEK_DETAIL_DEFAULT_POST_OFFSET = 0

class PostCommentRefreshRequest(BaseModel):
    max_comments_per_post: int = Field(default=0, ge=0)
    fetch_replies: bool = Field(default=True)

@router.get("/seasons/{season_id}/analytics/week/{week_index}/live-health")
async def get_season_analytics_week_live_health(
    season_id: UUID,
    week_index: int,
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Query(default="network"),
    timezone: str = Query(default="America/New_York"),
    platforms: str | None = Query(default=None, description="Comma-separated platform list"),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.socials.analytics import get_week_live_health_snapshot

    parsed_platforms = _parse_platform_query(platforms)
    canonical_source_scope = normalize_source_scope_param(source_scope)
    cache_key = _week_live_health_cache_key(
        season_id=str(season_id),
        week_index=week_index,
        source_scope=source_scope,
        platforms=parsed_platforms,
        timezone=timezone,
    )
    cached_payload = _get_ttl_cached_payload(
        _WEEK_LIVE_HEALTH_CACHE,
        _WEEK_LIVE_HEALTH_CACHE_LOCK,
        cache_key,
    )
    if cached_payload is not None:
        return cached_payload
    started_at = perf_counter()
    try:
        payload = await _run_admin_repo_call(
            get_week_live_health_snapshot,
            str(season_id),
            week_index=week_index,
            platforms=parsed_platforms,
            timezone=timezone,
            source_scope=canonical_source_scope,
        )
        _set_ttl_cached_payload(
            _WEEK_LIVE_HEALTH_CACHE,
            _WEEK_LIVE_HEALTH_CACHE_LOCK,
            cache_key,
            payload,
            ttl_seconds=_WEEK_LIVE_HEALTH_CACHE_TTL_SECONDS,
            max_entries=_WEEK_LIVE_HEALTH_CACHE_MAX_ENTRIES,
        )
        duration_ms = int((perf_counter() - started_at) * 1000)
        logger.info(
            "Social week live-health request completed: season=%s week=%s source_scope=%s platforms=%s duration_ms=%s",
            season_id,
            week_index,
            source_scope,
            ",".join(parsed_platforms) if parsed_platforms else "all",
            duration_ms,
        )
        return payload
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        duration_ms = int((perf_counter() - started_at) * 1000)
        logger.exception(
            "Failed to compute social week live-health: season=%s week=%s source_scope=%s platforms=%s duration_ms=%s",
            season_id,
            week_index,
            source_scope,
            ",".join(parsed_platforms) if parsed_platforms else "all",
            duration_ms,
        )
        raise _to_social_read_http_exception(exc) from exc

@router.get("/seasons/{season_id}/analytics")
async def get_season_analytics(
    season_id: UUID,
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Query(default="network"),
    timezone: str = Query(default="America/New_York"),
    week: int | None = Query(default=None, ge=0, le=200),
    platforms: str | None = Query(default=None, description="Comma-separated platform list"),
    include: str | None = Query(
        default=None,
        description="Comma-separated include list: rows,flags,schedule,benchmark",
    ),
    _: InternalAdminUser = None,
) -> dict:
    from trr_backend.socials.analytics import get_analytics

    parsed_platforms = _parse_platform_query(platforms)
    include_options = parse_analytics_include(include)
    canonical_source_scope = normalize_source_scope_param(source_scope)

    started_at = perf_counter()
    try:
        cache_key = _analytics_cache_key(
            season_id=str(season_id),
            source_scope=source_scope,
            platforms=parsed_platforms,
            timezone=timezone,
            week=week,
            include_rows=include_options.include_rows,
            include_flags=include_options.include_flags,
            include_schedule=include_options.include_schedule,
            include_benchmark=include_options.include_benchmark,
        )
        cached_payload = _get_ttl_cached_payload(
            _ANALYTICS_CACHE,
            _ANALYTICS_CACHE_LOCK,
            cache_key,
        )
        if cached_payload is not None:
            log_read_path(
                "season-social-analytics",
                latency_ms=(perf_counter() - started_at) * 1000,
                payload=cached_payload,
                extra=analytics_read_path_extra(
                    cache="hit",
                    source_scope=source_scope,
                    week=week,
                    platforms=parsed_platforms,
                ),
            )
            return cached_payload
        payload = await run_in_threadpool(
            get_analytics,
            str(season_id),
            platforms=parsed_platforms,
            timezone=timezone,
            week=week,
            source_scope=canonical_source_scope,
            include_rows=include_options.include_rows,
            include_jobs=False,
            include_flags=include_options.include_flags,
            include_schedule=include_options.include_schedule,
            include_benchmark=include_options.include_benchmark,
        )
        _set_ttl_cached_payload(
            _ANALYTICS_CACHE,
            _ANALYTICS_CACHE_LOCK,
            cache_key,
            payload,
            ttl_seconds=_ANALYTICS_CACHE_TTL_SECONDS,
            max_entries=_ANALYTICS_CACHE_MAX_ENTRIES,
        )
        duration_ms = int((perf_counter() - started_at) * 1000)
        logger.info(
            "Social analytics request completed: season=%s source_scope=%s week=%s platforms=%s duration_ms=%s",
            season_id,
            source_scope,
            week,
            ",".join(parsed_platforms) if parsed_platforms else "all",
            duration_ms,
        )
        log_read_path(
            "season-social-analytics",
            latency_ms=(perf_counter() - started_at) * 1000,
            payload=payload,
            extra=analytics_read_path_extra(
                cache="miss",
                source_scope=source_scope,
                week=week,
                platforms=parsed_platforms,
            ),
        )
        return payload
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        duration_ms = int((perf_counter() - started_at) * 1000)
        logger.exception(
            "Failed to compute social analytics: season=%s source_scope=%s week=%s platforms=%s duration_ms=%s",
            season_id,
            source_scope,
            week,
            ",".join(parsed_platforms) if parsed_platforms else "all",
            duration_ms,
        )
        raise _to_social_read_http_exception(exc) from exc

@router.get("/seasons/{season_id}/analytics/week/{week_index}/summary")
async def get_season_analytics_week_summary(
    season_id: UUID,
    week_index: int,
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Query(default="network"),
    timezone: str = Query(default="America/New_York"),
    platforms: str | None = Query(default=None, description="Comma-separated platform list"),
    include: WeekSummaryInclude = Query(default="totals_only"),
    max_comments_per_post: int = Query(default=_WEEK_DETAIL_DEFAULT_MAX_COMMENTS_PER_POST, ge=0, le=500),
    sort_field: WeekDetailSortField = Query(default="posted_at"),
    sort_dir: WeekDetailSortDir = Query(default="desc"),
    _: InternalAdminUser = None,
) -> dict:
    from trr_backend.socials.analytics import get_week_detail_summary, get_week_detail_summary_fast

    parsed_platforms = _parse_platform_query(platforms)
    canonical_source_scope = normalize_source_scope_param(source_scope)
    cache_key = _week_summary_cache_key(
        season_id=str(season_id),
        week_index=week_index,
        source_scope=source_scope,
        platforms=parsed_platforms,
        timezone=timezone,
        include=include,
        max_comments_per_post=max_comments_per_post,
        sort_field=sort_field,
        sort_dir=sort_dir,
    )
    cached_payload = _get_week_summary_cached_payload(cache_key)
    if cached_payload is not None:
        return cached_payload
    started_at = perf_counter()
    trace_id = get_trace_id()

    try:
        if include == "full":
            payload = await _run_admin_repo_call(
                get_week_detail_summary,
                str(season_id),
                week_index=week_index,
                platforms=parsed_platforms,
                timezone=timezone,
                source_scope=canonical_source_scope,
                max_comments_per_post=max_comments_per_post,
                sort_field=sort_field,
                sort_dir=sort_dir,
            )
        else:
            payload = await _run_admin_repo_call(
                get_week_detail_summary_fast,
                str(season_id),
                week_index=week_index,
                platforms=parsed_platforms,
                timezone=timezone,
                source_scope=canonical_source_scope,
            )
        _set_week_summary_cached_payload(cache_key, payload)
        duration_ms = int((perf_counter() - started_at) * 1000)
        logger.info(
            (
                "Social week detail summary completed: season=%s week=%s source_scope=%s platforms=%s "
                "include=%s max_comments_per_post=%s duration_ms=%s trace_id=%s"
            ),
            season_id,
            week_index,
            source_scope,
            ",".join(parsed_platforms) if parsed_platforms else "all",
            include,
            max_comments_per_post,
            duration_ms,
            trace_id,
        )
        return payload
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        duration_ms = int((perf_counter() - started_at) * 1000)
        logger.exception(
            (
                "Failed to compute week detail summary: season=%s week=%s source_scope=%s platforms=%s "
                "include=%s max_comments_per_post=%s duration_ms=%s trace_id=%s"
            ),
            season_id,
            week_index,
            source_scope,
            ",".join(parsed_platforms) if parsed_platforms else "all",
            include,
            max_comments_per_post,
            duration_ms,
            trace_id,
        )
        raise _to_social_read_http_exception(exc) from exc

@router.get("/seasons/{season_id}/analytics/week/{week_index}")
async def get_season_analytics_week_detail(
    season_id: UUID,
    week_index: int,
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Query(default="network"),
    timezone: str = Query(default="America/New_York"),
    platforms: str | None = Query(default=None, description="Comma-separated platform list"),
    max_comments_per_post: int = Query(default=_WEEK_DETAIL_DEFAULT_MAX_COMMENTS_PER_POST, ge=0, le=500),
    post_limit: int = Query(default=_WEEK_DETAIL_DEFAULT_POST_LIMIT, ge=1, le=100),
    post_offset: int = Query(default=_WEEK_DETAIL_DEFAULT_POST_OFFSET, ge=0),
    sort_field: WeekDetailSortField = Query(default="posted_at"),
    sort_dir: WeekDetailSortDir = Query(default="desc"),
    include_status: bool = Query(default=True),
    _: InternalAdminUser = None,
) -> dict:
    from trr_backend.socials.analytics import get_week_detail

    parsed_platforms = _parse_platform_query(platforms)
    normalized_platforms = _normalize_target_platforms(parsed_platforms)
    normalized_timezone = str(timezone or "").strip() or "America/New_York"
    canonical_source_scope = normalize_source_scope_param(source_scope)
    cache_key = _week_detail_cache_key(
        season_id=str(season_id),
        week_index=week_index,
        source_scope=source_scope,
        platforms=normalized_platforms,
        timezone=normalized_timezone,
        max_comments_per_post=max_comments_per_post,
        sort_field=sort_field,
        sort_dir=sort_dir,
        include_status=include_status,
    )
    cached_payload: dict[str, Any] | None = _get_week_detail_cached_payload(cache_key)
    requested_end = post_limit + post_offset
    started_at = perf_counter()
    trace_id = get_trace_id()

    try:
        base_payload: dict[str, Any] | None = cached_payload
        if base_payload is None:
            base_payload = await _run_admin_repo_call(
                get_week_detail,
                str(season_id),
                week_index=week_index,
                platforms=parsed_platforms,
                timezone=normalized_timezone,
                source_scope=canonical_source_scope,
                max_comments_per_post=max_comments_per_post,
                post_limit=requested_end,
                post_offset=0,
                sort_field=sort_field,
                sort_dir=sort_dir,
                include_status=include_status,
            )
            _set_week_detail_cached_payload(cache_key, base_payload)
        else:
            cached_posts, cached_total = week_detail_cached_post_counts(base_payload)

            if requested_end > cached_posts and cached_total > cached_posts:
                base_payload = await _run_admin_repo_call(
                    get_week_detail,
                    str(season_id),
                    week_index=week_index,
                    platforms=parsed_platforms,
                    timezone=normalized_timezone,
                    source_scope=canonical_source_scope,
                    max_comments_per_post=max_comments_per_post,
                    post_limit=requested_end,
                    post_offset=0,
                    sort_field=sort_field,
                    sort_dir=sort_dir,
                    include_status=include_status,
                )
            _set_week_detail_cached_payload(cache_key, base_payload)

        paged_payload = page_week_detail_payload(
            base_payload,
            post_limit=post_limit,
            post_offset=post_offset,
            sort_field=sort_field,
            sort_dir=sort_dir,
        )
        duration_ms = int((perf_counter() - started_at) * 1000)
        logger.info(
            (
                "Social week detail completed: season=%s week=%s source_scope=%s platforms=%s "
                "max_comments_per_post=%s post_limit=%s post_offset=%s include_status=%s duration_ms=%s trace_id=%s"
            ),
            season_id,
            week_index,
            source_scope,
            ",".join(parsed_platforms) if parsed_platforms else "all",
            max_comments_per_post,
            post_limit,
            post_offset,
            include_status,
            duration_ms,
            trace_id,
        )
        return paged_payload
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        duration_ms = int((perf_counter() - started_at) * 1000)
        logger.exception(
            (
                "Failed to compute week detail: season=%s week=%s source_scope=%s platforms=%s "
                "max_comments_per_post=%s post_limit=%s post_offset=%s include_status=%s duration_ms=%s trace_id=%s"
            ),
            season_id,
            week_index,
            source_scope,
            ",".join(parsed_platforms) if parsed_platforms else "all",
            max_comments_per_post,
            post_limit,
            post_offset,
            include_status,
            duration_ms,
            trace_id,
        )
        raise _to_social_read_http_exception(exc) from exc

@router.get("/seasons/{season_id}/analytics/comments-coverage")
async def get_season_comments_coverage(
    season_id: UUID,
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Query(default="network"),
    timezone: str = Query(default="America/New_York"),
    platforms: str | None = Query(default=None, description="Comma-separated platform list"),
    date_start: datetime | None = Query(default=None),
    date_end: datetime | None = Query(default=None),
    _: InternalAdminUser = None,
) -> dict:
    from trr_backend.socials.analytics import get_comments_coverage

    parsed_platforms = _parse_platform_query(platforms)
    cache_key = _coverage_cache_window_key(
        season_id=str(season_id),
        source_scope=source_scope,
        platforms=parsed_platforms,
        timezone=timezone,
        date_start=date_start,
        date_end=date_end,
    )
    cached_payload = _get_ttl_cached_payload(
        _COMMENTS_COVERAGE_CACHE,
        _COMMENTS_COVERAGE_CACHE_LOCK,
        cache_key,
    )
    if cached_payload is not None:
        return cached_payload

    try:
        payload = await _run_admin_repo_call(
            get_comments_coverage,
            str(season_id),
            platforms=parsed_platforms,
            timezone=timezone,
            source_scope=source_scope,
            date_start=date_start,
            date_end=date_end,
        )
        _set_ttl_cached_payload(
            _COMMENTS_COVERAGE_CACHE,
            _COMMENTS_COVERAGE_CACHE_LOCK,
            cache_key,
            payload,
            ttl_seconds=_COVERAGE_CACHE_TTL_SECONDS,
            max_entries=_COVERAGE_CACHE_MAX_ENTRIES,
        )
        return payload
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to compute comments coverage: season=%s", season_id)
        raise _internal_error_response(exc) from exc

@router.get("/seasons/{season_id}/analytics/mirror-coverage")
async def get_season_mirror_coverage(
    season_id: UUID,
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Query(default="network"),
    timezone: str = Query(default="America/New_York"),
    platforms: str | None = Query(default=None, description="Comma-separated platform list"),
    date_start: datetime | None = Query(default=None),
    date_end: datetime | None = Query(default=None),
    _: InternalAdminUser = None,
) -> dict:
    from trr_backend.socials.analytics import get_mirror_coverage

    parsed_platforms = _parse_platform_query(platforms)
    cache_key = _coverage_cache_window_key(
        season_id=str(season_id),
        source_scope=source_scope,
        platforms=parsed_platforms,
        timezone=timezone,
        date_start=date_start,
        date_end=date_end,
    )
    cached_payload = _get_ttl_cached_payload(
        _MIRROR_COVERAGE_CACHE,
        _MIRROR_COVERAGE_CACHE_LOCK,
        cache_key,
    )
    if cached_payload is not None:
        return cached_payload

    try:
        payload = await _run_admin_repo_call(
            get_mirror_coverage,
            str(season_id),
            platforms=parsed_platforms,
            timezone=timezone,
            source_scope=source_scope,
            date_start=date_start,
            date_end=date_end,
        )
        _set_ttl_cached_payload(
            _MIRROR_COVERAGE_CACHE,
            _MIRROR_COVERAGE_CACHE_LOCK,
            cache_key,
            payload,
            ttl_seconds=_COVERAGE_CACHE_TTL_SECONDS,
            max_entries=_COVERAGE_CACHE_MAX_ENTRIES,
        )
        return payload
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to compute mirror coverage: season=%s", season_id)
        raise _internal_error_response(exc) from exc

@router.get("/seasons/{season_id}/analytics/posts/{platform}/{source_id}")
async def get_post_comments(
    season_id: UUID,
    platform: str,
    source_id: str,
    _: InternalAdminUser = None,
) -> dict:
    from trr_backend.socials.analytics import get_post_comments as _get

    try:
        return await _run_admin_repo_call(_get, str(season_id), platform=platform, source_id=source_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to fetch post comments: season=%s platform=%s source_id=%s",
            season_id,
            platform,
            source_id,
        )
        raise _internal_error_response(exc) from exc

@router.get("/seasons/{season_id}/tiktok/overview")
async def get_season_tiktok_overview(
    season_id: UUID,
    date_start: datetime | None = Query(default=None),
    date_end: datetime | None = Query(default=None),
    cast_member_id: UUID | None = Query(default=None),
    hashtag: str | None = Query(default=None),
    keyword: str | None = Query(default=None),
    sound_id: str | None = Query(default=None),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.socials.analytics import get_tiktok_overview

    try:
        return await _run_admin_repo_call(
            get_tiktok_overview,
            str(season_id),
            date_start=date_start,
            date_end=date_end,
            cast_member_id=str(cast_member_id) if cast_member_id else None,
            hashtag=hashtag,
            keyword=keyword,
            sound_id=sound_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch TikTok overview: season=%s", season_id)
        raise _internal_error_response(exc) from exc

@router.get("/seasons/{season_id}/tiktok/cast-members")
async def get_season_tiktok_cast_members(
    season_id: UUID,
    date_start: datetime | None = Query(default=None),
    date_end: datetime | None = Query(default=None),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.socials.analytics import get_tiktok_cast_members

    try:
        return await _run_admin_repo_call(
            get_tiktok_cast_members,
            str(season_id),
            date_start=date_start,
            date_end=date_end,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch TikTok cast members: season=%s", season_id)
        raise _internal_error_response(exc) from exc

@router.get("/seasons/{season_id}/tiktok/hashtags")
async def get_season_tiktok_hashtags(
    season_id: UUID,
    token_type: str = Query(default="hashtag"),
    date_start: datetime | None = Query(default=None),
    date_end: datetime | None = Query(default=None),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.socials.analytics import get_tiktok_hashtags

    try:
        return await _run_admin_repo_call(
            get_tiktok_hashtags,
            str(season_id),
            token_type=token_type,
            date_start=date_start,
            date_end=date_end,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch TikTok hashtag/keyword trends: season=%s", season_id)
        raise _internal_error_response(exc) from exc

@router.get("/seasons/{season_id}/tiktok/sounds")
async def get_season_tiktok_sounds(
    season_id: UUID,
    date_start: datetime | None = Query(default=None),
    date_end: datetime | None = Query(default=None),
    search: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=250),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.socials.analytics import get_tiktok_sounds

    try:
        return await _run_admin_repo_call(
            get_tiktok_sounds,
            str(season_id),
            date_start=date_start,
            date_end=date_end,
            search=search,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch TikTok sounds: season=%s", season_id)
        raise _internal_error_response(exc) from exc

@router.get("/seasons/{season_id}/tiktok/content-health")
async def get_season_tiktok_content_health(
    season_id: UUID,
    date_start: datetime | None = Query(default=None),
    date_end: datetime | None = Query(default=None),
    cast_member_id: UUID | None = Query(default=None),
    hashtag: str | None = Query(default=None),
    keyword: str | None = Query(default=None),
    sound_id: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=250),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.socials.analytics import get_tiktok_content_health

    try:
        return await _run_admin_repo_call(
            get_tiktok_content_health,
            str(season_id),
            date_start=date_start,
            date_end=date_end,
            cast_member_id=str(cast_member_id) if cast_member_id else None,
            hashtag=hashtag,
            keyword=keyword,
            sound_id=sound_id,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch TikTok content health: season=%s", season_id)
        raise _internal_error_response(exc) from exc

@router.get("/seasons/{season_id}/tiktok/sounds/{sound_id}")
async def get_season_tiktok_sound_detail(
    season_id: UUID,
    sound_id: str,
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.socials.analytics import get_tiktok_sound_detail

    try:
        return await _run_admin_repo_call(get_tiktok_sound_detail, str(season_id), sound_id=sound_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch TikTok sound detail: season=%s sound_id=%s", season_id, sound_id)
        raise _internal_error_response(exc) from exc

@router.get("/seasons/{season_id}/tiktok/sounds/{sound_id}/posts")
async def get_season_tiktok_sound_posts(
    season_id: UUID,
    sound_id: str,
    limit: int = Query(default=100, ge=1, le=500),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.socials.analytics import get_tiktok_sound_posts

    try:
        return await _run_admin_repo_call(get_tiktok_sound_posts, str(season_id), sound_id=sound_id, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch TikTok sound posts: season=%s sound_id=%s", season_id, sound_id)
        raise _internal_error_response(exc) from exc

@router.get("/seasons/{season_id}/tiktok/posts/{post_id}/detail")
async def get_season_tiktok_post_detail(
    season_id: UUID,
    post_id: str,
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.socials.analytics import get_tiktok_post_detail

    try:
        return await _run_admin_repo_call(get_tiktok_post_detail, str(season_id), post_id=post_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch TikTok post detail: season=%s post_id=%s", season_id, post_id)
        raise _internal_error_response(exc) from exc

@router.get("/seasons/{season_id}/tiktok/sentiment-trends")
async def get_season_tiktok_sentiment_trends(
    season_id: UUID,
    date_start: datetime | None = Query(default=None),
    date_end: datetime | None = Query(default=None),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.socials.analytics import get_tiktok_sentiment_trends

    try:
        return await _run_admin_repo_call(
            get_tiktok_sentiment_trends,
            str(season_id),
            date_start=date_start,
            date_end=date_end,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch TikTok sentiment trends: season=%s", season_id)
        raise _internal_error_response(exc) from exc

@router.post("/seasons/{season_id}/analytics/posts/{platform}/{source_id}/refresh")
async def refresh_post_comments_for_post(
    season_id: UUID,
    platform: str,
    source_id: str,
    payload: PostCommentRefreshRequest | None = None,
    _: InternalAdminUser = None,
) -> dict:
    from trr_backend.socials.analytics import get_post_comments as _get_post_comments
    from trr_backend.socials.control_plane import refresh_post as _refresh_post

    request_payload = payload or PostCommentRefreshRequest()

    try:
        refresh_summary = await _run_admin_repo_call(
            _refresh_post,
            str(season_id),
            platform=platform,
            source_id=source_id,
            max_comments_per_post=request_payload.max_comments_per_post,
            fetch_replies=request_payload.fetch_replies,
        )
        invalidate_week_detail_cache()
        refreshed = await _run_admin_repo_call(
            _get_post_comments,
            str(season_id),
            platform=platform,
            source_id=source_id,
        )
        refreshed["refresh"] = refresh_summary
        return refreshed
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to refresh post comments: season=%s platform=%s source_id=%s",
            season_id,
            platform,
            source_id,
        )
        raise _internal_error_response(exc) from exc

@router.post("/seasons/{season_id}/instagram/mirror/requeue")
async def requeue_instagram_mirror_jobs(
    season_id: UUID,
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Query(default="network"),
    limit: int = Query(default=1000, ge=1, le=5000),
    failed_only: bool = Query(default=False),
    date_start: datetime | None = Query(default=None),
    date_end: datetime | None = Query(default=None),
    _: InternalAdminUser = None,
) -> dict:
    from trr_backend.socials.instagram.media_mirror import requeue_instagram_media_mirror_jobs

    try:
        return await _run_admin_repo_call(
            requeue_instagram_media_mirror_jobs,
            str(season_id),
            source_scope=source_scope,
            limit=limit,
            failed_only=failed_only,
            date_start=date_start,
            date_end=date_end,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to requeue instagram media mirror jobs: season=%s source_scope=%s",
            season_id,
            source_scope,
        )
        raise _internal_error_response(exc) from exc

@router.post("/seasons/{season_id}/{platform}/mirror/requeue")
async def requeue_platform_mirror_jobs(
    season_id: UUID,
    platform: str,
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Query(default="network"),
    limit: int = Query(default=1000, ge=1, le=5000),
    failed_only: bool = Query(default=False),
    date_start: datetime | None = Query(default=None),
    date_end: datetime | None = Query(default=None),
    _: InternalAdminUser = None,
) -> dict:
    from trr_backend.socials.control_plane import requeue_media_mirror_jobs

    normalized_platform = (platform or "").strip().lower()
    try:
        return await _run_admin_repo_call(
            requeue_media_mirror_jobs,
            str(season_id),
            platform=normalized_platform,
            source_scope=source_scope,
            limit=limit,
            failed_only=failed_only,
            date_start=date_start,
            date_end=date_end,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to requeue %s media mirror jobs: season=%s source_scope=%s",
            normalized_platform,
            season_id,
            source_scope,
        )
        raise _internal_error_response(exc) from exc

@router.get("/seasons/{season_id}/analytics/export.csv")
async def export_season_analytics_csv(
    season_id: UUID,
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Query(default="network"),
    timezone: str = Query(default="America/New_York"),
    week: int | None = Query(default=None, ge=0, le=200),
    platforms: str | None = Query(default=None),
    _: InternalAdminUser = None,
) -> Response:
    from trr_backend.socials.analytics import (
        build_csv,
        get_analytics,
    )

    parsed_platforms = None
    if platforms and platforms.strip():
        parsed_platforms = [item.strip().lower() for item in platforms.split(",") if item.strip()]
    try:
        snapshot = await _run_admin_repo_call(
            get_analytics,
            str(season_id),
            platforms=parsed_platforms,
            timezone=timezone,
            week=week,
            source_scope=source_scope,
            include_rows=True,
        )
        csv_text = await _run_admin_repo_call(build_csv, snapshot)
        filename = f"social_report_{season_id}_{datetime.now().strftime('%Y%m%d')}.csv"
        return Response(
            content=csv_text,
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to export social CSV: season=%s", season_id)
        raise _internal_error_response(exc) from exc

@router.get("/seasons/{season_id}/analytics/export.pdf")
async def export_season_analytics_pdf(
    season_id: UUID,
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Query(default="network"),
    timezone: str = Query(default="America/New_York"),
    week: int | None = Query(default=None, ge=0, le=200),
    platforms: str | None = Query(default=None),
    _: InternalAdminUser = None,
) -> Response:
    from trr_backend.socials.analytics import (
        build_pdf,
        get_analytics,
        pdf_filename,
    )

    parsed_platforms = None
    if platforms and platforms.strip():
        parsed_platforms = [item.strip().lower() for item in platforms.split(",") if item.strip()]
    try:
        snapshot = await _run_admin_repo_call(
            get_analytics,
            str(season_id),
            platforms=parsed_platforms,
            timezone=timezone,
            week=week,
            source_scope=source_scope,
            include_rows=False,
        )
        pdf_bytes = await _run_admin_repo_call(build_pdf, snapshot)
        summary = snapshot.get("summary") or {}
        filename = pdf_filename(
            str(summary.get("show_id") or "show"),
            int(summary.get("season_number") or 0),
        )
        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to export social PDF: season=%s", season_id)
        raise _internal_error_response(exc) from exc

ROUTE_PREFIXES = ("/admin/socials/seasons/",)


def surface_routes(router: Any) -> list[RouteRecord]:
    return [
        record
        for record in routes_matching(router, ROUTE_PREFIXES)
        if "/analytics" in record[1] or "/tiktok/" in record[1] or "/mirror/" in record[1]
    ]
