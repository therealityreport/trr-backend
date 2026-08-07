# ruff: noqa: F401, F403, F405, F722, I001, UP037
"""Profile reads, Instagram profile reads, and hashtag routes with shared profile helpers."""

from __future__ import annotations

from fastapi import APIRouter

from ._shared import *
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    # Type-only declarations for underscore-prefixed helpers that are injected
    # at runtime via the dynamic ``__all__`` star export from ``._shared``.
    from ._shared import (
        _ACCOUNT_PROFILE_CACHE_MAX_ENTRIES,
        _ACCOUNT_PROFILE_CACHE_TTL_SECONDS,
        _ACCOUNT_PROFILE_COLLABORATORS_CACHE,
        _ACCOUNT_PROFILE_COLLABORATORS_CACHE_LOCK,
        _ACCOUNT_PROFILE_DASHBOARD_CACHE,
        _ACCOUNT_PROFILE_DASHBOARD_CACHE_LOCK,
        _ACCOUNT_PROFILE_HASHTAG_TIMELINE_CACHE,
        _ACCOUNT_PROFILE_HASHTAG_TIMELINE_CACHE_LOCK,
        _ACCOUNT_PROFILE_HASHTAGS_CACHE,
        _ACCOUNT_PROFILE_HASHTAGS_CACHE_LOCK,
        _ACCOUNT_PROFILE_POSTS_CACHE,
        _ACCOUNT_PROFILE_POSTS_CACHE_LOCK,
        _ACCOUNT_PROFILE_PROGRESS_CACHE_TTL_SECONDS,
        _ACCOUNT_PROFILE_SUMMARY_CACHE,
        _ACCOUNT_PROFILE_SUMMARY_CACHE_LOCK,
        _account_profile_cache_key,
        _can_use_local_catalog_inline_fallback,
        _clear_account_profile_caches,
        _get_ttl_cached_payload,
        _internal_error_response,
        _lookup_error_to_not_found,
        _raise_if_modal_social_dispatch_unresolvable,
        _remote_worker_unavailable_message,
        _resolve_account_profile_singleflight,
        _set_ttl_cached_payload,
        _to_social_read_http_exception,
        _value_error_to_bad_request,
        _worker_health_detail,
    )

router = APIRouter()


class SocialBladeProfileRefreshRequest(BaseModel):
    force: bool = False


def _resolve_social_account_comments_route_execution(
    *,
    allow_inline_dev_fallback: bool,
    platform: str = "instagram",
) -> dict[str, Any]:
    from trr_backend.socials.pipelines.account_catalog.launch import INSTAGRAM_COMMENTS_SCRAPLING_WORKER_LANE
    from trr_backend.socials.control_plane.runtime import SocialWorkerUnavailableError
    from trr_backend.socials.control_plane.worker_health import (
        assert_worker_available_when_queue_enabled,
        is_queue_enabled,
    )

    queue_enabled = is_queue_enabled()
    remote_plane_enforced = is_remote_job_plane_enabled()
    used_inline_fallback = False
    requires_modal_executor = is_modal_remote_executor_enabled()
    can_use_local_inline_fallback = _can_use_local_catalog_inline_fallback(
        allow_inline_dev_fallback=allow_inline_dev_fallback,
        remote_plane_enforced=remote_plane_enforced,
        requires_modal_executor=requires_modal_executor,
    )

    if queue_enabled:
        try:
            assert_worker_available_when_queue_enabled(
                required_worker_lane=None if requires_modal_executor else INSTAGRAM_COMMENTS_SCRAPLING_WORKER_LANE,
                required_execution_backend="modal" if requires_modal_executor else None,
                platform=platform,
            )
            if requires_modal_executor:
                _raise_if_modal_social_dispatch_unresolvable(platform)
        except SocialWorkerUnavailableError as exc:
            if can_use_local_inline_fallback:
                queue_enabled = False
                used_inline_fallback = True
            else:
                raise HTTPException(
                    status_code=503,
                    detail={
                        "code": (
                            "SOCIAL_MODAL_EXECUTOR_REQUIRED"
                            if requires_modal_executor
                            else "SOCIAL_REMOTE_JOB_PLANE_ENFORCED"
                            if remote_plane_enforced
                            else "SOCIAL_WORKER_UNAVAILABLE"
                        ),
                        "message": (
                            "Instagram comments scraping requires the Modal remote executor."
                            if requires_modal_executor
                            else _remote_worker_unavailable_message(exc)
                            if remote_plane_enforced
                            else str(exc)
                        ),
                        "execution_mode": canonical_execution_mode(),
                        "execution_owner": execution_owner_label(),
                        "required_execution_backend": "modal" if requires_modal_executor else None,
                        "worker_health": _worker_health_detail(exc.worker_health),
                    },
                ) from exc
    elif remote_plane_enforced or (requires_modal_executor and not can_use_local_inline_fallback):
        raise HTTPException(
            status_code=503,
            detail={
                "code": (
                    "SOCIAL_MODAL_EXECUTOR_REQUIRED" if requires_modal_executor else "SOCIAL_REMOTE_JOB_PLANE_ENFORCED"
                ),
                "message": (
                    "Instagram comments scraping requires the Modal remote executor."
                    if requires_modal_executor
                    else "Social ingest remote-worker ownership is enforced."
                ),
                "execution_mode": canonical_execution_mode(),
                "execution_owner": execution_owner_label(),
                "required_execution_backend": "modal" if requires_modal_executor else None,
            },
        )
    else:
        used_inline_fallback = can_use_local_inline_fallback

    return {
        "queue_enabled": queue_enabled,
        "used_inline_fallback": used_inline_fallback,
        "requires_modal_executor": requires_modal_executor,
    }


class SocialAccountProfileHashtagAssignmentInput(BaseModel):
    show_id: UUID
    assignment_scope: Literal["global", "platform"] = Field(default="global")
    platform: Literal["instagram", "tiktok", "twitter", "youtube", "facebook", "threads"] | None = Field(default=None)


class SocialAccountProfileHashtagInput(BaseModel):
    hashtag: str = Field(..., min_length=1, max_length=128)
    assignments: list[SocialAccountProfileHashtagAssignmentInput] = Field(default_factory=list)


class SocialAccountProfileHashtagsPutRequest(BaseModel):
    hashtags: list[SocialAccountProfileHashtagInput] = Field(default_factory=list)


class SocialAccountCommentsScrapeRequest(SourceScopedRequest):
    mode: Literal["profile", "single_post"] = Field(default="profile")
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Field(default="network")
    source_id: str | None = Field(default=None, min_length=1, max_length=64)
    max_posts: int | None = Field(default=None, ge=1)
    max_comments_per_post: int | None = Field(default=None, ge=0)
    refresh_policy: Literal["stale_or_missing", "all_saved_posts"] = Field(default="stale_or_missing")
    target_filter: Literal["incomplete"] | None = Field(default=None)
    comments_load_strategy: InstagramCommentsLoadStrategy = Field(default="public_relay")
    comments_worker_count: int | None = Field(default=None, ge=1, le=24)
    comments_target_batch_size: int | None = Field(default=None, ge=1, le=500)
    date_start: str | None = Field(default=None, max_length=64)
    date_end: str | None = Field(default=None, max_length=64)
    allow_inline_dev_fallback: bool = Field(default=False)
    dry_run: bool = Field(default=False)

    @model_validator(mode="after")
    def validate_shape(self) -> SocialAccountCommentsScrapeRequest:
        if self.mode == "single_post" and not str(self.source_id or "").strip():
            raise ValueError("source_id is required for single_post comment scrapes")
        if self.mode == "single_post" and self.target_filter is not None:
            raise ValueError("target_filter is only supported for profile comment scrapes")
        normalized_start = str(self.date_start or "").strip() or None
        normalized_end = str(self.date_end or "").strip() or None
        if normalized_start is not None or normalized_end is not None:
            from trr_backend.socials.pipelines.comments.instagram import _normalize_comment_date_window

            try:
                _normalize_comment_date_window(normalized_start, normalized_end)
            except ValueError as exc:
                raise ValueError(str(exc)) from exc
        self.date_start = normalized_start
        self.date_end = normalized_end
        return self


class SocialAccountCommentsAuditCursorRetryRequest(BaseModel):
    limit: int = Field(default=50, ge=1, le=500)
    shortcodes: list[str] | None = Field(default=None, max_length=500)
    stop_reasons: list[str] | None = Field(default=None, max_length=20)
    show_ids: list[str] | None = Field(default=None, max_length=100)
    season_ids: list[str] | None = Field(default=None, max_length=100)
    show_filters: list[str] | None = Field(default=None, max_length=100)
    show_filter: str | None = Field(default=None, max_length=128)
    batch_size: int = Field(default=1, ge=1, le=25)
    comments_worker_count: int | None = Field(default=None, ge=1, le=24)
    max_comments_per_post: int = Field(default=0, ge=0)
    comments_load_strategy: InstagramCommentsLoadStrategy = Field(default="public_relay")
    date_start: str | None = Field(default=None, max_length=64)
    date_end: str | None = Field(default=None, max_length=64)
    skip_launch_auth_probe: bool = Field(default=False)
    attach_to_active_run: bool = Field(default=True)
    dispatch_immediately: bool = Field(default=True)
    force_rerun_existing: bool = Field(default=False)
    dry_run: bool = Field(default=False)


class SocialAccountCommentsAuthenticatedFollowupRequest(BaseModel):
    comments_worker_count: int | None = Field(default=1, ge=1, le=4)
    comments_target_batch_size: int = Field(default=1, ge=1, le=25)
    comments_enable_media_followups: bool | None = Field(default=None)
    dispatch_immediately: bool = Field(default=True)
    dry_run: bool = Field(default=False)
    operator_confirmation: str | None = Field(default=None)


class SocialAccountCommentsPublicRecoveryRequest(BaseModel):
    comments_worker_count: int | None = Field(default=4, ge=1, le=4)
    comments_target_batch_size: int = Field(default=10, ge=1, le=25)
    comments_enable_media_followups: bool | None = Field(default=None)
    dispatch_immediately: bool = Field(default=False)
    dry_run: bool = Field(default=False)


def _enqueue_instagram_comments_audit_cursor_retries_background(**kwargs: Any) -> None:
    from trr_backend.socials.pipelines.comments.instagram import enqueue_instagram_comments_audit_cursor_retries

    try:
        result = enqueue_instagram_comments_audit_cursor_retries(**kwargs)
        logger.info(
            "Auto-attached audit cursor recovery targets for @%s: performed=%s mode=%s selected=%s",
            kwargs.get("account_handle"),
            (result.get("enqueue") or {}).get("performed") if isinstance(result, dict) else None,
            (result.get("enqueue") or {}).get("mode") if isinstance(result, dict) else None,
            result.get("selected_target_source_ids_count") if isinstance(result, dict) else None,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Failed to auto-attach audit cursor recovery targets for @%s: %s",
            kwargs.get("account_handle"),
            exc,
            exc_info=True,
        )


def _refresh_social_account_profile_socialblade(
    *,
    normalized_platform: str,
    safe_handle: str,
    force: bool,
) -> dict[str, Any]:
    from trr_backend.socials.socialblade.auth import (
        load_socialblade_cookies_from_sources,
        refresh_socialblade_cookies,
    )
    from trr_backend.socials.socialblade.scraper import scrape_socialblade
    from trr_backend.socials.socialblade.service import (
        refresh_and_persist_socialblade,
        scrape_socialblade_then_following,
    )

    refresh_socialblade_cookies("account_page_refresh", allow_headless_fallback=False)
    cookies = load_socialblade_cookies_from_sources()

    def _scrape_account(normalized_handle: str) -> dict[str, Any]:
        return scrape_socialblade(
            normalized_handle,
            cookies,
            platform=normalized_platform,
            allow_login_fallback=False,
            allow_visible_browser_retry=normalized_platform in {"instagram", "tiktok"},
        )

    return refresh_and_persist_socialblade(
        person_id=None,
        platform=normalized_platform,
        handle=safe_handle,
        scraper=lambda normalized_handle: scrape_socialblade_then_following(
            _scrape_account,
            normalized_handle,
            platform=normalized_platform,
            source="account_page",
        ),
        source="account_page",
        force=force,
    )


class CookieRefreshRequest(BaseModel):
    headless: bool = Field(
        default=False,
        description="Run browser in headless mode (default: headed for interactive login)",
    )
    timeout_seconds: int = Field(default=180, ge=30, le=600)
    operator_confirmation: str | None = Field(default=None)
    allow_cookie_refresh: bool = Field(default=True)


def _env_truthy_default(name: str, *, default: bool) -> bool:
    raw = str(os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on", "enabled"}


def _instagram_comments_auth_probe_allows_rendered_fallback(
    *,
    status: str,
    reason: str | None,
) -> bool:
    del status, reason
    return False


def _cookie_health_auth_probe_metadata(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **dict(payload),
        "probe_only": True,
        "probe_source": "cookie_health",
        "repair_action": None,
        "repair_available": False,
    }


def _instagram_comments_auth_probe_is_rate_limited(payload: Mapping[str, Any]) -> bool:
    reason = str(payload.get("reason") or payload.get("comments_auth_blocker") or "").strip().lower()
    status = str(payload.get("status") or payload.get("result") or "").strip().lower()
    return (
        bool(payload.get("rate_limited"))
        or reason in {"http_429", "rate_limited"}
        or "429" in reason
        or status == "rate_limited"
    )


@router.get("/profiles/{platform}/{account_handle}/summary")
def get_social_account_profile_summary_route(
    request: Request,
    platform: str,
    account_handle: str,
    _: InternalAdminUser = cast("InternalAdminUser", None),
) -> dict[str, Any]:
    detail = social_profile_reads.normalize_profile_summary_detail(request.query_params.get("detail"))

    cache_key = _account_profile_cache_key(
        surface="summary",
        platform=platform,
        account_handle=account_handle,
        extra=(detail,),
    )
    try:
        return _resolve_account_profile_singleflight(
            cache_key,
            lambda: social_profile_reads.get_profile_summary(
                platform=platform,
                account_handle=account_handle,
                detail=detail,
            ),
            cache=_ACCOUNT_PROFILE_SUMMARY_CACHE,
            cache_lock=_ACCOUNT_PROFILE_SUMMARY_CACHE_LOCK,
            ttl_seconds=_ACCOUNT_PROFILE_CACHE_TTL_SECONDS,
            max_entries=_ACCOUNT_PROFILE_CACHE_MAX_ENTRIES,
        )
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to fetch social account profile summary: platform=%s account=%s",
            platform,
            account_handle,
        )
        raise _to_social_read_http_exception(exc) from exc


@router.get("/profiles/{platform}/{account_handle}/dashboard")
def get_social_account_profile_dashboard_route(
    request: Request,
    platform: str,
    account_handle: str,
    run_id: str | None = Query(default=None),
    recent_log_limit: int = Query(default=25, ge=0, le=100),
    _: InternalAdminUser = cast("InternalAdminUser", None),
) -> dict[str, Any]:
    from trr_backend.socials.profile_dashboard import build_social_account_profile_dashboard

    detail = social_profile_reads.normalize_profile_summary_detail(request.query_params.get("detail"))
    normalized_run_id = str(run_id or "").strip() or None
    dashboard_cache_ttl_seconds = (
        _ACCOUNT_PROFILE_PROGRESS_CACHE_TTL_SECONDS if normalized_run_id else _ACCOUNT_PROFILE_CACHE_TTL_SECONDS
    )
    cache_key = _account_profile_cache_key(
        surface="dashboard",
        platform=platform,
        account_handle=account_handle,
        extra=(detail, normalized_run_id, recent_log_limit),
    )
    try:
        return _resolve_account_profile_singleflight(
            cache_key,
            lambda: build_social_account_profile_dashboard(
                platform=platform,
                account_handle=account_handle,
                detail=detail,
                run_id=normalized_run_id,
                recent_log_limit=recent_log_limit,
            ),
            cache=_ACCOUNT_PROFILE_DASHBOARD_CACHE,
            cache_lock=_ACCOUNT_PROFILE_DASHBOARD_CACHE_LOCK,
            ttl_seconds=dashboard_cache_ttl_seconds,
            max_entries=_ACCOUNT_PROFILE_CACHE_MAX_ENTRIES,
        )
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to fetch social account profile dashboard: platform=%s account=%s",
            platform,
            account_handle,
        )
        raise _to_social_read_http_exception(exc) from exc


@router.get("/profiles/{platform}/{account_handle}/live-profile-total")
def get_social_account_live_profile_total_route(
    platform: str,
    account_handle: str,
    _: InternalAdminUser = cast("InternalAdminUser", None),
) -> dict[str, Any]:
    try:
        return social_profile_reads.get_live_profile_total(platform=platform, account_handle=account_handle)
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to fetch social account live profile total: platform=%s account=%s",
            platform,
            account_handle,
        )
        raise _to_social_read_http_exception(exc) from exc


@router.get("/profiles/{platform}/{account_handle}/socialblade")
def get_social_account_profile_socialblade_route(
    platform: str,
    account_handle: str,
    _: InternalAdminUser = cast("InternalAdminUser", None),
) -> dict[str, Any]:
    from trr_backend.repositories.socialblade_growth import get_growth_data
    from trr_backend.socials.socialblade.service import (
        SocialBladeRefreshError,
        sanitize_socialblade_handle,
        sanitize_socialblade_platform,
    )

    try:
        normalized_platform = sanitize_socialblade_platform(platform)
        safe_handle = sanitize_socialblade_handle(account_handle, platform=normalized_platform)
    except SocialBladeRefreshError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not safe_handle:
        raise HTTPException(status_code=400, detail="Invalid handle")

    data = get_growth_data(None, safe_handle, platform=normalized_platform)
    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"No SocialBlade data found for {normalized_platform}/@{safe_handle}",
        )
    return data


@router.post("/profiles/{platform}/{account_handle}/socialblade/refresh")
async def refresh_social_account_profile_socialblade_route(
    platform: str,
    account_handle: str,
    body: SocialBladeProfileRefreshRequest,
    _: InternalAdminUser = cast("InternalAdminUser", None),
) -> dict[str, Any]:
    from trr_backend.socials.socialblade.service import (
        SocialBladeRefreshError,
        sanitize_socialblade_handle,
        sanitize_socialblade_platform,
    )

    try:
        normalized_platform = sanitize_socialblade_platform(platform)
        safe_handle = sanitize_socialblade_handle(account_handle, platform=normalized_platform)
    except SocialBladeRefreshError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not safe_handle:
        raise HTTPException(status_code=400, detail="Invalid handle")

    try:
        return await run_in_threadpool(
            _refresh_social_account_profile_socialblade,
            normalized_platform=normalized_platform,
            safe_handle=safe_handle,
            force=body.force,
        )
    except SocialBladeRefreshError as exc:
        logger.exception(
            "Failed to refresh SocialBlade account: platform=%s account=%s",
            normalized_platform,
            safe_handle,
        )
        raise _internal_error_response(exc, status_code=502) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to refresh social account SocialBlade data: platform=%s account=%s",
            platform,
            account_handle,
        )
        raise _to_social_read_http_exception(exc) from exc


@router.get("/profiles/{platform}/{account_handle}/posts")
def get_social_account_profile_posts_route(
    request: Request,
    platform: str,
    account_handle: str,
    page: int = Query(default=1, ge=1, le=10_000),
    page_size: int = Query(default=25, ge=1, le=100),
    limit: int | None = Query(default=None, ge=1, le=100),
    search: str | None = Query(default=None),
    comments_only: bool = Query(default=False),
    comment_filter: str | None = Query(default=None),
    sort_by: str | None = Query(default=None),
    sort_dir: str | None = Query(default=None),
    _: InternalAdminUser = cast("InternalAdminUser", None),
) -> dict[str, Any]:
    started_at = perf_counter()
    effective_page_size = page_size if "page_size" in request.query_params else (limit or page_size)
    cache_key = _account_profile_cache_key(
        surface="posts",
        platform=platform,
        account_handle=account_handle,
        page=page,
        page_size=effective_page_size,
        search=search,
        comments_only=comments_only,
        comment_filter=comment_filter,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )
    cached_payload = _get_ttl_cached_payload(_ACCOUNT_PROFILE_POSTS_CACHE, _ACCOUNT_PROFILE_POSTS_CACHE_LOCK, cache_key)
    if cached_payload is not None:
        if str(os.getenv("TRR_SOCIAL_PROFILE_PERF_DEBUG") or "").strip().lower() in {"1", "true", "yes", "on"}:
            logger.info(
                "[social-profile-perf] route=get_social_account_profile_posts_route platform=%s handle=%s "
                "cache_status=hit elapsed_ms=%s page_size=%s",
                platform,
                account_handle,
                int((perf_counter() - started_at) * 1000),
                effective_page_size,
            )
        return cached_payload
    try:
        payload = social_profile_reads.get_profile_posts(
            platform=platform,
            account_handle=account_handle,
            page=page,
            page_size=effective_page_size,
            search=search,
            comments_only=comments_only,
            comment_filter=comment_filter,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )
        _set_ttl_cached_payload(
            _ACCOUNT_PROFILE_POSTS_CACHE,
            _ACCOUNT_PROFILE_POSTS_CACHE_LOCK,
            cache_key,
            payload,
            ttl_seconds=_ACCOUNT_PROFILE_CACHE_TTL_SECONDS,
            max_entries=_ACCOUNT_PROFILE_CACHE_MAX_ENTRIES,
        )
        if str(os.getenv("TRR_SOCIAL_PROFILE_PERF_DEBUG") or "").strip().lower() in {"1", "true", "yes", "on"}:
            logger.info(
                "[social-profile-perf] route=get_social_account_profile_posts_route platform=%s handle=%s "
                "cache_status=miss elapsed_ms=%s page_size=%s",
                platform,
                account_handle,
                int((perf_counter() - started_at) * 1000),
                effective_page_size,
            )
        return payload
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to fetch social account profile posts: platform=%s account=%s",
            platform,
            account_handle,
        )
        raise _to_social_read_http_exception(exc) from exc


@router.get("/profiles/instagram/{account_handle}/profile")
def get_instagram_profile_detail_route(
    account_handle: str,
    source_scope: str = Query(default="network"),
    _: InternalAdminUser = cast("InternalAdminUser", None),
) -> dict[str, Any]:
    from trr_backend.socials.instagram.profile_stages import get_instagram_profile_detail

    try:
        return get_instagram_profile_detail(account_handle=account_handle, source_scope=source_scope)
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch Instagram profile detail: account=%s", account_handle)
        raise _to_social_read_http_exception(exc) from exc


@router.get("/profiles/instagram/{account_handle}/relationships")
def get_instagram_profile_relationships_route(
    account_handle: str,
    relationship_type: Literal["following"] = Query(default="following", alias="type"),
    source_scope: str = Query(default="bravo"),
    page: int = Query(default=1, ge=1, le=10_000),
    page_size: int = Query(default=25, ge=1, le=100),
    _: InternalAdminUser = cast("InternalAdminUser", None),
) -> dict[str, Any]:
    from trr_backend.socials.instagram.profile_stages import get_instagram_profile_relationships

    try:
        canonical_source_scope = preserve_source_scope_param(source_scope, default="bravo")
        return get_instagram_profile_relationships(
            account_handle=account_handle,
            source_scope=canonical_source_scope,
            relationship_type=relationship_type,
            page=page,
            page_size=page_size,
        )
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch Instagram profile relationships: account=%s", account_handle)
        raise _to_social_read_http_exception(exc) from exc


@router.get("/profiles/{platform}/{account_handle}/hashtags")
def get_social_account_profile_hashtags_route(
    platform: str,
    account_handle: str,
    window: Literal["all", "30d", "365d"] | None = Query(default=None),
    assignment_status: Literal["all", "assigned", "unassigned"] | None = Query(default="all"),
    _: InternalAdminUser = cast("InternalAdminUser", None),
) -> dict[str, Any]:
    cache_key = _account_profile_cache_key(
        surface="hashtags",
        platform=platform,
        account_handle=account_handle,
        window=window,
        extra=("assignment_status", assignment_status),
    )
    cached_payload = _get_ttl_cached_payload(
        _ACCOUNT_PROFILE_HASHTAGS_CACHE,
        _ACCOUNT_PROFILE_HASHTAGS_CACHE_LOCK,
        cache_key,
    )
    if cached_payload is not None:
        return cached_payload
    try:
        payload = social_profile_reads.get_profile_hashtags(
            platform=platform,
            account_handle=account_handle,
            window=window,
            assignment_status=assignment_status,
        )
        _set_ttl_cached_payload(
            _ACCOUNT_PROFILE_HASHTAGS_CACHE,
            _ACCOUNT_PROFILE_HASHTAGS_CACHE_LOCK,
            cache_key,
            payload,
            ttl_seconds=_ACCOUNT_PROFILE_CACHE_TTL_SECONDS,
            max_entries=_ACCOUNT_PROFILE_CACHE_MAX_ENTRIES,
        )
        return payload
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to fetch social account profile hashtags: platform=%s account=%s",
            platform,
            account_handle,
        )
        raise _to_social_read_http_exception(exc) from exc


@router.get("/profiles/{platform}/{account_handle}/hashtags/conflicts")
def get_social_account_profile_hashtag_conflicts_route(
    platform: str,
    account_handle: str,
    limit: int = Query(default=25, ge=1, le=100),
    _: InternalAdminUser = cast("InternalAdminUser", None),
) -> dict[str, Any]:
    from trr_backend.socials.read_models.account_profile import get_social_hashtag_assignment_conflict_history

    cache_key = _account_profile_cache_key(
        surface="hashtag-conflicts",
        platform=platform,
        account_handle=account_handle,
        extra=("limit", limit),
    )
    cached_payload = _get_ttl_cached_payload(
        _ACCOUNT_PROFILE_HASHTAGS_CACHE,
        _ACCOUNT_PROFILE_HASHTAGS_CACHE_LOCK,
        cache_key,
    )
    if cached_payload is not None:
        return cached_payload
    try:
        payload = get_social_hashtag_assignment_conflict_history(limit=limit)
        _set_ttl_cached_payload(
            _ACCOUNT_PROFILE_HASHTAGS_CACHE,
            _ACCOUNT_PROFILE_HASHTAGS_CACHE_LOCK,
            cache_key,
            payload,
            ttl_seconds=_ACCOUNT_PROFILE_CACHE_TTL_SECONDS,
            max_entries=_ACCOUNT_PROFILE_CACHE_MAX_ENTRIES,
        )
        return payload
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to fetch social account profile hashtag conflicts: platform=%s account=%s",
            platform,
            account_handle,
        )
        raise _to_social_read_http_exception(exc) from exc


@router.get("/profiles/{platform}/{account_handle}/hashtags/timeline")
def get_social_account_profile_hashtag_timeline_route(
    platform: str,
    account_handle: str,
    window: Literal["all", "30d", "365d"] | None = Query(default=None),
    _: InternalAdminUser = cast("InternalAdminUser", None),
) -> dict[str, Any]:
    cache_key = _account_profile_cache_key(
        surface="hashtags_timeline",
        platform=platform,
        account_handle=account_handle,
        window=window,
    )
    cached_payload = _get_ttl_cached_payload(
        _ACCOUNT_PROFILE_HASHTAG_TIMELINE_CACHE,
        _ACCOUNT_PROFILE_HASHTAG_TIMELINE_CACHE_LOCK,
        cache_key,
    )
    if cached_payload is not None:
        return cached_payload
    try:
        payload = social_profile_reads.get_profile_hashtag_timeline(
            platform=platform,
            account_handle=account_handle,
            window=window,
        )
        _set_ttl_cached_payload(
            _ACCOUNT_PROFILE_HASHTAG_TIMELINE_CACHE,
            _ACCOUNT_PROFILE_HASHTAG_TIMELINE_CACHE_LOCK,
            cache_key,
            payload,
            ttl_seconds=_ACCOUNT_PROFILE_CACHE_TTL_SECONDS,
            max_entries=_ACCOUNT_PROFILE_CACHE_MAX_ENTRIES,
        )
        return payload
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc


@router.put("/profiles/{platform}/{account_handle}/hashtags")
def put_social_account_profile_hashtags_route(
    platform: str,
    account_handle: str,
    payload: SocialAccountProfileHashtagsPutRequest,
    user: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.shared_accounts import put_social_account_profile_hashtags

    try:
        response = put_social_account_profile_hashtags(
            platform=platform,
            account_handle=account_handle,
            hashtags=[item.model_dump() for item in payload.hashtags],
            updated_by=(user or {}).get("email"),
        )
        _clear_account_profile_caches()
        return response
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc


@router.get("/profiles/{platform}/{account_handle}/collaborators-tags")
def get_social_account_profile_collaborators_tags_route(
    platform: str,
    account_handle: str,
    _: InternalAdminUser = cast("InternalAdminUser", None),
) -> dict[str, Any]:
    cache_key = _account_profile_cache_key(
        surface="collaborators-tags",
        platform=platform,
        account_handle=account_handle,
    )
    cached_payload = _get_ttl_cached_payload(
        _ACCOUNT_PROFILE_COLLABORATORS_CACHE,
        _ACCOUNT_PROFILE_COLLABORATORS_CACHE_LOCK,
        cache_key,
    )
    if cached_payload is not None:
        return cached_payload
    try:
        payload = social_profile_reads.get_profile_collaborators_tags(
            platform=platform,
            account_handle=account_handle,
        )
        _set_ttl_cached_payload(
            _ACCOUNT_PROFILE_COLLABORATORS_CACHE,
            _ACCOUNT_PROFILE_COLLABORATORS_CACHE_LOCK,
            cache_key,
            payload,
            ttl_seconds=_ACCOUNT_PROFILE_CACHE_TTL_SECONDS,
            max_entries=_ACCOUNT_PROFILE_CACHE_MAX_ENTRIES,
        )
        return payload
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc


__all__ = [name for name in globals() if not name.startswith("__")]
