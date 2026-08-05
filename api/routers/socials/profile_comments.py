# ruff: noqa: F401, F403, F405, I001
"""Profile-scoped comment read, audit, scrape, and run-control routes."""
from __future__ import annotations

from fastapi import APIRouter

from ._shared import *
from .profile_reads import *

router = APIRouter()

@router.get("/profiles/{platform}/{account_handle}/comments")
def get_social_account_profile_comments_route(
    platform: str,
    account_handle: str,
    page: int = Query(default=1, ge=1, le=10_000),
    page_size: int = Query(default=25, ge=1, le=100),
    post_source_id: str | None = Query(default=None),
    search: str | None = Query(default=None),
    sort_by: str | None = Query(default=None),
    sort_dir: str | None = Query(default=None),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    started_at = perf_counter()
    cache_key = _account_profile_cache_key(
        surface="comments",
        platform=platform,
        account_handle=account_handle,
        page=page,
        page_size=page_size,
        post_source_id=post_source_id,
        search=search,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )
    cached_payload = _get_ttl_cached_payload(
        _ACCOUNT_PROFILE_POSTS_CACHE,
        _ACCOUNT_PROFILE_POSTS_CACHE_LOCK,
        cache_key,
    )
    if cached_payload is not None:
        log_read_path(
            "social-account-profile-comments",
            latency_ms=(perf_counter() - started_at) * 1000,
            payload=cached_payload,
            extra={
                "cache": "hit",
                "platform": platform,
                "account_handle": account_handle,
                "page": page,
                "page_size": page_size,
                "post_source_id": post_source_id,
                "search": search,
                "sort_by": sort_by,
                "sort_dir": sort_dir,
            },
        )
        return cached_payload
    try:
        payload = social_profile_reads.get_profile_comments(
            platform=platform,
            account_handle=account_handle,
            page=page,
            page_size=page_size,
            post_source_id=post_source_id,
            search=search,
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
        log_read_path(
            "social-account-profile-comments",
            latency_ms=(perf_counter() - started_at) * 1000,
            payload=payload,
            extra={
                "cache": "miss",
                "platform": platform,
                "account_handle": account_handle,
                "page": page,
                "page_size": page_size,
                "post_source_id": post_source_id,
                "search": search,
                "sort_by": sort_by,
                "sort_dir": sort_dir,
            },
        )
        return payload
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to read social account profile comments: platform=%s account=%s page=%s page_size=%s",
            platform,
            account_handle,
            page,
            page_size,
        )
        raise _to_social_read_http_exception(exc) from exc

@router.get("/profiles/{platform}/{account_handle}/comments/audit-cursor-retries")
def get_social_account_comments_audit_cursor_retries_route(
    platform: str,
    account_handle: str,
    limit: int = Query(default=50, ge=1, le=500),
    stop_reason: list[str] | None = Query(default=None),
    shortcode: list[str] | None = Query(default=None),
    show_id: list[str] | None = Query(default=None),
    season_id: list[str] | None = Query(default=None),
    show_filter: list[str] | None = Query(default=None),
    date_start: str | None = Query(default=None, max_length=64),
    date_end: str | None = Query(default=None, max_length=64),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.socials.pipelines.comments.instagram import get_instagram_comments_audit_cursor_recovery

    if platform.strip().lower() != "instagram":
        raise HTTPException(
            status_code=400,
            detail={
                "code": "SOCIAL_ACCOUNT_COMMENTS_UNSUPPORTED_PLATFORM",
                "message": "Audit cursor retries are Instagram-only.",
            },
        )
    try:
        return get_instagram_comments_audit_cursor_recovery(
            account_handle=account_handle,
            limit=limit,
            shortcodes=shortcode,
            stop_reasons=stop_reason,
            show_ids=show_id,
            season_ids=season_id,
            show_filters=show_filter,
            date_start=date_start,
            date_end=date_end,
        )
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc

@router.post("/profiles/{platform}/{account_handle}/comments/audit-cursor-retries")
async def post_social_account_comments_audit_cursor_retries_route(
    platform: str,
    account_handle: str,
    payload: SocialAccountCommentsAuditCursorRetryRequest,
    user: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.runtime import (
        SocialIngestConflictError,
        SocialIngestValidationError,
    )
    from trr_backend.socials.pipelines.comments.instagram import enqueue_instagram_comments_audit_cursor_retries

    if platform.strip().lower() != "instagram":
        raise HTTPException(
            status_code=400,
            detail={
                "code": "SOCIAL_ACCOUNT_COMMENTS_UNSUPPORTED_PLATFORM",
                "message": "Audit cursor retries are Instagram-only.",
            },
        )
    try:
        return await run_in_threadpool(
            enqueue_instagram_comments_audit_cursor_retries,
            account_handle=account_handle,
            limit=payload.limit,
            shortcodes=payload.shortcodes,
            stop_reasons=payload.stop_reasons,
            show_ids=payload.show_ids,
            season_ids=payload.season_ids,
            show_filters=[
                *list(payload.show_filters or []),
                *([payload.show_filter] if payload.show_filter else []),
            ],
            batch_size=payload.batch_size,
            comments_worker_count=payload.comments_worker_count,
            max_comments_per_post=payload.max_comments_per_post,
            comments_load_strategy=payload.comments_load_strategy,
            date_start=payload.date_start,
            date_end=payload.date_end,
            skip_launch_auth_probe=payload.skip_launch_auth_probe,
            dry_run=payload.dry_run,
            attach_to_active_run=payload.attach_to_active_run,
            dispatch_immediately=payload.dispatch_immediately,
            force_rerun_existing=payload.force_rerun_existing,
            initiated_by=(user or {}).get("email"),
        )
    except SocialIngestConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": exc.code, "message": str(exc), **jsonable_encoder(exc.detail)},
        ) from exc
    except SocialIngestValidationError as exc:
        raise HTTPException(
            status_code=400,
            detail={"code": exc.code, "message": str(exc), **jsonable_encoder(getattr(exc, "detail", {}) or {})},
        ) from exc

@router.post("/profiles/{platform}/{account_handle}/comments/scrape")
async def post_social_account_comments_scrape_route(
    platform: str,
    account_handle: str,
    payload: SocialAccountCommentsScrapeRequest,
    background_tasks: BackgroundTasks,
    user: InternalAdminUser,
    dry_run: bool = Query(default=False),
) -> dict[str, Any]:
    from trr_backend.socials.pipelines.account_catalog.launch import (
        INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
        INSTAGRAM_COMMENTS_SCRAPLING_WORKER_LANE,
    )
    from trr_backend.socials.control_plane.runtime import (
        SocialIngestConflictError,
        SocialIngestValidationError,
        SocialWorkerUnavailableError,
    )
    from trr_backend.socials.pipelines.comments.instagram import _dispatch_due_social_jobs_in_background
    from trr_backend.socials.pipelines.comments.instagram import (
        preview_social_account_comments_scrape,
        start_social_account_comments_scrape,
    )

    if dry_run or payload.dry_run:
        try:
            return await run_in_threadpool(
                preview_social_account_comments_scrape,
                platform=platform,
                account_handle=account_handle,
                mode=payload.mode,
                source_id=payload.source_id,
                max_posts=payload.max_posts,
                refresh_policy=payload.refresh_policy,
                target_filter=payload.target_filter,
                comments_load_strategy=payload.comments_load_strategy,
                date_start=payload.date_start,
                date_end=payload.date_end,
            )
        except SocialIngestValidationError as exc:
            raise HTTPException(status_code=400, detail={"code": exc.code, "message": str(exc)}) from exc

    execution_state = await run_in_threadpool(
        _resolve_social_account_comments_route_execution,
        allow_inline_dev_fallback=payload.allow_inline_dev_fallback,
        platform=platform,
    )
    queue_enabled = bool(execution_state["queue_enabled"])
    used_inline_fallback = bool(execution_state["used_inline_fallback"])
    requires_modal_executor = bool(execution_state["requires_modal_executor"])
    try:
        result = await run_in_threadpool(
            start_social_account_comments_scrape,
            platform=platform,
            account_handle=account_handle,
            mode=payload.mode,
            source_scope=payload.source_scope,
            source_id=payload.source_id,
            max_posts=payload.max_posts,
            max_comments_per_post=payload.max_comments_per_post,
            refresh_policy=payload.refresh_policy,
            target_filter=payload.target_filter,
            comments_load_strategy=payload.comments_load_strategy,
            comments_worker_count=payload.comments_worker_count,
            comments_target_batch_size=payload.comments_target_batch_size,
            date_start=payload.date_start,
            date_end=payload.date_end,
            initiated_by=(user or {}).get("email"),
            inline_worker_id=None if queue_enabled else f"api-background:comments:{platform}",
            allow_local_dev_inline_bypass=used_inline_fallback,
            dispatch_immediately=not queue_enabled,
        )
        if (
            queue_enabled
            and result.get("run_id")
            and str(payload.mode or "").strip().lower() == "profile"
            and str(payload.target_filter or "").strip().lower() == "incomplete"
        ):
            background_tasks.add_task(
                _enqueue_instagram_comments_audit_cursor_retries_background,
                account_handle=account_handle,
                limit=50,
                batch_size=1,
                max_comments_per_post=0,
                comments_load_strategy=payload.comments_load_strategy,
                date_start=payload.date_start,
                date_end=payload.date_end,
                skip_launch_auth_probe=True,
                dry_run=False,
                attach_to_active_run=True,
                dispatch_immediately=True,
                force_rerun_existing=False,
                initiated_by=(user or {}).get("email") or "comments-incomplete-fill-auto-cursor-recovery",
            )
            result["auto_audit_cursor_recovery"] = {
                "requested": True,
                "mode": "background_attach_to_active_run",
                "limit": 50,
                "batch_size": 1,
            }
        _clear_account_profile_caches()
        if queue_enabled and result.get("run_id"):
            background_tasks.add_task(_dispatch_due_social_jobs_in_background, run_id=str(result["run_id"]))
        if not queue_enabled and result.get("run_id"):
            _start_runs_in_background(
                [str(result["run_id"])],
                background_tasks,
                worker_prefix=f"api-background:comments:{platform}",
                stage=INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
                platform="instagram",
                supported_platforms=["instagram"],
                metadata_updates={"worker_lane": INSTAGRAM_COMMENTS_SCRAPLING_WORKER_LANE},
            )
        return result
    except SocialIngestConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": exc.code, "message": str(exc), **jsonable_encoder(exc.detail)},
        ) from exc
    except SocialIngestValidationError as exc:
        status_code = 503 if exc.code == "SOCIAL_INSTAGRAM_COMMENTS_AUTH_REPAIR_FAILED" else 400
        raise HTTPException(
            status_code=status_code,
            detail={"code": exc.code, "message": str(exc), **jsonable_encoder(getattr(exc, "detail", {}) or {})},
        ) from exc
    except SocialWorkerUnavailableError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "SOCIAL_MODAL_EXECUTOR_REQUIRED" if requires_modal_executor else "SOCIAL_WORKER_UNAVAILABLE",
                "message": (
                    "Instagram comments scraping requires the Modal remote executor."
                    if requires_modal_executor
                    else str(exc)
                ),
                "execution_mode": canonical_execution_mode(),
                "execution_owner": execution_owner_label(),
                "worker_health": _worker_health_detail(exc.worker_health),
                "required_worker_lane": None if requires_modal_executor else INSTAGRAM_COMMENTS_SCRAPLING_WORKER_LANE,
                "required_execution_backend": "modal" if requires_modal_executor else None,
            },
        ) from exc

@router.get("/profiles/{platform}/{account_handle}/comments/runs/{run_id}/progress")
def get_social_account_comments_scrape_progress_route(
    platform: str,
    account_handle: str,
    run_id: UUID,
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.socials.pipelines.comments.instagram import get_social_account_comments_scrape_run_progress

    cache_key = _account_profile_cache_key(
        surface="comments-run-progress",
        platform=platform,
        account_handle=account_handle,
        extra=str(run_id),
    )
    try:
        return _resolve_account_profile_singleflight(
            cache_key,
            lambda: get_social_account_comments_scrape_run_progress(
                platform=platform,
                account_handle=account_handle,
                run_id=str(run_id),
                auto_rebalance_slow_shards=False,
            ),
        )
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc

@router.post("/profiles/{platform}/{account_handle}/comments/runs/{run_id}/rebalance")
def post_social_account_comments_run_rebalance_route(
    platform: str,
    account_handle: str,
    run_id: UUID,
    _: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.socials.pipelines.comments.instagram import rebalance_slow_instagram_comments_shards

    if platform.strip().lower() != "instagram":
        raise HTTPException(
            status_code=400,
            detail={
                "code": "UNSUPPORTED_PLATFORM",
                "message": "Comments shard rebalance is only supported for Instagram.",
            },
        )
    if not account_handle.strip():
        raise HTTPException(
            status_code=400,
            detail={"code": "ACCOUNT_HANDLE_REQUIRED", "message": "account_handle is required."},
        )
    try:
        result = rebalance_slow_instagram_comments_shards(run_id=str(run_id))
        _clear_account_profile_caches()
        return result
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc

@router.post("/profiles/{platform}/{account_handle}/comments/runs/{run_id}/resume")
def post_social_account_comments_run_resume_route(
    platform: str,
    account_handle: str,
    run_id: UUID,
    user: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.runtime import (
        SocialIngestConflictError,
        SocialIngestValidationError,
        SocialWorkerUnavailableError,
    )
    from trr_backend.socials.pipelines.comments.instagram import resume_social_account_comments_run

    try:
        result = resume_social_account_comments_run(
            platform=platform,
            account_handle=account_handle,
            run_id=str(run_id),
            initiated_by=(user or {}).get("email"),
        )
        _clear_account_profile_caches()
        return result
    except SocialIngestConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": exc.code, "message": str(exc), **jsonable_encoder(exc.detail)},
        ) from exc
    except SocialIngestValidationError as exc:
        status_code = 503 if exc.code == "SOCIAL_INSTAGRAM_COMMENTS_AUTH_REPAIR_FAILED" else 400
        raise HTTPException(status_code=status_code, detail={"code": exc.code, "message": str(exc)}) from exc
    except SocialWorkerUnavailableError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "SOCIAL_WORKER_UNAVAILABLE",
                "message": str(exc),
                "execution_mode": canonical_execution_mode(),
                "execution_owner": execution_owner_label(),
                "worker_health": _worker_health_detail(exc.worker_health),
            },
        ) from exc
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc

@router.post("/profiles/{platform}/{account_handle}/comments/runs/{run_id}/repair-auth")
async def post_social_account_comments_run_repair_auth_route(
    platform: str,
    account_handle: str,
    run_id: UUID,
    payload: CatalogRepairAuthRequest,
    background_tasks: BackgroundTasks,
    user: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.runtime import SocialIngestValidationError
    from trr_backend.socials.pipelines.comments.instagram import (
        execute_social_account_comments_run_auth_repair,
        request_social_account_comments_run_auth_repair,
    )

    try:
        _require_instagram_auth_refresh_confirmation(platform, payload.operator_confirmation)
        result = request_social_account_comments_run_auth_repair(
            platform=platform,
            account_handle=account_handle,
            run_id=str(run_id),
            initiated_by=(user or {}).get("email"),
        )
        background_tasks.add_task(
            execute_social_account_comments_run_auth_repair,
            platform=platform,
            account_handle=account_handle,
            run_id=str(run_id),
            initiated_by=(user or {}).get("email"),
            allow_cookie_refresh=bool(payload.allow_cookie_refresh),
        )
        _clear_account_profile_caches()
        return result
    except SocialIngestValidationError as exc:
        raise HTTPException(status_code=400, detail={"code": exc.code, "message": str(exc)}) from exc
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc

@router.post("/profiles/{platform}/{account_handle}/comments/runs/{run_id}/public-recovery")
async def post_social_account_comments_run_public_recovery_route(
    platform: str,
    account_handle: str,
    run_id: UUID,
    payload: SocialAccountCommentsPublicRecoveryRequest,
    user: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.runtime import (
        SocialIngestConflictError,
        SocialIngestValidationError,
        SocialWorkerUnavailableError,
    )
    from trr_backend.socials.pipelines.comments.instagram import start_social_account_comments_public_recovery

    try:
        result = await run_in_threadpool(
            start_social_account_comments_public_recovery,
            platform=platform,
            account_handle=account_handle,
            run_id=str(run_id),
            comments_worker_count=payload.comments_worker_count,
            comments_target_batch_size=payload.comments_target_batch_size,
            comments_enable_media_followups=payload.comments_enable_media_followups,
            dispatch_immediately=payload.dispatch_immediately,
            dry_run=payload.dry_run,
            initiated_by=(user or {}).get("email"),
        )
        _clear_account_profile_caches()
        return result
    except SocialIngestConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": exc.code, "message": str(exc), **jsonable_encoder(exc.detail)},
        ) from exc
    except SocialIngestValidationError as exc:
        raise HTTPException(
            status_code=400,
            detail={"code": exc.code, "message": str(exc), **jsonable_encoder(getattr(exc, "detail", {}) or {})},
        ) from exc
    except SocialWorkerUnavailableError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "SOCIAL_WORKER_UNAVAILABLE",
                "message": str(exc),
                "execution_mode": canonical_execution_mode(),
                "execution_owner": execution_owner_label(),
                "worker_health": _worker_health_detail(exc.worker_health),
            },
        ) from exc
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc

@router.post("/profiles/{platform}/{account_handle}/comments/runs/{run_id}/authenticated-followup")
async def post_social_account_comments_run_authenticated_followup_route(
    platform: str,
    account_handle: str,
    run_id: UUID,
    payload: SocialAccountCommentsAuthenticatedFollowupRequest,
    user: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.runtime import (
        SocialIngestConflictError,
        SocialIngestValidationError,
        SocialWorkerUnavailableError,
    )
    from trr_backend.socials.pipelines.comments.instagram import start_social_account_comments_authenticated_followup

    try:
        if not payload.dry_run:
            _require_instagram_auth_refresh_confirmation(platform, payload.operator_confirmation)
        result = await run_in_threadpool(
            start_social_account_comments_authenticated_followup,
            platform=platform,
            account_handle=account_handle,
            run_id=str(run_id),
            comments_worker_count=payload.comments_worker_count,
            comments_target_batch_size=payload.comments_target_batch_size,
            comments_enable_media_followups=payload.comments_enable_media_followups,
            dispatch_immediately=payload.dispatch_immediately,
            dry_run=payload.dry_run,
            initiated_by=(user or {}).get("email"),
        )
        _clear_account_profile_caches()
        return result
    except SocialIngestConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": exc.code, "message": str(exc), **jsonable_encoder(exc.detail)},
        ) from exc
    except SocialIngestValidationError as exc:
        status_code = 503 if exc.code == "SOCIAL_INSTAGRAM_COMMENTS_AUTH_REPAIR_FAILED" else 400
        raise HTTPException(
            status_code=status_code,
            detail={"code": exc.code, "message": str(exc), **jsonable_encoder(getattr(exc, "detail", {}) or {})},
        ) from exc
    except SocialWorkerUnavailableError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "SOCIAL_WORKER_UNAVAILABLE",
                "message": str(exc),
                "execution_mode": canonical_execution_mode(),
                "execution_owner": execution_owner_label(),
                "worker_health": _worker_health_detail(exc.worker_health),
            },
        ) from exc
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc

@router.post("/profiles/{platform}/{account_handle}/comments/runs/{run_id}/cancel")
def post_social_account_comments_run_cancel_route(
    platform: str,
    account_handle: str,
    run_id: UUID,
    user: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.socials.pipelines.comments.instagram import cancel_social_account_comments_run

    try:
        result = cancel_social_account_comments_run(
            platform=platform,
            account_handle=account_handle,
            run_id=str(run_id),
            cancelled_by=(user or {}).get("email"),
        )
        _clear_account_profile_caches()
        return result
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc

@router.post("/profiles/{platform}/{account_handle}/comments/runs/{run_id}/guarded-restart")
def post_social_account_comments_run_guarded_restart_route(
    platform: str,
    account_handle: str,
    run_id: UUID,
    user: InternalAdminUser,
    use_proof_defaults: bool = Query(default=False),
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.runtime import (
        SocialIngestConflictError,
        SocialIngestValidationError,
        SocialWorkerUnavailableError,
    )
    from trr_backend.socials.pipelines.comments.instagram import (
        guarded_restart_social_account_comments_run,
    )

    try:
        result = guarded_restart_social_account_comments_run(
            platform=platform,
            account_handle=account_handle,
            run_id=str(run_id),
            initiated_by=(user or {}).get("email"),
            use_proof_defaults=use_proof_defaults,
        )
        _clear_account_profile_caches()
        return result
    except SocialIngestConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": exc.code, "message": str(exc), **jsonable_encoder(exc.detail)},
        ) from exc
    except SocialIngestValidationError as exc:
        status_code = 503 if exc.code == "SOCIAL_INSTAGRAM_COMMENTS_AUTH_REPAIR_FAILED" else 400
        raise HTTPException(status_code=status_code, detail={"code": exc.code, "message": str(exc)}) from exc
    except SocialWorkerUnavailableError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "SOCIAL_WORKER_UNAVAILABLE",
                "message": str(exc),
                "execution_mode": canonical_execution_mode(),
                "execution_owner": execution_owner_label(),
                "worker_health": _worker_health_detail(exc.worker_health),
            },
        ) from exc
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc

@router.post("/profiles/{platform}/{account_handle}/comments/runs/{run_id}/jobs/{job_id}/cancel")
def post_social_account_comments_job_cancel_route(
    platform: str,
    account_handle: str,
    run_id: UUID,
    job_id: UUID,
    user: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.socials.pipelines.comments.instagram import cancel_social_account_comments_job

    try:
        result = cancel_social_account_comments_job(
            platform=platform,
            account_handle=account_handle,
            run_id=str(run_id),
            job_id=str(job_id),
            cancelled_by=(user or {}).get("email"),
        )
        _clear_account_profile_caches()
        return result
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc

__all__ = [name for name in globals() if not name.startswith("__")]
