# ruff: noqa: F401, F403, F405, I001
"""Profile-scoped catalog backfill and sync routes."""
from __future__ import annotations

from fastapi import APIRouter

from ._shared import *
from .catalog_reads import *
from .catalog_operations import *

router = APIRouter()

@router.post("/profiles/{platform}/{account_handle}/catalog/backfill")
async def post_social_account_catalog_backfill_route(
    platform: str,
    account_handle: str,
    payload: CatalogBackfillRequest,
    background_tasks: BackgroundTasks,
    user: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.runtime import (
        SocialIngestConflictError,
        SocialIngestValidationError,
        SocialWorkerUnavailableError,
    )
    from trr_backend.socials.pipelines.account_catalog.launch import (
        _merge_catalog_run_config,
        _normalize_social_account_catalog_backfill_selected_tasks,
        begin_social_account_catalog_backfill_launch,
        launch_social_account_catalog_backfill,
    )
    from trr_backend.socials.control_plane.shared_accounts import _normalize_catalog_backfill_window

    execution_state = await run_in_threadpool(
        _resolve_social_account_catalog_route_execution,
        platform=platform,
        allow_inline_dev_fallback=payload.allow_inline_dev_fallback,
        execution_preference=payload.execution_preference,
    )
    queue_enabled = bool(execution_state["queue_enabled"])
    used_inline_fallback = bool(execution_state["used_inline_fallback"])
    requires_modal_executor = bool(execution_state["requires_modal_executor"])

    try:
        normalized_platform = str(platform or "").strip().lower()
        if normalized_platform == "twitter" and payload.backfill_scope == "full_history":
            date_start, date_end = _twitter_catalog_backfill_default_window()
        else:
            date_start = payload.date_start if payload.backfill_scope == "bounded_window" else None
            date_end = payload.date_end if payload.backfill_scope == "bounded_window" else None
        date_start, date_end = _normalize_catalog_backfill_window(
            date_start=date_start,
            date_end=date_end,
        )
        request_selected_tasks = payload.selected_tasks
        if request_selected_tasks is None:
            # Defense in depth: an omitted selected_tasks runs ALL lanes
            # (post_details + comments + media) for every platform, so a backfill
            # can never silently drop comments/media (the Finding-1 gap). The app
            # always sends explicit tasks; this only governs API callers that omit them.
            request_selected_tasks = _normalize_social_account_catalog_backfill_selected_tasks(None)
        normalized_selected_tasks = _normalize_social_account_catalog_backfill_selected_tasks(request_selected_tasks)
        requires_apply_confirmation = _instagram_backfill_requires_apply_confirmation(
            platform=normalized_platform,
            date_start=date_start,
            date_end=date_end,
            selected_tasks=normalized_selected_tasks,
        )
        use_async_catalog_kickoff = queue_enabled or (used_inline_fallback and normalized_platform == "instagram")
        if payload.apply_run_id is not None:
            if not requires_apply_confirmation:
                raise ValueError("apply_run_id is only supported for Instagram 2025 backfill apply.")
            apply_run_id = str(payload.apply_run_id)
            required_confirmation = _instagram_2025_backfill_apply_confirmation(apply_run_id)
            if str(payload.operator_confirmation or "").strip() != required_confirmation:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "code": "INSTAGRAM_2025_BACKFILL_APPLY_CONFIRMATION_REQUIRED",
                        "message": "Confirm Live APPLY before catalog jobs are created.",
                        "run_id": apply_run_id,
                        "required_confirmation": required_confirmation,
                    },
                )
            result = {
                "run_id": apply_run_id,
                "status": "queued",
                "platform": normalized_platform,
                "account_handle": account_handle,
                "selected_tasks": normalized_selected_tasks,
                "effective_selected_tasks": normalized_selected_tasks,
                "catalog_run_id": apply_run_id,
                "comments_run_id": None,
                "catalog_status": "queued",
                "comments_status": None,
                "catalog_action": "backfill",
                "catalog_action_scope": payload.backfill_scope,
                "launch_state": "finalizing",
                "launch_task_resolution_pending": True,
                "requires_apply_confirmation": False,
                "apply_required": False,
                "apply_run_id": apply_run_id,
                "enable_cap4_canary": bool(payload.enable_cap4_canary),
                "runbook_state": {
                    "phase": "live_apply",
                    "state": "applied",
                    "mandatory": True,
                    "current_comments_cap": 2,
                    "speed_canary_optional": True,
                    "speed_canary_cap": 4,
                    "minimum_completed_comments_jobs": 25,
                    "message": "Live APPLY accepted at cap 2. Catalog jobs can now be finalized.",
                },
            }
            _queue_catalog_backfill_finalize_task(
                background_tasks=background_tasks,
                platform=platform,
                account_handle=account_handle,
                run_id=apply_run_id,
                source_scope=payload.source_scope,
                date_start=date_start,
                date_end=date_end,
                initiated_by=(user or {}).get("email"),
                allow_local_dev_inline_bypass=used_inline_fallback,
                execution_preference=payload.execution_preference,
                selected_tasks=normalized_selected_tasks,
                details_refresh_worker_count=payload.detail_worker_count,
                comments_worker_count=payload.comments_worker_count,
                comments_enable_media_followups=payload.comments_enable_media_followups,
                launch_group_id=None,
                force_catalog_rediscovery=payload.force_catalog_rediscovery,
                enable_cap4_canary=payload.enable_cap4_canary,
            )
        elif use_async_catalog_kickoff:
            result = await run_in_threadpool(
                begin_social_account_catalog_backfill_launch,
                platform=platform,
                account_handle=account_handle,
                source_scope=payload.source_scope,
                date_start=date_start,
                date_end=date_end,
                initiated_by=(user or {}).get("email"),
                allow_local_dev_inline_bypass=used_inline_fallback,
                execution_preference=payload.execution_preference,
                selected_tasks=normalized_selected_tasks,
                details_refresh_worker_count=payload.detail_worker_count,
                comments_worker_count=payload.comments_worker_count,
                comments_enable_media_followups=payload.comments_enable_media_followups,
                force_catalog_rediscovery=payload.force_catalog_rediscovery,
                enable_cap4_canary=payload.enable_cap4_canary,
            )
            if requires_apply_confirmation:
                result = _attach_instagram_apply_metadata(result)
                apply_run_id = str(result.get("apply_run_id") or "").strip()
                if apply_run_id:
                    await run_in_threadpool(
                        _merge_catalog_run_config,
                        run_id=apply_run_id,
                        metadata_updates=_instagram_2025_backfill_apply_pending_metadata(apply_run_id),
                    )
            else:
                _queue_catalog_backfill_finalize_task(
                    background_tasks=background_tasks,
                    platform=platform,
                    account_handle=account_handle,
                    run_id=str(result.get("run_id") or ""),
                    source_scope=payload.source_scope,
                    date_start=date_start,
                    date_end=date_end,
                    initiated_by=(user or {}).get("email"),
                    allow_local_dev_inline_bypass=used_inline_fallback,
                    execution_preference=payload.execution_preference,
                    selected_tasks=normalized_selected_tasks,
                    details_refresh_worker_count=payload.detail_worker_count,
                    comments_worker_count=payload.comments_worker_count,
                    comments_enable_media_followups=payload.comments_enable_media_followups,
                    launch_group_id=str(result.get("launch_group_id") or ""),
                    force_catalog_rediscovery=payload.force_catalog_rediscovery,
                    enable_cap4_canary=payload.enable_cap4_canary,
                )
        else:
            result = await run_in_threadpool(
                launch_social_account_catalog_backfill,
                platform=platform,
                account_handle=account_handle,
                source_scope=payload.source_scope,
                date_start=date_start,
                date_end=date_end,
                initiated_by=(user or {}).get("email"),
                inline_worker_id=None if queue_enabled else f"api-background:catalog:{platform}",
                allow_local_dev_inline_bypass=used_inline_fallback,
                execution_preference=payload.execution_preference,
                selected_tasks=normalized_selected_tasks,
                details_refresh_worker_count=payload.detail_worker_count,
                comments_worker_count=payload.comments_worker_count,
                comments_enable_media_followups=payload.comments_enable_media_followups,
                force_catalog_rediscovery=payload.force_catalog_rediscovery,
                enable_cap4_canary=payload.enable_cap4_canary,
            )
        _clear_account_profile_caches()
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
                "code": ("SOCIAL_MODAL_EXECUTOR_REQUIRED" if requires_modal_executor else "SOCIAL_WORKER_UNAVAILABLE"),
                "message": str(exc),
                "execution_mode": canonical_execution_mode(),
                "execution_owner": execution_owner_label(),
                "worker_health": _worker_health_detail(exc.worker_health),
                "required_execution_backend": "modal" if requires_modal_executor else None,
            },
        ) from exc
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc

    return _finalize_social_account_catalog_route_response(
        result=result,
        platform=platform,
        queue_enabled=queue_enabled,
        used_inline_fallback=used_inline_fallback,
        requires_modal_executor=requires_modal_executor,
        background_tasks=background_tasks,
    )

@router.post("/profiles/{platform}/{account_handle}/catalog/remediate-drift")
async def post_social_account_catalog_remediate_drift_route(
    platform: str,
    account_handle: str,
    payload: CatalogRemediateDriftRequest,
    user: InternalAdminUser,
) -> dict[str, Any]:
    """Cancel and optionally replace a stale catalog run under the runtime-supersession guard."""
    from trr_backend.socials.pipelines.account_catalog.launch import (
        remediate_social_account_catalog_runtime_supersession,
    )

    initiated_by = (user or {}).get("email")
    try:
        return remediate_social_account_catalog_runtime_supersession(
            platform=platform,
            account_handle=account_handle,
            run_id=str(payload.run_id) if payload.run_id else None,
            requeue_canary=payload.requeue_canary,
            source_scope=payload.source_scope,
            initiated_by=initiated_by,
            cancelled_by=initiated_by,
        )
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc

@router.post("/profiles/{platform}/{account_handle}/catalog/sync-recent")
async def post_social_account_catalog_sync_recent_route(
    platform: str,
    account_handle: str,
    payload: CatalogSyncRecentRequest,
    background_tasks: BackgroundTasks,
    user: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.runtime import (
        SocialIngestConflictError,
        SocialIngestValidationError,
        SocialWorkerUnavailableError,
    )
    from trr_backend.socials.control_plane import sync_recent_social_account_catalog

    execution_state = await run_in_threadpool(
        _resolve_social_account_catalog_route_execution,
        platform=platform,
        allow_inline_dev_fallback=payload.allow_inline_dev_fallback,
    )
    queue_enabled = bool(execution_state["queue_enabled"])
    used_inline_fallback = bool(execution_state["used_inline_fallback"])
    requires_modal_executor = bool(execution_state["requires_modal_executor"])

    try:
        result = sync_recent_social_account_catalog(
            platform=platform,
            account_handle=account_handle,
            source_scope=payload.source_scope,
            lookback_days=payload.lookback_days,
            initiated_by=(user or {}).get("email"),
            inline_worker_id=None if queue_enabled else f"api-background:catalog:{platform}",
            allow_local_dev_inline_bypass=used_inline_fallback,
        )
        _clear_account_profile_caches()
    except SocialIngestConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": exc.code, "message": str(exc), **jsonable_encoder(exc.detail)},
        ) from exc
    except SocialIngestValidationError as exc:
        raise HTTPException(status_code=400, detail={"code": exc.code, "message": str(exc)}) from exc
    except SocialWorkerUnavailableError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "SOCIAL_MODAL_EXECUTOR_REQUIRED" if requires_modal_executor else "SOCIAL_WORKER_UNAVAILABLE",
                "message": str(exc),
                "execution_mode": canonical_execution_mode(),
                "execution_owner": execution_owner_label(),
                "worker_health": _worker_health_detail(exc.worker_health),
                "required_execution_backend": "modal" if requires_modal_executor else None,
            },
        ) from exc
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc

    return _finalize_social_account_catalog_route_response(
        result=result,
        platform=platform,
        queue_enabled=queue_enabled,
        used_inline_fallback=used_inline_fallback,
        requires_modal_executor=requires_modal_executor,
        background_tasks=background_tasks,
    )

@router.post("/profiles/{platform}/{account_handle}/catalog/sync-newer")
async def post_social_account_catalog_sync_newer_route(
    platform: str,
    account_handle: str,
    payload: CatalogSyncNewerRequest,
    background_tasks: BackgroundTasks,
    user: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.runtime import (
        SocialIngestConflictError,
        SocialIngestValidationError,
        SocialWorkerUnavailableError,
    )
    from trr_backend.socials.control_plane import sync_newer_social_account_catalog

    execution_state = await run_in_threadpool(
        _resolve_social_account_catalog_route_execution,
        platform=platform,
        allow_inline_dev_fallback=payload.allow_inline_dev_fallback,
    )
    queue_enabled = bool(execution_state["queue_enabled"])
    used_inline_fallback = bool(execution_state["used_inline_fallback"])
    requires_modal_executor = bool(execution_state["requires_modal_executor"])

    try:
        result = sync_newer_social_account_catalog(
            platform=platform,
            account_handle=account_handle,
            source_scope=payload.source_scope,
            initiated_by=(user or {}).get("email"),
            inline_worker_id=None if queue_enabled else f"api-background:catalog:{platform}",
            allow_local_dev_inline_bypass=used_inline_fallback,
        )
        _clear_account_profile_caches()
    except SocialIngestConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": exc.code, "message": str(exc), **jsonable_encoder(exc.detail)},
        ) from exc
    except SocialIngestValidationError as exc:
        raise HTTPException(status_code=400, detail={"code": exc.code, "message": str(exc)}) from exc
    except SocialWorkerUnavailableError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "SOCIAL_MODAL_EXECUTOR_REQUIRED" if requires_modal_executor else "SOCIAL_WORKER_UNAVAILABLE",
                "message": str(exc),
                "execution_mode": canonical_execution_mode(),
                "execution_owner": execution_owner_label(),
                "worker_health": _worker_health_detail(exc.worker_health),
                "required_execution_backend": "modal" if requires_modal_executor else None,
            },
        ) from exc
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc

    return _finalize_social_account_catalog_route_response(
        result=result,
        platform=platform,
        queue_enabled=queue_enabled,
        used_inline_fallback=used_inline_fallback,
        requires_modal_executor=requires_modal_executor,
        background_tasks=background_tasks,
    )

@router.post("/profiles/{platform}/{account_handle}/catalog/resume-tail")
async def post_social_account_catalog_resume_tail_route(
    platform: str,
    account_handle: str,
    payload: CatalogResumeTailRequest,
    background_tasks: BackgroundTasks,
    user: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.socials.pipelines.account_catalog.launch import (
        SOCIAL_ACCOUNT_CATALOG_BACKFILL_SELECTED_TASKS,
        launch_social_account_catalog_backfill,
    )
    from trr_backend.socials.control_plane.runtime import (
        SocialIngestConflictError,
        SocialIngestValidationError,
        SocialWorkerUnavailableError,
    )

    execution_state = await run_in_threadpool(
        _resolve_social_account_catalog_route_execution,
        platform=platform,
        allow_inline_dev_fallback=payload.allow_inline_dev_fallback,
    )
    queue_enabled = bool(execution_state["queue_enabled"])
    used_inline_fallback = bool(execution_state["used_inline_fallback"])
    requires_modal_executor = bool(execution_state["requires_modal_executor"])

    try:
        result = await run_in_threadpool(
            launch_social_account_catalog_backfill,
            platform=platform,
            account_handle=account_handle,
            source_scope=payload.source_scope,
            initiated_by=(user or {}).get("email"),
            inline_worker_id=None if queue_enabled else f"api-background:catalog:{platform}",
            allow_local_dev_inline_bypass=used_inline_fallback,
            selected_tasks=list(SOCIAL_ACCOUNT_CATALOG_BACKFILL_SELECTED_TASKS),
        )
        _clear_account_profile_caches()
    except SocialIngestConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": exc.code, "message": str(exc), **jsonable_encoder(exc.detail)},
        ) from exc
    except SocialIngestValidationError as exc:
        raise HTTPException(status_code=400, detail={"code": exc.code, "message": str(exc)}) from exc
    except SocialWorkerUnavailableError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "SOCIAL_MODAL_EXECUTOR_REQUIRED" if requires_modal_executor else "SOCIAL_WORKER_UNAVAILABLE",
                "message": str(exc),
                "execution_mode": canonical_execution_mode(),
                "execution_owner": execution_owner_label(),
                "worker_health": _worker_health_detail(exc.worker_health),
                "required_execution_backend": "modal" if requires_modal_executor else None,
            },
        ) from exc
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc

    return {
        **_finalize_social_account_catalog_route_response(
            result=result,
            platform=platform,
            queue_enabled=queue_enabled,
            used_inline_fallback=used_inline_fallback,
            requires_modal_executor=requires_modal_executor,
            background_tasks=background_tasks,
        ),
        "deprecated_route": True,
    }

__all__ = [name for name in globals() if not name.startswith("__")]
