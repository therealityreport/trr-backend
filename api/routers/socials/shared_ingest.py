# ruff: noqa: F401, F403, F405, I001, UP037
"""Shared-account source, ingest, review, and status routes."""
from __future__ import annotations

from fastapi import APIRouter

from ._shared import *
from .season_runs import *

router = APIRouter()

@router.get("/shared/sources")
def get_shared_account_sources_route(
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Query(default="network"),
    include_inactive: bool = Query(default=True),
    platforms: str | None = Query(default=None, description="Comma-separated platform list"),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.shared_source_config import get_shared_account_sources

    try:
        canonical_source_scope = normalize_source_scope_param(source_scope)
        return get_shared_account_sources(
            source_scope=canonical_source_scope,
            include_inactive=include_inactive,
            platforms=_parse_platform_query(platforms),
        )
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to read shared account sources: source_scope=%s", source_scope)
        raise _to_social_read_http_exception(exc) from exc

@router.put("/shared/sources")
def put_shared_account_sources_route(
    payload: SharedAccountSourcesPutRequest,
    user: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.shared_source_config import put_shared_account_sources

    try:
        return put_shared_account_sources(
            source_scope=payload.source_scope,
            sources=[source.model_dump() for source in payload.sources],
            updated_by=(user or {}).get("email"),
        )
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc

@router.post("/shared/ingest")
async def ingest_shared_social_accounts(
    payload: SharedSocialIngestRequest,
    background_tasks: BackgroundTasks,
    user: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.runtime import (
        SocialIngestValidationError,
        SocialWorkerUnavailableError,
    )
    from trr_backend.socials.control_plane.worker_health import (
        assert_worker_available_when_queue_enabled,
        is_queue_enabled,
    )
    from trr_backend.socials.control_plane import ingest_shared_accounts

    queue_enabled = is_queue_enabled()
    remote_plane_enforced = is_remote_job_plane_enabled()
    blocked_platforms = _blocked_remote_only_platforms(payload.platforms)
    requires_modal_executor = bool(blocked_platforms)
    used_inline_fallback = False
    worker_health: dict[str, Any] | None = None
    if queue_enabled:
        try:
            worker_health = await run_in_threadpool(
                assert_worker_available_when_queue_enabled,
                required_execution_backend="modal" if requires_modal_executor else None,
            )
        except SocialWorkerUnavailableError as exc:
            worker_health = exc.worker_health
            if blocked_platforms:
                raise HTTPException(
                    status_code=503,
                    detail={
                        "code": "SOCIAL_REMOTE_WORKER_REQUIRED",
                        "message": "Modal worker execution is required for platform(s): "
                        + ", ".join(blocked_platforms),
                        "required_platforms": blocked_platforms,
                        "required_execution_backend": "modal",
                        "execution_mode": canonical_execution_mode(),
                        "execution_owner": execution_owner_label(),
                        "worker_health": _worker_health_detail(exc.worker_health),
                    },
                ) from exc
            if payload.allow_inline_dev_fallback and _is_local_or_dev_runtime() and not remote_plane_enforced:
                queue_enabled = False
                used_inline_fallback = True
            else:
                raise HTTPException(
                    status_code=503,
                    detail={
                        "code": (
                            "SOCIAL_REMOTE_JOB_PLANE_ENFORCED" if remote_plane_enforced else "SOCIAL_WORKER_UNAVAILABLE"
                        ),
                        "message": _remote_worker_unavailable_message(exc) if remote_plane_enforced else str(exc),
                        "execution_mode": canonical_execution_mode(),
                        "execution_owner": execution_owner_label(),
                        "worker_health": _worker_health_detail(exc.worker_health),
                    },
                ) from exc
    elif requires_modal_executor:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "SOCIAL_REMOTE_WORKER_REQUIRED",
                "message": "Modal worker execution is required for platform(s): " + ", ".join(blocked_platforms),
                "required_platforms": blocked_platforms,
                "required_execution_backend": "modal",
                "execution_mode": canonical_execution_mode(),
                "execution_owner": execution_owner_label(),
            },
        )
    elif not remote_plane_enforced:
        used_inline_fallback = bool(payload.allow_inline_dev_fallback)
    else:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "SOCIAL_REMOTE_JOB_PLANE_ENFORCED",
                "message": (
                    "Social ingest remote-worker ownership is enforced "
                    "(TRR_JOB_PLANE_MODE=remote or TRR_LONG_JOB_ENFORCE_REMOTE=1)."
                ),
                "execution_mode": canonical_execution_mode(),
                "execution_owner": execution_owner_label(),
            },
        )

    try:
        result = ingest_shared_accounts(
            platforms=payload.platforms,
            source_scope=payload.source_scope,
            accounts_override=payload.accounts_override,
            date_start=payload.date_start,
            date_end=payload.date_end,
            initiated_by=(user or {}).get("email"),
            inline_worker_id=None if queue_enabled else "api-background:shared",
        )
    except SocialIngestValidationError as exc:
        raise HTTPException(status_code=400, detail={"code": exc.code, "message": str(exc)}) from exc
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc

    run_id = str(result.get("run_id") or "").strip()
    if not run_id:
        raise HTTPException(
            status_code=500,
            detail={"code": "INGEST_RUN_NOT_CREATED", "message": "Failed to create shared ingest run"},
        )
    if not queue_enabled:
        _start_runs_in_background([run_id], background_tasks, worker_prefix="api-background:shared")

    execution_mode, execution_mode_canonical, execution_mode_legacy = _resolve_social_execution_modes(
        queue_enabled=queue_enabled,
        used_inline_fallback=used_inline_fallback,
    )
    response_payload = {
        **result,
        "status": "queued" if queue_enabled else "started",
        "run_id": run_id,
        "execution_mode": execution_mode,
        "execution_mode_canonical": execution_mode_canonical,
        "execution_mode_legacy": execution_mode_legacy,
        "execution_owner": execution_owner_label(),
        "execution_backend_canonical": (
            str(result.get("execution_backend_canonical") or execution_metadata()["execution_backend_canonical"])
        ),
        "jobs_url": f"/api/v1/admin/socials/shared/ingest/runs?run_id={run_id}",
        "message": ("Shared account ingest queued." if queue_enabled else "Shared account ingest started inline."),
    }
    if used_inline_fallback and worker_health is not None:
        response_payload["worker_health"] = worker_health
    return response_payload

@router.get("/shared/ingest/runs")
def get_shared_ingest_runs(
    limit: int = Query(default=_INGEST_JOBS_DEFAULT_LIMIT, ge=1, le=_INGEST_JOBS_MAX_LIMIT),
    status: str | None = Query(default=None),
    source_scope: Literal["bravo", "network", "creator", "community", "news"] | None = Query(default=None),
    run_id: str | None = Query(default=None),
    _: InternalAdminUser = None,
) -> list[dict[str, Any]]:
    from trr_backend.socials.control_plane.shared_accounts import list_shared_runs

    canonical_source_scope = normalize_source_scope_param(source_scope) if source_scope is not None else None
    return list_shared_runs(limit=limit, status=status, source_scope=canonical_source_scope, run_id=run_id)

@router.post("/shared/ingest/runs/{run_id}/cancel")
def cancel_shared_ingest_run(
    run_id: str,
    user: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.shared_accounts import cancel_shared_run

    try:
        return cancel_shared_run(run_id, cancelled_by=(user or {}).get("email"))
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc

@router.get("/shared/review-queue")
def get_shared_review_queue(
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Query(default="network"),
    review_status: Literal["open", "resolved", "ignored"] = Query(default="open"),
    limit: int = Query(default=100, ge=1, le=500),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.shared_accounts import list_shared_review_queue

    try:
        canonical_source_scope = normalize_source_scope_param(source_scope)
        return list_shared_review_queue(source_scope=canonical_source_scope, review_status=review_status, limit=limit)
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to read shared review queue: source_scope=%s review_status=%s limit=%s",
            source_scope,
            review_status,
            limit,
        )
        raise _to_social_read_http_exception(exc) from exc

@router.post("/shared/review-queue/{item_id}/resolve")
def resolve_shared_review_queue(
    item_id: str,
    payload: SharedReviewResolveRequest,
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.shared_accounts import resolve_shared_review_queue_item

    try:
        return resolve_shared_review_queue_item(
            item_id,
            resolution_action=payload.resolution_action,
            resolved_show_id=str(payload.resolved_show_id) if payload.resolved_show_id else None,
            resolved_season_id=str(payload.resolved_season_id) if payload.resolved_season_id else None,
        )
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc

@router.get("/seasons/{season_id}/shared-status")
def get_season_shared_status_route(
    season_id: UUID,
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Query(default="network"),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.shared_accounts import get_season_shared_status

    started_at = perf_counter()
    try:
        canonical_source_scope = normalize_source_scope_param(source_scope)
        payload = get_season_shared_status(str(season_id), source_scope=canonical_source_scope)
        log_read_path(
            "season-social-shared-status",
            latency_ms=(perf_counter() - started_at) * 1000,
            payload=payload,
            extra={
                "source_scope": canonical_source_scope,
                "season_id": season_id,
            },
        )
        return payload
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to read season shared status: season=%s source_scope=%s", season_id, source_scope)
        raise _to_social_read_http_exception(exc) from exc

__all__ = [name for name in globals() if not name.startswith("__")]
