# ruff: noqa: F403, F405, I001, UP037
"""Worker health, queue status, live-status, and job-debug route surface."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from fastapi import APIRouter

from ._shared import *
from ._surfaces import RouteRecord, routes_matching

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable

    # These underscore-prefixed helpers are re-exported at runtime by
    # ``from ._shared import *`` via _shared's dynamic ``__all__``; the
    # declarations below only make them visible to static type checkers.
    from ._shared import (
        _internal_error_response,
        _run_admin_repo_call,
        _to_social_read_http_exception,
    )

router = APIRouter()


class CancelStuckJobsRequest(BaseModel):
    job_ids: list[UUID] | None = Field(default=None, max_length=500)


class RecoverStaleMediaMirrorJobsRequest(BaseModel):
    run_id: UUID
    stage: Literal["media_mirror", "comment_media_mirror", "all"] = Field(default="media_mirror")
    stale_after_seconds: int = Field(default=900, ge=30, le=86_400)
    recover_limit: int = Field(default=5, ge=1, le=250)
    dispatch_limit: int = Field(default=8, ge=1, le=250)
    skip_dispatch: bool = Field(default=False)
    confirm_recovery: str


class DrainMediaMirrorAccountRequest(BaseModel):
    run_id: UUID
    account_handle: str = Field(min_length=1, max_length=128)
    stage: Literal["media_mirror", "comment_media_mirror", "all"] = Field(default="media_mirror")
    stale_after_seconds: int = Field(default=900, ge=30, le=86_400)
    recover_limit: int = Field(default=25, ge=1, le=250)
    dispatch_limit: int = Field(default=8, ge=1, le=250)
    dry_run: bool = Field(default=False)
    confirm_drain: str


class DismissRecentFailuresRequest(BaseModel):
    job_ids: list[UUID] = Field(default_factory=list, max_length=500)
    dismiss_all_visible: bool = False


class ResetSocialIngestHealthRequest(BaseModel):
    pass


class PurgeInactiveWorkersRequest(BaseModel):
    stale_after_seconds: int | None = Field(default=None, ge=5, le=86_400)


class JobDebugRequest(BaseModel):
    apply_patch: bool = Field(default=False)
    confirm_apply: bool = Field(default=False)
    include_context: bool = Field(default=True)


@router.get("/ingest/worker-health")
def get_social_ingest_worker_health(_: InternalAdminUser = cast(Any, None)) -> dict:
    from trr_backend.socials.control_plane.worker_health import (
        get_worker_health,
        is_queue_enabled,
    )

    started_at = perf_counter()
    try:
        health = get_worker_health()
        payload = {"queue_enabled": is_queue_enabled(), **health}
        log_read_path(
            "season-social-worker-health",
            latency_ms=(perf_counter() - started_at) * 1000,
            payload=payload,
        )
        return payload
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch social ingest worker health")
        raise _to_social_read_http_exception(exc) from exc


@router.get("/ingest/backfill-health")
def get_social_ingest_backfill_health(
    run_limit: int = Query(
        default=40,
        ge=1,
        le=200,
        description="Max number of recent catalog-backfill runs to enumerate.",
    ),
    recent_log_limit: int = Query(
        default=20,
        ge=1,
        le=100,
        description="Per-run recent-log rows scanned for 401/403/checkpoint classification.",
    ),
    include_terminal_runs: bool = Query(
        default=True,
        description="Include terminal (completed/failed) runs alongside active ones.",
    ),
    _: InternalAdminUser = cast(Any, None),
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.backfill_health import get_backfill_health

    started_at = perf_counter()
    try:
        payload = get_backfill_health(
            run_limit=run_limit,
            recent_log_limit=recent_log_limit,
            include_terminal_runs=include_terminal_runs,
        )
        log_read_path(
            "season-social-backfill-health",
            latency_ms=(perf_counter() - started_at) * 1000,
            payload=payload,
        )
        return payload
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch social ingest backfill health")
        raise _to_social_read_http_exception(exc) from exc


@router.get("/ingest/workers/{worker_id}/detail")
def get_social_ingest_worker_detail(worker_id: str, _: InternalAdminUser = cast(Any, None)) -> dict[str, Any]:
    from trr_backend.socials.control_plane.queue_status import _legacy_repo

    try:
        return _legacy_repo().get_worker_detail(worker_id)
    except ValueError as exc:
        if str(exc) == "worker_not_found":
            raise HTTPException(status_code=404, detail="Worker not found") from exc
        if str(exc) == "worker_heartbeat_schema_missing":
            raise HTTPException(status_code=503, detail="Worker heartbeat schema is not available") from exc
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch social ingest worker detail: worker_id=%s", worker_id)
        raise _internal_error_response(exc) from exc


@router.post("/ingest/workers/purge-inactive")
def purge_social_ingest_inactive_workers(
    payload: PurgeInactiveWorkersRequest | None = None,
    _: InternalAdminUser = cast(Any, None),
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.queue_status import _legacy_repo as _repo

    try:
        return _repo().purge_inactive_workers(stale_after_seconds=(payload.stale_after_seconds if payload else None))
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to purge inactive social ingest workers")
        raise _internal_error_response(exc) from exc


@router.get("/ingest/queue-status")
def get_social_ingest_queue_status(
    fresh: bool = Query(default=False, description="Bypass queue-status TTL cache when true"),
    detail: Literal["summary", "full"] | None = Query(
        default=None,
        description="Use full diagnostics only for explicit operator refreshes",
    ),
    include_recent_failures: bool = Query(default=False, description="Include recent failed queue jobs"),
    include_stuck_jobs: bool = Query(default=False, description="Include stuck job detail and stale claim buckets"),
    include_runs_summary: bool = Query(default=False, description="Include scrape run status aggregates"),
    summary_only: bool = Query(default=True, description="Use the bounded status summary path"),
    statement_timeout_ms: int = Query(default=2000, ge=1000, le=30000),
    _: InternalAdminUser = cast(Any, None),
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.queue_status import _legacy_repo

    try:
        full_diagnostics = detail == "full" or (fresh and detail != "summary")
        effective_summary_only = False if full_diagnostics else summary_only
        effective_include_recent_failures = include_recent_failures or full_diagnostics
        effective_include_stuck_jobs = include_stuck_jobs or full_diagnostics
        effective_include_runs_summary = include_runs_summary or full_diagnostics
        return _legacy_repo().get_queue_status(
            fresh=fresh,
            include_recent_failures=effective_include_recent_failures,
            include_stuck_jobs=effective_include_stuck_jobs,
            include_runs_summary=effective_include_runs_summary,
            summary_only=effective_summary_only,
            statement_timeout_ms=statement_timeout_ms,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch social ingest queue status")
        raise _to_social_read_http_exception(exc) from exc


@router.post("/ingest/stuck-jobs/cancel")
def cancel_social_ingest_stuck_jobs(
    payload: CancelStuckJobsRequest | None = None,
    user: InternalAdminUser = cast(Any, None),
) -> dict[str, Any]:
    from trr_backend.socials.control_plane import recovery as recovery_control_plane

    try:
        job_ids = [str(job_id) for job_id in (payload.job_ids if payload and payload.job_ids else [])]
        return recovery_control_plane.cancel_stuck_jobs(
            job_ids=job_ids or None,
            cancelled_by=(user or {}).get("email"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to cancel stuck social ingest jobs")
        raise _internal_error_response(exc) from exc


@router.post("/ingest/media-mirror/recover-stale")
def recover_stale_social_media_mirror_jobs(
    payload: RecoverStaleMediaMirrorJobsRequest,
    user: InternalAdminUser = cast(Any, None),
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.dispatch_runtime import dispatch_due_social_jobs
    from trr_backend.socials.control_plane import recovery as recovery_control_plane

    confirm_required = "RECOVER MEDIA MIRROR JOBS"
    if payload.confirm_recovery != confirm_required:
        raise HTTPException(
            status_code=400,
            detail=f"confirm_recovery must equal {confirm_required!r}",
        )

    try:
        stages = ["media_mirror", "comment_media_mirror"] if payload.stage == "all" else [payload.stage]
        recovered_by_stage: dict[str, list[str]] = {}
        recovered_job_ids: list[str] = []
        for stage in stages:
            recovered = recovery_control_plane.recover_stale_running_jobs(
                run_id=str(payload.run_id),
                stage=stage,
                platform="instagram",
                stale_after_seconds=payload.stale_after_seconds,
                limit=payload.recover_limit,
            )
            stage_job_ids = [str(row.get("id") or "").strip() for row in recovered if str(row.get("id") or "").strip()]
            recovered_by_stage[stage] = stage_job_ids
            recovered_job_ids.extend(stage_job_ids)

        dispatch = (
            {"dispatched_job_ids": [], "dispatch_attempts": 0, "skipped": True}
            if payload.skip_dispatch
            else dispatch_due_social_jobs(run_id=str(payload.run_id), limit=payload.dispatch_limit)
        )
        return {
            "ok": True,
            "run_id": str(payload.run_id),
            "stage": payload.stage,
            "stages": stages,
            "stale_after_seconds": payload.stale_after_seconds,
            "recover_limit": payload.recover_limit,
            "dispatch_limit": payload.dispatch_limit,
            "recovered_job_ids": recovered_job_ids,
            "recovered_by_stage": recovered_by_stage,
            "recovered_count": len(recovered_job_ids),
            "dispatch": dispatch,
            "initiated_by": (user or {}).get("email"),
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to recover stale social media mirror jobs")
        raise _internal_error_response(exc) from exc


@router.post("/ingest/media-mirror/drain-account")
def drain_social_media_mirror_account_jobs(
    payload: DrainMediaMirrorAccountRequest,
    user: InternalAdminUser = cast(Any, None),
) -> dict[str, Any]:
    # ``drain_media_mirror_account_jobs`` is published into the launch module's
    # globals at runtime by its provider bridge, so static checkers cannot see
    # it as an import symbol; resolve it via the module object with an explicit type.
    from trr_backend.socials.pipelines.account_catalog import launch as _account_catalog_launch

    drain_media_mirror_account_jobs: Callable[..., dict[str, Any]] = cast(
        Any, _account_catalog_launch
    ).drain_media_mirror_account_jobs

    confirm_required = "DRAIN BRAVO MEDIA"
    if payload.confirm_drain != confirm_required:
        raise HTTPException(
            status_code=400,
            detail=f"confirm_drain must equal {confirm_required!r}",
        )
    normalized_account = payload.account_handle.strip().lower().lstrip("@")
    if not normalized_account:
        raise HTTPException(status_code=400, detail="account_handle is required")

    try:
        result = drain_media_mirror_account_jobs(
            run_id=str(payload.run_id),
            account_handle=normalized_account,
            stage=payload.stage,
            stale_after_seconds=payload.stale_after_seconds,
            recover_limit=payload.recover_limit,
            dispatch_limit=payload.dispatch_limit,
            dry_run=payload.dry_run,
        )
        result["initiated_by"] = (user or {}).get("email")
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to drain social media mirror account jobs")
        raise _internal_error_response(exc) from exc


@router.post("/ingest/dispatch-blocked-jobs/cancel")
def cancel_social_ingest_dispatch_blocked_jobs(
    payload: CancelStuckJobsRequest | None = None,
    user: InternalAdminUser = cast(Any, None),
) -> dict[str, Any]:
    from trr_backend.socials.control_plane import recovery as recovery_control_plane

    try:
        job_ids = [str(job_id) for job_id in (payload.job_ids if payload and payload.job_ids else [])]
        return recovery_control_plane.cancel_dispatch_blocked_jobs(
            job_ids=job_ids or None,
            cancelled_by=(user or {}).get("email"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to cancel dispatch-blocked social ingest jobs")
        raise _internal_error_response(exc) from exc


@router.post("/ingest/active-jobs/cancel")
def cancel_social_ingest_active_jobs(
    user: InternalAdminUser = cast(Any, None),
) -> dict[str, Any]:
    from trr_backend.socials.control_plane import recovery as recovery_control_plane

    try:
        return recovery_control_plane.cancel_active_jobs(cancelled_by=(user or {}).get("email"))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to cancel active social ingest jobs")
        raise _internal_error_response(exc) from exc


@router.post("/ingest/recent-failures/dismiss")
def dismiss_social_ingest_recent_failures(
    payload: DismissRecentFailuresRequest | None = None,
    user: InternalAdminUser = cast(Any, None),
) -> dict[str, Any]:
    from trr_backend.socials.control_plane import recovery as recovery_control_plane

    try:
        job_ids = [str(job_id) for job_id in (payload.job_ids if payload else [])]
        return recovery_control_plane.dismiss_recent_failures(
            job_ids=job_ids,
            dismiss_all_visible=bool(payload.dismiss_all_visible) if payload else False,
            dismissed_by=(user or {}).get("email"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to dismiss recent social ingest failures")
        raise _internal_error_response(exc) from exc


@router.post("/ingest/reset-health")
def reset_social_ingest_health_route(
    payload: ResetSocialIngestHealthRequest | None = None,
    user: InternalAdminUser = cast(Any, None),
) -> dict[str, Any]:
    del payload
    from trr_backend.socials.control_plane import recovery as recovery_control_plane

    try:
        return recovery_control_plane.reset_social_ingest_health(reset_by=(user or {}).get("email"))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to reset social ingest health")
        raise _internal_error_response(exc) from exc


@router.post("/ingest/jobs/{job_id}/debug")
def debug_social_ingest_job(
    job_id: UUID,
    payload: JobDebugRequest | None = None,
    _: InternalAdminUser = cast(Any, None),
) -> dict[str, Any]:
    from trr_backend.socials.control_plane import recovery as recovery_control_plane

    try:
        request_payload = payload or JobDebugRequest()
        return recovery_control_plane.debug_ingest_job_with_openai(
            str(job_id),
            apply_patch=request_payload.apply_patch,
            confirm_apply=request_payload.confirm_apply,
            include_context=request_payload.include_context,
        )
    except ValueError as exc:
        error_code = str(exc)
        if error_code == "job_not_found":
            raise HTTPException(status_code=404, detail="Job not found") from exc
        if error_code == "openai_api_key_missing":
            raise HTTPException(status_code=503, detail="OPENAI_API_KEY is not configured") from exc
        raise HTTPException(status_code=400, detail=error_code) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to debug social ingest job: job_id=%s", job_id)
        raise _internal_error_response(exc) from exc


@router.get("/ingest/health-dot")
def get_social_ingest_health_dot(_: InternalAdminUser = cast(Any, None)) -> dict[str, Any]:
    from trr_backend.socials.control_plane.queue_status import _legacy_repo

    try:
        status_payload = _legacy_repo().get_queue_status(
            include_recent_failures=False,
            include_stuck_jobs=False,
            include_runs_summary=False,
            summary_only=True,
            statement_timeout_ms=2000,
        )
        payload = social_live_status.build_social_ingest_health_dot(status_payload)
        payload["alerts"] = list(status_payload.get("alerts") or []) if isinstance(status_payload, dict) else []
        return payload
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch social ingest health dot payload")
        raise _internal_error_response(exc) from exc


@router.get("/live-status")
def get_social_live_status(_: InternalAdminUser = cast(Any, None)) -> dict[str, Any]:
    try:
        return social_live_status.build_live_status_payload()
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch social live status")
        raise _to_social_read_http_exception(exc) from exc


@router.get("/live-status/stream")
async def stream_social_live_status(request: Request, _: InternalAdminUser = cast(Any, None)) -> StreamingResponse:
    async def event_stream() -> AsyncGenerator[str, None]:
        while True:
            if await request.is_disconnected():
                return
            try:
                payload = await asyncio.wait_for(
                    _run_admin_repo_call(social_live_status.build_live_status_payload),
                    timeout=social_live_status.LIVE_STATUS_STREAM_FETCH_TIMEOUT_SECONDS,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Social live-status stream tick degraded", exc_info=True)
                payload = social_live_status.build_degraded_live_status_payload("live_status_stream_timeout", exc)
            yield "event: live_status\n"
            yield f"data: {json.dumps(payload, default=str)}\n\n"
            await asyncio.sleep(social_live_status.LIVE_STATUS_STREAM_INTERVAL_SECONDS)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )


ROUTE_PREFIXES = (
    "/admin/socials/ingest/",
    "/admin/socials/live-status",
)


def surface_routes(router: Any) -> list[RouteRecord]:
    return routes_matching(router, ROUTE_PREFIXES)
