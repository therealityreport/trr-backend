# ruff: noqa: F401, F403, F405, UP037
"""Season ingest run routes and shared season-ingest helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from fastapi import APIRouter

from ._shared import *

if TYPE_CHECKING:
    # These underscore-prefixed helpers are re-exported at runtime by the star
    # import above via ``_shared``'s dynamic ``__all__``; the imports below
    # only make them visible to static type checkers.
    from ._shared import (
        _internal_error_response,
        _normalize_target_platforms,
        _run_admin_repo_call,
        _to_social_read_http_exception,
        _value_error_to_bad_request,
    )

router = APIRouter()

_INGEST_JOBS_DEFAULT_LIMIT = 50

_INGEST_JOBS_MAX_LIMIT = 250

_INGEST_JOBS_DEFAULT_OFFSET = 0

_INGEST_JOBS_MAX_OFFSET = 5000


def _env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(minimum, min(maximum, value))


def _comments_run_workers_cap() -> int:
    legacy_default = _env_int("SOCIAL_INLINE_COMMENTS_WORKERS", 4, minimum=1, maximum=8)
    return _env_int("SOCIAL_COMMENTS_RUN_WORKERS", legacy_default, minimum=1, maximum=8)


_INLINE_EXECUTION_TIMEOUT_SECONDS_DEFAULT = 600


def _inline_execution_timeout_seconds() -> int:
    return _env_int(
        "SOCIAL_INLINE_EXECUTION_TIMEOUT_SECONDS",
        _INLINE_EXECUTION_TIMEOUT_SECONDS_DEFAULT,
        minimum=30,
        maximum=7200,
    )


def _execute_with_timeout(
    func: Any,
    args: tuple[Any, ...] = (),
    kwargs: dict[str, Any] | None = None,
    timeout_seconds: int = 600,
) -> Any:
    """Execute *func* in a daemon thread with a hard timeout.

    Raises ``TimeoutError`` if the function does not complete within
    *timeout_seconds*.  Any exception raised by *func* is re-raised in the
    calling thread.
    """
    kwargs = kwargs or {}
    result: list[Any] = [None]
    exception: list[BaseException | None] = [None]

    def _target() -> None:
        try:
            result[0] = func(*args, **kwargs)
        except BaseException as exc:  # noqa: BLE001
            exception[0] = exc

    thread = Thread(target=_target, daemon=True)
    thread.start()
    thread.join(timeout=timeout_seconds)
    if thread.is_alive():
        raise TimeoutError(f"Inline execution exceeded {timeout_seconds}s timeout")
    if exception[0] is not None:
        raise exception[0]
    return result[0]


def _normalize_run_summary_payload(summary: Any) -> dict[str, Any]:
    payload = dict(summary) if isinstance(summary, dict) else {}
    if not payload:
        return {}
    total_jobs = int(payload.get("total_jobs") or 0)
    completed_jobs = int(payload.get("completed_jobs") or 0)
    failed_jobs = int(payload.get("failed_jobs") or 0)
    active_jobs = int(payload.get("active_jobs") or 0)
    if completed_jobs == 0 and total_jobs > 0:
        completed_jobs = max(0, total_jobs - failed_jobs - active_jobs)
    payload["completed_jobs"] = completed_jobs
    return payload


def _remote_only_social_platforms() -> set[str]:
    default_platforms = ",".join(SOCIAL_SUPPORTED_PLATFORMS)
    raw = str(os.getenv("SOCIAL_REMOTE_ONLY_PLATFORMS") or default_platforms).strip().lower()
    if not raw or raw in {"none", "off", "disabled"}:
        return set()
    return {
        token.strip() for token in raw.split(",") if token.strip() and token.strip() in set(SOCIAL_SUPPORTED_PLATFORMS)
    }


def _blocked_remote_only_platforms(platforms: list[str] | None) -> list[str]:
    requested_platforms = set(_normalize_target_platforms(platforms))
    return sorted(requested_platforms & _remote_only_social_platforms())


def _run_inline_season_ingest(
    run_id: str,
    *,
    platforms: list[str] | None,
    ingest_mode: str,
    worker_prefix: str,
) -> None:
    from trr_backend.socials.control_plane import execute_run

    run_inline_season_ingest_execution(
        run_id,
        platforms=platforms,
        supported_platforms=SOCIAL_SUPPORTED_PLATFORMS,
        ingest_mode=ingest_mode,
        worker_prefix=worker_prefix,
        comments_workers_cap=_comments_run_workers_cap(),
        execute_run=execute_run,
        thread_pool_executor_factory=ThreadPoolExecutor,
    )


def _parse_utc_iso_datetime(value: str | None) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _resolve_ingest_window(
    *,
    season_id: str,
    source_scope: str,
    week_index: int | None,
    timezone: str,
    date_start: datetime | None,
    date_end: datetime | None,
) -> tuple[datetime | None, datetime | None, dict[str, Any] | None]:
    if week_index is None:
        return date_start, date_end, None

    from trr_backend.socials.windowing import resolve_week_window

    week_payload = resolve_week_window(
        season_id,
        week_index=week_index,
        source_scope=source_scope,
        timezone=timezone,
    )
    return (
        _parse_utc_iso_datetime(str(week_payload.get("start") or "")),
        _parse_utc_iso_datetime(str(week_payload.get("end") or "")),
        week_payload,
    )


def _build_ingest_scope_payload(
    *,
    resolved_week: dict[str, Any] | None,
    date_start: datetime | None,
    date_end: datetime | None,
    platforms: list[str] | None,
) -> dict[str, Any]:
    scope_type = "season"
    if resolved_week is not None:
        scope_type = "week"
    elif date_start is not None or date_end is not None:
        scope_type = "date_window"
    return {
        "type": scope_type,
        "platforms": _normalize_target_platforms(platforms),
        "week": resolved_week,
        "date_start": date_start.isoformat().replace("+00:00", "Z") if isinstance(date_start, datetime) else None,
        "date_end": date_end.isoformat().replace("+00:00", "Z") if isinstance(date_end, datetime) else None,
    }


class SeasonSocialTargetInput(BaseModel):
    platform: Literal["instagram", "tiktok", "twitter", "youtube", "facebook", "threads", "reddit"]
    accounts: list[str] = Field(default_factory=list)
    hashtags: list[str] = Field(default_factory=list)
    keywords: list[str] = Field(default_factory=list)
    timezone: str = Field(default="America/New_York")
    is_active: bool = Field(default=True)
    config: dict = Field(default_factory=dict)


class SeasonSocialTargetsPutRequest(SourceScopedRequest):
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Field(default="network")
    targets: list[SeasonSocialTargetInput]


class SeasonSocialIngestRequest(SourceScopedRequest):
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Field(default="network")
    platforms: list[Literal["instagram", "tiktok", "twitter", "youtube", "facebook", "threads"]] | None = Field(
        default=None
    )
    sync_strategy: Literal["incremental", "full_refresh"] = Field(default="incremental")
    comment_refresh_policy: Literal["balanced", "missing_only"] = Field(default="balanced")
    comment_anchor_source_ids: (
        dict[Literal["instagram", "tiktok", "twitter", "youtube", "facebook", "threads"], list[str]] | None
    ) = Field(default=None)
    accounts_override: list[str] | None = Field(default=None)
    hashtags_override: list[str] | None = Field(default=None)
    keywords_override: list[str] | None = Field(default=None)
    sound_ids: list[str] | None = Field(default=None, description="Optional TikTok sound IDs or sound URLs")
    max_posts_per_target: int = Field(default=0, ge=0)
    max_comments_per_post: int = Field(default=0, ge=0)
    max_replies_per_post: int = Field(default=0, ge=0)
    fetch_replies: bool = Field(default=True)
    ingest_mode: Literal["posts_only", "posts_and_comments", "comments_only", "details_refresh"] = Field(
        default="posts_and_comments"
    )
    week_index: int | None = Field(default=None, ge=0, le=200)
    timezone: str = Field(default="America/New_York", min_length=1, max_length=64)
    date_start: datetime | None = None
    date_end: datetime | None = None
    runner_strategy: Literal["single_runner", "adaptive_dual_runner"] | None = Field(default=None)
    runner_count: int | None = Field(default=None, ge=1, le=2)
    window_shard_hours: int | None = Field(default=None, ge=1, le=24)
    runner_b_start_offset_hours: int | None = Field(default=None, ge=0, le=168)
    day_weight_profile: Literal["default", "rhoslc_default"] | None = Field(default=None)
    priority_mode: Literal["default", "episode_peak_weighted"] | None = Field(default=None)
    youtube_source_mode: Literal["hybrid", "api_only", "scraper_only"] | None = Field(default=None)
    youtube_force_reindex: bool = Field(default=False)
    youtube_force_media_refresh: bool = Field(default=False)
    youtube_force_comment_refresh: bool = Field(default=False)
    allow_inline_dev_fallback: bool = Field(default=False)
    client_session_id: str | None = Field(default=None, max_length=200)
    client_workflow_id: str | None = Field(default=None, max_length=200)


class SeasonSocialOrchestrationRequest(SourceScopedRequest):
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Field(default="network")
    platforms: list[Literal["instagram", "tiktok", "twitter", "youtube", "facebook", "threads"]] | None = Field(
        default=None
    )
    sync_strategy: Literal["incremental", "full_refresh"] = Field(default="incremental")
    comment_refresh_policy: Literal["balanced", "missing_only"] = Field(default="balanced")
    comment_anchor_source_ids: (
        dict[Literal["instagram", "tiktok", "twitter", "youtube", "facebook", "threads"], list[str]] | None
    ) = Field(default=None)
    accounts_override: list[str] | None = Field(default=None)
    hashtags_override: list[str] | None = Field(default=None)
    keywords_override: list[str] | None = Field(default=None)
    sound_ids: list[str] | None = Field(default=None)
    max_posts_per_target: int = Field(default=0, ge=0)
    max_comments_per_post: int = Field(default=0, ge=0)
    max_replies_per_post: int = Field(default=0, ge=0)
    fetch_replies: bool = Field(default=True)
    ingest_mode: Literal["posts_only", "posts_and_comments", "comments_only", "details_refresh"] = Field(
        default="posts_and_comments"
    )
    week_index: int | None = Field(default=None, ge=0, le=200)
    timezone: str = Field(default="America/New_York", min_length=1, max_length=64)
    runner_strategy: Literal["single_runner", "adaptive_dual_runner"] | None = Field(default=None)
    runner_count: int | None = Field(default=None, ge=1, le=2)
    window_shard_hours: int | None = Field(default=None, ge=1, le=24)
    runner_b_start_offset_hours: int | None = Field(default=None, ge=0, le=168)
    day_weight_profile: Literal["default", "rhoslc_default"] | None = Field(default=None)
    priority_mode: Literal["default", "episode_peak_weighted"] | None = Field(default=None)
    youtube_source_mode: Literal["hybrid", "api_only", "scraper_only"] | None = Field(default=None)
    youtube_force_reindex: bool = Field(default=False)
    youtube_force_media_refresh: bool = Field(default=False)
    youtube_force_comment_refresh: bool = Field(default=False)
    resume_existing: bool = Field(default=True)
    client_session_id: str | None = Field(default=None, max_length=200)
    client_workflow_id: str | None = Field(default=None, max_length=200)


class SharedAccountSourceInput(BaseModel):
    platform: Literal["instagram", "tiktok", "twitter", "youtube", "facebook", "threads"]
    account_handle: str = Field(..., min_length=1, max_length=128)
    is_active: bool = Field(default=True)
    scrape_priority: int = Field(default=100, ge=1, le=10_000)
    metadata: dict[str, Any] = Field(default_factory=dict)


class SharedAccountSourcesPutRequest(SourceScopedRequest):
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Field(default="network")
    sources: list[SharedAccountSourceInput]


class SyncSessionRetryRequest(BaseModel):
    retry_kind: Literal[
        "retry_missing_comments",
        "retry_failed_media",
        "retry_missing_avatars",
        "retry_missing_comment_media",
    ]


class SharedSocialIngestRequest(SourceScopedRequest):
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Field(default="network")
    platforms: list[Literal["instagram", "tiktok", "twitter", "youtube", "facebook", "threads"]] | None = Field(
        default=None
    )
    accounts_override: list[str] | None = Field(default=None)
    date_start: datetime | None = None
    date_end: datetime | None = None
    allow_inline_dev_fallback: bool = Field(default=False)


class SharedReviewResolveRequest(BaseModel):
    resolution_action: Literal["resolve", "ignore"] = Field(default="resolve")
    resolved_show_id: UUID | None = None
    resolved_season_id: UUID | None = None


def _social_sync_sse_chunk(event_type: str, payload: dict[str, Any]) -> bytes:
    return f"event: {event_type}\ndata: {json.dumps(jsonable_encoder(payload))}\n\n".encode()


def _build_sync_session_stream_payload_sync(sync_session_id: str) -> dict[str, Any]:
    from trr_backend.repositories.social_sync_orchestrator import evaluate_sync_session
    from trr_backend.socials.control_plane.dispatch_runtime import legacy as social_repo

    sync_session = evaluate_sync_session(sync_session_id)
    season_id = str(sync_session.get("season_id") or "").strip()
    current_run_id = str(sync_session.get("current_run_id") or "").strip()
    run_progress = (
        social_repo.get_run_progress_snapshot(season_id, current_run_id, recent_log_limit=20)
        if season_id and current_run_id
        else None
    )
    return {
        "sync_session": sync_session,
        "run_progress": run_progress,
        "emitted_at": datetime.now(UTC).isoformat(),
    }


async def _build_sync_session_stream_payload(sync_session_id: str) -> dict[str, Any]:
    return await _run_admin_repo_call(_build_sync_session_stream_payload_sync, sync_session_id)


@router.get("/seasons/{season_id}/ingest/jobs")
async def get_season_ingest_jobs(
    season_id: UUID,
    limit: int = Query(default=_INGEST_JOBS_DEFAULT_LIMIT, ge=1, le=_INGEST_JOBS_MAX_LIMIT),
    offset: int = Query(default=_INGEST_JOBS_DEFAULT_OFFSET, ge=0, le=_INGEST_JOBS_MAX_OFFSET),
    run_id: UUID | None = Query(default=None),
    status: str | None = Query(default=None),
    platform: Literal["instagram", "tiktok", "twitter", "youtube", "facebook", "threads"] | None = Query(default=None),
    _: InternalAdminUser = cast(Any, None),
) -> dict:
    from trr_backend.socials.control_plane import list_jobs

    try:
        jobs = await _run_admin_repo_call(
            list_jobs,
            str(season_id),
            limit=limit,
            offset=offset,
            run_id=(str(run_id) if run_id else None),
            status=status,
            platform=platform,
        )
        returned = len(jobs)
        return {
            "season_id": str(season_id),
            "run_id": str(run_id) if run_id else None,
            "filters": {"status": status, "platform": platform},
            "jobs": jobs,
            "pagination": {
                "limit": int(limit),
                "offset": int(offset),
                "returned": returned,
                "has_more": returned >= int(limit),
            },
        }
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to list social jobs: season=%s", season_id)
        raise _to_social_read_http_exception(exc) from exc


@router.get("/seasons/{season_id}/ingest/runs")
async def get_season_ingest_runs(
    season_id: UUID,
    limit: int = Query(default=50, ge=1, le=250),
    status: Literal["queued", "pending", "running", "retrying", "completed", "failed", "cancelled"] | None = Query(
        default=None
    ),
    source_scope: Literal["bravo", "network", "creator", "community", "news"] | None = Query(default=None),
    run_id: UUID | None = Query(default=None),
    client_session_id: str | None = Query(default=None, max_length=200),
    client_workflow_id: str | None = Query(default=None, max_length=200),
    platforms: list[str] | None = Query(default=None),
    week_index: int | None = Query(default=None, ge=0),
    date_start: datetime | None = Query(default=None),
    date_end: datetime | None = Query(default=None),
    _: InternalAdminUser = cast(Any, None),
) -> dict:
    from trr_backend.socials.control_plane import list_runs

    started_at = perf_counter()
    try:
        runs = await _run_admin_repo_call(
            list_runs,
            str(season_id),
            limit=limit,
            status=status,
            source_scope=source_scope,
            run_id=str(run_id) if run_id else None,
            client_session_id=(str(client_session_id or "").strip() or None),
            client_workflow_id=(str(client_workflow_id or "").strip() or None),
            platforms=platforms,
            week_index=week_index,
            date_start=date_start,
            date_end=date_end,
        )
        duration_ms = int((perf_counter() - started_at) * 1000)
        logger.info(
            "Social ingest runs request completed: season=%s source_scope=%s status=%s limit=%s duration_ms=%s",
            season_id,
            source_scope,
            status,
            limit,
            duration_ms,
        )
        return {
            "season_id": str(season_id),
            "filters": {
                "status": status,
                "source_scope": source_scope,
                "run_id": str(run_id) if run_id else None,
                "client_session_id": str(client_session_id or "").strip() or None,
                "client_workflow_id": str(client_workflow_id or "").strip() or None,
                "platforms": platforms,
                "week_index": week_index,
                "date_start": date_start.isoformat() if date_start else None,
                "date_end": date_end.isoformat() if date_end else None,
            },
            "runs": runs,
        }
    except Exception as exc:  # noqa: BLE001
        duration_ms = int((perf_counter() - started_at) * 1000)
        logger.exception(
            "Failed to list social runs: season=%s source_scope=%s status=%s limit=%s duration_ms=%s",
            season_id,
            source_scope,
            status,
            limit,
            duration_ms,
        )
        raise _to_social_read_http_exception(exc) from exc


@router.get("/seasons/{season_id}/ingest/runs/summary")
async def get_season_ingest_runs_summary(
    season_id: UUID,
    limit: int = Query(default=20, ge=1, le=100),
    source_scope: Literal["bravo", "network", "creator", "community", "news"] | None = Query(default=None),
    client_session_id: str | None = Query(default=None, max_length=200),
    client_workflow_id: str | None = Query(default=None, max_length=200),
    platforms: list[str] | None = Query(default=None),
    week_index: int | None = Query(default=None, ge=0),
    date_start: datetime | None = Query(default=None),
    date_end: datetime | None = Query(default=None),
    _: InternalAdminUser = cast(Any, None),
) -> dict:
    from trr_backend.socials.control_plane import list_run_summaries

    started_at = perf_counter()
    try:
        summaries = await _run_admin_repo_call(
            list_run_summaries,
            str(season_id),
            limit=limit,
            source_scope=source_scope,
            client_session_id=(str(client_session_id or "").strip() or None),
            client_workflow_id=(str(client_workflow_id or "").strip() or None),
            platforms=platforms,
            week_index=week_index,
            date_start=date_start,
            date_end=date_end,
        )
        duration_ms = int((perf_counter() - started_at) * 1000)
        logger.info(
            "Social ingest run summary request completed: season=%s source_scope=%s limit=%s duration_ms=%s",
            season_id,
            source_scope,
            limit,
            duration_ms,
        )
        return {
            "season_id": str(season_id),
            "filters": {
                "source_scope": source_scope,
                "limit": limit,
                "client_session_id": str(client_session_id or "").strip() or None,
                "client_workflow_id": str(client_workflow_id or "").strip() or None,
                "platforms": platforms,
                "week_index": week_index,
                "date_start": date_start.isoformat() if date_start else None,
                "date_end": date_end.isoformat() if date_end else None,
            },
            "summaries": summaries,
        }
    except Exception as exc:  # noqa: BLE001
        duration_ms = int((perf_counter() - started_at) * 1000)
        logger.exception(
            "Failed to list social run summaries: season=%s source_scope=%s limit=%s duration_ms=%s",
            season_id,
            source_scope,
            limit,
            duration_ms,
        )
        raise _to_social_read_http_exception(exc) from exc


@router.get("/seasons/{season_id}/ingest/runs/{run_id}/progress")
async def get_season_ingest_run_progress(
    season_id: UUID,
    run_id: UUID,
    recent_log_limit: int = Query(default=20, ge=1, le=100),
    _: InternalAdminUser = cast(Any, None),
) -> dict[str, Any]:
    from trr_backend.socials.control_plane import run_reads as run_read_control_plane

    started_at = perf_counter()
    try:
        payload = await _run_admin_repo_call(
            run_read_control_plane.get_run_progress_snapshot,
            str(season_id),
            str(run_id),
            recent_log_limit=recent_log_limit,
        )
        duration_ms = int((perf_counter() - started_at) * 1000)
        logger.info(
            "Social ingest run progress request completed: season=%s run_id=%s recent_log_limit=%s duration_ms=%s",
            season_id,
            run_id,
            recent_log_limit,
            duration_ms,
        )
        return payload
    except ValueError as exc:
        message = str(exc)
        if message == "run_not_found":
            raise HTTPException(status_code=404, detail=message) from exc
        if message in {"social_ingest_queue_schema_missing", "run_progress_requires_scrape_jobs_run_id"}:
            raise HTTPException(status_code=503, detail=message) from exc
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        duration_ms = int((perf_counter() - started_at) * 1000)
        logger.exception(
            "Failed to fetch social ingest run progress: season=%s run_id=%s duration_ms=%s",
            season_id,
            run_id,
            duration_ms,
        )
        raise _internal_error_response(exc) from exc


@router.post("/seasons/{season_id}/ingest/runs/{run_id}/cancel")
async def cancel_season_ingest_run(
    season_id: UUID,
    run_id: UUID,
    user: InternalAdminUser = cast(Any, None),
) -> dict:
    from trr_backend.socials.control_plane import cancel_run

    try:
        payload = await _run_admin_repo_call(
            cancel_run,
            str(season_id),
            str(run_id),
            cancelled_by=(user or {}).get("email"),
        )
        return payload
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to cancel social ingest run: season=%s run_id=%s", season_id, run_id)
        raise _internal_error_response(exc) from exc


__all__ = [name for name in globals() if not name.startswith("__")]
