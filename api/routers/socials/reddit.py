# ruff: noqa: F403, F405, UP037
"""Reddit refresh and Reddit analytics route surface."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from fastapi import APIRouter

from ._shared import *
from ._surfaces import RouteRecord, routes_matching

if TYPE_CHECKING:
    # These underscore-prefixed helpers are re-exported at runtime by
    # ``from ._shared import *`` via _shared's dynamic ``__all__``; the
    # declarations below only make them visible to static type checkers.
    from ._shared import (
        _internal_error_response,
        _run_admin_repo_call,
        _value_error_to_bad_request,
    )

router = APIRouter()


def _reddit_refresh_worker_health_payload(
    *,
    healthy: bool,
    reason: str,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "healthy": healthy,
        "healthy_workers": 1 if healthy else 0,
        "active_workers": 1 if healthy else 0,
        "total_workers": 1 if healthy else 0,
        "reason": reason,
        "execution_backend_canonical": execution_backend_canonical(),
    }
    if isinstance(extra, dict):
        payload.update(extra)
    return payload


class RedditRefreshRunRequest(BaseModel):
    community_id: UUID
    season_id: UUID
    period_key: str = Field(..., min_length=1, max_length=160)
    period_stable_key: str | None = Field(default=None, min_length=1, max_length=160)
    subreddit: str = Field(..., min_length=1, max_length=120)
    show_name: str = Field(..., min_length=1, max_length=200)
    show_aliases: list[str] = Field(default_factory=list)
    cast_names: list[str] = Field(default_factory=list)
    is_show_focused: bool = Field(default=False)
    analysis_flairs: list[str] = Field(default_factory=list)
    analysis_all_flairs: list[str] = Field(default_factory=list)
    force_include_flairs: list[str] = Field(default_factory=list)
    sort_modes: list[Literal["new", "hot", "top"]] | None = Field(default=None)
    limit_per_mode: int = Field(default=35, ge=1, le=100)
    period_start: datetime | None = None
    period_end: datetime | None = None
    exhaustive_window: bool = Field(default=True)
    search_backfill: bool = Field(default=True)
    seed_post_urls: list[str] = Field(default_factory=list)
    coverage_mode: Literal["standard", "adaptive_deep", "max_coverage"] = Field(default="standard")
    max_backfill_queries: int | None = Field(default=None, ge=1, le=30)
    max_backfill_pages_per_query: int | None = Field(default=None, ge=1, le=50)
    period_label: str | None = Field(default=None, min_length=1, max_length=120)
    run_config_hash: str | None = Field(default=None, min_length=8, max_length=64)
    fetch_comments: bool = Field(default=False)
    comment_delta_only: bool = Field(default=True)
    max_pages: int = Field(default=10_000, ge=1, le=10_000)
    mode: Literal["sync_posts", "sync_details", "sync_full"] = Field(default="sync_posts")
    client_session_id: str | None = Field(default=None, max_length=200)
    client_workflow_id: str | None = Field(default=None, max_length=200)


class RedditRefreshBackfillRequest(BaseModel):
    community_id: UUID
    season_id: UUID
    container_keys: list[str] = Field(default_factory=list, max_length=100)
    mode: Literal["sync_posts", "sync_details", "sync_full"] = Field(default="sync_full")
    detail_refresh: bool = Field(default=False)


class RedditCacheBulkRequest(BaseModel):
    community_id: UUID
    season_id: UUID
    period_keys: list[str] = Field(default_factory=list, max_length=25)
    container_keys: list[str] = Field(default_factory=list, max_length=25)


def _start_reddit_refresh_run_from_serialized_payload(
    *,
    serialized_payload: dict[str, Any],
    background_tasks: BackgroundTasks,
) -> dict[str, Any]:
    from trr_backend.repositories.reddit_refresh import (
        create_or_reuse_refresh_run,
        execute_refresh_run,
        get_refresh_run,
    )

    remote_mode = is_remote_job_plane_enabled()
    runtime_execution = execution_metadata()
    execution_mode = canonical_execution_mode()
    execution_owner = execution_owner_label()
    execution_backend = str(runtime_execution.get("execution_backend_canonical") or "local")
    run_row = create_or_reuse_refresh_run(payload=serialized_payload)
    run_id = str(run_row.get("id"))
    reused = bool(run_row.get("reused"))
    run = get_refresh_run(run_id)
    should_dispatch_modal = execution_backend == "modal" and (
        not reused
        or (
            isinstance(run, dict)
            and str(run.get("status") or "").strip().lower() == "queued"
            and not str(run.get("claimed_by_worker_id") or "").strip()
            and not str(run.get("heartbeat_at") or "").strip()
        )
    )
    modal_dispatched = False
    if should_dispatch_modal:
        modal_ready, modal_reason = modal_dispatch_ready(function_name=modal_reddit_refresh_function_name())
        if not modal_ready:
            raise HTTPException(
                status_code=503,
                detail={
                    "code": "REDDIT_REMOTE_DISPATCH_UNAVAILABLE",
                    "message": ("Reddit refresh remote-worker ownership is enforced and Modal dispatch is not ready."),
                    "execution_mode": execution_mode,
                    "execution_owner": execution_owner,
                    "worker_health": _reddit_refresh_worker_health_payload(
                        healthy=False,
                        reason=modal_reason or "modal_dispatch_unavailable",
                    ),
                },
            )
        reddit_runtime_health = get_modal_reddit_runtime_health()
        if not bool(reddit_runtime_health.get("healthy")):
            missing_env = reddit_runtime_health.get("missing_env")
            missing_env_list = (
                [str(item).strip() for item in missing_env if str(item).strip()]
                if isinstance(missing_env, list)
                else []
            )
            missing_env_text = f" Missing: {', '.join(missing_env_list)}." if missing_env_list else ""
            raise HTTPException(
                status_code=503,
                detail={
                    "code": "REDDIT_REMOTE_RUNTIME_UNHEALTHY",
                    "message": (
                        f"Reddit refresh remote worker is missing Reddit OAuth configuration.{missing_env_text}"
                    ),
                    "execution_mode": execution_mode,
                    "execution_owner": execution_owner,
                    "worker_health": _reddit_refresh_worker_health_payload(
                        healthy=False,
                        reason=str(reddit_runtime_health.get("reason") or "reddit_runtime_unhealthy"),
                        extra=reddit_runtime_health,
                    ),
                },
            )
        modal_dispatched = dispatch_reddit_refresh(run_id=run_id)
        if not modal_dispatched:
            raise HTTPException(
                status_code=503,
                detail={
                    "code": "REDDIT_REMOTE_DISPATCH_UNAVAILABLE",
                    "message": "Reddit refresh remote-worker dispatch could not be started.",
                    "execution_mode": execution_mode,
                    "execution_owner": execution_owner,
                    "worker_health": _reddit_refresh_worker_health_payload(
                        healthy=False,
                        reason="modal_dispatch_failed",
                    ),
                },
            )
        modal_metadata = modal_execution_metadata()
        execution_mode = modal_metadata["execution_mode_canonical"]
        execution_owner = modal_metadata["execution_owner"]
        execution_backend = modal_metadata["execution_backend_canonical"]
        logger.info(
            "Queued reddit refresh run for Modal ownership: run_id=%s reused=%s",
            run_id,
            reused,
        )
        run = get_refresh_run(run_id)
    elif not reused:
        if remote_mode:
            logger.info(
                "Queued reddit refresh run for remote worker ownership: run_id=%s execution_mode=%s",
                run_id,
                execution_mode,
            )
        else:
            background_tasks.add_task(execute_refresh_run, run_id, worker_id="api-background:reddit-refresh")
            logger.info("Queued reddit refresh run for API background execution: run_id=%s", run_id)
    if modal_dispatched and isinstance(run, dict):
        run = {**run, **modal_execution_metadata()}
    return {
        "run": run,
        "reused": reused,
        "execution_owner": execution_owner,
        "execution_mode_canonical": execution_mode,
        "execution_backend_canonical": execution_backend,
    }


class AutoCategorizeFlairsRequest(BaseModel):
    show_id: UUID


def serialize_reddit_refresh_payload(payload: Any) -> dict[str, Any]:
    data = payload.model_dump()
    period_stable_key = getattr(payload, "period_stable_key", None)
    period_label = getattr(payload, "period_label", None)
    run_config_hash = getattr(payload, "run_config_hash", None)
    period_start = getattr(payload, "period_start", None)
    period_end = getattr(payload, "period_end", None)

    if isinstance(period_stable_key, str):
        normalized_stable_key = period_stable_key.strip()
        data["period_stable_key"] = normalized_stable_key or None
    if isinstance(period_label, str):
        normalized_period_label = period_label.strip()
        data["period_label"] = normalized_period_label or None
    if isinstance(run_config_hash, str):
        normalized_hash = run_config_hash.strip().lower()
        data["run_config_hash"] = normalized_hash or None
    if isinstance(period_start, datetime):
        data["period_start"] = period_start.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(period_end, datetime):
        data["period_end"] = period_end.astimezone(UTC).isoformat().replace("+00:00", "Z")
    data["community_id"] = str(payload.community_id)
    data["season_id"] = str(payload.season_id)
    return data


def normalize_reddit_backfill_container_keys(values: list[str]) -> list[str]:
    return [item.strip() for item in values if isinstance(item, str) and item.strip()]


def serialize_reddit_backfill_payload(payload: Any) -> dict[str, Any]:
    return {
        "community_id": str(payload.community_id),
        "season_id": str(payload.season_id),
        "container_keys": normalize_reddit_backfill_container_keys(payload.container_keys),
        "mode": payload.mode,
        "detail_refresh": bool(payload.detail_refresh),
    }


@router.post("/reddit/runs")
async def start_reddit_refresh_run(
    payload: RedditRefreshRunRequest,
    background_tasks: BackgroundTasks,
    _: InternalAdminUser = cast(Any, None),
) -> dict[str, Any]:
    try:
        serialized = _serialize_reddit_refresh_payload(payload)
        return _start_reddit_refresh_run_from_serialized_payload(
            serialized_payload=serialized,
            background_tasks=background_tasks,
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to start reddit refresh run: community_id=%s season_id=%s period_key=%s",
            payload.community_id,
            payload.season_id,
            payload.period_key,
        )
        raise _internal_error_response(exc) from exc


@router.post("/reddit/runs/backfill")
async def backfill_reddit_refresh_runs(
    payload: RedditRefreshBackfillRequest,
    request: Request,
    _: InternalAdminUser = cast(Any, None),
) -> dict[str, Any]:
    from trr_backend.pipeline.admin_operations import start_operation_for_stream
    from trr_backend.repositories.reddit_refresh import (
        REDDIT_BACKFILL_OPERATION_TYPE,
        build_reddit_refresh_backfill_operation_producer,
    )

    serialized_backfill = _serialize_reddit_backfill_payload(payload)
    try:
        producer = build_reddit_refresh_backfill_operation_producer(
            request_payload=serialized_backfill,
        )
        operation = start_operation_for_stream(
            operation_type=REDDIT_BACKFILL_OPERATION_TYPE,
            producer=producer,
            request_payload=serialized_backfill,
            initiated_by=None,
            request=request,
        )

        return {
            "status": "attached" if bool(operation.get("attached")) else "started",
            "requested": serialized_backfill,
            "operation": operation,
            "operation_id": str(operation.get("id") or "").strip() or None,
            "attached": bool(operation.get("attached")),
        }
    except HTTPException:
        raise
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to start reddit refresh backfill: community_id=%s season_id=%s",
            payload.community_id,
            payload.season_id,
        )
        raise _internal_error_response(exc) from exc


@router.get("/reddit/runs")
async def list_reddit_refresh_runs(
    community_id: UUID | None = Query(default=None),
    season_id: UUID | None = Query(default=None),
    period_key: str | None = Query(default=None, min_length=1, max_length=200),
    status: str | None = Query(default=None),
    limit: int = Query(default=25, ge=1, le=100),
    _: InternalAdminUser = cast(Any, None),
) -> dict[str, Any]:
    from trr_backend.repositories.reddit_refresh import list_refresh_runs

    statuses = [item.strip().lower() for item in (status or "").split(",") if item.strip()]
    try:
        runs = await _run_admin_repo_call(
            list_refresh_runs,
            community_id=str(community_id) if community_id else None,
            season_id=str(season_id) if season_id else None,
            period_key=period_key.strip() if isinstance(period_key, str) else None,
            statuses=statuses or None,
            limit=limit,
        )
        return {"runs": runs}
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to list reddit refresh runs: community_id=%s season_id=%s period_key=%s status=%s",
            community_id,
            season_id,
            period_key,
            status,
        )
        raise _internal_error_response(exc) from exc


@router.get("/reddit/runs/{run_id}")
async def get_reddit_refresh_run(run_id: UUID, _: InternalAdminUser = cast(Any, None)) -> dict[str, Any]:
    from trr_backend.repositories.reddit_refresh import get_refresh_run

    try:
        return await _run_admin_repo_call(get_refresh_run, str(run_id))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch reddit refresh run: run_id=%s", run_id)
        raise _internal_error_response(exc) from exc


@router.get("/reddit/cache")
async def get_reddit_cached_period_payload(
    community_id: UUID = Query(...),
    season_id: UUID = Query(...),
    period_key: str = Query(..., min_length=1, max_length=160),
    _: InternalAdminUser = cast(Any, None),
) -> dict[str, Any]:
    from trr_backend.repositories.reddit_refresh import (
        get_cached_period_payload,
        get_cached_period_payload_snapshot,
    )

    try:
        payload = await _run_admin_repo_call(
            get_cached_period_payload_snapshot,
            community_id=str(community_id),
            season_id=str(season_id),
            period_key=period_key.strip(),
        )
        if payload is None:
            legacy_payload = await _run_admin_repo_call(
                get_cached_period_payload,
                community_id=str(community_id),
                season_id=str(season_id),
                period_key=period_key.strip(),
            )
            if legacy_payload is not None:
                payload = {
                    "discovery": legacy_payload,
                    "resolved_period_key": period_key.strip(),
                    "cache_status": "fresh",
                    "cache_age_seconds": None,
                    "run_status": "completed",
                    "phase": None,
                    "partial_failures": [],
                }
        if payload is None:
            raise HTTPException(status_code=404, detail="Cached reddit payload not found")
        return payload
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to fetch cached reddit payload: community_id=%s season_id=%s period_key=%s",
            community_id,
            season_id,
            period_key,
        )
        raise _internal_error_response(exc) from exc


@router.post("/reddit/cache/bulk")
async def get_reddit_cached_period_payload_bulk(
    payload: RedditCacheBulkRequest,
    _: InternalAdminUser = cast(Any, None),
) -> dict[str, Any]:
    from trr_backend.repositories.reddit_refresh import (
        get_cached_period_payload,
        get_cached_period_payload_snapshot,
        resolve_cached_period_key,
    )

    try:

        def _normalize_period_key(value: str) -> str:
            return str(value or "").strip()

        derived_period_keys: list[str] = []
        seen_derived: set[str] = set()
        for raw_container_key in payload.container_keys:
            container_key = str(raw_container_key or "").strip().lower()
            if not container_key:
                continue
            if container_key in seen_derived:
                continue
            seen_derived.add(container_key)
            derived_period_keys.append(
                f"community:{payload.community_id}:season:{payload.season_id}:container:{container_key}"
            )

        unique_period_keys: list[str] = []
        seen: set[str] = set()
        for raw_key in [*derived_period_keys, *payload.period_keys]:
            normalized = _normalize_period_key(raw_key)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            unique_period_keys.append(normalized)

        if not unique_period_keys:
            raise HTTPException(
                status_code=400,
                detail="At least one non-empty period_key or container_key is required",
            )

        misses: list[str] = []
        matched_period_key: str | None = None
        resolved_period_key: str | None = None
        discovery: dict[str, Any] | None = None
        partial_failures: list[dict[str, Any]] = []
        community_id = str(payload.community_id)
        season_id = str(payload.season_id)

        for period_key in unique_period_keys:
            resolved_candidate_key = await _run_admin_repo_call(
                resolve_cached_period_key,
                community_id=community_id,
                season_id=season_id,
                period_key=period_key,
            )
            if not resolved_candidate_key:
                misses.append(period_key)
                partial_failures.append({"period_key": period_key, "reason": "cache_miss"})
                continue
            matched_period_key = period_key
            resolved_period_key = resolved_candidate_key
            break

        if resolved_period_key:
            snapshot = await _run_admin_repo_call(
                get_cached_period_payload_snapshot,
                community_id=community_id,
                season_id=season_id,
                period_key=resolved_period_key,
            )
            if snapshot is None:
                legacy_payload = await _run_admin_repo_call(
                    get_cached_period_payload,
                    community_id=community_id,
                    season_id=season_id,
                    period_key=resolved_period_key,
                )
                if legacy_payload is not None:
                    snapshot = {
                        "discovery": legacy_payload,
                        "resolved_period_key": resolved_period_key,
                        "cache_status": "fresh",
                        "cache_age_seconds": None,
                        "run_status": "completed",
                        "phase": None,
                        "partial_failures": [],
                    }
            if snapshot is None:
                # Cache row disappeared between key resolution and payload fetch.
                misses.append(matched_period_key or resolved_period_key)
                partial_failures.append(
                    {
                        "period_key": matched_period_key or resolved_period_key,
                        "reason": "cache_row_disappeared",
                    }
                )
                matched_period_key = None
            else:
                snapshot_discovery = snapshot.get("discovery")
                discovery = snapshot_discovery if isinstance(snapshot_discovery, dict) else None
                snapshot_partial_failures = snapshot.get("partial_failures")
                partial_failures.extend(
                    snapshot_partial_failures if isinstance(snapshot_partial_failures, list) else []
                )

        return {
            "discovery": discovery,
            "matched_period_key": matched_period_key,
            "misses": misses,
            "source": "cache" if discovery is not None else "none",
            "partial_failures": partial_failures,
        }
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed bulk cached reddit payload fetch: community_id=%s season_id=%s keys=%s",
            payload.community_id,
            payload.season_id,
            len(payload.period_keys),
        )
        raise _internal_error_response(exc) from exc


@router.get("/reddit/analytics/community/{community_id}/summary")
async def get_reddit_community_analytics_summary(
    community_id: UUID,
    scope: Literal["season", "all"] = Query(default="season"),
    season_id: UUID | None = Query(default=None),
    _: InternalAdminUser = cast(Any, None),
) -> dict[str, Any]:
    from trr_backend.repositories.reddit_refresh import get_reddit_community_analytics_summary

    try:
        if scope == "season" and season_id is None:
            raise HTTPException(status_code=400, detail="season_id is required when scope=season")
        return await _run_admin_repo_call(
            get_reddit_community_analytics_summary,
            community_id=str(community_id),
            scope=scope,
            season_id=str(season_id) if season_id else None,
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to fetch reddit analytics summary: community_id=%s scope=%s season_id=%s",
            community_id,
            scope,
            season_id,
        )
        raise _internal_error_response(exc) from exc


@router.get("/reddit/analytics/community/{community_id}/shows")
async def get_reddit_community_analytics_shows(
    community_id: UUID,
    scope: Literal["season", "all"] = Query(default="season"),
    season_id: UUID | None = Query(default=None),
    _: InternalAdminUser = cast(Any, None),
) -> dict[str, Any]:
    from trr_backend.repositories.reddit_refresh import get_reddit_community_show_breakdown

    try:
        if scope == "season" and season_id is None:
            raise HTTPException(status_code=400, detail="season_id is required when scope=season")
        return await _run_admin_repo_call(
            get_reddit_community_show_breakdown,
            community_id=str(community_id),
            scope=scope,
            season_id=str(season_id) if season_id else None,
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to fetch reddit analytics show breakdown: community_id=%s scope=%s season_id=%s",
            community_id,
            scope,
            season_id,
        )
        raise _internal_error_response(exc) from exc


@router.get("/reddit/analytics/community/{community_id}/flairs")
async def get_reddit_community_analytics_flairs(
    community_id: UUID,
    scope: Literal["season", "all"] = Query(default="season"),
    season_id: UUID | None = Query(default=None),
    _: InternalAdminUser = cast(Any, None),
) -> dict[str, Any]:
    from trr_backend.repositories.reddit_refresh import get_reddit_community_flair_breakdown

    try:
        if scope == "season" and season_id is None:
            raise HTTPException(status_code=400, detail="season_id is required when scope=season")
        return await _run_admin_repo_call(
            get_reddit_community_flair_breakdown,
            community_id=str(community_id),
            scope=scope,
            season_id=str(season_id) if season_id else None,
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to fetch reddit analytics flair breakdown: community_id=%s scope=%s season_id=%s",
            community_id,
            scope,
            season_id,
        )
        raise _internal_error_response(exc) from exc


@router.get("/reddit/analytics/community/{community_id}/flairs/{flair_key}")
async def get_reddit_community_analytics_flair_detail(
    community_id: UUID,
    flair_key: str,
    scope: Literal["season", "all"] = Query(default="season"),
    season_id: UUID | None = Query(default=None),
    container_key: str | None = Query(default=None, max_length=160),
    page: int = Query(default=1, ge=1, le=10_000),
    per_page: int = Query(default=50, ge=1, le=200),
    _: InternalAdminUser = cast(Any, None),
) -> dict[str, Any]:
    from trr_backend.repositories.reddit_refresh import get_reddit_community_flair_detail

    try:
        if scope == "season" and season_id is None:
            raise HTTPException(status_code=400, detail="season_id is required when scope=season")
        return await _run_admin_repo_call(
            get_reddit_community_flair_detail,
            community_id=str(community_id),
            flair_key=flair_key,
            scope=scope,
            season_id=str(season_id) if season_id else None,
            container_key=container_key,
            page=page,
            per_page=per_page,
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to fetch reddit analytics flair detail: community_id=%s flair_key=%s scope=%s season_id=%s",
            community_id,
            flair_key,
            scope,
            season_id,
        )
        raise _internal_error_response(exc) from exc


@router.get("/reddit/analytics/community/{community_id}/posts")
async def get_reddit_community_analytics_posts(
    community_id: UUID,
    scope: Literal["season", "all"] = Query(default="season"),
    season_id: UUID | None = Query(default=None),
    container_key: str | None = Query(default=None, max_length=160),
    flair_key: str | None = Query(default=None, max_length=200),
    page: int = Query(default=1, ge=1, le=10_000),
    per_page: int = Query(default=50, ge=1, le=200),
    _: InternalAdminUser = cast(Any, None),
) -> dict[str, Any]:
    from trr_backend.repositories.reddit_refresh import list_reddit_community_posts

    try:
        if scope == "season" and season_id is None:
            raise HTTPException(status_code=400, detail="season_id is required when scope=season")
        return await _run_admin_repo_call(
            list_reddit_community_posts,
            community_id=str(community_id),
            scope=scope,
            season_id=str(season_id) if season_id else None,
            container_key=container_key,
            flair_key=flair_key,
            page=page,
            per_page=per_page,
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            (
                "Failed to fetch reddit analytics posts: community_id=%s scope=%s "
                "season_id=%s container_key=%s flair_key=%s"
            ),
            community_id,
            scope,
            season_id,
            container_key,
            flair_key,
        )
        raise _internal_error_response(exc) from exc


@router.post("/reddit/communities/{community_id}/auto-categorize-flairs")
async def auto_categorize_community_flairs(
    community_id: UUID,
    body: AutoCategorizeFlairsRequest,
    _: InternalAdminUser = cast(Any, None),
) -> dict[str, Any]:
    """Auto-categorize a community's post flairs as 'cast' or 'season' using show data."""
    from trr_backend.repositories.reddit_flair_categorizer import auto_categorize_flairs

    try:
        return await _run_admin_repo_call(
            auto_categorize_flairs,
            community_id=str(community_id),
            show_id=str(body.show_id),
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to auto-categorize flairs: community_id=%s show_id=%s", community_id, body.show_id)
        raise _internal_error_response(exc) from exc


@router.post("/reddit/auto-categorize-flairs-batch")
async def auto_categorize_flairs_batch(
    body: AutoCategorizeFlairsRequest,
    _: InternalAdminUser = cast(Any, None),
) -> dict[str, Any]:
    """Auto-categorize flairs for ALL communities linked to a show."""
    from trr_backend.repositories.reddit_flair_categorizer import auto_categorize_flairs_batch

    try:
        return await _run_admin_repo_call(auto_categorize_flairs_batch, show_id=str(body.show_id))
    except HTTPException:
        raise
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to batch auto-categorize flairs: show_id=%s", body.show_id)
        raise _internal_error_response(exc) from exc


ROUTE_PREFIXES = ("/admin/socials/reddit/",)


def surface_routes(router: Any) -> list[RouteRecord]:
    return routes_matching(router, ROUTE_PREFIXES)


_serialize_reddit_refresh_payload = serialize_reddit_refresh_payload
_serialize_reddit_backfill_payload = serialize_reddit_backfill_payload
