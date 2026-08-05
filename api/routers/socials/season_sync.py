# ruff: noqa: F401, F403, F405, I001, UP037
"""Season targets, ingest, orchestration, and sync-session routes."""

from __future__ import annotations

from fastapi import APIRouter

from ._shared import *
from .season_runs import *

router = APIRouter()


@router.get("/seasons/{season_id}/targets")
async def get_season_targets(
    season_id: UUID,
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Query(default="network"),
    _: InternalAdminUser = None,
) -> dict:
    from trr_backend.socials.control_plane.dispatch_runtime import legacy as social_repo

    started_at = perf_counter()
    try:
        payload = await _run_admin_repo_call(social_repo.get_targets, str(season_id), source_scope=source_scope)
        duration_ms = int((perf_counter() - started_at) * 1000)
        logger.info(
            "Social targets request completed: season=%s source_scope=%s duration_ms=%s",
            season_id,
            source_scope,
            duration_ms,
        )
        return payload
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        duration_ms = int((perf_counter() - started_at) * 1000)
        logger.exception(
            "Failed to read season social targets: season=%s source_scope=%s duration_ms=%s",
            season_id,
            source_scope,
            duration_ms,
        )
        raise _to_social_read_http_exception(exc) from exc


@router.put("/seasons/{season_id}/targets")
async def put_season_targets(
    season_id: UUID,
    payload: SeasonSocialTargetsPutRequest,
    user: InternalAdminUser,
) -> dict:
    from trr_backend.socials.control_plane.dispatch_runtime import legacy as social_repo

    try:
        rows = [target.model_dump() for target in payload.targets]
        return await _run_admin_repo_call(
            social_repo.put_targets,
            str(season_id),
            source_scope=payload.source_scope,
            targets=rows,
            updated_by=user.get("email"),
        )
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to write season social targets: season=%s", season_id)
        raise _internal_error_response(exc) from exc


@router.post("/seasons/{season_id}/ingest")
async def ingest_season_social(
    season_id: UUID,
    payload: SeasonSocialIngestRequest,
    background_tasks: BackgroundTasks,
    user: InternalAdminUser = None,
) -> dict:
    from trr_backend.socials.control_plane.runtime import (
        SocialIngestValidationError,
        SocialWorkerUnavailableError,
    )
    from trr_backend.socials.control_plane.worker_health import (
        assert_worker_available_when_queue_enabled,
        is_queue_enabled,
    )
    from trr_backend.socials.control_plane import dispatch as dispatch_control_plane
    from trr_backend.socials.control_plane import recovery as recovery_control_plane

    sid = str(season_id)
    email = user.get("email") if user else None

    try:
        queue_enabled = is_queue_enabled()
        remote_plane_enforced = is_remote_job_plane_enabled()
        blocked_platforms = _blocked_remote_only_platforms(payload.platforms)
        requires_modal_executor = bool(blocked_platforms)
        used_inline_fallback = False
        warnings: list[str] = []
        worker_health: dict[str, Any] | None = None
        resolved_date_start, resolved_date_end, resolved_week = _resolve_ingest_window(
            season_id=sid,
            source_scope=payload.source_scope,
            week_index=payload.week_index,
            timezone=payload.timezone,
            date_start=payload.date_start,
            date_end=payload.date_end,
        )
        if resolved_week is not None and (payload.date_start is not None or payload.date_end is not None):
            warnings.append(
                "week_index supplied; using the canonical season week window "
                "instead of the provided date_start/date_end."
            )
        if not queue_enabled and remote_plane_enforced:
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
        if not queue_enabled and requires_modal_executor:
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
        if queue_enabled:
            try:
                worker_health = await run_in_threadpool(
                    assert_worker_available_when_queue_enabled,
                    required_execution_backend="modal" if requires_modal_executor else None,
                )
            except SocialWorkerUnavailableError as exc:
                worker_health = exc.worker_health
                if remote_plane_enforced:
                    worker_health_detail = jsonable_encoder(worker_health) if worker_health is not None else None
                    raise HTTPException(
                        status_code=503,
                        detail={
                            "code": "SOCIAL_REMOTE_JOB_PLANE_ENFORCED",
                            "message": _remote_worker_unavailable_message(exc),
                            "execution_mode": canonical_execution_mode(),
                            "execution_owner": execution_owner_label(),
                            "worker_health": worker_health_detail,
                        },
                    ) from exc
                if blocked_platforms:
                    worker_health_detail = jsonable_encoder(worker_health) if worker_health is not None else None
                    raise HTTPException(
                        status_code=503,
                        detail={
                            "code": "SOCIAL_REMOTE_WORKER_REQUIRED",
                            "message": "Modal worker execution is required for platform(s): "
                            + ", ".join(blocked_platforms),
                            "required_platforms": blocked_platforms,
                            "required_execution_backend": "modal",
                            "worker_health": worker_health_detail,
                        },
                    ) from exc
                if payload.allow_inline_dev_fallback and _is_local_or_dev_runtime():
                    queue_enabled = False
                    used_inline_fallback = True
                    warnings.append(
                        "No healthy social ingest worker heartbeat detected; using inline dev fallback execution."
                    )
                    logger.warning(
                        "Falling back to inline social ingest execution in dev/local runtime: season=%s",
                        sid,
                    )
                else:
                    worker_health_detail = jsonable_encoder(worker_health) if worker_health is not None else None
                    raise HTTPException(
                        status_code=503,
                        detail={
                            "code": "SOCIAL_WORKER_UNAVAILABLE",
                            "message": str(exc),
                            "worker_health": worker_health_detail,
                        },
                    ) from exc

        inline_worker_id = "api-background" if not queue_enabled else None
        run_payload = await _run_admin_repo_call(
            dispatch_control_plane.ingest_season,
            sid,
            platforms=payload.platforms,
            accounts_override=payload.accounts_override,
            hashtags_override=payload.hashtags_override,
            keywords_override=payload.keywords_override,
            source_scope=payload.source_scope,
            max_posts_per_target=payload.max_posts_per_target,
            max_comments_per_post=payload.max_comments_per_post,
            max_replies_per_post=payload.max_replies_per_post,
            fetch_replies=payload.fetch_replies,
            ingest_mode=payload.ingest_mode,
            sync_strategy=payload.sync_strategy,
            comment_refresh_policy=payload.comment_refresh_policy,
            comment_anchor_source_ids=payload.comment_anchor_source_ids,
            sound_ids=payload.sound_ids,
            runner_strategy=payload.runner_strategy,
            runner_count=payload.runner_count,
            window_shard_hours=payload.window_shard_hours,
            runner_b_start_offset_hours=payload.runner_b_start_offset_hours,
            day_weight_profile=payload.day_weight_profile,
            priority_mode=payload.priority_mode,
            youtube_source_mode=payload.youtube_source_mode,
            youtube_force_reindex=payload.youtube_force_reindex,
            youtube_force_media_refresh=payload.youtube_force_media_refresh,
            youtube_force_comment_refresh=payload.youtube_force_comment_refresh,
            week_index=payload.week_index,
            window_timezone=payload.timezone,
            run_scope_label=(resolved_week or {}).get("label"),
            client_session_id=payload.client_session_id,
            client_workflow_id=payload.client_workflow_id,
            date_start=resolved_date_start,
            date_end=resolved_date_end,
            initiated_by=email,
            inline_worker_id=inline_worker_id,
        )

        run_id = str(run_payload.get("run_id") or "")
        if run_id and not queue_enabled:

            def _run_sync() -> None:
                timeout = _inline_execution_timeout_seconds()
                logger.info(
                    "Starting inline dev-fallback execution with %ds timeout: season=%s run_id=%s",
                    timeout,
                    sid,
                    run_id,
                )
                try:
                    _execute_with_timeout(
                        _run_inline_season_ingest,
                        kwargs={
                            "run_id": run_id,
                            "platforms": payload.platforms,
                            "ingest_mode": payload.ingest_mode,
                            "worker_prefix": "api-background",
                        },
                        timeout_seconds=timeout,
                    )
                except TimeoutError:
                    logger.error(
                        "Inline execution timed out after %ds: season=%s run_id=%s",
                        timeout,
                        sid,
                        run_id,
                    )
                except Exception:  # noqa: BLE001
                    logger.exception("Background social ingest run failed: season=%s run_id=%s", sid, run_id)
                    try:
                        recovered = recovery_control_plane.recover_stale_running_jobs(run_id=run_id, limit=250)
                        if recovered:
                            logger.warning(
                                "Recovered stale inline ingest jobs after failure: season=%s run_id=%s recovered=%s",
                                sid,
                                run_id,
                                len(recovered),
                            )
                        _execute_with_timeout(
                            _run_inline_season_ingest,
                            kwargs={
                                "run_id": run_id,
                                "platforms": payload.platforms,
                                "ingest_mode": payload.ingest_mode,
                                "worker_prefix": "api-background:recovery",
                            },
                            timeout_seconds=timeout,
                        )
                    except TimeoutError:
                        logger.error(
                            "Inline recovery execution timed out after %ds: season=%s run_id=%s",
                            timeout,
                            sid,
                            run_id,
                        )
                    except Exception:  # noqa: BLE001
                        logger.exception(
                            "Background social ingest inline recovery failed: season=%s run_id=%s",
                            sid,
                            run_id,
                        )

            background_tasks.add_task(_run_sync)
        # Week-detail analytics should not serve stale in-memory snapshots after ingest kickoff.
        invalidate_week_detail_cache()
        execution_mode, execution_mode_canonical, execution_mode_legacy = _resolve_social_execution_modes(
            queue_enabled=queue_enabled,
            used_inline_fallback=used_inline_fallback,
        )

        status_value = str(run_payload.get("status") or "").strip().lower()
        if not status_value:
            status_value = "queued" if execution_mode == "queued" else "started"
        elif execution_mode != "queued" and status_value in {"queued", "pending"}:
            status_value = "started"
        job_count = int(run_payload.get("queued_or_started_jobs") or 0)

        response_payload: dict[str, Any] = {
            "status": status_value,
            "season_id": sid,
            "run_id": run_payload.get("run_id"),
            "stages": run_payload.get("stages") or [],
            "queued_or_started_jobs": job_count,
            "job_count": job_count,
            "summary": run_payload.get("summary") or {},
            "summary_normalized": _normalize_run_summary_payload(run_payload.get("summary")),
            "execution_mode": execution_mode,
            "message": (
                "Ingest run queued. Poll /ingest/jobs with run_id for stage progress."
                if execution_mode == "queued"
                else "Ingest run started inline. Poll /ingest/jobs with run_id for stage progress."
            ),
        }
        if warnings:
            response_payload["warnings"] = warnings
        if used_inline_fallback and worker_health is not None:
            response_payload["worker_health"] = worker_health
        response_payload["scope"] = _build_ingest_scope_payload(
            resolved_week=resolved_week,
            date_start=resolved_date_start,
            date_end=resolved_date_end,
            platforms=payload.platforms,
        )
        response_payload["execution_owner"] = execution_owner_label()
        response_payload["execution_mode_canonical"] = execution_mode_canonical
        response_payload["execution_mode_legacy"] = execution_mode_legacy
        response_payload["execution_backend_canonical"] = str(
            run_payload.get("execution_backend_canonical") or execution_metadata()["execution_backend_canonical"]
        )
        response_payload["execution_mode_deprecation"] = _social_execution_mode_deprecation_payload()
        return response_payload
    except HTTPException:
        raise
    except SocialIngestValidationError as exc:
        raise HTTPException(
            status_code=400,
            detail={"code": exc.code, "message": str(exc)},
        ) from exc
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to enqueue social ingest: season=%s", sid)
        raise _internal_error_response(exc) from exc


@router.post("/seasons/{season_id}/ingest/orchestrations")
async def orchestrate_season_social_ingest(
    season_id: UUID,
    payload: SeasonSocialOrchestrationRequest,
    background_tasks: BackgroundTasks,
    user: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.runtime import SocialWorkerUnavailableError
    from trr_backend.socials.control_plane.worker_health import (
        assert_worker_available_when_queue_enabled,
        is_queue_enabled,
    )
    from trr_backend.socials.control_plane import orchestrate_season_ingest

    sid = str(season_id)
    queue_enabled = is_queue_enabled()
    remote_plane_enforced = is_remote_job_plane_enabled()
    worker_health: dict[str, Any] | None = None
    if queue_enabled:
        try:
            worker_health = await run_in_threadpool(assert_worker_available_when_queue_enabled)
        except SocialWorkerUnavailableError as exc:
            if remote_plane_enforced:
                raise HTTPException(
                    status_code=503,
                    detail={
                        "code": "SOCIAL_ORCHESTRATION_REMOTE_WORKER_REQUIRED",
                        "message": "Season social orchestration requires healthy remote workers reporting heartbeats.",
                        "execution_mode": canonical_execution_mode(),
                        "execution_owner": execution_owner_label(),
                        "worker_health": _worker_health_detail(exc.worker_health),
                    },
                ) from exc
            queue_enabled = False
            worker_health = exc.worker_health
    elif remote_plane_enforced:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "SOCIAL_ORCHESTRATION_REMOTE_WORKER_REQUIRED",
                "message": "Season social orchestration requires queue mode and remote worker ownership.",
                "execution_mode": canonical_execution_mode(),
                "execution_owner": execution_owner_label(),
            },
        )

    try:
        result = await _run_admin_repo_call(
            orchestrate_season_ingest,
            sid,
            platforms=payload.platforms,
            source_scope=payload.source_scope,
            timezone=payload.timezone,
            week_index=payload.week_index,
            sync_strategy=payload.sync_strategy,
            max_posts_per_target=payload.max_posts_per_target,
            max_comments_per_post=payload.max_comments_per_post,
            max_replies_per_post=payload.max_replies_per_post,
            fetch_replies=payload.fetch_replies,
            ingest_mode=payload.ingest_mode,
            comment_refresh_policy=payload.comment_refresh_policy,
            comment_anchor_source_ids=payload.comment_anchor_source_ids,
            sound_ids=payload.sound_ids,
            runner_strategy=payload.runner_strategy,
            runner_count=payload.runner_count,
            window_shard_hours=payload.window_shard_hours,
            runner_b_start_offset_hours=payload.runner_b_start_offset_hours,
            day_weight_profile=payload.day_weight_profile,
            priority_mode=payload.priority_mode,
            youtube_source_mode=payload.youtube_source_mode,
            youtube_force_reindex=payload.youtube_force_reindex,
            youtube_force_media_refresh=payload.youtube_force_media_refresh,
            youtube_force_comment_refresh=payload.youtube_force_comment_refresh,
            accounts_override=payload.accounts_override,
            hashtags_override=payload.hashtags_override,
            keywords_override=payload.keywords_override,
            client_session_id=payload.client_session_id,
            client_workflow_id=payload.client_workflow_id,
            resume_existing=payload.resume_existing,
            initiated_by=(user or {}).get("email"),
        )
        if not queue_enabled:
            run_ids = [
                str(row.get("run_id") or "").strip()
                for row in (result.get("created_runs") or [])
                if str(row.get("run_id") or "").strip()
            ]
            _start_runs_in_background(run_ids, background_tasks, worker_prefix="api-background:orchestration")
        return {
            "status": "queued" if queue_enabled else "started",
            "message": (
                "Season social orchestration queued."
                if queue_enabled
                else "Season social orchestration started inline."
            ),
            "worker_health": worker_health,
            **result,
        }
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to orchestrate social ingest: season=%s", sid)
        raise _internal_error_response(exc) from exc


@router.post("/seasons/{season_id}/sync-sessions")
async def create_season_sync_session(
    season_id: UUID,
    payload: SeasonSocialIngestRequest,
    request: Request,
    _: BackgroundTasks,
    user: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.runtime import SocialWorkerUnavailableError
    from trr_backend.socials.control_plane.worker_health import (
        assert_worker_available_when_queue_enabled,
        is_queue_enabled,
    )
    from trr_backend.repositories.social_sync_orchestrator import create_sync_session

    sid = str(season_id)
    try:
        raw_payload = await request.json()
        raw_source_scope = raw_payload.get("source_scope") if isinstance(raw_payload, Mapping) else None
        source_scope = (
            preserve_source_scope_param(str(raw_source_scope), default=payload.source_scope)
            if raw_source_scope is not None
            else payload.source_scope
        )
        queue_enabled = is_queue_enabled()
        remote_plane_enforced = is_remote_job_plane_enabled()
        blocked_platforms = _blocked_remote_only_platforms(payload.platforms)
        requires_modal_executor = bool(blocked_platforms)
        if not queue_enabled and requires_modal_executor:
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
        if queue_enabled:
            try:
                await run_in_threadpool(
                    assert_worker_available_when_queue_enabled,
                    required_execution_backend="modal" if requires_modal_executor else None,
                )
            except SocialWorkerUnavailableError as exc:
                worker_health_detail = _worker_health_detail(exc.worker_health)
                if remote_plane_enforced:
                    raise HTTPException(
                        status_code=503,
                        detail={
                            "code": "SOCIAL_REMOTE_JOB_PLANE_ENFORCED",
                            "message": _remote_worker_unavailable_message(
                                exc,
                                default_message=(
                                    "Social sync-session kickoff requires healthy remote workers because "
                                    "remote-worker ownership is enforced."
                                ),
                            ),
                            "execution_mode": canonical_execution_mode(),
                            "execution_owner": execution_owner_label(),
                            "worker_health": worker_health_detail,
                        },
                    ) from exc
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
                            "worker_health": worker_health_detail,
                        },
                    ) from exc
                raise HTTPException(
                    status_code=503,
                    detail={
                        "code": "SOCIAL_WORKER_UNAVAILABLE",
                        "message": "No healthy social ingest worker heartbeat detected for sync-session kickoff.",
                        "execution_mode": canonical_execution_mode(),
                        "execution_owner": execution_owner_label(),
                        "worker_health": worker_health_detail,
                    },
                ) from exc
        resolved_date_start, resolved_date_end, _ = _resolve_ingest_window(
            season_id=sid,
            source_scope=source_scope,
            week_index=payload.week_index,
            timezone=payload.timezone,
            date_start=payload.date_start,
            date_end=payload.date_end,
        )
        if resolved_date_start is None or resolved_date_end is None:
            raise HTTPException(status_code=400, detail="date_start/date_end or week_index is required")
        config = payload.model_dump()
        config["source_scope"] = source_scope
        config["date_start"] = resolved_date_start
        config["date_end"] = resolved_date_end
        result = await _run_admin_repo_call(
            create_sync_session,
            sid,
            source_scope=source_scope,
            platforms=payload.platforms,
            date_start=resolved_date_start,
            date_end=resolved_date_end,
            config=config,
            initiated_by=(user or {}).get("email"),
        )
        return result
    except HTTPException:
        raise
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to create sync session: season=%s", sid)
        if is_database_service_unavailable_error(exc):
            raise HTTPException(status_code=503, detail=database_service_unavailable_detail(exc)) from exc
        raise _internal_error_response(exc) from exc


@router.get("/seasons/{season_id}/sync-sessions/{sync_session_id}")
async def get_season_sync_session(
    season_id: UUID,
    sync_session_id: UUID,
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.repositories.social_sync_orchestrator import evaluate_sync_session

    try:
        payload = await _run_admin_repo_call(evaluate_sync_session, str(sync_session_id))
        if payload.get("season_id") != str(season_id):
            raise HTTPException(status_code=404, detail="sync_session_not_found")
        return payload
    except HTTPException:
        raise
    except ValueError as exc:
        message = str(exc)
        if message == "sync_session_not_found":
            raise HTTPException(status_code=404, detail=message) from exc
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch sync session: season=%s sync_session=%s", season_id, sync_session_id)
        raise _internal_error_response(exc) from exc


@router.get("/seasons/{season_id}/sync-sessions/{sync_session_id}/stream")
async def stream_season_sync_session(
    season_id: UUID,
    sync_session_id: UUID,
    request: Request,
    _: InternalAdminUser = None,
) -> StreamingResponse:
    async def event_stream() -> Any:
        sync_session_token = str(sync_session_id)
        season_token = str(season_id)
        sequence = 0
        while True:
            if await request.is_disconnected():
                break
            try:
                payload = await _build_sync_session_stream_payload(sync_session_token)
                session_payload = payload.get("sync_session") if isinstance(payload.get("sync_session"), dict) else {}
                if str(session_payload.get("season_id") or "") != season_token:
                    yield _social_sync_sse_chunk(
                        "error",
                        {"error": "sync_session_not_found", "sync_session_id": sync_session_token},
                    )
                    break
                sequence += 1
                yield _social_sync_sse_chunk(
                    "sync_session",
                    {
                        "seq": sequence,
                        **payload,
                    },
                )
                session_status = str(session_payload.get("status") or "").strip().lower()
                if session_status in {"completed", "failed", "cancelled"}:
                    break
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "Failed to stream sync session: season=%s sync_session=%s",
                    season_id,
                    sync_session_id,
                )
                yield _social_sync_sse_chunk(
                    "error",
                    {
                        "error": str(exc),
                        "sync_session_id": sync_session_token,
                    },
                )
                break
            yield b": keep-alive\n\n"
            await asyncio.sleep(2)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-store, max-age=0",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/seasons/{season_id}/sync-sessions/{sync_session_id}/cancel")
async def cancel_season_sync_session(
    season_id: UUID,
    sync_session_id: UUID,
    user: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.repositories.social_sync_orchestrator import cancel_sync_session

    try:
        return await _run_admin_repo_call(
            cancel_sync_session,
            str(season_id),
            str(sync_session_id),
            cancelled_by=(user or {}).get("email"),
        )
    except ValueError as exc:
        message = str(exc)
        if message == "sync_session_not_found":
            raise HTTPException(status_code=404, detail=message) from exc
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to cancel sync session: season=%s sync_session=%s", season_id, sync_session_id)
        raise _internal_error_response(exc) from exc


@router.post("/seasons/{season_id}/sync-sessions/{sync_session_id}/retry")
async def retry_season_sync_session(
    season_id: UUID,
    sync_session_id: UUID,
    payload: SyncSessionRetryRequest,
    user: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.repositories.social_sync_orchestrator import retry_sync_session

    try:
        return await _run_admin_repo_call(
            retry_sync_session,
            str(season_id),
            str(sync_session_id),
            retry_kind=payload.retry_kind,
            initiated_by=(user or {}).get("email"),
        )
    except ValueError as exc:
        message = str(exc)
        if message == "sync_session_not_found":
            raise HTTPException(status_code=404, detail=message) from exc
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to retry sync session: season=%s sync_session=%s", season_id, sync_session_id)
        raise _internal_error_response(exc) from exc


@router.get("/seasons/{season_id}/ingest/schedule-preview")
async def get_season_ingest_schedule_preview(
    season_id: UUID,
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Query(default="network"),
    platforms: str | None = Query(default=None, description="Comma-separated platform list"),
    accounts_override: str | None = Query(default=None, description="Comma-separated account overrides"),
    ingest_mode: Literal["posts_only", "posts_and_comments", "comments_only", "details_refresh"] = Query(
        default="posts_and_comments",
    ),
    max_comments_per_post: int = Query(default=0, ge=0),
    week_index: int | None = Query(default=None, ge=0, le=200),
    timezone: str = Query(default="America/New_York"),
    date_start: datetime | None = Query(default=None),
    date_end: datetime | None = Query(default=None),
    runner_strategy: Literal["single_runner", "adaptive_dual_runner"] = Query(default="single_runner"),
    runner_count: int = Query(default=1, ge=1, le=2),
    window_shard_hours: int = Query(default=2, ge=1, le=24),
    runner_b_start_offset_hours: int | None = Query(default=None, ge=0, le=168),
    day_weight_profile: Literal["default", "rhoslc_default"] = Query(default="default"),
    priority_mode: Literal["default", "episode_peak_weighted"] = Query(default="default"),
    _: InternalAdminUser = None,
) -> dict:
    from trr_backend.socials.control_plane import preview_ingest_schedule

    parsed_platforms = _parse_platform_query(platforms)
    account_overrides = [item.strip() for item in (accounts_override or "").split(",") if item.strip()] or None
    try:
        resolved_date_start, resolved_date_end, resolved_week = _resolve_ingest_window(
            season_id=str(season_id),
            source_scope=source_scope,
            week_index=week_index,
            timezone=timezone,
            date_start=date_start,
            date_end=date_end,
        )
        return await _run_admin_repo_call(
            preview_ingest_schedule,
            str(season_id),
            platforms=parsed_platforms,
            source_scope=source_scope,
            accounts_override=account_overrides,
            ingest_mode=ingest_mode,
            max_comments_per_post=max_comments_per_post,
            date_start=resolved_date_start,
            date_end=resolved_date_end,
            runner_strategy=runner_strategy,
            runner_count=runner_count,
            window_shard_hours=window_shard_hours,
            runner_b_start_offset_hours=runner_b_start_offset_hours,
            day_weight_profile=day_weight_profile,
            priority_mode=priority_mode,
            week_index=week_index,
            window_timezone=timezone,
            run_scope_label=(resolved_week or {}).get("label"),
        )
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to build social ingest schedule preview: season=%s", season_id)
        raise _internal_error_response(exc) from exc


__all__ = [name for name in globals() if not name.startswith("__")]
