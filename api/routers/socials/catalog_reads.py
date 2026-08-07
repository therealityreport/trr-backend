# ruff: noqa: F401, F403, F405, I001, UP037
"""Profile-scoped catalog read routes and shared catalog launch helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from fastapi import APIRouter

from ._shared import *

if TYPE_CHECKING:
    from ._shared import (
        _account_profile_cache_key,
        _can_use_local_catalog_inline_fallback,
        _clear_account_profile_caches,
        _internal_error_response,
        _is_local_or_dev_runtime,
        _lookup_error_to_not_found,
        _raise_if_modal_social_dispatch_unresolvable,
        _remote_worker_unavailable_message,
        _resolve_account_profile_catalog_run_progress,
        _resolve_account_profile_singleflight,
        _resolve_social_execution_modes,
        _social_execution_mode_deprecation_payload,
        _start_runs_in_background,
        _to_social_read_http_exception,
        _value_error_to_bad_request,
        _worker_health_detail,
    )

router = APIRouter()

TWITTER_CATALOG_BACKFILL_LOOKBACK_DAYS = 365


def _twitter_catalog_backfill_default_window(now: datetime | None = None) -> tuple[datetime, datetime]:
    end_at = now or datetime.now(tz=UTC)
    if end_at.tzinfo is None:
        end_at = end_at.replace(tzinfo=UTC)
    return end_at - timedelta(days=TWITTER_CATALOG_BACKFILL_LOOKBACK_DAYS), end_at


def _finalize_catalog_backfill_launch_task(
    *,
    platform: str,
    account_handle: str,
    run_id: str,
    source_scope: str,
    date_start: datetime | None,
    date_end: datetime | None,
    initiated_by: str | None,
    allow_local_dev_inline_bypass: bool,
    execution_preference: str,
    selected_tasks: list[str] | None,
    details_refresh_worker_count: int | None,
    comments_worker_count: int | None,
    comments_enable_media_followups: bool | None,
    launch_group_id: str | None,
    force_catalog_rediscovery: bool = False,
    enable_cap4_canary: bool = False,
) -> None:
    from trr_backend.socials.control_plane.dispatch_runtime import legacy as social_repo

    normalized_platform = str(platform or "").strip().lower()
    normalized_account = str(account_handle or "").strip().lower()
    normalized_run_id = str(run_id or "").strip()
    normalized_launch_group_id = str(launch_group_id or "").strip() or None

    if not normalized_run_id:
        return

    try:
        logger.info(
            "[catalog-launch] finalize_start platform=%s account=%s run_id=%s launch_group_id=%s",
            normalized_platform,
            normalized_account,
            normalized_run_id,
            normalized_launch_group_id,
        )
        result = social_repo.finalize_social_account_catalog_backfill_launch(
            platform=normalized_platform,
            account_handle=normalized_account,
            run_id=normalized_run_id,
            source_scope=source_scope,
            date_start=date_start,
            date_end=date_end,
            initiated_by=initiated_by,
            allow_local_dev_inline_bypass=allow_local_dev_inline_bypass,
            execution_preference=execution_preference,
            selected_tasks=selected_tasks,
            details_refresh_worker_count=details_refresh_worker_count,
            comments_worker_count=comments_worker_count,
            comments_enable_media_followups=comments_enable_media_followups,
            launch_group_id=normalized_launch_group_id,
            force_catalog_rediscovery=force_catalog_rediscovery,
            enable_cap4_canary=enable_cap4_canary,
        )
        if allow_local_dev_inline_bypass or not social_repo.is_queue_enabled():
            catalog_run_id = str((result or {}).get("catalog_run_id") or (result or {}).get("run_id") or "").strip()
            comments_run_id = str((result or {}).get("comments_run_id") or "").strip()
            comments_worker_threads: list[Thread] = []
            if catalog_run_id:
                worker_count = _resolve_local_catalog_inline_worker_count(
                    result=result,
                    platform=normalized_platform,
                )
                worker_threads: list[Thread] = []
                worker_lanes = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L"]
                for worker_index in range(worker_count):
                    worker_lane = (
                        worker_lanes[worker_index] if worker_index < len(worker_lanes) else str(worker_index + 1)
                    )
                    worker_id = (
                        f"api-background:catalog:{normalized_platform}"
                        if worker_count == 1
                        else f"api-background:catalog:{normalized_platform}:{worker_index + 1}"
                    )
                    worker_thread = Thread(
                        target=social_repo.execute_run_with_inline_worker_registration,
                        kwargs={
                            "run_id": catalog_run_id,
                            "worker_id": worker_id,
                            "platform": normalized_platform,
                            "supported_platforms": [normalized_platform],
                            "metadata_updates": ({"worker_lane": worker_lane.lower()} if worker_count > 1 else None),
                        },
                        name=f"catalog-inline:{normalized_platform}:{worker_index + 1}",
                        daemon=True,
                    )
                    worker_thread.start()
                    worker_threads.append(worker_thread)
                for worker_thread in worker_threads:
                    worker_thread.join()
            if (
                catalog_run_id
                and not comments_run_id
                and bool((result or {}).get("comments_deferred_until_catalog_complete"))
            ):
                comments_run_id = _start_deferred_comments_inline_followup(
                    catalog_run_id=catalog_run_id,
                    normalized_platform=normalized_platform,
                    normalized_account=normalized_account,
                    source_scope=source_scope,
                    initiated_by=initiated_by,
                    allow_local_dev_inline_bypass=allow_local_dev_inline_bypass,
                    launch_group_id=normalized_launch_group_id,
                    result=result,
                    start_social_account_comments_scrape=social_repo.start_social_account_comments_scrape,
                    social_ingest_conflict_error=social_repo.SocialIngestConflictError,
                    merge_catalog_run_config=social_repo._merge_catalog_run_config,
                    metadata_dict=social_repo._metadata_dict,
                    build_attached_comments_followup=social_repo._build_attached_comments_followup,
                    comments_worker_count=comments_worker_count,
                    comments_enable_media_followups=comments_enable_media_followups,
                )
            if comments_run_id:
                if not comments_worker_threads:
                    comments_worker_threads = _start_local_comments_inline_workers(
                        comments_run_id=comments_run_id,
                        normalized_platform=normalized_platform,
                        result=result,
                        execute_run_with_inline_worker_registration=social_repo.execute_run_with_inline_worker_registration,
                        comments_stage=social_repo.INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
                        comments_worker_lane=social_repo.INSTAGRAM_COMMENTS_SCRAPLING_WORKER_LANE,
                    )
                for worker_thread in comments_worker_threads:
                    worker_thread.join()
    except Exception:
        logger.exception(
            "[catalog-launch] finalize_background_task_failed platform=%s account=%s run_id=%s launch_group_id=%s",
            normalized_platform,
            normalized_account,
            normalized_run_id,
            normalized_launch_group_id,
        )
        raise
    finally:
        _clear_account_profile_caches()


def _resolve_local_catalog_inline_worker_count(*, result: Mapping[str, Any] | None, platform: str) -> int:
    selected_tasks = {
        str(item or "").strip().lower()
        for item in [
            *list((result or {}).get("selected_tasks") or []),
            *list((result or {}).get("effective_selected_tasks") or []),
        ]
        if str(item or "").strip()
    }
    if platform != "instagram" or "post_details" not in selected_tasks:
        return 1
    raw = (os.getenv("SOCIAL_INSTAGRAM_DETAILS_REFRESH_LOCAL_WORKERS") or "").strip()
    default_workers = 1
    try:
        value = int(raw or str(default_workers))
    except ValueError:
        value = default_workers
    return _apply_local_inline_db_worker_budget(value)


def _apply_local_inline_db_worker_budget(worker_count: int) -> int:
    try:
        requested_workers = int(worker_count or 1)
    except (TypeError, ValueError):
        requested_workers = 1
    requested_workers = max(1, min(requested_workers, 12))
    try:
        pool_max = int((os.getenv("TRR_DB_POOL_MAXCONN") or "4").strip() or "4")
    except ValueError:
        pool_max = 4
    try:
        reserve_connections = int((os.getenv("SOCIAL_LOCAL_INLINE_DB_CONNECTION_RESERVE") or "2").strip() or "2")
    except ValueError:
        reserve_connections = 2
    available_worker_slots = max(1, pool_max - max(1, reserve_connections))
    return max(1, min(requested_workers, available_worker_slots))


def _resolve_local_comments_inline_worker_count(*, result: Mapping[str, Any] | None) -> int:
    target_readiness = result.get("target_readiness") if isinstance(result, Mapping) else None
    comments_preview = target_readiness.get("comments_preview") if isinstance(target_readiness, Mapping) else None
    raw_count = None
    if isinstance(comments_preview, Mapping):
        raw_count = comments_preview.get("comments_shard_count") or comments_preview.get(
            "recommended_comments_shard_count"
        )
    try:
        value = int(raw_count or 1)
    except (TypeError, ValueError):
        value = 1
    return _apply_local_inline_db_worker_budget(value)


def _start_local_comments_inline_workers(
    *,
    comments_run_id: str,
    normalized_platform: str,
    result: Mapping[str, Any] | None,
    execute_run_with_inline_worker_registration: Callable[..., Any],
    comments_stage: str,
    comments_worker_lane: str,
) -> list[Thread]:
    worker_count = _resolve_local_comments_inline_worker_count(result=result)
    worker_threads: list[Thread] = []
    for worker_index in range(worker_count):
        worker_id = (
            f"api-background:comments:{normalized_platform}"
            if worker_count == 1
            else f"api-background:comments:{normalized_platform}:{worker_index + 1}"
        )
        worker_thread = Thread(
            target=execute_run_with_inline_worker_registration,
            kwargs={
                "run_id": comments_run_id,
                "worker_id": worker_id,
                "stage": comments_stage,
                "platform": "instagram",
                "supported_platforms": ["instagram"],
                "metadata_updates": {"worker_lane": comments_worker_lane},
            },
            name=f"comments-inline:{normalized_platform}:{worker_index + 1}",
            daemon=True,
        )
        worker_thread.start()
        worker_threads.append(worker_thread)
    return worker_threads


def _start_deferred_comments_inline_followup(
    *,
    catalog_run_id: str,
    normalized_platform: str,
    normalized_account: str,
    source_scope: str,
    initiated_by: str | None,
    allow_local_dev_inline_bypass: bool,
    launch_group_id: str | None,
    result: Mapping[str, Any] | None,
    start_social_account_comments_scrape: Callable[..., Mapping[str, Any] | None],
    social_ingest_conflict_error: type[Exception],
    merge_catalog_run_config: Callable[..., Any],
    metadata_dict: Callable[[Any], dict[str, Any]],
    build_attached_comments_followup: Callable[..., dict[str, Any]],
    comments_worker_count: int | None = None,
    comments_enable_media_followups: bool | None = None,
) -> str:
    effective_tasks = {
        str(task or "").strip().lower()
        for task in list((result or {}).get("effective_selected_tasks") or [])
        if str(task or "").strip()
    }
    comments_source = "deferred_after_catalog"
    try:
        comments_result = start_social_account_comments_scrape(
            normalized_platform,
            normalized_account,
            mode="profile",
            source_scope=source_scope,
            max_posts=None,
            max_comments_per_post=None,
            refresh_policy="stale_or_missing",
            initiated_by=initiated_by or "catalog_completion_followup",
            allow_local_dev_inline_bypass=allow_local_dev_inline_bypass,
            comments_enable_media_followups=(
                bool(comments_enable_media_followups)
                if comments_enable_media_followups is not None
                else "media" in effective_tasks
            ),
            launch_group_id=launch_group_id,
            skip_launch_auth_probe=False,
            comments_worker_count=comments_worker_count,
        )
    except social_ingest_conflict_error as exc:
        if getattr(exc, "code", "") != "SOCIAL_ACCOUNT_COMMENTS_RUN_ALREADY_ACTIVE":
            raise
        comments_result = {
            "run_id": str((getattr(exc, "detail", {}) or {}).get("run_id") or "").strip() or None,
            "status": str((getattr(exc, "detail", {}) or {}).get("status") or "running").strip().lower() or "running",
        }
        comments_source = "reused_run"

    comments_run_id = str((comments_result or {}).get("run_id") or "").strip()
    if not comments_run_id:
        return ""

    attached_followups = metadata_dict((result or {}).get("attached_followups"))
    merge_catalog_run_config(
        run_id=catalog_run_id,
        metadata_updates={
            "comments_run_id": comments_run_id,
            "attached_followups": {
                **attached_followups,
                "comments": build_attached_comments_followup(
                    run_id=comments_run_id,
                    status=str((comments_result or {}).get("status") or "running").strip().lower() or "running",
                    source=comments_source,
                ),
            },
            "deferred_comments_followup": {
                "state": "started",
                "started_at": datetime.now(UTC).isoformat(),
                "comments_run_id": comments_run_id,
            },
        },
    )
    return comments_run_id


def _queue_catalog_backfill_finalize_task(
    *,
    background_tasks: BackgroundTasks,
    platform: str,
    account_handle: str,
    run_id: str,
    source_scope: str,
    date_start: datetime | None,
    date_end: datetime | None,
    initiated_by: str | None,
    allow_local_dev_inline_bypass: bool,
    execution_preference: str,
    selected_tasks: list[str] | None,
    details_refresh_worker_count: int | None,
    comments_worker_count: int | None,
    comments_enable_media_followups: bool | None,
    launch_group_id: str | None,
    force_catalog_rediscovery: bool = False,
    enable_cap4_canary: bool = False,
) -> None:
    if not str(run_id or "").strip():
        return

    thread = Thread(
        target=_finalize_catalog_backfill_launch_task,
        kwargs={
            "platform": platform,
            "account_handle": account_handle,
            "run_id": run_id,
            "source_scope": source_scope,
            "date_start": date_start,
            "date_end": date_end,
            "initiated_by": initiated_by,
            "allow_local_dev_inline_bypass": allow_local_dev_inline_bypass,
            "execution_preference": execution_preference,
            "selected_tasks": selected_tasks,
            "details_refresh_worker_count": details_refresh_worker_count,
            "comments_worker_count": comments_worker_count,
            "comments_enable_media_followups": comments_enable_media_followups,
            "launch_group_id": launch_group_id,
            "force_catalog_rediscovery": force_catalog_rediscovery,
            "enable_cap4_canary": enable_cap4_canary,
        },
        name=f"catalog-backfill-finalize:{platform}:{account_handle}:{run_id}",
        daemon=True,
    )
    thread.start()


def _resolve_social_account_catalog_route_execution(
    *,
    platform: str,
    allow_inline_dev_fallback: bool,
    execution_preference: Literal["auto", "prefer_local_inline"] = "auto",
    pipeline_ingest_mode: str = "shared_account_catalog_backfill",
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.runtime import SocialWorkerUnavailableError
    from trr_backend.socials.control_plane.shared_accounts import _shared_account_catalog_requires_modal_executor
    from trr_backend.socials.control_plane.worker_health import (
        assert_worker_available_when_queue_enabled,
        is_queue_enabled,
    )

    queue_enabled = is_queue_enabled()
    remote_plane_enforced = is_remote_job_plane_enabled()
    used_inline_fallback = False
    normalized_execution_preference = str(execution_preference or "auto").strip().lower() or "auto"
    prefer_local_inline = normalized_execution_preference == "prefer_local_inline"
    requires_modal_executor = _shared_account_catalog_requires_modal_executor(
        platform=platform,
        pipeline_ingest_mode=pipeline_ingest_mode,
    )
    can_use_local_inline_fallback = _can_use_local_catalog_inline_fallback(
        allow_inline_dev_fallback=(allow_inline_dev_fallback or prefer_local_inline),
        remote_plane_enforced=remote_plane_enforced,
        requires_modal_executor=requires_modal_executor,
        explicit_local_preference=prefer_local_inline,
    )
    if prefer_local_inline:
        if not can_use_local_inline_fallback:
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "SOCIAL_LOCAL_INLINE_PREFERENCE_UNAVAILABLE",
                    "message": (
                        "execution_preference=prefer_local_inline is only available when local/dev inline execution "
                        "is permitted."
                    ),
                    "execution_preference": normalized_execution_preference,
                    "is_local_or_dev_runtime": _is_local_or_dev_runtime(),
                    "remote_plane_enforced": remote_plane_enforced,
                },
            )
        return {
            "queue_enabled": False,
            "used_inline_fallback": True,
            "requires_modal_executor": requires_modal_executor,
        }
    if queue_enabled:
        try:
            assert_worker_available_when_queue_enabled(
                required_execution_backend="modal" if requires_modal_executor else None,
                platform=platform if requires_modal_executor else None,
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
                            "Shared-account catalog operations for this platform require the Modal remote executor."
                            if requires_modal_executor
                            else _remote_worker_unavailable_message(exc)
                            if remote_plane_enforced
                            else str(exc)
                        ),
                        "execution_mode": canonical_execution_mode(),
                        "execution_owner": execution_owner_label(),
                        "worker_health": _worker_health_detail(exc.worker_health),
                        "required_execution_backend": "modal" if requires_modal_executor else None,
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
                    "Shared-account catalog operations for this platform require the Modal remote executor."
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


def _build_catalog_worker_cap_transparency(result: Mapping[str, Any]) -> dict[str, Any]:
    """Surface the honest worker-cap truth from the adaptive worker plan.

    Reads the ``adaptive_worker_plan`` recorded on the launch result and exposes:
    - ``requested_details_worker_count``: what the operator asked for (pre-cap).
    - ``details_refresh_worker_count``: what was actually applied after the binding cap.
    - ``live_apply_binding_cap``: the v4 Live APPLY binding cap (``runbook_state.binding_cap``).
    - ``worker_cap_note``: a human-readable explanation of the request-vs-applied delta.

    Also surfaces the idempotency outcome (``deduped``) when the reservation reused an
    existing run rather than inserting a fresh one. Defensive against missing keys; never
    raises. Returns an empty dict only when no plan and no dedupe signal are present.
    """

    plan = result.get("adaptive_worker_plan")
    plan_map: Mapping[str, Any] = plan if isinstance(plan, Mapping) else {}
    runbook_state = plan_map.get("runbook_state")
    runbook_map: Mapping[str, Any] = runbook_state if isinstance(runbook_state, Mapping) else {}

    requested = plan_map.get("requested_details_worker_count")
    applied = plan_map.get("details_refresh_worker_count")
    binding_cap = runbook_map.get("binding_cap")
    if binding_cap is None:
        # Fall back to the budget decision's binding-cap signal when the plan's
        # runbook_state does not carry it (e.g. older or partial plan payloads).
        budget_decision = result.get("budget_decision")
        budget_map: Mapping[str, Any] = budget_decision if isinstance(budget_decision, Mapping) else {}
        budget_limits = budget_map.get("limits")
        limits_map: Mapping[str, Any] = budget_limits if isinstance(budget_limits, Mapping) else {}
        binding_cap = budget_map.get("live_apply_binding_cap")
        if binding_cap is None:
            binding_cap = limits_map.get("live_apply_binding_cap")

    transparency: dict[str, Any] = {}
    if plan_map:
        transparency["requested_details_worker_count"] = requested
        transparency["details_refresh_worker_count"] = applied
        transparency["live_apply_binding_cap"] = binding_cap
        requested_label = requested if requested is not None else "auto"
        applied_label = applied if applied is not None else "auto"
        cap_label = binding_cap if binding_cap is not None else "n/a"
        transparency["worker_cap_note"] = (
            f"requested {requested_label}, applied {applied_label} "
            f"(v4 binding cap {cap_label}; set enable_cap4_canary for 4)"
        )

    # Idempotency outcome from the reservation (deduped == True means an existing run was
    # reused). Only surfaced when the launch result carries the additive key.
    if "deduped" in result:
        transparency["deduped"] = bool(result.get("deduped"))

    return transparency


def _build_catalog_comments_skip_transparency(run_config: Mapping[str, Any]) -> dict[str, Any]:
    """Surface the precise, run-config-driven reason the comments stage was skipped.

    Delegates to the canonical ``derive_comments_skip_reason`` helper so the dashboard shows
    the exact reason instead of any hardcoded narrative. Defensive against missing keys; never
    raises (the helper itself is safe to call with ``{}``).
    """

    from trr_backend.socials.pipelines.account_catalog.launch import (
        derive_comments_skip_reason,
    )

    skip = derive_comments_skip_reason(run_config or {})
    return {
        "comments_skip_reason": skip.get("reason"),
        "comments_skip_detail": skip.get("detail"),
        "comments_operator_action": skip.get("operator_action"),
    }


def _finalize_social_account_catalog_route_response(
    *,
    result: Mapping[str, Any],
    platform: str,
    queue_enabled: bool,
    used_inline_fallback: bool,
    requires_modal_executor: bool,
    background_tasks: BackgroundTasks,
) -> dict[str, Any]:
    run_id = str(result.get("run_id") or "").strip()
    catalog_run_id = str(result.get("catalog_run_id") or run_id or "").strip()
    comments_run_id = str(result.get("comments_run_id") or "").strip()
    launch_resolution_pending = result.get("launch_task_resolution_pending") is True or str(
        result.get("launch_state") or ""
    ).strip().lower() in {"pending", "finalizing"}
    if not queue_enabled and catalog_run_id and not launch_resolution_pending:
        logger.warning(
            "Catalog route using inline fallback: platform=%s queue_enabled=%s requires_modal_executor=%s",
            platform,
            queue_enabled,
            requires_modal_executor,
        )
        _start_runs_in_background(
            [catalog_run_id],
            background_tasks,
            worker_prefix=f"api-background:catalog:{platform}",
        )
    if not queue_enabled and comments_run_id and not launch_resolution_pending:
        from trr_backend.socials.pipelines.account_catalog.launch import (
            INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
            INSTAGRAM_COMMENTS_SCRAPLING_WORKER_LANE,
        )

        _start_runs_in_background(
            [comments_run_id],
            background_tasks,
            worker_prefix=f"api-background:comments:{platform}",
            stage=INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
            platform="instagram",
            supported_platforms=["instagram"],
            metadata_updates={"worker_lane": INSTAGRAM_COMMENTS_SCRAPLING_WORKER_LANE},
        )
    execution_mode, execution_mode_canonical, execution_mode_legacy = _resolve_social_execution_modes(
        queue_enabled=queue_enabled,
        used_inline_fallback=used_inline_fallback,
    )
    return {
        **result,
        "status": "queued" if queue_enabled else "started",
        "queue_enabled": queue_enabled,
        "used_inline_fallback": used_inline_fallback,
        "requires_modal_executor": requires_modal_executor,
        "execution_mode": execution_mode,
        "execution_mode_canonical": execution_mode_canonical,
        "execution_mode_legacy": execution_mode_legacy,
        "deprecations": [_social_execution_mode_deprecation_payload()],
        # Honest operator-facing transparency: worker-cap truth (requested vs applied vs
        # binding cap) and the precise comments-skip reason. Both degrade gracefully when
        # the launch result lacks the underlying metadata.
        **_build_catalog_worker_cap_transparency(result),
        **_build_catalog_comments_skip_transparency(result),
    }


class CatalogBackfillRequest(SourceScopedRequest):
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Field(default="network")
    date_start: datetime | None = None
    date_end: datetime | None = None
    backfill_scope: Literal["full_history", "bounded_window"] = Field(default="full_history")
    allow_inline_dev_fallback: bool = Field(default=False)
    execution_preference: Literal["auto", "prefer_local_inline"] = Field(default="auto")
    selected_tasks: list[Literal["post_details", "comments", "media"]] | None = Field(default=None)
    detail_worker_count: int | None = Field(default=None, ge=1, le=12)
    comments_worker_count: int | None = Field(default=None, ge=1, le=24)
    comments_enable_media_followups: bool | None = Field(default=None)
    force_catalog_rediscovery: bool = Field(default=False)
    enable_cap4_canary: bool = Field(default=False)
    apply_run_id: UUID | None = Field(default=None)
    operator_confirmation: str | None = Field(default=None)

    @model_validator(mode="after")
    def validate_selected_tasks(self) -> CatalogBackfillRequest:
        if self.selected_tasks is None:
            return self
        normalized = [str(task or "").strip().lower() for task in self.selected_tasks if str(task or "").strip()]
        if not normalized:
            raise ValueError("selected_tasks must include at least one of post_details, comments, or media")
        deduped: list[Literal["post_details", "comments", "media"]] = []
        for task in ("post_details", "comments", "media"):
            if task in normalized:
                deduped.append(task)
        self.selected_tasks = deduped
        return self


def _instagram_2025_backfill_apply_confirmation(run_id: str) -> str:
    return f"APPLY INSTAGRAM 2025 BACKFILL {run_id}"


def _instagram_2025_backfill_apply_pending_metadata(run_id: str) -> dict[str, Any]:
    confirmation = _instagram_2025_backfill_apply_confirmation(run_id)
    return {
        "launch_state": "pending_apply_confirmation",
        "launch_task_resolution_pending": True,
        "requires_apply_confirmation": True,
        "apply_required": True,
        "apply_run_id": run_id,
        "required_confirmation": confirmation,
        "runbook_state": {
            "phase": "live_apply",
            "state": "pending_apply_confirmation",
            "mandatory": True,
            "current_comments_cap": 2,
            "speed_canary_optional": True,
            "speed_canary_cap": 4,
            "minimum_completed_comments_jobs": 25,
            "message": "Live APPLY at cap 2 must be confirmed before catalog jobs are created.",
        },
    }


def _instagram_backfill_requires_apply_confirmation(
    *,
    platform: str,
    date_start: datetime | None,
    date_end: datetime | None,
    selected_tasks: Sequence[str],
) -> bool:
    normalized_platform = str(platform or "").strip().lower()
    if normalized_platform != "instagram":
        return False
    if not ({"post_details", "comments", "media"} & set(selected_tasks)):
        return False
    if date_start is None and date_end is None:
        return False
    start_year = date_start.year if date_start else None
    end_year = date_end.year if date_end else None
    if start_year is not None and end_year is not None:
        return start_year <= 2025 <= end_year
    return start_year == 2025 or end_year == 2025


def _attach_instagram_apply_metadata(result: Mapping[str, Any]) -> dict[str, Any]:
    run_id = str(result.get("run_id") or result.get("catalog_run_id") or "").strip()
    apply_metadata = _instagram_2025_backfill_apply_pending_metadata(run_id) if run_id else {}
    return {
        **dict(result),
        **apply_metadata,
        "requires_apply_confirmation": True,
        "apply_required": True,
        "apply_run_id": run_id or None,
    }


class CatalogSyncRecentRequest(SourceScopedRequest):
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Field(default="network")
    lookback_days: int = Field(default=1, ge=1, le=30)
    allow_inline_dev_fallback: bool = Field(default=False)


class CatalogSyncNewerRequest(SourceScopedRequest):
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Field(default="network")
    allow_inline_dev_fallback: bool = Field(default=False)


class CatalogResumeTailRequest(SourceScopedRequest):
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Field(default="network")
    allow_inline_dev_fallback: bool = Field(default=False)


class CatalogRemediateDriftRequest(BaseModel):
    run_id: UUID | None = None
    requeue_canary: bool = Field(default=False)
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Field(default="bravo")

    @model_validator(mode="after")
    def normalize_source_scope(self) -> CatalogRemediateDriftRequest:
        self.source_scope = preserve_source_scope_param(self.source_scope, default="bravo")  # type: ignore[assignment]
        return self


@router.get("/profiles/{platform}/{account_handle}/catalog/posts")
def get_social_account_catalog_posts_route(
    platform: str,
    account_handle: str,
    page: int = Query(default=1, ge=1, le=10_000),
    page_size: int = Query(default=25, ge=1, le=100),
    assignment_status: Literal["assigned", "unassigned", "ambiguous", "needs_review"] | None = Query(default=None),
    _: InternalAdminUser = cast("InternalAdminUser", None),
) -> dict[str, Any]:
    try:
        return social_profile_reads.get_catalog_posts(
            platform=platform,
            account_handle=account_handle,
            page=page,
            page_size=page_size,
            assignment_status=assignment_status,
        )
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc


@router.get("/profiles/{platform}/{account_handle}/catalog/posts/{source_id}/detail")
def get_social_account_catalog_post_detail_route(
    platform: str,
    account_handle: str,
    source_id: str,
    _: InternalAdminUser = cast("InternalAdminUser", None),
) -> dict[str, Any]:
    try:
        return social_profile_reads.get_catalog_post_detail(
            platform=platform,
            account_handle=account_handle,
            source_id=source_id,
        )
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to fetch social account catalog post detail: platform=%s account=%s source_id=%s",
            platform,
            account_handle,
            source_id,
        )
        raise _to_social_read_http_exception(exc) from exc


@router.get("/profiles/{platform}/{account_handle}/catalog/review-queue")
def get_social_account_catalog_review_queue_route(
    platform: str,
    account_handle: str,
    _: InternalAdminUser = cast("InternalAdminUser", None),
) -> dict[str, Any]:
    cache_key = _account_profile_cache_key(
        surface="catalog-review-queue",
        platform=platform,
        account_handle=account_handle,
    )
    try:
        return _resolve_account_profile_singleflight(
            cache_key,
            lambda: social_profile_reads.get_catalog_review_queue(platform=platform, account_handle=account_handle),
        )
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to fetch social account catalog review queue: platform=%s account=%s",
            platform,
            account_handle,
        )
        raise _to_social_read_http_exception(exc) from exc


@router.get("/profiles/{platform}/{account_handle}/catalog/runs/{run_id}/progress")
def get_social_account_catalog_run_progress_route(
    platform: str,
    account_handle: str,
    run_id: UUID,
    recent_log_limit: int = Query(default=20, ge=1, le=100),
    fast: bool = Query(default=False),
    _: InternalAdminUser = cast("InternalAdminUser", None),
) -> dict[str, Any]:
    try:
        progress_payload = _resolve_account_profile_catalog_run_progress(
            platform=platform,
            account_handle=account_handle,
            run_id=str(run_id),
            recent_log_limit=recent_log_limit,
            fast=fast,
            loader=social_profile_reads.get_catalog_run_progress,
        )
        # Surface the precise, run-config-driven comments-skip reason so the dashboard
        # shows the exact cause instead of any hardcoded narrative. The progress payload
        # already exposes effective_selected_tasks / stage_graph / target_readiness, which
        # is exactly what the helper reads; it is fully defensive against missing keys.
        if isinstance(progress_payload, Mapping):
            return {
                **progress_payload,
                **_build_catalog_worker_cap_transparency(progress_payload),
                **_build_catalog_comments_skip_transparency(progress_payload),
            }
        return progress_payload
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to fetch social account catalog run progress: platform=%s account=%s run_id=%s",
            platform,
            account_handle,
            run_id,
        )
        raise _to_social_read_http_exception(exc) from exc


@router.get("/profiles/{platform}/{account_handle}/catalog/budget")
def get_social_account_catalog_budget_decision_route(
    platform: str,
    account_handle: str,
    _: InternalAdminUser = cast("InternalAdminUser", None),
) -> dict[str, Any]:
    try:
        return social_profile_reads.get_catalog_budget_decision(
            platform=platform,
            account_handle=account_handle,
        )
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to fetch social account catalog budget decision: platform=%s account=%s",
            platform,
            account_handle,
        )
        raise _to_social_read_http_exception(exc) from exc


@router.get("/profiles/{platform}/{account_handle}/catalog/runs/{run_id}/diagnostics")
def get_social_account_catalog_run_diagnostics_route(
    platform: str,
    account_handle: str,
    run_id: UUID,
    _: InternalAdminUser = cast("InternalAdminUser", None),
) -> dict[str, Any]:
    try:
        return social_profile_reads.get_catalog_run_diagnostics(
            platform=platform,
            account_handle=account_handle,
            run_id=str(run_id),
        )
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to fetch social account catalog run diagnostics: platform=%s account=%s run_id=%s",
            platform,
            account_handle,
            run_id,
        )
        raise _to_social_read_http_exception(exc) from exc


@router.get("/profiles/{platform}/{account_handle}/catalog/verification")
def get_social_account_catalog_verification_route(
    platform: str,
    account_handle: str,
    run_id: UUID | None = None,
    _: InternalAdminUser = cast("InternalAdminUser", None),
) -> dict[str, Any]:
    try:
        return social_profile_reads.get_catalog_verification(
            platform=platform,
            account_handle=account_handle,
            run_id=str(run_id) if run_id else None,
        )
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to fetch social account catalog verification: platform=%s account=%s run_id=%s",
            platform,
            account_handle,
            run_id,
        )
        raise _internal_error_response(exc) from exc


@router.get("/profiles/{platform}/{account_handle}/catalog/gap-analysis")
def get_social_account_catalog_gap_analysis_route(
    platform: str,
    account_handle: str,
    _: InternalAdminUser = cast("InternalAdminUser", None),
) -> dict[str, Any]:
    cache_key = _account_profile_cache_key(
        surface="catalog-gap-analysis",
        platform=platform,
        account_handle=account_handle,
    )
    try:
        return _resolve_account_profile_singleflight(
            cache_key,
            lambda: social_profile_reads.get_catalog_gap_analysis_status(
                platform=platform,
                account_handle=account_handle,
            ),
        )
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to fetch social account catalog gap analysis: platform=%s account=%s",
            platform,
            account_handle,
        )
        raise _to_social_read_http_exception(exc) from exc


__all__ = [name for name in globals() if not name.startswith("__")]
