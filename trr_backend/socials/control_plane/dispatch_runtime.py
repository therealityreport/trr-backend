# ruff: noqa: F822
"""Dispatch and execution runtime entrypoints for the social control plane."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from trr_backend.socials.control_plane.run_lifecycle import _legacy_module as _published
from trr_backend.socials.control_plane.run_lifecycle import legacy as _legacy_proxy
from trr_backend.socials.provider_registry import LateModuleProvider, adopt_published, publish_module_slot

legacy: Any = _legacy_proxy
del _legacy_proxy
_PROVIDER = LateModuleProvider(
    globals(),
    prefix="DISPATCH_RUNTIME_PROVIDER",
    commit=publish_module_slot(globals(), "legacy"),
)
_require_provider_ready = _PROVIDER.require
_configure_legacy_provider = _PROVIDER.configure
register_provider_publication_callback = _PROVIDER.register_module_publication_callback
adopt_published(_PROVIDER, _published)
del _published

def _call_extracted_override(name: str, local_impl: Any, /, *args: Any, **kwargs: Any) -> Any:
    candidate = getattr(legacy, name, None)
    if callable(candidate) and not getattr(candidate, "__trr_delegates_to_control_plane__", False):
        return candidate(*args, **kwargs)
    return local_impl(*args, **kwargs)


def _instagram_public_comments_worker_cap(run_id: str, cache: dict[str, int | None]) -> int | None:
    """Read and cache an Instagram public-comments worker cap for one sweep."""
    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        return None
    if normalized_run_id in cache:
        return cache[normalized_run_id]
    cap: int | None = None
    try:
        row = legacy.pg.fetch_one(
            "select config from social.scrape_runs where id = %s::uuid",
            [normalized_run_id],
        )
        config = legacy._metadata_dict((row or {}).get("config"))
        raw_cap = config.get("comments_worker_cap_current")
        if raw_cap is not None:
            cap = max(1, legacy._normalize_non_negative_int(raw_cap))
    except Exception:  # noqa: BLE001
        cap = None
    cache[normalized_run_id] = cap
    return cap


def _terminal_frontier_continuation_next_available_at(cooldown: dict[str, Any] | None) -> Any:
    cooldown_until = legacy._coerce_dt(legacy._metadata_dict(cooldown).get("cooldown_until"))
    if cooldown_until is not None:
        return cooldown_until
    return legacy._now_utc() + timedelta(seconds=max(5, legacy._modal_dispatch_retry_delay_seconds()))


def _reconcile_terminal_frontier_modal_call(
    row: dict[str, Any],
    *,
    inspection: dict[str, Any],
    now_utc: Any,
) -> dict[str, Any] | None:
    job_id = str(row.get("id") or "").strip()
    run_id = str(row.get("run_id") or "").strip()
    platform = legacy._normalize_platform_name(row.get("platform")) or ""
    config = legacy._metadata_dict(row.get("config"))
    account_handle = legacy._resolve_dispatch_account_handle(config)
    if not job_id or not run_id or not platform or not account_handle:
        return None
    if legacy._job_stage_from_row(row) != legacy.SHARED_ACCOUNT_POSTS_STAGE:
        return None
    if not legacy._shared_account_uses_frontier_strategy(config, platform=platform):
        return None
    frontier = legacy._get_shared_account_run_frontier(
        run_id=run_id,
        platform=platform,
        account_handle=account_handle,
    )
    if not frontier or bool(frontier.get("exhausted")):
        return None
    frontier_status = str(frontier.get("status") or "").strip().lower()
    if frontier_status in {"completed", "failed", "cancelled"}:
        return None
    if not str(frontier.get("next_cursor") or "").strip():
        return None
    lease_owner = str(frontier.get("lease_owner") or "").strip()
    worker_id = str(row.get("worker_id") or "").strip()
    if lease_owner and lease_owner not in {job_id, worker_id}:
        return None
    enqueue_result = legacy._coerce_shared_posts_enqueue_result(
        legacy._enqueue_shared_posts_job(
            run_id=run_id,
            platform=platform,
            source_scope=str(config.get("source_scope") or row.get("source_scope") or "network"),
            account_handle=account_handle,
            shared_account_source_id=config.get("shared_account_source_id"),
            pipeline_ingest_mode=str(
                config.get("pipeline_ingest_mode") or legacy.SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE
            ),
            runner_count=legacy._normalize_non_negative_int(config.get("runner_count"))
            or legacy.CATALOG_BACKFILL_FULL_HISTORY_RUNNER_COUNT,
            expected_total_posts=legacy._normalize_non_negative_int(frontier.get("total_posts"))
            or legacy._normalize_non_negative_int(config.get("expected_total_posts"))
            or None,
            runner_strategy=legacy.CATALOG_FULL_HISTORY_FRONTIER_STRATEGY,
            partition_strategy=legacy.CATALOG_FULL_HISTORY_FRONTIER_STRATEGY,
            required_worker_lane=str(config.get("required_worker_lane") or "").strip().lower() or None,
            required_execution_backend=str(config.get("required_execution_backend") or "").strip().lower() or None,
            allow_local_dev_inline_bypass=bool(config.get("allow_local_dev_inline_bypass")),
            frontier_auth_allowed=config.get("frontier_auth_allowed"),
            frontier_auth_reason=str(config.get("frontier_auth_reason") or "").strip().lower() or None,
            frontier_transport=(
                str(frontier.get("last_transport") or config.get("frontier_transport") or "").strip().lower() or None
            ),
            transport_preference=str(config.get("transport_preference") or "").strip().lower() or None,
            allow_public_transport_fallback=config.get("allow_public_transport_fallback"),
            selected_tasks=config.get("selected_tasks"),
            effective_selected_tasks=config.get("effective_selected_tasks"),
            details_refresh_skip_detail_fetch=config.get("details_refresh_skip_detail_fetch"),
            details_refresh_force_detail_fetch=config.get("details_refresh_force_detail_fetch"),
            details_refresh_worker_count=legacy._normalize_non_negative_int(config.get("details_refresh_worker_count"))
            or None,
            comments_worker_count=legacy._normalize_non_negative_int(config.get("comments_worker_count")) or None,
            details_refresh_skip_media_followups=config.get("details_refresh_skip_media_followups"),
            tiktok_comments_in_posts_stage=bool(config.get("tiktok_comments_in_posts_stage")),
            tiktok_direct_comment_api_override=bool(config.get("tiktok_direct_comment_api_override")),
            twitter_comments_in_posts_stage=bool(config.get("twitter_comments_in_posts_stage")),
            initiated_by="terminal_modal_frontier_reconciliation",
            priority=legacy._normalize_non_negative_int(row.get("priority")) or 101,
        )
    )
    if enqueue_result.status == "failed":
        release_status = "retrying"
        old_job_status = "retrying"
        error_code = "terminal_modal_frontier_requeue_failed"
        next_available_at = legacy._now_utc() + timedelta(seconds=max(5, legacy._modal_dispatch_retry_delay_seconds()))
    elif enqueue_result.status == "deferred_by_cooldown":
        release_status = "retrying"
        old_job_status = "retrying"
        error_code = "terminal_modal_frontier_deferred_by_cooldown"
        next_available_at = _terminal_frontier_continuation_next_available_at(enqueue_result.cooldown)
    else:
        release_status = "queued"
        old_job_status = "cancelled"
        error_code = "terminal_modal_frontier_requeued"
        next_available_at = None
    legacy._release_shared_account_run_frontier(
        run_id=run_id,
        platform=platform,
        account_handle=account_handle,
        lease_owner=lease_owner or None,
        status=release_status,
    )
    dispatch = legacy._job_dispatch_metadata(row)
    metadata_updates = dict(row.get("metadata") or {})
    metadata_updates["dispatch"] = {
        **dispatch,
        "remote_invocation_status": str(inspection.get("status") or "").strip().lower() or None,
        "remote_invocation_checked_at": inspection.get("checked_at") or legacy._iso(now_utc),
        "remote_task_id": str(inspection.get("task_id") or "").strip() or None,
        "remote_blocked_reason": str(inspection.get("reason") or "").strip() or None,
        "lease_expires_at": None,
    }
    metadata_updates["terminal_modal_frontier_reconciliation"] = {
        "source": "reconcile_terminal_modal_running_jobs",
        "remote_status": str(inspection.get("status") or "").strip().lower() or None,
        "function_call_id": str(dispatch.get("remote_invocation_id") or "").strip() or None,
        "status": enqueue_result.status,
        "continuation_job_id": enqueue_result.job_id,
        "frontier_release_status": release_status,
        "cooldown": enqueue_result.cooldown,
        "error": enqueue_result.error,
        "reconciled_at": legacy._iso(now_utc),
    }
    metadata_updates["retryable"] = old_job_status == "retrying"
    legacy._finish_job(
        job_id,
        status=old_job_status,
        items_found=legacy._normalize_non_negative_int(row.get("items_found")),
        error_message="Terminal Modal invocation reconciled for unfinished shared-account frontier.",
        metadata=metadata_updates,
        last_error_code=error_code,
        last_error_class="ModalFrontierTerminalReconciliation",
        next_available_at=next_available_at,
    )
    return {
        "id": job_id,
        "run_id": run_id,
        "platform": platform,
        "status": old_job_status,
        "remote_invocation_id": str(dispatch.get("remote_invocation_id") or "").strip() or None,
        "frontier_status": release_status,
        "continuation_job_id": enqueue_result.job_id,
        "deferred_by_cooldown": enqueue_result.status == "deferred_by_cooldown",
    }


def recover_dispatch_blocked_no_progress_jobs(*, limit: int = 100) -> list[dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 500))
    blocked_rows, _ = legacy._list_dispatch_blocked_jobs(limit=safe_limit)
    no_progress_seconds = legacy._resolve_modal_dispatch_no_progress_seconds()
    blocked_failure_limit = legacy._resolve_modal_dispatch_blocked_failure_limit()
    recovered: list[dict[str, Any]] = []
    affected_run_ids: set[str] = set()
    for row in blocked_rows:
        job_id = str(row.get("id") or "").strip()
        if not job_id:
            continue
        full_row = (
            legacy.pg.fetch_one(
                """
                select
                  id::text as id,
                  run_id::text as run_id,
                  status,
                  items_found,
                  attempt_count,
                  max_attempts,
                  error_message,
                  last_error_code,
                  metadata
                from social.scrape_jobs
                where id = %s::uuid
                """,
                [job_id],
            )
            or {}
        )
        if not legacy._job_is_dispatch_blocked(full_row):
            continue
        dispatch = legacy._job_dispatch_metadata(full_row)
        blocked_for_seconds = legacy._dispatch_blocked_seconds(dispatch, full_row)
        blocked_failure_count = legacy._dispatch_metadata_blocked_failure_count(
            dispatch
        ) or legacy._dispatch_metadata_attempt_count(dispatch)
        if blocked_for_seconds < no_progress_seconds and blocked_failure_count < blocked_failure_limit:
            continue
        reason = legacy._normalize_dispatch_blocked_reason(
            remote_blocked_reason=dispatch.get("remote_blocked_reason"),
            last_dispatch_error_code=dispatch.get("last_dispatch_error_code"),
            last_dispatch_error=dispatch.get("last_dispatch_error"),
        )
        attempt_count = legacy._normalize_non_negative_int(full_row.get("attempt_count"))
        max_attempts = max(1, legacy._normalize_non_negative_int(full_row.get("max_attempts")) or 1)
        if reason == legacy.STALE_MODAL_DISPATCH_UNCLAIMED_ERROR_CODE and attempt_count < max_attempts:
            now_utc = legacy._now_utc()
            retry_dispatch = {
                **dispatch,
                "remote_invocation_id": None,
                "remote_invocation_status": None,
                "remote_invocation_checked_at": None,
                "remote_task_id": None,
                "remote_pending_since": None,
                "remote_blocked_reason": None,
                "lease_expires_at": None,
                "last_dispatch_error": None,
                "last_dispatch_error_code": None,
                "last_dispatch_error_at": None,
                "dispatch_attempt_count": 0,
                "dispatch_blocked_failure_count": 0,
                "dispatch_blocked_terminalized_at": None,
                "stale_unclaimed_dispatch_exhausted": False,
            }
            legacy._finish_job(
                job_id,
                status="retrying",
                items_found=legacy._normalize_non_negative_int(full_row.get("items_found")),
                error_message=None,
                metadata={
                    "dispatch": retry_dispatch,
                    "retryable_failure_recovery": {
                        "reason": reason,
                        "recovered_at": legacy._iso(now_utc),
                        "source": "recover_dispatch_blocked_no_progress_jobs",
                    },
                },
                last_error_code=None,
                last_error_class=None,
                next_available_at=now_utc + timedelta(seconds=max(5, legacy._modal_dispatch_retry_delay_seconds())),
            )
            run_id = str(full_row.get("run_id") or "").strip()
            if run_id:
                affected_run_ids.add(run_id)
            recovered.append(
                {
                    "id": job_id,
                    "run_id": run_id or None,
                    "status": "retrying",
                    "stuck_reason": reason,
                }
            )
            continue
        legacy._finish_job(
            job_id,
            status="failed",
            items_found=legacy._normalize_non_negative_int(full_row.get("items_found")),
            error_message=str(dispatch.get("last_dispatch_error") or "Modal dispatch blocked"),
            metadata={
                "dispatch": {
                    **dispatch,
                    "remote_invocation_status": "failed",
                    "remote_invocation_checked_at": legacy._iso(legacy._now_utc()),
                    "remote_blocked_reason": reason,
                    "dispatch_blocked_failure_count": blocked_failure_count,
                    "dispatch_blocked_terminalized_at": legacy._iso(legacy._now_utc()),
                }
            },
            last_error_code="modal_dispatch_blocked",
            last_error_class="ModalDispatchBlocked",
        )
        run_id = str(full_row.get("run_id") or "").strip()
        if run_id:
            affected_run_ids.add(run_id)
        recovered.append(
            {
                "id": job_id,
                "run_id": run_id or None,
                "status": "failed",
                "stuck_reason": reason,
            }
        )
    for run_id in sorted(affected_run_ids):
        legacy._finalize_run_status(run_id, force_recompute=True)
    return recovered


def reconcile_terminal_modal_running_jobs(
    *,
    run_id: str | None = None,
    platform: str | None = None,
    account_handle: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    features = legacy._scrape_jobs_features()
    if not bool(features.get("has_queue_fields")):
        return []
    safe_limit = max(1, min(int(limit), 250))
    normalized_platform = legacy._normalize_platform_name(platform) or None
    normalized_account = legacy._normalize_account_handle(account_handle) or None
    rows = legacy.pg.fetch_all(
        """
        select
          id::text as id,
          run_id::text as run_id,
          platform,
          source_scope,
          status,
          priority,
          items_found,
          attempt_count,
          max_attempts,
          worker_id,
          started_at,
          claimed_at,
          heartbeat_at,
          created_at,
          error_message,
          last_error_code,
          config,
          metadata
        from social.scrape_jobs
        where status = 'running'
          and (%s::uuid is null or run_id = %s::uuid)
          and (%s::text is null or platform = %s::text)
          and (%s::text is null or lower(coalesce(config->>'account', '')) = %s::text)
          and coalesce(metadata->'dispatch'->>'dispatch_backend', '') = 'modal'
          and nullif(coalesce(metadata->'dispatch'->>'remote_invocation_id', ''), '') is not null
          and coalesce(heartbeat_at, started_at, claimed_at, created_at) < now() - interval '300 seconds'
        order by coalesce(heartbeat_at, started_at, claimed_at, created_at) asc
        limit %s
        """,
        [run_id, run_id, normalized_platform, normalized_platform, normalized_account, normalized_account, safe_limit],
    )
    if not rows:
        return []
    reconciled_rows: list[dict[str, Any]] = []
    now_utc = legacy._now_utc()
    for row in rows:
        job_id = str(row.get("id") or "").strip()
        if not job_id:
            continue
        stage = legacy._job_stage_from_row(row)
        last_activity = legacy._coerce_dt(
            row.get("heartbeat_at") or row.get("started_at") or row.get("claimed_at") or row.get("created_at")
        )
        if last_activity is None:
            continue
        if last_activity.tzinfo is None:
            last_activity = last_activity.replace(tzinfo=legacy.UTC)
        dispatch = legacy._job_dispatch_metadata(row)
        remote_invocation_id = str(dispatch.get("remote_invocation_id") or "").strip()
        if not remote_invocation_id:
            continue
        inspection = legacy._refresh_remote_modal_invocation_state(row, lease_expires_at=None)
        inspection_status = str(inspection.get("status") or "").strip().lower()
        if inspection_status != "completed":
            if (
                inspection_status
                and inspection_status != "unknown"
                and not legacy._modal_invocation_is_nonterminal(inspection_status)
            ):
                frontier_result = _reconcile_terminal_frontier_modal_call(
                    row,
                    inspection=inspection,
                    now_utc=now_utc,
                )
                if frontier_result is not None:
                    reconciled_rows.append(frontier_result)
            continue
        metadata_updates = dict(row.get("metadata") or {})
        metadata_updates["dispatch"] = {
            **dispatch,
            "remote_invocation_status": "completed",
            "remote_invocation_checked_at": inspection.get("checked_at") or legacy._iso(now_utc),
            "remote_task_id": str(inspection.get("task_id") or "").strip() or None,
            "remote_blocked_reason": str(inspection.get("reason") or "").strip() or None,
        }
        activity = dict(metadata_updates.get("activity") or {})
        activity.setdefault("phase", f"{stage}_end")
        activity["last_progress_at"] = legacy._iso(now_utc)
        metadata_updates["activity"] = activity
        metadata_updates["terminal_modal_reconciliation"] = {
            "source": "reconcile_terminal_modal_running_jobs",
            "function_call_id": remote_invocation_id,
            "remote_status": inspection_status,
            "reason": "modal_call_completed_but_db_job_still_running",
            "reconciled_at": legacy._iso(now_utc),
        }
        legacy._finish_job(
            job_id,
            status="completed",
            items_found=legacy._normalize_non_negative_int(row.get("items_found")),
            metadata=metadata_updates,
        )
        reconciled_rows.append(
            {
                "id": job_id,
                "run_id": str(row.get("run_id") or "").strip() or None,
                "platform": str(row.get("platform") or "").strip() or None,
                "status": "completed",
                "remote_invocation_id": remote_invocation_id,
            }
        )
    return reconciled_rows


def recover_stale_unclaimed_dispatched_jobs(
    *,
    run_id: str | None = None,
    platform: str | None = None,
    account_handle: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    features = legacy._scrape_jobs_features()
    if not bool(features.get("has_queue_fields")):
        return []
    safe_limit = max(1, min(int(limit), 250))
    normalized_platform = legacy._normalize_platform_name(platform) or None
    normalized_account = legacy._normalize_account_handle(account_handle) or None
    rows = legacy.pg.fetch_all(
        """
        select
          id::text as id,
          run_id::text as run_id,
          platform,
          status,
          items_found,
          attempt_count,
          error_message,
          last_error_code,
          config,
          metadata
        from social.scrape_jobs
        where status in ('queued', 'pending', 'retrying', 'running')
          and (%s::uuid is null or run_id = %s::uuid)
          and (%s::text is null or platform = %s::text)
          and (%s::text is null or lower(coalesce(config->>'account', '')) = %s::text)
          and coalesce(worker_id, '') = ''
          and claimed_at is null
          and coalesce(metadata->'dispatch'->>'dispatch_backend', '') = 'modal'
          and nullif(coalesce(metadata->'dispatch'->>'dispatch_requested_at', ''), '') is not null
          and nullif(coalesce(metadata->'dispatch'->>'lease_expires_at', ''), '') is not null
          and (metadata->'dispatch'->>'lease_expires_at')::timestamptz <= now()
        order by (metadata->'dispatch'->>'lease_expires_at')::timestamptz asc, created_at asc
        limit %s
        """,
        [run_id, run_id, normalized_platform, normalized_platform, normalized_account, normalized_account, safe_limit],
    )
    if not rows:
        return []

    recovered_rows: list[dict[str, Any]] = []
    recovery_limit = legacy._resolve_stale_modal_dispatch_recovery_limit()
    recovery_backoff = timedelta(seconds=max(5, legacy._modal_dispatch_retry_delay_seconds()))
    recovery_error = "Remote Modal dispatch lease expired before any worker claimed the job."
    now_utc = legacy._now_utc()

    for row in rows:
        job_id = str(row.get("id") or "").strip()
        if not job_id:
            continue
        existing_metadata = dict(row.get("metadata") or {})
        dispatch = legacy._job_dispatch_metadata(row)
        remote_invocation_id = str(dispatch.get("remote_invocation_id") or "").strip()
        if remote_invocation_id:
            refreshed_lease_expires_at = legacy._now_utc() + timedelta(
                seconds=legacy._resolve_stale_seconds_for_job(
                    platform=str(row.get("platform") or ""),
                    stage=legacy._job_stage_from_row(row),
                    has_queue_fields=True,
                )
            )
            inspection = legacy._refresh_remote_modal_invocation_state(
                row,
                lease_expires_at=refreshed_lease_expires_at,
            )
            inspection_status = str(inspection.get("status") or "").strip().lower()
            if legacy._modal_invocation_is_nonterminal(inspection_status):
                continue

        recovery_count = legacy._dispatch_metadata_recovery_count(dispatch) + 1
        exhausted = recovery_count >= recovery_limit
        dispatch_payload = {
            **dispatch,
            "remote_invocation_id": None if remote_invocation_id else dispatch.get("remote_invocation_id"),
            "remote_invocation_status": (
                "failed" if remote_invocation_id else legacy._modal_invocation_status(dispatch) or None
            ),
            "remote_invocation_checked_at": (
                legacy._iso(now_utc) if remote_invocation_id else dispatch.get("remote_invocation_checked_at")
            ),
            "remote_task_id": None,
            "remote_pending_since": None,
            "remote_blocked_reason": None,
            "lease_expires_at": None,
            "last_dispatch_error": recovery_error,
            "last_dispatch_error_at": legacy._iso(now_utc),
            "last_dispatch_error_code": legacy.STALE_MODAL_DISPATCH_UNCLAIMED_ERROR_CODE,
            "stale_unclaimed_recovery_count": recovery_count,
            "stale_unclaimed_recovered_at": legacy._iso(now_utc),
            "stale_unclaimed_recovery_limit": recovery_limit,
            "stale_unclaimed_dispatch_exhausted": exhausted,
        }
        metadata_updates = {
            **existing_metadata,
            "dispatch": dispatch_payload,
            "job_error_code": legacy.STALE_MODAL_DISPATCH_UNCLAIMED_ERROR_CODE,
            "retryable": not exhausted,
        }
        if exhausted:
            legacy._finish_job(
                job_id,
                status="failed",
                items_found=legacy._normalize_non_negative_int(row.get("items_found")),
                error_message=recovery_error,
                metadata=metadata_updates,
                last_error_code=legacy.STALE_MODAL_DISPATCH_UNCLAIMED_ERROR_CODE,
                last_error_class="ModalDispatchUnclaimed",
            )
            recovered_rows.append(
                {
                    "id": job_id,
                    "run_id": str(row.get("run_id") or "").strip() or None,
                    "platform": str(row.get("platform") or "").strip() or None,
                    "status": "failed",
                    "recovery_count": recovery_count,
                    "exhausted": True,
                }
            )
            continue
        legacy._finish_job(
            job_id,
            status="retrying",
            items_found=legacy._normalize_non_negative_int(row.get("items_found")),
            error_message=recovery_error,
            metadata=metadata_updates,
            last_error_code=legacy.STALE_MODAL_DISPATCH_UNCLAIMED_ERROR_CODE,
            last_error_class="ModalDispatchUnclaimed",
            next_available_at=now_utc + recovery_backoff,
        )
        recovered_rows.append(
            {
                "id": job_id,
                "run_id": str(row.get("run_id") or "").strip() or None,
                "platform": str(row.get("platform") or "").strip() or None,
                "status": "retrying",
                "recovery_count": recovery_count,
                "exhausted": False,
            }
        )

    affected_run_ids = sorted({str(row.get("run_id") or "").strip() for row in recovered_rows if row.get("run_id")})
    for affected_run_id in affected_run_ids:
        legacy._finalize_run_status(affected_run_id, force_recompute=True)
    return recovered_rows


def dispatch_due_social_jobs(*, run_id: str | None = None, limit: int | None = None) -> dict[str, Any]:
    # Worker C handoff: this extracted dispatcher now carries the legacy parity
    # fixes for auth cooldowns, remote function stamping, run-scoped comments
    # capacity recovery, and capacity-stage accounting. The legacy body remains
    # as a compatibility/reference surface; do not flip additional direct callers
    # without a fresh whole-body parity diff.
    safe_limit = max(1, min(int(limit or legacy._modal_dispatch_limit()), legacy.SOCIAL_JOB_CLAIM_BATCH_SIZE_MAX))
    if not legacy.is_queue_enabled():
        return {"dispatched_job_ids": [], "dispatch_attempts": 0, "reason": "queue_disabled"}
    if not legacy.is_modal_remote_executor_enabled():
        return {"dispatched_job_ids": [], "dispatch_attempts": 0, "reason": "remote_executor_not_modal"}
    if legacy._run_pause_after_current_requested(run_id):
        legacy._touch_modal_social_dispatcher_heartbeat(
            metadata_updates={
                "dispatch_enabled": True,
                "last_dispatch_blocked_reason": "pause_after_current",
                "last_dispatch_error": None,
                "last_dispatch_error_code": None,
            }
        )
        return {"dispatched_job_ids": [], "dispatch_attempts": 0, "reason": "pause_after_current"}
    recovered_capacity = (
        legacy.recover_failed_instagram_comments_capacity_jobs(
            run_id=run_id,
            limit=max(safe_limit, 25),
        )
        if run_id
        else []
    )
    recovered_blocked = _call_extracted_override(
        "recover_dispatch_blocked_no_progress_jobs",
        recover_dispatch_blocked_no_progress_jobs,
        limit=max(safe_limit, 25),
    )
    resolution = legacy._modal_social_dispatch_resolution()
    ready = bool(resolution.get("resolved"))
    reason = str(resolution.get("reason") or "").strip() or None
    if not ready:
        legacy._touch_modal_social_dispatcher_heartbeat(
            metadata_updates={
                "dispatch_enabled": False,
                "last_dispatch_error": reason,
                "last_dispatch_error_code": reason,
                "last_dispatch_blocked_reason": reason,
                "last_dispatch_error_at": legacy._iso(legacy._now_utc()),
            }
        )
        return {
            "dispatched_job_ids": [],
            "dispatch_attempts": 0,
            "reason": reason,
            "recovered_dispatch_blocked_job_ids": [
                str(row.get("id") or "").strip() for row in recovered_blocked if str(row.get("id") or "").strip()
            ],
            "recovered_capacity_job_ids": [
                str(row.get("id") or "").strip() for row in recovered_capacity if str(row.get("id") or "").strip()
            ],
        }

    recovered_unclaimed = _call_extracted_override(
        "recover_stale_unclaimed_dispatched_jobs",
        recover_stale_unclaimed_dispatched_jobs,
        run_id=run_id,
        limit=max(safe_limit, 25),
    )

    candidates = legacy._list_candidate_jobs_for_modal_dispatch(run_id=run_id, limit=max(safe_limit * 4, 50))
    running_counts = legacy._current_modal_dispatch_running_counts()
    if len(running_counts) == 4:
        (
            running_by_stage,
            running_by_stage_platform,
            running_by_run,
            running_by_run_stage_platform,
        ) = running_counts
        active_accounts_by_stage_platform: dict[tuple[str, str], set[str]] = {}
    else:
        (
            running_by_stage,
            running_by_stage_platform,
            running_by_run,
            running_by_run_stage_platform,
            active_accounts_by_stage_platform,
        ) = running_counts
    priority_recovery_override_slots = legacy._modal_comment_recovery_priority_override_slots()
    priority_recovery_running_count = (
        legacy._current_modal_priority_comment_recovery_running_count() if priority_recovery_override_slots > 0 else 0
    )
    dispatched_job_ids: list[str] = []
    dispatch_attempts = 0
    # REVISED §4: per-run active-worker cap for Instagram public comments runs.
    # Cached run worker caps keyed by run_id so a multi-shard run is queried once.
    worker_cap_by_run: dict[str, int | None] = {}

    for job in candidates:
        if len(dispatched_job_ids) >= safe_limit:
            break
        stage = legacy._job_stage_from_row(job)
        capacity_stage = legacy._modal_dispatch_capacity_stage(stage)
        platform = legacy._normalize_platform_name(job.get("platform")) or "unknown"
        status = str(job.get("status") or "").strip().lower()
        job_run_id = str(job.get("run_id") or "").strip()
        if job_run_id and legacy._run_pause_after_current_requested(job_run_id):
            continue
        job_config = legacy._metadata_dict(job.get("config"))
        job_dispatch = legacy._job_dispatch_metadata(job)
        job_ingest_mode = legacy._resolve_pipeline_ingest_mode(job_config.get("pipeline_ingest_mode"))
        priority_recovery_job = legacy._job_is_priority_comment_recovery(job, job_config, stage=stage)
        priority_recovery_override_allowed = (
            priority_recovery_job and priority_recovery_running_count < priority_recovery_override_slots
        )
        if legacy._job_blocked_by_posts_auth_cooldown(job) is not None:
            continue
        if legacy._job_required_lane_blocks_modal_dispatch(job_config, platform=platform):
            continue
        existing_remote_invocation_id = str(job_dispatch.get("remote_invocation_id") or "").strip()
        terminal_remote_invocation = False
        if existing_remote_invocation_id:
            refreshed_lease_expires_at = legacy._now_utc() + timedelta(
                seconds=legacy._resolve_stale_seconds_for_job(
                    platform=platform,
                    stage=stage,
                    has_queue_fields=True,
                )
            )
            inspection = legacy._refresh_remote_modal_invocation_state(
                job,
                lease_expires_at=refreshed_lease_expires_at,
            )
            inspection_status = str(inspection.get("status") or "").strip().lower()
            if legacy._modal_invocation_is_nonterminal(inspection_status):
                if (
                    inspection_status == "pending"
                    and status in {"queued", "pending", "retrying"}
                    and legacy._modal_pending_capacity_invocation_is_stale(job_dispatch, inspection)
                ):
                    job_id_for_clear = str(job.get("id") or "").strip()
                    if not job_id_for_clear:
                        continue
                    job_dispatch = legacy._clear_stale_pending_modal_dispatch(job_id_for_clear, job_dispatch)
                    existing_remote_invocation_id = ""
                else:
                    if priority_recovery_job:
                        priority_recovery_running_count += 1
                    running_by_stage[capacity_stage] = running_by_stage.get(capacity_stage, 0) + 1
                    running_by_stage_platform[(capacity_stage, platform)] = (
                        running_by_stage_platform.get((capacity_stage, platform), 0) + 1
                    )
                    nonterminal_account = legacy._resolve_dispatch_account_handle(job_config)
                    if nonterminal_account:
                        active_accounts_by_stage_platform.setdefault((capacity_stage, platform), set()).add(
                            nonterminal_account
                        )
                    if job_run_id:
                        running_by_run[job_run_id] = running_by_run.get(job_run_id, 0) + 1
                        if job_ingest_mode == legacy.SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE:
                            key = (job_run_id, capacity_stage, platform)
                            running_by_run_stage_platform[key] = running_by_run_stage_platform.get(key, 0) + 1
                    continue
            if existing_remote_invocation_id and inspection_status == "unknown":
                job_id_for_clear = str(job.get("id") or "").strip()
                if status in {"queued", "pending", "retrying"} and job_id_for_clear:
                    legacy._touch_job_dispatch_metadata(
                        job_id_for_clear,
                        dispatch_backend=str(job_dispatch.get("dispatch_backend") or "modal"),
                        dispatch_requested_at=legacy._coerce_dt(job_dispatch.get("dispatch_requested_at")),
                        dispatch_attempt_count=legacy._dispatch_metadata_attempt_count(job_dispatch),
                        remote_invocation_id=None,
                        lease_expires_at=None,
                        remote_invocation_status="unknown",
                        remote_invocation_checked_at=legacy._now_utc(),
                        remote_task_id=None,
                        remote_pending_since=None,
                        remote_blocked_reason=None,
                    )
                    job_dispatch = {
                        **job_dispatch,
                        "remote_invocation_id": None,
                        "lease_expires_at": None,
                        "remote_invocation_status": "unknown",
                    }
                else:
                    continue
            elif existing_remote_invocation_id:
                terminal_remote_invocation = True
                job_dispatch = {
                    **job_dispatch,
                    "remote_invocation_status": inspection_status,
                    "remote_invocation_checked_at": inspection.get("checked_at"),
                    "remote_blocked_reason": str(inspection.get("reason") or "").strip() or None,
                }
        if not terminal_remote_invocation and legacy._dispatch_request_is_fresh(job):
            continue
        if (
            running_by_stage.get(capacity_stage, 0) >= legacy._modal_dispatch_stage_global_cap(stage)
            and not priority_recovery_override_allowed
        ):
            continue
        candidate_account = legacy._resolve_dispatch_account_handle(job_config)
        sp_key = (capacity_stage, platform)
        existing_accounts = active_accounts_by_stage_platform.get(sp_key, set())
        if (
            candidate_account
            and capacity_stage in {legacy.SHARED_ACCOUNT_DISCOVERY_STAGE, legacy.SHARED_ACCOUNT_POSTS_STAGE}
            and candidate_account in existing_accounts
            and not legacy._job_allows_parallel_same_account_dispatch(job_config)
        ):
            continue
        prospective_accounts = existing_accounts | {candidate_account} if candidate_account else existing_accounts
        effective_cap = legacy._modal_dispatch_effective_platform_cap(
            stage,
            platform,
            active_account_count=len(prospective_accounts) if prospective_accounts else 1,
            job_config=job_config,
        )
        if (
            effective_cap is not None
            and running_by_stage_platform.get(sp_key, 0) >= effective_cap
            and not priority_recovery_override_allowed
        ):
            continue
        if job_ingest_mode == legacy.SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE and job_run_id:
            if running_by_run.get(job_run_id, 0) >= legacy._resolve_catalog_run_in_flight_cap():
                continue
            if capacity_stage == legacy.SHARED_ACCOUNT_POSTS_STAGE:
                platform_run_cap = legacy._modal_dispatch_effective_platform_cap(
                    stage,
                    platform,
                    active_account_count=1,
                    job_config=job_config,
                )
                if (
                    platform_run_cap is not None
                    and running_by_run_stage_platform.get((job_run_id, stage, platform), 0) >= platform_run_cap
                ):
                    continue

        # REVISED §4: Instagram public comments runs decouple active workers from
        # the (batch-size-10) shard count. Claim only up to
        # comments_worker_cap_current - active_running_or_pending_jobs for the run.
        # This is an *additional* bound layered on top of the stage/platform caps
        # above and applies only to runs whose config carries a worker cap (i.e.
        # public comments runs launched with §4 enabled); all other dispatch is
        # unchanged.
        if job_run_id and stage == legacy.INSTAGRAM_COMMENTS_SCRAPLING_STAGE and platform == "instagram":
            run_worker_cap = _instagram_public_comments_worker_cap(job_run_id, worker_cap_by_run)
            if run_worker_cap is not None and running_by_run.get(job_run_id, 0) >= run_worker_cap:
                continue

        job_id = str(job.get("id") or "").strip()
        if not job_id:
            continue
        dispatch_attempts += 1
        dispatch_attempt_count = legacy._dispatch_metadata_attempt_count(job_dispatch) + 1
        blocked_failure_count = legacy._dispatch_metadata_blocked_failure_count(job_dispatch)
        blocked_first_seen_at = legacy._coerce_dt(job_dispatch.get("dispatch_blocked_first_seen_at"))
        lease_expires_at = legacy._now_utc() + timedelta(
            seconds=legacy._resolve_stale_seconds_for_job(
                platform=platform,
                stage=stage,
                has_queue_fields=True,
            )
        )
        legacy._touch_job_dispatch_metadata(
            job_id,
            dispatch_backend="modal",
            dispatch_requested_at=legacy._now_utc(),
            dispatch_attempt_count=dispatch_attempt_count,
            lease_expires_at=lease_expires_at,
            last_dispatch_error=None,
            last_dispatch_error_code=None,
            last_dispatch_error_at=None,
            remote_invocation_status=None,
            remote_invocation_checked_at=None,
            remote_task_id=None,
            remote_pending_since=None,
            remote_blocked_reason=None,
            dispatch_blocked_failure_count=0,
            dispatch_blocked_first_seen_at=None,
        )
        dispatch_kwargs: dict[str, Any] = {"job_id": job_id, "stage": stage}
        if priority_recovery_override_allowed:
            dispatch_kwargs["priority_recovery"] = True
        dispatch_result = legacy.dispatch_social_job(**dispatch_kwargs)
        if dispatch_result.get("dispatched"):
            legacy._touch_job_dispatch_metadata(
                job_id,
                dispatch_backend="modal",
                dispatch_requested_at=legacy._now_utc(),
                dispatch_attempt_count=dispatch_attempt_count,
                remote_invocation_id=str(dispatch_result.get("call_id") or "").strip() or None,
                remote_function_name=str(dispatch_result.get("function_name") or "").strip() or None,
                lease_expires_at=lease_expires_at,
                last_dispatch_error=None,
                last_dispatch_error_code=None,
                last_dispatch_error_at=None,
                remote_invocation_status="unknown",
                remote_invocation_checked_at=None,
                remote_task_id=None,
                remote_pending_since=None,
                remote_blocked_reason=None,
                dispatch_blocked_failure_count=0,
                dispatch_blocked_first_seen_at=None,
            )
            dispatched_job_ids.append(job_id)
            running_by_stage[capacity_stage] = running_by_stage.get(capacity_stage, 0) + 1
            running_by_stage_platform[(capacity_stage, platform)] = (
                running_by_stage_platform.get((capacity_stage, platform), 0) + 1
            )
            if candidate_account:
                active_accounts_by_stage_platform.setdefault((capacity_stage, platform), set()).add(candidate_account)
            if job_run_id:
                running_by_run[job_run_id] = running_by_run.get(job_run_id, 0) + 1
                if job_ingest_mode == legacy.SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE:
                    key = (job_run_id, capacity_stage, platform)
                    running_by_run_stage_platform[key] = running_by_run_stage_platform.get(key, 0) + 1
            if priority_recovery_override_allowed:
                priority_recovery_running_count += 1
        else:
            dispatch_reason = str(dispatch_result.get("reason") or "dispatch_failed")
            dispatch_reason_code = str(dispatch_result.get("reason_code") or "").strip().lower() or (
                legacy._normalize_dispatch_blocked_reason(
                    last_dispatch_error_code="modal_dispatch_failed",
                    last_dispatch_error=dispatch_reason,
                )
            )
            blocked_failure_count = max(1, blocked_failure_count + 1)
            blocked_first_seen_at = blocked_first_seen_at or legacy._now_utc()
            blocked_for_seconds = max(0, int((legacy._now_utc() - blocked_first_seen_at).total_seconds()))
            legacy._touch_job_dispatch_metadata(
                job_id,
                dispatch_backend="modal",
                dispatch_requested_at=legacy._now_utc(),
                dispatch_attempt_count=dispatch_attempt_count,
                lease_expires_at=None,
                last_dispatch_error=dispatch_reason,
                last_dispatch_error_code="modal_dispatch_failed",
                last_dispatch_error_at=legacy._now_utc(),
                remote_invocation_status="unknown",
                remote_invocation_checked_at=legacy._now_utc(),
                remote_task_id=None,
                remote_pending_since=None,
                remote_blocked_reason=dispatch_reason_code,
                dispatch_blocked_failure_count=blocked_failure_count,
                dispatch_blocked_first_seen_at=blocked_first_seen_at,
            )
            if (
                blocked_failure_count >= legacy._resolve_modal_dispatch_blocked_failure_limit()
                or blocked_for_seconds >= legacy._resolve_modal_dispatch_no_progress_seconds()
            ):
                legacy._finish_job(
                    job_id,
                    status="failed",
                    items_found=legacy._normalize_non_negative_int(job.get("items_found")),
                    error_message=dispatch_reason,
                    metadata={
                        "dispatch": {
                            **job_dispatch,
                            "dispatch_backend": "modal",
                            "dispatch_requested_at": legacy._iso(legacy._now_utc()),
                            "dispatch_attempt_count": dispatch_attempt_count,
                            "last_dispatch_error": dispatch_reason,
                            "last_dispatch_error_code": "modal_dispatch_failed",
                            "last_dispatch_error_at": legacy._iso(legacy._now_utc()),
                            "remote_invocation_status": "failed",
                            "remote_invocation_checked_at": legacy._iso(legacy._now_utc()),
                            "remote_blocked_reason": dispatch_reason_code,
                            "dispatch_blocked_failure_count": blocked_failure_count,
                            "dispatch_blocked_first_seen_at": legacy._iso(blocked_first_seen_at),
                            "dispatch_blocked_terminalized_at": legacy._iso(legacy._now_utc()),
                        }
                    },
                    last_error_code="modal_dispatch_blocked",
                    last_error_class="ModalDispatchBlocked",
                )
                if job_run_id:
                    legacy._finalize_run_status(job_run_id, force_recompute=True)

    heartbeat_updates: dict[str, Any] = {
        "dispatch_enabled": True,
        "active_invocations": sum(running_by_stage.values()),
    }
    if dispatched_job_ids:
        heartbeat_updates.update(
            {
                "last_dispatch_success_at": legacy._iso(legacy._now_utc()),
                "last_dispatch_error": None,
                "last_dispatch_error_code": None,
                "last_dispatch_blocked_reason": None,
            }
        )
    legacy._touch_modal_social_dispatcher_heartbeat(metadata_updates=heartbeat_updates)
    return {
        "dispatched_job_ids": dispatched_job_ids,
        "dispatch_attempts": dispatch_attempts,
        "recovered_unclaimed_job_ids": [
            str(row.get("id") or "").strip() for row in recovered_unclaimed if str(row.get("id") or "").strip()
        ],
        "recovered_dispatch_blocked_job_ids": [
            str(row.get("id") or "").strip() for row in recovered_blocked if str(row.get("id") or "").strip()
        ],
        "recovered_capacity_job_ids": [
            str(row.get("id") or "").strip() for row in recovered_capacity if str(row.get("id") or "").strip()
        ],
        "reason": None,
    }


def _mark_claimed_runs_running(jobs: list[dict[str, Any]]) -> None:
    run_ids = sorted({str(job.get("run_id") or "").strip() for job in jobs if str(job.get("run_id") or "").strip()})
    for run_id in run_ids:
        legacy._set_run_status(run_id, "running")


def claim_and_process_social_job(*, job_id: str, worker_id: str) -> dict[str, Any]:
    claimed = legacy._claim_job_by_id(job_id=job_id, worker_id=worker_id)
    if not claimed:
        return {
            "job_id": job_id,
            "claimed": False,
            "job": legacy._load_current_job_row(job_id),
        }
    _mark_claimed_runs_running([claimed])
    legacy._mark_claimed_modal_dispatch_running(claimed)
    result = _call_extracted_override("process_claimed_job", process_claimed_job, claimed, worker_id=worker_id)
    run_id = str(claimed.get("run_id") or "").strip() or None
    _call_extracted_override("dispatch_due_social_jobs", dispatch_due_social_jobs, run_id=run_id, limit=1)
    return {
        "job_id": job_id,
        "claimed": True,
        "job": result,
    }


def recover_and_dispatch_due_social_jobs(*, limit: int | None = None) -> dict[str, Any]:
    safe_limit = max(1, min(int(limit or legacy._modal_dispatch_limit()), 250))
    terminal_reconciled = reconcile_terminal_modal_running_jobs(limit=safe_limit)
    recovered = legacy.recover_stale_running_jobs(limit=safe_limit)
    recovered_unclaimed = _call_extracted_override(
        "recover_stale_unclaimed_dispatched_jobs",
        recover_stale_unclaimed_dispatched_jobs,
        limit=safe_limit,
    )
    recovered_capacity: list[dict[str, Any]] = []
    stale_followup_claims: dict[str, Any] = {"scanned": 0, "refinalized": 0, "failed": 0}
    try:
        from trr_backend.socials.control_plane import run_lifecycle

        stale_followup_claims = run_lifecycle.recover_stale_deferred_comments_followup_claims(limit=max(safe_limit, 25))
    except Exception as exc:  # noqa: BLE001
        legacy.logger.warning("[recover_and_dispatch] stale deferred-followup claim sweep skipped: %s", exc)
    # bug-1: self-heal retryable deferred-comments-followup failures (gated off by
    # default). Guarded so a failure here can never break the dispatch sweep.
    followup_retry: dict[str, Any] = {"enabled": False}
    try:
        from trr_backend.socials.control_plane import run_lifecycle

        followup_retry = run_lifecycle.recover_failed_deferred_comments_followups(limit=max(safe_limit, 25))
    except Exception as exc:  # noqa: BLE001
        legacy.logger.warning("[recover_and_dispatch] deferred followup retry sweep skipped: %s", exc)
    # B3: re-finalize runs left non-terminal by a deferred/raised _finish_job finalize.
    # Guarded so a failure here can never break the dispatch sweep.
    unfinalized_recovery: dict[str, Any] = {"scanned": 0, "finalized": 0}
    try:
        from trr_backend.socials.control_plane import run_lifecycle

        unfinalized_recovery = run_lifecycle.recover_unfinalized_terminal_runs(limit=max(safe_limit, 25))
    except Exception as exc:  # noqa: BLE001
        legacy.logger.warning("[recover_and_dispatch] unfinalized-terminal-run sweep skipped: %s", exc)
    deadline_recovery: dict[str, Any] = {"scanned": 0, "expired_runs": 0, "failed_jobs": 0, "finalized_runs": 0}
    try:
        from trr_backend.socials.control_plane import run_lifecycle

        deadline_recovery = run_lifecycle.recover_catalog_run_deadline_exceeded_jobs(limit=max(safe_limit, 25))
    except Exception as exc:  # noqa: BLE001
        legacy.logger.warning("[recover_and_dispatch] catalog-run-deadline sweep skipped: %s", exc)
    child_cancellation_recovery: dict[str, Any] = {
        "scanned": 0,
        "claimed": 0,
        "cancelled": 0,
        "not_found": 0,
        "retryable": 0,
        "skipped": 0,
    }
    try:
        from trr_backend.socials.control_plane import run_lifecycle

        child_cancellation_recovery = run_lifecycle.recover_deferred_comments_child_cancellations(
            limit=max(safe_limit, 25)
        )
    except Exception as exc:  # noqa: BLE001
        legacy.logger.warning("[recover_and_dispatch] deferred child-cancellation sweep skipped: %s", exc)
    # The sweep is the other entrypoint that must stay on the cap-aware owner;
    # keep legacy overrides for the remaining extracted helpers only.
    dispatch_summary = dispatch_due_social_jobs(limit=limit)
    return {
        "reconciled_terminal_jobs": [
            str(row.get("id") or "").strip() for row in terminal_reconciled if str(row.get("id") or "").strip()
        ],
        "recovered_jobs": [str(row.get("id") or "").strip() for row in recovered],
        "recovered_unclaimed_jobs": [
            str(row.get("id") or "").strip() for row in recovered_unclaimed if str(row.get("id") or "").strip()
        ],
        "recovered_capacity_jobs": [
            str(row.get("id") or "").strip() for row in recovered_capacity if str(row.get("id") or "").strip()
        ],
        "stale_deferred_comments_followup_claims": stale_followup_claims,
        "deferred_comments_followup_retry": followup_retry,
        "unfinalized_terminal_runs": unfinalized_recovery,
        "catalog_run_deadline_sweep": deadline_recovery,
        "deferred_comments_child_cancellation": child_cancellation_recovery,
        **dispatch_summary,
    }


def claim_next_queued_jobs(
    *,
    worker_id: str,
    stage: str | None = None,
    platform: str | None = None,
    run_id: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    normalized_platform = legacy._normalize_platform_name(platform) or None
    safe_limit = legacy._resolve_job_claim_batch_size(limit, stage=stage)
    claimed_jobs = legacy._claim_next_jobs(
        worker_id=worker_id,
        run_id=run_id,
        stage=stage,
        platform=normalized_platform,
        limit=safe_limit,
    )
    _mark_claimed_runs_running(claimed_jobs)
    return claimed_jobs


def process_claimed_job(job: dict[str, Any], *, worker_id: str | None = None) -> dict[str, Any]:
    return legacy._execute_claimed_job(job, worker_id=worker_id)
