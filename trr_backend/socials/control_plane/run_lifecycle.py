"""Run lifecycle mutation entrypoints for the social control plane."""

from __future__ import annotations

import importlib
from datetime import datetime
from types import ModuleType
from typing import Any
from uuid import UUID

from psycopg2 import InterfaceError, OperationalError
from psycopg2.pool import PoolError

SOCIAL_CONTROL_POOL_NAME = "social_control"
_SCRAPE_RUN_ALLOWED_STATUSES = {"queued", "running", "completed", "failed", "cancelled"}
JobProgressState = dict[str, Any]


def _legacy_module() -> ModuleType:
    return importlib.import_module("trr_backend.socials.social_season_analytics_impl")


class _LegacyModuleProxy:
    def __getattr__(self, name: str) -> Any:
        return getattr(_legacy_module(), name)


legacy = _LegacyModuleProxy()


def _normalize_scrape_run_status(status: str) -> str:
    normalized = str(status or "").strip().lower()
    if normalized in _SCRAPE_RUN_ALLOWED_STATUSES:
        return normalized
    if normalized in {"pending", "retrying"}:
        return "queued"
    return normalized


def _call_with_optional_conn(
    loader,
    /,
    *args: Any,
    conn: Any | None = None,
    **kwargs: Any,
) -> Any:
    if conn is None:
        return loader(*args, **kwargs)
    try:
        return loader(*args, conn=conn, **kwargs)
    except TypeError as exc:
        if "unexpected keyword argument 'conn'" not in str(exc):
            raise
        return loader(*args, **kwargs)


def new_job_progress_state() -> JobProgressState:
    """Create the mutable progress watermark carried by platform job runners."""
    return legacy._new_job_progress_state()


def now_utc() -> datetime:
    return legacy._now_utc()


def format_time(dt: datetime | None) -> str | None:
    return legacy._iso(dt)


def metadata_dict(value: Any) -> dict[str, Any]:
    return legacy._metadata_dict(value)


def touch_job_heartbeat(job_id: str, *, worker_id: str | None = None) -> bool:
    return bool(legacy._touch_job_heartbeat(job_id, worker_id=worker_id))


def emit_job_progress(
    *,
    job_id: str,
    stage: str,
    platform: str,
    account: str,
    scraped_posts: int,
    scraped_comments: int,
    posts_upserted: int,
    comments_upserted: int,
    activity: dict[str, Any] | None,
    progress_state: JobProgressState,
    worker_id: str | None = None,
    force: bool = False,
    extra_metadata: dict[str, Any] | None = None,
) -> bool:
    kwargs: dict[str, Any] = {
        "job_id": job_id,
        "stage": stage,
        "platform": platform,
        "account": account,
        "scraped_posts": scraped_posts,
        "scraped_comments": scraped_comments,
        "posts_upserted": posts_upserted,
        "comments_upserted": comments_upserted,
        "activity": activity,
        "progress_state": progress_state,
        "force": force,
    }
    if worker_id is not None:
        kwargs["worker_id"] = worker_id
    if extra_metadata is not None:
        kwargs["extra_metadata"] = extra_metadata
    return bool(legacy._emit_job_progress(**kwargs))


def finish_job(
    job_id: str,
    *,
    status: str,
    items_found: int,
    error_message: str | None = None,
    metadata: dict[str, Any] | None = None,
    last_error_code: str | None = None,
    last_error_class: str | None = None,
    next_available_at: datetime | None = None,
    expected_worker_id: str | None = None,
) -> None:
    kwargs: dict[str, Any] = {"status": status, "items_found": items_found}
    if error_message is not None:
        kwargs["error_message"] = error_message
    if metadata is not None:
        kwargs["metadata"] = metadata
    if last_error_code is not None:
        kwargs["last_error_code"] = last_error_code
    if last_error_class is not None:
        kwargs["last_error_class"] = last_error_class
    if next_available_at is not None:
        kwargs["next_available_at"] = next_available_at
    if expected_worker_id is not None:
        kwargs["expected_worker_id"] = expected_worker_id
    legacy._finish_job(job_id, **kwargs)


def retry_backoff_seconds(attempt_count: int) -> int:
    return int(legacy._retry_backoff_seconds(attempt_count))


def _create_run(
    context: legacy.SeasonContext | None,
    *,
    source_scope: str,
    initiated_by: str | None,
    config: dict[str, Any],
    status: str,
    conn: Any | None = None,
) -> str:
    status = _normalize_scrape_run_status(status)
    initial_summary = _build_run_summary_payload(
        total_jobs=0,
        completed_jobs=0,
        failed_jobs=0,
        active_jobs=0,
        items_found_total=0,
        stage_counts={},
    )
    row = _call_with_optional_conn(
        legacy.pg.fetch_one,
        """
        insert into social.scrape_runs (
          season_id,
          show_id,
          source_scope,
          status,
          initiated_by,
          config,
          summary,
          started_at
        )
        values (
          %s,
          %s,
          %s,
          %s,
          %s,
          %s::jsonb,
          %s::jsonb,
          case when %s = 'running' then now() else null end
        )
        returning id::text
        """,
        [
            context.season_id if context is not None else None,
            context.show_id if context is not None else None,
            source_scope,
            status,
            initiated_by,
            legacy.json.dumps(config),
            legacy.json.dumps(initial_summary),
            status,
        ],
        conn=conn,
    )
    if not row:
        raise RuntimeError("Failed to create social scrape run")
    run_id = str(row["id"])
    if legacy._column_exists("social", "scrape_runs", "sync_session_id"):
        sync_session_id = str(config.get("sync_session_id") or "").strip() or None
        pass_kind = str(config.get("pass_kind") or "").strip() or None
        pass_attempt = legacy._normalize_non_negative_int(config.get("pass_attempt")) or None
        pass_sequence = legacy._normalize_non_negative_int(config.get("pass_sequence")) or None
        if sync_session_id or pass_kind or pass_attempt is not None or pass_sequence is not None:
            _call_with_optional_conn(
                legacy.pg.fetch_one,
                """
                update social.scrape_runs
                set
                  sync_session_id = %s::uuid,
                  pass_kind = %s,
                  pass_attempt = %s,
                  pass_sequence = %s
                where id = %s::uuid
                returning id::text
                """,
                [sync_session_id, pass_kind, pass_attempt, pass_sequence, run_id],
                conn=conn,
            )
    return run_id


def _set_run_status(run_id: str, status: str, *, conn: Any | None = None) -> None:
    status = _normalize_scrape_run_status(status)
    _call_with_optional_conn(
        legacy.pg.fetch_one,
        """
        update social.scrape_runs
        set
          status = %s,
          started_at = case
            when %s = 'running' then coalesce(started_at, now())
            else started_at
          end,
          completed_at = case
            when %s in ('queued', 'pending', 'retrying', 'running') then null
            when %s in ('completed', 'failed', 'cancelled') then coalesce(completed_at, now())
            else completed_at
          end,
          cancelled_at = case
            when %s in ('queued', 'pending', 'retrying', 'running') then null
            when %s = 'cancelled' then coalesce(cancelled_at, now())
            else cancelled_at
          end
        where id = %s
        returning id::text
        """,
        [status, status, status, status, status, status, run_id],
        conn=conn,
    )
    legacy._invalidate_queue_status_cache()
    if status in {"completed", "failed", "cancelled"}:
        legacy._invalidate_week_detail_cache_after_run_terminal_status()


def _merge_run_config(
    run_id: str,
    *,
    config_updates: dict[str, Any],
    conn: Any | None = None,
) -> dict[str, Any]:
    row = (
        _call_with_optional_conn(
            legacy.pg.fetch_one,
            """
            update social.scrape_runs
            set config = coalesce(config, '{}'::jsonb) || %s::jsonb
            where id = %s::uuid
            returning config
            """,
            [legacy._json_dumps(legacy._metadata_dict(config_updates)), run_id],
            conn=conn,
        )
        or {}
    )
    legacy._invalidate_queue_status_cache()
    return legacy._metadata_dict(row.get("config"))


def _deferred_comments_followup_retryable_reason(exc: BaseException) -> str | None:
    message = str(exc or "").strip().lower()
    if "must appear in the group by clause" in message and ("p.*" in message or "fb_comment_count" in message):
        return "sql_grouping_contract"
    if isinstance(exc, (InterfaceError, OperationalError, PoolError)):
        return "database_retryable"
    if "connection pool" in message or "server closed the connection" in message:
        return "database_retryable"
    return None


def _maybe_start_deferred_comments_followup(
    *,
    run_id: str,
    run_status: str,
    run_config: dict[str, Any],
    summary: dict[str, Any],
    conn: Any | None = None,
) -> None:
    if str(run_status or "").strip().lower() != "completed":
        return
    if not legacy._shared_account_catalog_scrape_complete(run_config=run_config, summary=summary, conn=conn):
        return
    followup = legacy._metadata_dict(run_config.get("deferred_comments_followup"))
    if str(followup.get("state") or "").strip().lower() != "pending":
        return
    if str(followup.get("platform") or "").strip().lower() != "instagram":
        return

    attached_followups = legacy._normalize_attached_followups(run_config.get("attached_followups"))
    now_iso = legacy._iso(legacy._now_utc())
    try:
        comments_source = "deferred_after_catalog"
        try:
            comments_result = legacy.start_social_account_comments_scrape(
                str(followup.get("platform") or "").strip(),
                str(followup.get("account_handle") or "").strip(),
                mode="profile",
                source_scope=str(followup.get("source_scope") or "network"),
                max_posts=None,
                max_comments_per_post=None,
                refresh_policy=str(followup.get("refresh_policy") or "all_saved_posts"),
                target_filter=str(followup.get("target_filter") or "").strip() or None,
                initiated_by="catalog_completion_followup",
                allow_local_dev_inline_bypass=bool(followup.get("allow_local_dev_inline_bypass")),
                comments_enable_media_followups=bool(followup.get("comments_enable_media_followups")),
                launch_group_id=str(followup.get("launch_group_id") or "").strip() or None,
            )
        except legacy.SocialIngestConflictError as exc:
            if exc.code != "SOCIAL_ACCOUNT_COMMENTS_RUN_ALREADY_ACTIVE":
                raise
            comments_result = {
                "run_id": str(exc.detail.get("run_id") or "").strip() or None,
                "status": str(exc.detail.get("status") or "running").strip().lower() or "running",
            }
            comments_source = "reused_run"
        _merge_run_config(
            run_id,
            config_updates={
                "attached_followups": {
                    **attached_followups,
                    "comments": legacy._build_attached_comments_followup(
                        run_id=str((comments_result or {}).get("run_id") or "").strip() or None,
                        status=str((comments_result or {}).get("status") or "").strip().lower() or "pending",
                        source=comments_source,
                    ),
                },
                "deferred_comments_followup": {
                    **followup,
                    "state": "started",
                    "started_at": now_iso,
                    "comments_run_id": str((comments_result or {}).get("run_id") or "").strip() or None,
                    "runtime_version": legacy._metadata_dict((comments_result or {}).get("runtime_version"))
                    or legacy._metadata_dict(followup.get("runtime_version"))
                    or dict(legacy._resolve_runtime_version_stamp()),
                    "created_by_runtime_version": legacy._metadata_dict(
                        (comments_result or {}).get("created_by_runtime_version")
                    )
                    or legacy._metadata_dict(followup.get("created_by_runtime_version"))
                    or dict(legacy._resolve_runtime_version_stamp()),
                },
            },
            conn=conn,
        )
    except Exception as exc:  # noqa: BLE001
        error_message = str(exc)
        retryable_reason = _deferred_comments_followup_retryable_reason(exc)
        retryable = retryable_reason is not None
        prior_failures = [dict(item) for item in list(followup.get("failure_history") or []) if isinstance(item, dict)]
        prior_failures.append(
            {
                "failed_at": now_iso,
                "error_message": error_message,
                "retryable": retryable,
                "retryable_reason": retryable_reason,
            }
        )
        _merge_run_config(
            run_id,
            config_updates={
                "attached_followups": {
                    **attached_followups,
                    "comments": legacy._build_attached_comments_followup(
                        run_id=str(followup.get("comments_run_id") or "").strip() or None,
                        status="failed",
                        source="deferred_after_catalog",
                        state="failed",
                        error_message=error_message,
                        failed_at=now_iso,
                        retryable=retryable,
                    ),
                },
                "deferred_comments_followup": {
                    **followup,
                    "state": "failed",
                    "failed_at": now_iso,
                    "error_message": error_message,
                    "retryable": retryable,
                    "retryable_reason": retryable_reason,
                    "failure_history": prior_failures[-5:],
                },
            },
            conn=conn,
        )
        legacy.logger.exception(
            "Failed to auto-start deferred Instagram comments followup after run finalization: run=%s",
            run_id,
        )


def _status_is_active(status: str | None) -> bool:
    return str(status or "").strip().lower() in {"queued", "pending", "retrying", "running"}


def _status_is_completed(status: str | None) -> bool:
    return str(status or "").strip().lower() == "completed"


def _status_is_failed(status: str | None) -> bool:
    return str(status or "").strip().lower() == "failed"


def _normalize_stage_counts(stage_counts: Any) -> dict[str, dict[str, int]]:
    if not isinstance(stage_counts, dict):
        return {}
    normalized: dict[str, dict[str, int]] = {}
    for stage, counters in stage_counts.items():
        stage_key = str(stage or "").strip() or "unknown"
        counter_map = dict(counters) if isinstance(counters, dict) else {}
        normalized[stage_key] = {
            "total": legacy._normalize_non_negative_int(counter_map.get("total")),
            "completed": legacy._normalize_non_negative_int(counter_map.get("completed")),
            "failed": legacy._normalize_non_negative_int(counter_map.get("failed")),
            "active": legacy._normalize_non_negative_int(counter_map.get("active")),
        }
    return normalized


# Audit fields written to ``social.scrape_runs.summary`` by cancellation and the
# guarded-restart flow (REVISED §6). A summary recompute rebuilds only the job
# count rollups, so these must be carried forward from the existing summary or
# run history loses its cancellation / restart provenance.
_PROTECTED_RUN_SUMMARY_FIELDS = (
    "cancelled_by",
    "cancel_requested_at",
    "cancel_reason",
    "guarded_restart",
    "guarded_restart_from_run_id",
    "guarded_restart_to_run_id",
    "public_blocked_pause",
    "dispatch_control",
)


def _preserve_protected_run_summary_fields(
    summary: dict[str, Any],
    existing_summary: Any,
) -> dict[str, Any]:
    """Carry protected audit fields from an existing summary into a recompute.

    Only copies a protected key when it is present (and not ``None``) on the
    existing summary, so a recompute refreshes count fields without clobbering
    cancellation / guarded-restart provenance. Returns ``summary`` mutated in
    place for caller convenience.
    """
    existing = legacy._metadata_dict(existing_summary)
    if not existing:
        return summary
    for field in _PROTECTED_RUN_SUMMARY_FIELDS:
        if field in summary:
            continue
        value = existing.get(field)
        if value is None and field not in existing:
            continue
        if value is None:
            continue
        summary[field] = value
    return summary


def _build_run_summary_payload(
    *,
    total_jobs: Any,
    completed_jobs: Any,
    failed_jobs: Any,
    active_jobs: Any,
    items_found_total: Any,
    stage_counts: Any,
) -> dict[str, Any]:
    return {
        "total_jobs": legacy._normalize_non_negative_int(total_jobs),
        "completed_jobs": legacy._normalize_non_negative_int(completed_jobs),
        "failed_jobs": legacy._normalize_non_negative_int(failed_jobs),
        "active_jobs": legacy._normalize_non_negative_int(active_jobs),
        "items_found_total": legacy._normalize_non_negative_int(items_found_total),
        "stage_counts": _normalize_stage_counts(stage_counts),
    }


def _persist_run_counters_and_summary(
    *,
    conn: Any,
    run_id: str,
    total_jobs: int,
    completed_jobs: int,
    failed_jobs: int,
    active_jobs: int,
    items_found_total: int,
    stage_counts: dict[str, dict[str, int]],
) -> dict[str, Any]:
    summary = _build_run_summary_payload(
        total_jobs=total_jobs,
        completed_jobs=completed_jobs,
        failed_jobs=failed_jobs,
        active_jobs=active_jobs,
        items_found_total=items_found_total,
        stage_counts=stage_counts,
    )
    try:
        UUID(str(run_id))
    except (ValueError, TypeError, AttributeError):
        return summary
    with legacy.pg.db_cursor(conn=conn) as cur:
        existing_row = (
            legacy.pg.fetch_one_with_cursor(
                cur,
                "select summary from social.scrape_runs where id = %s",
                [run_id],
            )
            or {}
        )
        _preserve_protected_run_summary_fields(summary, existing_row.get("summary"))
        legacy.pg.fetch_one_with_cursor(
            cur,
            """
            update social.scrape_runs
            set
              total_jobs = %s,
              completed_jobs = %s,
              failed_jobs = %s,
              active_jobs = %s,
              items_found_total = %s,
              stage_counts = %s::jsonb,
              summary = %s::jsonb
            where id = %s
            returning id::text
            """,
            [
                int(total_jobs),
                int(completed_jobs),
                int(failed_jobs),
                int(active_jobs),
                int(items_found_total),
                legacy.json.dumps(stage_counts),
                legacy.json.dumps(summary),
                run_id,
            ],
        )
    return summary


def _increment_stage_counter(
    stage_counts: dict[str, dict[str, int]],
    *,
    stage: str,
    key: str,
    delta: int,
) -> dict[str, dict[str, int]]:
    if not delta:
        return stage_counts
    bucket = dict(stage_counts.get(stage) or {"total": 0, "completed": 0, "failed": 0, "active": 0})
    bucket[key] = max(0, legacy._normalize_non_negative_int(bucket.get(key)) + int(delta))
    stage_counts[stage] = bucket
    return stage_counts


def _increment_run_counters_on_job_create(
    *,
    run_id: str,
    stage: str,
    status: str,
    conn: Any | None = None,
) -> None:
    if not run_id or not legacy._run_counter_columns_ready():
        return
    stage_key = str(stage or "unknown").strip() or "unknown"
    if conn is not None:
        with legacy.pg.db_cursor(conn=conn) as cur:
            row = (
                legacy.pg.fetch_one_with_cursor(
                    cur,
                    """
                select
                  total_jobs,
                  completed_jobs,
                  failed_jobs,
                  active_jobs,
                  items_found_total,
                  stage_counts
                from social.scrape_runs
                where id = %s
                for update
                """,
                    [run_id],
                )
                or {}
            )
            _persist_incremented_run_create_counters(
                conn=conn,
                run_id=run_id,
                row=row,
                stage_key=stage_key,
                status=status,
            )
        return
    with legacy.pg.db_connection() as write_conn:
        with legacy.pg.db_cursor(conn=write_conn) as cur:
            row = (
                legacy.pg.fetch_one_with_cursor(
                    cur,
                    """
                select
                  total_jobs,
                  completed_jobs,
                  failed_jobs,
                  active_jobs,
                  items_found_total,
                  stage_counts
                from social.scrape_runs
                where id = %s
                for update
                """,
                    [run_id],
                )
                or {}
            )
            _persist_incremented_run_create_counters(
                conn=write_conn,
                run_id=run_id,
                row=row,
                stage_key=stage_key,
                status=status,
            )


def _persist_incremented_run_create_counters(
    *,
    conn: Any,
    run_id: str,
    row: dict[str, Any],
    stage_key: str,
    status: str,
) -> None:
    if not row:
        return
    total_jobs = legacy._normalize_non_negative_int(row.get("total_jobs")) + 1
    completed_jobs = legacy._normalize_non_negative_int(row.get("completed_jobs"))
    failed_jobs = legacy._normalize_non_negative_int(row.get("failed_jobs"))
    active_jobs = legacy._normalize_non_negative_int(row.get("active_jobs")) + (1 if _status_is_active(status) else 0)
    items_found_total = legacy._normalize_non_negative_int(row.get("items_found_total"))
    stage_counts = _normalize_stage_counts(row.get("stage_counts"))
    stage_counts = _increment_stage_counter(stage_counts, stage=stage_key, key="total", delta=1)
    if _status_is_active(status):
        stage_counts = _increment_stage_counter(stage_counts, stage=stage_key, key="active", delta=1)
    _persist_run_counters_and_summary(
        conn=conn,
        run_id=run_id,
        total_jobs=total_jobs,
        completed_jobs=completed_jobs,
        failed_jobs=failed_jobs,
        active_jobs=active_jobs,
        items_found_total=items_found_total,
        stage_counts=stage_counts,
    )


def _increment_run_counters_on_job_finish(
    *,
    run_id: str,
    stage: str,
    prior_status: str,
    new_status: str,
    prior_items_found: int,
    new_items_found: int,
) -> None:
    if not run_id or not legacy._run_counter_columns_ready():
        return
    stage_key = str(stage or "unknown").strip() or "unknown"
    active_delta = (1 if _status_is_active(new_status) else 0) - (1 if _status_is_active(prior_status) else 0)
    completed_delta = (1 if _status_is_completed(new_status) else 0) - (1 if _status_is_completed(prior_status) else 0)
    failed_delta = (1 if _status_is_failed(new_status) else 0) - (1 if _status_is_failed(prior_status) else 0)
    items_delta = legacy._normalize_non_negative_int(new_items_found) - legacy._normalize_non_negative_int(
        prior_items_found
    )

    with legacy.pg.db_connection() as conn:
        with legacy.pg.db_cursor(conn=conn) as cur:
            row = (
                legacy.pg.fetch_one_with_cursor(
                    cur,
                    """
                select
                  total_jobs,
                  completed_jobs,
                  failed_jobs,
                  active_jobs,
                  items_found_total,
                  stage_counts
                from social.scrape_runs
                where id = %s
                for update
                """,
                    [run_id],
                )
                or {}
            )
            if not row:
                return
            total_jobs = legacy._normalize_non_negative_int(row.get("total_jobs"))
            completed_jobs = max(0, legacy._normalize_non_negative_int(row.get("completed_jobs")) + completed_delta)
            failed_jobs = max(0, legacy._normalize_non_negative_int(row.get("failed_jobs")) + failed_delta)
            active_jobs = max(0, legacy._normalize_non_negative_int(row.get("active_jobs")) + active_delta)
            items_found_total = max(0, legacy._normalize_non_negative_int(row.get("items_found_total")) + items_delta)
            stage_counts = _normalize_stage_counts(row.get("stage_counts"))
            stage_counts = _increment_stage_counter(stage_counts, stage=stage_key, key="active", delta=active_delta)
            stage_counts = _increment_stage_counter(
                stage_counts,
                stage=stage_key,
                key="completed",
                delta=completed_delta,
            )
            stage_counts = _increment_stage_counter(stage_counts, stage=stage_key, key="failed", delta=failed_delta)
            _persist_run_counters_and_summary(
                conn=conn,
                run_id=run_id,
                total_jobs=total_jobs,
                completed_jobs=completed_jobs,
                failed_jobs=failed_jobs,
                active_jobs=active_jobs,
                items_found_total=items_found_total,
                stage_counts=stage_counts,
            )


def _recompute_run_summary_from_jobs(run_id: str) -> dict[str, Any]:
    summary_row = (
        legacy.pg.fetch_one(
            """
        with job_rows as (
          select
            j.*,
            coalesce(j.config->>'stage', j.metadata->>'stage', j.job_type, 'unknown') as effective_stage,
            (
              j.status = 'failed'
              and j.platform = 'instagram'
              and coalesce(j.config->>'stage', j.metadata->>'stage', j.job_type) = %s
              and exists (
                select 1
                from social.scrape_jobs child
                where child.run_id = j.run_id
                  and child.id <> j.id
                  and coalesce(child.config->>'stage', child.metadata->>'stage', child.job_type) =
                    coalesce(j.config->>'stage', j.metadata->>'stage', j.job_type)
                  and (
                    child.config->>'comments_retry_rebalance_source_job_id' = j.id::text
                    or (
                      child.created_at > j.created_at
                      and nullif(child.config->>'comments_shard_index', '') =
                        nullif(j.config->>'comments_shard_index', '')
                    )
                  )
              )
            ) as superseded_by_comments_rebalance
          from social.scrape_jobs j
          where j.run_id = %s
        ),
        effective_jobs as (
          select *
          from job_rows
          where not superseded_by_comments_rebalance
        ),
        stats as (
          select
            count(*)::int as total_jobs,
            count(*) filter (where status = 'completed')::int as completed_jobs,
            count(*) filter (where status = 'failed')::int as failed_jobs,
            count(*) filter (where status in ('queued', 'pending', 'retrying', 'running'))::int as active_jobs,
            coalesce(sum(items_found), 0)::int as items_found_total
          from effective_jobs
        ),
        stage_stats as (
          select
            effective_stage as stage,
            count(*)::int as total,
            count(*) filter (where status = 'completed')::int as completed,
            count(*) filter (where status = 'failed')::int as failed,
            count(*) filter (where status in ('queued', 'pending', 'retrying', 'running'))::int as active
          from effective_jobs
          group by effective_stage
        )
        select
          (select row_to_json(stats) from stats) as stats,
          coalesce((select jsonb_object_agg(stage, jsonb_build_object(
            'total', total,
            'completed', completed,
            'failed', failed,
            'active', active
          )) from stage_stats), '{}'::jsonb) as stage_counts
        """,
            [legacy.INSTAGRAM_COMMENTS_SCRAPLING_STAGE, run_id],
        )
        or {}
    )
    stats = dict(summary_row.get("stats") or {})
    return _build_run_summary_payload(
        total_jobs=stats.get("total_jobs"),
        completed_jobs=stats.get("completed_jobs"),
        failed_jobs=stats.get("failed_jobs"),
        active_jobs=stats.get("active_jobs"),
        items_found_total=stats.get("items_found_total"),
        stage_counts=summary_row.get("stage_counts"),
    )


def _update_run_summary(
    run_id: str,
    *,
    force_recompute: bool = False,
    conn: Any | None = None,
) -> dict[str, Any]:
    if legacy._run_counter_columns_ready() and not force_recompute:
        row = (
            _call_with_optional_conn(
                legacy.pg.fetch_one,
                """
                select
                  total_jobs,
                  completed_jobs,
                  failed_jobs,
                  active_jobs,
                  items_found_total,
                  stage_counts,
                  summary
                from social.scrape_runs
                where id = %s
                """,
                [run_id],
                conn=conn,
            )
            or {}
        )
        summary = _build_run_summary_payload(
            total_jobs=row.get("total_jobs"),
            completed_jobs=row.get("completed_jobs"),
            failed_jobs=row.get("failed_jobs"),
            active_jobs=row.get("active_jobs"),
            items_found_total=row.get("items_found_total"),
            stage_counts=row.get("stage_counts"),
        )
        _preserve_protected_run_summary_fields(summary, row.get("summary"))
        _call_with_optional_conn(
            legacy.pg.fetch_one,
            """
            update social.scrape_runs
            set summary = %s::jsonb
            where id = %s
            returning id::text
            """,
            [legacy.json.dumps(summary), run_id],
            conn=conn,
        )
        return summary

    summary = _recompute_run_summary_from_jobs(run_id)
    if legacy._run_counter_columns_ready():
        if conn is not None:
            _persist_run_counters_and_summary(
                conn=conn,
                run_id=run_id,
                total_jobs=int(summary.get("total_jobs") or 0),
                completed_jobs=int(summary.get("completed_jobs") or 0),
                failed_jobs=int(summary.get("failed_jobs") or 0),
                active_jobs=int(summary.get("active_jobs") or 0),
                items_found_total=int(summary.get("items_found_total") or 0),
                stage_counts=dict(summary.get("stage_counts") or {}),
            )
        else:
            with legacy.pg.db_connection() as managed_conn:
                _persist_run_counters_and_summary(
                    conn=managed_conn,
                    run_id=run_id,
                    total_jobs=int(summary.get("total_jobs") or 0),
                    completed_jobs=int(summary.get("completed_jobs") or 0),
                    failed_jobs=int(summary.get("failed_jobs") or 0),
                    active_jobs=int(summary.get("active_jobs") or 0),
                    items_found_total=int(summary.get("items_found_total") or 0),
                    stage_counts=dict(summary.get("stage_counts") or {}),
                )
    else:
        existing_row = (
            _call_with_optional_conn(
                legacy.pg.fetch_one,
                "select summary from social.scrape_runs where id = %s",
                [run_id],
                conn=conn,
            )
            or {}
        )
        _preserve_protected_run_summary_fields(summary, existing_row.get("summary"))
        _call_with_optional_conn(
            legacy.pg.fetch_one,
            """
            update social.scrape_runs
            set summary = %s::jsonb
            where id = %s
            returning id::text
            """,
            [legacy.json.dumps(summary), run_id],
            conn=conn,
        )
    return summary


def reconcile_run_summaries(*, run_ids: list[str] | None = None, limit: int = 100) -> dict[str, Any]:
    if not legacy._run_counter_columns_ready():
        return {"reconciled_runs": 0, "run_ids": []}

    safe_limit = max(1, min(int(limit), 500))
    if run_ids:
        candidate_run_ids = [str(run_id).strip() for run_id in run_ids if str(run_id).strip()][:safe_limit]
    else:
        rows = legacy.pg.fetch_all(
            """
            select id::text as id
            from social.scrape_runs
            where status in ('queued', 'running', 'failed')
            order by created_at desc
            limit %s
            """,
            [safe_limit],
        )
        candidate_run_ids = [str(row.get("id") or "").strip() for row in rows if str(row.get("id") or "").strip()]

    reconciled: list[str] = []
    for candidate_run_id in candidate_run_ids:
        summary = _recompute_run_summary_from_jobs(candidate_run_id)
        with legacy.pg.db_connection() as conn:
            _persist_run_counters_and_summary(
                conn=conn,
                run_id=candidate_run_id,
                total_jobs=int(summary.get("total_jobs") or 0),
                completed_jobs=int(summary.get("completed_jobs") or 0),
                failed_jobs=int(summary.get("failed_jobs") or 0),
                active_jobs=int(summary.get("active_jobs") or 0),
                items_found_total=int(summary.get("items_found_total") or 0),
                stage_counts=dict(summary.get("stage_counts") or {}),
            )
        reconciled.append(candidate_run_id)
    return {"reconciled_runs": len(reconciled), "run_ids": reconciled}


def _run_job_status_breakdown(run_id: str, *, conn: Any | None = None) -> dict[str, int]:
    row = (
        _call_with_optional_conn(
            legacy.pg.fetch_one,
            """
            select
              count(*) filter (where status = 'running')::int as running_jobs,
              count(*) filter (where status in ('queued', 'pending', 'retrying'))::int as queued_jobs,
              count(*) filter (where status = 'cancelling')::int as cancelling_jobs
            from social.scrape_jobs
            where run_id = %s::uuid
            """,
            [run_id],
            conn=conn,
        )
        or {}
    )
    return {
        "running_jobs": legacy._normalize_non_negative_int(row.get("running_jobs")),
        "queued_jobs": legacy._normalize_non_negative_int(row.get("queued_jobs")),
        "cancelling_jobs": legacy._normalize_non_negative_int(row.get("cancelling_jobs")),
    }


def _finalize_run_status(run_id: str, *, force_recompute: bool = False) -> dict[str, Any]:
    lock_key = int(legacy.hashlib.md5(run_id.encode()).hexdigest()[:15], 16) % (2**31)
    try:
        with legacy.pg.advisory_session_lock(
            lock_key,
            label="run-finalize-lock",
            pool_name=SOCIAL_CONTROL_POOL_NAME,
        ) as lock_conn:
            return _finalize_run_status_locked(run_id, lock_conn, force_recompute=force_recompute)
    except legacy.pg.AdvisoryLockUnavailable:
        legacy.logger.debug("[finalize_run_status] skipped — another worker is finalizing run=%s", run_id[:8])
        current = (
            legacy.pg.fetch_one(
                "select status from social.scrape_runs where id = %s",
                [run_id],
                pool_name=SOCIAL_CONTROL_POOL_NAME,
            )
            or {}
        )
        return {"status": current.get("status", "running")}
    except (
        legacy.pg.DatabaseServiceUnavailableError,
        InterfaceError,
        OperationalError,
        PoolError,
    ) as exc:
        legacy.logger.warning(
            "[finalize_run_status] deferred after database connection failure run=%s error=%s",
            run_id[:8],
            exc,
        )
        return {"status": "finalize_deferred", "finalize_deferred": True, "error": str(exc)}


def finalize_run_status(run_id: str, *, force_recompute: bool = False) -> dict[str, Any]:
    return _finalize_run_status(run_id, force_recompute=force_recompute)


def _finalize_run_status_locked(
    run_id: str,
    lock_conn: Any,
    *,
    force_recompute: bool = False,
) -> dict[str, Any]:
    summary = _update_run_summary(run_id, force_recompute=force_recompute, conn=lock_conn)
    active_jobs = int(summary.get("active_jobs") or 0)
    failed_jobs = int(summary.get("failed_jobs") or 0)
    current = (
        legacy.pg.fetch_one(
            "select status, config from social.scrape_runs where id = %s",
            [run_id],
            conn=lock_conn,
        )
        or {}
    )
    if str(current.get("status")) == "cancelled":
        return summary
    current_config = legacy._metadata_dict(current.get("config"))
    stage_counts = _normalize_stage_counts(legacy._metadata_dict(summary).get("stage_counts"))
    classify_stage = legacy._metadata_dict(stage_counts.get(legacy.POST_CLASSIFY_STAGE))
    classify_jobs_created = 0
    if legacy._normalize_non_negative_int(classify_stage.get("total")) <= 0:
        classify_jobs_created = _call_with_optional_conn(
            legacy._maybe_enqueue_shared_catalog_classify_jobs_after_fetch,
            run_id=run_id,
            source_scope=str(current_config.get("source_scope") or "").strip() or "network",
            run_config=current_config,
            conn=lock_conn,
        )
    if classify_jobs_created > 0:
        summary = _update_run_summary(run_id, force_recompute=True, conn=lock_conn)
        active_jobs = int(summary.get("active_jobs") or 0)
        failed_jobs = int(summary.get("failed_jobs") or 0)
    if active_jobs <= 0 and failed_jobs > 0 and not force_recompute:
        summary = _update_run_summary(run_id, force_recompute=True, conn=lock_conn)
        active_jobs = int(summary.get("active_jobs") or 0)
        failed_jobs = int(summary.get("failed_jobs") or 0)
    status_breakdown = _run_job_status_breakdown(run_id, conn=lock_conn)
    status_breakdown_active_jobs = (
        status_breakdown["running_jobs"] + status_breakdown["queued_jobs"] + status_breakdown["cancelling_jobs"]
    )
    if active_jobs > 0 and status_breakdown_active_jobs <= 0:
        summary = _update_run_summary(run_id, force_recompute=True, conn=lock_conn)
        active_jobs = int(summary.get("active_jobs") or 0)
        failed_jobs = int(summary.get("failed_jobs") or 0)
        status_breakdown = _run_job_status_breakdown(run_id, conn=lock_conn)
    fetch_terminal_error = legacy._resolve_pipeline_ingest_mode(
        current_config.get("pipeline_ingest_mode")
    ) == legacy.SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE and _call_with_optional_conn(
        legacy._shared_catalog_fetch_has_terminal_error,
        run_id,
        conn=lock_conn,
    )
    if status_breakdown["running_jobs"] > 0:
        next_status = "running"
    elif status_breakdown["cancelling_jobs"] > 0:
        next_status = "cancelling"
    elif active_jobs > 0 or status_breakdown["queued_jobs"] > 0:
        next_status = "queued"
    elif failed_jobs > 0 or fetch_terminal_error:
        next_status = "failed"
    else:
        next_status = "completed"
    _set_run_status(run_id, next_status, conn=lock_conn)
    _maybe_start_deferred_comments_followup(
        run_id=run_id,
        run_status=next_status,
        run_config=current_config,
        summary=summary,
        conn=lock_conn,
    )
    if _call_with_optional_conn(legacy._column_exists, "social", "scrape_runs", "sync_session_id", conn=lock_conn):
        run_row = (
            legacy.pg.fetch_one(
                "select sync_session_id::text as sync_session_id from social.scrape_runs where id = %s::uuid",
                [run_id],
                conn=lock_conn,
            )
            or {}
        )
        sync_session_id = str(run_row.get("sync_session_id") or "").strip()
        if sync_session_id:
            try:
                from trr_backend.repositories.social_sync_orchestrator import evaluate_sync_session

                evaluate_sync_session(sync_session_id)
            except Exception:  # noqa: BLE001
                legacy.logger.exception(
                    "Failed to evaluate sync session after run finalization: run=%s",
                    run_id,
                )
    return summary
