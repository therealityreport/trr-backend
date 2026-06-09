"""Queue-status read seam extracted from the legacy social control-plane monolith."""

from __future__ import annotations

import copy
from importlib import import_module
from typing import Any

SOCIAL_CONTROL_POOL_NAME = "social_control"


def _legacy_repo():
    return import_module("trr_backend.socials.social_season_analytics_impl")


def invalidate_queue_status_cache() -> None:
    repo = _legacy_repo()
    repo._clear_social_hot_path_caches()
    with repo._queue_status_cache_lock:
        repo._queue_status_cache = None
        repo._queue_status_last_good_cache = None


def get_queue_status(
    *,
    recent_failures_limit: int = 20,
    statement_timeout_ms: int = 5000,
    include_recent_failures: bool = True,
    include_stuck_jobs: bool = True,
    stuck_jobs_limit: int = 100,
    include_runs_summary: bool = True,
    summary_only: bool = False,
    fresh: bool = False,
) -> dict[str, Any]:
    repo = _legacy_repo()
    safe_recent_failures_limit = max(1, min(int(recent_failures_limit), 100))
    safe_statement_timeout_ms = max(1000, min(int(statement_timeout_ms), 30000))
    safe_include_recent_failures = bool(include_recent_failures)
    safe_include_stuck_jobs = bool(include_stuck_jobs)
    safe_stuck_jobs_limit = max(1, min(int(stuck_jobs_limit), 500))
    safe_include_runs_summary = bool(include_runs_summary)
    safe_summary_only = bool(summary_only)
    cache_ttl_seconds = repo._resolve_positive_int_env(
        "SOCIAL_QUEUE_STATUS_CACHE_TTL_SECONDS",
        repo.SOCIAL_QUEUE_STATUS_CACHE_TTL_SECONDS_DEFAULT,
        minimum=0,
    )

    if cache_ttl_seconds > 0 and not fresh:
        now = repo.time_module.monotonic()
        with repo._queue_status_cache_lock:
            if repo._queue_status_cache is not None:
                (
                    cached_at,
                    cached_limit,
                    cached_timeout,
                    cached_include_recent_failures,
                    cached_include_stuck_jobs,
                    cached_stuck_jobs_limit,
                    cached_include_runs_summary,
                    cached_summary_only,
                    cached_payload,
                ) = repo._queue_status_cache
                if (
                    cached_limit == safe_recent_failures_limit
                    and cached_timeout == safe_statement_timeout_ms
                    and cached_include_recent_failures == safe_include_recent_failures
                    and cached_include_stuck_jobs == safe_include_stuck_jobs
                    and cached_stuck_jobs_limit == safe_stuck_jobs_limit
                    and cached_include_runs_summary == safe_include_runs_summary
                    and cached_summary_only == safe_summary_only
                    and (now - cached_at) < cache_ttl_seconds
                ):
                    return copy.deepcopy(cached_payload)

    def _finalize(payload: dict[str, Any]) -> dict[str, Any]:
        if not payload.get("queue", {}).get("error"):
            with repo._queue_status_cache_lock:
                repo._queue_status_last_good_cache = (repo.time_module.monotonic(), copy.deepcopy(payload))
        if cache_ttl_seconds > 0:
            with repo._queue_status_cache_lock:
                repo._queue_status_cache = (
                    repo.time_module.monotonic(),
                    safe_recent_failures_limit,
                    safe_statement_timeout_ms,
                    safe_include_recent_failures,
                    safe_include_stuck_jobs,
                    safe_stuck_jobs_limit,
                    safe_include_runs_summary,
                    safe_summary_only,
                    payload,
                )
        return copy.deepcopy(payload)

    queue_payload: dict[str, Any] = {
        "by_status": repo._empty_queue_status_counts(),
        "by_stage": {},
        "by_stage_platform": {},
        "by_platform": {},
        "by_job_type": {},
        "running_jobs": [],
        "recent_failures": [],
        "silent_drop_warnings": [],
        "silent_drop_warnings_total": 0,
        "stuck_jobs": [],
        "stuck_jobs_total": 0,
        "dispatch_blocked_jobs": [],
        "dispatch_blocked_jobs_total": 0,
        "dispatch_blocked_by_reason": {},
        "waiting_for_claim_jobs_total": 0,
        "retrying_dispatch_jobs_total": 0,
        "stale_claims": {
            "total": 0,
            "by_reason": {},
            "by_platform": {},
            "by_stage": {},
        },
        "runs_by_status": repo._empty_queue_status_counts(),
        "runs_total": 0,
    }
    errors: list[str] = []

    try:
        with repo.pg.db_connection(label="queue-status:relation-check", pool_name=SOCIAL_CONTROL_POOL_NAME) as conn:
            scrape_jobs_exists = repo._relation_exists("social.scrape_jobs", conn=conn)
        if not scrape_jobs_exists:
            queue_payload["error"] = "scrape_jobs_table_missing"
            return _finalize(
                {
                    "queue_enabled": repo.is_queue_enabled(),
                    "workers": repo.get_worker_health(),
                    "queue": queue_payload,
                }
            )
    except Exception as exc:  # noqa: BLE001
        errors.append(f"scrape_jobs_relation_check_failed: {exc}")

    if not safe_summary_only:
        try:
            repo._reconcile_active_queue_runs(limit=200)
        except Exception as exc:  # noqa: BLE001
            repo.logger.warning("Queue status active-run reconciliation failed: %s", exc)
            errors.append(f"queue_run_reconciliation_failed: {exc}")

        try:
            repo.recover_dispatch_blocked_no_progress_jobs(limit=safe_stuck_jobs_limit)
        except Exception as exc:  # noqa: BLE001
            repo.logger.warning("Queue status blocked-job recovery failed: %s", exc)
            errors.append(f"queue_dispatch_blocked_recovery_failed: {exc}")

    try:
        with repo.pg.db_connection(label="queue-status:aggregate", pool_name=SOCIAL_CONTROL_POOL_NAME) as conn:
            with repo.pg.db_cursor(conn=conn) as cur:
                cur.execute("set local statement_timeout = %s", [str(safe_statement_timeout_ms)])
                aggregate_rows = repo.pg.fetch_all_with_cursor(
                    cur,
                    """
                    with active_runs as (
                      select id
                      from social.scrape_runs
                      where status = any(%s::text[])
                    )
                    select
                      coalesce(j.platform, 'unknown') as platform,
                      coalesce(j.job_type, 'unknown') as job_type,
                      coalesce(j.status, 'unknown') as status,
                      lower(
                        coalesce(
                          nullif(j.config->>'stage', ''),
                          nullif(j.metadata->>'stage', ''),
                          nullif(j.job_type, ''),
                          'unknown'
                        )
                      ) as stage,
                      count(*)::bigint as total
                    from social.scrape_jobs j
                    left join active_runs ar on ar.id = j.run_id
                    where j.status = any(%s::text[])
                       or ar.id is not null
                    group by 1, 2, 3, 4
                    """,
                    [list(repo._RUN_PROGRESS_ACTIVE_JOB_STATUSES), list(repo._RUN_PROGRESS_ACTIVE_JOB_STATUSES)],
                )
        by_status = repo._empty_queue_status_counts()
        by_stage: dict[str, dict[str, int]] = {}
        by_stage_platform: dict[str, dict[str, dict[str, int]]] = {}
        by_platform: dict[str, dict[str, int]] = {}
        by_job_type: dict[str, dict[str, int]] = {}

        for row in aggregate_rows:
            status = str(row.get("status") or "unknown").strip().lower() or "unknown"
            platform = str(row.get("platform") or "unknown").strip().lower() or "unknown"
            job_type = str(row.get("job_type") or "unknown").strip().lower() or "unknown"
            stage = repo._normalize_social_job_stage_for_stale(row.get("stage")) or "unknown"
            total = int(row.get("total") or 0)

            by_status[status] = by_status.get(status, 0) + total
            stage_bucket = by_stage.setdefault(stage, {})
            stage_bucket[status] = int(stage_bucket.get(status) or 0) + total
            stage_platform_bucket = by_stage_platform.setdefault(stage, {}).setdefault(platform, {})
            stage_platform_bucket[status] = int(stage_platform_bucket.get(status) or 0) + total
            platform_bucket = by_platform.setdefault(platform, {})
            platform_bucket[status] = int(platform_bucket.get(status) or 0) + total
            job_type_bucket = by_job_type.setdefault(job_type, {})
            job_type_bucket[status] = int(job_type_bucket.get(status) or 0) + total

        queue_payload["by_status"] = by_status
        queue_payload["by_stage"] = by_stage
        queue_payload["by_stage_platform"] = by_stage_platform
        queue_payload["by_platform"] = by_platform
        queue_payload["by_job_type"] = by_job_type
    except Exception as exc:  # noqa: BLE001
        repo.logger.warning("Queue status aggregate query failed: %s", exc)
        errors.append(f"queue_aggregate_query_failed: {exc}")

    if not safe_summary_only:
        try:
            with repo.pg.db_connection(label="queue-status:running-jobs", pool_name=SOCIAL_CONTROL_POOL_NAME) as conn:
                with repo.pg.db_cursor(conn=conn) as cur:
                    cur.execute("set local statement_timeout = %s", [str(safe_statement_timeout_ms)])
                    running_jobs = repo.pg.fetch_all_with_cursor(
                        cur,
                        """
                        select
                          j.id::text as id,
                          j.run_id::text as run_id,
                          j.platform,
                          j.job_type,
                          lower(
                            coalesce(
                              nullif(j.config->>'stage', ''),
                              nullif(j.metadata->>'stage', ''),
                              nullif(j.job_type, ''),
                              'unknown'
                            )
                          ) as stage,
                          nullif(coalesce(j.config->>'account', j.metadata->>'account', ''), '') as account_handle,
                          j.worker_id,
                          j.started_at,
                          j.heartbeat_at,
                          nullif(coalesce(j.metadata->'dispatch'->>'dispatch_backend', ''), '') as dispatch_backend,
                          nullif(
                            coalesce(j.config->>'required_execution_backend', ''),
                            ''
                          ) as required_execution_backend
                        from social.scrape_jobs j
                        where j.status = 'running'
                        order by coalesce(j.heartbeat_at, j.started_at, j.created_at) desc, j.created_at desc
                        """,
                    )
            queue_payload["running_jobs"] = [
                {
                    "id": str(row.get("id") or ""),
                    "run_id": str(row.get("run_id") or "").strip() or None,
                    "platform": str(row.get("platform") or "").strip().lower() or "unknown",
                    "job_type": str(row.get("job_type") or "").strip().lower() or "unknown",
                    "stage": repo._normalize_social_job_stage_for_stale(row.get("stage")) or "unknown",
                    "account_handle": str(row.get("account_handle") or "").strip() or None,
                    "worker_id": str(row.get("worker_id") or "").strip() or None,
                    "started_at": repo._iso(repo._coerce_dt(row.get("started_at"))),
                    "heartbeat_at": repo._iso(repo._coerce_dt(row.get("heartbeat_at"))),
                    "dispatch_backend": str(row.get("dispatch_backend") or "").strip().lower() or None,
                    "required_execution_backend": str(row.get("required_execution_backend") or "").strip().lower()
                    or None,
                }
                for row in running_jobs
            ]
        except Exception as exc:  # noqa: BLE001
            repo.logger.warning("Queue status running-jobs query failed: %s", exc)
            errors.append(f"queue_running_jobs_query_failed: {exc}")

    if safe_include_recent_failures:
        try:
            features = repo._scrape_jobs_features()
            select_run_id = "run_id::text as run_id" if features.get("has_run_id") else "null::text as run_id"
            select_last_error_code = (
                "last_error_code" if features.get("has_queue_fields") else "null::text as last_error_code"
            )
            select_last_error_class = (
                "last_error_class" if features.get("has_queue_fields") else "null::text as last_error_class"
            )

            with repo.pg.db_connection(
                label="queue-status:recent-failures", pool_name=SOCIAL_CONTROL_POOL_NAME
            ) as conn:
                with repo.pg.db_cursor(conn=conn) as cur:
                    cur.execute("set local statement_timeout = %s", [str(safe_statement_timeout_ms)])
                    recent_failures = repo.pg.fetch_all_with_cursor(
                        cur,
                        f"""
                        select
                          id::text as id,
                          {select_run_id},
                          platform,
                          job_type,
                          status,
                          error_message,
                          {select_last_error_code},
                          {select_last_error_class},
                          created_at,
                          completed_at
                        from social.scrape_jobs
                        where status = any(%s::text[])
                          and {repo._recent_failure_not_dismissed_sql("social.scrape_jobs")}
                        order by coalesce(completed_at, created_at) desc
                        limit %s
                        """,
                        [list(repo._RECENT_FAILURE_TERMINAL_STATUSES), safe_recent_failures_limit],
                    )
            queue_payload["recent_failures"] = [
                {
                    "id": str(row.get("id") or ""),
                    "run_id": str(row.get("run_id") or "").strip() or None,
                    "platform": str(row.get("platform") or ""),
                    "job_type": str(row.get("job_type") or ""),
                    "status": str(row.get("status") or ""),
                    "error_message": str(row.get("error_message") or "") or None,
                    "last_error_code": str(row.get("last_error_code") or "") or None,
                    "last_error_class": str(row.get("last_error_class") or "") or None,
                    "created_at": repo._iso(repo._coerce_dt(row.get("created_at"))),
                    "completed_at": repo._iso(repo._coerce_dt(row.get("completed_at"))),
                }
                for row in recent_failures
            ]
        except Exception as exc:  # noqa: BLE001
            repo.logger.warning("Queue status recent-failures query failed: %s", exc)
            errors.append(f"queue_recent_failures_query_failed: {exc}")

    try:
        with repo.pg.db_connection(label="queue-status:silent-drop-warnings", pool_name=SOCIAL_CONTROL_POOL_NAME) as conn:
            with repo.pg.db_cursor(conn=conn) as cur:
                cur.execute("set local statement_timeout = %s", [str(safe_statement_timeout_ms)])
                silent_drop_warnings = repo.pg.fetch_all_with_cursor(
                    cur,
                    """
                    select
                      j.id::text as id,
                      j.run_id::text as run_id,
                      coalesce(
                        nullif(j.metadata->'diagnostics'->'post_persist_truthfulness'->>'platform', ''),
                        nullif(j.platform, ''),
                        'unknown'
                      ) as platform,
                      coalesce(
                        nullif(j.metadata->'diagnostics'->'post_persist_truthfulness'->>'account', ''),
                        nullif(j.config->>'account', ''),
                        nullif(j.metadata->>'account', '')
                      ) as account_handle,
                      lower(
                        coalesce(
                          nullif(j.config->>'stage', ''),
                          nullif(j.metadata->>'stage', ''),
                          nullif(j.job_type, ''),
                          'unknown'
                        )
                      ) as stage,
                      j.job_type,
                      j.status,
                      j.metadata->'diagnostics'->'post_persist_truthfulness'->>'posts_checked' as posts_checked,
                      j.metadata->'diagnostics'->'post_persist_truthfulness'->>'posts_upserted' as posts_upserted,
                      j.metadata->'diagnostics'->'post_persist_truthfulness'->>'media_assets_persisted' as media_assets_persisted,
                      coalesce(j.completed_at, j.created_at) as observed_at
                    from social.scrape_jobs j
                    where coalesce(
                      j.metadata->'diagnostics'->'post_persist_truthfulness'->>'silent_drop_detected',
                      'false'
                    ) = 'true'
                      and coalesce(j.completed_at, j.created_at) >= now() - interval '7 days'
                    order by coalesce(j.completed_at, j.created_at) desc
                    limit %s
                    """,
                    [min(safe_recent_failures_limit, 20)],
                )
        queue_payload["silent_drop_warnings"] = [
            {
                "id": str(row.get("id") or ""),
                "run_id": str(row.get("run_id") or "").strip() or None,
                "platform": str(row.get("platform") or "").strip().lower() or "unknown",
                "account_handle": str(row.get("account_handle") or "").strip().lstrip("@") or None,
                "stage": repo._normalize_social_job_stage_for_stale(row.get("stage")) or "unknown",
                "job_type": str(row.get("job_type") or "").strip().lower() or "unknown",
                "status": str(row.get("status") or "").strip().lower() or "unknown",
                "posts_checked": repo._normalize_non_negative_int(row.get("posts_checked")),
                "posts_upserted": repo._normalize_non_negative_int(row.get("posts_upserted")),
                "media_assets_persisted": repo._normalize_non_negative_int(row.get("media_assets_persisted")),
                "observed_at": repo._iso(repo._coerce_dt(row.get("observed_at"))),
            }
            for row in silent_drop_warnings
        ]
        queue_payload["silent_drop_warnings_total"] = len(queue_payload["silent_drop_warnings"])
    except Exception as exc:  # noqa: BLE001
        repo.logger.warning("Queue status silent-drop warning query failed: %s", exc)
        queue_payload["silent_drop_warnings_error"] = str(exc)

    if safe_include_stuck_jobs and not safe_summary_only:
        try:
            stuck_jobs, stuck_jobs_total = repo._list_stuck_jobs(limit=safe_stuck_jobs_limit)
            queue_payload["stuck_jobs"] = stuck_jobs
            queue_payload["stuck_jobs_total"] = stuck_jobs_total
            stale_claims_by_reason: dict[str, int] = {}
            stale_claims_by_platform: dict[str, int] = {}
            stale_claims_by_stage: dict[str, int] = {}
            for row in stuck_jobs:
                reason = str(row.get("stuck_reason") or "unknown").strip().lower() or "unknown"
                platform = str(row.get("platform") or "unknown").strip().lower() or "unknown"
                stage = repo._normalize_social_job_stage_for_stale(row.get("job_type")) or "unknown"
                stale_claims_by_reason[reason] = int(stale_claims_by_reason.get(reason) or 0) + 1
                stale_claims_by_platform[platform] = int(stale_claims_by_platform.get(platform) or 0) + 1
                stale_claims_by_stage[stage] = int(stale_claims_by_stage.get(stage) or 0) + 1
            queue_payload["stale_claims"] = {
                "total": int(stuck_jobs_total or 0),
                "by_reason": stale_claims_by_reason,
                "by_platform": stale_claims_by_platform,
                "by_stage": stale_claims_by_stage,
            }
        except Exception as exc:  # noqa: BLE001
            repo.logger.warning("Queue status stuck-jobs query failed: %s", exc)
            errors.append(f"queue_stuck_jobs_query_failed: {exc}")

    if not safe_summary_only:
        try:
            dispatch_blocked_jobs, dispatch_blocked_jobs_total = repo._list_dispatch_blocked_jobs(
                limit=safe_stuck_jobs_limit
            )
            queue_payload["dispatch_blocked_jobs"] = dispatch_blocked_jobs
            queue_payload["dispatch_blocked_jobs_total"] = dispatch_blocked_jobs_total
            blocked_by_reason: dict[str, int] = {}
            for row in dispatch_blocked_jobs:
                reason = str(row.get("stuck_reason") or "dispatch_blocked").strip().lower() or "dispatch_blocked"
                blocked_by_reason[reason] = int(blocked_by_reason.get(reason) or 0) + 1
            queue_payload["dispatch_blocked_by_reason"] = blocked_by_reason
        except Exception as exc:  # noqa: BLE001
            repo.logger.warning("Queue status dispatch-blocked query failed: %s", exc)
            errors.append(f"queue_dispatch_blocked_query_failed: {exc}")

        try:
            with repo.pg.db_connection(
                label="queue-status:dispatch-health", pool_name=SOCIAL_CONTROL_POOL_NAME
            ) as conn:
                with repo.pg.db_cursor(conn=conn) as cur:
                    cur.execute("set local statement_timeout = %s", [str(safe_statement_timeout_ms)])
                    dispatch_rows = repo.pg.fetch_all_with_cursor(
                        cur,
                        """
                        select
                          id::text as id,
                          status,
                          worker_id,
                          metadata,
                          available_at,
                          created_at
                        from social.scrape_jobs
                        where status in ('queued', 'pending', 'retrying')
                          and lower(coalesce(metadata->'dispatch'->>'dispatch_backend', '')) = 'modal'
                        """,
                    )
            dispatch_health = repo._build_run_dispatch_health(dispatch_rows)
            queue_payload["waiting_for_claim_jobs_total"] = repo._normalize_non_negative_int(
                dispatch_health.get("queued_unclaimed_jobs")
            )
            queue_payload["retrying_dispatch_jobs_total"] = repo._normalize_non_negative_int(
                dispatch_health.get("retrying_dispatch_jobs")
            )
        except Exception as exc:  # noqa: BLE001
            repo.logger.warning("Queue status dispatch-health query failed: %s", exc)
            errors.append(f"queue_dispatch_health_query_failed: {exc}")

    if safe_include_runs_summary and not safe_summary_only:
        try:
            with repo.pg.db_connection(
                label="queue-status:runs-relation-check", pool_name=SOCIAL_CONTROL_POOL_NAME
            ) as conn:
                scrape_runs_exists = repo._relation_exists("social.scrape_runs", conn=conn)
            if scrape_runs_exists:
                run_failure_not_dismissed_sql = repo._run_failure_not_dismissed_sql("r")
                with repo.pg.db_connection(
                    label="queue-status:runs-summary", pool_name=SOCIAL_CONTROL_POOL_NAME
                ) as conn:
                    with repo.pg.db_cursor(conn=conn) as cur:
                        cur.execute("set local statement_timeout = %s", [str(safe_statement_timeout_ms)])
                        run_rows = repo.pg.fetch_all_with_cursor(
                            cur,
                            f"""
                            select
                              coalesce(status, 'unknown') as status,
                              count(*)::bigint as total
                            from social.scrape_runs r
                            where (
                              coalesce(r.status, 'unknown') not in ('failed', 'retrying')
                              or {run_failure_not_dismissed_sql}
                            )
                            group by 1
                            """,
                        )
                runs_by_status = repo._empty_queue_status_counts()
                runs_total = 0
                for row in run_rows:
                    status = str(row.get("status") or "unknown").strip().lower() or "unknown"
                    total = int(row.get("total") or 0)
                    runs_by_status[status] = runs_by_status.get(status, 0) + total
                    runs_total += total
                queue_payload["runs_by_status"] = runs_by_status
                queue_payload["runs_total"] = runs_total
            else:
                queue_payload["runs_by_status"] = repo._empty_queue_status_counts()
                queue_payload["runs_total"] = 0
        except Exception as exc:  # noqa: BLE001
            repo.logger.warning("Queue status runs-summary query failed: %s", exc)
            errors.append(f"queue_runs_summary_query_failed: {exc}")

    workers_payload: dict[str, Any]
    try:
        workers_payload = repo.get_worker_health()
        if safe_summary_only:
            workers_payload = dict(workers_payload)
            workers_payload["workers"] = []
    except Exception as exc:  # noqa: BLE001
        repo.logger.warning("Queue status worker-health query failed: %s", exc)
        errors.append(f"queue_worker_health_failed: {exc}")
        queue_payload["error"] = "; ".join(errors)
        workers_payload = {
            "healthy": False,
            "healthy_workers": 0,
            "fresh_workers": 0,
            "stale_workers": 0,
            "stale_hidden_count": 0,
            "active_workers": 0,
            "total_workers": 0,
            "stale_after_seconds": repo._resolve_positive_int_env(
                "SOCIAL_WORKER_HEARTBEAT_STALE_SECONDS",
                repo.SOCIAL_WORKER_HEARTBEAT_STALE_SECONDS_DEFAULT,
                minimum=30,
            ),
            "executor_backend": (
                "modal" if repo.is_modal_remote_executor_enabled() else repo.execution_backend_canonical()
            ),
            "dispatch_enabled": False,
            "dispatcher_heartbeat_fresh": False,
            "active_invocations": 0,
            "oldest_queued_age_seconds": None,
            "stale_running_count": 0,
            "last_dispatch_success_at": None,
            "last_dispatch_error": None,
            "workers": [],
            "reason": "health_query_failed",
        }
        workers_payload["alerts"] = repo._build_worker_health_alerts(workers_payload)

    if errors:
        stale_fallback_seconds = repo._resolve_positive_int_env(
            "SOCIAL_QUEUE_STATUS_STALE_FALLBACK_SECONDS",
            repo.SOCIAL_QUEUE_STATUS_STALE_FALLBACK_SECONDS_DEFAULT,
            minimum=0,
        )
        if stale_fallback_seconds > 0:
            with repo._queue_status_cache_lock:
                if repo._queue_status_last_good_cache is not None:
                    cached_at, cached_payload = repo._queue_status_last_good_cache
                    if (repo.time_module.monotonic() - cached_at) <= stale_fallback_seconds:
                        return copy.deepcopy(cached_payload)
        queue_payload["error"] = "; ".join(errors)

    return _finalize(
        {
            "queue_enabled": repo.is_queue_enabled(),
            "remote_plane": repo._configured_execution_metadata(),
            "workers": workers_payload,
            "queue": queue_payload,
            "alerts": repo._build_queue_status_alerts(workers_payload=workers_payload, queue_payload=queue_payload),
        }
    )


__all__ = ["get_queue_status", "invalidate_queue_status_cache"]
