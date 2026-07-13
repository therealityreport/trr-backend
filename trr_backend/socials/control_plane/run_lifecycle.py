"""Run lifecycle mutation entrypoints for the social control plane."""

from __future__ import annotations

import importlib
from datetime import UTC, datetime, timedelta
from types import ModuleType
from typing import Any
from uuid import UUID

from psycopg2 import InterfaceError, OperationalError
from psycopg2.pool import PoolError

SOCIAL_CONTROL_POOL_NAME = "social_control"
_SCRAPE_RUN_ALLOWED_STATUSES = {"queued", "running", "cancelling", "completed", "failed", "cancelled"}
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


_VALID_CATALOG_SOURCE_SCOPES = ("network", "creator", "community", "news")


def _normalize_run_source_scope(value: str | None, *, default: str = "network") -> str:
    """Validate/normalize a scrape-run source_scope before it is persisted.

    Mirrors api.normalize_source_scope_param and launch._normalize_catalog_source_scope so
    no lifecycle caller (e.g. a deferred comments follow-up) can insert an unvalidated scope
    such as a raw "bravo" that would split rows from the canonical "network" value.
    """
    normalized = str(value or default).strip().lower() or default
    if normalized == "bravo":
        return "network"
    if normalized in _VALID_CATALOG_SOURCE_SCOPES:
        return normalized
    raise ValueError(f"Unsupported source scope: {value!r}")


def _create_run(
    context: legacy.SeasonContext | None,
    *,
    source_scope: str,
    initiated_by: str | None,
    config: dict[str, Any],
    status: str,
    conn: Any | None = None,
) -> str:
    source_scope = _normalize_run_source_scope(source_scope)
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


def _set_run_status(
    run_id: str,
    status: str,
    *,
    conn: Any | None = None,
    expected_status: str | None = None,
) -> bool:
    status = _normalize_scrape_run_status(status)
    where_clause = "where id = %s"
    params: list[Any] = [status, status, status, status, status, status, run_id]
    if expected_status is not None:
        where_clause += " and status = %s"
        params.append(str(expected_status or "").strip().lower())
    row = _call_with_optional_conn(
        legacy.pg.fetch_one,
        f"""
        update social.scrape_runs
        set
          status = %s,
          started_at = case
            when %s = 'running' then coalesce(started_at, now())
            else started_at
          end,
          completed_at = case
            when %s in ('queued', 'pending', 'retrying', 'running', 'cancelling') then null
            when %s in ('completed', 'failed', 'cancelled') then coalesce(completed_at, now())
            else completed_at
          end,
          cancelled_at = case
            when %s in ('queued', 'pending', 'retrying', 'running') then null
            when %s in ('cancelling', 'cancelled') then coalesce(cancelled_at, now())
            else cancelled_at
          end
        {where_clause}
        returning id::text
        """,
        params,
        conn=conn,
    )
    if not row:
        return False
    legacy._invalidate_queue_status_cache()
    if status in {"completed", "failed", "cancelled"}:
        legacy._invalidate_week_detail_cache_after_run_terminal_status()
    return True


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
) -> dict[str, Any] | None:
    run_status = str(run_status or "").strip().lower()
    followup = legacy._metadata_dict(run_config.get("deferred_comments_followup"))
    comments_result: dict[str, Any] | None = None
    launch_claimed_at = str(followup.get("launch_claimed_at") or "").strip()
    launch_claim_token = str(followup.get("launch_claim_token") or "").strip()
    if launch_claimed_at or launch_claim_token:
        # The claim is written while holding the run-finalize lock. Re-read the
        # parent after that lock is released so cancellation wins before launch.
        current = (
            legacy.pg.fetch_one(
                "select status, config from social.scrape_runs where id = %s::uuid",
                [run_id],
                pool_name=SOCIAL_CONTROL_POOL_NAME,
            )
            or {}
        )
        current_status = str(current.get("status") or "").strip().lower()
        current_config = legacy._metadata_dict(current.get("config"))
        current_followup = legacy._metadata_dict(current_config.get("deferred_comments_followup"))
        if current_status in {"cancelling", "cancelled"}:
            cancelled_at = legacy._iso(legacy._now_utc())
            attached_followups = legacy._normalize_attached_followups(current_config.get("attached_followups"))
            child_run_id = str(current_followup.get("comments_run_id") or "").strip() or None
            cancelled_followup = {
                **current_followup,
                "state": "cancelled",
                "launch_claim_token": None,
                "launch_claimed_at": None,
                "launch_lease_expires_at": None,
                "cancelled_at": cancelled_at,
                "cancel_reason": "parent_run_cancelled_before_deferred_followup",
            }
            if child_run_id:
                _cancel_deferred_comments_child_durably(
                    run_id=run_id,
                    followup=cancelled_followup,
                    child_run_id=child_run_id,
                    cancelled_by="parent_run_cancelled_before_deferred_followup",
                    cancel_reason="parent_run_cancelled_before_deferred_followup",
                    attached_followups=attached_followups,
                    conn=conn,
                )
            else:
                _merge_run_config(
                    run_id,
                    config_updates={"deferred_comments_followup": cancelled_followup},
                    conn=conn,
                )
            return {"_deferred_followup_parent_cancelled": True}
        if current_status not in {"completed", "queued", "running"}:
            return None
        current_claim_token, current_claimed_at = _deferred_comments_followup_claim_identity(current_followup)
        if launch_claim_token:
            if current_claim_token != launch_claim_token:
                return None
        elif current_claimed_at != launch_claimed_at:
            return None
        run_status = current_status
        run_config = current_config
        followup = current_followup

        if followup.get("launch_recovered_at"):
            recovered_child = _find_recovered_deferred_comments_child(run_id=run_id, followup=followup)
            if recovered_child:
                comments_result = {
                    "run_id": recovered_child["run_id"],
                    "status": recovered_child["status"],
                    "runtime_version": legacy._metadata_dict(recovered_child["config"].get("required_runtime_version")),
                    "created_by_runtime_version": legacy._metadata_dict(
                        recovered_child["config"].get("created_by_runtime_version")
                    ),
                }
                recovered_existing_child = True
            else:
                recovered_existing_child = False
        else:
            recovered_existing_child = False
    else:
        recovered_existing_child = False

    if run_status not in {"completed", "queued", "running"}:
        return None
    if not legacy._shared_account_catalog_scrape_complete(run_config=run_config, summary=summary, conn=conn):
        return None
    if bool(run_config.get("comments_streaming_enabled")):
        try:
            reconcile_result = legacy.reconcile_instagram_catalog_comments_streaming_targets(run_id=run_id)
        except Exception:
            legacy.logger.exception(
                "Failed to reconcile streaming Instagram comments targets after run finalization: run=%s",
                run_id,
            )
            return None
        if reconcile_result is None:
            return None
        return {"comments_streaming_latest_reconcile": reconcile_result}
    if str(followup.get("state") or "").strip().lower() != "pending":
        return None
    if str(followup.get("platform") or "").strip().lower() != "instagram":
        return None

    attached_followups = legacy._normalize_attached_followups(run_config.get("attached_followups"))
    now_iso = legacy._iso(legacy._now_utc())
    try:
        comments_source = "deferred_after_catalog"
        if not recovered_existing_child:
            comments_result = legacy.start_social_account_comments_scrape(
                str(followup.get("platform") or "").strip(),
                str(followup.get("account_handle") or "").strip(),
                mode="profile",
                source_scope=str(followup.get("source_scope") or "network"),
                max_posts=None,
                max_comments_per_post=None,
                refresh_policy=str(followup.get("refresh_policy") or "all_saved_posts"),
                target_filter=str(followup.get("target_filter") or "").strip() or None,
                date_start=legacy._coerce_dt(followup.get("date_start")),
                date_end=legacy._coerce_dt(followup.get("date_end")),
                initiated_by="catalog_completion_followup",
                allow_local_dev_inline_bypass=bool(followup.get("allow_local_dev_inline_bypass")),
                comments_enable_media_followups=bool(followup.get("comments_enable_media_followups")),
                # bug-3: honor an operator-requested comments parallelism on the
                # deferred follow-up instead of always defaulting. None preserves the
                # launcher's default. Signature verified to accept comments_worker_count.
                comments_worker_count=legacy._normalize_non_negative_int(followup.get("comments_worker_count")) or None,
                launch_group_id=str(followup.get("launch_group_id") or "").strip() or None,
                cancel_active_before_relaunch=True,
            )
        config_updates = {
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
                "launch_claim_token": None,
                "launch_claimed_at": None,
                "launch_lease_expires_at": None,
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
        }
        if launch_claim_token:
            committed = _cas_deferred_comments_followup_state(
                run_id=run_id,
                expected_state="pending",
                followup=config_updates["deferred_comments_followup"],
                config_updates={"attached_followups": config_updates["attached_followups"]},
                expected_launch_claim_token=launch_claim_token,
                expected_launch_claimed_at=launch_claimed_at or None,
                conn=conn,
            )
            if committed is None:
                child_run_id = str((comments_result or {}).get("run_id") or "").strip()
                _cancel_deferred_comments_child_durably(
                    run_id=run_id,
                    followup=followup,
                    child_run_id=child_run_id,
                    cancelled_by="parent_run_cancelled_during_deferred_followup_launch",
                    cancel_reason="parent_run_cancelled_during_deferred_followup_launch",
                    attached_followups=None,
                    preserve_current_followup=True,
                    conn=conn,
                )
                return None
        else:
            _merge_run_config(run_id, config_updates=config_updates, conn=conn)
        return config_updates
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
        config_updates = {
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
                "launch_claim_token": None,
                "launch_claimed_at": None,
                "launch_lease_expires_at": None,
                "failed_at": now_iso,
                "error_message": error_message,
                "retryable": retryable,
                "retryable_reason": retryable_reason,
                "failure_history": prior_failures[-5:],
            },
        }
        if launch_claim_token:
            _cas_deferred_comments_followup_state(
                run_id=run_id,
                expected_state="pending",
                followup=config_updates["deferred_comments_followup"],
                config_updates={"attached_followups": config_updates["attached_followups"]},
                expected_launch_claim_token=launch_claim_token,
                expected_launch_claimed_at=launch_claimed_at or None,
                conn=conn,
            )
        else:
            _merge_run_config(run_id, config_updates=config_updates, conn=conn)
        legacy.logger.exception(
            "Failed to auto-start deferred Instagram comments followup after run finalization: run=%s",
            run_id,
        )
        return config_updates


# bug-1: self-healing retry of deferred-comments-followup launches that failed
# transiently (e.g. DB pool saturation). A COMPLETED catalog run is never
# re-finalized, so a failed followup (state="failed", retryable=true) would
# otherwise never retry — silently skipping the comments backfill. This is a
# dedicated sweep (NOT an in-finalizer retry, which would be dead code).
# Disabled by default; enable via the env flag once the diagnostics (dg-3/dg-4)
# confirm it acts only on genuinely-stuck runs.
_DEFERRED_FOLLOWUP_RETRY_ENABLED_ENV = "SOCIAL_DEFERRED_COMMENTS_FOLLOWUP_RETRY_ENABLED"
_DEFERRED_FOLLOWUP_RETRY_MAX_ATTEMPTS = 5
_DEFERRED_FOLLOWUP_RETRY_BACKOFF_SECONDS = 600
_DEFERRED_FOLLOWUP_CLAIM_LEASE_SECONDS = 300
_DEFERRED_CHILD_CANCELLATION_CLAIM_LEASE_SECONDS = 300
_DEFERRED_CHILD_CANCELLATION_MAX_BACKOFF_SECONDS = 3600


def _deferred_comments_followup_retry_enabled() -> bool:
    return bool(legacy._env_truthy(_DEFERRED_FOLLOWUP_RETRY_ENABLED_ENV, default=False))


def _deferred_comments_followup_claim_is_stale(
    followup: dict[str, Any],
    *,
    now: datetime | None = None,
) -> bool:
    claimed_at = legacy._coerce_dt(followup.get("launch_claimed_at"))
    if claimed_at is None:
        return False
    current = now or legacy._now_utc()
    if current.tzinfo is None:
        current = current.replace(tzinfo=UTC)
    if claimed_at.tzinfo is None:
        claimed_at = claimed_at.replace(tzinfo=UTC)
    lease_expires_at = legacy._coerce_dt(followup.get("launch_lease_expires_at"))
    if lease_expires_at is not None:
        if lease_expires_at.tzinfo is None:
            lease_expires_at = lease_expires_at.replace(tzinfo=UTC)
        return lease_expires_at <= current
    return (current - claimed_at).total_seconds() >= _DEFERRED_FOLLOWUP_CLAIM_LEASE_SECONDS


def _deferred_comments_followup_claim_identity(followup: dict[str, Any]) -> tuple[str, str]:
    return (
        str(followup.get("launch_claim_token") or "").strip(),
        str(followup.get("launch_claimed_at") or "").strip(),
    )


def recover_stale_deferred_comments_followup_claims(*, limit: int = 25) -> dict[str, Any]:
    """Ungated recovery for completed parents whose launch claim lease expired.

    Re-finalization reuses the normal advisory lock and stale-claim CAS, so this
    sweep never creates a second ownership path for deferred launches.
    """
    try:
        candidates = (
            legacy.pg.fetch_all(
                """
                select id::text as run_id
                from social.scrape_runs
                where status = 'completed'
                  and config->'deferred_comments_followup'->>'state' = 'pending'
                  and nullif(config->'deferred_comments_followup'->>'launch_claimed_at', '') is not null
                  and coalesce(
                    nullif(config->'deferred_comments_followup'->>'launch_lease_expires_at', '')::timestamptz,
                    nullif(config->'deferred_comments_followup'->>'launch_claimed_at', '')::timestamptz
                      + interval '300 seconds'
                  ) <= now()
                order by completed_at asc nulls first
                limit %s
                """,
                [max(1, min(int(limit), 500))],
                pool_name=SOCIAL_CONTROL_POOL_NAME,
            )
            or []
        )
    except (legacy.pg.DatabaseServiceUnavailableError, InterfaceError, OperationalError, PoolError) as exc:
        legacy.logger.warning("[deferred_followup_claim_recovery] candidate scan deferred: %s", exc)
        return {"scanned": 0, "refinalized": 0, "failed": 0, "deferred": True}

    refinalized = 0
    failed = 0
    for row in candidates:
        run_id = str(row.get("run_id") or "").strip()
        if not run_id:
            continue
        try:
            _finalize_run_status(run_id, force_recompute=True)
        except Exception:  # noqa: BLE001 - candidates must be isolated
            failed += 1
            legacy.logger.exception("[deferred_followup_claim_recovery] re-finalize failed run=%s", run_id)
            continue
        refinalized += 1
    return {"scanned": len(candidates), "refinalized": refinalized, "failed": failed}


def _cas_deferred_comments_followup_state(
    *,
    run_id: str,
    expected_state: str,
    followup: dict[str, Any],
    config_updates: dict[str, Any] | None = None,
    expected_launch_claim_token: str | None = None,
    expected_launch_claimed_at: str | None = None,
    conn: Any | None = None,
) -> dict[str, Any] | None:
    payload = {
        "deferred_comments_followup": legacy._metadata_dict(followup),
        **legacy._metadata_dict(config_updates),
    }
    row = (
        _call_with_optional_conn(
            legacy.pg.fetch_one,
            """
            update social.scrape_runs
            set config = coalesce(config, '{}'::jsonb) || %s::jsonb
            where id = %s::uuid
              and config->'deferred_comments_followup'->>'state' = %s
              and status not in ('cancelling', 'cancelled')
              and (
                %s::text is null
                or nullif(config->'deferred_comments_followup'->>'launch_claim_token', '') = %s
              )
              and (
                %s::text is null
                or nullif(config->'deferred_comments_followup'->>'launch_claimed_at', '') = %s
              )
            returning status, config, summary
            """,
            [
                legacy._json_dumps(payload),
                run_id,
                expected_state,
                expected_launch_claim_token,
                expected_launch_claim_token,
                expected_launch_claimed_at,
                expected_launch_claimed_at,
            ],
            conn=conn,
        )
        or {}
    )
    if not row:
        return None
    legacy._invalidate_queue_status_cache()
    return {
        "status": str(row.get("status") or "").strip().lower(),
        "config": legacy._metadata_dict(row.get("config")),
        "summary": legacy._metadata_dict(row.get("summary")),
    }


def _find_recovered_deferred_comments_child(
    *,
    run_id: str,
    followup: dict[str, Any],
) -> dict[str, Any] | None:
    launch_group_id = str(followup.get("launch_group_id") or "").strip()
    existing_child_id = str(followup.get("comments_run_id") or "").strip()
    if not launch_group_id and not existing_child_id:
        return None
    platform = str(followup.get("platform") or "").strip().lower()
    account_handle = str(followup.get("account_handle") or "").strip().lower().lstrip("@")
    row = (
        legacy.pg.fetch_one(
            """
            select id::text as run_id, status, config, summary
            from social.scrape_runs
            where id <> %s::uuid
              and (
                (
                  nullif(%s, '') is not null
                  and id::text = %s
                )
                or (
                  nullif(%s, '') is not null
                  and coalesce(config->>'launch_group_id', '') = %s
                  and lower(coalesce(config->>'stage', '')) = 'instagram_comments_scrapling'
                  and lower(coalesce(config->>'platform', '')) = %s
                  and ltrim(lower(coalesce(config->>'account', '')), '@') = %s
                )
              )
            order by created_at desc
            limit 1
            """,
            [
                run_id,
                existing_child_id,
                existing_child_id,
                launch_group_id,
                launch_group_id,
                platform,
                account_handle,
            ],
            pool_name=SOCIAL_CONTROL_POOL_NAME,
        )
        or {}
    )
    child_run_id = str(row.get("run_id") or "").strip()
    if not child_run_id:
        return None
    return {
        "run_id": child_run_id,
        "status": str(row.get("status") or "").strip().lower() or "queued",
        "config": legacy._metadata_dict(row.get("config")),
        "summary": legacy._metadata_dict(row.get("summary")),
    }


def _cancel_deferred_comments_child(
    *,
    followup: dict[str, Any],
    child_run_id: str | None = None,
    cancelled_by: str | None = None,
) -> dict[str, Any] | None:
    normalized_child_run_id = str(child_run_id or followup.get("comments_run_id") or "").strip()
    if not normalized_child_run_id:
        return None
    platform = str(followup.get("platform") or "").strip()
    account_handle = str(followup.get("account_handle") or "").strip()
    try:
        return legacy.cancel_social_account_comments_run(
            platform=platform,
            account_handle=account_handle,
            run_id=normalized_child_run_id,
            cancelled_by=cancelled_by,
        )
    except LookupError:
        return {"run_id": normalized_child_run_id, "status": "not_found"}
    except Exception as exc:  # noqa: BLE001
        legacy.logger.exception(
            "[deferred_followup] failed to cancel child after parent cancellation race child_run=%s",
            normalized_child_run_id,
        )
        return {"run_id": normalized_child_run_id, "status": "cancel_failed", "error": str(exc)}


def _deferred_child_cancellation_backoff_seconds(attempt_count: int) -> int:
    return min(
        _DEFERRED_CHILD_CANCELLATION_MAX_BACKOFF_SECONDS,
        30 * (2 ** max(0, min(int(attempt_count), 8) - 1)),
    )


def _persist_deferred_child_cancellation_intent(
    *,
    run_id: str,
    followup: dict[str, Any],
    child_run_id: str,
    cancelled_by: str | None,
    cancel_reason: str,
    attached_followups: dict[str, Any] | None = None,
    preserve_current_followup: bool = False,
    conn: Any | None = None,
) -> dict[str, Any]:
    """Durably record the exact child and cancellation intent before I/O."""
    now_iso = legacy._iso(legacy._now_utc())
    prior = legacy._metadata_dict(followup.get("child_cancellation"))
    cancellation = {
        **prior,
        "state": "pending",
        "child_run_id": child_run_id,
        "intent_at": str(prior.get("intent_at") or now_iso),
        "updated_at": now_iso,
        "attempt_count": legacy._normalize_non_negative_int(prior.get("attempt_count")) + 1,
        "last_attempt_at": now_iso,
        "cancelled_by": cancelled_by,
        "cancel_reason": cancel_reason,
        "claim_token": None,
        "claimed_at": None,
        "claim_lease_expires_at": None,
        "next_attempt_at": None,
        "last_error": None,
    }
    persisted_followup = {
        **followup,
        "state": "cancelled",
        "launch_claim_token": None,
        "launch_claimed_at": None,
        "launch_lease_expires_at": None,
        "cancelled_at": str(followup.get("cancelled_at") or now_iso),
        "cancelled_by": cancelled_by,
        "cancel_reason": cancel_reason,
        "child_cancellation": cancellation,
    }
    updates: dict[str, Any] = {"deferred_comments_followup": persisted_followup}
    if attached_followups is not None:
        updates["attached_followups"] = {
            **attached_followups,
            "comments": legacy._build_attached_comments_followup(
                run_id=child_run_id,
                status="cancelling",
                source="deferred_after_catalog",
                state="cancelling",
            ),
        }
    if preserve_current_followup:
        row = _call_with_optional_conn(
            legacy.pg.fetch_one,
            """
            update social.scrape_runs
            set config = jsonb_set(
              coalesce(config, '{}'::jsonb),
              '{deferred_comments_followup}',
              coalesce(config->'deferred_comments_followup', '{}'::jsonb) || %s::jsonb,
              true
            )
            where id = %s::uuid
            returning id::text
            """,
            [legacy._json_dumps({"child_cancellation": cancellation}), run_id],
            conn=conn,
        )
        if not row:
            raise RuntimeError(f"Failed to persist deferred child cancellation intent for run {run_id}")
        legacy._invalidate_queue_status_cache()
    else:
        _merge_run_config(run_id, config_updates=updates, conn=conn)
    return persisted_followup


def _persist_deferred_child_cancellation_outcome(
    *,
    run_id: str,
    followup: dict[str, Any],
    outcome: dict[str, Any] | None,
    attached_followups: dict[str, Any] | None = None,
    expected_claim_token: str | None = None,
    preserve_current_followup: bool = False,
    conn: Any | None = None,
) -> bool:
    cancellation = legacy._metadata_dict(followup.get("child_cancellation"))
    child_run_id = str(cancellation.get("child_run_id") or "").strip()
    status = str((outcome or {}).get("status") or "").strip().lower()
    now = legacy._now_utc()
    now_iso = legacy._iso(now)
    terminal_state = status if status in {"cancelled", "not_found"} else None
    if terminal_state:
        updated_cancellation = {
            **cancellation,
            "state": terminal_state,
            "completed_at": now_iso,
            "updated_at": now_iso,
            "claim_token": None,
            "claimed_at": None,
            "claim_lease_expires_at": None,
            "next_attempt_at": None,
            "last_error": None,
        }
    else:
        attempts = max(1, legacy._normalize_non_negative_int(cancellation.get("attempt_count")))
        error = str((outcome or {}).get("error") or f"non-terminal cancellation outcome: {status or 'unknown'}")
        updated_cancellation = {
            **cancellation,
            "state": "retryable",
            "updated_at": now_iso,
            "last_error": error,
            "last_error_at": now_iso,
            "claim_token": None,
            "claimed_at": None,
            "claim_lease_expires_at": None,
            "next_attempt_at": legacy._iso(
                now + timedelta(seconds=_deferred_child_cancellation_backoff_seconds(attempts))
            ),
        }
    persisted_followup = {**followup, "child_cancellation": updated_cancellation}
    updates: dict[str, Any] = {"deferred_comments_followup": persisted_followup}
    attached_comments = legacy._metadata_dict(
        legacy._normalize_attached_followups(attached_followups).get("comments")
        if attached_followups is not None
        else None
    )
    attached_child_run_id = str(attached_comments.get("run_id") or "").strip()
    if terminal_state and attached_followups is not None and attached_child_run_id == child_run_id:
        updates["attached_followups"] = {
            **attached_followups,
            "comments": legacy._build_attached_comments_followup(
                run_id=child_run_id,
                status=terminal_state,
                source="deferred_after_catalog",
                state=terminal_state,
            ),
        }
    if preserve_current_followup and expected_claim_token is None:
        row = _call_with_optional_conn(
            legacy.pg.fetch_one,
            """
            update social.scrape_runs
            set config = jsonb_set(
              coalesce(config, '{}'::jsonb),
              '{deferred_comments_followup}',
              coalesce(config->'deferred_comments_followup', '{}'::jsonb) || %s::jsonb,
              true
            )
            where id = %s::uuid
            returning id::text
            """,
            [legacy._json_dumps({"child_cancellation": updated_cancellation}), run_id],
            conn=conn,
        )
        if row:
            legacy._invalidate_queue_status_cache()
        return bool(row)
    if expected_claim_token is None:
        _merge_run_config(run_id, config_updates=updates, conn=conn)
        return True
    row = _call_with_optional_conn(
        legacy.pg.fetch_one,
        """
        update social.scrape_runs
        set config = coalesce(config, '{}'::jsonb) || %s::jsonb
        where id = %s::uuid
          and config #>> '{deferred_comments_followup,child_cancellation,claim_token}' = %s
        returning id::text
        """,
        [legacy._json_dumps(updates), run_id, expected_claim_token],
        conn=conn,
    )
    if row:
        legacy._invalidate_queue_status_cache()
    return bool(row)


def _cancel_deferred_comments_child_durably(
    *,
    run_id: str,
    followup: dict[str, Any],
    child_run_id: str,
    cancelled_by: str | None,
    cancel_reason: str,
    attached_followups: dict[str, Any] | None = None,
    preserve_current_followup: bool = False,
    conn: Any | None = None,
) -> dict[str, Any] | None:
    persisted = _persist_deferred_child_cancellation_intent(
        run_id=run_id,
        followup=followup,
        child_run_id=child_run_id,
        cancelled_by=cancelled_by,
        cancel_reason=cancel_reason,
        attached_followups=attached_followups,
        preserve_current_followup=preserve_current_followup,
        conn=conn,
    )
    if conn is not None:
        # The caller owns this transaction. Do not perform an external cancel
        # before its durable intent commit; the ungated recovery sweep drains it
        # immediately after commit instead.
        return {"run_id": child_run_id, "status": "pending"}
    outcome = _cancel_deferred_comments_child(
        followup=persisted,
        child_run_id=child_run_id,
        cancelled_by=cancelled_by,
    )
    _persist_deferred_child_cancellation_outcome(
        run_id=run_id,
        followup=persisted,
        outcome=outcome,
        attached_followups=attached_followups,
        preserve_current_followup=preserve_current_followup,
        conn=conn,
    )
    return outcome


def cancel_deferred_comments_followup(
    run_id: str,
    *,
    cancelled_by: str | None = None,
    conn: Any | None = None,
) -> dict[str, Any]:
    """Close a claimed/started deferred followup after its parent is cancelled.

    The parent status transition happens in the caller. Once that commit exists,
    the launcher CAS below cannot attach a new child. Any child already attached is
    cancelled here without holding the parent finalization lock.
    """
    current = (
        _call_with_optional_conn(
            legacy.pg.fetch_one,
            "select status, config from social.scrape_runs where id = %s::uuid",
            [run_id],
            conn=conn,
        )
        or {}
    )
    current_config = legacy._metadata_dict(current.get("config"))
    followup = legacy._metadata_dict(current_config.get("deferred_comments_followup"))
    if not followup:
        return {"cancelled": False, "child_run_id": None, "child_cancellation": None}

    child_run_id = str(followup.get("comments_run_id") or "").strip() or None
    cancelled_at = legacy._iso(legacy._now_utc())
    cancelled_followup = {**followup, "cancelled_at": cancelled_at}
    attached_followups = legacy._normalize_attached_followups(current_config.get("attached_followups"))
    if child_run_id:
        child_cancellation = _cancel_deferred_comments_child_durably(
            run_id=run_id,
            followup=cancelled_followup,
            child_run_id=child_run_id,
            cancelled_by=cancelled_by,
            cancel_reason="parent_run_cancelled",
            attached_followups=attached_followups,
            conn=conn,
        )
    else:
        child_cancellation = None
        _merge_run_config(
            run_id,
            config_updates={
                "deferred_comments_followup": {
                    **cancelled_followup,
                    "state": "cancelled",
                    "launch_claim_token": None,
                    "launch_claimed_at": None,
                    "launch_lease_expires_at": None,
                    "cancelled_by": cancelled_by,
                    "cancel_reason": "parent_run_cancelled",
                }
            },
            conn=conn,
        )
    return {
        "cancelled": True,
        "child_run_id": child_run_id,
        "child_cancellation": child_cancellation,
    }


def _claim_deferred_child_cancellation(
    *,
    run_id: str,
    followup: dict[str, Any],
) -> dict[str, Any] | None:
    cancellation = legacy._metadata_dict(followup.get("child_cancellation"))
    state = str(cancellation.get("state") or "").strip().lower()
    child_run_id = str(cancellation.get("child_run_id") or "").strip()
    if state not in {"pending", "retryable", "claimed"} or not child_run_id:
        return None
    now = legacy._now_utc()
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    if state == "retryable":
        next_attempt_at = legacy._coerce_dt(cancellation.get("next_attempt_at"))
        if next_attempt_at is not None:
            if next_attempt_at.tzinfo is None:
                next_attempt_at = next_attempt_at.replace(tzinfo=UTC)
            if next_attempt_at > now:
                return None
    if state == "claimed":
        lease_expires_at = legacy._coerce_dt(cancellation.get("claim_lease_expires_at"))
        if lease_expires_at is not None:
            if lease_expires_at.tzinfo is None:
                lease_expires_at = lease_expires_at.replace(tzinfo=UTC)
            if lease_expires_at > now:
                return None

    claim_token = str(legacy.uuid4())
    claimed_at = legacy._iso(now)
    claimed = {
        **cancellation,
        "state": "claimed",
        "claim_token": claim_token,
        "claimed_at": claimed_at,
        "claim_lease_expires_at": legacy._iso(
            now + timedelta(seconds=_DEFERRED_CHILD_CANCELLATION_CLAIM_LEASE_SECONDS)
        ),
        "attempt_count": legacy._normalize_non_negative_int(cancellation.get("attempt_count")) + 1,
        "last_attempt_at": claimed_at,
    }
    claimed_followup = {**followup, "child_cancellation": claimed}
    row = (
        legacy.pg.fetch_one(
            """
            update social.scrape_runs
            set config = coalesce(config, '{}'::jsonb) || %s::jsonb
            where id = %s::uuid
              and config #>> '{deferred_comments_followup,child_cancellation,state}' = %s
              and config #>> '{deferred_comments_followup,child_cancellation,child_run_id}' = %s
              and coalesce(config #>> '{deferred_comments_followup,child_cancellation,claim_token}', '') = %s
              and (
                %s <> 'retryable'
                or nullif(config #>> '{deferred_comments_followup,child_cancellation,next_attempt_at}', '') is null
                or (config #>> '{deferred_comments_followup,child_cancellation,next_attempt_at}')::timestamptz <= now()
              )
              and (
                %s <> 'claimed'
                or nullif(
                  config #>> '{deferred_comments_followup,child_cancellation,claim_lease_expires_at}',
                  ''
                ) is null
                or (
                  config #>> '{deferred_comments_followup,child_cancellation,claim_lease_expires_at}'
                )::timestamptz <= now()
              )
            returning config
            """,
            [
                legacy._json_dumps({"deferred_comments_followup": claimed_followup}),
                run_id,
                state,
                child_run_id,
                str(cancellation.get("claim_token") or ""),
                state,
                state,
            ],
            pool_name=SOCIAL_CONTROL_POOL_NAME,
        )
        or {}
    )
    if not row:
        return None
    legacy._invalidate_queue_status_cache()
    config = legacy._metadata_dict(row.get("config"))
    return {
        "run_id": run_id,
        "claim_token": claim_token,
        "followup": legacy._metadata_dict(config.get("deferred_comments_followup")) or claimed_followup,
        "attached_followups": legacy._normalize_attached_followups(config.get("attached_followups")),
    }


def recover_deferred_comments_child_cancellations(*, limit: int = 25) -> dict[str, Any]:
    """Drain any committed due child-cancellation intent without holding locks.

    Parent status is intentionally not a selector: attach-CAS loss can leave an
    orphan intent on a completed parent when a newer launch claimant won.
    """
    safe_limit = max(1, min(int(limit), 500))
    try:
        candidates = (
            legacy.pg.fetch_all(
                """
                select id::text as run_id, config
                from social.scrape_runs
                where config #>> '{deferred_comments_followup,child_cancellation,child_run_id}' <> ''
                  and (
                    config #>> '{deferred_comments_followup,child_cancellation,state}' = 'pending'
                    or (
                      config #>> '{deferred_comments_followup,child_cancellation,state}' = 'retryable'
                      and (
                        nullif(config #>> '{deferred_comments_followup,child_cancellation,next_attempt_at}', '') is null
                        or (
                          config #>> '{deferred_comments_followup,child_cancellation,next_attempt_at}'
                        )::timestamptz <= now()
                      )
                    )
                    or (
                      config #>> '{deferred_comments_followup,child_cancellation,state}' = 'claimed'
                      and (
                        nullif(
                          config #>> '{deferred_comments_followup,child_cancellation,claim_lease_expires_at}',
                          ''
                        ) is null
                        or (
                          config #>> '{deferred_comments_followup,child_cancellation,claim_lease_expires_at}'
                        )::timestamptz <= now()
                      )
                    )
                  )
                order by coalesce(cancelled_at, completed_at, created_at) asc nulls first
                limit %s
                """,
                [safe_limit],
                pool_name=SOCIAL_CONTROL_POOL_NAME,
            )
            or []
        )
    except (legacy.pg.DatabaseServiceUnavailableError, InterfaceError, OperationalError, PoolError) as exc:
        legacy.logger.warning("[deferred_child_cancellation] candidate scan deferred: %s", exc)
        return {
            "scanned": 0,
            "claimed": 0,
            "cancelled": 0,
            "not_found": 0,
            "retryable": 0,
            "skipped": 0,
            "deferred": True,
        }

    summary = {
        "scanned": len(candidates),
        "claimed": 0,
        "cancelled": 0,
        "not_found": 0,
        "retryable": 0,
        "skipped": 0,
    }
    for row in candidates:
        run_id = str(row.get("run_id") or "").strip()
        config = legacy._metadata_dict(row.get("config"))
        followup = legacy._metadata_dict(config.get("deferred_comments_followup"))
        if not run_id:
            summary["skipped"] += 1
            continue
        try:
            claim = _claim_deferred_child_cancellation(run_id=run_id, followup=followup)
        except Exception:  # noqa: BLE001 - one corrupt/colliding candidate must not stop the sweep
            summary["skipped"] += 1
            legacy.logger.exception("[deferred_child_cancellation] claim failed parent_run=%s", run_id)
            continue
        if claim is None:
            summary["skipped"] += 1
            continue
        summary["claimed"] += 1
        claimed_followup = legacy._metadata_dict(claim.get("followup"))
        cancellation = legacy._metadata_dict(claimed_followup.get("child_cancellation"))
        child_run_id = str(cancellation.get("child_run_id") or "").strip()
        outcome = _cancel_deferred_comments_child(
            followup=claimed_followup,
            child_run_id=child_run_id,
            cancelled_by=str(cancellation.get("cancelled_by") or "").strip() or None,
        )
        outcome_status = str((outcome or {}).get("status") or "").strip().lower()
        try:
            persisted = _persist_deferred_child_cancellation_outcome(
                run_id=run_id,
                followup=claimed_followup,
                outcome=outcome,
                attached_followups=legacy._normalize_attached_followups(claim.get("attached_followups")),
                expected_claim_token=str(claim.get("claim_token") or ""),
            )
        except Exception:  # noqa: BLE001 - expired claim makes persistence failure retryable
            persisted = False
            legacy.logger.exception("[deferred_child_cancellation] outcome persist failed parent_run=%s", run_id)
        if not persisted:
            summary["skipped"] += 1
        elif outcome_status in {"cancelled", "not_found"}:
            summary[outcome_status] += 1
        else:
            summary["retryable"] += 1
    return summary


def _claim_deferred_comments_followup_locked(
    *,
    run_id: str,
    run_config: dict[str, Any],
    conn: Any,
) -> dict[str, Any] | None:
    """Atomically reserve a pending followup before releasing the finalize lock."""
    followup = legacy._metadata_dict(run_config.get("deferred_comments_followup"))
    if str(followup.get("state") or "").strip().lower() != "pending":
        return None
    existing_claimed_at = str(followup.get("launch_claimed_at") or "").strip()
    claim_reclaimed = bool(existing_claimed_at and _deferred_comments_followup_claim_is_stale(followup))
    if existing_claimed_at and not claim_reclaimed:
        return None
    claimed_at_dt = legacy._now_utc()
    claimed_at = legacy._iso(claimed_at_dt)
    launch_claim_token = str(legacy.uuid4())
    lease_expires_at = legacy._iso(claimed_at_dt + timedelta(seconds=_DEFERRED_FOLLOWUP_CLAIM_LEASE_SECONDS))
    claimed_followup = {
        **followup,
        "launch_claim_token": launch_claim_token,
        "launch_claimed_at": claimed_at,
        "launch_lease_expires_at": lease_expires_at,
    }
    if claim_reclaimed:
        claimed_followup.update(
            {
                "launch_recovered_at": claimed_at,
                "launch_recovered_from_token": str(followup.get("launch_claim_token") or "").strip() or None,
                "launch_recovery_count": (
                    legacy._normalize_non_negative_int(followup.get("launch_recovery_count")) or 0
                )
                + 1,
            }
        )
    row = (
        _call_with_optional_conn(
            legacy.pg.fetch_one,
            """
            update social.scrape_runs
            set config = coalesce(config, '{}'::jsonb) || %s::jsonb
            where id = %s::uuid
              and status in ('completed', 'queued', 'running')
              and config->'deferred_comments_followup'->>'state' = 'pending'
              and (
                nullif(config->'deferred_comments_followup'->>'launch_claimed_at', '') is null
                or coalesce(
                  nullif(config->'deferred_comments_followup'->>'launch_lease_expires_at', '')::timestamptz,
                  nullif(config->'deferred_comments_followup'->>'launch_claimed_at', '')::timestamptz
                    + interval '300 seconds'
                ) <= now()
              )
            returning status, config, summary
            """,
            [legacy._json_dumps({"deferred_comments_followup": claimed_followup}), run_id],
            conn=conn,
        )
        or {}
    )
    if not row:
        return None
    legacy._invalidate_queue_status_cache()
    return {
        "status": str(row.get("status") or "").strip().lower(),
        "config": legacy._metadata_dict(row.get("config")),
        "summary": legacy._metadata_dict(row.get("summary")),
        "launch_claimed_at": claimed_at,
        "launch_claim_token": launch_claim_token,
        "launch_lease_expires_at": lease_expires_at,
        "launch_reclaimed": claim_reclaimed,
    }


def _restore_deferred_comments_followup_failed_after_skipped_retry(
    *,
    run_id: str,
    followup: dict[str, Any],
    reason: str,
) -> None:
    now_iso = legacy._iso(legacy._now_utc())
    error_message = "Deferred comments follow-up retry did not start."
    prior_failures = [dict(item) for item in list(followup.get("failure_history") or []) if isinstance(item, dict)]
    prior_failures.append(
        {
            "failed_at": now_iso,
            "error_message": error_message,
            "retryable": True,
            "retryable_reason": reason,
        }
    )
    restored = {
        **followup,
        "state": "failed",
        "launch_claimed_at": None,
        "failed_at": now_iso,
        "error_message": error_message,
        "retryable": True,
        "retryable_reason": reason,
        "failure_history": prior_failures[-5:],
    }
    _cas_deferred_comments_followup_state(
        run_id=run_id,
        expected_state="pending",
        followup=restored,
        conn=None,
    )


def _retry_deferred_comments_followup_locked(*, run_id: str) -> str:
    """Reserve one failed followup retry under the run-finalize lock, then launch.

    Returns "retried", "exhausted", or "skipped". Uses the SAME advisory lock key
    as _finalize_run_status so a concurrent finalize cannot clobber the jsonb
    config write, and re-checks state under the lock to avoid double-firing. The
    nested comments launch runs only after the advisory lock is released.
    """

    lock_key = int(legacy.hashlib.md5(run_id.encode()).hexdigest()[:15], 16) % (2**31)
    launch_payload: dict[str, Any] | None = None
    repended_followup: dict[str, Any] | None = None
    try:
        with legacy.pg.advisory_session_lock(
            lock_key,
            label="run-finalize-lock",
            pool_name="session_control",
        ) as lock_conn:
            run_row = legacy.pg.fetch_one(
                "select id::text as run_id, status, config, summary from social.scrape_runs where id = %s",
                [run_id],
                conn=lock_conn,
            )
            if not run_row:
                return "skipped"
            run_status = str(run_row.get("status") or "").strip().lower()
            run_config = legacy._metadata_dict(run_row.get("config"))
            summary = legacy._metadata_dict(run_row.get("summary"))
            followup = legacy._metadata_dict(run_config.get("deferred_comments_followup"))
            # Re-check under the lock — another worker may have already retried it.
            if str(followup.get("state") or "").strip().lower() != "failed":
                return "skipped"
            attempts = legacy._normalize_non_negative_int(followup.get("retry_attempts")) or 0
            if attempts >= _DEFERRED_FOLLOWUP_RETRY_MAX_ATTEMPTS:
                _merge_run_config(
                    run_id,
                    config_updates={"deferred_comments_followup": {**followup, "state": "failed_exhausted"}},
                    conn=lock_conn,
                )
                return "exhausted"
            repended = {
                **followup,
                "state": "pending",
                "launch_claim_token": str(legacy.uuid4()),
                "launch_claimed_at": legacy._iso(legacy._now_utc()),
                "launch_lease_expires_at": legacy._iso(
                    legacy._now_utc() + timedelta(seconds=_DEFERRED_FOLLOWUP_CLAIM_LEASE_SECONDS)
                ),
                "retry_attempts": attempts + 1,
                "last_retry_at": legacy._iso(legacy._now_utc()),
                # Clear stale failure fields so the new attempt starts clean. Shallow
                # merge keeps failure_history intact for the audit trail.
                "error_message": None,
                "failed_at": None,
                "retryable_reason": None,
            }
            updated = _cas_deferred_comments_followup_state(
                run_id=run_id,
                expected_state="failed",
                followup=repended,
                conn=lock_conn,
            )
            if updated is None:
                return "skipped"
            repended_followup = repended
            launch_payload = {
                "run_status": str(updated.get("status") or run_status).strip().lower(),
                "run_config": legacy._metadata_dict(updated.get("config"))
                or {**run_config, "deferred_comments_followup": repended},
                "summary": legacy._metadata_dict(updated.get("summary")) or summary,
            }
    except legacy.pg.AdvisoryLockUnavailable:
        return "skipped"
    except (legacy.pg.DatabaseServiceUnavailableError, InterfaceError, OperationalError, PoolError):
        return "skipped"
    if launch_payload is None or repended_followup is None:
        return "skipped"
    result = _maybe_start_deferred_comments_followup(
        run_id=run_id,
        run_status=str(launch_payload.get("run_status") or ""),
        run_config=legacy._metadata_dict(launch_payload.get("run_config")),
        summary=legacy._metadata_dict(launch_payload.get("summary")),
        conn=None,
    )
    if result and result.get("_deferred_followup_parent_cancelled"):
        return "skipped"
    if result is not None:
        return "retried"
    _restore_deferred_comments_followup_failed_after_skipped_retry(
        run_id=run_id,
        followup=repended_followup,
        reason="deferred_retry_launch_skipped",
    )
    return "skipped"


def recover_failed_deferred_comments_followups(*, limit: int = 25) -> dict[str, Any]:
    """bug-1 sweep: re-attempt deferred-comments-followup launches that failed
    with a retryable error and have not exhausted their retry budget.

    Disabled by default behind _DEFERRED_FOLLOWUP_RETRY_ENABLED_ENV. Enforces a
    mandatory backoff (via failed_at) and a hard retry_attempts cap so it cannot
    hammer the database under sustained pool saturation. ALREADY_ACTIVE reuse in
    the launcher makes a double-fire reuse the existing comments run rather than
    duplicate it.
    """

    if not _deferred_comments_followup_retry_enabled():
        return {"enabled": False, "scanned": 0, "retried": 0, "exhausted": 0, "skipped": 0}

    try:
        candidates = (
            legacy.pg.fetch_all(
                """
                select id::text as run_id, config
                from social.scrape_runs
                where status = 'completed'
                  and config->'deferred_comments_followup'->>'state' = 'failed'
                  and coalesce((config->'deferred_comments_followup'->>'retryable')::boolean, false) = true
                order by completed_at desc nulls last
                limit %s
                """,
                [max(1, int(limit))],
                pool_name=SOCIAL_CONTROL_POOL_NAME,
            )
            or []
        )
    except (legacy.pg.DatabaseServiceUnavailableError, InterfaceError, OperationalError, PoolError) as exc:
        legacy.logger.warning("[deferred_followup_retry] candidate scan deferred: %s", exc)
        return {"enabled": True, "scanned": 0, "retried": 0, "exhausted": 0, "skipped": 0, "deferred": True}

    retried = 0
    exhausted = 0
    skipped = 0
    now = legacy._now_utc()
    for row in candidates:
        run_id = str(row.get("run_id") or "").strip()
        if not run_id:
            continue
        followup = legacy._metadata_dict(legacy._metadata_dict(row.get("config")).get("deferred_comments_followup"))
        # Mandatory backoff: skip runs whose last failure is too recent so the
        # sweep cannot worsen pool pressure by re-launching every tick.
        last_failed = legacy._coerce_dt(followup.get("failed_at"))
        if last_failed is not None and (now - last_failed).total_seconds() < _DEFERRED_FOLLOWUP_RETRY_BACKOFF_SECONDS:
            skipped += 1
            continue
        outcome = _retry_deferred_comments_followup_locked(run_id=run_id)
        if outcome == "retried":
            retried += 1
        elif outcome == "exhausted":
            exhausted += 1
        else:
            skipped += 1
    return {
        "enabled": True,
        "scanned": len(candidates),
        "retried": retried,
        "exhausted": exhausted,
        "skipped": skipped,
    }


def recover_catalog_run_deadline_exceeded_jobs(*, limit: int = 25) -> dict[str, Any]:
    """Fail queued catalog jobs for runs whose operator deadline has elapsed."""
    safe_limit = max(1, min(int(limit), 500))
    try:
        candidates = (
            legacy.pg.fetch_all(
                """
                select id::text as run_id, status, config
                from social.scrape_runs
                where status in ('queued', 'pending', 'retrying', 'running')
                  and coalesce(config->>'pipeline_ingest_mode', '') = %s
                  and nullif(config->>'catalog_run_deadline_at', '') is not null
                order by started_at asc nulls first, created_at asc
                limit %s
                """,
                [legacy.SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE, safe_limit],
                pool_name=SOCIAL_CONTROL_POOL_NAME,
            )
            or []
        )
    except (legacy.pg.DatabaseServiceUnavailableError, InterfaceError, OperationalError, PoolError) as exc:
        legacy.logger.warning("[catalog_run_deadline] candidate scan deferred: %s", exc)
        return {
            "scanned": 0,
            "expired_runs": 0,
            "failed_jobs": 0,
            "finalized_runs": 0,
            "affected_run_ids": [],
            "deferred": True,
        }

    now = legacy._now_utc()
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    failed_rows: list[dict[str, Any]] = []
    affected_run_ids: set[str] = set()
    expired_runs = 0
    skipped_invalid_deadline = 0
    for row in candidates:
        run_id = str(row.get("run_id") or "").strip()
        config = legacy._metadata_dict(row.get("config"))
        deadline_at = legacy._coerce_dt(config.get("catalog_run_deadline_at"))
        if not run_id or deadline_at is None:
            skipped_invalid_deadline += 1
            continue
        if deadline_at.tzinfo is None:
            deadline_at = deadline_at.replace(tzinfo=UTC)
        if deadline_at > now:
            continue
        expired_runs += 1
        deadline_seconds = legacy._normalize_non_negative_int(config.get("catalog_run_deadline_seconds")) or None
        rows = (
            legacy.pg.fetch_all(
                """
                update social.scrape_jobs j
                set
                  status = 'failed',
                  error_message = 'Catalog run deadline exceeded before queued job dispatch.',
                  completed_at = now(),
                  heartbeat_at = now(),
                  last_error_code = 'run_deadline_exceeded',
                  last_error_class = 'CatalogRunDeadlineExceeded',
                  metadata = coalesce(j.metadata, '{}'::jsonb) || jsonb_build_object(
                    'retryable', false,
                    'job_error_code', 'run_deadline_exceeded',
                    'run_deadline', jsonb_build_object(
                      'source', 'catalog_run_deadline_sweep',
                      'deadline_at', %s,
                      'deadline_seconds', %s,
                      'exceeded_at', %s,
                      'prior_status', j.status,
                      'run_status', %s
                    )
                  )
                where j.run_id = %s::uuid
                  and j.status in ('queued', 'pending', 'retrying')
                returning
                  j.id::text as id,
                  j.run_id::text as run_id,
                  j.platform,
                  coalesce(j.config->>'stage', j.metadata->>'stage', j.job_type, 'unknown') as stage,
                  j.status
                """,
                [
                    legacy._iso(deadline_at),
                    deadline_seconds,
                    legacy._iso(now),
                    str(row.get("status") or "").strip().lower() or None,
                    run_id,
                ],
                pool_name=SOCIAL_CONTROL_POOL_NAME,
            )
            or []
        )
        if not rows:
            continue
        failed_rows.extend(rows)
        affected_run_ids.add(run_id)

    if failed_rows:
        legacy._invalidate_queue_status_cache()
    finalized_runs = 0
    for run_id in sorted(affected_run_ids):
        try:
            _finalize_run_status(run_id, force_recompute=True)
        except Exception:  # noqa: BLE001
            legacy.logger.exception("[catalog_run_deadline] re-finalize failed run=%s", run_id)
            continue
        finalized_runs += 1
    return {
        "scanned": len(candidates),
        "expired_runs": expired_runs,
        "failed_jobs": len(failed_rows),
        "finalized_runs": finalized_runs,
        "affected_run_ids": sorted(affected_run_ids),
        "skipped_invalid_deadline": skipped_invalid_deadline,
    }


def _clear_run_finalize_pending_marker(run_id: str) -> None:
    """B3: clear the finalize_pending marker once a stuck run has been re-finalized."""
    normalized = str(run_id or "").strip()
    if not normalized:
        return
    try:
        _merge_run_config(
            normalized,
            config_updates={
                "finalize_pending": False,
                "finalize_cleared_at": legacy._iso(legacy._now_utc()),
            },
        )
    except Exception:  # noqa: BLE001
        legacy.logger.debug("[unfinalized_terminal_runs] could not clear finalize_pending run=%s", normalized[:8])


def recover_unfinalized_terminal_runs(*, limit: int = 25) -> dict[str, Any]:
    """B3 sweep: re-finalize runs whose jobs are all terminal but whose run status is
    still active. Closes the gap where _finish_job's finalize raised or deferred (e.g.
    under DB pool saturation) and left the parent run non-terminal indefinitely.

    Structural detector — does NOT depend on the finalize_pending marker (which may be
    absent if the marker write itself failed): run status active, at least one job, and
    no job still in an active state. _finalize_run_status is idempotent, so re-running it
    on an already-correct run is harmless.
    """
    try:
        candidates = (
            legacy.pg.fetch_all(
                """
                select r.id::text as run_id, r.status
                from social.scrape_runs r
                where r.status in ('queued', 'pending', 'retrying', 'running')
                  and exists (select 1 from social.scrape_jobs j where j.run_id = r.id)
                  and not exists (
                    select 1
                    from social.scrape_jobs j
                    where j.run_id = r.id
                      and j.status in ('queued', 'pending', 'retrying', 'running', 'cancelling')
                  )
                order by r.started_at desc nulls last
                limit %s
                """,
                [max(1, int(limit))],
                pool_name=SOCIAL_CONTROL_POOL_NAME,
            )
            or []
        )
    except (legacy.pg.DatabaseServiceUnavailableError, InterfaceError, OperationalError, PoolError) as exc:
        legacy.logger.warning("[unfinalized_terminal_runs] candidate scan deferred: %s", exc)
        return {"scanned": 0, "finalized": 0, "deferred": True}

    finalized = 0
    for row in candidates:
        run_id = str(row.get("run_id") or "").strip()
        if not run_id:
            continue
        try:
            _finalize_run_status(run_id, force_recompute=True)
        except Exception:  # noqa: BLE001
            legacy.logger.exception("[unfinalized_terminal_runs] re-finalize failed run=%s", run_id)
            continue
        finalized += 1
        _clear_run_finalize_pending_marker(run_id)
    return {"scanned": len(candidates), "finalized": finalized}


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
    "comments_followup",
    "comments_deferred_until_catalog_complete",
    "deferred_comments_followup",
    "attached_followups",
    "guarded_restart",
    "guarded_restart_from_run_id",
    "guarded_restart_to_run_id",
    "public_blocked_pause",
    "dispatch_control",
    "stalled_frontier_recovery",
    "stalled_frontier_recoveries",
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


def _comments_followup_summary_from_config(config: Any) -> dict[str, Any] | None:
    run_config = legacy._metadata_dict(config)
    attached_followups = legacy._metadata_dict(run_config.get("attached_followups"))
    attached_comments = legacy._metadata_dict(attached_followups.get("comments"))
    deferred = legacy._metadata_dict(run_config.get("deferred_comments_followup"))
    if attached_comments:
        payload = dict(attached_comments)
        if deferred:
            deferred_state = str(deferred.get("state") or "").strip().lower() or None
            if deferred_state == "pending":
                payload.setdefault("deferred_until", "catalog_complete")
            else:
                payload.pop("deferred_until", None)
            payload.setdefault("deferred_state", deferred_state)
            payload.setdefault("account_handle", deferred.get("account_handle"))
            payload.setdefault("platform", deferred.get("platform"))
        return payload
    if not deferred:
        return None
    state = str(deferred.get("state") or "pending").strip().lower() or "pending"
    return {
        "state": state,
        "status": "deferred" if state == "pending" else state,
        "source": "deferred_after_catalog",
        "deferred_until": "catalog_complete",
        "platform": str(deferred.get("platform") or "").strip().lower() or None,
        "account_handle": str(deferred.get("account_handle") or "").strip().lower().lstrip("@") or None,
        "source_scope": str(deferred.get("source_scope") or "").strip().lower() or None,
        "refresh_policy": str(deferred.get("refresh_policy") or "").strip() or None,
        "target_filter": str(deferred.get("target_filter") or "").strip() or None,
        "comments_run_id": str(deferred.get("comments_run_id") or "").strip() or None,
        "comments_enable_media_followups": bool(deferred.get("comments_enable_media_followups")),
        "pending_reason": "waiting_for_catalog_completion" if state == "pending" else None,
    }


def _apply_run_config_summary_fields(summary: dict[str, Any], config: Any) -> dict[str, Any]:
    run_config = legacy._metadata_dict(config)
    comments_followup = _comments_followup_summary_from_config(run_config)
    if comments_followup:
        deferred = legacy._metadata_dict(run_config.get("deferred_comments_followup"))
        deferred_state = str(deferred.get("state") or comments_followup.get("deferred_state") or "").strip().lower()
        summary["comments_followup"] = comments_followup
        summary["comments_deferred_until_catalog_complete"] = (
            str(comments_followup.get("deferred_until") or "").strip().lower() == "catalog_complete"
            and deferred_state == "pending"
        )
    if run_config.get("deferred_comments_followup") is not None:
        summary["deferred_comments_followup"] = legacy._metadata_dict(run_config.get("deferred_comments_followup"))
    if run_config.get("attached_followups") is not None:
        summary["attached_followups"] = legacy._metadata_dict(run_config.get("attached_followups"))
    if run_config.get("dispatch_control") is not None:
        summary["dispatch_control"] = legacy._metadata_dict(run_config.get("dispatch_control"))
    return summary


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
                "select summary, config from social.scrape_runs where id = %s",
                [run_id],
            )
            or {}
        )
        _apply_run_config_summary_fields(summary, existing_row.get("config"))
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
    _increment_run_counters_on_job_create_batch(
        run_id=run_id,
        stage=stage,
        status=status,
        count=1,
        conn=conn,
    )


def _increment_run_counters_on_job_create_batch(
    *,
    run_id: str,
    stage: str,
    status: str,
    count: int,
    conn: Any | None = None,
) -> None:
    count = legacy._normalize_non_negative_int(count)
    if count <= 0:
        return
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
                count=count,
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
                count=count,
            )


def _persist_incremented_run_create_counters(
    *,
    conn: Any,
    run_id: str,
    row: dict[str, Any],
    stage_key: str,
    status: str,
    count: int = 1,
) -> None:
    if not row:
        return
    count = max(1, legacy._normalize_non_negative_int(count))
    total_jobs = legacy._normalize_non_negative_int(row.get("total_jobs")) + count
    completed_jobs = legacy._normalize_non_negative_int(row.get("completed_jobs"))
    failed_jobs = legacy._normalize_non_negative_int(row.get("failed_jobs"))
    active_jobs = legacy._normalize_non_negative_int(row.get("active_jobs")) + (
        count if _status_is_active(status) else 0
    )
    items_found_total = legacy._normalize_non_negative_int(row.get("items_found_total"))
    stage_counts = _normalize_stage_counts(row.get("stage_counts"))
    stage_counts = _increment_stage_counter(stage_counts, stage=stage_key, key="total", delta=count)
    if _status_is_active(status):
        stage_counts = _increment_stage_counter(stage_counts, stage=stage_key, key="active", delta=count)
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
    conn: Any | None = None,
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

    def _increment_with_connection(write_conn: Any) -> None:
        with legacy.pg.db_cursor(conn=write_conn) as cur:
            row = legacy.pg.fetch_one_with_cursor(
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
            conn=write_conn,
            run_id=run_id,
            total_jobs=total_jobs,
            completed_jobs=completed_jobs,
            failed_jobs=failed_jobs,
            active_jobs=active_jobs,
            items_found_total=items_found_total,
            stage_counts=stage_counts,
        )

    if conn is not None:
        _increment_with_connection(conn)
        return
    with legacy.pg.db_connection() as write_conn:
        _increment_with_connection(write_conn)


def _recompute_run_summary_from_jobs(run_id: str, *, conn: Any | None = None) -> dict[str, Any]:
    summary_row = (
        _call_with_optional_conn(
            legacy.pg.fetch_one,
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
            conn=conn,
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

    summary = _recompute_run_summary_from_jobs(run_id, conn=conn)
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
            pool_name="session_control",
        ) as lock_conn:
            locked_result = _finalize_run_status_locked(run_id, lock_conn, force_recompute=force_recompute)
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
    # B1: advisory lock released above — run deferred-comments followup + sync-session
    # eval here, with fresh connections, so we never hold the run-finalize lock across
    # nested launches that acquire their own advisory locks.
    return _run_post_finalize_followups(run_id, locked_result)


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
    current_status = str(current.get("status") or "").strip().lower()
    if current_status == "cancelled":
        return {"summary": summary, "skip_followups": True}
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
    elif current_status == "cancelling":
        next_status = "cancelled"
    elif failed_jobs > 0 or fetch_terminal_error:
        next_status = "failed"
    else:
        next_status = "completed"
    try:
        status_update = _set_run_status(
            run_id,
            next_status,
            conn=lock_conn,
            expected_status=current_status,
        )
    except TypeError as exc:
        if "unexpected keyword argument 'expected_status'" not in str(exc):
            raise
        # Compatibility for older injected lifecycle shims. The real
        # implementation above always uses the conditional transition.
        status_update = _set_run_status(run_id, next_status, conn=lock_conn)
    if status_update is False:
        # Cancellation is intentionally outside the advisory lock in some
        # callers. If it wins this conditional transition, do not launch any
        # followup based on the stale pre-cancellation snapshot.
        refreshed = (
            legacy.pg.fetch_one(
                "select status, config from social.scrape_runs where id = %s::uuid",
                [run_id],
                conn=lock_conn,
            )
            or {}
        )
        return {
            "summary": summary,
            "status": str(refreshed.get("status") or current_status).strip().lower(),
            "skip_followups": True,
        }

    deferred_followup_claimed = False
    deferred_followup_claimed_at: str | None = None
    if status_update is True and next_status in {"completed", "queued", "running"}:
        claim = _claim_deferred_comments_followup_locked(
            run_id=run_id,
            run_config=current_config,
            conn=lock_conn,
        )
        if claim is not None:
            deferred_followup_claimed = True
            deferred_followup_claimed_at = str(claim.get("launch_claimed_at") or "").strip() or None
            current_config = legacy._metadata_dict(claim.get("config")) or current_config
    # B1: do NOT run the deferred-comments followup or sync-session evaluation while
    # holding the run-finalize advisory lock + lock_conn. Both launch nested work that
    # acquires its own advisory locks / pooled connections; running them under this lock
    # is the stall class that pinned runs in "finalizing". The caller runs them via
    # _run_post_finalize_followups after releasing the lock, with fresh connections.
    result = {
        "summary": summary,
        "next_status": next_status,
        "run_config": current_config,
    }
    # A None return is retained as a compatibility affordance for tests and
    # legacy shims that replace _set_run_status. Production writes return bool.
    if status_update is not None:
        result["deferred_followup_claimed"] = deferred_followup_claimed
        result["deferred_followup_claimed_at"] = deferred_followup_claimed_at
    return result


def _run_post_finalize_followups(
    run_id: str,
    locked_result: dict[str, Any],
) -> dict[str, Any]:
    """Run the deferred-comments followup and sync-session evaluation AFTER the
    run-finalize advisory lock has been released (B1).

    Uses fresh connections (conn=None) so a nested launch that takes its own advisory
    lock can never deadlock against the run-finalize lock or starve the social_control
    pool. Preserves the original ordering: comments followup first, then sync eval.
    """
    summary = locked_result.get("summary") or {}
    if locked_result.get("skip_followups"):
        return summary
    next_status = str(locked_result.get("next_status") or "")
    if next_status == "cancelled":
        return summary
    current_config = legacy._metadata_dict(locked_result.get("run_config"))
    followup_claimed = locked_result.get("deferred_followup_claimed")
    if followup_claimed is False:
        followup_updates = None
    else:
        followup_updates = _maybe_start_deferred_comments_followup(
            run_id=run_id,
            run_status=next_status,
            run_config=current_config,
            summary=summary,
            conn=None,
        )
    if followup_updates and not followup_updates.get("_deferred_followup_parent_cancelled"):
        summary = _update_run_summary(run_id, force_recompute=True, conn=None)
        refreshed_config = dict(current_config)
        refreshed_config.update(followup_updates)
        _apply_run_config_summary_fields(summary, refreshed_config)
    if _call_with_optional_conn(legacy._column_exists, "social", "scrape_runs", "sync_session_id", conn=None):
        run_row = (
            legacy.pg.fetch_one(
                "select sync_session_id::text as sync_session_id from social.scrape_runs where id = %s::uuid",
                [run_id],
                pool_name=SOCIAL_CONTROL_POOL_NAME,
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
