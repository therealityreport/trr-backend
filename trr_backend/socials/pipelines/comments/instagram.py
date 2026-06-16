# ruff: noqa: F821, UP037
"""Instagram comments launch, preview, progress, and cancellation pipeline."""

from __future__ import annotations

import asyncio
import math
import os
import re
import time as time_module
from collections import Counter
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, Sequence

import trr_backend.socials.social_season_analytics_impl as _core
from trr_backend.socials.instagram.comments_scrapling.public_mode import (
    PUBLIC_COMMENTS_SCRAPE_MODE,
    comments_load_strategy_for_mode,
    comments_public_mode_from_config,
)

_RESERVED_CORE_EXPORTS = {
    "__builtins__",
    "__cached__",
    "__doc__",
    "__file__",
    "__loader__",
    "__name__",
    "__package__",
    "__spec__",
    "_core",
    "_IMPORTED_CORE_NAMES",
    "_LOCAL_ROOM_NAMES",
    "_RESERVED_CORE_EXPORTS",
    "_sync_core_overrides",
}
_IMPORTED_CORE_NAMES: set[str] = set()
for _name, _value in _core.__dict__.items():
    if _name in _RESERVED_CORE_EXPORTS:
        continue
    globals()[_name] = _value
    _IMPORTED_CORE_NAMES.add(_name)
_LOCAL_ROOM_NAMES: set[str] = set()
_LOCAL_ROOM_FUNCTIONS: dict[str, Any] = {}
_CORE_ROOM_WRAPPERS: dict[str, Any] = {}

INSTAGRAM_COMMENTS_AUDIT_CURSOR_RETRY_STOP_REASONS = (
    "pagination_deadline_exceeded",
    "pagination_page_cap_reached",
    "network_budget_exhausted",
    "network_policy_blocked",
    "network_stop",
    "network_stopped",
    "proxy_budget_exhausted",
    "proxy_network_stop",
    "static_cdn_budget_exhausted",
)


def _sync_core_overrides() -> None:
    for _name in _IMPORTED_CORE_NAMES - _LOCAL_ROOM_NAMES:
        if hasattr(_core, _name):
            globals()[_name] = getattr(_core, _name)


def _room_callable(name: str, local_impl: Any) -> Any:
    candidate = getattr(_core, name, None)
    if callable(candidate) and candidate is not _CORE_ROOM_WRAPPERS.get(name):
        return candidate
    return local_impl


def _instagram_comments_stale_after_hours() -> int:
    return _resolve_positive_int_env(
        "SOCIAL_INSTAGRAM_COMMENTS_STALE_AFTER_HOURS",
        72,
        minimum=1,
    )


def _instagram_comments_target_count_expr(alias: str = "j") -> str:
    return f"""
        case
          when jsonb_typeof(coalesce({alias}.config->'target_source_ids', {alias}.metadata->'target_source_ids'))
            = 'array'
          then jsonb_array_length(coalesce({alias}.config->'target_source_ids', {alias}.metadata->'target_source_ids'))
          when nullif(coalesce({alias}.config->>'shortcode', {alias}.metadata->>'shortcode', ''), '') is not null
          then 1
          else 0
        end
    """


def _instagram_social_account_comments_coverage_diagnostics(
    account_handle: str,
    *,
    conn: Any | None = None,
) -> dict[str, Any]:
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    owner_match_clause = _social_account_profile_owner_match_sql("instagram", alias="p")
    lifecycle_supported = _call_profile_summary_loader_with_conn(
        _comment_lifecycle_supported,
        "instagram_comments",
        conn=conn,
    )
    active_comment_condition = "c.is_missing is not true" if lifecycle_supported else "true"
    missing_comment_condition = "c.is_missing is true" if lifecycle_supported else "false"
    reported_comments_expr = _instagram_fetchable_comments_sql("p")
    media_urls_expr = "coalesce(to_jsonb(c) -> 'media_urls', '[]'::jsonb)"
    parent_external_expr = "nullif(c.parent_comment_external_id, '')"
    reply_depth_expr = "coalesce(c.reply_depth, 0)"
    reply_condition = f"""
        coalesce(c.is_reply, false)
        or c.parent_comment_id is not null
        or {parent_external_expr} is not null
        or ({reply_depth_expr}) > 0
    """
    job_target_count_expr = _instagram_comments_target_count_expr("j")
    sql = f"""
            with posts as materialized (
              select
                p.id,
                {reported_comments_expr}::bigint as reported_comments
              from social.instagram_posts p
              where {owner_match_clause}
                and nullif(p.shortcode, '') is not null
            ),
            comment_rows as materialized (
              select
                c.id,
                c.post_id,
                c.is_missing,
                c.last_seen_at,
                c.scraped_at,
                ({reply_condition}) as is_reply_row,
                case
                  when jsonb_typeof({media_urls_expr}) = 'array'
                  then jsonb_array_length({media_urls_expr})
                  else 0
                end as media_url_count
              from social.instagram_comments c
              join posts p on p.id = c.post_id
            ),
            comment_by_post as (
              select
                c.post_id,
                count(c.id) filter (where {active_comment_condition})::bigint as saved_comments,
                max(coalesce(c.last_seen_at, c.scraped_at)) as last_seen_at
              from comment_rows c
              group by c.post_id
            ),
            comment_rollup as (
              select
                count(c.id) filter (
                  where {active_comment_condition}
                    and not c.is_reply_row
                )::bigint as saved_top_level_comments,
                count(c.id) filter (
                  where {active_comment_condition}
                    and c.is_reply_row
                )::bigint as saved_reply_comments,
                count(c.id) filter (where {active_comment_condition})::bigint as flattened_saved_comments,
                count(c.id) filter (
                  where {active_comment_condition}
                    and c.media_url_count > 0
                )::bigint as saved_media_comments,
                count(c.id) filter (where {missing_comment_condition})::bigint as missing_marked_comments,
                max(coalesce(c.last_seen_at, c.scraped_at)) as last_seen_at
              from comment_rows c
            ),
            post_rollup as (
              select
                count(*)::int as available_posts,
                count(*) filter (where reported_comments > 0)::int as eligible_posts,
                count(*) filter (
                  where reported_comments > 0
                    and coalesce(cbp.saved_comments, 0) = 0
                )::int as missing_posts,
                count(*) filter (
                  where reported_comments > 0
                    and coalesce(cbp.saved_comments, 0) > 0
                    and (
                      coalesce(cbp.saved_comments, 0) < reported_comments
                      or coalesce(cbp.last_seen_at, to_timestamp(0)) < now() - make_interval(hours => %s)
                    )
                )::int as stale_posts,
                coalesce(sum(reported_comments), 0)::bigint as reported_comments
              from posts p
              left join comment_by_post cbp on cbp.post_id = p.id
            ),
            comments_jobs as materialized (
              select
                j.id,
                j.run_id,
                j.status,
                j.config,
                j.metadata,
                j.attempt_count,
                j.max_attempts,
                j.last_error_code,
                j.error_message,
                j.created_at as job_created_at,
                j.started_at as job_started_at,
                j.completed_at as job_completed_at,
                r.status as run_status,
                r.created_at as run_created_at,
                r.started_at as run_started_at,
                r.completed_at as run_completed_at,
                {job_target_count_expr}::int as target_count,
                lower(coalesce(j.last_error_code, j.metadata->>'error_code', '')) as error_code
              from social.scrape_jobs j
              left join social.scrape_runs r on r.id = j.run_id
              where j.platform = 'instagram'
                and coalesce(j.config->>'stage', j.metadata->>'stage', j.job_type) = %s
                and ltrim(lower(coalesce(j.config->>'account', j.metadata->>'account', '')), '@') = %s
            ),
            job_rollup as (
              select
                coalesce(
                  sum(
                    case
                      when coalesce(
                        metadata->'fetcher_runtime'->'hidden_comments'->>'merged_comments',
                        metadata->'runtime_metadata'->'hidden_comments'->>'merged_comments',
                        ''
                      ) ~ '^[0-9]+$'
                      then coalesce(
                        metadata->'fetcher_runtime'->'hidden_comments'->>'merged_comments',
                        metadata->'runtime_metadata'->'hidden_comments'->>'merged_comments'
                      )::int
                      else 0
                    end
                  ),
                  0
                )::int as hidden_recovered_comments,
                count(*) filter (where status in ('queued', 'pending', 'running', 'retrying'))::int
                  as retryable_jobs,
                coalesce(
                  sum(target_count) filter (where status in ('queued', 'pending', 'running', 'retrying')),
                  0
                )::int as retryable_targets,
                count(*) filter (
                  where status in ('failed', 'cancelled')
                    and (
                      error_code = any(%s)
                      or error_code like '%%unavailable%%'
                      or error_code like '%%blocked%%'
                    )
                )::int as terminal_unavailable_jobs,
                coalesce(
                  sum(target_count) filter (
                    where status in ('failed', 'cancelled')
                      and (
                        error_code = any(%s)
                        or error_code like '%%unavailable%%'
                        or error_code like '%%blocked%%'
                      )
                  ),
                  0
                )::int as terminal_unavailable_targets
              from comments_jobs
            ),
            latest_job as (
              select *
              from comments_jobs
              order by coalesce(run_started_at, run_created_at, job_started_at, job_created_at) desc, id desc
              limit 1
            )
            select
              pr.available_posts,
              pr.eligible_posts,
              pr.missing_posts,
              pr.stale_posts,
              pr.reported_comments,
              coalesce(cr.saved_top_level_comments, 0)::bigint as saved_top_level_comments,
              coalesce(cr.saved_reply_comments, 0)::bigint as saved_reply_comments,
              coalesce(cr.flattened_saved_comments, 0)::bigint as flattened_saved_comments,
              coalesce(cr.saved_media_comments, 0)::bigint as saved_media_comments,
              coalesce(cr.missing_marked_comments, 0)::bigint as missing_marked_comments,
              coalesce(jr.hidden_recovered_comments, 0)::int as hidden_recovered_comments,
              coalesce(jr.retryable_jobs, 0)::int as retryable_jobs,
              coalesce(jr.retryable_targets, 0)::int as retryable_targets,
              coalesce(jr.terminal_unavailable_jobs, 0)::int as terminal_unavailable_jobs,
              coalesce(jr.terminal_unavailable_targets, 0)::int as terminal_unavailable_targets,
              lj.run_id::text as last_run_id,
              lj.id::text as last_run_job_id,
              coalesce(lj.run_status, lj.status) as last_run_status,
              lj.status as last_job_status,
              lj.run_created_at as last_run_created_at,
              lj.run_started_at as last_run_started_at,
              lj.run_completed_at as last_run_completed_at,
              lj.job_created_at as last_job_created_at,
              lj.job_started_at as last_job_started_at,
              lj.job_completed_at as last_job_completed_at,
              lj.error_code as last_error_code,
              lj.error_message as last_error_message
            from post_rollup pr
            cross join comment_rollup cr
            cross join job_rollup jr
            left join latest_job lj on true
            """
    terminal_codes = list(_INSTAGRAM_COMMENTS_TERMINAL_UNAVAILABLE_ERROR_CODES)
    params = [
        normalized_account,
        _instagram_comments_stale_after_hours(),
        INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
        normalized_account,
        terminal_codes,
        terminal_codes,
    ]
    if conn is None:
        row = pg.fetch_one(sql, params) or {}
    else:
        with pg.db_cursor(conn=conn, label="instagram_comments_coverage_diagnostics") as cur:
            row = pg.fetch_one_with_cursor(cur, sql, params) or {}
    last_run_metadata = {
        "run_id": str(row.get("last_run_id") or "").strip() or None,
        "job_id": str(row.get("last_run_job_id") or "").strip() or None,
        "status": str(row.get("last_run_status") or "").strip().lower() or None,
        "job_status": str(row.get("last_job_status") or "").strip().lower() or None,
        "created_at": row.get("last_run_created_at") or row.get("last_job_created_at"),
        "started_at": row.get("last_run_started_at") or row.get("last_job_started_at"),
        "completed_at": row.get("last_run_completed_at") or row.get("last_job_completed_at"),
        "last_error_code": str(row.get("last_error_code") or "").strip().lower() or None,
        "error_message": str(row.get("last_error_message") or "").strip() or None,
    }
    if not last_run_metadata["run_id"] and not last_run_metadata["job_id"]:
        last_run_metadata = {}
    return {
        "available_posts": _normalize_non_negative_int(row.get("available_posts")),
        "eligible_posts": _normalize_non_negative_int(row.get("eligible_posts")),
        "missing_posts": _normalize_non_negative_int(row.get("missing_posts")),
        "stale_posts": _normalize_non_negative_int(row.get("stale_posts")),
        "reported_comments": _normalize_non_negative_int(row.get("reported_comments")),
        "saved_top_level_comments": _normalize_non_negative_int(row.get("saved_top_level_comments")),
        "saved_reply_comments": _normalize_non_negative_int(row.get("saved_reply_comments")),
        "flattened_saved_comments": _normalize_non_negative_int(row.get("flattened_saved_comments")),
        "saved_media_comments": _normalize_non_negative_int(row.get("saved_media_comments")),
        "hidden_recovered_comments": _normalize_non_negative_int(row.get("hidden_recovered_comments")),
        "missing_marked_comments": _normalize_non_negative_int(row.get("missing_marked_comments")),
        "retryable": _normalize_non_negative_int(row.get("retryable_targets")),
        "retryable_jobs": _normalize_non_negative_int(row.get("retryable_jobs")),
        "retryable_targets": _normalize_non_negative_int(row.get("retryable_targets")),
        "terminal_unavailable": _normalize_non_negative_int(row.get("terminal_unavailable_targets")),
        "terminal_unavailable_jobs": _normalize_non_negative_int(row.get("terminal_unavailable_jobs")),
        "terminal_unavailable_targets": _normalize_non_negative_int(row.get("terminal_unavailable_targets")),
        "last_run_metadata": last_run_metadata,
    }


def get_social_account_comments_coverage_diagnostics(platform: str, account_handle: str) -> dict[str, Any]:
    normalized_platform = _normalize_social_account_profile_platform(platform)
    if normalized_platform != "instagram":
        raise ValueError("Comments coverage diagnostics are currently supported for Instagram profiles.")
    return _instagram_social_account_comments_coverage_diagnostics(account_handle)


def _instagram_comments_target_priority(refresh_policy: str) -> str:
    normalized_refresh_policy = str(refresh_policy or "stale_or_missing").strip().lower() or "stale_or_missing"
    if normalized_refresh_policy == "all_saved_posts":
        target_priority = (
            str(os.getenv("SOCIAL_INSTAGRAM_COMMENTS_TARGET_PRIORITY", "gap_first") or "gap_first").strip().lower()
        )
        return target_priority if target_priority in {"gap_first", "posted_at_desc"} else "gap_first"
    return "missing_first_recent"


_INSTAGRAM_COMMENTS_LOAD_STRATEGIES = {"cursor_api", "single_session_load_all", "public_relay"}
_INSTAGRAM_COMMENTS_SINGLE_SESSION_ENV = "SOCIAL_INSTAGRAM_COMMENTS_SINGLE_SESSION_LOAD_ALL_ENABLED"
_BROWSER_SESSION_INVALIDATED_REASON = "browser_session_invalidated"


def _env_truthy(name: str) -> bool:
    return str(os.getenv(name) or "").strip().lower() in {"1", "true", "yes", "on", "enabled"}


def _metadata_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "enabled"}
    return False


def _instagram_comments_single_session_load_all_enabled() -> bool:
    return _env_truthy(_INSTAGRAM_COMMENTS_SINGLE_SESSION_ENV)


def _normalize_instagram_comments_load_strategy(value: str | None) -> str:
    normalized = str(value or "cursor_api").strip().lower() or "cursor_api"
    if normalized not in _INSTAGRAM_COMMENTS_LOAD_STRATEGIES:
        allowed = ", ".join(sorted(_INSTAGRAM_COMMENTS_LOAD_STRATEGIES))
        raise SocialIngestValidationError(
            "SOCIAL_ACCOUNT_COMMENTS_INVALID_LOAD_STRATEGY",
            f"Unsupported comments_load_strategy: {normalized}. Allowed values: {allowed}.",
        )
    return normalized


def _assert_instagram_comments_load_strategy_enabled(load_strategy: str) -> None:
    if load_strategy != "single_session_load_all":
        return
    if _instagram_comments_single_session_load_all_enabled():
        return
    raise SocialIngestValidationError(
        "SOCIAL_INSTAGRAM_COMMENTS_LOAD_STRATEGY_DISABLED",
        (
            "Instagram comments load strategy single_session_load_all is disabled. "
            f"Set {_INSTAGRAM_COMMENTS_SINGLE_SESSION_ENV}=true to enable it."
        ),
    )


def _instagram_comments_load_strategy_metadata(
    *,
    load_strategy: str,
    mode: str,
    target_count: int,
    recommended_shard_count: int,
    effective_shard_count: int,
) -> dict[str, Any]:
    public_relay = load_strategy == "public_relay"
    single_session = load_strategy == "single_session_load_all"
    forced_single_session = single_session and mode == "profile" and target_count > 1
    if public_relay:
        session_scope = "public_relay"
    elif single_session:
        session_scope = "post_continuous" if mode == "single_post" else "profile_single_worker"
    else:
        session_scope = "cursor_api_worker"
    return {
        "comments_load_strategy": load_strategy,
        "comments_session_scope": session_scope,
        "comments_internal_pagination": "cursor_preserved",
        "comments_sharding_forced_single_session": forced_single_session,
        "recommended_comments_shard_count": recommended_shard_count,
        "effective_comments_shard_count": effective_shard_count,
        "single_session_enabled": _instagram_comments_single_session_load_all_enabled(),
    }


def _public_comments_config_overlay(config: Mapping[str, Any] | None) -> dict[str, Any]:
    normalized = dict(config or {})
    public_comments_mode = comments_public_mode_from_config(normalized)
    normalized["comments_load_strategy"] = comments_load_strategy_for_mode(
        normalized.get("comments_load_strategy"),
        public_mode=public_comments_mode,
    )
    if public_comments_mode:
        normalized.update(
            {
                "instagram_scrape_mode": PUBLIC_COMMENTS_SCRAPE_MODE,
                "comments_session_scope": "public_relay",
                "comments_auth_validation_mode": "public_relay",
                "comments_proxy_shard_sessions": False,
            }
        )
    normalized["instagram_access_proof"] = _instagram_comments_access_proof(public_mode=public_comments_mode)
    return normalized


def _instagram_comments_access_proof(*, public_mode: bool) -> dict[str, Any]:
    if public_mode:
        return {
            "auth_state": "public",
            "cookie_state": "none",
            "proxy_state": "none",
            "decodo_state": "not_used",
            "no_cookies": True,
            "no_decodo": True,
            "proof_label": "No cookies · No Decodo",
        }
    return {
        "auth_state": "authenticated",
        "cookie_state": "required",
        "proxy_state": "configured_by_environment",
        "decodo_state": "environment_dependent",
        "no_cookies": False,
        "no_decodo": False,
        "proof_label": "Auth/proxy dependent",
    }


def _instagram_comments_cancel_active_before_relaunch_enabled(value: Any = None) -> bool:
    if value is not None:
        return _metadata_truthy(value)
    raw_value = str(os.getenv("SOCIAL_INSTAGRAM_COMMENTS_CANCEL_ACTIVE_BEFORE_RELAUNCH", "1")).strip().lower()
    return raw_value in {"1", "true", "yes", "on"}


def _instagram_comments_bulk_insert_threshold() -> int:
    raw_value = str(os.getenv("SOCIAL_INSTAGRAM_COMMENTS_BULK_INSERT_THRESHOLD", "25")).strip()
    try:
        requested = int(raw_value)
    except ValueError:
        return 25
    return max(2, min(requested, 10000))


def _instagram_comments_load_strategy_warnings(metadata: Mapping[str, Any]) -> list[dict[str, str]]:
    if not bool(metadata.get("comments_sharding_forced_single_session")):
        return []
    return [
        {
            "code": "INSTAGRAM_COMMENTS_SINGLE_SESSION_FORCES_ONE_SHARD",
            "message": "single_session_load_all runs profile comment scrapes in one comments shard.",
        }
    ]


def _normalize_instagram_comments_target_filter(value: str | None) -> str | None:
    normalized = str(value or "").strip().lower()
    if not normalized or normalized == "all":
        return None
    if normalized != "incomplete":
        raise SocialIngestValidationError(
            "SOCIAL_ACCOUNT_COMMENTS_INVALID_TARGET_FILTER",
            "Profile comments scraping supports target_filter=incomplete.",
        )
    return normalized


def _instagram_comments_target_preview_cache_ttl_seconds() -> int:
    raw = str(os.getenv("SOCIAL_INSTAGRAM_COMMENTS_TARGET_PREVIEW_CACHE_TTL_SECONDS") or "").strip()
    if not raw:
        return SOCIAL_INSTAGRAM_COMMENTS_TARGET_PREVIEW_CACHE_TTL_SECONDS_DEFAULT
    try:
        return max(0, min(int(raw), 3600))
    except ValueError:
        return SOCIAL_INSTAGRAM_COMMENTS_TARGET_PREVIEW_CACHE_TTL_SECONDS_DEFAULT


def _get_instagram_comments_target_preview_cache(
    cache_key: tuple[Any, ...],
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    ttl_seconds = _instagram_comments_target_preview_cache_ttl_seconds()
    cache_metadata = {
        "enabled": ttl_seconds > 0,
        "hit": False,
        "age_seconds": None,
        "ttl_seconds": ttl_seconds,
    }
    if ttl_seconds <= 0:
        return None, cache_metadata
    now_monotonic = time_module.monotonic()
    with _INSTAGRAM_COMMENTS_TARGET_PREVIEW_CACHE_LOCK:
        cached = _INSTAGRAM_COMMENTS_TARGET_PREVIEW_CACHE.get(cache_key)
        if not cached:
            return None, cache_metadata
        cached_at, cached_payload = cached
        age_seconds = max(0.0, now_monotonic - cached_at)
        if age_seconds > ttl_seconds:
            _INSTAGRAM_COMMENTS_TARGET_PREVIEW_CACHE.pop(cache_key, None)
            return None, cache_metadata
        cache_metadata.update({"hit": True, "age_seconds": round(age_seconds, 3)})
        return copy.deepcopy(cached_payload), cache_metadata


def _set_instagram_comments_target_preview_cache(cache_key: tuple[Any, ...], payload: dict[str, Any]) -> None:
    ttl_seconds = _instagram_comments_target_preview_cache_ttl_seconds()
    if ttl_seconds <= 0:
        return
    with _INSTAGRAM_COMMENTS_TARGET_PREVIEW_CACHE_LOCK:
        if len(_INSTAGRAM_COMMENTS_TARGET_PREVIEW_CACHE) >= 128:
            oldest_key = min(
                _INSTAGRAM_COMMENTS_TARGET_PREVIEW_CACHE,
                key=lambda key: _INSTAGRAM_COMMENTS_TARGET_PREVIEW_CACHE[key][0],
            )
            _INSTAGRAM_COMMENTS_TARGET_PREVIEW_CACHE.pop(oldest_key, None)
        _INSTAGRAM_COMMENTS_TARGET_PREVIEW_CACHE[cache_key] = (time_module.monotonic(), copy.deepcopy(payload))


def _normalize_comment_date_window(
    date_start: str | None,
    date_end: str | None,
) -> tuple[datetime | None, datetime | None]:
    """Parse an ISO 8601 comment-target date window into UTC datetimes.

    The window is start-inclusive and end-exclusive. Returns ``(None, None)``
    when both bounds are absent. Naive inputs are assumed to be UTC. Raises
    ``ValueError`` on malformed input or when ``date_start`` is not strictly
    before ``date_end``.
    """

    def _parse_one(raw: str | None) -> datetime | None:
        if raw is None:
            return None
        text = str(raw).strip()
        if not text:
            return None
        normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise ValueError(f"Invalid ISO 8601 datetime: {raw!r}") from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    start_dt = _parse_one(date_start)
    end_dt = _parse_one(date_end)
    if start_dt is None and end_dt is None:
        return (None, None)
    if start_dt is not None and end_dt is not None and start_dt >= end_dt:
        raise ValueError("date_start must be strictly before date_end")
    return (start_dt, end_dt)


def _comment_date_window_predicate(
    start_dt: datetime | None,
    end_dt: datetime | None,
    *,
    alias: str,
    column: str = "posted_at",
) -> tuple[str, list[Any]]:
    """Build a posted_at window predicate and its bound params for ``alias``.

    Returns an empty predicate (and no params) when the window is unbounded.
    Start is inclusive, end is exclusive.
    """

    clauses: list[str] = []
    params: list[Any] = []
    if start_dt is not None:
        clauses.append(f"{alias}.{column} >= %s")
        params.append(start_dt)
    if end_dt is not None:
        clauses.append(f"{alias}.{column} < %s")
        params.append(end_dt)
    if not clauses:
        return "", []
    return " and " + " and ".join(clauses), params


def _instagram_social_account_comment_target_preview(
    account_handle: str,
    *,
    limit: int | None,
    refresh_policy: str = "stale_or_missing",
    target_filter: str | None = None,
    sample_limit: int = 12,
    date_start: str | None = None,
    date_end: str | None = None,
) -> dict[str, Any]:
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    normalized_refresh_policy = str(refresh_policy or "stale_or_missing").strip().lower() or "stale_or_missing"
    normalized_target_filter = _normalize_instagram_comments_target_filter(target_filter)
    if normalized_refresh_policy not in {"stale_or_missing", "all_saved_posts"}:
        raise SocialIngestValidationError(
            "SOCIAL_ACCOUNT_COMMENTS_INVALID_REFRESH_POLICY",
            "Profile comments scraping supports stale_or_missing and all_saved_posts refreshes.",
        )
    safe_limit = None if limit is None else max(1, min(int(limit), 500))
    safe_sample_limit = max(1, min(int(sample_limit or 12), 50))
    if safe_limit is not None:
        safe_sample_limit = min(safe_sample_limit, safe_limit)
    window_start, window_end = _normalize_comment_date_window(date_start, date_end)
    target_priority = _instagram_comments_target_priority(normalized_refresh_policy)
    cache_key = (
        "instagram_comments_target_preview",
        normalized_account,
        normalized_refresh_policy,
        normalized_target_filter,
        target_priority,
        safe_limit,
        safe_sample_limit,
        window_start.isoformat() if window_start is not None else None,
        window_end.isoformat() if window_end is not None else None,
    )
    total_started_at = time_module.perf_counter()
    cache_lookup_started_at = time_module.perf_counter()
    cached_payload, cache_metadata = _get_instagram_comments_target_preview_cache(cache_key)
    cache_lookup_ms = round((time_module.perf_counter() - cache_lookup_started_at) * 1000, 1)
    if cached_payload is not None:
        timing = _metadata_dict(cached_payload.get("timing"))
        timing.update(
            {
                "cache_lookup_ms": cache_lookup_ms,
                "total_ms": round((time_module.perf_counter() - total_started_at) * 1000, 1),
            }
        )
        cached_payload["timing"] = timing
        cached_payload["preview_cache"] = cache_metadata
        cached_payload["cache"] = cache_metadata
        return cached_payload

    query_started_at = time_module.perf_counter()
    owner_match_clause = _social_account_profile_owner_match_sql("instagram", alias="p")
    if normalized_target_filter == "incomplete":
        target_source_ids = _instagram_social_account_incomplete_comment_target_shortcodes(
            normalized_account,
            limit=safe_limit,
            date_start=date_start,
            date_end=date_end,
        )
        raw_target_count = len(target_source_ids)
        target_count = raw_target_count
        sample_target_source_ids = target_source_ids[:safe_sample_limit]
        query_ms = round((time_module.perf_counter() - query_started_at) * 1000, 1)
    elif normalized_refresh_policy == "all_saved_posts":
        table, source_id_column, posted_at_column = _shared_catalog_base_query_parts("instagram")
        reported_comments_expr = _instagram_fetchable_comments_sql("p")
        catalog_reported_comments_expr = _instagram_fetchable_comments_sql(
            "p",
            fb_comment_count_expr="null::text",
        )
        active_count_expr = (
            "count(c.id) filter (where c.is_missing is not true)::bigint"
            if _comment_lifecycle_supported("instagram_comments")
            else "count(c.id)::bigint"
        )
        order_clause = (
            "missing_comments_gap desc, reported_comments desc nulls last, posted_at desc nulls last, shortcode desc"
            if target_priority == "gap_first"
            else "posted_at desc nulls last, shortcode desc"
        )
        owner_window_sql, owner_window_params = _comment_date_window_predicate(
            window_start, window_end, alias="p"
        )
        catalog_window_sql, catalog_window_params = _comment_date_window_predicate(
            window_start, window_end, alias="p", column=posted_at_column
        )
        sql = f"""
        with saved_posts as (
          select
            p.id as post_id,
            p.shortcode::text as shortcode,
            p.posted_at,
            {reported_comments_expr}::bigint as reported_comments
          from social.instagram_posts p
          where {owner_match_clause}
            and nullif(p.shortcode, '') is not null{owner_window_sql}
          union all
          select
            null::uuid as post_id,
            p.{source_id_column}::text as shortcode,
            p.{posted_at_column} as posted_at,
            {catalog_reported_comments_expr}::bigint as reported_comments
          from social.{table} p
          where lower(p.source_account) = lower(%s)
            and nullif(p.{source_id_column}::text, '') is not null{catalog_window_sql}
        ),
        deduped_posts as (
          select
            (max(post_id::text) filter (where post_id is not null))::uuid as post_id,
            shortcode,
            max(posted_at) as posted_at,
            max(reported_comments) as reported_comments
          from saved_posts
          group by shortcode
        ),
        saved_comment_counts as (
          select
            dp.shortcode,
            {active_count_expr} as saved_comments
          from deduped_posts dp
          left join social.instagram_comments c on c.post_id = dp.post_id
          group by dp.shortcode
        ),
        targets as (
          select
            dp.shortcode,
            greatest(coalesce(dp.reported_comments, 0) - coalesce(scc.saved_comments, 0), 0) as missing_comments_gap,
            dp.reported_comments,
            dp.posted_at
          from deduped_posts dp
          left join saved_comment_counts scc on scc.shortcode = dp.shortcode
        )
        select
          (select count(*)::int from targets) as raw_target_source_ids_count,
          array(
            select shortcode
            from targets
            order by {order_clause}
            limit %s
          ) as sample_target_source_ids
        """
        params: list[Any] = [
            normalized_account,
            *owner_window_params,
            normalized_account,
            *catalog_window_params,
            safe_sample_limit,
        ]
        row = pg.fetch_one(sql, params) or {}
        query_ms = round((time_module.perf_counter() - query_started_at) * 1000, 1)
        raw_target_count = _normalize_non_negative_int(row.get("raw_target_source_ids_count"))
        target_count = min(raw_target_count, safe_limit) if safe_limit is not None else raw_target_count
        sample_target_source_ids = _as_text_list(row.get("sample_target_source_ids"))[:safe_sample_limit]
    else:
        reported_comments_expr = _instagram_fetchable_comments_sql("p")
        active_count_expr = (
            "(count(c.id) filter (where c.is_missing = false))::bigint"
            if _comment_lifecycle_supported("instagram_comments")
            else "count(c.id)::bigint"
        )
        owner_window_sql, owner_window_params = _comment_date_window_predicate(
            window_start, window_end, alias="p"
        )
        sql = f"""
        with posts as (
          select
            p.shortcode,
            p.posted_at,
            {reported_comments_expr}::bigint as reported_comments,
            {active_count_expr} as saved_comments,
            max(coalesce(c.last_seen_at, c.scraped_at)) as last_seen_at
          from social.instagram_posts p
          left join social.instagram_comments c on c.post_id = p.id
          where {owner_match_clause}
            and nullif(p.shortcode, '') is not null
            and {reported_comments_expr} > 0{owner_window_sql}
          group by p.shortcode, p.posted_at, p.comments_count, p.fb_comment_count, p.raw_data
        ),
        targets as (
          select
            shortcode,
            posted_at,
            reported_comments,
            saved_comments,
            last_seen_at
          from posts
          where reported_comments > 0
            and (
              saved_comments = 0
              or saved_comments < reported_comments
              or coalesce(last_seen_at, to_timestamp(0)) < now() - make_interval(hours => %s)
            )
        )
        select
          (select count(*)::int from targets) as raw_target_source_ids_count,
          array(
            select shortcode
            from targets
            order by
              case when saved_comments = 0 then 0 else 1 end asc,
              posted_at desc nulls last,
              shortcode desc
            limit %s
          ) as sample_target_source_ids
        """
        params = [
            normalized_account,
            *owner_window_params,
            _instagram_comments_stale_after_hours(),
            safe_sample_limit,
        ]
        row = pg.fetch_one(sql, params) or {}
        query_ms = round((time_module.perf_counter() - query_started_at) * 1000, 1)
        raw_target_count = _normalize_non_negative_int(row.get("raw_target_source_ids_count"))
        target_count = min(raw_target_count, safe_limit) if safe_limit is not None else raw_target_count
        sample_target_source_ids = _as_text_list(row.get("sample_target_source_ids"))[:safe_sample_limit]
    shard_count = _instagram_comments_profile_shard_count(target_count)
    payload = {
        "target_source_ids_count": target_count,
        "raw_target_source_ids_count": raw_target_count,
        "sample_target_source_ids": sample_target_source_ids,
        "comments_shard_count": shard_count,
        "comments_sharding_enabled": shard_count > 1,
        "recommended_comments_shard_count": _instagram_comments_recommended_shard_count(target_count=target_count),
        "refresh_policy": normalized_refresh_policy,
        "target_filter": normalized_target_filter,
        "incomplete_fill": normalized_target_filter == "incomplete",
        "target_priority": target_priority,
        "date_start": window_start.isoformat() if window_start is not None else None,
        "date_end": window_end.isoformat() if window_end is not None else None,
        "target_window": (
            {
                "date_start": window_start.isoformat() if window_start is not None else None,
                "date_end": window_end.isoformat() if window_end is not None else None,
                "end_exclusive": True,
            }
            if (window_start is not None or window_end is not None)
            else None
        ),
        "timing": {
            "target_preview_ms": query_ms,
            "target_count_ms": query_ms,
            "sample_target_source_ids_ms": query_ms,
            "cache_lookup_ms": cache_lookup_ms,
            "total_ms": round((time_module.perf_counter() - total_started_at) * 1000, 1),
        },
        "preview_cache": cache_metadata,
        "cache": cache_metadata,
        "debug": {
            "target_plan_strategy": "bounded_profile_preview",
            "bounded": True,
            "full_target_list_built": False,
            "sample_limit": safe_sample_limit,
            "max_posts": safe_limit,
            "query_refresh_policy": normalized_refresh_policy,
            "query_target_filter": normalized_target_filter,
            "raw_target_source_ids_count": raw_target_count,
        },
    }
    _set_instagram_comments_target_preview_cache(cache_key, payload)
    return payload


def _instagram_social_account_comments_target_counts(
    account_handle: str,
    *,
    conn: Any | None = None,
) -> dict[str, int]:
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    owner_match_clause = _social_account_profile_owner_match_sql("instagram", alias="p")
    lifecycle_supported = _call_profile_summary_loader_with_conn(
        _comment_lifecycle_supported,
        "instagram_comments",
        conn=conn,
    )
    active_condition = "c.is_missing is not true" if lifecycle_supported else "true"
    missing_condition = "c.is_missing is true" if lifecycle_supported else "false"
    reported_comments_expr = _instagram_reported_comments_sql("p")
    facebook_comments_expr = _instagram_external_facebook_comments_sql("p")
    sql = f"""
            with posts as materialized (
              select
                p.id,
                {reported_comments_expr}::bigint as reported_comments,
                {facebook_comments_expr}::bigint as facebook_comments
              from social.instagram_posts p
              where {owner_match_clause}
                and nullif(p.shortcode, '') is not null
            ),
            comment_counts as materialized (
              select
                c.post_id,
                count(c.id) filter (
                  where {active_condition}
                    and coalesce(c.source_snapshot_type, '') <> 'fb_crosspost'
                )::bigint as saved_comments,
                count(c.id) filter (
                  where {missing_condition}
                    and coalesce(c.source_snapshot_type, '') <> 'fb_crosspost'
                )::bigint as classified_missing_comments,
                max(coalesce(c.last_seen_at, c.scraped_at)) as last_seen_at
              from social.instagram_comments c
              join posts p on p.id = c.post_id
              group by c.post_id
            ),
            reconciled_posts as (
              select
                p.id,
                p.reported_comments,
                p.facebook_comments,
                coalesce(cc.saved_comments, 0) as saved_comments,
                coalesce(cc.saved_comments, 0)
                  + coalesce(p.facebook_comments, 0)
                  + coalesce(cc.classified_missing_comments, 0) as accounted_comments,
                cc.last_seen_at
              from posts p
              left join comment_counts cc on cc.post_id = p.id
            )
            select
              count(*)::int as available_posts,
              coalesce(
                sum(
                  case
                    when reported_comments > 0 then 1
                    else 0
                  end
                ),
                0
              )::int as eligible_posts,
              coalesce(
                sum(
                  case
                    when reported_comments > 0 and accounted_comments = 0 then 1
                    else 0
                  end
                ),
                0
              )::int as missing_posts,
              coalesce(
                sum(
                  case
                    when reported_comments > 0
                      and accounted_comments > 0
                      and (
                        accounted_comments < reported_comments
                        or coalesce(last_seen_at, to_timestamp(0)) < now() - make_interval(hours => %s)
                      )
                    then 1
                    else 0
                  end
                ),
                0
              )::int as stale_posts
            from reconciled_posts
            """
    params = [normalized_account, _instagram_comments_stale_after_hours()]
    if conn is None:
        row = pg.fetch_one(sql, params) or {}
    else:
        with pg.db_cursor(conn=conn, label="instagram_comments_target_counts") as cur:
            row = pg.fetch_one_with_cursor(cur, sql, params) or {}
    return {
        "available_posts": _normalize_non_negative_int(row.get("available_posts")),
        "eligible_posts": _normalize_non_negative_int(row.get("eligible_posts")),
        "missing_posts": _normalize_non_negative_int(row.get("missing_posts")),
        "stale_posts": _normalize_non_negative_int(row.get("stale_posts")),
    }


def get_active_social_account_comments_run(
    platform: str,
    account_handle: str,
    *,
    conn: Any | None = None,
) -> dict[str, Any] | None:
    for row in _social_account_comments_recent_runs(platform, account_handle, limit=10, conn=conn):
        if _status_is_active(str(row.get("status") or "").strip().lower() or None):
            return row
    return None


def _instagram_social_account_comment_target_shortcodes(
    account_handle: str,
    *,
    limit: int | None,
    refresh_policy: str = "stale_or_missing",
    date_start: str | None = None,
    date_end: str | None = None,
) -> list[str]:
    _sync_core_overrides()
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    owner_match_clause = _social_account_profile_owner_match_sql("instagram", alias="p")
    normalized_refresh_policy = str(refresh_policy or "stale_or_missing").strip().lower() or "stale_or_missing"
    safe_limit = None if limit is None else max(1, min(int(limit), 500))
    window_start, window_end = _normalize_comment_date_window(date_start, date_end)
    if normalized_refresh_policy == "all_saved_posts":
        target_priority = _instagram_comments_target_priority(normalized_refresh_policy)
        table, source_id_column, posted_at_column = _shared_catalog_base_query_parts("instagram")
        reported_comments_expr = _instagram_fetchable_comments_sql("p")
        catalog_reported_comments_expr = _instagram_fetchable_comments_sql(
            "p",
            fb_comment_count_expr="null::text",
        )
        active_count_expr = (
            "count(c.id) filter (where c.is_missing is not true)::bigint"
            if _comment_lifecycle_supported("instagram_comments")
            else "count(c.id)::bigint"
        )
        order_sql = (
            """
            order by
              missing_comments_gap desc,
              reported_comments desc nulls last,
              posted_at desc nulls last,
              shortcode desc
            """
            if target_priority == "gap_first"
            else "order by posted_at desc nulls last, shortcode desc"
        )
        owner_window_sql, owner_window_params = _comment_date_window_predicate(
            window_start, window_end, alias="p"
        )
        catalog_window_sql, catalog_window_params = _comment_date_window_predicate(
            window_start, window_end, alias="p", column=posted_at_column
        )
        sql = f"""
        with saved_posts as (
          select
            p.id as post_id,
            p.shortcode::text as shortcode,
            p.posted_at,
            {reported_comments_expr}::bigint as reported_comments
          from social.instagram_posts p
          where {owner_match_clause}
            and nullif(p.shortcode, '') is not null{owner_window_sql}
          union all
          select
            null::uuid as post_id,
            p.{source_id_column}::text as shortcode,
            p.{posted_at_column} as posted_at,
            {catalog_reported_comments_expr}::bigint as reported_comments
          from social.{table} p
          where lower(p.source_account) = lower(%s)
            and nullif(p.{source_id_column}::text, '') is not null{catalog_window_sql}
        ),
        deduped_posts as (
          select
            (max(post_id::text) filter (where post_id is not null))::uuid as post_id,
            shortcode,
            max(posted_at) as posted_at,
            max(reported_comments) as reported_comments
          from saved_posts
          group by shortcode
        ),
        saved_comment_counts as (
          select
            dp.shortcode,
            {active_count_expr} as saved_comments
          from deduped_posts dp
          left join social.instagram_comments c on c.post_id = dp.post_id
          group by dp.shortcode
        )
        select
          dp.shortcode,
          greatest(coalesce(dp.reported_comments, 0) - coalesce(scc.saved_comments, 0), 0) as missing_comments_gap,
          dp.reported_comments,
          dp.posted_at
        from deduped_posts dp
        left join saved_comment_counts scc on scc.shortcode = dp.shortcode
        {order_sql}
        """
        params: list[Any] = [
            normalized_account,
            *owner_window_params,
            normalized_account,
            *catalog_window_params,
        ]
        if safe_limit is not None:
            sql += " limit %s"
            params.append(safe_limit)
        rows = pg.fetch_all(sql, params)
    else:
        reported_comments_expr = _instagram_fetchable_comments_sql("p")
        active_count_expr = (
            "(count(c.id) filter (where c.is_missing = false))::bigint"
            if _comment_lifecycle_supported("instagram_comments")
            else "count(c.id)::bigint"
        )
        owner_window_sql, owner_window_params = _comment_date_window_predicate(
            window_start, window_end, alias="p"
        )
        sql = f"""
        with posts as (
          select
            p.shortcode,
            p.posted_at,
            {reported_comments_expr}::bigint as reported_comments,
            {active_count_expr} as saved_comments,
            max(coalesce(c.last_seen_at, c.scraped_at)) as last_seen_at
          from social.instagram_posts p
          left join social.instagram_comments c on c.post_id = p.id
          where {owner_match_clause}
            and nullif(p.shortcode, '') is not null
            and {reported_comments_expr} > 0{owner_window_sql}
          group by p.shortcode, p.posted_at, p.comments_count, p.fb_comment_count, p.raw_data
        )
        select shortcode
        from posts
        where reported_comments > 0
          and (
            saved_comments = 0
            or saved_comments < reported_comments
            or coalesce(last_seen_at, to_timestamp(0)) < now() - make_interval(hours => %s)
          )
        order by
          case when saved_comments = 0 then 0 else 1 end asc,
          posted_at desc nulls last,
          shortcode desc
        """
        params = [
            normalized_account,
            *owner_window_params,
            _instagram_comments_stale_after_hours(),
        ]
        if safe_limit is not None:
            sql += " limit %s"
            params.append(safe_limit)
        rows = pg.fetch_all(sql, params)
    return [str(row.get("shortcode") or "").strip() for row in rows if str(row.get("shortcode") or "").strip()]


def _instagram_social_account_incomplete_comment_target_shortcodes(
    account_handle: str,
    *,
    limit: int | None,
    date_start: str | None = None,
    date_end: str | None = None,
) -> list[str]:
    """Return comments-tab incomplete post shortcodes for the account.

    This mirrors the comments-only `comment_filter=incomplete` predicate while
    selecting only shortcodes, so target enumeration avoids the profile total
    count query.
    """
    _sync_core_overrides()

    normalized_account = _normalize_social_account_profile_handle(account_handle)
    safe_limit = None if limit is None else max(1, min(int(limit), 500))
    window_start, window_end = _normalize_comment_date_window(date_start, date_end)
    owner_window_sql, owner_window_params = _comment_date_window_predicate(
        window_start, window_end, alias="p"
    )
    collaborator_window_sql, collaborator_window_params = _comment_date_window_predicate(
        window_start, window_end, alias="p"
    )
    owner_match_clause = _social_account_profile_owner_match_sql("instagram", alias="p")
    lifecycle_supported = _comment_lifecycle_supported("instagram_comments")
    active_condition = "c.is_missing is not true" if lifecycle_supported else "true"
    missing_condition = "c.is_missing is true" if lifecycle_supported else "false"
    fb_crosspost_condition = (
        "coalesce(c.phase, '') = 'fb_crosspost'"
        if _column_exists("social", "instagram_comments", "phase")
        else "coalesce(c.source_snapshot_type, '') = 'instagram_fb_crosspost_comments'"
    )
    parent_external_expr = "nullif(c.parent_comment_external_id, '')"
    reply_depth_expr = "coalesce(c.reply_depth, 0)"
    reply_condition = f"""
        (
          coalesce(c.is_reply, false)
          or c.parent_comment_id is not null
          or {parent_external_expr} is not null
          or ({reply_depth_expr}) > 0
        )
    """
    reported_comments_expr = _instagram_reported_comments_sql("p")
    facebook_comments_expr = _instagram_external_facebook_comments_sql("p")
    catalog_reported_comments_expr = _instagram_reported_comments_sql("p")
    catalog_facebook_comments_expr = "0"
    accounted_comments_sql = (
        "coalesce(saved_comment_counts.saved_parent_comments, 0) + "
        "coalesce(saved_comment_counts.saved_child_replies, 0) + "
        "coalesce(d.facebook_comments, 0) + "
        "coalesce(saved_comment_counts.classified_missing_comments, 0)"
    )
    missing_comments_sql = f"greatest(d.comments_count - greatest(coalesce({accounted_comments_sql}, 0), 0), 0)"
    filter_where_sql = _comments_only_profile_filter_where_sql(
        comment_filter="incomplete",
        reported_comments_sql="d.comments_count",
        saved_comments_sql="saved_comment_counts.saved_comments",
        accounted_comments_sql=accounted_comments_sql,
        reported_comments_is_normalized=True,
    )
    collaborator_membership_available = _instagram_catalog_collaborator_membership_available()
    collaborator_rows_sql = ""
    deduped_rows_sql = """
        deduped_rows as materialized (
          select
            shortcode,
            max(posted_at) as posted_at,
            max(comments_count)::bigint as comments_count,
            max(facebook_comments)::bigint as facebook_comments,
            max(_profile_dataset_priority)::int as _profile_dataset_priority
          from owner_rows
          group by shortcode
        ),
    """
    params: list[Any] = [normalized_account, *owner_window_params]
    if collaborator_membership_available:
        collaborator_rows_sql = f"""
        collaborator_rows as materialized (
          select
            p.source_id as shortcode,
            p.posted_at,
            {catalog_reported_comments_expr}::bigint as comments_count,
            {catalog_facebook_comments_expr}::bigint as facebook_comments,
            1::int as _profile_dataset_priority
          from social.instagram_account_catalog_post_collaborators m
          join social.instagram_account_catalog_posts p
            on p.id = m.catalog_post_id
          where m.collaborator_handle = %s
            and lower(p.source_account) <> %s
            and nullif(p.source_id, '') is not null
            and {catalog_reported_comments_expr} > 0{collaborator_window_sql}
        ),
        """
        deduped_rows_sql = """
        deduped_rows as materialized (
          select
            shortcode,
            max(posted_at) as posted_at,
            max(comments_count)::bigint as comments_count,
            max(facebook_comments)::bigint as facebook_comments,
            max(_profile_dataset_priority)::int as _profile_dataset_priority
          from (
            select * from owner_rows
            union all
            select * from collaborator_rows
          ) candidate_rows
          group by shortcode
        ),
        """
        params.extend([normalized_account, normalized_account, *collaborator_window_params])
    sql = f"""
        with owner_rows as materialized (
          select
            p.shortcode as shortcode,
            p.posted_at,
            {reported_comments_expr}::bigint as comments_count,
            {facebook_comments_expr}::bigint as facebook_comments,
            3::int as _profile_dataset_priority
          from social.instagram_posts p
          where {owner_match_clause}
            and nullif(p.shortcode, '') is not null
            and {reported_comments_expr} > 0{owner_window_sql}
        ),
        {collaborator_rows_sql}
        {deduped_rows_sql}
        saved_comment_counts as materialized (
          select
            p.shortcode,
            count(distinct c.id) filter (
              where {active_condition} and not ({fb_crosspost_condition}) and not {reply_condition}
            )::int as saved_parent_comments,
            count(distinct c.id) filter (
              where {active_condition} and not ({fb_crosspost_condition}) and {reply_condition}
            )::int as saved_child_replies,
            count(distinct c.id) filter (
              where {active_condition} and not ({fb_crosspost_condition})
            )::int as saved_comments,
            count(distinct c.id) filter (
              where {missing_condition} and not ({fb_crosspost_condition})
            )::int as classified_missing_comments
          from social.instagram_comments c
          join social.instagram_posts p
            on p.id = c.post_id
          join deduped_rows d
            on p.shortcode = d.shortcode
          group by p.shortcode
        )
        select d.shortcode
        from deduped_rows d
        left join saved_comment_counts
          on saved_comment_counts.shortcode = d.shortcode
        where {filter_where_sql}
        order by
          {missing_comments_sql} desc,
          d.comments_count desc nulls last,
          d.posted_at desc nulls last,
          d.shortcode desc
    """
    if safe_limit is not None:
        sql += " limit %s"
        params.append(safe_limit)
    rows = pg.fetch_all(sql, params)
    return [str(row.get("shortcode") or "").strip() for row in rows if str(row.get("shortcode") or "").strip()]


def _instagram_comments_reported_gap_is_tolerable(*, reported_comments: int, saved_comments: int) -> bool:
    reported = max(0, int(reported_comments or 0))
    saved = max(0, int(saved_comments or 0))
    unresolved_gap = max(0, reported - saved)
    if unresolved_gap <= 0:
        return True
    if reported <= 0 or saved <= 0:
        return False
    max_absolute_gap = _resolve_int_env_with_bounds(
        "SOCIAL_INSTAGRAM_COMMENTS_PREFILTER_GAP_MAX",
        _resolve_int_env_with_bounds("SOCIAL_INSTAGRAM_COMMENTS_HIDDEN_UNAVAILABLE_GAP_MAX", 1, minimum=0, maximum=50),
        minimum=0,
        maximum=500,
    )
    ratio_raw = (
        os.getenv("SOCIAL_INSTAGRAM_COMMENTS_PREFILTER_GAP_RATIO")
        or os.getenv("SOCIAL_INSTAGRAM_COMMENTS_HIDDEN_UNAVAILABLE_GAP_RATIO")
        or "0"
    )
    try:
        max_ratio = float(ratio_raw)
    except (TypeError, ValueError):
        max_ratio = 0.02
    max_ratio = max(0.0, min(max_ratio, 0.25))
    ratio_gap = int(reported * max_ratio)
    if ratio_gap < reported * max_ratio:
        ratio_gap += 1
    return unresolved_gap <= max(max_absolute_gap, ratio_gap)


def _instagram_filter_incomplete_comment_targets(
    account_handle: str,
    target_source_ids: Sequence[Any],
) -> list[str]:
    """Return requested shortcodes that still need comment hydration.

    Unknown shortcodes are preserved so a missing materialized post row does not
    accidentally drop work. Known posts are kept only when saved Instagram
    rows plus Facebook and classified-missing counts are below the reported
    Instagram total.
    """

    normalized_account = _normalize_social_account_profile_handle(account_handle)
    requested = _normalize_unique_terms(
        [str(item or "").strip() for item in target_source_ids if str(item or "").strip()]
    )
    if not normalized_account or not requested:
        return requested

    owner_match_clause = _social_account_profile_owner_match_sql("instagram", alias="p")
    reported_comments_expr = _instagram_reported_comments_sql("p")
    facebook_comments_expr = _instagram_external_facebook_comments_sql("p")
    lifecycle_supported = _comment_lifecycle_supported("instagram_comments")
    active_condition = "c.is_missing is not true" if lifecycle_supported else "true"
    missing_condition = "c.is_missing is true" if lifecycle_supported else "false"
    parent_external_expr = "nullif(coalesce(to_jsonb(c) ->> 'parent_comment_external_id', ''), '')"
    reply_depth_expr = """
        case
          when coalesce(to_jsonb(c) ->> 'reply_depth', '') ~ '^[0-9]+$'
          then (to_jsonb(c) ->> 'reply_depth')::int
          else 0
        end
    """
    reply_condition = f"""
        (
          coalesce(c.is_reply, false)
          or c.parent_comment_id is not null
          or {parent_external_expr} is not null
          or ({reply_depth_expr}) > 0
        )
    """
    rows = pg.fetch_all(
        f"""
        with requested as (
          select
            nullif(shortcode, '')::text as shortcode,
            ordinality::int as sort_order
          from unnest(%s::text[]) with ordinality as request(shortcode, ordinality)
          where nullif(shortcode, '') is not null
        ),
        owner_posts as (
          select
            p.id as post_id,
            p.shortcode::text as shortcode,
            {reported_comments_expr}::bigint as reported_comments,
            {facebook_comments_expr}::bigint as facebook_comments,
            row_number() over (
              partition by p.shortcode
              order by p.posted_at desc nulls last, p.id desc
            ) as row_number
          from social.instagram_posts p
          join requested r on r.shortcode = p.shortcode
          where {owner_match_clause}
        ),
        selected_posts as (
          select post_id, shortcode, reported_comments, facebook_comments
          from owner_posts
          where row_number = 1
        ),
        saved_comment_counts as (
          select
            sp.shortcode,
            count(c.id) filter (
              where {active_condition} and not {reply_condition}
            )::bigint as saved_parent_comments,
            count(c.id) filter (
              where {active_condition} and {reply_condition}
            )::bigint as saved_child_replies,
            count(c.id) filter (where {missing_condition})::bigint as classified_missing_comments
          from selected_posts sp
          left join social.instagram_comments c on c.post_id = sp.post_id
          group by sp.shortcode
        )
        select
          r.shortcode,
          sp.post_id is null as post_missing,
          coalesce(sp.reported_comments, 0)::bigint as reported_comments,
          coalesce(scc.saved_parent_comments, 0)::bigint as saved_parent_comments,
          coalesce(scc.saved_child_replies, 0)::bigint as saved_child_replies,
          coalesce(sp.facebook_comments, 0)::bigint as facebook_comments,
          coalesce(scc.classified_missing_comments, 0)::bigint as classified_missing_comments
        from requested r
        left join selected_posts sp on sp.shortcode = r.shortcode
        left join saved_comment_counts scc on scc.shortcode = r.shortcode
        order by r.sort_order
        """,
        [requested, normalized_account],
    )
    if not rows:
        logger.warning(
            "Instagram comments incomplete-target filter returned no verification rows; preserving requested targets: "
            "account=%s target_count=%d",
            normalized_account,
            len(requested),
        )
        return requested
    incomplete: list[str] = []
    for row in rows:
        shortcode = str(row.get("shortcode") or "").strip()
        if not shortcode:
            continue
        if row.get("post_missing"):
            incomplete.append(shortcode)
            continue
        reported = _normalize_non_negative_int(row.get("reported_comments"))
        saved = _normalize_non_negative_int(row.get("saved_parent_comments")) + _normalize_non_negative_int(
            row.get("saved_child_replies")
        )
        accounted = (
            saved
            + _normalize_non_negative_int(row.get("facebook_comments"))
            + _normalize_non_negative_int(row.get("classified_missing_comments"))
        )
        if reported > accounted and not _instagram_comments_reported_gap_is_tolerable(
            reported_comments=reported,
            saved_comments=accounted,
        ):
            incomplete.append(shortcode)
    return incomplete


def _instagram_comments_profile_shard_count(target_count: int) -> int:
    if target_count <= 1:
        return 1
    recommended = _instagram_comments_recommended_shard_count(target_count=target_count)
    raw_value = str(os.getenv("SOCIAL_INSTAGRAM_COMMENTS_PROFILE_SHARD_COUNT") or "").strip()
    if not raw_value:
        return recommended
    try:
        requested = int(raw_value)
    except ValueError:
        return recommended
    requested = max(1, min(requested, _instagram_comments_max_shard_count()))
    return min(requested, target_count)


def _normalize_instagram_comments_worker_count(value: Any, *, target_count: int | None = None) -> int | None:
    requested = _normalize_non_negative_int(value)
    if requested <= 0:
        return None
    effective_target_count = max(1, int(target_count or requested))
    return min(max(1, requested), min(24, effective_target_count))


def _instagram_comments_job_max_attempts(config: Mapping[str, Any] | None = None) -> int:
    raw_value = None
    if config is not None:
        raw_value = config.get("comments_max_attempts")
    if raw_value is None:
        raw_value = os.getenv("SOCIAL_INSTAGRAM_COMMENTS_MAX_ATTEMPTS")
    raw_text = str(raw_value or "").strip()
    if not raw_text:
        return SOCIAL_INSTAGRAM_COMMENTS_MAX_ATTEMPTS_DEFAULT
    try:
        requested = int(raw_text)
    except ValueError:
        return SOCIAL_INSTAGRAM_COMMENTS_MAX_ATTEMPTS_DEFAULT
    return max(1, min(requested, 12))


def _instagram_comments_recommended_shard_count(
    *,
    target_count: int,
    modal_pending_jobs: int = 0,
    db_pressure_high: bool = False,
) -> int:
    if target_count <= 1:
        return 1
    if db_pressure_high or modal_pending_jobs >= 25:
        return min(2, target_count)
    if target_count >= 1000:
        return min(8, target_count)
    if target_count >= 300:
        return min(4, target_count)
    if target_count >= 100:
        return min(4, target_count)
    return min(2, target_count)


def _chunk_instagram_comment_targets(target_source_ids: Sequence[str], shard_count: int) -> list[list[str]]:
    targets = [str(item or "").strip() for item in target_source_ids if str(item or "").strip()]
    if not targets:
        return []
    effective_shard_count = max(1, min(int(shard_count or 1), len(targets)))
    base_size, remainder = divmod(len(targets), effective_shard_count)
    chunks: list[list[str]] = []
    start = 0
    for index in range(effective_shard_count):
        chunk_size = base_size + (1 if index < remainder else 0)
        chunks.append(targets[start : start + chunk_size])
        start += chunk_size
    return chunks


def _comments_shard_count_for_batch_size(*, target_count: int, batch_size: int | None) -> int | None:
    if target_count <= 0 or batch_size is None:
        return None
    requested_batch_size = max(0, int(batch_size or 0))
    if requested_batch_size <= 0:
        return None
    return min(
        _instagram_comments_max_shard_count(),
        target_count,
        max(1, math.ceil(target_count / requested_batch_size)),
    )


def _instagram_comments_max_shard_count() -> int:
    raw_value = str(os.getenv("SOCIAL_INSTAGRAM_COMMENTS_MAX_SHARD_COUNT") or "").strip()
    if not raw_value:
        return 1000
    try:
        requested = int(raw_value)
    except ValueError:
        return 1000
    return max(1, min(requested, 5000))


def _create_instagram_comments_shard_jobs(
    *,
    run_id: str,
    platform: str,
    source_scope: str,
    source_id: str | None,
    account_handle: str,
    mode: str,
    run_config: Mapping[str, Any],
    target_source_id_shards: Sequence[Sequence[str]],
    target_source_ids_count: int,
    comments_shard_count: int,
    initiated_by: str | None,
    job_status: str,
    priority: int,
    max_attempts: int,
    required_worker_lane: str | None,
    required_execution_backend: str | None,
    inline_worker_id: str | None,
    conn: Any,
) -> tuple[list[str], str]:
    if (
        inline_worker_id
        or len(target_source_id_shards) < _instagram_comments_bulk_insert_threshold()
        or not bool(_scrape_jobs_features().get("has_queue_fields"))
    ):
        job_ids: list[str] = []
        for shard_index, target_source_id_shard in enumerate(target_source_id_shards, start=1):
            job_ids.append(
                _create_job(
                    None,
                    run_id=run_id,
                    platform=platform,
                    source_scope=source_scope,
                    job_type="comments",
                    stage=INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
                    config={
                        **dict(run_config),
                        "source_id": source_id if mode == "single_post" else None,
                        "target_source_ids": list(target_source_id_shard),
                        "target_source_ids_count": target_source_ids_count,
                        "comments_shard_index": shard_index,
                        "comments_shard_count": comments_shard_count,
                        "comments_shard_target_count": len(target_source_id_shard),
                        "account": account_handle,
                        "required_worker_lane": required_worker_lane,
                        "required_execution_backend": required_execution_backend,
                    },
                    initiated_by=initiated_by,
                    status=job_status,
                    priority=priority,
                    max_attempts=max_attempts,
                    worker_id=inline_worker_id if comments_shard_count == 1 else None,
                    preclaim=bool(inline_worker_id and comments_shard_count == 1),
                    conn=conn,
                    track_run_counters=False,
                )
            )
        return job_ids, "single"

    required_backend = _job_required_execution_backend(dict(run_config), platform=platform)
    creator_runtime_version = dict(_resolve_runtime_version_stamp())
    effective_runtime_version = _resolve_effective_runtime_version(
        required_execution_backend=required_backend,
        stage=INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
    )
    runtime_metadata = _job_runtime_metadata(
        INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
        runtime_version=effective_runtime_version,
        created_by_runtime_version=creator_runtime_version,
    )
    values: list[tuple[Any, ...]] = []
    for shard_index, target_source_id_shard in enumerate(target_source_id_shards, start=1):
        config_payload = {
            **dict(run_config),
            "source_id": source_id if mode == "single_post" else None,
            "target_source_ids": list(target_source_id_shard),
            "target_source_ids_count": target_source_ids_count,
            "comments_shard_index": shard_index,
            "comments_shard_count": comments_shard_count,
            "comments_shard_target_count": len(target_source_id_shard),
            "account": account_handle,
            "required_worker_lane": required_worker_lane,
            "required_execution_backend": required_execution_backend,
        }
        if required_backend:
            config_payload.setdefault("required_execution_backend", required_backend)
        if effective_runtime_version:
            config_payload.setdefault("required_runtime_version", effective_runtime_version)
        elif required_backend != "modal":
            config_payload.setdefault("required_runtime_version", creator_runtime_version)
        if creator_runtime_version:
            config_payload.setdefault("created_by_runtime_version", creator_runtime_version)
        values.append(
            (
                run_id,
                platform,
                "comments",
                _json_dumps(config_payload),
                job_status,
                priority,
                source_scope,
                initiated_by,
                _json_dumps(runtime_metadata),
                max(1, int(max_attempts)),
            )
        )
    rows = pg.execute_values_returning(
        """
        insert into social.scrape_jobs (
          run_id,
          platform,
          job_type,
          config,
          status,
          available_at,
          priority,
          source_scope,
          initiated_by,
          metadata,
          attempt_count,
          max_attempts
        )
        select
          run_id::uuid,
          platform,
          job_type,
          config::jsonb,
          status,
          now(),
          priority,
          source_scope,
          initiated_by,
          metadata::jsonb,
          0,
          max_attempts
        from (values %s) as v(
          run_id,
          platform,
          job_type,
          config,
          status,
          priority,
          source_scope,
          initiated_by,
          metadata,
          max_attempts
        )
        returning id::text
        """,
        values,
        conn=conn,
    )
    return [str(row.get("id") or "").strip() for row in rows if str(row.get("id") or "").strip()], "bulk"


def _instagram_comments_launch_auth_check_enabled() -> bool:
    raw = str(os.getenv("SOCIAL_INSTAGRAM_COMMENTS_LAUNCH_AUTH_CHECK") or "").strip().lower()
    if raw:
        return raw in {"1", "true", "yes", "on"}
    return not bool(os.getenv("PYTEST_CURRENT_TEST"))


def _comments_launch_auth_metadata(
    *,
    attempted: bool = False,
    status: str = "skipped",
    reason: str | None = None,
    probe: dict[str, Any] | None = None,
    repair_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "auth_repair_attempted": bool(attempted),
        "auth_repair_status": str(status or "skipped").strip().lower() or "skipped",
        "auth_repair_reason": str(reason or "").strip() or None,
        "comments_auth_probe": probe or None,
        "auth_repair_result": repair_result or None,
    }


def _public_comments_launch_auth_metadata(metadata: Mapping[str, Any] | None) -> dict[str, Any]:
    data = _metadata_dict(metadata)
    if not data:
        return {}
    return {
        "auth_repair_attempted": bool(data.get("auth_repair_attempted")),
        "auth_repair_status": str(data.get("auth_repair_status") or "skipped").strip().lower() or "skipped",
        "auth_repair_reason": str(data.get("auth_repair_reason") or "").strip() or None,
        "comments_auth_probe": _metadata_dict(data.get("comments_auth_probe")) or None,
    }


def _comments_launch_auth_blocker_detail(
    *,
    account_handle: str,
    probe: Mapping[str, Any] | None,
    reason: str | None,
) -> dict[str, Any]:
    probe_payload = _metadata_dict(probe)
    probe_shortcode = str(probe_payload.get("shortcode") or "").strip() or None
    normalized_reason = str(reason or probe_payload.get("reason") or "comments_auth_probe_failed").strip()
    return {
        "platform": "instagram",
        "account_handle": _normalize_social_account_profile_handle(account_handle),
        "probe_shortcode": probe_shortcode,
        "reason": normalized_reason,
        "status": str(probe_payload.get("status") or probe_payload.get("result") or "auth_blocked").strip().lower()
        or "auth_blocked",
        "session_source": str(
            probe_payload.get("session_source")
            or probe_payload.get("auth_source")
            or probe_payload.get("auth_session_source")
            or ""
        ).strip()
        or None,
        "cookie_fingerprint": str(probe_payload.get("cookie_fingerprint") or "").strip() or None,
        "cookie_fingerprint_algorithm": str(probe_payload.get("cookie_fingerprint_algorithm") or "").strip() or None,
        "operator_action": "Repair Instagram comments auth, then relaunch the shared-account comments job.",
        "comments_auth_probe": probe_payload or None,
    }


def _probe_instagram_comments_endpoint_for_launch(
    *,
    account_handle: str,
    shortcode: str,
) -> dict[str, Any]:
    """Probe the comments API with the same session/proxy path used by jobs."""

    async def _probe() -> dict[str, Any]:
        from trr_backend.socials.instagram.comments_scrapling.fetcher import InstagramCommentsScraplingFetcher
        from trr_backend.socials.instagram.comments_scrapling.proxy import select_comments_proxy
        from trr_backend.socials.instagram.comments_scrapling.session import resolve_comments_scrapling_session

        session = resolve_comments_scrapling_session(
            browser_account_id=account_handle,
            caller_context=f"comments_launch_auth_probe:{account_handle}",
        )
        cookie_fingerprint = _instagram_cookie_fingerprint(session.auth_session.cookies)[:16]
        proxy_session_key = str(session.browser_account_id or account_handle).strip().lower().lstrip("@")
        fetcher = InstagramCommentsScraplingFetcher(
            cookies=session.cookies,
            raw_cookies=session.auth_session.cookies,
            browser_account_id=session.browser_account_id,
            proxy_config=select_comments_proxy(session_key=proxy_session_key or account_handle),
        )
        try:
            await fetcher.warmup()
            payload = await fetcher.validate_comments_endpoint(shortcode, mode="comments_endpoint")
            runtime_metadata = _metadata_dict(fetcher.runtime_metadata)
            return {
                **payload,
                "account_handle": account_handle,
                "cookie_fingerprint": cookie_fingerprint,
                "cookie_fingerprint_algorithm": "sha256:16",
                "auth_source": str(session.auth_session.source or "").strip() or None,
                "session_source": str(session.auth_session.source or "").strip() or None,
                "transport_failures": _metadata_dict(runtime_metadata.get("transport_failures")) or None,
                "challenge_responses": _metadata_dict(runtime_metadata.get("challenge_responses")) or None,
                "retry_reason_counts": _metadata_dict(runtime_metadata.get("retry_reason_counts")) or None,
            }
        finally:
            await fetcher.aclose()

    try:
        return asyncio.run(_probe())
    except Exception as exc:  # noqa: BLE001
        error_code = str(getattr(exc, "error_code", "") or "").strip().lower()
        if error_code in {
            "instagram_comments_warmup_auth_failed",
            "instagram_comments_warmup_no_cookies",
            "instagram_comments_cookie_bridge_failed",
            "instagram_comments_endpoint_auth_blocked",
        }:
            status = "auth_blocked"
            retryable = False
        elif error_code in {
            "instagram_comments_warmup_transport_error",
            "instagram_comments_endpoint_transport_blocked",
        }:
            status = "transport_blocked"
            retryable = True
        else:
            status = "transport_blocked"
            retryable = True
        return {
            "mode": "comments_endpoint",
            "account_handle": account_handle,
            "shortcode": shortcode,
            "status": status,
            "result": status,
            "reason": error_code or exc.__class__.__name__,
            "retryable": retryable,
            "exception_class": exc.__class__.__name__,
        }


def _ensure_instagram_comments_auth_ready_for_launch(
    *,
    account_handle: str,
    representative_shortcode: str | None,
) -> dict[str, Any]:
    _sync_core_overrides()
    shortcode = str(representative_shortcode or "").strip()
    if not _instagram_comments_launch_auth_check_enabled() or not shortcode:
        reason = "development_only_auth_probe_bypass" if os.getenv("PYTEST_CURRENT_TEST") else "auth_probe_unavailable"
        return _comments_launch_auth_metadata(status="skipped", reason=reason)

    first_probe = _probe_instagram_comments_endpoint_for_launch(
        account_handle=account_handle,
        shortcode=shortcode,
    )
    first_status = str(first_probe.get("status") or first_probe.get("result") or "").strip().lower()
    if first_status == "valid":
        return _comments_launch_auth_metadata(status="skipped", probe=first_probe)
    if first_status != "auth_blocked":
        return _comments_launch_auth_metadata(
            status="skipped",
            reason=str(first_probe.get("reason") or first_status or "comments_auth_probe_not_valid").strip() or None,
            probe=first_probe,
        )
    first_reason = str(first_probe.get("reason") or "").strip().lower()
    if first_reason == _BROWSER_SESSION_INVALIDATED_REASON:
        return _comments_launch_auth_metadata(
            status="failed",
            reason=_BROWSER_SESSION_INVALIDATED_REASON,
            probe=first_probe,
        )
    repair_result = refresh_platform_cookies_interactive(
        "instagram",
        headless=True,
        timeout_seconds=300,
        account_handle=account_handle,
    )
    repair_payload = _metadata_dict(repair_result)
    if not bool(repair_payload.get("success")):
        reason = str(repair_payload.get("reason") or "instagram_auth_repair_failed").strip().lower()
        return _comments_launch_auth_metadata(
            attempted=True,
            status="failed",
            reason=reason,
            probe=first_probe,
            repair_result=repair_payload,
        )

    second_probe = _probe_instagram_comments_endpoint_for_launch(
        account_handle=account_handle,
        shortcode=shortcode,
    )
    second_status = str(second_probe.get("status") or second_probe.get("result") or "").strip().lower()
    if second_status == "valid":
        return _comments_launch_auth_metadata(
            attempted=True,
            status="succeeded",
            probe=second_probe,
            repair_result=repair_payload,
        )

    reason = str(second_probe.get("reason") or second_status or "comments_auth_probe_failed_after_repair").strip()
    return _comments_launch_auth_metadata(
        attempted=True,
        status="failed",
        reason=reason or "comments_auth_probe_failed_after_repair",
        probe=second_probe,
        repair_result=repair_payload,
    )


def start_social_account_comments_scrape(
    platform: str,
    account_handle: str,
    *,
    mode: str,
    source_scope: str = "network",
    source_id: str | None = None,
    max_posts: int | None = None,
    max_comments_per_post: int | None = None,
    refresh_policy: str = "stale_or_missing",
    target_filter: str | None = None,
    comments_load_strategy: str = "public_relay",
    initiated_by: str | None = None,
    inline_worker_id: str | None = None,
    allow_local_dev_inline_bypass: bool = False,
    comments_enable_media_followups: bool = False,
    launch_group_id: str | None = None,
    dispatch_immediately: bool = True,
    skip_launch_auth_probe: bool = False,
    target_source_ids: Sequence[Any] | None = None,
    comments_worker_count: int | None = None,
    comments_target_batch_size: int | None = None,
    cancel_active_before_relaunch: bool | None = None,
    date_start: str | None = None,
    date_end: str | None = None,
) -> dict[str, Any]:
    _sync_core_overrides()
    normalized_platform = _normalize_social_account_profile_platform(platform)
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    normalized_mode = str(mode or "").strip().lower()
    normalized_refresh_policy = str(refresh_policy or "stale_or_missing").strip().lower()
    normalized_target_filter = _normalize_instagram_comments_target_filter(target_filter)
    window_start, window_end = _normalize_comment_date_window(date_start, date_end)
    normalized_date_start = window_start.isoformat() if window_start is not None else None
    normalized_date_end = window_end.isoformat() if window_end is not None else None
    public_comments_mode = comments_public_mode_from_config(
        {
            "comments_load_strategy": comments_load_strategy,
        }
    )
    effective_comments_load_strategy = comments_load_strategy_for_mode(
        comments_load_strategy,
        public_mode=public_comments_mode,
    )
    normalized_load_strategy = _normalize_instagram_comments_load_strategy(effective_comments_load_strategy)
    _assert_instagram_comments_load_strategy_enabled(normalized_load_strategy)
    if normalized_platform != "instagram":
        raise SocialIngestValidationError(
            "SOCIAL_ACCOUNT_COMMENTS_UNSUPPORTED_PLATFORM",
            "Standalone comments scraping is currently only supported for Instagram.",
        )
    if normalized_mode not in {"profile", "single_post"}:
        raise SocialIngestValidationError("SOCIAL_ACCOUNT_COMMENTS_INVALID_MODE", "Unsupported comments scrape mode.")
    if normalized_mode == "profile" and normalized_refresh_policy not in {"stale_or_missing", "all_saved_posts"}:
        raise SocialIngestValidationError(
            "SOCIAL_ACCOUNT_COMMENTS_INVALID_REFRESH_POLICY",
            "Profile comments scraping supports stale_or_missing and all_saved_posts refreshes.",
        )
    if normalized_mode != "profile" and normalized_target_filter is not None:
        raise SocialIngestValidationError(
            "SOCIAL_ACCOUNT_COMMENTS_INVALID_TARGET_FILTER",
            "target_filter is only supported for profile comment scrapes.",
        )
    _assert_social_account_profile_exists(normalized_platform, normalized_account)
    normalized_max_posts = None if max_posts is None else max(1, int(max_posts))
    normalized_max_comments_per_post = (
        None if max_comments_per_post is None else max(0, int(max_comments_per_post))
    )
    explicit_target_source_ids = list(
        dict.fromkeys(str(item or "").strip() for item in list(target_source_ids or []) if str(item or "").strip())
    )
    should_cancel_active_before_relaunch = _instagram_comments_cancel_active_before_relaunch_enabled(
        cancel_active_before_relaunch
    )
    effective_profile_max_comments_per_post = (
        0
        if normalized_mode == "profile" and normalized_max_comments_per_post is None
        else normalized_max_comments_per_post
    )
    queue_enabled = is_queue_enabled()
    modal_queue_dispatch = queue_enabled and not allow_local_dev_inline_bypass and is_modal_remote_executor_enabled()
    required_worker_lane = None if modal_queue_dispatch else INSTAGRAM_COMMENTS_SCRAPLING_WORKER_LANE
    required_execution_backend = "modal" if modal_queue_dispatch else None
    comments_auth_preflight_platform = (
        None
        if public_comments_mode
        else normalized_platform
        if _instagram_comments_launch_auth_check_enabled() and not skip_launch_auth_probe
        else None
    )
    if queue_enabled and not allow_local_dev_inline_bypass:
        if modal_queue_dispatch:
            assert_worker_available_when_queue_enabled(
                required_execution_backend="modal",
                platform=comments_auth_preflight_platform,
            )
        else:
            assert_worker_available_when_queue_enabled(
                required_worker_lane=INSTAGRAM_COMMENTS_SCRAPLING_WORKER_LANE,
                platform=comments_auth_preflight_platform,
            )

    lock_key = _social_account_comments_start_lock_key(normalized_platform, normalized_account)
    lock_label = f"comments-scrape-lock:{normalized_platform}:{normalized_account[:48]}"
    run_id: str | None = None
    payload: dict[str, Any] | None = None
    cancelled_active_run: dict[str, Any] | None = None
    with pg.db_connection(label=lock_label, pool_name="social_control") as lock_conn:
        with pg.db_cursor(conn=lock_conn, label=lock_label) as cur:
            lock_row = pg.fetch_one_with_cursor(cur, "select pg_try_advisory_lock(%s) as locked", [lock_key]) or {}
        if not bool(lock_row.get("locked")):
            active_run = _room_callable(
                "get_active_social_account_comments_run",
                get_active_social_account_comments_run,
            )(
                normalized_platform,
                normalized_account,
                conn=lock_conn,
            )
            if not active_run:
                raise SocialIngestConflictError(
                    "SOCIAL_ACCOUNT_COMMENTS_LAUNCH_IN_PROGRESS",
                    f"Comments sync is already starting for @{normalized_account}.",
                    detail={
                        "platform": normalized_platform,
                        "account_handle": normalized_account,
                        "status": "starting",
                        "retryable": True,
                    },
                )
            raise SocialIngestConflictError(
                "SOCIAL_ACCOUNT_COMMENTS_RUN_ALREADY_ACTIVE",
                (
                    f"Comments scrape run {active_run.get('run_id') or 'unknown'} "
                    f"is already active for @{normalized_account}."
                ),
                detail=active_run,
            )
        try:
            active_run = _room_callable(
                "get_active_social_account_comments_run",
                get_active_social_account_comments_run,
            )(
                normalized_platform,
                normalized_account,
                conn=lock_conn,
            )
            if active_run:
                if not should_cancel_active_before_relaunch:
                    raise SocialIngestConflictError(
                        "SOCIAL_ACCOUNT_COMMENTS_RUN_ALREADY_ACTIVE",
                        (
                            f"Comments scrape run {active_run.get('run_id') or 'unknown'} "
                            f"is already active for @{normalized_account}."
                        ),
                        detail=active_run,
                    )
                cancelled_active_run = cancel_social_account_comments_run(
                    platform=normalized_platform,
                    account_handle=normalized_account,
                    run_id=str(active_run.get("run_id") or ""),
                    cancelled_by="comments_relaunch_guard",
                    conn=lock_conn,
                )
            target_source_ids: list[str]
            target_enumeration_started_at = time_module.perf_counter()
            if normalized_mode == "single_post":
                normalized_source_id = str(source_id or "").strip()
                if not normalized_source_id and len(explicit_target_source_ids) == 1:
                    normalized_source_id = explicit_target_source_ids[0]
                if not normalized_source_id:
                    raise SocialIngestValidationError(
                        "SOCIAL_ACCOUNT_COMMENTS_SOURCE_ID_REQUIRED",
                        "source_id is required for single-post comment scrapes.",
                    )
                target_source_ids = [normalized_source_id]
            else:
                if explicit_target_source_ids:
                    target_source_ids = explicit_target_source_ids
                elif normalized_target_filter == "incomplete":
                    target_source_ids = _room_callable(
                        "_instagram_social_account_incomplete_comment_target_shortcodes",
                        _instagram_social_account_incomplete_comment_target_shortcodes,
                    )(
                        normalized_account,
                        limit=normalized_max_posts,
                        date_start=normalized_date_start,
                        date_end=normalized_date_end,
                    )
                else:
                    target_source_ids = _room_callable(
                        "_instagram_social_account_comment_target_shortcodes",
                        _instagram_social_account_comment_target_shortcodes,
                    )(
                        normalized_account,
                        limit=normalized_max_posts,
                        refresh_policy=normalized_refresh_policy,
                        date_start=normalized_date_start,
                        date_end=normalized_date_end,
                    )
                if not target_source_ids:
                    message = (
                        f"No incomplete Instagram comments were found for @{normalized_account}."
                        if normalized_target_filter == "incomplete"
                        else (
                            f"No saved Instagram posts were found for @{normalized_account}."
                            if normalized_refresh_policy == "all_saved_posts"
                            else f"No stale or missing Instagram comments were found for @{normalized_account}."
                        )
                    )
                    raise SocialIngestValidationError(
                        "SOCIAL_ACCOUNT_COMMENTS_NOTHING_TO_REFRESH",
                        message,
                    )
            target_enumeration_ms = round((time_module.perf_counter() - target_enumeration_started_at) * 1000, 1)

            deferred_launch_auth_reason = "catalog_parallel_launch" if skip_launch_auth_probe else None
            if public_comments_mode:
                launch_auth_metadata = _comments_launch_auth_metadata(
                    status="skipped",
                    reason="public_relay_mode_no_auth_probe",
                )
            else:
                launch_auth_metadata = (
                    _comments_launch_auth_metadata(
                        status="deferred",
                        reason=deferred_launch_auth_reason,
                    )
                    if deferred_launch_auth_reason
                    else _ensure_instagram_comments_auth_ready_for_launch(
                        account_handle=normalized_account,
                        representative_shortcode=target_source_ids[0] if target_source_ids else None,
                    )
                )
            public_launch_auth_metadata = _public_comments_launch_auth_metadata(launch_auth_metadata)
            if public_launch_auth_metadata.get("auth_repair_status") == "failed":
                reason = str(
                    public_launch_auth_metadata.get("auth_repair_reason") or "instagram_auth_repair_failed"
                ).strip()
                raise SocialIngestValidationError(
                    "SOCIAL_INSTAGRAM_COMMENTS_AUTH_REPAIR_FAILED",
                    f"Instagram comments auth repair failed before launch: {reason.replace('_', ' ')}.",
                    detail=_comments_launch_auth_blocker_detail(
                        account_handle=normalized_account,
                        probe=public_launch_auth_metadata.get("comments_auth_probe"),
                        reason=reason,
                    ),
                )

            target_source_ids_count = len(target_source_ids)
            recommended_comments_shard_count = _instagram_comments_recommended_shard_count(
                target_count=target_source_ids_count
            )
            requested_comments_worker_count = _normalize_instagram_comments_worker_count(
                comments_worker_count,
                target_count=target_source_ids_count,
            )
            requested_comments_batch_shard_count = _comments_shard_count_for_batch_size(
                target_count=target_source_ids_count,
                batch_size=comments_target_batch_size,
            )
            default_comments_shard_count = (
                requested_comments_batch_shard_count
                or requested_comments_worker_count
                or (_instagram_comments_profile_shard_count(target_source_ids_count) if normalized_mode == "profile" else 1)
            )
            effective_comments_target_batch_size = (
                max(1, int(comments_target_batch_size or 0)) if requested_comments_batch_shard_count else None
            )
            comments_shard_count = (
                1
                if normalized_load_strategy == "single_session_load_all" and normalized_mode == "profile"
                else default_comments_shard_count
            )
            strategy_metadata = _instagram_comments_load_strategy_metadata(
                load_strategy=normalized_load_strategy,
                mode=normalized_mode,
                target_count=target_source_ids_count,
                recommended_shard_count=recommended_comments_shard_count,
                effective_shard_count=comments_shard_count,
            )
            strategy_warnings = _instagram_comments_load_strategy_warnings(strategy_metadata)
            target_source_id_shards = _chunk_instagram_comment_targets(target_source_ids, comments_shard_count)
            if not target_source_id_shards:
                target_source_id_shards = [target_source_ids]
                comments_shard_count = 1
                strategy_metadata = _instagram_comments_load_strategy_metadata(
                    load_strategy=normalized_load_strategy,
                    mode=normalized_mode,
                    target_count=target_source_ids_count,
                    recommended_shard_count=recommended_comments_shard_count,
                    effective_shard_count=comments_shard_count,
                )
                strategy_warnings = _instagram_comments_load_strategy_warnings(strategy_metadata)
            comments_max_attempts = _instagram_comments_job_max_attempts(
                {
                    "target_filter": normalized_target_filter,
                    "incomplete_fill": normalized_target_filter == "incomplete",
                }
            )
            comments_auth_validation_mode = "public_relay" if public_comments_mode else "comments_endpoint"
            run_status = "queued" if queue_enabled else "running"
            job_status = "queued" if queue_enabled else "pending"
            planned_job_creation_mode = (
                "bulk"
                if (
                    not inline_worker_id
                    and len(target_source_id_shards) >= _instagram_comments_bulk_insert_threshold()
                    and bool(_scrape_jobs_features().get("has_queue_fields"))
                )
                else "single"
            )
            job_creation_payload = {
                "mode": planned_job_creation_mode,
                "job_count": len(target_source_id_shards),
            }
            run_config = {
                "platform": normalized_platform,
                "account": normalized_account,
                "source_scope": source_scope,
                "mode": normalized_mode,
                "stage": INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
                "refresh_policy": normalized_refresh_policy,
                "target_filter": normalized_target_filter,
                "incomplete_fill": normalized_target_filter == "incomplete",
                "max_posts": normalized_max_posts if normalized_mode == "profile" else None,
                "max_comments_per_post": (
                    effective_profile_max_comments_per_post
                    if normalized_mode == "profile"
                    else normalized_max_comments_per_post
                ),
                "comments_enable_media_followups": bool(comments_enable_media_followups),
                "instagram_scrape_mode": PUBLIC_COMMENTS_SCRAPE_MODE if public_comments_mode else None,
                "comments_worker_count": requested_comments_worker_count,
                "comments_target_batch_size": effective_comments_target_batch_size,
                "date_start": normalized_date_start,
                "date_end": normalized_date_end,
                "target_window": (
                    {
                        "date_start": normalized_date_start,
                        "date_end": normalized_date_end,
                        "end_exclusive": True,
                    }
                    if (normalized_date_start is not None or normalized_date_end is not None)
                    else None
                ),
                "launch_group_id": str(launch_group_id or "").strip() or None,
                "required_worker_lane": required_worker_lane,
                "required_execution_backend": required_execution_backend,
                "allow_local_dev_inline_bypass": bool(allow_local_dev_inline_bypass),
                "ingest_mode": "comments_only",
                "target_source_ids_count": target_source_ids_count,
                "explicit_target_source_ids": bool(explicit_target_source_ids),
                **strategy_metadata,
                "comments_shard_count": comments_shard_count,
                "comments_sharding_enabled": comments_shard_count > 1,
                "comments_proxy_shard_sessions": (
                    str(os.getenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_SHARD_SESSIONS", "1")).strip().lower()
                    in {"1", "true", "yes", "on"}
                    and normalized_load_strategy != "single_session_load_all"
                ),
                "comments_auth_validation_mode": comments_auth_validation_mode,
                "recommended_comments_shard_count": recommended_comments_shard_count,
                "strategy_warnings": strategy_warnings,
                "comments_max_attempts": comments_max_attempts,
                "job_creation": job_creation_payload,
                "relaunch_guard": {
                    "cancel_active_before_relaunch": should_cancel_active_before_relaunch,
                    "cancelled_previous_run_id": (
                        str(cancelled_active_run.get("run_id") or "").strip() if cancelled_active_run else None
                    ),
                    "cancelled_previous_job_count": (
                        _normalize_non_negative_int(cancelled_active_run.get("cancelled_jobs"))
                        if cancelled_active_run
                        else 0
                    ),
                },
                "timing": {
                    "target_enumeration_ms": target_enumeration_ms,
                    "target_source_ids_count": target_source_ids_count,
                },
            }
            run_config = _public_comments_config_overlay(run_config)
            if public_launch_auth_metadata.get("comments_auth_probe") or public_launch_auth_metadata.get(
                "auth_repair_attempted"
            ):
                run_config.update(public_launch_auth_metadata)
            creator_runtime_version = dict(_resolve_runtime_version_stamp())
            effective_runtime_version = _resolve_effective_runtime_version(
                required_execution_backend=required_execution_backend,
                stage=INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
            )
            if effective_runtime_version:
                run_config["required_runtime_version"] = effective_runtime_version
            elif required_execution_backend != "modal":
                run_config["required_runtime_version"] = creator_runtime_version
            if creator_runtime_version:
                run_config["created_by_runtime_version"] = creator_runtime_version
            run_id = _create_run(
                None,
                source_scope=source_scope,
                initiated_by=initiated_by,
                config=run_config,
                status=run_status,
                conn=lock_conn,
            )
            job_ids, job_creation_mode = _create_instagram_comments_shard_jobs(
                run_id=run_id,
                platform=normalized_platform,
                source_scope=source_scope,
                source_id=source_id,
                account_handle=normalized_account,
                mode=normalized_mode,
                run_config=run_config,
                target_source_id_shards=target_source_id_shards,
                target_source_ids_count=target_source_ids_count,
                comments_shard_count=comments_shard_count,
                initiated_by=initiated_by,
                job_status=job_status,
                priority=105,
                max_attempts=comments_max_attempts,
                required_worker_lane=required_worker_lane,
                required_execution_backend=required_execution_backend,
                inline_worker_id=inline_worker_id,
                conn=lock_conn,
            )
            job_creation_payload = {"mode": job_creation_mode, "job_count": len(job_ids)}
            if _run_counter_columns_ready():
                _persist_run_counters_and_summary(
                    conn=lock_conn,
                    run_id=run_id,
                    total_jobs=len(job_ids),
                    completed_jobs=0,
                    failed_jobs=0,
                    active_jobs=len(job_ids) if _status_is_active(job_status) else 0,
                    items_found_total=0,
                    stage_counts={
                        INSTAGRAM_COMMENTS_SCRAPLING_STAGE: {
                            "total": len(job_ids),
                            "completed": 0,
                            "failed": 0,
                            "active": len(job_ids) if _status_is_active(job_status) else 0,
                        },
                    },
                )
            payload = {
                "run_id": run_id,
                "status": run_status,
                "mode": normalized_mode,
                "platform": normalized_platform,
                "account_handle": normalized_account,
                "target_source_ids": target_source_ids,
                "target_source_ids_count": target_source_ids_count,
                "target_filter": normalized_target_filter,
                "incomplete_fill": normalized_target_filter == "incomplete",
                **strategy_metadata,
                "comments_shard_count": comments_shard_count,
                "comments_sharding_enabled": comments_shard_count > 1,
                "recommended_comments_shard_count": recommended_comments_shard_count,
                "strategy_warnings": strategy_warnings,
                "timing": {
                    "target_enumeration_ms": target_enumeration_ms,
                    "target_source_ids_count": target_source_ids_count,
                },
                "comments_enable_media_followups": bool(comments_enable_media_followups),
                "comments_worker_count": requested_comments_worker_count,
                "job_creation": job_creation_payload,
                "launch_group_id": str(launch_group_id or "").strip() or None,
                "required_worker_lane": required_worker_lane,
                "required_execution_backend": required_execution_backend,
                "runtime_version": _metadata_dict(run_config.get("required_runtime_version")) or None,
                "created_by_runtime_version": _metadata_dict(run_config.get("created_by_runtime_version")) or None,
                "instagram_access_proof": _metadata_dict(run_config.get("instagram_access_proof")),
                "relaunch_guard": _metadata_dict(run_config.get("relaunch_guard")),
            }
            if public_launch_auth_metadata.get("comments_auth_probe") or public_launch_auth_metadata.get(
                "auth_repair_attempted"
            ):
                payload.update(public_launch_auth_metadata)
        finally:
            try:
                with pg.db_cursor(conn=lock_conn, label=lock_label) as cur:
                    pg.fetch_one_with_cursor(cur, "select pg_advisory_unlock(%s) as unlocked", [lock_key])
            except Exception:  # noqa: BLE001
                logger.debug(
                    "[comments-scrape-lock] advisory unlock failed for %s/%s",
                    normalized_platform,
                    normalized_account,
                    exc_info=True,
                )
    if queue_enabled and dispatch_immediately and run_id:
        dispatch_due_social_jobs(run_id=run_id)
    return payload or {}


def _normalize_instagram_comments_audit_retry_stop_reasons(
    stop_reasons: Sequence[Any] | None,
) -> list[str]:
    raw_values = stop_reasons or INSTAGRAM_COMMENTS_AUDIT_CURSOR_RETRY_STOP_REASONS
    values = [
        str(value or "").strip().lower()
        for value in raw_values
        if str(value or "").strip()
    ]
    return list(dict.fromkeys(values)) or list(INSTAGRAM_COMMENTS_AUDIT_CURSOR_RETRY_STOP_REASONS)


def _normalize_instagram_comments_audit_retry_shortcodes(shortcodes: Sequence[Any] | None) -> list[str]:
    normalized: list[str] = []
    for value in shortcodes or []:
        for part in str(value or "").split(","):
            shortcode = part.strip()
            if shortcode:
                normalized.append(shortcode)
    return list(dict.fromkeys(normalized))


def _normalize_instagram_comments_show_filter_values(values: Sequence[Any] | Any | None) -> list[str]:
    raw_values = values if isinstance(values, (list, tuple, set)) else [values]
    normalized: list[str] = []
    for value in raw_values:
        for part in str(value or "").split(","):
            item = part.strip()
            if item:
                normalized.append(item)
    return list(dict.fromkeys(normalized))


def _instagram_comments_show_filter_terms(values: Sequence[Any] | Any | None) -> list[str]:
    terms: list[str] = []
    for value in _normalize_instagram_comments_show_filter_values(values):
        lowered = value.strip().lower().lstrip("#")
        if not lowered:
            continue
        terms.append(lowered)
        compact = re.sub(r"[^a-z0-9]+", "", lowered)
        if compact:
            terms.append(compact)
        spaced = re.sub(r"[^a-z0-9]+", " ", lowered).strip()
        if spaced:
            terms.append(spaced)
    return list(dict.fromkeys(term for term in terms if term))


def _load_instagram_comments_audit_cursor_rows(
    *,
    account_handle: str,
    limit: int,
    shortcodes: Sequence[Any] | None = None,
    stop_reasons: Sequence[Any] | None = None,
    show_ids: Sequence[Any] | None = None,
    season_ids: Sequence[Any] | None = None,
    show_filters: Sequence[Any] | None = None,
) -> list[dict[str, Any]]:
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    safe_limit = max(1, min(int(limit or 1), 500))
    audit_row_limit = min(max(safe_limit * 50, safe_limit), 10_000)
    normalized_shortcodes = _normalize_instagram_comments_audit_retry_shortcodes(shortcodes)
    normalized_stop_reasons = _normalize_instagram_comments_audit_retry_stop_reasons(stop_reasons)
    normalized_show_ids = _normalize_instagram_comments_show_filter_values(show_ids)
    normalized_season_ids = _normalize_instagram_comments_show_filter_values(season_ids)
    normalized_show_terms = _instagram_comments_show_filter_terms(show_filters)
    params: list[Any] = [normalized_account, normalized_stop_reasons]
    shortcode_sql = ""
    if normalized_shortcodes:
        shortcode_sql = "and a.shortcode = any(%s::text[])"
        params.append(normalized_shortcodes)
    show_filter_sql = ""
    show_filter_clauses: list[str] = []
    if normalized_show_ids:
        show_filter_clauses.append("(p.show_id::text = any(%s::text[]) or sh.id::text = any(%s::text[]))")
        params.extend([normalized_show_ids, normalized_show_ids])
    if normalized_season_ids:
        show_filter_clauses.append("(p.season_id::text = any(%s::text[]) or se.id::text = any(%s::text[]))")
        params.extend([normalized_season_ids, normalized_season_ids])
    if normalized_show_terms:
        show_filter_clauses.append(
            """
            exists (
              select 1
              from unnest(%s::text[]) term(value)
              where
                lower(coalesce(sh.slug, '')) = term.value
                or lower(coalesce(sh.name, '')) = term.value
                or lower(regexp_replace(coalesce(sh.name, ''), '[^a-zA-Z0-9]+', '', 'g')) = term.value
                or lower(coalesce(p.caption, '')) like '%%' || term.value || '%%'
                or lower(regexp_replace(coalesce(p.caption, ''), '[^a-zA-Z0-9]+', '', 'g')) like '%%' || term.value || '%%'
                or lower(coalesce(p.raw_data::text, '')) like '%%' || term.value || '%%'
                or exists (
                  select 1
                  from jsonb_array_elements_text(coalesce(p.hashtags, '[]'::jsonb)) hashtag(value)
                  where lower(ltrim(hashtag.value, '#')) = term.value
                )
            )
            """
        )
        params.append(normalized_show_terms)
    if show_filter_clauses:
        show_filter_sql = "and (" + " or ".join(show_filter_clauses) + ")"
    params.append(audit_row_limit)
    return pg.fetch_all(
        f"""
        select
          a.post_id,
          a.shortcode,
          a.source_account,
          a.cursor_stop_reason,
          a.cursor_min_id,
          a.cursor_param,
          a.cursor_payload,
          a.created_at::text,
          p.show_id::text as show_id,
          p.season_id::text as season_id,
          sh.slug as show_slug,
          sh.name as show_name
        from social.instagram_post_comments_audit a
        left join social.instagram_posts p
          on p.id = a.post_id or p.shortcode = a.shortcode
        left join core.shows sh on sh.id = p.show_id
        left join core.seasons se on se.id = p.season_id
        where ltrim(lower(coalesce(a.source_account, '')), '@') = %s
          and a.cursor_stop_reason = any(%s::text[])
          and a.cursor_payload is not null
          and a.cursor_payload <> '{{}}'::jsonb
          {shortcode_sql}
          {show_filter_sql}
        order by a.created_at desc
        limit %s
        """,
        params,
    )


def _select_instagram_comments_audit_cursor_retry_targets(
    rows: Sequence[Mapping[str, Any]],
) -> tuple[list[str], list[dict[str, Any]]]:
    from trr_backend.socials.instagram.comments_scrapling import job_runner as comments_job_runner

    selected_shortcodes: list[str] = []
    selected_rows: list[dict[str, Any]] = []
    seen_shortcodes: set[str] = set()
    for row in rows:
        shortcode = str(row.get("shortcode") or "").strip()
        if not shortcode or shortcode in seen_shortcodes:
            continue
        top_level_checkpoint = comments_job_runner._normalize_audit_top_level_checkpoint(row)
        reply_checkpoints = comments_job_runner._normalize_audit_reply_checkpoints(row)
        if not top_level_checkpoint and not reply_checkpoints:
            continue
        seen_shortcodes.add(shortcode)
        selected_shortcodes.append(shortcode)
        selected_rows.append(
            {
                "shortcode": shortcode,
                "post_id": row.get("post_id"),
                "cursor_stop_reason": row.get("cursor_stop_reason"),
                "created_at": row.get("created_at"),
                "show_id": row.get("show_id"),
                "season_id": row.get("season_id"),
                "show_slug": row.get("show_slug"),
                "show_name": row.get("show_name"),
                "has_top_level_cursor": bool(top_level_checkpoint),
                "reply_resume_count": len(reply_checkpoints),
            }
        )
    return list(dict.fromkeys(selected_shortcodes)), selected_rows


def _active_instagram_comments_run_for_account(account_handle: str) -> dict[str, Any] | None:
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    return get_active_social_account_comments_run("instagram", normalized_account)


def _instagram_comments_audit_cursor_counts_by_shortcode(
    *,
    shortcodes: Sequence[str],
    active_run_id: str | None,
) -> dict[str, dict[str, Any]]:
    normalized_shortcodes = [str(shortcode or "").strip() for shortcode in shortcodes if str(shortcode or "").strip()]
    if not normalized_shortcodes:
        return {}
    rows = pg.fetch_all(
        """
        with targets as (
          select unnest(%s::text[]) as shortcode
        ),
        counts as (
          select
            p.shortcode,
            p.id::text as post_id,
            coalesce(p.comments_count, 0)::int as reported_comment_count,
            count(c.id) filter (where coalesce(c.is_missing, false) = false)::int as saved_comment_count
          from social.instagram_posts p
          left join social.instagram_comments c on c.post_id = p.id
          where p.shortcode = any(%s::text[])
          group by p.shortcode, p.id, p.comments_count
        ),
        active_jobs as (
          select
            t.shortcode,
            count(j.id)::int as job_count,
            count(j.id) filter (where j.status = 'queued')::int as queued_count,
            count(j.id) filter (where j.status = 'running')::int as running_count,
            count(j.id) filter (where j.status = 'completed')::int as completed_count,
            count(j.id) filter (where j.status = 'failed')::int as failed_count,
            count(j.id) filter (where j.status = 'cancelled')::int as cancelled_count,
            array_remove(array_agg(j.id::text order by j.created_at desc) filter (
              where j.status in ('queued', 'pending', 'retrying', 'running')
            ), null) as active_job_ids,
            array_remove(array_agg(jsonb_array_length(coalesce(j.config->'target_source_ids', '[]'::jsonb)) order by j.created_at desc) filter (
              where j.status in ('queued', 'pending', 'retrying', 'running')
            ), null) as active_job_target_counts
          from targets t
          left join social.scrape_jobs j
            on %s::uuid is not null
           and j.run_id = %s::uuid
           and coalesce(j.config->>'stage', j.metadata->>'stage', j.job_type) = %s
           and (j.config->'target_source_ids') ? t.shortcode
          group by t.shortcode
        )
        select
          t.shortcode,
          c.post_id,
          coalesce(c.reported_comment_count, 0)::int as reported_comment_count,
          coalesce(c.saved_comment_count, 0)::int as saved_comment_count,
          greatest(coalesce(c.reported_comment_count, 0) - coalesce(c.saved_comment_count, 0), 0)::int as missing_comment_gap,
          coalesce(aj.job_count, 0)::int as active_run_job_count,
          coalesce(aj.queued_count, 0)::int as active_run_queued_count,
          coalesce(aj.running_count, 0)::int as active_run_running_count,
          coalesce(aj.completed_count, 0)::int as active_run_completed_count,
          coalesce(aj.failed_count, 0)::int as active_run_failed_count,
          coalesce(aj.cancelled_count, 0)::int as active_run_cancelled_count,
          coalesce(aj.active_job_ids, array[]::text[]) as active_job_ids,
          coalesce(aj.active_job_target_counts, array[]::int[]) as active_job_target_counts
        from targets t
        left join counts c using (shortcode)
        left join active_jobs aj using (shortcode)
        """,
        [
            normalized_shortcodes,
            normalized_shortcodes,
            active_run_id,
            active_run_id,
            INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
        ],
    )
    return {str(row.get("shortcode") or ""): dict(row) for row in rows}


def get_instagram_comments_audit_cursor_recovery(
    *,
    account_handle: str,
    limit: int = 50,
    shortcodes: Sequence[Any] | None = None,
    stop_reasons: Sequence[Any] | None = None,
    show_ids: Sequence[Any] | None = None,
    season_ids: Sequence[Any] | None = None,
    show_filters: Sequence[Any] | None = None,
) -> dict[str, Any]:
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    normalized_stop_reasons = _normalize_instagram_comments_audit_retry_stop_reasons(stop_reasons)
    normalized_show_ids = _normalize_instagram_comments_show_filter_values(show_ids)
    normalized_season_ids = _normalize_instagram_comments_show_filter_values(season_ids)
    normalized_show_terms = _instagram_comments_show_filter_terms(show_filters)
    rows = _load_instagram_comments_audit_cursor_rows(
        account_handle=normalized_account,
        limit=limit,
        shortcodes=shortcodes,
        stop_reasons=normalized_stop_reasons,
        show_ids=normalized_show_ids,
        season_ids=normalized_season_ids,
        show_filters=normalized_show_terms,
    )
    target_source_ids, selected_rows = _select_instagram_comments_audit_cursor_retry_targets(rows)
    safe_limit = max(1, min(int(limit or 1), 500))
    target_source_ids = target_source_ids[:safe_limit]
    selected_target_set = set(target_source_ids)
    selected_rows = [row for row in selected_rows if str(row.get("shortcode") or "").strip() in selected_target_set]
    active_run = _active_instagram_comments_run_for_account(normalized_account)
    active_run_id = str((active_run or {}).get("run_id") or "").strip() or None
    counts = _instagram_comments_audit_cursor_counts_by_shortcode(
        shortcodes=target_source_ids,
        active_run_id=active_run_id,
    )
    progress_rows: list[dict[str, Any]] = []
    for row in selected_rows:
        shortcode = str(row.get("shortcode") or "").strip()
        count_row = counts.get(shortcode, {})
        progress_rows.append(
            {
                **row,
                "reported_comment_count": _normalize_non_negative_int(count_row.get("reported_comment_count")),
                "saved_comment_count": _normalize_non_negative_int(count_row.get("saved_comment_count")),
                "missing_comment_gap": _normalize_non_negative_int(count_row.get("missing_comment_gap")),
                "active_run_id": active_run_id,
                "active_run_job_count": _normalize_non_negative_int(count_row.get("active_run_job_count")),
                "active_run_queued_count": _normalize_non_negative_int(count_row.get("active_run_queued_count")),
                "active_run_running_count": _normalize_non_negative_int(count_row.get("active_run_running_count")),
                "active_run_completed_count": _normalize_non_negative_int(count_row.get("active_run_completed_count")),
                "active_run_failed_count": _normalize_non_negative_int(count_row.get("active_run_failed_count")),
                "active_run_cancelled_count": _normalize_non_negative_int(count_row.get("active_run_cancelled_count")),
                "active_job_ids": _as_text_list(count_row.get("active_job_ids")),
                "active_job_target_counts": [
                    _normalize_non_negative_int(value) for value in count_row.get("active_job_target_counts") or []
                ],
            }
        )
    progress_rows.sort(
        key=lambda item: (
            -_normalize_non_negative_int(item.get("missing_comment_gap")),
            str(item.get("shortcode") or ""),
        )
    )
    return {
        "ok": True,
        "account": normalized_account,
        "selected_target_source_ids": target_source_ids,
        "selected_target_source_ids_count": len(target_source_ids),
        "inspected_audit_rows_count": len(rows),
        "eligible_stop_reasons": normalized_stop_reasons,
        "show_filter": {
            "show_ids": normalized_show_ids,
            "season_ids": normalized_season_ids,
            "terms": normalized_show_terms,
        },
        "active_run": active_run,
        "progress_rows": progress_rows,
        "rows": progress_rows,
    }


def _split_instagram_comments_audit_cursor_targets_into_active_run(
    *,
    run_id: str,
    account_handle: str,
    target_source_ids: Sequence[str],
    batch_size: int,
    initiated_by: str | None,
    dispatch_immediately: bool,
    force_rerun_existing: bool = False,
) -> dict[str, Any]:
    normalized_run_id = str(run_id or "").strip()
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    remaining_targets = [
        str(target or "").strip()
        for target in target_source_ids
        if str(target or "").strip()
    ]
    if not normalized_run_id or not remaining_targets:
        return {"created_job_ids": [], "created_target_job_ids": [], "reason": "run_id_or_targets_required"}
    target_set = set(remaining_targets)
    rows = pg.fetch_all(
        """
        select
          r.id::text as run_id,
          r.source_scope,
          r.initiated_by,
          r.config as run_config,
          j.id::text as job_id,
          j.status,
          j.priority,
          j.config,
          j.metadata,
          jsonb_array_length(coalesce(j.config->'target_source_ids', '[]'::jsonb)) as target_count,
          array(
            select value
            from jsonb_array_elements_text(coalesce(j.config->'target_source_ids', '[]'::jsonb)) value
            where value = any(%s::text[])
            order by value
          ) as matched_targets
        from social.scrape_runs r
        join social.scrape_jobs j on j.run_id = r.id
        where r.id = %s::uuid
          and r.status in ('queued', 'pending', 'retrying', 'running')
          and j.status in ('queued', 'pending', 'retrying')
          and coalesce(j.config->>'stage', j.metadata->>'stage', j.job_type) = %s
          and ltrim(lower(coalesce(j.config->>'account', j.metadata->>'account', r.config->>'account', '')), '@') = %s
          and (j.config->'target_source_ids') ?| %s::text[]
        order by jsonb_array_length(coalesce(j.config->'target_source_ids', '[]'::jsonb)) desc, j.created_at asc
        """,
        [
            list(target_set),
            normalized_run_id,
            INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
            normalized_account,
            list(target_set),
        ],
    )
    created_target_job_ids: list[str] = []
    created_remainder_job_ids: list[str] = []
    cancelled_source_job_ids: list[str] = []
    skipped_sources: list[dict[str, Any]] = []
    created_target_rows: list[dict[str, Any]] = []
    retry_group_id = str(uuid4())
    created_sequence = 0
    safe_batch_size = max(1, int(batch_size or 1))
    for row in rows:
        source_job_id = str(row.get("job_id") or "").strip()
        if not source_job_id:
            continue
        config = _public_comments_config_overlay(_metadata_dict(row.get("config")))
        metadata = _metadata_dict(row.get("metadata"))
        dispatch = _metadata_dict(metadata.get("dispatch"))
        remote_invocation_id = str(dispatch.get("remote_invocation_id") or "").strip()
        remote_status = str(dispatch.get("remote_invocation_status") or "").strip().lower()
        if remote_invocation_id and remote_status in {"pending", "running", "queued"}:
            if not force_rerun_existing or remote_status == "running":
                skipped_sources.append(
                    {
                        "job_id": source_job_id,
                        "reason": "remote_invocation_active",
                        "remote_invocation_status": remote_status,
                    }
                )
                continue
        source_targets = [
            str(target or "").strip()
            for target in config.get("target_source_ids") or []
            if str(target or "").strip()
        ]
        matched_targets = [target for target in source_targets if target in target_set]
        retry_targets = [target for target in matched_targets if target in remaining_targets]
        if not retry_targets:
            continue
        if len(source_targets) == 1 and len(retry_targets) == 1 and source_targets[0] == retry_targets[0]:
            if not force_rerun_existing:
                remaining_targets = [target for target in remaining_targets if target not in set(retry_targets)]
                skipped_sources.append(
                    {
                        "job_id": source_job_id,
                        "reason": "already_batch_size_1",
                        "target_source_ids": retry_targets,
                    }
                )
                continue
        cancelled = pg.fetch_one(
            """
            update social.scrape_jobs
            set
              status = 'cancelled',
              completed_at = now(),
              error_message = coalesce(error_message, 'Split into audit cursor retry batches'),
              metadata = coalesce(metadata, '{}'::jsonb) || jsonb_build_object(
                'comments_audit_cursor_retry_split_at', %s,
                'comments_audit_cursor_retry_group_id', %s,
                'comments_audit_cursor_retry_targets', %s,
                'comments_audit_cursor_retry_remainder_targets', %s,
                'comments_audit_cursor_retry_batch_size', %s,
                'comments_audit_cursor_retry_force_rerun', %s
              )
            where id = %s::uuid
              and status in ('queued', 'pending', 'retrying')
            returning id::text
            """,
            [
                _iso(_now_utc()),
                retry_group_id,
                len(retry_targets),
                len([target for target in source_targets if target not in set(retry_targets)]),
                safe_batch_size,
                bool(force_rerun_existing),
                source_job_id,
            ],
        )
        if not cancelled:
            skipped_sources.append({"job_id": source_job_id, "reason": "source_status_changed"})
            continue
        cancelled_source_job_ids.append(source_job_id)
        original_shard_count = _normalize_non_negative_int(config.get("comments_shard_count")) or len(rows) or 1
        source_priority = _normalize_non_negative_int(row.get("priority")) or 105
        source_scope = str(row.get("source_scope") or config.get("source_scope") or "network")
        job_initiated_by = initiated_by or str(row.get("initiated_by") or "") or None
        target_chunks = [
            retry_targets[index : index + safe_batch_size]
            for index in range(0, len(retry_targets), safe_batch_size)
        ]
        remainder_targets = [target for target in source_targets if target not in set(retry_targets)]
        remainder_chunks = [remainder_targets] if remainder_targets else []
        total_new_chunks = len(target_chunks) + len(remainder_chunks)
        effective_shard_count = original_shard_count + total_new_chunks
        for chunk in target_chunks:
            created_sequence += 1
            retry_config = _public_comments_config_overlay({
                **config,
                "target_source_ids": chunk,
                "comments_audit_cursor_retry": True,
                "comments_audit_cursor_retry_source_job_id": source_job_id,
                "comments_audit_cursor_retry_group_id": retry_group_id,
                "comments_audit_cursor_retry_index": created_sequence,
                "comments_audit_cursor_retry_count": len(target_chunks),
                "comments_audit_cursor_retry_force_rerun": bool(force_rerun_existing),
                "comments_target_batch_size": safe_batch_size,
                "max_comments_per_post": 0,
                "comments_shard_index": original_shard_count + created_sequence,
                "comments_shard_count": effective_shard_count,
                "comments_shard_target_count": len(chunk),
                "account": normalized_account,
            })
            job_id = _create_job(
                None,
                run_id=normalized_run_id,
                platform="instagram",
                source_scope=source_scope,
                job_type="comments",
                stage=INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
                config=retry_config,
                initiated_by=job_initiated_by,
                status="queued",
                priority=max(1, min(source_priority, 104)),
                max_attempts=_instagram_comments_job_max_attempts(retry_config),
            )
            created_target_job_ids.append(job_id)
            for target in chunk:
                created_target_rows.append({"shortcode": target, "job_id": job_id, "source_job_id": source_job_id})
        for chunk in remainder_chunks:
            created_sequence += 1
            remainder_config = _public_comments_config_overlay({
                **config,
                "target_source_ids": chunk,
                "comments_audit_cursor_retry_remainder": True,
                "comments_audit_cursor_retry_source_job_id": source_job_id,
                "comments_audit_cursor_retry_group_id": retry_group_id,
                "comments_shard_index": original_shard_count + created_sequence,
                "comments_shard_count": effective_shard_count,
                "comments_shard_target_count": len(chunk),
                "account": normalized_account,
            })
            created_remainder_job_ids.append(
                _create_job(
                    None,
                    run_id=normalized_run_id,
                    platform="instagram",
                    source_scope=source_scope,
                    job_type="comments",
                    stage=INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
                    config=remainder_config,
                    initiated_by=job_initiated_by,
                    status="queued",
                    priority=source_priority,
                    max_attempts=_instagram_comments_job_max_attempts(remainder_config),
                )
            )
        retry_target_set = set(retry_targets)
        remaining_targets = [target for target in remaining_targets if target not in retry_target_set]
    if remaining_targets:
        run_row = pg.fetch_one(
            """
            select
              r.id::text as run_id,
              r.source_scope,
              r.initiated_by,
              r.config as run_config,
              coalesce(max(j.priority), 105) as source_priority,
              count(j.id)::int as existing_job_count
            from social.scrape_runs r
            left join social.scrape_jobs j
              on j.run_id = r.id
             and coalesce(j.config->>'stage', j.metadata->>'stage', j.job_type) = %s
            where r.id = %s::uuid
              and r.status in ('queued', 'pending', 'retrying', 'running')
              and ltrim(lower(coalesce(r.config->>'account', '')), '@') = %s
            group by r.id, r.source_scope, r.initiated_by, r.config
            """,
            [INSTAGRAM_COMMENTS_SCRAPLING_STAGE, normalized_run_id, normalized_account],
        )
        if run_row:
            run_config = _public_comments_config_overlay(_metadata_dict(run_row.get("run_config")))
            source_priority = _normalize_non_negative_int(run_row.get("source_priority")) or 105
            source_scope = str(run_row.get("source_scope") or run_config.get("source_scope") or "network")
            job_initiated_by = initiated_by or str(run_row.get("initiated_by") or "") or None
            existing_job_count = _normalize_non_negative_int(run_row.get("existing_job_count"))
            base_shard_count = max(
                _normalize_non_negative_int(run_config.get("comments_shard_count")),
                existing_job_count,
                1,
            )
            target_chunks = [
                remaining_targets[index : index + safe_batch_size]
                for index in range(0, len(remaining_targets), safe_batch_size)
            ]
            effective_shard_count = base_shard_count + len(target_chunks)
            for chunk in target_chunks:
                created_sequence += 1
                retry_config = _public_comments_config_overlay({
                    **run_config,
                    "target_source_ids": chunk,
                    "target_source_ids_count": len(chunk),
                    "explicit_target_source_ids": True,
                    "comments_audit_cursor_retry": True,
                    "comments_audit_cursor_retry_source_job_id": None,
                    "comments_audit_cursor_retry_group_id": retry_group_id,
                    "comments_audit_cursor_retry_index": created_sequence,
                    "comments_audit_cursor_retry_count": len(target_chunks),
                    "comments_audit_cursor_retry_standalone": True,
                    "comments_audit_cursor_retry_force_rerun": bool(force_rerun_existing),
                    "comments_target_batch_size": safe_batch_size,
                    "max_comments_per_post": 0,
                    "comments_shard_index": base_shard_count + created_sequence,
                    "comments_shard_count": effective_shard_count,
                    "comments_shard_target_count": len(chunk),
                    "account": normalized_account,
                })
                job_id = _create_job(
                    None,
                    run_id=normalized_run_id,
                    platform="instagram",
                    source_scope=source_scope,
                    job_type="comments",
                    stage=INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
                    config=retry_config,
                    initiated_by=job_initiated_by,
                    status="queued",
                    priority=max(1, min(source_priority, 104)),
                    max_attempts=_instagram_comments_job_max_attempts(retry_config),
                )
                created_target_job_ids.append(job_id)
                for target in chunk:
                    created_target_rows.append({"shortcode": target, "job_id": job_id, "source_job_id": None})
            created_target_set = {target for chunk in target_chunks for target in chunk}
            remaining_targets = [target for target in remaining_targets if target not in created_target_set]
    created_job_ids = [*created_target_job_ids, *created_remainder_job_ids]
    if dispatch_immediately and created_job_ids:
        dispatch_due_social_jobs(run_id=normalized_run_id)
    return {
        "run_id": normalized_run_id,
        "created_job_ids": created_job_ids,
        "created_job_count": len(created_job_ids),
        "created_target_job_ids": created_target_job_ids,
        "created_target_job_count": len(created_target_job_ids),
        "created_remainder_job_ids": created_remainder_job_ids,
        "cancelled_source_job_ids": cancelled_source_job_ids,
        "skipped_sources": skipped_sources,
        "retry_group_id": retry_group_id if created_job_ids else None,
        "pending_target_source_ids": remaining_targets,
        "target_rows": created_target_rows,
        "force_rerun_existing": bool(force_rerun_existing),
    }


def enqueue_instagram_comments_audit_cursor_retries(
    *,
    account_handle: str,
    limit: int = 50,
    shortcodes: Sequence[Any] | None = None,
    stop_reasons: Sequence[Any] | None = None,
    show_ids: Sequence[Any] | None = None,
    season_ids: Sequence[Any] | None = None,
    show_filters: Sequence[Any] | None = None,
    batch_size: int = 1,
    comments_worker_count: int | None = None,
    max_comments_per_post: int = 0,
    comments_load_strategy: str = "public_relay",
    skip_launch_auth_probe: bool = False,
    dry_run: bool = False,
    attach_to_active_run: bool = True,
    dispatch_immediately: bool = True,
    force_rerun_existing: bool = False,
    initiated_by: str | None = None,
) -> dict[str, Any]:
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    normalized_stop_reasons = _normalize_instagram_comments_audit_retry_stop_reasons(stop_reasons)
    safe_batch_size = max(1, int(batch_size or 1))
    recovery = get_instagram_comments_audit_cursor_recovery(
        account_handle=normalized_account,
        limit=limit,
        shortcodes=shortcodes,
        stop_reasons=normalized_stop_reasons,
        show_ids=show_ids,
        season_ids=season_ids,
        show_filters=show_filters,
    )
    target_source_ids = _as_text_list(recovery.get("selected_target_source_ids"))
    payload: dict[str, Any] = {
        **recovery,
        "mode": "dry_run" if dry_run else "enqueue",
        "batch_size": safe_batch_size,
        "max_comments_per_post": max(0, int(max_comments_per_post or 0)),
        "enqueue": {"requested": not dry_run, "performed": False},
    }
    if dry_run:
        return payload
    if not target_source_ids:
        payload.update({"ok": False, "failure_reason": "no_eligible_audit_cursor_targets"})
        return payload
    recovery_active_run = _metadata_dict(recovery.get("active_run"))
    recovery_active_run_id = str(recovery_active_run.get("run_id") or "").strip()
    def _attach_targets_to_active_run(active_run_id: str, *, active_run_detail: Mapping[str, Any] | None = None) -> dict[str, Any]:
        split_result = _split_instagram_comments_audit_cursor_targets_into_active_run(
            run_id=active_run_id,
            account_handle=normalized_account,
            target_source_ids=target_source_ids,
            batch_size=safe_batch_size,
            initiated_by=initiated_by or "audit-cursor-retry",
            dispatch_immediately=dispatch_immediately,
            force_rerun_existing=force_rerun_existing,
        )
        payload["active_run"] = _metadata_dict(active_run_detail) or recovery_active_run
        payload["enqueue"] = {
            "requested": True,
            "performed": bool(split_result.get("created_target_job_ids")),
            "mode": "active_run_split",
            "result": split_result,
        }
        if not split_result.get("created_target_job_ids"):
            payload["failure_reason"] = split_result.get("reason") or "no_active_queued_targets_split"
        return payload
    try:
        result = start_social_account_comments_scrape(
            "instagram",
            normalized_account,
            mode="profile",
            refresh_policy="all_saved_posts",
            target_source_ids=target_source_ids,
            max_comments_per_post=max(0, int(max_comments_per_post or 0)),
            comments_load_strategy=comments_load_strategy,
            initiated_by=initiated_by or "audit-cursor-retry",
            comments_worker_count=comments_worker_count,
            comments_target_batch_size=safe_batch_size,
            skip_launch_auth_probe=skip_launch_auth_probe,
            dispatch_immediately=dispatch_immediately,
        )
        payload["enqueue"] = {"requested": True, "performed": True, "mode": "new_run", "result": result}
        return payload
    except SocialWorkerUnavailableError:
        if not attach_to_active_run or not recovery_active_run_id:
            raise
        return _attach_targets_to_active_run(recovery_active_run_id)
    except SocialIngestConflictError as exc:
        active_run = _metadata_dict(getattr(exc, "detail", {}) or {}).get("run_id")
        active_run_id = str(active_run or "").strip()
        if not attach_to_active_run or not active_run_id:
            raise
        return _attach_targets_to_active_run(active_run_id, active_run_detail=getattr(exc, "detail", {}) or {})


def preview_social_account_comments_scrape(
    platform: str,
    account_handle: str,
    *,
    mode: str,
    source_id: str | None = None,
    max_posts: int | None = None,
    refresh_policy: str = "stale_or_missing",
    target_filter: str | None = None,
    comments_load_strategy: str = "public_relay",
    date_start: str | None = None,
    date_end: str | None = None,
) -> dict[str, Any]:
    started_at = time_module.perf_counter()
    # Validate the window eagerly so malformed input fails fast with a 400.
    _normalize_comment_date_window(date_start, date_end)
    normalized_platform = _normalize_social_account_profile_platform(platform)
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    normalized_mode = str(mode or "").strip().lower()
    normalized_target_filter = _normalize_instagram_comments_target_filter(target_filter)
    normalized_load_strategy = _normalize_instagram_comments_load_strategy(comments_load_strategy)
    _assert_instagram_comments_load_strategy_enabled(normalized_load_strategy)
    if normalized_platform != "instagram":
        raise SocialIngestValidationError(
            "SOCIAL_ACCOUNT_COMMENTS_UNSUPPORTED_PLATFORM",
            "Standalone comments scraping is currently only supported for Instagram.",
        )
    if normalized_mode not in {"profile", "single_post"}:
        raise SocialIngestValidationError("SOCIAL_ACCOUNT_COMMENTS_INVALID_MODE", "Unsupported comments scrape mode.")
    if normalized_mode == "single_post":
        if normalized_target_filter is not None:
            raise SocialIngestValidationError(
                "SOCIAL_ACCOUNT_COMMENTS_INVALID_TARGET_FILTER",
                "target_filter is only supported for profile comment scrapes.",
            )
        normalized_source_id = str(source_id or "").strip()
        target_source_ids = [normalized_source_id] if normalized_source_id else []
        target_count = len(target_source_ids)
        recommended_shard_count = 1
        effective_shard_count = 1
        strategy_metadata = _instagram_comments_load_strategy_metadata(
            load_strategy=normalized_load_strategy,
            mode=normalized_mode,
            target_count=target_count,
            recommended_shard_count=recommended_shard_count,
            effective_shard_count=effective_shard_count,
        )
        strategy_warnings = _instagram_comments_load_strategy_warnings(strategy_metadata)
        return {
            "dry_run": True,
            "platform": normalized_platform,
            "account_handle": normalized_account,
            "mode": normalized_mode,
            "refresh_policy": str(refresh_policy or "stale_or_missing").strip().lower() or "stale_or_missing",
            "target_priority": "single_post",
            "target_source_ids_count": target_count,
            **strategy_metadata,
            "comments_shard_count": 1,
            "comments_sharding_enabled": False,
            "recommended_comments_shard_count": recommended_shard_count,
            "strategy_warnings": strategy_warnings,
            "sample_target_source_ids": target_source_ids[:1],
            "timing": {
                "target_preview_ms": round((time_module.perf_counter() - started_at) * 1000, 1),
                "target_count_ms": 0.0,
                "sample_target_source_ids_ms": 0.0,
                "cache_lookup_ms": 0.0,
                "total_ms": round((time_module.perf_counter() - started_at) * 1000, 1),
            },
            "preview_cache": {
                "enabled": False,
                "hit": False,
                "age_seconds": None,
                "ttl_seconds": 0,
            },
            "cache": {
                "enabled": False,
                "hit": False,
                "age_seconds": None,
                "ttl_seconds": 0,
            },
            "debug": {
                "target_plan_strategy": "single_post_preview",
                "bounded": True,
                "full_target_list_built": False,
                "sample_limit": 1,
                "max_posts": None,
            },
        }
    else:
        normalized_refresh_policy = str(refresh_policy or "stale_or_missing").strip().lower() or "stale_or_missing"
        if normalized_refresh_policy not in {"stale_or_missing", "all_saved_posts"}:
            raise SocialIngestValidationError(
                "SOCIAL_ACCOUNT_COMMENTS_INVALID_REFRESH_POLICY",
                "Profile comments scraping supports stale_or_missing and all_saved_posts refreshes.",
            )
        plan = _instagram_social_account_comment_target_preview(
            normalized_account,
            limit=None if max_posts is None else max(1, int(max_posts)),
            refresh_policy=normalized_refresh_policy,
            target_filter=normalized_target_filter,
            date_start=date_start,
            date_end=date_end,
        )
    timing = _metadata_dict(plan.get("timing"))
    timing["total_ms"] = round((time_module.perf_counter() - started_at) * 1000, 1)
    target_count = _normalize_non_negative_int(plan.get("target_source_ids_count"))
    recommended_shard_count = _normalize_non_negative_int(
        plan.get("recommended_comments_shard_count")
    ) or _instagram_comments_recommended_shard_count(target_count=target_count)
    default_shard_count = _normalize_non_negative_int(plan.get("comments_shard_count")) or 1
    effective_shard_count = 1 if normalized_load_strategy == "single_session_load_all" else default_shard_count
    strategy_metadata = _instagram_comments_load_strategy_metadata(
        load_strategy=normalized_load_strategy,
        mode=normalized_mode,
        target_count=target_count,
        recommended_shard_count=recommended_shard_count,
        effective_shard_count=effective_shard_count,
    )
    strategy_warnings = _instagram_comments_load_strategy_warnings(strategy_metadata)
    return {
        "dry_run": True,
        "platform": normalized_platform,
        "account_handle": normalized_account,
        "mode": normalized_mode,
        **plan,
        **strategy_metadata,
        "comments_shard_count": effective_shard_count,
        "comments_sharding_enabled": effective_shard_count > 1,
        "recommended_comments_shard_count": recommended_shard_count,
        "strategy_warnings": strategy_warnings,
        "timing": timing,
    }


def rebalance_failed_instagram_comments_shard(
    *,
    failed_job_id: str,
    max_retry_shard_size: int = 10,
) -> dict[str, Any]:
    _sync_core_overrides()
    row = pg.fetch_one(
        """
        select
          j.id::text as job_id,
          j.run_id::text as run_id,
          j.platform,
          j.source_scope,
          j.config,
          j.metadata,
          j.initiated_by,
          j.items_found
        from social.scrape_jobs j
        where j.id = %s::uuid
          and j.status = 'failed'
          and coalesce(j.config->>'stage', j.metadata->>'stage', j.job_type) = %s
        """,
        [failed_job_id, INSTAGRAM_COMMENTS_SCRAPLING_STAGE],
    )
    if not row:
        return {"created_job_ids": [], "reason": "failed_job_not_found"}
    config = _metadata_dict(row.get("config"))
    metadata = _metadata_dict(row.get("metadata"))
    retry_rebalance = _metadata_dict(metadata.get("retry_rebalance"))
    remaining_targets = [
        str(item or "").strip()
        for item in retry_rebalance.get("remaining_target_source_ids") or []
        if str(item or "").strip()
    ]
    if not remaining_targets:
        remaining_targets = _comments_job_remaining_target_source_ids(
            row=row,
            config=config,
            metadata=metadata,
            require_items_found_for_progress=True,
        )
    if not remaining_targets:
        return {"created_job_ids": [], "reason": "no_remaining_targets"}
    safe_max_retry_shard_size = max(1, int(max_retry_shard_size or 10))
    retry_shard_count = max(1, (len(remaining_targets) + safe_max_retry_shard_size - 1) // safe_max_retry_shard_size)
    chunks = _chunk_instagram_comment_targets(remaining_targets, retry_shard_count)
    created_job_ids: list[str] = []
    retry_group_id = str(uuid4())
    for index, chunk in enumerate(chunks, start=1):
        retry_config = {
            **config,
            "target_source_ids": chunk,
            "comments_retry_rebalance": True,
            "comments_retry_rebalance_source_job_id": str(row.get("job_id") or ""),
            "comments_retry_rebalance_group_id": retry_group_id,
            "comments_retry_rebalance_index": index,
            "comments_retry_rebalance_count": len(chunks),
            "comments_shard_target_count": len(chunk),
        }
        created_job_ids.append(
            _create_job(
                None,
                run_id=str(row.get("run_id") or ""),
                platform="instagram",
                source_scope=str(row.get("source_scope") or "network"),
                job_type="comments",
                stage=INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
                config=retry_config,
                initiated_by=str(row.get("initiated_by") or "") or None,
                status="queued",
                priority=110,
                max_attempts=_instagram_comments_job_max_attempts(retry_config),
            )
        )
    return {"created_job_ids": created_job_ids, "retry_group_id": retry_group_id}


def rebalance_waiting_instagram_comments_shards(
    *,
    run_id: str,
    max_waiting_shard_size: int = 12,
    max_rebalanced_shards: int = 4,
    dispatch_immediately: bool = True,
) -> dict[str, Any]:
    _sync_core_overrides()
    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        return {"created_job_ids": [], "reason": "run_id_required"}
    safe_max_waiting_shard_size = max(1, int(max_waiting_shard_size or 12))
    safe_max_rebalanced_shards = max(1, int(max_rebalanced_shards or 4))
    rows = pg.fetch_all(
        """
        select
          r.id::text as run_id,
          r.source_scope,
          r.initiated_by,
          j.id::text as job_id,
          j.status,
          j.priority,
          j.config,
          j.metadata,
          j.items_found,
          j.created_at
        from social.scrape_runs r
        join social.scrape_jobs j on j.run_id = r.id
        where r.id = %s::uuid
          and r.status in ('queued', 'pending', 'retrying', 'running')
          and j.status in ('queued', 'pending', 'retrying')
          and coalesce(j.config->>'stage', j.metadata->>'stage', j.job_type) = %s
          and jsonb_array_length(coalesce(j.config->'target_source_ids', '[]'::jsonb)) > %s
        order by
          jsonb_array_length(coalesce(j.config->'target_source_ids', '[]'::jsonb)) desc,
          j.priority asc,
          j.created_at asc
        limit %s
        """,
        [
            normalized_run_id,
            INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
            safe_max_waiting_shard_size,
            safe_max_rebalanced_shards,
        ],
    )
    created_job_ids: list[str] = []
    rebalanced_sources: list[str] = []
    skipped_sources: list[dict[str, Any]] = []
    rebalance_group_id = str(uuid4())
    for row in rows:
        config = _metadata_dict(row.get("config"))
        metadata = _metadata_dict(row.get("metadata"))
        dispatch = _metadata_dict(metadata.get("dispatch"))
        remote_invocation_id = str(dispatch.get("remote_invocation_id") or "").strip()
        remote_status = str(dispatch.get("remote_invocation_status") or "").strip().lower()
        if remote_invocation_id and remote_status in {"pending", "running", "queued"}:
            skipped_sources.append(
                {
                    "job_id": str(row.get("job_id") or ""),
                    "reason": "remote_invocation_active",
                    "remote_invocation_status": remote_status,
                }
            )
            continue
        status = str(row.get("status") or "").strip().lower()
        if status == "retrying":
            target_source_ids = _comments_job_remaining_target_source_ids(
                row=row,
                config=config,
                metadata=metadata,
                require_items_found_for_progress=True,
            )
        else:
            target_source_ids = _comments_job_target_source_ids(config=config, metadata=metadata)
        if len(target_source_ids) <= safe_max_waiting_shard_size:
            skipped_sources.append(
                {
                    "job_id": str(row.get("job_id") or ""),
                    "reason": "remaining_targets_below_threshold",
                    "remaining_target_source_ids_count": len(target_source_ids),
                }
            )
            continue
        retry_shard_count = max(
            1,
            (len(target_source_ids) + safe_max_waiting_shard_size - 1) // safe_max_waiting_shard_size,
        )
        chunks = _chunk_instagram_comment_targets(target_source_ids, retry_shard_count)
        if len(chunks) <= 1:
            continue
        source_job_id = str(row.get("job_id") or "").strip()
        cancelled = pg.fetch_one(
            """
            update social.scrape_jobs
            set
              status = 'cancelled',
              completed_at = now(),
              error_message = coalesce(error_message, 'Rebalanced oversized waiting comments shard'),
              metadata = coalesce(metadata, '{}'::jsonb) || jsonb_build_object(
                'comments_waiting_rebalanced_at', %s,
                'comments_waiting_rebalance_group_id', %s,
                'comments_waiting_rebalance_original_targets', %s,
                'comments_waiting_rebalance_remaining_targets', %s,
                'comments_waiting_rebalance_max_shard_size', %s
              )
            where id = %s::uuid
              and status in ('queued', 'pending', 'retrying')
            returning id::text
            """,
            [
                _iso(_now_utc()),
                rebalance_group_id,
                len(_comments_job_target_source_ids(config=config, metadata=metadata)),
                len(target_source_ids),
                safe_max_waiting_shard_size,
                source_job_id,
            ],
        )
        if not cancelled:
            skipped_sources.append({"job_id": source_job_id, "reason": "source_status_changed"})
            continue
        original_shard_count = _normalize_non_negative_int(config.get("comments_shard_count")) or len(rows) or 1
        effective_shard_count = original_shard_count + len(chunks)
        source_priority = _normalize_non_negative_int(row.get("priority")) or 110
        for index, chunk in enumerate(chunks, start=1):
            retry_config = {
                **config,
                "target_source_ids": chunk,
                "comments_waiting_rebalance": True,
                "comments_waiting_rebalance_source_job_id": source_job_id,
                "comments_waiting_rebalance_group_id": rebalance_group_id,
                "comments_waiting_rebalance_index": index,
                "comments_waiting_rebalance_count": len(chunks),
                "comments_waiting_rebalance_original_target_count": len(target_source_ids),
                "comments_waiting_rebalance_max_shard_size": safe_max_waiting_shard_size,
                "comments_shard_index": original_shard_count + index,
                "comments_shard_count": effective_shard_count,
                "comments_shard_target_count": len(chunk),
            }
            created_job_ids.append(
                _create_job(
                    None,
                    run_id=normalized_run_id,
                    platform="instagram",
                    source_scope=str(row.get("source_scope") or config.get("source_scope") or "network"),
                    job_type="comments",
                    stage=INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
                    config=retry_config,
                    initiated_by=str(row.get("initiated_by") or "") or None,
                    status="queued",
                    priority=max(1, min(source_priority, 109)),
                    max_attempts=_instagram_comments_job_max_attempts(retry_config),
                )
            )
        rebalanced_sources.append(source_job_id)

    if dispatch_immediately and created_job_ids:
        dispatch_due_social_jobs(run_id=normalized_run_id)
    return {
        "created_job_ids": created_job_ids,
        "created_job_count": len(created_job_ids),
        "rebalanced_source_job_ids": rebalanced_sources,
        "skipped_sources": skipped_sources,
        "rebalance_group_id": rebalance_group_id if created_job_ids else None,
    }


def _comments_job_target_source_ids(
    *,
    config: Mapping[str, Any],
    metadata: Mapping[str, Any],
) -> list[str]:
    return [
        str(item or "").strip()
        for item in (config.get("target_source_ids") or metadata.get("target_source_ids") or [])
        if str(item or "").strip()
    ]


def _comments_job_processed_post_count(
    *,
    row: Mapping[str, Any],
    metadata: Mapping[str, Any],
    require_items_found_for_progress: bool = False,
) -> int:
    stage_counters = _metadata_dict(metadata.get("stage_counters"))
    cumulative_counters = _metadata_dict(metadata.get("cumulative_counters"))
    activity = _metadata_dict(metadata.get("activity"))
    processed_posts = max(
        _normalize_non_negative_int(stage_counters.get("posts"))
        + _normalize_non_negative_int(cumulative_counters.get("posts")),
        _normalize_non_negative_int(activity.get("posts_checked")),
        _normalize_non_negative_int(activity.get("matched_posts")),
        _normalize_non_negative_int(activity.get("saved_posts")),
    )
    if require_items_found_for_progress and _normalize_non_negative_int(row.get("items_found")) <= 0:
        return 0
    return processed_posts


def _comments_job_remaining_target_source_ids(
    *,
    row: Mapping[str, Any],
    config: Mapping[str, Any],
    metadata: Mapping[str, Any],
    require_items_found_for_progress: bool = False,
) -> list[str]:
    retry_rebalance = _metadata_dict(metadata.get("retry_rebalance"))
    retry_targets = [
        str(item or "").strip()
        for item in retry_rebalance.get("remaining_target_source_ids") or []
        if str(item or "").strip()
    ]
    if retry_targets:
        return retry_targets
    original_targets = _comments_job_target_source_ids(config=config, metadata=metadata)
    processed_posts = _comments_job_processed_post_count(
        row=row,
        metadata=metadata,
        require_items_found_for_progress=require_items_found_for_progress,
    )
    return original_targets[min(processed_posts, len(original_targets)) :]


def repair_instagram_comments_scrape_run_target_gaps(
    *,
    run_id: str,
    max_retry_shard_size: int = 25,
    dispatch_immediately: bool = True,
) -> dict[str, Any]:
    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        return {"created_job_ids": [], "reason": "run_id_required"}
    run_row = pg.fetch_one(
        """
        select
          id::text as run_id,
          source_scope,
          initiated_by,
          config
        from social.scrape_runs
        where id = %s::uuid
          and coalesce(config->>'stage', '') = %s
        """,
        [normalized_run_id, INSTAGRAM_COMMENTS_SCRAPLING_STAGE],
    )
    if not run_row:
        return {"created_job_ids": [], "reason": "run_not_found"}
    run_config = _public_comments_config_overlay(_metadata_dict(run_row.get("config")))
    account_handle = _normalize_social_account_profile_handle(run_config.get("account"))
    if not account_handle:
        return {"created_job_ids": [], "reason": "account_missing"}
    refresh_policy = str(run_config.get("refresh_policy") or "stale_or_missing").strip().lower() or "stale_or_missing"
    max_posts = run_config.get("max_posts")
    target_source_ids = _instagram_social_account_comment_target_shortcodes(
        account_handle,
        limit=None if max_posts is None else max(1, int(max_posts)),
        refresh_policy=refresh_policy,
    )
    job_rows = pg.fetch_all(
        """
        select status, config
        from social.scrape_jobs
        where run_id = %s::uuid
          and coalesce(config->>'stage', metadata->>'stage', job_type) = %s
        """,
        [normalized_run_id, INSTAGRAM_COMMENTS_SCRAPLING_STAGE],
    )
    active_assigned_targets: set[str] = set()
    for row in job_rows:
        status = str(row.get("status") or "").strip().lower()
        if status not in {"queued", "pending", "retrying", "running"}:
            continue
        config = _metadata_dict(row.get("config"))
        for target in config.get("target_source_ids") or []:
            normalized_target = str(target or "").strip()
            if normalized_target:
                active_assigned_targets.add(normalized_target)
    missing_targets = [target for target in target_source_ids if target not in active_assigned_targets]
    if not missing_targets:
        return {
            "created_job_ids": [],
            "reason": "no_missing_targets",
            "target_source_ids_count": len(target_source_ids),
            "assigned_target_source_ids_count": len(active_assigned_targets),
        }
    safe_max_retry_shard_size = max(1, int(max_retry_shard_size or 25))
    retry_shard_count = max(1, (len(missing_targets) + safe_max_retry_shard_size - 1) // safe_max_retry_shard_size)
    chunks = _chunk_instagram_comment_targets(missing_targets, retry_shard_count)
    created_job_ids: list[str] = []
    repair_group_id = str(uuid4())
    original_shard_count = _normalize_non_negative_int(run_config.get("comments_shard_count")) or len(job_rows) or 1
    effective_shard_count = original_shard_count + len(chunks)
    for index, chunk in enumerate(chunks, start=1):
        repair_config = _public_comments_config_overlay({
            **run_config,
            "target_source_ids": chunk,
            "comments_target_gap_repair": True,
            "comments_target_gap_repair_group_id": repair_group_id,
            "comments_target_gap_repair_index": index,
            "comments_target_gap_repair_count": len(chunks),
            "comments_shard_index": original_shard_count + index,
            "comments_shard_count": effective_shard_count,
            "comments_shard_target_count": len(chunk),
            "account": account_handle,
        })
        created_job_ids.append(
            _create_job(
                None,
                run_id=normalized_run_id,
                platform="instagram",
                source_scope=str(run_row.get("source_scope") or run_config.get("source_scope") or "network"),
                job_type="comments",
                stage=INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
                config=repair_config,
                initiated_by=str(run_row.get("initiated_by") or "") or None,
                status="queued",
                priority=108,
                max_attempts=_instagram_comments_job_max_attempts(repair_config),
            )
        )
    if dispatch_immediately and created_job_ids:
        dispatch_due_social_jobs(run_id=normalized_run_id)
    return {
        "created_job_ids": created_job_ids,
        "repair_group_id": repair_group_id,
        "missing_target_source_ids_count": len(missing_targets),
        "target_source_ids_count": len(target_source_ids),
        "assigned_target_source_ids_count": len(active_assigned_targets),
    }


def _comments_slow_shard_rebalance_enabled() -> bool:
    return str(os.getenv("SOCIAL_INSTAGRAM_COMMENTS_SLOW_SHARD_REBALANCE_ENABLED", "1")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def rebalance_slow_instagram_comments_shards(
    *,
    run_id: str,
    max_retry_shard_size: int = 75,
    slow_elapsed_seconds: int | None = None,
    slow_posts_per_minute: float | None = None,
    min_remaining_targets: int | None = None,
    max_rebalanced_shards: int | None = None,
    dispatch_immediately: bool = True,
) -> dict[str, Any]:
    _sync_core_overrides()
    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        return {"created_job_ids": [], "reason": "run_id_required"}
    if not _comments_slow_shard_rebalance_enabled():
        return {"created_job_ids": [], "reason": "disabled"}

    safe_elapsed_seconds = max(
        60,
        int(slow_elapsed_seconds or os.getenv("SOCIAL_INSTAGRAM_COMMENTS_SLOW_SHARD_REBALANCE_SECONDS") or 900),
    )
    safe_posts_per_minute = float(
        slow_posts_per_minute
        if slow_posts_per_minute is not None
        else os.getenv("SOCIAL_INSTAGRAM_COMMENTS_SLOW_SHARD_POSTS_PER_MINUTE") or 0.25
    )
    safe_min_remaining_targets = max(
        1,
        int(min_remaining_targets or os.getenv("SOCIAL_INSTAGRAM_COMMENTS_SLOW_SHARD_MIN_REMAINING_TARGETS") or 100),
    )
    safe_max_rebalanced_shards = max(
        1,
        int(max_rebalanced_shards or os.getenv("SOCIAL_INSTAGRAM_COMMENTS_SLOW_SHARD_MAX_REBALANCES_PER_RUN") or 2),
    )
    safe_max_retry_shard_size = max(1, int(max_retry_shard_size or 75))
    safe_max_rebalance_depth = max(
        1,
        int(os.getenv("SOCIAL_INSTAGRAM_COMMENTS_SLOW_SHARD_MAX_REBALANCE_DEPTH") or 2),
    )

    rows = pg.fetch_all(
        """
        select
          r.id::text as run_id,
          r.source_scope,
          r.initiated_by,
          r.config as run_config,
          j.id::text as job_id,
          j.status,
          j.config,
          j.metadata,
          j.items_found,
          j.claimed_at,
          j.started_at,
          j.created_at
        from social.scrape_runs r
        join social.scrape_jobs j on j.run_id = r.id
        where r.id = %s::uuid
          and r.status in ('queued', 'pending', 'retrying', 'running')
          and j.status = 'running'
          and coalesce(j.config->>'stage', j.metadata->>'stage', j.job_type) = %s
        order by coalesce(j.claimed_at, j.started_at) asc nulls last, j.created_at asc
        """,
        [normalized_run_id, INSTAGRAM_COMMENTS_SCRAPLING_STAGE],
    )
    created_job_ids: list[str] = []
    rebalanced_sources: list[str] = []
    rebalance_group_id = str(uuid4())
    for row in rows:
        if len(rebalanced_sources) >= safe_max_rebalanced_shards:
            break
        config = _metadata_dict(row.get("config"))
        metadata = _metadata_dict(row.get("metadata"))
        rebalance_depth = _normalize_non_negative_int(config.get("comments_slow_rebalance_depth"))
        if config.get("comments_slow_rebalance") and rebalance_depth <= 0:
            rebalance_depth = 1
        if metadata.get("comments_slow_rebalanced_at") or rebalance_depth >= safe_max_rebalance_depth:
            continue
        target_source_ids = [
            str(item or "").strip() for item in config.get("target_source_ids") or [] if str(item or "").strip()
        ]
        if not target_source_ids:
            continue
        running_since = _coerce_dt(row.get("claimed_at") or row.get("started_at") or row.get("created_at"))
        if not isinstance(running_since, datetime):
            continue
        elapsed_seconds = max(1, int((_now_utc() - running_since).total_seconds()))
        if elapsed_seconds < safe_elapsed_seconds:
            continue
        stage_counters = _metadata_dict(metadata.get("stage_counters"))
        activity = _metadata_dict(metadata.get("activity"))
        # Remaining targets must be sliced against this job's current target list.
        # Cumulative counters can include prior retry attempts for the same source
        # shard and would overcount processed posts here.
        processed_posts = max(
            _normalize_non_negative_int(stage_counters.get("posts")),
            _normalize_non_negative_int(activity.get("posts_checked")),
        )
        posts_per_minute = (processed_posts * 60.0) / elapsed_seconds
        if processed_posts > 0 and posts_per_minute > safe_posts_per_minute:
            continue
        remaining_targets = target_source_ids[min(processed_posts, len(target_source_ids)) :]
        if len(remaining_targets) < safe_min_remaining_targets:
            continue
        retry_shard_count = max(1, (len(remaining_targets) + safe_max_retry_shard_size - 1) // safe_max_retry_shard_size)
        chunks = _chunk_instagram_comment_targets(remaining_targets, retry_shard_count)
        if not chunks:
            continue

        source_job_id = str(row.get("job_id") or "").strip()
        original_shard_count = _normalize_non_negative_int(config.get("comments_shard_count")) or len(rows) or 1
        effective_shard_count = original_shard_count + len(chunks)
        pg.fetch_one(
            """
            update social.scrape_jobs
            set
              status = 'cancelled',
              completed_at = now(),
              error_message = coalesce(error_message, 'Automatically rebalanced slow comments shard'),
              metadata = coalesce(metadata, '{}'::jsonb) || jsonb_build_object(
                'comments_slow_rebalanced_at', %s,
                'comments_slow_rebalance_group_id', %s,
                'comments_slow_rebalance_remaining_targets', %s,
                'comments_slow_rebalance_elapsed_seconds', %s,
                'comments_slow_rebalance_posts_per_minute', %s
              )
            where id = %s::uuid
              and status = 'running'
            returning id::text
            """,
            [
                _iso(_now_utc()),
                rebalance_group_id,
                len(remaining_targets),
                elapsed_seconds,
                round(posts_per_minute, 4),
                source_job_id,
            ],
        )
        for index, chunk in enumerate(chunks, start=1):
            root_job_id = str(
                config.get("comments_slow_rebalance_root_job_id")
                or config.get("comments_slow_rebalance_source_job_id")
                or source_job_id
            ).strip()
            retry_config = {
                **config,
                "target_source_ids": chunk,
                "comments_slow_rebalance": True,
                "comments_slow_rebalance_source_job_id": source_job_id,
                "comments_slow_rebalance_parent_job_id": source_job_id,
                "comments_slow_rebalance_root_job_id": root_job_id or source_job_id,
                "comments_slow_rebalance_depth": rebalance_depth + 1,
                "comments_slow_rebalance_group_id": rebalance_group_id,
                "comments_slow_rebalance_index": index,
                "comments_slow_rebalance_count": len(chunks),
                "comments_shard_index": original_shard_count + index,
                "comments_shard_count": effective_shard_count,
                "comments_shard_target_count": len(chunk),
            }
            created_job_ids.append(
                _create_job(
                    None,
                    run_id=normalized_run_id,
                    platform="instagram",
                    source_scope=str(row.get("source_scope") or config.get("source_scope") or "network"),
                    job_type="comments",
                    stage=INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
                    config=retry_config,
                    initiated_by=str(row.get("initiated_by") or "") or None,
                    status="queued",
                    priority=109,
                    max_attempts=_instagram_comments_job_max_attempts(retry_config),
                )
            )
        rebalanced_sources.append(source_job_id)

    if dispatch_immediately and created_job_ids:
        dispatch_due_social_jobs(run_id=normalized_run_id)
    return {
        "created_job_ids": created_job_ids,
        "created_job_count": len(created_job_ids),
        "rebalanced_source_job_ids": rebalanced_sources,
        "rebalance_group_id": rebalance_group_id if created_job_ids else None,
    }


def _comments_progress_count_map(value: Any) -> dict[str, int]:
    if not isinstance(value, Mapping):
        return {}
    counts: dict[str, int] = {}
    for raw_key, raw_value in value.items():
        key = str(raw_key or "").strip().lower()
        if not key:
            continue
        counts[key] = counts.get(key, 0) + _normalize_non_negative_int(raw_value)
    return counts


def _comments_progress_reason_counts_from_values(value: Any) -> dict[str, int]:
    if isinstance(value, Mapping):
        values = value.values()
    elif isinstance(value, (list, tuple, set)):
        values = value
    else:
        return {}
    counts: Counter[str] = Counter()
    for raw_reason in values:
        reason = str(raw_reason or "").strip().lower() or "unknown"
        counts[reason] += 1
    return dict(counts)


def _comments_progress_merge_counts(*maps: Any) -> dict[str, int]:
    merged: Counter[str] = Counter()
    for count_map in maps:
        for key, value in _comments_progress_count_map(count_map).items():
            merged[key] += value
    return dict(merged)


def _comments_progress_per_minute(count: int, elapsed_seconds: int) -> float | None:
    if elapsed_seconds <= 0:
        return None
    return round((_normalize_non_negative_int(count) * 60.0) / elapsed_seconds, 2)


def _comments_progress_per_second(count: int, elapsed_seconds: int) -> float | None:
    if elapsed_seconds <= 0:
        return None
    return round(_normalize_non_negative_int(count) / elapsed_seconds, 4)


def _comments_progress_average_seconds(elapsed_seconds: int, count: int, *, digits: int = 2) -> float | None:
    normalized_count = _normalize_non_negative_int(count)
    if elapsed_seconds <= 0 or normalized_count <= 0:
        return None
    return round(elapsed_seconds / normalized_count, digits)


def _comments_progress_estimated_seconds_remaining(
    remaining_count: int,
    average_seconds_per_item: float | None,
) -> int | None:
    if average_seconds_per_item is None:
        return None
    return round(_normalize_non_negative_int(remaining_count) * average_seconds_per_item)


_INSTAGRAM_STATIC_CDN_HOST = "static.cdninstagram.com"
_INSTAGRAM_COMMENTS_NETWORK_STOP_REASONS = {
    "network_budget_exhausted",
    "network_policy_blocked",
    "network_stop",
    "network_stopped",
    "proxy_budget_exhausted",
    "proxy_network_stop",
    "static_cdn_budget_exhausted",
}


def _comments_progress_network_spend_payload(
    *,
    bytes_by_host: Mapping[str, int],
    request_count_by_host: Mapping[str, int],
    blocked_request_count_by_host: Mapping[str, int],
    blocked_bytes_estimate_by_host: Mapping[str, int],
    policy_modes: Mapping[str, int] | None = None,
    host_limit: int = 8,
) -> dict[str, Any]:
    byte_counts = Counter(_comments_progress_count_map(bytes_by_host))
    request_counts = Counter(_comments_progress_count_map(request_count_by_host))
    blocked_request_counts = Counter(_comments_progress_count_map(blocked_request_count_by_host))
    blocked_byte_counts = Counter(_comments_progress_count_map(blocked_bytes_estimate_by_host))
    all_hosts = set(byte_counts) | set(request_counts) | set(blocked_request_counts) | set(blocked_byte_counts)
    top_hosts = sorted(
        (
            {
                "host": host,
                "bytes": int(byte_counts.get(host, 0)),
                "request_count": int(request_counts.get(host, 0)),
                "blocked_request_count": int(blocked_request_counts.get(host, 0)),
                "blocked_bytes_estimate": int(blocked_byte_counts.get(host, 0)),
            }
            for host in all_hosts
        ),
        key=lambda item: (
            -int(item.get("bytes") or 0),
            -int(item.get("request_count") or 0),
            str(item.get("host") or ""),
        ),
    )[: max(1, int(host_limit or 8))]
    static_bytes = int(byte_counts.get(_INSTAGRAM_STATIC_CDN_HOST, 0))
    static_requests = int(request_counts.get(_INSTAGRAM_STATIC_CDN_HOST, 0))
    static_blocked_requests = int(blocked_request_counts.get(_INSTAGRAM_STATIC_CDN_HOST, 0))
    total_bytes = int(sum(byte_counts.values()))
    payload: dict[str, Any] = {
        "observed_proxy_bytes": total_bytes,
        "observed_proxy_megabytes": round(total_bytes / 1_000_000, 3) if total_bytes else 0,
        "observed_request_count": int(sum(request_counts.values())),
        "static_cdninstagram_bytes": static_bytes,
        "static_cdninstagram_megabytes": round(static_bytes / 1_000_000, 3) if static_bytes else 0,
        "static_cdninstagram_request_count": static_requests,
        "static_cdninstagram_blocked_request_count": static_blocked_requests,
        "blocked_request_count": int(sum(blocked_request_counts.values())),
        "blocked_bytes_estimate": int(sum(blocked_byte_counts.values())),
        "bytes_by_host": dict(sorted(byte_counts.items())),
        "request_count_by_host": dict(sorted(request_counts.items())),
        "blocked_request_count_by_host": dict(sorted(blocked_request_counts.items())),
        "blocked_bytes_estimate_by_host": dict(sorted(blocked_byte_counts.items())),
        "top_hosts": top_hosts,
        "spend_basis": "observed_proxy_response_bytes",
    }
    normalized_policy_modes = _comments_progress_count_map(policy_modes or {})
    if normalized_policy_modes:
        payload["network_policy_modes"] = normalized_policy_modes
    return payload


def _comments_progress_sample_by_shortcode(value: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(value, Mapping):
        return {}
    samples: list[Any] = []
    for key in ("samples", "slowest"):
        raw_items = value.get(key)
        if isinstance(raw_items, list):
            samples.extend(raw_items)
    by_shortcode: dict[str, dict[str, Any]] = {}
    for raw_sample in samples:
        if not isinstance(raw_sample, Mapping):
            continue
        shortcode = str(raw_sample.get("shortcode") or raw_sample.get("source_id") or "").strip()
        if shortcode:
            by_shortcode[shortcode] = dict(raw_sample)
    return by_shortcode


def _comments_progress_target_count_from_sample(sample: Mapping[str, Any], *keys: str) -> int | None:
    for key in keys:
        if key not in sample:
            continue
        value = sample.get(key)
        if value is None:
            continue
        return _normalize_non_negative_int(value)
    return None


def _comments_progress_has_network_stop_reason(*reason_sets: Mapping[str, int]) -> bool:
    for reason_set in reason_sets:
        for reason in reason_set:
            if str(reason or "").strip().lower() in _INSTAGRAM_COMMENTS_NETWORK_STOP_REASONS:
                return True
    return False


def _comments_progress_latest_mapping_value(value: Any) -> str | None:
    if not isinstance(value, Mapping):
        return None
    for raw_value in reversed(list(value.values())):
        normalized = str(raw_value or "").strip().lower()
        if normalized:
            return normalized
    return None


def _comments_progress_latest_sample_reason(samples: Any, *, key: str) -> str | None:
    if not isinstance(samples, list):
        return None
    for sample in reversed(samples):
        if not isinstance(sample, Mapping):
            continue
        normalized = str(sample.get(key) or "").strip().lower()
        if normalized:
            return normalized
    return None


def _comments_progress_stop_reason_counts(comment_capture: Mapping[str, Any]) -> dict[str, int]:
    counts = _comments_progress_count_map(comment_capture.get("stop_reasons"))
    if counts:
        return counts
    samples = comment_capture.get("samples")
    if not isinstance(samples, list):
        return {}
    return _comments_progress_reason_counts_from_values(
        [sample.get("stop_reason") for sample in samples if isinstance(sample, Mapping)]
    )


def _comments_progress_safe_probe_payload(value: Mapping[str, Any]) -> dict[str, Any]:
    allowed_keys = {
        "status",
        "result",
        "reason",
        "mode",
        "advisory_continue",
        "session_source",
        "cookie_fingerprint",
        "cookie_fingerprint_algorithm",
        "probe_shortcode",
        "retryable",
        "operator_action",
    }
    return {key: value.get(key) for key in allowed_keys if value.get(key) is not None}


def _comments_progress_largest_gap_samples(
    *,
    targets: Sequence[Any],
    reason_maps: Sequence[Mapping[str, Any]],
    limit: int = 10,
) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw_target in targets:
        source_id = str(raw_target or "").strip()
        if not source_id or source_id in seen:
            continue
        seen.add(source_id)
        reason = None
        for reason_map in reason_maps:
            candidate = str(reason_map.get(source_id) or "").strip().lower()
            if candidate:
                reason = candidate
                break
        samples.append(
            {
                "source_id": source_id,
                "reason": reason or "targeted_retry_gap",
                "rank": len(samples) + 1,
            }
        )
        if len(samples) >= max(1, int(limit or 10)):
            break
    return samples


def _comments_progress_coverage_state(
    *,
    status: str,
    row_incomplete_posts: int,
    row_completion_reason_counts: Mapping[str, int],
    latest_fetch_reason: str | None,
    latest_failure_reason: Any,
    latest_error_code: str,
    retry_target_count: int,
) -> str:
    reason_keys = {str(key or "").strip().lower() for key in row_completion_reason_counts}
    latest_reason = str(latest_fetch_reason or latest_failure_reason or latest_error_code or "").strip().lower()
    if latest_error_code in {
        "instagram_comments_endpoint_auth_blocked",
        "instagram_comments_auth_failed",
        "instagram_comments_browser_session_invalidated",
        "checkpoint_required",
    } or "auth" in latest_reason or "checkpoint" in latest_reason or "challenge" in latest_reason:
        return "auth_blocked"
    if "http_429" in reason_keys or latest_reason == "http_429" or "rate" in latest_reason:
        return "rate_limited"
    if latest_reason in {"transport_timeout", "zstd_decode_error"} or "transport" in latest_reason:
        return "transport_failed"
    if "reply_tail_incomplete" in reason_keys or latest_reason == "reply_tail_incomplete":
        return "partial_reply_gap"
    if "relay" in latest_reason or "graphql" in latest_reason:
        return "partial_relay_gap"
    if row_incomplete_posts > 0 or retry_target_count > 0:
        return "partial_parent_only"
    if status in {"failed", "cancelled"} or latest_failure_reason:
        return "transport_failed"
    return "complete"


def _comments_progress_operational_state(
    *,
    effective_run_status: str | None,
    manual_auth_required: bool,
    retrying_jobs: int,
    queued_jobs: int,
    active_jobs: int,
    failed_jobs: int,
    failed_remaining_targets: int,
    incomplete_posts_total: int,
    stale_shards: int,
) -> str | None:
    if manual_auth_required:
        return "blocked_auth"
    if retrying_jobs > 0:
        return "retrying"
    if active_jobs > 0:
        return "running"
    if queued_jobs > 0:
        return "queued"
    if stale_shards > 0 and effective_run_status in {"running", "queued", "retrying"}:
        return "retrying"
    if failed_remaining_targets > 0 or incomplete_posts_total > 0:
        return "partial_incomplete"
    if failed_jobs > 0:
        return "failed_retryable" if failed_remaining_targets > 0 else "failed_terminal"
    if effective_run_status in {"completed", "cancelled", "failed"}:
        return effective_run_status
    return effective_run_status


def _comments_progress_recommended_next_action(
    *,
    operational_state: str | None,
    failed_remaining_targets: int,
    failed_jobs: int,
    stale_shards: int,
    incomplete_posts_total: int,
    network_stopped_targets: int = 0,
) -> str:
    if operational_state == "blocked_auth":
        return "repair_auth_then_retry"
    if operational_state == "retrying":
        return "wait_for_retry_jobs"
    if stale_shards > 0:
        return "mark_stale_jobs_terminal_or_retry"
    if network_stopped_targets > 0:
        return "retry_network_stopped_targets"
    if failed_remaining_targets > 0:
        return "retry_largest_gaps"
    if failed_jobs > 0:
        return "retry_failed_shards"
    if incomplete_posts_total > 0:
        return "retry_incomplete_targets"
    return "none"


def _build_comments_scrape_run_progress_payload(
    *,
    rows: Sequence[Mapping[str, Any]],
    platform: str,
    account_handle: str,
) -> dict[str, Any]:
    first = _metadata_dict(rows[0]) if rows else {}
    run_config = _metadata_dict(first.get("run_config"))
    raw_summary = _metadata_dict(first.get("summary"))
    total_jobs = len(rows)
    active_jobs = 0
    queued_jobs = 0
    retrying_jobs = 0
    completed_jobs = 0
    failed_jobs = 0
    cancelled_jobs = 0
    failed_remaining_targets = 0
    items_found_total = 0
    comments_processed_total = 0
    comments_upserted_total = 0
    comments_inserted_total = 0
    comments_refreshed_total = 0
    comments_changed_total = 0
    new_comments_total = 0
    has_comment_write_breakdown = False
    has_comment_changed_breakdown = False
    completed_posts = 0
    matched_posts = 0
    complete_posts_total = 0
    incomplete_posts_total = 0
    has_comment_completeness_breakdown = False
    completion_reason_counts_total: Counter[str] = Counter()
    fetch_reason_counts_total: Counter[str] = Counter()
    stop_reason_counts_total: Counter[str] = Counter()
    retry_reason_counts_total: Counter[str] = Counter()
    target_source_ids: list[str] = []
    latest_job_metadata: dict[str, Any] = {}
    latest_job_status: str | None = None
    latest_error: Any = None
    latest_error_code: str | None = None
    latest_comments_endpoint_probe: dict[str, Any] = {}
    queue_wait_seconds_values: list[int] = []
    comment_shards: list[dict[str, Any]] = []
    stale_shards = 0
    retry_source_job_ids: list[str] = []
    targeted_retry_targets: list[str] = []
    largest_remaining_gaps: list[dict[str, Any]] = []
    network_bytes_by_host_total: Counter[str] = Counter()
    network_request_count_by_host_total: Counter[str] = Counter()
    network_blocked_request_count_by_host_total: Counter[str] = Counter()
    network_blocked_bytes_estimate_by_host_total: Counter[str] = Counter()
    network_policy_modes_total: Counter[str] = Counter()
    target_progress_by_source: dict[str, dict[str, Any]] = {}
    network_stopped_target_source_ids: list[str] = []

    for row in rows:
        config = _metadata_dict(row.get("config"))
        metadata = _metadata_dict(row.get("metadata"))
        activity = _metadata_dict(metadata.get("activity"))
        stage_counters = _metadata_dict(metadata.get("stage_counters"))
        cumulative_counters = _metadata_dict(metadata.get("cumulative_counters"))
        persist_counters = _metadata_dict(metadata.get("persist_counters"))
        status = str(row.get("job_status") or "").strip().lower()
        row_items_found = _normalize_non_negative_int(row.get("items_found"))
        latest_job_status = status
        latest_job_metadata = metadata
        latest_error = row.get("error_message") or latest_error
        latest_error_code = str(row.get("last_error_code") or metadata.get("error_code") or "").strip().lower() or None
        fetcher_runtime = _metadata_dict(metadata.get("fetcher_runtime")) or _metadata_dict(
            metadata.get("runtime_metadata")
        )
        runtime_metadata = _metadata_dict(metadata.get("runtime_metadata"))
        endpoint_probe = _metadata_dict(fetcher_runtime.get("comments_auth_validation")) or _metadata_dict(
            metadata.get("comments_endpoint_probe")
        )
        if endpoint_probe:
            latest_comments_endpoint_probe = _comments_progress_safe_probe_payload(endpoint_probe)
        network_policy = _metadata_dict(fetcher_runtime.get("network_policy"))
        row_network_bytes_by_host = Counter(_comments_progress_count_map(fetcher_runtime.get("bytes_by_host")))
        row_network_request_count_by_host = Counter(
            _comments_progress_count_map(fetcher_runtime.get("request_count_by_host"))
            or _comments_progress_count_map(network_policy.get("request_count_by_host"))
        )
        row_network_blocked_request_count_by_host = Counter(
            _comments_progress_count_map(network_policy.get("blocked_request_count_by_host"))
        )
        row_network_blocked_bytes_estimate_by_host = Counter(
            _comments_progress_count_map(network_policy.get("blocked_bytes_estimate_by_host"))
        )
        network_bytes_by_host_total.update(row_network_bytes_by_host)
        network_request_count_by_host_total.update(row_network_request_count_by_host)
        network_blocked_request_count_by_host_total.update(row_network_blocked_request_count_by_host)
        network_blocked_bytes_estimate_by_host_total.update(row_network_blocked_bytes_estimate_by_host)
        network_policy_mode = str(network_policy.get("mode") or "").strip().lower()
        if network_policy_mode:
            network_policy_modes_total[network_policy_mode] += 1

        if status == "completed":
            completed_jobs += 1
        elif status == "cancelled":
            cancelled_jobs += 1
        elif status == "failed":
            failed_jobs += 1
        elif status in {"queued", "pending", "retrying", "cancelling", "running"}:
            if status in {"queued", "pending"}:
                queued_jobs += 1
            if status == "retrying":
                retrying_jobs += 1
            if status == "running":
                active_jobs += 1

        current_stage_posts = _normalize_non_negative_int(stage_counters.get("posts"))
        current_stage_comments = _normalize_non_negative_int(stage_counters.get("comments"))
        cumulative_posts = _normalize_non_negative_int(cumulative_counters.get("posts"))
        cumulative_comments = _normalize_non_negative_int(cumulative_counters.get("comments"))
        cumulative_comments_upserted = _normalize_non_negative_int(cumulative_counters.get("comments_upserted"))
        has_row_inserted = "comments_inserted" in cumulative_counters or "comments_inserted" in persist_counters
        has_row_refreshed = "comments_refreshed" in cumulative_counters or "comments_refreshed" in persist_counters
        has_row_changed = "comments_changed" in cumulative_counters or "comments_changed" in persist_counters
        cumulative_comments_inserted = _normalize_non_negative_int(cumulative_counters.get("comments_inserted"))
        cumulative_comments_refreshed = _normalize_non_negative_int(cumulative_counters.get("comments_refreshed"))
        cumulative_comments_changed = _normalize_non_negative_int(cumulative_counters.get("comments_changed"))
        if status == "running":
            stage_posts = cumulative_posts + current_stage_posts
            stage_comments = cumulative_comments + current_stage_comments
        else:
            stage_posts = max(cumulative_posts, current_stage_posts)
            stage_comments = max(cumulative_comments, current_stage_comments)
        if stage_comments <= 0 and row_items_found > stage_posts:
            stage_comments = max(row_items_found - stage_posts, 0)
        row_comments_upserted_current = _normalize_non_negative_int(persist_counters.get("comments_upserted"))
        row_comments_upserted = (
            cumulative_comments_upserted + row_comments_upserted_current
            if status == "running"
            else max(cumulative_comments_upserted, row_comments_upserted_current)
        )
        row_comments_inserted_current = _normalize_non_negative_int(persist_counters.get("comments_inserted"))
        row_comments_inserted = (
            cumulative_comments_inserted + row_comments_inserted_current
            if status == "running"
            else max(cumulative_comments_inserted, row_comments_inserted_current)
        )
        row_comments_refreshed_current = _normalize_non_negative_int(persist_counters.get("comments_refreshed"))
        row_comments_refreshed = (
            cumulative_comments_refreshed + row_comments_refreshed_current
            if status == "running"
            else max(cumulative_comments_refreshed, row_comments_refreshed_current)
        )
        if has_row_inserted and not has_row_refreshed:
            row_comments_refreshed = max(row_comments_upserted - row_comments_inserted, 0)
            has_row_refreshed = True
        row_comments_changed_current = _normalize_non_negative_int(persist_counters.get("comments_changed"))
        row_comments_changed = (
            cumulative_comments_changed + row_comments_changed_current
            if status == "running"
            else max(cumulative_comments_changed, row_comments_changed_current)
        )
        row_has_write_breakdown = has_row_inserted or has_row_refreshed
        if has_row_changed:
            row_has_write_breakdown = True
            has_comment_changed_breakdown = True
        # "New comments" is a net-new saved-row count. Existing rows can still
        # be refreshed or receive changed metadata, but those writes do not
        # increase the account's saved-comments total.
        row_new_comments = row_comments_inserted
        row_items_found_display = max(row_items_found, stage_posts + stage_comments)
        items_found_total += row_items_found_display
        comments_processed_total += stage_comments
        comments_upserted_total += row_comments_upserted
        if row_has_write_breakdown:
            has_comment_write_breakdown = True
            comments_inserted_total += row_comments_inserted
            comments_refreshed_total += row_comments_refreshed
            comments_changed_total += row_comments_changed
            new_comments_total += row_new_comments
        activity_posts = _normalize_non_negative_int(activity.get("posts_checked"))
        row_posts = stage_posts or activity_posts
        row_matched_posts = max(
            row_posts,
            _normalize_non_negative_int(activity.get("matched_posts")),
            _normalize_non_negative_int(persist_counters.get("posts_upserted")),
        )
        completed_posts += row_posts
        matched_posts += row_matched_posts
        comment_completeness = _metadata_dict(metadata.get("comment_completeness"))
        row_complete_posts = _normalize_non_negative_int(comment_completeness.get("complete_posts"))
        row_incomplete_posts = _normalize_non_negative_int(comment_completeness.get("incomplete_posts"))
        row_completion_reason_counts = _comments_progress_count_map(comment_completeness.get("completion_reasons"))
        if row_complete_posts or row_incomplete_posts or row_completion_reason_counts:
            has_comment_completeness_breakdown = True
            complete_posts_total += row_complete_posts
            incomplete_posts_total += row_incomplete_posts
            completion_reason_counts_total.update(row_completion_reason_counts)
        retry_rebalance = _metadata_dict(metadata.get("retry_rebalance"))
        remaining_retry_targets = [
            str(item or "").strip()
            for item in retry_rebalance.get("remaining_target_source_ids") or []
            if str(item or "").strip()
        ]
        failed_remaining_targets += len(remaining_retry_targets)
        targeted_retry_targets.extend(remaining_retry_targets)
        retry_source_job_id = str(config.get("comments_retry_rebalance_source_job_id") or "").strip()
        if retry_source_job_id:
            retry_source_job_ids.append(retry_source_job_id)
        shard_target_source_ids = [
            str(item or "").strip() for item in config.get("target_source_ids") or [] if str(item or "").strip()
        ]
        for source_id in config.get("target_source_ids") or []:
            normalized_source_id = str(source_id or "").strip()
            if normalized_source_id:
                target_source_ids.append(normalized_source_id)

        created_at = _coerce_dt(row.get("job_created_at") or row.get("created_at"))
        started_at = _coerce_dt(row.get("job_started_at") or row.get("started_at"))
        completed_at = _coerce_dt(row.get("job_completed_at") or row.get("completed_at"))
        queue_wait_seconds: int | None = None
        if created_at and started_at:
            queue_wait_seconds = max(0, int((started_at - created_at).total_seconds()))
            queue_wait_seconds_values.append(queue_wait_seconds)
        shard_elapsed_seconds = (
            max(1, int(((completed_at or _now_utc()) - started_at).total_seconds()))
            if isinstance(started_at, datetime)
            else 0
        )
        shard_target_count = _normalize_non_negative_int(config.get("comments_shard_target_count")) or len(
            shard_target_source_ids
        )
        remaining_targets = remaining_retry_targets
        retry_target_count = len(remaining_targets)
        if status == "completed":
            remaining_target_count = 0
        elif shard_target_count > 0 and status in {"queued", "pending", "retrying", "running"}:
            remaining_target_count = max(shard_target_count - min(row_posts, shard_target_count), 0)
        else:
            remaining_target_count = 0
        post_fetch_failures = _metadata_dict(metadata.get("post_fetch_failures"))
        post_auth_failures = _metadata_dict(metadata.get("post_auth_failures"))
        auth_failed_target_source_ids = [
            str(item or "").strip()
            for item in (
                post_auth_failures.get("target_source_ids")
                or metadata.get("auth_failed_target_source_ids")
                or []
            )
            if str(item or "").strip()
        ]
        comment_capture = _metadata_dict(metadata.get("comment_capture"))
        post_latency_by_shortcode = _comments_progress_sample_by_shortcode(metadata.get("post_latency"))
        post_fetch_failure_target_metadata = _metadata_dict(post_fetch_failures.get("target_metadata"))
        top_level_checkpoint_by_source: dict[str, dict[str, Any]] = {}
        for raw_checkpoint in metadata.get("top_level_checkpoints") or []:
            if not isinstance(raw_checkpoint, Mapping):
                continue
            checkpoint_source = str(
                raw_checkpoint.get("target_shortcode")
                or raw_checkpoint.get("source_id")
                or raw_checkpoint.get("shortcode")
                or ""
            ).strip()
            if checkpoint_source:
                top_level_checkpoint_by_source[checkpoint_source] = dict(raw_checkpoint)
        for raw_checkpoint in _metadata_dict(metadata.get("top_level_checkpoint_summary")).get("items") or []:
            if not isinstance(raw_checkpoint, Mapping):
                continue
            checkpoint_source = str(
                raw_checkpoint.get("target_shortcode")
                or raw_checkpoint.get("source_id")
                or raw_checkpoint.get("shortcode")
                or ""
            ).strip()
            if checkpoint_source:
                top_level_checkpoint_by_source[checkpoint_source] = dict(raw_checkpoint)
        reply_resume_counts_by_source: Counter[str] = Counter()
        for raw_checkpoint in _metadata_dict(metadata.get("reply_checkpoint_summary")).get("items") or []:
            if not isinstance(raw_checkpoint, Mapping):
                continue
            checkpoint_source = str(
                raw_checkpoint.get("target_shortcode")
                or raw_checkpoint.get("source_id")
                or raw_checkpoint.get("shortcode")
                or ""
            ).strip()
            if checkpoint_source:
                reply_resume_counts_by_source[checkpoint_source] += 1
        row_fetch_reason_counts = Counter(_comments_progress_count_map(post_fetch_failures.get("reason_counts")))
        if not row_fetch_reason_counts:
            row_fetch_reason_counts.update(
                _comments_progress_reason_counts_from_values(post_fetch_failures.get("fetch_reasons"))
            )
        row_fetch_reason_counts.update(
            _comments_progress_reason_counts_from_values(post_auth_failures.get("fetch_reasons"))
        )
        row_fetch_reason_counts.update(
            _comments_progress_reason_counts_from_values(metadata.get("auth_failed_fetch_reasons"))
        )
        row_stop_reason_counts = _comments_progress_stop_reason_counts(comment_capture)
        row_retry_reason_counts = _comments_progress_merge_counts(
            fetcher_runtime.get("retry_reason_counts"),
            runtime_metadata.get("retry_reason_counts"),
        )
        row_has_network_stop_reason = _comments_progress_has_network_stop_reason(
            row_fetch_reason_counts,
            row_stop_reason_counts,
            row_retry_reason_counts,
        )
        fetch_reason_counts_total.update(row_fetch_reason_counts)
        stop_reason_counts_total.update(row_stop_reason_counts)
        retry_reason_counts_total.update(row_retry_reason_counts)
        shard_error_message = row.get("error_message")
        shard_error_code = str(row.get("last_error_code") or metadata.get("last_error_code") or "").strip().lower()
        shard_has_current_progress = row_posts > 0 or row_items_found > 0
        shard_error_text = str(shard_error_message or "").strip().lower()
        stale_dispatch_error = shard_error_code == "stale_modal_dispatch_unclaimed" or (
            "modal dispatch lease expired" in shard_error_text and "before any worker claimed" in shard_error_text
        )
        stale_heartbeat_error = shard_error_code in {
            "stale_heartbeat",
            "stale_heartbeat_timeout",
            "stale_modal_dispatch_unclaimed",
        } or "stale heartbeat" in shard_error_text
        if stale_dispatch_error or stale_heartbeat_error:
            stale_shards += 1
        if (
            status in {"queued", "pending", "retrying", "running"}
            and shard_has_current_progress
            and stale_dispatch_error
        ):
            shard_error_message = None
        latest_failure_reason = (
            shard_error_message
            or metadata.get("latest_failure_reason")
            or metadata.get("failure_reason")
            or _metadata_dict(metadata.get("activity")).get("failure_reason")
        )
        latest_fetch_reason = (
            _comments_progress_latest_mapping_value(post_fetch_failures.get("fetch_reasons"))
            or _comments_progress_latest_mapping_value(metadata.get("incomplete_fetch_reasons"))
            or _comments_progress_latest_mapping_value(runtime_metadata.get("incomplete_fetch_reasons"))
            or _comments_progress_latest_mapping_value(post_auth_failures.get("fetch_reasons"))
            or _comments_progress_latest_mapping_value(metadata.get("auth_failed_fetch_reasons"))
            or str(latest_failure_reason or "").strip().lower()
            or None
        )
        latest_stop_reason = str(
            _metadata_dict(comment_capture.get("latest")).get("stop_reason") or ""
        ).strip().lower() or _comments_progress_latest_sample_reason(comment_capture.get("samples"), key="stop_reason")
        gap_samples = _comments_progress_largest_gap_samples(
            targets=remaining_targets,
            reason_maps=[
                _metadata_dict(metadata.get("incomplete_fetch_reasons")),
                _metadata_dict(runtime_metadata.get("incomplete_fetch_reasons")),
                _metadata_dict(post_fetch_failures.get("fetch_reasons")),
                _metadata_dict(post_auth_failures.get("fetch_reasons")),
            ],
            limit=10,
        )
        largest_remaining_gaps.extend(gap_samples)
        target_reason_maps = [
            _metadata_dict(metadata.get("incomplete_fetch_reasons")),
            _metadata_dict(runtime_metadata.get("incomplete_fetch_reasons")),
            _metadata_dict(post_fetch_failures.get("fetch_reasons")),
            _metadata_dict(post_auth_failures.get("fetch_reasons")),
            _metadata_dict(metadata.get("auth_failed_fetch_reasons")),
        ]
        row_current_target_fetch = _metadata_dict(metadata.get("current_target_fetch"))
        current_fetch_source = str(row_current_target_fetch.get("shortcode") or "").strip()
        for target_index, source_id in enumerate(shard_target_source_ids, start=1):
            if not source_id:
                continue
            target_row = target_progress_by_source.setdefault(
                source_id,
                {
                    "source_id": source_id,
                    "shortcode": source_id,
                    "job_ids": [],
                    "statuses": {},
                },
            )
            job_id_text = str(row.get("job_id") or "").strip()
            if job_id_text:
                target_row["job_id"] = job_id_text
                target_row.setdefault("job_ids", [])
                if job_id_text not in target_row["job_ids"]:
                    target_row["job_ids"].append(job_id_text)
            if status:
                target_row["status"] = status
                statuses = _metadata_dict(target_row.get("statuses"))
                statuses[status] = _normalize_non_negative_int(statuses.get(status)) + 1
                target_row["statuses"] = statuses
            target_row["target_index"] = target_index
            target_row["job_target_count"] = shard_target_count
            target_row["shard_index"] = _normalize_non_negative_int(config.get("comments_shard_index")) or None
            target_row["shard_count"] = _normalize_non_negative_int(config.get("comments_shard_count")) or None
            reason = None
            for reason_map in target_reason_maps:
                candidate = str(reason_map.get(source_id) or "").strip().lower()
                if candidate:
                    reason = candidate
                    break
            if reason:
                target_row["latest_reason"] = reason
                target_row["fetch_reason"] = reason
            if latest_stop_reason:
                target_row["latest_stop_reason"] = latest_stop_reason
            if source_id in remaining_targets or source_id in remaining_retry_targets:
                target_row["remaining"] = True
                target_row["retryable"] = True
            if source_id in auth_failed_target_source_ids:
                target_row["auth_failed"] = True
                target_row["remaining"] = True
            if row_has_network_stop_reason and (
                reason in _INSTAGRAM_COMMENTS_NETWORK_STOP_REASONS
                or source_id in remaining_targets
                or source_id in remaining_retry_targets
            ):
                target_row["network_stopped"] = True
                target_row["retryable"] = True
                if source_id in remaining_targets or source_id in remaining_retry_targets:
                    network_stopped_target_source_ids.append(source_id)
            target_metadata = _metadata_dict(post_fetch_failure_target_metadata.get(source_id))
            sample = post_latency_by_shortcode.get(source_id) or {}
            if current_fetch_source == source_id:
                sample = {**sample, **row_current_target_fetch}
                target_row["current_phase"] = row_current_target_fetch.get("phase")
            reported_count = _comments_progress_target_count_from_sample(
                sample,
                "reported_comment_count",
                "expected_comment_count",
                "comments_count",
            )
            if reported_count is None:
                reported_count = _comments_progress_target_count_from_sample(
                    target_metadata,
                    "reported_comment_count",
                    "expected_comment_count",
                    "comments_count",
                )
            saved_count = _comments_progress_target_count_from_sample(
                sample,
                "stored_total_comments",
                "saved_comment_count",
                "comments_upserted",
            )
            if saved_count is None:
                saved_count = _comments_progress_target_count_from_sample(
                    target_metadata,
                    "stored_total_comments",
                    "saved_comment_count",
                    "comments_upserted",
                )
            observed_count = _comments_progress_target_count_from_sample(
                sample,
                "observed_comment_count",
                "comments_fetched",
                "top_level_comment_count",
            )
            if reported_count is not None:
                target_row["reported_comment_count"] = reported_count
            if saved_count is not None:
                target_row["saved_comment_count"] = saved_count
            if observed_count is not None:
                target_row["observed_comment_count"] = observed_count
            if reported_count is not None and saved_count is not None:
                target_row["missing_comment_gap"] = max(reported_count - saved_count, 0)
            if source_id in top_level_checkpoint_by_source:
                checkpoint = top_level_checkpoint_by_source[source_id]
                target_row["has_top_level_cursor"] = True
                target_row["cursor_stop_reason"] = checkpoint.get("stop_reason")
                if checkpoint.get("pages_seen") is not None:
                    target_row["pages_seen"] = _normalize_non_negative_int(checkpoint.get("pages_seen"))
            if reply_resume_counts_by_source.get(source_id):
                target_row["reply_resume_count"] = int(reply_resume_counts_by_source[source_id])
        coverage_state = _comments_progress_coverage_state(
            status=status,
            row_incomplete_posts=row_incomplete_posts,
            row_completion_reason_counts=row_completion_reason_counts,
            latest_fetch_reason=latest_fetch_reason,
            latest_failure_reason=latest_failure_reason,
            latest_error_code=shard_error_code,
            retry_target_count=retry_target_count,
        )
        shard_average_seconds_per_post = _comments_progress_average_seconds(shard_elapsed_seconds, row_posts)
        shard_average_seconds_per_comment = _comments_progress_average_seconds(
            shard_elapsed_seconds,
            stage_comments,
            digits=4,
        )
        shard_payload = {
            "job_id": str(row.get("job_id") or "").strip() or None,
            "shard_index": _normalize_non_negative_int(config.get("comments_shard_index")) or None,
            "shard_count": _normalize_non_negative_int(config.get("comments_shard_count")) or None,
            "status": status or None,
            "target_count": shard_target_count,
            "target_source_ids_count": shard_target_count,
            "comments_shard_target_count": shard_target_count,
            "processed_post_count": row_posts,
            "completed_posts": row_posts,
            "matched_posts": row_matched_posts,
            "complete_posts": row_complete_posts,
            "incomplete_posts": row_incomplete_posts,
            "completion_reason_counts": row_completion_reason_counts,
            "remaining_target_count": remaining_target_count,
            "retry_target_count": retry_target_count,
            "coverage_state": coverage_state,
            "comments_processed": stage_comments,
            "comments_upserted": row_comments_upserted,
            "queue_wait_seconds": queue_wait_seconds,
            "posts_per_minute": _comments_progress_per_minute(row_posts, shard_elapsed_seconds),
            "posts_per_second": _comments_progress_per_second(row_posts, shard_elapsed_seconds),
            "comments_per_minute": _comments_progress_per_minute(stage_comments, shard_elapsed_seconds),
            "comments_per_second": _comments_progress_per_second(stage_comments, shard_elapsed_seconds),
            "average_seconds_per_post": shard_average_seconds_per_post,
            "average_seconds_per_comment": shard_average_seconds_per_comment,
            "estimated_seconds_remaining": _comments_progress_estimated_seconds_remaining(
                remaining_target_count,
                shard_average_seconds_per_post,
            ),
            "items_found_total": row_items_found_display,
            "error_message": shard_error_message,
            "latest_failure_reason": latest_failure_reason,
            "latest_fetch_reason": latest_fetch_reason,
            "fetch_reason_counts": dict(row_fetch_reason_counts),
            "latest_stop_reason": latest_stop_reason,
            "stop_reason_counts": row_stop_reason_counts,
            "retry_reason_counts": row_retry_reason_counts,
        }
        if row_network_bytes_by_host or row_network_request_count_by_host or row_network_blocked_request_count_by_host:
            shard_payload["network_spend"] = _comments_progress_network_spend_payload(
                bytes_by_host=row_network_bytes_by_host,
                request_count_by_host=row_network_request_count_by_host,
                blocked_request_count_by_host=row_network_blocked_request_count_by_host,
                blocked_bytes_estimate_by_host=row_network_blocked_bytes_estimate_by_host,
                policy_modes={network_policy_mode: 1} if network_policy_mode else {},
                host_limit=5,
            )
        if gap_samples:
            shard_payload["largest_remaining_gaps"] = gap_samples
        if row_has_write_breakdown:
            shard_payload["comments_inserted"] = row_comments_inserted
            shard_payload["comments_refreshed"] = row_comments_refreshed
            shard_payload["new_comments"] = row_new_comments
            if has_row_changed:
                shard_payload["comments_changed"] = row_comments_changed
        comment_shards.append(shard_payload)

    raw_run_status = str(first.get("run_status") or "").strip().lower() or None
    effective_run_status = raw_run_status
    if raw_run_status != "cancelled":
        if active_jobs > 0:
            effective_run_status = "running"
        elif retrying_jobs > 0:
            effective_run_status = "retrying"
        elif queued_jobs > 0:
            effective_run_status = "queued"
        elif total_jobs > 0 and (failed_jobs > 0 or failed_remaining_targets > 0):
            effective_run_status = "failed"
        elif total_jobs > 0 and completed_jobs >= total_jobs:
            effective_run_status = "completed"

    started_at = _coerce_dt(first.get("started_at") or first.get("created_at"))
    completed_at = _coerce_dt(first.get("completed_at"))
    elapsed_until = completed_at or _now_utc()
    elapsed_seconds = (
        max(1, int((elapsed_until - started_at).total_seconds())) if isinstance(started_at, datetime) else 0
    )
    posts_per_minute = _comments_progress_per_minute(completed_posts, elapsed_seconds)
    posts_per_second = _comments_progress_per_second(completed_posts, elapsed_seconds)
    comments_per_minute = _comments_progress_per_minute(comments_processed_total, elapsed_seconds)
    comments_per_second = _comments_progress_per_second(comments_processed_total, elapsed_seconds)
    run_target_total = _normalize_non_negative_int(run_config.get("target_source_ids_count"))
    target_source_ids_count = run_target_total or len(dict.fromkeys(target_source_ids))
    shard_count = _normalize_non_negative_int(run_config.get("comments_shard_count")) or max(1, total_jobs)
    del raw_summary
    post_progress_total = target_source_ids_count or None
    display_completed_posts = (
        min(completed_posts, target_source_ids_count) if target_source_ids_count else completed_posts
    )
    display_matched_posts = min(matched_posts, target_source_ids_count) if target_source_ids_count else matched_posts
    remaining_post_count = (
        max(post_progress_total - display_completed_posts, 0) if post_progress_total is not None else None
    )
    average_seconds_per_post = _comments_progress_average_seconds(elapsed_seconds, display_completed_posts)
    average_seconds_per_comment = _comments_progress_average_seconds(
        elapsed_seconds,
        comments_processed_total,
        digits=4,
    )
    estimated_seconds_remaining = (
        _comments_progress_estimated_seconds_remaining(remaining_post_count, average_seconds_per_post)
        if remaining_post_count is not None
        else None
    )
    summary = {
        "total_jobs": total_jobs,
        "completed_jobs": completed_jobs,
        "failed_jobs": failed_jobs,
        "cancelled_jobs": cancelled_jobs,
        "active_jobs": active_jobs + queued_jobs + retrying_jobs,
        "running_jobs": active_jobs,
        "queued_jobs": queued_jobs,
        "retrying_jobs": retrying_jobs,
        "items_found_total": items_found_total,
        "comments_processed_total": comments_processed_total,
        "comments_upserted_total": comments_upserted_total,
    }
    if has_comment_write_breakdown:
        summary["comments_inserted_total"] = comments_inserted_total
        summary["comments_refreshed_total"] = comments_refreshed_total
        summary["new_comments_total"] = new_comments_total
        if has_comment_changed_breakdown:
            summary["comments_changed_total"] = comments_changed_total
    if has_comment_completeness_breakdown:
        summary["complete_posts_total"] = complete_posts_total
        summary["incomplete_posts_total"] = incomplete_posts_total
        summary["completion_reason_counts"] = dict(completion_reason_counts_total)
    if fetch_reason_counts_total:
        summary["fetch_reason_counts"] = dict(fetch_reason_counts_total)
    if stop_reason_counts_total:
        summary["stop_reason_counts"] = dict(stop_reason_counts_total)
    if retry_reason_counts_total:
        summary["retry_reason_counts"] = dict(retry_reason_counts_total)
    network_spend = _comments_progress_network_spend_payload(
        bytes_by_host=network_bytes_by_host_total,
        request_count_by_host=network_request_count_by_host_total,
        blocked_request_count_by_host=network_blocked_request_count_by_host_total,
        blocked_bytes_estimate_by_host=network_blocked_bytes_estimate_by_host_total,
        policy_modes=network_policy_modes_total,
    )
    if network_spend.get("observed_proxy_bytes") or network_spend.get("observed_request_count"):
        summary["observed_network_bytes_total"] = network_spend.get("observed_proxy_bytes", 0)
        summary["static_cdninstagram_bytes"] = network_spend.get("static_cdninstagram_bytes", 0)
        summary["static_cdninstagram_blocked_requests"] = network_spend.get(
            "static_cdninstagram_blocked_request_count",
            0,
        )
    network_stopped_target_source_ids = list(dict.fromkeys(network_stopped_target_source_ids))
    target_progress_rows = list(target_progress_by_source.values())
    for target_row in target_progress_rows:
        job_ids = [str(item or "").strip() for item in target_row.get("job_ids") or [] if str(item or "").strip()]
        if job_ids:
            target_row["job_ids"] = job_ids[-5:]
        else:
            target_row.pop("job_ids", None)
        if not target_row.get("latest_reason") and target_row.get("network_stopped"):
            target_row["latest_reason"] = "network_stopped"
        target_row.setdefault("remaining", False)
        target_row.setdefault("retryable", False)
        target_row.setdefault("network_stopped", False)
    target_progress_rows.sort(
        key=lambda item: (
            not bool(item.get("remaining")),
            not bool(item.get("network_stopped")),
            -_normalize_non_negative_int(item.get("missing_comment_gap")),
            str(item.get("shortcode") or item.get("source_id") or ""),
        )
    )
    latest_auth_context = _metadata_dict(latest_job_metadata.get("auth_context"))
    latest_fetcher_runtime = _metadata_dict(latest_job_metadata.get("fetcher_runtime")) or _metadata_dict(
        latest_job_metadata.get("runtime_metadata")
    )
    auth_validation_mode = (
        str(
            latest_auth_context.get("comments_auth_validation_mode")
            or run_config.get("comments_auth_validation_mode")
            or run_config.get("auth_validation_mode")
            or latest_comments_endpoint_probe.get("mode")
            or ""
        )
        .strip()
        .lower()
        or None
    )
    probe_status = (
        str(latest_comments_endpoint_probe.get("status") or latest_comments_endpoint_probe.get("result") or "")
        .strip()
        .lower()
    )
    probe_advisory_continue = _metadata_truthy(latest_comments_endpoint_probe.get("advisory_continue"))
    run_has_active_progress = (
        effective_run_status in {"queued", "pending", "retrying", "running"}
        and active_jobs + queued_jobs + retrying_jobs > 0
        and (
            items_found_total > 0 or comments_processed_total > 0 or comments_upserted_total > 0 or completed_posts > 0
        )
    )
    endpoint_probe_advisory_active = (
        probe_status == "auth_blocked" and probe_advisory_continue and run_has_active_progress
    )
    endpoint_probe_auth_codes = {
        "instagram_comments_endpoint_auth_blocked",
        "checkpoint_required",
    }
    hard_auth_error_codes = {
        "instagram_comments_endpoint_auth_blocked",
        "instagram_comments_auth_failed",
        "instagram_comments_browser_session_invalidated",
        "instagram_comments_warmup_auth_failed",
        "instagram_comments_warmup_no_cookies",
        "checkpoint_required",
    }
    manual_auth_required = (
        (probe_status == "auth_blocked" and not endpoint_probe_advisory_active)
        or latest_error_code in hard_auth_error_codes - endpoint_probe_auth_codes
        or (latest_error_code in endpoint_probe_auth_codes and not endpoint_probe_advisory_active)
    )
    operational_state = _comments_progress_operational_state(
        effective_run_status=effective_run_status,
        manual_auth_required=manual_auth_required,
        retrying_jobs=retrying_jobs,
        queued_jobs=queued_jobs,
        active_jobs=active_jobs,
        failed_jobs=failed_jobs,
        failed_remaining_targets=failed_remaining_targets,
        incomplete_posts_total=incomplete_posts_total,
        stale_shards=stale_shards,
    )
    recommended_next_action = _comments_progress_recommended_next_action(
        operational_state=operational_state,
        failed_remaining_targets=failed_remaining_targets,
        failed_jobs=failed_jobs,
        stale_shards=stale_shards,
        incomplete_posts_total=incomplete_posts_total,
        network_stopped_targets=len(network_stopped_target_source_ids),
    )
    proxy_session_state = {
        key: value
        for key, value in {
            "selected_proxy_fingerprint": latest_fetcher_runtime.get("selected_proxy_fingerprint"),
            "proxy_session_mode": latest_fetcher_runtime.get("proxy_session_mode"),
            "global_rate_limit_key": latest_fetcher_runtime.get("global_rate_limit_key"),
            "transport": latest_fetcher_runtime.get("transport"),
        }.items()
        if value is not None
    }
    post_progress = {
        "completed_posts": display_completed_posts,
        "matched_posts": display_matched_posts,
        "total_posts": post_progress_total,
    }
    if has_comment_completeness_breakdown:
        post_progress["complete_posts"] = complete_posts_total
        post_progress["incomplete_posts"] = incomplete_posts_total
    instagram_access_proof = _metadata_dict(run_config.get("instagram_access_proof")) or _metadata_dict(
        latest_job_metadata.get("instagram_access_proof")
    )
    if not instagram_access_proof:
        instagram_access_proof = _instagram_comments_access_proof(
            public_mode=comments_public_mode_from_config(run_config),
        )
    worker_counters = {
        "total": total_jobs,
        "active": active_jobs + queued_jobs + retrying_jobs,
        "running": active_jobs,
        "queued": queued_jobs,
        "retrying": retrying_jobs,
        "completed": completed_jobs,
        "cancelled": cancelled_jobs,
        "failed": failed_jobs,
        "stale": stale_shards,
    }
    return {
        "run_id": str(first.get("run_id") or "").strip(),
        "platform": platform,
        "account_handle": account_handle,
        "run_status": effective_run_status,
        "operational_state": operational_state,
        "recommended_next_action": recommended_next_action,
        "created_at": first.get("created_at"),
        "started_at": first.get("started_at"),
        "completed_at": first.get("completed_at"),
        "summary": summary,
        "network_spend": network_spend,
        "job_status": latest_job_status,
        "job_metadata": latest_job_metadata,
        "error_message": latest_error,
        "mode": str(run_config.get("mode") or "").strip().lower() or None,
        "refresh_policy": str(run_config.get("refresh_policy") or "").strip().lower() or None,
        "target_filter": str(run_config.get("target_filter") or "").strip().lower() or None,
        "incomplete_fill": bool(run_config.get("incomplete_fill")),
        "auth_validation_mode": auth_validation_mode,
        "comments_endpoint_probe": latest_comments_endpoint_probe or None,
        "comments_endpoint_probe_advisory_active": endpoint_probe_advisory_active,
        "manual_auth_required": manual_auth_required,
        "proxy_session_state": proxy_session_state or None,
        "instagram_access_proof": instagram_access_proof,
        "target_source_ids": list(dict.fromkeys(target_source_ids)),
        "target_source_ids_count": target_source_ids_count,
        "comments_shard_count": shard_count,
        "comments_sharding_enabled": shard_count > 1,
        "recommended_comments_shard_count": _normalize_non_negative_int(
            run_config.get("recommended_comments_shard_count")
        )
        or _instagram_comments_recommended_shard_count(target_count=target_source_ids_count),
        "active_comment_jobs": active_jobs,
        "queued_comment_jobs": queued_jobs,
        "retrying_comment_jobs": retrying_jobs,
        "completed_comment_jobs": completed_jobs,
        "cancelled_comment_jobs": cancelled_jobs,
        "failed_comment_jobs": failed_jobs,
        "stale_comment_jobs": stale_shards,
        "worker_counters": worker_counters,
        "post_progress": post_progress,
        "throughput": {
            "elapsed_seconds": elapsed_seconds,
            "posts_per_minute": posts_per_minute,
            "posts_per_second": posts_per_second,
            "comments_per_minute": comments_per_minute,
            "comments_per_second": comments_per_second,
            "average_seconds_per_post": average_seconds_per_post,
            "average_seconds_per_comment": average_seconds_per_comment,
            "remaining_posts": remaining_post_count,
            "estimated_seconds_remaining": estimated_seconds_remaining,
        },
        "cancellation_summary": {
            "cancelled_jobs": cancelled_jobs,
            "failed_jobs": failed_jobs,
            "remaining_target_source_ids_count": failed_remaining_targets,
            "resume_recommendation": "stale_or_missing" if cancelled_jobs or failed_remaining_targets else None,
        },
        "retry_progress": {
            "retry_target_count": failed_remaining_targets,
            "retry_source_job_ids": list(dict.fromkeys(retry_source_job_ids)),
            "targeted_retry_target_count": len(dict.fromkeys(targeted_retry_targets)),
            "network_stopped_target_count": len(network_stopped_target_source_ids),
            "network_stopped_target_source_ids": network_stopped_target_source_ids[:50],
            "largest_remaining_gaps": largest_remaining_gaps[:10],
            "target_progress_rows": target_progress_rows[:50],
            "top_incomplete_reasons": dict(completion_reason_counts_total or fetch_reason_counts_total),
        },
        "target_progress_rows": target_progress_rows[:50],
        "target_progress": target_progress_rows[:50],
        "largest_remaining_gaps": largest_remaining_gaps[:10],
        "top_incomplete_reasons": dict(completion_reason_counts_total or fetch_reason_counts_total),
        "comment_shards": comment_shards,
        "shards": comment_shards,
        "shard_progress": comment_shards,
        "timing": {
            "queue_wait_seconds_max": max(queue_wait_seconds_values or [0]),
            "queue_wait_seconds_avg": (
                round(sum(queue_wait_seconds_values) / len(queue_wait_seconds_values), 2)
                if queue_wait_seconds_values
                else None
            ),
        },
    }


def get_social_account_comments_scrape_run_progress(
    platform: str,
    account_handle: str,
    run_id: str,
    auto_rebalance_slow_shards: bool = False,
) -> dict[str, Any]:
    normalized_platform = _normalize_social_account_profile_platform(platform)
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    auto_rebalance_payload: dict[str, Any] | None = None
    if auto_rebalance_slow_shards:
        try:
            auto_rebalance_payload = rebalance_slow_instagram_comments_shards(run_id=run_id)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Instagram comments slow-shard rebalance check failed: run_id=%s error=%s",
                run_id,
                exc,
                exc_info=True,
            )
            auto_rebalance_payload = {"created_job_ids": [], "error": str(exc)}
    rows = pg.fetch_all(
        """
        select
          r.id::text as run_id,
          r.status as run_status,
          r.created_at,
          r.started_at,
          r.completed_at,
          r.config as run_config,
          r.summary,
          j.id::text as job_id,
          j.status as job_status,
          j.items_found,
          j.created_at as job_created_at,
          j.started_at as job_started_at,
          j.completed_at as job_completed_at,
          j.error_message,
          j.last_error_code,
          j.last_error_class,
          j.metadata,
          j.config
        from social.scrape_runs r
        join social.scrape_jobs j on j.run_id = r.id
        where r.id = %s::uuid
          and j.platform = %s
          and coalesce(j.config->>'stage', j.metadata->>'stage', j.job_type) = %s
          and ltrim(lower(coalesce(j.config->>'account', j.metadata->>'account', '')), '@') = %s
          and not (
            j.status = 'failed'
            and exists (
              select 1
              from social.scrape_jobs child
              where child.run_id = j.run_id
                and child.id <> j.id
                and child.platform = j.platform
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
          )
        order by j.created_at asc, j.id asc
        """,
        [run_id, normalized_platform, INSTAGRAM_COMMENTS_SCRAPLING_STAGE, normalized_account],
    )
    if not rows:
        raise LookupError("Comments scrape run not found.")
    payload = _build_comments_scrape_run_progress_payload(
        rows=rows,
        platform=normalized_platform,
        account_handle=normalized_account,
    )
    if auto_rebalance_payload:
        payload["auto_rebalance"] = auto_rebalance_payload
    return payload


def resume_social_account_comments_run(
    *,
    platform: str,
    account_handle: str,
    run_id: str,
    initiated_by: str | None = None,
) -> dict[str, Any]:
    normalized_platform = _normalize_social_account_profile_platform(platform)
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    normalized_run_id = str(run_id or "").strip()
    if normalized_platform != "instagram":
        raise SocialIngestValidationError(
            "SOCIAL_ACCOUNT_COMMENTS_UNSUPPORTED_PLATFORM",
            "Standalone comments scraping is currently only supported for Instagram.",
        )
    if not normalized_run_id:
        raise LookupError("Comments scrape run not found.")
    rows = pg.fetch_all(
        """
        select
          r.id::text as run_id,
          r.status as run_status,
          r.source_scope,
          r.initiated_by,
          r.config as run_config,
          j.id::text as job_id,
          j.status as job_status,
          j.items_found,
          j.config,
          j.metadata,
          j.created_at as job_created_at
        from social.scrape_runs r
        join social.scrape_jobs j on j.run_id = r.id
        where r.id = %s::uuid
          and j.platform = %s
          and coalesce(j.config->>'stage', j.metadata->>'stage', j.job_type) = %s
          and ltrim(lower(coalesce(j.config->>'account', j.metadata->>'account', '')), '@') = %s
        order by j.created_at asc, j.id asc
        """,
        [normalized_run_id, normalized_platform, INSTAGRAM_COMMENTS_SCRAPLING_STAGE, normalized_account],
    )
    if not rows:
        raise LookupError("Comments scrape run not found.")
    run_status = str(rows[0].get("run_status") or "").strip().lower()
    if _status_is_active(run_status):
        raise SocialIngestConflictError(
            "SOCIAL_ACCOUNT_COMMENTS_RUN_ALREADY_ACTIVE",
            f"Comments scrape run {normalized_run_id} is still active for @{normalized_account}.",
            detail={
                "run_id": normalized_run_id,
                "platform": normalized_platform,
                "account_handle": normalized_account,
                "status": run_status,
            },
        )

    remaining_target_source_ids: list[str] = []
    original_target_count = 0
    processed_target_count = 0
    for row in rows:
        config = _metadata_dict(row.get("config"))
        metadata = _metadata_dict(row.get("metadata"))
        original_targets = _comments_job_target_source_ids(config=config, metadata=metadata)
        processed_posts = min(
            _comments_job_processed_post_count(row=row, metadata=metadata),
            len(original_targets),
        )
        original_target_count += len(original_targets)
        processed_target_count += processed_posts
        remaining_target_source_ids.extend(
            _comments_job_remaining_target_source_ids(
                row=row,
                config=config,
                metadata=metadata,
            )
        )
    remaining_target_source_ids = list(dict.fromkeys(remaining_target_source_ids))
    if not remaining_target_source_ids:
        return {
            "run_id": None,
            "resumed_from_run_id": normalized_run_id,
            "status": "no_work",
            "accepted": False,
            "remaining_target_source_ids_count": 0,
            "processed_target_source_ids_count": processed_target_count,
            "original_target_source_ids_count": original_target_count,
        }

    run_config = _public_comments_config_overlay(_metadata_dict(rows[0].get("run_config")))
    normalized_mode = str(run_config.get("mode") or "profile").strip().lower() or "profile"
    source_scope = str(rows[0].get("source_scope") or run_config.get("source_scope") or "network").strip() or "network"
    payload = start_social_account_comments_scrape(
        normalized_platform,
        normalized_account,
        mode=normalized_mode,
        source_scope=source_scope,
        source_id=remaining_target_source_ids[0] if normalized_mode == "single_post" else None,
        max_posts=None,
        max_comments_per_post=_normalize_non_negative_int(run_config.get("max_comments_per_post")) or None,
        refresh_policy=str(run_config.get("refresh_policy") or "stale_or_missing"),
        target_filter=run_config.get("target_filter"),
        comments_load_strategy=str(run_config.get("comments_load_strategy") or "public_relay"),
        initiated_by=initiated_by or str(rows[0].get("initiated_by") or "").strip() or "resume_comments_run",
        allow_local_dev_inline_bypass=bool(run_config.get("allow_local_dev_inline_bypass")),
        comments_enable_media_followups=bool(run_config.get("comments_enable_media_followups")),
        launch_group_id=str(run_config.get("launch_group_id") or "").strip() or None,
        target_source_ids=remaining_target_source_ids,
        comments_worker_count=_normalize_non_negative_int(run_config.get("comments_worker_count")) or None,
        comments_target_batch_size=_normalize_non_negative_int(run_config.get("comments_target_batch_size")) or None,
        date_start=(str(run_config.get("date_start")).strip() or None) if run_config.get("date_start") else None,
        date_end=(str(run_config.get("date_end")).strip() or None) if run_config.get("date_end") else None,
    )
    payload.update(
        {
            "resumed_from_run_id": normalized_run_id,
            "remaining_target_source_ids_count": len(remaining_target_source_ids),
            "processed_target_source_ids_count": processed_target_count,
            "original_target_source_ids_count": original_target_count,
            "accepted": True,
        }
    )
    return payload


# Default BRAVOTV public-comments proof window, applied only when the guarded
# restart request explicitly asks for proof defaults and the original run did
# not carry an explicit date window.
_GUARDED_RESTART_PROOF_DATE_START = "2025-01-01T00:00:00+00:00"
_GUARDED_RESTART_PROOF_DATE_END = "2027-01-01T00:00:00+00:00"
# Public-only relaunch shape (REVISED §5 Backend): ramp starts at 12 workers
# with batch-size-10 shards.
_GUARDED_RESTART_WORKER_CAP_START = 12
_GUARDED_RESTART_TARGET_BATCH_SIZE = 10


def _guarded_restart_normalized_date_window(value: Any) -> str | None:
    """Return a normalized ISO date-window value from run config, or None."""
    text = str(value or "").strip()
    return text or None


def _stamp_guarded_restart_audit_on_old_run(
    *,
    run_id: str,
    new_run_id: str,
    cancel_reason: str,
    conn: Any | None = None,
) -> None:
    """Record guarded-restart audit fields on the cancelled run summary.

    ``cancel_social_account_comments_run`` already writes ``cancelled_by`` and
    ``cancel_requested_at`` to the run summary; this layers the guarded-restart
    audit fields on top with an explicit jsonb merge so neither write clobbers
    the other. The protected-field preservation in
    ``control_plane.run_lifecycle`` keeps these intact across summary recompute.
    """
    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        return
    try:
        pg.fetch_one(
            """
            update social.scrape_runs
            set summary = coalesce(summary, '{}'::jsonb) || jsonb_build_object(
              'cancel_reason', %s,
              'guarded_restart', true,
              'guarded_restart_to_run_id', %s
            )
            where id = %s::uuid
            returning id::text
            """,
            [cancel_reason, str(new_run_id or "").strip() or None, normalized_run_id],
            conn=conn,
        )
    except Exception:  # noqa: BLE001
        logger.warning(
            "[comments-guarded-restart] failed to stamp audit fields on old run %s (continuing)",
            normalized_run_id,
            exc_info=True,
        )


def guarded_restart_social_account_comments_run(
    *,
    platform: str,
    account_handle: str,
    run_id: str,
    initiated_by: str | None = None,
    use_proof_defaults: bool = False,
) -> dict[str, Any]:
    """Cancel an active/blocked comments run and relaunch the same public-only window.

    REVISED §5 (Backend): the relaunch is public-relay only (no auth probe, no
    cookies, no proxy/Decodo). The original date window and target filter are
    preserved; the worker cap starts at 12 and shards use batch size 10.
    """
    normalized_platform = _normalize_social_account_profile_platform(platform)
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    normalized_run_id = str(run_id or "").strip()
    if normalized_platform != "instagram":
        raise SocialIngestValidationError(
            "SOCIAL_ACCOUNT_COMMENTS_UNSUPPORTED_PLATFORM",
            "Standalone comments scraping is currently only supported for Instagram.",
        )
    if not normalized_run_id:
        raise LookupError("Comments scrape run not found.")

    lock_key = _social_account_comments_start_lock_key(normalized_platform, normalized_account)
    lock_label = f"comments-guarded-restart-lock:{normalized_platform}:{normalized_account[:48]}"
    with pg.db_connection(label=lock_label, pool_name="social_control") as lock_conn:
        with pg.db_cursor(conn=lock_conn, label=lock_label) as cur:
            lock_row = pg.fetch_one_with_cursor(cur, "select pg_try_advisory_lock(%s) as locked", [lock_key]) or {}
        if not bool(lock_row.get("locked")):
            raise SocialIngestConflictError(
                "SOCIAL_ACCOUNT_COMMENTS_LAUNCH_IN_PROGRESS",
                f"Comments sync is already starting for @{normalized_account}.",
                detail={
                    "platform": normalized_platform,
                    "account_handle": normalized_account,
                    "status": "starting",
                    "retryable": True,
                },
            )
        try:
            run_row = _load_social_account_comments_run_row(
                platform=normalized_platform,
                account_handle=normalized_account,
                run_id=normalized_run_id,
                conn=lock_conn,
            )
            old_run_id = str(run_row.get("id") or "").strip()
            if not old_run_id:
                raise LookupError("Comments scrape run not found.")
            original_config = _public_comments_config_overlay(_metadata_dict(run_row.get("config")))
            normalized_mode = str(original_config.get("mode") or "profile").strip().lower() or "profile"
            source_scope = (
                str(run_row.get("source_scope") or original_config.get("source_scope") or "network").strip()
                or "network"
            )
            original_date_start = _guarded_restart_normalized_date_window(original_config.get("date_start"))
            original_date_end = _guarded_restart_normalized_date_window(original_config.get("date_end"))
            if original_date_start is None and original_date_end is None and use_proof_defaults:
                restart_date_start = _GUARDED_RESTART_PROOF_DATE_START
                restart_date_end = _GUARDED_RESTART_PROOF_DATE_END
                used_proof_defaults = True
            else:
                restart_date_start = original_date_start
                restart_date_end = original_date_end
                used_proof_defaults = False
            original_target_filter = _normalize_instagram_comments_target_filter(
                original_config.get("target_filter")
            )
            restart_target_filter = (
                original_target_filter if original_target_filter is not None else "incomplete"
            ) if normalized_mode == "profile" else None

            cancellation_summary = cancel_social_account_comments_run(
                platform=normalized_platform,
                account_handle=normalized_account,
                run_id=old_run_id,
                cancelled_by="comments_guarded_restart",
                conn=lock_conn,
            )

            new_run_payload = start_social_account_comments_scrape(
                normalized_platform,
                normalized_account,
                mode=normalized_mode,
                source_scope=source_scope,
                source_id=str(original_config.get("source_id") or "").strip() or None,
                max_posts=_normalize_non_negative_int(original_config.get("max_posts")) or None,
                max_comments_per_post=_normalize_non_negative_int(original_config.get("max_comments_per_post"))
                or None,
                refresh_policy=str(original_config.get("refresh_policy") or "stale_or_missing"),
                target_filter=restart_target_filter,
                comments_load_strategy="public_relay",
                initiated_by=initiated_by or "comments_guarded_restart",
                allow_local_dev_inline_bypass=bool(original_config.get("allow_local_dev_inline_bypass")),
                comments_enable_media_followups=bool(original_config.get("comments_enable_media_followups")),
                launch_group_id=str(original_config.get("launch_group_id") or "").strip() or None,
                skip_launch_auth_probe=True,
                comments_worker_count=_GUARDED_RESTART_WORKER_CAP_START,
                comments_target_batch_size=_GUARDED_RESTART_TARGET_BATCH_SIZE,
                cancel_active_before_relaunch=False,
                date_start=restart_date_start,
                date_end=restart_date_end,
            )
            new_run_id = str(new_run_payload.get("run_id") or "").strip()

            _stamp_guarded_restart_audit_on_old_run(
                run_id=old_run_id,
                new_run_id=new_run_id,
                cancel_reason="public_comments_guarded_restart",
                conn=lock_conn,
            )
        finally:
            try:
                with pg.db_cursor(conn=lock_conn, label=lock_label) as cur:
                    pg.fetch_one_with_cursor(cur, "select pg_advisory_unlock(%s) as unlocked", [lock_key])
            except Exception:  # noqa: BLE001
                logger.debug(
                    "[comments-guarded-restart-lock] advisory unlock failed for %s/%s",
                    normalized_platform,
                    normalized_account,
                    exc_info=True,
                )

    public_only_proof = {
        "no_cookies": True,
        "no_proxy": True,
        "comments_load_strategy": "public_relay",
    }
    instagram_access_proof = _metadata_dict(new_run_payload.get("instagram_access_proof"))
    if instagram_access_proof:
        public_only_proof["instagram_access_proof"] = instagram_access_proof

    return {
        "accepted": True,
        "old_run_id": old_run_id,
        "new_run_id": new_run_id or None,
        "platform": normalized_platform,
        "account_handle": normalized_account,
        "public_only_proof": public_only_proof,
        "comments_load_strategy": "public_relay",
        "date_window": {
            "date_start": restart_date_start,
            "date_end": restart_date_end,
            "end_exclusive": True,
            "used_proof_defaults": used_proof_defaults,
        },
        "target_filter": restart_target_filter,
        "comments_worker_cap_start": _GUARDED_RESTART_WORKER_CAP_START,
        "comments_target_batch_size": _GUARDED_RESTART_TARGET_BATCH_SIZE,
        "cancellation_summary": cancellation_summary,
        "new_run": new_run_payload,
    }


def cancel_social_account_comments_run(
    *,
    platform: str,
    account_handle: str,
    run_id: str,
    cancelled_by: str | None = None,
    conn: Any | None = None,
) -> dict[str, Any]:
    run_row = _load_social_account_comments_run_row(
        platform=platform,
        account_handle=account_handle,
        run_id=run_id,
        conn=conn,
    )
    normalized_run_id = str(run_row.get("id") or "").strip()
    if not normalized_run_id:
        raise LookupError("Comments run not found.")
    cancel_requested_at = _now_utc()
    pg.fetch_one(
        """
        update social.scrape_runs
        set
          status = 'cancelled',
          cancelled_at = now(),
          completed_at = now(),
          summary = coalesce(summary, '{}'::jsonb) || jsonb_build_object(
            'cancelled_by', %s,
            'cancel_requested_at', %s
          )
        where id = %s::uuid
        returning id::text
        """,
        [cancelled_by, _iso(cancel_requested_at), normalized_run_id],
        conn=conn,
    )
    cancelled_jobs = pg.execute_returning(
        """
        update social.scrape_jobs
        set
          status = 'cancelled',
          completed_at = now(),
          error_message = coalesce(error_message, 'Cancelled by user request'),
          metadata = coalesce(metadata, '{}'::jsonb) || jsonb_build_object(
            'cancel_reason', 'comments_run_cancelled_by_admin',
            'cancelled_by', %s,
            'cancelled_at', %s
          )
        where run_id = %s::uuid
          and status in ('queued', 'pending', 'retrying', 'running', 'cancelling')
        returning id::text as id
        """,
        [cancelled_by, _iso(cancel_requested_at), normalized_run_id],
        conn=conn,
    )
    cancelled_job_ids = [str(row.get("id") or "").strip() for row in cancelled_jobs if str(row.get("id") or "").strip()]
    for job_id in cancelled_job_ids:
        _clear_worker_heartbeat_for_job(
            job_id=job_id,
            status="idle",
            metadata={"source": "cancel_social_account_comments_run", "job_status": "cancelled"},
        )
    summary = _update_run_summary(normalized_run_id, force_recompute=True, conn=conn)
    _invalidate_queue_status_cache()
    return {
        "run_id": normalized_run_id,
        "status": "cancelled",
        "accepted": True,
        "cancel_requested_at": _iso(cancel_requested_at),
        "cancelled_jobs": len(cancelled_job_ids),
        "cancelled_job_ids": cancelled_job_ids,
        "summary": summary,
    }


def cancel_social_account_comments_job(
    *,
    platform: str,
    account_handle: str,
    run_id: str,
    job_id: str,
    cancelled_by: str | None = None,
    conn: Any | None = None,
) -> dict[str, Any]:
    run_row = _load_social_account_comments_run_row(
        platform=platform,
        account_handle=account_handle,
        run_id=run_id,
        conn=conn,
    )
    normalized_run_id = str(run_row.get("id") or "").strip()
    normalized_job_id = str(job_id or "").strip()
    if not normalized_run_id or not normalized_job_id:
        raise LookupError("Comments job not found.")
    normalized_platform = _normalize_social_account_profile_platform(platform)
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    cancel_requested_at = _now_utc()
    cancelled_job = pg.fetch_one(
        """
        update social.scrape_jobs
        set
          status = 'cancelled',
          completed_at = now(),
          error_message = coalesce(error_message, 'Cancelled by user request'),
          metadata = coalesce(metadata, '{}'::jsonb) || jsonb_build_object(
            'cancel_reason', 'comments_shard_cancelled_by_admin',
            'cancelled_by', %s,
            'cancelled_at', %s
          )
        where id = %s::uuid
          and run_id = %s::uuid
          and platform = %s
          and coalesce(config->>'stage', metadata->>'stage', job_type) = %s
          and ltrim(lower(coalesce(config->>'account', metadata->>'account', '')), '@') = %s
          and status in ('queued', 'pending', 'retrying', 'running', 'cancelling')
        returning id::text as id, status
        """,
        [
            cancelled_by,
            _iso(cancel_requested_at),
            normalized_job_id,
            normalized_run_id,
            normalized_platform,
            INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
            normalized_account,
        ],
        conn=conn,
    )
    if not cancelled_job:
        raise LookupError("Active comments job not found.")
    _clear_worker_heartbeat_for_job(
        job_id=normalized_job_id,
        status="idle",
        metadata={"source": "cancel_social_account_comments_job", "job_status": "cancelled"},
    )
    _update_run_summary(normalized_run_id, force_recompute=True, conn=conn)
    _invalidate_queue_status_cache()
    return {
        "run_id": normalized_run_id,
        "job_id": str(cancelled_job.get("id") or normalized_job_id),
        "status": "cancelled",
        "accepted": True,
        "cancel_requested_at": _iso(cancel_requested_at),
    }


_LOCAL_ROOM_NAMES = {
    "_instagram_comments_stale_after_hours",
    "_instagram_comments_target_count_expr",
    "_instagram_social_account_comments_coverage_diagnostics",
    "get_social_account_comments_coverage_diagnostics",
    "_instagram_comments_target_priority",
    "_normalize_instagram_comments_target_filter",
    "_instagram_comments_target_preview_cache_ttl_seconds",
    "_get_instagram_comments_target_preview_cache",
    "_set_instagram_comments_target_preview_cache",
    "_instagram_social_account_comment_target_preview",
    "INSTAGRAM_COMMENTS_AUDIT_CURSOR_RETRY_STOP_REASONS",
    "_normalize_instagram_comments_audit_retry_stop_reasons",
    "_normalize_instagram_comments_audit_retry_shortcodes",
    "_load_instagram_comments_audit_cursor_rows",
    "_select_instagram_comments_audit_cursor_retry_targets",
    "get_instagram_comments_audit_cursor_recovery",
    "enqueue_instagram_comments_audit_cursor_retries",
    "_instagram_social_account_comments_target_counts",
    "get_active_social_account_comments_run",
    "_instagram_social_account_comment_target_shortcodes",
    "_instagram_social_account_incomplete_comment_target_shortcodes",
    "_instagram_comments_reported_gap_is_tolerable",
    "_instagram_filter_incomplete_comment_targets",
    "_instagram_comments_profile_shard_count",
    "_instagram_comments_job_max_attempts",
    "_instagram_comments_recommended_shard_count",
    "_chunk_instagram_comment_targets",
    "_instagram_comments_launch_auth_check_enabled",
    "_comments_launch_auth_metadata",
    "_public_comments_launch_auth_metadata",
    "_probe_instagram_comments_endpoint_for_launch",
    "_ensure_instagram_comments_auth_ready_for_launch",
    "start_social_account_comments_scrape",
    "preview_social_account_comments_scrape",
    "rebalance_slow_instagram_comments_shards",
    "rebalance_failed_instagram_comments_shard",
    "rebalance_waiting_instagram_comments_shards",
    "repair_instagram_comments_scrape_run_target_gaps",
    "_build_comments_scrape_run_progress_payload",
    "get_social_account_comments_scrape_run_progress",
    "resume_social_account_comments_run",
    "guarded_restart_social_account_comments_run",
    "cancel_social_account_comments_run",
    "cancel_social_account_comments_job",
}
_LOCAL_ROOM_FUNCTIONS = {_name: globals()[_name] for _name in _LOCAL_ROOM_NAMES}
_CORE_ROOM_WRAPPERS = {_name: getattr(_core, _name, None) for _name in _LOCAL_ROOM_NAMES}
__all__ = [
    "_instagram_comments_stale_after_hours",
    "_instagram_comments_target_count_expr",
    "_instagram_social_account_comments_coverage_diagnostics",
    "get_social_account_comments_coverage_diagnostics",
    "_instagram_comments_target_priority",
    "_normalize_instagram_comments_target_filter",
    "_instagram_comments_target_preview_cache_ttl_seconds",
    "_get_instagram_comments_target_preview_cache",
    "_set_instagram_comments_target_preview_cache",
    "_instagram_social_account_comment_target_preview",
    "INSTAGRAM_COMMENTS_AUDIT_CURSOR_RETRY_STOP_REASONS",
    "_normalize_instagram_comments_audit_retry_stop_reasons",
    "_normalize_instagram_comments_audit_retry_shortcodes",
    "_load_instagram_comments_audit_cursor_rows",
    "_select_instagram_comments_audit_cursor_retry_targets",
    "get_instagram_comments_audit_cursor_recovery",
    "enqueue_instagram_comments_audit_cursor_retries",
    "_instagram_social_account_comments_target_counts",
    "get_active_social_account_comments_run",
    "_instagram_social_account_comment_target_shortcodes",
    "_instagram_social_account_incomplete_comment_target_shortcodes",
    "_instagram_comments_reported_gap_is_tolerable",
    "_instagram_filter_incomplete_comment_targets",
    "_instagram_comments_profile_shard_count",
    "_instagram_comments_job_max_attempts",
    "_instagram_comments_recommended_shard_count",
    "_chunk_instagram_comment_targets",
    "_instagram_comments_launch_auth_check_enabled",
    "_comments_launch_auth_metadata",
    "_public_comments_launch_auth_metadata",
    "_probe_instagram_comments_endpoint_for_launch",
    "_ensure_instagram_comments_auth_ready_for_launch",
    "start_social_account_comments_scrape",
    "preview_social_account_comments_scrape",
    "rebalance_slow_instagram_comments_shards",
    "rebalance_failed_instagram_comments_shard",
    "rebalance_waiting_instagram_comments_shards",
    "repair_instagram_comments_scrape_run_target_gaps",
    "_build_comments_scrape_run_progress_payload",
    "get_social_account_comments_scrape_run_progress",
    "resume_social_account_comments_run",
    "guarded_restart_social_account_comments_run",
    "cancel_social_account_comments_run",
    "cancel_social_account_comments_job",
]
