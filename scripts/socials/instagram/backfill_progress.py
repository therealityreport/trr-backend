#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

try:
    from trr_backend.db import pg
    from trr_backend.utils.env import load_env
except ModuleNotFoundError:  # pragma: no cover - script execution convenience
    repo_root = Path(__file__).resolve().parents[3]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from trr_backend.db import pg
    from trr_backend.utils.env import load_env


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_safe(item) for item in value]
    return str(value)


def _metadata(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _clean_handle(value: Any) -> str | None:
    text = str(value or "").strip().lower().lstrip("@")
    return text or None


def _fetch_run(run_id: str) -> dict[str, Any]:
    row = pg.fetch_one(
        """
        select
          id::text as id,
          status,
          total_jobs,
          completed_jobs,
          failed_jobs,
          active_jobs,
          items_found_total,
          config,
          summary,
          created_at,
          started_at,
          completed_at,
          cancelled_at,
          now() as snapshot_at
        from social.scrape_runs
        where id = %s::uuid
        """,
        [run_id],
    )
    if not row:
        raise ValueError(f"social.scrape_runs row not found for run_id={run_id}")
    return row


def _fetch_frontiers(run_id: str) -> list[dict[str, Any]]:
    return pg.fetch_all(
        """
        select
          id::text as id,
          platform,
          account_handle,
          strategy,
          status,
          next_cursor is not null as next_cursor_present,
          total_posts,
          posts_checked,
          posts_saved,
          pages_scanned,
          last_transport,
          lease_owner,
          lease_expires_at,
          retry_count,
          exhausted,
          metadata,
          updated_at,
          extract(epoch from (now() - updated_at))::int as updated_age_seconds,
          case
            when lease_expires_at is not null then extract(epoch from (lease_expires_at - now()))::int
            else null
          end as lease_expires_in_seconds
        from social.shared_account_run_frontiers
        where run_id = %s::uuid
        order by updated_at desc
        """,
        [run_id],
    )


def _fetch_catalog_counts(run_id: str) -> dict[str, Any]:
    return pg.fetch_one(
        """
        select
          count(*)::int as rows,
          count(distinct source_id)::int as distinct_source_ids,
          (count(*) - count(distinct source_id))::int as duplicate_source_ids,
          count(*) filter (where posted_at is null)::int as posted_at_null,
          min(posted_at) as oldest_posted_at,
          max(posted_at) as newest_posted_at
        from social.instagram_account_catalog_posts
        where last_backfill_run_id = %s::uuid
        """,
        [run_id],
    ) or {}


def _fetch_detail_counts(run_id: str) -> dict[str, Any]:
    return pg.fetch_one(
        """
        with run_catalog as (
          select
            source_id,
            max(coalesce(likes, 0)) as likes,
            max(coalesce(comments_count, 0)) as comments_count,
            max(coalesce(shares, 0)) as shares,
            max(coalesce(views, 0)) as views
          from social.instagram_account_catalog_posts
          where last_backfill_run_id = %s::uuid
          group by source_id
        )
        select
          count(*)::int as catalog_rows,
          count(ip.id)::int as instagram_posts_rows,
          count(sp.id)::int as canonical_social_posts_rows,
          count(*) filter (where ip.id is null)::int as missing_instagram_posts_rows,
          count(*) filter (where sp.id is null)::int as missing_canonical_social_posts_rows,
          count(*) filter (
            where ip.id is not null
              and (
                nullif(ip.hosted_thumbnail_url, '') is not null
                or coalesce(jsonb_array_length(
                  case
                    when jsonb_typeof(to_jsonb(ip.hosted_media_urls)) = 'array'
                    then to_jsonb(ip.hosted_media_urls)
                    else '[]'::jsonb
                  end
                ), 0) > 0
              )
          )::int as hosted_media_linked_rows,
          count(*) filter (
            where sp.id is not null
              and (
                (run_catalog.likes > 0 and coalesce(sp.like_count, -1) < run_catalog.likes)
                or (run_catalog.comments_count > 0 and coalesce(sp.comment_count, -1) < run_catalog.comments_count)
                or (
                  greatest(coalesce(run_catalog.shares, 0), coalesce(ip.media_repost_count, 0)) > 0
                  and coalesce(sp.share_count, -1)
                    < greatest(coalesce(run_catalog.shares, 0), coalesce(ip.media_repost_count, 0))
                )
                or (run_catalog.views > 0 and coalesce(sp.view_count, -1) < run_catalog.views)
              )
          )::int as canonical_metric_mismatch_rows
        from run_catalog
        left join social.instagram_posts ip on ip.shortcode = run_catalog.source_id
        left join social.social_posts sp on sp.platform = 'instagram' and sp.source_id = run_catalog.source_id
        """,
        [run_id],
    ) or {}


def _fetch_comment_counts(run_id: str) -> dict[str, Any]:
    sample_limit = min(_env_int("SOCIAL_INSTAGRAM_PROGRESS_COMMENT_SAMPLE_LIMIT", 250), 1000)
    return pg.fetch_one(
        """
        with run_catalog as (
          select distinct source_id
          from social.instagram_account_catalog_posts
          where last_backfill_run_id = %s::uuid
        ),
        post_details as materialized (
          select ip.id, ip.comments_count, ip.posted_at
          from run_catalog
          join social.instagram_posts ip on ip.shortcode = run_catalog.source_id
        ),
        reported_totals as (
          select
            coalesce(sum(coalesce(comments_count, 0)), 0)::bigint as reported_from_post_details,
            count(*) filter (where coalesce(comments_count, 0) > 0)::int as posts_reporting_comments
          from post_details
        ),
        rollup_totals as (
          select
            coalesce(sum(coalesce(r.total_comment_count, 0)), 0)::bigint as rows,
            count(*) filter (where coalesce(r.total_comment_count, 0) > 0)::int as posts_with_comments,
            count(*) filter (
              where coalesce(pd.comments_count, 0) > 0
                and coalesce(r.total_comment_count, 0) = 0
            )::int as posts_reporting_comments_without_saved_comments,
            count(r.post_id)::int as post_comment_rollup_rows
          from post_details pd
          left join social.instagram_post_comment_rollups r on r.post_id = pd.id
        ),
        sample_posts as (
          select id
          from post_details
          where coalesce(comments_count, 0) > 0
          order by posted_at desc nulls last, comments_count desc nulls last, id
          limit %s
        ),
        comment_media_sample as (
          select
            count(*) filter (where c.media_mirror_status = 'pending')::int as comment_media_pending,
            count(*) filter (where c.media_mirror_status in ('mirrored', 'complete', 'completed'))::int
              as comment_media_mirrored,
            count(*) filter (where c.media_mirror_status = 'failed')::int as comment_media_failed
          from sample_posts sp
          join social.instagram_comments c on c.post_id = sp.id
        )
        select
          reported_totals.reported_from_post_details,
          rollup_totals.rows,
          rollup_totals.posts_with_comments,
          reported_totals.posts_reporting_comments,
          rollup_totals.posts_reporting_comments_without_saved_comments,
          rollup_totals.post_comment_rollup_rows,
          comment_media_sample.comment_media_pending,
          comment_media_sample.comment_media_mirrored,
          comment_media_sample.comment_media_failed,
          (select count(*)::int from sample_posts) as comment_media_sample_posts_checked,
          %s::int as comment_media_sample_limit,
          'instagram_post_comment_rollups'::text as saved_counts_source,
          'bounded_recent_reporting_posts'::text as comment_media_counts_source,
          false as rows_bounded,
          false as posts_with_comments_bounded,
          false as posts_reporting_comments_without_saved_comments_bounded,
          true as comment_media_counts_bounded
        from reported_totals
        cross join rollup_totals
        cross join comment_media_sample
        """,
        [run_id, sample_limit, sample_limit],
    ) or {}


def _fetch_media_status_counts(run_id: str) -> list[dict[str, Any]]:
    return pg.fetch_all(
        """
        with run_catalog as (
          select distinct source_id
          from social.instagram_account_catalog_posts
          where last_backfill_run_id = %s::uuid
        )
        select
          case
            when nullif(ip.media_mirror_status, '') is null
              and (
                nullif(ip.hosted_thumbnail_url, '') is not null
                or coalesce(jsonb_array_length(
                  case
                    when jsonb_typeof(to_jsonb(ip.hosted_media_urls)) = 'array'
                    then to_jsonb(ip.hosted_media_urls)
                    else '[]'::jsonb
                  end
                ), 0) > 0
              )
            then 'hosted_unmarked'
            else coalesce(nullif(ip.media_mirror_status, ''), 'unknown')
          end as status,
          count(*)::int as rows
        from run_catalog
        join social.instagram_posts ip on ip.shortcode = run_catalog.source_id
        group by 1
        order by status
        """,
        [run_id],
    )


def _fetch_job_counts(run_id: str) -> list[dict[str, Any]]:
    return pg.fetch_all(
        """
        select
          coalesce(config->>'stage', metadata->>'stage', job_type, 'unknown') as stage,
          status,
          count(*)::int as jobs,
          min(available_at) as next_available_at,
          max(last_error_code) filter (where last_error_code is not null) as sample_last_error_code
        from social.scrape_jobs
        where run_id = %s::uuid
        group by coalesce(config->>'stage', metadata->>'stage', job_type, 'unknown'), status
        order by stage, status
        """,
        [run_id],
    )


def _fetch_comments_recovery_summary(run_id: str) -> dict[str, Any]:
    row = pg.fetch_one(
        """
        with comments_jobs as (
          select
            status,
            last_error_code,
            case
              when jsonb_typeof(coalesce(config->'target_source_ids', metadata->'target_source_ids')) = 'array'
              then jsonb_array_length(coalesce(config->'target_source_ids', metadata->'target_source_ids'))
              when nullif(coalesce(config->>'shortcode', metadata->>'shortcode', ''), '') is not null
              then 1
              else 0
            end as target_count
          from social.scrape_jobs
          where run_id = %s::uuid
            and platform = 'instagram'
            and coalesce(config->>'stage', metadata->>'stage', job_type, 'unknown') = 'comments_scrapling'
        )
        select
          count(*) filter (where status = 'queued')::int as queued_jobs,
          count(*) filter (where status = 'pending')::int as pending_jobs,
          count(*) filter (where status = 'retrying')::int as retrying_jobs,
          count(*) filter (where status = 'running')::int as running_jobs,
          count(*) filter (where status = 'completed')::int as completed_jobs,
          count(*) filter (where status = 'failed')::int as failed_jobs,
          count(*) filter (
            where status in ('failed', 'retrying')
              and lower(coalesce(last_error_code, '')) in (
                'instagram_comments_public_recovery_pending',
                'instagram_comments_public_requires_approval'
              )
          )::int as public_recovery_jobs,
          coalesce(sum(target_count) filter (
            where status in ('failed', 'retrying')
              and lower(coalesce(last_error_code, '')) in (
                'instagram_comments_public_recovery_pending',
                'instagram_comments_public_requires_approval'
              )
          ), 0)::int as public_recovery_target_source_ids_count,
          count(*) filter (
            where status in ('failed', 'retrying')
              and lower(coalesce(last_error_code, '')) in (
                'instagram_comments_endpoint_auth_blocked',
                'instagram_comments_auth_failed',
                'instagram_comments_browser_session_invalidated',
                'instagram_comments_warmup_auth_failed',
                'instagram_comments_warmup_no_cookies',
                'checkpoint_required'
              )
          )::int as authenticated_followup_jobs,
          coalesce(sum(target_count) filter (
            where status in ('failed', 'retrying')
              and lower(coalesce(last_error_code, '')) in (
                'instagram_comments_endpoint_auth_blocked',
                'instagram_comments_auth_failed',
                'instagram_comments_browser_session_invalidated',
                'instagram_comments_warmup_auth_failed',
                'instagram_comments_warmup_no_cookies',
                'checkpoint_required'
              )
          ), 0)::int as authenticated_followup_target_source_ids_count
        from comments_jobs
        """,
        [run_id],
    ) or {}
    public_targets = _fmt_count(row.get("public_recovery_target_source_ids_count"))
    auth_targets = _fmt_count(row.get("authenticated_followup_target_source_ids_count"))
    return {
        **row,
        "public_recovery_bucket": {
            "name": "public_recovery",
            "source_error_codes": [
                "instagram_comments_public_recovery_pending",
                "instagram_comments_public_requires_approval",
            ],
            "target_load_strategy": "public_relay",
            "target_scrape_mode": "public_first",
            "target_auth_validation_mode": "public_relay",
            "target_source_ids_count": public_targets,
            "source_job_count": _fmt_count(row.get("public_recovery_jobs")),
            "status": "ready" if public_targets else "empty",
        },
        "authenticated_followup_bucket": {
            "name": "authenticated_followup",
            "source_error_codes": [
                "instagram_comments_endpoint_auth_blocked",
                "instagram_comments_auth_failed",
                "instagram_comments_browser_session_invalidated",
                "instagram_comments_warmup_auth_failed",
                "instagram_comments_warmup_no_cookies",
                "checkpoint_required",
            ],
            "target_load_strategy": "instagram_comments_endpoint_cursor",
            "target_scrape_mode": "authenticated",
            "target_auth_validation_mode": "comments_endpoint",
            "target_source_ids_count": auth_targets,
            "source_job_count": _fmt_count(row.get("authenticated_followup_jobs")),
            "status": "ready" if auth_targets else "empty",
        },
    }


def _fetch_media_completion_rates(run_id: str) -> dict[str, Any]:
    return pg.fetch_one(
        """
        select
          count(*) filter (
            where coalesce(config->>'stage', metadata->>'stage', job_type, 'unknown') = 'media_mirror'
              and status = 'completed'
              and completed_at >= now() - interval '15 minutes'
          )::int as media_completed_15m,
          count(*) filter (
            where coalesce(config->>'stage', metadata->>'stage', job_type, 'unknown') = 'media_mirror'
              and status = 'completed'
              and completed_at >= now() - interval '1 hour'
          )::int as media_completed_1h,
          count(*) filter (
            where coalesce(config->>'stage', metadata->>'stage', job_type, 'unknown') = 'media_mirror'
              and status = 'completed'
              and completed_at >= now() - interval '6 hours'
          )::int as media_completed_6h,
          max(completed_at) filter (
            where coalesce(config->>'stage', metadata->>'stage', job_type, 'unknown') = 'media_mirror'
              and status = 'completed'
          ) as latest_media_completed_at
        from social.scrape_jobs
        where run_id = %s::uuid
        """,
        [run_id],
    ) or {}


def _fetch_latest_jobs(run_id: str) -> list[dict[str, Any]]:
    return pg.fetch_all(
        """
        select
          id::text as id,
          coalesce(config->>'stage', metadata->>'stage', job_type, 'unknown') as stage,
          status,
          attempt_count,
          max_attempts,
          available_at,
          worker_id,
          claimed_at,
          heartbeat_at,
          extract(epoch from (now() - heartbeat_at))::int as heartbeat_age_seconds,
          case
            when nullif(metadata #>> '{activity,last_progress_at}', '') is not null
            then extract(epoch from (now() - (metadata #>> '{activity,last_progress_at}')::timestamptz))::int
            else null
          end as progress_age_seconds,
          last_error_code,
          last_error_class,
          error_message,
          metadata->'auth_cooldown' as auth_cooldown,
          metadata->'activity' as activity,
          metadata->'dispatch' as dispatch,
          metadata->'retrieval_meta' as retrieval_meta
        from social.scrape_jobs
        where run_id = %s::uuid
        order by coalesce(completed_at, heartbeat_at, claimed_at, available_at, created_at) desc
        limit 5
        """,
        [run_id],
    )


def _fetch_auth_cooldown(platform: str | None, account_handle: str | None) -> dict[str, Any] | None:
    if not platform or not account_handle:
        return None
    return pg.fetch_one(
        """
        select
          platform,
          account_handle,
          blocker_kind,
          null::text as blocked_reason,
          consecutive_auth_failures as consecutive_failures,
          cooldown_until,
          last_error_code,
          updated_at as last_blocked_at,
          updated_at
        from social.account_auth_cooldown
        where platform = %s
          and account_handle = %s
        order by updated_at desc
        limit 1
        """,
        [platform, account_handle],
    )


def _comments_followup_payload(summary: dict[str, Any], config: dict[str, Any]) -> dict[str, Any] | None:
    deferred = _metadata(config.get("deferred_comments_followup"))
    attached = _metadata(_metadata(summary.get("attached_followups")).get("comments"))
    config_attached = _metadata(_metadata(config.get("attached_followups")).get("comments"))
    payload = (
        config_attached
        or deferred
        or attached
        or _metadata(summary.get("comments_followup"))
    )
    if not payload:
        return None
    payload = dict(payload)
    if deferred:
        deferred_state = str(deferred.get("state") or "").strip().lower()
        state = str(payload.get("state") or deferred_state or "pending").strip().lower() or "pending"
        payload.setdefault("state", state)
        payload.setdefault("status", "deferred" if deferred_state == "pending" else state)
        payload.setdefault("source", "deferred_after_catalog")
        payload.setdefault("deferred_state", deferred_state or None)
        if deferred_state == "pending":
            payload.setdefault("deferred_until", "catalog_complete")
            payload.setdefault("pending_reason", "waiting_for_catalog_completion")
        else:
            payload.pop("deferred_until", None)
            payload.pop("pending_reason", None)
        payload.setdefault("platform", deferred.get("platform"))
        payload.setdefault("account_handle", deferred.get("account_handle"))
        payload.setdefault("source_scope", deferred.get("source_scope"))
    return payload


def build_progress(run_id: str) -> dict[str, Any]:
    run = _fetch_run(run_id)
    config = _metadata(run.get("config"))
    summary = _metadata(run.get("summary"))
    frontiers = _fetch_frontiers(run_id)
    platform = str(config.get("platform") or (frontiers[0].get("platform") if frontiers else "instagram")).strip()
    account = (
        _clean_handle(config.get("account"))
        or _clean_handle(config.get("account_handle"))
        or _clean_handle(frontiers[0].get("account_handle") if frontiers else None)
    )
    progress = {
        "run": run,
        "catalog": _fetch_catalog_counts(run_id),
        "details": _fetch_detail_counts(run_id),
        "comments": _fetch_comment_counts(run_id),
        "media_mirror_status_counts": _fetch_media_status_counts(run_id),
        "frontiers": frontiers,
        "job_counts": _fetch_job_counts(run_id),
        "media_completion_rates": _fetch_media_completion_rates(run_id),
        "latest_jobs": _fetch_latest_jobs(run_id),
        "auth_cooldown": _fetch_auth_cooldown(platform or "instagram", account),
        "comments_followup": _comments_followup_payload(summary, config),
        "comments_recovery_summary": _fetch_comments_recovery_summary(run_id),
        "dispatch_control": summary.get("dispatch_control") or config.get("dispatch_control"),
    }
    progress["speed"] = _speed_summary(progress)
    progress["alerts"] = _build_alerts(progress)
    return progress


def _fmt_count(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _fmt_seconds(value: Any) -> str:
    seconds = _fmt_count(value)
    if seconds <= 0:
        return "0s"
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    parts: list[str] = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if seconds or not parts:
        parts.append(f"{seconds}s")
    return "".join(parts)


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _elapsed_seconds(start: Any, end: Any) -> float | None:
    start_dt = _parse_datetime(start)
    end_dt = _parse_datetime(end)
    if not start_dt or not end_dt:
        return None
    seconds = (end_dt - start_dt).total_seconds()
    return seconds if seconds > 0 else None


def _fmt_rate(value: float | None, suffix: str) -> str:
    if value is None:
        return f"unknown{suffix}"
    if value >= 100:
        text = f"{value:.0f}"
    elif value >= 10:
        text = f"{value:.1f}"
    else:
        text = f"{value:.2f}"
    return f"{text}{suffix}"


def _job_count(job_counts: list[dict[str, Any]], stage: str, status: str) -> int:
    for row in job_counts:
        if str(row.get("stage") or "") == stage and str(row.get("status") or "") == status:
            return _fmt_count(row.get("jobs"))
    return 0


def _speed_summary(progress: dict[str, Any]) -> dict[str, Any]:
    run = _metadata(progress.get("run"))
    frontiers = list(progress.get("frontiers") or [])
    frontier = _metadata(frontiers[0] if frontiers else {})
    job_counts = list(progress.get("job_counts") or [])
    media_rates = _metadata(progress.get("media_completion_rates"))
    elapsed = _elapsed_seconds(run.get("started_at"), run.get("snapshot_at"))
    posts_saved = _fmt_count(frontier.get("posts_saved"))
    pages_scanned = _fmt_count(frontier.get("pages_scanned"))
    posts_per_minute = (posts_saved / (elapsed / 60)) if elapsed else None
    pages_per_hour = (pages_scanned / (elapsed / 3600)) if elapsed else None
    media_completed_1h = _fmt_count(media_rates.get("media_completed_1h"))
    media_per_hour = float(media_completed_1h) if media_completed_1h > 0 else None
    media_queued = _job_count(job_counts, "media_mirror", "queued")
    media_running = _job_count(job_counts, "media_mirror", "running")
    media_eta_seconds = (media_queued / media_per_hour * 3600) if media_per_hour else None
    if media_eta_seconds is not None:
        media_eta_seconds = max(0, int(media_eta_seconds))
    return {
        "account_handle": frontier.get("account_handle"),
        "posts_per_minute": posts_per_minute,
        "pages_per_hour": pages_per_hour,
        "media_per_hour": media_per_hour,
        "media_completed_15m": _fmt_count(media_rates.get("media_completed_15m")),
        "media_completed_1h": media_completed_1h,
        "media_completed_6h": _fmt_count(media_rates.get("media_completed_6h")),
        "latest_media_completed_at": media_rates.get("latest_media_completed_at"),
        "media_queued": media_queued,
        "media_running": media_running,
        "media_eta_seconds": media_eta_seconds,
        "last_page_completed_at": frontier.get("updated_at"),
    }


def _env_int(name: str, default: int) -> int:
    try:
        return max(0, int(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        return default


def _build_alerts(progress: dict[str, Any]) -> list[dict[str, Any]]:
    frontier_threshold = _env_int("SOCIAL_INSTAGRAM_FRONTIER_MOVEMENT_ALERT_SECONDS", 600)
    heartbeat_threshold = _env_int("SOCIAL_INSTAGRAM_BACKFILL_HEARTBEAT_ALERT_SECONDS", 900)
    alerts: list[dict[str, Any]] = []
    for frontier in progress.get("frontiers") or []:
        metadata = _metadata(frontier.get("metadata"))
        updated_age = _fmt_count(frontier.get("updated_age_seconds"))
        if (
            str(frontier.get("status") or "").strip().lower() == "running"
            and frontier.get("next_cursor_present")
            and not frontier.get("exhausted")
            and updated_age >= frontier_threshold
        ):
            alerts.append(
                {
                    "code": "frontier_page_no_recent_movement",
                    "account_handle": frontier.get("account_handle"),
                    "pages_scanned": _fmt_count(frontier.get("pages_scanned")),
                    "posts_saved": _fmt_count(frontier.get("posts_saved")),
                    "oldest_seen": metadata.get("oldest_posted_at_seen"),
                    "age_seconds": updated_age,
                    "threshold_seconds": frontier_threshold,
                }
            )
    for job in progress.get("latest_jobs") or []:
        stage = str(job.get("stage") or "").strip().lower()
        status = str(job.get("status") or "").strip().lower()
        heartbeat_age = _fmt_count(job.get("heartbeat_age_seconds"))
        if stage == "shared_account_posts" and status == "running" and heartbeat_age >= heartbeat_threshold:
            activity = _metadata(job.get("activity"))
            alerts.append(
                {
                    "code": "shared_posts_worker_heartbeat_stale",
                    "job_id": job.get("id"),
                    "worker_id": job.get("worker_id"),
                    "heartbeat_age_seconds": heartbeat_age,
                    "threshold_seconds": heartbeat_threshold,
                    "pages_scanned": _fmt_count(activity.get("pages_scanned")),
                    "posts_saved": _fmt_count(activity.get("saved_posts")),
                }
            )
    return alerts


def print_compact(progress: dict[str, Any]) -> None:
    run = _metadata(progress.get("run"))
    catalog = _metadata(progress.get("catalog"))
    details = _metadata(progress.get("details"))
    comments = _metadata(progress.get("comments"))
    frontiers = list(progress.get("frontiers") or [])
    latest_jobs = list(progress.get("latest_jobs") or [])
    cooldown = _metadata(progress.get("auth_cooldown"))
    comments_followup = _metadata(progress.get("comments_followup"))
    comments_recovery = _metadata(progress.get("comments_recovery_summary"))
    public_recovery_bucket = _metadata(comments_recovery.get("public_recovery_bucket"))
    authenticated_bucket = _metadata(comments_recovery.get("authenticated_followup_bucket"))
    dispatch_control = _metadata(progress.get("dispatch_control"))
    speed = _speed_summary(progress)

    print(
        "run={id} status={status} jobs={completed}/{total} active={active} failed={failed} items={items}".format(
            id=run.get("id"),
            status=run.get("status"),
            completed=_fmt_count(run.get("completed_jobs")),
            total=_fmt_count(run.get("total_jobs")),
            active=_fmt_count(run.get("active_jobs")),
            failed=_fmt_count(run.get("failed_jobs")),
            items=_fmt_count(run.get("items_found_total")),
        )
    )
    print(
        "catalog rows={rows} distinct={distinct} dupes={dupes} oldest={oldest} newest={newest}".format(
            rows=_fmt_count(catalog.get("rows")),
            distinct=_fmt_count(catalog.get("distinct_source_ids")),
            dupes=_fmt_count(catalog.get("duplicate_source_ids")),
            oldest=_json_safe(catalog.get("oldest_posted_at")),
            newest=_json_safe(catalog.get("newest_posted_at")),
        )
    )
    print(
        "details instagram_posts={posts}/{catalog} canonical={canonical}/{catalog} hosted_media={hosted} canonical_metric_mismatches={mismatches}".format(
            posts=_fmt_count(details.get("instagram_posts_rows")),
            catalog=_fmt_count(details.get("catalog_rows")),
            canonical=_fmt_count(details.get("canonical_social_posts_rows")),
            hosted=_fmt_count(details.get("hosted_media_linked_rows")),
            mismatches=_fmt_count(details.get("canonical_metric_mismatch_rows")),
        )
    )
    print(
        "comments reported={reported} saved_rows={rows} posts_with_comments={posts} posts_reporting={reporting} reporting_without_saved={missing} saved_source={source} comment_media_pending={media_pending} comment_media_mirrored={media_mirrored} comment_media_failed={media_failed} comment_media_sample={sample_checked}/{sample_limit} followup_state={state} deferred_until={until}".format(
            reported=_fmt_count(comments.get("reported_from_post_details")),
            rows=_fmt_count(comments.get("rows")),
            posts=_fmt_count(comments.get("posts_with_comments")),
            reporting=_fmt_count(comments.get("posts_reporting_comments")),
            missing=_fmt_count(comments.get("posts_reporting_comments_without_saved_comments")),
            source=comments.get("saved_counts_source") or "instagram_comments",
            media_pending=_fmt_count(comments.get("comment_media_pending")),
            media_mirrored=_fmt_count(comments.get("comment_media_mirrored")),
            media_failed=_fmt_count(comments.get("comment_media_failed")),
            sample_checked=_fmt_count(comments.get("comment_media_sample_posts_checked")),
            sample_limit=_fmt_count(comments.get("comment_media_sample_limit")),
            state=comments_followup.get("state") or comments_followup.get("status") or "none",
            until=comments_followup.get("deferred_until") or "none",
        )
    )
    print(
        (
            "comments_recovery running={running} retrying={retrying} queued={queued} failed={failed} "
            "completed={completed} public_recovery={public_status} public_jobs={public_jobs} "
            "public_targets={public_targets} auth_followup={auth_status} auth_targets={auth_targets}"
        ).format(
            running=_fmt_count(comments_recovery.get("running_jobs")),
            retrying=_fmt_count(comments_recovery.get("retrying_jobs")),
            queued=_fmt_count(comments_recovery.get("queued_jobs")) + _fmt_count(comments_recovery.get("pending_jobs")),
            failed=_fmt_count(comments_recovery.get("failed_jobs")),
            completed=_fmt_count(comments_recovery.get("completed_jobs")),
            public_status=public_recovery_bucket.get("status") or "empty",
            public_jobs=_fmt_count(comments_recovery.get("public_recovery_jobs")),
            public_targets=_fmt_count(public_recovery_bucket.get("target_source_ids_count")),
            auth_status=authenticated_bucket.get("status") or "empty",
            auth_targets=_fmt_count(authenticated_bucket.get("target_source_ids_count")),
        )
    )
    media_counts = ", ".join(
        f"{row.get('status')}={_fmt_count(row.get('rows'))}" for row in progress.get("media_mirror_status_counts") or []
    )
    print(f"media_mirror {media_counts or 'none'}")
    print(
        "speed account=@{account} posts_per_min={posts_rate} pages_per_hour={page_rate} media_per_hour={media_rate} media_completed_15m={media_15m} media_completed_1h={media_1h} media_completed_6h={media_6h} media_eta={media_eta} media_running={media_running} media_queued={media_queued} latest_media_completed_at={latest_media_completed_at}".format(
            account=speed.get("account_handle") or "unknown",
            posts_rate=_fmt_rate(speed.get("posts_per_minute"), ""),
            page_rate=_fmt_rate(speed.get("pages_per_hour"), ""),
            media_rate=_fmt_rate(speed.get("media_per_hour"), ""),
            media_15m=_fmt_count(speed.get("media_completed_15m")),
            media_1h=_fmt_count(speed.get("media_completed_1h")),
            media_6h=_fmt_count(speed.get("media_completed_6h")),
            media_eta=(
                _fmt_seconds(speed.get("media_eta_seconds"))
                if speed.get("media_eta_seconds") is not None
                else "unknown"
            ),
            media_running=_fmt_count(speed.get("media_running")),
            media_queued=_fmt_count(speed.get("media_queued")),
            latest_media_completed_at=_json_safe(speed.get("latest_media_completed_at")),
        )
    )
    for frontier in frontiers[:2]:
        metadata = _metadata(frontier.get("metadata"))
        print(
            "frontier account=@{account} status={status} pages={pages} checked={checked} saved={saved} cursor={cursor} transport={transport} retry_at={retry_at} last_page_completed_at={last_page_completed_at} last_error={error}".format(
                account=frontier.get("account_handle"),
                status=frontier.get("status"),
                pages=_fmt_count(frontier.get("pages_scanned")),
                checked=_fmt_count(frontier.get("posts_checked")),
                saved=_fmt_count(frontier.get("posts_saved")),
                cursor="yes" if frontier.get("next_cursor_present") else "no",
                transport=frontier.get("last_transport") or "unknown",
                retry_at=metadata.get("retry_available_at")
                or metadata.get("job_available_at")
                or metadata.get("auth_cooldown_until")
                or metadata.get("cooldown_until")
                or "none",
                last_page_completed_at=_json_safe(frontier.get("updated_at")),
                error=metadata.get("last_error_code") or "none",
            )
        )
        print(
            "frontier_health account=@{account} updated_age={updated_age} lease_owner={lease_owner} lease_expires_in={lease_expires_in}".format(
                account=frontier.get("account_handle"),
                updated_age=_fmt_seconds(frontier.get("updated_age_seconds")),
                lease_owner=frontier.get("lease_owner") or "none",
                lease_expires_in=(
                    _fmt_seconds(frontier.get("lease_expires_in_seconds"))
                    if frontier.get("lease_expires_in_seconds") is not None
                    else "none"
                ),
            )
        )
    if cooldown:
        print(
            "auth_cooldown account=@{account} until={until} failures={failures} last_error={error}".format(
                account=cooldown.get("account_handle"),
                until=_json_safe(cooldown.get("cooldown_until")),
                failures=_fmt_count(cooldown.get("consecutive_failures")),
                error=cooldown.get("last_error_code") or "none",
            )
        )
    if dispatch_control:
        print(
            "dispatch_control pause_after_current={pause} paused_at={paused_at} reason={reason}".format(
                pause=dispatch_control.get("pause_after_current"),
                paused_at=dispatch_control.get("paused_at") or "none",
                reason=dispatch_control.get("pause_reason") or "none",
            )
        )
    if latest_jobs:
        latest = latest_jobs[0]
        print(
            "latest_job id={id} stage={stage} status={status} attempts={attempt}/{max_attempts} worker={worker} heartbeat_age={heartbeat_age} progress_age={progress_age} available_at={available_at} error={error}".format(
                id=latest.get("id"),
                stage=latest.get("stage"),
                status=latest.get("status"),
                attempt=_fmt_count(latest.get("attempt_count")),
                max_attempts=_fmt_count(latest.get("max_attempts")),
                worker=latest.get("worker_id") or "none",
                heartbeat_age=_fmt_seconds(latest.get("heartbeat_age_seconds")),
                progress_age=(
                    _fmt_seconds(latest.get("progress_age_seconds"))
                    if latest.get("progress_age_seconds") is not None
                    else "none"
                ),
                available_at=_json_safe(latest.get("available_at")),
                error=latest.get("last_error_code") or "none",
            )
        )
    alerts = list(progress.get("alerts") or [])
    if alerts:
        alert_text = ", ".join(
            f"{alert.get('code')}:{alert.get('account_handle') or alert.get('job_id')}" for alert in alerts
        )
        print(f"alerts {alert_text}")
    else:
        print("alerts none")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compact Instagram catalog backfill progress for one run.")
    parser.add_argument("--run-id", required=True, help="social.scrape_runs id")
    parser.add_argument("--json", action="store_true", help="Emit full JSON instead of compact text")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_env()
    progress = build_progress(str(args.run_id).strip())
    if args.json:
        print(json.dumps(_json_safe(progress), indent=2, sort_keys=True))
    else:
        print_compact(progress)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
