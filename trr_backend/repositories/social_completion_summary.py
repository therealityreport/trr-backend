"""Narrow shared-schema reads for social completion and landing health."""

# ruff: noqa: E501 -- SQL contract expressions are kept readable and grep-friendly.

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from typing import Any, TypedDict

from psycopg2.errors import UndefinedColumn, UndefinedTable

from trr_backend.db import pg
from trr_backend.socials.instagram import payload_compare, payload_sidecars

SUPPORTED_COMPLETION_PLATFORM = "instagram"
SCRAPE_JOB_HEALTH_WINDOW_HOURS = 8
SCRAPE_JOB_HEALTH_PLATFORMS = ("instagram", "tiktok", "twitter", "youtube")
COMPLETION_COMPARE_SURFACE = "social_completion_summary"
COMPLETION_COMPARE_LOG_PREFIX = "Instagram completion payload comparison payload_compare="
COMPLETION_TYPED_FAST_PATH_MIN_YEAR = 2025

logger = logging.getLogger(__name__)


class CompletionLane(TypedDict):
    finished: int
    in_progress: int
    not_started: int


class CompletionLanes(TypedDict):
    comments: CompletionLane
    details: CompletionLane
    media: CompletionLane


class SocialCompletionSummary(TypedDict):
    platform: str
    handle: str
    year: int
    total_posts: int
    total_reported_comments: int
    saved_comments: int
    missing_comments: int
    accounted_comments: int
    lanes: CompletionLanes


class ScrapeJobHealthSummary(TypedDict):
    window_hours: int
    window_started_at: str | None
    generated_at: str | None
    total_jobs: int
    active_jobs: int
    failed_jobs: int
    failure_signal_jobs: int
    in_failed_sql_transaction_hits: int
    latest_failure_at: str | None


_COMPLETION_SUMMARY_SQL_TEMPLATE = r"""
with target_input as (
  select %s::text as handle, %s::int as year
),
target as (
  select
    handle,
    make_timestamptz(year, 1, 1, 0, 0, 0) as start_at,
    make_timestamptz(year + 1, 1, 1, 0, 0, 0) as end_at
  from target_input
),
catalog_candidates as materialized (
  select
    cp.source_id as shortcode,
    cp.posted_at,
    coalesce(cp.comments_count, 0)::bigint as catalog_comments_count,
    __CATALOG_SIDECAR_PRESENT__ as sidecar_present
  from social.instagram_account_catalog_posts cp
  __CATALOG_PAYLOAD_JOIN__
  cross join target t
  where cp.posted_at >= t.start_at
    and cp.posted_at < t.end_at
    and nullif(cp.source_id, '') is not null
    and (
      nullif(regexp_replace(lower(regexp_replace(coalesce(cp.source_account, ''), '^@+', '')), '[^a-z0-9._-]+', '', 'g'), '') = t.handle
      or nullif(regexp_replace(lower(regexp_replace(coalesce(cp.owner_username, ''), '^@+', '')), '[^a-z0-9._-]+', '', 'g'), '') = t.handle
      __CATALOG_RAW_OWNER_MATCH__
      or exists (
        select 1
        from social.instagram_account_catalog_post_collaborators collaborator
        where collaborator.catalog_post_id = cp.id
          and collaborator.collaborator_handle = t.handle
      )
      or exists (
        select 1
        from jsonb_array_elements_text(coalesce(cp.collaborators, '[]'::jsonb)) collaborator(value)
        where nullif(
          regexp_replace(lower(regexp_replace(coalesce(collaborator.value, ''), '^@+', '')), '[^a-z0-9._-]+', '', 'g'),
          ''
        ) = t.handle
      )
      __CATALOG_RAW_COLLABORATOR_MATCH__
    )
),
filtered_posts as materialized (
  select
    p.shortcode,
    p.id as post_id,
    p.posted_at,
    __POST_DETAIL_COMMENTS_COUNT__ as detail_comments_count,
    p.comments_count::bigint as instagram_reported_comments,
    lower(coalesce(p.media_mirror_status, '')) as media_mirror_status,
    __POST_SIDECAR_PRESENT__ as sidecar_present
  from social.instagram_posts p
  __POST_PAYLOAD_JOIN__
  __POST_DETAIL_COUNT_JOINS__
  cross join target t
  where p.posted_at >= t.start_at
    and p.posted_at < t.end_at
    and nullif(p.shortcode, '') is not null
    and (
      nullif(regexp_replace(lower(regexp_replace(coalesce(p.source_account, ''), '^@+', '')), '[^a-z0-9._-]+', '', 'g'), '') = t.handle
      or nullif(regexp_replace(lower(regexp_replace(coalesce(p.owner_username, ''), '^@+', '')), '[^a-z0-9._-]+', '', 'g'), '') = t.handle
      or nullif(regexp_replace(lower(regexp_replace(coalesce(p.username, ''), '^@+', '')), '[^a-z0-9._-]+', '', 'g'), '') = t.handle
      or exists (
        select 1
        from jsonb_array_elements_text(coalesce(p.collaborators, '[]'::jsonb)) collaborator(value)
        where nullif(
          regexp_replace(lower(regexp_replace(coalesce(collaborator.value, ''), '^@+', '')), '[^a-z0-9._-]+', '', 'g'),
          ''
        ) = t.handle
      )
      or exists (
        select 1
        from jsonb_array_elements(
          coalesce(
            nullif(p.collaborators_detail, '[]'::jsonb),
            __POST_RAW_COLLABORATOR_DETAIL__
            '[]'::jsonb
          )
        ) collaborator(detail)
        where nullif(
          regexp_replace(
            lower(regexp_replace(coalesce(collaborator.detail ->> 'username', ''), '^@+', '')),
            '[^a-z0-9._-]+',
            '',
            'g'
          ),
          ''
        ) = t.handle
      )
    )
),
post_counts as materialized (
  select
    p.shortcode,
    p.post_id,
    p.posted_at,
    p.instagram_reported_comments,
    p.media_mirror_status,
    p.sidecar_present,
    p.detail_comments_count
  from filtered_posts p
),
candidate_post_ids as materialized (
  select distinct post_id
  from post_counts
  where post_id is not null
),
fb_crosspost_counts as materialized (
  select
    candidate.post_id,
    count(*)::bigint as fb_crosspost_count
  from candidate_post_ids candidate
  join social.instagram_comments c
    on c.post_id = candidate.post_id
   and c.phase = 'fb_crosspost'
  group by candidate.post_id
),
missing_rollup_saved_counts as materialized (
  select
    candidate.post_id,
    count(*) filter (where c.phase is distinct from 'fb_crosspost')::bigint as saved_comment_count
  from candidate_post_ids candidate
  left join social.instagram_post_comment_rollups candidate_rollup
    on candidate_rollup.post_id = candidate.post_id
  join social.instagram_comments c on c.post_id = candidate.post_id
  where candidate_rollup.post_id is null
  group by candidate.post_id
),
post_candidates as materialized (
  select
    p.shortcode,
    p.post_id,
    p.posted_at,
    p.detail_comments_count,
    p.media_mirror_status,
    p.sidecar_present,
    saved.saved_comments,
    greatest(coalesce(p.instagram_reported_comments, p.detail_comments_count, 0), 0)::bigint as health_reported_comments,
    greatest(
      coalesce(
        r.missing_comment_count,
        greatest(
          coalesce(p.instagram_reported_comments, p.detail_comments_count, 0)::bigint
            - saved.saved_comments,
          0
        ),
        0
      ),
      0
    )::bigint as missing_comments
  from post_counts p
  left join social.instagram_post_comment_rollups r on r.post_id = p.post_id
  left join fb_crosspost_counts fb on fb.post_id = p.post_id
  left join missing_rollup_saved_counts fallback on fallback.post_id = p.post_id
  cross join lateral (
    select greatest(
      case
        when r.post_id is not null then
          -- Preserve comment_capture_health.saved_comment_count exactly: every
          -- non-Facebook row counted as saved, including classified-missing rows.
          coalesce(r.total_comment_count, 0)::bigint - coalesce(fb.fb_crosspost_count, 0)::bigint
        else coalesce(fallback.saved_comment_count, 0)::bigint
      end,
      0
    )::bigint as saved_comments
  ) saved
),
matched_shortcodes as materialized (
  select shortcode from catalog_candidates
  union
  select shortcode from post_candidates
),
catalog as materialized (
  select
    matched.shortcode,
    max(c.catalog_comments_count)::bigint as catalog_comments_count,
    coalesce(bool_or(c.sidecar_present), false) as sidecar_present
  from matched_shortcodes matched
  left join catalog_candidates c on c.shortcode = matched.shortcode
  group by matched.shortcode
),
latest_post as materialized (
  select distinct on (p.shortcode)
    p.shortcode,
    p.post_id,
    coalesce(p.detail_comments_count, 0)::bigint as detail_comments_count,
    lower(coalesce(p.media_mirror_status, '')) as media_mirror_status,
    p.saved_comments,
    p.health_reported_comments,
    p.missing_comments,
    p.posted_at,
    p.sidecar_present
  from post_candidates p
  order by p.shortcode, p.posted_at desc nulls last, p.post_id desc
),
scored as (
  select
    matched.shortcode,
    lp.post_id,
    greatest(
      coalesce(c.catalog_comments_count, 0),
      coalesce(lp.health_reported_comments, 0),
      coalesce(lp.detail_comments_count, 0)
    )::bigint as reported_comments,
    coalesce(lp.saved_comments, 0)::bigint as saved_comments,
    greatest(
      coalesce(
        lp.missing_comments,
        greatest(
          greatest(
            coalesce(c.catalog_comments_count, 0),
            coalesce(lp.health_reported_comments, 0),
            coalesce(lp.detail_comments_count, 0)
          ) - coalesce(lp.saved_comments, 0),
          0
        ),
        0
      ),
      0
    )::bigint as missing_comments,
    lp.media_mirror_status,
    coalesce(c.sidecar_present, false) or coalesce(lp.sidecar_present, false) as sidecar_present
  from matched_shortcodes matched
  left join catalog c on c.shortcode = matched.shortcode
  left join latest_post lp on lp.shortcode = matched.shortcode
)
select
  count(*)::bigint as total_posts,
  coalesce(sum(reported_comments), 0)::bigint as total_reported_comments,
  coalesce(sum(saved_comments), 0)::bigint as saved_comments,
  coalesce(sum(missing_comments), 0)::bigint as missing_comments,
  coalesce(sum(saved_comments + missing_comments), 0)::bigint as accounted_comments,
  count(*) filter (
    where reported_comments = 0
       or (reported_comments > 0 and missing_comments = 0 and reported_comments <= saved_comments)
  )::bigint as comments_finished,
  count(*) filter (
    where reported_comments > 0 and saved_comments > 0 and missing_comments > 0
  )::bigint as comments_in_progress,
  count(*) filter (where reported_comments > 0 and saved_comments = 0)::bigint as comments_not_started,
  count(*) filter (where post_id is not null)::bigint as details_finished,
  count(*) filter (where post_id is null)::bigint as details_not_started,
  count(*) filter (where media_mirror_status in ('complete', 'completed', 'mirrored', 'up_to_date'))::bigint as media_finished,
  count(*) filter (where media_mirror_status in ('pending', 'partial', 'queued', 'retrying', 'running', 'failed'))::bigint as media_in_progress,
  count(*) filter (
    where post_id is null
       or coalesce(media_mirror_status, '') not in ('complete', 'completed', 'mirrored', 'up_to_date', 'pending', 'partial', 'queued', 'retrying', 'running', 'failed')
  )::bigint as media_not_started,
  coalesce(bool_or(sidecar_present), false) as sidecar_present
from scored
"""


_CATALOG_RAW_OWNER_MATCH_SQL = r"""or nullif(
        regexp_replace(
          lower(
            regexp_replace(
              coalesce(
                __CATALOG_RAW_DATA__ ->> 'username',
                __CATALOG_RAW_DATA__ ->> 'ownerUsername',
                __CATALOG_RAW_DATA__ -> 'owner' ->> 'username',
                __CATALOG_RAW_DATA__ -> 'user' ->> 'username',
                ''
              ),
              '^@+',
              ''
            )
          ),
          '[^a-z0-9._-]+',
          '',
          'g'
        ),
        ''
      ) = t.handle"""

_CATALOG_RAW_COLLABORATOR_MATCH_SQL = r"""or exists (
        select 1
        from jsonb_array_elements(coalesce(__CATALOG_RAW_DATA__ -> 'collaborators_detail', '[]'::jsonb)) collaborator(detail)
        where nullif(
          regexp_replace(
            lower(regexp_replace(coalesce(collaborator.detail ->> 'username', ''), '^@+', '')),
            '[^a-z0-9._-]+',
            '',
            'g'
          ),
          ''
        ) = t.handle
      )"""

_POST_RAW_DETAIL_COMMENTS_COUNT_SQL = r"""greatest(
      coalesce(nullif(regexp_replace(coalesce(payload_counts.comments_count, ''), '[^0-9]', '', 'g'), '')::bigint, 0),
      coalesce(nullif(regexp_replace(coalesce(payload_counts.comments, ''), '[^0-9]', '', 'g'), '')::bigint, 0),
      coalesce(nullif(regexp_replace(coalesce(payload_counts.comment_count, ''), '[^0-9]', '', 'g'), '')::bigint, 0),
      coalesce(nullif(regexp_replace(coalesce(payload_counts."commentsCount", ''), '[^0-9]', '', 'g'), '')::bigint, 0),
      coalesce(nullif(regexp_replace(coalesce(payload_counts.edge_media_to_comment ->> 'count', ''), '[^0-9]', '', 'g'), '')::bigint, 0),
      coalesce(nullif(regexp_replace(coalesce(payload_counts.edge_media_to_parent_comment ->> 'count', ''), '[^0-9]', '', 'g'), '')::bigint, 0),
      coalesce(nullif(regexp_replace(coalesce(payload_counts.edge_media_preview_comment ->> 'count', ''), '[^0-9]', '', 'g'), '')::bigint, 0),
      coalesce(nullif(regexp_replace(coalesce(media_counts.comments_count, ''), '[^0-9]', '', 'g'), '')::bigint, 0),
      coalesce(nullif(regexp_replace(coalesce(media_counts.comments, ''), '[^0-9]', '', 'g'), '')::bigint, 0),
      coalesce(nullif(regexp_replace(coalesce(media_counts.comment_count, ''), '[^0-9]', '', 'g'), '')::bigint, 0),
      coalesce(nullif(regexp_replace(coalesce(media_counts."commentsCount", ''), '[^0-9]', '', 'g'), '')::bigint, 0),
      coalesce(nullif(regexp_replace(coalesce(metrics_counts.comments_count, ''), '[^0-9]', '', 'g'), '')::bigint, 0),
      coalesce(nullif(regexp_replace(coalesce(metrics_counts.comments, ''), '[^0-9]', '', 'g'), '')::bigint, 0),
      0
    )::bigint"""

_POST_RAW_DETAIL_COUNT_JOINS_SQL = r"""cross join lateral jsonb_to_record(
    case when jsonb_typeof(__POST_RAW_DATA__) = 'object' then __POST_RAW_DATA__ else '{}'::jsonb end
  ) as payload_counts(
    comments_count text,
    comments text,
    comment_count text,
    "commentsCount" text,
    edge_media_to_comment jsonb,
    edge_media_to_parent_comment jsonb,
    edge_media_preview_comment jsonb,
    media jsonb,
    metrics jsonb
  )
  cross join lateral jsonb_to_record(
    case when jsonb_typeof(payload_counts.media) = 'object' then payload_counts.media else '{}'::jsonb end
  ) as media_counts(comments_count text, comments text, comment_count text, "commentsCount" text)
  cross join lateral jsonb_to_record(
    case when jsonb_typeof(payload_counts.metrics) = 'object' then payload_counts.metrics else '{}'::jsonb end
  ) as metrics_counts(comments_count text, comments text)"""


def _completion_summary_sql(*, sidecar: bool, raw_compatible: bool) -> str:
    substitutions = {
        "__CATALOG_PAYLOAD_JOIN__": (
            "left join lateral ("
            f"select payload.catalog_post_id{', payload.raw_data' if raw_compatible else ''} "
            "from social.instagram_account_catalog_post_payloads payload "
            "where payload.catalog_post_id = cp.id limit 1"
            ") cp_payload on true"
            if sidecar
            else ""
        ),
        "__CATALOG_RAW_OWNER_MATCH__": _CATALOG_RAW_OWNER_MATCH_SQL if raw_compatible else "",
        "__CATALOG_RAW_COLLABORATOR_MATCH__": _CATALOG_RAW_COLLABORATOR_MATCH_SQL if raw_compatible else "",
        "__CATALOG_RAW_DATA__": "coalesce(cp_payload.raw_data, cp.raw_data)" if sidecar else "cp.raw_data",
        "__CATALOG_SIDECAR_PRESENT__": "(cp_payload.catalog_post_id is not null)" if sidecar else "false",
        "__POST_PAYLOAD_JOIN__": (
            "left join lateral ("
            f"select payload.post_id{', payload.raw_data' if raw_compatible else ''} "
            "from social.instagram_post_payloads payload "
            "where payload.post_id = p.id limit 1"
            ") p_payload on true"
            if sidecar
            else ""
        ),
        "__POST_DETAIL_COMMENTS_COUNT__": (
            _POST_RAW_DETAIL_COMMENTS_COUNT_SQL
            if raw_compatible
            else "greatest(coalesce(p.comments_count, 0), 0)::bigint"
        ),
        "__POST_DETAIL_COUNT_JOINS__": _POST_RAW_DETAIL_COUNT_JOINS_SQL if raw_compatible else "",
        "__POST_RAW_COLLABORATOR_DETAIL__": (
            "nullif(__POST_RAW_DATA__ -> 'collaborators_detail', '[]'::jsonb)," if raw_compatible else ""
        ),
        "__POST_RAW_DATA__": "coalesce(p_payload.raw_data, p.raw_data)" if sidecar else "p.raw_data",
        "__POST_SIDECAR_PRESENT__": "(p_payload.post_id is not null)" if sidecar else "false",
    }
    sql = _COMPLETION_SUMMARY_SQL_TEMPLATE
    for token, replacement in substitutions.items():
        sql = sql.replace(token, replacement)
    return sql


_COMPLETION_SUMMARY_SQL = _completion_summary_sql(sidecar=False, raw_compatible=False)
_COMPLETION_SUMMARY_SIDECAR_SQL = _completion_summary_sql(sidecar=True, raw_compatible=False)
_COMPLETION_SUMMARY_RAW_COMPAT_SQL = _completion_summary_sql(sidecar=False, raw_compatible=True)
_COMPLETION_SUMMARY_RAW_COMPAT_SIDECAR_SQL = _completion_summary_sql(sidecar=True, raw_compatible=True)


_SCRAPE_JOB_HEALTH_SQL = """
/* landing_social_scrape_job_health */
with input as (
  select %s::int as window_hours, %s::text[] as platforms
),
recent_jobs as (
  select
    status,
    error_message,
    last_error_code,
    metadata,
    created_at
  from social.scrape_jobs
  cross join input
  where platform = any(input.platforms)
    and created_at >= now() - (input.window_hours * interval '1 hour')
),
job_signals as (
  select
    1::int as job_row,
    status,
    created_at,
    coalesce(error_message, '') || ' ' || coalesce(last_error_code, '') as error_signal,
    coalesce(error_message, '') || ' ' || coalesce(last_error_code, '') || ' ' ||
      coalesce(metadata::text, '') as diagnostic_text
  from recent_jobs
)
select
  now() as generated_at,
  now() - (input.window_hours * interval '1 hour') as window_started_at,
  count(job_signals.job_row)::bigint as total_jobs,
  count(*) filter (
    where status in ('queued', 'pending', 'running', 'retrying', 'cancelling')
  )::bigint as active_jobs,
  count(*) filter (where status in ('failed', 'error'))::bigint as failed_jobs,
  count(*) filter (
    where status in ('failed', 'error') or nullif(trim(error_signal), '') is not null
  )::bigint as failure_signal_jobs,
  count(*) filter (
    where diagnostic_text ilike '%%InFailedSqlTransaction%%'
  )::bigint as in_failed_sql_transaction_hits,
  max(created_at) filter (
    where status in ('failed', 'error') or nullif(trim(error_signal), '') is not null
  ) as latest_failure_at
from input
left join job_signals on true
group by input.window_hours
"""


def _read_count(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError, OverflowError):
        return 0


def _to_iso_string(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        normalized = value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return normalized.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    rendered = str(value).strip()
    return rendered or None


def get_social_completion_summary(
    *,
    platform: str,
    account_handle: str,
    year: int,
) -> SocialCompletionSummary:
    """Return the existing annual Instagram completion contract."""

    mode = payload_sidecars.payload_read_mode()
    identity = {"platform": platform, "handle": account_handle, "year": year}

    if mode == "legacy":
        return _completion_summary_from_row(
            _fetch_completion_summary_mode_row(
                sidecar=False,
                account_handle=account_handle,
                year=year,
            ),
            platform=platform,
            account_handle=account_handle,
            year=year,
        )

    if mode == "sidecar":
        try:
            sidecar_row = _fetch_completion_summary_mode_row(
                sidecar=True,
                account_handle=account_handle,
                year=year,
            )
        except UndefinedTable:
            legacy_summary = _completion_summary_from_row(
                _fetch_completion_summary_mode_row(
                    sidecar=False,
                    account_handle=account_handle,
                    year=year,
                ),
                platform=platform,
                account_handle=account_handle,
                year=year,
            )
            _log_payload_compare_event(
                identity=identity,
                legacy_payload=legacy_summary,
                new_payload={},
                sidecar_present=False,
                schema_unavailable=True,
            )
            return legacy_summary
        return _completion_summary_from_row(
            sidecar_row,
            platform=platform,
            account_handle=account_handle,
            year=year,
        )

    legacy_summary = _completion_summary_from_row(
        _fetch_completion_summary_mode_row(
            sidecar=False,
            account_handle=account_handle,
            year=year,
        ),
        platform=platform,
        account_handle=account_handle,
        year=year,
    )
    if not payload_compare.should_sample_payload_compare(
        surface=COMPLETION_COMPARE_SURFACE,
        entity_identity=identity,
    ):
        return legacy_summary

    try:
        sidecar_row = _fetch_completion_summary_mode_row(
            sidecar=True,
            account_handle=account_handle,
            year=year,
        )
    except UndefinedTable:
        _log_payload_compare_event(
            identity=identity,
            legacy_payload=legacy_summary,
            new_payload={},
            sidecar_present=False,
            schema_unavailable=True,
        )
        return legacy_summary

    sidecar_summary = _completion_summary_from_row(
        sidecar_row,
        platform=platform,
        account_handle=account_handle,
        year=year,
    )
    _log_payload_compare_event(
        identity=identity,
        legacy_payload=legacy_summary,
        new_payload=sidecar_summary,
        sidecar_present=bool(sidecar_row.get("sidecar_present")),
        schema_unavailable=False,
    )
    return legacy_summary


def _fetch_completion_summary_mode_row(
    *,
    sidecar: bool,
    account_handle: str,
    year: int,
) -> dict[str, Any]:
    compatibility_sql = _COMPLETION_SUMMARY_RAW_COMPAT_SIDECAR_SQL if sidecar else _COMPLETION_SUMMARY_RAW_COMPAT_SQL
    if year < COMPLETION_TYPED_FAST_PATH_MIN_YEAR:
        return _fetch_completion_summary_row(
            compatibility_sql,
            account_handle=account_handle,
            year=year,
        )

    current_schema_sql = _COMPLETION_SUMMARY_SIDECAR_SQL if sidecar else _COMPLETION_SUMMARY_SQL
    try:
        return _fetch_completion_summary_row(
            current_schema_sql,
            account_handle=account_handle,
            year=year,
        )
    except (UndefinedColumn, UndefinedTable):
        # Compatibility is deliberately a retry, never the current-schema hot
        # path. It retains historical raw ownership/count fallbacks for older
        # installations while normal requests stay on canonical typed columns.
        return _fetch_completion_summary_row(
            compatibility_sql,
            account_handle=account_handle,
            year=year,
        )


def _fetch_completion_summary_row(sql: str, *, account_handle: str, year: int) -> dict[str, Any]:
    return (
        pg.fetch_one(
            sql,
            [account_handle, year],
            pool_name="social_profile",
        )
        or {}
    )


def _completion_summary_from_row(
    row: dict[str, Any],
    *,
    platform: str,
    account_handle: str,
    year: int,
) -> SocialCompletionSummary:
    return {
        "platform": platform,
        "handle": account_handle,
        "year": year,
        "total_posts": _read_count(row.get("total_posts")),
        "total_reported_comments": _read_count(row.get("total_reported_comments")),
        "saved_comments": _read_count(row.get("saved_comments")),
        "missing_comments": _read_count(row.get("missing_comments")),
        "accounted_comments": _read_count(row.get("accounted_comments")),
        "lanes": {
            "comments": {
                "finished": _read_count(row.get("comments_finished")),
                "in_progress": _read_count(row.get("comments_in_progress")),
                "not_started": _read_count(row.get("comments_not_started")),
            },
            "details": {
                "finished": _read_count(row.get("details_finished")),
                "in_progress": 0,
                "not_started": _read_count(row.get("details_not_started")),
            },
            "media": {
                "finished": _read_count(row.get("media_finished")),
                "in_progress": _read_count(row.get("media_in_progress")),
                "not_started": _read_count(row.get("media_not_started")),
            },
        },
    }


def _log_payload_compare_event(
    *,
    identity: dict[str, Any],
    legacy_payload: SocialCompletionSummary,
    new_payload: dict[str, Any] | SocialCompletionSummary,
    sidecar_present: bool,
    schema_unavailable: bool,
) -> None:
    event = payload_compare.build_payload_compare_event(
        surface=COMPLETION_COMPARE_SURFACE,
        entity_identity=identity,
        legacy_payload=legacy_payload,
        new_payload=new_payload,
        sidecar_present=sidecar_present,
        schema_unavailable=schema_unavailable,
    )
    # Modal's normal log stream renders the message but does not preserve
    # arbitrary LogRecord ``extra`` fields. The compact event has capped
    # mismatch records and fixed-length hashes, so the compare-soak gate remains
    # both searchable and raw-free even when request trace headers are untrusted.
    message = COMPLETION_COMPARE_LOG_PREFIX + json.dumps(event, sort_keys=True, separators=(",", ":"))
    logger.info(message, extra={"payload_compare": event})


def get_social_landing_scrape_job_health() -> ScrapeJobHealthSummary:
    """Return the landing page's existing eight-hour scrape-job health rollup."""

    row = (
        pg.fetch_one(
            _SCRAPE_JOB_HEALTH_SQL,
            [SCRAPE_JOB_HEALTH_WINDOW_HOURS, list(SCRAPE_JOB_HEALTH_PLATFORMS)],
            pool_name="social_profile",
        )
        or {}
    )
    return {
        "window_hours": SCRAPE_JOB_HEALTH_WINDOW_HOURS,
        "window_started_at": _to_iso_string(row.get("window_started_at")),
        "generated_at": _to_iso_string(row.get("generated_at")),
        "total_jobs": _read_count(row.get("total_jobs")),
        "active_jobs": _read_count(row.get("active_jobs")),
        "failed_jobs": _read_count(row.get("failed_jobs")),
        "failure_signal_jobs": _read_count(row.get("failure_signal_jobs")),
        "in_failed_sql_transaction_hits": _read_count(row.get("in_failed_sql_transaction_hits")),
        "latest_failure_at": _to_iso_string(row.get("latest_failure_at")),
    }
