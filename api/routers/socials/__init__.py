"""
API endpoints for social media scraping and analytics.

Provides endpoints to:
1. Scrape Instagram posts from profiles with filtering
2. Scrape TikTok posts from profiles with filtering
3. Search Twitter/X for tweets by hashtag/phrase
4. Scrape YouTube channel videos by keywords
5. Configure social accounts for shows/seasons/cast members
6. Retrieve cached social media data
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Lock, Thread
from time import monotonic as monotonic
from time import perf_counter
from typing import Any, Literal
from uuid import UUID

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel, Field, model_validator
from starlette.concurrency import run_in_threadpool

from api.auth import InternalAdminUser
from trr_backend.db.pg import database_service_unavailable_detail, is_database_service_unavailable_error
from trr_backend.job_plane import (
    canonical_execution_mode,
    execution_backend_canonical,
    execution_metadata,
    execution_owner_label,
    is_modal_remote_executor_enabled,
    is_remote_job_plane_enabled,
)
from trr_backend.modal_dispatch import (
    dispatch_reddit_refresh,
    get_modal_reddit_runtime_health,
    modal_app_name,
    modal_dispatch_ready,
    modal_environment_name,
    modal_execution_metadata,
    modal_reddit_refresh_function_name,
    modal_social_job_function_name,
    resolve_modal_function,
)
from trr_backend.observability import get_trace_id
from trr_backend.read_path_diagnostics import log_read_path
from trr_backend.repositories.twitter_standalone import persist_standalone_twitter_search
from trr_backend.socials.api.handlers import live_status as social_live_status
from trr_backend.socials.api.handlers import profile_reads as social_profile_reads
from trr_backend.socials.inline_ingest import (
    normalize_target_platforms as _normalize_inline_target_platforms,
)
from trr_backend.socials.inline_ingest import (
    run_inline_season_ingest_execution,
)
from trr_backend.socials.platforms import SOCIAL_SUPPORTED_PLATFORMS

from ._analytics_cache import (
    _ANALYTICS_CACHE,
    _ANALYTICS_CACHE_LOCK,
    _ANALYTICS_CACHE_MAX_ENTRIES,
    _ANALYTICS_CACHE_TTL_SECONDS,
    _COMMENTS_COVERAGE_CACHE,
    _COMMENTS_COVERAGE_CACHE_LOCK,
    _COVERAGE_CACHE_MAX_ENTRIES,
    _COVERAGE_CACHE_TTL_SECONDS,
    _MIRROR_COVERAGE_CACHE,
    _MIRROR_COVERAGE_CACHE_LOCK,
    _WEEK_LIVE_HEALTH_CACHE,
    _WEEK_LIVE_HEALTH_CACHE_LOCK,
    _WEEK_LIVE_HEALTH_CACHE_MAX_ENTRIES,
    _WEEK_LIVE_HEALTH_CACHE_TTL_SECONDS,
    WeekDetailSortDir,
    WeekDetailSortField,
    WeekSummaryInclude,
    _analytics_cache_key,
    _coverage_cache_window_key,
    _get_ttl_cached_payload,
    _get_week_detail_cached_payload,
    _get_week_summary_cached_payload,
    _set_ttl_cached_payload,
    _set_week_detail_cached_payload,
    _set_week_summary_cached_payload,
    _week_detail_cache_key,
    _week_live_health_cache_key,
    _week_summary_cache_key,
    invalidate_week_detail_cache,
)
from ._analytics_cache import (
    _clear_ttl_cache as _clear_ttl_cache,
)
from ._analytics_cache import (
    invalidate_week_summary_cache as invalidate_week_summary_cache,
)
from ._profile_cache import (
    _ACCOUNT_PROFILE_CACHE_MAX_ENTRIES,
    _ACCOUNT_PROFILE_CACHE_TTL_SECONDS,
    _ACCOUNT_PROFILE_COLLABORATORS_CACHE,
    _ACCOUNT_PROFILE_COLLABORATORS_CACHE_LOCK,
    _ACCOUNT_PROFILE_DASHBOARD_CACHE,
    _ACCOUNT_PROFILE_DASHBOARD_CACHE_LOCK,
    _ACCOUNT_PROFILE_HASHTAG_TIMELINE_CACHE,
    _ACCOUNT_PROFILE_HASHTAG_TIMELINE_CACHE_LOCK,
    _ACCOUNT_PROFILE_HASHTAGS_CACHE,
    _ACCOUNT_PROFILE_HASHTAGS_CACHE_LOCK,
    _ACCOUNT_PROFILE_POSTS_CACHE,
    _ACCOUNT_PROFILE_POSTS_CACHE_LOCK,
    _ACCOUNT_PROFILE_PROGRESS_CACHE_TTL_SECONDS,
    _ACCOUNT_PROFILE_SUMMARY_CACHE,
    _ACCOUNT_PROFILE_SUMMARY_CACHE_LOCK,
    _account_profile_cache_key,
    _clear_account_profile_caches,
    _resolve_account_profile_catalog_freshness,
    _resolve_account_profile_catalog_run_progress,
    _resolve_account_profile_singleflight,
)
from ._profile_cache import (
    _ACCOUNT_PROFILE_CATALOG_FRESHNESS_CACHE as _ACCOUNT_PROFILE_CATALOG_FRESHNESS_CACHE,
)
from ._profile_cache import (
    _ACCOUNT_PROFILE_CATALOG_FRESHNESS_CACHE_LOCK as _ACCOUNT_PROFILE_CATALOG_FRESHNESS_CACHE_LOCK,
)
from .analytics_read import (
    analytics_read_path_extra,
    page_week_detail_payload,
    parse_analytics_include,
    week_detail_cached_post_counts,
)
from .reddit import (
    serialize_reddit_backfill_payload as _serialize_reddit_backfill_payload,
)
from .reddit import (
    serialize_reddit_refresh_payload as _serialize_reddit_refresh_payload,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/socials", tags=["admin-socials"])

SourceScopeParam = Literal["bravo", "network", "creator", "community", "news"]
TWITTER_CATALOG_BACKFILL_LOOKBACK_DAYS = 365
INSTAGRAM_AUTH_REFRESH_CONFIRMATION = "I UNDERSTAND INSTAGRAM AUTH RISK"
INSTAGRAM_AUTH_REFRESH_WARNING = (
    "Manual Instagram auth can surface CAPTCHA, verification code, checkpoint, or account-lock prompts. "
    "Complete those steps yourself before confirming a validated-cookie sync."
)
SOCIAL_LANDING_PROGRESS_ROLLUP_CACHE_TTL_SECONDS = int(
    os.getenv("SOCIAL_LANDING_PROGRESS_ROLLUP_CACHE_TTL_SECONDS", "300")
)
SOCIAL_LANDING_PROGRESS_ROLLUP_CACHE_MAX_ENTRIES = int(
    os.getenv("SOCIAL_LANDING_PROGRESS_ROLLUP_CACHE_MAX_ENTRIES", "128")
)
_SOCIAL_LANDING_PROGRESS_ROLLUP_CACHE: dict[Any, tuple[float, dict[str, Any]]] = {}
_SOCIAL_LANDING_PROGRESS_ROLLUP_CACHE_LOCK = Lock()


def normalize_source_scope_param(value: str | None, *, default: str = "network") -> str:
    normalized = str(value or default).strip().lower() or default
    if normalized == "bravo":
        return "network"
    if normalized in {"network", "creator", "community", "news"}:
        return normalized
    raise ValueError(f"Unsupported source scope: {value}")


def preserve_source_scope_param(value: str | None, *, default: str = "network") -> str:
    normalized = str(value or default).strip().lower() or default
    if normalized in {"bravo", "network", "creator", "community", "news"}:
        return normalized
    raise ValueError(f"Unsupported source scope: {value}")


def _twitter_catalog_backfill_default_window(now: datetime | None = None) -> tuple[datetime, datetime]:
    end_at = now or datetime.now(tz=UTC)
    if end_at.tzinfo is None:
        end_at = end_at.replace(tzinfo=UTC)
    return end_at - timedelta(days=TWITTER_CATALOG_BACKFILL_LOOKBACK_DAYS), end_at


class SourceScopedRequest(BaseModel):
    source_scope: SourceScopeParam = Field(default="network")

    @model_validator(mode="after")
    def normalize_source_scope(self) -> SourceScopedRequest:
        self.source_scope = normalize_source_scope_param(self.source_scope)  # type: ignore[assignment]
        return self


class SocialBladeProfileRefreshRequest(BaseModel):
    force: bool = False


class SocialLandingSocialBladeRowsRequest(BaseModel):
    platforms: list[str] = Field(default_factory=list, max_length=8)
    person_ids: list[str] = Field(default_factory=list, max_length=1000)
    account_handles: list[str] = Field(default_factory=list, max_length=5000)


class SocialLandingSocialBladeProgressCountsRequest(BaseModel):
    platforms: list[str] = Field(default_factory=list, max_length=5000)
    account_handles: list[str] = Field(default_factory=list, max_length=5000)


class SocialLandingProgressRollupRequest(BaseModel):
    platforms: list[str] = Field(default_factory=list, max_length=5000)
    account_handles: list[str] = Field(default_factory=list, max_length=5000)


def _normalize_landing_progress_targets(
    platforms: list[str],
    account_handles: list[str],
) -> list[tuple[str, str]]:
    if len(platforms) != len(account_handles):
        raise HTTPException(status_code=400, detail="platforms and account_handles must have matching lengths")

    allowed_platforms = {"instagram", "tiktok", "twitter", "youtube", "facebook", "threads"}
    targets: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for platform_raw, handle_raw in zip(platforms, account_handles, strict=True):
        platform = str(platform_raw or "").strip().lower()
        handle = str(handle_raw or "").strip().lower().lstrip("@")
        if platform not in allowed_platforms or not handle:
            continue
        key = (platform, handle)
        if key in seen:
            continue
        seen.add(key)
        targets.append(key)
    return targets


def _landing_progress_cache_key(targets: list[tuple[str, str]]) -> tuple[Any, ...]:
    return tuple(sorted(targets))


def _sql_json_text_non_negative_int(expr: str) -> str:
    return (
        "coalesce(nullif(regexp_replace(coalesce("
        f"{expr}, ''), '[^0-9]', '', 'g'), '')::bigint, 0)"
    )


def _instagram_reported_comments_sql(alias: str) -> str:
    safe_alias = alias.strip() or "p"
    raw = f"coalesce({safe_alias}.raw_data, '{{}}'::jsonb)"
    raw_candidates = [
        f"{raw} ->> 'comments_count'",
        f"{raw} ->> 'comments'",
        f"{raw} ->> 'comment_count'",
        f"{raw} ->> 'commentsCount'",
        f"{raw} -> 'edge_media_to_comment' ->> 'count'",
        f"{raw} -> 'edge_media_to_parent_comment' ->> 'count'",
        f"{raw} -> 'edge_media_preview_comment' ->> 'count'",
        f"{raw} -> 'media' ->> 'comments_count'",
        f"{raw} -> 'media' ->> 'comments'",
        f"{raw} -> 'media' ->> 'comment_count'",
        f"{raw} -> 'media' ->> 'commentsCount'",
        f"{raw} -> 'metrics' ->> 'comments_count'",
        f"{raw} -> 'metrics' ->> 'comments'",
    ]
    return (
        f"greatest(coalesce({safe_alias}.comments_count, 0), "
        + ", ".join(_sql_json_text_non_negative_int(candidate) for candidate in raw_candidates)
        + ", 0)"
    )


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


def _social_landing_reddit_dashboard_summary() -> tuple[dict[str, int], list[dict[str, Any]]]:
    from trr_backend.repositories.admin_reddit_reads import list_reddit_communities

    omitted_sections: list[dict[str, Any]] = []
    try:
        payload, _query_count = list_reddit_communities(include_inactive=True)
        communities = payload.get("communities") if isinstance(payload, dict) else []
        community_rows = communities if isinstance(communities, list) else []
        show_ids = {
            str(community.get("trr_show_id") or "").strip()
            for community in community_rows
            if isinstance(community, dict) and str(community.get("trr_show_id") or "").strip()
        }
        active_count = sum(
            1 for community in community_rows if isinstance(community, dict) and bool(community.get("is_active"))
        )
        return (
            {
                "active_community_count": active_count,
                "archived_community_count": max(0, len(community_rows) - active_count),
                "show_count": len(show_ids),
            },
            omitted_sections,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to build social landing reddit summary", exc_info=True)
        omitted_sections.append(
            {
                "section": "reddit_dashboard",
                "reason": type(exc).__name__,
                "retryable": True,
            }
        )
        return (
            {
                "active_community_count": 0,
                "archived_community_count": 0,
                "show_count": 0,
            },
            omitted_sections,
        )


@router.get("/landing-summary")
def get_social_landing_summary(_: InternalAdminUser = None) -> dict[str, Any]:
    from trr_backend.repositories.covered_shows import list_covered_shows

    try:
        covered_shows, _query_count = list_covered_shows()
        reddit_dashboard, omitted_sections = _social_landing_reddit_dashboard_summary()
        payload: dict[str, Any] = {
            "covered_shows": covered_shows,
            "reddit_dashboard": reddit_dashboard,
        }
        if omitted_sections:
            payload["omitted_sections"] = omitted_sections
        return payload
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to build social landing summary")
        raise _to_social_read_http_exception(exc) from exc


@router.post("/landing-socialblade-rows")
def post_social_landing_socialblade_rows(
    payload: SocialLandingSocialBladeRowsRequest,
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.db import pg

    platforms = [
        platform.strip().lower()
        for platform in payload.platforms
        if platform.strip().lower() in {"instagram", "youtube", "facebook"}
    ]
    if not platforms:
        platforms = ["instagram", "youtube", "facebook"]
    person_ids: list[str] = []
    for value in payload.person_ids:
        raw = str(value or "").strip()
        if not raw:
            continue
        try:
            person_ids.append(str(UUID(raw)))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"Invalid person_id: {raw}") from exc
    account_handles = sorted(
        {str(value or "").strip() for value in payload.account_handles if str(value or "").strip()}
    )
    if not person_ids and not account_handles:
        return {"rows": []}

    try:
        rows = pg.fetch_all(
            """
            SELECT
              id::text AS id,
              person_id::text AS person_id,
              platform,
              account_handle,
              scraped_at,
              updated_at,
              created_at,
              stats_refreshed,
              raw_response->>'socialblade_url' AS socialblade_url
            FROM pipeline.socialblade_growth_data
            WHERE platform = ANY(%s::text[])
              AND (
                person_id = ANY(%s::uuid[])
                OR account_handle = ANY(%s::text[])
              )
            ORDER BY
              platform ASC,
              account_handle ASC,
              person_id ASC NULLS LAST,
              updated_at DESC NULLS LAST,
              scraped_at DESC NULLS LAST,
              created_at DESC NULLS LAST,
              id ASC
            """,
            [platforms, person_ids, account_handles],
            pool_name="social_profile",
        )
        return {"rows": jsonable_encoder(rows)}
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to read social landing SocialBlade rows")
        raise _to_social_read_http_exception(exc) from exc


@router.post("/landing-socialblade-progress-counts")
def post_social_landing_socialblade_progress_counts(
    payload: SocialLandingSocialBladeProgressCountsRequest,
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.db import pg

    targets = _normalize_landing_progress_targets(payload.platforms, payload.account_handles)

    if not targets:
        return {"rows": []}

    try:
        rows = pg.fetch_all(
            """
            WITH targets AS (
              SELECT DISTINCT
                lower(input.platform) AS platform,
                lower(regexp_replace(input.account_handle, '^@+', '')) AS account_handle
              FROM unnest(%s::text[], %s::text[]) AS input(platform, account_handle)
              WHERE input.platform IN ('instagram', 'tiktok', 'twitter', 'youtube', 'facebook', 'threads')
                AND nullif(trim(input.account_handle), '') IS NOT NULL
            )
            SELECT
              targets.platform,
              targets.account_handle,
              (targets.platform IN ('instagram', 'youtube', 'tiktok'))::boolean AS socialblade_supported,
              count(growth.id)::int AS socialblade_scraped_count,
              count(growth.id) filter (where coalesce(growth.stats_refreshed, false) = true)::int
                AS socialblade_saved_count
            FROM targets
            LEFT JOIN pipeline.socialblade_growth_data growth
              ON lower(coalesce(nullif(growth.platform, ''), 'instagram')) = targets.platform
             AND lower(
               regexp_replace(
                 coalesce(nullif(growth.account_handle, ''), growth.instagram_handle, ''),
                 '^@+',
                 ''
               )
             ) = targets.account_handle
            GROUP BY targets.platform, targets.account_handle
            ORDER BY targets.platform ASC, targets.account_handle ASC
            """,
            [[platform for platform, _handle in targets], [handle for _platform, handle in targets]],
            pool_name="social_profile",
        )
        return {"rows": jsonable_encoder(rows)}
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to read social landing SocialBlade progress counts")
        raise _to_social_read_http_exception(exc) from exc


@router.post("/landing-progress-rollup")
def post_social_landing_progress_rollup(
    payload: SocialLandingProgressRollupRequest,
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.db import pg

    targets = _normalize_landing_progress_targets(payload.platforms, payload.account_handles)
    if not targets:
        return {
            "rows": [],
            "cache_status": "bypass",
            "generated_at": datetime.now(tz=UTC).isoformat(),
        }

    cache_key = _landing_progress_cache_key(targets)
    cached_payload = _get_ttl_cached_payload(
        _SOCIAL_LANDING_PROGRESS_ROLLUP_CACHE,
        _SOCIAL_LANDING_PROGRESS_ROLLUP_CACHE_LOCK,
        cache_key,
    )
    if cached_payload is not None:
        cached_payload["cache_status"] = "hit"
        return cached_payload

    started_at = perf_counter()
    instagram_reported_comments_sql = _instagram_reported_comments_sql("p")
    try:
        rows = pg.fetch_all(
            f"""
            WITH targets AS (
              SELECT DISTINCT
                lower(input.platform) AS platform,
                ltrim(lower(input.account_handle), '@') AS account_handle
              FROM unnest(%s::text[], %s::text[]) AS input(platform, account_handle)
              WHERE input.platform IN ('instagram', 'tiktok', 'twitter', 'youtube', 'facebook', 'threads')
                AND nullif(trim(input.account_handle), '') IS NOT NULL
            ),
            materialized_rows AS (
              SELECT
                targets.platform,
                targets.account_handle,
                p.id,
                nullif(p.shortcode, '') AS source_id,
                ({instagram_reported_comments_sql})::bigint AS reported_comments,
                jsonb_array_length(coalesce(p.media_urls, '[]'::jsonb))::bigint AS source_media_files,
                (
                  jsonb_array_length(coalesce(p.hosted_media_urls, '[]'::jsonb)) +
                  case when nullif(p.hosted_thumbnail_url, '') is not null then 1 else 0 end
                )::bigint AS hosted_media_files
              FROM targets
              INNER JOIN social.instagram_posts p
                ON targets.platform = 'instagram'
               AND ltrim(lower(coalesce(p.source_account, '')), '@') = targets.account_handle
              UNION ALL
              SELECT
                targets.platform,
                targets.account_handle,
                p.id,
                nullif(p.video_id, '') AS source_id,
                greatest(coalesce(p.comments_count, 0), 0)::bigint AS reported_comments,
                jsonb_array_length(coalesce(p.media_urls, '[]'::jsonb))::bigint AS source_media_files,
                (
                  jsonb_array_length(coalesce(p.hosted_media_urls, '[]'::jsonb)) +
                  case when nullif(p.hosted_thumbnail_url, '') is not null then 1 else 0 end
                )::bigint AS hosted_media_files
              FROM targets
              INNER JOIN social.tiktok_posts p
                ON targets.platform = 'tiktok'
               AND ltrim(lower(coalesce(p.source_account, '')), '@') = targets.account_handle
              UNION ALL
              SELECT
                targets.platform,
                targets.account_handle,
                p.id,
                nullif(p.tweet_id, '') AS source_id,
                0::bigint AS reported_comments,
                jsonb_array_length(coalesce(p.media_urls, '[]'::jsonb))::bigint AS source_media_files,
                (
                  jsonb_array_length(coalesce(p.hosted_media_urls, '[]'::jsonb)) +
                  case when nullif(p.hosted_thumbnail_url, '') is not null then 1 else 0 end
                )::bigint AS hosted_media_files
              FROM targets
              INNER JOIN social.twitter_tweets p
                ON targets.platform = 'twitter'
               AND ltrim(lower(coalesce(p.source_account, '')), '@') = targets.account_handle
              UNION ALL
              SELECT
                targets.platform,
                targets.account_handle,
                p.id,
                nullif(p.video_id, '') AS source_id,
                greatest(coalesce(p.comments_count, 0), 0)::bigint AS reported_comments,
                0::bigint AS source_media_files,
                case when nullif(p.hosted_thumbnail_url, '') is not null then 1 else 0 end::bigint AS hosted_media_files
              FROM targets
              INNER JOIN social.youtube_videos p
                ON targets.platform = 'youtube'
               AND ltrim(lower(coalesce(p.source_account, '')), '@') = targets.account_handle
              UNION ALL
              SELECT
                targets.platform,
                targets.account_handle,
                p.id,
                nullif(p.post_id, '') AS source_id,
                greatest(coalesce(p.comments_count, 0), 0)::bigint AS reported_comments,
                jsonb_array_length(coalesce(p.media_urls, '[]'::jsonb))::bigint AS source_media_files,
                (
                  jsonb_array_length(coalesce(p.hosted_media_urls, '[]'::jsonb)) +
                  case when nullif(p.hosted_thumbnail_url, '') is not null then 1 else 0 end
                )::bigint AS hosted_media_files
              FROM targets
              INNER JOIN social.facebook_posts p
                ON targets.platform = 'facebook'
               AND ltrim(lower(coalesce(p.source_account, '')), '@') = targets.account_handle
              UNION ALL
              SELECT
                targets.platform,
                targets.account_handle,
                p.id,
                nullif(p.post_id, '') AS source_id,
                0::bigint AS reported_comments,
                jsonb_array_length(coalesce(p.media_urls, '[]'::jsonb))::bigint AS source_media_files,
                (
                  jsonb_array_length(coalesce(p.hosted_media_urls, '[]'::jsonb)) +
                  case when nullif(p.hosted_thumbnail_url, '') is not null then 1 else 0 end
                )::bigint AS hosted_media_files
              FROM targets
              INNER JOIN social.meta_threads_posts p
                ON targets.platform = 'threads'
               AND ltrim(lower(coalesce(p.source_account, '')), '@') = targets.account_handle
            ),
            catalog_rows AS (
              SELECT
                targets.platform,
                targets.account_handle,
                nullif(p.source_id, '') AS source_id,
                ({instagram_reported_comments_sql})::bigint AS reported_comments,
                jsonb_array_length(coalesce(p.media_urls, '[]'::jsonb))::bigint AS source_media_files
              FROM targets
              INNER JOIN social.instagram_account_catalog_posts p
                ON targets.platform = 'instagram'
               AND ltrim(lower(coalesce(p.source_account, '')), '@') = targets.account_handle
              UNION ALL
              SELECT
                targets.platform,
                targets.account_handle,
                nullif(p.source_id, '') AS source_id,
                greatest(coalesce(p.comments_count, 0), 0)::bigint AS reported_comments,
                jsonb_array_length(coalesce(p.media_urls, '[]'::jsonb))::bigint AS source_media_files
              FROM targets
              INNER JOIN social.tiktok_account_catalog_posts p
                ON targets.platform = 'tiktok'
               AND ltrim(lower(coalesce(p.source_account, '')), '@') = targets.account_handle
              UNION ALL
              SELECT
                targets.platform,
                targets.account_handle,
                nullif(p.source_id, '') AS source_id,
                greatest(coalesce(p.comments_count, 0), 0)::bigint + greatest(coalesce(p.quotes, 0), 0)::bigint
                  AS reported_comments,
                jsonb_array_length(coalesce(p.media_urls, '[]'::jsonb))::bigint AS source_media_files
              FROM targets
              INNER JOIN social.twitter_account_catalog_posts p
                ON targets.platform = 'twitter'
               AND ltrim(lower(coalesce(p.source_account, '')), '@') = targets.account_handle
              UNION ALL
              SELECT
                targets.platform,
                targets.account_handle,
                nullif(p.source_id, '') AS source_id,
                greatest(coalesce(p.comments_count, 0), 0)::bigint AS reported_comments,
                jsonb_array_length(coalesce(p.media_urls, '[]'::jsonb))::bigint AS source_media_files
              FROM targets
              INNER JOIN social.youtube_account_catalog_posts p
                ON targets.platform = 'youtube'
               AND ltrim(lower(coalesce(p.source_account, '')), '@') = targets.account_handle
              UNION ALL
              SELECT
                targets.platform,
                targets.account_handle,
                nullif(p.source_id, '') AS source_id,
                greatest(coalesce(p.comments_count, 0), 0)::bigint AS reported_comments,
                jsonb_array_length(coalesce(p.media_urls, '[]'::jsonb))::bigint AS source_media_files
              FROM targets
              INNER JOIN social.facebook_account_catalog_posts p
                ON targets.platform = 'facebook'
               AND ltrim(lower(coalesce(p.source_account, '')), '@') = targets.account_handle
              UNION ALL
              SELECT
                targets.platform,
                targets.account_handle,
                nullif(p.source_id, '') AS source_id,
                greatest(coalesce(p.comments_count, 0), 0)::bigint AS reported_comments,
                jsonb_array_length(coalesce(p.media_urls, '[]'::jsonb))::bigint AS source_media_files
              FROM targets
              INNER JOIN social.threads_account_catalog_posts p
                ON targets.platform = 'threads'
               AND ltrim(lower(coalesce(p.source_account, '')), '@') = targets.account_handle
            ),
            materialized_counts AS (
              SELECT
                rows.platform,
                rows.account_handle,
                count(*)::int AS saved_count,
                sum(rows.reported_comments)::int AS comments_total_count,
                sum(rows.hosted_media_files)::int AS media_saved_count
              FROM materialized_rows rows
              GROUP BY rows.platform, rows.account_handle
            ),
            catalog_counts AS (
              SELECT
                rows.platform,
                rows.account_handle,
                count(*)::int AS scraped_count,
                sum(rows.reported_comments)::int AS comments_total_count,
                sum(rows.source_media_files)::int AS media_total_count
              FROM catalog_rows rows
              GROUP BY rows.platform, rows.account_handle
            ),
            instagram_profile_targets AS (
              SELECT DISTINCT ON (targets.account_handle)
                targets.account_handle,
                profiles.id AS profile_id,
                greatest(coalesce(profiles.follows_count, 0), 0)::int AS following_total_count
              FROM targets
              INNER JOIN social.instagram_profiles profiles
                ON targets.platform = 'instagram'
               AND ltrim(
                 lower(coalesce(profiles.normalized_username, profiles.username, profiles.source_account, '')),
                 '@'
               ) = targets.account_handle
              ORDER BY
                targets.account_handle,
                profiles.last_scraped_at DESC NULLS LAST,
                profiles.updated_at DESC NULLS LAST,
                profiles.id
            ),
            following_counts AS (
              SELECT
                'instagram'::text AS platform,
                profile_targets.account_handle,
                count(relationships.id) FILTER (WHERE coalesce(relationships.is_missing, false) = false)::int
                  AS following_saved_count,
                max(profile_targets.following_total_count)::int AS following_total_count
              FROM instagram_profile_targets profile_targets
              LEFT JOIN social.instagram_profile_relationships relationships
                ON relationships.owner_profile_id = profile_targets.profile_id
               AND relationships.relationship_type = 'following'
              GROUP BY profile_targets.account_handle
            ),
            socialblade_counts AS (
              SELECT
                targets.platform,
                targets.account_handle,
                count(growth.id)::int AS socialblade_scraped_count,
                count(growth.id) FILTER (WHERE coalesce(growth.stats_refreshed, false) = true)::int
                  AS socialblade_saved_count
              FROM targets
              LEFT JOIN pipeline.socialblade_growth_data growth
                ON lower(coalesce(nullif(growth.platform, ''), 'instagram')) = targets.platform
               AND ltrim(
                 lower(coalesce(nullif(growth.account_handle, ''), growth.instagram_handle, '')),
                 '@'
               ) = targets.account_handle
              GROUP BY targets.platform, targets.account_handle
            )
            SELECT
              targets.platform,
              targets.account_handle,
              coalesce(materialized_counts.saved_count, 0)::int AS saved_count,
              coalesce(catalog_counts.scraped_count, 0)::int AS scraped_count,
              (targets.platform IN ('instagram', 'youtube', 'tiktok'))::boolean AS socialblade_supported,
              coalesce(socialblade_counts.socialblade_scraped_count, 0)::int AS socialblade_scraped_count,
              coalesce(socialblade_counts.socialblade_saved_count, 0)::int AS socialblade_saved_count,
              coalesce(following_counts.following_saved_count, 0)::int AS following_saved_count,
              coalesce(following_counts.following_total_count, 0)::int AS following_total_count,
              0::int AS comments_saved_count,
              greatest(
                coalesce(materialized_counts.comments_total_count, 0),
                coalesce(catalog_counts.comments_total_count, 0)
              )::int AS comments_total_count,
              coalesce(materialized_counts.media_saved_count, 0)::int AS media_saved_count,
              greatest(
                coalesce(catalog_counts.media_total_count, 0),
                coalesce(materialized_counts.media_saved_count, 0)
              )::int AS media_total_count
            FROM targets
            LEFT JOIN materialized_counts
              ON materialized_counts.platform = targets.platform
             AND materialized_counts.account_handle = targets.account_handle
            LEFT JOIN catalog_counts
              ON catalog_counts.platform = targets.platform
             AND catalog_counts.account_handle = targets.account_handle
            LEFT JOIN following_counts
              ON following_counts.platform = targets.platform
             AND following_counts.account_handle = targets.account_handle
            LEFT JOIN socialblade_counts
              ON socialblade_counts.platform = targets.platform
             AND socialblade_counts.account_handle = targets.account_handle
            ORDER BY targets.platform ASC, targets.account_handle ASC
            """,
            [[platform for platform, _handle in targets], [handle for _platform, handle in targets]],
            pool_name="social_profile",
        )
        generated_at = datetime.now(tz=UTC).isoformat()
        payload_out = {
            "rows": jsonable_encoder(rows),
            "cache_status": "miss",
            "generated_at": generated_at,
        }
        _set_ttl_cached_payload(
            _SOCIAL_LANDING_PROGRESS_ROLLUP_CACHE,
            _SOCIAL_LANDING_PROGRESS_ROLLUP_CACHE_LOCK,
            cache_key,
            payload_out,
            ttl_seconds=SOCIAL_LANDING_PROGRESS_ROLLUP_CACHE_TTL_SECONDS,
            max_entries=SOCIAL_LANDING_PROGRESS_ROLLUP_CACHE_MAX_ENTRIES,
        )
        logger.info(
            "Built social landing progress rollup targets=%s rows=%s elapsed_ms=%s cache_status=miss",
            len(targets),
            len(rows),
            int((perf_counter() - started_at) * 1000),
        )
        return payload_out
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to read social landing progress rollup")
        raise _to_social_read_http_exception(exc) from exc


def _raise_if_modal_social_dispatch_unresolvable(platform: str | None = None) -> None:
    resolution = resolve_modal_function(modal_social_job_function_name())
    if bool(resolution.get("resolved")):
        return
    platform_label = str(platform or "social").strip().lower() or "social"
    raise HTTPException(
        status_code=503,
        detail={
            "code": "SOCIAL_MODAL_DISPATCH_UNAVAILABLE",
            "message": (
                f"{platform_label.capitalize()} shared-account catalog dispatch is configured for Modal, "
                "but the configured Modal target could not be resolved."
            ),
            "reason": resolution.get("reason") or "modal_resolution_failed",
            "resolution_error": resolution.get("error"),
            "configured_app_name": resolution.get("app_name") or modal_app_name(),
            "configured_function_name": resolution.get("function_name") or modal_social_job_function_name(),
            "modal_environment": resolution.get("modal_environment") or modal_environment_name(),
            "required_execution_backend": "modal",
            "execution_mode": canonical_execution_mode(),
            "execution_owner": execution_owner_label(),
        },
    )


_WEEK_DETAIL_DEFAULT_MAX_COMMENTS_PER_POST = 0
_WEEK_DETAIL_DEFAULT_POST_LIMIT = 20
_WEEK_DETAIL_DEFAULT_POST_OFFSET = 0
_INGEST_JOBS_DEFAULT_LIMIT = 50
_INGEST_JOBS_MAX_LIMIT = 250
_INGEST_JOBS_DEFAULT_OFFSET = 0
_INGEST_JOBS_MAX_OFFSET = 5000


def _env_truthy(name: str) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(minimum, min(maximum, value))


def _comments_run_workers_cap() -> int:
    legacy_default = _env_int("SOCIAL_INLINE_COMMENTS_WORKERS", 4, minimum=1, maximum=8)
    return _env_int("SOCIAL_COMMENTS_RUN_WORKERS", legacy_default, minimum=1, maximum=8)


# Default timeout (seconds) for inline dev-fallback execution.
# Configurable via SOCIAL_INLINE_EXECUTION_TIMEOUT_SECONDS env var.
_INLINE_EXECUTION_TIMEOUT_SECONDS_DEFAULT = 600


def _inline_execution_timeout_seconds() -> int:
    return _env_int(
        "SOCIAL_INLINE_EXECUTION_TIMEOUT_SECONDS",
        _INLINE_EXECUTION_TIMEOUT_SECONDS_DEFAULT,
        minimum=30,
        maximum=7200,
    )


async def _run_admin_repo_call(func: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Any:
    """Keep async admin routes from blocking the event loop on sync repository work."""
    return await run_in_threadpool(func, *args, **kwargs)


def _execute_with_timeout(
    func: Any,
    args: tuple[Any, ...] = (),
    kwargs: dict[str, Any] | None = None,
    timeout_seconds: int = 600,
) -> Any:
    """Execute *func* in a daemon thread with a hard timeout.

    Raises ``TimeoutError`` if the function does not complete within
    *timeout_seconds*.  Any exception raised by *func* is re-raised in the
    calling thread.
    """
    kwargs = kwargs or {}
    result: list[Any] = [None]
    exception: list[BaseException | None] = [None]

    def _target() -> None:
        try:
            result[0] = func(*args, **kwargs)
        except BaseException as exc:  # noqa: BLE001
            exception[0] = exc

    thread = Thread(target=_target, daemon=True)
    thread.start()
    thread.join(timeout=timeout_seconds)
    if thread.is_alive():
        raise TimeoutError(f"Inline execution exceeded {timeout_seconds}s timeout")
    if exception[0] is not None:
        raise exception[0]
    return result[0]


SocialExecutionMode = Literal["queued", "inline"]
SocialExecutionModeCanonical = Literal["queued", "inline", "inline_fallback"]
SocialExecutionModeLegacy = Literal["queue", "inline", "inline_fallback"]


def _resolve_social_execution_modes(
    *,
    queue_enabled: bool,
    used_inline_fallback: bool = False,
) -> tuple[SocialExecutionMode, SocialExecutionModeCanonical, SocialExecutionModeLegacy]:
    execution_mode: SocialExecutionMode = "queued" if queue_enabled else "inline"
    execution_mode_canonical: SocialExecutionModeCanonical = (
        "inline_fallback" if used_inline_fallback else execution_mode
    )
    execution_mode_legacy: SocialExecutionModeLegacy = (
        "queue"
        if execution_mode_canonical == "queued"
        else ("inline_fallback" if execution_mode_canonical == "inline_fallback" else "inline")
    )
    return execution_mode, execution_mode_canonical, execution_mode_legacy


def _social_execution_mode_deprecation_payload() -> dict[str, Any]:
    return {
        "field": "execution_mode_legacy",
        "message": "execution_mode_legacy is deprecated; use execution_mode_canonical.",
    }


def _start_runs_in_background(
    run_ids: list[str],
    background_tasks: BackgroundTasks,
    *,
    worker_prefix: str,
    stage: str | None = None,
    platform: str | None = None,
    supported_platforms: list[str] | None = None,
    metadata_updates: Mapping[str, Any] | None = None,
) -> None:
    from trr_backend.repositories.social_season_analytics import execute_run_with_inline_worker_registration

    def _runner() -> None:
        for index, run_id in enumerate(run_ids, start=1):
            worker_id = worker_prefix if len(run_ids) == 1 else f"{worker_prefix}:{index}"
            execute_run_with_inline_worker_registration(
                run_id,
                worker_id=worker_id,
                stage=stage,
                platform=platform,
                supported_platforms=supported_platforms,
                metadata_updates=metadata_updates,
            )

    if not run_ids:
        return
    background_tasks.add_task(_runner)


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
) -> None:
    from trr_backend.repositories.social_season_analytics import (
        INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
        INSTAGRAM_COMMENTS_SCRAPLING_WORKER_LANE,
        SocialIngestConflictError,
        _build_attached_comments_followup,
        _merge_catalog_run_config,
        _metadata_dict,
        execute_run_with_inline_worker_registration,
        finalize_social_account_catalog_backfill_launch,
        is_queue_enabled,
        start_social_account_comments_scrape,
    )

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
        result = finalize_social_account_catalog_backfill_launch(
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
        )
        if allow_local_dev_inline_bypass or not is_queue_enabled():
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
                        target=execute_run_with_inline_worker_registration,
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
                    start_social_account_comments_scrape=start_social_account_comments_scrape,
                    social_ingest_conflict_error=SocialIngestConflictError,
                    merge_catalog_run_config=_merge_catalog_run_config,
                    metadata_dict=_metadata_dict,
                    build_attached_comments_followup=_build_attached_comments_followup,
                    comments_worker_count=comments_worker_count,
                    comments_enable_media_followups=comments_enable_media_followups,
                )
            if comments_run_id:
                if not comments_worker_threads:
                    comments_worker_threads = _start_local_comments_inline_workers(
                        comments_run_id=comments_run_id,
                        normalized_platform=normalized_platform,
                        result=result,
                        execute_run_with_inline_worker_registration=execute_run_with_inline_worker_registration,
                        comments_stage=INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
                        comments_worker_lane=INSTAGRAM_COMMENTS_SCRAPLING_WORKER_LANE,
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
        },
        name=f"catalog-backfill-finalize:{platform}:{account_handle}:{run_id}",
        daemon=True,
    )
    thread.start()


def _cancel_catalog_run_in_background(
    *,
    platform: str,
    account_handle: str,
    run_id: str,
    cancelled_by: str | None,
) -> None:
    from trr_backend.repositories.social_season_analytics import (
        cancel_social_account_catalog_run,
        reconcile_cancelled_shared_run,
    )

    normalized_platform = str(platform or "").strip().lower()
    normalized_account = str(account_handle or "").strip().lower()
    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        return

    def _runner() -> None:
        logger.info(
            "[catalog-cancel] finalize_start platform=%s account=%s run_id=%s",
            normalized_platform,
            normalized_account,
            normalized_run_id,
        )
        try:
            cancel_social_account_catalog_run(
                platform=normalized_platform,
                account_handle=normalized_account,
                run_id=normalized_run_id,
                cancelled_by=cancelled_by,
                reconcile_summary=False,
            )
        finally:
            try:
                reconcile_cancelled_shared_run(normalized_run_id)
            except Exception:  # noqa: BLE001
                logger.exception(
                    "[catalog-cancel] reconcile_failed platform=%s account=%s run_id=%s",
                    normalized_platform,
                    normalized_account,
                    normalized_run_id,
                )
            _clear_account_profile_caches()

    Thread(
        target=_runner,
        name=f"catalog-cancel:{normalized_platform}:{normalized_account[:24]}",
        daemon=True,
    ).start()


def _normalize_run_summary_payload(summary: Any) -> dict[str, Any]:
    payload = dict(summary) if isinstance(summary, dict) else {}
    if not payload:
        return {}
    total_jobs = int(payload.get("total_jobs") or 0)
    completed_jobs = int(payload.get("completed_jobs") or 0)
    failed_jobs = int(payload.get("failed_jobs") or 0)
    active_jobs = int(payload.get("active_jobs") or 0)
    if completed_jobs == 0 and total_jobs > 0:
        completed_jobs = max(0, total_jobs - failed_jobs - active_jobs)
    payload["completed_jobs"] = completed_jobs
    return payload


def _normalize_target_platforms(platforms: list[str] | None) -> list[str]:
    return _normalize_inline_target_platforms(platforms, supported_platforms=SOCIAL_SUPPORTED_PLATFORMS)


def _remote_only_social_platforms() -> set[str]:
    default_platforms = ",".join(SOCIAL_SUPPORTED_PLATFORMS)
    raw = str(os.getenv("SOCIAL_REMOTE_ONLY_PLATFORMS") or default_platforms).strip().lower()
    if not raw or raw in {"none", "off", "disabled"}:
        return set()
    return {
        token.strip() for token in raw.split(",") if token.strip() and token.strip() in set(SOCIAL_SUPPORTED_PLATFORMS)
    }


def _blocked_remote_only_platforms(platforms: list[str] | None) -> list[str]:
    requested_platforms = set(_normalize_target_platforms(platforms))
    return sorted(requested_platforms & _remote_only_social_platforms())


def _run_inline_season_ingest(
    run_id: str,
    *,
    platforms: list[str] | None,
    ingest_mode: str,
    worker_prefix: str,
) -> None:
    from trr_backend.repositories.social_season_analytics import execute_run

    run_inline_season_ingest_execution(
        run_id,
        platforms=platforms,
        supported_platforms=SOCIAL_SUPPORTED_PLATFORMS,
        ingest_mode=ingest_mode,
        worker_prefix=worker_prefix,
        comments_workers_cap=_comments_run_workers_cap(),
        execute_run=execute_run,
        thread_pool_executor_factory=ThreadPoolExecutor,
    )


def _parse_utc_iso_datetime(value: str | None) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _resolve_ingest_window(
    *,
    season_id: str,
    source_scope: str,
    week_index: int | None,
    timezone: str,
    date_start: datetime | None,
    date_end: datetime | None,
) -> tuple[datetime | None, datetime | None, dict[str, Any] | None]:
    if week_index is None:
        return date_start, date_end, None

    from trr_backend.repositories.social_season_analytics import resolve_week_window

    week_payload = resolve_week_window(
        season_id,
        week_index=week_index,
        source_scope=source_scope,
        timezone=timezone,
    )
    return (
        _parse_utc_iso_datetime(str(week_payload.get("start") or "")),
        _parse_utc_iso_datetime(str(week_payload.get("end") or "")),
        week_payload,
    )


def _build_ingest_scope_payload(
    *,
    resolved_week: dict[str, Any] | None,
    date_start: datetime | None,
    date_end: datetime | None,
    platforms: list[str] | None,
) -> dict[str, Any]:
    scope_type = "season"
    if resolved_week is not None:
        scope_type = "week"
    elif date_start is not None or date_end is not None:
        scope_type = "date_window"
    return {
        "type": scope_type,
        "platforms": _normalize_target_platforms(platforms),
        "week": resolved_week,
        "date_start": date_start.isoformat().replace("+00:00", "Z") if isinstance(date_start, datetime) else None,
        "date_end": date_end.isoformat().replace("+00:00", "Z") if isinstance(date_end, datetime) else None,
    }


def _parse_platform_query(platforms: str | None) -> list[str] | None:
    if not platforms or not platforms.strip():
        return None

    requested = [item.strip().lower() for item in platforms.split(",") if item.strip()]
    if not requested:
        raise HTTPException(
            status_code=400,
            detail={"code": "INVALID_PLATFORM_FILTER", "message": "No valid platforms were provided"},
        )

    unsupported = sorted({item for item in requested if item not in SOCIAL_SUPPORTED_PLATFORMS})
    if unsupported:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "INVALID_PLATFORM_FILTER",
                "message": f"Unsupported platforms: {', '.join(unsupported)}",
            },
        )

    deduped: list[str] = []
    for platform in requested:
        if platform in deduped:
            continue
        deduped.append(platform)
    return deduped


def _value_error_to_bad_request(exc: ValueError) -> HTTPException:
    message = str(exc)
    if message.upper().startswith("INVALID_PLATFORM_FILTER"):
        return HTTPException(
            status_code=400,
            detail={"code": "INVALID_PLATFORM_FILTER", "message": message.split(":", 1)[-1].strip() or message},
        )
    return HTTPException(status_code=400, detail=message)


def _lookup_error_to_not_found(exc: LookupError) -> HTTPException:
    return HTTPException(status_code=404, detail=str(exc) or "Not found")


def _to_social_read_http_exception(error: Exception) -> HTTPException:
    if isinstance(error, ValueError):
        return _value_error_to_bad_request(error)
    if is_database_service_unavailable_error(error):
        return HTTPException(status_code=503, detail=database_service_unavailable_detail(error))
    return HTTPException(status_code=500, detail=str(error) or "Internal server error")


def _worker_health_detail(worker_health: Any) -> Any:
    return jsonable_encoder(worker_health) if worker_health is not None else None


def _remote_worker_unavailable_message(
    exc: Exception,
    *,
    default_message: str = (
        "Social ingest remote-worker ownership is enforced and no healthy worker is currently reporting heartbeats."
    ),
) -> str:
    exc_message = str(exc or "").strip()
    if not exc_message:
        return default_message
    return f"Social ingest remote-worker ownership is enforced. {exc_message.removesuffix('.')}."


def _is_local_or_dev_runtime() -> bool:
    runtime_markers = [
        os.getenv("APP_ENV"),
        os.getenv("ENV"),
        os.getenv("ENVIRONMENT"),
        os.getenv("TRR_ENV"),
        os.getenv("TRR_ENVIRONMENT"),
    ]
    normalized = {str(value or "").strip().lower() for value in runtime_markers if str(value or "").strip()}
    if normalized & {"local", "dev", "development", "test"}:
        return True

    if _env_truthy("TRR_LOCAL_DEV") or _env_truthy("SOCIAL_ALLOW_INLINE_DEV_FALLBACK"):
        return True

    if canonical_execution_mode() == "local" and not is_remote_job_plane_enabled():
        return True

    return False


def _can_use_local_catalog_inline_fallback(
    *,
    allow_inline_dev_fallback: bool,
    remote_plane_enforced: bool,
    requires_modal_executor: bool = False,
    explicit_local_preference: bool = False,
) -> bool:
    if not _is_local_or_dev_runtime():
        return False
    if requires_modal_executor and not explicit_local_preference:
        return False
    if _env_truthy("TRR_ALLOW_LOCAL_ADMIN_OPERATION_OVERRIDE"):
        return True
    return bool(allow_inline_dev_fallback) and not remote_plane_enforced


def _resolve_social_account_catalog_route_execution(
    *,
    platform: str,
    allow_inline_dev_fallback: bool,
    execution_preference: Literal["auto", "prefer_local_inline"] = "auto",
    pipeline_ingest_mode: str = "shared_account_catalog_backfill",
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import (
        SocialWorkerUnavailableError,
        _shared_account_catalog_requires_modal_executor,
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


def _resolve_social_account_comments_route_execution(
    *,
    allow_inline_dev_fallback: bool,
    platform: str = "instagram",
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import (
        INSTAGRAM_COMMENTS_SCRAPLING_WORKER_LANE,
        SocialWorkerUnavailableError,
        assert_worker_available_when_queue_enabled,
        is_queue_enabled,
    )

    queue_enabled = is_queue_enabled()
    remote_plane_enforced = is_remote_job_plane_enabled()
    used_inline_fallback = False
    requires_modal_executor = is_modal_remote_executor_enabled()
    can_use_local_inline_fallback = _can_use_local_catalog_inline_fallback(
        allow_inline_dev_fallback=allow_inline_dev_fallback,
        remote_plane_enforced=remote_plane_enforced,
        requires_modal_executor=requires_modal_executor,
    )

    if queue_enabled:
        try:
            assert_worker_available_when_queue_enabled(
                required_worker_lane=None if requires_modal_executor else INSTAGRAM_COMMENTS_SCRAPLING_WORKER_LANE,
                required_execution_backend="modal" if requires_modal_executor else None,
                platform=platform,
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
                            "Instagram comments scraping requires the Modal remote executor."
                            if requires_modal_executor
                            else _remote_worker_unavailable_message(exc)
                            if remote_plane_enforced
                            else str(exc)
                        ),
                        "execution_mode": canonical_execution_mode(),
                        "execution_owner": execution_owner_label(),
                        "required_execution_backend": "modal" if requires_modal_executor else None,
                        "worker_health": _worker_health_detail(exc.worker_health),
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
                    "Instagram comments scraping requires the Modal remote executor."
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
        from trr_backend.repositories.social_season_analytics import (
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
    }


def _load_social_auth_or_503(
    *,
    platform: str,
    surface: str,
    loader: Callable[[], Any],
) -> Any:
    try:
        credentials = loader()
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=503,
            detail={
                "code": f"{platform.upper()}_AUTH_PRECHECK_FAILED",
                "message": f"{platform.title()} {surface} requires configured auth artifacts before it can run.",
                "platform": platform,
                "surface": surface,
                "reason": str(exc),
            },
        ) from exc

    has_credentials = False
    if isinstance(credentials, tuple):
        has_credentials = any(bool(item) for item in credentials)
    elif isinstance(credentials, dict):
        has_credentials = any(bool(key) and value not in (None, "", [], {}, ()) for key, value in credentials.items())
    else:
        has_credentials = bool(credentials)
    if not has_credentials:
        raise HTTPException(
            status_code=503,
            detail={
                "code": f"{platform.upper()}_AUTH_REQUIRED",
                "message": (
                    f"{platform.title()} {surface} requires configured cookies or auth tokens before it can run."
                ),
                "platform": platform,
                "surface": surface,
            },
        )
    return credentials


# Request/Response Models


class InstagramScrapeRequest(BaseModel):
    """Request to scrape Instagram posts."""

    username: str = Field(..., description="Instagram username to scrape (without @)")
    hashtags: list[str] = Field(..., description="Hashtags to filter by (without #)")
    date_start: datetime = Field(..., description="Start date for filtering")
    date_end: datetime = Field(..., description="End date for filtering")
    delay_seconds: float = Field(default=2.0, ge=0.5, le=10.0, description="Delay between requests")
    max_pages: int | None = Field(default=None, ge=1, le=500, description="Maximum pages to fetch")

    # Optional metadata for association
    show_id: UUID | None = Field(default=None, description="Associated show ID")
    season_number: int | None = Field(default=None, ge=0, le=100, description="Associated season")
    person_id: UUID | None = Field(default=None, description="Associated person ID")
    allow_inline_dev_fallback: bool = Field(default=False)


class InstagramPostResponse(BaseModel):
    """Single Instagram post in response."""

    shortcode: str
    post_type: str
    date_time: str
    caption: str
    profile_tags: list[str]
    sponsored: bool
    likes: int
    comments: int
    video_views: int
    url: str
    username: str


class InstagramScrapeResponse(BaseModel):
    """Response from Instagram scrape operation."""

    success: bool
    username: str
    posts_found: int
    posts: list[InstagramPostResponse]
    filters_applied: dict
    error: str | None = None


class SocialAccountConfig(BaseModel):
    """Configuration for a social account to track."""

    platform: Literal["instagram", "tiktok", "twitter", "youtube", "facebook", "threads"]
    username: str
    hashtags: list[str] = Field(default=[])
    entity_type: Literal["show", "season", "person"]
    show_id: UUID | None = None
    season_number: int | None = None
    person_id: UUID | None = None


# Endpoints


@router.post("/instagram/scrape", response_model=InstagramScrapeResponse)
async def scrape_instagram(
    request: InstagramScrapeRequest,
    user: InternalAdminUser,
) -> InstagramScrapeResponse:
    """
    Scrape Instagram posts from a profile with optional filtering.

    This is a synchronous endpoint that returns results immediately.
    For large scrapes, consider using the async version.

    Requires admin access (allowlist only).
    """
    from trr_backend.socials.instagram import InstagramScraper, ScrapeConfig

    logger.info(f"Instagram scrape requested by {user.get('email')} for @{request.username}")

    config = ScrapeConfig(
        username=request.username,
        hashtags=request.hashtags,
        date_start=request.date_start,
        date_end=request.date_end,
        delay_seconds=request.delay_seconds,
        max_pages=request.max_pages,
        show_id=request.show_id,
        season_number=request.season_number,
        person_id=request.person_id,
    )

    try:
        from trr_backend.socials.instagram.auth_runtime import _load_instagram_cookies

        cookies = _load_social_auth_or_503(platform="instagram", surface="scrape", loader=_load_instagram_cookies)
        scraper = InstagramScraper(cookies=cookies)
        posts = scraper.scrape(config)

        return InstagramScrapeResponse(
            success=True,
            username=request.username,
            posts_found=len(posts),
            posts=[
                InstagramPostResponse(
                    shortcode=p.shortcode,
                    post_type=p.post_type,
                    date_time=p.date_time,
                    caption=p.caption,
                    profile_tags=p.profile_tags,
                    sponsored=p.sponsored,
                    likes=p.likes,
                    comments=p.comments,
                    video_views=p.video_views,
                    url=p.url,
                    username=p.username,
                )
                for p in posts
            ],
            filters_applied={
                "hashtags": request.hashtags,
                "date_start": request.date_start.isoformat() if request.date_start else None,
                "date_end": request.date_end.isoformat() if request.date_end else None,
            },
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Instagram scrape failed: {e}", exc_info=True)
        return InstagramScrapeResponse(
            success=False,
            username=request.username,
            posts_found=0,
            posts=[],
            filters_applied={},
            error=str(e),
        )


@router.post("/instagram/scrape/async")
async def scrape_instagram_async(
    request: InstagramScrapeRequest,
    background_tasks: BackgroundTasks,
    user: InternalAdminUser,
) -> dict:
    """
    Start an async Instagram scrape operation.

    Returns immediately with a job ID. Results can be polled or will be
    stored in the database when complete.

    Requires admin access (allowlist only).
    """
    from trr_backend.db import pg
    from trr_backend.repositories.social_season_analytics import (
        SocialIngestValidationError,
        SocialWorkerUnavailableError,
        assert_worker_available_when_queue_enabled,
        execute_run,
        ingest_season,
        is_queue_enabled,
    )

    if request.show_id is None or request.season_number is None:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "BAD_REQUEST",
                "message": "show_id and season_number are required for async ingest",
            },
        )

    season_row = pg.fetch_one(
        """
        select id::text as season_id
        from core.seasons
        where show_id = %s::uuid and season_number = %s
        limit 1
        """,
        [str(request.show_id), int(request.season_number)],
    )
    if not season_row or not str(season_row.get("season_id") or "").strip():
        raise HTTPException(
            status_code=404,
            detail={
                "code": "SEASON_NOT_FOUND",
                "message": "No season found for the provided show_id and season_number",
            },
        )

    season_id = str(season_row.get("season_id") or "").strip()
    queue_enabled = is_queue_enabled()
    remote_plane_enforced = is_remote_job_plane_enabled()
    used_inline_fallback = False
    worker_health: dict[str, Any] | None = None
    if queue_enabled:
        try:
            worker_health = await run_in_threadpool(assert_worker_available_when_queue_enabled)
        except SocialWorkerUnavailableError as exc:
            worker_health = exc.worker_health
            if request.allow_inline_dev_fallback and _is_local_or_dev_runtime() and not remote_plane_enforced:
                queue_enabled = False
                used_inline_fallback = True
            else:
                raise HTTPException(
                    status_code=503,
                    detail={
                        "code": (
                            "SOCIAL_REMOTE_JOB_PLANE_ENFORCED" if remote_plane_enforced else "SOCIAL_WORKER_UNAVAILABLE"
                        ),
                        "message": _remote_worker_unavailable_message(exc) if remote_plane_enforced else str(exc),
                        "execution_mode": canonical_execution_mode(),
                        "execution_owner": execution_owner_label(),
                        "worker_health": _worker_health_detail(exc.worker_health),
                    },
                ) from exc
    elif not remote_plane_enforced:
        used_inline_fallback = bool(request.allow_inline_dev_fallback)
    else:
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

    try:
        run_payload = ingest_season(
            season_id,
            platforms=["instagram"],
            accounts_override=[request.username],
            hashtags_override=request.hashtags or [],
            keywords_override=[],
            source_scope="network",
            max_posts_per_target=0,
            max_comments_per_post=0,
            max_replies_per_post=0,
            fetch_replies=False,
            ingest_mode="posts_only",
            sync_strategy="incremental",
            comment_refresh_policy="balanced",
            comment_anchor_source_ids=None,
            date_start=request.date_start,
            date_end=request.date_end,
            initiated_by=user.get("email"),
            inline_worker_id=None if queue_enabled else "api-background",
        )
    except SocialIngestValidationError as exc:
        raise HTTPException(status_code=400, detail={"code": exc.code, "message": str(exc)}) from exc

    run_id = str(run_payload.get("run_id") or "").strip()
    if not run_id:
        raise HTTPException(
            status_code=500,
            detail={"code": "INGEST_RUN_NOT_CREATED", "message": "Failed to create async Instagram ingest run"},
        )

    if not queue_enabled:
        background_tasks.add_task(execute_run, run_id, worker_id="api-background:instagram", platform="instagram")

    execution_mode, execution_mode_canonical, execution_mode_legacy = _resolve_social_execution_modes(
        queue_enabled=queue_enabled,
        used_inline_fallback=used_inline_fallback,
    )
    logger.info("Async Instagram scrape requested by %s - run %s", user.get("email"), run_id)
    response_payload = {
        "job_id": run_id,
        "run_id": run_id,
        "season_id": season_id,
        "status": "queued" if queue_enabled else "started",
        "execution_mode": execution_mode,
        "execution_mode_canonical": execution_mode_canonical,
        "execution_mode_legacy": execution_mode_legacy,
        "execution_owner": execution_owner_label(),
        "execution_backend_canonical": execution_metadata()["execution_backend_canonical"],
        "execution_mode_deprecation": _social_execution_mode_deprecation_payload(),
        "jobs_url": f"/api/v1/admin/socials/seasons/{season_id}/ingest/jobs?run_id={run_id}",
        "runs_url": f"/api/v1/admin/socials/seasons/{season_id}/ingest/runs?run_id={run_id}",
        "message": (
            "Async Instagram ingest run queued. Poll /ingest/jobs with run_id for progress."
            if execution_mode == "queued"
            else "Async Instagram ingest run started inline. Poll /ingest/jobs with run_id for progress."
        ),
    }
    if used_inline_fallback and worker_health is not None:
        response_payload["worker_health"] = worker_health
    return response_payload


@router.get("/instagram/preview/{username}")
async def preview_instagram_profile(
    username: str,
    user: InternalAdminUser,
) -> dict:
    """
    Preview basic info about an Instagram profile.

    Returns profile metadata and recent post count without full scraping.
    Useful for validating usernames before configuring scrape jobs.

    Requires admin access (allowlist only).
    """
    from trr_backend.socials.instagram import InstagramScraper

    logger.info(f"Instagram preview requested by {user.get('email')} for @{username}")

    try:
        scraper = InstagramScraper(cookies={})
        data = scraper.fetch_profile_info(username, delay=0)

        if not data:
            raise HTTPException(status_code=404, detail=f"Profile not found: @{username}")

        user_data = data.get("data", {}).get("user", {})
        if not user_data:
            raise HTTPException(status_code=404, detail=f"Profile not found: @{username}")

        timeline = user_data.get("edge_owner_to_timeline_media", {})

        return {
            "username": user_data.get("username"),
            "full_name": user_data.get("full_name"),
            "biography": user_data.get("biography"),
            "is_verified": user_data.get("is_verified", False),
            "is_private": user_data.get("is_private", False),
            "followers": user_data.get("edge_followed_by", {}).get("count", 0),
            "following": user_data.get("edge_follow", {}).get("count", 0),
            "post_count": timeline.get("count", 0),
            "profile_pic_url": user_data.get("profile_pic_url_hd") or user_data.get("profile_pic_url"),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Instagram preview failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Instagram preview failed",
            headers={"x-error-code": "SOCIAL_PREVIEW_FAILED"},
        ) from e


# TikTok Models


class TikTokScrapeRequest(BaseModel):
    """Request to scrape TikTok posts."""

    username: str = Field(..., description="TikTok username to scrape (without @)")
    hashtags: list[str] = Field(..., description="Hashtags to filter by (without #)")
    date_start: datetime = Field(..., description="Start date for filtering")
    date_end: datetime = Field(..., description="End date for filtering")
    delay_seconds: float = Field(default=2.0, ge=0.5, le=10.0, description="Delay between requests")
    max_pages: int | None = Field(default=None, ge=1, le=500, description="Maximum pages to fetch")

    # Optional metadata for association
    show_id: UUID | None = Field(default=None, description="Associated show ID")
    season_number: int | None = Field(default=None, ge=0, le=100, description="Associated season")
    person_id: UUID | None = Field(default=None, description="Associated person ID")


class TikTokPostResponse(BaseModel):
    """Single TikTok post in response."""

    video_id: str
    date_time: str
    description: str
    hashtags: list[str]
    mentions: list[str]
    likes: int
    comments: int
    shares: int
    views: int
    url: str
    username: str
    author_nickname: str
    duration: int
    music_title: str
    music_author: str


class TikTokScrapeResponse(BaseModel):
    """Response from TikTok scrape operation."""

    success: bool
    username: str
    posts_found: int
    posts: list[TikTokPostResponse]
    filters_applied: dict
    diagnostics: dict[str, Any] | None = None
    error: str | None = None


def _build_tiktok_scrape_diagnostics(retrieval_meta: dict[str, Any]) -> dict[str, Any] | None:
    allowed_keys = (
        "retrieval_mode",
        "http_client",
        "fallback_chain",
        "stop_reason",
        "error_code",
        "risk_state",
        "operator_summary",
        "operator_action",
        "triage_bucket",
        "profile_enrichment_status",
    )
    diagnostics = {key: retrieval_meta[key] for key in allowed_keys if key in retrieval_meta}
    return diagnostics or None


# TikTok Endpoints


@router.post("/tiktok/scrape", response_model=TikTokScrapeResponse)
async def scrape_tiktok(
    request: TikTokScrapeRequest,
    user: InternalAdminUser,
) -> TikTokScrapeResponse:
    """
    Scrape TikTok posts from a profile with optional filtering.

    Requires admin access (allowlist only).
    """
    from trr_backend.repositories.social_season_analytics import _load_tiktok_cookies
    from trr_backend.socials.tiktok.direct_scrape import scrape_tiktok as run_tiktok_scrape

    logger.info(f"TikTok scrape requested by {user.get('email')} for @{request.username}")

    def _load_cookies(surface: str) -> Any:
        return _load_social_auth_or_503(platform="tiktok", surface=surface, loader=_load_tiktok_cookies)

    return TikTokScrapeResponse(**run_tiktok_scrape(request, load_cookies=_load_cookies, logger=logger))


@router.get("/tiktok/preview/{username}")
async def preview_tiktok_profile(
    username: str,
    user: InternalAdminUser,
) -> dict:
    """
    Preview basic info about a TikTok profile.

    Returns profile metadata without full scraping.
    Useful for validating usernames before configuring scrape jobs.

    Requires admin access (allowlist only).
    """
    from trr_backend.repositories.social_season_analytics import _load_tiktok_cookies
    from trr_backend.socials.tiktok.direct_scrape import preview_tiktok_profile as run_tiktok_preview

    logger.info(f"TikTok preview requested by {user.get('email')} for @{username}")

    def _load_cookies(surface: str) -> Any:
        return _load_social_auth_or_503(platform="tiktok", surface=surface, loader=_load_tiktok_cookies)

    return run_tiktok_preview(username, load_cookies=_load_cookies, logger=logger)


# Twitter/X Models


class TwitterSearchRequest(BaseModel):
    """Request to search Twitter/X for tweets."""

    query: str = Field(..., description="Search query (hashtag or phrase, e.g., RHOSLC or #RHOSLC)")
    date_start: datetime = Field(..., description="Start date for search")
    date_end: datetime = Field(..., description="End date for search")
    include_replies: bool = Field(default=False, description="Include reply tweets in results")
    include_links: bool = Field(default=True, description="Include tweets with links")
    mirror_to_s3: bool = Field(default=False, description="Mirror discovered media URLs to S3")
    delay_seconds: float = Field(default=2.0, ge=0.5, le=10.0, description="Delay between requests")
    max_pages: int | None = Field(default=None, ge=1, le=100, description="Maximum pages to fetch")

    # Optional metadata for association
    show_id: UUID | None = Field(default=None, description="Associated show ID")
    season_number: int | None = Field(default=None, ge=0, le=100, description="Associated season")
    person_id: UUID | None = Field(default=None, description="Associated person ID")

    # Persistence options
    persist: bool = Field(default=False, description="Upsert results to social.twitter_tweets")
    scrape_query: str | None = Field(
        default=None,
        description="Label stored on persisted rows; defaults to query value when omitted",
    )


class TweetResponse(BaseModel):
    """Single tweet in response."""

    tweet_id: str
    date_time: str
    text: str
    hashtags: list[str]
    mentions: list[str]
    likes: int
    retweets: int
    replies: int
    quotes: int
    views: int
    bookmarks: int = 0
    shares: int = 0
    url: str
    username: str
    display_name: str
    user_verified: bool
    is_reply: bool
    is_retweet: bool
    is_quote: bool
    thread_root_tweet_id: str | None = None
    thread_position: int | None = None
    is_thread_part: bool = False
    twitter_context_role: str | None = None
    media_urls: list[str]
    hosted_media_urls: list[str] = Field(default_factory=list)


class TwitterSearchResponse(BaseModel):
    """Response from Twitter search operation."""

    success: bool
    query: str
    tweets_found: int
    tweets: list[TweetResponse]
    search_query_used: str
    filters_applied: dict
    retrieval_meta: dict | None = None
    complete: bool = False
    persist_summary: dict | None = None
    scrape_run_id: str | None = None
    error: str | None = None


class TweetRepliesRequest(BaseModel):
    """Request to fetch replies for a tweet."""

    tweet_id: str = Field(..., description="Tweet ID to fetch replies for")
    mirror_to_s3: bool = Field(default=False, description="Mirror discovered media URLs to S3")
    delay_seconds: float = Field(default=2.0, ge=0.5, le=10.0, description="Delay between requests")
    search_max_pages: int | None = Field(default=None, ge=1, le=100, description="Maximum search fallback pages")
    twikit_max_pages: int | None = Field(default=None, ge=1, le=100, description="Maximum twikit fallback pages")


class TweetRepliesResponse(BaseModel):
    """Response from tweet replies operation."""

    success: bool
    tweet_id: str
    replies_found: int
    replies: list[TweetResponse]
    error: str | None = None


class TweetQuotesRequest(BaseModel):
    """Request to fetch quote tweets for a tweet."""

    tweet_id: str = Field(..., description="Tweet ID to fetch quote tweets for")
    mirror_to_s3: bool = Field(default=False, description="Mirror discovered media URLs to S3")
    delay_seconds: float = Field(default=2.0, ge=0.5, le=10.0, description="Delay between requests")
    max_pages: int = Field(default=60, ge=1, le=100, description="Maximum search pages for quote fallbacks")


class TweetQuotesResponse(BaseModel):
    """Response from tweet quotes operation."""

    success: bool
    tweet_id: str
    quotes_found: int
    quotes: list[TweetResponse]
    source_used: str | None = None
    failure_reason: str | None = None
    error: str | None = None


def _tweet_to_response(tweet: Any) -> TweetResponse:
    return TweetResponse(
        tweet_id=tweet.tweet_id,
        date_time=tweet.date_time,
        text=tweet.text,
        hashtags=tweet.hashtags,
        mentions=tweet.mentions,
        likes=tweet.likes,
        retweets=tweet.retweets,
        replies=tweet.replies,
        quotes=tweet.quotes,
        views=tweet.views,
        bookmarks=getattr(tweet, "bookmarks", 0) or 0,
        shares=getattr(tweet, "shares", 0) or getattr(tweet, "retweets", 0) or 0,
        url=tweet.url,
        username=tweet.username,
        display_name=tweet.display_name,
        user_verified=tweet.user_verified,
        is_reply=tweet.is_reply,
        is_retweet=tweet.is_retweet,
        is_quote=tweet.is_quote,
        thread_root_tweet_id=getattr(tweet, "thread_root_tweet_id", None),
        thread_position=getattr(tweet, "thread_position", None),
        is_thread_part=bool(getattr(tweet, "is_thread_part", False)),
        twitter_context_role=getattr(tweet, "twitter_context_role", None),
        media_urls=tweet.media_urls,
        hosted_media_urls=getattr(tweet, "hosted_media_urls", []) or [],
    )


# Twitter/X Endpoints


@router.post("/twitter/search", response_model=TwitterSearchResponse)
async def search_twitter(
    request: TwitterSearchRequest,
    user: InternalAdminUser,
) -> TwitterSearchResponse:
    """
    Search Twitter/X for tweets matching a query (hashtag or phrase).

    Uses Twitter advanced search syntax to filter by date range.
    Example: searching for "RHOSLC" from 2026-01-01 to 2026-01-11.

    Requires admin access (allowlist only).
    """
    from trr_backend.repositories.social_season_analytics import _load_twikit_credentials, _load_twitter_auth
    from trr_backend.socials.twitter.direct_scrape import search_twitter as run_twitter_search

    logger.info(f"Twitter search requested by {user.get('email')} for query: {request.query}")

    def _load_auth() -> tuple[Any, Any, Any]:
        twitter_cookies, twitter_bearer = _load_twitter_auth()
        twikit_creds = _load_twikit_credentials(twitter_cookies)
        return twitter_cookies, twitter_bearer, twikit_creds

    return TwitterSearchResponse(
        **run_twitter_search(
            request,
            load_auth=_load_auth,
            persist_search=persist_standalone_twitter_search,
            logger=logger,
        )
    )


@router.post("/twitter/replies", response_model=TweetRepliesResponse)
async def fetch_tweet_replies(
    request: TweetRepliesRequest,
    user: InternalAdminUser,
) -> TweetRepliesResponse:
    """
    Fetch replies/comments for a specific tweet.

    Requires admin access (allowlist only).
    """
    from trr_backend.repositories.social_season_analytics import _load_twikit_credentials, _load_twitter_auth
    from trr_backend.socials.twitter.direct_scrape import fetch_tweet_replies as run_tweet_replies

    logger.info(f"Twitter replies requested by {user.get('email')} for tweet: {request.tweet_id}")

    def _load_auth() -> tuple[Any, Any, Any]:
        twitter_cookies, twitter_bearer = _load_twitter_auth()
        twikit_creds = _load_twikit_credentials(twitter_cookies)
        return twitter_cookies, twitter_bearer, twikit_creds

    return TweetRepliesResponse(**run_tweet_replies(request, load_auth=_load_auth, logger=logger))


@router.post("/twitter/quotes", response_model=TweetQuotesResponse)
async def fetch_tweet_quotes(
    request: TweetQuotesRequest,
    user: InternalAdminUser,
) -> TweetQuotesResponse:
    """
    Fetch quote tweets for a specific tweet.

    Requires admin access (allowlist only).
    """
    from trr_backend.repositories.social_season_analytics import _load_twikit_credentials, _load_twitter_auth
    from trr_backend.socials.twitter.direct_scrape import fetch_tweet_quotes as run_tweet_quotes

    logger.info(f"Twitter quotes requested by {user.get('email')} for tweet: {request.tweet_id}")

    def _load_auth() -> tuple[Any, Any, Any]:
        twitter_cookies, twitter_bearer = _load_twitter_auth()
        twikit_creds = _load_twikit_credentials(twitter_cookies)
        return twitter_cookies, twitter_bearer, twikit_creds

    return TweetQuotesResponse(**run_tweet_quotes(request, load_auth=_load_auth, logger=logger))


# YouTube Models


class YouTubeScrapeRequest(BaseModel):
    """Request to scrape YouTube channel videos."""

    channel_handle: str = Field(default="", description="YouTube channel handle (without @)")
    source_type: Literal["account", "playlist"] = Field(default="account", description="YouTube source mode")
    playlist_id: str | None = Field(default=None, description="YouTube playlist ID when source_type is playlist")
    playlist_url: str | None = Field(default=None, description="YouTube playlist URL when source_type is playlist")
    keywords: list[str] = Field(..., description="Keywords to filter by (e.g., RHOSLC, 'Salt Lake City')")
    date_start: datetime = Field(..., description="Start date for filtering")
    date_end: datetime = Field(..., description="End date for filtering")
    delay_seconds: float = Field(default=2.0, ge=0.5, le=10.0, description="Delay between requests")
    max_results: int | None = Field(default=None, ge=1, le=500, description="Maximum videos to fetch")

    # Optional metadata for association
    show_id: UUID | None = Field(default=None, description="Associated show ID")
    season_number: int | None = Field(default=None, ge=0, le=100, description="Associated season")
    person_id: UUID | None = Field(default=None, description="Associated person ID")

    @model_validator(mode="after")
    def validate_source(self) -> YouTubeScrapeRequest:
        if self.source_type == "playlist":
            if not (str(self.playlist_id or "").strip() or str(self.playlist_url or "").strip()):
                raise ValueError("playlist_id or playlist_url is required when source_type is playlist")
            return self
        if not str(self.channel_handle or "").strip():
            raise ValueError("channel_handle is required when source_type is account")
        return self


class YouTubeVideoResponse(BaseModel):
    """Single YouTube video in response."""

    video_id: str
    title: str
    description: str
    date_time: str
    channel_title: str
    duration: str
    duration_seconds: int
    views: int
    likes: int
    comments: int
    url: str
    thumbnail_url: str
    keywords_matched: list[str]


class YouTubeScrapeResponse(BaseModel):
    """Response from YouTube scrape operation."""

    success: bool
    channel_handle: str
    videos_found: int
    videos: list[YouTubeVideoResponse]
    filters_applied: dict
    error: str | None = None


# YouTube Endpoints


@router.post("/youtube/scrape", response_model=YouTubeScrapeResponse)
async def scrape_youtube(
    request: YouTubeScrapeRequest,
    user: InternalAdminUser,
) -> YouTubeScrapeResponse:
    """
    Scrape YouTube channel videos with keyword filtering.

    Searches for videos from a specific channel that match the given keywords
    and fall within the date range.

    Requires admin access (allowlist only).
    """
    from trr_backend.socials.youtube.direct_scrape import scrape_youtube as run_youtube_scrape

    logger.info(f"YouTube scrape requested by {user.get('email')} for @{request.channel_handle}")

    return YouTubeScrapeResponse(**run_youtube_scrape(request, logger=logger))


# Facebook Models / Endpoints


class FacebookScrapeRequest(BaseModel):
    page_handle: str = Field(..., description="Facebook page handle (without leading /)")
    hashtags: list[str] = Field(default_factory=list, description="Optional hashtag filter (without #)")
    keywords: list[str] = Field(default_factory=list, description="Optional keyword filter")
    date_start: datetime | None = Field(default=None, description="Optional start date for filtering")
    date_end: datetime | None = Field(default=None, description="Optional end date for filtering")
    delay_seconds: float = Field(default=1.25, ge=0.25, le=10.0, description="Delay between requests")
    max_pages: int | None = Field(default=1, ge=1, le=100, description="Maximum discovery pages")


class FacebookMediaProvenanceResponse(BaseModel):
    platform: str
    matched_by: str
    fallback_used: bool


class FacebookShareResponse(BaseModel):
    sharer_name: str
    profile_url: str | None = None
    post_url: str | None = None
    caption_snippet: str | None = None
    posted_at: str | None = None
    privacy_label: str | None = None
    media_preview_urls: list[str] = Field(default_factory=list)


class FacebookPostResponse(BaseModel):
    post_id: str
    post_type: str
    username: str
    caption: str
    likes: int
    comments: int
    shares: int
    views: int
    url: str
    thumbnail_url: str | None = None
    media_urls: list[str] = Field(default_factory=list)
    posted_at: str | None = None
    reactions: dict[str, int] = Field(
        default_factory=dict,
        description="Per-reaction breakdown (Like, Love, Haha, etc.)",
    )
    share_details: list[FacebookShareResponse] = Field(default_factory=list)
    media_provenance: FacebookMediaProvenanceResponse | None = None


def _facebook_post_response(post: Any) -> FacebookPostResponse:
    raw_media_provenance = getattr(post, "media_provenance", None)
    if hasattr(raw_media_provenance, "to_dict"):
        media_provenance = dict(raw_media_provenance.to_dict() or {})
    elif isinstance(raw_media_provenance, dict):
        media_provenance = dict(raw_media_provenance or {})
    else:
        media_provenance = {}
    share_details = []
    for share in getattr(post, "share_details", []) or []:
        posted_at = getattr(share, "posted_at", None)
        share_details.append(
            FacebookShareResponse(
                sharer_name=str(getattr(share, "sharer_name", "") or ""),
                profile_url=str(getattr(share, "profile_url", "") or "") or None,
                post_url=str(getattr(share, "post_url", "") or "") or None,
                caption_snippet=str(getattr(share, "caption_snippet", "") or "") or None,
                posted_at=(
                    datetime.fromtimestamp(int(posted_at), tz=UTC).isoformat() if posted_at is not None else None
                ),
                privacy_label=str(getattr(share, "privacy_label", "") or "") or None,
                media_preview_urls=[
                    str(url) for url in (getattr(share, "media_preview_urls", []) or []) if str(url).strip()
                ],
            )
        )
    return FacebookPostResponse(
        post_id=str(getattr(post, "post_id", "") or ""),
        post_type=str(getattr(post, "post_type", "feed") or "feed"),
        username=str(getattr(post, "username", "") or ""),
        caption=str(getattr(post, "caption", "") or ""),
        likes=int(getattr(post, "likes", 0) or 0),
        comments=int(getattr(post, "comments", 0) or 0),
        shares=int(getattr(post, "shares", 0) or 0),
        views=int(getattr(post, "views", 0) or 0),
        url=str(getattr(post, "url", "") or ""),
        thumbnail_url=str(getattr(post, "thumbnail_url", "") or "") or None,
        media_urls=[str(url) for url in (getattr(post, "media_urls", []) or []) if str(url)],
        posted_at=(
            datetime.fromtimestamp(int(post.posted_at), tz=UTC).isoformat()
            if getattr(post, "posted_at", None) is not None
            else None
        ),
        reactions=dict(getattr(post, "reactions", {}) or {}),
        share_details=share_details,
        media_provenance=(
            FacebookMediaProvenanceResponse(
                platform=str(media_provenance.get("platform") or "facebook"),
                matched_by=str(media_provenance.get("matched_by") or "native"),
                fallback_used=bool(media_provenance.get("fallback_used", False)),
            )
            if media_provenance
            else None
        ),
    )


class FacebookScrapeResponse(BaseModel):
    success: bool
    page_handle: str
    posts_found: int
    posts: list[FacebookPostResponse]
    filters_applied: dict
    retrieval_meta: dict | None = None
    error: str | None = None


class FacebookSearchPostsRequest(BaseModel):
    search_url: str | None = Field(default=None, description="Direct Facebook search URL")
    profile_url: str | None = Field(default=None, description="Facebook profile/page URL used to build search URL")
    query: str = Field(..., description="Search query such as a hashtag or phrase")
    date_start: datetime | None = Field(default=None, description="Optional start date for filtering")
    date_end: datetime | None = Field(default=None, description="Optional end date for filtering")
    max_posts: int = Field(default=25, ge=1, le=100, description="Maximum posts to return")
    include_share_details: bool = Field(default=False, description="Also fetch people who shared the post")
    include_comments: bool = Field(default=False, description="Also fetch visible comments for each post")
    max_comments: int = Field(default=100, ge=0, le=1000, description="Max comments per post")
    max_shares: int = Field(default=100, ge=0, le=500, description="Max share-detail rows per post")
    allow_cross_platform_media_fallback: bool = Field(
        default=True,
        description="Allow strict Instagram media fallback when Facebook media is unavailable",
    )
    delay_seconds: float = Field(default=1.25, ge=0.25, le=10.0, description="Delay between requests")

    @model_validator(mode="after")
    def validate_search_source(self) -> FacebookSearchPostsRequest:
        if not str(self.query or "").strip():
            raise ValueError("query is required")
        if not str(self.search_url or "").strip() and not str(self.profile_url or "").strip():
            raise ValueError("search_url or profile_url is required")
        return self


class FacebookSearchPostsResponse(BaseModel):
    success: bool
    query: str
    posts_found: int
    posts: list[FacebookPostResponse]
    retrieval_meta: dict | None = None
    error: str | None = None


@router.post("/facebook/scrape", response_model=FacebookScrapeResponse)
async def scrape_facebook(
    request: FacebookScrapeRequest,
    user: InternalAdminUser,
) -> FacebookScrapeResponse:
    from trr_backend.repositories.social_season_analytics import _load_facebook_cookies
    from trr_backend.socials.facebook.direct_scrape import scrape_facebook as run_facebook_scrape

    logger.info("Facebook scrape requested by %s for %s", user.get("email"), request.page_handle)

    def _load_cookies(surface: str) -> Any:
        return _load_social_auth_or_503(platform="facebook", surface=surface, loader=_load_facebook_cookies)

    return FacebookScrapeResponse(**run_facebook_scrape(request, load_cookies=_load_cookies, logger=logger))


@router.post("/facebook/search-posts", response_model=FacebookSearchPostsResponse)
async def search_facebook_posts(
    request: FacebookSearchPostsRequest,
    user: InternalAdminUser,
) -> FacebookSearchPostsResponse:
    from trr_backend.repositories.social_season_analytics import _load_facebook_cookies
    from trr_backend.socials.facebook.direct_scrape import search_facebook_posts as run_facebook_search_posts

    logger.info("Facebook search requested by %s for query=%s", user.get("email"), request.query)

    def _load_cookies(surface: str) -> Any:
        return _load_social_auth_or_503(platform="facebook", surface=surface, loader=_load_facebook_cookies)

    return FacebookSearchPostsResponse(**run_facebook_search_posts(request, load_cookies=_load_cookies, logger=logger))


@router.get("/facebook/preview/{page_handle}")
async def preview_facebook_page(page_handle: str, user: InternalAdminUser) -> dict:
    from trr_backend.repositories.social_season_analytics import _load_facebook_cookies
    from trr_backend.socials.facebook.direct_scrape import preview_facebook_page as run_facebook_preview

    logger.info("Facebook preview requested by %s for %s", user.get("email"), page_handle)

    def _load_cookies(surface: str) -> Any:
        return _load_social_auth_or_503(platform="facebook", surface=surface, loader=_load_facebook_cookies)

    return run_facebook_preview(page_handle, load_cookies=_load_cookies, logger=logger)


class FacebookPostScrapeRequest(BaseModel):
    post_url: str = Field(..., description="Facebook post/video/reel URL (supports /share/v/ short links)")
    fetch_comments: bool = Field(default=True, description="Also extract comments from the page")
    max_comments: int = Field(default=100, ge=0, le=1000, description="Max comments to extract")
    fetch_shares: bool = Field(default=False, description="Also extract people who shared the post")
    max_shares: int = Field(default=100, ge=0, le=500, description="Max share-detail rows to extract")
    allow_cross_platform_media_fallback: bool = Field(
        default=True,
        description="Allow strict Instagram media fallback when Facebook media is unavailable",
    )


class FacebookCommentResponse(BaseModel):
    comment_id: str
    username: str
    text: str
    likes: int = 0
    created_at: int | None = None
    is_reply: bool = False
    reply_count: int = 0


class FacebookPostScrapeResponse(BaseModel):
    success: bool
    post: FacebookPostResponse | None = None
    comments: list[FacebookCommentResponse] = Field(default_factory=list)
    comments_found: int = 0
    shares_found: int = 0
    error: str | None = None


@router.post("/facebook/scrape-post", response_model=FacebookPostScrapeResponse)
async def scrape_facebook_post(
    request: FacebookPostScrapeRequest,
    user: InternalAdminUser,
) -> FacebookPostScrapeResponse:
    from trr_backend.repositories.social_season_analytics import _load_facebook_cookies
    from trr_backend.socials.facebook.direct_scrape import scrape_facebook_post as run_facebook_post_scrape

    logger.info("Facebook post scrape requested by %s for %s", user.get("email"), request.post_url)

    def _load_cookies(surface: str) -> Any:
        return _load_social_auth_or_503(platform="facebook", surface=surface, loader=_load_facebook_cookies)

    return FacebookPostScrapeResponse(**run_facebook_post_scrape(request, load_cookies=_load_cookies, logger=logger))


# Threads Models / Endpoints


class ThreadsScrapeRequest(BaseModel):
    username: str = Field(..., description="Threads username (without @)")
    hashtags: list[str] = Field(default_factory=list, description="Optional hashtag filter (without #)")
    keywords: list[str] = Field(default_factory=list, description="Optional keyword filter")
    date_start: datetime | None = Field(default=None, description="Optional start date")
    date_end: datetime | None = Field(default=None, description="Optional end date")
    delay_seconds: float = Field(default=1.0, ge=0.25, le=10.0, description="Delay between requests")
    max_pages: int | None = Field(default=1, ge=1, le=100, description="Maximum profile pages to inspect")


class ThreadsPostResponse(BaseModel):
    post_id: str
    username: str
    text: str
    likes: int
    replies: int
    reposts: int
    quotes: int
    views: int
    url: str
    thumbnail_url: str | None = None
    media_urls: list[str] = Field(default_factory=list)
    posted_at: str | None = None


class ThreadsScrapeResponse(BaseModel):
    success: bool
    username: str
    posts_found: int
    posts: list[ThreadsPostResponse]
    filters_applied: dict
    retrieval_meta: dict | None = None
    error: str | None = None


@router.post("/threads/scrape", response_model=ThreadsScrapeResponse)
async def scrape_threads(
    request: ThreadsScrapeRequest,
    user: InternalAdminUser,
) -> ThreadsScrapeResponse:
    from trr_backend.repositories.social_season_analytics import _load_threads_cookies
    from trr_backend.socials.threads.direct_scrape import scrape_threads as run_threads_scrape

    logger.info("Threads scrape requested by %s for @%s", user.get("email"), request.username)

    def _load_cookies(surface: str) -> Any:
        return _load_social_auth_or_503(platform="threads", surface=surface, loader=_load_threads_cookies)

    return ThreadsScrapeResponse(**run_threads_scrape(request, load_cookies=_load_cookies, logger=logger))


@router.get("/threads/preview/{username}")
async def preview_threads_profile(username: str, user: InternalAdminUser) -> dict:
    from trr_backend.repositories.social_season_analytics import _load_threads_cookies
    from trr_backend.socials.threads.direct_scrape import preview_threads_profile as run_threads_preview

    logger.info("Threads preview requested by %s for @%s", user.get("email"), username)

    def _load_cookies(surface: str) -> Any:
        return _load_social_auth_or_503(platform="threads", surface=surface, loader=_load_threads_cookies)

    return run_threads_preview(username, load_cookies=_load_cookies, logger=logger)


# ---------------------------------------------------------------------------
# Season social analytics (Bravo-first)
# ---------------------------------------------------------------------------


class SeasonSocialTargetInput(BaseModel):
    platform: Literal["instagram", "tiktok", "twitter", "youtube", "facebook", "threads", "reddit"]
    accounts: list[str] = Field(default_factory=list)
    hashtags: list[str] = Field(default_factory=list)
    keywords: list[str] = Field(default_factory=list)
    timezone: str = Field(default="America/New_York")
    is_active: bool = Field(default=True)
    config: dict = Field(default_factory=dict)


class SeasonSocialTargetsPutRequest(SourceScopedRequest):
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Field(default="network")
    targets: list[SeasonSocialTargetInput]


class SeasonSocialIngestRequest(SourceScopedRequest):
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Field(default="network")
    platforms: list[Literal["instagram", "tiktok", "twitter", "youtube", "facebook", "threads"]] | None = Field(
        default=None
    )
    sync_strategy: Literal["incremental", "full_refresh"] = Field(default="incremental")
    comment_refresh_policy: Literal["balanced", "missing_only"] = Field(default="balanced")
    comment_anchor_source_ids: (
        dict[Literal["instagram", "tiktok", "twitter", "youtube", "facebook", "threads"], list[str]] | None
    ) = Field(default=None)
    accounts_override: list[str] | None = Field(default=None)
    hashtags_override: list[str] | None = Field(default=None)
    keywords_override: list[str] | None = Field(default=None)
    sound_ids: list[str] | None = Field(default=None, description="Optional TikTok sound IDs or sound URLs")
    max_posts_per_target: int = Field(default=0, ge=0)
    max_comments_per_post: int = Field(default=0, ge=0)
    max_replies_per_post: int = Field(default=0, ge=0)
    fetch_replies: bool = Field(default=True)
    ingest_mode: Literal["posts_only", "posts_and_comments", "comments_only", "details_refresh"] = Field(
        default="posts_and_comments"
    )
    week_index: int | None = Field(default=None, ge=0, le=200)
    timezone: str = Field(default="America/New_York", min_length=1, max_length=64)
    date_start: datetime | None = None
    date_end: datetime | None = None
    runner_strategy: Literal["single_runner", "adaptive_dual_runner"] | None = Field(default=None)
    runner_count: int | None = Field(default=None, ge=1, le=2)
    window_shard_hours: int | None = Field(default=None, ge=1, le=24)
    runner_b_start_offset_hours: int | None = Field(default=None, ge=0, le=168)
    day_weight_profile: Literal["default", "rhoslc_default"] | None = Field(default=None)
    priority_mode: Literal["default", "episode_peak_weighted"] | None = Field(default=None)
    youtube_source_mode: Literal["hybrid", "api_only", "scraper_only"] | None = Field(default=None)
    youtube_force_reindex: bool = Field(default=False)
    youtube_force_media_refresh: bool = Field(default=False)
    youtube_force_comment_refresh: bool = Field(default=False)
    allow_inline_dev_fallback: bool = Field(default=False)
    client_session_id: str | None = Field(default=None, max_length=200)
    client_workflow_id: str | None = Field(default=None, max_length=200)


class SeasonSocialOrchestrationRequest(SourceScopedRequest):
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Field(default="network")
    platforms: list[Literal["instagram", "tiktok", "twitter", "youtube", "facebook", "threads"]] | None = Field(
        default=None
    )
    sync_strategy: Literal["incremental", "full_refresh"] = Field(default="incremental")
    comment_refresh_policy: Literal["balanced", "missing_only"] = Field(default="balanced")
    comment_anchor_source_ids: (
        dict[Literal["instagram", "tiktok", "twitter", "youtube", "facebook", "threads"], list[str]] | None
    ) = Field(default=None)
    accounts_override: list[str] | None = Field(default=None)
    hashtags_override: list[str] | None = Field(default=None)
    keywords_override: list[str] | None = Field(default=None)
    sound_ids: list[str] | None = Field(default=None)
    max_posts_per_target: int = Field(default=0, ge=0)
    max_comments_per_post: int = Field(default=0, ge=0)
    max_replies_per_post: int = Field(default=0, ge=0)
    fetch_replies: bool = Field(default=True)
    ingest_mode: Literal["posts_only", "posts_and_comments", "comments_only", "details_refresh"] = Field(
        default="posts_and_comments"
    )
    week_index: int | None = Field(default=None, ge=0, le=200)
    timezone: str = Field(default="America/New_York", min_length=1, max_length=64)
    runner_strategy: Literal["single_runner", "adaptive_dual_runner"] | None = Field(default=None)
    runner_count: int | None = Field(default=None, ge=1, le=2)
    window_shard_hours: int | None = Field(default=None, ge=1, le=24)
    runner_b_start_offset_hours: int | None = Field(default=None, ge=0, le=168)
    day_weight_profile: Literal["default", "rhoslc_default"] | None = Field(default=None)
    priority_mode: Literal["default", "episode_peak_weighted"] | None = Field(default=None)
    youtube_source_mode: Literal["hybrid", "api_only", "scraper_only"] | None = Field(default=None)
    youtube_force_reindex: bool = Field(default=False)
    youtube_force_media_refresh: bool = Field(default=False)
    youtube_force_comment_refresh: bool = Field(default=False)
    resume_existing: bool = Field(default=True)
    client_session_id: str | None = Field(default=None, max_length=200)
    client_workflow_id: str | None = Field(default=None, max_length=200)


class SharedAccountSourceInput(BaseModel):
    platform: Literal["instagram", "tiktok", "twitter", "youtube", "facebook", "threads"]
    account_handle: str = Field(..., min_length=1, max_length=128)
    is_active: bool = Field(default=True)
    scrape_priority: int = Field(default=100, ge=1, le=10_000)
    metadata: dict[str, Any] = Field(default_factory=dict)


class SharedAccountSourcesPutRequest(SourceScopedRequest):
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Field(default="network")
    sources: list[SharedAccountSourceInput]


class SyncSessionRetryRequest(BaseModel):
    retry_kind: Literal[
        "retry_missing_comments",
        "retry_failed_media",
        "retry_missing_avatars",
        "retry_missing_comment_media",
    ]


class SharedSocialIngestRequest(SourceScopedRequest):
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Field(default="network")
    platforms: list[Literal["instagram", "tiktok", "twitter", "youtube", "facebook", "threads"]] | None = Field(
        default=None
    )
    accounts_override: list[str] | None = Field(default=None)
    date_start: datetime | None = None
    date_end: datetime | None = None
    allow_inline_dev_fallback: bool = Field(default=False)


class SharedReviewResolveRequest(BaseModel):
    resolution_action: Literal["resolve", "ignore"] = Field(default="resolve")
    resolved_show_id: UUID | None = None
    resolved_season_id: UUID | None = None


class SocialAccountProfileHashtagAssignmentInput(BaseModel):
    show_id: UUID


class SocialAccountProfileHashtagInput(BaseModel):
    hashtag: str = Field(..., min_length=1, max_length=128)
    assignments: list[SocialAccountProfileHashtagAssignmentInput] = Field(default_factory=list)


class SocialAccountProfileHashtagsPutRequest(BaseModel):
    hashtags: list[SocialAccountProfileHashtagInput] = Field(default_factory=list)


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


class CatalogRepairAuthRequest(BaseModel):
    allow_inline_dev_fallback: bool = Field(default=False)
    operator_confirmation: str | None = Field(default=None)
    allow_cookie_refresh: bool = Field(default=False)


class CatalogReviewResolveRequest(BaseModel):
    resolution_action: Literal["assign_show", "mark_non_show"]
    show_id: UUID | None = None

    @model_validator(mode="after")
    def validate_resolution(self) -> CatalogReviewResolveRequest:
        if self.resolution_action == "assign_show" and self.show_id is None:
            raise ValueError("show_id is required when assigning a show hashtag")
        return self


class PostCommentRefreshRequest(BaseModel):
    max_comments_per_post: int = Field(default=0, ge=0)
    fetch_replies: bool = Field(default=True)


class SocialAccountCommentsScrapeRequest(SourceScopedRequest):
    mode: Literal["profile", "single_post"] = Field(default="profile")
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Field(default="network")
    source_id: str | None = Field(default=None, min_length=1, max_length=64)
    max_posts: int | None = Field(default=None, ge=1)
    max_comments_per_post: int | None = Field(default=None, ge=0)
    refresh_policy: Literal["stale_or_missing", "all_saved_posts"] = Field(default="stale_or_missing")
    target_filter: Literal["incomplete"] | None = Field(default=None)
    comments_load_strategy: Literal["cursor_api", "single_session_load_all"] = Field(default="cursor_api")
    allow_inline_dev_fallback: bool = Field(default=False)
    dry_run: bool = Field(default=False)

    @model_validator(mode="after")
    def validate_shape(self) -> SocialAccountCommentsScrapeRequest:
        if self.mode == "single_post" and not str(self.source_id or "").strip():
            raise ValueError("source_id is required for single_post comment scrapes")
        if self.mode == "single_post" and self.target_filter is not None:
            raise ValueError("target_filter is only supported for profile comment scrapes")
        return self


class SocialAccountCommentsAuditCursorRetryRequest(BaseModel):
    limit: int = Field(default=50, ge=1, le=500)
    shortcodes: list[str] | None = Field(default=None, max_length=500)
    stop_reasons: list[str] | None = Field(default=None, max_length=20)
    show_ids: list[str] | None = Field(default=None, max_length=100)
    season_ids: list[str] | None = Field(default=None, max_length=100)
    show_filters: list[str] | None = Field(default=None, max_length=100)
    show_filter: str | None = Field(default=None, max_length=128)
    batch_size: int = Field(default=1, ge=1, le=25)
    comments_worker_count: int | None = Field(default=None, ge=1, le=24)
    max_comments_per_post: int = Field(default=0, ge=0)
    comments_load_strategy: Literal["cursor_api", "single_session_load_all"] = Field(default="cursor_api")
    skip_launch_auth_probe: bool = Field(default=False)
    attach_to_active_run: bool = Field(default=True)
    dispatch_immediately: bool = Field(default=True)
    force_rerun_existing: bool = Field(default=False)
    dry_run: bool = Field(default=False)


class CancelStuckJobsRequest(BaseModel):
    job_ids: list[UUID] | None = Field(default=None, max_length=500)


class DismissRecentFailuresRequest(BaseModel):
    job_ids: list[UUID] = Field(default_factory=list, max_length=500)
    dismiss_all_visible: bool = False


class ResetSocialIngestHealthRequest(BaseModel):
    pass


class PurgeInactiveWorkersRequest(BaseModel):
    stale_after_seconds: int | None = Field(default=None, ge=5, le=86_400)


class JobDebugRequest(BaseModel):
    apply_patch: bool = Field(default=False)
    confirm_apply: bool = Field(default=False)
    include_context: bool = Field(default=True)


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


@router.get("/seasons/{season_id}/targets")
async def get_season_targets(
    season_id: UUID,
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Query(default="network"),
    _: InternalAdminUser = None,
) -> dict:
    from trr_backend.repositories.social_season_analytics import get_targets

    started_at = perf_counter()
    try:
        payload = await _run_admin_repo_call(get_targets, str(season_id), source_scope=source_scope)
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
    from trr_backend.repositories.social_season_analytics import put_targets

    try:
        rows = [target.model_dump() for target in payload.targets]
        return await _run_admin_repo_call(
            put_targets,
            str(season_id),
            source_scope=payload.source_scope,
            targets=rows,
            updated_by=user.get("email"),
        )
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to write season social targets: season=%s", season_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/seasons/{season_id}/ingest")
async def ingest_season_social(
    season_id: UUID,
    payload: SeasonSocialIngestRequest,
    background_tasks: BackgroundTasks,
    user: InternalAdminUser = None,
) -> dict:
    from trr_backend.repositories.social_season_analytics import (
        SocialIngestValidationError,
        SocialWorkerUnavailableError,
        assert_worker_available_when_queue_enabled,
        ingest_season,
        is_queue_enabled,
        recover_stale_running_jobs,
    )

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
            ingest_season,
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
                        recovered = recover_stale_running_jobs(run_id=run_id, limit=250)
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
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/seasons/{season_id}/ingest/orchestrations")
async def orchestrate_season_social_ingest(
    season_id: UUID,
    payload: SeasonSocialOrchestrationRequest,
    background_tasks: BackgroundTasks,
    user: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import (
        SocialWorkerUnavailableError,
        assert_worker_available_when_queue_enabled,
        is_queue_enabled,
        orchestrate_season_ingest,
    )

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
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/seasons/{season_id}/sync-sessions")
async def create_season_sync_session(
    season_id: UUID,
    payload: SeasonSocialIngestRequest,
    request: Request,
    _: BackgroundTasks,
    user: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import (
        SocialWorkerUnavailableError,
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
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def _social_sync_sse_chunk(event_type: str, payload: dict[str, Any]) -> bytes:
    return f"event: {event_type}\ndata: {json.dumps(jsonable_encoder(payload))}\n\n".encode()


def _build_sync_session_stream_payload_sync(sync_session_id: str) -> dict[str, Any]:
    from trr_backend.repositories import social_season_analytics as social_repo
    from trr_backend.repositories.social_sync_orchestrator import evaluate_sync_session

    sync_session = evaluate_sync_session(sync_session_id)
    season_id = str(sync_session.get("season_id") or "").strip()
    current_run_id = str(sync_session.get("current_run_id") or "").strip()
    run_progress = (
        social_repo.get_run_progress_snapshot(season_id, current_run_id, recent_log_limit=20)
        if season_id and current_run_id
        else None
    )
    return {
        "sync_session": sync_session,
        "run_progress": run_progress,
        "emitted_at": datetime.now(UTC).isoformat(),
    }


async def _build_sync_session_stream_payload(sync_session_id: str) -> dict[str, Any]:
    return await _run_admin_repo_call(_build_sync_session_stream_payload_sync, sync_session_id)


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
        raise HTTPException(status_code=500, detail=str(exc)) from exc


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
        raise HTTPException(status_code=500, detail=str(exc)) from exc


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
        raise HTTPException(status_code=500, detail=str(exc)) from exc


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
    from trr_backend.repositories.social_season_analytics import preview_ingest_schedule

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
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/shared/sources")
def get_shared_account_sources_route(
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Query(default="network"),
    include_inactive: bool = Query(default=True),
    platforms: str | None = Query(default=None, description="Comma-separated platform list"),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import get_shared_account_sources

    try:
        canonical_source_scope = normalize_source_scope_param(source_scope)
        return get_shared_account_sources(
            source_scope=canonical_source_scope,
            include_inactive=include_inactive,
            platforms=_parse_platform_query(platforms),
        )
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to read shared account sources: source_scope=%s", source_scope)
        raise _to_social_read_http_exception(exc) from exc


@router.put("/shared/sources")
def put_shared_account_sources_route(
    payload: SharedAccountSourcesPutRequest,
    user: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import put_shared_account_sources

    try:
        return put_shared_account_sources(
            source_scope=payload.source_scope,
            sources=[source.model_dump() for source in payload.sources],
            updated_by=(user or {}).get("email"),
        )
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc


@router.get("/profiles/{platform}/{account_handle}/summary")
def get_social_account_profile_summary_route(
    request: Request,
    platform: str,
    account_handle: str,
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    detail = social_profile_reads.normalize_profile_summary_detail(request.query_params.get("detail"))

    cache_key = _account_profile_cache_key(
        surface="summary",
        platform=platform,
        account_handle=account_handle,
        extra=(detail,),
    )
    try:
        return _resolve_account_profile_singleflight(
            cache_key,
            lambda: social_profile_reads.get_profile_summary(
                platform=platform,
                account_handle=account_handle,
                detail=detail,
            ),
            cache=_ACCOUNT_PROFILE_SUMMARY_CACHE,
            cache_lock=_ACCOUNT_PROFILE_SUMMARY_CACHE_LOCK,
            ttl_seconds=_ACCOUNT_PROFILE_CACHE_TTL_SECONDS,
            max_entries=_ACCOUNT_PROFILE_CACHE_MAX_ENTRIES,
        )
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to fetch social account profile summary: platform=%s account=%s",
            platform,
            account_handle,
        )
        raise _to_social_read_http_exception(exc) from exc


@router.get("/profiles/{platform}/{account_handle}/dashboard")
def get_social_account_profile_dashboard_route(
    request: Request,
    platform: str,
    account_handle: str,
    run_id: str | None = Query(default=None),
    recent_log_limit: int = Query(default=25, ge=0, le=100),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.socials.profile_dashboard import build_social_account_profile_dashboard

    detail = social_profile_reads.normalize_profile_summary_detail(request.query_params.get("detail"))
    normalized_run_id = str(run_id or "").strip() or None
    dashboard_cache_ttl_seconds = (
        _ACCOUNT_PROFILE_PROGRESS_CACHE_TTL_SECONDS if normalized_run_id else _ACCOUNT_PROFILE_CACHE_TTL_SECONDS
    )
    cache_key = _account_profile_cache_key(
        surface="dashboard",
        platform=platform,
        account_handle=account_handle,
        extra=(detail, normalized_run_id, recent_log_limit),
    )
    try:
        return _resolve_account_profile_singleflight(
            cache_key,
            lambda: build_social_account_profile_dashboard(
                platform=platform,
                account_handle=account_handle,
                detail=detail,
                run_id=normalized_run_id,
                recent_log_limit=recent_log_limit,
            ),
            cache=_ACCOUNT_PROFILE_DASHBOARD_CACHE,
            cache_lock=_ACCOUNT_PROFILE_DASHBOARD_CACHE_LOCK,
            ttl_seconds=dashboard_cache_ttl_seconds,
            max_entries=_ACCOUNT_PROFILE_CACHE_MAX_ENTRIES,
        )
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to fetch social account profile dashboard: platform=%s account=%s",
            platform,
            account_handle,
        )
        raise _to_social_read_http_exception(exc) from exc


@router.get("/profiles/{platform}/{account_handle}/live-profile-total")
def get_social_account_live_profile_total_route(
    platform: str,
    account_handle: str,
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    try:
        return social_profile_reads.get_live_profile_total(platform=platform, account_handle=account_handle)
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to fetch social account live profile total: platform=%s account=%s",
            platform,
            account_handle,
        )
        raise _to_social_read_http_exception(exc) from exc


@router.get("/profiles/{platform}/{account_handle}/socialblade")
def get_social_account_profile_socialblade_route(
    platform: str,
    account_handle: str,
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.repositories.socialblade_growth import get_growth_data
    from trr_backend.socials.socialblade.service import (
        SocialBladeRefreshError,
        sanitize_socialblade_handle,
        sanitize_socialblade_platform,
    )

    try:
        normalized_platform = sanitize_socialblade_platform(platform)
        safe_handle = sanitize_socialblade_handle(account_handle, platform=normalized_platform)
    except SocialBladeRefreshError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not safe_handle:
        raise HTTPException(status_code=400, detail="Invalid handle")

    data = get_growth_data(None, safe_handle, platform=normalized_platform)
    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"No SocialBlade data found for {normalized_platform}/@{safe_handle}",
        )
    return data


def _refresh_social_account_profile_socialblade(
    *,
    normalized_platform: str,
    safe_handle: str,
    force: bool,
) -> dict[str, Any]:
    from trr_backend.socials.socialblade.auth import (
        load_socialblade_cookies_from_sources,
        refresh_socialblade_cookies,
    )
    from trr_backend.socials.socialblade.scraper import scrape_socialblade
    from trr_backend.socials.socialblade.service import (
        refresh_and_persist_socialblade,
        scrape_socialblade_then_following,
    )

    refresh_socialblade_cookies("account_page_refresh", allow_headless_fallback=False)
    cookies = load_socialblade_cookies_from_sources()

    def _scrape_account(normalized_handle: str) -> dict[str, Any]:
        return scrape_socialblade(
            normalized_handle,
            cookies,
            platform=normalized_platform,
            allow_login_fallback=False,
            allow_visible_browser_retry=normalized_platform in {"instagram", "tiktok"},
        )

    return refresh_and_persist_socialblade(
        person_id=None,
        platform=normalized_platform,
        handle=safe_handle,
        scraper=lambda normalized_handle: scrape_socialblade_then_following(
            _scrape_account,
            normalized_handle,
            platform=normalized_platform,
            source="account_page",
        ),
        source="account_page",
        force=force,
    )


@router.post("/profiles/{platform}/{account_handle}/socialblade/refresh")
async def refresh_social_account_profile_socialblade_route(
    platform: str,
    account_handle: str,
    body: SocialBladeProfileRefreshRequest,
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.socials.socialblade.service import (
        SocialBladeRefreshError,
        sanitize_socialblade_handle,
        sanitize_socialblade_platform,
    )

    try:
        normalized_platform = sanitize_socialblade_platform(platform)
        safe_handle = sanitize_socialblade_handle(account_handle, platform=normalized_platform)
    except SocialBladeRefreshError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not safe_handle:
        raise HTTPException(status_code=400, detail="Invalid handle")

    try:
        return await run_in_threadpool(
            _refresh_social_account_profile_socialblade,
            normalized_platform=normalized_platform,
            safe_handle=safe_handle,
            force=body.force,
        )
    except SocialBladeRefreshError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to refresh social account SocialBlade data: platform=%s account=%s",
            platform,
            account_handle,
        )
        raise _to_social_read_http_exception(exc) from exc


@router.get("/profiles/{platform}/{account_handle}/posts")
def get_social_account_profile_posts_route(
    request: Request,
    platform: str,
    account_handle: str,
    page: int = Query(default=1, ge=1, le=10_000),
    page_size: int = Query(default=25, ge=1, le=100),
    limit: int | None = Query(default=None, ge=1, le=100),
    search: str | None = Query(default=None),
    comments_only: bool = Query(default=False),
    comment_filter: str | None = Query(default=None),
    sort_by: str | None = Query(default=None),
    sort_dir: str | None = Query(default=None),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    started_at = perf_counter()
    effective_page_size = page_size if "page_size" in request.query_params else (limit or page_size)
    cache_key = _account_profile_cache_key(
        surface="posts",
        platform=platform,
        account_handle=account_handle,
        page=page,
        page_size=effective_page_size,
        search=search,
        comments_only=comments_only,
        comment_filter=comment_filter,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )
    cached_payload = _get_ttl_cached_payload(_ACCOUNT_PROFILE_POSTS_CACHE, _ACCOUNT_PROFILE_POSTS_CACHE_LOCK, cache_key)
    if cached_payload is not None:
        if str(os.getenv("TRR_SOCIAL_PROFILE_PERF_DEBUG") or "").strip().lower() in {"1", "true", "yes", "on"}:
            logger.info(
                "[social-profile-perf] route=get_social_account_profile_posts_route platform=%s handle=%s "
                "cache_status=hit elapsed_ms=%s page_size=%s",
                platform,
                account_handle,
                int((perf_counter() - started_at) * 1000),
                effective_page_size,
            )
        return cached_payload
    try:
        payload = social_profile_reads.get_profile_posts(
            platform=platform,
            account_handle=account_handle,
            page=page,
            page_size=effective_page_size,
            search=search,
            comments_only=comments_only,
            comment_filter=comment_filter,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )
        _set_ttl_cached_payload(
            _ACCOUNT_PROFILE_POSTS_CACHE,
            _ACCOUNT_PROFILE_POSTS_CACHE_LOCK,
            cache_key,
            payload,
            ttl_seconds=_ACCOUNT_PROFILE_CACHE_TTL_SECONDS,
            max_entries=_ACCOUNT_PROFILE_CACHE_MAX_ENTRIES,
        )
        if str(os.getenv("TRR_SOCIAL_PROFILE_PERF_DEBUG") or "").strip().lower() in {"1", "true", "yes", "on"}:
            logger.info(
                "[social-profile-perf] route=get_social_account_profile_posts_route platform=%s handle=%s "
                "cache_status=miss elapsed_ms=%s page_size=%s",
                platform,
                account_handle,
                int((perf_counter() - started_at) * 1000),
                effective_page_size,
            )
        return payload
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to fetch social account profile posts: platform=%s account=%s",
            platform,
            account_handle,
        )
        raise _to_social_read_http_exception(exc) from exc


@router.get("/profiles/{platform}/{account_handle}/comments")
def get_social_account_profile_comments_route(
    platform: str,
    account_handle: str,
    page: int = Query(default=1, ge=1, le=10_000),
    page_size: int = Query(default=25, ge=1, le=100),
    post_source_id: str | None = Query(default=None),
    search: str | None = Query(default=None),
    sort_by: str | None = Query(default=None),
    sort_dir: str | None = Query(default=None),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    started_at = perf_counter()
    cache_key = _account_profile_cache_key(
        surface="comments",
        platform=platform,
        account_handle=account_handle,
        page=page,
        page_size=page_size,
        post_source_id=post_source_id,
        search=search,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )
    cached_payload = _get_ttl_cached_payload(
        _ACCOUNT_PROFILE_POSTS_CACHE,
        _ACCOUNT_PROFILE_POSTS_CACHE_LOCK,
        cache_key,
    )
    if cached_payload is not None:
        log_read_path(
            "social-account-profile-comments",
            latency_ms=(perf_counter() - started_at) * 1000,
            payload=cached_payload,
            extra={
                "cache": "hit",
                "platform": platform,
                "account_handle": account_handle,
                "page": page,
                "page_size": page_size,
                "post_source_id": post_source_id,
                "search": search,
                "sort_by": sort_by,
                "sort_dir": sort_dir,
            },
        )
        return cached_payload
    try:
        payload = social_profile_reads.get_profile_comments(
            platform=platform,
            account_handle=account_handle,
            page=page,
            page_size=page_size,
            post_source_id=post_source_id,
            search=search,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )
        _set_ttl_cached_payload(
            _ACCOUNT_PROFILE_POSTS_CACHE,
            _ACCOUNT_PROFILE_POSTS_CACHE_LOCK,
            cache_key,
            payload,
            ttl_seconds=_ACCOUNT_PROFILE_CACHE_TTL_SECONDS,
            max_entries=_ACCOUNT_PROFILE_CACHE_MAX_ENTRIES,
        )
        log_read_path(
            "social-account-profile-comments",
            latency_ms=(perf_counter() - started_at) * 1000,
            payload=payload,
            extra={
                "cache": "miss",
                "platform": platform,
                "account_handle": account_handle,
                "page": page,
                "page_size": page_size,
                "post_source_id": post_source_id,
                "search": search,
                "sort_by": sort_by,
                "sort_dir": sort_dir,
            },
        )
        return payload
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to read social account profile comments: platform=%s account=%s page=%s page_size=%s",
            platform,
            account_handle,
            page,
            page_size,
        )
        raise _to_social_read_http_exception(exc) from exc


@router.get("/profiles/instagram/{account_handle}/profile")
def get_instagram_profile_detail_route(
    account_handle: str,
    source_scope: str = Query(default="network"),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.socials.instagram.profile_stages import get_instagram_profile_detail

    try:
        return get_instagram_profile_detail(account_handle=account_handle, source_scope=source_scope)
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch Instagram profile detail: account=%s", account_handle)
        raise _to_social_read_http_exception(exc) from exc


@router.get("/profiles/instagram/{account_handle}/relationships")
def get_instagram_profile_relationships_route(
    account_handle: str,
    relationship_type: Literal["following"] = Query(default="following", alias="type"),
    source_scope: str = Query(default="bravo"),
    page: int = Query(default=1, ge=1, le=10_000),
    page_size: int = Query(default=25, ge=1, le=100),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.socials.instagram.profile_stages import get_instagram_profile_relationships

    try:
        canonical_source_scope = preserve_source_scope_param(source_scope, default="bravo")
        return get_instagram_profile_relationships(
            account_handle=account_handle,
            source_scope=canonical_source_scope,
            relationship_type=relationship_type,
            page=page,
            page_size=page_size,
        )
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch Instagram profile relationships: account=%s", account_handle)
        raise _to_social_read_http_exception(exc) from exc


@router.get("/profiles/{platform}/{account_handle}/comments/audit-cursor-retries")
def get_social_account_comments_audit_cursor_retries_route(
    platform: str,
    account_handle: str,
    limit: int = Query(default=50, ge=1, le=500),
    stop_reason: list[str] | None = Query(default=None),
    shortcode: list[str] | None = Query(default=None),
    show_id: list[str] | None = Query(default=None),
    season_id: list[str] | None = Query(default=None),
    show_filter: list[str] | None = Query(default=None),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.socials.pipelines.comments.instagram import get_instagram_comments_audit_cursor_recovery

    if platform.strip().lower() != "instagram":
        raise HTTPException(
            status_code=400,
            detail={"code": "SOCIAL_ACCOUNT_COMMENTS_UNSUPPORTED_PLATFORM", "message": "Audit cursor retries are Instagram-only."},
        )
    try:
        return get_instagram_comments_audit_cursor_recovery(
            account_handle=account_handle,
            limit=limit,
            shortcodes=shortcode,
            stop_reasons=stop_reason,
            show_ids=show_id,
            season_ids=season_id,
            show_filters=show_filter,
        )
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc


@router.post("/profiles/{platform}/{account_handle}/comments/audit-cursor-retries")
async def post_social_account_comments_audit_cursor_retries_route(
    platform: str,
    account_handle: str,
    payload: SocialAccountCommentsAuditCursorRetryRequest,
    user: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import SocialIngestConflictError, SocialIngestValidationError
    from trr_backend.socials.pipelines.comments.instagram import enqueue_instagram_comments_audit_cursor_retries

    if platform.strip().lower() != "instagram":
        raise HTTPException(
            status_code=400,
            detail={"code": "SOCIAL_ACCOUNT_COMMENTS_UNSUPPORTED_PLATFORM", "message": "Audit cursor retries are Instagram-only."},
        )
    try:
        return await run_in_threadpool(
            enqueue_instagram_comments_audit_cursor_retries,
            account_handle=account_handle,
            limit=payload.limit,
            shortcodes=payload.shortcodes,
            stop_reasons=payload.stop_reasons,
            show_ids=payload.show_ids,
            season_ids=payload.season_ids,
            show_filters=[
                *list(payload.show_filters or []),
                *([payload.show_filter] if payload.show_filter else []),
            ],
            batch_size=payload.batch_size,
            comments_worker_count=payload.comments_worker_count,
            max_comments_per_post=payload.max_comments_per_post,
            comments_load_strategy=payload.comments_load_strategy,
            skip_launch_auth_probe=payload.skip_launch_auth_probe,
            dry_run=payload.dry_run,
            attach_to_active_run=payload.attach_to_active_run,
            dispatch_immediately=payload.dispatch_immediately,
            force_rerun_existing=payload.force_rerun_existing,
            initiated_by=(user or {}).get("email"),
        )
    except SocialIngestConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": exc.code, "message": str(exc), **jsonable_encoder(exc.detail)},
        ) from exc
    except SocialIngestValidationError as exc:
        raise HTTPException(
            status_code=400,
            detail={"code": exc.code, "message": str(exc), **jsonable_encoder(getattr(exc, "detail", {}) or {})},
        ) from exc


@router.post("/profiles/{platform}/{account_handle}/comments/scrape")
async def post_social_account_comments_scrape_route(
    platform: str,
    account_handle: str,
    payload: SocialAccountCommentsScrapeRequest,
    background_tasks: BackgroundTasks,
    user: InternalAdminUser,
    dry_run: bool = Query(default=False),
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import (
        INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
        INSTAGRAM_COMMENTS_SCRAPLING_WORKER_LANE,
        SocialIngestConflictError,
        SocialIngestValidationError,
        SocialWorkerUnavailableError,
        _dispatch_due_social_jobs_in_background,
    )
    from trr_backend.socials.pipelines.comments.instagram import (
        preview_social_account_comments_scrape,
        start_social_account_comments_scrape,
    )

    if dry_run or payload.dry_run:
        try:
            return await run_in_threadpool(
                preview_social_account_comments_scrape,
                platform=platform,
                account_handle=account_handle,
                mode=payload.mode,
                source_id=payload.source_id,
                max_posts=payload.max_posts,
                refresh_policy=payload.refresh_policy,
                target_filter=payload.target_filter,
                comments_load_strategy=payload.comments_load_strategy,
            )
        except SocialIngestValidationError as exc:
            raise HTTPException(status_code=400, detail={"code": exc.code, "message": str(exc)}) from exc

    execution_state = await run_in_threadpool(
        _resolve_social_account_comments_route_execution,
        allow_inline_dev_fallback=payload.allow_inline_dev_fallback,
        platform=platform,
    )
    queue_enabled = bool(execution_state["queue_enabled"])
    used_inline_fallback = bool(execution_state["used_inline_fallback"])
    requires_modal_executor = bool(execution_state["requires_modal_executor"])
    try:
        result = await run_in_threadpool(
            start_social_account_comments_scrape,
            platform=platform,
            account_handle=account_handle,
            mode=payload.mode,
            source_scope=payload.source_scope,
            source_id=payload.source_id,
            max_posts=payload.max_posts,
            max_comments_per_post=payload.max_comments_per_post,
            refresh_policy=payload.refresh_policy,
            target_filter=payload.target_filter,
            comments_load_strategy=payload.comments_load_strategy,
            initiated_by=(user or {}).get("email"),
            inline_worker_id=None if queue_enabled else f"api-background:comments:{platform}",
            allow_local_dev_inline_bypass=used_inline_fallback,
            dispatch_immediately=not queue_enabled,
        )
        _clear_account_profile_caches()
        if queue_enabled and result.get("run_id"):
            background_tasks.add_task(_dispatch_due_social_jobs_in_background, run_id=str(result["run_id"]))
        if not queue_enabled and result.get("run_id"):
            _start_runs_in_background(
                [str(result["run_id"])],
                background_tasks,
                worker_prefix=f"api-background:comments:{platform}",
                stage=INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
                platform="instagram",
                supported_platforms=["instagram"],
                metadata_updates={"worker_lane": INSTAGRAM_COMMENTS_SCRAPLING_WORKER_LANE},
            )
        return result
    except SocialIngestConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": exc.code, "message": str(exc), **jsonable_encoder(exc.detail)},
        ) from exc
    except SocialIngestValidationError as exc:
        status_code = 503 if exc.code == "SOCIAL_INSTAGRAM_COMMENTS_AUTH_REPAIR_FAILED" else 400
        raise HTTPException(
            status_code=status_code,
            detail={"code": exc.code, "message": str(exc), **jsonable_encoder(getattr(exc, "detail", {}) or {})},
        ) from exc
    except SocialWorkerUnavailableError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "SOCIAL_MODAL_EXECUTOR_REQUIRED" if requires_modal_executor else "SOCIAL_WORKER_UNAVAILABLE",
                "message": (
                    "Instagram comments scraping requires the Modal remote executor."
                    if requires_modal_executor
                    else str(exc)
                ),
                "execution_mode": canonical_execution_mode(),
                "execution_owner": execution_owner_label(),
                "worker_health": _worker_health_detail(exc.worker_health),
                "required_worker_lane": None if requires_modal_executor else INSTAGRAM_COMMENTS_SCRAPLING_WORKER_LANE,
                "required_execution_backend": "modal" if requires_modal_executor else None,
            },
        ) from exc


@router.get("/profiles/{platform}/{account_handle}/comments/runs/{run_id}/progress")
def get_social_account_comments_scrape_progress_route(
    platform: str,
    account_handle: str,
    run_id: UUID,
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.socials.pipelines.comments.instagram import get_social_account_comments_scrape_run_progress

    cache_key = _account_profile_cache_key(
        surface="comments-run-progress",
        platform=platform,
        account_handle=account_handle,
        extra=str(run_id),
    )
    try:
        return _resolve_account_profile_singleflight(
            cache_key,
            lambda: get_social_account_comments_scrape_run_progress(
                platform=platform,
                account_handle=account_handle,
                run_id=str(run_id),
                auto_rebalance_slow_shards=False,
            ),
        )
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc


@router.post("/profiles/{platform}/{account_handle}/comments/runs/{run_id}/rebalance")
def post_social_account_comments_run_rebalance_route(
    platform: str,
    account_handle: str,
    run_id: UUID,
    _: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.socials.pipelines.comments.instagram import rebalance_slow_instagram_comments_shards

    if platform.strip().lower() != "instagram":
        raise HTTPException(
            status_code=400,
            detail={
                "code": "UNSUPPORTED_PLATFORM",
                "message": "Comments shard rebalance is only supported for Instagram.",
            },
        )
    if not account_handle.strip():
        raise HTTPException(
            status_code=400,
            detail={"code": "ACCOUNT_HANDLE_REQUIRED", "message": "account_handle is required."},
        )
    try:
        result = rebalance_slow_instagram_comments_shards(run_id=str(run_id))
        _clear_account_profile_caches()
        return result
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc


@router.post("/profiles/{platform}/{account_handle}/comments/runs/{run_id}/resume")
def post_social_account_comments_run_resume_route(
    platform: str,
    account_handle: str,
    run_id: UUID,
    user: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import (
        SocialIngestConflictError,
        SocialIngestValidationError,
        SocialWorkerUnavailableError,
    )
    from trr_backend.socials.pipelines.comments.instagram import resume_social_account_comments_run

    try:
        result = resume_social_account_comments_run(
            platform=platform,
            account_handle=account_handle,
            run_id=str(run_id),
            initiated_by=(user or {}).get("email"),
        )
        _clear_account_profile_caches()
        return result
    except SocialIngestConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": exc.code, "message": str(exc), **jsonable_encoder(exc.detail)},
        ) from exc
    except SocialIngestValidationError as exc:
        status_code = 503 if exc.code == "SOCIAL_INSTAGRAM_COMMENTS_AUTH_REPAIR_FAILED" else 400
        raise HTTPException(status_code=status_code, detail={"code": exc.code, "message": str(exc)}) from exc
    except SocialWorkerUnavailableError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "SOCIAL_WORKER_UNAVAILABLE",
                "message": str(exc),
                "execution_mode": canonical_execution_mode(),
                "execution_owner": execution_owner_label(),
                "worker_health": _worker_health_detail(exc.worker_health),
            },
        ) from exc
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc


@router.post("/profiles/{platform}/{account_handle}/comments/runs/{run_id}/cancel")
def post_social_account_comments_run_cancel_route(
    platform: str,
    account_handle: str,
    run_id: UUID,
    user: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.socials.pipelines.comments.instagram import cancel_social_account_comments_run

    try:
        result = cancel_social_account_comments_run(
            platform=platform,
            account_handle=account_handle,
            run_id=str(run_id),
            cancelled_by=(user or {}).get("email"),
        )
        _clear_account_profile_caches()
        return result
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc


@router.post("/profiles/{platform}/{account_handle}/comments/runs/{run_id}/jobs/{job_id}/cancel")
def post_social_account_comments_job_cancel_route(
    platform: str,
    account_handle: str,
    run_id: UUID,
    job_id: UUID,
    user: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.socials.pipelines.comments.instagram import cancel_social_account_comments_job

    try:
        result = cancel_social_account_comments_job(
            platform=platform,
            account_handle=account_handle,
            run_id=str(run_id),
            job_id=str(job_id),
            cancelled_by=(user or {}).get("email"),
        )
        _clear_account_profile_caches()
        return result
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc


@router.get("/profiles/{platform}/{account_handle}/catalog/posts")
def get_social_account_catalog_posts_route(
    platform: str,
    account_handle: str,
    page: int = Query(default=1, ge=1, le=10_000),
    page_size: int = Query(default=25, ge=1, le=100),
    assignment_status: Literal["assigned", "unassigned", "ambiguous", "needs_review"] | None = Query(default=None),
    _: InternalAdminUser = None,
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
    _: InternalAdminUser = None,
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


@router.get("/profiles/{platform}/{account_handle}/hashtags")
def get_social_account_profile_hashtags_route(
    platform: str,
    account_handle: str,
    window: Literal["all", "30d", "365d"] | None = Query(default=None),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    cache_key = _account_profile_cache_key(
        surface="hashtags",
        platform=platform,
        account_handle=account_handle,
        window=window,
    )
    cached_payload = _get_ttl_cached_payload(
        _ACCOUNT_PROFILE_HASHTAGS_CACHE,
        _ACCOUNT_PROFILE_HASHTAGS_CACHE_LOCK,
        cache_key,
    )
    if cached_payload is not None:
        return cached_payload
    try:
        payload = social_profile_reads.get_profile_hashtags(
            platform=platform,
            account_handle=account_handle,
            window=window,
        )
        _set_ttl_cached_payload(
            _ACCOUNT_PROFILE_HASHTAGS_CACHE,
            _ACCOUNT_PROFILE_HASHTAGS_CACHE_LOCK,
            cache_key,
            payload,
            ttl_seconds=_ACCOUNT_PROFILE_CACHE_TTL_SECONDS,
            max_entries=_ACCOUNT_PROFILE_CACHE_MAX_ENTRIES,
        )
        return payload
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to fetch social account profile hashtags: platform=%s account=%s",
            platform,
            account_handle,
        )
        raise _to_social_read_http_exception(exc) from exc


@router.get("/profiles/{platform}/{account_handle}/hashtags/timeline")
def get_social_account_profile_hashtag_timeline_route(
    platform: str,
    account_handle: str,
    window: Literal["all", "30d", "365d"] | None = Query(default=None),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    cache_key = _account_profile_cache_key(
        surface="hashtags_timeline",
        platform=platform,
        account_handle=account_handle,
        window=window,
    )
    cached_payload = _get_ttl_cached_payload(
        _ACCOUNT_PROFILE_HASHTAG_TIMELINE_CACHE,
        _ACCOUNT_PROFILE_HASHTAG_TIMELINE_CACHE_LOCK,
        cache_key,
    )
    if cached_payload is not None:
        return cached_payload
    try:
        payload = social_profile_reads.get_profile_hashtag_timeline(
            platform=platform,
            account_handle=account_handle,
            window=window,
        )
        _set_ttl_cached_payload(
            _ACCOUNT_PROFILE_HASHTAG_TIMELINE_CACHE,
            _ACCOUNT_PROFILE_HASHTAG_TIMELINE_CACHE_LOCK,
            cache_key,
            payload,
            ttl_seconds=_ACCOUNT_PROFILE_CACHE_TTL_SECONDS,
            max_entries=_ACCOUNT_PROFILE_CACHE_MAX_ENTRIES,
        )
        return payload
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc


@router.get("/profiles/{platform}/{account_handle}/catalog/review-queue")
def get_social_account_catalog_review_queue_route(
    platform: str,
    account_handle: str,
    _: InternalAdminUser = None,
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


@router.put("/profiles/{platform}/{account_handle}/hashtags")
def put_social_account_profile_hashtags_route(
    platform: str,
    account_handle: str,
    payload: SocialAccountProfileHashtagsPutRequest,
    user: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import put_social_account_profile_hashtags

    try:
        response = put_social_account_profile_hashtags(
            platform=platform,
            account_handle=account_handle,
            hashtags=[item.model_dump() for item in payload.hashtags],
            updated_by=(user or {}).get("email"),
        )
        _clear_account_profile_caches()
        return response
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc


@router.get("/profiles/{platform}/{account_handle}/catalog/runs/{run_id}/progress")
def get_social_account_catalog_run_progress_route(
    platform: str,
    account_handle: str,
    run_id: UUID,
    recent_log_limit: int = Query(default=20, ge=1, le=100),
    fast: bool = Query(default=False),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    try:
        return _resolve_account_profile_catalog_run_progress(
            platform=platform,
            account_handle=account_handle,
            run_id=str(run_id),
            recent_log_limit=recent_log_limit,
            fast=fast,
            loader=social_profile_reads.get_catalog_run_progress,
        )
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


@router.get("/profiles/{platform}/{account_handle}/catalog/verification")
def get_social_account_catalog_verification_route(
    platform: str,
    account_handle: str,
    run_id: UUID | None = None,
    _: InternalAdminUser = None,
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
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/profiles/{platform}/{account_handle}/catalog/gap-analysis")
def get_social_account_catalog_gap_analysis_route(
    platform: str,
    account_handle: str,
    _: InternalAdminUser = None,
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


def _to_optional_request_header_value(value: str | None) -> str | None:
    if not value:
        return None
    normalized = str(value).strip()
    return normalized or None


def _start_social_catalog_gap_analysis_operation(
    *,
    platform: str,
    account_handle: str,
    request: Request,
) -> dict[str, Any]:
    from trr_backend.pipeline.admin_operations import ensure_operation_execution
    from trr_backend.repositories import admin_operations as admin_operations_repo
    from trr_backend.repositories.social_season_analytics import (
        SOCIAL_CATALOG_GAP_ANALYSIS_OPERATION_TYPE,
        build_social_account_catalog_gap_analysis_operation_producer,
    )

    request_payload = {
        "platform": platform,
        "account_handle": account_handle,
    }
    producer = build_social_account_catalog_gap_analysis_operation_producer(request_payload=request_payload)
    request_id = _to_optional_request_header_value(request.headers.get("x-trr-request-id"))
    client_session_id = _to_optional_request_header_value(request.headers.get("x-trr-tab-session-id"))
    client_workflow_id = _to_optional_request_header_value(request.headers.get("x-trr-flow-key"))

    operation, attached = admin_operations_repo.create_or_attach_operation(
        operation_type=SOCIAL_CATALOG_GAP_ANALYSIS_OPERATION_TYPE,
        request_payload=request_payload,
        initiated_by=None,
        request_id=request_id,
        client_session_id=client_session_id,
        client_workflow_id=client_workflow_id,
        allow_attach=True,
    )
    operation_id = str(operation.get("id") or "").strip()
    if not operation_id:
        raise RuntimeError("Failed to create social catalog gap-analysis operation")
    if not attached:
        ensure_operation_execution(operation_id, producer=producer, request_id=request_id)

    refreshed = admin_operations_repo.get_operation(operation_id) or operation
    refreshed["attached"] = attached
    return refreshed


@router.post("/profiles/{platform}/{account_handle}/catalog/gap-analysis/run")
def post_social_account_catalog_gap_analysis_run_route(
    platform: str,
    account_handle: str,
    request: Request,
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    try:
        operation = _start_social_catalog_gap_analysis_operation(
            platform=platform,
            account_handle=account_handle,
            request=request,
        )
        payload = social_profile_reads.get_catalog_gap_analysis_status(
            platform=platform,
            account_handle=account_handle,
        )
        payload["attached"] = bool(operation.get("attached"))
        payload["operation_id"] = str(operation.get("id") or payload.get("operation_id") or "").strip() or None
        return payload
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to start social account catalog gap analysis: platform=%s account=%s",
            platform,
            account_handle,
        )
        raise _to_social_read_http_exception(exc) from exc


@router.post("/profiles/{platform}/{account_handle}/catalog/runs/{run_id}/cancel")
def post_social_account_catalog_run_cancel_route(
    platform: str,
    account_handle: str,
    run_id: UUID,
    background_tasks: BackgroundTasks,
    user: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import (
        request_cancel_social_account_catalog_run,
    )

    try:
        result = request_cancel_social_account_catalog_run(
            platform=platform,
            account_handle=account_handle,
            run_id=str(run_id),
            cancelled_by=(user or {}).get("email"),
        )
        background_tasks.add_task(
            _cancel_catalog_run_in_background,
            platform=platform,
            account_handle=account_handle,
            run_id=str(run_id),
            cancelled_by=(user or {}).get("email"),
        )
        return result
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc


@router.post("/profiles/{platform}/{account_handle}/catalog/runs/{run_id}/dismiss")
def post_social_account_catalog_run_dismiss_route(
    platform: str,
    account_handle: str,
    run_id: UUID,
    user: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import dismiss_social_account_catalog_run

    try:
        result = dismiss_social_account_catalog_run(
            platform=platform,
            account_handle=account_handle,
            run_id=str(run_id),
            dismissed_by=(user or {}).get("email"),
        )
        _clear_account_profile_caches()
        return result
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc


@router.post("/profiles/{platform}/{account_handle}/catalog/review-queue/{item_id}/resolve")
def post_social_account_catalog_review_queue_resolve_route(
    platform: str,
    account_handle: str,
    item_id: UUID,
    payload: CatalogReviewResolveRequest,
    user: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import resolve_social_account_catalog_review_queue_item

    del platform, account_handle
    try:
        response = resolve_social_account_catalog_review_queue_item(
            item_id=str(item_id),
            resolution_action=payload.resolution_action,
            show_id=str(payload.show_id) if payload.show_id else None,
            updated_by=(user or {}).get("email"),
        )
        _clear_account_profile_caches()
        return response
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc


@router.post("/profiles/{platform}/{account_handle}/catalog/freshness")
def post_social_account_catalog_freshness_route(
    platform: str,
    account_handle: str,
    force: bool = Query(default=False, description="Bypass cached freshness and run the deep live-profile probe"),
    statement_timeout_ms: int = Query(default=3000, ge=1000, le=30000),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    try:
        return _resolve_account_profile_catalog_freshness(
            platform=platform,
            account_handle=account_handle,
            force=bool(force),
            statement_timeout_ms=statement_timeout_ms,
            loader=social_profile_reads.get_catalog_freshness,
        )
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to fetch social account catalog freshness: platform=%s account=%s",
            platform,
            account_handle,
        )
        raise _to_social_read_http_exception(exc) from exc


@router.get("/profiles/{platform}/{account_handle}/collaborators-tags")
def get_social_account_profile_collaborators_tags_route(
    platform: str,
    account_handle: str,
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    cache_key = _account_profile_cache_key(
        surface="collaborators-tags",
        platform=platform,
        account_handle=account_handle,
    )
    cached_payload = _get_ttl_cached_payload(
        _ACCOUNT_PROFILE_COLLABORATORS_CACHE,
        _ACCOUNT_PROFILE_COLLABORATORS_CACHE_LOCK,
        cache_key,
    )
    if cached_payload is not None:
        return cached_payload
    try:
        payload = social_profile_reads.get_profile_collaborators_tags(
            platform=platform,
            account_handle=account_handle,
        )
        _set_ttl_cached_payload(
            _ACCOUNT_PROFILE_COLLABORATORS_CACHE,
            _ACCOUNT_PROFILE_COLLABORATORS_CACHE_LOCK,
            cache_key,
            payload,
            ttl_seconds=_ACCOUNT_PROFILE_CACHE_TTL_SECONDS,
            max_entries=_ACCOUNT_PROFILE_CACHE_MAX_ENTRIES,
        )
        return payload
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc


@router.post("/profiles/{platform}/{account_handle}/catalog/backfill")
async def post_social_account_catalog_backfill_route(
    platform: str,
    account_handle: str,
    payload: CatalogBackfillRequest,
    background_tasks: BackgroundTasks,
    user: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import (
        SocialIngestConflictError,
        SocialIngestValidationError,
        SocialWorkerUnavailableError,
        _normalize_catalog_backfill_window,
        _normalize_social_account_catalog_backfill_selected_tasks,
        begin_social_account_catalog_backfill_launch,
        launch_social_account_catalog_backfill,
    )

    execution_state = await run_in_threadpool(
        _resolve_social_account_catalog_route_execution,
        platform=platform,
        allow_inline_dev_fallback=payload.allow_inline_dev_fallback,
        execution_preference=payload.execution_preference,
    )
    queue_enabled = bool(execution_state["queue_enabled"])
    used_inline_fallback = bool(execution_state["used_inline_fallback"])
    requires_modal_executor = bool(execution_state["requires_modal_executor"])

    try:
        normalized_platform = str(platform or "").strip().lower()
        if normalized_platform == "twitter" and payload.backfill_scope == "full_history":
            date_start, date_end = _twitter_catalog_backfill_default_window()
        else:
            date_start = payload.date_start if payload.backfill_scope == "bounded_window" else None
            date_end = payload.date_end if payload.backfill_scope == "bounded_window" else None
        date_start, date_end = _normalize_catalog_backfill_window(
            date_start=date_start,
            date_end=date_end,
        )
        request_selected_tasks = payload.selected_tasks
        if request_selected_tasks is None:
            request_selected_tasks = (
                ["post_details"]
                if normalized_platform == "instagram"
                else _normalize_social_account_catalog_backfill_selected_tasks(None)
            )
        normalized_selected_tasks = _normalize_social_account_catalog_backfill_selected_tasks(request_selected_tasks)
        use_async_catalog_kickoff = queue_enabled or (used_inline_fallback and normalized_platform == "instagram")
        if use_async_catalog_kickoff:
            result = await run_in_threadpool(
                begin_social_account_catalog_backfill_launch,
                platform=platform,
                account_handle=account_handle,
                source_scope=payload.source_scope,
                date_start=date_start,
                date_end=date_end,
                initiated_by=(user or {}).get("email"),
                allow_local_dev_inline_bypass=used_inline_fallback,
                execution_preference=payload.execution_preference,
                selected_tasks=normalized_selected_tasks,
                details_refresh_worker_count=payload.detail_worker_count,
                comments_worker_count=payload.comments_worker_count,
                comments_enable_media_followups=payload.comments_enable_media_followups,
                force_catalog_rediscovery=payload.force_catalog_rediscovery,
            )
            _queue_catalog_backfill_finalize_task(
                background_tasks=background_tasks,
                platform=platform,
                account_handle=account_handle,
                run_id=str(result.get("run_id") or ""),
                source_scope=payload.source_scope,
                date_start=date_start,
                date_end=date_end,
                initiated_by=(user or {}).get("email"),
                allow_local_dev_inline_bypass=used_inline_fallback,
                execution_preference=payload.execution_preference,
                selected_tasks=normalized_selected_tasks,
                details_refresh_worker_count=payload.detail_worker_count,
                comments_worker_count=payload.comments_worker_count,
                comments_enable_media_followups=payload.comments_enable_media_followups,
                launch_group_id=str(result.get("launch_group_id") or ""),
                force_catalog_rediscovery=payload.force_catalog_rediscovery,
            )
        else:
            result = await run_in_threadpool(
                launch_social_account_catalog_backfill,
                platform=platform,
                account_handle=account_handle,
                source_scope=payload.source_scope,
                date_start=date_start,
                date_end=date_end,
                initiated_by=(user or {}).get("email"),
                inline_worker_id=None if queue_enabled else f"api-background:catalog:{platform}",
                allow_local_dev_inline_bypass=used_inline_fallback,
                execution_preference=payload.execution_preference,
                selected_tasks=normalized_selected_tasks,
                details_refresh_worker_count=payload.detail_worker_count,
                comments_worker_count=payload.comments_worker_count,
                comments_enable_media_followups=payload.comments_enable_media_followups,
                force_catalog_rediscovery=payload.force_catalog_rediscovery,
            )
        _clear_account_profile_caches()
    except SocialIngestConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": exc.code, "message": str(exc), **jsonable_encoder(exc.detail)},
        ) from exc
    except SocialIngestValidationError as exc:
        status_code = 503 if exc.code == "SOCIAL_INSTAGRAM_COMMENTS_AUTH_REPAIR_FAILED" else 400
        raise HTTPException(status_code=status_code, detail={"code": exc.code, "message": str(exc)}) from exc
    except SocialWorkerUnavailableError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "code": ("SOCIAL_MODAL_EXECUTOR_REQUIRED" if requires_modal_executor else "SOCIAL_WORKER_UNAVAILABLE"),
                "message": str(exc),
                "execution_mode": canonical_execution_mode(),
                "execution_owner": execution_owner_label(),
                "worker_health": _worker_health_detail(exc.worker_health),
                "required_execution_backend": "modal" if requires_modal_executor else None,
            },
        ) from exc
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc

    return _finalize_social_account_catalog_route_response(
        result=result,
        platform=platform,
        queue_enabled=queue_enabled,
        used_inline_fallback=used_inline_fallback,
        requires_modal_executor=requires_modal_executor,
        background_tasks=background_tasks,
    )


@router.post("/profiles/{platform}/{account_handle}/catalog/remediate-drift")
async def post_social_account_catalog_remediate_drift_route(
    platform: str,
    account_handle: str,
    payload: CatalogRemediateDriftRequest,
    user: InternalAdminUser,
) -> dict[str, Any]:
    """Cancel and optionally replace a stale catalog run under the runtime-supersession guard."""
    from trr_backend.repositories.social_season_analytics import (
        remediate_social_account_catalog_runtime_supersession,
    )

    initiated_by = (user or {}).get("email")
    try:
        return remediate_social_account_catalog_runtime_supersession(
            platform=platform,
            account_handle=account_handle,
            run_id=str(payload.run_id) if payload.run_id else None,
            requeue_canary=payload.requeue_canary,
            source_scope=payload.source_scope,
            initiated_by=initiated_by,
            cancelled_by=initiated_by,
        )
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc


@router.post("/profiles/{platform}/{account_handle}/catalog/sync-recent")
async def post_social_account_catalog_sync_recent_route(
    platform: str,
    account_handle: str,
    payload: CatalogSyncRecentRequest,
    background_tasks: BackgroundTasks,
    user: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import (
        SocialIngestConflictError,
        SocialIngestValidationError,
        SocialWorkerUnavailableError,
        sync_recent_social_account_catalog,
    )

    execution_state = await run_in_threadpool(
        _resolve_social_account_catalog_route_execution,
        platform=platform,
        allow_inline_dev_fallback=payload.allow_inline_dev_fallback,
    )
    queue_enabled = bool(execution_state["queue_enabled"])
    used_inline_fallback = bool(execution_state["used_inline_fallback"])
    requires_modal_executor = bool(execution_state["requires_modal_executor"])

    try:
        result = sync_recent_social_account_catalog(
            platform=platform,
            account_handle=account_handle,
            source_scope=payload.source_scope,
            lookback_days=payload.lookback_days,
            initiated_by=(user or {}).get("email"),
            inline_worker_id=None if queue_enabled else f"api-background:catalog:{platform}",
            allow_local_dev_inline_bypass=used_inline_fallback,
        )
        _clear_account_profile_caches()
    except SocialIngestConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": exc.code, "message": str(exc), **jsonable_encoder(exc.detail)},
        ) from exc
    except SocialIngestValidationError as exc:
        raise HTTPException(status_code=400, detail={"code": exc.code, "message": str(exc)}) from exc
    except SocialWorkerUnavailableError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "SOCIAL_MODAL_EXECUTOR_REQUIRED" if requires_modal_executor else "SOCIAL_WORKER_UNAVAILABLE",
                "message": str(exc),
                "execution_mode": canonical_execution_mode(),
                "execution_owner": execution_owner_label(),
                "worker_health": _worker_health_detail(exc.worker_health),
                "required_execution_backend": "modal" if requires_modal_executor else None,
            },
        ) from exc
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc

    return _finalize_social_account_catalog_route_response(
        result=result,
        platform=platform,
        queue_enabled=queue_enabled,
        used_inline_fallback=used_inline_fallback,
        requires_modal_executor=requires_modal_executor,
        background_tasks=background_tasks,
    )


@router.post("/profiles/{platform}/{account_handle}/catalog/sync-newer")
async def post_social_account_catalog_sync_newer_route(
    platform: str,
    account_handle: str,
    payload: CatalogSyncNewerRequest,
    background_tasks: BackgroundTasks,
    user: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import (
        SocialIngestConflictError,
        SocialIngestValidationError,
        SocialWorkerUnavailableError,
        sync_newer_social_account_catalog,
    )

    execution_state = await run_in_threadpool(
        _resolve_social_account_catalog_route_execution,
        platform=platform,
        allow_inline_dev_fallback=payload.allow_inline_dev_fallback,
    )
    queue_enabled = bool(execution_state["queue_enabled"])
    used_inline_fallback = bool(execution_state["used_inline_fallback"])
    requires_modal_executor = bool(execution_state["requires_modal_executor"])

    try:
        result = sync_newer_social_account_catalog(
            platform=platform,
            account_handle=account_handle,
            source_scope=payload.source_scope,
            initiated_by=(user or {}).get("email"),
            inline_worker_id=None if queue_enabled else f"api-background:catalog:{platform}",
            allow_local_dev_inline_bypass=used_inline_fallback,
        )
        _clear_account_profile_caches()
    except SocialIngestConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": exc.code, "message": str(exc), **jsonable_encoder(exc.detail)},
        ) from exc
    except SocialIngestValidationError as exc:
        raise HTTPException(status_code=400, detail={"code": exc.code, "message": str(exc)}) from exc
    except SocialWorkerUnavailableError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "SOCIAL_MODAL_EXECUTOR_REQUIRED" if requires_modal_executor else "SOCIAL_WORKER_UNAVAILABLE",
                "message": str(exc),
                "execution_mode": canonical_execution_mode(),
                "execution_owner": execution_owner_label(),
                "worker_health": _worker_health_detail(exc.worker_health),
                "required_execution_backend": "modal" if requires_modal_executor else None,
            },
        ) from exc
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc

    return _finalize_social_account_catalog_route_response(
        result=result,
        platform=platform,
        queue_enabled=queue_enabled,
        used_inline_fallback=used_inline_fallback,
        requires_modal_executor=requires_modal_executor,
        background_tasks=background_tasks,
    )


@router.post("/profiles/{platform}/{account_handle}/catalog/resume-tail")
async def post_social_account_catalog_resume_tail_route(
    platform: str,
    account_handle: str,
    payload: CatalogResumeTailRequest,
    background_tasks: BackgroundTasks,
    user: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import (
        SOCIAL_ACCOUNT_CATALOG_BACKFILL_SELECTED_TASKS,
        SocialIngestConflictError,
        SocialIngestValidationError,
        SocialWorkerUnavailableError,
        launch_social_account_catalog_backfill,
    )

    execution_state = await run_in_threadpool(
        _resolve_social_account_catalog_route_execution,
        platform=platform,
        allow_inline_dev_fallback=payload.allow_inline_dev_fallback,
    )
    queue_enabled = bool(execution_state["queue_enabled"])
    used_inline_fallback = bool(execution_state["used_inline_fallback"])
    requires_modal_executor = bool(execution_state["requires_modal_executor"])

    try:
        result = await run_in_threadpool(
            launch_social_account_catalog_backfill,
            platform=platform,
            account_handle=account_handle,
            source_scope=payload.source_scope,
            initiated_by=(user or {}).get("email"),
            inline_worker_id=None if queue_enabled else f"api-background:catalog:{platform}",
            allow_local_dev_inline_bypass=used_inline_fallback,
            selected_tasks=list(SOCIAL_ACCOUNT_CATALOG_BACKFILL_SELECTED_TASKS),
        )
        _clear_account_profile_caches()
    except SocialIngestConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": exc.code, "message": str(exc), **jsonable_encoder(exc.detail)},
        ) from exc
    except SocialIngestValidationError as exc:
        raise HTTPException(status_code=400, detail={"code": exc.code, "message": str(exc)}) from exc
    except SocialWorkerUnavailableError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "SOCIAL_MODAL_EXECUTOR_REQUIRED" if requires_modal_executor else "SOCIAL_WORKER_UNAVAILABLE",
                "message": str(exc),
                "execution_mode": canonical_execution_mode(),
                "execution_owner": execution_owner_label(),
                "worker_health": _worker_health_detail(exc.worker_health),
                "required_execution_backend": "modal" if requires_modal_executor else None,
            },
        ) from exc
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc

    return {
        **_finalize_social_account_catalog_route_response(
            result=result,
            platform=platform,
            queue_enabled=queue_enabled,
            used_inline_fallback=used_inline_fallback,
            requires_modal_executor=requires_modal_executor,
            background_tasks=background_tasks,
        ),
        "deprecated_route": True,
    }


@router.post("/profiles/{platform}/{account_handle}/catalog/runs/{run_id}/manual-auth")
@router.post("/profiles/{platform}/{account_handle}/catalog/runs/{run_id}/repair-auth")
async def post_social_account_catalog_run_repair_auth_route(
    platform: str,
    account_handle: str,
    run_id: UUID,
    payload: CatalogRepairAuthRequest,
    background_tasks: BackgroundTasks,
    user: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import (
        SocialIngestValidationError,
        execute_social_account_catalog_run_auth_repair,
        request_social_account_catalog_run_auth_repair,
    )

    try:
        _require_instagram_auth_refresh_confirmation(platform, payload.operator_confirmation)
        result = request_social_account_catalog_run_auth_repair(
            platform=platform,
            account_handle=account_handle,
            run_id=str(run_id),
            initiated_by=(user or {}).get("email"),
        )
        background_tasks.add_task(
            execute_social_account_catalog_run_auth_repair,
            platform=platform,
            account_handle=account_handle,
            run_id=str(run_id),
            initiated_by=(user or {}).get("email"),
            allow_cookie_refresh=bool(payload.allow_cookie_refresh),
        )
        _clear_account_profile_caches()
        return result
    except SocialIngestValidationError as exc:
        raise HTTPException(status_code=400, detail={"code": exc.code, "message": str(exc)}) from exc
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc


class CookieRefreshRequest(BaseModel):
    headless: bool = Field(
        default=False,
        description="Run browser in headless mode (default: headed for interactive login)",
    )
    timeout_seconds: int = Field(default=180, ge=30, le=600)
    operator_confirmation: str | None = Field(default=None)
    allow_cookie_refresh: bool = Field(default=True)


def _require_instagram_auth_refresh_confirmation(platform: str, confirmation: str | None) -> None:
    normalized = str(platform or "").strip().lower()
    if normalized != "instagram":
        return
    if str(confirmation or "").strip() == INSTAGRAM_AUTH_REFRESH_CONFIRMATION:
        return
    raise HTTPException(
        status_code=400,
        detail={
            "code": "INSTAGRAM_AUTH_REFRESH_CONFIRMATION_REQUIRED",
            "message": INSTAGRAM_AUTH_REFRESH_WARNING,
            "required_confirmation": INSTAGRAM_AUTH_REFRESH_CONFIRMATION,
        },
    )


def _env_truthy_default(name: str, *, default: bool) -> bool:
    raw = str(os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on", "enabled"}


def _instagram_comments_auth_probe_allows_rendered_fallback(
    *,
    status: str,
    reason: str | None,
) -> bool:
    del status, reason
    return False


def _cookie_health_auth_probe_metadata(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **dict(payload),
        "probe_only": True,
        "probe_source": "cookie_health",
        "repair_action": None,
        "repair_available": False,
    }


@router.get("/profiles/{platform}/{account_handle}/cookies/health")
def get_cookie_health_route(
    platform: str,
    account_handle: str,
    force: bool = Query(default=False, description="Bypass validation cache"),
    posts_auth: bool = Query(default=False, description="Include Instagram Modal posts endpoint auth probe"),
    comments_auth: bool = Query(default=False, description="Include Instagram Modal comments endpoint auth probe"),
    user: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import (
        check_platform_cookie_health,
        probe_modal_instagram_comments_auth_health,
        probe_modal_instagram_posts_auth_health,
    )

    health = check_platform_cookie_health(platform, force=force)
    auth_probe_blocked = False
    auth_probe_reason: str | None = None
    local_cookie_fingerprint = str(health.get("cookie_fingerprint") or "").strip() or None
    if posts_auth and str(platform or "").strip().lower() == "instagram":
        posts_probe = _cookie_health_auth_probe_metadata(probe_modal_instagram_posts_auth_health(account_handle))
        posts_cookie_fingerprint = str(posts_probe.get("cookie_fingerprint") or "").strip() or None
        posts_status = (
            str(posts_probe.get("status") or posts_probe.get("result") or ("valid" if posts_probe.get("ready") else ""))
            .strip()
            .lower()
        )
        posts_reason = str(posts_probe.get("reason") or "").strip() or None
        posts_category = (
            "ready"
            if bool(posts_probe.get("ready")) or posts_status == "valid"
            else "transport"
            if posts_status == "transport_blocked"
            else "auth"
            if posts_status == "auth_blocked"
            else "fetch"
            if posts_status == "fetch_blocked"
            else "unknown"
        )
        health = {
            **health,
            "posts_auth_health": {
                "platform": "instagram",
                "account_handle": str(posts_probe.get("account_handle") or account_handle).strip() or account_handle,
                "ready": bool(posts_probe.get("ready")),
                "status": posts_status or None,
                "category": posts_category,
                "reason": posts_reason,
                "execution_backend": str(posts_probe.get("execution_backend") or "modal").strip().lower() or "modal",
                "probe_only": True,
                "probe_source": "cookie_health",
                "repair_action": None,
                "repair_available": False,
                "cookie_fingerprint": posts_cookie_fingerprint,
                "cookie_fingerprint_match": (
                    local_cookie_fingerprint == posts_cookie_fingerprint
                    if local_cookie_fingerprint and posts_cookie_fingerprint
                    else None
                ),
            },
            "posts_auth_probe": posts_probe,
        }
        if posts_category == "auth":
            auth_probe_blocked = True
            auth_probe_reason = posts_reason or "posts_auth_blocked"
    if comments_auth and str(platform or "").strip().lower() == "instagram":
        comments_probe = _cookie_health_auth_probe_metadata(probe_modal_instagram_comments_auth_health(account_handle))
        comments_cookie_fingerprint = str(comments_probe.get("cookie_fingerprint") or "").strip() or None
        comments_status = (
            str(
                comments_probe.get("status")
                or comments_probe.get("result")
                or ("valid" if comments_probe.get("ready") else "")
            )
            .strip()
            .lower()
        )
        comments_reason = str(comments_probe.get("reason") or "").strip() or None
        comments_category = (
            "ready"
            if bool(comments_probe.get("ready")) or comments_status == "valid"
            else "transport"
            if comments_status == "transport_blocked"
            else "auth"
            if comments_status == "auth_blocked"
            else "fetch"
            if comments_status == "fetch_blocked"
            else "unknown"
        )
        comments_auth_health = {
            "platform": "instagram",
            "account_handle": str(comments_probe.get("account_handle") or account_handle).strip() or account_handle,
            "shortcode": str(comments_probe.get("shortcode") or "").strip() or None,
            "ready": bool(comments_probe.get("ready")),
            "status": comments_status or None,
            "category": comments_category,
            "reason": comments_reason,
            "execution_backend": str(comments_probe.get("execution_backend") or "modal").strip().lower() or "modal",
            "probe_only": True,
            "probe_source": "cookie_health",
            "repair_action": None,
            "repair_available": False,
        }
        if comments_cookie_fingerprint:
            comments_auth_health["cookie_fingerprint"] = comments_cookie_fingerprint
        if local_cookie_fingerprint and comments_cookie_fingerprint:
            comments_auth_health["cookie_fingerprint_match"] = local_cookie_fingerprint == comments_cookie_fingerprint
        health = {
            **health,
            "comments_auth_health": comments_auth_health,
            "comments_auth_probe": comments_probe,
        }
        if comments_category == "auth":
            auth_probe_blocked = True
            auth_probe_reason = comments_reason or "comments_auth_blocked"
    if auth_probe_blocked:
        health = {
            **health,
            "healthy": False,
            "reason": auth_probe_reason,
            "auth_surface_blocked": True,
            "auth_surface_probe_only": True,
        }
    return health


@router.post("/profiles/{platform}/{account_handle}/cookies/refresh")
def post_cookie_refresh_route(
    platform: str,
    account_handle: str,
    payload: CookieRefreshRequest,
    user: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import (
        check_platform_cookie_health,
        refresh_platform_cookies_interactive,
    )

    # Pre-check: is refresh available in this runtime?
    health = check_platform_cookie_health(platform, force=False)
    if not health.get("refresh_supported"):
        raise HTTPException(
            status_code=400,
            detail={
                "code": "COOKIE_REFRESH_NOT_SUPPORTED",
                "message": f"Cookie refresh is not supported for {platform}.",
            },
        )
    if not health.get("refresh_available"):
        raise HTTPException(
            status_code=403,
            detail={
                "code": "COOKIE_REFRESH_REQUIRES_LOCAL",
                "message": (
                    "Cookie refresh requires a local dev environment. A headed browser cannot run on remote workers."
                ),
            },
        )
    _require_instagram_auth_refresh_confirmation(platform, payload.operator_confirmation)

    result = refresh_platform_cookies_interactive(
        platform,
        headless=payload.headless,
        timeout_seconds=payload.timeout_seconds,
        account_handle=account_handle,
        allow_cookie_refresh=bool(payload.allow_cookie_refresh),
    )
    if not result.get("success") and result.get("reason") == "refresh_already_in_progress":
        raise HTTPException(
            status_code=409,
            detail={
                "code": "COOKIE_REFRESH_IN_PROGRESS",
                "message": "A cookie refresh is already in progress for this platform.",
            },
        )
    return result


@router.post("/shared/ingest")
async def ingest_shared_social_accounts(
    payload: SharedSocialIngestRequest,
    background_tasks: BackgroundTasks,
    user: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import (
        SocialIngestValidationError,
        SocialWorkerUnavailableError,
        assert_worker_available_when_queue_enabled,
        ingest_shared_accounts,
        is_queue_enabled,
    )

    queue_enabled = is_queue_enabled()
    remote_plane_enforced = is_remote_job_plane_enabled()
    blocked_platforms = _blocked_remote_only_platforms(payload.platforms)
    requires_modal_executor = bool(blocked_platforms)
    used_inline_fallback = False
    worker_health: dict[str, Any] | None = None
    if queue_enabled:
        try:
            worker_health = await run_in_threadpool(
                assert_worker_available_when_queue_enabled,
                required_execution_backend="modal" if requires_modal_executor else None,
            )
        except SocialWorkerUnavailableError as exc:
            worker_health = exc.worker_health
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
                        "worker_health": _worker_health_detail(exc.worker_health),
                    },
                ) from exc
            if payload.allow_inline_dev_fallback and _is_local_or_dev_runtime() and not remote_plane_enforced:
                queue_enabled = False
                used_inline_fallback = True
            else:
                raise HTTPException(
                    status_code=503,
                    detail={
                        "code": (
                            "SOCIAL_REMOTE_JOB_PLANE_ENFORCED" if remote_plane_enforced else "SOCIAL_WORKER_UNAVAILABLE"
                        ),
                        "message": _remote_worker_unavailable_message(exc) if remote_plane_enforced else str(exc),
                        "execution_mode": canonical_execution_mode(),
                        "execution_owner": execution_owner_label(),
                        "worker_health": _worker_health_detail(exc.worker_health),
                    },
                ) from exc
    elif requires_modal_executor:
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
    elif not remote_plane_enforced:
        used_inline_fallback = bool(payload.allow_inline_dev_fallback)
    else:
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

    try:
        result = ingest_shared_accounts(
            platforms=payload.platforms,
            source_scope=payload.source_scope,
            accounts_override=payload.accounts_override,
            date_start=payload.date_start,
            date_end=payload.date_end,
            initiated_by=(user or {}).get("email"),
            inline_worker_id=None if queue_enabled else "api-background:shared",
        )
    except SocialIngestValidationError as exc:
        raise HTTPException(status_code=400, detail={"code": exc.code, "message": str(exc)}) from exc
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc

    run_id = str(result.get("run_id") or "").strip()
    if not run_id:
        raise HTTPException(
            status_code=500,
            detail={"code": "INGEST_RUN_NOT_CREATED", "message": "Failed to create shared ingest run"},
        )
    if not queue_enabled:
        _start_runs_in_background([run_id], background_tasks, worker_prefix="api-background:shared")

    execution_mode, execution_mode_canonical, execution_mode_legacy = _resolve_social_execution_modes(
        queue_enabled=queue_enabled,
        used_inline_fallback=used_inline_fallback,
    )
    response_payload = {
        **result,
        "status": "queued" if queue_enabled else "started",
        "run_id": run_id,
        "execution_mode": execution_mode,
        "execution_mode_canonical": execution_mode_canonical,
        "execution_mode_legacy": execution_mode_legacy,
        "execution_owner": execution_owner_label(),
        "execution_backend_canonical": (
            str(result.get("execution_backend_canonical") or execution_metadata()["execution_backend_canonical"])
        ),
        "jobs_url": f"/api/v1/admin/socials/shared/ingest/runs?run_id={run_id}",
        "message": ("Shared account ingest queued." if queue_enabled else "Shared account ingest started inline."),
    }
    if used_inline_fallback and worker_health is not None:
        response_payload["worker_health"] = worker_health
    return response_payload


@router.get("/shared/ingest/runs")
def get_shared_ingest_runs(
    limit: int = Query(default=_INGEST_JOBS_DEFAULT_LIMIT, ge=1, le=_INGEST_JOBS_MAX_LIMIT),
    status: str | None = Query(default=None),
    source_scope: Literal["bravo", "network", "creator", "community", "news"] | None = Query(default=None),
    run_id: str | None = Query(default=None),
    _: InternalAdminUser = None,
) -> list[dict[str, Any]]:
    from trr_backend.repositories.social_season_analytics import list_shared_runs

    canonical_source_scope = normalize_source_scope_param(source_scope) if source_scope is not None else None
    return list_shared_runs(limit=limit, status=status, source_scope=canonical_source_scope, run_id=run_id)


@router.post("/shared/ingest/runs/{run_id}/cancel")
def cancel_shared_ingest_run(
    run_id: str,
    user: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import cancel_shared_run

    try:
        return cancel_shared_run(run_id, cancelled_by=(user or {}).get("email"))
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc


@router.get("/shared/review-queue")
def get_shared_review_queue(
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Query(default="network"),
    review_status: Literal["open", "resolved", "ignored"] = Query(default="open"),
    limit: int = Query(default=100, ge=1, le=500),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import list_shared_review_queue

    try:
        canonical_source_scope = normalize_source_scope_param(source_scope)
        return list_shared_review_queue(source_scope=canonical_source_scope, review_status=review_status, limit=limit)
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to read shared review queue: source_scope=%s review_status=%s limit=%s",
            source_scope,
            review_status,
            limit,
        )
        raise _to_social_read_http_exception(exc) from exc


@router.post("/shared/review-queue/{item_id}/resolve")
def resolve_shared_review_queue(
    item_id: str,
    payload: SharedReviewResolveRequest,
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import resolve_shared_review_queue_item

    try:
        return resolve_shared_review_queue_item(
            item_id,
            resolution_action=payload.resolution_action,
            resolved_show_id=str(payload.resolved_show_id) if payload.resolved_show_id else None,
            resolved_season_id=str(payload.resolved_season_id) if payload.resolved_season_id else None,
        )
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc


@router.get("/seasons/{season_id}/shared-status")
def get_season_shared_status_route(
    season_id: UUID,
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Query(default="network"),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import get_season_shared_status

    started_at = perf_counter()
    try:
        canonical_source_scope = normalize_source_scope_param(source_scope)
        payload = get_season_shared_status(str(season_id), source_scope=canonical_source_scope)
        log_read_path(
            "season-social-shared-status",
            latency_ms=(perf_counter() - started_at) * 1000,
            payload=payload,
            extra={
                "source_scope": canonical_source_scope,
                "season_id": season_id,
            },
        )
        return payload
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to read season shared status: season=%s source_scope=%s", season_id, source_scope)
        raise _to_social_read_http_exception(exc) from exc


@router.get("/ingest/worker-health")
def get_social_ingest_worker_health(_: InternalAdminUser = None) -> dict:
    from trr_backend.repositories.social_season_analytics import get_worker_health, is_queue_enabled

    started_at = perf_counter()
    try:
        health = get_worker_health()
        payload = {"queue_enabled": is_queue_enabled(), **health}
        log_read_path(
            "season-social-worker-health",
            latency_ms=(perf_counter() - started_at) * 1000,
            payload=payload,
        )
        return payload
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch social ingest worker health")
        raise _to_social_read_http_exception(exc) from exc


@router.get("/ingest/backfill-health")
def get_social_ingest_backfill_health(
    run_limit: int = Query(
        default=40,
        ge=1,
        le=200,
        description="Max number of recent catalog-backfill runs to enumerate.",
    ),
    recent_log_limit: int = Query(
        default=20,
        ge=1,
        le=100,
        description="Per-run recent-log rows scanned for 401/403/checkpoint classification.",
    ),
    include_terminal_runs: bool = Query(
        default=True,
        description="Include terminal (completed/failed) runs alongside active ones.",
    ),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.backfill_health import get_backfill_health

    started_at = perf_counter()
    try:
        payload = get_backfill_health(
            run_limit=run_limit,
            recent_log_limit=recent_log_limit,
            include_terminal_runs=include_terminal_runs,
        )
        log_read_path(
            "season-social-backfill-health",
            latency_ms=(perf_counter() - started_at) * 1000,
            payload=payload,
        )
        return payload
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch social ingest backfill health")
        raise _to_social_read_http_exception(exc) from exc


@router.get("/ingest/workers/{worker_id}/detail")
def get_social_ingest_worker_detail(worker_id: str, _: InternalAdminUser = None) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import get_worker_detail

    try:
        return get_worker_detail(worker_id)
    except ValueError as exc:
        if str(exc) == "worker_not_found":
            raise HTTPException(status_code=404, detail="Worker not found") from exc
        if str(exc) == "worker_heartbeat_schema_missing":
            raise HTTPException(status_code=503, detail="Worker heartbeat schema is not available") from exc
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch social ingest worker detail: worker_id=%s", worker_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/ingest/workers/purge-inactive")
def purge_social_ingest_inactive_workers(
    payload: PurgeInactiveWorkersRequest | None = None,
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import purge_inactive_workers

    try:
        return purge_inactive_workers(stale_after_seconds=(payload.stale_after_seconds if payload else None))
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to purge inactive social ingest workers")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/ingest/queue-status")
def get_social_ingest_queue_status(
    fresh: bool = Query(default=False, description="Bypass queue-status TTL cache when true"),
    detail: Literal["summary", "full"] | None = Query(
        default=None,
        description="Use full diagnostics only for explicit operator refreshes",
    ),
    include_recent_failures: bool = Query(default=False, description="Include recent failed queue jobs"),
    include_stuck_jobs: bool = Query(default=False, description="Include stuck job detail and stale claim buckets"),
    include_runs_summary: bool = Query(default=False, description="Include scrape run status aggregates"),
    summary_only: bool = Query(default=True, description="Use the bounded status summary path"),
    statement_timeout_ms: int = Query(default=2000, ge=1000, le=30000),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import get_queue_status

    try:
        full_diagnostics = detail == "full" or (fresh and detail != "summary")
        effective_summary_only = False if full_diagnostics else summary_only
        effective_include_recent_failures = include_recent_failures or full_diagnostics
        effective_include_stuck_jobs = include_stuck_jobs or full_diagnostics
        effective_include_runs_summary = include_runs_summary or full_diagnostics
        return get_queue_status(
            fresh=fresh,
            include_recent_failures=effective_include_recent_failures,
            include_stuck_jobs=effective_include_stuck_jobs,
            include_runs_summary=effective_include_runs_summary,
            summary_only=effective_summary_only,
            statement_timeout_ms=statement_timeout_ms,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch social ingest queue status")
        raise _to_social_read_http_exception(exc) from exc


@router.post("/ingest/stuck-jobs/cancel")
def cancel_social_ingest_stuck_jobs(
    payload: CancelStuckJobsRequest | None = None,
    user: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import cancel_stuck_jobs

    try:
        job_ids = [str(job_id) for job_id in (payload.job_ids if payload and payload.job_ids else [])]
        return cancel_stuck_jobs(
            job_ids=job_ids or None,
            cancelled_by=(user or {}).get("email"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to cancel stuck social ingest jobs")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/ingest/dispatch-blocked-jobs/cancel")
def cancel_social_ingest_dispatch_blocked_jobs(
    payload: CancelStuckJobsRequest | None = None,
    user: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import cancel_dispatch_blocked_jobs

    try:
        job_ids = [str(job_id) for job_id in (payload.job_ids if payload and payload.job_ids else [])]
        return cancel_dispatch_blocked_jobs(
            job_ids=job_ids or None,
            cancelled_by=(user or {}).get("email"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to cancel dispatch-blocked social ingest jobs")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/ingest/active-jobs/cancel")
def cancel_social_ingest_active_jobs(
    user: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import cancel_active_jobs

    try:
        return cancel_active_jobs(cancelled_by=(user or {}).get("email"))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to cancel active social ingest jobs")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/ingest/recent-failures/dismiss")
def dismiss_social_ingest_recent_failures(
    payload: DismissRecentFailuresRequest | None = None,
    user: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import dismiss_recent_failures

    try:
        job_ids = [str(job_id) for job_id in (payload.job_ids if payload else [])]
        return dismiss_recent_failures(
            job_ids=job_ids,
            dismiss_all_visible=bool(payload.dismiss_all_visible) if payload else False,
            dismissed_by=(user or {}).get("email"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to dismiss recent social ingest failures")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/ingest/reset-health")
def reset_social_ingest_health_route(
    payload: ResetSocialIngestHealthRequest | None = None,
    user: InternalAdminUser = None,
) -> dict[str, Any]:
    del payload
    from trr_backend.repositories.social_season_analytics import reset_social_ingest_health

    try:
        return reset_social_ingest_health(reset_by=(user or {}).get("email"))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to reset social ingest health")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/ingest/jobs/{job_id}/debug")
def debug_social_ingest_job(
    job_id: UUID,
    payload: JobDebugRequest | None = None,
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import debug_ingest_job_with_openai

    try:
        request_payload = payload or JobDebugRequest()
        return debug_ingest_job_with_openai(
            str(job_id),
            apply_patch=request_payload.apply_patch,
            confirm_apply=request_payload.confirm_apply,
            include_context=request_payload.include_context,
        )
    except ValueError as exc:
        error_code = str(exc)
        if error_code == "job_not_found":
            raise HTTPException(status_code=404, detail="Job not found") from exc
        if error_code == "openai_api_key_missing":
            raise HTTPException(status_code=503, detail="OPENAI_API_KEY is not configured") from exc
        raise HTTPException(status_code=400, detail=error_code) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to debug social ingest job: job_id=%s", job_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/ingest/health-dot")
def get_social_ingest_health_dot(_: InternalAdminUser = None) -> dict[str, Any]:
    import trr_backend.socials.social_season_analytics_impl as social_core

    try:
        status_payload = social_core.get_queue_status(
            include_recent_failures=False,
            include_stuck_jobs=False,
            include_runs_summary=False,
            summary_only=True,
            statement_timeout_ms=2000,
        )
        payload = social_live_status.build_social_ingest_health_dot(status_payload)
        payload["alerts"] = list(status_payload.get("alerts") or []) if isinstance(status_payload, dict) else []
        return payload
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch social ingest health dot payload")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/live-status")
def get_social_live_status(_: InternalAdminUser = None) -> dict[str, Any]:
    try:
        return social_live_status.build_live_status_payload()
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch social live status")
        raise _to_social_read_http_exception(exc) from exc


@router.get("/live-status/stream")
async def stream_social_live_status(request: Request, _: InternalAdminUser = None) -> StreamingResponse:
    async def event_stream() -> Any:
        while True:
            if await request.is_disconnected():
                return
            try:
                payload = await asyncio.wait_for(
                    _run_admin_repo_call(social_live_status.build_live_status_payload),
                    timeout=social_live_status.LIVE_STATUS_STREAM_FETCH_TIMEOUT_SECONDS,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Social live-status stream tick degraded", exc_info=True)
                payload = social_live_status.build_degraded_live_status_payload("live_status_stream_timeout", exc)
            yield "event: live_status\n"
            yield f"data: {json.dumps(payload, default=str)}\n\n"
            await asyncio.sleep(social_live_status.LIVE_STATUS_STREAM_INTERVAL_SECONDS)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )


@router.post("/reddit/runs")
async def start_reddit_refresh_run(
    payload: RedditRefreshRunRequest,
    background_tasks: BackgroundTasks,
    _: InternalAdminUser = None,
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
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/reddit/runs/backfill")
async def backfill_reddit_refresh_runs(
    payload: RedditRefreshBackfillRequest,
    request: Request,
    _: InternalAdminUser = None,
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
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/reddit/runs")
async def list_reddit_refresh_runs(
    community_id: UUID | None = Query(default=None),
    season_id: UUID | None = Query(default=None),
    period_key: str | None = Query(default=None, min_length=1, max_length=200),
    status: str | None = Query(default=None),
    limit: int = Query(default=25, ge=1, le=100),
    _: InternalAdminUser = None,
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
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/reddit/runs/{run_id}")
async def get_reddit_refresh_run(run_id: UUID, _: InternalAdminUser = None) -> dict[str, Any]:
    from trr_backend.repositories.reddit_refresh import get_refresh_run

    try:
        return await _run_admin_repo_call(get_refresh_run, str(run_id))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch reddit refresh run: run_id=%s", run_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/reddit/cache")
async def get_reddit_cached_period_payload(
    community_id: UUID = Query(...),
    season_id: UUID = Query(...),
    period_key: str = Query(..., min_length=1, max_length=160),
    _: InternalAdminUser = None,
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
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/reddit/cache/bulk")
async def get_reddit_cached_period_payload_bulk(
    payload: RedditCacheBulkRequest,
    _: InternalAdminUser = None,
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
                discovery = snapshot.get("discovery") if isinstance(snapshot.get("discovery"), dict) else None
                partial_failures.extend(
                    snapshot.get("partial_failures") if isinstance(snapshot.get("partial_failures"), list) else []
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
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/reddit/analytics/community/{community_id}/summary")
async def get_reddit_community_analytics_summary(
    community_id: UUID,
    scope: Literal["season", "all"] = Query(default="season"),
    season_id: UUID | None = Query(default=None),
    _: InternalAdminUser = None,
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
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/reddit/analytics/community/{community_id}/shows")
async def get_reddit_community_analytics_shows(
    community_id: UUID,
    scope: Literal["season", "all"] = Query(default="season"),
    season_id: UUID | None = Query(default=None),
    _: InternalAdminUser = None,
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
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/reddit/analytics/community/{community_id}/flairs")
async def get_reddit_community_analytics_flairs(
    community_id: UUID,
    scope: Literal["season", "all"] = Query(default="season"),
    season_id: UUID | None = Query(default=None),
    _: InternalAdminUser = None,
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
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/reddit/analytics/community/{community_id}/flairs/{flair_key}")
async def get_reddit_community_analytics_flair_detail(
    community_id: UUID,
    flair_key: str,
    scope: Literal["season", "all"] = Query(default="season"),
    season_id: UUID | None = Query(default=None),
    container_key: str | None = Query(default=None, max_length=160),
    page: int = Query(default=1, ge=1, le=10_000),
    per_page: int = Query(default=50, ge=1, le=200),
    _: InternalAdminUser = None,
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
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/reddit/analytics/community/{community_id}/posts")
async def get_reddit_community_analytics_posts(
    community_id: UUID,
    scope: Literal["season", "all"] = Query(default="season"),
    season_id: UUID | None = Query(default=None),
    container_key: str | None = Query(default=None, max_length=160),
    flair_key: str | None = Query(default=None, max_length=200),
    page: int = Query(default=1, ge=1, le=10_000),
    per_page: int = Query(default=50, ge=1, le=200),
    _: InternalAdminUser = None,
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
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Reddit flair auto-categorization
# ---------------------------------------------------------------------------


class AutoCategorizeFlairsRequest(BaseModel):
    show_id: UUID


class AutoCategorizeFlairsResponse(BaseModel):
    categories: dict[str, str]
    matched: int
    total: int


@router.post("/reddit/communities/{community_id}/auto-categorize-flairs")
async def auto_categorize_community_flairs(
    community_id: UUID,
    body: AutoCategorizeFlairsRequest,
    _: InternalAdminUser = None,
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
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/reddit/auto-categorize-flairs-batch")
async def auto_categorize_flairs_batch(
    body: AutoCategorizeFlairsRequest,
    _: InternalAdminUser = None,
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
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/seasons/{season_id}/ingest/jobs")
async def get_season_ingest_jobs(
    season_id: UUID,
    limit: int = Query(default=_INGEST_JOBS_DEFAULT_LIMIT, ge=1, le=_INGEST_JOBS_MAX_LIMIT),
    offset: int = Query(default=_INGEST_JOBS_DEFAULT_OFFSET, ge=0, le=_INGEST_JOBS_MAX_OFFSET),
    run_id: UUID | None = Query(default=None),
    status: str | None = Query(default=None),
    platform: Literal["instagram", "tiktok", "twitter", "youtube", "facebook", "threads"] | None = Query(default=None),
    _: InternalAdminUser = None,
) -> dict:
    from trr_backend.repositories.social_season_analytics import list_jobs

    try:
        jobs = await _run_admin_repo_call(
            list_jobs,
            str(season_id),
            limit=limit,
            offset=offset,
            run_id=(str(run_id) if run_id else None),
            status=status,
            platform=platform,
        )
        returned = len(jobs)
        return {
            "season_id": str(season_id),
            "run_id": str(run_id) if run_id else None,
            "filters": {"status": status, "platform": platform},
            "jobs": jobs,
            "pagination": {
                "limit": int(limit),
                "offset": int(offset),
                "returned": returned,
                "has_more": returned >= int(limit),
            },
        }
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to list social jobs: season=%s", season_id)
        raise _to_social_read_http_exception(exc) from exc


@router.get("/seasons/{season_id}/ingest/runs")
async def get_season_ingest_runs(
    season_id: UUID,
    limit: int = Query(default=50, ge=1, le=250),
    status: Literal["queued", "pending", "running", "retrying", "completed", "failed", "cancelled"] | None = Query(
        default=None
    ),
    source_scope: Literal["bravo", "network", "creator", "community", "news"] | None = Query(default=None),
    run_id: UUID | None = Query(default=None),
    client_session_id: str | None = Query(default=None, max_length=200),
    client_workflow_id: str | None = Query(default=None, max_length=200),
    platforms: list[str] | None = Query(default=None),
    week_index: int | None = Query(default=None, ge=0),
    date_start: datetime | None = Query(default=None),
    date_end: datetime | None = Query(default=None),
    _: InternalAdminUser = None,
) -> dict:
    from trr_backend.repositories.social_season_analytics import list_runs

    started_at = perf_counter()
    try:
        runs = await _run_admin_repo_call(
            list_runs,
            str(season_id),
            limit=limit,
            status=status,
            source_scope=source_scope,
            run_id=str(run_id) if run_id else None,
            client_session_id=(str(client_session_id or "").strip() or None),
            client_workflow_id=(str(client_workflow_id or "").strip() or None),
            platforms=platforms,
            week_index=week_index,
            date_start=date_start,
            date_end=date_end,
        )
        duration_ms = int((perf_counter() - started_at) * 1000)
        logger.info(
            "Social ingest runs request completed: season=%s source_scope=%s status=%s limit=%s duration_ms=%s",
            season_id,
            source_scope,
            status,
            limit,
            duration_ms,
        )
        return {
            "season_id": str(season_id),
            "filters": {
                "status": status,
                "source_scope": source_scope,
                "run_id": str(run_id) if run_id else None,
                "client_session_id": str(client_session_id or "").strip() or None,
                "client_workflow_id": str(client_workflow_id or "").strip() or None,
                "platforms": platforms,
                "week_index": week_index,
                "date_start": date_start.isoformat() if date_start else None,
                "date_end": date_end.isoformat() if date_end else None,
            },
            "runs": runs,
        }
    except Exception as exc:  # noqa: BLE001
        duration_ms = int((perf_counter() - started_at) * 1000)
        logger.exception(
            "Failed to list social runs: season=%s source_scope=%s status=%s limit=%s duration_ms=%s",
            season_id,
            source_scope,
            status,
            limit,
            duration_ms,
        )
        raise _to_social_read_http_exception(exc) from exc


@router.get("/seasons/{season_id}/ingest/runs/summary")
async def get_season_ingest_runs_summary(
    season_id: UUID,
    limit: int = Query(default=20, ge=1, le=100),
    source_scope: Literal["bravo", "network", "creator", "community", "news"] | None = Query(default=None),
    client_session_id: str | None = Query(default=None, max_length=200),
    client_workflow_id: str | None = Query(default=None, max_length=200),
    platforms: list[str] | None = Query(default=None),
    week_index: int | None = Query(default=None, ge=0),
    date_start: datetime | None = Query(default=None),
    date_end: datetime | None = Query(default=None),
    _: InternalAdminUser = None,
) -> dict:
    from trr_backend.repositories.social_season_analytics import list_run_summaries

    started_at = perf_counter()
    try:
        summaries = await _run_admin_repo_call(
            list_run_summaries,
            str(season_id),
            limit=limit,
            source_scope=source_scope,
            client_session_id=(str(client_session_id or "").strip() or None),
            client_workflow_id=(str(client_workflow_id or "").strip() or None),
            platforms=platforms,
            week_index=week_index,
            date_start=date_start,
            date_end=date_end,
        )
        duration_ms = int((perf_counter() - started_at) * 1000)
        logger.info(
            "Social ingest run summary request completed: season=%s source_scope=%s limit=%s duration_ms=%s",
            season_id,
            source_scope,
            limit,
            duration_ms,
        )
        return {
            "season_id": str(season_id),
            "filters": {
                "source_scope": source_scope,
                "limit": limit,
                "client_session_id": str(client_session_id or "").strip() or None,
                "client_workflow_id": str(client_workflow_id or "").strip() or None,
                "platforms": platforms,
                "week_index": week_index,
                "date_start": date_start.isoformat() if date_start else None,
                "date_end": date_end.isoformat() if date_end else None,
            },
            "summaries": summaries,
        }
    except Exception as exc:  # noqa: BLE001
        duration_ms = int((perf_counter() - started_at) * 1000)
        logger.exception(
            "Failed to list social run summaries: season=%s source_scope=%s limit=%s duration_ms=%s",
            season_id,
            source_scope,
            limit,
            duration_ms,
        )
        raise _to_social_read_http_exception(exc) from exc


@router.get("/seasons/{season_id}/ingest/runs/{run_id}/progress")
async def get_season_ingest_run_progress(
    season_id: UUID,
    run_id: UUID,
    recent_log_limit: int = Query(default=20, ge=1, le=100),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import get_run_progress_snapshot

    started_at = perf_counter()
    try:
        payload = await _run_admin_repo_call(
            get_run_progress_snapshot,
            str(season_id),
            str(run_id),
            recent_log_limit=recent_log_limit,
        )
        duration_ms = int((perf_counter() - started_at) * 1000)
        logger.info(
            "Social ingest run progress request completed: season=%s run_id=%s recent_log_limit=%s duration_ms=%s",
            season_id,
            run_id,
            recent_log_limit,
            duration_ms,
        )
        return payload
    except ValueError as exc:
        message = str(exc)
        if message == "run_not_found":
            raise HTTPException(status_code=404, detail=message) from exc
        if message in {"social_ingest_queue_schema_missing", "run_progress_requires_scrape_jobs_run_id"}:
            raise HTTPException(status_code=503, detail=message) from exc
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        duration_ms = int((perf_counter() - started_at) * 1000)
        logger.exception(
            "Failed to fetch social ingest run progress: season=%s run_id=%s duration_ms=%s",
            season_id,
            run_id,
            duration_ms,
        )
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/seasons/{season_id}/ingest/runs/{run_id}/cancel")
async def cancel_season_ingest_run(
    season_id: UUID,
    run_id: UUID,
    user: InternalAdminUser = None,
) -> dict:
    from trr_backend.repositories.social_season_analytics import cancel_run

    try:
        payload = await _run_admin_repo_call(
            cancel_run,
            str(season_id),
            str(run_id),
            cancelled_by=(user or {}).get("email"),
        )
        return payload
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to cancel social ingest run: season=%s run_id=%s", season_id, run_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/seasons/{season_id}/analytics/week/{week_index}/live-health")
async def get_season_analytics_week_live_health(
    season_id: UUID,
    week_index: int,
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Query(default="network"),
    timezone: str = Query(default="America/New_York"),
    platforms: str | None = Query(default=None, description="Comma-separated platform list"),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import get_week_live_health_snapshot

    parsed_platforms = _parse_platform_query(platforms)
    cache_key = _week_live_health_cache_key(
        season_id=str(season_id),
        week_index=week_index,
        source_scope=source_scope,
        platforms=parsed_platforms,
        timezone=timezone,
    )
    cached_payload = _get_ttl_cached_payload(
        _WEEK_LIVE_HEALTH_CACHE,
        _WEEK_LIVE_HEALTH_CACHE_LOCK,
        cache_key,
    )
    if cached_payload is not None:
        return cached_payload
    started_at = perf_counter()
    try:
        payload = await _run_admin_repo_call(
            get_week_live_health_snapshot,
            str(season_id),
            week_index=week_index,
            platforms=parsed_platforms,
            timezone=timezone,
            source_scope=source_scope,
        )
        _set_ttl_cached_payload(
            _WEEK_LIVE_HEALTH_CACHE,
            _WEEK_LIVE_HEALTH_CACHE_LOCK,
            cache_key,
            payload,
            ttl_seconds=_WEEK_LIVE_HEALTH_CACHE_TTL_SECONDS,
            max_entries=_WEEK_LIVE_HEALTH_CACHE_MAX_ENTRIES,
        )
        duration_ms = int((perf_counter() - started_at) * 1000)
        logger.info(
            "Social week live-health request completed: season=%s week=%s source_scope=%s platforms=%s duration_ms=%s",
            season_id,
            week_index,
            source_scope,
            ",".join(parsed_platforms) if parsed_platforms else "all",
            duration_ms,
        )
        return payload
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        duration_ms = int((perf_counter() - started_at) * 1000)
        logger.exception(
            "Failed to compute social week live-health: season=%s week=%s source_scope=%s platforms=%s duration_ms=%s",
            season_id,
            week_index,
            source_scope,
            ",".join(parsed_platforms) if parsed_platforms else "all",
            duration_ms,
        )
        raise _to_social_read_http_exception(exc) from exc


@router.get("/seasons/{season_id}/analytics")
async def get_season_analytics(
    season_id: UUID,
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Query(default="network"),
    timezone: str = Query(default="America/New_York"),
    week: int | None = Query(default=None, ge=0, le=200),
    platforms: str | None = Query(default=None, description="Comma-separated platform list"),
    include: str | None = Query(
        default=None,
        description="Comma-separated include list: rows,flags,schedule,benchmark",
    ),
    _: InternalAdminUser = None,
) -> dict:
    from trr_backend.repositories.social_season_analytics import get_analytics

    parsed_platforms = _parse_platform_query(platforms)
    include_options = parse_analytics_include(include)

    started_at = perf_counter()
    try:
        cache_key = _analytics_cache_key(
            season_id=str(season_id),
            source_scope=source_scope,
            platforms=parsed_platforms,
            timezone=timezone,
            week=week,
            include_rows=include_options.include_rows,
            include_flags=include_options.include_flags,
            include_schedule=include_options.include_schedule,
            include_benchmark=include_options.include_benchmark,
        )
        cached_payload = _get_ttl_cached_payload(
            _ANALYTICS_CACHE,
            _ANALYTICS_CACHE_LOCK,
            cache_key,
        )
        if cached_payload is not None:
            log_read_path(
                "season-social-analytics",
                latency_ms=(perf_counter() - started_at) * 1000,
                payload=cached_payload,
                extra=analytics_read_path_extra(
                    cache="hit",
                    source_scope=source_scope,
                    week=week,
                    platforms=parsed_platforms,
                ),
            )
            return cached_payload
        payload = await run_in_threadpool(
            get_analytics,
            str(season_id),
            platforms=parsed_platforms,
            timezone=timezone,
            week=week,
            source_scope=source_scope,
            include_rows=include_options.include_rows,
            include_jobs=False,
            include_flags=include_options.include_flags,
            include_schedule=include_options.include_schedule,
            include_benchmark=include_options.include_benchmark,
        )
        _set_ttl_cached_payload(
            _ANALYTICS_CACHE,
            _ANALYTICS_CACHE_LOCK,
            cache_key,
            payload,
            ttl_seconds=_ANALYTICS_CACHE_TTL_SECONDS,
            max_entries=_ANALYTICS_CACHE_MAX_ENTRIES,
        )
        duration_ms = int((perf_counter() - started_at) * 1000)
        logger.info(
            "Social analytics request completed: season=%s source_scope=%s week=%s platforms=%s duration_ms=%s",
            season_id,
            source_scope,
            week,
            ",".join(parsed_platforms) if parsed_platforms else "all",
            duration_ms,
        )
        log_read_path(
            "season-social-analytics",
            latency_ms=(perf_counter() - started_at) * 1000,
            payload=payload,
            extra=analytics_read_path_extra(
                cache="miss",
                source_scope=source_scope,
                week=week,
                platforms=parsed_platforms,
            ),
        )
        return payload
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        duration_ms = int((perf_counter() - started_at) * 1000)
        logger.exception(
            "Failed to compute social analytics: season=%s source_scope=%s week=%s platforms=%s duration_ms=%s",
            season_id,
            source_scope,
            week,
            ",".join(parsed_platforms) if parsed_platforms else "all",
            duration_ms,
        )
        raise _to_social_read_http_exception(exc) from exc


@router.get("/seasons/{season_id}/analytics/week/{week_index}/summary")
async def get_season_analytics_week_summary(
    season_id: UUID,
    week_index: int,
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Query(default="network"),
    timezone: str = Query(default="America/New_York"),
    platforms: str | None = Query(default=None, description="Comma-separated platform list"),
    include: WeekSummaryInclude = Query(default="totals_only"),
    max_comments_per_post: int = Query(default=_WEEK_DETAIL_DEFAULT_MAX_COMMENTS_PER_POST, ge=0, le=500),
    sort_field: WeekDetailSortField = Query(default="posted_at"),
    sort_dir: WeekDetailSortDir = Query(default="desc"),
    _: InternalAdminUser = None,
) -> dict:
    from trr_backend.repositories.social_season_analytics import get_week_detail_summary, get_week_detail_summary_fast

    parsed_platforms = _parse_platform_query(platforms)
    cache_key = _week_summary_cache_key(
        season_id=str(season_id),
        week_index=week_index,
        source_scope=source_scope,
        platforms=parsed_platforms,
        timezone=timezone,
        include=include,
        max_comments_per_post=max_comments_per_post,
        sort_field=sort_field,
        sort_dir=sort_dir,
    )
    cached_payload = _get_week_summary_cached_payload(cache_key)
    if cached_payload is not None:
        return cached_payload
    started_at = perf_counter()
    trace_id = get_trace_id()

    try:
        if include == "full":
            payload = await _run_admin_repo_call(
                get_week_detail_summary,
                str(season_id),
                week_index=week_index,
                platforms=parsed_platforms,
                timezone=timezone,
                source_scope=source_scope,
                max_comments_per_post=max_comments_per_post,
                sort_field=sort_field,
                sort_dir=sort_dir,
            )
        else:
            payload = await _run_admin_repo_call(
                get_week_detail_summary_fast,
                str(season_id),
                week_index=week_index,
                platforms=parsed_platforms,
                timezone=timezone,
                source_scope=source_scope,
            )
        _set_week_summary_cached_payload(cache_key, payload)
        duration_ms = int((perf_counter() - started_at) * 1000)
        logger.info(
            (
                "Social week detail summary completed: season=%s week=%s source_scope=%s platforms=%s "
                "include=%s max_comments_per_post=%s duration_ms=%s trace_id=%s"
            ),
            season_id,
            week_index,
            source_scope,
            ",".join(parsed_platforms) if parsed_platforms else "all",
            include,
            max_comments_per_post,
            duration_ms,
            trace_id,
        )
        return payload
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        duration_ms = int((perf_counter() - started_at) * 1000)
        logger.exception(
            (
                "Failed to compute week detail summary: season=%s week=%s source_scope=%s platforms=%s "
                "include=%s max_comments_per_post=%s duration_ms=%s trace_id=%s"
            ),
            season_id,
            week_index,
            source_scope,
            ",".join(parsed_platforms) if parsed_platforms else "all",
            include,
            max_comments_per_post,
            duration_ms,
            trace_id,
        )
        raise _to_social_read_http_exception(exc) from exc


@router.get("/seasons/{season_id}/analytics/week/{week_index}")
async def get_season_analytics_week_detail(
    season_id: UUID,
    week_index: int,
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Query(default="network"),
    timezone: str = Query(default="America/New_York"),
    platforms: str | None = Query(default=None, description="Comma-separated platform list"),
    max_comments_per_post: int = Query(default=_WEEK_DETAIL_DEFAULT_MAX_COMMENTS_PER_POST, ge=0, le=500),
    post_limit: int = Query(default=_WEEK_DETAIL_DEFAULT_POST_LIMIT, ge=1, le=100),
    post_offset: int = Query(default=_WEEK_DETAIL_DEFAULT_POST_OFFSET, ge=0),
    sort_field: WeekDetailSortField = Query(default="posted_at"),
    sort_dir: WeekDetailSortDir = Query(default="desc"),
    include_status: bool = Query(default=True),
    _: InternalAdminUser = None,
) -> dict:
    from trr_backend.repositories.social_season_analytics import get_week_detail

    parsed_platforms = _parse_platform_query(platforms)
    normalized_platforms = _normalize_target_platforms(parsed_platforms)
    normalized_timezone = str(timezone or "").strip() or "America/New_York"
    cache_key = _week_detail_cache_key(
        season_id=str(season_id),
        week_index=week_index,
        source_scope=source_scope,
        platforms=normalized_platforms,
        timezone=normalized_timezone,
        max_comments_per_post=max_comments_per_post,
        sort_field=sort_field,
        sort_dir=sort_dir,
        include_status=include_status,
    )
    cached_payload: dict[str, Any] | None = _get_week_detail_cached_payload(cache_key)
    requested_end = post_limit + post_offset
    started_at = perf_counter()
    trace_id = get_trace_id()

    try:
        base_payload: dict[str, Any] | None = cached_payload
        if base_payload is None:
            base_payload = await _run_admin_repo_call(
                get_week_detail,
                str(season_id),
                week_index=week_index,
                platforms=parsed_platforms,
                timezone=normalized_timezone,
                source_scope=source_scope,
                max_comments_per_post=max_comments_per_post,
                post_limit=requested_end,
                post_offset=0,
                sort_field=sort_field,
                sort_dir=sort_dir,
                include_status=include_status,
            )
            _set_week_detail_cached_payload(cache_key, base_payload)
        else:
            cached_posts, cached_total = week_detail_cached_post_counts(base_payload)

            if requested_end > cached_posts and cached_total > cached_posts:
                base_payload = await _run_admin_repo_call(
                    get_week_detail,
                    str(season_id),
                    week_index=week_index,
                    platforms=parsed_platforms,
                    timezone=normalized_timezone,
                    source_scope=source_scope,
                    max_comments_per_post=max_comments_per_post,
                    post_limit=requested_end,
                    post_offset=0,
                    sort_field=sort_field,
                    sort_dir=sort_dir,
                    include_status=include_status,
                )
            _set_week_detail_cached_payload(cache_key, base_payload)

        paged_payload = page_week_detail_payload(
            base_payload,
            post_limit=post_limit,
            post_offset=post_offset,
            sort_field=sort_field,
            sort_dir=sort_dir,
        )
        duration_ms = int((perf_counter() - started_at) * 1000)
        logger.info(
            (
                "Social week detail completed: season=%s week=%s source_scope=%s platforms=%s "
                "max_comments_per_post=%s post_limit=%s post_offset=%s include_status=%s duration_ms=%s trace_id=%s"
            ),
            season_id,
            week_index,
            source_scope,
            ",".join(parsed_platforms) if parsed_platforms else "all",
            max_comments_per_post,
            post_limit,
            post_offset,
            include_status,
            duration_ms,
            trace_id,
        )
        return paged_payload
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        duration_ms = int((perf_counter() - started_at) * 1000)
        logger.exception(
            (
                "Failed to compute week detail: season=%s week=%s source_scope=%s platforms=%s "
                "max_comments_per_post=%s post_limit=%s post_offset=%s include_status=%s duration_ms=%s trace_id=%s"
            ),
            season_id,
            week_index,
            source_scope,
            ",".join(parsed_platforms) if parsed_platforms else "all",
            max_comments_per_post,
            post_limit,
            post_offset,
            include_status,
            duration_ms,
            trace_id,
        )
        raise _to_social_read_http_exception(exc) from exc


@router.get("/seasons/{season_id}/analytics/comments-coverage")
async def get_season_comments_coverage(
    season_id: UUID,
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Query(default="network"),
    timezone: str = Query(default="America/New_York"),
    platforms: str | None = Query(default=None, description="Comma-separated platform list"),
    date_start: datetime | None = Query(default=None),
    date_end: datetime | None = Query(default=None),
    _: InternalAdminUser = None,
) -> dict:
    from trr_backend.repositories.social_season_analytics import get_comments_coverage

    parsed_platforms = _parse_platform_query(platforms)
    cache_key = _coverage_cache_window_key(
        season_id=str(season_id),
        source_scope=source_scope,
        platforms=parsed_platforms,
        timezone=timezone,
        date_start=date_start,
        date_end=date_end,
    )
    cached_payload = _get_ttl_cached_payload(
        _COMMENTS_COVERAGE_CACHE,
        _COMMENTS_COVERAGE_CACHE_LOCK,
        cache_key,
    )
    if cached_payload is not None:
        return cached_payload

    try:
        payload = await _run_admin_repo_call(
            get_comments_coverage,
            str(season_id),
            platforms=parsed_platforms,
            timezone=timezone,
            source_scope=source_scope,
            date_start=date_start,
            date_end=date_end,
        )
        _set_ttl_cached_payload(
            _COMMENTS_COVERAGE_CACHE,
            _COMMENTS_COVERAGE_CACHE_LOCK,
            cache_key,
            payload,
            ttl_seconds=_COVERAGE_CACHE_TTL_SECONDS,
            max_entries=_COVERAGE_CACHE_MAX_ENTRIES,
        )
        return payload
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to compute comments coverage: season=%s", season_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/seasons/{season_id}/analytics/mirror-coverage")
async def get_season_mirror_coverage(
    season_id: UUID,
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Query(default="network"),
    timezone: str = Query(default="America/New_York"),
    platforms: str | None = Query(default=None, description="Comma-separated platform list"),
    date_start: datetime | None = Query(default=None),
    date_end: datetime | None = Query(default=None),
    _: InternalAdminUser = None,
) -> dict:
    from trr_backend.repositories.social_season_analytics import get_mirror_coverage

    parsed_platforms = _parse_platform_query(platforms)
    cache_key = _coverage_cache_window_key(
        season_id=str(season_id),
        source_scope=source_scope,
        platforms=parsed_platforms,
        timezone=timezone,
        date_start=date_start,
        date_end=date_end,
    )
    cached_payload = _get_ttl_cached_payload(
        _MIRROR_COVERAGE_CACHE,
        _MIRROR_COVERAGE_CACHE_LOCK,
        cache_key,
    )
    if cached_payload is not None:
        return cached_payload

    try:
        payload = await _run_admin_repo_call(
            get_mirror_coverage,
            str(season_id),
            platforms=parsed_platforms,
            timezone=timezone,
            source_scope=source_scope,
            date_start=date_start,
            date_end=date_end,
        )
        _set_ttl_cached_payload(
            _MIRROR_COVERAGE_CACHE,
            _MIRROR_COVERAGE_CACHE_LOCK,
            cache_key,
            payload,
            ttl_seconds=_COVERAGE_CACHE_TTL_SECONDS,
            max_entries=_COVERAGE_CACHE_MAX_ENTRIES,
        )
        return payload
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to compute mirror coverage: season=%s", season_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/seasons/{season_id}/analytics/posts/{platform}/{source_id}")
async def get_post_comments(
    season_id: UUID,
    platform: str,
    source_id: str,
    _: InternalAdminUser = None,
) -> dict:
    from trr_backend.repositories.social_season_analytics import get_post_comments as _get

    try:
        return await _run_admin_repo_call(_get, str(season_id), platform=platform, source_id=source_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to fetch post comments: season=%s platform=%s source_id=%s",
            season_id,
            platform,
            source_id,
        )
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/seasons/{season_id}/tiktok/overview")
async def get_season_tiktok_overview(
    season_id: UUID,
    date_start: datetime | None = Query(default=None),
    date_end: datetime | None = Query(default=None),
    cast_member_id: UUID | None = Query(default=None),
    hashtag: str | None = Query(default=None),
    keyword: str | None = Query(default=None),
    sound_id: str | None = Query(default=None),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import get_tiktok_overview

    try:
        return await _run_admin_repo_call(
            get_tiktok_overview,
            str(season_id),
            date_start=date_start,
            date_end=date_end,
            cast_member_id=str(cast_member_id) if cast_member_id else None,
            hashtag=hashtag,
            keyword=keyword,
            sound_id=sound_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch TikTok overview: season=%s", season_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/seasons/{season_id}/tiktok/cast-members")
async def get_season_tiktok_cast_members(
    season_id: UUID,
    date_start: datetime | None = Query(default=None),
    date_end: datetime | None = Query(default=None),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import get_tiktok_cast_members

    try:
        return await _run_admin_repo_call(
            get_tiktok_cast_members,
            str(season_id),
            date_start=date_start,
            date_end=date_end,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch TikTok cast members: season=%s", season_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/seasons/{season_id}/tiktok/hashtags")
async def get_season_tiktok_hashtags(
    season_id: UUID,
    token_type: str = Query(default="hashtag"),
    date_start: datetime | None = Query(default=None),
    date_end: datetime | None = Query(default=None),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import get_tiktok_hashtags

    try:
        return await _run_admin_repo_call(
            get_tiktok_hashtags,
            str(season_id),
            token_type=token_type,
            date_start=date_start,
            date_end=date_end,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch TikTok hashtag/keyword trends: season=%s", season_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/seasons/{season_id}/tiktok/sounds")
async def get_season_tiktok_sounds(
    season_id: UUID,
    date_start: datetime | None = Query(default=None),
    date_end: datetime | None = Query(default=None),
    search: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=250),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import get_tiktok_sounds

    try:
        return await _run_admin_repo_call(
            get_tiktok_sounds,
            str(season_id),
            date_start=date_start,
            date_end=date_end,
            search=search,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch TikTok sounds: season=%s", season_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/seasons/{season_id}/tiktok/content-health")
async def get_season_tiktok_content_health(
    season_id: UUID,
    date_start: datetime | None = Query(default=None),
    date_end: datetime | None = Query(default=None),
    cast_member_id: UUID | None = Query(default=None),
    hashtag: str | None = Query(default=None),
    keyword: str | None = Query(default=None),
    sound_id: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=250),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import get_tiktok_content_health

    try:
        return await _run_admin_repo_call(
            get_tiktok_content_health,
            str(season_id),
            date_start=date_start,
            date_end=date_end,
            cast_member_id=str(cast_member_id) if cast_member_id else None,
            hashtag=hashtag,
            keyword=keyword,
            sound_id=sound_id,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch TikTok content health: season=%s", season_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/seasons/{season_id}/tiktok/sounds/{sound_id}")
async def get_season_tiktok_sound_detail(
    season_id: UUID,
    sound_id: str,
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import get_tiktok_sound_detail

    try:
        return await _run_admin_repo_call(get_tiktok_sound_detail, str(season_id), sound_id=sound_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch TikTok sound detail: season=%s sound_id=%s", season_id, sound_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/seasons/{season_id}/tiktok/sounds/{sound_id}/posts")
async def get_season_tiktok_sound_posts(
    season_id: UUID,
    sound_id: str,
    limit: int = Query(default=100, ge=1, le=500),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import get_tiktok_sound_posts

    try:
        return await _run_admin_repo_call(get_tiktok_sound_posts, str(season_id), sound_id=sound_id, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch TikTok sound posts: season=%s sound_id=%s", season_id, sound_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/seasons/{season_id}/tiktok/posts/{post_id}/detail")
async def get_season_tiktok_post_detail(
    season_id: UUID,
    post_id: str,
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import get_tiktok_post_detail

    try:
        return await _run_admin_repo_call(get_tiktok_post_detail, str(season_id), post_id=post_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch TikTok post detail: season=%s post_id=%s", season_id, post_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/seasons/{season_id}/tiktok/sentiment-trends")
async def get_season_tiktok_sentiment_trends(
    season_id: UUID,
    date_start: datetime | None = Query(default=None),
    date_end: datetime | None = Query(default=None),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    from trr_backend.repositories.social_season_analytics import get_tiktok_sentiment_trends

    try:
        return await _run_admin_repo_call(
            get_tiktok_sentiment_trends,
            str(season_id),
            date_start=date_start,
            date_end=date_end,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch TikTok sentiment trends: season=%s", season_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/seasons/{season_id}/analytics/posts/{platform}/{source_id}/refresh")
async def refresh_post_comments_for_post(
    season_id: UUID,
    platform: str,
    source_id: str,
    payload: PostCommentRefreshRequest | None = None,
    _: InternalAdminUser = None,
) -> dict:
    from trr_backend.repositories.social_season_analytics import (
        get_post_comments as _get_post_comments,
    )
    from trr_backend.repositories.social_season_analytics import (
        refresh_post as _refresh_post,
    )

    request_payload = payload or PostCommentRefreshRequest()

    try:
        refresh_summary = await _run_admin_repo_call(
            _refresh_post,
            str(season_id),
            platform=platform,
            source_id=source_id,
            max_comments_per_post=request_payload.max_comments_per_post,
            fetch_replies=request_payload.fetch_replies,
        )
        invalidate_week_detail_cache()
        refreshed = await _run_admin_repo_call(
            _get_post_comments,
            str(season_id),
            platform=platform,
            source_id=source_id,
        )
        refreshed["refresh"] = refresh_summary
        return refreshed
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to refresh post comments: season=%s platform=%s source_id=%s",
            season_id,
            platform,
            source_id,
        )
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/seasons/{season_id}/instagram/mirror/requeue")
async def requeue_instagram_mirror_jobs(
    season_id: UUID,
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Query(default="network"),
    limit: int = Query(default=1000, ge=1, le=5000),
    failed_only: bool = Query(default=False),
    date_start: datetime | None = Query(default=None),
    date_end: datetime | None = Query(default=None),
    _: InternalAdminUser = None,
) -> dict:
    from trr_backend.repositories.social_season_analytics import requeue_instagram_media_mirror_jobs

    try:
        return await _run_admin_repo_call(
            requeue_instagram_media_mirror_jobs,
            str(season_id),
            source_scope=source_scope,
            limit=limit,
            failed_only=failed_only,
            date_start=date_start,
            date_end=date_end,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to requeue instagram media mirror jobs: season=%s source_scope=%s",
            season_id,
            source_scope,
        )
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/seasons/{season_id}/{platform}/mirror/requeue")
async def requeue_platform_mirror_jobs(
    season_id: UUID,
    platform: str,
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Query(default="network"),
    limit: int = Query(default=1000, ge=1, le=5000),
    failed_only: bool = Query(default=False),
    date_start: datetime | None = Query(default=None),
    date_end: datetime | None = Query(default=None),
    _: InternalAdminUser = None,
) -> dict:
    from trr_backend.repositories.social_season_analytics import requeue_media_mirror_jobs

    normalized_platform = (platform or "").strip().lower()
    try:
        return await _run_admin_repo_call(
            requeue_media_mirror_jobs,
            str(season_id),
            platform=normalized_platform,
            source_scope=source_scope,
            limit=limit,
            failed_only=failed_only,
            date_start=date_start,
            date_end=date_end,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to requeue %s media mirror jobs: season=%s source_scope=%s",
            normalized_platform,
            season_id,
            source_scope,
        )
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/seasons/{season_id}/analytics/export.csv")
async def export_season_analytics_csv(
    season_id: UUID,
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Query(default="network"),
    timezone: str = Query(default="America/New_York"),
    week: int | None = Query(default=None, ge=0, le=200),
    platforms: str | None = Query(default=None),
    _: InternalAdminUser = None,
) -> Response:
    from trr_backend.repositories.social_season_analytics import build_csv, get_analytics

    parsed_platforms = None
    if platforms and platforms.strip():
        parsed_platforms = [item.strip().lower() for item in platforms.split(",") if item.strip()]
    try:
        snapshot = await _run_admin_repo_call(
            get_analytics,
            str(season_id),
            platforms=parsed_platforms,
            timezone=timezone,
            week=week,
            source_scope=source_scope,
            include_rows=True,
        )
        csv_text = await _run_admin_repo_call(build_csv, snapshot)
        filename = f"social_report_{season_id}_{datetime.now().strftime('%Y%m%d')}.csv"
        return Response(
            content=csv_text,
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to export social CSV: season=%s", season_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/seasons/{season_id}/analytics/export.pdf")
async def export_season_analytics_pdf(
    season_id: UUID,
    source_scope: Literal["bravo", "network", "creator", "community", "news"] = Query(default="network"),
    timezone: str = Query(default="America/New_York"),
    week: int | None = Query(default=None, ge=0, le=200),
    platforms: str | None = Query(default=None),
    _: InternalAdminUser = None,
) -> Response:
    from trr_backend.repositories.social_season_analytics import (
        build_pdf,
        get_analytics,
        pdf_filename,
    )

    parsed_platforms = None
    if platforms and platforms.strip():
        parsed_platforms = [item.strip().lower() for item in platforms.split(",") if item.strip()]
    try:
        snapshot = await _run_admin_repo_call(
            get_analytics,
            str(season_id),
            platforms=parsed_platforms,
            timezone=timezone,
            week=week,
            source_scope=source_scope,
            include_rows=False,
        )
        pdf_bytes = await _run_admin_repo_call(build_pdf, snapshot)
        summary = snapshot.get("summary") or {}
        filename = pdf_filename(
            str(summary.get("show_id") or "show"),
            int(summary.get("season_number") or 0),
        )
        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to export social PDF: season=%s", season_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
