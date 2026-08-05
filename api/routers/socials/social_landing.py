# ruff: noqa: F401, F403, F405
"""Social landing-summary routes and shared legacy-scrape helpers."""
from __future__ import annotations

from fastapi import APIRouter

from ._shared import *

router = APIRouter()

SOCIAL_LANDING_PROGRESS_ROLLUP_CACHE_TTL_SECONDS = int(
    os.getenv("SOCIAL_LANDING_PROGRESS_ROLLUP_CACHE_TTL_SECONDS", "300")
)

SOCIAL_LANDING_PROGRESS_ROLLUP_CACHE_MAX_ENTRIES = int(
    os.getenv("SOCIAL_LANDING_PROGRESS_ROLLUP_CACHE_MAX_ENTRIES", "128")
)

_SOCIAL_LANDING_PROGRESS_ROLLUP_CACHE: dict[Any, tuple[float, dict[str, Any]]] = {}

_SOCIAL_LANDING_PROGRESS_ROLLUP_CACHE_LOCK = Lock()

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
    return f"coalesce(nullif(regexp_replace(coalesce({expr}, ''), '[^0-9]', '', 'g'), '')::bigint, 0)"

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

    started_at = perf_counter()
    targets = _normalize_landing_progress_targets(payload.platforms, payload.account_handles)
    if not targets:
        total_ms = int((perf_counter() - started_at) * 1000)
        return {
            "rows": [],
            "cache_status": "bypass",
            "generated_at": datetime.now(tz=UTC).isoformat(),
            "timing": {"backend_ms": total_ms, "database_ms": 0, "total_ms": total_ms},
        }

    cache_key = _landing_progress_cache_key(targets)
    cached_payload = _get_ttl_cached_payload(
        _SOCIAL_LANDING_PROGRESS_ROLLUP_CACHE,
        _SOCIAL_LANDING_PROGRESS_ROLLUP_CACHE_LOCK,
        cache_key,
    )
    if cached_payload is not None:
        total_ms = int((perf_counter() - started_at) * 1000)
        cached_payload["cache_status"] = "hit"
        cached_payload["timing"] = {"backend_ms": total_ms, "database_ms": 0, "total_ms": total_ms}
        return cached_payload

    instagram_reported_comments_sql = _instagram_reported_comments_sql("p")
    try:
        db_started_at = perf_counter()
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
        db_ms = int((perf_counter() - db_started_at) * 1000)
        total_ms = int((perf_counter() - started_at) * 1000)
        generated_at = datetime.now(tz=UTC).isoformat()
        payload_out = {
            "rows": jsonable_encoder(rows),
            "cache_status": "miss",
            "generated_at": generated_at,
            "timing": {"backend_ms": total_ms, "database_ms": db_ms, "total_ms": total_ms},
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
            "Built social landing progress rollup targets=%s rows=%s elapsed_ms=%s db_ms=%s cache_status=miss",
            len(targets),
            len(rows),
            total_ms,
            db_ms,
        )
        return payload_out
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to read social landing progress rollup")
        raise _to_social_read_http_exception(exc) from exc

__all__ = [name for name in globals() if not name.startswith("__")]
