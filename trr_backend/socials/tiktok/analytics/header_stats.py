# ruff: noqa: F821, UP037
"""TikTok social-account profile lite header statistics.

Moved out of ``social_season_analytics_impl``; the legacy repository keeps
same-named wrappers that delegate through ``_LOCAL_ROOM_FUNCTIONS``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from psycopg2 import errors as psycopg_errors

from trr_backend.socials.control_plane.run_lifecycle import _legacy_module as _published_legacy_module
from trr_backend.socials.provider_registry import (
    LateNamespaceProvider,
    LateProviderProxy,
    adopt_published,
    publish_module_slot,
)

if TYPE_CHECKING:
    from trr_backend.db import pg

    # Repository names resolved through the published provider namespace.
    def _comment_lifecycle_supported(*args: Any, **kwargs: Any) -> Any: ...
    def _normalize_non_negative_int(*args: Any, **kwargs: Any) -> Any: ...
    def _normalize_social_account_profile_handle(*args: Any, **kwargs: Any) -> Any: ...
    def _social_account_profile_owner_match_sql(*args: Any, **kwargs: Any) -> Any: ...


# Room names are declared before the provider is built so that a late
# ``adopt_published`` (leaf imported after the repository published) still
# captures the repository wrappers for ``_room_callable``.
_LOCAL_ROOM_NAMES: set[str] = {
    "_tiktok_social_account_lite_header_stats",
}
_IMPORTED_CORE_NAMES: set[str] = set()
_LOCAL_ROOM_FUNCTIONS: dict[str, Any] = {}
_CORE_ROOM_WRAPPERS: dict[str, Any] = {}


_PROVIDER = LateNamespaceProvider(
    globals(),
    prefix="TIKTOK_ANALYTICS_HEADER_STATS_PROVIDER",
    room_names=_LOCAL_ROOM_NAMES,
    imported_names=_IMPORTED_CORE_NAMES,
    room_wrappers=_CORE_ROOM_WRAPPERS,
    commit=publish_module_slot(globals(), "_core"),
)
_core: Any = LateProviderProxy(_PROVIDER)
_require_provider_ready = _PROVIDER.require
_configure_legacy_provider = _PROVIDER.configure
_sync_core_overrides = _PROVIDER.sync
_room_callable = _PROVIDER.room_callable
adopt_published(_PROVIDER, _published_legacy_module)
del _published_legacy_module


def _tiktok_social_account_lite_header_stats(
    account_handle: str,
    *,
    conn: Any | None = None,
) -> dict[str, Any]:
    _sync_core_overrides()
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    owner_match_clause = _social_account_profile_owner_match_sql("tiktok", alias="p")
    lifecycle_supported = _comment_lifecycle_supported("tiktok_comments", conn=conn)
    active_comment_filter = "and c.is_missing is not true" if lifecycle_supported else ""
    sql = f"""
            with filtered_posts as materialized (
              select
                p.id,
                p.posted_at,
                coalesce(p.likes, 0)::bigint as likes,
                greatest(coalesce(p.comments_count, 0), 0)::int as reported_comments,
                coalesce(p.views, 0)::bigint as views,
                case
                  when jsonb_typeof(to_jsonb(p) -> 'media_urls') = 'array'
                    then to_jsonb(p) -> 'media_urls'
                  else '[]'::jsonb
                end as media_urls,
                case
                  when jsonb_typeof(to_jsonb(p) -> 'hosted_media_urls') = 'array'
                    then to_jsonb(p) -> 'hosted_media_urls'
                  else '[]'::jsonb
                end as hosted_media_urls,
                nullif(to_jsonb(p) ->> 'thumbnail_url', '') as thumbnail_url,
                nullif(to_jsonb(p) ->> 'hosted_thumbnail_url', '') as hosted_thumbnail_url,
                nullif(to_jsonb(p) ->> 'hosted_user_avatar_url', '') as hosted_user_avatar_url
              from social.tiktok_posts p
              where {owner_match_clause}
                and nullif(p.video_id, '') is not null
            ),
            post_totals as (
              select
                count(*)::int as total_posts,
                coalesce(sum(likes + reported_comments), 0)::bigint as total_engagement,
                coalesce(sum(views), 0)::bigint as total_views,
                min(posted_at) as first_post_at,
                max(posted_at) as last_post_at,
                count(*) filter (where reported_comments > 0)::int as retrieved_comment_posts,
                coalesce(sum(reported_comments), 0)::int as reported_comments,
                coalesce(
                  sum(
                    jsonb_array_length(media_urls)
                    + case when thumbnail_url is not null then 1 else 0 end
                  ),
                  0
                )::int as total_post_media_files,
                coalesce(
                  sum(
                    least(jsonb_array_length(hosted_media_urls), jsonb_array_length(media_urls))
                    + case
                        when thumbnail_url is not null and hosted_thumbnail_url is not null then 1
                        else 0
                      end
                  ),
                  0
                )::int as saved_post_media_files,
                count(distinct hosted_user_avatar_url) filter (where hosted_user_avatar_url is not null)::int
                  as saved_avatar_files
              from filtered_posts
            ),
            comment_counts as (
              select
                c.post_id,
                count(*)::int as saved_comments
              from social.tiktok_comments c
              join filtered_posts fp on fp.id = c.post_id
              where 1 = 1
                {active_comment_filter}
              group by c.post_id
            ),
            comment_totals as (
              select
                coalesce(sum(coalesce(cc.saved_comments, 0)), 0)::int as saved_comments,
                count(*) filter (where coalesce(cc.saved_comments, 0) > 0)::int as saved_comment_posts,
                coalesce(sum(greatest(fp.reported_comments - coalesce(cc.saved_comments, 0), 0)), 0)::int
                  as missing_comments
              from filtered_posts fp
              left join comment_counts cc on cc.post_id = fp.id
            ),
            comment_media_rows as (
              select
                case
                  when jsonb_typeof(to_jsonb(c) -> 'media_urls') = 'array'
                    then to_jsonb(c) -> 'media_urls'
                  else '[]'::jsonb
                end as media_urls,
                case
                  when jsonb_typeof(to_jsonb(c) -> 'hosted_media_urls') = 'array'
                    then to_jsonb(c) -> 'hosted_media_urls'
                  else '[]'::jsonb
                end as hosted_media_urls
              from social.tiktok_comments c
              join filtered_posts fp on fp.id = c.post_id
              where 1 = 1
                {active_comment_filter}
            ),
            comment_media_totals as (
              select
                coalesce(sum(jsonb_array_length(media_urls)), 0)::int as total_comment_media_files,
                coalesce(
                  sum(least(jsonb_array_length(hosted_media_urls), jsonb_array_length(media_urls))),
                  0
                )::int as saved_comment_media_files
              from comment_media_rows
            )
            select
              post_totals.total_posts,
              post_totals.total_engagement,
              post_totals.total_views,
              post_totals.first_post_at,
              post_totals.last_post_at,
              post_totals.total_post_media_files,
              post_totals.saved_post_media_files,
              comment_totals.saved_comments,
              comment_totals.saved_comment_posts,
              comment_totals.missing_comments,
              comment_media_totals.total_comment_media_files,
              comment_media_totals.saved_comment_media_files,
              post_totals.saved_avatar_files,
              post_totals.reported_comments,
              post_totals.retrieved_comment_posts
            from post_totals
            cross join comment_totals
            cross join comment_media_totals
            """
    try:
        if conn is None:
            row = pg.fetch_one(sql, [normalized_account]) or {}
        else:
            with pg.db_cursor(conn=conn, label="tiktok_lite_header_stats") as cur:
                row = pg.fetch_one_with_cursor(cur, sql, [normalized_account]) or {}
    except psycopg_errors.UndefinedTable:
        row = {}

    totals = {
        "total_posts": _normalize_non_negative_int(row.get("total_posts")),
        "total_engagement": _normalize_non_negative_int(row.get("total_engagement")),
        "total_views": _normalize_non_negative_int(row.get("total_views")),
        "first_post_at": row.get("first_post_at"),
        "last_post_at": row.get("last_post_at"),
    }
    saved_comments = _normalize_non_negative_int(row.get("saved_comments"))
    missing_comments = _normalize_non_negative_int(row.get("missing_comments"))
    reported_comments = _normalize_non_negative_int(row.get("reported_comments"))
    retrieved_comments = max(reported_comments, saved_comments + missing_comments)
    total_post_media_files = _normalize_non_negative_int(row.get("total_post_media_files"))
    saved_post_media_files = min(_normalize_non_negative_int(row.get("saved_post_media_files")), total_post_media_files)
    total_comment_media_files = _normalize_non_negative_int(row.get("total_comment_media_files"))
    saved_comment_media_files = min(
        _normalize_non_negative_int(row.get("saved_comment_media_files")),
        total_comment_media_files,
    )
    total_files = total_post_media_files + total_comment_media_files
    saved_files = saved_post_media_files + saved_comment_media_files
    saved_comment_posts = _normalize_non_negative_int(row.get("saved_comment_posts"))
    retrieved_comment_posts = max(
        saved_comment_posts,
        _normalize_non_negative_int(row.get("retrieved_comment_posts")),
    )
    return {
        "totals": totals,
        "comments_saved_summary": {
            "saved_comments": saved_comments,
            "retrieved_comments": retrieved_comments,
            "saved_comment_posts": saved_comment_posts,
            "retrieved_comment_posts": retrieved_comment_posts,
            "saved_comment_media_files": saved_comment_media_files,
        },
        "media_coverage": {
            "saved_files": min(saved_files, total_files),
            "total_files": total_files,
            "saved_post_media_files": saved_post_media_files,
            "total_post_media_files": total_post_media_files,
            "saved_comment_media_files": saved_comment_media_files,
            "total_comment_media_files": total_comment_media_files,
            "saved_avatar_files": _normalize_non_negative_int(row.get("saved_avatar_files")),
            "saved_reel_still_files": 0,
            "total_reel_still_files": 0,
        },
    }


_LOCAL_ROOM_FUNCTIONS.update({_name: globals()[_name] for _name in _LOCAL_ROOM_NAMES})
__all__ = [
    "_tiktok_social_account_lite_header_stats",
]
