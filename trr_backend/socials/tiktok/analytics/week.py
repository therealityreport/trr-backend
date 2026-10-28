# ruff: noqa: F821, UP037
"""TikTok week analytics: per-week post detail rows and the fast week summary.

Moved out of ``social_season_analytics_impl``; the legacy repository keeps
same-named wrappers that delegate through ``_LOCAL_ROOM_FUNCTIONS``.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import TYPE_CHECKING, Any

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
    def _as_text_list(*args: Any, **kwargs: Any) -> Any: ...
    def _build_display_thumbnail_payload(*args: Any, **kwargs: Any) -> Any: ...
    def _build_handle_details_from_tokens(*args: Any, **kwargs: Any) -> Any: ...
    def _build_post_status_payload(*args: Any, **kwargs: Any) -> Any: ...
    def _comment_lifecycle_supported(*args: Any, **kwargs: Any) -> Any: ...
    def _extract_platform_post_asset_manifest(*args: Any, **kwargs: Any) -> Any: ...
    def _iso(*args: Any, **kwargs: Any) -> Any: ...
    def _json_text_list(*args: Any, **kwargs: Any) -> Any: ...
    def _parse_hashtags(*args: Any, **kwargs: Any) -> Any: ...
    def _parse_mentions(*args: Any, **kwargs: Any) -> Any: ...
    def _platform_hosted_media_expr(*args: Any, **kwargs: Any) -> Any: ...
    def _platform_posts_has_column(*args: Any, **kwargs: Any) -> Any: ...
    def _platform_thumbnail_expr(*args: Any, **kwargs: Any) -> Any: ...
    def _resolve_platform_profile_url(*args: Any, **kwargs: Any) -> Any: ...
    def _resolve_post_avatar_url(*args: Any, **kwargs: Any) -> Any: ...
    def _safe_percent(*args: Any, **kwargs: Any) -> Any: ...
    def _tiktok_saves_expr(*args: Any, **kwargs: Any) -> Any: ...
    def _week_detail_order_by_clause(*args: Any, **kwargs: Any) -> Any: ...
    def _week_summary_fast_generic(*args: Any, **kwargs: Any) -> Any: ...


# Room names are declared before the provider is built so that a late
# ``adopt_published`` (leaf imported after the repository published) still
# captures the repository wrappers for ``_room_callable``.
_LOCAL_ROOM_NAMES: set[str] = {
    "_week_detail_tiktok",
    "_week_summary_fast_tiktok",
}
_IMPORTED_CORE_NAMES: set[str] = set()
_LOCAL_ROOM_FUNCTIONS: dict[str, Any] = {}
_CORE_ROOM_WRAPPERS: dict[str, Any] = {}


_PROVIDER = LateNamespaceProvider(
    globals(),
    prefix="TIKTOK_ANALYTICS_WEEK_PROVIDER",
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


def _week_detail_tiktok(
    season_id: str,
    *,
    start_dt: datetime,
    end_dt: datetime,
    account_handles: set[str],
    max_comments: int,
    post_limit: int = 20,
    post_offset: int = 0,
    sort_field: str = "posted_at",
    sort_dir: str = "desc",
) -> dict[str, Any]:
    _sync_core_overrides()
    thumbnail_expr = _platform_thumbnail_expr("p", "tiktok")
    saves_expr = _tiktok_saves_expr("p")
    mentions_expr = (
        "coalesce(p.mentions, '[]'::jsonb)" if _platform_posts_has_column("tiktok", "mentions") else "'[]'::jsonb"
    )
    account_handles_list = sorted(account_handles)
    account_filter = (
        "and ltrim(lower(coalesce(nullif(p.username, ''), nullif(p.source_account, ''), '')), '@') = any(%s)"
        if account_handles_list
        else ""
    )
    params = [season_id, start_dt, end_dt]
    if account_handles_list:
        params.append(account_handles_list)
    count_rows = pg.fetch_all(
        f"""
        select count(*)::int as post_count
        from social.tiktok_posts p
        where p.season_id = %s
          and p.posted_at >= %s
          and p.posted_at <= %s
        {account_filter}
        """,
        params,
    )
    total_post_count = int((count_rows[0] if count_rows else {}).get("post_count") or 0)
    effective_post_limit = max(0, post_limit + post_offset)
    order_by_clause = _week_detail_order_by_clause(
        timestamp_expr="p.posted_at",
        field_expr_by_sort={
            "likes": "p.likes",
            "views": "p.views",
            "comments_count": "p.comments_count",
            "shares": "p.shares",
            "retweets": "0",
        },
        engagement_expr=(
            "coalesce(p.likes, 0) + coalesce(p.comments_count, 0) + coalesce(p.shares, 0) + coalesce(p.views, 0)"
        ),
        sort_field=sort_field,
        sort_dir=sort_dir,
    )
    posts = pg.fetch_all(
        f"""
        select
          p.id,
          p.video_id as source_id,
          p.username as author,
          p.nickname,
          p.description as text,
          coalesce(p.likes, 0) as likes,
          coalesce(p.comments_count, 0) as comments_count,
          coalesce(p.shares, 0) as shares,
          {saves_expr} as saves,
          coalesce(p.views, 0) as views,
          coalesce(p.hashtags, '[]'::jsonb) as hashtags,
          {mentions_expr} as mentions,
          {thumbnail_expr} as thumbnail_url,
          coalesce(nullif(p.thumbnail_url, ''), '') as source_thumbnail_url,
          coalesce(nullif(to_jsonb(p) ->> 'hosted_thumbnail_url', ''), '') as hosted_thumbnail_url,
          coalesce(p.media_urls, '[]'::jsonb) as source_media_urls,
          {_platform_hosted_media_expr("p")} as hosted_media_urls,
          p.duration_seconds,
          nullif(coalesce(to_jsonb(p) ->> 'user_avatar_url', ''), '') as user_avatar_url,
          nullif(coalesce(to_jsonb(p) ->> 'hosted_user_avatar_url', ''), '') as hosted_user_avatar_url,
          coalesce(to_jsonb(p) -> 'raw_data', '{{}}'::jsonb) as raw_data,
          p.posted_at as ts
        from social.tiktok_posts p
        where p.season_id = %s
          and p.posted_at >= %s
          and p.posted_at <= %s
          {account_filter}
        order by {order_by_clause}
        limit {effective_post_limit}
        """,
        params,
    )

    post_ids = [p["id"] for p in posts]
    comments_by_post: dict[Any, list[dict]] = defaultdict(list)
    comment_counts_by_post: dict[Any, int] = defaultdict(int)
    tiktok_comment_active_filter = (
        "and coalesce(c.is_missing, false) = false" if _comment_lifecycle_supported("tiktok_comments") else ""
    )

    if post_ids:
        count_rows = pg.fetch_all(
            f"""
            select c.post_id, count(*)::int as cnt
            from social.tiktok_comments c
            where c.post_id = any(%s::uuid[])
              {tiktok_comment_active_filter}
            group by c.post_id
            """,
            [post_ids],
        )
        for row in count_rows:
            comment_counts_by_post[row["post_id"]] = row["cnt"]

        limit_clause = f"limit {max_comments}" if max_comments > 0 else "limit all"
        comment_rows = pg.fetch_all(
            f"""
            select sub.*
            from unnest(%s::uuid[]) as pid(id)
            cross join lateral (
              select
                c.comment_id,
                c.post_id,
                c.username as author,
                c.text,
                coalesce(c.likes, 0) as likes,
                coalesce(c.is_reply, false) as is_reply,
                coalesce(c.reply_count, 0) as reply_count,
                c.created_at,
                coalesce(to_jsonb(c) -> 'media_urls', '[]'::jsonb) as media_urls,
                coalesce(to_jsonb(c) -> 'hosted_media_urls', '[]'::jsonb) as hosted_media_urls,
                nullif(coalesce(to_jsonb(c) ->> 'media_mirror_status', ''), '') as media_mirror_status
              from social.tiktok_comments c
              where c.post_id = pid.id
                {tiktok_comment_active_filter}
              order by c.likes desc nulls last, c.created_at asc
              {limit_clause}
            ) sub
            """,
            [post_ids],
        )
        for row in comment_rows:
            comments_by_post[row["post_id"]].append(row)

    result_posts = []
    total_engagement = 0
    total_comments_count = 0
    saved_comments_count = 0
    for p in posts:
        engagement = p["likes"] + p["comments_count"] + p["shares"] + p["views"]
        total_engagement += engagement
        total_comments_count += p["comments_count"]
        db_comment_count = comment_counts_by_post.get(p["id"], 0)
        saved_comments_count += db_comment_count
        post_comments = comments_by_post.get(p["id"], [])

        hashtags = _json_text_list(p.get("hashtags"), strip_prefix="#")
        if not hashtags:
            hashtags = _parse_hashtags(p.get("text"))
        mentions = _json_text_list(p.get("mentions"), prefix="@", strip_prefix="@")
        if not mentions:
            mentions = _parse_mentions(p.get("text"))

        author = p["author"] or ""
        user_avatar_url = _resolve_post_avatar_url(
            hosted_avatar_url=p.get("hosted_user_avatar_url"),
            direct_avatar_url=p.get("user_avatar_url"),
            raw_data=p.get("raw_data"),
        )
        user_url = _resolve_platform_profile_url("tiktok", handle=author)
        mentions_detail = _build_handle_details_from_tokens("tiktok", mentions)
        source_media_urls = _as_text_list(p.get("source_media_urls"))
        hosted_media_urls = _as_text_list(p.get("hosted_media_urls"))
        source_thumbnail_url = str(p.get("source_thumbnail_url") or "").strip() or None
        hosted_thumbnail_url = str(p.get("hosted_thumbnail_url") or "").strip() or None
        display_thumbnail = _build_display_thumbnail_payload(
            asset_manifest=_extract_platform_post_asset_manifest("tiktok", dict(p)),
            hosted_thumbnail_url=hosted_thumbnail_url,
            source_thumbnail_url=source_thumbnail_url,
            hosted_media_urls=hosted_media_urls,
            source_media_urls=source_media_urls,
        )
        result_posts.append(
            {
                "source_id": p["source_id"],
                "author": author,
                "nickname": p.get("nickname") or "",
                "text": p.get("text") or "",
                "url": f"https://www.tiktok.com/@{author}/video/{p['source_id']}" if p["source_id"] and author else "",
                "posted_at": _iso(p["ts"]),
                "likes": p["likes"],
                "comments_count": p["comments_count"],
                "shares": p["shares"],
                "saves": int(p.get("saves") or 0),
                "views": p["views"],
                "hashtags": hashtags,
                "thumbnail_url": p.get("thumbnail_url"),
                "source_thumbnail_url": source_thumbnail_url,
                "hosted_thumbnail_url": hosted_thumbnail_url,
                **display_thumbnail,
                "media_urls": source_media_urls,
                "source_media_urls": source_media_urls,
                "hosted_media_urls": hosted_media_urls,
                "duration_seconds": p.get("duration_seconds"),
                "mentions": mentions,
                "user": {
                    "username": author,
                    "display_name": p.get("nickname") or "",
                    "url": user_url,
                    "avatar_url": user_avatar_url,
                },
                "mentions_detail": mentions_detail,
                "engagement": engagement,
                "total_comments_available": db_comment_count,
                "comments": [
                    {
                        "comment_id": c["comment_id"],
                        "author": c["author"] or "",
                        "text": c["text"] or "",
                        "likes": c["likes"],
                        "is_reply": c["is_reply"],
                        "reply_count": c["reply_count"],
                        "created_at": _iso(c["created_at"]),
                        "media_urls": _as_text_list(c.get("media_urls")),
                        "hosted_media_urls": _as_text_list(c.get("hosted_media_urls")),
                        "media_mirror_status": c.get("media_mirror_status"),
                    }
                    for c in post_comments
                ],
                "status": _build_post_status_payload(
                    expected_comments=int(p["comments_count"] or 0),
                    saved_comments=db_comment_count,
                    source_media_urls=_as_text_list(p.get("source_media_urls")),
                    hosted_media_urls=_as_text_list(p.get("hosted_media_urls")),
                    source_thumbnail_url=str(p.get("source_thumbnail_url") or "").strip() or None,
                    hosted_thumbnail_url=str(p.get("hosted_thumbnail_url") or "").strip() or None,
                    media_mirror_status=(
                        "partial"
                        if len(_as_text_list(p.get("hosted_media_urls")))
                        < len(_as_text_list(p.get("source_media_urls")))
                        else "complete"
                    ),
                    last_refresh_at=None,
                    last_refresh_reason="tiktok_metadata_unavailable"
                    if not _as_text_list(p.get("source_media_urls")) and int(p.get("views") or 0) <= 0
                    else None,
                ),
            }
        )

    return {
        "posts": result_posts,
        "total_posts": total_post_count,
        "totals": {
            "posts": len(result_posts),
            "total_comments": total_comments_count,
            "total_engagement": total_engagement,
            "expected_comments_total": total_comments_count,
            "saved_comments_total": saved_comments_count,
            "comments_saved_pct": _safe_percent(saved_comments_count, total_comments_count),
        },
    }


def _week_summary_fast_tiktok(
    *,
    season_id: str,
    start_dt: datetime,
    end_dt: datetime,
    account_handles: set[str],
) -> dict[str, Any]:
    _sync_core_overrides()
    comments_active_filter = (
        "and coalesce(c.is_missing, false) = false" if _comment_lifecycle_supported("tiktok_comments") else ""
    )
    return _week_summary_fast_generic(
        season_id=season_id,
        start_dt=start_dt,
        end_dt=end_dt,
        account_handles=account_handles,
        post_table="tiktok_posts",
        timestamp_column="posted_at",
        account_expr="coalesce(nullif(p.username, ''), nullif(p.source_account, ''))",
        comment_table="tiktok_comments",
        comment_fk_column="post_id",
        likes_expr="p.likes",
        comments_expr="p.comments_count",
        views_expr="p.views",
        shares_expr="p.shares",
        comments_active_filter=comments_active_filter,
    )


_LOCAL_ROOM_FUNCTIONS.update({_name: globals()[_name] for _name in _LOCAL_ROOM_NAMES})
__all__ = [
    "_week_detail_tiktok",
    "_week_summary_fast_tiktok",
]
