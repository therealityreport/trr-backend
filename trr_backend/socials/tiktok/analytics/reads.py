# ruff: noqa: F821, UP037
"""TikTok season analytics read models: overview, cast members, hashtags, sounds, content
health, sound detail/posts, post detail, and sentiment trends.

Moved out of ``trr_backend.socials.analytics.read_models``, which re-exports them.
The legacy repository keeps same-named wrappers that delegate through
``_LOCAL_ROOM_FUNCTIONS``; repository helpers are resolved through the published
provider namespace (each read calls ``_sync_core_overrides()`` itself).
"""

from __future__ import annotations

from collections.abc import Mapping
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
    def _coerce_dt(*args: Any, **kwargs: Any) -> Any: ...
    def _iso(*args: Any, **kwargs: Any) -> Any: ...
    def _median_int(*args: Any, **kwargs: Any) -> Any: ...
    def _normalize_tiktok_sound_id(*args: Any, **kwargs: Any) -> Any: ...
    def _platform_posts_has_column(*args: Any, **kwargs: Any) -> Any: ...
    def _relation_exists(*args: Any, **kwargs: Any) -> Any: ...
    def _tiktok_canonical_url_expr(*args: Any, **kwargs: Any) -> Any: ...
    def _tiktok_filter_sql(*args: Any, **kwargs: Any) -> Any: ...
    def _tiktok_saves_expr(*args: Any, **kwargs: Any) -> Any: ...
    def get_post_comments(*args: Any, **kwargs: Any) -> Any: ...


# Room names are declared before the provider is built so that a late
# ``adopt_published`` (leaf imported after the repository published) still
# captures the repository wrappers for ``_room_callable``.
_LOCAL_ROOM_NAMES: set[str] = {
    "get_tiktok_overview",
    "get_tiktok_cast_members",
    "get_tiktok_hashtags",
    "get_tiktok_sounds",
    "get_tiktok_content_health",
    "get_tiktok_sound_detail",
    "get_tiktok_sound_posts",
    "get_tiktok_post_detail",
    "get_tiktok_sentiment_trends",
}
_IMPORTED_CORE_NAMES: set[str] = set()
_LOCAL_ROOM_FUNCTIONS: dict[str, Any] = {}
_CORE_ROOM_WRAPPERS: dict[str, Any] = {}


_PROVIDER = LateNamespaceProvider(
    globals(),
    prefix="TIKTOK_ANALYTICS_READS_PROVIDER",
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


def get_tiktok_overview(
    season_id: str,
    *,
    date_start: datetime | None = None,
    date_end: datetime | None = None,
    cast_member_id: str | None = None,
    hashtag: str | None = None,
    keyword: str | None = None,
    sound_id: str | None = None,
) -> dict[str, Any]:
    _sync_core_overrides()
    where_sql, where_params = _tiktok_filter_sql(
        season_id=season_id,
        date_start=date_start,
        date_end=date_end,
        cast_member_id=cast_member_id,
        hashtag=hashtag,
        keyword=keyword,
        sound_id=sound_id,
    )
    saves_expr = _tiktok_saves_expr("p")
    row = (
        pg.fetch_one(
            f"""
        select
          count(*)::int as post_count,
          coalesce(sum(coalesce(p.views, 0)), 0)::bigint as total_views,
          coalesce(sum(coalesce(p.likes, 0)), 0)::bigint as total_likes,
          coalesce(sum(coalesce(p.comments_count, 0)), 0)::bigint as total_comments,
          coalesce(sum(coalesce(p.shares, 0)), 0)::bigint as total_shares,
          coalesce(sum({saves_expr}), 0)::bigint as total_saves,
          coalesce(
            avg(
              case
                when coalesce(p.views, 0) > 0
                  then (
                    coalesce(p.likes, 0)
                    + coalesce(p.comments_count, 0)
                    + coalesce(p.shares, 0)
                    + {saves_expr}
                  )::numeric / p.views
                else null
              end
            ),
            0
          )::numeric as avg_engagement_rate
        from social.tiktok_posts p
        where {where_sql}
        """,
            where_params,
        )
        or {}
    )

    daily_rows = pg.fetch_all(
        f"""
        select
          date_trunc('day', p.posted_at)::date as period_start,
          count(*)::int as posts,
          coalesce(sum(coalesce(p.views, 0)), 0)::bigint as views,
          coalesce(sum(coalesce(p.likes, 0)), 0)::bigint as likes,
          coalesce(sum(coalesce(p.comments_count, 0)), 0)::bigint as comments,
          coalesce(sum(coalesce(p.shares, 0)), 0)::bigint as shares,
          coalesce(sum({saves_expr}), 0)::bigint as saves
        from social.tiktok_posts p
        where {where_sql}
          and p.posted_at is not null
        group by period_start
        order by period_start asc
        """,
        where_params,
    )
    hourly_rows = pg.fetch_all(
        f"""
        select
          date_trunc('hour', p.posted_at) as period_start,
          count(*)::int as posts,
          coalesce(sum(coalesce(p.views, 0)), 0)::bigint as views,
          coalesce(sum(coalesce(p.likes, 0)), 0)::bigint as likes,
          coalesce(sum(coalesce(p.comments_count, 0)), 0)::bigint as comments,
          coalesce(sum(coalesce(p.shares, 0)), 0)::bigint as shares,
          coalesce(sum({saves_expr}), 0)::bigint as saves
        from social.tiktok_posts p
        where {where_sql}
          and p.posted_at is not null
        group by period_start
        order by period_start asc
        """,
        where_params,
    )
    weekly_rows = pg.fetch_all(
        f"""
        select
          date_trunc('week', p.posted_at)::date as period_start,
          count(*)::int as posts,
          coalesce(sum(coalesce(p.views, 0)), 0)::bigint as views,
          coalesce(sum(coalesce(p.likes, 0)), 0)::bigint as likes,
          coalesce(sum(coalesce(p.comments_count, 0)), 0)::bigint as comments,
          coalesce(sum(coalesce(p.shares, 0)), 0)::bigint as shares,
          coalesce(sum({saves_expr}), 0)::bigint as saves
        from social.tiktok_posts p
        where {where_sql}
          and p.posted_at is not null
        group by period_start
        order by period_start asc
        """,
        where_params,
    )

    def _series_row_payload(series_row: Mapping[str, Any]) -> dict[str, Any]:
        views = int(series_row.get("views") or 0)
        likes = int(series_row.get("likes") or 0)
        comments = int(series_row.get("comments") or 0)
        shares = int(series_row.get("shares") or 0)
        saves = int(series_row.get("saves") or 0)
        engagement_rate = round(((likes + comments + shares + saves) / views), 6) if views > 0 else 0.0
        return {
            "period_start": str(series_row.get("period_start") or ""),
            "posts": int(series_row.get("posts") or 0),
            "views": views,
            "likes": likes,
            "comments": comments,
            "shares": shares,
            "saves": saves,
            "engagement_rate": engagement_rate,
        }

    hourly_series = [_series_row_payload(series_row) for series_row in hourly_rows]
    daily_series = [_series_row_payload(series_row) for series_row in daily_rows]
    weekly_series = [_series_row_payload(series_row) for series_row in weekly_rows]

    wow_delta: dict[str, float | None] = {
        "views": None,
        "likes": None,
        "comments": None,
        "shares": None,
        "saves": None,
        "engagement_rate": None,
    }
    if len(weekly_series) >= 2:
        current = weekly_series[-1]
        previous = weekly_series[-2]

        def _pct_delta(current_value: Any, previous_value: Any) -> float | None:
            current_number = float(current_value or 0)
            previous_number = float(previous_value or 0)
            if previous_number <= 0:
                return None
            return round(((current_number - previous_number) / previous_number) * 100.0, 3)

        wow_delta = {
            "views": _pct_delta(current.get("views"), previous.get("views")),
            "likes": _pct_delta(current.get("likes"), previous.get("likes")),
            "comments": _pct_delta(current.get("comments"), previous.get("comments")),
            "shares": _pct_delta(current.get("shares"), previous.get("shares")),
            "saves": _pct_delta(current.get("saves"), previous.get("saves")),
            "engagement_rate": _pct_delta(current.get("engagement_rate"), previous.get("engagement_rate")),
        }

    return {
        "season_id": season_id,
        "kpis": {
            "post_count": int(row.get("post_count") or 0),
            "views": int(row.get("total_views") or 0),
            "likes": int(row.get("total_likes") or 0),
            "comments": int(row.get("total_comments") or 0),
            "shares": int(row.get("total_shares") or 0),
            "saves": int(row.get("total_saves") or 0),
            "engagement_rate": float(row.get("avg_engagement_rate") or 0.0),
            "follower_delta": None,
        },
        "wow_delta_pct": wow_delta,
        "time_series": {
            "hourly": hourly_series,
            "daily": daily_series,
            "weekly": weekly_series,
        },
    }


def get_tiktok_cast_members(
    season_id: str,
    *,
    date_start: datetime | None = None,
    date_end: datetime | None = None,
) -> dict[str, Any]:
    _sync_core_overrides()
    if not _relation_exists("social.tiktok_post_cast_members"):
        return {"season_id": season_id, "cast_members": []}
    start_dt = _coerce_dt(date_start)
    end_dt = _coerce_dt(date_end)
    params: list[Any] = [season_id]
    date_sql = ""
    if start_dt:
        date_sql += " and p.posted_at >= %s"
        params.append(start_dt)
    if end_dt:
        date_sql += " and p.posted_at <= %s"
        params.append(end_dt)
    rows = pg.fetch_all(
        f"""
        select
          coalesce(cm.cast_member_id::text, '') as cast_member_id,
          coalesce(nullif(cm.cast_member_name, ''), 'Unknown') as cast_member_name,
          count(distinct cm.post_id)::int as post_count,
          coalesce(
            avg(coalesce(p.likes, 0) + coalesce(p.comments_count, 0) + coalesce(p.shares, 0)),
            0
          )::numeric as avg_engagement,
          max(coalesce(p.likes, 0) + coalesce(p.comments_count, 0) + coalesce(p.shares, 0))::bigint as max_engagement
        from social.tiktok_post_cast_members cm
        join social.tiktok_posts p on p.id = cm.post_id
        where p.season_id = %s::uuid
          {date_sql}
        group by cast_member_id, cast_member_name
        order by avg_engagement desc, post_count desc, cast_member_name asc
        """,
        params,
    )
    return {
        "season_id": season_id,
        "cast_members": [
            {
                "cast_member_id": str(row.get("cast_member_id") or "") or None,
                "cast_member_name": str(row.get("cast_member_name") or ""),
                "post_count": int(row.get("post_count") or 0),
                "avg_engagement": float(row.get("avg_engagement") or 0.0),
                "top_post_engagement": int(row.get("max_engagement") or 0),
            }
            for row in rows
        ],
    }


def get_tiktok_hashtags(
    season_id: str,
    *,
    token_type: str = "hashtag",
    date_start: datetime | None = None,
    date_end: datetime | None = None,
) -> dict[str, Any]:
    _sync_core_overrides()
    if not _relation_exists("social.tiktok_post_tokens"):
        return {"season_id": season_id, "token_type": token_type, "tokens": []}
    normalized_type = str(token_type or "hashtag").strip().lower()
    if normalized_type not in {"hashtag", "keyword", "mention"}:
        normalized_type = "hashtag"
    start_dt = _coerce_dt(date_start)
    end_dt = _coerce_dt(date_end)
    params: list[Any] = [season_id, normalized_type]
    date_sql = ""
    if start_dt:
        date_sql += " and p.posted_at >= %s"
        params.append(start_dt)
    if end_dt:
        date_sql += " and p.posted_at <= %s"
        params.append(end_dt)
    rows = pg.fetch_all(
        f"""
        select
          tk.token,
          tk.normalized_token,
          count(*)::int as use_count,
          count(distinct tk.post_id)::int as post_count
        from social.tiktok_post_tokens tk
        join social.tiktok_posts p on p.id = tk.post_id
        where p.season_id = %s::uuid
          and tk.token_type = %s
          {date_sql}
        group by tk.token, tk.normalized_token
        order by use_count desc, post_count desc, tk.normalized_token asc
        limit 250
        """,
        params,
    )
    return {
        "season_id": season_id,
        "token_type": normalized_type,
        "tokens": [
            {
                "token": str(row.get("token") or ""),
                "normalized_token": str(row.get("normalized_token") or ""),
                "use_count": int(row.get("use_count") or 0),
                "post_count": int(row.get("post_count") or 0),
            }
            for row in rows
        ],
    }


def get_tiktok_sounds(
    season_id: str,
    *,
    date_start: datetime | None = None,
    date_end: datetime | None = None,
    search: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    _sync_core_overrides()
    safe_limit = max(1, min(int(limit), 250))
    if not _platform_posts_has_column("tiktok", "sound_id"):
        return {"season_id": season_id, "sounds": []}

    start_dt = _coerce_dt(date_start)
    end_dt = _coerce_dt(date_end)
    params: list[Any] = [season_id]
    where_parts = ["p.season_id = %s::uuid", "coalesce(p.sound_id, '') <> ''"]
    if start_dt:
        where_parts.append("p.posted_at >= %s")
        params.append(start_dt)
    if end_dt:
        where_parts.append("p.posted_at <= %s")
        params.append(end_dt)

    search_value = str(search or "").strip().lower()
    if search_value:
        where_parts.append(
            """
            (
              lower(coalesce(p.sound_id, '')) like %s
              or lower(coalesce(p.sound_title, '')) like %s
              or lower(coalesce(p.sound_author, '')) like %s
            )
            """
        )
        wildcard = f"%{search_value}%"
        params.extend([wildcard, wildcard, wildcard])

    saves_expr = _tiktok_saves_expr("p")
    rows = pg.fetch_all(
        f"""
        select
          p.sound_id,
          coalesce(nullif(max(p.sound_title), ''), nullif(max(s.title), ''), nullif(max(p.sound_id), '')) as title,
          coalesce(nullif(max(p.sound_author), ''), nullif(max(s.artist_name), '')) as artist_name,
          max(coalesce(p.sound_usage_count, 0))::bigint as usage_count,
          count(*)::int as creator_post_count,
          coalesce(sum(coalesce(p.views, 0)), 0)::bigint as creator_views,
          coalesce(sum(coalesce(p.likes, 0)), 0)::bigint as creator_likes,
          coalesce(sum(coalesce(p.comments_count, 0)), 0)::bigint as creator_comments,
          coalesce(sum(coalesce(p.shares, 0)), 0)::bigint as creator_shares,
          coalesce(sum({saves_expr}), 0)::bigint as creator_saves,
          max(p.posted_at) as last_creator_post_at,
          max(s.last_seen_at) as last_seen_at
        from social.tiktok_posts p
        left join social.tiktok_sounds s on s.sound_id = p.sound_id
        where {" and ".join(where_parts)}
        group by p.sound_id
        order by usage_count desc, creator_post_count desc, p.sound_id asc
        limit %s
        """,
        [*params, safe_limit],
    )

    related_counts_by_sound: dict[str, int] = {}
    if _relation_exists("social.tiktok_sound_posts"):
        related_rows = pg.fetch_all(
            """
            select sound_id, count(*)::int as related_post_count
            from social.tiktok_sound_posts
            where sound_id = any(%s::text[])
            group by sound_id
            """,
            [[str(row.get("sound_id") or "") for row in rows if str(row.get("sound_id") or "")]],
        )
        related_counts_by_sound = {
            str(item.get("sound_id") or ""): int(item.get("related_post_count") or 0) for item in related_rows
        }

    sounds = []
    for row in rows:
        sound_id = str(row.get("sound_id") or "")
        if not sound_id:
            continue
        views = int(row.get("creator_views") or 0)
        likes = int(row.get("creator_likes") or 0)
        comments = int(row.get("creator_comments") or 0)
        shares = int(row.get("creator_shares") or 0)
        saves = int(row.get("creator_saves") or 0)
        engagement_rate = round(((likes + comments + shares + saves) / views), 6) if views > 0 else 0.0
        sounds.append(
            {
                "sound_id": sound_id,
                "title": str(row.get("title") or "") or None,
                "artist_name": str(row.get("artist_name") or "") or None,
                "usage_count": int(row.get("usage_count") or 0),
                "creator_post_count": int(row.get("creator_post_count") or 0),
                "creator_views": views,
                "creator_likes": likes,
                "creator_comments": comments,
                "creator_shares": shares,
                "creator_saves": saves,
                "creator_engagement_rate": engagement_rate,
                "related_post_count": int(related_counts_by_sound.get(sound_id, 0)),
                "last_creator_post_at": _iso(_coerce_dt(row.get("last_creator_post_at"))),
                "last_seen_at": _iso(_coerce_dt(row.get("last_seen_at"))),
            }
        )
    return {"season_id": season_id, "sounds": sounds}


def get_tiktok_content_health(
    season_id: str,
    *,
    date_start: datetime | None = None,
    date_end: datetime | None = None,
    cast_member_id: str | None = None,
    hashtag: str | None = None,
    keyword: str | None = None,
    sound_id: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    _sync_core_overrides()
    where_sql, where_params = _tiktok_filter_sql(
        season_id=season_id,
        date_start=date_start,
        date_end=date_end,
        cast_member_id=cast_member_id,
        hashtag=hashtag,
        keyword=keyword,
        sound_id=sound_id,
    )
    safe_limit = max(1, min(int(limit), 250))
    saves_expr = _tiktok_saves_expr("p")
    quality_expr = "coalesce(p.quality_score, 0)" if _platform_posts_has_column("tiktok", "quality_score") else "0"
    velocity_expr = "coalesce(p.velocity_24h, 0)" if _platform_posts_has_column("tiktok", "velocity_24h") else "0"
    sound_id_expr = "coalesce(p.sound_id, '')" if _platform_posts_has_column("tiktok", "sound_id") else "''"
    sound_title_expr = "coalesce(p.sound_title, '')" if _platform_posts_has_column("tiktok", "sound_title") else "''"
    sound_author_expr = "coalesce(p.sound_author, '')" if _platform_posts_has_column("tiktok", "sound_author") else "''"
    canonical_url_expr = _tiktok_canonical_url_expr("p")
    rows = pg.fetch_all(
        f"""
        select
          p.id::text as id,
          p.video_id,
          p.source_account,
          p.posted_at,
          coalesce(p.description, '') as caption,
          coalesce(p.thumbnail_url, '') as thumbnail_url,
          {canonical_url_expr} as url,
          coalesce(p.views, 0)::bigint as views,
          coalesce(p.likes, 0)::bigint as likes,
          coalesce(p.comments_count, 0)::bigint as comments,
          coalesce(p.shares, 0)::bigint as shares,
          {saves_expr}::bigint as saves,
          {quality_expr}::numeric as quality_score,
          {velocity_expr}::numeric as velocity_24h,
          {sound_id_expr} as sound_id,
          {sound_title_expr} as sound_title,
          {sound_author_expr} as sound_author
        from social.tiktok_posts p
        where {where_sql}
        order by p.posted_at desc nulls last
        limit %s
        """,
        [*where_params, max(400, safe_limit)],
    )
    if not rows:
        return {"season_id": season_id, "thresholds": {}, "posts": []}

    saves_values = [int(row.get("saves") or 0) for row in rows]
    comments_values = [int(row.get("comments") or 0) for row in rows]
    velocity_values = [int(float(row.get("velocity_24h") or 0)) for row in rows]
    quality_values = [float(row.get("quality_score") or 0) for row in rows]

    median_saves = _median_int(saves_values)
    median_comments = _median_int(comments_values)
    median_velocity = _median_int(velocity_values)
    non_zero_quality = sorted(value for value in quality_values if value > 0)
    median_quality = non_zero_quality[len(non_zero_quality) // 2] if non_zero_quality else 0.0

    posts: list[dict[str, Any]] = []
    for row in rows:
        saves = int(row.get("saves") or 0)
        comments = int(row.get("comments") or 0)
        views = int(row.get("views") or 0)
        likes = int(row.get("likes") or 0)
        shares = int(row.get("shares") or 0)
        quality_score = float(row.get("quality_score") or 0)
        velocity_24h = float(row.get("velocity_24h") or 0)

        flags: list[str] = []
        if median_saves > 0 and saves < (median_saves * 0.5):
            flags.append("low_saves")
        if median_comments > 0 and comments < (median_comments * 0.5):
            flags.append("low_comments")
        if median_velocity > 0 and velocity_24h < (median_velocity * 0.5):
            flags.append("low_velocity_24h")
        if median_quality > 0 and quality_score > 0 and quality_score < (median_quality * 0.6):
            flags.append("low_quality_score")
        if not str(row.get("thumbnail_url") or "").strip():
            flags.append("missing_thumbnail")
        if not str(row.get("caption") or "").strip():
            flags.append("missing_caption")

        if not flags:
            continue

        health_score = max(0.0, round(100.0 - (len(flags) * 12.5), 2))
        engagement_rate = round(((likes + comments + shares + saves) / views), 6) if views > 0 else 0.0
        posts.append(
            {
                "post_id": str(row.get("video_id") or ""),
                "source_account": str(row.get("source_account") or "") or None,
                "posted_at": _iso(_coerce_dt(row.get("posted_at"))),
                "caption": str(row.get("caption") or "") or None,
                "thumbnail_url": str(row.get("thumbnail_url") or "") or None,
                "url": str(row.get("url") or "") or None,
                "sound_id": str(row.get("sound_id") or "") or None,
                "sound_title": str(row.get("sound_title") or "") or None,
                "sound_author": str(row.get("sound_author") or "") or None,
                "metrics": {
                    "views": views,
                    "likes": likes,
                    "comments": comments,
                    "shares": shares,
                    "saves": saves,
                    "velocity_24h": velocity_24h,
                    "quality_score": quality_score,
                    "engagement_rate": engagement_rate,
                },
                "reason_flags": flags,
                "health_score": health_score,
            }
        )

    posts.sort(
        key=lambda item: (
            len(item.get("reason_flags") or []),
            -(float((item.get("metrics") or {}).get("engagement_rate") or 0)),
            str(item.get("posted_at") or ""),
        ),
        reverse=True,
    )

    return {
        "season_id": season_id,
        "thresholds": {
            "median_saves": median_saves,
            "median_comments": median_comments,
            "median_velocity_24h": median_velocity,
            "median_quality_score": round(median_quality, 6),
        },
        "posts": posts[:safe_limit],
    }


def get_tiktok_sound_detail(season_id: str, *, sound_id: str) -> dict[str, Any]:
    _sync_core_overrides()
    normalized_sound_id = _normalize_tiktok_sound_id(sound_id)
    if not normalized_sound_id:
        raise ValueError("Invalid sound_id")
    if not _relation_exists("social.tiktok_sounds"):
        return {"season_id": season_id, "sound": None}
    row = pg.fetch_one(
        """
        select
          sound_id,
          title,
          artist_name,
          usage_count,
          source_url,
          last_seen_at,
          updated_at
        from social.tiktok_sounds
        where sound_id = %s
        """,
        [normalized_sound_id],
    )
    creator_post_count = 0
    if _platform_posts_has_column("tiktok", "sound_id"):
        creator_row = (
            pg.fetch_one(
                """
            select count(*)::int as count
            from social.tiktok_posts p
            where p.season_id = %s::uuid
              and coalesce(p.sound_id, '') = %s
            """,
                [season_id, normalized_sound_id],
            )
            or {}
        )
        creator_post_count = int(creator_row.get("count") or 0)
    return {
        "season_id": season_id,
        "sound": (
            {
                "sound_id": str(row.get("sound_id") or ""),
                "title": str(row.get("title") or "") or None,
                "artist_name": str(row.get("artist_name") or "") or None,
                "usage_count": int(row.get("usage_count") or 0),
                "source_url": str(row.get("source_url") or "") or None,
                "last_seen_at": _iso(_coerce_dt(row.get("last_seen_at"))),
                "updated_at": _iso(_coerce_dt(row.get("updated_at"))),
                "creator_post_count": creator_post_count,
            }
            if row
            else None
        ),
    }


def get_tiktok_sound_posts(
    season_id: str,
    *,
    sound_id: str,
    limit: int = 100,
) -> dict[str, Any]:
    _sync_core_overrides()
    normalized_sound_id = _normalize_tiktok_sound_id(sound_id)
    if not normalized_sound_id:
        raise ValueError("Invalid sound_id")
    safe_limit = max(1, min(int(limit), 500))
    if not _relation_exists("social.tiktok_sound_posts"):
        return {"season_id": season_id, "sound_id": normalized_sound_id, "posts": []}
    rows = pg.fetch_all(
        """
        select
          sound_id,
          platform_post_id,
          creator_handle,
          posted_at,
          views,
          likes,
          comments,
          shares,
          thumbnail_url,
          caption
        from social.tiktok_sound_posts
        where sound_id = %s
        order by coalesce(views, 0) desc, coalesce(likes, 0) desc, posted_at desc nulls last
        limit %s
        """,
        [normalized_sound_id, safe_limit],
    )
    return {
        "season_id": season_id,
        "sound_id": normalized_sound_id,
        "posts": [
            {
                "platform_post_id": str(row.get("platform_post_id") or ""),
                "creator_handle": str(row.get("creator_handle") or "") or None,
                "posted_at": _iso(_coerce_dt(row.get("posted_at"))),
                "views": int(row.get("views") or 0),
                "likes": int(row.get("likes") or 0),
                "comments": int(row.get("comments") or 0),
                "shares": int(row.get("shares") or 0),
                "thumbnail_url": str(row.get("thumbnail_url") or "") or None,
                "caption": str(row.get("caption") or "") or None,
            }
            for row in rows
        ],
    }


def get_tiktok_post_detail(season_id: str, *, post_id: str) -> dict[str, Any]:
    _sync_core_overrides()
    payload = _room_callable("get_post_comments", get_post_comments)(season_id, platform="tiktok", source_id=post_id)
    cast_rows: list[dict[str, Any]] = []
    if _relation_exists("social.tiktok_post_cast_members"):
        cast_rows = pg.fetch_all(
            """
            select
              cm.cast_member_id::text as cast_member_id,
              cm.cast_member_name,
              cm.confidence,
              cm.source
            from social.tiktok_post_cast_members cm
            join social.tiktok_posts p on p.id = cm.post_id
            where p.season_id = %s::uuid
              and p.video_id = %s
            order by cm.confidence desc nulls last, cm.cast_member_name asc
            """,
            [season_id, post_id],
        )
    payload["cast_members"] = [
        {
            "cast_member_id": str(row.get("cast_member_id") or "") or None,
            "cast_member_name": str(row.get("cast_member_name") or "") or None,
            "confidence": float(row.get("confidence") or 0.0),
            "source": str(row.get("source") or "") or None,
        }
        for row in cast_rows
    ]
    return payload


def get_tiktok_sentiment_trends(
    season_id: str,
    *,
    date_start: datetime | None = None,
    date_end: datetime | None = None,
) -> dict[str, Any]:
    _sync_core_overrides()
    if not _relation_exists("social.tiktok_post_comment_enrichment"):
        return {"season_id": season_id, "timeline": []}
    start_dt = _coerce_dt(date_start)
    end_dt = _coerce_dt(date_end)
    params: list[Any] = [season_id]
    date_sql = ""
    if start_dt:
        date_sql += " and p.posted_at >= %s"
        params.append(start_dt)
    if end_dt:
        date_sql += " and p.posted_at <= %s"
        params.append(end_dt)
    rows = pg.fetch_all(
        f"""
        select
          date_trunc('day', coalesce(p.posted_at, now()))::date as day,
          sum(coalesce(e.positive_count, 0))::bigint as positive_count,
          sum(coalesce(e.neutral_count, 0))::bigint as neutral_count,
          sum(coalesce(e.negative_count, 0))::bigint as negative_count,
          sum(coalesce(e.toxicity_count, 0))::bigint as toxicity_count,
          sum(coalesce(e.cast_mentions_count, 0))::bigint as cast_mentions_count
        from social.tiktok_post_comment_enrichment e
        join social.tiktok_posts p on p.id = e.post_id
        where p.season_id = %s::uuid
          {date_sql}
        group by day
        order by day asc
        """,
        params,
    )
    timeline = []
    for row in rows:
        positive_count = int(row.get("positive_count") or 0)
        neutral_count = int(row.get("neutral_count") or 0)
        negative_count = int(row.get("negative_count") or 0)
        toxicity_count = int(row.get("toxicity_count") or 0)
        total = positive_count + neutral_count + negative_count
        controversy_score = round(((negative_count + toxicity_count) / max(total, 1)) * 100.0, 3) if total > 0 else 0.0
        timeline.append(
            {
                "day": str(row.get("day") or ""),
                "positive_count": positive_count,
                "neutral_count": neutral_count,
                "negative_count": negative_count,
                "toxicity_count": toxicity_count,
                "cast_mentions_count": int(row.get("cast_mentions_count") or 0),
                "controversy_score": controversy_score,
            }
        )
    return {"season_id": season_id, "timeline": timeline}


_LOCAL_ROOM_FUNCTIONS.update({_name: globals()[_name] for _name in _LOCAL_ROOM_NAMES})
__all__ = [
    "get_tiktok_cast_members",
    "get_tiktok_content_health",
    "get_tiktok_hashtags",
    "get_tiktok_overview",
    "get_tiktok_post_detail",
    "get_tiktok_sentiment_trends",
    "get_tiktok_sound_detail",
    "get_tiktok_sound_posts",
    "get_tiktok_sounds",
]
