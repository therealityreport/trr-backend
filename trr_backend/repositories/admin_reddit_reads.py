from __future__ import annotations

import re
import unicodedata
from typing import Any

from trr_backend.db import pg
from trr_backend.repositories import reddit_refresh

COMMUNITIES_TABLE = "admin.reddit_communities"
THREADS_TABLE = "admin.reddit_threads"

CANONICAL_CONTAINER_KEY_RE = re.compile(r"^(episode-\d+|period-preseason|period-postseason)$", re.I)


def _to_int(value: Any, *, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped and re.fullmatch(r"-?\d+", stripped):
            return int(stripped)
    return default


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return False


def _to_string_or_none(value: Any) -> str | None:
    if isinstance(value, str):
        trimmed = value.strip()
        return trimmed or None
    return None


def _to_string_array(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


def _to_object_record(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    return dict(value)


def _slugify_token(value: str | None, fallback: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    normalized = "".join(char for char in normalized if not unicodedata.combining(char))
    normalized = normalized.lower().replace("&", " and ").replace("'", "").replace("’", "")
    normalized = re.sub(r"[^a-z0-9]+", "-", normalized)
    normalized = re.sub(r"-{2,}", "-", normalized).strip("-")
    return normalized or fallback


def _build_detail_slug_base(*, title: str | None, author: str | None) -> str:
    return f"{_slugify_token(title, 'untitled-post')}--u-{_slugify_token(author, 'unknown-author')}"


def _is_canonical_container_key(value: str | None) -> bool:
    return bool(CANONICAL_CONTAINER_KEY_RE.fullmatch(str(value or "").strip().lower()))


def _canonical_reddit_match_container_sql(
    *,
    season_id: str,
    period_key_expr: str,
    period_start_expr: str,
    period_end_expr: str,
    posted_at_expr: str,
) -> str:
    return reddit_refresh._canonical_reddit_match_container_key_sql(  # type: ignore[attr-defined]
        season_id=season_id,
        period_key_expr=period_key_expr,
        period_start_expr=period_start_expr,
        period_end_expr=period_end_expr,
        posted_at_expr=posted_at_expr,
    )


def get_reddit_post_details_by_community_and_season(
    *,
    community_id: str,
    season_id: str,
    reddit_post_id: str,
    comments_limit: int | None = None,
) -> tuple[dict[str, Any] | None, int]:
    normalized_comments_limit = max(25, min(500, int(comments_limit or 250)))
    post_row = pg.fetch_one(
        """
        select
          p.reddit_post_id,
          p.subreddit,
          p.title,
          p.selftext,
          p.url,
          p.permalink,
          p.author,
          p.score,
          p.num_comments,
          p.posted_at::text,
          p.link_flair_text,
          p.canonical_flair_key,
          p.upvote_ratio,
          p.is_self,
          p.post_type,
          p.thumbnail,
          p.content_url,
          p.is_nsfw,
          p.is_spoiler,
          p.author_flair_text,
          p.detail_scraped_at::text,
          p.source_sorts,
          p.media_metadata,
          p.poll_data
        from social.reddit_posts p
        join social.reddit_period_post_matches m
          on m.reddit_post_id = p.reddit_post_id
        where m.community_id = %s::uuid
          and m.season_id = %s::uuid
          and p.reddit_post_id = %s
        order by m.updated_at desc
        limit 1
        """,
        [community_id, season_id, reddit_post_id],
    )
    if post_row is None:
        return None, 1

    matches_rows = pg.fetch_all(
        """
        select
          period_key,
          period_start::text,
          period_end::text,
          is_show_match,
          passes_flair_filter,
          match_score,
          match_type,
          admin_approved,
          flair_mode,
          source_sorts,
          matched_terms,
          matched_cast_terms,
          cross_show_terms,
          link_flair_text,
          canonical_flair_key,
          created_at::text,
          updated_at::text
        from social.reddit_period_post_matches
        where community_id = %s::uuid
          and season_id = %s::uuid
          and reddit_post_id = %s
        order by updated_at desc
        """,
        [community_id, season_id, reddit_post_id],
    )
    comments_rows = pg.fetch_all(
        """
        select
          reddit_comment_id,
          parent_comment_id,
          author,
          body,
          score,
          depth,
          created_at_utc::text,
          author_flair_text,
          is_submitter,
          controversiality,
          ups,
          downs,
          gildings
        from social.reddit_comments
        where reddit_post_id = %s
        order by created_at_utc asc nulls last, depth asc, score desc
        limit %s
        """,
        [reddit_post_id, normalized_comments_limit],
    )
    comment_summary_row = (
        pg.fetch_one(
            """
            select
              count(*)::int as total_comments,
              count(*) filter (where coalesce(depth, 0) = 0)::int as top_level_comments,
              min(created_at_utc)::text as earliest_comment_at,
              max(created_at_utc)::text as latest_comment_at
            from social.reddit_comments
            where reddit_post_id = %s
            """,
            [reddit_post_id],
        )
        or {}
    )
    media_rows = pg.fetch_all(
        """
        select
          id::text,
          reddit_comment_id,
          source_url,
          media_type,
          hosted_url,
          status,
          content_type,
          size_bytes::bigint,
          error_message,
          created_at::text
        from social.reddit_media_mirrors
        where reddit_post_id = %s
        order by created_at desc
        """,
        [reddit_post_id],
    )
    media_summary_row = (
        pg.fetch_one(
            """
            select
              count(*)::int as total_media,
              count(*) filter (where status = 'mirrored')::int as mirrored_media,
              count(*) filter (where status = 'pending')::int as pending_media,
              count(*) filter (where status = 'failed')::int as failed_media
            from social.reddit_media_mirrors
            where reddit_post_id = %s
            """,
            [reddit_post_id],
        )
        or {}
    )
    assigned_threads = pg.fetch_all(
        f"""
        select *
        from {THREADS_TABLE}
        where community_id = %s::uuid
          and reddit_post_id = %s
          and (trr_season_id = %s::uuid or trr_season_id is null)
        order by posted_at desc nulls last, created_at desc
        """,
        [community_id, reddit_post_id, season_id],
    )

    total_comments = _to_int(comment_summary_row.get("total_comments"))
    top_level_comments = _to_int(comment_summary_row.get("top_level_comments"))
    payload = {
        "reddit_post_id": _to_string_or_none(post_row.get("reddit_post_id")) or reddit_post_id,
        "subreddit": _to_string_or_none(post_row.get("subreddit")),
        "title": _to_string_or_none(post_row.get("title")),
        "text": _to_string_or_none(post_row.get("selftext")),
        "url": _to_string_or_none(post_row.get("url")),
        "permalink": _to_string_or_none(post_row.get("permalink")),
        "author": _to_string_or_none(post_row.get("author")),
        "score": _to_int(post_row.get("score")),
        "num_comments": _to_int(post_row.get("num_comments")),
        "posted_at": _to_string_or_none(post_row.get("posted_at")),
        "link_flair_text": _to_string_or_none(post_row.get("link_flair_text")),
        "canonical_flair_key": _to_string_or_none(post_row.get("canonical_flair_key")),
        "upvote_ratio": (
            post_row.get("upvote_ratio") if isinstance(post_row.get("upvote_ratio"), (int, float)) else None
        ),
        "is_self": post_row.get("is_self") if isinstance(post_row.get("is_self"), bool) else None,
        "post_type": _to_string_or_none(post_row.get("post_type")),
        "thumbnail": _to_string_or_none(post_row.get("thumbnail")),
        "content_url": _to_string_or_none(post_row.get("content_url")),
        "is_nsfw": post_row.get("is_nsfw") if isinstance(post_row.get("is_nsfw"), bool) else None,
        "is_spoiler": post_row.get("is_spoiler") if isinstance(post_row.get("is_spoiler"), bool) else None,
        "author_flair_text": _to_string_or_none(post_row.get("author_flair_text")),
        "detail_scraped_at": _to_string_or_none(post_row.get("detail_scraped_at")),
        "source_sorts": _to_string_array(post_row.get("source_sorts")),
        "media_metadata": _to_object_record(post_row.get("media_metadata")),
        "poll_data": _to_object_record(post_row.get("poll_data")),
        "matches": [
            {
                **dict(row),
                "match_score": _to_int(row.get("match_score")),
                "source_sorts": _to_string_array(row.get("source_sorts")),
                "matched_terms": _to_string_array(row.get("matched_terms")),
                "matched_cast_terms": _to_string_array(row.get("matched_cast_terms")),
                "cross_show_terms": _to_string_array(row.get("cross_show_terms")),
            }
            for row in matches_rows
        ],
        "comments": [
            {
                **dict(row),
                "score": _to_int(row.get("score")),
                "depth": _to_int(row.get("depth")),
                "controversiality": (
                    _to_int(row.get("controversiality")) if row.get("controversiality") is not None else None
                ),
                "ups": _to_int(row.get("ups")) if row.get("ups") is not None else None,
                "downs": _to_int(row.get("downs")) if row.get("downs") is not None else None,
                "gildings": _to_object_record(row.get("gildings")),
            }
            for row in comments_rows
        ],
        "comment_summary": {
            "total_comments": total_comments,
            "top_level_comments": top_level_comments,
            "reply_comments": max(0, total_comments - top_level_comments),
            "earliest_comment_at": _to_string_or_none(comment_summary_row.get("earliest_comment_at")),
            "latest_comment_at": _to_string_or_none(comment_summary_row.get("latest_comment_at")),
        },
        "media": [
            {
                **dict(row),
                "size_bytes": _to_int(row.get("size_bytes")) if row.get("size_bytes") is not None else None,
            }
            for row in media_rows
        ],
        "media_summary": {
            "total_media": _to_int(media_summary_row.get("total_media")),
            "mirrored_media": _to_int(media_summary_row.get("mirrored_media")),
            "pending_media": _to_int(media_summary_row.get("pending_media")),
            "failed_media": _to_int(media_summary_row.get("failed_media")),
        },
        "assigned_threads": [dict(row) for row in assigned_threads],
    }
    return payload, 7


def list_reddit_communities(
    *,
    trr_show_id: str | None = None,
    include_inactive: bool = False,
    trr_season_id: str | None = None,
    include_global_threads_for_season: bool = True,
    include_assigned_threads: bool = False,
) -> tuple[dict[str, Any], int]:
    query_count = 1
    if include_assigned_threads:
        rows = pg.fetch_all(
            f"""
            SELECT
              c.*,
              COALESCE(
                json_agg(t ORDER BY t.posted_at DESC NULLS LAST, t.created_at DESC)
                FILTER (WHERE t.id IS NOT NULL),
                '[]'::json
              ) AS assigned_threads,
              COUNT(t.id)::int AS assigned_thread_count
            FROM {COMMUNITIES_TABLE} c
            LEFT JOIN {THREADS_TABLE} t
              ON t.community_id = c.id
             AND (
                %s::uuid IS NULL
                OR t.trr_season_id = %s::uuid
                OR (%s::boolean AND t.trr_season_id IS NULL)
             )
            WHERE (%s::uuid IS NULL OR c.trr_show_id = %s::uuid)
              AND (%s::boolean OR c.is_active = true)
            GROUP BY c.id
            ORDER BY c.trr_show_name ASC, lower(c.subreddit) ASC
            """,
            [
                trr_season_id,
                trr_season_id,
                include_global_threads_for_season,
                trr_show_id,
                trr_show_id,
                include_inactive,
            ],
        )
        communities = [
            {
                **dict(row),
                "assigned_threads": (
                    row.get("assigned_threads") if isinstance(row.get("assigned_threads"), list) else []
                ),
                "assigned_thread_count": _to_int(row.get("assigned_thread_count")),
            }
            for row in rows
        ]
        return {"communities": communities}, query_count

    rows = pg.fetch_all(
        f"""
        SELECT *
          FROM {COMMUNITIES_TABLE}
         WHERE (%s::uuid IS NULL OR trr_show_id = %s::uuid)
           AND (%s::boolean OR is_active = true)
         ORDER BY trr_show_name ASC, lower(subreddit) ASC
        """,
        [trr_show_id, trr_show_id, include_inactive],
    )
    communities = [
        {
            **dict(row),
            "assigned_thread_count": 0,
            "assigned_threads": [],
        }
        for row in rows
    ]
    return {"communities": communities}, query_count


def get_reddit_community_by_id(community_id: str) -> tuple[dict[str, Any] | None, int]:
    row = pg.fetch_one(f"SELECT * FROM {COMMUNITIES_TABLE} WHERE id = %s::uuid LIMIT 1", [community_id])
    return (dict(row) if row else None), 1


def list_reddit_threads(
    *,
    community_id: str | None = None,
    trr_show_id: str | None = None,
    trr_season_id: str | None = None,
    include_global_threads_for_season: bool = True,
) -> tuple[dict[str, Any], int]:
    rows = pg.fetch_all(
        f"""
        SELECT *
          FROM {THREADS_TABLE}
         WHERE (%s::uuid IS NULL OR community_id = %s::uuid)
           AND (%s::uuid IS NULL OR trr_show_id = %s::uuid)
           AND (
             %s::uuid IS NULL
             OR trr_season_id = %s::uuid
             OR (%s::boolean AND trr_season_id IS NULL)
           )
         ORDER BY posted_at DESC NULLS LAST, created_at DESC
        """,
        [
            community_id,
            community_id,
            trr_show_id,
            trr_show_id,
            trr_season_id,
            trr_season_id,
            include_global_threads_for_season,
        ],
    )
    return {"threads": [dict(row) for row in rows]}, 1


def get_reddit_thread_by_id(thread_id: str) -> tuple[dict[str, Any] | None, int]:
    row = pg.fetch_one(f"SELECT * FROM {THREADS_TABLE} WHERE id = %s::uuid LIMIT 1", [thread_id])
    return (dict(row) if row else None), 1


def get_stored_post_counts_by_community_and_season(community_id: str, season_id: str) -> tuple[dict[str, Any], int]:
    canonical_container_sql = _canonical_reddit_match_container_sql(
        season_id=season_id,
        period_key_expr="m.period_key",
        period_start_expr="m.period_start",
        period_end_expr="m.period_end",
        posted_at_expr="p.posted_at",
    )
    counts_rows = pg.fetch_all(
        f"""
        WITH scoped AS (
          SELECT DISTINCT
            m.reddit_post_id,
            {canonical_container_sql} AS container_key
          FROM social.reddit_period_post_matches m
          LEFT JOIN social.reddit_posts p ON p.reddit_post_id = m.reddit_post_id
          WHERE m.community_id = %s::uuid
            AND m.season_id = %s::uuid
            AND m.passes_flair_filter = true
        )
        SELECT
          container_key,
          COUNT(DISTINCT reddit_post_id)::int AS post_count
        FROM scoped
        WHERE container_key <> 'unmapped'
        GROUP BY container_key
        ORDER BY container_key
        """,
        [community_id, season_id],
    )
    counts = {
        str(row.get("container_key")): _to_int(row.get("post_count"))
        for row in counts_rows
        if str(row.get("container_key") or "").strip()
    }
    totals_row = (
        pg.fetch_one(
            """
        SELECT
          COUNT(DISTINCT reddit_post_id)::int AS total_posts,
          COUNT(DISTINCT reddit_post_id) FILTER (WHERE passes_flair_filter = true)::int AS tracked_total_posts
        FROM social.reddit_period_post_matches
        WHERE community_id = %s::uuid
          AND season_id = %s::uuid
        """,
            [community_id, season_id],
        )
        or {}
    )
    tracked_flair_rows = pg.fetch_all(
        f"""
        WITH scoped AS (
          SELECT DISTINCT
            m.reddit_post_id,
            {canonical_container_sql} AS container_key,
            COALESCE(NULLIF(m.canonical_flair_key, ''), NULLIF(p.canonical_flair_key, ''), '') AS flair_key,
            COALESCE(
              NULLIF(TRIM(m.link_flair_text), ''),
              NULLIF(TRIM(p.link_flair_text), ''),
              '(No Flair)'
            ) AS flair_label
          FROM social.reddit_period_post_matches m
          LEFT JOIN social.reddit_posts p ON p.reddit_post_id = m.reddit_post_id
          WHERE m.community_id = %s::uuid
            AND m.season_id = %s::uuid
            AND m.passes_flair_filter = true
        ),
        flair_totals AS (
          SELECT
            flair_key,
            MIN(flair_label) AS flair_label,
            COUNT(DISTINCT reddit_post_id)::int AS post_count
          FROM scoped
          GROUP BY flair_key
        ),
        flair_containers AS (
          SELECT
            flair_key,
            container_key,
            COUNT(DISTINCT reddit_post_id)::int AS container_post_count
          FROM scoped
          WHERE container_key <> 'unmapped'
          GROUP BY flair_key, container_key
        )
        SELECT
          t.flair_key,
          t.flair_label,
          t.post_count,
          c.container_key,
          c.container_post_count
        FROM flair_totals t
        LEFT JOIN flair_containers c ON c.flair_key = t.flair_key
        ORDER BY t.post_count DESC, t.flair_label ASC, c.container_key ASC
        """,
        [community_id, season_id],
    )
    by_flair: dict[str, dict[str, Any]] = {}
    for row in tracked_flair_rows:
        flair_key = str(row.get("flair_key") or "")
        current = by_flair.get(flair_key)
        if current is None:
            current = {
                "flair_key": flair_key,
                "flair_label": row.get("flair_label") or "(No Flair)",
                "post_count": _to_int(row.get("post_count")),
                "container_counts": [],
            }
            by_flair[flair_key] = current
        container_key = str(row.get("container_key") or "").strip()
        if container_key:
            current["container_counts"].append(
                {
                    "container_key": container_key,
                    "post_count": _to_int(row.get("container_post_count")),
                }
            )

    pending_rows = pg.fetch_all(
        f"""
        WITH scoped AS (
          SELECT DISTINCT
            m.reddit_post_id,
            {canonical_container_sql} AS container_key,
            COALESCE(NULLIF(m.canonical_flair_key, ''), NULLIF(p.canonical_flair_key, ''), '') AS flair_key,
            COALESCE(
              NULLIF(TRIM(m.link_flair_text), ''),
              NULLIF(TRIM(p.link_flair_text), ''),
              '(No Flair)'
            ) AS flair_label
          FROM social.reddit_period_post_matches m
          LEFT JOIN social.reddit_posts p ON p.reddit_post_id = m.reddit_post_id
          WHERE m.community_id = %s::uuid
            AND m.season_id = %s::uuid
            AND m.passes_flair_filter = true
        ),
        unassigned AS (
          SELECT s.*
          FROM scoped s
          WHERE s.container_key <> 'unmapped'
            AND NOT EXISTS (
              SELECT 1
              FROM {THREADS_TABLE} t
              WHERE t.community_id = %s::uuid
                AND t.reddit_post_id = s.reddit_post_id
                AND (t.trr_season_id = %s::uuid OR t.trr_season_id IS NULL)
            )
        )
        SELECT
          container_key,
          flair_key,
          MIN(flair_label) AS flair_label,
          COUNT(DISTINCT reddit_post_id)::int AS post_count
        FROM unassigned
        GROUP BY container_key, flair_key
        ORDER BY container_key ASC, post_count DESC, flair_label ASC
        """,
        [community_id, season_id, community_id, season_id],
    )
    pending = [
        {
            "container_key": str(row.get("container_key") or ""),
            "flair_key": str(row.get("flair_key") or ""),
            "flair_label": row.get("flair_label") or "(No Flair)",
            "post_count": _to_int(row.get("post_count")),
        }
        for row in pending_rows
        if str(row.get("container_key") or "").strip()
    ]
    payload = {
        "counts": counts,
        "total_posts": _to_int(totals_row.get("total_posts")),
        "tracked_total_posts": _to_int(totals_row.get("tracked_total_posts")),
        "tracked_flair_counts": list(by_flair.values()),
        "pending_tracked_flair_counts": pending,
        "flair_counts": [
            {"flair": row["flair_label"], "post_count": row["post_count"]} for row in list(by_flair.values())
        ],
    }
    return payload, 4


def get_reddit_community_analytics_summary(
    *,
    community_id: str,
    scope: str,
    season_id: str | None = None,
) -> tuple[dict[str, Any], int]:
    payload = reddit_refresh.get_reddit_community_analytics_summary(
        community_id=community_id,
        scope=scope,
        season_id=season_id,
    )
    return payload, 2


def get_reddit_community_analytics_posts(
    *,
    community_id: str,
    scope: str,
    season_id: str | None = None,
    container_key: str | None = None,
    flair_key: str | None = None,
    page: int = 1,
    per_page: int = 50,
) -> tuple[dict[str, Any], int]:
    payload = reddit_refresh.list_reddit_community_posts(
        community_id=community_id,
        scope=scope,
        season_id=season_id,
        container_key=container_key,
        flair_key=flair_key,
        page=page,
        per_page=per_page,
    )
    return payload, 2


def get_stored_window_posts_by_community_and_season(
    community_id: str,
    season_id: str,
    container_key: str,
    page: int = 1,
    per_page: int = 200,
) -> tuple[dict[str, Any], int]:
    normalized_container_key = str(container_key or "").strip().lower()
    if not _is_canonical_container_key(normalized_container_key):
        raise ValueError("container_key must be a canonical season window key")

    normalized_page = max(1, int(page))
    normalized_per_page = max(1, min(int(per_page), 200))
    offset = (normalized_page - 1) * normalized_per_page
    canonical_container_sql = _canonical_reddit_match_container_sql(
        season_id=season_id,
        period_key_expr="m.period_key",
        period_start_expr="m.period_start",
        period_end_expr="m.period_end",
        posted_at_expr="p.posted_at",
    )

    count_result = (
        pg.fetch_one(
            f"""
        WITH scoped AS (
          SELECT DISTINCT ON (m.reddit_post_id)
            m.reddit_post_id
          FROM social.reddit_period_post_matches m
          JOIN social.reddit_posts p ON p.reddit_post_id = m.reddit_post_id
          WHERE m.community_id = %s::uuid
            AND m.season_id = %s::uuid
            AND m.passes_flair_filter = true
            AND {canonical_container_sql} = %s
          ORDER BY m.reddit_post_id, m.updated_at DESC, p.posted_at DESC NULLS LAST
        )
        SELECT COUNT(*)::int AS total_count
        FROM scoped
        """,
            [community_id, season_id, normalized_container_key],
        )
        or {}
    )

    rows_result = pg.fetch_all(
        f"""
        WITH scoped AS (
          SELECT DISTINCT ON (m.reddit_post_id)
            p.reddit_post_id,
            p.title,
            p.selftext AS text,
            p.url,
            p.permalink,
            p.author,
            p.score,
            p.num_comments,
            p.posted_at::text,
            COALESCE(
              NULLIF(TRIM(m.link_flair_text), ''),
              NULLIF(TRIM(p.link_flair_text), '')
            ) AS link_flair_text,
            m.is_show_match,
            m.match_score
          FROM social.reddit_period_post_matches m
          JOIN social.reddit_posts p ON p.reddit_post_id = m.reddit_post_id
          WHERE m.community_id = %s::uuid
            AND m.season_id = %s::uuid
            AND m.passes_flair_filter = true
            AND {canonical_container_sql} = %s
          ORDER BY m.reddit_post_id, m.updated_at DESC, p.posted_at DESC NULLS LAST
        )
        SELECT
          reddit_post_id,
          title,
          text,
          url,
          permalink,
          author,
          score,
          num_comments,
          posted_at,
          link_flair_text,
          is_show_match,
          match_score
        FROM scoped
        ORDER BY posted_at DESC NULLS LAST, num_comments DESC NULLS LAST, score DESC NULLS LAST
        LIMIT %s
        OFFSET %s
        """,
        [community_id, season_id, normalized_container_key, normalized_per_page, offset],
    )

    total_count = _to_int(count_result.get("total_count"))
    payload = {
        "pagination": {
            "page": normalized_page,
            "per_page": normalized_per_page,
            "total_count": total_count,
        },
        "posts": [
            {
                "reddit_post_id": row.get("reddit_post_id"),
                "title": (row.get("title") or "").strip() or "(Untitled Post)",
                "text": row.get("text"),
                "url": (row.get("url") or "").strip(),
                "permalink": (row.get("permalink") or "").strip() or None,
                "author": (row.get("author") or "").strip() or None,
                "score": max(0, _to_int(row.get("score"))),
                "num_comments": max(0, _to_int(row.get("num_comments"))),
                "posted_at": row.get("posted_at"),
                "link_flair_text": (row.get("link_flair_text") or "").strip() or None,
                "is_show_match": _to_bool(row.get("is_show_match")),
                "passes_flair_filter": True,
                "match_score": row.get("match_score") if isinstance(row.get("match_score"), int) else None,
                "match_type": "flair",
            }
            for row in rows_result
        ],
    }
    return payload, 2


def resolve_reddit_post_detail_by_slug(
    *,
    community_id: str,
    season_id: str,
    container_key: str,
    title_slug: str | None = None,
    author_slug: str | None = None,
    reddit_post_id: str | None = None,
) -> tuple[dict[str, Any] | None, int]:
    canonical_container_sql = _canonical_reddit_match_container_sql(
        season_id=season_id,
        period_key_expr="m.period_key",
        period_start_expr="m.period_start",
        period_end_expr="m.period_end",
        posted_at_expr="p.posted_at",
    )
    rows = pg.fetch_all(
        f"""
        WITH scoped AS (
          SELECT DISTINCT ON (p.reddit_post_id)
            p.reddit_post_id,
            p.title,
            p.author,
            p.posted_at::text,
            p.url,
            p.permalink,
            {canonical_container_sql} AS canonical_container_key
          FROM social.reddit_period_post_matches m
          JOIN social.reddit_posts p ON p.reddit_post_id = m.reddit_post_id
          WHERE m.community_id = %s::uuid
            AND m.season_id = %s::uuid
          ORDER BY p.reddit_post_id, m.updated_at DESC
        )
        SELECT
          reddit_post_id,
          title,
          author,
          posted_at,
          url,
          permalink
        FROM scoped
        WHERE canonical_container_key = %s
        """,
        [community_id, season_id, container_key],
    )
    candidates = [
        {
            **dict(row),
            "slug_base": _build_detail_slug_base(title=row.get("title"), author=row.get("author")),
        }
        for row in rows
    ]
    collisions: dict[str, int] = {}
    for candidate in candidates:
        slug_base = str(candidate["slug_base"])
        collisions[slug_base] = collisions.get(slug_base, 0) + 1

    normalized_post_id = str(reddit_post_id or "").strip()
    requested_base = (
        f"{str(title_slug or '').strip().lower()}--u-{str(author_slug or '').strip().lower()}"
        if str(title_slug or "").strip() and str(author_slug or "").strip()
        else ""
    )
    matched = (
        next((candidate for candidate in candidates if candidate["reddit_post_id"] == normalized_post_id), None)
        if normalized_post_id
        else None
    ) or (
        next(
            (
                candidate
                for candidate in candidates
                if candidate["slug_base"] == requested_base and collisions.get(candidate["slug_base"], 0) == 1
            ),
            None,
        )
        if requested_base
        else None
    )

    if not matched:
        return None, 1

    collision = collisions.get(str(matched["slug_base"]), 0) > 1
    payload = {
        "reddit_post_id": matched["reddit_post_id"],
        "detail_slug": (
            f"{matched['slug_base']}--p-{matched['reddit_post_id']}" if collision else matched["slug_base"]
        ),
        "collision": collision,
        "post": {
            "title": matched["title"],
            "author": matched["author"],
            "posted_at": matched["posted_at"],
            "url": matched["url"],
            "permalink": matched["permalink"],
        },
    }
    return payload, 1
