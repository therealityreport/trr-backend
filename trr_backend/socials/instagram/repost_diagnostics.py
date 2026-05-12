"""Read-only diagnostics for Instagram repost metadata coverage.

This module intentionally does not repair rows. The diagnostics are bounded by
statement timeout and sample limits so operators can inspect source thinness
without opening an unguarded write path.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from collections.abc import Iterable, Mapping
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from trr_backend.db import pg

SOURCE_SHAPE_XDT_LIKE = "xdt-like"
SOURCE_SHAPE_V1_INFO_LIKE = "v1-info-like"
SOURCE_SHAPE_PERMALINK_LIKE = "permalink-like"
SOURCE_SHAPE_COMMENTS_HEADER_LIKE = "comments-header-like"
SOURCE_SHAPE_UNKNOWN = "unknown"

SOURCE_SHAPES = (
    SOURCE_SHAPE_XDT_LIKE,
    SOURCE_SHAPE_V1_INFO_LIKE,
    SOURCE_SHAPE_PERMALINK_LIKE,
    SOURCE_SHAPE_COMMENTS_HEADER_LIKE,
    SOURCE_SHAPE_UNKNOWN,
)

REPOST_ALIAS_MEDIA_REPOST_COUNT = "media_repost_count"
REPOST_ALIAS_REPOST_COUNT = "repostCount"
REPOST_ALIAS_RESHARE_COUNT = "reshareCount"
REPOST_ALIAS_SHARE_COUNT = "shareCount"
REPOST_ALIAS_SOURCE_ABSENT = "source_absent"

REPOST_ALIAS_BUCKETS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (REPOST_ALIAS_MEDIA_REPOST_COUNT, ("media_repost_count",)),
    (REPOST_ALIAS_REPOST_COUNT, ("repostCount", "repost_count")),
    (REPOST_ALIAS_RESHARE_COUNT, ("reshareCount", "reshare_count")),
    (REPOST_ALIAS_SHARE_COUNT, ("shareCount", "share_count")),
)
REPOST_ALIAS_COUNTERS = (
    REPOST_ALIAS_MEDIA_REPOST_COUNT,
    REPOST_ALIAS_REPOST_COUNT,
    REPOST_ALIAS_RESHARE_COUNT,
    REPOST_ALIAS_SHARE_COUNT,
    REPOST_ALIAS_SOURCE_ABSENT,
)

DEFAULT_SAMPLE_LIMIT = 25
MAX_SAMPLE_LIMIT = 250
DEFAULT_STATEMENT_TIMEOUT_MS = 5000
MAX_STATEMENT_TIMEOUT_MS = 30000
DEFAULT_POOL_NAME = "social_profile"

_COMMENT_HEADER_KEYS = {
    "comment_filter_param",
    "has_more_headload_comments",
    "has_more_comments",
    "next_min_id",
    "child_comments",
}
_POST_RICH_KEYS = {
    "code",
    "shortcode",
    "shortCode",
    "permalink",
    "permalink_url",
    "post_url",
    "media_type",
    "image_versions2",
    "video_versions",
    "display_url",
    "displayUrl",
    "thumbnail_url",
    "thumbnailUrl",
    "edge_media_to_caption",
    "edge_sidecar_to_children",
    "carousel_media",
    "caption",
    "owner",
}
_XDT_MEDIA_DICT_KEYS = {
    "original_width",
    "original_height",
    "can_viewer_reshare",
    "clips_metadata",
    "media_overlay_info",
    "coauthor_producers",
    "coauthorProducers",
}
_V1_INFO_KEYS = {
    "items",
    "image_versions2",
    "video_versions",
    "carousel_media",
    "fb_comment_count",
    "fb_like_count",
    "crosspost_metadata",
}
_PERMALINK_KEYS = {
    "og:image",
    "og_image",
    "og:video",
    "og_video",
    "json_ld",
    "thumbnailUrl",
    "displayUrl",
    "ownerUsername",
    "shortCode",
    "permalink_url",
    "post_url",
}

SOURCE_SHAPE_SQL_CASE = """
case
  when p.raw_data is null
    or p.raw_data = '{}'::jsonb
    or jsonb_typeof(p.raw_data) <> 'object'
    then 'unknown'
  when (
      p.raw_data ? 'comment_filter_param'
      or p.raw_data ? 'has_more_headload_comments'
      or p.raw_data ? 'next_min_id'
      or (p.raw_data ? 'comments' and p.raw_data ? 'status')
    )
    and not (
      p.raw_data ? 'code'
      or p.raw_data ? 'shortcode'
      or p.raw_data ? 'shortCode'
      or p.raw_data ? 'media_type'
      or p.raw_data ? 'image_versions2'
      or p.raw_data ? 'video_versions'
      or p.raw_data ? 'displayUrl'
      or p.raw_data ? 'thumbnailUrl'
      or p.raw_data ? 'edge_media_to_caption'
      or p.raw_data ? 'caption'
    )
    then 'comments-header-like'
  when (
      p.raw_data ? 'items'
      or p.raw_data ? 'xdt_api__v1__media__shortcode__web_info'
      or (p.raw_data -> 'data') ? 'xdt_api__v1__media__shortcode__web_info'
    )
    then 'v1-info-like'
  when (
      p.raw_data ? 'xdt_shortcode_media'
      or p.raw_data ? 'xdt_api__v1__feed__user_timeline_graphql_connection'
      or (p.raw_data -> 'data') ? 'xdt_shortcode_media'
      or (p.raw_data -> 'data') ? 'xdt_api__v1__feed__user_timeline_graphql_connection'
      or coalesce(p.raw_data ->> '__typename', '') ilike 'XDT%%'
      or (
        p.raw_data ? 'code'
        and p.raw_data ? 'pk'
        and (
          p.raw_data ? 'original_width'
          or p.raw_data ? 'original_height'
          or p.raw_data ? 'can_viewer_reshare'
          or p.raw_data ? 'clips_metadata'
          or p.raw_data ? 'media_overlay_info'
          or p.raw_data ? 'coauthor_producers'
          or p.raw_data ? 'coauthorProducers'
        )
      )
    )
    then 'xdt-like'
  when (
      p.raw_data ? 'image_versions2'
      or p.raw_data ? 'video_versions'
      or p.raw_data ? 'carousel_media'
      or p.raw_data ? 'fb_comment_count'
      or p.raw_data ? 'fb_like_count'
      or (p.raw_data ? 'media_type' and p.raw_data ? 'pk')
    )
    then 'v1-info-like'
  when (
      p.raw_data ? 'og:image'
      or p.raw_data ? 'og_image'
      or p.raw_data ? 'og:video'
      or p.raw_data ? 'og_video'
      or p.raw_data ? 'json_ld'
      or p.raw_data ? 'thumbnailUrl'
      or p.raw_data ? 'displayUrl'
      or p.raw_data ? 'ownerUsername'
      or p.raw_data ? 'shortCode'
      or p.raw_data ? 'permalink_url'
      or p.raw_data ? 'post_url'
    )
    then 'permalink-like'
  else 'unknown'
end
"""

REPOST_ALIAS_SQL_CASE = """
case
  when p.raw_data ? 'media_repost_count' then 'media_repost_count'
  when p.raw_data ? 'repostCount' or p.raw_data ? 'repost_count' then 'repostCount'
  when p.raw_data ? 'reshareCount' or p.raw_data ? 'reshare_count' then 'reshareCount'
  when p.raw_data ? 'shareCount' or p.raw_data ? 'share_count' then 'shareCount'
  else 'source_absent'
end
"""


def _safe_percent(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round((max(0, numerator) * 100.0) / denominator, 1)


def _safe_int(value: Any) -> int:
    if value is None or isinstance(value, bool):
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _safe_float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, int | float):
        return float(value)
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _safe_sample_limit(limit: int | None) -> int:
    if limit is None:
        return DEFAULT_SAMPLE_LIMIT
    return max(0, min(int(limit), MAX_SAMPLE_LIMIT))


def _safe_statement_timeout_ms(statement_timeout_ms: int | None) -> int:
    if statement_timeout_ms is None:
        return DEFAULT_STATEMENT_TIMEOUT_MS
    return max(0, min(int(statement_timeout_ms), MAX_STATEMENT_TIMEOUT_MS))


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _has_any_key(payload: Mapping[str, Any], keys: set[str] | tuple[str, ...]) -> bool:
    return any(key in payload for key in keys)


def _iter_nested_mappings(value: Any) -> Iterable[Mapping[str, Any]]:
    stack = [value]
    while stack:
        current = stack.pop()
        if isinstance(current, Mapping):
            yield current
            stack.extend(current.values())
        elif isinstance(current, list | tuple):
            stack.extend(current)


def _has_nested_key(payload: Mapping[str, Any], key: str) -> bool:
    return any(key in item for item in _iter_nested_mappings(payload))


def _has_nested_key_prefix(payload: Mapping[str, Any], prefix: str) -> bool:
    return any(any(str(key).startswith(prefix) for key in item) for item in _iter_nested_mappings(payload))


def _is_comments_header_like(payload: Mapping[str, Any]) -> bool:
    has_comment_header = _has_any_key(payload, _COMMENT_HEADER_KEYS) or (
        isinstance(payload.get("comments"), list) and "status" in payload
    )
    if not has_comment_header:
        return False
    return not _has_any_key(payload, _POST_RICH_KEYS)


def _is_v1_info_wrapper_like(payload: Mapping[str, Any]) -> bool:
    if _has_nested_key(payload, "xdt_api__v1__media__shortcode__web_info"):
        return True
    if isinstance(payload.get("items"), list):
        return True
    return False


def _is_v1_info_like(payload: Mapping[str, Any]) -> bool:
    if _has_any_key(payload, _V1_INFO_KEYS):
        return True
    return "media_type" in payload and "pk" in payload


def _is_xdt_like(payload: Mapping[str, Any]) -> bool:
    typename = str(payload.get("__typename") or "")
    if typename.startswith("XDT"):
        return True
    if _has_nested_key(payload, "xdt_shortcode_media"):
        return True
    if _has_nested_key(payload, "xdt_api__v1__feed__user_timeline_graphql_connection"):
        return True
    if _has_nested_key_prefix(payload, "xdt_") and not _has_nested_key(
        payload,
        "xdt_api__v1__media__shortcode__web_info",
    ):
        return True
    return "code" in payload and "pk" in payload and _has_any_key(payload, _XDT_MEDIA_DICT_KEYS)


def _is_permalink_like(payload: Mapping[str, Any]) -> bool:
    if _has_any_key(payload, _PERMALINK_KEYS):
        return True
    url_value = str(payload.get("url") or payload.get("permalink") or "").strip().lower()
    return "instagram.com/" in url_value and not _has_any_key(payload, _POST_RICH_KEYS)


def classify_raw_data_source_shape(raw_data: Mapping[str, Any] | None) -> str:
    """Classify an Instagram ``raw_data`` object using stable key fingerprints."""

    payload = _as_mapping(raw_data)
    if not payload:
        return SOURCE_SHAPE_UNKNOWN
    if _is_comments_header_like(payload):
        return SOURCE_SHAPE_COMMENTS_HEADER_LIKE
    if _is_v1_info_wrapper_like(payload):
        return SOURCE_SHAPE_V1_INFO_LIKE
    if _is_xdt_like(payload):
        return SOURCE_SHAPE_XDT_LIKE
    if _is_v1_info_like(payload):
        return SOURCE_SHAPE_V1_INFO_LIKE
    if _is_permalink_like(payload):
        return SOURCE_SHAPE_PERMALINK_LIKE
    return SOURCE_SHAPE_UNKNOWN


def detect_repost_alias(raw_data: Mapping[str, Any] | None) -> str:
    """Return the first repost source alias present in ``raw_data`` or ``source_absent``."""

    payload = _as_mapping(raw_data)
    if not payload:
        return REPOST_ALIAS_SOURCE_ABSENT
    for item in _iter_nested_mappings(payload):
        for bucket, aliases in REPOST_ALIAS_BUCKETS:
            if any(alias in item for alias in aliases):
                return bucket
    return REPOST_ALIAS_SOURCE_ABSENT


def _row_media_repost_populated(row: Mapping[str, Any]) -> bool:
    return row.get("media_repost_count") is not None


def build_repost_coverage(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    materialized_rows = list(rows)
    total = len(materialized_rows)
    populated = sum(1 for row in materialized_rows if _row_media_repost_populated(row))
    return {
        "total_instagram_posts": total,
        "posts_with_media_repost_count": populated,
        "percent_populated": _safe_percent(populated, total),
    }


def _ordered_counter_payload(counter: Counter[str], ordered_keys: tuple[str, ...]) -> dict[str, int]:
    return {key: int(counter.get(key) or 0) for key in ordered_keys}


def summarize_repost_diagnostics(
    rows: Iterable[Mapping[str, Any]],
    *,
    sample_limit: int | None = DEFAULT_SAMPLE_LIMIT,
) -> dict[str, Any]:
    materialized_rows = list(rows)
    safe_limit = _safe_sample_limit(sample_limit)
    source_shape_counts: Counter[str] = Counter()
    alias_counts: Counter[str] = Counter()
    alias_counts_by_shape: dict[str, Counter[str]] = {}
    thin_gap_samples: list[dict[str, Any]] = []
    thin_gap_count = 0

    for row in materialized_rows:
        raw_data = _as_mapping(row.get("raw_data"))
        source_shape = classify_raw_data_source_shape(raw_data)
        alias = detect_repost_alias(raw_data)
        source_shape_counts[source_shape] += 1
        alias_counts[alias] += 1
        alias_counts_by_shape.setdefault(source_shape, Counter())[alias] += 1

        if source_shape == SOURCE_SHAPE_COMMENTS_HEADER_LIKE and not _row_media_repost_populated(row):
            thin_gap_count += 1
            if len(thin_gap_samples) < safe_limit:
                thin_gap_samples.append(
                    {
                        "id": row.get("id"),
                        "shortcode": row.get("shortcode"),
                        "source_account": row.get("source_account"),
                        "username": row.get("username"),
                    }
                )

    histogram = [
        {"source_shape": shape, "rows": int(source_shape_counts.get(shape) or 0)}
        for shape in SOURCE_SHAPES
        if source_shape_counts.get(shape)
    ]
    return {
        "coverage": build_repost_coverage(materialized_rows),
        "source_shape_histogram": histogram,
        "repost_alias_counters": _ordered_counter_payload(alias_counts, REPOST_ALIAS_COUNTERS),
        "repost_alias_counters_by_source_shape": {
            shape: _ordered_counter_payload(alias_counts_by_shape.get(shape, Counter()), REPOST_ALIAS_COUNTERS)
            for shape in SOURCE_SHAPES
            if source_shape_counts.get(shape)
        },
        "thin_source_repost_gaps": {
            "comments_header_like_without_media_repost_count": thin_gap_count,
            "sample_limit": safe_limit,
            "samples": thin_gap_samples,
        },
    }


def _fetch_all_readonly(
    query: str,
    params: Iterable[Any] | None = None,
    *,
    conn: Any | None = None,
    pool_name: str = DEFAULT_POOL_NAME,
    statement_timeout_ms: int | None = DEFAULT_STATEMENT_TIMEOUT_MS,
) -> list[dict[str, Any]]:
    safe_timeout = _safe_statement_timeout_ms(statement_timeout_ms)
    if safe_timeout <= 0:
        return pg.fetch_all(query, params or [], conn=conn, pool_name=pool_name)
    if conn is not None:
        with pg.db_cursor(conn=conn, label="instagram-repost-diagnostics") as cur:
            cur.execute("set local statement_timeout = %s", [str(safe_timeout)])
            return pg.fetch_all_with_cursor(cur, query, params or [])
    with pg.db_connection(label="instagram-repost-diagnostics", pool_name=pool_name) as managed_conn:
        with pg.db_cursor(conn=managed_conn, label="instagram-repost-diagnostics") as cur:
            cur.execute("set local statement_timeout = %s", [str(safe_timeout)])
            return pg.fetch_all_with_cursor(cur, query, params or [])


def _fetch_one_readonly(
    query: str,
    params: Iterable[Any] | None = None,
    *,
    conn: Any | None = None,
    pool_name: str = DEFAULT_POOL_NAME,
    statement_timeout_ms: int | None = DEFAULT_STATEMENT_TIMEOUT_MS,
) -> dict[str, Any] | None:
    rows = _fetch_all_readonly(
        query,
        params,
        conn=conn,
        pool_name=pool_name,
        statement_timeout_ms=statement_timeout_ms,
    )
    return rows[0] if rows else None


def get_repost_coverage(
    *,
    conn: Any | None = None,
    pool_name: str = DEFAULT_POOL_NAME,
    statement_timeout_ms: int | None = DEFAULT_STATEMENT_TIMEOUT_MS,
) -> dict[str, Any]:
    """Return total Instagram posts, repost-populated posts, and populated percent."""

    row = (
        _fetch_one_readonly(
            """
        select
          count(*)::int as total_instagram_posts,
          count(*) filter (where media_repost_count is not null)::int as posts_with_media_repost_count,
          case
            when count(*) = 0 then null
            else round(
              (count(*) filter (where media_repost_count is not null) * 100.0) / count(*),
              1
            )
          end as percent_populated
        from social.instagram_posts
        """,
            [],
            conn=conn,
            pool_name=pool_name,
            statement_timeout_ms=statement_timeout_ms,
        )
        or {}
    )
    total = _safe_int(row.get("total_instagram_posts"))
    populated = _safe_int(row.get("posts_with_media_repost_count"))
    percent = _safe_float_or_none(row.get("percent_populated"))
    if percent is None:
        percent = _safe_percent(populated, total)
    return {
        "total_instagram_posts": total,
        "posts_with_media_repost_count": populated,
        "percent_populated": percent,
    }


def get_source_shape_histogram(
    *,
    conn: Any | None = None,
    pool_name: str = DEFAULT_POOL_NAME,
    statement_timeout_ms: int | None = DEFAULT_STATEMENT_TIMEOUT_MS,
) -> list[dict[str, Any]]:
    """Return a read-only histogram of Instagram ``raw_data`` source shapes."""

    rows = _fetch_all_readonly(
        f"""
        with classified as (
          select {SOURCE_SHAPE_SQL_CASE} as source_shape
          from social.instagram_posts p
        )
        select source_shape, count(*)::int as rows
        from classified
        group by source_shape
        order by
          case source_shape
            when 'xdt-like' then 1
            when 'v1-info-like' then 2
            when 'permalink-like' then 3
            when 'comments-header-like' then 4
            else 5
          end,
          source_shape
        """,
        [],
        conn=conn,
        pool_name=pool_name,
        statement_timeout_ms=statement_timeout_ms,
    )
    return [
        {
            "source_shape": str(row.get("source_shape") or SOURCE_SHAPE_UNKNOWN),
            "rows": _safe_int(row.get("rows")),
        }
        for row in rows
    ]


def get_repost_alias_counters(
    *,
    conn: Any | None = None,
    pool_name: str = DEFAULT_POOL_NAME,
    statement_timeout_ms: int | None = DEFAULT_STATEMENT_TIMEOUT_MS,
) -> dict[str, Any]:
    """Return repost source alias counters overall and split by source shape."""

    rows = _fetch_all_readonly(
        f"""
        with classified as (
          select
            {SOURCE_SHAPE_SQL_CASE} as source_shape,
            {REPOST_ALIAS_SQL_CASE} as repost_alias
          from social.instagram_posts p
        )
        select source_shape, repost_alias, count(*)::int as rows
        from classified
        group by source_shape, repost_alias
        order by source_shape, repost_alias
        """,
        [],
        conn=conn,
        pool_name=pool_name,
        statement_timeout_ms=statement_timeout_ms,
    )
    totals: Counter[str] = Counter()
    by_shape: dict[str, Counter[str]] = {}
    for row in rows:
        source_shape = str(row.get("source_shape") or SOURCE_SHAPE_UNKNOWN)
        alias = str(row.get("repost_alias") or REPOST_ALIAS_SOURCE_ABSENT)
        count = _safe_int(row.get("rows"))
        totals[alias] += count
        by_shape.setdefault(source_shape, Counter())[alias] += count
    return {
        "totals": _ordered_counter_payload(totals, REPOST_ALIAS_COUNTERS),
        "by_source_shape": {
            shape: _ordered_counter_payload(counter, REPOST_ALIAS_COUNTERS)
            for shape, counter in sorted(by_shape.items())
        },
    }


def get_thin_source_repost_gaps(
    *,
    sample_limit: int | None = DEFAULT_SAMPLE_LIMIT,
    conn: Any | None = None,
    pool_name: str = DEFAULT_POOL_NAME,
    statement_timeout_ms: int | None = DEFAULT_STATEMENT_TIMEOUT_MS,
) -> dict[str, Any]:
    """Return bounded samples for comments-header-like rows missing repost metadata."""

    safe_limit = _safe_sample_limit(sample_limit)
    count_row = (
        _fetch_one_readonly(
            f"""
        with classified as (
          select media_repost_count, {SOURCE_SHAPE_SQL_CASE} as source_shape
          from social.instagram_posts p
        )
        select count(*)::int as comments_header_like_without_media_repost_count
        from classified
        where source_shape = 'comments-header-like'
          and media_repost_count is null
        """,
            [],
            conn=conn,
            pool_name=pool_name,
            statement_timeout_ms=statement_timeout_ms,
        )
        or {}
    )
    sample_rows = (
        _fetch_all_readonly(
            f"""
            with classified as (
              select
                p.id::text as id,
                p.shortcode,
                p.source_account,
                p.username,
                p.media_repost_count,
                {SOURCE_SHAPE_SQL_CASE} as source_shape
              from social.instagram_posts p
            )
            select id, shortcode, source_account, username
            from classified
            where source_shape = 'comments-header-like'
              and media_repost_count is null
            order by id
            limit %s
            """,
            [safe_limit],
            conn=conn,
            pool_name=pool_name,
            statement_timeout_ms=statement_timeout_ms,
        )
        if safe_limit > 0
        else []
    )
    return {
        "comments_header_like_without_media_repost_count": _safe_int(
            count_row.get("comments_header_like_without_media_repost_count")
        ),
        "sample_limit": safe_limit,
        "samples": [
            {
                "id": row.get("id"),
                "shortcode": row.get("shortcode"),
                "source_account": row.get("source_account"),
                "username": row.get("username"),
            }
            for row in sample_rows
        ],
    }


def build_repost_diagnostics_report(
    *,
    sample_limit: int | None = DEFAULT_SAMPLE_LIMIT,
    conn: Any | None = None,
    pool_name: str = DEFAULT_POOL_NAME,
    statement_timeout_ms: int | None = DEFAULT_STATEMENT_TIMEOUT_MS,
) -> dict[str, Any]:
    """Build the complete read-only diagnostics payload."""

    safe_timeout = _safe_statement_timeout_ms(statement_timeout_ms)
    return {
        "diagnostic": "instagram_repost_metadata",
        "mode": "read_only",
        "write_repair": {
            "implemented": False,
            "reason": "repair writes are intentionally out of scope for this diagnostics slice",
            "required_future_counters": {
                "attempted": 0,
                "updated": 0,
                "skipped_thin_source": 0,
                "failed": 0,
                "rate_limited": 0,
            },
        },
        "bounds": {
            "sample_limit": _safe_sample_limit(sample_limit),
            "statement_timeout_ms": safe_timeout,
            "pool_name": pool_name,
        },
        "coverage": get_repost_coverage(
            conn=conn,
            pool_name=pool_name,
            statement_timeout_ms=safe_timeout,
        ),
        "source_shape_histogram": get_source_shape_histogram(
            conn=conn,
            pool_name=pool_name,
            statement_timeout_ms=safe_timeout,
        ),
        "repost_alias_counters": get_repost_alias_counters(
            conn=conn,
            pool_name=pool_name,
            statement_timeout_ms=safe_timeout,
        ),
        "thin_source_repost_gaps": get_thin_source_repost_gaps(
            sample_limit=sample_limit,
            conn=conn,
            pool_name=pool_name,
            statement_timeout_ms=safe_timeout,
        ),
    }


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_safe(item) for item in value]
    return str(value)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="instagram_repost_diagnostics",
        description="Read-only Instagram repost metadata coverage and source-shape diagnostics.",
    )
    parser.add_argument("--sample-limit", type=int, default=DEFAULT_SAMPLE_LIMIT)
    parser.add_argument("--statement-timeout-ms", type=int, default=DEFAULT_STATEMENT_TIMEOUT_MS)
    parser.add_argument("--pool-name", default=DEFAULT_POOL_NAME)
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        from trr_backend.utils.env import load_env

        load_env()
    except Exception:
        # The helper is useful in already-configured runtimes too.
        pass
    args = parse_args(argv if argv is not None else sys.argv[1:])
    payload = build_repost_diagnostics_report(
        sample_limit=args.sample_limit,
        pool_name=args.pool_name,
        statement_timeout_ms=args.statement_timeout_ms,
    )
    print(json.dumps(_json_safe(payload), indent=2 if args.pretty else None, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
