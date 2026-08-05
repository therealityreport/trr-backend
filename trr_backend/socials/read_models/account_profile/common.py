# ruff: noqa: F821
"""Profile read models for social account catalog pages."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from trr_backend.socials.provider_registry import LateNamespaceProvider, publish_mapping_slot
from trr_backend.socials.read_models.account_profile.comment_breakdown import (
    build_instagram_comment_breakdown,
    instagram_comment_completeness_from_breakdown,
    instagram_facebook_comment_count_from_row,
    instagram_facebook_crosspost_payload_from_row,
)

_IMPORTED_CORE_NAMES: set[str] = set()
_LOCAL_ROOM_NAMES: set[str] = set()
_LOCAL_ROOM_FUNCTIONS: dict[str, Any] = {}
_CORE_ROOM_WRAPPERS: dict[str, Any] = {}


def _unconfigured_social_account_profile_post_item(*_args: Any, **_kwargs: Any) -> Any:
    raise RuntimeError(
        "ACCOUNT_PROFILE_PROVIDER_UNCONFIGURED: "
        "trr_backend.socials.social_season_analytics_impl has not finished loading"
)
_CORE_SOCIAL_ACCOUNT_PROFILE_POST_ITEM = _unconfigured_social_account_profile_post_item
_SOCIAL_ACCOUNT_PROFILE_COMMENT_SORT_FIELDS = {"user", "comment", "likes", "replies", "created"}
_SOCIAL_ACCOUNT_PROFILE_COMMENT_SORT_DIRECTIONS = {"asc", "desc"}
_SOCIAL_ACCOUNT_PROFILE_DEFAULT_PAGE_SIZE = 25


def _instagram_owner_account_match_sql(*, alias: str = "p") -> str:
    provider = _require_provider_ready()
    return provider["_instagram_owner_account_match_sql"](alias=alias)
_LOCAL_ROOM_NAMES.add("_instagram_owner_account_match_sql")


def _publish_provider_binding(name: str, value: Any) -> None:
    globals()[name] = value
_OPTIONAL_PROVIDER_ROOM_WRAPPERS = {
    "instagram_comment_rollup_health",
    "rebuild_instagram_post_comment_rollups",
}
_PROVIDER = LateNamespaceProvider(
    globals(),
    prefix="ACCOUNT_PROFILE_PROVIDER",
    room_names=_LOCAL_ROOM_NAMES,
    imported_names=_IMPORTED_CORE_NAMES,
    room_wrappers=_CORE_ROOM_WRAPPERS,
    required_room_names=lambda: _LOCAL_ROOM_NAMES - _OPTIONAL_PROVIDER_ROOM_WRAPPERS,
    publisher=lambda name, value: _publish_provider_binding(name, value),
    commit=publish_mapping_slot(globals(), "_CORE_SOCIAL_ACCOUNT_PROFILE_POST_ITEM", "_social_account_profile_post_item"),  # noqa: E501
    unconfigured_message="ACCOUNT_PROFILE_PROVIDER_UNCONFIGURED: trr_backend.socials.social_season_analytics_impl has not finished loading",  # noqa: E501
    mismatch_message=(
        "ACCOUNT_PROFILE_PROVIDER_MISMATCH: "
        "account-profile provider is already configured with a different mapping"
    ),
)
_require_provider_ready = _PROVIDER.require
_configure_legacy_provider = _PROVIDER.configure
_sync_core_overrides = _PROVIDER.sync
_room_callable = _PROVIDER.room_callable


def _normalize_social_account_profile_comment_sort_by(value: str | None) -> str:
    normalized = str(value or "created").strip().lower()
    if normalized not in _SOCIAL_ACCOUNT_PROFILE_COMMENT_SORT_FIELDS:
        allowed = ", ".join(sorted(_SOCIAL_ACCOUNT_PROFILE_COMMENT_SORT_FIELDS))
        raise ValueError(f"INVALID_COMMENT_SORT_BY: sort_by must be one of {allowed}")
    return normalized


def _normalize_social_account_profile_comment_sort_dir(value: str | None) -> str:
    normalized = str(value or "desc").strip().lower()
    if normalized not in _SOCIAL_ACCOUNT_PROFILE_COMMENT_SORT_DIRECTIONS:
        raise ValueError("INVALID_COMMENT_SORT_DIR: sort_dir must be one of asc, desc")
    return normalized


def _comment_search_condition_sql(
    search: str | None,
    *,
    comment_alias: str = "c",
    post_alias: str = "p",
) -> tuple[str, list[Any]]:
    normalized_search = str(search or "").strip().lower()
    if not normalized_search:
        return "", []
    pattern = f"%{normalized_search}%"
    return (
        f"""
        (
          lower(coalesce({comment_alias}.text, '')) like %s
          or lower(coalesce({comment_alias}.username, '')) like %s
          or lower(coalesce({comment_alias}.user_id, '')) like %s
          or lower(coalesce({comment_alias}.comment_id, '')) like %s
          or lower(coalesce(to_jsonb({comment_alias}) ->> 'author_full_name', '')) like %s
          or lower(coalesce({post_alias}.shortcode, '')) like %s
        )
        """,
        [pattern, pattern, pattern, pattern, pattern, pattern],
    )


def _comment_search_sql_parts(search: str | None) -> tuple[str, list[Any]]:
    condition_sql, params = _comment_search_condition_sql(search)
    if not condition_sql:
        return "", []
    return f"and {condition_sql}", params


def _instagram_profile_posts_page_search_where_sql(search: str | None, *, alias: str = "d") -> tuple[str, list[Any]]:
    normalized_search = str(search or "").strip().lower()
    if not normalized_search:
        return "", []
    normalized_handle_sql = (
        "nullif("
        "regexp_replace("
        "lower(regexp_replace(coalesce(__TERM_VALUE__, ''), '^@+', '')), "
        "'[^a-z0-9._-]+', '', 'g'"
        "), '')"
    )
    normalized_hashtag_sql = (
        "nullif("
        "regexp_replace("
        "lower(regexp_replace(coalesce(__TERM_VALUE__, ''), '^#+', '')), "
        "'[^a-z0-9]+', '', 'g'"
        "), '')"
    )
    handle_exprs = (f"{alias}.mentions", f"{alias}.collaborators", f"{alias}.profile_tags")
    hashtag_exprs = (f"{alias}.hashtags",)

    if normalized_search.startswith("@"):
        exact_handle = _normalize_social_account_profile_handle_term(normalized_search).lstrip("@").lower()
        if not exact_handle:
            return "and false", []
        clause = _social_account_profile_exact_jsonb_term_sql(handle_exprs, term_sql=normalized_handle_sql)
        return f"and ({clause})", [exact_handle] * len(handle_exprs)

    if normalized_search.startswith("#"):
        exact_hashtag = _normalize_social_account_profile_hashtag_term(normalized_search).lower()
        if not exact_hashtag:
            return "and false", []
        clause = _social_account_profile_exact_jsonb_term_sql(hashtag_exprs, term_sql=normalized_hashtag_sql)
        return f"and ({clause})", [exact_hashtag] * len(hashtag_exprs)

    text_expr = (
        f"lower(concat_ws(' ', coalesce({alias}.source_id, ''), coalesce({alias}.shortcode, ''), "
        f"coalesce({alias}.title, ''), coalesce({alias}.caption, ''), coalesce({alias}.description, ''), "
        f"coalesce({alias}.text, ''), coalesce({alias}.show_name, ''), coalesce({alias}.permalink, '')))"
    )
    hashtag_clause = _social_account_profile_exact_jsonb_term_sql(hashtag_exprs, term_sql=normalized_hashtag_sql)
    handle_clause = _social_account_profile_exact_jsonb_term_sql(handle_exprs, term_sql=normalized_handle_sql)
    clauses = [f"{text_expr} like %s", hashtag_clause, handle_clause]
    params: list[Any] = [f"%{normalized_search}%"]
    params.extend([normalized_search] * len(hashtag_exprs))
    params.extend([normalized_search] * len(handle_exprs))
    return f"and ({' or '.join(clauses)})", params


def _comments_order_by_sql(sort_by: str | None, sort_dir: str | None) -> str:
    normalized_sort_by = _normalize_social_account_profile_comment_sort_by(sort_by)
    normalized_sort_dir = _normalize_social_account_profile_comment_sort_dir(sort_dir)
    direction = "asc" if normalized_sort_dir == "asc" else "desc"
    sort_expressions = {
        "user": "lower(coalesce(nullif(to_jsonb(c) ->> 'author_full_name', ''), c.username, c.user_id, ''))",
        "comment": "lower(coalesce(c.text, ''))",
        "likes": "coalesce(c.likes, 0)",
        "replies": "coalesce(c.reply_count, 0)",
        "created": "c.created_at",
    }
    primary = sort_expressions[normalized_sort_by]
    if normalized_sort_by == "created":
        return f"{primary} {direction} nulls last, c.id {'asc' if direction == 'asc' else 'desc'}"
    return f"{primary} {direction} nulls last, c.created_at desc nulls last, c.id desc"


def _instagram_post_comment_rollups_available(*, conn: Any | None = None) -> bool:
    return _relation_exists("social.instagram_post_comment_rollups", conn=conn)


def _tiktok_post_comment_rollups_available(*, conn: Any | None = None) -> bool:
    return _relation_exists("social.tiktok_post_comment_rollups", conn=conn)


def _youtube_post_comment_rollups_available(*, conn: Any | None = None) -> bool:
    return _relation_exists("social.youtube_post_comment_rollups", conn=conn)


def _instagram_saved_comment_counts_cte_sql(*, source_cte: str, active_condition: str, use_rollup: bool) -> str:
    if use_rollup:
        return f"""
        saved_comment_counts as materialized (
          select
            r.post_id::text as profile_row_id,
            r.active_comment_count::int as saved_comments
          from social.instagram_post_comment_rollups r
          join {source_cte} d
            on d.profile_row_id is not null
           and r.post_id = d.profile_row_id::uuid
        )
        """
    return f"""
        saved_comment_counts as materialized (
          select
            c.post_id::text as profile_row_id,
            count(*) filter (where {active_condition})::int as saved_comments
          from social.instagram_comments c
          join {source_cte} d
            on d.profile_row_id is not null
           and c.post_id = d.profile_row_id::uuid
          group by c.post_id
        )
        """


def _fetch_materialized_comments_only_profile_rows_page(
    platform: str,
    account_handle: str,
    *,
    page: int,
    page_size: int,
    search: str | None = None,
    comment_filter: str | None = None,
    sort_by: str | None = None,
    sort_dir: str | None = None,
    conn: Any | None = None,
) -> tuple[list[dict[str, Any]], int]:
    normalized_platform = _normalize_social_account_profile_platform(platform)
    if normalized_platform not in {"tiktok", "youtube"}:
        return _require_provider_ready()[
            "_fetch_materialized_comments_only_profile_rows_page"
        ](
            platform,
            account_handle,
            page=page,
            page_size=page_size,
            search=search,
            comment_filter=comment_filter,
            sort_by=sort_by,
            sort_dir=sort_dir,
            conn=conn,
        )

    table, source_id_column, posted_at_column = _social_account_profile_base_query_parts(normalized_platform)
    comment_table = f"{normalized_platform}_comments"
    comment_fk_col = "post_id" if normalized_platform == "tiktok" else "video_id"
    rollup_table = f"social.{normalized_platform}_post_comment_rollups"
    rollup_fk_col = "post_id" if normalized_platform == "tiktok" else "video_id"
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    safe_page = max(1, int(page))
    safe_page_size = max(1, min(int(page_size), _SOCIAL_ACCOUNT_PROFILE_MAX_PAGE_SIZE))
    safe_offset = (safe_page - 1) * safe_page_size
    posted_at_projection = f"p.{posted_at_column} as posted_at," if posted_at_column != "posted_at" else ""
    order_by_sql = _comments_only_profile_order_by_sql(
        sort_by=sort_by,
        sort_dir=sort_dir,
    )
    use_rollup = (
        _tiktok_post_comment_rollups_available(conn=conn)
        if normalized_platform == "tiktok"
        else _youtube_post_comment_rollups_available(conn=conn)
    )
    base_rows_sql = f"""
        with base_rows as (
          select
            p.id::text as profile_row_id,
            p.show_id::text as show_id,
            p.season_id::text as season_id,
            p.source_account,
            p.{source_id_column} as source_id,
            {posted_at_projection}
            s.season_number,
            sh.name as show_name,
            sh.slug as show_slug,
            p.*
          from social.{table} p
          left join core.seasons s on s.id = p.season_id
          left join core.shows sh on sh.id = coalesce(p.show_id, s.show_id)
          where {_social_account_profile_owner_match_sql(normalized_platform, alias="p")}
        )
    """
    if use_rollup:
        saved_comment_counts_sql = f"""
            saved_comment_counts as (
              select
                r.{rollup_fk_col}::text as join_key,
                coalesce(r.active_comment_count, 0)::int as saved_comments
              from {rollup_table} r
              join base_rows p on p.profile_row_id = r.{rollup_fk_col}::text
            )
        """
    else:
        active_filter = (
            "and coalesce(c.is_missing, false) = false"
            if _comment_lifecycle_supported(comment_table, conn=conn)
            else ""
        )
        saved_comment_counts_sql = f"""
            saved_comment_counts as (
              select
                c.{comment_fk_col}::text as join_key,
                count(*)::int as saved_comments
              from social.{comment_table} c
              join base_rows p on p.profile_row_id = c.{comment_fk_col}::text
              where 1 = 1
                {active_filter}
              group by c.{comment_fk_col}
            )
        """
    saved_counts_sql = f"""
        {base_rows_sql},
        {saved_comment_counts_sql},
        filtered_rows as (
          select
            base_rows.*,
            coalesce(saved_comment_counts.saved_comments, 0)::int as saved_comments
          from base_rows
          left join saved_comment_counts on saved_comment_counts.join_key = base_rows.profile_row_id
          where greatest(
            coalesce(base_rows.comments_count, 0),
            coalesce(saved_comment_counts.saved_comments, 0)
          ) > 0
        )
    """
    total_sql = f"""
        {saved_counts_sql}
        select count(*)::int as total
        from filtered_rows
    """
    page_sql = f"""
        {saved_counts_sql}
        select *
        from filtered_rows
        order by {order_by_sql}
        limit %s offset %s
    """
    total_params: list[Any] = [normalized_account]
    page_params: list[Any] = [normalized_account, safe_page_size, safe_offset]
    if conn is None:
        total_row = pg.fetch_one(total_sql, total_params) or {}
        rows = pg.fetch_all(page_sql, page_params)
    else:
        with pg.db_cursor(conn=conn, label=f"{normalized_platform}_comments_only_profile_total") as cur:
            total_row = pg.fetch_one_with_cursor(cur, total_sql, total_params) or {}
        with pg.db_cursor(conn=conn, label=f"{normalized_platform}_comments_only_profile_rows") as cur:
            rows = pg.fetch_all_with_cursor(cur, page_sql, page_params)
    return rows, _normalize_non_negative_int(total_row.get("total"))


def rebuild_instagram_post_comment_rollups(
    *,
    account_handle: str | None = None,
    post_ids: list[str] | tuple[str, ...] | None = None,
    limit: int | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Refresh persisted Instagram comment-count rollups for selected posts."""
    _sync_core_overrides()
    normalized_post_ids = [str(post_id).strip() for post_id in (post_ids or []) if str(post_id).strip()]
    normalized_account = _normalize_social_account_profile_handle(account_handle) if account_handle else None
    safe_limit = int(limit) if limit is not None else None
    if safe_limit is not None and safe_limit <= 0:
        raise ValueError("limit must be greater than 0")

    where_sql = "p.id in (select distinct c.post_id from social.instagram_comments c where c.post_id is not null)"
    params: list[Any] = []
    if normalized_post_ids:
        where_sql = "p.id = any(%s::uuid[])"
        params.append(normalized_post_ids)
    elif normalized_account:
        where_sql = _social_account_profile_owner_match_sql("instagram", alias="p")
        params.append(normalized_account)

    limit_sql = ""
    if safe_limit is not None:
        limit_sql = "limit %s"
        params.append(safe_limit)

    target_sql = f"""
        select p.id
        from social.instagram_posts p
        where {where_sql}
        order by p.id
        {limit_sql}
    """
    with pg.db_connection(label="instagram-comment-rollup-rebuild") as conn:
        if not _instagram_post_comment_rollups_available(conn=conn):
            raise RuntimeError("social.instagram_post_comment_rollups is missing; run the migration first")
        with pg.db_cursor(conn=conn, label="instagram_comment_rollup_rebuild_targets") as cur:
            cur.execute(f"select count(*)::int as target_count from ({target_sql}) targets", params)
            count_row = cur.fetchone() or {}
            target_count = int(count_row.get("target_count") or 0)
        if dry_run or target_count == 0:
            return {
                "ok": True,
                "dry_run": bool(dry_run),
                "target_count": target_count,
                "refreshed_count": 0,
                "account_handle": normalized_account,
                "post_ids": normalized_post_ids,
                "limit": safe_limit,
            }
        with pg.db_cursor(conn=conn, label="instagram_comment_rollup_rebuild") as cur:
            cur.execute(
                f"""
                with target_posts as materialized (
                  {target_sql}
                )
                select social.refresh_instagram_post_comment_rollup(id) as refreshed
                from target_posts
                """,
                params,
            )
            refreshed_count = len(cur.fetchall())
    return {
        "ok": True,
        "dry_run": False,
        "target_count": target_count,
        "refreshed_count": refreshed_count,
        "account_handle": normalized_account,
        "post_ids": normalized_post_ids,
        "limit": safe_limit,
    }


def instagram_comment_rollup_health(*, sample_limit: int = 25) -> dict[str, Any]:
    """Return exact rollup/count mismatch status for operator health checks."""
    _sync_core_overrides()
    safe_sample_limit = max(1, min(int(sample_limit), 100))
    with pg.db_read_connection(label="instagram-comment-rollup-health", pool_name="health") as conn:
        if not _instagram_post_comment_rollups_available(conn=conn):
            return {
                "status": "unavailable",
                "reason": "rollup_table_missing",
                "rollup_table": "social.instagram_post_comment_rollups",
                "sample_limit": safe_sample_limit,
            }
        with pg.db_cursor(conn=conn, label="instagram_comment_rollup_health") as cur:
            cur.execute(
                """
                with comment_counts as materialized (
                  select
                    c.post_id,
                    count(*) filter (where coalesce(c.is_missing, false) = false)::int as active_comment_count,
                    count(*) filter (where coalesce(c.is_missing, false) = true)::int as missing_comment_count,
                    count(*)::int as total_comment_count
                  from social.instagram_comments c
                  where c.post_id is not null
                  group by c.post_id
                ),
                compared as materialized (
                  select
                    coalesce(r.post_id, cc.post_id) as post_id,
                    coalesce(cc.active_comment_count, 0)::int as expected_active_comment_count,
                    coalesce(r.active_comment_count, 0)::int as rollup_active_comment_count,
                    coalesce(cc.missing_comment_count, 0)::int as expected_missing_comment_count,
                    coalesce(r.missing_comment_count, 0)::int as rollup_missing_comment_count,
                    coalesce(cc.total_comment_count, 0)::int as expected_total_comment_count,
                    coalesce(r.total_comment_count, 0)::int as rollup_total_comment_count,
                    r.updated_at as rollup_updated_at,
                    (r.post_id is null) as missing_rollup,
                    (cc.post_id is null) as rollup_without_comments
                  from comment_counts cc
                  full join social.instagram_post_comment_rollups r
                    on r.post_id = cc.post_id
                ),
                mismatches as materialized (
                  select *
                  from compared
                  where
                    missing_rollup
                    or expected_active_comment_count <> rollup_active_comment_count
                    or expected_missing_comment_count <> rollup_missing_comment_count
                    or expected_total_comment_count <> rollup_total_comment_count
                    or (rollup_without_comments and (
                      rollup_active_comment_count <> 0
                      or rollup_missing_comment_count <> 0
                      or rollup_total_comment_count <> 0
                    ))
                ),
                mismatch_sample as (
                  select *
                  from mismatches
                  order by post_id
                  limit %s
                )
                select
                  (select count(*)::int from social.instagram_post_comment_rollups) as rollup_rows,
                  (select count(*)::int from comment_counts) as comment_post_rows,
                  (select coalesce(sum(total_comment_count), 0)::int from comment_counts) as comment_rows,
                  (select count(*)::int from mismatches) as mismatch_count,
                  coalesce(
                    (select jsonb_agg(to_jsonb(mismatch_sample)) from mismatch_sample),
                    '[]'::jsonb
                  ) as mismatch_sample
                """,
                [safe_sample_limit],
            )
            row = dict(cur.fetchone() or {})
    mismatch_count = int(row.get("mismatch_count") or 0)
    return {
        "status": "healthy" if mismatch_count == 0 else "stale",
        "reason": "ok" if mismatch_count == 0 else "rollup_count_mismatch",
        "rollup_table": "social.instagram_post_comment_rollups",
        "rollup_rows": int(row.get("rollup_rows") or 0),
        "comment_post_rows": int(row.get("comment_post_rows") or 0),
        "comment_rows": int(row.get("comment_rows") or 0),
        "mismatch_count": mismatch_count,
        "sample_limit": safe_sample_limit,
        "mismatch_sample": row.get("mismatch_sample") or [],
    }


def _fetch_instagram_profile_rows_page(
    account_handle: str,
    *,
    page: int,
    page_size: int,
    search: str | None = None,
    sort_by: str | None = None,
    sort_dir: str | None = None,
    conn: Any | None = None,
    _payload_mode_override: str | None = None,
) -> tuple[list[dict[str, Any]], int]:
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    safe_page = max(1, int(page))
    safe_page_size = max(1, min(int(page_size), _SOCIAL_ACCOUNT_PROFILE_MAX_PAGE_SIZE))
    safe_offset = (safe_page - 1) * safe_page_size
    owner_match_clause = _instagram_owner_account_match_sql(alias="p")
    reported_comments_expr = _instagram_reported_comments_sql("p")
    catalog_reported_comments_expr = _instagram_reported_comments_sql("p")
    search_where_sql, search_params = _instagram_profile_posts_page_search_where_sql(search, alias="filtered_rows")
    order_by_sql = _comments_only_profile_order_by_sql(
        sort_by=sort_by,
        sort_dir=sort_dir,
        missing_comments_sql="greatest(coalesce(filtered_rows.missing_comments, 0), 0)",
    )
    page_order_by_sql = order_by_sql.replace("filtered_rows.", "page_rows.")
    lifecycle_supported = _comment_lifecycle_supported("instagram_comments", conn=conn)
    active_condition = "c.is_missing is not true" if lifecycle_supported else "true"
    use_comment_rollups = _instagram_post_comment_rollups_available(conn=conn)
    collaborator_rows_available = _instagram_catalog_collaborator_membership_available(conn=conn)
    from trr_backend.socials.instagram import payload_sidecars

    payload_mode = _payload_mode_override or payload_sidecars.payload_read_mode()
    sidecar_join, sidecar_projection = _instagram_payload_sidecar_sql(
        row_kind="mixed", row_alias="page_rows", mode=payload_mode
    )
    collaborator_rows_sql = (
        f"""
        collaborator_rows as materialized (
          select
            p.id::text as id,
            materialized_post.id::text as profile_row_id,
            p.assigned_show_id::text as show_id,
            p.assigned_season_id::text as season_id,
            p.source_account,
            p.source_id as source_id,
            p.source_id as shortcode,
            p.posted_at,
            s.season_number,
            sh.name as show_name,
            sh.slug as show_slug,
            p.title,
            p.caption,
            p.description,
            p.text,
            p.media_type,
            coalesce(p.media_urls, '[]'::jsonb) as media_urls,
            nullif(p.thumbnail_url, '') as thumbnail_url,
            coalesce(p.media_urls, '[]'::jsonb) as source_media_urls,
            coalesce(
              nullif(p.hosted_media_urls, '[]'::jsonb),
              nullif(p.raw_data -> 'hosted_media_urls', '[]'::jsonb),
              '[]'::jsonb
            ) as hosted_media_urls,
            nullif(p.thumbnail_url, '') as source_thumbnail_url,
            coalesce(nullif(p.hosted_thumbnail_url, ''), nullif(p.raw_data ->> 'hosted_thumbnail_url', ''))
              as hosted_thumbnail_url,
            coalesce(nullif(p.post_format, ''), nullif(p.raw_data ->> 'post_format', '')) as post_format,
            coalesce(p.hashtags, '[]'::jsonb) as hashtags,
            coalesce(p.mentions, '[]'::jsonb) as mentions,
            coalesce(p.collaborators, '[]'::jsonb) as collaborators,
            coalesce(
              nullif(p.collaborators_detail, '[]'::jsonb),
              nullif(p.raw_data -> 'collaborators_detail', '[]'::jsonb),
              '[]'::jsonb
            )
              as collaborators_detail,
            coalesce(p.profile_tags, '[]'::jsonb) as profile_tags,
            coalesce(p.likes, 0)::bigint as likes,
            {catalog_reported_comments_expr}::bigint as comments_count,
            null::integer as fb_comment_count,
            null::timestamptz as facebook_crosspost_observed_at,
            null::text as facebook_crosspost_source,
            null::text as facebook_post_id,
            null::text as facebook_post_url,
            coalesce(p.views, 0)::bigint as views,
            null::bigint as media_repost_count,
            coalesce(p.shares, 0)::bigint as shares,
            coalesce(p.retweets, 0)::bigint as retweets,
            coalesce(p.replies_count, 0)::bigint as replies_count,
            coalesce(p.quotes, 0)::bigint as quotes,
            coalesce(p.raw_data, '{{}}'::jsonb) as raw_data,
            p.permalink,
            'catalog'::text as _profile_source_surface,
            'collaborator'::text as _profile_match_mode,
            1::int as _profile_dataset_priority
          from social.instagram_account_catalog_post_collaborators m
          join social.instagram_account_catalog_posts p
            on p.id = m.catalog_post_id
          left join social.instagram_posts materialized_post
            on materialized_post.shortcode = p.source_id
          left join core.seasons s on s.id = p.assigned_season_id
          left join core.shows sh on sh.id = coalesce(p.assigned_show_id, s.show_id)
          where m.collaborator_handle = %s
            and lower(p.source_account) <> %s
            and nullif(p.source_id, '') is not null
        )
        """
        if collaborator_rows_available
        else """
        collaborator_rows as materialized (
          select *
          from owner_rows
          where false
        )
        """
    )
    candidate_rows_sql = f"""
        with owner_rows as materialized (
          select
            p.id::text as id,
            p.id::text as profile_row_id,
            p.show_id::text as show_id,
            p.season_id::text as season_id,
            p.source_account,
            p.shortcode as source_id,
            p.shortcode as shortcode,
            p.posted_at,
            s.season_number,
            sh.name as show_name,
            sh.slug as show_slug,
            null::text as title,
            p.caption,
            null::text as description,
            null::text as text,
            p.media_type,
            coalesce(p.media_urls, '[]'::jsonb) as media_urls,
            nullif(p.thumbnail_url, '') as thumbnail_url,
            coalesce(p.media_urls, '[]'::jsonb) as source_media_urls,
            coalesce(p.hosted_media_urls, '[]'::jsonb) as hosted_media_urls,
            nullif(p.thumbnail_url, '') as source_thumbnail_url,
            nullif(coalesce(p.hosted_thumbnail_url, ''), '') as hosted_thumbnail_url,
            nullif(coalesce(p.post_format, ''), '') as post_format,
            coalesce(p.hashtags, '[]'::jsonb) as hashtags,
            coalesce(p.mentions, '[]'::jsonb) as mentions,
            coalesce(p.collaborators, '[]'::jsonb) as collaborators,
            coalesce(p.collaborators_detail, '[]'::jsonb) as collaborators_detail,
            coalesce(p.profile_tags, '[]'::jsonb) as profile_tags,
            coalesce(p.likes, 0)::bigint as likes,
            {reported_comments_expr}::bigint as comments_count,
            p.fb_comment_count,
            p.facebook_crosspost_observed_at,
            p.facebook_crosspost_source,
            p.facebook_post_id,
            p.facebook_post_url,
            coalesce(p.views, 0)::bigint as views,
            p.media_repost_count::bigint as media_repost_count,
            coalesce(p.media_repost_count, 0)::bigint as shares,
            0::bigint as retweets,
            0::bigint as replies_count,
            0::bigint as quotes,
            coalesce(p.raw_data, '{{}}'::jsonb) as raw_data,
            null::text as permalink,
            'materialized'::text as _profile_source_surface,
            'owner'::text as _profile_match_mode,
            3::int as _profile_dataset_priority
          from social.instagram_posts p
          left join core.seasons s on s.id = p.season_id
          left join core.shows sh on sh.id = coalesce(p.show_id, s.show_id)
          where {owner_match_clause}
            and nullif(p.shortcode, '') is not null
          union all
          select
            p.id::text as id,
            materialized_post.id::text as profile_row_id,
            p.assigned_show_id::text as show_id,
            p.assigned_season_id::text as season_id,
            p.source_account,
            p.source_id as source_id,
            p.source_id as shortcode,
            p.posted_at,
            s.season_number,
            sh.name as show_name,
            sh.slug as show_slug,
            p.title,
            p.caption,
            p.description,
            p.text,
            p.media_type,
            coalesce(p.media_urls, '[]'::jsonb) as media_urls,
            nullif(p.thumbnail_url, '') as thumbnail_url,
            coalesce(p.media_urls, '[]'::jsonb) as source_media_urls,
            coalesce(
              nullif(p.hosted_media_urls, '[]'::jsonb),
              nullif(p.raw_data -> 'hosted_media_urls', '[]'::jsonb),
              '[]'::jsonb
            ) as hosted_media_urls,
            nullif(p.thumbnail_url, '') as source_thumbnail_url,
            coalesce(nullif(p.hosted_thumbnail_url, ''), nullif(p.raw_data ->> 'hosted_thumbnail_url', ''))
              as hosted_thumbnail_url,
            coalesce(nullif(p.post_format, ''), nullif(p.raw_data ->> 'post_format', '')) as post_format,
            coalesce(p.hashtags, '[]'::jsonb) as hashtags,
            coalesce(p.mentions, '[]'::jsonb) as mentions,
            coalesce(p.collaborators, '[]'::jsonb) as collaborators,
            coalesce(
              nullif(p.collaborators_detail, '[]'::jsonb),
              nullif(p.raw_data -> 'collaborators_detail', '[]'::jsonb),
              '[]'::jsonb
            )
              as collaborators_detail,
            coalesce(p.profile_tags, '[]'::jsonb) as profile_tags,
            coalesce(p.likes, 0)::bigint as likes,
            {catalog_reported_comments_expr}::bigint as comments_count,
            null::integer as fb_comment_count,
            null::timestamptz as facebook_crosspost_observed_at,
            null::text as facebook_crosspost_source,
            null::text as facebook_post_id,
            null::text as facebook_post_url,
            coalesce(p.views, 0)::bigint as views,
            null::bigint as media_repost_count,
            coalesce(p.shares, 0)::bigint as shares,
            coalesce(p.retweets, 0)::bigint as retweets,
            coalesce(p.replies_count, 0)::bigint as replies_count,
            coalesce(p.quotes, 0)::bigint as quotes,
            coalesce(p.raw_data, '{{}}'::jsonb) as raw_data,
            p.permalink,
            'catalog'::text as _profile_source_surface,
            'owner'::text as _profile_match_mode,
            2::int as _profile_dataset_priority
          from social.instagram_account_catalog_posts p
          left join social.instagram_posts materialized_post
            on materialized_post.shortcode = p.source_id
          left join core.seasons s on s.id = p.assigned_season_id
          left join core.shows sh on sh.id = coalesce(p.assigned_show_id, s.show_id)
          where lower(p.source_account) = %s
            and nullif(p.source_id, '') is not null
        ),
        {collaborator_rows_sql},
        deduped_rows as materialized (
          select distinct on (source_id)
            *
          from (
            select * from owner_rows
            union all
            select * from collaborator_rows
          ) candidate_rows
          order by
            source_id,
            _profile_dataset_priority desc,
            posted_at desc nulls last,
            id desc
        ),
        {
        _instagram_saved_comment_counts_cte_sql(
            source_cte="deduped_rows",
            active_condition=active_condition,
            use_rollup=use_comment_rollups,
        )
    },
        filtered_rows as materialized (
          select
            d.*,
            coalesce(saved_comment_counts.saved_comments, 0)::int as saved_comments,
            greatest(
              coalesce(d.comments_count, 0) - coalesce(saved_comment_counts.saved_comments, 0),
              0
            )::int as missing_comments
          from deduped_rows d
          left join saved_comment_counts
            on saved_comment_counts.profile_row_id = d.profile_row_id
          where 1 = 1
            {search_where_sql}
        )
    """
    params: list[Any] = [normalized_account, normalized_account]
    if collaborator_rows_available:
        params.extend([normalized_account, normalized_account])
    params.extend(search_params)
    page_sql = f"""
        {candidate_rows_sql},
        page_rows as materialized (
          select *, count(*) over()::int as _total_count
          from filtered_rows
          order by {order_by_sql}
          limit %s offset %s
        )
        select page_rows.* {sidecar_projection}
        from page_rows
        {sidecar_join}
        order by {page_order_by_sql}
    """
    try:
        if conn is None:
            rows = pg.fetch_all(page_sql, [*params, safe_page_size, safe_offset])
        else:
            with pg.db_cursor(conn=conn, label="instagram_profile_posts_rows") as cur:
                rows = pg.fetch_all_with_cursor(cur, page_sql, [*params, safe_page_size, safe_offset])
    except psycopg_errors.UndefinedTable:
        if payload_mode == "legacy":
            raise
        _log_instagram_payload_schema_unavailable(
            surface="instagram.profile.search",
            entity_identity=normalized_account,
        )
        return _fetch_instagram_profile_rows_page(
            account_handle,
            page=page,
            page_size=page_size,
            search=search,
            sort_by=sort_by,
            sort_dir=sort_dir,
            conn=conn,
            _payload_mode_override="legacy",
        )
    if conn is None:
        total_row = {"total": rows[0].get("_total_count")} if rows and rows[0].get("_total_count") is not None else {}
        if not total_row:
            total_sql = f"""
                {candidate_rows_sql}
                select count(*)::int as total
                from filtered_rows
            """
            total_row = pg.fetch_one(total_sql, params) or {}
    else:
        total_row = {"total": rows[0].get("_total_count")} if rows and rows[0].get("_total_count") is not None else {}
        if not total_row:
            total_sql = f"""
                {candidate_rows_sql}
                select count(*)::int as total
                from filtered_rows
            """
            with pg.db_cursor(conn=conn, label="instagram_profile_posts_total") as cur:
                total_row = pg.fetch_one_with_cursor(cur, total_sql, params) or {}
    rows = _instagram_payload_rows_for_read(
        rows,
        row_kind="mixed",
        mode=payload_mode,
        surface="instagram.profile.search",
    )
    return rows, _normalize_non_negative_int(total_row.get("total"))


def _fetch_instagram_profile_rows_page_no_search(
    account_handle: str,
    *,
    page: int,
    page_size: int,
    sort_by: str | None = None,
    sort_dir: str | None = None,
    conn: Any | None = None,
    _payload_mode_override: str | None = None,
) -> tuple[list[dict[str, Any]], int]:
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    safe_page = max(1, int(page))
    safe_page_size = max(1, min(int(page_size), _SOCIAL_ACCOUNT_PROFILE_MAX_PAGE_SIZE))
    safe_offset = (safe_page - 1) * safe_page_size
    normalized_sort_by = _normalize_social_account_profile_post_sort_by(sort_by)
    normalized_sort_dir = _normalize_social_account_profile_post_sort_dir(sort_dir)
    owner_match_clause = _instagram_owner_account_match_sql(alias="p")
    reported_comments_expr = _instagram_reported_comments_sql("p")
    order_by_sql = _comments_only_profile_order_by_sql(
        sort_by=sort_by,
        sort_dir=sort_dir,
        missing_comments_sql="greatest(coalesce(filtered_rows.missing_comments, 0), 0)",
    )
    lifecycle_supported = _comment_lifecycle_supported("instagram_comments", conn=conn)
    active_condition = "c.is_missing is not true" if lifecycle_supported else "true"
    use_comment_rollups = _instagram_post_comment_rollups_available(conn=conn)
    limit_missing_score_window = (
        normalized_sort_by == "missing_comments" and normalized_sort_dir == "desc" and not use_comment_rollups
    )
    score_candidate_limit = safe_offset + safe_page_size
    collaborator_rows_available = _instagram_catalog_collaborator_membership_available(conn=conn)
    from trr_backend.socials.instagram import payload_sidecars

    payload_mode = _payload_mode_override or payload_sidecars.payload_read_mode()
    post_sidecar_join, post_sidecar_projection = _instagram_payload_sidecar_sql(
        row_kind="post", row_alias="p", mode=payload_mode
    )
    catalog_sidecar_join, catalog_sidecar_projection = _instagram_payload_sidecar_sql(
        row_kind="catalog", row_alias="p", mode=payload_mode
    )
    collaborator_rows_sql = (
        """
        collaborator_rows as materialized (
          select
            'catalog'::text as _row_kind,
            p.id::text as _row_id,
            p.id::text as id,
            materialized_post.id::text as profile_row_id,
            p.source_id as source_id,
            p.posted_at,
            p.title,
            p.caption,
            p.description,
            p.text,
            coalesce(p.likes, 0)::bigint as likes,
            greatest(coalesce(p.comments_count, 0), 0)::bigint as comments_count,
            'catalog'::text as _profile_source_surface,
            'collaborator'::text as _profile_match_mode,
            1::int as _profile_dataset_priority
          from social.instagram_account_catalog_post_collaborators m
          join social.instagram_account_catalog_posts p
            on p.id = m.catalog_post_id
          left join social.instagram_posts materialized_post
            on materialized_post.shortcode = p.source_id
          where m.collaborator_handle = %s
            and lower(p.source_account) <> %s
            and nullif(p.source_id, '') is not null
        )
        """
        if collaborator_rows_available
        else """
        collaborator_rows as materialized (
          select *
          from owner_rows
          where false
        )
        """
    )
    score_source_rows_sql = (
        """
        score_source_rows as materialized (
          select *
          from deduped_rows
          order by
            comments_count desc nulls last,
            posted_at desc nulls last,
            _row_id desc
          limit %s
        )
        """
        if limit_missing_score_window
        else """
        score_source_rows as materialized (
          select *
          from deduped_rows
        )
        """
    )
    scored_rows_sql = f"""
        with owner_rows as materialized (
          select
            'materialized'::text as _row_kind,
            p.id::text as _row_id,
            p.id::text as id,
            p.id::text as profile_row_id,
            p.shortcode as source_id,
            p.posted_at,
            null::text as title,
            p.caption,
            null::text as description,
            null::text as text,
            coalesce(p.likes, 0)::bigint as likes,
            {reported_comments_expr}::bigint as comments_count,
            'materialized'::text as _profile_source_surface,
            'owner'::text as _profile_match_mode,
            3::int as _profile_dataset_priority
          from social.instagram_posts p
          where {owner_match_clause}
            and nullif(p.shortcode, '') is not null
          union all
          select
            'catalog'::text as _row_kind,
            p.id::text as _row_id,
            p.id::text as id,
            materialized_post.id::text as profile_row_id,
            p.source_id as source_id,
            p.posted_at,
            p.title,
            p.caption,
            p.description,
            p.text,
            coalesce(p.likes, 0)::bigint as likes,
            greatest(coalesce(p.comments_count, 0), 0)::bigint as comments_count,
            'catalog'::text as _profile_source_surface,
            'owner'::text as _profile_match_mode,
            2::int as _profile_dataset_priority
          from social.instagram_account_catalog_posts p
          left join social.instagram_posts materialized_post
            on materialized_post.shortcode = p.source_id
          where lower(p.source_account) = %s
            and nullif(p.source_id, '') is not null
        ),
        {collaborator_rows_sql},
        deduped_rows as materialized (
          select distinct on (source_id)
            *
          from (
            select * from owner_rows
            union all
            select * from collaborator_rows
          ) candidate_rows
          order by
            source_id,
            _profile_dataset_priority desc,
            posted_at desc nulls last,
            _row_id desc
        ),
        total_count as materialized (
          select count(*)::int as total
          from deduped_rows
        ),
        {score_source_rows_sql},
        {
        _instagram_saved_comment_counts_cte_sql(
            source_cte="score_source_rows",
            active_condition=active_condition,
            use_rollup=use_comment_rollups,
        )
    },
        filtered_rows as materialized (
          select
            d.*,
            coalesce(saved_comment_counts.saved_comments, 0)::int as saved_comments,
            greatest(
              coalesce(d.comments_count, 0) - coalesce(saved_comment_counts.saved_comments, 0),
              0
            )::int as missing_comments
          from score_source_rows d
          left join saved_comment_counts
            on saved_comment_counts.profile_row_id = d.profile_row_id
        )
    """
    params: list[Any] = [normalized_account, normalized_account]
    if collaborator_rows_available:
        params.extend([normalized_account, normalized_account])
    if limit_missing_score_window:
        params.append(score_candidate_limit)
    page_keys_sql = f"""
        {scored_rows_sql},
        page_keys as materialized (
          select
            row_number() over (order by {order_by_sql})::int as _page_rank,
            filtered_rows.*,
            (select total from total_count) as _total_count
          from filtered_rows
          order by {order_by_sql}
          limit %s offset %s
        )
        select
          page_keys._page_rank,
          p.id::text as id,
          p.id::text as profile_row_id,
          p.show_id::text as show_id,
          p.season_id::text as season_id,
          p.source_account,
          p.shortcode as source_id,
          p.shortcode as shortcode,
          p.posted_at,
          s.season_number,
          sh.name as show_name,
          sh.slug as show_slug,
          null::text as title,
          p.caption,
          null::text as description,
          null::text as text,
          p.media_type,
          coalesce(p.media_urls, '[]'::jsonb) as media_urls,
          nullif(p.thumbnail_url, '') as thumbnail_url,
          coalesce(p.media_urls, '[]'::jsonb) as source_media_urls,
          coalesce(p.hosted_media_urls, '[]'::jsonb) as hosted_media_urls,
          nullif(p.thumbnail_url, '') as source_thumbnail_url,
          nullif(coalesce(p.hosted_thumbnail_url, ''), '') as hosted_thumbnail_url,
          nullif(coalesce(p.post_format, ''), '') as post_format,
          coalesce(p.hashtags, '[]'::jsonb) as hashtags,
          coalesce(p.mentions, '[]'::jsonb) as mentions,
          coalesce(p.collaborators, '[]'::jsonb) as collaborators,
          coalesce(p.collaborators_detail, '[]'::jsonb) as collaborators_detail,
          coalesce(p.profile_tags, '[]'::jsonb) as profile_tags,
          coalesce(p.likes, 0)::bigint as likes,
          {reported_comments_expr}::bigint as comments_count,
          p.fb_comment_count,
          p.facebook_crosspost_observed_at,
          p.facebook_crosspost_source,
          p.facebook_post_id,
          p.facebook_post_url,
          coalesce(p.views, 0)::bigint as views,
          p.media_repost_count::bigint as media_repost_count,
          coalesce(p.media_repost_count, 0)::bigint as shares,
          0::bigint as retweets,
          0::bigint as replies_count,
          0::bigint as quotes,
          coalesce(p.raw_data, '{{}}'::jsonb) as raw_data,
          null::text as permalink,
          page_keys.saved_comments,
          page_keys.missing_comments,
          page_keys._profile_source_surface,
          page_keys._profile_match_mode,
          page_keys._profile_dataset_priority,
          page_keys._total_count
          {post_sidecar_projection}
        from page_keys
        join social.instagram_posts p
          on page_keys._row_kind = 'materialized'
         and p.id::text = page_keys._row_id
        {post_sidecar_join}
        left join core.seasons s on s.id = p.season_id
        left join core.shows sh on sh.id = coalesce(p.show_id, s.show_id)
        union all
        select
          page_keys._page_rank,
          p.id::text as id,
          materialized_post.id::text as profile_row_id,
          p.assigned_show_id::text as show_id,
          p.assigned_season_id::text as season_id,
          p.source_account,
          p.source_id as source_id,
          p.source_id as shortcode,
          p.posted_at,
          s.season_number,
          sh.name as show_name,
          sh.slug as show_slug,
          p.title,
          p.caption,
          p.description,
          p.text,
          p.media_type,
          coalesce(p.media_urls, '[]'::jsonb) as media_urls,
          nullif(p.thumbnail_url, '') as thumbnail_url,
          coalesce(p.media_urls, '[]'::jsonb) as source_media_urls,
          coalesce(
            nullif(p.hosted_media_urls, '[]'::jsonb),
            nullif(p.raw_data -> 'hosted_media_urls', '[]'::jsonb),
            '[]'::jsonb
          ) as hosted_media_urls,
          nullif(p.thumbnail_url, '') as source_thumbnail_url,
          coalesce(nullif(p.hosted_thumbnail_url, ''), nullif(p.raw_data ->> 'hosted_thumbnail_url', ''))
            as hosted_thumbnail_url,
          coalesce(nullif(p.post_format, ''), nullif(p.raw_data ->> 'post_format', '')) as post_format,
          coalesce(p.hashtags, '[]'::jsonb) as hashtags,
          coalesce(p.mentions, '[]'::jsonb) as mentions,
          coalesce(p.collaborators, '[]'::jsonb) as collaborators,
          coalesce(
            nullif(p.collaborators_detail, '[]'::jsonb),
            nullif(p.raw_data -> 'collaborators_detail', '[]'::jsonb),
            '[]'::jsonb
          )
            as collaborators_detail,
          coalesce(p.profile_tags, '[]'::jsonb) as profile_tags,
          coalesce(p.likes, 0)::bigint as likes,
          greatest(coalesce(p.comments_count, 0), 0)::bigint as comments_count,
          null::integer as fb_comment_count,
          null::timestamptz as facebook_crosspost_observed_at,
          null::text as facebook_crosspost_source,
          null::text as facebook_post_id,
          null::text as facebook_post_url,
          coalesce(p.views, 0)::bigint as views,
          null::bigint as media_repost_count,
          coalesce(p.shares, 0)::bigint as shares,
          coalesce(p.retweets, 0)::bigint as retweets,
          coalesce(p.replies_count, 0)::bigint as replies_count,
          coalesce(p.quotes, 0)::bigint as quotes,
          coalesce(p.raw_data, '{{}}'::jsonb) as raw_data,
          p.permalink,
          page_keys.saved_comments,
          page_keys.missing_comments,
          page_keys._profile_source_surface,
          page_keys._profile_match_mode,
          page_keys._profile_dataset_priority,
          page_keys._total_count
          {catalog_sidecar_projection}
        from page_keys
        join social.instagram_account_catalog_posts p
          on page_keys._row_kind = 'catalog'
         and p.id::text = page_keys._row_id
        {catalog_sidecar_join}
        left join social.instagram_posts materialized_post
          on materialized_post.shortcode = p.source_id
        left join core.seasons s on s.id = p.assigned_season_id
        left join core.shows sh on sh.id = coalesce(p.assigned_show_id, s.show_id)
        order by _page_rank asc
    """
    try:
        if conn is None:
            rows = pg.fetch_all(page_keys_sql, [*params, safe_page_size, safe_offset])
        else:
            with pg.db_cursor(conn=conn, label="instagram_profile_posts_rows") as cur:
                rows = pg.fetch_all_with_cursor(cur, page_keys_sql, [*params, safe_page_size, safe_offset])
    except psycopg_errors.UndefinedTable:
        if payload_mode == "legacy":
            raise
        _log_instagram_payload_schema_unavailable(
            surface="instagram.profile.normal",
            entity_identity=normalized_account,
        )
        return _fetch_instagram_profile_rows_page_no_search(
            account_handle,
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            sort_dir=sort_dir,
            conn=conn,
            _payload_mode_override="legacy",
        )
    if conn is None:
        total_row = {"total": rows[0].get("_total_count")} if rows and rows[0].get("_total_count") is not None else {}
        if not total_row:
            total_sql = f"""
                {scored_rows_sql}
                select total
                from total_count
            """
            total_row = pg.fetch_one(total_sql, params) or {}
    else:
        total_row = {"total": rows[0].get("_total_count")} if rows and rows[0].get("_total_count") is not None else {}
        if not total_row:
            total_sql = f"""
                {scored_rows_sql}
                select total
                from total_count
            """
            with pg.db_cursor(conn=conn, label="instagram_profile_posts_total") as cur:
                total_row = pg.fetch_one_with_cursor(cur, total_sql, params) or {}
    rows = _instagram_payload_rows_for_read(
        rows,
        row_kind="mixed",
        mode=payload_mode,
        surface="instagram.profile.normal",
    )
    return rows, _normalize_non_negative_int(total_row.get("total"))


def _fetch_instagram_profile_rows_page_no_search_created(
    account_handle: str,
    *,
    page: int,
    page_size: int,
    conn: Any | None = None,
    _payload_mode_override: str | None = None,
) -> tuple[list[dict[str, Any]], int]:
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    safe_page = max(1, int(page))
    safe_page_size = max(1, min(int(page_size), _SOCIAL_ACCOUNT_PROFILE_MAX_PAGE_SIZE))
    safe_offset = (safe_page - 1) * safe_page_size
    owner_match_clause = _instagram_owner_account_match_sql(alias="p")
    reported_comments_expr = _instagram_reported_comments_sql("p")
    lifecycle_supported = _comment_lifecycle_supported("instagram_comments", conn=conn)
    active_condition = "c.is_missing is not true" if lifecycle_supported else "true"
    use_comment_rollups = _instagram_post_comment_rollups_available(conn=conn)
    collaborator_rows_available = _instagram_catalog_collaborator_membership_available(conn=conn)
    from trr_backend.socials.instagram import payload_sidecars

    payload_mode = _payload_mode_override or payload_sidecars.payload_read_mode()
    post_sidecar_join, post_sidecar_projection = _instagram_payload_sidecar_sql(
        row_kind="post", row_alias="p", mode=payload_mode
    )
    catalog_sidecar_join, catalog_sidecar_projection = _instagram_payload_sidecar_sql(
        row_kind="catalog", row_alias="p", mode=payload_mode
    )
    collaborator_rows_sql = (
        """
        collaborator_rows as materialized (
          select
            'catalog'::text as _row_kind,
            p.id::text as _row_id,
            materialized_post.id::text as profile_row_id,
            p.source_id,
            p.posted_at,
            'catalog'::text as _profile_source_surface,
            'collaborator'::text as _profile_match_mode,
            1::int as _profile_dataset_priority
          from social.instagram_account_catalog_post_collaborators m
          join social.instagram_account_catalog_posts p
            on p.id = m.catalog_post_id
          left join social.instagram_posts materialized_post
            on materialized_post.shortcode = p.source_id
          where m.collaborator_handle = %s
            and lower(p.source_account) <> %s
            and nullif(p.source_id, '') is not null
        )
        """
        if collaborator_rows_available
        else """
        collaborator_rows as materialized (
          select *
          from owner_rows
          where false
        )
        """
    )
    candidate_rows_sql = f"""
        with owner_rows as materialized (
          select
            'materialized'::text as _row_kind,
            p.id::text as _row_id,
            p.id::text as profile_row_id,
            p.shortcode as source_id,
            p.posted_at,
            'materialized'::text as _profile_source_surface,
            'owner'::text as _profile_match_mode,
            3::int as _profile_dataset_priority
          from social.instagram_posts p
          where {owner_match_clause}
            and nullif(p.shortcode, '') is not null
          union all
          select
            'catalog'::text as _row_kind,
            p.id::text as _row_id,
            materialized_post.id::text as profile_row_id,
            p.source_id,
            p.posted_at,
            'catalog'::text as _profile_source_surface,
            'owner'::text as _profile_match_mode,
            2::int as _profile_dataset_priority
          from social.instagram_account_catalog_posts p
          left join social.instagram_posts materialized_post
            on materialized_post.shortcode = p.source_id
          where lower(p.source_account) = %s
            and nullif(p.source_id, '') is not null
        ),
        {collaborator_rows_sql},
        deduped_rows as materialized (
          select distinct on (source_id)
            *
          from (
            select * from owner_rows
            union all
            select * from collaborator_rows
          ) candidate_rows
          order by
            source_id,
            _profile_dataset_priority desc,
            posted_at desc nulls last,
            _row_id desc
        )
    """
    page_sql = f"""
        {candidate_rows_sql},
        page_keys as materialized (
          select
            row_number() over (order by posted_at desc nulls last, _row_id desc)::int as _page_rank,
            deduped_rows.*,
            count(*) over()::int as _total_count
          from deduped_rows
          order by posted_at desc nulls last, _row_id desc
          limit %s offset %s
        ),
        {
        _instagram_saved_comment_counts_cte_sql(
            source_cte="page_keys",
            active_condition=active_condition,
            use_rollup=use_comment_rollups,
        )
    }
        select
          page_keys._page_rank,
          p.id::text as id,
          p.id::text as profile_row_id,
          p.show_id::text as show_id,
          p.season_id::text as season_id,
          p.source_account,
          p.shortcode as source_id,
          p.shortcode as shortcode,
          p.posted_at,
          s.season_number,
          sh.name as show_name,
          sh.slug as show_slug,
          null::text as title,
          p.caption,
          null::text as description,
          null::text as text,
          p.media_type,
          coalesce(p.media_urls, '[]'::jsonb) as media_urls,
          nullif(p.thumbnail_url, '') as thumbnail_url,
          coalesce(p.media_urls, '[]'::jsonb) as source_media_urls,
          coalesce(p.hosted_media_urls, '[]'::jsonb) as hosted_media_urls,
          nullif(p.thumbnail_url, '') as source_thumbnail_url,
          nullif(coalesce(p.hosted_thumbnail_url, ''), '') as hosted_thumbnail_url,
          nullif(coalesce(p.post_format, ''), '') as post_format,
          coalesce(p.hashtags, '[]'::jsonb) as hashtags,
          coalesce(p.mentions, '[]'::jsonb) as mentions,
          coalesce(p.collaborators, '[]'::jsonb) as collaborators,
          coalesce(p.collaborators_detail, '[]'::jsonb) as collaborators_detail,
          coalesce(p.profile_tags, '[]'::jsonb) as profile_tags,
          coalesce(p.likes, 0)::bigint as likes,
          {reported_comments_expr}::bigint as comments_count,
          p.fb_comment_count,
          p.facebook_crosspost_observed_at,
          p.facebook_crosspost_source,
          p.facebook_post_id,
          p.facebook_post_url,
          coalesce(p.views, 0)::bigint as views,
          p.media_repost_count::bigint as media_repost_count,
          coalesce(p.media_repost_count, 0)::bigint as shares,
          0::bigint as retweets,
          0::bigint as replies_count,
          0::bigint as quotes,
          coalesce(p.raw_data, '{{}}'::jsonb) as raw_data,
          null::text as permalink,
          coalesce(saved_comment_counts.saved_comments, 0)::int as saved_comments,
          greatest(({reported_comments_expr})::bigint - coalesce(saved_comment_counts.saved_comments, 0), 0)::int
            as missing_comments,
          page_keys._profile_source_surface,
          page_keys._profile_match_mode,
          page_keys._profile_dataset_priority,
          page_keys._total_count
          {post_sidecar_projection}
        from page_keys
        join social.instagram_posts p
          on page_keys._row_kind = 'materialized'
         and p.id::text = page_keys._row_id
        {post_sidecar_join}
        left join saved_comment_counts
          on saved_comment_counts.profile_row_id = p.id::text
        left join core.seasons s on s.id = p.season_id
        left join core.shows sh on sh.id = coalesce(p.show_id, s.show_id)
        union all
        select
          page_keys._page_rank,
          p.id::text as id,
          materialized_post.id::text as profile_row_id,
          p.assigned_show_id::text as show_id,
          p.assigned_season_id::text as season_id,
          p.source_account,
          p.source_id as source_id,
          p.source_id as shortcode,
          p.posted_at,
          s.season_number,
          sh.name as show_name,
          sh.slug as show_slug,
          p.title,
          p.caption,
          p.description,
          p.text,
          p.media_type,
          coalesce(p.media_urls, '[]'::jsonb) as media_urls,
          nullif(p.thumbnail_url, '') as thumbnail_url,
          coalesce(p.media_urls, '[]'::jsonb) as source_media_urls,
          coalesce(
            nullif(p.hosted_media_urls, '[]'::jsonb),
            nullif(p.raw_data -> 'hosted_media_urls', '[]'::jsonb),
            '[]'::jsonb
          ) as hosted_media_urls,
          nullif(p.thumbnail_url, '') as source_thumbnail_url,
          coalesce(nullif(p.hosted_thumbnail_url, ''), nullif(p.raw_data ->> 'hosted_thumbnail_url', ''))
            as hosted_thumbnail_url,
          coalesce(nullif(p.post_format, ''), nullif(p.raw_data ->> 'post_format', '')) as post_format,
          coalesce(p.hashtags, '[]'::jsonb) as hashtags,
          coalesce(p.mentions, '[]'::jsonb) as mentions,
          coalesce(p.collaborators, '[]'::jsonb) as collaborators,
          coalesce(
            nullif(p.collaborators_detail, '[]'::jsonb),
            nullif(p.raw_data -> 'collaborators_detail', '[]'::jsonb),
            '[]'::jsonb
          )
            as collaborators_detail,
          coalesce(p.profile_tags, '[]'::jsonb) as profile_tags,
          coalesce(p.likes, 0)::bigint as likes,
          greatest(coalesce(p.comments_count, 0), 0)::bigint as comments_count,
          null::integer as fb_comment_count,
          null::timestamptz as facebook_crosspost_observed_at,
          null::text as facebook_crosspost_source,
          null::text as facebook_post_id,
          null::text as facebook_post_url,
          coalesce(p.views, 0)::bigint as views,
          null::bigint as media_repost_count,
          coalesce(p.shares, 0)::bigint as shares,
          coalesce(p.retweets, 0)::bigint as retweets,
          coalesce(p.replies_count, 0)::bigint as replies_count,
          coalesce(p.quotes, 0)::bigint as quotes,
          coalesce(p.raw_data, '{{}}'::jsonb) as raw_data,
          p.permalink,
          coalesce(saved_comment_counts.saved_comments, 0)::int as saved_comments,
          greatest(coalesce(p.comments_count, 0)::bigint - coalesce(saved_comment_counts.saved_comments, 0), 0)::int
            as missing_comments,
          page_keys._profile_source_surface,
          page_keys._profile_match_mode,
          page_keys._profile_dataset_priority,
          page_keys._total_count
          {catalog_sidecar_projection}
        from page_keys
        join social.instagram_account_catalog_posts p
          on page_keys._row_kind = 'catalog'
         and p.id::text = page_keys._row_id
        {catalog_sidecar_join}
        left join social.instagram_posts materialized_post
          on materialized_post.shortcode = p.source_id
        left join saved_comment_counts
          on saved_comment_counts.profile_row_id = materialized_post.id::text
        left join core.seasons s on s.id = p.assigned_season_id
        left join core.shows sh on sh.id = coalesce(p.assigned_show_id, s.show_id)
        order by _page_rank asc
    """
    params: list[Any] = [normalized_account, normalized_account]
    if collaborator_rows_available:
        params.extend([normalized_account, normalized_account])
    try:
        if conn is None:
            rows = pg.fetch_all(page_sql, [*params, safe_page_size, safe_offset])
        else:
            with pg.db_cursor(conn=conn, label="instagram_profile_posts_created_rows") as cur:
                rows = pg.fetch_all_with_cursor(cur, page_sql, [*params, safe_page_size, safe_offset])
    except psycopg_errors.UndefinedTable:
        if payload_mode != "legacy":
            _log_instagram_payload_schema_unavailable(
                surface="instagram.profile.created",
                entity_identity=normalized_account,
            )
            return _fetch_instagram_profile_rows_page_no_search_created(
                account_handle,
                page=page,
                page_size=page_size,
                conn=conn,
                _payload_mode_override="legacy",
            )
        raise
    rows = _instagram_payload_rows_for_read(
        rows,
        row_kind="mixed",
        mode=payload_mode,
        surface="instagram.profile.created",
    )
    if rows:
        total = _normalize_non_negative_int(rows[0].get("_total_count"))
    else:
        total_sql = f"""
            {candidate_rows_sql}
            select count(*)::int as total
            from deduped_rows
        """
        if conn is None:
            total_row = pg.fetch_one(total_sql, params) or {}
        else:
            with pg.db_cursor(conn=conn, label="instagram_profile_posts_created_total") as cur:
                total_row = pg.fetch_one_with_cursor(cur, total_sql, params) or {}
        total = _normalize_non_negative_int(total_row.get("total"))
    return rows, total


def _social_account_profile_post_item(
    platform: str,
    row: Mapping[str, Any],
    *,
    account_handle: str,
    known_handle_identity_index: Mapping[str, set[str]] | None = None,
) -> dict[str, Any]:
    payload = _CORE_SOCIAL_ACCOUNT_PROFILE_POST_ITEM(
        platform,
        row,
        account_handle=account_handle,
        known_handle_identity_index=known_handle_identity_index,
    )
    normalized_platform = _normalize_social_account_profile_platform(platform)
    if normalized_platform != "instagram":
        return payload

    metrics = payload.get("metrics") if isinstance(payload.get("metrics"), Mapping) else {}
    has_split_counts = "saved_parent_comments" in row or "saved_child_replies" in row
    saved_parent_comments = (
        _normalize_non_negative_int(row.get("saved_parent_comments"))
        if has_split_counts
        else _normalize_non_negative_int(row.get("saved_comments"))
    )
    saved_child_replies = _normalize_non_negative_int(row.get("saved_child_replies")) if has_split_counts else 0
    facebook_comments = instagram_facebook_comment_count_from_row(row)
    breakdown = build_instagram_comment_breakdown(
        reported_comments=metrics.get("comments_count") or row.get("comments_count"),
        saved_parent_comments=saved_parent_comments,
        saved_child_replies=saved_child_replies,
        expected_child_replies=row.get("expected_child_replies"),
        facebook_comments=facebook_comments,
        classified_missing_comments=row.get("classified_missing_comments"),
        missing_reasons=row.get("missing_reasons") if isinstance(row.get("missing_reasons"), Mapping) else None,
        facebook_crosspost_observed_at=row.get("facebook_crosspost_observed_at"),
        facebook_crosspost_source=row.get("facebook_crosspost_source"),
    )
    payload["saved_comments"] = breakdown["saved_instagram_comments"]
    payload["comment_breakdown"] = breakdown
    payload["comment_completeness"] = instagram_comment_completeness_from_breakdown(breakdown)
    payload["facebook_crosspost"] = instagram_facebook_crosspost_payload_from_row(
        row,
        facebook_comments=facebook_comments,
    )
    return payload


def _instagram_comment_active_sql(comment_alias: str, lifecycle_supported: bool) -> str:
    return f"and {comment_alias}.is_missing = false" if lifecycle_supported else ""


def _instagram_top_level_comment_sql(comment_alias: str) -> str:
    return f"""
        and coalesce({comment_alias}.is_reply, false) = false
        and {comment_alias}.parent_comment_id is null
        and nullif(coalesce(to_jsonb({comment_alias}) ->> 'parent_comment_external_id', ''), '') is null
        """


def _comment_item_matches_search(item: dict[str, Any], search: str | None) -> bool:
    normalized_search = str(search or "").strip().lower()
    if not normalized_search:
        return True
    haystack = " ".join(
        str(value or "")
        for value in [
            item.get("text"),
            item.get("username"),
            item.get("ownerUsername"),
            item.get("display_name"),
            item.get("author_full_name"),
            item.get("user_id"),
            item.get("comment_id"),
            item.get("external_id"),
            item.get("post_source_id"),
        ]
    ).lower()
    return normalized_search in haystack


def _comment_item_sort_value(item: dict[str, Any], sort_by: str) -> Any:
    if sort_by == "user":
        return str(
            item.get("display_name")
            or item.get("author_full_name")
            or item.get("ownerUsername")
            or item.get("username")
            or ""
        ).casefold()
    if sort_by == "comment":
        return str(item.get("text") or "").casefold()
    if sort_by == "likes":
        return _normalize_non_negative_int(item.get("likes_count") or item.get("likesCount") or item.get("likes"))
    if sort_by == "replies":
        return _normalize_non_negative_int(
            item.get("replies_count") or item.get("repliesCount") or item.get("reply_count")
        )
    created_at = _social_account_profile_row_datetime(item.get("timestamp") or item.get("created_at"))
    return created_at or datetime.min.replace(tzinfo=UTC)


def _filter_sort_comment_items(
    items: list[dict[str, Any]],
    *,
    search: str | None,
    sort_by: str | None,
    sort_dir: str | None,
) -> list[dict[str, Any]]:
    normalized_sort_by = _normalize_social_account_profile_comment_sort_by(sort_by)
    normalized_sort_dir = _normalize_social_account_profile_comment_sort_dir(sort_dir)
    filtered = [item for item in items if _comment_item_matches_search(item, search)]
    return sorted(
        filtered,
        key=lambda item: (
            _comment_item_sort_value(item, normalized_sort_by),
            _social_account_profile_row_datetime(item.get("timestamp") or item.get("created_at"))
            or datetime.min.replace(tzinfo=UTC),
            str(item.get("id") or ""),
        ),
        reverse=normalized_sort_dir == "desc",
    )


def _social_account_profile_comment_identity_keys(item: dict[str, Any]) -> list[str]:
    keys = [
        str(item.get("id") or "").strip(),
        str(item.get("comment_id") or "").strip(),
        str(item.get("external_id") or "").strip(),
    ]
    return list(dict.fromkeys(key for key in keys if key))


def _social_account_profile_comment_primary_key(item: dict[str, Any]) -> str:
    keys = _social_account_profile_comment_identity_keys(item)
    if keys:
        return keys[0]
    return ":".join(
        [
            str(item.get("username") or item.get("ownerUsername") or "unknown"),
            str(item.get("created_at") or item.get("timestamp") or ""),
            str(item.get("text") or ""),
        ]
    )


def _social_account_profile_comment_parent_key(item: dict[str, Any]) -> str | None:
    return str(item.get("parent_comment_id") or item.get("parent_comment_external_id") or "").strip() or None


def _clone_social_account_profile_comment_thread(item: dict[str, Any]) -> dict[str, Any]:
    raw_replies = item.get("replies")
    replies = raw_replies if isinstance(raw_replies, list) else []
    return {
        **item,
        "replies": [
            _clone_social_account_profile_comment_thread(reply) for reply in replies if isinstance(reply, dict)
        ],
    }


def _thread_social_account_profile_comment_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    nodes_by_primary_key: dict[str, dict[str, Any]] = {}
    node_by_lookup_key: dict[str, dict[str, Any]] = {}
    for item in items:
        primary_key = _social_account_profile_comment_primary_key(item)
        node = _clone_social_account_profile_comment_thread(item)
        nodes_by_primary_key[primary_key] = node
        for key in _social_account_profile_comment_identity_keys(item):
            node_by_lookup_key.setdefault(key, node)

    roots: list[dict[str, Any]] = []
    for item in items:
        primary_key = _social_account_profile_comment_primary_key(item)
        node = nodes_by_primary_key.get(primary_key)
        if node is None:
            continue
        parent_key = _social_account_profile_comment_parent_key(item)
        parent = node_by_lookup_key.get(parent_key) if parent_key else None
        if parent is not None and parent is not node:
            parent["replies"] = [*parent.get("replies", []), node]
            continue
        roots.append(node)
    return roots


def get_social_account_profile_summary(
    platform: str,
    account_handle: str,
    *,
    detail: str = "full",
) -> dict[str, Any]:
    _sync_core_overrides()
    normalized_platform = _normalize_social_account_profile_platform(platform)
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    normalized_detail = _normalize_social_account_profile_summary_detail(detail)
    read_conn_label = f"profile-summary:{normalized_platform}:{normalized_account[:48]}"
    breakdown: dict[str, float] = {}
    try:
        with _social_account_profile_summary_connection(read_conn_label) as summary_conn:
            with _social_profile_perf_span(breakdown, "account_exists"):
                source_rows = _call_profile_summary_loader_with_conn(
                    _assert_social_account_profile_exists,
                    normalized_platform,
                    normalized_account,
                    conn=summary_conn,
                )
            if normalized_detail == "distribution":
                if normalized_platform in set(CATALOG_SUPPORTED_PLATFORMS):
                    per_show_counts = _timed_social_account_profile_summary_query(
                        platform=normalized_platform,
                        account_handle=normalized_account,
                        query_name="catalog_show_distribution",
                        loader=lambda: _call_profile_summary_loader_with_conn(
                            _shared_catalog_grouped_counts,
                            normalized_platform,
                            normalized_account,
                            group_by="show",
                            conn=summary_conn,
                        ),
                        fallback=lambda _exc: [],
                    )
                    per_season_counts = _timed_social_account_profile_summary_query(
                        platform=normalized_platform,
                        account_handle=normalized_account,
                        query_name="catalog_season_distribution",
                        loader=lambda: _call_profile_summary_loader_with_conn(
                            _shared_catalog_grouped_counts,
                            normalized_platform,
                            normalized_account,
                            group_by="season",
                            conn=summary_conn,
                        ),
                        fallback=lambda _exc: [],
                    )
                else:
                    per_show_counts, per_season_counts = (
                        _social_account_profile_grouped_counts(
                            normalized_platform,
                            normalized_account,
                            group_by="show",
                        ),
                        _social_account_profile_grouped_counts(
                            normalized_platform,
                            normalized_account,
                            group_by="season",
                        ),
                    )
                return {
                    "summary_detail": "distribution",
                    "platform": normalized_platform,
                    "account_handle": normalized_account,
                    "per_show_counts": per_show_counts,
                    "per_season_counts": per_season_counts,
                }
            if normalized_detail in {"lite", "distribution"}:
                analysis_rows = []
                row_totals = {}
                catalog_dataset_rows = False
            else:
                with _social_profile_perf_span(breakdown, "analysis_rows"):
                    analysis_rows = _call_profile_summary_loader_with_conn(
                        _social_account_profile_analysis_rows,
                        normalized_platform,
                        normalized_account,
                        limit=_SOCIAL_ACCOUNT_PROFILE_SUMMARY_ANALYSIS_LIMIT,
                        conn=summary_conn,
                    )
                    row_totals = _social_account_profile_summary_totals_from_rows(
                        normalized_platform,
                        normalized_account,
                        analysis_rows,
                    )
                    catalog_dataset_rows = any(
                        str((row or {}).get("_profile_source_surface") or "").strip().lower() == "catalog"
                        for row in analysis_rows
                    )
            comments_coverage_state: dict[str, list[dict[str, Any]]] = {"recent_runs": []}

            def _load_comments_coverage_payload() -> dict[str, Any]:
                recent_comments_runs = _social_account_comments_recent_runs(
                    normalized_platform,
                    normalized_account,
                    limit=10,
                    conn=summary_conn,
                )
                comments_coverage_state["recent_runs"] = list(recent_comments_runs)
                latest_comments_run = recent_comments_runs[0] if recent_comments_runs else {}
                if normalized_detail in {"lite", "distribution"}:
                    return {
                        "coverage_counts_deferred": True,
                        "coverage_counts_deferred_reason": "lite_summary",
                        "last_comments_run_at": latest_comments_run.get("created_at"),
                        "last_comments_run_status": latest_comments_run.get("status"),
                    }
                return {
                    **_call_profile_summary_loader_with_conn(
                        _instagram_social_account_comments_target_counts,
                        normalized_account,
                        conn=summary_conn,
                    ),
                    "last_comments_run_at": latest_comments_run.get("created_at"),
                    "last_comments_run_status": latest_comments_run.get("status"),
                }

            query_loaders: dict[str, Callable[[], Any]] = {}
            if normalized_detail == "full":
                query_loaders["assignment_rows"] = lambda: _timed_social_account_profile_summary_query(
                    platform=normalized_platform,
                    account_handle=normalized_account,
                    query_name="assignment_rows",
                    loader=lambda: _call_profile_summary_loader_with_conn(
                        _fetch_social_account_profile_assignment_rows,
                        normalized_platform,
                        normalized_account,
                        conn=summary_conn,
                    ),
                    fallback=lambda _exc: [],
                )
            if normalized_platform in set(CATALOG_SUPPORTED_PLATFORMS):
                query_loaders["catalog_totals"] = lambda: _timed_social_account_profile_summary_query(
                    platform=normalized_platform,
                    account_handle=normalized_account,
                    query_name="catalog_totals",
                    loader=lambda: _call_profile_summary_loader_with_conn(
                        _shared_catalog_summary_totals,
                        normalized_platform,
                        normalized_account,
                        conn=summary_conn,
                    ),
                    fallback=lambda _exc: {},
                )
                query_loaders["recent_catalog_runs"] = lambda: _timed_social_account_profile_summary_query(
                    platform=normalized_platform,
                    account_handle=normalized_account,
                    query_name="recent_catalog_runs",
                    loader=lambda: _call_profile_summary_loader_with_conn(
                        _catalog_recent_runs_header
                        if normalized_detail in {"lite", "distribution"}
                        else _catalog_recent_runs,
                        normalized_platform,
                        normalized_account,
                        conn=summary_conn,
                        limit=3,
                    ),
                    fallback=lambda _exc: [],
                )
            if normalized_platform == "instagram" and normalized_detail in {"lite", "distribution"}:
                query_loaders["lite_header_stats"] = lambda: _timed_social_account_profile_summary_query(
                    platform=normalized_platform,
                    account_handle=normalized_account,
                    query_name="lite_header_stats",
                    loader=lambda: _call_profile_summary_loader_with_conn(
                        _instagram_social_account_lite_header_stats,
                        normalized_account,
                        conn=summary_conn,
                    ),
                    fallback=lambda _exc: {},
                )
            elif normalized_platform == "tiktok" and normalized_detail in {"full", "lite"}:
                query_loaders["lite_header_stats"] = lambda: _timed_social_account_profile_summary_query(
                    platform=normalized_platform,
                    account_handle=normalized_account,
                    query_name="lite_header_stats",
                    loader=lambda: _call_profile_summary_loader_with_conn(
                        _tiktok_social_account_lite_header_stats,
                        normalized_account,
                        conn=summary_conn,
                    ),
                    fallback=lambda _exc: {},
                )
            elif normalized_platform == "instagram" and normalized_detail == "full":
                query_loaders["detail_rollup"] = lambda: _timed_social_account_profile_summary_query(
                    platform=normalized_platform,
                    account_handle=normalized_account,
                    query_name="detail_rollup",
                    loader=lambda: _call_profile_summary_loader_with_conn(
                        _instagram_social_account_detail_rollup,
                        normalized_account,
                        conn=summary_conn,
                    ),
                    fallback=lambda _exc: None,
                )
            if normalized_platform == "instagram" and normalized_detail in {"full", "lite"}:
                query_loaders["comments_coverage"] = lambda: _timed_social_account_profile_summary_query(
                    platform=normalized_platform,
                    account_handle=normalized_account,
                    query_name="comments_coverage",
                    loader=_load_comments_coverage_payload,
                    fallback=lambda _exc: None,
                )

            query_results: dict[str, Any] = {}
            with _social_profile_perf_span(breakdown, "query_loaders"):
                if query_loaders:
                    # Reuse one read connection end-to-end so summary requests do not
                    # stall between subqueries when worker writes are already occupying
                    # the remaining session-pool slots.
                    for query_name, loader in query_loaders.items():
                        query_results[query_name] = loader()

            assignment_rows = list(query_results.get("assignment_rows") or [])
            catalog_totals = dict(query_results.get("catalog_totals") or {})
            recent_catalog_runs = list(query_results.get("recent_catalog_runs") or [])
            if normalized_detail in {"lite", "distribution"}:
                recent_catalog_runs = [_lite_social_account_catalog_run(row) for row in recent_catalog_runs[:3]]
            detail_rollup = query_results.get("detail_rollup")
            lite_header_stats = query_results.get("lite_header_stats")
            lite_comments_saved_summary = (
                lite_header_stats.get("comments_saved_summary") if isinstance(lite_header_stats, Mapping) else None
            )
            comments_saved_summary = (
                _instagram_comments_saved_summary_from_detail_rollup(detail_rollup)
                if isinstance(detail_rollup, dict)
                else dict(lite_comments_saved_summary)
                if isinstance(lite_comments_saved_summary, Mapping)
                else None
            )
            comments_coverage_recent_runs = list(comments_coverage_state.get("recent_runs") or [])
            active_comments_run = next(
                (
                    row
                    for row in comments_coverage_recent_runs
                    if _status_is_active(str((row or {}).get("status") or "").strip().lower() or None)
                ),
                None,
            )
            comments_coverage = _resolve_social_account_comments_coverage_status(
                query_results.get("comments_coverage"),
                recent_runs=comments_coverage_recent_runs,
                comments_saved_summary=comments_saved_summary,
                active_run=active_comments_run,
            )
            lite_media_coverage = (
                lite_header_stats.get("media_coverage") if isinstance(lite_header_stats, Mapping) else None
            )
            media_coverage = (
                _instagram_media_coverage_from_detail_rollup(detail_rollup)
                if isinstance(detail_rollup, dict)
                else dict(lite_media_coverage)
                if isinstance(lite_media_coverage, Mapping)
                else None
            )

            with _social_profile_perf_span(breakdown, "summary_totals"):
                totals = _timed_social_account_profile_summary_query(
                    platform=normalized_platform,
                    account_handle=normalized_account,
                    query_name="summary_totals",
                    loader=(
                        lambda: (
                            row_totals
                            if normalized_platform == "instagram" and catalog_dataset_rows
                            else dict(lite_header_stats.get("totals") or {})
                            if normalized_platform == "instagram"
                            and normalized_detail in {"lite", "distribution"}
                            and isinstance(lite_header_stats, Mapping)
                            else _merge_social_account_profile_summary_totals(
                                _call_profile_summary_loader_with_conn(
                                    _social_account_profile_summary_totals,
                                    normalized_platform,
                                    normalized_account,
                                    conn=summary_conn,
                                ),
                                row_totals,
                            )
                            if normalized_platform == "instagram"
                            else _call_profile_summary_loader_with_conn(
                                _social_account_profile_summary_totals,
                                normalized_platform,
                                normalized_account,
                                conn=summary_conn,
                            )
                        )
                    ),
                    fallback=lambda _exc: row_totals,
                )
            latest_catalog_run = recent_catalog_runs[0] if recent_catalog_runs else {}
            if normalized_detail == "lite":
                hashtag_items = []
                per_show_counts = []
                per_season_counts = []
            elif normalized_detail == "distribution":
                hashtag_items = []
                if normalized_platform in set(CATALOG_SUPPORTED_PLATFORMS):
                    per_show_counts = _timed_social_account_profile_summary_query(
                        platform=normalized_platform,
                        account_handle=normalized_account,
                        query_name="catalog_show_distribution",
                        loader=lambda: _call_profile_summary_loader_with_conn(
                            _shared_catalog_grouped_counts,
                            normalized_platform,
                            normalized_account,
                            group_by="show",
                            conn=summary_conn,
                        ),
                        fallback=lambda _exc: [],
                    )
                    per_season_counts = _timed_social_account_profile_summary_query(
                        platform=normalized_platform,
                        account_handle=normalized_account,
                        query_name="catalog_season_distribution",
                        loader=lambda: _call_profile_summary_loader_with_conn(
                            _shared_catalog_grouped_counts,
                            normalized_platform,
                            normalized_account,
                            group_by="season",
                            conn=summary_conn,
                        ),
                        fallback=lambda _exc: [],
                    )
                else:
                    per_show_counts, per_season_counts = (
                        _social_account_profile_grouped_counts(
                            normalized_platform,
                            normalized_account,
                            group_by="show",
                        ),
                        _social_account_profile_grouped_counts(
                            normalized_platform,
                            normalized_account,
                            group_by="season",
                        ),
                    )
            elif normalized_platform in set(CATALOG_SUPPORTED_PLATFORMS) and normalized_platform != "instagram":
                hashtag_items = _build_social_account_profile_hashtag_items(
                    analysis_rows,
                    platform=normalized_platform,
                    account_handle=normalized_account,
                    assignment_rows=assignment_rows,
                )
                per_show_counts, per_season_counts = _social_account_profile_grouped_counts_from_rows(
                    normalized_platform,
                    normalized_account,
                    analysis_rows,
                )
            else:
                hashtag_items = _social_account_profile_hashtag_items(
                    normalized_platform,
                    normalized_account,
                    assignment_rows=assignment_rows,
                    rows=analysis_rows if normalized_platform == "instagram" else None,
                )
                per_show_counts, per_season_counts = (
                    _serialize_social_account_profile_post_buckets(
                        analysis_rows,
                        normalized_platform,
                        account_handle=normalized_account,
                    )
                    if normalized_platform == "instagram"
                    else (
                        _social_account_profile_grouped_counts(
                            normalized_platform,
                            normalized_account,
                            group_by="show",
                        ),
                        _social_account_profile_grouped_counts(
                            normalized_platform,
                            normalized_account,
                            group_by="season",
                        ),
                    )
                )
            if normalized_detail == "full":
                entity_payload = _build_social_account_profile_entity_aggregates(
                    analysis_rows,
                    platform=normalized_platform,
                    account_handle=normalized_account,
                )
            else:
                entity_payload = {"collaborators": [], "tags": []}
            identity_fields = _social_account_profile_optional_identity_fields(
                normalized_platform,
                normalized_account,
                source_rows=source_rows,
                analysis_rows=analysis_rows,
            )
            resolved_profile_url = _first_non_empty_str(
                identity_fields.pop("profile_url", None),
                _platform_profile_url_for_handle(normalized_platform, normalized_account),
            )
            identity_avatar_url = identity_fields.pop("avatar_url", None)
            resolved_avatar_url = (
                _first_non_empty_str(identity_avatar_url)
                if normalized_detail in {"lite", "distribution"}
                else _first_non_empty_str(
                    _social_account_profile_avatar_url(
                        normalized_platform,
                        normalized_account,
                        analysis_rows,
                        conn=summary_conn,
                    ),
                    identity_avatar_url,
                )
            )
        with _social_profile_perf_span(breakdown, "finalize_payload"):
            primary_source_metadata = _metadata_dict((source_rows[0] or {}).get("metadata")) if source_rows else {}
            primary_source_scope = str((source_rows[0] or {}).get("source_scope") or "") if source_rows else ""
            shared_profile = _shared_profile_contract(
                source_scope=primary_source_scope.strip() or "network",
                platform=normalized_platform,
                account_handle=normalized_account,
                metadata=primary_source_metadata,
            )
            source_status_rows = []
            for row in source_rows:
                payload = dict(row)
                payload["metadata"] = _metadata_dict(payload.get("metadata"))
                payload.update(
                    _shared_profile_contract(
                        source_scope=str(payload.get("source_scope") or "network"),
                        platform=normalized_platform,
                        account_handle=normalized_account,
                        metadata=payload["metadata"],
                    )
                )
                source_status_rows.append(payload)
            persisted_total_posts = max(
                _normalize_non_negative_int(totals.get("total_posts")),
                _normalize_non_negative_int(catalog_totals.get("catalog_total_posts")),
            )
            historical_total_posts = (
                _historical_catalog_expected_total_posts(normalized_platform, normalized_account)
                if persisted_total_posts <= 0
                and _normalize_non_negative_int(identity_fields.get("live_total_posts")) <= 0
                else 0
            )
            resolved_total_posts = max(
                persisted_total_posts,
                _normalize_non_negative_int(identity_fields.get("live_total_posts")),
                historical_total_posts,
            )
            payload = {
                "summary_detail": normalized_detail,
                "platform": normalized_platform,
                "account_handle": normalized_account,
                "network_name": shared_profile["network_name"],
                "profile_url": resolved_profile_url,
                "avatar_url": resolved_avatar_url,
                "total_posts": resolved_total_posts,
                "total_engagement": _normalize_non_negative_int(totals.get("total_engagement")),
                "total_views": _normalize_non_negative_int(totals.get("total_views")),
                "first_post_at": totals.get("first_post_at"),
                "last_post_at": totals.get("last_post_at"),
                "catalog_total_posts": _normalize_non_negative_int(catalog_totals.get("catalog_total_posts")),
                "catalog_assigned_posts": _normalize_non_negative_int(catalog_totals.get("catalog_assigned_posts")),
                "catalog_pending_review_posts": _normalize_non_negative_int(
                    catalog_totals.get("catalog_pending_review_posts")
                ),
                "catalog_unassigned_posts": _normalize_non_negative_int(catalog_totals.get("catalog_unassigned_posts")),
                "catalog_first_post_at": catalog_totals.get("catalog_first_post_at"),
                "catalog_last_post_at": catalog_totals.get("catalog_last_post_at"),
                "live_catalog_total_posts": _normalize_non_negative_int(catalog_totals.get("catalog_total_posts")),
                "live_catalog_total_engagement": _normalize_non_negative_int(
                    catalog_totals.get("catalog_total_engagement")
                ),
                "live_catalog_total_views": _normalize_non_negative_int(catalog_totals.get("catalog_total_views")),
                "live_catalog_first_post_at": catalog_totals.get("catalog_first_post_at"),
                "live_catalog_last_post_at": catalog_totals.get("catalog_last_post_at"),
                "live_catalog_caption_rows": _normalize_non_negative_int(catalog_totals.get("caption_rows")),
                "live_catalog_hashtag_instances": _normalize_non_negative_int(
                    catalog_totals.get("stored_hashtag_instances")
                ),
                "live_catalog_unique_hashtags": len(hashtag_items),
                "last_catalog_run_at": latest_catalog_run.get("created_at"),
                "last_catalog_run_status": latest_catalog_run.get("status"),
                "catalog_recent_runs": recent_catalog_runs,
                "per_show_counts": per_show_counts,
                "per_season_counts": per_season_counts,
                "top_hashtags": hashtag_items[:10],
                "top_collaborators": entity_payload.get("collaborators", [])[:10],
                "top_tags": entity_payload.get("tags", [])[:10],
                "source_status": source_status_rows,
                "comments_saved_summary": comments_saved_summary,
                "comments_coverage": comments_coverage,
                "media_coverage": media_coverage,
                **identity_fields,
            }
        return payload
    finally:
        _log_social_profile_perf(
            route="get_social_account_profile_summary",
            platform=normalized_platform,
            handle=normalized_account,
            breakdown=breakdown,
        )


def get_social_account_profile_posts(
    platform: str,
    account_handle: str,
    *,
    page: int = 1,
    page_size: int = _SOCIAL_ACCOUNT_PROFILE_DEFAULT_PAGE_SIZE,
    search: str | None = None,
    comments_only: bool = False,
    comment_filter: str | None = None,
    sort_by: str | None = None,
    sort_dir: str | None = None,
) -> dict[str, Any]:
    _sync_core_overrides()
    normalized_platform = _normalize_social_account_profile_platform(platform)
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    breakdown: dict[str, float] = {}
    sort_metadata: dict[str, Any] | None = None
    try:
        with _social_profile_perf_span(breakdown, "normalize"):
            safe_page = max(1, int(page))
            safe_page_size = max(1, min(int(page_size), _SOCIAL_ACCOUNT_PROFILE_MAX_PAGE_SIZE))
            normalized_search = str(search or "").strip() or None
            requested_sort_by = str(sort_by or "").strip().lower() or None
            normalized_comment_filter = _normalize_social_account_profile_comment_filter(comment_filter)
            normalized_sort_by = _normalize_social_account_profile_post_sort_by(sort_by)
            normalized_sort_dir = _normalize_social_account_profile_post_sort_dir(sort_dir)
        if normalized_platform == "instagram" and comments_only and normalized_search is None:
            with _social_account_profile_summary_connection(
                "social-profile-posts-instagram-comments-only"
            ) as read_conn:
                with _social_profile_perf_span(breakdown, "assert_profile"):
                    _assert_social_account_profile_exists(normalized_platform, normalized_account, conn=read_conn)
                with _social_profile_perf_span(breakdown, "fetch_rows"):
                    rows, total = _fetch_instagram_comments_only_profile_rows_page(
                        normalized_account,
                        page=safe_page,
                        page_size=safe_page_size,
                        search=normalized_search,
                        comment_filter=normalized_comment_filter,
                        sort_by=normalized_sort_by,
                        sort_dir=normalized_sort_dir,
                        conn=read_conn,
                    )
        elif normalized_platform in {"tiktok", "twitter", "youtube"} and comments_only and normalized_search is None:
            with _social_account_profile_summary_connection(
                f"social-profile-posts-{normalized_platform}-comments-only"
            ) as read_conn:
                with _social_profile_perf_span(breakdown, "assert_profile"):
                    _assert_social_account_profile_exists(normalized_platform, normalized_account, conn=read_conn)
                with _social_profile_perf_span(breakdown, "fetch_rows"):
                    materialized_comments_loader = _room_callable(
                        "_fetch_materialized_comments_only_profile_rows_page",
                        _fetch_materialized_comments_only_profile_rows_page,
                    )
                    rows, total = materialized_comments_loader(
                        normalized_platform,
                        normalized_account,
                        page=safe_page,
                        page_size=safe_page_size,
                        sort_by=normalized_sort_by,
                        sort_dir=normalized_sort_dir,
                        conn=read_conn,
                    )
        elif normalized_platform == "instagram" and not comments_only:
            with _social_account_profile_summary_connection("social-profile-posts-instagram") as read_conn:
                with _social_profile_perf_span(breakdown, "assert_profile"):
                    _assert_social_account_profile_exists(normalized_platform, normalized_account, conn=read_conn)
                instagram_rollups_available = _instagram_post_comment_rollups_available(conn=read_conn)
                if requested_sort_by == "missing_comments":
                    sort_metadata = {
                        "sort_by": "missing_comments",
                        "sort_dir": normalized_sort_dir,
                        "rollup_table": "social.instagram_post_comment_rollups",
                        "rollup_available": instagram_rollups_available,
                        "mode": "persisted_rollup"
                        if instagram_rollups_available
                        else ("bounded_page_score" if normalized_search is None else "live_comment_count"),
                        "exact": instagram_rollups_available or normalized_search is not None,
                        "candidate_limit": None
                        if instagram_rollups_available or normalized_search is not None
                        else (safe_page - 1) * safe_page_size + safe_page_size,
                    }
                try:
                    with _social_profile_perf_span(breakdown, "fetch_rows"):
                        if normalized_search is None and requested_sort_by is None:
                            rows, total = _fetch_instagram_profile_rows_page_no_search_created(
                                normalized_account,
                                page=safe_page,
                                page_size=safe_page_size,
                                conn=read_conn,
                            )
                        elif normalized_search is None:
                            rows, total = _fetch_instagram_profile_rows_page_no_search(
                                normalized_account,
                                page=safe_page,
                                page_size=safe_page_size,
                                sort_by=normalized_sort_by,
                                sort_dir=normalized_sort_dir,
                                conn=read_conn,
                            )
                        else:
                            rows, total = _fetch_instagram_profile_rows_page(
                                normalized_account,
                                page=safe_page,
                                page_size=safe_page_size,
                                search=normalized_search,
                                sort_by=normalized_sort_by,
                                sort_dir=normalized_sort_dir,
                                conn=read_conn,
                            )
                except psycopg_errors.UndefinedTable:
                    with _social_profile_perf_span(breakdown, "fetch_rows_fallback"):
                        matching_rows = _instagram_social_account_profile_dataset_rows(
                            normalized_account,
                            search=normalized_search,
                            comments_only=comments_only,
                            sort_by=normalized_sort_by,
                            sort_dir=normalized_sort_dir,
                            conn=read_conn,
                        )
                    total = len(matching_rows)
                    rows = matching_rows[(safe_page - 1) * safe_page_size : safe_page * safe_page_size]
        elif normalized_platform == "instagram":
            with _social_profile_perf_span(breakdown, "assert_profile"):
                _assert_social_account_profile_exists(normalized_platform, normalized_account)
            with _social_profile_perf_span(breakdown, "fetch_rows"):
                matching_rows = _instagram_social_account_profile_dataset_rows(
                    normalized_account,
                    search=normalized_search,
                    comments_only=comments_only,
                    sort_by=normalized_sort_by,
                    sort_dir=normalized_sort_dir,
                )
            total = len(matching_rows)
            rows = matching_rows[(safe_page - 1) * safe_page_size : safe_page * safe_page_size]
        else:
            with _social_profile_perf_span(breakdown, "assert_profile"):
                _assert_social_account_profile_exists(normalized_platform, normalized_account)
            with _social_profile_perf_span(breakdown, "fetch_total"):
                total = _social_account_profile_total_posts(
                    normalized_platform,
                    normalized_account,
                    search=normalized_search,
                )
            with _social_profile_perf_span(breakdown, "fetch_rows"):
                rows = _fetch_social_account_profile_rows(
                    normalized_platform,
                    normalized_account,
                    limit=safe_page_size,
                    offset=(safe_page - 1) * safe_page_size,
                    search=normalized_search,
                )
        with _social_profile_perf_span(breakdown, "build_items"):
            known_handle_identity_index = _build_social_account_profile_known_handle_identity_index(
                normalized_platform,
                rows,
            )
            items = [
                _social_account_profile_post_item(
                    normalized_platform,
                    row,
                    account_handle=normalized_account,
                    known_handle_identity_index=known_handle_identity_index,
                )
                | {
                    "match_mode": str(row.get("_profile_match_mode") or "owner"),
                    "source_surface": str(row.get("_profile_source_surface") or "materialized"),
                }
                for row in rows
            ]
        payload = {
            "items": items,
            "pagination": {
                "page": safe_page,
                "page_size": safe_page_size,
                "total": total,
                "total_pages": max(1, (total + safe_page_size - 1) // safe_page_size) if safe_page_size else 1,
            },
        }
        if sort_metadata is not None:
            payload["sort_metadata"] = sort_metadata
        return payload
    finally:
        _log_social_profile_perf(
            route="get_social_account_profile_posts",
            platform=normalized_platform,
            handle=normalized_account,
            breakdown=breakdown,
        )


def get_social_account_profile_comments(
    platform: str,
    account_handle: str,
    *,
    page: int = 1,
    page_size: int = _SOCIAL_ACCOUNT_PROFILE_DEFAULT_PAGE_SIZE,
    post_source_id: str | None = None,
    search: str | None = None,
    sort_by: str | None = None,
    sort_dir: str | None = None,
) -> dict[str, Any]:
    _sync_core_overrides()
    normalized_platform = _normalize_social_account_profile_platform(platform)
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    normalized_post_source_id = str(post_source_id or "").strip() or None
    normalized_search = str(search or "").strip() or None
    normalized_sort_by = _normalize_social_account_profile_comment_sort_by(sort_by)
    normalized_sort_dir = _normalize_social_account_profile_comment_sort_dir(sort_dir)
    safe_page = max(1, int(page))
    safe_page_size = max(1, min(int(page_size), _SOCIAL_ACCOUNT_PROFILE_MAX_PAGE_SIZE))
    if normalized_platform in {"tiktok", "twitter", "youtube"}:
        _assert_social_account_profile_exists(normalized_platform, normalized_account)
        source_rows = (
            _fetch_shared_catalog_rows(
                normalized_platform,
                normalized_account,
                source_ids=[normalized_post_source_id],
                limit=1,
            )
            if normalized_post_source_id
            else _fetch_shared_catalog_rows(normalized_platform, normalized_account)
        )
        discussion_items: list[dict[str, Any]] = []
        for row in source_rows:
            source_id = str(row.get("source_id") or "").strip()
            season_id = str(row.get("season_id") or "").strip()
            if not source_id or not season_id:
                continue
            try:
                detail_payload = get_post_comments(season_id, platform=normalized_platform, source_id=source_id)
            except ValueError:
                continue
            post_url = _social_account_profile_post_url(normalized_platform, row, account_handle=normalized_account)
            discussion_items.extend(
                _social_discussion_items_from_post_detail(
                    normalized_platform,
                    detail_payload,
                    post_id=str(row.get("id") or "").strip() or None,
                    post_source_id=source_id,
                    post_url=post_url,
                )
            )
        discussion_items = _filter_sort_comment_items(
            discussion_items,
            search=normalized_search,
            sort_by=normalized_sort_by,
            sort_dir=normalized_sort_dir,
        )
        total = len(discussion_items)
        start = (safe_page - 1) * safe_page_size
        end = start + safe_page_size
        return {
            "items": discussion_items[start:end],
            "pagination": {
                "page": safe_page,
                "page_size": safe_page_size,
                "total": total,
                "total_pages": max(1, (total + safe_page_size - 1) // safe_page_size) if safe_page_size else 1,
            },
        }
    if normalized_platform != "instagram":
        raise ValueError(
            "Account-profile comments are currently only supported for Instagram, TikTok, Twitter, and YouTube."
        )
    owner_match_clause = _social_account_profile_owner_match_sql("instagram", alias="p")
    read_conn_label = f"profile-comments:{normalized_platform}:{normalized_account[:48]}"
    breakdown: dict[str, float] = {}
    try:
        with _social_account_profile_summary_connection(read_conn_label) as read_conn:
            with _social_profile_perf_span(breakdown, "account_exists"):
                _assert_social_account_profile_exists(normalized_platform, normalized_account, conn=read_conn)
            lifecycle_supported = _call_profile_summary_loader_with_conn(
                _comment_lifecycle_supported, "instagram_comments", conn=read_conn
            )
            active_filter = _instagram_comment_active_sql("c", lifecycle_supported)
            search_filter_sql, search_filter_params = _comment_search_sql_parts(normalized_search)
            order_by_sql = _comments_order_by_sql(normalized_sort_by, normalized_sort_dir)
            if normalized_post_source_id:
                parent_filter_sql = _instagram_top_level_comment_sql("c")
                reply_active_filter = _instagram_comment_active_sql("reply", lifecycle_supported)
                reply_match_active_filter = _instagram_comment_active_sql("reply_match", lifecycle_supported)
                parent_search_condition_sql, parent_search_params = _comment_search_condition_sql(
                    normalized_search,
                    comment_alias="c",
                    post_alias="p",
                )
                reply_search_condition_sql, reply_search_params = _comment_search_condition_sql(
                    normalized_search,
                    comment_alias="reply_match",
                    post_alias="p",
                )
                thread_search_sql = ""
                thread_search_params: list[Any] = []
                if parent_search_condition_sql:
                    thread_search_sql = f"""
                            and (
                              {parent_search_condition_sql}
                              or exists (
                                select 1
                                from social.instagram_comments reply_match
                                where reply_match.post_id = c.post_id
                                  {reply_match_active_filter}
                                  and (
                                    reply_match.parent_comment_id::text = c.id::text
                                    or nullif(
                                      coalesce(to_jsonb(reply_match) ->> 'parent_comment_external_id', ''),
                                      ''
                                    ) = c.comment_id
                                  )
                                  and {reply_search_condition_sql}
                              )
                            )
                            """
                    thread_search_params = [*parent_search_params, *reply_search_params]
                breakdown_active_condition = "bc.is_missing is not true" if lifecycle_supported else "true"
                breakdown_missing_condition = "bc.is_missing is true" if lifecycle_supported else "false"
                breakdown_fb_crosspost_condition = "coalesce(to_jsonb(bc) ->> 'phase', '') = 'fb_crosspost'"
                breakdown_parent_external_expr = (
                    "nullif(coalesce(to_jsonb(bc) ->> 'parent_comment_external_id', ''), '')"
                )
                breakdown_reply_depth_expr = """
                            case
                              when coalesce(to_jsonb(bc) ->> 'reply_depth', '') ~ '^[0-9]+$'
                              then (to_jsonb(bc) ->> 'reply_depth')::int
                              else 0
                            end
                            """
                breakdown_reply_condition = f"""
                            (
                              coalesce(bc.is_reply, false)
                              or bc.parent_comment_id is not null
                              or {breakdown_parent_external_expr} is not null
                              or ({breakdown_reply_depth_expr}) > 0
                            )
                            """
                breakdown_reported_comments_expr = _instagram_reported_comments_sql("p")
                breakdown_facebook_comments_expr = _instagram_external_facebook_comments_sql("p")
                comments_sql = f"""
                        with filtered_posts as materialized (
                          select p.id
                          from social.instagram_posts p
                          where {owner_match_clause}
                            and p.shortcode = %s
                        ),
                        post_breakdown as (
                          select
                            p.id as post_id,
                            {breakdown_reported_comments_expr}::bigint as breakdown_reported_comments,
                            {breakdown_facebook_comments_expr}::bigint as breakdown_facebook_comments,
                            count(bc.id) filter (
                              where {breakdown_active_condition}
                                and not ({breakdown_fb_crosspost_condition})
                                and not {breakdown_reply_condition}
                            )::bigint as breakdown_saved_parent_comments,
                            count(bc.id) filter (
                              where {breakdown_active_condition}
                                and not ({breakdown_fb_crosspost_condition})
                                and {breakdown_reply_condition}
                            )::bigint as breakdown_saved_child_replies,
                            coalesce(sum(
                              greatest(
                                coalesce(bc.reply_count, 0),
                                case
                                  when coalesce(to_jsonb(bc) ->> 'child_comment_count', '') ~ '^[0-9]+$'
                                  then (to_jsonb(bc) ->> 'child_comment_count')::int
                                  else 0
                                end
                              )
                            ) filter (
                              where {breakdown_active_condition}
                                and not ({breakdown_fb_crosspost_condition})
                                and not {breakdown_reply_condition}
                            ), 0)::bigint as breakdown_expected_child_replies,
                            count(bc.id) filter (
                              where {breakdown_missing_condition}
                                and not ({breakdown_fb_crosspost_condition})
                            )::bigint
                              as breakdown_classified_missing_comments,
                            p.fb_comment_count,
                            p.fb_like_count,
                            p.is_shared_to_fb,
                            p.facebook_post_id,
                            p.facebook_post_url,
                            p.facebook_crosspost_observed_at,
                            p.facebook_crosspost_source,
                            p.crosspost_metadata,
                            p.social_context,
                            coalesce(p.raw_data, '{{}}'::jsonb) as raw_data
                          from filtered_posts fp
                          join social.instagram_posts p on p.id = fp.id
                          left join social.instagram_comments bc on bc.post_id = p.id
                          group by
                            p.id,
                            p.comments_count,
                            p.fb_comment_count,
                            p.fb_like_count,
                            p.is_shared_to_fb,
                            p.facebook_post_id,
                            p.facebook_post_url,
                            p.facebook_crosspost_observed_at,
                            p.facebook_crosspost_source,
                            p.crosspost_metadata,
                            p.social_context,
                            p.raw_data
                        ),
                        comment_total as (
                          select count(*)::int as total_count
                          from social.instagram_comments c
                          join filtered_posts fp on fp.id = c.post_id
                          join social.instagram_posts p on p.id = c.post_id
                          where 1 = 1
                            {active_filter}
                            {parent_filter_sql}
                            {thread_search_sql}
                        ),
                        page_ids as (
                          select
                            c.id,
                            c.post_id,
                            c.comment_id,
                            c.created_at,
                            row_number() over (order by {order_by_sql}) as sort_position
                          from social.instagram_comments c
                          join filtered_posts fp on fp.id = c.post_id
                          join social.instagram_posts p on p.id = c.post_id
                          where 1 = 1
                            {active_filter}
                            {parent_filter_sql}
                            {thread_search_sql}
                          order by {order_by_sql}
                          limit %s
                          offset %s
                        ),
                        page_rows as (
                          select
                            c.id::text as id,
                            c.comment_id,
                            c.post_id::text as post_id,
                            p.shortcode as post_source_id,
                            p.media_type as post_media_type,
                            nullif(p.post_format, '') as post_format,
                            coalesce(
                              nullif(p.permalink, ''),
                              nullif(p.raw_data ->> 'post_url', ''),
                              nullif(p.raw_data ->> 'permalink_url', ''),
                              nullif(p.raw_data ->> 'canonical_url', ''),
                              nullif(p.raw_data ->> 'url', ''),
                              nullif(p.raw_data ->> 'link', '')
                            ) as post_url,
                            c.username,
                            c.user_id,
                            nullif(coalesce(to_jsonb(c) ->> 'author_full_name', ''), '') as author_full_name,
                            nullif(
                              coalesce(to_jsonb(c) ->> 'author_profile_pic_url', ''),
                              ''
                            ) as author_profile_pic_url,
                            nullif(
                              coalesce(to_jsonb(c) ->> 'hosted_author_profile_pic_url', ''),
                              ''
                            ) as hosted_author_profile_pic_url,
                            nullif(
                              coalesce(to_jsonb(c) ->> 'author_profile_pic_url_hd', ''),
                              ''
                            ) as author_profile_pic_url_hd,
                            case
                              when lower(coalesce(to_jsonb(c) ->> 'author_is_verified', '')) in ('true', 'false')
                              then (to_jsonb(c) ->> 'author_is_verified')::boolean
                              else null
                            end as author_is_verified,
                            c.text,
                            c.likes,
                            coalesce(c.reply_count, 0) as replies_count,
                            coalesce(to_jsonb(c) -> 'media_urls', '[]'::jsonb) as media_urls,
                            coalesce(to_jsonb(c) -> 'hosted_media_urls', '[]'::jsonb) as hosted_media_urls,
                            c.is_reply,
                            c.created_at,
                            c.parent_comment_id::text as parent_comment_id,
                            nullif(
                              coalesce(to_jsonb(c) ->> 'parent_comment_external_id', ''),
                              ''
                            ) as parent_comment_external_id,
                            case
                              when coalesce(to_jsonb(c) ->> 'reply_depth', '') ~ '^[0-9]+$'
                              then (to_jsonb(c) ->> 'reply_depth')::int
                              else null
                            end as reply_depth,
                            nullif(coalesce(to_jsonb(c) ->> 'source_snapshot_type', ''), '') as source_snapshot_type,
                            comment_total.total_count,
                            pb.breakdown_reported_comments,
                            pb.breakdown_saved_parent_comments,
                            pb.breakdown_saved_child_replies,
                            pb.breakdown_expected_child_replies,
                            pb.breakdown_facebook_comments,
                            pb.breakdown_classified_missing_comments,
                            pb.fb_comment_count,
                            pb.fb_like_count,
                            pb.is_shared_to_fb,
                            pb.facebook_post_id,
                            pb.facebook_post_url,
                            pb.facebook_crosspost_observed_at,
                            pb.facebook_crosspost_source,
                            pb.crosspost_metadata,
                            pb.social_context,
                            pb.raw_data,
                            ids.sort_position,
                            0::int as row_kind
                          from page_ids ids
                          join social.instagram_comments c on c.id = ids.id
                          join social.instagram_posts p on p.id = ids.post_id
                          cross join comment_total
                          cross join post_breakdown pb
                        ),
                        reply_rows as (
                          select
                            reply.id::text as id,
                            reply.comment_id,
                            reply.post_id::text as post_id,
                            p.shortcode as post_source_id,
                            p.media_type as post_media_type,
                            nullif(p.post_format, '') as post_format,
                            coalesce(
                              nullif(p.permalink, ''),
                              nullif(p.raw_data ->> 'post_url', ''),
                              nullif(p.raw_data ->> 'permalink_url', ''),
                              nullif(p.raw_data ->> 'canonical_url', ''),
                              nullif(p.raw_data ->> 'url', ''),
                              nullif(p.raw_data ->> 'link', '')
                            ) as post_url,
                            reply.username,
                            reply.user_id,
                            nullif(coalesce(to_jsonb(reply) ->> 'author_full_name', ''), '') as author_full_name,
                            nullif(
                              coalesce(to_jsonb(reply) ->> 'author_profile_pic_url', ''),
                              ''
                            ) as author_profile_pic_url,
                            nullif(
                              coalesce(to_jsonb(reply) ->> 'hosted_author_profile_pic_url', ''),
                              ''
                            ) as hosted_author_profile_pic_url,
                            nullif(
                              coalesce(to_jsonb(reply) ->> 'author_profile_pic_url_hd', ''),
                              ''
                            ) as author_profile_pic_url_hd,
                            case
                              when lower(coalesce(to_jsonb(reply) ->> 'author_is_verified', '')) in ('true', 'false')
                              then (to_jsonb(reply) ->> 'author_is_verified')::boolean
                              else null
                            end as author_is_verified,
                            reply.text,
                            reply.likes,
                            coalesce(reply.reply_count, 0) as replies_count,
                            coalesce(to_jsonb(reply) -> 'media_urls', '[]'::jsonb) as media_urls,
                            coalesce(to_jsonb(reply) -> 'hosted_media_urls', '[]'::jsonb) as hosted_media_urls,
                            reply.is_reply,
                            reply.created_at,
                            reply.parent_comment_id::text as parent_comment_id,
                            nullif(
                              coalesce(to_jsonb(reply) ->> 'parent_comment_external_id', ''),
                              ''
                            ) as parent_comment_external_id,
                            case
                              when coalesce(to_jsonb(reply) ->> 'reply_depth', '') ~ '^[0-9]+$'
                              then (to_jsonb(reply) ->> 'reply_depth')::int
                              else null
                            end as reply_depth,
                            nullif(
                              coalesce(to_jsonb(reply) ->> 'source_snapshot_type', ''),
                              ''
                            ) as source_snapshot_type,
                            comment_total.total_count,
                            pb.breakdown_reported_comments,
                            pb.breakdown_saved_parent_comments,
                            pb.breakdown_saved_child_replies,
                            pb.breakdown_expected_child_replies,
                            pb.breakdown_facebook_comments,
                            pb.breakdown_classified_missing_comments,
                            pb.fb_comment_count,
                            pb.fb_like_count,
                            pb.is_shared_to_fb,
                            pb.facebook_post_id,
                            pb.facebook_post_url,
                            pb.facebook_crosspost_observed_at,
                            pb.facebook_crosspost_source,
                            pb.crosspost_metadata,
                            pb.social_context,
                            pb.raw_data,
                            ids.sort_position,
                            1::int as row_kind
                          from page_ids ids
                          join social.instagram_comments parent on parent.id = ids.id
                          join social.instagram_comments reply
                            on reply.post_id = ids.post_id
                           and reply.id <> parent.id
                           and (
                             reply.parent_comment_id::text = parent.id::text
                             or nullif(
                               coalesce(to_jsonb(reply) ->> 'parent_comment_external_id', ''),
                               ''
                             ) = parent.comment_id
                          )
                          join social.instagram_posts p on p.id = ids.post_id
                          cross join comment_total
                          cross join post_breakdown pb
                          where 1 = 1
                            {reply_active_filter}
                        )
                        select
                          id,
                          comment_id,
                          post_id,
                          post_source_id,
                          post_media_type,
                          post_format,
                          post_url,
                          username,
                          user_id,
                          author_full_name,
                          author_profile_pic_url,
                          hosted_author_profile_pic_url,
                          author_profile_pic_url_hd,
                          author_is_verified,
                          text,
                          likes,
                          replies_count,
                          media_urls,
                          hosted_media_urls,
                          is_reply,
                          created_at,
                          parent_comment_id,
                          parent_comment_external_id,
                          reply_depth,
                          source_snapshot_type,
                          total_count,
                          breakdown_reported_comments,
                          breakdown_saved_parent_comments,
                          breakdown_saved_child_replies,
                          breakdown_expected_child_replies,
                          breakdown_facebook_comments,
                          breakdown_classified_missing_comments,
                          fb_comment_count,
                          fb_like_count,
                          is_shared_to_fb,
                          facebook_post_id,
                          facebook_post_url,
                          facebook_crosspost_observed_at,
                          facebook_crosspost_source,
                          crosspost_metadata,
                          social_context,
                          raw_data
                        from (
                          select * from page_rows
                          union all
                          select * from reply_rows
                          union all
                          select
                            null::text as id,
                            null::text as comment_id,
                            null::text as post_id,
                            null::text as post_source_id,
                            null::text as post_media_type,
                            null::text as post_format,
                            null::text as post_url,
                            null::text as username,
                            null::text as user_id,
                            null::text as author_full_name,
                            null::text as author_profile_pic_url,
                            null::text as hosted_author_profile_pic_url,
                            null::text as author_profile_pic_url_hd,
                            null::boolean as author_is_verified,
                            null::text as text,
                            null::int as likes,
                            null::int as replies_count,
                            '[]'::jsonb as media_urls,
                            '[]'::jsonb as hosted_media_urls,
                            null::boolean as is_reply,
                            null::timestamptz as created_at,
                            null::text as parent_comment_id,
                            null::text as parent_comment_external_id,
                            null::int as reply_depth,
                            null::text as source_snapshot_type,
                            comment_total.total_count,
                            pb.breakdown_reported_comments,
                            pb.breakdown_saved_parent_comments,
                            pb.breakdown_saved_child_replies,
                            pb.breakdown_expected_child_replies,
                            pb.breakdown_facebook_comments,
                            pb.breakdown_classified_missing_comments,
                            pb.fb_comment_count,
                            pb.fb_like_count,
                            pb.is_shared_to_fb,
                            pb.facebook_post_id,
                            pb.facebook_post_url,
                            pb.facebook_crosspost_observed_at,
                            pb.facebook_crosspost_source,
                            pb.crosspost_metadata,
                            pb.social_context,
                            pb.raw_data,
                            null::bigint as sort_position,
                            null::int as row_kind
                          from comment_total
                          cross join post_breakdown pb
                          where not exists (select 1 from page_rows)
                        ) as comment_rows
                        order by
                          sort_position asc nulls last,
                          row_kind asc nulls last,
                          created_at asc nulls last,
                          id asc nulls last
                        """
                comments_params = [
                    normalized_account,
                    normalized_post_source_id,
                    *thread_search_params,
                    *thread_search_params,
                    safe_page_size,
                    (safe_page - 1) * safe_page_size,
                ]
            else:
                comments_sql = f"""
                        with filtered_posts as materialized (
                          select p.id
                          from social.instagram_posts p
                          where {owner_match_clause}
                        ),
                        comment_total as (
                          select count(*)::int as total_count
                          from social.instagram_comments c
                          join filtered_posts fp on fp.id = c.post_id
                          join social.instagram_posts p on p.id = c.post_id
                          where 1 = 1
                            {active_filter}
                            {search_filter_sql}
                        ),
                        page_ids as (
                          select
                            c.id,
                            c.post_id,
                            c.created_at,
                            row_number() over (order by {order_by_sql}) as sort_position
                          from social.instagram_comments c
                          join filtered_posts fp on fp.id = c.post_id
                          join social.instagram_posts p on p.id = c.post_id
                          where 1 = 1
                            {active_filter}
                            {search_filter_sql}
                          order by {order_by_sql}
                          limit %s
                          offset %s
                        ),
                        page_rows as (
                          select
                            c.id::text as id,
                            c.comment_id,
                            c.post_id::text as post_id,
                            p.shortcode as post_source_id,
                            p.media_type as post_media_type,
                            nullif(p.post_format, '') as post_format,
                            coalesce(
                              nullif(p.permalink, ''),
                              nullif(p.raw_data ->> 'post_url', ''),
                              nullif(p.raw_data ->> 'permalink_url', ''),
                              nullif(p.raw_data ->> 'canonical_url', ''),
                              nullif(p.raw_data ->> 'url', ''),
                              nullif(p.raw_data ->> 'link', '')
                            ) as post_url,
                            c.username,
                            c.user_id,
                            nullif(coalesce(to_jsonb(c) ->> 'author_full_name', ''), '') as author_full_name,
                            nullif(
                              coalesce(to_jsonb(c) ->> 'author_profile_pic_url', ''),
                              ''
                            ) as author_profile_pic_url,
                            nullif(
                              coalesce(to_jsonb(c) ->> 'hosted_author_profile_pic_url', ''),
                              ''
                            ) as hosted_author_profile_pic_url,
                            nullif(
                              coalesce(to_jsonb(c) ->> 'author_profile_pic_url_hd', ''),
                              ''
                            ) as author_profile_pic_url_hd,
                            case
                              when lower(coalesce(to_jsonb(c) ->> 'author_is_verified', '')) in ('true', 'false')
                              then (to_jsonb(c) ->> 'author_is_verified')::boolean
                              else null
                            end as author_is_verified,
                            c.text,
                            c.likes,
                            coalesce(c.reply_count, 0) as replies_count,
                            coalesce(to_jsonb(c) -> 'media_urls', '[]'::jsonb) as media_urls,
                            coalesce(to_jsonb(c) -> 'hosted_media_urls', '[]'::jsonb) as hosted_media_urls,
                            c.is_reply,
                            c.created_at,
                            c.parent_comment_id::text as parent_comment_id,
                            nullif(
                              coalesce(to_jsonb(c) ->> 'parent_comment_external_id', ''),
                              ''
                            ) as parent_comment_external_id,
                            case
                              when coalesce(to_jsonb(c) ->> 'reply_depth', '') ~ '^[0-9]+$'
                              then (to_jsonb(c) ->> 'reply_depth')::int
                              else null
                            end as reply_depth,
                            nullif(coalesce(to_jsonb(c) ->> 'source_snapshot_type', ''), '') as source_snapshot_type,
                            comment_total.total_count,
                            ids.sort_position
                          from page_ids ids
                          join social.instagram_comments c on c.id = ids.id
                          join social.instagram_posts p on p.id = ids.post_id
                          cross join comment_total
                        )
                        select
                          id,
                          comment_id,
                          post_id,
                          post_source_id,
                          post_media_type,
                          post_format,
                          post_url,
                          username,
                          user_id,
                          author_full_name,
                          author_profile_pic_url,
                          hosted_author_profile_pic_url,
                          author_profile_pic_url_hd,
                          author_is_verified,
                          text,
                          likes,
                          replies_count,
                          media_urls,
                          hosted_media_urls,
                          is_reply,
                          created_at,
                          parent_comment_id,
                          parent_comment_external_id,
                          reply_depth,
                          source_snapshot_type,
                          total_count
                        from (
                          select * from page_rows
                          union all
                          select
                            null::text as id,
                            null::text as comment_id,
                            null::text as post_id,
                            null::text as post_source_id,
                            null::text as post_media_type,
                            null::text as post_format,
                            null::text as post_url,
                            null::text as username,
                            null::text as user_id,
                            null::text as author_full_name,
                            null::text as author_profile_pic_url,
                            null::text as hosted_author_profile_pic_url,
                            null::text as author_profile_pic_url_hd,
                            null::boolean as author_is_verified,
                            null::text as text,
                            null::int as likes,
                            null::int as replies_count,
                            '[]'::jsonb as media_urls,
                            '[]'::jsonb as hosted_media_urls,
                            null::boolean as is_reply,
                            null::timestamptz as created_at,
                            null::text as parent_comment_id,
                            null::text as parent_comment_external_id,
                            null::int as reply_depth,
                            null::text as source_snapshot_type,
                            comment_total.total_count,
                            null::bigint as sort_position
                          from comment_total
                          where not exists (select 1 from page_rows)
                        ) as comment_rows
                        order by sort_position asc nulls last, created_at desc nulls last, id desc nulls last
                        """
                comments_params = [
                    normalized_account,
                    *search_filter_params,
                    *search_filter_params,
                    safe_page_size,
                    (safe_page - 1) * safe_page_size,
                ]
            with _social_profile_perf_span(breakdown, "comments_query"):
                with pg.db_cursor(conn=read_conn, label="social_account_profile_comments") as cur:
                    rows = pg.fetch_all_with_cursor(
                        cur,
                        comments_sql,
                        comments_params,
                    )
            with _social_profile_perf_span(breakdown, "finalize_payload"):
                items = [_format_instagram_profile_comment_row(row) for row in rows if str(row.get("id") or "").strip()]
                if normalized_post_source_id:
                    items = _thread_social_account_profile_comment_items(items)
                total = _normalize_non_negative_int((rows[0] or {}).get("total_count")) if rows else 0
                payload = {
                    "items": items,
                    "pagination": {
                        "page": safe_page,
                        "page_size": safe_page_size,
                        "total": total,
                        "total_pages": max(1, (total + safe_page_size - 1) // safe_page_size) if safe_page_size else 1,
                    },
                }
                if normalized_post_source_id:
                    breakdown_row = rows[0] if rows else {}
                    facebook_comments = _normalize_non_negative_int(breakdown_row.get("breakdown_facebook_comments"))
                    comment_breakdown = build_instagram_comment_breakdown(
                        reported_comments=breakdown_row.get("breakdown_reported_comments"),
                        saved_parent_comments=breakdown_row.get("breakdown_saved_parent_comments"),
                        saved_child_replies=breakdown_row.get("breakdown_saved_child_replies"),
                        expected_child_replies=breakdown_row.get("breakdown_expected_child_replies"),
                        facebook_comments=facebook_comments,
                        classified_missing_comments=breakdown_row.get("breakdown_classified_missing_comments"),
                        facebook_crosspost_observed_at=breakdown_row.get("facebook_crosspost_observed_at"),
                        facebook_crosspost_source=breakdown_row.get("facebook_crosspost_source"),
                    )
                    payload["comment_breakdown"] = comment_breakdown
                    payload["comment_completeness"] = instagram_comment_completeness_from_breakdown(comment_breakdown)
                    payload["facebook_crosspost"] = instagram_facebook_crosspost_payload_from_row(
                        breakdown_row,
                        facebook_comments=facebook_comments,
                    )
                    payload["pagination_mode"] = "parent_threads"
        return payload
    finally:
        _log_social_profile_perf(
            route="get_social_account_profile_comments",
            platform=normalized_platform,
            handle=normalized_account,
            breakdown=breakdown,
        )


def get_social_account_profile_hashtags(
    platform: str,
    account_handle: str,
    *,
    window: str | None = None,
    assignment_status: str | None = None,
) -> dict[str, Any]:
    _sync_core_overrides()
    normalized_platform = _normalize_social_account_profile_platform(platform)
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    normalized_assignment_status = str(assignment_status or "all").strip().lower()
    if normalized_assignment_status not in {"all", "assigned", "unassigned"}:
        raise ValueError("INVALID_HASHTAG_ASSIGNMENT_STATUS: assignment_status must be all, assigned, or unassigned")
    _assert_social_account_profile_exists(normalized_platform, normalized_account)
    assignment_rows = _fetch_social_account_profile_assignment_rows(
        normalized_platform,
        normalized_account,
        include_all_platform_scopes=True,
    )
    lookback_days = _social_account_profile_window_to_lookback_days(window)
    preloaded_rows: list[dict[str, Any]] | None = None
    if normalized_platform == "instagram":
        posted_since = (
            _now_utc() - timedelta(days=int(lookback_days)) if lookback_days is not None and lookback_days > 0 else None
        )
        preloaded_rows = _fetch_instagram_social_account_profile_entity_rows(
            normalized_account,
            posted_since=posted_since,
        )
    items = _social_account_profile_hashtag_items(
        normalized_platform,
        normalized_account,
        assignment_rows=assignment_rows,
        lookback_days=lookback_days,
        rows=preloaded_rows,
    )
    if normalized_assignment_status == "assigned":
        items = [item for item in items if item.get("assignments")]
    elif normalized_assignment_status == "unassigned":
        items = [item for item in items if not item.get("assignments")]
    return {"items": items, "assignment_status": normalized_assignment_status}


def get_social_account_profile_collaborators_tags(platform: str, account_handle: str) -> dict[str, Any]:
    _sync_core_overrides()
    normalized_platform = _normalize_social_account_profile_platform(platform)
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    _assert_social_account_profile_exists(normalized_platform, normalized_account)
    rows = (
        _fetch_instagram_social_account_profile_entity_rows(normalized_account)
        if normalized_platform == "instagram"
        else _social_account_profile_analysis_rows(normalized_platform, normalized_account)
    )
    payload = _build_social_account_profile_entity_aggregates(
        rows,
        platform=normalized_platform,
        account_handle=normalized_account,
    )
    return {
        "collaborators": payload.get("collaborators", []),
        "tags": payload.get("tags", []),
        "mentions": payload.get("mentions", []),
    }


_LOCAL_ROOM_NAMES.update({
    "_social_account_profile_post_item",
    "get_social_account_profile_summary",
    "get_social_account_profile_posts",
    "get_social_account_profile_comments",
    "get_social_account_profile_hashtags",
    "get_social_account_profile_collaborators_tags",
    "instagram_comment_rollup_health",
    "rebuild_instagram_post_comment_rollups",
    "_fetch_materialized_comments_only_profile_rows_page",
})
_LOCAL_ROOM_FUNCTIONS.update({_name: globals()[_name] for _name in _LOCAL_ROOM_NAMES})
__all__ = [
    "get_social_account_profile_summary",
    "get_social_account_profile_posts",
    "get_social_account_profile_comments",
    "get_social_account_profile_hashtags",
    "get_social_account_profile_collaborators_tags",
    "instagram_comment_rollup_health",
    "rebuild_instagram_post_comment_rollups",
]
