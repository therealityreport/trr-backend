# ruff: noqa: F821, UP037
"""TikTok comment coverage reads: per-post coverage counts, comment-gap priority by video id,
and gap-first ordering of shared-account catalog posts.

Moved out of ``social_season_analytics_impl``; the legacy repository keeps
same-named wrappers that delegate through ``_LOCAL_ROOM_FUNCTIONS``.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from trr_backend.socials.control_plane.run_lifecycle import _legacy_module as _published_legacy_module
from trr_backend.socials.provider_registry import (
    LateNamespaceProvider,
    LateProviderProxy,
    adopt_published,
    publish_module_slot,
)

if TYPE_CHECKING:
    from psycopg2 import errors as psycopg_errors

    from trr_backend.db import pg

    # Repository names resolved through the published provider namespace.
    def _comment_lifecycle_supported(*args: Any, **kwargs: Any) -> Any: ...
    def _normalize_non_negative_int(*args: Any, **kwargs: Any) -> Any: ...
    def _normalize_social_account_profile_handle(*args: Any, **kwargs: Any) -> Any: ...
    def _shared_tiktok_catalog_comments_enabled(*args: Any, **kwargs: Any) -> Any: ...
    def _social_account_profile_owner_match_sql(*args: Any, **kwargs: Any) -> Any: ...


# Room names are declared before the provider is built so that a late
# ``adopt_published`` (leaf imported after the repository published) still
# captures the repository wrappers for ``_room_callable``.
_LOCAL_ROOM_NAMES: set[str] = {
    "_load_tiktok_comment_coverage_for_post",
    "_load_tiktok_comment_gap_priority_by_video_id",
    "_prioritize_shared_tiktok_posts_for_comment_gaps",
}
_IMPORTED_CORE_NAMES: set[str] = set()
_LOCAL_ROOM_FUNCTIONS: dict[str, Any] = {}
_CORE_ROOM_WRAPPERS: dict[str, Any] = {}


_PROVIDER = LateNamespaceProvider(
    globals(),
    prefix="TIKTOK_COMMENT_COVERAGE_PROVIDER",
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


def _load_tiktok_comment_coverage_for_post(
    post_db_id: str,
    *,
    conn: Any | None = None,
) -> dict[str, int]:
    _sync_core_overrides()
    normalized_post_id = str(post_db_id or "").strip()
    if not normalized_post_id:
        return {"active_comments": 0, "missing_comments": 0, "total_comments": 0}
    lifecycle_supported = _comment_lifecycle_supported("tiktok_comments", conn=conn)
    active_condition = "coalesce(is_missing, false) = false" if lifecycle_supported else "true"
    missing_condition = "coalesce(is_missing, false) = true" if lifecycle_supported else "false"
    with pg.db_cursor(conn=conn, label="tiktok-comment-coverage") as cur:
        row = (
            pg.fetch_one_with_cursor(
                cur,
                f"""
            select
              count(*) filter (where {active_condition})::int as active_comments,
              count(*) filter (where {missing_condition})::int as missing_comments,
              count(*)::int as total_comments
            from social.tiktok_comments
            where post_id = %s
            """,
                [normalized_post_id],
            )
            or {}
        )
    return {
        "active_comments": _normalize_non_negative_int(row.get("active_comments")),
        "missing_comments": _normalize_non_negative_int(row.get("missing_comments")),
        "total_comments": _normalize_non_negative_int(row.get("total_comments")),
    }


def _load_tiktok_comment_gap_priority_by_video_id(
    *,
    account_handle: str,
    video_ids: Sequence[str],
    conn: Any | None = None,
) -> dict[str, dict[str, int]]:
    _sync_core_overrides()
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    normalized_video_ids = sorted(
        {str(video_id or "").strip() for video_id in video_ids if str(video_id or "").strip()}
    )
    if not normalized_account or not normalized_video_ids:
        return {}
    lifecycle_supported = _comment_lifecycle_supported("tiktok_comments", conn=conn)
    active_comment_join_filter = "and c.is_missing is not true" if lifecycle_supported else ""
    owner_match_clause = _social_account_profile_owner_match_sql("tiktok", alias="p")
    sql = f"""
        select
          p.video_id,
          greatest(coalesce(max(p.comments_count), 0), 0)::int as reported_comments,
          count(c.id)::int as active_comments,
          greatest(coalesce(max(p.comments_count), 0) - count(c.id), 0)::int as missing_gap
        from social.tiktok_posts p
        left join social.tiktok_comments c
          on c.post_id = p.id
         {active_comment_join_filter}
        where {owner_match_clause}
          and p.video_id = any(%s)
        group by p.video_id
    """
    try:
        if conn is None:
            rows = pg.fetch_all(sql, [normalized_account, normalized_video_ids])
        else:
            with pg.db_cursor(conn=conn, label="tiktok-comment-gap-priority") as cur:
                rows = pg.fetch_all_with_cursor(cur, sql, [normalized_account, normalized_video_ids])
    except psycopg_errors.UndefinedTable:
        return {}
    priorities: dict[str, dict[str, int]] = {}
    for row in rows:
        video_id = str(row.get("video_id") or "").strip()
        if not video_id:
            continue
        priorities[video_id] = {
            "reported_comments": _normalize_non_negative_int(row.get("reported_comments")),
            "active_comments": _normalize_non_negative_int(row.get("active_comments")),
            "missing_gap": _normalize_non_negative_int(row.get("missing_gap")),
        }
    return priorities


def _prioritize_shared_tiktok_posts_for_comment_gaps(
    posts: Sequence[Any],
    *,
    account_handle: str,
    config: Mapping[str, Any],
) -> tuple[list[Any], dict[str, Any]]:
    _sync_core_overrides()
    post_list = list(posts)
    if not post_list or not _shared_tiktok_catalog_comments_enabled(config):
        return post_list, {"enabled": False, "prioritized_posts": 0, "known_gap_posts": 0}
    video_ids = [str(getattr(post, "video_id", "") or "").strip() for post in post_list]
    priorities = _room_callable(
        "_load_tiktok_comment_gap_priority_by_video_id", _load_tiktok_comment_gap_priority_by_video_id
    )(
        account_handle=account_handle,
        video_ids=video_ids,
    )

    def _priority(pair: tuple[int, Any]) -> tuple[int, int, int]:
        index, post = pair
        video_id = str(getattr(post, "video_id", "") or "").strip()
        priority = priorities.get(video_id) or {}
        reported_comments = max(
            _normalize_non_negative_int(getattr(post, "comments", 0)),
            _normalize_non_negative_int(priority.get("reported_comments")),
        )
        active_comments = _normalize_non_negative_int(priority.get("active_comments"))
        missing_gap = max(
            _normalize_non_negative_int(priority.get("missing_gap")),
            max(reported_comments - active_comments, 0),
        )
        return (-missing_gap, -reported_comments, index)

    ordered_pairs = sorted(enumerate(post_list), key=_priority)
    return [post for _index, post in ordered_pairs], {
        "enabled": True,
        "prioritized_posts": len(post_list),
        "known_gap_posts": sum(
            1 for priority in priorities.values() if _normalize_non_negative_int(priority.get("missing_gap")) > 0
        ),
    }


_LOCAL_ROOM_FUNCTIONS.update({_name: globals()[_name] for _name in _LOCAL_ROOM_NAMES})
__all__ = [
    "_load_tiktok_comment_coverage_for_post",
    "_load_tiktok_comment_gap_priority_by_video_id",
    "_prioritize_shared_tiktok_posts_for_comment_gaps",
]
