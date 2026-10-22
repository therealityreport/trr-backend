# ruff: noqa: F821, UP037
"""TikTok comment persistence: recursive comment-tree upsert into ``social.tiktok_comments``
and per-post comment enrichment rollups.

Moved out of ``social_season_analytics_impl``; the legacy repository keeps
same-named wrappers that delegate through ``_LOCAL_ROOM_FUNCTIONS``.
"""

from __future__ import annotations

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
    from trr_backend.socials.model_types import SeasonContext

    # Repository names resolved through the published provider namespace.
    TOXICITY_MARKERS: Any

    def _comment_lifecycle_supported(*args: Any, **kwargs: Any) -> Any: ...
    def _enqueue_tiktok_comment_media_mirror_job(*args: Any, **kwargs: Any) -> Any: ...
    def _now_utc(*args: Any, **kwargs: Any) -> Any: ...

    _parse_tiktok_time: Any

    def _pg_upsert(*args: Any, **kwargs: Any) -> Any: ...
    def _relation_exists(*args: Any, **kwargs: Any) -> Any: ...
    def _tiktok_comments_has_column(*args: Any, **kwargs: Any) -> Any: ...
    def sentiment_for_text(*args: Any, **kwargs: Any) -> Any: ...


# Room names are declared before the provider is built so that a late
# ``adopt_published`` (leaf imported after the repository published) still
# captures the repository wrappers for ``_room_callable``.
_LOCAL_ROOM_NAMES: set[str] = {
    "_upsert_tiktok_comment_tree",
    "_persist_tiktok_comment_enrichment",
}
_IMPORTED_CORE_NAMES: set[str] = set()
_LOCAL_ROOM_FUNCTIONS: dict[str, Any] = {}
_CORE_ROOM_WRAPPERS: dict[str, Any] = {}


_PROVIDER = LateNamespaceProvider(
    globals(),
    prefix="TIKTOK_COMMENTS_PERSISTENCE_PROVIDER",
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


def _upsert_tiktok_comment_tree(
    context: SeasonContext | None,
    *,
    job_id: str | None,
    run_id: str | None,
    account: str,
    post_id: str,
    comment: Any,
    parent_comment_db_id: str | None = None,
    source_scope: str = "network",
    observed_comment_ids: set[str] | None = None,
    persist_stats: dict[str, int] | None = None,
    conn: Any | None = None,
) -> int:
    _sync_core_overrides()
    if persist_stats is not None:
        persist_stats["comments_fetched"] = int(persist_stats.get("comments_fetched") or 0) + 1

    created_at = _parse_tiktok_time(getattr(comment, "created_at", None))
    comment_external_id = str(getattr(comment, "comment_id", "") or "")
    if observed_comment_ids is not None and comment_external_id:
        observed_comment_ids.add(comment_external_id)
    if not comment_external_id.strip():
        if persist_stats is not None:
            persist_stats["comments_skipped_missing_id"] = (
                int(persist_stats.get("comments_skipped_missing_id") or 0) + 1
            )
        total = 0
        for reply in getattr(comment, "replies", []) or []:
            total += _room_callable("_upsert_tiktok_comment_tree", _upsert_tiktok_comment_tree)(
                context,
                job_id=job_id,
                run_id=run_id,
                account=account,
                post_id=post_id,
                comment=reply,
                parent_comment_db_id=parent_comment_db_id,
                observed_comment_ids=observed_comment_ids,
                persist_stats=persist_stats,
                source_scope=source_scope,
                conn=conn,
            )
        return total
    media_urls = [str(url).strip() for url in (getattr(comment, "media_urls", []) or []) if str(url).strip()]
    payload = {
        "comment_id": comment_external_id,
        "post_id": post_id,
        "parent_comment_id": parent_comment_db_id,
        "username": getattr(comment, "username", ""),
        "user_id": getattr(comment, "user_id", None),
        "nickname": getattr(comment, "nickname", None),
        "text": getattr(comment, "text", ""),
        "likes": int(getattr(comment, "likes", 0) or 0),
        "is_reply": bool(getattr(comment, "is_reply", False)),
        "reply_count": int(getattr(comment, "reply_count", 0) or 0),
        "created_at": created_at,
        "scraped_at": _now_utc(),
        "raw_data": comment.to_dict() if hasattr(comment, "to_dict") else {},
        "source_account": account,
    }
    if context is not None:
        payload["season_id"] = context.season_id
    if _tiktok_comments_has_column("comment_language", conn=conn):
        payload["comment_language"] = str(getattr(comment, "comment_language", "") or "").strip() or None
    if _tiktok_comments_has_column("is_author_liked", conn=conn):
        is_author_liked = getattr(comment, "is_author_liked", None)
        payload["is_author_liked"] = bool(is_author_liked) if isinstance(is_author_liked, (bool, int)) else None
    if _tiktok_comments_has_column("aweme_id", conn=conn):
        payload["aweme_id"] = str(getattr(comment, "aweme_id", "") or "").strip() or None
    if _tiktok_comments_has_column("parent_source_comment_id", conn=conn):
        payload["parent_source_comment_id"] = (
            str(getattr(comment, "parent_source_comment_id", "") or "").strip() or None
        )
    if _tiktok_comments_has_column("user_url", conn=conn):
        payload["user_url"] = str(getattr(comment, "user_url", "") or "").strip() or None
    if _tiktok_comments_has_column("user_bio", conn=conn):
        payload["user_bio"] = str(getattr(comment, "user_bio", "") or "").strip() or None
    if _tiktok_comments_has_column("user_avatar_url", conn=conn):
        payload["user_avatar_url"] = str(getattr(comment, "user_avatar_url", "") or "").strip() or None
    if _tiktok_comments_has_column("user_region", conn=conn):
        payload["user_region"] = str(getattr(comment, "user_region", "") or "").strip() or None
    if _tiktok_comments_has_column("user_language", conn=conn):
        payload["user_language"] = str(getattr(comment, "user_language", "") or "").strip() or None
    if _tiktok_comments_has_column("media_urls", conn=conn):
        payload["media_urls"] = media_urls
    if media_urls and _tiktok_comments_has_column("media_mirror_status", conn=conn):
        payload["media_mirror_status"] = "pending"
    if media_urls and _tiktok_comments_has_column("media_mirror_error", conn=conn):
        payload["media_mirror_error"] = None
    if job_id:
        payload["job_id"] = job_id
    if _comment_lifecycle_supported("tiktok_comments", conn=conn):
        payload["is_missing"] = False
        payload["missing_at"] = None
        payload["last_seen_at"] = _now_utc()
        if run_id is not None:
            payload["last_seen_run_id"] = run_id
    row = _pg_upsert("tiktok_comments", payload, conflict_col="comment_id", conn=conn)
    comment_db_id = (row or {}).get("id")
    if persist_stats is not None and row:
        persist_stats["comments_upserted"] = int(persist_stats.get("comments_upserted") or 0) + 1
    if row and media_urls and context is not None:
        try:
            mirror_job_id = _enqueue_tiktok_comment_media_mirror_job(
                context,
                run_id=run_id,
                account=account,
                comment_row=row,
                parent_job_id=job_id,
                source_scope=source_scope,
                conn=conn,
            )
            if mirror_job_id and persist_stats is not None:
                persist_stats["comment_media_mirror_jobs_enqueued"] = (
                    int(persist_stats.get("comment_media_mirror_jobs_enqueued") or 0) + 1
                )
        except Exception:  # noqa: BLE001
            if persist_stats is not None:
                persist_stats["comment_media_mirror_job_enqueue_errors"] = (
                    int(persist_stats.get("comment_media_mirror_job_enqueue_errors") or 0) + 1
                )

    total = 1 if row else 0
    for reply in getattr(comment, "replies", []) or []:
        total += _room_callable("_upsert_tiktok_comment_tree", _upsert_tiktok_comment_tree)(
            context,
            job_id=job_id,
            run_id=run_id,
            account=account,
            post_id=post_id,
            comment=reply,
            parent_comment_db_id=comment_db_id,
            observed_comment_ids=observed_comment_ids,
            persist_stats=persist_stats,
            source_scope=source_scope,
            conn=conn,
        )
    return total


def _persist_tiktok_comment_enrichment(
    *,
    post_id: str,
    cast_names: list[str],
    conn: Any | None = None,
) -> None:
    _sync_core_overrides()
    if not _relation_exists("social.tiktok_post_comment_enrichment"):
        return
    comment_rows = pg.fetch_all(
        """
        select coalesce(text, '') as text
        from social.tiktok_comments
        where post_id = %s::uuid
          and coalesce(is_missing, false) = false
        """,
        [post_id],
    )
    positive = 0
    neutral = 0
    negative = 0
    toxicity = 0
    cast_mentions_count = 0
    cast_lower = [str(name or "").strip().lower() for name in cast_names if str(name or "").strip()]
    for row in comment_rows:
        text = str((row or {}).get("text") or "")
        label, _score = sentiment_for_text(text)
        if label == "positive":
            positive += 1
        elif label == "negative":
            negative += 1
        else:
            neutral += 1
        lowered = text.lower()
        if any(marker in lowered for marker in TOXICITY_MARKERS):
            toxicity += 1
        if cast_lower and any(name in lowered for name in cast_lower):
            cast_mentions_count += 1
    with pg.db_cursor(conn=conn) as cur:
        pg.fetch_one_with_cursor(
            cur,
            """
            insert into social.tiktok_post_comment_enrichment (
              post_id,
              positive_count,
              neutral_count,
              negative_count,
              toxicity_count,
              cast_mentions_count,
              updated_at
            )
            values (%s::uuid, %s, %s, %s, %s, %s, now())
            on conflict (post_id) do update
            set
              positive_count = excluded.positive_count,
              neutral_count = excluded.neutral_count,
              negative_count = excluded.negative_count,
              toxicity_count = excluded.toxicity_count,
              cast_mentions_count = excluded.cast_mentions_count,
              updated_at = now()
            returning post_id::text
            """,
            [post_id, positive, neutral, negative, toxicity, cast_mentions_count],
        )


_LOCAL_ROOM_FUNCTIONS.update({_name: globals()[_name] for _name in _LOCAL_ROOM_NAMES})
__all__ = [
    "_persist_tiktok_comment_enrichment",
    "_upsert_tiktok_comment_tree",
]
