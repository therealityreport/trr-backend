from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

from trr_backend.db import pg
from trr_backend.socials.instagram.scraper import InstagramComment


@dataclass(slots=True)
class PersistedInstagramComments:
    post_id: str
    stored_total_comments: int
    comments_upserted: int
    comments_marked_missing: int
    comment_media_mirror_jobs_enqueued: int
    comment_media_mirror_job_enqueue_errors: int


@dataclass(slots=True)
class InstagramCommentsMaterializationError(Exception):
    message: str
    failure_metadata: dict[str, Any]
    error_code: str = "instagram_comments_post_materialization_failed"
    retryable: bool = False
    runtime_metadata: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        if self.runtime_metadata is None:
            self.runtime_metadata = dict(self.failure_metadata)

    def __str__(self) -> str:
        return self.message


class _MaterializedInstagramPost(SimpleNamespace):
    def to_dict(self) -> dict[str, Any]:
        return dict(getattr(self, "raw_data", {}) or {})


def _load_repo_helpers():
    from trr_backend.repositories import social_season_analytics as repo

    return repo


def find_instagram_post_for_comments(
    *,
    account_handle: str,
    shortcode: str,
    conn: Any | None = None,
) -> dict[str, Any] | None:
    normalized_account = str(account_handle or "").strip().lower().lstrip("@")
    query = """
        select
          p.id::text as id,
          p.season_id::text as season_id,
          coalesce(nullif(p.source_account, ''), nullif(p.username, ''), '') as account_handle
        from social.instagram_posts p
        where p.shortcode = %s
          and ltrim(lower(coalesce(nullif(p.source_account, ''), nullif(p.username, ''), '')), '@') = %s
        order by p.posted_at desc nulls last, p.id desc
        limit 1
        """
    if conn is not None:
        with pg.db_cursor(conn=conn) as cur:
            return pg.fetch_one_with_cursor(cur, query, [shortcode, normalized_account])
    return pg.fetch_one(query, [shortcode, normalized_account])


def _materialize_instagram_post_for_comments(
    *,
    repo: Any,
    account_handle: str,
    shortcode: str,
    conn: Any,
) -> dict[str, Any]:
    existing_post = find_instagram_post_for_comments(account_handle=account_handle, shortcode=shortcode, conn=conn)
    if existing_post:
        return existing_post

    normalized_account = str(account_handle or "").strip().lower().lstrip("@")
    failure_metadata: dict[str, Any] = {
        "platform": "instagram",
        "account_handle": normalized_account,
        "shortcode": str(shortcode or "").strip(),
        "materialization_stage": "comments_scrapling",
        "materialization_mode": "materialize_before_persist",
    }
    catalog_rows = repo._fetch_shared_catalog_rows(
        "instagram",
        normalized_account,
        source_ids=[str(shortcode or "").strip()],
        limit=1,
        conn=conn,
    )
    catalog_row = next(iter(catalog_rows or []), None)
    if not catalog_row:
        failure_metadata["catalog_row_found"] = False
        raise InstagramCommentsMaterializationError(
            f"Instagram post {shortcode} for @{account_handle} could not be materialized before comment persistence.",
            failure_metadata=failure_metadata,
        )

    failure_metadata["catalog_row_found"] = True
    failure_metadata["catalog_row_id"] = str(catalog_row.get("id") or "").strip() or None
    season_id = str(catalog_row.get("season_id") or "").strip() or None
    context = repo.get_season_context(season_id, conn=conn) if season_id else None
    post = _MaterializedInstagramPost(
        shortcode=str(catalog_row.get("source_id") or shortcode or "").strip(),
        taken_at=catalog_row.get("posted_at"),
        media_urls=list(catalog_row.get("media_urls") or []),
        thumbnail_url=catalog_row.get("thumbnail_url"),
        likes=catalog_row.get("likes"),
        comments=catalog_row.get("comments_count"),
        video_views_observed=catalog_row.get("views"),
        video_views=catalog_row.get("views"),
        hosted_media_urls=list(catalog_row.get("hosted_media_urls") or []),
        hosted_thumbnail_url=catalog_row.get("hosted_thumbnail_url"),
        profile_tags=list(catalog_row.get("profile_tags") or []),
        collaborators=list(catalog_row.get("collaborators") or []),
        hashtags=list(catalog_row.get("hashtags") or []),
        mentions=list(catalog_row.get("mentions") or []),
        media_mirror_status=catalog_row.get("media_mirror_status"),
        media_mirror_error=catalog_row.get("media_mirror_error"),
        media_mirror_attempt_count=catalog_row.get("media_mirror_attempt_count"),
        media_mirror_last_attempt_at=catalog_row.get("media_mirror_last_attempt_at"),
        media_mirror_last_job_id=catalog_row.get("media_mirror_last_job_id"),
        metadata_scraped_at=catalog_row.get("metadata_scraped_at"),
        duration_seconds=catalog_row.get("duration_seconds"),
        raw_data=dict(catalog_row),
        username=normalized_account,
        source_account=normalized_account,
    )
    upserted = repo._upsert_instagram_post(
        context,
        job_id=None,
        account=normalized_account,
        post=post,
        conn=conn,
    )
    failure_metadata["materialized_post_id"] = str((upserted or {}).get("id") or "").strip() or None

    materialized_post = find_instagram_post_for_comments(account_handle=account_handle, shortcode=shortcode, conn=conn)
    if materialized_post:
        return materialized_post

    failure_metadata["materialization_upserted"] = bool(upserted)
    raise InstagramCommentsMaterializationError(
        f"Instagram post {shortcode} for @{account_handle} could not be materialized before comment persistence.",
        failure_metadata=failure_metadata,
    )


def persist_instagram_comments_for_post(
    *,
    account_handle: str,
    shortcode: str,
    comments: list[InstagramComment],
    run_id: str | None,
    job_id: str | None,
    is_complete: bool,
    source_scope: str = "bravo",
    conn: Any | None = None,
) -> PersistedInstagramComments:
    if conn is None:
        with pg.db_connection() as managed_conn:
            return persist_instagram_comments_for_post(
                account_handle=account_handle,
                shortcode=shortcode,
                comments=comments,
                run_id=run_id,
                job_id=job_id,
                is_complete=is_complete,
                source_scope=source_scope,
                conn=managed_conn,
            )

    repo = _load_repo_helpers()
    post_row = _materialize_instagram_post_for_comments(
        repo=repo,
        account_handle=account_handle,
        shortcode=shortcode,
        conn=conn,
    )
    observed_comment_ids: set[str] = set()
    persist_stats = repo._new_comment_persist_stats()
    comments_upserted = 0
    comments_marked_missing = 0
    post_id = str(post_row.get("id") or "").strip()
    season_id = str(post_row.get("season_id") or "").strip() or None

    if season_id:
        context = repo.get_season_context(season_id, conn=conn)
        comments_upserted = repo._batch_upsert_instagram_comments(
            context,
            job_id=job_id,
            run_id=run_id,
            account=account_handle,
            post_id=post_id,
            comments=comments,
            observed_comment_ids=observed_comment_ids,
            persist_stats=persist_stats,
            source_scope=source_scope,
            conn=conn,
        )
    else:
        comments_upserted = _persist_without_season_context(
            repo=repo,
            post_id=post_id,
            account_handle=account_handle,
            comments=comments,
            run_id=run_id,
            job_id=job_id,
            observed_comment_ids=observed_comment_ids,
            persist_stats=persist_stats,
            conn=conn,
        )
    if is_complete:
        comments_marked_missing = repo._mark_missing_comments_for_anchor(
            platform="instagram",
            anchor_id=post_id,
            observed_comment_ids=observed_comment_ids,
            conn=conn,
        )
        repo._reconcile_post_comment_count(
            platform="instagram",
            post_db_id=post_id,
            conn=conn,
        )

    stored_total = repo._count_stored_comments([post_id], "instagram", conn=conn).get(post_id, 0)
    return PersistedInstagramComments(
        post_id=post_id,
        stored_total_comments=int(stored_total or 0),
        comments_upserted=comments_upserted,
        comments_marked_missing=comments_marked_missing,
        comment_media_mirror_jobs_enqueued=int(persist_stats.get("comment_media_mirror_jobs_enqueued") or 0),
        comment_media_mirror_job_enqueue_errors=int(persist_stats.get("comment_media_mirror_job_enqueue_errors") or 0),
    )


def _persist_without_season_context(
    *,
    repo: Any,
    post_id: str,
    account_handle: str,
    comments: list[InstagramComment],
    run_id: str | None,
    job_id: str | None,
    observed_comment_ids: set[str],
    persist_stats: dict[str, int],
    conn: Any,
) -> int:
    has_profile_pic = repo._column_exists("social", "instagram_comments", "author_profile_pic_url")
    has_verified = repo._column_exists("social", "instagram_comments", "author_is_verified")
    has_media = repo._column_exists("social", "instagram_comments", "media_urls")
    has_lifecycle = repo._comment_lifecycle_supported("instagram_comments")
    flat: list[tuple[InstagramComment, str | None]] = []
    for comment in comments:
        flat.extend(repo._flatten_instagram_comment_tree(comment))

    top_level: list[dict[str, Any]] = []
    replies: list[dict[str, Any]] = []
    now = repo._now_utc()
    for comment_obj, parent_external_id in flat:
        persist_stats["comments_fetched"] = int(persist_stats.get("comments_fetched") or 0) + 1
        external_id = str(getattr(comment_obj, "comment_id", "") or "").strip()
        if external_id:
            observed_comment_ids.add(external_id)
        if not external_id:
            persist_stats["comments_skipped_missing_id"] = (
                int(persist_stats.get("comments_skipped_missing_id") or 0) + 1
            )
            continue
        payload: dict[str, Any] = {
            "comment_id": external_id,
            "post_id": post_id,
            "parent_comment_id": None,
            "username": getattr(comment_obj, "username", ""),
            "user_id": getattr(comment_obj, "user_id", None),
            "text": getattr(comment_obj, "text", ""),
            "likes": int(getattr(comment_obj, "likes", 0) or 0),
            "is_reply": bool(getattr(comment_obj, "is_reply", False)),
            "reply_count": int(getattr(comment_obj, "reply_count", 0) or 0),
            "created_at": repo._parse_instagram_time(getattr(comment_obj, "created_at", None)),
            "scraped_at": now,
            "raw_data": comment_obj.to_dict() if hasattr(comment_obj, "to_dict") else {},
            "season_id": None,
            "source_account": account_handle,
        }
        if job_id:
            payload["job_id"] = job_id
        if has_lifecycle:
            payload["is_missing"] = False
            payload["missing_at"] = None
            payload["last_seen_at"] = now
            if run_id:
                payload["last_seen_run_id"] = run_id
        if has_profile_pic:
            payload["author_profile_pic_url"] = (
                str(getattr(comment_obj, "owner_profile_pic_url", "") or "").strip() or None
            )
        if has_verified:
            payload["author_is_verified"] = getattr(comment_obj, "owner_is_verified", None)
        if has_media:
            payload["media_urls"] = [
                str(url).strip() for url in (getattr(comment_obj, "media_urls", []) or []) if str(url).strip()
            ]
        if parent_external_id is None:
            top_level.append(payload)
        else:
            payload["_parent_external_id"] = parent_external_id
            replies.append(payload)

    ext_to_db: dict[str, str] = {}
    upserted = 0
    for batch in (top_level, replies):
        if batch is replies:
            for payload in batch:
                parent_external_id = str(payload.pop("_parent_external_id", "") or "").strip()
                payload["parent_comment_id"] = ext_to_db.get(parent_external_id)
        rows = (
            repo._pg_upsert_many("instagram_comments", batch, conflict_col=["post_id", "comment_id"], conn=conn)
            if batch
            else []
        )
        for row in rows:
            ext_id = str(row.get("comment_id") or "").strip()
            db_id = str(row.get("id") or "").strip()
            if ext_id and db_id:
                ext_to_db[ext_id] = db_id
        upserted += len(rows)
    persist_stats["comments_upserted"] = int(persist_stats.get("comments_upserted") or 0) + upserted
    return upserted
