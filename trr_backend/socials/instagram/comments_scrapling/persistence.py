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
    comments_inserted: int = 0
    comments_refreshed: int = 0
    comments_changed: int = 0
    stored_parent_comments: int = 0
    stored_child_replies: int = 0
    expected_child_replies: int = 0
    stored_reply_gap_total: int = 0
    stored_reply_gap_parent_count: int = 0
    stored_reply_gap_samples: list[dict[str, Any]] | None = None


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


def _comment_effective_reply_depth(comment: InstagramComment, parent_external_id: str | None, *, fallback: int) -> int:
    try:
        parsed_depth = int(getattr(comment, "reply_depth", 0) or 0)
    except (TypeError, ValueError):
        parsed_depth = 0
    if parsed_depth > 0 or not parent_external_id:
        return max(0, parsed_depth)
    return max(0, int(fallback or 0))


def _upsert_write_counts(rows: list[dict[str, Any]]) -> tuple[int, int]:
    total = len(rows)
    inserted = 0
    for row in rows:
        if bool(row.pop("__trr_inserted", False)):
            inserted += 1
    return total, inserted


def _record_comment_write_counts(
    persist_stats: dict[str, int],
    *,
    total: int,
    inserted: int,
    changed: int | None = None,
) -> None:
    if total <= 0:
        return
    refreshed = max(total - inserted, 0)
    changed_count = max(0, int(inserted if changed is None else changed))
    changed_count = min(total, changed_count)
    persist_stats["comments_upserted"] = int(persist_stats.get("comments_upserted") or 0) + total
    persist_stats["comments_inserted"] = int(persist_stats.get("comments_inserted") or 0) + inserted
    persist_stats["comments_refreshed"] = int(persist_stats.get("comments_refreshed") or 0) + refreshed
    persist_stats["comments_changed"] = int(persist_stats.get("comments_changed") or 0) + changed_count


def _repo_column_exists(repo: Any, table: str, column: str, *, conn: Any) -> bool:
    column_exists = getattr(repo, "_column_exists", None)
    if not callable(column_exists):
        return False
    try:
        return bool(column_exists("social", table, column, conn=conn))
    except TypeError:
        return bool(column_exists("social", table, column))


def _empty_persisted_reply_topology() -> dict[str, Any]:
    return {
        "stored_parent_comments": 0,
        "stored_child_replies": 0,
        "expected_child_replies": 0,
        "stored_reply_gap_total": 0,
        "stored_reply_gap_parent_count": 0,
        "stored_reply_gap_samples": [],
    }


def _load_persisted_instagram_reply_topology(
    *,
    repo: Any,
    post_id: str,
    conn: Any,
) -> dict[str, Any]:
    normalized_post_id = str(post_id or "").strip()
    if not normalized_post_id:
        return _empty_persisted_reply_topology()
    if not callable(getattr(repo, "_column_exists", None)):
        return _empty_persisted_reply_topology()

    has_parent_external_id = _repo_column_exists(
        repo,
        "instagram_comments",
        "parent_comment_external_id",
        conn=conn,
    )
    has_child_comment_count = _repo_column_exists(
        repo,
        "instagram_comments",
        "child_comment_count",
        conn=conn,
    )
    parent_external_parent_filter = (
        "and nullif(parent.parent_comment_external_id, '') is null" if has_parent_external_id else ""
    )
    reply_parent_match = "reply.parent_comment_id = parent.id"
    if has_parent_external_id:
        reply_parent_match = (
            "(reply.parent_comment_id = parent.id "
            "or nullif(reply.parent_comment_external_id, '') = parent.comment_id)"
        )
    expected_child_count_expr = (
        "greatest(coalesce(parent.reply_count, 0), coalesce(parent.child_comment_count, 0))"
        if has_child_comment_count
        else "coalesce(parent.reply_count, 0)"
    )
    base_ctes = f"""
        with parents as materialized (
          select
            parent.id,
            parent.comment_id,
            {expected_child_count_expr}::int as expected_reply_count
          from social.instagram_comments parent
          where parent.post_id = %s::uuid
            and coalesce(parent.is_reply, false) = false
            and parent.parent_comment_id is null
            {parent_external_parent_filter}
            and coalesce(parent.is_missing, false) = false
            and parent.deleted_at is null
            and nullif(parent.comment_id, '') is not null
        ),
        reply_counts as materialized (
          select
            parent.id,
            count(reply.id)::int as saved_reply_count
          from parents parent
          left join social.instagram_comments reply
            on reply.post_id = %s::uuid
           and ({reply_parent_match})
           and coalesce(reply.is_missing, false) = false
           and reply.deleted_at is null
           and nullif(reply.comment_id, '') is not null
          group by parent.id
        ),
        parent_gaps as materialized (
          select
            parent.comment_id,
            greatest(parent.expected_reply_count, 0)::int as expected_reply_count,
            coalesce(reply_counts.saved_reply_count, 0)::int as saved_reply_count,
            greatest(
              parent.expected_reply_count - coalesce(reply_counts.saved_reply_count, 0),
              0
            )::int as missing_reply_count
          from parents parent
          left join reply_counts on reply_counts.id = parent.id
        )
        """
    totals = pg.fetch_one(
        f"""
        {base_ctes}
        select
          count(*)::int as stored_parent_comments,
          coalesce(sum(saved_reply_count), 0)::int as stored_child_replies,
          coalesce(sum(expected_reply_count), 0)::int as expected_child_replies,
          coalesce(sum(missing_reply_count), 0)::int as stored_reply_gap_total,
          count(*) filter (where missing_reply_count > 0)::int as stored_reply_gap_parent_count
        from parent_gaps
        """,
        [normalized_post_id, normalized_post_id],
        conn=conn,
    ) or {}
    samples = pg.fetch_all(
        f"""
        {base_ctes}
        select
          comment_id,
          expected_reply_count,
          saved_reply_count,
          missing_reply_count
        from parent_gaps
        where missing_reply_count > 0
        order by missing_reply_count desc, comment_id asc
        limit 10
        """,
        [normalized_post_id, normalized_post_id],
        conn=conn,
    )
    return {
        "stored_parent_comments": int(totals.get("stored_parent_comments") or 0),
        "stored_child_replies": int(totals.get("stored_child_replies") or 0),
        "expected_child_replies": int(totals.get("expected_child_replies") or 0),
        "stored_reply_gap_total": int(totals.get("stored_reply_gap_total") or 0),
        "stored_reply_gap_parent_count": int(totals.get("stored_reply_gap_parent_count") or 0),
        "stored_reply_gap_samples": [
            {
                "comment_id": str(row.get("comment_id") or "").strip(),
                "expected_reply_count": int(row.get("expected_reply_count") or 0),
                "saved_reply_count": int(row.get("saved_reply_count") or 0),
                "missing_reply_count": int(row.get("missing_reply_count") or 0),
            }
            for row in (samples or [])
            if str(row.get("comment_id") or "").strip()
        ],
    }


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
    source_scope: str = "network",
    enable_media_followups: bool = True,
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
                enable_media_followups=enable_media_followups,
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
            enable_media_followups=enable_media_followups,
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
            enable_media_followups=enable_media_followups,
            conn=conn,
        )
    if isinstance(is_complete, bool) and is_complete:
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
    reply_topology = _load_persisted_instagram_reply_topology(repo=repo, post_id=post_id, conn=conn)
    return PersistedInstagramComments(
        post_id=post_id,
        stored_total_comments=int(stored_total or 0),
        comments_upserted=comments_upserted,
        comments_marked_missing=comments_marked_missing,
        comment_media_mirror_jobs_enqueued=int(persist_stats.get("comment_media_mirror_jobs_enqueued") or 0),
        comment_media_mirror_job_enqueue_errors=int(persist_stats.get("comment_media_mirror_job_enqueue_errors") or 0),
        comments_inserted=int(persist_stats.get("comments_inserted") or 0),
        comments_refreshed=int(persist_stats.get("comments_refreshed") or 0),
        comments_changed=int(persist_stats.get("comments_changed") or 0),
        stored_parent_comments=int(reply_topology.get("stored_parent_comments") or 0),
        stored_child_replies=int(reply_topology.get("stored_child_replies") or 0),
        expected_child_replies=int(reply_topology.get("expected_child_replies") or 0),
        stored_reply_gap_total=int(reply_topology.get("stored_reply_gap_total") or 0),
        stored_reply_gap_parent_count=int(reply_topology.get("stored_reply_gap_parent_count") or 0),
        stored_reply_gap_samples=list(reply_topology.get("stored_reply_gap_samples") or []),
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
    enable_media_followups: bool,
    conn: Any,
) -> int:
    has_profile_pic = repo._column_exists("social", "instagram_comments", "author_profile_pic_url")
    has_verified = repo._column_exists("social", "instagram_comments", "author_is_verified")
    has_media = repo._column_exists("social", "instagram_comments", "media_urls")
    has_hosted_media = repo._column_exists("social", "instagram_comments", "hosted_media_urls")
    has_media_mirror_status = repo._column_exists("social", "instagram_comments", "media_mirror_status")
    has_media_mirror_error = repo._column_exists("social", "instagram_comments", "media_mirror_error")
    # Phase 2: Apify-source owner-metadata columns. Each gated independently so
    # partial migrations remain safe.
    has_comment_url = repo._column_exists("social", "instagram_comments", "comment_url")
    has_author_fbid_v2 = repo._column_exists("social", "instagram_comments", "author_fbid_v2")
    has_author_is_mentionable = repo._column_exists("social", "instagram_comments", "author_is_mentionable")
    has_author_is_private = repo._column_exists("social", "instagram_comments", "author_is_private")
    has_author_latest_reel_media = repo._column_exists("social", "instagram_comments", "author_latest_reel_media")
    has_author_profile_pic_id = repo._column_exists("social", "instagram_comments", "author_profile_pic_id")
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
        raw_data_for_write = getattr(repo, "_instagram_comment_raw_data_for_write", None)
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
            "raw_data": (
                raw_data_for_write(comment_obj)
                if callable(raw_data_for_write)
                else comment_obj.to_dict() if hasattr(comment_obj, "to_dict") else {}
            ),
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
        media_urls = [str(url).strip() for url in (getattr(comment_obj, "media_urls", []) or []) if str(url).strip()]
        if has_media:
            payload["media_urls"] = media_urls
        if has_hosted_media:
            payload["hosted_media_urls"] = [
                str(url).strip() for url in (getattr(comment_obj, "hosted_media_urls", []) or []) if str(url).strip()
            ]
        if has_media_mirror_status:
            payload["media_mirror_status"] = "deferred" if media_urls else None
        if has_media_mirror_error:
            payload["media_mirror_error"] = (
                None
                if not media_urls
                else "season_context_missing"
                if enable_media_followups
                else "media_followups_disabled"
            )
        # Phase 2: write Apify-source owner-metadata columns when present.
        if has_comment_url:
            payload["comment_url"] = (
                str(getattr(comment_obj, "comment_url", "") or "").strip() or None
            )
        if has_author_fbid_v2:
            payload["author_fbid_v2"] = (
                str(getattr(comment_obj, "owner_fbid_v2", "") or "").strip() or None
            )
        if has_author_is_mentionable:
            payload["author_is_mentionable"] = getattr(comment_obj, "owner_is_mentionable", None)
        if has_author_is_private:
            payload["author_is_private"] = getattr(comment_obj, "owner_is_private", None)
        if has_author_latest_reel_media:
            payload["author_latest_reel_media"] = getattr(comment_obj, "owner_latest_reel_media", None)
        if has_author_profile_pic_id:
            payload["author_profile_pic_id"] = (
                str(getattr(comment_obj, "owner_profile_pic_id", "") or "").strip() or None
            )
        reply_depth = _comment_effective_reply_depth(
            comment_obj,
            parent_external_id,
            fallback=1 if parent_external_id else 0,
        )
        repo._apply_instagram_comment_queryable_columns(
            payload,
            comment_obj,
            parent_external_id=parent_external_id,
            reply_depth=reply_depth,
        )
        if parent_external_id is None:
            top_level.append(payload)
        else:
            payload["_parent_external_id"] = parent_external_id
            replies.append(payload)

    dedupe_payloads = getattr(repo, "_dedupe_instagram_comment_payloads_for_upsert", None)
    if callable(dedupe_payloads):
        top_level = dedupe_payloads(top_level)
        replies = dedupe_payloads(replies)

    ext_to_db: dict[str, str] = {}
    upserted = 0
    for batch in (top_level, replies):
        if batch is replies:
            for payload in batch:
                parent_external_id = str(payload.pop("_parent_external_id", "") or "").strip()
                payload["parent_comment_id"] = ext_to_db.get(parent_external_id)
        load_baseline = getattr(repo, "_load_instagram_comment_write_baseline", None)
        count_new_or_changed = getattr(repo, "_count_new_or_changed_instagram_comment_payloads", None)
        write_baseline = load_baseline(batch, conn=conn) if callable(load_baseline) and batch else {}
        preserve_ranked = getattr(repo, "_preserve_existing_ranked_instagram_comment_values", None)
        if callable(preserve_ranked) and write_baseline:
            preserve_ranked(batch, write_baseline)
        batch_changed = (
            count_new_or_changed(batch, write_baseline)
            if callable(count_new_or_changed)
            else None
        )
        rows = (
            repo._pg_upsert_many(
                "instagram_comments",
                batch,
                conflict_col=["post_id", "comment_id"],
                conn=conn,
                include_inserted_flag=True,
            )
            if batch
            else []
        )
        batch_total, batch_inserted = _upsert_write_counts(rows)
        for row in rows:
            ext_id = str(row.get("comment_id") or "").strip()
            db_id = str(row.get("id") or "").strip()
            if ext_id and db_id:
                ext_to_db[ext_id] = db_id
        upserted += batch_total
        _record_comment_write_counts(persist_stats, total=batch_total, inserted=batch_inserted, changed=batch_changed)
    return upserted
