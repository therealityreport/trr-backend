"""Canonical Instagram persistence seam.

This module owns the import surface for Instagram post, catalog-post, and
comment persistence. The post/catalog implementations currently live in
`catalog_ingest`, while comment persistence still bridges to the legacy core
until that helper cluster can move without changing DB behavior.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

import trr_backend.socials.social_season_analytics_impl as _core
from trr_backend.socials.instagram import catalog_ingest as _catalog_ingest

_SAFE_COMMENT_COLUMN_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_COMMENT_WRITE_ID_FIELDS = frozenset({"post_id", "comment_id"})
_COMMENT_WRITE_IGNORED_FIELDS = frozenset(
    {
        "scraped_at",
        "last_seen_at",
        "last_seen_run_id",
        "job_id",
        "media_mirror_status",
        "media_mirror_error",
    }
)


def _upsert_instagram_post(*args: Any, **kwargs: Any) -> Any:
    return _catalog_ingest._upsert_instagram_post(*args, **kwargs)


def _shared_catalog_instagram_post_payload(*args: Any, **kwargs: Any) -> Any:
    return _catalog_ingest._shared_catalog_instagram_post_payload(*args, **kwargs)


def _batch_upsert_shared_catalog_instagram_posts(*args: Any, **kwargs: Any) -> Any:
    return _catalog_ingest._batch_upsert_shared_catalog_instagram_posts(*args, **kwargs)


def _instagram_comment_raw_payload(comment: Any) -> dict[str, Any]:
    if hasattr(comment, "to_dict"):
        value = comment.to_dict()
    else:
        value = getattr(comment, "raw_data", None)
    return dict(value) if isinstance(value, Mapping) else {}


def _instagram_comment_author_full_name(comment: Any, raw_data: Mapping[str, Any]) -> str | None:
    for candidate in (
        getattr(comment, "owner_full_name", None),
        getattr(comment, "author_full_name", None),
        raw_data.get("ownerFullName"),
    ):
        normalized = str(candidate or "").strip()
        if normalized:
            return normalized
    for key in ("owner", "user"):
        nested = raw_data.get(key)
        if isinstance(nested, Mapping):
            normalized = str(nested.get("full_name") or nested.get("fullName") or "").strip()
            if normalized:
                return normalized
    return None


def _instagram_comment_author_profile_pic_hd(comment: Any, raw_data: Mapping[str, Any]) -> str | None:
    for candidate in (
        getattr(comment, "owner_profile_pic_url_hd", None),
        getattr(comment, "author_profile_pic_url_hd", None),
        raw_data.get("ownerProfilePicUrlHd"),
    ):
        normalized = str(candidate or "").strip()
        if normalized:
            return normalized
    for key in ("owner", "user"):
        nested = raw_data.get(key)
        if isinstance(nested, Mapping):
            normalized = str(nested.get("profile_pic_url_hd") or nested.get("profilePicUrlHd") or "").strip()
            if normalized:
                return normalized
    return None


def _apply_instagram_comment_queryable_columns(
    payload: dict[str, Any],
    comment: Any,
    *,
    parent_external_id: str | None = None,
    reply_depth: int | None = None,
    source_snapshot_type: str = "full_comments_scrape",
) -> None:
    raw_data = _instagram_comment_raw_payload(comment)
    if _core._column_exists("social", "instagram_comments", "author_full_name"):
        payload["author_full_name"] = _instagram_comment_author_full_name(comment, raw_data)
    if _core._column_exists("social", "instagram_comments", "author_profile_pic_url_hd"):
        payload["author_profile_pic_url_hd"] = _instagram_comment_author_profile_pic_hd(comment, raw_data)
    if _core._column_exists("social", "instagram_comments", "parent_comment_external_id"):
        payload["parent_comment_external_id"] = str(parent_external_id or "").strip() or None
    if _core._column_exists("social", "instagram_comments", "reply_depth"):
        payload["reply_depth"] = max(0, int(reply_depth or 0))
    if _core._column_exists("social", "instagram_comments", "source_snapshot_type"):
        payload["source_snapshot_type"] = (
            str(getattr(comment, "source_snapshot_type", "") or source_snapshot_type).strip() or source_snapshot_type
        )


def _instagram_comment_effective_reply_depth(comment: Any, *, parent_external_id: str | None, fallback: int) -> int:
    try:
        parsed_depth = int(getattr(comment, "reply_depth", 0) or 0)
    except (TypeError, ValueError):
        parsed_depth = 0
    if parsed_depth > 0 or not parent_external_id:
        return max(0, parsed_depth)
    return max(0, int(fallback or 0))


def _flatten_instagram_comment_tree(
    comment: Any,
    *,
    parent_external_id: str | None = None,
) -> list[tuple[Any, str | None]]:
    """Flatten a comment tree into a list of (comment, parent_external_id) tuples."""
    external_id = str(getattr(comment, "comment_id", "") or "").strip()
    result: list[tuple[Any, str | None]] = [(comment, parent_external_id)]
    for reply in getattr(comment, "replies", []) or []:
        result.extend(
            _flatten_instagram_comment_tree(
                reply,
                parent_external_id=external_id if external_id else parent_external_id,
            )
        )
    return result


def _upsert_write_counts(rows: list[dict[str, Any]]) -> tuple[int, int]:
    total = len(rows)
    inserted = 0
    for row in rows:
        if bool(row.pop("__trr_inserted", False)):
            inserted += 1
    return total, inserted


def _normalize_comment_compare_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _normalize_comment_compare_value(inner)
            for key, inner in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, list):
        return [_normalize_comment_compare_value(item) for item in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, tuple):
        return tuple(_normalize_comment_compare_value(item) for item in value)
    return value


def _comment_write_compare_columns(payloads: list[dict[str, Any]]) -> list[str]:
    columns: set[str] = set()
    for payload in payloads:
        for column in payload:
            if column in _COMMENT_WRITE_ID_FIELDS or column in _COMMENT_WRITE_IGNORED_FIELDS:
                continue
            if column.startswith("_"):
                continue
            if _SAFE_COMMENT_COLUMN_RE.match(column):
                columns.add(column)
    return sorted(columns)


def _load_instagram_comment_write_baseline(
    payloads: list[dict[str, Any]],
    *,
    conn: Any | None = None,
) -> dict[tuple[str, str], dict[str, Any]]:
    post_ids = sorted({str(payload.get("post_id") or "").strip() for payload in payloads if payload.get("post_id")})
    comment_ids = sorted(
        {
            str(payload.get("comment_id") or "").strip()
            for payload in payloads
            if str(payload.get("comment_id") or "").strip()
        }
    )
    if not post_ids or not comment_ids:
        return {}
    compare_columns = _comment_write_compare_columns(payloads)
    selected_columns = ", ".join(compare_columns)
    selected_sql = f", {selected_columns}" if selected_columns else ""
    rows = _core.pg.fetch_all(
        f"""
        select post_id::text as post_id, comment_id{selected_sql}
        from social.instagram_comments
        where post_id::text = any(%s)
          and comment_id = any(%s)
        """,
        [post_ids, comment_ids],
        conn=conn,
    )
    return {(str(row.get("post_id") or "").strip(), str(row.get("comment_id") or "").strip()): row for row in rows}


def _comment_payload_has_meaningful_change(payload: dict[str, Any], existing: Mapping[str, Any]) -> bool:
    for column in _comment_write_compare_columns([payload]):
        if column not in existing:
            continue
        if _normalize_comment_compare_value(payload.get(column)) != _normalize_comment_compare_value(
            existing.get(column)
        ):
            return True
    return False


def _count_new_or_changed_instagram_comment_payloads(
    payloads: list[dict[str, Any]],
    baseline: Mapping[tuple[str, str], Mapping[str, Any]],
) -> int:
    changed = 0
    for payload in payloads:
        key = (str(payload.get("post_id") or "").strip(), str(payload.get("comment_id") or "").strip())
        existing = baseline.get(key)
        if not existing or _comment_payload_has_meaningful_change(payload, existing):
            changed += 1
    return changed


def _record_comment_write_counts(
    persist_stats: dict[str, int] | None,
    *,
    total: int,
    inserted: int,
    changed: int | None = None,
) -> None:
    if persist_stats is None or total <= 0:
        return
    refreshed = max(total - inserted, 0)
    changed_count = max(0, int(inserted if changed is None else changed))
    changed_count = min(total, changed_count)
    persist_stats["comments_upserted"] = int(persist_stats.get("comments_upserted") or 0) + total
    persist_stats["comments_inserted"] = int(persist_stats.get("comments_inserted") or 0) + inserted
    persist_stats["comments_refreshed"] = int(persist_stats.get("comments_refreshed") or 0) + refreshed
    persist_stats["comments_changed"] = int(persist_stats.get("comments_changed") or 0) + changed_count


def _instagram_comment_cache_attr(name: str) -> bool | None:
    return getattr(_core, name, None)


def _set_instagram_comment_cache_attr(name: str, value: bool) -> bool:
    setattr(_core, name, value)
    return value


def _cached_instagram_comment_column(cache_name: str, column_name: str) -> bool:
    cached = _instagram_comment_cache_attr(cache_name)
    if cached is None:
        cached = _set_instagram_comment_cache_attr(
            cache_name,
            _core._column_exists("social", "instagram_comments", column_name),
        )
    return bool(cached)


def _upsert_instagram_comment_tree(
    context: Any,
    *,
    job_id: str | None,
    run_id: str | None,
    account: str,
    post_id: str,
    comment: Any,
    parent_comment_db_id: str | None = None,
    parent_comment_external_id: str | None = None,
    reply_depth: int = 0,
    observed_comment_ids: set[str] | None = None,
    persist_stats: dict[str, int] | None = None,
    source_scope: str = "network",
    enable_media_followups: bool = True,
    conn: Any | None = None,
) -> int:
    if persist_stats is not None:
        persist_stats["comments_fetched"] = int(persist_stats.get("comments_fetched") or 0) + 1

    created_at = _core._parse_instagram_time(getattr(comment, "created_at", None))
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
            total += _upsert_instagram_comment_tree(
                context,
                job_id=job_id,
                run_id=run_id,
                account=account,
                post_id=post_id,
                comment=reply,
                parent_comment_db_id=parent_comment_db_id,
                parent_comment_external_id=parent_comment_external_id,
                reply_depth=reply_depth + 1,
                observed_comment_ids=observed_comment_ids,
                persist_stats=persist_stats,
                source_scope=source_scope,
                enable_media_followups=enable_media_followups,
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
        "text": getattr(comment, "text", ""),
        "likes": int(getattr(comment, "likes", 0) or 0),
        "is_reply": bool(getattr(comment, "is_reply", False)),
        "reply_count": int(getattr(comment, "reply_count", 0) or 0),
        "created_at": created_at,
        "scraped_at": _core._now_utc(),
        "raw_data": comment.to_dict() if hasattr(comment, "to_dict") else {},
        "season_id": context.season_id,
        "source_account": account,
    }
    if job_id:
        payload["job_id"] = job_id
    if _core._comment_lifecycle_supported("instagram_comments"):
        payload["is_missing"] = False
        payload["missing_at"] = None
        payload["last_seen_at"] = _core._now_utc()
        if run_id is not None:
            payload["last_seen_run_id"] = run_id
    if _core._column_exists("social", "instagram_comments", "author_profile_pic_url"):
        payload["author_profile_pic_url"] = str(getattr(comment, "owner_profile_pic_url", "") or "").strip() or None
    if _core._column_exists("social", "instagram_comments", "author_is_verified"):
        payload["author_is_verified"] = getattr(comment, "owner_is_verified", None)
    if _core._column_exists("social", "instagram_comments", "media_urls"):
        payload["media_urls"] = media_urls
    if _core._column_exists("social", "instagram_comments", "hosted_media_urls"):
        payload["hosted_media_urls"] = [
            str(url).strip() for url in (getattr(comment, "hosted_media_urls", []) or []) if str(url).strip()
        ]
    if media_urls and _core._column_exists("social", "instagram_comments", "media_mirror_status"):
        payload["media_mirror_status"] = "pending"
    if media_urls and _core._column_exists("social", "instagram_comments", "media_mirror_error"):
        payload["media_mirror_error"] = None
    if _core._column_exists("social", "instagram_comments", "comment_url"):
        payload["comment_url"] = str(getattr(comment, "comment_url", "") or "").strip() or None
    if _core._column_exists("social", "instagram_comments", "author_fbid_v2"):
        payload["author_fbid_v2"] = str(getattr(comment, "owner_fbid_v2", "") or "").strip() or None
    if _core._column_exists("social", "instagram_comments", "author_is_mentionable"):
        payload["author_is_mentionable"] = getattr(comment, "owner_is_mentionable", None)
    if _core._column_exists("social", "instagram_comments", "author_is_private"):
        payload["author_is_private"] = getattr(comment, "owner_is_private", None)
    if _core._column_exists("social", "instagram_comments", "author_latest_reel_media"):
        payload["author_latest_reel_media"] = getattr(comment, "owner_latest_reel_media", None)
    if _core._column_exists("social", "instagram_comments", "author_profile_pic_id"):
        payload["author_profile_pic_id"] = str(getattr(comment, "owner_profile_pic_id", "") or "").strip() or None
    effective_reply_depth = _instagram_comment_effective_reply_depth(
        comment,
        parent_external_id=parent_comment_external_id,
        fallback=reply_depth,
    )
    _apply_instagram_comment_queryable_columns(
        payload,
        comment,
        parent_external_id=parent_comment_external_id,
        reply_depth=effective_reply_depth,
    )
    write_baseline = _load_instagram_comment_write_baseline([payload], conn=conn) if persist_stats is not None else {}
    changed_count = (
        _count_new_or_changed_instagram_comment_payloads([payload], write_baseline)
        if persist_stats is not None
        else None
    )
    row = _core._pg_upsert(
        "instagram_comments",
        payload,
        conflict_col=["post_id", "comment_id"],
        conn=conn,
        include_inserted_flag=True,
    )
    comment_db_id = (row or {}).get("id")
    if persist_stats is not None and row:
        inserted = 1 if bool(row.pop("__trr_inserted", False)) else 0
        _record_comment_write_counts(persist_stats, total=1, inserted=inserted, changed=changed_count)
    if row and media_urls and enable_media_followups:
        try:
            mirror_job_id = _core._enqueue_platform_comment_media_mirror_job(
                context,
                platform="instagram",
                run_id=run_id,
                source_scope=source_scope,
                account=account,
                comment_row={**payload, **row},
                parent_job_id=job_id,
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
        total += _upsert_instagram_comment_tree(
            context,
            job_id=job_id,
            run_id=run_id,
            account=account,
            post_id=post_id,
            comment=reply,
            parent_comment_db_id=comment_db_id,
            parent_comment_external_id=comment_external_id,
            reply_depth=reply_depth + 1,
            observed_comment_ids=observed_comment_ids,
            persist_stats=persist_stats,
            source_scope=source_scope,
            enable_media_followups=enable_media_followups,
            conn=conn,
        )
    return total


def _batch_upsert_instagram_comments(
    context: Any,
    *,
    job_id: str | None,
    run_id: str | None,
    account: str,
    post_id: str,
    comments: list[Any],
    observed_comment_ids: set[str] | None = None,
    persist_stats: dict[str, int] | None = None,
    source_scope: str = "network",
    enable_media_followups: bool = True,
    conn: Any | None = None,
) -> int:
    """Batch upsert Instagram comments for a single post."""
    if not comments:
        return 0

    has_profile_pic = _cached_instagram_comment_column("_instagram_comment_has_profile_pic", "author_profile_pic_url")
    has_verified = _cached_instagram_comment_column("_instagram_comment_has_verified", "author_is_verified")
    has_media = _cached_instagram_comment_column("_instagram_comment_has_media", "media_urls")
    has_hosted_media = _cached_instagram_comment_column("_instagram_comment_has_hosted_media", "hosted_media_urls")
    has_media_mirror_status = _cached_instagram_comment_column(
        "_instagram_comment_has_media_mirror_status",
        "media_mirror_status",
    )
    has_media_mirror_error = _cached_instagram_comment_column(
        "_instagram_comment_has_media_mirror_error",
        "media_mirror_error",
    )
    has_comment_url = _cached_instagram_comment_column("_instagram_comment_has_comment_url", "comment_url")
    has_author_fbid_v2 = _cached_instagram_comment_column("_instagram_comment_has_author_fbid_v2", "author_fbid_v2")
    has_author_is_mentionable = _cached_instagram_comment_column(
        "_instagram_comment_has_author_is_mentionable",
        "author_is_mentionable",
    )
    has_author_is_private = _cached_instagram_comment_column(
        "_instagram_comment_has_author_is_private",
        "author_is_private",
    )
    has_author_latest_reel_media = _cached_instagram_comment_column(
        "_instagram_comment_has_author_latest_reel_media",
        "author_latest_reel_media",
    )
    has_author_profile_pic_id = _cached_instagram_comment_column(
        "_instagram_comment_has_author_profile_pic_id",
        "author_profile_pic_id",
    )
    has_lifecycle = _core._comment_lifecycle_supported("instagram_comments")

    flat: list[tuple[Any, str | None]] = []
    for comment in comments:
        flat.extend(_flatten_instagram_comment_tree(comment))

    top_level_payloads: list[dict[str, Any]] = []
    reply_payloads: list[dict[str, Any]] = []

    for comment_obj, parent_ext_id in flat:
        if persist_stats is not None:
            persist_stats["comments_fetched"] = int(persist_stats.get("comments_fetched") or 0) + 1

        external_id = str(getattr(comment_obj, "comment_id", "") or "").strip()
        if observed_comment_ids is not None and external_id:
            observed_comment_ids.add(external_id)
        if not external_id:
            if persist_stats is not None:
                persist_stats["comments_skipped_missing_id"] = (
                    int(persist_stats.get("comments_skipped_missing_id") or 0) + 1
                )
            continue

        created_at = _core._parse_instagram_time(getattr(comment_obj, "created_at", None))
        now = _core._now_utc()
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
            "created_at": created_at,
            "scraped_at": now,
            "raw_data": comment_obj.to_dict() if hasattr(comment_obj, "to_dict") else {},
            "season_id": context.season_id,
            "source_account": account,
        }
        if job_id:
            payload["job_id"] = job_id
        if has_lifecycle:
            payload["is_missing"] = False
            payload["missing_at"] = None
            payload["last_seen_at"] = now
            if run_id is not None:
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
            payload["media_mirror_status"] = "pending" if media_urls else None
        if has_media_mirror_error:
            payload["media_mirror_error"] = None
        if has_comment_url:
            payload["comment_url"] = str(getattr(comment_obj, "comment_url", "") or "").strip() or None
        if has_author_fbid_v2:
            payload["author_fbid_v2"] = str(getattr(comment_obj, "owner_fbid_v2", "") or "").strip() or None
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
        effective_reply_depth = _instagram_comment_effective_reply_depth(
            comment_obj,
            parent_external_id=parent_ext_id,
            fallback=1 if parent_ext_id else 0,
        )
        _apply_instagram_comment_queryable_columns(
            payload,
            comment_obj,
            parent_external_id=parent_ext_id,
            reply_depth=effective_reply_depth,
        )

        if parent_ext_id is None:
            top_level_payloads.append(payload)
        else:
            payload["_parent_external_id"] = parent_ext_id
            reply_payloads.append(payload)

    total_upserted = 0
    batch_size = _core._BATCH_UPSERT_COMMENT_SIZE

    ext_id_to_db_id: dict[str, str] = {}
    for i in range(0, len(top_level_payloads), batch_size):
        batch = top_level_payloads[i : i + batch_size]
        batch_by_comment_id = {str(item.get("comment_id") or ""): item for item in batch}
        write_baseline = _load_instagram_comment_write_baseline(batch, conn=conn) if persist_stats is not None else {}
        batch_changed = (
            _count_new_or_changed_instagram_comment_payloads(batch, write_baseline)
            if persist_stats is not None
            else None
        )
        rows = _core._pg_upsert_many(
            "instagram_comments",
            batch,
            conflict_col=["post_id", "comment_id"],
            conn=conn,
            include_inserted_flag=True,
        )
        batch_total, batch_inserted = _upsert_write_counts(rows)
        for row in rows:
            db_id = str(row.get("id") or "")
            ext_id = str(row.get("comment_id") or "")
            if db_id and ext_id:
                ext_id_to_db_id[ext_id] = db_id
                payload = batch_by_comment_id.get(ext_id) or {}
                media_urls = list(payload.get("media_urls") or [])
                if media_urls and enable_media_followups:
                    try:
                        mirror_job_id = _core._enqueue_platform_comment_media_mirror_job(
                            context,
                            platform="instagram",
                            run_id=run_id,
                            source_scope=source_scope,
                            account=account,
                            comment_row={**payload, **row},
                            parent_job_id=job_id,
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
        total_upserted += batch_total
        _record_comment_write_counts(persist_stats, total=batch_total, inserted=batch_inserted, changed=batch_changed)

    for payload in reply_payloads:
        parent_ext = payload.pop("_parent_external_id", None)
        if parent_ext and parent_ext in ext_id_to_db_id:
            payload["parent_comment_id"] = ext_id_to_db_id[parent_ext]

    for i in range(0, len(reply_payloads), batch_size):
        batch = reply_payloads[i : i + batch_size]
        batch_by_comment_id = {str(item.get("comment_id") or ""): item for item in batch}
        write_baseline = _load_instagram_comment_write_baseline(batch, conn=conn) if persist_stats is not None else {}
        batch_changed = (
            _count_new_or_changed_instagram_comment_payloads(batch, write_baseline)
            if persist_stats is not None
            else None
        )
        rows = _core._pg_upsert_many(
            "instagram_comments",
            batch,
            conflict_col=["post_id", "comment_id"],
            conn=conn,
            include_inserted_flag=True,
        )
        batch_total, batch_inserted = _upsert_write_counts(rows)
        for row in rows:
            db_id = str(row.get("id") or "")
            ext_id = str(row.get("comment_id") or "")
            if db_id and ext_id:
                ext_id_to_db_id[ext_id] = db_id
                payload = batch_by_comment_id.get(ext_id) or {}
                media_urls = list(payload.get("media_urls") or [])
                if media_urls and enable_media_followups:
                    try:
                        mirror_job_id = _core._enqueue_platform_comment_media_mirror_job(
                            context,
                            platform="instagram",
                            run_id=run_id,
                            source_scope=source_scope,
                            account=account,
                            comment_row={**payload, **row},
                            parent_job_id=job_id,
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
        total_upserted += batch_total
        _record_comment_write_counts(persist_stats, total=batch_total, inserted=batch_inserted, changed=batch_changed)

    return total_upserted


__all__ = [
    "_batch_upsert_instagram_comments",
    "_batch_upsert_shared_catalog_instagram_posts",
    "_shared_catalog_instagram_post_payload",
    "_upsert_instagram_comment_tree",
    "_upsert_instagram_post",
]
