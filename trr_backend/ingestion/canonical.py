"""Independent Instagram persistence in the caller's fenced transaction.

No network, queue dispatch, transaction commit, or missing-comment reconciliation
occurs here. Source payloads must already be sanitized. Observation times belong
to the capture, including during replay. Schema mismatches and stale observations
raise; callers must roll back the transaction and must not issue a success receipt.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from psycopg2 import sql
from psycopg2.extras import Json, RealDictCursor

from trr_backend.socials.instagram.post_normalizer import (
    InstagramComment,
    InstagramPost,
    normalize_instagram_comment,
    normalize_instagram_post,
)


class CanonicalWriteError(ValueError):
    """A canonical write cannot safely preserve its required semantics."""


class OlderObservation(CanonicalWriteError):  # noqa: N818
    """Captured source predates a stored observation; replay must not overwrite it."""


# Source fields with maintained queryable consumers. Omitted/empty fields preserve
# stored metadata; false and zero are observations, not missing values.
_POST_FIELDS = """
post_format duration_seconds metadata_source metadata_scraped_at
metadata_last_attempted_at metadata_last_failed_at metadata_consecutive_failures metadata_error
product_type video_play_count alt_text width height is_comments_disabled
source_input_url source_post_id permalink caption_id caption_is_edited caption_has_translation
owner_user_id owner_username owner_profile_pic_url owner_profile_pic_url_hd owner_full_name owner_is_verified
location_id location_name location_raw original_width original_height like_and_view_counts_disabled
comments_disabled commenting_disabled_for_viewer media_repost_count is_paid_partnership is_advertisement
can_viewer_reshare has_audio audio_url music_info video_duration asset_manifest
hosted_thumbnail_url hosted_media_urls media_mirror_status media_mirror_error media_mirror_attempt_count
media_mirror_last_attempt_at media_mirror_last_job_id hosted_owner_profile_pic_url hosted_tagged_profile_pics
profile_pic_mirror_status profile_pic_mirror_error fb_comment_count fb_like_count is_shared_to_fb
crosspost_metadata social_context facebook_post_id facebook_post_url facebook_crosspost_observed_at
facebook_crosspost_source
""".split()
_COMMENT_FIELDS = """
media_urls hosted_media_urls media_mirror_status media_mirror_error media_mirror_attempt_count
media_mirror_last_attempt_at media_mirror_last_job_id hosted_author_profile_pic_url comment_url
""".split()
_COMMENT_METADATA = {
    "is_covered": "isCovered",
    "is_ranked": "isRanked",
    "comment_index": "commentIndex",
    "phase": "api_phase",
    "did_report_as_spam": "didReportAsSpam",
    "status": "status",
    "is_edited": "isEdited",
    "is_pinned": "isPinned",
    "meta_ai_comment_type": "metaAiCommentType",
    "child_comment_count": "childCommentCount",
    "liked_by_media_coauthors": "likedByMediaCoauthors",
    "cursor_min_id": "cursorMinId",
    "cursor_param": "cursorParam",
    "cursor_payload": "cursorPayload",
    "comment_filter_param": "commentFilterParam",
}
_POST_ASSIGNMENT_FIELDS = frozenset({"show_id", "season_id", "person_id", "job_id"})
_COMMENT_ASSIGNMENT_FIELDS = frozenset({"season_id", "job_id"})
_POST_TIMESTAMP_FIELDS = frozenset(
    {
        "metadata_scraped_at",
        "metadata_last_attempted_at",
        "metadata_last_failed_at",
        "media_mirror_last_attempt_at",
        "facebook_crosspost_observed_at",
    }
)


def _time(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, bool):
        raise CanonicalWriteError("Boolean source timestamps are invalid")
    elif isinstance(value, (int, float)):
        result = datetime.fromtimestamp(value, UTC)
    elif str(value).strip().isdigit():
        result = datetime.fromtimestamp(int(str(value).strip()), UTC)
    else:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise CanonicalWriteError("Observation and source timestamps must include a timezone")
    return result.astimezone(UTC)


def _present(value: Any) -> bool:
    return value is not None and value != "" and value != [] and value != {}


def _first(source: Mapping[str, Any], *keys: str) -> Any:
    return next((source[key] for key in keys if key in source and source[key] is not None), None)


def _number(value: Any) -> int | None:
    if isinstance(value, Mapping):
        value = _first(value, "count", "total_count", "value")
    if value is None or isinstance(value, bool):
        return None
    try:
        return max(0, int(str(value).replace(",", "")))
    except (ValueError, TypeError):
        return None


def _merge(existing: Mapping[str, Any], incoming: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(existing)
    for key, value in incoming.items():
        if not _present(value):
            continue
        # Coverage and its original capture reference form one observation.
        # A newer proof must never inherit a nested reference from an older
        # capture when the incoming proof omits that optional reference.
        if key == "detail_snapshot" and isinstance(value, Mapping):
            value = dict(value)
        elif isinstance(value, Mapping) and isinstance(result.get(key), Mapping):
            value = _merge(result[key], value)
        result[key] = value
    return result


def _adapt(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return Json(
            value,
            dumps=lambda obj: json.dumps(
                obj, default=lambda item: item.isoformat() if isinstance(item, datetime) else str(item)
            ),
        )
    return value


def _row(conn: Any, table: str, keys: Mapping[str, Any]) -> dict[str, Any]:
    predicates = sql.SQL(" and ").join(sql.SQL("{}=%s").format(sql.Identifier(k)) for k in keys)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            sql.SQL("select * from social.{} where {} for update").format(sql.Identifier(table), predicates),
            list(keys.values()),
        )
        return dict(cur.fetchone() or {})


def _upsert(conn: Any, table: str, payload: Mapping[str, Any], conflict: Sequence[str]) -> dict[str, Any]:
    columns = list(payload)
    updates = [key for key in columns if key not in {*conflict, "id", "first_seen_at", "created_at"}]
    query = sql.SQL("insert into social.{} ({}) values ({}) on conflict ({}) do update set {} returning *").format(
        sql.Identifier(table),
        sql.SQL(",").join(map(sql.Identifier, columns)),
        sql.SQL(",").join(sql.Placeholder() for _ in columns),
        sql.SQL(",").join(map(sql.Identifier, conflict)),
        sql.SQL(",").join(sql.SQL("{}=excluded.{}").format(sql.Identifier(k), sql.Identifier(k)) for k in updates),
    )
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, [_adapt(payload[k]) for k in columns])
        return dict(cur.fetchone())


def _guard(conn: Any, identity: str, observed_at: datetime) -> datetime:
    observed_at = _time(observed_at)
    if observed_at is None:
        raise CanonicalWriteError("Original observed_at is required")
    if conn is None or conn.autocommit:
        raise CanonicalWriteError("An explicit non-autocommit connection is required")
    # Cross-account runs and replay share canonical identities. Run/target fences
    # alone do not serialize them. The transaction lock also covers absent rows.
    with conn.cursor() as cur:
        cur.execute("select pg_advisory_xact_lock(hashtextextended(%s, 0))", ("instagram:" + identity,))
    return observed_at


def _reject_older(observed_at: datetime, *rows: Mapping[str, Any]) -> None:
    for row in rows:
        for key in ("scraped_at", "last_seen_at", "last_scraped_at", "metadata_scraped_at", "payload_updated_at"):
            stamp = _time(row.get(key))
            if stamp and observed_at < stamp:
                raise OlderObservation(f"Source observation predates stored {key}")


def _sidecar(
    conn: Any, table: str, key: str, identity: str, payload: Mapping[str, Any], observed_at: datetime, *, assets: bool
) -> None:
    values = {
        key: identity,
        "raw_data": payload.get("raw_data") or {},
        "child_posts_data": payload.get("child_posts_data") or [],
        "payload_updated_at": observed_at,
    }
    if assets:
        values["asset_manifest"] = payload.get("asset_manifest") or {}
    _upsert(conn, table, values, [key])


def normalize_post_source(source_payload: Mapping[str, Any], *, account: str) -> InstagramPost:
    """Adapt the maintained scraper serializer and native source shapes explicitly."""
    source = dict(source_payload)
    if "post_type" in source and "shortcode" in source:
        source.setdefault("type", source["post_type"])
        source.setdefault("owner", source.get("owner_detail") or {"username": source.get("username")})
        if source.get("tagged_users_detail"):
            source.setdefault("taggedUsers", source["tagged_users_detail"])
        if source.get("collaborators_detail"):
            source["coauthor_producers"] = source["collaborators_detail"]
    return normalize_instagram_post(source, account_handle=account)


def normalize_comment_source(
    source_payload: Mapping[str, Any], *, parent_comment_id: str | None = None
) -> InstagramComment:
    """Adapt serialized transport comments while retaining full original evidence."""
    source = dict(source_payload)
    source.setdefault("id", source.get("comment_id"))
    if "username" in source and not (source.get("owner") or source.get("user")):
        source["owner"] = {
            "username": source.get("username"),
            "id": source.get("user_id"),
            "full_name": source.get("owner_full_name"),
            "profile_pic_url": source.get("owner_profile_pic_url"),
            "profile_pic_url_hd": source.get("owner_profile_pic_url_hd"),
            "is_verified": source.get("owner_is_verified"),
        }
    children = source.get("replies") or source.get("child_comments") or []
    source.pop("child_comments", None)
    source["replies"] = []
    source_created_at = _first(source, "created_at", "timestamp")
    if source_created_at is not None:
        stamp = _time(source_created_at)
        source["created_at"] = int(stamp.timestamp()) if stamp else None
    comment = normalize_instagram_comment(
        source,
        parent_comment_id=parent_comment_id
        or source.get("parent_comment_external_id")
        or source.get("parent_comment_id"),
        source_snapshot_type=str(source.get("source_snapshot_type") or "full_comments_scrape"),
    )
    comment.raw_data = dict(source_payload)
    comment.replies = [normalize_comment_source(child, parent_comment_id=comment.comment_id) for child in children]
    comment.replies_count = (
        _number(_first(source_payload, "child_comment_count", "repliesCount", "reply_count", "replies_count")) or 0
    )
    for key in ("owner", "user"):
        raw_owner = source_payload.get(key)
        if isinstance(raw_owner, Mapping):
            hd = raw_owner.get("hd_profile_pic_url_info")
            if isinstance(hd, Mapping) and hd.get("url"):
                comment.author.profile_pic_url_hd = comment.author.profile_pic_url_hd or hd["url"]
                comment.author.profile_pic_url = comment.author.profile_pic_url or hd["url"]
    return comment


def _child_payloads(normalized: InstagramPost, node: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Keep the carousel shape consumed by the existing media/viewer paths."""
    result = []
    actors = node.get("childPosts") or []
    for child in normalized.child_posts:
        image = next((v.url for v in child.media_variants if v.media_type == "image"), None)
        video = next((v.url for v in child.media_variants if v.media_type == "video"), None)
        raw = actors[child.slide_index] if child.slide_index < len(actors) else {}
        kind = raw.get("type") or raw.get("__typename")
        if not kind:
            edges = (node.get("edge_sidecar_to_children") or {}).get("edges") or []
            raw = edges[child.slide_index].get("node") if child.slide_index < len(edges) else {}
            kind = str(raw.get("__typename") or "").replace("Graph", "") or ("Video" if video else "Image")
        result.append(
            {
                "slide_index": child.slide_index,
                "type": kind,
                "display_url": image,
                "video_url": video,
                "width": child.width,
                "height": child.height,
                "alt": child.alt_text,
                "tagged_users_detail": [u.to_dict() for u in child.tagged_users],
            }
        )
    return result


def _post_payload(
    normalized: InstagramPost, source: Mapping[str, Any], account: str, observed_at: datetime
) -> dict[str, Any]:
    """Map native API/GraphQL/Apify and scraper DTO source without importing its runtime."""
    node = dict(normalized.raw_data)
    # The DTO retains the original unwrapped node; source may be its envelope or
    # a serialized scraper DTO. Explicit source values have the capture's priority.
    node.update(source)
    owner = normalized.owner
    raw_owner = _first(node, "owner_detail", "owner", "user") or {}
    if not isinstance(raw_owner, Mapping):
        raw_owner = {}
    posted_at_source = _first(node, "taken_at", "taken_at_timestamp", "timestamp", "date_time", "datePublished")
    posted_at = None if posted_at_source in (None, "", 0, "0") else _time(posted_at_source)
    payload: dict[str, Any] = {
        "shortcode": normalized.shortcode,
        "media_id": normalized.source_id,
        "username": (owner.username if owner else None) or node.get("username") or account,
        "source_account": account,
        "scraped_at": observed_at,
        "caption": normalized.caption.text or (node.get("caption") if isinstance(node.get("caption"), str) else None),
        "media_type": normalized.media_type or node.get("post_type"),
        "media_urls": node.get("media_urls") or normalized.media_urls,
        "thumbnail_url": node.get("thumbnail_url") or normalized.thumbnail_url,
        "posted_at": posted_at,
        "hashtags": normalized.hashtags,
        "mentions": normalized.mentions,
        "profile_tags": node.get("profile_tags") or [u.username for u in normalized.tagged_users if u.username],
        "collaborators": [u.username for u in normalized.collaborators if u.username],
        "tagged_users_detail": node.get("tagged_users_detail") or [u.to_dict() for u in normalized.tagged_users],
        "collaborators_detail": node.get("collaborators_detail") or [u.to_dict() for u in normalized.collaborators],
        "owner_user_id": owner.user_id if owner else None,
        "owner_username": owner.username if owner else None,
        "owner_full_name": owner.full_name if owner else None,
        "owner_profile_pic_url": owner.profile_pic_url if owner else None,
        "owner_profile_pic_url_hd": owner.profile_pic_url_hd if owner else None,
        "owner_is_verified": owner.is_verified if owner else None,
        "width": normalized.width,
        "height": normalized.height,
        "alt_text": normalized.alt_text,
        "music_info": normalized.music_info,
        "audio_url": normalized.audio_url,
        "video_duration": normalized.video_duration,
        "video_play_count": normalized.video_play_count,
        "media_repost_count": normalized.media_repost_count,
        "permalink": normalized.permalink,
        "caption_id": normalized.caption.caption_id,
        "caption_is_edited": normalized.caption.is_edited,
        "caption_has_translation": normalized.caption.has_translation,
        "child_posts_data": node.get("child_posts_data") or _child_payloads(normalized, node),
        "raw_data": dict(source),
    }
    # Serializer DTO aliases absent from the independent normalizer.
    if isinstance(node.get("collaborators"), list) and all(isinstance(v, str) for v in node["collaborators"]):
        payload["collaborators"] = node["collaborators"]
    for attr, dest in (
        ("user_id", "owner_user_id"),
        ("full_name", "owner_full_name"),
        ("profile_pic_url", "owner_profile_pic_url"),
        ("profile_pic_url_hd", "owner_profile_pic_url_hd"),
        ("is_verified", "owner_is_verified"),
    ):
        if _present(raw_owner.get(attr)):
            payload[dest] = raw_owner[attr]
    if normalized.location:
        payload.update(
            location_id=normalized.location.location_id,
            location_name=normalized.location.name,
            location_raw=node.get("location") or normalized.to_dict()["location"],
        )
    flags = {
        "comments_disabled": "comments_disabled",
        "like_count_disabled": "like_and_view_counts_disabled",
        "paid_partnership": "is_paid_partnership",
        "advertisement": "is_advertisement",
    }
    for flag, column in flags.items():
        if flag in normalized.flags:
            payload[column] = normalized.flags[flag]
    for key in _POST_FIELDS:
        if _present(node.get(key)):
            payload[key] = _time(node[key]) if key in _POST_TIMESTAMP_FIELDS else node[key]
    for key, aliases in {
        "source_input_url": ("input_url", "inputUrl"),
        "source_post_id": ("source_post_id", "pk", "id"),
        "likes": ("like_count", "likesCount", "likes_count", "edge_liked_by", "edge_media_preview_like", "likes"),
        "comments_count": (
            "comment_count",
            "commentsCount",
            "comments_count",
            "edge_media_to_comment",
            "edge_media_to_parent_comment",
            "comments",
        ),
        "views": ("video_views_observed", "video_view_count", "videoViewCount", "view_count", "play_count"),
    }.items():
        value = _first(node, *aliases)
        if key in {"likes", "comments_count", "views"}:
            value = _number(value)
        if value is not None:
            payload[key] = value
    reported_comments = []
    for scope in [
        node,
        *(value for key in ("media", "node", "graphql", "metrics") if isinstance(value := node.get(key), Mapping)),
    ]:
        for key in (
            "comments_count",
            "comments",
            "comment_count",
            "commentsCount",
            "edge_media_to_comment",
            "edge_media_to_parent_comment",
            "edge_media_preview_comment",
        ):
            value = _number(scope.get(key))
            if value is not None:
                reported_comments.append(value)
    if reported_comments:
        payload["comments_count"] = max(reported_comments)
    if "video_views_observed" in node and node["video_views_observed"] is None:
        payload.pop("views", None)

    def hidden(*keys: str) -> bool:
        return any(str(node.get(k) or "").lower() in {"true", "1", "yes", "on", "y", "t"} for k in keys)

    if hidden("like_and_view_counts_disabled", "hide_like_and_view_counts", "like_count_hidden", "likes_hidden"):
        payload.pop("likes", None)
    if hidden(
        "like_and_view_counts_disabled",
        "hide_like_and_view_counts",
        "like_count_hidden",
        "likes_hidden",
        "view_count_hidden",
        "views_hidden",
    ):
        payload.pop("views", None)
    if hidden("comment_count_hidden", "comments_count_hidden"):
        payload.pop("comments_count", None)
    view_metrics = {"observed_at": observed_at.isoformat()}
    if node.get("video_views_source"):
        view_metrics["source"] = node["video_views_source"]
    if payload.get("views") is not None:
        view_metrics["observed_count"] = payload["views"]
    candidates = node.get("video_views_raw_candidates")
    if isinstance(candidates, list) and candidates:
        view_metrics["raw_candidates"] = [
            {"source": item.get("source"), "raw": item.get("raw"), "parsed": _number(item.get("parsed"))}
            for item in candidates[:10]
            if isinstance(item, Mapping)
        ]
    if len(view_metrics) > 1:
        payload["raw_data"]["view_metrics"] = view_metrics
    crosspost = node.get("facebook_crosspost")
    if isinstance(crosspost, Mapping):
        for key, column in {
            "comments_count": "fb_comment_count",
            "likes_count": "fb_like_count",
            "metadata": "crosspost_metadata",
            "post_id": "facebook_post_id",
            "post_url": "facebook_post_url",
            "observed_at": "facebook_crosspost_observed_at",
            "source": "facebook_crosspost_source",
            "social_context": "social_context",
            "is_shared_to_fb": "is_shared_to_fb",
        }.items():
            if _present(crosspost.get(key)):
                payload[column] = _time(crosspost[key]) if column in _POST_TIMESTAMP_FIELDS else crosspost[key]
    # Existing readers consume the scraper serializer at the root of raw_data.
    # Keep its aliases alongside native source fields; immutable source evidence
    # is separately retained by the observation/evidence records.
    for column, raw_key in {
        "shortcode": "shortcode",
        "media_id": "pk",
        "media_type": "post_type",
        "username": "username",
        "caption": "caption",
        "likes": "likes",
        "comments_count": "comments",
        "views": "video_views",
        "permalink": "url",
        "media_urls": "media_urls",
        "thumbnail_url": "thumbnail_url",
        "profile_tags": "profile_tags",
        "hashtags": "hashtags",
        "mentions": "mentions",
        "collaborators": "collaborators",
        "tagged_users_detail": "tagged_users_detail",
        "collaborators_detail": "collaborators_detail",
        "child_posts_data": "child_posts_data",
    }.items():
        if _present(payload.get(column)):
            payload["raw_data"][raw_key] = payload[column]
    for key in ("detail_snapshot", "video_views_source", "video_views_raw_candidates"):
        if key in node:
            payload["raw_data"][key] = node[key]
    return {k: v for k, v in payload.items() if _present(v)}


def write_post(
    conn: Any,
    *,
    run_id: str,
    account: str,
    observed_at: datetime,
    normalized: InstagramPost,
    source_payload: Mapping[str, Any],
    assignment: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Atomically populate all existing post models; the caller commits the receipt."""
    shortcode = str(normalized.shortcode or "").strip()
    if not shortcode or not account or not run_id:
        raise CanonicalWriteError("Post shortcode, run_id and account are required")
    if normalize_post_source(source_payload, account=account).shortcode != shortcode:
        raise CanonicalWriteError("Normalized post identity differs from source capture")
    if assignment and set(assignment) - _POST_ASSIGNMENT_FIELDS:
        raise CanonicalWriteError("Unsupported assignment fields")
    observed_at = _guard(conn, "post:" + shortcode, observed_at)
    old = _row(conn, "instagram_posts", {"shortcode": shortcode})
    catalog = _row(conn, "instagram_account_catalog_posts", {"source_id": shortcode})
    canonical = _row(conn, "social_posts", {"platform": "instagram", "source_id": shortcode})
    post_sidecar = _row(conn, "instagram_post_payloads", {"post_id": old["id"]}) if old else {}
    catalog_sidecar = (
        _row(conn, "instagram_account_catalog_post_payloads", {"catalog_post_id": catalog["id"]}) if catalog else {}
    )
    _reject_older(observed_at, old, catalog, canonical, post_sidecar, catalog_sidecar)
    incoming = _post_payload(normalized, source_payload, account, observed_at)
    incoming.update(assignment or {})
    # Sidecars are authoritative preservation state during the current rollout.
    for key in ("raw_data", "child_posts_data", "asset_manifest"):
        if key in post_sidecar:
            old[key] = post_sidecar[key]
    merged = _merge(old, incoming)
    payload = {
        key: merged[key] for key in set(incoming) | {"raw_data", "child_posts_data", "asset_manifest"} if key in merged
    }
    if "views" in payload:
        payload["views"] = max(
            _number(old.get("views")) or 0,
            _number(catalog.get("views")) or 0,
            _number(canonical.get("view_count")) or 0,
            payload["views"],
        )
        merged["views"] = payload["views"]
    post = _upsert(conn, "instagram_posts", payload, ["shortcode"])
    _sidecar(conn, "instagram_post_payloads", "post_id", post["id"], merged, observed_at, assets=True)
    catalog_payload = {
        k: merged[k]
        for k in (
            "source_account",
            "posted_at",
            "permalink",
            "caption",
            "media_type",
            "media_urls",
            "thumbnail_url",
            "hashtags",
            "mentions",
            "collaborators",
            "profile_tags",
            "likes",
            "comments_count",
            "views",
            "raw_data",
            "music_info",
            "audio_url",
            "child_posts_data",
            "owner_username",
            "video_play_count",
            "video_duration",
        )
        if k in merged and _present(merged[k])
    }
    # Do not let unknown/hidden metrics inherit stale detail values as a fresh catalog observation.
    for key in ("likes", "comments_count", "views"):
        if key not in incoming:
            catalog_payload.pop(key, None)
    catalog_payload.update(
        source_id=shortcode,
        last_backfill_run_id=run_id,
        last_seen_at=observed_at,
        updated_at=observed_at,
        first_seen_at=observed_at,
    )
    if _present(merged.get("media_repost_count")):
        catalog_payload["shares"] = merged["media_repost_count"]
    if _present(merged.get("is_paid_partnership")):
        catalog_payload["paid_partnership"] = merged["is_paid_partnership"]
    for key in ("raw_data", "child_posts_data"):
        preserved = catalog_sidecar.get(key, catalog.get(key))
        if isinstance(preserved, Mapping):
            catalog_payload[key] = _merge(preserved, catalog_payload.get(key) or {})
        elif not catalog_payload.get(key) and preserved:
            catalog_payload[key] = preserved
    catalog = _upsert(conn, "instagram_account_catalog_posts", catalog_payload, ["source_id"])
    _sidecar(
        conn,
        "instagram_account_catalog_post_payloads",
        "catalog_post_id",
        catalog["id"],
        catalog_payload,
        observed_at,
        assets=False,
    )
    owner = post.get("username") or account
    canonical_payload = {
        "platform": "instagram",
        "source_id": shortcode,
        "owner_handle": owner,
        "owner_handle_norm": owner.lower().lstrip("@"),
        "owner_id": post.get("owner_user_id"),
        "canonical_url": post.get("permalink") or f"https://www.instagram.com/p/{shortcode}/",
        "body": post.get("caption"),
        "media_type": post.get("media_type"),
        "posted_at": post.get("posted_at"),
        "last_seen_at": observed_at,
        "last_scraped_at": observed_at,
        "first_seen_at": observed_at,
        "updated_at": observed_at,
    }
    for dest, sources in {
        "like_count": ("likes",),
        "comment_count": ("comments_count",),
        "share_count": ("media_repost_count", "shares"),
        "view_count": ("views",),
    }.items():
        canonical_payload[dest] = max(
            [_number(canonical.get(dest)) or 0] + [_number(post.get(k)) or 0 for k in sources]
        )
    canonical = _upsert(
        conn,
        "social_posts",
        _merge(canonical, canonical_payload) | {"first_seen_at": observed_at},
        ["platform", "source_id"],
    )
    for table, row, source_id in (
        ("instagram_posts", post, shortcode),
        ("instagram_account_catalog_posts", catalog, shortcode),
    ):
        _upsert(
            conn,
            "social_post_legacy_refs",
            {
                "platform": "instagram",
                "post_id": canonical["id"],
                "legacy_schema": "social",
                "legacy_table": table,
                "legacy_pk": str(row["id"]),
                "legacy_source_id": source_id,
            },
            ["platform", "legacy_table", "legacy_pk"],
        )
    # The fenced business receipt provides idempotence; each consumer-visible
    # source model retains its reference and original capture observation.
    with conn.cursor() as cur:
        for table, source_row in (("instagram_posts", post), ("instagram_account_catalog_posts", catalog)):
            cur.execute(
                """insert into social.social_post_observations
                (platform,post_id,source_table,source_pk,source_url,scrape_run_id,observed_at,raw_payload,normalized_payload)
                values ('instagram',%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    canonical["id"],
                    table,
                    str(source_row["id"]),
                    canonical_payload["canonical_url"],
                    run_id,
                    observed_at,
                    _adapt(dict(source_payload)),
                    _adapt(canonical_payload),
                ),
            )
    _entities_and_assets(conn, canonical["id"], post, observed_at)
    _collaborators(conn, catalog)
    return {
        "post_id": str(post["id"]),
        "catalog_post_id": str(catalog["id"]),
        "canonical_post_id": str(canonical["id"]),
        "shortcode": shortcode,
        "media_urls": post.get("media_urls") or [],
        "hosted_media_urls": post.get("hosted_media_urls") or [],
    }


def _entities_and_assets(conn: Any, post_id: str, post: Mapping[str, Any], observed_at: datetime) -> None:
    entities: dict[tuple[str, str], dict[str, Any]] = {}

    def entity(kind: str, key: Any, details: Mapping[str, Any] | None = None) -> None:
        key = str(key or "").strip().lstrip("@#")
        if key:
            entities[kind, key.lower()] = {
                "platform": "instagram",
                "post_id": post_id,
                "entity_type": kind,
                "entity_key": key,
                "entity_key_norm": key.lower(),
                "entity_payload": dict(details or {}),
                "last_seen_at": observed_at,
            }

    for key, kind in (("hashtags", "hashtag"), ("mentions", "mention"), ("collaborators", "collaborator")):
        for value in post.get(key) or []:
            entity(kind, value)
    for key, kind in (("tagged_users_detail", "tagged_user"), ("collaborators_detail", "collaborator")):
        for value in post.get(key) or []:
            entity(kind, value.get("username") or value.get("user_id") or value.get("id"), value)
    if post.get("location_id") or post.get("location_name"):
        entity("location", post.get("location_id") or post.get("location_name"), post.get("location_raw"))
    if entities:
        columns = list(next(iter(entities.values())))
        conflict = {"platform", "entity_type", "entity_key_norm", "post_id"}
        query = sql.SQL("""insert into social.social_post_entities ({}) values {}
            on conflict(platform,entity_type,entity_key_norm,post_id) do update set {}""").format(
            sql.SQL(",").join(map(sql.Identifier, columns)),
            sql.SQL(",").join(
                sql.SQL("({})").format(sql.SQL(",").join(sql.Placeholder() for _ in columns)) for _ in entities
            ),
            sql.SQL(",").join(
                sql.SQL("{}=excluded.{}").format(sql.Identifier(k), sql.Identifier(k))
                for k in columns
                if k not in conflict
            ),
        )
        with conn.cursor() as cur:
            cur.execute(query, [_adapt(value[k]) for value in entities.values() for k in columns])
    sources = post.get("media_urls") or ([post["thumbnail_url"]] if post.get("thumbnail_url") else [])
    hosted = post.get("hosted_media_urls") or []
    positions = range(max(len(sources), len(hosted), 1 if post.get("hosted_thumbnail_url") else 0))
    old_assets = {}
    if positions:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """select * from social.social_post_media_assets
                where platform='instagram' and post_id=%s and position=any(%s) for update""",
                (post_id, list(positions)),
            )
            old_assets = {row["position"]: dict(row) for row in cur.fetchall()}
    for position in positions:
        old = old_assets.get(position, {})
        asset = {
            "platform": "instagram",
            "post_id": post_id,
            "position": position,
            "media_type": post.get("media_type"),
            "width": post.get("width"),
            "height": post.get("height"),
            "duration_seconds": post.get("duration_seconds"),
            "mirror_status": post.get("media_mirror_status"),
            "mirror_error": post.get("media_mirror_error"),
            "mirror_last_attempt_at": post.get("media_mirror_last_attempt_at"),
            "source_url": sources[position] if position < len(sources) else None,
            "hosted_url": hosted[position] if position < len(hosted) else None,
            "media_payload": {"source": "instagram_posts", "raw_child_posts": post.get("child_posts_data") or []},
        }
        if position == 0:
            asset.update(thumbnail_url=post.get("thumbnail_url"), hosted_thumbnail_url=post.get("hosted_thumbnail_url"))
        _upsert(conn, "social_post_media_assets", _merge(old, asset), ["platform", "post_id", "position"])


def _collaborators(conn: Any, catalog: Mapping[str, Any]) -> None:
    handles = {str(v).strip().lstrip("@").lower(): "collaborators" for v in catalog.get("collaborators") or []}
    for detail in (catalog.get("raw_data") or {}).get("collaborators_detail") or []:
        if detail.get("username"):
            handles[str(detail["username"]).strip().lstrip("@").lower()] = "collaborators_detail"
    with conn.cursor() as cur:
        cur.execute(
            "delete from social.instagram_account_catalog_post_collaborators where catalog_post_id=%s", (catalog["id"],)
        )
    for handle, source in handles.items():
        if handle:
            _upsert(
                conn,
                "instagram_account_catalog_post_collaborators",
                {
                    "catalog_post_id": catalog["id"],
                    "source_id": catalog["source_id"],
                    "source_account": catalog["source_account"].lower().lstrip("@"),
                    "collaborator_handle": handle,
                    "collaborator_source": source,
                    "posted_at": catalog.get("posted_at"),
                },
                ["catalog_post_id", "collaborator_handle"],
            )


def _strip_comment_metadata(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            k: _strip_comment_metadata(v)
            for k, v in value.items()
            if k
            not in set(_COMMENT_METADATA)
            | set(_COMMENT_METADATA.values())
            | {"api_phase", "comment_phase", "source_phase"}
        }
    return [_strip_comment_metadata(v) for v in value] if isinstance(value, list) else value


# Explicit native comment attachment keys mirror the maintained parser; never
# inspect arbitrary source keys or transport objects for URL-looking strings.
_COMMENT_MEDIA_CONTAINERS = tuple(
    """
media comment_media commentMedia comment_gif commentGif media_versions attachment attachments content preview
image images image_versions2 video_versions carousel_media sticker stickers sticker_asset stickerAsset
static_sticker staticSticker gift gifts gift_media giftMedia gif gif_media gifMedia giphy_media_info giphyMediaInfo
 tenor_media_info tenorMediaInfo visual_media visualMedia animated_media animatedMedia original downsized
fixed_height fixedHeight fixed_width fixedWidth clip
""".split()
)
_COMMENT_MEDIA_URL_KEYS = """
url uri display_url displayUrl video_url videoUrl image_url imageUrl thumbnail_url thumbnailUrl media_url mediaUrl
animated_image_url animatedImageUrl fixed_height_url fixedHeightUrl fixed_width_url fixedWidthUrl webp src
""".split()


def _comment_media_urls(source: Mapping[str, Any]) -> list[str]:
    from trr_backend.socials.instagram.comment_media_selection import select_comment_media

    source = select_comment_media(source)
    urls: list[str] = []

    def append(value: Any) -> None:
        if isinstance(value, str) and value.startswith(("https://", "http://")) and value not in urls:
            urls.append(value)

    for key in ("media_urls", "mediaUrls"):
        for value in source.get(key) or []:
            append(value)
    for key in ("media_url", "mediaUrl"):
        append(source.get(key))

    def primary_image(node: Mapping[str, Any]) -> Any:
        versions = node.get("image_versions2")
        candidates = versions.get("candidates") if isinstance(versions, Mapping) else []
        values = [candidates[0].get("url") if candidates else None, node.get("display_url"), node.get("displayUrl")]
        for value in node.get("images") or []:
            values.extend(
                [value.get("url"), value.get("displayUrl"), value.get("display_url")]
                if isinstance(value, Mapping)
                else [value]
            )
        return next((v for v in values if isinstance(v, str) and v.startswith(("https://", "http://"))), None)

    def primary_video(node: Mapping[str, Any]) -> Any:
        versions = node.get("video_versions") or []
        values = [versions[0].get("url") if versions else None, node.get("video_url"), node.get("videoUrl")]
        return next((v for v in values if isinstance(v, str) and v.startswith(("https://", "http://"))), None)

    def collect(node: Any) -> None:
        if isinstance(node, list):
            for value in node:
                collect(value)
            return
        if not isinstance(node, Mapping):
            return
        # Preserve the maintained parser's primary renditions and traversal order.
        children = [
            *(node.get("carousel_media") or []),
            *(edge.get("node") or {} for edge in (node.get("edge_sidecar_to_children") or {}).get("edges") or []),
            *(node.get("childPosts") or []),
        ]
        if children:
            for child in children:
                if isinstance(child, Mapping):
                    append(primary_video(child) or primary_image(child))
        else:
            image, video = primary_image(node), primary_video(node)
            is_video = (
                node.get("media_type") == 2
                or node.get("__typename") in {"GraphVideo", "XDTGraphVideo"}
                or str(node.get("type") or "").lower() in {"video", "reel"}
                or node.get("product_type") == "clips"
            )
            append((video or image) if is_video else (image or video))
        for key in _COMMENT_MEDIA_URL_KEYS:
            append(node.get(key))
        for key in _COMMENT_MEDIA_CONTAINERS:
            collect(node.get(key))

    for key in (
        "media",
        "comment_media",
        "commentMedia",
        "attachment",
        "attachments",
        "content",
        "preview",
        "clip",
        "sticker",
        "stickers",
        "gift",
        "gifts",
        "comment_gif",
        "commentGif",
        "gif",
        "gifMedia",
        "giphy_media_info",
        "giphyMediaInfo",
        "tenor_media_info",
        "tenorMediaInfo",
        "visual_media",
        "visualMedia",
        "animated_media",
        "animatedMedia",
    ):
        collect(source.get(key))
    if isinstance(source.get("image_versions2"), Mapping) or isinstance(source.get("video_versions"), list):
        collect(source)
    return urls


def _comment_payload(
    comment: InstagramComment,
    source: Mapping[str, Any],
    *,
    post_id: str,
    account: str,
    observed_at: datetime,
    run_id: str,
    parent_id: str | None,
    depth: int,
    metadata_writes_enabled: bool,
) -> dict[str, Any]:
    author = comment.author
    payload = {
        "post_id": post_id,
        "comment_id": comment.comment_id,
        "username": author.username or "",
        "user_id": author.user_id,
        "text": comment.text,
        "likes": max(0, comment.likes_count),
        "reply_count": _number(_first(source, "child_comment_count", "repliesCount", "reply_count", "replies_count"))
        or 0,
        "is_reply": bool(parent_id),
        "parent_comment_external_id": parent_id,
        "reply_depth": depth,
        "created_at": _time(_first(source, "created_at", "timestamp") or comment.created_at),
        "scraped_at": observed_at,
        "last_seen_at": observed_at,
        "last_seen_run_id": run_id,
        "first_seen_at": observed_at,
        "source_account": account,
        "raw_data": dict(source),
        "source_snapshot_type": comment.source_snapshot_type,
        "author_full_name": author.full_name,
        "author_profile_pic_url": author.profile_pic_url,
        "author_profile_pic_url_hd": author.profile_pic_url_hd,
        "author_is_verified": author.is_verified,
    }
    owner = _first(source, "owner", "user") or {}
    for dest, attr in {
        "author_fbid_v2": "fbid_v2",
        "author_is_mentionable": "is_mentionable",
        "author_is_private": "is_private",
        "author_latest_reel_media": "latest_reel_media",
        "author_profile_pic_id": "profile_pic_id",
    }.items():
        payload[dest] = _first(source, dest, "owner_" + attr, attr)
        if payload[dest] is None and isinstance(owner, Mapping):
            payload[dest] = owner.get(attr)
    for key in _COMMENT_FIELDS:
        if _present(source.get(key)):
            payload[key] = _time(source[key]) if key == "media_mirror_last_attempt_at" else source[key]
    media_urls = _comment_media_urls(source)
    if media_urls:
        payload["media_urls"] = media_urls
    for column, raw_key in {
        "comment_id": "comment_id",
        "username": "username",
        "user_id": "user_id",
        "text": "text",
        "likes": "likes",
        "reply_count": "reply_count",
        "is_reply": "is_reply",
        "reply_depth": "reply_depth",
        "author_full_name": "owner_full_name",
        "author_profile_pic_url": "owner_profile_pic_url",
        "author_profile_pic_url_hd": "owner_profile_pic_url_hd",
        "author_is_verified": "owner_is_verified",
    }.items():
        if _present(payload.get(column)):
            payload["raw_data"][raw_key] = payload[column]
    payload["raw_data"]["parent_comment_id"] = parent_id
    payload["raw_data"]["reply_count_observed"] = len(comment.replies)
    if metadata_writes_enabled:
        for column, alias in _COMMENT_METADATA.items():
            value = _first(source, column, alias)
            if value is not None:
                payload[column] = value
        phase = _first(source, "phase", "api_phase", "comment_phase", "source_phase")
        payload["phase"] = (
            phase
            if phase in {"ranked", "headload", "fb_crosspost", "child"}
            else ("child" if parent_id else "ranked" if payload.get("is_ranked") else None)
        )
    else:
        payload["raw_data"] = _strip_comment_metadata(payload["raw_data"])
    return {k: v for k, v in payload.items() if _present(v)}


def write_comments(
    conn: Any,
    *,
    run_id: str,
    account: str,
    observed_at: datetime,
    post_id: str,
    comments: Sequence[InstagramComment],
    source_payloads: Sequence[Mapping[str, Any]],
    metadata_writes_enabled: bool = True,
    assignment: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Persist full comments/replies with post-scoped identity and ranked evidence.

    Return observed identities for the application's sealed-manifest reconciliation
    and media dispatch. This method cannot declare missing/deleted comments or
    certify pagination completeness. Inline listing samples are rejected.
    """
    if len(comments) != len(source_payloads) or not post_id or not account or not run_id:
        raise CanonicalWriteError("Each comment requires its source payload, post_id, account and run_id")
    if assignment and set(assignment) - _COMMENT_ASSIGNMENT_FIELDS:
        raise CanonicalWriteError("Unsupported assignment fields")
    observed_at = _guard(conn, "comments:" + post_id, observed_at)
    post = _row(conn, "instagram_posts", {"id": post_id})
    if not post:
        raise CanonicalWriteError("Comment parent post does not exist")
    # Flatten/validate before writing; standalone replies retain external parents.
    records: dict[str, tuple[InstagramComment, Mapping[str, Any], str | None, int]] = {}

    def visit(comment: InstagramComment, source: Mapping[str, Any], parent: str | None, depth: int) -> None:
        source_id = str(_first(source, "comment_id", "id", "pk") or "")
        if source_id != comment.comment_id:
            raise CanonicalWriteError("Normalized comment identity differs from source capture")
        if (
            not comment.comment_id
            or comment.source_snapshot_type != "full_comments_scrape"
            or source.get("source_snapshot_type", "full_comments_scrape") != "full_comments_scrape"
        ):
            raise CanonicalWriteError("Only identified full-comments observations can be persisted")
        children = source.get("replies") or source.get("child_comments") or []
        parent = (
            parent
            or comment.parent_comment_id
            or source.get("parent_comment_external_id")
            or source.get("parent_comment_id")
        )
        depth = max(depth, int(source.get("reply_depth") or (1 if parent else 0)))
        if comment.comment_id in records:
            _, previous, previous_parent, previous_depth = records[comment.comment_id]
            if previous_parent != parent:
                raise CanonicalWriteError("Conflicting parents for one post-scoped comment")
            source = _merge(previous, source)
            if previous.get("is_ranked"):
                source["is_ranked"] = True
                if previous.get("comment_index") is not None:
                    source["comment_index"] = previous["comment_index"]
            cursor = {**(previous.get("cursor_payload") or {}), **(source.get("cursor_payload") or {})}
            phases = [previous.get("phase"), source.get("phase")]
            if len(set(filter(None, phases))) > 1:
                cursor["phase_observations"] = list(
                    dict.fromkeys([*(cursor.get("phase_observations") or []), *filter(None, phases)])
                )
            if cursor:
                source["cursor_payload"] = cursor
            depth = max(depth, previous_depth)
        records[comment.comment_id] = comment, source, parent, depth
        if len(children) != len(comment.replies):
            raise CanonicalWriteError("Reply source/normalized identity coverage differs")
        for child, raw in zip(comment.replies, children, strict=True):
            visit(child, raw, comment.comment_id, depth + 1)

    for comment, source in zip(comments, source_payloads, strict=True):
        visit(comment, source, None, 0)
    external_parents = {parent for _, _, parent, _ in records.values() if parent and parent not in records}
    lookup_ids = sorted(set(records) | external_parents)
    locked = {}
    if lookup_ids:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """select * from social.instagram_comments
                where post_id=%s and comment_id=any(%s::text[])
                order by comment_id for update""",
                (post_id, lookup_ids),
            )
            locked = {row["comment_id"]: dict(row) for row in cur.fetchall()}
    # External parents provide identities only; their newer captures must not
    # reject an older page that does not observe or rewrite those parent rows.
    baseline = {key: locked.get(key, {}) for key in records}
    parent_ids = {key: locked[key]["id"] for key in external_parents if key in locked}
    _reject_older(observed_at, *baseline.values())
    identities: dict[str, str] = {}
    media: list[dict[str, Any]] = []
    pending = dict(records)
    while pending:
        ready = [(key, record) for key, record in pending.items() if not record[2] or record[2] not in pending]
        if not ready:
            raise CanonicalWriteError("Comment parent cycle")
        for key, (comment, source, parent, depth) in ready:
            existing = baseline[key]
            payload = _comment_payload(
                comment,
                source,
                post_id=post_id,
                account=account,
                observed_at=observed_at,
                run_id=run_id,
                parent_id=parent,
                depth=depth,
                metadata_writes_enabled=metadata_writes_enabled,
            )
            parent_db_id = identities.get(parent or "")
            if parent and not parent_db_id:
                parent_db_id = parent_ids.get(parent)
                if not parent_db_id:
                    raise CanonicalWriteError("Reply parent is absent from this post")
            payload["parent_comment_id"] = parent_db_id
            payload.update(assignment or {})
            payload = _merge(existing, payload)
            if not payload.get("comment_url") and post.get("shortcode"):
                payload["comment_url"] = f"https://www.instagram.com/p/{post['shortcode']}/c/{key}/"
            if not metadata_writes_enabled:
                for column in _COMMENT_METADATA:
                    payload.pop(column, None)
                payload["raw_data"] = _strip_comment_metadata(payload["raw_data"])
            payload.update(is_missing=False, missing_at=None, parent_comment_id=parent_db_id)
            if (
                metadata_writes_enabled
                and existing.get("is_ranked")
                and not source.get("is_ranked", source.get("isRanked"))
            ):
                payload["is_ranked"] = True
                if existing.get("comment_index") is not None:
                    payload["comment_index"] = existing["comment_index"]
            if metadata_writes_enabled:
                cursor = {
                    **(existing.get("cursor_payload") or {}),
                    **(source.get("cursor_payload") or source.get("cursorPayload") or {}),
                }
                phases = [existing.get("phase"), payload.get("phase")]
                if len(set(filter(None, phases))) > 1:
                    cursor["phase_observations"] = list(
                        dict.fromkeys([*(cursor.get("phase_observations") or []), *filter(None, phases)])
                    )
                if cursor:
                    payload["cursor_payload"] = cursor
            row = _upsert(conn, "instagram_comments", payload, ["post_id", "comment_id"])
            identities[key] = str(row["id"])
            if row.get("media_urls"):
                media.append(
                    {
                        "comment_id": str(row["id"]),
                        "media_urls": row["media_urls"],
                        "hosted_media_urls": row.get("hosted_media_urls") or [],
                    }
                )
            del pending[key]
    return {
        "post_id": str(post_id),
        "comment_ids": identities,
        "observed_comment_ids": sorted(identities),
        "media": media,
    }
