"""Persistence adapter for the Instagram posts Scrapling lane.

Converts raw GraphQL edge nodes into InstagramPost-compatible DTOs,
then persists through the canonical _upsert_instagram_post() repo helper.
This preserves view monotonicity, optional-column guards, assignment
payloads, and all mirror metadata behavior.

Handles two distinct Instagram GraphQL response shapes:

1. Legacy "Graph*" shape (older shortcode_media queries):
   - __typename: GraphImage / GraphVideo / GraphSidecar (+ XDT variants)
   - shortcode, display_url, edge_media_preview_like.count,
     edge_media_to_comment.count, edge_media_to_caption.edges[0].node.text,
     edge_sidecar_to_children.edges[*].node, owner.username

2. XDTMediaDict shape (returned by the profile timeline connection
   xdt_api__v1__feed__user_timeline_graphql_connection):
   - __typename: XDTMediaDict
   - code (= shortcode), pk/id, like_count, comment_count,
     media_type (1=image, 2=video, 8=carousel),
     caption: {text: ...} (dict, not list of edges),
     image_versions2.candidates[0].url, video_versions[0].url,
     carousel_media: list[XDTMediaDict],
     user.username, taken_at (seconds, not _timestamp),
     view_count / play_count

The adapter prefers legacy fields when present, falls back to XDTMediaDict
fields. Unknown/empty shortcodes cause a skip (logged).
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import UTC, datetime
from typing import Any

from trr_backend.socials.instagram.post_normalizer import _extract_repost_count, normalize_instagram_post

logger = logging.getLogger("socials.instagram.posts_scrapling.persistence")

# Legacy typename → media_type (Graph* / XDTGraph* variants)
_TYPENAME_TO_MEDIA_TYPE = {
    "GraphImage": "image",
    "GraphVideo": "video",
    "GraphSidecar": "carousel",
    "XDTGraphImage": "image",
    "XDTGraphVideo": "video",
    "XDTGraphSidecar": "carousel",
}

# XDTMediaDict integer media_type → string media_type
# 1 = photo, 2 = video, 8 = carousel (per Instagram's internal media type codes)
_INT_MEDIA_TYPE_TO_STRING = {
    1: "image",
    2: "video",
    8: "carousel",
}


@dataclass(slots=True)
class PersistedInstagramPosts:
    posts_upserted: int
    posts_skipped: int
    posts_skipped_by_reason: dict[str, int] = field(default_factory=dict)


@dataclass
class _ScraplingPostDTO:
    """Lightweight DTO that satisfies _upsert_instagram_post's getattr() contract.

    Field names match InstagramPost (scraper.py:224-281) exactly so the
    canonical repo helper reads them correctly.
    """

    shortcode: str
    post_type: str
    date_time: str
    taken_at: int
    caption: str
    profile_tags: list[str]
    sponsored: bool
    likes: int
    comments: int
    video_views: int
    video_views_observed: int | None
    video_views_source: str | None
    video_views_raw_candidates: list[dict[str, Any]]
    url: str
    pk: str
    username: str
    media_urls: list[str] = field(default_factory=list)
    thumbnail_url: str | None = None
    hashtags: list[str] = field(default_factory=list)
    mentions: list[str] = field(default_factory=list)
    collaborators: list[str] = field(default_factory=list)
    tagged_users_detail: list[Any] = field(default_factory=list)
    collaborators_detail: list[Any] = field(default_factory=list)
    owner_detail: Any = None
    product_type: str | None = None
    video_play_count: int | None = None
    alt_text: str | None = None
    width: int | None = None
    height: int | None = None
    is_comments_disabled: bool | None = None
    comments_disabled: bool | None = None
    like_and_view_counts_disabled: bool | None = None
    commenting_disabled_for_viewer: bool | None = None
    media_repost_count: int | None = None
    is_paid_partnership: bool | None = None
    is_advertisement: bool | None = None
    can_viewer_reshare: bool | None = None
    has_audio: bool | None = None
    caption_id: str | None = None
    caption_is_edited: bool | None = None
    caption_has_translation: bool | None = None
    source_post_id: str | None = None
    input_url: str | None = None
    owner_user_id: str | None = None
    owner_username: str | None = None
    owner_profile_pic_url_hd: str | None = None
    location_id: str | None = None
    location_name: str | None = None
    location_raw: dict[str, Any] | None = None
    original_width: int | None = None
    original_height: int | None = None
    music_info: dict[str, Any] | None = None
    audio_url: str | None = None
    video_duration: float | None = None
    child_posts_data: list[dict[str, Any]] = field(default_factory=list)
    inline_comment_samples: list[Any] = field(default_factory=list)
    # Mirror fields (left empty — mirroring handled separately)
    hosted_media_urls: list[str] = field(default_factory=list)
    hosted_thumbnail_url: str | None = None
    media_mirror_status: str | None = None
    media_mirror_error: str | None = None
    media_mirror_attempt_count: int | None = None
    media_mirror_last_attempt_at: str | None = None
    media_mirror_last_job_id: str | None = None
    metadata_scraped_at: datetime | None = None
    metadata_source: str | None = None
    duration_seconds: int | None = None
    _raw_node: dict[str, Any] = field(default_factory=dict, repr=False)

    def to_dict(self) -> dict[str, Any]:
        return dict(self._raw_node)


def _extract_shortcode(node: dict[str, Any]) -> str:
    """Shortcode field name differs: legacy='shortcode', XDTMediaDict='code'."""
    return str(node.get("shortcode") or node.get("code") or "").strip()


def _extract_media_type(node: dict[str, Any]) -> str:
    """Media type is expressed as either a __typename string (legacy) or
    an integer media_type field (XDTMediaDict)."""
    typename = str(node.get("__typename") or "").strip()
    if typename in _TYPENAME_TO_MEDIA_TYPE:
        return _TYPENAME_TO_MEDIA_TYPE[typename]
    media_type_int = node.get("media_type")
    if isinstance(media_type_int, int):
        return _INT_MEDIA_TYPE_TO_STRING.get(media_type_int, "unknown")
    return "unknown"


def _extract_caption(node: dict[str, Any]) -> str:
    """Caption shape differs:
    - Legacy: edge_media_to_caption.edges[0].node.text
    - XDTMediaDict: caption.text (caption is a dict)
    """
    # Legacy path
    caption_edges = (node.get("edge_media_to_caption") or {}).get("edges") or []
    if caption_edges and isinstance(caption_edges[0], dict):
        return str((caption_edges[0].get("node") or {}).get("text") or "")
    # XDTMediaDict path
    caption_obj = node.get("caption")
    if isinstance(caption_obj, dict):
        return str(caption_obj.get("text") or "")
    return ""


def _extract_likes(node: dict[str, Any]) -> int:
    """Likes in legacy are at edge_media_preview_like.count; in XDTMediaDict at like_count."""
    preview = node.get("edge_media_preview_like")
    if isinstance(preview, dict):
        return int(preview.get("count") or 0)
    return int(node.get("like_count") or 0)


def _extract_comments(node: dict[str, Any]) -> int:
    """Comments in legacy are at edge_media_to_comment.count; in XDTMediaDict at comment_count."""
    edge = node.get("edge_media_to_comment")
    if isinstance(edge, dict):
        return int(edge.get("count") or 0)
    return int(node.get("comment_count") or 0)


def _extract_views(node: dict[str, Any]) -> int:
    """Views fan out across several fields depending on media type and shape."""
    for key in ("video_view_count", "video_play_count", "play_count", "view_count"):
        raw = node.get(key)
        if raw is not None:
            try:
                return int(raw)
            except (TypeError, ValueError):
                continue
    return 0


def _extract_taken_at(node: dict[str, Any]) -> int:
    """Legacy uses taken_at_timestamp; XDTMediaDict uses taken_at (both unix seconds)."""
    return int(node.get("taken_at_timestamp") or node.get("taken_at") or 0)


def _extract_username(node: dict[str, Any], *, account_handle: str) -> str:
    """Username may be on owner.username (legacy) or user.username (XDTMediaDict)."""
    for candidate in (node.get("owner") or {}, node.get("user") or {}):
        if isinstance(candidate, dict):
            value = str(candidate.get("username") or "").strip()
            if value:
                return value
    return account_handle


def _extract_pk(node: dict[str, Any]) -> str:
    """XDTMediaDict has pk as a number; legacy stores id as a shortcode-ish composite."""
    pk_raw = node.get("pk") or node.get("id") or ""
    return str(pk_raw).strip()


def _collect_image_versions_urls(image_versions_obj: Any) -> list[str]:
    """image_versions2 is a dict with a 'candidates' list of {url, width, height}."""
    urls: list[str] = []
    if not isinstance(image_versions_obj, dict):
        return urls
    candidates = image_versions_obj.get("candidates") or []
    for cand in candidates:
        if isinstance(cand, dict):
            url = str(cand.get("url") or "").strip()
            if url and url not in urls:
                urls.append(url)
    return urls


def _collect_video_versions_urls(video_versions: Any) -> list[str]:
    """video_versions is a list of {url, width, height, type}."""
    urls: list[str] = []
    if not isinstance(video_versions, list):
        return urls
    for v in video_versions:
        if isinstance(v, dict):
            url = str(v.get("url") or "").strip()
            if url and url not in urls:
                urls.append(url)
    return urls


def _is_video_media_node(node: dict[str, Any]) -> bool:
    typename = str(node.get("__typename") or "").strip()
    if typename in {"GraphVideo", "XDTGraphVideo"}:
        return True
    media_type_int = node.get("media_type")
    if media_type_int == 2:
        return True
    return bool(node.get("is_video") or node.get("video_url") or node.get("video_versions"))


def _select_primary_media_url_for_node(
    node: dict[str, Any],
    *,
    image_urls: list[str],
    video_urls: list[str],
) -> str | None:
    if _is_video_media_node(node) and video_urls:
        return video_urls[0]
    if image_urls:
        return image_urls[0]
    if video_urls:
        return video_urls[0]
    return None


def _extract_media_urls(node: dict[str, Any]) -> tuple[list[str], str | None]:
    """Return canonical media_urls plus thumbnail_url for legacy/XDT shapes.

    ``media_urls`` stores the post payloads to mirror. Cover images stay in
    ``thumbnail_url`` so reels mirror one cover image and one video, not a
    duplicate cover/video candidate set.
    """
    media_urls: list[str] = []
    thumbnail_url: str | None = None

    # Legacy top-level display_url + video_url
    display_url = str(node.get("display_url") or "").strip()
    top_image_urls = [display_url] if display_url else []
    if display_url:
        thumbnail_url = display_url
    video_url = str(node.get("video_url") or "").strip()
    top_video_urls = [video_url] if video_url else []

    # XDTMediaDict top-level image_versions2 / video_versions
    image_urls = _collect_image_versions_urls(node.get("image_versions2"))
    if image_urls and not thumbnail_url:
        thumbnail_url = image_urls[0]
    top_image_urls.extend(image_urls)
    top_video_urls.extend(_collect_video_versions_urls(node.get("video_versions")))

    # Legacy carousel children
    sidecar_edges = (node.get("edge_sidecar_to_children") or {}).get("edges") or []
    child_urls: list[str] = []
    for child_edge in sidecar_edges:
        child = child_edge.get("node") or {} if isinstance(child_edge, dict) else {}
        child_image_urls = [str(child.get("display_url") or "").strip()]
        child_video_urls = [str(child.get("video_url") or "").strip()]
        selected = _select_primary_media_url_for_node(
            child,
            image_urls=[url for url in child_image_urls if url],
            video_urls=[url for url in child_video_urls if url],
        )
        if selected:
            child_urls.append(selected)

    # XDTMediaDict carousel_media
    carousel_media = node.get("carousel_media") or []
    if isinstance(carousel_media, list):
        for child in carousel_media:
            if not isinstance(child, dict):
                continue
            selected = _select_primary_media_url_for_node(
                child,
                image_urls=_collect_image_versions_urls(child.get("image_versions2")),
                video_urls=_collect_video_versions_urls(child.get("video_versions")),
            )
            if selected:
                child_urls.append(selected)

    if child_urls:
        media_urls.extend(child_urls)
    else:
        selected = _select_primary_media_url_for_node(
            node,
            image_urls=top_image_urls,
            video_urls=top_video_urls,
        )
        if selected:
            media_urls.append(selected)

    return media_urls, thumbnail_url


def _normalizer_object_to_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if hasattr(value, "to_dict"):
        result = value.to_dict()
        return dict(result) if isinstance(result, dict) else {}
    if is_dataclass(value):
        result = asdict(value)
        return dict(result) if isinstance(result, dict) else {}
    if isinstance(value, dict):
        return dict(value)
    return {}


def _graph_node_to_post_dto(node: dict[str, Any], *, account_handle: str) -> _ScraplingPostDTO:
    """Convert a raw GraphQL edge node into a DTO that _upsert_instagram_post can read.

    Handles both legacy GraphQL shapes (GraphImage/GraphVideo/GraphSidecar) and
    the newer XDTMediaDict shape returned by the profile timeline connection.
    """
    normalized = normalize_instagram_post(node, account_handle=account_handle)
    shortcode = normalized.shortcode or _extract_shortcode(node)
    post_type = normalized.media_type or _extract_media_type(node)
    caption = normalized.caption.text or _extract_caption(node)
    likes = _extract_likes(node)
    comments = _extract_comments(node)
    views = normalized.video_view_count if normalized.video_view_count is not None else _extract_views(node)
    taken_at = _extract_taken_at(node)
    date_time = datetime.fromtimestamp(taken_at, tz=UTC).isoformat() if taken_at else ""
    username = (
        normalized.owner.username if normalized.owner and normalized.owner.username else None
    ) or _extract_username(
        node,
        account_handle=account_handle,
    )
    pk = normalized.source_id or _extract_pk(node)
    fallback_media_urls, fallback_thumbnail_url = _extract_media_urls(node)
    media_urls = normalized.media_urls or fallback_media_urls
    thumbnail_url = normalized.thumbnail_url or fallback_thumbnail_url
    owner_detail = normalized.owner
    location_raw = _normalizer_object_to_dict(normalized.location)
    flags = normalized.flags
    input_url = str(node.get("input_url") or node.get("inputUrl") or "").strip() or None

    return _ScraplingPostDTO(
        shortcode=shortcode,
        post_type=post_type,
        date_time=date_time,
        taken_at=taken_at,
        caption=caption,
        sponsored=bool(flags.get("paid_partnership") or flags.get("advertisement")),
        likes=likes,
        comments=comments,
        video_views=views,
        video_views_observed=views if views > 0 else None,
        video_views_source="graphql_scrapling" if views > 0 else None,
        video_views_raw_candidates=[],
        url=normalized.permalink or (f"https://www.instagram.com/p/{shortcode}/" if shortcode else ""),
        pk=pk,
        username=username,
        media_urls=media_urls,
        thumbnail_url=thumbnail_url,
        hashtags=normalized.hashtags,
        mentions=normalized.mentions,
        collaborators=[user.username for user in normalized.collaborators if user.username],
        profile_tags=[user.username for user in normalized.tagged_users if user.username],
        tagged_users_detail=normalized.tagged_users,
        collaborators_detail=normalized.collaborators,
        owner_detail=owner_detail,
        product_type=str(node.get("product_type") or node.get("productType") or "") or None,
        video_play_count=normalized.video_play_count,
        alt_text=normalized.alt_text,
        width=normalized.width,
        height=normalized.height,
        is_comments_disabled=flags.get("comments_disabled"),
        comments_disabled=flags.get("comments_disabled"),
        like_and_view_counts_disabled=flags.get("like_count_disabled"),
        commenting_disabled_for_viewer=bool(node.get("commenting_disabled_for_viewer"))
        if node.get("commenting_disabled_for_viewer") is not None
        else None,
        media_repost_count=normalized.media_repost_count
        if normalized.media_repost_count is not None
        else _extract_repost_count(node),
        is_paid_partnership=flags.get("paid_partnership"),
        is_advertisement=flags.get("advertisement"),
        can_viewer_reshare=bool(node.get("can_viewer_reshare")) if node.get("can_viewer_reshare") is not None else None,
        has_audio=bool(node.get("has_audio")) if node.get("has_audio") is not None else None,
        caption_id=normalized.caption.caption_id,
        caption_is_edited=normalized.caption.is_edited,
        caption_has_translation=normalized.caption.has_translation,
        source_post_id=normalized.source_id,
        input_url=input_url,
        owner_user_id=owner_detail.user_id if owner_detail else None,
        owner_username=owner_detail.username if owner_detail else username,
        owner_profile_pic_url_hd=owner_detail.profile_pic_url_hd if owner_detail else None,
        location_id=normalized.location.location_id if normalized.location else None,
        location_name=normalized.location.name if normalized.location else None,
        location_raw=location_raw or None,
        original_width=normalized.width,
        original_height=normalized.height,
        music_info=normalized.music_info,
        audio_url=normalized.audio_url,
        video_duration=normalized.video_duration,
        child_posts_data=[_normalizer_object_to_dict(child) for child in normalized.child_posts],
        inline_comment_samples=normalized.inline_comment_samples,
        metadata_scraped_at=datetime.now(tz=UTC),
        metadata_source="posts_scrapling",
        _raw_node=node,
    )


def persist_instagram_posts(
    *,
    account_handle: str,
    post_nodes: list[dict[str, Any]],
    run_id: str | None,
    job_id: str | None,
    season_id: str | None = None,
    source_scope: str = "network",
) -> PersistedInstagramPosts:
    """Adapt raw GraphQL nodes and persist through the canonical repo helper."""
    from trr_backend.db import pg
    from trr_backend.repositories import social_season_analytics as repo

    context = repo.get_season_context(season_id) if season_id else None
    posts_upserted = 0
    posts_skipped = 0
    inline_comments_upserted = 0
    inline_comments_skipped = 0
    posts_skipped_by_reason: dict[str, int] = {}

    def _record_skip(reason: str) -> None:
        nonlocal posts_skipped
        posts_skipped += 1
        posts_skipped_by_reason[reason] = int(posts_skipped_by_reason.get(reason) or 0) + 1

    with pg.db_connection() as conn:
        for node in post_nodes:
            if not isinstance(node, dict):
                _record_skip("invalid_node_type")
                continue
            shortcode = _extract_shortcode(node)
            if not shortcode:
                _record_skip("missing_shortcode")
                continue
            try:
                dto = _graph_node_to_post_dto(node, account_handle=account_handle)
            except Exception:  # noqa: BLE001
                logger.exception("Failed to adapt post %s into canonical DTO", shortcode)
                _record_skip("dto_adaptation_exception")
                continue

            try:
                upserted = repo._upsert_instagram_post(
                    context,
                    job_id=job_id,
                    account=account_handle,
                    post=dto,
                    conn=conn,
                )
            except Exception:  # noqa: BLE001
                logger.exception("Failed to upsert post %s via canonical helper", shortcode)
                _record_skip("canonical_upsert_exception")
                continue

            if not upserted:
                logger.warning("Canonical Instagram post upsert returned no row for shortcode=%s", shortcode)
                _record_skip("canonical_upsert_returned_none")
                continue

            posts_upserted += 1
            inline_samples = list(getattr(dto, "inline_comment_samples", []) or [])
            if inline_samples:
                post_id = str((upserted or {}).get("id") or "").strip()
                if context is not None and post_id:
                    inline_stats: dict[str, int] = {}
                    try:
                        inline_comments_upserted += int(
                            repo._batch_upsert_instagram_comments(
                                context,
                                job_id=job_id,
                                run_id=run_id,
                                account=account_handle,
                                post_id=post_id,
                                comments=inline_samples,
                                persist_stats=inline_stats,
                                source_scope=source_scope,
                                enable_media_followups=False,
                                conn=conn,
                            )
                            or 0
                        )
                    except Exception:  # noqa: BLE001
                        logger.exception("Failed to persist inline comment samples for shortcode=%s", shortcode)
                        inline_comments_skipped += len(inline_samples)
                else:
                    inline_comments_skipped += len(inline_samples)

    if job_id:
        existing_row = (
            pg.fetch_one(
                "select metadata from social.scrape_jobs where id = %s",
                [job_id],
            )
            or {}
        )
        metadata = dict(existing_row.get("metadata") or {})
        existing_summary = metadata.get("posts_scrapling_persist_diagnostics")
        existing_reasons = (
            dict(existing_summary.get("posts_skipped_by_reason") or {}) if isinstance(existing_summary, dict) else {}
        )
        merged_reasons = {
            reason: int(existing_reasons.get(reason) or 0) + int(posts_skipped_by_reason.get(reason) or 0)
            for reason in sorted(set(existing_reasons) | set(posts_skipped_by_reason))
        }
        metadata["posts_scrapling_persist_diagnostics"] = {
            "posts_upserted": int((existing_summary or {}).get("posts_upserted") or 0) + posts_upserted,
            "posts_skipped": int((existing_summary or {}).get("posts_skipped") or 0) + posts_skipped,
            "posts_skipped_by_reason": merged_reasons,
            "inline_comments_upserted": int((existing_summary or {}).get("inline_comments_upserted") or 0)
            + inline_comments_upserted,
            "inline_comments_skipped": int((existing_summary or {}).get("inline_comments_skipped") or 0)
            + inline_comments_skipped,
        }
        pg.fetch_one(
            """
            update social.scrape_jobs
            set metadata = %s::jsonb
            where id = %s
            returning id::text
            """,
            [json.dumps(metadata), job_id],
        )

    return PersistedInstagramPosts(
        posts_upserted=posts_upserted,
        posts_skipped=posts_skipped,
        posts_skipped_by_reason=posts_skipped_by_reason,
    )
