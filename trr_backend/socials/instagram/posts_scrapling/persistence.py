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

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

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
    music_info: dict[str, Any] | None = None
    video_duration: float | None = None
    child_posts_data: list[dict[str, Any]] = field(default_factory=list)
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


def _extract_media_urls(node: dict[str, Any]) -> tuple[list[str], str | None]:
    """Return (media_urls, thumbnail_url) handling both legacy and XDTMediaDict shapes.

    Dedupes. Returns the first image_versions url as the thumbnail hint.
    """
    media_urls: list[str] = []
    thumbnail_url: str | None = None

    # Legacy top-level display_url + video_url
    display_url = str(node.get("display_url") or "").strip()
    if display_url:
        media_urls.append(display_url)
        thumbnail_url = display_url
    video_url = str(node.get("video_url") or "").strip()
    if video_url and video_url not in media_urls:
        media_urls.append(video_url)

    # XDTMediaDict top-level image_versions2 / video_versions
    image_urls = _collect_image_versions_urls(node.get("image_versions2"))
    if image_urls and not thumbnail_url:
        thumbnail_url = image_urls[0]
    for url in image_urls:
        if url not in media_urls:
            media_urls.append(url)
    for url in _collect_video_versions_urls(node.get("video_versions")):
        if url not in media_urls:
            media_urls.append(url)

    # Legacy carousel children
    sidecar_edges = (node.get("edge_sidecar_to_children") or {}).get("edges") or []
    for child_edge in sidecar_edges:
        child = child_edge.get("node") or {} if isinstance(child_edge, dict) else {}
        for url_key in ("display_url", "video_url"):
            child_url = str(child.get(url_key) or "").strip()
            if child_url and child_url not in media_urls:
                media_urls.append(child_url)

    # XDTMediaDict carousel_media
    carousel_media = node.get("carousel_media") or []
    if isinstance(carousel_media, list):
        for child in carousel_media:
            if not isinstance(child, dict):
                continue
            for url in _collect_image_versions_urls(child.get("image_versions2")):
                if url not in media_urls:
                    media_urls.append(url)
            for url in _collect_video_versions_urls(child.get("video_versions")):
                if url not in media_urls:
                    media_urls.append(url)

    return media_urls, thumbnail_url


def _graph_node_to_post_dto(node: dict[str, Any], *, account_handle: str) -> _ScraplingPostDTO:
    """Convert a raw GraphQL edge node into a DTO that _upsert_instagram_post can read.

    Handles both legacy GraphQL shapes (GraphImage/GraphVideo/GraphSidecar) and
    the newer XDTMediaDict shape returned by the profile timeline connection.
    """
    shortcode = _extract_shortcode(node)
    post_type = _extract_media_type(node)
    caption = _extract_caption(node)
    likes = _extract_likes(node)
    comments = _extract_comments(node)
    views = _extract_views(node)
    taken_at = _extract_taken_at(node)
    date_time = datetime.fromtimestamp(taken_at, tz=UTC).isoformat() if taken_at else ""
    username = _extract_username(node, account_handle=account_handle)
    pk = _extract_pk(node)
    media_urls, thumbnail_url = _extract_media_urls(node)

    return _ScraplingPostDTO(
        shortcode=shortcode,
        post_type=post_type,
        date_time=date_time,
        taken_at=taken_at,
        caption=caption,
        profile_tags=[],
        sponsored=False,
        likes=likes,
        comments=comments,
        video_views=views,
        video_views_observed=views if views > 0 else None,
        video_views_source="graphql_scrapling" if views > 0 else None,
        video_views_raw_candidates=[],
        url=f"https://www.instagram.com/p/{shortcode}/" if shortcode else "",
        pk=pk,
        username=username,
        media_urls=media_urls,
        thumbnail_url=thumbnail_url,
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
    source_scope: str = "bravo",
) -> PersistedInstagramPosts:
    """Adapt raw GraphQL nodes and persist through the canonical repo helper."""
    from trr_backend.db import pg
    from trr_backend.repositories import social_season_analytics as repo

    context = repo.get_season_context(season_id) if season_id else None
    posts_upserted = 0
    posts_skipped = 0

    with pg.db_connection() as conn:
        for node in post_nodes:
            if not isinstance(node, dict):
                posts_skipped += 1
                continue
            shortcode = _extract_shortcode(node)
            if not shortcode:
                posts_skipped += 1
                continue
            try:
                dto = _graph_node_to_post_dto(node, account_handle=account_handle)
                repo._upsert_instagram_post(
                    context,
                    job_id=job_id,
                    account=account_handle,
                    post=dto,
                    conn=conn,
                )
                posts_upserted += 1
            except Exception:  # noqa: BLE001
                logger.exception("Failed to upsert post %s via canonical helper", shortcode)
                posts_skipped += 1

    return PersistedInstagramPosts(posts_upserted=posts_upserted, posts_skipped=posts_skipped)
