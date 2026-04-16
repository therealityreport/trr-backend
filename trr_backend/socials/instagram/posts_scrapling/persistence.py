"""Persistence adapter for the Instagram posts Scrapling lane.

Converts raw GraphQL edge nodes into InstagramPost-compatible DTOs,
then persists through the canonical _upsert_instagram_post() repo helper.
This preserves view monotonicity, optional-column guards, assignment
payloads, and all mirror metadata behavior.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger("socials.instagram.posts_scrapling.persistence")

_TYPENAME_TO_MEDIA_TYPE = {
    "GraphImage": "image",
    "GraphVideo": "video",
    "GraphSidecar": "carousel",
    "XDTGraphImage": "image",
    "XDTGraphVideo": "video",
    "XDTGraphSidecar": "carousel",
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


def _graph_node_to_post_dto(node: dict[str, Any], *, account_handle: str) -> _ScraplingPostDTO:
    """Convert a raw GraphQL edge node into a DTO that _upsert_instagram_post can read."""
    shortcode = str(node.get("shortcode") or "").strip()
    typename = str(node.get("__typename") or "").strip()
    post_type = _TYPENAME_TO_MEDIA_TYPE.get(typename, "unknown")

    # Caption
    caption_edges = (node.get("edge_media_to_caption") or {}).get("edges") or []
    caption = ""
    if caption_edges and isinstance(caption_edges[0], dict):
        caption = str((caption_edges[0].get("node") or {}).get("text") or "")

    # Engagement
    likes = int((node.get("edge_media_preview_like") or {}).get("count") or 0)
    comments = int((node.get("edge_media_to_comment") or {}).get("count") or 0)
    views = int(node.get("video_view_count") or node.get("video_play_count") or 0)

    # Timestamp
    taken_at = int(node.get("taken_at_timestamp") or node.get("taken_at") or 0)
    date_time = datetime.fromtimestamp(taken_at, tz=UTC).isoformat() if taken_at else ""

    # Media URLs
    media_urls: list[str] = []
    display_url = str(node.get("display_url") or "").strip()
    if display_url:
        media_urls.append(display_url)
    video_url = str(node.get("video_url") or "").strip()
    if video_url and video_url not in media_urls:
        media_urls.append(video_url)
    sidecar_edges = (node.get("edge_sidecar_to_children") or {}).get("edges") or []
    for child_edge in sidecar_edges:
        child = child_edge.get("node") or {} if isinstance(child_edge, dict) else {}
        for url_key in ("display_url", "video_url"):
            child_url = str(child.get(url_key) or "").strip()
            if child_url and child_url not in media_urls:
                media_urls.append(child_url)

    # Owner
    owner = node.get("owner") or {}
    username = str(owner.get("username") or "").strip() or account_handle
    pk = str(node.get("id") or node.get("pk") or "").strip()

    thumbnail_url = display_url or (media_urls[0] if media_urls else None)

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
            shortcode = str(node.get("shortcode") or "").strip()
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
