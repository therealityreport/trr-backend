"""Persistence adapter for the TikTok posts Scrapling lane.

Converts raw TikTok API itemList entries into TikTokPost-compatible DTOs,
then persists through the canonical _upsert_tiktok_post() repo helper.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger("socials.tiktok.posts_scrapling.persistence")


@dataclass(slots=True)
class PersistedTikTokPosts:
    posts_upserted: int
    posts_skipped: int
    posts_skipped_by_reason: dict[str, int] = field(default_factory=dict)


@dataclass
class _ScraplingTikTokPostDTO:
    """DTO satisfying _upsert_tiktok_post's getattr() contract.

    Fields match TikTokPost (scraper.py:198-237).
    """

    video_id: str
    date_time: str
    create_time: int
    description: str
    hashtags: list[str]
    mentions: list[str]
    likes: int
    comments: int
    shares: int
    saves: int
    views: int
    url: str
    username: str
    author_nickname: str
    duration: int
    music_title: str
    music_author: str
    user_avatar_url: str | None = None
    media_urls: list[str] = field(default_factory=list)
    thumbnail_url: str | None = None
    _raw_item: dict[str, Any] = field(default_factory=dict, repr=False)

    def to_dict(self) -> dict[str, Any]:
        return dict(self._raw_item)


def _tiktok_item_to_post_dto(item: dict[str, Any], *, account_handle: str) -> _ScraplingTikTokPostDTO:
    """Convert raw TikTok API item to a DTO that _upsert_tiktok_post can read."""
    stats = item.get("stats") or {}
    author = item.get("author") or {}
    music = item.get("music") or {}
    video = item.get("video") or {}

    video_id = str(item.get("id") or "").strip()
    create_time = int(item.get("createTime") or 0)
    date_time = datetime.fromtimestamp(create_time, tz=UTC).isoformat() if create_time else ""

    description = str(item.get("desc") or "")

    # Extract hashtags from challenges array if present
    challenges = item.get("challenges") or []
    hashtags = [
        str(c.get("title") or "").strip()
        for c in challenges
        if isinstance(c, dict) and str(c.get("title") or "").strip()
    ]

    username = str(author.get("uniqueId") or "").strip() or account_handle
    thumbnail_url = str(video.get("cover") or video.get("originCover") or "").strip() or None

    return _ScraplingTikTokPostDTO(
        video_id=video_id,
        date_time=date_time,
        create_time=create_time,
        description=description,
        hashtags=hashtags,
        mentions=[],
        likes=int(stats.get("diggCount") or 0),
        comments=int(stats.get("commentCount") or 0),
        shares=int(stats.get("shareCount") or 0),
        saves=int(stats.get("collectCount") or 0),
        views=int(stats.get("playCount") or 0),
        url=f"https://www.tiktok.com/@{account_handle}/video/{video_id}",
        username=username,
        author_nickname=str(author.get("nickname") or ""),
        duration=int(video.get("duration") or 0),
        music_title=str(music.get("title") or ""),
        music_author=str(music.get("authorName") or ""),
        user_avatar_url=str(author.get("avatarThumb") or "").strip() or None,
        thumbnail_url=thumbnail_url,
        _raw_item=item,
    )


def persist_tiktok_posts(
    *,
    account_handle: str,
    post_items: list[dict[str, Any]],
    run_id: str | None,
    job_id: str | None,
    season_id: str | None = None,
) -> PersistedTikTokPosts:
    """Adapt raw TikTok API items and persist through canonical repo helper."""
    from trr_backend.db import pg
    from trr_backend.repositories import social_season_analytics as repo

    context = repo.get_season_context(season_id) if season_id else None
    posts_upserted = 0
    posts_skipped = 0
    posts_skipped_by_reason: dict[str, int] = {}

    def _record_skip(reason: str) -> None:
        nonlocal posts_skipped
        posts_skipped += 1
        posts_skipped_by_reason[reason] = int(posts_skipped_by_reason.get(reason) or 0) + 1

    with pg.db_connection() as conn:
        for item in post_items:
            if not isinstance(item, dict):
                _record_skip("invalid_item")
                continue
            video_id = str(item.get("id") or "").strip()
            if not video_id:
                _record_skip("missing_video_id")
                continue
            try:
                dto = _tiktok_item_to_post_dto(item, account_handle=account_handle)
                repo._upsert_tiktok_post(
                    context,
                    job_id=job_id,
                    account=account_handle,
                    post=dto,
                    conn=conn,
                )
                posts_upserted += 1
            except Exception:  # noqa: BLE001
                logger.exception("Failed to upsert TikTok post %s via canonical helper", video_id)
                _record_skip("upsert_failed")

    return PersistedTikTokPosts(
        posts_upserted=posts_upserted,
        posts_skipped=posts_skipped,
        posts_skipped_by_reason=posts_skipped_by_reason,
    )
