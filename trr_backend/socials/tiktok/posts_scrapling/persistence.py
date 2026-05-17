"""Persistence adapter for the TikTok posts Scrapling lane.

Converts raw TikTok API itemList entries into TikTokPost-compatible DTOs,
then persists through the canonical _upsert_tiktok_post() repo helper.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
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
    from trr_backend.socials.tiktok.scraper import TikTokScrapeConfig, TikTokScraper

    parser = TikTokScraper(cookies={})
    parse_item = dict(item)
    if not parse_item.get("hashtags") and isinstance(parse_item.get("challenges"), list):
        parse_item["hashtags"] = [
            {"name": str(challenge.get("title") or "").strip()}
            for challenge in parse_item.get("challenges") or []
            if isinstance(challenge, dict) and str(challenge.get("title") or "").strip()
        ]
    parsed = parser._parse_post_item(  # noqa: SLF001
        parse_item,
        TikTokScrapeConfig(username=account_handle),
    )

    return _ScraplingTikTokPostDTO(
        video_id=parsed.video_id,
        date_time=parsed.date_time,
        create_time=parsed.create_time,
        description=parsed.description,
        hashtags=list(parsed.hashtags or []),
        mentions=list(parsed.mentions or []),
        likes=parsed.likes,
        comments=parsed.comments,
        shares=parsed.shares,
        saves=parsed.saves,
        views=parsed.views,
        url=parsed.url,
        username=parsed.username,
        author_nickname=parsed.author_nickname,
        duration=parsed.duration,
        music_title=parsed.music_title,
        music_author=parsed.music_author,
        user_avatar_url=parsed.user_avatar_url,
        media_urls=list(parsed.media_urls or []),
        thumbnail_url=parsed.thumbnail_url,
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
            try:
                dto = _tiktok_item_to_post_dto(item, account_handle=account_handle)
            except Exception:  # noqa: BLE001
                logger.exception("Failed to adapt TikTok post item via canonical parser")
                _record_skip("adapt_failed")
                continue
            if not str(dto.video_id or "").strip():
                _record_skip("missing_video_id")
                continue
            try:
                repo._upsert_tiktok_post(
                    context,
                    job_id=job_id,
                    account=account_handle,
                    post=dto,
                    conn=conn,
                )
                posts_upserted += 1
            except Exception:  # noqa: BLE001
                logger.exception("Failed to upsert TikTok post %s via canonical helper", dto.video_id)
                _record_skip("upsert_failed")

    return PersistedTikTokPosts(
        posts_upserted=posts_upserted,
        posts_skipped=posts_skipped,
        posts_skipped_by_reason=posts_skipped_by_reason,
    )
