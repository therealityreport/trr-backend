"""TikTok scraper data models and the small coercion helpers they share."""

import logging
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger(__name__)


def _coerce_utc_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _dict_or_empty(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


@dataclass
class TikTokScrapeConfig:
    """Configuration for a TikTok scrape operation."""

    username: str
    hashtags: list[str] = field(default_factory=list)
    date_start: datetime | None = None
    date_end: datetime | None = None
    delay_seconds: float = 2.0
    max_pages: int | None = None  # None = no limit

    # Performance tuning
    fast_mode: bool = False
    """When True, uses aggressive rate-limiting tiers and reduced delays."""

    scrape_mode: str = "ytdlp"
    """Scraping strategy: 'ytdlp' (production default), 'api' (experimental
    direct TikTok API), 'browser_intercept' (experimental Playwright
    interception), or 'auto' (compatibility alias to 'ytdlp')."""

    ytdlp_max_videos_hint: int | None = None
    """Advisory upper bound for yt-dlp profile enumeration in fallback mode."""

    fetch_comment_replies: bool = True
    """When False, only fetch top-level comments and skip reply chains."""

    # Metadata for tracking
    show_id: int | None = None
    season_number: int | None = None
    person_id: int | None = None

    def __post_init__(self):
        """Apply fast_mode overrides when enabled."""
        if self.fast_mode:
            if self.delay_seconds == 2.0:  # Only override if at default
                self.delay_seconds = 0.5
            logger.info(
                "TikTokScrapeConfig fast_mode enabled: delay=%.2fs, scrape_mode=%s",
                self.delay_seconds,
                self.scrape_mode,
            )

    @property
    def start_timestamp(self) -> float:
        coerced = _coerce_utc_datetime(self.date_start)
        return coerced.timestamp() if coerced else 0

    @property
    def end_timestamp(self) -> float:
        coerced = _coerce_utc_datetime(self.date_end)
        return coerced.timestamp() if coerced else datetime.now(UTC).timestamp()

    def matches_hashtags(self, text: str) -> bool:
        """Check if text contains any of the configured hashtags."""
        if not self.hashtags:
            return True  # No filter = match all
        text_lower = text.lower()
        return any(f"#{tag.lower().lstrip('#')}" in text_lower for tag in self.hashtags)

    def is_in_date_range(self, timestamp: int) -> bool | None:
        """
        Check if timestamp is in date range.
        Returns None if before range (stop iteration), True if in range, False if after.
        """
        if timestamp < self.start_timestamp:
            return None  # Before range - stop
        if timestamp > self.end_timestamp:
            return False  # After range - skip
        return True  # In range


@dataclass
class TikTokComment:
    """Represents a single TikTok comment with reply support."""

    comment_id: str
    text: str
    username: str
    user_id: str
    nickname: str
    created_at: int
    date_time: str
    likes: int
    is_reply: bool
    parent_comment_id: str | None  # ID of parent comment if this is a reply
    reply_count: int
    replies: list["TikTokComment"] = field(default_factory=list)
    avatar_thumbnail_url: str | None = None
    comment_language: str | None = None
    is_author_liked: bool | None = None
    aweme_id: str | None = None
    parent_source_comment_id: str | None = None
    user_url: str | None = None
    user_bio: str | None = None
    user_avatar_url: str | None = None
    user_region: str | None = None
    user_language: str | None = None
    media_urls: list[str] = field(default_factory=list)
    hosted_media_urls: list[str] = field(default_factory=list)
    media_mirror_status: str | None = None
    media_mirror_error: str | None = None
    media_mirror_attempt_count: int | None = None
    media_mirror_last_attempt_at: str | None = None
    media_mirror_last_job_id: str | None = None

    # Post reference
    video_id: str = ""
    post_url: str = ""

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        # Convert nested replies
        result["replies"] = [r.to_dict() if hasattr(r, "to_dict") else r for r in self.replies]
        return result


@dataclass
class TikTokPost:
    """Represents a single TikTok post with extracted data."""

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
    duration: int  # seconds
    music_title: str
    music_author: str
    user_avatar_url: str | None = None

    # Media URLs
    media_urls: list[str] = field(default_factory=list)
    thumbnail_url: str | None = None

    # Comments (populated when fetch_comments is called)
    comment_list: list[TikTokComment] = field(default_factory=list)

    # Optional tracking metadata
    show_id: int | None = None
    season_number: int | None = None
    person_id: int | None = None

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        # Convert nested comments
        result["comment_list"] = [c.to_dict() if hasattr(c, "to_dict") else c for c in self.comment_list]
        return result


__all__ = [
    "TikTokComment",
    "TikTokPost",
    "TikTokScrapeConfig",
]
