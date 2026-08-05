"""Import-neutral models and defaults shared by social control-plane modules."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

COMMENT_MEDIA_MIRROR_STAGE = "comment_media_mirror"
DEFAULT_COMMENT_REFRESH_POLICY = "balanced"
DEFAULT_YOUTUBE_SOURCE_MODE = "hybrid"


@dataclass(slots=True)
class SeasonContext:
    season_id: str
    show_id: str
    show_name: str | None
    season_number: int
    anchor_date: date
    show_slug: str | None = None


@dataclass(slots=True)
class WeekWindow:
    week_index: int
    start_local: datetime
    end_local: datetime
    week_type: str = "episode"
    episode_number: int | None = None


@dataclass(slots=True)
class IngestOptions:
    platforms: set[str] | None
    source_scope: str
    sync_strategy: str
    max_posts_per_target: int
    max_comments_per_post: int
    max_replies_per_post: int
    fetch_replies: bool
    ingest_mode: str
    date_start: datetime | None
    date_end: datetime | None
    comment_refresh_policy: str = DEFAULT_COMMENT_REFRESH_POLICY
    comment_anchor_source_ids: dict[str, set[str]] | None = None
    sound_ids: list[str] | None = None
    youtube_source_mode: str = DEFAULT_YOUTUBE_SOURCE_MODE
    youtube_force_reindex: bool = False
    youtube_force_media_refresh: bool = False
    youtube_force_comment_refresh: bool = False
    comments_enable_media_followups: bool = False
    details_refresh_skip_detail_fetch: bool = False
    details_refresh_force_detail_fetch: bool = False
    details_refresh_skip_media_followups: bool = False


@dataclass(slots=True)
class SentimentAnalyzerContext:
    cast_terms: set[str]
    cast_phrases: set[str]
    episode_terms: set[str]
    episode_summary: str


__all__ = [
    "COMMENT_MEDIA_MIRROR_STAGE",
    "DEFAULT_COMMENT_REFRESH_POLICY",
    "DEFAULT_YOUTUBE_SOURCE_MODE",
    "IngestOptions",
    "SeasonContext",
    "SentimentAnalyzerContext",
    "WeekWindow",
]
