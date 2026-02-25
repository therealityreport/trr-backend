"""
Instagram scraping module.

Provides tools for fetching and analyzing Instagram posts
with support for filtering by hashtags, accounts, and date ranges.
Includes comment and reply fetching with like counts.
"""

from .crawlee_adapter import run_stage_with_crawlee
from .permalink_metadata import (
    InstagramMediaResolution,
    InstagramPermalinkMetadata,
    fetch_permalink_media_item,
    fetch_permalink_metadata,
    parse_permalink_metadata,
    resolve_instagram_media,
)
from .scraper import (
    InstagramComment,
    InstagramPost,
    InstagramScraper,
    InstagramUserDetail,
    ScrapeConfig,
    load_cookies_from_file,
)

__all__ = [
    "InstagramScraper",
    "InstagramPost",
    "InstagramComment",
    "InstagramUserDetail",
    "ScrapeConfig",
    "load_cookies_from_file",
    "InstagramPermalinkMetadata",
    "InstagramMediaResolution",
    "fetch_permalink_media_item",
    "fetch_permalink_metadata",
    "parse_permalink_metadata",
    "resolve_instagram_media",
    "run_stage_with_crawlee",
]
