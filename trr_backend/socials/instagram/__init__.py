"""
Instagram scraping module.

Provides tools for fetching and analyzing Instagram posts
with support for filtering by hashtags, accounts, and date ranges.
Includes comment and reply fetching with like counts.
"""

from .permalink_metadata import (
    InstagramPermalinkMetadata,
    fetch_permalink_media_item,
    fetch_permalink_metadata,
    parse_permalink_metadata,
)
from .scraper import (
    InstagramComment,
    InstagramPost,
    InstagramScraper,
    ScrapeConfig,
    load_cookies_from_file,
)

__all__ = [
    "InstagramScraper",
    "InstagramPost",
    "InstagramComment",
    "ScrapeConfig",
    "load_cookies_from_file",
    "InstagramPermalinkMetadata",
    "fetch_permalink_media_item",
    "fetch_permalink_metadata",
    "parse_permalink_metadata",
]
