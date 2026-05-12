"""
YouTube scraping module.

Provides tools for fetching and analyzing YouTube videos
with support for filtering by channel, keywords, and date ranges.
Includes comment and reply fetching with like counts.
"""

# Compatibility exports remain during the platform cleanup so scripts, route
# tests, and catalog diagnostics can migrate to direct_scrape/ops deliberately.
from .api_client import YouTubeDataApiClient
from .crawlee_adapter import run_stage_with_crawlee
from .direct_scrape import scrape_youtube, video_to_payload
from .media_resolver import YouTubeMediaResolution, resolve_youtube_media
from .scraper import YouTubeComment, YouTubeScrapeConfig, YouTubeScraper, YouTubeVideo

__all__ = [
    "YouTubeDataApiClient",
    "YouTubeScraper",
    "YouTubeVideo",
    "YouTubeComment",
    "YouTubeScrapeConfig",
    "YouTubeMediaResolution",
    "resolve_youtube_media",
    "run_stage_with_crawlee",
    "scrape_youtube",
    "video_to_payload",
]
