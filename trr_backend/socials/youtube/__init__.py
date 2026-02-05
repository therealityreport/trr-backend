"""
YouTube scraping module.

Provides tools for fetching and analyzing YouTube videos
with support for filtering by channel, keywords, and date ranges.
Includes comment and reply fetching with like counts.
"""

from .scraper import YouTubeComment, YouTubeScrapeConfig, YouTubeScraper, YouTubeVideo

__all__ = ["YouTubeScraper", "YouTubeVideo", "YouTubeComment", "YouTubeScrapeConfig"]
