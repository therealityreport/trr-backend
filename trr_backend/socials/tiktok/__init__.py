"""
TikTok scraping module.

Provides tools for fetching and analyzing TikTok posts
with support for filtering by hashtags, accounts, and date ranges.
Includes comment and reply fetching with like counts.
"""

from .scraper import TikTokComment, TikTokPost, TikTokScrapeConfig, TikTokScraper

__all__ = ["TikTokScraper", "TikTokPost", "TikTokComment", "TikTokScrapeConfig"]
