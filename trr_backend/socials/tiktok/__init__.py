"""
TikTok scraping module.

Provides tools for fetching and analyzing TikTok posts with support for
filtering by hashtags, accounts, and date ranges.

Direct admin preview/scrape lives in ``direct_scrape`` and remains separate
from ``posts_scrapling`` claimed jobs. Legacy scraper comment helpers are not a
persisted backend TikTok comments ingestion contract.
"""

# Compatibility exports remain during the platform cleanup so existing scripts,
# route tests, and monkeypatch paths can migrate to direct_scrape/ops deliberately.
from .crawlee_adapter import run_stage_with_crawlee
from .direct_scrape import build_scrape_diagnostics, preview_tiktok_profile, scrape_tiktok
from .media_resolver import TikTokMediaResolution, resolve_tiktok_media
from .scraper import TikTokComment, TikTokPost, TikTokScrapeConfig, TikTokScraper

__all__ = [
    "TikTokScraper",
    "TikTokPost",
    "TikTokComment",
    "TikTokScrapeConfig",
    "TikTokMediaResolution",
    "resolve_tiktok_media",
    "run_stage_with_crawlee",
    "build_scrape_diagnostics",
    "scrape_tiktok",
    "preview_tiktok_profile",
]
