"""
Twitter/X scraping module.

Provides tools for fetching and analyzing tweets
with support for filtering by hashtags, phrases, and date ranges.
"""

from .crawlee_adapter import run_stage_with_crawlee
from .direct_scrape import fetch_tweet_quotes, fetch_tweet_replies, search_twitter, tweet_to_payload
from .public_mirror import fetch_public_conversation
from .public_search import search_public_posts
from .scraper import Tweet, TwitterScrapeConfig, TwitterScraper, mirror_tweet_media

# Compatibility exports remain here so router and test monkeypatch paths can
# move from scraper internals to platform-owned direct operations gradually.
__all__ = [
    "TwitterScraper",
    "Tweet",
    "TwitterScrapeConfig",
    "fetch_tweet_quotes",
    "fetch_tweet_replies",
    "fetch_public_conversation",
    "mirror_tweet_media",
    "run_stage_with_crawlee",
    "search_twitter",
    "search_public_posts",
    "tweet_to_payload",
]
