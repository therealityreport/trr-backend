"""
Standalone (non-season) tweet persistence.

Provides upsert_standalone_tweets() for persisting tweets scraped by
arbitrary hashtag or @mention queries, without requiring a season_id.
"""
from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from trr_backend.repositories.social_season_analytics import _pg_upsert_many
from trr_backend.socials.twitter.scraper import Tweet

logger = logging.getLogger(__name__)


def upsert_standalone_tweets(
    tweets: list[Tweet],
    *,
    scrape_query: str,
) -> list[dict[str, Any]]:
    """Upsert a list of Tweet objects into social.twitter_tweets.

    Uses tweet_id as the conflict key. Sets scrape_query on every row
    so callers can later filter by search term.

    Returns the list of upserted rows as returned by _pg_upsert_many.
    """
    if not tweets:
        return []

    now = datetime.now(UTC).isoformat()
    payloads = [_tweet_to_payload(t, scrape_query=scrape_query, scraped_at=now) for t in tweets]
    rows = _pg_upsert_many("twitter_tweets", payloads, conflict_col="tweet_id")
    logger.info("upsert_standalone_tweets: %d upserted for query %r", len(rows), scrape_query)
    return rows


def _tweet_to_payload(tweet: Tweet, *, scrape_query: str, scraped_at: str) -> dict[str, Any]:
    """Convert a Tweet dataclass to a social.twitter_tweets insert payload."""
    created_at_ts: str | None = None
    if tweet.created_at is not None:
        try:
            created_at_ts = datetime.fromtimestamp(tweet.created_at, tz=UTC).isoformat()
        except (OSError, OverflowError, ValueError):
            created_at_ts = None

    return {
        "tweet_id": tweet.tweet_id,
        "username": tweet.username,
        "display_name": tweet.display_name,
        "user_verified": tweet.user_verified,
        "text": tweet.text,
        "hashtags": tweet.hashtags or [],
        "mentions": tweet.mentions or [],
        "media_urls": tweet.media_urls or [],
        "likes": tweet.likes,
        "retweets": tweet.retweets,
        "replies_count": tweet.replies,
        "quotes": tweet.quotes,
        "views": tweet.views,
        "is_reply": tweet.is_reply,
        "is_retweet": tweet.is_retweet,
        "is_quote": tweet.is_quote,
        "reply_to_tweet_id": tweet.reply_to_tweet_id,
        "quoted_tweet_id": tweet.quoted_tweet_id,
        "created_at": created_at_ts,
        "scraped_at": scraped_at,
        "scrape_query": scrape_query,
        "raw_data": tweet.to_dict() if hasattr(tweet, "to_dict") else {},
        # season_id, job_id, show_id, person_id intentionally omitted (NULL)
    }
