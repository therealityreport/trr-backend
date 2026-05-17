"""
Standalone (non-season) Twitter search persistence.

Provides tweet upsert helpers plus per-query scrape provenance so repeated
hashtag and mention searches can preserve membership history.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from trr_backend.db import pg
from trr_backend.socials.control_plane import _adapt_payload_json_values, _pg_upsert_many
from trr_backend.socials.twitter.scraper import Tweet

logger = logging.getLogger(__name__)


def upsert_standalone_tweets(
    tweets: list[Tweet],
    *,
    scrape_query: str,
    conn: Any | None = None,
) -> list[dict[str, Any]]:
    """Upsert Tweet objects into social.twitter_tweets."""
    if not tweets:
        return []

    now = datetime.now(UTC).isoformat()
    payloads = [_tweet_to_payload(tweet, scrape_query=scrape_query, scraped_at=now) for tweet in tweets]
    rows = _pg_upsert_many("twitter_tweets", payloads, conflict_col="tweet_id", conn=conn)
    logger.info("upsert_standalone_tweets: %d upserted for query %r", len(rows), scrape_query)
    return rows


def persist_standalone_twitter_search(
    tweets: list[Tweet],
    *,
    raw_query: str,
    normalized_search_query: str,
    scrape_query_label: str,
    window_start_day: str,
    window_end_day_exclusive: str,
    requested_via: str,
    retrieval_meta: dict[str, Any] | None,
    complete: bool,
) -> dict[str, Any]:
    """Persist tweet rows plus scrape-run provenance for one standalone search."""
    retrieval_payload = dict(retrieval_meta or {})
    deduped_tweets = _dedupe_tweets_by_id(tweets)
    posts_checked = int(retrieval_payload.get("posts_checked") or 0)

    with pg.db_connection() as conn:
        upserted_rows = upsert_standalone_tweets(
            deduped_tweets,
            scrape_query=scrape_query_label,
            conn=conn,
        )
        run_row = _insert_scrape_query_run(
            {
                "scrape_query_label": scrape_query_label,
                "raw_query": raw_query,
                "normalized_search_query": normalized_search_query,
                "window_start_day": window_start_day,
                "window_end_day_exclusive": window_end_day_exclusive,
                "requested_via": requested_via,
                "complete": bool(complete),
                "posts_checked": posts_checked,
                "tweets_found": len(deduped_tweets),
                "retrieval_meta": retrieval_payload,
                "updated_at": datetime.now(UTC).isoformat(),
            },
            conn=conn,
        )
        membership_rows = _insert_scrape_query_memberships(
            str(run_row["id"]),
            [tweet.tweet_id for tweet in deduped_tweets],
            conn=conn,
        )

    return {
        "requested": True,
        "succeeded": True,
        "scrape_query_label": scrape_query_label,
        "scrape_run_id": str(run_row["id"]),
        "tweets_upserted": len(upserted_rows),
        "tweet_memberships_created": len(membership_rows),
        "tweet_memberships_total": len(deduped_tweets),
        "requested_via": requested_via,
        "error": None,
    }


def _dedupe_tweets_by_id(tweets: list[Tweet]) -> list[Tweet]:
    deduped: list[Tweet] = []
    seen_tweet_ids: set[str] = set()
    for tweet in tweets:
        tweet_id = str(getattr(tweet, "tweet_id", "") or "").strip()
        if not tweet_id or tweet_id in seen_tweet_ids:
            continue
        seen_tweet_ids.add(tweet_id)
        deduped.append(tweet)
    return deduped


def _insert_scrape_query_run(payload: dict[str, Any], *, conn: Any | None = None) -> dict[str, Any]:
    columns = list(payload.keys())
    placeholders = ", ".join(["%s"] * len(columns))
    col_list = ", ".join(columns)
    adapted = _adapt_payload_json_values(payload)
    sql = f"""
        INSERT INTO social.twitter_scrape_queries ({col_list})
        VALUES ({placeholders})
        RETURNING *
    """
    with pg.db_cursor(conn=conn) as cur:
        return pg.fetch_one_with_cursor(cur, sql, [adapted[column] for column in columns])


def _insert_scrape_query_memberships(
    scrape_query_id: str,
    tweet_ids: list[str],
    *,
    conn: Any | None = None,
) -> list[dict[str, Any]]:
    if not tweet_ids:
        return []

    rows = [(scrape_query_id, tweet_id) for tweet_id in tweet_ids if str(tweet_id or "").strip()]
    if not rows:
        return []

    sql = (
        "INSERT INTO social.twitter_scrape_query_tweets (scrape_query_id, tweet_id) "
        "VALUES %s "
        "ON CONFLICT (scrape_query_id, tweet_id) DO NOTHING "
        "RETURNING *"
    )
    return pg.execute_values_returning(sql, rows, conn=conn)


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
        "bookmarks": int(getattr(tweet, "bookmarks", 0) or 0),
        "shares": int(getattr(tweet, "shares", 0) or 0) or int(getattr(tweet, "retweets", 0) or 0),
        "is_reply": tweet.is_reply,
        "is_retweet": tweet.is_retweet,
        "is_quote": tweet.is_quote,
        "reply_to_tweet_id": tweet.reply_to_tweet_id,
        "quoted_tweet_id": tweet.quoted_tweet_id,
        "thread_root_tweet_id": getattr(tweet, "thread_root_tweet_id", None),
        "thread_position": getattr(tweet, "thread_position", None),
        "is_thread_part": bool(getattr(tweet, "is_thread_part", False)),
        "twitter_context_role": str(getattr(tweet, "twitter_context_role", "") or "").strip() or None,
        "created_at": created_at_ts,
        "scraped_at": scraped_at,
        "scrape_query": scrape_query,
        "raw_data": tweet.to_dict() if hasattr(tweet, "to_dict") else {},
        # season_id, job_id, show_id, person_id intentionally omitted (NULL)
    }
