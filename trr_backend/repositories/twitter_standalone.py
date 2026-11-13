"""
Standalone (non-season) Twitter search persistence.

Provides tweet upsert helpers plus per-query scrape provenance so repeated
hashtag and mention searches can preserve membership history.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any, cast

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
    from trr_backend.socials.twitter.public_models import PublicPost

    if any(isinstance(tweet, PublicPost) for tweet in tweets):
        raise ValueError("Public observations require the verified public persistence path")
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
    public_acceptance: dict | None = None,
) -> dict[str, Any]:
    """Persist tweet rows plus scrape-run provenance for one standalone search."""
    retrieval_payload = dict(retrieval_meta or {})
    deduped_tweets = _dedupe_tweets_by_id(tweets)
    posts_checked = int(retrieval_payload.get("posts_checked") or 0)

    with pg.db_connection() as conn:
        if public_acceptance is not None:
            upserted_rows = upsert_public_tweets(
                deduped_tweets, scrape_query=scrape_query_label, conn=conn, acceptance=public_acceptance
            )
        else:
            upserted_rows = upsert_standalone_tweets(deduped_tweets, scrape_query=scrape_query_label, conn=conn)
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
        return cast("dict[str, Any]", pg.fetch_one_with_cursor(cur, sql, [adapted[column] for column in columns]))


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
    from trr_backend.socials.twitter.public_models import PublicPost

    if isinstance(tweet, PublicPost):
        created_at_ts = tweet.date_time
    elif tweet.created_at is not None:
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
        "bookmarks": getattr(tweet, "bookmarks", None),
        "shares": getattr(tweet, "shares", None),
        "is_reply": tweet.is_reply,
        "is_retweet": tweet.is_retweet,
        "is_quote": tweet.is_quote,
        "reply_to_tweet_id": tweet.reply_to_tweet_id,
        "quoted_tweet_id": tweet.quoted_tweet_id,
        "thread_root_tweet_id": getattr(tweet, "thread_root_tweet_id", None),
        "thread_position": getattr(tweet, "thread_position", None),
        "is_thread_part": getattr(tweet, "is_thread_part", None),
        "twitter_context_role": (
            {"root": "reply_parent", "thread": "account_reply", "reply": "audience_reply"}.get(
                getattr(tweet, "twitter_context_role", None), getattr(tweet, "twitter_context_role", None)
            )
        ),
        "created_at": created_at_ts,
        "scraped_at": scraped_at,
        "scrape_query": scrape_query,
        "raw_data": tweet.to_dict() if hasattr(tweet, "to_dict") else {},
        # season_id, job_id, show_id, person_id intentionally omitted (NULL)
    }


def public_upsert_sql(columns: list[str]) -> str:
    """Known stored fields survive unavailable public observations; zero remains a value."""
    allowed = {
        "tweet_id",
        "username",
        "display_name",
        "user_verified",
        "text",
        "hashtags",
        "mentions",
        "media_urls",
        "likes",
        "retweets",
        "replies_count",
        "quotes",
        "views",
        "bookmarks",
        "shares",
        "is_reply",
        "is_retweet",
        "is_quote",
        "reply_to_tweet_id",
        "quoted_tweet_id",
        "thread_root_tweet_id",
        "thread_position",
        "is_thread_part",
        "twitter_context_role",
        "created_at",
        "scraped_at",
        "scrape_query",
        "raw_data",
    }
    if not columns or not set(columns) <= allowed or len(columns) != len(set(columns)):
        raise ValueError("Invalid public persistence columns")
    updates = []
    for key in columns:
        if key == "tweet_id":
            continue
        new = f'EXCLUDED."{key}"'
        if key in {"hashtags", "mentions", "media_urls"}:
            new = f"NULLIF({new}, '[]'::jsonb)"
        if key in {"text", "display_name", "username"}:
            new = f"NULLIF({new}, '')"
        if key == "raw_data":
            # Preserve the observation that justifies each retained known field.
            metrics = {
                "likes": "likes",
                "retweets": "retweets",
                "replies_count": "replies",
                "quotes": "quotes",
                "views": "views",
                "bookmarks": "bookmarks",
                "shares": "shares",
                "created_at": "date_time",
                "user_verified": "user_verified",
                "is_reply": "is_reply",
                "is_retweet": "is_retweet",
                "is_quote": "is_quote",
                "is_thread_part": "is_thread_part",
                "display_name": "display_name",
            }
            fields = ", ".join(
                f"'{field}', CASE WHEN EXCLUDED.\"{column}\" IS NOT NULL THEN EXCLUDED.raw_data->'evidence' "
                f"ELSE COALESCE(stored.raw_data->'field_evidence'->'{field}', stored.raw_data->'evidence') END"
                for column, field in metrics.items()
            )
            updates.append(
                "raw_data = EXCLUDED.raw_data || jsonb_build_object('field_evidence', jsonb_build_object("
                + fields
                + "))"
            )
        else:
            updates.append(f'"{key}" = COALESCE({new}, stored."{key}")')
    names = ", ".join(f'"{key}"' for key in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    return (
        f"INSERT INTO social.twitter_tweets AS stored ({names}) VALUES ({placeholders}) "
        f"ON CONFLICT (tweet_id) DO UPDATE SET {', '.join(updates)} "
        "WHERE stored.season_id IS NULL AND stored.show_id IS NULL "
        "AND stored.person_id IS NULL AND stored.job_id IS NULL RETURNING *"
    )


def upsert_public_tweets(tweets, *, scrape_query: str, conn, acceptance: dict) -> list[dict]:
    """Internal gated path: acceptance evidence is supplied by the owning release/job.

    The admin research API deliberately cannot enable this with a request flag.
    A caller owns a transaction and must supply verified live and schema checks.
    """
    required = {"rhoslc_search", "bravotv_profile", "nullable_schema", "known_value_merge", "consumer_nulls"}
    if not isinstance(acceptance, dict) or not all(acceptance.get(key) is True for key in required):
        raise ValueError("Public persistence acceptance gates have not passed")
    from trr_backend.socials.twitter.public_models import PublicPost

    if not all(isinstance(tweet, PublicPost) for tweet in tweets):
        raise ValueError("Expected nullable public observations")
    rows = []
    for tweet in _dedupe_tweets_by_id(tweets):
        payload = _tweet_to_payload(tweet, scrape_query=scrape_query, scraped_at=datetime.now(UTC).isoformat())
        adapted = _adapt_payload_json_values(payload)
        with pg.db_cursor(conn=conn) as cur:
            row = pg.fetch_one_with_cursor(cur, public_upsert_sql(list(payload)), list(adapted.values()))
            if row is not None:
                rows.append(row)
    return rows
