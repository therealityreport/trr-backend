"""Direct admin Twitter/X scrape operations.

This module owns route-facing Twitter/X orchestration without importing legacy
shared-account catalog repositories. Shared-account catalog scraping remains in
``trr_backend.socials.twitter.posts_catalog``.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from fastapi import HTTPException

from trr_backend.socials.twitter.diagnostics import safe_quote_fetch_metadata, safe_retrieval_metadata
from trr_backend.socials.twitter.models import TwitterPersistenceSummary

logger = logging.getLogger(__name__)


def tweet_to_payload(tweet: Any) -> dict[str, Any]:
    """Convert a Tweet-like object into the route-visible tweet payload."""
    return {
        "tweet_id": _read(tweet, "tweet_id"),
        "date_time": _read(tweet, "date_time"),
        "text": _read(tweet, "text"),
        "hashtags": _list_value(_read(tweet, "hashtags")),
        "mentions": _list_value(_read(tweet, "mentions")),
        "likes": _read(tweet, "likes", 0),
        "retweets": _read(tweet, "retweets", 0),
        "replies": _read(tweet, "replies", 0),
        "quotes": _read(tweet, "quotes", 0),
        "views": _read(tweet, "views", 0),
        "bookmarks": _read(tweet, "bookmarks", 0),
        "shares": _read(tweet, "shares", 0) or _read(tweet, "retweets", 0),
        "url": _read(tweet, "url"),
        "username": _read(tweet, "username"),
        "display_name": _read(tweet, "display_name"),
        "user_verified": _read(tweet, "user_verified", False),
        "is_reply": _read(tweet, "is_reply", False),
        "is_retweet": _read(tweet, "is_retweet", False),
        "is_quote": _read(tweet, "is_quote", False),
        "thread_root_tweet_id": _read(tweet, "thread_root_tweet_id"),
        "thread_position": _read(tweet, "thread_position"),
        "is_thread_part": _read(tweet, "is_thread_part", False),
        "twitter_context_role": _read(tweet, "twitter_context_role"),
        "media_urls": _list_value(_read(tweet, "media_urls")),
        "hosted_media_urls": _list_value(_read(tweet, "hosted_media_urls")),
    }


def search_twitter(
    request: Any,
    *,
    load_auth: Callable[[], tuple[Any, Any, Any]],
    persist_search: Callable[..., dict[str, Any]],
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """Run direct Twitter/X search and return the current route response shape."""
    operation_logger = logger or globals()["logger"]

    from trr_backend.socials import twitter as twitter_module

    config = twitter_module.TwitterScrapeConfig(
        query=_read(request, "query"),
        date_start=_read(request, "date_start"),
        date_end=_read(request, "date_end"),
        include_replies=bool(_read(request, "include_replies", False)),
        include_links=bool(_read(request, "include_links", True)),
        delay_seconds=float(_read(request, "delay_seconds", 2.0)),
        max_pages=_read(request, "max_pages"),
        show_id=_read(request, "show_id"),
        season_number=_read(request, "season_number"),
        person_id=_read(request, "person_id"),
    )
    search_query_used = config.build_search_query()

    try:
        twitter_cookies, twitter_bearer, twikit_creds = _load_auth_values(load_auth)
        scraper = twitter_module.TwitterScraper(
            cookies=twitter_cookies,
            bearer_token=twitter_bearer,
            twikit_credentials=twikit_creds,
        )
        tweets = scraper.scrape(config)
        retrieval_meta = safe_retrieval_metadata(getattr(scraper, "last_retrieval_meta", {}) or {})
        complete = bool(retrieval_meta.get("complete"))

        if bool(_read(request, "mirror_to_s3", False)):
            twitter_module.mirror_tweet_media(tweets)

        persist_summary: dict[str, Any] | None = None
        if bool(_read(request, "persist", False)):
            label = _persist_label(request)
            try:
                persist_summary = persist_search(
                    tweets,
                    raw_query=_read(request, "query"),
                    normalized_search_query=search_query_used,
                    scrape_query_label=label,
                    window_start_day=config.window_start_day(),
                    window_end_day_exclusive=config.window_end_day_exclusive(),
                    requested_via="api",
                    retrieval_meta=retrieval_meta,
                    complete=complete,
                )
            except Exception as upsert_err:  # noqa: BLE001
                operation_logger.warning(
                    "persist_standalone_twitter_search failed for query %r: %s",
                    label,
                    upsert_err,
                )
                persist_summary = TwitterPersistenceSummary.failed(
                    scrape_query_label=label,
                    tweet_memberships_total=len(tweets),
                    error=upsert_err,
                    requested_via="api",
                ).to_payload()

        return {
            "success": True,
            "query": _read(request, "query"),
            "tweets_found": len(tweets),
            "tweets": [tweet_to_payload(tweet) for tweet in tweets],
            "search_query_used": search_query_used,
            "filters_applied": {
                "query": _read(request, "query"),
                "date_start": _read(request, "date_start").isoformat(),
                "date_end": _read(request, "date_end").isoformat(),
                "window_contract": "whole_day",
                "window_start_day": config.window_start_day(),
                "window_end_day_inclusive": config.window_end_day_inclusive(),
                "window_end_day_exclusive": config.window_end_day_exclusive(),
                "include_replies": bool(_read(request, "include_replies", False)),
                "include_links": bool(_read(request, "include_links", True)),
            },
            "retrieval_meta": retrieval_meta,
            "complete": complete,
            "persist_summary": persist_summary,
            "scrape_run_id": _scrape_run_id(persist_summary),
            "error": None,
        }
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        operation_logger.error("Twitter search failed: %s", exc, exc_info=True)
        return {
            "success": False,
            "query": _read(request, "query"),
            "tweets_found": 0,
            "tweets": [],
            "search_query_used": search_query_used,
            "filters_applied": {},
            "retrieval_meta": None,
            "complete": False,
            "persist_summary": None,
            "scrape_run_id": None,
            "error": str(exc),
        }


def fetch_tweet_replies(
    request: Any,
    *,
    load_auth: Callable[[], tuple[Any, Any, Any]],
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """Fetch direct Twitter/X replies and return the current route response shape."""
    operation_logger = logger or globals()["logger"]

    try:
        from trr_backend.socials import twitter as twitter_module

        twitter_cookies, twitter_bearer, twikit_creds = _load_auth_values(load_auth)
        scraper = twitter_module.TwitterScraper(
            cookies=twitter_cookies,
            bearer_token=twitter_bearer,
            twikit_credentials=twikit_creds,
        )
        reply_kwargs: dict[str, Any] = {}
        if _read(request, "search_max_pages") is not None:
            reply_kwargs["search_max_pages"] = _read(request, "search_max_pages")
        if _read(request, "twikit_max_pages") is not None:
            reply_kwargs["twikit_max_pages"] = _read(request, "twikit_max_pages")

        replies = scraper.fetch_tweet_replies(
            _read(request, "tweet_id"),
            _read(request, "delay_seconds", 2.0),
            **reply_kwargs,
        )
        if bool(_read(request, "mirror_to_s3", False)):
            twitter_module.mirror_tweet_media(replies)

        return {
            "success": True,
            "tweet_id": _read(request, "tweet_id"),
            "replies_found": len(replies),
            "replies": [tweet_to_payload(reply) for reply in replies],
            "error": None,
        }
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        operation_logger.error("Twitter replies fetch failed: %s", exc, exc_info=True)
        return {
            "success": False,
            "tweet_id": _read(request, "tweet_id"),
            "replies_found": 0,
            "replies": [],
            "error": str(exc),
        }


def fetch_tweet_quotes(
    request: Any,
    *,
    load_auth: Callable[[], tuple[Any, Any, Any]],
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """Fetch direct Twitter/X quote tweets and return the current route response shape."""
    operation_logger = logger or globals()["logger"]

    try:
        from trr_backend.socials import twitter as twitter_module

        twitter_cookies, twitter_bearer, twikit_creds = _load_auth_values(load_auth)
        scraper = twitter_module.TwitterScraper(
            cookies=twitter_cookies,
            bearer_token=twitter_bearer,
            twikit_credentials=twikit_creds,
        )
        quotes = scraper.fetch_tweet_quotes(
            _read(request, "tweet_id"),
            delay=_read(request, "delay_seconds", 2.0),
            max_pages=_read(request, "max_pages", 60),
        )
        if bool(_read(request, "mirror_to_s3", False)):
            twitter_module.mirror_tweet_media(quotes)

        quote_meta = safe_quote_fetch_metadata(scraper)
        return {
            "success": True,
            "tweet_id": _read(request, "tweet_id"),
            "quotes_found": len(quotes),
            "quotes": [tweet_to_payload(quote) for quote in quotes],
            "source_used": quote_meta.get("source_used"),
            "failure_reason": quote_meta.get("failure_reason"),
            "error": None,
        }
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        operation_logger.error("Twitter quotes fetch failed: %s", exc, exc_info=True)
        return {
            "success": False,
            "tweet_id": _read(request, "tweet_id"),
            "quotes_found": 0,
            "quotes": [],
            "source_used": None,
            "failure_reason": None,
            "error": str(exc),
        }


def _load_auth_values(load_auth: Callable[[], tuple[Any, Any, Any]]) -> tuple[Any, Any, Any]:
    values = load_auth()
    if len(values) == 2:
        twitter_cookies, twitter_bearer = values
        return twitter_cookies, twitter_bearer, None
    twitter_cookies, twitter_bearer, twikit_creds = values
    return twitter_cookies, twitter_bearer, twikit_creds


def _persist_label(request: Any) -> str:
    query = str(_read(request, "query") or "")
    return str(_read(request, "scrape_query") or query).strip() or query


def _scrape_run_id(persist_summary: dict[str, Any] | None) -> str | None:
    if isinstance(persist_summary, dict) and persist_summary.get("scrape_run_id"):
        return str(persist_summary["scrape_run_id"])
    return None


def _read(value: Any, field: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(field, default)
    return getattr(value, field, default)


def _list_value(value: Any) -> list[Any]:
    return list(value or [])
