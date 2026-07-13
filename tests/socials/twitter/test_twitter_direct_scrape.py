from __future__ import annotations

import inspect
import json
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import HTTPException

import trr_backend.socials.twitter.direct_scrape as direct_scrape
from trr_backend.socials.twitter import Tweet
from trr_backend.socials.twitter.scraper import TwitterScrapeConfig, TwitterScraper


def _tweet(
    tweet_id: str,
    *,
    is_reply: bool = False,
    is_quote: bool = False,
    hosted_media_urls: list[str] | None = None,
) -> Tweet:
    return Tweet(
        tweet_id=tweet_id,
        date_time="2026-02-13 12:34:56",
        created_at=1770986096,
        text="tweet body",
        hashtags=["RHOSLC"],
        mentions=["BravoTV"],
        likes=12,
        retweets=1,
        replies=2,
        quotes=3,
        views=100,
        url=f"https://x.com/viewer/status/{tweet_id}",
        username="viewer",
        display_name="Viewer",
        user_verified=False,
        is_reply=is_reply,
        is_retweet=False,
        is_quote=is_quote,
        reply_to_tweet_id="root-1" if is_reply else None,
        quoted_tweet_id="root-1" if is_quote else None,
        media_urls=["https://video.twimg.com/media/video-1.mp4"],
        hosted_media_urls=hosted_media_urls or [],
    )


def _search_request(**overrides: Any) -> SimpleNamespace:
    values = {
        "query": "#RHOSLC",
        "date_start": datetime(2026, 2, 1, 9, 15, tzinfo=UTC),
        "date_end": datetime(2026, 2, 10, 17, 45, tzinfo=UTC),
        "include_replies": False,
        "include_links": True,
        "mirror_to_s3": False,
        "delay_seconds": 0.5,
        "max_pages": 3,
        "show_id": None,
        "season_number": None,
        "person_id": None,
        "persist": False,
        "scrape_query": None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_tweet_to_payload_preserves_current_route_tweet_fields() -> None:
    payload = direct_scrape.tweet_to_payload(_tweet("tweet-1", hosted_media_urls=["https://cdn.example/media.mp4"]))

    assert payload == {
        "tweet_id": "tweet-1",
        "date_time": "2026-02-13 12:34:56",
        "text": "tweet body",
        "hashtags": ["RHOSLC"],
        "mentions": ["BravoTV"],
        "likes": 12,
        "retweets": 1,
        "replies": 2,
        "quotes": 3,
        "views": 100,
        "bookmarks": 0,
        "shares": 1,
        "url": "https://x.com/viewer/status/tweet-1",
        "username": "viewer",
        "display_name": "Viewer",
        "user_verified": False,
        "is_reply": False,
        "is_retweet": False,
        "is_quote": False,
        "thread_root_tweet_id": None,
        "thread_position": None,
        "is_thread_part": False,
        "twitter_context_role": None,
        "media_urls": ["https://video.twimg.com/media/video-1.mp4"],
        "hosted_media_urls": ["https://cdn.example/media.mp4"],
    }


def test_search_twitter_uses_injected_auth_mirroring_and_persistence(monkeypatch: pytest.MonkeyPatch) -> None:
    tweet = _tweet("tweet-search")
    persist_calls: list[dict[str, Any]] = []
    mirror_calls: list[list[Tweet]] = []

    class FakeTwitterScraper:
        instances: list[FakeTwitterScraper] = []

        def __init__(self, *, cookies: Any, bearer_token: Any, twikit_credentials: Any) -> None:
            self.cookies = cookies
            self.bearer_token = bearer_token
            self.twikit_credentials = twikit_credentials
            self.last_retrieval_meta = {
                "complete": True,
                "posts_checked": 7,
                "stop_reason": "no_cursor",
                "fallback_attempts": [{"transport": "graphql", "authorization": "Bearer secret"}],
                "cookies": {"auth_token": "secret-cookie"},
            }
            self.scrape_configs: list[Any] = []
            self.instances.append(self)

        def scrape(self, config: Any) -> list[Tweet]:
            self.scrape_configs.append(config)
            return [tweet]

    def _mirror(tweets: list[Tweet]) -> dict[str, list[str]]:
        mirror_calls.append(tweets)
        tweets[0].hosted_media_urls = ["https://cdn.example.com/tweet-search.mp4"]
        return {tweets[0].tweet_id: tweets[0].hosted_media_urls}

    def _persist(tweets: list[Tweet], **kwargs: Any) -> dict[str, Any]:
        persist_calls.append({"tweets": tweets, **kwargs})
        return {
            "requested": True,
            "succeeded": True,
            "scrape_query_label": kwargs["scrape_query_label"],
            "scrape_run_id": "run-direct",
            "tweets_upserted": len(tweets),
            "tweet_memberships_created": len(tweets),
            "tweet_memberships_total": len(tweets),
            "requested_via": kwargs["requested_via"],
            "error": None,
        }

    monkeypatch.setattr("trr_backend.socials.twitter.TwitterScraper", FakeTwitterScraper)
    monkeypatch.setattr("trr_backend.socials.twitter.mirror_tweet_media", _mirror)

    response = direct_scrape.search_twitter(
        _search_request(mirror_to_s3=True, persist=True, scrape_query="RHOSLC-S4"),
        load_auth=lambda: ({"auth_token": "cookie-auth"}, "bearer-token", {"auth_token": "twikit-auth"}),
        persist_search=_persist,
    )

    assert response["success"] is True
    assert response["tweets_found"] == 1
    assert response["tweets"][0]["hosted_media_urls"] == ["https://cdn.example.com/tweet-search.mp4"]
    assert response["search_query_used"] == "#RHOSLC since:2026-02-01 until:2026-02-11"
    assert response["filters_applied"]["window_contract"] == "whole_day"
    assert response["retrieval_meta"]["posts_checked"] == 7
    assert response["retrieval_meta"]["fallback_attempts"] == [{"transport": "graphql", "authorization": "[redacted]"}]
    assert "cookies" not in response["retrieval_meta"]
    assert response["complete"] is True
    assert response["persist_summary"]["scrape_run_id"] == "run-direct"
    assert response["scrape_run_id"] == "run-direct"
    assert len(mirror_calls) == 1
    assert len(persist_calls) == 1
    assert persist_calls[0]["scrape_query_label"] == "RHOSLC-S4"
    assert persist_calls[0]["window_start_day"] == "2026-02-01"
    assert persist_calls[0]["window_end_day_exclusive"] == "2026-02-11"
    assert FakeTwitterScraper.instances[0].cookies == {"auth_token": "cookie-auth"}
    assert FakeTwitterScraper.instances[0].bearer_token == "bearer-token"
    assert FakeTwitterScraper.instances[0].twikit_credentials == {"auth_token": "twikit-auth"}


def test_search_twitter_persistence_failure_isolated_in_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeTwitterScraper:
        def __init__(self, **_kwargs: Any) -> None:
            self.last_retrieval_meta = {"complete": False, "posts_checked": 5, "stop_reason": "max_pages_reached"}

        def scrape(self, _config: Any) -> list[Tweet]:
            return [_tweet("tweet-fail")]

    monkeypatch.setattr("trr_backend.socials.twitter.TwitterScraper", FakeTwitterScraper)

    response = direct_scrape.search_twitter(
        _search_request(persist=True),
        load_auth=lambda: ({}, None, None),
        persist_search=lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("db write failed")),
    )

    assert response["success"] is True
    assert response["complete"] is False
    assert response["persist_summary"] == {
        "requested": True,
        "succeeded": False,
        "scrape_query_label": "#RHOSLC",
        "scrape_run_id": None,
        "tweets_upserted": 0,
        "tweet_memberships_created": 0,
        "tweet_memberships_total": 1,
        "requested_via": "api",
        "error": "db write failed",
    }


def test_search_twitter_re_raises_http_exception_from_auth_loader() -> None:
    with pytest.raises(HTTPException) as exc_info:
        direct_scrape.search_twitter(
            _search_request(),
            load_auth=lambda: (_ for _ in ()).throw(HTTPException(status_code=401, detail="auth failed")),
            persist_search=lambda *_args, **_kwargs: {},
        )

    assert exc_info.value.status_code == 401
    assert exc_info.value.detail == "auth failed"


def test_fetch_replies_passes_page_budgets_and_optional_mirroring(monkeypatch: pytest.MonkeyPatch) -> None:
    reply = _tweet("reply-1", is_reply=True)
    captured: dict[str, Any] = {}

    class FakeTwitterScraper:
        def __init__(self, **_kwargs: Any) -> None:
            pass

        def fetch_tweet_replies(self, tweet_id: str, delay: float, **kwargs: Any) -> list[Tweet]:
            captured.update({"tweet_id": tweet_id, "delay": delay, **kwargs})
            return [reply]

    def _mirror(tweets: list[Tweet]) -> dict[str, list[str]]:
        tweets[0].hosted_media_urls = ["https://cdn.example.com/reply-1.mp4"]
        return {tweets[0].tweet_id: tweets[0].hosted_media_urls}

    monkeypatch.setattr("trr_backend.socials.twitter.TwitterScraper", FakeTwitterScraper)
    monkeypatch.setattr("trr_backend.socials.twitter.mirror_tweet_media", _mirror)

    response = direct_scrape.fetch_tweet_replies(
        SimpleNamespace(
            tweet_id="root-1",
            delay_seconds=0.75,
            search_max_pages=19,
            twikit_max_pages=11,
            mirror_to_s3=True,
        ),
        load_auth=lambda: ({}, None, None),
    )

    assert response["success"] is True
    assert response["tweet_id"] == "root-1"
    assert response["replies_found"] == 1
    assert response["replies"][0]["hosted_media_urls"] == ["https://cdn.example.com/reply-1.mp4"]
    assert captured == {
        "tweet_id": "root-1",
        "delay": 0.75,
        "search_max_pages": 19,
        "twikit_max_pages": 11,
    }


def test_fetch_replies_re_raises_http_exception_from_auth_loader() -> None:
    with pytest.raises(HTTPException) as exc_info:
        direct_scrape.fetch_tweet_replies(
            SimpleNamespace(tweet_id="root-1", delay_seconds=0.5, mirror_to_s3=False),
            load_auth=lambda: (_ for _ in ()).throw(HTTPException(status_code=403, detail="forbidden")),
        )

    assert exc_info.value.status_code == 403


def test_fetch_quotes_preserves_source_diagnostics(monkeypatch: pytest.MonkeyPatch) -> None:
    quote = _tweet("quote-1", is_quote=True)
    captured: dict[str, Any] = {}

    class FakeTwitterScraper:
        def __init__(self, **_kwargs: Any) -> None:
            self.last_quote_fetch_meta = {
                "source_used": "twikit",
                "failure_reason": None,
                "attempts": [{"source": "graphql", "cookie": "secret"}],
            }
            self.last_quote_fetch_reason = None

        def fetch_tweet_quotes(self, tweet_id: str, *, delay: float, max_pages: int) -> list[Tweet]:
            captured.update({"tweet_id": tweet_id, "delay": delay, "max_pages": max_pages})
            return [quote]

    monkeypatch.setattr("trr_backend.socials.twitter.TwitterScraper", FakeTwitterScraper)

    response = direct_scrape.fetch_tweet_quotes(
        SimpleNamespace(tweet_id="root-1", delay_seconds=0.5, max_pages=3, mirror_to_s3=False),
        load_auth=lambda: ({}, None, None),
    )

    assert response["success"] is True
    assert response["quotes_found"] == 1
    assert response["source_used"] == "twikit"
    assert response["failure_reason"] is None
    assert response["quotes"][0]["tweet_id"] == "quote-1"
    assert captured == {"tweet_id": "root-1", "delay": 0.5, "max_pages": 3}


def test_fetch_quotes_re_raises_http_exception_from_auth_loader() -> None:
    with pytest.raises(HTTPException) as exc_info:
        direct_scrape.fetch_tweet_quotes(
            SimpleNamespace(tweet_id="root-1", delay_seconds=0.5, max_pages=3, mirror_to_s3=False),
            load_auth=lambda: (_ for _ in ()).throw(HTTPException(status_code=429, detail="rate limited")),
        )

    assert exc_info.value.status_code == 429


def test_syndication_filter_treats_naive_window_bounds_as_utc(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Response:
        status_code = 200

        def __init__(self, entries: list[dict[str, Any]]) -> None:
            self.text = (
                '<script id="__NEXT_DATA__" type="application/json">'
                + json.dumps({"props": {"pageProps": {"timeline": {"entries": entries}}}})
                + "</script>"
            )

        def raise_for_status(self) -> None:
            return None

    def _entry(tweet_id: str, created_at: str) -> dict[str, Any]:
        return {
            "content": {
                "tweet": {
                    "id_str": tweet_id,
                    "created_at": created_at,
                    "full_text": "#RHOSLC",
                    "user": {"screen_name": "bravo", "name": "Bravo"},
                }
            }
        }

    scraper = TwitterScraper()
    scraper.session = SimpleNamespace(
        get=lambda *_args, **_kwargs: _Response(
            [
                _entry("before-window", "Sat Jan 31 23:59:59 +0000 2026"),
                _entry("start-boundary", "Sun Feb 01 00:00:00 +0000 2026"),
                _entry("end-boundary", "Mon Feb 02 00:00:00 +0000 2026"),
            ]
        )
    )
    monkeypatch.setattr(scraper, "_rate_limit", lambda *_args, **_kwargs: None)

    tweets = scraper._scrape_syndication(
        "bravo",
        TwitterScrapeConfig(query="RHOSLC", date_start=datetime(2026, 2, 1), date_end=datetime(2026, 2, 1)),
    )

    assert [tweet.tweet_id for tweet in tweets] == ["start-boundary"]


def test_direct_scrape_keeps_shared_catalog_and_legacy_repository_out_of_direct_module() -> None:
    source = inspect.getsource(direct_scrape)

    assert "trr_backend.repositories.social_season_analytics" not in source
    assert "from trr_backend.socials.twitter.posts_catalog" not in source
    assert "import trr_backend.socials.twitter.posts_catalog" not in source
    assert "scrape_shared_twitter_posts" not in source
