from __future__ import annotations

import json
import sys
import types
import urllib.parse
from datetime import UTC, datetime
from typing import Any, cast

import requests

from trr_backend.socials.twitter.scraper import Tweet, TwitterScrapeConfig, TwitterScraper


def _tweet(tweet_id: str, *, reply_to: str | None = "root", is_quote: bool = False) -> Tweet:
    return Tweet(
        tweet_id=tweet_id,
        date_time="2026-01-24 19:56:08",
        created_at=int(datetime(2026, 1, 24, 19, 56, 8, tzinfo=UTC).timestamp()),
        text=f"tweet {tweet_id}",
        hashtags=[],
        mentions=[],
        likes=0,
        retweets=0,
        replies=0,
        quotes=0,
        views=0,
        url=f"https://x.com/viewer/status/{tweet_id}",
        username="viewer",
        display_name="Viewer",
        user_verified=False,
        is_reply=bool(reply_to),
        is_retweet=False,
        is_quote=is_quote,
        reply_to_tweet_id=reply_to,
        quoted_tweet_id="root" if is_quote else None,
        media_urls=[],
        thread_root_tweet_id="root" if reply_to else None,
        is_thread_part=bool(reply_to),
    )


def _tweet_detail_payload(entries: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "data": {
            "threaded_conversation_with_injections_v2": {
                "instructions": [
                    {
                        "type": "TimelineAddEntries",
                        "entries": entries,
                    }
                ]
            }
        }
    }


def _conversation_entry(tweet_id: str) -> dict[str, Any]:
    return {
        "entryId": f"conversationthread-{tweet_id}",
        "content": {
            "items": [
                {
                    "item": {
                        "itemContent": {
                            "tweet_results": {
                                "result": {
                                    "rest_id": tweet_id,
                                }
                            }
                        }
                    }
                }
            ]
        },
    }


def _bottom_cursor(value: str) -> dict[str, Any]:
    return {
        "entryId": "cursor-bottom-1",
        "content": {
            "entryType": "TimelineTimelineCursor",
            "cursorType": "Bottom",
            "value": value,
        },
    }


def _search_timeline_payload(entries: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "data": {
            "search_by_raw_query": {
                "search_timeline": {
                    "timeline": {
                        "instructions": [
                            {
                                "type": "TimelineAddEntries",
                                "entries": entries,
                            }
                        ]
                    }
                }
            }
        }
    }


def test_extract_tweet_detail_replies_returns_bottom_cursor(monkeypatch) -> None:
    scraper = TwitterScraper(cookies={"auth_token": "auth", "ct0": "csrf"})

    def _parse_tweet_result(result: dict[str, Any], _config: Any) -> Tweet:
        tweet_id = str(result["rest_id"])
        return _tweet(tweet_id, reply_to=None if tweet_id == "root" else "root")

    monkeypatch.setattr(scraper, "_parse_tweet_result", _parse_tweet_result)

    replies, cursor = scraper._extract_replies_from_tweet_detail_payload(
        payload=_tweet_detail_payload(
            [
                {
                    "entryId": "tweet-root",
                    "content": {"itemContent": {"tweet_results": {"result": {"rest_id": "root"}}}},
                },
                _conversation_entry("reply-1"),
                _bottom_cursor("cursor-1"),
            ]
        ),
        tweet_id="root",
        seen_ids=set(),
    )

    assert [tweet.tweet_id for tweet in replies] == ["reply-1"]
    assert cursor == "cursor-1"


def test_fetch_tweet_replies_paginates_tweet_detail_cursor(monkeypatch) -> None:
    scraper = TwitterScraper(cookies={"auth_token": "auth", "ct0": "csrf"})
    scraper._detail_hash = "detail-hash"
    requested_cursors: list[str | None] = []

    def _parse_tweet_result(result: dict[str, Any], _config: Any) -> Tweet:
        tweet_id = str(result["rest_id"])
        return _tweet(tweet_id)

    class FakeResponse:
        def __init__(self, payload: dict[str, Any]) -> None:
            self.status_code = 200
            self._payload = payload

        def json(self) -> dict[str, Any]:
            return self._payload

        def raise_for_status(self) -> None:
            return None

    def _get(url: str, **_kwargs: Any) -> FakeResponse:
        query = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        variables = json.loads(query["variables"][0])
        requested_cursors.append(variables.get("cursor"))
        if variables.get("cursor") is None:
            return FakeResponse(_tweet_detail_payload([_conversation_entry("reply-1"), _bottom_cursor("cursor-1")]))
        return FakeResponse(_tweet_detail_payload([_conversation_entry("reply-2")]))

    monkeypatch.setattr(scraper, "_parse_tweet_result", _parse_tweet_result)
    monkeypatch.setattr(scraper.session, "get", _get)
    monkeypatch.setattr(scraper, "_rate_limit", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        scraper,
        "_fetch_tweet_replies_via_search",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("Search fallback should not run")),
    )

    replies = scraper.fetch_tweet_replies("root", delay=0, search_max_pages=3)

    assert [tweet.tweet_id for tweet in replies] == ["reply-1", "reply-2"]
    assert requested_cursors == [None, "cursor-1"]
    assert scraper.last_reply_fetch_reason is None


def test_fetch_tweet_replies_merges_recency_pass_for_large_threads(monkeypatch) -> None:
    scraper = TwitterScraper(cookies={"auth_token": "auth", "ct0": "csrf"})
    scraper._detail_hash = "detail-hash"
    requested_rankings: list[str] = []

    def _parse_tweet_result(result: dict[str, Any], _config: Any) -> Tweet:
        return _tweet(str(result["rest_id"]))

    class FakeResponse:
        status_code = 200

        def __init__(self, payload: dict[str, Any]) -> None:
            self._payload = payload

        def json(self) -> dict[str, Any]:
            return self._payload

        def raise_for_status(self) -> None:
            return None

    def _get(url: str, **_kwargs: Any) -> FakeResponse:
        query = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        variables = json.loads(query["variables"][0])
        ranking = str(variables.get("rankingMode") or "")
        requested_rankings.append(ranking)
        if ranking == "Recency":
            return FakeResponse(_tweet_detail_payload([_conversation_entry("recency-reply")]))
        return FakeResponse(_tweet_detail_payload([_conversation_entry("relevance-reply")]))

    monkeypatch.setattr(scraper, "_parse_tweet_result", _parse_tweet_result)
    monkeypatch.setattr(scraper.session, "get", _get)
    monkeypatch.setattr(scraper, "_rate_limit", lambda *_args, **_kwargs: None)

    replies = scraper.fetch_tweet_replies("root", delay=0, search_max_pages=6)

    assert [tweet.tweet_id for tweet in replies] == ["relevance-reply", "recency-reply"]
    assert requested_rankings == ["Relevance", "Recency"]


def test_fetch_tweet_quotes_forwards_progress_callback_to_search(monkeypatch) -> None:
    scraper = TwitterScraper(cookies={"auth_token": "auth", "ct0": "csrf"})
    progress_events: list[dict[str, Any]] = []

    def _fetch_tweet_quotes_via_search(
        *,
        tweet_id: str,
        delay: float,
        max_pages: int = 5,
        progress_callback=None,
    ) -> list[Tweet]:
        assert tweet_id == "root"
        assert delay == 0
        assert max_pages == 6
        if progress_callback:
            progress_callback(
                {
                    "phase": "quote_search_page",
                    "pages_scanned": 1,
                    "quotes_fetched": 1,
                    "tweet_id": tweet_id,
                }
            )
        return [_tweet("quote-1", reply_to=None, is_quote=True)]

    monkeypatch.setattr(scraper, "_fetch_tweet_quotes_via_search", _fetch_tweet_quotes_via_search)
    monkeypatch.setattr(
        scraper,
        "_fetch_quotes_via_tweet_detail",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("Detail fallback should not run")),
    )

    quotes = scraper.fetch_tweet_quotes("root", delay=0, max_pages=6, progress_callback=progress_events.append)

    assert [tweet.tweet_id for tweet in quotes] == ["quote-1"]
    assert progress_events == [
        {
            "phase": "quote_search_page",
            "pages_scanned": 1,
            "quotes_fetched": 1,
            "tweet_id": "root",
        }
    ]


def test_search_timeline_reply_fallback_accepts_thread_root_without_direct_reply(monkeypatch) -> None:
    scraper = TwitterScraper(cookies={"auth_token": "auth", "ct0": "csrf"})

    def _parse_tweet_result(result: dict[str, Any], _config: Any) -> Tweet:
        tweet = _tweet(str(result["rest_id"]), reply_to=None)
        tweet.is_reply = True
        tweet.thread_root_tweet_id = "root"
        tweet.is_thread_part = True
        return tweet

    monkeypatch.setattr(scraper, "_ensure_auth", lambda: None)
    monkeypatch.setattr(scraper, "_parse_tweet_result", _parse_tweet_result)
    monkeypatch.setattr(
        scraper,
        "_fetch_search",
        lambda *_args, **_kwargs: _search_timeline_payload(
            [
                {
                    "entryId": "tweet-reply-with-root",
                    "content": {
                        "itemContent": {
                            "tweet_results": {
                                "result": {
                                    "rest_id": "reply-with-root",
                                }
                            }
                        }
                    },
                }
            ]
        ),
    )

    replies = scraper._fetch_tweet_replies_via_search(tweet_id="root", delay=0, max_pages=1)

    assert [reply.tweet_id for reply in replies] == ["reply-with-root"]


def test_search_timeline_404_latches_unavailable_after_rediscovery(monkeypatch) -> None:
    scraper = TwitterScraper(cookies={"auth_token": "auth", "ct0": "csrf"})
    scraper._search_hash = "stale-search"
    scraper._detail_hash = "detail"
    requested_urls: list[str] = []
    rediscoveries = 0

    class FakeResponse:
        status_code = 404

        def raise_for_status(self) -> None:
            error = requests.exceptions.HTTPError("404 Client Error")
            error.response = cast(Any, self)
            raise error

        def json(self) -> dict[str, Any]:
            return {}

    def _get(url: str, **_kwargs: Any) -> FakeResponse:
        requested_urls.append(url)
        return FakeResponse()

    def _discover() -> None:
        nonlocal rediscoveries
        rediscoveries += 1
        scraper._search_hash = "rediscovered-search"
        scraper._detail_hash = "detail"
        scraper._user_by_screen_name_hash = "user"

    monkeypatch.setattr(scraper.session, "get", _get)
    monkeypatch.setattr(scraper, "_ensure_auth", lambda: None)
    monkeypatch.setattr(scraper, "_rate_limit", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(scraper, "_discover_graphql_hashes", _discover)

    replies = scraper._fetch_tweet_replies_via_search(tweet_id="root", delay=0, max_pages=1)

    assert replies == []
    assert scraper._search_timeline_unavailable is True
    assert scraper.last_reply_fetch_reason == "http_404"
    assert rediscoveries == 1
    assert len(requested_urls) == 2


def test_search_timeline_unavailable_skips_later_reply_and_quote_requests(monkeypatch) -> None:
    scraper = TwitterScraper(cookies={"auth_token": "auth", "ct0": "csrf"})
    scraper._mark_search_timeline_unavailable("http_404")

    monkeypatch.setattr(
        scraper.session,
        "get",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("SearchTimeline should be skipped")),
    )

    replies = scraper._fetch_tweet_replies_via_search(tweet_id="root", delay=0, max_pages=1)
    quotes = scraper._fetch_tweet_quotes_via_search(tweet_id="root", delay=0, max_pages=1)

    assert replies == []
    assert quotes == []
    assert scraper.last_reply_fetch_reason == "http_404"
    assert scraper.last_quote_fetch_reason == "http_404"


def test_twikit_client_transaction_failure_latches_unavailable(monkeypatch) -> None:
    created_clients = 0

    class FakeTwikitClient:
        def __init__(self, _locale: str) -> None:
            nonlocal created_clients
            created_clients += 1

        def set_cookies(self, _cookies: dict[str, str]) -> None:
            return None

        async def search_tweet(self, *_args: Any, **_kwargs: Any) -> list[Any]:
            raise Exception("Couldn't get KEY_BYTE indices")

    fake_twikit = types.SimpleNamespace(Client=FakeTwikitClient)
    monkeypatch.setitem(sys.modules, "twikit", fake_twikit)

    scraper = TwitterScraper(twikit_credentials={"auth_token": "auth", "ct0": "csrf"})

    first = scraper._search_tweets_via_twikit(query="quoted_tweet_id:root", max_pages=1, delay=0)
    second = scraper._search_tweets_via_twikit(query="conversation_id:root", max_pages=1, delay=0)

    assert first == []
    assert second == []
    assert scraper._twikit_search_unavailable is True
    assert scraper._last_twikit_search_error == "twikit_client_transaction_unavailable"
    assert created_clients == 1


def test_from_query_fallback_tries_playwright_before_syndication(monkeypatch) -> None:
    scraper = TwitterScraper(cookies={"auth_token": "auth", "ct0": "csrf"}, twikit_credentials={"auth": "cookie"})
    fallback_order: list[str] = []

    monkeypatch.setattr(scraper, "_ensure_auth", lambda: None)
    monkeypatch.setattr("trr_backend.socials.twitter.scraper.time.sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        scraper,
        "_fetch_search",
        lambda *_args, **_kwargs: _search_timeline_payload([]),
    )
    monkeypatch.setattr(
        scraper,
        "_scrape_via_twikit",
        lambda _config: fallback_order.append("twikit") or [],
    )
    monkeypatch.setattr(
        scraper,
        "_fetch_search_via_playwright",
        lambda **_kwargs: fallback_order.append("playwright") or [_tweet("playwright", reply_to=None)],
    )
    monkeypatch.setattr(
        scraper,
        "_scrape_syndication",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("syndication should be last resort")),
    )

    tweets = scraper.scrape(
        TwitterScrapeConfig(
            query="from:thetraitorsus",
            date_start=datetime(2026, 1, 1, tzinfo=UTC),
            date_end=datetime(2026, 2, 1, tzinfo=UTC),
            delay_seconds=0,
            max_pages=1,
        )
    )

    assert [tweet.tweet_id for tweet in tweets] == ["playwright"]
    assert fallback_order == ["twikit", "playwright"]
    assert scraper.last_retrieval_meta["retrieval_mode"] == "playwright"
    assert scraper.last_retrieval_meta["fallback_attempts"] == ["twikit", "playwright"]
