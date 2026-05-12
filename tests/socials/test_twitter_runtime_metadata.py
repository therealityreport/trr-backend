from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from trr_backend.socials.twitter.diagnostics import safe_runtime_metadata
from trr_backend.socials.twitter.scraper import Tweet, TwitterScrapeConfig, TwitterScraper


def _make_scraper() -> TwitterScraper:
    with patch.object(TwitterScraper, "_create_session", return_value=MagicMock()):
        return TwitterScraper(cookies={})


def _twitter_config() -> TwitterScrapeConfig:
    return TwitterScrapeConfig(
        query="from:bravotv",
        date_start=datetime(2025, 8, 1, tzinfo=UTC),
        date_end=datetime(2025, 8, 31, tzinfo=UTC),
        delay_seconds=0,
        max_pages=1,
    )


def _make_tweet(tweet_id: str) -> Tweet:
    return Tweet(
        tweet_id=tweet_id,
        date_time="2025-08-14 00:00:00",
        created_at=int(datetime(2025, 8, 14, tzinfo=UTC).timestamp()),
        text="Bravo tweet",
        hashtags=[],
        mentions=[],
        likes=1,
        retweets=0,
        replies=0,
        quotes=0,
        views=10,
        url=f"https://x.com/bravotv/status/{tweet_id}",
        username="bravotv",
        display_name="Bravo TV",
        user_verified=True,
        is_reply=False,
        is_retweet=False,
        is_quote=False,
    )


def test_scrape_runtime_metadata_tracks_final_fallback_chain(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = _make_scraper()
    monkeypatch.setattr(scraper, "_ensure_auth", lambda: None)

    def _fake_fetch_search(*_args, **_kwargs):
        scraper._request_count = 1  # noqa: SLF001
        return None

    def _fake_twikit(_config: TwitterScrapeConfig):
        scraper._request_count = 3  # noqa: SLF001
        return [_make_tweet("tweet-fallback")]

    monkeypatch.setattr(scraper, "_fetch_search", _fake_fetch_search)
    monkeypatch.setattr(scraper, "_scrape_via_twikit", _fake_twikit)
    monkeypatch.setattr(scraper, "_fetch_search_via_playwright", lambda **_kwargs: [])

    scraper._twikit_credentials = {"auth_token": "a", "ct0": "b"}  # noqa: SLF001
    tweets = scraper.scrape(_twitter_config())

    assert len(tweets) == 1
    assert scraper.runtime_metadata["request_count"] == 3
    assert scraper.runtime_metadata["fallback_chain"] == ["graphql", "twikit"]
    assert scraper.runtime_metadata["transport"] == "twikit"
    assert scraper.runtime_metadata["complete"] is True


def test_playwright_partial_fallback_keeps_incomplete_runtime_state(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = _make_scraper()
    monkeypatch.setattr(scraper, "_ensure_auth", lambda: None)
    monkeypatch.setattr(scraper, "_fetch_search", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(scraper, "_scrape_syndication", lambda *_args, **_kwargs: [])
    scraper._twikit_credentials = None  # noqa: SLF001

    def _fake_playwright(**_kwargs):
        scraper._last_playwright_search_meta = {  # noqa: SLF001
            "page_budget": 5,
            "payloads_captured": 6,
            "scrolls_performed": 12,
            "stop_reason": "playwright_payload_budget_reached",
        }
        return [_make_tweet("tweet-playwright")]

    monkeypatch.setattr(scraper, "_fetch_search_via_playwright", _fake_playwright)

    tweets = scraper.scrape(_twitter_config())

    assert len(tweets) == 1
    assert scraper.last_retrieval_meta["retrieval_mode"] == "playwright"
    assert scraper.last_retrieval_meta["stop_reason"] == "playwright_payload_budget_reached"
    assert scraper.last_retrieval_meta["complete"] is False
    assert scraper.last_retrieval_meta["playwright_payloads_captured"] == 6
    assert scraper.runtime_metadata["transport"] == "playwright"
    assert scraper.runtime_metadata["complete"] is False


def test_safe_runtime_metadata_exposes_platform_owned_shape() -> None:
    scraper = _make_scraper()
    scraper._runtime_state.request_count = 2  # noqa: SLF001
    scraper._runtime_state.transport = "graphql"  # noqa: SLF001
    scraper._runtime_state.fallback_chain = ["graphql"]  # noqa: SLF001
    scraper._runtime_state.stop_reason = "no_cursor"  # noqa: SLF001
    scraper._runtime_state.retryable = False  # noqa: SLF001
    scraper._runtime_state.complete = True  # noqa: SLF001

    assert safe_runtime_metadata(scraper) == {
        "request_count": 2,
        "transport": "graphql",
        "fallback_chain": ["graphql"],
        "stop_reason": "no_cursor",
        "retryable": False,
        "complete": True,
    }
