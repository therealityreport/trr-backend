"""Tests for TwitterScraper fast_mode, rate limiting, and backfill diagnostics."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from trr_backend.socials.twitter.scraper import (
    Tweet,
    TwitterScrapeConfig,
    TwitterScraper,
    classify_twitter_search_complete,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _make_scraper() -> TwitterScraper:
    """Return a minimal TwitterScraper with no real HTTP session."""
    with patch.object(TwitterScraper, "_create_session", return_value=MagicMock()):
        return TwitterScraper(cookies={})


# ---------------------------------------------------------------------------
# TwitterScrapeConfig.__post_init__ — fast_mode delay override
# ---------------------------------------------------------------------------


class TestTwitterScrapeConfigFastMode:
    """Tests that __post_init__ handles the fast_mode delay override correctly."""

    def test_fast_mode_overrides_default_delay(self):
        """When fast_mode=True and delay_seconds is at its default (2.0),
        __post_init__ must reduce delay_seconds to 0.5."""
        config = TwitterScrapeConfig(
            query="RHOSLC",
            date_start=datetime(2024, 1, 1),
            date_end=datetime(2024, 1, 2),
            fast_mode=True,
            # delay_seconds intentionally NOT passed → default 2.0
        )
        assert config.delay_seconds == 0.5

    def test_fast_mode_does_not_override_explicit_delay(self):
        """When fast_mode=True but delay_seconds is explicitly set to a
        non-default value, __post_init__ must NOT override it."""
        config = TwitterScrapeConfig(
            query="RHOSLC",
            date_start=datetime(2024, 1, 1),
            date_end=datetime(2024, 1, 2),
            fast_mode=True,
            delay_seconds=1.0,  # explicit, non-default
        )
        assert config.delay_seconds == 1.0

    def test_no_fast_mode_leaves_delay_unchanged(self):
        """When fast_mode=False the default delay is left as 2.0."""
        config = TwitterScrapeConfig(
            query="RHOSLC",
            date_start=datetime(2024, 1, 1),
            date_end=datetime(2024, 1, 2),
        )
        assert config.delay_seconds == 2.0
        assert config.window_start_day() == "2024-01-01"
        assert config.window_end_day_inclusive() == "2024-01-02"
        assert config.window_end_day_exclusive() == "2024-01-03"


# ---------------------------------------------------------------------------
# TwitterScraper._track_response_status — counters and timestamp
# ---------------------------------------------------------------------------


class TestTrackResponseStatus:
    """Tests for the _track_response_status adaptive rate-limiting helper."""

    def test_non_429_increments_consecutive_success(self):
        """A 200-range response must increment _consecutive_success by 1."""
        scraper = _make_scraper()
        assert scraper._consecutive_success == 0

        scraper._track_response_status(200)
        assert scraper._consecutive_success == 1

        scraper._track_response_status(304)
        assert scraper._consecutive_success == 2

    def test_429_resets_consecutive_success_to_zero(self):
        """A 429 response must reset _consecutive_success to 0."""
        scraper = _make_scraper()
        scraper._consecutive_success = 15

        scraper._track_response_status(429)
        assert scraper._consecutive_success == 0

    def test_429_sets_last_429_at(self):
        """A 429 response must record a timestamp in _last_429_at."""
        import time

        scraper = _make_scraper()
        assert scraper._last_429_at is None

        before = time.monotonic()
        scraper._track_response_status(429)
        after = time.monotonic()

        assert scraper._last_429_at is not None
        assert before <= scraper._last_429_at <= after

    def test_non_429_does_not_set_last_429_at(self):
        """A successful response must not alter _last_429_at."""
        scraper = _make_scraper()
        assert scraper._last_429_at is None

        scraper._track_response_status(200)
        assert scraper._last_429_at is None

    def test_5xx_does_not_increment_consecutive_success(self):
        """A 5xx response (not 429) should not increment _consecutive_success
        because only 2xx–3xx responses count as successes."""
        scraper = _make_scraper()
        scraper._track_response_status(500)
        assert scraper._consecutive_success == 0

    def test_multiple_successes_after_429_resets_counter(self):
        """After a 429 resets the counter, subsequent successes should
        increment from zero again."""
        scraper = _make_scraper()
        for _ in range(5):
            scraper._track_response_status(200)
        assert scraper._consecutive_success == 5

        scraper._track_response_status(429)
        assert scraper._consecutive_success == 0

        scraper._track_response_status(200)
        assert scraper._consecutive_success == 1


@pytest.mark.parametrize(
    ("stop_reason", "retryable", "error_code", "expected"),
    [
        ("no_cursor", False, None, True),
        ("older_than_window_repeated", False, None, True),
        ("max_pages_reached", False, None, False),
        ("graphql_fetch_failed", False, None, False),
        ("no_cursor", True, "twitter_search_fallback_exhausted", False),
    ],
)
def test_classify_twitter_search_complete(stop_reason, retryable, error_code, expected):
    assert (
        classify_twitter_search_complete(
            stop_reason=stop_reason,
            retryable=retryable,
            error_code=error_code,
        )
        is expected
    )


def _make_tweet(tweet_id: str = "tweet-1") -> Tweet:
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


def test_scrape_marks_exhausted_fallback_chain_as_retryable(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = _make_scraper()
    monkeypatch.setattr(scraper, "_ensure_auth", lambda: None)
    monkeypatch.setattr(scraper, "_fetch_search", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(scraper, "_last_graphql_status_code", 500)

    def _fake_twikit(_config: TwitterScrapeConfig):
        scraper._last_twikit_search_error = "twikit_request_error"  # noqa: SLF001
        return []

    def _fake_playwright(**_kwargs):
        scraper._last_playwright_search_error = "playwright_error"  # noqa: SLF001
        return []

    monkeypatch.setattr(scraper, "_scrape_via_twikit", _fake_twikit)
    monkeypatch.setattr(scraper, "_scrape_syndication", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(scraper, "_fetch_search_via_playwright", _fake_playwright)
    scraper._twikit_credentials = {"auth_token": "a", "ct0": "b"}  # noqa: SLF001

    tweets = scraper.scrape(
        TwitterScrapeConfig(
            query="from:bravotv",
            date_start=datetime(2025, 8, 1, tzinfo=UTC),
            date_end=datetime(2025, 8, 31, tzinfo=UTC),
            delay_seconds=0,
            max_pages=1,
        )
    )

    assert tweets == []
    assert scraper.last_retrieval_meta["error_code"] == "twitter_search_fallback_exhausted"
    assert scraper.last_retrieval_meta["retryable"] is True
    assert scraper.last_retrieval_meta["twikit_failure_reason"] == "twikit_request_error"
    assert scraper.last_retrieval_meta["playwright_failure_reason"] == "playwright_error"


def test_scrape_emits_progress_for_successful_twikit_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = _make_scraper()
    progress_events: list[dict[str, object]] = []

    monkeypatch.setattr(scraper, "_ensure_auth", lambda: None)
    monkeypatch.setattr(scraper, "_fetch_search", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(scraper, "_last_graphql_status_code", 500)
    monkeypatch.setattr(scraper, "_fetch_search_via_playwright", lambda **_kwargs: [])
    monkeypatch.setattr(scraper, "_scrape_via_twikit", lambda _config: [_make_tweet("tweet-fallback")])
    scraper._twikit_credentials = {"auth_token": "a", "ct0": "b"}  # noqa: SLF001

    tweets = scraper.scrape(
        TwitterScrapeConfig(
            query="from:bravotv",
            date_start=datetime(2025, 8, 1, tzinfo=UTC),
            date_end=datetime(2025, 8, 31, tzinfo=UTC),
            delay_seconds=0,
            max_pages=1,
        ),
        progress_cb=lambda payload: progress_events.append(dict(payload)),
    )

    assert len(tweets) == 1
    assert scraper.last_retrieval_meta["error_code"] is None
    assert any(event.get("phase") == "scrape_twikit_fallback" for event in progress_events)
