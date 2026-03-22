"""Tests for TwitterScraper fast_mode and adaptive rate-limiting behaviour."""
from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from trr_backend.socials.twitter.scraper import TwitterScrapeConfig, TwitterScraper


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
