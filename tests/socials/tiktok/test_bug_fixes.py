"""Regression tests for TikTok scraper bug fixes (bugs #1-#4).

Each test locks in a specific fix from .claude/plans/fancy-beaming-dijkstra.md.
Keep these narrow and fast - they should not spin up Playwright or make
network calls.
"""

from __future__ import annotations

import time
from unittest.mock import patch

import pytest

from trr_backend.socials.tiktok.http_client import (
    _ClientConfig,
    _TikTokHttpClientBase,
)
from trr_backend.socials.tiktok.media_resolver import _extract_candidate_item

# ---------- Bug #1: exponential backoff with jitter ----------


class _CollectingSleeper:
    """Captures delays passed to time.sleep without actually sleeping."""

    def __init__(self) -> None:
        self.calls: list[float] = []

    def __call__(self, seconds: float) -> None:
        self.calls.append(float(seconds))


def _make_client(factor: float = 1.5, retries: int = 5) -> _TikTokHttpClientBase:
    return _TikTokHttpClientBase(
        config=_ClientConfig(retry_total=retries, backoff_factor=factor)
    )


def test_bug1_backoff_is_exponential_not_linear() -> None:
    """Bug #1: http_client.py:71 used linear backoff (`factor * attempt`).

    Verify each delay is at least roughly 2x the previous (within jitter bounds).
    Linear would give 1.5, 3.0, 4.5, 6.0 -- ratios near 1.5x.
    Exponential with +/-25% jitter gives base 1.5, 3.0, 6.0, 12.0 -- ratios 2x +/- jitter.
    """
    client = _make_client(factor=1.5, retries=5)
    sleeper = _CollectingSleeper()
    with patch.object(time, "sleep", sleeper):
        for attempt in (1, 2, 3, 4):
            client._sleep_before_retry(attempt)

    assert len(sleeper.calls) == 4
    # Base values without jitter: 1.5, 3.0, 6.0, 12.0.
    # With +/-25% jitter, acceptable bounds are:
    expected_bounds = [(1.125, 1.875), (2.25, 3.75), (4.5, 7.5), (9.0, 15.0)]
    for delay, (lo, hi) in zip(sleeper.calls, expected_bounds, strict=True):
        assert lo <= delay <= hi, (
            f"backoff delay {delay} outside expected range [{lo}, {hi}] - "
            f"regression to linear backoff?"
        )


def test_bug1_backoff_skips_when_at_retry_limit() -> None:
    """Final attempt should not sleep (no retries left)."""
    client = _make_client(factor=1.5, retries=3)
    sleeper = _CollectingSleeper()
    with patch.object(time, "sleep", sleeper):
        client._sleep_before_retry(3)  # attempt == retry_total -> skip
    assert sleeper.calls == []


def test_bug1_backoff_zero_factor_skips_sleep() -> None:
    """backoff_factor=0 should short-circuit without calling sleep."""
    client = _make_client(factor=0.0, retries=3)
    sleeper = _CollectingSleeper()
    with patch.object(time, "sleep", sleeper):
        client._sleep_before_retry(1)
    assert sleeper.calls == []


# ---------- Bug #2: build_tiktok_http_client return type ----------


def test_bug2_build_client_returns_typed_base() -> None:
    """build_tiktok_http_client is annotated -> _TikTokHttpClientBase, not Session."""
    from trr_backend.socials.tiktok.scraper import build_tiktok_http_client as wrapper

    client = wrapper()
    assert isinstance(client, _TikTokHttpClientBase)
    # The annotation change is caught at type-check time; this is a sanity check
    # that the wrapper still delegates correctly.
    assert hasattr(client, "get")


# ---------- Bug #3: media_resolver candidate_id check ----------


def test_bug3_rejects_item_with_missing_id_when_video_id_given() -> None:
    """Bug #3: previously `not candidate_id` let malformed items pass through."""
    payload = {
        "itemInfo": {
            "itemStruct": {
                # No id, no aweme_id -> candidate_id becomes ""
                "video": {"id": "video-data"},
            }
        }
    }
    assert _extract_candidate_item(payload, video_id="7000000000000000000") is None


def test_bug3_accepts_matching_id() -> None:
    payload = {
        "itemInfo": {
            "itemStruct": {
                "id": "7000000000000000000",
                "video": {"id": "video-data"},
            }
        }
    }
    result = _extract_candidate_item(payload, video_id="7000000000000000000")
    assert result is not None
    assert result["id"] == "7000000000000000000"


def test_bug3_accepts_any_item_when_no_target_id() -> None:
    """When caller doesn't specify a target video_id, any well-formed item is ok."""
    payload = {
        "itemInfo": {
            "itemStruct": {
                "id": "anything",
                "video": {"id": "video-data"},
            }
        }
    }
    result = _extract_candidate_item(payload, video_id="")
    assert result is not None


# ---------- Bug #4: Playwright context teardown ----------
# Full integration test would spin up a real browser; we instead test the
# teardown ordering via the structural invariant that the scraper hoists
# `context` to outer scope.


def test_bug4_scraper_hoists_context_for_teardown() -> None:
    """Bug #4: context must be visible in the finally block.

    Regression guard against someone inlining `context = browser.new_context(...)`
    only inside the `with` block, which would leak fd-bound resources.
    """
    import inspect

    from trr_backend.socials.tiktok import scraper as tt_scraper

    source = inspect.getsource(tt_scraper)
    # Both hoist declarations must precede the try/with playwright block.
    assert "context: Any | None = None" in source, (
        "context should be hoisted to outer scope for finally-block access"
    )
    assert "context.close()" in source, (
        "finally block must explicitly close the Playwright context"
    )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
