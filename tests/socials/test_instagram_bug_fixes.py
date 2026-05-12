"""Regression tests for Instagram scraper bug fixes (bugs #5-#10).

Each test locks in a specific fix from .claude/plans/fancy-beaming-dijkstra.md.
No network; all IO is mocked or structural.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from trr_backend.socials.instagram.apify_scraper import normalize_apify_post
from trr_backend.socials.instagram.identity_pool import (
    InstagramIdentityPool,
    InstagramScraperIdentity,
)
from trr_backend.socials.instagram.request_client import (
    InstagramRequestClient,
    InstagramRequestFailure,
)

# ---------- Bug #5: 401 is not retryable ----------


def _fake_response(status_code: int, *, headers: dict[str, str] | None = None, text: str = "") -> SimpleNamespace:
    return SimpleNamespace(
        status_code=status_code,
        headers=headers or {"content-type": "text/html"},
        text=text,
        content=text.encode("utf-8"),
    )


def test_bug5_401_raises_non_retryable() -> None:
    client = InstagramRequestClient(session=MagicMock())
    with pytest.raises(InstagramRequestFailure) as exc_info:
        client._classify_response(_fake_response(401, text="not authorized"))
    assert exc_info.value.retryable is False
    assert str(exc_info.value) == "unauthorized" or "unauthorized" in str(exc_info.value)


# ---------- Bug #6: 403 is not retryable ----------


def test_bug6_403_raises_non_retryable() -> None:
    client = InstagramRequestClient(session=MagicMock())
    with pytest.raises(InstagramRequestFailure) as exc_info:
        client._classify_response(_fake_response(403, text="blocked"))
    assert exc_info.value.retryable is False


def test_bug56_429_remains_retryable() -> None:
    """Sanity: we only changed 401/403 - 429 must still be retryable."""
    client = InstagramRequestClient(session=MagicMock())
    with pytest.raises(InstagramRequestFailure) as exc_info:
        client._classify_response(_fake_response(429, text="rate limited"))
    assert exc_info.value.retryable is True


# ---------- Bug #7: validation_skipped returns True ----------


def test_bug7_skip_validation_returns_passing_tuple() -> None:
    from trr_backend.socials.instagram.auth_resolver import _validate_cookies_via_graphql

    # Structurally valid cookies so we hit the require_validation=False branch.
    cookies = {
        "sessionid": "fake-sessionid-value",
        "ds_user_id": "1234567890",
        "csrftoken": "fake-csrf",
    }
    validated, reason, category, stale_ok = _validate_cookies_via_graphql(
        cookies, session_account_id=None, require_validation=False
    )
    assert validated is True, (
        "Bug #7: when validation is skipped, the cookies should be treated as passing (True), not as invalid (False)"
    )
    assert reason == "validation_skipped"
    assert category == "validation_skipped"
    assert stale_ok is False


# ---------- Bug #8: identity_pool does not mutate in filter predicate ----------


def _make_identity(
    session_id: str, *, age: float = 0.0, retired: bool = False, max_age: float = 100.0
) -> InstagramScraperIdentity:
    return InstagramScraperIdentity(
        session_id=session_id,
        generation=1,
        proxy_url=None,
        proxy_label=None,
        cookies={},
        created_at=0.0,
        retired=retired,
    )


def test_bug8_is_expired_predicate_is_pure() -> None:
    """Calling the predicate alone must not mutate identity state."""
    pool = InstagramIdentityPool(
        proxy_urls=[],
        base_cookies={"sessionid": "x"},
        max_age_seconds=60,
        max_requests=100,
        max_generations=2,
        probe_timeout_seconds=1.0,
        probe_func=lambda *_args, **_kwargs: True,
        clock=lambda: 1000.0,  # will be overridden per-test
    )
    identity = _make_identity("id-1")
    # Clock returns 500.0 -> age = 500 > max_age=60 -> expired
    pool._clock = lambda: 500.0
    assert pool._is_expired_for_age(identity) is True
    # Pure predicate: no state change.
    assert identity.retired is False
    assert identity.retire_reason is None


def test_bug8_acquire_explicitly_retires_aged_identities() -> None:
    """acquire() must itself mark aged identities retired after splitting."""
    pool = InstagramIdentityPool(
        proxy_urls=[],
        base_cookies={"sessionid": "x"},
        max_age_seconds=60,
        max_requests=100,
        max_generations=2,
        probe_timeout_seconds=1.0,
        probe_func=lambda *_args, **_kwargs: True,
        clock=lambda: 0.0,
    )
    # Seed a single identity via the pool's own flow (gen 1 -> 1 identity).
    assert len(pool._identities) == 1
    first = pool._identities[0]
    # Advance clock so it's expired.
    pool._clock = lambda: 999.0
    # acquire() will seed a new generation because all current are retired.
    new_id = pool.acquire()
    assert first.retired is True
    assert first.retire_reason == "max_age_exceeded"
    assert new_id.session_id != first.session_id


# ---------- Bug #9: response.cookies merged into session ----------


def test_bug9_fetch_profile_page_context_merges_response_cookies() -> None:
    """When the warming GET returns Set-Cookie, merge into session.cookies."""
    from trr_backend.socials.instagram.scraper import InstagramScraper

    scraper = InstagramScraper(cookies={"sessionid": "stale"})
    # Build a fake response with a cookies attribute that iterates like a RequestsCookieJar.
    fake_cookies = MagicMock()
    fake_cookies.__bool__ = MagicMock(return_value=True)
    fake_cookies.items = MagicMock(return_value=[("csrftoken", "new-csrf-value")])
    fake_response = SimpleNamespace(text="<html></html>", cookies=fake_cookies)

    # Monkey-patch the request path to return our fake response and bypass
    # html parsing nuance.
    scraper._get = MagicMock(return_value=fake_response)
    scraper._extract_profile_page_context = MagicMock(return_value={})

    scraper._warm_profile_request_context("somebody")

    # The merged csrftoken should now live on the session cookie jar.
    assert scraper.session.cookies.get("csrftoken") == "new-csrf-value"


# ---------- Bug #10: apify timestamp parse logs on failure ----------


def test_bug10_apify_timestamp_failure_emits_debug_log(caplog: pytest.LogCaptureFixture) -> None:
    raw = {
        "type": "Image",
        "timestamp": "not-a-real-iso-timestamp",
        "caption": "hello",
    }
    with caplog.at_level(logging.DEBUG, logger="trr_backend.socials.instagram.apify_scraper"):
        post = normalize_apify_post(raw)
    # normalize_apify_post returns a dict; verify posted_at entry is None.
    assert post.get("posted_at") is None
    # At least one log record mentions the offending value.
    assert any("not-a-real-iso-timestamp" in rec.getMessage() for rec in caplog.records), (
        "Bug #10: apify timestamp parse failures must be logged for drift diagnosis"
    )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
