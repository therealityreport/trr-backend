"""Fetcher-level guardrails for budgeted public proxy fan-out.

Covers the run-scoped budget kill-switch (proxy use trips off once estimated
spend reaches the run budget) and the CDN/static "media-direct" tripwire (a
media host fetched while proxied trips the proxy off and counts the leak). No
daily/cross-run cap exists.
"""

from __future__ import annotations

import os
from types import SimpleNamespace
from unittest.mock import patch

from trr_backend.socials.instagram.comments_scrapling.fetcher import InstagramCommentsScraplingFetcher
from trr_backend.socials.instagram.comments_scrapling.proxy import CommentsProxyConfig

_GB = 1_073_741_824


def _proxy_config() -> CommentsProxyConfig:
    return CommentsProxyConfig(
        browser_proxy={"server": "http://gate.decodo.com:7000", "username": "u", "password": "p"},
        api_proxy_url="http://u:p@gate.decodo.com:7000",
        proxy_rotator=None,
        fingerprint="gate.decodo.com:7000:decodo:abc123",
        session_mode="sticky",
    )


def _proxied_fetcher(env: dict[str, str] | None = None) -> InstagramCommentsScraplingFetcher:
    # Budget config is resolved at __init__, so set env before constructing.
    with patch.dict(os.environ, {**os.environ, **(env or {})}, clear=True):
        return InstagramCommentsScraplingFetcher(
            cookies=[],
            raw_cookies={},
            browser_account_id="bravotv",
            proxy_config=_proxy_config(),
        )


def _resp(url: str, nbytes: int) -> SimpleNamespace:
    return SimpleNamespace(url=url, content=b"x" * nbytes, headers={})


def test_run_budget_kill_switch_trips_and_is_sticky():
    f = _proxied_fetcher(
        {
            "SOCIAL_INSTAGRAM_COMMENTS_PROXY_DOLLARS_PER_GB": "1.0",
            "SOCIAL_INSTAGRAM_COMMENTS_PROXY_RUN_BUDGET_USD": "1.0",
        }
    )
    # Under budget -> proxy allowed.
    assert f._public_proxy_use_allowed() is True
    # 2 GiB at $1/GB == $2 >= $1 run budget -> trips.
    f._bytes_total = 2 * _GB
    assert f._public_proxy_use_allowed() is False
    assert f._proxy_budget_exhausted is True
    # Sticky: stays direct even if byte counter is reset.
    f._bytes_total = 0
    assert f._public_proxy_use_allowed() is False


def test_unpriced_budget_never_trips_on_volume():
    # No $/GB configured -> run cap disabled; large volume still allowed.
    f = _proxied_fetcher({"SOCIAL_INSTAGRAM_COMMENTS_PROXY_DOLLARS_PER_GB": "0"})
    f._bytes_total = 100 * _GB
    assert f._public_proxy_use_allowed() is True
    assert f._proxy_budget_exhausted is False


def test_cdn_leak_tripwire_disables_proxy_for_run():
    f = _proxied_fetcher()
    assert f._public_proxy_use_allowed() is True
    f._record_response_bytes(_resp("https://scontent.cdninstagram.com/v/t51/x.jpg", 5000))
    assert f._proxy_cdn_bytes_leak >= 5000
    assert f._proxy_cdn_bytes_leak_by_host.get("scontent.cdninstagram.com", 0) >= 5000
    assert f._proxy_budget_exhausted is True
    assert f._public_proxy_use_allowed() is False


def test_www_instagram_response_does_not_trip_tripwire():
    f = _proxied_fetcher()
    f._record_response_bytes(_resp("https://www.instagram.com/api/v1/media/1/comments/", 5000))
    assert f._proxy_cdn_bytes_leak == 0
    assert f._proxy_budget_exhausted is False
    assert f._public_proxy_use_allowed() is True


def test_no_proxy_fetcher_never_uses_proxy_or_counts_leak():
    with patch.dict(os.environ, dict(os.environ), clear=True):
        f = InstagramCommentsScraplingFetcher(
            cookies=[], raw_cookies={}, browser_account_id="bravotv", proxy_config=None
        )
    # No proxy configured -> gate is False and no CDN leak is attributed.
    assert f._public_proxy_use_allowed() is False
    f._record_response_bytes(_resp("https://scontent.cdninstagram.com/x.jpg", 5000))
    assert f._proxy_cdn_bytes_leak == 0
    assert f._proxy_budget_exhausted is False
