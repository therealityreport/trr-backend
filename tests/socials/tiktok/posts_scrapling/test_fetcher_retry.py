"""Retry/backoff branch coverage for TikTok posts Scrapling fetcher.

Mirrors the Instagram posts retry test suite adapted for TikTok's GET
transport. TikTok-specific: integer `statusCode` field (0 == ok),
challenge detection via body patterns.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

import httpx
import pytest


def _make_httpx_response(
    *,
    status_code: int,
    text: str = "",
    headers: dict | None = None,
    json_data=None,
) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.text = text
    resp.headers = headers or {}
    if json_data is not None:
        resp.json = MagicMock(return_value=json_data)
    else:
        resp.json = MagicMock(side_effect=ValueError("no json"))
    return resp


def _make_fetcher():
    """Build a fetcher with scrapling mocked out, ready for _fetch_api_get patching."""
    with patch("scrapling.fetchers.StealthyFetcher", MagicMock()):
        from trr_backend.socials.tiktok.posts_scrapling.fetcher import TikTokPostsScraplingFetcher

        fetcher = TikTokPostsScraplingFetcher(cookies=[], raw_cookies={"sessionid": "x"})
    return fetcher


_SLEEP_TARGET = "trr_backend.socials.tiktok.posts_scrapling.fetcher.asyncio.sleep"


@pytest.fixture
def _no_sleep(monkeypatch):
    """Patch the module-local asyncio.sleep so retries don't actually wait."""

    async def _noop(*a, **k):
        return None

    monkeypatch.setattr(_SLEEP_TARGET, _noop)


# 1 — 429 then 200 succeeds
def test_tiktok_retries_on_429_then_succeeds(_no_sleep) -> None:
    fetcher = _make_fetcher()
    responses = iter(
        [
            _make_httpx_response(status_code=429, text="slow"),
            _make_httpx_response(status_code=200, json_data={"statusCode": 0, "itemList": []}),
        ]
    )

    async def fake_get(*a, **kw):
        return next(responses)

    fetcher._fetch_api_get = fake_get

    result = asyncio.run(fetcher._fetch_api_json("u", params={}, referer="r"))
    assert result["failed"] is False


# 2 — 429 exhausts retries with retryable=True
def test_tiktok_429_exhausts(_no_sleep) -> None:
    fetcher = _make_fetcher()
    resp = _make_httpx_response(status_code=429, text="")

    async def fake_get(*a, **kw):
        return resp

    fetcher._fetch_api_get = fake_get

    result = asyncio.run(fetcher._fetch_api_json("u", params={}, referer="r"))
    assert result["failed"] is True
    assert result["retryable"] is True


# 3 — 401 non-retryable, auth_failed
def test_tiktok_401_non_retryable_auth_failed(_no_sleep) -> None:
    fetcher = _make_fetcher()
    resp = _make_httpx_response(status_code=401, text="")

    async def fake_get(*a, **kw):
        return resp

    fetcher._fetch_api_get = fake_get

    result = asyncio.run(fetcher._fetch_api_json("u", params={}, referer="r"))
    assert result["failed"] is True
    assert result["auth_failed"] is True
    assert result["retryable"] is False


# 4 — 400 non-auth non-retryable
def test_tiktok_400_non_auth_non_retryable(_no_sleep) -> None:
    fetcher = _make_fetcher()
    resp = _make_httpx_response(status_code=400, text="bad")

    async def fake_get(*a, **kw):
        return resp

    fetcher._fetch_api_get = fake_get

    result = asyncio.run(fetcher._fetch_api_json("u", params={}, referer="r"))
    assert result["failed"] is True
    assert result["auth_failed"] is False
    assert result["retryable"] is False


# 5 — Retry-After respected
def test_tiktok_respects_retry_after() -> None:
    fetcher = _make_fetcher()
    sleep_calls: list[float] = []

    async def capture_sleep(s: float) -> None:
        sleep_calls.append(s)

    responses = iter(
        [
            _make_httpx_response(status_code=429, text="", headers={"retry-after": "5"}),
            _make_httpx_response(status_code=200, json_data={"statusCode": 0}),
        ]
    )

    async def fake_get(*a, **kw):
        return next(responses)

    fetcher._fetch_api_get = fake_get

    with patch(_SLEEP_TARGET, capture_sleep):
        asyncio.run(fetcher._fetch_api_json("u", params={}, referer="r"))

    assert 5.0 in sleep_calls


# 6 — TimeoutException retries then succeeds
def test_tiktok_timeout_retries_then_succeeds(_no_sleep) -> None:
    fetcher = _make_fetcher()
    call_count = {"n": 0}

    async def fake_get(*a, **kw):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise httpx.TimeoutException("x")
        return _make_httpx_response(status_code=200, json_data={"statusCode": 0})

    fetcher._fetch_api_get = fake_get

    result = asyncio.run(fetcher._fetch_api_json("u", params={}, referer="r"))
    assert result["failed"] is False
    assert call_count["n"] == 2


# 7 — Timeout exhausts
def test_tiktok_timeout_exhausts(_no_sleep) -> None:
    fetcher = _make_fetcher()

    async def fake_get(*a, **kw):
        raise httpx.TimeoutException("x")

    fetcher._fetch_api_get = fake_get

    result = asyncio.run(fetcher._fetch_api_json("u", params={}, referer="r"))
    assert result["failed"] is True
    assert result["retryable"] is True
    assert result["reason"] == "transport_timeout"


# 8 — Redirect to login is auth_failed
def test_tiktok_redirect_to_login_is_auth_failed(_no_sleep) -> None:
    fetcher = _make_fetcher()
    resp = _make_httpx_response(
        status_code=302,
        headers={"location": "https://www.tiktok.com/login/"},
    )

    async def fake_get(*a, **kw):
        return resp

    fetcher._fetch_api_get = fake_get

    result = asyncio.run(fetcher._fetch_api_json("u", params={}, referer="r"))
    assert result["failed"] is True
    assert result["auth_failed"] is True
    assert result["reason"] == "redirect_to_login"


# 9 — Challenge HTML is blocked
def test_tiktok_challenge_html_is_blocked(_no_sleep) -> None:
    fetcher = _make_fetcher()
    resp = _make_httpx_response(
        status_code=200,
        text="<html><body>Please verify captcha</body></html>",
    )

    async def fake_get(*a, **kw):
        return resp

    fetcher._fetch_api_get = fake_get

    result = asyncio.run(fetcher._fetch_api_json("u", params={}, referer="r"))
    assert result["failed"] is True
    assert result["auth_failed"] is True
    assert result["reason"] == "challenge_or_blocked"


# 10 — TikTok statusCode nonzero with auth marker
def test_tiktok_statuscode_nonzero_with_auth_marker(_no_sleep) -> None:
    """TikTok uses integer statusCode; 0 == ok. Nonzero with auth-related
    msg should mark auth_failed. Reason will be tiktok_status_{code} —
    we assert only on failed/auth_failed, not the exact reason string.
    """
    fetcher = _make_fetcher()
    resp = _make_httpx_response(
        status_code=200,
        text='{"statusCode":10002,"statusMsg":"login required"}',
        json_data={"statusCode": 10002, "statusMsg": "login required"},
    )

    async def fake_get(*a, **kw):
        return resp

    fetcher._fetch_api_get = fake_get

    result = asyncio.run(fetcher._fetch_api_json("u", params={}, referer="r"))
    assert result["failed"] is True
    assert result["auth_failed"] is True


# 11 — TikTok statusCode zero is success
def test_tiktok_statuscode_zero_is_success(_no_sleep) -> None:
    fetcher = _make_fetcher()
    resp = _make_httpx_response(
        status_code=200,
        json_data={"statusCode": 0, "itemList": [{"id": "1"}]},
    )

    async def fake_get(*a, **kw):
        return resp

    fetcher._fetch_api_get = fake_get

    result = asyncio.run(fetcher._fetch_api_json("u", params={}, referer="r"))
    assert result["failed"] is False
    assert result["payload"]["itemList"][0]["id"] == "1"
