"""Retry/backoff branch coverage for Instagram posts Scrapling fetcher.

Mirrors tests/socials/test_instagram_comments_scrapling_retry.py adapted
for POST form-data transport.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import httpx

_FIXTURE_DIR = Path(__file__).parents[3] / "fixtures" / "instagram" / "scrapling"


def _fixture_json(name: str) -> dict:
    return json.loads((_FIXTURE_DIR / name).read_text(encoding="utf-8"))


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
    """Build a fetcher with scrapling mocked out."""
    with patch("scrapling.fetchers.StealthyFetcher", MagicMock()):
        from trr_backend.socials.instagram.posts_scrapling.fetcher import (
            InstagramPostsScraplingFetcher,
        )

        fetcher = InstagramPostsScraplingFetcher(
            cookies=[],
            raw_cookies={"sessionid": "x", "csrftoken": "y", "ds_user_id": "1"},
            browser_account_id="t",
        )
        asyncio.run(fetcher._rebuild_http_client())
        return fetcher


_SLEEP_TARGET = "trr_backend.socials.instagram.posts_scrapling.fetcher.asyncio.sleep"
_URL = "https://www.instagram.com/graphql/query"


class _TrackingClient:
    def __init__(self) -> None:
        self.closed = False

    async def aclose(self) -> None:
        self.closed = True


def test_rebuild_http_client_closes_previous_client(monkeypatch) -> None:
    fetcher = _make_fetcher()
    old = _TrackingClient()
    fetcher._http_client = cast(Any, old)
    monkeypatch.setattr(
        "trr_backend.socials.instagram.posts_scrapling.fetcher.httpx.AsyncClient",
        lambda **_kwargs: _TrackingClient(),
    )

    asyncio.run(fetcher._rebuild_http_client())

    assert old.closed is True


# 1 — 429 then 200 succeeds
def test_fetch_retries_on_429_then_succeeds() -> None:
    fetcher = _make_fetcher()
    resp_429 = _make_httpx_response(status_code=429, text="slow")
    resp_ok = _make_httpx_response(status_code=200, json_data={"status": "ok", "data": {}})
    fetcher._fetch_graphql = AsyncMock(side_effect=[resp_429, resp_ok])

    with patch(_SLEEP_TARGET, AsyncMock()):
        result = asyncio.run(fetcher._fetch_json_response(_URL, referer="r", data={}, headers={}))

    assert result["failed"] is False
    assert fetcher._fetch_graphql.await_count == 2


# 2 — 429 exhausts retries with retryable=True
def test_fetch_gives_up_after_max_retries_with_retryable_true() -> None:
    from trr_backend.socials.instagram.posts_scrapling.fetcher import (
        InstagramPostsScraplingFetcher,
    )

    fetcher = _make_fetcher()
    resp_429 = _make_httpx_response(status_code=429, text="still slow")
    fetcher._fetch_graphql = AsyncMock(return_value=resp_429)

    with patch(_SLEEP_TARGET, AsyncMock()):
        result = asyncio.run(fetcher._fetch_json_response(_URL, referer="r", data={}, headers={}))

    assert result["failed"] is True
    assert result["retryable"] is True
    assert result["reason"] == "http_429"
    assert fetcher._fetch_graphql.await_count == InstagramPostsScraplingFetcher._MAX_TRANSIENT_RETRIES + 1


# 3 — 401 is auth_failed and not retryable
def test_fetch_401_not_retryable_and_auth_failed() -> None:
    fetcher = _make_fetcher()
    resp = _make_httpx_response(status_code=401, text="unauth")
    fetcher._fetch_graphql = AsyncMock(return_value=resp)

    with patch(_SLEEP_TARGET, AsyncMock()):
        result = asyncio.run(fetcher._fetch_json_response(_URL, referer="r", data={}, headers={}))

    assert result["failed"] is True
    assert result["retryable"] is False
    assert result["auth_failed"] is True


# 4 — 400 (non-auth text) fails non-retryably with auth_failed=False
def test_fetch_400_validation_non_retryable() -> None:
    fetcher = _make_fetcher()
    resp = _make_httpx_response(status_code=400, text="bad request")
    fetcher._fetch_graphql = AsyncMock(return_value=resp)

    with patch(_SLEEP_TARGET, AsyncMock()):
        result = asyncio.run(fetcher._fetch_json_response(_URL, referer="r", data={}, headers={}))

    assert result["failed"] is True
    assert result["retryable"] is False
    assert result["auth_failed"] is False


# 5 — Retry-After header respected
def test_fetch_respects_retry_after_header() -> None:
    fetcher = _make_fetcher()
    sleep_calls: list[float] = []

    async def capture_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    resp_429 = _make_httpx_response(status_code=429, text="slow", headers={"retry-after": "7"})
    resp_ok = _make_httpx_response(status_code=200, json_data={"status": "ok"})
    fetcher._fetch_graphql = AsyncMock(side_effect=[resp_429, resp_ok])

    with patch(_SLEEP_TARGET, capture_sleep):
        asyncio.run(fetcher._fetch_json_response(_URL, referer="r", data={}, headers={}))

    assert 7.0 in sleep_calls


# 6 — httpx.TimeoutException is retryable, then succeeds
def test_timeout_exception_is_retryable_then_succeeds() -> None:
    fetcher = _make_fetcher()
    resp_ok = _make_httpx_response(status_code=200, json_data={"status": "ok"})
    fetcher._fetch_graphql = AsyncMock(side_effect=[httpx.TimeoutException("slow"), resp_ok])

    with patch(_SLEEP_TARGET, AsyncMock()):
        result = asyncio.run(fetcher._fetch_json_response(_URL, referer="r", data={}, headers={}))

    assert result["failed"] is False
    assert fetcher._fetch_graphql.await_count == 2


# 7 — timeout exhausts retries with retryable=True
def test_timeout_exhausts_retries() -> None:
    from trr_backend.socials.instagram.posts_scrapling.fetcher import (
        InstagramPostsScraplingFetcher,
    )

    fetcher = _make_fetcher()
    fetcher._fetch_graphql = AsyncMock(side_effect=httpx.TimeoutException("dead"))

    with patch(_SLEEP_TARGET, AsyncMock()):
        result = asyncio.run(fetcher._fetch_json_response(_URL, referer="r", data={}, headers={}))

    assert result["failed"] is True
    assert result["retryable"] is True
    assert result["reason"] == "transport_timeout"
    assert fetcher.runtime_metadata["retry_reason_counts"]["transport_timeout"] == (
        InstagramPostsScraplingFetcher._MAX_TRANSIENT_RETRIES + 1
    )
    assert fetcher._fetch_graphql.await_count == InstagramPostsScraplingFetcher._MAX_TRANSIENT_RETRIES + 1


def test_transport_error_exhausts_retries() -> None:
    from trr_backend.socials.instagram.posts_scrapling.fetcher import (
        InstagramPostsScraplingFetcher,
    )

    fetcher = _make_fetcher()
    fetcher._fetch_graphql = AsyncMock(side_effect=httpx.ConnectError("connection reset"))

    with patch(_SLEEP_TARGET, AsyncMock()):
        result = asyncio.run(fetcher._fetch_json_response(_URL, referer="r", data={}, headers={}))

    assert result["failed"] is True
    assert result["retryable"] is True
    assert result["reason"] == "transport_error"
    assert fetcher.runtime_metadata["retry_reason_counts"]["transport_error"] == (
        InstagramPostsScraplingFetcher._MAX_TRANSIENT_RETRIES + 1
    )
    assert fetcher._fetch_graphql.await_count == InstagramPostsScraplingFetcher._MAX_TRANSIENT_RETRIES + 1


def test_rebuild_http_client_closes_existing_async_client() -> None:
    fetcher = _make_fetcher()
    stale_client = AsyncMock()
    fetcher._http_client = stale_client

    asyncio.run(fetcher._rebuild_http_client())

    stale_client.aclose.assert_awaited_once()
    assert fetcher._http_client is not stale_client


# 8 — Redirect to /accounts/login → auth_failed, non-retryable
def test_redirect_to_login_is_auth_failed() -> None:
    fetcher = _make_fetcher()
    resp = _make_httpx_response(
        status_code=302,
        text="",
        headers={"location": "https://www.instagram.com/accounts/login/?next=/"},
    )
    fetcher._fetch_graphql = AsyncMock(return_value=resp)

    with patch(_SLEEP_TARGET, AsyncMock()):
        result = asyncio.run(fetcher._fetch_json_response(_URL, referer="r", data={}, headers={}))

    assert result["failed"] is True
    assert result["auth_failed"] is True
    assert result["reason"] == "redirect_to_login"
    assert result["retryable"] is False


# 9 — Redirect to /challenge/ → auth_failed, non-retryable
def test_redirect_to_checkpoint_is_auth_failed() -> None:
    fetcher = _make_fetcher()
    resp = _make_httpx_response(
        status_code=302,
        text="",
        headers={"location": "https://www.instagram.com/challenge/"},
    )
    fetcher._fetch_graphql = AsyncMock(return_value=resp)

    with patch(_SLEEP_TARGET, AsyncMock()):
        result = asyncio.run(fetcher._fetch_json_response(_URL, referer="r", data={}, headers={}))

    assert result["failed"] is True
    assert result["auth_failed"] is True
    assert result["reason"] == "redirect_to_checkpoint"


def test_redirect_to_homepage_rewarms_profile_and_retries_once() -> None:
    fetcher = _make_fetcher()
    fetcher._fetch_graphql = AsyncMock(
        side_effect=[
            _make_httpx_response(status_code=302, headers={"location": "https://www.instagram.com/"}),
            _make_httpx_response(status_code=200, json_data=_fixture_json("posts_graphql_success.json")),
        ]
    )
    fetcher._fetch_page = AsyncMock(return_value=_make_httpx_response(status_code=200, text="<html></html>"))
    fetcher._rebuild_http_client = AsyncMock()

    result = asyncio.run(
        fetcher._fetch_json_response(
            _URL,
            referer="https://www.instagram.com/bravotv/",
            data={},
            headers={},
        )
    )

    assert fetcher._fetch_graphql.await_count == 2
    fetcher._fetch_page.assert_awaited_once_with(
        "https://www.instagram.com/bravotv/",
        referer="https://www.instagram.com/bravotv/",
    )
    assert result["failed"] is False
    assert fetcher.runtime_metadata["retry_reason_counts"]["homepage_redirect_recovery"] == 1


def test_redirect_to_homepage_marks_auth_failed_after_recovery_retry() -> None:
    fetcher = _make_fetcher()
    fetcher._fetch_graphql = AsyncMock(
        side_effect=[
            _make_httpx_response(status_code=302, headers={"location": "https://www.instagram.com/"}),
            _make_httpx_response(status_code=302, headers={"location": "https://www.instagram.com/"}),
        ]
    )
    fetcher._fetch_page = AsyncMock(return_value=_make_httpx_response(status_code=200, text="<html></html>"))
    fetcher._rebuild_http_client = AsyncMock()

    result = asyncio.run(
        fetcher._fetch_json_response(
            _URL,
            referer="https://www.instagram.com/bravotv/",
            data={},
            headers={},
        )
    )

    assert result["failed"] is True
    assert result["auth_failed"] is True
    assert result["reason"] == "redirect_to_homepage"


# 10 — HTML body marks challenge
def test_html_body_is_challenge() -> None:
    fetcher = _make_fetcher()
    resp = _make_httpx_response(
        status_code=200,
        text="<html><body>challenge</body></html>",
    )
    fetcher._fetch_graphql = AsyncMock(return_value=resp)

    with patch(_SLEEP_TARGET, AsyncMock()):
        result = asyncio.run(fetcher._fetch_json_response(_URL, referer="r", data={}, headers={}))

    assert result["failed"] is True
    assert result["reason"] == "html_challenge_or_auth_required"


# 11 — API-level status != 'ok' with login marker is auth_failed
def test_api_status_with_login_marker_is_auth_failed() -> None:
    fetcher = _make_fetcher()
    json_payload = {"status": "fail", "message": "login_required"}
    resp = _make_httpx_response(
        status_code=200,
        text='{"status":"fail","message":"login_required"}',
        json_data=json_payload,
    )
    fetcher._fetch_graphql = AsyncMock(return_value=resp)

    with patch(_SLEEP_TARGET, AsyncMock()):
        result = asyncio.run(fetcher._fetch_json_response(_URL, referer="r", data={}, headers={}))

    assert result["failed"] is True
    assert result["auth_failed"] is True
    # reason is the status_value string ("fail"), not "api_status_fail"
    assert result["reason"] == "fail"
