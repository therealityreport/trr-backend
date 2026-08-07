from __future__ import annotations

import asyncio
import json
import threading
from datetime import UTC, datetime
from typing import Any, cast

import pytest
import requests

from trr_backend.socials.instagram.identity_pool import InstagramIdentityPool, InstagramScraperIdentity
from trr_backend.socials.instagram.request_client import InstagramRequestClient, InstagramRequestFailure
from trr_backend.socials.instagram.scraper import InstagramScraper, ScrapeConfig


class _FakeResponse:
    def __init__(self, *, json_payload: dict[str, Any] | None = None, text: str = "", status_code: int = 200) -> None:
        self._json_payload = json_payload or {}
        self.text = text
        self.status_code = status_code
        self.cookies: dict[str, str] = {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"http_{self.status_code}")

    def json(self) -> dict[str, Any]:
        return self._json_payload


def _graphql_posts_payload(
    nodes: list[dict[str, Any]],
    *,
    has_next: bool = False,
    end_cursor: str | None = None,
) -> dict[str, Any]:
    return {
        "data": {
            "xdt_api__v1__feed__user_timeline_graphql_connection": {
                "edges": [{"node": node} for node in nodes],
                "page_info": {"has_next_page": has_next, "end_cursor": end_cursor},
            }
        }
    }


def test_fetch_posts_graphql_warms_profile_page_and_uses_public_web_headers(monkeypatch) -> None:
    scraper = InstagramScraper(cookies={})
    captured: dict[str, Any] = {}

    def _fake_get(url: str, **kwargs: Any) -> _FakeResponse:
        captured["warm_url"] = url
        captured["warm_headers"] = kwargs.get("headers") or {}
        scraper.session.cookies.set("csrftoken", "csrf-token")
        scraper.session.cookies.set("mid", "mid-token")
        return _FakeResponse(
            text=(
                '"LSD",[],{"token":"lsd-token"},323],["ServerNonce",[],{"ServerNonce":"nonce"},141],'
                'bloks_version\\":\\"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\\"'
            )
        )

    def _fake_post(url: str, **kwargs: Any) -> _FakeResponse:
        captured["post_url"] = url
        captured["post_headers"] = kwargs.get("headers") or {}
        captured["post_data"] = dict(kwargs.get("data") or {})
        captured["post_cookies"] = kwargs.get("cookies") or {}
        return _FakeResponse(
            json_payload={
                "data": {
                    "xdt_api__v1__feed__user_timeline_graphql_connection": {
                        "edges": [{"node": {"code": "ABC123", "pk": "1", "id": "1_1"}}],
                        "page_info": {"has_next_page": False, "end_cursor": None},
                    }
                }
            }
        )

    monkeypatch.setattr(scraper, "_get", _fake_get)
    monkeypatch.setattr(scraper, "_post", _fake_post)

    payload = scraper.fetch_posts_graphql("bravotv", None, 0.0)

    assert payload is not None
    assert scraper.last_retrieval_meta["transport"] == "requests_enriched"
    assert scraper.last_retrieval_meta["retrieval_transport"] == "requests_enriched"
    assert captured["warm_url"] == "https://www.instagram.com/bravotv/"
    assert captured["post_url"] == scraper.GRAPHQL_URL
    assert captured["post_headers"]["x-fb-friendly-name"] == "PolarisProfilePostsQuery"
    assert captured["post_headers"]["x-root-field-name"] == "xdt_api__v1__feed__user_timeline_graphql_connection"
    assert captured["post_headers"]["x-fb-lsd"] == "lsd-token"
    assert captured["post_headers"]["x-csrftoken"] == "csrf-token"
    assert captured["post_headers"]["x-asbd-id"] == scraper.WEB_X_ASBD_ID
    assert (
        captured["post_headers"]["x-bloks-version-id"]
        == "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    assert captured["post_data"]["doc_id"] == "26859136577041380"
    assert captured["post_data"]["av"] == "0"
    assert captured["post_data"]["__user"] == "0"
    assert captured["post_data"]["lsd"] == "lsd-token"
    assert captured["post_data"]["jazoest"] == "2913"
    variables = json.loads(captured["post_data"]["variables"])
    assert variables["__relay_internal__pv__PolarisImmersiveFeedChainingEnabledrelayprovider"] is True
    assert variables["__relay_internal__pv__PolarisAIGMAccountLabelEnabledrelayprovider"] is False


def test_fetch_posts_graphql_retries_cursor_pages_with_fresh_profile_context(monkeypatch) -> None:
    scraper = InstagramScraper(cookies={})
    warm_force_flags: list[bool] = []
    post_headers: list[dict[str, Any]] = []

    def _fake_warm(username: str, *, timeout: Any = None, force: bool = False) -> dict[str, str]:
        del timeout
        assert username == "bravotv"
        warm_force_flags.append(force)
        scraper.session.cookies.set("csrftoken", f"csrf-{len(warm_force_flags)}")
        return {
            "lsd": f"lsd-{len(warm_force_flags)}",
            "bloks_version": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        }

    def _fake_post(url: str, **kwargs: Any) -> _FakeResponse:
        assert url == scraper.GRAPHQL_URL
        post_headers.append(dict(kwargs.get("headers") or {}))
        if len(post_headers) == 1:
            raise requests.exceptions.HTTPError("http_401")
        return _FakeResponse(
            json_payload={
                "data": {
                    "xdt_api__v1__feed__user_timeline_graphql_connection": {
                        "edges": [{"node": {"code": "ABC123", "pk": "1", "id": "1_1"}}],
                        "page_info": {"has_next_page": True, "end_cursor": "cursor-2"},
                    }
                }
            }
        )

    monkeypatch.setattr(scraper, "_warm_profile_request_context", _fake_warm)
    monkeypatch.setattr(scraper, "_post", _fake_post)
    monkeypatch.setattr(scraper, "_playwright_graphql_fallback_enabled", lambda: False)

    payload = scraper.fetch_posts_graphql("bravotv", "cursor-1", 0.0)

    assert payload is not None
    assert warm_force_flags == [True]
    assert len(post_headers) == 2
    assert post_headers[0]["x-fb-lsd"] == "lsd-1"
    assert post_headers[1]["x-fb-lsd"] == "lsd-1"
    assert post_headers[0]["x-csrftoken"] == "csrf-1"
    assert post_headers[1]["x-csrftoken"] == "csrf-1"


def test_fetch_posts_graphql_retries_initial_pages_with_fresh_session(monkeypatch) -> None:
    scraper = InstagramScraper(cookies={})
    warm_force_flags: list[bool] = []
    post_attempts = 0
    reset_calls = 0
    first_attempt_failures_remaining = len(scraper._profile_posts_doc_ids())

    def _fake_warm(username: str, *, timeout: Any = None, force: bool = False) -> dict[str, str]:
        del timeout
        assert username == "bravotv"
        warm_force_flags.append(force)
        scraper.session.cookies.set("csrftoken", f"csrf-{len(warm_force_flags)}")
        return {
            "lsd": f"lsd-{len(warm_force_flags)}",
            "bloks_version": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        }

    def _fake_post(url: str, **kwargs: Any) -> _FakeResponse:
        nonlocal post_attempts, first_attempt_failures_remaining
        assert url == scraper.GRAPHQL_URL
        post_attempts += 1
        if first_attempt_failures_remaining > 0:
            first_attempt_failures_remaining -= 1
            response = requests.Response()
            response.status_code = 403
            raise requests.exceptions.HTTPError("http_403", response=response)
        return _FakeResponse(
            json_payload={
                "data": {
                    "xdt_api__v1__feed__user_timeline_graphql_connection": {
                        "edges": [{"node": {"code": "ABC123", "pk": "1", "id": "1_1"}}],
                        "page_info": {"has_next_page": True, "end_cursor": "cursor-2"},
                    }
                }
            }
        )

    def _fake_reset() -> None:
        nonlocal reset_calls
        reset_calls += 1

    monkeypatch.setattr(scraper, "_warm_profile_request_context", _fake_warm)
    monkeypatch.setattr(scraper, "_post", _fake_post)
    monkeypatch.setattr(scraper, "_reset_request_session", _fake_reset)
    monkeypatch.setattr(scraper, "_resolve_graphql_retry_backoff_seconds", lambda cursor, attempt_index: 0.0)

    payload = scraper.fetch_posts_graphql("bravotv", None, 0.0)

    assert payload is not None
    assert post_attempts == len(scraper._profile_posts_doc_ids()) + 1
    assert warm_force_flags == [False, True]
    assert reset_calls == 1


def test_fetch_posts_graphql_records_cursor_unauthorized_failure_metadata(monkeypatch) -> None:
    scraper = InstagramScraper(cookies={})

    def _fake_warm(username: str, *, timeout: Any = None, force: bool = False) -> dict[str, str]:
        del timeout, force
        assert username == "bravotv"
        scraper.session.cookies.set("csrftoken", "csrf-1")
        return {
            "lsd": "lsd-1",
            "bloks_version": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        }

    def _fake_post(_url: str, **_kwargs: Any) -> _FakeResponse:
        response = requests.Response()
        response.status_code = 401
        raise requests.exceptions.HTTPError("http_401", response=response)

    monkeypatch.setattr(scraper, "_warm_profile_request_context", _fake_warm)
    monkeypatch.setattr(scraper, "_post", _fake_post)
    monkeypatch.setattr(scraper, "_playwright_graphql_fallback_enabled", lambda: False)

    payload = scraper.fetch_posts_graphql("bravotv", "cursor-1", 0.0)

    assert payload is None
    assert scraper.last_retrieval_meta["error_code"] == "instagram_graphql_cursor_unauthorized"
    assert scraper.last_retrieval_meta["error_class"] == "HTTPError"
    assert scraper.last_retrieval_meta["error_status_code"] == 401
    assert scraper.last_retrieval_meta["retryable"] is False
    assert scraper.last_retrieval_meta["graphql_cursor"] == "cursor-1"
    assert scraper.last_retrieval_meta["transport"] == "requests_enriched"


def test_fetch_posts_graphql_clears_cursor_context_after_request_client_auth_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scraper = InstagramScraper(cookies={"sessionid": "seed", "csrftoken": "csrf", "ds_user_id": "1"})
    reset_calls = 0
    popped_usernames: list[str] = []

    monkeypatch.setattr(
        scraper,
        "_warm_profile_request_context",
        lambda *args, **kwargs: {
            "lsd": "lsd-token",
            "bloks_version": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        },
    )
    monkeypatch.setattr(scraper, "_resolve_graphql_cursor_retry_attempts", lambda cursor: 1)
    monkeypatch.setattr(scraper, "_playwright_graphql_fallback_enabled", lambda: False)

    def _fake_reset() -> None:
        nonlocal reset_calls
        reset_calls += 1

    monkeypatch.setattr(scraper, "_reset_request_session", _fake_reset)
    monkeypatch.setattr(
        scraper,
        "_pop_profile_page_context_cache_entry",
        lambda username: popped_usernames.append(username),
    )

    class _Client:
        def post_form_json(self, *args, **kwargs):
            raise InstagramRequestFailure("unauthorized", status_code=401, retryable=False)

    scraper._request_client = cast(InstagramRequestClient, _Client())

    payload = scraper.fetch_posts_graphql(
        "bravotv",
        "cursor-1",
        0.0,
        allow_browser_fallback=False,
        allow_recovery=False,
    )

    assert payload is None
    assert scraper.last_retrieval_meta["error_code"] == "instagram_graphql_cursor_unauthorized"
    assert reset_calls == 1
    assert popped_usernames == ["bravotv"]


def test_fetch_posts_graphql_records_checkpoint_required_failure_metadata(monkeypatch) -> None:
    scraper = InstagramScraper(cookies={"sessionid": "sessionid", "ds_user_id": "123"})

    def _fake_warm(username: str, *, timeout: Any = None, force: bool = False) -> dict[str, str]:
        del timeout, force
        assert username == "bravotv"
        scraper.session.cookies.set("csrftoken", "csrf-1")
        return {
            "lsd": "lsd-1",
            "bloks_version": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        }

    def _fake_post(_url: str, **_kwargs: Any) -> _FakeResponse:
        response = requests.Response()
        response.status_code = 400
        response._content = b'{"message":"checkpoint_required","status":"fail"}'
        raise requests.exceptions.HTTPError("http_400", response=response)

    monkeypatch.setattr(scraper, "_warm_profile_request_context", _fake_warm)
    monkeypatch.setattr(scraper, "_post", _fake_post)
    monkeypatch.setattr(scraper, "_playwright_graphql_fallback_enabled", lambda: False)

    payload = scraper.fetch_posts_graphql("bravotv", None, 0.0)

    assert payload is None
    assert scraper.last_retrieval_meta["error_code"] == "instagram_graphql_checkpoint_required"
    assert scraper.last_retrieval_meta["error_status_code"] == 400
    assert scraper.last_retrieval_meta["error_message"] == "checkpoint_required"


def test_fetch_posts_graphql_handles_empty_request_client_payload(monkeypatch) -> None:
    scraper = InstagramScraper(cookies={"sessionid": "sessionid", "ds_user_id": "123"})

    monkeypatch.setattr(
        scraper,
        "_warm_profile_request_context",
        lambda *_args, **_kwargs: {
            "lsd": "lsd-1",
            "bloks_version": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        },
    )
    monkeypatch.setattr(scraper._request_client, "post_form_json", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(scraper, "_playwright_graphql_fallback_enabled", lambda: False)
    monkeypatch.setattr(scraper, "_resolve_graphql_cursor_retry_attempts", lambda _cursor: 1)
    monkeypatch.setattr(scraper, "_resolve_graphql_retry_backoff_seconds", lambda *_args: 0.0)

    payload = scraper.fetch_posts_graphql("bravotv", None, 0.0, allow_recovery=False)

    assert payload is None
    assert scraper.last_retrieval_meta["error_code"] == "instagram_graphql_empty_response"
    assert scraper.last_retrieval_meta["error_class"] == "NoneType"
    assert scraper.last_retrieval_meta["retryable"] is True


def test_fetch_posts_graphql_uses_browser_fallback_after_requests_fail(monkeypatch) -> None:
    scraper = InstagramScraper(cookies={})

    def _fake_warm(username: str, *, timeout: Any = None, force: bool = False) -> dict[str, str]:
        del timeout, force
        assert username == "bravotv"
        scraper.session.cookies.set("csrftoken", "csrf-1")
        return {
            "lsd": "lsd-1",
            "bloks_version": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        }

    def _fake_post(_url: str, **_kwargs: Any) -> _FakeResponse:
        response = requests.Response()
        response.status_code = 403
        raise requests.exceptions.HTTPError("http_403", response=response)

    browser_payload = {
        "data": {
            "xdt_api__v1__feed__user_timeline_graphql_connection": {
                "edges": [{"node": {"code": "ABC123", "pk": "1", "id": "1_1"}}],
                "page_info": {"has_next_page": True, "end_cursor": "cursor-2"},
            }
        }
    }

    monkeypatch.setattr(scraper, "_warm_profile_request_context", _fake_warm)
    monkeypatch.setattr(scraper, "_post", _fake_post)
    monkeypatch.setattr(scraper, "_playwright_graphql_fallback_enabled", lambda: True)

    def _fake_browser_fetch(username: str, cursor=None, request_timeout=None):
        del username, cursor, request_timeout
        scraper.last_retrieval_meta["transport"] = "playwright"
        scraper.last_retrieval_meta["retrieval_transport"] = "playwright"
        return browser_payload

    monkeypatch.setattr(
        scraper,
        "_fetch_posts_graphql_with_browser",
        _fake_browser_fetch,
    )

    payload = scraper.fetch_posts_graphql("bravotv", None, 0.0)

    assert payload == browser_payload
    assert scraper.last_retrieval_meta["transport"] == "playwright"
    assert scraper.last_retrieval_meta["retrieval_transport"] == "playwright"


def test_fetch_posts_graphql_skips_browser_fallback_when_disabled(monkeypatch) -> None:
    scraper = InstagramScraper(cookies={})

    def _fake_warm(username: str, *, timeout: Any = None, force: bool = False) -> dict[str, str]:
        del timeout, force
        assert username == "bravotv"
        scraper.session.cookies.set("csrftoken", "csrf-1")
        return {
            "lsd": "lsd-1",
            "bloks_version": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        }

    def _fake_post(_url: str, **_kwargs: Any) -> _FakeResponse:
        response = requests.Response()
        response.status_code = 403
        raise requests.exceptions.HTTPError("http_403", response=response)

    monkeypatch.setattr(scraper, "_warm_profile_request_context", _fake_warm)
    monkeypatch.setattr(scraper, "_post", _fake_post)
    monkeypatch.setattr(scraper, "_playwright_graphql_fallback_enabled", lambda: True)
    monkeypatch.setattr(
        scraper,
        "_fetch_posts_graphql_with_browser",
        lambda *_args, **_kwargs: pytest.fail("browser fallback should stay disabled"),
    )

    payload = scraper.fetch_posts_graphql("bravotv", None, 0.0, allow_browser_fallback=False)

    assert payload is None
    assert scraper.last_retrieval_meta["transport"] == "requests_enriched"


@pytest.mark.parametrize(
    ("cookies", "expected_reason"),
    [
        ({"sessionid": "session"}, "missing_csrftoken_and_ds_user_id"),
        ({"sessionid": "session", "csrftoken": "csrf"}, "missing_ds_user_id"),
        ({"sessionid": "session", "ds_user_id": "123"}, "missing_csrftoken"),
        ({"sessionid": "session", "csrftoken": "csrf", "ds_user_id": "123"}, None),
    ],
)
def test_validate_cookies_requires_complete_instagram_cookie_triplet(
    cookies: dict[str, str],
    expected_reason: str | None,
) -> None:
    scraper = InstagramScraper(cookies=cookies)

    result = scraper._validate_cookies()

    assert result["valid"] is (expected_reason is None)
    assert result["reason"] == expected_reason


@pytest.mark.parametrize(
    ("error_code", "cursor", "expect_browser_fallback"),
    [
        ("instagram_graphql_checkpoint_required", None, True),
        ("instagram_graphql_cursor_rate_limited", "cursor-1", False),
        ("instagram_graphql_cursor_unauthorized", "cursor-1", True),
        ("instagram_graphql_cursor_forbidden", "cursor-1", True),
    ],
)
def test_fetch_posts_graphql_applies_browser_fallback_policy(
    monkeypatch: pytest.MonkeyPatch,
    error_code: str,
    cursor: str | None,
    expect_browser_fallback: bool,
) -> None:
    scraper = InstagramScraper(cookies={})
    scraper._fallback_chain = ["graphql"]
    browser_called = False

    monkeypatch.setattr(
        scraper,
        "_warm_profile_request_context",
        lambda *_args, **_kwargs: {
            "lsd": "lsd-1",
            "bloks_version": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        },
    )

    def _fake_post(_url: str, **_kwargs: Any) -> _FakeResponse:
        raise requests.exceptions.RequestException("network failure")

    def _fake_browser_fetch(*_args: Any, **_kwargs: Any) -> dict[str, Any] | None:
        nonlocal browser_called
        browser_called = True
        return {"unexpected": True}

    monkeypatch.setattr(scraper, "_post", _fake_post)
    monkeypatch.setattr(scraper, "_playwright_graphql_fallback_enabled", lambda: True)
    monkeypatch.setattr(scraper, "_fetch_posts_graphql_with_browser", _fake_browser_fetch)
    monkeypatch.setattr(scraper, "_graphql_request_error_details", lambda **_kwargs: {"error_code": error_code})
    monkeypatch.setattr(scraper, "_resolve_graphql_retry_backoff_seconds", lambda *_args: 0.0)
    monkeypatch.setattr(scraper, "_try_auto_refresh_cookies", lambda: {"refreshed": False})
    monkeypatch.setattr(scraper, "_maybe_rotate_identity_after_failure", lambda **_kwargs: None)
    monkeypatch.setattr(scraper, "_is_local_environment", lambda: False)

    payload = scraper.fetch_posts_graphql("bravotv", cursor, 0.0)

    assert browser_called is expect_browser_fallback
    if expect_browser_fallback:
        assert payload == {"unexpected": True}
        assert scraper.last_retrieval_meta["fallback_chain"] == ["graphql", "browser_intercept"]
        assert scraper.last_retrieval_meta["identity_mode"] == "legacy"
    else:
        assert payload is None
        assert scraper.last_retrieval_meta["fallback_chain"] == ["graphql"]
        assert scraper.last_retrieval_meta["identity_mode"] == "legacy"


def test_fetch_posts_graphql_browser_fallback_success_annotates_chain_without_duplicates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scraper = InstagramScraper(cookies={})
    scraper._fallback_chain = ["graphql"]

    monkeypatch.setattr(
        scraper,
        "_warm_profile_request_context",
        lambda *_args, **_kwargs: {
            "lsd": "lsd-1",
            "bloks_version": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        },
    )

    def _fake_post(_url: str, **_kwargs: Any) -> _FakeResponse:
        response = requests.Response()
        response.status_code = 403
        raise requests.exceptions.HTTPError("http_403", response=response)

    def _fake_browser_fetch(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        scraper.last_retrieval_meta["transport"] = "playwright"
        scraper.last_retrieval_meta["retrieval_transport"] = "playwright"
        return {"ok": True}

    monkeypatch.setattr(scraper, "_post", _fake_post)
    monkeypatch.setattr(scraper, "_playwright_graphql_fallback_enabled", lambda: True)
    monkeypatch.setattr(scraper, "_fetch_posts_graphql_with_browser", _fake_browser_fetch)
    monkeypatch.setattr(scraper, "_try_auto_refresh_cookies", lambda: {"refreshed": False})
    monkeypatch.setattr(scraper, "_maybe_rotate_identity_after_failure", lambda **_kwargs: None)
    monkeypatch.setattr(scraper, "_is_local_environment", lambda: False)

    payload = scraper.fetch_posts_graphql("bravotv", None, 0.0)

    assert payload == {"ok": True}
    assert scraper.last_retrieval_meta["fallback_chain"] == ["graphql", "browser_intercept"]
    assert scraper.last_retrieval_meta["identity_mode"] == "legacy"


def test_resolve_graphql_retry_backoff_seconds_uses_bounded_jitter(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = InstagramScraper(cookies={})
    captured: dict[str, float] = {}

    def _fake_uniform(low: float, high: float) -> float:
        captured["low"] = low
        captured["high"] = high
        return 1.23

    monkeypatch.setattr("trr_backend.socials.instagram.scraper.random.uniform", _fake_uniform)

    backoff = scraper._resolve_graphql_retry_backoff_seconds("cursor-1", 1)  # noqa: SLF001

    assert backoff == 1.23
    assert captured == {"low": 1.5, "high": 3.0}


def test_fetch_posts_graphql_uses_explicit_page_size_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """page_size kwarg should override the default PROFILE_POSTS_PAGE_SIZE in GraphQL variables."""
    import json as _json

    scraper = InstagramScraper(cookies={"sessionid": "x", "csrftoken": "y", "ds_user_id": "1"})
    captured: dict[str, Any] = {}

    monkeypatch.setattr(scraper, "_rate_limit", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        scraper,
        "_warm_profile_request_context",
        lambda *_args, **_kwargs: {
            "lsd": "lsd-1",
            "bloks_version": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        },
    )
    monkeypatch.setattr(
        scraper,
        "_request_cookies",
        lambda: {"sessionid": "x", "csrftoken": "y", "ds_user_id": "1"},
    )
    monkeypatch.setattr(scraper, "_graphql_form_runtime_fields", lambda **_kwargs: {})
    monkeypatch.setattr(scraper, "_profile_posts_doc_ids", lambda: ["doc"])

    def _fake_post(_url: str, *, data: dict[str, Any] | None = None, **_kwargs: Any) -> _FakeResponse:
        assert data is not None
        captured["variables"] = _json.loads(data["variables"])
        return _FakeResponse(
            json_payload={
                "data": {
                    "xdt_api__v1__feed__user_timeline_graphql_connection": {
                        "edges": [{"node": {"id": "1"}}],
                        "page_info": {"has_next_page": False, "end_cursor": None},
                    }
                }
            },
        )

    monkeypatch.setattr(scraper, "_post", _fake_post)

    scraper.fetch_posts_graphql("bravotv", None, 0.15, page_size=50)

    assert captured["variables"]["first"] == 50
    assert captured["variables"]["data"]["count"] == 50


def test_fetch_posts_graphql_uses_default_page_size_without_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without page_size kwarg, should use the default PROFILE_POSTS_PAGE_SIZE (33)."""
    import json as _json

    scraper = InstagramScraper(cookies={"sessionid": "x", "csrftoken": "y", "ds_user_id": "1"})
    captured: dict[str, Any] = {}

    monkeypatch.setattr(scraper, "_rate_limit", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        scraper,
        "_warm_profile_request_context",
        lambda *_args, **_kwargs: {
            "lsd": "lsd-1",
            "bloks_version": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        },
    )
    monkeypatch.setattr(
        scraper,
        "_request_cookies",
        lambda: {"sessionid": "x", "csrftoken": "y", "ds_user_id": "1"},
    )
    monkeypatch.setattr(scraper, "_graphql_form_runtime_fields", lambda **_kwargs: {})
    monkeypatch.setattr(scraper, "_profile_posts_doc_ids", lambda: ["doc"])

    def _fake_post(_url: str, *, data: dict[str, Any] | None = None, **_kwargs: Any) -> _FakeResponse:
        assert data is not None
        captured["variables"] = _json.loads(data["variables"])
        return _FakeResponse(
            json_payload={
                "data": {
                    "xdt_api__v1__feed__user_timeline_graphql_connection": {
                        "edges": [{"node": {"id": "1"}}],
                        "page_info": {"has_next_page": False, "end_cursor": None},
                    }
                }
            },
        )

    monkeypatch.setattr(scraper, "_post", _fake_post)

    scraper.fetch_posts_graphql("bravotv", None, 0.15)

    assert captured["variables"]["first"] == scraper.PROFILE_POSTS_PAGE_SIZE
    assert captured["variables"]["data"]["count"] == scraper.PROFILE_POSTS_PAGE_SIZE


def test_request_cookies_updates_active_identity_cookie_jar(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = InstagramScraper(cookies={"sessionid": "seed"})
    scraper._identity_pool_enabled = True
    scraper._active_identity = cast(
        InstagramScraperIdentity, type("Identity", (), {"cookies": {"sessionid": "seed"}})()
    )
    scraper.session.cookies.set("csrftoken", "csrf-token")

    merged = scraper._request_cookies()

    assert merged["sessionid"] == "seed"
    assert merged["csrftoken"] == "csrf-token"
    assert scraper._active_identity.cookies["csrftoken"] == "csrf-token"


@pytest.mark.parametrize(
    ("status_code", "headers", "body"),
    [
        (429, {"Content-Type": "application/json"}, b'{"message":"rate_limited"}'),
        (200, {"Content-Type": "text/html"}, b'<html><a href="/accounts/login/">Log in</a></html>'),
    ],
)
def test_identity_pool_does_not_record_success_for_unsuccessful_response(
    monkeypatch: pytest.MonkeyPatch,
    status_code: int,
    headers: dict[str, str],
    body: bytes,
) -> None:
    scraper = InstagramScraper(cookies={"sessionid": "seed"})
    scraper._identity_pool_enabled = True
    scraper._active_identity = cast(
        InstagramScraperIdentity,
        type("Identity", (), {"session_id": "identity-a", "cookies": {"sessionid": "seed"}})(),
    )
    success_calls: list[str] = []
    request_calls: list[str] = []

    class _Pool:
        def record_success(self, session_id: str) -> None:
            success_calls.append(session_id)

        def record_request(self, session_id: str) -> None:
            request_calls.append(session_id)

    response = requests.Response()
    response.status_code = status_code
    response.headers.update(headers)
    response._content = body

    scraper._identity_pool = cast(InstagramIdentityPool, _Pool())
    monkeypatch.setattr(scraper.session, "post", lambda *_args, **_kwargs: response)

    assert scraper._post("https://www.instagram.com/graphql/query/") is response
    assert success_calls == []
    assert request_calls == ["identity-a"]


def test_identity_pool_records_success_for_successful_json_response(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = InstagramScraper(cookies={"sessionid": "seed"})
    scraper._identity_pool_enabled = True
    scraper._active_identity = cast(
        InstagramScraperIdentity,
        type("Identity", (), {"session_id": "identity-a", "cookies": {"sessionid": "seed"}})(),
    )
    success_calls: list[str] = []

    class _Pool:
        def record_success(self, session_id: str) -> None:
            success_calls.append(session_id)

        def record_request(self, _session_id: str) -> None:
            return None

    response = requests.Response()
    response.status_code = 200
    response.headers["Content-Type"] = "application/json"
    response._content = b'{"ok": true}'

    scraper._identity_pool = cast(InstagramIdentityPool, _Pool())
    monkeypatch.setattr(scraper.session, "post", lambda *_args, **_kwargs: response)

    assert scraper._post("https://www.instagram.com/graphql/query/") is response
    assert success_calls == ["identity-a"]


def test_reset_request_session_preserves_active_identity_cookies(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = InstagramScraper(cookies={})
    scraper._identity_pool_enabled = True
    scraper._active_identity = cast(
        InstagramScraperIdentity,
        type("Identity", (), {"cookies": {"sessionid": "seed", "csrftoken": "csrf"}})(),
    )
    scraper.session.cookies.set("mid", "mid-token")

    scraper._reset_request_session()

    assert scraper.session.cookies.get("sessionid") == "seed"
    assert scraper.session.cookies.get("csrftoken") == "csrf"
    assert scraper.session.cookies.get("mid") == "mid-token"


def test_fetch_posts_graphql_checkpoint_required_uses_same_identity_refresh(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = InstagramScraper(cookies={"sessionid": "seed", "csrftoken": "csrf", "ds_user_id": "1"})
    scraper._identity_pool_enabled = True
    scraper._active_identity = cast(
        InstagramScraperIdentity,
        type("Identity", (), {"session_id": "identity-a", "cookies": dict(scraper.cookies)})(),
    )
    refresh_attempts: list[str] = []

    monkeypatch.setattr(
        scraper,
        "_warm_profile_request_context",
        lambda *_args, **_kwargs: {
            "lsd": "lsd-1",
            "bloks_version": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        },
    )

    def _fake_post(_url: str, **_kwargs: Any) -> _FakeResponse:
        response = requests.Response()
        response.status_code = 400
        response._content = b'{"message":"checkpoint_required","status":"fail"}'
        raise requests.exceptions.HTTPError("http_400", response=response)

    monkeypatch.setattr(scraper, "_post", _fake_post)
    monkeypatch.setattr(scraper, "_playwright_graphql_fallback_enabled", lambda: False)
    monkeypatch.setattr(scraper, "_is_local_environment", lambda: False)
    monkeypatch.setattr(
        scraper,
        "_try_auto_refresh_cookies",
        lambda: refresh_attempts.append(cast(InstagramScraperIdentity, scraper._active_identity).session_id)
        or {"refreshed": False, "reason": "noop"},
    )

    payload = scraper.fetch_posts_graphql("bravotv", None, 0.0)

    assert payload is None
    assert refresh_attempts == ["identity-a"]
    assert scraper._active_identity.session_id == "identity-a"


def test_fetch_posts_graphql_retires_identity_after_repeated_hard_403(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = InstagramScraper(cookies={"sessionid": "seed", "csrftoken": "csrf", "ds_user_id": "1"})
    scraper._identity_pool_enabled = True
    retire_calls: list[tuple[str, str]] = []
    acquire_calls = 0

    class _Pool:
        proxy_probe_failures: list[str] = []

        def merge_cookies(self, session_id: str, cookies: dict[str, str]) -> None:
            del session_id
            assert scraper._active_identity is not None
            scraper._active_identity.cookies = dict(cookies)

        def retire(self, session_id: str, *, reason: str, block_reason: str | None = None) -> None:
            retire_calls.append((session_id, reason if block_reason is None else block_reason))

        def acquire(self):
            nonlocal acquire_calls
            acquire_calls += 1
            return type("Identity", (), {"session_id": f"identity-{acquire_calls}", "cookies": dict(scraper.cookies)})()

    scraper._identity_pool = cast(InstagramIdentityPool, _Pool())
    scraper._active_identity = scraper._identity_pool.acquire()

    monkeypatch.setattr(
        scraper,
        "_warm_profile_request_context",
        lambda *_args, **_kwargs: {
            "lsd": "lsd-1",
            "bloks_version": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        },
    )
    monkeypatch.setattr(scraper, "_try_auto_refresh_cookies", lambda: {"refreshed": False, "reason": "noop"})
    monkeypatch.setattr(scraper, "_playwright_graphql_fallback_enabled", lambda: False)
    monkeypatch.setattr(scraper, "_is_local_environment", lambda: False)

    def _fake_post(_url: str, **_kwargs: Any) -> _FakeResponse:
        response = requests.Response()
        response.status_code = 403
        raise requests.exceptions.HTTPError("http_403", response=response)

    monkeypatch.setattr(scraper, "_post", _fake_post)

    payload = scraper.fetch_posts_graphql("bravotv", None, 0.0)

    assert payload is None
    assert retire_calls
    assert retire_calls[0][0] == "identity-1"


def test_scrape_graphql_marks_initial_failure_without_shortcode_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = InstagramScraper(cookies={"sessionid": "seed", "csrftoken": "csrf", "ds_user_id": "1"})

    monkeypatch.setattr(scraper, "fetch_profile_info", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(scraper, "fetch_posts_graphql", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(scraper, "_extract_profile_total_posts", lambda *_args, **_kwargs: None)

    posts = scraper._scrape_graphql(
        cast(
            ScrapeConfig,
            type(
                "Cfg",
                (),
                {
                    "username": "bravotv",
                    "delay_seconds": 0.0,
                    "max_pages": 1,
                    "no_match_page_limit": None,
                    "max_scrape_seconds": 60.0,
                    "date_start": None,
                    "date_end": None,
                    "hashtags": [],
                    "fast_mode": False,
                    "matches_hashtags": lambda self, text: True,
                    "is_in_date_range": lambda self, ts: True,
                    "show_id": None,
                    "season_number": None,
                    "person_id": None,
                },
            )(),
        )
    )

    assert posts == []
    assert scraper.last_retrieval_meta["initial_page_failed"] is True
    assert "shortcode_fallback_used" not in scraper.last_retrieval_meta


def test_scrape_graphql_unknown_timestamp_does_not_trip_date_start(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = InstagramScraper(cookies={"sessionid": "seed", "csrftoken": "csrf", "ds_user_id": "1"})

    monkeypatch.setattr(scraper, "fetch_profile_info", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        scraper,
        "fetch_posts_graphql",
        lambda *_args, **_kwargs: _graphql_posts_payload(
            [{"code": "UNKNOWN_TS", "pk": "1", "id": "1", "taken_at_timestamp": 0}]
        ),
    )

    posts = scraper._scrape_graphql(
        ScrapeConfig(
            username="bravotv",
            hashtags=[],
            date_start=datetime(2026, 1, 1, tzinfo=UTC),
            delay_seconds=0.0,
            max_pages=1,
            max_scrape_seconds=60.0,
        )
    )

    assert [post.shortcode for post in posts] == ["UNKNOWN_TS"]
    assert posts[0].taken_at == 0
    assert scraper.last_retrieval_meta["stop_reason"] == "no_more_pages"


def test_scrape_graphql_single_older_pinned_node_does_not_stop_page(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = InstagramScraper(cookies={"sessionid": "seed", "csrftoken": "csrf", "ds_user_id": "1"})

    monkeypatch.setattr(scraper, "fetch_profile_info", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        scraper,
        "fetch_posts_graphql",
        lambda *_args, **_kwargs: _graphql_posts_payload(
            [
                {"code": "OLD_PINNED", "pk": "1", "id": "1", "taken_at_timestamp": 1_700_000_000},
                {"code": "IN_RANGE", "pk": "2", "id": "2", "taken_at_timestamp": 1_767_225_600},
            ]
        ),
    )

    posts = scraper._scrape_graphql(
        ScrapeConfig(
            username="bravotv",
            hashtags=[],
            date_start=datetime(2026, 1, 1, tzinfo=UTC),
            delay_seconds=0.0,
            max_pages=1,
            max_scrape_seconds=60.0,
        )
    )

    assert [post.shortcode for post in posts] == ["IN_RANGE"]
    assert scraper.last_retrieval_meta["stop_reason"] == "no_more_pages"


def test_scrape_graphql_cursor_failure_survives_final_retrieval_meta(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = InstagramScraper(cookies={"sessionid": "seed", "csrftoken": "csrf", "ds_user_id": "1"})
    calls = 0

    monkeypatch.setattr(scraper, "fetch_profile_info", lambda *_args, **_kwargs: None)

    def _fake_fetch(_username: str, cursor: str | None, *_args: Any, **_kwargs: Any):
        nonlocal calls
        calls += 1
        if calls == 1:
            return _graphql_posts_payload(
                [{"code": "FIRST", "pk": "1", "id": "1", "taken_at_timestamp": 1_767_225_600}],
                has_next=True,
                end_cursor="cursor-2",
            )
        assert cursor == "cursor-2"
        scraper.last_retrieval_meta = {
            "error_code": "instagram_graphql_cursor_unauthorized",
            "error_class": "HTTPError",
            "error_status_code": 401,
            "retryable": True,
            "graphql_cursor": cursor,
            "transport": "requests_enriched",
        }
        return None

    monkeypatch.setattr(scraper, "fetch_posts_graphql", _fake_fetch)

    posts = scraper._scrape_graphql(
        ScrapeConfig(
            username="bravotv",
            hashtags=[],
            delay_seconds=0.0,
            max_pages=None,
            max_scrape_seconds=60.0,
        )
    )

    assert [post.shortcode for post in posts] == ["FIRST"]
    assert scraper.last_retrieval_meta["stop_reason"] == "graphql_empty_or_error"
    assert scraper.last_retrieval_meta["error_code"] == "instagram_graphql_cursor_unauthorized"
    assert scraper.last_retrieval_meta["error_status_code"] == 401
    assert scraper.last_retrieval_meta["retryable"] is True
    assert scraper.last_retrieval_meta["graphql_cursor"] == "cursor-2"
    assert scraper.last_retrieval_meta["partial_history"] is True


def test_fetch_posts_graphql_uses_request_client(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = InstagramScraper(cookies={})
    calls: list[str] = []

    class _Client:
        def post_form_json(
            self,
            url: str,
            *,
            query_type: str,
            headers: dict[str, Any],
            cookies: dict[str, str],
            data: dict[str, Any],
            **_kwargs: Any,
        ) -> dict[str, Any]:
            del url, headers, cookies, data
            calls.append(query_type)
            return {
                "data": {
                    "xdt_api__v1__feed__user_timeline_graphql_connection": {
                        "edges": [],
                        "page_info": {"has_next_page": False, "end_cursor": None},
                    }
                }
            }

    monkeypatch.setattr(scraper, "_request_client", _Client())
    monkeypatch.setattr(scraper, "_warm_profile_request_context", lambda *args, **kwargs: {"lsd": "lsd-token"})

    payload = scraper.fetch_posts_graphql("bravotv", None, 0.0)

    assert payload is not None
    assert calls == ["graphql_profile_posts"]


def test_fetch_profile_page_content_graphql_uses_copied_profile_query(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = InstagramScraper(cookies={"sessionid": "x", "csrftoken": "y", "ds_user_id": "541"})
    captured: dict[str, Any] = {}

    monkeypatch.setattr(scraper, "resolve_profile_user_id_graphql", lambda *_args, **_kwargs: "123456")
    monkeypatch.setattr(
        scraper,
        "_warm_profile_request_context",
        lambda *_args, **_kwargs: {
            "lsd": "lsd-1",
            "bloks_version": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        },
    )
    monkeypatch.setattr(scraper, "_request_cookies", lambda: {"sessionid": "x", "csrftoken": "y", "ds_user_id": "541"})
    monkeypatch.setattr(scraper, "_graphql_form_runtime_fields", lambda **_kwargs: {"lsd": "lsd-1", "jazoest": "2913"})
    monkeypatch.setattr(scraper, "_profile_page_content_doc_ids", lambda: ["35710877621861450"])

    class _Client:
        def post_form_json(
            self,
            url: str,
            *,
            query_type: str,
            headers: dict[str, Any],
            cookies: dict[str, str],
            data: dict[str, Any],
            **_kwargs: Any,
        ) -> dict[str, Any]:
            captured.update(
                {
                    "url": url,
                    "query_type": query_type,
                    "headers": headers,
                    "cookies": cookies,
                    "data": data,
                    "variables": json.loads(data["variables"]),
                }
            )
            return {
                "data": {
                    "user": {
                        "id": "123456",
                        "pk": "123456",
                        "username": "sampleaccount",
                        "follower_count": 123_456,
                    }
                }
            }

    monkeypatch.setattr(scraper, "_request_client", _Client())

    payload = scraper.fetch_profile_page_content_graphql("sampleaccount", delay=0.0)

    assert payload is not None
    assert captured["url"] == scraper.GRAPHQL_URL
    assert captured["query_type"] == "graphql_profile_page_content"
    assert captured["headers"]["x-fb-friendly-name"] == "PolarisProfilePageContentQuery"
    assert captured["headers"]["x-root-field-name"] == "fetch__XDTUserDict"
    assert captured["headers"]["x-ig-app-id"] == "936619743392459"
    assert captured["headers"]["x-fb-lsd"] == "lsd-1"
    assert captured["data"]["doc_id"] == "35710877621861450"
    assert captured["variables"]["id"] == "123456"
    assert captured["variables"]["enable_integrity_filters"] is True
    assert payload["data"]["user"]["username"] == "sampleaccount"


def test_redirect_login_failure_marks_auth_block_and_attempts_interactive_repair(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scraper = InstagramScraper(cookies={"sessionid": "seed", "csrftoken": "csrf", "ds_user_id": "1"})
    monkeypatch.setattr(scraper, "_is_local_environment", lambda: True)
    monkeypatch.setattr(scraper, "_playwright_graphql_fallback_enabled", lambda: False)
    monkeypatch.setattr(
        scraper, "_try_interactive_login", lambda: {"refreshed": False, "reason": "interactive_login_error"}
    )
    monkeypatch.setattr(scraper, "_try_auto_refresh_cookies", lambda: {"refreshed": False, "reason": "noop"})
    monkeypatch.setattr(
        scraper,
        "_warm_profile_request_context",
        lambda *args, **kwargs: {
            "lsd": "lsd-token",
            "bloks_version": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        },
    )

    class _Client:
        def post_form_json(self, *args, **kwargs):
            raise InstagramRequestFailure(
                "redirect_login",
                status_code=302,
                retryable=False,
                redirect_target="https://www.instagram.com/accounts/login/",
            )

    scraper._request_client = cast(InstagramRequestClient, _Client())

    payload = scraper.fetch_posts_graphql("bravotv", "cursor-1", 0.0)

    assert payload is None
    assert scraper.last_retrieval_meta["request_error_code"] == "redirect_login"
    assert scraper.last_retrieval_meta["redirect_target"] == "https://www.instagram.com/accounts/login/"


def test_fetch_posts_graphql_can_disable_auth_recovery_side_effects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scraper = InstagramScraper(cookies={"sessionid": "seed", "csrftoken": "csrf", "ds_user_id": "1"})
    monkeypatch.setattr(scraper, "_is_local_environment", lambda: True)
    monkeypatch.setattr(
        scraper,
        "_try_auto_refresh_cookies",
        lambda: pytest.fail("auto-refresh should be disabled for non-repairing shared fetches"),
    )
    monkeypatch.setattr(
        scraper,
        "_try_interactive_login",
        lambda: pytest.fail("interactive login should be disabled for non-repairing shared fetches"),
    )
    monkeypatch.setattr(
        scraper,
        "_fetch_posts_graphql_with_browser",
        lambda *_args, **_kwargs: pytest.fail("browser fallback should be disabled for this probe"),
    )
    monkeypatch.setattr(
        scraper,
        "_warm_profile_request_context",
        lambda *args, **kwargs: {
            "lsd": "lsd-token",
            "bloks_version": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        },
    )

    class _Client:
        def post_form_json(self, *args, **kwargs):
            raise InstagramRequestFailure(
                "redirect_login",
                status_code=302,
                retryable=False,
                redirect_target="https://www.instagram.com/accounts/login/",
            )

    scraper._request_client = cast(InstagramRequestClient, _Client())

    payload = scraper.fetch_posts_graphql(
        "bravotv",
        "cursor-1",
        0.0,
        allow_browser_fallback=False,
        allow_recovery=False,
    )

    assert payload is None
    assert scraper.last_retrieval_meta["request_error_code"] == "redirect_login"


def test_interactive_login_runs_sync_playwright_outside_active_async_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scraper = InstagramScraper(cookies={"sessionid": "seed"}, browser_account_id="bravotv")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_INTERACTIVE_LOGIN", "1")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_AUTH_REPAIR_CONFIRMATION", "I UNDERSTAND INSTAGRAM AUTH RISK")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_BROWSER_MODE", "headless")
    monkeypatch.setattr(scraper, "_is_local_environment", lambda: True)
    caller_thread_id = threading.get_ident()
    captured: dict[str, Any] = {}

    def fake_interactive_chrome_login(**kwargs: Any) -> dict[str, str]:
        captured["thread_id"] = threading.get_ident()
        captured["kwargs"] = kwargs
        return {"sessionid": "fresh-session", "csrftoken": "fresh-csrf", "ds_user_id": "123"}

    from trr_backend.socials.instagram import cookie_refresh_runtime

    monkeypatch.setattr(cookie_refresh_runtime, "interactive_chrome_login", fake_interactive_chrome_login)

    async def run_login() -> dict[str, Any]:
        return scraper._try_interactive_login()

    result = asyncio.run(run_login())

    assert result["refreshed"] is True
    assert result["method"] == "interactive_chrome"
    assert captured["thread_id"] != caller_thread_id
    assert captured["kwargs"]["validation_username"] == "bravotv"
    assert scraper.cookies["sessionid"] == "fresh-session"
    assert scraper.session.cookies.get("csrftoken", domain=".instagram.com") == "fresh-csrf"


def test_interactive_login_is_disabled_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = InstagramScraper(cookies={"sessionid": "seed"}, browser_account_id="bravotv")
    monkeypatch.delenv("SOCIAL_INSTAGRAM_INTERACTIVE_LOGIN", raising=False)
    monkeypatch.setattr(
        scraper,
        "_is_local_environment",
        lambda: pytest.fail("default-disabled interactive login should stop before runtime checks"),
    )

    result = scraper._try_interactive_login()

    assert result == {"refreshed": False, "reason": "interactive_login_disabled"}


def test_interactive_login_requires_operator_confirmation(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = InstagramScraper(cookies={"sessionid": "seed"}, browser_account_id="bravotv")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_INTERACTIVE_LOGIN", "1")
    monkeypatch.delenv("SOCIAL_INSTAGRAM_AUTH_REPAIR_CONFIRMATION", raising=False)
    monkeypatch.setattr(
        scraper,
        "_is_local_environment",
        lambda: pytest.fail("unconfirmed interactive login should stop before runtime checks"),
    )

    result = scraper._try_interactive_login()

    assert result == {"refreshed": False, "reason": "instagram_auth_repair_confirmation_required"}


def test_fetch_profile_info_tolerates_duplicate_csrftoken_session_cookies(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = InstagramScraper(cookies={"sessionid": "seed", "csrftoken": "csrf", "ds_user_id": "1"})
    scraper.session.cookies.set("csrftoken", "csrf-a", domain=".instagram.com")
    scraper.session.cookies.set("csrftoken", "csrf-b", domain="i.instagram.com")

    warmed: list[bool] = []
    monkeypatch.setattr(
        scraper,
        "_warm_profile_request_context",
        lambda *args, **kwargs: warmed.append(True) or {"lsd": "lsd-token"},
    )

    class _Client:
        def get_json(self, *args, **kwargs):
            return {"user": {"username": "bravotv"}}

    monkeypatch.setattr(scraper, "_request_client", _Client())
    monkeypatch.setattr(scraper, "_get", lambda *args, **kwargs: _FakeResponse())

    payload = scraper.fetch_profile_info("bravotv", delay=0.0)

    assert payload == {"user": {"username": "bravotv"}}
    assert warmed == []


def test_checkpoint_failure_does_not_retire_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = InstagramScraper(cookies={"sessionid": "seed", "csrftoken": "csrf", "ds_user_id": "1"})
    scraper._identity_pool_enabled = True

    retire_calls: list[str] = []
    refresh_attempted: list[bool] = []

    class _Pool:
        proxy_probe_failures: list[str] = []

        def retire(self, session_id: str, *, reason: str, block_reason: str | None = None) -> None:
            retire_calls.append(f"{session_id}:{reason}:{block_reason}")

        def merge_cookies(self, session_id: str, cookies: dict[str, str]) -> None:
            del session_id, cookies

    scraper._identity_pool = cast(InstagramIdentityPool, _Pool())
    scraper._active_identity = cast(
        InstagramScraperIdentity,
        type("Identity", (), {"session_id": "ig-1", "cookies": dict(scraper.cookies)})(),
    )
    monkeypatch.setattr(
        scraper,
        "_try_auto_refresh_cookies",
        lambda: refresh_attempted.append(True) or {"refreshed": False, "reason": "noop"},
    )
    monkeypatch.setattr(scraper, "_playwright_graphql_fallback_enabled", lambda: False)
    monkeypatch.setattr(scraper, "_is_local_environment", lambda: False)
    monkeypatch.setattr(
        scraper,
        "_warm_profile_request_context",
        lambda *args, **kwargs: {
            "lsd": "lsd-token",
            "bloks_version": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        },
    )

    class _Client:
        def post_form_json(self, *args, **kwargs):
            raise InstagramRequestFailure("checkpoint_required", status_code=400, retryable=False)

    scraper._request_client = cast(InstagramRequestClient, _Client())

    payload = scraper.fetch_posts_graphql("bravotv", "cursor-1", 0.0)

    assert payload is None
    assert refresh_attempted == [True]
    assert retire_calls == []
