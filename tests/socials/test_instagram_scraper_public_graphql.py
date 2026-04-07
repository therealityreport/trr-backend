from __future__ import annotations

from typing import Any

import pytest
import requests

from trr_backend.socials.instagram.scraper import InstagramScraper


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
    assert captured["post_headers"]["x-fb-friendly-name"] == "PolarisProfilePostsTabContentQuery_connection"
    assert captured["post_headers"]["x-fb-lsd"] == "lsd-token"
    assert captured["post_headers"]["x-csrftoken"] == "csrf-token"
    assert captured["post_headers"]["x-asbd-id"] == scraper.WEB_X_ASBD_ID
    assert (
        captured["post_headers"]["x-bloks-version-id"]
        == "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    assert captured["post_data"]["doc_id"] == "25645538101792896"
    assert captured["post_data"]["av"] == "0"
    assert captured["post_data"]["__user"] == "0"
    assert captured["post_data"]["lsd"] == "lsd-token"
    assert captured["post_data"]["jazoest"] == "2913"


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
    assert scraper.last_retrieval_meta["retryable"] is True
    assert scraper.last_retrieval_meta["graphql_cursor"] == "cursor-1"
    assert scraper.last_retrieval_meta["transport"] == "requests_enriched"


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
    ("error_code", "cursor"),
    [
        ("instagram_graphql_checkpoint_required", None),
        ("instagram_graphql_cursor_rate_limited", "cursor-1"),
        ("instagram_graphql_cursor_unauthorized", "cursor-1"),
        ("instagram_graphql_cursor_forbidden", "cursor-1"),
    ],
)
def test_fetch_posts_graphql_skips_browser_fallback_for_unrecoverable_errors(
    monkeypatch: pytest.MonkeyPatch,
    error_code: str,
    cursor: str | None,
) -> None:
    scraper = InstagramScraper(cookies={})
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

    payload = scraper.fetch_posts_graphql("bravotv", cursor, 0.0)

    assert payload is None
    assert browser_called is False


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
