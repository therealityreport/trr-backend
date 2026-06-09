from __future__ import annotations

from unittest.mock import MagicMock

import pytest


@pytest.fixture
def _mock_scrapling(monkeypatch):
    mock_module = MagicMock()
    mock_module.StealthyFetcher = MagicMock()
    mock_module.ProxyRotator = MagicMock()
    monkeypatch.setitem(__import__("sys").modules, "scrapling.fetchers", mock_module)
    return mock_module.StealthyFetcher


def test_tiktok_fetcher_constructs(_mock_scrapling):
    from trr_backend.socials.tiktok.posts_scrapling.fetcher import TikTokPostsScraplingFetcher

    fetcher = TikTokPostsScraplingFetcher(
        cookies=[{"name": "sessionid", "value": "abc", "domain": ".tiktok.com", "path": "/"}],
        raw_cookies={"sessionid": "abc"},
    )
    assert fetcher._request_count == 0


def test_tiktok_build_api_headers():
    from trr_backend.socials.tiktok.posts_scrapling.fetcher import _build_tiktok_headers

    headers = _build_tiktok_headers(referer="https://www.tiktok.com/@testuser")
    assert headers["origin"] == "https://www.tiktok.com"
    assert "user-agent" in headers
    assert headers["referer"] == "https://www.tiktok.com/@testuser"


def test_tiktok_result_dataclass():
    from trr_backend.socials.tiktok.posts_scrapling.fetcher import TikTokPostsFetchResult

    result = TikTokPostsFetchResult(posts=[{"id": "123"}])
    assert len(result.posts) == 1
    assert result.fetch_failed is False


def test_tiktok_extracts_sec_uid_from_warmup_html():
    from trr_backend.socials.tiktok.posts_scrapling.fetcher import _extract_sec_uid_from_text

    assert _extract_sec_uid_from_text('{"user":{"secUid":"SEC123"}}') == "SEC123"
    assert _extract_sec_uid_from_text(r"{\"user\":{\"secUid\":\"SEC456\"}}") == "SEC456"


def test_tiktok_challenge_detection():
    from trr_backend.socials.tiktok.posts_scrapling.fetcher import _classify_challenge_response, _is_challenge_response

    assert _is_challenge_response("<html><body>captcha verify</body></html>") is True
    assert _is_challenge_response('{"statusCode": 0}') is False
    assert _classify_challenge_response("X-Bogus or _signature is required") == "js_generated_params_required"


def test_tiktok_posts_scrapling_page_size_defaults_to_30(monkeypatch):
    from trr_backend.socials.tiktok.posts_scrapling.fetcher import tiktok_posts_scrapling_page_size

    monkeypatch.delenv("SOCIAL_TIKTOK_POSTS_SCRAPLING_PAGE_SIZE", raising=False)

    assert tiktok_posts_scrapling_page_size() == 30


@pytest.mark.parametrize(
    ("env_value", "expected"),
    [
        ("not-an-int", 30),
        ("4", 10),
        ("99", 50),
        ("42", 42),
    ],
)
def test_tiktok_posts_scrapling_page_size_env_bounds(monkeypatch, env_value, expected):
    from trr_backend.socials.tiktok.posts_scrapling.fetcher import tiktok_posts_scrapling_page_size

    monkeypatch.setenv("SOCIAL_TIKTOK_POSTS_SCRAPLING_PAGE_SIZE", env_value)

    assert tiktok_posts_scrapling_page_size() == expected


def test_tiktok_fetch_posts_page_preserves_explicit_count_override(_mock_scrapling, monkeypatch):
    import asyncio

    from trr_backend.socials.tiktok.posts_scrapling.fetcher import TikTokPostsScraplingFetcher

    captured_params = {}
    fetcher = TikTokPostsScraplingFetcher(cookies=[], raw_cookies={})

    async def _fake_fetch_api_json(_url, *, params, referer):
        del referer
        captured_params.update(params)
        return {"failed": False, "payload": {"itemList": [], "hasMore": False}}

    monkeypatch.setenv("SOCIAL_TIKTOK_POSTS_SCRAPLING_PAGE_SIZE", "50")
    monkeypatch.setattr(fetcher, "_fetch_api_json", _fake_fetch_api_json)

    asyncio.run(fetcher.fetch_posts_page(sec_uid="SEC_UID", count=7))

    assert captured_params["count"] == "7"


def test_tiktok_resolve_sec_uid_falls_back_to_warmup_html(_mock_scrapling, monkeypatch):
    import asyncio

    from trr_backend.socials.tiktok.posts_scrapling.fetcher import TikTokPostsScraplingFetcher

    fetcher = TikTokPostsScraplingFetcher(cookies=[], raw_cookies={})
    fetcher._warmup_sec_uid = "SEC_FROM_HTML"

    async def _fake_fetch_api_json(_url, *, params, referer):
        del params, referer
        return {"failed": True, "reason": "non_json_response", "payload": None}

    monkeypatch.setattr(fetcher, "_fetch_api_json", _fake_fetch_api_json)

    assert asyncio.run(fetcher.resolve_sec_uid("bravotv")) == "SEC_FROM_HTML"
    assert fetcher.runtime_metadata["sec_uid_source"] == "warmup_html"


def test_tiktok_runtime_metadata_never_exposes_cookie_values(_mock_scrapling):
    from trr_backend.socials.tiktok.posts_scrapling.fetcher import TikTokPostsScraplingFetcher

    fetcher = TikTokPostsScraplingFetcher(
        cookies=[],
        raw_cookies={"sessionid": "existing-session-secret"},
    )
    fetcher._warmup_cookie_delta = {"sessionid": "new-tiktok-value", "msToken": "signed-token"}

    meta = fetcher.runtime_metadata
    serialized = repr(meta)
    assert "existing-session-secret" not in serialized
    assert "new-tiktok-value" not in serialized
    assert "signed-token" not in serialized
    # sorted alphabetically: msToken comes before sessionid (capital M sorts before lowercase s)
    assert meta.get("warmup_cookie_names") == ["msToken", "sessionid"]
    assert meta.get("warmup_cookie_count") == 2
    assert {
        "warmup_cookie_names",
        "warmup_cookie_count",
        "selected_proxy_fingerprint",
        "sec_uid_resolved",
        "request_count",
        "transport",
    }.issubset(meta)
    assert "warmup_cookie_delta" not in meta
    assert "raw_cookies" not in meta
    assert "cookies" not in meta


def test_tiktok_runtime_metadata_includes_seed_cookie_and_limitation(_mock_scrapling):
    from trr_backend.socials.tiktok.posts_scrapling.fetcher import TikTokPostsScraplingFetcher

    fetcher = TikTokPostsScraplingFetcher(
        cookies=[],
        raw_cookies={"sessionid": "existing-session-secret", "ttwid": "browser-id"},
    )

    meta = fetcher.runtime_metadata
    serialized = repr(meta)
    assert "existing-session-secret" not in serialized
    assert "browser-id" not in serialized
    assert meta["seed_cookie_names"] == ["sessionid", "ttwid"]
    assert meta["seed_cookie_count"] == 2
    assert meta["api_signature_limitation"] == "tiktok_api_may_require_js_generated_params"


def test_tiktok_runtime_metadata_exposes_proxy_fingerprint_only(_mock_scrapling):
    from trr_backend.socials.tiktok.posts_scrapling.fetcher import TikTokPostsScraplingFetcher
    from trr_backend.socials.tiktok.posts_scrapling.proxy import TikTokPostsProxyConfig

    proxy_config = TikTokPostsProxyConfig(
        browser_proxy="http://tiktokuser:secret-pass@proxy.example.test:8000",
        api_proxy_url="http://api-user:api-secret@proxy.example.test:8000",
        proxy_rotator=None,
        fingerprint="proxy.example.test:8000:explicit",
    )
    fetcher = TikTokPostsScraplingFetcher(
        cookies=[],
        raw_cookies={},
        proxy_config=proxy_config,
    )

    meta = fetcher.runtime_metadata
    serialized = repr(meta)
    assert meta["selected_proxy_fingerprint"] == "proxy.example.test:8000:explicit"
    assert "tiktokuser" not in serialized
    assert "secret-pass" not in serialized
    assert "api-user" not in serialized
    assert "api-secret" not in serialized
    assert "api_proxy_url" not in meta
    assert "browser_proxy" not in meta


def test_tiktok_merge_warmup_cookies_syncs_browser_cookie_payload(_mock_scrapling):
    from unittest.mock import MagicMock

    from trr_backend.socials.tiktok.posts_scrapling.fetcher import TikTokPostsScraplingFetcher

    fetcher = TikTokPostsScraplingFetcher(
        cookies=[{"name": "sessionid", "value": "old", "domain": ".tiktok.com", "path": "/"}],
        raw_cookies={"sessionid": "old"},
    )
    fake_resp = MagicMock()
    fake_resp.cookies = {"sessionid": "new", "msToken": "token"}

    fetcher._merge_warmup_cookies(fake_resp)

    assert fetcher._raw_cookies == {"sessionid": "new", "msToken": "token"}
    browser_cookies = {cookie["name"]: cookie for cookie in fetcher._cookies}
    assert browser_cookies["sessionid"]["value"] == "new"
    assert browser_cookies["msToken"]["domain"] == ".tiktok.com"
    assert fetcher.runtime_metadata["cookie_sync_count"] == 2


def test_tiktok_warmup_emits_structured_log_success(_mock_scrapling, caplog):
    import asyncio
    from unittest.mock import AsyncMock, MagicMock

    from trr_backend.socials.tiktok.posts_scrapling.fetcher import TikTokPostsScraplingFetcher

    fetcher = TikTokPostsScraplingFetcher(cookies=[], raw_cookies={"sessionid": "x"})

    fake_resp = MagicMock()
    fake_resp.status_code = 200
    fake_resp.text = "<html>not challenge</html>"
    fake_resp.cookies = {"ttwid": "y"}
    fetcher._fetcher.async_fetch = AsyncMock(return_value=fake_resp)

    with caplog.at_level("INFO", logger="socials.tiktok.posts_scrapling.fetcher"):
        asyncio.run(fetcher.warmup("someone"))

    events = [r for r in caplog.records if getattr(r, "event", None) == "warmup_success"]
    assert len(events) == 1
    assert events[0].account == "someone"


def test_tiktok_warmup_optional_xhr_capture_metadata(_mock_scrapling, monkeypatch):
    import asyncio
    from unittest.mock import AsyncMock, MagicMock

    from trr_backend.socials.tiktok.posts_scrapling.fetcher import TikTokPostsScraplingFetcher

    monkeypatch.setenv("SOCIAL_TIKTOK_POSTS_CAPTURE_XHR", "true")
    fetcher = TikTokPostsScraplingFetcher(cookies=[], raw_cookies={})

    fake_resp = MagicMock()
    fake_resp.status_code = 200
    fake_resp.text = "<html>not challenge</html>"
    fake_resp.cookies = {}
    fake_resp.captured_xhr = [
        {"url": "https://www.tiktok.com/api/user/detail/?uniqueId=someone"},
        {"url": "https://www.tiktok.com/api/post/item_list/?count=30"},
    ]
    fetcher._fetcher.async_fetch = AsyncMock(return_value=fake_resp)

    asyncio.run(fetcher.warmup("someone"))

    call_kwargs = fetcher._fetcher.async_fetch.await_args.kwargs
    assert call_kwargs["capture_xhr"] is True
    meta = fetcher.runtime_metadata
    assert meta["capture_xhr_enabled"] is True
    assert meta["captured_xhr_count"] == 2
    assert meta["captured_xhr_paths"] == ["/api/post/item_list/", "/api/user/detail/"]
