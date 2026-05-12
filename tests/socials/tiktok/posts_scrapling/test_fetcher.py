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


def test_tiktok_challenge_detection():
    from trr_backend.socials.tiktok.posts_scrapling.fetcher import _is_challenge_response

    assert _is_challenge_response("<html><body>captcha verify</body></html>") is True
    assert _is_challenge_response('{"statusCode": 0}') is False


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
