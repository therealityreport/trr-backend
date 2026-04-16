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


def test_tiktok_runtime_metadata_never_exposes_cookie_values(_mock_scrapling):
    from trr_backend.socials.tiktok.posts_scrapling.fetcher import TikTokPostsScraplingFetcher

    fetcher = TikTokPostsScraplingFetcher(
        cookies=[],
        raw_cookies={"sessionid": "existing"},
    )
    fetcher._warmup_cookie_delta = {"sessionid": "new-tiktok-value", "msToken": "signed-token"}

    meta = fetcher.runtime_metadata
    serialized = repr(meta)
    assert "new-tiktok-value" not in serialized
    assert "signed-token" not in serialized
    # sorted alphabetically: msToken comes before sessionid (capital M sorts before lowercase s)
    assert meta.get("warmup_cookie_names") == ["msToken", "sessionid"]
    assert meta.get("warmup_cookie_count") == 2
    assert "warmup_cookie_delta" not in meta


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
