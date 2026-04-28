from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_FIXTURE_DIR = Path(__file__).parents[3] / "fixtures" / "instagram" / "scrapling"


def _fixture_text(name: str) -> str:
    return (_FIXTURE_DIR / name).read_text(encoding="utf-8")


@pytest.fixture
def _mock_scrapling(monkeypatch):
    """Prevent real Scrapling import."""
    mock_fetcher_cls = MagicMock()
    mock_module = MagicMock()
    mock_module.StealthyFetcher = mock_fetcher_cls
    mock_module.ProxyRotator = MagicMock()
    monkeypatch.setitem(__import__("sys").modules, "scrapling.fetchers", mock_module)
    return mock_fetcher_cls


def test_fetcher_constructs_without_error(_mock_scrapling):
    from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher

    fetcher = InstagramPostsScraplingFetcher(
        cookies=[{"name": "sessionid", "value": "abc", "domain": ".instagram.com", "path": "/"}],
        raw_cookies={"sessionid": "abc", "csrftoken": "xyz", "ds_user_id": "123"},
        browser_account_id="test",
    )
    assert fetcher._request_count == 0


def test_extract_page_tokens():
    from trr_backend.socials.instagram.posts_scrapling.fetcher import _extract_page_tokens

    html = """
    "LSD",[],{"token":"abc123_lsd"}
    bloks_version":"deadbeef1234567890abcdef12345678"
    "__spin_r":1234
    "__spin_b":"spin_b_val"
    "__spin_t":5678
    "hsi":"9999"
    """
    tokens = _extract_page_tokens(html)
    assert tokens["lsd"] == "abc123_lsd"
    assert tokens["bloks_version"] == "deadbeef1234567890abcdef12345678"
    assert tokens["__spin_r"] == "1234"
    assert tokens["__spin_b"] == "spin_b_val"
    assert tokens["__spin_t"] == "5678"
    assert tokens["hsi"] == "9999"


def test_extract_page_tokens_missing_values():
    from trr_backend.socials.instagram.posts_scrapling.fetcher import _extract_page_tokens

    tokens = _extract_page_tokens("<html><body>nothing here</body></html>")
    assert tokens == {}


def test_build_graphql_headers():
    from trr_backend.socials.instagram.posts_scrapling.fetcher import _build_graphql_headers

    headers = _build_graphql_headers(
        referer="https://www.instagram.com/testuser/",
        csrftoken="csrf_val",
        lsd_token="lsd_val",
        bloks_version="bloks_val",
    )
    assert headers["x-csrftoken"] == "csrf_val"
    assert headers["x-fb-lsd"] == "lsd_val"
    assert headers["x-bloks-version-id"] == "bloks_val"
    assert headers["x-requested-with"] == "XMLHttpRequest"
    assert headers["referer"] == "https://www.instagram.com/testuser/"
    assert "x-ig-app-id" in headers


def test_build_graphql_form_data():
    from trr_backend.socials.instagram.posts_scrapling.fetcher import _build_graphql_form_data

    data = _build_graphql_form_data(
        username="testuser",
        cursor=None,
        page_size=33,
        viewer_id="123",
        page_tokens={"lsd": "lsd_val", "__spin_r": "1234"},
        doc_id="25645538101792896",
    )
    assert data["doc_id"] == "25645538101792896"
    assert data["fb_api_req_friendly_name"] == "PolarisProfilePostsTabContentQuery_connection"
    variables = json.loads(data["variables"])
    assert variables["username"] == "testuser"
    assert variables["first"] == 33
    assert variables["after"] is None
    assert data["lsd"] == "lsd_val"


def test_result_dataclass():
    from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsFetchResult

    result = InstagramPostsFetchResult(posts=[{"shortcode": "abc"}])
    assert len(result.posts) == 1
    assert result.fetch_failed is False
    assert result.has_next_page is False


def test_runtime_metadata_never_exposes_cookie_values(_mock_scrapling):
    """warmup_cookie_delta previously leaked {name: value} into scrape_jobs.metadata.
    Must now only expose names and counts."""
    from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher

    fetcher = InstagramPostsScraplingFetcher(
        cookies=[],
        raw_cookies={"sessionid": "existing"},
        browser_account_id="test",
    )
    # Simulate warmup having merged new cookies
    fetcher._warmup_cookie_delta = {"sessionid": "new-sensitive-value", "csrftoken": "secret-token"}

    meta = fetcher.runtime_metadata
    # Cookie values must not appear in the metadata dict, anywhere
    serialized = repr(meta)
    assert "new-sensitive-value" not in serialized
    assert "secret-token" not in serialized
    # Names and count should be reported (sorted alphabetically for deterministic order)
    assert meta.get("warmup_cookie_names") == ["csrftoken", "sessionid"]
    assert meta.get("warmup_cookie_count") == 2
    # The old field must not exist
    assert "warmup_cookie_delta" not in meta


def test_runtime_metadata_reports_delay_and_proxy_session_mode(_mock_scrapling):
    from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher
    from trr_backend.socials.instagram.posts_scrapling.proxy import PostsProxyConfig

    fetcher = InstagramPostsScraplingFetcher(
        cookies=[],
        raw_cookies={"sessionid": "existing"},
        browser_account_id="test",
        proxy_config=PostsProxyConfig(
            browser_proxy="http://proxy:8080",
            api_proxy_url="http://proxy:8080",
            proxy_rotator=None,
            fingerprint="proxy:8080:explicit",
            session_mode="explicit",
        ),
    )

    meta = fetcher.runtime_metadata
    assert meta["proxy_session_mode"] == "explicit"
    assert meta["api_delay_seconds"] >= 0


def test_sync_response_cookies_updates_direct_request_state(_mock_scrapling):
    from unittest.mock import MagicMock

    from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher

    fetcher = InstagramPostsScraplingFetcher(
        cookies=[],
        raw_cookies={"sessionid": "existing", "csrftoken": "old", "ds_user_id": "1"},
        browser_account_id="test",
    )
    response = MagicMock()
    response.cookies = {"csrftoken": "fresh-csrf", "ds_user_id": "2"}

    fetcher._sync_response_cookies(response)

    assert fetcher._raw_cookies["csrftoken"] == "fresh-csrf"
    assert fetcher._raw_cookies["ds_user_id"] == "2"


def test_warmup_emits_structured_log_success(_mock_scrapling, caplog):
    """After a successful warmup, an info log with event=warmup_success should fire."""
    import asyncio
    from unittest.mock import AsyncMock, MagicMock

    from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher

    fetcher = InstagramPostsScraplingFetcher(
        cookies=[],
        raw_cookies={"sessionid": "x"},
        browser_account_id="t",
    )

    # Mock the browser navigation to return a fake response with status 200
    fake_resp = MagicMock()
    fake_resp.status_code = 200
    fake_resp.text = "<html>LSD token stuff</html>"
    fake_resp.cookies = {"fresh_cookie": "v"}
    fetcher._fetcher.async_fetch = AsyncMock(return_value=fake_resp)

    with caplog.at_level("INFO", logger="socials.instagram.posts_scrapling.fetcher"):
        asyncio.run(fetcher.warmup("bravotv"))

    events = [r for r in caplog.records if getattr(r, "event", None) == "warmup_success"]
    assert len(events) == 1
    assert events[0].account == "bravotv"
    assert events[0].cookie_count >= 1


def test_warmup_raises_auth_error_code(_mock_scrapling):
    import asyncio
    from unittest.mock import AsyncMock

    from trr_backend.socials.instagram.posts_scrapling.fetcher import (
        InstagramPostsScraplingFetcher,
        InstagramPostsWarmupError,
    )

    fetcher = InstagramPostsScraplingFetcher(
        cookies=[],
        raw_cookies={"sessionid": "x"},
        browser_account_id="t",
    )
    fake_resp = MagicMock()
    fake_resp.status_code = 200
    fake_resp.text = _fixture_text("auth_failure.html")
    fake_resp.cookies = {}
    fetcher._fetcher.async_fetch = AsyncMock(return_value=fake_resp)

    with pytest.raises(InstagramPostsWarmupError) as exc_info:
        asyncio.run(fetcher.warmup("bravotv"))

    assert exc_info.value.error_code == "instagram_posts_warmup_auth_failed"
    assert exc_info.value.retryable is False


def test_warmup_raises_no_cookie_when_no_prior_session(_mock_scrapling):
    import asyncio
    from unittest.mock import AsyncMock

    from trr_backend.socials.instagram.posts_scrapling.fetcher import (
        InstagramPostsScraplingFetcher,
        InstagramPostsWarmupError,
    )

    fetcher = InstagramPostsScraplingFetcher(
        cookies=[],
        raw_cookies={"csrftoken": "csrf"},
        browser_account_id="t",
    )
    fake_resp = MagicMock()
    fake_resp.status_code = 200
    fake_resp.text = _fixture_text("no_cookie_warmup.html")
    fake_resp.cookies = {}
    fetcher._fetcher.async_fetch = AsyncMock(return_value=fake_resp)
    fetcher._rebuild_http_client = AsyncMock()

    with pytest.raises(InstagramPostsWarmupError) as exc_info:
        asyncio.run(fetcher.warmup("bravotv"))

    assert exc_info.value.error_code == "instagram_posts_warmup_no_cookies"
    assert exc_info.value.retryable is True
    assert fetcher.runtime_metadata["warmup_cookie_count"] == 0
    fetcher._rebuild_http_client.assert_not_awaited()


def test_warmup_allows_no_new_cookies_when_prior_session_exists(_mock_scrapling):
    import asyncio
    from unittest.mock import AsyncMock

    from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher

    fetcher = InstagramPostsScraplingFetcher(
        cookies=[],
        raw_cookies={"sessionid": "existing", "csrftoken": "csrf"},
        browser_account_id="t",
    )
    fake_resp = MagicMock()
    fake_resp.status_code = 200
    fake_resp.text = _fixture_text("no_cookie_warmup.html")
    fake_resp.cookies = {}
    fetcher._fetcher.async_fetch = AsyncMock(return_value=fake_resp)
    fetcher._rebuild_http_client = AsyncMock()

    asyncio.run(fetcher.warmup("bravotv"))

    fetcher._rebuild_http_client.assert_awaited_once()
