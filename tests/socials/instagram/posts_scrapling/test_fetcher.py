from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest


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
