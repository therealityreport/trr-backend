"""Pagination + doc_id rotation behavior for Instagram posts fetcher."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

import pytest


def _make_fetcher(monkeypatch):
    """Build a fetcher with scrapling mocked out."""
    with patch("scrapling.fetchers.StealthyFetcher", MagicMock()):
        from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher

        fetcher = InstagramPostsScraplingFetcher(
            cookies=[],
            raw_cookies={"sessionid": "x", "csrftoken": "y", "ds_user_id": "1"},
            browser_account_id="t",
        )
        fetcher._page_tokens = {"lsd": "L", "bloks_version": "B"}
    return fetcher


@pytest.fixture
def _no_sleep(monkeypatch):
    """Patch the module-local asyncio.sleep so retries don't actually wait."""

    async def _noop(*a, **k):
        return None

    monkeypatch.setattr("trr_backend.socials.instagram.posts_scrapling.fetcher.asyncio.sleep", _noop)


def test_fetch_posts_page_returns_failure_on_all_doc_ids_failed(monkeypatch, _no_sleep):
    """If every doc_id returns a failure response, fetch_posts_page returns failed."""
    fetcher = _make_fetcher(monkeypatch)

    async def always_fail(*a, **kw):
        return {
            "failed": True,
            "auth_failed": False,
            "reason": "http_500",
            "retryable": True,
            "payload": None,
        }

    monkeypatch.setattr(fetcher, "_fetch_json_response", always_fail)
    result = asyncio.run(fetcher.fetch_posts_page("testuser"))
    assert result.fetch_failed is True
    assert result.posts == []


def test_fetch_posts_page_extracts_edges_on_first_doc_id_success(monkeypatch, _no_sleep):
    """If the first doc_id succeeds, extract edges and stop trying other doc_ids."""
    fetcher = _make_fetcher(monkeypatch)
    payload = {
        "data": {
            "xdt_api__v1__feed__user_timeline_graphql_connection": {
                "edges": [
                    {"node": {"__typename": "XDTMediaDict", "code": "AAA"}},
                    {"node": {"__typename": "XDTMediaDict", "code": "BBB"}},
                ],
                "page_info": {"has_next_page": True, "end_cursor": "cursor_1"},
            }
        }
    }

    call_count = {"n": 0}

    async def succeed(*a, **kw):
        call_count["n"] += 1
        return {
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
            "payload": payload,
        }

    monkeypatch.setattr(fetcher, "_fetch_json_response", succeed)
    result = asyncio.run(fetcher.fetch_posts_page("testuser"))
    assert len(result.posts) == 2
    assert result.posts[0]["code"] == "AAA"
    assert result.has_next_page is True
    assert result.end_cursor == "cursor_1"
    assert call_count["n"] == 1, "Should not have tried additional doc_ids after success"


def test_fetch_posts_page_rotates_doc_id_on_empty_connection(monkeypatch, _no_sleep):
    """If the first doc_id returns valid response but no connection data,
    try the next doc_id."""
    fetcher = _make_fetcher(monkeypatch)
    call_count = {"n": 0}

    async def rotator(*a, **kw):
        call_count["n"] += 1
        if call_count["n"] == 1:
            # First doc_id returns valid response but no connection
            return {
                "failed": False,
                "auth_failed": False,
                "reason": None,
                "retryable": False,
                "payload": {"data": {}},
            }
        # Second doc_id returns posts
        return {
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
            "payload": {
                "data": {
                    "xdt_api__v1__feed__user_timeline_graphql_connection": {
                        "edges": [{"node": {"__typename": "XDTMediaDict", "code": "CCC"}}],
                        "page_info": {"has_next_page": False, "end_cursor": None},
                    }
                }
            },
        }

    monkeypatch.setattr(fetcher, "_fetch_json_response", rotator)
    result = asyncio.run(fetcher.fetch_posts_page("testuser"))
    assert len(result.posts) == 1
    assert result.posts[0]["code"] == "CCC"
    assert call_count["n"] == 2, "Second doc_id should have been tried"


def test_fetch_posts_page_breaks_on_auth_failed_without_doc_id_rotation(monkeypatch, _no_sleep):
    """Auth failures should NOT rotate doc_ids — they are terminal for the
    current page request.

    Production behavior (fetcher.py line 392-395): when current_auth is True,
    the loop breaks immediately. For cursor=None (first-page fetch), this means
    only the first doc_id is attempted before bailing out with auth_failed=True.
    """
    fetcher = _make_fetcher(monkeypatch)
    call_count = {"n": 0}

    async def auth_fail(*a, **kw):
        call_count["n"] += 1
        return {
            "failed": True,
            "auth_failed": True,
            "reason": "redirect_to_login",
            "retryable": False,
            "payload": None,
        }

    monkeypatch.setattr(fetcher, "_fetch_json_response", auth_fail)
    result = asyncio.run(fetcher.fetch_posts_page("testuser"))
    assert result.auth_failed is True
    # Production break condition: `if current_auth or (cursor and not current_retryable): break`
    # With cursor=None and auth_failed=True, break fires on the FIRST doc_id.
    # The loop never reaches doc_ids 2 or 3.
    assert call_count["n"] == 1, (
        "Auth failure on cursor=None should break after the first doc_id, not rotate through all PROFILE_POSTS_DOC_IDS"
    )
