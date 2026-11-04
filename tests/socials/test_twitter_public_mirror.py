from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
import requests

from trr_backend.socials.twitter import direct_scrape, public_mirror


def _post(post_id="100", parent=None):
    return {
        "id": post_id,
        "text": "Public post #space @NASA",
        "author": {"screen_name": "NASA", "name": "NASA", "verified": True},
        "created_at": "Tue Nov 03 15:00:36 +0000 2026",
        "replying_to": {"status": parent} if parent else None,
        "likes": 5,
    }


def _page(*posts, cursor=None, thread=()):
    return {
        "code": 200,
        "status": _post(),
        "thread": list(thread),
        "replies": list(posts),
        "cursor": {"bottom": cursor},
    }


def _serve(monkeypatch, *pages):
    pending = iter(pages)
    urls = []

    def fetch(url):
        urls.append(url)
        page = next(pending)
        if isinstance(page, Exception):
            raise page
        return json.dumps(page).encode()

    monkeypatch.setattr(public_mirror, "_fetch_page", fetch)
    return urls


def test_conversation_maps_thread_and_nested_parentage_and_deduplicates(monkeypatch):
    urls = _serve(
        monkeypatch,
        _page(_post("101", "100"), cursor="next +/", thread=[_post("102", "101")]),
        _page(_post("101", "100"), _post("103", "102")),
    )
    result = public_mirror.fetch_public_conversation("100")

    assert [post.tweet_id for post in result.tweets] == ["100", "102", "101", "103"]
    assert result.tweets[-1].reply_to_tweet_id == "102"
    assert result.reply_ids == {"101", "103"}
    assert result.tweets[1].twitter_context_role == "thread"
    assert result.tweets[-1].likes == 5
    assert result.tweets[-1].created_at > 0
    assert urls[-1].endswith("?cursor=next+%2B%2F")
    assert [page["added"] for page in result.pages] == [3, 1]
    assert len(result.pages[0]["sha256"]) == 64
    coverage = result.coverage()
    assert coverage["pagination_exhausted"] is True
    assert coverage["complete"] is False
    assert coverage["thread_completeness"] == "unknown"
    assert coverage["upstream_authentication"].startswith("unknown")
    assert coverage["authentication_provided"] is False


def test_repeated_cursor_stops_without_losing_new_posts(monkeypatch):
    urls = _serve(monkeypatch, _page(_post("101", "100"), cursor="same"), _page(_post("102", "101"), cursor="same"))
    result = public_mirror.fetch_public_conversation("100", max_pages=10)
    assert result.stop_reason == "repeated_cursor"
    assert len(urls) == 2
    assert len(result.tweets) == 3
    assert result.coverage()["pagination_exhausted"] is False


def test_duplicate_page_with_new_cursor_stops_without_progress(monkeypatch):
    urls = _serve(monkeypatch, _page(cursor="one"), _page(cursor="two"))
    result = public_mirror.fetch_public_conversation("100")
    assert result.stop_reason == "no_progress"
    assert len(urls) == 2


def test_page_budget_and_item_budget_do_not_claim_exhaustion(monkeypatch):
    _serve(monkeypatch, _page(_post("101", "100"), cursor="one"))
    result = public_mirror.fetch_public_conversation("100", max_pages=1)
    assert result.stop_reason == "max_pages"
    assert result.next_cursor == "one"
    _serve(monkeypatch, _page(_post("101", "100"), _post("102", "101")))
    result = public_mirror.fetch_public_conversation("100", max_items=2)
    assert [post.tweet_id for post in result.tweets] == ["100", "101"]
    assert result.stop_reason == "max_items"
    assert result.coverage()["pagination_exhausted"] is False


@pytest.mark.parametrize("failure", [requests.Timeout("private network detail"), ValueError("Mirror HTTP 429")])
def test_request_failure_retains_previous_page(monkeypatch, failure):
    _serve(monkeypatch, _page(_post("101", "100"), cursor="one"), failure)
    result = public_mirror.fetch_public_conversation("100")
    assert result.stop_reason == "error"
    assert len(result.tweets) == 2
    assert len(result.pages) == 1
    assert result.error is not None
    assert "private network detail" not in result.error
    assert result.next_cursor == "one"


@pytest.mark.parametrize(
    "bad_page",
    [
        {"code": 429},
        {**_page(), "replies": None},
        {**_page(), "status": _post("999")},
        {**_page(), "cursor": "bad"},
        {**_page(), "cursor": {"bottom": 123}},
        _page(_post("101", "100"), {**_post("102"), "text": None}),
        _page(_post("101", "bad-parent")),
        _page({**_post("101"), "created_at": ["invalid"]}),
        _page({**_post("101"), "replying_to": "invalid"}),
    ],
)
def test_invalid_page_does_not_accept_partial_rows(monkeypatch, bad_page):
    _serve(monkeypatch, bad_page)
    result = public_mirror.fetch_public_conversation("100")
    assert result.stop_reason == "error"
    assert result.tweets == []
    assert result.pages == []


def test_non_json_response_is_an_explicit_failure(monkeypatch):
    monkeypatch.setattr(public_mirror, "_fetch_page", lambda _: b"<html>unavailable</html>")
    result = public_mirror.fetch_public_conversation("100")
    assert result.stop_reason == "error"
    assert result.error is not None


@pytest.mark.parametrize(
    "kwargs",
    [
        {"tweet_id": "100/other"},
        {"tweet_id": "100", "max_pages": 0},
        {"tweet_id": "100", "max_pages": 11},
        {"tweet_id": "100", "max_items": True},
    ],
)
def test_invalid_input_never_fetches(monkeypatch, kwargs):
    monkeypatch.setattr(public_mirror, "_fetch_page", lambda _: pytest.fail("must not fetch"))
    with pytest.raises(ValueError):
        public_mirror.fetch_public_conversation(**kwargs)


def test_opt_in_direct_collector_skips_auth_and_returns_partial_coverage(monkeypatch):
    _serve(monkeypatch, _page(_post("101", "100"), cursor="next", thread=[_post("099")]), requests.Timeout())
    response = direct_scrape.fetch_tweet_replies(
        SimpleNamespace(tweet_id="100"),
        load_auth=lambda: pytest.fail("must not load credentials"),
        conversation_provider="fxtwitter",
    )
    assert response["success"] is True
    assert response["replies_found"] == 1
    assert response["replies"][0]["reply_to_tweet_id"] == "100"
    assert response["complete"] is False
    assert response["error"] == "Mirror request failed"
    assert response["retrieval_meta"]["stop_reason"] == "error"


def test_opt_in_direct_collector_does_not_hide_initial_failure(monkeypatch):
    _serve(monkeypatch, ValueError("Mirror HTTP 503"))
    response = direct_scrape.fetch_tweet_replies(
        SimpleNamespace(tweet_id="100"),
        load_auth=lambda: pytest.fail("must not load credentials"),
        conversation_provider="fxtwitter",
    )
    assert response["success"] is False
    assert response["replies"] == []
    assert response["error"] == "Mirror HTTP 503"
    assert response["complete"] is False


@pytest.mark.parametrize("provider,upload", [("other", False), ("fxtwitter", True)])
def test_direct_collector_rejects_unsupported_or_paid_media_path(monkeypatch, provider, upload):
    monkeypatch.setattr(public_mirror, "_fetch_page", lambda _: pytest.fail("must not fetch"))
    response = direct_scrape.fetch_tweet_replies(
        SimpleNamespace(tweet_id="100", mirror_to_s3=upload),
        load_auth=lambda: pytest.fail("must not load credentials"),
        conversation_provider=provider,
    )
    assert response["success"] is False
    assert response["error"]


@pytest.mark.parametrize(
    "status,chunks,expected",
    [
        (302, [], "Mirror HTTP 302"),
        (200, [b"x" * 1_000_001, b"x" * 1_000_000], "exceeds size limit"),
        (200, [b"{}"], None),
    ],
)
def test_transport_disables_ambient_auth_redirects_and_bounds_body(monkeypatch, status, chunks, expected):
    captured = {}

    class Response:
        status_code = status

        def __enter__(self):
            return self

        def __exit__(self, *_):
            captured["response_closed"] = True

        def iter_content(self, **_):
            return iter(chunks)

    class Session:
        trust_env = True

        def __enter__(self):
            return self

        def __exit__(self, *_):
            captured["session_closed"] = True

        def get(self, url, **kwargs):
            captured.update(kwargs, trust_env=self.trust_env)
            return Response()

    monkeypatch.setattr(public_mirror.requests, "Session", Session)
    if expected:
        with pytest.raises(ValueError, match=expected):
            public_mirror._fetch_page("https://api.fxtwitter.com/2/conversation/100")
    else:
        assert public_mirror._fetch_page("https://api.fxtwitter.com/2/conversation/100") == b"{}"
    assert captured["trust_env"] is False
    assert captured["allow_redirects"] is False
    assert captured["timeout"] == (10, 20)
    assert set(captured["headers"]) == {"Accept", "User-Agent"}
    assert captured["session_closed"] and captured["response_closed"]
