from types import SimpleNamespace

import pytest

from trr_backend.socials.twitter import public_thread


def post(id, parent=None, replies=1):
    return SimpleNamespace(tweet_id=id, reply_to_tweet_id=parent, replies=replies)


def result(posts, pages=1, error=None):
    return SimpleNamespace(tweets=posts, pages=[{}] * pages, error=error, stop_reason="max_pages", next_cursor="more")


def test_shared_request_budget_dedup_and_parent_links(monkeypatch):
    calls = []

    def fetch(id, **limits):
        calls.append((id, limits))
        if id == "100":
            return result([post("100"), post("101", "100"), post("102", "100")], 2)
        return result([post(id, "100"), post(id + "0", id)])

    monkeypatch.setattr(public_thread, "fetch_public_conversation", fetch)
    data = public_thread.expand_public_thread("100", provider="fxtwitter")
    assert data["requests_used"] == 5
    assert len(calls) == 4
    assert len({p.tweet_id for p in data["posts"]}) == len(data["posts"])
    assert data["unvisited_branches"]
    assert data["complete"] is False
    assert data["status"] == "partial"
    assert data["branches"][1]["continuation_unvisited"] is True


def test_error_consumes_request_and_stops(monkeypatch):
    monkeypatch.setattr(public_thread, "fetch_public_conversation", lambda *a, **k: result([], 0, "Mirror HTTP 429"))
    data = public_thread.expand_public_thread("100", provider="fxtwitter")
    assert data["requests_used"] == 1 and data["stop_reason"] == "provider_error"
    assert data["status"] == "unavailable"


def test_post_budget_and_unknown_coverage(monkeypatch):
    monkeypatch.setattr(
        public_thread, "fetch_public_conversation", lambda *a, **k: result([post("100"), post("101", "100")])
    )
    data = public_thread.expand_public_thread("100", provider="fxtwitter", max_posts=1)
    assert len(data["posts"]) == 1 and data["complete"] is False


@pytest.mark.parametrize(
    "kwargs", [{"provider": "other"}, {"max_requests": 6}, {"max_posts": 101}, {"max_requests": True}]
)
def test_invalid_bounds(kwargs):
    with pytest.raises(ValueError):
        public_thread.expand_public_thread("100", **({"provider": "fxtwitter"} | kwargs))
