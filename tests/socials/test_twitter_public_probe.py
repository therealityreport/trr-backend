import json

import pytest

from trr_backend.socials.twitter import public_mirror
from trr_backend.socials.twitter.public_probe import probe_public_conversation


def test_deployed_probe_uses_anonymous_collector(monkeypatch):
    post = {"id": "100", "text": "Root", "author": {"screen_name": "NASA"}}
    reply = {**post, "id": "101", "text": "Reply", "replying_to": {"status": "100"}}
    monkeypatch.setattr(
        public_mirror,
        "_fetch_page",
        lambda _, **kwargs: json.dumps(
            {"code": 200, "status": post, "thread": [], "replies": [reply], "cursor": {}}
        ).encode(),
    )
    result = probe_public_conversation("100")
    assert result["success"] is True
    assert result["authentication_loader_calls"] == 0
    assert result["replies"][0]["reply_to_tweet_id"] == "100"
    assert result["complete"] is False
    assert all(len(value) == 64 for value in result["source_sha256"].values())


@pytest.mark.parametrize("kwargs", [{"max_pages": 3}, {"max_items": 41}, {"max_pages": True}])
def test_probe_rejects_unbounded_work(kwargs):
    with pytest.raises(ValueError):
        probe_public_conversation("100", **kwargs)


def test_probe_reports_blocked_access(monkeypatch):
    def blocked(_, **kwargs):
        raise ValueError("Mirror HTTP 403")

    monkeypatch.setattr(public_mirror, "_fetch_page", blocked)
    result = probe_public_conversation("100")
    assert result["success"] is False
    assert result["error"] == "Mirror HTTP 403"
    assert result["authentication_loader_calls"] == 0


def test_search_probe_is_bounded(monkeypatch):
    from trr_backend.socials.twitter import public_search
    from trr_backend.socials.twitter.public_probe import probe_public_search

    calls = []

    def search(query, **kwargs):
        calls.append((query, kwargs))
        return {"status": "partial", "posts": [], "complete": False}

    monkeypatch.setattr(public_search, "search_public_posts", search)
    result = probe_public_search("Artemis")
    assert calls == [("Artemis", {"provider": "lurkbird", "max_pages": 1, "max_items": 10})]
    assert result["state_free"] is True
    assert len(result["source_sha256"]) == 64


def test_ui_dispatch_probe_never_loads_auth_or_persists(monkeypatch):
    from pathlib import Path

    from trr_backend.socials.twitter import public_search
    from trr_backend.socials.twitter.public_probe import probe_public_research_search

    fixture = Path(__file__).parents[1] / "fixtures/twitter/lurkbird-search.html"
    monkeypatch.setattr(public_search, "_fetch", lambda _, **kwargs: fixture.read_bytes())
    result = probe_public_research_search("Artemis")
    assert result["tweets_found"] == 2
    assert result["forbidden_calls"] == {"auth": 0, "persist": 0}
    assert result["tweets"][0]["likes"] is None
    assert result["complete"] is False
    assert len(result["source_sha256"]) == 3
