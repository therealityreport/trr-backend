from pathlib import Path

import pytest

from trr_backend.socials.twitter import public_search

FIXTURE = Path(__file__).parents[1] / "fixtures/twitter/lurkbird-search.html"
URL = "https://lurkbird.com/search/Artemis/?product=latest"


def test_recorded_search(monkeypatch):
    monkeypatch.setattr(public_search, "_fetch", lambda _: FIXTURE.read_bytes())
    result = public_search.search_public_posts("Artemis", provider="lurkbird", max_items=1)
    assert result["posts"][0]["tweet_id"] == "2117814845315891536"
    assert result["posts"][0]["text"] == "@landusethot ty artemis"
    assert result["posts"][0]["date_time"] == "2026-11-04T03:03:00.000Z"
    assert result["stop_reason"] == "max_items"
    assert result["complete"] is False


def test_pagination_deduplicates_and_stops_repeated_page(monkeypatch):
    calls = []

    def fetch(url):
        calls.append(url)
        return FIXTURE.read_bytes()

    monkeypatch.setattr(public_search, "_fetch", fetch)
    result = public_search.search_public_posts("Artemis", provider="lurkbird", max_pages=2)
    assert len(result["posts"]) == 2
    assert len(calls) == 2 and "cursor=" in calls[1]
    assert result["stop_reason"] == "repeated_page"


def test_new_page_adds_ids(monkeypatch):
    body = FIXTURE.read_bytes()
    pages = iter([body, body.replace(b"2117814845315891536", b"2117814845315891537")])
    monkeypatch.setattr(public_search, "_fetch", lambda _: next(pages))
    result = public_search.search_public_posts("Artemis", provider="lurkbird", max_pages=2)
    assert len(result["posts"]) == 3
    assert result["stop_reason"] == "max_pages"


@pytest.mark.parametrize("body", [b"<html>Login required</html>", b"<html>challenge</html>", b'<ol class="feed"></ol>'])
def test_empty_or_blocked_is_unavailable(monkeypatch, body):
    monkeypatch.setattr(public_search, "_fetch", lambda _: body)
    result = public_search.search_public_posts("Artemis", provider="lurkbird")
    assert result["status"] == "unavailable"
    assert result["complete"] is False


def test_partial_failure_retains_records(monkeypatch):
    count = 0

    def fetch(_):
        nonlocal count
        count += 1
        if count == 2:
            raise ValueError("Public search HTTP 429")
        return FIXTURE.read_bytes()

    monkeypatch.setattr(public_search, "_fetch", fetch)
    result = public_search.search_public_posts("Artemis", provider="lurkbird", max_pages=2)
    assert len(result["posts"]) == 2
    assert result["error"] == "Public search HTTP 429"
    assert count == 2


@pytest.mark.parametrize(
    "options", [{"provider": "other"}, {"max_pages": 3}, {"max_items": 41}, {"sort": "global"}, {"max_pages": True}]
)
def test_explicit_provider_and_bounds(options):
    with pytest.raises(ValueError):
        public_search.search_public_posts("Artemis", **({"provider": "lurkbird"} | options))


def test_external_continuation_rejected():
    body = FIXTURE.read_bytes().replace(b'href="/search/', b'href="https://evil.invalid/search/')
    with pytest.raises(ValueError, match="left the query"):
        public_search._parse(body, url=URL)
