"""Protected-route proof with the real public adapter and recorded HTML."""

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import jwt
import pytest
from fastapi.testclient import TestClient

from api.main import app
from trr_backend.socials.twitter import direct_scrape, public_search

ENDPOINT = "/api/v1/admin/socials/twitter/search"
PAYLOAD = {"mode": "anonymous_research", "provider": "lurkbird", "query": "Artemis"}
FIXTURE = Path(__file__).parents[2] / "fixtures/twitter/lurkbird-search.html"


@pytest.fixture
def headers(monkeypatch):
    secret = "fixture-only-twitter-research-secret-123456"
    monkeypatch.setenv("SUPABASE_JWT_SECRET", secret)
    token = jwt.encode(
        {"sub": "fixture-admin", "role": "service_role", "exp": datetime.now(UTC) + timedelta(minutes=5)},
        secret,
        algorithm="HS256",
    )
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def forbid_side_effects(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("Anonymous search must not load X auth, persist, or upload media")

    monkeypatch.setattr("trr_backend.socials.control_plane.runtime._load_twitter_auth", forbidden)
    monkeypatch.setattr("trr_backend.socials.control_plane.runtime._load_twikit_credentials", forbidden)
    monkeypatch.setattr("api.routers.socials.platform_scrape.persist_standalone_twitter_search", forbidden)
    monkeypatch.setattr("trr_backend.socials.twitter.scraper.mirror_tweet_media", forbidden)


def test_anonymous_route_uses_adapter_off_event_loop_without_auth_or_writes(headers, forbid_side_effects, monkeypatch):
    calls = []

    def fetch(url, **kwargs):
        with pytest.raises(RuntimeError, match="no running event loop"):
            asyncio.get_running_loop()
        calls.append(url)
        return FIXTURE.read_bytes()

    monkeypatch.setattr(public_search, "_fetch", fetch)
    response = TestClient(app).post(ENDPOINT, headers=headers, json=PAYLOAD)
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["tweets_found"] == 2
    assert body["tweets"][0]["tweet_id"] == "2117814845315891536"
    for key in (
        "likes",
        "retweets",
        "replies",
        "quotes",
        "views",
        "shares",
        "bookmarks",
        "user_verified",
        "is_reply",
        "is_retweet",
        "is_quote",
    ):
        assert body["tweets"][0][key] is None
    assert body["complete"] is False
    assert body["persist_summary"] is None
    assert body["scrape_run_id"] is None
    meta = body["retrieval_meta"]
    assert meta["provider"] == "lurkbird" and meta["status"] == "partial"
    assert (meta["max_pages"], meta["max_items"]) == (1, 20)
    assert meta["authentication_provided"] is False
    assert "unknown" in meta["coverage"]
    assert len(calls) == 1


def test_later_failure_retains_partial_rows(headers, forbid_side_effects, monkeypatch):
    calls = []

    def fetch(url, **kwargs):
        calls.append(url)
        if len(calls) == 2:
            raise ValueError("Public search HTTP 429")
        return FIXTURE.read_bytes()

    monkeypatch.setattr(public_search, "_fetch", fetch)
    response = TestClient(app).post(ENDPOINT, headers=headers, json=PAYLOAD | {"max_pages": 2, "max_items": 40})
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True and body["tweets_found"] == 2
    assert body["error"] == "Public search HTTP 429"
    assert body["retrieval_meta"]["status"] == "partial"
    assert body["retrieval_meta"]["stop_reason"] == "error"
    assert body["complete"] is False


@pytest.mark.parametrize("html", [b"<html>challenge</html>", b'<ol class="feed"></ol>'])
def test_blocked_or_empty_source_is_unavailable_not_zero_matches(headers, forbid_side_effects, monkeypatch, html):
    monkeypatch.setattr(public_search, "_fetch", lambda _, **kwargs: html)
    response = TestClient(app).post(ENDPOINT, headers=headers, json=PAYLOAD)
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is False and body["tweets"] == []
    assert body["retrieval_meta"]["status"] == "unavailable"
    assert body["complete"] is False


@pytest.mark.parametrize(
    "options",
    [
        {"provider": None},
        {"max_pages": 3},
        {"max_items": 41},
        {"persist": True},
        {"mirror_to_s3": True},
        {"date_start": "2026-01-01T00:00:00Z"},
        {"include_replies": False},
        {"include_links": True},
        {"season_number": 1},
        {"query": " "},
        {"query": "x" * 301},
    ],
)
def test_incompatible_options_rejected_before_provider(headers, monkeypatch, options):
    monkeypatch.setattr(public_search, "_fetch", lambda _, **kwargs: pytest.fail("must not fetch"))
    response = TestClient(app).post(ENDPOINT, headers=headers, json=PAYLOAD | options)
    assert response.status_code == 422


def test_existing_mode_still_requires_dates(headers, monkeypatch):
    monkeypatch.setattr(public_search, "_fetch", lambda _, **kwargs: pytest.fail("must not fetch"))
    assert TestClient(app).post(ENDPOINT, headers=headers, json={"query": "Artemis"}).status_code == 422


def test_anonymous_route_still_requires_admin(monkeypatch):
    monkeypatch.setattr(public_search, "_fetch", lambda _, **kwargs: pytest.fail("must not fetch"))
    assert TestClient(app).post(ENDPOINT, json=PAYLOAD).status_code == 401


def test_dict_direct_call_uses_same_anonymous_path(monkeypatch):
    monkeypatch.setattr(public_search, "_fetch", lambda _, **kwargs: FIXTURE.read_bytes())
    body = direct_scrape.search_twitter(
        PAYLOAD,
        load_auth=lambda: pytest.fail("must not load auth"),
        persist_search=lambda *_args, **_kwargs: pytest.fail("must not persist"),
    )
    assert body["tweets_found"] == 2
    assert body["tweets"][0]["likes"] is None


@pytest.mark.parametrize("option", ["persist", "mirror_to_s3"])
def test_dict_direct_call_cannot_silently_ignore_write_flags(monkeypatch, option):
    monkeypatch.setattr(public_search, "_fetch", lambda _, **kwargs: pytest.fail("must not fetch"))
    with pytest.raises(ValueError, match="does not persist"):
        direct_scrape.search_twitter(
            PAYLOAD | {option: True},
            load_auth=lambda: pytest.fail("must not load auth"),
            persist_search=lambda *_args, **_kwargs: pytest.fail("must not persist"),
        )


def test_fxtwitter_route_preserves_unknown_metrics_and_bypasses_auth(headers, forbid_side_effects, monkeypatch):
    import json

    from trr_backend.socials.twitter import public_collect

    payload = {
        "code": 200,
        "results": [
            {
                "id": "2121297625022365945",
                "text": "#RHOSLC",
                "author": {"screen_name": "BravoTV"},
                "reposts": 7,
                "quotes": 3,
            }
        ],
    }
    monkeypatch.setattr(public_collect, "fetch", lambda *a, **kw: json.dumps(payload).encode())
    response = TestClient(app).post(
        ENDPOINT,
        headers=headers,
        json={"mode": "anonymous_research", "provider": "fxtwitter", "query": "#RHOSLC", "max_pages": 2},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    post = body["tweets"][0]
    assert post["retweets"] == 7 and post["quotes"] == 3
    assert post["likes"] is None and post["shares"] is None and post["date_time"] is None
    assert body["complete"] is False and body["retrieval_meta"]["status"] == "complete"


def test_fxtwitter_date_and_persistence_contract():
    from pydantic import ValidationError

    from api.routers.socials.platform_scrape import TwitterSearchRequest

    request = TwitterSearchRequest.model_validate(
        {
            "mode": "anonymous_research",
            "provider": "fxtwitter",
            "query": "from:BravoTV #RHOSLC",
            "date_start": "2026-11-01T00:00:00Z",
            "date_end": "2026-11-13T00:00:00Z",
        }
    )
    assert request.provider == "fxtwitter"
    assert request.date_start is not None and request.date_start.isoformat() == "2026-11-01T00:00:00+00:00"
    assert request.date_end is not None and request.date_end.isoformat() == "2026-11-13T00:00:00+00:00"
    with pytest.raises(ValidationError):
        TwitterSearchRequest(mode="anonymous_research", provider="fxtwitter", query="#RHOSLC", persist=True)


def test_public_options_cannot_enter_standard_auth_and_unknown_options_rejected():
    from pydantic import ValidationError

    from api.routers.socials.platform_scrape import TwitterSearchRequest

    for fields in ({"capability": "profile"}, {"verify_parents": True}):
        with pytest.raises(ValidationError):
            TwitterSearchRequest.model_validate(
                {
                    "query": "@BravoTV",
                    "date_start": "2026-11-01T00:00:00Z",
                    "date_end": "2026-11-13T00:00:00Z",
                    **fields,
                }
            )
    with pytest.raises(ValidationError):
        TwitterSearchRequest.model_validate(
            {"mode": "anonymous_research", "provider": "fxtwitter", "query": "#RHOSLC", "unknown_option": True}
        )


def test_parent_verification_does_not_continue_after_rate_limit(monkeypatch):
    import json

    from trr_backend.socials.twitter import public_collect, public_verification
    from trr_backend.socials.twitter.anonymous_search import search_anonymous_twitter

    replies = {
        "code": 200,
        "results": [
            {"id": "123", "text": "#RHOSLC", "author": {"screen_name": "BravoTV"}, "replying_to": {"status": "100"}}
        ],
        "cursor": {"bottom": "next"},
    }
    pending = iter([replies, ValueError("Mirror HTTP 429")])

    def fetch(*a, **kw):
        value = next(pending)
        if isinstance(value, Exception):
            raise value
        return json.dumps(value).encode()

    monkeypatch.setattr(public_collect, "fetch", fetch)
    monkeypatch.setattr(
        public_verification, "fetch", lambda *a, **kw: pytest.fail("blocked collection must stop verification")
    )
    r = search_anonymous_twitter(
        {
            "query": "#RHOSLC",
            "mode": "anonymous_research",
            "provider": "fxtwitter",
            "max_pages": 2,
            "verify_parents": True,
        }
    )
    assert r["retrieval_meta"]["parent_checks"][0]["status"] == "unavailable"
