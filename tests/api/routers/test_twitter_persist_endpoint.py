# tests/api/routers/test_twitter_persist_endpoint.py
"""
Tests for the persist=True path on POST /api/v1/admin/socials/twitter/search.
Follows the same pattern as test_socials_twitter_admin_routes.py.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

import jwt
import pytest
from fastapi.testclient import TestClient

import api.routers.socials as socials_router
from api.main import app
from trr_backend.socials.twitter.scraper import Tweet, TwitterScraper


def _make_admin_token(secret: str = "test-secret-32-bytes-minimum-abcdef") -> str:
    now = datetime.now(tz=UTC)
    payload = {
        "sub": "admin-twitter-persist",
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=5)).timestamp()),
        "role": "service_role",
    }
    return jwt.encode(payload, secret, algorithm="HS256")


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def _make_tweet(tweet_id: str = "t99") -> Tweet:
    return Tweet(
        tweet_id=tweet_id, date_time="2026-01-05 20:00:00", created_at=1736114400,
        text="hello world", hashtags=["RHOSLC"], mentions=[],
        likes=5, retweets=1, replies=0, quotes=0, views=100,
        url=f"https://x.com/u/status/{tweet_id}", username="user",
        display_name="User", user_verified=False,
        is_reply=False, is_retweet=False, is_quote=False,
    )


_SECRET = "test-secret-32-bytes-minimum-abcdef"


def test_persist_true_calls_upsert(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """When persist=True, upsert_standalone_tweets is called with the scraped tweets."""
    monkeypatch.setenv("SUPABASE_JWT_SECRET", _SECRET)
    upsert_calls: list[dict] = []

    monkeypatch.setattr("trr_backend.repositories.social_season_analytics._load_twitter_auth", lambda: ({}, None))
    monkeypatch.setattr(
        "trr_backend.repositories.social_season_analytics._load_twikit_credentials",
        lambda *_a, **_kw: {},
    )
    monkeypatch.setattr(TwitterScraper, "scrape", lambda self, config: [_make_tweet()])
    monkeypatch.setattr(
        socials_router,
        "upsert_standalone_tweets",
        lambda tweets, *, scrape_query: upsert_calls.append({"tweets": tweets, "scrape_query": scrape_query}) or [],
    )

    resp = client.post(
        "/api/v1/admin/socials/twitter/search",
        headers={"Authorization": f"Bearer {_make_admin_token()}"},
        json={
            "query": "#RHOSLC",
            "date_start": "2026-01-01T00:00:00",
            "date_end": "2026-01-11T00:00:00",
            "persist": True,
        },
    )
    assert resp.status_code == 200
    assert resp.json()["success"] is True
    assert len(upsert_calls) == 1
    assert upsert_calls[0]["scrape_query"] == "#RHOSLC"


def test_persist_true_uses_explicit_scrape_query(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """When scrape_query is provided, it is used instead of query."""
    monkeypatch.setenv("SUPABASE_JWT_SECRET", _SECRET)
    upsert_calls: list[dict] = []

    monkeypatch.setattr("trr_backend.repositories.social_season_analytics._load_twitter_auth", lambda: ({}, None))
    monkeypatch.setattr(
        "trr_backend.repositories.social_season_analytics._load_twikit_credentials",
        lambda *_a, **_kw: {},
    )
    monkeypatch.setattr(TwitterScraper, "scrape", lambda self, config: [_make_tweet()])
    monkeypatch.setattr(
        socials_router,
        "upsert_standalone_tweets",
        lambda tweets, *, scrape_query: upsert_calls.append({"scrape_query": scrape_query}) or [],
    )

    client.post(
        "/api/v1/admin/socials/twitter/search",
        headers={"Authorization": f"Bearer {_make_admin_token()}"},
        json={
            "query": "#RHOSLC",
            "date_start": "2026-01-01T00:00:00",
            "date_end": "2026-01-11T00:00:00",
            "persist": True,
            "scrape_query": "RHOSLC-S4-premiere",
        },
    )
    assert upsert_calls[0]["scrape_query"] == "RHOSLC-S4-premiere"


def test_persist_defaults_scrape_query_to_query_value(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """When persist=True and scrape_query is omitted, the query value is used as the label."""
    monkeypatch.setenv("SUPABASE_JWT_SECRET", _SECRET)
    upsert_calls: list[dict] = []

    monkeypatch.setattr("trr_backend.repositories.social_season_analytics._load_twitter_auth", lambda: ({}, None))
    monkeypatch.setattr(
        "trr_backend.repositories.social_season_analytics._load_twikit_credentials",
        lambda *_a, **_kw: {},
    )
    monkeypatch.setattr(TwitterScraper, "scrape", lambda self, config: [_make_tweet()])
    monkeypatch.setattr(
        socials_router,
        "upsert_standalone_tweets",
        lambda tweets, *, scrape_query: upsert_calls.append({"scrape_query": scrape_query}) or [],
    )

    client.post(
        "/api/v1/admin/socials/twitter/search",
        headers={"Authorization": f"Bearer {_make_admin_token()}"},
        json={
            "query": "@BravoTV",
            "date_start": "2026-01-01T00:00:00",
            "date_end": "2026-01-11T00:00:00",
            "persist": True,
            # scrape_query intentionally omitted
        },
    )
    assert upsert_calls[0]["scrape_query"] == "@BravoTV"


def test_persist_false_does_not_call_upsert(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """When persist is not set, upsert is never called."""
    monkeypatch.setenv("SUPABASE_JWT_SECRET", _SECRET)
    upsert_calls: list = []

    monkeypatch.setattr("trr_backend.repositories.social_season_analytics._load_twitter_auth", lambda: ({}, None))
    monkeypatch.setattr(
        "trr_backend.repositories.social_season_analytics._load_twikit_credentials",
        lambda *_a, **_kw: {},
    )
    monkeypatch.setattr(TwitterScraper, "scrape", lambda self, config: [_make_tweet()])
    monkeypatch.setattr(
        socials_router,
        "upsert_standalone_tweets",
        lambda *a, **kw: upsert_calls.append(1) or [],
    )

    client.post(
        "/api/v1/admin/socials/twitter/search",
        headers={"Authorization": f"Bearer {_make_admin_token()}"},
        json={
            "query": "#RHOSLC",
            "date_start": "2026-01-01T00:00:00",
            "date_end": "2026-01-11T00:00:00",
        },
    )
    assert upsert_calls == []
