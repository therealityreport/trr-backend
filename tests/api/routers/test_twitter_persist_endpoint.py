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
        "nbf": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=5)).timestamp()),
        "role": "service_role",
    }
    return jwt.encode(payload, secret, algorithm="HS256")


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def _make_tweet(tweet_id: str = "t99") -> Tweet:
    return Tweet(
        tweet_id=tweet_id,
        date_time="2026-01-05 20:00:00",
        created_at=1736114400,
        text="hello world",
        hashtags=["RHOSLC"],
        mentions=[],
        likes=5,
        retweets=1,
        replies=0,
        quotes=0,
        views=100,
        url=f"https://x.com/u/status/{tweet_id}",
        username="user",
        display_name="User",
        user_verified=False,
        is_reply=False,
        is_retweet=False,
        is_quote=False,
    )


_SECRET = "test-secret-32-bytes-minimum-abcdef"


def _stub_scrape_with_meta(tweets: list[Tweet], meta: dict | None = None):
    def _scrape(self, config):
        self.last_retrieval_meta = dict(
            meta or {"complete": True, "posts_checked": len(tweets), "stop_reason": "no_cursor"}
        )
        return list(tweets)

    return _scrape


def test_persist_true_calls_upsert(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """When persist=True, scrape provenance persistence is called with the scraped tweets."""
    monkeypatch.setenv("SUPABASE_JWT_SECRET", _SECRET)
    persist_calls: list[dict] = []

    monkeypatch.setattr("trr_backend.repositories.social_season_analytics._load_twitter_auth", lambda: ({}, None))
    monkeypatch.setattr(
        "trr_backend.repositories.social_season_analytics._load_twikit_credentials",
        lambda *_a, **_kw: {},
    )
    monkeypatch.setattr(
        TwitterScraper,
        "scrape",
        _stub_scrape_with_meta([_make_tweet()], {"complete": True, "posts_checked": 7}),
    )
    monkeypatch.setattr(
        socials_router,
        "persist_standalone_twitter_search",
        lambda tweets, **kwargs: (
            persist_calls.append({"tweets": tweets, **kwargs})
            or {
                "requested": True,
                "succeeded": True,
                "scrape_query_label": kwargs["scrape_query_label"],
                "scrape_run_id": "run-1",
                "tweets_upserted": len(tweets),
                "tweet_memberships_created": len(tweets),
                "tweet_memberships_total": len(tweets),
                "requested_via": kwargs["requested_via"],
                "error": None,
            }
        ),
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
    assert len(persist_calls) == 1
    assert persist_calls[0]["scrape_query_label"] == "#RHOSLC"
    assert len(persist_calls[0]["tweets"]) == 1
    assert resp.json()["complete"] is True
    assert resp.json()["persist_summary"]["scrape_run_id"] == "run-1"
    assert resp.json()["scrape_run_id"] == "run-1"
    assert resp.json()["retrieval_meta"]["posts_checked"] == 7


def test_persist_true_uses_explicit_scrape_query(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """When scrape_query is provided, it is used instead of query."""
    monkeypatch.setenv("SUPABASE_JWT_SECRET", _SECRET)
    persist_calls: list[dict] = []

    monkeypatch.setattr("trr_backend.repositories.social_season_analytics._load_twitter_auth", lambda: ({}, None))
    monkeypatch.setattr(
        "trr_backend.repositories.social_season_analytics._load_twikit_credentials",
        lambda *_a, **_kw: {},
    )
    monkeypatch.setattr(TwitterScraper, "scrape", _stub_scrape_with_meta([_make_tweet()]))
    monkeypatch.setattr(
        socials_router,
        "persist_standalone_twitter_search",
        lambda tweets, **kwargs: (
            persist_calls.append(kwargs)
            or {
                "requested": True,
                "succeeded": True,
                "scrape_query_label": kwargs["scrape_query_label"],
                "scrape_run_id": "run-explicit",
                "tweets_upserted": len(tweets),
                "tweet_memberships_created": len(tweets),
                "tweet_memberships_total": len(tweets),
                "requested_via": kwargs["requested_via"],
                "error": None,
            }
        ),
    )

    resp = client.post(
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
    assert resp.status_code == 200
    assert persist_calls[0]["scrape_query_label"] == "RHOSLC-S4-premiere"
    assert resp.json()["persist_summary"]["scrape_query_label"] == "RHOSLC-S4-premiere"


def test_persist_defaults_scrape_query_to_query_value(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """When persist=True and scrape_query is omitted, the query value is used as the label."""
    monkeypatch.setenv("SUPABASE_JWT_SECRET", _SECRET)
    persist_calls: list[dict] = []

    monkeypatch.setattr("trr_backend.repositories.social_season_analytics._load_twitter_auth", lambda: ({}, None))
    monkeypatch.setattr(
        "trr_backend.repositories.social_season_analytics._load_twikit_credentials",
        lambda *_a, **_kw: {},
    )
    monkeypatch.setattr(TwitterScraper, "scrape", _stub_scrape_with_meta([_make_tweet()]))
    monkeypatch.setattr(
        socials_router,
        "persist_standalone_twitter_search",
        lambda tweets, **kwargs: (
            persist_calls.append(kwargs)
            or {
                "requested": True,
                "succeeded": True,
                "scrape_query_label": kwargs["scrape_query_label"],
                "scrape_run_id": "run-default",
                "tweets_upserted": len(tweets),
                "tweet_memberships_created": len(tweets),
                "tweet_memberships_total": len(tweets),
                "requested_via": kwargs["requested_via"],
                "error": None,
            }
        ),
    )

    resp = client.post(
        "/api/v1/admin/socials/twitter/search",
        headers={"Authorization": f"Bearer {_make_admin_token()}"},
        json={
            "query": "@BravoTV",
            "date_start": "2026-01-01T00:00:00",
            "date_end": "2026-01-11T00:00:00",
            "persist": True,
        },
    )
    assert resp.status_code == 200
    assert persist_calls[0]["scrape_query_label"] == "@BravoTV"


def test_persist_false_does_not_call_upsert(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """When persist is not set, upsert is never called."""
    monkeypatch.setenv("SUPABASE_JWT_SECRET", _SECRET)
    persist_calls: list = []

    monkeypatch.setattr("trr_backend.repositories.social_season_analytics._load_twitter_auth", lambda: ({}, None))
    monkeypatch.setattr(
        "trr_backend.repositories.social_season_analytics._load_twikit_credentials",
        lambda *_a, **_kw: {},
    )
    monkeypatch.setattr(TwitterScraper, "scrape", _stub_scrape_with_meta([_make_tweet()]))
    monkeypatch.setattr(
        socials_router,
        "persist_standalone_twitter_search",
        lambda *a, **kw: persist_calls.append(1) or {},
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
    assert persist_calls == []


def test_persist_true_still_records_empty_results(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", _SECRET)
    persist_calls: list[dict] = []

    monkeypatch.setattr("trr_backend.repositories.social_season_analytics._load_twitter_auth", lambda: ({}, None))
    monkeypatch.setattr(
        "trr_backend.repositories.social_season_analytics._load_twikit_credentials",
        lambda *_a, **_kw: {},
    )
    monkeypatch.setattr(TwitterScraper, "scrape", _stub_scrape_with_meta([], {"complete": True, "posts_checked": 0}))
    monkeypatch.setattr(
        socials_router,
        "persist_standalone_twitter_search",
        lambda tweets, **kwargs: (
            persist_calls.append({"tweets": tweets, **kwargs})
            or {
                "requested": True,
                "succeeded": True,
                "scrape_query_label": kwargs["scrape_query_label"],
                "scrape_run_id": "run-empty",
                "tweets_upserted": 0,
                "tweet_memberships_created": 0,
                "tweet_memberships_total": 0,
                "requested_via": kwargs["requested_via"],
                "error": None,
            }
        ),
    )

    resp = client.post(
        "/api/v1/admin/socials/twitter/search",
        headers={"Authorization": f"Bearer {_make_admin_token()}"},
        json={
            "query": "#RHOSLC",
            "date_start": "2026-01-01T09:15:00",
            "date_end": "2026-01-11T17:45:00",
            "persist": True,
        },
    )

    assert resp.status_code == 200
    assert len(persist_calls) == 1
    assert persist_calls[0]["window_start_day"] == "2026-01-01"
    assert persist_calls[0]["window_end_day_exclusive"] == "2026-01-12"
    assert resp.json()["persist_summary"]["scrape_run_id"] == "run-empty"
    assert resp.json()["complete"] is True


def test_persist_failure_is_isolated_in_summary(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", _SECRET)

    monkeypatch.setattr("trr_backend.repositories.social_season_analytics._load_twitter_auth", lambda: ({}, None))
    monkeypatch.setattr(
        "trr_backend.repositories.social_season_analytics._load_twikit_credentials",
        lambda *_a, **_kw: {},
    )
    monkeypatch.setattr(
        TwitterScraper,
        "scrape",
        _stub_scrape_with_meta(
            [_make_tweet()],
            {"complete": False, "posts_checked": 500, "stop_reason": "max_pages_reached"},
        ),
    )
    monkeypatch.setattr(
        socials_router,
        "persist_standalone_twitter_search",
        lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("db write failed")),
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
    assert resp.json()["complete"] is False
    assert resp.json()["persist_summary"]["succeeded"] is False
    assert resp.json()["persist_summary"]["error"] == "db write failed"
    assert resp.json()["retrieval_meta"]["stop_reason"] == "max_pages_reached"
