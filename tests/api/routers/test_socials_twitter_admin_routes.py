"""Tests for Twitter admin social routes."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import jwt
import pytest
from fastapi.testclient import TestClient

from api.main import app
from trr_backend.socials.twitter.scraper import Tweet


def _make_admin_token(secret: str, subject: str = "admin-twitter") -> str:
    now = datetime.now(tz=UTC)
    payload = {
        "sub": subject,
        "iat": int(now.timestamp()),
        "nbf": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=5)).timestamp()),
        "role": "service_role",
    }
    return jwt.encode(payload, secret, algorithm="HS256")


def _build_tweet(
    tweet_id: str,
    *,
    is_reply: bool = False,
    is_quote: bool = False,
    quoted_tweet_id: str | None = None,
) -> Tweet:
    return Tweet(
        tweet_id=tweet_id,
        date_time="2026-02-13 12:34:56",
        created_at=1739450096,
        text="tweet body",
        hashtags=["rhoslc"],
        mentions=["bravo"],
        likes=12,
        retweets=1,
        replies=2,
        quotes=3,
        views=100,
        url=f"https://x.com/viewer/status/{tweet_id}",
        username="viewer",
        display_name="Viewer",
        user_verified=False,
        is_reply=is_reply,
        is_retweet=False,
        is_quote=is_quote,
        reply_to_tweet_id="root-1" if is_reply else None,
        quoted_tweet_id=quoted_tweet_id,
        media_urls=["https://video.twimg.com/media/video-1.mp4"],
    )


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def test_search_twitter_includes_hosted_media_field_without_mirroring(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    tweet = _build_tweet("tweet-1")

    from trr_backend.socials.twitter import TwitterScraper

    monkeypatch.setattr("trr_backend.repositories.social_season_analytics._load_twitter_auth", lambda: ({}, None))
    monkeypatch.setattr(
        "trr_backend.repositories.social_season_analytics._load_twikit_credentials",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(TwitterScraper, "scrape", lambda self, config: [tweet])  # noqa: ARG005

    def _unexpected_mirror(*_args, **_kwargs):
        raise RuntimeError("should not mirror")

    monkeypatch.setattr("trr_backend.socials.twitter.mirror_tweet_media", _unexpected_mirror)

    response = client.post(
        "/api/v1/admin/socials/twitter/search",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "query": "RHOSLC",
            "date_start": "2026-02-01T00:00:00Z",
            "date_end": "2026-02-10T00:00:00Z",
            "mirror_to_s3": False,
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["tweets"][0]["tweet_id"] == "tweet-1"
    assert body["tweets"][0]["hosted_media_urls"] == []


def test_search_twitter_runs_mirror_when_requested(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    tweet = _build_tweet("tweet-2")
    mirror_calls = {"count": 0}

    from trr_backend.socials.twitter import TwitterScraper

    monkeypatch.setattr("trr_backend.repositories.social_season_analytics._load_twitter_auth", lambda: ({}, None))
    monkeypatch.setattr(
        "trr_backend.repositories.social_season_analytics._load_twikit_credentials",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(TwitterScraper, "scrape", lambda self, config: [tweet])  # noqa: ARG005

    def _fake_mirror(tweets: list[Tweet]) -> dict[str, list[str]]:
        mirror_calls["count"] += 1
        tweets[0].hosted_media_urls = ["https://cdn.example.com/media/video-2.mp4"]
        return {tweets[0].tweet_id: tweets[0].hosted_media_urls}

    monkeypatch.setattr("trr_backend.socials.twitter.mirror_tweet_media", _fake_mirror)

    response = client.post(
        "/api/v1/admin/socials/twitter/search",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "query": "RHOSLC",
            "date_start": "2026-02-01T00:00:00Z",
            "date_end": "2026-02-10T00:00:00Z",
            "mirror_to_s3": True,
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert mirror_calls["count"] == 1
    assert body["tweets"][0]["hosted_media_urls"] == ["https://cdn.example.com/media/video-2.mp4"]


def test_twitter_replies_returns_full_tweet_schema_and_mirroring(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    reply = _build_tweet("reply-1", is_reply=True)
    mirror_calls = {"count": 0}

    from trr_backend.socials.twitter import TwitterScraper

    monkeypatch.setattr("trr_backend.repositories.social_season_analytics._load_twitter_auth", lambda: ({}, None))
    monkeypatch.setattr(
        "trr_backend.repositories.social_season_analytics._load_twikit_credentials",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(TwitterScraper, "fetch_tweet_replies", lambda self, tweet_id, delay: [reply])  # noqa: ARG005

    def _fake_mirror(tweets: list[Tweet]) -> dict[str, list[str]]:
        mirror_calls["count"] += 1
        tweets[0].hosted_media_urls = ["https://cdn.example.com/media/reply-1.mp4"]
        return {tweets[0].tweet_id: tweets[0].hosted_media_urls}

    monkeypatch.setattr("trr_backend.socials.twitter.mirror_tweet_media", _fake_mirror)

    response = client.post(
        "/api/v1/admin/socials/twitter/replies",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "tweet_id": "root-123",
            "delay_seconds": 0.5,
            "mirror_to_s3": True,
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["replies_found"] == 1
    assert mirror_calls["count"] == 1
    reply_payload = body["replies"][0]
    assert reply_payload["tweet_id"] == "reply-1"
    assert reply_payload["text"] == "tweet body"
    assert reply_payload["hashtags"] == ["rhoslc"]
    assert reply_payload["media_urls"] == ["https://video.twimg.com/media/video-1.mp4"]
    assert reply_payload["hosted_media_urls"] == ["https://cdn.example.com/media/reply-1.mp4"]


def test_twitter_replies_skips_mirroring_when_flag_disabled(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    reply = _build_tweet("reply-2", is_reply=True)

    from trr_backend.socials.twitter import TwitterScraper

    monkeypatch.setattr("trr_backend.repositories.social_season_analytics._load_twitter_auth", lambda: ({}, None))
    monkeypatch.setattr(
        "trr_backend.repositories.social_season_analytics._load_twikit_credentials",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(TwitterScraper, "fetch_tweet_replies", lambda self, tweet_id, delay: [reply])  # noqa: ARG005

    def _unexpected_mirror(*_args, **_kwargs):
        raise RuntimeError("should not mirror")

    monkeypatch.setattr("trr_backend.socials.twitter.mirror_tweet_media", _unexpected_mirror)

    response = client.post(
        "/api/v1/admin/socials/twitter/replies",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "tweet_id": "root-123",
            "delay_seconds": 0.5,
            "mirror_to_s3": False,
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["replies_found"] == 1
    assert body["replies"][0]["hosted_media_urls"] == []


def test_twitter_replies_passes_search_and_twikit_page_budgets(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    reply = _build_tweet("reply-budget", is_reply=True)
    captured: dict[str, object] = {}

    from trr_backend.socials.twitter import TwitterScraper

    monkeypatch.setattr("trr_backend.repositories.social_season_analytics._load_twitter_auth", lambda: ({}, None))
    monkeypatch.setattr(
        "trr_backend.repositories.social_season_analytics._load_twikit_credentials",
        lambda *_args, **_kwargs: {},
    )

    def _fake_fetch_replies(
        self,
        tweet_id: str,
        delay: float,
        *,
        search_max_pages: int = 8,
        twikit_max_pages: int = 5,
    ) -> list[Tweet]:
        del self
        captured["tweet_id"] = tweet_id
        captured["delay"] = delay
        captured["search_max_pages"] = search_max_pages
        captured["twikit_max_pages"] = twikit_max_pages
        return [reply]

    monkeypatch.setattr(TwitterScraper, "fetch_tweet_replies", _fake_fetch_replies)

    response = client.post(
        "/api/v1/admin/socials/twitter/replies",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "tweet_id": "root-budget",
            "delay_seconds": 0.75,
            "search_max_pages": 19,
            "twikit_max_pages": 11,
            "mirror_to_s3": False,
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["replies_found"] == 1
    assert captured == {
        "tweet_id": "root-budget",
        "delay": 0.75,
        "search_max_pages": 19,
        "twikit_max_pages": 11,
    }


def test_twitter_quotes_returns_full_tweet_schema_and_diagnostics(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    quote = _build_tweet("quote-1", is_quote=True, quoted_tweet_id="root-123")
    mirror_calls = {"count": 0}

    from trr_backend.socials.twitter import TwitterScraper

    monkeypatch.setattr("trr_backend.repositories.social_season_analytics._load_twitter_auth", lambda: ({}, None))
    monkeypatch.setattr(
        "trr_backend.repositories.social_season_analytics._load_twikit_credentials",
        lambda *_args, **_kwargs: {},
    )

    def _fake_fetch_quotes(self, tweet_id: str, delay: float, max_pages: int) -> list[Tweet]:  # noqa: ARG001
        self.last_quote_fetch_meta = {"source_used": "twikit", "failure_reason": None, "attempts": []}
        self.last_quote_fetch_reason = None
        return [quote]

    monkeypatch.setattr(TwitterScraper, "fetch_tweet_quotes", _fake_fetch_quotes)

    def _fake_mirror(tweets: list[Tweet]) -> dict[str, list[str]]:
        mirror_calls["count"] += 1
        tweets[0].hosted_media_urls = ["https://cdn.example.com/media/quote-1.mp4"]
        return {tweets[0].tweet_id: tweets[0].hosted_media_urls}

    monkeypatch.setattr("trr_backend.socials.twitter.mirror_tweet_media", _fake_mirror)

    response = client.post(
        "/api/v1/admin/socials/twitter/quotes",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "tweet_id": "root-123",
            "delay_seconds": 0.5,
            "max_pages": 3,
            "mirror_to_s3": True,
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["quotes_found"] == 1
    assert body["source_used"] == "twikit"
    assert body["failure_reason"] is None
    assert mirror_calls["count"] == 1
    quote_payload = body["quotes"][0]
    assert quote_payload["tweet_id"] == "quote-1"
    assert quote_payload["is_quote"] is True
    assert quote_payload["hosted_media_urls"] == ["https://cdn.example.com/media/quote-1.mp4"]


def test_twitter_quotes_skips_mirroring_when_flag_disabled(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    quote = _build_tweet("quote-2", is_quote=True, quoted_tweet_id="root-123")

    from trr_backend.socials.twitter import TwitterScraper

    monkeypatch.setattr("trr_backend.repositories.social_season_analytics._load_twitter_auth", lambda: ({}, None))
    monkeypatch.setattr(
        "trr_backend.repositories.social_season_analytics._load_twikit_credentials",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(TwitterScraper, "fetch_tweet_quotes", lambda self, tweet_id, delay, max_pages: [quote])  # noqa: ARG005

    def _unexpected_mirror(*_args, **_kwargs):
        raise RuntimeError("should not mirror")

    monkeypatch.setattr("trr_backend.socials.twitter.mirror_tweet_media", _unexpected_mirror)

    response = client.post(
        "/api/v1/admin/socials/twitter/quotes",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "tweet_id": "root-123",
            "delay_seconds": 0.5,
            "max_pages": 3,
            "mirror_to_s3": False,
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["quotes_found"] == 1
    assert body["quotes"][0]["hosted_media_urls"] == []


def test_twitter_quotes_default_max_pages_is_60(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    quote = _build_tweet("quote-default", is_quote=True, quoted_tweet_id="root-456")
    captured: dict[str, object] = {}

    from trr_backend.socials.twitter import TwitterScraper

    monkeypatch.setattr("trr_backend.repositories.social_season_analytics._load_twitter_auth", lambda: ({}, None))
    monkeypatch.setattr(
        "trr_backend.repositories.social_season_analytics._load_twikit_credentials",
        lambda *_args, **_kwargs: {},
    )

    def _fake_fetch_quotes(self, tweet_id: str, delay: float, max_pages: int) -> list[Tweet]:
        del self
        captured["tweet_id"] = tweet_id
        captured["delay"] = delay
        captured["max_pages"] = max_pages
        return [quote]

    monkeypatch.setattr(TwitterScraper, "fetch_tweet_quotes", _fake_fetch_quotes)

    response = client.post(
        "/api/v1/admin/socials/twitter/quotes",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "tweet_id": "root-456",
            "delay_seconds": 0.5,
            "mirror_to_s3": False,
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["quotes_found"] == 1
    assert captured["max_pages"] == 60
