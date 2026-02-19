"""Tests for admin show Google News sync and unified news endpoints."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch
from uuid import uuid4

import jwt
import pytest
from fastapi.testclient import TestClient

from api.main import app


def _make_admin_token(secret: str, subject: str = "admin-1") -> str:
    now = datetime.now(tz=UTC)
    payload = {
        "sub": subject,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=5)).timestamp()),
        "role": "service_role",
    }
    return jwt.encode(payload, secret, algorithm="HS256")


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def test_google_news_sync_returns_409_when_url_not_configured(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")
    show_id = str(uuid4())
    mock_db = MagicMock()

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_news._show_exists", return_value=True):
            with patch("api.routers.admin_show_news._resolve_google_news_link", return_value=None):
                response = client.post(
                    f"/api/v1/admin/shows/{show_id}/google-news/sync",
                    headers={"Authorization": f"Bearer {token}"},
                    json={},
                )

    assert response.status_code == 409
    assert "Google News URL is not configured" in response.json().get("detail", "")


def test_google_news_sync_persists_google_snapshot(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")
    show_id = str(uuid4())
    mock_db = MagicMock()

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_news._show_exists", return_value=True):
            with patch(
                "api.routers.admin_show_news._resolve_google_news_link",
                return_value={
                    "url": "https://news.google.com/topics/topic-1?ceid=US:en&oc=3",
                    "status": "approved",
                },
            ):
                with patch("api.routers.admin_show_news._fetch_show_snapshot", return_value=None):
                    with patch("api.routers.admin_show_news._ensure_google_source"):
                        with patch(
                            "api.routers.admin_show_news._show_name_and_aliases",
                            return_value=("The Real Housewives of Salt Lake City", ["RHOSLC"]),
                        ):
                            with patch("api.routers.admin_show_news._build_show_cast_index", return_value=[]):
                                with patch("api.routers.admin_show_news._load_season_windows", return_value={}):
                                    with patch(
                                        "api.routers.admin_show_news.fetch_google_news",
                                        return_value={
                                            "items": [
                                                {
                                                    "headline": "RHOSLC update",
                                                    "article_url": "https://example.com/story-1",
                                                    "published_at": "2026-02-15T10:00:00Z",
                                                    "publisher_name": "People",
                                                    "publisher_domain": "people.com",
                                                    "feed_rank": 0,
                                                }
                                            ],
                                            "resolved_feed_url": "https://news.google.com/rss/topics/topic-1",
                                            "fallback_used": False,
                                            "attempted_feeds": ["https://news.google.com/rss/topics/topic-1"],
                                            "errors": [],
                                        },
                                    ):
                                        with patch(
                                            "api.routers.admin_show_news._upsert_show_snapshot",
                                            return_value={
                                                "show_id": show_id,
                                                "source_id": "google_news",
                                                "variant": "default",
                                                "fetched_at": "2026-02-15T10:00:00Z",
                                                "payload_sha256": "abc123",
                                            },
                                        ) as upsert_mock:
                                            response = client.post(
                                                f"/api/v1/admin/shows/{show_id}/google-news/sync",
                                                headers={"Authorization": f"Bearer {token}"},
                                                json={"force": True},
                                            )

    assert response.status_code == 200
    payload = response.json()
    assert payload["synced"] is True
    assert payload["count"] == 1
    assert payload["snapshot"]["source_id"] == "google_news"
    assert upsert_mock.call_args.kwargs["source_id"] == "google_news"


def test_unified_news_applies_source_topic_and_season_filters(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")
    show_id = str(uuid4())
    mock_db = MagicMock()

    def _snapshot_side_effect(*_args, **kwargs):
        source_id = kwargs.get("source_id")
        if source_id == "bravo":
            return {
                "source_id": "bravo",
                "fetched_at": "2026-02-14T10:00:00Z",
                "payload_sha256": "bravo-sha",
                "payload": {"normalized": {}},
            }
        if source_id == "google_news":
            return {
                "source_id": "google_news",
                "fetched_at": "2026-02-15T10:00:00Z",
                "payload_sha256": "google-sha",
                "payload": {
                    "normalized": {
                        "news": [
                            {
                                "headline": "Season 5 reunion details",
                                "article_url": "https://example.com/google-story",
                                "published_at": "2026-02-15T11:30:00Z",
                                "publisher_name": "People",
                                "publisher_domain": "people.com",
                                "feed_rank": 0,
                            }
                        ]
                    }
                },
            }
        return None

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_news._show_exists", return_value=True):
            with patch("api.routers.admin_show_news._fetch_show_snapshot", side_effect=_snapshot_side_effect):
                with patch(
                    "api.routers.admin_show_bravo._extract_news_from_snapshot",
                    return_value=[
                        {
                            "headline": "Bravo cast update",
                            "article_url": "https://www.bravotv.com/story-1",
                            "published_at": "2026-02-12T10:00:00Z",
                            "person_tags": [],
                        }
                    ],
                ):
                    with patch(
                        "api.routers.admin_show_news._build_show_cast_index",
                        return_value=[],
                    ):
                        season_window = {
                            5: (
                                datetime(2026, 1, 1, tzinfo=UTC).date(),
                                datetime(2026, 12, 31, tzinfo=UTC).date(),
                            )
                        }
                        with patch(
                            "api.routers.admin_show_news._load_season_windows",
                            return_value=season_window,
                        ):
                            response = client.get(
                                f"/api/v1/admin/shows/{show_id}/news?source=people.com&topic=reunion&season_number=5",
                                headers={"Authorization": f"Bearer {token}"},
                            )

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 1
    assert payload["news"][0]["source_id"] == "google_news"
    assert payload["news"][0]["publisher_domain"] == "people.com"


def test_unified_news_sorting_supports_trending_and_latest(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")
    show_id = str(uuid4())
    mock_db = MagicMock()

    def _snapshot_side_effect(*_args, **kwargs):
        source_id = kwargs.get("source_id")
        if source_id == "bravo":
            return {
                "source_id": "bravo",
                "fetched_at": "2026-02-14T10:00:00Z",
                "payload_sha256": "bravo-sha",
                "payload": {"normalized": {}},
            }
        if source_id == "google_news":
            return {
                "source_id": "google_news",
                "fetched_at": "2026-02-15T10:00:00Z",
                "payload_sha256": "google-sha",
                "payload": {
                    "normalized": {
                        "news": [
                            {
                                "headline": "Google rank 1 older",
                                "article_url": "https://example.com/google-1",
                                "published_at": "2026-02-10T10:00:00Z",
                                "publisher_name": "People",
                                "publisher_domain": "people.com",
                                "feed_rank": 0,
                            },
                            {
                                "headline": "Google rank 2 newer",
                                "article_url": "https://example.com/google-2",
                                "published_at": "2026-02-14T10:00:00Z",
                                "publisher_name": "People",
                                "publisher_domain": "people.com",
                                "feed_rank": 1,
                            },
                        ]
                    }
                },
            }
        return None

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_news._show_exists", return_value=True):
            with patch("api.routers.admin_show_news._fetch_show_snapshot", side_effect=_snapshot_side_effect):
                with patch(
                    "api.routers.admin_show_bravo._extract_news_from_snapshot",
                    return_value=[
                        {
                            "headline": "Newest bravo item",
                            "article_url": "https://www.bravotv.com/newest",
                            "published_at": "2026-02-16T10:00:00Z",
                            "person_tags": [],
                        }
                    ],
                ):
                    with patch("api.routers.admin_show_news._build_show_cast_index", return_value=[]):
                        with patch("api.routers.admin_show_news._load_season_windows", return_value={}):
                            trending_response = client.get(
                                f"/api/v1/admin/shows/{show_id}/news?sort=trending",
                                headers={"Authorization": f"Bearer {token}"},
                            )
                            latest_response = client.get(
                                f"/api/v1/admin/shows/{show_id}/news?sort=latest",
                                headers={"Authorization": f"Bearer {token}"},
                            )

    assert trending_response.status_code == 200
    trending_payload = trending_response.json()
    trending_urls = [item["article_url"] for item in trending_payload["news"]]
    assert trending_urls[:2] == ["https://example.com/google-1", "https://example.com/google-2"]
    assert trending_payload["news"][0]["trending_rank"] == 1

    assert latest_response.status_code == 200
    latest_payload = latest_response.json()
    latest_urls = [item["article_url"] for item in latest_payload["news"]]
    assert latest_urls[0] == "https://www.bravotv.com/newest"
    assert latest_payload["news"][0]["trending_rank"] is None
