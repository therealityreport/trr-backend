"""Tests for admin show Google News sync and unified news endpoints."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import uuid4

import jwt
import pytest
from fastapi.testclient import TestClient

from api.main import app
from api.routers import admin_show_news


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
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
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
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
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


def test_google_news_sync_bypasses_stale_guard_when_snapshot_missing_images(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    show_id = str(uuid4())
    mock_db = MagicMock()
    fresh_snapshot = {
        "show_id": show_id,
        "source_id": "google_news",
        "variant": "default",
        "status": "success",
        "fetched_at": datetime.now(tz=UTC).isoformat(),
        "payload": {
            "normalized": {
                "news": [
                    {
                        "headline": "Old item",
                        "article_url": "https://example.com/old-item",
                        "image_url": "https://images.example.com/old-item.jpg",
                        "published_at": "2026-02-15T10:00:00Z",
                    }
                ]
            }
        },
    }

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_news._show_exists", return_value=True):
            with patch(
                "api.routers.admin_show_news._resolve_google_news_link",
                return_value={
                    "url": "https://news.google.com/topics/topic-1?ceid=US:en&oc=3",
                    "status": "approved",
                },
            ):
                with patch("api.routers.admin_show_news._fetch_show_snapshot", return_value=fresh_snapshot):
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
                                                    "headline": "Fresh item",
                                                    "article_url": "https://example.com/fresh",
                                                    "image_url": "https://images.example.com/fresh.jpg",
                                                    "published_at": "2026-02-16T10:00:00Z",
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
                                    ) as fetch_mock:
                                        with patch(
                                            "api.routers.admin_show_news._sync_google_news_featured_images",
                                            return_value={
                                                "attempted": 1,
                                                "imported": 1,
                                                "skipped": 0,
                                                "mirrored": 1,
                                                "linked_items": 1,
                                                "errors": [],
                                            },
                                        ):
                                            with patch(
                                                "api.routers.admin_show_news._upsert_show_snapshot",
                                                return_value={
                                                    "show_id": show_id,
                                                    "source_id": "google_news",
                                                    "variant": "default",
                                                    "fetched_at": "2026-02-16T10:00:00Z",
                                                    "payload_sha256": "next-sha",
                                                },
                                            ):
                                                response = client.post(
                                                    f"/api/v1/admin/shows/{show_id}/google-news/sync",
                                                    headers={"Authorization": f"Bearer {token}"},
                                                    json={},
                                                )

    assert response.status_code == 200
    payload = response.json()
    assert payload["synced"] is True
    assert payload["stale_guard_skipped"] is False
    assert fetch_mock.called


def test_unified_news_applies_source_topic_and_season_filters(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
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
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
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


def test_google_news_featured_image_sync_imports_to_media_pipeline() -> None:
    show_id = str(uuid4())
    image_url = "https://images.example.com/story-1.jpg"
    hosted_url = "https://cdn.trr.example.com/media/story-1.jpg"
    items = [
        {
            "headline": "RHOSLC headline",
            "article_url": "https://example.com/story-1",
            "image_url": image_url,
        }
    ]
    mock_db = MagicMock()
    admin_user = {"email": "admin@example.com"}

    with patch(
        "api.routers.admin_scrape.import_images",
        return_value=SimpleNamespace(
            imported=1,
            skipped_duplicates=0,
            errors=[],
            assets=[SimpleNamespace(id=str(uuid4()), hosted_url=hosted_url)],
        ),
    ) as import_images_mock:
        sync_result = admin_show_news._sync_google_news_featured_images(
            db=mock_db,
            admin_user=admin_user,
            show_id=show_id,
            items=items,
        )

    assert import_images_mock.called
    assert sync_result["attempted"] == 1
    assert sync_result["mirrored"] == 1
    assert items[0]["original_image_url"] == image_url
    assert items[0]["hosted_image_url"] == hosted_url
    assert items[0]["image_url"] == hosted_url
    assert items[0]["featured_image_synced"] is True


def test_unified_news_rejects_invalid_sources_filter(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    show_id = str(uuid4())
    mock_db = MagicMock()

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_news._show_exists", return_value=True):
            response = client.get(
                f"/api/v1/admin/shows/{show_id}/news?sources=bravo,invalid_source",
                headers={"Authorization": f"Bearer {token}"},
            )

    assert response.status_code == 422
    assert "Invalid sources filter" in str(response.json().get("detail"))


def test_unified_news_dedupes_and_paginates(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
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
                                "headline": "Duplicate story",
                                "article_url": "https://news.google.com/read/abc",
                                "canonical_article_url": "https://www.bravotv.com/story-dup?utm_source=google",
                                "published_at": "2026-02-14T11:00:00Z",
                                "publisher_name": "People",
                                "publisher_domain": "people.com",
                                "person_tags": [],
                                "topic_tags": ["drama"],
                                "season_matches": [],
                                "feed_rank": 0,
                            },
                            {
                                "headline": "Unique google story",
                                "article_url": "https://example.com/google-unique",
                                "published_at": "2026-02-13T11:00:00Z",
                                "publisher_name": "People",
                                "publisher_domain": "people.com",
                                "person_tags": [],
                                "topic_tags": ["drama"],
                                "season_matches": [],
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
                            "headline": "Duplicate story",
                            "article_url": "https://www.bravotv.com/story-dup?utm_source=trr",
                            "published_at": "2026-02-16T10:00:00Z",
                            "person_tags": [],
                        }
                    ],
                ):
                    with patch("api.routers.admin_show_news._load_season_windows", return_value={}):
                        page_one = client.get(
                            f"/api/v1/admin/shows/{show_id}/news?sort=latest&limit=1",
                            headers={"Authorization": f"Bearer {token}"},
                        )
                        assert page_one.status_code == 200
                        first_payload = page_one.json()
                        assert first_payload["count"] == 1
                        assert first_payload["total_count"] == 2
                        assert isinstance(first_payload["next_cursor"], str)

                        page_two = client.get(
                            f"/api/v1/admin/shows/{show_id}/news?sort=latest&limit=1&cursor={first_payload['next_cursor']}",
                            headers={"Authorization": f"Bearer {token}"},
                        )
                        assert page_two.status_code == 200
                        second_payload = page_two.json()
                        assert second_payload["count"] == 1
                        assert second_payload["next_cursor"] is None


def test_unified_news_skips_cast_lookup_when_tags_already_present(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    show_id = str(uuid4())
    mock_db = MagicMock()

    def _snapshot_side_effect(*_args, **kwargs):
        source_id = kwargs.get("source_id")
        if source_id == "google_news":
            return {
                "source_id": "google_news",
                "fetched_at": "2026-02-15T10:00:00Z",
                "payload_sha256": "google-sha",
                "payload": {
                    "normalized": {
                        "news": [
                            {
                                "headline": "Tagged item",
                                "article_url": "https://example.com/google-tagged",
                                "published_at": "2026-02-15T11:30:00Z",
                                "publisher_name": "People",
                                "publisher_domain": "people.com",
                                "person_tags": [{"person_id": str(uuid4()), "person_name": "Jane Doe"}],
                                "topic_tags": ["reunion"],
                                "season_matches": [],
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
                with patch("api.routers.admin_show_news._load_season_windows", return_value={}):
                    with patch("api.routers.admin_show_news._build_show_cast_index") as cast_index_mock:
                        response = client.get(
                            f"/api/v1/admin/shows/{show_id}/news?sources=google_news",
                            headers={"Authorization": f"Bearer {token}"},
                        )

    assert response.status_code == 200
    assert cast_index_mock.called is False


def test_google_news_sync_async_returns_job_id(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
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
                with patch("api.routers.admin_show_news._create_google_news_sync_job", return_value=str(uuid4())):
                    with patch("api.routers.admin_show_news._run_google_news_sync_job"):
                        response = client.post(
                            f"/api/v1/admin/shows/{show_id}/google-news/sync",
                            headers={"Authorization": f"Bearer {token}"},
                            json={"async": True},
                        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["queued"] is True
    assert isinstance(payload["job_id"], str)
    assert payload["status"] == "queued"


def test_google_news_sync_async_remote_mode_does_not_start_in_api(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("TRR_JOB_PLANE_MODE", "remote")
    monkeypatch.setenv("TRR_LONG_JOB_ENFORCE_REMOTE", "1")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
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
                with patch("api.routers.admin_show_news._create_google_news_sync_job", return_value=str(uuid4())):
                    with patch("api.routers.admin_show_news._run_google_news_sync_job") as runner_mock:
                        response = client.post(
                            f"/api/v1/admin/shows/{show_id}/google-news/sync",
                            headers={"Authorization": f"Bearer {token}"},
                            json={"async": True},
                        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["queued"] is True
    assert payload["execution_owner"] == "remote_worker"
    assert payload["execution_mode_canonical"] == "remote"
    assert runner_mock.called is False


def test_google_news_sync_featured_image_marks_terminal_when_source_missing() -> None:
    item = {
        "headline": "No image story",
        "article_url": "https://example.com/story-no-image",
        "mirror_status": "missing_image",
        "mirror_attempt_count": 0,
    }
    result = admin_show_news._sync_google_news_featured_images(
        db=MagicMock(),
        admin_user={"email": "admin@example.com"},
        show_id=str(uuid4()),
        items=[item],
    )

    assert result["attempted"] == 0
    assert result["mirrored"] == 0
    assert item["mirror_status"] == "missing_image_terminal"
    assert item["mirror_attempt_count"] == 1
    assert item["mirror_retry_after"] is None


def test_google_news_sync_featured_image_marks_terminal_when_article_missing() -> None:
    item = {
        "headline": "Missing article",
        "article_url": "",
        "image_url": "https://images.example.com/story.jpg",
        "mirror_status": "pending",
        "mirror_attempt_count": 0,
    }
    result = admin_show_news._sync_google_news_featured_images(
        db=MagicMock(),
        admin_user={"email": "admin@example.com"},
        show_id=str(uuid4()),
        items=[item],
    )

    assert result["attempted"] == 0
    assert result["mirrored"] == 0
    assert item["mirror_status"] == "missing_image_terminal"
    assert item["last_mirror_error"] == "Missing article URL required for mirroring"
    assert item["mirror_retry_after"] is None


def test_google_news_sync_featured_image_calls_heartbeat_periodically() -> None:
    items = [
        {
            "headline": f"Story {index}",
            "article_url": f"https://example.com/story-{index}",
            "image_url": f"https://images.example.com/story-{index}.jpg",
            "mirror_status": "pending",
            "mirror_attempt_count": 0,
        }
        for index in range(1, 7)
    ]
    heartbeat_calls = 0

    def _heartbeat() -> None:
        nonlocal heartbeat_calls
        heartbeat_calls += 1

    asset_counter = 0

    def _fake_import_images(*_args, **_kwargs):
        nonlocal asset_counter
        asset_counter += 1
        return SimpleNamespace(
            imported=1,
            skipped_duplicates=0,
            errors=[],
            assets=[SimpleNamespace(id=uuid4(), hosted_url=f"https://cdn.example.com/story-{asset_counter}.jpg")],
        )

    with patch("api.routers.admin_scrape.import_images", side_effect=_fake_import_images):
        result = admin_show_news._sync_google_news_featured_images(
            db=MagicMock(),
            admin_user={"email": "admin@example.com"},
            show_id=str(uuid4()),
            items=items,
            heartbeat_cb=_heartbeat,
        )

    assert result["attempted"] == 6
    assert heartbeat_calls == 2


def test_snapshot_backfill_ignores_terminal_missing_image_items() -> None:
    snapshot = {
        "status": "success",
        "fetched_at": datetime.now(tz=UTC).isoformat(),
        "payload": {
            "normalized": {
                "news": [
                    {
                        "headline": "Terminal no image",
                        "article_url": "https://example.com/terminal",
                        "mirror_status": "missing_image_terminal",
                        "mirror_attempt_count": 2,
                    }
                ]
            }
        },
    }
    assert admin_show_news._snapshot_needs_google_image_backfill(snapshot) is False


def test_reconcile_stale_google_news_sync_jobs_marks_orphaned_jobs_failed() -> None:
    with patch("api.routers.admin_show_news.pg.execute_returning", return_value=[{"id": str(uuid4())}]) as exec_mock:
        reconciled = admin_show_news._reconcile_stale_google_news_sync_jobs(show_id=str(uuid4()))

    assert len(reconciled) == 1
    assert exec_mock.called


def test_reconcile_stale_google_news_sync_jobs_uses_heartbeat_filter() -> None:
    with patch("api.routers.admin_show_news.pg.execute_returning", return_value=[]) as exec_mock:
        reconciled = admin_show_news._reconcile_stale_google_news_sync_jobs(show_id=str(uuid4()))

    assert reconciled == []
    sql = exec_mock.call_args.args[0]
    assert "COALESCE(heartbeat_at, updated_at, created_at)" in sql


def test_google_sync_stale_timeout_env_parsing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GOOGLE_NEWS_SYNC_STALE_TIMEOUT_MINUTES", "30")
    assert admin_show_news._get_google_sync_stale_timeout_minutes() == 30
    monkeypatch.setenv("GOOGLE_NEWS_SYNC_STALE_TIMEOUT_MINUTES", "invalid")
    assert admin_show_news._get_google_sync_stale_timeout_minutes() == 15
    monkeypatch.setenv("GOOGLE_NEWS_SYNC_STALE_TIMEOUT_MINUTES", "0")
    assert admin_show_news._get_google_sync_stale_timeout_minutes() == 1


def test_infer_topic_tags_uses_word_boundaries() -> None:
    tags = admin_show_news._infer_topic_tags("This podcast recap is live")
    assert "casting" not in tags


def test_build_show_cast_index_only_adds_unique_first_name_aliases() -> None:
    show_id = str(uuid4())
    with patch(
        "api.routers.admin_show_news.pg.fetch_all",
        side_effect=[
            [
                {"person_id": "person-1", "person_name": "Jen Shah"},
                {"person_id": "person-2", "person_name": "Jen Lee"},
                {"person_id": "person-3", "person_name": "Meredith Marks"},
            ]
        ],
    ):
        cast_index = admin_show_news._build_show_cast_index(show_id)

    jen_aliases = next(ref["name_aliases"] for ref in cast_index if ref["person_id"] == "person-1")
    second_jen_aliases = next(ref["name_aliases"] for ref in cast_index if ref["person_id"] == "person-2")
    meredith_aliases = next(ref["name_aliases"] for ref in cast_index if ref["person_id"] == "person-3")
    assert "jen" not in jen_aliases
    assert "jen" not in second_jen_aliases
    assert "meredith" in meredith_aliases


def test_unified_news_response_includes_facets(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    show_id = str(uuid4())
    mock_db = MagicMock()
    person_id = str(uuid4())

    def _snapshot_side_effect(*_args, **kwargs):
        if kwargs.get("source_id") == "google_news":
            return {
                "source_id": "google_news",
                "fetched_at": "2026-02-15T10:00:00Z",
                "payload_sha256": "google-sha",
                "payload": {
                    "normalized": {
                        "news": [
                            {
                                "headline": "Season 4 reunion recap",
                                "article_url": "https://example.com/story-facet",
                                "published_at": "2026-02-15T11:30:00Z",
                                "publisher_name": "People",
                                "publisher_domain": "people.com",
                                "person_tags": [{"person_id": person_id, "person_name": "Jane Doe"}],
                                "topic_tags": ["reunion"],
                                "season_matches": [{"season_number": 4, "match_types": ["mention"]}],
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
                with patch("api.routers.admin_show_news._load_season_windows", return_value={}):
                    response = client.get(
                        f"/api/v1/admin/shows/{show_id}/news?sources=google_news",
                        headers={"Authorization": f"Bearer {token}"},
                    )

    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload.get("facets"), dict)
    assert payload["facets"]["sources"][0]["token"] == "people.com"
    assert payload["facets"]["people"][0]["person_id"] == person_id
    assert payload["facets"]["topics"][0]["topic"] == "reunion"
    assert payload["facets"]["seasons"][0]["season_number"] == 4
