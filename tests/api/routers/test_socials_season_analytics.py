"""Tests for season social analytics admin endpoints."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import patch
from urllib.parse import urlencode
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


@pytest.fixture(autouse=True)
def _default_local_job_plane(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRR_JOB_PLANE_MODE", "local")
    monkeypatch.delenv("TRR_LONG_JOB_ENFORCE_REMOTE", raising=False)


def test_get_season_targets(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    expected = {
        "season_id": season_id,
        "show_id": str(uuid4()),
        "season_number": 10,
        "show_name": "Test Show",
        "source_scope": "bravo",
        "targets": [],
        "using_defaults": True,
    }

    with patch("trr_backend.repositories.social_season_analytics.get_targets", return_value=expected):
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/targets",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["season_id"] == season_id
    assert response.json()["using_defaults"] is True


def test_put_season_targets(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    expected = {
        "season_id": season_id,
        "show_id": str(uuid4()),
        "season_number": 10,
        "show_name": "Test Show",
        "source_scope": "bravo",
        "targets": [
            {
                "platform": "instagram",
                "accounts": ["bravotv"],
                "hashtags": ["rhoslc"],
                "keywords": ["Real Housewives"],
                "timezone": "America/New_York",
                "is_active": True,
                "config": {},
            }
        ],
    }

    payload = {
        "source_scope": "bravo",
        "targets": [
            {
                "platform": "instagram",
                "accounts": ["bravotv"],
                "hashtags": ["rhoslc"],
                "keywords": ["Real Housewives"],
            }
        ],
    }

    with patch("trr_backend.repositories.social_season_analytics.put_targets", return_value=expected):
        response = client.put(
            f"/api/v1/admin/socials/seasons/{season_id}/targets",
            headers={"Authorization": f"Bearer {token}"},
            json=payload,
        )

    assert response.status_code == 200
    body = response.json()
    assert body["season_id"] == season_id
    assert body["targets"][0]["platform"] == "instagram"


def test_get_shared_account_sources(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "source_scope": "bravo",
        "using_defaults": False,
        "sources": [
            {
                "id": "shared-source-1",
                "platform": "instagram",
                "source_scope": "bravo",
                "account_handle": "bravotv",
                "is_active": True,
                "scrape_priority": 100,
            }
        ],
    }

    with patch("trr_backend.repositories.social_season_analytics.get_shared_account_sources", return_value=expected):
        response = client.get(
            "/api/v1/admin/socials/shared/sources?source_scope=bravo&include_inactive=true",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["source_scope"] == "bravo"
    assert body["sources"][0]["account_handle"] == "bravotv"


def test_get_social_account_profile_summary(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "platform": "instagram",
        "account_handle": "bravotv",
        "total_posts": 42,
        "per_show_counts": [{"show_id": str(uuid4()), "show_name": "RHOSLC", "post_count": 12}],
        "per_season_counts": [{"season_id": str(uuid4()), "season_number": 6, "post_count": 9}],
        "top_hashtags": [{"hashtag": "rhoslc", "usage_count": 5}],
        "top_collaborators": [],
        "top_tags": [],
        "source_status": [],
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.get_social_account_profile_summary",
        return_value=expected,
    ):
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/bravotv/summary",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["platform"] == "instagram"
    assert body["account_handle"] == "bravotv"
    assert body["total_posts"] == 42


def test_get_social_account_profile_posts(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "items": [
            {
                "id": "post-1",
                "source_id": "source-1",
                "platform": "instagram",
                "account_handle": "bravotv",
                "show_name": "RHOSLC",
                "season_number": 6,
            }
        ],
        "pagination": {"page": 2, "page_size": 10, "total": 11, "total_pages": 2},
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.get_social_account_profile_posts",
        return_value=expected,
    ) as mocked:
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/bravotv/posts?page=2&page_size=10",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["pagination"]["page"] == 2
    assert mocked.call_args.kwargs["page"] == 2
    assert mocked.call_args.kwargs["page_size"] == 10


def test_put_social_account_profile_hashtags(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    show_id = str(uuid4())
    season_id = str(uuid4())
    payload = {
        "hashtags": [
            {
                "hashtag": "rhoslc",
                "assignments": [
                    {"show_id": show_id},
                    {"show_id": show_id, "season_id": season_id},
                ],
            }
        ]
    }
    expected = {
        "items": [
            {
                "hashtag": "rhoslc",
                "assignments": [
                    {"show_id": show_id, "season_id": None},
                    {"show_id": show_id, "season_id": season_id},
                ],
            }
        ]
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.put_social_account_profile_hashtags",
        return_value=expected,
    ) as mocked:
        response = client.put(
            "/api/v1/admin/socials/profiles/instagram/bravotv/hashtags",
            headers={"Authorization": f"Bearer {token}"},
            json=payload,
        )

    assert response.status_code == 200
    assert response.json()["items"][0]["hashtag"] == "rhoslc"
    assert mocked.call_args.kwargs["updated_by"] is None


def test_get_social_account_profile_collaborators_tags(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "collaborators": [{"handle": "andycohen", "usage_count": 3}],
        "tags": [{"handle": "bravoandy", "usage_count": 5}],
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.get_social_account_profile_collaborators_tags",
        return_value=expected,
    ):
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/bravotv/collaborators-tags",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["collaborators"][0]["handle"] == "andycohen"
    assert body["tags"][0]["handle"] == "bravoandy"


def test_social_account_profile_summary_invalid_platform_returns_400(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.social_season_analytics.get_social_account_profile_summary",
        side_effect=ValueError("INVALID_PLATFORM_FILTER: Unsupported platform 'myspace'"),
    ):
        response = client.get(
            "/api/v1/admin/socials/profiles/myspace/bravotv/summary",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "INVALID_PLATFORM_FILTER"


def test_social_account_profile_summary_missing_account_returns_404(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.social_season_analytics.get_social_account_profile_summary",
        side_effect=LookupError("Social account profile not found."),
    ):
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/missing/summary",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 404
    assert response.json()["detail"] == "Social account profile not found."


def test_post_shared_ingest_returns_run_metadata(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "run_id": "shared-run-123",
        "status": "queued",
        "source_scope": "bravo",
        "ingest_mode": "shared_account_async",
        "shared_scrape_status": {"status": "queued", "job_count": 4},
        "classification_status": None,
        "materialization_status": None,
        "review_queue_count": 0,
    }

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
        with patch(
            "trr_backend.repositories.social_season_analytics.ingest_shared_accounts", return_value=expected
        ) as ingest_mock:
            with patch(
                "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
                return_value={"healthy": True, "healthy_workers": 1},
            ):
                response = client.post(
                    "/api/v1/admin/socials/shared/ingest",
                    headers={"Authorization": f"Bearer {token}"},
                    json={"source_scope": "bravo", "platforms": ["instagram", "twitter"]},
                )

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == "shared-run-123"
    assert body["ingest_mode"] == "shared_account_async"
    assert ingest_mock.call_args.kwargs["platforms"] == ["instagram", "twitter"]


def test_get_shared_review_queue(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "source_scope": "bravo",
        "review_status": "open",
        "count": 1,
        "items": [
            {
                "id": "review-1",
                "platform": "instagram",
                "source_id": "abc123",
                "review_reason": "ambiguous_match",
                "review_status": "open",
            }
        ],
    }

    with patch("trr_backend.repositories.social_season_analytics.list_shared_review_queue", return_value=expected):
        response = client.get(
            "/api/v1/admin/socials/shared/review-queue?source_scope=bravo&review_status=open&limit=10",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 1
    assert body["items"][0]["review_reason"] == "ambiguous_match"


def test_get_season_shared_status_route(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    expected = {
        "season_id": season_id,
        "ingest_mode": "shared_account_async",
        "matched_posts": 8,
        "review_queue_count": 2,
        "shared_scrape_status": {"status": "running", "job_count": 2, "active_jobs": 1},
        "classification_status": {"status": "complete", "job_count": 2, "active_jobs": 0},
        "materialization_status": {"status": "queued", "job_count": 1, "active_jobs": 0},
    }

    with patch("trr_backend.repositories.social_season_analytics.get_season_shared_status", return_value=expected):
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/shared-status?source_scope=bravo",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["season_id"] == season_id
    assert body["shared_scrape_status"]["status"] == "running"


def test_ingest_allows_zero_comments_limit(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    expected = {
        "season_id": season_id,
        "show_id": str(uuid4()),
        "season_number": 6,
        "source_scope": "bravo",
        "results": [],
    }
    payload = {
        "source_scope": "bravo",
        "platforms": ["instagram"],
        "max_posts_per_target": 5000,
        "max_comments_per_post": 0,
        "fetch_replies": False,
    }

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=False):
        with patch("trr_backend.repositories.social_season_analytics.ingest_season", return_value=expected) as mocked:
            response = client.post(
                f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                headers={"Authorization": f"Bearer {token}"},
                json=payload,
            )

    assert response.status_code == 200
    assert response.json()["season_id"] == season_id
    assert mocked.call_args.kwargs["max_comments_per_post"] == 0
    assert mocked.call_args.kwargs["sync_strategy"] == "incremental"


def test_ingest_returns_run_id_and_stage_metadata(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    expected = {
        "season_id": season_id,
        "run_id": "run-123",
        "status": "queued",
        "stages": ["posts", "comments"],
        "queued_or_started_jobs": 4,
        "summary": {"total_jobs": 4},
    }
    payload = {
        "source_scope": "bravo",
        "platforms": ["instagram"],
        "sync_strategy": "full_refresh",
    }

    with patch("trr_backend.repositories.social_season_analytics.ingest_season", return_value=expected) as ingest_mock:
        with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
            with patch(
                "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
                return_value={"healthy": True, "healthy_workers": 1},
            ):
                response = client.post(
                    f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                    headers={"Authorization": f"Bearer {token}"},
                    json=payload,
                )

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == "run-123"
    assert body["stages"] == ["posts", "comments"]
    assert ingest_mock.call_args.kwargs["ingest_mode"] == "posts_and_comments"
    assert ingest_mock.call_args.kwargs["sync_strategy"] == "full_refresh"


def test_ingest_accepts_comments_only_mode(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    expected = {
        "season_id": season_id,
        "run_id": "run-456",
        "status": "queued",
        "stages": ["comments"],
        "queued_or_started_jobs": 1,
        "summary": {"total_jobs": 1},
    }
    payload = {
        "source_scope": "bravo",
        "platforms": ["instagram"],
        "sync_strategy": "incremental",
        "ingest_mode": "comments_only",
    }

    with patch("trr_backend.repositories.social_season_analytics.ingest_season", return_value=expected) as ingest_mock:
        with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
            with patch(
                "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
                return_value={"healthy": True, "healthy_workers": 1},
            ):
                response = client.post(
                    f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                    headers={"Authorization": f"Bearer {token}"},
                    json=payload,
                )

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == "run-456"
    assert body["stages"] == ["comments"]
    assert ingest_mock.call_args.kwargs["ingest_mode"] == "comments_only"


def test_ingest_passes_comment_targeting_options(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    expected = {
        "season_id": season_id,
        "run_id": "run-789",
        "status": "queued",
        "stages": ["comments"],
        "queued_or_started_jobs": 1,
        "summary": {"total_jobs": 1},
    }
    payload = {
        "source_scope": "bravo",
        "platforms": ["instagram"],
        "sync_strategy": "incremental",
        "ingest_mode": "comments_only",
        "comment_refresh_policy": "missing_only",
        "comment_anchor_source_ids": {
            "instagram": ["abc123", "def456"],
        },
    }

    with patch("trr_backend.repositories.social_season_analytics.ingest_season", return_value=expected) as ingest_mock:
        with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
            with patch(
                "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
                return_value={"healthy": True, "healthy_workers": 1},
            ):
                response = client.post(
                    f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                    headers={"Authorization": f"Bearer {token}"},
                    json=payload,
                )

    assert response.status_code == 200
    assert ingest_mock.call_args.kwargs["comment_refresh_policy"] == "missing_only"
    assert ingest_mock.call_args.kwargs["comment_anchor_source_ids"] == {"instagram": ["abc123", "def456"]}


def test_ingest_passes_override_fields_and_details_refresh_mode(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    expected = {
        "season_id": season_id,
        "run_id": "run-details-refresh",
        "status": "queued",
        "stages": ["posts"],
        "queued_or_started_jobs": 2,
        "summary": {"total_jobs": 2},
    }
    payload = {
        "source_scope": "bravo",
        "platforms": ["instagram"],
        "ingest_mode": "details_refresh",
        "accounts_override": ["@BravoTV", "https://instagram.com/BRAVOTV/"],
        "hashtags_override": ["RHOSLC"],
        "keywords_override": ["Salt Lake City"],
        "sound_ids": ["7540327234013301517"],
    }

    with patch("trr_backend.repositories.social_season_analytics.ingest_season", return_value=expected) as ingest_mock:
        with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
            with patch(
                "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
                return_value={"healthy": True, "healthy_workers": 1},
            ):
                response = client.post(
                    f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                    headers={"Authorization": f"Bearer {token}"},
                    json=payload,
                )

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == "run-details-refresh"
    assert body["execution_mode"] == "queued"
    assert body["execution_mode_canonical"] == "queued"
    assert body["execution_mode_legacy"] == "queue"
    assert body["execution_mode_deprecation"]["field"] == "execution_mode_legacy"
    assert body["job_count"] == 2
    assert ingest_mock.call_args.kwargs["ingest_mode"] == "details_refresh"
    assert ingest_mock.call_args.kwargs["accounts_override"] == payload["accounts_override"]
    assert ingest_mock.call_args.kwargs["hashtags_override"] == payload["hashtags_override"]
    assert ingest_mock.call_args.kwargs["keywords_override"] == payload["keywords_override"]
    assert ingest_mock.call_args.kwargs["sound_ids"] == payload["sound_ids"]


def test_ingest_accepts_scheduler_fields(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    expected = {
        "season_id": season_id,
        "run_id": "run-scheduler",
        "status": "queued",
        "stages": ["posts", "comments"],
        "queued_or_started_jobs": 8,
        "summary": {"total_jobs": 8},
    }
    payload = {
        "source_scope": "bravo",
        "platforms": ["instagram"],
        "runner_strategy": "adaptive_dual_runner",
        "runner_count": 2,
        "window_shard_hours": 2,
        "runner_b_start_offset_hours": 48,
        "day_weight_profile": "rhoslc_default",
        "priority_mode": "episode_peak_weighted",
        "date_start": "2026-01-01T00:00:00Z",
        "date_end": "2026-01-08T00:00:00Z",
    }

    with patch("trr_backend.repositories.social_season_analytics.ingest_season", return_value=expected) as ingest_mock:
        with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
            with patch(
                "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
                return_value={"healthy": True, "healthy_workers": 1},
            ):
                response = client.post(
                    f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                    headers={"Authorization": f"Bearer {token}"},
                    json=payload,
                )

    assert response.status_code == 200
    assert ingest_mock.call_args.kwargs["runner_strategy"] == "adaptive_dual_runner"
    assert ingest_mock.call_args.kwargs["runner_count"] == 2
    assert ingest_mock.call_args.kwargs["window_shard_hours"] == 2
    assert ingest_mock.call_args.kwargs["runner_b_start_offset_hours"] == 48
    assert ingest_mock.call_args.kwargs["day_weight_profile"] == "rhoslc_default"
    assert ingest_mock.call_args.kwargs["priority_mode"] == "episode_peak_weighted"


def test_ingest_resolves_week_index_to_canonical_window(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    expected = {
        "season_id": season_id,
        "run_id": "run-week-2",
        "status": "queued",
        "stages": ["posts", "comments"],
        "queued_or_started_jobs": 4,
        "summary": {"total_jobs": 4},
    }
    payload = {
        "source_scope": "bravo",
        "platforms": ["instagram"],
        "week_index": 2,
        "timezone": "America/New_York",
        "date_start": "2026-01-01T00:00:00Z",
        "date_end": "2026-01-02T00:00:00Z",
    }
    resolved_week = {
        "week_index": 2,
        "label": "Week 2",
        "start": "2026-01-07T00:00:00Z",
        "end": "2026-01-13T23:59:59Z",
        "week_type": "episode",
        "episode_number": 2,
        "timezone": "America/New_York",
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.resolve_week_window",
        return_value=resolved_week,
    ) as resolve_mock:
        with patch(
            "trr_backend.repositories.social_season_analytics.ingest_season", return_value=expected
        ) as ingest_mock:
            with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
                with patch(
                    "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
                    return_value={"healthy": True, "healthy_workers": 1},
                ):
                    response = client.post(
                        f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                        headers={"Authorization": f"Bearer {token}"},
                        json=payload,
                    )

    assert response.status_code == 200
    body = response.json()
    assert body["scope"]["type"] == "week"
    assert body["scope"]["week"]["week_index"] == 2
    assert any("canonical season week window" in warning for warning in (body.get("warnings") or []))
    assert resolve_mock.called
    assert ingest_mock.call_args.kwargs["week_index"] == 2
    assert ingest_mock.call_args.kwargs["window_timezone"] == "America/New_York"
    assert ingest_mock.call_args.kwargs["run_scope_label"] == "Week 2"
    assert ingest_mock.call_args.kwargs["date_start"] == datetime(2026, 1, 7, 0, 0, tzinfo=UTC)
    assert ingest_mock.call_args.kwargs["date_end"] == datetime(2026, 1, 13, 23, 59, 59, tzinfo=UTC)


@pytest.mark.parametrize(
    ("code", "message"),
    [
        ("NO_INGEST_TARGETS", "No executable ingest targets"),
        ("INVALID_ACCOUNT_HANDLE", "Invalid account handle"),
    ],
)
def test_ingest_maps_structured_validation_errors_to_400(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    code: str,
    message: str,
) -> None:
    from trr_backend.repositories.social_season_analytics import SocialIngestValidationError

    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    payload = {"source_scope": "bravo", "platforms": ["instagram"]}

    with patch(
        "trr_backend.repositories.social_season_analytics.ingest_season",
        side_effect=SocialIngestValidationError(code, message),
    ):
        with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=False):
            response = client.post(
                f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                headers={"Authorization": f"Bearer {token}"},
                json=payload,
            )

    assert response.status_code == 400
    body = response.json()
    assert body["detail"]["code"] == code
    assert message.lower() in str(body["detail"]["message"]).lower()


@pytest.mark.parametrize(
    ("path", "patch_target"),
    [
        (
            "/api/v1/admin/socials/seasons/{season_id}/analytics?source_scope=bravo",
            "trr_backend.repositories.social_season_analytics.get_analytics",
        ),
        (
            "/api/v1/admin/socials/seasons/{season_id}/analytics/week/3?source_scope=bravo",
            "trr_backend.repositories.social_season_analytics.get_week_detail",
        ),
        (
            "/api/v1/admin/socials/seasons/{season_id}/analytics/comments-coverage?source_scope=bravo",
            "trr_backend.repositories.social_season_analytics.get_comments_coverage",
        ),
        (
            "/api/v1/admin/socials/seasons/{season_id}/analytics/mirror-coverage?source_scope=bravo",
            "trr_backend.repositories.social_season_analytics.get_mirror_coverage",
        ),
    ],
)
def test_analytics_platform_filter_value_errors_map_to_structured_400(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    path: str,
    patch_target: str,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    endpoint = path.format(season_id=season_id)

    with patch(patch_target, side_effect=ValueError("INVALID_PLATFORM_FILTER: unsupported")):
        response = client.get(
            endpoint,
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 400
    body = response.json()
    assert body["detail"]["code"] == "INVALID_PLATFORM_FILTER"
    assert "unsupported" in str(body["detail"]["message"]).lower()


def test_analytics_platform_filter_query_validation_rejects_unknown_platform(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    response = client.get(
        f"/api/v1/admin/socials/seasons/{season_id}/analytics?source_scope=bravo&platforms=instagram,myspace",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 400
    body = response.json()
    assert body["detail"]["code"] == "INVALID_PLATFORM_FILTER"
    assert "myspace" in str(body["detail"]["message"]).lower()


def test_instagram_scrape_async_returns_400_when_show_or_season_missing(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    payload = {
        "username": "bravotv",
        "hashtags": ["rhoslc"],
        "date_start": "2026-01-01T00:00:00Z",
        "date_end": "2026-01-02T00:00:00Z",
    }

    response = client.post(
        "/api/v1/admin/socials/instagram/scrape/async",
        headers={"Authorization": f"Bearer {token}"},
        json=payload,
    )

    assert response.status_code == 400
    body = response.json()
    assert body["detail"]["code"] == "BAD_REQUEST"


def test_instagram_scrape_async_creates_trackable_ingest_run(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    show_id = str(uuid4())
    season_id = str(uuid4())
    payload = {
        "username": "BravoTV",
        "hashtags": ["rhoslc"],
        "date_start": "2026-01-01T00:00:00Z",
        "date_end": "2026-01-02T00:00:00Z",
        "show_id": show_id,
        "season_number": 6,
    }

    with patch("trr_backend.db.pg.fetch_one", return_value={"season_id": season_id}):
        with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
            with patch(
                "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
                return_value={"healthy": True, "healthy_workers": 1},
            ):
                with patch(
                    "trr_backend.repositories.social_season_analytics.ingest_season",
                    return_value={"run_id": "run-ig-async"},
                ) as ingest_mock:
                    response = client.post(
                        "/api/v1/admin/socials/instagram/scrape/async",
                        headers={"Authorization": f"Bearer {token}"},
                        json=payload,
                    )

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == "run-ig-async"
    assert body["job_id"] == "run-ig-async"
    assert body["execution_mode"] == "queued"
    assert body["execution_mode_canonical"] == "queued"
    assert body["execution_mode_legacy"] == "queue"
    assert body["execution_mode_deprecation"]["field"] == "execution_mode_legacy"
    assert "/ingest/jobs?run_id=run-ig-async" in body["jobs_url"]
    assert ingest_mock.call_args.kwargs["ingest_mode"] == "posts_only"
    assert ingest_mock.call_args.kwargs["accounts_override"] == ["BravoTV"]
    assert ingest_mock.call_args.kwargs["hashtags_override"] == ["rhoslc"]


def test_instagram_scrape_async_starts_inline_when_queue_disabled(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    show_id = str(uuid4())
    season_id = str(uuid4())
    payload = {
        "username": "BravoTV",
        "hashtags": ["rhoslc"],
        "date_start": "2026-01-01T00:00:00Z",
        "date_end": "2026-01-02T00:00:00Z",
        "show_id": show_id,
        "season_number": 6,
    }

    with patch("trr_backend.db.pg.fetch_one", return_value={"season_id": season_id}):
        with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=False):
            with patch(
                "trr_backend.repositories.social_season_analytics.ingest_season",
                return_value={"run_id": str(uuid4())},
            ):
                with patch("trr_backend.repositories.social_season_analytics.execute_run") as execute_mock:
                    response = client.post(
                        "/api/v1/admin/socials/instagram/scrape/async",
                        headers={"Authorization": f"Bearer {token}"},
                        json=payload,
                    )

    assert response.status_code == 200
    body = response.json()
    assert isinstance(body["run_id"], str)
    assert body["execution_mode_canonical"] == "inline"
    assert body["execution_owner"] == "local_api"
    assert execute_mock.called is True


def test_instagram_scrape_async_allows_explicit_local_dev_fallback(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("TRR_LOCAL_DEV", "1")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    show_id = str(uuid4())
    season_id = str(uuid4())
    payload = {
        "username": "BravoTV",
        "hashtags": ["rhoslc"],
        "date_start": "2026-01-01T00:00:00Z",
        "date_end": "2026-01-02T00:00:00Z",
        "show_id": show_id,
        "season_number": 6,
        "allow_inline_dev_fallback": True,
    }

    with patch("trr_backend.db.pg.fetch_one", return_value={"season_id": season_id}):
        with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=False):
            with patch(
                "trr_backend.repositories.social_season_analytics.ingest_season",
                return_value={"run_id": str(uuid4())},
            ):
                with patch("trr_backend.repositories.social_season_analytics.execute_run"):
                    response = client.post(
                        "/api/v1/admin/socials/instagram/scrape/async",
                        headers={"Authorization": f"Bearer {token}"},
                        json=payload,
                    )

    assert response.status_code == 200
    body = response.json()
    assert isinstance(body["run_id"], str)
    assert body["execution_mode_canonical"] == "inline_fallback"
    assert body["execution_owner"] == "local_api"


def test_post_shared_ingest_starts_inline_when_queue_disabled(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "run_id": "shared-run-123",
        "status": "pending",
        "source_scope": "bravo",
        "ingest_mode": "shared_account_async",
        "shared_scrape_status": {"status": "pending", "job_count": 4},
        "classification_status": None,
        "materialization_status": None,
        "review_queue_count": 0,
    }

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=False):
        with patch("trr_backend.repositories.social_season_analytics.ingest_shared_accounts", return_value=expected):
            with patch("trr_backend.repositories.social_season_analytics.execute_run") as execute_mock:
                response = client.post(
                    "/api/v1/admin/socials/shared/ingest",
                    headers={"Authorization": f"Bearer {token}"},
                    json={"source_scope": "bravo", "platforms": ["instagram", "twitter"]},
                )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "started"
    assert body["execution_mode_canonical"] == "inline"
    assert body["execution_owner"] == "local_api"
    assert execute_mock.called is True


def test_get_ingest_jobs_supports_run_filters(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    run_id = str(uuid4())

    with patch("trr_backend.repositories.social_season_analytics.list_jobs", return_value=[]) as mocked:
        response = client.get(
            (
                f"/api/v1/admin/socials/seasons/{season_id}/ingest/jobs"
                f"?run_id={run_id}&status=running&platform=instagram&limit=75&offset=25"
            ),
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == run_id
    assert body["pagination"] == {
        "limit": 75,
        "offset": 25,
        "returned": 0,
        "has_more": False,
    }
    assert mocked.call_args.kwargs["limit"] == 75
    assert mocked.call_args.kwargs["offset"] == 25
    assert mocked.call_args.kwargs["run_id"] == run_id
    assert mocked.call_args.kwargs["status"] == "running"
    assert mocked.call_args.kwargs["platform"] == "instagram"


def test_get_ingest_runs_supports_filters(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    run_id = str(uuid4())
    workflow_id = "social-orch-demo"
    date_start = "2025-08-14T04:00:00+00:00"
    date_end = "2025-09-16T23:59:59.999999+00:00"

    runs_payload = [
        {
            "id": str(uuid4()),
            "season_id": season_id,
            "status": "completed",
            "source_scope": "bravo",
        }
    ]
    query = urlencode(
        {
            "status": "completed",
            "source_scope": "bravo",
            "run_id": run_id,
            "limit": 25,
            "client_workflow_id": workflow_id,
            "platforms": "twitter",
            "week_index": 2,
            "date_start": date_start,
            "date_end": date_end,
        }
    )

    with patch("trr_backend.repositories.social_season_analytics.list_runs", return_value=runs_payload) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/ingest/runs?{query}",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["season_id"] == season_id
    assert body["filters"]["status"] == "completed"
    assert body["filters"]["source_scope"] == "bravo"
    assert body["filters"]["run_id"] == run_id
    assert body["filters"]["client_workflow_id"] == workflow_id
    assert body["filters"]["platforms"] == ["twitter"]
    assert body["filters"]["week_index"] == 2
    assert body["filters"]["date_start"] == date_start
    assert body["filters"]["date_end"] == date_end
    assert body["runs"] == runs_payload
    assert mocked.call_args.kwargs["status"] == "completed"
    assert mocked.call_args.kwargs["source_scope"] == "bravo"
    assert mocked.call_args.kwargs["run_id"] == run_id
    assert mocked.call_args.kwargs["client_workflow_id"] == workflow_id
    assert mocked.call_args.kwargs["platforms"] == ["twitter"]
    assert mocked.call_args.kwargs["week_index"] == 2
    assert mocked.call_args.kwargs["date_start"].isoformat() == date_start
    assert mocked.call_args.kwargs["date_end"].isoformat() == date_end
    assert mocked.call_args.kwargs["limit"] == 25


def test_orchestrate_ingest_endpoint_queues_grouped_runs(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
        with patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            return_value={"healthy": True, "healthy_workers": 2},
        ):
            with patch(
                "trr_backend.repositories.social_season_analytics.orchestrate_season_ingest",
                return_value={
                    "season_id": season_id,
                    "orchestration_id": "social-orch-demo",
                    "scope": "single_week_single_platform",
                    "requested_runs": [{"slot_key": "week:2|platform:instagram"}],
                    "created_runs": [{"run_id": "run-1", "slot_key": "week:2|platform:instagram"}],
                    "reused_runs": [],
                    "total_requested_runs": 1,
                    "created_count": 1,
                    "reused_count": 0,
                    "execution_owner": "remote_worker",
                    "execution_mode_canonical": "remote",
                    "remote_job_plane_enforced": True,
                    "weeks": [{"week_index": 2, "label": "Week 2"}],
                    "platforms": ["instagram"],
                },
            ) as mocked:
                response = client.post(
                    f"/api/v1/admin/socials/seasons/{season_id}/ingest/orchestrations",
                    headers={"Authorization": f"Bearer {token}"},
                    json={"week_index": 2, "platforms": ["instagram"], "resume_existing": True},
                )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "queued"
    assert body["orchestration_id"] == "social-orch-demo"
    assert body["scope"] == "single_week_single_platform"
    assert body["execution_owner"] == "remote_worker"
    assert mocked.call_args.kwargs["week_index"] == 2
    assert mocked.call_args.kwargs["platforms"] == ["instagram"]
    assert mocked.call_args.kwargs["resume_existing"] is True


def test_orchestrate_ingest_endpoint_starts_inline_when_queue_disabled(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=False):
        with patch(
            "trr_backend.repositories.social_season_analytics.orchestrate_season_ingest",
            return_value={
                "season_id": season_id,
                "orchestration_id": "social-orch-demo",
                "scope": "single_week_single_platform",
                "requested_runs": [{"slot_key": "week:2|platform:instagram"}],
                "created_runs": [{"run_id": "run-1", "slot_key": "week:2|platform:instagram"}],
                "reused_runs": [],
                "total_requested_runs": 1,
                "created_count": 1,
                "reused_count": 0,
                "execution_owner": "local_api",
                "execution_mode_canonical": "local",
                "remote_job_plane_enforced": False,
                "weeks": [{"week_index": 2, "label": "Week 2"}],
                "platforms": ["instagram"],
            },
        ) as mocked:
            with patch("trr_backend.repositories.social_season_analytics.execute_run") as execute_mock:
                response = client.post(
                    f"/api/v1/admin/socials/seasons/{season_id}/ingest/orchestrations",
                    headers={"Authorization": f"Bearer {token}"},
                    json={"week_index": 2, "platforms": ["instagram"], "resume_existing": True},
                )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "started"
    assert body["message"] == "Season social orchestration started inline."
    assert mocked.call_args.kwargs["week_index"] == 2
    assert execute_mock.called is True


def test_get_ingest_runs_rejects_invalid_season_id(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    response = client.get(
        "/api/v1/admin/socials/seasons/not-a-uuid/ingest/runs",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 422


def test_get_ingest_runs_requires_admin_auth(client: TestClient) -> None:
    season_id = str(uuid4())
    response = client.get(f"/api/v1/admin/socials/seasons/{season_id}/ingest/runs")
    assert response.status_code in {401, 403}


def test_get_week_detail_endpoint_returns_youtube_comment_totals(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    payload = {
        "season_id": season_id,
        "week": {"week_index": 3, "label": "Week 3", "start": "2025-09-30T00:00:00Z", "end": "2025-10-07T00:00:00Z"},
        "platforms": {
            "youtube": {
                "posts": [
                    {
                        "source_id": "vid123",
                        "comments_count": 420,
                        "total_comments_available": 420,
                    }
                ],
                "totals": {"posts": 1, "total_comments": 420, "total_engagement": 1000},
            }
        },
        "totals": {"posts": 1, "total_comments": 420, "total_engagement": 1000},
    }

    with patch("trr_backend.repositories.social_season_analytics.get_week_detail", return_value=payload):
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/3?source_scope=bravo",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["totals"]["total_comments"] == 420
    assert body["platforms"]["youtube"]["posts"][0]["comments_count"] == 420


def test_get_week_detail_endpoint_includes_additive_week_metadata(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    payload = {
        "season_id": season_id,
        "week": {
            "week_index": 3,
            "label": "BYE WEEK (Jan 15-Jan 22)",
            "start": "2026-01-16T01:00:00Z",
            "end": "2026-01-23T00:59:59Z",
            "week_type": "bye",
            "episode_number": None,
        },
        "platforms": {
            "youtube": {
                "posts": [],
                "totals": {"posts": 0, "total_comments": 0, "total_engagement": 0},
            }
        },
        "totals": {"posts": 0, "total_comments": 0, "total_engagement": 0},
    }

    with patch("trr_backend.repositories.social_season_analytics.get_week_detail", return_value=payload):
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/3?source_scope=bravo",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["week"]["label"] == "BYE WEEK (Jan 15-Jan 22)"
    assert body["week"]["week_type"] == "bye"
    assert body["week"]["episode_number"] is None


def test_get_week_detail_endpoint_includes_additive_diagnostics(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    payload = {
        "season_id": season_id,
        "week": {"week_index": 3, "label": "Week 3", "start": "2025-09-30T00:00:00Z", "end": "2025-10-07T00:00:00Z"},
        "platforms": {
            "youtube": {
                "posts": [],
                "totals": {
                    "posts": 1,
                    "total_comments": 420,
                    "total_engagement": 1000,
                    "expected_comments_total": 500,
                    "saved_comments_total": 420,
                    "comments_saved_pct": 84.0,
                },
            }
        },
        "totals": {
            "posts": 1,
            "total_comments": 420,
            "total_engagement": 1000,
            "expected_comments_total": 500,
            "saved_comments_total": 420,
            "comments_saved_pct": 84.0,
        },
        "diagnostics": {
            "run_id": "run-abc",
            "generated_at": "2026-02-24T00:00:00Z",
            "source_scope": "bravo",
        },
    }

    with patch("trr_backend.repositories.social_season_analytics.get_week_detail", return_value=payload):
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/3?source_scope=bravo",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["totals"]["expected_comments_total"] == 500
    assert body["totals"]["saved_comments_total"] == 420
    assert body["totals"]["comments_saved_pct"] == 84.0
    assert body["diagnostics"]["run_id"] == "run-abc"


def test_get_week_detail_endpoint_passes_threads_topic_field(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    payload = {
        "season_id": season_id,
        "week": {"week_index": 3, "label": "Week 3", "start": "2025-09-30T00:00:00Z", "end": "2025-10-07T00:00:00Z"},
        "platforms": {
            "threads": {
                "posts": [
                    {
                        "source_id": "th-1",
                        "author": "bravotv",
                        "text": "Post body",
                        "topic": "bravotv > rhoslc",
                        "posted_at": "2026-01-01T00:00:00Z",
                        "replies_count": 3,
                        "likes": 10,
                        "reposts": 2,
                        "quotes": 1,
                        "views": 20,
                        "engagement": 36,
                        "total_comments_available": 3,
                        "comments": [],
                    }
                ],
                "totals": {"posts": 1, "total_comments": 3, "total_engagement": 36},
                "total_posts": 1,
            }
        },
        "totals": {"posts": 1, "total_comments": 3, "total_engagement": 36},
    }

    with patch("trr_backend.repositories.social_season_analytics.get_week_detail", return_value=payload):
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/3?source_scope=bravo",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["platforms"]["threads"]["posts"][0]["topic"] == "bravotv > rhoslc"


def test_get_week_detail_endpoint_defaults_to_25_comments_and_paginated_page(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    posts = [
        {
            "source_id": f"p{i}",
            "author": "bravotv",
            "text": f"Post {i}",
            "url": "https://instagram.com/p/abc",
            "posted_at": (datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=i)).isoformat(),
            "engagement": 10 + i,
            "total_comments_available": 0,
            "comments": [],
            "likes": 0,
            "comments_count": 0,
            "views": 100,
            "thumbnail_url": None,
        }
        for i in range(25)
    ]
    payload: dict[str, Any] = {
        "season_id": season_id,
        "week": {
            "week_index": 3,
            "label": "Week 3",
            "start": "2025-09-30T00:00:00Z",
            "end": "2025-10-07T00:00:00Z",
        },
        "platforms": {
            "instagram": {
                "posts": posts[:20],
                "total_posts": 25,
                "totals": {"posts": 20, "total_comments": 0, "total_engagement": 0},
            }
        },
        "totals": {"posts": 25, "total_comments": 0, "total_engagement": 0},
    }

    with patch("trr_backend.repositories.social_season_analytics.get_week_detail", return_value=payload) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/3?source_scope=bravo",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["pagination"] == {
        "limit": 20,
        "offset": 0,
        "returned": 20,
        "total": 25,
        "has_more": True,
    }
    assert mocked.call_count == 1
    assert mocked.call_args.kwargs["max_comments_per_post"] == 0
    assert mocked.call_args.kwargs["post_limit"] == 20
    assert mocked.call_args.kwargs["post_offset"] == 0


def test_get_week_summary_endpoint_forwards_defaults_and_shape(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    payload: dict[str, Any] = {
        "week": {"week_index": 3, "label": "Week 3", "start": "2025-09-30T00:00:00Z", "end": "2025-10-07T00:00:00Z"},
        "season": {
            "season_id": season_id,
            "show_id": str(uuid4()),
            "show_name": "Test",
            "show_slug": "test",
            "season_number": 6,
        },
        "source_scope": "bravo",
        "platforms": {"instagram": {"total_posts": 12, "totals": {"posts": 12}}},
        "totals": {"posts": 12},
        "meta": {"performance": {"total_duration_ms": 10, "by_platform": {"instagram": 2}}},
    }

    from api.routers import socials as socials_router

    socials_router.invalidate_week_summary_cache()
    with patch(
        "trr_backend.repositories.social_season_analytics.get_week_detail_summary_fast", return_value=payload
    ) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/3/summary?source_scope=bravo",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["platforms"]["instagram"]["totals"]["posts"] == 12
    assert mocked.call_args.kwargs["source_scope"] == "bravo"


def test_get_week_summary_endpoint_include_full_uses_legacy_path(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    payload: dict[str, Any] = {
        "week": {"week_index": 3, "label": "Week 3", "start": "2025-09-30T00:00:00Z", "end": "2025-10-07T00:00:00Z"},
        "season": {
            "season_id": season_id,
            "show_id": str(uuid4()),
            "show_name": "Test",
            "show_slug": "test",
            "season_number": 6,
        },
        "source_scope": "bravo",
        "platforms": {"instagram": {"total_posts": 12, "totals": {"posts": 12}}},
        "totals": {"posts": 12},
        "meta": {"performance": {"total_duration_ms": 10, "by_platform": {"instagram": 2}}},
    }

    from api.routers import socials as socials_router

    socials_router.invalidate_week_summary_cache()
    with patch(
        "trr_backend.repositories.social_season_analytics.get_week_detail_summary", return_value=payload
    ) as full_mock:
        with patch("trr_backend.repositories.social_season_analytics.get_week_detail_summary_fast") as fast_mock:
            response = client.get(
                f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/3/summary?source_scope=bravo&include=full",
                headers={"Authorization": f"Bearer {token}"},
            )

    assert response.status_code == 200
    assert full_mock.called
    assert not fast_mock.called


def test_get_schedule_preview_endpoint_returns_payload(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    preview_payload = {
        "season_id": season_id,
        "scheduler": {
            "strategy": "adaptive_dual_runner",
            "runner_count": 2,
            "window_shard_hours": 2,
            "runner_b_start_offset_hours": 48,
            "day_weight_profile": "rhoslc_default",
            "priority_mode": "episode_peak_weighted",
        },
        "lanes": {"A": [{"shard_index": 0}], "B": [{"shard_index": 1}]},
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.preview_ingest_schedule", return_value=preview_payload
    ) as mocked:
        response = client.get(
            (
                f"/api/v1/admin/socials/seasons/{season_id}/ingest/schedule-preview"
                "?source_scope=bravo&platforms=instagram&runner_strategy=adaptive_dual_runner&runner_count=2"
                "&window_shard_hours=2&day_weight_profile=rhoslc_default&priority_mode=episode_peak_weighted"
            ),
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["scheduler"]["strategy"] == "adaptive_dual_runner"
    assert body["scheduler"]["runner_b_start_offset_hours"] == 48
    assert mocked.call_args.kwargs["runner_count"] == 2


def test_get_schedule_preview_endpoint_resolves_week_scope(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    resolved_week = {
        "week_index": 3,
        "label": "Week 3",
        "start": "2026-01-14T00:00:00Z",
        "end": "2026-01-20T23:59:59Z",
        "week_type": "episode",
        "episode_number": 3,
        "timezone": "America/New_York",
    }
    preview_payload = {
        "season_id": season_id,
        "scope": {"type": "week", "label": "Week 3", "week_index": 3, "timezone": "America/New_York"},
        "scheduler": {"strategy": "single_runner"},
        "lanes": {"A": [{"shard_index": 0}], "B": []},
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.resolve_week_window",
        return_value=resolved_week,
    ) as resolve_mock:
        with patch(
            "trr_backend.repositories.social_season_analytics.preview_ingest_schedule", return_value=preview_payload
        ) as mocked:
            response = client.get(
                (
                    f"/api/v1/admin/socials/seasons/{season_id}/ingest/schedule-preview"
                    "?source_scope=bravo&platforms=instagram&week_index=3&timezone=America/New_York"
                ),
                headers={"Authorization": f"Bearer {token}"},
            )

    assert response.status_code == 200
    assert response.json()["scope"]["type"] == "week"
    assert resolve_mock.called
    assert mocked.call_args.kwargs["week_index"] == 3
    assert mocked.call_args.kwargs["window_timezone"] == "America/New_York"
    assert mocked.call_args.kwargs["run_scope_label"] == "Week 3"
    assert mocked.call_args.kwargs["date_start"] == datetime(2026, 1, 14, 0, 0, tzinfo=UTC)
    assert mocked.call_args.kwargs["date_end"] == datetime(2026, 1, 20, 23, 59, 59, tzinfo=UTC)


def test_get_week_detail_endpoint_supports_page_offset_and_de_duplicated_newest_first_order(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    posts = [
        {
            "source_id": f"p{i}",
            "author": "bravotv",
            "text": f"Post {i}",
            "url": "https://instagram.com/p/abc",
            "posted_at": (datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=i)).isoformat(),
            "engagement": 10 + i,
            "total_comments_available": 0,
            "comments": [],
            "likes": 0,
            "comments_count": 0,
            "views": 100,
            "thumbnail_url": None,
        }
        for i in range(40)
    ]
    payload: dict[str, Any] = {
        "season_id": season_id,
        "week": {
            "week_index": 3,
            "label": "Week 3",
            "start": "2025-09-30T00:00:00Z",
            "end": "2025-10-07T00:00:00Z",
        },
        "platforms": {
            "instagram": {
                "posts": posts,
                "total_posts": 40,
                "totals": {"posts": 40, "total_comments": 0, "total_engagement": 0},
            }
        },
        "totals": {"posts": 40, "total_comments": 0, "total_engagement": 0},
    }

    from api.routers import socials as socials_router

    socials_router.invalidate_week_detail_cache()
    with patch("trr_backend.repositories.social_season_analytics.get_week_detail", return_value=payload) as mocked:
        response_page1 = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/3?source_scope=bravo",
            headers={"Authorization": f"Bearer {token}"},
        )
        response_page2 = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/3?source_scope=bravo&post_limit=20&post_offset=20",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response_page1.status_code == 200
    assert response_page2.status_code == 200

    page1 = response_page1.json()
    page2 = response_page2.json()
    assert page1["pagination"]["returned"] == 20
    assert page1["pagination"]["offset"] == 0
    assert page1["pagination"]["has_more"] is True
    assert [post["source_id"] for post in page1["platforms"]["instagram"]["posts"]] == [
        f"p{i}" for i in range(39, 19, -1)
    ]
    assert page1["pagination"]["total"] == 40

    assert page2["pagination"]["returned"] == 20
    assert page2["pagination"]["offset"] == 20
    assert page2["pagination"]["has_more"] is False
    assert page2["pagination"]["total"] == 40
    assert [post["source_id"] for post in page2["platforms"]["instagram"]["posts"]] == [
        f"p{i}" for i in range(19, -1, -1)
    ]
    assert len({post["source_id"] for post in page1["platforms"]["instagram"]["posts"]}) == 20
    assert len({post["source_id"] for post in page2["platforms"]["instagram"]["posts"]}) == 20
    assert mocked.call_count == 1


def test_get_week_detail_endpoint_forwards_sort_params_to_repository(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    payload: dict[str, Any] = {
        "season_id": season_id,
        "week": {"week_index": 3, "label": "Week 3", "start": "2025-09-30T00:00:00Z", "end": "2025-10-07T00:00:00Z"},
        "platforms": {
            "instagram": {
                "posts": [],
                "total_posts": 0,
                "totals": {"posts": 0, "total_comments": 0, "total_engagement": 0},
            }
        },
        "totals": {"posts": 0, "total_comments": 0, "total_engagement": 0},
    }

    with patch("trr_backend.repositories.social_season_analytics.get_week_detail", return_value=payload) as mocked:
        response = client.get(
            (
                f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/3"
                "?source_scope=bravo&sort_field=likes&sort_dir=asc"
            ),
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert mocked.call_count == 1
    assert mocked.call_args.kwargs["sort_field"] == "likes"
    assert mocked.call_args.kwargs["sort_dir"] == "asc"


def test_get_week_detail_endpoint_sorts_page_by_requested_metric(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    payload: dict[str, Any] = {
        "season_id": season_id,
        "week": {"week_index": 3, "label": "Week 3", "start": "2025-09-30T00:00:00Z", "end": "2025-10-07T00:00:00Z"},
        "platforms": {
            "instagram": {
                "posts": [
                    {"source_id": "ig-1", "posted_at": "2026-01-01T00:00:00Z", "likes": 10},
                    {"source_id": "ig-2", "posted_at": "2026-01-02T00:00:00Z", "likes": 90},
                ],
                "total_posts": 2,
                "totals": {"posts": 2, "total_comments": 0, "total_engagement": 0},
            },
            "tiktok": {
                "posts": [
                    {"source_id": "tt-1", "posted_at": "2026-01-03T00:00:00Z", "likes": 50},
                    {"source_id": "tt-2", "posted_at": "2026-01-04T00:00:00Z", "likes": 70},
                ],
                "total_posts": 2,
                "totals": {"posts": 2, "total_comments": 0, "total_engagement": 0},
            },
        },
        "totals": {"posts": 4, "total_comments": 0, "total_engagement": 0},
    }

    from api.routers import socials as socials_router

    socials_router.invalidate_week_detail_cache()
    with patch("trr_backend.repositories.social_season_analytics.get_week_detail", return_value=payload):
        response = client.get(
            (
                f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/3"
                "?source_scope=bravo&sort_field=likes&sort_dir=desc&post_limit=2"
            ),
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    flat_posts = [post for platform_payload in body["platforms"].values() for post in platform_payload["posts"]]
    ordered = sorted(flat_posts, key=lambda post: int(post.get("sort_rank", 0)))
    assert [post["source_id"] for post in ordered] == ["ig-2", "tt-2"]
    assert body["pagination"]["returned"] == 2
    assert body["pagination"]["total"] == 4


def test_get_week_detail_endpoint_uses_cached_payload_when_repeating_same_page_request(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    payload: dict[str, Any] = {
        "season_id": season_id,
        "week": {
            "week_index": 3,
            "label": "Week 3",
            "start": "2025-09-30T00:00:00Z",
            "end": "2025-10-07T00:00:00Z",
        },
        "platforms": {
            "instagram": {
                "posts": [
                    {"source_id": "cached-1", "posted_at": "2026-10-01T00:00:00Z"},
                    {"source_id": "cached-2", "posted_at": "2026-09-30T00:00:00Z"},
                    {"source_id": "cached-3", "posted_at": "2026-09-29T00:00:00Z"},
                    {"source_id": "cached-4", "posted_at": "2026-09-28T00:00:00Z"},
                    {"source_id": "cached-5", "posted_at": "2026-09-27T00:00:00Z"},
                ],
                "total_posts": 5,
                "totals": {"posts": 5, "total_comments": 0, "total_engagement": 0},
            }
        },
        "totals": {"posts": 5, "total_comments": 0, "total_engagement": 0},
    }

    from api.routers import socials as socials_router

    socials_router.invalidate_week_detail_cache()
    with patch("trr_backend.repositories.social_season_analytics.get_week_detail", return_value=payload) as mocked:
        response_first = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/3?source_scope=bravo",
            headers={"Authorization": f"Bearer {token}"},
        )
        response_second = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/3?source_scope=bravo",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response_first.status_code == 200
    assert response_second.status_code == 200
    assert mocked.call_count == 1


def test_get_week_detail_endpoint_cache_key_includes_sort_signature(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    payload: dict[str, Any] = {
        "season_id": season_id,
        "week": {"week_index": 3, "label": "Week 3", "start": "2025-09-30T00:00:00Z", "end": "2025-10-07T00:00:00Z"},
        "platforms": {
            "instagram": {
                "posts": [{"source_id": "cached-1", "posted_at": "2026-10-01T00:00:00Z", "likes": 1, "views": 100}],
                "total_posts": 1,
                "totals": {"posts": 1, "total_comments": 0, "total_engagement": 0},
            }
        },
        "totals": {"posts": 1, "total_comments": 0, "total_engagement": 0},
    }

    from api.routers import socials as socials_router

    socials_router.invalidate_week_detail_cache()
    with patch("trr_backend.repositories.social_season_analytics.get_week_detail", return_value=payload) as mocked:
        response_a = client.get(
            (
                f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/3"
                "?source_scope=bravo&sort_field=likes&sort_dir=desc"
            ),
            headers={"Authorization": f"Bearer {token}"},
        )
        response_b = client.get(
            (
                f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/3"
                "?source_scope=bravo&sort_field=likes&sort_dir=desc"
            ),
            headers={"Authorization": f"Bearer {token}"},
        )
        response_c = client.get(
            (
                f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/3"
                "?source_scope=bravo&sort_field=views&sort_dir=desc"
            ),
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response_a.status_code == 200
    assert response_b.status_code == 200
    assert response_c.status_code == 200
    assert mocked.call_count == 2


def test_get_analytics_endpoint_includes_additive_week_metadata(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    expected = {
        "window": {"week": None},
        "summary": {},
        "weekly": [
            {
                "week_index": 2,
                "label": "BYE WEEK (Jan 15-Jan 22)",
                "week_type": "bye",
                "episode_number": None,
                "start": "2026-01-16T01:00:00Z",
                "end": "2026-01-23T00:59:59Z",
                "post_volume": 0,
                "comment_volume": 0,
                "engagement": 0,
                "sentiment": {"positive": 0, "neutral": 0, "negative": 0},
            }
        ],
        "weekly_platform_posts": [
            {
                "week_index": 2,
                "label": "BYE WEEK (Jan 15-Jan 22)",
                "week_type": "bye",
                "episode_number": None,
                "start": "2026-01-16T01:00:00Z",
                "end": "2026-01-23T00:59:59Z",
                "posts": {"instagram": 0, "youtube": 0, "tiktok": 0, "twitter": 0},
                "comments": {"instagram": 0, "youtube": 0, "tiktok": 0, "twitter": 0},
                "reported_comments": {"instagram": 0, "youtube": 0, "tiktok": 0, "twitter": 0},
                "total_posts": 0,
                "total_comments": 0,
                "total_reported_comments": 0,
                "comments_saved_pct": None,
            }
        ],
        "weekly_platform_engagement": [],
        "weekly_daily_activity": [
            {
                "week_index": 2,
                "label": "BYE WEEK (Jan 15-Jan 22)",
                "week_type": "bye",
                "episode_number": None,
                "start": "2026-01-16T01:00:00Z",
                "end": "2026-01-23T00:59:59Z",
                "days": [
                    {
                        "day_index": 0,
                        "date_local": "2026-01-15",
                        "posts": {"instagram": 0, "youtube": 0, "tiktok": 0, "twitter": 0},
                        "comments": {"instagram": 0, "youtube": 0, "tiktok": 0, "twitter": 0},
                        "reported_comments": {"instagram": 0, "youtube": 0, "tiktok": 0, "twitter": 0},
                        "total_posts": 0,
                        "total_comments": 0,
                        "total_reported_comments": 0,
                    }
                ],
            }
        ],
        "platform_breakdown": [],
        "themes": {"positive": [], "negative": []},
        "leaderboards": {"bravo_content": [], "viewer_discussion": []},
        "jobs": [],
    }

    with patch("trr_backend.repositories.social_season_analytics.get_analytics", return_value=expected):
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["weekly"][0]["label"] == "BYE WEEK (Jan 15-Jan 22)"
    assert body["weekly"][0]["week_type"] == "bye"
    assert body["weekly"][0]["episode_number"] is None
    assert body["weekly_daily_activity"][0]["days"][0]["reported_comments"]["instagram"] == 0
    assert body["weekly_daily_activity"][0]["days"][0]["total_reported_comments"] == 0


def test_get_post_comments_endpoint_returns_youtube_effective_stats(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    payload = {
        "platform": "youtube",
        "source_id": "vid123",
        "stats": {"views": 1000, "likes": 100, "comments_count": 420, "engagement": 1520},
        "total_comments_in_db": 420,
        "comments": [],
    }

    with patch("trr_backend.repositories.social_season_analytics.get_post_comments", return_value=payload):
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics/posts/youtube/vid123",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["stats"]["comments_count"] == 420
    assert body["total_comments_in_db"] == 420


def test_get_post_comments_endpoint_returns_twitter_quotes_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    payload = {
        "platform": "twitter",
        "source_id": "tweet-1",
        "stats": {"likes": 10, "retweets": 2, "replies_count": 1, "quotes": 5, "views": 100, "engagement": 118},
        "total_comments_in_db": 1,
        "total_quotes_in_db": 2,
        "comments": [],
        "quotes": [
            {
                "comment_id": "quote-1",
                "author": "viewer",
                "text": "quote body",
                "likes": 4,
                "is_reply": False,
            }
        ],
    }

    with patch("trr_backend.repositories.social_season_analytics.get_post_comments", return_value=payload):
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics/posts/twitter/tweet-1",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["total_quotes_in_db"] == 2
    assert body["quotes"][0]["comment_id"] == "quote-1"


def test_get_post_comments_endpoint_passes_threads_topic_field(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    payload = {
        "platform": "threads",
        "source_id": "th-1",
        "author": "bravotv",
        "text": "Post body",
        "topic": "bravotv > rhoslc",
        "stats": {"likes": 10, "replies_count": 3, "reposts": 2, "quotes": 1, "views": 20, "engagement": 36},
        "total_comments_in_db": 3,
        "comments": [],
    }

    with patch("trr_backend.repositories.social_season_analytics.get_post_comments", return_value=payload):
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics/posts/threads/th-1",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["topic"] == "bravotv > rhoslc"


def test_refresh_post_comments_endpoint_returns_latest_post_detail(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    refreshed_post = {
        "platform": "tiktok",
        "source_id": "72899887766",
        "author": "bravotv",
        "text": "Example post",
        "url": "https://www.tiktok.com/@bravotv/video/72899887766",
        "posted_at": "2026-02-17T10:00:00+00:00",
        "stats": {"comments_count": 604, "likes": 59500, "engagement": 564500},
        "total_comments_in_db": 604,
        "comments": [],
    }
    refresh_summary = {
        "platform": "tiktok",
        "source_id": "72899887766",
        "comments_fetched": 604,
        "comments_upserted": 604,
        "total_comments_in_db": 604,
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.refresh_post",
        return_value=refresh_summary,
    ) as refresh_mock:
        with patch(
            "trr_backend.repositories.social_season_analytics.get_post_comments",
            return_value=refreshed_post,
        ) as get_mock:
            response = client.post(
                f"/api/v1/admin/socials/seasons/{season_id}/analytics/posts/tiktok/72899887766/refresh",
                headers={"Authorization": f"Bearer {token}"},
                json={"max_comments_per_post": 1000, "fetch_replies": True},
            )

    assert response.status_code == 200
    body = response.json()
    assert body["source_id"] == "72899887766"
    assert body["refresh"]["comments_upserted"] == 604
    assert refresh_mock.call_args.kwargs["platform"] == "tiktok"
    assert refresh_mock.call_args.kwargs["source_id"] == "72899887766"
    assert refresh_mock.call_args.kwargs["max_comments_per_post"] == 1000
    assert get_mock.call_args.kwargs["platform"] == "tiktok"


def test_requeue_instagram_mirror_jobs_endpoint(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    expected = {
        "season_id": season_id,
        "source_scope": "bravo",
        "failed_only": True,
        "scanned": 20,
        "queued_jobs": 8,
        "skipped": 10,
        "failed": 2,
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.requeue_instagram_media_mirror_jobs",
        return_value=expected,
    ) as mocked:
        response = client.post(
            f"/api/v1/admin/socials/seasons/{season_id}/instagram/mirror/requeue"
            "?source_scope=bravo&limit=500&failed_only=true"
            "&date_start=2026-02-01T00:00:00Z&date_end=2026-02-08T00:00:00Z",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["queued_jobs"] == 8
    assert mocked.call_args.kwargs["source_scope"] == "bravo"
    assert mocked.call_args.kwargs["limit"] == 500
    assert mocked.call_args.kwargs["failed_only"] is True
    assert mocked.call_args.kwargs["date_start"] is not None
    assert mocked.call_args.kwargs["date_end"] is not None


def test_requeue_platform_mirror_jobs_endpoint(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    expected = {
        "season_id": season_id,
        "platform": "tiktok",
        "source_scope": "bravo",
        "failed_only": False,
        "scanned": 15,
        "queued_jobs": 4,
        "skipped": 11,
        "failed": 0,
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.requeue_media_mirror_jobs",
        return_value=expected,
    ) as mocked:
        response = client.post(
            f"/api/v1/admin/socials/seasons/{season_id}/tiktok/mirror/requeue"
            "?source_scope=bravo&limit=250"
            "&date_start=2026-02-01T00:00:00Z&date_end=2026-02-08T00:00:00Z",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["platform"] == "tiktok"
    assert mocked.call_args.kwargs["platform"] == "tiktok"
    assert mocked.call_args.kwargs["source_scope"] == "bravo"
    assert mocked.call_args.kwargs["limit"] == 250
    assert mocked.call_args.kwargs["failed_only"] is False
    assert mocked.call_args.kwargs["date_start"] is not None
    assert mocked.call_args.kwargs["date_end"] is not None


def test_cancel_ingest_run_endpoint(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    run_id = str(uuid4())
    expected = {"run_id": run_id, "status": "cancelled", "cancelled_jobs": 2}

    with patch("trr_backend.repositories.social_season_analytics.cancel_run", return_value=expected):
        response = client.post(
            f"/api/v1/admin/socials/seasons/{season_id}/ingest/runs/{run_id}/cancel",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["status"] == "cancelled"
    assert response.json()["run_id"] == run_id


def test_ingest_returns_400_when_queue_schema_missing(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    payload = {"source_scope": "bravo", "platforms": ["instagram"]}

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=False):
        with patch(
            "trr_backend.repositories.social_season_analytics.ingest_season",
            side_effect=ValueError("Social ingest queue schema is not migrated"),
        ):
            response = client.post(
                f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                headers={"Authorization": f"Bearer {token}"},
                json=payload,
            )

    assert response.status_code == 400
    assert "not migrated" in response.json()["detail"]


def test_ingest_returns_503_when_queue_enabled_and_worker_missing(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories.social_season_analytics import SocialWorkerUnavailableError

    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    payload = {"source_scope": "bravo", "platforms": ["instagram"]}

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
        with patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            side_effect=SocialWorkerUnavailableError(
                "No healthy social ingest workers are reporting heartbeats.",
                worker_health={
                    "healthy": False,
                    "healthy_workers": 0,
                    "reason": "no_healthy_workers",
                    "workers": [],
                },
            ),
        ):
            with patch("trr_backend.repositories.social_season_analytics.ingest_season") as ingest_mock:
                response = client.post(
                    f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                    headers={"Authorization": f"Bearer {token}"},
                    json=payload,
                )

    assert response.status_code == 503
    body = response.json()
    assert body["detail"]["code"] == "SOCIAL_WORKER_UNAVAILABLE"
    assert body["detail"]["worker_health"]["healthy"] is False
    ingest_mock.assert_not_called()


def test_ingest_returns_503_when_remote_job_plane_enforced_and_queue_disabled(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("TRR_LONG_JOB_ENFORCE_REMOTE", "1")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    payload = {"source_scope": "bravo", "platforms": ["twitter"], "allow_inline_dev_fallback": True}

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=False):
        with patch("trr_backend.repositories.social_season_analytics.ingest_season") as ingest_mock:
            response = client.post(
                f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                headers={"Authorization": f"Bearer {token}"},
                json=payload,
            )

    assert response.status_code == 503
    body = response.json()
    assert body["detail"]["code"] == "SOCIAL_REMOTE_JOB_PLANE_ENFORCED"
    ingest_mock.assert_not_called()


def test_ingest_returns_503_when_remote_job_plane_enforced_and_worker_missing_even_with_inline_fallback(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories.social_season_analytics import SocialWorkerUnavailableError

    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.setenv("TRR_LONG_JOB_ENFORCE_REMOTE", "1")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    payload = {
        "source_scope": "bravo",
        "platforms": ["youtube"],
        "allow_inline_dev_fallback": True,
    }

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
        with patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            side_effect=SocialWorkerUnavailableError(
                "No healthy social ingest workers are reporting heartbeats.",
                worker_health={
                    "healthy": False,
                    "healthy_workers": 0,
                    "reason": "no_healthy_workers",
                    "workers": [],
                },
            ),
        ):
            with patch("trr_backend.repositories.social_season_analytics.ingest_season") as ingest_mock:
                response = client.post(
                    f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                    headers={"Authorization": f"Bearer {token}"},
                    json=payload,
                )

    assert response.status_code == 503
    body = response.json()
    assert body["detail"]["code"] == "SOCIAL_REMOTE_JOB_PLANE_ENFORCED"
    assert body["detail"]["worker_health"]["healthy"] is False
    ingest_mock.assert_not_called()


def test_ingest_falls_back_inline_in_dev_when_worker_missing_and_flag_enabled(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories.social_season_analytics import SocialWorkerUnavailableError

    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.setenv("SOCIAL_REMOTE_ONLY_PLATFORMS", "instagram,tiktok")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    payload = {
        "source_scope": "bravo",
        "platforms": ["youtube"],
        "allow_inline_dev_fallback": True,
    }
    expected = {
        "season_id": season_id,
        "run_id": "run-inline-fallback",
        "status": "queued",
        "stages": ["posts", "comments"],
        "queued_or_started_jobs": 2,
        "summary": {"total_jobs": 2},
    }

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
        with patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            side_effect=SocialWorkerUnavailableError(
                "No healthy social ingest workers are reporting heartbeats.",
                worker_health={
                    "healthy": False,
                    "healthy_workers": 0,
                    "reason": "no_healthy_workers",
                    "workers": [],
                },
            ),
        ):
            with patch(
                "trr_backend.repositories.social_season_analytics.ingest_season",
                return_value=expected,
            ) as ingest_mock:
                with patch("trr_backend.repositories.social_season_analytics.execute_run", return_value=None):
                    response = client.post(
                        f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                        headers={"Authorization": f"Bearer {token}"},
                        json=payload,
                    )

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == "run-inline-fallback"
    assert body["execution_mode"] == "inline"
    assert body["execution_mode_canonical"] == "inline_fallback"
    assert body["execution_mode_legacy"] == "inline_fallback"
    assert body["execution_mode_deprecation"]["field"] == "execution_mode_legacy"
    assert body["status"] == "started"
    assert body["worker_health"]["healthy"] is False
    assert isinstance(body.get("warnings"), list)
    assert any("inline dev fallback" in str(item).lower() for item in (body.get("warnings") or []))
    ingest_mock.assert_called_once()


def test_ingest_requires_remote_worker_for_instagram_even_with_inline_fallback_enabled(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories.social_season_analytics import SocialWorkerUnavailableError

    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.setenv("SOCIAL_REMOTE_ONLY_PLATFORMS", "instagram,tiktok")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    payload = {
        "source_scope": "bravo",
        "platforms": ["instagram"],
        "allow_inline_dev_fallback": True,
    }

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
        with patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            side_effect=SocialWorkerUnavailableError(
                "No healthy social ingest workers are reporting heartbeats.",
                worker_health={
                    "healthy": False,
                    "healthy_workers": 0,
                    "reason": "no_healthy_workers",
                    "workers": [],
                },
            ),
        ):
            with patch("trr_backend.repositories.social_season_analytics.ingest_season") as ingest_mock:
                response = client.post(
                    f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                    headers={"Authorization": f"Bearer {token}"},
                    json=payload,
                )

    assert response.status_code == 503
    body = response.json()
    assert body["detail"]["code"] == "SOCIAL_REMOTE_WORKER_REQUIRED"
    assert body["detail"]["required_platforms"] == ["instagram"]
    ingest_mock.assert_not_called()


def test_ingest_comments_only_inline_fallback_spawns_per_platform_workers(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    payload = {
        "source_scope": "bravo",
        "platforms": ["instagram"],
        "ingest_mode": "comments_only",
    }
    expected = {
        "season_id": season_id,
        "run_id": "run-inline-comments-only",
        "status": "pending",
        "stages": ["comments"],
        "queued_or_started_jobs": 4,
        "summary": {"total_jobs": 4},
    }
    worker_counts: list[int] = []

    class _FakeFuture:
        def result(self) -> None:
            return None

    class _FakeThreadPoolExecutor:
        def __init__(self, *, max_workers: int):
            worker_counts.append(max_workers)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def submit(self, fn, *args, **kwargs):  # noqa: ANN001
            fn(*args, **kwargs)
            return _FakeFuture()

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=False):
        with patch("trr_backend.repositories.social_season_analytics.ingest_season", return_value=expected):
            with patch("trr_backend.repositories.social_season_analytics.execute_run", return_value=None):
                with patch("api.routers.socials.ThreadPoolExecutor", _FakeThreadPoolExecutor):
                    response = client.post(
                        f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                        headers={"Authorization": f"Bearer {token}"},
                        json=payload,
                    )

    assert response.status_code == 200
    # One worker per platform (1 platform requested = 1 worker)
    assert worker_counts == [1]


def test_ingest_inline_background_failure_runs_recovery_path(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    payload = {
        "source_scope": "bravo",
        "platforms": ["youtube"],
    }
    expected = {
        "season_id": season_id,
        "run_id": "run-inline-recovery",
        "status": "pending",
        "stages": ["posts", "comments"],
        "queued_or_started_jobs": 2,
        "summary": {"total_jobs": 2},
    }
    execute_worker_ids: list[str | None] = []

    def _execute_with_initial_failure(
        run_id: str, *, worker_id: str | None = None, stage: str | None = None, platform: str | None = None
    ) -> None:
        execute_worker_ids.append(worker_id)
        if worker_id == "api-background":
            raise RuntimeError("inline execution failed")
        return None

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=False):
        with patch("trr_backend.repositories.social_season_analytics.ingest_season", return_value=expected):
            with patch(
                "trr_backend.repositories.social_season_analytics.recover_stale_running_jobs",
                return_value=[{"id": "job-stale"}],
            ) as recover_mock:
                with patch(
                    "trr_backend.repositories.social_season_analytics.execute_run",
                    side_effect=_execute_with_initial_failure,
                ):
                    response = client.post(
                        f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                        headers={"Authorization": f"Bearer {token}"},
                        json=payload,
                    )

    assert response.status_code == 200
    assert execute_worker_ids == ["api-background", "api-background:recovery"]
    recover_mock.assert_called_once_with(run_id="run-inline-recovery", limit=250)


def test_ingest_comments_only_queue_mode_does_not_spawn_inline_runners(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("SOCIAL_COMMENTS_RUN_WORKERS", "3")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    payload = {
        "source_scope": "bravo",
        "platforms": ["instagram", "youtube", "tiktok", "twitter"],
        "ingest_mode": "comments_only",
    }
    expected = {
        "season_id": season_id,
        "run_id": "run-queue-comments-only",
        "status": "queued",
        "stages": ["comments"],
        "queued_or_started_jobs": 4,
        "summary": {"total_jobs": 4},
    }
    worker_counts: list[int] = []
    execute_calls: list[dict[str, object]] = []

    class _FakeFuture:
        def result(self) -> None:
            return None

    class _FakeThreadPoolExecutor:
        def __init__(self, *, max_workers: int):
            worker_counts.append(max_workers)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def submit(self, fn, *args, **kwargs):  # noqa: ANN001
            fn(*args, **kwargs)
            return _FakeFuture()

    def _record_execute(
        run_id: str, *, worker_id: str | None = None, stage: str | None = None, platform: str | None = None
    ) -> None:
        execute_calls.append(
            {
                "run_id": run_id,
                "worker_id": worker_id,
                "stage": stage,
                "platform": platform,
            }
        )

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
        with patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            return_value={"healthy": True, "healthy_workers": 1},
        ):
            with patch("trr_backend.repositories.social_season_analytics.ingest_season", return_value=expected):
                with patch("trr_backend.repositories.social_season_analytics.execute_run", side_effect=_record_execute):
                    with patch("api.routers.socials.ThreadPoolExecutor", _FakeThreadPoolExecutor):
                        response = client.post(
                            f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                            headers={"Authorization": f"Bearer {token}"},
                            json=payload,
                        )

    assert response.status_code == 200
    body = response.json()
    assert body["execution_mode"] == "queued"
    assert body["execution_mode_canonical"] == "queued"
    assert body["execution_mode_legacy"] == "queue"
    assert body["execution_mode_deprecation"]["field"] == "execution_mode_legacy"
    assert body["status"] == "queued"
    assert worker_counts == []
    assert execute_calls == []


def test_get_mirror_coverage_endpoint(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    expected = {
        "season_id": season_id,
        "up_to_date": False,
        "needs_mirror_count": 4,
        "mirrored_count": 9,
        "failed_count": 1,
        "partial_count": 2,
        "pending_count": 1,
        "posts_scanned": 13,
        "by_platform": {"instagram": {"needs_mirror_count": 4}},
        "evaluated_at": "2026-02-24T12:00:00+00:00",
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.get_mirror_coverage",
        return_value=expected,
    ) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics/mirror-coverage"
            "?source_scope=bravo&platforms=instagram,twitter&timezone=America/New_York",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["needs_mirror_count"] == 4
    assert mocked.call_args.kwargs["source_scope"] == "bravo"
    assert mocked.call_args.kwargs["platforms"] == ["instagram", "twitter"]


def test_ingest_keeps_503_when_worker_missing_outside_dev_even_with_fallback_flag(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories.social_season_analytics import SocialWorkerUnavailableError

    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.delenv("TRR_LOCAL_DEV", raising=False)
    monkeypatch.delenv("SOCIAL_ALLOW_INLINE_DEV_FALLBACK", raising=False)
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    payload = {
        "source_scope": "bravo",
        "platforms": ["instagram"],
        "allow_inline_dev_fallback": True,
    }

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
        with patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            side_effect=SocialWorkerUnavailableError(
                "No healthy social ingest workers are reporting heartbeats.",
                worker_health={
                    "healthy": False,
                    "healthy_workers": 0,
                    "reason": "no_healthy_workers",
                    "workers": [],
                },
            ),
        ):
            with patch("trr_backend.repositories.social_season_analytics.ingest_season") as ingest_mock:
                response = client.post(
                    f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                    headers={"Authorization": f"Bearer {token}"},
                    json=payload,
                )

    assert response.status_code == 503
    body = response.json()
    assert body["detail"]["code"] == "SOCIAL_WORKER_UNAVAILABLE"
    assert body["detail"]["worker_health"]["healthy"] is False
    ingest_mock.assert_not_called()


def test_ingest_with_queue_enabled_and_worker_present_succeeds(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    payload = {"source_scope": "bravo", "platforms": ["instagram"]}
    expected = {
        "season_id": season_id,
        "run_id": "run-healthy",
        "status": "queued",
        "stages": ["posts", "comments"],
        "queued_or_started_jobs": 2,
        "summary": {"total_jobs": 2},
    }

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
        with patch(
            "trr_backend.repositories.social_season_analytics.assert_worker_available_when_queue_enabled",
            return_value={"healthy": True, "healthy_workers": 1},
        ) as worker_guard:
            with patch("trr_backend.repositories.social_season_analytics.ingest_season", return_value=expected):
                response = client.post(
                    f"/api/v1/admin/socials/seasons/{season_id}/ingest",
                    headers={"Authorization": f"Bearer {token}"},
                    json=payload,
                )

    assert response.status_code == 200
    assert response.json()["run_id"] == "run-healthy"
    assert response.json()["execution_mode"] == "queued"
    assert response.json()["execution_mode_canonical"] == "queued"
    assert response.json()["execution_mode_legacy"] == "queue"
    assert response.json()["execution_mode_deprecation"]["field"] == "execution_mode_legacy"
    worker_guard.assert_called_once_with()


def test_get_worker_health_endpoint_returns_health_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "healthy": True,
        "healthy_workers": 2,
        "active_workers": 2,
        "total_workers": 2,
        "stale_after_seconds": 180,
        "workers": [{"worker_id": "social-worker:host:1"}],
        "reason": None,
    }

    with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
        with patch("trr_backend.repositories.social_season_analytics.get_worker_health", return_value=expected):
            response = client.get(
                "/api/v1/admin/socials/ingest/worker-health",
                headers={"Authorization": f"Bearer {token}"},
            )

    assert response.status_code == 200
    body = response.json()
    assert body["queue_enabled"] is True
    assert body["healthy"] is True
    assert body["healthy_workers"] == 2


def test_purge_inactive_workers_endpoint_returns_counts(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "stale_after_seconds": 180,
        "active_workers": 2,
        "total_workers_before": 30,
        "total_workers_after": 2,
        "deleted_workers": 28,
        "reason": None,
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.purge_inactive_workers", return_value=expected
    ) as mocked:
        response = client.post(
            "/api/v1/admin/socials/ingest/workers/purge-inactive",
            headers={"Authorization": f"Bearer {token}"},
            json={"stale_after_seconds": 180},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_args.kwargs["stale_after_seconds"] == 180


def test_purge_inactive_workers_endpoint_returns_500_on_unhandled_error(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.social_season_analytics.purge_inactive_workers",
        side_effect=RuntimeError("purge workers failed"),
    ):
        response = client.post(
            "/api/v1/admin/socials/ingest/workers/purge-inactive",
            headers={"Authorization": f"Bearer {token}"},
            json={},
        )

    assert response.status_code == 500
    assert response.json()["detail"] == "purge workers failed"


def test_get_queue_status_endpoint_returns_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "queue_enabled": True,
        "workers": {
            "healthy": True,
            "healthy_workers": 1,
            "active_workers": 1,
            "total_workers": 1,
            "stale_after_seconds": 180,
            "workers": [{"worker_id": "social-worker:host:1"}],
            "reason": None,
        },
        "queue": {
            "by_status": {
                "queued": 1,
                "pending": 0,
                "running": 0,
                "retrying": 0,
                "failed": 0,
                "cancelled": 0,
                "completed": 0,
            },
            "by_platform": {"instagram": {"queued": 1}},
            "by_job_type": {"ingest_posts": {"queued": 1}},
            "recent_failures": [],
            "stuck_jobs": [],
            "stuck_jobs_total": 0,
            "runs_by_status": {
                "queued": 1,
                "pending": 0,
                "running": 0,
                "retrying": 0,
                "failed": 0,
                "cancelled": 0,
                "completed": 0,
            },
            "runs_total": 1,
        },
    }

    with patch("trr_backend.repositories.social_season_analytics.get_queue_status", return_value=expected):
        response = client.get(
            "/api/v1/admin/socials/ingest/queue-status",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == expected


def test_get_queue_status_endpoint_forwards_fresh_flag(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.social_season_analytics.get_queue_status", return_value={"ok": True}
    ) as mocked:
        response = client.get(
            "/api/v1/admin/socials/ingest/queue-status?fresh=true",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    assert mocked.call_args.kwargs.get("fresh") is True


def test_get_queue_status_endpoint_returns_500_on_unhandled_error(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.social_season_analytics.get_queue_status",
        side_effect=RuntimeError("queue status failed"),
    ):
        response = client.get(
            "/api/v1/admin/socials/ingest/queue-status",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 500
    assert response.json()["detail"] == "queue status failed"


def test_get_run_progress_endpoint_returns_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    run_id = str(uuid4())
    expected = {
        "season_id": season_id,
        "run_id": run_id,
        "stages": {"posts": {"jobs_total": 2}},
        "per_handle": [],
        "recent_log": [],
        "worker_runtime": {"active_workers_now": 1},
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.get_run_progress_snapshot", return_value=expected
    ) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/ingest/runs/{run_id}/progress?recent_log_limit=15",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_args.kwargs["recent_log_limit"] == 15


def test_get_week_live_health_endpoint_returns_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    expected = {
        "season_id": season_id,
        "day_account_rows": [
            {"day": "2025-11-25", "platform": "instagram", "account": "bravotv", "posts": 1, "comments": 2, "likes": 3}
        ],
        "asset_health": [
            {"asset": "images", "scraped": 3, "saved": 2},
        ],
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.get_week_live_health_snapshot", return_value=expected
    ) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/11/live-health?platforms=instagram,tiktok&timezone=America/New_York&source_scope=bravo",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_args.kwargs["week_index"] == 11
    assert mocked.call_args.kwargs["platforms"] == ["instagram", "tiktok"]
    assert mocked.call_args.kwargs["timezone"] == "America/New_York"


def test_get_health_dot_endpoint_returns_lightweight_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "queue_enabled": True,
        "workers": {
            "healthy": True,
            "healthy_workers": 2,
        },
        "queue": {
            "by_status": {
                "running": 3,
                "pending": 4,
                "queued": 5,
                "failed": 1,
            },
        },
        "updated_at": "2026-02-28T12:00:00Z",
    }

    with patch("trr_backend.repositories.social_season_analytics.get_queue_status") as mocked:
        mocked.return_value = {
            "queue_enabled": True,
            "workers": {
                "healthy": True,
                "healthy_workers": 2,
            },
            "queue": {
                "by_status": {
                    "running": 3,
                    "pending": 4,
                    "queued": 5,
                    "failed": 1,
                },
            },
        }
        response = client.get(
            "/api/v1/admin/socials/ingest/health-dot",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["queue_enabled"] is expected["queue_enabled"]
    assert body["workers"] == expected["workers"]
    assert body["queue"] == expected["queue"]
    assert isinstance(body.get("updated_at"), str)
    mocked.assert_called_once_with(
        include_recent_failures=False,
        include_stuck_jobs=False,
        include_runs_summary=False,
    )


def test_get_health_dot_endpoint_returns_500_on_unhandled_error(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.social_season_analytics.get_queue_status",
        side_effect=RuntimeError("health dot failed"),
    ):
        response = client.get(
            "/api/v1/admin/socials/ingest/health-dot",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 500
    assert response.json()["detail"] == "health dot failed"


def test_cancel_stuck_jobs_endpoint_accepts_targeted_job_ids(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    job_id = str(uuid4())
    expected = {
        "requested_job_ids_count": 1,
        "cancelled_jobs": 1,
        "cancelled_job_ids": [job_id],
        "affected_run_ids": [],
        "stuck_jobs_remaining": 0,
    }

    with patch("trr_backend.repositories.social_season_analytics.cancel_stuck_jobs", return_value=expected) as mocked:
        response = client.post(
            "/api/v1/admin/socials/ingest/stuck-jobs/cancel",
            headers={"Authorization": f"Bearer {token}"},
            json={"job_ids": [job_id]},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_args.kwargs["job_ids"] == [job_id]


def test_cancel_stuck_jobs_endpoint_defaults_to_clear_all(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "requested_job_ids_count": 0,
        "cancelled_jobs": 3,
        "cancelled_job_ids": [str(uuid4()), str(uuid4()), str(uuid4())],
        "affected_run_ids": [str(uuid4())],
        "stuck_jobs_remaining": 0,
    }

    with patch("trr_backend.repositories.social_season_analytics.cancel_stuck_jobs", return_value=expected) as mocked:
        response = client.post(
            "/api/v1/admin/socials/ingest/stuck-jobs/cancel",
            headers={"Authorization": f"Bearer {token}"},
            json={},
        )

    assert response.status_code == 200
    assert response.json()["cancelled_jobs"] == 3
    assert mocked.call_args.kwargs["job_ids"] is None


def test_cancel_stuck_jobs_endpoint_returns_500_on_unhandled_error(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.social_season_analytics.cancel_stuck_jobs",
        side_effect=RuntimeError("cancel stuck failed"),
    ):
        response = client.post(
            "/api/v1/admin/socials/ingest/stuck-jobs/cancel",
            headers={"Authorization": f"Bearer {token}"},
            json={},
        )

    assert response.status_code == 500
    assert response.json()["detail"] == "cancel stuck failed"


def test_cancel_active_jobs_endpoint_returns_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "cancelled_jobs": 5,
        "cancelled_job_ids": [str(uuid4())],
        "affected_run_ids": [str(uuid4())],
        "active_jobs_remaining": 0,
    }

    with patch("trr_backend.repositories.social_season_analytics.cancel_active_jobs", return_value=expected) as mocked:
        response = client.post(
            "/api/v1/admin/socials/ingest/active-jobs/cancel",
            headers={"Authorization": f"Bearer {token}"},
            json={},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_count == 1


def test_cancel_active_jobs_endpoint_returns_500_on_unhandled_error(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.social_season_analytics.cancel_active_jobs",
        side_effect=RuntimeError("cancel active failed"),
    ):
        response = client.post(
            "/api/v1/admin/socials/ingest/active-jobs/cancel",
            headers={"Authorization": f"Bearer {token}"},
            json={},
        )

    assert response.status_code == 500
    assert response.json()["detail"] == "cancel active failed"


def test_get_worker_detail_endpoint_returns_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "worker": {"worker_id": "social-worker:test"},
        "current_job": {"id": str(uuid4()), "run_id": str(uuid4()), "platform": "twitter"},
        "run": {"run_id": str(uuid4()), "status": "running"},
        "currently_scraping": "comments_scan",
        "progress_made": {"items_found": 12},
    }

    with patch("trr_backend.repositories.social_season_analytics.get_worker_detail", return_value=expected):
        response = client.get(
            "/api/v1/admin/socials/ingest/workers/social-worker:test/detail",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == expected


def test_get_worker_detail_endpoint_returns_404_when_worker_missing(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.social_season_analytics.get_worker_detail",
        side_effect=ValueError("worker_not_found"),
    ):
        response = client.get(
            "/api/v1/admin/socials/ingest/workers/social-worker:missing/detail",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 404
    assert response.json()["detail"] == "Worker not found"


def test_get_worker_detail_endpoint_returns_500_on_unhandled_error(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.social_season_analytics.get_worker_detail",
        side_effect=RuntimeError("worker detail failed"),
    ):
        response = client.get(
            "/api/v1/admin/socials/ingest/workers/social-worker:test/detail",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 500
    assert response.json()["detail"] == "worker detail failed"


def test_debug_job_endpoint_returns_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    job_id = str(uuid4())
    expected = {
        "job_id": job_id,
        "run_id": str(uuid4()),
        "model_used": "gpt-5.3-codex",
        "fallback_used": False,
        "analysis": {"root_cause": "x", "confidence": 0.8, "files_touched": [], "tests_to_run": []},
        "patch_unified_diff": "--- a/api/routers/socials.py\n+++ b/api/routers/socials.py\n",
        "apply": {
            "enabled": False,
            "requested": False,
            "applied": False,
            "check_ok": False,
            "error": None,
            "files_changed": [],
        },
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.debug_ingest_job_with_openai",
        return_value=expected,
    ) as mocked:
        response = client.post(
            f"/api/v1/admin/socials/ingest/jobs/{job_id}/debug",
            headers={"Authorization": f"Bearer {token}"},
            json={"apply_patch": True, "confirm_apply": True, "include_context": False},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_args.kwargs == {
        "apply_patch": True,
        "confirm_apply": True,
        "include_context": False,
    }


def test_debug_job_endpoint_returns_apply_blocked_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    job_id = str(uuid4())
    expected = {
        "job_id": job_id,
        "run_id": str(uuid4()),
        "model_used": "gpt-5.3-codex",
        "fallback_used": False,
        "analysis": {"root_cause": "x", "confidence": 0.8, "files_touched": [], "tests_to_run": []},
        "patch_unified_diff": "",
        "apply": {
            "enabled": False,
            "requested": True,
            "applied": False,
            "check_ok": False,
            "error": "Patch apply is disabled by server configuration.",
            "files_changed": [],
        },
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.debug_ingest_job_with_openai",
        return_value=expected,
    ):
        response = client.post(
            f"/api/v1/admin/socials/ingest/jobs/{job_id}/debug",
            headers={"Authorization": f"Bearer {token}"},
            json={"apply_patch": True, "confirm_apply": True},
        )

    assert response.status_code == 200
    assert response.json()["apply"]["error"] == "Patch apply is disabled by server configuration."


def test_debug_job_endpoint_returns_apply_success_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    job_id = str(uuid4())
    expected = {
        "job_id": job_id,
        "run_id": str(uuid4()),
        "model_used": "gpt-5.3-codex",
        "fallback_used": False,
        "analysis": {
            "root_cause": "x",
            "confidence": 0.8,
            "files_touched": ["api/routers/socials.py"],
            "tests_to_run": [],
        },
        "patch_unified_diff": "--- a/api/routers/socials.py\n+++ b/api/routers/socials.py\n",
        "apply": {
            "enabled": True,
            "requested": True,
            "applied": True,
            "check_ok": True,
            "error": None,
            "files_changed": ["api/routers/socials.py"],
        },
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.debug_ingest_job_with_openai",
        return_value=expected,
    ):
        response = client.post(
            f"/api/v1/admin/socials/ingest/jobs/{job_id}/debug",
            headers={"Authorization": f"Bearer {token}"},
            json={"apply_patch": True, "confirm_apply": True},
        )

    assert response.status_code == 200
    assert response.json()["apply"]["applied"] is True


def test_debug_job_endpoint_returns_404_for_missing_job(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    job_id = str(uuid4())

    with patch(
        "trr_backend.repositories.social_season_analytics.debug_ingest_job_with_openai",
        side_effect=ValueError("job_not_found"),
    ):
        response = client.post(
            f"/api/v1/admin/socials/ingest/jobs/{job_id}/debug",
            headers={"Authorization": f"Bearer {token}"},
            json={},
        )

    assert response.status_code == 404
    assert response.json()["detail"] == "Job not found"


def test_debug_job_endpoint_returns_503_when_openai_key_missing(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    job_id = str(uuid4())

    with patch(
        "trr_backend.repositories.social_season_analytics.debug_ingest_job_with_openai",
        side_effect=ValueError("openai_api_key_missing"),
    ):
        response = client.post(
            f"/api/v1/admin/socials/ingest/jobs/{job_id}/debug",
            headers={"Authorization": f"Bearer {token}"},
            json={},
        )

    assert response.status_code == 503
    assert response.json()["detail"] == "OPENAI_API_KEY is not configured"


def test_debug_job_endpoint_returns_500_on_unhandled_error(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    job_id = str(uuid4())

    with patch(
        "trr_backend.repositories.social_season_analytics.debug_ingest_job_with_openai",
        side_effect=RuntimeError("debug failed"),
    ):
        response = client.post(
            f"/api/v1/admin/socials/ingest/jobs/{job_id}/debug",
            headers={"Authorization": f"Bearer {token}"},
            json={},
        )

    assert response.status_code == 500
    assert response.json()["detail"] == "debug failed"


def test_get_comments_coverage_endpoint(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    expected = {
        "season_id": season_id,
        "total_saved_comments": 10,
        "total_reported_comments": 10,
        "coverage_pct": 100.0,
        "up_to_date": True,
        "stale_posts_count": 0,
        "posts_scanned": 5,
        "by_platform": {"instagram": {"saved_comments": 10, "reported_comments": 10}},
        "evaluated_at": "2026-02-24T12:00:00+00:00",
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.get_comments_coverage",
        return_value=expected,
    ) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics/comments-coverage"
            "?source_scope=bravo&platforms=instagram,twitter&timezone=America/New_York",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["total_saved_comments"] == 10
    assert mocked.call_args.kwargs["source_scope"] == "bravo"
    assert mocked.call_args.kwargs["platforms"] == ["instagram", "twitter"]


def test_get_analytics_endpoint_accepts_facebook_threads_platform_filters(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    expected = {
        "window": {"week": None},
        "totals": {"posts": 0, "comments": 0},
        "weekly": [],
        "platforms": {},
        "rows": [],
    }

    with patch("trr_backend.repositories.social_season_analytics.get_analytics", return_value=expected) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics"
            "?source_scope=bravo&platforms=facebook,threads&timezone=America/New_York",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert mocked.call_args.kwargs["platforms"] == ["facebook", "threads"]


def test_get_week_detail_endpoint_returns_facebook_threads_platform_maps(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    payload = {
        "season_id": season_id,
        "week": {"week_index": 2, "label": "Week 2", "start": "2026-01-10T00:00:00Z", "end": "2026-01-17T00:00:00Z"},
        "platforms": {
            "facebook": {"posts": [], "totals": {"posts": 0, "total_comments": 0, "total_engagement": 0}},
            "threads": {"posts": [], "totals": {"posts": 0, "total_comments": 0, "total_engagement": 0}},
        },
        "totals": {"posts": 0, "total_comments": 0, "total_engagement": 0},
    }

    with patch("trr_backend.repositories.social_season_analytics.get_week_detail", return_value=payload):
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics/week/2?source_scope=bravo",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert "facebook" in body["platforms"]
    assert "threads" in body["platforms"]


def test_export_csv(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    snapshot = {
        "rows": [
            {
                "week_index": 1,
                "platform": "instagram",
                "kind": "comment",
                "source_id": "abc",
                "timestamp": "2026-02-10T10:00:00+00:00",
                "author": "viewer",
                "url": "https://example.com",
                "engagement": 10,
                "sentiment": "positive",
                "text": "Great episode",
            }
        ]
    }

    with patch("trr_backend.repositories.social_season_analytics.get_analytics", return_value=snapshot):
        with patch("trr_backend.repositories.social_season_analytics.build_csv", return_value="a,b\n1,2\n"):
            response = client.get(
                f"/api/v1/admin/socials/seasons/{season_id}/analytics/export.csv",
                headers={"Authorization": f"Bearer {token}"},
            )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    assert "attachment; filename=" in response.headers["content-disposition"]
    assert "1,2" in response.text


def test_get_analytics_allows_week_zero(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    expected = {
        "window": {"week": 0},
        "summary": {},
        "weekly": [],
        "weekly_platform_engagement": [],
        "weekly_daily_activity": [],
        "platform_breakdown": [],
        "themes": {"positive": [], "negative": []},
        "leaderboards": {"bravo_content": [], "viewer_discussion": []},
        "jobs": [],
    }

    with patch("trr_backend.repositories.social_season_analytics.get_analytics", return_value=expected) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics?week=0",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["window"]["week"] == 0
    assert "weekly_platform_engagement" in response.json()
    assert "weekly_daily_activity" in response.json()
    assert mocked.call_args.kwargs["week"] == 0


def test_get_analytics_include_slices_forwarded(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    expected = {
        "window": {"week": None},
        "summary": {},
        "weekly": [],
        "weekly_platform_posts": [],
        "weekly_platform_engagement": [],
        "weekly_daily_activity": [],
        "platform_breakdown": [],
        "themes": {"positive": [], "negative": []},
        "leaderboards": {"bravo_content": [], "viewer_discussion": []},
        "jobs": [],
        "benchmark": {"week_index": 1},
    }

    with patch("trr_backend.repositories.social_season_analytics.get_analytics", return_value=expected) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/analytics?include=rows,benchmark",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["benchmark"]["week_index"] == 1
    assert mocked.call_args.kwargs["include_rows"] is True
    assert mocked.call_args.kwargs["include_flags"] is False
    assert mocked.call_args.kwargs["include_schedule"] is False
    assert mocked.call_args.kwargs["include_benchmark"] is True


def test_get_ingest_run_summary_endpoint(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    summaries = [
        {
            "run_id": str(uuid4()),
            "status": "completed",
            "source_scope": "bravo",
            "success_rate_pct": 100.0,
        }
    ]

    with patch("trr_backend.repositories.social_season_analytics.list_run_summaries", return_value=summaries) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/ingest/runs/summary?source_scope=bravo&limit=20",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["season_id"] == season_id
    assert body["filters"]["source_scope"] == "bravo"
    assert body["summaries"] == summaries
    assert mocked.call_args.kwargs["source_scope"] == "bravo"
    assert mocked.call_args.kwargs["limit"] == 20


def test_export_pdf(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())

    snapshot = {
        "summary": {
            "show_id": str(uuid4()),
            "season_number": 10,
        }
    }

    with patch("trr_backend.repositories.social_season_analytics.get_analytics", return_value=snapshot):
        with patch(
            "trr_backend.repositories.social_season_analytics.build_pdf",
            return_value=b"%PDF-1.4\n...",
        ):
            with patch(
                "trr_backend.repositories.social_season_analytics.pdf_filename",
                return_value="social_report_test.pdf",
            ):
                response = client.get(
                    f"/api/v1/admin/socials/seasons/{season_id}/analytics/export.pdf",
                    headers={"Authorization": f"Bearer {token}"},
                )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/pdf")
    assert response.headers["content-disposition"] == 'attachment; filename="social_report_test.pdf"'
    assert response.content.startswith(b"%PDF")


def test_get_tiktok_overview_endpoint(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    expected = {
        "season_id": season_id,
        "kpis": {"post_count": 10, "views": 1000, "likes": 100, "comments": 40, "shares": 20, "saves": 5},
    }
    with patch("trr_backend.repositories.social_season_analytics.get_tiktok_overview", return_value=expected) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/tiktok/overview?sound_id=7540327234013301517",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["kpis"]["post_count"] == 10
    assert mocked.call_args.kwargs["sound_id"] == "7540327234013301517"


def test_get_tiktok_sound_posts_endpoint(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    expected = {
        "season_id": season_id,
        "sound_id": "7540327234013301517",
        "posts": [{"platform_post_id": "123"}],
    }
    with patch(
        "trr_backend.repositories.social_season_analytics.get_tiktok_sound_posts", return_value=expected
    ) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/tiktok/sounds/7540327234013301517/posts?limit=25",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["sound_id"] == "7540327234013301517"
    assert mocked.call_args.kwargs["sound_id"] == "7540327234013301517"
    assert mocked.call_args.kwargs["limit"] == 25


def test_get_tiktok_sounds_endpoint(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    expected = {
        "season_id": season_id,
        "sounds": [{"sound_id": "7540327234013301517", "creator_post_count": 3}],
    }
    with patch("trr_backend.repositories.social_season_analytics.get_tiktok_sounds", return_value=expected) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/tiktok/sounds?search=lisa&limit=20",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["sounds"][0]["sound_id"] == "7540327234013301517"
    assert mocked.call_args.kwargs["search"] == "lisa"
    assert mocked.call_args.kwargs["limit"] == 20


def test_get_tiktok_content_health_endpoint(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    expected = {
        "season_id": season_id,
        "thresholds": {"median_saves": 12},
        "posts": [{"post_id": "abc", "reason_flags": ["low_saves"]}],
    }
    with patch(
        "trr_backend.repositories.social_season_analytics.get_tiktok_content_health",
        return_value=expected,
    ) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/tiktok/content-health?hashtag=rhoslc",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["posts"][0]["post_id"] == "abc"
    assert mocked.call_args.kwargs["hashtag"] == "rhoslc"
