"""Tests for season social analytics admin endpoints."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import patch
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


def test_get_season_targets(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")
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
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")
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


def test_ingest_allows_zero_comments_limit(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")
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
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")
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
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")
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


def test_get_ingest_jobs_supports_run_filters(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")
    season_id = str(uuid4())
    run_id = str(uuid4())

    with patch("trr_backend.repositories.social_season_analytics.list_jobs", return_value=[]) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/ingest/jobs?run_id={run_id}&status=running&platform=instagram",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["run_id"] == run_id
    assert mocked.call_args.kwargs["run_id"] == run_id
    assert mocked.call_args.kwargs["status"] == "running"
    assert mocked.call_args.kwargs["platform"] == "instagram"


def test_get_ingest_runs_supports_filters(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")
    season_id = str(uuid4())
    run_id = str(uuid4())

    runs_payload = [
        {
            "id": str(uuid4()),
            "season_id": season_id,
            "status": "completed",
            "source_scope": "bravo",
        }
    ]

    with patch("trr_backend.repositories.social_season_analytics.list_runs", return_value=runs_payload) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/ingest/runs?status=completed&source_scope=bravo&run_id={run_id}&limit=25",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["season_id"] == season_id
    assert body["filters"]["status"] == "completed"
    assert body["filters"]["source_scope"] == "bravo"
    assert body["filters"]["run_id"] == run_id
    assert body["runs"] == runs_payload
    assert mocked.call_args.kwargs["status"] == "completed"
    assert mocked.call_args.kwargs["source_scope"] == "bravo"
    assert mocked.call_args.kwargs["run_id"] == run_id
    assert mocked.call_args.kwargs["limit"] == 25


def test_get_ingest_runs_rejects_invalid_season_id(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

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
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")
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


def test_get_week_detail_endpoint_includes_additive_diagnostics(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")
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


def test_get_post_comments_endpoint_returns_youtube_effective_stats(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")
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


def test_refresh_post_comments_endpoint_returns_latest_post_detail(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")
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
        "trr_backend.repositories.social_season_analytics.refresh_post_comments",
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
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")
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
            f"/api/v1/admin/socials/seasons/{season_id}/instagram/mirror/requeue?source_scope=bravo&limit=500&failed_only=true",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["queued_jobs"] == 8
    assert mocked.call_args.kwargs["source_scope"] == "bravo"
    assert mocked.call_args.kwargs["limit"] == 500
    assert mocked.call_args.kwargs["failed_only"] is True


def test_cancel_ingest_run_endpoint(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")
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
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")
    season_id = str(uuid4())
    payload = {"source_scope": "bravo", "platforms": ["instagram"]}

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

    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")
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


def test_ingest_falls_back_inline_in_dev_when_worker_missing_and_flag_enabled(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories.social_season_analytics import SocialWorkerUnavailableError

    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    monkeypatch.setenv("APP_ENV", "development")
    token = _make_admin_token("test-secret")
    season_id = str(uuid4())
    payload = {
        "source_scope": "bravo",
        "platforms": ["instagram"],
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
    assert body["execution_mode"] == "inline_fallback"
    assert body["status"] == "started"
    assert body["worker_health"]["healthy"] is False
    assert isinstance(body.get("warnings"), list)
    assert any("inline dev fallback" in str(item).lower() for item in (body.get("warnings") or []))
    ingest_mock.assert_called_once()


def test_ingest_keeps_503_when_worker_missing_outside_dev_even_with_fallback_flag(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories.social_season_analytics import SocialWorkerUnavailableError

    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.delenv("TRR_LOCAL_DEV", raising=False)
    monkeypatch.delenv("SOCIAL_ALLOW_INLINE_DEV_FALLBACK", raising=False)
    token = _make_admin_token("test-secret")
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
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")
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
    assert response.json()["execution_mode"] == "queue"
    worker_guard.assert_called_once_with()


def test_get_worker_health_endpoint_returns_health_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")
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


def test_export_csv(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")
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
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")
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
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")
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
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")
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
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")
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
