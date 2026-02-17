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
    }

    with patch("trr_backend.repositories.social_season_analytics.ingest_season", return_value=expected) as ingest_mock:
        with patch("trr_backend.repositories.social_season_analytics.is_queue_enabled", return_value=True):
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


def test_get_ingest_jobs_supports_run_filters(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")
    season_id = str(uuid4())
    run_id = str(uuid4())

    with patch("trr_backend.repositories.social_season_analytics.list_jobs", return_value=[]) as mocked:
        response = client.get(
            f"/api/v1/admin/socials/seasons/{season_id}/ingest/jobs"
            f"?run_id={run_id}&status=running&platform=instagram",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["run_id"] == run_id
    assert mocked.call_args.kwargs["run_id"] == run_id
    assert mocked.call_args.kwargs["status"] == "running"
    assert mocked.call_args.kwargs["platform"] == "instagram"


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
    assert mocked.call_args.kwargs["week"] == 0


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
