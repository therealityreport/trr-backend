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
