"""Tests for the account hashtag timeline admin endpoint."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

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


def test_get_social_account_profile_hashtag_timeline(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "platform": "instagram",
        "account_handle": "bravotv",
        "years": [
            {"year": 2022, "label": "2022", "order": 1},
            {"year": 2023, "label": "2023", "order": 2},
        ],
        "series": [
            {
                "hashtag": "bravo",
                "display_hashtag": "#bravo",
                "points": [
                    {"year": 2022, "order": 1, "rank": 1, "usage_count": 8, "in_top_ten": True, "segment_id": 1},
                    {"year": 2023, "order": 2, "rank": 2, "usage_count": 7, "in_top_ten": True, "segment_id": 1},
                ],
            }
        ],
        "top_rank_limit": 10,
        "off_chart_rank": 11,
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.get_social_account_profile_hashtag_timeline",
        return_value=expected,
    ) as mocked:
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/bravotv/hashtags/timeline",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["series"][0]["hashtag"] == "bravo"
    assert mocked.call_args.kwargs["platform"] == "instagram"
    assert mocked.call_args.kwargs["account_handle"] == "bravotv"
