"""Regression tests for sanitized admin socials internal errors."""

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
        "nbf": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=5)).timestamp()),
        "role": "admin",
        "email": "admin@example.com",
    }
    return jwt.encode(payload, secret, algorithm="HS256")


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def test_socials_internal_errors_do_not_reflect_raw_exception_text(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    season_id = str(uuid4())
    sentinel = "INTERNAL-DB-SECRET-a1b2"
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

    with patch(
        "trr_backend.repositories.social_season_analytics.put_targets",
        side_effect=RuntimeError(sentinel),
    ):
        response = client.put(
            f"/api/v1/admin/socials/seasons/{season_id}/targets",
            headers={"Authorization": f"Bearer {token}"},
            json=payload,
        )

    assert response.status_code == 500
    assert sentinel not in response.text
    assert response.json()["detail"]["code"] == "INTERNAL_ERROR"
