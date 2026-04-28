from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import jwt
from fastapi.testclient import TestClient

from api.main import app


def _make_admin_token(secret: str) -> str:
    now = datetime.now(tz=UTC)
    return jwt.encode(
        {
            "sub": "admin-1",
            "iat": int(now.timestamp()),
            "nbf": int(now.timestamp()),
            "exp": int((now + timedelta(minutes=5)).timestamp()),
            "role": "admin",
            "email": "admin@example.com",
        },
        secret,
        algorithm="HS256",
    )


def test_get_instagram_profile_detail_route(monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    client = TestClient(app)
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {"profile": {"username": "nasa", "id": "528817151"}}

    with patch(
        "trr_backend.repositories.social_season_analytics.get_instagram_profile_detail",
        return_value=expected,
    ) as mocked:
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/nasa/profile?source_scope=bravo",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_args.kwargs == {"account_handle": "nasa", "source_scope": "bravo"}


def test_get_instagram_profile_relationships_route_forwards_following_type(monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    client = TestClient(app)
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "owner": {"username": "nasa"},
        "relationship_type": "following",
        "items": [],
        "pagination": {"page": 2, "page_size": 10, "total": 0, "total_pages": 1},
    }

    with patch(
        "trr_backend.repositories.social_season_analytics.get_instagram_profile_relationships",
        return_value=expected,
    ) as mocked:
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/nasa/relationships?type=following&page=2&page_size=10",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_args.kwargs == {
        "account_handle": "nasa",
        "source_scope": "bravo",
        "relationship_type": "following",
        "page": 2,
        "page_size": 10,
    }
