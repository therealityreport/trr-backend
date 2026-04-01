"""Tests for admin cast photo variant endpoint."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch
from uuid import uuid4

import jwt
import pytest
from fastapi.testclient import TestClient

from api.main import app
from trr_backend.media.image_variants import VariantResult


def _make_admin_token(secret: str, subject: str = "admin-1") -> str:
    now = datetime.now(tz=UTC)
    payload = {
        "sub": subject,
        "iat": int(now.timestamp()),
        "nbf": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=5)).timestamp()),
        "role": "service_role",
    }
    return jwt.encode(payload, secret, algorithm="HS256")


@pytest.fixture
def client():
    return TestClient(app)


def test_generate_cast_photo_variants_success(client, monkeypatch):
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    photo_id = str(uuid4())
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    mock_db = MagicMock()

    variants = [
        VariantResult(
            variant_key="card",
            format="webp",
            hosted_url="https://cdn.example.com/cast-photo-variants/x/card.webp",
            width=720,
            height=900,
            bytes=120_000,
            crop_signature="base",
        ),
        VariantResult(
            variant_key="card",
            format="jpg",
            hosted_url="https://cdn.example.com/cast-photo-variants/x/card.jpg",
            width=720,
            height=900,
            bytes=160_000,
            crop_signature="base",
        ),
    ]

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch(
            "api.routers.admin_cast_photos.generate_cast_photo_variants",
            return_value=variants,
        ):
            response = client.post(
                f"/api/v1/admin/cast-photos/{photo_id}/variants",
                json={"force": True},
                headers={"Authorization": f"Bearer {token}"},
            )

    assert response.status_code == 200
    data = response.json()
    assert data["photo_id"] == photo_id
    assert data["generated"] == 2
    assert data["crop_signature"] == "base"
    assert len(data["variants"]) == 2
    assert data["variants"][0]["variant_key"] == "card"


def test_generate_cast_photo_variants_runtime_error_returns_409(client, monkeypatch):
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    photo_id = str(uuid4())
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    mock_db = MagicMock()

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch(
            "api.routers.admin_cast_photos.generate_cast_photo_variants",
            side_effect=RuntimeError("Cast photo not found"),
        ):
            response = client.post(
                f"/api/v1/admin/cast-photos/{photo_id}/variants",
                json={"force": False},
                headers={"Authorization": f"Bearer {token}"},
            )

    assert response.status_code == 409
    assert "Cast photo not found" in response.json()["detail"]
