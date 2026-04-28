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


def test_list_cast_photo_tags_uses_repository(client, monkeypatch):
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    photo_id = str(uuid4())
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    row = {
        "cast_photo_id": photo_id,
        "people_names": ["Person One"],
        "people_ids": ["person-1"],
        "people_count": 1,
        "people_count_source": "manual",
        "detector": None,
        "created_at": "2026-04-27T12:00:00+00:00",
        "updated_at": "2026-04-27T12:00:00+00:00",
        "created_by_firebase_uid": "admin-user",
        "updated_by_firebase_uid": "admin-user",
    }

    with patch(
        "api.routers.admin_cast_photos.cast_photo_tags_repo.list_tag_rows_by_photo_ids",
        return_value=[row],
    ) as list_tags:
        response = client.get(
            f"/api/v1/admin/cast-photos/tags?photo_ids={photo_id}",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["tags"] == [row]
    list_tags.assert_called_once_with([photo_id])


def test_list_cast_photo_tag_photo_ids_uses_repository(client, monkeypatch):
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    photo_id = str(uuid4())

    with patch(
        "api.routers.admin_cast_photos.cast_photo_tags_repo.list_photo_ids_by_person_id",
        return_value=[photo_id],
    ) as list_photo_ids:
        response = client.get(
            "/api/v1/admin/cast-photos/tags/photo-ids?person_id=person-1",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == {"photo_ids": [photo_id]}
    list_photo_ids.assert_called_once_with("person-1")


def test_upsert_cast_photo_tags_uses_repository(client, monkeypatch):
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    photo_id = str(uuid4())
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    row = {
        "cast_photo_id": photo_id,
        "people_names": ["Person One"],
        "people_ids": ["person-1"],
        "people_count": 1,
        "people_count_source": "manual",
        "detector": None,
        "created_at": "2026-04-27T12:00:00+00:00",
        "updated_at": "2026-04-27T12:00:00+00:00",
        "created_by_firebase_uid": "admin-user",
        "updated_by_firebase_uid": "admin-user",
    }

    with patch(
        "api.routers.admin_cast_photos.cast_photo_tags_repo.upsert_cast_photo_tag_row",
        return_value=row,
    ) as upsert_tag:
        response = client.post(
            "/api/v1/admin/cast-photos/tags",
            json={
                "cast_photo_id": photo_id,
                "people_names": ["Person One"],
                "people_ids": ["person-1"],
                "people_count": 1,
                "people_count_source": "manual",
                "created_by_firebase_uid": "admin-user",
                "updated_by_firebase_uid": "admin-user",
            },
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json()["tag"] == row
    upsert_tag.assert_called_once_with(
        cast_photo_id=photo_id,
        people_names=["Person One"],
        people_ids=["person-1"],
        people_count=1,
        people_count_source="manual",
        detector=None,
        created_by_firebase_uid="admin-user",
        updated_by_firebase_uid="admin-user",
    )


def test_update_cast_photo_face_boxes_uses_repository(client, monkeypatch):
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    photo_id = str(uuid4())
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    face_boxes = [{"index": 1, "x": 0.1, "y": 0.2, "width": 0.3, "height": 0.4}]

    with patch(
        "api.routers.admin_cast_photos.cast_photo_tags_repo.set_cast_photo_face_boxes",
        return_value=True,
    ) as set_face_boxes:
        response = client.post(
            f"/api/v1/admin/cast-photos/{photo_id}/face-boxes",
            json={"face_boxes": face_boxes},
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == {"updated": True}
    set_face_boxes.assert_called_once_with(photo_id, face_boxes)
