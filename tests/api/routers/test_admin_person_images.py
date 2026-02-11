"""Tests for admin person images refresh endpoint."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch
from uuid import uuid4

import jwt
import pytest
from fastapi.testclient import TestClient

from api.main import app
from api.routers import admin_person_images


def _make_admin_token(secret: str, subject: str = "admin-1") -> str:
    """Create a valid admin JWT token."""
    now = datetime.now(tz=UTC)
    payload = {
        "sub": subject,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=5)).timestamp()),
        "role": "service_role",
    }
    return jwt.encode(payload, secret, algorithm="HS256")


def _make_allowlist_user_token(secret: str, email: str, subject: str = "user-1") -> str:
    """Create a valid user JWT token with allowlist-able email."""
    now = datetime.now(tz=UTC)
    payload = {
        "sub": subject,
        "email": email,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=5)).timestamp()),
        "role": "authenticated",
    }
    return jwt.encode(payload, secret, algorithm="HS256")


def _mock_media_link_lookup(mock_db: MagicMock, row: dict | None, *, error: object | None = None) -> None:
    """Mock media_links lookup query for facebank seed endpoint tests."""
    mock_response = MagicMock()
    mock_response.data = [row] if row is not None else []
    mock_response.error = error
    query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
    query.execute.return_value = mock_response


@pytest.fixture
def client():
    return TestClient(app)


class TestRefreshPersonImages:
    """Test POST /api/v1/admin/person/{person_id}/refresh-images."""

    def test_returns_401_without_auth(self, client):
        """Unauthenticated requests should return 401."""
        person_id = str(uuid4())

        # Mock the database client to avoid DatabaseConnectionError in CI
        mock_db = MagicMock()
        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            response = client.post(f"/api/v1/admin/person/{person_id}/refresh-images")
        assert response.status_code == 401

    def test_returns_404_for_unknown_person(self, client, monkeypatch):
        """Unknown person_id should return 404."""
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret")

        # Mock the database call to return no person
        mock_db = MagicMock()
        mock_response = MagicMock()
        mock_response.data = []
        mock_response.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = mock_response

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            response = client.post(
                f"/api/v1/admin/person/{person_id}/refresh-images",
                headers={"Authorization": f"Bearer {token}"},
            )

        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    def test_refresh_success_returns_summary(self, client, monkeypatch):
        """Successful refresh returns summary with counts."""
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret")

        person_data = {
            "id": person_id,
            "full_name": "Test Person",
            "external_ids": {"imdb": "nm12345678"},
        }

        # Mock the database
        mock_db = MagicMock()
        mock_response = MagicMock()
        mock_response.data = [person_data]
        mock_response.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = mock_response

        # Mock the module-level functions
        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch(
                "trr_backend.repositories.cast_tmdb.get_cast_tmdb_by_person_id",
                return_value=None,
            ):
                with patch(
                    "trr_backend.ingestion.cast_photo_sources.fetch_all_cast_photos",
                    return_value=[
                        {
                            "person_id": person_id,
                            "source": "imdb",
                            "url": "http://test/1.jpg",
                        },
                    ],
                ):
                    with patch(
                        "trr_backend.repositories.cast_photos.upsert_cast_photos",
                        return_value=[{"id": "p1"}],
                    ):
                        with patch(
                            "api.routers.admin_person_images._mirror_person_photos",
                            return_value=(1, 0),
                        ):
                            with patch(
                                "api.routers.admin_person_images._prune_person_s3_objects",
                                return_value=0,
                            ):
                                response = client.post(
                                    f"/api/v1/admin/person/{person_id}/refresh-images",
                                    headers={"Authorization": f"Bearer {token}"},
                                )

        assert response.status_code == 200
        data = response.json()
        assert data["person_id"] == person_id
        assert data["person_name"] == "Test Person"
        assert data["photos_fetched"] == 1
        assert data["photos_upserted"] == 1
        assert data["photos_mirrored"] == 1

    def test_skip_mirror_option(self, client, monkeypatch):
        """skip_mirror=True should skip S3 mirroring."""
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret")

        person_data = {"id": person_id, "full_name": "Test", "external_ids": {}}

        mock_db = MagicMock()
        mock_response = MagicMock()
        mock_response.data = [person_data]
        mock_response.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = mock_response

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch(
                "trr_backend.repositories.cast_tmdb.get_cast_tmdb_by_person_id",
                return_value=None,
            ):
                with patch(
                    "trr_backend.ingestion.cast_photo_sources.fetch_all_cast_photos",
                    return_value=[],
                ):
                    with patch(
                        "trr_backend.repositories.cast_photos.upsert_cast_photos",
                        return_value=[],
                    ):
                        with patch("api.routers.admin_person_images._mirror_person_photos") as mock_mirror:
                            response = client.post(
                                f"/api/v1/admin/person/{person_id}/refresh-images",
                                json={"skip_mirror": True},
                                headers={"Authorization": f"Bearer {token}"},
                            )

        assert response.status_code == 200
        mock_mirror.assert_not_called()


class TestUpdateFacebankSeed:
    """Test PATCH /api/v1/admin/person/{person_id}/gallery/{link_id}/facebank-seed."""

    def test_allows_allowlist_user(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        monkeypatch.setenv("ADMIN_EMAIL_ALLOWLIST", "admin@example.com")
        monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "internal-secret")
        person_id = str(uuid4())
        link_id = str(uuid4())
        token = _make_allowlist_user_token("test-secret", "admin@example.com")

        mock_db = MagicMock()
        _mock_media_link_lookup(
            mock_db,
            {
                "id": link_id,
                "entity_id": person_id,
                "entity_type": "person",
                "kind": "gallery",
                "facebank_seed": False,
            },
        )

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch(
                "api.routers.admin_person_images.update_media_link_facebank_seed",
                return_value={"id": link_id, "facebank_seed": True},
            ):
                response = client.patch(
                    f"/api/v1/admin/person/{person_id}/gallery/{link_id}/facebank-seed",
                    json={"facebank_seed": True},
                    headers={"Authorization": f"Bearer {token}"},
                )

        assert response.status_code == 200
        data = response.json()
        assert data["link_id"] == link_id
        assert data["person_id"] == person_id
        assert data["facebank_seed"] is True

    def test_rejects_service_role_without_internal_secret_header(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        monkeypatch.setenv("ADMIN_EMAIL_ALLOWLIST", "admin@example.com")
        monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "internal-secret")
        person_id = str(uuid4())
        link_id = str(uuid4())
        token = _make_admin_token("test-secret")

        mock_db = MagicMock()
        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            response = client.patch(
                f"/api/v1/admin/person/{person_id}/gallery/{link_id}/facebank-seed",
                json={"facebank_seed": True},
                headers={"Authorization": f"Bearer {token}"},
            )

        assert response.status_code == 403
        assert "Allowlist admin access required" in response.json()["detail"]

    def test_rejects_service_role_with_invalid_internal_secret_header(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        monkeypatch.setenv("ADMIN_EMAIL_ALLOWLIST", "admin@example.com")
        monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "internal-secret")
        person_id = str(uuid4())
        link_id = str(uuid4())
        token = _make_admin_token("test-secret")

        mock_db = MagicMock()
        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            response = client.patch(
                f"/api/v1/admin/person/{person_id}/gallery/{link_id}/facebank-seed",
                json={"facebank_seed": True},
                headers={
                    "Authorization": f"Bearer {token}",
                    "X-TRR-Internal-Admin-Secret": "wrong-secret",
                },
            )

        assert response.status_code == 403
        assert "Allowlist admin access required" in response.json()["detail"]

    def test_allows_service_role_with_valid_internal_secret_header(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        monkeypatch.setenv("ADMIN_EMAIL_ALLOWLIST", "admin@example.com")
        monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "internal-secret")
        person_id = str(uuid4())
        link_id = str(uuid4())
        token = _make_admin_token("test-secret")

        mock_db = MagicMock()
        _mock_media_link_lookup(
            mock_db,
            {
                "id": link_id,
                "entity_id": person_id,
                "entity_type": "person",
                "kind": "gallery",
                "facebank_seed": False,
            },
        )

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch(
                "api.routers.admin_person_images.update_media_link_facebank_seed",
                return_value={"id": link_id, "facebank_seed": True},
            ):
                response = client.patch(
                    f"/api/v1/admin/person/{person_id}/gallery/{link_id}/facebank-seed",
                    json={"facebank_seed": True},
                    headers={
                        "Authorization": f"Bearer {token}",
                        "X-TRR-Internal-Admin-Secret": "internal-secret",
                    },
                )

        assert response.status_code == 200
        data = response.json()
        assert data["link_id"] == link_id
        assert data["person_id"] == person_id
        assert data["facebank_seed"] is True

    def test_returns_404_when_media_link_not_found(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        monkeypatch.setenv("ADMIN_EMAIL_ALLOWLIST", "admin@example.com")
        person_id = str(uuid4())
        link_id = str(uuid4())
        token = _make_allowlist_user_token("test-secret", "admin@example.com")

        mock_db = MagicMock()
        _mock_media_link_lookup(mock_db, None)

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            response = client.patch(
                f"/api/v1/admin/person/{person_id}/gallery/{link_id}/facebank-seed",
                json={"facebank_seed": True},
                headers={"Authorization": f"Bearer {token}"},
            )

        assert response.status_code == 404
        assert "Media link not found" in response.json()["detail"]

    def test_returns_409_when_media_link_is_not_person_gallery(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        monkeypatch.setenv("ADMIN_EMAIL_ALLOWLIST", "admin@example.com")
        person_id = str(uuid4())
        link_id = str(uuid4())
        token = _make_allowlist_user_token("test-secret", "admin@example.com")

        mock_db = MagicMock()
        _mock_media_link_lookup(
            mock_db,
            {
                "id": link_id,
                "entity_id": person_id,
                "entity_type": "show",
                "kind": "poster",
                "facebank_seed": False,
            },
        )

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            response = client.patch(
                f"/api/v1/admin/person/{person_id}/gallery/{link_id}/facebank-seed",
                json={"facebank_seed": True},
                headers={"Authorization": f"Bearer {token}"},
            )

        assert response.status_code == 409
        assert "not a person gallery image" in response.json()["detail"]

    def test_returns_409_when_media_link_person_mismatch(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        monkeypatch.setenv("ADMIN_EMAIL_ALLOWLIST", "admin@example.com")
        person_id = str(uuid4())
        other_person_id = str(uuid4())
        link_id = str(uuid4())
        token = _make_allowlist_user_token("test-secret", "admin@example.com")

        mock_db = MagicMock()
        _mock_media_link_lookup(
            mock_db,
            {
                "id": link_id,
                "entity_id": other_person_id,
                "entity_type": "person",
                "kind": "gallery",
                "facebank_seed": False,
            },
        )

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            response = client.patch(
                f"/api/v1/admin/person/{person_id}/gallery/{link_id}/facebank-seed",
                json={"facebank_seed": True},
                headers={"Authorization": f"Bearer {token}"},
            )

        assert response.status_code == 409
        assert "does not belong to this person" in response.json()["detail"]

    def test_returns_502_when_update_media_link_fails(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        monkeypatch.setenv("ADMIN_EMAIL_ALLOWLIST", "admin@example.com")
        person_id = str(uuid4())
        link_id = str(uuid4())
        token = _make_allowlist_user_token("test-secret", "admin@example.com")

        mock_db = MagicMock()
        _mock_media_link_lookup(
            mock_db,
            {
                "id": link_id,
                "entity_id": person_id,
                "entity_type": "person",
                "kind": "gallery",
                "facebank_seed": False,
            },
        )

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch(
                "api.routers.admin_person_images.update_media_link_facebank_seed",
                side_effect=RuntimeError("db write failed"),
            ):
                response = client.patch(
                    f"/api/v1/admin/person/{person_id}/gallery/{link_id}/facebank-seed",
                    json={"facebank_seed": True},
                    headers={"Authorization": f"Bearer {token}"},
                )

        assert response.status_code == 502
        assert "Database error updating facebank_seed" in response.json()["detail"]


def test_pick_autocount_url_prefers_hosted_when_available() -> None:
    row = {
        "source": "fandom",
        "image_url": "https://real-housewives.fandom.com/wiki/Special:FilePath/Bad.png",
        "thumb_url": "https://static.wikia.nocookie.net/real-housewives/images/1/1a/Good.png",
        "hosted_url": "https://cdn.example.com/x.png",
    }
    assert admin_person_images._pick_autocount_url(row) == row["hosted_url"]


def test_pick_autocount_urls_normalizes_stale_wikia_revision_path() -> None:
    row = {
        "source": "fandom",
        "thumb_url": "https://static.wikia.nocookie.net/real-housewives/images/0/08/angie_k_s3.jpeg/revision/latest",
    }
    urls = admin_person_images._pick_autocount_urls(row)
    assert urls[0] == "https://static.wikia.nocookie.net/real-housewives/images/0/08/angie_k_s3.jpeg"


def test_pick_autocount_url_prefers_tmdb_image() -> None:
    row = {
        "source": "tmdb",
        "image_url": "https://image.tmdb.org/t/p/original/x.png",
        "hosted_url": "https://cdn.example.com/x.png",
    }
    assert admin_person_images._pick_autocount_url(row) == row["image_url"]


def test_pick_autocount_url_falls_back_to_hosted() -> None:
    row = {
        "source": "fandom",
        "hosted_url": "https://cdn.example.com/x.png",
    }
    assert admin_person_images._pick_autocount_url(row) == row["hosted_url"]
