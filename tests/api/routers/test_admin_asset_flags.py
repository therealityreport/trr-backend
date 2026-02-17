from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch
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
def client():
    return TestClient(app)


def test_archive_returns_401_without_auth(client):
    mock_db = MagicMock()
    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        response = client.post(
            "/api/v1/admin/assets/archive",
            json={"origin": "media_assets", "asset_id": str(uuid4())},
        )
    assert response.status_code == 401


def test_archive_marks_archived_and_deletes_s3_best_effort(client, monkeypatch):
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    asset_id = str(uuid4())
    mock_db = MagicMock()

    # _fetch_row chain
    fetch_resp = MagicMock()
    fetch_resp.data = [
        {
            "id": asset_id,
            "hosted_bucket": "bucket",
            "hosted_key": "media/aa/aabbcc.jpg",
            "hosted_url": "https://cdn.example/media/aa/aabbcc.jpg",
            "metadata": {},
        }
    ]
    fetch_resp.error = None

    (
        mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value.execute
    ).return_value = fetch_resp

    update_resp = MagicMock()
    update_resp.data = [{"id": asset_id}]
    update_resp.error = None
    (
        mock_db.schema.return_value.table.return_value.update.return_value.eq.return_value.execute
    ).return_value = update_resp

    mock_s3 = MagicMock()

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_asset_flags.get_s3_client", return_value=mock_s3):
            with patch("api.routers.admin_asset_flags.get_s3_bucket", return_value="bucket"):
                response = client.post(
                    "/api/v1/admin/assets/archive",
                    headers={"Authorization": f"Bearer {token}"},
                    json={"origin": "media_assets", "asset_id": asset_id, "reason": "hide"},
                )

    assert response.status_code == 200
    body = response.json()
    assert body["origin"] == "media_assets"
    assert body["asset_id"] == asset_id
    assert body["archived_at"]
    mock_s3.delete_object.assert_called()


def test_star_updates_metadata(client, monkeypatch):
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    asset_id = str(uuid4())
    mock_db = MagicMock()

    fetch_resp = MagicMock()
    fetch_resp.data = [{"id": asset_id, "hosted_bucket": None, "hosted_key": None, "hosted_url": None, "metadata": {}}]
    fetch_resp.error = None
    (
        mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value.execute
    ).return_value = fetch_resp

    update_resp = MagicMock()
    update_resp.data = [{"id": asset_id}]
    update_resp.error = None
    (
        mock_db.schema.return_value.table.return_value.update.return_value.eq.return_value.execute
    ).return_value = update_resp

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        response = client.post(
            "/api/v1/admin/assets/star",
            headers={"Authorization": f"Bearer {token}"},
            json={"origin": "cast_photos", "asset_id": asset_id, "starred": True},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["origin"] == "cast_photos"
    assert body["asset_id"] == asset_id
    assert body["starred"] is True
    assert body["starred_at"]


def test_content_type_updates_cast_photo_context_type(client, monkeypatch):
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    asset_id = str(uuid4())
    mock_db = MagicMock()

    fetch_resp = MagicMock()
    fetch_resp.data = [{"id": asset_id, "metadata": {}, "context_type": "other"}]
    fetch_resp.error = None
    (
        mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value.execute
    ).return_value = fetch_resp

    update_resp = MagicMock()
    update_resp.data = [{"id": asset_id}]
    update_resp.error = None
    (
        mock_db.schema.return_value.table.return_value.update.return_value.eq.return_value.execute
    ).return_value = update_resp

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        response = client.post(
            "/api/v1/admin/assets/content-type",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "origin": "cast_photos",
                "asset_id": asset_id,
                "content_type": "confessional",
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["origin"] == "cast_photos"
    assert body["asset_id"] == asset_id
    assert body["content_type"] == "CONFESSIONAL"
    assert body["context_type"] == "confessional"
    update_payload = mock_db.schema.return_value.table.return_value.update.call_args.args[0]
    assert update_payload["context_type"] == "confessional"
    assert update_payload["metadata"]["fandom_section_tag"] == "CONFESSIONAL"


def test_content_type_rejects_unsupported_value(client, monkeypatch):
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")
    mock_db = MagicMock()

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        response = client.post(
            "/api/v1/admin/assets/content-type",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "origin": "media_assets",
                "asset_id": str(uuid4()),
                "content_type": "not-a-type",
            },
        )

    assert response.status_code == 400
