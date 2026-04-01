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
        "nbf": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=5)).timestamp()),
        "role": "service_role",
    }
    return jwt.encode(payload, secret, algorithm="HS256")


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def test_upload_show_icon_success(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    mock_db = MagicMock()

    inserted = {
        "id": str(uuid4()),
        "show_key": "rhoslc",
        "filename": "BrackStar.png",
        "s3_key": "icons/rhoslc/brackstar.png",
        "hosted_url": "https://cdn.example.com/icons/rhoslc/brackstar.png",
        "content_type": "image/png",
        "size_bytes": 9,
        "created_by": "admin@trr.com",
        "created_at": "2026-02-27T00:00:00+00:00",
    }

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_icons.get_s3_bucket", return_value="trr-backend"):
            with patch("api.routers.admin_show_icons.get_s3_client", return_value=MagicMock()):
                with patch("api.routers.admin_show_icons.upload_bytes_to_s3", return_value=("etag", 9)):
                    with patch(
                        "api.routers.admin_show_icons.build_hosted_url",
                        return_value="https://cdn.example.com/icons/rhoslc/brackstar.png",
                    ):
                        with patch(
                            "api.routers.admin_show_icons._insert_icon_record",
                            return_value=inserted,
                        ) as insert_mock:
                            response = client.post(
                                "/api/v1/admin/shows/rhoslc/icons",
                                headers={"Authorization": f"Bearer {token}"},
                                files={"file": ("BrackStar.png", b"icon-bytes", "image/png")},
                            )

    assert response.status_code == 200
    payload = response.json()
    assert payload["show_key"] == "rhoslc"
    assert payload["hosted_url"].startswith("https://cdn.example.com/icons/rhoslc/")
    inserted_payload = insert_mock.call_args.args[1]
    assert inserted_payload["show_key"] == "rhoslc"


def test_list_show_icons_returns_array(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    mock_db = MagicMock()

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_icons._list_icon_records", return_value=[]):
            response = client.get(
                "/api/v1/admin/shows/rhoslc/icons",
                headers={"Authorization": f"Bearer {token}"},
            )

    assert response.status_code == 200
    assert response.json() == {"icons": []}


def test_delete_show_icon_removes_s3_and_record(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    mock_db = MagicMock()
    icon_id = str(uuid4())
    mock_s3 = MagicMock()
    existing = {
        "id": icon_id,
        "show_key": "rhoslc",
        "filename": "BrackStar.png",
        "s3_key": "icons/rhoslc/brackstar.png",
        "hosted_url": "https://cdn.example.com/icons/rhoslc/brackstar.png",
        "content_type": "image/png",
        "size_bytes": 9,
        "created_by": "admin@trr.com",
        "created_at": "2026-02-27T00:00:00+00:00",
    }

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_icons._get_icon_record", return_value=existing):
            with patch("api.routers.admin_show_icons._delete_icon_record") as delete_mock:
                with patch("api.routers.admin_show_icons.get_s3_bucket", return_value="trr-backend"):
                    with patch("api.routers.admin_show_icons.get_s3_client", return_value=mock_s3):
                        response = client.delete(
                            f"/api/v1/admin/shows/rhoslc/icons/{icon_id}",
                            headers={"Authorization": f"Bearer {token}"},
                        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["deleted"] is True
    assert payload["s3_deleted"] is True
    mock_s3.delete_object.assert_called_once_with(
        Bucket="trr-backend",
        Key="icons/rhoslc/brackstar.png",
    )
    delete_mock.assert_called_once_with(mock_db, icon_id)
