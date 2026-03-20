"""Tests for admin media asset Getty replacement endpoints."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch
from uuid import uuid4

import jwt
import pytest
from fastapi.testclient import TestClient

from api.main import app
from trr_backend.integrations.picdetective import ReverseImageCandidate
from trr_backend.media.getty_replacement import ResolvedPublicReplacement


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


def test_reverse_image_search_uses_shared_candidate_search(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    asset_id = str(uuid4())
    mock_db = MagicMock()
    (
        mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value.execute.return_value.data
    ) = [
        {
            "id": asset_id,
            "source": "getty",
            "source_url": "https://media.gettyimages.com/source.jpg",
            "width": 1600,
            "height": 900,
            "metadata": {},
        }
    ]

    monkeypatch.setattr(
        "api.routers.admin_media_assets.search_public_replacement_candidates",
        lambda *args, **kwargs: [
            ReverseImageCandidate(
                title="Bravo gallery",
                source_domain="bravotv.com",
                page_url="https://www.bravotv.com/gallery",
                thumbnail_b64=None,
                width=1825,
                height=1217,
            )
        ],
    )

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        response = client.post(
            f"/api/v1/admin/media-assets/{asset_id}/reverse-image-search",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["search_url"] == "https://media.gettyimages.com/source.jpg"
    assert payload["candidates"][0]["source_domain"] == "bravotv.com"


def test_replace_from_url_uses_shared_replacement_pipeline(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    asset_id = str(uuid4())
    mock_db = MagicMock()
    (
        mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value.execute.return_value.data
    ) = [
        {
            "id": asset_id,
            "source": "getty",
            "source_url": "https://media.gettyimages.com/source.jpg",
            "width": 1600,
            "height": 900,
            "hosted_url": None,
            "hosted_key": None,
            "metadata": {},
        }
    ]

    monkeypatch.setattr(
        "api.routers.admin_media_assets.resolve_public_replacement_from_page",
        lambda *args, **kwargs: ResolvedPublicReplacement(
            page_url="https://www.bravotv.com/gallery",
            source_domain="bravotv.com",
            image_url="https://www.bravotv.com/sites/bravo/files/gallery-01.jpg",
            width=1825,
            height=1217,
        ),
    )
    monkeypatch.setattr(
        "api.routers.admin_media_assets.apply_media_asset_replacement",
        lambda *args, **kwargs: {
            "asset_id": asset_id,
            "status": "replaced",
            "new_source": "bravotv.com",
            "new_source_url": "https://www.bravotv.com/gallery",
            "new_hosted_url": "https://cdn.example.com/replaced.jpg",
            "width": 1825,
            "height": 1217,
        },
    )

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        response = client.post(
            f"/api/v1/admin/media-assets/{asset_id}/replace-from-url",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "page_url": "https://www.bravotv.com/gallery",
                "source_domain": "bravotv.com",
                "expected_width": 1600,
                "expected_height": 900,
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "replaced"
    assert payload["new_source"] == "bravotv.com"
    assert payload["new_hosted_url"] == "https://cdn.example.com/replaced.jpg"
