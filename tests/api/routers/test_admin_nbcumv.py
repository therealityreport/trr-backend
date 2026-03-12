from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch
from uuid import uuid4

import jwt
import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from api.main import app
from api.routers.admin_nbcumv import (
    NbcumvPreviewRequest,
    _extract_caption_people,
    _match_people_names,
    _postgres_text_array_literal,
)


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


def test_preview_request_requires_at_least_one_filter() -> None:
    with pytest.raises(ValidationError):
        NbcumvPreviewRequest()


def test_extract_caption_people_parses_pictured_block() -> None:
    caption = (
        "WATCH WHAT HAPPENS LIVE WITH ANDY COHEN -- Episode 23037 -- "
        "Pictured: Andy Cohen, Maura Higgins and Rob Rausch -- (Photo by: Charles Sykes/Bravo)"
    )

    assert _extract_caption_people(caption) == ["Andy Cohen", "Maura Higgins", "Rob Rausch"]


def test_match_people_names_marks_ambiguous_and_unmatched() -> None:
    index = {
        "andy cohen": [{"person_id": "p1", "full_name": "Andy Cohen"}],
        "rob rausch": [
            {"person_id": "p2", "full_name": "Rob Rausch"},
            {"person_id": "p3", "full_name": "Rob Rausch"},
        ],
    }

    result = _match_people_names(index, ["Andy Cohen", "Rob Rausch", "Maura Higgins"])

    assert result["resolved"] == [{"person_id": "p1", "full_name": "Andy Cohen", "matched_name": "Andy Cohen"}]
    assert result["ambiguous"] == ["Rob Rausch"]
    assert result["unmatched"] == ["Maura Higgins"]


def test_postgres_text_array_literal_serializes_aliases() -> None:
    assert _postgres_text_array_literal(["nbcu", "nbcumv", "nbc media village"]) == (
        '{"nbcu","nbcumv","nbc media village"}'
    )


def test_preview_endpoint_returns_resolved_people_and_existing_asset(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    mock_db = MagicMock()

    image = {
        "lbx_id": "70761513",
        "lbx_filename": "NUP_209993_01872.JPG",
        "location": "https://lightbox-thumbnails.s3.us-west-2.amazonaws.com/NUP_209993/x/NUP_209993_01872.JPG",
        "lbx_caption": (
            "WATCH WHAT HAPPENS LIVE WITH ANDY COHEN -- Pictured: Mac Forehand -- (Photo by: Charles Sykes/Bravo)"
        ),
        "showIds": ["show-1"],
    }
    getty_asset = {
        "detail_url": "https://www.gettyimages.com/detail/news-photo/x/2264300032",
        "people": [{"text": "Mac Forehand", "id": 1}],
        "object_name": "NUP_209993_01872.JPG",
    }

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_nbcumv._ensure_sources"):
            with patch("api.routers.admin_nbcumv.nbcumv.search_images", return_value=[image]):
                with patch(
                    "api.routers.admin_nbcumv._load_eligible_people_index",
                    return_value={"mac forehand": [{"person_id": "person-1", "full_name": "Mac Forehand"}]},
                ):
                    with patch("api.routers.admin_nbcumv.getty.resolve_asset_by_object_name", return_value=getty_asset):
                        with patch(
                            "api.routers.admin_nbcumv._existing_asset_by_nbcumv_id",
                            return_value={"id": "asset-1"},
                        ):
                            with patch(
                                "api.routers.admin_nbcumv._existing_person_links",
                                return_value=["person-1"],
                            ):
                                response = client.post(
                                    "/api/v1/admin/nbcumv/preview",
                                    headers={"Authorization": f"Bearer {token}"},
                                    json={"filename": "NUP_209993_01872.JPG"},
                                )

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 1
    item = payload["items"][0]
    assert item["lbx_filename"] == "NUP_209993_01872.JPG"
    assert item["person_ids"] == ["person-1"]
    assert item["already_imported"] is True
    assert item["existing_asset_id"] == "asset-1"
    assert item["existing_person_ids"] == ["person-1"]


def test_import_endpoint_creates_gallery_links_for_resolved_people(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    mock_db = MagicMock()
    person_id = str(uuid4())

    image = {
        "lbx_id": "70761513",
        "lbx_filename": "NUP_209993_01872.JPG",
        "location": "https://lightbox-thumbnails.s3.us-west-2.amazonaws.com/NUP_209993/x/NUP_209993_01872.JPG",
        "lbx_caption": (
            "WATCH WHAT HAPPENS LIVE WITH ANDY COHEN -- Pictured: Mac Forehand -- (Photo by: Charles Sykes/Bravo)"
        ),
        "showIds": ["show-1"],
        "created": "2026-03-05T00:00:00.000Z",
        "liveDate": "2026-03-04T00:00:00.000Z",
    }
    asset = {"id": "asset-1", "hosted_url": "https://cdn.example.com/media/asset-1.jpg"}

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_nbcumv._ensure_sources"):
            with patch("api.routers.admin_nbcumv._load_eligible_people_index", return_value={}):
                with patch("api.routers.admin_nbcumv.nbcumv.fetch_image_by_identity", return_value=image):
                    with patch("api.routers.admin_nbcumv.getty.fetch_asset_detail", return_value=None):
                        with patch("api.routers.admin_nbcumv._existing_asset_by_nbcumv_id", return_value=None):
                            with patch(
                                "api.routers.admin_nbcumv.nbcumv.download_hires_image",
                                return_value=(b"jpeg-bytes", "image/jpeg"),
                            ):
                                with patch(
                                    "api.routers.admin_nbcumv.nbcumv.extract_embedded_metadata",
                                    return_value={"dimensions": {"width": 2000, "height": 3000}},
                                ):
                                    with patch("api.routers.admin_nbcumv.get_s3_bucket", return_value="media-bucket"):
                                        with patch("api.routers.admin_nbcumv.get_s3_client", return_value=MagicMock()):
                                            with patch(
                                                "api.routers.admin_nbcumv.upload_bytes_to_s3",
                                                return_value=("etag-1", len(b"jpeg-bytes")),
                                            ):
                                                with patch(
                                                    "api.routers.admin_nbcumv.build_hosted_url",
                                                    return_value="https://cdn.example.com/media/asset-1.jpg",
                                                ):
                                                    with patch(
                                                        "api.routers.admin_nbcumv._hydrate_people",
                                                        return_value=[
                                                            {
                                                                "person_id": person_id,
                                                                "full_name": "Mac Forehand",
                                                            }
                                                        ],
                                                    ):
                                                        with patch(
                                                            "api.routers.admin_nbcumv._upsert_nbcumv_asset",
                                                            return_value=asset,
                                                        ):
                                                            with patch(
                                                                "api.routers.admin_nbcumv._existing_person_links",
                                                                side_effect=[[], [person_id]],
                                                            ):
                                                                with patch(
                                                                    "api.routers.admin_nbcumv.create_media_link_for_entity"
                                                                ) as create_link:
                                                                    with patch(
                                                                        "api.routers.admin_nbcumv.generate_media_asset_variants"
                                                                    ):
                                                                        response = client.post(
                                                                            "/api/v1/admin/nbcumv/import",
                                                                            headers={
                                                                                "Authorization": f"Bearer {token}"
                                                                            },
                                                                            json={
                                                                                "assign_people": True,
                                                                                "items": [
                                                                                    {
                                                                                        "lbx_id": "70761513",
                                                                                        "lbx_filename": (
                                                                                            "NUP_209993_01872.JPG"
                                                                                        ),
                                                                                        "location": image["location"],
                                                                                        "show_ids": ["show-1"],
                                                                                        "person_ids": [person_id],
                                                                                    }
                                                                                ],
                                                                            },
                                                                        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["imported_asset_ids"] == ["asset-1"]
    assert payload["created_gallery_links"] == 1
    assert payload["skipped_duplicates"] == 0
    create_link.assert_called_once()


def test_import_endpoint_skips_download_for_existing_hosted_asset(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    mock_db = MagicMock()
    person_id = str(uuid4())

    image = {
        "lbx_id": "70761513",
        "lbx_filename": "NUP_209993_01872.JPG",
        "location": "https://lightbox-thumbnails.s3.us-west-2.amazonaws.com/NUP_209993/x/NUP_209993_01872.JPG",
        "lbx_caption": (
            "WATCH WHAT HAPPENS LIVE WITH ANDY COHEN -- Pictured: Mac Forehand -- (Photo by: Charles Sykes/Bravo)"
        ),
        "showIds": ["show-1"],
    }
    existing_asset = {
        "id": "asset-1",
        "hosted_url": "https://cdn.example.com/media/asset-1.jpg",
        "hosted_bucket": "media-bucket",
        "hosted_key": "media/ab/asset.jpg",
        "hosted_etag": "etag-1",
        "metadata": {"embedded_file": {"dimensions": {"width": 2000, "height": 3000}}},
    }

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_nbcumv._ensure_sources"):
            with patch("api.routers.admin_nbcumv._load_eligible_people_index", return_value={}):
                with patch("api.routers.admin_nbcumv.nbcumv.fetch_image_by_identity", return_value=image):
                    with patch("api.routers.admin_nbcumv.getty.fetch_asset_detail", return_value=None):
                        with patch(
                            "api.routers.admin_nbcumv._existing_asset_by_nbcumv_id",
                            return_value=existing_asset,
                        ):
                            with patch(
                                "api.routers.admin_nbcumv._hydrate_people",
                                return_value=[
                                    {
                                        "person_id": person_id,
                                        "full_name": "Mac Forehand",
                                    }
                                ],
                            ):
                                with patch(
                                    "api.routers.admin_nbcumv._upsert_nbcumv_asset",
                                    return_value=existing_asset,
                                ):
                                    with patch(
                                        "api.routers.admin_nbcumv._existing_person_links",
                                        side_effect=[[person_id], [person_id]],
                                    ):
                                        with patch(
                                            "api.routers.admin_nbcumv.nbcumv.download_hires_image"
                                        ) as download_image:
                                            with patch("api.routers.admin_nbcumv.create_media_link_for_entity"):
                                                with patch("api.routers.admin_nbcumv.generate_media_asset_variants"):
                                                    response = client.post(
                                                        "/api/v1/admin/nbcumv/import",
                                                        headers={"Authorization": f"Bearer {token}"},
                                                        json={
                                                            "assign_people": True,
                                                            "items": [
                                                                {
                                                                    "lbx_id": "70761513",
                                                                    "lbx_filename": "NUP_209993_01872.JPG",
                                                                    "location": image["location"],
                                                                    "show_ids": ["show-1"],
                                                                    "person_ids": [person_id],
                                                                }
                                                            ],
                                                        },
                                                    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["skipped_duplicates"] == 1
    download_image.assert_not_called()
