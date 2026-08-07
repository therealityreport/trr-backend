from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock, patch
from uuid import uuid4

import jwt
import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from api.main import app
from api.routers.admin_nbcumv import (
    NbcumvImportItem,
    NbcumvPreviewRequest,
    _extract_caption_people,
    _import_single_item,
    _match_people_names,
    _postgres_text_array_literal,
)


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


def test_preview_request_requires_at_least_one_filter() -> None:
    with pytest.raises(ValidationError):
        NbcumvPreviewRequest()


def test_preview_request_accepts_new_cloudsearch_filters() -> None:
    request = NbcumvPreviewRequest(
        search_text="Brandi Glanville",
        show_name="The Real Housewives Ultimate Girls Trip: Ex-Wives Club",
        meta_type="Episodic",
        season="2",
        episode="207",
        network="Peacock",
    )

    assert request.search_text == "Brandi Glanville"
    assert request.show_name == "The Real Housewives Ultimate Girls Trip: Ex-Wives Club"
    assert request.meta_type == "Episodic"
    assert request.season == "2"
    assert request.episode == "207"
    assert request.network == "Peacock"


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


def test_import_single_item_uses_passed_getty_asset_for_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    image = {
        "lbx_id": "70761513",
        "lbx_filename": "NUP_188900_0304.JPG",
        "location": "https://lightbox-thumbnails.s3.us-west-2.amazonaws.com/NUP_188900/x/NUP_188900_0304.JPG",
        "lbx_caption": (
            "WATCH WHAT HAPPENS LIVE WITH ANDY COHEN -- Episode 16173 -- "
            "Pictured: Andy Cohen, Brandi Glanville, Kelly Dodd -- (Photo by: Charles Sykes/Bravo)"
        ),
        "lbx_episodeNumber": "16173",
        "showIds": ["show-1"],
    }
    getty_asset = {
        "detail_url": "https://www.gettyimages.com/detail/news-photo/example/1246182942",
        "editorial_id": "1246182942",
        "object_name": "NUP_188900_0304.JPG",
        "title": "Watch What Happens Live With Andy Cohen - Season 16",
        "caption": (
            "WATCH WHAT HAPPENS LIVE WITH ANDY COHEN -- Episode 16173 -- "
            "Pictured: (l-r) Andy Cohen, Brandi Glanville, Kelly Dodd -- "
            "(Photo by: Charles Sykes/Bravo via Getty Images)"
        ),
        "event_name": "Watch What Happens Live With Andy Cohen - Season 16",
        "event_id": "775921530",
        "event_date": "October 30, 2019",
        "date_created": "October 30, 2019",
        "keyword_texts": ["Brandi Glanville", "Kelly Dodd", "Andy Cohen", "Season 16", "Talkshow"],
        "preview_image_url": "https://media.gettyimages.com/example-preview.jpg",
        "original_image_url": "https://media.gettyimages.com/example-original.jpg",
        "details": {
            "credit_display": "Bravo / Contributor",
            "collection_display": "NBCUniversal",
        },
        "people": [
            {"text": "Andy Cohen - Television Personality"},
            {"text": "Brandi Glanville"},
            {"text": "Kelly Dodd"},
        ],
        "people_count": 3,
    }
    captured: dict[str, object] = {}
    mock_db = MagicMock()
    fetch_getty = MagicMock()
    resolve_getty = MagicMock()

    monkeypatch.setattr("api.routers.admin_nbcumv.getty.fetch_asset_detail", fetch_getty)
    monkeypatch.setattr("api.routers.admin_nbcumv.getty.resolve_asset_by_object_name", resolve_getty)
    monkeypatch.setattr("api.routers.admin_nbcumv._existing_asset_by_nbcumv_id", lambda db, lbx_id: None)
    monkeypatch.setattr(
        "api.routers.admin_nbcumv.nbcumv.download_hires_image",
        lambda lbx_id, filename: (b"jpeg-bytes", "image/jpeg"),
    )
    monkeypatch.setattr(
        "api.routers.admin_nbcumv.nbcumv.extract_embedded_metadata",
        lambda image_bytes: {"dimensions": {"width": 4500, "height": 3000}},
    )
    monkeypatch.setattr("api.routers.admin_nbcumv.get_s3_bucket", lambda: "media-bucket")
    monkeypatch.setattr("api.routers.admin_nbcumv.get_s3_client", lambda: MagicMock())
    monkeypatch.setattr(
        "api.routers.admin_nbcumv.upload_bytes_to_s3",
        lambda client, bucket, key, data, content_type: ("etag-1", len(data)),
    )
    monkeypatch.setattr(
        "api.routers.admin_nbcumv.build_hosted_url",
        lambda key: "https://cdn.example.com/media/asset-1.jpg",
    )
    monkeypatch.setattr(
        "api.routers.admin_nbcumv._upsert_nbcumv_asset",
        lambda db, **kwargs: captured.update(kwargs) or {"id": "asset-1", "hosted_url": kwargs.get("hosted_url")},
    )
    monkeypatch.setattr("api.routers.admin_nbcumv._existing_person_links", lambda db, asset_id: [])
    monkeypatch.setattr(
        "api.routers.admin_nbcumv.generate_media_asset_variants",
        lambda db, asset_id, force=False: None,
    )

    result = _import_single_item(
        db=mock_db,
        item=NbcumvImportItem(
            lbx_id="70761513",
            lbx_filename="NUP_188900_0304.JPG",
            location=image["location"],
            nbcumv_image=image,
            getty_asset=getty_asset,
            getty_detail_url="https://www.gettyimages.com/detail/news-photo/example/1246182942",
            show_ids=["show-1"],
        ),
        assign_people=False,
        people_index={},
    )

    metadata = captured["metadata"]
    assert result["asset_id"] == "asset-1"
    assert isinstance(metadata, dict)
    assert metadata["getty"]["editorial_id"] == "1246182942"
    assert metadata["getty_tags"] == ["Brandi Glanville", "Kelly Dodd", "Andy Cohen", "Season 16", "Talkshow"]
    assert metadata["getty_event_title"] == "Watch What Happens Live With Andy Cohen - Season 16"
    assert metadata["source_page_url"] == "https://www.gettyimages.com/detail/news-photo/example/1246182942"
    assert metadata["getty_detail_page_url"] == "https://www.gettyimages.com/detail/news-photo/example/1246182942"
    assert metadata["getty_original_image_url"] == "https://media.gettyimages.com/example-original.jpg"
    assert metadata["getty_preview_image_url"] == "https://media.gettyimages.com/example-preview.jpg"
    assert metadata["people_names"] == ["Andy Cohen", "Brandi Glanville", "Kelly Dodd"]
    assert metadata["episode_number"] == 16173
    fetch_getty.assert_not_called()
    resolve_getty.assert_not_called()


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
        "status": "0",
        "is_hidden": True,
        "tags": ["HIDDEN"],
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
    assert item["status"] == "0"
    assert item["is_hidden"] is True
    assert item["effective_tags"] == ["HIDDEN"]


def test_preview_endpoint_forwards_new_cloudsearch_filters(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    mock_db = MagicMock()
    captured: dict[str, Any] = {}

    def _fake_search_images(filters):
        captured["filters"] = filters
        return []

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_nbcumv._ensure_sources"):
            with patch("api.routers.admin_nbcumv.nbcumv.search_images", side_effect=_fake_search_images):
                with patch("api.routers.admin_nbcumv._load_eligible_people_index", return_value={}):
                    response = client.post(
                        "/api/v1/admin/nbcumv/preview",
                        headers={"Authorization": f"Bearer {token}"},
                        json={
                            "search_text": "Brandi Glanville",
                            "nup_prefix": "NUP_195460",
                            "show_name": "The Real Housewives Ultimate Girls Trip: Ex-Wives Club",
                            "meta_type": "Episodic",
                            "season": "2",
                            "episode": "207",
                            "network": "Peacock",
                        },
                    )

    assert response.status_code == 200
    filters = captured["filters"]
    assert filters.search_text == "Brandi Glanville"
    assert filters.nup_prefix == "NUP_195460"
    assert filters.show_name == "The Real Housewives Ultimate Girls Trip: Ex-Wives Club"
    assert filters.meta_type == "Episodic"
    assert filters.season == "2"
    assert filters.episode == "207"
    assert filters.network == "Peacock"


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


def test_import_single_item_adds_hidden_tag_for_status_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    image = {
        "lbx_id": "70761513",
        "lbx_filename": "NUP_195460_02015.JPG",
        "location": "https://lightbox-thumbnails.s3.us-west-2.amazonaws.com/NUP_195460/x/NUP_195460_02015.JPG",
        "lbx_caption": "The Real Housewives Ultimate Girls Trip -- Pictured: Brandi Glanville",
        "showIds": ["show-1"],
        "status": "0",
        "is_hidden": True,
        "tags": ["HIDDEN"],
    }
    captured: dict[str, object] = {}

    monkeypatch.setattr("api.routers.admin_nbcumv.getty.fetch_asset_detail", lambda *args, **kwargs: None)
    monkeypatch.setattr("api.routers.admin_nbcumv.getty.resolve_asset_by_object_name", lambda *args, **kwargs: None)
    monkeypatch.setattr("api.routers.admin_nbcumv._existing_asset_by_nbcumv_id", lambda db, lbx_id: None)
    monkeypatch.setattr(
        "api.routers.admin_nbcumv.nbcumv.download_hires_image",
        lambda lbx_id, filename: (b"jpeg-bytes", "image/jpeg"),
    )
    monkeypatch.setattr(
        "api.routers.admin_nbcumv.nbcumv.extract_embedded_metadata",
        lambda image_bytes: {"dimensions": {"width": 4500, "height": 3000}},
    )
    monkeypatch.setattr("api.routers.admin_nbcumv.get_s3_bucket", lambda: "media-bucket")
    monkeypatch.setattr("api.routers.admin_nbcumv.get_s3_client", lambda: MagicMock())
    monkeypatch.setattr(
        "api.routers.admin_nbcumv.upload_bytes_to_s3",
        lambda client, bucket, key, data, content_type: ("etag-1", len(data)),
    )
    monkeypatch.setattr(
        "api.routers.admin_nbcumv.build_hosted_url",
        lambda key: "https://cdn.example.com/media/asset-1.jpg",
    )
    monkeypatch.setattr(
        "api.routers.admin_nbcumv._upsert_nbcumv_asset",
        lambda db, **kwargs: captured.update(kwargs) or {"id": "asset-1", "hosted_url": kwargs.get("hosted_url")},
    )
    monkeypatch.setattr("api.routers.admin_nbcumv._existing_person_links", lambda db, asset_id: [])
    monkeypatch.setattr(
        "api.routers.admin_nbcumv.generate_media_asset_variants",
        lambda db, asset_id, force=False: None,
    )

    _import_single_item(
        db=MagicMock(),
        item=NbcumvImportItem(
            lbx_id="70761513",
            lbx_filename="NUP_195460_02015.JPG",
            location=image["location"],
            nbcumv_image=image,
            show_ids=["show-1"],
        ),
        assign_people=False,
        people_index={},
    )

    metadata = captured["metadata"]
    assert isinstance(metadata, dict)
    assert metadata["status"] == "0"
    assert metadata["is_hidden"] is True
    assert "HIDDEN" in metadata["tags"]


def test_import_single_item_removes_hidden_tag_when_status_becomes_active(monkeypatch: pytest.MonkeyPatch) -> None:
    image = {
        "lbx_id": "70761513",
        "lbx_filename": "NUP_195460_02015.JPG",
        "location": "https://lightbox-thumbnails.s3.us-west-2.amazonaws.com/NUP_195460/x/NUP_195460_02015.JPG",
        "lbx_caption": "The Real Housewives Ultimate Girls Trip -- Pictured: Brandi Glanville",
        "showIds": ["show-1"],
        "status": "1",
        "is_hidden": False,
    }
    captured: dict[str, object] = {}
    existing_asset = {
        "id": "asset-1",
        "hosted_url": "https://cdn.example.com/media/asset-1.jpg",
        "hosted_bucket": "media-bucket",
        "hosted_key": "media/ab/asset.jpg",
        "hosted_etag": "etag-1",
        "metadata": {
            "embedded_file": {"dimensions": {"width": 2000, "height": 3000}},
            "tags": ["Bravo", "HIDDEN"],
        },
    }

    monkeypatch.setattr("api.routers.admin_nbcumv.getty.fetch_asset_detail", lambda *args, **kwargs: None)
    monkeypatch.setattr("api.routers.admin_nbcumv.getty.resolve_asset_by_object_name", lambda *args, **kwargs: None)
    monkeypatch.setattr("api.routers.admin_nbcumv._existing_asset_by_nbcumv_id", lambda db, lbx_id: existing_asset)
    monkeypatch.setattr(
        "api.routers.admin_nbcumv._upsert_nbcumv_asset",
        lambda db, **kwargs: captured.update(kwargs) or {"id": "asset-1", "hosted_url": kwargs.get("hosted_url")},
    )
    monkeypatch.setattr("api.routers.admin_nbcumv._existing_person_links", lambda db, asset_id: [])
    monkeypatch.setattr(
        "api.routers.admin_nbcumv.generate_media_asset_variants",
        lambda db, asset_id, force=False: None,
    )

    _import_single_item(
        db=MagicMock(),
        item=NbcumvImportItem(
            lbx_id="70761513",
            lbx_filename="NUP_195460_02015.JPG",
            location=image["location"],
            nbcumv_image=image,
            show_ids=["show-1"],
        ),
        assign_people=False,
        people_index={},
    )

    metadata = captured["metadata"]
    assert isinstance(metadata, dict)
    assert metadata["status"] == "1"
    assert metadata["is_hidden"] is False
    assert metadata["tags"] == ["Bravo"]
