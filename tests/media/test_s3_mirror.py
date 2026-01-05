from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from trr_backend.media import s3_mirror


def test_guess_ext_from_content_type() -> None:
    assert s3_mirror.guess_ext_from_content_type("image/webp") == ".webp"
    assert s3_mirror.guess_ext_from_content_type("image/jpeg") == ".jpg"
    assert s3_mirror.guess_ext_from_content_type("image/png") == ".png"
    assert s3_mirror.guess_ext_from_content_type("application/octet-stream") == ".bin"


def test_build_cast_photo_s3_key_structure() -> None:
    """Test S3 key structure uses stable IDs and includes /photos/ segment."""
    key = s3_mirror.build_cast_photo_s3_key("nm11883948", "fandom", "abc123", ".webp")
    assert key == "images/people/nm11883948/photos/fandom/abc123.webp"

    # Test with UUID fallback
    key = s3_mirror.build_cast_photo_s3_key("32ddc0a5-2bea-4a62-ba53-eda033af8efd", "tmdb", "xyz789", ".jpg")
    assert key == "images/people/32ddc0a5-2bea-4a62-ba53-eda033af8efd/photos/tmdb/xyz789.jpg"


def test_get_person_s3_prefix() -> None:
    """Test S3 prefix generation for prune operations."""
    prefix = s3_mirror.get_person_s3_prefix("nm11883948")
    assert prefix == "images/people/nm11883948/photos/"

    prefix = s3_mirror.get_person_s3_prefix("uuid-123")
    assert prefix == "images/people/uuid-123/photos/"


def test_sha256_stability() -> None:
    data = b"test-bytes"
    assert s3_mirror._sha256_bytes(data) == s3_mirror._sha256_bytes(data)


def test_download_image_sets_referer_for_fandom(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = {}

    def fake_get(url, headers=None, timeout=None, stream=None):  # noqa: ANN001
        captured["headers"] = headers or {}
        response = MagicMock()
        response.raise_for_status.return_value = None
        response.headers = {"Content-Type": "image/webp"}
        response.content = b"bytes"
        return response

    monkeypatch.setattr(s3_mirror.requests, "get", fake_get)
    s3_mirror.download_image("https://example.com/x.webp", source="fandom", referer="https://fandom.test")
    assert captured["headers"]["referer"] == "https://fandom.test"


def test_mirror_skips_upload_if_object_exists(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_S3_BUCKET", "bucket")
    monkeypatch.setenv("AWS_S3_PREFIX", "dev")
    monkeypatch.setenv("AWS_CDN_BASE_URL", "https://cdn.example.com")

    fake_s3 = MagicMock()
    fake_s3.head_object.return_value = {
        "ContentType": "image/webp",
        "ContentLength": 123,
        "ETag": "\"etag\"",
    }

    monkeypatch.setattr(s3_mirror, "download_image", lambda *args, **kwargs: (b"data", "image/webp"))
    monkeypatch.setattr(
        s3_mirror,
        "upload_bytes_to_s3",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("upload called")),
    )

    row = {
        "id": "photo-1",
        "person_id": "person-1",
        "imdb_person_id": "nm123",
        "source": "fandom",
        "image_url": "https://example.com/img.webp",
        "source_page_url": "https://example.com",
    }

    result = s3_mirror.mirror_cast_photo_row(row, s3_client=fake_s3)
    assert result is not None
    assert result["hosted_bytes"] == 123
    assert result["hosted_etag"] == "etag"
    assert result["hosted_url"].startswith("https://cdn.example.com/")


# ---------------------------------------------------------------------------
# S3 Prune Tests
# ---------------------------------------------------------------------------


def test_list_s3_objects_under_prefix() -> None:
    """Test listing S3 objects under a prefix with pagination."""
    fake_s3 = MagicMock()

    # Simulate paginated response
    fake_paginator = MagicMock()
    fake_paginator.paginate.return_value = [
        {"Contents": [{"Key": "images/people/nm123/photos/fandom/abc.webp"}]},
        {"Contents": [{"Key": "images/people/nm123/photos/tmdb/xyz.jpg"}]},
    ]
    fake_s3.get_paginator.return_value = fake_paginator

    keys = s3_mirror.list_s3_objects_under_prefix(
        fake_s3, "bucket", "images/people/nm123/photos/"
    )

    assert len(keys) == 2
    assert "images/people/nm123/photos/fandom/abc.webp" in keys
    assert "images/people/nm123/photos/tmdb/xyz.jpg" in keys


def test_list_s3_objects_empty_prefix() -> None:
    """Test listing S3 objects when prefix is empty."""
    fake_s3 = MagicMock()
    fake_paginator = MagicMock()
    fake_paginator.paginate.return_value = [{"Contents": []}]
    fake_s3.get_paginator.return_value = fake_paginator

    keys = s3_mirror.list_s3_objects_under_prefix(fake_s3, "bucket", "images/people/nm999/photos/")
    assert keys == []


def test_delete_s3_objects_empty_list() -> None:
    """Test deleting zero objects returns 0."""
    fake_s3 = MagicMock()
    count = s3_mirror.delete_s3_objects(fake_s3, "bucket", [])
    assert count == 0
    fake_s3.delete_objects.assert_not_called()


def test_delete_s3_objects_batch() -> None:
    """Test batch deletion of S3 objects."""
    fake_s3 = MagicMock()
    fake_s3.delete_objects.return_value = {"Errors": []}

    keys = ["key1", "key2", "key3"]
    count = s3_mirror.delete_s3_objects(fake_s3, "bucket", keys)

    assert count == 3
    fake_s3.delete_objects.assert_called_once()
    call_args = fake_s3.delete_objects.call_args
    assert call_args[1]["Bucket"] == "bucket"
    assert len(call_args[1]["Delete"]["Objects"]) == 3


def test_delete_s3_objects_partial_failure() -> None:
    """Test batch deletion with some failures."""
    fake_s3 = MagicMock()
    fake_s3.delete_objects.return_value = {
        "Errors": [{"Key": "key2", "Code": "AccessDenied"}]
    }

    keys = ["key1", "key2", "key3"]
    count = s3_mirror.delete_s3_objects(fake_s3, "bucket", keys)

    # 3 keys - 1 error = 2 deleted
    assert count == 2


def test_prune_orphaned_cast_photo_objects_no_orphans(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test prune when all S3 objects are referenced in DB."""
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_S3_BUCKET", "bucket")
    monkeypatch.setenv("AWS_CDN_BASE_URL", "https://cdn.example.com")

    fake_s3 = MagicMock()
    fake_paginator = MagicMock()
    fake_paginator.paginate.return_value = [
        {"Contents": [
            {"Key": "images/people/nm123/photos/fandom/abc.webp"},
            {"Key": "images/people/nm123/photos/tmdb/xyz.jpg"},
        ]}
    ]
    fake_s3.get_paginator.return_value = fake_paginator

    fake_db = MagicMock()

    # Mock fetch_hosted_keys_for_person to return same keys as S3
    with patch("trr_backend.repositories.cast_photos.fetch_hosted_keys_for_person") as mock_fetch:
        mock_fetch.return_value = {
            "images/people/nm123/photos/fandom/abc.webp",
            "images/people/nm123/photos/tmdb/xyz.jpg",
        }

        orphaned = s3_mirror.prune_orphaned_cast_photo_objects(
            fake_db, "nm123", s3_client=fake_s3
        )

    assert orphaned == []
    fake_s3.delete_objects.assert_not_called()


def test_prune_orphaned_cast_photo_objects_with_orphans(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test prune deletes S3 objects not referenced in DB."""
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_S3_BUCKET", "bucket")
    monkeypatch.setenv("AWS_CDN_BASE_URL", "https://cdn.example.com")

    fake_s3 = MagicMock()
    fake_paginator = MagicMock()
    fake_paginator.paginate.return_value = [
        {"Contents": [
            {"Key": "images/people/nm123/photos/fandom/abc.webp"},
            {"Key": "images/people/nm123/photos/fandom/orphan1.jpg"},
            {"Key": "images/people/nm123/photos/tmdb/orphan2.png"},
        ]}
    ]
    fake_s3.get_paginator.return_value = fake_paginator
    fake_s3.delete_objects.return_value = {"Errors": []}

    fake_db = MagicMock()

    # Mock fetch_hosted_keys_for_person to return only one key (rest are orphans)
    with patch("trr_backend.repositories.cast_photos.fetch_hosted_keys_for_person") as mock_fetch:
        mock_fetch.return_value = {"images/people/nm123/photos/fandom/abc.webp"}

        orphaned = s3_mirror.prune_orphaned_cast_photo_objects(
            fake_db, "nm123", s3_client=fake_s3
        )

    assert len(orphaned) == 2
    assert "images/people/nm123/photos/fandom/orphan1.jpg" in orphaned
    assert "images/people/nm123/photos/tmdb/orphan2.png" in orphaned
    fake_s3.delete_objects.assert_called_once()


def test_prune_orphaned_cast_photo_objects_dry_run(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test prune with dry_run=True doesn't delete."""
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_S3_BUCKET", "bucket")
    monkeypatch.setenv("AWS_CDN_BASE_URL", "https://cdn.example.com")

    fake_s3 = MagicMock()
    fake_paginator = MagicMock()
    fake_paginator.paginate.return_value = [
        {"Contents": [{"Key": "images/people/nm123/photos/fandom/orphan.jpg"}]}
    ]
    fake_s3.get_paginator.return_value = fake_paginator

    fake_db = MagicMock()

    with patch("trr_backend.repositories.cast_photos.fetch_hosted_keys_for_person") as mock_fetch:
        mock_fetch.return_value = set()  # No DB references = all orphans

        orphaned = s3_mirror.prune_orphaned_cast_photo_objects(
            fake_db, "nm123", dry_run=True, s3_client=fake_s3
        )

    assert len(orphaned) == 1
    assert "images/people/nm123/photos/fandom/orphan.jpg" in orphaned
    # Should NOT have called delete_objects due to dry_run
    fake_s3.delete_objects.assert_not_called()
