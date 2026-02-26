from __future__ import annotations

import io
import sys
import types
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


def test_build_show_image_s3_key_structure() -> None:
    key = s3_mirror.build_show_image_s3_key("tt1234567", "poster", "tmdb", "abc123", ".jpg")
    assert key == "images/shows/tt1234567/poster/tmdb/abc123.jpg"

    key = s3_mirror.build_show_image_s3_key(
        "2c0c9d84-77c2-4c4f-a2b9-6a8e3a42db5c",
        "backdrop",
        "imdb",
        "def456",
        ".webp",
    )
    assert key == "images/shows/2c0c9d84-77c2-4c4f-a2b9-6a8e3a42db5c/backdrop/imdb/def456.webp"


def test_build_season_image_s3_key_structure() -> None:
    key = s3_mirror.build_season_image_s3_key("tt7654321", 3, "tmdb", "aaa111", ".png")
    assert key == "images/seasons/tt7654321/season-3/tmdb/aaa111.png"


def test_build_episode_image_s3_key_structure() -> None:
    key = s3_mirror.build_episode_image_s3_key("tt9876543", "tmdb", "bbb222", ".jpg")
    assert key == "images/episodes/tt9876543/tmdb/bbb222.jpg"


def test_build_logo_s3_key_structure() -> None:
    key = s3_mirror.build_logo_s3_key("networks", 123, "abc123", ".png")
    assert key == "images/logos/networks/123/abc123.png"


def test_build_logo_variant_s3_key_structure() -> None:
    key = s3_mirror.build_logo_variant_s3_key("watch-providers", 531, "black", "abc123", ".png")
    assert key == "images/logos/watch-providers/531/black/abc123.png"


def test_build_monochrome_logo_variants_preserves_transparency() -> None:
    from PIL import Image

    image = Image.new("RGBA", (6, 6), (0, 0, 0, 0))
    for y in range(2, 4):
        for x in range(2, 4):
            image.putpixel((x, y), (12, 34, 56, 255))

    raw = io.BytesIO()
    image.save(raw, format="PNG")

    black_payload, white_payload = s3_mirror._build_monochrome_logo_variants(raw.getvalue(), "image/png")
    black = Image.open(io.BytesIO(black_payload[0])).convert("RGBA")
    white = Image.open(io.BytesIO(white_payload[0])).convert("RGBA")

    assert black.getpixel((0, 0))[3] == 0
    assert white.getpixel((0, 0))[3] == 0
    assert black.getpixel((2, 2)) == (0, 0, 0, 255)
    assert white.getpixel((2, 2)) == (255, 255, 255, 255)


def test_apply_logo_variant_upload_skips_upload_when_sha_matches_without_force(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_S3_BUCKET", "bucket")
    monkeypatch.setenv("AWS_CDN_BASE_URL", "https://cdn.example.com")

    data = b"same-bytes"
    sha = s3_mirror._sha256_bytes(data)
    key = s3_mirror.build_logo_variant_s3_key("networks", 10, "black", sha, ".png")
    url = s3_mirror.build_hosted_url(key)
    head_mock = MagicMock()
    monkeypatch.setattr(s3_mirror, "_head_object", head_mock)

    patch, mirrored = s3_mirror._apply_logo_variant_upload(
        row={
            "hosted_logo_black_key": key,
            "hosted_logo_black_url": url,
            "hosted_logo_black_sha256": sha,
        },
        kind="networks",
        entity_id=10,
        variant="black",
        data=data,
        content_type="image/png",
        ext=".png",
        force=False,
        s3_client=MagicMock(),
    )

    assert patch == {}
    assert mirrored == 0
    head_mock.assert_not_called()


def test_get_person_s3_prefix() -> None:
    """Test S3 prefix generation for prune operations."""
    prefix = s3_mirror.get_person_s3_prefix("nm11883948")
    assert prefix == "images/people/nm11883948/photos/"

    prefix = s3_mirror.get_person_s3_prefix("uuid-123")
    assert prefix == "images/people/uuid-123/photos/"


def test_get_show_s3_prefix() -> None:
    prefix = s3_mirror.get_show_s3_prefix("tt1234567")
    assert prefix == "images/shows/tt1234567/"


def test_build_hosted_url_normalizes_slashes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_S3_BUCKET", "bucket")
    monkeypatch.setenv("AWS_CDN_BASE_URL", "https://cdn.example.com/")
    assert s3_mirror.build_hosted_url("/images/test.png") == "https://cdn.example.com/images/test.png"


def test_cdn_base_url_rejects_placeholder(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_S3_BUCKET", "bucket")
    monkeypatch.setenv("AWS_CDN_BASE_URL", "https://dxxxx.cloudfront.net")
    with pytest.raises(RuntimeError):
        s3_mirror.get_cdn_base_url()


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


def test_download_image_rejects_non_image_content_type(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_get(url, headers=None, timeout=None, stream=None):  # noqa: ANN001
        response = MagicMock()
        response.raise_for_status.return_value = None
        response.headers = {"Content-Type": "text/html; charset=utf-8"}
        response.content = b"<html>nope</html>"
        return response

    monkeypatch.setattr(s3_mirror.requests, "get", fake_get)
    with pytest.raises(RuntimeError):
        s3_mirror.download_image("https://example.com/x.html", source="fandom")


def test_download_image_sniffs_image_type_when_missing_content_type(monkeypatch: pytest.MonkeyPatch) -> None:
    png_bytes = b"\x89PNG\r\n\x1a\n" + b"rest"

    def fake_get(url, headers=None, timeout=None, stream=None):  # noqa: ANN001
        response = MagicMock()
        response.raise_for_status.return_value = None
        response.headers = {}
        response.content = png_bytes
        return response

    monkeypatch.setattr(s3_mirror.requests, "get", fake_get)
    data, content_type = s3_mirror.download_image("https://example.com/x.png", source="fandom")
    assert data == png_bytes
    assert content_type == "image/png"


def test_sniff_image_content_type_detects_svg() -> None:
    svg = b'<?xml version="1.0"?><svg xmlns="http://www.w3.org/2000/svg"></svg>'
    assert s3_mirror._sniff_image_content_type(svg) == "image/svg+xml"


def test_ensure_png_bytes_rasterizes_svg_with_cairosvg(monkeypatch: pytest.MonkeyPatch) -> None:
    png_bytes = b"\x89PNG\r\n\x1a\npng"
    monkeypatch.setitem(sys.modules, "cairosvg", types.SimpleNamespace(svg2png=lambda **kwargs: png_bytes))
    svg = b"<svg xmlns='http://www.w3.org/2000/svg'/>"
    result = s3_mirror._ensure_png_bytes(svg, "image/svg+xml")
    assert result == (png_bytes, "image/png", ".png")


def test_mirror_show_image_row_logo_normalizes_upload_to_png(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_S3_BUCKET", "bucket")
    monkeypatch.setenv("AWS_CDN_BASE_URL", "https://cdn.example.com")

    upload_mock = MagicMock(return_value=("etag", 9))
    monkeypatch.setattr(s3_mirror, "download_image", lambda *args, **kwargs: (b"raw-logo", "image/svg+xml"))
    monkeypatch.setattr(
        s3_mirror,
        "ensure_logo_png_bytes",
        lambda data, content_type: (b"png-logo", "image/png", ".png"),
    )
    monkeypatch.setattr(s3_mirror, "_head_object", lambda *args, **kwargs: None)
    monkeypatch.setattr(s3_mirror, "upload_bytes_to_s3", upload_mock)

    row = {
        "id": "show-image-1",
        "show_id": "show-1",
        "source": "imdb",
        "kind": "logo",
        "url": "https://example.com/logo.svg",
    }

    patch = s3_mirror.mirror_show_image_row(row, s3_client=MagicMock())

    assert patch is not None
    assert patch["hosted_key"].endswith(".png")
    assert patch["hosted_content_type"] == "image/png"
    upload_call = upload_mock.call_args.kwargs
    assert upload_call["data"] == b"png-logo"
    assert upload_call["content_type"] == "image/png"


def test_normalize_fandom_file_url_strips_revision_latest_suffix() -> None:
    url = "https://static.wikia.nocookie.net/real-housewives/images/0/08/angie_k_s3.jpeg/revision/latest"
    normalized = s3_mirror.normalize_fandom_file_url(url, referer="https://real-housewives.fandom.com/wiki/Test")
    assert normalized == "https://static.wikia.nocookie.net/real-housewives/images/0/08/angie_k_s3.jpeg"


def test_mirror_skips_upload_if_object_exists(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_S3_BUCKET", "bucket")
    monkeypatch.setenv("AWS_S3_PREFIX", "dev")
    monkeypatch.setenv("AWS_CDN_BASE_URL", "https://cdn.example.com")

    fake_s3 = MagicMock()
    fake_s3.head_object.return_value = {
        "ContentType": "image/webp",
        "ContentLength": 123,
        "ETag": '"etag"',
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


def test_mirror_cast_photo_fallbacks_to_thumb_when_primary_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_S3_BUCKET", "bucket")
    monkeypatch.setenv("AWS_S3_PREFIX", "dev")
    monkeypatch.setenv("AWS_CDN_BASE_URL", "https://cdn.example.com")

    fake_s3 = MagicMock()
    monkeypatch.setattr(s3_mirror, "_head_object", lambda *args, **kwargs: None)

    def fake_download(url, source=None, referer=None, headers=None):  # noqa: ANN001
        if "Special:FilePath" in url:
            raise RuntimeError("Non-image response content-type: text/html")
        return b"\x89PNG\r\n\x1a\nrest", "image/png"

    monkeypatch.setattr(s3_mirror, "download_image", fake_download)
    monkeypatch.setattr(s3_mirror, "upload_bytes_to_s3", lambda *args, **kwargs: ("etag", 10))

    row = {
        "id": "photo-1",
        "person_id": "person-1",
        "imdb_person_id": "nm123",
        "source": "fandom",
        "image_url": "https://real-housewives.fandom.com/wiki/Special:FilePath/Bad.png",
        "thumb_url": "https://static.wikia.nocookie.net/real-housewives/images/1/1a/Good.png",
        "source_page_url": "https://real-housewives.fandom.com/wiki/Test",
    }

    result = s3_mirror.mirror_cast_photo_row(row, s3_client=fake_s3)
    assert result is not None
    assert result["hosted_url"].startswith("https://cdn.example.com/")
    assert result["hosted_content_type"] == "image/png"
    assert result["image_url"] == row["thumb_url"]


def test_mirror_tmdb_logo_skips_upload_if_object_exists(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_S3_BUCKET", "bucket")
    monkeypatch.setenv("AWS_CDN_BASE_URL", "https://cdn.example.com")

    fake_s3 = MagicMock()
    fake_s3.head_object.return_value = {
        "ContentType": "image/png",
        "ContentLength": 321,
        "ETag": '"etag-logo"',
    }

    monkeypatch.setattr(s3_mirror, "download_image", lambda *args, **kwargs: (b"data", "image/png"))
    monkeypatch.setattr(
        s3_mirror,
        "upload_bytes_to_s3",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("upload called")),
    )

    row = {
        "id": 42,
        "tmdb_logo_path": "/logo.png",
    }

    result = s3_mirror.mirror_tmdb_logo_row(row, kind="networks", s3_client=fake_s3)
    assert result is not None
    assert result["hosted_logo_bytes"] == 321
    assert result["hosted_logo_etag"] == "etag-logo"
    assert result["hosted_logo_url"].startswith("https://cdn.example.com/")
    assert result["logo_path"].endswith(".png")


def test_mirror_tmdb_logo_skips_when_already_hosted(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_S3_BUCKET", "bucket")
    monkeypatch.setenv("AWS_CDN_BASE_URL", "https://cdn.example.com")

    row = {
        "id": 99,
        "tmdb_logo_path": "/logo.png",
        "hosted_logo_url": "https://cdn.example.com/images/logos/networks/99/abc.png",
    }

    result = s3_mirror.mirror_tmdb_logo_row(row, kind="networks")
    assert result is None


def test_mirror_external_logo_skips_upload_if_object_exists(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_S3_BUCKET", "bucket")
    monkeypatch.setenv("AWS_CDN_BASE_URL", "https://cdn.example.com")

    fake_s3 = MagicMock()
    fake_s3.head_object.return_value = {
        "ContentType": "image/png",
        "ContentLength": 500,
        "ETag": '"etag-external-logo"',
    }

    monkeypatch.setattr(s3_mirror, "download_image", lambda *args, **kwargs: (b"\x89PNG\r\n\x1a\nrest", "image/png"))
    monkeypatch.setattr(
        s3_mirror,
        "upload_bytes_to_s3",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("upload called")),
    )

    row = {"provider_id": 531}

    result = s3_mirror.mirror_external_logo_row(
        row,
        kind="watch-providers",
        source_url="https://upload.wikimedia.org/file.png",
        id_field="provider_id",
        s3_client=fake_s3,
    )
    assert result is not None
    assert result["hosted_logo_bytes"] == 500
    assert result["hosted_logo_etag"] == "etag-external-logo"
    assert result["hosted_logo_url"].startswith("https://cdn.example.com/")
    assert result["logo_path"].endswith(".png")


def test_mirror_external_logo_skips_when_already_hosted(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_S3_BUCKET", "bucket")
    monkeypatch.setenv("AWS_CDN_BASE_URL", "https://cdn.example.com")

    row = {
        "id": 777,
        "hosted_logo_url": "https://cdn.example.com/images/logos/networks/777/existing.png",
    }

    result = s3_mirror.mirror_external_logo_row(
        row,
        kind="networks",
        source_url="https://upload.wikimedia.org/file.png",
    )
    assert result is None


def test_mirror_logo_monochrome_variants_row_generates_black_and_white(monkeypatch: pytest.MonkeyPatch) -> None:
    from PIL import Image

    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_S3_BUCKET", "bucket")
    monkeypatch.setenv("AWS_CDN_BASE_URL", "https://cdn.example.com")

    img = Image.new("RGBA", (8, 8), (0, 0, 0, 0))
    for y in range(2, 6):
        for x in range(2, 6):
            img.putpixel((x, y), (10, 20, 30, 255))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    png = buf.getvalue()

    monkeypatch.setattr(s3_mirror, "download_image", lambda *args, **kwargs: (png, "image/png"))
    monkeypatch.setattr(s3_mirror, "_head_object", lambda *args, **kwargs: None)
    monkeypatch.setattr(s3_mirror, "upload_bytes_to_s3", lambda *args, **kwargs: ("etag", 123))

    result = s3_mirror.mirror_logo_monochrome_variants_row(
        {"id": 1},
        kind="networks",
        source_url="https://example.com/logo.png",
        s3_client=MagicMock(),
    )

    assert result is not None
    assert result.black_mirrored == 1
    assert result.white_mirrored == 1
    assert result.patch["hosted_logo_black_url"].startswith("https://cdn.example.com/")
    assert result.patch["hosted_logo_white_url"].startswith("https://cdn.example.com/")
    assert result.patch["hosted_logo_black_key"].startswith("images/logos/networks/1/black/")
    assert result.patch["hosted_logo_white_key"].startswith("images/logos/networks/1/white/")


def test_mirror_logo_monochrome_variants_row_skips_when_existing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_S3_BUCKET", "bucket")
    monkeypatch.setenv("AWS_CDN_BASE_URL", "https://cdn.example.com")

    result = s3_mirror.mirror_logo_monochrome_variants_row(
        {
            "id": 1,
            "hosted_logo_black_url": "https://cdn.example.com/black.png",
            "hosted_logo_white_url": "https://cdn.example.com/white.png",
        },
        kind="networks",
        source_url="https://example.com/logo.png",
    )
    assert result is None


def test_mirror_logo_monochrome_variants_row_raises_on_transparency_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_S3_BUCKET", "bucket")
    monkeypatch.setenv("AWS_CDN_BASE_URL", "https://cdn.example.com")
    monkeypatch.setattr(s3_mirror, "download_image", lambda *args, **kwargs: (b"not-an-image", "image/png"))
    monkeypatch.setattr(
        s3_mirror,
        "_build_monochrome_logo_variants",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("transparent_extraction_failed")),
    )

    with pytest.raises(RuntimeError, match="transparent_extraction_failed"):
        s3_mirror.mirror_logo_monochrome_variants_row(
            {"id": 1},
            kind="networks",
            source_url="https://example.com/logo.png",
            s3_client=MagicMock(),
        )


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

    keys = s3_mirror.list_s3_objects_under_prefix(fake_s3, "bucket", "images/people/nm123/photos/")

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
    fake_s3.delete_objects.return_value = {"Errors": [{"Key": "key2", "Code": "AccessDenied"}]}

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
        {
            "Contents": [
                {"Key": "images/people/nm123/photos/fandom/abc.webp"},
                {"Key": "images/people/nm123/photos/tmdb/xyz.jpg"},
            ]
        }
    ]
    fake_s3.get_paginator.return_value = fake_paginator

    fake_db = MagicMock()

    # Mock fetch_hosted_keys_for_person to return same keys as S3
    with patch("trr_backend.repositories.cast_photos.fetch_hosted_keys_for_person") as mock_fetch:
        mock_fetch.return_value = {
            "images/people/nm123/photos/fandom/abc.webp",
            "images/people/nm123/photos/tmdb/xyz.jpg",
        }

        orphaned = s3_mirror.prune_orphaned_cast_photo_objects(fake_db, "nm123", s3_client=fake_s3)

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
        {
            "Contents": [
                {"Key": "images/people/nm123/photos/fandom/abc.webp"},
                {"Key": "images/people/nm123/photos/fandom/orphan1.jpg"},
                {"Key": "images/people/nm123/photos/tmdb/orphan2.png"},
            ]
        }
    ]
    fake_s3.get_paginator.return_value = fake_paginator
    fake_s3.delete_objects.return_value = {"Errors": []}

    fake_db = MagicMock()

    # Mock fetch_hosted_keys_for_person to return only one key (rest are orphans)
    with patch("trr_backend.repositories.cast_photos.fetch_hosted_keys_for_person") as mock_fetch:
        mock_fetch.return_value = {"images/people/nm123/photos/fandom/abc.webp"}

        orphaned = s3_mirror.prune_orphaned_cast_photo_objects(fake_db, "nm123", s3_client=fake_s3)

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
    fake_paginator.paginate.return_value = [{"Contents": [{"Key": "images/people/nm123/photos/fandom/orphan.jpg"}]}]
    fake_s3.get_paginator.return_value = fake_paginator

    fake_db = MagicMock()

    with patch("trr_backend.repositories.cast_photos.fetch_hosted_keys_for_person") as mock_fetch:
        mock_fetch.return_value = set()  # No DB references = all orphans

        orphaned = s3_mirror.prune_orphaned_cast_photo_objects(fake_db, "nm123", dry_run=True, s3_client=fake_s3)

    assert len(orphaned) == 1
    assert "images/people/nm123/photos/fandom/orphan.jpg" in orphaned
    # Should NOT have called delete_objects due to dry_run
    fake_s3.delete_objects.assert_not_called()
