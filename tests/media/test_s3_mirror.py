from __future__ import annotations

import io
import json
import sys
import types
from dataclasses import dataclass
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ProfileNotFound

from trr_backend.media import s3_mirror


def test_guess_ext_from_content_type() -> None:
    assert s3_mirror.guess_ext_from_content_type("image/webp") == ".webp"
    assert s3_mirror.guess_ext_from_content_type("image/jpeg") == ".jpg"
    assert s3_mirror.guess_ext_from_content_type("image/png") == ".png"
    assert s3_mirror.guess_ext_from_content_type("application/octet-stream") == ".bin"


def test_infer_media_extension_prefers_url_suffix() -> None:
    assert s3_mirror.infer_media_extension("https://video.twimg.com/path/file.mp4?tag=1", "image/jpeg") == ".mp4"
    assert s3_mirror.infer_media_extension("https://pbs.twimg.com/media/file.jpeg", None) == ".jpeg"
    assert s3_mirror.infer_media_extension("https://example.com/file", "video/webm") == ".webm"


def test_mirror_url_to_s3_uploads_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OBJECT_STORAGE_REGION", "us-east-1")
    monkeypatch.setenv("OBJECT_STORAGE_BUCKET", "bucket")
    monkeypatch.setenv("OBJECT_STORAGE_PUBLIC_BASE_URL", "https://cdn.example.com")

    class _FakeResponse:
        headers = {"Content-Type": "video/mp4"}

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

        def raise_for_status(self) -> None:
            return None

        def iter_content(self, chunk_size: int):  # noqa: ANN001
            del chunk_size
            yield b"abc123"

    monkeypatch.setattr(s3_mirror.requests, "get", lambda *args, **kwargs: _FakeResponse())  # noqa: ARG005
    monkeypatch.setattr(s3_mirror, "_head_object", lambda *_args, **_kwargs: None)
    upload_mock = MagicMock(return_value=("etag-1", 6))
    monkeypatch.setattr(s3_mirror, "upload_bytes_to_s3", upload_mock)

    result = s3_mirror.mirror_url_to_s3(
        "https://video.twimg.com/ext_tw_video/12345/pu/vid.mp4",
        s3_client=MagicMock(),
        bucket="bucket",
    )

    assert result.status == "mirrored"
    assert result.error is None
    assert result.hosted_url is not None and result.hosted_url.startswith("https://cdn.example.com/media/")
    assert result.hosted_key is not None and result.hosted_key.endswith(".mp4")
    upload_mock.assert_called_once()


def test_mirror_url_to_s3_skips_upload_when_existing_object(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OBJECT_STORAGE_REGION", "us-east-1")
    monkeypatch.setenv("OBJECT_STORAGE_BUCKET", "bucket")
    monkeypatch.setenv("OBJECT_STORAGE_PUBLIC_BASE_URL", "https://cdn.example.com")

    class _FakeResponse:
        headers = {"Content-Type": "video/mp4"}

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

        def raise_for_status(self) -> None:
            return None

        def iter_content(self, chunk_size: int):  # noqa: ANN001
            del chunk_size
            yield b"abc123"

    monkeypatch.setattr(s3_mirror.requests, "get", lambda *args, **kwargs: _FakeResponse())  # noqa: ARG005
    monkeypatch.setattr(
        s3_mirror,
        "_head_object",
        lambda *_args, **_kwargs: {"ContentType": "video/mp4", "ContentLength": 99},
    )
    upload_mock = MagicMock()
    monkeypatch.setattr(s3_mirror, "upload_bytes_to_s3", upload_mock)

    result = s3_mirror.mirror_url_to_s3(
        "https://video.twimg.com/ext_tw_video/12345/pu/vid.mp4",
        s3_client=MagicMock(),
        bucket="bucket",
    )

    assert result.status == "skipped"
    assert result.error is None
    assert result.size_bytes == 99
    upload_mock.assert_not_called()


def test_mirror_url_to_s3_invalid_url_is_skipped() -> None:
    result = s3_mirror.mirror_url_to_s3("ftp://example.com/file.mp4")
    assert result.status == "skipped"
    assert result.error == "invalid_source_url"


def test_mirror_url_to_s3_fails_for_oversized_assets(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OBJECT_STORAGE_REGION", "us-east-1")
    monkeypatch.setenv("OBJECT_STORAGE_BUCKET", "bucket")
    monkeypatch.setenv("OBJECT_STORAGE_PUBLIC_BASE_URL", "https://cdn.example.com")

    class _FakeResponse:
        headers = {"Content-Type": "video/mp4"}

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

        def raise_for_status(self) -> None:
            return None

        def iter_content(self, chunk_size: int):  # noqa: ANN001
            del chunk_size
            yield b"12345"
            yield b"67890"

    monkeypatch.setattr(s3_mirror.requests, "get", lambda *args, **kwargs: _FakeResponse())  # noqa: ARG005
    result = s3_mirror.mirror_url_to_s3(
        "https://video.twimg.com/ext_tw_video/12345/pu/vid.mp4",
        s3_client=MagicMock(),
        bucket="bucket",
        max_bytes=8,
    )
    assert result.status == "failed"
    assert result.error == "asset_too_large"


@dataclass
class _FakeBotoSession:
    kwargs: dict[str, object]

    def client(self, service_name: str, **kwargs):  # noqa: ANN001
        return {"service_name": service_name, **kwargs}


def test_load_s3_config_ignores_profile_when_explicit_creds_present(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OBJECT_STORAGE_REGION", "us-east-1")
    monkeypatch.setenv("OBJECT_STORAGE_BUCKET", "bucket")
    monkeypatch.setenv("OBJECT_STORAGE_PUBLIC_BASE_URL", "https://cdn.example.com")
    monkeypatch.setenv("OBJECT_STORAGE_PROFILE", "trr")
    monkeypatch.setenv("OBJECT_STORAGE_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("OBJECT_STORAGE_SECRET_ACCESS_KEY", "secret")

    config = s3_mirror.get_s3_config()

    assert config.profile_name is None


def test_get_s3_client_falls_back_to_env_creds_when_profile_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OBJECT_STORAGE_REGION", "us-east-1")
    monkeypatch.setenv("OBJECT_STORAGE_BUCKET", "bucket")
    monkeypatch.setenv("OBJECT_STORAGE_PUBLIC_BASE_URL", "https://cdn.example.com")
    monkeypatch.setenv("OBJECT_STORAGE_PROFILE", "trr")
    monkeypatch.setenv("OBJECT_STORAGE_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("OBJECT_STORAGE_SECRET_ACCESS_KEY", "secret")
    monkeypatch.setenv("OBJECT_STORAGE_SESSION_TOKEN", "token")

    session_calls: list[dict[str, object]] = []

    def _fake_session(**kwargs):  # noqa: ANN001
        if kwargs.get("profile_name") == "trr":
            raise ProfileNotFound(profile="trr")
        session_calls.append(dict(kwargs))
        return _FakeBotoSession(dict(kwargs))

    monkeypatch.setattr(s3_mirror.boto3, "Session", _fake_session)

    client = s3_mirror.get_s3_client()

    assert session_calls == [{"region_name": "us-east-1"}]
    assert client == {
        "service_name": "s3",
        "region_name": "us-east-1",
        "aws_access_key_id": "key",
        "aws_secret_access_key": "secret",
        "aws_session_token": "token",
    }


def test_object_storage_env_aliases_override_aws_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OBJECT_STORAGE_REGION", "us-east-1")
    monkeypatch.setenv("OBJECT_STORAGE_BUCKET", "aws-bucket")
    monkeypatch.setenv("OBJECT_STORAGE_PUBLIC_BASE_URL", "https://cdn.aws.example.com")
    monkeypatch.setenv("OBJECT_STORAGE_PROVIDER", "r2")
    monkeypatch.setenv("OBJECT_STORAGE_BUCKET", "r2-bucket")
    monkeypatch.setenv("OBJECT_STORAGE_REGION", "auto")
    monkeypatch.setenv("OBJECT_STORAGE_ENDPOINT_URL", "https://example-account.r2.cloudflarestorage.com")
    monkeypatch.setenv("OBJECT_STORAGE_PUBLIC_BASE_URL", "https://cdn.r2.example.com")
    monkeypatch.setenv("OBJECT_STORAGE_ACCESS_KEY_ID", "r2-key")
    monkeypatch.setenv("OBJECT_STORAGE_SECRET_ACCESS_KEY", "r2-secret")

    config = s3_mirror.get_s3_config()

    assert config.provider == "r2"
    assert config.bucket == "r2-bucket"
    assert config.region == "auto"
    assert config.endpoint_url == "https://example-account.r2.cloudflarestorage.com"
    assert config.cdn_base_url == "https://cdn.r2.example.com"
    assert config.access_key_id == "r2-key"
    assert config.secret_access_key == "r2-secret"


def test_get_s3_client_uses_endpoint_url_for_r2(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OBJECT_STORAGE_PROVIDER", "r2")
    monkeypatch.setenv("OBJECT_STORAGE_BUCKET", "bucket")
    monkeypatch.setenv("OBJECT_STORAGE_REGION", "auto")
    monkeypatch.setenv("OBJECT_STORAGE_ENDPOINT_URL", "https://example-account.r2.cloudflarestorage.com")
    monkeypatch.setenv("OBJECT_STORAGE_PUBLIC_BASE_URL", "https://cdn.r2.example.com")
    monkeypatch.setenv("OBJECT_STORAGE_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("OBJECT_STORAGE_SECRET_ACCESS_KEY", "secret")

    session_calls: list[dict[str, object]] = []

    def _fake_session(**kwargs):  # noqa: ANN001
        session_calls.append(dict(kwargs))
        return _FakeBotoSession(dict(kwargs))

    monkeypatch.setattr(s3_mirror.boto3, "Session", _fake_session)

    client = s3_mirror.get_s3_client()

    assert session_calls == [{"region_name": "auto"}]
    assert client["endpoint_url"] == "https://example-account.r2.cloudflarestorage.com"
    assert client["aws_access_key_id"] == "key"
    assert client["aws_secret_access_key"] == "secret"
    assert client["region_name"] == "auto"


def test_mirror_urls_to_s3_isolates_failures_and_deduplicates(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    def _fake_mirror(url: str, **kwargs):  # noqa: ANN001
        del kwargs
        calls.append(url)
        if "bad" in url:
            raise RuntimeError("boom")
        return s3_mirror.MirrorResult(
            source_url=url,
            hosted_url=f"https://cdn.example.com/{url.rsplit('/', 1)[-1]}",
            hosted_key="media/key",
            sha256="abc",
            content_type="video/mp4",
            size_bytes=10,
            status="mirrored",
            error=None,
        )

    monkeypatch.setattr(s3_mirror, "mirror_url_to_s3", _fake_mirror)
    results = s3_mirror.mirror_urls_to_s3(
        [
            "https://example.com/ok.mp4",
            "https://example.com/bad.mp4",
            "https://example.com/ok.mp4",
        ]
    )

    assert len(results) == 3
    assert calls == ["https://example.com/ok.mp4", "https://example.com/bad.mp4"]
    assert results[0].status == "mirrored"
    assert results[1].status == "failed"
    assert results[2].status == "mirrored"


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
    monkeypatch.setenv("OBJECT_STORAGE_REGION", "us-east-1")
    monkeypatch.setenv("OBJECT_STORAGE_BUCKET", "bucket")
    monkeypatch.setenv("OBJECT_STORAGE_PUBLIC_BASE_URL", "https://cdn.example.com")

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
    monkeypatch.setenv("OBJECT_STORAGE_REGION", "us-east-1")
    monkeypatch.setenv("OBJECT_STORAGE_BUCKET", "bucket")
    monkeypatch.setenv("OBJECT_STORAGE_PUBLIC_BASE_URL", "https://cdn.example.com/")
    assert s3_mirror.build_hosted_url("/images/test.png") == "https://cdn.example.com/images/test.png"
    assert s3_mirror.build_public_object_url("/images/test.png") == "https://cdn.example.com/images/test.png"


def test_provider_neutral_storage_aliases_match_legacy_helpers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OBJECT_STORAGE_PROVIDER", "r2")
    monkeypatch.setenv("OBJECT_STORAGE_BUCKET", "r2-bucket")
    monkeypatch.setenv("OBJECT_STORAGE_REGION", "auto")
    monkeypatch.setenv("OBJECT_STORAGE_ENDPOINT_URL", "https://example-account.r2.cloudflarestorage.com")
    monkeypatch.setenv("OBJECT_STORAGE_PUBLIC_BASE_URL", "https://pub.example.com")

    config = s3_mirror.get_hosted_media_storage_config()

    assert config.bucket == "r2-bucket"
    assert s3_mirror.get_object_storage_bucket() == "r2-bucket"
    assert s3_mirror.get_public_base_url() == "https://pub.example.com"
    assert s3_mirror.get_cdn_base_url() == "https://pub.example.com"
    assert s3_mirror.build_public_object_url("media/example.webp") == "https://pub.example.com/media/example.webp"


def test_cdn_base_url_rejects_placeholder(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OBJECT_STORAGE_REGION", "us-east-1")
    monkeypatch.setenv("OBJECT_STORAGE_BUCKET", "bucket")
    monkeypatch.setenv("OBJECT_STORAGE_PUBLIC_BASE_URL", "https://dxxxx.cloudfront.net")
    with pytest.raises(RuntimeError):
        s3_mirror.get_cdn_base_url()


@pytest.mark.parametrize(
    "cdn_url",
    [
        "https://s3.amazonaws.com",
        "https://trr-backend.s3.amazonaws.com",
        "https://s3.us-east-1.amazonaws.com",
        "https://trr-backend.s3.us-east-1.amazonaws.com",
    ],
)
def test_cdn_base_url_rejects_s3_endpoints(monkeypatch: pytest.MonkeyPatch, cdn_url: str) -> None:
    monkeypatch.setenv("OBJECT_STORAGE_REGION", "us-east-1")
    monkeypatch.setenv("OBJECT_STORAGE_BUCKET", "bucket")
    monkeypatch.setenv("OBJECT_STORAGE_PUBLIC_BASE_URL", cdn_url)
    with pytest.raises(RuntimeError, match="must not be a direct S3 endpoint"):
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


def test_normalize_fandom_file_url_strips_revision_latest_suffix() -> None:
    url = "https://static.wikia.nocookie.net/real-housewives/images/0/08/angie_k_s3.jpeg/revision/latest"
    normalized = s3_mirror.normalize_fandom_file_url(url, referer="https://real-housewives.fandom.com/wiki/Test")
    assert normalized == "https://static.wikia.nocookie.net/real-housewives/images/0/08/angie_k_s3.jpeg"


def test_mirror_skips_upload_if_object_exists(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OBJECT_STORAGE_REGION", "us-east-1")
    monkeypatch.setenv("OBJECT_STORAGE_BUCKET", "bucket")
    monkeypatch.setenv("OBJECT_STORAGE_PREFIX", "dev")
    monkeypatch.setenv("OBJECT_STORAGE_PUBLIC_BASE_URL", "https://cdn.example.com")

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
    monkeypatch.setenv("OBJECT_STORAGE_REGION", "us-east-1")
    monkeypatch.setenv("OBJECT_STORAGE_BUCKET", "bucket")
    monkeypatch.setenv("OBJECT_STORAGE_PREFIX", "dev")
    monkeypatch.setenv("OBJECT_STORAGE_PUBLIC_BASE_URL", "https://cdn.example.com")

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
    monkeypatch.setenv("OBJECT_STORAGE_REGION", "us-east-1")
    monkeypatch.setenv("OBJECT_STORAGE_BUCKET", "bucket")
    monkeypatch.setenv("OBJECT_STORAGE_PUBLIC_BASE_URL", "https://cdn.example.com")

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
    monkeypatch.setenv("OBJECT_STORAGE_REGION", "us-east-1")
    monkeypatch.setenv("OBJECT_STORAGE_BUCKET", "bucket")
    monkeypatch.setenv("OBJECT_STORAGE_PUBLIC_BASE_URL", "https://cdn.example.com")

    row = {
        "id": 99,
        "tmdb_logo_path": "/logo.png",
        "hosted_logo_url": "https://cdn.example.com/images/logos/networks/99/abc.png",
    }

    result = s3_mirror.mirror_tmdb_logo_row(row, kind="networks")
    assert result is None


def test_mirror_external_logo_skips_upload_if_object_exists(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OBJECT_STORAGE_REGION", "us-east-1")
    monkeypatch.setenv("OBJECT_STORAGE_BUCKET", "bucket")
    monkeypatch.setenv("OBJECT_STORAGE_PUBLIC_BASE_URL", "https://cdn.example.com")

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
    monkeypatch.setenv("OBJECT_STORAGE_REGION", "us-east-1")
    monkeypatch.setenv("OBJECT_STORAGE_BUCKET", "bucket")
    monkeypatch.setenv("OBJECT_STORAGE_PUBLIC_BASE_URL", "https://cdn.example.com")

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

    monkeypatch.setenv("OBJECT_STORAGE_REGION", "us-east-1")
    monkeypatch.setenv("OBJECT_STORAGE_BUCKET", "bucket")
    monkeypatch.setenv("OBJECT_STORAGE_PUBLIC_BASE_URL", "https://cdn.example.com")

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
    monkeypatch.setenv("OBJECT_STORAGE_REGION", "us-east-1")
    monkeypatch.setenv("OBJECT_STORAGE_BUCKET", "bucket")
    monkeypatch.setenv("OBJECT_STORAGE_PUBLIC_BASE_URL", "https://cdn.example.com")

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
    monkeypatch.setenv("OBJECT_STORAGE_REGION", "us-east-1")
    monkeypatch.setenv("OBJECT_STORAGE_BUCKET", "bucket")
    monkeypatch.setenv("OBJECT_STORAGE_PUBLIC_BASE_URL", "https://cdn.example.com")
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
    monkeypatch.setenv("OBJECT_STORAGE_REGION", "us-east-1")
    monkeypatch.setenv("OBJECT_STORAGE_BUCKET", "bucket")
    monkeypatch.setenv("OBJECT_STORAGE_PUBLIC_BASE_URL", "https://cdn.example.com")

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
    monkeypatch.setenv("OBJECT_STORAGE_REGION", "us-east-1")
    monkeypatch.setenv("OBJECT_STORAGE_BUCKET", "bucket")
    monkeypatch.setenv("OBJECT_STORAGE_PUBLIC_BASE_URL", "https://cdn.example.com")

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
    monkeypatch.setenv("OBJECT_STORAGE_REGION", "us-east-1")
    monkeypatch.setenv("OBJECT_STORAGE_BUCKET", "bucket")
    monkeypatch.setenv("OBJECT_STORAGE_PUBLIC_BASE_URL", "https://cdn.example.com")

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


# ---------------------------------------------------------------------------
# yt-dlp Twitter video fallback tests
# ---------------------------------------------------------------------------


def test_mirror_url_to_s3_falls_back_to_ytdlp_for_twitter_video(monkeypatch: pytest.MonkeyPatch) -> None:
    """When a Twitter video URL returns 403, mirror_url_to_s3 should fall back
    to yt-dlp to resolve a fresh video URL from the tweet page, then retry."""
    monkeypatch.setenv("OBJECT_STORAGE_REGION", "us-east-1")
    monkeypatch.setenv("OBJECT_STORAGE_BUCKET", "bucket")
    monkeypatch.setenv("OBJECT_STORAGE_PUBLIC_BASE_URL", "https://cdn.example.com")

    expired_url = "https://video.twimg.com/ext_tw_video/12345/pu/vid/old.mp4?tag=12&token=expired"
    fresh_url = "https://video.twimg.com/ext_tw_video/12345/pu/vid/fresh.mp4?tag=12&token=new"
    tweet_url = "https://x.com/user/status/9999"

    call_count = 0

    class _FakeResponse403:
        headers = {"Content-Type": "video/mp4"}

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

        def raise_for_status(self) -> None:
            import requests as _req

            resp = _req.models.Response()
            resp.status_code = 403
            raise _req.exceptions.HTTPError(response=resp)

        def iter_content(self, chunk_size: int):  # noqa: ANN001
            del chunk_size
            return iter([])

    class _FakeResponseOK:
        headers = {"Content-Type": "video/mp4"}

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

        def raise_for_status(self) -> None:
            return None

        def iter_content(self, chunk_size: int):  # noqa: ANN001
            del chunk_size
            yield b"fresh-video-bytes"

    def _fake_get(*args, **kwargs):  # noqa: ANN001, ARG005
        nonlocal call_count
        call_count += 1
        url = args[0] if args else kwargs.get("url", "")
        if url == expired_url:
            return _FakeResponse403()
        return _FakeResponseOK()

    monkeypatch.setattr(s3_mirror.requests, "get", _fake_get)
    monkeypatch.setattr(s3_mirror, "_head_object", lambda *_args, **_kwargs: None)
    upload_mock = MagicMock(return_value=("etag-fresh", 17))
    monkeypatch.setattr(s3_mirror, "upload_bytes_to_s3", upload_mock)

    # Mock shutil.which to report yt-dlp as available
    monkeypatch.setattr(s3_mirror.shutil, "which", lambda cmd: "/usr/local/bin/yt-dlp" if cmd == "yt-dlp" else None)

    # Mock subprocess.run to return a fresh URL from yt-dlp
    ytdlp_payload = json.dumps({"url": fresh_url})
    fake_proc = MagicMock()
    fake_proc.returncode = 0
    fake_proc.stdout = ytdlp_payload
    fake_proc.stderr = ""
    subprocess_calls: list[list[str]] = []

    def _fake_subprocess_run(cmd, **kwargs):  # noqa: ANN001, ARG005
        subprocess_calls.append(cmd)
        return fake_proc

    monkeypatch.setattr(s3_mirror.subprocess, "run", _fake_subprocess_run)

    result = s3_mirror.mirror_url_to_s3(
        expired_url,
        s3_client=MagicMock(),
        bucket="bucket",
        tweet_url=tweet_url,
    )

    assert result.status == "mirrored"
    assert result.error is None
    assert result.hosted_url is not None and result.hosted_url.startswith("https://cdn.example.com/media/")
    # yt-dlp should have been called exactly once
    assert len(subprocess_calls) == 1
    assert "yt-dlp" in subprocess_calls[0][0]
    assert tweet_url in subprocess_calls[0]
    upload_mock.assert_called_once()


def test_mirror_url_to_s3_no_ytdlp_fallback_without_tweet_url(monkeypatch: pytest.MonkeyPatch) -> None:
    """When tweet_url is not provided, a 403 on a Twitter video URL should
    NOT trigger yt-dlp fallback — backward compatibility."""
    monkeypatch.setenv("OBJECT_STORAGE_REGION", "us-east-1")
    monkeypatch.setenv("OBJECT_STORAGE_BUCKET", "bucket")
    monkeypatch.setenv("OBJECT_STORAGE_PUBLIC_BASE_URL", "https://cdn.example.com")

    expired_url = "https://video.twimg.com/ext_tw_video/12345/pu/vid/old.mp4?tag=12&token=expired"

    class _FakeResponse403:
        headers = {"Content-Type": "video/mp4"}

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

        def raise_for_status(self) -> None:
            import requests as _req

            resp = _req.models.Response()
            resp.status_code = 403
            raise _req.exceptions.HTTPError(response=resp)

        def iter_content(self, chunk_size: int):  # noqa: ANN001
            del chunk_size
            return iter([])

    monkeypatch.setattr(s3_mirror.requests, "get", lambda *args, **kwargs: _FakeResponse403())  # noqa: ARG005

    result = s3_mirror.mirror_url_to_s3(
        expired_url,
        s3_client=MagicMock(),
        bucket="bucket",
        # No tweet_url — should NOT attempt yt-dlp
    )

    assert result.status == "failed"
    assert result.error == "http_403"
