from __future__ import annotations

import hashlib
import io
import json
import logging
import mimetypes
import os
import re
import shutil
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote, unquote, urlparse

import boto3
import requests
from botocore.client import Config
from botocore.exceptions import ClientError, ProfileNotFound

from trr_backend.object_storage import load_object_storage_config

_DEFAULT_HEADERS = {
    "accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}

_MEDIA_MIRROR_HEADERS = {
    "accept": "*/*",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
}
_MEDIA_MIRROR_CHUNK_SIZE_BYTES = 64 * 1024


def _is_image_content_type(value: str | None) -> bool:
    if not value:
        return False
    content_type = value.split(";", 1)[0].strip().lower()
    return content_type.startswith("image/")


def _looks_like_svg(data: bytes) -> bool:
    if not data:
        return False
    head = data.lstrip()[:4096].lower()
    if head.startswith(b"<svg"):
        return True
    return head.startswith(b"<?xml") and b"<svg" in head


def _sniff_image_content_type(data: bytes) -> str | None:
    if not data:
        return None
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if data.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if data.startswith(b"GIF87a") or data.startswith(b"GIF89a"):
        return "image/gif"
    if data.startswith(b"RIFF") and len(data) >= 12 and data[8:12] == b"WEBP":
        return "image/webp"
    if _looks_like_svg(data):
        return "image/svg+xml"
    return None


def svg_rasterizer_available() -> bool:
    try:
        import cairosvg  # type: ignore

        return bool(getattr(cairosvg, "svg2png", None))
    except Exception:
        return False


def _is_http_url(value: str | None) -> bool:
    if not isinstance(value, str):
        return False
    trimmed = value.strip().lower()
    return trimmed.startswith("http://") or trimmed.startswith("https://")


def normalize_fandom_file_url(url: str, *, referer: str | None) -> str:
    cleaned = (url or "").strip()
    if not cleaned:
        return cleaned
    if cleaned.startswith("//"):
        cleaned = f"https:{cleaned}"
    if cleaned.startswith("/wiki/"):
        parsed = urlparse(referer or "")
        base = (
            f"{parsed.scheme}://{parsed.netloc}"
            if parsed.scheme and parsed.netloc
            else "https://real-housewives.fandom.com"
        )
        cleaned = f"{base}{cleaned}"
    lowered = cleaned.lower()
    if "/wiki/file:" in lowered or "/wiki/file%3a" in lowered:
        parsed = urlparse(cleaned)
        path = unquote(parsed.path or "")
        if "file:" in path.lower():
            file_part = (
                path.split("File:", 1)[1].lstrip("/") if "File:" in path else path.split("file:", 1)[1].lstrip("/")
            )
            if file_part:
                file_part = quote(unquote(file_part), safe="")
                base = (
                    f"{parsed.scheme}://{parsed.netloc}"
                    if parsed.scheme and parsed.netloc
                    else "https://real-housewives.fandom.com"
                )
                cleaned = f"{base}/wiki/Special:FilePath/{file_part}"
    parsed = urlparse(cleaned)
    if parsed.netloc.lower().endswith("static.wikia.nocookie.net") and "/revision/latest" in parsed.path.lower():
        base_path = re.split(r"/revision/latest.*", parsed.path, maxsplit=1, flags=re.IGNORECASE)[0]
        cleaned = parsed._replace(path=base_path, query="", fragment="").geturl()
    return cleaned


# Backward compatibility for older call sites/tests.
def _normalize_fandom_file_url(url: str, *, referer: str | None) -> str:
    return normalize_fandom_file_url(url, referer=referer)


def _iter_unique_http_urls(candidates: list[str | None]) -> list[str]:
    seen: set[str] = set()
    urls: list[str] = []
    for value in candidates:
        if not _is_http_url(value):
            continue
        normalized = str(value).strip()
        if normalized in seen:
            continue
        seen.add(normalized)
        urls.append(normalized)
    return urls


def _build_cast_photo_download_urls(
    row: Mapping[str, Any],
    *,
    source: str,
    referer: str | None,
) -> list[str]:
    image_url = row.get("image_url")
    url = row.get("url")
    thumb_url = row.get("thumb_url")
    if source in {"fandom", "fandom-gallery"}:
        normalized = [
            normalize_fandom_file_url(str(value), referer=referer) if isinstance(value, str) else None
            for value in (image_url, url, thumb_url)
        ]
        return _iter_unique_http_urls([*normalized, image_url, url, thumb_url])
    return _iter_unique_http_urls([image_url, url, thumb_url])


@dataclass(frozen=True)
class S3Config:
    bucket: str
    region: str
    cdn_base_url: str
    prefix: str
    provider: str
    endpoint_url: str | None
    access_key_id: str | None
    secret_access_key: str | None
    session_token: str | None
    profile_name: str | None


HostedMediaStorageConfig = S3Config


def _validate_public_base_url(value: str) -> str:
    base = (value or "").strip()
    if not base:
        raise RuntimeError("Missing required environment variable: OBJECT_STORAGE_PUBLIC_BASE_URL")
    if not base.startswith("https://"):
        raise RuntimeError("OBJECT_STORAGE_PUBLIC_BASE_URL must start with https://")
    if "dxxxx" in base.lower():
        raise RuntimeError("OBJECT_STORAGE_PUBLIC_BASE_URL contains placeholder 'dxxxx'; set the real public host")
    parsed = urlparse(base)
    host = (parsed.netloc or "").strip().lower()
    if not host:
        raise RuntimeError("OBJECT_STORAGE_PUBLIC_BASE_URL must include a valid host")
    if host == "s3.amazonaws.com" or host.endswith(".s3.amazonaws.com"):
        raise RuntimeError("OBJECT_STORAGE_PUBLIC_BASE_URL must not be a direct S3 endpoint")
    if re.match(r"^s3[.-][a-z0-9-]+\.amazonaws\.com$", host):
        raise RuntimeError("OBJECT_STORAGE_PUBLIC_BASE_URL must not be a direct S3 endpoint")
    if re.match(r"^[a-z0-9.-]+\.s3[.-][a-z0-9-]+\.amazonaws\.com$", host):
        raise RuntimeError("OBJECT_STORAGE_PUBLIC_BASE_URL must not be a direct S3 endpoint")
    return base.rstrip("/")


def _load_hosted_media_storage_config() -> HostedMediaStorageConfig:
    storage = load_object_storage_config(require_bucket=True, require_public_base_url=True)
    profile_name = storage.profile_name
    return S3Config(
        bucket=storage.bucket,
        region=storage.region,
        cdn_base_url=_validate_public_base_url(storage.public_base_url or ""),
        prefix=storage.prefix,
        provider=storage.provider,
        endpoint_url=storage.endpoint_url,
        access_key_id=storage.access_key_id,
        secret_access_key=storage.secret_access_key,
        session_token=storage.session_token,
        profile_name=profile_name,
    )


def _load_s3_config() -> S3Config:
    return _load_hosted_media_storage_config()


def get_hosted_media_storage_config() -> HostedMediaStorageConfig:
    return _load_hosted_media_storage_config()


def get_s3_config() -> S3Config:
    return get_hosted_media_storage_config()


def _build_boto3_session(config: S3Config) -> boto3.Session:
    if config.profile_name:
        try:
            return boto3.Session(profile_name=config.profile_name, region_name=config.region)
        except ProfileNotFound:
            if config.access_key_id and config.secret_access_key:
                return boto3.Session(region_name=config.region)
            raise
    return boto3.Session(region_name=config.region)


def get_object_storage_client():
    config = get_hosted_media_storage_config()
    session = _build_boto3_session(config)
    client_kwargs: dict[str, Any] = {"region_name": config.region}
    if config.endpoint_url:
        client_kwargs["endpoint_url"] = config.endpoint_url
        client_kwargs["config"] = Config(signature_version="s3v4", s3={"addressing_style": "path"})
    if config.access_key_id and config.secret_access_key:
        client_kwargs["aws_access_key_id"] = config.access_key_id
        client_kwargs["aws_secret_access_key"] = config.secret_access_key
        if config.session_token:
            client_kwargs["aws_session_token"] = config.session_token
    return session.client("s3", **client_kwargs)


def get_s3_client():
    return get_object_storage_client()


def get_object_storage_bucket() -> str:
    return get_hosted_media_storage_config().bucket


def get_s3_bucket() -> str:
    return get_object_storage_bucket()


def get_s3_prefix() -> str:
    return get_hosted_media_storage_config().prefix


def get_public_base_url() -> str:
    return get_hosted_media_storage_config().cdn_base_url


def get_cdn_base_url() -> str:
    return get_public_base_url()


def build_public_object_url(hosted_key: str) -> str:
    key = str(hosted_key or "").strip()
    if not key:
        raise RuntimeError("hosted_key is required to build hosted_url")
    return f"{get_public_base_url()}/{key.lstrip('/')}"


def build_hosted_url(hosted_key: str) -> str:
    return build_public_object_url(hosted_key)


def guess_ext_from_content_type(content_type: str | None) -> str:
    if not content_type:
        return ".bin"
    ct = content_type.split(";", 1)[0].strip().lower()
    if ct == "image/webp":
        return ".webp"
    if ct in ("image/jpeg", "image/jpg"):
        return ".jpg"
    if ct == "image/png":
        return ".png"
    return ".bin"


def infer_media_extension(url: str, content_type: str | None) -> str:
    parsed = urlparse(str(url or "").strip())
    suffix = os.path.splitext(unquote(parsed.path or ""))[1].lower()
    if suffix in {".jpg", ".jpeg", ".png", ".gif", ".webp", ".mp4", ".mov", ".m4v", ".webm"}:
        return suffix

    normalized_ct = (content_type or "").split(";", 1)[0].strip().lower()
    guessed = mimetypes.guess_extension(normalized_ct) if normalized_ct else None
    if guessed in {".jpe"}:
        return ".jpg"
    if guessed:
        return guessed.lower()
    return guess_ext_from_content_type(content_type)


def _sanitize_path_segment(name: str) -> str:
    """Sanitize a name for use in object-storage paths (lowercase, hyphens, no special chars)."""
    if not name:
        return "unknown"
    # Lowercase and replace spaces/underscores with hyphens
    slug = name.lower().strip()
    slug = re.sub(r"[\s_]+", "-", slug)
    # Remove any characters that aren't alphanumeric or hyphens
    slug = re.sub(r"[^a-z0-9\-]", "", slug)
    # Collapse multiple hyphens
    slug = re.sub(r"-+", "-", slug)
    # Strip leading/trailing hyphens
    slug = slug.strip("-")
    return slug or "unknown"


def _sanitize_filename(filename: str) -> str:
    """Sanitize uploaded filenames for stable hosted-object keys."""
    raw = (filename or "").strip()
    if not raw:
        return "icon.bin"
    basename = os.path.basename(raw)
    stem, ext = os.path.splitext(basename)
    safe_stem = re.sub(r"[^A-Za-z0-9]+", "-", stem).strip("-").lower() or "icon"
    safe_ext = re.sub(r"[^A-Za-z0-9.]+", "", ext).lower()
    if not safe_ext.startswith("."):
        safe_ext = f".{safe_ext}" if safe_ext else ""
    if len(safe_ext) > 12:
        safe_ext = safe_ext[:12]
    return f"{safe_stem}{safe_ext or '.bin'}"


def build_icon_s3_key(show_key: str, filename: str) -> str:
    """
    Build the hosted-object key for show icons.

    Path: icons/{show_key}/{sanitized_filename}
    """
    safe_show_key = _sanitize_path_segment(show_key)
    safe_filename = _sanitize_filename(filename)
    return f"icons/{safe_show_key}/{safe_filename}"


def get_show_icon_s3_prefix(show_key: str) -> str:
    """Build the S3 prefix for a show's icons."""
    return f"icons/{_sanitize_path_segment(show_key)}/"


def build_cast_photo_s3_key(
    person_identifier: str,
    source: str,
    sha256: str,
    ext: str,
) -> str:
    """
    Build S3 key for cast photos.

    Path: images/people/{person_identifier}/photos/{source}/{sha256}.{ext}

    Args:
        person_identifier: IMDb person ID (nm...) preferred, or UUID as fallback
        source: Image source (fandom, imdb, tmdb)
        sha256: SHA256 hash of image content
        ext: File extension with leading dot
    """
    segments = [
        "images",
        "people",
        str(person_identifier),
        "photos",
        source,
        f"{sha256}{ext}",
    ]
    return "/".join(segments)


def build_shared_media_s3_key(sha256: str, ext: str) -> str:
    """
    Build a shared, content-addressed S3 key for identical bytes across entities.

    Path: media/{sha256[:2]}/{sha256}{ext}

    This is used by the unified media_assets model (and by cast photo mirroring as of TASK3)
    so that identical images converge to a single hosted object regardless of which person
    (or source) they were ingested under.
    """
    prefix = sha256[:2]
    return f"media/{prefix}/{sha256}{ext}"


def build_show_image_s3_key(
    show_identifier: str,
    kind: str,
    source: str,
    sha256: str,
    ext: str,
) -> str:
    """
    Build S3 key for show images using IMDb title ID or show UUID.

    Path: images/shows/{imdb_id_or_show_id}/{kind}/{source}/{sha256}.{ext}
    """
    segments = [
        "images",
        "shows",
        str(show_identifier),
        str(kind),
        source,
        f"{sha256}{ext}",
    ]
    return "/".join(segments)


def build_season_image_s3_key(
    show_identifier: str,
    season_number: int,
    source: str,
    sha256: str,
    ext: str,
) -> str:
    """
    Build S3 key for season images using show identifier.

    Path: images/seasons/{imdb_id_or_show_id}/season-{season_number}/{source}/{sha256}.{ext}
    """
    segments = [
        "images",
        "seasons",
        str(show_identifier),
        f"season-{int(season_number)}",
        source,
        f"{sha256}{ext}",
    ]
    return "/".join(segments)


def build_instagram_profile_pic_s3_key(
    username: str,
    sha256: str,
    ext: str,
) -> str:
    """
    Build S3 key for Instagram profile pictures.

    Path: social/instagram/profile-pics/{username}/{sha256}{ext}

    Content-addressed: same user + same image bytes = same key (dedup across posts).
    """
    safe_username = _sanitize_path_segment(username)
    segments = [
        "social",
        "instagram",
        "profile-pics",
        safe_username,
        f"{sha256}{ext}",
    ]
    return "/".join(segments)


def build_profile_pic_s3_key(
    platform: str,
    username: str,
    sha256: str,
    ext: str,
) -> str:
    """
    Build S3 key for profile pictures on any platform.

    Path: social/{platform}/profile-pics/{username}/{sha256}{ext}

    Content-addressed: same image bytes = same key (natural dedup).
    Old avatars preserved when account changes theirs (new SHA256 = new key).
    """
    safe_platform = _sanitize_path_segment(platform)
    safe_username = _sanitize_path_segment(username)
    segments = [
        "social",
        safe_platform,
        "profile-pics",
        safe_username,
        f"{sha256}{ext}",
    ]
    return "/".join(segments)


def build_episode_image_s3_key(
    episode_identifier: str,
    source: str,
    sha256: str,
    ext: str,
) -> str:
    """
    Build S3 key for episode images using IMDb title ID or episode UUID.

    Path: images/episodes/{episode_imdb_id_or_episode_id}/{source}/{sha256}.{ext}
    """
    segments = [
        "images",
        "episodes",
        str(episode_identifier),
        source,
        f"{sha256}{ext}",
    ]
    return "/".join(segments)


def build_logo_s3_key(
    kind: str,
    entity_id: str | int,
    sha256: str,
    ext: str,
) -> str:
    """
    Build S3 key for TMDb logo assets.

    Path: images/logos/{kind}/{entity_id}/{sha256}.{ext}
    """
    segments = [
        "images",
        "logos",
        str(kind),
        str(entity_id),
        f"{sha256}{ext}",
    ]
    return "/".join(segments)


def build_logo_variant_s3_key(
    kind: str,
    entity_id: str | int,
    variant: str,
    sha256: str,
    ext: str,
) -> str:
    """
    Build S3 key for monochrome logo variants.

    Path: images/logos/{kind}/{entity_id}/{variant}/{sha256}.{ext}
    """
    segments = [
        "images",
        "logos",
        str(kind),
        str(entity_id),
        str(variant),
        f"{sha256}{ext}",
    ]
    return "/".join(segments)


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def download_image(
    url: str,
    *,
    source: str,
    referer: str | None = None,
    headers: Mapping[str, str] | None = None,
) -> tuple[bytes, str | None]:
    merged = {**_DEFAULT_HEADERS, **(headers or {})}
    if source in {"fandom", "fandom-gallery"}:
        merged["referer"] = referer or "https://real-housewives.fandom.com/"
    resp = requests.get(url, headers=merged, timeout=(5, 30), stream=True)
    resp.raise_for_status()
    content_type = resp.headers.get("Content-Type")
    data = resp.content or b""
    if not data:
        raise RuntimeError("Empty image response")
    if not _is_image_content_type(content_type):
        sniffed = _sniff_image_content_type(data)
        if sniffed:
            content_type = sniffed
        else:
            raise RuntimeError(f"Non-image response content-type: {content_type}")
    return data, content_type


def _ensure_png_bytes(
    data: bytes,
    content_type: str | None,
) -> tuple[bytes, str, str] | None:
    """
    Return PNG-encoded bytes (data, content_type, ext) or None if conversion fails.
    """
    ct = (content_type or "").split(";", 1)[0].strip().lower()
    if ct == "image/png":
        return data, "image/png", ".png"

    # Prefer direct SVG rasterization when available.
    if ct in {"image/svg+xml", "image/svg"} or _looks_like_svg(data):
        try:
            import cairosvg  # type: ignore

            png_bytes = cairosvg.svg2png(bytestring=data)
            if png_bytes:
                return png_bytes, "image/png", ".png"
        except Exception:
            pass

    try:
        from PIL import Image  # type: ignore
    except Exception:
        return None

    try:
        image = Image.open(io.BytesIO(data))
        if image.mode not in ("RGB", "RGBA"):
            image = image.convert("RGBA")
        output = io.BytesIO()
        image.save(output, format="PNG")
        return output.getvalue(), "image/png", ".png"
    except Exception:
        return None


def _sanitize_etag(value: str | None) -> str | None:
    if not value:
        return None
    return value.strip('"')


def _extract_image_dimensions(data: bytes) -> tuple[int | None, int | None]:
    try:
        from PIL import Image  # type: ignore
    except Exception:
        return None, None
    try:
        with Image.open(io.BytesIO(data)) as image:
            width = int(image.width) if image.width else None
            height = int(image.height) if image.height else None
            return width, height
    except Exception:
        return None, None


def _is_meaningful_alpha(image) -> bool:
    if image.mode != "RGBA":
        return False
    alpha = image.getchannel("A")
    minimum, maximum = alpha.getextrema()
    if maximum <= 0:
        return False
    if minimum < 255:
        return True
    # Fully opaque alpha channel: treat as not meaningful.
    return False


def _derive_alpha_mask_from_opaque_logo(image):
    from PIL import Image, ImageChops, ImageFilter

    rgb = image.convert("RGB")
    width, height = rgb.size
    if width <= 2 or height <= 2:
        return None

    sample_points = [
        (0, 0),
        (width - 1, 0),
        (0, height - 1),
        (width - 1, height - 1),
        (width // 2, 0),
        (width // 2, height - 1),
        (0, height // 2),
        (width - 1, height // 2),
    ]
    colors = [rgb.getpixel(point) for point in sample_points]
    bg = tuple(int(sum(ch) / len(colors)) for ch in zip(*colors, strict=False))

    bg_img = Image.new("RGB", rgb.size, bg)
    diff = ImageChops.difference(rgb, bg_img).convert("L")
    # Start with a strict threshold; relax later via luminance fallback if needed.
    alpha = diff.point(lambda px: 255 if px > 18 else 0)
    alpha = alpha.filter(ImageFilter.MedianFilter(size=3))

    bbox = alpha.getbbox()
    if not bbox:
        return None
    coverage = (bbox[2] - bbox[0]) * (bbox[3] - bbox[1]) / max(width * height, 1)
    if coverage < 0.003:
        return None
    return alpha


def _derive_alpha_mask_luminance_fallback(image):
    from PIL import ImageStat

    gray = image.convert("L")
    mean = ImageStat.Stat(gray).mean[0]
    # If background is likely light, keep dark pixels; otherwise keep light pixels.
    threshold = 230 if mean > 127 else 25
    if mean > 127:
        alpha = gray.point(lambda px: 255 if px < threshold else 0)
    else:
        alpha = gray.point(lambda px: 255 if px > threshold else 0)

    bbox = alpha.getbbox()
    if not bbox:
        return None
    width, height = gray.size
    coverage = (bbox[2] - bbox[0]) * (bbox[3] - bbox[1]) / max(width * height, 1)
    if coverage < 0.003:
        return None
    return alpha


def _build_monochrome_logo_variants(
    data: bytes,
    content_type: str | None,
) -> tuple[tuple[bytes, str, str], tuple[bytes, str, str]]:
    try:
        from PIL import Image
    except Exception as exc:
        raise RuntimeError("logo_decode_failed") from exc

    try:
        image = Image.open(io.BytesIO(data))
    except Exception as exc:
        raise RuntimeError("logo_decode_failed") from exc

    if image.mode not in ("RGB", "RGBA"):
        image = image.convert("RGBA")

    if _is_meaningful_alpha(image):
        alpha = image.getchannel("A")
    else:
        alpha = _derive_alpha_mask_from_opaque_logo(image)
        if alpha is None:
            alpha = _derive_alpha_mask_luminance_fallback(image)
        if alpha is None:
            raise RuntimeError("transparent_extraction_failed")

    black = Image.new("RGBA", image.size, (0, 0, 0, 0))
    black.putalpha(alpha)

    white = Image.new("RGBA", image.size, (255, 255, 255, 0))
    white.putalpha(alpha)

    black_out = io.BytesIO()
    black.save(black_out, format="PNG")
    white_out = io.BytesIO()
    white.save(white_out, format="PNG")
    return (
        (black_out.getvalue(), "image/png", ".png"),
        (white_out.getvalue(), "image/png", ".png"),
    )


@dataclass(frozen=True)
class MonochromeLogoMirrorResult:
    patch: dict[str, Any]
    black_mirrored: int
    white_mirrored: int


def _apply_logo_variant_upload(
    *,
    row: Mapping[str, Any],
    kind: str,
    entity_id: str | int,
    variant: str,
    data: bytes,
    content_type: str,
    ext: str,
    force: bool,
    s3_client,
) -> tuple[dict[str, Any], int]:
    field_prefix = f"hosted_logo_{variant}"
    key_field = f"{field_prefix}_key"
    url_field = f"{field_prefix}_url"
    sha_field = f"{field_prefix}_sha256"
    content_type_field = f"{field_prefix}_content_type"
    bytes_field = f"{field_prefix}_bytes"
    etag_field = f"{field_prefix}_etag"
    at_field = f"{field_prefix}_at"

    existing_url = row.get(url_field)
    existing_key = row.get(key_field)
    existing_sha = row.get(sha_field)

    sha256 = _sha256_bytes(data)
    key = build_logo_variant_s3_key(
        kind=kind,
        entity_id=entity_id,
        variant=variant,
        sha256=sha256,
        ext=ext,
    )
    desired_url = build_hosted_url(key)

    if not force and existing_sha == sha256 and isinstance(existing_url, str) and existing_url.strip():
        patch: dict[str, Any] = {}
        if existing_key != key:
            patch[key_field] = key
        if existing_url != desired_url:
            patch[url_field] = desired_url
        return patch, 0

    bucket = get_s3_bucket()
    head = _head_object(s3_client, bucket, key)
    if head is None:
        etag, bytes_len = upload_bytes_to_s3(
            s3_client,
            bucket=bucket,
            key=key,
            data=data,
            content_type=content_type,
        )
        hosted_content_type = content_type
        hosted_bytes = bytes_len
        hosted_etag = etag
    else:
        hosted_content_type = head.get("ContentType") or content_type
        hosted_bytes = int(head.get("ContentLength")) if head.get("ContentLength") is not None else len(data)
        hosted_etag = _sanitize_etag(head.get("ETag"))

    mirrored = int(force or existing_sha != sha256 or not existing_key or not existing_url)
    patch = {
        key_field: key,
        url_field: desired_url,
        sha_field: sha256,
        content_type_field: hosted_content_type,
        bytes_field: hosted_bytes,
        etag_field: hosted_etag,
        at_field: datetime.now(UTC).isoformat(),
    }
    return patch, mirrored


def _head_object(s3_client, bucket: str, key: str) -> dict[str, Any] | None:
    try:
        return s3_client.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            return None
        raise


def upload_bytes_to_s3(
    s3_client,
    *,
    bucket: str,
    key: str,
    data: bytes,
    content_type: str,
) -> tuple[str | None, int]:
    response = s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=data,
        ContentType=content_type,
        CacheControl="public, max-age=31536000, immutable",
    )
    etag = _sanitize_etag(response.get("ETag"))
    return etag, len(data)


@dataclass(frozen=True)
class MirrorResult:
    source_url: str
    hosted_url: str | None
    hosted_key: str | None
    sha256: str | None
    content_type: str | None
    size_bytes: int | None
    status: str
    error: str | None


_log = logging.getLogger(__name__)


def _is_twitter_video_url(url: str) -> bool:
    """Return True if *url* is hosted on Twitter's video CDN."""
    try:
        return "video.twimg.com" in urlparse(url).netloc
    except Exception:  # noqa: BLE001
        return False


def _resolve_twitter_video_via_ytdlp(tweet_url: str) -> str | None:
    """Use yt-dlp to resolve a fresh direct video URL from *tweet_url*.

    Returns the resolved URL string, or ``None`` on any failure.
    """
    if not shutil.which("yt-dlp"):
        _log.debug("yt-dlp not found on PATH; skipping Twitter video fallback")
        return None

    cmd = [
        "yt-dlp",
        "--dump-single-json",
        "--no-playlist",
        "--skip-download",
        "--format",
        "best[ext=mp4]/best",
        tweet_url,
    ]

    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
    except Exception as exc:  # noqa: BLE001
        _log.warning("yt-dlp subprocess failed for %s: %s", tweet_url, exc)
        return None

    if proc.returncode != 0:
        message = (proc.stderr or proc.stdout or "").strip()[:240]
        _log.warning("yt-dlp exited %d for %s: %s", proc.returncode, tweet_url, message)
        return None

    try:
        payload = json.loads(proc.stdout or "{}")
    except json.JSONDecodeError as exc:
        _log.warning("yt-dlp JSON parse failed for %s: %s", tweet_url, exc)
        return None

    resolved = str(payload.get("url") or "").strip() or None
    if resolved:
        _log.info("yt-dlp resolved fresh video URL for %s", tweet_url)
    return resolved


def mirror_url_to_s3(
    url: str,
    *,
    s3_client=None,
    bucket: str | None = None,
    max_bytes: int = 50 * 1024 * 1024,
    tweet_url: str | None = None,
) -> MirrorResult:
    source_url = str(url or "").strip()
    if not _is_http_url(source_url):
        return MirrorResult(
            source_url=source_url,
            hosted_url=None,
            hosted_key=None,
            sha256=None,
            content_type=None,
            size_bytes=None,
            status="skipped",
            error="invalid_source_url",
        )
    try:
        max_bytes_limit = int(max_bytes)
    except (TypeError, ValueError):
        max_bytes_limit = 0
    if max_bytes_limit <= 0:
        return MirrorResult(
            source_url=source_url,
            hosted_url=None,
            hosted_key=None,
            sha256=None,
            content_type=None,
            size_bytes=None,
            status="failed",
            error="invalid_max_bytes",
        )

    try:
        s3 = s3_client or get_s3_client()
        target_bucket = str(bucket or "").strip() or get_s3_bucket()
    except Exception as exc:
        return MirrorResult(
            source_url=source_url,
            hosted_url=None,
            hosted_key=None,
            sha256=None,
            content_type=None,
            size_bytes=None,
            status="failed",
            error=f"s3_setup_failed:{exc}",
        )

    data_parts: list[bytes] = []
    size_bytes = 0
    digest = hashlib.sha256()
    content_type: str | None = None
    try:
        with requests.get(
            source_url,
            headers=_MEDIA_MIRROR_HEADERS,
            timeout=(10, 60),
            stream=True,
        ) as response:
            response.raise_for_status()
            content_type = (response.headers.get("Content-Type") or "").split(";", 1)[0].strip().lower() or None
            for chunk in response.iter_content(chunk_size=_MEDIA_MIRROR_CHUNK_SIZE_BYTES):
                if not chunk:
                    continue
                size_bytes += len(chunk)
                if size_bytes > max_bytes_limit:
                    return MirrorResult(
                        source_url=source_url,
                        hosted_url=None,
                        hosted_key=None,
                        sha256=None,
                        content_type=content_type,
                        size_bytes=size_bytes,
                        status="failed",
                        error="asset_too_large",
                    )
                digest.update(chunk)
                data_parts.append(chunk)
    except requests.exceptions.Timeout:
        return MirrorResult(
            source_url=source_url,
            hosted_url=None,
            hosted_key=None,
            sha256=None,
            content_type=content_type,
            size_bytes=size_bytes or None,
            status="failed",
            error="request_timeout",
        )
    except requests.exceptions.ConnectionError:
        return MirrorResult(
            source_url=source_url,
            hosted_url=None,
            hosted_key=None,
            sha256=None,
            content_type=content_type,
            size_bytes=size_bytes or None,
            status="failed",
            error="connection_error",
        )
    except requests.exceptions.HTTPError as exc:
        status_code = getattr(getattr(exc, "response", None), "status_code", None)
        reason = f"http_{int(status_code)}" if status_code is not None else "http_error"

        # -- yt-dlp fallback for expired Twitter video CDN URLs -----------
        if (
            status_code in (401, 403, 404)
            and tweet_url
            and _is_twitter_video_url(source_url)
        ):
            fresh_url = _resolve_twitter_video_via_ytdlp(tweet_url)
            if fresh_url and fresh_url != source_url:
                _log.info(
                    "Retrying mirror with fresh yt-dlp URL for %s",
                    source_url,
                )
                return mirror_url_to_s3(
                    fresh_url,
                    s3_client=s3,
                    bucket=target_bucket,
                    max_bytes=max_bytes_limit,
                    tweet_url=None,  # prevent infinite recursion
                )
        # -----------------------------------------------------------------

        return MirrorResult(
            source_url=source_url,
            hosted_url=None,
            hosted_key=None,
            sha256=None,
            content_type=content_type,
            size_bytes=size_bytes or None,
            status="failed",
            error=reason,
        )
    except requests.exceptions.RequestException:
        return MirrorResult(
            source_url=source_url,
            hosted_url=None,
            hosted_key=None,
            sha256=None,
            content_type=content_type,
            size_bytes=size_bytes or None,
            status="failed",
            error="request_error",
        )

    if size_bytes <= 0 or not data_parts:
        return MirrorResult(
            source_url=source_url,
            hosted_url=None,
            hosted_key=None,
            sha256=None,
            content_type=content_type,
            size_bytes=0,
            status="failed",
            error="empty_response_body",
        )

    data = b"".join(data_parts)
    sha256 = digest.hexdigest()
    ext = infer_media_extension(source_url, content_type)
    key = build_shared_media_s3_key(sha256, ext)

    try:
        head = _head_object(s3, target_bucket, key)
    except Exception as exc:
        return MirrorResult(
            source_url=source_url,
            hosted_url=None,
            hosted_key=key,
            sha256=sha256,
            content_type=content_type,
            size_bytes=size_bytes,
            status="failed",
            error=f"s3_head_failed:{exc.__class__.__name__}",
        )

    if head is None:
        try:
            _, uploaded_bytes = upload_bytes_to_s3(
                s3,
                bucket=target_bucket,
                key=key,
                data=data,
                content_type=content_type or "application/octet-stream",
            )
        except Exception as exc:
            return MirrorResult(
                source_url=source_url,
                hosted_url=None,
                hosted_key=key,
                sha256=sha256,
                content_type=content_type,
                size_bytes=size_bytes,
                status="failed",
                error=f"upload_failed:{exc.__class__.__name__}",
            )
        return MirrorResult(
            source_url=source_url,
            hosted_url=build_hosted_url(key),
            hosted_key=key,
            sha256=sha256,
            content_type=content_type or "application/octet-stream",
            size_bytes=uploaded_bytes,
            status="mirrored",
            error=None,
        )

    return MirrorResult(
        source_url=source_url,
        hosted_url=build_hosted_url(key),
        hosted_key=key,
        sha256=sha256,
        content_type=(head.get("ContentType") or content_type or "application/octet-stream"),
        size_bytes=int(head.get("ContentLength")) if head.get("ContentLength") is not None else size_bytes,
        status="skipped",
        error=None,
    )


def mirror_urls_to_s3(
    urls: list[str],
    *,
    s3_client=None,
    bucket: str | None = None,
    max_bytes: int = 50 * 1024 * 1024,
    tweet_url: str | None = None,
) -> list[MirrorResult]:
    results: list[MirrorResult] = []
    cache: dict[str, MirrorResult] = {}
    for raw_url in urls or []:
        source_url = str(raw_url or "").strip()
        if source_url in cache:
            cached = cache[source_url]
            results.append(
                MirrorResult(
                    source_url=source_url,
                    hosted_url=cached.hosted_url,
                    hosted_key=cached.hosted_key,
                    sha256=cached.sha256,
                    content_type=cached.content_type,
                    size_bytes=cached.size_bytes,
                    status=cached.status,
                    error=cached.error,
                )
            )
            continue
        try:
            result = mirror_url_to_s3(
                source_url,
                s3_client=s3_client,
                bucket=bucket,
                max_bytes=max_bytes,
                tweet_url=tweet_url,
            )
        except Exception as exc:
            result = MirrorResult(
                source_url=source_url,
                hosted_url=None,
                hosted_key=None,
                sha256=None,
                content_type=None,
                size_bytes=None,
                status="failed",
                error=f"mirror_exception:{exc.__class__.__name__}",
            )
        cache[source_url] = result
        results.append(result)
    return results


def mirror_reddit_media(
    *,
    source_url: str,
    reddit_post_id: str,
    reddit_comment_id: str | None = None,
    media_type: str = "image",
    s3_client=None,
    bucket: str | None = None,
    max_bytes: int = 50 * 1024 * 1024,
) -> dict[str, Any]:
    """Mirror a single Reddit media URL into hosted object storage and return a row dict.

    Handles dedup: if the source_url already has a 'mirrored' row for the same
    post (and optionally comment), the existing record is returned unchanged.
    """
    from trr_backend.db import pg  # lazy import to avoid circular deps

    source_url = str(source_url or "").strip()
    if not _is_http_url(source_url):
        return {
            "source_url": source_url,
            "reddit_post_id": reddit_post_id,
            "reddit_comment_id": reddit_comment_id,
            "media_type": media_type,
            "status": "skipped",
            "error_message": "invalid_url",
        }

    # Dedup: check if already mirrored for this post+url
    existing = pg.fetch_one(
        """
        select id, source_url, hosted_key, hosted_url, sha256, size_bytes,
               content_type, status, error_message
        from social.reddit_media_mirrors
        where reddit_post_id = %s
          and source_url = %s
          and status = 'mirrored'
        limit 1
        """,
        [reddit_post_id, source_url],
    )
    if existing:
        return dict(existing)

    result = mirror_url_to_s3(
        source_url,
        s3_client=s3_client,
        bucket=bucket,
        max_bytes=max_bytes,
    )

    row_data = {
        "reddit_post_id": reddit_post_id,
        "reddit_comment_id": reddit_comment_id,
        "source_url": source_url,
        "media_type": media_type,
        "hosted_key": result.hosted_key,
        "hosted_url": result.hosted_url,
        "sha256": result.sha256,
        "size_bytes": result.size_bytes,
        "content_type": result.content_type,
        "status": result.status if result.status in ("mirrored", "skipped") else "failed",
        "error_message": result.error,
    }

    try:
        inserted = pg.fetch_one(
            """
            insert into social.reddit_media_mirrors (
              reddit_post_id, reddit_comment_id, source_url, media_type,
              hosted_key, hosted_url, sha256, size_bytes, content_type,
              status, error_message
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            on conflict do nothing
            returning id
            """,
            [
                row_data["reddit_post_id"],
                row_data["reddit_comment_id"],
                row_data["source_url"],
                row_data["media_type"],
                row_data["hosted_key"],
                row_data["hosted_url"],
                row_data["sha256"],
                row_data["size_bytes"],
                row_data["content_type"],
                row_data["status"],
                row_data["error_message"],
            ],
        )
        if inserted:
            row_data["id"] = inserted.get("id")
    except Exception:  # noqa: BLE001
        # Non-fatal: mirror record insert failed but the S3 upload may have succeeded
        pass

    return row_data


def mirror_cast_photo_row(
    row: Mapping[str, Any],
    *,
    force: bool = False,
    s3_client=None,
) -> dict[str, Any] | None:
    hosted_url = row.get("hosted_url")
    hosted_key = row.get("hosted_key")
    if not force:
        if hosted_key:
            desired_url = build_hosted_url(hosted_key)
            if hosted_url != desired_url:
                return {"hosted_url": desired_url}
            if hosted_url:
                return None
        elif hosted_url:
            return None

    source = str(row.get("source") or "").strip() or "fandom"
    candidate_urls = _build_cast_photo_download_urls(row, source=source, referer=row.get("source_page_url"))
    if not candidate_urls:
        return None

    referer = row.get("source_page_url")
    used_url = candidate_urls[0]
    last_error: Exception | None = None
    for candidate_url in candidate_urls:
        used_url = candidate_url
        try:
            data, content_type = download_image(used_url, source=source, referer=referer)
            break
        except Exception as exc:
            last_error = exc
    else:
        if last_error is not None:
            raise last_error
        raise RuntimeError("Unable to download source image")
    sha256 = _sha256_bytes(data)
    current_sha = row.get("hosted_sha256")

    if current_sha and current_sha == sha256 and hosted_url and not force:
        return None

    ext = guess_ext_from_content_type(content_type)
    key = build_shared_media_s3_key(sha256, ext)
    bucket = get_s3_bucket()
    s3_client = s3_client or get_s3_client()

    head = _head_object(s3_client, bucket, key)
    if head is None:
        etag, bytes_len = upload_bytes_to_s3(
            s3_client,
            bucket=bucket,
            key=key,
            data=data,
            content_type=content_type or "application/octet-stream",
        )
        hosted_content_type = content_type or "application/octet-stream"
        hosted_bytes = bytes_len
        hosted_etag = etag
    else:
        hosted_content_type = head.get("ContentType") or content_type
        hosted_bytes = int(head.get("ContentLength")) if head.get("ContentLength") is not None else len(data)
        hosted_etag = _sanitize_etag(head.get("ETag"))

    hosted_url = build_hosted_url(key)
    hosted_at = datetime.now(UTC).isoformat()

    patch = {
        "hosted_bucket": bucket,
        "hosted_key": key,
        "hosted_url": hosted_url,
        "hosted_sha256": sha256,
        "hosted_content_type": hosted_content_type,
        "hosted_bytes": hosted_bytes,
        "hosted_etag": hosted_etag,
        "hosted_at": hosted_at,
    }
    if source in {"fandom", "fandom-gallery"} and isinstance(used_url, str) and used_url:
        if used_url != row.get("image_url") or used_url != row.get("url"):
            patch["image_url"] = used_url
            patch["url"] = used_url
            patch["image_url_canonical"] = used_url
    return patch


def mirror_media_asset_row(
    row: Mapping[str, Any],
    *,
    force: bool = False,
    s3_client=None,
) -> dict[str, Any] | None:
    hosted_url = row.get("hosted_url")
    hosted_key = row.get("hosted_key")
    if not force:
        if hosted_key:
            desired_url = build_hosted_url(hosted_key)
            if hosted_url != desired_url:
                return {"hosted_url": desired_url}
            if hosted_url:
                return None
        elif hosted_url:
            return None

    source_url = row.get("source_url")
    if not isinstance(source_url, str) or not source_url.strip():
        return None

    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    referer = (
        metadata.get("page_url")
        if isinstance(metadata.get("page_url"), str)
        else metadata.get("source_page_url")
        if isinstance(metadata.get("source_page_url"), str)
        else None
    )
    source = str(row.get("source") or "").strip().lower() or "web_scrape"

    from trr_backend.scraping.url_image_scraper import download_and_hash_image

    data, sha256, content_type = download_and_hash_image(source_url, referer=referer)
    current_sha = row.get("hosted_sha256")

    if current_sha and current_sha == sha256 and hosted_url and not force:
        return None

    ext = guess_ext_from_content_type(content_type)
    key = row.get("hosted_key") or build_shared_media_s3_key(sha256, ext)
    bucket = get_s3_bucket()
    s3_client = s3_client or get_s3_client()

    head = _head_object(s3_client, bucket, key)
    if head is None:
        etag, bytes_len = upload_bytes_to_s3(
            s3_client,
            bucket=bucket,
            key=key,
            data=data,
            content_type=content_type or "application/octet-stream",
        )
        hosted_content_type = content_type or "application/octet-stream"
        hosted_bytes = bytes_len
        hosted_etag = etag
    else:
        hosted_content_type = head.get("ContentType") or content_type
        hosted_bytes = int(head.get("ContentLength")) if head.get("ContentLength") is not None else len(data)
        hosted_etag = _sanitize_etag(head.get("ETag"))

    hosted_url = build_hosted_url(key)
    hosted_at = datetime.now(UTC).isoformat()
    width, height = _extract_image_dimensions(data)
    metadata_out = dict(metadata or {})
    metadata_out["mirrored_at"] = hosted_at
    metadata_out.setdefault("mirrored_from", source_url)

    patch: dict[str, Any] = {
        "source": source,
        "sha256": sha256,
        "hosted_bucket": bucket,
        "hosted_key": key,
        "hosted_url": hosted_url,
        "hosted_sha256": sha256,
        "hosted_content_type": hosted_content_type,
        "hosted_bytes": hosted_bytes,
        "hosted_etag": hosted_etag,
        "hosted_at": hosted_at,
        "metadata": metadata_out,
    }
    if isinstance(width, int) and width > 0:
        patch["width"] = width
    if isinstance(height, int) and height > 0:
        patch["height"] = height
    return patch


def _get_tmdb_original_url(file_path: str) -> str:
    """Build TMDb original resolution URL from file_path."""
    return f"https://image.tmdb.org/t/p/original{file_path}"


def mirror_tmdb_logo_row(
    row: Mapping[str, Any],
    *,
    kind: str,
    id_field: str = "id",
    logo_path_field: str = "tmdb_logo_path",
    force: bool = False,
    s3_client=None,
) -> dict[str, Any] | None:
    """
    Mirror a TMDb logo image into hosted object storage and return hosted_logo_* fields.
    """
    hosted_url = row.get("hosted_logo_url")
    hosted_key = row.get("hosted_logo_key")
    if not force:
        if hosted_key:
            desired_url = build_hosted_url(hosted_key)
            if hosted_url != desired_url:
                return {"hosted_logo_url": desired_url}
            if hosted_url:
                return None
        elif hosted_url:
            return None

    logo_path = row.get(logo_path_field)
    if not isinstance(logo_path, str) or not logo_path.strip():
        return None

    entity_id = row.get(id_field)
    if entity_id is None:
        return None

    candidate_url = _get_tmdb_original_url(logo_path)
    data, content_type = download_image(candidate_url, source="tmdb")
    png_payload = _ensure_png_bytes(data, content_type)
    if not png_payload:
        return None
    png_bytes, png_content_type, ext = png_payload
    sha256 = _sha256_bytes(png_bytes)
    current_sha = row.get("hosted_logo_sha256")

    if current_sha and current_sha == sha256 and hosted_url and not force:
        return None

    key = build_logo_s3_key(
        kind=kind,
        entity_id=entity_id,
        sha256=sha256,
        ext=ext,
    )
    bucket = get_s3_bucket()
    s3_client = s3_client or get_s3_client()

    head = _head_object(s3_client, bucket, key)
    if head is None:
        etag, bytes_len = upload_bytes_to_s3(
            s3_client,
            bucket=bucket,
            key=key,
            data=png_bytes,
            content_type=png_content_type,
        )
        hosted_content_type = png_content_type
        hosted_bytes = bytes_len
        hosted_etag = etag
    else:
        hosted_content_type = head.get("ContentType") or png_content_type
        hosted_bytes = int(head.get("ContentLength")) if head.get("ContentLength") is not None else len(png_bytes)
        hosted_etag = _sanitize_etag(head.get("ETag"))

    hosted_url = build_hosted_url(key)
    hosted_at = datetime.now(UTC).isoformat()

    return {
        "logo_path": key,
        "hosted_logo_key": key,
        "hosted_logo_url": hosted_url,
        "hosted_logo_sha256": sha256,
        "hosted_logo_content_type": hosted_content_type,
        "hosted_logo_bytes": hosted_bytes,
        "hosted_logo_etag": hosted_etag,
        "hosted_logo_at": hosted_at,
    }


def mirror_external_logo_row(
    row: Mapping[str, Any],
    *,
    kind: str,
    source_url: str,
    id_field: str = "id",
    force: bool = False,
    s3_client=None,
    source: str = "wikimedia",
) -> dict[str, Any] | None:
    """
    Mirror an external logo URL (for example Wikimedia) into hosted object storage.
    """
    hosted_url = row.get("hosted_logo_url")
    hosted_key = row.get("hosted_logo_key")
    if not force:
        if hosted_key:
            desired_url = build_hosted_url(hosted_key)
            if hosted_url != desired_url:
                return {"hosted_logo_url": desired_url}
            if hosted_url:
                return None
        elif hosted_url:
            return None

    candidate_url = str(source_url or "").strip()
    if not candidate_url:
        return None

    entity_id = row.get(id_field)
    if entity_id is None:
        return None

    data, content_type = download_image(candidate_url, source=source)
    png_payload = _ensure_png_bytes(data, content_type)
    if not png_payload:
        return None
    png_bytes, png_content_type, ext = png_payload
    sha256 = _sha256_bytes(png_bytes)
    current_sha = row.get("hosted_logo_sha256")

    if current_sha and current_sha == sha256 and hosted_url and not force:
        return None

    key = build_logo_s3_key(
        kind=kind,
        entity_id=entity_id,
        sha256=sha256,
        ext=ext,
    )
    bucket = get_s3_bucket()
    s3_client = s3_client or get_s3_client()

    head = _head_object(s3_client, bucket, key)
    if head is None:
        etag, bytes_len = upload_bytes_to_s3(
            s3_client,
            bucket=bucket,
            key=key,
            data=png_bytes,
            content_type=png_content_type,
        )
        hosted_content_type = png_content_type
        hosted_bytes = bytes_len
        hosted_etag = etag
    else:
        hosted_content_type = head.get("ContentType") or png_content_type
        hosted_bytes = int(head.get("ContentLength")) if head.get("ContentLength") is not None else len(png_bytes)
        hosted_etag = _sanitize_etag(head.get("ETag"))

    hosted_url = build_hosted_url(key)
    hosted_at = datetime.now(UTC).isoformat()

    return {
        "logo_path": key,
        "hosted_logo_key": key,
        "hosted_logo_url": hosted_url,
        "hosted_logo_sha256": sha256,
        "hosted_logo_content_type": hosted_content_type,
        "hosted_logo_bytes": hosted_bytes,
        "hosted_logo_etag": hosted_etag,
        "hosted_logo_at": hosted_at,
    }


def mirror_logo_monochrome_variants_row(
    row: Mapping[str, Any],
    *,
    kind: str,
    source_url: str,
    id_field: str = "id",
    force: bool = False,
    s3_client=None,
    source: str = "wikimedia",
) -> MonochromeLogoMirrorResult | None:
    """
    Generate and mirror black/white transparent logo variants into hosted object storage.
    """
    entity_id = row.get(id_field)
    if entity_id is None:
        return None
    candidate_url = str(source_url or "").strip()
    if not candidate_url:
        return None

    existing_black = str(row.get("hosted_logo_black_url") or "").strip()
    existing_white = str(row.get("hosted_logo_white_url") or "").strip()
    if not force and existing_black and existing_white:
        return None

    s3_client = s3_client or get_s3_client()
    data, content_type = download_image(candidate_url, source=source)
    try:
        black_payload, white_payload = _build_monochrome_logo_variants(data, content_type)
    except RuntimeError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("transparent_extraction_failed") from exc

    black_patch, black_mirrored = _apply_logo_variant_upload(
        row=row,
        kind=kind,
        entity_id=entity_id,
        variant="black",
        data=black_payload[0],
        content_type=black_payload[1],
        ext=black_payload[2],
        force=force,
        s3_client=s3_client,
    )
    white_patch, white_mirrored = _apply_logo_variant_upload(
        row=row,
        kind=kind,
        entity_id=entity_id,
        variant="white",
        data=white_payload[0],
        content_type=white_payload[1],
        ext=white_payload[2],
        force=force,
        s3_client=s3_client,
    )
    patch = {**black_patch, **white_patch}
    if not patch:
        return None

    return MonochromeLogoMirrorResult(
        patch=patch,
        black_mirrored=black_mirrored,
        white_mirrored=white_mirrored,
    )


def mirror_show_image_row(
    row: Mapping[str, Any],
    *,
    force: bool = False,
    s3_client=None,
) -> dict[str, Any] | None:
    """
    Mirror a show image into hosted object storage.

    For TMDb images with file_path, always uses original resolution.
    Returns patch dict with hosted_* fields, or None if already hosted.
    """
    hosted_url = row.get("hosted_url")
    hosted_key = row.get("hosted_key")
    if not force:
        if hosted_key:
            desired_url = build_hosted_url(hosted_key)
            if hosted_url != desired_url:
                return {"hosted_url": desired_url}
            if hosted_url:
                return None
        elif hosted_url:
            return None

    source = str(row.get("source") or "").strip() or "imdb"

    # Get IMDb ID from joined shows table - required for S3 path
    imdb_id = None
    shows_data = row.get("shows")
    if isinstance(shows_data, dict):
        imdb_id = shows_data.get("imdb_id")
    show_identifier = imdb_id or row.get("show_id")
    if not show_identifier:
        return None  # Can't build S3 path without an identifier
    kind = str(row.get("kind") or "media").strip() or "media"

    # Determine the source URL to download
    # For TMDb: prefer original resolution via file_path
    # For IMDb: use the url field directly
    file_path = row.get("file_path")
    if source == "tmdb" and file_path:
        candidate_url = _get_tmdb_original_url(file_path)
    else:
        candidate_url = row.get("url")

    if not candidate_url:
        return None

    # Download the image (no special referer needed for TMDb/IMDb)
    data, content_type = download_image(candidate_url, source=source)
    sha256 = _sha256_bytes(data)
    current_sha = row.get("hosted_sha256")

    if current_sha and current_sha == sha256 and hosted_url and not force:
        return None

    ext = guess_ext_from_content_type(content_type)
    key = build_show_image_s3_key(
        show_identifier=str(show_identifier),
        kind=kind,
        source=source,
        sha256=sha256,
        ext=ext,
    )
    bucket = get_s3_bucket()
    s3_client = s3_client or get_s3_client()

    head = _head_object(s3_client, bucket, key)
    if head is None:
        etag, bytes_len = upload_bytes_to_s3(
            s3_client,
            bucket=bucket,
            key=key,
            data=data,
            content_type=content_type or "application/octet-stream",
        )
        hosted_content_type = content_type or "application/octet-stream"
        hosted_bytes = bytes_len
        hosted_etag = etag
    else:
        hosted_content_type = head.get("ContentType") or content_type
        hosted_bytes = int(head.get("ContentLength")) if head.get("ContentLength") is not None else len(data)
        hosted_etag = _sanitize_etag(head.get("ETag"))

    hosted_url = build_hosted_url(key)
    hosted_at = datetime.now(UTC).isoformat()

    return {
        "hosted_bucket": bucket,
        "hosted_key": key,
        "hosted_url": hosted_url,
        "hosted_sha256": sha256,
        "hosted_content_type": hosted_content_type,
        "hosted_bytes": hosted_bytes,
        "hosted_etag": hosted_etag,
        "hosted_at": hosted_at,
    }


def mirror_season_image_row(
    row: Mapping[str, Any],
    *,
    force: bool = False,
    s3_client=None,
) -> dict[str, Any] | None:
    """
    Mirror a season image into hosted object storage.
    """
    hosted_url = row.get("hosted_url")
    hosted_key = row.get("hosted_key")
    if not force:
        if hosted_key:
            desired_url = build_hosted_url(hosted_key)
            if hosted_url != desired_url:
                return {"hosted_url": desired_url}
            if hosted_url:
                return None
        elif hosted_url:
            return None

    source = str(row.get("source") or "").strip() or "tmdb"
    season_number = row.get("season_number")
    if not isinstance(season_number, int):
        return None

    imdb_id = None
    shows_data = row.get("shows")
    if isinstance(shows_data, dict):
        imdb_id = shows_data.get("imdb_id")
    show_identifier = imdb_id or row.get("show_id") or row.get("season_id")
    if not show_identifier:
        return None

    file_path = row.get("file_path")
    url_original = row.get("url_original")
    if source == "tmdb" and isinstance(file_path, str) and file_path.strip():
        candidate_url = _get_tmdb_original_url(file_path)
    elif isinstance(url_original, str) and url_original.strip():
        candidate_url = url_original
    else:
        return None

    data, content_type = download_image(candidate_url, source=source)
    sha256 = _sha256_bytes(data)
    current_sha = row.get("hosted_sha256")

    if current_sha and current_sha == sha256 and hosted_url and not force:
        return None

    ext = guess_ext_from_content_type(content_type)
    key = build_season_image_s3_key(
        show_identifier=str(show_identifier),
        season_number=season_number,
        source=source,
        sha256=sha256,
        ext=ext,
    )
    bucket = get_s3_bucket()
    s3_client = s3_client or get_s3_client()

    head = _head_object(s3_client, bucket, key)
    if head is None:
        etag, bytes_len = upload_bytes_to_s3(
            s3_client,
            bucket=bucket,
            key=key,
            data=data,
            content_type=content_type or "application/octet-stream",
        )
        hosted_content_type = content_type or "application/octet-stream"
        hosted_bytes = bytes_len
        hosted_etag = etag
    else:
        hosted_content_type = head.get("ContentType") or content_type
        hosted_bytes = int(head.get("ContentLength")) if head.get("ContentLength") is not None else len(data)
        hosted_etag = _sanitize_etag(head.get("ETag"))

    hosted_url = build_hosted_url(key)
    hosted_at = datetime.now(UTC).isoformat()

    return {
        "hosted_bucket": bucket,
        "hosted_key": key,
        "hosted_url": hosted_url,
        "hosted_sha256": sha256,
        "hosted_content_type": hosted_content_type,
        "hosted_bytes": hosted_bytes,
        "hosted_etag": hosted_etag,
        "hosted_at": hosted_at,
    }


# ---------------------------------------------------------------------------
# S3 Prune Functions
# ---------------------------------------------------------------------------


def list_s3_objects_under_prefix(
    s3_client,
    bucket: str,
    prefix: str,
) -> list[str]:
    """
    List all S3 object keys under a given prefix.

    Args:
        s3_client: boto3 S3 client
        bucket: S3 bucket name
        prefix: Key prefix to list under (e.g., "images/people/nm123/photos/")

    Returns:
        List of full object keys
    """
    keys: list[str] = []
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        contents = page.get("Contents", [])
        for obj in contents:
            key = obj.get("Key")
            if key:
                keys.append(key)

    return keys


def delete_s3_objects(
    s3_client,
    bucket: str,
    keys: list[str],
) -> int:
    """
    Batch delete S3 objects by key.

    Uses DeleteObjects API for efficiency (up to 1000 keys per request).

    Args:
        s3_client: boto3 S3 client
        bucket: S3 bucket name
        keys: List of object keys to delete

    Returns:
        Count of successfully deleted objects
    """
    if not keys:
        return 0

    deleted_count = 0
    # DeleteObjects supports up to 1000 keys per request
    chunk_size = 1000

    for i in range(0, len(keys), chunk_size):
        chunk = keys[i : i + chunk_size]
        delete_request = {
            "Objects": [{"Key": k} for k in chunk],
            "Quiet": True,
        }

        try:
            response = s3_client.delete_objects(Bucket=bucket, Delete=delete_request)
            # In Quiet mode, only errors are returned
            errors = response.get("Errors", [])
            deleted_count += len(chunk) - len(errors)
        except ClientError:
            # If the whole batch fails, count nothing
            pass

    return deleted_count


def get_person_s3_prefix(person_identifier: str) -> str:
    """
    Build the S3 prefix for a person's photos.

    Args:
        person_identifier: IMDb person ID (nm...) or person UUID

    Returns:
        S3 prefix like "images/people/nm123/photos/"
    """
    return f"images/people/{person_identifier}/photos/"


def get_show_s3_prefix(show_identifier: str) -> str:
    """
    Build the S3 prefix for a show's images.

    Args:
        show_identifier: IMDb title ID (tt...) or show UUID

    Returns:
        S3 prefix like "images/shows/tt123/"
    """
    return f"images/shows/{show_identifier}/"


def get_season_s3_prefix(show_identifier: str) -> str:
    """
    Build the S3 prefix for a show's season images.
    """
    return f"images/seasons/{show_identifier}/"


def prune_orphaned_cast_photo_objects(
    db,
    person_identifier: str,
    *,
    dry_run: bool = False,
    verbose: bool = False,
    s3_client=None,
) -> list[str]:
    """
    Delete S3 objects under a person's prefix that aren't referenced in cast_photos.

    This function:
    1. Lists all S3 objects under the person's photo prefix
    2. Queries the database for all hosted_key values for that person
    3. Deletes any S3 objects not referenced by the database

    Args:
        db: Supabase client
        person_identifier: IMDb person ID (nm...) or person UUID for S3 prefix
        dry_run: If True, only report what would be deleted
        verbose: If True, print detailed progress
        s3_client: Optional boto3 S3 client (creates one if not provided)

    Returns:
        List of orphaned keys (deleted or would be deleted if dry_run)
    """
    from trr_backend.repositories.cast_photos import fetch_hosted_keys_for_person

    bucket = get_s3_bucket()
    s3_client = s3_client or get_s3_client()
    prefix = get_person_s3_prefix(person_identifier)

    # 1. List all S3 objects under this person's prefix
    s3_keys = set(list_s3_objects_under_prefix(s3_client, bucket, prefix))

    if verbose:
        print(f"  S3 objects under {prefix}: {len(s3_keys)}")

    if not s3_keys:
        return []

    # 2. Get all hosted_key values from database for this person
    db_keys = fetch_hosted_keys_for_person(db, person_identifier)

    if verbose:
        print(f"  DB hosted_key references: {len(db_keys)}")

    # 3. Find orphaned keys (in S3 but not referenced by DB)
    orphaned = s3_keys - db_keys

    if not orphaned:
        if verbose:
            print("  No orphaned S3 objects found.")
        return []

    if verbose or dry_run:
        for key in sorted(orphaned):
            action = "WOULD DELETE" if dry_run else "DELETING"
            print(f"  {action}: {key}")

    # 4. Delete orphaned objects
    if not dry_run:
        deleted_count = delete_s3_objects(s3_client, bucket, list(orphaned))
        if verbose:
            print(f"  Deleted {deleted_count} orphaned objects")

    return list(orphaned)


def prune_orphaned_show_image_objects(
    db,
    show_identifier: str,
    *,
    show_id: str,
    dry_run: bool = False,
    verbose: bool = False,
    s3_client=None,
) -> list[str]:
    """
    Delete S3 objects under a show's prefix that aren't referenced in show_images.
    """
    from trr_backend.repositories.show_images import fetch_hosted_keys_for_show

    bucket = get_s3_bucket()
    s3_client = s3_client or get_s3_client()
    prefix = get_show_s3_prefix(show_identifier)

    s3_keys = set(list_s3_objects_under_prefix(s3_client, bucket, prefix))
    if verbose:
        print(f"  S3 objects under {prefix}: {len(s3_keys)}")
    if not s3_keys:
        return []

    db_keys = fetch_hosted_keys_for_show(db, show_id=show_id)
    if verbose:
        print(f"  DB hosted_key references: {len(db_keys)}")

    orphaned = s3_keys - db_keys
    if not orphaned:
        if verbose:
            print("  No orphaned S3 objects found.")
        return []

    if verbose or dry_run:
        for key in sorted(orphaned):
            action = "WOULD DELETE" if dry_run else "DELETING"
            print(f"  {action}: {key}")

    if not dry_run:
        deleted_count = delete_s3_objects(s3_client, bucket, list(orphaned))
        if verbose:
            print(f"  Deleted {deleted_count} orphaned objects")

    return list(orphaned)


def prune_orphaned_season_image_objects(
    db,
    show_identifier: str,
    *,
    show_id: str,
    dry_run: bool = False,
    verbose: bool = False,
    s3_client=None,
) -> list[str]:
    """
    Delete S3 objects under a show's season prefix that aren't referenced in season_images.
    """
    from trr_backend.repositories.season_images import fetch_hosted_keys_for_show

    bucket = get_s3_bucket()
    s3_client = s3_client or get_s3_client()
    prefix = get_season_s3_prefix(show_identifier)

    s3_keys = set(list_s3_objects_under_prefix(s3_client, bucket, prefix))
    if verbose:
        print(f"  S3 objects under {prefix}: {len(s3_keys)}")
    if not s3_keys:
        return []

    db_keys = fetch_hosted_keys_for_show(db, show_id=show_id)
    if verbose:
        print(f"  DB hosted_key references: {len(db_keys)}")

    orphaned = s3_keys - db_keys
    if not orphaned:
        if verbose:
            print("  No orphaned S3 objects found.")
        return []

    if verbose or dry_run:
        for key in sorted(orphaned):
            action = "WOULD DELETE" if dry_run else "DELETING"
            print(f"  {action}: {key}")

    if not dry_run:
        deleted_count = delete_s3_objects(s3_client, bucket, list(orphaned))
        if verbose:
            print(f"  Deleted {deleted_count} orphaned objects")

    return list(orphaned)


def mirror_episode_image_row(
    row: Mapping[str, Any],
    *,
    force: bool = False,
    s3_client=None,
) -> dict[str, Any] | None:
    """
    Mirror an episode image into hosted object storage.

    For TMDb images with file_path, always uses original resolution.
    Returns patch dict with hosted_* fields, or None if already hosted.
    """
    hosted_url = row.get("hosted_url")
    hosted_key = row.get("hosted_key")
    if not force:
        if hosted_key:
            desired_url = build_hosted_url(hosted_key)
            if hosted_url != desired_url:
                return {"hosted_url": desired_url}
            if hosted_url:
                return None
        elif hosted_url:
            return None

    source = str(row.get("source") or "").strip() or "tmdb"

    # Get episode identifier for S3 path: prefer IMDb ID, fallback to episode_id UUID
    episode_identifier = None
    episodes_data = row.get("episodes")
    if isinstance(episodes_data, dict):
        episode_identifier = episodes_data.get("imdb_id")
    if not episode_identifier:
        episode_identifier = row.get("episode_id")
    if not episode_identifier:
        return None  # Can't build S3 path without an identifier

    # Determine the source URL to download
    file_path = row.get("file_path")
    url = row.get("url")
    url_original = row.get("url_original")

    if source == "tmdb" and isinstance(file_path, str) and file_path.strip():
        candidate_url = _get_tmdb_original_url(file_path)
    elif isinstance(url, str) and url.strip():
        candidate_url = url
    elif isinstance(url_original, str) and url_original.strip():
        candidate_url = url_original
    else:
        return None

    # Download the image
    data, content_type = download_image(candidate_url, source=source)
    sha256 = _sha256_bytes(data)
    current_sha = row.get("hosted_sha256")

    if current_sha and current_sha == sha256 and hosted_url and not force:
        return None

    ext = guess_ext_from_content_type(content_type)
    key = build_episode_image_s3_key(
        episode_identifier=str(episode_identifier),
        source=source,
        sha256=sha256,
        ext=ext,
    )
    bucket = get_s3_bucket()
    s3_client = s3_client or get_s3_client()

    head = _head_object(s3_client, bucket, key)
    if head is None:
        etag, bytes_len = upload_bytes_to_s3(
            s3_client,
            bucket=bucket,
            key=key,
            data=data,
            content_type=content_type or "application/octet-stream",
        )
        hosted_content_type = content_type or "application/octet-stream"
        hosted_bytes = bytes_len
        hosted_etag = etag
    else:
        hosted_content_type = head.get("ContentType") or content_type
        hosted_bytes = int(head.get("ContentLength")) if head.get("ContentLength") is not None else len(data)
        hosted_etag = _sanitize_etag(head.get("ETag"))

    hosted_url = build_hosted_url(key)
    hosted_at = datetime.now(UTC).isoformat()

    return {
        "hosted_bucket": bucket,
        "hosted_key": key,
        "hosted_url": hosted_url,
        "hosted_sha256": sha256,
        "hosted_content_type": hosted_content_type,
        "hosted_bytes": hosted_bytes,
        "hosted_etag": hosted_etag,
        "hosted_at": hosted_at,
    }


def get_episode_s3_prefix(episode_identifier: str) -> str:
    """
    Build the S3 prefix for an episode's images.

    Args:
        episode_identifier: IMDb episode ID (tt...) or episode UUID

    Returns:
        S3 prefix like "images/episodes/tt123/"
    """
    return f"images/episodes/{episode_identifier}/"


def prune_orphaned_episode_image_objects(
    db,
    episode_identifier: str,
    *,
    episode_id: str,
    dry_run: bool = False,
    verbose: bool = False,
    s3_client=None,
) -> list[str]:
    """
    Delete S3 objects under an episode's prefix that aren't referenced in episode_images.
    """
    from trr_backend.repositories.episode_images import fetch_hosted_keys_for_episode

    bucket = get_s3_bucket()
    s3_client = s3_client or get_s3_client()
    prefix = get_episode_s3_prefix(episode_identifier)

    s3_keys = set(list_s3_objects_under_prefix(s3_client, bucket, prefix))
    if verbose:
        print(f"  S3 objects under {prefix}: {len(s3_keys)}")
    if not s3_keys:
        return []

    db_keys = fetch_hosted_keys_for_episode(db, episode_id=episode_id)
    if verbose:
        print(f"  DB hosted_key references: {len(db_keys)}")

    orphaned = s3_keys - db_keys
    if not orphaned:
        if verbose:
            print("  No orphaned S3 objects found.")
        return []

    if verbose or dry_run:
        for key in sorted(orphaned):
            action = "WOULD DELETE" if dry_run else "DELETING"
            print(f"  {action}: {key}")

    if not dry_run:
        deleted_count = delete_s3_objects(s3_client, bucket, list(orphaned))
        if verbose:
            print(f"  Deleted {deleted_count} orphaned objects")

    return list(orphaned)


def mirror_reddit_media_batch(
    items: list[dict[str, Any]],
    *,
    max_concurrent: int = 5,  # noqa: ARG001 — reserved for future async upgrade
) -> list[dict[str, Any]]:
    """
    Mirror a batch of Reddit media items sequentially.

    Each item in *items* must contain the keyword arguments accepted by
    :func:`mirror_reddit_media`: ``source_url``, ``reddit_post_id``,
    and optionally ``reddit_comment_id`` and ``media_type``.

    Returns a list of result dicts (one per input item).
    """
    results: list[dict[str, Any]] = []
    for item in items or []:
        try:
            result = mirror_reddit_media(
                source_url=item.get("source_url", ""),
                reddit_post_id=item.get("reddit_post_id", ""),
                reddit_comment_id=item.get("reddit_comment_id"),
                media_type=item.get("media_type", "image"),
            )
        except Exception as exc:
            result = {
                "id": None,
                "source_url": item.get("source_url", ""),
                "hosted_key": None,
                "hosted_url": None,
                "sha256": None,
                "size_bytes": None,
                "content_type": None,
                "status": "failed",
                "error": f"mirror_exception:{exc.__class__.__name__}",
            }
        results.append(result)
    return results
