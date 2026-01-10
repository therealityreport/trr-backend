from __future__ import annotations

import hashlib
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import boto3
import requests
from botocore.exceptions import ClientError

_DEFAULT_HEADERS = {
    "accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}


@dataclass(frozen=True)
class S3Config:
    bucket: str
    region: str
    cdn_base_url: str
    prefix: str
    profile_name: str | None


def _require_env(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _require_region() -> str:
    region = (os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "").strip()
    if not region:
        raise RuntimeError("Missing required environment variable: AWS_REGION (or AWS_DEFAULT_REGION)")
    return region


def _validate_cdn_base_url(value: str) -> str:
    base = (value or "").strip()
    if not base:
        raise RuntimeError("Missing required environment variable: AWS_CDN_BASE_URL")
    if not base.startswith("https://"):
        raise RuntimeError("AWS_CDN_BASE_URL must start with https://")
    if "dxxxx" in base.lower():
        raise RuntimeError("AWS_CDN_BASE_URL contains placeholder 'dxxxx'; set the real CDN domain")
    return base.rstrip("/")


def _load_s3_config() -> S3Config:
    bucket = _require_env("AWS_S3_BUCKET")
    region = _require_region()
    cdn_base_url = _validate_cdn_base_url(_require_env("AWS_CDN_BASE_URL"))
    prefix = (os.getenv("AWS_S3_PREFIX") or "").strip().strip("/")
    profile_name = (os.getenv("AWS_PROFILE") or os.getenv("AWS_DEFAULT_PROFILE") or "").strip() or None
    return S3Config(
        bucket=bucket,
        region=region,
        cdn_base_url=cdn_base_url,
        prefix=prefix,
        profile_name=profile_name,
    )


def get_s3_config() -> S3Config:
    return _load_s3_config()


def _build_boto3_session(config: S3Config) -> boto3.Session:
    if config.profile_name:
        return boto3.Session(profile_name=config.profile_name, region_name=config.region)
    return boto3.Session(region_name=config.region)


def get_s3_client():
    config = get_s3_config()
    access_key = (os.getenv("AWS_ACCESS_KEY_ID") or "").strip()
    secret_key = (os.getenv("AWS_SECRET_ACCESS_KEY") or "").strip()

    session = _build_boto3_session(config)
    if config.profile_name:
        return session.client("s3", region_name=config.region)
    if access_key and secret_key:
        return session.client(
            "s3",
            region_name=config.region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
    return session.client("s3", region_name=config.region)


def get_s3_bucket() -> str:
    return get_s3_config().bucket


def get_s3_prefix() -> str:
    return get_s3_config().prefix


def get_cdn_base_url() -> str:
    return get_s3_config().cdn_base_url


def build_hosted_url(hosted_key: str) -> str:
    key = str(hosted_key or "").strip()
    if not key:
        raise RuntimeError("hosted_key is required to build hosted_url")
    return f"{get_cdn_base_url()}/{key.lstrip('/')}"


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


def _sanitize_path_segment(name: str) -> str:
    """Sanitize a name for use in S3 paths (lowercase, hyphens, no special chars)."""
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

    try:
        import io

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
    candidate_url = row.get("image_url") or row.get("url") or row.get("thumb_url")
    if not candidate_url:
        return None

    # Get person identifier for S3 path: prefer IMDb ID, fallback to person_id UUID
    person_identifier = row.get("imdb_person_id")
    if not person_identifier:
        # Try to get from joined people table
        people_data = row.get("people")
        if isinstance(people_data, dict):
            external_ids = people_data.get("external_ids") or {}
            person_identifier = external_ids.get("imdb")
    if not person_identifier:
        # Fallback to person_id UUID if no IMDb ID available
        person_identifier = row.get("person_id")
    if not person_identifier:
        return None  # Can't build S3 path without any identifier

    referer = row.get("source_page_url")
    data, content_type = download_image(candidate_url, source=source, referer=referer)
    sha256 = _sha256_bytes(data)
    current_sha = row.get("hosted_sha256")

    if current_sha and current_sha == sha256 and hosted_url and not force:
        return None

    ext = guess_ext_from_content_type(content_type)
    key = build_cast_photo_s3_key(
        person_identifier=person_identifier,
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
    Mirror a TMDb logo image to S3 and return hosted_logo_* fields.
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


def mirror_show_image_row(
    row: Mapping[str, Any],
    *,
    force: bool = False,
    s3_client=None,
) -> dict[str, Any] | None:
    """
    Mirror a show image to S3.

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
    Mirror a season image to S3.
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
