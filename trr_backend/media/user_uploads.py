"""
User media uploads with S3 presigned POST flow.

This module handles user-uploaded images:
- source_url = NULL (no external source)
- hosted_url is the source of truth
- ingest_status = 'hosted' (no mirroring needed)

Flow:
1. initiate: Create upload session, generate presigned POST
2. (client): Upload directly to S3 temp location
3. finalize: Validate, compute SHA256, move to canonical key, create asset+link
"""

from __future__ import annotations

import hashlib
import io
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4, uuid5

from botocore.exceptions import ClientError
from PIL import Image

from trr_backend.media.s3_mirror import (
    get_cdn_base_url,
    get_s3_bucket,
    get_s3_client,
    guess_ext_from_content_type,
)

if TYPE_CHECKING:
    from trr_backend.db.session import DbSession
else:
    DbSession = Any

# Configuration
MAX_UPLOAD_BYTES = 10 * 1024 * 1024  # 10 MB
UPLOAD_EXPIRY_HOURS = 1
ALLOWED_CONTENT_TYPES = frozenset({"image/jpeg", "image/png", "image/webp"})
ALLOWED_ENTITY_TYPES = frozenset({"show", "season", "episode", "person"})
ALLOWED_KINDS = frozenset({"poster", "backdrop", "logo", "profile", "still", "gallery"})

# UUID namespace for user upload asset IDs (content-addressed by SHA256)
_USER_UPLOAD_ASSET_NAMESPACE = UUID("8e4f3c2d-1a5b-4e6f-9d8c-7b2a1e3f4c5d")


@dataclass
class MediaUploadSession:
    """Result of creating an upload session."""

    upload_id: str
    bucket: str
    temp_key: str
    presigned_url: str
    presigned_fields: dict[str, str]
    expires_at: datetime


@dataclass
class FinalizedUpload:
    """Result of finalizing an upload session."""

    asset_id: str
    link_id: str
    hosted_url: str
    canonical_key: str
    sha256: str
    width: int | None
    height: int | None


def _sanitize_filename(filename: str | None) -> str:
    """Sanitize a filename for use in S3 paths."""
    if not filename:
        return "upload"
    # Remove directory separators and dangerous characters
    name = re.sub(r"[/\\:*?\"<>|]", "", filename)
    # Remove path traversal sequences (.. or consecutive dots at start)
    name = re.sub(r"\.{2,}", "", name)
    # Limit length
    name = name[:100] if len(name) > 100 else name
    return name.strip() or "upload"


def _build_temp_key(
    entity_type: str,
    entity_id: str,
    upload_id: str,
    original_filename: str | None,
) -> str:
    """Build the temporary S3 key for an upload."""
    sanitized = _sanitize_filename(original_filename)
    return f"uploads/{entity_type}/{entity_id}/{upload_id}/{sanitized}"


def _build_canonical_key(sha256: str, ext: str) -> str:
    """Build the content-addressed canonical S3 key."""
    prefix = sha256[:2]
    return f"media/{prefix}/{sha256}{ext}"


def _asset_id_for_sha256(sha256: str) -> UUID:
    """Generate deterministic asset ID from SHA256 hash."""
    return uuid5(_USER_UPLOAD_ASSET_NAMESPACE, f"user_upload:{sha256}")


def _sha256_bytes(data: bytes) -> str:
    """Compute SHA256 hex digest of bytes."""
    return hashlib.sha256(data).hexdigest()


def _extract_image_dimensions(data: bytes) -> tuple[int | None, int | None]:
    """Extract width and height from image data using Pillow."""
    try:
        image = Image.open(io.BytesIO(data))
        return image.width, image.height
    except Exception:
        return None, None


def _validate_content_type(content_type: str) -> None:
    """Validate content type is in the allowed list."""
    ct = content_type.split(";", 1)[0].strip().lower()
    if ct not in ALLOWED_CONTENT_TYPES:
        allowed = ", ".join(sorted(ALLOWED_CONTENT_TYPES))
        raise ValueError(f"Invalid content_type '{content_type}'. Allowed: {allowed}")


def _validate_entity_type(entity_type: str) -> None:
    """Validate entity type is in the allowed list."""
    if entity_type not in ALLOWED_ENTITY_TYPES:
        allowed = ", ".join(sorted(ALLOWED_ENTITY_TYPES))
        raise ValueError(f"Invalid entity_type '{entity_type}'. Allowed: {allowed}")


def _validate_kind(kind: str) -> None:
    """Validate kind is in the allowed list."""
    if kind not in ALLOWED_KINDS:
        allowed = ", ".join(sorted(ALLOWED_KINDS))
        raise ValueError(f"Invalid kind '{kind}'. Allowed: {allowed}")


def _validate_expected_bytes(expected_bytes: int | None) -> None:
    """Validate expected bytes is within limit."""
    if expected_bytes is not None and expected_bytes > MAX_UPLOAD_BYTES:
        raise ValueError(f"expected_bytes ({expected_bytes}) exceeds maximum ({MAX_UPLOAD_BYTES})")


def generate_presigned_post(
    bucket: str,
    key: str,
    content_type: str,
    max_bytes: int,
    *,
    expires_in: int = 3600,
    s3_client: Any | None = None,
) -> dict[str, Any]:
    """
    Generate presigned POST with enforcement conditions.

    Conditions enforced at S3 level:
    - Content-Type must match declared type
    - Content-Length-Range (0, max_bytes)

    Returns:
        dict with 'url' and 'fields' for the presigned POST
    """
    client = s3_client or get_s3_client()

    # Enforce content-type and size at S3 level
    conditions = [
        {"Content-Type": content_type},
        ["content-length-range", 0, max_bytes],
    ]

    fields = {"Content-Type": content_type}

    response = client.generate_presigned_post(
        Bucket=bucket,
        Key=key,
        Fields=fields,
        Conditions=conditions,
        ExpiresIn=expires_in,
    )

    return response


def create_media_upload_session(
    db: DbSession,
    *,
    entity_type: str,
    entity_id: str,
    kind: str,
    content_type: str,
    expected_bytes: int | None = None,
    original_filename: str | None = None,
    caption: str | None = None,
    alt_text: str | None = None,
    make_primary: bool = False,
    uploader_user_id: str | None = None,
    s3_client: Any | None = None,
) -> MediaUploadSession:
    """
    Create an upload session and return presigned POST info.

    Args:
        db: Supabase client
        entity_type: 'show', 'season', 'episode', or 'person'
        entity_id: UUID of the target entity
        kind: 'poster', 'backdrop', 'logo', 'profile', 'still', or 'gallery'
        content_type: MIME type (must be image/jpeg, image/png, or image/webp)
        expected_bytes: Optional expected file size (must be <= 10MB)
        original_filename: Optional original filename for reference
        caption: Optional caption for the image
        alt_text: Optional alt text for accessibility
        make_primary: Whether to make this the primary image for entity+kind
        uploader_user_id: Optional user ID of the uploader

    Returns:
        MediaUploadSession with presigned POST info

    Raises:
        ValueError: If validation fails
    """
    # Validate inputs
    _validate_entity_type(entity_type)
    _validate_kind(kind)
    _validate_content_type(content_type)
    _validate_expected_bytes(expected_bytes)

    # Generate upload ID and build temp key
    upload_id = str(uuid4())
    bucket = get_s3_bucket()
    temp_key = _build_temp_key(entity_type, entity_id, upload_id, original_filename)

    # Calculate expiry
    now = datetime.now(UTC)
    expires_at = now + timedelta(hours=UPLOAD_EXPIRY_HOURS)
    expires_in_seconds = int(UPLOAD_EXPIRY_HOURS * 3600)

    # Generate presigned POST
    presigned = generate_presigned_post(
        bucket=bucket,
        key=temp_key,
        content_type=content_type,
        max_bytes=expected_bytes or MAX_UPLOAD_BYTES,
        expires_in=expires_in_seconds,
        s3_client=s3_client,
    )

    # Create database record
    insert_payload = {
        "id": upload_id,
        "uploader_user_id": uploader_user_id,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "kind": kind,
        "original_filename": original_filename,
        "content_type": content_type,
        "expected_bytes": expected_bytes,
        "caption": caption,
        "alt_text": alt_text,
        "make_primary": make_primary,
        "status": "initiated",
        "expires_at": expires_at.isoformat(),
        "s3_bucket": bucket,
        "s3_temp_key": temp_key,
    }

    # Remove None values
    insert_payload = {k: v for k, v in insert_payload.items() if v is not None}

    response = db.schema("core").table("media_uploads").insert(insert_payload).execute()
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Failed to create upload session: {response.error}")

    return MediaUploadSession(
        upload_id=upload_id,
        bucket=bucket,
        temp_key=temp_key,
        presigned_url=presigned["url"],
        presigned_fields=presigned["fields"],
        expires_at=expires_at,
    )


def _head_object(s3_client: Any, bucket: str, key: str) -> dict[str, Any] | None:
    """Get object metadata, or None if not found."""
    try:
        return s3_client.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            return None
        raise


def _get_object_bytes(s3_client: Any, bucket: str, key: str) -> bytes:
    """Download object and return bytes."""
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()


def _copy_object(
    s3_client: Any,
    bucket: str,
    source_key: str,
    dest_key: str,
    content_type: str,
) -> dict[str, Any]:
    """Server-side copy of an object."""
    return s3_client.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": source_key},
        Key=dest_key,
        ContentType=content_type,
        MetadataDirective="REPLACE",
        CacheControl="public, max-age=31536000, immutable",
    )


def _delete_object(s3_client: Any, bucket: str, key: str) -> None:
    """Delete an object (best-effort, ignores errors)."""
    try:
        s3_client.delete_object(Bucket=bucket, Key=key)
    except Exception:
        pass  # Best-effort cleanup


def _sanitize_etag(value: str | None) -> str | None:
    """Remove quotes from ETag if present."""
    if not value:
        return None
    return value.strip('"')


def finalize_media_upload_session(
    db: DbSession,
    upload_id: str,
    *,
    s3_client: Any | None = None,
) -> FinalizedUpload:
    """
    Finalize an upload: validate, move to canonical, create asset+link.

    Steps:
    1. Load upload session; reject if expired/canceled/finalized
    2. HEAD temp object; enforce size <= max, content-type in allowlist
    3. Stream object to compute SHA256
    4. Extract image dimensions (width/height)
    5. Build canonical key: media/{sha256[:2]}/{sha256}{ext}
    6. If canonical object exists: reuse (dedup by SHA256)
    7. Copy temp -> canonical (server-side), delete temp
    8. Upsert media_asset with source_url=NULL, ingest_status='hosted'
    9. Insert media_link with entity context
    10. If make_primary=true: call set_primary_media_link() RPC
    11. Update media_uploads: status='finalized', link IDs
    12. Return asset + link identifiers and URLs

    Args:
        db: Supabase client
        upload_id: The upload session ID

    Returns:
        FinalizedUpload with asset/link details

    Raises:
        ValueError: If session is invalid or expired
        RuntimeError: If S3 operations fail
    """
    client = s3_client or get_s3_client()
    now = datetime.now(UTC)

    # 1. Load upload session
    response = db.schema("core").table("media_uploads").select("*").eq("id", upload_id).single().execute()
    if hasattr(response, "error") and response.error:
        raise ValueError(f"Upload session not found: {upload_id}")

    session = response.data
    if not session:
        raise ValueError(f"Upload session not found: {upload_id}")

    status = session.get("status")
    if status == "finalized":
        raise ValueError(f"Upload session already finalized: {upload_id}")
    if status in ("expired", "canceled"):
        raise ValueError(f"Upload session is {status}: {upload_id}")

    # Check expiry
    expires_at_str = session.get("expires_at")
    if expires_at_str:
        expires_at = datetime.fromisoformat(expires_at_str.replace("Z", "+00:00"))
        if now > expires_at:
            # Mark as expired
            db.schema("core").table("media_uploads").update(
                {"status": "expired", "error": "Session expired before finalization"}
            ).eq("id", upload_id).execute()
            raise ValueError(f"Upload session expired: {upload_id}")

    bucket = session["s3_bucket"]
    temp_key = session["s3_temp_key"]
    entity_type = session["entity_type"]
    entity_id = session["entity_id"]
    kind = session["kind"]
    make_primary = session.get("make_primary", False)
    caption = session.get("caption")
    alt_text = session.get("alt_text")

    # 2. HEAD temp object
    head = _head_object(client, bucket, temp_key)
    if head is None:
        db.schema("core").table("media_uploads").update(
            {"status": "failed", "error": "Temp object not found in S3"}
        ).eq("id", upload_id).execute()
        raise RuntimeError(f"Temp object not found: {temp_key}")

    actual_size = head.get("ContentLength", 0)
    actual_content_type = head.get("ContentType", "")

    # Validate size
    if actual_size > MAX_UPLOAD_BYTES:
        db.schema("core").table("media_uploads").update(
            {"status": "failed", "error": f"File too large: {actual_size} bytes"}
        ).eq("id", upload_id).execute()
        _delete_object(client, bucket, temp_key)
        raise ValueError(f"File too large: {actual_size} > {MAX_UPLOAD_BYTES}")

    # Validate content type
    ct_base = actual_content_type.split(";", 1)[0].strip().lower()
    if ct_base not in ALLOWED_CONTENT_TYPES:
        db.schema("core").table("media_uploads").update(
            {"status": "failed", "error": f"Invalid content type: {actual_content_type}"}
        ).eq("id", upload_id).execute()
        _delete_object(client, bucket, temp_key)
        raise ValueError(f"Invalid content type: {actual_content_type}")

    # 3. Download and compute SHA256
    data = _get_object_bytes(client, bucket, temp_key)
    sha256 = _sha256_bytes(data)

    # 4. Extract dimensions
    width, height = _extract_image_dimensions(data)

    # 5. Build canonical key
    ext = guess_ext_from_content_type(ct_base)
    canonical_key = _build_canonical_key(sha256, ext)

    # 6. Check if canonical exists (dedup)
    canonical_exists = _head_object(client, bucket, canonical_key) is not None

    # 7. Copy temp -> canonical if needed
    if not canonical_exists:
        _copy_object(client, bucket, temp_key, canonical_key, ct_base)

    # Delete temp object
    _delete_object(client, bucket, temp_key)

    # Build hosted URL
    cdn_base = get_cdn_base_url()
    hosted_url = f"{cdn_base}/{canonical_key}"

    # 8. Upsert media_asset
    asset_id = str(_asset_id_for_sha256(sha256))
    asset_payload = {
        "id": asset_id,
        "media_type": "image",
        "source": "user_upload",
        "source_url": None,  # Critical: no external source
        "width": width,
        "height": height,
        "caption": caption,
        "alt_text": alt_text,
        "hosted_bucket": bucket,
        "hosted_key": canonical_key,
        "hosted_url": hosted_url,
        "hosted_sha256": sha256,
        "hosted_content_type": ct_base,
        "hosted_bytes": actual_size,
        "hosted_at": now.isoformat(),
        "ingest_status": "hosted",  # Prevents mirror loop
        "ingest_completed_at": now.isoformat(),
    }

    # Remove None values except source_url (explicitly NULL)
    asset_clean = {k: v for k, v in asset_payload.items() if v is not None or k == "source_url"}

    asset_response = (
        db.schema("core").table("media_assets").upsert(asset_clean, on_conflict="id", default_to_null=False).execute()
    )
    if hasattr(asset_response, "error") and asset_response.error:
        raise RuntimeError(f"Failed to upsert media_asset: {asset_response.error}")

    # 9. Insert media_link
    link_id = str(uuid4())
    link_payload = {
        "id": link_id,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "media_asset_id": asset_id,
        "kind": kind,
        "is_primary": False,  # Will be set by RPC if make_primary=true
        "context": {},
    }

    link_response = db.schema("core").table("media_links").insert(link_payload).execute()
    if hasattr(link_response, "error") and link_response.error:
        raise RuntimeError(f"Failed to insert media_link: {link_response.error}")

    # 10. Set primary if requested (atomic RPC)
    if make_primary:
        set_primary_media_link(db, entity_type, entity_id, kind, link_id)

    # 11. Update media_uploads status
    db.schema("core").table("media_uploads").update(
        {
            "status": "finalized",
            "media_asset_id": asset_id,
            "media_link_id": link_id,
        }
    ).eq("id", upload_id).execute()

    return FinalizedUpload(
        asset_id=asset_id,
        link_id=link_id,
        hosted_url=hosted_url,
        canonical_key=canonical_key,
        sha256=sha256,
        width=width,
        height=height,
    )


def set_primary_media_link(
    db: DbSession,
    entity_type: str,
    entity_id: str,
    kind: str,
    media_link_id: str,
) -> None:
    """
    Atomically set a link as primary, unsetting others.

    Calls the core.set_primary_media_link() RPC which:
    - Locks rows for the entity+kind
    - Unsets all existing primaries
    - Sets the specified link as primary
    - Validates the link belongs to the entity+kind

    Args:
        db: Supabase client
        entity_type: 'show', 'season', 'episode', or 'person'
        entity_id: UUID of the entity
        kind: Image kind ('poster', 'backdrop', etc.)
        media_link_id: The link to make primary

    Raises:
        RuntimeError: If the RPC fails
    """
    response = (
        db.schema("core")
        .rpc(
            "set_primary_media_link",
            {
                "p_entity_type": entity_type,
                "p_entity_id": entity_id,
                "p_kind": kind,
                "p_media_link_id": media_link_id,
            },
        )
        .execute()
    )

    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Failed to set primary media link: {response.error}")


def cancel_media_upload_session(
    db: DbSession,
    upload_id: str,
    *,
    s3_client: Any | None = None,
) -> None:
    """
    Cancel an upload session and clean up temp S3 object.

    Args:
        db: Supabase client
        upload_id: The upload session ID

    Raises:
        ValueError: If session not found or already finalized
    """
    client = s3_client or get_s3_client()

    # Load session
    response = db.schema("core").table("media_uploads").select("*").eq("id", upload_id).single().execute()
    if hasattr(response, "error") and response.error:
        raise ValueError(f"Upload session not found: {upload_id}")

    session = response.data
    if not session:
        raise ValueError(f"Upload session not found: {upload_id}")

    status = session.get("status")
    if status == "finalized":
        raise ValueError(f"Cannot cancel finalized upload: {upload_id}")

    # Delete temp object (best-effort)
    bucket = session["s3_bucket"]
    temp_key = session["s3_temp_key"]
    _delete_object(client, bucket, temp_key)

    # Update status
    db.schema("core").table("media_uploads").update(
        {
            "status": "canceled",
        }
    ).eq("id", upload_id).execute()


def get_upload_session_status(
    db: DbSession,
    upload_id: str,
) -> dict[str, Any] | None:
    """
    Get the status of an upload session.

    Args:
        db: Supabase client
        upload_id: The upload session ID

    Returns:
        Session data dict or None if not found
    """
    response = (
        db.schema("core")
        .table("media_uploads")
        .select("id,status,expires_at,error,media_asset_id,media_link_id")
        .eq("id", upload_id)
        .single()
        .execute()
    )
    if hasattr(response, "error") and response.error:
        return None
    return response.data
