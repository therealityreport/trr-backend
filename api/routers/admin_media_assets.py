"""Admin endpoints for managing media_assets."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from api.auth import AdminUser
from api.deps import SupabaseAdminClient
from trr_backend.media.s3_mirror import (
    build_hosted_url,
    get_s3_bucket,
    get_s3_client,
    guess_ext_from_content_type,
    upload_bytes_to_s3,
)
from trr_backend.repositories.media_assets import (
    update_asset_with_mirror_result,
    update_ingest_status,
)
from trr_backend.scraping.url_image_scraper import download_and_hash_image

router = APIRouter(prefix="/admin", tags=["admin-media-assets"])


class MirrorMediaAssetRequest(BaseModel):
    force: bool = False
    referer: str | None = None


class MirrorMediaAssetResponse(BaseModel):
    asset_id: str
    hosted_url: str | None = None
    hosted_key: str | None = None
    status: str
    bytes: int | None = None
    content_type: str | None = None


class DeleteMediaAssetResponse(BaseModel):
    asset_id: str
    deleted_links: int
    deleted_asset: bool
    s3_deleted: bool
    s3_error: str | None = None


def _build_media_asset_s3_key(sha256: str, ext: str) -> str:
    return f"media/{sha256[:2]}/{sha256}{ext}"


@router.post("/media-assets/{asset_id}/mirror", response_model=MirrorMediaAssetResponse)
def mirror_media_asset(
    asset_id: UUID,
    payload: MirrorMediaAssetRequest | None = None,
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
) -> MirrorMediaAssetResponse:
    asset_id_str = str(asset_id)
    payload = payload or MirrorMediaAssetRequest()

    response = (
        db.schema("core")
        .table("media_assets")
        .select("id, source_url, hosted_url, hosted_key, metadata")
        .eq("id", asset_id_str)
        .limit(1)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise HTTPException(status_code=502, detail="Database error fetching media asset")
    if not response.data:
        raise HTTPException(status_code=404, detail="Media asset not found")

    row = response.data[0]
    if row.get("hosted_url") and not payload.force:
        return MirrorMediaAssetResponse(
            asset_id=asset_id_str,
            hosted_url=row.get("hosted_url"),
            hosted_key=row.get("hosted_key"),
            status="skipped",
        )

    source_url = row.get("source_url")
    if not source_url:
        raise HTTPException(status_code=409, detail="Media asset has no source_url to mirror")

    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    referer = payload.referer or (metadata.get("page_url") if isinstance(metadata, dict) else None)

    update_ingest_status(
        db,
        asset_id_str,
        "in_progress",
    )

    try:
        image_bytes, sha256, content_type = download_and_hash_image(
            source_url,
            referer=referer,
        )
    except Exception as exc:
        update_ingest_status(
            db,
            asset_id_str,
            "failed",
            error=str(exc),
            failed_at=datetime.now(UTC).isoformat(),
        )
        raise HTTPException(status_code=502, detail=f"Failed to download source_url: {exc}") from exc

    ext = guess_ext_from_content_type(content_type)
    hosted_key = row.get("hosted_key") or _build_media_asset_s3_key(sha256, ext)

    try:
        s3_client = get_s3_client()
        bucket = get_s3_bucket()
        etag, file_size = upload_bytes_to_s3(
            s3_client,
            bucket=bucket,
            key=hosted_key,
            data=image_bytes,
            content_type=content_type,
        )
        hosted_url = build_hosted_url(hosted_key)
        update_asset_with_mirror_result(
            db,
            asset_id=asset_id_str,
            sha256=sha256,
            hosted_bucket=bucket,
            hosted_key=hosted_key,
            hosted_url=hosted_url,
            hosted_bytes=file_size,
            hosted_content_type=content_type,
            hosted_etag=etag,
            completed_at=datetime.now(UTC).isoformat(),
        )
    except Exception as exc:
        update_ingest_status(
            db,
            asset_id_str,
            "failed",
            error=str(exc),
            failed_at=datetime.now(UTC).isoformat(),
        )
        raise HTTPException(status_code=502, detail=f"Failed to upload to S3: {exc}") from exc

    return MirrorMediaAssetResponse(
        asset_id=asset_id_str,
        hosted_url=hosted_url,
        hosted_key=hosted_key,
        status="hosted",
        bytes=file_size,
        content_type=content_type,
    )


@router.delete("/media-assets/{asset_id}", response_model=DeleteMediaAssetResponse)
def delete_media_asset(
    asset_id: UUID,
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
) -> DeleteMediaAssetResponse:
    """
    Delete a media asset from the unified media_assets/media_links model.

    This is intended for admin cleanup of web scrape imports.
    Deletes:
    - core.media_links rows referencing the asset
    - core.media_assets row
    - best-effort S3 object delete (hosted_key) if present
    """
    asset_id_str = str(asset_id)

    response = (
        db.schema("core")
        .table("media_assets")
        .select("id, hosted_bucket, hosted_key")
        .eq("id", asset_id_str)
        .limit(1)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise HTTPException(status_code=502, detail="Database error fetching media asset")
    if not response.data:
        raise HTTPException(status_code=404, detail="Media asset not found")

    row = response.data[0]
    hosted_bucket = row.get("hosted_bucket") or None
    hosted_key = row.get("hosted_key") or None

    deleted_links = 0
    try:
        links_response = (
            db.schema("core")
            .table("media_links")
            .delete()
            .eq("media_asset_id", asset_id_str)
            .execute()
        )
        deleted_links = len(links_response.data or [])
    except Exception:
        # If link deletion fails, do not proceed to delete the asset row.
        raise HTTPException(status_code=502, detail="Database error deleting media links")

    deleted_asset = False
    try:
        asset_delete_response = (
            db.schema("core")
            .table("media_assets")
            .delete()
            .eq("id", asset_id_str)
            .execute()
        )
        deleted_asset = bool(asset_delete_response.data)
    except Exception:
        raise HTTPException(status_code=502, detail="Database error deleting media asset")

    s3_deleted = False
    s3_error: str | None = None
    if hosted_key:
        try:
            s3_client = get_s3_client()
            bucket = hosted_bucket or get_s3_bucket()
            s3_client.delete_object(Bucket=bucket, Key=hosted_key)
            s3_deleted = True
        except Exception as exc:
            s3_error = str(exc)

    return DeleteMediaAssetResponse(
        asset_id=asset_id_str,
        deleted_links=deleted_links,
        deleted_asset=deleted_asset,
        s3_deleted=s3_deleted,
        s3_error=s3_error,
    )
