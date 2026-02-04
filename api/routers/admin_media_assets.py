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
