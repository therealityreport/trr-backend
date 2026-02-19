"""Admin endpoints for managing media_assets."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from api.auth import AdminUser
from api.deps import SupabaseAdminClient
from trr_backend.media.image_variants import generate_media_asset_variants
from trr_backend.media.s3_mirror import (
    get_s3_bucket,
    get_s3_client,
    mirror_media_asset_row,
)
from trr_backend.repositories.media_assets import (
    update_asset_with_mirror_result,
    update_ingest_status,
)

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


class GenerateMediaAssetVariantsRequest(BaseModel):
    force: bool = False
    crop: dict | None = None


class MediaAssetVariantItem(BaseModel):
    variant_key: str
    format: str
    hosted_url: str
    width: int
    height: int
    bytes: int
    crop_signature: str


class GenerateMediaAssetVariantsResponse(BaseModel):
    asset_id: str
    generated: int
    crop_signature: str
    variants: list[MediaAssetVariantItem]


class DetectTextOverlayResponse(BaseModel):
    asset_id: str
    status: str
    has_text_overlay: bool | None = None
    text_overlay_confidence: float | None = None
    text_overlay_detector: str | None = None
    text_overlay_model: str | None = None
    text_overlay_detected_at: str | None = None
    text_overlay_prompt_version: str | None = None
    text_overlay_error_code: str | None = None


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
        .select(
            "id, source, source_url, hosted_url, hosted_key, hosted_sha256, hosted_bucket, "
            "hosted_content_type, hosted_bytes, hosted_etag, metadata"
        )
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
    if not isinstance(source_url, str) or not source_url.strip():
        raise HTTPException(status_code=409, detail="Media asset has no source_url to mirror")

    update_ingest_status(
        db,
        asset_id_str,
        "in_progress",
    )

    try:
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        if payload.referer:
            metadata = {**metadata, "page_url": payload.referer}
            row = {**row, "metadata": metadata}
        patch = mirror_media_asset_row(row, force=payload.force)
    except Exception as exc:
        update_ingest_status(
            db,
            asset_id_str,
            "failed",
            error=str(exc),
            failed_at=datetime.now(UTC).isoformat(),
        )
        raise HTTPException(status_code=502, detail=f"Failed to mirror source_url: {exc}") from exc

    if not patch:
        update_ingest_status(
            db,
            asset_id_str,
            "hosted",
            completed_at=datetime.now(UTC).isoformat(),
        )
        return MirrorMediaAssetResponse(
            asset_id=asset_id_str,
            hosted_url=row.get("hosted_url"),
            hosted_key=row.get("hosted_key"),
            status="skipped",
        )

    try:
        if set(patch.keys()) == {"hosted_url"}:
            db.schema("core").table("media_assets").update({"hosted_url": patch["hosted_url"]}).eq(
                "id", asset_id_str
            ).execute()
            update_ingest_status(
                db,
                asset_id_str,
                "hosted",
                completed_at=datetime.now(UTC).isoformat(),
            )
            hosted_url = patch["hosted_url"]
            hosted_key = row.get("hosted_key")
            file_size = int(row.get("hosted_bytes") or 0) or None
            content_type = row.get("hosted_content_type")
        else:
            now_iso = str(patch.get("hosted_at") or datetime.now(UTC).isoformat())
            update_asset_with_mirror_result(
                db,
                asset_id=asset_id_str,
                sha256=str(patch.get("sha256") or patch.get("hosted_sha256") or ""),
                hosted_bucket=str(patch.get("hosted_bucket") or ""),
                hosted_key=str(patch.get("hosted_key") or ""),
                hosted_url=str(patch.get("hosted_url") or ""),
                hosted_bytes=int(patch.get("hosted_bytes") or 0),
                hosted_content_type=(
                    str(patch.get("hosted_content_type"))
                    if patch.get("hosted_content_type") is not None
                    else None
                ),
                hosted_etag=(
                    str(patch.get("hosted_etag")) if patch.get("hosted_etag") is not None else None
                ),
                width=int(patch.get("width")) if patch.get("width") is not None else None,
                height=int(patch.get("height")) if patch.get("height") is not None else None,
                completed_at=now_iso,
                metadata=patch.get("metadata") if isinstance(patch.get("metadata"), dict) else None,
            )
            hosted_url = patch.get("hosted_url")
            hosted_key = patch.get("hosted_key")
            file_size = patch.get("hosted_bytes")
            content_type = patch.get("hosted_content_type")
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
        hosted_url=str(hosted_url) if hosted_url else None,
        hosted_key=str(hosted_key) if hosted_key else None,
        status="hosted",
        bytes=int(file_size) if file_size is not None else None,
        content_type=str(content_type) if content_type else None,
    )


@router.post("/media-assets/{asset_id}/variants", response_model=GenerateMediaAssetVariantsResponse)
def generate_variants_for_media_asset(
    asset_id: UUID,
    payload: GenerateMediaAssetVariantsRequest | None = None,
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
) -> GenerateMediaAssetVariantsResponse:
    asset_id_str = str(asset_id)
    payload = payload or GenerateMediaAssetVariantsRequest()
    try:
        variants = generate_media_asset_variants(
            db,
            asset_id=asset_id_str,
            crop=payload.crop,
            force=payload.force,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to generate variants: {exc}") from exc

    crop_signature = variants[0].crop_signature if variants else ("base" if not payload.crop else "custom")
    return GenerateMediaAssetVariantsResponse(
        asset_id=asset_id_str,
        generated=len(variants),
        crop_signature=crop_signature,
        variants=[
            MediaAssetVariantItem(
                variant_key=item.variant_key,
                format=item.format,
                hosted_url=item.hosted_url,
                width=item.width,
                height=item.height,
                bytes=item.bytes,
                crop_signature=item.crop_signature,
            )
            for item in variants
        ],
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
        links_response = db.schema("core").table("media_links").delete().eq("media_asset_id", asset_id_str).execute()
        deleted_links = len(links_response.data or [])
    except Exception as exc:
        # If link deletion fails, do not proceed to delete the asset row.
        raise HTTPException(status_code=502, detail="Database error deleting media links") from exc

    deleted_asset = False
    try:
        asset_delete_response = db.schema("core").table("media_assets").delete().eq("id", asset_id_str).execute()
        deleted_asset = bool(asset_delete_response.data)
    except Exception as exc:
        raise HTTPException(status_code=502, detail="Database error deleting media asset") from exc

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


@router.post("/media-assets/{asset_id}/detect-text-overlay", response_model=DetectTextOverlayResponse)
def detect_text_overlay_media_asset(
    asset_id: UUID,
    force: bool = Query(default=False),
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
) -> DetectTextOverlayResponse:
    """
    Detect whether a media asset contains overlaid text and persist results to core.media_assets.metadata.

    Query param:
    - force: boolean (default false). If false and metadata already has has_text_overlay, returns existing values.
    """
    asset_id_str = str(asset_id)

    try:
        from trr_backend.vision.text_overlay import (
            TextOverlayDatabaseError,
            TextOverlayDetectionNotConfiguredError,
            TextOverlayTargetFetchError,
            TextOverlayTargetInvalidError,
            TextOverlayTargetNotFoundError,
            detect_and_update_media_asset_text_overlay,
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Text overlay detection module not available: {exc}") from exc

    try:
        result = detect_and_update_media_asset_text_overlay(db, asset_id_str, force=force)
    except TextOverlayDetectionNotConfiguredError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except TextOverlayTargetNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except TextOverlayTargetInvalidError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except (TextOverlayTargetFetchError, TextOverlayDatabaseError) as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"Text overlay detection failed: {exc}") from exc

    status = "unknown" if result.status == "unknown" else ("detected" if force else "ok")
    return DetectTextOverlayResponse(
        asset_id=asset_id_str,
        status=status,
        has_text_overlay=result.has_text_overlay,
        text_overlay_confidence=result.confidence,
        text_overlay_detector=result.detector,
        text_overlay_model=result.model,
        text_overlay_detected_at=result.detected_at,
        text_overlay_prompt_version=result.prompt_version,
        text_overlay_error_code=result.reason_code,
    )
