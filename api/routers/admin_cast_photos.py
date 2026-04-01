"""Admin endpoints for cast photo operations."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from api.auth import InternalAdminUser
from api.deps import SupabaseAdminClient
from trr_backend.media.image_variants import generate_cast_photo_variants
from trr_backend.media.s3_mirror import mirror_cast_photo_row
from trr_backend.repositories.cast_photos import update_cast_photo_hosted_fields

router = APIRouter(prefix="/admin", tags=["admin-cast-photos"])


class MirrorCastPhotoResponse(BaseModel):
    photo_id: str
    hosted_url: str | None = None
    hosted_key: str | None = None
    status: str


class DetectTextOverlayResponse(BaseModel):
    photo_id: str
    status: str
    has_text_overlay: bool | None = None
    text_overlay_confidence: float | None = None
    text_overlay_detector: str | None = None
    text_overlay_model: str | None = None
    text_overlay_detected_at: str | None = None
    text_overlay_prompt_version: str | None = None
    text_overlay_error_code: str | None = None


class GenerateCastPhotoVariantsRequest(BaseModel):
    force: bool = False
    crop: dict | None = None


class CastPhotoVariantItem(BaseModel):
    variant_key: str
    format: str
    hosted_url: str
    width: int
    height: int
    bytes: int
    crop_signature: str


class GenerateCastPhotoVariantsResponse(BaseModel):
    photo_id: str
    generated: int
    crop_signature: str
    variants: list[CastPhotoVariantItem]


@router.post("/cast-photos/{photo_id}/mirror", response_model=MirrorCastPhotoResponse)
def mirror_cast_photo(
    photo_id: UUID,
    db: SupabaseAdminClient,
    _: InternalAdminUser,
    force: bool = Query(default=False),
) -> MirrorCastPhotoResponse:
    response = (
        db.schema("core")
        .table("cast_photos")
        .select(
            "id,person_id,imdb_person_id,source,source_page_url,image_url,url,thumb_url,"
            "hosted_url,hosted_key,hosted_sha256"
        )
        .eq("id", str(photo_id))
        .limit(1)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise HTTPException(status_code=502, detail="Database error fetching cast photo")
    if not response.data:
        raise HTTPException(status_code=404, detail="Cast photo not found")

    row = response.data[0]
    patch = mirror_cast_photo_row(row, force=force)
    if not patch:
        return MirrorCastPhotoResponse(
            photo_id=str(photo_id),
            hosted_url=row.get("hosted_url"),
            hosted_key=row.get("hosted_key"),
            status="skipped",
        )

    patch["hosted_at"] = datetime.now(UTC).isoformat()
    updated = update_cast_photo_hosted_fields(db, str(photo_id), patch)

    return MirrorCastPhotoResponse(
        photo_id=str(photo_id),
        hosted_url=updated.get("hosted_url"),
        hosted_key=updated.get("hosted_key"),
        status="hosted",
    )


@router.post("/cast-photos/{photo_id}/detect-text-overlay", response_model=DetectTextOverlayResponse)
def detect_text_overlay_cast_photo(
    photo_id: UUID,
    db: SupabaseAdminClient,
    _: InternalAdminUser,
    force: bool = Query(default=False),
) -> DetectTextOverlayResponse:
    """
    Detect whether a cast photo contains overlaid text and persist results to core.cast_photos.metadata.

    Query param:
    - force: boolean (default false). If false and metadata already has has_text_overlay, returns existing values.
    """
    photo_id_str = str(photo_id)

    try:
        from trr_backend.vision.text_overlay import (
            TextOverlayDatabaseError,
            TextOverlayDetectionNotConfiguredError,
            TextOverlayTargetFetchError,
            TextOverlayTargetInvalidError,
            TextOverlayTargetNotFoundError,
            detect_and_update_cast_photo_text_overlay,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="Text overlay detection module not available",
            headers={"x-error-code": "VISION_MODULE_UNAVAILABLE"},
        ) from exc

    try:
        result = detect_and_update_cast_photo_text_overlay(db, photo_id_str, force=force)
    except TextOverlayDetectionNotConfiguredError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except TextOverlayTargetNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except TextOverlayTargetInvalidError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except (TextOverlayTargetFetchError, TextOverlayDatabaseError) as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=502,
            detail="Text overlay detection failed",
            headers={"x-error-code": "VISION_DETECTION_FAILED"},
        ) from exc

    status = "unknown" if result.status == "unknown" else ("detected" if force else "ok")
    return DetectTextOverlayResponse(
        photo_id=photo_id_str,
        status=status,
        has_text_overlay=result.has_text_overlay,
        text_overlay_confidence=result.confidence,
        text_overlay_detector=result.detector,
        text_overlay_model=result.model,
        text_overlay_detected_at=result.detected_at,
        text_overlay_prompt_version=result.prompt_version,
        text_overlay_error_code=result.reason_code,
    )


@router.post("/cast-photos/{photo_id}/variants", response_model=GenerateCastPhotoVariantsResponse)
def generate_variants_for_cast_photo(
    photo_id: UUID,
    db: SupabaseAdminClient,
    _: InternalAdminUser,
    payload: GenerateCastPhotoVariantsRequest | None = None,
) -> GenerateCastPhotoVariantsResponse:
    photo_id_str = str(photo_id)
    payload = payload or GenerateCastPhotoVariantsRequest()

    try:
        variants = generate_cast_photo_variants(
            db,
            photo_id=photo_id_str,
            crop=payload.crop,
            force=payload.force,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail="Failed to generate variants",
            headers={"x-error-code": "MEDIA_VARIANTS_FAILED"},
        ) from exc

    crop_signature = variants[0].crop_signature if variants else ("base" if not payload.crop else "custom")
    return GenerateCastPhotoVariantsResponse(
        photo_id=photo_id_str,
        generated=len(variants),
        crop_signature=crop_signature,
        variants=[
            CastPhotoVariantItem(
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
