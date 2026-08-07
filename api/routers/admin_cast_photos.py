"""Admin endpoints for cast photo operations."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any, Literal, NoReturn
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from api.auth import InternalAdminUser
from api.deps import SupabaseAdminClient
from trr_backend.media.image_variants import generate_cast_photo_variants
from trr_backend.media.s3_mirror import mirror_cast_photo_row
from trr_backend.repositories import cast_photo_tags as cast_photo_tags_repo
from trr_backend.repositories.cast_photos import update_cast_photo_hosted_fields

router = APIRouter(prefix="/admin", tags=["admin-cast-photos"])
logger = logging.getLogger(__name__)


class CastPhotoTagsItem(BaseModel):
    cast_photo_id: str
    people_names: list[str] | None = None
    people_ids: list[str] | None = None
    people_count: int | None = None
    people_count_source: Literal["auto", "manual"] | None = None
    detector: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    created_by_firebase_uid: str | None = None
    updated_by_firebase_uid: str | None = None


class CastPhotoTagsListResponse(BaseModel):
    tags: list[CastPhotoTagsItem]


class CastPhotoIdsForPersonResponse(BaseModel):
    photo_ids: list[str]


class UpsertCastPhotoTagsRequest(BaseModel):
    cast_photo_id: UUID
    people_names: list[str] | None = None
    people_ids: list[str] | None = None
    people_count: int | None = None
    people_count_source: Literal["auto", "manual"] | None = None
    detector: str | None = None
    created_by_firebase_uid: str | None = None
    updated_by_firebase_uid: str | None = None


class UpsertCastPhotoTagsResponse(BaseModel):
    tag: CastPhotoTagsItem | None = None


class UpdateCastPhotoFaceBoxesRequest(BaseModel):
    face_boxes: list[Any] | None = None


class UpdateCastPhotoFaceBoxesResponse(BaseModel):
    updated: bool


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


def _raise_cast_photo_tags_database_error(operation: str, error: Exception) -> NoReturn:
    logger.warning("Cast photo tags database operation failed: %s", operation, exc_info=True)
    raise HTTPException(status_code=502, detail=f"Database error during {operation}") from error


@router.get("/cast-photos/tags", response_model=CastPhotoTagsListResponse)
def list_cast_photo_tags(
    _: InternalAdminUser,
    photo_ids: list[UUID] | None = Query(default=None),
) -> CastPhotoTagsListResponse:
    try:
        rows = cast_photo_tags_repo.list_tag_rows_by_photo_ids([str(photo_id) for photo_id in photo_ids or []])
    except Exception as exc:
        _raise_cast_photo_tags_database_error("fetching cast photo tags", exc)

    return CastPhotoTagsListResponse(tags=[CastPhotoTagsItem(**row) for row in rows])


@router.get("/cast-photos/tags/photo-ids", response_model=CastPhotoIdsForPersonResponse)
def list_cast_photo_tag_photo_ids(
    _: InternalAdminUser,
    person_id: str = Query(..., min_length=1),
) -> CastPhotoIdsForPersonResponse:
    try:
        photo_ids = cast_photo_tags_repo.list_photo_ids_by_person_id(person_id)
    except Exception as exc:
        _raise_cast_photo_tags_database_error("fetching cast photo IDs for person", exc)

    return CastPhotoIdsForPersonResponse(photo_ids=photo_ids)


@router.post("/cast-photos/tags", response_model=UpsertCastPhotoTagsResponse)
def upsert_cast_photo_tags(
    _: InternalAdminUser,
    payload: UpsertCastPhotoTagsRequest,
) -> UpsertCastPhotoTagsResponse:
    try:
        row = cast_photo_tags_repo.upsert_cast_photo_tag_row(
            cast_photo_id=str(payload.cast_photo_id),
            people_names=payload.people_names,
            people_ids=payload.people_ids,
            people_count=payload.people_count,
            people_count_source=payload.people_count_source,
            detector=payload.detector,
            created_by_firebase_uid=payload.created_by_firebase_uid,
            updated_by_firebase_uid=payload.updated_by_firebase_uid,
        )
    except Exception as exc:
        _raise_cast_photo_tags_database_error("upserting cast photo tags", exc)

    return UpsertCastPhotoTagsResponse(tag=CastPhotoTagsItem(**row) if row else None)


@router.post("/cast-photos/{photo_id}/face-boxes", response_model=UpdateCastPhotoFaceBoxesResponse)
def update_cast_photo_face_boxes(
    photo_id: UUID,
    _: InternalAdminUser,
    payload: UpdateCastPhotoFaceBoxesRequest | None = None,
) -> UpdateCastPhotoFaceBoxesResponse:
    try:
        updated = cast_photo_tags_repo.set_cast_photo_face_boxes(
            str(photo_id),
            (payload or UpdateCastPhotoFaceBoxesRequest()).face_boxes,
        )
    except Exception as exc:
        _raise_cast_photo_tags_database_error("updating cast photo face boxes", exc)

    return UpdateCastPhotoFaceBoxesResponse(updated=updated)


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
