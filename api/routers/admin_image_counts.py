"""Admin endpoints for auto-counting people in images."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from api.auth import AdminUser
from api.deps import SupabaseAdminClient
from trr_backend.clients.screenalytics import ScreenalyticsClientError, count_people
from trr_backend.repositories.cast_photo_tags import (
    get_tags_by_photo_ids,
    has_manual_tags,
    upsert_cast_photo_tags,
)
from trr_backend.repositories.media_links import (
    has_manual_people_tags,
    list_person_links_by_asset_id,
    update_person_links_context,
)

router = APIRouter(prefix="/admin", tags=["admin-images"])


class AutoCountResponse(BaseModel):
    people_count: int
    face_count: int
    detector: str
    model: str | None = None
    people_count_source: str = "auto"


@router.post("/cast-photos/{photo_id}/auto-count", response_model=AutoCountResponse)
def auto_count_cast_photo(
    photo_id: UUID,
    force: bool = Query(default=False),
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
) -> AutoCountResponse:
    response = (
        db.schema("core")
        .table("cast_photos")
        .select("id, hosted_url, url")
        .eq("id", str(photo_id))
        .limit(1)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise HTTPException(status_code=502, detail="Database error fetching cast photo")
    if not response.data:
        raise HTTPException(status_code=404, detail="Cast photo not found")

    row = response.data[0]
    image_url = row.get("hosted_url") or row.get("url")
    if not image_url:
        raise HTTPException(status_code=400, detail="Cast photo has no image URL")

    tag_rows = get_tags_by_photo_ids(db, [str(photo_id)])
    tag_row = tag_rows.get(str(photo_id))
    if has_manual_tags(tag_row) and not force:
        raise HTTPException(status_code=409, detail="Manual tags/count exist; use force to overwrite")

    try:
        result = count_people(image_url)
    except ScreenalyticsClientError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    upsert_cast_photo_tags(
        db,
        cast_photo_id=str(photo_id),
        people_names=tag_row.get("people_names") if tag_row else None,
        people_ids=tag_row.get("people_ids") if tag_row else None,
        people_count=result.people_count,
        people_count_source="auto",
        detector=result.detector,
        updated_by_firebase_uid="system:auto",
    )

    return AutoCountResponse(
        people_count=result.people_count,
        face_count=result.face_count,
        detector=result.detector,
        model=result.model,
    )


@router.post("/media-assets/{asset_id}/auto-count", response_model=AutoCountResponse)
def auto_count_media_asset(
    asset_id: UUID,
    force: bool = Query(default=False),
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
) -> AutoCountResponse:
    response = (
        db.schema("core")
        .table("media_assets")
        .select("id, hosted_url, source_url")
        .eq("id", str(asset_id))
        .limit(1)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise HTTPException(status_code=502, detail="Database error fetching media asset")
    if not response.data:
        raise HTTPException(status_code=404, detail="Media asset not found")

    row = response.data[0]
    image_url = row.get("hosted_url") or row.get("source_url")
    if not image_url:
        raise HTTPException(status_code=400, detail="Media asset has no image URL")

    links = list_person_links_by_asset_id(db, str(asset_id))
    if not links:
        raise HTTPException(status_code=404, detail="No person links found for asset")
    if any(has_manual_people_tags(link.get("context")) for link in links) and not force:
        raise HTTPException(status_code=409, detail="Manual tags/count exist; use force to overwrite")

    try:
        result = count_people(image_url)
    except ScreenalyticsClientError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    update_person_links_context(
        db,
        links,
        {
            "people_count": result.people_count,
            "people_count_source": "auto",
            "people_count_detector": result.detector,
        },
    )

    return AutoCountResponse(
        people_count=result.people_count,
        face_count=result.face_count,
        detector=result.detector,
        model=result.model,
    )
