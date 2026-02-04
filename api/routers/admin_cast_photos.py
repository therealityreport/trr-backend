"""Admin endpoints for cast photo operations."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from api.auth import AdminUser
from api.deps import SupabaseAdminClient
from trr_backend.media.s3_mirror import mirror_cast_photo_row
from trr_backend.repositories.cast_photos import update_cast_photo_hosted_fields

router = APIRouter(prefix="/admin", tags=["admin-cast-photos"])


class MirrorCastPhotoResponse(BaseModel):
    photo_id: str
    hosted_url: str | None = None
    hosted_key: str | None = None
    status: str


@router.post("/cast-photos/{photo_id}/mirror", response_model=MirrorCastPhotoResponse)
def mirror_cast_photo(
    photo_id: UUID,
    force: bool = Query(default=False),
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
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
