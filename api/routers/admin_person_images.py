"""
Admin endpoints for person image management.

Provides endpoints to:
1. Refresh images from sources (IMDb, TMDb, Fandom)
2. Mirror images to S3
3. Stream progress via SSE
"""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncGenerator
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from api.auth import AdminUser
from api.deps import SupabaseAdminClient

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/person", tags=["admin-person"])

# Valid sources for person images
SourceType = Literal["imdb", "tmdb", "fandom", "fandom-gallery"]
ALL_SOURCES: list[SourceType] = ["imdb", "tmdb", "fandom", "fandom-gallery"]


class RefreshImagesRequest(BaseModel):
    """Request to refresh person images from sources."""

    sources: list[SourceType] | None = Field(
        default=None,
        description="Sources to fetch from. Default: all",
    )
    limit_per_source: int = Field(
        default=50,
        ge=1,
        le=200,
        description="Max images per source",
    )
    skip_mirror: bool = Field(
        default=False,
        description="Skip S3 mirroring",
    )
    skip_prune: bool = Field(
        default=False,
        description="Skip pruning orphaned S3 objects",
    )
    force_mirror: bool = Field(
        default=False,
        description="Force re-mirror even if already hosted",
    )


class RefreshImagesResponse(BaseModel):
    """Response after refreshing person images."""

    person_id: str
    person_name: str | None
    imdb_person_id: str | None
    tmdb_person_id: int | None
    sources_used: list[str]
    photos_fetched: int
    photos_upserted: int
    photos_mirrored: int
    photos_failed: int
    photos_pruned: int
    errors: list[str]


# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------


def _get_person_details(db: SupabaseAdminClient, person_id: str) -> dict | None:
    """Fetch person details including external IDs."""
    response = (
        db.schema("core").table("people").select("id,full_name,external_ids").eq("id", person_id).limit(1).execute()
    )
    if hasattr(response, "error") and response.error:
        raise HTTPException(status_code=502, detail="Database error")
    return response.data[0] if response.data else None


def _extract_imdb_id(external_ids: dict | None) -> str | None:
    """Extract IMDb person ID from external_ids."""
    if not external_ids:
        return None
    return external_ids.get("imdb")


def _get_tmdb_id(db: SupabaseAdminClient, person_id: str, external_ids: dict | None) -> int | None:
    """Get TMDb person ID from cast_tmdb table or external_ids."""
    from trr_backend.repositories.cast_tmdb import get_cast_tmdb_by_person_id

    cast_tmdb = get_cast_tmdb_by_person_id(db, person_id)
    if cast_tmdb and cast_tmdb.get("tmdb_id"):
        return int(cast_tmdb["tmdb_id"])
    if external_ids:
        tmdb_id = external_ids.get("tmdb_id") or external_ids.get("tmdb")
        if tmdb_id:
            return int(tmdb_id)
    return None


def _mirror_person_photos(
    db: SupabaseAdminClient,
    person_id: str,
    imdb_person_id: str | None,
    *,
    force: bool = False,
) -> tuple[int, int]:
    """Mirror unmirrored photos to S3. Returns (mirrored, failed)."""
    from trr_backend.media.s3_mirror import get_cdn_base_url, mirror_cast_photo_row
    from trr_backend.repositories.cast_photos import (
        fetch_cast_photos_missing_hosted,
        update_cast_photo_hosted_fields,
    )

    cdn_base_url = None if force else get_cdn_base_url()
    # When force=True, include photos that already have hosted_url so they get re-uploaded
    rows = fetch_cast_photos_missing_hosted(
        db, person_ids=[person_id], cdn_base_url=cdn_base_url, include_hosted=force
    )
    if not rows:
        return 0, 0

    mirrored, failed = 0, 0
    for row in rows:
        if not row.get("imdb_person_id") and imdb_person_id:
            row["imdb_person_id"] = imdb_person_id
        try:
            patch = mirror_cast_photo_row(row, force=force)
            if patch:
                update_cast_photo_hosted_fields(db, str(row["id"]), patch)
                mirrored += 1
        except Exception as exc:
            logger.warning(f"Mirror failed for {row.get('id')}: {exc}")
            failed += 1
    return mirrored, failed


def _prune_person_s3_objects(db: SupabaseAdminClient, person_identifier: str) -> int:
    """Prune orphaned S3 objects. Returns count pruned."""
    from trr_backend.media.s3_mirror import prune_orphaned_cast_photo_objects

    try:
        orphaned = prune_orphaned_cast_photo_objects(db, person_identifier)
        return len(orphaned)
    except Exception as exc:
        logger.warning(f"Prune failed: {exc}")
        return 0


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("/{person_id}/refresh-images", response_model=RefreshImagesResponse)
def refresh_person_images(
    person_id: UUID,
    request: RefreshImagesRequest | None = None,
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
) -> RefreshImagesResponse:
    """
    Refresh images for a person from external sources.

    Fetches from IMDb, TMDb, Fandom; upserts to DB; mirrors to S3.
    """
    from trr_backend.ingestion.cast_photo_sources import fetch_all_cast_photos
    from trr_backend.repositories.cast_photos import upsert_cast_photos

    request = request or RefreshImagesRequest()
    person_id_str = str(person_id)

    # 1. Get person details
    person = _get_person_details(db, person_id_str)
    if not person:
        raise HTTPException(status_code=404, detail=f"Person {person_id} not found")

    external_ids = person.get("external_ids") or {}
    imdb_person_id = _extract_imdb_id(external_ids)
    tmdb_person_id = _get_tmdb_id(db, person_id_str, external_ids)
    person_name = person.get("full_name")
    sources = request.sources or ALL_SOURCES
    errors: list[str] = []

    # 2. Fetch photos
    try:
        photos = fetch_all_cast_photos(
            person_id_str,
            imdb_person_id=imdb_person_id,
            tmdb_person_id=tmdb_person_id,
            person_name=person_name,
            sources=list(sources),
            limit_per_source=request.limit_per_source,
        )
    except Exception as exc:
        logger.exception(f"Fetch error for {person_id}")
        errors.append(f"Fetch: {exc}")
        photos = []

    # 3. Upsert to database
    photos_upserted = 0
    if photos:
        imdb_photos = [p for p in photos if p.get("source") == "imdb"]
        other_photos = [p for p in photos if p.get("source") != "imdb"]
        try:
            if imdb_photos:
                photos_upserted += len(upsert_cast_photos(db, imdb_photos, dedupe_on="source_image_id"))
            if other_photos:
                photos_upserted += len(upsert_cast_photos(db, other_photos, dedupe_on="image_url_canonical"))
        except Exception as exc:
            logger.exception(f"Upsert error for {person_id}")
            errors.append(f"Upsert: {exc}")

    # 4. Mirror to S3
    photos_mirrored, photos_failed = 0, 0
    if not request.skip_mirror:
        try:
            photos_mirrored, photos_failed = _mirror_person_photos(
                db, person_id_str, imdb_person_id, force=request.force_mirror
            )
        except Exception as exc:
            logger.exception(f"Mirror error for {person_id}")
            errors.append(f"Mirror: {exc}")

    # 5. Prune orphaned S3 objects
    photos_pruned = 0
    if not request.skip_mirror and not request.skip_prune:
        person_identifier = imdb_person_id or person_id_str
        photos_pruned = _prune_person_s3_objects(db, person_identifier)

    return RefreshImagesResponse(
        person_id=person_id_str,
        person_name=person_name,
        imdb_person_id=imdb_person_id,
        tmdb_person_id=tmdb_person_id,
        sources_used=list(sources),
        photos_fetched=len(photos),
        photos_upserted=photos_upserted,
        photos_mirrored=photos_mirrored,
        photos_failed=photos_failed,
        photos_pruned=photos_pruned,
        errors=errors,
    )


@router.post("/{person_id}/refresh-images/stream")
async def refresh_person_images_stream(
    person_id: UUID,
    request: RefreshImagesRequest | None = None,
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
) -> StreamingResponse:
    """Refresh images with SSE streaming progress."""
    from trr_backend.ingestion.cast_photo_sources import fetch_all_cast_photos
    from trr_backend.media.s3_mirror import mirror_cast_photo_row
    from trr_backend.repositories.cast_photos import (
        fetch_cast_photos_missing_hosted,
        update_cast_photo_hosted_fields,
        upsert_cast_photos,
    )

    request = request or RefreshImagesRequest()
    person_id_str = str(person_id)

    async def event_generator() -> AsyncGenerator[str, None]:
        errors: list[str] = []

        # 1. Get person
        person = _get_person_details(db, person_id_str)
        if not person:
            yield f"event: error\ndata: {json.dumps({'error': 'Person not found'})}\n\n"
            return

        external_ids = person.get("external_ids") or {}
        imdb_person_id = _extract_imdb_id(external_ids)
        tmdb_person_id = _get_tmdb_id(db, person_id_str, external_ids)
        person_name = person.get("full_name")
        sources = request.sources or ALL_SOURCES

        # 2. Fetch
        yield f"event: progress\ndata: {json.dumps({'stage': 'fetching', 'message': 'Fetching from sources...'})}\n\n"
        try:
            photos = fetch_all_cast_photos(
                person_id_str,
                imdb_person_id=imdb_person_id,
                tmdb_person_id=tmdb_person_id,
                person_name=person_name,
                sources=list(sources),
                limit_per_source=request.limit_per_source,
            )
        except Exception as exc:
            errors.append(str(exc))
            photos = []
        data = {"stage": "fetching", "current": len(photos), "total": len(photos)}
        yield f"event: progress\ndata: {json.dumps(data)}\n\n"

        # 3. Upsert
        photos_upserted = 0
        if photos:
            yield f"event: progress\ndata: {json.dumps({'stage': 'upserting'})}\n\n"
            imdb_photos = [p for p in photos if p.get("source") == "imdb"]
            other_photos = [p for p in photos if p.get("source") != "imdb"]
            try:
                if imdb_photos:
                    photos_upserted += len(upsert_cast_photos(db, imdb_photos, dedupe_on="source_image_id"))
                if other_photos:
                    photos_upserted += len(upsert_cast_photos(db, other_photos, dedupe_on="image_url_canonical"))
            except Exception as exc:
                errors.append(str(exc))
            yield f"event: progress\ndata: {json.dumps({'stage': 'upserting', 'current': photos_upserted})}\n\n"

        # 4. Mirror
        photos_mirrored, photos_failed = 0, 0
        if not request.skip_mirror:
            yield f"event: progress\ndata: {json.dumps({'stage': 'mirroring'})}\n\n"
            from trr_backend.media.s3_mirror import get_cdn_base_url

            cdn_url = None if request.force_mirror else get_cdn_base_url()
            # When force_mirror=True, include photos that already have hosted_url so they get re-uploaded
            rows = fetch_cast_photos_missing_hosted(
                db, person_ids=[person_id_str], cdn_base_url=cdn_url, include_hosted=request.force_mirror
            )
            for idx, row in enumerate(rows):
                if not row.get("imdb_person_id") and imdb_person_id:
                    row["imdb_person_id"] = imdb_person_id
                try:
                    patch = mirror_cast_photo_row(row, force=request.force_mirror)
                    if patch:
                        update_cast_photo_hosted_fields(db, str(row["id"]), patch)
                        photos_mirrored += 1
                except Exception:
                    photos_failed += 1
                if (idx + 1) % 5 == 0:
                    data = {"stage": "mirroring", "current": idx + 1, "total": len(rows)}
                    yield f"event: progress\ndata: {json.dumps(data)}\n\n"

        # 5. Prune
        photos_pruned = 0
        if not request.skip_mirror and not request.skip_prune:
            yield f"event: progress\ndata: {json.dumps({'stage': 'pruning'})}\n\n"
            photos_pruned = _prune_person_s3_objects(db, imdb_person_id or person_id_str)

        # 6. Complete
        complete_data = {
            "person_id": person_id_str,
            "photos_fetched": len(photos),
            "photos_upserted": photos_upserted,
            "photos_mirrored": photos_mirrored,
            "photos_failed": photos_failed,
            "photos_pruned": photos_pruned,
            "errors": errors,
        }
        yield f"event: complete\ndata: {json.dumps(complete_data)}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
