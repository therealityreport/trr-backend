"""Admin endpoints for auto-counting people in images."""

from __future__ import annotations

from typing import Any
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
    has_people_count,
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


class AutoCountShowImagesRequest(BaseModel):
    season_number: int | None = None
    force: bool = False


class AutoCountShowImagesResponse(BaseModel):
    assets_total: int
    assets_counted: int
    assets_skipped: int
    assets_failed: int


@router.post("/cast-photos/{photo_id}/auto-count", response_model=AutoCountResponse)
def auto_count_cast_photo(
    photo_id: UUID,
    force: bool = Query(default=False),
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
) -> AutoCountResponse:
    response = (
        db.schema("core").table("cast_photos").select("id, hosted_url, url").eq("id", str(photo_id)).limit(1).execute()
    )
    if hasattr(response, "error") and response.error:
        raise HTTPException(status_code=502, detail="Database error fetching cast photo")
    if not response.data:
        raise HTTPException(status_code=404, detail="Cast photo not found")

    row = response.data[0]
    image_url = row.get("hosted_url") or row.get("url")
    if not image_url:
        raise HTTPException(
            status_code=409,
            detail="Cast photo has no hosted_url or url to analyze",
        )

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
        raise HTTPException(
            status_code=409,
            detail="Media asset has no hosted_url or source_url to analyze",
        )

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


@router.post("/shows/{show_id}/auto-count-images", response_model=AutoCountShowImagesResponse)
def auto_count_show_images(
    show_id: UUID,
    payload: AutoCountShowImagesRequest,
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
) -> AutoCountShowImagesResponse:
    show_id_str = str(show_id)

    # Fetch seasons for the show (optionally filter by season_number)
    season_query = db.schema("core").table("seasons").select("id, season_number").eq("show_id", show_id_str)
    if payload.season_number is not None:
        season_query = season_query.eq("season_number", payload.season_number)
    season_response = season_query.execute()
    if hasattr(season_response, "error") and season_response.error:
        raise HTTPException(status_code=502, detail="Database error fetching seasons")

    seasons = season_response.data or []
    season_ids = [row["id"] for row in seasons]

    if payload.season_number is not None and not season_ids:
        return AutoCountShowImagesResponse(
            assets_total=0,
            assets_counted=0,
            assets_skipped=0,
            assets_failed=0,
        )

    # Fetch episodes for the show (optionally filter by season)
    episode_query = db.schema("core").table("episodes").select("id, season_id").eq("show_id", show_id_str)
    if season_ids:
        episode_query = episode_query.in_("season_id", season_ids)
    episode_response = episode_query.execute()
    if hasattr(episode_response, "error") and episode_response.error:
        raise HTTPException(status_code=502, detail="Database error fetching episodes")

    episode_ids = [row["id"] for row in (episode_response.data or [])]

    # Fetch media_links for show/season/episode entities
    links: list[dict[str, Any]] = []
    try:
        show_links = (
            db.schema("core")
            .table("media_links")
            .select("id, media_asset_id, context")
            .eq("entity_type", "show")
            .eq("entity_id", show_id_str)
            .execute()
        )
        if not (hasattr(show_links, "error") and show_links.error):
            links.extend(show_links.data or [])
    except Exception:
        pass

    if season_ids:
        season_links = (
            db.schema("core")
            .table("media_links")
            .select("id, media_asset_id, context")
            .eq("entity_type", "season")
            .in_("entity_id", season_ids)
            .execute()
        )
        if hasattr(season_links, "error") and season_links.error:
            raise HTTPException(status_code=502, detail="Database error fetching season links")
        links.extend(season_links.data or [])

    if episode_ids:
        episode_links = (
            db.schema("core")
            .table("media_links")
            .select("id, media_asset_id, context")
            .eq("entity_type", "episode")
            .in_("entity_id", episode_ids)
            .execute()
        )
        if hasattr(episode_links, "error") and episode_links.error:
            raise HTTPException(status_code=502, detail="Database error fetching episode links")
        links.extend(episode_links.data or [])

    # Group links by media_asset_id
    links_by_asset: dict[str, list[dict[str, Any]]] = {}
    for link in links:
        media_asset_id = link.get("media_asset_id")
        if not media_asset_id:
            continue
        links_by_asset.setdefault(media_asset_id, []).append(link)

    asset_ids = list(links_by_asset.keys())
    if not asset_ids:
        return AutoCountShowImagesResponse(
            assets_total=0,
            assets_counted=0,
            assets_skipped=0,
            assets_failed=0,
        )

    assets_response = (
        db.schema("core").table("media_assets").select("id, hosted_url, source_url").in_("id", asset_ids).execute()
    )
    if hasattr(assets_response, "error") and assets_response.error:
        raise HTTPException(status_code=502, detail="Database error fetching media assets")

    assets = {row["id"]: row for row in (assets_response.data or [])}

    assets_total = len(asset_ids)
    assets_counted = 0
    assets_skipped = 0
    assets_failed = 0

    for asset_id in asset_ids:
        links_for_asset = links_by_asset.get(asset_id, [])
        if not links_for_asset:
            assets_skipped += 1
            continue

        if not payload.force:
            if any(has_manual_people_tags(link.get("context")) for link in links_for_asset):
                assets_skipped += 1
                continue
            if any(has_people_count(link.get("context")) for link in links_for_asset):
                assets_skipped += 1
                continue

        asset = assets.get(asset_id)
        if not asset:
            assets_skipped += 1
            continue

        image_url = asset.get("hosted_url")
        if not image_url:
            assets_skipped += 1
            continue

        try:
            result = count_people(image_url)
        except ScreenalyticsClientError:
            assets_failed += 1
            continue

        update_person_links_context(
            db,
            links_for_asset,
            {
                "people_count": result.people_count,
                "people_count_source": "auto",
                "people_count_detector": result.detector,
            },
        )
        assets_counted += 1

    return AutoCountShowImagesResponse(
        assets_total=assets_total,
        assets_counted=assets_counted,
        assets_skipped=assets_skipped,
        assets_failed=assets_failed,
    )
