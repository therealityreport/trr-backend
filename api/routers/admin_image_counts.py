"""Admin endpoints for auto-counting people in images."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from api.auth import AdminUser
from api.deps import SupabaseAdminClient
from trr_backend.clients.screenalytics import (
    ScreenalyticsClientError,
    auto_thumbnail_crop,
    count_people,
    face_centroid,
)
from trr_backend.media.s3_mirror import normalize_fandom_file_url
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


def _is_http_url(value: str | None) -> bool:
    if not isinstance(value, str):
        return False
    trimmed = value.strip().lower()
    return trimmed.startswith("http://") or trimmed.startswith("https://")


def _iter_unique_urls(candidates: list[str | None]) -> list[str]:
    seen: set[str] = set()
    urls: list[str] = []
    for value in candidates:
        if not _is_http_url(value):
            continue
        normalized = str(value).strip()
        if normalized in seen:
            continue
        seen.add(normalized)
        urls.append(normalized)
    return urls


def _build_cast_photo_count_urls(row: dict[str, Any]) -> list[str]:
    source = str(row.get("source") or "").lower()
    hosted_url = row.get("hosted_url")
    image_url = row.get("image_url")
    url = row.get("url")
    thumb_url = row.get("thumb_url")
    referer = row.get("source_page_url") if isinstance(row.get("source_page_url"), str) else None
    if source in {"fandom", "fandom-gallery"}:
        normalized = [
            normalize_fandom_file_url(str(value), referer=referer) if isinstance(value, str) else None
            for value in (image_url, url, thumb_url)
        ]
        return _iter_unique_urls([hosted_url, *normalized, image_url, url, thumb_url])
    return _iter_unique_urls([hosted_url, image_url, url, thumb_url])


def _build_media_asset_count_urls(row: dict[str, Any]) -> list[str]:
    hosted_url = row.get("hosted_url")
    source_url = row.get("source_url")
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    source_url_lower = source_url.lower() if isinstance(source_url, str) else ""
    referer = None
    if isinstance(metadata, dict):
        referer = (metadata.get("page_url") if isinstance(metadata.get("page_url"), str) else None) or (
            metadata.get("source_page_url") if isinstance(metadata.get("source_page_url"), str) else None
        )
    if isinstance(source_url, str) and (
        "fandom" in source_url_lower or "static.wikia.nocookie.net" in source_url_lower
    ):
        normalized = normalize_fandom_file_url(source_url, referer=referer)
        return _iter_unique_urls([hosted_url, normalized, source_url])
    return _iter_unique_urls([hosted_url, source_url])


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
        db.schema("core")
        .table("cast_photos")
        .select("id, hosted_url, url, image_url, thumb_url, source, source_page_url, metadata")
        .eq("id", str(photo_id))
        .limit(1)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise HTTPException(status_code=502, detail="Database error fetching cast photo")
    if not response.data:
        raise HTTPException(status_code=404, detail="Cast photo not found")

    row = response.data[0]
    image_urls = _build_cast_photo_count_urls(row)
    if not image_urls:
        raise HTTPException(
            status_code=409,
            detail="Cast photo has no valid image URL to analyze",
        )

    tag_rows = get_tags_by_photo_ids(db, [str(photo_id)])
    tag_row = tag_rows.get(str(photo_id))
    if has_manual_tags(tag_row) and not force:
        raise HTTPException(status_code=409, detail="Manual tags/count exist; use force to overwrite")

    result = None
    last_error: ScreenalyticsClientError | None = None
    for image_url in image_urls:
        try:
            result = count_people(image_url)
            break
        except ScreenalyticsClientError as exc:
            last_error = exc
    if result is None:
        raise HTTPException(status_code=502, detail=str(last_error or "Failed to auto-count cast photo"))

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
    generated_crop = auto_thumbnail_crop(result)
    centroid = face_centroid(result)
    if generated_crop is not None or centroid is not None:
        metadata = dict(row.get("metadata") or {})
        existing_crop = metadata.get("thumbnail_crop")
        if not (isinstance(existing_crop, dict) and existing_crop.get("mode") == "manual"):
            if generated_crop is not None:
                metadata["thumbnail_crop"] = {
                    **generated_crop,
                    "generated_at": datetime.now(UTC).isoformat(),
                }
            elif centroid is not None:
                cx, cy = centroid
                metadata["thumbnail_crop"] = {
                    "x": cx,
                    "y": cy,
                    "zoom": 1,
                    "mode": "auto",
                    "strategy": "face_centroid_v1",
                    "generated_at": datetime.now(UTC).isoformat(),
                }
            try:
                db.schema("core").table("cast_photos").update(
                    {"metadata": metadata}
                ).eq("id", str(photo_id)).execute()
            except Exception:
                # Best effort: count should still succeed if crop write fails.
                pass

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
        .select("id, hosted_url, source_url, metadata")
        .eq("id", str(asset_id))
        .limit(1)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise HTTPException(status_code=502, detail="Database error fetching media asset")
    if not response.data:
        raise HTTPException(status_code=404, detail="Media asset not found")

    row = response.data[0]
    image_urls = _build_media_asset_count_urls(row)
    if not image_urls:
        raise HTTPException(
            status_code=409,
            detail="Media asset has no valid image URL to analyze",
        )

    links = list_person_links_by_asset_id(db, str(asset_id))
    if not links:
        raise HTTPException(status_code=404, detail="No person links found for asset")
    if any(has_manual_people_tags(link.get("context")) for link in links) and not force:
        raise HTTPException(status_code=409, detail="Manual tags/count exist; use force to overwrite")

    result = None
    last_error: ScreenalyticsClientError | None = None
    for image_url in image_urls:
        try:
            result = count_people(image_url)
            break
        except ScreenalyticsClientError as exc:
            last_error = exc
    if result is None:
        raise HTTPException(status_code=502, detail=str(last_error or "Failed to auto-count media asset"))

    update_person_links_context(
        db,
        links,
        {
            "people_count": result.people_count,
            "people_count_source": "auto",
            "people_count_detector": result.detector,
        },
    )
    generated_crop = auto_thumbnail_crop(result)
    centroid = face_centroid(result)
    if generated_crop is not None or centroid is not None:
        now = datetime.now(UTC).isoformat()
        for link in links:
            context = dict(link.get("context") or {})
            existing_crop = context.get("thumbnail_crop")
            if isinstance(existing_crop, dict) and existing_crop.get("mode") == "manual":
                continue
            if generated_crop is not None:
                context["thumbnail_crop"] = {
                    **generated_crop,
                    "generated_at": now,
                }
            elif centroid is not None:
                cx, cy = centroid
                context["thumbnail_crop"] = {
                    "x": cx,
                    "y": cy,
                    "zoom": 1,
                    "mode": "auto",
                    "strategy": "face_centroid_v1",
                    "generated_at": now,
                }
            try:
                db.schema("core").table("media_links").update(
                    {"context": context, "updated_at": now}
                ).eq("id", link["id"]).execute()
            except Exception:
                # Best effort: count should still succeed if crop write fails.
                continue

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
