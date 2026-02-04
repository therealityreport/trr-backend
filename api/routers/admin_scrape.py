"""
Admin endpoints for URL-based image scraping.

Provides endpoints to:
1. Preview images from a URL (scrape without saving)
2. Import selected images to S3 and database
3. Import with SSE streaming progress
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import AsyncGenerator
from difflib import SequenceMatcher
from typing import Literal
from urllib.parse import unquote, urlparse
from uuid import UUID

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, HttpUrl, model_validator

from api.auth import AdminUser
from api.deps import SupabaseAdminClient
from trr_backend.scraping.url_image_scraper import (
    download_and_hash_image,
    scrape_url_for_images,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/scrape", tags=["admin-scrape"])


# Request/Response Models


class ScrapePreviewRequest(BaseModel):
    """Request to preview images from a URL."""

    url: HttpUrl
    min_width: int = Field(default=200, ge=0, le=2000)
    limit: int = Field(default=50, ge=1, le=100)


class ImageCandidateResponse(BaseModel):
    """An image candidate from scraping."""

    id: str
    original_url: str
    best_url: str
    width: int | None = None
    height: int | None = None
    alt_text: str | None = None
    context: str | None = None
    thumbnail_url: str


class ScrapePreviewResponse(BaseModel):
    """Response with scraped image candidates."""

    url: str
    page_title: str | None
    domain: str
    images: list[ImageCandidateResponse]
    total_found: int
    error: str | None = None


class ImportImageItem(BaseModel):
    """Single image to import."""

    candidate_id: str
    url: HttpUrl
    caption: str | None = None


EntityType = Literal["season", "person"]


class ImportRequest(BaseModel):
    """Request to import selected images."""

    entity_type: EntityType = "season"

    # Season-specific
    show_id: UUID | None = None
    season_number: int | None = Field(default=None, ge=0, le=100)

    # Person-specific
    person_id: UUID | None = None

    # Common
    source_url: HttpUrl
    images: list[ImportImageItem] = Field(min_length=1, max_length=50)

    # Cast matching - when True for season imports, auto-match filenames to cast
    match_cast: bool = False

    @model_validator(mode="after")
    def validate_entity_fields(self):
        if self.entity_type == "season":
            if not self.show_id:
                raise ValueError("show_id required for season")
            if self.season_number is None:
                raise ValueError("season_number required for season")
        elif self.entity_type == "person":
            if not self.person_id:
                raise ValueError("person_id required for person")
        return self


class MediaAssetSummary(BaseModel):
    """Summary of an imported media asset."""

    id: str
    hosted_url: str
    width: int | None = None
    height: int | None = None
    caption: str | None = None
    matched_person_id: str | None = None
    matched_person_name: str | None = None
    match_confidence: float | None = None


class ImportResponse(BaseModel):
    """Response after importing images."""

    imported: int
    skipped_duplicates: int
    errors: list[str]
    assets: list[MediaAssetSummary]


# Cast Member Matching Helpers


def _extract_filename_from_url(url: str) -> str:
    """Extract and clean filename from URL for cast matching."""
    parsed = urlparse(url)
    path = unquote(parsed.path)
    filename = path.split("/")[-1] if "/" in path else path
    return filename


def _fuzzy_match_cast(filename: str, cast_members: list[dict]) -> tuple[dict | None, float]:
    """
    Match filename against cast member names.

    Returns (best_match, confidence) where best_match is None if no match found.
    """
    # Clean filename for matching
    clean_name = re.sub(r"[-_]", " ", filename.lower())
    clean_name = re.sub(r"\.[a-z]{3,4}$", "", clean_name)  # Remove extension
    clean_name = re.sub(r"\d+x\d+", "", clean_name)  # Remove dimensions
    clean_name = re.sub(r"(scaled|large|medium|small|thumb)", "", clean_name)
    clean_name = clean_name.strip()

    if not clean_name:
        return None, 0.0

    best_match = None
    best_score = 0.0

    for member in cast_members:
        full_name = (member.get("full_name") or "").lower()
        if not full_name:
            continue

        # Try sequence matching
        score = SequenceMatcher(None, clean_name, full_name).ratio()

        # Boost if name parts are found in filename
        name_parts = full_name.split()
        for part in name_parts:
            if len(part) > 2 and part in clean_name:
                score = max(score, 0.7)  # Boost if name part found

        # Exact full name match
        if full_name in clean_name or clean_name in full_name:
            score = max(score, 0.9)

        if score > best_score and score >= 0.6:  # 60% threshold
            best_score = score
            best_match = member

    return best_match, best_score


def _get_season_cast(db, show_id: str, season_number: int) -> list[dict]:
    """
    Get cast members for a season with their names.

    Returns list of dicts with person_id, full_name, identifier.
    """
    from trr_backend.db import pg

    sql = """
        SELECT DISTINCT
            vsc.person_id::text,
            p.full_name,
            p.identifier
        FROM core.v_season_cast vsc
        JOIN core.people p ON p.id = vsc.person_id
        JOIN core.seasons s ON s.id = vsc.season_id
        WHERE vsc.show_id = %s
          AND s.season_number = %s
        ORDER BY p.full_name
    """
    return pg.fetch_all(sql, [show_id, season_number])


# Endpoints


@router.post("/preview", response_model=ScrapePreviewResponse)
def preview_scrape(
    request: ScrapePreviewRequest,
    _: AdminUser,
) -> ScrapePreviewResponse:
    """
    Scrape a URL and return image candidates for preview.

    Does not save anything - just returns what images are available.
    """
    result = scrape_url_for_images(
        str(request.url),
        min_width=request.min_width,
        limit=request.limit,
    )

    if result.error:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to fetch URL: {result.error}",
        )

    return ScrapePreviewResponse(
        url=result.url,
        page_title=result.page_title,
        domain=result.domain,
        images=[
            ImageCandidateResponse(
                id=img.id,
                original_url=img.original_url,
                best_url=img.best_url,
                width=img.width,
                height=img.height,
                alt_text=img.alt_text,
                context=img.context,
                thumbnail_url=img.thumbnail_url or img.best_url,
            )
            for img in result.images
        ],
        total_found=result.total_found,
        error=None,
    )


@router.post("/import", response_model=ImportResponse)
def import_images(
    request: ImportRequest,
    db: SupabaseAdminClient,
    _: AdminUser,
) -> ImportResponse:
    """
    Import selected images to S3 and save metadata to database.

    Images are:
    1. Downloaded and hashed
    2. Uploaded to S3 with content-addressed key
    3. Saved to media_assets table
    4. Linked to season via media_links table
    """
    from trr_backend.media.s3_mirror import (
        build_cast_photo_s3_key,
        build_hosted_url,
        build_season_image_s3_key,
        get_s3_bucket,
        get_s3_client,
        guess_ext_from_content_type,
        upload_bytes_to_s3,
    )
    from trr_backend.repositories.web_scrape_images import (
        create_media_asset_from_scrape,
        create_media_link_for_entity,
        find_asset_by_sha256,
        get_person_identifier,
        get_season_and_show_identifiers,
    )

    # Extract domain for source naming
    parsed_source = urlparse(str(request.source_url))
    source_domain = parsed_source.netloc.replace("www.", "")
    source = f"web_scrape:{source_domain}"

    # Entity-specific setup
    if request.entity_type == "season":
        identifiers = get_season_and_show_identifiers(db, str(request.show_id), request.season_number)
        if not identifiers:
            raise HTTPException(
                status_code=404,
                detail=f"Season {request.season_number} not found for show {request.show_id}",
            )
        entity_id = identifiers["season_id"]
        path_identifier = identifiers["show_identifier"]
        link_context = {
            "entity_type": "season",
            "entity_id": entity_id,
            "show_id": str(request.show_id),
            "season_number": request.season_number,
            "source_url": str(request.source_url),
        }

    elif request.entity_type == "person":
        person_info = get_person_identifier(db, str(request.person_id))
        if not person_info:
            raise HTTPException(
                status_code=404,
                detail=f"Person {request.person_id} not found",
            )
        entity_id = str(request.person_id)
        path_identifier = person_info["identifier"]
        link_context = {
            "entity_type": "person",
            "entity_id": entity_id,
            "person_full_name": person_info.get("full_name"),
            "source_url": str(request.source_url),
        }

    # Fetch cast members for matching if enabled
    cast_members: list[dict] = []
    if request.match_cast and request.entity_type == "season":
        cast_members = _get_season_cast(db, str(request.show_id), request.season_number)
        logger.info(f"Loaded {len(cast_members)} cast members for matching")

    s3_client = get_s3_client()
    bucket = get_s3_bucket()

    imported_count = 0
    skipped_count = 0
    errors: list[str] = []
    assets: list[MediaAssetSummary] = []
    auto_count_assets: dict[str, str] = {}

    for idx, img in enumerate(request.images):
        # Cast matching - extract filename and try to match
        matched_person: dict | None = None
        match_confidence: float = 0.0
        if cast_members:
            filename = _extract_filename_from_url(str(img.url))
            matched_person, match_confidence = _fuzzy_match_cast(filename, cast_members)
            if matched_person:
                logger.info(
                    f"Matched '{filename}' to {matched_person['full_name']} (confidence: {match_confidence:.2f})"
                )
        try:
            # Download image
            image_data, sha256, content_type = download_and_hash_image(
                str(img.url),
                referer=str(request.source_url),
            )

            # Check for duplicate
            existing = find_asset_by_sha256(db, sha256)
            if existing:
                # Asset exists - just create a link if needed
                logger.info(f"Image {idx} already exists with sha256={sha256[:16]}...")
                skipped_count += 1

                # Still create link to entity
                create_media_link_for_entity(
                    db,
                    entity_type=request.entity_type,
                    entity_id=entity_id,
                    media_asset_id=existing["id"],
                    kind="gallery",
                    position=idx,
                    context=link_context,
                )

                # Also link to person if matched
                if matched_person:
                    person_link_ctx = {
                        "entity_type": "person",
                        "entity_id": matched_person["person_id"],
                        "matched_from_season": True,
                        "match_confidence": match_confidence,
                        "source_url": str(request.source_url),
                    }
                    create_media_link_for_entity(
                        db,
                        entity_type="person",
                        entity_id=matched_person["person_id"],
                        media_asset_id=existing["id"],
                        kind="gallery",
                        position=idx,
                        context=person_link_ctx,
                    )

                if request.entity_type == "person" or matched_person:
                    auto_count_assets[existing["id"]] = existing.get("hosted_url") or existing.get("source_url") or str(img.url)

                assets.append(
                    MediaAssetSummary(
                        id=existing["id"],
                        hosted_url=existing.get("hosted_url", ""),
                        width=existing.get("width"),
                        height=existing.get("height"),
                        caption=img.caption,
                        matched_person_id=matched_person["person_id"] if matched_person else None,
                        matched_person_name=matched_person["full_name"] if matched_person else None,
                        match_confidence=match_confidence if matched_person else None,
                    )
                )
                continue

            # Build S3 key based on entity type
            ext = guess_ext_from_content_type(content_type)
            if request.entity_type == "season":
                s3_key = build_season_image_s3_key(
                    show_identifier=path_identifier,
                    season_number=request.season_number,
                    source="web_scrape",
                    sha256=sha256,
                    ext=ext,
                )
            elif request.entity_type == "person":
                s3_key = build_cast_photo_s3_key(
                    person_identifier=path_identifier,
                    source="web_scrape",
                    sha256=sha256,
                    ext=ext,
                )

            etag, file_size = upload_bytes_to_s3(
                s3_client,
                bucket=bucket,
                key=s3_key,
                data=image_data,
                content_type=content_type,
            )

            hosted_url = build_hosted_url(s3_key)

            # Create media asset record
            asset = create_media_asset_from_scrape(
                db,
                source=source,
                source_url=str(img.url),
                sha256=sha256,
                hosted_bucket=bucket,
                hosted_key=s3_key,
                hosted_url=hosted_url,
                hosted_bytes=file_size,
                hosted_etag=etag,
                content_type=content_type,
                width=None,  # Could extract with PIL if needed
                height=None,
                caption=img.caption,
                metadata={
                    "page_url": str(request.source_url),
                    "candidate_id": img.candidate_id,
                },
            )

            # Create media link to entity
            create_media_link_for_entity(
                db,
                entity_type=request.entity_type,
                entity_id=entity_id,
                media_asset_id=asset["id"],
                kind="gallery",
                position=idx,
                context=link_context,
            )

            # Also link to person if matched
            if matched_person:
                person_link_ctx = {
                    "entity_type": "person",
                    "entity_id": matched_person["person_id"],
                    "matched_from_season": True,
                    "match_confidence": match_confidence,
                    "source_url": str(request.source_url),
                }
                create_media_link_for_entity(
                    db,
                    entity_type="person",
                    entity_id=matched_person["person_id"],
                    media_asset_id=asset["id"],
                    kind="gallery",
                    position=idx,
                    context=person_link_ctx,
                )

            if request.entity_type == "person" or matched_person:
                auto_count_assets[asset["id"]] = hosted_url or str(img.url)

            imported_count += 1
            assets.append(
                MediaAssetSummary(
                    id=asset["id"],
                    hosted_url=hosted_url,
                    width=asset.get("width"),
                    height=asset.get("height"),
                    caption=img.caption,
                    matched_person_id=matched_person["person_id"] if matched_person else None,
                    matched_person_name=matched_person["full_name"] if matched_person else None,
                    match_confidence=match_confidence if matched_person else None,
                )
            )

        except Exception as exc:
            logger.exception(f"Failed to import image {idx}: {img.url}")
            errors.append(f"Image {idx}: {exc}")

    if auto_count_assets:
        try:
            from trr_backend.clients.screenalytics import ScreenalyticsClientError, count_people
            from trr_backend.repositories.media_links import (
                has_manual_people_tags,
                has_people_count,
                list_person_links_by_asset_id,
                update_person_links_context,
            )

            for asset_id, image_url in auto_count_assets.items():
                links = list_person_links_by_asset_id(db, asset_id)
                if not links:
                    continue
                if any(has_manual_people_tags(link.get("context")) for link in links):
                    continue
                if any(has_people_count(link.get("context")) for link in links):
                    continue
                if not image_url:
                    continue
                try:
                    result = count_people(image_url)
                    update_person_links_context(
                        db,
                        links,
                        {
                            "people_count": result.people_count,
                            "people_count_source": "auto",
                            "people_count_detector": result.detector,
                        },
                    )
                except ScreenalyticsClientError as exc:
                    logger.warning("Auto-count failed for media_asset %s: %s", asset_id, exc)
        except Exception as exc:
            logger.warning("Auto-count setup failed: %s", exc)

    return ImportResponse(
        imported=imported_count,
        skipped_duplicates=skipped_count,
        errors=errors,
        assets=assets,
    )


@router.post("/import/stream")
async def import_images_stream(
    request: ImportRequest,
    db: SupabaseAdminClient,
    _: AdminUser,
) -> StreamingResponse:
    """
    Import images with SSE streaming progress updates.

    Streams events:
    - progress: {"current": 1, "total": 5, "url": "...", "status": "downloading"}
    - imported: {"current": 1, "url": "...", "asset_id": "...", "status": "success"}
    - skipped: {"current": 1, "url": "...", "status": "duplicate"}
    - error: {"current": 1, "url": "...", "error": "..."}
    - complete: {"imported": 5, "skipped_duplicates": 0, "errors": [], "assets": [...]}
    """
    from trr_backend.media.s3_mirror import (
        build_cast_photo_s3_key,
        build_hosted_url,
        build_season_image_s3_key,
        get_s3_bucket,
        get_s3_client,
        guess_ext_from_content_type,
        upload_bytes_to_s3,
    )
    from trr_backend.repositories.web_scrape_images import (
        create_media_asset_from_scrape,
        create_media_link_for_entity,
        find_asset_by_sha256,
        get_person_identifier,
        get_season_and_show_identifiers,
    )

    async def event_generator() -> AsyncGenerator[str, None]:
        # Extract domain for source naming
        parsed_source = urlparse(str(request.source_url))
        source_domain = parsed_source.netloc.replace("www.", "")
        source = f"web_scrape:{source_domain}"

        # Entity-specific setup
        if request.entity_type == "season":
            identifiers = get_season_and_show_identifiers(db, str(request.show_id), request.season_number)
            if not identifiers:
                yield f"event: error\ndata: {json.dumps({'error': f'Season {request.season_number} not found'})}\n\n"
                return
            entity_id = identifiers["season_id"]
            path_identifier = identifiers["show_identifier"]
            link_context = {
                "entity_type": "season",
                "entity_id": entity_id,
                "show_id": str(request.show_id),
                "season_number": request.season_number,
                "source_url": str(request.source_url),
            }
        elif request.entity_type == "person":
            person_info = get_person_identifier(db, str(request.person_id))
            if not person_info:
                err_data = {"error": f"Person {request.person_id} not found"}
                yield f"event: error\ndata: {json.dumps(err_data)}\n\n"
                return
            entity_id = str(request.person_id)
            path_identifier = person_info["identifier"]
            link_context = {
                "entity_type": "person",
                "entity_id": entity_id,
                "person_full_name": person_info.get("full_name"),
                "source_url": str(request.source_url),
            }

        # Fetch cast members for matching if enabled
        cast_members: list[dict] = []
        if request.match_cast and request.entity_type == "season":
            cast_members = _get_season_cast(db, str(request.show_id), request.season_number)
            logger.info(f"Loaded {len(cast_members)} cast members for matching")

        s3_client = get_s3_client()
        bucket = get_s3_bucket()

        imported_count = 0
        skipped_count = 0
        errors: list[str] = []
        assets: list[dict] = []
        total = len(request.images)

        for idx, img in enumerate(request.images):
            current = idx + 1
            img_url = str(img.url)

            # Cast matching - extract filename and try to match
            matched_person: dict | None = None
            match_confidence: float = 0.0
            if cast_members:
                filename = _extract_filename_from_url(img_url)
                matched_person, match_confidence = _fuzzy_match_cast(filename, cast_members)

            # Yield progress event
            progress_data = {
                "current": current,
                "total": total,
                "url": img_url,
                "status": "downloading",
            }
            yield f"event: progress\ndata: {json.dumps(progress_data)}\n\n"

            try:
                # Download image
                image_data, sha256, content_type = download_and_hash_image(
                    img_url,
                    referer=str(request.source_url),
                )

                # Check for duplicate
                existing = find_asset_by_sha256(db, sha256)
                if existing:
                    logger.info(f"Image {idx} already exists with sha256={sha256[:16]}...")
                    skipped_count += 1

                    # Still create link to entity
                    create_media_link_for_entity(
                        db,
                        entity_type=request.entity_type,
                        entity_id=entity_id,
                        media_asset_id=existing["id"],
                        kind="gallery",
                        position=idx,
                        context=link_context,
                    )

                    # Also link to person if matched
                    if matched_person:
                        person_link_ctx = {
                            "entity_type": "person",
                            "entity_id": matched_person["person_id"],
                            "matched_from_season": True,
                            "match_confidence": match_confidence,
                            "source_url": str(request.source_url),
                        }
                        create_media_link_for_entity(
                            db,
                            entity_type="person",
                            entity_id=matched_person["person_id"],
                            media_asset_id=existing["id"],
                            kind="gallery",
                            position=idx,
                            context=person_link_ctx,
                        )

                    assets.append(
                        {
                            "id": existing["id"],
                            "hosted_url": existing.get("hosted_url", ""),
                            "width": existing.get("width"),
                            "height": existing.get("height"),
                            "caption": img.caption,
                            "matched_person_id": (matched_person["person_id"] if matched_person else None),
                            "matched_person_name": (matched_person["full_name"] if matched_person else None),
                            "match_confidence": (match_confidence if matched_person else None),
                        }
                    )

                    skipped_data = {
                        "current": current,
                        "total": total,
                        "url": img_url,
                        "asset_id": existing["id"],
                        "status": "duplicate",
                        "matched_person_id": (matched_person["person_id"] if matched_person else None),
                    }
                    yield f"event: skipped\ndata: {json.dumps(skipped_data)}\n\n"
                    continue

                # Build S3 key based on entity type
                ext = guess_ext_from_content_type(content_type)
                if request.entity_type == "season":
                    s3_key = build_season_image_s3_key(
                        show_identifier=path_identifier,
                        season_number=request.season_number,
                        source="web_scrape",
                        sha256=sha256,
                        ext=ext,
                    )
                elif request.entity_type == "person":
                    s3_key = build_cast_photo_s3_key(
                        person_identifier=path_identifier,
                        source="web_scrape",
                        sha256=sha256,
                        ext=ext,
                    )

                etag, file_size = upload_bytes_to_s3(
                    s3_client,
                    bucket=bucket,
                    key=s3_key,
                    data=image_data,
                    content_type=content_type,
                )

                hosted_url = build_hosted_url(s3_key)

                # Create media asset record
                asset = create_media_asset_from_scrape(
                    db,
                    source=source,
                    source_url=img_url,
                    sha256=sha256,
                    hosted_bucket=bucket,
                    hosted_key=s3_key,
                    hosted_url=hosted_url,
                    hosted_bytes=file_size,
                    hosted_etag=etag,
                    content_type=content_type,
                    width=None,
                    height=None,
                    caption=img.caption,
                    metadata={
                        "page_url": str(request.source_url),
                        "candidate_id": img.candidate_id,
                    },
                )

                # Create media link to entity
                create_media_link_for_entity(
                    db,
                    entity_type=request.entity_type,
                    entity_id=entity_id,
                    media_asset_id=asset["id"],
                    kind="gallery",
                    position=idx,
                    context=link_context,
                )

                # Also link to person if matched
                if matched_person:
                    person_link_ctx = {
                        "entity_type": "person",
                        "entity_id": matched_person["person_id"],
                        "matched_from_season": True,
                        "match_confidence": match_confidence,
                        "source_url": str(request.source_url),
                    }
                    create_media_link_for_entity(
                        db,
                        entity_type="person",
                        entity_id=matched_person["person_id"],
                        media_asset_id=asset["id"],
                        kind="gallery",
                        position=idx,
                        context=person_link_ctx,
                    )

                imported_count += 1
                assets.append(
                    {
                        "id": asset["id"],
                        "hosted_url": hosted_url,
                        "width": asset.get("width"),
                        "height": asset.get("height"),
                        "caption": img.caption,
                        "matched_person_id": (matched_person["person_id"] if matched_person else None),
                        "matched_person_name": (matched_person["full_name"] if matched_person else None),
                        "match_confidence": (match_confidence if matched_person else None),
                    }
                )

                imported_data = {
                    "current": current,
                    "total": total,
                    "url": img_url,
                    "asset_id": asset["id"],
                    "status": "success",
                    "matched_person_id": (matched_person["person_id"] if matched_person else None),
                }
                yield f"event: imported\ndata: {json.dumps(imported_data)}\n\n"

            except Exception as exc:
                logger.exception(f"Failed to import image {idx}: {img_url}")
                error_msg = f"Image {idx}: {exc}"
                errors.append(error_msg)
                error_data = {
                    "current": current,
                    "total": total,
                    "url": img_url,
                    "error": str(exc),
                }
                yield f"event: error\ndata: {json.dumps(error_data)}\n\n"

        # Final completion event
        complete_data = {
            "imported": imported_count,
            "skipped_duplicates": skipped_count,
            "errors": errors,
            "assets": assets,
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
