"""
Admin endpoints for URL-based image scraping.

Provides endpoints to:
1. Preview images from a URL (scrape without saving)
2. Import selected images to S3 and database
"""

from __future__ import annotations

import logging
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
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


class ImportResponse(BaseModel):
    """Response after importing images."""

    imported: int
    skipped_duplicates: int
    errors: list[str]
    assets: list[MediaAssetSummary]


# Endpoints


@router.post("/preview", response_model=ScrapePreviewResponse)
def preview_scrape(
    request: ScrapePreviewRequest,
    _: AdminUser = Depends(),
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
    _: AdminUser = Depends(),
) -> ImportResponse:
    """
    Import selected images to S3 and save metadata to database.

    Images are:
    1. Downloaded and hashed
    2. Uploaded to S3 with content-addressed key
    3. Saved to media_assets table
    4. Linked to season via media_links table
    """
    from urllib.parse import urlparse

    from trr_backend.media.s3_mirror import (
        build_hosted_url,
        build_season_image_s3_key,
        get_s3_bucket,
        get_s3_client,
        guess_ext_from_content_type,
        upload_bytes_to_s3,
    )
    from trr_backend.repositories.web_scrape_images import (
        create_media_asset_from_scrape,
        create_media_link_for_season,
        find_asset_by_sha256,
        get_season_and_show_identifiers,
    )

    # Get show identifier for S3 path
    identifiers = get_season_and_show_identifiers(db, str(request.show_id), request.season_number)
    if not identifiers:
        raise HTTPException(
            status_code=404,
            detail=f"Season {request.season_number} not found for show {request.show_id}",
        )

    season_id = identifiers["season_id"]
    show_identifier = identifiers["show_identifier"]

    # Extract domain for source naming
    parsed_source = urlparse(str(request.source_url))
    source_domain = parsed_source.netloc.replace("www.", "")
    source = f"web_scrape:{source_domain}"

    s3_client = get_s3_client()
    bucket = get_s3_bucket()

    imported_count = 0
    skipped_count = 0
    errors: list[str] = []
    assets: list[MediaAssetSummary] = []

    for idx, img in enumerate(request.images):
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

                # Still create link to season
                create_media_link_for_season(
                    db,
                    season_id=season_id,
                    media_asset_id=existing["id"],
                    kind="gallery",
                    position=idx,
                    context={"source_page": str(request.source_url)},
                )

                assets.append(
                    MediaAssetSummary(
                        id=existing["id"],
                        hosted_url=existing.get("hosted_url", ""),
                        width=existing.get("width"),
                        height=existing.get("height"),
                        caption=img.caption,
                    )
                )
                continue

            # Build S3 key and upload
            ext = guess_ext_from_content_type(content_type)
            s3_key = build_season_image_s3_key(
                show_identifier=show_identifier,
                season_number=request.season_number,
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

            # Create media link to season
            create_media_link_for_season(
                db,
                season_id=season_id,
                media_asset_id=asset["id"],
                kind="gallery",
                position=idx,
                context={"source_page": str(request.source_url)},
            )

            imported_count += 1
            assets.append(
                MediaAssetSummary(
                    id=asset["id"],
                    hosted_url=hosted_url,
                    width=asset.get("width"),
                    height=asset.get("height"),
                    caption=img.caption,
                )
            )

        except Exception as exc:
            logger.exception(f"Failed to import image {idx}: {img.url}")
            errors.append(f"Image {idx}: {exc}")

    return ImportResponse(
        imported=imported_count,
        skipped_duplicates=skipped_count,
        errors=errors,
        assets=assets,
    )
