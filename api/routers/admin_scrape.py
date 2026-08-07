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
import os
import re
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from difflib import SequenceMatcher
from typing import Any, Literal
from urllib.parse import unquote, urlparse
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, HttpUrl, model_validator

from api.auth import InternalAdminUser
from api.deps import SupabaseAdminClient
from trr_backend.pipeline.admin_operation_registry import (
    get_show_sync_capabilities,
)
from trr_backend.pipeline.admin_operations import operation_stream_response, start_operation_for_stream
from trr_backend.scraping.url_image_scraper import (
    download_and_hash_image,
    scrape_url_for_images,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/scrape", tags=["admin-scrape"])


def _clean_optional_import_text(value: str | None) -> str | None:
    cleaned = str(value or "").strip()
    return cleaned or None


def _brand_logo_routing_v2_enabled() -> bool:
    return str(os.getenv("BRAND_LOGO_ROUTING_V2", "true")).strip().lower() not in {"0", "false", "off", "no"}


def _is_non_blocking_logo_variant_failure(error: Exception) -> bool:
    message = str(error or "").strip().lower()
    return message in {"logo_decode_failed", "transparent_extraction_failed"}


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
    bytes: int | None = None
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


# "kind" is stored in core.media_links.kind (text), so expanding is safe.
# Keep values in sync with TRR-APP's ImageScrapeDrawer kind options.
ImageKind = Literal[
    "poster",
    "backdrop",
    "logo",
    "episode_still",
    "cast",
    "promo",
    "intro",
    "reunion",
    "other",
]

LogoTargetType = Literal[
    "show",
    "network",
    "streaming",
    "production",
    "franchise",
    "publication",
    "social",
    "other",
]


class ImportImageItem(BaseModel):
    """Single image to import."""

    candidate_id: str
    url: HttpUrl
    caption: str | None = None
    kind: ImageKind = "other"
    person_ids: list[UUID] | None = None
    # Optional per-image metadata for richer gallery organization.
    context_section: str | None = None
    context_type: str | None = None
    source_logo: str | None = None
    logo_target_type: LogoTargetType | None = None
    logo_target_key: str | None = None
    logo_target_label: str | None = None
    logo_set_primary: bool = False
    asset_name: str | None = None

    @model_validator(mode="after")
    def validate_import_image_item(self):
        self.caption = _clean_optional_import_text(self.caption)
        self.context_section = _clean_optional_import_text(self.context_section)
        self.context_type = _clean_optional_import_text(self.context_type)
        self.source_logo = _clean_optional_import_text(self.source_logo)
        self.logo_target_key = _clean_optional_import_text(self.logo_target_key)
        self.logo_target_label = _clean_optional_import_text(self.logo_target_label)
        self.asset_name = _clean_optional_import_text(self.asset_name)

        if self.person_ids:
            deduped_ids: list[UUID] = []
            seen_ids: set[UUID] = set()
            for person_id in self.person_ids:
                if person_id in seen_ids:
                    continue
                seen_ids.add(person_id)
                deduped_ids.append(person_id)
            self.person_ids = deduped_ids or None

        has_logo_metadata = any(
            [
                self.source_logo,
                self.logo_target_type,
                self.logo_target_key,
                self.logo_target_label,
                self.logo_set_primary,
            ]
        )
        if self.kind == "logo":
            if self.person_ids:
                raise ValueError("person_ids are not supported for logo images")
            if (self.logo_target_type or _legacy_logo_target_type(self.source_logo)) is None:
                raise ValueError("logo_target_type is required for logo images")
            return self

        if has_logo_metadata:
            raise ValueError("logo target fields are only supported for logo images")
        return self


EntityType = Literal["season", "show", "person"]


class ImportRequest(BaseModel):
    """Request to import selected images."""

    entity_type: EntityType = "season"

    # Season-specific
    show_id: UUID | None = None
    season_number: int | None = Field(default=None, ge=0, le=100)
    season_id: UUID | None = None

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
            if not self.season_id:
                if not self.show_id:
                    raise ValueError("show_id required for season")
                if self.season_number is None:
                    raise ValueError("season_number required for season")
        elif self.entity_type == "show":
            if not self.show_id:
                raise ValueError("show_id required for show")
        elif self.entity_type == "person":
            if not self.person_id:
                raise ValueError("person_id required for person")
        return self


def _build_people_tags_context(db: SupabaseAdminClient, person_ids: set[str]) -> dict:
    """
    Build a deterministic people tags payload for media_links.context.

    Used to persist PEOPLE tags during URL import (so SOLO/GROUP filtering works),
    without asserting a manual people-count value.
    """
    if not person_ids:
        return {}

    try:
        response = (
            db.schema("core").table("people").select("id, full_name").in_("id", list(person_ids)).limit(500).execute()
        )
    except Exception as exc:
        logger.warning("Failed to lookup people names for tags: %s", exc)
        ids_sorted = sorted(person_ids)
        return {
            "people_ids": ids_sorted,
            "people_names": ids_sorted,
        }

    if hasattr(response, "error") and response.error:
        logger.warning("People lookup error for tags: %s", response.error)
        ids_sorted = sorted(person_ids)
        return {
            "people_ids": ids_sorted,
            "people_names": ids_sorted,
        }

    rows = response.data or []
    cleaned: list[dict] = []
    for row in rows:
        pid = row.get("id")
        name = row.get("full_name")
        if pid and name:
            cleaned.append({"id": str(pid), "full_name": str(name)})

    cleaned.sort(key=lambda r: r["full_name"].lower())
    people_ids = [r["id"] for r in cleaned]
    people_names = [r["full_name"] for r in cleaned]

    if not people_ids:
        ids_sorted = sorted(person_ids)
        return {
            "people_ids": ids_sorted,
            "people_names": ids_sorted,
        }

    return {
        "people_ids": people_ids,
        "people_names": people_names,
    }


def _extract_source_domain(source_url: str) -> str:
    parsed_source = urlparse(source_url)
    return parsed_source.netloc.replace("www.", "").strip().lower()


def _legacy_logo_target_type(source_logo: str | None) -> LogoTargetType | None:
    normalized = str(source_logo or "").strip().upper()
    if normalized == "SHOW":
        return "show"
    if normalized == "SOURCE":
        return "publication"
    return None


def _resolve_logo_target_for_image(
    *,
    img: ImportImageItem,
    request: ImportRequest,
    source_domain: str,
) -> tuple[LogoTargetType, str, str, bool]:
    target_type = img.logo_target_type or _legacy_logo_target_type(img.source_logo)
    if target_type is None:
        raise ValueError("logo_target_type is required for logo images")

    target_key = str(img.logo_target_key or "").strip()
    target_label = str(img.logo_target_label or "").strip()

    if target_type == "show":
        if not target_key:
            if request.show_id is not None:
                target_key = str(request.show_id)
            elif request.season_id is not None:
                target_key = str(request.season_id)
        if not target_label:
            target_label = "Show"
    elif target_type == "publication":
        if not target_key:
            target_key = source_domain
        if not target_label:
            target_label = source_domain
    else:
        if not target_label and target_key:
            target_label = target_key

    if not target_key:
        raise ValueError("logo_target_key is required for logo images")
    if not target_label:
        raise ValueError("logo_target_label is required for logo images")

    return target_type, target_key, target_label, bool(img.logo_set_primary)


def _augment_logo_context(
    context: dict[str, object],
    *,
    img: ImportImageItem,
    logo_target: tuple[LogoTargetType, str, str, bool] | None,
) -> None:
    if img.source_logo:
        context["source_logo"] = img.source_logo
    if not logo_target:
        return
    target_type, target_key, target_label, set_primary = logo_target
    context["logo_target_type"] = target_type
    context["logo_target_key"] = target_key
    context["logo_target_label"] = target_label
    context["logo_set_primary"] = bool(set_primary)


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
    text_overlay_attempted: int = 0
    text_overlay_succeeded: int = 0
    text_overlay_failed: int = 0


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


def _get_show_identifier(db: SupabaseAdminClient, show_id: str) -> dict[str, str] | None:
    """Resolve show identifier for S3 key construction (IMDb ID preferred, UUID fallback)."""
    response = db.schema("core").table("shows").select("id, external_ids").eq("id", show_id).limit(1).execute()
    if hasattr(response, "error") and response.error:
        logger.error("Failed to lookup show identifier for show_id=%s: %s", show_id, response.error)
        return None
    if not response.data:
        return None

    row = response.data[0]
    external_ids = row.get("external_ids") if isinstance(row.get("external_ids"), dict) else {}
    imdb_id = external_ids.get("imdb_id") or external_ids.get("imdb")
    identifier = str(imdb_id or show_id)
    return {
        "show_id": str(row.get("id") or show_id),
        "show_identifier": identifier,
    }


def _import_non_show_logo_target(
    *,
    db: SupabaseAdminClient,
    target_type: LogoTargetType,
    target_key: str,
    target_label: str,
    set_primary: bool,
    image_data: bytes,
    sha256: str,
    content_type: str,
    source_url: str,
    source_page_url: str,
    source_domain: str,
    metadata: dict[str, object],
) -> tuple[str, str | None, str | None]:
    """
    Import logo targets outside the core show/season/person media-links flow.

    Returns: (status, hosted_logo_url, created_asset_id)
    """
    from trr_backend.media.s3_mirror import (
        build_logo_s3_key,
        get_s3_bucket,
        get_s3_client,
        guess_ext_from_content_type,
        mirror_logo_monochrome_variants_row,
        upload_bytes_to_s3,
    )

    admin_show_sync = get_show_sync_capabilities()
    if target_type in {"network", "streaming", "production"}:
        explicit_id: int | None = int(target_key) if target_key.isdigit() else None
        target_row, config = admin_show_sync._resolve_dimension_target(  # noqa: SLF001
            db,
            target_type=target_type,
            explicit_id=explicit_id,
            entity_key=target_key,
        )
        table = config["table"]
        id_field = config["id_field"]
        name_field = config["name_field"]
        entity_type = config["entity_type"]
        logo_kind = config["logo_kind"]
        entity_id = str(target_row.get(id_field) or "")
        entity_key = str(target_row.get(name_field) or "").casefold()
        display_name = str(target_row.get(name_field) or target_label or target_key)

        ext = guess_ext_from_content_type(content_type)
        key = build_logo_s3_key(kind=logo_kind, entity_id=entity_id, sha256=sha256, ext=ext)
        s3_client = get_s3_client()
        bucket = get_s3_bucket()
        etag, file_size = upload_bytes_to_s3(
            s3_client,
            bucket=bucket,
            key=key,
            data=image_data,
            content_type=content_type,
        )
        hosted_url = admin_show_sync.build_hosted_url(key)  # type: ignore[attr-defined]
        patch: dict[str, object] = {
            "hosted_logo_key": key,
            "hosted_logo_url": hosted_url,
            "hosted_logo_sha256": sha256,
            "hosted_logo_content_type": content_type,
            "hosted_logo_bytes": file_size,
            "hosted_logo_etag": etag,
            "hosted_logo_at": datetime.now(tz=UTC).isoformat(),
        }
        try:
            variant_patch = mirror_logo_monochrome_variants_row(
                {id_field: target_row.get(id_field), **patch},
                kind=logo_kind,
                id_field=id_field,
                source_url=hosted_url,
                force=True,
                s3_client=s3_client,
                source="override",
            )
        except Exception as error:  # noqa: BLE001
            if not _is_non_blocking_logo_variant_failure(error):
                raise
            logger.warning(
                "Non-blocking logo variant mirror failure for %s %s (%s): %s",
                target_type,
                target_key,
                source_url,
                error,
            )
            variant_patch = None
        if variant_patch and isinstance(variant_patch.patch, dict):
            patch.update(variant_patch.patch)

        if set_primary:
            core_update = db.schema("core").table(table).update(patch).eq(id_field, target_row.get(id_field)).execute()
            if hasattr(core_update, "error") and core_update.error:
                raise RuntimeError(f"Failed to update {table} row: {core_update.error}")

        admin_show_sync._upsert_dimension_logo_asset_row(  # noqa: SLF001
            db,
            entity_type=entity_type,
            entity_key=entity_key,
            entity_id=entity_id,
            display_name=display_name,
            source_url=source_url,
            source_rank=0,
            mirror_status="mirrored",
            failure_reason=None,
            patch=patch,
            is_primary=bool(set_primary),
        )
        if set_primary:
            admin_show_sync._set_dimension_asset_primary_flag(  # noqa: SLF001
                db,
                entity_type=entity_type,
                entity_key=entity_key,
                source_url=source_url,
            )
        admin_show_sync._upsert_logo_import_audit(  # noqa: SLF001
            db,
            target_type=target_type,
            target_id=entity_id or target_key,
            target_key=entity_key,
            source_type="url",
            source_url=source_url,
            uploaded_filename=None,
            hosted_logo_url=hosted_url,
            hosted_logo_sha256=sha256,
            status="imported",
            failure_reason=None,
            created_by="admin-scrape",
        )
        return "imported", hosted_url, None

    logo_kind = f"brands-{target_type}"
    ext = guess_ext_from_content_type(content_type)
    s3_client = get_s3_client()
    bucket = get_s3_bucket()
    key = build_logo_s3_key(kind=logo_kind, entity_id=target_key.casefold(), sha256=sha256, ext=ext)
    etag, file_size = upload_bytes_to_s3(
        s3_client,
        bucket=bucket,
        key=key,
        data=image_data,
        content_type=content_type,
    )
    hosted_url = admin_show_sync.build_hosted_url(key)  # type: ignore[attr-defined]
    patch: dict[str, object] = {
        "hosted_logo_key": key,
        "hosted_logo_url": hosted_url,
        "hosted_logo_sha256": sha256,
        "hosted_logo_content_type": content_type,
        "hosted_logo_bytes": file_size,
        "hosted_logo_etag": etag,
        "hosted_logo_at": datetime.now(tz=UTC).isoformat(),
    }
    try:
        variant_patch = mirror_logo_monochrome_variants_row(
            {"id": target_key.casefold(), **patch},
            kind=logo_kind,
            source_url=hosted_url,
            force=True,
            s3_client=s3_client,
            source="override",
        )
    except Exception as error:  # noqa: BLE001
        if not _is_non_blocking_logo_variant_failure(error):
            raise
        logger.warning(
            "Non-blocking brand logo variant mirror failure for %s %s (%s): %s",
            target_type,
            target_key,
            source_url,
            error,
        )
        variant_patch = None
    if variant_patch and isinstance(variant_patch.patch, dict):
        patch.update(variant_patch.patch)

    if set_primary:
        reset_primary = (
            db.schema("admin")
            .table("brand_logo_assets")
            .update({"is_primary": False, "updated_at": datetime.now(tz=UTC).isoformat()})
            .eq("target_type", target_type)
            .eq("target_key", target_key.casefold())
            .execute()
        )
        if hasattr(reset_primary, "error") and reset_primary.error:
            raise RuntimeError(f"Failed to reset brand_logo_assets primary flag: {reset_primary.error}")

    payload = {
        "target_type": target_type,
        "target_key": target_key.casefold(),
        "target_label": target_label,
        "source_url": source_url,
        "source_page_url": source_page_url,
        "source_domain": source_domain,
        "source_rank": 0,
        "run_id": f"manual-{datetime.now(tz=UTC).strftime('%Y%m%dT%H%M%SZ')}",
        "hosted_logo_key": patch.get("hosted_logo_key"),
        "hosted_logo_url": patch.get("hosted_logo_url"),
        "hosted_logo_sha256": patch.get("hosted_logo_sha256"),
        "hosted_logo_content_type": patch.get("hosted_logo_content_type"),
        "hosted_logo_bytes": patch.get("hosted_logo_bytes"),
        "hosted_logo_etag": patch.get("hosted_logo_etag"),
        "hosted_logo_at": patch.get("hosted_logo_at"),
        "hosted_logo_black_key": patch.get("hosted_logo_black_key"),
        "hosted_logo_black_url": patch.get("hosted_logo_black_url"),
        "hosted_logo_black_sha256": patch.get("hosted_logo_black_sha256"),
        "hosted_logo_black_content_type": patch.get("hosted_logo_black_content_type"),
        "hosted_logo_black_bytes": patch.get("hosted_logo_black_bytes"),
        "hosted_logo_black_etag": patch.get("hosted_logo_black_etag"),
        "hosted_logo_black_at": patch.get("hosted_logo_black_at"),
        "hosted_logo_white_key": patch.get("hosted_logo_white_key"),
        "hosted_logo_white_url": patch.get("hosted_logo_white_url"),
        "hosted_logo_white_sha256": patch.get("hosted_logo_white_sha256"),
        "hosted_logo_white_content_type": patch.get("hosted_logo_white_content_type"),
        "hosted_logo_white_bytes": patch.get("hosted_logo_white_bytes"),
        "hosted_logo_white_etag": patch.get("hosted_logo_white_etag"),
        "hosted_logo_white_at": patch.get("hosted_logo_white_at"),
        "base_logo_format": admin_show_sync._detect_base_logo_format(  # noqa: SLF001
            source_url=source_url,
            content_type=str(content_type or "").strip() or None,
        ),
        "mirror_status": "mirrored",
        "failure_reason": None,
        "is_primary": bool(set_primary),
        "metadata": metadata,
        "updated_at": datetime.now(tz=UTC).isoformat(),
    }
    response = (
        db.schema("admin")
        .table("brand_logo_assets")
        .upsert(payload, on_conflict="target_type,target_key,source_url")
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Failed to upsert admin.brand_logo_assets row: {response.error}")
    rows = response.data or []
    asset_id = str(rows[0].get("id")) if rows and isinstance(rows[0], dict) and rows[0].get("id") else None

    admin_show_sync._upsert_logo_import_audit(  # noqa: SLF001
        db,
        target_type=target_type,
        target_id=target_key.casefold(),
        target_key=target_key.casefold(),
        source_type="url",
        source_url=source_url,
        uploaded_filename=None,
        hosted_logo_url=hosted_url,
        hosted_logo_sha256=sha256,
        status="imported",
        failure_reason=None,
        created_by="admin-scrape",
    )
    return "imported", hosted_url, asset_id


def _ensure_show_logo_variants_on_media_asset(
    *,
    db: SupabaseAdminClient,
    asset_id: str,
    hosted_url: str,
    show_identifier: str,
) -> None:
    from trr_backend.media.s3_mirror import get_s3_client, mirror_logo_monochrome_variants_row

    if not hosted_url or not show_identifier:
        return
    row = {
        "id": show_identifier,
        "hosted_logo_black_url": None,
        "hosted_logo_white_url": None,
    }
    variant = mirror_logo_monochrome_variants_row(
        row,
        kind="shows",
        source_url=hosted_url,
        force=True,
        s3_client=get_s3_client(),
        source="override",
    )
    if not variant or not isinstance(variant.patch, dict):
        return
    black_url = variant.patch.get("hosted_logo_black_url")
    white_url = variant.patch.get("hosted_logo_white_url")
    if not black_url and not white_url:
        return

    response = db.schema("core").table("media_assets").select("metadata").eq("id", asset_id).limit(1).execute()
    if hasattr(response, "error") and response.error:
        logger.warning("Failed to fetch media_asset %s metadata for logo variants: %s", asset_id, response.error)
        return
    rows = response.data or []
    current = rows[0] if rows else {}
    metadata = dict(current.get("metadata") or {})
    if black_url:
        metadata["logo_black_url"] = black_url
        metadata["hosted_logo_black_url"] = black_url
    if white_url:
        metadata["logo_white_url"] = white_url
        metadata["hosted_logo_white_url"] = white_url
    update = db.schema("core").table("media_assets").update({"metadata": metadata}).eq("id", asset_id).execute()
    if hasattr(update, "error") and update.error:
        logger.warning("Failed to update media_asset %s logo variant metadata: %s", asset_id, update.error)


# Endpoints


@router.post("/preview", response_model=ScrapePreviewResponse)
def preview_scrape(
    request: ScrapePreviewRequest,
    _: InternalAdminUser,
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
                bytes=getattr(img, "bytes", None),
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
    _: InternalAdminUser,
) -> ImportResponse:
    """
    Import selected images to S3 and save metadata to database.

    Images are:
    1. Downloaded and hashed
    2. Uploaded to S3 with content-addressed key
    3. Saved to media_assets table
    4. Linked to season via media_links table
    """
    from trr_backend.media.image_variants import generate_media_asset_variants
    from trr_backend.media.s3_mirror import (
        build_cast_photo_s3_key,
        build_hosted_url,
        build_season_image_s3_key,
        build_show_image_s3_key,
        get_s3_bucket,
        get_s3_client,
        guess_ext_from_content_type,
        upload_bytes_to_s3,
    )
    from trr_backend.repositories.media_assets import update_asset_with_mirror_result
    from trr_backend.repositories.web_scrape_images import (
        create_media_asset_from_scrape,
        create_media_link_for_entity,
        find_asset_by_sha256,
        get_person_identifier,
        get_season_and_show_identifiers,
    )

    # Extract domain for source naming
    source_domain = _extract_source_domain(str(request.source_url))
    source = f"web_scrape:{source_domain}"

    # Entity-specific setup. The entity_type Literal makes the chain below
    # exhaustive; defaults exist only so the type checker sees bindings.
    entity_id = ""
    resolved_show_id = ""
    resolved_season_number: int | None = None
    path_identifier = ""
    link_context: dict[str, Any] = {}
    if request.entity_type == "season":
        if request.season_id:
            entity_id = str(request.season_id)
            resolved_show_id = str(request.show_id) if request.show_id else ""
            resolved_season_number = request.season_number
            path_identifier = resolved_show_id or entity_id
        else:
            identifiers = get_season_and_show_identifiers(
                db,
                str(request.show_id) if request.show_id else None,
                request.season_number,
                season_id=str(request.season_id) if request.season_id else None,
            )
            if not identifiers:
                raise HTTPException(
                    status_code=404,
                    detail=f"Season {request.season_number} not found for show {request.show_id}",
                )
            entity_id = identifiers["season_id"]
            path_identifier = identifiers["show_identifier"]
            resolved_show_id = identifiers.get("show_id") or str(request.show_id)
            resolved_season_number = identifiers.get("season_number") or request.season_number
        link_context = {
            "entity_type": "season",
            "entity_id": entity_id,
            "show_id": resolved_show_id,
            "season_number": resolved_season_number,
            "source_url": str(request.source_url),
        }

    elif request.entity_type == "show":
        show_info = _get_show_identifier(db, str(request.show_id))
        if not show_info:
            raise HTTPException(
                status_code=404,
                detail=f"Show {request.show_id} not found",
            )
        entity_id = show_info["show_id"]
        resolved_show_id = show_info["show_id"]
        resolved_season_number = request.season_number
        path_identifier = show_info["show_identifier"]
        link_context = {
            "entity_type": "show",
            "entity_id": entity_id,
            "show_id": resolved_show_id,
            "source_url": str(request.source_url),
        }
        if request.season_number is not None:
            link_context["season_number"] = int(request.season_number)
        if request.season_id is not None:
            link_context["season_id"] = str(request.season_id)

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

    cast_members: list[dict] = []
    if request.match_cast and request.entity_type == "season":
        if resolved_season_number is None:
            logger.warning(
                "Season number missing for cast matching (show_id=%s, season_id=%s)",
                resolved_show_id,
                entity_id,
            )
        else:
            cast_members = _get_season_cast(db, resolved_show_id, resolved_season_number)
            logger.info(f"Loaded {len(cast_members)} cast members for matching")

    # Best-effort: fetch page metadata (publish date, title).
    page_title: str | None = None
    page_published_at: str | None = None
    if request.entity_type in ("season", "show"):
        try:
            from trr_backend.scraping.url_image_scraper import (
                extract_page_published_at,
                fetch_page_html,
            )

            html, page_title = fetch_page_html(str(request.source_url), timeout=20.0)
            page_published_at = extract_page_published_at(html)
            if page_title:
                link_context["source_page_title"] = page_title
                link_context["source_page_url"] = str(request.source_url)
        except Exception as exc:
            logger.warning("Failed to fetch page metadata for %s: %s", request.source_url, exc)

    s3_client = get_s3_client()
    bucket = get_s3_bucket()

    imported_count = 0
    skipped_count = 0
    errors: list[str] = []
    assets: list[MediaAssetSummary] = []
    auto_count_assets: dict[str, Any] = {}
    text_overlay_asset_ids: set[str] = set()

    for idx, img in enumerate(request.images):
        logo_target: tuple[LogoTargetType, str, str, bool] | None = None

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
            if img.kind == "logo" and _brand_logo_routing_v2_enabled():
                logo_target = _resolve_logo_target_for_image(
                    img=img,
                    request=request,
                    source_domain=source_domain,
                )
            # Download image
            image_data, sha256, content_type = download_and_hash_image(
                str(img.url),
                referer=str(request.source_url),
            )

            if img.kind == "logo" and _brand_logo_routing_v2_enabled() and logo_target and logo_target[0] != "show":
                _routed_asset, routed_url, routed_asset_id = _import_non_show_logo_target(
                    db=db,
                    target_type=logo_target[0],
                    target_key=logo_target[1],
                    target_label=logo_target[2],
                    set_primary=logo_target[3],
                    image_data=image_data,
                    sha256=sha256,
                    content_type=content_type,
                    source_url=str(img.url),
                    source_page_url=str(request.source_url),
                    source_domain=source_domain,
                    metadata={
                        "page_url": str(request.source_url),
                        "source_page_url": str(request.source_url),
                        "source_page_title": page_title,
                        "page_title": page_title,
                        "source_created_at": page_published_at,
                        "candidate_id": img.candidate_id,
                        "source_logo": img.source_logo,
                        "asset_name": img.asset_name,
                        "logo_target_type": logo_target[0],
                        "logo_target_key": logo_target[1],
                        "logo_target_label": logo_target[2],
                        "logo_set_primary": bool(logo_target[3]),
                    },
                )
                imported_count += 1
                assets.append(
                    MediaAssetSummary(
                        id=routed_asset_id or f"brand-logo-{idx}",
                        hosted_url=routed_url or "",
                        width=None,
                        height=None,
                        caption=img.caption,
                    )
                )
                continue

            # Build S3 key based on entity type (used for mirror updates too).
            # The entity_type Literal makes the chain exhaustive; the default
            # exists only so the type checker sees a binding.
            ext = guess_ext_from_content_type(content_type)
            s3_key = ""
            if request.entity_type == "season":
                season_number_for_key = resolved_season_number or request.season_number
                if season_number_for_key is None:
                    raise RuntimeError("season_number is required for season image key generation")
                s3_key = build_season_image_s3_key(
                    show_identifier=path_identifier,
                    season_number=season_number_for_key,
                    source="web_scrape",
                    sha256=sha256,
                    ext=ext,
                )
            elif request.entity_type == "show":
                s3_key = build_show_image_s3_key(
                    show_identifier=path_identifier,
                    kind=img.kind,
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

            # Check for duplicate
            existing = find_asset_by_sha256(db, sha256)
            if existing:
                # Asset exists - just create a link if needed
                logger.info(f"Image {idx} already exists with sha256={sha256[:16]}...")
                skipped_count += 1

                # If existing asset is not mirrored, mirror it now using the downloaded bytes
                if not existing.get("hosted_url"):
                    try:
                        etag, file_size = upload_bytes_to_s3(
                            s3_client,
                            bucket=bucket,
                            key=s3_key,
                            data=image_data,
                            content_type=content_type,
                        )
                        hosted_url = build_hosted_url(s3_key)
                        update_asset_with_mirror_result(
                            db,
                            asset_id=existing["id"],
                            sha256=sha256,
                            hosted_bucket=bucket,
                            hosted_key=s3_key,
                            hosted_url=hosted_url,
                            hosted_bytes=file_size,
                            hosted_content_type=content_type,
                            hosted_etag=etag,
                            completed_at=datetime.now(UTC).isoformat(),
                        )
                        existing["hosted_url"] = hosted_url
                    except Exception as exc:
                        logger.warning("Failed to mirror existing media_asset %s: %s", existing.get("id"), exc)

                # Still create link to entity
                tag_person_ids: set[str] = set()
                if img.person_ids:
                    tag_person_ids.update({str(person_id) for person_id in img.person_ids})
                if matched_person:
                    tag_person_ids.add(str(matched_person["person_id"]))
                tags_ctx = _build_people_tags_context(db, tag_person_ids)

                link_kind = img.kind if request.entity_type in ("season", "show") else "gallery"
                season_link_ctx = {**link_context, **tags_ctx}
                if page_published_at:
                    season_link_ctx["source_created_at"] = page_published_at
                if img.context_section:
                    season_link_ctx["context_section"] = img.context_section
                if img.context_type:
                    season_link_ctx["context_type"] = img.context_type
                _augment_logo_context(season_link_ctx, img=img, logo_target=logo_target)
                if img.asset_name:
                    season_link_ctx["asset_name"] = img.asset_name
                create_media_link_for_entity(
                    db,
                    entity_type=request.entity_type,
                    entity_id=entity_id,
                    media_asset_id=existing["id"],
                    kind=link_kind,
                    position=idx,
                    context=season_link_ctx,
                )
                if request.entity_type == "season" and resolved_show_id:
                    show_link_ctx = {
                        **season_link_ctx,
                        "linked_from_entity_type": "season",
                        "linked_from_entity_id": entity_id,
                    }
                    create_media_link_for_entity(
                        db,
                        entity_type="show",
                        entity_id=resolved_show_id,
                        media_asset_id=existing["id"],
                        kind=link_kind,
                        position=idx,
                        context=show_link_ctx,
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
                    if page_published_at:
                        person_link_ctx["source_created_at"] = page_published_at
                    if img.context_section:
                        person_link_ctx["context_section"] = img.context_section
                    if img.context_type:
                        person_link_ctx["context_type"] = img.context_type
                    _augment_logo_context(person_link_ctx, img=img, logo_target=logo_target)
                    if img.asset_name:
                        person_link_ctx["asset_name"] = img.asset_name
                    create_media_link_for_entity(
                        db,
                        entity_type="person",
                        entity_id=matched_person["person_id"],
                        media_asset_id=existing["id"],
                        kind="gallery",
                        position=idx,
                        context={**person_link_ctx, **tags_ctx},
                    )

                if request.entity_type == "season" and img.person_ids:
                    assigned_ids = {str(person_id) for person_id in img.person_ids}
                    if matched_person:
                        assigned_ids.discard(matched_person["person_id"])
                    for person_id in assigned_ids:
                        person_link_ctx = {
                            "entity_type": "person",
                            "entity_id": person_id,
                            "assigned_from_season": True,
                            "source_url": str(request.source_url),
                            "show_id": resolved_show_id,
                            "season_number": resolved_season_number,
                        }
                        if page_published_at:
                            person_link_ctx["source_created_at"] = page_published_at
                        if img.context_section:
                            person_link_ctx["context_section"] = img.context_section
                        if img.context_type:
                            person_link_ctx["context_type"] = img.context_type
                        _augment_logo_context(person_link_ctx, img=img, logo_target=logo_target)
                        if img.asset_name:
                            person_link_ctx["asset_name"] = img.asset_name
                        create_media_link_for_entity(
                            db,
                            entity_type="person",
                            entity_id=person_id,
                            media_asset_id=existing["id"],
                            kind="gallery",
                            position=idx,
                            context={**person_link_ctx, **tags_ctx},
                        )

                if existing.get("hosted_url"):
                    text_overlay_asset_ids.add(existing["id"])

                if request.entity_type == "person" or matched_person:
                    if existing.get("hosted_url"):
                        auto_count_assets[existing["id"]] = existing.get("hosted_url")

                try:
                    generate_media_asset_variants(
                        db,
                        asset_id=str(existing["id"]),
                        force=False,
                    )
                except Exception as exc:
                    logger.warning(
                        "Variant generation failed for existing media_asset %s: %s",
                        existing.get("id"),
                        exc,
                    )
                if (
                    img.kind == "logo"
                    and _brand_logo_routing_v2_enabled()
                    and logo_target
                    and logo_target[0] == "show"
                    and request.entity_type in {"season", "show"}
                ):
                    _ensure_show_logo_variants_on_media_asset(
                        db=db,
                        asset_id=str(existing["id"]),
                        hosted_url=str(existing.get("hosted_url") or ""),
                        show_identifier=str(path_identifier or ""),
                    )

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

            etag, file_size = upload_bytes_to_s3(
                s3_client,
                bucket=bucket,
                key=s3_key,
                data=image_data,
                content_type=content_type,
            )

            hosted_url = build_hosted_url(s3_key)

            # Create media asset record
            asset_metadata: dict[str, object] = {
                "page_url": str(request.source_url),
                "source_page_url": str(request.source_url),
                "source_page_title": page_title,
                "page_title": page_title,
                "source_created_at": page_published_at,
                "candidate_id": img.candidate_id,
                "source_logo": img.source_logo,
                "asset_name": img.asset_name,
            }
            _augment_logo_context(asset_metadata, img=img, logo_target=logo_target)
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
                metadata=asset_metadata,
            )

            # Create media link to entity
            tag_person_ids: set[str] = set()
            if img.person_ids:
                tag_person_ids.update({str(person_id) for person_id in img.person_ids})
            if matched_person:
                tag_person_ids.add(str(matched_person["person_id"]))
            tags_ctx = _build_people_tags_context(db, tag_person_ids)

            link_kind = img.kind if request.entity_type in ("season", "show") else "gallery"
            season_link_ctx = {**link_context, **tags_ctx}
            if page_published_at:
                season_link_ctx["source_created_at"] = page_published_at
            if img.context_section:
                season_link_ctx["context_section"] = img.context_section
            if img.context_type:
                season_link_ctx["context_type"] = img.context_type
            _augment_logo_context(season_link_ctx, img=img, logo_target=logo_target)
            if img.asset_name:
                season_link_ctx["asset_name"] = img.asset_name
            create_media_link_for_entity(
                db,
                entity_type=request.entity_type,
                entity_id=entity_id,
                media_asset_id=asset["id"],
                kind=link_kind,
                position=idx,
                context=season_link_ctx,
            )
            if request.entity_type == "season" and resolved_show_id:
                show_link_ctx = {
                    **season_link_ctx,
                    "linked_from_entity_type": "season",
                    "linked_from_entity_id": entity_id,
                }
                create_media_link_for_entity(
                    db,
                    entity_type="show",
                    entity_id=resolved_show_id,
                    media_asset_id=asset["id"],
                    kind=link_kind,
                    position=idx,
                    context=show_link_ctx,
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
                if page_published_at:
                    person_link_ctx["source_created_at"] = page_published_at
                if img.context_section:
                    person_link_ctx["context_section"] = img.context_section
                if img.context_type:
                    person_link_ctx["context_type"] = img.context_type
                _augment_logo_context(person_link_ctx, img=img, logo_target=logo_target)
                if img.asset_name:
                    person_link_ctx["asset_name"] = img.asset_name
                create_media_link_for_entity(
                    db,
                    entity_type="person",
                    entity_id=matched_person["person_id"],
                    media_asset_id=asset["id"],
                    kind="gallery",
                    position=idx,
                    context={**person_link_ctx, **tags_ctx},
                )

            if request.entity_type == "season" and img.person_ids:
                assigned_ids = {str(person_id) for person_id in img.person_ids}
                if matched_person:
                    assigned_ids.discard(matched_person["person_id"])
                for person_id in assigned_ids:
                    person_link_ctx = {
                        "entity_type": "person",
                        "entity_id": person_id,
                        "assigned_from_season": True,
                        "source_url": str(request.source_url),
                        "show_id": resolved_show_id,
                        "season_number": resolved_season_number,
                    }
                    if page_published_at:
                        person_link_ctx["source_created_at"] = page_published_at
                    if img.context_section:
                        person_link_ctx["context_section"] = img.context_section
                    if img.context_type:
                        person_link_ctx["context_type"] = img.context_type
                    _augment_logo_context(person_link_ctx, img=img, logo_target=logo_target)
                    if img.asset_name:
                        person_link_ctx["asset_name"] = img.asset_name
                    create_media_link_for_entity(
                        db,
                        entity_type="person",
                        entity_id=person_id,
                        media_asset_id=asset["id"],
                        kind="gallery",
                        position=idx,
                        context={**person_link_ctx, **tags_ctx},
                    )

            if hosted_url:
                text_overlay_asset_ids.add(asset["id"])

            if request.entity_type == "person" or matched_person:
                auto_count_url = img.url or hosted_url
                if auto_count_url:
                    auto_count_assets[asset["id"]] = auto_count_url

            try:
                generate_media_asset_variants(
                    db,
                    asset_id=str(asset["id"]),
                    force=False,
                )
            except Exception as exc:
                logger.warning(
                    "Variant generation failed for media_asset %s: %s",
                    asset.get("id"),
                    exc,
                )
            if (
                img.kind == "logo"
                and _brand_logo_routing_v2_enabled()
                and logo_target
                and logo_target[0] == "show"
                and request.entity_type in {"season", "show"}
            ):
                _ensure_show_logo_variants_on_media_asset(
                    db=db,
                    asset_id=str(asset["id"]),
                    hosted_url=hosted_url,
                    show_identifier=str(path_identifier or ""),
                )

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
            from trr_backend.repositories.media_links import (
                has_manual_people_tags,
                has_people_count,
                list_person_links_by_asset_id,
                update_person_links_context,
            )
            from trr_backend.vision.people_count_service import (
                PeopleCountServiceError,
                count_people,
                is_runtime_configured,
            )

            if not is_runtime_configured():
                auto_count_assets = {}
            else:
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
                    except PeopleCountServiceError as exc:
                        logger.warning("Auto-count failed for media_asset %s: %s", asset_id, exc)
        except Exception as exc:
            logger.warning("Auto-count setup failed: %s", exc)

    text_overlay_attempted = 0
    text_overlay_succeeded = 0
    text_overlay_failed = 0
    if text_overlay_asset_ids:
        try:
            from trr_backend.vision.text_overlay import (
                detect_and_update_media_asset_text_overlay,
                is_text_overlay_detection_configured,
            )

            if is_text_overlay_detection_configured():
                for asset_id in list(text_overlay_asset_ids)[:25]:
                    text_overlay_attempted += 1
                    try:
                        detect_and_update_media_asset_text_overlay(db, asset_id, force=False)
                        text_overlay_succeeded += 1
                    except Exception as exc:
                        text_overlay_failed += 1
                        logger.warning("Text overlay detect failed for media_asset %s: %s", asset_id, exc)
        except Exception as exc:
            logger.warning("Text overlay setup failed: %s", exc)

    return ImportResponse(
        imported=imported_count,
        skipped_duplicates=skipped_count,
        errors=errors,
        assets=assets,
        text_overlay_attempted=text_overlay_attempted,
        text_overlay_succeeded=text_overlay_succeeded,
        text_overlay_failed=text_overlay_failed,
    )


@router.post("/import/stream")
async def import_images_stream(
    request: ImportRequest,
    connection: Request,
    db: SupabaseAdminClient,
    admin: InternalAdminUser,
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
    from trr_backend.media.image_variants import generate_media_asset_variants
    from trr_backend.media.s3_mirror import (
        build_cast_photo_s3_key,
        build_hosted_url,
        build_season_image_s3_key,
        build_show_image_s3_key,
        get_s3_bucket,
        get_s3_client,
        guess_ext_from_content_type,
        upload_bytes_to_s3,
    )
    from trr_backend.repositories.media_assets import update_asset_with_mirror_result
    from trr_backend.repositories.web_scrape_images import (
        create_media_asset_from_scrape,
        create_media_link_for_entity,
        find_asset_by_sha256,
        get_person_identifier,
        get_season_and_show_identifiers,
    )

    async def event_generator() -> AsyncGenerator[str, None]:
        # Extract domain for source naming
        source_domain = _extract_source_domain(str(request.source_url))
        source = f"web_scrape:{source_domain}"

        # Entity-specific setup. The entity_type Literal makes the chain below
        # exhaustive; defaults exist only so the type checker sees bindings.
        entity_id = ""
        resolved_show_id = ""
        resolved_season_number: int | None = None
        path_identifier = ""
        link_context: dict[str, Any] = {}
        if request.entity_type == "season":
            if request.season_id:
                entity_id = str(request.season_id)
                resolved_show_id = str(request.show_id) if request.show_id else ""
                resolved_season_number = request.season_number
                path_identifier = resolved_show_id or entity_id
            else:
                identifiers = get_season_and_show_identifiers(
                    db,
                    str(request.show_id) if request.show_id else None,
                    request.season_number,
                    season_id=str(request.season_id) if request.season_id else None,
                )
                if not identifiers:
                    err_data = {"error": f"Season {request.season_number} not found"}
                    yield f"event: error\ndata: {json.dumps(err_data)}\n\n"
                    return
                entity_id = identifiers["season_id"]
                path_identifier = identifiers["show_identifier"]
                resolved_show_id = identifiers.get("show_id") or str(request.show_id)
                resolved_season_number = identifiers.get("season_number") or request.season_number
            link_context = {
                "entity_type": "season",
                "entity_id": entity_id,
                "show_id": resolved_show_id,
                "season_number": resolved_season_number,
                "source_url": str(request.source_url),
            }
        elif request.entity_type == "show":
            show_info = _get_show_identifier(db, str(request.show_id))
            if not show_info:
                err_data = {"error": f"Show {request.show_id} not found"}
                yield f"event: error\ndata: {json.dumps(err_data)}\n\n"
                return
            entity_id = show_info["show_id"]
            resolved_show_id = show_info["show_id"]
            resolved_season_number = request.season_number
            path_identifier = show_info["show_identifier"]
            link_context = {
                "entity_type": "show",
                "entity_id": entity_id,
                "show_id": resolved_show_id,
                "source_url": str(request.source_url),
            }
            if request.season_number is not None:
                link_context["season_number"] = int(request.season_number)
            if request.season_id is not None:
                link_context["season_id"] = str(request.season_id)
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
            if resolved_season_number is None:
                logger.warning(
                    "Season number missing for cast matching (show_id=%s, season_id=%s)",
                    resolved_show_id,
                    entity_id,
                )
            else:
                cast_members = _get_season_cast(db, resolved_show_id, resolved_season_number)
                logger.info(f"Loaded {len(cast_members)} cast members for matching")

        # Best-effort: fetch page metadata for CAST imports (publish date, title).
        page_title: str | None = None
        page_published_at: str | None = None
        wants_cast_meta = request.entity_type in ("season", "show")
        if wants_cast_meta:
            try:
                from trr_backend.scraping.url_image_scraper import (
                    extract_page_published_at,
                    fetch_page_html,
                )

                html, page_title = fetch_page_html(str(request.source_url), timeout=20.0)
                page_published_at = extract_page_published_at(html)
                if page_title:
                    link_context["source_page_title"] = page_title
                    link_context["source_page_url"] = str(request.source_url)
            except Exception as exc:
                logger.warning("Failed to fetch page metadata for %s: %s", request.source_url, exc)

        s3_client = get_s3_client()
        bucket = get_s3_bucket()

        imported_count = 0
        skipped_count = 0
        errors: list[str] = []
        assets: list[dict] = []
        auto_count_assets: dict[str, Any] = {}
        text_overlay_asset_ids: set[str] = set()
        total = len(request.images)

        for idx, img in enumerate(request.images):
            current = idx + 1
            img_url = str(img.url)
            logo_target: tuple[LogoTargetType, str, str, bool] | None = None

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
                if img.kind == "logo" and _brand_logo_routing_v2_enabled():
                    logo_target = _resolve_logo_target_for_image(
                        img=img,
                        request=request,
                        source_domain=source_domain,
                    )
                # Download image
                image_data, sha256, content_type = download_and_hash_image(
                    img_url,
                    referer=str(request.source_url),
                )

                if img.kind == "logo" and _brand_logo_routing_v2_enabled() and logo_target and logo_target[0] != "show":
                    _, routed_url, routed_asset_id = _import_non_show_logo_target(
                        db=db,
                        target_type=logo_target[0],
                        target_key=logo_target[1],
                        target_label=logo_target[2],
                        set_primary=logo_target[3],
                        image_data=image_data,
                        sha256=sha256,
                        content_type=content_type,
                        source_url=img_url,
                        source_page_url=str(request.source_url),
                        source_domain=source_domain,
                        metadata={
                            "page_url": str(request.source_url),
                            "source_page_url": str(request.source_url),
                            "source_page_title": page_title,
                            "page_title": page_title,
                            "source_created_at": page_published_at,
                            "candidate_id": img.candidate_id,
                            "source_logo": img.source_logo,
                            "asset_name": img.asset_name,
                            "logo_target_type": logo_target[0],
                            "logo_target_key": logo_target[1],
                            "logo_target_label": logo_target[2],
                            "logo_set_primary": bool(logo_target[3]),
                        },
                    )
                    imported_count += 1
                    assets.append(
                        {
                            "id": routed_asset_id or f"brand-logo-{idx}",
                            "hosted_url": routed_url or "",
                            "width": None,
                            "height": None,
                            "caption": img.caption,
                            "matched_person_id": None,
                            "matched_person_name": None,
                            "match_confidence": None,
                        }
                    )
                    imported_data = {
                        "current": current,
                        "total": total,
                        "url": img_url,
                        "asset_id": routed_asset_id,
                        "status": "success",
                        "matched_person_id": None,
                    }
                    yield f"event: imported\ndata: {json.dumps(imported_data)}\n\n"
                    continue

                # Build S3 key based on entity type (used for mirror updates too).
                # The entity_type Literal makes the chain exhaustive; the default
                # exists only so the type checker sees a binding.
                ext = guess_ext_from_content_type(content_type)
                s3_key = ""
                if request.entity_type == "season":
                    season_number_for_key = resolved_season_number or request.season_number
                    if season_number_for_key is None:
                        raise RuntimeError("season_number is required for season image key generation")
                    s3_key = build_season_image_s3_key(
                        show_identifier=path_identifier,
                        season_number=season_number_for_key,
                        source="web_scrape",
                        sha256=sha256,
                        ext=ext,
                    )
                elif request.entity_type == "show":
                    s3_key = build_show_image_s3_key(
                        show_identifier=path_identifier,
                        kind=img.kind,
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

                # Check for duplicate
                existing = find_asset_by_sha256(db, sha256)
                if existing:
                    logger.info(f"Image {idx} already exists with sha256={sha256[:16]}...")
                    skipped_count += 1

                    # If existing asset is not mirrored, mirror it now using the downloaded bytes
                    if not existing.get("hosted_url"):
                        try:
                            etag, file_size = upload_bytes_to_s3(
                                s3_client,
                                bucket=bucket,
                                key=s3_key,
                                data=image_data,
                                content_type=content_type,
                            )
                            hosted_url = build_hosted_url(s3_key)
                            update_asset_with_mirror_result(
                                db,
                                asset_id=existing["id"],
                                sha256=sha256,
                                hosted_bucket=bucket,
                                hosted_key=s3_key,
                                hosted_url=hosted_url,
                                hosted_bytes=file_size,
                                hosted_content_type=content_type,
                                hosted_etag=etag,
                                completed_at=datetime.now(UTC).isoformat(),
                            )
                            existing["hosted_url"] = hosted_url
                        except Exception as exc:
                            logger.warning(
                                "Failed to mirror existing media_asset %s: %s",
                                existing.get("id"),
                                exc,
                            )

                    # Still create link to entity
                    tag_person_ids: set[str] = set()
                    if img.person_ids:
                        tag_person_ids.update({str(person_id) for person_id in img.person_ids})
                    if matched_person:
                        tag_person_ids.add(str(matched_person["person_id"]))
                    tags_ctx = _build_people_tags_context(db, tag_person_ids)

                    link_kind = img.kind if request.entity_type in ("season", "show") else "gallery"
                    season_link_ctx = {**link_context, **tags_ctx}
                    if page_published_at:
                        season_link_ctx["source_created_at"] = page_published_at
                    if img.context_section:
                        season_link_ctx["context_section"] = img.context_section
                    if img.context_type:
                        season_link_ctx["context_type"] = img.context_type
                    _augment_logo_context(season_link_ctx, img=img, logo_target=logo_target)
                    if img.asset_name:
                        season_link_ctx["asset_name"] = img.asset_name
                    create_media_link_for_entity(
                        db,
                        entity_type=request.entity_type,
                        entity_id=entity_id,
                        media_asset_id=existing["id"],
                        kind=link_kind,
                        position=idx,
                        context=season_link_ctx,
                    )
                    if request.entity_type == "season" and resolved_show_id:
                        show_link_ctx = {
                            **season_link_ctx,
                            "linked_from_entity_type": "season",
                            "linked_from_entity_id": entity_id,
                        }
                        create_media_link_for_entity(
                            db,
                            entity_type="show",
                            entity_id=resolved_show_id,
                            media_asset_id=existing["id"],
                            kind=link_kind,
                            position=idx,
                            context=show_link_ctx,
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
                        if page_published_at:
                            person_link_ctx["source_created_at"] = page_published_at
                        if img.context_section:
                            person_link_ctx["context_section"] = img.context_section
                        if img.context_type:
                            person_link_ctx["context_type"] = img.context_type
                        _augment_logo_context(person_link_ctx, img=img, logo_target=logo_target)
                        if img.asset_name:
                            person_link_ctx["asset_name"] = img.asset_name
                        create_media_link_for_entity(
                            db,
                            entity_type="person",
                            entity_id=matched_person["person_id"],
                            media_asset_id=existing["id"],
                            kind="gallery",
                            position=idx,
                            context={**person_link_ctx, **tags_ctx},
                        )

                    if request.entity_type == "season" and img.person_ids:
                        assigned_ids = {str(person_id) for person_id in img.person_ids}
                        if matched_person:
                            assigned_ids.discard(matched_person["person_id"])
                        for person_id in assigned_ids:
                            person_link_ctx = {
                                "entity_type": "person",
                                "entity_id": person_id,
                                "assigned_from_season": True,
                                "source_url": str(request.source_url),
                                "show_id": resolved_show_id,
                                "season_number": resolved_season_number,
                            }
                            if page_published_at:
                                person_link_ctx["source_created_at"] = page_published_at
                            if img.context_section:
                                person_link_ctx["context_section"] = img.context_section
                            if img.context_type:
                                person_link_ctx["context_type"] = img.context_type
                            _augment_logo_context(person_link_ctx, img=img, logo_target=logo_target)
                            if img.asset_name:
                                person_link_ctx["asset_name"] = img.asset_name
                            create_media_link_for_entity(
                                db,
                                entity_type="person",
                                entity_id=person_id,
                                media_asset_id=existing["id"],
                                kind="gallery",
                                position=idx,
                                context={**person_link_ctx, **tags_ctx},
                            )

                    if existing.get("hosted_url"):
                        text_overlay_asset_ids.add(existing["id"])

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

                    if request.entity_type == "person" or matched_person:
                        if existing.get("hosted_url"):
                            auto_count_assets[existing["id"]] = existing.get("hosted_url")

                    try:
                        generate_media_asset_variants(
                            db,
                            asset_id=str(existing["id"]),
                            force=False,
                        )
                    except Exception as exc:
                        logger.warning(
                            "Variant generation failed for existing media_asset %s: %s",
                            existing.get("id"),
                            exc,
                        )
                    if (
                        img.kind == "logo"
                        and _brand_logo_routing_v2_enabled()
                        and logo_target
                        and logo_target[0] == "show"
                        and request.entity_type in {"season", "show"}
                    ):
                        _ensure_show_logo_variants_on_media_asset(
                            db=db,
                            asset_id=str(existing["id"]),
                            hosted_url=str(existing.get("hosted_url") or ""),
                            show_identifier=str(path_identifier or ""),
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

                etag, file_size = upload_bytes_to_s3(
                    s3_client,
                    bucket=bucket,
                    key=s3_key,
                    data=image_data,
                    content_type=content_type,
                )

                hosted_url = build_hosted_url(s3_key)

                # Create media asset record
                asset_metadata: dict[str, object] = {
                    "page_url": str(request.source_url),
                    "source_page_url": str(request.source_url),
                    "source_page_title": page_title,
                    "page_title": page_title,
                    "source_created_at": page_published_at,
                    "candidate_id": img.candidate_id,
                    "source_logo": img.source_logo,
                    "asset_name": img.asset_name,
                }
                _augment_logo_context(asset_metadata, img=img, logo_target=logo_target)
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
                    metadata=asset_metadata,
                )

                # Create media link to entity
                tag_person_ids: set[str] = set()
                if img.person_ids:
                    tag_person_ids.update({str(person_id) for person_id in img.person_ids})
                if matched_person:
                    tag_person_ids.add(str(matched_person["person_id"]))
                tags_ctx = _build_people_tags_context(db, tag_person_ids)

                link_kind = img.kind if request.entity_type in ("season", "show") else "gallery"
                season_link_ctx = {**link_context, **tags_ctx}
                if page_published_at:
                    season_link_ctx["source_created_at"] = page_published_at
                if img.context_section:
                    season_link_ctx["context_section"] = img.context_section
                if img.context_type:
                    season_link_ctx["context_type"] = img.context_type
                _augment_logo_context(season_link_ctx, img=img, logo_target=logo_target)
                if img.asset_name:
                    season_link_ctx["asset_name"] = img.asset_name
                create_media_link_for_entity(
                    db,
                    entity_type=request.entity_type,
                    entity_id=entity_id,
                    media_asset_id=asset["id"],
                    kind=link_kind,
                    position=idx,
                    context=season_link_ctx,
                )
                if request.entity_type == "season" and resolved_show_id:
                    show_link_ctx = {
                        **season_link_ctx,
                        "linked_from_entity_type": "season",
                        "linked_from_entity_id": entity_id,
                    }
                    create_media_link_for_entity(
                        db,
                        entity_type="show",
                        entity_id=resolved_show_id,
                        media_asset_id=asset["id"],
                        kind=link_kind,
                        position=idx,
                        context=show_link_ctx,
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
                    if page_published_at:
                        person_link_ctx["source_created_at"] = page_published_at
                    if img.context_section:
                        person_link_ctx["context_section"] = img.context_section
                    if img.context_type:
                        person_link_ctx["context_type"] = img.context_type
                    _augment_logo_context(person_link_ctx, img=img, logo_target=logo_target)
                    if img.asset_name:
                        person_link_ctx["asset_name"] = img.asset_name
                    create_media_link_for_entity(
                        db,
                        entity_type="person",
                        entity_id=matched_person["person_id"],
                        media_asset_id=asset["id"],
                        kind="gallery",
                        position=idx,
                        context={**person_link_ctx, **tags_ctx},
                    )

                if request.entity_type == "season" and img.person_ids:
                    assigned_ids = {str(person_id) for person_id in img.person_ids}
                    if matched_person:
                        assigned_ids.discard(matched_person["person_id"])
                    for person_id in assigned_ids:
                        person_link_ctx = {
                            "entity_type": "person",
                            "entity_id": person_id,
                            "assigned_from_season": True,
                            "source_url": str(request.source_url),
                            "show_id": resolved_show_id,
                            "season_number": resolved_season_number,
                        }
                        if page_published_at:
                            person_link_ctx["source_created_at"] = page_published_at
                        if img.context_section:
                            person_link_ctx["context_section"] = img.context_section
                        if img.context_type:
                            person_link_ctx["context_type"] = img.context_type
                        _augment_logo_context(person_link_ctx, img=img, logo_target=logo_target)
                        if img.asset_name:
                            person_link_ctx["asset_name"] = img.asset_name
                        create_media_link_for_entity(
                            db,
                            entity_type="person",
                            entity_id=person_id,
                            media_asset_id=asset["id"],
                            kind="gallery",
                            position=idx,
                            context={**person_link_ctx, **tags_ctx},
                        )

                if hosted_url:
                    text_overlay_asset_ids.add(asset["id"])

                    if request.entity_type == "person" or matched_person:
                        auto_count_url = img.url or hosted_url
                        if auto_count_url:
                            auto_count_assets[asset["id"]] = auto_count_url

                try:
                    generate_media_asset_variants(
                        db,
                        asset_id=str(asset["id"]),
                        force=False,
                    )
                except Exception as exc:
                    logger.warning(
                        "Variant generation failed for media_asset %s: %s",
                        asset.get("id"),
                        exc,
                    )
                if (
                    img.kind == "logo"
                    and _brand_logo_routing_v2_enabled()
                    and logo_target
                    and logo_target[0] == "show"
                    and request.entity_type in {"season", "show"}
                ):
                    _ensure_show_logo_variants_on_media_asset(
                        db=db,
                        asset_id=str(asset["id"]),
                        hosted_url=hosted_url,
                        show_identifier=str(path_identifier or ""),
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

        if auto_count_assets:
            try:
                from trr_backend.repositories.media_links import (
                    has_manual_people_tags,
                    has_people_count,
                    list_person_links_by_asset_id,
                    update_person_links_context,
                )
                from trr_backend.vision.people_count_service import PeopleCountServiceError, count_people

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
                    except PeopleCountServiceError as exc:
                        logger.warning("Auto-count failed for media_asset %s: %s", asset_id, exc)
            except Exception as exc:
                logger.warning("Auto-count setup failed: %s", exc)

        text_overlay_attempted = 0
        text_overlay_succeeded = 0
        text_overlay_failed = 0
        if text_overlay_asset_ids:
            try:
                from trr_backend.vision.text_overlay import (
                    detect_and_update_media_asset_text_overlay,
                    is_text_overlay_detection_configured,
                )

                if is_text_overlay_detection_configured():
                    for asset_id in list(text_overlay_asset_ids)[:25]:
                        text_overlay_attempted += 1
                        try:
                            detect_and_update_media_asset_text_overlay(db, asset_id, force=False)
                            text_overlay_succeeded += 1
                        except Exception as exc:
                            text_overlay_failed += 1
                            logger.warning(
                                "Text overlay detect failed for media_asset %s: %s",
                                asset_id,
                                exc,
                            )
            except Exception as exc:
                logger.warning("Text overlay setup failed: %s", exc)

        # Final completion event
        complete_data = {
            "imported": imported_count,
            "skipped_duplicates": skipped_count,
            "errors": errors,
            "assets": assets,
            "text_overlay_attempted": text_overlay_attempted,
            "text_overlay_succeeded": text_overlay_succeeded,
            "text_overlay_failed": text_overlay_failed,
        }
        yield f"event: complete\ndata: {json.dumps(complete_data)}\n\n"

    actor = str((admin or {}).get("email") or (admin or {}).get("id") or "admin")
    request_payload = {"request": request.model_dump(mode="json")}
    operation = start_operation_for_stream(
        operation_type="admin_scrape_import_images",
        producer=event_generator,
        request_payload=request_payload,
        initiated_by=actor,
        request=connection,
    )
    return operation_stream_response(str(operation.get("id")), request=connection)


def build_scrape_import_operation_producer(
    *,
    request_payload: dict[str, Any],
    db: SupabaseAdminClient | None = None,
):
    from trr_backend.db.admin import create_supabase_admin_client

    payload_data = request_payload.get("request") if isinstance(request_payload.get("request"), dict) else {}
    parsed_request = ImportRequest.model_validate(payload_data)
    initiated_by = str(request_payload.get("initiated_by") or "admin")

    def _producer():
        local_db = db or create_supabase_admin_client()
        started_payload = {
            "stage": "starting",
            "message": "Starting image import...",
            "total": len(parsed_request.images),
            "actor": initiated_by,
        }
        yield f"event: progress\ndata: {json.dumps(started_payload)}\n\n"
        try:
            result = import_images(parsed_request, local_db, {"id": initiated_by})
            complete_payload = result.model_dump() if isinstance(result, BaseModel) else dict(result or {})
            yield f"event: complete\ndata: {json.dumps(complete_payload)}\n\n"
        except HTTPException as exc:
            error_payload = {
                "stage": "error",
                "error": "Import failed",
                "detail": str(exc.detail),
                "status": int(exc.status_code),
            }
            yield f"event: error\ndata: {json.dumps(error_payload)}\n\n"
        except Exception as exc:  # noqa: BLE001
            error_payload = {
                "stage": "error",
                "error": "Import failed",
                "detail": str(exc),
            }
            yield f"event: error\ndata: {json.dumps(error_payload)}\n\n"

    return _producer
